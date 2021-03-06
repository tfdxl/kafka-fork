/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Higher level consumer access to the network layer with basic support for request futures. This class
 * is thread-safe, but provides no synchronization for response callbacks. This guarantees that no locks
 * are held when they are invoked.
 */
public class ConsumerNetworkClient implements Closeable {
    private static final int MAX_POLL_TIMEOUT_MS = 5000;

    // the mutable state of this class is protected by the object's monitor (excluding the wakeup
    // flag and the request completion queue below).
    private final Logger log;
    private final KafkaClient client;

    //缓冲队列
    private final UnsentRequests unsent = new UnsentRequests();

    //用于管理kafka集群的元数据
    private final Metadata metadata;
    private final Time time;
    private final long retryBackoffMs;
    private final int maxPollTimeoutMs;
    private final long unsentExpiryMs;
    private final AtomicBoolean wakeupDisabled = new AtomicBoolean();

    //我们不需要很高的吞吐量，所以使用公平锁去避免饿死
    // We do not need high throughput, so use a fair lock to try to avoid starvation
    private final ReentrantLock lock = new ReentrantLock(true);

    // when requests complete, they are transferred to this queue prior to invocation. The purpose
    // is to avoid invoking them while holding this object's monitor which can open the door for deadlocks.
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

    //由调用KafkaConsumer对象的消费者线程之外的其他线程进行设置，表示要中断KafkaConsumer线程
    // this flag allows the client to be safely woken up without waiting on the lock above. It is
    // atomic to avoid the need to acquire the lock above in order to enable it concurrently.
    private final AtomicBoolean wakeup = new AtomicBoolean(false);

    public ConsumerNetworkClient(LogContext logContext,
                                 KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 long requestTimeoutMs,
                                 int maxPollTimeoutMs) {
        this.log = logContext.logger(ConsumerNetworkClient.class);
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.maxPollTimeoutMs = Math.min(maxPollTimeoutMs, MAX_POLL_TIMEOUT_MS);
        this.unsentExpiryMs = requestTimeoutMs;
    }

    /**
     * 发送一个请求
     * 注意：请求并不会实际传输到网络，直到一次poll被调用，因此请求要么传输成功要么传输失败，通过future能够获取
     * send的结果，
     * Send a new request. Note that the request is not actually transmitted on the
     * network until one of the {@link #poll(long)} variants is invoked. At this
     * point the request will either be transmitted successfully or will fail.
     * Use the returned future to obtain the result of the send. Note that there is no
     * need to check for disconnects explicitly on the {@link ClientResponse} object;
     * instead, the future will be failed with a {@link DisconnectException}.
     *
     * @param node           The destination of the request
     * @param requestBuilder A builder for the request payload
     * @return A future which indicates the result of the send.
     */
    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        long now = time.milliseconds();

        //构造请求future完成的处理器
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();

        //包装成底层的请求
        ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,
                completionHandler);

        //放到对应node的没有发送的请求里面
        unsent.put(node, clientRequest);

        // wakeup the client in case it is blocking in poll so that we can send the queued request
        client.wakeup();
        return completionHandler.future;
    }

    /**
     * 查找kafka集群中负载最低的Node
     *
     * @return
     */
    public Node leastLoadedNode() {
        lock.lock();
        try {
            return client.leastLoadedNode(time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    public boolean hasReadyNodes() {
        lock.lock();
        try {
            return client.hasReadyNodes();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block until the metadata has been refreshed.
     */
    public void awaitMetadataUpdate() {
        awaitMetadataUpdate(Long.MAX_VALUE);
    }

    /**
     * Block waiting on the metadata refresh with a timeout.
     *
     * @return true if update succeeded, false otherwise.
     */
    public boolean awaitMetadataUpdate(long timeout) {
        long startMs = time.milliseconds();
        int version = this.metadata.requestUpdate();
        do {
            poll(timeout);
            AuthenticationException ex = this.metadata.getAndClearAuthenticationException();
            if (ex != null) {
                throw ex;
            }
            //当metadata版本没变和没超时的情况下，一直通过poll循环调用
        } while (this.metadata.version() == version && time.milliseconds() - startMs < timeout);
        return this.metadata.version() > version;
    }

    /**
     * Ensure our metadata is fresh (if an update is expected, this will block
     * until it has completed).
     */
    public void ensureFreshMetadata() {
        if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(time.milliseconds()) == 0) {
            awaitMetadataUpdate();
        }
    }

    /**
     * Wakeup an active poll. This will cause the polling thread to throw an exception either
     * on the current poll if one is active, or the next poll.
     */
    public void wakeup() {
        // wakeup should be safe without holding the client lock since it simply delegates to
        // Selector's wakeup, which is threadsafe
        log.debug("Received user wakeup");
        this.wakeup.set(true);
        this.client.wakeup();
    }

    /**
     * 无限阻塞，直到future完成
     * Block indefinitely until the given request future has finished.
     *
     * @param future The request future to await.
     * @throws WakeupException    if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(RequestFuture<?> future) {
        while (!future.isDone()) {
            poll(Long.MAX_VALUE, time.milliseconds(), future);
        }
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     *
     * @param future  The request future to wait for
     * @param timeout The maximum duration (in ms) to wait for the request
     * @return true if the future is done, false otherwise
     * @throws WakeupException    if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public boolean poll(RequestFuture<?> future, long timeout) {
        long begin = time.milliseconds();
        long remaining = timeout;
        long now = begin;
        do {
            poll(remaining, now, future);
            now = time.milliseconds();
            long elapsed = now - begin;
            remaining = timeout - elapsed;
        } while (!future.isDone() && remaining > 0);
        return future.isDone();
    }

    /**
     * Poll for any network IO.
     *
     * @param timeout The maximum time to wait for an IO event.
     * @throws WakeupException    if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(long timeout) {
        poll(timeout, time.milliseconds(), null);
    }

    /**
     * Poll for any network IO.
     *
     * @param timeout timeout in milliseconds
     * @param now     current time in milliseconds
     */
    public void poll(long timeout, long now, PollCondition pollCondition) {
        poll(timeout, now, pollCondition, false);
    }

    /**
     * Poll for any network IO.
     *
     * @param timeout       timeout in milliseconds 最长的阻塞时间
     * @param now           current time in milliseconds 当前的时间戳
     * @param disableWakeup If TRUE disable triggering wake-ups 表示
     */
    public void poll(long timeout, long now, PollCondition pollCondition, boolean disableWakeup) {
        // there may be handlers which need to be invoked if we woke up the previous call to poll
        firePendingCompletedRequests();

        lock.lock();
        try {
            // Handle async disconnects prior to attempting any sends
            handlePendingDisconnects();

            // send all the requests we can send now
            trySend(now);

            /**
             * 计算超时的时间，此超时时间由timeout和maxPollTimeoutMs的较小值决定，
             * 调用NetworkClient的poll方法
             */
            // check whether the poll is still needed by the caller. Note that if the expected completion
            // condition becomes satisfied after the call to shouldBlock() (because of a fired completion
            // handler), the client will be woken up.
            if (pendingCompletion.isEmpty() && (pollCondition == null || pollCondition.shouldBlock())) {

                //如果没有还米有响应的请求，那么就不用阻塞比retry backoff更长的时间
                // if there are no requests in flight, do not block longer than the retry backoff
                if (client.inFlightRequestCount() == 0) {
                    timeout = Math.min(timeout, retryBackoffMs);
                }
                client.poll(Math.min(maxPollTimeoutMs, timeout), now);
                //重置当前时间
                now = time.milliseconds();
            } else {
                client.poll(0, now);
            }

            // handle any disconnects by failing the active requests. note that disconnects must
            // be checked immediately following poll since any subsequent call to client.ready()
            // will reset the disconnect status
            checkDisconnects(now);
            if (!disableWakeup) {
                //查看是否有其他线程中断
                // trigger wakeups after checking for disconnects so that the callbacks will be ready
                // to be fired on the next call to poll()
                maybeTriggerWakeup();
            }
            // throw InterruptException if this thread is interrupted
            maybeThrowInterruptException();

            //再次尝试发送请求，因为缓冲空间也行被清掉了，或者连接已经完成
            // try again to send requests since buffer space may have been
            // cleared or a connect finished in the poll
            trySend(now);

            //使超时的请求失败掉
            // fail requests that couldn't be sent if they have expired
            failExpiredRequests(now);

            //清除没有发送的请求集合，避免map无效增长
            // clean unsent requests collection to keep the map from growing indefinitely
            unsent.clean();
        } finally {
            lock.unlock();
        }

        // called without the lock to avoid deadlock potential if handlers need to acquire locks
        firePendingCompletedRequests();
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups.
     */
    public void pollNoWakeup() {
        poll(0, time.milliseconds(), null, true);
    }

    /**
     * Block until all pending requests from the given node have finished.
     *
     * @param node      The node to await requests from
     * @param timeoutMs The maximum time in milliseconds to block
     * @return true If all requests finished, false if the timeout expired first
     */
    public boolean awaitPendingRequests(Node node, long timeoutMs) {

        //开始时间
        long startMs = time.milliseconds();
        //超时时间
        long remainingMs = timeoutMs;

        //等待unset和inFlightRequest中的请求全部完成(正常收到响应，或者出现异常)
        while (hasPendingRequests(node) && remainingMs > 0) {
            poll(remainingMs);
            remainingMs = timeoutMs - (time.milliseconds() - startMs);
        }

        return !hasPendingRequests(node);
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     *
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        lock.lock();
        try {
            return unsent.requestCount(node) + client.inFlightRequestCount(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     *
     * @param node The node in question
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests(Node node) {
        if (unsent.hasRequests(node)) {
            return true;
        }
        lock.lock();
        try {
            return client.hasInFlightRequests(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     *
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        lock.lock();
        try {
            return unsent.requestCount() + client.inFlightRequestCount();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     *
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests() {
        if (unsent.hasRequests()) {
            return true;
        }
        lock.lock();
        try {
            return client.hasInFlightRequests();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 触发完成的请求
     */
    private void firePendingCompletedRequests() {
        boolean completedRequestsFired = false;
        for (; ; ) {
            final RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null) {
                break;
            }

            completionHandler.fireCompletion();
            completedRequestsFired = true;
        }

        //唤醒client若果阻塞在future的完成上
        // wakeup the client in case it is blocking in poll for this future's completion
        if (completedRequestsFired) {
            client.wakeup();
        }
    }

    /**
     * 检测连接状态，检测消费者与每个Node之间的连接状态，当检测到连接断开的Node时，回将其在unsent集合中对应的全部ClientRequest对象清除
     * 之后调用这些ClientRequest的回调函数
     *
     * @param now
     */
    private void checkDisconnects(long now) {

        //任何的影响请求的连接断开操作已经被NetworkClient处理了，所以我们仅仅需要检查任何还没有发送的请求的连接是否已经断开了
        //如果断开的话，那么我们完成对应的future并且设置ClientResponse的断开标志
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse

        //遍历nodes
        for (Node node : unsent.nodes()) {

            //如果connection已经断开了
            if (client.connectionFailed(node)) {

                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
                //清除掉node对应的所有的请求
                Collection<ClientRequest> requests = unsent.remove(node);

                //遍历所有的请求
                for (ClientRequest request : requests) {

                    //获取请求完成的回调处理器
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();

                    //检查对应这个节点的认证是否异常
                    AuthenticationException authenticationException = client.authenticationException(node);

                    //认证异常，构造一个认证异常的结果
                    if (authenticationException != null) {
                        handler.onFailure(authenticationException);
                    } else
                        //没有认证异常的，那么直接完成这次请求
                    {
                        handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
                                request.callback(), request.destination(), request.createdTimeMs(), now, true,
                                null, null));
                    }
                }
            }
        }
    }

    private void handlePendingDisconnects() {
        lock.lock();
        try {
            while (true) {
                Node node = pendingDisconnects.poll();
                if (node == null) {
                    break;
                }

                failUnsentRequests(node, DisconnectException.INSTANCE);
                client.disconnect(node.idString());
            }
        } finally {
            lock.unlock();
        }
    }

    public void disconnectAsync(Node node) {
        pendingDisconnects.offer(node);
        client.wakeup();
    }

    private void failExpiredRequests(long now) {
        // clear all expired unsent requests and fail their corresponding futures
        Collection<ClientRequest> expiredRequests = unsent.removeExpiredRequests(now, unsentExpiryMs);
        for (ClientRequest request : expiredRequests) {
            RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
            handler.onFailure(new TimeoutException("Failed to send request after " + unsentExpiryMs + " ms."));
        }
    }

    private void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
        lock.lock();
        try {
            Collection<ClientRequest> unsentRequests = unsent.remove(node);
            for (ClientRequest unsentRequest : unsentRequests) {
                RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) unsentRequest.callback();
                handler.onFailure(e);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 循环处理unsent中缓存的请求
     *
     * @param now
     * @return
     */
    private boolean trySend(long now) {
        // send any requests that can be sent now
        boolean requestsSent = false;

        /**
         * 对每个node节点，循环遍历其对应的ClientRequest列表，每次循环都调用ready方法检测消费者与
         * 此节点之间的链接，以及发送请求的条件，如果符合发送条件，那么调用send方法
         */
        for (Node node : unsent.nodes()) {
            //获取node对应的请求
            final Iterator<ClientRequest> iterator = unsent.requestIterator(node);
            while (iterator.hasNext()) {
                final ClientRequest request = iterator.next();
                //检测Network.ready是否可以发送请求
                if (client.ready(node, now)) {
                    //发送请求
                    client.send(request, now);
                    //从unset集合中删除此请求
                    iterator.remove();
                    requestsSent = true;
                }
            }
        }
        return requestsSent;
    }

    /**
     * 查看是否有其他线程中断，如果有中断请求那么抛出WakeupException
     */
    public void maybeTriggerWakeup() {
        if (!wakeupDisabled.get() && wakeup.get()) {
            log.debug("Raising WakeupException in response to user wakeup");
            wakeup.set(false);
            throw new WakeupException();
        }
    }

    private void maybeThrowInterruptException() {
        if (Thread.interrupted()) {
            throw new InterruptException(new InterruptedException());
        }
    }

    public void disableWakeups() {
        wakeupDisabled.set(true);
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            client.close();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Find whether a previous connection has failed. Note that the failure state will persist until either
     * {@link #tryConnect(Node)} or {@link #send(Node, AbstractRequest.Builder)} has been called.
     *
     * @param node Node to connect to if possible
     */
    public boolean connectionFailed(Node node) {
        lock.lock();
        try {
            return client.connectionFailed(node);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 初始化一个连接如果当前可能
     * Initiate a connection if currently possible. This is only really useful for resetting the failed
     * status of a socket. If there is an actual request to send, then {@link #send(Node, AbstractRequest.Builder)}
     * should be used.
     *
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        lock.lock();
        try {
            client.ready(node, time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    /**
     * When invoking poll from a multi-threaded environment, it is possible that the condition that
     * the caller is awaiting has already been satisfied prior to the invocation of poll. We therefore
     * introduce this interface to push the condition checking as close as possible to the invocation
     * of poll. In particular, the check will be done while holding the lock used to protect concurrent
     * access to {@link org.apache.kafka.clients.NetworkClient}, which means implementations must be
     * very careful about locking order if the callback must acquire additional locks.
     */
    public interface PollCondition {
        /**
         * Return whether the caller is still awaiting an IO event.
         *
         * @return true if so, false otherwise.
         */
        boolean shouldBlock();
    }

    /**
     * 线程安全的工具类，保存了对每个node发送的请求，暂时还没有发送
     * A threadsafe helper class to hold requests per node that have not been sent yet
     */
    private final static class UnsentRequests {

        private final ConcurrentMap<Node, ConcurrentLinkedQueue<ClientRequest>> unsent;

        private UnsentRequests() {
            unsent = new ConcurrentHashMap<>();
        }

        public void put(Node node, ClientRequest request) {

            // 使用锁避免在queue上的put和remove并发操作
            // the lock protects the put from a concurrent removal of the queue for the node
            synchronized (unsent) {
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
                if (requests == null) {
                    requests = new ConcurrentLinkedQueue<>();
                    unsent.put(node, requests);
                }
                requests.add(request);
            }
        }

        /**
         * 统计node下面的请求
         *
         * @param node
         * @return
         */
        public int requestCount(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? 0 : requests.size();
        }

        /**
         * 统计所有的请求
         *
         * @return
         */
        public int requestCount() {
            int total = 0;
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) {
                total += requests.size();
            }
            return total;
        }

        /**
         * node对应是否有请求
         *
         * @param node
         * @return
         */
        public boolean hasRequests(Node node) {
            //获取node对应的请求
            final ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests != null && !requests.isEmpty();
        }

        /**
         * 是否有总请求
         *
         * @return
         */
        public boolean hasRequests() {
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) {
                if (!requests.isEmpty()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * 删除一些过期的请求
         * @param now
         * @param unsentExpiryMs
         * @return
         */
        public Collection<ClientRequest> removeExpiredRequests(long now, long unsentExpiryMs) {
            final List<ClientRequest> expiredRequests = new ArrayList<>();
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) {
                final Iterator<ClientRequest> requestIterator = requests.iterator();
                while (requestIterator.hasNext()) {
                    final ClientRequest request = requestIterator.next();
                    if (request.createdTimeMs() < now - unsentExpiryMs) {
                        expiredRequests.add(request);
                        requestIterator.remove();
                    } else {
                        break;
                    }
                }
            }
            return expiredRequests;
        }

        public void clean() {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                Iterator<ConcurrentLinkedQueue<ClientRequest>> iterator = unsent.values().iterator();
                while (iterator.hasNext()) {
                    ConcurrentLinkedQueue<ClientRequest> requests = iterator.next();
                    if (requests.isEmpty()) {
                        iterator.remove();
                    }
                }
            }
        }

        public Collection<ClientRequest> remove(Node node) {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                final ConcurrentLinkedQueue<ClientRequest> requests = unsent.remove(node);
                return requests == null ? Collections.<ClientRequest>emptyList() : requests;
            }
        }

        /**
         * 获取node下面缓存的请求
         *
         * @param node
         * @return
         */
        public Iterator<ClientRequest> requestIterator(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? Collections.<ClientRequest>emptyIterator() : requests.iterator();
        }

        /**
         * 获取所有的node
         *
         * @return
         */
        public Collection<Node> nodes() {
            return unsent.keySet();
        }
    }

    private class RequestFutureCompletionHandler implements RequestCompletionHandler {

        private final RequestFuture<ClientResponse> future;
        private ClientResponse response;
        private RuntimeException e;

        private RequestFutureCompletionHandler() {
            //构造一个future
            this.future = new RequestFuture<>();
        }

        public void fireCompletion() {
            if (e != null) {
                future.raise(e);
            } else if (response.wasDisconnected()) {
                RequestHeader requestHeader = response.requestHeader();
                int correlation = requestHeader.correlationId();
                log.debug("Cancelled {} request {} with correlation id {} due to node {} being disconnected",
                        requestHeader.apiKey(), requestHeader, correlation, response.destination());
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                future.raise(response.versionMismatch());
            } else {
                future.complete(response);
            }
        }

        public void onFailure(RuntimeException e) {
            this.e = e;
            pendingCompletion.add(this);
        }

        @Override
        public void onComplete(ClientResponse response) {
            this.response = response;
            pendingCompletion.add(this);
        }
    }

}
