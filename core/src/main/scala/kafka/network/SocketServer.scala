/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels.{Selector => NSelector, _}
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, KafkaChannel, ListenerName, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.slf4j.event.Level

import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.util.control.ControlThrowable

/**
  * Nio的socket服务器，线程模型是1个Acceptor线程处理新的链接，N个Processor线程处理来自socket的读请求
  * M个Handler线程处理请求并且产生响应到processor线程去写
  * An NIO socket server. The threading model is
  * 1 Acceptor thread that handles new connections
  * Acceptor has N Processor threads that each have their own selector and read requests from sockets
  * M Handler threads that handle requests and produce responses back to the processor threads for writing.
  */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {

  //最大的排队请求，requestQueue缓存的最大的请求的个数
  private val maxQueuedRequests = config.queuedMaxRequests

  //每一个IP的最大连接数
  private val maxConnectionsPerIp = config.maxConnectionsPerIp

  //Map<String,Integer>具体指定的ip的连接的限定个数
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  //日志上下文
  private val logContext = new LogContext(s"[SocketServer brokerId=${config.brokerId}] ")

  //日志的标志
  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", "socket-server-metrics")
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", "socket-server-metrics")
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))

  //内存池
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE

  //Processor线程和Handler线程之间交换数据的队列
  val requestChannel = new RequestChannel(maxQueuedRequests)

  //ProcessorId --> Processor
  private val processors = new ConcurrentHashMap[Int, Processor]()

  //生成ProcessorId
  private var nextProcessorId = 0

  //链接接收器，可以拓展,host+port+protocol --->Acceptor
  private[network] val acceptors = new ConcurrentHashMap[EndPoint, Acceptor]()
  private var connectionQuotas: ConnectionQuotas = _

  //是否停止处理请求
  private var stoppedProcessingRequests = false

  /**
    * Start the socket server
    */
  def startup() {
    this.synchronized {
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
      createAcceptorAndProcessors(config.numNetworkThreads, config.listeners)
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {

        def value: Double = SocketServer.this.synchronized {
          val ioWaitRatioMetricNames = processors.values.asScala.map { p =>
            metrics.metricName("io-wait-ratio", "socket-server-metrics", p.metricTags)
          }
          ioWaitRatioMetricNames.map { metricName =>
            Option(metrics.metric(metricName)).fold(0.0)(_.value)
          }.sum / processors.size
        }
      }
    )
    newGauge("MemoryPoolAvailable",
      new Gauge[Long] {
        def value = memoryPool.availableMemory()
      }
    )
    newGauge("MemoryPoolUsed",
      new Gauge[Long] {
        def value = memoryPool.size() - memoryPool.availableMemory()
      }
    )
    info("Started " + acceptors.size + " acceptor threads")
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

  private def createAcceptorAndProcessors(processorsPerListener: Int,
                                          endpoints: Seq[EndPoint]): Unit = synchronized {

    val sendBufferSize = config.socketSendBufferBytes
    val recvBufferSize = config.socketReceiveBufferBytes

    val brokerId = config.brokerId

    //遍历所有的host+port
    endpoints.foreach { endpoint =>
      val listenerName = endpoint.listenerName
      val securityProtocol = endpoint.securityProtocol

      val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId, connectionQuotas)
      //绑定到一个线程
      KafkaThread.nonDaemon(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor).start()
      //等到启动
      acceptor.awaitStartup()
      acceptors.put(endpoint, acceptor)
      //给acceptor添加Processor
      addProcessors(acceptor, endpoint, processorsPerListener)
    }
  }

  private def addProcessors(acceptor: Acceptor, endpoint: EndPoint, newProcessorsPerListener: Int): Unit = synchronized {
    val listenerName = endpoint.listenerName
    val securityProtocol = endpoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()

    for (_ <- 0 until newProcessorsPerListener) {
      val processor = newProcessor(nextProcessorId, connectionQuotas, listenerName, securityProtocol, memoryPool)
      listenerProcessors += processor
      requestChannel.addProcessor(processor)
      nextProcessorId += 1
    }
    listenerProcessors.foreach(p => processors.put(p.id, p))
    acceptor.addProcessors(listenerProcessors)
  }

  /**
    * Stop processing requests and new connections.
    */
  def stopProcessingRequests() = {
    info("Stopping socket server request processors")
    this.synchronized {
      acceptors.asScala.values.foreach(_.shutdown())
      processors.asScala.values.foreach(_.shutdown())
      requestChannel.clear()
      stoppedProcessingRequests = true
    }
    info("Stopped socket server request processors")
  }

  def resizeThreadPool(oldNumNetworkThreads: Int, newNumNetworkThreads: Int): Unit = synchronized {
    info(s"Resizing network thread pool size for each listener from $oldNumNetworkThreads to $newNumNetworkThreads")
    if (newNumNetworkThreads > oldNumNetworkThreads) {
      acceptors.asScala.foreach { case (endpoint, acceptor) =>
        addProcessors(acceptor, endpoint, newNumNetworkThreads - oldNumNetworkThreads)
      }
    } else if (newNumNetworkThreads < oldNumNetworkThreads)
      acceptors.asScala.values.foreach(_.removeProcessors(oldNumNetworkThreads - newNumNetworkThreads, requestChannel))
  }

  /**
    * Shutdown the socket server. If still processing requests, shutdown
    * acceptors and processors first.
    */
  def shutdown() = {
    info("Shutting down socket server")
    this.synchronized {
      if (!stoppedProcessingRequests)
        stopProcessingRequests()
      requestChannel.shutdown()
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      acceptors.get(endpoints(listenerName)).serverChannel.socket.getLocalPort
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    info(s"Adding listeners for endpoints $listenersAdded")
    createAcceptorAndProcessors(config.numNetworkThreads, listenersAdded)
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      acceptors.asScala.remove(endpoint).foreach(_.shutdown())
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider,
      memoryPool,
      logContext
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors.get(index)

}

/**
  * A base class with some helper variables and methods
  */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  //标志当前startup是否完成
  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  //标志当前的线程是否存活
  private val alive = new AtomicBoolean(true)

  /**
    * 交给子类实现
    */
  def wakeup(): Unit

  /**
    * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
    */
  def shutdown(): Unit = {

    //设置alive false
    if (alive.getAndSet(false))
      wakeup()
    shutdownLatch.await()
  }

  /**
    * 阻塞等待启动操作完成
    * Wait for the thread to completely start up
    */
  def awaitStartup(): Unit = startupLatch.await

  /**
    * Record that the thread startup is complete
    */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    //创建shutdownLatch
    shutdownLatch = new CountDownLatch(1)
    //标志启动成功了
    startupLatch.countDown()
  }

  /**
    * Record that the thread shutdown is complete
    */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
    * Is the server still running?
    */
  protected def isRunning: Boolean = alive.get

  /**
    * Close `channel` and decrement the connection count.
    */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      CoreUtils.swallow(channel.socket().close(), this, Level.ERROR)
      CoreUtils.swallow(channel.close(), this, Level.ERROR)
    }
  }
}

/**
  * Thread that accepts and configures new connections. There is one of these per endpoint.
  */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  //创建了选择器
  private val nioSelector = NSelector.open()

  //创建ServerSocketChannel
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  //创建存储Processor的缓冲区
  private val processors = new ArrayBuffer[Processor]()

  private[network] def addProcessors(newProcessors: Buffer[Processor]): Unit = synchronized {
    newProcessors.foreach { processor =>
      //分别创建线程
      KafkaThread.nonDaemon(s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor).start()
    }
    processors ++= newProcessors
  }

  private[network] def removeProcessors(removeCount: Int, requestChannel: RequestChannel): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.shutdown())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  override def shutdown(): Unit = {
    super.shutdown()
    processors.foreach(_.shutdown())
  }

  /**
    * Accept loop that checks for new connection attempts
    */
  def run() {
    //注册Accept事件
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    //开启完成了，唤醒等待的
    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          //接收Accept事件
          val ready = nioSelector.select(500)
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                //先删除
                iter.remove()
                //保证是连接事件，不然报错
                if (key.isAcceptable) {
                  //锁住自己，获取到轮询的processor,这种block真的不好理解
                  val processor = synchronized {
                    currentProcessor = currentProcessor % processors.size
                    processors(currentProcessor)
                  }
                  accept(key, processor)
                } else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread, mod(numProcessors) will be done later
                currentProcessor = currentProcessor + 1
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {

    //构造一个地址
    val socketAddress =
      if (host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)

    //打开ServerSocketChannel
    val serverChannel = ServerSocketChannel.open()
    //设置为非阻塞
    serverChannel.configureBlocking(false)

    //设置接受缓冲器的大小
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) {
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)
    }

    try {
      //绑定地址
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    return serverChannel
  }

  /**
    * 接收一个新的连接
    * Accept a new connection
    */
  def accept(key: SelectionKey, processor: Processor) {

    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]

    //这个时候都一样，并没有关联的状态，那么就获取一个连接就好了
    //SocketChannel
    val socketChannel = serverSocketChannel.accept()
    try {
      //连接的额度+1
      connectionQuotas.inc(socketChannel.socket().getInetAddress)

      //设置来自客户端的socket非阻塞
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      //设置为长连接属性
      socketChannel.socket().setKeepAlive(true)
      //设置发送缓冲区大小
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) {
        socketChannel.socket().setSendBufferSize(sendBufferSize)
      }

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
        .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
          socketChannel.socket.getSendBufferSize, sendBufferSize,
          socketChannel.socket.getReceiveBufferSize, recvBufferSize))
      //交给processor进行处理
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
    * Wakeup the thread for selection.
    */
  @Override
  def wakeup = nioSelector.wakeup()

}

private[kafka] object Processor {
  val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"
}

/**
  * 线程处理来自一个链接的所有的请求:主要用于请求和写回响应的操作,不参与真正的业务逻辑处理，也就是个框架的作用
  * Thread that processes all requests from a single connection. There are N of these running in parallel
  * each of which has its own selector
  */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               config: KafkaConfig,
                               metrics: Metrics,
                               credentialProvider: CredentialProvider,
                               memoryPool: MemoryPool,
                               logContext: LogContext) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  import Processor._

  //object存储static方法和常量
  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote, index) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort, Integer.parseInt(index))
        }
      }
      case _ => None
    }
  }

  /**
    * 连接id
    *
    * @param localHost
    * @param localPort
    * @param remoteHost
    * @param remotePort
    * @param index
    */
  private[network] case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
  }

  //新的连接，保存了由此Processor处理的新建的SocketChannel
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()

  //保存没有发送的响应，和客户端不会对服务器的响应进行再一次响应，所以inflightResponses在发送之后就删除
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()

  //响应队列
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  newGauge(IdlePercentMetricName,
    new Gauge[Double] {
      def value = {
        Option(metrics.metric(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags))).fold(0.0)(_.value)
      }
    },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString)
  )

  //负责管理网络连接
  private val selector = createSelector(
    ChannelBuilders.serverChannelBuilder(listenerName,
      listenerName == config.interBrokerListenerName,
      securityProtocol,
      config,
      credentialProvider.credentialCache,
      credentialProvider.tokenCache))

  // Visible to override for testing
  protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }
    new KSelector(
      maxRequestSize,
      connectionsMaxIdleMs,
      metrics,
      time,
      "socket-server",
      metricTags,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
  // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
  // closed, connection ids are not reused while requests from the closed connection are being processed.
  private var nextConnectionIndex = 0

  override def run() {
    //标识Processor的初始化完成
    startupComplete()
    try {
      while (isRunning) {
        try {

          /**
            * 处理新建的SocketChannel
            */
          // setup any new connections that have been queued up
          configureNewConnections()
          // register any new responses for writing
          /**
            * 处理要发送的响应
            */
          processNewResponses()

          /**
            * 读取请求，发送响应
            * 每次调用都会将读取的请求，发送成功的请求以及断开的连接加入completedReceives,completedSends,disconnected队列中等待处理
            */
          poll()
          processCompletedReceives()
          processCompletedSends()
          processDisconnected()
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug("Closing selector - processor " + id)
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  private def processException(errorMessage: String, throwable: Throwable) {
    throwable match {
      case e: ControlThrowable => throw e
      case e => error(errorMessage, e)
    }
  }

  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable) {
    if (openOrClosingChannel(channelId).isDefined) {
      error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }

  private def processNewResponses() {
    var curr: RequestChannel.Response = null
    while ( {
      //竟然能拆分成两个语句执行，其实它关心的是最后一句的判别
      curr = dequeueResponse();
      curr != null
    }) {
      val channelId = curr.request.context.connectionId
      try {
        curr.responseAction match {
          //没有响应需要发送给客户端
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(curr)
            trace("Socket server received empty response to send, registering for read: " + curr)
            openOrClosingChannel(channelId).foreach(c => selector.unmute(c.id))
          case RequestChannel.SendAction =>
            //表示Response需要发送给客户端，则查找对应的KafkaChannel,为它注册WRITE时间
            val responseSend = curr.responseSend.getOrElse(
              throw new IllegalStateException(s"responseSend must be defined for SendAction, response: $curr"))
            sendResponse(curr, responseSend)
          case RequestChannel.CloseConnectionAction =>
            //关闭连接
            updateRequestMetrics(curr)
            trace("Closing socket connection actively according to the response code.")
            close(channelId)
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send) {
    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long
    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L, response)
    }
    // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
    // removed from the Selector after discarding any pending staged receives.
    // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long
    if (openOrClosingChannel(connectionId).isDefined) {

      //发送响应
      selector.send(responseSend)
      //加入待发送的响应
      inflightResponses += (connectionId -> response)
    }
  }

  private def poll() {
    try selector.poll(300)
    catch {
      //匿名的对象
      case (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed due to illegal state or IO exception")
    }
  }

  /**
    * 处理已经完成的接收的请求
    */
  private def processCompletedReceives() {
    selector.completedReceives.asScala.foreach { receive =>
      try {
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            //抽取RequestHeader
            val header = RequestHeader.parse(receive.payload)
            //封装上下文
            val context = new RequestContext(header, receive.source, channel.socketAddress,
              channel.principal, listenerName, securityProtocol)
            //封装成请求
            val req = new RequestChannel.Request(processor = id, context = context,
              startTimeNanos = time.nanoseconds, memoryPool, receive.payload, requestChannel.metrics)
            //添加到requestQueue等待后续处理
            requestChannel.sendRequest(req)
            //取消注册OP_READ事件，连接不再读取数据
            selector.mute(receive.source)
          case None =>
            // This should never happen since completed receives are processed immediately after `poll()`
            throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
        }
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
  }

  private def processCompletedSends() {
    selector.completedSends.asScala.foreach { send =>
      try {
        //将inflightResponse里保存的数据删除
        val resp = inflightResponses.remove(send.destination).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
        }
        //更新一些统计
        updateRequestMetrics(resp)
        //为对应的连接注册OP_READ事件，允许从该连接读取数据
        selector.unmute(send.destination)
      } catch {
        case e: Throwable => processChannelException(send.destination,
          s"Exception while processing completed send to ${send.destination}", e)
      }
    }
  }

  private def updateRequestMetrics(response: RequestChannel.Response) {
    //获取到请求
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  /**
    * 处理断开的请求
    */
  private def processDisconnected() {
    selector.disconnected.keySet.asScala.foreach { connectionId =>
      try {
        val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        connectionQuotas.dec(InetAddress.getByName(remoteHost))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }

  /**
    * Close the connection identified by `connectionId` and decrement the connection count.
    * The channel will be immediately removed from the selector's `channels` or `closingChannels`
    * and no further disconnect notifications will be sent for this channel by the selector.
    * If responses are pending for the channel, they are dropped and metrics is updated.
    * If the channel has already been removed from selector, no action is taken.
    */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
    * Queue up a new connection for reading
    */
  def accept(socketChannel: SocketChannel) {
    //添加到newConnections
    newConnections.add(socketChannel)
    //唤醒Acceptor线程
    wakeup()
  }

  /**
    * Register any new connections that have been queued up
    */
  private def configureNewConnections() {

    //处理新来的连接，知道队列不是空的
    while (!newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        //注册读事件
        selector.register(connectionId(channel.socket), channel)
      } catch {
        // We explicitly catch all exceptions and close the socket to avoid a socket leak.
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          processException(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
    * Close the selector and all open connections
    */
  private def closeAll() {
    //关闭所有的channel
    selector.channels.asScala.foreach { channel =>
      close(channel.id)
    }
    //关闭selector
    selector.close()
    removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString))
  }

  // 'protected` to allow override for testing
  protected[network] def connectionId(socket: Socket): String = {
    val localHost = socket.getLocalAddress.getHostAddress
    val localPort = socket.getLocalPort
    val remoteHost = socket.getInetAddress.getHostAddress
    val remotePort = socket.getPort
    val connId = ConnectionId(localHost, localPort, remoteHost, remotePort, nextConnectionIndex).toString
    nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
    connId
  }

  private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
    responseQueue.put(response)
    wakeup()
  }

  private def dequeueResponse(): RequestChannel.Response = {
    //从响应队列中拿出响应
    val response = responseQueue.poll()
    if (response != null) {
      //在请求中写入响应的队列拿出的时间
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds()
    }
    response
  }

  private[network] def responseQueueSize = responseQueue.size

  // Only for testing
  private[network] def inflightResponseCount: Int = inflightResponses.size

  // Visible for testing
  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  // Visible for testing
  private[network] def numStagedReceives(connectionId: String): Int =
    openOrClosingChannel(connectionId).map(c => selector.numStagedReceives(c)).getOrElse(0)

  /**
    * Wakeup the thread for selection.
    */
  override def wakeup() = selector.wakeup()

  override def shutdown(): Unit = {
    super.shutdown()
    removeMetric("IdlePercent", Map("networkProcessor" -> id.toString))
  }

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }

  //地址->连接统计
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    //先锁住counts表
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
