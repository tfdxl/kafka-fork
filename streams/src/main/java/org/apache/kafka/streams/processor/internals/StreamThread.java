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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.*;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singleton;

public class StreamThread extends Thread {

    private final static int UNLIMITED_RECORDS = -1;
    private static final AtomicInteger STREAM_THREAD_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * Stream thread states are the possible states that a stream thread can be in.
     * A thread must only be in one state at a time
     * The expected state transitions with the following defined states is:
     * <p>
     * <pre>
     *                +-------------+
     *          +<--- | Created (0) |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +<--- | Running (1) | <----+
     *          |     +-----+-------+      |
     *          |           |              |
     *          |           v              |
     *          |     +-----+-------+      |
     *          +<--- | Partitions  |      |
     *          |     | Revoked (2) | <----+
     *          |     +-----+-------+      |
     *          |           |              |
     *          |           v              |
     *          |     +-----+-------+      |
     *          |     | Partitions  |      |
     *          |     | Assigned (3)| ---->+
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +---> | Pending     |
     *                | Shutdown (4)|
     *                +-----+-------+
     *                      |
     *                      v
     *                +-----+-------+
     *                | Dead (5)    |
     *                +-------------+
     * </pre>
     *
     * <p>
     * Note the following:
     * - Any state can go to PENDING_SHUTDOWN.
     * That is because streams can be closed at any time.
     * - State PENDING_SHUTDOWN may want to transit to some other states other than DEAD, in the corner case when
     * the shutdown is triggered while the thread is still in the rebalance loop.
     * In this case we will forbid the transition but will not treat as an error.
     * - State PARTITIONS_REVOKED may want transit to itself indefinitely, in the corner case when
     * the coordinator repeatedly fails in-between revoking partitions and assigning new partitions.
     * In this case we will forbid the transition but will not treat as an error.
     */
    public enum State implements ThreadStateTransitionValidator {
        CREATED(1, 4), RUNNING(2, 4), PARTITIONS_REVOKED(3, 4), PARTITIONS_ASSIGNED(1, 2, 4), PENDING_SHUTDOWN(5), DEAD;

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isRunning() {
            return equals(RUNNING) || equals(PARTITIONS_REVOKED) || equals(PARTITIONS_ASSIGNED);
        }

        @Override
        public boolean isValidTransition(final ThreadStateTransitionValidator newState) {
            State tmpState = (State) newState;
            return validTransitions.contains(tmpState.ordinal());
        }
    }

    /**
     * Listen to state change events
     */
    public interface StateListener {

        /**
         * Called when state changes
         *
         * @param thread   thread changing state
         * @param newState current state
         * @param oldState previous state
         */
        void onChange(final Thread thread, final ThreadStateTransitionValidator newState, final ThreadStateTransitionValidator oldState);
    }

    /**
     * Set the {@link StreamThread.StateListener} to be notified when state changes. Note this API is internal to
     * Kafka Streams and is not intended to be used by an external application.
     */
    public void setStateListener(final StreamThread.StateListener listener) {
        stateListener = listener;
    }

    /**
     * @return The state this instance is in
     */
    public State state() {
        // we do not need to use the state lock since the variable is volatile
        return state;
    }

    /**
     * Sets the state
     *
     * @param newState New state
     * @return The state prior to the call to setState, or null if the transition is invalid
     */
    State setState(final State newState) {
        final State oldState;

        synchronized (stateLock) {
            oldState = state;

            if (state == State.PENDING_SHUTDOWN && newState != State.DEAD) {
                // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                // refused but we do not throw exception here
                return null;
            } else if (state == State.DEAD) {
                // when the state is already in NOT_RUNNING, all its transitions
                // will be refused but we do not throw exception here
                return null;
            } else if (state == State.PARTITIONS_REVOKED && newState == State.PARTITIONS_REVOKED) {
                // when the state is already in PARTITIONS_REVOKED, its transition to itself will be
                // refused but we do not throw exception here
                return null;
            } else if (!state.isValidTransition(newState)) {
                log.error("Unexpected state transition from {} to {}", oldState, newState);
                throw new StreamsException(logPrefix + "Unexpected state transition from " + oldState + " to " + newState);
            } else {
                log.info("State transition from {} to {}", oldState, newState);
            }

            state = newState;
            if (newState == State.RUNNING) {
                updateThreadMetadata(taskManager.activeTasks(), taskManager.standbyTasks());
            } else {
                updateThreadMetadata(Collections.<TaskId, StreamTask>emptyMap(), Collections.<TaskId, StandbyTask>emptyMap());
            }
        }

        if (stateListener != null) {
            stateListener.onChange(this, state, oldState);
        }

        return oldState;
    }

    public boolean isRunningAndNotRebalancing() {
        // we do not need to grab stateLock since it is a single read
        return state == State.RUNNING;
    }

    public boolean isRunning() {
        synchronized (stateLock) {
            return state == State.RUNNING || state == State.PARTITIONS_REVOKED || state == State.PARTITIONS_ASSIGNED;
        }
    }

    static class RebalanceListener implements ConsumerRebalanceListener {
        private final Time time;
        private final TaskManager taskManager;
        private final StreamThread streamThread;
        private final Logger log;

        RebalanceListener(final Time time,
                          final TaskManager taskManager,
                          final StreamThread streamThread,
                          final Logger log) {
            this.time = time;
            this.taskManager = taskManager;
            this.streamThread = streamThread;
            this.log = log;
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> assignment) {
            log.debug("at state {}: partitions {} assigned at the end of consumer rebalance.\n" +
                            "\tcurrent suspended active tasks: {}\n" +
                            "\tcurrent suspended standby tasks: {}\n",
                    streamThread.state,
                    assignment,
                    taskManager.suspendedActiveTaskIds(),
                    taskManager.suspendedStandbyTaskIds());

            final long start = time.milliseconds();
            try {
                if (streamThread.setState(State.PARTITIONS_ASSIGNED) == null) {
                    return;
                }
                taskManager.createTasks(assignment);
            } catch (final Throwable t) {
                log.error("Error caught during partition assignment, " +
                        "will abort the current process and re-throw at the end of rebalance: {}", t.getMessage());
                streamThread.setRebalanceException(t);
            } finally {
                log.info("partition assignment took {} ms.\n" +
                                "\tcurrent active tasks: {}\n" +
                                "\tcurrent standby tasks: {}\n" +
                                "\tprevious active tasks: {}\n",
                        time.milliseconds() - start,
                        taskManager.activeTaskIds(),
                        taskManager.standbyTaskIds(),
                        taskManager.prevActiveTaskIds());
            }
        }

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> assignment) {
            log.debug("at state {}: partitions {} revoked at the beginning of consumer rebalance.\n" +
                            "\tcurrent assigned active tasks: {}\n" +
                            "\tcurrent assigned standby tasks: {}\n",
                    streamThread.state,
                    assignment,
                    taskManager.activeTaskIds(),
                    taskManager.standbyTaskIds());

            if (streamThread.setState(State.PARTITIONS_REVOKED) != null) {
                final long start = time.milliseconds();
                try {
                    // suspend active tasks
                    taskManager.suspendTasksAndState();
                } catch (final Throwable t) {
                    log.error("Error caught during partition revocation, " +
                            "will abort the current process and re-throw at the end of rebalance: {}", t.getMessage());
                    streamThread.setRebalanceException(t);
                } finally {
                    streamThread.clearStandbyRecords();

                    log.info("partition revocation took {} ms.\n" +
                                    "\tsuspended active tasks: {}\n" +
                                    "\tsuspended standby tasks: {}",
                            time.milliseconds() - start,
                            taskManager.suspendedActiveTaskIds(),
                            taskManager.suspendedStandbyTaskIds());
                }
            }
        }
    }

    static abstract class AbstractTaskCreator<T extends Task> {
        final String applicationId;
        final InternalTopologyBuilder builder;
        final StreamsConfig config;
        final StreamsMetrics streamsMetrics;
        final StateDirectory stateDirectory;
        final Sensor taskCreatedSensor;
        final ChangelogReader storeChangelogReader;
        final Time time;
        final Logger log;


        AbstractTaskCreator(final InternalTopologyBuilder builder,
                            final StreamsConfig config,
                            final StreamsMetrics streamsMetrics,
                            final StateDirectory stateDirectory,
                            final Sensor taskCreatedSensor,
                            final ChangelogReader storeChangelogReader,
                            final Time time,
                            final Logger log) {
            this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
            this.builder = builder;
            this.config = config;
            this.streamsMetrics = streamsMetrics;
            this.stateDirectory = stateDirectory;
            this.taskCreatedSensor = taskCreatedSensor;
            this.storeChangelogReader = storeChangelogReader;
            this.time = time;
            this.log = log;
        }

        public InternalTopologyBuilder builder() {
            return builder;
        }

        public StateDirectory stateDirectory() {
            return stateDirectory;
        }

        Collection<T> createTasks(final Consumer<byte[], byte[]> consumer, final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
            final List<T> createdTasks = new ArrayList<>();
            for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
                final TaskId taskId = newTaskAndPartitions.getKey();
                final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();
                T task = createTask(consumer, taskId, partitions);
                if (task != null) {
                    log.trace("Created task {} with assigned partitions {}", taskId, partitions);
                    createdTasks.add(task);
                }

            }
            return createdTasks;
        }

        abstract T createTask(final Consumer<byte[], byte[]> consumer, final TaskId id, final Set<TopicPartition> partitions);

        public void close() {
        }
    }

    static class TaskCreator extends AbstractTaskCreator<StreamTask> {
        private final ThreadCache cache;
        private final KafkaClientSupplier clientSupplier;
        private final String threadClientId;
        private final Producer<byte[], byte[]> threadProducer;

        TaskCreator(final InternalTopologyBuilder builder,
                    final StreamsConfig config,
                    final StreamsMetrics streamsMetrics,
                    final StateDirectory stateDirectory,
                    final Sensor taskCreatedSensor,
                    final ChangelogReader storeChangelogReader,
                    final ThreadCache cache,
                    final Time time,
                    final KafkaClientSupplier clientSupplier,
                    final Producer<byte[], byte[]> threadProducer,
                    final String threadClientId,
                    final Logger log) {
            super(builder,
                    config,
                    streamsMetrics,
                    stateDirectory,
                    taskCreatedSensor,
                    storeChangelogReader,
                    time,
                    log);
            this.cache = cache;
            this.clientSupplier = clientSupplier;
            this.threadProducer = threadProducer;
            this.threadClientId = threadClientId;
        }

        @Override
        StreamTask createTask(final Consumer<byte[], byte[]> consumer, final TaskId taskId, final Set<TopicPartition> partitions) {
            taskCreatedSensor.record();

            return new StreamTask(
                    taskId,
                    partitions,
                    builder.build(taskId.topicGroupId),
                    consumer,
                    storeChangelogReader,
                    config,
                    streamsMetrics,
                    stateDirectory,
                    cache,
                    time,
                    createProducer(taskId));

        }

        private Producer<byte[], byte[]> createProducer(final TaskId id) {
            // eos
            if (threadProducer == null) {
                final Map<String, Object> producerConfigs = config.getProducerConfigs(threadClientId + "-" + id);
                log.info("Creating producer client for task {}", id);
                producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, applicationId + "-" + id);
                return clientSupplier.getProducer(producerConfigs);
            }

            return threadProducer;
        }

        @Override
        public void close() {
            if (threadProducer != null) {
                try {
                    threadProducer.close();
                } catch (final Throwable e) {
                    log.error("Failed to close producer due to the following error:", e);
                }
            }
        }
    }

    static class StandbyTaskCreator extends AbstractTaskCreator<StandbyTask> {
        StandbyTaskCreator(final InternalTopologyBuilder builder,
                           final StreamsConfig config,
                           final StreamsMetrics streamsMetrics,
                           final StateDirectory stateDirectory,
                           final Sensor taskCreatedSensor,
                           final ChangelogReader storeChangelogReader,
                           final Time time,
                           final Logger log) {
            super(builder,
                    config,
                    streamsMetrics,
                    stateDirectory,
                    taskCreatedSensor,
                    storeChangelogReader,
                    time,
                    log);
        }

        @Override
        StandbyTask createTask(final Consumer<byte[], byte[]> consumer,
                               final TaskId taskId,
                               final Set<TopicPartition> partitions) {
            taskCreatedSensor.record();

            final ProcessorTopology topology = builder.build(taskId.topicGroupId);

            if (!topology.stateStores().isEmpty()) {
                return new StandbyTask(taskId,
                        partitions,
                        topology,
                        consumer,
                        storeChangelogReader,
                        config,
                        streamsMetrics,
                        stateDirectory);
            } else {
                log.trace("Skipped standby task {} with assigned partitions {} " +
                        "since it does not have any state stores to materialize", taskId, partitions);
                return null;
            }
        }
    }

    /**
     * This class extends {@link StreamsMetricsImpl(Metrics, String, String, Map)} and
     * overrides one of its functions for efficiency
     */
    static class StreamsMetricsThreadImpl extends StreamsMetricsImpl {
        final Sensor commitTimeSensor;
        final Sensor pollTimeSensor;
        final Sensor processTimeSensor;
        final Sensor punctuateTimeSensor;
        final Sensor taskCreatedSensor;
        final Sensor tasksClosedSensor;
        final Sensor skippedRecordsSensor;

        StreamsMetricsThreadImpl(final Metrics metrics, final String groupName, final String prefix, final Map<String, String> tags) {
            super(metrics, groupName, tags);
            commitTimeSensor = metrics.sensor(prefix + ".commit-latency", Sensor.RecordingLevel.INFO);
            commitTimeSensor.add(metrics.metricName("commit-latency-avg", this.groupName, "The average commit time in ms", this.tags), new Avg());
            commitTimeSensor.add(metrics.metricName("commit-latency-max", this.groupName, "The maximum commit time in ms", this.tags), new Max());
            commitTimeSensor.add(createMeter(metrics, new Count(), "commit", "commit calls"));

            pollTimeSensor = metrics.sensor(prefix + ".poll-latency", Sensor.RecordingLevel.INFO);
            pollTimeSensor.add(metrics.metricName("poll-latency-avg", this.groupName, "The average poll time in ms", this.tags), new Avg());
            pollTimeSensor.add(metrics.metricName("poll-latency-max", this.groupName, "The maximum poll time in ms", this.tags), new Max());
            pollTimeSensor.add(createMeter(metrics, new Count(), "poll", "record-poll calls"));

            processTimeSensor = metrics.sensor(prefix + ".process-latency", Sensor.RecordingLevel.INFO);
            processTimeSensor.add(metrics.metricName("process-latency-avg", this.groupName, "The average process time in ms", this.tags), new Avg());
            processTimeSensor.add(metrics.metricName("process-latency-max", this.groupName, "The maximum process time in ms", this.tags), new Max());
            processTimeSensor.add(createMeter(metrics, new Count(), "process", "process calls"));

            punctuateTimeSensor = metrics.sensor(prefix + ".punctuate-latency", Sensor.RecordingLevel.INFO);
            punctuateTimeSensor.add(metrics.metricName("punctuate-latency-avg", this.groupName, "The average punctuate time in ms", this.tags), new Avg());
            punctuateTimeSensor.add(metrics.metricName("punctuate-latency-max", this.groupName, "The maximum punctuate time in ms", this.tags), new Max());
            punctuateTimeSensor.add(createMeter(metrics, new Count(), "punctuate", "punctuate calls"));

            taskCreatedSensor = metrics.sensor(prefix + ".task-created", Sensor.RecordingLevel.INFO);
            taskCreatedSensor.add(createMeter(metrics, new Count(), "task-created", "newly created tasks"));

            tasksClosedSensor = metrics.sensor(prefix + ".task-closed", Sensor.RecordingLevel.INFO);
            tasksClosedSensor.add(createMeter(metrics, new Count(), "task-closed", "closed tasks"));

            skippedRecordsSensor = metrics.sensor(prefix + ".skipped-records");
            skippedRecordsSensor.add(createMeter(metrics, new Sum(), "skipped-records", "skipped records"));

        }

        private Meter createMeter(Metrics metrics, SampledStat stat, String baseName, String descriptiveName) {
            MetricName rateMetricName = metrics.metricName(baseName + "-rate", groupName,
                    String.format("The average per-second number of %s", descriptiveName), tags);
            MetricName totalMetricName = metrics.metricName(baseName + "-total", groupName,
                    String.format("The total number of %s", descriptiveName), tags);
            return new Meter(stat, rateMetricName, totalMetricName);
        }

        void removeAllSensors() {
            removeSensor(commitTimeSensor);
            removeSensor(pollTimeSensor);
            removeSensor(processTimeSensor);
            removeSensor(punctuateTimeSensor);
            removeSensor(taskCreatedSensor);
            removeSensor(tasksClosedSensor);
            removeSensor(skippedRecordsSensor);

        }
    }

    private final Time time;
    private final long pollTimeMs;
    private final long commitTimeMs;
    private final Object stateLock;
    private final Logger log;
    private final String logPrefix;
    private final TaskManager taskManager;
    private final StreamsMetricsThreadImpl streamsMetrics;

    private long lastCommitMs;
    private long timerStartedMs;
    private String originalReset;
    private Throwable rebalanceException = null;
    private boolean processStandbyRecords = false;
    private volatile State state = State.CREATED;
    private volatile ThreadMetadata threadMetadata;
    private StreamThread.StateListener stateListener;
    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> standbyRecords;

    // package-private for testing
    final ConsumerRebalanceListener rebalanceListener;
    final Consumer<byte[], byte[]> restoreConsumer;
    final Consumer<byte[], byte[]> consumer;
    final InternalTopologyBuilder builder;

    public static StreamThread create(final InternalTopologyBuilder builder,
                                      final StreamsConfig config,
                                      final KafkaClientSupplier clientSupplier,
                                      final AdminClient adminClient,
                                      final UUID processId,
                                      final String clientId,
                                      final Metrics metrics,
                                      final Time time,
                                      final StreamsMetadataState streamsMetadataState,
                                      final long cacheSizeBytes,
                                      final StateDirectory stateDirectory,
                                      final StateRestoreListener userStateRestoreListener) {
        final String threadClientId = clientId + "-StreamThread-" + STREAM_THREAD_ID_SEQUENCE.getAndIncrement();

        final String logPrefix = String.format("stream-thread [%s] ", threadClientId);
        final LogContext logContext = new LogContext(logPrefix);
        final Logger log = logContext.logger(StreamThread.class);

        log.info("Creating restore consumer client");
        final Map<String, Object> restoreConsumerConfigs = config.getRestoreConsumerConfigs(threadClientId);
        final Consumer<byte[], byte[]> restoreConsumer = clientSupplier.getRestoreConsumer(restoreConsumerConfigs);
        final StoreChangelogReader changelogReader = new StoreChangelogReader(restoreConsumer, userStateRestoreListener, logContext);

        Producer<byte[], byte[]> threadProducer = null;
        final boolean eosEnabled = StreamsConfig.EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        if (!eosEnabled) {
            final Map<String, Object> producerConfigs = config.getProducerConfigs(threadClientId);
            log.info("Creating shared producer client");
            threadProducer = clientSupplier.getProducer(producerConfigs);
        }

        StreamsMetricsThreadImpl streamsMetrics = new StreamsMetricsThreadImpl(
                metrics,
                "stream-metrics",
                "thread." + threadClientId,
                Collections.singletonMap("client-id", threadClientId));

        final ThreadCache cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);

        final AbstractTaskCreator<StreamTask> activeTaskCreator = new TaskCreator(builder,
                config,
                streamsMetrics,
                stateDirectory,
                streamsMetrics.taskCreatedSensor,
                changelogReader,
                cache,
                time,
                clientSupplier,
                threadProducer,
                threadClientId,
                log);
        final AbstractTaskCreator<StandbyTask> standbyTaskCreator = new StandbyTaskCreator(builder,
                config,
                streamsMetrics,
                stateDirectory,
                streamsMetrics.taskCreatedSensor,
                changelogReader,
                time,
                log);
        TaskManager taskManager = new TaskManager(changelogReader,
                processId,
                logPrefix,
                restoreConsumer,
                streamsMetadataState,
                activeTaskCreator,
                standbyTaskCreator,
                adminClient,
                new AssignedStreamsTasks(logContext),
                new AssignedStandbyTasks(logContext));

        log.info("Creating consumer client");
        final String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        final Map<String, Object> consumerConfigs = config.getConsumerConfigs(applicationId, threadClientId);
        consumerConfigs.put(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);
        String originalReset = null;
        if (!builder.latestResetTopicsPattern().pattern().equals("") || !builder.earliestResetTopicsPattern().pattern().equals("")) {
            originalReset = (String) consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        }
        final Consumer<byte[], byte[]> consumer = clientSupplier.getConsumer(consumerConfigs);
        taskManager.setConsumer(consumer);

        return new StreamThread(time,
                config,
                restoreConsumer,
                consumer,
                originalReset,
                taskManager,
                streamsMetrics,
                builder,
                threadClientId,
                logContext);
    }

    public StreamThread(final Time time,
                        final StreamsConfig config,
                        final Consumer<byte[], byte[]> restoreConsumer,
                        final Consumer<byte[], byte[]> consumer,
                        final String originalReset,
                        final TaskManager taskManager,
                        final StreamsMetricsThreadImpl streamsMetrics,
                        final InternalTopologyBuilder builder,
                        final String threadClientId,
                        final LogContext logContext) {
        super(threadClientId);

        this.stateLock = new Object();
        this.standbyRecords = new HashMap<>();

        this.time = time;
        this.builder = builder;
        this.streamsMetrics = streamsMetrics;
        this.logPrefix = logContext.logPrefix();
        this.log = logContext.logger(StreamThread.class);
        this.rebalanceListener = new RebalanceListener(time, taskManager, this, this.log);
        this.taskManager = taskManager;
        this.restoreConsumer = restoreConsumer;
        this.consumer = consumer;
        this.originalReset = originalReset;

        this.pollTimeMs = config.getLong(StreamsConfig.POLL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);

        updateThreadMetadata(Collections.<TaskId, StreamTask>emptyMap(), Collections.<TaskId, StandbyTask>emptyMap());
    }

    /**
     * Execute the stream processors
     *
     * @throws KafkaException   for any Kafka-related exceptions
     * @throws RuntimeException for any other non-Kafka exceptions
     */
    @Override
    public void run() {
        log.info("Starting");
        if (setState(State.RUNNING) == null) {
            log.info("StreamThread already shutdown. Not running");
            return;
        }
        boolean cleanRun = false;
        try {
            runLoop();
            cleanRun = true;
        } catch (final KafkaException e) {
            // just re-throw the exception as it should be logged already
            throw e;
        } catch (final Exception e) {
            // we have caught all Kafka related exceptions, and other runtime exceptions
            // should be due to user application errors
            log.error("Encountered the following error during processing:", e);
            throw e;
        } finally {
            completeShutdown(cleanRun);
        }
    }

    void setRebalanceException(final Throwable rebalanceException) {
        this.rebalanceException = rebalanceException;
    }

    /**
     * Main event loop for polling, and processing records through topologies.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException      if the store's change log does not contain the partition
     */
    private void runLoop() {
        long recordsProcessedBeforeCommit = UNLIMITED_RECORDS;
        consumer.subscribe(builder.sourceTopicPattern(), rebalanceListener);

        while (isRunning()) {
            try {
                recordsProcessedBeforeCommit = runOnce(recordsProcessedBeforeCommit);
            } catch (final TaskMigratedException ignoreAndRejoinGroup) {
                log.warn("Detected a task that got migrated to another thread. " +
                        "This implies that this thread missed a rebalance and dropped out of the consumer group. " +
                        "Trying to rejoin the consumer group now.", ignoreAndRejoinGroup);
            }
        }
    }

    /**
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException      if the store's change log does not contain the partition
     * @throws TaskMigratedException if another thread wrote to the changelog topic that is currently restored
     *                               or if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    // Visible for testing
    long runOnce(final long recordsProcessedBeforeCommit) {
        long processedBeforeCommit = recordsProcessedBeforeCommit;

        ConsumerRecords<byte[], byte[]> records;

        timerStartedMs = time.milliseconds();

        if (state == State.PARTITIONS_ASSIGNED) {
            // try to fetch some records with zero poll millis
            // to unblock the restoration as soon as possible
            records = pollRequests(0L);

            if (taskManager.updateNewAndRestoringTasks()) {
                setState(State.RUNNING);
            }
        } else {
            // try to fetch some records if necessary
            records = pollRequests(pollTimeMs);

            // if state changed after the poll call,
            // try to initialize the assigned tasks again
            if (state == State.PARTITIONS_ASSIGNED) {
                if (taskManager.updateNewAndRestoringTasks()) {
                    setState(State.RUNNING);
                }
            }
        }

        if (records != null && !records.isEmpty() && taskManager.hasActiveRunningTasks()) {
            streamsMetrics.pollTimeSensor.record(computeLatency(), timerStartedMs);
            addRecordsToTasks(records);
            final long totalProcessed = processAndMaybeCommit(recordsProcessedBeforeCommit);
            if (totalProcessed > 0) {
                final long processLatency = computeLatency();
                streamsMetrics.processTimeSensor.record(processLatency / (double) totalProcessed,
                        timerStartedMs);
                processedBeforeCommit = adjustRecordsProcessedBeforeCommit(recordsProcessedBeforeCommit,
                        totalProcessed,
                        processLatency,
                        commitTimeMs);
            }
        }

        punctuate();
        maybeCommit(timerStartedMs);
        maybeUpdateStandbyTasks(timerStartedMs);
        return processedBeforeCommit;
    }

    /**
     * Get the next batch of records by polling.
     *
     * @param pollTimeMs poll time millis parameter for the consumer poll
     * @return Next batch of records or null if no records available.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    private ConsumerRecords<byte[], byte[]> pollRequests(final long pollTimeMs) {
        ConsumerRecords<byte[], byte[]> records = null;

        try {
            records = consumer.poll(pollTimeMs);
        } catch (final InvalidOffsetException e) {
            resetInvalidOffsets(e);
        }

        if (rebalanceException != null) {
            if (rebalanceException instanceof TaskMigratedException) {
                throw (TaskMigratedException) rebalanceException;
            } else {
                throw new StreamsException(logPrefix + "Failed to rebalance.", rebalanceException);
            }
        }

        return records;
    }

    private void resetInvalidOffsets(final InvalidOffsetException e) {
        final Set<TopicPartition> partitions = e.partitions();
        final Set<String> loggedTopics = new HashSet<>();
        final Set<TopicPartition> seekToBeginning = new HashSet<>();
        final Set<TopicPartition> seekToEnd = new HashSet<>();

        for (final TopicPartition partition : partitions) {
            if (builder.earliestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToBeginning, "Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
            } else if (builder.latestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToEnd, "Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
            } else {
                if (originalReset == null || (!originalReset.equals("earliest") && !originalReset.equals("latest"))) {
                    final String errorMessage = "No valid committed offset found for input topic %s (partition %s) and no valid reset policy configured." +
                            " You need to set configuration parameter \"auto.offset.reset\" or specify a topic specific reset " +
                            "policy via StreamsBuilder#stream(..., Consumed.with(Topology.AutoOffsetReset)) or StreamsBuilder#table(..., Consumed.with(Topology.AutoOffsetReset))";
                    throw new StreamsException(String.format(errorMessage, partition.topic(), partition.partition()), e);
                }

                if (originalReset.equals("earliest")) {
                    addToResetList(partition, seekToBeginning, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "earliest", loggedTopics);
                } else if (originalReset.equals("latest")) {
                    addToResetList(partition, seekToEnd, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "latest", loggedTopics);
                }
            }
        }

        if (!seekToBeginning.isEmpty()) {
            consumer.seekToBeginning(seekToBeginning);
        }
        if (!seekToEnd.isEmpty()) {
            consumer.seekToEnd(seekToEnd);
        }
    }

    private void addToResetList(final TopicPartition partition, final Set<TopicPartition> partitions, final String logMessage, final String resetPolicy, final Set<String> loggedTopics) {
        final String topic = partition.topic();
        if (loggedTopics.add(topic)) {
            log.info(logMessage, topic, resetPolicy);
        }
        partitions.add(partition);
    }

    /**
     * Take records and add them to each respective task
     *
     * @param records Records, can be null
     */
    private void addRecordsToTasks(final ConsumerRecords<byte[], byte[]> records) {
        if (records != null && !records.isEmpty()) {
            int numAddedRecords = 0;

            for (final TopicPartition partition : records.partitions()) {
                final StreamTask task = taskManager.activeTask(partition);
                numAddedRecords += task.addRecords(partition, records.records(partition));
            }
            streamsMetrics.skippedRecordsSensor.record(records.count() - numAddedRecords, timerStartedMs);
        }
    }

    /**
     * Schedule the records processing by selecting which record is processed next. Commits may
     * happen as records are processed.
     *
     * @param recordsProcessedBeforeCommit number of records to be processed before commit is called.
     *                                     if UNLIMITED_RECORDS, then commit is never called
     * @return Number of records processed since last commit.
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    private long processAndMaybeCommit(final long recordsProcessedBeforeCommit) {

        long processed;
        long totalProcessedSinceLastMaybeCommit = 0;
        // Round-robin scheduling by taking one record from each task repeatedly
        // until no task has any records left
        do {
            processed = taskManager.process();
            if (processed > 0) {
                streamsMetrics.processTimeSensor.record(computeLatency() / (double) processed, timerStartedMs);
            }
            totalProcessedSinceLastMaybeCommit += processed;

            punctuate();

            if (recordsProcessedBeforeCommit != UNLIMITED_RECORDS &&
                    totalProcessedSinceLastMaybeCommit >= recordsProcessedBeforeCommit) {
                totalProcessedSinceLastMaybeCommit = 0;
                maybeCommit(timerStartedMs);
            }
            // commit any tasks that have requested a commit
            final int committed = taskManager.maybeCommitActiveTasks();
            if (committed > 0) {
                streamsMetrics.commitTimeSensor.record(computeLatency() / (double) committed, timerStartedMs);
            }
        } while (processed != 0);

        return totalProcessedSinceLastMaybeCommit;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    private void punctuate() {
        final int punctuated = taskManager.punctuate();
        if (punctuated > 0) {
            streamsMetrics.punctuateTimeSensor.record(computeLatency() / (double) punctuated, timerStartedMs);
        }
    }

    /**
     * Adjust the number of records that should be processed by scheduler. This avoids
     * scenarios where the processing time is higher than the commit time.
     *
     * @param prevRecordsProcessedBeforeCommit Previous number of records processed by scheduler.
     * @param totalProcessed                   Total number of records processed in this last round.
     * @param processLatency                   Total processing latency in ms processed in this last round.
     * @param commitTime                       Desired commit time in ms.
     * @return An adjusted number of records to be processed in the next round.
     */
    private long adjustRecordsProcessedBeforeCommit(final long prevRecordsProcessedBeforeCommit, final long totalProcessed,
                                                    final long processLatency, final long commitTime) {
        long recordsProcessedBeforeCommit = UNLIMITED_RECORDS;
        // check if process latency larger than commit latency
        // note that once we set recordsProcessedBeforeCommit, it will never be UNLIMITED_RECORDS again, so
        // we will never process all records again. This might be an issue if the initial measurement
        // was off due to a slow start.
        if (processLatency > 0 && processLatency > commitTime) {
            // push down
            recordsProcessedBeforeCommit = Math.max(1, (commitTime * totalProcessed) / processLatency);
            log.debug("processing latency {} > commit time {} for {} records. Adjusting down recordsProcessedBeforeCommit={}",
                    processLatency, commitTime, totalProcessed, recordsProcessedBeforeCommit);
        } else if (prevRecordsProcessedBeforeCommit != UNLIMITED_RECORDS && processLatency > 0) {
            // push up
            recordsProcessedBeforeCommit = Math.max(1, (commitTime * totalProcessed) / processLatency);
            log.debug("processing latency {} < commit time {} for {} records. Adjusting up recordsProcessedBeforeCommit={}",
                    processLatency, commitTime, totalProcessed, recordsProcessedBeforeCommit);
        }

        return recordsProcessedBeforeCommit;
    }

    /**
     * Commit all tasks owned by this thread if specified interval time has elapsed
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    void maybeCommit(final long now) {
        if (commitTimeMs >= 0 && lastCommitMs + commitTimeMs < now) {
            if (log.isTraceEnabled()) {
                log.trace("Committing all active tasks {} and standby tasks {} since {}ms has elapsed (commit interval is {}ms)",
                        taskManager.activeTaskIds(), taskManager.standbyTaskIds(), now - lastCommitMs, commitTimeMs);
            }

            int committed = taskManager.commitAll();
            if (committed > 0) {
                streamsMetrics.commitTimeSensor.record(computeLatency() / (double) committed, timerStartedMs);

                // try to purge the committed records for repartition topics if possible
                taskManager.maybePurgeCommitedRecords();
            }
            if (log.isDebugEnabled()) {
                log.debug("Committed all active tasks {} and standby tasks {} in {}ms",
                        taskManager.activeTaskIds(), taskManager.standbyTaskIds(), timerStartedMs - now);
            }

            lastCommitMs = now;

            processStandbyRecords = true;
        }
    }

    private void maybeUpdateStandbyTasks(final long now) {
        if (state == State.RUNNING && taskManager.hasStandbyRunningTasks()) {
            if (processStandbyRecords) {
                if (!standbyRecords.isEmpty()) {
                    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> remainingStandbyRecords = new HashMap<>();

                    for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> entry : standbyRecords.entrySet()) {
                        final TopicPartition partition = entry.getKey();
                        List<ConsumerRecord<byte[], byte[]>> remaining = entry.getValue();
                        if (remaining != null) {
                            final StandbyTask task = taskManager.standbyTask(partition);
                            remaining = task.update(partition, remaining);
                            if (remaining != null) {
                                remainingStandbyRecords.put(partition, remaining);
                            } else {
                                restoreConsumer.resume(singleton(partition));
                            }
                        }
                    }

                    standbyRecords = remainingStandbyRecords;

                    log.debug("Updated standby tasks {} in {}ms", taskManager.standbyTaskIds(), time.milliseconds() - now);
                }
                processStandbyRecords = false;
            }

            try {
                final ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(0);

                if (!records.isEmpty()) {
                    for (final TopicPartition partition : records.partitions()) {
                        final StandbyTask task = taskManager.standbyTask(partition);

                        if (task == null) {
                            throw new StreamsException(logPrefix + "Missing standby task for partition " + partition);
                        }

                        final List<ConsumerRecord<byte[], byte[]>> remaining = task.update(partition, records.records(partition));
                        if (remaining != null) {
                            restoreConsumer.pause(singleton(partition));
                            standbyRecords.put(partition, remaining);
                        }
                    }
                }
            } catch (final InvalidOffsetException recoverableException) {
                log.warn("Updating StandbyTasks failed. Deleting StandbyTasks stores to recreate from scratch.", recoverableException);
                final Set<TopicPartition> partitions = recoverableException.partitions();
                for (final TopicPartition partition : partitions) {
                    final StandbyTask task = taskManager.standbyTask(partition);
                    log.info("Reinitializing StandbyTask {}", task);
                    task.reinitializeStateStoresForPartitions(recoverableException.partitions());
                }
                restoreConsumer.seekToBeginning(partitions);
            }
        }
    }

    /**
     * Compute the latency based on the current marked timestamp, and update the marked timestamp
     * with the current system timestamp.
     *
     * @return latency
     */
    private long computeLatency() {
        final long previousTimeMs = timerStartedMs;
        timerStartedMs = time.milliseconds();

        return Math.max(timerStartedMs - previousTimeMs, 0);
    }

    /**
     * Shutdown this stream thread.
     * <p>
     * Note that there is nothing to prevent this function from being called multiple times
     * (e.g., in testing), hence the state is set only the first time
     */
    public void shutdown() {
        log.info("Informed to shut down");
        State oldState = setState(State.PENDING_SHUTDOWN);
        if (oldState == State.CREATED) {
            // The thread may not have been started. Take responsibility for shutting down
            completeShutdown(true);
        }
    }

    private void completeShutdown(final boolean cleanRun) {
        // set the state to pending shutdown first as it may be called due to error;
        // its state may already be PENDING_SHUTDOWN so it will return false but we
        // intentionally do not check the returned flag
        setState(State.PENDING_SHUTDOWN);

        log.info("Shutting down");

        try {
            taskManager.shutdown(cleanRun);
        } catch (final Throwable e) {
            log.error("Failed to close task manager due to the following error:", e);
        }
        try {
            consumer.close();
        } catch (final Throwable e) {
            log.error("Failed to close consumer due to the following error:", e);
        }
        try {
            restoreConsumer.close();
        } catch (final Throwable e) {
            log.error("Failed to close restore consumer due to the following error:", e);
        }
        streamsMetrics.removeAllSensors();

        setState(State.DEAD);
        log.info("Shutdown complete");
    }

    private void clearStandbyRecords() {
        standbyRecords.clear();
    }

    /**
     * Return information about the current {@link StreamThread}.
     *
     * @return {@link ThreadMetadata}.
     */
    public final ThreadMetadata threadMetadata() {
        return threadMetadata;
    }

    private void updateThreadMetadata(final Map<TaskId, StreamTask> activeTasks, final Map<TaskId, StandbyTask> standbyTasks) {
        final Set<TaskMetadata> activeTasksMetadata = new HashSet<>();
        for (Map.Entry<TaskId, StreamTask> task : activeTasks.entrySet()) {
            activeTasksMetadata.add(new TaskMetadata(task.getKey().toString(), task.getValue().partitions()));
        }
        final Set<TaskMetadata> standbyTasksMetadata = new HashSet<>();
        for (Map.Entry<TaskId, StandbyTask> task : standbyTasks.entrySet()) {
            standbyTasksMetadata.add(new TaskMetadata(task.getKey().toString(), task.getValue().partitions()));
        }

        threadMetadata = new ThreadMetadata(this.getName(), this.state().name(), activeTasksMetadata, standbyTasksMetadata);
    }

    public Map<TaskId, StreamTask> tasks() {
        return taskManager.activeTasks();
    }

    /**
     * Produces a string representation containing useful information about a StreamThread.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamThread instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information about a StreamThread, starting with the given indent.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamThread instance.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder()
                .append(indent).append("\tStreamsThread threadId: ").append(getName()).append("\n");

        sb.append(taskManager.toString(indent));
        return sb.toString();
    }

    // this is for testing only
    TaskManager taskManager() {
        return taskManager;
    }
}
