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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Abstract assignor implementation which does some common grunt work (in particular collecting
 * partition counts which are always needed in assignors).
 */
public abstract class AbstractPartitionAssignor implements PartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);

    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.get(key);
        if (list == null) {
            list = new ArrayList<>();
            map.put(key, list);
        }
        list.add(value);
    }

    protected static List<TopicPartition> partitions(String topic, int numPartitions) {
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new TopicPartition(topic, i));
        }
        return partitions;
    }

    public static void main(String[] args) {
        List<TopicPartition> topicPartitions = partitions("topic", 8);
        System.err.println(topicPartitions);
    }

    /**
     * Perform the group assignment given the partition counts and member subscriptions
     *
     * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in metadata will be excluded
     *                           from this map.
     * @param subscriptions      Map from the memberId to their respective topic subscription
     * @return Map from each member to the list of partitions assigned to them.
     */
    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, Subscription> subscriptions);

    @Override
    public Subscription subscription(Set<String> topics) {
        return new Subscription(new ArrayList<>(topics));
    }

    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {

        //所有订阅的topic
        Set<String> allSubscribedTopics = new HashSet<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
            allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());

        //topic --> partition count
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null && numPartitions > 0)
                partitionsPerTopic.put(topic, numPartitions);
            else
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }

        //将分区分配的逻辑委托给assign重载。交给子类去实现
        Map<String, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, subscriptions);

        // this class maintains no user data, so just wrap the results
        //整理分配结果
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        return assignments;
    }

    @Override
    public void onAssignment(Assignment assignment) {
        // this assignor maintains no internal state, so nothing to do
    }
}
