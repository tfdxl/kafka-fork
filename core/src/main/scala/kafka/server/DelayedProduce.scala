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

package kafka.server


import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

/**
  * 生产的partition的状态
  *
  * @param requiredOffset
  * @param responseStatus
  */
case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, errorCode: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.error.code, responseStatus.baseOffset, requiredOffset)
}

/**
  * The produce metadata maintained by the delayed produce operation
  */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString: String = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceRequiredAcks, produceStatus)
}

/**
  * A delayed produce operation that can be created by the replica manager and watched
  * in the produce operation purgatory
  */
class DelayedProduce(delayMs: Long, //延迟的时长
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                     lockOpt: Option[Lock] = None)
  extends DelayedOperation(delayMs, lockOpt) {

  //根据前面写入消息的返回的结果，设置ProducePartitionStatus的ackPending的字段
  // first update the acks pending variable according to the error code
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    //对应分区的写入操作完成，等待isr集合中的副本完成同步
    //如果写入操作出现了异常，那么该分区不需要等待
    if (status.responseStatus.error == Errors.NONE) {
      // Timeout error state will be cleared when required acks are received
      status.acksPending = true

      //这个错误码是预先设置的，我就说嘛，还么有等待哪里来的错误呢，如果isr集合中的副本在此
      //请求超时之前顺利的完成了同步，那么会清除这个错误码，那么就使用这个错误码
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      //如果在追加日志的时候已经抛出异常了，那么就不用在z这个partition上进行等待了
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  /**
    * 检测是否满足DelayedProduce的执行条件
    * The delayed produce operation can be completed if every partition
    * it produces to is satisfied by one of the following:
    *
    * Case A: This broker is no longer the leader: set an error in response
    * Case B: This broker is the leader:
    *   B.1 - If there was a local error thrown while checking if at least requiredAcks
    * replicas have caught up to this operation: set an error in response
    *   B.2 - Otherwise, set the response with no error.
    */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    //遍历[TopicPartition,ProducePartitionStatus]
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // skip those partitions that have already been satisfied
      //是否等待ISR集合中其他的副本与Leader副本同步requiredOffset之前的消息
      if (status.acksPending) {

        //定义了两个变量
        val (hasEnough, error) = replicaManager.getPartition(topicPartition) match {
          case Some(partition) =>
            if (partition eq ReplicaManager.OfflinePartition)
              (false, Errors.KAFKA_STORAGE_ERROR)
            else
              partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            // Case A
            //该分区出现了Leader副本的迁移
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
        // Case B.1 || B.2
        //此分区的leader副本hw大于对应的requiredOffset
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }

    //检查全部的分区是否都已经符合了DelayedProduce的条件
    // check if every partition has satisfied at least one of case A or B
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        //记录超时
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
    * Upon completion, return the current response status along with the error code per partition
    */
  override def onComplete() {
    //根据ProduceMetadata记录的相关信息，为每一个Partition产生响应状态
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    responseCallback(responseStatus)
  }
}

//就是DelayedProduce的监控的一些辅助方法
object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
      "requests",
      TimeUnit.SECONDS,
      tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

