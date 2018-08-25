/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

/**
  * 定时任务，处理方法是run
  */
trait TimerTask extends Runnable {

  //延迟操作的延迟时长，超时时间戳
  val delayMs: Long // timestamp in millisecond

  //外层的entry
  private[this] var timerTaskEntry: TimerTaskEntry = null

  //取消任务，就是将任务指向的entry设置为null
  def cancel(): Unit = {
    synchronized {
      if (timerTaskEntry != null) timerTaskEntry.remove()
      timerTaskEntry = null
    }
  }

  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      //若果已将有并且不是新设置的那么就删除
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()

      timerTaskEntry = entry
    }
  }

  private[timer] def getTimerTaskEntry(): TimerTaskEntry = {
    timerTaskEntry
  }

}
