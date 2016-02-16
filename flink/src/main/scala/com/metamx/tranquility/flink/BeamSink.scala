/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.metamx.tranquility.flink

import com.metamx.common.scala.Logging
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.twitter.util.Return
import com.twitter.util.Throw
import java.util.concurrent.atomic.AtomicReference
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * This class provides a sink that can propagate any event type to Druid.
  *
  * @param beamFactory your implementation of [[BeamFactory]].
  */
class BeamSink[T](beamFactory: BeamFactory[T])
  extends RichSinkFunction[T] with Logging
{
  val sender = new AtomicReference[Tranquilizer[T]]()
  val total = new LongCounter()
  val accepted = new LongCounter()
  val dropped = new LongCounter()
  val exceptions = new LongCounter()

  override def open(parameters: Configuration) = {
    getRuntimeContext.addAccumulator("Druid: Total", total)
    getRuntimeContext.addAccumulator("Druid: Accepted", accepted)
    getRuntimeContext.addAccumulator("Druid: Dropped", dropped)
    getRuntimeContext.addAccumulator("Druid: Exceptions", exceptions)
    sender.set(beamFactory.tranquilizer)
    sender.get.start()
  }

  override def invoke(value: T) = {
    total.add(1)
    sender.get.send(value) respond {
      case Return(()) => accepted.add(1)
      case Throw(e: MessageDroppedException) =>
        log.warn(e, "Message was dropped by druid")
        dropped.add(1)
      case Throw(e) =>
        exceptions.add(1)
        log.error(e, "Failed to send message to Druid.")
    }
  }

  override def close() = sender.get.stop()
}
