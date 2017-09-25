package io.flow.event.actors.v2

import io.flow.event.Record
import io.flow.event.v2.actors.PollActorBatch

import scala.util.Try

/**
  * Poll Actor periodically polls a kinesis stream (by default every 5
  * seconds), invoking process once per message.
  *
  * To extend this class:
  *   - implement system, queue, process(record)
  *   - call start(...) w/ the name of the execution context to use
  */
trait PollActor extends PollActorBatch {

  override def processBatch(records: Seq[Record]): Unit = {
    records.map(r => Try(process(r)))
  }

  def process(record: Record): Unit

}