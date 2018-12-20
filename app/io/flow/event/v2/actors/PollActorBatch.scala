package io.flow.event.v2.actors

import akka.actor.{Actor, ActorLogging, ActorSystem}
import com.github.ghik.silencer.silent
import io.flow.akka.SafeReceive
import io.flow.event.Record
import io.flow.event.v2.{MockQueue, Queue}
import io.flow.log.RollbarLogger

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, SECONDS}
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}

/**
  * [[PollActorBatch]] periodically polls a kinesis stream invoking process once per message batch
  *
  * To extend this class:
  *   - implement required methods
  *   - call start(...) w/ the name of the execution context to use
  */
trait PollActorBatch extends Actor with ActorLogging {

  /**
    * Called once for every batch read off the stream
    */
  def processBatch(records: Seq[Record]): Unit

  /**
    * Called once for every event read off the stream - if true,
    * we then call process(record). Override this method to
    * filter specific records to process
    */
  @silent def accepts(record: Record): Boolean = true

  /**
    * Allows the retrieved Seq[Record] to be transformed (filtered, modified, ...) before being processed
    * This function is called right after [[accepts]] and offers more flexibility
    */
  def transform(records: Seq[Record]): Seq[Record] = records

  def system: ActorSystem

  def queue: Queue

  def logger: RollbarLogger

  private implicit lazy val configuredRollbar: RollbarLogger = logger.fingerprint("PollActorBatch").withKeyValue("class", getClass.getName)

  private[this] def defaultDuration = {
    queue match {
      case _:  MockQueue => FiniteDuration(20, MILLISECONDS)
      case _ => FiniteDuration(5, SECONDS)
    }
  }

  def start[T: TypeTag](pollTime: FiniteDuration = defaultDuration): Unit = {
    logger
      .withKeyValue("class", getClass.getName)
      .withKeyValue("poll_time", pollTime.toSeconds)
      .info("Scheduling")

    queue.consume[T](
      pollTime = pollTime,
      f = processWithErrorHandler
    )
  }

  override def receive: Receive = SafeReceive(PartialFunction.empty)

  private def processWithErrorHandler(records: Seq[Record]): Unit = {
    Try {
      val transformedRecords = transform(records.filter(accepts))
      if (transformedRecords.nonEmpty)
        processBatch(transformedRecords)
    } match {
      case Success(_) => // no-op
      case Failure(ex) => {
        ex.printStackTrace(System.err)

        // explicitly catch and only warn on duplicate key value constraint errors on partitioned tables
        // which is a work around to on conflict not working for child partition tables
        if (PollActorErrors.filterExceptionMessage(ex.getMessage)) {
          logger
            .withKeyValue("class", this.getClass.getName)
            .warn(s"FlowEventWarning Error processing record: ${ex.getMessage}")
        } else {
          val msg = s"FlowEventError Error processing record: ${ex.getMessage}"
          logger
            .withKeyValue("class", this.getClass.getName)
            .error(msg)
          throw new RuntimeException(msg, ex)
        }
      }
    }
  }
}