package io.flow.event.v2.actors

import akka.actor.{Actor, ActorLogging, ActorSystem}
import io.flow.event.Record
import io.flow.event.v2.{MockQueue, Queue}
import io.flow.play.actors.ErrorHandler
import play.api.Logger

import scala.concurrent.ExecutionContext
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
trait PollActorBatch extends Actor with ActorLogging with ErrorHandler {

  /**
    * Called once for every batch read off the stream
    */
  def processBatch(records: Seq[Record]): Unit

  /**
    * Called once for every event read off the stream - if true,
    * we then call process(record). Override this method to
    * filter specific records to process
    */
  def accepts(record: Record): Boolean = true

  def system: ActorSystem

  def queue: Queue

  private[this] implicit var ec: ExecutionContext = _

  private[this] def defaultDuration = {
    queue match {
      case _:  MockQueue => FiniteDuration(20, MILLISECONDS)
      case _ => FiniteDuration(5, SECONDS)
    }
  }

  def start[T: TypeTag](
                         executionContextName: String,
                         pollTime: FiniteDuration = defaultDuration
                       ) {
    val ec = system.dispatchers.lookup(executionContextName)
    startWithExecutionContext(ec, pollTime)
  }

  def startWithExecutionContext[T: TypeTag](
                                             executionContext: ExecutionContext,
                                             pollTime: FiniteDuration = FiniteDuration(5, SECONDS)
                                           ) {
    Logger.info(s"[${getClass.getName}] Scheduling poll every $pollTime")

    this.ec = executionContext

    queue.consume[T](
      pollTime = pollTime,
      f = processWithErrorHandler
    )
  }

  override def receive: Receive = {
    case msg: Any => logUnhandledMessage(msg)
  }

  private def processWithErrorHandler(records: Seq[Record]): Unit = {
    Try {
      val filteredRecords = records.filter(accepts)
      if (filteredRecords.nonEmpty)
        processBatch(filteredRecords)
    } match {
      case Success(res) => // no-op
      case Failure(ex) => {
        ex.printStackTrace(System.err)

        // explicitly catch and only warn on duplicate key value constraint errors on partitioned tables
        // which is a work around to on conflict not working for child partition tables
        if (PollActorErrors.filterExceptionMessage(ex.getMessage)) {
          Logger.warn(s"[${this.getClass.getName}] FlowEventWarning Error processing record: ${ex.getMessage}")
        } else {
          val msg = s"[${this.getClass.getName}] FlowEventError Error processing record: ${ex.getMessage}"
          Logger.error(msg)
          throw new RuntimeException(msg, ex)
        }
      }
    }
  }
}