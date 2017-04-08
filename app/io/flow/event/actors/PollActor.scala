package io.flow.event.actors

import akka.actor.{Actor, ActorLogging, ActorSystem}
import io.flow.event.{MockQueue, Record}
import io.flow.event.v2.Queue
import io.flow.play.actors.ErrorHandler
import play.api.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}

/**
  * Poll Actor periodically polls a kinesis stream (by default every 5
  * seconds), invoking process once per message.
  * 
  * To extend this class:
  *   - implement system, queue, process(record)
  *   - call start(...) w/ the name of the execution context to use
  */
trait PollActor extends Actor with ActorLogging with ErrorHandler {

  /**
    * Called once for every event read off the stream
    */
  def process(record: Record)

  def system: ActorSystem

  def queue: Queue

  private[this] implicit var ec: ExecutionContext = null

  private[this] def defaultDuration = {
    queue match {
      case _:  MockQueue => FiniteDuration(10, MILLISECONDS)
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
    this.consumer = Some(queue.consumer[T])

    system.scheduler.schedule(pollTime, pollTime, self, Poll)
  }

  private[this] var consumer: Option[io.flow.event.v2.Consumer] = None
  private[this] case object Poll

  def receive = akka.event.LoggingReceive {

    case msg @ Poll => withErrorHandler(msg) {
      consumer.getOrElse {
        sys.error(s"[${this.getClass.getName}] Must call start before polling")
      }.consume { record =>
        Try {
          process(record)
        } match {
          case Success(_) => // no-op
          case Failure(ex) => {
            ex.printStackTrace(System.err)

            // explicitly catch and only warn on duplicate key value constraint errors on partitioned tables
            if (PollActor.filterExceptionMessage(ex.getMessage)) {
              Logger.warn(s"[${self.getClass.getName}] FlowEventWarning Error processing record: ${ex.getMessage}", ex)
            } else {
              Logger.error(s"[${self.getClass.getName}] FlowEventError Error processing record: ${ex.getMessage}", ex)
            }
          }
        }
      }
    }

    case msg: Any => logUnhandledMessage(msg)

  }
}

object PollActor {
  /** Checks whether the first line of an exception message matches a partman partitioning error, which is not critical. */
  def filterExceptionMessage(message: String): Boolean = {
    message.split("\\r?\\n").headOption.exists(_.matches(".*duplicate key value violates unique constraint.*_p\\d{4}_\\d{2}_\\d{2}_pkey.*"))
  }
}
