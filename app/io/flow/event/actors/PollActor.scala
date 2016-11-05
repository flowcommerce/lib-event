package io.flow.event.actors

import akka.actor.{Actor, ActorLogging, ActorSystem}
import io.flow.event.{Queue, Record}
import io.flow.play.actors.ErrorHandler
import play.api.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

/**
  * Poll Actor periodicaly polls a kinesis stream (by default every 5
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

  def start[T: TypeTag](
    executionContextName: String,
    pollTime: FiniteDuration = FiniteDuration(5, SECONDS)
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
    this.stream = Some(queue.stream[T])

    system.scheduler.schedule(pollTime, pollTime, self, Poll)
  }

  private[this] var stream: Option[io.flow.event.Stream] = None
  private[this] case object Poll

  def receive = akka.event.LoggingReceive {

    case msg @ Poll => withErrorHandler(msg) {
      stream match {
        case None => {
          sys.error("Must call start before polling")
        }

        case Some(s) => {
          s.consume { record =>
            process(record)
          }
        }
      }
    }

    case msg: Any => logUnhandledMessage(msg)

  }

}
