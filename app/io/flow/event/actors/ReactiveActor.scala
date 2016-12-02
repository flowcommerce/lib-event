package io.flow.event.actors

import akka.actor.{Actor, ActorLogging, ActorSystem}
import io.flow.play.actors.ErrorHandler
import play.api.Logger
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object ReactiveActor {
  object Messages {
    case object Changed
  }
}

/**
  * Reactive Actor responds to change events, invoking process no more
  * than once per quiet period. This allows an actor to receive an
  * event on every change (even high volume), invoking process quickly
  * but periodically (e.g. 2x / second rather than after every event).
  * 
  * A poll interval ensures that process is called at least once every
  * 60 seconds (the default)
  * 
  * To extend this class:
  *   - implement system, process()
  *   - call start(...) w/ the name of the execution context to use
  *   - Send a ReactiveActor.Messages.Changed message on every change,
  *     or just rely on Poll
  */
trait ReactiveActor extends Actor with ActorLogging with ErrorHandler {

  def system: ActorSystem

  def process()

  def quietTimeMs = 500

  def start(
    executionContextName: String,
    pollTime: FiniteDuration = FiniteDuration(60, SECONDS),
    pingTime: FiniteDuration = FiniteDuration(100, MILLISECONDS)
  ) {
    implicit val ec = system.dispatchers.lookup(executionContextName)
    startWithExecutionContext(pollTime, pingTime)
  }

  def startWithExecutionContext(
    pollTime: FiniteDuration = FiniteDuration(60, SECONDS),
    pingTime: FiniteDuration = FiniteDuration(100, MILLISECONDS)
  ) (
    implicit executionContext: ExecutionContext
  ) {
    Logger.info(s"[${getClass.getName}] Scheduling poll every $pollTime, ping every $pingTime")
    system.scheduler.schedule(pollTime, pollTime, self, Poll)
    system.scheduler.schedule(pingTime, pingTime, self, Ping)
  }

  private[this] var nextProcess: Option[DateTime] = None
  private[this] case object Ping
  private[this] case object Poll
  
  override def receive = akka.event.LoggingReceive {

    case msg @ ReactiveActor.Messages.Changed => withErrorHandler(msg) {
      setNextProcess()
    }

    case msg @ Poll => withErrorHandler(msg) {
      setNextProcess()
    }

    case msg @ Ping => withErrorHandler(msg) {
      nextProcess.foreach { ts =>
        if (ts.isBeforeNow) {
          nextProcess = None
          Try {
            process()
          } match {
            case Success(_) => // no-op
            case Failure(ex) => {
              Logger.error(s"[${getClass.getName}] FlowEventError Error processing batch: ${ex.getMessage}", ex)
            }
          }
        }
      }
    }

    case msg: Any => logUnhandledMessage(msg)

  }

  def setNextProcess() {
    if (nextProcess.isEmpty) {
      nextProcess = Some((DateTime.now).plusMillis(quietTimeMs))
    }
  }

}
