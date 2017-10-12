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
    private[ReactiveActor] case object Ping
    private[ReactiveActor] case object Poll
    private[ReactiveActor] case object ProcessNow
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

  import ReactiveActor.Messages._

  def system: ActorSystem

  def process()

  def quietTimeMs = 500

  def pollTime: FiniteDuration = FiniteDuration(60, SECONDS)

  def pingTime: FiniteDuration = FiniteDuration(100, MILLISECONDS)

  def start(
    executionContextName: String,
    pollTime: FiniteDuration = pollTime,
    pingTime: FiniteDuration = pingTime
  ) {
    implicit val ec = system.dispatchers.lookup(executionContextName)
    startWithExecutionContext(pollTime, pingTime)
  }

  def startWithExecutionContext(
    pollTime: FiniteDuration = pollTime,
    pingTime: FiniteDuration = pingTime
  ) (
    implicit executionContext: ExecutionContext
  ) {
    Logger.info(s"[${getClass.getName}] Scheduling poll every $pollTime, ping every $pingTime")
    system.scheduler.schedule(pollTime, pollTime, self, Poll)
    system.scheduler.schedule(pingTime, pingTime, self, Ping)
  }

  private[this] var nextProcess: Option[DateTime] = None

  override def receive = akka.event.LoggingReceive {

    case msg @ Changed => withErrorHandler(msg) {
      setNextProcess()
    }

    case msg @ Poll => withErrorHandler(msg) {
      setNextProcess()
    }

    case msg @ Ping => withErrorHandler(msg) {
      nextProcess.foreach { ts =>
        if (ts.isBeforeNow) {
          nextProcess = None
          doProcess()
        }
      }
    }

    case msg @ ProcessNow => withErrorHandler(msg) {
      doProcess()
    }

    case msg: Any => logUnhandledMessage(msg)

  }

  private def doProcess() = {
    Try {
      process()
    } match {
      case Success(_) => // no-op
      case Failure(ex) => {
        ex.printStackTrace(System.err)
        Logger.error(s"[${getClass.getName}] FlowEventError Error processing batch: ${ex.getMessage}", ex)
      }
    }
  }

  protected final def processNow() = self ! ProcessNow

  def setNextProcess() {
    if (nextProcess.isEmpty) {
      nextProcess = Some(DateTime.now.plusMillis(quietTimeMs))
    }
  }

}
