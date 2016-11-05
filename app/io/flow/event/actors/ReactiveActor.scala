package io.flow.event.actors

import akka.actor.{Actor, ActorLogging, ActorSystem}
import io.flow.event.actors.ReactiveActor.Messages.{Ping, Poll}
import io.flow.play.actors.ErrorHandler
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


object ReactiveActor {
  object Messages {
    case object Changed
    case object Ping
    case object Poll
  }
}

trait ReactiveActor extends Actor with ActorLogging with ErrorHandler {

  def pollTimeSeconds: Int = 60

  private[this] var nextProcess: Option[DateTime] = None

  def quietTimeMs = 500

  def executionContext: ExecutionContext

  private[this] implicit val ec = executionContext

  def system: ActorSystem

  override def receive = akka.event.LoggingReceive {

    case msg @ ReactiveActor.Messages.Changed => withErrorHandler(msg) {
      setNextProcess()
    }

    case msg @ ReactiveActor.Messages.Poll => withErrorHandler(msg) {
      setNextProcess()
    }

    case msg @ Ping => withErrorHandler(msg) {
      nextProcess.foreach { ts =>
        if (ts.isBeforeNow) {
          process()
          nextProcess = None
        }
      }
    }

    case msg: Any => logUnhandledMessage(msg)

  }

  def process()

  system.scheduler.schedule(
    FiniteDuration(pollTimeSeconds, SECONDS),
    FiniteDuration(pollTimeSeconds, SECONDS),
    self,
    Poll
  )

  system.scheduler.schedule(
    FiniteDuration(250, MILLISECONDS),
    FiniteDuration(250, MILLISECONDS),
    self,
    Ping
  )

  def setNextProcess() {
    if (nextProcess.isEmpty) {
      nextProcess = Some((new DateTime()).plusMillis(quietTimeMs))
    }
  }

}
