package io.flow.event.actors


import akka.actor.{Actor, ActorLogging, ActorSystem}
import io.flow.event.actors.PollActor.Messages.Poll
import io.flow.event.{Queue, Record}
import io.flow.play.actors.ErrorHandler

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object PollActor {
  object Messages {
    case object Poll
  }
}

trait PollActor extends Actor with ActorLogging with ErrorHandler {

  def pollTimeSeconds: Int = 5

  def executionContext: ExecutionContext

  private[this] implicit val ec = executionContext

  def system: ActorSystem

  def queue: Queue

  def stream: io.flow.event.Stream

  def process(record: Record)

  def receive = akka.event.LoggingReceive {

    case msg @ PollActor.Messages.Poll => withErrorHandler(msg) {
      stream.consume { record =>
        process(record)
      }
    }

    case msg: Any => logUnhandledMessage(msg)

  }

  system.scheduler.schedule(
    FiniteDuration(pollTimeSeconds, SECONDS),
    FiniteDuration(pollTimeSeconds, SECONDS),
    self,
    Poll
  )

}
