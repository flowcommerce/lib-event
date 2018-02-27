package io.flow.event.v2

import io.flow.event.{StreamNames, Util}

import scala.collection.concurrent
import scala.reflect.runtime.universe._

/**
  * This trait allows a dump of what producers/consumers are actually being used within a service.
  *
  * Note that while consumers are generally set up at application start up [via DI], producers may be started on the
  * fly. Thus the model of how producers are used may expand over the life of a service instance, for example giving a
  * different answer after an hour's usage
  *
  */
trait StreamUsage {
  import io.flow.event.v2.StreamUsage._

  protected def markProduced[T:TypeTag](): Unit = {
    StreamNames.fromType[T] match {
      case Left(errors) => sys.error(errors.mkString(", "))
      case Right(name) => {
        val usage = usageMap.getOrElse(name, StreamUsed(name, typeOf[T]))
        println(name)
        usageMap.put(name, usage.copy(produced = true))
      }
    }
  }

  protected def markConsumed[T:TypeTag](): Unit = {
    StreamNames.fromType[T] match {
      case Left(errors) => sys.error(errors.mkString(", "))
      case Right(name) => {
        val usage = usageMap.getOrElse(name, StreamUsed(name, typeOf[T]))
        println(name)
        usageMap.put(name, usage.copy(consumed = true))
      }
    }
  }
}

object StreamUsage {
  protected val usageMap = concurrent.TrieMap[String, StreamUsed]()

  def allStreamsUsed: Seq[StreamUsed] = usageMap.values.toSeq.sortBy(_.streamName)

  def writtenStreams: Seq[StreamUsed] = usageMap.values.filter(_.produced).toSeq.sortBy(_.streamName)

  def readStreams: Seq[StreamUsed] = usageMap.values.filter(_.consumed).toSeq.sortBy(_.streamName)
}

case class StreamUsed (
  streamName: String,
  eventClass: Type,
  consumed: Boolean = false,
  produced: Boolean = false
)