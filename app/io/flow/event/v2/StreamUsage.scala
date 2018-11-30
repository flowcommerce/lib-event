package io.flow.event.v2

import io.flow.event.Record
import io.flow.util.StreamNames
import play.api.libs.json.JsValue

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.reflect.runtime.universe._

/**
  * This trait allows a dump of what producers/consumers are actually being used within a service.
  *
  * Note that while consumers are generally set up at application start up [via DI], producers may be started on the
  * fly. Thus the model of how producers are used may expand over the life of a service instance, for example giving a
  * different answer after an hour's usage than at application startup.
  *
  */
trait StreamUsage {
  import io.flow.event.v2.StreamUsage._

  protected def markProducesStream(streamName:String, eventClass: Type): Unit = {
    val usage = usageMap.getOrElseUpdate(streamName, StreamUsed(streamName, eventClass))
    if (!usage.produced) {
      usageMap.put(streamName, usage.copy(produced = true))
    }
  }

  protected def markConsumesStream(streamName: String, eventClass: Type): Unit = {
    val usage = usageMap.getOrElseUpdate(streamName, StreamUsed(streamName, eventClass))
    if (!usage.consumed) {
      usageMap.put(streamName, usage.copy(consumed = true))
    }
  }

  protected def markAcceptedEvent(streamName: String, r: Record): Unit = {
    r.discriminator.foreach{ d=>
      usageMap.get(streamName).foreach{ usage =>
        if (!usage.eventsAccepted.contains(d)){
          usageMap.put(streamName, usage.copy(eventsAccepted = usage.eventsAccepted + d))
        }
      }
    }
  }

  protected def markProducedEvent(streamName: String, e: JsValue): Unit = {
    (e \ "discriminator")
      .asOpt[String] .foreach { d =>
      usageMap.get(streamName).foreach { usage =>
        if (!usage.eventsProduced.contains(d)) {
          usageMap.put(streamName, usage.copy(eventsProduced = usage.eventsProduced + d))
        }
      }
    }
  }
}

object StreamUsage {
  protected val usageMap: TrieMap[String, StreamUsed] = concurrent.TrieMap[String, StreamUsed]()

  def allStreamsUsed: Seq[StreamUsed] = usageMap.values.toSeq.sortBy(_.streamName)

  def writtenStreams: Seq[StreamUsed] = usageMap.values.filter(_.produced).toSeq.sortBy(_.streamName)

  def readStreams: Seq[StreamUsed] = usageMap.values.filter(_.consumed).toSeq.sortBy(_.streamName)
}

case class StreamUsed (
  streamName: String,
  eventClass: Type,
  serviceName: String,
  specName: String,
  consumed: Boolean = false,
  produced: Boolean = false,
  eventsAccepted : Set[String] = Set.empty,
  eventsProduced : Set[String] = Set.empty
)
object StreamUsed {
  def apply(streamName: String, eventClass: Type): StreamUsed = {
    val (serviceName, specName) = StreamNames.parse(eventClass.toString) match {
      case Some(apid) => (apid.service, apid.name)
      case None => ("", "")
    }
    new StreamUsed(streamName,
      eventClass,
      serviceName = serviceName,
      specName = specName
    )
  }
}