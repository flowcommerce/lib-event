package io.flow.event.v2

import io.flow.event.Util
import play.api.libs.json.JsValue

trait KinesisShardProvider[T] {

  /**
    * Provides the kinesis shard to publish an event
    *
    * @param event the event to publish
    * @param js the json representation of the event
    */
  def get(event: T, js: JsValue): String
}

object OrganizationOrEventIdShardProvider extends KinesisShardProvider[Any] {

  def apply[T]: KinesisShardProvider[T] = this.asInstanceOf[KinesisShardProvider[T]]

  override def get(event: Any, js: JsValue): String =
    Util.parseString(js, "organization")
      .orElse(Util.parseString(js, "organization_id"))
      .getOrElse(Util.mustParseString(js, "event_id"))

}
