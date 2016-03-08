package io.flow.events

/*
queue.publish[UserCreated](Json.toJson(UserCreated(id)))

// event: Will be of type JsValue
queue.receive[UserCreated] { event =>
  println("received event: " + event.as[UserCreated])
}
*/

import play.api.libs.json.JsValue

trait Queue {
  def publish[T](event: JsValue)
  def receive[T]()(f: JsValue => T)
}
