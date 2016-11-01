package io.flow.event

case class KinesisShardMessageSummary(messages: Seq[Message], nextShardIterator: Option[String])
