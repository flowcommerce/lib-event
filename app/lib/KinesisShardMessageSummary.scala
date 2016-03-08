package io.flow.events

case class KinesisShardMessageSummary(messages: Seq[String], nextShardIterator: Option[String])
