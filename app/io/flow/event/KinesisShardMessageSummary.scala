package io.flow.event

case class KinesisShardMessageSummary(messages: Seq[String], nextShardIterator: Option[String])
