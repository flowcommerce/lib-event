package io.flow.event

trait SequenceNumberProvider {

  /**
    * Returns the current sequence number for the specific stream and shard.
    * Returns None to indicate we are at the start of the stream.
    */
  def current(streamName: String, shardId: String): Option[String]

  /**
    * Responsible for recording the latest sequence number
    */
  def snapshot(streamName: String, shardId: String, sequenceNumber: String)
}





















