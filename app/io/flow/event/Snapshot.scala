package io.flow.event


case class Snapshot (
  streamName: String,
  shardId: String,
  sequenceNumber: String
)
