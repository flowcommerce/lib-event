package io.flow.event

import org.joda.time.DateTime


case class Snapshot (
  streamName: String,
  shardId: String,
  sequenceNumber: String
)

case class LocalSnapshotManager(
  streamName: String,
  shardId: String
) {

  def key = s"$streamName:$shardId"

  /**
    * Helpers to maintain in memory copy of latest stream snapshot, inclusive of name, shard id, and sequence number
    */
  def latestSnapshot: Option[Snapshot] = LocalSnapshotManager.latestSnapshotsMap.get(key)
  def putSnapshot(snapshot: Snapshot) {
    LocalSnapshotManager.latestSnapshotsMap += (key -> snapshot)
  }

  /**
    * Helpers to main timestamps of most recent event consumed from a unique stream name/shard id
    */
  def latestEventTimeReceived: Option[DateTime] = LocalSnapshotManager.latestEventTimeReceivedMap.get(key).flatten
  def putLatestEventTimeReceived(optionalTimestamp: Option[DateTime]) {
    LocalSnapshotManager.latestEventTimeReceivedMap += (key -> optionalTimestamp)
  }
}

object LocalSnapshotManager {
  val latestSnapshotsMap = scala.collection.mutable.Map.empty[String, Snapshot]
  val latestEventTimeReceivedMap = scala.collection.mutable.Map.empty[String, Option[DateTime]]

  def snapshotNameAndShardId(key: String): LocalSnapshotManager = {
    val parsedKey = key.split(":")

    LocalSnapshotManager(
      streamName = parsedKey.headOption.getOrElse { sys.error(s"Invalid snapshot key [$key]") },
      shardId = parsedKey.reverse.headOption.getOrElse { sys.error(s"Invalid snapshot key [$key]") }
    )
  }
}

