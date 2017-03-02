package io.flow.event.actors

import akka.actor.{Actor, ActorLogging, ActorSystem}
import io.flow.event._
import io.flow.play.actors.ErrorHandler
import org.joda.time.DateTime
import play.api.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

/**
  * Poll Actor periodically polls a kinesis stream (by default every 5
  * seconds), invoking process once per message.
  * 
  * To extend this class:
  *   - implement system, queue, process(record)
  *   - call start(...) w/ the name of the execution context to use
  */
trait PollActor extends Actor with ActorLogging with ErrorHandler {

  /**
    * Called once for every event read off the stream
    */
  def process(record: Record)

  def system: ActorSystem

  def queue: Queue

  def sequenceNumberProvider: SequenceNumberProvider

  private[this] implicit var ec: ExecutionContext = null

  private[this] def defaultDuration = {
    queue match {
      case q:  MockQueue => FiniteDuration(10, MILLISECONDS)
      case _ => FiniteDuration(5, SECONDS)
    }
  }

  private[this] def defaultRecordSnapshotDuration = FiniteDuration(60, SECONDS)

  def start[T: TypeTag](
    executionContextName: String,
    pollTime: FiniteDuration = defaultDuration,
    recordSnapshotTime: FiniteDuration = defaultRecordSnapshotDuration
  ) {
    val ec = system.dispatchers.lookup(executionContextName)
    startWithExecutionContext(ec, pollTime, recordSnapshotTime)
  }

  def startWithExecutionContext[T: TypeTag](
    executionContext: ExecutionContext,
    pollTime: FiniteDuration = FiniteDuration(5, SECONDS),
    recordSnapshotTime: FiniteDuration = defaultRecordSnapshotDuration
  ) {
    Logger.info(s"[${getClass.getName}] Scheduling poll every $pollTime")

    this.ec = executionContext
    this.stream = Some(queue.stream[T](sequenceNumberProvider))

    system.scheduler.schedule(pollTime, pollTime, self, Poll)

    /**
      * schedule actor message to record snapshots
      */
    system.scheduler.schedule(recordSnapshotTime, recordSnapshotTime, self, RecordSnapshots)
  }

  private[this] var stream: Option[io.flow.event.Stream] = None

  private[this] case object Poll
  private[this] case object RecordSnapshots

  def receive = akka.event.LoggingReceive {

    case msg @ Poll => withErrorHandler(msg) {
      stream match {
        case None => {
          sys.error("Must call start before polling")
        }

        case Some(s) => {
          s.consume { record =>

            val snapshotsHelper = LocalSnapshotManager(record.streamName, record.shardId)
            if (snapshotsHelper.latestEventTimeReceived.isEmpty) {
              snapshotsHelper.putLatestEventTimeReceived(Some(DateTime.now))
            }

            Try {
              process(record)
            } match {
              case Success(_) => setCurrentSnapshot(record)
              case Failure(ex) => {
                ex.printStackTrace(System.err)

                // explicitly catch and only warn on duplicate key value constraint errors on partitioned tables
                PollActor.filterExceptionMessage(ex.getMessage) match {
                  case false =>  Logger.error(s"[${self.getClass.getName}] FlowEventError Error processing record: ${ex.getMessage}", ex)
                  case true => Logger.warn(s"[${self.getClass.getName}] FlowEventWarning Error processing record: ${ex.getMessage}", ex)
                }
              }
            }
          }
        }
      }
    }

    /**
      * Cycle through the timestamps for last received event in each stream name/shard,
      * if an event was received within the scheduled cycle, record the snapshot.
      */
    case msg @ RecordSnapshots => withErrorHandler(msg) {
      LocalSnapshotManager.latestEventTimeReceivedMap.foreach { elem =>
        val snapshotHelper = LocalSnapshotManager.snapshotNameAndShardId(elem._1)
        val timestamp = elem._2

        timestamp.foreach { recordedTime =>

          // check if time to record snapshot
          if (recordedTime.isBeforeNow) {
            snapshotHelper.latestSnapshot.foreach { snapshot =>
              sequenceNumberProvider.snapshot(
                streamName = snapshot.streamName,
                shardId = snapshot.shardId,
                sequenceNumber = snapshot.sequenceNumber
              )
            }

            // reset last event time received
            snapshotHelper.putLatestEventTimeReceived(None)
          }
        }
      }
    }

    case msg: Any => logUnhandledMessage(msg)

  }

  /**
    *  Store latest snapshot on consumed event in memory
    */
  def setCurrentSnapshot(record: Record) {
    val latestSnapshot =
      Snapshot(
        streamName = record.streamName,
        shardId = record.shardId,
        sequenceNumber = record.sequenceNumber
      )

    LocalSnapshotManager(record.streamName, record.shardId).putSnapshot(latestSnapshot)

  }
}

object PollActor {
  /** Checks whether the first line of an exception message matches a partman partitioning error, which is not critical. */
  def filterExceptionMessage(message: String): Boolean = {
    message.split("\\r?\\n").headOption.exists(_.matches(".*duplicate key value violates unique constraint.*_p\\d{4}_\\d{2}_\\d{2}_pkey.*"))
  }
}
