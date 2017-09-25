package io.flow.event.v2.actors

object PollActorErrors {
  /** Checks whether the first line of an exception message matches a partman partitioning error, which is not critical. */
  def filterExceptionMessage(message: String): Boolean = {
    message.split("\\r?\\n").headOption.exists(_.matches(".*duplicate key value violates unique constraint.*_p\\d{4}_\\d{2}_\\d{2}_pkey.*"))
  }
}
