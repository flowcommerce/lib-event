package io.flow.event


import org.scalatestplus.play.{PlaySpec, OneAppPerSuite}
import io.flow.event.actors.PollActor

class RegexSpec extends PlaySpec with OneAppPerSuite {

  def process(record: io.flow.event.Record): Unit = ???
  def queue: io.flow.event.Queue = ???
  def system: akka.actor.ActorSystem = ???

  "match on a partition table name" in {
    val msg = "blah blah duplicate key value violates unique constraint \"inventory_event_p2017_01_12_pkey blah blah"

    PollActor.filterExceptionMessage(msg) must equal(true)

    val msg2 = """ERROR: duplicate key value violates unique constraint "inventory_event_p2017_01_13_pkey"
      Detail: Key (organization_id, number)=(tst-b222abcb7e4c415c9b8f989b97d397e6, tst-c57e02745852400fa43d279a9b765043) already exists.
    """

    PollActor.filterExceptionMessage(msg2) must equal(true)

    val msg3 = "blah blah duplicate key value violates unique constraint \"cool_posts_pkey blah blah"

    PollActor.filterExceptionMessage(msg3) must equal(false)
  }

}
