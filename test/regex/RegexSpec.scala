package io.flow.event


import org.scalatestplus.play.{PlaySpec, OneAppPerSuite}

class RegexSpec extends PlaySpec with OneAppPerSuite {

  "match on a partition table name" in {
    val msg = "blah blah duplicate key value violates unique constraint \"inventory_event_p2017_01_12_pkey blah blah"

    msg.matches(".*duplicate key value violates unique constraint.*_p\\d{4}_\\d{2}_\\d{2}_pkey.*") must equal(true)



    val msg2 = "blah blah duplicate key value violates unique constraint \"cool_posts_pkey blah blah"

    msg2.matches(".*duplicate key value violates unique constraint.*_p\\d{4}_\\d{2}_\\d{2}_pkey.*") must equal(false)
  }

}
