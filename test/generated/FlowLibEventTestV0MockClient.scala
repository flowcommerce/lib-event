/**
 * Generated by API Builder - https://www.apibuilder.io
 * Service version: 0.0.1
 * apibuilder 0.14.75 app.apibuilder.io/flow/lib-event-test/0.0.1/play_2_4_mock_client
 */
package io.flow.lib.event.test.v0.mock {

  object Factories {

    def randomString(): String = {
      "Test " + _root_.java.util.UUID.randomUUID.toString.replaceAll("-", " ")
    }

    def makeTestObject(): io.flow.lib.event.test.v0.models.TestObject = io.flow.lib.event.test.v0.models.TestObject(
      id = Factories.randomString()
    )

    def makeTestObjectDeleted(): io.flow.lib.event.test.v0.models.TestObjectDeleted = io.flow.lib.event.test.v0.models.TestObjectDeleted(
      eventId = Factories.randomString(),
      timestamp = java.time.Instant.now,
      id = Factories.randomString()
    )

    def makeTestObjectUpserted(): io.flow.lib.event.test.v0.models.TestObjectUpserted = io.flow.lib.event.test.v0.models.TestObjectUpserted(
      eventId = Factories.randomString(),
      timestamp = java.time.Instant.now,
      testObject = io.flow.lib.event.test.v0.mock.Factories.makeTestObject()
    )

    def makeTestEvent(): io.flow.lib.event.test.v0.models.TestEvent = io.flow.lib.event.test.v0.mock.Factories.makeTestObjectUpserted()

  }

}