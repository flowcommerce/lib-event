/**
 * Generated by API Builder - https://www.apibuilder.io
 * Service version: 0.0.1
 * apibuilder 0.14.89 app.apibuilder.io/flow/lib-event-test/latest/play_2_4_mock_client
 */
package io.flow.lib.event.test.v0.mock {

  object Factories {

    def randomString(length: Int = 24): String = {
      _root_.scala.util.Random.alphanumeric.take(length).mkString
    }

    def makeTestObject(): io.flow.lib.event.test.v0.models.TestObject = io.flow.lib.event.test.v0.models.TestObject(
      id = Factories.randomString(24)
    )

    def makeTestObjectDeleted(): io.flow.lib.event.test.v0.models.TestObjectDeleted = io.flow.lib.event.test.v0.models.TestObjectDeleted(
      eventId = Factories.randomString(24),
      timestamp = _root_.org.joda.time.DateTime.now,
      id = Factories.randomString(24)
    )

    def makeTestObjectUpserted(): io.flow.lib.event.test.v0.models.TestObjectUpserted = io.flow.lib.event.test.v0.models.TestObjectUpserted(
      eventId = Factories.randomString(24),
      timestamp = _root_.org.joda.time.DateTime.now,
      testObject = io.flow.lib.event.test.v0.mock.Factories.makeTestObject()
    )

    def makeTestEvent(): io.flow.lib.event.test.v0.models.TestEvent = io.flow.lib.event.test.v0.mock.Factories.makeTestObjectUpserted()

  }

}