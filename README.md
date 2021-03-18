[![Build Status](https://travis-ci.org/flowcommerce/lib-event.svg?branch=master)](https://travis-ci.org/flowcommerce/lib-event)

# lib-event
Events publishing and receiving lib

## Publishing a new version

  go run release.go

## Configuration

```hocon
production.experience.event.v0.experience_event.json = {
  maxRecords = 1000
  idleMillisBetweenCalls = 1500
  idleTimeBetweenReadsMs = 1000
  maxLeasesForWorker = 2147483647
  maxLeasesToStealAtOneTime = 1
  metricsLevel = "NONE" // "NONE", "SUMMARY", or "DETAILED"
}
```

## Testing

```
apibuilder upload flow lib-event-test ./lib-event-test.json --version 0.0.1
apibuilder update
sbt test
```