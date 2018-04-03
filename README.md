[![Build Status](https://travis-ci.org/flowcommerce/lib-event.svg?branch=master)](https://travis-ci.org/flowcommerce/lib-event)

# lib-event
Events publishing and receiving lib

## Publishing a new version

  go run release.go


## Testing

```
apibuilder upload flow lib-event-test ./lib-event-test.json --version 0.0.1
apibuilder update
sbt test
```