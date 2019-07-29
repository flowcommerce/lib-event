#!/bin/bash

# Runs localstack in docker

if [ "$(docker ps | grep localstack)" == "" ]; then
    echo "Starting localstack"
    docker run --rm -d --name localstack -p 4568-4569:4568-4569 --user localstack -e SERVICES=kinesis,dynamodb localstack/localstack
else
    echo "Localstack already running"
    exit 1
fi
