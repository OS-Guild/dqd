# DQD - Queue daemon

DQD is a daemon that enables flows of consuming data from queue systems or pushing data, make it optimal
It was designed to run as a side car container adjacent to an existing rest-service affectively enabling the service to behave as a worker. 

## Features

- Multiple Providers support
- Multiple pipelines per instance
- Error handling
- Backpressure with dyanmic concurrency 
- Rate metrics
- Single binary or Docker image

## Supported Providers

- AWS SQS
- Azure Queue
- Azure Service bus

# Usage

## Installing/Running
### Using Go

run ```go install github.com/soluto/dqd```
run ```dqd```

### Using Docker 
run ```docker run -v `pwd`:/etc/dqd soluto/dqd``` in a folder that conatins dqd.yaml

### Publishing a new version
for each git tag a new image is being pushed to ghcr.io: `soluto/dqd:<tag_name>`

## Status
DQD is in very early stage (pre-alpha) and shouldn't be used in production yet.