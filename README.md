# DQD - Queue daemon

DQD is a daemon that enables flows of consuming data from queue systems or pushing data, making it optimal
We designed it to run as a sidecar container adjacent to an existing rest-service affectively, enabling the service to behave as a worker.

## Features

- Multiple Providers support
- Multiple pipelines per instance
- Error handling
- Backpressure with dynamic concurrency
- Rate metrics
- Single binary or Docker image

## Supported Providers

- AWS SQS
- Azure Queue
- Azure Service bus

# Usage

## Installing/Running

### Using Go

run `go install github.com/soluto/dqd`
run `dqd`

### Using Docker

run `` docker run -v `pwd`:/etc/dqd soluto/dqd `` in a folder that conatins dqd.yaml

## Examples

### Consume and Process using an HTTP route

The dqd.yaml should look like this:

```
sources:
    my-queue:
        type: sqs
        url:  http://aws-sqs:9324/queue/my-sqs
        region: us-east-1
pipe:
    source: my-queue
    rate:
        fixed: 1
    handler:
        http:
            endpoint: http://localhost:3000/processSqsMessages

```

### Send to Queue Using DQD

The dqd.yaml should look like this:

```
sources:
    my-queue:
        type: sqs
        url:  http://aws-sqs:9324/queue/my-sqs
        region: us-east-1
        visibilityTimeoutInSeconds: 60
```

In your code, send the data to `http://localhost:9999/my-sqs`

### Example for DQD configuration in docker-compose

```
image: soluto/dqd
ports:
    - containerPort: 8888
        protocol: TCP
      env:
        - name: AWS_SDK_LOAD_CONFIG
          value: 'true'
```
