# DQD - Dequeue daemon

DQD is a daemon that pull data from a queue and invoke a rest endpoint.
It was designed to run as a side car container adjacent to an existing rest-service affectively enabling the service to behave as a worker. 

## Features

- Backpressure
- Rate metrics

## Supported Providers

- Azure Queue (with SAS tokens)


## Status

DQD is in very early stage (pre-alpha) and shouldn't be used in production yet.
