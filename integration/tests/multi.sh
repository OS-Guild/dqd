#!/bin/sh
cd "$(dirname "$0")"

./clean-azure.sh
docker-compose -f ../docker/docker-compose.base.yaml down --remove-orphans
PRODUCER_SOURCE=sqs PIPES_CONFIG=multi MESSAGES_COUNT=500 COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose --project-directory ../docker -f ../docker/docker-compose.base.yaml -f ../docker/docker-compose.producer.yaml -f ../docker/docker-compose.worker.yaml -f ../docker/providers/azure-local/docker-compose.yaml -f ../docker/providers/sqs-local/docker-compose.yaml up --remove-orphans --build --exit-code-from="worker"
./clean-azure.sh