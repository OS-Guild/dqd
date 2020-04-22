# syntax = docker/dockerfile:1.0-experimental
FROM golang:1.14.0-alpine as builder
RUN apk update && apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download 
RUN go mod verify
COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build go build -o /go/bin/worker

ENTRYPOINT ["/go/bin/worker"]