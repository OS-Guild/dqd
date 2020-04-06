# syntax = docker/dockerfile:1.0-experimental
FROM golang:1.14.0-alpine as builder
RUN apk update && apk add --no-cache git ca-certificates && update-ca-certificates
ENV USER=appuser
ENV UID=10001
RUN adduser \    
    --disabled-password \    
    --gecos "" \    
    --home "/nonexistent" \    
    --shell "/sbin/nologin" \    
    --no-create-home \    
    --uid "${UID}" \    
    "${USER}"WORKDIR $GOPATH/src/mypackage/myapp/

WORKDIR /src
COPY go.mod .
RUN go mod download
RUN go mod verify
COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/dqd

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /go/bin/dqd /go/bin/dqd
USER appuser:appuser
ENTRYPOINT ["/go/bin/dqd"]