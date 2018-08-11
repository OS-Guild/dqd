# ------------- Build ----------------------
FROM golang:1.10.1-stretch as build

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOBIN=$GOPATH/bin
WORKDIR /src

COPY . .
RUN go get .
RUN go build -o main .

# ------------- Release ----------------------
FROM scratch as release

COPY --from=build /etc/ssl/certs/ /etc/ssl/certs/
COPY --from=build /src/main /
CMD [ "/main" ]
