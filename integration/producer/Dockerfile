FROM alpine:3.10.1

ENV VEGETA_VERSION 12.8.3

RUN set -ex \
 && apk add --no-cache ca-certificates \
 && apk add --no-cache --virtual .build-deps \
    openssl \
 && wget -q "https://github.com/tsenart/vegeta/releases/download/v$VEGETA_VERSION/vegeta-$VEGETA_VERSION-linux-amd64.tar.gz" -O /tmp/vegeta.tar.gz \
 && cd bin \
 && tar xzf /tmp/vegeta.tar.gz \
 && rm /tmp/vegeta.tar.gz \
 && apk del .build-deps

COPY entry.sh /
RUN chmod +x entry.sh

ENV TARGET=http://localhost:9999
ENV DATA="{""test"": ""abcd""}"

ENTRYPOINT [ "/entry.sh" ]