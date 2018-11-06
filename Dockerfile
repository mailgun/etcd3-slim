FROM alpine:3.8

WORKDIR /work

RUN \
    apk update && \
    apk add bash curl

COPY . \
    /work/

ENTRYPOINT /work/docker-init.sh
