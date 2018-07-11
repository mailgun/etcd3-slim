#!/usr/bin/env bash

set -x

KERNEL_NAME=$(uname -s | awk '{print tolower($0)}')
ETCD3_DIST=etcd-${ETCD3_VERSION}-${KERNEL_NAME}-amd64
if [[ "$KERNEL_NAME" =~ ^(darwin|windows)$ ]]; then
    ZIP=zip
else
    ZIP=tar.gz
fi
ETCD3_ARCHIVE=${ETCD3_DIST}.${ZIP}
FIXTURES_DIR=tests/fixtures
WORKSPACE_DIR=tests/.workspace

mkdir -p ${WORKSPACE_DIR}

if [ ! -f ${WORKSPACE_DIR}/${ETCD3_ARCHIVE} ]; then
    curl -s -L https://github.com/coreos/etcd/releases/download/${ETCD3_VERSION}/${ETCD3_ARCHIVE} > ${WORKSPACE_DIR}/${ETCD3_ARCHIVE}
fi
if [ ! -d ${WORKSPACE_DIR}/${ETCD3_DIST} ]; then
    tar -xzf ${WORKSPACE_DIR}/${ETCD3_ARCHIVE} -C ${WORKSPACE_DIR}
fi

${WORKSPACE_DIR}/${ETCD3_DIST}/etcd \
    --name=test \
    --listen-client-urls 'https://127.0.0.1:2379' \
    --data-dir=${WORKSPACE_DIR}/data \
    --advertise-client-urls 'https://127.0.0.1:2379' \
    --cert-file=${FIXTURES_DIR}/localhost.pem \
    --key-file=${FIXTURES_DIR}/localhost-key.pem \
    &>${WORKSPACE_DIR}/etcd.log \
    &

echo $! > ${WORKSPACE_DIR}/etcd.pid
disown
