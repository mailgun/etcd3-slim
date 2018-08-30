#!/usr/bin/env bash

set -x

SCRIPT_DIR="$(dirname $0)"
source ${SCRIPT_DIR}/defaults.sh

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

INITIAL_CLUSTER="test1=http://127.0.0.1:2380"
if (( ${CLUSTER_SIZE} > 1 )); then
    for i in $(seq 2 ${CLUSTER_SIZE}); do
        INITIAL_CLUSTER+=",test${i}=http://127.0.0.1:${i}2380"
    done
fi

for i in $(seq 1 ${CLUSTER_SIZE}); do
    client_port="2379"
    peer_port="2380"
    if (( ${i} > 1 )); then
        client_port="${i}${client_port}"
        peer_port="${i}${peer_port}"
    fi
    ${WORKSPACE_DIR}/${ETCD3_DIST}/etcd \
        --name="test${i}" \
        --listen-client-urls="https://0.0.0.0:${client_port}" \
        --advertise-client-urls="https://127.0.0.1:${client_port}" \
        --listen-peer-urls="http://127.0.0.1:${peer_port}" \
        --initial-advertise-peer-urls="http://127.0.0.1:${peer_port}" \
        --initial-cluster=${INITIAL_CLUSTER} \
        --initial-cluster-state=new \
        --data-dir=${WORKSPACE_DIR}/data${i} \
        --cert-file=${FIXTURES_DIR}/localhost.pem \
        --key-file=${FIXTURES_DIR}/localhost-key.pem \
        &>${WORKSPACE_DIR}/etcd${i}.log \
        &

    echo $! > ${WORKSPACE_DIR}/etcd${i}.pid
    disown
done

