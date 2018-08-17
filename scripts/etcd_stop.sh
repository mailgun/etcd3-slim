#!/usr/bin/env bash

set -x

SCRIPT_DIR="$(dirname $0)"
source ${SCRIPT_DIR}/defaults.sh

WORKSPACE_DIR=tests/.workspace

for i in $(seq 1 ${CLUSTER_SIZE}); do
    kill $(cat ${WORKSPACE_DIR}/etcd${i}.pid)
    rm ${WORKSPACE_DIR}/etcd${i}.pid
done
