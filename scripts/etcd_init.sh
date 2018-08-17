#!/usr/bin/env bash

set -x

SCRIPT_DIR="$(dirname $0)"
source ${SCRIPT_DIR}/defaults.sh

export ETCDCTL_API=3

KERNEL_NAME=$(uname -s | awk '{print tolower($0)}')
ETCD3_DIST=etcd-${ETCD3_VERSION}-${KERNEL_NAME}-amd64
WORKSPACE_DIR=tests/.workspace
ETCDCTL="${WORKSPACE_DIR}/${ETCD3_DIST}/etcdctl --endpoints=https://127.0.0.1:2379 --insecure-transport=false --insecure-skip-tls-verify=true"

${ETCDCTL} role add test
${ETCDCTL} user add root:root
${ETCDCTL} user add test:test
${ETCDCTL} user grant-role test test
${ETCDCTL} role grant-permission test --prefix=true readwrite /
${ETCDCTL} auth enable
${ETCDCTL} --user test:test get / --prefix=true
