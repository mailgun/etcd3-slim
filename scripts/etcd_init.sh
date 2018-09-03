#!/usr/bin/env bash

set -x

SCRIPT_DIR="$(dirname $0)"
source ${SCRIPT_DIR}/common.sh

export ETCDCTL_API=3

KERNEL_NAME=$(uname -s | awk '{print tolower($0)}')
ETCD3_DIST=etcd-${ETCD3_VERSION}-${KERNEL_NAME}-amd64

if [ "${ETCD3_TLS}" ]; then
    TLS_PARAMS="--insecure-transport=false --insecure-skip-tls-verify=true"
else
    TLS_PARAMS=""
fi

ETCDCTL="${WORKSPACE_DIR}/${ETCD3_DIST}/etcdctl ${TLS_PARAMS}"

if [ "${ETCD3_USER}" ]; then
    ${ETCDCTL} role add ${ETCD3_USER}
    ${ETCDCTL} user add root:root
    ${ETCDCTL} user add ${ETCD3_USER}:${ETCD3_PASSWORD}
    ${ETCDCTL} user grant-role ${ETCD3_USER} ${ETCD3_USER}
    ${ETCDCTL} role grant-permission ${ETCD3_USER} --prefix=true readwrite /
    ${ETCDCTL} auth enable
    ${ETCDCTL} --user ${ETCD3_USER}:${ETCD3_PASSWORD} get / --prefix=true
fi
