#!/usr/bin/env bash

set -x

WORKSPACE_DIR=tests/.workspace

kill $(cat ${WORKSPACE_DIR}/etcd.pid)
rm ${WORKSPACE_DIR}/etcd.pid
