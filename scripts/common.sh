#!/usr/bin/env bash

export ETCD3_VERSION=${ETCD3_VERSION:=v3.3.8}
export CLUSTER_SIZE=${CLUSTER_SIZE:=3}
export ETCD3_PASSWORD=${ETCD3_PASSWORD:=${ETCD3_USER}}
export WORKSPACE_DIR=tests/.workspace
export FIXTURES_DIR=tests/fixtures
