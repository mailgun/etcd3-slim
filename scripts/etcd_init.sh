#!/usr/bin/env bash

set -x

export ETCDCTL_API=3
ETCDCTL="etcdctl --endpoints=https://127.0.0.1:2379 --insecure-transport=false --insecure-skip-tls-verify=true"

${ETCDCTL} role add test
${ETCDCTL} role list
${ETCDCTL} user add root:root
${ETCDCTL} user add test:test
${ETCDCTL} user grant-role test test
${ETCDCTL} role grant-permission test --prefix=true readwrite /
${ETCDCTL} auth enable
${ETCDCTL} --user test:test get / --prefix=true
