#! /bin/sh -e

export CLUSTER_SIZE=1
/work/scripts/etcd_start.sh

/work/scripts/etcd_init.sh

tail -f /work/tests/.workspace/etcd1.log
