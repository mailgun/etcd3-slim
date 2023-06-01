#!/bin/sh
# This script assumes that protoc is installed and available on the PATH.

# Make sure the script fails fast.
set -eux

SCRIPT_PATH=$(dirname "$0")                  # relative
REPO_ROOT=$(cd "${SCRIPT_PATH}/.." && pwd )  # absolutized and normalized
SRC_DIR=$REPO_ROOT/proto
PYTHON_DST_DIR=$REPO_ROOT/etcd3/_protobuf

pip install google-api-python-client
pip install grpcio
pip install grpcio-tools

GOOGLE_API_PATH=$(pip show google-api-python-client | grep Location:)
GOOGLE_API_PATH=${GOOGLE_API_PATH:10}

python -m grpc.tools.protoc \
    -I=$SRC_DIR \
    -I=$GOOGLE_API_PATH \
    --python_out=$PYTHON_DST_DIR \
    --pyi_out=$PYTHON_DST_DIR \
    --grpc_python_out=$PYTHON_DST_DIR \
    $SRC_DIR/*.proto

touch $PYTHON_DST_DIR/__init__.py
