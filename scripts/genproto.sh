#!/bin/sh

REPO_ROOT=$(git rev-parse --show-toplevel)
PYTHON_DST_DIR=$REPO_ROOT/etcd3/_grpc_stubs
VENDOR_DIR=$REPO_ROOT/proto_vendor

# Pull dependencies.
ETCD_DIR=$VENDOR_DIR/etcd
if [ ! -d $ETCD_DIR ]; then
  git clone --depth 1 https://github.com/etcd-io/etcd.git $ETCD_DIR
fi
GOOGLEAPIS_DIR=$VENDOR_DIR/googleapis
if [ ! -d $GOOGLEAPIS_DIR ]; then
  git clone --depth 1 https://github.com/googleapis/googleapis $GOOGLEAPIS_DIR
fi
GOGOPROTOBUF_DIR=$VENDOR_DIR/gogoprotobuf
if [ ! -d $GOGOPROTOBUF_DIR ]; then
  git clone --depth 1 https://github.com/gogo/protobuf.git $GOGOPROTOBUF_DIR
fi
GRPC_GATEWAY_DIR=$VENDOR_DIR/grpc_gateway
if [ ! -d $GRPC_GATEWAY_DIR ]; then
  # v2+ contains breaking changes to `protoc-gen-swagger` directory that etcd depends on.
  git clone -b v1.16.0 --depth 1 https://github.com/grpc-ecosystem/grpc-gateway.git $GRPC_GATEWAY_DIR
fi

# Build Python stubs
INCLUDE_PATH="$VENDOR_DIR:$ETCD_DIR:$GOGOPROTOBUF_DIR:$GOGOPROTOBUF_DIR/protobuf:$GOOGLEAPIS_DIR:$GRPC_GATEWAY_DIR"
mkdir -p "$PYTHON_DST_DIR"
pip install grpcio grpcio-tools
python -m grpc.tools.protoc \
    -I=$ETCD_DIR/api/authpb:$INCLUDE_PATH \
    --python_out=$PYTHON_DST_DIR \
    --pyi_out=$PYTHON_DST_DIR \
    --grpc_python_out=$PYTHON_DST_DIR \
    auth.proto
python -m grpc.tools.protoc \
    -I=$ETCD_DIR/api/mvccpb:$INCLUDE_PATH \
    --python_out=$PYTHON_DST_DIR \
    --pyi_out=$PYTHON_DST_DIR \
    --grpc_python_out=$PYTHON_DST_DIR \
    kv.proto
python -m grpc.tools.protoc \
    -I=$ETCD_DIR/api/etcdserverpb:$INCLUDE_PATH \
    --python_out=$PYTHON_DST_DIR \
    --pyi_out=$PYTHON_DST_DIR \
    --grpc_python_out=$PYTHON_DST_DIR \
    rpc.proto
touch $PYTHON_DST_DIR/__init__.py
