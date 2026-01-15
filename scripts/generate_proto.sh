#!/bin/bash
# Script to generate Python code from Protocol Buffer definitions

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$PROJECT_ROOT/distributedlog/protocol"

echo "Generating Python code from Protocol Buffers..."

# Generate message definitions
python -m grpc_tools.protoc \
    -I"$PROJECT_ROOT" \
    --python_out="$PROJECT_ROOT" \
    --pyi_out="$PROJECT_ROOT" \
    "$PROTO_DIR/messages/log.proto"

# Generate gRPC service definitions
python -m grpc_tools.protoc \
    -I"$PROJECT_ROOT" \
    --python_out="$PROJECT_ROOT" \
    --grpc_python_out="$PROJECT_ROOT" \
    --pyi_out="$PROJECT_ROOT" \
    "$PROTO_DIR/rpc/broker_service.proto" \
    "$PROTO_DIR/rpc/replication_service.proto" \
    "$PROTO_DIR/rpc/consensus_service.proto"

echo "✓ Protocol buffer generation complete!"
echo ""
echo "Generated files:"
find "$PROTO_DIR" -name "*_pb2.py*" -o -name "*_pb2_grpc.py"
