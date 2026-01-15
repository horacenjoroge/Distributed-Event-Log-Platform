# Multi-stage build for DistributedLog broker
FROM python:3.10-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Generate protocol buffers
COPY distributedlog/protocol distributedlog/protocol/
RUN python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    --pyi_out=. \
    distributedlog/protocol/messages/log.proto \
    distributedlog/protocol/rpc/broker_service.proto \
    distributedlog/protocol/rpc/replication_service.proto \
    distributedlog/protocol/rpc/consensus_service.proto

# Final stage
FROM python:3.10-slim

WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY distributedlog distributedlog/
COPY config config/
COPY pyproject.toml .

# Copy generated proto files from builder
COPY --from=builder /build/distributedlog/protocol distributedlog/protocol/

# Create data directory
RUN mkdir -p /var/lib/distributedlog/data

# Make sure scripts are in PATH
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/app:$PYTHONPATH

# Expose ports
# 9092: Client port
# 9093: gRPC port
# 9094: Metrics port
EXPOSE 9092 9093 9094

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.connect(('localhost', 9092)); s.close()" || exit 1

# Default command
CMD ["python", "-m", "distributedlog.broker.server.main"]
