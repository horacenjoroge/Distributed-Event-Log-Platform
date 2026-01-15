# DistributedLog - Distributed Commit Log System

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Build your own Kafka/Pulsar from scratch** - Learn distributed systems by doing, based on Martin Kleppmann's "Designing Data-Intensive Applications"

DistributedLog is a production-grade distributed commit log system built from the ground up, featuring partitioning, replication, consensus, and exactly-once semantics.

## 🎯 What is This?

This project is an educational journey into the depths of distributed systems. By building a Kafka/Pulsar-like system from scratch, you'll gain deep understanding of:

- **Distributed consensus** (Raft algorithm)
- **Replication protocols** (leader-follower)
- **Network partitioning** and failure handling
- **Exactly-once semantics** and transactions
- **Zero-copy data transfers**
- **High-throughput I/O**

## 🚀 Features

### Core Capabilities
- ✅ **Append-only commit log** - Custom implementation without existing log libraries
- ✅ **Partitioning** - Horizontal scaling across multiple brokers
- ✅ **Leader-follower replication** - High availability with automatic failover
- ✅ **Raft consensus** - Leader election from scratch
- ✅ **Exactly-once delivery** - Producer idempotence and transactional writes
- ✅ **Consumer groups** - Automatic rebalancing
- ✅ **Transactional writes** - Two-phase commit protocol
- ✅ **Zero-copy transfers** - Optimized data movement
- ✅ **Cluster coordination** - Metadata management with etcd/Zookeeper

### Production Hardening (The Deep End)
- 🔥 Split-brain scenario handling
- 🔥 Corrupted log segment recovery
- 🔥 Zombie leader/consumer prevention
- 🔥 Log compaction without data loss
- 🔥 Cascading rebalance protection
- 🔥 Clock skew tolerance
- 🔥 Thundering herd mitigation
- 🔥 Byzantine failure detection
- 🔥 Multi-datacenter replication

## 📋 Prerequisites

- **Python 3.10+**
- **Docker & Docker Compose** (for running the cluster)
- **etcd or Zookeeper** (for coordination)

## 🛠️ Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd "Distributed Event Log Platform"

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
make install-dev

# Generate Protocol Buffer files
make proto
```

### Running with Docker

```bash
# Start the entire cluster (3 brokers + etcd + monitoring)
make docker-up

# View logs
make docker-logs

# Stop the cluster
make docker-down
```

### Access Points

Once running, you can access:

- **Broker 0**: `localhost:9092` (gRPC: `9093`, Metrics: `9094`)
- **Broker 1**: `localhost:9192` (gRPC: `9193`, Metrics: `9194`)
- **Broker 2**: `localhost:9292` (gRPC: `9293`, Metrics: `9294`)
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

## 📁 Project Structure

```
distributedlog/
├── core/
│   ├── log/              # Append-only log implementation
│   ├── index/            # Offset indexing for fast lookups
│   └── storage/          # Disk I/O layer with zero-copy
├── broker/
│   ├── server/           # Broker server implementation
│   ├── replication/      # Leader-follower replication
│   └── coordinator/      # Cluster coordination
├── producer/             # Producer client
├── consumer/             # Consumer client with group support
├── consensus/            # Raft consensus implementation
├── protocol/
│   ├── messages/         # Protocol Buffer definitions
│   └── rpc/              # gRPC service definitions
├── admin/                # Admin CLI and API
├── metrics/              # Prometheus metrics
├── utils/                # Logging and configuration
└── tests/                # Test suite
```

## 🏗️ Architecture

### High-Level Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Producer   │────▶│   Broker 0  │◀───▶│   Broker 1  │
└─────────────┘     │  (Leader)   │     │ (Follower)  │
                    └─────────────┘     └─────────────┘
                           │                    │
┌─────────────┐            │                    │
│  Consumer   │◀───────────┴────────────────────┘
│   Group     │                    ▲
└─────────────┘                    │
                            ┌──────┴──────┐
                            │etcd/Zookeeper│
                            │ (Coordination)│
                            └─────────────┘
```

### Components

1. **Broker**: Handles read/write requests, manages partitions
2. **Consensus Layer**: Raft for leader election and log replication
3. **Storage Layer**: Append-only log segments with indexes
4. **Coordination**: Cluster metadata in etcd/Zookeeper
5. **Clients**: Producers and consumers with built-in retries

## 🔧 Configuration

Configuration is managed through YAML files in the `config/` directory:

- `default.yaml` - Default configuration
- `broker-N.yaml` - Broker-specific overrides

Key settings:
```yaml
broker:
  id: 0
  port: 9092
  grpc_port: 9093
  data_dir: "/var/lib/distributedlog/data"
  
  log:
    segment_size_bytes: 1073741824  # 1GB
    retention_hours: 168             # 7 days
  
  replication:
    default_replication_factor: 3
    min_insync_replicas: 2
```

Override with environment variables:
```bash
export BROKER_ID=0
export BROKER_HOST=broker-0
export LOG_LEVEL=DEBUG
```

## 📊 Monitoring

### Metrics

DistributedLog exposes Prometheus metrics on port `9094`:

- **Throughput**: Messages per second
- **Latency**: p50, p95, p99 latencies
- **Replication lag**: Follower lag behind leader
- **Storage**: Disk usage, segment counts
- **Errors**: Error rates by type

### Grafana Dashboards

Pre-configured dashboards are available in `docker/grafana/dashboards/`:

- Cluster overview
- Broker performance
- Replication health
- Consumer lag

## 🧪 Testing

```bash
# Run all tests
make test

# Run with coverage
pytest --cov=distributedlog --cov-report=html

# Run specific test
pytest distributedlog/tests/unit/test_log.py
```

## 📚 Learning Path

This project follows a phased approach:

### Phase 1: Single-Node Commit Log (Weeks 1-2)
- ✅ **Task 1**: Project setup, infrastructure, protocol definitions

### Phase 2: Multi-Node Replication (Weeks 3-4)
- [ ] **Task 2**: Basic replication
- [ ] **Task 3**: Leader election with Raft
- [ ] **Task 4**: Failure handling

### Phase 3: Producer & Consumer (Weeks 5-6)
- [ ] **Task 5**: Producer with batching
- [ ] **Task 6**: Consumer groups
- [ ] **Task 7**: Exactly-once semantics

### Phase 4: Advanced Features (Weeks 7-8)
- [ ] **Task 8**: Transactions
- [ ] **Task 9**: Log compaction
- [ ] **Task 10**: Performance optimization

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Workflow

1. Create a feature branch: `git checkout -b feat/your-feature`
2. Make your changes with tests
3. Run quality checks: `make lint && make test`
4. Submit a pull request

## 📖 Resources

### Books
- **"Designing Data-Intensive Applications"** by Martin Kleppmann
- **"Distributed Systems"** by Maarten van Steen
- **"Database Internals"** by Alex Petrov

### Papers
- [Raft Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Kafka: A Distributed Messaging System](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/09/Kafka.pdf)
- [Chain Replication](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)

### Similar Projects
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Pulsar](https://pulsar.apache.org/)
- [NATS Streaming](https://nats.io/)

## ⚠️ Production Readiness

**Status: Educational/Alpha**

This project is designed for learning distributed systems. While it implements production concepts, it's not yet battle-tested for production use. Use at your own risk!

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Martin Kleppmann for "Designing Data-Intensive Applications"
- The Raft paper authors (Diego Ongaro and John Ousterhout)
- Apache Kafka and Pulsar teams for inspiration

## 💬 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/distributedlog/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/distributedlog/discussions)
- **Documentation**: [Wiki](https://github.com/yourusername/distributedlog/wiki)

---

**Built with ❤️ for learning distributed systems**
