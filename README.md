<div align="center">

# SimpleKafka

**A distributed log based messaging system built from scratch in Java inspired by Apache Kafka**

[![Java](https://img.shields.io/badge/Java-11-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/Maven-C71A36?style=for-the-badge&logo=apache-maven&logoColor=white)](https://maven.apache.org/)
[![ZooKeeper](https://img.shields.io/badge/Apache%20ZooKeeper-3.8.1-D22128?style=for-the-badge&logo=apache&logoColor=white)](https://zookeeper.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

</div>

## Overview

SimpleKafka is a from scratch implementation of core Apache Kafka internals. It demonstrates distributed systems concepts including partitioned logs leader follower replication cluster coordination via ZooKeeper and custom binary protocol design. Built to showcase low level systems programming and distributed architecture skills.

## Key Features

- **Custom Binary Protocol** - Length prefixed TCP protocol for all operations with minimal overhead
- **Segmented Log Storage** - Append only logs split into 1MB segments with sparse indexing for efficient lookups
- **Cluster Coordination** - Automatic broker registration controller election and failure detection using ZooKeeper
- **Leader Follower Replication** - Partition leaders handle writes and replicate to followers with acknowledgments
- **Topic Partitioning** - Topics split across partitions distributed for scalability
- **Fault Tolerance** - Automatic rebalancing and leader reassignment on broker failures
- **Non Blocking I/O** - Java NIO based server with thread pooled request handling
- **Client APIs** - Clean producer consumer and admin interfaces

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Producer   │    │   Consumer   │    │  ZooKeeper   │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       │           TCP/Binary Protocol       │ Coordination
       │                  │                  │
       └──────────────────┼──────────────────┘
                          ▼
                    ┌─────────────┐
                    │Broker Cluster│
                    │  (3 Nodes)   │
                    └─────────────┘
```

## Tech Stack

| Technology | Purpose |
|------------|---------|
| Java 11 | Core language |
| Maven | Build and dependency management |
| Java NIO | Non blocking network I/O |
| Apache ZooKeeper 3.8.1 | Distributed coordination and metadata |

## Project Structure

```text
src/main/java/simplekafka/
├── broker/
│   ├── SimpleKafkaBroker.java   # Broker server request routing and controller logic
│   ├── Partition.java           # Log storage engine with segments and indexing
│   ├── Protocol.java            # Binary protocol definitions and encoders
│   ├── ZookeeperClient.java     # ZooKeeper integration layer
│   └── BrokerInfo.java          # Broker metadata model
└── client/
    ├── SimpleKafkaProducer.java # High level producer API
    ├── SimpleKafkaConsumer.java # High level consumer API
    ├── SimpleKafkaClient.java   # Low level client implementation
    └── SimpleKafkaAdmin.java    # Topic administration tool
```

## Quick Start

### Prerequisites

- [Java 11+](https://openjdk.org/)
- [Maven 3.6+](https://maven.apache.org/)
- [Apache ZooKeeper](https://zookeeper.apache.org/) running on `localhost:2181`

### Build

```bash
mvn clean package
```

### Run a 3 Node Cluster

Open 4 terminals

**Terminal 1 - ZooKeeper**
```bash
zkServer start
```

**Terminal 2 - Broker 1**
```bash
java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar simplekafka.broker.SimpleKafkaBroker 1 localhost 9091 2181
```

**Terminal 3 - Broker 2**
```bash
java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar simplekafka.broker.SimpleKafkaBroker 2 localhost 9092 2181
```

**Terminal 4 - Broker 3**
```bash
java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar simplekafka.broker.SimpleKafkaBroker 3 localhost 9093 2181
```

### Produce and Consume

**Create topic**
```bash
java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar simplekafka.client.SimpleKafkaAdmin localhost 9091 orders 3 1
```

**Start producer**
```bash
java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar simplekafka.client.SimpleKafkaProducer localhost 9091 orders
```

**Start consumer**
```bash
java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar simplekafka.client.SimpleKafkaConsumer localhost 9091 orders 0
```

## Technical Highlights

- **Storage Engine** - Implements log segments with binary search over index for offset lookups
- **Coordination** - Uses ephemeral znodes for liveness detection and atomic operations for controller election
- **Networking** - Custom protocol with type prefixes and length fields for reliable framing
- **Replication** - Asynchronous replication to followers with ack tracking
- **Concurrency** - Read write locks on partitions executor service for parallel request handling

## What I Learned

Building this project deepened my understanding of distributed systems file systems concurrency and network programming. I implemented patterns used in production systems like Kafka from first principles.

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details
