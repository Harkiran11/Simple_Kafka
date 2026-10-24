# Architecture

## Components

### Broker
The broker is the core server handling all client requests. It manages partitions stores messages and participates in replication. One broker acts as controller.

### Partition
Each partition maintains an ordered log split into segments. Each segment consists of a log file and index file. Messages are appended sequentially with automatic offset assignment.

### Protocol
Custom binary protocol with message type byte followed by type specific fields. All multi byte integers are in big endian order.

### ZooKeeper
Provides distributed coordination for broker discovery controller election and cluster metadata storage.

## Data Flow

1. Client connects to any broker
2. For produce to non leader partition broker forwards to leader
3. Leader appends to log gets offset replicates to followers
4. Leader sends response back to client
5. For consume client fetches from leader reading from log by offset

## Storage Layout

```
data/
└── <brokerId>/
    └── <topic>/
        └── <partition>/
            ├── 00000000000000000000.log
            ├── 00000000000000000000.index
            └── ...
```

## ZooKeeper Paths

```
/brokers/           # Ephemeral nodes for live brokers
/controller         # Ephemeral node for elected controller
/topics/            # Persistent nodes for topic metadata
  └── <topic>/
      └── partitions/
          └── <id>   # Leader and followers info
```
