# Mini Google File System (GFS)

A simplified, educational implementation of **Google File System (GFS)** concepts in Java.
This project demonstrates core distributed-systems ideas such as:

* Centralized metadata management
* Chunk-based storage
* ChunkServer registration
* Client–Master–ChunkServer interaction
* Fault-awareness foundations

---

## Architecture Overview

```
                +-------------------+
                |      Client       |
                |-------------------|
                | CREATE / READ /   |
                | WRITE requests    |
                +---------+---------+
                          |
                          v
                +-------------------+
                |       Master      |
                |-------------------|
                | Metadata          |
                |  - file → chunks  |
                |  - chunk → servers|
                | Registration      |
                +---------+---------+
                          |
          ---------------------------------------
          |                                     |
          v                                     v
+-------------------+               +-------------------+
|   ChunkServer 1   |               |   ChunkServer 2   |
|-------------------|               |-------------------|
| Stores chunk data |               | Stores chunk data |
| Handles read/write|               | Handles read/write|
+-------------------+               +-------------------+
```

---

##System Flow

### 1. ChunkServer Registration

1. ChunkServer starts
2. Connects to Master
3. Sends `REGISTER_CHUNKSERVER`
4. Master stores server address
5. Master sends ACK

### 2. File Creation

1. Client sends `CREATE_FILE` to Master
2. Master allocates a new chunk ID
3. Master assigns ChunkServer replicas
4. Metadata is stored
5. Chunk info is returned to Client

### 3. Write Chunk

1. Client contacts ChunkServer directly
2. Sends `WRITE_CHUNK` with data
3. ChunkServer persists data to disk

### 4. Read Chunk

1. Client contacts ChunkServer
2. Sends `READ_CHUNK`
3. ChunkServer returns stored data

---

## Core Components

### Master

* Maintains all metadata
* No actual file data stored
* Handles:

  * ChunkServer registration
  * File creation
  * Chunk lookup

### ChunkServer

* Stores chunk data on disk
* Handles read/write requests
* Registers itself with Master on startup

### Client

* Requests metadata from Master
* Communicates directly with ChunkServers

---

## Message Protocol

All communication uses Java `ObjectInputStream` / `ObjectOutputStream`.

### Message Fields (Typical)

```java
public class Message implements Serializable {
    public RequestType type;
    public String fileName;
    public String chunkId;
    public List<String> chunkList;
    public List<String> chunkServerList;
    public byte[] data;
}
```

### Design Notes

* Single message class simplifies protocol
* Only relevant fields are populated per request
* Uses `Serializable` for network transport

---

## Heartbeats & Failure Detection (Design)

### Why Heartbeats?

ChunkServers can fail silently. The Master must detect:

* Dead servers
* Stale metadata

### Heartbeat Mechanism

* Each ChunkServer periodically sends `HEARTBEAT` messages
* Master records last-seen timestamps
* If timeout exceeded → server is marked dead

### Failure Handling

* Dead servers removed from metadata
* Chunks can be re-replicated (future work)

---

## How to Run

### 1. Start Master

```bash
java Master
```

### 2. Start ChunkServers

```bash
java ChunkServer 6001
java ChunkServer 6002
```

### 3. Run Client

```bash
java Client
```

---

## Future Improvements

* Heartbeat-based re-replication
* Multiple chunk files per file
* Load-aware replica placement
* Versioning & consistency guarantees

---

## Educational Purpose

This project is intended for learning distributed systems concepts.
It is **not production-grade**, but closely mirrors real GFS design patterns.

---

## Status

✔ ChunkServer registration
✔ File creation
✔ Read / Write
✔ End-to-end correctness

---
Footer
© 202
