# Build Your Own Redis (Python)

<p align="center">
  <img src="assets/LOGO-FIX.png" width="100%" alt="Project Banner">
</p>

This repository contains a high-performance, concurrent, and persistent Redis-compatible server built from scratch using Python. This project was developed as part of a deep-dive into network programming, data structures, and storage engines.

## 🚀 Overview

This is my implementation of the "Build Your Own Redis" challenge. It's a journey into the internals of one of the world's most popular data stores, focusing on re-implementing core Redis features while maintaining protocol compatibility.

## ✨ Features Implemented

- **Core Engine**: Support for fundamental commands like `PING`, `ECHO`, `SET`, `GET`, `DEL`, `INCR`, and `TYPE`.
- **Advanced Data Structures**:
  - **Streams**: XADD, XREAD, XRANGE with consumer group support.
  - **Sorted Sets**: ZADD, ZRANGE, ZRANK for ordered data management.
  - **Hashes**: HSET, HGET, HDEL for structured data storage.
- **Persistence**:
  - **RDB (Redis Database File)**: Loading data from periodic snapshots.
  - **AOF (Append Only File)**: Durable command logging with `fsync always` support and automatic replay on startup.
  - **AOF Rewrite (Bonus)**: Background compaction via `BGREWRITEAOF` to optimize storage usage.

- **Concurrency & Transactions**:
  - Multi-threaded connection handling.
  - Full support for `MULTI`, `EXEC`, `DISCARD`, and optimistic locking via `WATCH`/`UNWATCH`.
- **Replication**: Master-Slave architecture with automated handshakes and propagation.
- **Geospatial**: GEOADD and GEODIST with Haversine distance calculations.
- **Security**: ACL (Access Control List) and AUTH implementation with password hashing.

## 🛠️ Tech Stack

- **Language**: Python 3.8+
- **Network**: Standard `socket` library for TCP communication.
- **Concurrency**: `threading` for handling thousands of simultaneous clients.

## 🏁 How to Run

1. **Install dependencies** (ensure you have Python 3.8+):
   ```sh
   # No external dependencies required! Just standard Python.
   ```

2. **Run the server**:
   ```sh
   ./your_program.sh --port 6379
   ```

3. **Connect using redis-cli**:
   ```sh
   redis-cli -p 6379 PING
   ```

---
*Developed as a learning project to understand the inner workings of distributed systems and storage engines.*
