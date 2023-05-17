# raft-pebble
This repository provides the `raftpebble` package. The package exports the
`PebbleKVStore` which is an implementation of both a `LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` [package here](https://github.com/hashicorp/raft).

This implementation uses [cockroachdb pebble](https://github.com/cockroachdb/pebble). pebble is
a simple persistent key-value store written in pure Go for adapter cockroachdb. It has a Log-Structured-Merge (LSM) 
design and it's meant to be a performant alternative to non-Go based stores like 
[RocksDB](https://github.com/facebook/rocksdb).

# references
* [https://github.com/facebook/rocksdb/wiki](https://github.com/facebook/rocksdb/wiki)
* [https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/](https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/)
* [https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md)
* [https://github.com/hashicorp/raft/tree/main/docs](https://github.com/hashicorp/raft/tree/main/docs)
* [https://raft.github.io/raft.pdf](https://raft.github.io/raft.pdf)
* [https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
