# raft-pebble
This repository provides the `raftpebble` package. The package exports the
`PebbleKVStore` which is an implementation of both a `LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` [package here](https://github.com/hashicorp/raft).

This implementation uses [cockroachdb pebble](https://github.com/cockroachdb/pebble). pebble is
a simple persistent key-value store written in pure Go for adapter cockroachdb. It has a Log-Structured-Merge (LSM) 
design and it's meant to be a performant alternative to non-Go based stores like 
[RocksDB](https://github.com/facebook/rocksdb).

# bench Pebble vs Badger
bench with [BBVA/raft-badger](https://github.com/weedge/raft-badger),
run `go test -run=NONE -benchmem -bench=. ./...` to diff bench result,
below just simple bench on macOS, more bench please test on product env machine (use SATA/NVMe SSD, think Compression and sync w, use diff k/v dataset, w/r mode)
```
goos: darwin
goarch: amd64
pkg: github.com/BBVA/raft-badger
cpu: Intel(R) Core(TM) i5-1038NG7 CPU @ 2.00GHz
BenchmarkBadgerStore_FirstIndex-8         558468              1893 ns/op            1005 B/op         17 allocs/op
BenchmarkBadgerStore_LastIndex-8          476727              2329 ns/op             991 B/op         17 allocs/op
BenchmarkBadgerStore_GetLog-8             260952              4269 ns/op            1780 B/op         43 allocs/op
BenchmarkBadgerStore_StoreLog-8            55231             21848 ns/op            4816 B/op         65 allocs/op
BenchmarkBadgerStore_StoreLogs-8           31244             37265 ns/op           10133 B/op        139 allocs/op
BenchmarkBadgerStore_DeleteRange-8         50634             23891 ns/op            4609 B/op         89 allocs/op
BenchmarkBadgerStore_Set-8                 77796             14079 ns/op            2675 B/op         41 allocs/op
BenchmarkBadgerStore_Get-8                652993              1546 ns/op             451 B/op         12 allocs/op
BenchmarkBadgerStore_SetUint64-8           75255             14539 ns/op            2729 B/op         41 allocs/op
BenchmarkBadgerStore_GetUint64-8          775291              1621 ns/op             449 B/op         12 allocs/op
BenchmarkSet-8                             77793             13437 ns/op            2703 B/op         40 allocs/op
BenchmarkGet-8                            554457              2234 ns/op             560 B/op         13 allocs/op
BenchmarkStoreLogs-8                       59142             20193 ns/op            4690 B/op         64 allocs/op
BenchmarkGetLog-8                         259209              4268 ns/op            1740 B/op         41 allocs/op
```
```
goos: darwin
goarch: amd64
pkg: github.com/weedge/raft-pebble
cpu: Intel(R) Core(TM) i5-1038NG7 CPU @ 2.00GHz
BenchmarkPebbleKVStoreStore_FirstIndex-8         1645522               728.4 ns/op             8 B/op          1 allocs/op
BenchmarkPebbleKVStoreStore_LastIndex-8          1514397               779.2 ns/op            88 B/op          2 allocs/op
BenchmarkPebbleKVStoreStore_GetLog-8              405542              2603 ns/op            1136 B/op         34 allocs/op
BenchmarkPebbleKVStoreStore_StoreLog-8             89127             12486 ns/op            1489 B/op         27 allocs/op
BenchmarkPebbleKVStoreStore_StoreLogs-8            22774             47654 ns/op            4195 B/op         75 allocs/op
BenchmarkPebbleKVStoreStore_DeleteRange-8         390418              5706 ns/op              89 B/op          3 allocs/op
BenchmarkPebbleKVStoreStore_Set-8                 254138              4334 ns/op              18 B/op          3 allocs/op
BenchmarkPebbleKVStoreStore_Get-8                3295018               382.8 ns/op            16 B/op          2 allocs/op
BenchmarkPebbleKVStoreStore_SetUint64-8           182452              5517 ns/op              20 B/op          2 allocs/op
BenchmarkPebbleKVStoreStore_GetUint64-8          3092731               500.6 ns/op            16 B/op          2 allocs/op
BenchmarkSet-8                                    259359              3996 ns/op              18 B/op          1 allocs/op
BenchmarkGet-8                                   1206459              1020 ns/op              16 B/op          1 allocs/op
BenchmarkStoreLogs-8                              127917              7970 ns/op            1480 B/op         26 allocs/op
BenchmarkGetLog-8                                 364390              3033 ns/op             784 B/op         29 allocs/op
```
pebble have more better ops on raft log store case.
more prioritization feature vs rocksdb diff see [pebble-rocksdb-kv-store](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md) 

# references
* [https://github.com/facebook/rocksdb/wiki](https://github.com/facebook/rocksdb/wiki)
* [https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/](https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/)
* [https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md)
* [https://github.com/hashicorp/raft/tree/main/docs](https://github.com/hashicorp/raft/tree/main/docs)
* [https://raft.github.io/raft.pdf](https://raft.github.io/raft.pdf)
* [https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
* [badger-txn](https://dgraph.io/blog/post/badger-txn/)
* [https://www.cockroachlabs.com/blog/consistency-model/](https://www.cockroachlabs.com/blog/consistency-model/)
