# PranavDB — tiny disk-backed DB building blocks

> A small, teaching/experimental database written in Go.
> Focus: on-disk B+-tree index and a simple row storage manager with free-list reuse.
> `main.go` contains runnable examples that exercise the current features.

---

## What this repo contains (glazed summary)

1. **Disk-based B+-tree index**

   * Non-clustered index stored in a binary `.idx` file.
   * Fixed-size pages (4 KiB) — one tree node per page.
   * Implements insertion, range search and deletion on-disk.
   * Designed for persistent storage and to be extended (splits, balancing, etc.).

2. **Row storage manager**

   * Disk-based binary row file format (one file per table).
   * Header contains schema and a free-list head.
   * Rows are variable-length; each row is prefixed with a 2-byte payload length.
   * Free-list is a linked list of freed row slots; freed slots store metadata so they can be reused efficiently.
   * The code includes write/read, free (tombstone) and free-slot reuse logic.

3. **SQL parser / lexer**

   * Work in progress — building a tiny lexer and parser to support basic SQL (`CREATE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `WHERE`).
   * Current focus: pluggable planner that will pick index vs full scan based on simple heuristics.

---

## Highlights / design notes

* Pages are **4 KiB** to match a page-based on-disk design (typical DB page size). Each B+-tree node is stored inside one page.
* Index file format is a compact binary layout (`.idx`) with page headers and a codec that encodes/decodes internal/leaf nodes.
* Row file header reserves bytes for schema encoding (simple byte codes per column) and stores `firstFreePage` for the free list.
* Schema encoding is minimal: each column maps to a 1-byte type code (supported basic types: `INT`, `FLOAT`, `STRING`). That mapping is converted to a compact byte array and persisted in the header.
* Free-list format (rowfile): a freed row starts with a 2-byte sentinel `0xFFFF`, followed by the free-list `next` pointer and the original payload length — so the allocator can find reusable slots across restarts.

---

## Directory structure

```
.
├── data/                  # row storage manager (row codec + file handler)
│   ├── rowCodec.go
│   └── rowFileHandler.go
├── index/                 # index logic (disk B+ tree)
│   ├── diskTree.go
│   └── indexFile.go
├── page/                  # page code & codecs for index pages
│   ├── IndexCodec.go
│   └── pageStruct.go
├── tree/                  # in-memory tree structs and helpers
│   └── tree.go
├── main.go                # example / demo code that exercises the modules
├── test_index.idx         # sample index file produced by tests/examples
├── test_rows.dat          # sample row file produced by tests/examples
└── README.md
```

* `data/` — row-level storage: schema parsing/encoding, `WriteRow`, `ReadRowAt`, `FreeRowAt`, free-list and header persistence.
* `index/` and `page/` — the on-disk B+-tree index and the page codecs/structures used to serialize/deserialize index nodes.
* `tree/` — shared tree node types (leaf/interior nodes, pairs, keys).
* `main.go` — small demo that creates files, inserts rows, builds index, runs queries, and demonstrates free-list reuse.

---

## File formats (concise)

### Row file header (fixed region at file start)

* Header size: **4096 bytes** (reserved block).
* Layout (important fields):

  * `bytes 0..1`   — `columnCount` (uint16)
  * `bytes 2..9`   — `firstFreePage` (uint64) — offset of free-list head (0 = none)
  * `bytes 10..(10+SchemaReserve-1)` — schema area (SchemaReserve = 1000 bytes): 1-byte type codes per column (only first `columnCount` bytes used)

### Row encoding (per row)

* `2 bytes` — payload length (uint16)
* `payload` — encoded columns according to schema codes:

  * `INT` → 4 bytes (int32)
  * `FLOAT` → 8 bytes (float64)
  * `STRING` → 2 bytes length (uint16) + bytes
* **Deleted slot format** (when freed):

  * `2 bytes` => `0xFFFF` marker (indicates free)
  * `8 bytes` => `nextFreeOffset` (uint64) — previous head of free list (becomes link)
  * `2 bytes` => `originalPayloadLen` (uint16)
  * rest unused in that slot

### Index files (.idx)

* Page size: **4 KiB**.
* One index node per page.
* Node header + payload encoded by page codec in `page/IndexCodec.go`.
* File header (root pointer, version, etc.) handled by `indexFile.go`.

---

## Build & run

Assuming Go 1.20+:

```sh
# build
go build -o pranavdb.exe

# run the demo
go run main.go
# or run the built binary
./pranavdb.exe
```

`main.go` demonstrates:

* creating a row file,
* inserting rows,
* reading rows,
* freeing a row and reusing the freed slot,
* using the index for point lookups and range queries.

---

## Quick usage example (from `main.go` behavior)

* Create a `rowfile` for schema `int,string,float`.
* `WriteRow` returns an offset; `ReadRowAt(offset)` decodes it back.
* `FreeRowAt(offset)` marks a row free and updates header/free-list.
* Subsequent `WriteRow` attempts to reuse freed slots when suitable.

---

## Testing

There are a few test/demo artifacts in the repo:

* Use the `main.go` demo to exercise the current features and see log output.

---

## Next steps / roadmap (what I’m working on)

* Complete lexer + parser to accept simple SQL (`CREATE TABLE`, `INSERT`, `SELECT ... WHERE`, `DELETE`).
* Finish B+-tree: node splitting and full internal node support (right now code demonstrates basic leaf operations).
* Add a simple planner that chooses index vs full-scan, and implement basic WHERE predicates (`=`, `>`, `<`, ranges`, `AND\`).
* WAL (write-ahead log) and crash-recovery for durability.
* Background vacuum/compaction to defragment row files (reclaim fragmented free space and rewrite indexes).

---



Which would you like next?
