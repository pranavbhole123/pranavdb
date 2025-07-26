# PranavDB

A lightweight, educational database engine implemented in Go. Demonstrates how a B+ Tree primary index and a slotted‑page on‑disk storage engine work together.

---

## 📖 Overview

**PranavDB** is built from scratch in Go to illustrate core database internals:

* **Primary Index**: Custom B+ Tree for fast key lookups.
* **Storage Engine**: 4 KiB fixed-size pages using a slotted‑page layout for variable-length row storage.
* **DiskManager**: Reads/writes pages to a single file, tracks free space.

Intended as a learning tool rather than production software.

---

## 💡 Features

* **B+ Tree**:

  * In-memory node splits and merges
  * On-disk serialization/deserialization
  * Configurable order

* **Slotted‑Page Storage**:

  * Page size: 4 KiB
  * Page header with slot count & free-space pointer
  * Slot array of (offset, length) entries
  * Row data packed from the end of page inward

* **Metadata (Superblock)**:

  * Page 0 stores root pointer, free‑list head, and format version

---

## 🔎 Implementation Details

### B+ Tree Node Layout

* **Page Header (16 bytes)**:

  * `NodeType` (1 byte): internal or leaf
  * `NumKeys` (2 bytes)
  * `ThisPageID`, `ParentPage`, `NextLeaf` (each 4 bytes)

* **Payload**:

  * Internal: up to M keys (`uint64`) + M+1 child `pageID`s (`uint32`)
  * Leaf: keys + (pageID, slotIndex) references to row pages

### Slotted‑Page Layout

* **Row Page Header (8 bytes)**:

  * `NumSlots` (`uint16`)
  * `FreeSpaceStart` (`uint16`)

* **Slot Array**: each slot is 4 bytes (`offset:uint16`, `length:uint16`)

* **Data Region**: rows packed backward from page end

---

## 📜 License

MIT © Pranav Dipesh Bhole
