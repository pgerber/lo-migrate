# Migrate Large Objects from Postgres to S3

This software is used by [Tocco AG](https://www.tocco.ch) the creator of the
[Tocco Business Framework](https://www.tocco.ch/software/branchenlosungen/ubersicht) to transition from
[Postgres Large Object](https://www.postgresql.org/docs/current/static/largeobjects.html) storage to an AWS S3 compatible
storage ([Ceph Object Storage](https://ceph.com/ceph-storage/object-storage/)). In addition, the key for referencing
binaries is updated from SHA1 to SHA2.

# Usage

TO BE WRITTEN

# Design

The objects are represented as [`lo`][lo]`::`[`Lo`][lo]s within the software. This objects are passed through multiple
groups of thread workers. To allow the [`Lo`][lo]s to be passed from thread to thread
[multi-reader and multi-writer queues](https://crates.io/crates/two-lock-queue) are used.

## Worker Threads

The [`Lo`][lo]s are passed from thread group to thread group in this order:

1. **The Observer Thread ([`thread`][thread]`::`[`Observer`][observer])**  
   This is just a single thread that reads the list of Large Object from the _nice_binary table. It creates the initial
   [`Lo`][lo] objects containing the sha1 hash, oid, mime type and size of the object. Then, it passes them on to the
   next thread group.

   info: The queue between [`Observer`][observer] and [`Receiver`][receiver] is referred to as **receiver** queue
   throughout the code.

2. **The Receiver Threads ([`thread`][thread]`::`[`Receiver`][receiver])**  
   Threads in this group fetch the Large Objects from Postgres and embed the data in the [`Lo`][lo] objects. For files
   larger than 1 MiB (configurable), the data is written to a temporary file, smaller object are kept in memory.
   Additionally, the SHA2 hash of the object is calculated and also stored on the [`Lo`][lo]s. [`Lo`][lo]s are then
   passed on to the next thread group.

   info: The queue between [`Receiver`][receiver] and [`Storer`][storer] is referred to as **storer** queue throughout
   the code.

3. **The Storer Threads ([`thread`][thread]`::`[`Storer`][storer])**  
   This group of threads is responsible for writing the data to the S3 storage and setting the correct meta data (SHA2
   hash, mime type, etc.). After dropping the data held in memory and removing temporary files the [`Lo`][lo]o are
   passed on to the next thread group.

   info: The queue between [`Storer`][storer] and [`Committer`][committer] is referred to as **committer** queue
   throughout the code.

4. **The Committer Threads ([`thread`][thread]`::`[`Committer`][committer])**  
   This thread commits the generated SHA2 hashes to the database. By default, committing 100 hashes per transaction.
   Afterwards, the [`Lo`][lo] objects are dropped for good.

Only objects that have no SHA2 set are copied and the hash is only set once the object is in the S3 storage. This way
copying can be stopped and resumed at any time without risking any data loss.

## The Monitor Thread
This thread, [`thread`][thread]`::`[`Monitor`][monitor], shows the number of [`Lo`][lo]s processed by each thread group,
status of the queue and the total amount of object migrated. To enable global stats, the atomic counters in
[`thread`][thread]`::`[`ThreadStat`][thread] are shared amongst all threads. It is the threads' responsibility to
increase their respective counters.

Message printed by the monitor thread look like this:
```
*******************************************************************
    Status at 2017-03-09 14:00:20 (updated every: 10s)

Progress Overview:
    0.08%, 19600 of 23768978 object have been migrated

Processed Objects by Thread Groups:
    observer thread  - processed:   29395, speed: 177.3 Lo/s
    receiver thread  - processed:   21195, speed: 177.2 Lo/s
    storer thread    - processed:   20075, speed: 177.3 Lo/s
    committer thread - processed:   19600, speed: 149.8 Lo/s

Queue Usage:
    receive queue    - used   8196 of   8196, 100.00% full, changed by: +0
    store queue      - used   1024 of   1024, 100.00% full, changed by: +0
    commit queue     - used    275 of   8196,   3.36% full, changed by: +275
```


[lo]: src/lo.rs
[committer]: src/thread/commit.rs
[monitor]: src/thread/monitor.rs
[observer]: src/thread/observe.rs
[receiver]: src/thread/receive.rs
[storer]: src/thread/store.rs
[thread]: src/thread/mod.rs
