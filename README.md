# leveldb_manager
Small service for snapshotting eleveldb without stopping the Erlang node.

This is a complement to eleveldb (https://github.com/basho/eleveldb).

The leveldb manager wraps leveldb accesses and:
- keeps track of which processes have in-progress accesses,
- allows the leveldb instance to be taken offline when idle,
- blocks new accesses when the instance is offline, and
- resumes blocked accesses when the instance becomes online.

While offlined, you can use leveldb_snapshot to create a snapshot of the leveldb instance.
This copies the metadata files and creates hard links to the read-only .sst files.
