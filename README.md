# Akka.Persistence.Azure
Azure Table and Blob Storage implementation for Akka.Persistence (Akka.Net)

## Technical Overview

This implementation makes use of Azure Table Storage and Blob storage and is set up to be able to be configured using HOCON style configuration.
See the Tests for samples which use the local development storage emulator.

It is also set up to use a sharded set of storage accounts with up 16 accounts.  The PersistenceId is used to determine which
storage account connection is used.  Since the Journal and the Snapshot Store are separate, technically you could use 32 different 
accounts, 16 for Journal and 16 for Snapshots.  If you do not want to shard then use the same storage account connection string in all
16 slots as seen in the tests.

IF you know that your aggregate snapshots will fit within the table storage size limitation, you can use the TableStorageSnapshotStore instead of the BlobStorageSnapshotStore
