# Hekaton-based shock absorber pattern for SQL Server

A "shock absorber pattern" uses in-memory OLTP (Hekaton) tables as a buffer to ingest a high-throughput or bursty stream of data into SQL Server. An asynchronous process in the form of a stored procedure intermittently polls this buffer table like a queue, and batches the contents into the target table.

This repository contains a framework procedure used to parameterize and build the necessary components.

Read the [full blog post on sqlsunday.com](https://sqlsunday.com/2021/07/15/a-shock-absorber-pattern-for-high-performance-data-ingest/) for more detail on how it works.

# Disclaimer

I do not consider this code to be production ready. You may freely use it for inspiration or to build your own solution, but I cannot
take any responsibility for the suitability of the code for any purpose. If you download code from the Internet, it's on you to make
sure it does what you think it does.

Pull requests welcome.

# Reference documentation

## TABLE ShockAbsorber.Perfstats

Used to log performance statistics on all data batches.

Column | Type | Description
--- | --- | ---
Batch | datetime2(3) | Timestamp when an execution started (PK)
Table | sysname | Qualified name of the target table
date | datetime2(3) | Timestamp when a batch completed (PK)
rowcount | bigint | Number of rows merged by the batch
duration | int | Duration of the batch, in milliseconds
fill_factor | tinyint | The table's fill factor
is_auto_update_stats_on | bit | If stats are automatically updated
is_auto_update_stats_async_on | bit | If stats are asynchronously updated
is_clustered | bit | If the table has a clustered index
has_non_clustered | bit | If the table has one or more non-clustered indexes
is_partitioned | bit | If the table is partitioned
compression | varchar(10) | Type of compression: NONE, ROW or PAGE

## VIEW ShockAbsorber.Perfstats_report

This view builds on the log table and provides execution statistics in a more human-friendly manner.

Column | Description
--- | ---
ExecutionNo | Ordinal number for the execution
Table | Table name
Execution started | Timestamp when the execution started
BatchNo | Batch ordinal number for the execution
Offset, seconds | When the batch completed, seconds after the execution start
Duration, seconds | How long the batch took
Rows | Row count of the batch
Rows/second | Throughput for the batch
Rows/second, rolling 10 avg | Rolling 10 batch average throughput

## PROCEDURE ShockAbsorber.Create

This procedure creates the necessary stored procedure, table value function and in-memory table required.

Parameter | Optional? | Type | Description
--- | --- | --- | ---
@Object | Required | sysname | Qualified name of the target table
@ShockAbsorber_schema | Optional | sysname | Name of the schema for the in-memory table. Defaults to "ShockAbsorber"
@Persisted | Optional | bit | If the in-memory tables should be persisted to disk. Defaults to 1.
@Interval_seconds | Optional | numeric(10, 2) | Polling interval in seconds. Defaults to 1.0.
@Interval_rows | Optional | bigint | Maximum number of rows per batch. Defaults to NULL (no maximum).
@Drop_existing | Optional | bit | If the procedure can drop and recreate existing objects. Defaults to 0.
@Maxdop | Optional | tinyint | Max degree of parallelism hint. Defaults to 0 (unlimited).
@HashBuckets | Optional | bigint | Hash bucket count on a hash index. Defaults to NULL, which means non-clustered index.

This procedure creates the following objects for each target table:

* An in-memory table to act as the buffer table
* A natively compiled inline table-value function to retrieve rows from the buffer table
* A stored procedure that runs the batch process

***Warning*** When @Drop_existing=1, this procedure drops and recreates all objects, including the buffer table. Any
data in the buffer table will be lost.

## Batch procedure

The batch procedure accepts the following, optional, parameters:

Parameter | Optional? | Type | Description
--- | --- | --- | ---
@RunOnlyOnce | Optional | bit | Run only a single batch? Defaults to 0.
@PrintStats | Optional | bit | Print row count and duration statistics to the output? Defaults to 0.
@MaxRows | Optional | bigint | Stops procedure after a specific number of rows. Defaults to NULL.
@MaxErrors | Optional | int | Stops procedure after a specific number of errors. Defaults to 3.
