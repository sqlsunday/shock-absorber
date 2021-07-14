IF (SCHEMA_ID('ShockAbsorber') IS NULL)
    EXEC('CREATE SCHEMA ShockAbsorber;')
GO

-------------------------------------------------------------------------------
---
--- This table logs statistics for every batch:
---
-------------------------------------------------------------------------------

IF (OBJECT_ID('ShockAbsorber.Perfstats') IS NULL) BEGIN;
    CREATE TABLE ShockAbsorber.Perfstats (
        Batch                               datetime2(3) NOT NULL,
        [Table]                             sysname NOT NULL,
        [date]                              datetime2(3) NOT NULL,
        [rowcount]                          bigint NOT NULL,
        duration                            int NOT NULL,
        fill_factor                         tinyint NOT NULL,
        is_auto_update_stats_on             bit NOT NULL,
        is_auto_update_stats_async_on       bit NOT NULL,
        is_clustered                        bit NOT NULL,
        has_non_clustered                   bit NOT NULL,
        is_partitioned                      bit NOT NULL,
        [compression]                       varchar(10) NOT NULL
    );

    --- Add a clustered primary key. Use the OPTIMIZE_FOR_SEQUENTIAL_KEY=ON hint
    --- if you're on SQL Server 2019 or higher.
    DECLARE @sql nvarchar(max)=N'
    ALTER TABLE ShockAbsorber.Perfstats ADD CONSTRAINT PK_Perfstats PRIMARY KEY CLUSTERED (Batch, [Date])'+
        (CASE WHEN CAST(LEFT(CAST(SERVERPROPERTY('productversion') AS nvarchar(100)), 2) AS tinyint)>=15
              THEN N' WITH (OPTIMIZE_FOR_SEQUENTIAL_KEY=ON)'
              ELSE N'' END);
    EXECUTE sys.sp_executesql @sql;

END;

GO

-------------------------------------------------------------------------------
---
--- Report view that compiles execution statistics
---
-------------------------------------------------------------------------------

CREATE OR ALTER VIEW ShockAbsorber.Perfstats_report
AS

SELECT DENSE_RANK() OVER (ORDER BY Batch) AS ExecutionNo,
       [Table],
       Batch AS [Batch started],
       ROW_NUMBER() OVER (PARTITION BY Batch ORDER BY [date]) AS BatchNo,
       CAST(0.001*DATEDIFF(ms, Batch, [date]) AS numeric(12, 3)) AS [Offset, seconds],
       CAST(0.001*[duration] AS numeric(12, 3)) AS [Duration, seconds],
       [rowcount] AS [Rows],
       1000*[rowcount]/NULLIF([duration], 0) AS [Rows/second],
       1000*SUM([rowcount]) OVER (PARTITION BY Batch ORDER BY [date] ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)/SUM(duration) OVER (PARTITION BY Batch ORDER BY [date] ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS [Rows/second, rolling 10 avg]
FROM ShockAbsorber.Perfstats;

GO

-------------------------------------------------------------------------------
---
--- Framework procedure that creates the Hekaton table, table value function,
--- and stored procedure to run the batch loads for an existing disk-based
--- table.
---
-------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE ShockAbsorber.[Create]
    @Object                 sysname,            --- Qualified table name
    @ShockAbsorber_schema   sysname=NULL,       --- Name of the schema for the in-memory table
    @Persisted              bit=1,              --- if in-memory tables should be persisted to disk
    @Interval_seconds       numeric(10, 2)=1.0, --- Polling interval in seconds
    @Interval_rows          bigint=NULL,        --- Maximum number of rows per batch
    @Drop_existing          bit=0,              --- If the procedure can drop and recreate existing objects
    @Maxdop                 tinyint=0,          --- Max degree of parallelism hint
    @HashBuckets            bigint=NULL         --- NULL=non-clustered index; >0 = hash index on primary key
AS

SET NOCOUNT ON;

DECLARE @columns TABLE (
    column_id               int NOT NULL,
    key_ordinal             int NOT NULL,
    [name]                  sysname NOT NULL,
    PRIMARY KEY CLUSTERED (column_id)
);

DECLARE @object_id          int=OBJECT_ID(@Object),
        @pk_index_id        int,
        @table_name         sysname,
        @pk_index_name      sysname,
        @sync_proc_name     sysname,
        @sync_func_name     sysname,
        @column_list        nvarchar(max),
        @key_column_list    nvarchar(max),
        @sql                nvarchar(max)=N'';

IF (@ShockAbsorber_schema IS NULL)
    SET @ShockAbsorber_schema=OBJECT_SCHEMA_NAME(@@PROCID);

--- Validate the object:
IF (@object_id IS NULL) BEGIN;
    THROW 50001, 'Invalid object name in @Object parameter.', 1;
    RETURN;
END;

--- Prefer a unique clustered index, otherwise, pick the first unique index
--- with the lowest column count
SELECT TOP (1) @pk_index_id=i.index_id
FROM sys.indexes AS i
INNER JOIN (
    SELECT index_id, COUNT(*) AS column_count
    FROM sys.index_columns
    WHERE [object_id]=@object_id AND key_ordinal>0
    GROUP BY index_id) AS ic ON i.index_id=ic.index_id
WHERE i.[object_id]=@object_id
  AND i.index_id>=1
  AND i.is_unique=1
ORDER BY (CASE WHEN i.index_id=1 THEN 0 ELSE ic.column_count END), i.index_id;

IF (@pk_index_id IS NULL) BEGIN;
    THROW 50001, 'There''s no unique index on this object.', 1;
    RETURN;
END;

INSERT INTO @columns (column_id, key_ordinal, [name])
SELECT c.column_id, ISNULL(ic.key_ordinal, 0), c.[name]
FROM sys.columns AS c
LEFT JOIN sys.index_columns AS ic ON
    ic.column_id=c.column_id AND
    ic.[object_id]=@object_id AND
    ic.index_id=@pk_index_id AND
    ic.key_ordinal>=1
WHERE c.[object_id]=@object_id
  AND c.generated_always_type=0;

SET @table_name=QUOTENAME(@ShockAbsorber_schema)+N'.'+QUOTENAME(OBJECT_SCHEMA_NAME(@object_id)+N'_'+OBJECT_NAME(@object_id));
SET @sync_proc_name=QUOTENAME(@ShockAbsorber_schema)+N'.'+QUOTENAME(N'SYNC_'+OBJECT_SCHEMA_NAME(@object_id)+N'_'+OBJECT_NAME(@object_id));
SET @sync_func_name=QUOTENAME(@ShockAbsorber_schema)+N'.'+QUOTENAME(N'FN_'+OBJECT_SCHEMA_NAME(@object_id)+N'_'+OBJECT_NAME(@object_id));
SET @pk_index_name=QUOTENAME(N'PK_'+OBJECT_NAME(@object_id));

SELECT @column_list=STRING_AGG(QUOTENAME([name]), N', ') WITHIN GROUP (ORDER BY column_id)
FROM @columns;

SELECT @key_column_list=STRING_AGG(QUOTENAME([name]), N', ') WITHIN GROUP (ORDER BY key_ordinal)
FROM @columns
WHERE key_ordinal>=1;

-------------------------------------------------------------------------------
--- Drop existing objects?
-------------------------------------------------------------------------------

IF (@Drop_existing=1)
    SET @sql=@sql+N'
DROP PROCEDURE IF EXISTS '+@sync_proc_name+N';
DROP FUNCTION IF EXISTS '+@sync_func_name+N';
DROP TABLE IF EXISTS '+@table_name+N';
';

-------------------------------------------------------------------------------
--- Create the in-memory table:
-------------------------------------------------------------------------------

SET @sql=@sql+N'
CREATE TABLE '+@table_name+N' (
    [##ShockAbsorberIdentity##] bigint IDENTITY(1, 1) NOT NULL,';

SELECT @sql=@sql+N'
    '+QUOTENAME(c.[name])+N' '+t.[name]+(CASE
			       WHEN c.user_type_id!=c.system_type_id THEN ''
			       WHEN t.[name] LIKE 'n%char%' OR t.[name] LIKE 'n%binary%' THEN '('+ISNULL(CAST(NULLIF(c.max_length, -1)/2 AS varchar(max)), 'max')+')'
			       WHEN t.[name] LIKE '%char%' OR t.[name] LIKE '%binary%'   THEN '('+ISNULL(CAST(NULLIF(c.max_length, -1)   AS varchar(max)), 'max')+')'
			       WHEN t.[name] IN ('numeric', 'decimal') THEN '('+CAST(c.[precision] AS varchar(max))+', '+CAST(c.scale AS varchar(max))+')'
			       WHEN t.[name] IN ('datetime2', 'time') THEN '('+CAST(c.scale AS varchar(max))+')'
			       WHEN t.[name]='xml' THEN ISNULL('('+xsc_sch.[name]+'.'+xsc.[name]+')', '')
			       ELSE ''
			       END)+
    ISNULL(N' COLLATE '+c.collation_name COLLATE database_default, N'')+
    (CASE c.is_nullable WHEN 0 THEN N' NOT NULL' ELSE N' NULL' END)+N','
FROM sys.columns AS c
INNER JOIN sys.types AS t ON c.system_type_id=t.user_type_id
LEFT JOIN sys.xml_schema_collections AS xsc ON c.xml_collection_id=xsc.xml_collection_id
LEFT JOIN sys.schemas AS xsc_sch ON xsc.[schema_id]=xsc_sch.[schema_id]
WHERE c.[object_id]=@object_id
  AND c.generated_always_type=0
ORDER BY c.column_id;

SET @sql=@sql+N'
    CONSTRAINT '+@pk_index_name+N' PRIMARY KEY NONCLUSTERED ([##ShockAbsorberIdentity##]),'+
    (CASE WHEN @Interval_rows>0 THEN N'' ELSE N'
    INDEX IX_DESC UNIQUE NONCLUSTERED ([##ShockAbsorberIdentity##] DESC),' END)+
    (CASE WHEN @HashBuckets>0 THEN N'
    INDEX IX HASH ('+(SELECT STRING_AGG(QUOTENAME([name]), N', ') WITHIN GROUP (ORDER BY key_ordinal) FROM @columns WHERE key_ordinal>=1)+N') WITH (BUCKET_COUNT='+CAST(@HashBuckets AS nvarchar(20))+N')'
    ELSE N'
    INDEX IX UNIQUE NONCLUSTERED ('+(SELECT STRING_AGG(QUOTENAME([name]), N', ') WITHIN GROUP (ORDER BY key_ordinal) FROM @columns WHERE key_ordinal>=1)+N', [##ShockAbsorberIdentity##])'
    END)+N'
) WITH (
    MEMORY_OPTIMIZED=ON,
    DURABILITY='+(CASE WHEN @Persisted=1 THEN N'SCHEMA_AND_DATA' ELSE N'SCHEMA_ONLY' END)+N'
);'

EXECUTE sys.sp_executesql @sql;

-------------------------------------------------------------------------------
--- Create the shock absorber table value function
-------------------------------------------------------------------------------

SET @sql=N'
-------------------------------------------------------------------------------
---
--- Get the dataset for '+@table_name+N' up to the high-water mark.
---
-------------------------------------------------------------------------------

CREATE FUNCTION '+@sync_func_name+N'(@identity bigint)
RETURNS TABLE
WITH NATIVE_COMPILATION, SCHEMABINDING
AS

RETURN (
    SELECT '+(SELECT STRING_AGG(QUOTENAME([name]), N',
           ') WITHIN GROUP (ORDER BY column_id) FROM @columns)+N'
    FROM '+@table_name+N' WITH (INDEX='+@pk_index_name+')
    WHERE [##ShockAbsorberIdentity##] IN (
        SELECT MAX([##ShockAbsorberIdentity##])
        FROM '+@table_name+N'
        WHERE [##ShockAbsorberIdentity##]<=@identity
        GROUP BY '+(SELECT STRING_AGG(QUOTENAME([name]), N',
                 ') WITHIN GROUP (ORDER BY key_ordinal) FROM @columns WHERE key_ordinal>=1)+N')
);
';

EXECUTE sys.sp_executesql @sql;

-------------------------------------------------------------------------------
--- Create the shock absorber procedure
-------------------------------------------------------------------------------

SET @sql=N'
-------------------------------------------------------------------------------
---
--- Synchronize '+@table_name+N' (up to the high-water mark)
--- into the base table, '+QUOTENAME(OBJECT_SCHEMA_NAME(@object_id))+N'.'+QUOTENAME(OBJECT_NAME(@object_id))+N'.
---
-------------------------------------------------------------------------------

CREATE PROCEDURE '+@sync_proc_name+N'
    @RunOnlyOnce    bit=0,          --- if the procedure should run only once, as opposed to continuously.
    @PrintStats     bit=0,          --- output timing and rowcount statistics using PRINT
    @MaxRows        bigint=NULL,    --- maximum number of total rows before the procedure terminates.
    @MaxErrors      int=3           --- maximum consecutive errors before the procedure terminates.
AS

SET NOCOUNT ON;

DECLARE @Batch datetime2(3)=SYSDATETIME(),
        @fill_factor tinyint,
        @is_auto_update_stats_on bit,
        @is_auto_update_stats_async_on bit,
        @is_clustered bit,
        @has_non_clustered bit,
        @is_partitioned bit,
        @compression varchar(20);

SELECT @fill_factor=i.fill_factor,
       @is_auto_update_stats_on=ISNULL(1-s.no_recompute, 0),
       @is_auto_update_stats_async_on=db.is_auto_update_stats_async_on,
       @is_clustered=CAST(i.[type] AS bit),
       @has_non_clustered=ISNULL(nc.has_non_clustered, 0),
       @is_partitioned=(CASE WHEN ds.[type]=''PS'' THEN 1 ELSE 0 END),
       @compression=part.data_compression_desc
FROM sys.indexes AS i
LEFT JOIN sys.stats AS s ON i.[object_id]=s.[object_id] AND s.[name]=i.[name]
INNER JOIN sys.data_spaces AS ds ON i.data_space_id=ds.data_space_id
OUTER APPLY (
    SELECT TOP (1) CAST(1 AS bit) AS has_non_clustered
    FROM sys.indexes
    WHERE [object_id]=i.[object_id]
      AND i.index_id>1) AS nc
LEFT JOIN sys.partitions AS part ON part.[object_id]=i.[object_id] AND part.index_id=i.index_id AND part.partition_number=1
INNER JOIN sys.databases AS db ON db.database_id=DB_ID()
WHERE i.[object_id]=OBJECT_ID('+QUOTENAME(QUOTENAME(OBJECT_SCHEMA_NAME(@object_id))+N'.'+QUOTENAME(OBJECT_NAME(@object_id)), N'''')+N')
  AND i.index_id IN (0, 1);

-------------------------------------------------------------------------------
--- Start the loop

DECLARE @MaxIdentity    bigint,
        @batchStart     datetime2(7),
        @nextIteration  datetime,
        @rowcount       bigint,
        @rowcount_total bigint=0,
        @error          int,
        @consecutiveErrors int=0;

WHILE (1=1) BEGIN;
    --- When we''re done, this is how long we''ll wait until we run again.
    SELECT @batchStart=SYSDATETIME(),
           @nextIteration=DATEADD(ms, '+CAST(1000.*@Interval_seconds AS nvarchar(20))+N', SYSDATETIME()),
           @rowcount=0;
'+(CASE WHEN @Interval_rows>0 THEN N'
    --- A snapshot high-water mark for the in-memory table.
    SELECT @MaxIdentity=MAX([##ShockAbsorberIdentity##])
    FROM (
        SELECT TOP ('+CAST(@Interval_rows AS nvarchar(20))+N') [##ShockAbsorberIdentity##]
        FROM '+@table_name+N' WITH (INDEX='+@pk_index_name+N', SNAPSHOT)
        ORDER BY [##ShockAbsorberIdentity##]
        ) AS id;
' ELSE N'
    --- A snapshot high-water mark for the in-memory table.
    SELECT @MaxIdentity=MAX([##ShockAbsorberIdentity##])
    FROM '+@table_name+N' WITH (INDEX=IX_DESC, SNAPSHOT);
' END)+N'
    IF (@MaxIdentity IS NOT NULL) BEGIN;
        BEGIN TRY;
            --- Get the data from the in-memory table as of the high-water mark,
            --- then merge that data into the base table:
            WITH inmem AS (
                SELECT '+(CASE WHEN @Interval_rows>0 THEN N'TOP ('+CAST(@Interval_rows AS nvarchar(20))+N') ' ELSE N'' END)+@column_list+N'
                FROM '+@sync_func_name+N'(@MaxIdentity))

            MERGE INTO '+QUOTENAME(OBJECT_SCHEMA_NAME(@object_id))+N'.'+QUOTENAME(OBJECT_NAME(@object_id))+N' WITH (READCOMMITTED) AS tbl
            USING inmem ON
                '+(SELECT STRING_AGG(N'tbl.'+QUOTENAME([name])+N'=inmem.'+QUOTENAME([name]), N' AND ') WITHIN GROUP (ORDER BY key_ordinal) FROM @columns WHERE key_ordinal>=1)+N'

            WHEN NOT MATCHED BY TARGET THEN
                INSERT ('+(SELECT STRING_AGG(QUOTENAME([name]), N', ') WITHIN GROUP (ORDER BY column_id) FROM @columns)+N')
                VALUES ('+(SELECT STRING_AGG(N'inmem.'+QUOTENAME([name]), N', ') WITHIN GROUP (ORDER BY column_id) FROM @columns)+N')

            WHEN MATCHED THEN
                UPDATE SET '+(SELECT STRING_AGG(N'tbl.'+QUOTENAME([name])+N'=inmem.'+QUOTENAME([name]), N', ') WITHIN GROUP (ORDER BY column_id) FROM @columns WHERE key_ordinal=0)+N'
                
            OPTION (MERGE JOIN, MAXDOP '+CAST(@Maxdop AS nvarchar(10))+N');

            --- Collect statistics:
            SELECT @error=@@ERROR, @rowcount=@@ROWCOUNT;

            --- If any data was moved, clear those rows from the in-memory table, up to the high-water mark:
            IF (@error=0 AND @rowcount>0)
                DELETE FROM '+@table_name+N' WITH (SNAPSHOT)
                WHERE [##ShockAbsorberIdentity##]<=@MaxIdentity;

            --- All good - reset the error count:
            SET @consecutiveErrors=0;
        END TRY
        BEGIN CATCH;
            IF (@PrintStats=1)
                PRINT CONVERT(varchar(100), @nextIteration, 121)+'' ** error **'';

            SET @consecutiveErrors=@consecutiveErrors+1;
            IF (@consecutiveErrors>=@MaxErrors) BEGIN;
                THROW;
                RETURN;
            END;
        END CATCH;
    END;

    SET @rowcount_total = @rowcount_total+@rowcount;

    IF (@batchStart<SYSDATETIME())
        INSERT INTO ShockAbsorber.Perfstats (Batch, [Table], [date], [rowcount], duration, fill_factor, is_auto_update_stats_on, is_auto_update_stats_async_on, is_clustered, has_non_clustered, is_partitioned, [compression])
        VALUES (@Batch,
                N'+QUOTENAME(OBJECT_SCHEMA_NAME(@object_id)+N'.'+OBJECT_NAME(@object_id), N'''')+N',
                SYSDATETIME(),
                @rowcount,
                DATEDIFF(ms, @batchStart, SYSDATETIME()),
                @fill_factor,
                @is_auto_update_stats_on,
                @is_auto_update_stats_async_on,
                @is_clustered,
                @has_non_clustered,
                @is_partitioned,
                @compression);

    IF (@PrintStats=1)
        PRINT CONVERT(varchar(100), @nextIteration, 121)+STR(@rowcount, 12, 0)+'' rows,''+STR(DATEDIFF(ms, @batchStart, SYSDATETIME()), 7, 0)+'' ms.'';

    IF (@runOnlyOnce=1)
        RETURN;

    IF (@rowcount_total>@MaxRows) BEGIN;
        PRINT ''Max number of rows reached.'';
        RETURN;
    END;

    IF (@rowcount=0 AND DATEADD(ms, 50, SYSDATETIME())<@nextIteration)
        WAITFOR TIME @nextIteration;
END;
';

PRINT @sql;
EXECUTE sys.sp_executesql @sql;

GO
