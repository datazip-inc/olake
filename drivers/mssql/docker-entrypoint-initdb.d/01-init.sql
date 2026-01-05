IF DB_ID('olake_mssql') IS NULL
BEGIN
    CREATE DATABASE olake_mssql;
END;
GO

USE olake_mssql;
GO

-- Enable CDC at database level (required before table-level CDC)
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'olake_mssql' AND is_cdc_enabled = 0)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END;
GO


-- 1) Sample table with primary key and identity (playground for snapshot, incremental and CDC)
IF OBJECT_ID('dbo.users', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.users (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        email NVARCHAR(255) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

-- 2) Simple events table without primary key (ROW_NUMBER fallback for snapshot/backfill tests)
IF OBJECT_ID('dbo.events', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.events (
        event_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        payload NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

-- Seed data
INSERT INTO dbo.users (email)
VALUES ('user1@example.com'),
       ('user2@example.com'),
       ('user3@example.com');
GO

INSERT INTO dbo.events (payload)
VALUES (N'{"type":"click"}'),
       (N'{"type":"view"}');
GO

-------------------------------------------------------------------------------
-- Enable CDC for tables
-------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.users'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'users',
        @role_name     = NULL,
        @supports_net_changes = 0;
END;
GO
