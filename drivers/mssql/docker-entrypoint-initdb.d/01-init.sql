-- Initialise test database and enable CDC
IF DB_ID('olake_mssql') IS NULL
BEGIN
    CREATE DATABASE olake_mssql;
END;
GO

USE olake_mssql;
GO

-- Enable CDC at database level
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'olake_mssql' AND is_cdc_enabled = 0)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END;
GO

-- Sample table with primary key and identity
IF OBJECT_ID('dbo.users', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.users (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        email NVARCHAR(255) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

-- Sample table without primary key (ROW_NUMBER fallback)
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

-- Comprehensive data type test table
IF OBJECT_ID('dbo.datatype_test', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.datatype_test (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        -- Integer types
        col_tinyint TINYINT NOT NULL DEFAULT 127,
        col_smallint SMALLINT NOT NULL DEFAULT 32767,
        col_int INT NOT NULL DEFAULT 2147483647,
        col_bigint BIGINT NOT NULL DEFAULT 9223372036854775807,
        -- Exact numeric
        col_decimal DECIMAL(18,2) NOT NULL DEFAULT 9999999999999999.99,
        col_numeric NUMERIC(10,5) NOT NULL DEFAULT 12345.67890,
        col_smallmoney SMALLMONEY NOT NULL DEFAULT $214748.3647,
        col_money MONEY NOT NULL DEFAULT $922337203685477.5807,
        -- Approximate numeric
        col_float FLOAT NOT NULL DEFAULT 3.141592653589793,
        col_real REAL NOT NULL DEFAULT 2.71828,
        -- Bit / boolean
        col_bit BIT NOT NULL DEFAULT 1,
        -- Character strings
        col_char CHAR(10) NOT NULL DEFAULT 'CHAR      ',
        col_varchar VARCHAR(255) NOT NULL DEFAULT 'VARCHAR string',
        col_text TEXT NULL,
        col_nchar NCHAR(10) NOT NULL DEFAULT N'NCHAR     ',
        col_nvarchar NVARCHAR(255) NOT NULL DEFAULT N'NVARCHAR string',
        col_ntext NTEXT NULL,
        -- Binary
        col_binary BINARY(16) NOT NULL DEFAULT 0x0123456789ABCDEF0123456789ABCDEF,
        col_varbinary VARBINARY(MAX) NULL,
        -- Date and time
        col_date DATE NOT NULL DEFAULT '2025-11-28',
        col_smalldatetime SMALLDATETIME NOT NULL DEFAULT '2025-11-28 12:30:00',
        col_datetime DATETIME NOT NULL DEFAULT '2025-11-28 12:30:45.123',
        col_datetime2 DATETIME2 NOT NULL DEFAULT '2025-11-28 12:30:45.1234567',
        col_datetimeoffset DATETIMEOFFSET NOT NULL DEFAULT '2025-11-28 12:30:45.1234567 +05:30',
        col_time TIME NOT NULL DEFAULT '12:30:45.1234567',
        -- Unique identifier
        col_uniqueidentifier UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        -- Nullable columns
        col_int_nullable INT NULL,
        col_varchar_nullable VARCHAR(255) NULL,
        col_datetime2_nullable DATETIME2 NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

-- Insert test data for datatype_test table
INSERT INTO dbo.datatype_test (
    col_text, col_ntext, col_varbinary, col_int_nullable, col_varchar_nullable, col_datetime2_nullable
)
VALUES (
    'This is a TEXT field',
    N'This is an NTEXT field',
    0x48656C6C6F576F726C64,
    42,
    'Nullable VARCHAR',
    '2025-11-28 15:30:00'
);
GO

-- Enable CDC for tables
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.users'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'users',
        @role_name     = NULL,
        @supports_net_changes = 0;
END;
GO

-- Enable CDC for datatype_test table
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.datatype_test'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'datatype_test',
        @role_name     = NULL,
        @supports_net_changes = 0;
END;
GO



