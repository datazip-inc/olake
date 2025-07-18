# OLake MySQL Connector Enhancement Report

## Project Overview

OLake is an open-source data replication tool designed to efficiently replicate data from various database sources to Apache Iceberg and other destinations. It's particularly focused on high-performance data ingestion with support for both full refresh and Change Data Capture (CDC) modes.

### Core Components:

1. **Driver Architecture**: 
   - Modular design with source connectors (drivers) for databases like PostgreSQL, MySQL, and MongoDB
   - Destination writers for Apache Iceberg, Parquet files, etc.
   - Abstract interfaces that define common behavior across drivers and destinations

2. **Command Structure**:
   - `spec`: Returns JSON Schema for connector configuration
   - `check`: Validates connection configurations
   - `discover`: Identifies available streams (tables) and their schemas
   - `sync`: Executes data replication jobs

3. **Key Features**:
   - Full refresh and CDC synchronization
   - Schema evolution support
   - Parallel processing for improved performance
   - Multiple catalog support for Iceberg (AWS Glue, Hive, JDBC, REST)
   - Partitioning and data type conversion

## Added Features

### 1. JDBC URL Parameters Support

Added the ability to specify custom JDBC URL parameters for MySQL connections, allowing users to fine-tune their connection settings without modifying the code. This feature enables:

- Setting connection timeouts
- Configuring character encoding
- Adjusting socket timeouts
- Setting server timezone
- Enabling/disabling SSL
- And many other MySQL-specific connection parameters

### 2. Enhanced SSL Configuration

Implemented a comprehensive SSL configuration system for MySQL connections with multiple security levels:

- **Simple SSL**: Basic SSL support with the option to skip certificate verification
- **Certificate-based Authentication**: Support for client certificates and CA verification
- **Multiple Security Modes**:
  - `disable`: No SSL
  - `require`: Use SSL without verification
  - `verify-ca`: Verify server certificate against trusted CA
  - `verify-full`: Verify both certificate and hostname

## Cursor Workflow

The cursor workflow in this project was particularly helpful for:

1. **Navigating the Codebase**:
   - Quickly finding related files and dependencies
   - Understanding the structure of the MySQL connector
   - Identifying existing SSL implementations in other parts of the project

2. **Code Modification**:
   - Efficiently editing the `Config` struct to add new fields
   - Implementing the `URI()` method to incorporate new parameters
   - Adding validation logic for the new configuration options

3. **Testing**:
   - Creating comprehensive test cases for the new features
   - Validating different SSL modes and JDBC parameter combinations

## Before/After Code Snippets

### Config Struct

**Before:**
```go
type Config struct {
    Host          string      `json:"hosts"`
    Username      string      `json:"username"`
    Password      string      `json:"password"`
    Database      string      `json:"database"`
    Port          int         `json:"port"`
    TLSSkipVerify bool        `json:"tls_skip_verify"` 
    UpdateMethod  interface{} `json:"update_method"`
    MaxThreads    int         `json:"max_threads"`
    RetryCount    int         `json:"backoff_retry_count"`
}
```

**After:**
```go
type Config struct {
    Host          string            `json:"hosts"`
    Username      string            `json:"username"`
    Password      string            `json:"password"`
    Database      string            `json:"database"`
    Port          int               `json:"port"`
    TLSSkipVerify bool              `json:"tls_skip_verify"`
    UpdateMethod  interface{}       `json:"update_method"`
    MaxThreads    int               `json:"max_threads"`
    RetryCount    int               `json:"backoff_retry_count"`
    JDBCURLParams map[string]string `json:"jdbc_url_params,omitempty"` // Custom connection parameters
    SSLConfig     *utils.SSLConfig  `json:"ssl_config,omitempty"`      // SSL configuration
}
```

### URI Method

**Before:**
```go
func (c *Config) URI() string {
    // Set default port if not specified
    if c.Port == 0 {
        c.Port = 3306
    }
    // Construct host string
    hostStr := c.Host
    if c.Host == "" {
        hostStr = "localhost"
    }

    cfg := mysql.Config{
        User:                 c.Username,
        Passwd:               c.Password,
        Net:                  "tcp",
        Addr:                 fmt.Sprintf("%s:%d", hostStr, c.Port),
        DBName:               c.Database,
        AllowNativePasswords: true,
    }

    return cfg.FormatDSN()
}
```

**After:**
```go
func (c *Config) URI() string {
    // Set default port if not specified
    if c.Port == 0 {
        c.Port = 3306
    }
    // Construct host string
    hostStr := c.Host
    if c.Host == "" {
        hostStr = "localhost"
    }

    cfg := mysql.Config{
        User:                 c.Username,
        Passwd:               c.Password,
        Net:                  "tcp",
        Addr:                 fmt.Sprintf("%s:%d", hostStr, c.Port),
        DBName:               c.Database,
        AllowNativePasswords: true,
    }

    // Apply TLS configuration
    if c.SSLConfig != nil {
        switch c.SSLConfig.Mode {
        case utils.SSLModeDisable:
            cfg.TLSConfig = "false"
        case utils.SSLModeRequire:
            cfg.TLSConfig = "true"
        case utils.SSLModeVerifyCA, utils.SSLModeVerifyFull:
            // Register a custom TLS config with the MySQL driver
            tlsConfigName := fmt.Sprintf("custom-tls-%s-%d", c.Host, c.Port)
            err := registerTLSConfig(tlsConfigName, c.SSLConfig)
            if err == nil {
                cfg.TLSConfig = tlsConfigName
            }
        }
    } else if c.TLSSkipVerify {
        cfg.TLSConfig = "skip-verify"
    }

    // Apply custom JDBC URL parameters
    if c.JDBCURLParams != nil && len(c.JDBCURLParams) > 0 {
        params := make(map[string]string)
        for k, v := range c.JDBCURLParams {
            params[k] = v
        }
        cfg.Params = params
    }

    return cfg.FormatDSN()
}
```

### Configuration Example

**Before:**
```json
{
  "hosts": "mysql-host",
  "username": "mysql-user",
  "password": "mysql-password",
  "database": "mysql-database",
  "port": 3306,
  "update_method": {
    "intial_wait_time": 10
   },
  "tls_skip_verify": true,
  "max_threads": 10,
  "backoff_retry_count": 2
}
```

**After:**
```json
{
  "hosts": "mysql-host",
  "username": "mysql-user",
  "password": "mysql-password",
  "database": "mysql-database",
  "port": 3306,
  "update_method": {
    "intial_wait_time": 10
   },
  "tls_skip_verify": true,
  "max_threads": 10,
  "backoff_retry_count": 2,
  "jdbc_url_params": {
    "connectTimeout": "30000",
    "socketTimeout": "30000",
    "useSSL": "true"
  },
  "ssl_config": {
    "mode": "verify-ca",
    "server_ca": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
    "client_cert": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
    "client_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----"
  }
}
```

## Conclusion

The enhancements to the MySQL connector significantly improve the flexibility and security of the OLake data replication tool. By adding support for JDBC URL parameters and comprehensive SSL configuration options, users can now:

1. Fine-tune their MySQL connections for optimal performance
2. Establish secure connections with various levels of certificate validation
3. Meet enterprise security requirements for data replication
4. Customize connection behavior without modifying the codebase

These features align with OLake's mission to provide high-performance, secure, and flexible data replication capabilities across various database sources and destinations. 