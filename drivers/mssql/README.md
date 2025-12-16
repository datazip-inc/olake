# MSSQL Source Connector

This driver provides a Microsoft SQL Server source for OLake, implementing:

- Full refresh backfills with chunked snapshotting
- Incremental sync using cursor fields
- Native SQL Server CDC using `fn_cdc_get_all_changes_*` with LSN tracking (and optional change tracking)

It follows the same patterns as the existing MySQL and Postgres drivers.

## Troubleshooting

### SQL Server Container Won't Start on macOS

If you encounter the error:
```
/opt/mssql/bin/sqlservr: Invalid mapping of address 0x2aaaacc23000 in reserved address space below 0x400000000000
```

This is a known limitation on macOS due to Virtual Address (VA) layout restrictions. SQL Server requires specific memory layout that conflicts with macOS security restrictions.

**Workarounds**:
1. **Use a Linux VM or remote SQL Server**: Run MSSQL in a Linux environment or connect to a remote SQL Server instance
2. **Use Docker Desktop with Linux containers**: Ensure you're using Linux containers, not macOS containers
3. **Use Azure SQL Database**: Connect to a cloud SQL Server instance for testing

The connector code is correct and will work properly on Linux/Windows environments.



