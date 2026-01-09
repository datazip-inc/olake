# MSSQL Source Connector

This driver provides a Microsoft SQL Server source for OLake, implementing:

- Full refresh backfills with chunked snapshotting
- Incremental sync using cursor fields
- Native SQL Server CDC using `fn_cdc_get_all_changes_*` with LSN tracking (and optional change tracking)


