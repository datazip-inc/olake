# Testing Guide: PostgreSQL CDC LSN Mismatch Handling

This guide provides comprehensive instructions for testing the new `on_lsn_mismatch` configuration option for PostgreSQL CDC in OLake.

---

## Table of Contents
1. [Overview](#overview)
2. [Test Environment Setup](#test-environment-setup)
3. [Configuration Validation Tests](#configuration-validation-tests)
4. [LSN Mismatch Simulation Tests](#lsn-mismatch-simulation-tests)
5. [Integration Tests](#integration-tests)
6. [Troubleshooting](#troubleshooting)

---

## Overview

### What is LSN Mismatch?
LSN (Log Sequence Number) mismatch occurs when the LSN stored in OLake's state (from previous sync) doesn't match the current confirmed flush LSN in the PostgreSQL replication slot. This can happen when:
- The replication slot is advanced externally
- State data is restored from an older backup
- The replication slot is recreated
- The database is restored to an earlier point in time

### New Configuration Option
The `on_lsn_mismatch` field controls behavior when LSN mismatch is detected:

| Value | Behavior | Use Case | Data Safety |
|-------|----------|----------|-------------|
| `"fail"` | Fail sync and request clear destination | **Iceberg destinations** | ✅ No duplicates |
| `"full_load"` | Perform full load with new LSN | **S3/Parquet destinations** | ⚠️ May cause duplicates |

**Default**: `"fail"` (backward compatible)

---

## Test Environment Setup

### Prerequisites
- Docker and Docker Compose installed
- Go 1.24.0 or higher
- PostgreSQL client tools (optional, for manual verification)

### Step 1: Start PostgreSQL Test Environment

```bash
cd drivers/postgres
docker compose up -d
```

This starts:
- PostgreSQL 15 on port 5433
- Database: `postgres`
- User: `postgres` / Password: `secret1234`
- Replication slot: `olake_slot`
- Publication: `olake_publication`

### Step 2: Verify PostgreSQL is Running

```bash
docker compose ps
```

Expected output: PostgreSQL container is running and healthy

### Step 3: Verify Replication Slot

```bash
docker compose exec postgres psql -U postgres -c "SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;"
```

Expected output:
```
   slot_name   |  plugin  | slot_type | active
---------------+----------+-----------+--------
 olake_slot    | pgoutput | logical   | f
```

---

## Configuration Validation Tests

These tests verify that the configuration parsing and validation work correctly.

### Test 1: Default Behavior (No Field Specified)

**Purpose**: Verify backward compatibility - default to "fail" when field is not specified

**Configuration**: `drivers/postgres/test-configs/source-cdc-default.json`

```bash
cd drivers/postgres
go run . check --config test-configs/source-cdc-default.json
```

**Expected Output**:
```
INFO CDC on_lsn_mismatch behavior set to: fail
INFO {"connectionStatus":{"status":"SUCCEEDED"},"type":"CONNECTION_STATUS"}
```

**Verification**:
- ✅ Default correctly applied to "fail"
- ✅ Connection succeeds
- ✅ No errors

---

### Test 2: Explicit "fail" Configuration

**Purpose**: Verify explicit "fail" value is accepted

**Configuration**: `drivers/postgres/test-configs/source-cdc-fail.json`

```bash
cd drivers/postgres
go run . check --config test-configs/source-cdc-fail.json
```

**Expected Output**:
```
INFO CDC on_lsn_mismatch behavior set to: fail
INFO {"connectionStatus":{"status":"SUCCEEDED"},"type":"CONNECTION_STATUS"}
```

**Verification**:
- ✅ "fail" value accepted
- ✅ Connection succeeds
- ✅ No errors

---

### Test 3: "full_load" Configuration

**Purpose**: Verify "full_load" value is accepted

**Configuration**: `drivers/postgres/test-configs/source-cdc-full-load.json`

```bash
cd drivers/postgres
go run . check --config test-configs/source-cdc-full-load.json
```

**Expected Output**:
```
INFO CDC on_lsn_mismatch behavior set to: full_load
INFO {"connectionStatus":{"status":"SUCCEEDED"},"type":"CONNECTION_STATUS"}
```

**Verification**:
- ✅ "full_load" value accepted
- ✅ Connection succeeds
- ✅ No errors

---

### Test 4: Invalid Value Validation

**Purpose**: Verify invalid values are rejected with clear error messages

**Configuration**: Create temporary invalid config

```bash
cat > /tmp/test-invalid.json <<'EOF'
{
  "host": "localhost",
  "port": 5433,
  "database": "postgres",
  "username": "postgres",
  "password": "secret1234",
  "update_method": {
    "type": "CDC",
    "replication_slot": "olake_slot",
    "publication": "olake_publication",
    "on_lsn_mismatch": "invalid_value"
  }
}
EOF

cd drivers/postgres
go run . check --config /tmp/test-invalid.json
```

**Expected Output**:
```
INFO {"connectionStatus":{"message":"invalid on_lsn_mismatch value 'invalid_value': must be 'fail' or 'full_load'","status":"FAILED"},"type":"CONNECTION_STATUS"}
```

**Verification**:
- ✅ Invalid value rejected
- ✅ Clear error message
- ✅ Connection fails as expected

---

## LSN Mismatch Simulation Tests

These tests simulate actual LSN mismatch scenarios to verify the runtime behavior.

### Prerequisites for LSN Mismatch Tests

1. **Create test data in PostgreSQL**:

```bash
docker compose exec postgres psql -U postgres <<'EOF'
-- Create test table
CREATE TABLE IF NOT EXISTS test_lsn_mismatch (
    id SERIAL PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Add table to publication
ALTER PUBLICATION olake_publication ADD TABLE test_lsn_mismatch;

-- Insert initial data
INSERT INTO test_lsn_mismatch (data)
SELECT 'test_data_' || i
FROM generate_series(1, 100) i;
EOF
```

2. **Run initial CDC sync** to establish LSN baseline:

```bash
# Use OLake CLI or UI to perform initial CDC sync
# This establishes the baseline LSN in the state
```

### Test 5: LSN Mismatch with "fail" Configuration

**Purpose**: Verify sync fails when LSN mismatch occurs with "fail" setting

**Steps**:

1. **Advance the replication slot externally** (simulate LSN mismatch):
```bash
docker compose exec postgres psql -U postgres <<'EOF'
-- Advance replication slot to current WAL position
SELECT pg_replication_slot_advance('olake_slot', pg_current_wal_lsn());
EOF
```

2. **Run CDC sync with "fail" configuration**:
```bash
cd drivers/postgres
# Run sync command (exact command depends on your setup)
# Use config: test-configs/source-cdc-fail.json
```

**Expected Behavior**:
- ❌ Sync fails with error: `"lsn mismatch, please proceed with clear destination..."`
- ⚠️ Log shows: `"LSN mismatch detected. Stored LSN [X], Current LSN [Y]"`
- ✅ No data is synced (prevents duplicates)

**Verification**:
- Check logs for LSN mismatch warning
- Verify sync failed with appropriate error message
- Confirm no duplicate data in destination

---

### Test 6: LSN Mismatch with "full_load" Configuration

**Purpose**: Verify full load occurs when LSN mismatch happens with "full_load" setting

**Steps**:

1. **Advance the replication slot externally** (if not already advanced):
```bash
docker compose exec postgres psql -U postgres <<'EOF'
SELECT pg_replication_slot_advance('olake_slot', pg_current_wal_lsn());
EOF
```

2. **Run CDC sync with "full_load" configuration**:
```bash
cd drivers/postgres
# Run sync command with config: test-configs/source-cdc-full-load.json
```

**Expected Behavior**:
- ✅ Sync succeeds (does not fail)
- ⚠️ Log shows: `"LSN mismatch detected. Stored LSN [X], Current LSN [Y]"`
- ⚠️ Log shows: `"on_lsn_mismatch is set to 'full_load'. Starting full load with new LSN. This may result in duplicate data."`
- ✅ Full load is performed with the new LSN
- ⚠️ May result in duplicate data if previous sync was partial

**Verification**:
- Check logs for LSN mismatch warning and full_load behavior message
- Verify sync succeeded
- Confirm data is present in destination (may include duplicates)

---

## Integration Tests

### Test 7: End-to-End with S3/Parquet Destination

**Purpose**: Verify full_load behavior works correctly with S3/Parquet destination

**Configuration**:
- Source: PostgreSQL with `"on_lsn_mismatch": "full_load"`
- Destination: S3 with Parquet format

**Steps**:
1. Configure source with `test-configs/source-cdc-full-load.json`
2. Configure S3/Parquet destination
3. Run initial sync
4. Advance replication slot (simulate LSN mismatch)
5. Run CDC sync again

**Expected Result**:
- ✅ Initial sync succeeds
- ✅ Second sync succeeds despite LSN mismatch
- ✅ Warning logged about potential duplicates
- ⚠️ Data may contain duplicates (acceptable for S3/Parquet)

---

### Test 8: End-to-End with Iceberg Destination

**Purpose**: Verify fail behavior protects Iceberg from duplicates

**Configuration**:
- Source: PostgreSQL with `"on_lsn_mismatch": "fail"` (or default)
- Destination: Iceberg

**Steps**:
1. Configure source with `test-configs/source-cdc-fail.json` (or default)
2. Configure Iceberg destination
3. Run initial sync
4. Advance replication slot (simulate LSN mismatch)
5. Run CDC sync again

**Expected Result**:
- ✅ Initial sync succeeds
- ❌ Second sync fails with LSN mismatch error
- ✅ No duplicate data in Iceberg
- ✅ User instructed to clear destination

---

## Troubleshooting

### Issue: "replication slot not found"

**Solution**:
```bash
docker compose exec postgres psql -U postgres <<'EOF'
SELECT pg_create_logical_replication_slot('olake_slot', 'pgoutput');
EOF
```

### Issue: "publication not found"

**Solution**:
```bash
docker compose exec postgres psql -U postgres <<'EOF'
CREATE PUBLICATION olake_publication FOR ALL TABLES;
EOF
```

### Issue: Cannot connect to PostgreSQL

**Solution**:
1. Verify PostgreSQL is running: `docker compose ps`
2. Check logs: `docker compose logs postgres`
3. Verify port is available: `lsof -i :5433`
4. Restart if needed: `docker compose restart postgres`

### Issue: LSN mismatch not occurring in tests

**Solution** (Manual LSN advancement):
```bash
# Get current LSN
docker compose exec postgres psql -U postgres -c "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'olake_slot';"

# Advance slot
docker compose exec postgres psql -U postgres -c "SELECT pg_replication_slot_advance('olake_slot', pg_current_wal_lsn());"

# Verify advancement
docker compose exec postgres psql -U postgres -c "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'olake_slot';"
```

### Issue: Need to reset test environment

**Solution**:
```bash
# Stop and remove containers
docker compose down -v

# Restart fresh
docker compose up -d

# Recreate replication slot and publication
docker compose exec postgres psql -U postgres <<'EOF'
SELECT pg_create_logical_replication_slot('olake_slot', 'pgoutput');
CREATE PUBLICATION olake_publication FOR ALL TABLES;
EOF
```

---

## Test Checklist

Use this checklist to track your testing progress:

### Configuration Validation
- [ ] Test 1: Default behavior (no field)
- [ ] Test 2: Explicit "fail" value
- [ ] Test 3: "full_load" value
- [ ] Test 4: Invalid value rejection

### LSN Mismatch Simulation
- [ ] Test 5: Mismatch with "fail" config
- [ ] Test 6: Mismatch with "full_load" config

### Integration Tests
- [ ] Test 7: E2E with S3/Parquet
- [ ] Test 8: E2E with Iceberg

### Code Quality
- [ ] Build check: `go build ./...`
- [ ] Format check: `gofmt -l -s`
- [ ] Linting: `golangci-lint run`

---

## Additional Resources

- **Test Configurations**: `drivers/postgres/test-configs/`
- **Test Results Log**: `drivers/postgres/TEST_RESULTS.md`
- **PostgreSQL CDC Documentation**: OLake docs (olake.io/docs)

---

## Notes for Reviewers

1. **Backward Compatibility**: All tests verify that default behavior matches the original implementation
2. **Safety First**: The default "fail" behavior prevents accidental data duplication
3. **Clear Warnings**: When "full_load" is used, clear warnings are logged about potential duplicates
4. **Production-Ready**: All edge cases (invalid values, missing fields) are handled with appropriate error messages

---

**Last Updated**: 2026-01-16
**Status**: All configuration validation tests passed ✅
