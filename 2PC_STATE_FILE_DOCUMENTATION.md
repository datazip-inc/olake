# 2PC State File Documentation

This document describes how the state file looks after 2PC (Two-Phase Commit) implementation across all sync modes (Full Refresh, Incremental, and CDC) for different drivers.

## Table of Contents
1. [Newly Introduced 2PC Fields](#newly-introduced-2pc-fields)
2. [Full Refresh Mode](#full-refresh-mode)
3. [Incremental Mode](#incremental-mode)
4. [CDC Mode](#cdc-mode)
   - [MySQL](#mysql-cdc)
   - [MongoDB](#mongodb-cdc)
   - [Postgres](#postgres-cdc)

---

## Newly Introduced 2PC Fields

The 2PC implementation introduces several new fields in the state file to enable atomic commits and recovery. These fields track the state before commit and allow the system to recover gracefully if a sync fails.

### Full Refresh Mode

**New Field:**
- **`chunks[].status`** (string, optional): Status of a chunk being processed
  - Values: `"preparing"` (chunk is being processed, not yet committed)
  - Purpose: Tracks which chunks are in-flight to enable recovery on failure
  - Location: `streams[].state.chunks[].status`

### Incremental Mode

**New Fields:**
- **`olake_next_cursor_{cursor_field}`** (any, optional): Next cursor value before commit
  - Format: `olake_next_cursor_{primary_cursor_field}` and `olake_next_cursor_{secondary_cursor_field}` (if secondary cursor exists)
  - Purpose: Stores the cursor position reached during sync before commit, enabling recovery
  - Location: `streams[].state.olake_next_cursor_*`
  - Example: If cursor field is `id`, the field is `olake_next_cursor_id`

### CDC Mode - MySQL

**New Fields:**
- **`next_cdc_pos`** (string, optional): Binlog position before writers commit
  - Format: `"{filename}:{position}"` (e.g., `"mysql-bin.000070:800000"`)
  - Purpose: Stores the CDC position reached during sync before commit, used for bounded recovery sync
  - Location: `global.state.next_cdc_pos`

- **`processing`** (array of strings, optional): Stream IDs currently being processed (not yet committed)
  - Format: Array of stream IDs (e.g., `["public.table_1", "public.table_2"]`)
  - Purpose: Tracks which streams are in-flight during CDC sync, enabling recovery for specific streams
  - Location: `global.state.processing`

### CDC Mode - MongoDB

**New Field:**
- **`next_data`** (string, optional): Resume token before commit
  - Format: MongoDB resume token string (e.g., `"82673F82FE000000022B0429296E1505"`)
  - Purpose: Stores the resume token reached during sync before commit, enabling per-stream recovery
  - Location: `streams[].state.next_data`
  - Note: This is per-stream (each stream has its own `next_data`)

### CDC Mode - Postgres

**New Fields:**
- **`next_cdc_pos`** (string, optional): LSN position before writers commit
  - Format: LSN string (e.g., `"2D9/AD005000"`)
  - Purpose: Stores the LSN position reached during sync before commit, used for bounded recovery sync
  - Location: `global.state.next_cdc_pos`

- **`processing`** (array of strings, optional): Stream IDs currently being processed (not yet committed)
  - Format: Array of stream IDs (e.g., `["public.table_1", "public.table_2"]`)
  - Purpose: Tracks which streams are in-flight during CDC sync, enabling recovery for specific streams
  - Location: `global.state.processing`

### Field Lifecycle

All 2PC fields follow a similar lifecycle:

1. **Before Commit**: Temporary fields are set when data is being written but not yet committed
2. **After Successful Commit**: Temporary fields are cleared, and main position/cursor fields are updated
3. **On Failure**: Temporary fields remain in state for recovery
4. **On Recovery**: System checks commit status:
   - If committed: Temporary fields are moved to main fields and cleared
   - If not committed: Temporary fields are cleared (rollback), and sync retries from last committed position

### Important Notes

- All 2PC fields use `omitempty` JSON tag, so they are omitted from JSON when empty/not set
- These fields are **temporary** and should not persist after successful commits
- Presence of these fields indicates a potential recovery scenario
- For global position drivers (MySQL/Postgres), `processing` array enables bounded sync (only sync uncommitted streams up to `next_cdc_pos`)
- For per-stream drivers (MongoDB), each stream's recovery is independent (no bounded sync needed)

---

## Full Refresh Mode

Full Refresh mode uses **chunks** to track progress. Each chunk represents a range of data (min-max values) that needs to be synced. The state tracks chunk status to enable recovery.

### State Structure

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "full_refresh",
      "state": {
        "chunks": [
          {
            "min": 1,
            "max": 1000,
            "status": "preparing"
          },
          {
            "min": 1001,
            "max": 2000
          },
          {
            "min": 2001,
            "max": 3000
          }
        ]
      }
    }
  ]
}
```

### Chunk Status Values

- **No status** (omitted): Chunk not yet started
- **`"preparing"`**: Chunk is being processed (data is being written but not yet committed)

### Scenarios

#### Scenario 1: Normal Operation - Before Commit

**State:** Chunk status is set to `"preparing"` when processing starts.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "full_refresh",
      "state": {
        "chunks": [
          {
            "min": 1,
            "max": 1000,
            "status": "preparing"  // ← Chunk being processed
          },
          {
            "min": 1001,
            "max": 2000
          }
        ]
      }
    }
  ]
}
```

**What happens:**
1. Chunk (1-1000) is selected for processing
2. Status is updated to `"preparing"` in state
3. Data is written to destination
4. Writer is not yet committed

#### Scenario 2: Normal Operation - After Successful Commit

**State:** Chunk is removed from state after successful commit.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "full_refresh",
      "state": {
        "chunks": [
          {
            "min": 1001,
            "max": 2000
          }
        ]
      }
    }
  ]
}
```

**What happens:**
1. Writer commits successfully
2. Chunk (1-1000) is removed from state
3. Next chunk (1001-2000) can be processed

#### Scenario 3: Failure During Processing - Commit Failed

**State:** Chunk remains with `"preparing"` status.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "full_refresh",
      "state": {
        "chunks": [
          {
            "min": 1,
            "max": 1000,
            "status": "preparing"  // ← Chunk still in preparing state
          },
          {
            "min": 1001,
            "max": 2000
          }
        ]
      }
    }
  ]
}
```

**What happens:**
1. Sync fails before commit
2. Chunk (1-1000) remains with `"preparing"` status
3. On next run, system checks if thread is committed
4. If committed: chunk is removed
5. If not committed: chunk is retried

#### Scenario 4: Recovery - Chunk Already Committed

**State:** Chunk has `"preparing"` status but thread is already committed.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "full_refresh",
      "state": {
        "chunks": [
          {
            "min": 1,
            "max": 1000,
            "status": "preparing"  // ← Status indicates processing, but thread is committed
          }
        ]
      }
    }
  ]
}
```

**What happens:**
1. On recovery, system detects chunk with `"preparing"` status
2. Checks if thread is committed using `IsThreadCommitted()`
3. If committed: chunk is removed (skip processing)
4. If not committed: chunk is retried

#### Scenario 5: All Chunks Completed

**State:** Empty chunks array indicates backfill is complete.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "full_refresh",
      "state": {
        "chunks": []  // ← Empty array = backfill complete
      }
    }
  ]
}
```

**What happens:**
1. All chunks have been processed and committed
2. `HasCompletedBackfill()` returns `true`
3. Stream can proceed to CDC (if applicable)

---

## Incremental Mode

Incremental mode uses **cursor values** to track the last synced position. The state uses `olake_next_cursor_*` fields for 2PC recovery.

### State Structure

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "incremental",
      "state": {
        "id": 100,
        "olake_next_cursor_id": 150,
        "created_at": "2024-01-15T10:30:00Z",
        "olake_next_cursor_created_at": "2024-01-15T11:00:00Z"
      }
    }
  ]
}
```

### Cursor Field Naming

- **Primary cursor**: `{cursor_field_name}` (e.g., `"id"`)
- **Secondary cursor** (if exists): `{secondary_cursor_field_name}` (e.g., `"created_at"`)
- **Next cursor (2PC)**: `olake_next_cursor_{cursor_field_name}` (e.g., `"olake_next_cursor_id"`)

### Scenarios

#### Scenario 1: Normal Operation - Before Commit

**State:** Current cursor values are preserved, `olake_next_cursor_*` fields store the new position.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "incremental",
      "state": {
        "id": 100,                          // ← Last committed cursor
        "olake_next_cursor_id": 150,         // ← New position (before commit)
        "created_at": "2024-01-15T10:30:00Z",
        "olake_next_cursor_created_at": "2024-01-15T11:00:00Z"
      }
    }
  ]
}
```

**What happens:**
1. Sync starts from cursor `id=100`
2. Processes records up to `id=150`
3. Before commit, saves `olake_next_cursor_id=150` in state
4. Writer is not yet committed

#### Scenario 2: Normal Operation - After Successful Commit

**State:** `olake_next_cursor_*` fields are moved to main cursor fields and then deleted.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "incremental",
      "state": {
        "id": 150,                          // ← Updated from olake_next_cursor_id
        "created_at": "2024-01-15T11:00:00Z" // ← Updated from olake_next_cursor_created_at
        // olake_next_cursor_* fields are deleted
      }
    }
  ]
}
```

**What happens:**
1. Writer commits successfully
2. `olake_next_cursor_id` → `id` (150)
3. `olake_next_cursor_created_at` → `created_at` (2024-01-15T11:00:00Z)
4. `olake_next_cursor_*` fields are deleted
5. Next sync starts from `id=150`

#### Scenario 3: Failure During Processing - Commit Failed

**State:** `olake_next_cursor_*` fields remain in state.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "incremental",
      "state": {
        "id": 100,                          // ← Last committed position
        "olake_next_cursor_id": 150,         // ← Next position (commit failed)
        "created_at": "2024-01-15T10:30:00Z",
        "olake_next_cursor_created_at": "2024-01-15T11:00:00Z"
      }
    }
  ]
}
```

**What happens:**
1. Sync fails before commit
2. `olake_next_cursor_*` fields remain in state
3. On next run, system checks if thread is committed
4. If committed: move `olake_next_cursor_*` to main cursor fields
5. If not committed: retry from `id=100`

#### Scenario 4: Recovery - Thread Already Committed

**State:** `olake_next_cursor_*` fields exist, but thread is already committed.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "incremental",
      "state": {
        "id": 100,
        "olake_next_cursor_id": 150,         // ← Recovery: check if committed
        "created_at": "2024-01-15T10:30:00Z",
        "olake_next_cursor_created_at": "2024-01-15T11:00:00Z"
      }
    }
  ]
}
```

**What happens:**
1. On recovery, system detects `olake_next_cursor_id` exists
2. Generates thread ID from starting position (`id=100`)
3. Checks if thread is committed using `IsThreadCommitted()`
4. If committed: move `olake_next_cursor_*` to main cursor fields
5. Next sync starts from `id=150`

#### Scenario 5: Recovery - Thread Not Committed

**State:** `olake_next_cursor_*` fields exist, thread is not committed.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "incremental",
      "state": {
        "id": 100,                          // ← Retry from here
        "olake_next_cursor_id": 150,         // ← Will be deleted
        "created_at": "2024-01-15T10:30:00Z",
        "olake_next_cursor_created_at": "2024-01-15T11:00:00Z"
      }
    }
  ]
}
```

**What happens:**
1. On recovery, system detects `olake_next_cursor_id` exists
2. Generates thread ID from starting position (`id=100`)
3. Checks if thread is committed - **NOT COMMITTED**
4. Deletes `olake_next_cursor_*` fields
5. Retries sync from `id=100`

---

## CDC Mode

CDC mode tracks change data capture positions. The state structure varies by driver:

- **MySQL/Postgres**: Global position (shared across all streams)
- **MongoDB**: Per-stream positions (each stream has its own resume token)

---

### MySQL CDC

MySQL uses a **global binlog position** shared across all streams. The state tracks `next_cdc_pos` and `processing` streams for 2PC recovery.

#### State Structure

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "server_id": 1234567890,
      "state": {
        "name": "mysql-bin.000070",
        "pos": 772591
      },
      "next_cdc_pos": "mysql-bin.000070:800000",
      "processing": ["public.table_1", "public.table_2"]
    },
    "streams": ["public.table_1", "public.table_2"]
  },
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "cdc",
      "state": {
        "chunks": []
      }
    },
    {
      "stream": "table_2",
      "namespace": "public",
      "sync_mode": "cdc",
      "state": {
        "chunks": []
      }
    }
  ]
}
```

#### Field Descriptions

- **`server_id`**: MySQL server ID for binlog connection
- **`state.position`**: Current committed CDC position (format: `{filename}:{position}`)
- **`next_cdc_pos`**: Position before writers commit (for 2PC recovery)
- **`processing`**: Array of stream IDs currently being processed (not yet committed)

#### Scenarios

##### Scenario 1: Normal Operation - Before Commit

**State:** `next_cdc_pos` and `processing` are set before commit.

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "server_id": 1234567890,
      "state": {
        "name": "mysql-bin.000070",
        "pos": 772591
      },
      "next_cdc_pos": "mysql-bin.000070:800000",  // ← Position before commit
      "processing": ["public.table_1", "public.table_2"]  // ← Streams being processed
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. CDC sync starts from position `mysql-bin.000070:772591`
2. Processes changes up to `mysql-bin.000070:800000`
3. Before commit, saves `next_cdc_pos` and `processing` streams
4. Writers are not yet committed

##### Scenario 2: Normal Operation - After Successful Commit

**State:** `next_cdc_pos` and `processing` are cleared, `state.position` is updated.

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "server_id": 1234567890,
      "state": {
        "name": "mysql-bin.000070",
        "pos": 800000  // ← Updated position
      }
      // next_cdc_pos and processing are cleared
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. All writers commit successfully
2. `processing` array becomes empty
3. `next_cdc_pos` is cleared
4. `state.position` is updated to `mysql-bin.000070:800000`
5. Next sync starts from this position

##### Scenario 3: Partial Commit - Some Streams Committed

**State:** `processing` array contains only uncommitted streams.

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "server_id": 1234567890,
      "state": {
        "name": "mysql-bin.000070",
        "pos": 772591
      },
      "next_cdc_pos": "mysql-bin.000070:800000",
      "processing": ["public.table_2"]  // ← Only uncommitted stream remains
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. `public.table_1` commits successfully → removed from `processing`
2. `public.table_2` fails to commit → remains in `processing`
3. `next_cdc_pos` remains set for recovery
4. On next run, only `public.table_2` will be synced (bounded sync)

##### Scenario 4: Recovery - All Streams Committed

**State:** `processing` is empty, but `next_cdc_pos` exists (cleanup pending).

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "server_id": 1234567890,
      "state": {
        "name": "mysql-bin.000070",
        "pos": 772591
      },
      "next_cdc_pos": "mysql-bin.000070:800000"  // ← Cleanup needed
      // processing is empty
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. On recovery, system detects `processing` is empty but `next_cdc_pos` exists
2. All streams were committed in previous run
3. Updates `state.position` from `next_cdc_pos`
4. Clears `next_cdc_pos`
5. Next sync starts from `mysql-bin.000070:800000`

##### Scenario 5: Recovery - Some Streams Not Committed (Bounded Sync), and committed once not removed yet

**State:** `processing` contains uncommitted streams as well as committed but which was not yet removed, `next_cdc_pos` is set.

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "server_id": 1234567890,
      "state": {
        "name": "mysql-bin.000070",
        "pos": 772591
      },
      "next_cdc_pos": "mysql-bin.000070:800000",  // ← Target position for bounded sync
      "processing": ["public.table_1","public.table_2"]  // ← Uncommitted stream
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. On recovery, system detects `processing` contains `public.table_1` and `public.table_2`
2. Checks commit status for each stream in `processing`
3. For `public.table_1`: thread is already committed so its removed
4. For `public.table_2`: thread not committed
5. Sets target position to `next_cdc_pos` (bounded sync)
6. Syncs only `public.table_2` up to `mysql-bin.000070:800000`
7. After commit, removes from `processing` and clears `next_cdc_pos`

---

### MongoDB CDC

MongoDB uses **per-stream resume tokens**. Each stream has its own `_data` (committed position) and `next_data` (position before commit) for 2PC recovery.

#### State Structure

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "collection_1",
      "namespace": "olake_mongodb_test",
      "sync_mode": "cdc",
      "state": {
        "_data": "82673F82FE000000022B0429296E1404",
        "next_data": "82673F82FE000000022B0429296E1505"
      }
    },
    {
      "stream": "collection_2",
      "namespace": "olake_mongodb_test",
      "sync_mode": "cdc",
      "state": {
        "_data": "82673F82FE000000022B0429296E1404"
      }
    }
  ]
}
```

#### Field Descriptions

- **`_data`**: Committed resume token (last successfully committed position)
- **`next_data`**: Resume token before commit (for 2PC recovery)

#### Scenarios

##### Scenario 1: Normal Operation - Before Commit

**State:** `_data` contains start position, `next_data` is the next position.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "collection_1",
      "namespace": "olake_mongodb_test",
      "sync_mode": "cdc",
      "state": {
        "_data": "82673F82FE000000022B0429296E1404",      // ← start from
        "next_data": "82673F82FE000000022B0429296E1505"     // ← target (before commit)
      }
    }
  ]
}
```

**What happens:**
1. CDC sync starts from `_data` (resume token)
2. Processes changes and updates position
3. Before commit, saves current position as `next_data`
4. Writer is not yet committed

##### Scenario 2: Normal Operation - After Successful Commit

**State:** `next_data` is moved to `_data`, `next_data` is cleared.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "collection_1",
      "namespace": "olake_mongodb_test",
      "sync_mode": "cdc",
      "state": {
        "_data": "82673F82FE000000022B0429296E1505"  // ← Updated from next_data
        // next_data is cleared
      }
    }
  ]
}
```

**What happens:**
1. Writer commits successfully
2. `next_data` → `_data` (82673F82FE000000022B0429296E1505)
3. `next_data` is cleared (set to `null`)
4. Next sync starts from new `_data`

##### Scenario 3: Failure During Processing - Commit Failed

**State:** `next_data` remains in state.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "collection_1",
      "namespace": "olake_mongodb_test",
      "sync_mode": "cdc",
      "state": {
        "_data": "82673F82FE000000022B0429296E1404",      // ← Last committed
        "next_data": "82673F82FE000000022B0429296E1505"   // ← Commit failed
      }
    }
  ]
}
```

**What happens:**
1. Sync fails before commit
2. `next_data` remains in state
3. On next run, system checks if thread is committed
4. If committed: move `next_data` to `_data`
5. If not committed: clear `next_data` (rollback)

##### Scenario 4: Recovery - Thread Already Committed

**State:** `next_data` exists, thread is already committed.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "collection_1",
      "namespace": "olake_mongodb_test",
      "sync_mode": "cdc",
      "state": {
        "_data": "82673F82FE000000022B0429296E1404",      // ← Starting position
        "next_data": "82673F82FE000000022B0429296E1505"   // ← Recovery: check if committed
      }
    }
  ]
}
```

**What happens:**
1. On recovery, system detects `next_data` exists
2. Generates thread ID from `_data` (starting position)
3. Checks if thread is committed using `IsThreadCommitted()`
4. If committed: move `next_data` to `_data`
5. Next sync starts from new `_data`

##### Scenario 5: Recovery - Thread Not Committed set next_data to new target

**State:** `next_data` exists, thread is not committed.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "collection_1",
      "namespace": "olake_mongodb_test",
      "sync_mode": "cdc",
      "state": {
        "_data": "82673F82FE000000022B0429296E1404",      // ← Retry from here
        "next_data": "82673F82FE000000022B0429296E1505"   // ← Will be cleared
      }
    }
  ]
}
```

**What happens:**
1. On recovery, system detects `next_data` exists
2. Generates thread ID from `_data` (starting position)
3. Checks if thread is committed - **NOT COMMITTED**
4. Clears `next_data` and sync till the new available target position
5. Retries sync from `_data` (82673F82FE000000022B0429296E1404)

##### Scenario 6: Multiple Streams - Mixed States

**State:** Different streams can have different recovery states.

```json
{
  "type": "STREAM",
  "streams": [
    {
      "stream": "collection_1",
      "namespace": "olake_mongodb_test",
      "sync_mode": "cdc",
      "state": {
        "_data": "82673F82FE000000022B0429296E1404",
        "next_data": "82673F82FE000000022B0429296E1505"  // ← Needs recovery
      }
    },
    {
      "stream": "collection_2",
      "namespace": "olake_mongodb_test",
      "sync_mode": "cdc",
      "state": {
        "_data": "82673F82FE000000022B0429296E1404"  // ← No recovery needed
      }
    }
  ]
}
```

**What happens:**
1. Each stream is checked independently for `next_data`
2. `collection_1`: has `next_data` → recovery check
3. `collection_2`: no `next_data` → normal sync
4. Recovery works per-stream (no bounded sync needed)

---

### Postgres CDC

Postgres uses a **global LSN (Log Sequence Number)** shared across all streams, similar to MySQL. The state tracks `next_cdc_pos` and `processing` streams for 2PC recovery. Additionally, Postgres requires LSN acknowledgment to advance the replication slot.

#### State Structure

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "lsn": "2D9/AD00445A",
      "next_cdc_pos": "2D9/AD005000",
      "processing": ["public.table_1", "public.table_2"]
    },
    "streams": ["public.table_1", "public.table_2"]
  },
  "streams": [
    {
      "stream": "table_1",
      "namespace": "public",
      "sync_mode": "cdc",
      "state": {
        "chunks": []
      }
    },
    {
      "stream": "table_2",
      "namespace": "public",
      "sync_mode": "cdc",
      "state": {
        "chunks": []
      }
    }
  ]
}
```

#### Field Descriptions

- **`lsn`**: Current committed CDC position (LSN format: `{high}/{low}`)
- **`next_cdc_pos`**: Position before writers commit (for 2PC recovery)
- **`processing`**: Array of stream IDs currently being processed (not yet committed)

#### Scenarios

##### Scenario 1: Normal Operation - Before Commit

**State:** `next_cdc_pos` and `processing` are set before commit.

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "lsn": "2D9/AD00445A",
      "next_cdc_pos": "2D9/AD005000",  // ← Position before commit
      "processing": ["public.table_1", "public.table_2"]  // ← Streams being processed
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. CDC sync starts from LSN `2D9/AD00445A`
2. Processes changes up to LSN `2D9/AD005000`
3. Before commit, saves `next_cdc_pos` and `processing` streams
4. Writers are not yet committed
5. **LSN is NOT acknowledged to Postgres yet** (waiting for commit)

##### Scenario 2: Normal Operation - After Successful Commit

**State:** `next_cdc_pos` and `processing` are cleared, `lsn` is updated, LSN is acknowledged.

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "lsn": "2D9/AD005000"  // ← Updated position
      // next_cdc_pos and processing are cleared
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. All writers commit successfully
2. `processing` array becomes empty
3. `next_cdc_pos` is cleared
4. `lsn` is updated to `2D9/AD005000`
5. **LSN is acknowledged to Postgres** (advances replication slot)
6. Next sync starts from this LSN

##### Scenario 3: Partial Commit - Some Streams Committed

**State:** `processing` array contains only uncommitted streams.

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "lsn": "2D9/AD00445A",
      "next_cdc_pos": "2D9/AD005000",
      "processing": ["public.table_2"]  // ← Only uncommitted stream remains
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. `public.table_1` commits successfully → removed from `processing`
2. `public.table_2` fails to commit → remains in `processing`
3. `next_cdc_pos` remains set for recovery
4. **LSN is NOT acknowledged** (waiting for all streams to commit)
5. On next run, only `public.table_2` will be synced (bounded sync)

##### Scenario 4: Recovery - All Streams Committed

**State:** `processing` is empty, but `next_cdc_pos` exists (cleanup pending).

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "lsn": "2D9/AD00445A",
      "next_cdc_pos": "2D9/AD005000"  // ← Cleanup needed
      // processing is empty
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. On recovery, system detects `processing` is empty but `next_cdc_pos` exists
2. All streams were committed in previous run
3. **Acknowledges LSN to Postgres** (advances replication slot)
4. Updates `lsn` from `next_cdc_pos`
5. Clears `next_cdc_pos`
6. Next sync starts from `2D9/AD005000`

##### Scenario 5: Recovery - Some Streams Not Committed (Bounded Sync)

**State:** `processing` contains uncommitted streams, `next_cdc_pos` is set.

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "lsn": "2D9/AD00445A",
      "next_cdc_pos": "2D9/AD005000",  // ← Target position for bounded sync
      "processing": ["public.table_2"]  // ← Uncommitted stream
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. On recovery, system detects `processing` contains `public.table_2`
2. Checks commit status for each stream in `processing`
3. For `public.table_2`: thread not committed
4. Sets target position to `next_cdc_pos` (bounded sync)
5. Syncs only `public.table_2` up to LSN `2D9/AD005000`
6. After commit, removes from `processing` and clears `next_cdc_pos`
7. **Acknowledges LSN to Postgres** (advances replication slot)

##### Scenario 6: Recovery - Stream Not Found in Streams List

**State:** `processing` contains a stream that no longer exists.

```json
{
  "type": "GLOBAL",
  "global": {
    "state": {
      "lsn": "2D9/AD00445A",
      "next_cdc_pos": "2D9/AD005000",
      "processing": ["public.old_table"]  // ← Stream no longer in streams list
    },
    "streams": ["public.table_1", "public.table_2"]
  }
}
```

**What happens:**
1. On recovery, system detects `public.old_table` in `processing`
2. Stream is not found in current streams list
3. Removes `public.old_table` from `processing`
4. If `processing` becomes empty, acknowledges LSN and clears `next_cdc_pos`

---
