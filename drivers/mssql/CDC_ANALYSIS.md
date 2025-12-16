# MSSQL CDC Implementation Analysis

## Comparison with Other Drivers

### 1. MongoDB CDC Flow

**PreCDC:**
- Gets resume token from state or current resume token
- Stores resume token in memory (`cdcCursor`)
- Gets cluster operation time for catch-up detection
- Resets streams if needed

**StreamChanges:**
- Uses resume token to start change stream
- Processes changes in loop
- Updates resume token after each change
- Handles idle checkpointing
- Exits when caught up to cluster op time

**PostCDC:**
- Saves resume token to state using `SetCursor`
- Only saves if `noErr == true`

**State Management:**
- Uses stream-level cursors (not global state)
- Each stream has its own resume token

---

### 2. MySQL CDC Flow

**PreCDC:**
- Gets or initializes global state (binlog position + server ID)
- Creates binlog connection with position
- Resets streams if needed

**StreamChanges:**
- Delegates to `BinlogConn.StreamMessages`
- Binlog connection handles streaming internally
- Checks initial wait time (after getting latest position)
- Exits when caught up to latest binlog position
- Updates position after each event

**PostCDC:**
- Saves final binlog position to global state using `SetGlobal`
- Only saves if `noErr == true`
- Cleans up binlog connection

**State Management:**
- Uses global state with binlog position
- Position updated during streaming

**Timestamp:**
- Uses `time.Unix(int64(ev.Header.Timestamp), 0)` from binlog event header

---

### 3. PostgreSQL CDC Flow

**PreCDC:**
- Creates WAL replicator
- Gets current WAL position
- If no global state: sets current position and resets streams
- If global state exists: validates LSN matches confirmed flush LSN
- Advances LSN if needed

**StreamChanges:**
- Delegates to `replicator.StreamChanges`
- Replicator handles WAL streaming
- Checks initial wait time
- Updates LSN position during streaming

**PostCDC:**
- Saves final LSN to global state using `SetGlobal`
- Acknowledges LSN to PostgreSQL
- Only saves if `noErr == true`
- Cleans up replicator

**State Management:**
- Uses global state with LSN string
- LSN updated during streaming

**Timestamp:**
- wal2json: Uses `changes.Timestamp.Time` from WAL message
- pgoutput: Uses `p.txnCommitTime` from transaction commit time

---

### 4. MSSQL CDC Flow (Current Implementation)

**PreCDC:**
- Gets or initializes global state (LSN string)
- Gets current max LSN if state is empty
- Resets streams if needed
- ✅ Matches pattern

**StreamChanges:**
- Gets fromLSN from global state
- Polls for changes between fromLSN and toLSN
- Checks initial wait time (before getting toLSN - slightly different from MySQL)
- Exits when caught up (fromLSN == toLSN)
- Updates state after processing each batch
- ✅ Matches pattern (with minor difference in wait time check)

**PostCDC:**
- Ensures final LSN is persisted
- Only saves if `noErr == true`
- ✅ Fixed to match pattern

**State Management:**
- Uses global state with LSN string (hex-encoded)
- LSN updated during streaming
- ✅ Matches PostgreSQL pattern

**Timestamp:**
- ⚠️ Currently uses `time.Now().UTC()`
- MSSQL CDC doesn't provide timestamps directly in change tables
- Would need to join with `cdc.lsn_time_mapping` to get transaction times
- This is acceptable but not ideal

---

## Issues Found and Fixed

### ✅ Fixed: LSN Not Persisted in State

**Issue:** When `fromLSN == toLSN` (no new changes), state wasn't updated before returning.

**Fix:** Added state update when exiting due to caught-up condition.

**Location:** `drivers/mssql/internal/cdc.go:89-90`

---

### ⚠️ Minor: Timestamp Handling

**Issue:** Using `time.Now()` instead of actual transaction timestamp.

**Analysis:**
- MySQL: Uses binlog event header timestamp
- PostgreSQL: Uses WAL message timestamp or transaction commit time
- MSSQL: CDC change tables don't include timestamps directly
- To get actual timestamps, would need to join with `cdc.lsn_time_mapping` table using `__$start_lsn`
- This would require additional queries and complexity

**Decision:** Keep `time.Now()` for now. This is acceptable because:
1. The actual change time is close to processing time for real-time CDC
2. Adding LSN-to-timestamp mapping would add complexity and performance overhead
3. The `_cdc_timestamp` field in the destination is optional and can be null

**Future Enhancement:** Could add optional timestamp lookup if needed.

---

### ✅ Verified: Initial Wait Time Logic

**Current:** Checks wait time before getting toLSN.

**MySQL Pattern:** Gets latest position first, then checks wait time.

**Analysis:** Both approaches are valid. MSSQL's approach is slightly more efficient (doesn't query if timing out). MySQL's approach ensures we have latest position before checking.

**Decision:** Keep current approach - it's valid and slightly more efficient.

---

### ✅ Verified: State Persistence

**Pattern:** All drivers use `SetGlobal` which internally calls `LogState()` to persist to file.

**MSSQL:** Now correctly persists LSN in both `StreamChanges` (during loop) and `PostCDC` (final persistence).

---

## Summary

### ✅ Correctly Implemented:
1. PreCDC: Initializes/validates global state, resets streams
2. StreamChanges: Polls for changes, updates state, exits when caught up
3. PostCDC: Persists final LSN (fixed)
4. State Management: Uses global state with LSN string
5. Operation Mapping: Correctly maps CDC operation codes (1=delete, 2=insert, 3/4=update)

### ⚠️ Acceptable Differences:
1. Timestamp: Using `time.Now()` instead of transaction timestamp (acceptable trade-off)
2. Initial Wait Time: Checks before getting toLSN (slightly different but valid)

### ✅ Pattern Alignment:
- Follows same overall structure as MySQL/PostgreSQL
- Uses global state like PostgreSQL
- Polling approach similar to MySQL binlog
- State persistence matches all drivers

---

## Conclusion

The MSSQL CDC implementation is **correctly aligned** with the patterns from other drivers. The only minor difference is timestamp handling, which is an acceptable trade-off given MSSQL CDC's architecture.

