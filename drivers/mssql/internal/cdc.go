package driver

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	cdcCursorKey = "lsn"
)

// CDC capture instance for a table
type captureInstance struct {
	schema       string
	table        string
	instanceName string
	startLSN     string
}

// prepareCaptureInstances discovers capture instances for selected streams
func (m *MSSQL) prepareCaptureInstances(ctx context.Context, streams []types.StreamInterface) error {
	// TODO: query capture instances per-stream in StreamChanges and remove capturesMap
	m.capturesMap = make(map[string][]captureInstance)
	streamIDs := make([]string, 0, len(streams))
	for _, stream := range streams {
		streamIDs = append(streamIDs, stream.ID())
	}

	// Discover capture instances for selected streams
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLCDCDiscoverQuery(streamIDs))
	if err != nil {
		return fmt.Errorf("failed to query MSSQL CDC tables: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			capture  captureInstance
			startLSN []byte // to track the start LSN of the capture instance
		)

		if err := rows.Scan(&capture.schema, &capture.table, &capture.instanceName, &startLSN); err != nil {
			return fmt.Errorf("failed to scan MSSQL CDC table: %s", err)
		}
		capture.startLSN = hex.EncodeToString(startLSN)

		streamID := fmt.Sprintf("%s.%s", capture.schema, capture.table)
		m.capturesMap[streamID] = append(m.capturesMap[streamID], capture)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if len(m.capturesMap) == 0 {
		logger.Warnf("No CDC capture instances found for selected streams")
	}

	return nil
}

// PreCDC initialises CDC state and starting LSN per stream.
func (m *MSSQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if !m.cdcSupported {
		return fmt.Errorf("invalid call; %s not running in CDC mode", m.Type())
	}

	if err := m.prepareCaptureInstances(ctx, streams); err != nil {
		return err
	}

	// Get current max LSN to use as start point for new streams
	currentLSN, err := m.currentMaxLSN(ctx)
	if err != nil {
		return fmt.Errorf("failed to get MSSQL current max LSN: %w", err)
	}

	for _, stream := range streams {
		// check if CDC is enabled for each stream
		enabled, err := m.isStreamCDCEnabled(ctx, stream.Namespace(), stream.Name())
		if err != nil {
			return err
		}
		if !enabled {
			return fmt.Errorf("CDC is not enabled for table %s.%s", stream.Namespace(), stream.Name())
		}

		// Initialize LSN for each stream if not present
		lsnVal := m.state.GetCursor(stream.Self(), cdcCursorKey)
		if lsnVal == nil {
			// New stream or first run: start from current max LSN
			m.state.SetCursor(stream.Self(), cdcCursorKey, currentLSN)
			lsnVal = currentLSN
		}
		m.lsnMap.Store(stream.ID(), lsnVal.(string))
	}

	return nil
}

// StreamChanges fetches a bounded window of CDC changes for a specific stream.
func (m *MSSQL) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	// Get current position for this stream
	fromLSNVal, exists := m.lsnMap.Load(stream.ID())
	if !exists {
		return fmt.Errorf("no LSN found for stream %s", stream.ID())
	}
	fromLSN := fromLSNVal.(string)

	// Get target LSN (current max LSN in DB)
	targetLSN, err := m.currentMaxLSN(ctx)
	if err != nil {
		return fmt.Errorf("failed to get MSSQL max LSN: %s", err)
	}

	// No changes yet
	if fromLSN >= targetLSN {
		return nil
	}

	// Get capture instance info
	captures, ok := m.capturesMap[stream.ID()]
	if !ok || len(captures) == 0 {
		return fmt.Errorf("CDC is not enabled for table %s.%s", stream.Namespace(), stream.Name())
	}

	// TODO: research how to handle schema evolution

	// When multiple capture instances exist for the same table (due to schema
	// evolution), pick the newest instance whose startLSN is <= fromLSN.
	// This guarantees we continue from an instance that was valid at our last
	// processed LSN.
	//
	// If there is a newer capture instance after the one we select, clamp
	// targetLSN to that newer instance's startLSN so we do not read rows that
	// conceptually belong to the new schema from the old capture instance.
	//
	// Note: we expect column-level data loss (e.g., new columns missing)
	// in the LSN range between the DDL and when the new capture instance becomes active.
	var selectedCapture *captureInstance
	for i := len(captures) - 1; i >= 0; i-- {
		// Skip if this capture started after fromLSN
		if captures[i].startLSN > fromLSN {
			continue
		}

		// Select the capture instance
		selectedCapture = &captures[i]

		// If a newer capture instance exists, restrict the targetLSN to the newer instance's startLSN
		nextIdx := i + 1
		if nextIdx < len(captures) && targetLSN > captures[nextIdx].startLSN {
			newerCapture := captures[nextIdx]
			logger.Warnf("Newer capture instance [%s] detected for stream %s at LSN %s. Clamping targetLSN", newerCapture.instanceName, stream.ID(), newerCapture.startLSN)
			targetLSN = newerCapture.startLSN
		}

		break
	}

	if selectedCapture == nil {
		return fmt.Errorf(
			"LSN %s is earlier than the start LSN of available capture instances for stream %s. Please perform full-refresh",
			fromLSN,
			stream.ID(),
		)
	}

	logger.Infof(
		"Selected CDC capture instance [%s] for stream %s (from LSN %s)",
		selectedCapture.instanceName,
		stream.ID(),
		fromLSN,
	)

	// Fetch changes
	err = m.fetchTableChangesInLSNRange(ctx, stream, *selectedCapture, fromLSN, targetLSN, processFn)
	if err != nil {
		return err
	}

	// Cache target LSN for this stream
	m.lsnMap.Store(stream.ID(), targetLSN)

	return nil
}

// PostCDC per stream state saving is handled here if no error occurred.
func (m *MSSQL) PostCDC(ctx context.Context, stream types.StreamInterface, noErr bool, _ string) error {
	if noErr {
		if val, exists := m.lsnMap.Load(stream.ID()); exists {
			m.state.SetCursor(stream.Self(), cdcCursorKey, val)
		} else {
			logger.Warnf("No LSN found for stream: %s", stream.ID())
		}
	}
	return nil
}

// fetchTableChangesInLSNRange fetches and emits CDC changes for a single table/capture-instance within an LSN range.
func (m *MSSQL) fetchTableChangesInLSNRange(ctx context.Context, stream types.StreamInterface, capture captureInstance, fromLSN, toLSN string, processFn abstract.CDCMsgFn) error {
	// Move the lower bound forward by one LSN to avoid re-emitting the last processed row.
	effectiveFromLSN, err := m.advanceLSN(ctx, fromLSN)
	if err != nil {
		return err
	}

	// SQL Server expects LSN parameters as binary; state stores them as hex strings.
	fromLSNBytes, err := hex.DecodeString(effectiveFromLSN)
	if err != nil {
		return fmt.Errorf("failed to parse fromLSN: %s", err)
	}
	toLSNBytes, err := hex.DecodeString(toLSN)
	if err != nil {
		return fmt.Errorf("failed to parse toLSN: %s", err)
	}

	// Query CDC rows for this capture instance between the two LSNs.
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)
	rows, err := m.client.QueryContext(ctx, query, fromLSNBytes, toLSNBytes)
	if err != nil {
		return fmt.Errorf("failed to query MSSQL CDC changes: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		// Use MapScan to properly convert data types including binary types
		// TODO: check if we can use MapScanConcurrent for mssql
		record := make(map[string]interface{})
		if err := jdbc.MapScan(rows, record, m.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan MSSQL CDC row: %s", err)
		}

		// Determine operation type from SQL Server CDC operation codes.
		// For updates, CDC emits "before" (3) and "after" (4); we skip "before".
		var operationType string
		if val, ok := record["__$operation"]; ok {
			var opCode int32 = val.(int32)
			if opCode == 3 {
				continue
			}
			operationType = operationTypeFromCDCCode(opCode)
		}

		// Remove metadata columns
		delete(record, "__$operation")
		delete(record, "__$start_lsn")
		delete(record, "__$seqval")
		delete(record, "__$update_mask")

		// Emit one normalized CDC change event.
		if err := processFn(ctx, abstract.CDCChange{
			Stream:    stream,
			Timestamp: time.Now().UTC(),
			Kind:      operationType,
			Data:      record,
		}); err != nil {
			return fmt.Errorf("failed to process MSSQL CDC change: %s", err)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (m *MSSQL) currentMaxLSN(ctx context.Context) (string, error) {
	var lsn []byte
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLCDCMaxLSNQuery()).Scan(&lsn)
	if err != nil {
		return "", err
	}
	if len(lsn) == 0 {
		return "", fmt.Errorf("no LSN available (CDC may not be initialized or no transactions exist)")
	}
	return hex.EncodeToString(lsn), nil
}

// advanceLSN returns the next valid LSN after the given LSN.
func (m *MSSQL) advanceLSN(ctx context.Context, lsnHex string) (string, error) {
	// Decode the hex string into raw bytes because SQL Server LSN functions use binary values.
	lsnBytes, err := hex.DecodeString(lsnHex)
	if err != nil {
		return "", fmt.Errorf("failed to parse LSN for advance: %s", err)
	}

	// Compute the next LSN.
	var nextLSNBytes []byte
	if err := m.client.QueryRowContext(ctx, jdbc.MSSQLCDCAdvanceLSNQuery(), lsnBytes).Scan(&nextLSNBytes); err != nil {
		return "", fmt.Errorf("failed to advance LSN: %s", err)
	}

	if len(nextLSNBytes) == 0 {
		return "", fmt.Errorf("advanced LSN is empty")
	}

	return hex.EncodeToString(nextLSNBytes), nil
}

// operationTypeFromCDCCode converts SQL Server CDC __$operation codes to our operationType string.
// Codes: 1=delete, 2=insert, 3/4=update (before/after).
func operationTypeFromCDCCode(code int32) string {
	switch code {
	case 1:
		return "delete"
	case 2:
		return "insert"
	case 3, 4:
		return "update"
	default:
		return "update"
	}
}
