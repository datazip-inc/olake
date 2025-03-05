package driver

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

func getPrimaryKeyColumn(db *sql.DB, table string) string {
	query := `
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = ? 
        AND CONSTRAINT_NAME = 'PRIMARY' 
        LIMIT 1`

	var pkColumn string
	err := db.QueryRow(query, table).Scan(&pkColumn)
	if err != nil {
		if err == sql.ErrNoRows {
			return ""
		}
		log.Printf("Error getting primary key for table %s: %v", table, err)
		return ""
	}
	return pkColumn
}

// backfill implements full refresh sync mode for MySQL
func (m *MySQL) backfill(stream protocol.Stream, pool *protocol.WriterPool) error {
	logger.Infof("starting backfill for stream [%s]", stream.ID())
	chunks := stream.GetStateChunks()

	if chunks == nil || chunks.Len() == 0 {
		chunks = types.NewSet[types.Chunk]()
		// Full load as no chunks state present
		if err := m.calculateChunks(stream, chunks); err != nil {
			return fmt.Errorf("failed to calculate chunks: %w", err)
		}
		// Save chunks state
		stream.SetStateChunks(chunks)
	}

	logger.Infof("running backfill for %d chunks", chunks.Len())
	return utils.Concurrent(context.TODO(), chunks.Array(), m.config.MaxThreads, func(ctx context.Context, chunk types.Chunk, number int) error {
		if err := m.processChunk(ctx, pool, stream, chunk); err != nil {
			return err
		}
		// Remove processed chunk from state
		stream.RemoveStateChunk(chunk)
		return nil
	})
}

func (m *MySQL) calculateChunks(stream protocol.Stream, chunks *types.Set[types.Chunk]) error {
	return m.withIsolation(context.Background(), func(tx *sql.Tx) error {
		// Get primary key column using the provided function
		pkColumn := getPrimaryKeyColumn(m.db, stream.Name())
		if pkColumn == "" {
			return fmt.Errorf("no primary key found for stream %s", stream.ID())
		}

		// Get table extremes
		minVal, maxVal, err := m.getTableExtremes(stream, pkColumn, tx)
		if err != nil {
			return err
		}
		chunks.Insert(types.Chunk{
			Min: "",
			Max: convertToString(minVal),
		},
		)

		logger.Infof("Stream %s extremes - min: %v, max: %v", stream.ID(), minVal, maxVal)

		//Calculate optimal chunk size based on table statistics
		chunkSize, err := m.calculateChunksSize(stream)
		if err != nil {
			return fmt.Errorf("failed to calculate chunk size: %w", err)
		}
		fmt.Println("chunkSize", chunkSize)
		// Generate chunks based on range
		query := fmt.Sprintf(`
    SELECT MAX(%[1]s) 
      FROM (
	SELECT %[1]s 
	FROM %[2]s.%[3]s 
	WHERE %[1]s > ? 
	ORDER BY %[1]s 
	LIMIT ?
) AS subquery
`, pkColumn, stream.Namespace(), stream.Name())

		var currentVal interface{} = minVal
		for {
			var nextValRaw interface{}
			err := tx.QueryRow(query, currentVal, chunkSize).Scan(&nextValRaw)
			if err != nil || nextValRaw == nil {
				// Add final chunk
				chunks.Insert(types.Chunk{
					Min: convertToString(currentVal),
					Max: convertToString(maxVal),
				})
				break
			}
			nextVal := convertToString(nextValRaw)
			chunks.Insert(types.Chunk{
				Min: convertToString(currentVal),
				Max: nextVal,
			})
			currentVal = nextVal
		}

		return nil
	})
}
func (m *MySQL) getTableExtremes(stream protocol.Stream, pkColumn string, tx *sql.Tx) (min, max any, err error) {
	query := fmt.Sprintf(`
		SELECT 
			MIN(%[1]s) as min_val,
			MAX(%[1]s) as max_val 
		FROM %[2]s.%[3]s
	`, pkColumn, stream.Namespace(), stream.Name())
	err = tx.QueryRow(query).Scan(&min, &max)
	if err != nil {
		return "", "", err
	}
	return convertToString(min), convertToString(max), err
}

// Helper function to convert MySQL results to string
func convertToString(value interface{}) string {
	switch v := value.(type) {
	case []byte:
		return string(v) // Convert byte slice to string
	case string:
		return v // Already a string
	default:
		return fmt.Sprintf("%v", v) // Fallback
	}
}

func (m *MySQL) processChunk(ctx context.Context, pool *protocol.WriterPool, stream protocol.Stream, chunk types.Chunk) error {
	return m.withIsolation(ctx, func(tx *sql.Tx) error {
		pkColumn := getPrimaryKeyColumn(m.db, stream.Name())
		if pkColumn == "" {
			return fmt.Errorf("no primary key found for stream %s", stream.ID())
		}

		query := fmt.Sprintf(`
		SELECT * 
		FROM %s.%s 
		WHERE %s > ? AND %s <= ?
		ORDER BY %s
	`, stream.Namespace(), stream.Name(), pkColumn, pkColumn, pkColumn)

		rows, err := m.db.QueryContext(ctx, query, chunk.Min, chunk.Max)
		if err != nil {
			return fmt.Errorf("failed to query chunk: %w", err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("failed to get columns: %w", err)
		}

		// Create wait channel for writer thread
		waitChan := make(chan error, 1)
		writer, err := pool.NewThread(ctx, stream, protocol.WithWaitChannel(waitChan))
		if err != nil {
			return fmt.Errorf("failed to create writer thread: %w", err)
		}
		defer func() {
			writer.Close()
			// Wait for chunk completion
			if werr := <-waitChan; werr != nil {
				err = werr
			}
		}()

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}

			// Convert to map
			record := make(map[string]interface{})
			for i, col := range columns {
				record[col] = values[i]
			}

			// Calculate record hash using primary key
			recordHash := utils.GetKeysHash(record, pkColumn)

			// Create and insert record
			exit, err := writer.Insert(types.CreateRawRecord(recordHash, record, time.Now().UnixNano()))
			if err != nil {
				return fmt.Errorf("failed to insert record: %w", err)
			}
			if exit {
				return nil
			}
		}

		return rows.Err()
	})
}

func (m *MySQL) calculateChunksSize(stream protocol.Stream) (int, error) {
	var totalRecords int
	query := `SELECT TABLE_ROWS
	        FROM INFORMATION_SCHEMA.TABLES
			WHERE TABLE_SCHEMA=DATABASE()
			AND TABLE_NAME = ?`
	err := m.db.QueryRow(query, stream.Name()).Scan(&totalRecords)
	if err != nil {
		return 0, fmt.Errorf("faield to get estimated records count:%v", err)

	}
	return totalRecords / (m.config.MaxThreads * 8), nil
}
func (m *MySQL) withIsolation(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
