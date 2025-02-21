package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
)

func (p *Postgres) prepareWALJSConfig(streams ...protocol.Stream) (*waljs.Config, error) {
	if !p.Driver.CDCSupport {
		return nil, fmt.Errorf("Invalid call; %s not running in CDC mode", p.Type())
	}

	config := &waljs.Config{
		Connection:          *p.config.Connection,
		ReplicationSlotName: p.cdcConfig.ReplicationSlot,
		InitialWaitTime:     time.Duration(p.cdcConfig.InitialWaitTime) * time.Second,
		State:               p.cdcState,
		FullSyncTables:      types.NewSet[protocol.Stream](),
		Tables:              types.NewSet[protocol.Stream](),
		BatchSize:           p.config.BatchSize,
	}

	for _, stream := range streams {
		if stream.Self().GetStateCursor() == nil {
			config.FullSyncTables.Insert(stream)
		}

		config.Tables.Insert(stream)
	}

	return config, nil
}

func (p *Postgres) StateType() types.StateType {
	return types.MixedType
}

// func (p *Postgres) GlobalState() any {
// 	return p.cdcState
// }

func (p *Postgres) SetupGlobalState(state *types.State) error {
	state.Type = p.StateType()
	// Setup raw state
	p.cdcState = types.NewGlobalState(&waljs.WALState{})

	return base.ManageGlobalState(state, p.cdcState, p)
}

// Write Ahead Log Sync
func (p *Postgres) RunChangeStream(pool *protocol.WriterPool, streams ...protocol.Stream) error {
	cdcCtx := context.TODO()
	config, err := p.prepareWALJSConfig(streams...)
	if err != nil {
		return err
	}

	socket, err := waljs.NewConnection(p.client, config)
	if err != nil {
		return err
	}
	insertionMap := make(map[protocol.Stream]protocol.InsertFunction)
	for _, stream := range streams {
		insert, err := pool.NewThread(cdcCtx, stream)
		if err != nil {
			return err
		}
		defer insert.Close()
		insertionMap[stream] = insert.Insert
	}

	return socket.OnMessage(func(message waljs.WalJSChange) (bool, error) {
		if message.Kind == "delete" {
			message.Data[jdbc.CDCDeletedAt] = message.Timestamp
		}
		if message.LSN != nil {
			message.Data[jdbc.CDCLSN] = message.LSN
		}
		message.Data[jdbc.CDCUpdatedAt] = message.Timestamp

		// get olake_key_id
		olakeID := utils.GetKeysHash(message.Data, message.Stream.GetStream().SourceDefinedPrimaryKey.Array()...)

		// insert record
		rawRecord := types.CreateRawRecord(olakeID, message.Data, message.Timestamp.UnixMilli())
		exit, err := insertionMap[message.Stream](rawRecord)
		if err != nil {
			return false, err
		}
		if exit {
			return false, nil
		}

		// TODO: State Management

		return false, nil
	})
}

func doesReplicationSlotExists(conn *sqlx.DB, slotName string) (bool, error) {
	var exists bool
	err := conn.QueryRow(
		"SELECT EXISTS(Select 1 from pg_replication_slots where slot_name = $1)",
		slotName,
	).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, validateReplicationSlot(conn, slotName)
}

func validateReplicationSlot(conn *sqlx.DB, slotName string) error {
	slot := waljs.ReplicationSlot{}
	err := conn.Get(&slot, fmt.Sprintf(waljs.ReplicationSlotTempl, slotName))
	if err != nil {
		return err
	}

	if slot.Plugin != "wal2json" {
		return fmt.Errorf("Plugin not supported[%s]: driver only supports wal2json", slot.Plugin)
	}

	if slot.SlotType != "logical" {
		return fmt.Errorf("only logical slots are supported: %s", slot.SlotType)
	}

	return nil
}
