package driver

import (
	"errors"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

var errNullDedupKeys = errors.New("all dedup keys are null")

type UpsertConfig struct {
	Enabled               bool
	DedupKeys             []string
	AllowTombstoneDeletes bool
}

func NewUpsertConfig(meta types.StreamMetadata, dedupKeys []string) (UpsertConfig, error) {
	// case 1 - dedup key is only _kafka_key -- upsert + tombstone deletes on
	// case 2 - dedup key selection [_kafka_key(choice) + columns] -- upsert only (no tombstones)
	cfg := UpsertConfig{
		Enabled:               !meta.AppendMode,
		DedupKeys:             dedupKeys,
		AllowTombstoneDeletes: isKafkaKeyOnlyDedup(dedupKeys),
	}
	return cfg, cfg.Validate()
}

func UpsertConfigFrom(meta types.StreamMetadata) (UpsertConfig, error) {
	return NewUpsertConfig(meta, meta.DedupKeys)
}

// isKafkaKeyOnlyDedup: checks if user selects only _kafka_key as dedup key
// need to enable upsert +  delete; anything else is upsert only
func isKafkaKeyOnlyDedup(dedupKeys []string) bool {
	return len(dedupKeys) == 1 && dedupKeys[0] == Key
}

func (c UpsertConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if len(c.DedupKeys) == 0 {
		return fmt.Errorf("upsert mode: dedup key is required")
	}
	for _, key := range c.DedupKeys {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("upsert mode: dedup key contains an empty field name")
		}
	}
	return nil
}

func (c UpsertConfig) checkDedupKeysExist(data map[string]any, kafkaKey string, keyFields map[string]any) (map[string]any, error) {
	if data == nil {
		data = map[string]any{}
	}
	for _, pk := range c.DedupKeys {
		if _, ok := data[pk]; ok {
			continue
		}
		//from parsed JSON key
		if keyFields != nil {
			if val, ok := keyFields[pk]; ok {
				data[pk] = val
				continue
			}
		}
		if pk == Key && kafkaKey != "" {
			data[pk] = kafkaKey
			continue
		}
	}

	anyPresent := false
	anyNonNull := false
	for _, pk := range c.DedupKeys {
		val, ok := data[pk]
		if !ok {
			continue
		}
		anyPresent = true
		if val != nil {
			anyNonNull = true
		}
	}
	if anyNonNull {
		return data, nil
	}
	if anyPresent {
		return nil, errNullDedupKeys
	}

	return nil, fmt.Errorf("missing dedup keys")
}

func (c UpsertConfig) generateOlakeIDFromExistingKeys(data map[string]any) string {
	existing := make([]string, 0, len(c.DedupKeys))
	for _, pk := range c.DedupKeys {
		if _, ok := data[pk]; ok {
			existing = append(existing, pk)
		}
	}
	return utils.GetKeysHash(data, existing...)
}
