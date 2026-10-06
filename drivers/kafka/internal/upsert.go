package driver

import (
	//nolint:gosec // G401: md5 used for non-crypto hashing
	"crypto/md5"
	"errors"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/utils"
)

var errNullDedupKeys = errors.New("all dedup keys are null")

// isKafkaKeyOnlyDedup checks if user selects only _kafka_key as dedup key
// need to enable upsert + delete; anything else is upsert only
func isKafkaKeyOnlyDedup(dedupKeys []string) bool {
	return len(dedupKeys) == 1 && dedupKeys[0] == Key
}

func validateDedupKey(dedupKeys []string) error {
	for _, key := range dedupKeys {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("upsert mode: dedup key contains an empty field name")
		}
	}
	return nil
}

func checkDedupKeysExist(dedupKeys []string, data map[string]any, kafkaKey string, keyFields map[string]any) (map[string]any, error) {
	if data == nil {
		data = map[string]any{}
	}
	for _, pk := range dedupKeys {
		if _, ok := data[pk]; ok {
			continue
		}
		// from parsed JSON key
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
	for _, pk := range dedupKeys {
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

// generateOlakeIDFromExistingKeys generates the olake ID from the existing dedup keys and data
func generateOlakeIDFromExistingKeys(dedupKeys []string, data map[string]any) string {
	if len(dedupKeys) == 1 && dedupKeys[0] == Key {
		s := fmt.Sprint(data[Key])
		trimmed := strings.TrimSpace(s)
		if len(trimmed) == 0 || trimmed[0] != '{' || !utils.IsJSON(s) {
			return s
		}
		//nolint:gosec // G401: md5 used for non-crypto hashing
		return fmt.Sprintf("%x", md5.Sum([]byte(s)))
	}
	keys := append([]string{}, dedupKeys...)
	return utils.GetKeysHash(data, keys...)
}
