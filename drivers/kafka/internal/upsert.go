package driver

import (
	//nolint:gosec // G401: md5 used for non-crypto hashing
	"crypto/md5"
	"encoding/json"
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

// generateOlakeIDFromExistingKeys: JSON _kafka_key is md5 of the stored string; other non-empty
// single keys are used as-is; empty string and 2+ keys are md5 of a JSON object (missing ≡ null).
func generateOlakeIDFromExistingKeys(dedupKeys []string, data map[string]any) string {
	if len(dedupKeys) == 1 && dedupKeys[0] == Key {
		s := fmt.Sprint(data[Key])
		trimmed := strings.TrimSpace(s)
		if trimmed != "" && trimmed[0] == '{' && utils.IsJSON(s) {
			//nolint:gosec // G401: md5 used for non-crypto hashing
			return fmt.Sprintf("%x", md5.Sum([]byte(s)))
		}
		if trimmed != "" {
			return s
		}
		return hashSortedDedupKeys(dedupKeys, data)
	}
	if len(dedupKeys) == 1 {
		id := utils.GetKeysHash(data, dedupKeys[0])
		if strings.TrimSpace(id) != "" {
			return id
		}
		return hashSortedDedupKeys(dedupKeys, data)
	}
	return hashSortedDedupKeys(dedupKeys, data)
}

func hashSortedDedupKeys(dedupKeys []string, data map[string]any) string {
	m := make(map[string]any, len(dedupKeys))
	for _, k := range dedupKeys {
		if val, ok := data[k]; ok && val != nil {
			m[k] = val
		} else {
			m[k] = nil
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		b = []byte(fmt.Sprint(m))
	}
	//nolint:gosec // G401: md5 used for non-crypto hashing
	return fmt.Sprintf("%x", md5.Sum(b))
}
