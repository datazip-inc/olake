package crypto

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

var (
	kmsClient *kms.Client
	once      sync.Once
	kmsKey    string // Ensure this is set via environment variables or configuration
)

func initKMS() {
	once.Do(func() {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			panic(fmt.Sprintf("Unable to load AWS config: %v", err))
		}
		kmsClient = kms.NewFromConfig(cfg)

		if kmsKey == "" {
			panic("KMS_KEY_ID not set in environment variables")
		}
	})
}

func Decrypt(cipher []byte) (string, error) {
	out, err := kmsClient.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: cipher,
	})
	if err != nil {
		return "", fmt.Errorf("decryption failed: %w", err)
	}
	return string(out.Plaintext), nil
}

// DecryptJSON decrypts all values in the JSON if kmsKey is set.
// Only works for flat JSON with string values.
func DecryptJSON(jsonData []byte) ([]byte, error) {
	if kmsKey == "" {
		// KMS key not set, return original JSON
		return jsonData, nil
	}

	// Initialize KMS if not already done
	initKMS()

	// Parse JSON into map
	var data map[string]string
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Decrypt each value
	for k, v := range data {
		if v == "" {
			continue
		}

		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("failed to base64 decode value for key '%s': %w", k, err)
		}

		decryptedValue, err := Decrypt(decoded)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt value for key '%s': %w", k, err)
		}

		data[k] = decryptedValue
	}

	// Marshal back to JSON
	decryptedJSON, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal decrypted JSON: %w", err)
	}

	return decryptedJSON, nil
}
