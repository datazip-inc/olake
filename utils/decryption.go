package utils

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/spf13/viper"
)

func getDecryptionConfig() (kmsClient *kms.Client, keyID string, localKey []byte, useKMS bool, disabled bool, err error) {
	key := viper.GetString("ENCRYPTION_KEY")

	if strings.TrimSpace(key) == "" {
		return nil, "", nil, false, true, nil
	}

	if strings.HasPrefix(key, "arn:aws:kms:") {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, "", nil, false, false, fmt.Errorf("failed to load AWS config: %w", err)
		}
		client := kms.NewFromConfig(cfg)
		return client, key, nil, true, false, nil
	}

	// Local AES-GCM Mode with SHA-256 derived key
	hash := sha256.Sum256([]byte(key))
	return nil, "", hash[:], false, false, nil
}

func Decrypt(cipherData []byte) (string, error) {
	kmsClient, _, localKey, useKMS, disabled, err := getDecryptionConfig()
	if err != nil {
		return "", fmt.Errorf("decryption failed: %w", err)
	}

	if disabled {
		return string(cipherData), nil
	}

	if useKMS {
		out, err := kmsClient.Decrypt(context.Background(), &kms.DecryptInput{
			CiphertextBlob: cipherData,
		})
		if err != nil {
			return "", fmt.Errorf("decryption failed: %w", err)
		}
		return string(out.Plaintext), nil
	}

	block, err := aes.NewCipher(localKey)
	if err != nil {
		return "", err
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := aead.NonceSize()
	if len(cipherData) < nonceSize {
		return "", errors.New("ciphertext too short")
	}

	nonce, ciphertext := cipherData[:nonceSize], cipherData[nonceSize:]

	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decryption failed: %w", err)
	}

	return string(plaintext), nil
}

// DecryptConfig decrypts base64 encoded encrypted data
func DecryptConfig(encryptedConfig string) (string, error) {
	// Use json.Unmarshal to properly handle JSON string unquoting
	var unquotedString string
	if err := json.Unmarshal([]byte(encryptedConfig), &unquotedString); err != nil {
		// If unmarshal fails, assume it's already unquoted
		unquotedString = encryptedConfig
	}

	encryptedData, err := base64.URLEncoding.DecodeString(unquotedString)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64 data: %v", err)
	}

	decrypted, err := Decrypt(encryptedData)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt data: %v", err)
	}

	return decrypted, nil
}
