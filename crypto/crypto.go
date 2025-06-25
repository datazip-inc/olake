package crypto

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
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/viper"
)

var (
	kmsClient *kms.Client
	localKey  []byte
	useKMS    bool
	once      sync.Once
)

type cryptoObj struct {
	EncryptedData string `json:"encrypted_data"`
}

// InitEncryption initializes encryption based on KMS key or passphrase
func InitEncryption() error {
	key := viper.GetString("ENCRYPTION_KEY")
	var initErr error

	once.Do(func() {
		if strings.HasPrefix(key, "arn:aws:kms:") {
			cfg, err := config.LoadDefaultConfig(context.Background())
			if err != nil {
				initErr = fmt.Errorf("failed to load AWS config: %w", err)
				return
			}
			kmsClient = kms.NewFromConfig(cfg)
			useKMS = true
		} else {
			// Local AES-GCM Mode with SHA-256 derived key
			hash := sha256.Sum256([]byte(key))
			localKey = hash[:]
			useKMS = false
		}
	})

	return initErr
}

func Decrypt(cipherData []byte) (string, error) {
	if err := InitEncryption(); err != nil {
		return "", fmt.Errorf("decryption failed: %w", err)
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
func DecryptJSONString(encryptedObjStr string) (string, error) {
	// Unmarshal the encrypted object
	cryptoObj := cryptoObj{}

	if err := json.Unmarshal([]byte(encryptedObjStr), &cryptoObj); err != nil {
		return "", fmt.Errorf("failed to unmarshal encrypted data: %v", err)
	}

	// Decode the base64-encoded encrypted data
	encryptedData, err := base64.StdEncoding.DecodeString(cryptoObj.EncryptedData)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64 data: %v", err)
	}

	// Decrypt the data
	decrypted, err := Decrypt(encryptedData)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt data: %v", err)
	}

	return string(decrypted), nil
}
