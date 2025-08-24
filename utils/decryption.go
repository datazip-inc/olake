/*
 * Copyright 2025 Olake By Datazip
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	"github.com/datazip-inc/olake/constants"
	"github.com/spf13/viper"
)

func getSecretKey() ([]byte, *kms.Client, error) {
	secretKey := viper.GetString(constants.EncryptionKey)
	if strings.TrimSpace(secretKey) == "" {
		return []byte{}, nil, nil // Encryption is disabled
	}

	if strings.HasPrefix(secretKey, "arn:aws:kms:") {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load AWS config: %s", err)
		}
		return []byte(secretKey), kms.NewFromConfig(cfg), nil
	}

	// Local AES-GCM Mode with SHA-256 derived key
	hash := sha256.Sum256([]byte(secretKey))
	return hash[:], nil, nil
}

func Decrypt(encryptedText string) (string, error) {
	if strings.TrimSpace(encryptedText) == "" {
		return "", fmt.Errorf("cannot decrypt empty or whitespace-only input")
	}

	key, kmsClient, err := getSecretKey()
	if err != nil || key == nil || len(key) == 0 {
		return encryptedText, err
	}

	var config string
	err = json.Unmarshal([]byte(encryptedText), &config)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal JSON string: %v", err)
	}

	encryptedData, err := base64.StdEncoding.DecodeString(config)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64 data: %v", err)
	}

	// Use KMS if client is provided
	if kmsClient != nil {
		result, err := kmsClient.Decrypt(context.Background(), &kms.DecryptInput{
			CiphertextBlob: encryptedData,
		})
		if err != nil {
			return "", fmt.Errorf("failed to decrypt with KMS: %s", err)
		}
		return string(result.Plaintext), nil
	}

	// Local AES-GCM decryption
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %s", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %s", err)
	}

	if len(encryptedData) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}

	plaintext, err := gcm.Open(nil, encryptedData[:gcm.NonceSize()], encryptedData[gcm.NonceSize():], nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt: %s", err)
	}
	return string(plaintext), nil
}
