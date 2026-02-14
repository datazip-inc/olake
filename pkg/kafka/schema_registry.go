package kafka

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/linkedin/goavro/v2"
)

// Init initializes the HTTP client for the SchemaRegistryClient
func (c *SchemaRegistryClient) Init() {
	if c.httpClient == nil {
		c.httpClient = &http.Client{
			Timeout: 30 * time.Second,
		}
	}
}

// schemaRegistryGetRequest makes an authenticated HTTP GET request to the schema registry
func (c *SchemaRegistryClient) schemaRegistryGetRequest(path string) (*http.Response, error) {
	url := fmt.Sprintf("%s%s", c.Endpoint, path)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %s", err)
	}

	// Set authentication headers (bearer token takes priority over basic auth)
	if c.BearerToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.BearerToken))
	} else if c.Username != "" && c.Password != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")

	return c.httpClient.Do(req)
}

// TODO: fetch schema by subject strategy if needed (e.g. latest, version specific)
// currently we only fetch by ID which is sufficient for deserialization of consumed messages
func (c *SchemaRegistryClient) FetchSchema(schemaID uint32) (*types.RegisteredSchema, error) {
	// if schema exists previously
	if schema, ok := c.schemaMap.Load(schemaID); ok {
		return schema.(*types.RegisteredSchema), nil
	}

	// fetch schema from registry
	resp, err := c.schemaRegistryGetRequest(fmt.Sprintf("/schemas/ids/%d", schemaID))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry returned status %d for schema ID %d", resp.StatusCode, schemaID)
	}

	var schemaResp struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&schemaResp); err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %s", err)
	}

	// determine schema type
	schemaType := types.SchemaType(schemaResp.SchemaType)

	// Note: AVRO is the default (if no schema type is shown on the response, the type is AVRO), PROTOBUF, JSON [official docs: https://docs.confluent.io/platform/current/schema-registry/develop/api.html]
	schemaType = utils.Ternary(schemaType == "", types.SchemaTypeAvro, schemaType).(types.SchemaType)

	registered := &types.RegisteredSchema{
		SchemaType: schemaType,
	}

	// parse Avro codec if schema type is Avro
	if schemaType == types.SchemaTypeAvro {
		normalizedSchema, err := typeutils.NormalizeAvroSchema(schemaResp.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize schema ID %d: %s", schemaID, err)
		}
		codec, err := goavro.NewCodec(normalizedSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to create Avro codec for schema ID %d: %s", schemaID, err)
		}
		registered.Codec = codec
	}

	// cache the schema
	c.schemaMap.Store(schemaID, registered)
	return registered, nil
}

// Validate validates the schema registry connection using lightweight /subject request
func (c *SchemaRegistryClient) Validate() error {
	resp, err := c.schemaRegistryGetRequest("/subjects")
	if err != nil {
		return fmt.Errorf("failed to connect to schema registry: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("schema registry authentication failed: invalid credentials")
	}
	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("schema registry authentication failed: access forbidden")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("schema registry returned unexpected status: %d", resp.StatusCode)
	}

	return nil
}
