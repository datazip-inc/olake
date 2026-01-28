package kafka

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/linkedin/goavro/v2"
)

func NewSchemaRegistryClient(endpoint, username, password string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		endpoint: endpoint,
		username: username,
		password: password,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// TODO: fetch schema by subject strategy if needed (e.g. latest, version specific)
// currently we only fetch by ID which is sufficient for deserialization of consumed messages
func (c *SchemaRegistryClient) FetchSchema(schemaID uint32) (*types.RegisteredSchema, error) {
	// if schema exists previously
	if schema, ok := c.schemaMap.Load(schemaID); ok {
		return schema.(*types.RegisteredSchema), nil
	}

	// fetch schema from registry
	url := fmt.Sprintf("%s/schemas/ids/%d", c.endpoint, schemaID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// set basic auth if credentials provided
	if c.username != "" && c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %w", err)
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
		return nil, fmt.Errorf("failed to decode schema response: %w", err)
	}

	// determine schema type
	schemaType := types.SchemaType(schemaResp.SchemaType)

	// Note: AVRO is the default (if no schema type is shown on the response, the type is AVRO), PROTOBUF, JSON [official docs: https://docs.confluent.io/platform/current/schema-registry/develop/api.html]
	schemaType = utils.Ternary(schemaType == "", types.SchemaTypeAvro, schemaType).(types.SchemaType)

	registered := &types.RegisteredSchema{
		Schema:     schemaResp.Schema,
		SchemaType: schemaType,
	}

	// parse Avro codec if schema type is Avro
	if schemaType == types.SchemaTypeAvro {
		codec, err := goavro.NewCodec(schemaResp.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to create Avro codec for schema ID %d: %w", schemaID, err)
		}
		registered.Codec = codec
	}

	// cache the schema
	c.schemaMap.Store(schemaID, registered)
	return registered, nil
}
