package destinationtest

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// glueService is Moto, which emulates the AWS Glue API in memory and checks no signatures.
var glueService = &testhandler.Service{
	Container: "olake-catalog-glue",
	Port:      15000,
	Target:    "testutils.glue.start",
	Ready:     httpReady("/moto-api/"),
}

// IcebergGlue writes Iceberg tables through a Glue catalog served by an emulator, never by AWS.
// Without glue_additional_config the writer ignores glue_endpoint and calls real AWS.
var IcebergGlue testhandler.DestinationKind = &icebergKind{
	name:      "iceberg-glue",
	catalog:   glueService,
	warehouse: "s3://" + bucket + "/iceberg_glue",
	config: map[string]any{
		"catalog_type":           "glue",
		"glue_additional_config": true,
		"glue_endpoint":          "http://" + glueService.Addr(),
		"glue_access_key":        serverUser,
		"glue_secret_key":        serverKey,
		"glue_region":            region,
	},
	dropNamespace: dropGlueDatabase,
	files: func(namespace string) string {
		return "iceberg_glue/" + namespace + ".db"
	},
}

// dropGlueDatabase deletes a database from the emulator, which drops its tables with it.
func dropGlueDatabase(ctx context.Context, database string) error {
	status, body, err := glueCall(ctx, "DeleteDatabase", `{"Name":"`+database+`"}`)
	if err == nil && status != http.StatusOK && !strings.Contains(body, "EntityNotFoundException") {
		err = fmt.Errorf("glue DeleteDatabase %s returned status %d: %s", database, status, body)
	}
	return err
}

// glueCall sends one Glue API action to the emulator. The region in the credential scope selects its store.
func glueCall(ctx context.Context, action, body string) (int, string, error) {
	return request(ctx, http.MethodPost, "http://"+glueService.Addr()+"/", map[string]string{
		"X-Amz-Target":  "AWSGlue." + action,
		"Content-Type":  "application/x-amz-json-1.1",
		"Authorization": "AWS4-HMAC-SHA256 Credential=" + serverUser + "/20260101/" + region + "/glue/aws4_request, SignedHeaders=host, Signature=0",
	}, body)
}
