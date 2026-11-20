package destinationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// restCatalogService is Apache Iceberg's REST catalog fixture. It reaches MinIO over its compose
// network, while the writer on the host uses MinIO's published port.
var restCatalogService = &testhandler.Service{
	Container: "olake-catalog-iceberg-rest",
	Port:      18181,
	Target:    "testutils.iceberg-rest.start",
	Ready:     httpReady("/v1/config"),
}

// IcebergREST writes Iceberg tables through a REST catalog. The fixture serves one warehouse and
// ignores the path a client names, but the writer needs one.
var IcebergREST testhandler.DestinationKind = &icebergKind{
	name:      "iceberg-rest",
	catalog:   restCatalogService,
	warehouse: "s3://" + bucket + "/iceberg_rest",
	config: map[string]any{
		"catalog_type":     "rest",
		"rest_catalog_url": "http://" + restCatalogService.Addr(),
	},
	dropNamespace: dropRESTNamespace,
	files: func(namespace string) string {
		return "iceberg_rest/" + namespace
	},
}

// dropRESTNamespace purges every table of a namespace and then drops the namespace.
func dropRESTNamespace(ctx context.Context, namespace string) error {
	namespaceURL := "http://" + restCatalogService.Addr() + "/v1/namespaces/" + namespace
	status, body, err := request(ctx, http.MethodGet, namespaceURL+"/tables", nil, "")
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status != http.StatusOK {
		return fmt.Errorf("list tables of %s returned status %d: %s", namespace, status, body)
	}
	var listing struct {
		Identifiers []struct {
			Name string `json:"name"`
		} `json:"identifiers"`
	}
	if err := json.Unmarshal([]byte(body), &listing); err != nil {
		return fmt.Errorf("list tables of %s: %s: %w", namespace, body, err)
	}
	for _, table := range listing.Identifiers {
		if err := restDelete(ctx, namespaceURL+"/tables/"+table.Name+"?purgeRequested=true"); err != nil {
			return err
		}
	}
	return restDelete(ctx, namespaceURL)
}

func restDelete(ctx context.Context, url string) error {
	status, body, err := request(ctx, http.MethodDelete, url, nil, "")
	if err == nil && status >= http.StatusMultipleChoices {
		err = fmt.Errorf("DELETE %s returned status %d: %s", url, status, body)
	}
	return err
}
