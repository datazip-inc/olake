package bigquery

import "github.com/datazip-inc/olake/drivers"

func init() {
	drivers.RegisterSource("bigquery", func(config map[string]any) (drivers.Source, error) {
		cfg := Config{
			ProjectID: config["project_id"].(string),
			DatasetID: config["dataset_id"].(string),
			TableID:   config["table_id"].(string),
		}
		return New(cfg)
	})
}