package driver

import (
	"fmt"
	"net/http"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/piyushsingariya/relec/safego"
)

type CRMSearchStream struct {
	IncrementalStream
	associations []string
}

func newCRMSearchStream(incrementalStream IncrementalStream, primaryKey, lastModifiedField string, associations, _ []string) *CRMSearchStream {
	crm := &CRMSearchStream{
		IncrementalStream: incrementalStream,
		associations:      associations,
	}

	crm.statePk = "updatedAt"
	crm.updatedAtField = "updatedAt"
	crm.primaryKey = primaryKey
	crm.limit = 100
	crm.lastModifiedField = lastModifiedField
	return crm
}

func (c *CRMSearchStream) path() (string, string) {
	if c.state_ != nil {
		return fmt.Sprintf("/crm/v3/objects/%s/search", c.entity), http.MethodPost
	}
	return fmt.Sprintf("/crm/v3/objects/%s", c.entity), http.MethodGet
}

func (c *CRMSearchStream) search() (int, any, error) {
	method, path := c.path()
	request := &utils.Request{
		URN:    formatEndpoint(path),
		Method: method,
	}

	return c.handleRequest(request)
}

func (c *CRMSearchStream) processSearch(nextPageToken map[string]any) ([]map[string]any, any, error) {
	streamRecords := []map[string]any{}
	properties, err := c.propertiesList()
	if err != nil {
		return nil, nil, err
	}
	payload := map[string]any{}
	if c.state_ != nil {
		payload = map[string]any{
			"filters": []map[string]any{
				{"value": int(c.state_.Unix() * 1000), "propertyName": c.lastModifiedField, "operator": "GTE"},
			},
			"sorts": []map[string]any{
				{"propertyName": c.lastModifiedField, "direction": "ASCENDING"},
			},
			"properties": properties,
			"limit":      c.limit,
		}
	}

	for key, value := range nextPageToken {
		payload[key] = value
	}

	_, rawResponse, err := c.search()
	if err != nil {
		return nil, nil, err
	}

	records, err := c.transform(c.parseResponse(rawResponse))
	if err != nil {
		return nil, nil, err
	}

	for _, record := range records {
		streamRecords = append(streamRecords, record)
	}

	return streamRecords, rawResponse, nil
}

func (c *CRMSearchStream) readRecords(send chan<- types.Record) error {
	paginationComplete := false
	var nextPageToken map[string]any
	latest_cursor := &time.Time{}

	for !paginationComplete {
		var records []types.Record
		var rawResponse any
		var err error

		if c.state_ != nil {
			records, rawResponse, err = c.processSearch(nextPageToken)
			if err != nil {
				return err
			}
		} else {
			records, rawResponse, err = c.readStreamRecords(nextPageToken, c.path)
			if err != nil {
				return err
			}
		}

		for _, record := range c.filterOldRecords(c.flatAssociations(records)) {
			cursor, err := typeutils.ReformatDate(record[c.updatedAtField])
			if err != nil {
				return err
			}
			if latest_cursor != nil {
				latest_cursor = types.ToPointer(utils.MaxDate(cursor, *latest_cursor))
			} else {
				latest_cursor = &cursor
			}

			if !safego.Insert(send, base.ReformatRecord(c, record)) {
				// channel was closed
				return nil
			}
		}

		nextPageToken, err = c.nextPageToken(rawResponse)
		if err != nil {
			logger.Warnf("Error occured while getting next page token from response[%v] for stream %s: %s", rawResponse, c.Name(), err)
			paginationComplete = true
		} else if c.state_ != nil && nextPageToken["after"].(int) >= 10000 {
			// Hubspot documentation states that the search endpoints are limited to 10,000 total results
			// for any given query. Attempting to page beyond 10,000 will result in a 400 error.
			// https://developers.hubspot.com/docs/api/crm/search. We stop getting data at 10,000 and
			// start a new search query with the latest state that has been collected.
			c.updateState(*latest_cursor, false)
			nextPageToken = nil
		}
	}

	c.updateState(*latest_cursor, true)
	return nil
}
