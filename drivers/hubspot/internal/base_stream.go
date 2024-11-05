package driver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
)

type Stream struct {
	name              string
	entity            string
	updatedAtField    string
	createdAtField    string
	lastModifiedField string
	moreKey           string
	dataField         string
	pageFilter        string
	pageField         string
	limitField        string
	limit             int
	offset            int
	batchSize         int
	availableSyncMode []types.SyncMode

	primaryKey string
	groupByKey string

	denormalizeRecords bool

	client           *http.Client
	grantedScopes    []string
	scopes           []string
	propertiesScopes []string
	_properties      map[string]*types.Property

	startDate time.Time
}

func newStream(name, entity string, client *http.Client, startDate time.Time) *Stream {
	return &Stream{
		name:              name,
		entity:            entity,
		client:            client,
		startDate:         startDate,
		primaryKey:        "id",
		dataField:         "results",
		pageFilter:        "offset",
		pageField:         "offset",
		limitField:        "limit",
		limit:             100,
		offset:            0,
		availableSyncMode: []types.SyncMode{types.FULLREFRESH},
	}
}

func (s *Stream) Name() string {
	return s.name
}

func (s *Stream) Namespace() string {
	return ""
}

func (s *Stream) Modes() []types.SyncMode {
	return s.availableSyncMode
}

func (s *Stream) PrimaryKey() []string {
	if s.primaryKey == "" {
		return []string{s.primaryKey}
	}
	return nil
}

func (s *Stream) properties() (map[string]*types.Property, error) {
	if s.entity == "" {
		return nil, fmt.Errorf("entity found to be empty")
	}

	if !s.propertiesScopeIsGranted() {
		return nil,
			fmt.Errorf("Check your API key has the following permissions granted: %v to be able to fetch all properties available.", s.propertiesScopes)
	}

	if s._properties != nil {
		return s._properties, nil
	}

	req, err := http.NewRequest("GET", formatEndpoint(fmt.Sprintf("/properties/v2/%s/properties", s.entity)), nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	response := []map[string]any{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	s._properties = make(map[string]*types.Property)

	for _, row := range response {
		s._properties[row["name"].(string)] = getFieldProps(row["type"].(string))
	}

	return s._properties, nil
}

func (s *Stream) propertiesList() ([]string, error) {
	properties, err := s.properties()
	if err != nil {
		return nil, err
	}

	list := []string{}
	for key := range properties {
		list = append(list, key)
	}

	return list, nil
}

func (s *Stream) propertiesScopeIsGranted() bool {
	return utils.IsSubset(s.grantedScopes, s.propertiesScopes)
}

func (s *Stream) Read(channel <-chan types.Record) error {
	return fmt.Errorf("no implementation on base stream")
}

func (s *Stream) setSyncModes(modes []types.SyncMode) {
	s.availableSyncMode = modes
}

func (s *Stream) ScopeIsGranted(grantedScopes []string) bool {
	s.grantedScopes = types.Set(grantedScopes)
	return utils.IsSubset(grantedScopes, s.scopes)
}

func (s *Stream) backoffTime(response *http.Response) float64 {
	if response.StatusCode == http.StatusTooManyRequests {
		parsed, err := strconv.ParseFloat(response.Header.Get("Retry-After"), 64)
		if err != nil {
			logger.Errorf("failed to parse retry-after: %s", err)
			return 0
		}

		return parsed
	}

	return 0
}

func (s *Stream) nextPageToken(response any) (map[string]any, error) {
	if resp, ok := response.(map[string]any); ok {
		if value, found := resp["paging"]; found {
			if paging, ok := value.(map[string]any); ok {
				if value, found := paging["next"]; found {
					if next, ok := value.(map[string]any); ok {
						return map[string]any{"after": next["after"]}, nil
					}
				}
			}
		} else {
			if s.moreKey != "" {
				if _, found := resp[s.moreKey]; !found {
					return nil, fmt.Errorf("key[%s] not found in response", s.moreKey)
				}
				if value, found := resp[s.pageField]; found {
					return map[string]any{s.pageFilter: value}, nil
				}
			}
			if _, found := resp[s.pageField]; found {
				if resp[s.pageFilter].(int)+s.limit < resp["total"].(int) {
					return map[string]any{s.pageFilter: resp[s.pageFilter].(int) + s.limit}, nil
				}
			}
		}
	} else if resp, ok := response.([]any); ok {
		if len(resp) >= s.limit {
			s.offset += s.limit
			return map[string]any{s.pageFilter: s.offset}, nil
		}
	}

	return nil, fmt.Errorf("failed to get next page token")
}

func (s *Stream) castRecordFieldsIfNeeded(record map[string]any) map[string]any {
	if s.entity == "" {
		return record
	}
	if _, found := record["properties"]; !found {
		return record
	}

	properties, err := s.properties()
	if err != nil {
		return record
	}

	if recordProperties, ok := record["properties"].(map[string]any); ok {
		for fieldName, fieldValue := range recordProperties {
			if _, found := properties[fieldName]; !found {
				logger.Warnf("Property discarded: not maching with properties schema: record id:%v, property_value: %s", record["id"], fieldName)
				continue
			}
			declaredFieldTypes := properties[fieldName].Type
			reformattedFieldValue, err := typeutils.ReformatValueOnDataTypes(declaredFieldTypes, fieldValue)
			if err != nil {
				logger.Warnf("failed to reformat for field[%s] to data-type:%v for record id:%v", fieldName, declaredFieldTypes, record["id"])
				continue
			}
			recordProperties[fieldName] = reformattedFieldValue
		}
	}

	return record
}

func (s *Stream) trasformSingleRecord(record map[string]any) map[string]any {
	// Preprocess a single record
	record = s.castRecordFieldsIfNeeded(record)
	if s.createdAtField != "" && s.updatedAtField != "" && record[s.updatedAtField] == nil {
		record[s.updatedAtField] = record[s.createdAtField]
	}

	return record
}

func (s *Stream) transform(records []types.Record, err error) ([]types.Record, error) {
	if err != nil {
		return nil, err
	}

	// Preprocess record before emitting
	transformed := []types.Record{}
	for _, record := range records {
		record = s.castRecordFieldsIfNeeded(record)
		if s.createdAtField != "" && s.updatedAtField != "" && record[s.updatedAtField] == nil {
			record[s.updatedAtField] = record[s.createdAtField]
		}

		transformed = append(transformed, record)
	}

	return transformed, nil
}

func (s *Stream) filterOldRecords(records []types.Record) []map[string]any {
	stream := []types.Record{}

	for _, record := range records {
		if uat, found := record[s.updatedAtField]; found {
			updatedAt, err := typeutils.ReformatDate(uat)
			if err != nil {
				logger.Warnf("failed to reformat[%s] for record %v: %s", s.updatedAtField, record, err)
				continue
			}

			if updatedAt.Before(s.startDate) {
				continue
			}
		}

		stream = append(stream, record)
	}

	return stream
}

func (s *Stream) flatAssociations(records []types.Record) []types.Record {
	// When result has associations we prefer to have it flat, so we transform this

	// "associations": {
	// 	"contacts": {
	// 		"results": [{"id": "201", "type": "company_to_contact"}, {"id": "251", "type": "company_to_contact"}]}
	// 	}
	// }

	// to this:

	// "contacts": [201, 251]

	stream := []types.Record{}
	for _, record := range records {
		if value, found := record["associations"]; found {
			delete(record, "associations")
			if associations, ok := value.(map[string]any); ok {
				for name, value := range associations {
					tempArray := []any{}
					if association, ok := value.(map[string]any); ok {
						if value, found := association["results"]; found {
							if results, ok := value.([]any); ok {
								for _, value := range results {
									if row, ok := value.(map[string]any); ok {
										tempArray = append(tempArray, row["id"])
									}
								}
							}

						}
					}
					record[strings.ReplaceAll(name, " ", "_")] = tempArray
				}
			}
		}

		// insert record
		stream = append(stream, record)
	}

	return stream
}

func (s *Stream) handleRequest(request *utils.Request) (int, any, error) {
	if request.URN == "" {
		return 0, nil, fmt.Errorf("empty request url")
	}

	req, err := request.ToHTTPRequest()
	if err != nil {
		return 0, nil, err
	}

	statusCode := 0
	var response any
	retryAfter := time.Second

	// only 3 attempts
	err = base.RetryOnFailure(3, &retryAfter, func() error {
		resp, err := s.client.Do(req)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return utils.ErrServerTimeout
			}
			urlErr, ok := err.(*url.Error)
			if ok && urlErr.Timeout() {
				return utils.ErrServerTimeout
			}

			return fmt.Errorf("Error getting response: %v", err)
		}
		defer func() {
			if resp.Body != nil {
				resp.Body.Close()
			}
		}()

		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("Error reading response: %v", err)
		}
		err = json.Unmarshal(respBody, &response)
		if err != nil {
			return fmt.Errorf("Error reading response: %v", err)
		}

		if resp.Header.Get("content-type") == "application/json;charset=utf-8" && resp.StatusCode != http.StatusOK {
			data := response.(map[string]any)
			return fmt.Errorf("%v", fmt.Sprintf("%s: %s", data["message"], string(respBody)))
		} else if resp.StatusCode == http.StatusTooManyRequests {
			retryAfterValue := resp.Header.Get("Retry-After")
			if value, err := strconv.Atoi(retryAfterValue); err != nil {
				return err
			} else {
				retryAfter = time.Second * time.Duration(value)
			}

			logger.Warnf(`Status 429 Rate Limit Exceeded: API rate-limit has been reached until %s seconds.
			See https://developers.hubspot.com/docs/api/usage-details.`,
				retryAfterValue,
			)
		}

		statusCode = resp.StatusCode
		return nil
	})
	if err != nil {
		return statusCode, nil, err
	}

	return statusCode, response, nil
}

func (s *Stream) parseResponse(response interface{}) ([]types.Record, error) {
	records := []types.Record{}
	if utils.IsInstance(response, reflect.Map) {
		response := response.(map[string]any)
		if response["status"] != nil && response["status"] == "error" {
			logger.Warnf("Stream `%s` cannot be procced. {%v}", s.Name(), response["message"])
			return nil, nil
		}

		if response[s.dataField] == nil {
			logger.Fatalf("Unexpected API response: %s not in %v", s.dataField, types.Keys(response))
		}

		// read records in the data field of response
		if data, ok := response[s.dataField].([]interface{}); ok {
			for _, rcd := range data {
				if record, ok := rcd.(map[string]any); ok {
					records = append(records, record)
				} else {
					logger.Fatalf("Unexpected API response: expected Map[string]any not %T", rcd)
				}
			}
		} else {
			logger.Fatalf("Unexpected API response: expected Array not %T", response[s.dataField])
		}
	} else if utils.IsInstance(response, reflect.Array) || utils.IsInstance(response, reflect.Slice) {
		if arr, ok := response.([]interface{}); ok {
			for _, element := range arr {
				if record, ok := element.(map[string]any); ok {
					records = append(records, record)
				} else {
					logger.Fatalf("Unexpected API response: expected Map[string]any not %T", element)
				}
			}
		} else {
			logger.Fatalf("Unexpected API response: expected Array not %T", response)
		}

	}

	return records, nil
}

func (s *Stream) readStreamRecords(nextPageToken map[string]any, f func() (path, method string)) ([]types.Record, any, error) {
	// properties = self._property_wrapper
	//     for chunk in properties.split():
	//         response = self.handle_request(
	//             stream_slice=stream_slice, streamstate_=streamstate_, next_page_token=next_page_token, properties=chunk, url=url
	//         )
	//         for record in self._transform(self.parse_response(response)):
	//             post_processor.add_record(record)
	url, method := f()
	request := &utils.Request{
		URN:         formatEndpoint(url),
		Method:      method,
		QueryParams: nextPageToken,
	}
	// populating defaults
	if request.QueryParams == nil {
		request.QueryParams = make(map[string]any)
	}
	request.QueryParams["limit"] = s.limit

	_, response, err := s.handleRequest(request)
	if err != nil {
		return nil, nil, err
	}

	records, err := s.transform(s.parseResponse(response))
	if err != nil {
		return nil, nil, err
	}

	return records, response, nil
}
