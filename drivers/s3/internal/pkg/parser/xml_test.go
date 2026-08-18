package parser

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXMLParser_InferSchema_WholeDocument(t *testing.T) {
	xmlData := `<root>
	<id>1</id>
	<name>Alice</name>
	</root>`

	config := XMLConfig{
		RowIdentifier: "",
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	//whole document wrapped under root tag
	rootType, err := result.Schema.GetType("root")
	require.NoError(t, err)
	assert.Equal(t, types.Object, rootType, "root should be inferred as Map")
}

func TestXMLParser_InferSchema_RowIdentifier(t *testing.T) {
	xmlData := `<root>
	<order id="1001" status="NEW">
		<order_date>2024-08-01</order_date>
		<customer>
			<id>CUST_55</id>
			<name>Alice</name>
		</customer>
	</order>
	</root>`

	config := XMLConfig{
		RowIdentifier: "order",
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	//check attibutes
	idType, err := result.Schema.GetType("_id")
	require.NoError(t, err)
	assert.Equal(t, types.String, idType)

	statusType, err := result.Schema.GetType("_status")
	require.NoError(t, err)
	assert.Equal(t, types.String, statusType)

	// nested object is a JSON string, date leaf infers timestamp
	customerType, err := result.Schema.GetType("customer")
	require.NoError(t, err)
	assert.Equal(t, types.String, customerType)

	orderDateType, err := result.Schema.GetType("order_date")
	require.NoError(t, err)
	assert.Equal(t, types.Timestamp, orderDateType)
}

func TestXMLParser_InferSchema_EmptyFile(t *testing.T) {
	xmlData := `	`

	config := XMLConfig{
		RowIdentifier: "",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)

	//whitespace only fail discovery
	_, err := parser.InferSchema(ctx, reader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty XML file")
}

func TestXMLParser_InferSchema_NoMatchingRowIdentifier(t *testing.T) {
	xmlData := `<root>
	<name>Alice</name>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)

	// row_identifier with no matching elements - no records
	_, err := parser.InferSchema(ctx, reader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no records found")
}

func TestXMLParser_InferSchema_Attributes(t *testing.T) {
	xmlData := `<root>
	<item id="1" type="a">
		<title>orders</title>
	</item>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Attributes become sibling fields alongside child
	idType, err := result.Schema.GetType("_id")
	require.NoError(t, err)
	assert.Equal(t, types.String, idType)

	statusType, err := result.Schema.GetType("_type")
	require.NoError(t, err)
	assert.Equal(t, types.String, statusType)

	titleType, err := result.Schema.GetType("title")
	require.NoError(t, err)
	assert.Equal(t, types.String, titleType)
}

func TestXMLParser_InferSchema_TruncatedAfterCompleteRecord(t *testing.T) {
	xmlData := `<root>
	<item>
		<name>a</name>
	</item>
	<item>
		<name>partial`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	nameType, err := result.Schema.GetType("name")
	require.NoError(t, err)
	assert.Equal(t, types.String, nameType)
}

func TestXMLParser_InferSchema_MalformedAfterCompleteRecord(t *testing.T) {
	xmlData := `<root>
	<item>
		<name>Alice</name>
	</item>
	</not-root>
	`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)

	_, err := parser.InferSchema(ctx, reader)
	require.Error(t, err)
}

func TestXMLParser_InferSchema_TruncatedBeforeAnyRecord(t *testing.T) {
	xmlData := `<root>
	<item>
		<name>partial`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)

	_, err := parser.InferSchema(ctx, reader)
	require.Error(t, err)
}

func TestXMLParser_StreamRecords_WholeDocument(t *testing.T) {
	xmlData := `<root>
	<id>1</id>
	<name>Alice</name>
	</root>`

	config := XMLConfig{
		RowIdentifier: "",
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 1)

	//one record under root name
	root, ok := records[0]["root"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "1", root["id"])
	assert.Equal(t, "Alice", root["name"])
}

func TestXMLParser_StreamRecords_RowIdentifier(t *testing.T) {
	xmlData := `<root>
	<order id="1001" status="NEW">
		<order_date>2024-08-01</order_date>
		<customer>
		<id>CUST_55</id>
		<name>Alice</name>
		</customer>
	</order>
	</root>`

	config := XMLConfig{
		RowIdentifier: "order",
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 1)

	//one row per order- attrs scalar, unique children strings, nested JSON string
	assert.Equal(t, "1001", records[0]["_id"])
	assert.Equal(t, "NEW", records[0]["_status"])
	assert.Equal(t, "2024-08-01", records[0]["order_date"])
	assert.Equal(t, `{"id":"CUST_55","name":"Alice"}`, records[0]["customer"])
}

func TestXMLParser_StreamRecords_NoMatchingRowIdentifier(t *testing.T) {
	xmlData := `<root>
	<name>Alice</name>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	//Stream succeeds with zerorecords (unlike InferSchema error)
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	assert.Len(t, records, 0)
}

func TestXMLParser_StreamRecords_Attributes(t *testing.T) {
	xmlData := `<root>
	<item id="1" type="a">
		<title>orders</title>
	</item>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 1)

	assert.Equal(t, "1", records[0]["_id"])
	assert.Equal(t, "a", records[0]["_type"])
	assert.Equal(t, "orders", records[0]["title"])
}

func TestXMLParser_StreamRecords_AttrChildNameCollision(t *testing.T) {
	xmlData := `<root>
	<order id="attrval">
		<id>childval</id>
	</order>
	</root>`

	config := XMLConfig{
		RowIdentifier: "order",
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 1)

	//attr and child with same local name stay separate (_id, id)
	assert.Equal(t, "attrval", records[0]["_id"])
	assert.Equal(t, "childval", records[0]["id"])
}

func TestXMLParser_StreamRecords_AttrSameNameAsElementWithText(t *testing.T) {
	xmlData := `<root>
	<item>
		<name name="attr">
			textval
		</name>
	</item>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 1)

	// nested <name> with attr+text is a JSON string
	assert.Equal(t, `{"_name":"attr","name":"textval"}`, records[0]["name"])
}

func TestXMLParser_StreamRecords_OverwriteSiblings(t *testing.T) {
	xmlData := `<root>
	<item>
		<tag>a</tag>
	</item>
	<item>
		<tag>a</tag>
		<tag>b</tag>
	</item>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 2)

	// unique tag is a string, a second sibling overwrites
	assert.Equal(t, "a", records[0]["tag"])
	assert.Equal(t, "b", records[1]["tag"])
}

func TestXMLParser_StreamRecords_SkipXMLNSAttributes(t *testing.T) {
	xmlData := `<root>
	<h:order xmlns:h="http://h.ns" id="1">
		<h:price>10</h:price>
		<f:price xmlns:f="http://f.ns">20</f:price>
	</h:order>
	</root>`

	config := XMLConfig{
		RowIdentifier: "order",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 1)

	assert.Equal(t, "1", records[0]["_id"])
	assert.Equal(t, "10", records[0]["h_price"])
	assert.Equal(t, "20", records[0]["f_price"])
	assert.NotContains(t, records[0], "price")
	assert.NotContains(t, records[0], "_xmlns")
	assert.NotContains(t, records[0], "_h")
	assert.NotContains(t, records[0], "_f")
}

func TestXMLParser_StreamRecords_NonUTF8Encoding(t *testing.T) {
	xmlData := []byte("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>" +
		"<root><item><name>Caf\xe9</name></item></root>")

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := bytes.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 1)

	assert.Equal(t, "Café", records[0]["name"])
}

func TestXMLParser_StreamRecords_EmptyLeavesAsNull(t *testing.T) {
	// empty / self-close / whitespace leaf is nil
	xmlData := `<root>
	<item>
		<str_col/>
		<empty_col></empty_col>
		<whitespace_col>   </whitespace_col>
		<empty_timestamp_col/>
	</item>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 1)

	assert.Equal(t, nil, records[0]["str_col"])
	assert.Equal(t, nil, records[0]["empty_col"])
	assert.Equal(t, nil, records[0]["whitespace_col"])
	assert.Equal(t, nil, records[0]["empty_timestamp_col"])
}

func TestXMLParser_StreamRecords_EmptyTimestampIsNull(t *testing.T) {
	xmlData := `<root>
	<item>
		<created>2026-08-17T14:45:30Z</created>
	</item>
	<item>
		<created></created>
	</item>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 2)

	assert.Equal(t, "2026-08-17T14:45:30Z", records[0]["created"])
	assert.Equal(t, nil, records[1]["created"])
	assert.NotEqual(t, "", records[1]["created"])
}

func TestXMLParser_StreamRecords_EmptyAttributeAsEmptyString(t *testing.T) {
	xmlData := `<root>
	<item id="">
		<name>a</name>
	</item>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 1)

	assert.Equal(t, "", records[0]["_id"])
	assert.Equal(t, "a", records[0]["name"])
}

func TestXMLParser_StreamRecords_EmptyRecordMidFile(t *testing.T) {
	xmlData := `<root>
	<item><name>a</name></item>
	<item/>
	<item><name>b</name></item>
	</root>`

	config := XMLConfig{
		RowIdentifier: "item",
	}
	stream := types.NewStream("test", "test", nil)
	parser := NewXMLParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(xmlData)
	var records []map[string]any
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}
	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 3)

	assert.Equal(t, "a", records[0]["name"])
	assert.Empty(t, records[1])
	assert.Equal(t, "b", records[2]["name"])
}
