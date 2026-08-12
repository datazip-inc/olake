package parser

import (
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

	//check nested object
	customerType, err := result.Schema.GetType("customer")
	require.NoError(t, err)
	assert.Equal(t, types.Object, customerType)

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

	//one row per order, attributes and nested columns as fields
	assert.Equal(t, "1001", records[0]["_id"])
	assert.Equal(t, "NEW", records[0]["_status"])
	assert.Equal(t, "2024-08-01", records[0]["order_date"])
	customer, ok := records[0]["customer"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Alice", customer["name"])
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

	//Attribute values as level 0 record fields
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

	//child <name> map: attr is _name, text is name
	nameVal, ok := records[0]["name"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "attr", nameVal["_name"])
	assert.Equal(t, "textval", nameVal["name"])
}
