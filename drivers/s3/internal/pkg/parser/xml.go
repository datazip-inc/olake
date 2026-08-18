package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"golang.org/x/net/html/charset"
)

const defaultMaxBytesForInference int64 = 10 * 1024 * 1024

// XMLParser implements the parser interface for XML files
type XMLParser struct {
	config XMLConfig
	stream *types.Stream
}

// NewXMLParser creates a new XML parser with the given configuration
func NewXMLParser(config XMLConfig, stream *types.Stream) *XMLParser {
	return &XMLParser{
		config: config,
		stream: stream,
	}
}

// InferSchema reads the first few records of an XML file to infer the schema
// Supports XML with a specified record tag or entire document as a single record
func (p *XMLParser) InferSchema(_ context.Context, reader io.Reader) (*types.Stream, error) {
	logger.Debug("Inferring XML schema from sample data")

	//TODO : implement sampling of records from first and last files to get more accurate schema
	maxSamples := 100

	var data []byte
	var err error

	if p.config.RowIdentifier == "" {
		data, err = io.ReadAll(reader)
	} else {
		limitedReader := io.LimitReader(reader, p.maxBytesForInference())
		data, err = io.ReadAll(limitedReader)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read XML file: %s", err)
	}

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty XML file")
	}

	sampleRecords, err := p.parseSampleXMLContent(trimmed, maxSamples)
	if err != nil {
		return nil, fmt.Errorf("failed to parse XML: %s", err)
	}

	if len(sampleRecords) == 0 {
		return nil, fmt.Errorf("no records found in XML file")
	}

	// Resolve schema for each sample Record and update stream schema
	for i, record := range sampleRecords {
		if err := typeutils.Resolve(p.stream, record); err != nil {
			return nil, fmt.Errorf("failed to resolve schema for record %d: %s", i, err)
		}
	}

	logger.Debugf("Inferred schema from XML file")
	return p.stream, nil
}

// StreamRecords reads and streams records from XML reader with context support
func (p *XMLParser) StreamRecords(ctx context.Context, reader io.Reader, callback RecordCallback) error {
	recordCount := 0

	if p.config.RowIdentifier == "" {
		// check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			record, err := p.parseXMLDocumentAsMap(reader)
			if err != nil {
				return fmt.Errorf("failed to parse XML document: %s", err)
			}

			if err := callback(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %s", err)
			}
			recordCount++
		}
	} else {
		// Stream XML records based on specified record tag
		decoder := p.newXMLDecoder(reader)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Read next XML token
			token, err := decoder.Token()
			if err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf("error reading XML token at record %d: %v", recordCount, err)
			}

			// Process only start elements that match the specified record tag
			startElement, ok := token.(xml.StartElement)
			if !ok || startElement.Name.Local != p.config.RowIdentifier {
				continue
			}

			// Parse XML element into a record
			record, err := p.parseXMLElement(decoder, startElement, nil)
			if err != nil {
				if !errors.Is(err, errNullValue) {
					logger.Warnf("skipping XML record %d due to %v", recordCount, err)
					continue
				}
				record = nil
			}

			// Convert parsed record to a map
			recordMap, err := p.parseXMLRecordAsMap(record, startElement.Name.Local)
			if err != nil {
				logger.Warnf("Error converting XML record %d: %v", recordCount, err)
				continue
			}

			// callback processing the record
			if err := callback(ctx, recordMap); err != nil {
				return fmt.Errorf("failed to process record: %s", err)
			}
			recordCount++
		}
	}
	logger.Infof("Processed %d records from XML file", recordCount)
	return nil
}

// parseSampleXMLContent parses XML data and returns a slice of records based on specified record tag or entire document
func (p *XMLParser) parseSampleXMLContent(data []byte, maxSamples int) ([]map[string]any, error) {
	if p.config.RowIdentifier == "" {
		record, err := p.parseXMLDocumentAsMap(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		return []map[string]any{record}, nil
	}

	logger.Debug("Parsing XML records by row_identifier")

	decoder := p.newXMLDecoder(bytes.NewReader(data))
	records := make([]map[string]any, 0, maxSamples)

	// Loop through XML tokens and extract records with specified record tag
	for len(records) < maxSamples {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			if len(records) > 0 && isTruncatedXMLErr(err) {
				logger.Warnf("Stopped reading XML after %d records (truncated): %v", len(records), err)
				break
			}
			return nil, fmt.Errorf("xml token error: %s", err)
		}

		// process start elements matching record tag
		startElement, ok := token.(xml.StartElement)
		if !ok || startElement.Name.Local != p.config.RowIdentifier {
			continue
		}

		// parse XML element into a record
		record, err := p.parseXMLElement(decoder, startElement, nil)
		if err != nil {
			if errors.Is(err, errNullValue) {
				record = nil
			} else if len(records) > 0 && isTruncatedXMLErr(err) {
				logger.Warnf("stopped reading XML after %d records (truncated): %v", len(records), err)
				break
			} else {
				return nil, err
			}
		}

		// convert parsed record to a map
		recordMap, err := p.parseXMLRecordAsMap(record, startElement.Name.Local)
		if err != nil {
			return nil, err
		}
		records = append(records, recordMap)
	}

	logger.Infof("Parsed %d records from XML for schema inference", len(records))
	return records, nil
}

// parseXMLDocumentAsMap parses the entire XML document as a single record and returns it as a map
func (p *XMLParser) parseXMLDocumentAsMap(reader io.Reader) (map[string]any, error) {
	decoder := p.newXMLDecoder(reader)

	// find root element and parse it as a single record
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return nil, fmt.Errorf("empty XML document")
		}
		if err != nil {
			return nil, fmt.Errorf("xml token error: %s", err)
		}

		startElement, ok := token.(xml.StartElement)
		if !ok {
			continue
		}

		record, err := p.parseXMLElement(decoder, startElement, nil)
		if err != nil {
			if !errors.Is(err, errNullValue) {
				return nil, err
			}
			record = nil
		}

		return p.documentRootAsMap(record, startElement.Name.Local)
	}
}

// parseXMLElement recursively parses an XML element into a map or string.
// Unique children overwrite; nested elements are stored as JSON strings.
// Attributes become sibling fields with a _ prefix; xmlns attributes are skipped.
func (p *XMLParser) parseXMLElement(decoder *xml.Decoder, startElement xml.StartElement, ns map[string]string) (any, error) {
	ns = p.withNameSpace(ns, startElement)
	fields := make(map[string]any)

	hasAttributes := false
	for _, attr := range startElement.Attr {
		if p.isXMLNSAttribute(attr) {
			continue
		}
		fields["_"+p.xmlName(attr.Name, ns)] = attr.Value
		hasAttributes = true
	}

	var text strings.Builder
	hasChildren := false

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return nil, fmt.Errorf("unexpected EOF while parsing <%s>: %w", startElement.Name.Local, io.ErrUnexpectedEOF)
		}
		if err != nil {
			return nil, fmt.Errorf("xml token error: %w", err)
		}

		// process XML tokens recursively - start elements, char data, end elements
		// ignore comments, directives, and processing instructions
		switch t := token.(type) {
		case xml.StartElement:
			hasChildren = true
			child, err := p.parseXMLElement(decoder, t, ns)
			if err != nil {
				if !errors.Is(err, errNullValue) {
					return nil, err
				}
				child = nil
			}
			if nested, ok := child.(map[string]any); ok {
				nestedJSON, err := json.Marshal(nested)
				if err != nil {
					return nil, err
				}
				child = string(nestedJSON)
			}
			fields[p.xmlName(t.Name, p.withNameSpace(ns, t))] = child

		case xml.CharData:
			if !hasChildren {
				text.Write([]byte(t))
			}

		case xml.EndElement:
			if t.Name.Local != startElement.Name.Local {
				continue
			}
			if hasChildren {
				return fields, nil
			}
			textValue := strings.TrimSpace(text.String())
			if hasAttributes {
				if textValue != "" {
					fields[p.xmlName(startElement.Name, ns)] = textValue
				}
				return fields, nil
			}
			if textValue == "" {
				return nil, errNullValue
			}
			return textValue, nil

		case xml.Comment, xml.Directive, xml.ProcInst:
			// ignore
		}
	}
}

// parseXMLRecordAsMap converts a parsed XML record into a map[string]any
func (p *XMLParser) parseXMLRecordAsMap(value any, tag string) (map[string]any, error) {
	switch v := value.(type) {
	case nil:
		return map[string]any{}, nil
	case map[string]any:
		return v, nil
	case string:
		if v == "" {
			return map[string]any{}, nil
		}
		return map[string]any{tag: v}, nil
	default:
		return nil, fmt.Errorf("invalid XML value type %T", value)
	}
}

// documentRootAsMap converts a parsed XML document root into a map[string]any
func (p *XMLParser) documentRootAsMap(record any, tag string) (map[string]any, error) {
	switch v := record.(type) {
	case nil:
		return map[string]any{tag: nil}, nil
	case string:
		return map[string]any{tag: v}, nil
	default:
		content, err := p.parseXMLRecordAsMap(record, tag)
		if err != nil {
			return nil, err
		}
		return map[string]any{tag: content}, nil
	}
}

// maxBytesForInference returns the maximum number of bytes to read for schema inference
func (p *XMLParser) maxBytesForInference() int64 {
	if p.config.MaxBytesForInference > 0 {
		return p.config.MaxBytesForInference
	}
	return defaultMaxBytesForInference
}

// isTruncatedXMLErr checks if the error is a truncated XML error
func isTruncatedXMLErr(err error) bool {
	var syntaxErr *xml.SyntaxError
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		(errors.As(err, &syntaxErr) && strings.Contains(syntaxErr.Msg, "unexpected EOF"))
}

// isXMLNSAttribute checks if the attribute is a xml namespace attribute
func (p *XMLParser) isXMLNSAttribute(attr xml.Attr) bool {
	if attr.Name.Space == "" && attr.Name.Local == "xmlns" {
		return true
	}
	return attr.Name.Space == "xmlns"
}

// xmlName returns prefix_local when the URI is in ns, otherwise Local.
func (p *XMLParser) xmlName(name xml.Name, ns map[string]string) string {
	if prefix := ns[name.Space]; prefix != "" {
		return prefix + "_" + name.Local
	}
	return name.Local
}

// withNameSpace copies parent prefixes and adds xmlns:prefix declarations from the element.
func (p *XMLParser) withNameSpace(namespace map[string]string, element xml.StartElement) map[string]string {
	out := make(map[string]string, len(namespace)+2)
	for k, v := range namespace {
		out[k] = v
	}
	for _, attr := range element.Attr {
		if attr.Name.Space == "xmlns" {
			out[attr.Value] = attr.Name.Local
		}
	}
	return out
}

// newXMLDecoder creates a new XML decoder with a charset reader
func (p *XMLParser) newXMLDecoder(r io.Reader) *xml.Decoder {
	decoder := xml.NewDecoder(r)
	decoder.CharsetReader = charset.NewReaderLabel
	return decoder
}
