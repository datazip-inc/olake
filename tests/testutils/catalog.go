package testutils

import (
	"fmt"
	"strings"
)

// The catalog files in a suite's working directory
const (
	AvailableStreamsFile = "available_streams.json"
	SelectedStreamsFile  = "selected_streams.json"
	// StreamsFile is the streams v1 catalog
	StreamsFile = "streams.json"
)

// CatalogFiles is one catalog in a suite's working directory: the available_streams.json +
// selected_streams.json pair, or streams.json under streams v1. Either way the harness reads and
// edits it as a single {"streams": [...], "selected_streams": {...}} document, the shape both
// formats share.
type CatalogFiles struct {
	streamsV1 bool
	streams   string
	available string
	selected  string
}

// Catalog is the working catalog discover writes and sync reads.
func (c *TestConfig) Catalog() CatalogFiles {
	return c.catalogNamed("")
}

// catalogNamed is the catalog whose files carry prefix ahead of their usual names.
func (c *TestConfig) catalogNamed(prefix string) CatalogFiles {
	return CatalogFiles{
		streamsV1: c.StreamsV1,
		streams:   c.GetFilePath(prefix + StreamsFile),
		available: c.GetFilePath(prefix + AvailableStreamsFile),
		selected:  c.GetFilePath(prefix + SelectedStreamsFile),
	}
}

// RenderCatalog renders the committed catalog templates into the catalog whose files carry prefix
// ahead of their usual names, an empty prefix being the working catalog, and returns it.
func (c *TestConfig) RenderCatalog(prefix string) (CatalogFiles, error) {
	files := []string{AvailableStreamsFile, SelectedStreamsFile}
	if c.StreamsV1 {
		files = []string{StreamsFile}
	}
	for _, file := range files {
		if err := c.RenderConfig(catalogTemplate(file), prefix+file); err != nil {
			return CatalogFiles{}, err
		}
	}
	return c.catalogNamed(prefix), nil
}

// catalogTemplate names the committed template a catalog file renders from.
func catalogTemplate(file string) string {
	return strings.TrimSuffix(file, ".json") + ".template.json"
}

// CatalogArgs points olake at the config's working catalog.
func (c *TestConfig) CatalogArgs() []string {
	if c.StreamsV1 {
		return []string{"--catalog", ContainerPath(StreamsFile)}
	}
	return []string{"--available-streams", ContainerPath(AvailableStreamsFile), "--selected-streams", ContainerPath(SelectedStreamsFile)}
}

func (f CatalogFiles) String() string {
	if f.streamsV1 {
		return f.streams
	}
	return fmt.Sprintf("%s + %s", f.available, f.selected)
}

// Read loads the catalog as a single {"streams", "selected_streams"} document.
func (f CatalogFiles) Read() (map[string]interface{}, error) {
	if f.streamsV1 {
		return readJSONDoc(f.streams)
	}
	available, err := readJSONDoc(f.available)
	if err != nil {
		return nil, err
	}
	selected, err := readJSONDoc(f.selected)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"streams":          available["streams"],
		"selected_streams": selected["selected_streams"],
	}, nil
}

// Write stores a {"streams", "selected_streams"} document as the catalog.
func (f CatalogFiles) Write(doc map[string]interface{}) error {
	if f.streamsV1 {
		return writeJSONDoc(f.streams, doc)
	}
	if err := writeJSONDoc(f.available, map[string]interface{}{"streams": doc["streams"]}); err != nil {
		return err
	}
	return writeJSONDoc(f.selected, map[string]interface{}{"selected_streams": doc["selected_streams"]})
}

// Edit reads the catalog, applies edit to it, and writes it back.
func (f CatalogFiles) Edit(edit func(doc map[string]interface{}) error) error {
	doc, err := f.Read()
	if err != nil {
		return err
	}
	if err := edit(doc); err != nil {
		return err
	}
	return f.Write(doc)
}

// AvailableStreamEntry returns the "stream" object of the streams[] entry for namespace.name, nil when absent.
func AvailableStreamEntry(doc map[string]interface{}, namespace, name string) map[string]interface{} {
	entries, _ := doc["streams"].([]interface{})
	for _, raw := range entries {
		wrapper, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		stream, ok := wrapper["stream"].(map[string]interface{})
		if ok && stream["namespace"] == namespace && stream["name"] == name {
			return stream
		}
	}
	return nil
}

// SelectedStreamEntry returns the selected_streams entry for namespace.name, nil when absent.
func SelectedStreamEntry(doc map[string]interface{}, namespace, name string) map[string]interface{} {
	selected, _ := doc["selected_streams"].(map[string]interface{})
	entries, _ := selected[namespace].([]interface{})
	for _, raw := range entries {
		entry, ok := raw.(map[string]interface{})
		if ok && fmt.Sprint(entry["stream_name"]) == name {
			return entry
		}
	}
	return nil
}
