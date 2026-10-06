package testutils

import "fmt"

// catalogFiles is one catalog on the host: the available_streams.json + selected_streams.json
// pair, or streams.json under streams v1. Either way the harness reads and edits it as a single
// {"streams": [...], "selected_streams": {...}} document, the shape both formats share.
type catalogFiles struct {
	streamsV1 bool
	streams   string
	available string
	selected  string
}

// catalog is the working catalog discover writes and sync reads.
func (t *TestConfig) catalog() catalogFiles {
	return catalogFiles{streamsV1: t.StreamsV1, streams: t.HostCatalogPath, available: t.HostAvailablePath, selected: t.HostSelectedPath}
}

// fixtureCatalog is the committed catalog a fresh discover must reproduce.
func (t *TestConfig) fixtureCatalog() catalogFiles {
	return catalogFiles{streamsV1: t.StreamsV1, streams: t.HostTestCatalogPath, available: t.HostTestAvailablePath, selected: t.HostTestSelectedPath}
}

func (f catalogFiles) String() string {
	if f.streamsV1 {
		return f.streams
	}
	return fmt.Sprintf("%s + %s", f.available, f.selected)
}

func (f catalogFiles) read() (map[string]interface{}, error) {
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

func (f catalogFiles) write(doc map[string]interface{}) error {
	if f.streamsV1 {
		return writeJSONDoc(f.streams, doc)
	}
	if err := writeJSONDoc(f.available, map[string]interface{}{"streams": doc["streams"]}); err != nil {
		return err
	}
	return writeJSONDoc(f.selected, map[string]interface{}{"selected_streams": doc["selected_streams"]})
}

// edit reads the catalog, applies edit to it, and writes it back.
func (f catalogFiles) edit(edit func(doc map[string]interface{}) error) error {
	doc, err := f.read()
	if err != nil {
		return err
	}
	if err := edit(doc); err != nil {
		return err
	}
	return f.write(doc)
}

// availableStreamEntry returns the "stream" object of the streams[] entry for namespace.name, nil when absent.
func availableStreamEntry(doc map[string]interface{}, namespace, name string) map[string]interface{} {
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

// selectedStreamEntry returns the selected_streams entry for namespace.name, nil when absent.
func selectedStreamEntry(doc map[string]interface{}, namespace, name string) map[string]interface{} {
	selected, _ := doc["selected_streams"].(map[string]interface{})
	entries, _ := selected[namespace].([]interface{})
	for _, raw := range entries {
		entry, ok := raw.(map[string]interface{})
		if ok && entry["stream_name"] == name {
			return entry
		}
	}
	return nil
}

// StreamsV1Suite is the suite a streams v1 sync runs as, so it can run alongside the default
// sync suite. Remove with streams v1 support.
const StreamsV1Suite = "v1"
