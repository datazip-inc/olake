package io.debezium.server.iceberg;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public class MessageParser {

    // Main parsing method - call this with your JSON string
    public static Message parse(String json) throws Exception {
        return new ObjectMapper().readValue(json, Message.class);
    }

    // Base message type with common fields
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = RecordsMessage.class, name = "records"),
        @JsonSubTypes.Type(value = CommitMessage.class, name = "commit")
    })
    @JsonIgnoreProperties(ignoreUnknown = true) // Ignore unknown fields
    public static abstract class Message {
        public String type;
        public Metadata metadata;
    }

    // Records message type
    public static class RecordsMessage extends Message {
        public List<Record> records;
    }

    // Commit message type
    public static class CommitMessage extends Message {
        // No additional fields needed
    }

    // Shared metadata structure
    public static class Metadata {
        public String dest_table_name;
        public String thread_id;
        public String primary_key; // Only present in records messages
    }

    // Record structure
    public static class Record {
        public List<RecordItem> record;
        public String record_type; // "u"/"c"/"r"
    }

    // Record item structure
    public static class RecordItem {
        public String ice_type;
        public String key;
        public String value;
    }
}