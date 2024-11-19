# MongoDB Driver

The MongoDB Driver enables data synchronization from MongoDB to your desired destination. It supports both **Full Refresh** and **CDC (Change Data Capture)** modes.

---

## Supported Modes

1. **Full Refresh**  
   Fetches the complete dataset from MongoDB.

2. **CDC (Change Data Capture)**  
   Tracks and syncs incremental changes from MongoDB in real time.

---

## Setup and Configuration

To run the MongoDB Driver, configure the following files with your specific credentials and settings:

- **`config.json`**: MongoDB connection details.  
- **`catalog.json`**: List of collections and fields to sync (generated using the *Discover* command).  
- **`write.json`**: Configuration for the destination where the data will be written.

Place these files in your project directory before running the commands.

---

## Commands

### Discover Command

The *Discover* command generates the `catalog.json` file, which defines the schema of the collections to be synced.

```bash
./build.sh driver-mongodb sync --config ./mongodb/examples/config.json
```
- **Input**:  
  - `config.json`: MongoDB connection details.  
- **Output**:  
  - `catalog.json`: Discovered schema.

---

### Sync Command

The *Sync* command fetches data from MongoDB and ingests it into the destination.

```bash
./build.sh driver-mongodb sync --config ./mongodb/examples/config.json --catalog ./mongodb/examples/catalog.json --destination ./mongodb/examples/write.json
```

