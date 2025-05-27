# Parquet-writer

## Commands

### Check Command
The *Check* command validates the destination configuration and ensures it's properly set up.

#### Usage
To check the destination configuration, use the following syntax:
```bash
./build.sh destination-parquet check --destination /path/to/destination.json
```

#### Example Response
When the check is successful, you'll see a response like this:
```json
{
  "connectionStatus": {
    "status": "SUCCEEDED"
  },
  "type": "CONNECTION_STATUS"
}
```