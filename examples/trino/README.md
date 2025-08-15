# Trino with OLake Integration Guide

This guide explains how to use Trino to query Iceberg tables managed by the OLake Docker stack.

## **Prerequisites**
Before starting, ensure:  
- The main OLake Docker stack is running.
- The iceberg-rest-catalog and minio services are healthy.  
- Data is already loaded into your Iceberg tables.
___
## **1. Configuration Overview**
The Trino setup comes with a pre-configured **iceberg.properties** file:  

```plaintext
config/trino/etc/catalog/iceberg.properties
```  

This file connects Trino to your Iceberg catalog.
___
## **2. Running Trino**

**Start the Trino Coordinator**  

From the project's root directory (```Olake```), run this command. It launches the Trino coordinator and connects it to the existing OLake services:

```bash
docker run -d \
  --name olake-trino-coordinator \
  --network app-network \
  -p 8888:8080 \
  -v "$(pwd)/config/trino/etc:/opt/trino-server/etc" \
  trinodb/trino:latest
  ```
___
## **3. Querying Data**
You can use either the **Trino UI** or a **SQL client**.  

### **Option A — Trino Web UI**
Open your browser and visit:

[http://localhost:8888](http://localhost:8888)


Log in with:
* Username: `admin`  

#### ⚠️ **Important:**  
In some setups (including the current OLake example), the Trino Web UI will not display available catalogs or schemas.  
Use the CLI or a SQL client (Option B) to view them instead. This is expected behavior — it is not a bug in your setup.


### **Option B — SQL Client (Recommended)**  

Using a SQL client like DBeaver provides a more reliable experience.  
📄 [Official DBeaver guide for Trino](https://dbeaver.com/docs/dbeaver/Database-driver-Trino/)


#### **Example Queries:**

Check if Iceberg catalog is registered:  
```sql
SHOW CATALOGS;
```  

List schemas in the Iceberg catalog:  
```sql
SHOW SCHEMAS IN iceberg;
```   

Preview data:  
```sql
SELECT * FROM iceberg.weather.weather LIMIT 10;
```
___
## **4. Troubleshooting**
View Trino container logs:  
```bash 
docker logs -f olake-trino-coordinator
```  
Check Trino container status:  
```bash 
docker ps -f name=olake-trino-coordinator
```  
Restart the Trino container:  
```bash 
docker restart olake-trino-coordinator
```
___
✅ **Tip:**    If the UI doesn’t show dropdowns for catalogs/schemas, queries will still work via CLI or a SQL client.
___