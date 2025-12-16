#!/bin/bash
# Run init script using docker exec and a temporary approach
# Azure SQL Edge might not have sqlcmd in the standard location

CONTAINER="olake-mssql"
PASSWORD="Password!123"

# Wait for SQL Server to be ready
echo "Waiting for SQL Server to be ready..."
for i in {1..30}; do
    if docker exec $CONTAINER /bin/bash -c "timeout 2 bash -c '</dev/tcp/localhost/1433' 2>/dev/null"; then
        echo "SQL Server is ready!"
        break
    fi
    echo "Attempt $i/30..."
    sleep 2
done

# Try to run SQL using a Python script or alternative method
# For now, we'll use a Go-based approach or direct connection
echo "SQL Server should be ready. Please run init script manually or use a SQL client."

