#!/bin/bash

# Script to grant REPLICATION SLAVE privilege to olake_user in MySQL

MYSQL_CONTAINER="db"
MYSQL_ROOT_USER="root"
MYSQL_ROOT_PASSWORD="olake"
MYSQL_USER="olake_user"

# Check if MySQL container is running
if ! docker ps -q -f "name=${MYSQL_CONTAINER}" >/dev/null; then
    echo "Error: MySQL container '${MYSQL_CONTAINER}' is not running."
    exit 1
fi

# Grant REPLICATION SLAVE privilege
echo "Granting REPLICATION SLAVE privilege to ${MYSQL_USER}..."
docker exec -i "${MYSQL_CONTAINER}" mysql -u"${MYSQL_ROOT_USER}" -p"${MYSQL_ROOT_PASSWORD}" <<EOF
GRANT REPLICATION SLAVE ON *.* TO '${MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
EOF

echo "Privilege granted successfully."
