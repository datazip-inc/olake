#!/bin/bash

set -e

echo "Waiting for keyfile..."
while [ ! -f /etc/mongodb/pki/keyfile ]; do 
    sleep 1
done

echo "Keyfile found, starting mongod without authentication first..."
mongod --replSet rs0 --bind_ip_all --port 27017 &
MONGO_PID=$!
echo "MongoDB started with PID: $MONGO_PID"

sleep 3

echo "Waiting for MongoDB to start..."
until mongosh --port 27017 --eval "db.runCommand({ ping: 1 })" >/dev/null 2>&1; do
    sleep 2
done

echo "Initializing replica set..."
mongosh --port 27017 --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'host.docker.internal:27017'}]})"

echo "Waiting for PRIMARY..."
for i in {1..30}; do
    STATE=$(mongosh --quiet --port 27017 --eval "rs.isMaster().ismaster" 2>/dev/null)
    if [ "$STATE" = "true" ]; then
        echo "PRIMARY elected"
        break
    fi
    echo "Still waiting for PRIMARY..."
    sleep 2
done

echo "Creating admin user..."
mongosh --port 27017 --eval "
    db = db.getSiblingDB('admin');
    db.createUser({
        user: 'admin',
        pwd: 'password',
        roles: [{ role: 'root', db: 'admin' }]
    });

    db = db.getSiblingDB('olake_mongodb_test');
    db.createCollection('test_collection');

    db = db.getSiblingDB('admin');
    
    try { db.dropUser('mongodb'); } catch(e) { print('User mongodb does not exist, skipping drop'); }

    db.createUser({
        user: 'mongodb',
        pwd: 'secure_password123',
        roles: [
            { role: 'readWrite', db: 'olake_mongodb_test' }
        ]
    });

    try { db.dropRole('splitVectorRole'); } catch(e) { print('Role splitVectorRole does not exist, skipping drop'); }

    db.createRole({
        role: 'splitVectorRole',
        privileges: [
            {
                resource: { db: '', collection: '' },
                actions: [ 'splitVector' ]
            }
        ],
        roles: []
    });

    db.grantRolesToUser('mongodb', [
        { role: 'splitVectorRole', db: 'admin' }
    ]);
"

echo "Created user and role"
echo "Stopping MongoDB to restart with authentication..."
if [ ! -z "$MONGO_PID" ] && kill -0 $MONGO_PID 2>/dev/null; then
    echo "Killing MongoDB process $MONGO_PID"
    kill $MONGO_PID
    wait $MONGO_PID
else
    echo "MongoDB process not found, using pkill"
    pkill mongod
    sleep 2
fi

echo "Starting MongoDB with authentication..."
exec mongod --replSet rs0 --bind_ip_all --port 27017 --keyFile /etc/mongodb/pki/keyfile 