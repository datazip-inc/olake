# mongodb fragment for the root Makefile (see the driver-fragment contract there).

PROBE.mongodb = docker exec primary_mongo mongosh --host localhost --port 27017 -u mongodb -p secure_password123 --authenticationDatabase admin --eval "db.adminCommand('ping')"
