#!/usr/bin/env bash
set -euo pipefail

# Wrap mongosh against our container:
function mongosh() {
  # note: container_name is "olake_mongo-test"
  echo "$@" | docker exec -i olake_mongo-test \
    mongosh --username olake --password olake --authenticationDatabase admin
}

# 1) initiate RS
mongosh 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'

# 2) wait for election
sleep 5

# 3) reconfigure so the member host resolves from outside:
mongosh '
  cfg = rs.conf();
  cfg.members[0].host = "localhost:27017";
  rs.reconfig(cfg, {force: true});
'

echo "Replica set initiated."
