function mongosh() {
	echo "$@" | docker exec -i olake_mongodb-test mongosh -u olake -p olake admin
}

mongosh 'rs.initiate()'
sleep 3
# mongosh 'cfg = rs.conf(); cfg.members[0].host="localhost:27017"; rs.reconfig(cfg);'
mongosh 'rs.initiate({_id:"rs0", members: [{_id:0, host:"db:27017"}]})'