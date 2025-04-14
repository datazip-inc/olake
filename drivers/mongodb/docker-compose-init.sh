function mongosh() {
	echo "$@" | docker exec -i mongodb-db-1 mongosh -u olake -p olake admin
}

mongosh 'rs.initiate()'
sleep 3
mongosh 'cfg = rs.conf(); cfg.members[0].host="localhost:27017"; rs.reconfig(cfg);'