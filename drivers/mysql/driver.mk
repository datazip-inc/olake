# mysql fragment for the root Makefile (see the driver-fragment contract there).

PROBE.mysql = docker exec olake_mysql-test mysql -h localhost -u root -proot1234 -e "SELECT 1"
