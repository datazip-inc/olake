# mysql fragment for the root Makefile (see the driver-fragment contract there).

PROBE.mysql = docker exec olake_mysql-test mysql -h localhost -u root -proot1234 -e "SELECT 1"

# Asserts the server that answered is the one the compose files asked for. MySQL's VERSION()
# starts with the image tag, so mysql:8.0 must answer 8.0.x and mysql:5.7.28 must answer
# 5.7.28-log. Without this the 5.7 variant is invisible when it breaks: every assertion in the
# sync suite passes against 8.0 too, so an EXTRA_COMPOSE_mysql that stopped threading through
# would still go green while testing nothing new.
#
# On 5.7 binlog_row_metadata does not exist at all. That absence is the whole reason the
# information_schema fallback in pkg/binlog exists, so it is asserted rather than assumed.
MYSQL_QUERY = docker exec olake_mysql-test mysql -h localhost -u root -proot1234 -N -B -e

# sed rather than $${tag\#*:}: "#" opens a comment inside a make variable assignment, which
# would truncate this value mid-expansion. Greedy .* also strips a registry host:port prefix.
VERIFY_STACK.mysql = \
	tag=$$($(COMPOSE) $(call SOURCE_COMPOSE_FILE,mysql) config --images | head -1 | sed 's|.*:||'); \
	version=$$($(MYSQL_QUERY) "SELECT VERSION()"); \
	echo "mysql image tag $$tag; server reports $$version"; \
	case "$$version" in "$$tag"*) ;; \
		*) echo "ERROR: expected a $$tag server, got '$$version'"; exit 1;; esac; \
	case "$$tag" in 5.7*) \
		meta=$$($(MYSQL_QUERY) "SHOW VARIABLES LIKE 'binlog_row_metadata'" 2>/dev/null); \
		if [ -n "$$meta" ]; then \
			echo "ERROR: expected binlog_row_metadata to be absent on $$tag, got: $$meta"; exit 1; fi; \
		echo "binlog_row_metadata absent, as expected on $$tag";; esac
