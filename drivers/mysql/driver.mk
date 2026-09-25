# mysql fragment for the root Makefile (see the driver-fragment contract there).

PROBE.mysql = docker exec olake_mysql-test mysql -h localhost -u root -proot1234 -e "SELECT 1"

MYSQL_QUERY = docker exec olake_mysql-test mysql -h localhost -u root -proot1234 -N -B -e

# EXPECT_VERSION must come from outside the compose files -- deriving it from them would compare
# the stack against itself and pass whatever booted. Empty means the default stack, nothing to
# assert. 5.7 predates binlog_row_metadata, and that absence is what pkg/binlog falls back for.
VERIFY_STACK.mysql = \
	version=$$($(MYSQL_QUERY) "SELECT VERSION()"); \
	echo "mysql server reports $$version (expected: $(or $(EXPECT_VERSION),any))"; \
	if [ -n '$(EXPECT_VERSION)' ]; then \
		case "$$version" in '$(EXPECT_VERSION)'*) ;; \
			*) echo "ERROR: expected a $(EXPECT_VERSION) server, got '$$version'"; exit 1;; esac; \
	fi; \
	case "$$version" in 5.7*) \
		meta=$$($(MYSQL_QUERY) "SHOW VARIABLES LIKE 'binlog_row_metadata'" 2>/dev/null); \
		if [ -n "$$meta" ]; then \
			echo "ERROR: expected binlog_row_metadata to be absent on 5.7, got: $$meta"; exit 1; fi; \
		echo "binlog_row_metadata absent, as expected on 5.7";; esac
