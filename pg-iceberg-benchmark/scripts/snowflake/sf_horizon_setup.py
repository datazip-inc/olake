# One-time Horizon setup from OLake docs (docs/shared/integrations/catalogs/horizon/Setup.mdx). New objects only.
# The token secret is written to /s/.pat (never printed).
import os, snowflake.connector as sc
c = sc.connect(account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"], password=os.environ["SNOWFLAKE_PASSWORD"], role="ACCOUNTADMIN", warehouse="COMPUTE_WH")
cur = c.cursor(); IP = os.environ["HOST_IP"]
stmts = """CREATE DATABASE IF NOT EXISTS OLAKE_BENCH_HZ
ALTER DATABASE OLAKE_BENCH_HZ SET CATALOG = 'SNOWFLAKE'
ALTER DATABASE OLAKE_BENCH_HZ SET EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
CREATE ROLE IF NOT EXISTS OLAKE_ROLE
GRANT USAGE, CREATE SCHEMA ON DATABASE OLAKE_BENCH_HZ TO ROLE OLAKE_ROLE
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE OLAKE_ROLE
GRANT ROLE OLAKE_ROLE TO ROLE SYSADMIN
CREATE USER IF NOT EXISTS OLAKE_SVC TYPE = SERVICE DEFAULT_ROLE = OLAKE_ROLE
GRANT ROLE OLAKE_ROLE TO USER OLAKE_SVC
USE SCHEMA OLAKE_BENCH_HZ.PUBLIC
CREATE NETWORK RULE IF NOT EXISTS olake_ip MODE = INGRESS TYPE = IPV4 VALUE_LIST = ('%s/32')
CREATE NETWORK POLICY IF NOT EXISTS olake_network_policy ALLOWED_NETWORK_RULE_LIST = ('OLAKE_BENCH_HZ.PUBLIC.olake_ip')
ALTER USER OLAKE_SVC SET NETWORK_POLICY = olake_network_policy""" % IP
for st in stmts.split("\n"):
    try: cur.execute(st); print("OK ", st[:100], "->", str(cur.fetchone())[:120])
    except Exception as e: print("ERR", st[:100], "->", str(e).replace("\n", " ")[:300])
try:
    cur.execute("ALTER USER OLAKE_SVC ADD PROGRAMMATIC ACCESS TOKEN OLAKE_PAT ROLE_RESTRICTION = 'OLAKE_ROLE' DAYS_TO_EXPIRY = 7")
    row = cur.fetchone(); cols = [d[0].lower() for d in cur.description]
    secret = row[cols.index("token_secret")] if "token_secret" in cols else max((str(x) for x in row), key=len)
    fd = os.open("/s/.pat", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, secret.encode()); os.close(fd)
    print("OK  token created, columns:", cols, "secret length", len(secret))
except Exception as e: print("ERR token ->", str(e).replace("\n", " ")[:300])
