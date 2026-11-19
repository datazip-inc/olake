# One-time Horizon setup from OLake docs (docs/shared/integrations/catalogs/horizon/Setup.mdx). New objects only.
# Run with: ./snowflake/sf_run.sh sf_horizon_setup.py
# Required in .snowflake_bench: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE,
#   SNOWFLAKE_ALLOWED_IP (public IP of the benchmark host, the only address allowed to use the token).
# Optional: SNOWFLAKE_ROLE (needs rights to create databases, roles, users and network policies; default ACCOUNTADMIN),
#   SNOWFLAKE_HORIZON_DB (default OLAKE_BENCH_HZ), SNOWFLAKE_HORIZON_ROLE (default OLAKE_ROLE), SNOWFLAKE_HORIZON_USER (default OLAKE_SVC).
# The token secret is written to snowflake/.pat (git-ignored, never printed).
import os, snowflake.connector as sc
E = os.environ
WH, IP = E["SNOWFLAKE_WAREHOUSE"], E["SNOWFLAKE_ALLOWED_IP"]
DB = E.get("SNOWFLAKE_HORIZON_DB") or "OLAKE_BENCH_HZ"
ROLE = E.get("SNOWFLAKE_HORIZON_ROLE") or "OLAKE_ROLE"
USER = E.get("SNOWFLAKE_HORIZON_USER") or "OLAKE_SVC"
c = sc.connect(account=E["SNOWFLAKE_ACCOUNT"], user=E["SNOWFLAKE_USER"], password=E["SNOWFLAKE_PASSWORD"],
               role=E.get("SNOWFLAKE_ROLE") or "ACCOUNTADMIN", warehouse=WH)
cur = c.cursor()
stmts = f"""CREATE DATABASE IF NOT EXISTS {DB}
ALTER DATABASE {DB} SET CATALOG = 'SNOWFLAKE'
ALTER DATABASE {DB} SET EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
CREATE ROLE IF NOT EXISTS {ROLE}
GRANT USAGE, CREATE SCHEMA ON DATABASE {DB} TO ROLE {ROLE}
GRANT USAGE ON WAREHOUSE {WH} TO ROLE {ROLE}
GRANT ROLE {ROLE} TO ROLE SYSADMIN
CREATE USER IF NOT EXISTS {USER} TYPE = SERVICE DEFAULT_ROLE = {ROLE}
GRANT ROLE {ROLE} TO USER {USER}
USE SCHEMA {DB}.PUBLIC
CREATE NETWORK RULE IF NOT EXISTS olake_ip MODE = INGRESS TYPE = IPV4 VALUE_LIST = ('{IP}/32')
CREATE NETWORK POLICY IF NOT EXISTS olake_network_policy ALLOWED_NETWORK_RULE_LIST = ('{DB}.PUBLIC.olake_ip')
ALTER USER {USER} SET NETWORK_POLICY = olake_network_policy"""
for st in stmts.split("\n"):
    try: cur.execute(st); print("OK ", st[:100], "->", str(cur.fetchone())[:120])
    except Exception as e: print("ERR", st[:100], "->", str(e).replace("\n", " ")[:300])
try:
    cur.execute(f"ALTER USER {USER} ADD PROGRAMMATIC ACCESS TOKEN OLAKE_PAT ROLE_RESTRICTION = '{ROLE}' DAYS_TO_EXPIRY = 7")
    row = cur.fetchone(); cols = [d[0].lower() for d in cur.description]
    secret = row[cols.index("token_secret")] if "token_secret" in cols else max((str(x) for x in row), key=len)
    fd = os.open("/s/.pat", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, secret.encode()); os.close(fd)
    print("OK  token created, columns:", cols, "secret length", len(secret))
except Exception as e: print("ERR token ->", str(e).replace("\n", " ")[:300])
