# usage: sf.py <sql file>   runs each statement (separated by ";\n"), prints rows. Credentials from env.
# SNOWFLAKE_ROLE and SNOWFLAKE_WAREHOUSE are optional; without them the user's defaults apply.
import os, sys, snowflake.connector as sc
kw = {"account": os.environ["SNOWFLAKE_ACCOUNT"], "user": os.environ["SNOWFLAKE_USER"], "password": os.environ["SNOWFLAKE_PASSWORD"], "login_timeout": 30}
for k, e in (("role", "SNOWFLAKE_ROLE"), ("warehouse", "SNOWFLAKE_WAREHOUSE")):
    if os.environ.get(e): kw[k] = os.environ[e]
cur = sc.connect(**kw).cursor()
for st in [s.strip() for s in open(sys.argv[1]).read().split(";\n") if s.strip() and not s.strip().startswith("--")]:
    try:
        cur.execute(st); rows = cur.fetchmany(50)
        print("OK ", st.replace("\n", " ")[:110]); [print("    ", r) for r in rows]
    except Exception as e:
        print("ERR", st.replace("\n", " ")[:110], "->", str(e).replace("\n", " ")[:300])
