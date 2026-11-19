# usage: sf.py <sql file>   runs each statement (separated by ";\n"), prints rows. Credentials from env.
import os, sys, snowflake.connector as sc
c = sc.connect(account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"], password=os.environ["SNOWFLAKE_PASSWORD"],
               role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"), warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"), login_timeout=30)
cur = c.cursor()
for st in [s.strip() for s in open(sys.argv[1]).read().split(";\n") if s.strip() and not s.strip().startswith("--")]:
    try:
        cur.execute(st); rows = cur.fetchmany(50)
        print("OK ", st.replace("\n", " ")[:110]); [print("    ", r) for r in rows]
    except Exception as e:
        print("ERR", st.replace("\n", " ")[:110], "->", str(e).replace("\n", " ")[:300])
