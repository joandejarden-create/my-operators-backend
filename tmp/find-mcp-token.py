import sqlite3
import os
import json

db = os.path.join(os.environ["APPDATA"], "Cursor", "User", "globalStorage", "state.vscdb")
print("db", db, "exists", os.path.exists(db), "size", os.path.getsize(db))
con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
cur = con.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]
print("tables", tables)

for table in tables:
    try:
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        print("cols", table, cols)
    except Exception as e:
        print("pragma fail", table, e)

# Search ItemTable-like key/value stores for webflow/mcp tokens
for table in tables:
    try:
        cur.execute(f"SELECT * FROM {table} LIMIT 1")
        row = cur.fetchone()
        if not row:
            continue
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        if "key" in cols and "value" in cols:
            cur.execute(
                f"SELECT key FROM {table} WHERE lower(key) LIKE '%webflow%' OR lower(key) LIKE '%mcp%' OR lower(key) LIKE '%oauth%' LIMIT 50"
            )
            keys = [r[0] for r in cur.fetchall()]
            print("matching keys in", table, len(keys))
            for k in keys[:40]:
                print(" KEY", k)
    except Exception as e:
        print("scan fail", table, e)
