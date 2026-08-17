import sqlite3
import os
import json
import base64
import urllib.request

db = os.path.join(os.environ["APPDATA"], "Cursor", "User", "globalStorage", "state.vscdb")
con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
cur = con.cursor()

# Pull anysphere.cursor-mcp blob and oauth-related values for user-webflow
cur.execute("SELECT key, value FROM ItemTable WHERE key = 'anysphere.cursor-mcp'")
row = cur.fetchone()
if row:
    raw = row[1]
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    print("anysphere.cursor-mcp len", len(raw))
    try:
        data = json.loads(raw)
        print("top keys", list(data.keys())[:30])
        out = r"C:\Dev\deal-capture-proxy\tmp\cursor-mcp-state.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print("wrote", out)
    except Exception as e:
        print("parse fail", e)
        print(raw[:500])

cur.execute(
    "SELECT key, value FROM ItemTable WHERE key LIKE 'mcpOAuth%' AND (lower(key) LIKE '%webflow%' OR lower(key) LIKE '%user-webflow%' OR key LIKE '%d2ViZmxv%')"
)
rows = cur.fetchall()
print("webflow oauth rows", len(rows))
for k, v in rows:
    if isinstance(v, bytes):
        v = v.decode("utf-8", errors="replace")
    print("KEY", k)
    print("VAL", (v[:300] + "...") if len(v) > 300 else v)
    print("---")

# Also search cursorDiskKV for mcp tokens
cur.execute(
    "SELECT key, value FROM cursorDiskKV WHERE lower(key) LIKE '%mcpoauth%' OR lower(key) LIKE '%user-webflow%' LIMIT 30"
)
rows2 = cur.fetchall()
print("diskKV matches", len(rows2))
for k, v in rows2:
    if isinstance(v, bytes):
        v = v.decode("utf-8", errors="replace")
    print("DKV", k, (v[:200] + "...") if len(str(v)) > 200 else v)
