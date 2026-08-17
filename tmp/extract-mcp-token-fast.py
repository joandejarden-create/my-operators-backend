import sqlite3
import os
import json

db = os.path.join(os.environ["APPDATA"], "Cursor", "User", "globalStorage", "state.vscdb")
con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
cur = con.cursor()

# Targeted key lookups only
keys = [
    "anysphere.cursor-mcp",
]
cur.execute("SELECT key FROM ItemTable WHERE key LIKE 'mcpOAuth%' AND key LIKE '%d2ViZmxv%'")
keys += [r[0] for r in cur.fetchall()]
cur.execute("SELECT key FROM ItemTable WHERE key LIKE 'mcpOAuth%' AND key LIKE '%user-webflow%'")
keys += [r[0] for r in cur.fetchall()]
# decode known base64 fragments for user-webflow tokens
cur.execute("SELECT key FROM ItemTable WHERE key LIKE 'mcpOAuth.secret%' LIMIT 200")
all_secret = [r[0] for r in cur.fetchall()]
print("secret count", len(all_secret))

# Get anysphere.cursor-mcp
cur.execute("SELECT value FROM ItemTable WHERE key = ?", ("anysphere.cursor-mcp",))
row = cur.fetchone()
if row:
    v = row[0]
    if isinstance(v, memoryview):
        v = v.tobytes()
    if isinstance(v, bytes):
        v = v.decode("utf-8", errors="replace")
    data = json.loads(v)
    with open(r"C:\Dev\deal-capture-proxy\tmp\cursor-mcp-state.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print("wrote cursor-mcp-state.json keys", list(data.keys())[:40])
    # Search recursively for access_token
    def find_tokens(obj, path=""):
        found = []
        if isinstance(obj, dict):
            for k, val in obj.items():
                p = f"{path}.{k}" if path else k
                if k in ("access_token", "accessToken", "token", "refresh_token", "refreshToken") and isinstance(val, str) and len(val) > 20:
                    found.append((p, val[:20] + "...", len(val)))
                else:
                    found.extend(find_tokens(val, p))
        elif isinstance(obj, list):
            for i, val in enumerate(obj[:50]):
                found.extend(find_tokens(val, f"{path}[{i}]"))
        return found
    toks = find_tokens(data)
    print("token fields", len(toks))
    for t in toks[:30]:
        print(t)

# Dump user-webflow related oauth values
for k in keys:
    if k == "anysphere.cursor-mcp":
        continue
    cur.execute("SELECT value FROM ItemTable WHERE key = ?", (k,))
    row = cur.fetchone()
    if not row:
        continue
    v = row[0]
    if isinstance(v, memoryview):
        v = v.tobytes()
    if isinstance(v, bytes):
        v = v.decode("utf-8", errors="replace")
    print("KEY", k)
    print("VAL", v[:500])
    print("---")
