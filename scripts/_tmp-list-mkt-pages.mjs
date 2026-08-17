import json
raw = open(
    r"C:\Users\joand\.cursor\projects\c-Dev-deal-capture-proxy\agent-tools\17e77217-a90f-47f0-b8e6-4ce71068f632.txt",
    encoding="utf-8",
).read()
data = json.loads(raw)
pages = data["result"]["pages"]
want = [
    "signup",
    "login",
    "log-in",
    "insights",
    "who-its-for",
    "privacy",
    "terms",
    "opportunity",
    "join",
    "home",
]
for p in pages:
    slug = p.get("slug") or ""
    title = p.get("title") or ""
    path = p.get("publishedPath") or ("/" + slug if slug else "/")
    blob = f"{slug} {title} {path}".lower()
    if any(w in blob for w in want):
        print(f"{p['id']}\t{title}\t{path}\tslug={slug}")
