import fs from "fs";

const raw = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/agent-tools/085fb835-9e42-4058-ac4c-0e68db1ad073.txt",
  "utf8"
);
const j = JSON.parse(raw);
console.log("keys", Object.keys(j));
const first = j.results?.[0] || j;
console.log("first keys", Object.keys(first));
const result = first.result || first;
console.log("result keys", Object.keys(result || {}));
const assets = result.assets || [];
console.log("assets", assets.length);
const hits = assets.filter((a) =>
  /joan|founder|dejarden|avatar|portrait|headshot|team/i.test(
    `${a.displayName || ""} ${a.originalFileName || ""} ${a.hostedUrl || ""}`
  )
);
console.log("hits", hits.length);
for (const a of hits) console.log(a.id, a.displayName, a.hostedUrl);
if (!hits.length) {
  for (const a of assets.slice(0, 40)) console.log("-", a.displayName, a.hostedUrl?.slice(-60));
}

const urls = [
  "https://my-operators-backend-production.up.railway.app/marketing/assets/founder-joan-dejarden.png",
  "https://www.dealality.com/",
];
for (const u of urls) {
  const r = await fetch(u, { method: u.endsWith(".png") ? "HEAD" : "GET" });
  console.log("fetch", u, r.status, r.headers.get("content-type"));
  if (!u.endsWith(".png") && r.ok) {
    const t = await r.text();
    const imgs = [...t.matchAll(/https:\/\/cdn\.prod\.website-files\.com\/[^"'\\\s>]+/g)].map((m) => m[0]);
    const interesting = [...new Set(imgs)].filter((x) =>
      /joan|founder|dejarden|avatar|headshot|portrait|team|people/i.test(x)
    );
    console.log("interesting imgs", interesting);
  }
}

// also search more asset pages
console.log("pagination", result.pagination);
