const fs = require("fs");
const path = require("path");
const file = path.join(
  process.env.USERPROFILE,
  ".cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/agent-tools/f8157ffc-17c5-472a-bc9e-7689de06c7da.txt"
);
const raw = fs.readFileSync(file, "utf8");
const j = JSON.parse(raw);
const pages =
  (j.result && j.result.pages) ||
  (Array.isArray(j) && j[0] && j[0].result && j[0].result.pages) ||
  j.pages ||
  [];
console.log("total", pages.length);
const hits = pages.filter((p) => /old-home|Old Home|old home/i.test(JSON.stringify(p)));
console.log(
  JSON.stringify(
    hits.map((p) => ({ id: p.id, title: p.title, slug: p.slug, publishedPath: p.publishedPath })),
    null,
    2
  )
);
if (!hits.length) {
  console.log(
    JSON.stringify(
      pages.slice(0, 20).map((p) => ({ id: p.id, title: p.title, slug: p.slug })),
      null,
      2
    )
  );
}
