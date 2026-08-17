import fs from "fs";

const pagesRaw = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/agent-tools/f8157ffc-17c5-472a-bc9e-7689de06c7da.txt",
  "utf8"
);
const j = JSON.parse(pagesRaw);
const pages =
  (j.result && j.result.pages) ||
  (Array.isArray(j) && j[0]?.result?.pages) ||
  j.pages ||
  [];
const hits = pages.filter((p) => /old-home|Old Home/i.test(`${p.title || ""} ${p.slug || ""} ${p.publishedPath || ""}`));
fs.writeFileSync(
  "tmp-old-home-page-hits.json",
  JSON.stringify(
    {
      total: pages.length,
      hits: hits.map((p) => ({ id: p.id, title: p.title, slug: p.slug, publishedPath: p.publishedPath })),
      sample: pages.slice(0, 15).map((p) => ({ id: p.id, title: p.title, slug: p.slug })),
    },
    null,
    2
  )
);
console.log("wrote tmp-old-home-page-hits.json", hits.length, "hits");
