import fs from "fs";

const body = fs.readFileSync("tmp-premium-body.html", "utf8");
const head = fs.readFileSync("tmp-premium-head.html", "utf8");
const foot = fs.readFileSync("tmp-premium-foot.html", "utf8");

// Split into chunks that fit WHTML limits (~5 actions max per call)
const cuts = [
  { name: "nav", start: body.indexOf("<nav id=\"nav\""), end: body.indexOf("<section id=\"hero\"") },
  { name: "hero", start: body.indexOf("<section id=\"hero\""), end: body.indexOf("<section id=\"problem\"") },
  { name: "problem", start: body.indexOf("<section id=\"problem\""), end: body.indexOf("<section id=\"how-it-works\"") },
  { name: "how", start: body.indexOf("<section id=\"how-it-works\""), end: body.indexOf("<section id=\"product-proof\"") },
  { name: "product", start: body.indexOf("<section id=\"product-proof\""), end: body.indexOf("<section id=\"trust\"") },
  { name: "trust", start: body.indexOf("<section id=\"trust\""), end: body.indexOf("<section id=\"cta\"") },
  { name: "cta-footer", start: body.indexOf("<section id=\"cta\""), end: body.lastIndexOf("</div>") },
];

const chunks = {};
for (const c of cuts) {
  if (c.start < 0 || c.end < 0) throw new Error(`missing cut ${c.name}`);
  chunks[c.name] = body.slice(c.start, c.end);
  console.log(c.name, chunks[c.name].length);
}

const wrapperOpen = '<div id="dc-premium">';
const wrapperClose = "</div>";

fs.writeFileSync(
  "tmp-premium-chunks.json",
  JSON.stringify({
    head,
    foot,
    chunks: {
      a: wrapperOpen + chunks.nav + chunks.hero,
      b: chunks.problem + chunks.how,
      c: chunks.product,
      d: chunks.trust + chunks["cta-footer"] + wrapperClose,
    },
  })
);

const j = JSON.parse(fs.readFileSync("tmp-premium-chunks.json", "utf8"));
for (const [k, v] of Object.entries(j.chunks)) console.log("chunk", k, v.length);
console.log("head", head.length, "foot", foot.length);
