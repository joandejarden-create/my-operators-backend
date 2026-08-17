import fs from "fs";

const j = JSON.parse(fs.readFileSync("tmp-premium-chunks.json", "utf8"));

function stripOuter(s) {
  return s.replace(/^<div id="dc-premium">/, "").replace(/<\/div>$/, "");
}

const parts = {
  a: stripOuter(j.chunks.a),
  b: j.chunks.b,
  c: j.chunks.c,
  d: stripOuter(j.chunks.d),
};

for (const [k, v] of Object.entries(parts)) {
  fs.writeFileSync(`tmp-chunk-${k}-inner.html`, v);
  console.log(k, v.length, v.slice(0, 40));
}
