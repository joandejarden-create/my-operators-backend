import fs from "fs";

const raw = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/agent-tools/dfa72e47-9998-4976-9d8b-4e87647b3b09.txt",
  "utf8"
);
const j = JSON.parse(raw);
const pages = j.result.pages;
const hits = pages.filter((p) => /old-home|home|landing/i.test(p.slug + " " + p.title));
console.log(hits.map((p) => ({ id: p.id, slug: p.slug, title: p.title })));
const oh = pages.find((p) => p.slug === "old-home");
console.log("old-home", oh);
