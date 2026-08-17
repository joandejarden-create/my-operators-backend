import fs from "fs";
const t = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-tools/72133154-cbcb-4197-9884-c23c0ce52ae6.txt",
  "utf8"
);
const data = JSON.parse(t);
const pages = data?.result?.pages || data?.pages || [];
const hits = pages.filter((p) => /old-home/i.test(p.slug || "") || /old-home/i.test(p.title || ""));
console.log(JSON.stringify(hits.map((p) => ({ id: p.id, slug: p.slug, title: p.title })), null, 2));
