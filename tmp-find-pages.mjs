import fs from "fs";
const raw = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-tools/04b710cc-bf83-4c90-83ae-87dc1723f1c8.txt",
  "utf8"
);
const j = JSON.parse(raw);
const pages =
  j?.[0]?.result?.pages ||
  j?.result?.pages ||
  j?.pages ||
  (Array.isArray(j) ? j : []);
const arr = Array.isArray(pages) ? pages : [];
for (const p of arr) {
  const slug = p.slug ?? "";
  const title = p.title ?? "";
  if (/home|old/i.test(slug + title) || slug === "" || slug === "old-home") {
    console.log(JSON.stringify({ id: p.id, title, slug }));
  }
}
console.log("total", arr.length);
