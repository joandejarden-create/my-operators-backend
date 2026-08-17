import fs from "fs";

const raw = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/agent-tools/dfa72e47-9998-4976-9d8b-4e87647b3b09.txt",
  "utf8"
);
const pages = JSON.parse(raw).result.pages;
for (const p of pages) {
  console.log(p.id, p.slug || "(no-slug)", p.title);
}

const h = fs.readFileSync("tmp-old-home-live-features.html", "utf8");
const start = h.indexOf('id="platform-features"');
const end = h.indexOf('id="platform-features-glow"');
console.log("\n=== FEATURES SECTION ===\n");
console.log(h.slice(start - 50, end + 80));
