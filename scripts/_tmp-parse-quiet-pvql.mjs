import fs from "fs";
const p =
  "C:/Users/joand/.cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/terminals/934814.txt";
const lines = fs.readFileSync(p, "utf8").split(/\n/);
let last = null;
const out = [];
for (const raw of lines) {
  const line = raw.replace(/\r$/, "");
  const m = line.match(/^PVQL ([a-z0-9-]+)\.\.\./);
  if (m) last = m[1];
  const r = line.match(/^(PASS|FAIL|ERR) (.+)/);
  if (r) out.push({ slug: last, status: r[1], detail: r[2].trim() });
}
console.log("n", out.length);
console.log(JSON.stringify(out.filter((x) => x.status !== "PASS"), null, 2));
console.log("pass", out.filter((x) => x.status === "PASS").length);
console.log("last3", out.slice(-3));
