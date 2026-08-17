import fs from "fs";
const c = fs.readFileSync("c:/Dev/deal-capture-proxy/tmp/many-futures.css", "utf8");
const needles = ["9b8afb", "155,138", "lavender", "c4b5fd", "mf-panel", "mf-accent-dim", "rgba(108,114,255"];
for (const n of needles) {
  let i = 0;
  let count = 0;
  while ((i = c.indexOf(n, i)) !== -1 && count < 3) {
    console.log("\n==", n, "@", i, "==");
    console.log(c.slice(Math.max(0, i - 80), i + 120));
    i += n.length;
    count++;
  }
}
// find tile-like class backgrounds
const re = /#[^#{]*\{[^}]{0,200}background:[^;]{0,120};[^}]{0,80}\}/g;
const all = c.match(re) || [];
console.log("\nbg rules sample", all.filter(x => /panel|tile|card|chip|path|capab|rail/i.test(x)).slice(0, 15).join("\n"));
