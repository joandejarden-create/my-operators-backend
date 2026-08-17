import fs from "fs";
const t = fs.readFileSync("tmp-verify-problem-live.html", "utf8");
const m = [...t.matchAll(/freeform-head[^"']+/g)].map((x) => x[0]);
console.log("freeform-head matches", m);
const i = t.indexOf('id="about"');
console.log(t.slice(i, i + 900));
console.log("---site footer scripts---");
for (const needle of [
  "old-home-problem-v2",
  "site-footer-auth",
  "memberstack.js",
  "old-home-footer-oh",
]) {
  console.log(needle, t.includes(needle));
}
