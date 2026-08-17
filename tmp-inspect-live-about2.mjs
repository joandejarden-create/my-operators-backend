import fs from "fs";
const t = fs.readFileSync("tmp-verify-problem-live.html", "utf8");
const links = [...t.matchAll(/href="(https:\/\/cdn\.prod\.website-files\.com[^"]+)"/g)].map((m) => m[1]);
console.log(
  "css links with freeform/head/old-home",
  links.filter((u) => /freeform-head|old-home-freeform-head|old-home-dark|problem/i.test(u))
);
const scripts = [...t.matchAll(/src="(https:\/\/cdn\.prod\.website-files\.com[^"]+)"/g)].map((m) => m[1]);
console.log(
  "js relevant",
  scripts.filter((u) => /problem|footer-oh|site-footer-auth|freeform-head/i.test(u))
);
const i = t.indexOf('id="about"');
console.log("ABOUT SNIP:\n", t.slice(i, i + 1200));
