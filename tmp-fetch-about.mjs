import fs from "fs";

const html = await (await fetch("https://www.dealality.com/old-home")).text();
const i = html.indexOf('id="about"');
fs.writeFileSync("tmp-live-about-snip.html", html.slice(Math.max(0, i - 50), i + 4500));
console.log("about index", i);

const live = await (
  await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69fe92f7d4f50fb6112eb9_old-home-footer-oh-20260729b.js"
  )
).text();
fs.writeFileSync("tmp-live-footer.js", live);

const local = fs.readFileSync("public/marketing/old-home-footer-oh.v20260729d.js", "utf8");
console.log("local len", local.length, "live len", live.length);
console.log("local opportunity", /opportunity|ohOpen/.test(local));
console.log("live opportunity", /opportunity|ohOpen/.test(live));

// Diff rough: find unique snippets
const localOnly = [];
for (const needle of ["ohOpenOpportunityReview", "about-visual", "modules-dot", "cta-primary", "#6C72FF"]) {
  console.log(needle, "local", local.includes(needle), "live", live.includes(needle));
}
