import fs from "fs";
const page = await (
  await fetch("https://mvp-deal-capture.webflow.io/old-home", { cache: "no-store" })
).text();
fs.writeFileSync("tmp/old-home-live.html", page);
const i = page.indexOf("disciplined way");
console.log({ inHtml: i, snip: i >= 0 ? page.slice(i - 40, i + 120) : null });
const scripts = [...page.matchAll(/src="(https:[^"]+)"/g)].map((m) => m[1]);
fs.writeFileSync("tmp/old-home-scripts.json", JSON.stringify(scripts, null, 2));
for (const u of scripts) {
  if (!/testimonial|quote|trust|founder|oh-/i.test(u) && !u.includes("website-files")) continue;
  try {
    const t = await (await fetch(u)).text();
    if (/disciplined way|deserve closer attention/i.test(t)) {
      console.log("HIT", u);
    }
  } catch {}
}
// also scan all website-files scripts
for (const u of scripts.filter((s) => s.includes("website-files"))) {
  try {
    const t = await (await fetch(u)).text();
    if (/disciplined way/i.test(t)) console.log("HIT2", u);
  } catch {}
}
