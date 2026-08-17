import fs from "fs";
const page = fs.readFileSync("tmp/old-home-live.html", "utf8");
const scripts = [...page.matchAll(/src="(https:[^"]+)"/g)].map((m) => m[1]);
console.log("scripts", scripts.length);
const hits = [];
for (const u of scripts) {
  try {
    const t = await (await fetch(u, { cache: "no-store" })).text();
    if (/disciplined way|deserve closer attention|I built Dealality so owners can centralize/i.test(t)) {
      hits.push(u);
      console.log("HIT", u);
    }
  } catch (e) {
    console.log("fail", u, String(e).slice(0, 80));
  }
}
console.log("hits", hits);
// inline scripts?
const inline = [...page.matchAll(/<script(?![^>]*src=)[^>]*>([\s\S]*?)<\/script>/gi)].map((m) => m[1]);
for (const [i, t] of inline.entries()) {
  if (/disciplined way|deserve closer/i.test(t)) console.log("inline hit", i, t.slice(0, 200));
}
