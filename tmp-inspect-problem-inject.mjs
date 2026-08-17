const t = await (await fetch("https://www.dealality.com/old-home?v=" + Date.now())).text();
const idx = t.indexOf("old-home-problem-v2");
console.log("idx", idx);
console.log(t.slice(Math.max(0, idx - 200), idx + 250));
const idx2 = t.indexOf("freeform-head");
console.log("---freeform---");
console.log(t.slice(Math.max(0, idx2 - 200), idx2 + 250));
const scripts = [...t.matchAll(/<script[^>]+src=["']([^"']+)["'][^>]*>/gi)].map((m) => m[1]);
scripts.filter((s) => /problem|boot|footer|freeform|w19|w21/i.test(s)).forEach((s) => console.log("SCRIPT", s));
