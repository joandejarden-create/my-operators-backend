const urls = [
  "https://www.dealality.com/old-home?cb=" + Date.now(),
  "https://dealality.com/old-home?cb=" + Date.now(),
];

for (const url of urls) {
  const res = await fetch(url, { headers: { "cache-control": "no-cache" } });
  const html = await res.text();
  const hasNew = html.includes("nearly 30 years");
  const hasOld = html.includes("working both sides");
  const hasTt30a = html.includes("testimonials.v20260730a");
  const hasBoot30a = html.includes("old-home-boot-guard.v20260730a") || html.includes("oldhomebootguard30a");
  const hasW4 = html.includes("testimonials.v20260729w4");
  console.log(JSON.stringify({ url, status: res.status, hasNew, hasOld, hasTt30a, hasBoot30a, hasW4 }, null, 2));
}

const cdn =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a8739c83f9c69c9343dfe_dealality-old-home-testimonials.v20260730a.js";
const js = await (await fetch(cdn)).text();
console.log({
  cdnOk: js.includes("startAutoplayOnce") && js.includes("allSlidesSeen"),
  len: js.length,
});
