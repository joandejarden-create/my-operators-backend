const url =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a87a25c017d7c6eee68ed_old-home-boot-guard.v20260730a.js";
const t = await (await fetch(url)).text();
console.log({
  has30a: t.includes("testimonials.v20260730a"),
  hasW4: t.includes("testimonials.v20260729w4"),
  pathGate: t.includes("/old-home"),
});
