const url =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a6c050ab86c971afcef84_old-home-hero-fit-boot.v20260729d.js";
const t = await (await fetch(url)).text();
const m = t.match(/dealality-old-home-hero-fit[^"']+/);
console.log({
  has29d: /v20260729d\.css/.test(t),
  cssRef: m && m[0],
  usesNewCss: t.includes("6a6a68e1fedb6e8369ce6830"),
  usesOldCss: t.includes("6a6a676f7f36da5ad78beca1"),
});
