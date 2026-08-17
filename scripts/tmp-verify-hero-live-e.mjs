const urls = [
  "https://www.dealality.com/old-home",
  "https://dealality.com/old-home",
];

async function check(url) {
  const html = await (await fetch(url, { redirect: "follow", cache: "no-store" })).text();
  return {
    url,
    has29e: /hero-fit-boot\.v20260729e/i.test(html) || /ohherofitboot29e/i.test(html),
    has29d: /hero-fit-boot\.v20260729d/i.test(html),
    hasPin307:
      /ohglobepindim307/i.test(html) ||
      /globe-bg\.v202607307/i.test(html) ||
      /globe-pin-dim\.v202607307/i.test(html),
    boot: (html.match(/old-home-hero-fit-boot[^"'\s]*/i) || [])[0] || null,
    globe: (html.match(/globe[^"'\s]*v20260730[^"'\s]*/i) || [])[0] || null,
  };
}

for (const url of urls) console.log(await check(url));

const cssUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a7598332f85a5833260a6_dealality-old-home-hero-fit.v20260729e.css";
const css = await (await fetch(cssUrl)).text();
console.log({
  cssOk: /min-width:1200px/.test(css) && /min-width:1440px/.test(css),
  breakpoints: [...css.matchAll(/@media\(([^)]+)\)/g)].map((m) => m[1]),
  ctaWide: /margin-top:clamp\(1\.65rem/.test(css),
  ctaUltra: /margin-top:clamp\(2\.1rem/.test(css),
});

const pinBoot =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a5d99a90e0c5e2961aabb_old-home-globe-pin-dim.v202607307.js";
const pin = await (await fetch(pinBoot)).text();
console.log({
  pinBootOk: /v202607307/.test(pin),
  dimYucatan: /Mérida|Cancún|Cozumel/.test(pin) || /Merida|Cancun|Cozumel/.test(pin),
});
