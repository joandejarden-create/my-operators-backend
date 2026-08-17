const urls = [
  "https://www.dealality.com/old-home",
  "https://dealality.com/old-home",
];
for (const url of urls) {
  const html = await (await fetch(url, { redirect: "follow" })).text();
  const has29d =
    /ohherofitboot29d/i.test(html) || /hero-fit-boot\.v20260729d/i.test(html);
  const has29c = /hero-fit-boot\.v20260729c/i.test(html);
  const hasPin307 =
    /ohglobepindim307/i.test(html) || /globe-bg\.v202607307/i.test(html);
  console.log({
    url,
    has29d,
    has29c,
    hasPin307,
    bootSnippet: (html.match(/hero-fit-boot[^"'\s]*/i) || [])[0] || null,
  });
}

const cssUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a68e1fedb6e8369ce6830_dealality-old-home-hero-fit.v20260729d.css";
const css = await (await fetch(cssUrl)).text();
const bps = [...css.matchAll(/@media\(([^)]+)\)/g)].map((m) => m[1]);
console.log({ cssStatus: "ok", breakpoints: bps });
