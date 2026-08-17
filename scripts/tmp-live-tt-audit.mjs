import fs from "fs";

const url = "https://www.dealality.com/old-home?nocache=" + Date.now();
const html = await (await fetch(url, { headers: { "cache-control": "no-cache" } })).text();

const cssUrls = [
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a629414ad57d94a7f3c87_dealality-old-home-freeform-head.v20260729w21.css",
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a5d73722c376813c9a44d_dealality-old-home-quote-tiles.v20260729a.css",
];

const cssBodies = {};
for (const u of cssUrls) {
  const r = await fetch(u);
  cssBodies[u] = { ok: r.ok, status: r.status, len: (await r.text()).length };
}

const ohTt = html.match(/<style id=["']oh-tt["'][\s\S]*?<\/style>/i)?.[0] || "";
const trustIdx = html.search(/id=["']trust["']/i);
const testimonialsIdx = html.search(/id=["']testimonials["']/i);
const snippetStart = Math.max(0, (testimonialsIdx > -1 ? testimonialsIdx : trustIdx) - 100);
const snippet = html.slice(snippetStart, snippetStart + 5500);

const arts = [...snippet.matchAll(/<article[\s\S]*?<\/article>/gi)].map((m) => {
  const a = m[0];
  return {
    hasImg: /<img\b/i.test(a),
    imgSrc: (a.match(/src=["']([^"']+)["']/i) || [])[1] || null,
    quote: (a.match(/<blockquote[^>]*>([\s\S]*?)<\/blockquote>/i) || [])[1]?.replace(/<[^>]+>/g, "").trim().slice(0, 120) || null,
    attr: (a.match(/<p[^>]*>([\s\S]*?)<\/p>/i) || [])[1]?.replace(/<[^>]+>/g, "").trim().slice(0, 80) || null,
  };
});

const scripts = [...html.matchAll(/src=["']([^"']*(?:quote-tiles|problem-v2|asset-boot|fouc-gate|freeform-head|old-home)[^"']*)["']/gi)].map(
  (m) => m[1]
);

const out = {
  url,
  ohTtPresent: !!ohTt,
  ohTtHasAutoHeight: /height:auto!important/i.test(ohTt),
  ohTtHas2Col: /grid-template-columns:repeat\(2/i.test(ohTt),
  ohTtHasShow2nd: /nth-child\(n\+2\)\{display:flex/i.test(ohTt),
  quoteTilesInHtml: /quote-tiles/i.test(html),
  problemV2InHtml: /problem-v2/i.test(html),
  w21InHtml: /v20260729w21\.css/i.test(html),
  assetBootInHtml: /old-home-asset-boot|OldHomeAssetBoot|oldhomeassetboot/i.test(html),
  foucGateInHtml: /fouc-gate|ohfoucgate/i.test(html),
  cssBodies,
  scripts: [...new Set(scripts)].slice(0, 40),
  arts,
};
fs.writeFileSync("tmp-live-tt-audit.json", JSON.stringify(out, null, 2));
console.log(JSON.stringify(out, null, 2));
