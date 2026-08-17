/**
 * Compare top-nav content width across marketing pages.
 */
const pages = [
  "/",
  "/signup",
  "/who-its-for",
  "/insights",
  "/privacy",
  "/terms",
  "/opportunity-review",
  "/old-home",
];

const results = [];
for (const path of pages) {
  const url = "https://www.dealality.com" + path;
  let html = "";
  try {
    html = await fetch(url, { headers: { "cache-control": "no-cache" } }).then(
      (r) => r.text()
    );
  } catch (e) {
    results.push({ path, error: String(e) });
    continue;
  }
  const hasOhNav = /id=["']nav["']|class=["'][^"']*oh-nav/.test(html);
  const hasWNav = /w-nav|navbar-2|class=["'][^"']*navbar/.test(html);
  const cssHits = [
    ...html.matchAll(/dealality-old-home[^\s"'<>]*\.(?:css|js)/g),
  ].map((m) => m[0]);
  const freeformNav = html.includes("1120px") && /#nav|\.oh-nav/.test(html);
  const maxWidthMentions = [
    ...html.matchAll(/max-width:\s*(\d+)px/gi),
  ]
    .map((m) => m[1])
    .filter((n) => ["1100", "1120", "1200", "1280", "1320", "1400"].includes(n));
  results.push({
    path,
    statusOk: html.length > 1000,
    hasOhNav,
    hasWNav,
    freeformHead: cssHits.filter((x) => x.includes("freeform-head")).slice(0, 3),
    navCleanup: cssHits.some((x) => x.includes("nav-cleanup")),
    bootGuard: cssHits.some((x) => x.includes("boot-guard")),
    sampleMaxWidths: [...new Set(maxWidthMentions)].slice(0, 8),
  });
}
console.log(JSON.stringify(results, null, 2));
