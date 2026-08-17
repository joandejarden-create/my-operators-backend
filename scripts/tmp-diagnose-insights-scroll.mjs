const r = await fetch("https://www.dealality.com/old-home?nocache=" + Date.now());
const t = await r.text();

const checks = {
  hasPrev: t.includes('id="insights-prev"'),
  hasNext: t.includes('id="insights-next"'),
  hasGrid: t.includes('id="insights-grid"'),
  hasScrollInsights: t.includes("scrollInsights"),
  hasCardStep: t.includes("cardStep"),
  hasSyncNav: t.includes("syncInsightsNav"),
  hasInsightsPrevListener: t.includes('insights-prev') && t.includes("scrollInsights"),
  readerHasCta: t.includes('cta-band-btn'),
  scriptCount: (t.match(/<script>/g) || []).length,
};

console.log(checks);

// Extract insights carousel script chunk
const idx = t.indexOf("insights-grid");
const footerScripts = [];
let i = 0;
while (true) {
  const s = t.indexOf("<script>", i);
  if (s < 0) break;
  const e = t.indexOf("</script>", s);
  if (e < 0) break;
  const body = t.slice(s, e + 9);
  if (body.includes("insights-prev") || body.includes("scrollInsights") || body.includes("oh-article-reader") || body.includes("cta-band-btn")) {
    footerScripts.push({
      len: body.length,
      head: body.slice(0, 120).replace(/\n/g, " "),
      hasPrev: body.includes("insights-prev"),
      hasScroll: body.includes("scrollInsights"),
      hasReader: body.includes("oh-article-reader"),
      hasCta: body.includes("cta-band-btn"),
      syntaxSuspect: /<\/script>\s*<script>/.test(body) === false && body.includes("})();</script>"),
    });
  }
  i = e + 9;
}
console.log("relevant scripts", footerScripts);

// Check if buttons are disabled in HTML
const prevSnippet = t.match(/id="insights-prev"[^>]*>/);
const nextSnippet = t.match(/id="insights-next"[^>]*>/);
console.log("prev tag", prevSnippet && prevSnippet[0]);
console.log("next tag", nextSnippet && nextSnippet[0]);

// Look for JS errors indicators - truncated scripts
const carouselStart = t.indexOf("var track=document.getElementById(\"insights-grid\")");
console.log("carousel script idx", carouselStart);
if (carouselStart >= 0) {
  console.log(t.slice(carouselStart, carouselStart + 800));
}
