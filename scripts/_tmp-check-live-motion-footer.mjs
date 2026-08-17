/**
 * Inspect live Home for motion + footer CSS refs.
 */
const html = await fetch("https://www.dealality.com/", {
  headers: { "cache-control": "no-cache" },
}).then((r) => r.text());

const motion = [...html.matchAll(/old-home-motion[^"'?\s]+/g)].map((m) => m[0]);
const freeformHead = [...html.matchAll(/dealality-old-home-freeform-head[^"'?\s]+/g)].map(
  (m) => m[0]
);
const alignHits = [];
for (const href of freeformHead) {
  const url = href.startsWith("http")
    ? href
    : `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/${href}`;
  // href may already be full CDN path fragment from match
}
const cssUrls = [
  ...html.matchAll(
    /https:\/\/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[^"'>\s]*freeform-head[^"'>\s]*/g
  ),
].map((m) => m[0]);
const motionUrls = [
  ...html.matchAll(
    /https:\/\/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[^"'>\s]*old-home-motion[^"'>\s]*/g
  ),
].map((m) => m[0]);

console.log(JSON.stringify({ motion, freeformHead, cssUrls, motionUrls }, null, 2));

for (const url of cssUrls.slice(0, 2)) {
  const css = await fetch(url).then((r) => r.text());
  const i = css.indexOf("align-items:end");
  const j = css.indexOf("align-items:start");
  console.log(
    JSON.stringify({
      url,
      alignItemsEndAt: i,
      snippet: i >= 0 ? css.slice(Math.max(0, i - 80), i + 40) : null,
      hasStartNearFooter: css.includes("align-items:start!important"),
    })
  );
}
