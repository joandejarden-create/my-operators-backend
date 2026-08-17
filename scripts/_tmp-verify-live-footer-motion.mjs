/**
 * Post-publish QA: motion path unlock + footer align-items.
 */
const html = await fetch("https://www.dealality.com/?" + Date.now(), {
  headers: { "cache-control": "no-cache" },
}).then((r) => r.text());

const motion = [
  ...html.matchAll(/old-home-motion\.prod\.v20260801[a-z]\.js/g),
].map((m) => m[0]);
const freeformHead = [
  ...html.matchAll(/freeform-head\.v20260801[a-z]\.css/g),
].map((m) => m[0]);
const boot = [...html.matchAll(/boot-guard\.v20260801[a-z]\.js/g)].map(
  (m) => m[0]
);
const hasFooterCss = html.includes("oh-footer-top-align");

const cssUrl = freeformHead[0]
  ? `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/${
      [...html.matchAll(/[0-9a-f]{24}_dealality-old-home-freeform-head\.v20260801f\.css/g)][0]?.[0] ||
      ""
    }`
  : null;

let alignSnippet = null;
const fhMatch = html.match(
  /https:\/\/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[0-9a-f]+_dealality-old-home-freeform-head\.v20260801f\.css/
);
if (fhMatch) {
  const css = await fetch(fhMatch[0]).then((r) => r.text());
  const i = css.indexOf("Footer columns right");
  alignSnippet = css.slice(i, i + 280);
}

const motionUrl = html.match(
  /https:\/\/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[0-9a-f]+_old-home-motion\.prod\.v20260801g\.js/
)?.[0];
let pathUnlock = null;
if (motionUrl) {
  const js = await fetch(motionUrl).then((r) => r.text());
  pathUnlock = /path === \"\/\"/.test(js);
}

console.log(
  JSON.stringify(
    {
      motion,
      boot,
      freeformHead,
      hasFooterCss,
      pathUnlock,
      alignSnippet,
    },
    null,
    2
  )
);
