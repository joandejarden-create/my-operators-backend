import fs from "fs";

const url = process.argv[2] || "https://www.dealality.com/old-home?cb=" + Date.now();
const res = await fetch(url, { headers: { "cache-control": "no-cache" } });
const html = await res.text();
fs.writeFileSync("tmp-old-home-tt-verify.html", html);

const ohTt = html.match(/<style id=["']oh-tt["'][\s\S]*?<\/style>/i)?.[0] || "";
const trustIdx = html.indexOf('id="trust"');
const trust = trustIdx >= 0 ? html.slice(trustIdx, trustIdx + 4000) : "";

console.log(
  JSON.stringify(
    {
      status: res.status,
      hasOhTtFixed: /height:320px!important/.test(ohTt),
      hasLineClamp: /-webkit-line-clamp:6/.test(ohTt),
      ohTtHeightAuto: /height:auto!important/.test(ohTt),
      hasTt30b: /testimonials\.v20260730b/.test(html),
      hasBoot30d: /boot-guard\.v20260730d/.test(html),
      hasBoot30b: /boot-guard\.v20260730b/.test(html),
      hasHowWeDoIt: /how-we-do-it\.v20260730d/.test(html),
      hasW16: /freeform-head\.v20260729w16/.test(html),
      joanShort: /Owners had options, but no clear way to compare them/.test(html),
      brand: /Owners arrive with clearer criteria/.test(html),
      operator: /Permission-based deal rooms/.test(html),
      natalieBrand: /Brand Development/.test(html),
      elena: /Elena Vargas/.test(html),
      sarah: /Sarah Mitchell/.test(trust),
      oldLongJoan: /After nearly 30 years in hospitality/.test(html),
      boots: [...html.matchAll(/old-home-boot-guard[^"'<\s]+/g)].map((m) => m[0]),
      tts: [...html.matchAll(/dealality-old-home-testimonials[^"'<\s]+/g)].map((m) => m[0]),
    },
    null,
    2
  )
);
