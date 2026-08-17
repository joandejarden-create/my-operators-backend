import fs from "node:fs";
import {
  WYNDHAM_SITEMAP_INDEX,
  WYNDHAM_FETCH_HEADERS,
  extractSitemapLocs,
  parseWyndhamPropertyUrl,
} from "../lib/wyndham-brand-directory-extract.js";

const idx = await (await fetch(WYNDHAM_SITEMAP_INDEX, { headers: WYNDHAM_FETCH_HEADERS })).text();
const children = extractSitemapLocs(idx).filter((u) => /properties/i.test(u));
const samples = [];
const calaRe = /mexico|colombia|panama|costa-rica|dominican|jamaica|brazil|peru|chile|argentina/i;

for (const u of children) {
  const name = u.split("/").pop();
  const xml = await (await fetch(u, { headers: WYNDHAM_FETCH_HEADERS })).text();
  const overs = extractSitemapLocs(xml).filter((x) => /\/overview\/?$/i.test(x));
  if (!overs.length) continue;
  const calaish = overs.filter((x) => calaRe.test(x));
  samples.push({
    sitemap: name,
    overview_count: overs.length,
    calaish_path_count: calaish.length,
    sample: calaish[0] || overs[0],
    parsed: parseWyndhamPropertyUrl(calaish[0] || overs[0]),
  });
  if (samples.length >= 12) break;
}

const p = "reports/research-engine-v2/webhound-active-brand-coverage-gap-code-probe.json";
const j = JSON.parse(fs.readFileSync(p, "utf8"));
j.probes.wyndham.property_sitemaps_count = children.length;
j.probes.wyndham.brand_sitemap_samples = samples;
j.probes.wyndham.note =
  "Some *properties_*.xml are empty (e.g. kg); others have /overview URLs. CALA often needs page JSON-LD country, not path alone.";
fs.writeFileSync(p, JSON.stringify(j, null, 2));
console.log(JSON.stringify({ children: children.length, samples }, null, 2));
