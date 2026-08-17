/**
 * Patch gap code probe with corrected Accor continent URLs + Wyndham property sitemaps.
 */
import fs from "node:fs";
import {
  ACCOR_HOTEL_SITEMAP_EN,
  ACCOR_FETCH_HEADERS,
  extractSitemapLocs,
} from "../lib/accor-brand-directory-extract.js";
import {
  ACCOR_CONTINENT_PAGES,
  parseAccorContinentHotelsFromHtml,
} from "../lib/accor-continent-directory-extract.js";
import {
  WYNDHAM_SITEMAP_INDEX,
  WYNDHAM_FETCH_HEADERS,
  extractSitemapLocs as wyndhamLocs,
  parseWyndhamPropertyUrl,
} from "../lib/wyndham-brand-directory-extract.js";

async function fetchText(url, headers = {}) {
  const res = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      ...headers,
    },
    redirect: "follow",
  });
  return { ok: res.ok, status: res.status, text: await res.text(), url: res.url };
}

const sm = await fetchText(ACCOR_HOTEL_SITEMAP_EN, ACCOR_FETCH_HEADERS);
const locs = extractSitemapLocs(sm.text).filter((u) => /\/hotel\//i.test(u));

const prior = JSON.parse(fs.readFileSync("reports/accor-property-directory-extract.json", "utf8"));
const calaPrior = (prior.propertyRows || [])
  .filter((r) =>
    ["Mexico", "Colombia", "Costa Rica", "Panama", "Dominican Republic"].includes(r.country)
  )
  .slice(0, 8);

const continent = {};
for (const [k, p] of Object.entries(ACCOR_CONTINENT_PAGES)) {
  const url = `https://all.accor.com/a/en/destination/continent/${p.slug}.html`;
  const res = await fetchText(url, ACCOR_FETCH_HEADERS);
  const hotels = res.ok ? parseAccorContinentHotelsFromHtml(res.text) : [];
  continent[k] = {
    url,
    status: res.status,
    ok: res.ok,
    count: hotels.length,
    sample: hotels.slice(0, 3),
  };
}

const wi = await fetchText(WYNDHAM_SITEMAP_INDEX, WYNDHAM_FETCH_HEADERS);
const children = wyndhamLocs(wi.text).filter((u) => /properties/i.test(u));
const child = await fetchText(children[0], WYNDHAM_FETCH_HEADERS);
const plocs = wyndhamLocs(child.text).filter((u) => /\/overview\/?$/i.test(u));

const patchPath = "reports/research-engine-v2/webhound-active-brand-coverage-gap-code-probe.json";
const patch = JSON.parse(fs.readFileSync(patchPath, "utf8"));
patch.probes.accor.continents = continent;
patch.probes.accor.sitemap.hotel_loc_count = locs.length;
patch.probes.accor.sitemap.url_shape_sample = locs.slice(0, 5);
patch.probes.accor.prior_cala_sample = calaPrior.map((r) => ({
  propertyId: r.propertyId,
  name: r.inferredHotelName,
  country: r.country,
  city: r.city,
  url: r.propertyUrl,
}));
patch.probes.accor.continent_url_pattern =
  "https://all.accor.com/a/en/destination/continent/{slug}.html";
patch.probes.wyndham.property_sitemaps_count = children.length;
patch.probes.wyndham.first_properties_overview_count = plocs.length;
patch.probes.wyndham.overview_url_sample = plocs.slice(0, 8);
patch.probes.wyndham.parsed_sample = plocs.slice(0, 5).map((u) => parseWyndhamPropertyUrl(u));
patch.reprobed_at = new Date().toISOString();
fs.writeFileSync(patchPath, JSON.stringify(patch, null, 2));

console.log(
  JSON.stringify(
    {
      accor_hotels: locs.length,
      continents: Object.fromEntries(
        Object.entries(continent).map(([k, v]) => [k, { status: v.status, count: v.count }])
      ),
      wyndham_property_sitemaps: children.length,
      wyndham_first_overview: plocs.length,
      prior_cala: calaPrior.length,
    },
    null,
    2
  )
);
