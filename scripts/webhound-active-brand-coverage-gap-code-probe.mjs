/**
 * Read-only code probe for Active Brand Setup gap parents.
 * No Airtable writes. Complements Webhound learning.
 */
import fs from "node:fs";
import {
  ACCOR_HOTEL_SITEMAP_EN,
  ACCOR_FETCH_HEADERS,
  extractSitemapLocs,
  accorHotelCodeFromUrl,
} from "../lib/accor-brand-directory-extract.js";
import {
  ACCOR_CONTINENT_PAGES,
  parseAccorContinentHotelsFromHtml,
} from "../lib/accor-continent-directory-extract.js";
import {
  WYNDHAM_SITEMAP_INDEX,
  WYNDHAM_FETCH_HEADERS,
} from "../lib/wyndham-brand-directory-extract.js";
import {
  BWH_ORIGIN,
  parseBwhPropertyUrl,
  loadBwhDirectorySeed,
} from "../lib/bwh-brand-directory-extract.js";
import {
  accorCountryCodeIsCala,
  wyndhamUrlLooksCala,
} from "../lib/brand-sitemap/cala-url-segments.js";

const TIMEOUT_MS = 25000;

async function fetchText(url, headers = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ...headers,
      },
      signal: ctrl.signal,
      redirect: "follow",
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, url: res.url, bytes: text.length, text };
  } catch (e) {
    return { ok: false, status: 0, url, error: String(e?.message || e), bytes: 0, text: "" };
  } finally {
    clearTimeout(t);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function probeAccor() {
  const sitemap = await fetchText(ACCOR_HOTEL_SITEMAP_EN, ACCOR_FETCH_HEADERS);
  const locs = sitemap.ok ? extractSitemapLocs(sitemap.text) : [];
  const hotelLocs = locs.filter((u) => /\/hotel\//i.test(u));
  const calaSample = [];
  const byCc = {};
  for (const u of hotelLocs) {
    const cc = (u.match(/\/([a-z]{2})\/hotel\//i) || [])[1]?.toUpperCase();
    if (!cc) continue;
    byCc[cc] = (byCc[cc] || 0) + 1;
    if (accorCountryCodeIsCala(cc) && calaSample.length < 8) {
      calaSample.push({ url: u, propertyId: accorHotelCodeFromUrl(u), cc });
    }
  }

  const continentProbes = {};
  for (const [key, page] of Object.entries(ACCOR_CONTINENT_PAGES)) {
    const url = `https://all.accor.com/a/en/destination/${page.slug}.html`;
    const res = await fetchText(url, ACCOR_FETCH_HEADERS);
    const hotels = res.ok ? parseAccorContinentHotelsFromHtml(res.text) : [];
    continentProbes[key] = {
      url,
      status: res.status,
      ok: res.ok,
      hotel_count_page1: hotels.length,
      sample: hotels.slice(0, 3).map((h) => ({
        propertyId: h.propertyId,
        name: h.inferredHotelName,
        url: h.propertyUrl,
      })),
    };
    await sleep(200);
  }

  return {
    parent: "Accor",
    sitemap: {
      url: ACCOR_HOTEL_SITEMAP_EN,
      status: sitemap.status,
      ok: sitemap.ok,
      hotel_loc_count: hotelLocs.length,
      cala_country_counts: Object.fromEntries(
        Object.entries(byCc)
          .filter(([cc]) => accorCountryCodeIsCala(cc))
          .sort((a, b) => b[1] - a[1])
      ),
      cala_sample: calaSample,
    },
    continents: continentProbes,
    property_url_pattern: "https://all.accor.com/hotel/{CODE}/index.en.shtml",
    property_id_pattern: "/hotel/([0-9A-Za-z]+)/",
    recommendation: "extend_existing_extractor_wire_autopilot",
  };
}

async function probeWyndham() {
  const index = await fetchText(WYNDHAM_SITEMAP_INDEX, WYNDHAM_FETCH_HEADERS);
  const childLocs = index.ok
    ? [...index.text.matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1].trim())
    : [];
  const hotelSitemaps = childLocs.filter((u) => /hotel|property|destination/i.test(u)).slice(0, 12);
  let calaUrls = [];
  for (const sm of hotelSitemaps.slice(0, 4)) {
    const res = await fetchText(sm, WYNDHAM_FETCH_HEADERS);
    if (!res.ok) continue;
    const locs = [...res.text.matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1].trim());
    for (const u of locs) {
      if (wyndhamUrlLooksCala(u)) calaUrls.push(u);
    }
    await sleep(150);
  }
  calaUrls = [...new Set(calaUrls)].slice(0, 20);

  return {
    parent: "Wyndham",
    sitemap_index: {
      url: WYNDHAM_SITEMAP_INDEX,
      status: index.status,
      ok: index.ok,
      child_count: childLocs.length,
      hotelish_children_sample: hotelSitemaps.slice(0, 8),
    },
    cala_property_url_sample: calaUrls.slice(0, 10),
    cala_url_count_sampled: calaUrls.length,
    property_url_hint: "wyndhamhotels.com/.../overview or brand path with property id",
    recommendation: "extend_existing_extractor_wire_autopilot",
  };
}

async function probeBwh() {
  const seed = loadBwhDirectorySeed();
  const hotels = Array.isArray(seed) ? seed : seed?.hotels || seed?.properties || [];
  const seedCount = hotels.length;
  const sampleUrl = hotels[0]?.propertyUrl || `${BWH_ORIGIN}/en_US/book/hotels-in-mexico`;
  const live = await fetchText(sampleUrl);
  const parsed = parseBwhPropertyUrl(hotels[0]?.propertyUrl || "");

  return {
    parent: "BWH Hotels",
    seed_catalog_count: seedCount,
    sample_seed_parse: parsed,
    live_probe: {
      url: sampleUrl,
      status: live.status,
      ok: live.ok,
      bytes: live.bytes,
      note: live.ok ? "reachable" : "likely_blocked_or_missing",
    },
    property_url_patterns: [
      "bestwestern.com/.../propertyCode.{NNNNN}.html",
      "bestwestern.com/.../hotel-details.{NNNNN}.html",
    ],
    recommendation: live.ok
      ? "extend_seed_plus_official_urls"
      : "steward_seed_catalog_live_often_blocked",
  };
}

async function probeSoftCollections() {
  const targets = [
    {
      parent: "Preferred Hotels & Resorts",
      urls: [
        "https://preferredhotels.com/",
        "https://preferredhotels.com/destinations",
        "https://preferredhotels.com/hotels",
      ],
    },
    {
      parent: "SLH",
      urls: [
        "https://www.slh.com/",
        "https://www.slh.com/hotels",
        "https://www.slh.com/destinations",
      ],
    },
    {
      parent: "Bunkhouse",
      urls: ["https://www.bunkhousehotels.com/", "https://www.bunkhousehotels.com/hotels"],
    },
    {
      parent: "Design Hotels (Accor soft brand)",
      urls: ["https://www.designhotels.com/", "https://www.designhotels.com/hotels"],
    },
  ];
  const out = [];
  for (const t of targets) {
    const probes = [];
    for (const url of t.urls) {
      const res = await fetchText(url);
      probes.push({
        url,
        status: res.status,
        ok: res.ok,
        bytes: res.bytes,
        has_json_ld: /application\/ld\+json/i.test(res.text),
        hotel_link_hints: (res.text.match(/\/hotel[s]?\//gi) || []).length,
      });
      await sleep(200);
    }
    out.push({ parent: t.parent, probes });
  }
  return out;
}

const report = {
  run_type: "webhound_active_brand_coverage_gap_code_probe",
  generated_at: new Date().toISOString(),
  production_writes: false,
  airtable_writes: false,
  webhound_session_id: "0abff16f-9e6e-4203-ae91-9ec79556cea6",
  probes: {},
};

report.probes.accor = await probeAccor();
await sleep(250);
report.probes.wyndham = await probeWyndham();
await sleep(250);
report.probes.bwh = await probeBwh();
await sleep(250);
report.probes.soft_collections = await probeSoftCollections();

fs.writeFileSync(
  "reports/research-engine-v2/webhound-active-brand-coverage-gap-code-probe.json",
  JSON.stringify(report, null, 2)
);
console.log(
  JSON.stringify(
    {
      ok: true,
      accor_sitemap_hotels: report.probes.accor.sitemap.hotel_loc_count,
      accor_cala_ccs: Object.keys(report.probes.accor.sitemap.cala_country_counts || {}).length,
      wyndham_cala_sample: report.probes.wyndham.cala_url_count_sampled,
      bwh_seed: report.probes.bwh.seed_catalog_count,
      soft: report.probes.soft_collections.map((s) => ({
        parent: s.parent,
        ok: s.probes.filter((p) => p.ok).length,
      })),
    },
    null,
    2
  )
);
