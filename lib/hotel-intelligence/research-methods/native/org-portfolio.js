/**
 * Organization → portfolio multiplier (Packet 2.4C).
 * Reconstruct public portfolio from first-party pages; match to Dealality dhl_* when possible.
 * Does NOT auto-assert OWNED_BY for every portfolio mention.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fetchResearchPage, htmlToSearchableText } from "../../room-count-research/fetch.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");

const ORG_PORTFOLIO_URLS = {
  "grupo hotelero santa fe": ["https://gsf-hotels.com/", "https://gsf-hotels.com/corporativo/"],
  "fibrahotel": ["https://www.fibrahotel.com/", "https://fibrahotel.mx/"],
  "fibra inn": ["https://fibrainn.mx/"],
  "grupo xcaret": ["https://www.hotelxcaret.com/", "https://www.grupoxcaret.com/"],
};

function loadGsfCohortHotels() {
  try {
    const raw = JSON.parse(
      fs.readFileSync(path.join(ROOT, "fixtures/golden-demo/gsf-mexico-ownership-cohort-v1.json"), "utf8")
    );
    return raw.hotels || [];
  } catch {
    return [];
  }
}

function normalizeOrgKey(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function classifyPortfolioRelationship(textNear, orgKey) {
  const t = String(textNear || "").toLowerCase();
  if (/third[- ]party|tercer|administrad|managed\s+by|operad/.test(t) && !/propiedad|owned|100%/.test(t)) {
    return "OPERATED_MANAGED";
  }
  if (/50\s*%|joint\s+venture|asociaci|fideicomiso/.test(t)) return "JV";
  if (/propiedad|owned|controlad|100\s*%/.test(t)) return "OWNED_CONTROLLED";
  if (/historical|former|anterior/.test(t)) return "HISTORICAL";
  return "UNKNOWN";
}

/**
 * @param {{ organizationName: string, alreadyResolvedHotelNames?: string[] }} opts
 */
export async function reconstructOrgPortfolio(opts = {}) {
  const orgName = opts.organizationName;
  const key = normalizeOrgKey(orgName);
  let urls = [];
  for (const [k, u] of Object.entries(ORG_PORTFOLIO_URLS)) {
    if (key.includes(k) || k.includes(key.slice(0, 12))) {
      urls = u;
      break;
    }
  }
  if (!urls.length && /santa\s+fe|gsf|hotel/i.test(orgName || "")) {
    urls = ORG_PORTFOLIO_URLS["grupo hotelero santa fe"];
  }

  const pages = [];
  for (const url of urls.slice(0, 2)) {
    const page = await fetchResearchPage(url, { timeoutMs: 20000 });
    if (page.ok) {
      pages.push({ url: page.url || url, text: htmlToSearchableText(page.text || "") });
    }
  }

  const corpus = pages.map((p) => p.text).join("\n");
  const cohort = loadGsfCohortHotels();
  const assets = [];

  // Match known Dealality hotels mentioned on org site
  for (const h of cohort) {
    const name = h.canonical_trading_name || h.name;
    if (!name) continue;
    const re = new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").slice(0, 40), "i");
    if (re.test(corpus) || new RegExp(String(h.name).slice(0, 24), "i").test(corpus)) {
      const idx = corpus.search(re);
      const near = idx >= 0 ? corpus.slice(Math.max(0, idx - 120), idx + 200) : "";
      assets.push({
        name,
        hotel_id: h.hotel_id || null,
        airtable_record_id: h.airtable_record_id || null,
        city: h.city,
        relationship_class: classifyPortfolioRelationship(near, key),
        match_basis: "cohort_name_on_org_site",
        source_urls: pages.map((p) => p.url),
      });
    }
  }

  // Heuristic hotel-name lines from site (non-matched)
  const hotelish = corpus.match(/(?:Krystal|Hyatt|Fiesta|City Express|Mahekal|Xcaret)[^\n.]{0,60}/gi) || [];
  for (const raw of [...new Set(hotelish)].slice(0, 20)) {
    const cleaned = raw.replace(/\s+/g, " ").trim();
    if (assets.some((a) => cleaned.toLowerCase().includes(String(a.name).toLowerCase().slice(0, 12)))) continue;
    assets.push({
      name: cleaned.slice(0, 80),
      hotel_id: null,
      airtable_record_id: null,
      city: null,
      relationship_class: "UNKNOWN",
      match_basis: "org_site_name_heuristic",
      source_urls: pages.map((p) => p.url),
    });
  }

  const censusMatches = assets.filter((a) => a.hotel_id).length;
  return {
    organization: orgName,
    pages_fetched: pages.map((p) => p.url),
    assets,
    portfolio_asset_count: assets.length,
    census_matches: censusMatches,
    owned_controlled: assets.filter((a) => a.relationship_class === "OWNED_CONTROLLED").length,
    operated_managed: assets.filter((a) => a.relationship_class === "OPERATED_MANAGED").length,
    jv: assets.filter((a) => a.relationship_class === "JV").length,
    unknown_relationship: assets.filter((a) => a.relationship_class === "UNKNOWN").length,
    note: "Portfolio membership ≠ OWNED_BY. Relationship_class is provisional from page context.",
  };
}
