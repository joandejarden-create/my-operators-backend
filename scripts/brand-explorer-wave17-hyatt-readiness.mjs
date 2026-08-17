#!/usr/bin/env node
/**
 * Wave 17 Hyatt cohort — READ-ONLY readiness + foundation audit.
 * Zero Airtable writes.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { isBrandStatusActive } from "../lib/brand-status-active.js";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { slugifyBrandName } from "../lib/partner-intelligence/brand-explorer-expansion-backlog-planner.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");
const PACKS = path.join(REPORTS, "brand-explorer-wave17-hyatt-source-packs");

const BASICS_TABLE = "Brand Setup - Brand Basics";

/** Founder-selected Wave 17 cohort in commercial priority order. */
const WAVE17_TARGETS = Object.freeze([
  {
    suppliedName: "Hyatt Centric",
    founderPriority: 1,
    strategicRole: "Urban lifestyle full-service",
    siblingWatch: ["Caption by Hyatt", "Hyatt Regency", "Hyatt Place", "Thompson Hotels", "Hotel Indigo", "Canopy by Hilton", "MGallery Collection"],
    officialUrls: [
      "https://www.hyatt.com/hyatt-centric",
      "https://newsroom.hyatt.com/hyatt-centric-brand-information",
      "https://about.hyatt.com/en/brands.html",
    ],
  },
  {
    suppliedName: "Hyatt Regency",
    founderPriority: 2,
    strategicRole: "Upper-upscale full-service / meetings",
    siblingWatch: ["Grand Hyatt", "Hyatt Centric", "Hyatt Place", "Marriott Hotels", "Sheraton", "Westin"],
    officialUrls: [
      "https://www.hyatt.com/hyatt-regency",
      "https://newsroom.hyatt.com/",
      "https://about.hyatt.com/en/brands.html",
    ],
  },
  {
    suppliedName: "Destination by Hyatt",
    founderPriority: 3,
    strategicRole: "Destination / resort affiliation collection",
    siblingWatch: ["The Unbound Collection by Hyatt", "Autograph Collection", "Tribute Portfolio", "Curio Collection by Hilton"],
    officialUrls: [
      "https://www.hyatt.com/destination-by-hyatt",
      "https://about.hyatt.com/en/brands.html",
      "https://newsroom.hyatt.com/",
    ],
  },
  {
    suppliedName: "The Unbound Collection by Hyatt",
    founderPriority: 4,
    strategicRole: "Soft brand / independent collection",
    siblingWatch: ["Destination by Hyatt", "Autograph Collection", "Tribute Portfolio", "Curio Collection by Hilton", "MGallery Collection", "Handwritten Collection", "Luxury Collection"],
    officialUrls: [
      "https://www.hyatt.com/unbound-collection",
      "https://about.hyatt.com/en/brands.html",
      "https://newsroom.hyatt.com/",
    ],
  },
  {
    suppliedName: "Thompson Hotels",
    founderPriority: 5,
    strategicRole: "Lifestyle / design-led",
    siblingWatch: ["Dream Hotels", "Hyatt Centric", "EDITION", "W Hotels", "Kimpton Hotels", "Hotel Indigo"],
    officialUrls: [
      "https://www.hyatt.com/thompson-hotels",
      "https://www.thompsonhotels.com/",
      "https://about.hyatt.com/en/brands.html",
    ],
  },
  {
    suppliedName: "Dream Hotels",
    founderPriority: 6,
    strategicRole: "Lifestyle / social / nightlife-leaning",
    siblingWatch: ["Thompson Hotels", "Hyatt Centric", "W Hotels", "EDITION"],
    officialUrls: [
      "https://www.hyatt.com/dream-hotels",
      "https://www.dreamhotels.com/",
      "https://about.hyatt.com/en/brands.html",
    ],
  },
  {
    suppliedName: "Caption by Hyatt",
    founderPriority: 7,
    strategicRole: "Lifestyle select-service / social commons",
    siblingWatch: ["Hyatt Centric", "Hyatt Place", "Hyatt House", "Moxy Hotels", "Aloft Hotels", "citizenM"],
    officialUrls: [
      "https://www.hyatt.com/caption-by-hyatt",
      "https://about.hyatt.com/en/brands.html",
      "https://newsroom.hyatt.com/",
    ],
  },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormula(v) {
  return String(v || "").replace(/'/g, "\\'");
}

async function airtableListAll(table, fields = []) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Airtable list failed ${res.status}`);
    rows.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return rows;
}

async function probeUrl(url) {
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: {
        "User-Agent": "Mozilla/5.0 DealalityBrandExplorerReadiness/1.0",
        Accept: "text/html,application/xhtml+xml",
      },
      signal: AbortSignal.timeout(15000),
    });
    const status = res.status;
    const botGated = status === 403 || status === 429;
    return { url, ok: status >= 200 && status < 400, status, botGated, missing: status === 404 };
  } catch (err) {
    return { url, ok: false, status: 0, botGated: false, missing: true, error: err.message };
  }
}

function normalizeName(s) {
  return nz(s)
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function nameSimilarity(a, b) {
  const na = normalizeName(a);
  const nb = normalizeName(b);
  if (!na || !nb) return 0;
  if (na === nb) return 1;
  const ta = na.split(" ").filter(Boolean);
  const tb = nb.split(" ").filter(Boolean);
  // Do NOT treat short parent tokens (e.g. "hyatt") as near-exact via includes().
  if (ta.length >= 2 && tb.length >= 2) {
    if (na.includes(nb) || nb.includes(na)) {
      const shorter = na.length <= nb.length ? ta : tb;
      if (shorter.length >= 2) return 0.9;
    }
  }
  let inter = 0;
  for (const t of ta) if (tb.has?.(t) || tb.includes(t)) inter += 1;
  return inter / Math.max(ta.size || ta.length, tb.size || tb.length);
}

/** Required / forbidden tokens for Wave 17 identity disambiguation. */
const IDENTITY_RULES = {
  "Hyatt Centric": { requireAny: [["centric"]], forbid: ["caption", "place", "house", "regency", "ziva", "zilara"] },
  "Hyatt Regency": { requireAny: [["regency"]], forbid: ["centric", "grand", "place", "house"] },
  "Destination by Hyatt": { requireAny: [["destination"]], forbid: ["unbound", "caption"] },
  "The Unbound Collection by Hyatt": { requireAny: [["unbound"]], forbid: ["destination", "caption"] },
  "Thompson Hotels": { requireAny: [["thompson"]], forbid: ["dream", "dreams", "design"] },
  "Dream Hotels": { requireAny: [["dream"]], forbid: ["dreams", "design", "thompson"] },
  "Caption by Hyatt": { requireAny: [["caption"]], forbid: ["centric", "destination", "place", "house"] },
};

function passesIdentityRules(suppliedName, candidateName) {
  const rules = IDENTITY_RULES[suppliedName];
  if (!rules) return true;
  const n = normalizeName(candidateName);
  const tokens = new Set(n.split(" "));
  const hasRequired = (rules.requireAny || []).some((group) =>
    group.every((t) => tokens.has(t))
  );
  if (!hasRequired) return false;
  // Reject clear wrong-brand tokens when required brand token is absent as exact token
  if (suppliedName === "Dream Hotels" && (tokens.has("design") || tokens.has("dreams") || tokens.has("thompson"))) {
    return false;
  }
  if (rules.forbid?.some((f) => tokens.has(f) && !(rules.requireAny || []).some((g) => g.includes(f)))) {
    // if forbid token present and it's not part of required group, reject when it indicates sibling
    const siblingHit = rules.forbid.filter((f) => tokens.has(f));
    if (siblingHit.length && !hasRequired) return false;
  }
  return true;
}

function resolveIdentity(supplied, basics) {
  const scored = basics
    .map((b) => ({ ...b, sim: nameSimilarity(supplied, b.name) }))
    .filter((b) => b.sim >= 0.5 && passesIdentityRules(supplied, b.name))
    // Never auto-pick bare parent "Hyatt" or vacation-ownership Hyatt for brand targets
    .filter((b) => normalizeName(b.name) !== "hyatt")
    .filter((b) => !/vacation ownership/i.test(b.parentCompany || ""))
    .sort((a, b) => b.sim - a.sim || String(a.name).localeCompare(b.name));

  const exact = scored.filter((b) => normalizeName(b.name) === normalizeName(supplied));
  const nearExact = scored.filter((b) => b.sim >= 0.9);
  const primaryPool = exact.length ? exact : nearExact.length ? nearExact : scored.filter((b) => b.sim >= 0.8);

  if (!scored.length) {
    return {
      primary: null,
      identityStatus: "IDENTITY_REVIEW_REQUIRED",
      identityConfidence: "low",
      duplicates: [],
      notes: ["no_brand_basics_match"],
    };
  }

  // Dream Hotels special: Dreams Resorts & Spas is a different brand — reject if only dreams match
  if (supplied === "Dream Hotels") {
    const dreamHotels = scored.filter((b) => /^dream hotels$/i.test(normalizeName(b.name)));
    if (!dreamHotels.length) {
      return {
        primary: null,
        identityStatus: "IDENTITY_REVIEW_REQUIRED",
        identityConfidence: "low",
        duplicates: scored.slice(0, 6),
        notes: [
          "missing_exact_dream_hotels_basics_record",
          "do_not_use_dreams_resorts_spas",
          "do_not_use_design_hotels",
        ],
      };
    }
  }

  if (primaryPool.length > 1) {
    const distinct = [...new Set(primaryPool.map((p) => p.name))];
    if (distinct.length > 1) {
      return {
        primary: null,
        identityStatus: "IDENTITY_REVIEW_REQUIRED",
        identityConfidence: "low",
        duplicates: primaryPool,
        notes: ["multiple_plausible_records"],
      };
    }
  }

  const primary = primaryPool[0] || (scored[0].sim >= 0.8 ? scored[0] : null);
  if (!primary) {
    return {
      primary: null,
      identityStatus: "IDENTITY_REVIEW_REQUIRED",
      identityConfidence: "low",
      duplicates: scored.slice(0, 6),
      notes: ["no_high_confidence_match"],
    };
  }

  const fuzzy = normalizeName(primary.name) !== normalizeName(supplied);
  return {
    primary,
    identityStatus: fuzzy ? "RESOLVED_FUZZY" : "RESOLVED",
    identityConfidence: fuzzy ? "medium" : "high",
    duplicates: scored.slice(0, 8),
    notes: fuzzy ? [`basics_name_alias:${supplied}=>${primary.name}`] : [],
  };
}

function classifyScore(total) {
  if (total >= 90) return "BUILD_PRIORITY";
  if (total >= 85) return "BUILD_ELIGIBLE";
  if (total >= 75) return "REMEDIATE_BEFORE_BUILD";
  if (total >= 60) return "SIGNIFICANT_BUILD_REQUIRED";
  return "HOLD";
}

function scoreBrand({
  identityConfidence,
  officialOkCount,
  officialKnownCount,
  presentationRowCount,
  presentationWithImage,
  languageFlags,
  softBrand,
  lifestyleCluster,
  propertyCandidateCount,
}) {
  let identity = 12;
  if (identityConfidence === "high") identity = 20;
  else if (identityConfidence === "medium") identity = 18;
  else if (identityConfidence === "low") identity = 10;

  // Prefer live 200s; bot-gated first-party Hyatt URLs still count as known official support.
  const officialSignal = Math.max(officialOkCount, officialKnownCount);
  let official = 8;
  if (officialSignal >= 2) official = 20;
  else if (officialSignal === 1) official = 16;
  else if (officialSignal > 0) official = 10;

  let property = 8;
  if (presentationWithImage >= 9) property = 15;
  else if (presentationWithImage >= 6) property = 13;
  else if (propertyCandidateCount >= 6) property = 13;
  else if (propertyCandidateCount >= 3 || officialSignal >= 1) property = 12;

  let momentum = officialSignal >= 1 ? 13 : 6;
  if (softBrand) momentum = Math.min(momentum, 12);

  let copy = presentationRowCount >= 80 ? 14 : presentationRowCount >= 40 ? 12 : officialSignal >= 1 ? 12 : 7;
  if (languageFlags.length) copy = Math.max(6, copy - 2);

  let tab = presentationRowCount >= 80 ? 9 : presentationRowCount >= 40 ? 7 : officialSignal >= 1 ? 8 : 4;

  let semantic = 5;
  if (softBrand) semantic -= 1;
  if (lifestyleCluster) semantic -= 1;
  if (semantic < 3) semantic = 3;

  const scores = {
    identity,
    officialSupport: official,
    propertyExamples: property,
    momentumReadiness: momentum,
    publicCopy: copy,
    tabReadiness: tab,
    semantic,
  };
  const total = Object.values(scores).reduce((a, b) => a + b, 0);
  return { scores, total };
}

function isKnownOfficialHyattUrl(url) {
  return /hyatt\.com|newsroom\.hyatt\.com|about\.hyatt\.com|thompsonhotels\.com|dreamhotels\.com/i.test(
    url || ""
  );
}

/** Curated property candidates (official Hyatt portfolio associations). */
const PROPERTY_CANDIDATES = {
  "hyatt-centric": [
    { propertyName: "Hyatt Centric Midtown 5th Avenue New York", city: "New York", country: "USA", geography: "International Reference", role: "gallery/openings", sourcePage: "https://www.hyatt.com/hyatt-centric/nycmt-hyatt-centric-midtown-5th-avenue-new-york", confidence: "high" },
    { propertyName: "Hyatt Centric Times Square New York", city: "New York", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/hyatt-centric", confidence: "high" },
    { propertyName: "Hyatt Centric Downtown Miami", city: "Miami", country: "USA", geography: "International Reference / CALA-adjacent", role: "gallery/openings", sourcePage: "https://www.hyatt.com/hyatt-centric", confidence: "high" },
    { propertyName: "Hyatt Centric Guatemala City", city: "Guatemala City", country: "Guatemala", geography: "CALA", role: "gallery/openings", sourcePage: "https://www.hyatt.com/hyatt-centric", confidence: "medium" },
    { propertyName: "Hyatt Centric San Isidro Lima", city: "Lima", country: "Peru", geography: "CALA", role: "gallery", sourcePage: "https://www.hyatt.com/hyatt-centric", confidence: "medium" },
    { propertyName: "Hyatt Centric The Pike Long Beach", city: "Long Beach", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/hyatt-centric", confidence: "high" },
    { propertyName: "Hyatt Centric Chicago Magnificent Mile", city: "Chicago", country: "USA", geography: "International Reference", role: "scenario/openings", sourcePage: "https://www.hyatt.com/hyatt-centric", confidence: "high" },
    { propertyName: "Hyatt Centric Candido Mendes Rio de Janeiro", city: "Rio de Janeiro", country: "Brazil", geography: "CALA", role: "gallery", sourcePage: "https://www.hyatt.com/hyatt-centric", confidence: "medium" },
  ],
  "hyatt-regency": [
    { propertyName: "Hyatt Regency Miami", city: "Miami", country: "USA", geography: "International Reference / CALA-adjacent", role: "gallery/openings", sourcePage: "https://www.hyatt.com/hyatt-regency", confidence: "high" },
    { propertyName: "Hyatt Regency Mexico City", city: "Mexico City", country: "Mexico", geography: "CALA", role: "gallery/openings", sourcePage: "https://www.hyatt.com/hyatt-regency", confidence: "high" },
    { propertyName: "Hyatt Regency Orlando", city: "Orlando", country: "USA", geography: "International Reference", role: "gallery/scenario", sourcePage: "https://www.hyatt.com/hyatt-regency", confidence: "high" },
    { propertyName: "Hyatt Regency Chicago", city: "Chicago", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/hyatt-regency", confidence: "high" },
    { propertyName: "Hyatt Regency San Antonio Riverwalk", city: "San Antonio", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/hyatt-regency", confidence: "high" },
    { propertyName: "Hyatt Regency Trinidad", city: "Port of Spain", country: "Trinidad and Tobago", geography: "CALA", role: "openings", sourcePage: "https://www.hyatt.com/hyatt-regency", confidence: "medium" },
    { propertyName: "Hyatt Regency Grand Cypress", city: "Orlando", country: "USA", geography: "International Reference", role: "scenario", sourcePage: "https://www.hyatt.com/hyatt-regency", confidence: "high" },
    { propertyName: "Hyatt Regency Cancun", city: "Cancún", country: "Mexico", geography: "CALA", role: "gallery/openings", sourcePage: "https://www.hyatt.com/hyatt-regency", confidence: "medium" },
  ],
  "destination-by-hyatt": [
    { propertyName: "Cavallo Point Lodge", city: "Sausalito", country: "USA", geography: "International Reference", role: "gallery/openings", sourcePage: "https://www.hyatt.com/destination-by-hyatt", confidence: "high" },
    { propertyName: "The Lodge at Torrey Pines", city: "La Jolla", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/destination-by-hyatt", confidence: "high" },
    { propertyName: "L'Auberge de Sedona", city: "Sedona", country: "USA", geography: "International Reference", role: "gallery/scenario", sourcePage: "https://www.hyatt.com/destination-by-hyatt", confidence: "high" },
    { propertyName: "Amangiri (confirm Destination vs Unbound affiliation before use)", city: "Canyon Point", country: "USA", geography: "International Reference", role: "identity_check", sourcePage: "https://www.hyatt.com/destination-by-hyatt", confidence: "low" },
    { propertyName: "Samoset Resort", city: "Rockport", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/destination-by-hyatt", confidence: "medium" },
    { propertyName: "The Inn at Perry Cabin", city: "St. Michaels", country: "USA", geography: "International Reference", role: "gallery/openings", sourcePage: "https://www.hyatt.com/destination-by-hyatt", confidence: "medium" },
    { propertyName: "CALA Destination property (confirm live Destination by Hyatt affiliation)", city: "TBD", country: "CALA", geography: "CALA", role: "needs_research", sourcePage: "https://www.hyatt.com/destination-by-hyatt", confidence: "low" },
  ],
  "the-unbound-collection-by-hyatt": [
    { propertyName: "Hotel Martinez, The Unbound Collection by Hyatt", city: "Cannes", country: "France", geography: "International Reference", role: "gallery/openings", sourcePage: "https://www.hyatt.com/unbound-collection", confidence: "high" },
    { propertyName: "The Confidante Miami Beach", city: "Miami Beach", country: "USA", geography: "International Reference / CALA-adjacent", role: "gallery/openings", sourcePage: "https://www.hyatt.com/unbound-collection", confidence: "high" },
    { propertyName: "Hotel Magaw (confirm live Unbound affiliation)", city: "TBD", country: "USA", geography: "International Reference", role: "needs_research", sourcePage: "https://www.hyatt.com/unbound-collection", confidence: "low" },
    { propertyName: "The Driskill, The Unbound Collection by Hyatt", city: "Austin", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/unbound-collection", confidence: "high" },
    { propertyName: "Hotel Emma, The Unbound Collection by Hyatt", city: "San Antonio", country: "USA", geography: "International Reference", role: "gallery/scenario", sourcePage: "https://www.hyatt.com/unbound-collection", confidence: "high" },
    { propertyName: "CALA Unbound property (confirm live affiliation)", city: "TBD", country: "CALA", geography: "CALA", role: "needs_research", sourcePage: "https://www.hyatt.com/unbound-collection", confidence: "low" },
    { propertyName: "Independent character hotel under Unbound (confirm affiliation)", city: "TBD", country: "International", geography: "International Reference", role: "openings", sourcePage: "https://www.hyatt.com/unbound-collection", confidence: "medium" },
  ],
  "thompson-hotels": [
    { propertyName: "Thompson Chicago", city: "Chicago", country: "USA", geography: "International Reference", role: "gallery/openings", sourcePage: "https://www.hyatt.com/thompson-hotels", confidence: "high" },
    { propertyName: "Thompson Nashville", city: "Nashville", country: "USA", geography: "International Reference", role: "gallery/scenario", sourcePage: "https://www.hyatt.com/thompson-hotels", confidence: "high" },
    { propertyName: "Thompson Dallas", city: "Dallas", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/thompson-hotels", confidence: "high" },
    { propertyName: "Thompson Madrid", city: "Madrid", country: "Spain", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/thompson-hotels", confidence: "high" },
    { propertyName: "Thompson Playa del Carmen", city: "Playa del Carmen", country: "Mexico", geography: "CALA", role: "gallery/openings", sourcePage: "https://www.hyatt.com/thompson-hotels", confidence: "high" },
    { propertyName: "Thompson Seattle", city: "Seattle", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/thompson-hotels", confidence: "high" },
    { propertyName: "Thompson Houston", city: "Houston", country: "USA", geography: "International Reference", role: "openings", sourcePage: "https://www.hyatt.com/thompson-hotels", confidence: "medium" },
  ],
  "dream-hotels": [
    { propertyName: "Dream Downtown New York", city: "New York", country: "USA", geography: "International Reference", role: "gallery/openings", sourcePage: "https://www.hyatt.com/dream-hotels", confidence: "high" },
    { propertyName: "Dream Hollywood", city: "Los Angeles", country: "USA", geography: "International Reference", role: "gallery/scenario", sourcePage: "https://www.hyatt.com/dream-hotels", confidence: "high" },
    { propertyName: "Dream Midtown", city: "New York", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/dream-hotels", confidence: "high" },
    { propertyName: "Dream Nashville", city: "Nashville", country: "USA", geography: "International Reference", role: "gallery", sourcePage: "https://www.hyatt.com/dream-hotels", confidence: "medium" },
    { propertyName: "Dream South Beach", city: "Miami Beach", country: "USA", geography: "International Reference / CALA-adjacent", role: "gallery/openings", sourcePage: "https://www.hyatt.com/dream-hotels", confidence: "high" },
    { propertyName: "CALA Dream property (confirm live Dream Hotels affiliation)", city: "TBD", country: "CALA", geography: "CALA", role: "needs_research", sourcePage: "https://www.hyatt.com/dream-hotels", confidence: "low" },
  ],
  "caption-by-hyatt": [
    { propertyName: "Caption by Hyatt Beale Street Memphis", city: "Memphis", country: "USA", geography: "International Reference", role: "gallery/openings", sourcePage: "https://www.hyatt.com/caption-by-hyatt", confidence: "high" },
    { propertyName: "Caption by Hyatt Denver Cherry Creek", city: "Denver", country: "USA", geography: "International Reference", role: "gallery/scenario", sourcePage: "https://www.hyatt.com/caption-by-hyatt", confidence: "high" },
    { propertyName: "Caption by Hyatt (confirm additional live openings)", city: "TBD", country: "USA", geography: "International Reference", role: "needs_research", sourcePage: "https://www.hyatt.com/caption-by-hyatt", confidence: "medium" },
    { propertyName: "Caption pipeline / announced development (confirm)", city: "TBD", country: "USA", geography: "International Reference", role: "momentum_candidate", sourcePage: "https://newsroom.hyatt.com/", confidence: "medium" },
    { propertyName: "CALA Caption (confirm if any live affiliation)", city: "TBD", country: "CALA", geography: "CALA", role: "needs_research", sourcePage: "https://www.hyatt.com/caption-by-hyatt", confidence: "low" },
    { propertyName: "Caption mixed-use / social commons prototype property", city: "TBD", country: "USA", geography: "International Reference", role: "scenario", sourcePage: "https://www.hyatt.com/caption-by-hyatt", confidence: "medium" },
  ],
};

const MOMENTUM_SEEDS = {
  "hyatt-centric": [
    { type: "openings", headline: "Hyatt Centric global expansion with new & upcoming openings", sourceHint: "Hyatt / Business Wire Feb 2025", brandSpecific: true },
    { type: "pipeline", headline: "Urban destination Centric pipeline additions (confirm property-level)", sourceHint: "hyatt.com/hyatt-centric", brandSpecific: true },
    { type: "portfolio", headline: "Centric CALA / Americas openings (confirm dated items)", sourceHint: "Hyatt newsroom", brandSpecific: true },
  ],
  "hyatt-regency": [
    { type: "openings", headline: "Hyatt Regency openings / renovations (property-specific dated items)", sourceHint: "Hyatt newsroom", brandSpecific: true },
    { type: "meetings", headline: "Regency meetings/group product milestones (brand-specific only)", sourceHint: "Hyatt newsroom", brandSpecific: true },
    { type: "pipeline", headline: "Regency new-build / conversion announcements", sourceHint: "Hyatt development", brandSpecific: true },
  ],
  "destination-by-hyatt": [
    { type: "affiliation", headline: "Destination by Hyatt new independent resort affiliations", sourceHint: "Hyatt newsroom", brandSpecific: true },
    { type: "portfolio", headline: "Destination collection additions / exits (confirm affiliation)", sourceHint: "hyatt.com/destination-by-hyatt", brandSpecific: true },
  ],
  "the-unbound-collection-by-hyatt": [
    { type: "affiliation", headline: "Unbound Collection new independent hotel additions", sourceHint: "Hyatt newsroom", brandSpecific: true },
    { type: "portfolio", headline: "Unbound soft-brand conversion / affiliation milestones", sourceHint: "hyatt.com/unbound-collection", brandSpecific: true },
  ],
  "thompson-hotels": [
    { type: "openings", headline: "Thompson Hotels openings (e.g. lifestyle urban markets)", sourceHint: "Hyatt newsroom / thompsonhotels.com", brandSpecific: true },
    { type: "pipeline", headline: "Thompson development announcements under Hyatt", sourceHint: "Hyatt newsroom", brandSpecific: true },
  ],
  "dream-hotels": [
    { type: "openings", headline: "Dream Hotels openings / renovations post-Hyatt integration", sourceHint: "Hyatt newsroom / dreamhotels.com", brandSpecific: true },
    { type: "brand", headline: "Dream brand positioning updates under Hyatt World of Hyatt", sourceHint: "Hyatt newsroom", brandSpecific: true },
  ],
  "caption-by-hyatt": [
    { type: "openings", headline: "Caption by Hyatt openings (Memphis / Denver and pipeline)", sourceHint: "Hyatt newsroom", brandSpecific: true },
    { type: "pipeline", headline: "Caption mixed-use / select-service development announcements", sourceHint: "Hyatt development", brandSpecific: true },
  ],
};

async function main() {
  console.log("[wave17-hyatt-readiness] READ ONLY — starting");
  const universeBefore = await loadActiveUniverse({ includeDetails: false });
  const activeSlugs = (universeBefore.brands || []).map((b) => nz(b.slug).toLowerCase()).filter(Boolean).sort();
  console.log(`[wave17] Active universe: ${universeBefore.totalCount}`);

  const basicsFields = [
    "Brand Name",
    "Brand Status",
    "Parent Company",
    "Company Validated",
    "Active Profile Approved",
    "Ready for Active Profile",
    "Founder Visual Review Pass",
  ];
  console.log("[wave17] Listing Brand Basics (read-only)…");
  const basicsRows = await airtableListAll(BASICS_TABLE, basicsFields);
  const basics = basicsRows.map((r) => ({
    recordId: r.id,
    name: nz(r.fields?.["Brand Name"]),
    brandStatus: nz(r.fields?.["Brand Status"]),
    parentCompany: nz(r.fields?.["Parent Company"]),
    companyValidated: r.fields?.["Company Validated"] === true,
    activeProfileApproved: r.fields?.["Active Profile Approved"] === true,
    readyForActiveProfile: r.fields?.["Ready for Active Profile"] === true,
    founderVisualReviewPass: r.fields?.["Founder Visual Review Pass"] === true,
  }));

  const hyattish = basics.filter((b) =>
    /hyatt|thompson|dream|caption|unbound|destination/i.test(b.name)
  );

  const results = [];
  for (const target of WAVE17_TARGETS) {
    const supplied = target.suppliedName;
    const resolved = resolveIdentity(supplied, basics);
    let identityStatus = resolved.identityStatus;
    let identityConfidence = resolved.identityConfidence;
    let primary = resolved.primary;
    const duplicates = resolved.duplicates || [];

    const alreadyActive = primary && isBrandStatusActive(primary.brandStatus);
    if (alreadyActive) {
      identityStatus = "ALREADY_ACTIVE";
    }

    const slug = primary
      ? slugifyBrandName(primary.name) || slugifyBrandName(supplied)
      : slugifyBrandName(supplied);

    const probes = [];
    for (const url of target.officialUrls) {
      probes.push(await probeUrl(url));
    }
    const officialOkCount = probes.filter((p) => p.ok).length;
    const officialKnownCount = probes.filter(
      (p) => p.ok || (p.botGated && isKnownOfficialHyattUrl(p.url)) || (p.status === 429 && isKnownOfficialHyattUrl(p.url))
    ).length;

    let presentationRowCount = 0;
    let presentationWithImage = 0;
    let languageFlags = [];
    if (primary?.recordId && primary?.name && identityStatus !== "IDENTITY_REVIEW_REQUIRED") {
      try {
        const { rows } = await listPresentationRowsLight(primary.recordId, primary.name);
        presentationRowCount = rows.length;
        presentationWithImage = rows.filter((r) => r.imageUrl).length;
        const blob = rows.map((r) => `${r.title}\n${r.body}`).join("\n");
        const prohibited = [
          "Census",
          "Webhound",
          "PVQL",
          "source pack",
          "scraped",
          "FDD",
          "Item 19",
          "DataForSEO",
          "Hotelbeds",
          "provenance",
        ];
        for (const p of prohibited) {
          if (new RegExp(`\\b${p}\\b`, "i").test(blob)) languageFlags.push(p);
        }
      } catch (err) {
        languageFlags.push(`presentation_read_error:${err.message}`);
      }
    }

    const collectionSoftBrand =
      /unbound|destination by hyatt/i.test(supplied) || /unbound|destination/i.test(primary?.name || "");
    const semanticRiskNotes = [...target.siblingWatch.map((s) => `differentiate_vs:${s}`)];
    if (collectionSoftBrand) semanticRiskNotes.push("soft_brand_collection_ambiguity");
    if (/dream|thompson/i.test(supplied)) semanticRiskNotes.push("lifestyle_sibling_overlap");
    if (/caption/i.test(supplied)) semanticRiskNotes.push("select_service_lifestyle_overlap");

    let scores = null;
    let total = null;
    let classification = identityStatus;
    const statusOkForScore =
      primary &&
      (nz(primary.brandStatus) === "Under Review" || nz(primary.brandStatus) === "Draft");
    const eligibleForScore =
      primary &&
      !alreadyActive &&
      identityStatus !== "IDENTITY_REVIEW_REQUIRED" &&
      identityStatus !== "DUPLICATE_REVIEW" &&
      identityStatus !== "TAXONOMY_REVIEW" &&
      statusOkForScore;

    if (eligibleForScore) {
      const propertyKey =
        slug === "unbound-collection-by-hyatt"
          ? "the-unbound-collection-by-hyatt"
          : slug;
      const propertyCandidatesEarly =
        PROPERTY_CANDIDATES[propertyKey] ||
        PROPERTY_CANDIDATES[slug] ||
        PROPERTY_CANDIDATES[slugifyBrandName(supplied)] ||
        [];
      const scoredObj = scoreBrand({
        identityConfidence,
        officialOkCount,
        officialKnownCount,
        presentationRowCount,
        presentationWithImage,
        languageFlags,
        softBrand: collectionSoftBrand,
        lifestyleCluster: /centric|thompson|dream|caption/i.test(supplied),
        propertyCandidateCount: propertyCandidatesEarly.filter((p) => p.confidence !== "low").length,
      });
      scores = scoredObj.scores;
      total = scoredObj.total;
      classification = classifyScore(total);
      if (nz(primary.brandStatus) === "Draft" && total >= 85) {
        classification = total >= 90 ? "BUILD_PRIORITY" : "BUILD_ELIGIBLE";
      }
    } else if (alreadyActive) {
      classification = "ALREADY_ACTIVE";
    } else if (identityStatus === "IDENTITY_REVIEW_REQUIRED") {
      classification = "IDENTITY_REVIEW_REQUIRED";
    }

    const buildRisk =
      classification === "IDENTITY_REVIEW_REQUIRED" || classification === "ALREADY_ACTIVE"
        ? "N/A"
        : collectionSoftBrand
          ? total >= 90
            ? "MODERATE"
            : "HIGH"
          : /thompson|dream|caption/i.test(supplied)
            ? total >= 90
              ? "LOW"
              : "MODERATE"
            : total >= 90
              ? "LOW"
              : total >= 85
                ? "MODERATE"
                : "HIGH";

    const propertyKey =
      slug === "unbound-collection-by-hyatt"
        ? "the-unbound-collection-by-hyatt"
        : slug;
    const propertyCandidates =
      PROPERTY_CANDIDATES[propertyKey] ||
      PROPERTY_CANDIDATES[slug] ||
      PROPERTY_CANDIDATES[slugifyBrandName(supplied)] ||
      [];
    const momentumCandidates =
      MOMENTUM_SEEDS[propertyKey] ||
      MOMENTUM_SEEDS[slug] ||
      MOMENTUM_SEEDS[slugifyBrandName(supplied)] ||
      [];

    const fieldSupport = {
      overview: officialOkCount >= 1 ? "READY" : "NEEDS_RESEARCH",
      positioning: officialOkCount >= 1 ? "READY" : "NEEDS_RESEARCH",
      projectFit: officialOkCount >= 1 ? "PARTIAL" : "NEEDS_RESEARCH",
      ownerProposition: officialOkCount >= 1 ? "PARTIAL" : "NEEDS_RESEARCH",
      economics: "PARTIAL",
      operatingModel: officialOkCount >= 1 ? "PARTIAL" : "NEEDS_RESEARCH",
      guestExperience: officialOkCount >= 1 ? "READY" : "NEEDS_RESEARCH",
      fb: /regency|thompson|dream|centric|destination|unbound/i.test(supplied) ? "PARTIAL" : "NEEDS_RESEARCH",
      meetingsGroups: /regency/i.test(supplied) ? "READY" : /centric|thompson|destination/i.test(supplied) ? "PARTIAL" : "NOT_APPLICABLE",
      loyaltyDistribution: "READY",
      standards: "PARTIAL",
      footprint: officialOkCount >= 1 ? "PARTIAL" : "NEEDS_RESEARCH",
      propertyExamples: propertyCandidates.filter((p) => p.confidence !== "low").length >= 3 ? "PARTIAL" : "NEEDS_RESEARCH",
      valueScenarios: "PARTIAL",
      ownerWatchouts: "PARTIAL",
      recentMomentum: "PARTIAL",
      imageInventory: propertyCandidates.length >= 6 ? "PARTIAL" : "NEEDS_RESEARCH",
      softBrandDifferentiation: collectionSoftBrand ? "SEMANTIC_RISK" : "NOT_APPLICABLE",
    };

    const draftStatusBlocker =
      primary && nz(primary.brandStatus) === "Draft"
        ? ["brand_status_draft_move_to_under_review_before_presentation_writes"]
        : [];

    results.push({
      suppliedName: supplied,
      exactBrandBasicsName: primary?.name || null,
      recordId: primary?.recordId || null,
      slug,
      parentCompany: primary?.parentCompany || null,
      brandStatus: primary?.brandStatus || null,
      founderPriority: target.founderPriority,
      strategicRole: target.strategicRole,
      identityStatus,
      identityConfidence,
      duplicateCandidates: duplicates.slice(0, 8).map((d) => ({
        name: d.name,
        recordId: d.recordId,
        brandStatus: d.brandStatus,
        similarity: Number(d.sim.toFixed(2)),
      })),
      aliasIssues: [
        ...(resolved.notes || []),
        ...(primary && normalizeName(primary.name) !== normalizeName(supplied)
          ? [`supplied_vs_basics_name_diff:${supplied}=>${primary.name}`]
          : []),
      ],
      eligibleForBuildPrep: total != null && total >= 85,
      scores,
      total,
      classification,
      buildRisk,
      primaryBlockers: [
        ...(alreadyActive ? ["already_active"] : []),
        ...(identityStatus === "IDENTITY_REVIEW_REQUIRED" ? ["identity_review_required"] : []),
        ...(total != null && total < 85 ? ["score_below_85"] : []),
        ...draftStatusBlocker,
        ...(languageFlags.length ? [`language_flags:${languageFlags.join("|")}`] : []),
      ],
      notes: [
        presentationRowCount === 0 && officialOkCount >= 1
          ? "presentation_empty_but_official_sources_support_factory_build"
          : null,
        collectionSoftBrand ? "soft_brand_collection_requires_strong_owner_differentiation" : null,
        nz(primary?.brandStatus) === "Draft"
          ? "basics_status_is_draft_not_under_review"
          : null,
        ...semanticRiskNotes.slice(0, 2),
      ].filter(Boolean),
      siblingWatch: target.siblingWatch,
      officialUrlProbes: probes,
      presentationRowCount,
      presentationWithImage,
      languageFlags,
      propertyCandidates,
      momentumCandidates,
      fieldSupport,
      companyValidated: primary?.companyValidated === true,
      releaseFieldsPresent: {
        activeProfileApproved: primary?.activeProfileApproved === true,
        readyForActiveProfile: primary?.readyForActiveProfile === true,
        founderVisualReviewPass: primary?.founderVisualReviewPass === true,
      },
    });
  }

  // Sequencing
  const buildEligible = results
    .filter((r) => r.eligibleForBuildPrep)
    .sort((a, b) => {
      const riskRank = { LOW: 0, MODERATE: 1, HIGH: 2 };
      const ra = riskRank[a.buildRisk] ?? 9;
      const rb = riskRank[b.buildRisk] ?? 9;
      if (ra !== rb) return ra - rb;
      if ((b.total || 0) !== (a.total || 0)) return (b.total || 0) - (a.total || 0);
      return a.founderPriority - b.founderPriority;
    });

  const batchA = buildEligible.slice(0, Math.min(4, Math.ceil(buildEligible.length / 2) || buildEligible.length));
  // Prefer founder top priorities in A when risk comparable — already sorted by risk then score then founder priority
  // Ensure Centric/Regency in A if eligible and LOW/MODERATE
  const batchASlugs = new Set(batchA.map((b) => b.slug));
  for (const must of ["hyatt-centric", "hyatt-regency"]) {
    const row = buildEligible.find((b) => b.slug === must);
    if (row && !batchASlugs.has(must) && batchA.length < 4) {
      batchA.push(row);
      batchASlugs.add(must);
    }
  }
  const batchB = buildEligible.filter((b) => !batchASlugs.has(b.slug));

  const universeAfter = await loadActiveUniverse({ includeDetails: false });
  const activeUnchanged =
    universeAfter.totalCount === universeBefore.totalCount &&
    JSON.stringify((universeAfter.brands || []).map((b) => b.slug).sort()) ===
      JSON.stringify(activeSlugs);

  const scoringAtLeast85 = results.filter((r) => (r.total || 0) >= 85).length;
  const identityBlocked = results.filter((r) => r.classification === "IDENTITY_REVIEW_REQUIRED");
  const readyStatement =
    scoringAtLeast85 >= 1
      ? identityBlocked.length
        ? "wave17_hyatt_foundation_complete_partial_build_eligibility"
        : scoringAtLeast85 === results.filter((r) => r.brandStatus === "Under Review" || r.brandStatus === "Draft").length
          ? "wave17_hyatt_foundation_complete_build_batches_identified"
          : "wave17_hyatt_foundation_complete_partial_build_eligibility"
      : identityBlocked.length === results.length
        ? "wave17_hyatt_foundation_blocked_identity_review"
        : "wave17_hyatt_foundation_complete_partial_build_eligibility";

  // Assign recommended batch on each row
  for (const r of results) {
    if (batchA.some((b) => b.slug === r.slug)) r.recommendedBatch = "A";
    else if (batchB.some((b) => b.slug === r.slug)) r.recommendedBatch = "B";
    else r.recommendedBatch = null;
  }

  const report = {
    version: "brand-explorer-wave17-hyatt-readiness-v1",
    generatedAt: new Date().toISOString(),
    readyStatement,
    airtableWrites: false,
    writePerformed: false,
    writeAudit: {
      brandStatusWrites: 0,
      releaseWrites: 0,
      companyValidatedWrites: 0,
      companyValidationDateWrites: 0,
      brandVerifiedWrites: 0,
      censusWrites: 0,
      recentMomentumWrites: 0,
      presentationWrites: 0,
      imageWrites: 0,
      nonTargetWrites: 0,
    },
    preflight: {
      activeUniverseCountBefore: universeBefore.totalCount,
      activeUniverseCountAfter: universeAfter.totalCount,
      activeUniverseUnchanged: activeUnchanged,
      activeSlugs,
      hyattTargetsInActive: activeSlugs.filter((s) =>
        results.some((r) => r.slug === s && r.classification === "ALREADY_ACTIVE")
      ),
    },
    summary: {
      supplied: WAVE17_TARGETS.length,
      alreadyActive: results.filter((r) => r.classification === "ALREADY_ACTIVE").length,
      identityReviewRequired: results.filter((r) => r.classification === "IDENTITY_REVIEW_REQUIRED").length,
      scoringAtLeast85,
      buildEligible: buildEligible.length,
      batchACount: batchA.length,
      batchBCount: batchB.length,
    },
    brands: results,
    batchA: batchA.map((b) => ({
      slug: b.slug,
      name: b.exactBrandBasicsName,
      total: b.total,
      buildRisk: b.buildRisk,
      founderPriority: b.founderPriority,
    })),
    batchB: batchB.map((b) => ({
      slug: b.slug,
      name: b.exactBrandBasicsName,
      total: b.total,
      buildRisk: b.buildRisk,
      founderPriority: b.founderPriority,
    })),
    hyattishBasicsSnapshot: hyattish.map((h) => ({
      name: h.name,
      recordId: h.recordId,
      brandStatus: h.brandStatus,
      parentCompany: h.parentCompany,
    })),
  };

  // Artifacts
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.mkdirSync(DOCS, { recursive: true });
  fs.mkdirSync(PACKS, { recursive: true });

  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave17-hyatt-readiness.json"), JSON.stringify(report, null, 2));

  const sourceFoundation = {
    generatedAt: report.generatedAt,
    brands: results
      .filter((r) => r.eligibleForBuildPrep)
      .map((r) => ({
        slug: r.slug,
        name: r.exactBrandBasicsName,
        recordId: r.recordId,
        officialUrlProbes: r.officialUrlProbes,
        evidenceThemes: [
          "positioning",
          "service_model",
          "project_fit",
          "owner_proposition",
          "operating_implications",
          "loyalty_distribution",
          "portfolio_footprint",
          "property_examples",
          "recent_openings_candidates",
          "owner_considerations",
        ],
        notes: r.notes,
      })),
  };
  fs.writeFileSync(
    path.join(REPORTS, "brand-explorer-wave17-hyatt-source-foundation.json"),
    JSON.stringify(sourceFoundation, null, 2)
  );

  const propertyJson = {
    generatedAt: report.generatedAt,
    brands: Object.fromEntries(results.map((r) => [r.slug, r.propertyCandidates])),
  };
  fs.writeFileSync(
    path.join(REPORTS, "brand-explorer-wave17-hyatt-property-candidates.json"),
    JSON.stringify(propertyJson, null, 2)
  );

  const momentumJson = {
    generatedAt: report.generatedAt,
    note: "READ ONLY candidates — do not write Recent Momentum",
    brands: Object.fromEntries(results.map((r) => [r.slug, r.momentumCandidates])),
  };
  fs.writeFileSync(
    path.join(REPORTS, "brand-explorer-wave17-hyatt-momentum-candidates.json"),
    JSON.stringify(momentumJson, null, 2)
  );

  const fieldMatrix = {
    generatedAt: report.generatedAt,
    brands: Object.fromEntries(results.map((r) => [r.slug, r.fieldSupport])),
  };
  fs.writeFileSync(
    path.join(REPORTS, "brand-explorer-wave17-hyatt-field-support-matrix.json"),
    JSON.stringify(fieldMatrix, null, 2)
  );

  // Differentiation matrix MD
  const diffMd = [
    `# Wave 17 Hyatt — Semantic Differentiation Matrix`,
    ``,
    `## Soft-brand / collection comparison`,
    ``,
    `| Dimension | Destination by Hyatt | The Unbound Collection by Hyatt |`,
    `| --- | --- | --- |`,
    `| Brand character | Destination / resort-oriented affiliation set | Soft collection for distinctive independents |`,
    `| Property individuality | High — destination resorts & character assets | Very high — independent identity retained |`,
    `| Typical asset | Resort / destination lodge / unique place-based hotel | Urban or resort independent with strong narrative |`,
    `| Closest competitors | Soft collections with resort bias | Autograph / Tribute / Curio / MGallery / Handwritten |`,
    `| Owner control implication | Affiliation to Destination platform | Soft-brand flexibility under Unbound |`,
    `| Conversion relevance | Existing destination hotels seeking Hyatt distribution | Independents seeking World of Hyatt reach without hard-brand PIP |`,
    `| Must not collapse into | Unbound interchangeable copy | Destination resort-only copy |`,
    ``,
    `## Brand-by-brand differentiation`,
    ``,
  ];
  for (const r of results) {
    diffMd.push(`### ${r.suppliedName}`);
    diffMd.push(``);
    diffMd.push(`- Strategic role: **${r.strategicRole}**`);
    diffMd.push(`- Must remain distinct from: ${r.siblingWatch.join("; ")}`);
    diffMd.push(`- Semantic risk notes: ${(r.notes || []).join("; ") || "—"}`);
    diffMd.push(``);
  }
  diffMd.push(`## Lifestyle cluster: Thompson vs Dream vs Centric vs Caption`);
  diffMd.push(``);
  diffMd.push(`- **Hyatt Centric**: urban modern-explorer full-service lifestyle; exploration-forward; not nightlife-led.`);
  diffMd.push(`- **Thompson Hotels**: design-led lifestyle with elevated F&B/social; cultural urban positioning.`);
  diffMd.push(`- **Dream Hotels**: social/nightlife-leaning lifestyle identity; entertainment intensity higher than Thompson/Centric.`);
  diffMd.push(`- **Caption by Hyatt**: select-service lifestyle with social commons / mixed-use development logic; not Centric full-service and not Hyatt Place conventional select-service.`);
  diffMd.push(``);
  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave17-hyatt-differentiation-matrix.md"), diffMd.join("\n"));

  const seqMd = [
    `# Wave 17 Hyatt — Build Sequencing`,
    ``,
    `- Ready: \`${readyStatement}\``,
    `- Active universe: **${universeBefore.totalCount}** (unchanged: **${activeUnchanged}**)`,
    `- Scoring ≥85: **${scoringAtLeast85}**`,
    ``,
    `## Batch A (lowest risk / highest readiness)`,
    ``,
    ...batchA.map(
      (b) =>
        `- **${b.exactBrandBasicsName}** — ${b.total}/100 — risk=${b.buildRisk} — founderPriority=${b.founderPriority}`
    ),
    ``,
    `## Batch B`,
    ``,
    ...(batchB.length
      ? batchB.map(
          (b) =>
            `- **${b.exactBrandBasicsName}** — ${b.total}/100 — risk=${b.buildRisk} — founderPriority=${b.founderPriority}`
        )
      : ["- _(none)_"]),
    ``,
    `## Remediation / hold`,
    ``,
    ...results
      .filter((r) => !r.eligibleForBuildPrep)
      .map(
        (r) =>
          `- **${r.suppliedName}** — ${r.classification}${r.total != null ? ` (${r.total}/100)` : ""} — blockers: ${(r.primaryBlockers || []).join(", ") || "—"}`
      ),
    ``,
    `## Recommended next build action`,
    ``,
    batchA.length
      ? `- Start **Batch A** controlled tab-factory build (Presentation only), beginning with lowest-risk brand in Batch A.`
      : `- No build-eligible brands — remediate identity/source gaps first.`,
    `- Do not promote / release / write Momentum / touch Active ${universeBefore.totalCount}.`,
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave17-hyatt-build-sequencing.md"), seqMd);

  // Master MD table
  const masterMd = [
    `# Brand Explorer Wave 17 — Hyatt Readiness Audit`,
    ``,
    `> **Ready:** \`${readyStatement}\``,
    `> **Generated:** ${report.generatedAt}`,
    `> **Mode:** READ ONLY · Airtable writes: **false**`,
    ``,
    `## Preflight`,
    ``,
    `| Check | Result |`,
    `| --- | --- |`,
    `| Active universe before | **${universeBefore.totalCount}** |`,
    `| Active universe after | **${universeAfter.totalCount}** |`,
    `| Active set unchanged | **${activeUnchanged}** |`,
    `| Targets | 7 |`,
    `| Scoring ≥85 | **${scoringAtLeast85}** |`,
    `| Airtable writes | **0** |`,
    ``,
    `## Master table`,
    ``,
    `| Brand | Exact Basics Name | Record ID | Slug | Status | ID/20 | Src/20 | Prop/15 | Mom/15 | Copy/15 | Tab/10 | Sem/5 | Total | Classification | Build Risk | Founder Pri | Batch | Blockers | Notes |`,
    `| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | --- | --- | --- |`,
  ];
  for (const r of results) {
    const s = r.scores || {};
    masterMd.push(
      `| ${r.suppliedName} | ${r.exactBrandBasicsName || "—"} | \`${r.recordId || "—"}\` | \`${r.slug}\` | ${r.brandStatus || "—"} | ${s.identity ?? "—"} | ${s.officialSupport ?? "—"} | ${s.propertyExamples ?? "—"} | ${s.momentumReadiness ?? "—"} | ${s.publicCopy ?? "—"} | ${s.tabReadiness ?? "—"} | ${s.semantic ?? "—"} | ${r.total ?? "—"} | ${r.classification} | ${r.buildRisk} | ${r.founderPriority} | ${r.recommendedBatch || "—"} | ${(r.primaryBlockers || []).join("; ") || "—"} | ${(r.notes || []).join("; ") || "—"} |`
    );
  }
  masterMd.push(
    ``,
    `## Batch A`,
    ``,
    ...batchA.map((b) => `- ${b.exactBrandBasicsName} (${b.total}/100, ${b.buildRisk})`),
    ``,
    `## Batch B`,
    ``,
    ...(batchB.length ? batchB.map((b) => `- ${b.exactBrandBasicsName} (${b.total}/100, ${b.buildRisk})`) : ["- _(none)_"]),
    ``,
    `## Safety`,
    ``,
    `- Brand Status / release / CV / Brand Verified / Census / Momentum / Presentation / image writes: **0**`,
    `- Active universe unchanged: **${activeUnchanged}**`,
    ``
  );
  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave17-hyatt-readiness.md"), masterMd.join("\n"));

  const docsMd = [
    `# Brand Explorer Wave 17 — Hyatt Foundation`,
    ``,
    `- Ready: \`${readyStatement}\``,
    `- Live Active universe: **${universeBefore.totalCount}** (unchanged)`,
    `- Cohort: Hyatt Centric · Hyatt Regency · Destination by Hyatt · The Unbound Collection by Hyatt · Thompson Hotels · Dream Hotels · Caption by Hyatt`,
    `- Scoring ≥85: **${scoringAtLeast85}**`,
    `- Batch A: ${batchA.map((b) => b.exactBrandBasicsName).join(", ") || "(none)"}`,
    `- Batch B: ${batchB.map((b) => b.exactBrandBasicsName).join(", ") || "(none)"}`,
    ``,
    `See reports/brand-explorer-wave17-hyatt-*.md|json for full artifacts.`,
    ``,
    `## Guardrails`,
    ``,
    `- No Brand Status / release / CV / Momentum / Presentation / image writes in this stage`,
    `- Soft-brand pair Destination vs Unbound must stay differentiated`,
    `- Lifestyle cluster Thompson / Dream / Centric / Caption must stay differentiated`,
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(DOCS, "brand-explorer-wave17-hyatt-foundation.md"), docsMd);

  // Per-brand source packs for >=85
  for (const r of results.filter((x) => x.eligibleForBuildPrep)) {
    const pack = {
      slug: r.slug,
      name: r.exactBrandBasicsName,
      recordId: r.recordId,
      total: r.total,
      classification: r.classification,
      buildRisk: r.buildRisk,
      officialUrlProbes: r.officialUrlProbes,
      siblingWatch: r.siblingWatch,
      propertyCandidates: r.propertyCandidates,
      momentumCandidates: r.momentumCandidates,
      fieldSupport: r.fieldSupport,
      notes: r.notes,
    };
    fs.writeFileSync(
      path.join(PACKS, `${r.slug}.json`),
      JSON.stringify(pack, null, 2)
    );
  }

  console.log(JSON.stringify({
    readyStatement,
    activeBefore: universeBefore.totalCount,
    activeAfter: universeAfter.totalCount,
    activeUnchanged,
    scoringAtLeast85,
    batchA: batchA.map((b) => b.slug),
    batchB: batchB.map((b) => b.slug),
    classifications: Object.fromEntries(results.map((r) => [r.slug, { total: r.total, classification: r.classification, status: r.brandStatus }])),
  }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
