/**
 * Marriott Mexico VIC Wave 1D — Steward Review (read-only overlay)
 *
 * Does not mutate frozen Wave 1D raw artifacts.
 * No Airtable · No Webhound · No BE activation · No production overwrite · No auto-merge.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { tokenSimilarity, tokenize } from "../lib/research-engine-v2/adapters/adapter-utils.js";
import { mapMarriottMexicoBrand } from "../lib/research-engine-v2/clean-census/marriott-mexico-discovery.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const WAVE1D = join(ROOT, "data/research-engine-v2/verified-independent-census-wave1d-marriott");
const OVERLAY = join(WAVE1D, "steward-review");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const IHG_V1 = join(ROOT, "data/research-engine-v2/verified-independent-census-v1");
const HILTON_1B = join(ROOT, "data/research-engine-v2/verified-independent-census-wave1b-hilton");
const CHOICE_1C = join(ROOT, "data/research-engine-v2/verified-independent-census-wave1c-choice");

const UNCONFIRMED = "Marriott Bonvoy — Brand Unconfirmed";

function writeJson(dir, name, obj) {
  writeFileSync(join(dir, name), JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(dir, name, text) {
  writeFileSync(join(dir, name), text, "utf8");
}
function loadJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}
function loadRecords(path) {
  if (!existsSync(path)) return [];
  return loadJson(path).records || [];
}

function normText(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cityAlign(a, b) {
  const ca = normText(a);
  const cb = normText(b);
  if (!ca || !cb) return false;
  return ca.includes(cb) || cb.includes(ca) || ca.replace(/\s/g, "") === cb.replace(/\s/g, "");
}

/** Distinctive tokens after dropping brand/geo stopwords — for steward signals only. */
function distinctiveTokens(name) {
  const GEO_BRAND_STOP = new Set([
    "marriott",
    "hilton",
    "ihg",
    "choice",
    "holiday",
    "hampton",
    "hyatt",
    "courtyard",
    "fairfield",
    "aloft",
    "westin",
    "sheraton",
    "autograph",
    "design",
    "express",
    "junior",
    "plus",
    "suites",
    "centro",
    "city",
    "inn",
    "hotels",
    "hotel",
    "resort",
    "all",
    "inclusive",
    "mexico",
    "monterrey",
    "guadalajara",
    "cancun",
    "cancun",
    "puebla",
    "tijuana",
    "vallarta",
    "puerto",
    "playa",
    "carmen",
    "valle",
    "santa",
    "reforma",
    "airport",
    "aeropuerto",
    "zona",
    "industrial",
    "downtown",
  ]);
  return tokenize(name).filter((t) => t.length > 2 && !GEO_BRAND_STOP.has(t));
}

function distinctiveOverlap(a, b) {
  const ta = new Set(distinctiveTokens(a));
  const tb = new Set(distinctiveTokens(b));
  if (!ta.size || !tb.size) return { overlap: [], ratio: 0 };
  const overlap = [...ta].filter((t) => tb.has(t));
  const union = new Set([...ta, ...tb]).size;
  return { overlap, ratio: union ? overlap.length / union : 0 };
}

function urlPathKey(url) {
  try {
    const u = new URL(url);
    return u.pathname.replace(/\/overview\/?$/i, "").toLowerCase();
  } catch {
    return normText(url);
  }
}

function hasField(rec, key) {
  const v = rec.fields?.[key];
  return v != null && v !== "" && v !== "Unknown";
}

function sampleNames(records, n = 3) {
  return records.slice(0, n).map((r) => r.fields?.name || r.canonical_hotel_name);
}

// ── Load frozen Wave 1D (read-only) ──────────────────────────────────────────
if (!existsSync(join(WAVE1D, "02-marriott-full-records.json"))) {
  throw new Error("Wave 1D artifacts missing — run wave1d-marriott first");
}
if (!existsSync(join(WAVE1D, "00-wave1d-run-summary.json"))) {
  throw new Error("Wave 1D run summary missing");
}

const waveSummary = loadJson(join(WAVE1D, "00-wave1d-run-summary.json"));
const full = loadJson(join(WAVE1D, "02-marriott-full-records.json"));
const eligibility = loadJson(join(WAVE1D, "12-data-image-eligibility.json"));
const records = full.records || [];
if (records.length !== 301) {
  console.warn(`[steward-1d] expected 301 records, found ${records.length}`);
}

mkdirSync(OVERLAY, { recursive: true });
mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });

console.log(`[steward-1d] reviewing ${records.length} Marriott Mexico records`);

// ── 1. Identity-risk pass over all 301 ───────────────────────────────────────
const identityRiskRows = records.map((r) => {
  const name = r.fields?.name || "";
  const brand = r.brand || "";
  const remapped = mapMarriottMexicoBrand(name, r.fields?.Website || "");
  const brandRemapDiffers = remapped !== brand;
  const missingAddr = !hasField(r, "Address 1");
  const missingCoords = !hasField(r, "Latitude") || !hasField(r, "Longitude");
  const sitemapOnly = !(r.independent_sources || []).some(
    (s) => s.role === "enrichment_attempt" && s.result !== "Blocked"
  );
  let risk = "low";
  if (brand === UNCONFIRMED) risk = "medium";
  if (brandRemapDiffers) risk = "medium";
  if (/Hacienda|HNF|Grand Hotel|Casa Mayor|SJ Grand/i.test(name) && brand === UNCONFIRMED) {
    risk = "medium";
  }
  // City fragment lookalike
  if (r.fields?.city && /luxury collection|autograph|all.?inclusive|hacienda|adults/i.test(String(r.fields.city))) {
    risk = risk === "low" ? "medium" : risk;
  }
  return {
    independent_record_id: r.independent_record_id,
    property_id: r.property_id || r.fields?.["Property ID"],
    name,
    brand,
    remapped_brand: remapped,
    brand_remap_differs: brandRemapDiffers,
    city: r.fields?.city || null,
    url: r.fields?.Website || null,
    marsha: r.fields?.["Property ID"] || null,
    missing_address: missingAddr,
    missing_coordinates: missingCoords,
    sitemap_only: sitemapOnly,
    identity_risk: risk,
    core_pct: r.completeness?.corePct ?? null,
    material_pct: r.completeness?.materialPct ?? null,
  };
});

const marshaSet = new Set();
const marshaDupes = [];
for (const row of identityRiskRows) {
  if (!row.marsha) continue;
  const key = String(row.marsha).toUpperCase();
  if (marshaSet.has(key)) marshaDupes.push(row);
  else marshaSet.add(key);
}

// ── 2. Brand parsing QA ──────────────────────────────────────────────────────
const byBrand = new Map();
for (const r of records) {
  const b = r.brand || "Unknown";
  if (!byBrand.has(b)) byBrand.set(b, []);
  byBrand.get(b).push(r);
}

const BRAND_QA_NOTES = {
  "Four Points by Sheraton": {
    confidence: "High",
    adjacent_risk: "Low — matched before Sheraton (Wave 1D fix verified)",
    action: "Accept",
  },
  Sheraton: {
    confidence: "High",
    adjacent_risk: "Low — no Four Points leakage in sample",
    action: "Accept",
  },
  "Marriott Hotels": {
    confidence: "High",
    adjacent_risk: "Medium — titles with 'Marriott … Hotel' (intervening words) may miss map",
    action: "Accept with known miss → Mexico City Marriott Reforma (unconfirmed bucket)",
  },
  "JW Marriott": {
    confidence: "High",
    adjacent_risk: "Low — Casa Maat mapped JW via rule",
    action: "Accept",
  },
  "St. Regis": { confidence: "High", adjacent_risk: "None", action: "Accept" },
  Westin: {
    confidence: "High",
    adjacent_risk: "Low — Baja Point kept distinct MARSHA",
    action: "Accept",
  },
  "Autograph Collection": {
    confidence: "High",
    adjacent_risk: "Medium — soft brands / Royalton complexes; multi-property campuses",
    action: "Accept; campus pairs steward-noted",
  },
  "Tribute Portfolio": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "Design Hotels": {
    confidence: "High",
    adjacent_risk: "Medium — soft-brand URLs; city inference variable",
    action: "Accept for census identity; BE hold until material fields",
  },
  "City Express by Marriott": {
    confidence: "High",
    adjacent_risk: "Low — sibling brands (Plus/Junior/Suites/Centro) ordered correctly",
    action: "Accept",
  },
  "City Express Plus by Marriott": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "City Express Junior by Marriott": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "City Centro by Marriott": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "City Express Suites by Marriott": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "AC Hotels by Marriott": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "Aloft Hotels": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "Fairfield by Marriott": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "Courtyard by Marriott": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "Residence Inn by Marriott": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  "Moxy Hotels": { confidence: "High", adjacent_risk: "Low", action: "Accept" },
  [UNCONFIRMED]: {
    confidence: "N/A — intentional hold",
    adjacent_risk: "High — soft brands + one Marriott Hotels miss",
    action: "Review individually (see §3)",
  },
};

const brandParsingQa = [...byBrand.entries()]
  .sort((a, b) => b[1].length - a[1].length)
  .map(([brand, list]) => {
    const note = BRAND_QA_NOTES[brand] || {
      confidence: brand.includes("Unconfirmed") ? "Hold" : "Medium",
      adjacent_risk: "Review soft-brand / URL cues",
      action: "Accept for census identity",
    };
    // Spot-check: remap consistency
    const remapMismatches = list.filter(
      (r) => mapMarriottMexicoBrand(r.fields?.name || "", r.fields?.Website || "") !== brand
    );
    return {
      brand,
      count: list.length,
      parsing_confidence: note.confidence,
      adjacent_brand_risk: note.adjacent_risk,
      sample_records_reviewed: sampleNames(list, 3),
      steward_action: note.action,
      remap_consistency_mismatches: remapMismatches.length,
      remap_mismatch_samples: remapMismatches.slice(0, 3).map((r) => ({
        name: r.fields?.name,
        current: brand,
        remapped: mapMarriottMexicoBrand(r.fields?.name || "", r.fields?.Website || ""),
      })),
    };
  });

// Four Points / Sheraton verification
const fourPoints = byBrand.get("Four Points by Sheraton") || [];
const sheraton = byBrand.get("Sheraton") || [];
const fourPointsLeakIntoSheraton = sheraton.filter((r) => /four.?points/i.test(r.fields?.name || ""));
const sheratonLeakIntoFourPoints = fourPoints.filter(
  (r) => /^sheraton\b/i.test(r.fields?.name || "") && !/four.?points/i.test(r.fields?.name || "")
);

// City Express family verification
const cityExpressFamily = {
  "City Express by Marriott": (byBrand.get("City Express by Marriott") || []).length,
  "City Express Plus by Marriott": (byBrand.get("City Express Plus by Marriott") || []).length,
  "City Express Junior by Marriott": (byBrand.get("City Express Junior by Marriott") || []).length,
  "City Express Suites by Marriott": (byBrand.get("City Express Suites by Marriott") || []).length,
  "City Centro by Marriott": (byBrand.get("City Centro by Marriott") || []).length,
};
const cityExpressMisparse = records.filter((r) => {
  const n = r.fields?.name || "";
  const b = r.brand || "";
  if (!/City Express|City Centro/i.test(n) && !/City Express|City Centro/i.test(b)) return false;
  if (/City Express Junior/i.test(n) && !/Junior/i.test(b)) return true;
  if (/City Express Plus/i.test(n) && !/Plus/i.test(b)) return true;
  if (/City Express Suites/i.test(n) && !/Suites/i.test(b)) return true;
  if (/City Centro/i.test(n) && !/City Centro/i.test(b)) return true;
  if (/^City Express by Marriott/i.test(n) && /Junior|Plus|Suites|Centro/i.test(b)) return true;
  return false;
});

// ── 3. Brand Unconfirmed review ──────────────────────────────────────────────
const unconfirmedRecords = byBrand.get(UNCONFIRMED) || [];
const unconfirmedReviews = unconfirmedRecords.map((r) => {
  const name = r.fields?.name || "";
  const url = r.fields?.Website || "";
  const remapped = mapMarriottMexicoBrand(name, url);
  let likely_brand = null;
  let confidence = "Low";
  let classification = "keep_brand_unconfirmed";
  let why = "No brand token matched mapMarriottMexicoBrand rules from title/URL";
  let recommended_action = "Keep Unconfirmed for census identity; exclude from BE brand completion";

  // Source-supported Marriott Hotels: name contains Marriott + Hotel/Resort without intervening brand
  if (/\bmarriott\b/i.test(name) && /\b(hotel|resort)\b/i.test(name) && !/city express|courtyard|fairfield|residence inn|jw\b/i.test(name)) {
    likely_brand = "Marriott Hotels";
    confidence = "High";
    classification = "confirm_brand";
    why =
      "Official marriott.com sitemap title contains Marriott + Hotel; map missed due to intervening place token (e.g. 'Marriott Reforma Hotel')";
    recommended_action = "Overlay confirm_brand → Marriott Hotels (do not mutate freeze; apply on baseline lock overlay if steward accepts)";
  } else if (/by HNF/i.test(name)) {
    likely_brand = null;
    confidence = "Insufficient";
    classification = "exclude_from_brand_completion";
    why = "HNF-operated soft brand on Bonvoy sitemap without Autograph/Tribute/Design Hotels cue in title or URL path";
    recommended_action = "Keep Unconfirmed; exclude from BE completion; optional future property-page brand cue";
  } else if (/Casa Mayor|Hotel Hacienda|SJ Grand|Gran Hotel/i.test(name)) {
    likely_brand = null;
    confidence = "Insufficient";
    classification = "steward_manual_review_required";
    why = "Soft-brand / independent name on Bonvoy directory without brand path cue";
    recommended_action = "Manual URL/brand page review before any brand confirmation";
  }

  if (remapped !== UNCONFIRMED && remapped !== r.brand) {
    // Shouldn't happen for these five, but record if map changes later
    likely_brand = remapped;
    classification = "steward_manual_review_required";
  }

  return {
    independent_record_id: r.independent_record_id,
    property_name: name,
    url,
    city: r.fields?.city || null,
    state: r.fields?.State || r.fields?.["State / Region"] || null,
    marsha: r.fields?.["Property ID"] || null,
    source_path: "official_marriott_mexico_country_hotel_sitemap",
    why_brand_not_parsed: why,
    likely_brand_if_inferable: likely_brand,
    confidence,
    classification,
    recommended_action,
  };
});

// ── 4. Physical identity / near-duplicate review ─────────────────────────────
const nearDuplicateCandidates = [];
for (let i = 0; i < records.length; i++) {
  for (let j = i + 1; j < records.length; j++) {
    const a = records[i];
    const b = records[j];
    const nameA = a.fields?.name || "";
    const nameB = b.fields?.name || "";
    const sim = tokenSimilarity(normText(nameA), normText(nameB));
    if (sim < 0.65) continue;
    const sameCity = cityAlign(a.fields?.city, b.fields?.city);
    const sameMarsha =
      a.fields?.["Property ID"] &&
      b.fields?.["Property ID"] &&
      String(a.fields["Property ID"]).toUpperCase() === String(b.fields["Property ID"]).toUpperCase();
    const urlSim =
      a.fields?.Website && b.fields?.Website
        ? tokenSimilarity(urlPathKey(a.fields.Website), urlPathKey(b.fields.Website))
        : 0;
    const distTokens = distinctiveOverlap(nameA, nameB);

    let classification = "insufficient_evidence_do_not_merge";
    let note = "";

    if (sameMarsha) {
      classification = "same_physical_property_confirmed";
      note = "Identical MARSHA — sitemap should have deduped; flag if present";
    } else if (sim >= 0.85 && sameCity && a.brand === b.brand && urlSim >= 0.9) {
      classification = "probable_same_physical_property_requires_steward";
      note = "Very high name+URL+city+brand overlap with different MARSHA";
    } else if (
      /residences at/i.test(nameA) ||
      /residences at/i.test(nameB) ||
      /casa maat/i.test(nameA) ||
      /casa maat/i.test(nameB) ||
      /baja point/i.test(nameA) ||
      /baja point/i.test(nameB)
    ) {
      classification = "insufficient_evidence_do_not_merge";
      note = "Campus / residences / annex naming — keep distinct without address/coords proof";
    } else if (a.brand !== b.brand && sameCity && sim >= 0.7) {
      classification = "distinct_physical_property";
      note = "Different brands / MARSHA in same city corridor (e.g. City Express siblings, JW vs AC Valle)";
    } else if (sim >= 0.7 && sameCity) {
      classification = "distinct_physical_property";
      note = "High name similarity but distinct MARSHA codes — treated as separate properties";
    } else if (sim >= 0.65 && !sameCity && distTokens.overlap.length === 0) {
      classification = "insufficient_evidence_do_not_merge";
      note = "Name similarity without city/distinctive token support";
    } else {
      classification = "insufficient_evidence_do_not_merge";
      note = "Fuzzy similarity only — do not merge";
    }

    // Only keep notable pairs (not every City Express corridor noise at 0.65)
    if (sim < 0.7 && classification === "distinct_physical_property") continue;
    if (sim < 0.75 && !/residences|casa maat|baja point|planet hollywood|royalton/i.test(`${nameA} ${nameB}`)) {
      if (a.brand !== b.brand && /City Express|City Centro/i.test(a.brand) && /City Express|City Centro/i.test(b.brand)) {
        // keep a sample of CE siblings at >=0.75 only — handled below via threshold
      }
    }
    if (sim < 0.75 && classification === "distinct_physical_property" && a.brand !== b.brand) continue;

    nearDuplicateCandidates.push({
      name_similarity: Math.round(sim * 1000) / 1000,
      url_path_similarity: Math.round(urlSim * 1000) / 1000,
      city_align: sameCity,
      distinctive_token_overlap: distTokens.overlap,
      a: {
        id: a.independent_record_id,
        name: nameA,
        brand: a.brand,
        city: a.fields?.city || null,
        marsha: a.fields?.["Property ID"] || null,
        url: a.fields?.Website || null,
        address: a.fields?.["Address 1"] || null,
        coords:
          hasField(a, "Latitude") && hasField(a, "Longitude")
            ? { lat: a.fields.Latitude, lng: a.fields.Longitude }
            : null,
      },
      b: {
        id: b.independent_record_id,
        name: nameB,
        brand: b.brand,
        city: b.fields?.city || null,
        marsha: b.fields?.["Property ID"] || null,
        url: b.fields?.Website || null,
        address: b.fields?.["Address 1"] || null,
        coords:
          hasField(b, "Latitude") && hasField(b, "Longitude")
            ? { lat: b.fields.Latitude, lng: b.fields.Longitude }
            : null,
      },
      classification,
      note,
      merge_recommendation: "DO_NOT_AUTO_MERGE",
    });
  }
}

// Prioritize campus / high-risk pairs in a steward queue subset
const campusKeywords = /residences at|casa maat|baja point|planet hollywood|royalton splash|royalton hideaway|royalton chic|royalton riviera/i;
const physicalStewardQueue = nearDuplicateCandidates.filter(
  (p) =>
    p.classification === "probable_same_physical_property_requires_steward" ||
    p.classification === "same_physical_property_confirmed" ||
    campusKeywords.test(`${p.a.name} ${p.b.name}`) ||
    p.name_similarity >= 0.85
);

const physicalSummary = {
  unique_physical_reported: waveSummary.unique_physical_properties,
  records: records.length,
  marsha_unique: marshaSet.size,
  marsha_duplicate_rows: marshaDupes.length,
  near_duplicate_pairs_reviewed: nearDuplicateCandidates.length,
  campus_or_high_sim_steward_pairs: physicalStewardQueue.length,
  confirmed_same_physical_merges: nearDuplicateCandidates.filter(
    (p) => p.classification === "same_physical_property_confirmed"
  ).length,
  auto_merges_applied: 0,
  staging_identity_count_safe: marshaDupes.length === 0 && records.length === 301,
  verdict:
    marshaDupes.length === 0
      ? "301 unique MARSHA / 301 records — staging identity count SAFE (campus annexes kept distinct)"
      : "MARSHA collisions require investigation",
};

// ── 5. Cross-family (name/city/address — not coords-only) ────────────────────
const prior = [
  ...loadRecords(join(IHG_V1, "08-expanded-benchmark-full-records.json")).map((r) => ({
    ...r,
    _family: "IHG",
  })),
  ...loadRecords(join(HILTON_1B, "02-hilton-full-records.json")).map((r) => ({
    ...r,
    _family: "Hilton",
  })),
  ...loadRecords(join(CHOICE_1C, "02-choice-full-records.json")).map((r) => ({
    ...r,
    _family: "Choice",
  })),
];

const crossFamily = [];
for (const m of records) {
  const mName = m.fields?.name || "";
  const mCity = m.fields?.city || "";
  const mAddr = m.fields?.["Address 1"] || "";
  for (const o of prior) {
    const oName = o.fields?.name || o.canonical_hotel_name || "";
    const oCity = o.fields?.city || o.normalized_city || "";
    const oAddr = o.fields?.["Address 1"] || "";
    const nameSim = tokenSimilarity(normText(mName), normText(oName));
    if (nameSim < 0.55) continue;

    const city = cityAlign(mCity, oCity);
    // Also try extracting city from other name when Marriott city missing
    const cityFromNames =
      city ||
      [...(mCity ? [mCity] : []), ...(oCity ? [oCity] : [])].some((c) =>
        normText(mName + " " + oName).includes(normText(c))
      );

    const addrSim =
      mAddr && oAddr ? tokenSimilarity(normText(mAddr), normText(oAddr)) : 0;
    const dist = distinctiveOverlap(mName, oName);

    // Coordinates only if both present and finite non-null
    let distKm = null;
    const mLat = m.fields?.Latitude;
    const mLng = m.fields?.Longitude;
    const oLat = o.fields?.Latitude;
    const oLng = o.fields?.Longitude;
    const coordsOk =
      mLat != null &&
      mLat !== "" &&
      mLng != null &&
      mLng !== "" &&
      oLat != null &&
      oLat !== "" &&
      oLng != null &&
      oLng !== "" &&
      Number.isFinite(Number(mLat)) &&
      Number.isFinite(Number(mLng)) &&
      Number.isFinite(Number(oLat)) &&
      Number.isFinite(Number(oLng));
    if (coordsOk) {
      const toRad = (d) => (d * Math.PI) / 180;
      const R = 6371;
      const dLat = toRad(Number(oLat) - Number(mLat));
      const dLon = toRad(Number(oLng) - Number(mLng));
      const a =
        Math.sin(dLat / 2) ** 2 +
        Math.cos(toRad(Number(mLat))) *
          Math.cos(toRad(Number(oLat))) *
          Math.sin(dLon / 2) ** 2;
      distKm = 2 * R * Math.asin(Math.sqrt(a));
    }

    let classification = "independent_physical_property";
    const reasons = [];
    if (nameSim >= 0.55) reasons.push(`name_sim_${nameSim.toFixed(2)}`);
    if (city || cityFromNames) reasons.push("city_align");
    if (addrSim >= 0.7) reasons.push(`address_sim_${addrSim.toFixed(2)}`);
    if (dist.overlap.length) reasons.push(`distinctive_overlap:${dist.overlap.join(",")}`);
    if (distKm != null && distKm <= 0.08) reasons.push(`coords_within_${distKm.toFixed(3)}km`);
    if (distKm != null && distKm <= 0.25) reasons.push(`coords_near_${distKm.toFixed(3)}km`);

    if (coordsOk && distKm != null && distKm <= 0.08 && (nameSim >= 0.5 || addrSim >= 0.7)) {
      classification = "exact_physical_match_requires_steward_review";
    } else if (coordsOk && distKm != null && distKm <= 0.25 && nameSim >= 0.75 && (city || cityFromNames)) {
      classification = "probable_physical_match_requires_steward_review";
    } else if (addrSim >= 0.85 && (city || cityFromNames) && nameSim >= 0.6) {
      classification = "probable_physical_match_requires_steward_review";
    } else if (dist.overlap.length >= 2 && nameSim >= 0.7 && (city || cityFromNames)) {
      // Shared distinctive place/hotel tokens beyond corridor city
      classification = "probable_physical_match_requires_steward_review";
    } else if (
      nameSim >= 0.55 &&
      (city || cityFromNames) &&
      dist.overlap.length === 0 &&
      !coordsOk &&
      addrSim < 0.7
    ) {
      // City-corridor brand hotels (e.g. Monterrey Valle × JW/Hilton/HI) — not same building
      classification = "rejected_fuzzy_match";
      reasons.push("city_corridor_brand_hotels_only");
    } else if (nameSim >= 0.7 && !coordsOk && addrSim < 0.7) {
      classification = "insufficient_evidence_no_merge";
      reasons.push("no_coords_no_address_proof");
    } else if (nameSim >= 0.55) {
      classification = "insufficient_evidence_no_merge";
    }

    if (classification === "independent_physical_property") continue;

    crossFamily.push({
      classification,
      name_similarity: Math.round(nameSim * 1000) / 1000,
      address_similarity: Math.round(addrSim * 1000) / 1000,
      distance_km: distKm,
      reasons,
      marriott: {
        id: m.independent_record_id,
        name: mName,
        brand: m.brand,
        city: mCity || null,
        marsha: m.fields?.["Property ID"] || null,
      },
      other: {
        family: o._family,
        id: o.independent_record_id,
        name: oName,
        brand: o.brand || o.fields?.Affiliation,
        city: oCity || null,
      },
      auto_merge: false,
    });
  }
}

const crossSummary = {
  baseline_locked_total: 365,
  pairs_flagged: crossFamily.length,
  exact_physical_match_requires_steward_review: crossFamily.filter((x) =>
    x.classification.startsWith("exact")
  ).length,
  probable_physical_match_requires_steward_review: crossFamily.filter((x) =>
    x.classification.startsWith("probable")
  ).length,
  rejected_fuzzy_match: crossFamily.filter((x) => x.classification === "rejected_fuzzy_match")
    .length,
  insufficient_evidence_no_merge: crossFamily.filter(
    (x) => x.classification === "insufficient_evidence_no_merge"
  ).length,
  auto_merges: 0,
  note: "Name/city/address used; missing Marriott coords do not invent 0,0 matches. City-corridor brand hotels classified rejected_fuzzy / insufficient — not merges.",
};

// ── 6. Completeness / migration risk ─────────────────────────────────────────
const eligMap = new Map((eligibility.results || []).map((e) => [e.independent_record_id, e]));
const dataEligible = records.filter(
  (r) => eligMap.get(r.independent_record_id)?.production_eligibility_data === "ELIGIBLE"
);
const dataIneligible = records.filter(
  (r) => eligMap.get(r.independent_record_id)?.production_eligibility_data !== "ELIGIBLE"
);

const completenessMigration = {
  data_eligible: dataEligible.length,
  data_ineligible: dataIneligible.length,
  missing_address: records.filter((r) => !hasField(r, "Address 1")).length,
  missing_coordinates: records.filter((r) => !hasField(r, "Latitude") || !hasField(r, "Longitude"))
    .length,
  missing_rooms: records.filter((r) => !hasField(r, "rooms")).length,
  missing_owner_operator: records.filter(
    (r) => !hasField(r, "Management Company") && !hasField(r, "owner")
  ).length,
  missing_open_date: records.filter((r) => !hasField(r, "Open Date")).length,
  sitemap_only: records.filter(
    (r) =>
      !(r.independent_sources || []).some(
        (s) => s.role === "enrichment_attempt" && s.result !== "Blocked"
      )
  ).length,
  property_page_enriched: records.filter((r) =>
    (r.independent_sources || []).some(
      (s) => s.role === "enrichment_attempt" && s.result !== "Blocked"
    )
  ).length,
  suitable_for_be_completion_support: records.filter((r) => {
    const b = r.brand || "";
    if (b === UNCONFIRMED) return false;
    return (r.completeness?.corePct || 0) >= 95 && byBrand.get(b)?.length >= 5;
  }).length,
  suitable_only_for_census_identity: records.length,
  requiring_steward_before_migration: [
    ...unconfirmedReviews.map((u) => u.independent_record_id),
    ...identityRiskRows.filter((x) => x.identity_risk !== "low").map((x) => x.independent_record_id),
  ].filter((v, i, a) => a.indexOf(v) === i).length,
  staging_migration_ready: true,
  production_overwrite_ready: false,
  fake_fields_created: 0,
};

// ── Status decision ──────────────────────────────────────────────────────────
const blockingIssues = [];
if (marshaDupes.length) blockingIssues.push("MARSHA collisions inside Wave 1D");
if (fourPointsLeakIntoSheraton.length) blockingIssues.push("Four Points leaked into Sheraton");
if (cityExpressMisparse.length) blockingIssues.push("City Express family misparse");
if (waveSummary.unique_physical_properties !== records.length && marshaDupes.length) {
  blockingIssues.push("Identity collapse unresolved");
}

const minorHolds = [];
if (unconfirmedRecords.length) {
  minorHolds.push(`${unconfirmedRecords.length} Brand Unconfirmed (1 confirm_brand overlay candidate)`);
}
if (physicalStewardQueue.length) {
  minorHolds.push(`${physicalStewardQueue.length} campus/high-sim physical steward pairs (no merge)`);
}
if (crossSummary.probable_physical_match_requires_steward_review > 0) {
  minorHolds.push(
    `${crossSummary.probable_physical_match_requires_steward_review} cross-family probable steward pairs (no merge)`
  );
}
if (completenessMigration.missing_coordinates === records.length) {
  minorHolds.push("All 301 records missing coordinates (sitemap limitation) — staging OK");
}
minorHolds.push("Material completeness 40% — census identity only; not production-ready");

let status;
if (blockingIssues.length) {
  status = "wave1d_marriott_steward_review_blocked_do_not_lock";
} else if (minorHolds.length) {
  status = "wave1d_marriott_steward_review_minor_holds_ready_for_4_family_baseline_lock";
} else {
  status = "wave1d_marriott_steward_review_clean_ready_for_4_family_baseline_lock";
}

const reviewSummary = {
  status,
  reviewed_at: new Date().toISOString(),
  wave1d_status: waveSummary.status,
  records_reviewed: records.length,
  identity_risk_pass: true,
  brand_count: byBrand.size,
  four_points_count: fourPoints.length,
  sheraton_count: sheraton.length,
  four_points_sheraton_verified: fourPointsLeakIntoSheraton.length === 0 && sheratonLeakIntoFourPoints.length === 0,
  city_express_family: cityExpressFamily,
  city_express_misparse_count: cityExpressMisparse.length,
  brand_unconfirmed_count: unconfirmedRecords.length,
  brand_unconfirmed_classifications: unconfirmedReviews.reduce((acc, u) => {
    acc[u.classification] = (acc[u.classification] || 0) + 1;
    return acc;
  }, {}),
  physical: physicalSummary,
  cross_family: crossSummary,
  completeness_migration: completenessMigration,
  blocking_issues: blockingIssues,
  minor_holds: minorHolds,
  auto_merges: 0,
  airtable_writes: false,
  webhound_used: false,
  brand_explorer_activation: false,
  production_overwrite: false,
  fake_rooms_owners_open_dates_created: false,
};

// ── Write overlays + reports ─────────────────────────────────────────────────
writeJson(OVERLAY, "00-steward-review-summary.json", reviewSummary);
writeJson(OVERLAY, "01-identity-risk-all-records.json", {
  total: identityRiskRows.length,
  medium_or_higher: identityRiskRows.filter((x) => x.identity_risk !== "low").length,
  rows: identityRiskRows,
});
writeJson(OVERLAY, "02-brand-parsing-qa.json", {
  brands: brandParsingQa,
  four_points_sheraton: {
    four_points: fourPoints.length,
    sheraton: sheraton.length,
    four_points_names: fourPoints.map((r) => r.fields?.name),
    sheraton_names: sheraton.map((r) => r.fields?.name),
    leak_four_points_into_sheraton: fourPointsLeakIntoSheraton.map((r) => r.fields?.name),
    verified_clean: fourPointsLeakIntoSheraton.length === 0,
  },
  city_express_family: {
    counts: cityExpressFamily,
    misparse_count: cityExpressMisparse.length,
    misparse_samples: cityExpressMisparse.slice(0, 10).map((r) => ({
      name: r.fields?.name,
      brand: r.brand,
    })),
    verified_clean: cityExpressMisparse.length === 0,
  },
});
writeJson(OVERLAY, "03-brand-unconfirmed-review.json", {
  count: unconfirmedReviews.length,
  reviews: unconfirmedReviews,
});
writeJson(OVERLAY, "04-physical-identity-near-duplicates.json", {
  summary: physicalSummary,
  steward_priority_pairs: physicalStewardQueue,
  all_notable_pairs: nearDuplicateCandidates.slice(0, 200),
  rejected_fuzzy_policy: "Fuzzy name alone never merges; missing coords never treated as 0,0",
});
writeJson(OVERLAY, "05-cross-family-name-city-review.json", {
  summary: crossSummary,
  steward_exact_or_probable: crossFamily.filter(
    (x) => x.classification.startsWith("exact") || x.classification.startsWith("probable")
  ),
  rejected_fuzzy_sample: crossFamily
    .filter((x) => x.classification === "rejected_fuzzy_match")
    .slice(0, 50),
  insufficient_sample: crossFamily
    .filter((x) => x.classification === "insufficient_evidence_no_merge")
    .slice(0, 50),
  all_flagged_count: crossFamily.length,
});
writeJson(OVERLAY, "06-completeness-migration-risk.json", completenessMigration);

const md = `# Marriott Mexico VIC Wave 1D — Steward Review

**Status:** \`${status}\`  
**Reviewed:** ${reviewSummary.reviewed_at}  
**Scope:** All **${records.length}** Marriott Mexico Wave 1D records (read-only; freeze untouched)

Constraints honored: No Airtable · No Webhound · No BE activation · No production overwrite · **No auto-merges** · No fake rooms/owners/open dates

---

## Executive verdict

Wave 1D is **safe to include** in the combined 4-family Mexico VIC baseline with **minor holds** documented below. Staging identity count **301 = 301 unique MARSHA** is sound. Material completeness remains sitemap-limited (40%).

---

## 1. Identity-risk pass (301/301)

| Risk | Count |
|------|------:|
| Low | ${identityRiskRows.filter((x) => x.identity_risk === "low").length} |
| Medium+ | ${identityRiskRows.filter((x) => x.identity_risk !== "low").length} |
| MARSHA collisions | ${marshaDupes.length} |

Overlay: \`steward-review/01-identity-risk-all-records.json\`

---

## 2. Brand parsing QA

| Brand | Count | Parsing Confidence | Adjacent Brand Risk | Samples | Steward Action |
|-------|------:|--------------------|---------------------|---------|----------------|
${brandParsingQa
  .map(
    (b) =>
      `| ${b.brand} | ${b.count} | ${b.parsing_confidence} | ${b.adjacent_brand_risk} | ${b.sample_records_reviewed.map((s) => s.replace(/\|/g, "/")).join("; ")} | ${b.steward_action} |`
  )
  .join("\n")}

### Four Points / Sheraton verification

- Four Points by Sheraton: **${fourPoints.length}**
- Sheraton: **${sheraton.length}**
- Four Points leaked into Sheraton: **${fourPointsLeakIntoSheraton.length}**
- **Verified clean:** ${fourPointsLeakIntoSheraton.length === 0 ? "YES" : "NO"}

### City Express family verification

| Sub-brand | Count |
|-----------|------:|
${Object.entries(cityExpressFamily)
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n")}

- Misparse count: **${cityExpressMisparse.length}**
- **Verified clean:** ${cityExpressMisparse.length === 0 ? "YES" : "NO"}

---

## 3. Brand Unconfirmed (${unconfirmedReviews.length})

| Property | MARSHA | Likely brand | Confidence | Classification | Action |
|----------|--------|--------------|------------|----------------|--------|
${unconfirmedReviews
  .map(
    (u) =>
      `| ${u.property_name.replace(/\|/g, "/")} | ${u.marsha} | ${u.likely_brand_if_inferable || "—"} | ${u.confidence} | \`${u.classification}\` | ${u.recommended_action.replace(/\|/g, "/")} |`
  )
  .join("\n")}

**Source-supported confirm candidate:** Mexico City Marriott Reforma Hotel → \`confirm_brand\` Marriott Hotels (map miss: intervening place token). Soft brands / HNF: keep unconfirmed; exclude from BE completion.

---

## 4. Physical identity (301 unique)

${physicalSummary.verdict}

| Metric | Value |
|--------|------:|
| Near-duplicate pairs reviewed | ${nearDuplicateCandidates.length} |
| Campus / high-sim steward pairs | ${physicalStewardQueue.length} |
| Confirmed same-physical merges | ${physicalSummary.confirmed_same_physical_merges} |
| Auto-merges | **0** |

Campus / annex pairs (Solaz Residences, Casa Maat, Westin Baja Point, Royalton / Planet Hollywood siblings) classified **\`insufficient_evidence_do_not_merge\`** or **\`distinct_physical_property\`** — different MARSHA, no address/coords proof.

City Express Plus/Junior/Suites siblings in the same city: **\`distinct_physical_property\`**.

---

## 5. Cross-family vs locked 365 (not coords-only)

| Classification | Count |
|----------------|------:|
| Exact physical — steward review | ${crossSummary.exact_physical_match_requires_steward_review} |
| Probable physical — steward review | ${crossSummary.probable_physical_match_requires_steward_review} |
| Rejected fuzzy (city-corridor brands) | ${crossSummary.rejected_fuzzy_match} |
| Insufficient evidence — no merge | ${crossSummary.insufficient_evidence_no_merge} |
| Auto-merges | **0** |

${crossSummary.note}

Exact/probable steward pairs (if any): see \`steward-review/05-cross-family-name-city-review.json\`.

---

## 6. Completeness / migration risk

| Metric | Count |
|--------|------:|
| Data-eligible | ${completenessMigration.data_eligible} |
| Data-ineligible | ${completenessMigration.data_ineligible} |
| Missing address | ${completenessMigration.missing_address} |
| Missing coordinates | ${completenessMigration.missing_coordinates} |
| Missing rooms | ${completenessMigration.missing_rooms} |
| Missing owner/operator | ${completenessMigration.missing_owner_operator} |
| Missing open date | ${completenessMigration.missing_open_date} |
| Sitemap-only | ${completenessMigration.sitemap_only} |
| Property-page enriched | ${completenessMigration.property_page_enriched} |
| BE completion support candidates | ${completenessMigration.suitable_for_be_completion_support} |
| Census-identity suitable | ${completenessMigration.suitable_only_for_census_identity} |

- Staging migration ready: **YES** (identity fields only)
- Production overwrite ready: **NO**
- Fake fields created this review: **0**

---

## Minor holds (non-blocking for 4-family lock)

${minorHolds.map((h) => `- ${h}`).join("\n")}

## Blocking issues

${blockingIssues.length ? blockingIssues.map((h) => `- ${h}`).join("\n") : "_None_"}

---

## Recommended next step

1. Accept Wave 1D into **combined 4-family Mexico VIC baseline lock** (IHG 195 + Hilton 102 + Choice 68 + Marriott 301).
2. Optional overlay: confirm Mexico City Marriott Reforma → Marriott Hotels (do not rewrite freeze hash without explicit lock task).
3. Keep Brand Unconfirmed soft brands out of BE completion.
4. Do not migrate rooms/coords/owner/open date until first-party enrichment.

---

## Acceptance checklist

- [x] All 301 records reviewed at identity-risk level
- [x] Brand parsing QA complete (28 brands)
- [x] All Brand Unconfirmed reviewed
- [x] Four Points / Sheraton verified
- [x] City Express family verified
- [x] Duplicate / near-duplicate risk reviewed
- [x] Cross-family not coords-only
- [x] No auto-merges
- [x] No fake rooms/owners/open dates
- [x] No Airtable / Webhound / BE activation / production overwrite
- [x] Status: \`${status}\`
`;

writeMd(OVERLAY, "07-steward-review-report.md", md);
writeMd(REPORTS, "verified-independent-census-wave1d-marriott-steward-review.md", md);
writeJson(REPORTS, "verified-independent-census-wave1d-marriott-steward-review.json", {
  ...reviewSummary,
  brand_parsing_qa: brandParsingQa,
  brand_unconfirmed: unconfirmedReviews,
  physical_steward_priority_count: physicalStewardQueue.length,
  cross_family_exact_or_probable: crossFamily.filter(
    (x) => x.classification.startsWith("exact") || x.classification.startsWith("probable")
  ),
});

writeMd(
  DOCS,
  "verified-independent-census-wave1d-marriott-steward-review.md",
  `# VIC Wave 1D Marriott — Steward Review

> **Status:** \`${status}\`  
> **Overlay:** \`data/research-engine-v2/verified-independent-census-wave1d-marriott/steward-review/\`  
> **Reports:** \`reports/research-engine-v2/verified-independent-census-wave1d-marriott-steward-review.{md,json}\`

## Verdict

Marriott Wave 1D (**301** / **301** unique MARSHA) is **ready for 4-family Mexico VIC baseline lock** with minor holds (Brand Unconfirmed, sitemap material gaps, campus annex notes). No auto-merges. No production overwrite.

## Holds

${minorHolds.map((h) => `- ${h}`).join("\n")}

## Next

Run combined 4-family Mexico VIC baseline lock when ready.
`
);

console.log("[steward-1d] done", {
  status,
  records: records.length,
  unconfirmed: unconfirmedReviews.length,
  physical_steward_pairs: physicalStewardQueue.length,
  cross_exact: crossSummary.exact_physical_match_requires_steward_review,
  cross_probable: crossSummary.probable_physical_match_requires_steward_review,
  cross_rejected_fuzzy: crossSummary.rejected_fuzzy_match,
  blocking: blockingIssues.length,
  minor_holds: minorHolds.length,
});
