/**
 * Brand Explorer 62 — background validation vs Hotel Property Census (read-only).
 * Patch proposals only. No Census writes. No production BE writes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePat, resolveTargetBase } from "../research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../research-engine-v2/production-census-write.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import { scanOwnerFacingForbiddenLanguage } from "./brand-explorer-public-visibility-quality-lock.js";
import {
  RECENT_MOMENTUM_FORBIDDEN_PATTERNS,
  RECENT_MOMENTUM_SLOT,
} from "./brand-explorer-recent-momentum-contract.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

export const VALIDATION_VERSION = "brand-explorer-62-background-validation-v1";
export const EXPECTED_ACTIVE = 62;
export const EXPECTED_CENSUS_RECORDS = 666;
export const EXPECTED_CENSUS_FIELDS = 101;
export const EXPECTED_HELD = 4;
export const HELD_FLEX_SLUG = "four-points-flex-by-sheraton";

export const STATUS = Object.freeze({
  PATCH_PLAN_READY: "brand_explorer_62_background_validation_patch_plan_ready",
  CLEAN: "brand_explorer_62_background_validation_clean_no_patch_needed",
  HOLD: "brand_explorer_62_background_validation_hold_before_patch",
});

const CENSUS_TABLE = "Hotel Property Census";
const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

/** Required Census fields for this validation (alias Brand → Current Brand; Source Family → Family / Source Family). */
export const REQUIRED_CENSUS_FIELDS = Object.freeze([
  "Property Name",
  "Current Brand",
  "Affiliation Status",
  "City",
  "State / Region",
  "Country",
  "Source URL",
  "Family / Source Family",
  "Data Eligible",
  "Data Confidence Tier",
  "Production Use Status",
  "Enrichment Status",
  "Human Review Required",
  "Latitude",
  "Longitude",
  "Address",
  "Radar Display Status",
  "Radar Display Reason",
  "Radar Geography Status",
  "Public Census Eligibility",
  "Public Display Confidence",
  "Public Display Review Status",
  "Hotel Description - Source Text",
  "Hotel Description - AI Summary",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "Property Type",
  "Asset Context",
  "Market / Submarket",
  "F&B Flag",
  "Meeting Space Flag",
  "Resort / Leisure Flag",
  "Extended Stay Flag",
  "Mixed-Use Flag",
  "Branded Residences Flag",
  "Last Reviewed Date",
  "Owner Name",
  "Developer Name",
  "Operator / Management Company",
  "Rooms / Keys",
  "Opening Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Brand Explorer Slug if mapped",
  "Property Identity Key",
]);

/** Extended forbidden terms beyond PVQL mechanical scan (process / internal leakage). */
export const EXTRA_FORBIDDEN_RE = Object.freeze([
  { id: "consumer_site", re: /\bconsumer site\b/i },
  { id: "source_data", re: /\bsource data\b/i },
  { id: "metadata", re: /\bmetadata\b/i },
  { id: "census_url", re: /\bcensus url\b/i },
  { id: "fdd", re: /\bFDD\b/ },
  { id: "item_19", re: /\bItem 19\b/i },
  { id: "loi", re: /\bLOI\b/ },
  { id: "confirm_fees_fdd", re: /\bconfirm fees\/?FDD\b/i },
  { id: "pipeline_extraction", re: /\bpipeline extraction\b/i },
  { id: "active_property_page", re: /\bactive property page\b/i },
  { id: "source_capture", re: /\bsource-capture\b/i },
  { id: "chd", re: /\bCHD\b/ },
  { id: "listed_on_choice", re: /\blisted on choicehotels\.com\b/i },
  { id: "source_supported", re: /\bsource-supported\b/i },
  { id: "source_pack", re: /\bsource pack\b/i },
  { id: "factory", re: /\b(tab factory|brand factory|factory preview|factory stage)\b/i },
  { id: "stage_process", re: /\b(factory stage|build stage|qa stage|process stage|stage gate)\b/i },
  { id: "qa_process", re: /\b(content QA|profile QA|QA gate|QA check|QA review|QA pass|QA fail)\b/i },
  { id: "governance", re: /\bgovernance\b/i },
  { id: "vic", re: /\bVIC\b/ },
  { id: "census", re: /\bcensus\b/i },
  { id: "staging", re: /\bstaging\b/i },
  { id: "sandbox", re: /\bsandbox\b/i },
  { id: "overlay", re: /\boverlay\b/i },
  { id: "adr", re: /\bADR\b/ },
  { id: "revpar", re: /\bRevPAR\b/i },
]);

/** Soft-brand / collection brands for mapping classification. */
const SOFT_BRAND_SLUGS = new Set([
  "ascend",
  "curio-collection",
  "autograph-collection",
  "tribute-portfolio",
  "design-hotels",
  "tapestry-collection-by-hilton",
  "small-luxury-hotels-of-the-world",
  "voco-hotels",
  "mgallery-collection",
  "handwritten-collection",
  "vignette-collection",
  "bw-premier-collection",
  "bw-signature-collection",
  "trademark-collection-by-wyndham",
  "radisson-individuals-by-choice",
  "preferred-hotels-and-resorts",
]);

/** Known brand name aliases → BE slug (lowercased keys). */
export const BRAND_ALIAS_TO_SLUG = Object.freeze({
  "ac hotels by marriott": "ac-hotels-by-marriott",
  "ac hotels": "ac-hotels-by-marriott",
  "aloft hotels": "aloft-hotels",
  aloft: "aloft-hotels",
  "ascend hotel collection": "ascend",
  ascend: "ascend",
  "autograph collection": "autograph-collection",
  "avid hotels": "avid-hotels",
  avid: "avid-hotels",
  "bunkhouse hotels": "bunkhouse-hotels",
  bunkhouse: "bunkhouse-hotels",
  "bw premier collection": "bw-premier-collection",
  "bw signature collection": "bw-signature-collection",
  "canopy by hilton": "canopy-by-hilton",
  canopy: "canopy-by-hilton",
  "city express by marriott": "city-express-by-marriott",
  "city express": "city-express-by-marriott",
  "comfort inn & suites": "comfort-inn-suites",
  "comfort inn": "comfort-inn-suites",
  "country inn & suites by choice": "country-inn-suites",
  "country inn & suites": "country-inn-suites",
  "courtyard by marriott": "courtyard-by-marriott",
  courtyard: "courtyard-by-marriott",
  "curio collection by hilton": "curio-collection",
  curio: "curio-collection",
  "dazzler by wyndham": "dazzler-by-wyndham",
  dazzler: "dazzler-by-wyndham",
  "design hotels": "design-hotels",
  "doubletree by hilton": "doubletree-by-hilton",
  doubletree: "doubletree-by-hilton",
  "even hotels": "even-hotels",
  "everhome suites": "everhome-suites",
  fairmont: "fairmont",
  "hampton by hilton": "hampton-by-hilton",
  hampton: "hampton-by-hilton",
  "handwritten collection": "handwritten-collection",
  "hilton garden inn": "hilton-garden-inn",
  "hilton hotels & resorts": "hilton-hotels-and-resorts",
  "hilton hotels and resorts": "hilton-hotels-and-resorts",
  hilton: "hilton-hotels-and-resorts",
  "holiday inn express": "holiday-inn-express",
  "home2 suites by hilton": "home2-suites-by-hilton",
  "homewood suites by hilton": "homewood-suites-by-hilton",
  "hotel indigo": "hotel-indigo",
  indigo: "hotel-indigo",
  ibis: "ibis",
  "kimpton hotels": "kimpton",
  kimpton: "kimpton",
  "mama shelter": "mama-shelter",
  "marriott hotels": "marriott-hotels",
  marriott: "marriott-hotels",
  mercure: "mercure",
  "mgallery collection": "mgallery-collection",
  mgallery: "mgallery-collection",
  "motto by hilton": "motto-by-hilton",
  "moxy hotels": "moxy-hotels",
  moxy: "moxy-hotels",
  novotel: "novotel",
  "preferred hotels & resorts": "preferred-hotels-and-resorts",
  preferred: "preferred-hotels-and-resorts",
  pullman: "pullman",
  "quality inn": "quality-inn",
  "radisson blu by choice": "radisson-blu",
  "radisson blu": "radisson-blu",
  "radisson by choice": "radisson",
  radisson: "radisson",
  "radisson individuals by choice": "radisson-individuals-by-choice",
  "radisson red by choice": "radisson-red",
  "radisson red": "radisson-red",
  "residence inn by marriott": "residence-inn-by-marriott",
  "residence inn": "residence-inn-by-marriott",
  sheraton: "sheraton",
  "small luxury hotels of the world": "small-luxury-hotels-of-the-world",
  slh: "small-luxury-hotels-of-the-world",
  "so/": "so",
  so: "so",
  "spark by hilton": "spark-by-hilton",
  "springhill suites by marriott": "springhill-suites-by-marriott",
  studiores: "studiores",
  "suburban studios": "suburban-studios",
  "tapestry collection by hilton": "tapestry-collection-by-hilton",
  tapestry: "tapestry-collection-by-hilton",
  "tempo by hilton": "tempo-by-hilton",
  "towneplace suites by marriott": "towneplace-suites-by-marriott",
  "trademark collection by wyndham": "trademark-collection-by-wyndham",
  "tribute portfolio": "tribute-portfolio",
  "tru by hilton": "tru-by-hilton",
  "vignette collection": "vignette-collection",
  "voco hotels": "voco-hotels",
  voco: "voco-hotels",
  westin: "westin",
  "woodspring suites": "woodspring-suites",
});

const WEBFLOW_FIELD_MATRIX = Object.freeze([
  { field: "Brand Name / card title", required: true, section: "card" },
  { field: "Architecture", required: true, section: "card" },
  { field: "Region Offered", required: true, section: "card" },
  { field: "overview.positioning", required: true, section: "overview" },
  { field: "overview.scenario.1–3", required: true, section: "overview" },
  { field: "valueOwners.scenario.1–4", required: true, section: "value" },
  { field: "footprint.region.*", required: true, section: "footprint" },
  { field: "footprint.openings (property examples)", required: true, section: "footprint" },
  { field: "footprint.momentum (Recent Momentum)", required: true, section: "footprint" },
  { field: "materials.gallery.1–6", required: true, section: "materials" },
  { field: "similar brands", required: false, section: "similar" },
  { field: "AI-Assisted Profile footnote", required: true, section: "footnote" },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function readJson(rel) {
  const p = path.isAbsolute(rel) ? rel : path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(absPath, data) {
  fs.mkdirSync(path.dirname(absPath), { recursive: true });
  fs.writeFileSync(absPath, `${JSON.stringify(data, null, 2)}\n`);
}

function writeMd(absPath, text) {
  fs.mkdirSync(path.dirname(absPath), { recursive: true });
  fs.writeFileSync(absPath, text.endsWith("\n") ? text : `${text}\n`);
}

function normalizeName(s) {
  return nz(s)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenize(s) {
  return normalizeName(s)
    .split(" ")
    .filter((t) => t.length > 2 && !["hotel", "the", "and", "by", "inn", "suites"].includes(t));
}

function nameSimilarity(a, b) {
  const na = normalizeName(a);
  const nb = normalizeName(b);
  if (!na || !nb) return 0;
  if (na === nb) return 1;
  if (na.includes(nb) || nb.includes(na)) return 0.92;
  const ta = new Set(tokenize(a));
  const tb = new Set(tokenize(b));
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter += 1;
  return inter / Math.max(ta.size, tb.size);
}

async function listAllRecords(baseId, token, tableIdOrName, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableIdOrName)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(`list ${tableIdOrName} ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function listTables(baseId, token) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`meta tables ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json.tables || [];
}

async function listPresentationRows(brandName, baseId, token) {
  const formula = `{Brand Name}='${String(brandName).replace(/'/g, "\\'")}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `presentation list ${res.status}`);
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      rows.push({
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        externalDisplayStatus: nz(f["External Display Status"]),
        active: f.Active !== false,
        imageUrl: nz(f["Image URL"] || f.ImageUrl),
        caseSummaryOverview: nz(f["Case Summary Overview"]),
        caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
        caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
        caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
        caseSummaryTags: nz(f["Case Summary Tags"]),
      });
    }
    offset = json.offset || "";
  } while (offset);
  return rows;
}

function isOwnerFacingRow(r) {
  if (r.active === false) return false;
  const eds = nz(r.externalDisplayStatus).toLowerCase();
  if (eds === "hidden" || eds === "internal" || eds === "draft") return false;
  return true;
}

function resolveSlugFromBrandString(brandStr, slugIndex) {
  const n = normalizeName(brandStr);
  if (!n) return null;
  if (BRAND_ALIAS_TO_SLUG[n]) return BRAND_ALIAS_TO_SLUG[n];
  // direct slug match via brand name index
  for (const [slug, names] of slugIndex.entries()) {
    for (const name of names) {
      if (normalizeName(name) === n) return slug;
    }
  }
  // soft contains
  for (const [alias, slug] of Object.entries(BRAND_ALIAS_TO_SLUG)) {
    if (n.includes(alias) || alias.includes(n)) return slug;
  }
  return null;
}

function censusPublicSupportEligible(rec) {
  const f = rec.fields || {};
  const reasons = [];
  if (f["Human Review Required"] === true) reasons.push("human_review_required");
  const aff = nz(f["Affiliation Status"]);
  if (aff === "Brand-Unconfirmed") reasons.push("brand_unconfirmed");
  if (!aff || aff === "Unknown") reasons.push("affiliation_unclear");
  if (f["Data Eligible"] === false) reasons.push("data_eligible_false");
  if (!nz(f["Source URL"])) reasons.push("missing_source_url");
  const prod = nz(f["Production Use Status"]);
  if (/hold|internal only/i.test(prod)) reasons.push("production_use_hold");
  const pubElig = nz(f["Public Census Eligibility"]);
  if (pubElig && !["Eligible", "Eligible With Limits"].includes(pubElig)) {
    reasons.push(`public_eligibility_${pubElig}`);
  }
  const conf = nz(f["Public Display Confidence"]);
  if (conf && ["Low", "Hold"].includes(conf)) reasons.push(`display_confidence_${conf}`);
  const radar = nz(f["Radar Display Status"]);
  if (radar && ["Internal Only", "Hold"].includes(radar)) reasons.push(`radar_${radar}`);
  return { eligible: reasons.length === 0, reasons };
}

function classifyBrandMapping(slug, censusForSlug, soft) {
  if (!censusForSlug.length) return "no_census_records_found";
  const blocked = censusForSlug.filter((r) => nz(r.fields?.["Affiliation Status"]) === "Brand-Unconfirmed");
  if (blocked.length === censusForSlug.length) return "blocked_brand_unconfirmed";
  const exactSlug = censusForSlug.filter((r) => nz(r.fields?.["Brand Explorer Slug if mapped"]) === slug);
  if (exactSlug.length) return soft ? "soft_brand_collection_match" : "exact_brand_match";
  const brandNames = censusForSlug.map((r) => normalizeName(r.fields?.["Current Brand"]));
  const aliasHit = brandNames.some((n) => BRAND_ALIAS_TO_SLUG[n] === slug);
  if (aliasHit) return soft ? "soft_brand_collection_match" : "alias_brand_match";
  if (soft) return "soft_brand_collection_match";
  const reviewish = censusForSlug.some(
    (r) => r.fields?.["Human Review Required"] === true || /conflict|uncertain/i.test(nz(r.fields?.["Steward Review Status"]))
  );
  if (reviewish) return "needs_steward_review";
  return "alias_brand_match";
}

function extractPropertyExamples(rows) {
  return (rows || [])
    .filter((r) => r.slotKey === "footprint.openings" && isOwnerFacingRow(r))
    .map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title,
      body: r.body,
      imageUrl: r.imageUrl,
    }));
}

function extractMomentum(rows) {
  return (rows || [])
    .filter((r) => r.slotKey === RECENT_MOMENTUM_SLOT && isOwnerFacingRow(r))
    .map((r) => ({
      recordId: r.recordId,
      title: r.title,
      body: r.body,
    }));
}

function classifyPropertyExample(example, censusPool, brandSlug) {
  const title = example.title;
  if (!title) {
    return { classification: "founder_decision_needed", reason: "empty_property_title", match: null };
  }
  let best = null;
  let bestScore = 0;
  for (const rec of censusPool) {
    const score = nameSimilarity(title, rec.fields?.["Property Name"]);
    if (score > bestScore) {
      bestScore = score;
      best = rec;
    }
  }
  if (!best || bestScore < 0.55) {
    return {
      classification: "missing_from_census",
      reason: `no_census_name_match score=${bestScore.toFixed(2)}`,
      match: null,
      score: bestScore,
    };
  }
  const support = censusPublicSupportEligible(best);
  const aff = nz(best.fields?.["Affiliation Status"]);
  if (aff === "Brand-Unconfirmed") {
    return {
      classification: "blocked_due_to_brand_unconfirmed",
      reason: "matched census Brand-Unconfirmed",
      match: slimCensus(best),
      score: bestScore,
    };
  }
  if (best.fields?.["Human Review Required"] === true) {
    return {
      classification: "blocked_due_to_human_review",
      reason: "matched census Human Review Required",
      match: slimCensus(best),
      score: bestScore,
    };
  }
  if (!support.eligible) {
    return {
      classification: "brand_mapping_uncertain",
      reason: `public_support_blocked:${support.reasons.join(",")}`,
      match: slimCensus(best),
      score: bestScore,
    };
  }
  const city = nz(best.fields?.City);
  const country = nz(best.fields?.Country);
  const body = example.body || "";
  const needsText =
    (city && !body.toLowerCase().includes(city.toLowerCase()) && !title.toLowerCase().includes(city.toLowerCase())) ||
    (country && !/mexico|méxico|dominican|puerto rico|colombia|brazil|chile|peru|panama|costa rica|jamaica|cuba|guatemala|honduras|salvador|nicaragua|ecuador|argentina|caribbean|cala/i.test(`${title} ${body}`) && /mexico/i.test(country));
  if (needsText) {
    return {
      classification: "confirmed_but_needs_text_update",
      reason: "census_match_ok_location_text_may_need_refresh",
      match: slimCensus(best),
      score: bestScore,
    };
  }
  return {
    classification: "confirmed_in_census",
    reason: `name_similarity=${bestScore.toFixed(2)} brand=${brandSlug}`,
    match: slimCensus(best),
    score: bestScore,
  };
}

function slimCensus(rec) {
  const f = rec.fields || {};
  return {
    recordId: rec.id,
    propertyName: nz(f["Property Name"]),
    currentBrand: nz(f["Current Brand"]),
    slug: nz(f["Brand Explorer Slug if mapped"]),
    affiliationStatus: nz(f["Affiliation Status"]),
    city: nz(f.City),
    stateRegion: nz(f["State / Region"]),
    country: nz(f.Country),
    sourceUrl: nz(f["Source URL"]) ? "[present]" : "",
    humanReviewRequired: f["Human Review Required"] === true,
    dataEligible: f["Data Eligible"] === true,
    publicEligibility: nz(f["Public Census Eligibility"]) || null,
    publicConfidence: nz(f["Public Display Confidence"]) || null,
    propertyType: nz(f["Property Type"]) || null,
    resortLeisure: f["Resort / Leisure Flag"] === true,
    extendedStay: f["Extended Stay Flag"] === true,
    mixedUse: f["Mixed-Use Flag"] === true,
    brandedResidences: f["Branded Residences Flag"] === true,
  };
}

function scanExtraForbidden(rows) {
  const hits = [];
  for (const r of rows || []) {
    if (!isOwnerFacingRow(r)) continue;
    // Skip process-word scan on momentum/openings announcement URLs only for raw URL;
    // still scan title+body for forbidden process terms.
    const text = [r.title, r.body, r.caseSummaryOverview, r.caseSummaryBrandRelevance, r.caseSummaryTags]
      .map(nz)
      .filter(Boolean)
      .join("\n");
    if (!text) continue;
    // Strip URLs before process-term scan to reduce false positives on path fragments
    const scrubbed = text.replace(/https?:\/\/\S+/gi, " ");
    for (const rule of EXTRA_FORBIDDEN_RE) {
      const m = scrubbed.match(rule.re);
      if (m) {
        hits.push({
          id: rule.id,
          term: m[0],
          slotKey: r.slotKey,
          recordId: r.recordId,
          snippet: m[0].slice(0, 80),
        });
      }
    }
  }
  return hits;
}

function classifyMomentumCard(card) {
  const body = nz(card.body);
  const title = nz(card.title);
  const blob = `${title}\n${body}`;
  if (!title || !body) return { classification: "remove", reason: "empty_title_or_body" };
  if (RECENT_MOMENTUM_FORBIDDEN_PATTERNS.some((re) => re.test(blob))) {
    return { classification: "remove", reason: "forbidden_momentum_pattern" };
  }
  const hasUrl = /https?:\/\/\S+/i.test(body);
  const hasDate = /\b(20\d{2}|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Q[1-4])\b/i.test(blob);
  const existenceOnly =
    /\b(property proof|official property page|directory listing|listed on [a-z0-9.-]+\.com|census proves|exists in (the )?census)\b/i.test(
      blob
    ) &&
    !/\b(open(ed|ing)?|sign(ed|ing)|develop(ment|ing)|convert(ed|ing|sion)|renovat|pipeline|groundbreak|announce|debut|launch)\b/i.test(
      blob
    );
  if (existenceOnly) return { classification: "remove", reason: "property_existence_not_momentum" };
  if (!hasDate) return { classification: "needs source", reason: "missing_date" };
  if (!hasUrl) return { classification: "needs source", reason: "missing_announcement_url" };
  const pvql = scanForbiddenLanguage(blob.replace(/https?:\/\/\S+/gi, " "));
  if (pvql.length) return { classification: "soften", reason: `forbidden:${pvql.map((h) => h.id).join(",")}` };
  return { classification: "keep", reason: "dated_source_backed_activity" };
}

function footprintClaimClassification(beClaimText, censusEligibleCount, countryHints) {
  if (!beClaimText) return "no_action";
  const claimsCount = beClaimText.match(/\b(\d{1,4})\s*(hotels?|properties|locations)\b/i);
  if (claimsCount) {
    const n = parseInt(claimsCount[1], 10);
    // Mexico census only — never auto-reduce global counts
    if (n > 0 && censusEligibleCount === 0) return "Census_may_be_incomplete";
    if (n > 0 && censusEligibleCount > 0 && n <= censusEligibleCount + 2 && /mexico|méxico|cala/i.test(beClaimText)) {
      return "BE_claim_supported_by_census";
    }
    if (/mexico|méxico|cala|caribbean|latin/i.test(beClaimText) && censusEligibleCount === 0) {
      return "BE_claim_needs_source_support";
    }
    if (claimsCount && !/mexico|méxico|cala/i.test(beClaimText) && n > 50) {
      return "no_action"; // global portfolio — census is Mexico-scoped
    }
  }
  if (/resort|leisure|extended.?stay|branded residences|mixed-?use/i.test(beClaimText)) {
    if (!countryHints?.hasFlagEvidence) return "Census_may_be_incomplete";
  }
  return "no_action";
}

function loadGateReports() {
  return {
    activeUniverse: readJson("reports/brand-explorer-active-universe-source-of-truth.json"),
    semantic:
      readJson("reports/brand-explorer-global-active-semantic-audit-refresh.json") ||
      readJson("reports/brand-explorer-global-active-semantic-audit.json"),
    pvqlQuiet: readJson("reports/brand-explorer-public-visibility-quality-lock-quiet.json"),
    qualityQuiet:
      readJson("reports/brand-explorer-24-tab-section-quality-audit-quiet.json") ||
      readJson("reports/brand-explorer-24-tab-section-quality-audit.json"),
    footnote:
      readJson("reports/brand-explorer-ai-assisted-footnote-audit-enriched.json") ||
      readJson("reports/brand-explorer-ai-assisted-footnote-audit.json") ||
      readJson("reports/brand-explorer-ai-assisted-footnote-standardization.json"),
    momentumEvidence:
      readJson("reports/brand-explorer-recent-momentum-evidence-quality.json") ||
      readJson("reports/brand-explorer-27-recent-momentum-evidence-audit.json"),
    mandatoryGates: readJson("reports/brand-explorer-mandatory-release-gates.json"),
    baseline62: readJson("reports/brand-explorer-62-active-public-full-baseline.json"),
  };
}

/**
 * Main read-only validation run.
 */
export async function runBrandExplorer62BackgroundValidation(opts = {}) {
  const dryRun = opts.dryRun !== false;
  const token = resolvePat();
  const bases = resolveTargetBase();
  const mvpBase = process.env.AIRTABLE_BASE_ID || bases.mvp_base_id;
  const generatedAt = new Date().toISOString();

  const gates = loadGateReports();
  const universe = gates.activeUniverse;
  const inventory = universe?.inventory || [];
  const activeCount = universe?.activeSourceOfTruth?.totalCount ?? inventory.length;
  const publicFullCount = inventory.filter((b) => b.publicFull === true).length;
  const inactiveIncluded = inventory.filter(
    (b) => !/active|live/i.test(nz(b.activeFlagReason)) && b.publicFull
  );
  const flexInActive = inventory.some((b) => b.slug === HELD_FLEX_SLUG);
  const unexpectedActive = [];

  const active62Ok =
    activeCount === EXPECTED_ACTIVE &&
    publicFullCount === EXPECTED_ACTIVE &&
    !flexInActive &&
    inventory.every((b) => b.publicFull === true);

  // --- Census schema + records ---
  if (!token || !bases.target_base_id) {
    throw new Error("Missing Airtable token or AIRTABLE_BASE_ID_ALT (Platform census base)");
  }
  const tables = await listTables(bases.target_base_id, token);
  const censusTable = tables.find((t) => t.id === CENSUS_TABLE_ID || t.name === CENSUS_TABLE);
  if (!censusTable) throw new Error("Hotel Property Census table not found on Platform base");

  const liveFieldNames = (censusTable.fields || []).map((f) => f.name);
  const liveFieldSet = new Set(liveFieldNames);
  /** User-facing aliases → live Airtable names */
  const fieldAliases = {
    Brand: "Current Brand",
    Developer: "Developer Name",
    "Source Family": "Family / Source Family",
    "Renovation Date": "Renovation / Conversion Date",
  };
  const missingRequired = REQUIRED_CENSUS_FIELDS.filter((n) => {
    if (liveFieldSet.has(n)) return false;
    const alias = fieldAliases[n];
    return !(alias && liveFieldSet.has(alias));
  });
  const fieldCount = liveFieldNames.length;

  const censusFieldsToFetch = [
    "Property Name",
    "Property Identity Key",
    "Current Brand",
    "Brand Explorer Slug if mapped",
    "Affiliation Status",
    "City",
    "State / Region",
    "Country",
    "Source URL",
    "Family / Source Family",
    "Data Eligible",
    "Data Confidence Tier",
    "Production Use Status",
    "Enrichment Status",
    "Human Review Required",
    "Latitude",
    "Longitude",
    "Address",
    "Radar Display Status",
    "Radar Display Reason",
    "Radar Geography Status",
    "Public Census Eligibility",
    "Public Display Confidence",
    "Public Display Review Status",
    "Hotel Description - Source Text",
    "Hotel Description - AI Summary",
    "Amenities - Source Text",
    "Amenities - Structured Tags",
    "Property Type",
    "Asset Context",
    "Market / Submarket",
    "F&B Flag",
    "Meeting Space Flag",
    "Resort / Leisure Flag",
    "Extended Stay Flag",
    "Mixed-Use Flag",
    "Branded Residences Flag",
    "Last Reviewed Date",
    "Steward Review Status",
  ];

  const censusRecords = await listAllRecords(
    bases.target_base_id,
    token,
    censusTable.id,
    censusFieldsToFetch
  );

  const identityKeys = censusRecords.map((r) => nz(r.fields?.["Property Identity Key"])).filter(Boolean);
  const dupKeys = identityKeys.filter((k, i) => identityKeys.indexOf(k) !== i);
  const heldCount = censusRecords.filter((r) => r.fields?.["Human Review Required"] === true).length;

  const censusContract = {
    table: CENSUS_TABLE,
    tableId: censusTable.id,
    fieldCount,
    expectedFieldCount: EXPECTED_CENSUS_FIELDS,
    recordCount: censusRecords.length,
    expectedRecordCount: EXPECTED_CENSUS_RECORDS,
    duplicateIdentityKeys: [...new Set(dupKeys)],
    heldHumanReviewRequired: heldCount,
    expectedHeld: EXPECTED_HELD,
    missingRequiredFields: missingRequired,
    radarFieldsPresent: [
      "Radar Display Status",
      "Radar Display Reason",
      "Radar Geography Status",
      "Public Census Eligibility",
      "Public Display Confidence",
      "Public Display Review Status",
    ].every((n) => liveFieldNames.includes(n)),
    contractOk:
      fieldCount === EXPECTED_CENSUS_FIELDS &&
      censusRecords.length === EXPECTED_CENSUS_RECORDS &&
      heldCount === EXPECTED_HELD &&
      dupKeys.length === 0 &&
      missingRequired.length === 0,
  };

  // Index census by slug
  const slugIndex = new Map();
  for (const b of inventory) {
    slugIndex.set(b.slug, [b.brandName, b.slug.replace(/-/g, " ")].filter(Boolean));
  }

  const censusBySlug = new Map();
  const unmappedCensus = [];
  for (const rec of censusRecords) {
    const f = rec.fields || {};
    let slug = nz(f["Brand Explorer Slug if mapped"]);
    if (!slug) {
      slug = resolveSlugFromBrandString(f["Current Brand"], slugIndex) || "";
    }
    if (!slug || !slugIndex.has(slug)) {
      unmappedCensus.push({
        recordId: rec.id,
        propertyName: nz(f["Property Name"]),
        currentBrand: nz(f["Current Brand"]),
        affiliationStatus: nz(f["Affiliation Status"]),
      });
      continue;
    }
    if (!censusBySlug.has(slug)) censusBySlug.set(slug, []);
    censusBySlug.get(slug).push(rec);
  }

  // --- Load presentation rows per brand (MVP base) ---
  if (!mvpBase || !token) throw new Error("Missing AIRTABLE_BASE_ID for Brand Explorer presentations");

  const brandResults = [];
  const patchProposals = [];
  const propertyExampleRows = [];
  const footprintRows = [];
  const textReviewRows = [];
  const momentumRows = [];
  const webflowBrandRows = [];
  const censusCorrectionFlags = [];
  const founderDecisions = [];

  for (const brand of inventory) {
    const slug = brand.slug;
    const name = brand.brandName;
    process.stdout.write(`[be62-bg] ${slug}... `);
    let rows = [];
    try {
      rows = await listPresentationRows(name, mvpBase, token);
      await sleep(400);
    } catch (e) {
      console.log(`ERR ${e.message}`);
      brandResults.push({
        slug,
        brandName: name,
        error: e.message,
        mappingClass: "needs_steward_review",
      });
      continue;
    }

    const ownerRows = rows.filter(isOwnerFacingRow);
    const censusForBrand = censusBySlug.get(slug) || [];
    const soft = SOFT_BRAND_SLUGS.has(slug);
    const mappingClass = classifyBrandMapping(slug, censusForBrand, soft);

    const eligibleCensus = censusForBrand.filter((r) => censusPublicSupportEligible(r).eligible);
    const examples = extractPropertyExamples(ownerRows);
    const exampleResults = examples.map((ex) => {
      const classified = classifyPropertyExample(ex, censusForBrand, slug);
      propertyExampleRows.push({
        brand: slug,
        title: ex.title,
        ...classified,
      });
      if (
        ["should_remove_from_BE", "confirmed_but_needs_text_update", "census_has_better_example"].includes(
          classified.classification
        )
      ) {
        patchProposals.push({
          brand: slug,
          field: "footprint.openings",
          currentValue: ex.title,
          proposedValue:
            classified.classification === "confirmed_but_needs_text_update"
              ? `Refresh location framing using Census city/country for ${classified.match?.propertyName || ex.title}`
              : classified.classification === "census_has_better_example"
                ? `Consider Census property: ${classified.match?.propertyName}`
                : "Remove or replace — not safe for public Census support",
          reason: classified.reason,
          sourceSupport: classified.match?.sourceUrl ? "census_source_url_present" : "none",
          censusSupport: classified.match ? classified.match : null,
          riskLevel: classified.classification === "confirmed_but_needs_text_update" ? "low" : "medium",
          founderApprovalNeeded: true,
          category:
            classified.classification === "confirmed_but_needs_text_update"
              ? "property_example_update"
              : "Census_crosscheck_update",
        });
      }
      if (classified.classification === "missing_from_census" && /mexico|méxico|cancun|cdmx|guadalajara/i.test(ex.title + ex.body)) {
        censusCorrectionFlags.push({
          type: "be_property_missing_from_census",
          brand: slug,
          propertyTitle: ex.title,
          note: "BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.",
        });
      }
      return { title: ex.title, ...classified };
    });

    // Footprint / count claims from region + portfolio slots
    const geoBodies = ownerRows
      .filter((r) => /^footprint\.(region|portfolio|geography)/i.test(r.slotKey) || r.slotKey === "footprint.portfolio_mix")
      .map((r) => `${r.title}\n${r.body}`)
      .join("\n");
    const flagEvidence = {
      hasFlagEvidence: eligibleCensus.some(
        (r) =>
          r.fields?.["Resort / Leisure Flag"] === true ||
          r.fields?.["Extended Stay Flag"] === true ||
          r.fields?.["Mixed-Use Flag"] === true ||
          r.fields?.["Branded Residences Flag"] === true
      ),
    };
    const footprintClass = footprintClaimClassification(geoBodies, eligibleCensus.length, flagEvidence);
    footprintRows.push({
      brand: slug,
      censusRecordCount: censusForBrand.length,
      censusEligiblePublicSupport: eligibleCensus.length,
      mappingClass,
      claimClassification: footprintClass,
      note:
        footprintClass === "Census_may_be_incomplete"
          ? "Mexico Census may not cover full CALA/global footprint — do not auto-reduce BE global counts"
          : null,
    });
    if (["BE_claim_should_be_softened", "BE_claim_should_be_removed", "BE_claim_needs_source_support"].includes(footprintClass)) {
      patchProposals.push({
        brand: slug,
        field: "footprint.region / portfolio claims",
        currentValue: geoBodies.slice(0, 180),
        proposedValue: "Soften regional count language to source-backed Mexico/CALA examples without inventing global totals from Census",
        reason: footprintClass,
        sourceSupport: "review_needed",
        censusSupport: { eligibleCount: eligibleCensus.length, totalMapped: censusForBrand.length },
        riskLevel: "medium",
        founderApprovalNeeded: true,
        category: "count_or_footprint_softening",
      });
    }

    // Owner-facing forbidden language
    const pvqlHits = scanOwnerFacingForbiddenLanguage(ownerRows);
    const extraHits = scanExtraForbidden(ownerRows);
    textReviewRows.push({
      brand: slug,
      pvqlForbiddenHits: pvqlHits.length,
      extraForbiddenHits: extraHits.length,
      pvqlHits: pvqlHits.slice(0, 10),
      extraHits: extraHits.slice(0, 10),
      pass: pvqlHits.length === 0 && extraHits.length === 0,
    });
    for (const h of [...pvqlHits, ...extraHits]) {
      patchProposals.push({
        brand: slug,
        field: h.slotKey || "owner_facing_text",
        currentValue: h.snippet || h.term || h.label,
        proposedValue: "Rewrite without internal/process/forbidden language",
        reason: `forbidden_language:${h.id || h.label}`,
        sourceSupport: "n/a",
        censusSupport: null,
        riskLevel: "high",
        founderApprovalNeeded: true,
        category: "safe_text_cleanup",
      });
    }

    // Recent Momentum
    const momentum = extractMomentum(ownerRows);
    for (const card of momentum) {
      const c = classifyMomentumCard(card);
      momentumRows.push({ brand: slug, title: card.title, ...c });
      if (c.classification !== "keep") {
        patchProposals.push({
          brand: slug,
          field: RECENT_MOMENTUM_SLOT,
          currentValue: card.title,
          proposedValue: `${c.classification} — ${c.reason}`,
          reason: c.reason,
          sourceSupport: c.classification === "needs source" ? "missing" : "review",
          censusSupport: "Census property existence is NOT momentum evidence",
          riskLevel: "high",
          founderApprovalNeeded: true,
          category: c.classification === "remove" ? "do_not_patch" : "founder_decision_needed",
          note: "Recent Momentum patches require separate founder approval",
        });
        if (c.classification === "remove" || c.classification === "needs source") {
          founderDecisions.push({
            brand: slug,
            topic: "recent_momentum",
            title: card.title,
            recommendation: c.classification,
            reason: c.reason,
          });
        }
      }
    }

    // Webflow field readiness (slot presence heuristics)
    const slotSet = new Set(ownerRows.map((r) => r.slotKey));
    const openingsCount = examples.length;
    const momentumCount = momentum.length;
    const galleryCount = ownerRows.filter((r) => /^materials\.gallery/i.test(r.slotKey)).length;
    const scenarioCount = ownerRows.filter((r) => /scenario\.\d/i.test(r.slotKey)).length;
    const valueScenarioCount = ownerRows.filter((r) => /^valueOwners\.scenario/i.test(r.slotKey) || /value.*scenario/i.test(r.slotKey)).length;
    const longBodies = ownerRows.filter((r) => (r.body || "").length > 900);
    const webflow = {
      brand: slug,
      openingsCount,
      momentumCount,
      galleryCount,
      scenarioCount,
      valueScenarioCount,
      longBodyCount: longBodies.length,
      renderRisk:
        openingsCount < 3 || galleryCount < 6 || momentumCount < 2
          ? "medium"
          : longBodies.length
            ? "low"
            : "none",
      textRisk: pvqlHits.length || extraHits.length ? "high" : "none",
      sourceRisk: exampleResults.some((e) => e.classification === "missing_from_census") ? "medium" : "low",
      censusSupport: eligibleCensus.length,
      fields: WEBFLOW_FIELD_MATRIX.map((f) => {
        let status = "present_assumed";
        let proposedAction = "no_action";
        if (f.field.includes("openings")) {
          status = openingsCount >= 3 ? "ok" : openingsCount > 0 ? "thin" : "missing";
          if (status !== "ok") proposedAction = "property_example_review";
        } else if (f.field.includes("momentum")) {
          status = momentumCount >= 2 ? "ok" : momentumCount > 0 ? "thin" : "missing";
          if (status !== "ok") proposedAction = "momentum_founder_review";
        } else if (f.field.includes("gallery")) {
          status = galleryCount >= 6 ? "ok" : "thin";
          if (status !== "ok") proposedAction = "Webflow_render_fix";
        } else if (f.field.includes("scenario.1–3")) {
          status = scenarioCount >= 3 ? "ok" : "thin";
        } else if (f.field.includes("scenario.1–4")) {
          status = valueScenarioCount >= 4 || scenarioCount >= 4 ? "ok" : "review";
        } else if (f.field.includes("footnote")) {
          status = "defer_to_footnote_audit";
        }
        return {
          field: f.field,
          required: f.required,
          currentStatus: status,
          renderRisk: status === "missing" ? "high" : status === "thin" ? "medium" : "low",
          textRisk: pvqlHits.length ? "high" : "low",
          sourceRisk: "see_census_crosscheck",
          censusSupport: f.field.includes("openings") ? eligibleCensus.length : null,
          proposedAction,
        };
      }),
    };
    webflowBrandRows.push(webflow);

    if (mappingClass === "needs_steward_review" || mappingClass === "blocked_brand_unconfirmed") {
      founderDecisions.push({
        brand: slug,
        topic: "brand_census_mapping",
        recommendation: mappingClass,
        reason: `${censusForBrand.length} census rows; eligible=${eligibleCensus.length}`,
      });
    }

    brandResults.push({
      slug,
      brandName: name,
      mappingClass,
      censusMapped: censusForBrand.length,
      censusEligiblePublicSupport: eligibleCensus.length,
      propertyExamples: exampleResults,
      footprintClass,
      textPass: pvqlHits.length === 0 && extraHits.length === 0,
      momentumCount,
      openingsCount,
    });
    console.log(
      `map=${mappingClass} census=${censusForBrand.length}/${eligibleCensus.length} ex=${examples.length} mom=${momentumCount}`
    );
  }

  // Better census examples suggestion (eligible census not used in BE)
  for (const brand of brandResults) {
    const usedTitles = new Set((brand.propertyExamples || []).map((e) => normalizeName(e.title)));
    const pool = (censusBySlug.get(brand.slug) || []).filter((r) => censusPublicSupportEligible(r).eligible);
    for (const rec of pool.slice(0, 8)) {
      const pname = nz(rec.fields?.["Property Name"]);
      const already = [...usedTitles].some((t) => nameSimilarity(t, pname) >= 0.7);
      if (!already && brand.openingsCount < 3) {
        propertyExampleRows.push({
          brand: brand.slug,
          title: pname,
          classification: "census_has_better_example",
          reason: "eligible_census_property_not_used_in_BE_openings",
          match: slimCensus(rec),
        });
        patchProposals.push({
          brand: brand.slug,
          field: "footprint.openings",
          currentValue: `(have ${brand.openingsCount} openings)`,
          proposedValue: `Consider adding Census-backed example: ${pname} (${nz(rec.fields?.City)}, ${nz(rec.fields?.Country)})`,
          reason: "BE openings thin; Census has eligible unused property",
          sourceSupport: "census_source_url_present",
          censusSupport: slimCensus(rec),
          riskLevel: "low",
          founderApprovalNeeded: true,
          category: "property_example_update",
        });
        break;
      }
    }
  }

  // Gate summary
  const semanticBuckets = gates.semantic?.severityTotals || gates.semantic?.bucketCounts || {};
  const semanticCHM = {
    critical: semanticBuckets.critical ?? semanticBuckets.C ?? 0,
    high: semanticBuckets.high ?? semanticBuckets.H ?? 0,
    medium: semanticBuckets.medium ?? semanticBuckets.M ?? 0,
  };
  const pvqlPass =
    gates.pvqlQuiet != null ? gates.pvqlQuiet?.summary?.overallPass === true : null;
  const pvqlCount = gates.pvqlQuiet?.summary?.publicFullProfileCount ?? null;
  const momentumPass =
    gates.momentumEvidence != null
      ? gates.momentumEvidence?.pass === true ||
        gates.momentumEvidence?.summary?.pass === true ||
        gates.momentumEvidence?.overallPass === true ||
        gates.momentumEvidence?.cliPass === true
      : null;
  const mandatoryPass =
    gates.mandatoryGates != null
      ? gates.mandatoryGates?.pass === true ||
        gates.mandatoryGates?.overallPass === true ||
        gates.mandatoryGates?.summary?.pass === true ||
        gates.mandatoryGates?.cliPass === true
      : null;
  const footnotePass =
    gates.footnote != null
      ? gates.footnote?.summary?.fail === 0 ||
        gates.footnote?.pass === true ||
        gates.footnote?.auditPass === true ||
        (typeof gates.footnote?.summary?.pass === "number" &&
          gates.footnote.summary.pass === gates.footnote.summary.totalRows &&
          gates.footnote.summary.fail === 0)
      : null;

  const actionablePatches = patchProposals.filter((p) => p.category !== "do_not_patch");
  const highRisk = actionablePatches.filter((p) => p.riskLevel === "high");

  // HOLD only for universe/contract failure — patch volume becomes PATCH_PLAN_READY.
  let status = STATUS.CLEAN;
  if (!active62Ok || !censusContract.contractOk) {
    status = STATUS.HOLD;
  } else if (actionablePatches.length > 0 || founderDecisions.length > 0) {
    status = STATUS.PATCH_PLAN_READY;
  }

  // Recommended first batch: safe_text_cleanup + confirmed_but_needs_text_update + webflow thin openings
  const firstBatch = actionablePatches
    .filter((p) =>
      ["safe_text_cleanup", "property_example_update", "Webflow_render_fix"].includes(p.category)
    )
    .filter((p) => p.riskLevel !== "high" || p.category === "safe_text_cleanup")
    .slice(0, 25);

  const mappingSummary = {};
  for (const b of brandResults) {
    mappingSummary[b.mappingClass] = (mappingSummary[b.mappingClass] || 0) + 1;
  }
  const exampleSummary = {};
  for (const e of propertyExampleRows) {
    exampleSummary[e.classification] = (exampleSummary[e.classification] || 0) + 1;
  }

  const plan = {
    version: VALIDATION_VERSION,
    generatedAt,
    dryRun,
    airtableWrites: false,
    censusWrites: false,
    brandExplorerWrites: false,
    companyValidatedUntouched: true,
    brandVerifiedUntouched: true,
    brandStatusUntouched: true,
    recentMomentumNotApplied: true,
    status,
    executiveSummary: {
      activeUniverse: activeCount,
      publicFull: publicFullCount,
      fourPointsFlexHeld: !flexInActive,
      censusRecords: censusRecords.length,
      censusFields: fieldCount,
      heldRecords: heldCount,
      brandsMappedExactOrAlias: brandResults.filter((b) =>
        ["exact_brand_match", "alias_brand_match", "soft_brand_collection_match"].includes(b.mappingClass)
      ).length,
      brandsNoCensus: brandResults.filter((b) => b.mappingClass === "no_census_records_found").length,
      patchProposalCount: patchProposals.length,
      actionablePatchCount: actionablePatches.length,
      highRiskPatchCount: highRisk.length,
      founderDecisionCount: founderDecisions.length,
      semanticCHM,
      pvqlPass,
      pvqlCount,
      momentumPass,
      mandatoryPass,
      footnotePass,
    },
    active62Validation: {
      ok: active62Ok,
      activeCount,
      publicFullCount,
      flexInActive,
      inactiveIncluded: inactiveIncluded.map((b) => b.slug),
      unexpectedActive,
      freezeDecision: "frozen_62_active_public_full_baseline_quality_clean_flex_held",
    },
    censusContract,
    brandMappingSummary: mappingSummary,
    brandMappings: brandResults.map((b) => ({
      slug: b.slug,
      brandName: b.brandName,
      mappingClass: b.mappingClass,
      censusMapped: b.censusMapped,
      censusEligiblePublicSupport: b.censusEligiblePublicSupport,
    })),
    propertyExampleSummary: exampleSummary,
    propertyExamples: propertyExampleRows,
    footprintValidation: footprintRows,
    ownerFacingTextReview: textReviewRows,
    forbiddenLanguage: {
      brandsWithHits: textReviewRows.filter((t) => !t.pass).map((t) => t.brand),
      totalPvqlHits: textReviewRows.reduce((n, t) => n + t.pvqlForbiddenHits, 0),
      totalExtraHits: textReviewRows.reduce((n, t) => n + t.extraForbiddenHits, 0),
    },
    webflowFieldReview: {
      matrixTemplate: WEBFLOW_FIELD_MATRIX,
      brands: webflowBrandRows,
    },
    recentMomentumReview: momentumRows,
    beVsCensusMismatches: propertyExampleRows.filter((e) =>
      [
        "missing_from_census",
        "confirmed_but_needs_text_update",
        "blocked_due_to_human_review",
        "blocked_due_to_brand_unconfirmed",
        "should_remove_from_BE",
        "brand_mapping_uncertain",
      ].includes(e.classification)
    ),
    censusCorrectionFlags,
    patchProposals,
    founderDecisionsNeeded: founderDecisions,
    brandExplorerSafety: {
      noCensusWrites: true,
      noProductionBePatchesApplied: true,
      noCompanyValidatedWrites: true,
      noBrandVerifiedWrites: true,
      noBrandStatusWrites: true,
      noRecentMomentumApplied: true,
      mexicoCensusNotUsedAsGlobalPortfolioSoT: true,
      blankCensusFieldsNotNegativeEvidence: true,
    },
    recommendedFirstPatchBatch: firstBatch,
    gateIngest: {
      semanticCHM,
      pvqlPass,
      pvqlCount,
      momentumPass,
      mandatoryPass,
      footnotePass,
      reportsPresent: Object.fromEntries(
        Object.entries(gates).map(([k, v]) => [k, Boolean(v)])
      ),
    },
    unmappedCensusSample: unmappedCensus.slice(0, 40),
    hardRulesHonored: [
      "Do not modify Census records",
      "Do not overwrite Census with Brand Explorer assumptions",
      "Do not patch production Brand Explorer without founder approval",
      "Do not write Company Validated / Brand Verified",
      "Do not create Recent Momentum from property existence",
      "Do not use held / Brand-Unconfirmed as public proof",
      "Mexico Census is regional crosscheck only — not global portfolio SoT",
    ],
  };

  return plan;
}

export function writeBackgroundValidationReports(plan) {
  const outDir = path.join(ROOT, "reports", "brand-explorer");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");

  const planJson = path.join(outDir, "brand-explorer-62-background-validation-plan.json");
  const planMd = path.join(outDir, "brand-explorer-62-background-validation-plan.md");
  const censusJson = path.join(outDir, "brand-explorer-62-new-census-crosscheck.json");
  const censusMd = path.join(outDir, "brand-explorer-62-new-census-crosscheck.md");
  const webflowJson = path.join(outDir, "brand-explorer-62-webflow-field-review.json");
  const webflowMd = path.join(outDir, "brand-explorer-62-webflow-field-review.md");
  const docsMd = path.join(docsDir, "brand-explorer-62-background-validation.md");

  writeJson(planJson, plan);
  writeMd(planMd, renderPlanMarkdown(plan));

  const censusReport = {
    version: VALIDATION_VERSION,
    generatedAt: plan.generatedAt,
    status: plan.status,
    censusContract: plan.censusContract,
    brandMappingSummary: plan.brandMappingSummary,
    brandMappings: plan.brandMappings,
    propertyExampleSummary: plan.propertyExampleSummary,
    propertyExamples: plan.propertyExamples,
    footprintValidation: plan.footprintValidation,
    beVsCensusMismatches: plan.beVsCensusMismatches,
    censusCorrectionFlags: plan.censusCorrectionFlags,
    unmappedCensusSample: plan.unmappedCensusSample,
    hardRulesHonored: plan.hardRulesHonored,
  };
  writeJson(censusJson, censusReport);
  writeMd(censusMd, renderCensusMarkdown(censusReport, plan));

  const webflowReport = {
    version: VALIDATION_VERSION,
    generatedAt: plan.generatedAt,
    status: plan.status,
    ownerFacingTextReview: plan.ownerFacingTextReview,
    forbiddenLanguage: plan.forbiddenLanguage,
    webflowFieldReview: plan.webflowFieldReview,
    recentMomentumReview: plan.recentMomentumReview,
    gateIngest: plan.gateIngest,
  };
  writeJson(webflowJson, webflowReport);
  writeMd(webflowMd, renderWebflowMarkdown(webflowReport, plan));

  writeMd(docsMd, renderDocsMarkdown(plan));

  return { planJson, planMd, censusJson, censusMd, webflowJson, webflowMd, docsMd };
}

function esc(s) {
  return String(s || "").replace(/\|/g, "\\|").replace(/\n/g, " ");
}

function renderPlanMarkdown(plan) {
  const lines = [];
  lines.push("# Brand Explorer 62 — Background Validation Plan");
  lines.push("");
  lines.push(`**Status:** \`${plan.status}\``);
  lines.push(`**Generated:** ${plan.generatedAt}`);
  lines.push(`**Mode:** dry-run / patch proposals only (no Census writes, no production BE patches)`);
  lines.push("");
  lines.push("## 1. Executive summary");
  lines.push("");
  const e = plan.executiveSummary;
  lines.push(`- Active universe: **${e.activeUniverse}** · public-full: **${e.publicFull}** · Four Points Flex held: **${e.fourPointsFlexHeld}**`);
  lines.push(`- Census: **${e.censusRecords}** records / **${e.censusFields}** fields · held: **${e.heldRecords}**`);
  lines.push(`- Brand↔Census mapped (exact/alias/soft): **${e.brandsMappedExactOrAlias}** · no census: **${e.brandsNoCensus}**`);
  lines.push(`- Patch proposals: **${e.patchProposalCount}** (actionable **${e.actionablePatchCount}**) · founder decisions: **${e.founderDecisionCount}**`);
  lines.push(`- Semantic C/H/M: ${JSON.stringify(e.semanticCHM)} · PVQL: ${e.pvqlPass} (${e.pvqlCount}) · momentum: ${e.momentumPass} · gates: ${e.mandatoryPass} · footnote: ${e.footnotePass}`);
  lines.push("");
  lines.push("## 2. Active 62 validation");
  lines.push("");
  lines.push(`- OK: **${plan.active62Validation.ok}**`);
  lines.push(`- Freeze: \`${plan.active62Validation.freezeDecision}\``);
  lines.push(`- Flex in active: **${plan.active62Validation.flexInActive}** (must be false)`);
  lines.push("");
  lines.push("## 3. New Census field contract");
  lines.push("");
  const c = plan.censusContract;
  lines.push(`- Contract OK: **${c.contractOk}** · fields ${c.fieldCount}/${c.expectedFieldCount} · records ${c.recordCount}/${c.expectedRecordCount}`);
  lines.push(`- Held (Human Review Required): ${c.heldHumanReviewRequired}/${c.expectedHeld}`);
  lines.push(`- Duplicate identity keys: ${c.duplicateIdentityKeys.length}`);
  lines.push(`- Missing required fields: ${c.missingRequiredFields.length ? c.missingRequiredFields.join(", ") : "none"}`);
  lines.push(`- Radar fields present: **${c.radarFieldsPresent}**`);
  lines.push("");
  lines.push("## 4. Brand-to-Census mapping");
  lines.push("");
  lines.push("| Mapping class | Count |");
  lines.push("| --- | ---: |");
  for (const [k, v] of Object.entries(plan.brandMappingSummary || {})) {
    lines.push(`| \`${k}\` | ${v} |`);
  }
  lines.push("");
  lines.push("## 5. Property example validation");
  lines.push("");
  lines.push("| Classification | Count |");
  lines.push("| --- | ---: |");
  for (const [k, v] of Object.entries(plan.propertyExampleSummary || {})) {
    lines.push(`| \`${k}\` | ${v} |`);
  }
  lines.push("");
  lines.push("## 6. Hotel count / footprint validation");
  lines.push("");
  lines.push("Mexico Census is used as property proof / regional crosscheck only — not global portfolio SoT. Blank enrichment fields are not negative evidence.");
  lines.push("");
  const fpCounts = {};
  for (const r of plan.footprintValidation || []) {
    fpCounts[r.claimClassification] = (fpCounts[r.claimClassification] || 0) + 1;
  }
  lines.push("| Claim class | Brands |");
  lines.push("| --- | ---: |");
  for (const [k, v] of Object.entries(fpCounts)) lines.push(`| \`${k}\` | ${v} |`);
  lines.push("");
  lines.push("## 7–8. Owner-facing text & forbidden language");
  lines.push("");
  lines.push(`- Brands with forbidden hits: **${(plan.forbiddenLanguage.brandsWithHits || []).length}**`);
  lines.push(`- PVQL hits: ${plan.forbiddenLanguage.totalPvqlHits} · Extra process-term hits: ${plan.forbiddenLanguage.totalExtraHits}`);
  if (plan.forbiddenLanguage.brandsWithHits?.length) {
    lines.push(`- Brands: ${plan.forbiddenLanguage.brandsWithHits.join(", ")}`);
  }
  lines.push("");
  lines.push("## 9. Webflow / product field readiness");
  lines.push("");
  lines.push("See `brand-explorer-62-webflow-field-review.md` for per-brand field matrix.");
  lines.push("");
  lines.push("## 10. Recent Momentum review");
  lines.push("");
  const momCounts = {};
  for (const m of plan.recentMomentumReview || []) {
    momCounts[m.classification] = (momCounts[m.classification] || 0) + 1;
  }
  lines.push("| Classification | Count |");
  lines.push("| --- | ---: |");
  for (const [k, v] of Object.entries(momCounts)) lines.push(`| \`${k}\` | ${v} |`);
  lines.push("");
  lines.push("## 11. BE vs Census mismatches");
  lines.push("");
  lines.push(`Count: **${(plan.beVsCensusMismatches || []).length}** (see census crosscheck JSON for full list).`);
  lines.push("");
  lines.push("## 12. Census correction flags");
  lines.push("");
  if (!(plan.censusCorrectionFlags || []).length) {
    lines.push("- None that require Census mutation from this lane (flags are informational only).");
  } else {
    for (const f of plan.censusCorrectionFlags.slice(0, 30)) {
      lines.push(`- \`${f.type}\` · ${f.brand} · ${esc(f.propertyTitle)} — ${esc(f.note)}`);
    }
  }
  lines.push("");
  lines.push("## 13. Brand Explorer patch proposal table");
  lines.push("");
  lines.push("| Brand | Field | Category | Risk | Founder OK? | Reason |");
  lines.push("| --- | --- | --- | --- | --- | --- |");
  for (const p of (plan.patchProposals || []).slice(0, 80)) {
    lines.push(
      `| ${p.brand} | ${esc(p.field)} | \`${p.category}\` | ${p.riskLevel} | ${p.founderApprovalNeeded} | ${esc(p.reason)} |`
    );
  }
  if ((plan.patchProposals || []).length > 80) {
    lines.push(`| … | … | … | … | … | +${plan.patchProposals.length - 80} more in JSON |`);
  }
  lines.push("");
  lines.push("## 14. Founder decisions needed");
  lines.push("");
  if (!(plan.founderDecisionsNeeded || []).length) {
    lines.push("- None.");
  } else {
    for (const d of plan.founderDecisionsNeeded.slice(0, 40)) {
      lines.push(`- **${d.brand}** · ${d.topic} · ${d.recommendation} — ${esc(d.reason || d.title)}`);
    }
  }
  lines.push("");
  lines.push("## 15. Brand Explorer safety result");
  lines.push("");
  for (const [k, v] of Object.entries(plan.brandExplorerSafety || {})) {
    lines.push(`- ${k}: **${v}**`);
  }
  lines.push("");
  lines.push("## 16. Recommended first patch batch");
  lines.push("");
  if (!(plan.recommendedFirstPatchBatch || []).length) {
    lines.push("- No first-batch patches proposed.");
  } else {
    lines.push("| Brand | Field | Category | Proposed |");
    lines.push("| --- | --- | --- | --- |");
    for (const p of plan.recommendedFirstPatchBatch) {
      lines.push(`| ${p.brand} | ${esc(p.field)} | \`${p.category}\` | ${esc(String(p.proposedValue).slice(0, 120))} |`);
    }
  }
  lines.push("");
  lines.push("---");
  lines.push("");
  lines.push(`**Final status:** \`${plan.status}\``);
  lines.push("");
  return lines.join("\n");
}

function renderCensusMarkdown(report, plan) {
  const lines = [];
  lines.push("# Brand Explorer 62 — New Census Crosscheck");
  lines.push("");
  lines.push(`**Status:** \`${report.status}\` · Generated: ${report.generatedAt}`);
  lines.push("");
  lines.push("## Census contract confirmation");
  lines.push("");
  const c = report.censusContract;
  lines.push(`| Check | Value | Expected |`);
  lines.push(`| --- | --- | --- |`);
  lines.push(`| Field count | ${c.fieldCount} | ${c.expectedFieldCount} |`);
  lines.push(`| Record count | ${c.recordCount} | ${c.expectedRecordCount} |`);
  lines.push(`| Held (Human Review Required) | ${c.heldHumanReviewRequired} | ${c.expectedHeld} |`);
  lines.push(`| Duplicate identity keys | ${c.duplicateIdentityKeys.length} | 0 |`);
  lines.push(`| Radar fields present | ${c.radarFieldsPresent} | true |`);
  lines.push(`| Contract OK | **${c.contractOk}** | true |`);
  lines.push("");
  lines.push("## Brand mapping summary");
  lines.push("");
  for (const [k, v] of Object.entries(report.brandMappingSummary || {})) {
    lines.push(`- \`${k}\`: ${v}`);
  }
  lines.push("");
  lines.push("## Property example classifications");
  lines.push("");
  for (const [k, v] of Object.entries(report.propertyExampleSummary || {})) {
    lines.push(`- \`${k}\`: ${v}`);
  }
  lines.push("");
  lines.push("## Per-brand mapping");
  lines.push("");
  lines.push("| Brand | Mapping | Census rows | Eligible public support |");
  lines.push("| --- | --- | ---: | ---: |");
  for (const b of report.brandMappings || []) {
    lines.push(`| ${b.slug} | \`${b.mappingClass}\` | ${b.censusMapped} | ${b.censusEligiblePublicSupport} |`);
  }
  lines.push("");
  lines.push("## Census correction flags (informational — do not auto-write Census)");
  lines.push("");
  if (!(report.censusCorrectionFlags || []).length) {
    lines.push("- None.");
  } else {
    for (const f of report.censusCorrectionFlags) {
      lines.push(`- ${f.type} · ${f.brand} · ${esc(f.propertyTitle)}`);
    }
  }
  lines.push("");
  lines.push("## Safety");
  lines.push("");
  for (const r of report.hardRulesHonored || []) lines.push(`- ${r}`);
  lines.push("");
  lines.push(`See also plan: status \`${plan.status}\`.`);
  lines.push("");
  return lines.join("\n");
}

function renderWebflowMarkdown(report, plan) {
  const lines = [];
  lines.push("# Brand Explorer 62 — Webflow / Product Field Review");
  lines.push("");
  lines.push(`**Status:** \`${report.status}\` · Generated: ${report.generatedAt}`);
  lines.push("");
  lines.push("## Gate ingest");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(report.gateIngest, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Forbidden language");
  lines.push("");
  lines.push(`- Brands with hits: ${(report.forbiddenLanguage.brandsWithHits || []).join(", ") || "none"}`);
  lines.push(`- PVQL hits: ${report.forbiddenLanguage.totalPvqlHits}`);
  lines.push(`- Extra hits: ${report.forbiddenLanguage.totalExtraHits}`);
  lines.push("");
  lines.push("## Recent Momentum");
  lines.push("");
  const momCounts = {};
  for (const m of report.recentMomentumReview || []) {
    momCounts[m.classification] = (momCounts[m.classification] || 0) + 1;
  }
  for (const [k, v] of Object.entries(momCounts)) lines.push(`- \`${k}\`: ${v}`);
  lines.push("");
  lines.push("## Field matrix (per brand)");
  lines.push("");
  for (const b of (report.webflowFieldReview?.brands || []).slice(0, 62)) {
    lines.push(`### ${b.brand}`);
    lines.push("");
    lines.push(
      `| Field | Required | Status | Render risk | Text risk | Source risk | Census support | Action |`
    );
    lines.push(`| --- | --- | --- | --- | --- | --- | --- | --- |`);
    for (const f of b.fields || []) {
      lines.push(
        `| ${esc(f.field)} | ${f.required} | ${f.currentStatus} | ${f.renderRisk} | ${f.textRisk} | ${f.sourceRisk} | ${f.censusSupport ?? "—"} | ${f.proposedAction} |`
      );
    }
    lines.push("");
  }
  lines.push(`Plan status: \`${plan.status}\``);
  lines.push("");
  return lines.join("\n");
}

function renderDocsMarkdown(plan) {
  const lines = [];
  lines.push("# Brand Explorer 62 — Background Validation");
  lines.push("");
  lines.push(`> **Status:** \`${plan.status}\`  `);
  lines.push(`> **Generated:** ${plan.generatedAt}  `);
  lines.push(`> **Writes:** none (Census untouched; production Brand Explorer patches not applied)`);
  lines.push("");
  lines.push("## Purpose");
  lines.push("");
  lines.push(
    "Retroactive validation of the protected Active/Live **62** Brand Explorer profiles against the production Hotel Property Census (666 / 101 fields, v1.1.1 + v1.1.2 Radar fields). Produces patch proposals only."
  );
  lines.push("");
  lines.push("## Results (snapshot)");
  lines.push("");
  const e = plan.executiveSummary;
  lines.push(`- Active 62 / public-full 62 / Four Points Flex held: **${e.fourPointsFlexHeld}**`);
  lines.push(`- Census contract: ${plan.censusContract.contractOk ? "OK" : "HOLD"} (${e.censusRecords} × ${e.censusFields}, held ${e.heldRecords})`);
  lines.push(`- Patch proposals: ${e.patchProposalCount} · Founder decisions: ${e.founderDecisionCount}`);
  lines.push(`- Semantic C/H/M: ${JSON.stringify(e.semanticCHM)}`);
  lines.push("");
  lines.push("## Artifacts");
  lines.push("");
  lines.push("- `reports/brand-explorer/brand-explorer-62-background-validation-plan.md`");
  lines.push("- `reports/brand-explorer/brand-explorer-62-background-validation-plan.json`");
  lines.push("- `reports/brand-explorer/brand-explorer-62-new-census-crosscheck.md`");
  lines.push("- `reports/brand-explorer/brand-explorer-62-new-census-crosscheck.json`");
  lines.push("- `reports/brand-explorer/brand-explorer-62-webflow-field-review.md`");
  lines.push("- `reports/brand-explorer/brand-explorer-62-webflow-field-review.json`");
  lines.push("");
  lines.push("## Next step");
  lines.push("");
  lines.push(
    "Founder reviews recommended first patch batch. No production Brand Explorer or Census writes from this lane until explicit approval."
  );
  lines.push("");
  return lines.join("\n");
}
