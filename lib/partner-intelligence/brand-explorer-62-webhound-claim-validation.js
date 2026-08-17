/**
 * Brand Explorer Active-62 — Webhound claim validation lane (read-only).
 *
 * Priority (founder clarification):
 *   P1 — public Brand Explorer tabs / public-facing factual claims
 *   P2 — Recent Momentum items (dated event proof; no directory-only)
 *   P3 — property examples (exists, brand match, geography, not stale affiliation)
 *   P4 — parent / brand family / soft-brand / loyalty-distribution
 *
 * Deprioritize broad footprint/regional presence and advisory guest/operator
 * framing unless they contain specific factual claims on a public tab.
 *
 * Never writes Airtable / BE / Setup / Census / Brand Status / CV.
 * Webhound is research sidecar — underlying public pages are evidence SoT.
 */
import fs from "node:fs";
import path from "node:path";
import {
  EXPECTED_ACTIVE_COUNT_62,
  FREEZE_DECISION_62,
  REPORT_JSON_62,
  ROOT,
} from "./brand-explorer-62-active-public-full-baseline.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";

export const VALIDATION_VERSION = "brand-explorer-62-webhound-claim-validation-readonly-v2";
export const VALIDATION_STATUS =
  "brand_explorer_62_webhound_claim_validation_readonly_complete_ready_for_claim_remediation_queue";
export const VALIDATION_STATUS_PACK_READY =
  "brand_explorer_62_webhound_claim_validation_pack_ready_awaiting_webhound";

export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
export const BASICS_TABLE = "Brand Setup - Brand Basics";

export const CLAIM_RESULT_CLASSES = Object.freeze([
  "supported_official",
  "supported_trusted_secondary",
  "partially_supported",
  "unsupported",
  "stale",
  "overclaimed",
  "needs_softening",
  "remove_candidate",
  "conflicting_sources",
  "not_factual_claim",
  "pending_webhound",
]);

export const REMEDIATION_ACTIONS = Object.freeze([
  "keep",
  "soften",
  "replace_with_sourced_wording",
  "remove",
  "needs_steward_review",
]);

export const RISK_LEVELS = Object.freeze(["high", "medium", "low", "none"]);

export const CLAIM_TYPES = Object.freeze([
  "brand_positioning",
  "parent_brand_family",
  "footprint_portfolio_mix",
  "property_example",
  "guest_profile",
  "owner_value_proposition",
  "operator_compatibility",
  "development_conversion_fit",
  "distribution_loyalty",
  "recent_momentum",
  "regional_presence",
  "collection_soft_brand",
  "public_watchout",
]);

/** Pack priority bands (lower = higher priority). */
export const PRIORITY_BAND = Object.freeze({
  P1_PUBLIC_TAB: 1,
  P2_RECENT_MOMENTUM: 2,
  P3_PROPERTY_EXAMPLE: 3,
  P4_PARENT_FAMILY: 4,
  DEPRIORITIZED: 9,
});

export const SOURCE_HIERARCHY = Object.freeze([
  "official_parent_brand_site",
  "official_brand_development_site",
  "official_property_page",
  "official_press_release",
  "owner_developer_page",
  "reputable_hospitality_publication",
  "tourism_board_convention_bureau",
  "trusted_secondary",
]);

export const REJECT_SOURCE_TYPES = Object.freeze([
  "ota",
  "affiliate_mirror",
  "generic_travel_blog",
  "unsourced_ai_summary",
  "stale_archive_only",
  "directory_only",
  "no_exact_brand_property_match",
]);

export const MOMENTUM_VALIDATION_RULES = Object.freeze([
  "real_dated_opening_signing_development_conversion_renovation_pipeline_or_property_proof",
  "source_or_publication_date_required",
  "exact_brand_or_property_match",
  "no_directory_only_momentum",
  "no_vague_growing_presence_without_evidence",
  "no_stale_momentum_as_current",
  "no_property_example_repurposed_as_momentum_without_event_support",
]);

const REPORTS_DIR = path.join(ROOT, "reports", "brand-explorer");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");
export const REPORT_JSON = "brand-explorer-62-webhound-claim-validation-readonly.json";
export const REPORT_MD = "brand-explorer-62-webhound-claim-validation-readonly.md";
export const DOCS_MD = "brand-explorer-62-webhound-claim-validation-readonly.md";
export const CLAIM_PACK_JSON = "brand-explorer-62-webhound-claim-validation-pack.json";
export const CLAIM_PACK_CSV = "brand-explorer-62-webhound-claim-validation-pack.csv";
export const PRIORITY_PACK_JSON = "brand-explorer-62-webhound-claim-validation-priority-pack.json";
export const PRIORITY_PACK_CSV = "brand-explorer-62-webhound-claim-validation-priority-pack.csv";

const SLOT_CLAIM_TYPE = Object.freeze([
  { re: /watchout|risk|caveat|diligence/i, type: "public_watchout" },
  { re: /^footprint\.momentum|momentum|openings\.press/i, type: "recent_momentum" },
  { re: /^overview\.positioning|^Guest Psychographics|target\.guest|audience|psychographic/i, type: "guest_profile" },
  { re: /^overview\.|positioning|pillars|promise|bestAt|differentiator|why_value/i, type: "brand_positioning" },
  { re: /soft|collection|architecture/i, type: "collection_soft_brand" },
  { re: /footprint\.portfolio|portfolio_mix|portfolio\.mix/i, type: "footprint_portfolio_mix" },
  { re: /footprint\.region|geo\.|geographic/i, type: "regional_presence" },
  { re: /footprint\.openings|property\.example|proof\.point|overview\.proof/i, type: "property_example" },
  { re: /valueOwners|value\.owner|owner\.value|scenario/i, type: "owner_value_proposition" },
  { re: /operator_compat|operations\.operator|operator\.fit/i, type: "operator_compatibility" },
  { re: /conversion|development|project\.fit|reposition|standards\.conversion/i, type: "development_conversion_fit" },
  { re: /loyalty|distribution|commercial|ALL|Bonvoy|Honors|Choice Privileges/i, type: "distribution_loyalty" },
]);

const FACTUAL_HINT =
  /\b(part of|owned by|by hilton|by marriott|by choice|by wyndham|by ihg|accor|soft[- ]?brand|collection|franchise|conversion|mexico|caribbean|latin america|cala|loyalty|bonvoy|honors|all\b|live limitless|opening|opened|signed|signing|development|portfolio|properties in|hotels in|independent|renovation|pipeline|announced)\b/i;

const SUBJECTIVE_HINT =
  /^(owners? should|consider|may want|ideal for|best when|typically|often|can help|helps owners|recommended when)\b/i;

const VAGUE_MOMENTUM_HINT =
  /\b(growing presence|expanding footprint|continued growth|momentum across|increasing presence)\b/i;

function nz(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.length ? String(v[0] ?? "").trim() : "";
  return String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function readFrozenBaseline() {
  const p = path.join(ROOT, "reports", REPORT_JSON_62);
  if (!fs.existsSync(p)) throw new Error(`Missing freeze artifact reports/${REPORT_JSON_62}`);
  const frozen = JSON.parse(fs.readFileSync(p, "utf8"));
  if (frozen.freezeDecision !== FREEZE_DECISION_62 || !frozen.frozen) {
    throw new Error(
      `Expected frozen ${FREEZE_DECISION_62}; got ${frozen.freezeDecision} frozen=${frozen.frozen}`
    );
  }
  return frozen;
}

export function publicTabFromSlot(slotKey) {
  const key = nz(slotKey);
  if (!key) return "Brand Basics";
  if (/^overview\.|^hero\./i.test(key)) return "Overview";
  if (/^valueOwners\./i.test(key)) return "Value to Owners";
  if (/^operations\./i.test(key)) return "Operating Model";
  if (/^commercial\./i.test(key)) return "Commercial Engine";
  if (/^loyalty\./i.test(key)) return "Loyalty Program";
  if (/^footprint\.momentum/i.test(key)) return "Recent Momentum";
  if (/^footprint\./i.test(key)) return "Footprint & Growth";
  if (/^standards\./i.test(key)) return "Owner Considerations";
  if (/^materials\./i.test(key)) return "Brand Materials";
  if (/^insight\./i.test(key)) return "Dealality Insight";
  if (/^economics\./i.test(key)) return "Economics";
  return "Public presentation";
}

function claimTypeForSlot(slotKey, text) {
  const sk = nz(slotKey);
  for (const row of SLOT_CLAIM_TYPE) {
    if (row.re.test(sk) || row.re.test(text)) return row.type;
  }
  return "brand_positioning";
}

function splitClaims(text) {
  return String(text || "")
    .replace(/\r/g, "")
    .split(/\n+|(?<=[.!?])\s+(?=[A-Z“"])/)
    .map((s) => s.replace(/\s+/g, " ").trim())
    .filter((s) => s.length >= 24 && s.length <= 420);
}

/** Momentum cards often use dated lines / bullets — keep finer splits. */
function splitMomentumClaims(text) {
  return String(text || "")
    .replace(/\r/g, "")
    .split(/\n+|;\s+|(?<=[.!?])\s+(?=[A-Z“"0-9])/)
    .map((s) => s.replace(/^[-•*]\s*/, "").replace(/\s+/g, " ").trim())
    .filter((s) => s.length >= 18 && s.length <= 480);
}

function classifyFactuality(text, claimType) {
  const t = nz(text);
  if (!t) return "not_factual_claim";
  if (claimType === "recent_momentum") {
    if (VAGUE_MOMENTUM_HINT.test(t) && !/\b(20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i.test(t)) {
      return "factual_candidate"; // still validate — likely unsupported / remove_candidate
    }
    return "factual_candidate";
  }
  if (SUBJECTIVE_HINT.test(t) && !FACTUAL_HINT.test(t)) return "not_factual_claim";
  if (FACTUAL_HINT.test(t)) return "factual_candidate";
  if (/\b(mexico|brazil|colombia|caribbean|hilton|marriott|accor|ihg|wyndham|choice)\b/i.test(t)) {
    return "factual_candidate";
  }
  // Public-tab owner/operator/conversion claims often omit FACTUAL_HINT tokens but still assert capability.
  if (
    ["owner_value_proposition", "operator_compatibility", "development_conversion_fit", "distribution_loyalty"].includes(
      claimType
    ) &&
    /\b(requires?|supports?|includes?|offers?|provides?|must|franchise|management|conversion|loyalty|distribution)\b/i.test(
      t
    )
  ) {
    return "factual_candidate";
  }
  return "not_factual_claim";
}

/**
 * Assign validation priority band per founder clarification.
 */
export function assignPriorityBand({ claimType, slotKey, publicTab, factuality }) {
  if (factuality === "not_factual_claim") return PRIORITY_BAND.DEPRIORITIZED;
  if (claimType === "recent_momentum" || /^footprint\.momentum/i.test(nz(slotKey))) {
    return PRIORITY_BAND.P2_RECENT_MOMENTUM;
  }
  if (claimType === "property_example") return PRIORITY_BAND.P3_PROPERTY_EXAMPLE;
  if (claimType === "parent_brand_family" || claimType === "collection_soft_brand") {
    return PRIORITY_BAND.P4_PARENT_FAMILY;
  }
  if (claimType === "regional_presence") return PRIORITY_BAND.DEPRIORITIZED;
  if (claimType === "footprint_portfolio_mix" && !/portfolio_mix|portfolio\.mix/i.test(nz(slotKey))) {
    return PRIORITY_BAND.DEPRIORITIZED;
  }
  // P1 — public-tab factual claims
  const p1Types = new Set([
    "owner_value_proposition",
    "brand_positioning",
    "development_conversion_fit",
    "operator_compatibility",
    "guest_profile",
    "distribution_loyalty",
    "public_watchout",
    "footprint_portfolio_mix",
  ]);
  if (p1Types.has(claimType) && publicTab && publicTab !== "Brand Basics") {
    return PRIORITY_BAND.P1_PUBLIC_TAB;
  }
  if (claimType === "distribution_loyalty") return PRIORITY_BAND.P4_PARENT_FAMILY;
  return PRIORITY_BAND.P1_PUBLIC_TAB;
}

function priorityLabel(band) {
  if (band === PRIORITY_BAND.P1_PUBLIC_TAB) return "P1_public_tab";
  if (band === PRIORITY_BAND.P2_RECENT_MOMENTUM) return "P2_recent_momentum";
  if (band === PRIORITY_BAND.P3_PROPERTY_EXAMPLE) return "P3_property_example";
  if (band === PRIORITY_BAND.P4_PARENT_FAMILY) return "P4_parent_family";
  return "deprioritized";
}

export function mapRecommendedAction(validationResult, claimType) {
  const r = nz(validationResult);
  if (r === "supported_official" || r === "supported_trusted_secondary") return "keep";
  if (r === "needs_softening" || r === "partially_supported") return "soften";
  if (r === "stale" || r === "overclaimed") {
    return claimType === "recent_momentum" ? "remove" : "replace_with_sourced_wording";
  }
  if (r === "unsupported" || r === "remove_candidate") return "remove";
  if (r === "conflicting_sources") return "needs_steward_review";
  if (r === "not_factual_claim") return "keep";
  return "needs_steward_review";
}

export function riskLevelForResult(validationResult, priorityBand) {
  const r = nz(validationResult);
  if (["unsupported", "overclaimed", "remove_candidate", "stale"].includes(r)) {
    return priorityBand <= PRIORITY_BAND.P2_RECENT_MOMENTUM ? "high" : "medium";
  }
  if (["partially_supported", "needs_softening", "conflicting_sources"].includes(r)) return "medium";
  if (["supported_official", "supported_trusted_secondary", "not_factual_claim"].includes(r)) return "none";
  return "low";
}

function isOwnerFacingRow(row) {
  const eds = nz(row.externalDisplayStatus);
  if (["Do Not Display", "Internal Only"].includes(eds)) return false;
  if (row.active === false) return false;
  return true;
}

async function listPresentationForBrand(baseId, token, brandName) {
  const formula = `{Brand Name}='${String(brandName).replace(/'/g, "\\'")}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `presentation ${res.status}`);
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      rows.push({
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        externalDisplayStatus: nz(f["External Display Status"]),
        active: f.Active !== false,
        caseSummaryOverview: nz(f["Case Summary Overview"]),
        caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
        caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
      });
    }
    offset = json.offset || "";
    await sleep(120);
  } while (offset);
  return rows;
}

async function fetchBasicsFields(baseId, token, recordId) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) return null;
  return json.fields || {};
}

function stableClaimId({ brandSlug, slotKey, field, claimText, claimType }) {
  const raw = [brandSlug, slotKey || "", field || "", claimType || "", nz(claimText).slice(0, 180)]
    .join("|")
    .toLowerCase();
  let h = 2166136261;
  for (let i = 0; i < raw.length; i += 1) {
    h ^= raw.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return `c_${(h >>> 0).toString(16).padStart(8, "0")}`;
}

function pushClaim(out, claim) {
  const publicTab = claim.publicTab || publicTabFromSlot(claim.slotKey || claim.section);
  const priorityBand =
    claim.priorityBand ??
    assignPriorityBand({
      claimType: claim.claimType,
      slotKey: claim.slotKey,
      publicTab,
      factuality: claim.factuality,
    });
  const claimId =
    claim.claimId ||
    stableClaimId({
      brandSlug: claim.brandSlug,
      slotKey: claim.slotKey,
      field: claim.field,
      claimText: claim.claimText,
      claimType: claim.claimType,
    });
  out.push({
    claimId,
    brandSlug: claim.brandSlug,
    brandName: claim.brandName,
    brandRecordId: claim.brandRecordId,
    publicTab,
    section: claim.section,
    slotKey: claim.slotKey || null,
    field: claim.field,
    presentationRecordId: claim.presentationRecordId || null,
    claimText: claim.claimText,
    claimType: claim.claimType,
    factuality: claim.factuality,
    priorityBand,
    priority: priorityLabel(priorityBand),
    webhoundSelected: false,
    validationResult: claim.validationResult || "pending_webhound",
    sourceFound: null,
    sourceUrl: null,
    sourceCategory: null,
    sourceTier: null,
    confidence: null,
    riskLevel: claim.riskLevel || null,
    recommendedAction: claim.recommendedAction || null,
    publicCopyChangeLater: claim.publicCopyChangeLater ?? null,
    notes: claim.notes || null,
  });
}

/**
 * Extract claims for frozen Active-62 (Airtable reads only).
 */
export async function extractActive62Claims({ token, baseId, maxBrands = null } = {}) {
  const apiKey = token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const bid = baseId || process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !bid) throw new Error("Set AIRTABLE_API_KEY/AIRTABLE_PAT and AIRTABLE_BASE_ID");

  const frozen = readFrozenBaseline();
  const live = await loadActiveUniverse({ includeBrandApi: false });
  if (live.totalCount !== EXPECTED_ACTIVE_COUNT_62) {
    throw new Error(`Active universe ${live.totalCount} ≠ ${EXPECTED_ACTIVE_COUNT_62}`);
  }

  let brands = frozen.brands || [];
  if (maxBrands) brands = brands.slice(0, maxBrands);

  const claims = [];
  const brandSummaries = [];

  for (const b of brands) {
    const slug = b.slug;
    const name = b.brandName || b.name;
    const recordId = b.recordId;
    const basics = await fetchBasicsFields(bid, apiKey, recordId);
    await sleep(80);

    const parent = nz(basics?.["Parent Company"]);
    const architecture = nz(basics?.["Brand Architecture"]);
    const regionOffered = basics?.["Region Offered"];
    const regions = Array.isArray(regionOffered)
      ? regionOffered.map(nz).filter(Boolean)
      : nz(regionOffered)
        ? [nz(regionOffered)]
        : [];

    // P4 — parent / family (still validate; not pack-first)
    if (parent) {
      pushClaim(claims, {
        brandSlug: slug,
        brandName: name,
        brandRecordId: recordId,
        publicTab: "Overview",
        section: "Brand Basics / parent family",
        field: "Parent Company",
        claimText: `${name} is part of / affiliated with ${parent}.`,
        claimType: "parent_brand_family",
        factuality: "factual_candidate",
        priorityBand: PRIORITY_BAND.P4_PARENT_FAMILY,
        validationResult: "pending_webhound",
      });
    }

    if (/soft|collection/i.test(architecture) || /collection/i.test(name)) {
      pushClaim(claims, {
        brandSlug: slug,
        brandName: name,
        brandRecordId: recordId,
        publicTab: "Overview",
        section: "Brand Basics / architecture",
        field: "Brand Architecture",
        claimText: `${name} operates as a ${architecture || "collection / soft-brand"} brand structure.`,
        claimType: "collection_soft_brand",
        factuality: "factual_candidate",
        priorityBand: PRIORITY_BAND.P4_PARENT_FAMILY,
        validationResult: "pending_webhound",
      });
    }

    // Regional presence — extract for inventory but DEPRIORITIZE (not pack priority).
    for (const region of regions.slice(0, 3)) {
      pushClaim(claims, {
        brandSlug: slug,
        brandName: name,
        brandRecordId: recordId,
        publicTab: "Footprint & Growth",
        section: "Brand Basics / Region Offered",
        field: "Region Offered",
        claimText: `${name} is offered / present in ${region}.`,
        claimType: "regional_presence",
        factuality: "factual_candidate",
        priorityBand: PRIORITY_BAND.DEPRIORITIZED,
        validationResult: "pending_webhound",
        notes: "Deprioritized unless same fact appears as a public-tab specific claim.",
      });
    }

    const rows = await listPresentationForBrand(bid, apiKey, name);
    let extractedFromPresentation = 0;
    for (const row of rows) {
      if (!isOwnerFacingRow(row)) continue;
      const publicTab = publicTabFromSlot(row.slotKey);
      const isMomentum = /^footprint\.momentum/i.test(row.slotKey) || /momentum/i.test(row.slotKey);
      const fields = [
        ["Title", row.title],
        ["Body", row.body],
        ["Case Summary Overview", row.caseSummaryOverview],
        ["Case Summary Brand Relevance", row.caseSummaryBrandRelevance],
        ["Case Summary Interpretation", row.caseSummaryInterpretation],
      ];
      for (const [field, raw] of fields) {
        if (!raw) continue;
        const parts = isMomentum ? splitMomentumClaims(raw) : splitClaims(raw);
        for (const sentence of parts) {
          const claimType = claimTypeForSlot(row.slotKey, sentence);
          const factuality = classifyFactuality(sentence, claimType);
          const priorityBand = assignPriorityBand({
            claimType,
            slotKey: row.slotKey,
            publicTab,
            factuality,
          });
          pushClaim(claims, {
            brandSlug: slug,
            brandName: name,
            brandRecordId: recordId,
            publicTab,
            section: row.slotKey || "presentation",
            slotKey: row.slotKey,
            field,
            presentationRecordId: row.recordId,
            claimText: sentence,
            claimType,
            factuality,
            priorityBand,
            validationResult:
              factuality === "not_factual_claim" ? "not_factual_claim" : "pending_webhound",
            recommendedAction: factuality === "not_factual_claim" ? "keep" : null,
            publicCopyChangeLater: false,
            notes: isMomentum
              ? "Momentum rules: dated event + exact brand/property match; reject directory-only / vague growth."
              : null,
          });
          extractedFromPresentation += 1;
        }
      }
    }

    brandSummaries.push({
      slug,
      name,
      recordId,
      parentCompany: parent || null,
      architecture: architecture || null,
      regions,
      presentationRows: rows.length,
      claimsExtracted: extractedFromPresentation,
    });
  }

  return {
    frozen,
    liveUniverseCount: live.totalCount,
    brandsChecked: brandSummaries,
    claims,
  };
}

/**
 * Select Webhound pack by founder priority with reserved band quotas so
 * Recent Momentum (P2) cannot crowd out public-tab (P1) / property (P3) / parent (P4).
 *
 * Default budget split for maxClaims=186:
 *   P2 momentum ~35% · P1 public tabs ~35% · P3 property ~18% · P4 parent/family ~12%
 */
export function selectWebhoundClaimPack(claims, { maxClaims = 186 } = {}) {
  const factual = claims.filter(
    (c) => c.factuality === "factual_candidate" && c.priorityBand !== PRIORITY_BAND.DEPRIORITIZED
  );

  const byBand = (band) =>
    factual
      .filter((c) => c.priorityBand === band)
      .sort((a, b) => a.brandSlug.localeCompare(b.brandSlug) || a.claimId.localeCompare(b.claimId));

  const brands = [...new Set(factual.map((c) => c.brandSlug))];
  const selected = [];
  const selectedIds = new Set();
  const perBrandType = new Map();
  const bandCounts = {
    [PRIORITY_BAND.P1_PUBLIC_TAB]: 0,
    [PRIORITY_BAND.P2_RECENT_MOMENTUM]: 0,
    [PRIORITY_BAND.P3_PROPERTY_EXAMPLE]: 0,
    [PRIORITY_BAND.P4_PARENT_FAMILY]: 0,
  };

  const quotas = {
    [PRIORITY_BAND.P2_RECENT_MOMENTUM]: Math.max(brands.length, Math.floor(maxClaims * 0.35)),
    [PRIORITY_BAND.P1_PUBLIC_TAB]: Math.max(brands.length, Math.floor(maxClaims * 0.35)),
    [PRIORITY_BAND.P3_PROPERTY_EXAMPLE]: Math.max(brands.length, Math.floor(maxClaims * 0.18)),
    [PRIORITY_BAND.P4_PARENT_FAMILY]: Math.max(brands.length, Math.floor(maxClaims * 0.12)),
  };

  const tryAdd = (c, { maxPerBrandType = 4, respectQuota = true } = {}) => {
    if (!c || selectedIds.has(c.claimId) || selected.length >= maxClaims) return false;
    const band = c.priorityBand;
    if (respectQuota && bandCounts[band] != null && bandCounts[band] >= quotas[band]) return false;
    const key = `${c.brandSlug}::${c.claimType}`;
    const n = perBrandType.get(key) || 0;
    if (n >= maxPerBrandType) return false;
    selected.push(c);
    selectedIds.add(c.claimId);
    perBrandType.set(key, n + 1);
    if (bandCounts[band] != null) bandCounts[band] += 1;
    return true;
  };

  const p1 = byBand(PRIORITY_BAND.P1_PUBLIC_TAB);
  const p2 = byBand(PRIORITY_BAND.P2_RECENT_MOMENTUM);
  const p3 = byBand(PRIORITY_BAND.P3_PROPERTY_EXAMPLE);
  const p4 = byBand(PRIORITY_BAND.P4_PARENT_FAMILY);

  // Round-robin brand coverage: 1–2 momentum, 1 public-tab, 1 property, 1 parent
  for (const slug of brands) {
    const m = p2.filter((c) => c.brandSlug === slug);
    tryAdd(m[0], { maxPerBrandType: 4 });
    tryAdd(m[1], { maxPerBrandType: 4 });
    tryAdd(
      p1.find((c) => c.brandSlug === slug && !selectedIds.has(c.claimId)),
      { maxPerBrandType: 4 }
    );
    tryAdd(
      p3.find((c) => c.brandSlug === slug && !selectedIds.has(c.claimId)),
      { maxPerBrandType: 3 }
    );
    tryAdd(
      p4.find((c) => c.brandSlug === slug && !selectedIds.has(c.claimId)),
      { maxPerBrandType: 2 }
    );
  }

  // Fill reserved quotas by priority order
  for (const c of p2) tryAdd(c, { maxPerBrandType: 4 });
  for (const c of p1) tryAdd(c, { maxPerBrandType: 4 });
  for (const c of p3) tryAdd(c, { maxPerBrandType: 3 });
  for (const c of p4) tryAdd(c, { maxPerBrandType: 2 });

  // Spill remaining capacity without quotas (still priority-ordered)
  for (const list of [p2, p1, p3, p4]) {
    for (const c of list) tryAdd(c, { maxPerBrandType: 6, respectQuota: false });
  }

  for (const c of claims) {
    c.webhoundSelected = selectedIds.has(c.claimId);
  }
  return selected;
}

export function claimsToCsv(claims) {
  const header = [
    "claim_id",
    "brand_slug",
    "brand_name",
    "public_tab",
    "section",
    "slot_key",
    "field",
    "claim_type",
    "claim_text",
    "priority",
    "priority_band",
    "validation_focus",
  ];
  const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  const lines = [header.join(",")];
  for (const c of claims) {
    const focus =
      c.claimType === "recent_momentum"
        ? "dated_event_exact_match_no_directory_only"
        : c.priorityBand === PRIORITY_BAND.P1_PUBLIC_TAB
          ? "public_tab_factual_accuracy"
          : c.priorityBand === PRIORITY_BAND.P3_PROPERTY_EXAMPLE
            ? "property_exists_brand_geo_match"
            : "parent_family_loyalty_structure";
    lines.push(
      [
        c.claimId,
        c.brandSlug,
        c.brandName,
        c.publicTab,
        c.section,
        c.slotKey || "",
        c.field,
        c.claimType,
        c.claimText,
        c.priority,
        c.priorityBand,
        focus,
      ]
        .map(esc)
        .join(",")
    );
  }
  return `${lines.join("\n")}\n`;
}

export function mergeWebhoundDatasetRows(claims, datasetRows = []) {
  const byId = new Map(claims.map((c) => [c.claimId, c]));
  let matched = 0;
  for (const row of datasetRows) {
    const id = nz(row.claim_id || row.claimId);
    const claim = byId.get(id);
    if (!claim) continue;
    matched += 1;
    const result = nz(row.validation_result || row.validationResult).toLowerCase();
    claim.validationResult = CLAIM_RESULT_CLASSES.includes(result) ? result : "partially_supported";
    claim.sourceFound = nz(row.source_title || row.sourceFound) || null;
    claim.sourceUrl = nz(row.source_url || row.sourceUrl) || null;
    claim.sourceCategory =
      nz(row.source_category || row.sourceCategory || row.source_tier || row.sourceTier) || null;
    claim.sourceTier = nz(row.source_tier || row.sourceTier) || claim.sourceCategory;
    claim.confidence = nz(row.confidence) || null;
    const actionRaw = nz(row.recommended_action || row.recommendedAction)
      .toLowerCase()
      .replace(/\s+/g, "_");
    const actionAliases = {
      keep: "keep",
      soften: "soften",
      replace: "replace_with_sourced_wording",
      replace_with_sourced_wording: "replace_with_sourced_wording",
      remove: "remove",
      needs_steward_review: "needs_steward_review",
      steward_review: "needs_steward_review",
    };
    claim.recommendedAction =
      actionAliases[actionRaw] || mapRecommendedAction(claim.validationResult, claim.claimType);
    claim.riskLevel =
      nz(row.risk_level || row.riskLevel) ||
      riskLevelForResult(claim.validationResult, claim.priorityBand);
    claim.publicCopyChangeLater =
      ["remove", "soften", "replace_with_sourced_wording"].includes(claim.recommendedAction) ||
      String(row.public_copy_change_later ?? row.publicCopyChangeLater ?? "")
        .toLowerCase()
        .trim() === "true" ||
      String(row.public_copy_change_later ?? "").toLowerCase() === "yes";
    claim.notes = nz(row.notes) || claim.notes;
    if (nz(row.public_tab || row.publicTab)) claim.publicTab = nz(row.public_tab || row.publicTab);
  }
  return { matched, claims: [...byId.values()] };
}

export function buildRemediationQueue(claims) {
  const actionable = new Set([
    "unsupported",
    "stale",
    "overclaimed",
    "needs_softening",
    "conflicting_sources",
    "partially_supported",
    "remove_candidate",
  ]);
  return claims
    .filter((c) => actionable.has(c.validationResult))
    .sort((a, b) => (a.priorityBand || 9) - (b.priorityBand || 9))
    .map((c, i) => ({
      id: `claim-rem-${i + 1}`,
      priorityBand: c.priorityBand,
      priority: c.priority,
      riskLevel: c.riskLevel || riskLevelForResult(c.validationResult, c.priorityBand),
      brand: c.brandName,
      brandSlug: c.brandSlug,
      publicTab: c.publicTab,
      section: c.section,
      claimId: c.claimId,
      claimType: c.claimType,
      claimText: c.claimText,
      sourceFound: c.sourceFound,
      sourceCategory: c.sourceCategory || c.sourceTier,
      sourceUrl: c.sourceUrl,
      validationResult: c.validationResult,
      recommendedAction:
        c.recommendedAction || mapRecommendedAction(c.validationResult, c.claimType),
      publicCopyChangeLater: c.publicCopyChangeLater === true,
      remediateNow: false,
    }));
}

export function assembleValidationReport({
  frozen,
  liveUniverseCount,
  brandsChecked,
  claims,
  webhoundPack,
  webhoundMeta = null,
}) {
  const counts = Object.fromEntries(CLAIM_RESULT_CLASSES.map((k) => [k, 0]));
  for (const c of claims) {
    const k = counts[c.validationResult] != null ? c.validationResult : "pending_webhound";
    counts[k] = (counts[k] || 0) + 1;
  }
  const remediationQueue = buildRemediationQueue(claims);
  const sourceGaps = claims.filter(
    (c) =>
      c.webhoundSelected &&
      (c.validationResult === "unsupported" ||
        c.validationResult === "remove_candidate" ||
        c.validationResult === "pending_webhound" ||
        (!c.sourceUrl &&
          ["partially_supported", "needs_softening", "stale"].includes(c.validationResult)))
  );

  const packByBand = {};
  for (const c of webhoundPack) {
    const label = priorityLabel(c.priorityBand);
    packByBand[label] = (packByBand[label] || 0) + 1;
  }

  const webhoundMerged = Number(webhoundMeta?.datasetRowsMerged || 0) > 0;
  const status = webhoundMerged ? VALIDATION_STATUS : VALIDATION_STATUS_PACK_READY;

  return {
    version: VALIDATION_VERSION,
    generatedAt: new Date().toISOString(),
    status,
    mode: "readonly",
    airtableWrites: false,
    brandExplorerWrites: false,
    brandSetupWrites: false,
    censusWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    companyValidatedWrites: false,
    brandVerifiedWrites: false,
    recentMomentumWrites: false,
    remediateNow: false,
    webhoundIsNotSourceOfTruth: true,
    evidenceSourceOfTruth: "underlying_public_pages",
    validationPriority: {
      P1: "public_tabs_public_facing_factual_claims",
      P2: "recent_momentum",
      P3: "property_examples",
      P4: "parent_brand_family_soft_brand_loyalty",
      deprioritized: ["broad_footprint_regional_presence", "generic_guest_psychographics", "advisory_operator_fit"],
    },
    momentumValidationRules: [...MOMENTUM_VALIDATION_RULES],
    freezeDecision: frozen.freezeDecision,
    freezeUnchanged: frozen.freezeDecision === FREEZE_DECISION_62 && frozen.frozen === true,
    activeUniverse: liveUniverseCount,
    excludedFromScope: {
      brands: [
        "four-points-flex-by-sheraton",
        "the-house-of-originals",
        "morgans-originals",
        "radisson-collection",
      ],
      note: "Held/excluded / Under Review brands excluded. Presentation public-facing claims only.",
    },
    sourceHierarchy: [...SOURCE_HIERARCHY],
    rejectSourceTypes: [...REJECT_SOURCE_TYPES],
    remediationActions: [...REMEDIATION_ACTIONS],
    webhound: webhoundMeta,
    summary: {
      brandsChecked: brandsChecked.length,
      claimsExtracted: claims.length,
      factualCandidates: claims.filter((c) => c.factuality === "factual_candidate").length,
      notFactualClaims: counts.not_factual_claim,
      webhoundPackSize: webhoundPack.length,
      packByPriorityBand: packByBand,
      supportedOfficial: counts.supported_official,
      supportedTrustedSecondary: counts.supported_trusted_secondary,
      partiallySupported: counts.partially_supported,
      unsupported: counts.unsupported,
      stale: counts.stale,
      overclaimed: counts.overclaimed,
      needsSoftening: counts.needs_softening,
      removeCandidate: counts.remove_candidate,
      conflictingSources: counts.conflicting_sources,
      pendingWebhound: counts.pending_webhound,
      remediationCount: remediationQueue.length,
      sourceGapCount: sourceGaps.length,
    },
    resultCounts: counts,
    brandsChecked,
    claims,
    webhoundPackClaimIds: webhoundPack.map((c) => c.claimId),
    remediationQueue,
    sourceGaps: sourceGaps.map((c) => ({
      claimId: c.claimId,
      brandSlug: c.brandSlug,
      publicTab: c.publicTab,
      claimType: c.claimType,
      priority: c.priority,
      validationResult: c.validationResult,
      claimText: c.claimText,
    })),
    confirmations: {
      readOnlyMode: true,
      activeUniverseRemains62: liveUniverseCount === 62,
      frozenBaselineUnchanged: frozen.freezeDecision === FREEZE_DECISION_62,
      noBrandExplorerWrites: true,
      noBrandSetupWrites: true,
      noHotelPropertyCensusWrites: true,
      noBrandStatusChanges: true,
      noReleaseFieldChanges: true,
      noCompanyValidatedOrBrandVerifiedWrites: true,
      noRemediationApplied: true,
      webhoundNotTreatedAsSourceOfTruth: true,
      priorityPublicTabsAndRecentMomentum: true,
    },
  };
}

export function writeClaimValidationArtifacts(report, { packClaims = [] } = {}) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD);
  const docsPath = path.join(DOCS_DIR, DOCS_MD);
  const packJsonPath = path.join(REPORTS_DIR, CLAIM_PACK_JSON);
  const packCsvPath = path.join(REPORTS_DIR, CLAIM_PACK_CSV);
  const priorityJsonPath = path.join(REPORTS_DIR, PRIORITY_PACK_JSON);
  const priorityCsvPath = path.join(REPORTS_DIR, PRIORITY_PACK_CSV);

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  const packPayload = {
    generatedAt: report.generatedAt,
    version: report.version,
    priority: report.validationPriority,
    momentumRules: report.momentumValidationRules,
    claims: packClaims,
  };
  fs.writeFileSync(packJsonPath, `${JSON.stringify(packPayload, null, 2)}\n`);
  fs.writeFileSync(packCsvPath, claimsToCsv(packClaims));
  fs.writeFileSync(priorityJsonPath, `${JSON.stringify(packPayload, null, 2)}\n`);
  fs.writeFileSync(priorityCsvPath, claimsToCsv(packClaims));
  const md = renderMarkdown(report);
  fs.writeFileSync(mdPath, md);
  fs.writeFileSync(docsPath, md);
  return {
    jsonPath,
    mdPath,
    docsPath,
    packJsonPath,
    packCsvPath,
    priorityJsonPath,
    priorityCsvPath,
  };
}

function renderMarkdown(report) {
  const s = report.summary || {};
  const lines = [];
  lines.push("# Brand Explorer 62 — Webhound Claim Validation (Read-Only)");
  lines.push("");
  lines.push(`**Status:** \`${report.status}\``);
  lines.push(`**Generated:** ${report.generatedAt}`);
  lines.push(`**Version:** \`${report.version}\``);
  lines.push(`**Freeze:** \`${report.freezeDecision}\` · unchanged=${report.freezeUnchanged}`);
  lines.push(
    `**Mode:** read-only · Webhound is **not** SoT · evidence = underlying public pages · **no remediation applied**`
  );
  lines.push("");
  lines.push("## Validation priority");
  lines.push("");
  lines.push("1. **P1 — Public tabs** — owner value, positioning, conversion/development fit, operator compatibility (factual), guest psychographics (factual only), portfolio/property refs, loyalty/distribution, parent/family, soft-brand, public watchouts");
  lines.push("2. **P2 — Recent Momentum** — dated opening/signing/development/conversion/renovation/pipeline/property proof; exact brand/property match; no directory-only or vague growth");
  lines.push("3. **P3 — Property examples** — exists, brand association, current name, city/country, not former/stale affiliation");
  lines.push("4. **P4 — Parent / brand family** — parent, collection, soft-brand, loyalty/distribution platform");
  lines.push("5. **Deprioritized** — broad footprint/regional presence; generic guest framing; advisory operator-fit without factual capability");
  lines.push("");
  lines.push("## Verdict");
  lines.push("");
  lines.push(
    `Checked **${s.brandsChecked}** Active-62 brands. Extracted **${s.claimsExtracted}** claim units (**${s.factualCandidates}** factual candidates). Webhound pack size **${s.webhoundPackSize}** (${JSON.stringify(s.packByPriorityBand || {})}). Supported official **${s.supportedOfficial}** · trusted secondary **${s.supportedTrustedSecondary}** · partial **${s.partiallySupported}** · unsupported **${s.unsupported}** · stale **${s.stale}** · overclaim **${s.overclaimed}** · soften **${s.needsSoftening}** · remove_candidate **${s.removeCandidate}** · conflicting **${s.conflictingSources}** · pending **${s.pendingWebhound}**. Remediation queue **${s.remediationCount}**.`
  );
  lines.push("");
  if (report.webhound?.sessionId || report.webhound?.sessions) {
    lines.push("## Webhound session(s)");
    lines.push("");
    if (report.webhound?.sessionId) {
      lines.push(`- Primary: \`${report.webhound.sessionId}\`${report.webhound.url ? ` · ${report.webhound.url}` : ""}`);
    }
    for (const sRow of report.webhound?.sessions || []) {
      lines.push(`- ${sRow.role || "session"}: \`${sRow.sessionId}\`${sRow.url ? ` · ${sRow.url}` : ""}`);
    }
    if (report.webhound.budgetUsd != null) lines.push(`- Budget: $${report.webhound.budgetUsd}`);
    if (report.webhound.spendUsd != null) lines.push(`- Spend: $${report.webhound.spendUsd}`);
    lines.push("");
  }
  lines.push("## Confirmations");
  lines.push("");
  for (const [k, v] of Object.entries(report.confirmations || {})) {
    lines.push(`- \`${k}\`: **${v}**`);
  }
  lines.push("");
  lines.push("## Claim remediation queue (top 40 — queue only, no writes)");
  lines.push("");
  for (const item of (report.remediationQueue || []).slice(0, 40)) {
    lines.push(
      `- **[${item.riskLevel}/${item.priority}]** ${item.brandSlug} · ${item.publicTab} · ${item.validationResult} · action=${item.recommendedAction} · ${String(item.claimText).slice(0, 120)}`
    );
  }
  if ((report.remediationQueue || []).length > 40) {
    lines.push(`- … +${report.remediationQueue.length - 40} more (see JSON)`);
  }
  if (!(report.remediationQueue || []).length) {
    lines.push("- _(empty until Webhound rows are merged)_");
  }
  lines.push("");
  lines.push("## Policy");
  lines.push("");
  lines.push("- No Brand Explorer / Brand Setup / Census / Brand Status / release / CV writes in this lane.");
  lines.push("- Do not remediate copy in this lane — remediation queue only.");
  lines.push("- Reject OTA / affiliate / generic travel blogs / unsourced AI summaries / directory-only as evidence.");
  lines.push("- Preferred sources follow official parent → brand → property → press → trusted secondary.");
  lines.push("");
  lines.push(`**Final status:** \`${report.status}\``);
  lines.push("");
  return `${lines.join("\n")}\n`;
}
