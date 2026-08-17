/**
 * Radisson Blu by Choice narrow brand extraction (dry-run preview + controlled apply).
 * Americas / Choice-controlled sources only — see docs/data-intelligence/radisson-blu-extraction-plan.md
 */
import { randomUUID } from "crypto";
import {
  MAP_PARTNER_FACT,
  MAP_PARTNER_SOURCE,
  PARTNER_INTELLIGENCE_GAP_COPY,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  getRegistryField,
  listBrandFieldsForExtractionRegistry,
} from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import { extractFromBrandSourceDocument } from "./run-extraction.js";
import { loadSourceDocumentText } from "./extract-source-text.js";
import { createPartnerFact } from "./airtable-facts.js";
import { patchPartnerSource } from "./airtable-source.js";
import { isSourceExtractable } from "./sync-reference-folder.js";

export const RADISSON_BLU_BRAND_ID = "recWPEvxBQxVVzSq3";
export const REPORT_JSON_NAME = "radisson-blu-extract.json";
export const REPORT_MD_NAME = "radisson-blu-extract.md";
export const RB_EXTRACT_BATCH_NOTE = "Radisson Blu by Choice narrow extraction allowlist (Americas).";
export const RB_EXTRACT_FACT_NOTE = "Radisson Blu by Choice narrow extraction allowlist (Americas).";

export const ALLOWLISTED_SOURCE_IDS = [
  "recC9utJdNaKWR56k",
  "recH1ZepKU6zJp7M2",
  "recWGLvwnDn0v5rmL",
  "reczafLghta09o2sB",
];

export const ALLOWLISTED_SOURCE_ID_SET = new Set(ALLOWLISTED_SOURCE_IDS);

export const PRESS_KIT_SOURCE_ID = "recWGLvwnDn0v5rmL";
/** Salesforce LWC shell — URL provenance only; not extractable. */
export const DEVELOPMENT_WEB_SOURCE_ID = "recC9utJdNaKWR56k";
/** @deprecated use DEVELOPMENT_WEB_SOURCE_ID */
export const DEVELOPMENT_SOURCE_ID = DEVELOPMENT_WEB_SOURCE_ID;
/** Primary owner/development evidence (Choice company one-pager PDF). */
export const ONE_PAGER_SOURCE_ID = "reczafLghta09o2sB";

/** Field keys where one-pager wins cross-source dedupe over web captures. */
export const ONE_PAGER_PRIORITY_KEYS = new Set([
  "be.overview.developmentModel",
  "be.overview.whyValue",
  "be.overview.typicalUseCase",
  "be.positioning.tagline",
  "be.positioning.summary",
  "be.positioning.guestPromise",
]);

/** User-requested keys → supported registry keys (for reporting). */
export const USER_REQUESTED_KEY_MAPPING = [
  { requested: "be.snapshot.brandName", registryKey: "be.identity.brandName", supported: true },
  { requested: "be.snapshot.parentCompany", registryKey: "be.identity.parentCompany", supported: true },
  {
    requested: "be.positioning.summary",
    registryKey: "be.positioning.summary",
    supported: true,
  },
  {
    requested: "be.positioning.segment",
    registryKey: null,
    supported: false,
    note: "No brand PI registry key; capture segment language in be.positioning.summary.",
  },
  {
    requested: "be.positioning.chainScale",
    registryKey: null,
    supported: false,
    note: "Hotel Chain Scale is Brand Setup — not a PI fact key; use upper-upscale in be.positioning.summary.",
  },
  {
    requested: "be.ownerConsiderations.developmentPositioning",
    registryKey: "be.overview.whyValue",
    supported: true,
  },
  {
    requested: "be.standards.conversionConsiderations",
    registryKey: "be.overview.developmentModel",
    supported: true,
  },
  {
    requested: "be.markets.regionsSupported",
    registryKey: "be.footprint.americasHotels",
    supported: true,
    note: "Brand registry uses be.footprint.americasHotels / geoIntro — not op.markets.regionsSupported.",
  },
  {
    requested: "be.brandFamily.context",
    registryKey: null,
    supported: false,
    note: "No registry key; ownership disclaimer may inform be.footprint.geoIntro when source-backed.",
  },
  {
    requested: "be.development.model",
    registryKey: "be.overview.developmentModel",
    supported: true,
  },
];

export const PRIMARY_TARGET_FACT_KEYS = [
  "be.identity.brandName",
  "be.identity.parentCompany",
  "be.positioning.summary",
  "be.positioning.tagline",
  "be.positioning.guestPromise",
  "be.overview.developmentModel",
  "be.overview.whyValue",
];

export const SECONDARY_TARGET_FACT_KEYS = [
  "be.positioning.history",
  "be.overview.typicalUseCase",
  "be.footprint.americasHotels",
  "be.footprint.geoIntro",
  "be.loyalty.programName",
];

export const DEFAULT_TARGET_FACT_KEYS = [
  ...PRIMARY_TARGET_FACT_KEYS,
  ...SECONDARY_TARGET_FACT_KEYS,
];

export const TARGET_KEY_PRIORITY = {
  "be.identity.brandName": "P0",
  "be.identity.parentCompany": "P0",
  "be.positioning.summary": "P0",
  "be.positioning.tagline": "P0",
  "be.positioning.guestPromise": "P0",
  "be.overview.developmentModel": "P1",
  "be.overview.whyValue": "P1",
  "be.positioning.history": "P2",
  "be.overview.typicalUseCase": "P2",
  "be.footprint.americasHotels": "P1",
  "be.footprint.geoIntro": "P2",
  "be.loyalty.programName": "P2",
};

const MARKUP_RE = /<!DOCTYPE|<html[\s>]|<meta[\s>]|<\/\w+>/i;
const BOOKING_NOISE_RE =
  /\b(Check availability|Book now|Find a hotel|Select dates|Room count|Adults|Children)\b/i;
const RHG_GLOBAL_RE =
  /\b(390\+?\s*(hotels?|Radisson Blu)|largest upper[- ]upscale brand in Europe|Europe.s leading upper[- ]upscale|over 390 Radisson Blu)\b/i;
const VAGUE_MARKETING_RE =
  /\b(world[- ]class|unparalleled|synerg(y|ies)|best[- ]in[- ]class)\b/i;

const REGION_OWNERSHIP_CAVEATS = [
  "PI package scoped to Radisson Blu by Choice (Americas) — recWPEvxBQxVVzSq3.",
  "Choice Hotels owns/franchises Radisson Blu in the Americas; Radisson Hotel Group operates the brand elsewhere.",
  "Do not import RHG global portfolio counts unless a Choice source states Americas scope.",
  "Separate Brand Basics row exists for RHG-global Radisson Blu — do not cross-link sources.",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeValue(v) {
  return nz(v).toLowerCase().replace(/\s+/g, " ").trim();
}

export function parseIdList(raw) {
  if (!raw) return [];
  return String(raw)
    .split(/[,\s]+/)
    .map((s) => s.trim())
    .filter((s) => /^rec[a-zA-Z0-9]+$/.test(s));
}

export function parseFactKeyList(raw) {
  if (!raw) return [];
  return String(raw)
    .split(/[,\s]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export function assertSourceAllowlisted(sourceId) {
  const id = nz(sourceId);
  if (!id) return { ok: false, reasons: ["missing_source_id"] };
  if (!ALLOWLISTED_SOURCE_ID_SET.has(id)) {
    return { ok: false, reasons: ["source_not_in_radisson_blu_allowlist"] };
  }
  return { ok: true, reasons: [] };
}

export function resolveSourceIds(cliSourceIds) {
  const requested = cliSourceIds.length ? cliSourceIds : [...ALLOWLISTED_SOURCE_IDS];
  const invalid = [];
  for (const id of requested) {
    const check = assertSourceAllowlisted(id);
    if (!check.ok) invalid.push({ id, reasons: check.reasons });
  }
  if (invalid.length) {
    const msg = invalid.map((row) => `${row.id} (${row.reasons.join(", ")})`).join("; ");
    throw new Error(`Source allowlist violation: ${msg}`);
  }
  return [...new Set(requested)];
}

export function resolveTargetFactKeys(cliKeys) {
  return cliKeys.length ? [...new Set(cliKeys)] : [...DEFAULT_TARGET_FACT_KEYS];
}

export function buildTargetKeyPlan(factKeys) {
  const registry = listBrandFieldsForExtractionRegistry();
  const registryByKey = new Map(registry.map((f) => [f.fieldKey, f]));

  return factKeys.map((fieldKey) => {
    const reg = registryByKey.get(fieldKey) || getRegistryField(fieldKey, "Brand Explorer");
    const priority =
      TARGET_KEY_PRIORITY[fieldKey] ||
      (PRIMARY_TARGET_FACT_KEYS.includes(fieldKey) ? "P0" : "P2");
    return {
      fieldKey,
      priority,
      registrySupported: Boolean(reg),
      displayLabel: reg?.displayLabel || null,
      explorerSection: reg?.explorerSection || null,
    };
  });
}

export function validateHtmlTextClean(doc) {
  if (!doc || doc.kind !== "html") return { ok: true, reasons: [] };
  const text = nz(doc.text);
  if (!text) return { ok: false, reasons: ["empty_html_text"] };
  if (MARKUP_RE.test(text.slice(0, 200))) {
    return { ok: false, reasons: ["html_markup_not_stripped"] };
  }
  return { ok: true, reasons: [] };
}

export function isRhgGlobalClaim(fieldKey, value, evidence) {
  const blob = `${value} ${evidence}`;
  if (RHG_GLOBAL_RE.test(blob) && !/\bAmericas\b/i.test(evidence)) return true;
  if (
    fieldKey === "be.identity.parentCompany" &&
    /\bRadisson Hotel Group\b/i.test(value) &&
    !/\bChoice Hotels\b/i.test(evidence)
  ) {
    return true;
  }
  if (
    (fieldKey === "be.footprint.globalHotels" ||
      fieldKey === "be.footprint.globalPipeline" ||
      fieldKey === "be.footprint.globalRooms") &&
    !/\bAmericas\b/i.test(evidence)
  ) {
    return true;
  }
  return false;
}

export function assessCandidateQuality(candidate) {
  const reasons = [];
  const value = nz(candidate.extractedValue);
  const evidence = nz(candidate.evidenceText);
  const fieldKey = candidate.fieldKey;

  if (!value) reasons.push("empty_value");
  if (value === PARTNER_INTELLIGENCE_GAP_COPY) reasons.push("gap_copy");
  if (candidate.dataGap === "Yes" || candidate._extractionSkipped) reasons.push("gap_or_skipped");
  if (MARKUP_RE.test(value) || MARKUP_RE.test(evidence)) reasons.push("markup_artifact");
  if (!evidence || evidence === PARTNER_INTELLIGENCE_GAP_COPY) reasons.push("weak_evidence");
  if (value.length < 3) reasons.push("value_too_short");
  if (BOOKING_NOISE_RE.test(value) && value.length < 150) reasons.push("booking_boilerplate");
  if (VAGUE_MARKETING_RE.test(value) && value.length < 80) reasons.push("vague_marketing");
  if (isRhgGlobalClaim(fieldKey, value, evidence)) reasons.push("rhg_global_or_wrong_region");
  if (candidate._registryFallback && fieldKey === "be.identity.parentCompany") {
    reasons.push("registry_fallback_blocked_for_parent");
  }

  return { clean: reasons.length === 0, reasons };
}

function pushCandidate(out, candidate) {
  out.push({
    extractionType: "Directly Stated",
    confidenceLevel: candidate.confidenceLevel || "High",
    confidenceScore: candidate.confidenceScore ?? 80,
    dataGap: "No",
    ...candidate,
  });
}

function extractSentence(text, re) {
  const m = text.match(re);
  return m ? m[0].trim() : null;
}

function inferBrandName(source, doc) {
  if (/\bRadisson Blu\b/i.test(doc.text)) {
    return {
      value: "Radisson Blu",
      evidence: extractSentence(doc.text, /Radisson Blu[^.]{0,80}\./i) || "Radisson Blu",
    };
  }
  return null;
}

function inferParentCompany(text) {
  const owned = extractSentence(
    text,
    /owned in the Americas regions by Choice Hotels[^.]{0,120}\./i
  );
  if (owned) {
    return { value: "Choice Hotels International", evidence: owned };
  }
  const franchised = extractSentence(
    text,
    /Franchised in the Americas by Choice Hotels International[^.]{0,120}\./i
  );
  if (franchised) {
    return { value: "Choice Hotels International", evidence: franchised };
  }
  return null;
}

function inferPositioningSummary(text) {
  const upper = extractSentence(text, /upper[- ]upscale[^.]{0,160}\./i);
  if (upper && /\bRadisson Blu\b/i.test(text)) {
    return {
      value:
        "Upper-upscale hospitality brand combining style with substance, innovation with comfort, and a sense of belonging in an elevated environment.",
      evidence: upper,
    };
  }
  const redefine = extractSentence(
    text,
    /redefine[s]? the upper[- ]upscale hospitality experience[^.]{0,120}\./i
  );
  if (redefine) return { value: redefine.replace(/\s+/g, " "), evidence: redefine };
  return null;
}

function inferTagline(text) {
  const m = text.match(/Think in Black\s*&\s*White Blu/i);
  if (m) return { value: "Think in Black & White Blu", evidence: m[0] };
  return null;
}

function inferGuestPromise(text) {
  const m = extractSentence(
    text,
    /combines style with substance, innovation with comfort[^.]{0,120}\./i
  );
  if (m) return { value: m.replace(/\s+/g, " "), evidence: m };
  const born = extractSentence(
    text,
    /Born as the first design hotel[^.]{0,160}\./i
  );
  if (born) return { value: born.replace(/\s+/g, " "), evidence: born };
  return null;
}

function inferDevelopmentModel(text) {
  const parts = [];
  if (/new build|new construction/i.test(text)) parts.push("new construction");
  if (/conversion/i.test(text)) parts.push("conversions");
  if (/adaptive reuse/i.test(text)) parts.push("adaptive reuse");
  if (!parts.length) return null;
  const evidence =
    extractSentence(text, /(new build|new construction|conversion|adaptive reuse)[^.]{0,120}\./i) ||
    parts.join(", ");
  return {
    value: `Development types include ${parts.join(", ")} (per Choice brand materials).`,
    evidence,
  };
}

function inferWhyValue(text, sourceId) {
  if (sourceId === ONE_PAGER_SOURCE_ID) {
    const ownerDev = extractSentence(
      text,
      /Whether new build, adaptive reuse or conversion, Radisson Blu[^.]{0,160}\./i
    );
    if (ownerDev) return { value: ownerDev.replace(/\s+/g, " "), evidence: ownerDev };
    const markets = extractSentence(
      text,
      /brings distinctive design to top urban and resort[^.]{0,80}\./i
    );
    if (markets) return { value: markets.replace(/\s+/g, " "), evidence: markets };
  }
  const m = extractSentence(text, /owner[- ]first philosophy[^.]{0,120}\./i);
  if (m) return { value: m.replace(/\s+/g, " "), evidence: m };
  const flex = extractSentence(text, /flexible brand framework[^.]{0,120}\./i);
  if (flex) return { value: flex.replace(/\s+/g, " "), evidence: flex };
  return null;
}

function inferTypicalUseCase(text, sourceId) {
  if (sourceId !== ONE_PAGER_SOURCE_ID) return null;
  const targetGuest = extractSentence(text, /TARGET GUEST[\s\S]{0,40}?(The Inspired Professional)/i);
  if (targetGuest) {
    return {
      value:
        "The Inspired Professional — upper-upscale guest seeking distinctive, non-boring hospitality experiences.",
      evidence: targetGuest.replace(/\s+/g, " ").trim(),
    };
  }
  const inspired = extractSentence(text, /appeals to the inspired professional[^.]{0,120}\./i);
  if (inspired) return { value: inspired.replace(/\s+/g, " "), evidence: inspired };
  return null;
}

function inferAmericasHotels(text, sourceId) {
  const pressKitMatch = text.match(
    /(\d+)\s+hotels?\s+with\s+a\s+combined\s+([\d,]+)\s+rooms?\s+in\s+operation\s+a?\s*in\s+the\s+Americas[^.]{0,60}\./i
  );
  if (pressKitMatch) {
    if (sourceId === PRESS_KIT_SOURCE_ID || /\bSeptember 30, 2024\b/i.test(text)) {
      return {
        value: `${pressKitMatch[1]} hotels (${pressKitMatch[2]} rooms) in operation in the Americas`,
        evidence: pressKitMatch[0].trim(),
      };
    }
  }

  if (sourceId === ONE_PAGER_SOURCE_ID) {
    const countMatch = text.match(/(\d+)\s+in\s+the\s+Americas/i);
    if (!countMatch) return null;
    const domestic = text.match(/(\d+)\s+Domestic/i);
    const international = text.match(/(\d+)\s+International/i);
    let value = `${countMatch[1]} hotels in the Americas`;
    if (domestic && international) {
      value += ` (${domestic[1]} domestic, ${international[1]} international)`;
    }
    const evidence =
      extractSentence(text, /AMERICAS BRAND PRESENCE[\s\S]{0,120}/i) ||
      countMatch[0].trim();
    return { value, evidence: String(evidence).replace(/\s+/g, " ").trim().slice(0, 320) };
  }

  return null;
}

function inferGeoIntro(text, sourceId) {
  if (sourceId !== PRESS_KIT_SOURCE_ID) return null;
  const m = extractSentence(
    text,
    /Outside of the Americas, the brands are owned by Radisson Hotel Group[^.]{0,80}\./i
  );
  if (m) {
    return {
      value:
        "Americas: Radisson Blu franchised by Choice Hotels International. Outside the Americas: Radisson Hotel Group (separate company).",
      evidence: m,
    };
  }
  return null;
}

function inferLoyalty(text) {
  if (/\bChoice Privileges\b/i.test(text)) {
    return {
      value: "Choice Privileges",
      evidence: extractSentence(text, /Choice Privileges[^.]{0,80}\./i) || "Choice Privileges",
    };
  }
  return null;
}

export function enrichRadissonBluCandidates(candidates, source, doc) {
  const out = [...candidates];
  const text = nz(doc.text);
  const anchor = source.localFilePath || source.sourceUrl || source.sourceTitle;
  const hasClean = (key) =>
    out.some((c) => c.fieldKey === key && assessCandidateQuality(c).clean);

  const enrichments = [
    ["be.identity.brandName", inferBrandName(source, doc)],
    ["be.identity.parentCompany", inferParentCompany(text)],
    ["be.positioning.summary", inferPositioningSummary(text)],
    ["be.positioning.tagline", inferTagline(text)],
    ["be.positioning.guestPromise", inferGuestPromise(text)],
    ["be.overview.developmentModel", inferDevelopmentModel(text)],
    ["be.overview.whyValue", inferWhyValue(text, source.id)],
    ["be.overview.typicalUseCase", inferTypicalUseCase(text, source.id)],
    ["be.footprint.americasHotels", inferAmericasHotels(text, source.id)],
    ["be.footprint.geoIntro", inferGeoIntro(text, source.id)],
    ["be.loyalty.programName", inferLoyalty(text)],
  ];

  for (const [fieldKey, hit] of enrichments) {
    if (!hit || hasClean(fieldKey)) continue;
    const reg = getRegistryField(fieldKey, "Brand Explorer");
    pushCandidate(out, {
      fieldKey,
      explorerSection: reg?.explorerSection || null,
      displayLabel: reg?.displayLabel || fieldKey,
      extractedValue: hit.value,
      normalizedValue: hit.value,
      evidenceText: hit.evidence,
      pageSectionAnchor: anchor,
      _sourceId: source.id,
      _sourceTitle: source.sourceTitle,
      _enriched: true,
    });
  }

  return out;
}

function filterRegistryForTargetKeys(registryFields, targetKeys) {
  const keySet = new Set(targetKeys);
  return registryFields.filter((f) => keySet.has(f.fieldKey));
}

function pickByTargetKeys(tagged, targetKeys) {
  const keySet = new Set(targetKeys);
  return tagged.filter((c) => keySet.has(c.fieldKey));
}

export function filterCleanCandidates(candidates, options = {}) {
  const skipped = [];
  const clean = [];

  for (const c of candidates) {
    const assessment = assessCandidateQuality(c);
    if (!assessment.clean) {
      skipped.push({
        fieldKey: c.fieldKey,
        sourceId: c._sourceId,
        extractedValuePreview: String(c.extractedValue || "").slice(0, 160),
        reasons: assessment.reasons,
      });
      continue;
    }
    clean.push(c);
  }

  const deduped = dedupeCandidates(clean, skipped);
  const sorted = [...deduped].sort((a, b) => {
    const pa = TARGET_KEY_PRIORITY[a.fieldKey] || "P9";
    const pb = TARGET_KEY_PRIORITY[b.fieldKey] || "P9";
    return pa.localeCompare(pb);
  });

  const limited =
    typeof options.limitFacts === "number" && options.limitFacts > 0
      ? sorted.slice(0, options.limitFacts)
      : sorted;

  if (limited.length < sorted.length) {
    for (const dropped of sorted.slice(limited.length)) {
      skipped.push({
        fieldKey: dropped.fieldKey,
        sourceId: dropped._sourceId,
        extractedValuePreview: String(dropped.extractedValue || "").slice(0, 160),
        reasons: ["limit_facts_cap"],
      });
    }
  }

  return { clean: limited, skipped };
}

function dedupeCandidates(candidates, skipped) {
  const byKeyValue = new Map();
  for (const c of candidates) {
    const compound = `${c.fieldKey}::${normalizeValue(c.extractedValue)}`;
    const existing = byKeyValue.get(compound);
    if (!existing) {
      byKeyValue.set(compound, c);
      continue;
    }
    const drop = (c.confidenceScore || 0) > (existing.confidenceScore || 0) ? existing : c;
    skipped.push({
      fieldKey: drop.fieldKey,
      sourceId: drop._sourceId,
      extractedValuePreview: String(drop.extractedValue || "").slice(0, 160),
      reasons: ["duplicate_same_value_same_key"],
    });
    byKeyValue.set(
      compound,
      (c.confidenceScore || 0) > (existing.confidenceScore || 0) ? c : existing
    );
  }
  return [...byKeyValue.values()];
}

export function validateSourceForRadissonBluExtract(source, doc) {
  const reasons = [];
  const allow = assertSourceAllowlisted(source.id);
  if (!allow.ok) reasons.push(...allow.reasons);

  if (source.brandId !== RADISSON_BLU_BRAND_ID) {
    reasons.push("not_linked_to_radisson_blu_brand");
  }
  if (source.status === "Stale") reasons.push("source_status_stale");
  if (!isSourceExtractable(source)) reasons.push("source_not_extractable_no_file_or_url");

  const htmlCheck = validateHtmlTextClean(doc);
  if (!htmlCheck.ok) reasons.push(...htmlCheck.reasons);

  return { ok: reasons.length === 0, reasons };
}

export function summarizeExistingFacts(facts, sourceIds, targetKeys) {
  const sourceSet = new Set(sourceIds);
  const keySet = new Set(targetKeys);
  const rows = [];

  for (const fact of facts || []) {
    if (fact.brandId !== RADISSON_BLU_BRAND_ID) continue;
    if (!sourceSet.has(fact.sourceRecordId)) continue;
    if (!keySet.has(fact.fieldName)) continue;

    let row = rows.find(
      (r) => r.sourceId === fact.sourceRecordId && r.fieldKey === fact.fieldName
    );
    if (!row) {
      row = { sourceId: fact.sourceRecordId, fieldKey: fact.fieldName, existingCount: 0, factIds: [] };
      rows.push(row);
    }
    row.existingCount += 1;
    row.factIds.push(fact.id);
  }

  return { rows, existingCount: rows.reduce((n, r) => n + r.existingCount, 0) };
}

export function assessExtractionQuality(sourcePreviews, wouldWrite) {
  const substantiveKeys = new Set([
    "be.positioning.summary",
    "be.identity.parentCompany",
    "be.positioning.guestPromise",
    "be.overview.developmentModel",
    "be.footprint.americasHotels",
  ]);
  const proposed = wouldWrite.factRowsWouldCreate || [];
  const substantive = proposed.filter((f) => substantiveKeys.has(f.fieldKey));
  const allHtmlClean = sourcePreviews.every((p) => p.htmlTextClean !== false);
  const readableSources = sourcePreviews.filter(
    (p) => p.previewAvailable && (p.textLength || 0) >= 500
  ).length;

  return {
    allHtmlTextClean: allHtmlClean,
    readableSourceCount: readableSources,
    substantiveFactCount: substantive.length,
    hasBrandName: proposed.some((f) => f.fieldKey === "be.identity.brandName"),
    hasParentCompany: proposed.some((f) => f.fieldKey === "be.identity.parentCompany"),
    hasAmericasFootprint: proposed.some((f) => f.fieldKey === "be.footprint.americasHotels"),
    hasOwnershipCaveat: proposed.some((f) => f.fieldKey === "be.footprint.geoIntro"),
    overall:
      allHtmlClean && substantive.length >= 4 && proposed.length >= 6
        ? "good_for_steward_review"
        : substantive.length >= 2
          ? "moderate_review_carefully"
          : "weak_needs_more_sources_or_curation",
    applyRecommended:
      allHtmlClean && substantive.length >= 4 && proposed.length >= 6,
    governancePublishStillBlocked: true,
    governanceBlockReason:
      "Facts must be created, human-approved, and governance publish dry-run must pass. No governance apply from this script.",
    platformIntelligenceNotes: [
      "Approved facts can feed Brand Explorer trust chip evidence and BAS brand-fit reads.",
      "Americas-scoped footprint facts support Scout/Radar corridor context without RHG global overclaim.",
      "Owner/development positioning facts support Deal Readiness owner-education prompts (read-only).",
      "Ownership disclaimer fact supports alignment snapshots when region scope is ambiguous.",
    ],
  };
}

export async function previewRadissonBluSource(source, targetKeys, options = {}) {
  const keyPlan = buildTargetKeyPlan(targetKeys);
  const supportedKeys = keyPlan.filter((k) => k.registrySupported).map((k) => k.fieldKey);
  const unsupportedKeys = keyPlan.filter((k) => !k.registrySupported).map((k) => k.fieldKey);

  let doc;
  try {
    doc = await loadSourceDocumentText(source);
  } catch (err) {
    return {
      sourceId: source.id,
      sourceTitle: source.sourceTitle,
      validation: { ok: false, reasons: [`document_load_error: ${err.message}`] },
      keyPlan,
      supportedKeys,
      unsupportedKeys,
      previewAvailable: false,
      previewSkippedReason: err.message,
      rawCandidateCount: 0,
      previewCandidates: [],
      skippedCandidates: [],
      documentKind: null,
      textLength: 0,
      htmlTextClean: false,
    };
  }

  const validation = validateSourceForRadissonBluExtract(source, doc);
  const htmlTextClean = validateHtmlTextClean(doc).ok;

  if (!validation.ok) {
    return {
      sourceId: source.id,
      sourceTitle: source.sourceTitle,
      validation,
      keyPlan,
      supportedKeys,
      unsupportedKeys,
      previewAvailable: false,
      previewSkippedReason: validation.reasons.join("; "),
      rawCandidateCount: 0,
      previewCandidates: [],
      skippedCandidates: [],
      documentKind: doc.kind,
      textLength: nz(doc.text).length,
      htmlTextClean,
    };
  }

  if (!supportedKeys.length) {
    return {
      sourceId: source.id,
      sourceTitle: source.sourceTitle,
      validation,
      keyPlan,
      supportedKeys,
      unsupportedKeys,
      previewAvailable: false,
      previewSkippedReason: "no_registry_supported_target_keys",
      rawCandidateCount: 0,
      previewCandidates: [],
      skippedCandidates: [],
      documentKind: doc.kind,
      textLength: nz(doc.text).length,
      htmlTextClean,
    };
  }

  const registryFields = filterRegistryForTargetKeys(
    listBrandFieldsForExtractionRegistry(),
    supportedKeys
  );

  let extraction;
  try {
    extraction = await extractFromBrandSourceDocument(source, registryFields);
  } catch (err) {
    return {
      sourceId: source.id,
      sourceTitle: source.sourceTitle,
      validation,
      keyPlan,
      supportedKeys,
      unsupportedKeys,
      previewAvailable: false,
      previewSkippedReason: `extraction_preview_error: ${err.message}`,
      rawCandidateCount: 0,
      previewCandidates: [],
      skippedCandidates: [],
      documentKind: doc.kind,
      textLength: nz(doc.text).length,
      htmlTextClean,
    };
  }

  const rawForKeys = pickByTargetKeys(extraction.tagged, supportedKeys);
  const enriched = enrichRadissonBluCandidates(rawForKeys, source, doc);
  const { clean, skipped } = filterCleanCandidates(enriched, {
    limitFacts: options.limitFacts,
  });

  return {
    sourceId: source.id,
    sourceTitle: source.sourceTitle,
    sourceUrl: source.sourceUrl,
    validation,
    keyPlan,
    supportedKeys,
    unsupportedKeys,
    previewAvailable: true,
    previewSkippedReason: null,
    documentKind: extraction.documentKind,
    classificationRole: extraction.classification?.role || null,
    extractor: extraction.extractor,
    textLength: nz(doc.text).length,
    htmlTextClean,
    rawCandidateCount: rawForKeys.length,
    previewCandidates: clean.map((c) => ({
      fieldKey: c.fieldKey,
      priority: TARGET_KEY_PRIORITY[c.fieldKey] || "—",
      extractedValuePreview: String(c.extractedValue || "").slice(0, 320),
      evidencePreview: String(c.evidenceText || "").slice(0, 320),
      extractionType: c.extractionType,
      confidenceLevel: c.confidenceLevel,
      confidenceScore: c.confidenceScore,
      dataGap: c.dataGap || "No",
      enriched: Boolean(c._enriched),
      wouldWriteOnApply: true,
      _candidate: c,
    })),
    skippedCandidates: skipped,
  };
}

function dedupeGlobalFacts(rows, skipped) {
  const singularKeys = new Set([
    "be.identity.brandName",
    "be.identity.parentCompany",
    "be.positioning.summary",
    "be.positioning.tagline",
    "be.positioning.guestPromise",
    "be.overview.developmentModel",
    "be.overview.whyValue",
    "be.overview.typicalUseCase",
    "be.footprint.americasHotels",
    "be.footprint.geoIntro",
  ]);

  function score(row) {
    let s = row._candidate?.confidenceScore || 0;
    if (row.enriched || row._candidate?._enriched) s += 20;
    if (row.sourceId === ONE_PAGER_SOURCE_ID && ONE_PAGER_PRIORITY_KEYS.has(row.fieldKey)) {
      s += 30;
    }
    if (row.sourceId === PRESS_KIT_SOURCE_ID) {
      if (row.fieldKey === "be.footprint.americasHotels" || row.fieldKey === "be.footprint.geoIntro") {
        s += 15;
      } else {
        s += 10;
      }
    }
    return s;
  }

  const byKey = new Map();
  const byKeyValue = new Map();

  for (const row of rows) {
    if (singularKeys.has(row.fieldKey)) {
      const existing = byKey.get(row.fieldKey);
      if (!existing) {
        byKey.set(row.fieldKey, row);
        continue;
      }
      const keep = score(row) >= score(existing) ? row : existing;
      const drop = keep === row ? existing : row;
      skipped.push({
        fieldKey: drop.fieldKey,
        sourceId: drop.sourceId,
        extractedValuePreview: drop.extractedValuePreview,
        reasons: ["duplicate_field_key_keep_best"],
      });
      byKey.set(row.fieldKey, keep);
      continue;
    }

    const compound = `${row.fieldKey}::${normalizeValue(row.extractedValuePreview)}`;
    if (!byKeyValue.has(compound)) {
      byKeyValue.set(compound, row);
      continue;
    }
    skipped.push({
      fieldKey: row.fieldKey,
      sourceId: row.sourceId,
      extractedValuePreview: row.extractedValuePreview,
      reasons: ["duplicate_same_value_same_key_cross_source"],
    });
  }

  return { facts: [...byKey.values(), ...byKeyValue.values()], skipped };
}

export function buildWouldWritePlan(sourcePreviews, targetKeys, options = {}) {
  const unsupportedKeys = new Set();
  const allSkipped = [];
  const allCandidates = [];
  const duplicateWarnings = [];

  for (const preview of sourcePreviews) {
    for (const key of preview.unsupportedKeys || []) unsupportedKeys.add(key);
    for (const s of preview.skippedCandidates || []) {
      allSkipped.push({ ...s, sourceId: preview.sourceId });
    }
    if (!preview.previewAvailable) continue;
    for (const c of preview.previewCandidates || []) {
      allCandidates.push({
        sourceId: preview.sourceId,
        sourceTitle: preview.sourceTitle,
        fieldKey: c.fieldKey,
        priority: c.priority,
        extractedValuePreview: c.extractedValuePreview,
        evidencePreview: c.evidencePreview,
        extractionType: c.extractionType,
        confidenceLevel: c.confidenceLevel,
        enriched: c.enriched,
        _candidate: c._candidate,
      });
    }
  }

  const globalDeduped = dedupeGlobalFacts(allCandidates, allSkipped);
  for (const row of globalDeduped.skipped) {
    if (row.reasons?.includes("duplicate_field_key_keep_best")) {
      duplicateWarnings.push(row);
    }
  }

  let facts = globalDeduped.facts;
  if (typeof options.limitFacts === "number" && options.limitFacts > 0) {
    const capped = facts.slice(0, options.limitFacts);
    for (const dropped of facts.slice(options.limitFacts)) {
      allSkipped.push({
        fieldKey: dropped.fieldKey,
        sourceId: dropped.sourceId,
        extractedValuePreview: dropped.extractedValuePreview,
        reasons: ["limit_facts_cap"],
      });
    }
    facts = capped;
  }

  const sourcesWithFacts = new Set(facts.map((f) => f.sourceId));

  return {
    targetFactKeys: targetKeys,
    unsupportedRegistryKeys: [...unsupportedKeys],
    duplicateWarnings,
    factRowsWouldCreate: facts.map(({ _candidate, ...row }) => row),
    proposedCandidates: facts,
    factRowsSkipped: allSkipped,
    sourcesWouldPatch: sourcePreviews
      .filter((p) => p.validation.ok && p.previewAvailable && sourcesWithFacts.has(p.sourceId))
      .map((p) => ({
        sourceId: p.sourceId,
        sourceTitle: p.sourceTitle,
        fields: {
          status: "Extracted",
          extractionRunId: "(assigned on apply)",
          notesAppend: RB_EXTRACT_BATCH_NOTE,
        },
      })),
    factsWouldCreateCount: facts.length,
    doesNotWrite: [
      "Human Review Status = Approved (facts remain Pending)",
      "Approved for Explorer Use changes",
      "Brand Setup profile governance fields",
      "Company Validated / Company Validation Date",
      "External Display Status / Show Trust Label",
      "Published Explorer Fields / platform field publishing",
      "Governance publish",
      "RHG-global portfolio facts without Americas Choice evidence",
      "Apply without --approve-radisson-blu-extract",
    ],
  };
}

async function createRadissonBluFacts(source, runId, candidates, brandId) {
  const today = new Date().toISOString().slice(0, 10);
  const created = [];

  for (const c of candidates) {
    const assessment = assessCandidateQuality(c);
    if (!assessment.clean) continue;

    const reg = getRegistryField(c.fieldKey, "Brand Explorer");
    const fields = {
      "Source Title": `${reg?.displayLabel || c.fieldKey} — ${runId}`,
      [MAP_PARTNER_FACT.profileType]: "Brand",
      [MAP_PARTNER_FACT.brand]: [brandId],
      [MAP_PARTNER_FACT.sourceRecord]: [source.id],
      [MAP_PARTNER_FACT.explorerType]: "Brand Explorer",
      [MAP_PARTNER_FACT.explorerSection]: c.explorerSection || reg?.explorerSection,
      [MAP_PARTNER_FACT.fieldName]: c.fieldKey,
      [MAP_PARTNER_FACT.extractedValue]: c.extractedValue,
      [MAP_PARTNER_FACT.normalizedValue]: c.normalizedValue || c.extractedValue,
      [MAP_PARTNER_FACT.evidenceText]: c.evidenceText,
      [MAP_PARTNER_FACT.pageSectionAnchor]: c.pageSectionAnchor || c._sourceTitle || "",
      [MAP_PARTNER_FACT.sourceType]: source.sourceType || "",
      [MAP_PARTNER_FACT.sourceQuality]: source.sourceQuality || "Medium",
      [MAP_PARTNER_FACT.confidenceScore]: c.confidenceScore,
      [MAP_PARTNER_FACT.confidenceLevel]: c.confidenceLevel,
      [MAP_PARTNER_FACT.extractionType]: c.extractionType,
      [MAP_PARTNER_FACT.publicVisibility]: "Public",
      [MAP_PARTNER_FACT.humanReviewStatus]: "Pending",
      [MAP_PARTNER_FACT.dataGap]: c.dataGap || "No",
      [MAP_PARTNER_FACT.followUpQuestion]: c.followUpQuestion || "",
      [MAP_PARTNER_FACT.lastUpdated]: today,
      [MAP_PARTNER_FACT.extractionRunId]: runId,
      [MAP_PARTNER_FACT.reviewerNotes]: RB_EXTRACT_FACT_NOTE,
    };

    const fact = await createPartnerFact(fields);
    created.push(fact);
  }

  return created;
}

export async function applyRadissonBluExtract({ sources, targetKeys, limitFacts, sourcePreviews }) {
  const runId = `pi-radisson-blu-${randomUUID().slice(0, 8)}`;
  const applyResult = {
    runId,
    batchNote: RB_EXTRACT_BATCH_NOTE,
    sourcesPatched: [],
    factsCreated: [],
    skipped: [],
  };

  const previews =
    sourcePreviews ||
    (await Promise.all(
      sources.map((source) => previewRadissonBluSource(source, targetKeys, { limitFacts }))
    ));

  const wouldWrite = buildWouldWritePlan(previews, targetKeys, { limitFacts });
  const bySource = new Map();
  for (const row of wouldWrite.proposedCandidates) {
    if (!row._candidate) continue;
    if (!bySource.has(row.sourceId)) bySource.set(row.sourceId, []);
    bySource.get(row.sourceId).push(row._candidate);
  }

  for (const source of sources) {
    const candidates = bySource.get(source.id) || [];
    if (!candidates.length) {
      const preview = previews.find((p) => p.sourceId === source.id);
      applyResult.skipped.push({
        sourceId: source.id,
        type: "source",
        reasons: [preview?.previewSkippedReason || "no_clean_candidates"],
      });
      continue;
    }

    const created = await createRadissonBluFacts(source, runId, candidates, RADISSON_BLU_BRAND_ID);

    const existingNotes = String(source.notes || "").trim();
    const noteLine = `${RB_EXTRACT_BATCH_NOTE} run ${runId}`;
    const sourcePatch = {
      [MAP_PARTNER_SOURCE.status]: "Extracted",
      [MAP_PARTNER_SOURCE.extractionRunId]: runId,
    };
    if (!existingNotes.includes(RB_EXTRACT_BATCH_NOTE)) {
      sourcePatch[MAP_PARTNER_SOURCE.notes] = existingNotes
        ? `${existingNotes}\n${noteLine}`
        : noteLine;
    }

    await patchPartnerSource(source.id, sourcePatch);

    applyResult.sourcesPatched.push({
      sourceId: source.id,
      sourceTitle: source.sourceTitle,
      patch: sourcePatch,
      factsCreated: created.length,
    });
    applyResult.factsCreated.push(
      ...created.map((f) => ({
        id: f.id,
        fieldName: f.fieldName,
        sourceRecordId: source.id,
        extractedValuePreview: String(f.extractedValue || "").slice(0, 120),
      }))
    );
  }

  return applyResult;
}

export function getRegionOwnershipCaveats() {
  return [...REGION_OWNERSHIP_CAVEATS];
}
