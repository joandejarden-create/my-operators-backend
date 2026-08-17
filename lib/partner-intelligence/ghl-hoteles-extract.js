/**
 * GHL Hoteles narrow operator extraction (dry-run preview + controlled apply).
 * @see docs/data-intelligence/ghl-hoteles-extraction-plan.md
 */
import { randomUUID } from "crypto";
import {
  MAP_PARTNER_FACT,
  MAP_PARTNER_SOURCE,
  PARTNER_INTELLIGENCE_GAP_COPY,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  getRegistryField,
  listOperatorFieldsForExtraction,
} from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import { extractFromOperatorSourceDocument } from "./run-extraction.js";
import { loadSourceDocumentText } from "./extract-source-text.js";
import { createPartnerFact } from "./airtable-facts.js";
import { patchPartnerSource } from "./airtable-source.js";
import { isSourceExtractable } from "./sync-reference-folder.js";

export const GHL_OPERATOR_ID = "reciI2tYQBfMoMK9G";
export const REPORT_JSON_NAME = "ghl-hoteles-extract.json";
export const REPORT_MD_NAME = "ghl-hoteles-extract.md";
export const GHL_EXTRACT_BATCH_NOTE = "GHL Hoteles narrow extraction allowlist.";
export const GHL_EXTRACT_FACT_NOTE = "GHL Hoteles narrow extraction allowlist.";

/** Spanish home — excluded from this workflow until explicitly allowlisted. */
export const EXCLUDED_SOURCE_ID = "recFqJpw4wJbMmVSF";

/** Hard-coded allowlist — English official sources only. */
export const ALLOWLISTED_SOURCE_IDS = [
  "recvjfaDa9AnCJkNx",
  "reckrUB2WmnSm02g3",
  "recy337fP8zhpvePy",
  "recqLGiIQAEP1I1Hv",
  "recoOcRjSD3VZb3qt",
];

export const ALLOWLISTED_SOURCE_ID_SET = new Set(ALLOWLISTED_SOURCE_IDS);

export const PRIMARY_TARGET_FACT_KEYS = [
  "op.snapshot.companyName",
  "op.snapshot.companyDescription",
  "op.snapshot.primaryServiceModel",
  "op.markets.regionsSupported",
  "op.brand.familiesOperated",
  "op.platform.offeredServices",
];

/** Optional keys — reported unsupported if absent from explorer registry. */
export const OPTIONAL_TARGET_FACT_KEYS = [
  "op.capabilities.managementServices",
  "op.portfolio.scale",
  "op.events.miceCapability",
];

export const DEFAULT_TARGET_FACT_KEYS = [
  ...PRIMARY_TARGET_FACT_KEYS,
  ...OPTIONAL_TARGET_FACT_KEYS,
];

export const TARGET_KEY_PRIORITY = {
  "op.snapshot.companyName": "P0",
  "op.snapshot.companyDescription": "P0",
  "op.snapshot.primaryServiceModel": "P0",
  "op.markets.regionsSupported": "P0",
  "op.platform.offeredServices": "P0",
  "op.brand.familiesOperated": "P1",
  "op.capabilities.managementServices": "P2",
  "op.portfolio.scale": "P2",
  "op.events.miceCapability": "P2",
};

const GHL_BRAND_FAMILIES = [
  "Geotel",
  "GHL Collection",
  "GHL Relax",
  "GHL Style",
  "Irotama Resort",
  "Latam Hotel Corporation",
  "GHL",
];

const MARKUP_RE = /<!DOCTYPE|<html[\s>]|<meta[\s>]|<\/\w+>/i;
const NAV_NOISE_RE = /\b(skip navigation|enable javascript)\b/i;
const BOOKING_NOISE_RE =
  /\b(Check availability|Flight \+ hotel|Promotional code|Book now|My booking|Mi reserva|Ver disponibilidad|Occupancy|Check-in|Check-out)\b/i;
const OFFER_NOISE_RE =
  /\b(Advance Purchase|Last Minute Discount|Weekend Discount|Birthday Plans|Romantic Plans)\b/i;
const WEAK_DESCRIPTION_RE =
  /\b(book always|best price online|make your reservations|choose your destination|discover all the (destinations|hotels)|official website\.?$|explore and discover all the possibilities|organise all your events with ghl hotels and achieve success)\b/i;
const VAGUE_MARKETING_RE =
  /\b(world[- ]class|best[- ]in[- ]class|leading|unparalleled|unique experiences|memorable stays)\b/i;

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
  if (id === EXCLUDED_SOURCE_ID) {
    return { ok: false, reasons: ["source_excluded_spanish_home"] };
  }
  if (!ALLOWLISTED_SOURCE_ID_SET.has(id)) {
    return { ok: false, reasons: ["source_not_in_ghl_hoteles_allowlist"] };
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
  const registry = listOperatorFieldsForExtraction();
  const registryByKey = new Map(registry.map((f) => [f.fieldKey, f]));

  return factKeys.map((fieldKey) => {
    const reg = registryByKey.get(fieldKey) || getRegistryField(fieldKey, "Operator Explorer");
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

export function assessCandidateQuality(candidate) {
  const reasons = [];
  const value = nz(candidate.extractedValue);
  const evidence = nz(candidate.evidenceText);

  if (!value) reasons.push("empty_value");
  if (value === PARTNER_INTELLIGENCE_GAP_COPY) reasons.push("gap_copy");
  if (candidate.dataGap === "Yes") reasons.push("data_gap");
  if (MARKUP_RE.test(value) || MARKUP_RE.test(evidence)) reasons.push("markup_artifact");
  if (!evidence || evidence === PARTNER_INTELLIGENCE_GAP_COPY) reasons.push("weak_evidence");
  if (value.length < 3) reasons.push("value_too_short");
  if (NAV_NOISE_RE.test(value) && value.length < 120) reasons.push("nav_boilerplate");
  if (BOOKING_NOISE_RE.test(value) && value.length < 100) reasons.push("booking_boilerplate");
  if (OFFER_NOISE_RE.test(value)) reasons.push("offer_promotion_noise");
  if (VAGUE_MARKETING_RE.test(value) && value.length < 90) reasons.push("vague_marketing_claim");
  if (
    candidate.fieldKey === "op.snapshot.companyDescription" &&
    WEAK_DESCRIPTION_RE.test(value)
  ) {
    reasons.push("weak_company_description");
  }
  if (
    candidate.fieldKey === "op.snapshot.primaryServiceModel" &&
    /\b(third[- ]party management|asset management|franchise management)\b/i.test(value) &&
    !/\b(third[- ]party|asset management|franchise management)\b/i.test(evidence)
  ) {
    reasons.push("management_structure_overclaim");
  }

  return { clean: reasons.length === 0, reasons };
}

function filterRegistryForTargetKeys(registryFields, targetKeys) {
  const keySet = new Set(targetKeys);
  return registryFields.filter((f) => keySet.has(f.fieldKey));
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

function extractScaleSentence(text) {
  const m = text.match(
    /With presence in\s+(\d+)\s+hotels,\s+(\d+)\s+countries,\s+(\d+)\s+destinations,\s+([\d.,]+)\s+rooms\s+and\s+([\d.,]+)\s+collaborators/i
  );
  if (!m) return null;
  return `With presence in ${m[1]} hotels, ${m[2]} countries, ${m[3]} destinations, ${m[4]} rooms and ${m[5]} collaborators`;
}

function detectBrandFamilies(text) {
  const found = [];
  for (const brand of GHL_BRAND_FAMILIES) {
    const re =
      brand === "GHL"
        ? /\bGHL Hotels\b|\bGHL Hoteles\b|\bbrand[s]?\s+GHL\b/i
        : new RegExp(`\\b${brand.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
    if (re.test(text)) found.push(brand);
  }
  const ordered = GHL_BRAND_FAMILIES.filter((b) => found.includes(b));
  return ordered.length ? ordered.join(", ") : null;
}

function detectRegions(text, source) {
  const regions = [];
  if (/latin america|latinoam[eé]rica/i.test(text)) regions.push("Latin America");
  for (const country of ["Colombia", "Peru", "Chile", "Guatemala"]) {
    if (new RegExp(`\\b${country}\\b`, "i").test(text)) regions.push(country);
  }
  if (!regions.length && /destinations in latin america/i.test(text)) {
    regions.push("Latin America");
  }
  if (!regions.length) return null;
  const val = [...new Set(regions)].join(", ");
  const evidence =
    extractScaleSentence(text) ||
    (regions.includes("Latin America")
      ? "Destinations in Latin America"
      : regions.slice(0, 3).join(", "));
  return { value: val, evidence, source };
}

function inferCompanyName(source, doc) {
  const text = nz(doc.text).slice(0, 1200);
  if (/\bGHL Hoteles\b/i.test(text)) return "GHL Hoteles";
  if (/\bGHL Hotels\b/i.test(text)) return "GHL Hotels";
  const title = nz(doc.title) || nz(source.sourceTitle);
  if (/ghl/i.test(title)) return "GHL Hoteles";
  return null;
}

function inferCompanyDescription(source, doc) {
  const text = nz(doc.text);
  const scale = extractScaleSentence(text);
  if (scale) {
    return {
      value: `${scale}, GHL operates hotels across Latin America.`,
      evidence: scale,
    };
  }
  const meta = nz(doc.metaDescription);
  if (meta && meta.length >= 40 && !BOOKING_NOISE_RE.test(meta)) {
    return { value: meta, evidence: meta };
  }
  const m = text.match(
    /Our hotels are located on one of the continents with the greatest biodiversity, culture, history and tradition[^.]{0,120}\./i
  );
  if (m) return { value: m[0].trim(), evidence: m[0].trim() };
  return null;
}

function inferPrimaryServiceModel(source, doc) {
  const text = nz(doc.text);
  const m = text.match(/hotels operated by GHL Hotels[^.]{0,160}\./i);
  if (m) {
    return {
      value: "Hotel operations",
      evidence: m[0].trim(),
    };
  }
  const m2 = text.match(/The hotels operated by GHL Hotels[^.]{0,160}\./i);
  if (m2) {
    return {
      value: "Hotel operations",
      evidence: m2[0].trim(),
    };
  }
  return null;
}

function inferOfferedServices(source, doc) {
  const text = nz(doc.text);
  const events =
    text.match(
      /guarantee success in all your celebrations and events thanks to their well-equipped facilities[^.]{0,120}\./i
    ) ||
    text.match(/celebrates your Events[^.]{0,120}\./i) ||
    text.match(/success in all your celebrations and events[^.]{0,120}\./i);
  if (events) {
    return {
      value: "Events and celebrations (meetings and group events at GHL hotels)",
      evidence: events[0].trim(),
    };
  }
  const brandServices = text.match(
    /GHL hotels offer complete and comprehensive services to their guests[^.]{0,120}\./i
  );
  if (brandServices && /brands\/ghl/i.test(nz(source.sourceUrl))) {
    return {
      value: "Full-service hotel guest services (rooms, meetings, and on-property services)",
      evidence: brandServices[0].trim(),
    };
  }
  return null;
}

function inferBrandFamilies(source, doc) {
  const text = nz(doc.text);
  const families = detectBrandFamilies(text);
  if (!families) return null;
  const evidenceMatch =
    text.match(/GHL Collection|GHL Relax|GHL Style|Geotel|Irotama|Latam Hotel Corporation|GHL hotels offer/i);
  return {
    value: families,
    evidence: evidenceMatch ? evidenceMatch[0] : families,
  };
}

export function enrichGhlHotelesCandidates(candidates, source, doc) {
  const out = [...candidates];
  const hasCleanKey = (key) =>
    out.some((c) => c.fieldKey === key && assessCandidateQuality(c).clean);

  const anchor = source.localFilePath || source.sourceUrl || source.sourceTitle;

  if (!hasCleanKey("op.snapshot.companyName")) {
    const name = inferCompanyName(source, doc);
    if (name) {
      pushCandidate(out, {
        fieldKey: "op.snapshot.companyName",
        explorerSection: "Company Snapshot",
        displayLabel: "Company Name",
        extractedValue: name,
        normalizedValue: name,
        evidenceText: nz(doc.title) || name,
        pageSectionAnchor: anchor,
        _sourceId: source.id,
        _sourceTitle: source.sourceTitle,
        _enriched: true,
      });
    }
  }

  if (!hasCleanKey("op.snapshot.companyDescription")) {
    const desc = inferCompanyDescription(source, doc);
    if (desc) {
      pushCandidate(out, {
        fieldKey: "op.snapshot.companyDescription",
        explorerSection: "Company Snapshot",
        displayLabel: "Company Description",
        extractedValue: desc.value,
        normalizedValue: desc.value,
        evidenceText: desc.evidence,
        pageSectionAnchor: anchor,
        confidenceLevel: "High",
        confidenceScore: 85,
        _sourceId: source.id,
        _sourceTitle: source.sourceTitle,
        _enriched: true,
      });
    }
  }

  if (!hasCleanKey("op.markets.regionsSupported")) {
    const regions = detectRegions(nz(doc.text), source);
    if (regions) {
      pushCandidate(out, {
        fieldKey: "op.markets.regionsSupported",
        explorerSection: "Regional Presence",
        displayLabel: "Regions Supported",
        extractedValue: regions.value,
        normalizedValue: regions.value,
        evidenceText: regions.evidence,
        pageSectionAnchor: anchor,
        confidenceLevel: regions.value.includes(",") ? "High" : "Medium",
        confidenceScore: regions.value.includes(",") ? 82 : 68,
        _sourceId: source.id,
        _sourceTitle: source.sourceTitle,
        _enriched: true,
      });
    }
  }

  if (!hasCleanKey("op.brand.familiesOperated")) {
    const brands = inferBrandFamilies(source, doc);
    if (brands) {
      pushCandidate(out, {
        fieldKey: "op.brand.familiesOperated",
        explorerSection: "Brand Relationships",
        displayLabel: "Brand Families Operated",
        extractedValue: brands.value,
        normalizedValue: brands.value,
        evidenceText: brands.evidence,
        pageSectionAnchor: anchor,
        confidenceLevel: "High",
        confidenceScore: 80,
        _sourceId: source.id,
        _sourceTitle: source.sourceTitle,
        _enriched: true,
      });
    }
  }

  if (!hasCleanKey("op.snapshot.primaryServiceModel")) {
    const model = inferPrimaryServiceModel(source, doc);
    if (model) {
      pushCandidate(out, {
        fieldKey: "op.snapshot.primaryServiceModel",
        explorerSection: "Company Snapshot",
        displayLabel: "Primary Service Model",
        extractedValue: model.value,
        normalizedValue: model.value,
        evidenceText: model.evidence,
        pageSectionAnchor: anchor,
        confidenceLevel: "Medium",
        confidenceScore: 72,
        _sourceId: source.id,
        _sourceTitle: source.sourceTitle,
        _enriched: true,
      });
    }
  }

  if (!hasCleanKey("op.platform.offeredServices")) {
    const services = inferOfferedServices(source, doc);
    if (services) {
      pushCandidate(out, {
        fieldKey: "op.platform.offeredServices",
        explorerSection: "Operating Platform",
        displayLabel: "Offered Services",
        extractedValue: services.value,
        normalizedValue: services.value,
        evidenceText: services.evidence,
        pageSectionAnchor: anchor,
        confidenceLevel: "Medium",
        confidenceScore: 74,
        _sourceId: source.id,
        _sourceTitle: source.sourceTitle,
        _enriched: true,
      });
    }
  }

  return out;
}

function pickByTargetKeys(tagged, targetKeys) {
  const keySet = new Set(targetKeys);
  return tagged.filter((c) => keySet.has(c.fieldKey));
}

function sortByPriority(candidates) {
  return [...candidates].sort((a, b) => {
    const pa = TARGET_KEY_PRIORITY[a.fieldKey] || "P9";
    const pb = TARGET_KEY_PRIORITY[b.fieldKey] || "P9";
    return pa.localeCompare(pb);
  });
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
        extractedValuePreview: String(c.extractedValue || "").slice(0, 120),
        reasons: assessment.reasons,
      });
      continue;
    }
    clean.push(c);
  }

  const deduped = dedupeCandidates(clean, skipped);
  const sorted = sortByPriority(deduped);
  const limited =
    typeof options.limitFacts === "number" && options.limitFacts > 0
      ? sorted.slice(0, options.limitFacts)
      : sorted;

  if (limited.length < sorted.length) {
    for (const dropped of sorted.slice(limited.length)) {
      skipped.push({
        fieldKey: dropped.fieldKey,
        sourceId: dropped._sourceId,
        extractedValuePreview: String(dropped.extractedValue || "").slice(0, 120),
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
    const keep =
      (c.confidenceScore || 0) > (existing.confidenceScore || 0) ? c : existing;
    const drop = keep === c ? existing : c;
    skipped.push({
      fieldKey: drop.fieldKey,
      sourceId: drop._sourceId,
      extractedValuePreview: String(drop.extractedValue || "").slice(0, 120),
      reasons: ["duplicate_same_value_same_key"],
    });
    byKeyValue.set(compound, keep);
  }
  return [...byKeyValue.values()];
}

export function validateSourceForGhlHotelesExtract(source, doc) {
  const reasons = [];
  const allow = assertSourceAllowlisted(source.id);
  if (!allow.ok) reasons.push(...allow.reasons);

  if (source.operatorId !== GHL_OPERATOR_ID) {
    reasons.push("not_linked_to_ghl_hoteles_operator");
  }
  if (source.status === "Stale") {
    reasons.push("source_status_stale");
  }
  if (!isSourceExtractable(source)) {
    reasons.push("source_not_extractable_no_file_or_url");
  }

  const htmlCheck = validateHtmlTextClean(doc);
  if (!htmlCheck.ok) reasons.push(...htmlCheck.reasons);

  return { ok: reasons.length === 0, reasons };
}

export function summarizeExistingFacts(facts, sourceIds, targetKeys) {
  const sourceSet = new Set(sourceIds);
  const keySet = new Set(targetKeys);
  const rows = [];

  for (const fact of facts || []) {
    if (fact.operatorId !== GHL_OPERATOR_ID) continue;
    if (!sourceSet.has(fact.sourceRecordId)) continue;
    if (!keySet.has(fact.fieldName)) continue;

    let row = rows.find(
      (r) => r.sourceId === fact.sourceRecordId && r.fieldKey === fact.fieldName
    );
    if (!row) {
      row = {
        sourceId: fact.sourceRecordId,
        fieldKey: fact.fieldName,
        existingCount: 0,
        factIds: [],
      };
      rows.push(row);
    }
    row.existingCount += 1;
    row.factIds.push(fact.id);
  }

  return { rows, existingCount: rows.reduce((n, r) => n + r.existingCount, 0) };
}

export function assessExtractionQuality(sourcePreviews, wouldWrite) {
  const substantiveKeys = new Set([
    "op.snapshot.companyDescription",
    "op.snapshot.primaryServiceModel",
    "op.markets.regionsSupported",
    "op.brand.familiesOperated",
    "op.platform.offeredServices",
  ]);
  const proposed = wouldWrite.factRowsWouldCreate || [];
  const substantive = proposed.filter((f) => substantiveKeys.has(f.fieldKey));
  const hasCompanyName = proposed.some((f) => f.fieldKey === "op.snapshot.companyName");
  const allHtmlClean = sourcePreviews.every((p) => p.htmlTextClean !== false);
  const readableSources = sourcePreviews.filter(
    (p) => p.previewAvailable && (p.textLength || 0) >= 500
  ).length;

  return {
    allHtmlTextClean: allHtmlClean,
    readableSourceCount: readableSources,
    substantiveFactCount: substantive.length,
    hasCompanyName,
    hasOfferedServices: substantive.some((f) => f.fieldKey === "op.platform.offeredServices"),
    hasRegions: substantive.some((f) => f.fieldKey === "op.markets.regionsSupported"),
    hasBrands: substantive.some((f) => f.fieldKey === "op.brand.familiesOperated"),
    overall:
      allHtmlClean && substantive.length >= 3 && hasCompanyName
        ? "good_for_steward_review"
        : substantive.length >= 2
          ? "moderate_review_carefully"
          : "weak_needs_more_sources_or_curation",
    applyRecommended:
      allHtmlClean &&
      substantive.length >= 3 &&
      hasCompanyName &&
      proposed.length >= 4,
    governancePublishStillBlocked: true,
    governanceBlockReason:
      "Facts must be created, steward-approved, and governance publish dry-run must pass before publish.",
  };
}

export async function previewGhlHotelesSource(source, targetKeys, options = {}) {
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

  const validation = validateSourceForGhlHotelesExtract(source, doc);
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
    listOperatorFieldsForExtraction(),
    supportedKeys
  );

  let extraction;
  try {
    extraction = await extractFromOperatorSourceDocument(source, registryFields);
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
  const enriched = enrichGhlHotelesCandidates(rawForKeys, source, doc);
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
    extractionQuality: {
      kind: doc.kind,
      textLength: nz(doc.text).length,
      readable: htmlTextClean && nz(doc.text).length >= 100,
      note: htmlTextClean ? "html_clean" : "html_quality_issue",
    },
    rawCandidateCount: rawForKeys.length,
    previewCandidates: clean.map((c) => ({
      fieldKey: c.fieldKey,
      priority: TARGET_KEY_PRIORITY[c.fieldKey] || "—",
      extractedValuePreview: String(c.extractedValue || "").slice(0, 300),
      evidencePreview: String(c.evidenceText || "").slice(0, 300),
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
    factRowsSkipped: allSkipped.concat(globalDeduped.skipped),
    sourcesWouldPatch: sourcePreviews
      .filter((p) => p.validation.ok && p.previewAvailable && sourcesWithFacts.has(p.sourceId))
      .map((p) => ({
        sourceId: p.sourceId,
        sourceTitle: p.sourceTitle,
        fields: {
          status: "Extracted",
          extractionRunId: "(assigned on apply)",
          notesAppend: GHL_EXTRACT_BATCH_NOTE,
        },
      })),
    factsWouldCreateCount: facts.length,
    doesNotWrite: [
      "Approved for Explorer Use",
      "Approved for Extraction (not auto-set)",
      "Human Review Status = Approved (facts remain Pending)",
      "Operator Setup profile governance fields",
      "Company Validated / Company Validation Date",
      "External Display Status / Show Trust Label",
      "Published Explorer Fields",
      "Gap facts / Not confirmed placeholders",
      "Spanish home source recFqJpw4wJbMmVSF",
      "All operator PI sources (allowlist only)",
    ],
  };
}

function dedupeGlobalFacts(rows, skipped) {
  const singularKeys = new Set([
    "op.snapshot.companyName",
    "op.snapshot.companyDescription",
    "op.snapshot.primaryServiceModel",
    "op.markets.regionsSupported",
    "op.brand.familiesOperated",
  ]);

  function candidateScore(row) {
    let score = row._candidate?.confidenceScore || 0;
    if (row.enriched || row._candidate?._enriched) score += 15;
    const val = nz(row.extractedValuePreview);
    if (row.fieldKey === "op.snapshot.companyDescription" && /35 hotels|collaborators/i.test(val)) {
      score += 40;
    }
    if (row.fieldKey === "op.brand.familiesOperated") score += val.split(",").length * 3;
    return score;
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
      const keep = candidateScore(row) >= candidateScore(existing) ? row : existing;
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
    const existing = byKeyValue.get(compound);
    if (!existing) {
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

async function createGhlHotelesFacts(source, runId, candidates, operatorId) {
  const today = new Date().toISOString().slice(0, 10);
  const created = [];

  for (const c of candidates) {
    const assessment = assessCandidateQuality(c);
    if (!assessment.clean) continue;

    const reg = getRegistryField(c.fieldKey, "Operator Explorer");
    const fields = {
      "Source Title": `${reg?.displayLabel || c.fieldKey} — ${runId}`,
      [MAP_PARTNER_FACT.profileType]: "Operator",
      [MAP_PARTNER_FACT.operator]: [operatorId],
      [MAP_PARTNER_FACT.sourceRecord]: [source.id],
      [MAP_PARTNER_FACT.explorerType]: "Operator Explorer",
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
      [MAP_PARTNER_FACT.reviewerNotes]: GHL_EXTRACT_FACT_NOTE,
    };

    const fact = await createPartnerFact(fields);
    created.push(fact);
  }

  return created;
}

export async function applyGhlHotelesExtract({ sources, targetKeys, limitFacts, sourcePreviews }) {
  const runId = `pi-ghl-extract-${randomUUID().slice(0, 8)}`;
  const applyResult = {
    runId,
    batchNote: GHL_EXTRACT_BATCH_NOTE,
    sourcesPatched: [],
    factsCreated: [],
    skipped: [],
  };

  const previews =
    sourcePreviews ||
    (await Promise.all(
      sources.map((source) => previewGhlHotelesSource(source, targetKeys))
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

    const created = await createGhlHotelesFacts(source, runId, candidates, GHL_OPERATOR_ID);

    const existingNotes = String(source.notes || "").trim();
    const noteLine = `${GHL_EXTRACT_BATCH_NOTE} run ${runId}`;
    const sourcePatch = {
      [MAP_PARTNER_SOURCE.status]: "Extracted",
      [MAP_PARTNER_SOURCE.extractionRunId]: runId,
    };
    if (!existingNotes.includes(GHL_EXTRACT_BATCH_NOTE)) {
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
