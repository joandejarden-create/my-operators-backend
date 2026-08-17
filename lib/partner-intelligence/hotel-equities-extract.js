/**
 * Hotel Equities narrow operator extraction (dry-run preview + controlled apply).
 * @see docs/data-intelligence/hotel-equities-extraction-plan.md
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

export const HE_OPERATOR_ID = "recWPKu5laVZxsvpn";
export const REPORT_JSON_NAME = "hotel-equities-extract.json";
export const REPORT_MD_NAME = "hotel-equities-extract.md";
export const HE_EXTRACT_BATCH_NOTE = "Hotel Equities narrow extraction allowlist.";
export const HE_EXTRACT_FACT_NOTE = "Hotel Equities narrow extraction allowlist.";

/** Hard-coded allowlist — broaden only via explicit code change. */
export const WEBSITE_SOURCE_IDS = [
  "rectG9wdsAeL7u0FG",
  "rec9FSzLhaLPcPvtv",
  "recy1oDTNe7kyQGbE",
];

export const PDF_SOURCE_IDS = [
  "recxdPFckVzA3ckmN",
  "rectqBTiGkq3hUlXa",
];

export const SOURCE_GROUPS = {
  website: WEBSITE_SOURCE_IDS,
  pdf: PDF_SOURCE_IDS,
  all: [...WEBSITE_SOURCE_IDS, ...PDF_SOURCE_IDS],
};

export const ALLOWLISTED_SOURCE_IDS = new Set(SOURCE_GROUPS.all);

/** PDF enrichment default keys — no companyName; prioritize substance beyond website package. */
export const PDF_ENRICHMENT_TARGET_FACT_KEYS = [
  "op.platform.offeredServices",
  "op.snapshot.companyDescription",
  "op.snapshot.primaryServiceModel",
  "op.markets.regionsSupported",
  "op.brand.familiesOperated",
  "op.ownerValueProposition",
  "op.operatingModel",
];

export const PRIMARY_TARGET_FACT_KEYS = [
  "op.snapshot.companyName",
  "op.snapshot.companyDescription",
  "op.snapshot.primaryServiceModel",
  "op.platform.offeredServices",
  "op.markets.regionsSupported",
];

export const SECONDARY_TARGET_FACT_KEYS = [
  "op.brand.familiesOperated",
  "op.ownerValueProposition",
  "op.operatingModel",
];

export const DEFAULT_TARGET_FACT_KEYS = [
  ...PRIMARY_TARGET_FACT_KEYS,
  ...SECONDARY_TARGET_FACT_KEYS,
];

export const TARGET_KEY_PRIORITY = {
  "op.snapshot.companyName": "P0",
  "op.snapshot.companyDescription": "P0",
  "op.snapshot.primaryServiceModel": "P0",
  "op.platform.offeredServices": "P0",
  "op.markets.regionsSupported": "P0",
  "op.brand.familiesOperated": "P1",
  "op.ownerValueProposition": "P1",
  "op.operatingModel": "P1",
};

const MARKUP_RE = /<!DOCTYPE|<html[\s>]|<meta[\s>]|<\/\w+>/i;
const NAV_NOISE_RE = /\b(skip navigation|enable javascript)\b/i;
const WEB_FRAGMENT_NOISE_RE =
  /\b(Discover Our Culture|Our Footprint\d|Read More|Skip to content|Toggle navigation|Book Now|CHECK IN)\b/i;
const PDF_NOISE_RE =
  /\b(confidential|©\s*\d{4}|all rights reserved|page\s+\d+\s+of\s+\d+)\b/i;
const PDF_DECK_SLIDE_NOISE_RE =
  /\b(\d+\s+United States Hotels|\d+\s+Canada Hotels|F&B Outlets|WHY HE CALA|HOTEL AND F&B PORTFOLIO)\b/i;
const VAGUE_MARKETING_RE =
  /\b(world[- ]class|best[- ]in[- ]class|leading|unparalleled|synerg(y|ies))\b/i;

export function resolveSourceGroup(cliGroup) {
  const g = nz(cliGroup) || "website";
  if (!SOURCE_GROUPS[g]) {
    throw new Error(
      `Invalid --source-group "${g}". Use: ${Object.keys(SOURCE_GROUPS).join(", ")}`
    );
  }
  return g;
}

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
  if (!ALLOWLISTED_SOURCE_IDS.has(id)) {
    return { ok: false, reasons: ["source_not_in_hotel_equities_allowlist"] };
  }
  return { ok: true, reasons: [] };
}

export function resolveSourceIds(cliSourceIds, sourceGroup = "website") {
  resolveSourceGroup(sourceGroup);
  const requested = cliSourceIds.length ? cliSourceIds : [...SOURCE_GROUPS[sourceGroup]];
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

export function resolveTargetFactKeys(cliKeys, sourceGroup = "website") {
  if (cliKeys.length) return [...new Set(cliKeys)];
  if (sourceGroup === "pdf") return [...PDF_ENRICHMENT_TARGET_FACT_KEYS];
  return [...DEFAULT_TARGET_FACT_KEYS];
}

export function buildTargetKeyPlan(factKeys) {
  const registry = listOperatorFieldsForExtraction();
  const registryByKey = new Map(registry.map((f) => [f.fieldKey, f]));

  return factKeys.map((fieldKey) => {
    const reg = registryByKey.get(fieldKey) || getRegistryField(fieldKey, "Operator Explorer");
    const priority =
      TARGET_KEY_PRIORITY[fieldKey] ||
      (PRIMARY_TARGET_FACT_KEYS.includes(fieldKey) ? "P0" : "P1");
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

export function assessCandidateQuality(candidate, options = {}) {
  const reasons = [];
  const value = nz(candidate.extractedValue);
  const evidence = nz(candidate.evidenceText);
  const docKind = options.documentKind || null;

  if (!value) reasons.push("empty_value");
  if (value === PARTNER_INTELLIGENCE_GAP_COPY) reasons.push("gap_copy");
  if (candidate.dataGap === "Yes") reasons.push("data_gap");
  if (MARKUP_RE.test(value) || MARKUP_RE.test(evidence)) reasons.push("markup_artifact");
  if (!evidence || evidence === PARTNER_INTELLIGENCE_GAP_COPY) reasons.push("weak_evidence");
  if (value.length < 3) reasons.push("value_too_short");
  if (NAV_NOISE_RE.test(value) && value.length < 120) reasons.push("nav_boilerplate");
  if (docKind === "pdf") {
    if (PDF_NOISE_RE.test(value) && value.length < 200) reasons.push("pdf_boilerplate");
    if (PDF_DECK_SLIDE_NOISE_RE.test(value)) reasons.push("pdf_deck_slide_noise");
    if (VAGUE_MARKETING_RE.test(value) && value.length < 80) reasons.push("vague_marketing_claim");
    if (/^page\s+\d+$/i.test(value)) reasons.push("pdf_page_artifact");
  }
  if (
    candidate.fieldKey === "op.markets.regionsSupported" &&
    WEB_FRAGMENT_NOISE_RE.test(value)
  ) {
    reasons.push("noisy_web_fragment");
  }
  if (
    candidate.fieldKey === "op.brand.familiesOperated" &&
    /^choice$/i.test(value) &&
    !/choice hotels/i.test(evidence)
  ) {
    reasons.push("ambiguous_brand_hit");
  }
  if (options.sourceGroup === "pdf" && candidate.fieldKey === "op.snapshot.companyName") {
    reasons.push("pdf_enrichment_skips_company_name");
  }

  return { clean: reasons.length === 0, reasons };
}

function filterRegistryForTargetKeys(registryFields, targetKeys) {
  const keySet = new Set(targetKeys);
  return registryFields.filter((f) => keySet.has(f.fieldKey));
}

function inferCompanyName(source, doc) {
  const title = nz(doc.title) || nz(source.sourceTitle);
  const cala = /cala\.htm/i.test(nz(source.sourceUrl)) || /cala/i.test(title);
  if (cala && /hotel equities/i.test(title + " " + nz(doc.text).slice(0, 800))) {
    return "Hotel Equities (CALA)";
  }
  if (/hotel equities/i.test(title + " " + nz(doc.text).slice(0, 800))) {
    return "Hotel Equities";
  }
  const m = title.match(/Hotel Equities(?:\s*\(CALA\))?/i);
  return m ? m[0] : null;
}

export function enrichHotelEquitiesCandidates(candidates, source, doc, options = {}) {
  const out = [...candidates];
  const qualityOpts = {
    documentKind: doc?.kind || null,
    sourceGroup: options.sourceGroup || "website",
  };
  const hasCleanKey = (key) =>
    out.some((c) => c.fieldKey === key && assessCandidateQuality(c, qualityOpts).clean);

  if (options.sourceGroup !== "pdf" && !hasCleanKey("op.snapshot.companyName")) {
    const name = inferCompanyName(source, doc);
    if (name) {
      out.push({
        fieldKey: "op.snapshot.companyName",
        explorerSection: "Company Snapshot",
        displayLabel: "Company Name",
        extractedValue: name,
        normalizedValue: name,
        evidenceText: nz(doc.title) || name,
        pageSectionAnchor: source.localFilePath || source.sourceUrl || source.sourceTitle,
        extractionType: "Directly Stated",
        confidenceLevel: "High",
        confidenceScore: 82,
        dataGap: "No",
        _sourceId: source.id,
        _sourceTitle: source.sourceTitle,
        _enriched: true,
      });
    }
  }

  if (!hasCleanKey("op.markets.regionsSupported")) {
    const mentions = [];
    const text = nz(doc.text);
    if (/caribbean/i.test(text)) mentions.push("Caribbean");
    if (/latin america|\bcala\b/i.test(text)) mentions.push("Latin America");
    if (/united states|u\.s\./i.test(text)) mentions.push("United States");
    if (/canada/i.test(text)) mentions.push("Canada");
    if (mentions.length) {
      const val = [...new Set(mentions)].join(", ");
      out.push({
        fieldKey: "op.markets.regionsSupported",
        explorerSection: "Regional Presence",
        displayLabel: "Regions Supported",
        extractedValue: val,
        normalizedValue: val,
        evidenceText: `Geography mentions in source: ${val}`,
        pageSectionAnchor: source.localFilePath || source.sourceUrl || source.sourceTitle,
        extractionType: "Directly Stated",
        confidenceLevel: "Medium",
        confidenceScore: 65,
        dataGap: "No",
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
  const qualityOpts = {
    documentKind: options.documentKind || null,
    sourceGroup: options.sourceGroup || "website",
  };

  for (const c of candidates) {
    const assessment = assessCandidateQuality(c, qualityOpts);
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

export function validateSourceForHotelEquitiesExtract(source, doc) {
  const reasons = [];
  const allow = assertSourceAllowlisted(source.id);
  if (!allow.ok) reasons.push(...allow.reasons);

  if (source.operatorId !== HE_OPERATOR_ID) {
    reasons.push("not_linked_to_hotel_equities_operator");
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
    if (fact.operatorId !== HE_OPERATOR_ID) continue;
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

export function listApprovedWebsiteFacts(allFacts, websiteSourceIds = WEBSITE_SOURCE_IDS) {
  const websiteSet = new Set(websiteSourceIds);
  return (allFacts || []).filter((fact) => {
    if (fact.operatorId !== HE_OPERATOR_ID) return false;
    const status = nz(fact.humanReviewStatus);
    if (status !== "Approved" && status !== "Edited") return false;
    return websiteSet.has(fact.sourceRecordId);
  });
}

function valuesSimilar(a, b) {
  const na = normalizeValue(a);
  const nb = normalizeValue(b);
  if (!na || !nb) return false;
  if (na === nb) return true;
  if (na.length > 40 && nb.length > 40 && (na.includes(nb) || nb.includes(na))) return true;
  return false;
}

export function filterAgainstApprovedFacts(candidates, approvedFacts, options = {}) {
  const skipped = [];
  const clean = [];
  const approvedByKey = new Map();
  for (const fact of approvedFacts || []) {
    const key = fact.fieldName;
    if (!approvedByKey.has(key)) approvedByKey.set(key, []);
    approvedByKey.get(key).push(fact);
  }

  for (const c of candidates) {
    const key = c.fieldKey;
    const approved = approvedByKey.get(key) || [];
    const value = nz(c.extractedValue);

    if (key === "op.snapshot.companyName") {
      skipped.push({
        fieldKey: key,
        sourceId: c._sourceId,
        extractedValuePreview: value.slice(0, 120),
        reasons: ["duplicate_company_name_avoided"],
      });
      continue;
    }

    const exactDup = approved.find((f) => valuesSimilar(f.approvedValue || f.extractedValue, value));
    if (exactDup) {
      skipped.push({
        fieldKey: key,
        sourceId: c._sourceId,
        extractedValuePreview: value.slice(0, 120),
        reasons: ["duplicate_of_approved_website_fact"],
        approvedFactId: exactDup.id,
      });
      continue;
    }

    if (key === "op.snapshot.companyDescription" && approved.length) {
      const bestApproved = approved.reduce((best, f) => {
        const len = nz(f.approvedValue || f.extractedValue).length;
        return len > nz(best?.approvedValue || best?.extractedValue).length ? f : best;
      }, approved[0]);
      const approvedLen = nz(bestApproved.approvedValue || bestApproved.extractedValue).length;
      if (value.length < approvedLen * 0.85 && valuesSimilar(value, bestApproved.approvedValue || bestApproved.extractedValue)) {
        skipped.push({
          fieldKey: key,
          sourceId: c._sourceId,
          extractedValuePreview: value.slice(0, 120),
          reasons: ["weaker_than_approved_website_description"],
          approvedFactId: bestApproved.id,
        });
        continue;
      }
    }

    clean.push(c);
  }

  return { clean, skipped, duplicateWarnings: skipped.filter((s) =>
    s.reasons.includes("duplicate_of_approved_website_fact") ||
    s.reasons.includes("weaker_than_approved_website_description")
  ) };
}

export function assessPublishScopeStrength(proposedFacts) {
  const substantiveKeys = new Set([
    "op.platform.offeredServices",
    "op.snapshot.companyDescription",
    "op.snapshot.primaryServiceModel",
    "op.markets.regionsSupported",
    "op.brand.familiesOperated",
  ]);
  const hits = proposedFacts.filter((f) => substantiveKeys.has(f.fieldKey));
  const hasOfferedServices = hits.some((f) => f.fieldKey === "op.platform.offeredServices");
  return {
    substantiveCount: hits.length,
    hasOfferedServices,
    strongEnoughForPublishScopeLater:
      hasOfferedServices &&
      hits.some(
        (f) =>
          f.fieldKey === "op.platform.offeredServices" &&
          !/F&B Outlets|\d+\s+United States Hotels/i.test(f.extractedValuePreview || "")
      ) &&
      hits.length >= 2,
    notes: hasOfferedServices
      ? "op.platform.offeredServices present — primary PDF enrichment goal."
      : "No op.platform.offeredServices candidate — review before publish-scope inclusion.",
  };
}

export async function previewHotelEquitiesSource(source, targetKeys, options = {}) {
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

  const validation = validateSourceForHotelEquitiesExtract(source, doc);
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
  const enriched = enrichHotelEquitiesCandidates(rawForKeys, source, doc, {
    sourceGroup: options.sourceGroup || "website",
  });
  let { clean, skipped } = filterCleanCandidates(enriched, {
    documentKind: doc.kind,
    sourceGroup: options.sourceGroup || "website",
    limitFacts: options.limitFacts,
  });

  if (options.approvedWebsiteFacts?.length) {
    const againstApproved = filterAgainstApprovedFacts(clean, options.approvedWebsiteFacts, {
      sourceGroup: options.sourceGroup,
    });
    clean = againstApproved.clean;
    skipped = skipped.concat(againstApproved.skipped);
  }

  const extractionQuality =
    doc.kind === "pdf"
      ? {
          kind: "pdf",
          textLength: nz(doc.text).length,
          readable: nz(doc.text).length >= 200,
          note:
            nz(doc.text).length >= 2000
              ? "substantive_pdf_text"
              : nz(doc.text).length >= 200
                ? "sparse_pdf_text"
                : "low_pdf_text_yield",
        }
      : {
          kind: doc.kind,
          textLength: nz(doc.text).length,
          readable: htmlTextClean && nz(doc.text).length >= 100,
          note: htmlTextClean ? "html_clean" : "html_quality_issue",
        };

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
    extractionQuality,
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
      if (
        s.reasons?.includes("duplicate_of_approved_website_fact") ||
        s.reasons?.includes("weaker_than_approved_website_description")
      ) {
        duplicateWarnings.push({ ...s, sourceId: preview.sourceId, sourceTitle: preview.sourceTitle });
      }
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

  const publishScopeStrength = assessPublishScopeStrength(facts);

  return {
    targetFactKeys: targetKeys,
    unsupportedRegistryKeys: [...unsupportedKeys],
    duplicateWarningsAgainstApproved: duplicateWarnings,
    publishScopeStrength,
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
          notesAppend: HE_EXTRACT_BATCH_NOTE,
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
      "All operator PI sources (allowlist only)",
    ],
  };
}

function dedupeGlobalFacts(rows, skipped) {
  const byKeyValue = new Map();
  for (const row of rows) {
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
  return { facts: [...byKeyValue.values()], skipped };
}

async function createHotelEquitiesFacts(source, runId, candidates, operatorId) {
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
      [MAP_PARTNER_FACT.reviewerNotes]: HE_EXTRACT_FACT_NOTE,
    };

    const fact = await createPartnerFact(fields);
    created.push(fact);
  }

  return created;
}

export async function applyHotelEquitiesExtract({ sources, targetKeys, limitFacts, sourcePreviews }) {
  const runId = `pi-he-extract-${randomUUID().slice(0, 8)}`;
  const applyResult = {
    runId,
    batchNote: HE_EXTRACT_BATCH_NOTE,
    sourcesPatched: [],
    factsCreated: [],
    skipped: [],
  };

  const previews =
    sourcePreviews ||
    (await Promise.all(
      sources.map((source) => previewHotelEquitiesSource(source, targetKeys))
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

    const created = await createHotelEquitiesFacts(source, runId, candidates, HE_OPERATOR_ID);

    const existingNotes = String(source.notes || "").trim();
    const noteLine = `${HE_EXTRACT_BATCH_NOTE} run ${runId}`;
    const sourcePatch = {
      [MAP_PARTNER_SOURCE.status]: "Extracted",
      [MAP_PARTNER_SOURCE.extractionRunId]: runId,
    };
    if (!existingNotes.includes(HE_EXTRACT_BATCH_NOTE)) {
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
