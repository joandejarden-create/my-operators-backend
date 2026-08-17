/**
 * Choice legacy mini-batch 1 — batch brand extraction (Comfort, Everhome, Quality).
 * Dry-run default; apply creates Pending facts only.
 * @see docs/data-intelligence/choice-legacy-batch-extraction-v1.md
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
import { getPartnerSourceById, patchPartnerSource } from "./airtable-source.js";
import { isSourceExtractable } from "./sync-reference-folder.js";
import {
  DEFAULT_BATCH_NAME,
  getBatchDefinition,
  getBatchExtractBrandConfigs as getBatchExtractConfigsFromManifest,
  MINI_BATCH_EXTRACT_BRANDS,
} from "./choice-legacy-batch-config.js";
import { buildTargetKeyPlan, validateHtmlTextClean } from "./radisson-blu-extract.js";

export const EXTRACT_VERSION = "1.1";
export const REPORT_JSON_NAME = "choice-legacy-batch-extract.json";
export const REPORT_MD_NAME = "choice-legacy-batch-extract.md";
export const BATCH_EXTRACT_NOTE =
  "Choice legacy mini-batch 1 extraction (Comfort, Everhome, Quality).";
export const BATCH_EXTRACT_FACT_NOTE =
  "Choice legacy mini-batch 1 — Pending fact; human review required before governance.";

export { MINI_BATCH_EXTRACT_BRANDS };

export function getBatchExtractNotes(batchName = DEFAULT_BATCH_NAME) {
  const batch = getBatchDefinition(batchName);
  return {
    extractNote: batch.extractionNote,
    factNote: batch.factNote,
    runPrefix: batch.extractionRunPrefix,
  };
}

export const USER_REQUESTED_KEY_MAPPING = [
  { requested: "be.positioning.segment", registryKey: null, supported: false, note: "Capture segment language in be.positioning.summary when source-backed." },
  { requested: "be.positioning.chainScale", registryKey: null, supported: false, note: "Hotel Chain Scale is Brand Setup — not a PI fact key." },
  { requested: "be.standards.conversionConsiderations", registryKey: "be.overview.developmentModel", supported: true },
  { requested: "be.ownerConsiderations.developmentPositioning", registryKey: "be.overview.whyValue", supported: true },
];

export const PRIMARY_TARGET_FACT_KEYS = [
  "be.identity.brandName",
  "be.identity.parentCompany",
  "be.positioning.summary",
  "be.positioning.guestPromise",
  "be.positioning.tagline",
  "be.overview.developmentModel",
  "be.overview.whyValue",
];

export const SECONDARY_TARGET_FACT_KEYS = [
  "be.overview.typicalUseCase",
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
  "be.overview.typicalUseCase": "P2",
  "be.footprint.geoIntro": "P2",
  "be.loyalty.programName": "P2",
};

export const PRIMARY_PDF_PRIORITY_KEYS = new Set([
  "be.overview.developmentModel",
  "be.overview.whyValue",
  "be.overview.typicalUseCase",
  "be.positioning.summary",
  "be.positioning.guestPromise",
]);

const BLOCKED_URL_PATTERNS = [
  /radissonhotels\.(net|com)/i,
  /radissonhotelgroup/i,
  /\brhg\b/i,
  /choicehotelsdevelopment\.com/i,
  /showpad\.com/i,
];

const MARKUP_RE = /<!DOCTYPE|<html[\s>]|<meta[\s>]|<\/\w+>/i;
const BOOKING_NOISE_RE =
  /\b(Check availability|Book now|Find a hotel|Select dates|Room count|Adults|Children)\b/i;
const VAGUE_MARKETING_RE =
  /\b(world[- ]class|unparalleled|synerg(y|ies)|best[- ]in[- ]class)\b/i;

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeValue(v) {
  return nz(v).toLowerCase().replace(/\s+/g, " ").trim();
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function extractSentence(text, re) {
  const m = text.match(re);
  return m ? m[0].trim() : null;
}

export function getBatchExtractBrandConfigs(batchName = DEFAULT_BATCH_NAME, brandFilter = null) {
  return getBatchExtractConfigsFromManifest(batchName, brandFilter);
}

export function resolveTargetFactKeys(cliKeys) {
  return cliKeys.length ? [...new Set(cliKeys)] : [...DEFAULT_TARGET_FACT_KEYS];
}

function isBlockedUrl(url) {
  if (!url) return false;
  return BLOCKED_URL_PATTERNS.some((re) => re.test(url));
}

function isGapCandidate(c) {
  return (
    c.dataGap === "Yes" ||
    c._extractionSkipped ||
    nz(c.extractedValue) === PARTNER_INTELLIGENCE_GAP_COPY ||
    nz(c.evidenceText) === PARTNER_INTELLIGENCE_GAP_COPY
  );
}

export function assessChoiceLegacyCandidateQuality(candidate, brandConfig) {
  const reasons = [];
  const value = nz(candidate.extractedValue);
  const evidence = nz(candidate.evidenceText);
  const fieldKey = candidate.fieldKey;

  if (!value) reasons.push("empty_value");
  if (isGapCandidate(candidate)) reasons.push("gap_or_skipped");
  if (MARKUP_RE.test(value) || MARKUP_RE.test(evidence)) reasons.push("markup_artifact");
  if (!evidence || evidence === PARTNER_INTELLIGENCE_GAP_COPY) reasons.push("weak_evidence");
  if (value.length < 3) reasons.push("value_too_short");
  if (BOOKING_NOISE_RE.test(value) && value.length < 150) reasons.push("booking_boilerplate");
  if (VAGUE_MARKETING_RE.test(value) && value.length < 80) reasons.push("vague_marketing");
  if (/\bradisson hotel group\b/i.test(value) && !/\bchoice hotels\b/i.test(evidence)) {
    reasons.push("rhg_global_reference");
  }
  if (candidate._registryFallback && fieldKey === "be.identity.parentCompany") {
    reasons.push("registry_fallback_blocked_for_parent");
  }
  if (candidate._aiInterpreted) {
    // allowed but flagged in report — not a blocker
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

function inferBrandName(brandConfig, doc) {
  const name = brandConfig.brandName;
  const re = new RegExp(escapeRegExp(name), "i");
  if (!re.test(doc.text)) return null;
  const sent = extractSentence(doc.text, new RegExp(`${escapeRegExp(name)}[^.]{0,100}\\.`, "i"));
  return { value: name, evidence: sent || name };
}

function inferParentCompany(text) {
  if (/\bChoice Hotels International\b/i.test(text)) {
    return {
      value: "Choice Hotels International",
      evidence:
        extractSentence(text, /Choice Hotels International[^.]{0,100}\./i) ||
        "Choice Hotels International",
    };
  }
  if (/\bChoice Hotels\b/i.test(text)) {
    return {
      value: "Choice Hotels International",
      evidence: extractSentence(text, /Choice Hotels[^.]{0,80}\./i) || "Choice Hotels",
    };
  }
  return null;
}

function inferPositioningSummary(text, brandName) {
  const brandRe = escapeRegExp(brandName.split(" ")[0]);
  const upper = extractSentence(text, new RegExp(`${brandRe}[^.]{0,200}\\.`, "i"));
  if (upper && upper.length >= 40) {
    return { value: upper.replace(/\s+/g, " "), evidence: upper };
  }
  const midscale = extractSentence(text, /(midscale|upper[- ]midscale|extended[- ]stay)[^.]{0,160}\./i);
  if (midscale && new RegExp(brandRe, "i").test(text)) {
    return { value: midscale.replace(/\s+/g, " "), evidence: midscale };
  }
  return null;
}

function inferTagline(text) {
  const quoted = text.match(/"([^"]{8,80})"/);
  if (quoted && !BOOKING_NOISE_RE.test(quoted[1])) {
    return { value: quoted[1], evidence: quoted[0] };
  }
  return null;
}

function inferGuestPromise(text) {
  const promise = extractSentence(
    text,
    /(guest[s]? (can|will|enjoy|experience)|we promise|our promise)[^.]{0,140}\./i
  );
  if (promise) return { value: promise.replace(/\s+/g, " "), evidence: promise };
  const comfort = extractSentence(text, /(comfortable|reliable|consistent)[^.]{0,120}\./i);
  if (comfort) return { value: comfort.replace(/\s+/g, " "), evidence: comfort };
  return null;
}

function inferDevelopmentModel(text) {
  const parts = [];
  if (/new build|new construction|ground[- ]up/i.test(text)) parts.push("new construction");
  if (/conversion/i.test(text)) parts.push("conversions");
  if (/adaptive reuse/i.test(text)) parts.push("adaptive reuse");
  if (/franchise/i.test(text) && !parts.length) parts.push("franchise development");
  if (!parts.length) return null;
  const evidence =
    extractSentence(
      text,
      /(new build|new construction|conversion|adaptive reuse|franchise)[^.]{0,140}\./i
    ) || parts.join(", ");
  return {
    value: `Development types include ${parts.join(", ")} (per Choice brand materials).`,
    evidence,
  };
}

function inferWhyValue(text, sourceId, brandConfig) {
  if (sourceId !== brandConfig.primaryPdfSourceId) return null;
  const owner = extractSentence(
    text,
    /(owner|franchisee|developer|investor)[^.]{0,160}\./i
  );
  if (owner && owner.length >= 30) {
    return { value: owner.replace(/\s+/g, " "), evidence: owner };
  }
  const value = extractSentence(text, /(value proposition|why choose)[^.]{0,160}\./i);
  if (value) return { value: value.replace(/\s+/g, " "), evidence: value };
  return null;
}

function inferTypicalUseCase(text, sourceId, brandConfig) {
  if (sourceId !== brandConfig.primaryPdfSourceId) return null;
  const segment = extractSentence(
    text,
    /(target guest|ideal guest|traveler|guest profile)[^.]{0,160}\./i
  );
  if (segment) return { value: segment.replace(/\s+/g, " "), evidence: segment };
  return null;
}

function inferGeoIntro(text, sourceId, brandConfig) {
  if (sourceId !== brandConfig.pressSourceId) return null;
  const hotels = extractSentence(text, /(\d+[,\d]*)\s+hotels?[^.]{0,100}\./i);
  if (hotels) {
    return {
      value: hotels.replace(/\s+/g, " "),
      evidence: hotels,
      _aiInterpreted: false,
    };
  }
  const americas = extractSentence(text, /(Americas|United States|North America)[^.]{0,120}\./i);
  if (americas) {
    return { value: americas.replace(/\s+/g, " "), evidence: americas };
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

export function enrichChoiceLegacyCandidates(candidates, source, doc, brandConfig) {
  const out = [...candidates];
  const text = nz(doc.text);
  const anchor = source.localFilePath || source.sourceUrl || source.sourceTitle;
  const hasClean = (key) =>
    out.some((c) => c.fieldKey === key && assessChoiceLegacyCandidateQuality(c, brandConfig).clean);

  const enrichments = [
    ["be.identity.brandName", inferBrandName(brandConfig, doc)],
    ["be.identity.parentCompany", inferParentCompany(text)],
    ["be.positioning.summary", inferPositioningSummary(text, brandConfig.brandName)],
    ["be.positioning.tagline", inferTagline(text)],
    ["be.positioning.guestPromise", inferGuestPromise(text)],
    ["be.overview.developmentModel", inferDevelopmentModel(text)],
    ["be.overview.whyValue", inferWhyValue(text, source.id, brandConfig)],
    ["be.overview.typicalUseCase", inferTypicalUseCase(text, source.id, brandConfig)],
    ["be.footprint.geoIntro", inferGeoIntro(text, source.id, brandConfig)],
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
      _aiInterpreted: hit._aiInterpreted === true,
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
  return tagged.filter((c) => keySet.has(c.fieldKey) && !isGapCandidate(c));
}

export function filterCleanCandidates(candidates, brandConfig, options = {}) {
  const skipped = [];
  const clean = [];

  for (const c of candidates) {
    if (isGapCandidate(c)) {
      skipped.push({
        fieldKey: c.fieldKey,
        sourceId: c._sourceId,
        extractedValuePreview: String(c.extractedValue || "").slice(0, 160),
        reasons: ["gap_fact_skipped"],
      });
      continue;
    }
    const assessment = assessChoiceLegacyCandidateQuality(c, brandConfig);
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

export function validateSourceForChoiceLegacyExtract(source, doc, brandConfig) {
  const reasons = [];
  const allowlist = new Set(brandConfig.allowlistedSourceIds);

  if (!allowlist.has(source.id)) reasons.push("source_not_in_brand_allowlist");
  if (source.brandId !== brandConfig.recordId) reasons.push("not_linked_to_target_brand");
  if (nz(source.approvedForExtraction) !== "Yes") reasons.push("source_not_approved_for_extraction");
  if (nz(source.status) === "Stale" || nz(source.status) === "Rejected") {
    reasons.push(`source_status_${nz(source.status).toLowerCase()}`);
  }
  if (!isSourceExtractable(source)) reasons.push("source_not_extractable_no_file_or_url");

  const url = nz(source.sourceUrl);
  if (url && isBlockedUrl(url)) reasons.push("blocked_development_or_third_party_url");

  const htmlCheck = validateHtmlTextClean(doc);
  if (!htmlCheck.ok) reasons.push(...htmlCheck.reasons);

  return { ok: reasons.length === 0, reasons };
}

export function summarizeExistingFacts(facts, brandConfig, sourceIds, targetKeys) {
  const sourceSet = new Set(sourceIds);
  const keySet = new Set(targetKeys);
  const rows = [];
  const existingKeyValues = new Set();

  for (const fact of facts || []) {
    if (fact.brandId !== brandConfig.recordId) continue;
    existingKeyValues.add(`${fact.fieldName}::${normalizeValue(fact.extractedValue)}`);
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

  return {
    rows,
    existingCount: rows.reduce((n, r) => n + r.existingCount, 0),
    existingKeyValues: [...existingKeyValues],
  };
}

export async function previewChoiceLegacySource(source, brandConfig, targetKeys, options = {}) {
  const keyPlan = buildTargetKeyPlan(targetKeys).map((k) => ({
    ...k,
    priority: TARGET_KEY_PRIORITY[k.fieldKey] || k.priority,
  }));
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

  const validation = validateSourceForChoiceLegacyExtract(source, doc, brandConfig);
  const htmlTextClean = validateHtmlTextClean(doc).ok;

  if (!validation.ok) {
    return {
      sourceId: source.id,
      sourceTitle: source.sourceTitle,
      sourceUrl: source.sourceUrl,
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
  const enriched = enrichChoiceLegacyCandidates(rawForKeys, source, doc, brandConfig);
  const { clean, skipped } = filterCleanCandidates(enriched, brandConfig, {
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
      aiInterpreted: Boolean(c._aiInterpreted),
      wouldWriteOnApply: true,
      _candidate: c,
    })),
    skippedCandidates: skipped,
  };
}

function dedupeGlobalFacts(rows, brandConfig, skipped) {
  const singularKeys = new Set([
    "be.identity.brandName",
    "be.identity.parentCompany",
    "be.positioning.summary",
    "be.positioning.tagline",
    "be.positioning.guestPromise",
    "be.overview.developmentModel",
    "be.overview.whyValue",
    "be.overview.typicalUseCase",
    "be.footprint.geoIntro",
    "be.loyalty.programName",
  ]);

  function score(row) {
    let s = row._candidate?.confidenceScore || 0;
    if (row.enriched || row._candidate?._enriched) s += 20;
    if (row.sourceId === brandConfig.primaryPdfSourceId && PRIMARY_PDF_PRIORITY_KEYS.has(row.fieldKey)) {
      s += 30;
    }
    if (row.sourceId === brandConfig.pressSourceId && row.fieldKey === "be.footprint.geoIntro") {
      s += 15;
    }
    if (row.sourceId === brandConfig.consumerSourceId && row.fieldKey === "be.positioning.guestPromise") {
      s += 15;
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

function filterAgainstExistingAirtableFacts(facts, existingKeyValues, skipped) {
  const existing = new Set(existingKeyValues || []);
  const kept = [];
  for (const row of facts) {
    const compound = `${row.fieldKey}::${normalizeValue(row.extractedValuePreview)}`;
    if (existing.has(compound)) {
      skipped.push({
        fieldKey: row.fieldKey,
        sourceId: row.sourceId,
        extractedValuePreview: row.extractedValuePreview,
        reasons: ["duplicate_existing_airtable_fact"],
      });
      continue;
    }
    kept.push(row);
  }
  return kept;
}

export function buildBrandWouldWritePlan(
  sourcePreviews,
  brandConfig,
  targetKeys,
  existingKeyValues,
  options = {}
) {
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
        aiInterpreted: c.aiInterpreted,
        _candidate: c._candidate,
      });
    }
  }

  const globalDeduped = dedupeGlobalFacts(allCandidates, brandConfig, allSkipped);
  for (const row of globalDeduped.skipped) {
    if (row.reasons?.includes("duplicate_field_key_keep_best")) {
      duplicateWarnings.push(row);
    }
  }

  let facts = filterAgainstExistingAirtableFacts(
    globalDeduped.facts,
    existingKeyValues,
    allSkipped
  );

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
  const highConfidence = facts.filter((f) => f.confidenceLevel === "High").length;
  const needsReview = facts.filter((f) => f.aiInterpreted || f.confidenceLevel !== "High").length;

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
          notesAppend: BATCH_EXTRACT_NOTE,
        },
      })),
    factsWouldCreateCount: facts.length,
    highConfidenceFactCount: highConfidence,
    factsNeedingReviewCount: needsReview,
    doesNotWrite: [
      "Human Review Status = Approved (facts remain Pending)",
      "Approved for Explorer Use changes",
      "Brand Setup profile governance fields",
      "Company Validated / Company Validation Date",
      "Governance publish / platform field publishing",
      "Gap facts",
      "Apply without --approve-choice-legacy-batch-extract",
    ],
  };
}

export function assessBrandExtractionQuality(brandConfig, sourcePreviews, wouldWrite) {
  const substantiveKeys = new Set([
    "be.positioning.summary",
    "be.identity.parentCompany",
    "be.positioning.guestPromise",
    "be.overview.developmentModel",
    "be.overview.whyValue",
  ]);
  const proposed = wouldWrite.factRowsWouldCreate || [];
  const substantive = proposed.filter((f) => substantiveKeys.has(f.fieldKey));
  const allHtmlClean = sourcePreviews.every((p) => p.htmlTextClean !== false);
  const readableSources = sourcePreviews.filter(
    (p) => p.previewAvailable && (p.textLength || 0) >= 500
  ).length;
  const sourcesInScope = sourcePreviews.filter((p) => p.validation.ok).length;

  return {
    brandKey: brandConfig.key,
    brandName: brandConfig.brandName,
    recordId: brandConfig.recordId,
    sourcesInScope,
    allHtmlTextClean: allHtmlClean,
    readableSourceCount: readableSources,
    substantiveFactCount: substantive.length,
    hasBrandName: proposed.some((f) => f.fieldKey === "be.identity.brandName"),
    hasParentCompany: proposed.some((f) => f.fieldKey === "be.identity.parentCompany"),
    hasDevelopmentModel: proposed.some((f) => f.fieldKey === "be.overview.developmentModel"),
    proposedFactCount: proposed.length,
    highConfidenceFactCount: wouldWrite.highConfidenceFactCount,
    factsNeedingReviewCount: wouldWrite.factsNeedingReviewCount,
    overall:
      allHtmlClean && substantive.length >= 3 && proposed.length >= 5
        ? "good_for_steward_review"
        : substantive.length >= 2
          ? "moderate_review_carefully"
          : "weak_needs_more_sources_or_curation",
    applyRecommended:
      sourcesInScope >= 2 && allHtmlClean && substantive.length >= 3 && proposed.length >= 4,
    splitOutRecommended: false,
    splitOutReason: null,
    governanceReadinessAfterFactApproval:
      substantive.length >= 3
        ? "likely_eligible_for_stewardship_recompute_and_publish_readiness_audit"
        : "blocked_until_more_approved_facts",
    risks: [
      "Facts remain Pending until human stewardship approval.",
      "Do not treat extracted positioning as Company Validated.",
      "Consumer HTML may include booking boilerplate — review evidence quotes.",
    ],
  };
}

async function createChoiceLegacyFacts(source, runId, candidates, brandConfig, factNote) {
  const today = new Date().toISOString().slice(0, 10);
  const created = [];

  for (const c of candidates) {
    const assessment = assessChoiceLegacyCandidateQuality(c, brandConfig);
    if (!assessment.clean || isGapCandidate(c)) continue;

    const reg = getRegistryField(c.fieldKey, "Brand Explorer");
    const fields = {
      "Source Title": `${reg?.displayLabel || c.fieldKey} — ${runId}`,
      [MAP_PARTNER_FACT.profileType]: "Brand",
      [MAP_PARTNER_FACT.brand]: [brandConfig.recordId],
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
      [MAP_PARTNER_FACT.dataGap]: "No",
      [MAP_PARTNER_FACT.followUpQuestion]: c.followUpQuestion || "",
      [MAP_PARTNER_FACT.lastUpdated]: today,
      [MAP_PARTNER_FACT.extractionRunId]: runId,
      [MAP_PARTNER_FACT.reviewerNotes]: c._aiInterpreted
        ? `${factNote} AI-interpreted from company materials.`
        : factNote,
    };

    const fact = await createPartnerFact(fields);
    created.push(fact);
  }

  return created;
}

export async function applyChoiceLegacyBatchExtract({
  brandReports,
  targetKeys,
  limitFacts,
  batchName = DEFAULT_BATCH_NAME,
}) {
  const { extractNote, factNote, runPrefix } = getBatchExtractNotes(batchName);
  const runId = `${runPrefix}${randomUUID().slice(0, 8)}`;
  const applyResult = {
    runId,
    batchName,
    batchNote: extractNote,
    brands: [],
    factsCreated: [],
    sourcesPatched: [],
    skipped: [],
  };

  for (const brandReport of brandReports) {
    const brandConfig = brandReport.brandConfig;
    const brandApply = {
      brandKey: brandConfig.key,
      brandName: brandConfig.brandName,
      recordId: brandConfig.recordId,
      factsCreated: [],
      sourcesPatched: [],
      skipped: [],
    };

    const bySource = new Map();
    for (const row of brandReport.wouldWrite.proposedCandidates || []) {
      if (!row._candidate) continue;
      if (!bySource.has(row.sourceId)) bySource.set(row.sourceId, []);
      bySource.get(row.sourceId).push(row._candidate);
    }

    for (const source of brandReport.sources) {
      const candidates = bySource.get(source.id) || [];
      if (!candidates.length) {
        const preview = brandReport.sourcePreviews.find((p) => p.sourceId === source.id);
        brandApply.skipped.push({
          sourceId: source.id,
          type: "source",
          reasons: [preview?.previewSkippedReason || "no_clean_candidates"],
        });
        continue;
      }

      const created = await createChoiceLegacyFacts(source, runId, candidates, brandConfig, factNote);
      const existingNotes = String(source.notes || "").trim();
      const noteLine = `${extractNote} run ${runId}`;
      const sourcePatch = {
        [MAP_PARTNER_SOURCE.status]: "Extracted",
        [MAP_PARTNER_SOURCE.extractionRunId]: runId,
      };
      if (!existingNotes.includes(extractNote)) {
        sourcePatch[MAP_PARTNER_SOURCE.notes] = existingNotes
          ? `${existingNotes}\n${noteLine}`
          : noteLine;
      }

      await patchPartnerSource(source.id, sourcePatch);

      brandApply.sourcesPatched.push({
        sourceId: source.id,
        sourceTitle: source.sourceTitle,
        patch: sourcePatch,
        factsCreated: created.length,
      });
      brandApply.factsCreated.push(
        ...created.map((f) => ({
          id: f.id,
          fieldName: f.fieldName,
          sourceRecordId: source.id,
          extractedValuePreview: String(f.extractedValue || "").slice(0, 120),
        }))
      );
    }

    applyResult.brands.push(brandApply);
    applyResult.factsCreated.push(...brandApply.factsCreated);
    applyResult.sourcesPatched.push(...brandApply.sourcesPatched);
    applyResult.skipped.push(...brandApply.skipped);
  }

  return applyResult;
}

export async function buildChoiceLegacyBatchExtractBrandReport(
  brandConfig,
  { targetKeys, existingFacts, limitFacts } = {}
) {
  const keys = resolveTargetFactKeys(targetKeys || []);
  const sources = [];
  for (const sourceId of brandConfig.allowlistedSourceIds) {
    const source = await getPartnerSourceById(sourceId);
    if (!source) {
      throw new Error(`Source not found: ${sourceId} (${brandConfig.brandName})`);
    }
    sources.push(source);
  }

  const existingSummary = summarizeExistingFacts(
    existingFacts,
    brandConfig,
    brandConfig.allowlistedSourceIds,
    keys
  );

  const sourcePreviews = [];
  for (const source of sources) {
    sourcePreviews.push(
      await previewChoiceLegacySource(source, brandConfig, keys, { limitFacts })
    );
  }

  const wouldWrite = buildBrandWouldWritePlan(
    sourcePreviews,
    brandConfig,
    keys,
    existingSummary.existingKeyValues,
    { limitFacts }
  );
  const extractionQuality = assessBrandExtractionQuality(
    brandConfig,
    sourcePreviews,
    wouldWrite
  );

  return {
    brandConfig,
    brandName: brandConfig.brandName,
    recordId: brandConfig.recordId,
    allowlistedSourceIds: brandConfig.allowlistedSourceIds,
    sources: sources.map((s) => ({
      id: s.id,
      sourceTitle: s.sourceTitle,
      sourceType: s.sourceType,
      sourceUrl: s.sourceUrl,
      status: s.status,
      approvedForExtraction: s.approvedForExtraction,
      localFilePath: s.localFilePath,
    })),
    sourcesInScope: sourcePreviews.filter((p) => p.validation.ok).map((p) => p.sourceId),
    existingFacts: existingSummary,
    sourcePreviews,
    wouldWrite,
    extractionQuality,
  };
}

export async function buildChoiceLegacyBatchExtractReport({
  brandFilter = null,
  batchName = DEFAULT_BATCH_NAME,
  targetKeys = [],
  limitFacts = null,
  existingFactsByBrand = new Map(),
  brandConfigsOverride = null,
} = {}) {
  const batch = getBatchDefinition(batchName);
  const brandConfigs =
    brandConfigsOverride && brandConfigsOverride.length
      ? brandConfigsOverride
      : getBatchExtractBrandConfigs(batchName, brandFilter);
  const keys = resolveTargetFactKeys(targetKeys);
  const keyPlan = buildTargetKeyPlan(keys).map((k) => ({
    ...k,
    priority: TARGET_KEY_PRIORITY[k.fieldKey] || k.priority,
  }));

  const brands = [];
  for (const brandConfig of brandConfigs) {
    const existingFacts = existingFactsByBrand.get(brandConfig.recordId) || [];
    brands.push(
      await buildChoiceLegacyBatchExtractBrandReport(brandConfig, {
        targetKeys: keys,
        existingFacts,
        limitFacts,
      })
    );
  }

  const totalProposed = brands.reduce((n, b) => n + b.wouldWrite.factsWouldCreateCount, 0);
  const totalHighConfidence = brands.reduce(
    (n, b) => n + b.wouldWrite.highConfidenceFactCount,
    0
  );
  const totalNeedsReview = brands.reduce(
    (n, b) => n + b.wouldWrite.factsNeedingReviewCount,
    0
  );
  const readyForApply = brands.filter((b) => b.extractionQuality.applyRecommended);
  const splitOut = brands.filter((b) => b.extractionQuality.splitOutRecommended);

  const batchApplyCommand = `npm run choice-legacy-batch-extract -- --batch ${batchName} --apply --approve-choice-legacy-batch-extract`;

  return {
    extractVersion: EXTRACT_VERSION,
    batchName,
    batchDisplayName: batch.displayName,
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    targetFactKeys: keys,
    keyPlan,
    userRequestedKeyMapping: USER_REQUESTED_KEY_MAPPING,
    brands,
    summary: {
      totalBrands: brands.length,
      totalSourcesInScope: brands.reduce((n, b) => n + b.sourcesInScope.length, 0),
      totalProposedFacts: totalProposed,
      proposedFactsByBrand: Object.fromEntries(
        brands.map((b) => [b.brandName, b.wouldWrite.factsWouldCreateCount])
      ),
      highConfidenceFacts: totalHighConfidence,
      factsNeedingReview: totalNeedsReview,
      brandsReadyForBatchApply: readyForApply.length,
      brandsToSplitOut: splitOut.length,
      duplicateWarnings: brands.reduce((n, b) => n + b.wouldWrite.duplicateWarnings.length, 0),
    },
    batchApplyCommand,
    nextRecommendedCommand:
      "npm run steward-partner-intelligence -- --entity-type brand --target-rec-id <rec…> --dry-run --recompute",
    perBrandStewardCommands: brands.map(
      (b) =>
        `npm run steward-partner-intelligence -- --entity-type brand --target-rec-id ${b.recordId} --dry-run --recompute`
    ),
    doesNotDo: [
      "Rebuild Brand Explorer content or overwrite Brand Setup fields",
      "Approve facts automatically",
      "Publish governance or set Company Validated",
      "Extract from development JS-shell pages or RHG/global sources",
      "Create gap facts",
      "Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema",
    ],
  };
}

export function buildChoiceLegacyBatchExtractMarkdown(report) {
  const s = report.summary;
  const lines = [
    "# Choice Legacy Mini-Batch Extraction v1",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Executive summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Brands | ${s.totalBrands} |`,
    `| Sources in scope | ${s.totalSourcesInScope} |`,
    `| Proposed facts (total) | ${s.totalProposedFacts} |`,
    `| High-confidence facts | ${s.highConfidenceFacts} |`,
    `| Facts needing review | ${s.factsNeedingReview} |`,
    `| Brands ready for batch apply | ${s.brandsReadyForBatchApply} |`,
    `| Brands to split out | ${s.brandsToSplitOut} |`,
    `| Duplicate warnings | ${s.duplicateWarnings} |`,
    "",
    "### Proposed facts by brand",
    "",
  ];

  for (const [name, count] of Object.entries(s.proposedFactsByBrand || {})) {
    lines.push(`- **${name}**: ${count}`);
  }

  lines.push("", "### Batch apply command", "", "```bash", report.batchApplyCommand, "```", "");

  for (const brand of report.brands) {
    const eq = brand.extractionQuality;
    lines.push(`## ${brand.brandName}`, "");
    lines.push(
      `- Record: \`${brand.recordId}\``,
      `- Sources in scope: ${brand.sourcesInScope.map((id) => `\`${id}\``).join(", ") || "none"}`,
      `- Proposed facts: **${brand.wouldWrite.factsWouldCreateCount}**`,
      `- Skipped candidates: ${brand.wouldWrite.factRowsSkipped.length}`,
      `- Extraction quality: **${eq.overall}**`,
      `- Apply recommended: **${eq.applyRecommended ? "yes" : "no"}**`,
      `- Split out: **${eq.splitOutRecommended ? "yes" : "no"}**`,
      `- Governance readiness (after fact approval): ${eq.governanceReadinessAfterFactApproval}`
    );
    lines.push("", "### Sources", "");
    for (const src of brand.sources) {
      lines.push(
        `- \`${src.id}\` — ${src.sourceTitle} (${src.sourceType}) · extraction=${src.approvedForExtraction}`
      );
    }
    lines.push("", "### Proposed facts", "");
    if (!brand.wouldWrite.factRowsWouldCreate.length) {
      lines.push("_None._", "");
    } else {
      lines.push("| Field | Source | Value preview | Confidence |", "|-------|--------|---------------|------------|");
      for (const row of brand.wouldWrite.factRowsWouldCreate) {
        lines.push(
          `| \`${row.fieldKey}\` | ${row.sourceTitle} | ${String(row.extractedValuePreview || "").slice(0, 80)} | ${row.confidenceLevel || "—"} |`
        );
      }
      lines.push("");
    }
    if (brand.wouldWrite.duplicateWarnings?.length) {
      lines.push("### Duplicate warnings", "");
      for (const w of brand.wouldWrite.duplicateWarnings) {
        lines.push(`- \`${w.fieldKey}\` from \`${w.sourceId}\``);
      }
      lines.push("");
    }
    if (brand.wouldWrite.unsupportedRegistryKeys?.length) {
      lines.push(
        `### Unsupported keys: ${brand.wouldWrite.unsupportedRegistryKeys.map((k) => `\`${k}\``).join(", ")}`,
        ""
      );
    }
    if (eq.risks?.length) {
      lines.push("### Risks / caveats", "");
      for (const r of eq.risks) lines.push(`- ${r}`);
      lines.push("");
    }
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) lines.push(`- ${item}`);
  lines.push("");

  return lines.join("\n");
}
