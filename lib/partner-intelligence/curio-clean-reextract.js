/**
 * Curio narrow clean re-extraction (dry-run preview + controlled apply).
 * @see docs/data-intelligence/curio-clean-reextraction-plan.md
 */
import { randomUUID } from "crypto";
import {
  MAP_PARTNER_FACT,
  MAP_PARTNER_SOURCE,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  getRegistryField,
  listBrandFieldsForExtractionRegistry,
} from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import { extractFromBrandSourceDocument } from "./run-extraction.js";
import { createPartnerFact } from "./airtable-facts.js";
import { patchPartnerSource } from "./airtable-source.js";
import { isSourceExtractable } from "./sync-reference-folder.js";
import {
  CURIO_BRAND_ID,
  PRIMARY_CONTAMINATED_SOURCE_ID,
  assessFactContamination,
} from "./curio-fact-contamination.js";

export const REPORT_JSON_NAME = "curio-clean-reextract.json";
export const REPORT_MD_NAME = "curio-clean-reextract.md";

/** Hard-coded allowlist — broaden only via explicit code change. */
export const ALLOWLISTED_SOURCE_IDS = new Set([
  "recy2pyEahF9UUsEk",
  "recL1qfHCOAUZr9Rz",
]);

export const BLOCKED_SOURCE_IDS = new Set([PRIMARY_CONTAMINATED_SOURCE_ID]);

export const CLEAN_REEXTRACT_BATCH_NOTE = "Curio clean re-extraction allowlist.";
export const CLEAN_REEXTRACT_FACT_NOTE = "Curio clean re-extraction allowlist.";

export const LOYALTY_FIELD_PREFIX = "be.loyalty.";

export const DEFAULT_TARGET_FACT_KEYS = [
  "be.identity.brandName",
  "be.identity.parentCompany",
  "be.positioning.summary",
  "be.positioning.tagline",
  "be.positioning.guestPromise",
  "be.overview.typicalUseCase",
  "be.development.conversionRelevance",
  "be.development.ownerConsiderations",
  "be.footprint.globalHotels",
  "be.footprint.regionalPresence",
];

export const TARGET_KEY_PRIORITY = {
  "be.identity.brandName": "P0",
  "be.identity.parentCompany": "P0",
  "be.positioning.summary": "P0",
  "be.positioning.tagline": "P1",
  "be.positioning.guestPromise": "P1",
  "be.overview.typicalUseCase": "P1",
  "be.development.conversionRelevance": "P2",
  "be.development.ownerConsiderations": "P2",
  "be.footprint.globalHotels": "P2",
  "be.footprint.regionalPresence": "P2",
};

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

export function isLoyaltyFieldKey(fieldKey) {
  return String(fieldKey || "").startsWith(LOYALTY_FIELD_PREFIX);
}

export function assertSourceAllowlisted(sourceId) {
  const id = String(sourceId || "").trim();
  if (!id) return { ok: false, reasons: ["missing_source_id"] };
  if (BLOCKED_SOURCE_IDS.has(id)) {
    return { ok: false, reasons: ["source_blocked_contamination_marker"] };
  }
  if (!ALLOWLISTED_SOURCE_IDS.has(id)) {
    return { ok: false, reasons: ["source_not_in_curio_clean_allowlist"] };
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
  const requested = cliKeys.length ? cliKeys : [...DEFAULT_TARGET_FACT_KEYS];
  const loyalty = requested.filter(isLoyaltyFieldKey);
  if (loyalty.length) {
    throw new Error(`Loyalty field keys are excluded from Curio clean path: ${loyalty.join(", ")}`);
  }
  return [...new Set(requested)];
}

export function buildTargetKeyPlan(factKeys) {
  const registry = listBrandFieldsForExtractionRegistry();
  const registryByKey = new Map(registry.map((f) => [f.fieldKey, f]));

  return factKeys.map((fieldKey) => {
    const reg = registryByKey.get(fieldKey) || getRegistryField(fieldKey, "Brand Explorer");
    return {
      fieldKey,
      priority: TARGET_KEY_PRIORITY[fieldKey] || "—",
      registrySupported: Boolean(reg),
      displayLabel: reg?.displayLabel || null,
      explorerSection: reg?.explorerSection || null,
      excludedReason: isLoyaltyFieldKey(fieldKey) ? "loyalty_field_excluded" : null,
    };
  });
}

export function validateSourceForCurioClean(source) {
  const reasons = [];
  const allow = assertSourceAllowlisted(source.id);
  if (!allow.ok) reasons.push(...allow.reasons);

  if (source.brandId !== CURIO_BRAND_ID) {
    reasons.push("not_linked_to_curio_brand");
  }
  if (source.status === "Stale") {
    reasons.push("source_status_stale");
  }
  if (!isSourceExtractable(source)) {
    reasons.push("source_not_extractable_no_file_or_url");
  }
  if (String(source.sourceTitle || "").toLowerCase().includes("mexico curio fdd")) {
    reasons.push("known_contaminated_source_title_marker");
  }

  return {
    ok: reasons.length === 0,
    reasons,
  };
}

export function indexExistingFacts(facts, sourceIds, targetKeys) {
  const sourceSet = new Set(sourceIds);
  const keySet = new Set(targetKeys);
  const bySourceKey = new Map();

  for (const fact of facts || []) {
    if (fact.brandId !== CURIO_BRAND_ID) continue;
    if (!sourceSet.has(fact.sourceRecordId)) continue;
    if (!keySet.has(fact.fieldName)) continue;

    const key = `${fact.sourceRecordId}:${fact.fieldName}`;
    if (!bySourceKey.has(key)) bySourceKey.set(key, []);
    bySourceKey.get(key).push(fact);
  }

  return bySourceKey;
}

export function summarizeExistingFacts(facts, sourceIds, targetKeys) {
  const index = indexExistingFacts(facts, sourceIds, targetKeys);
  const rows = [];
  let contaminatedCount = 0;

  for (const [compoundKey, factRows] of index.entries()) {
    const [sourceId, fieldKey] = compoundKey.split(":");
    const warnings = [];
    for (const fact of factRows) {
      const assessment = assessFactContamination(fact, { includeSecondary: true });
      if (assessment.contaminated) {
        contaminatedCount += 1;
        warnings.push({
          factId: fact.id,
          humanReviewStatus: fact.humanReviewStatus,
          reasons: assessment.reasons,
          extractedValuePreview: String(fact.extractedValue || "").slice(0, 120),
        });
      }
    }
    rows.push({
      sourceId,
      fieldKey,
      existingCount: factRows.length,
      factIds: factRows.map((f) => f.id),
      contaminationWarnings: warnings,
    });
  }

  return { rows, contaminatedCount };
}

function filterRegistryForTargetKeys(registryFields, targetKeys) {
  const keySet = new Set(targetKeys);
  return registryFields.filter((f) => keySet.has(f.fieldKey));
}

function pickPreviewCandidates(tagged, targetKeys, limitFacts) {
  const keySet = new Set(targetKeys);
  const filtered = tagged.filter((c) => keySet.has(c.fieldKey) && !isLoyaltyFieldKey(c.fieldKey));
  filtered.sort((a, b) => {
    const pa = TARGET_KEY_PRIORITY[a.fieldKey] || "P9";
    const pb = TARGET_KEY_PRIORITY[b.fieldKey] || "P9";
    return pa.localeCompare(pb);
  });
  return typeof limitFacts === "number" && limitFacts > 0 ? filtered.slice(0, limitFacts) : filtered;
}

function contaminationWarningsForCandidate(candidate) {
  const pseudoFact = {
    fieldName: candidate.fieldKey,
    extractedValue: candidate.extractedValue,
    approvedValue: "",
    evidenceText: candidate.evidenceText,
    sourceRecordId: candidate._sourceId,
    brandId: CURIO_BRAND_ID,
  };
  const assessment = assessFactContamination(pseudoFact, { includeSecondary: true });
  return assessment.contaminated ? assessment.reasons : [];
}

export async function previewCurioCleanSource(source, targetKeys, options = {}) {
  const validation = validateSourceForCurioClean(source);
  const keyPlan = buildTargetKeyPlan(targetKeys);
  const supportedKeys = keyPlan.filter((k) => k.registrySupported).map((k) => k.fieldKey);
  const unsupportedKeys = keyPlan.filter((k) => !k.registrySupported).map((k) => k.fieldKey);

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
      previewCandidates: [],
      documentKind: null,
      classificationRole: null,
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
      previewCandidates: [],
      documentKind: null,
      classificationRole: null,
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
      previewSkippedReason: `extraction_preview_error: ${err.message || String(err)}`,
      previewCandidates: [],
      documentKind: null,
      classificationRole: null,
    };
  }

  const candidates = pickPreviewCandidates(
    extraction.tagged,
    supportedKeys,
    options.limitFacts
  ).map((c) => ({
    fieldKey: c.fieldKey,
    priority: TARGET_KEY_PRIORITY[c.fieldKey] || "—",
    extractedValuePreview: String(c.extractedValue || "").slice(0, 200),
    evidencePreview: String(c.evidenceText || "").slice(0, 200),
    extractionType: c.extractionType,
    confidenceLevel: c.confidenceLevel,
    dataGap: c.dataGap || "No",
    contaminationWarnings: contaminationWarningsForCandidate(c),
    wouldWriteOnApply: true,
  }));

  return {
    sourceId: source.id,
    sourceTitle: source.sourceTitle,
    validation,
    keyPlan,
    supportedKeys,
    unsupportedKeys,
    previewAvailable: true,
    previewSkippedReason: null,
    documentKind: extraction.documentKind,
    classificationRole: extraction.classification?.role || null,
    extractor: extraction.extractor,
    rawCandidateCount: extraction.tagged.length,
    previewCandidates: candidates,
  };
}

export function buildWouldWritePlan(sourcePreviews, targetKeys) {
  const unsupportedKeys = new Set();
  const rows = [];

  for (const preview of sourcePreviews) {
    for (const key of preview.unsupportedKeys || []) unsupportedKeys.add(key);
    if (!preview.previewAvailable) continue;
    for (const c of preview.previewCandidates || []) {
      rows.push({
        sourceId: preview.sourceId,
        sourceTitle: preview.sourceTitle,
        fieldKey: c.fieldKey,
        priority: c.priority,
        extractedValuePreview: c.extractedValuePreview,
        contaminationWarnings: c.contaminationWarnings,
        blockedOnApply: (c.contaminationWarnings || []).length > 0,
      });
    }
  }

  return {
    targetFactKeys: targetKeys,
    unsupportedRegistryKeys: [...unsupportedKeys],
    factRowsWouldCreate: rows.filter((r) => !r.blockedOnApply),
    factRowsBlockedByContamination: rows.filter((r) => r.blockedOnApply),
    sourcesWouldPatch: sourcePreviews
      .filter((p) => p.validation.ok && p.previewAvailable)
      .map((p) => ({
        sourceId: p.sourceId,
        sourceTitle: p.sourceTitle,
        fields: {
          status: "Extracted",
          extractionRunId: "(assigned on apply)",
          notesAppend: CLEAN_REEXTRACT_BATCH_NOTE,
        },
      })),
    factsWouldCreateCount: rows.filter((r) => !r.blockedOnApply).length,
    doesNotWrite: [
      "Approved for Explorer Use",
      "Human Review Status (facts remain Pending)",
      "Brand Basics profile governance fields",
      "Company Validated / Company Validation Date",
      "External Display Status / Show Trust Label",
    ],
  };
}

async function createCurioCleanFacts(source, runId, candidates, brandId) {
  const today = new Date().toISOString().slice(0, 10);
  const created = [];

  for (const c of candidates) {
    const warnings = contaminationWarningsForCandidate(c);
    if (warnings.length) continue;

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
      [MAP_PARTNER_FACT.reviewerNotes]: CLEAN_REEXTRACT_FACT_NOTE,
    };

    const fact = await createPartnerFact(fields);
    created.push(fact);
  }

  return created;
}

export async function applyCurioCleanReextract({ sources, targetKeys, limitFacts }) {
  const runId = `pi-curio-clean-${randomUUID().slice(0, 8)}`;
  const applyResult = {
    runId,
    batchNote: CLEAN_REEXTRACT_BATCH_NOTE,
    sourcesPatched: [],
    factsCreated: [],
    skipped: [],
  };

  for (const source of sources) {
    const validation = validateSourceForCurioClean(source);
    if (!validation.ok) {
      applyResult.skipped.push({
        sourceId: source.id,
        type: "source",
        reasons: validation.reasons,
      });
      continue;
    }

    const preview = await previewCurioCleanSource(source, targetKeys, { limitFacts });
    if (!preview.previewAvailable) {
      applyResult.skipped.push({
        sourceId: source.id,
        type: "source",
        reasons: [preview.previewSkippedReason || "preview_unavailable"],
      });
      continue;
    }

    const keySet = new Set(preview.supportedKeys);
    const registryFields = filterRegistryForTargetKeys(
      listBrandFieldsForExtractionRegistry(),
      preview.supportedKeys
    );
    const extraction = await extractFromBrandSourceDocument(source, registryFields);
    const candidates = pickPreviewCandidates(extraction.tagged, preview.supportedKeys, limitFacts);

    const created = await createCurioCleanFacts(source, runId, candidates, CURIO_BRAND_ID);

    const existingNotes = String(source.notes || "").trim();
    const noteLine = `${CLEAN_REEXTRACT_BATCH_NOTE} run ${runId}`;
    const sourcePatch = {
      [MAP_PARTNER_SOURCE.status]: "Extracted",
      [MAP_PARTNER_SOURCE.extractionRunId]: runId,
    };
    if (!existingNotes.includes(CLEAN_REEXTRACT_BATCH_NOTE)) {
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

    if (!created.length) {
      applyResult.skipped.push({
        sourceId: source.id,
        type: "source",
        reasons: ["no_clean_candidates_written"],
      });
    }
  }

  return applyResult;
}
