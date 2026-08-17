/**
 * Run extraction for Partner Intelligence sources → Extracted Facts (Pending).
 * Operator batch mode: all sources → merge → one fact per registry field.
 */
import { randomUUID } from "crypto";
import {
  MAP_PARTNER_FACT,
  MAP_PARTNER_SOURCE,
  PARTNER_INTELLIGENCE_FLAGS,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  PILOT_OPERATORS,
  PILOT_BRANDS,
  listOperatorFieldsForExtraction,
  listBrandFieldsForExtractionRegistry,
} from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import { extractBrandFactsFromText,
  mergeBrandExtractionCandidates,
} from "./brand-extract-rules.js";
import { resolveBrandExtractionContext } from "./brand-extraction-context.js";
import { getRegistryField } from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import {
  getPartnerSourceById,
  patchPartnerSource,
  listPartnerSources,
} from "./airtable-source.js";
import { createPartnerFact } from "./airtable-facts.js";
import { loadSourceDocumentText } from "./extract-source-text.js";
import { classifySourceDocument } from "./classify-source-document.js";
import { enrichDocumentTextForExtraction } from "./parse-deck-pdf-text.js";
import { extractOperatorFactsWithLlm, isLlmExtractionEnabled } from "./llm-extract-operator-facts.js";
import {
  mergeExtractionCandidates,
  structuredRegionalFacts,
} from "./merge-extraction-candidates.js";
import { PARTNER_INTELLIGENCE_GAP_COPY } from "../../api/lib/partner-intelligence-field-map.js";
import {
  syncOperatorReferenceFolder,
  syncBrandReferenceFolder,
  isSourceExtractable,
  approveSourcesForBatchExtraction,
  approveBrandSourcesForBatchExtraction,
  getReferenceFolderForOperator,
  getReferenceFolderForBrand,
  listReadableReferenceFiles,
  listBrandReferenceFiles,
} from "./sync-reference-folder.js";

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function gapFactForField(field) {
  return {
    fieldKey: field.fieldKey,
    explorerSection: field.explorerSection,
    displayLabel: field.displayLabel,
    extractedValue: PARTNER_INTELLIGENCE_GAP_COPY,
    normalizedValue: PARTNER_INTELLIGENCE_GAP_COPY,
    evidenceText: PARTNER_INTELLIGENCE_GAP_COPY,
    extractionType: "Needs Confirmation",
    confidenceLevel: "Low",
    confidenceScore: 20,
    dataGap: "Yes",
    followUpQuestion: `Confirm ${field.displayLabel} with operator or additional source.`,
    _sourceRole: "any",
    _sourceId: null,
    _sourceTitle: "merge",
    _extractor: "merge",
  };
}

function ensureFullRegistryCoverage(merged, registryFields) {
  const keys = new Set(merged.map((m) => m.fieldKey));
  const out = [...merged];
  for (const field of registryFields) {
    if (!keys.has(field.fieldKey)) out.push(gapFactForField(field));
  }
  return out;
}

function fieldsForSourceRole(registryFields, sourceRole) {
  return registryFields.filter((f) => {
    const roles = f.sourceRoles || ["any"];
    return roles.includes(sourceRole) || roles.includes("any");
  });
}

async function createFactsFromMerged(source, runId, mergedFacts, linkIds = {}) {
  const today = new Date().toISOString().slice(0, 10);
  const created = [];
  const explorerType = linkIds.explorerType || "Operator Explorer";
  const profileType = source?.profileType || (explorerType === "Brand Explorer" ? "Brand" : "Operator");

  for (const c of mergedFacts) {
    const reg = getRegistryField(c.fieldKey, explorerType);
    const fields = {
      "Source Title": `${reg?.displayLabel || c.fieldKey} — ${runId}`,
      [MAP_PARTNER_FACT.profileType]: profileType,
      [MAP_PARTNER_FACT.operator]: linkIds.operatorId ? [linkIds.operatorId] : [],
      [MAP_PARTNER_FACT.brand]: linkIds.brandId ? [linkIds.brandId] : [],
      [MAP_PARTNER_FACT.sourceRecord]: c._sourceId ? [c._sourceId] : source ? [source.id] : [],
      [MAP_PARTNER_FACT.explorerType]: explorerType,
      [MAP_PARTNER_FACT.explorerSection]: c.explorerSection || reg?.explorerSection,
      [MAP_PARTNER_FACT.fieldName]: c.fieldKey,
      [MAP_PARTNER_FACT.extractedValue]: c.extractedValue,
      [MAP_PARTNER_FACT.normalizedValue]: c.normalizedValue || c.extractedValue,
      [MAP_PARTNER_FACT.evidenceText]: c.evidenceText,
      [MAP_PARTNER_FACT.pageSectionAnchor]: c.pageSectionAnchor || c._sourceTitle || "",
      [MAP_PARTNER_FACT.sourceType]: source?.sourceType || c._sourceType || "",
      [MAP_PARTNER_FACT.sourceQuality]: source?.sourceQuality || "Medium",
      [MAP_PARTNER_FACT.confidenceScore]: c.confidenceScore,
      [MAP_PARTNER_FACT.confidenceLevel]: c.confidenceLevel,
      [MAP_PARTNER_FACT.extractionType]: c.extractionType,
      [MAP_PARTNER_FACT.publicVisibility]: "Public",
      [MAP_PARTNER_FACT.humanReviewStatus]: "Pending",
      [MAP_PARTNER_FACT.dataGap]: c.dataGap || "No",
      [MAP_PARTNER_FACT.followUpQuestion]: c.followUpQuestion || "",
      [MAP_PARTNER_FACT.lastUpdated]: today,
      [MAP_PARTNER_FACT.extractionRunId]: runId,
    };
    if (c._sourceTitle && c._sourceId !== source?.id) {
      fields[MAP_PARTNER_FACT.reviewerNotes] = `Merged from: ${c._sourceTitle}`;
    }
    const fact = await createPartnerFact(fields);
    created.push(fact);
  }

  return created;
}

export async function extractFromOperatorSourceDocument(source, registryFields) {
  const classification = classifySourceDocument(source);
  const doc = await loadSourceDocumentText(source);
  const enriched = enrichDocumentTextForExtraction(doc.text, classification, source);
  const sourceText = enriched.text;
  const structured = enriched.structured;

  const applicableFields = fieldsForSourceRole(registryFields, classification.role);
  const sourceMeta = {
    sourceTitle: source.sourceTitle,
    sourceUrl: source.sourceUrl,
    sourceType: source.sourceType,
    localFilePath: source.localFilePath,
    metaDescription: doc.metaDescription,
    sourceRole: classification.role,
    pageSectionAnchor: source.localFilePath || source.sourceUrl || source.sourceTitle,
  };

  const { usedLlm, extractor, facts } = await extractOperatorFactsWithLlm(
    sourceText,
    applicableFields,
    sourceMeta
  );

  const tagged = facts.map((f) => ({
    ...f,
    _sourceRole: classification.role,
    _sourceId: source.id,
    _sourceTitle: source.sourceTitle,
    _extractor: extractor,
  }));

  if (structured) {
    tagged.push(...structuredRegionalFacts(structured, source.id, source.sourceTitle));
  }

  return {
    classification,
    documentKind: structured ? "regional_deck" : doc.kind,
    usedLlm,
    extractor,
    tagged,
  };
}

export async function extractFromBrandSourceDocument(source, registryFields) {
  const classification = classifySourceDocument(source);
  const doc = await loadSourceDocumentText(source);
  const sourceText = nz(doc.text);
  const applicableFields = fieldsForSourceRole(registryFields, classification.role);
  const brandContext = resolveBrandExtractionContext({ brandId: source.brandId });
  const facts = extractBrandFactsFromText(sourceText, applicableFields, {
    sourceTitle: source.sourceTitle,
    sourceRole: classification.role,
    localFilePath: source.localFilePath,
    brandContext,
  });

  const tagged = facts.map((f) => ({
    ...f,
    _sourceRole: classification.role,
    _sourceId: source.id,
    _sourceTitle: source.sourceTitle,
    _extractor: "brand_rules",
  }));

  return {
    classification,
    documentKind: doc.kind,
    usedLlm: false,
    extractor: "brand_rules",
    tagged,
  };
}

/**
 * @param {string} sourceId
 * @param {{ force?: boolean, runId?: string }} opts
 */
export async function runPartnerSourceExtraction(sourceId, opts = {}) {
  if (!PARTNER_INTELLIGENCE_FLAGS.extractionEnabled && !opts.force) {
    throw new Error("Extraction disabled (set PARTNER_INTELLIGENCE_EXTRACTION_ENABLED=1).");
  }

  const source = await getPartnerSourceById(sourceId);
  if (!source) throw new Error("Source not found.");
  if (nz(source.approvedForExtraction) !== "Yes" && !opts.force) {
    throw new Error("Source is not Approved for Extraction.");
  }
  if (!isSourceExtractable(source)) {
    throw new Error("Source has no URL or local file path to read.");
  }

  const runId = opts.runId || `pi-${randomUUID().slice(0, 8)}`;
  const registryFields = listOperatorFieldsForExtraction();
  const { tagged, documentKind, usedLlm, extractor } = await extractFromOperatorSourceDocument(
    source,
    registryFields
  );
  const mergedRaw = mergeExtractionCandidates(tagged, registryFields);
  const merged = ensureFullRegistryCoverage(mergedRaw, registryFields);
  const created = await createFactsFromMerged(source, runId, merged, {
    operatorId: source.operatorId,
    explorerType: "Operator Explorer",
  });

  await patchPartnerSource(sourceId, {
    [MAP_PARTNER_SOURCE.status]: "Extracted",
    [MAP_PARTNER_SOURCE.extractionRunId]: runId,
  });

  return {
    runId,
    sourceId,
    sourceTitle: source.sourceTitle,
    inputKind: source.localFilePath ? "local" : "url",
    documentKind,
    usedLlm,
    extractor,
    mergedFieldCount: merged.length,
    factsCreated: created.length,
    factsWithValues: created.filter((f) => f.dataGap !== "Yes").length,
    gapFacts: created.filter((f) => f.dataGap === "Yes").length,
    facts: created,
  };
}

/**
 * Sync reference folder + extract all sources → merge → one Pending row per registry field.
 */
export async function runPartnerOperatorExtraction(operatorId, opts = {}) {
  if (!PARTNER_INTELLIGENCE_FLAGS.extractionEnabled && !opts.force) {
    throw new Error("Extraction disabled (set PARTNER_INTELLIGENCE_EXTRACTION_ENABLED=1).");
  }

  const runId = opts.runId || `pi-${randomUUID().slice(0, 8)}`;
  const pilot = Object.values(PILOT_OPERATORS).find((p) => p.recordId === operatorId);
  const region = pilot?.region || "";
  const registryFields = listOperatorFieldsForExtraction();

  let folderSync = null;
  if (opts.syncFolder !== false) {
    folderSync = await syncOperatorReferenceFolder(operatorId, { region });
  }

  if (opts.force) {
    await approveSourcesForBatchExtraction(operatorId);
  }

  const { sources } = await listPartnerSources({ operatorId, limit: 100 });
  const extractable = sources.filter(isSourceExtractable);
  const referenceFolder = getReferenceFolderForOperator(operatorId);
  const folderScan = referenceFolder ? listReadableReferenceFiles(referenceFolder) : null;

  const allTagged = [];
  const sourceRuns = [];
  let usedLlmAny = false;
  let extractorMode = isLlmExtractionEnabled() ? "llm" : "rules";

  for (const source of extractable) {
    if (nz(source.approvedForExtraction) !== "Yes" && !opts.force) {
      sourceRuns.push({
        sourceId: source.id,
        sourceTitle: source.sourceTitle,
        skipped: true,
        reason: "not_approved_for_extraction",
      });
      continue;
    }

    try {
      const result = await extractFromOperatorSourceDocument(source, registryFields);
      usedLlmAny = usedLlmAny || result.usedLlm;
      allTagged.push(...result.tagged);
      sourceRuns.push({
        sourceId: source.id,
        sourceTitle: source.sourceTitle,
        role: result.classification.role,
        documentKind: result.documentKind,
        candidateCount: result.tagged.length,
        usedLlm: result.usedLlm,
        extractor: result.extractor,
      });
      await patchPartnerSource(source.id, {
        [MAP_PARTNER_SOURCE.status]: "Extracted",
        [MAP_PARTNER_SOURCE.extractionRunId]: runId,
      });
    } catch (err) {
      sourceRuns.push({
        sourceId: source.id,
        sourceTitle: source.sourceTitle,
        error: err.message || String(err),
      });
    }
  }

  const mergedRaw = mergeExtractionCandidates(allTagged, registryFields);
  const merged = ensureFullRegistryCoverage(mergedRaw, registryFields);
  const anchorSource = extractable[0] || null;
  const created = await createFactsFromMerged(anchorSource, runId, merged, {
    operatorId,
    explorerType: "Operator Explorer",
  });

  return {
    runId,
    operatorId,
    referenceFolder,
    referenceRoot: folderSync?.referenceRoot,
    folderSync,
    folderFileCount: folderScan?.files?.length ?? 0,
    registryFieldCount: registryFields.length,
    sourcesConsidered: extractable.length,
    sourceRuns,
    usedLlm: usedLlmAny,
    extractor: usedLlmAny ? "llm" : extractorMode,
    mergedFieldCount: merged.length,
    factsCreated: created.length,
    factsWithValues: created.filter((f) => f.dataGap !== "Yes").length,
    gapFacts: created.filter((f) => f.dataGap === "Yes").length,
    facts: created,
  };
}

/**
 * Sync brand reference folder + extract all sources → merge → facts (source-grounded rules).
 */
export async function runPartnerBrandExtraction(brandId, opts = {}) {
  if (!PARTNER_INTELLIGENCE_FLAGS.extractionEnabled && !opts.force) {
    throw new Error("Extraction disabled (set PARTNER_INTELLIGENCE_EXTRACTION_ENABLED=1).");
  }

  const runId = opts.runId || `pi-brand-${randomUUID().slice(0, 8)}`;
  const pilot = Object.values(PILOT_BRANDS).find((p) => p.recordId === brandId);
  const registryFields = listBrandFieldsForExtractionRegistry();

  let folderSync = null;
  if (opts.syncFolder !== false) {
    folderSync = await syncBrandReferenceFolder(brandId, {
      autoApproveExtraction: opts.force || opts.autoApprove,
    });
  }

  if (opts.force) {
    await approveBrandSourcesForBatchExtraction(brandId);
  }

  const { sources } = await listPartnerSources({ brandId, limit: 100 });
  const extractable = sources.filter(isSourceExtractable);
  const referenceFolder = getReferenceFolderForBrand(brandId);
  const folderScan = pilot
    ? listBrandReferenceFiles({
        referenceFolder: pilot.referenceFolder,
        includeSubpaths: pilot.includeSubpaths || [],
        brandNameMatch: pilot.brandNameMatch,
      })
    : null;

  const allTagged = [];
  const sourceRuns = [];

  for (const source of extractable) {
    if (nz(source.approvedForExtraction) !== "Yes" && !opts.force) {
      sourceRuns.push({
        sourceId: source.id,
        sourceTitle: source.sourceTitle,
        skipped: true,
        reason: "not_approved_for_extraction",
      });
      continue;
    }

    try {
      const result = await extractFromBrandSourceDocument(source, registryFields);
      allTagged.push(...result.tagged);
      sourceRuns.push({
        sourceId: source.id,
        sourceTitle: source.sourceTitle,
        localFilePath: source.localFilePath,
        role: result.classification.role,
        documentKind: result.documentKind,
        candidateCount: result.tagged.length,
        extractor: result.extractor,
      });
      await patchPartnerSource(source.id, {
        [MAP_PARTNER_SOURCE.status]: "Extracted",
        [MAP_PARTNER_SOURCE.extractionRunId]: runId,
      });
    } catch (err) {
      sourceRuns.push({
        sourceId: source.id,
        sourceTitle: source.sourceTitle,
        localFilePath: source.localFilePath,
        error: err.message || String(err),
      });
    }
  }

  const merged = mergeBrandExtractionCandidates(allTagged, registryFields);
  const anchorSource = extractable[0] || null;
  const created = await createFactsFromMerged(anchorSource, runId, merged, {
    brandId,
    explorerType: "Brand Explorer",
  });

  return {
    runId,
    brandId,
    brandName: pilot?.brandName || null,
    referenceFolder,
    referenceRoot: folderSync?.referenceRoot,
    folderSync,
    folderFileCount: folderScan?.files?.length ?? 0,
    registryFieldCount: registryFields.length,
    sourcesConsidered: extractable.length,
    sourceRuns,
    extractor: "brand_rules",
    mergedFieldCount: merged.length,
    factsCreated: created.length,
    factsWithValues: created.filter((f) => f.dataGap !== "Yes").length,
    gapFacts: created.filter((f) => f.dataGap === "Yes").length,
    facts: created,
    merged,
  };
}
