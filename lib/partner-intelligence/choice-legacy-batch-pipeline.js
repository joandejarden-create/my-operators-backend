/**
 * Choice legacy batch pipeline orchestrator v1.
 * Coordinates source package → URL capture → stewardship → extract → fact stewardship → governance → verification.
 * @see docs/data-intelligence/choice-legacy-batch-pipeline-v1.md
 */
import { PARTNER_INTELLIGENCE_LINKS } from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { listPartnerSources } from "./airtable-source.js";
import {
  inspectActiveBrand,
  UPGRADE_PROFILE_STATUS,
} from "./active-brand-governance-upgrade.js";
import {
  applyMiniBatchLocalPdfRegistrations,
  buildChoiceLegacyMiniBatchReport,
} from "./choice-legacy-brand-source-package-batch.js";
import { fetchBrandSources } from "./choice-legacy-brand-source-package.js";
import {
  DEFAULT_BATCH_NAME,
  getBatchBrandConfigs,
  getBatchDefinition,
  getBatchExtractBrandConfig,
  parseBatchNameFromArgv,
  buildBatchPipelineCommands,
  isRhgBlockedUrl,
  isRhgContaminatedLocalPath,
} from "./choice-legacy-batch-config.js";
import {
  applyChoiceLegacyBatchExtract,
  buildChoiceLegacyBatchExtractReport,
  getBatchExtractBrandConfigs,
} from "./choice-legacy-batch-extract.js";
import {
  applyChoiceLegacyBatchFactStewardship,
  buildChoiceLegacyBatchFactStewardshipReport,
  isBatchExtractionFact,
} from "./choice-legacy-batch-fact-stewardship.js";
import {
  applyChoiceLegacyBatchGovernancePublish,
  buildChoiceLegacyBatchGovernancePublishReport,
} from "./choice-legacy-batch-governance-publish.js";
import {
  applyChoiceLegacyBatchStewardship,
  buildChoiceLegacyBatchStewardshipReport,
  isSourceFullyApproved,
} from "./choice-legacy-batch-source-stewardship.js";
import {
  applyChoiceLegacyBatchUrlCapture,
  buildChoiceLegacyBatchUrlCaptureReport,
} from "./choice-legacy-batch-url-capture.js";
import {
  EXPECTED_DISPLAY_LABEL,
  EXPECTED_SOURCE_BASIS,
} from "./choice-legacy-batch-governance-publish.js";
import { resolveBatchExtractConfig } from "./brand-source-auto-resolver.js";

export const PIPELINE_VERSION = "1";
export const REPORT_JSON_NAME = "choice-legacy-batch-pipeline.json";
export const REPORT_MD_NAME = "choice-legacy-batch-pipeline.md";
export const APPLY_FLAG = "--approve-choice-legacy-batch-pipeline";

export const PIPELINE_STAGE = {
  BLOCKED: "Blocked",
  SOURCE_PACKAGE: "Source Package Needed",
  URL_CAPTURE: "URL Capture Needed",
  SOURCE_STEWARDSHIP: "Source Stewardship Needed",
  EXTRACTION: "Extraction Needed",
  FACT_STEWARDSHIP: "Fact Stewardship Needed",
  GOVERNANCE_PUBLISH: "Governance Publish Needed",
  VERIFICATION: "Verification Needed",
  PLATFORM_READY: "Platform Ready",
};

export const STAGE_ORDER = [
  PIPELINE_STAGE.SOURCE_PACKAGE,
  PIPELINE_STAGE.URL_CAPTURE,
  PIPELINE_STAGE.SOURCE_STEWARDSHIP,
  PIPELINE_STAGE.EXTRACTION,
  PIPELINE_STAGE.FACT_STEWARDSHIP,
  PIPELINE_STAGE.GOVERNANCE_PUBLISH,
  PIPELINE_STAGE.VERIFICATION,
  PIPELINE_STAGE.PLATFORM_READY,
];

export { parseBatchNameFromArgv };

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function buildPipelineApplyCommand(batchName = DEFAULT_BATCH_NAME) {
  return `npm run choice-legacy-batch-pipeline -- --batch ${batchName} --apply ${APPLY_FLAG}`;
}

async function fetchAllBrandFacts(recordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: recordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

export function resolveExtractConfigFromStewardshipBrand(stewardshipBrand, batchName) {
  const base = getBatchExtractBrandConfig(batchName, stewardshipBrand.key) || {
    key: stewardshipBrand.key,
    brandName: stewardshipBrand.brandName,
    recordId: stewardshipBrand.recordId,
    disallowRhgGlobal: getBatchDefinition(batchName).disallowRhgGlobal,
  };
  // Reusable auto-resolver: prefer a non-empty manifest allowlist (shipped
  // batches stay stable) and otherwise generate the allowlist from approved,
  // linked, company-controlled Source Library rows — no manual source IDs.
  return resolveBatchExtractConfig({
    base,
    sourceRows: stewardshipBrand.sourceRows || [],
  });
}

export function collectBrandBlockers({
  sourcePackageRow,
  liveSources = [],
  batchName,
}) {
  const blockers = [];
  if (sourcePackageRow?.splitOutRecommended) {
    blockers.push(`split_out:${sourcePackageRow.splitOutReason || "review_required"}`);
  }
  if (sourcePackageRow?.duplicateCheck?.isDuplicate && sourcePackageRow?.duplicateCheck?.conflict) {
    blockers.push("duplicate_source_conflict");
  }
  for (const path of sourcePackageRow?.rhgLocalWarnings || []) {
    blockers.push(`rhg_local:${path}`);
  }
  for (const source of liveSources) {
    if (isRhgBlockedUrl(source.sourceUrl)) blockers.push(`rhg_url:${source.id}`);
    if (isRhgContaminatedLocalPath(source.localFilePath)) {
      blockers.push(`rhg_local_path:${source.localFilePath}`);
    }
  }
  if (getBatchDefinition(batchName).disallowRhgGlobal && blockers.some((b) => b.startsWith("rhg_"))) {
    return [...new Set(blockers)];
  }
  return [...new Set(blockers.filter((b) => !b.startsWith("rhg_") || getBatchDefinition(batchName).disallowRhgGlobal))];
}

export function detectBrandPipelineStage(ctx) {
  const {
    sourcePackageRow,
    urlCaptureRows = [],
    stewardshipBrand,
    extractBrand,
    factBrand,
    governanceBrand,
    verificationRow,
    liveSources = [],
    liveFacts = [],
    batchName,
    extractConfig,
  } = ctx;

  const blockers = collectBrandBlockers({
    sourcePackageRow,
    liveSources,
    batchName,
  });

  if (blockers.includes("duplicate_source_conflict")) {
    return { stage: PIPELINE_STAGE.BLOCKED, blockers, applyRecommended: false };
  }

  if (
    verificationRow?.profileStatus === UPGRADE_PROFILE_STATUS.PLATFORM_READY ||
    verificationRow?.platformReady
  ) {
    return {
      stage: PIPELINE_STAGE.PLATFORM_READY,
      blockers: blockers.filter((b) => b.startsWith("split_out")),
      applyRecommended: false,
    };
  }

  const batchFacts = liveFacts.filter((f) => isBatchExtractionFact(f, batchName));
  const pendingFacts = batchFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  const approvedFacts = batchFacts.filter((f) => nz(f.humanReviewStatus) === "Approved");
  const approvedSources = liveSources.filter((s) => isSourceFullyApproved(s));
  const extractionReadySources = liveSources.filter(
    (s) => isSourceFullyApproved(s) && nz(s.approvedForExtraction) === "Yes"
  );

  if (governanceBrand?.eligibleForBatchApply) {
    return { stage: PIPELINE_STAGE.GOVERNANCE_PUBLISH, blockers, applyRecommended: true };
  }

  if (pendingFacts.length > 0 && (factBrand?.recommendedApproveCount || 0) > 0) {
    return { stage: PIPELINE_STAGE.FACT_STEWARDSHIP, blockers, applyRecommended: true };
  }

  if (
    extractionReadySources.length > 0 &&
    pendingFacts.length === 0 &&
    approvedFacts.length === 0 &&
    (extractBrand?.wouldWrite?.factsWouldCreateCount || 0) > 0
  ) {
    return { stage: PIPELINE_STAGE.EXTRACTION, blockers, applyRecommended: true };
  }

  if (
    extractionReadySources.length > 0 &&
    batchFacts.length === 0 &&
    (extractBrand?.wouldWrite?.factsWouldCreateCount || 0) > 0
  ) {
    return { stage: PIPELINE_STAGE.EXTRACTION, blockers, applyRecommended: true };
  }

  if ((stewardshipBrand?.eligibleCount || 0) > 0) {
    return { stage: PIPELINE_STAGE.SOURCE_STEWARDSHIP, blockers, applyRecommended: true };
  }

  const urlReady = urlCaptureRows.filter((u) => u.status === "ready_to_capture").length;
  if (urlReady > 0 || sourcePackageRow?.readyForUrlCapture) {
    const hasUncaptured = urlCaptureRows.some(
      (u) => u.status === "ready_to_capture" && !u.sourceLibraryRowId
    );
    if (hasUncaptured || (liveSources.length > 0 && urlReady > 0 && approvedSources.length < liveSources.length + urlReady)) {
      return { stage: PIPELINE_STAGE.URL_CAPTURE, blockers, applyRecommended: urlReady > 0 };
    }
  }

  const pdfStatus = sourcePackageRow?.pdfRegistration?.registrationStatus;
  if (pdfStatus === "ready_to_register_local" || liveSources.length === 0) {
    return {
      stage: PIPELINE_STAGE.SOURCE_PACKAGE,
      blockers,
      applyRecommended: pdfStatus === "ready_to_register_local",
    };
  }

  if (approvedFacts.length >= 3 && !governanceBrand?.eligibleForBatchApply) {
    return { stage: PIPELINE_STAGE.VERIFICATION, blockers, applyRecommended: false };
  }

  if (blockers.length) {
    return { stage: PIPELINE_STAGE.BLOCKED, blockers, applyRecommended: false };
  }

  return { stage: PIPELINE_STAGE.VERIFICATION, blockers, applyRecommended: false };
}

function countApprovedExplorerSources(sources) {
  return (sources || []).filter((s) => nz(s.approvedForExplorerUse) === "Yes").length;
}

function buildBrandPipelineRow(ctx) {
  const {
    brandConfig,
    sourcePackageRow,
    stewardshipBrand,
    extractBrand,
    factBrand,
    governanceBrand,
    verificationRow,
    liveSources,
    liveFacts,
    stageInfo,
    actions = [],
  } = ctx;

  const batchFacts = (liveFacts || []).filter((f) =>
    isBatchExtractionFact(f, ctx.batchName)
  );
  const pendingFacts = batchFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  const approvedFacts = batchFacts.filter((f) => nz(f.humanReviewStatus) === "Approved");
  const approvedSources = countApprovedExplorerSources(liveSources);

  return {
    brandKey: brandConfig.key,
    brandName: brandConfig.brandName,
    recordId: brandConfig.recordId,
    explorerStatus: sourcePackageRow?.explorerActive ? "Active" : "Not Active",
    profileStatus: sourcePackageRow?.profileStatus || verificationRow?.profileStatus || null,
    sourceCount: liveSources?.length || 0,
    approvedSourceCount: approvedSources.length,
    pendingFactCount: pendingFacts.length,
    approvedFactCount: approvedFacts.length,
    heldFactCount: factBrand?.holdCount ?? pendingFacts.length,
    governanceStatus:
      verificationRow?.governance?.liveValidationStatus ||
      governanceBrand?.currentGovernance?.validationStatus ||
      null,
    expectedChip: EXPECTED_DISPLAY_LABEL,
    sourceBasis: EXPECTED_SOURCE_BASIS,
    currentStage: stageInfo.stage,
    blockers: stageInfo.blockers || [],
    actionsAppliedOrProposed: actions,
    rebuildNeeded: false,
    platformReady: stageInfo.stage === PIPELINE_STAGE.PLATFORM_READY,
    applyRecommended: stageInfo.applyRecommended,
    splitOutRecommended: Boolean(sourcePackageRow?.splitOutRecommended),
    splitOutReason: sourcePackageRow?.splitOutReason || null,
  };
}

export async function buildVerificationRows({
  batchName,
  brandConfigs,
  catalogRows,
  readinessById = new Map(),
}) {
  const rows = [];
  for (const brandConfig of brandConfigs) {
    const batchEntry = {
      key: brandConfig.key,
      displayName: brandConfig.brandName,
      aliases: [brandConfig.brandName],
    };
    const resolution = {
      resolved: true,
      recordId: brandConfig.recordId,
      airtableName: brandConfig.brandName,
      fields: catalogRows.get(brandConfig.recordId)?.fields || {},
      matchMethod: "batch_config",
      nameMismatch: false,
    };
    const sources = await fetchBrandSources(brandConfig.recordId);
    const facts = await fetchAllBrandFacts(brandConfig.recordId);
    const readinessReport = readinessById.get(brandConfig.recordId) || null;
    rows.push(
      inspectActiveBrand({
        batchEntry,
        resolution,
        sources,
        facts,
        readinessReport,
      })
    );
  }
  return rows;
}

export async function runChoiceLegacyBatchPipeline({
  batchName = DEFAULT_BATCH_NAME,
  mode = "dry-run",
  brandFilter = null,
  governanceReport = null,
  readinessReport = null,
  probeLive = true,
  probeUrls = true,
  targetProfilesById = null,
  applyPatch = null,
  catalogRows = null,
} = {}) {
  const batch = getBatchDefinition(batchName);
  const brandConfigs = getBatchBrandConfigs(batchName, brandFilter);
  const isApply = mode === "apply";
  const stageResults = {};
  const applyLog = [];

  let sourcePackageReport = await buildChoiceLegacyMiniBatchReport({
    governanceReport,
    probeLive,
    brandFilter,
    batchName,
  });

  if (isApply) {
    const needsPdf = sourcePackageReport.brands.filter(
      (b) => b.pdfRegistration?.registrationStatus === "ready_to_register_local"
    );
    if (needsPdf.length) {
      const result = await applyMiniBatchLocalPdfRegistrations(sourcePackageReport, { brandFilter });
      stageResults.source_package = { applied: result.applied.length, skipped: result.skipped, errors: result.errors };
      applyLog.push(...result.applied.map((a) => ({ stage: "source_package", ...a })));
      if (result.errors?.length) {
        return finalizePipelineReport({
          batchName,
          mode,
          brandConfigs,
          sourcePackageReport,
          stageResults,
          applyLog,
          halted: true,
          haltReason: "source_package_errors",
          errors: result.errors,
        });
      }
      sourcePackageReport = await buildChoiceLegacyMiniBatchReport({
        governanceReport,
        probeLive: false,
        brandFilter,
        batchName,
      });
    } else {
      stageResults.source_package = { skipped: true, reason: "already_complete" };
    }
  }

  let urlCaptureReport = await buildChoiceLegacyBatchUrlCaptureReport({
    brandFilter,
    probeUrls,
    batchName,
  });

  if (isApply) {
    const ready = urlCaptureReport.urls.filter((u) => u.status === "ready_to_capture");
    if (ready.length) {
      const result = await applyChoiceLegacyBatchUrlCapture(urlCaptureReport, { brandFilter, batchName });
      stageResults.url_capture = {
        captured: result.captured.length,
        skippedDuplicates: result.skippedDuplicates.length,
        failed: result.failed.length,
      };
      applyLog.push(...result.captured.map((c) => ({ stage: "url_capture", ...c })));
      if (result.failed?.length) {
        stageResults.url_capture.blockedBrands = [...new Set(result.failed.map((f) => f.brandKey))];
      }
      urlCaptureReport = await buildChoiceLegacyBatchUrlCaptureReport({
        brandFilter,
        probeUrls: false,
        batchName,
      });
    } else {
      stageResults.url_capture = { skipped: true, reason: "already_complete" };
    }
  }

  let stewardshipReport = await buildChoiceLegacyBatchStewardshipReport({ brandFilter, batchName });

  if (isApply) {
    if (stewardshipReport.summary.sourcesEligibleForApproval > 0) {
      const result = await applyChoiceLegacyBatchStewardship(stewardshipReport, { brandFilter });
      stageResults.source_stewardship = {
        applied: result.applied.length,
        skipped: result.skipped.length,
        errors: result.errors.length,
      };
      applyLog.push(...result.applied.map((a) => ({ stage: "source_stewardship", ...a })));
      if (result.errors?.length) {
        return finalizePipelineReport({
          batchName,
          mode,
          brandConfigs,
          sourcePackageReport,
          urlCaptureReport,
          stewardshipReport,
          stageResults,
          applyLog,
          halted: true,
          haltReason: "source_stewardship_errors",
          errors: result.errors,
        });
      }
      stewardshipReport = await buildChoiceLegacyBatchStewardshipReport({ brandFilter, batchName });
    } else {
      stageResults.source_stewardship = { skipped: true, reason: "already_complete" };
    }
  }

  const extractConfigs = stewardshipReport.brands.map((b) =>
    resolveExtractConfigFromStewardshipBrand(b, batchName)
  );
  const existingFactsByBrand = new Map();
  for (const cfg of brandConfigs) {
    existingFactsByBrand.set(cfg.recordId, await fetchAllBrandFacts(cfg.recordId));
  }

  let extractReport = await buildChoiceLegacyBatchExtractReport({
    brandFilter,
    batchName,
    existingFactsByBrand,
    brandConfigsOverride: extractConfigs,
  });

  if (isApply) {
    const readyBrands = extractReport.brands.filter((b) => b.extractionQuality?.applyRecommended);
    if (readyBrands.length && extractReport.summary.totalProposedFacts > 0) {
      const result = await applyChoiceLegacyBatchExtract({
        brandReports: extractReport.brands,
        targetKeys: extractReport.targetFactKeys,
        batchName,
      });
      stageResults.extraction = {
        runId: result.runId,
        factsCreated: result.factsCreated.length,
      };
      applyLog.push({ stage: "extraction", runId: result.runId, facts: result.factsCreated.length });
      for (const cfg of brandConfigs) {
        existingFactsByBrand.set(cfg.recordId, await fetchAllBrandFacts(cfg.recordId));
      }
      extractReport = await buildChoiceLegacyBatchExtractReport({
        brandFilter,
        batchName,
        existingFactsByBrand,
        brandConfigsOverride: extractConfigs,
      });
    } else {
      stageResults.extraction = { skipped: true, reason: "nothing_to_extract" };
    }
  }

  let factStewardshipReport = await buildChoiceLegacyBatchFactStewardshipReport({
    brandFilter,
    batchName,
    existingFactsByBrand,
    brandConfigsOverride: extractConfigs,
  });

  if (isApply) {
    if (factStewardshipReport.summary.factsRecommendedForBatchApproval > 0) {
      const result = await applyChoiceLegacyBatchFactStewardship(factStewardshipReport, { brandFilter });
      stageResults.fact_stewardship = {
        applied: result.applied.length,
        skipped: result.skipped.length,
        errors: result.errors.length,
      };
      applyLog.push(...result.applied.map((a) => ({ stage: "fact_stewardship", ...a })));
      for (const cfg of brandConfigs) {
        existingFactsByBrand.set(cfg.recordId, await fetchAllBrandFacts(cfg.recordId));
      }
      factStewardshipReport = await buildChoiceLegacyBatchFactStewardshipReport({
        brandFilter,
        batchName,
        existingFactsByBrand,
        brandConfigsOverride: extractConfigs,
      });
    } else {
      stageResults.fact_stewardship = { skipped: true, reason: "already_complete" };
    }
  }

  let governancePublishReport = await buildChoiceLegacyBatchGovernancePublishReport({
    brandFilter,
    batchName,
    targetProfilesById,
    mode: isApply ? "apply" : "dry-run",
    brandConfigsOverride: extractConfigs,
  });

  if (isApply && applyPatch) {
    const eligible = governancePublishReport.brands.filter((b) => b.eligibleForBatchApply);
    if (eligible.length) {
      const result = await applyChoiceLegacyBatchGovernancePublish(governancePublishReport, {
        brandFilter,
        applyPatch,
      });
      stageResults.governance_publish = {
        applied: result.applied.length,
        skipped: result.skipped.length,
        errors: result.errors.length,
      };
      applyLog.push(...result.applied.map((a) => ({ stage: "governance_publish", ...a })));
      governancePublishReport = await buildChoiceLegacyBatchGovernancePublishReport({
        brandFilter,
        batchName,
        targetProfilesById,
        mode: "apply",
        brandConfigsOverride: extractConfigs,
      });
    } else {
      stageResults.governance_publish = { skipped: true, reason: "not_eligible" };
    }
  }

  const catalogMap =
    catalogRows ||
    new Map(
      brandConfigs.map((b) => [
        b.recordId,
        { id: b.recordId, name: b.brandName, fields: targetProfilesById?.get(b.recordId)?.fields || {} },
      ])
    );
  const readinessById = new Map(
    (readinessReport?.packages || [])
      .filter((p) => p.entityType === "brand" && p.recordId)
      .map((p) => [p.recordId, p])
  );

  const verificationRows = await buildVerificationRows({
    batchName,
    brandConfigs,
    catalogRows: catalogMap,
    readinessById,
  });

  stageResults.verification = {
    mode: "dry_run",
    platformReady: verificationRows.filter((r) => r.profileStatus === UPGRADE_PROFILE_STATUS.PLATFORM_READY)
      .length,
  };

  return finalizePipelineReport({
    batchName,
    mode,
    brandConfigs,
    sourcePackageReport,
    urlCaptureReport,
    stewardshipReport,
    extractReport,
    factStewardshipReport,
    governancePublishReport,
    verificationRows,
    stageResults,
    applyLog,
    existingFactsByBrand,
  });
}

async function finalizePipelineReport(ctx) {
  const {
    batchName,
    mode,
    brandConfigs,
    sourcePackageReport,
    urlCaptureReport,
    stewardshipReport,
    extractReport,
    factStewardshipReport,
    governancePublishReport,
    verificationRows = [],
    stageResults = {},
    applyLog = [],
    halted = false,
    haltReason = null,
    errors = [],
    existingFactsByBrand = new Map(),
  } = ctx;

  const batch = getBatchDefinition(batchName);
  const sourceByKey = new Map((sourcePackageReport?.brands || []).map((b) => [b.key, b]));
  const stewardshipByKey = new Map((stewardshipReport?.brands || []).map((b) => [b.key, b]));
  const extractByKey = new Map((extractReport?.brands || []).map((b) => [b.brandConfig?.key || b.brandKey, b]));
  const factByKey = new Map((factStewardshipReport?.brands || []).map((b) => [b.brandKey, b]));
  const governanceByKey = new Map((governancePublishReport?.brands || []).map((b) => [b.brandKey, b]));
  const verificationByKey = new Map((verificationRows || []).map((r) => [r.key, r]));
  const urlByKey = new Map();
  for (const row of urlCaptureReport?.urls || []) {
    if (!urlByKey.has(row.brandKey)) urlByKey.set(row.brandKey, []);
    urlByKey.get(row.brandKey).push(row);
  }

  const brands = [];
  for (const brandConfig of brandConfigs) {
    const liveSources = await fetchBrandSources(brandConfig.recordId);
    const liveFacts = existingFactsByBrand.get(brandConfig.recordId) || (await fetchAllBrandFacts(brandConfig.recordId));
    const sourcePackageRow = sourceByKey.get(brandConfig.key);
    const stageInfo = detectBrandPipelineStage({
      sourcePackageRow,
      urlCaptureRows: urlByKey.get(brandConfig.key) || [],
      stewardshipBrand: stewardshipByKey.get(brandConfig.key),
      extractBrand: extractByKey.get(brandConfig.key),
      factBrand: factByKey.get(brandConfig.key),
      governanceBrand: governanceByKey.get(brandConfig.key),
      verificationRow: verificationByKey.get(brandConfig.key),
      liveSources,
      liveFacts,
      batchName,
      extractConfig: getBatchExtractBrandConfig(batchName, brandConfig.key),
    });

    brands.push(
      buildBrandPipelineRow({
        brandConfig,
        sourcePackageRow,
        stewardshipBrand: stewardshipByKey.get(brandConfig.key),
        extractBrand: extractByKey.get(brandConfig.key),
        factBrand: factByKey.get(brandConfig.key),
        governanceBrand: governanceByKey.get(brandConfig.key),
        verificationRow: verificationByKey.get(brandConfig.key),
        liveSources,
        liveFacts,
        stageInfo,
        batchName,
        actions: applyLog.filter((a) => a.brand === brandConfig.brandName || a.brandKey === brandConfig.key),
      })
    );
  }

  const platformReady = brands.filter((b) => b.platformReady).length;
  const blocked = brands.filter((b) => b.currentStage === PIPELINE_STAGE.BLOCKED).length;
  const changed = applyLog.length;
  const applyRecommended = brands.some((b) => b.applyRecommended && b.currentStage !== PIPELINE_STAGE.PLATFORM_READY);

  const pipelineSummary = {
    sourcePackage: summarizeStage(sourcePackageReport, "source_package", stageResults.source_package),
    urlCapture: summarizeStage(urlCaptureReport, "url_capture", stageResults.url_capture),
    sourceStewardship: summarizeStage(stewardshipReport, "source_stewardship", stageResults.source_stewardship),
    extraction: summarizeStage(extractReport, "extraction", stageResults.extraction),
    factStewardship: summarizeStage(factStewardshipReport, "fact_stewardship", stageResults.fact_stewardship),
    governancePublish: summarizeStage(governancePublishReport, "governance_publish", stageResults.governance_publish),
    verification: stageResults.verification || { mode: "dry_run" },
  };

  const nextStage = brands
    .filter((b) => !b.platformReady && b.currentStage !== PIPELINE_STAGE.BLOCKED)
    .map((b) => b.currentStage)[0];

  return {
    pipelineVersion: PIPELINE_VERSION,
    batchName,
    batchDisplayName: batch.displayName,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified: mode === "apply" && applyLog.length > 0,
    halted,
    haltReason,
    errors,
    executiveSummary: {
      batchName,
      brandsIncluded: brands.map((b) => b.brandName),
      brandsPlatformReady: platformReady,
      brandsChanged: changed,
      brandsBlocked: blocked,
      currentStageByBrand: Object.fromEntries(brands.map((b) => [b.brandName, b.currentStage])),
      nextRecommendedAction: halted
        ? `Resolve ${haltReason} before continuing`
        : nextStage || "No action — batch Platform Ready",
      applyRecommended: mode === "dry-run" && applyRecommended,
      exactApplyCommand: buildPipelineApplyCommand(batchName),
    },
    brands,
    pipelineSummary,
    stageResults,
    applyLog,
    batchApplyCommands: buildBatchPipelineCommands(batchName),
    doesNotDo: [
      "Rebuild Brand Explorer content or overwrite Brand Setup content fields",
      "Auto-approve unsafe or held facts",
      "Set Company Validated or Company Validation Date",
      "Weaken RHG/global safeguards for Choice/Americas brands",
      "Publish Source-Informed posture for official Choice company materials",
      "Duplicate existing sources or facts",
      "Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema",
    ],
  };
}

function summarizeStage(report, key, applyResult) {
  if (applyResult?.skipped) {
    return { status: "skipped", reason: applyResult.reason, applyResult };
  }
  if (applyResult) {
    return { status: "applied", applyResult };
  }
  if (!report) return { status: "not_run" };
  return { status: "planned", summary: report.summary || null };
}

export function buildChoiceLegacyBatchPipelineMarkdown(report) {
  const es = report.executiveSummary;
  const lines = [
    `# Choice Legacy Batch Pipeline — ${report.batchDisplayName}`,
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Batch: **${report.batchName}**`,
    "",
    "## Executive summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Brands | ${es.brandsIncluded.length} |`,
    `| Platform Ready | ${es.brandsPlatformReady} |`,
    `| Blocked | ${es.brandsBlocked} |`,
    `| Apply recommended | ${es.applyRecommended ? "Yes" : "No"} |`,
    `| Next action | ${es.nextRecommendedAction} |`,
    "",
    "### Current stage by brand",
    "",
    "| Brand | Stage |",
    "|-------|-------|",
    ...Object.entries(es.currentStageByBrand).map(([name, stage]) => `| ${name} | ${stage} |`),
    "",
    "### Apply command",
    "",
    "```bash",
    es.exactApplyCommand,
    "```",
    "",
    "## Pipeline stages",
    "",
    "| Stage | Status |",
    "|-------|--------|",
    ...Object.entries(report.pipelineSummary).map(([k, v]) => `| ${k} | ${v.status} |`),
    "",
    "## Brands",
    "",
  ];

  for (const b of report.brands) {
    lines.push(
      `### ${b.brandName}`,
      "",
      `- Record: \`${b.recordId}\``,
      `- Explorer: **${b.explorerStatus}** · Profile: **${b.profileStatus || "—"}**`,
      `- Stage: **${b.currentStage}** · Platform Ready: **${b.platformReady ? "yes" : "no"}**`,
      `- Sources: ${b.approvedSourceCount}/${b.sourceCount} approved`,
      `- Facts: ${b.approvedFactCount} approved · ${b.pendingFactCount} pending · ${b.heldFactCount} held`,
      `- Governance: **${b.governanceStatus || "—"}** · Chip: **${b.expectedChip}** · Basis: **${b.sourceBasis}**`,
      `- Rebuild needed: **No**`,
      `- Blockers: ${b.blockers.length ? b.blockers.join("; ") : "none"}`,
      ""
    );
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo || []) lines.push(`- ${item}`);

  return lines.join("\n");
}
