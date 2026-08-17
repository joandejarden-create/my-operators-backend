/**
 * Choice legacy mini-batch 1 — batch fact stewardship (dry-run default).
 * Reviews Pending facts across Comfort, Everhome, Quality together.
 * @see docs/data-intelligence/choice-legacy-batch-fact-stewardship-v1.md
 */
import {
  MAP_PARTNER_FACT,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { getRegistryField } from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import { listPartnerFacts, patchPartnerFact } from "./airtable-facts.js";
import { getPartnerSourceById } from "./airtable-source.js";
import {
  DEFAULT_BATCH_NAME,
  getBatchDefinition,
  getBatchExtractBrandConfigs,
  MINI_BATCH_EXTRACT_BRANDS,
} from "./choice-legacy-batch-config.js";
import { assessPackageReadiness } from "./profile-governance-publish-readiness.js";
import {
  buildPackageFromRecords,
  buildSafeFactPatch,
  isLinkedToTarget,
  simulateFactsApproved,
} from "./stewardship-package.js";

export const STEWARDSHIP_VERSION = "1";
export const REPORT_JSON_NAME = "choice-legacy-batch-fact-stewardship.json";
export const REPORT_MD_NAME = "choice-legacy-batch-fact-stewardship.md";

export const BATCH_EXTRACT_RUN_PREFIX = "pi-choice-legacy-batch-";

export const PRIORITY_APPROVE_KEYS = new Set([
  "be.identity.brandName",
  "be.identity.parentCompany",
  "be.positioning.summary",
  "be.positioning.guestPromise",
  "be.overview.developmentModel",
  "be.footprint.geoIntro",
  "be.loyalty.programName",
]);

export const CAREFUL_REVIEW_KEYS = new Set([
  "be.overview.typicalUseCase",
  "be.overview.whyValue",
]);

export const UNSUPPORTED_FACT_KEYS = new Set([
  "be.positioning.segment",
  "be.positioning.chainScale",
]);

const BLOCKED_URL_PATTERNS = [
  /radissonhotels\.(net|com)/i,
  /radissonhotelgroup/i,
  /\brhg\b/i,
  /choicehotelsdevelopment\.com/i,
];

const LAYOUT_NOISE_RE =
  /invest inComfort|inComfort At|®\s*There'?s never been|At the heart of th/i;
const COMPANY_VALIDATED_RE =
  /\b(company validated|officially validated|certified by choice)\b/i;
const OVERCLAIM_RE =
  /\b(#1|largest in the world|market leader globally|unparalleled)\b/i;

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeValue(v) {
  return nz(v).toLowerCase().replace(/\s+/g, " ").trim();
}

function isBlockedUrl(url) {
  if (!url) return false;
  return BLOCKED_URL_PATTERNS.some((re) => re.test(url));
}

export function getBrandExtractConfig(brandKey, batchName = DEFAULT_BATCH_NAME) {
  return getBatchExtractBrandConfigs(batchName).find((b) => b.key === brandKey) || null;
}

export function isBatchExtractionFact(fact, batchName = null) {
  const runId = nz(fact.extractionRunId);
  const notes = nz(fact.reviewerNotes);
  if (batchName) {
    const batch = getBatchDefinition(batchName);
    return runId.startsWith(batch.extractionRunPrefix) || notes.includes(batch.extractionNote);
  }
  return (
    runId.startsWith("pi-choice-legacy-batch-") ||
    notes.includes("Choice legacy mini-batch 1") ||
    notes.includes("Choice legacy mini-batch 2")
  );
}

export function isRegistrySupportedKey(fieldKey) {
  if (UNSUPPORTED_FACT_KEYS.has(fieldKey)) return false;
  return Boolean(getRegistryField(fieldKey, "Brand Explorer"));
}

function isFragmentaryValue(fieldKey, value) {
  const v = nz(value);
  if (!v) return true;

  if (fieldKey === "be.footprint.geoIntro") {
    if (v.length < 25) return true;
    if (/^(North America|United States alone|Americas regions by Choice Hotels)\.?$/i.test(v)) {
      return true;
    }
  }

  if (fieldKey === "be.positioning.guestPromise" && v.length < 50) {
    return true;
  }

  if (fieldKey === "be.overview.typicalUseCase" && (v.length < 30 || /^travelers\.?$/i.test(v))) {
    return true;
  }

  return false;
}

function isWeakEvidence(evidence, value, fieldKey) {
  const e = nz(evidence);
  const v = nz(value);
  if (!e) return true;

  if (
    [
      "be.loyalty.programName",
      "be.identity.brandName",
      "be.identity.parentCompany",
    ].includes(fieldKey) &&
    v.length >= 3 &&
    e.toLowerCase().includes(v.toLowerCase().slice(0, Math.min(14, v.length)))
  ) {
    return false;
  }

  if (fieldKey === "be.overview.developmentModel" && /per Choice brand materials/i.test(v)) {
    return e.length < 8;
  }

  if (e.length < 12) return true;
  if (e === v && v.length < 40) return true;
  return false;
}

function isNoisyPositioningSummary(value) {
  return LAYOUT_NOISE_RE.test(nz(value));
}

function isAiInterpretedFact(fact) {
  const notes = nz(fact.reviewerNotes);
  return /AI-interpreted/i.test(notes);
}

/**
 * @returns {{ recommendation: 'approve'|'hold'|'reject', reasons: string[], riskLevel: 'low'|'medium'|'high' }}
 */
export function assessPendingFact(fact, source, brandConfig, context = {}) {
  const reasons = [];
  let riskLevel = "low";
  const fieldKey = nz(fact.fieldName);
  const value = nz(fact.extractedValue);
  const evidence = nz(fact.evidenceText);

  if (nz(fact.humanReviewStatus) !== "Pending") {
    return {
      recommendation: "hold",
      reasons: [`not_pending:${fact.humanReviewStatus}`],
      riskLevel: "low",
    };
  }

  if (!isBatchExtractionFact(fact)) {
    return { recommendation: "hold", reasons: ["outside_batch_extraction_run"], riskLevel: "medium" };
  }

  if (!isLinkedToTarget("brand", brandConfig.recordId, fact)) {
    return { recommendation: "reject", reasons: ["wrong_brand_link"], riskLevel: "high" };
  }

  if (!isRegistrySupportedKey(fieldKey)) {
    return { recommendation: "reject", reasons: ["unsupported_registry_key"], riskLevel: "high" };
  }

  const allowlist = new Set(brandConfig.allowlistedSourceIds);
  if (!fact.sourceRecordId || !allowlist.has(fact.sourceRecordId)) {
    return { recommendation: "reject", reasons: ["source_not_in_brand_allowlist"], riskLevel: "high" };
  }

  if (!source) {
    return { recommendation: "hold", reasons: ["source_record_not_loaded"], riskLevel: "medium" };
  }

  if (source.brandId !== brandConfig.recordId) {
    return { recommendation: "reject", reasons: ["source_wrong_brand_link"], riskLevel: "high" };
  }

  if (nz(source.approvedForExplorerUse) !== "Yes") {
    reasons.push("source_explorer_use_not_yes");
    riskLevel = "medium";
  }
  if (nz(source.approvedForExtraction) !== "Yes") {
    reasons.push("source_extraction_not_yes");
    riskLevel = "medium";
  }
  if (isBlockedUrl(source.sourceUrl)) {
    return { recommendation: "reject", reasons: ["blocked_source_url"], riskLevel: "high" };
  }

  const dupKey = `${fieldKey}::${normalizeValue(value)}`;
  if (context.approvedKeyValues?.has(dupKey)) {
    return { recommendation: "reject", reasons: ["duplicate_existing_approved_fact"], riskLevel: "medium" };
  }
  if (context.pendingKeyValues?.get(dupKey) && context.pendingKeyValues.get(dupKey) !== fact.id) {
    return { recommendation: "reject", reasons: ["duplicate_pending_same_key_value"], riskLevel: "medium" };
  }

  if (fact.dataGap === "Yes" || value.includes("—") && value.length < 30) {
    return { recommendation: "reject", reasons: ["gap_fact"], riskLevel: "high" };
  }

  if (COMPANY_VALIDATED_RE.test(value) || COMPANY_VALIDATED_RE.test(evidence)) {
    return { recommendation: "reject", reasons: ["implies_company_validated"], riskLevel: "high" };
  }

  if (OVERCLAIM_RE.test(value) && !/\bchoice\b/i.test(evidence)) {
    return { recommendation: "hold", reasons: ["possible_overclaim"], riskLevel: "high" };
  }

  if (isAiInterpretedFact(fact)) {
    reasons.push("ai_interpreted_review_carefully");
    riskLevel = "medium";
  }

  if (fieldKey === "be.positioning.summary" && isNoisyPositioningSummary(value)) {
    return {
      recommendation: "hold",
      reasons: ["noisy_pdf_layout_language"],
      riskLevel: "medium",
    };
  }

  if (isWeakEvidence(evidence, value, fieldKey)) {
    return { recommendation: "hold", reasons: ["weak_evidence_quote"], riskLevel: "medium" };
  }

  if (isFragmentaryValue(fieldKey, value)) {
    return { recommendation: "hold", reasons: ["fragmentary_value"], riskLevel: "medium" };
  }

  if (CAREFUL_REVIEW_KEYS.has(fieldKey)) {
    if (fieldKey === "be.overview.typicalUseCase") {
      return { recommendation: "hold", reasons: ["careful_review_typical_use_case"], riskLevel: "medium" };
    }
    if (value.length < 40) {
      return { recommendation: "hold", reasons: ["careful_review_short_why_value"], riskLevel: "medium" };
    }
    reasons.push("careful_review_secondary_key");
    riskLevel = "low";
  }

  if (reasons.some((r) => r.startsWith("source_"))) {
    return { recommendation: "hold", reasons, riskLevel };
  }

  if (PRIORITY_APPROVE_KEYS.has(fieldKey) || CAREFUL_REVIEW_KEYS.has(fieldKey)) {
    return {
      recommendation: "approve",
      reasons: reasons.length ? reasons : ["clean_priority_fact"],
      riskLevel,
    };
  }

  return { recommendation: "hold", reasons: ["non_priority_key_manual_review"], riskLevel: "medium" };
}

export function buildFactReviewRow(fact, source, brandConfig, assessment) {
  return {
    factId: fact.id,
    fieldKey: fact.fieldName,
    sourceId: fact.sourceRecordId,
    sourceTitle: source?.sourceTitle || null,
    valuePreview: nz(fact.extractedValue).slice(0, 200),
    evidencePreview: nz(fact.evidenceText).slice(0, 200),
    extractionRunId: fact.extractionRunId || null,
    confidenceLevel: fact.confidenceLevel || null,
    recommendation: assessment.recommendation,
    reasons: assessment.reasons,
    riskLevel: assessment.riskLevel,
    humanReviewStatus: fact.humanReviewStatus,
  };
}

export function projectGovernanceAfterApproval(brandConfig, sources, facts, approveFactIds) {
  const approvedSet = new Set(approveFactIds);
  const simulatedFacts = simulateFactsApproved(facts, approveFactIds);
  const pkg = buildPackageFromRecords({
    sources,
    facts: simulatedFacts,
    published: [],
    entityType: "brand",
    targetRecId: brandConfig.recordId,
  });
  const targetProfile = { id: brandConfig.recordId, entityType: "brand", name: brandConfig.brandName };
  const assessment = assessPackageReadiness(pkg, targetProfile);
  const approvedInScope = simulatedFacts.filter(
    (f) =>
      approvedSet.has(f.id) &&
      f.brandId === brandConfig.recordId &&
      ["Approved", "Edited"].includes(nz(f.humanReviewStatus))
  );

  return {
    approvedFactCount: approvedInScope.length,
    eligible: assessment.eligible,
    blockReasons: assessment.blockReasons,
    warnings: assessment.warnings,
    projectedStatus: assessment.eligible
      ? "likely_governance_publish_dry_run_eligible"
      : "blocked_pending_more_approved_facts_or_sources",
  };
}

export async function buildBrandFactStewardshipReport(brandConfig, { allFacts, sourcesById } = {}) {
  const brandFacts = (allFacts || []).filter((f) => f.brandId === brandConfig.recordId);
  const pendingFacts = brandFacts.filter(
    (f) => nz(f.humanReviewStatus) === "Pending" && isBatchExtractionFact(f)
  );

  const approvedKeyValues = new Set(
    brandFacts
      .filter((f) => ["Approved", "Edited"].includes(nz(f.humanReviewStatus)))
      .map((f) => `${f.fieldName}::${normalizeValue(f.extractedValue)}`)
  );

  const pendingKeyValues = new Map();
  for (const f of pendingFacts) {
    const key = `${f.fieldName}::${normalizeValue(f.extractedValue)}`;
    if (!pendingKeyValues.has(key)) pendingKeyValues.set(key, f.id);
  }

  const context = { approvedKeyValues, pendingKeyValues };
  const factRows = [];
  const counts = { approve: 0, hold: 0, reject: 0 };

  const sources = [];
  for (const sourceId of brandConfig.allowlistedSourceIds) {
    const source = sourcesById.get(sourceId) || (await getPartnerSourceById(sourceId));
    if (source) sources.push(source);
  }

  for (const fact of pendingFacts) {
    const source = sourcesById.get(fact.sourceRecordId) || sources.find((s) => s.id === fact.sourceRecordId);
    const assessment = assessPendingFact(fact, source, brandConfig, context);
    counts[assessment.recommendation] += 1;
    factRows.push(buildFactReviewRow(fact, source, brandConfig, assessment));
  }

  const approveIds = factRows.filter((r) => r.recommendation === "approve").map((r) => r.factId);
  const governanceProjection = projectGovernanceAfterApproval(
    brandConfig,
    sources,
    brandFacts,
    approveIds
  );

  const splitOutRecommended =
    counts.approve < 4 ||
    factRows.filter((r) => r.recommendation === "reject").length > 2 ||
    governanceProjection.approvedFactCount < 4;

  return {
    brandKey: brandConfig.key,
    brandName: brandConfig.brandName,
    recordId: brandConfig.recordId,
    allowlistedSourceIds: brandConfig.allowlistedSourceIds,
    pendingFactCount: pendingFacts.length,
    recommendedApproveCount: counts.approve,
    holdCount: counts.hold,
    rejectCount: counts.reject,
    factRows,
    approveFactIds: approveIds,
    governanceProjection,
    splitOutRecommended,
    splitOutReason: splitOutRecommended
      ? counts.approve < 4
        ? "too_few_clean_approve_facts"
        : "material_quality_or_governance_blockers"
      : null,
  };
}

export async function buildChoiceLegacyBatchFactStewardshipReport({
  brandFilter = null,
  batchName = DEFAULT_BATCH_NAME,
  existingFactsByBrand = null,
  brandConfigsOverride = null,
} = {}) {
  const batch = getBatchDefinition(batchName);
  const brandConfigs =
    brandConfigsOverride && brandConfigsOverride.length
      ? brandConfigsOverride
      : getBatchExtractBrandConfigs(batchName, brandFilter);
  const sourcesById = new Map();

  for (const brandConfig of brandConfigs) {
    for (const sourceId of brandConfig.allowlistedSourceIds) {
      if (!sourcesById.has(sourceId)) {
        const source = await getPartnerSourceById(sourceId);
        if (source) sourcesById.set(sourceId, source);
      }
    }
  }

  const brands = [];
  for (const brandConfig of brandConfigs) {
    let allFacts = existingFactsByBrand?.get(brandConfig.recordId);
    if (!allFacts) {
      allFacts = [];
      let offset = null;
      do {
        const page = await listPartnerFacts({ brandId: brandConfig.recordId, limit: 100, offset });
        allFacts.push(...(page.facts || []));
        offset = page.offset;
      } while (offset);
    }
    brands.push(
      await buildBrandFactStewardshipReport(brandConfig, { allFacts, sourcesById })
    );
  }

  const allApprove = brands.flatMap((b) => b.approveFactIds);
  const totalPending = brands.reduce((n, b) => n + b.pendingFactCount, 0);
  const totalApprove = brands.reduce((n, b) => n + b.recommendedApproveCount, 0);
  const totalHold = brands.reduce((n, b) => n + b.holdCount, 0);
  const totalReject = brands.reduce((n, b) => n + b.rejectCount, 0);
  const governanceEligible = brands.filter((b) => b.governanceProjection.eligible);
  const governanceBlocked = brands.filter((b) => !b.governanceProjection.eligible);
  const splitOut = brands.filter((b) => b.splitOutRecommended);

  const batchApplyCommand = `npm run choice-legacy-batch-fact-stewardship -- --batch ${batchName} --apply --approve-choice-legacy-batch-fact-stewardship`;

  return {
    stewardshipVersion: STEWARDSHIP_VERSION,
    batchName,
    batchDisplayName: batch.displayName,
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    batchExtractRunPrefix: batch.extractionRunPrefix,
    priorityApproveKeys: [...PRIORITY_APPROVE_KEYS],
    carefulReviewKeys: [...CAREFUL_REVIEW_KEYS],
    unsupportedKeys: [...UNSUPPORTED_FACT_KEYS],
    brands,
    eligibleApproveFactIds: allApprove,
    summary: {
      totalPendingFactsReviewed: totalPending,
      factsRecommendedForBatchApproval: totalApprove,
      factsHeld: totalHold,
      factsRejected: totalReject,
      brandsProjectedGovernanceEligible: governanceEligible.length,
      brandsStillBlocked: governanceBlocked.length,
      brandsToSplitOut: splitOut.length,
    },
    batchApplyCommand,
    nextRecommendedCommand:
      "npm run audit-partner-intelligence-publish-readiness && npm run publish-partner-intelligence-profile-governance -- --entity-type brand --target-rec-id <rec…> --dry-run",
    perBrandPublishCommands: brands.map(
      (b) =>
        `npm run publish-partner-intelligence-profile-governance -- --entity-type brand --target-rec-id ${b.recordId} --dry-run`
    ),
    doesNotDo: [
      "Rebuild Explorer content or overwrite Brand Setup fields",
      "Publish governance or set Company Validated / Company Validation Date",
      "Auto-approve held or rejected facts on apply",
      "Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema",
      "Approve unsupported keys (segment, chainScale) or gap facts",
    ],
  };
}

export async function applyChoiceLegacyBatchFactStewardship(report, { brandFilter = null } = {}) {
  const applied = [];
  const skipped = [];
  const errors = [];

  let brands = report.brands;
  if (brandFilter) {
    brands = brands.filter(
      (b) => b.brandKey === brandFilter || b.recordId === brandFilter
    );
  }

  const approveIds = new Set(report.eligibleApproveFactIds || []);
  for (const brand of brands) {
    for (const factId of brand.approveFactIds || []) {
      if (!approveIds.has(factId)) continue;

      let fact = null;
      let offset = null;
      do {
        const page = await listPartnerFacts({ brandId: brand.recordId, limit: 100, offset });
        fact = (page.facts || []).find((f) => f.id === factId);
        offset = page.offset;
      } while (!fact && offset);

      if (!fact) {
        skipped.push({ factId, brand: brand.brandName, reason: "fact_not_found" });
        continue;
      }

      const patchResult = buildSafeFactPatch(fact, "brand", brand.recordId, {
        approvedFactIds: approveIds,
        allowWrites: true,
      });

      if (!patchResult.patch) {
        skipped.push({
          factId,
          brand: brand.brandName,
          reason: patchResult.skipped?.join("; ") || "patch_blocked",
        });
        continue;
      }

      try {
        await patchPartnerFact(factId, patchResult.patch);
        applied.push({
          factId,
          brand: brand.brandName,
          recordId: brand.recordId,
          fieldKey: fact.fieldName,
          patch: patchResult.patch,
        });
      } catch (err) {
        errors.push({
          factId,
          brand: brand.brandName,
          message: err.message || String(err),
        });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
  }

  return { applied, skipped, errors, rejected: false };
}

export function buildChoiceLegacyBatchFactStewardshipMarkdown(report) {
  const s = report.summary;
  const lines = [
    "# Choice Legacy Mini-Batch Fact Stewardship v1",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Executive summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Pending facts reviewed | ${s.totalPendingFactsReviewed} |`,
    `| Recommended for batch approval | ${s.factsRecommendedForBatchApproval} |`,
    `| Held | ${s.factsHeld} |`,
    `| Rejected (recommendation) | ${s.factsRejected} |`,
    `| Brands projected governance-eligible | ${s.brandsProjectedGovernanceEligible} |`,
    `| Brands still blocked | ${s.brandsStillBlocked} |`,
    `| Brands to split out | ${s.brandsToSplitOut} |`,
    "",
    "### Batch apply command",
    "",
    "```bash",
    report.batchApplyCommand,
    "```",
    "",
    "## Brands",
    "",
  ];

  for (const brand of report.brands) {
    lines.push(`### ${brand.brandName}`, "");
    lines.push(
      `- Record: \`${brand.recordId}\``,
      `- Pending reviewed: **${brand.pendingFactCount}**`,
      `- Approve: **${brand.recommendedApproveCount}** · Hold: **${brand.holdCount}** · Reject: **${brand.rejectCount}**`,
      `- Governance after approval: **${brand.governanceProjection.projectedStatus}**`,
      `- Split out: **${brand.splitOutRecommended ? "yes" : "no"}**${brand.splitOutReason ? ` (${brand.splitOutReason})` : ""}`,
      "",
      "| Fact ID | Field | Source | Recommendation | Risk | Reason |",
      "|---------|-------|--------|----------------|------|--------|"
    );
    for (const row of brand.factRows) {
      lines.push(
        `| \`${row.factId}\` | \`${row.fieldKey}\` | ${row.sourceTitle || row.sourceId} | **${row.recommendation}** | ${row.riskLevel} | ${row.reasons.join("; ")} |`
      );
      lines.push(`| | value: ${row.valuePreview} | | | | |`);
    }
    lines.push("");
  }

  if (report.applyResult) {
    lines.push(
      "## Apply result",
      "",
      `- Applied: **${report.applyResult.applied?.length ?? 0}**`,
      `- Skipped: **${report.applyResult.skipped?.length ?? 0}**`,
      `- Errors: **${report.applyResult.errors?.length ?? 0}**`,
      ""
    );
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) lines.push(`- ${item}`);
  lines.push("");

  return lines.join("\n");
}
