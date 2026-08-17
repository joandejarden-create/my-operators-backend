/**
 * Choice legacy mini-batch 1 — batch profile governance publish (dry-run default).
 * Comfort, Everhome, Quality — company-materials governance only.
 * @see docs/data-intelligence/choice-legacy-batch-governance-publish-v1.md
 */
import {
  GOVERNANCE_VALIDATION_STATUS,
  GOVERNANCE_USAGE_PERMISSION,
  GOVERNANCE_EXTERNAL_DISPLAY,
  GOVERNANCE_EXTERNAL_DISPLAY_LABEL,
  GOVERNANCE_EXTERNAL_SOURCE_BASIS,
} from "../profile-governance/profile-governance-fields.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { getPartnerSourceById } from "./airtable-source.js";
import {
  DEFAULT_BATCH_NAME,
  getBatchDefinition,
  getBatchExtractBrandConfigs,
  MINI_BATCH_EXTRACT_BRANDS,
} from "./choice-legacy-batch-config.js";
import {
  assessPackageReadiness,
  assessPublishScopeSourceBasis,
  isApprovedExplorerSource,
  isApprovedPublishFact,
  classifySourceBasisBucket,
  SOURCE_READY_STATUSES,
} from "./profile-governance-publish-readiness.js";
import {
  BRAND_TABLE,
  NEVER_PUBLISH_API_KEYS,
  buildPublishPlanEntry,
} from "./profile-governance-publish.js";
import { buildPackageFromRecords } from "./stewardship-package.js";

export const GOVERNANCE_PUBLISH_VERSION = "1";
export const REPORT_JSON_NAME = "choice-legacy-batch-governance-publish.json";
export const REPORT_MD_NAME = "choice-legacy-batch-governance-publish.md";

export const EXPECTED_VALIDATION_STATUS = GOVERNANCE_VALIDATION_STATUS.companyPublished;
export const EXPECTED_DISPLAY_LABEL = GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyPublished;
export const EXPECTED_SOURCE_BASIS = GOVERNANCE_EXTERNAL_SOURCE_BASIS.companyPublished;
export const EXPECTED_USAGE_PERMISSION = GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed;
export const EXPECTED_EXTERNAL_DISPLAY = GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel;

export const BATCH_APPLY_COMMAND =
  "npm run choice-legacy-batch-governance-publish -- --apply --approve-choice-legacy-batch-governance-publish";

export const POST_APPLY_VERIFY_COMMANDS = [
  "npm run audit-partner-intelligence-publish-readiness",
  "npm run active-brand-governance-upgrade -- --dry-run",
  "npm run test:partner-intelligence-publish-readiness",
  "npm run test:partner-intelligence-profile-governance-publish",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function getBatchGovernanceBrandConfigs(batchName = DEFAULT_BATCH_NAME, brandFilter = null) {
  return getBatchExtractBrandConfigs(batchName, brandFilter);
}

export function countBrandFacts(facts, brandConfig) {
  const brandFacts = (facts || []).filter((f) => f.brandId === brandConfig.recordId);
  const allowlist = new Set(brandConfig.allowlistedSourceIds);
  const approved = brandFacts.filter(
    (f) =>
      isApprovedPublishFact(f) &&
      allowlist.has(f.sourceRecordId)
  );
  const pending = brandFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  return {
    approvedFactCount: approved.length,
    pendingFactCount: pending.length,
    approvedFactIds: approved.map((f) => f.id),
  };
}

export function countApprovedCompanySources(sources, brandConfig) {
  const rows = (sources || []).filter(
    (s) =>
      brandConfig.allowlistedSourceIds.includes(s.id) &&
      isApprovedExplorerSource(s)
  );
  const publishScopeRows = rows.filter((s) => {
    const status = nz(s.status);
    return SOURCE_READY_STATUSES.has(status) || status === "Needs Review";
  });
  const basis = assessPublishScopeSourceBasis(publishScopeRows.length ? publishScopeRows : rows);
  const checkRows = publishScopeRows.length ? publishScopeRows : rows;
  const nonCompany = checkRows.filter((s) => classifySourceBasisBucket(s) !== "company");
  return {
    approvedSourceCount: checkRows.length,
    explorerApprovedSourceCount: rows.length,
    allCompanyControlled: basis.allCompany && nonCompany.length === 0 && checkRows.length > 0,
    sourceBasis: basis,
    approvedSources: checkRows.map((s) => ({
      id: s.id,
      sourceTitle: s.sourceTitle,
      sourceType: s.sourceType,
      status: s.status,
      profileSourceType: classifySourceBasisBucket(s),
    })),
    nonCompanySourceIds: nonCompany.map((s) => s.id),
  };
}

/**
 * Batch-specific governance posture guards (beyond standard readiness).
 */
export function assessBatchGovernancePosture({
  readiness,
  planEntry,
  sourceSummary,
}) {
  const blockers = [];
  const warnings = [...(readiness?.warnings || [])];

  if (!readiness?.eligible) {
    blockers.push("readiness_not_eligible");
    for (const r of readiness?.blockReasons || []) {
      blockers.push(`readiness:${r}`);
    }
  }

  if (!sourceSummary.allCompanyControlled) {
    blockers.push("not_all_company_controlled_sources");
    if (sourceSummary.nonCompanySourceIds?.length) {
      blockers.push(`misclassified_sources:${sourceSummary.nonCompanySourceIds.join(",")}`);
    }
  }

  const proposed = planEntry?.proposed || readiness?.proposal?.proposed || null;
  const expected = planEntry?.expectedGovernance || readiness?.proposal?.expectedGovernance || null;

  if (!proposed) {
    blockers.push("missing_proposed_governance");
    return { blockers: [...new Set(blockers)], warnings: [...new Set(warnings)], eligible: false };
  }

  if (proposed.validationStatus === GOVERNANCE_VALIDATION_STATUS.sourceInformed) {
    blockers.push("proposes_source_informed_validation");
  }
  if (proposed.validationStatus !== EXPECTED_VALIDATION_STATUS) {
    if (proposed.validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated) {
      blockers.push("proposes_company_validated_validation");
    } else if (!blockers.includes("proposes_source_informed_validation")) {
      warnings.push(`unexpected_validation_status:${proposed.validationStatus}`);
    }
  }

  if (proposed.companyValidated === true) {
    blockers.push("proposes_company_validated_checkbox");
  }
  if (proposed.companyValidationDate) {
    blockers.push("proposes_company_validation_date");
  }

  if (proposed.externalDisplayStatus === GOVERNANCE_EXTERNAL_DISPLAY.doNotDisplay) {
    blockers.push("proposes_do_not_display");
  }
  if (proposed.usagePermission === GOVERNANCE_USAGE_PERMISSION.doNotUse) {
    blockers.push("proposes_do_not_use");
  }

  const displayLabel = expected?.displayLabel || null;
  const sourceBasis =
    expected?.sourceBasis ||
    (expected?.displaySubtitle?.includes("Source Basis:")
      ? expected.displaySubtitle.split("Source Basis:")[1]?.split("·")[0]?.trim()
      : null);

  if (displayLabel === GOVERNANCE_EXTERNAL_DISPLAY_LABEL.sourceInformed) {
    blockers.push("expected_chip_source_informed_profile");
  }
  if (sourceBasis === GOVERNANCE_EXTERNAL_SOURCE_BASIS.sourceInformed) {
    blockers.push("expected_source_basis_reviewed_sources");
  }

  if (planEntry?.protection?.blocked) {
    for (const r of planEntry.protection.reasons || []) {
      blockers.push(`protected:${r}`);
    }
  }

  if (planEntry?.changeClass === "downgrade" || readiness?.changeClass === "downgrade") {
    blockers.push("would_downgrade_existing_governance");
  }

  const patchKeys = Object.keys(planEntry?.write?.patch || {});
  for (const key of patchKeys) {
    if (/company validated/i.test(key) && planEntry.write.patch[key] === true) {
      blockers.push("patch_includes_company_validated");
    }
    if (/company validation date/i.test(key) && planEntry.write.patch[key]) {
      blockers.push("patch_includes_company_validation_date");
    }
  }

  for (const key of NEVER_PUBLISH_API_KEYS) {
    if (proposed[key] != null && proposed[key] !== false) {
      blockers.push(`never_publish_field_present:${key}`);
    }
  }

  const writeOk =
    planEntry?.write?.status === "dry_run" ||
    planEntry?.write?.status === "pending_apply" ||
    planEntry?.write?.reason === "no_changes";

  return {
    blockers: [...new Set(blockers)],
    warnings: [...new Set(warnings)],
    eligible: blockers.length === 0 && writeOk,
    proposed,
    expectedGovernance: expected,
    displayLabel: displayLabel || EXPECTED_DISPLAY_LABEL,
    sourceBasis: sourceBasis || EXPECTED_SOURCE_BASIS,
  };
}

export function buildBrandGovernancePublishRow({
  brandConfig,
  sources,
  facts,
  targetProfile,
  mode = "dry-run",
  applyTimestamp = null,
}) {
  const sourceSummary = countApprovedCompanySources(sources, brandConfig);
  const factSummary = countBrandFacts(facts, brandConfig);

  const pkg = buildPackageFromRecords({
    sources,
    facts,
    published: [],
    entityType: "brand",
    targetRecId: brandConfig.recordId,
  });

  const readiness = assessPackageReadiness(pkg, targetProfile);

  const packageEntry = {
    entityKey: `brand:${brandConfig.recordId}`,
    entityType: "brand",
    recordId: brandConfig.recordId,
    entityName: brandConfig.brandName,
    blockReasons: readiness.blockReasons,
    changeClass: readiness.changeClass,
    proposed: readiness.proposal,
  };

  const planEntry = buildPublishPlanEntry({
    packageEntry,
    targetProfile,
    mode,
    applyTimestamp,
  });

  const posture = assessBatchGovernancePosture({
    readiness,
    planEntry,
    sourceSummary,
  });

  const batchEligible = posture.eligible && posture.blockers.length === 0;

  const splitOutRecommended =
    posture.blockers.some((b) =>
      /misclassified|source_informed|reviewed_sources|downgrade|protected/.test(b)
    ) || !sourceSummary.allCompanyControlled;

  return {
    brandKey: brandConfig.key,
    brandName: brandConfig.brandName,
    recordId: brandConfig.recordId,
    allowlistedSourceIds: brandConfig.allowlistedSourceIds,
    approvedSourceCount: sourceSummary.approvedSourceCount,
    approvedFactCount: factSummary.approvedFactCount,
    pendingFactCount: factSummary.pendingFactCount,
    allCompanyControlledSources: sourceSummary.allCompanyControlled,
    readinessStatus: readiness.eligible ? "eligible" : "blocked",
    readinessBlockReasons: readiness.blockReasons,
    changeClass: readiness.changeClass,
    proposedGovernance: posture.proposed,
    proposedGovernanceFields: posture.proposed
      ? {
          validationStatus: posture.proposed.validationStatus,
          usagePermission: posture.proposed.usagePermission,
          sourceType: posture.proposed.sourceType,
          sourceRegion: posture.proposed.sourceRegion,
          confidenceLevel: posture.proposed.confidenceLevel,
          lastReviewedDate: posture.proposed.lastReviewedDate,
          externalDisplayStatus: posture.proposed.externalDisplayStatus,
          companyValidated: posture.proposed.companyValidated,
          companyValidationDate: posture.proposed.companyValidationDate,
        }
      : null,
    expectedExplorerChip: posture.displayLabel,
    expectedSourceBasis: posture.sourceBasis,
    expectedDisplaySubtitle: posture.expectedGovernance?.displaySubtitle || null,
    blockers: posture.blockers,
    warnings: posture.warnings,
    eligibleForBatchApply: batchEligible,
    splitOutRecommended,
    splitOutReason: splitOutRecommended
      ? posture.blockers[0] || "unique_governance_risk"
      : null,
    publishPlan: {
      writeStatus: planEntry.write?.status || null,
      writeReason: planEntry.write?.reason || null,
      fieldUpdates: planEntry.fieldDiff?.wouldUpdate || [],
      skippedFields: planEntry.fieldDiff?.skipped || [],
      patch: planEntry.write?.patch || null,
    },
    currentGovernance: planEntry.currentGovernance,
    protection: planEntry.protection,
  };
}

export async function buildChoiceLegacyBatchGovernancePublishReport({
  brandFilter = null,
  batchName = DEFAULT_BATCH_NAME,
  targetProfilesById = null,
  sourcesById = null,
  factsByBrand = null,
  mode = "dry-run",
  applyTimestamp = null,
  brandConfigsOverride = null,
} = {}) {
  const batch = getBatchDefinition(batchName);
  const brandConfigs =
    brandConfigsOverride && brandConfigsOverride.length
      ? brandConfigsOverride
      : getBatchGovernanceBrandConfigs(batchName, brandFilter);
  const localSourcesById = sourcesById || new Map();
  const localFactsByBrand = factsByBrand || new Map();
  const profiles = targetProfilesById || new Map();

  for (const brandConfig of brandConfigs) {
    for (const sourceId of brandConfig.allowlistedSourceIds) {
      if (!localSourcesById.has(sourceId)) {
        const source = await getPartnerSourceById(sourceId);
        if (source) localSourcesById.set(sourceId, source);
      }
    }

    if (!localFactsByBrand.has(brandConfig.recordId)) {
      const allFacts = [];
      let offset = null;
      do {
        const page = await listPartnerFacts({ brandId: brandConfig.recordId, limit: 100, offset });
        allFacts.push(...(page.facts || []));
        offset = page.offset;
      } while (offset);
      localFactsByBrand.set(brandConfig.recordId, allFacts);
    }
  }

  const brands = [];
  for (const brandConfig of brandConfigs) {
    const sources = brandConfig.allowlistedSourceIds
      .map((id) => localSourcesById.get(id))
      .filter(Boolean);
    const facts = localFactsByBrand.get(brandConfig.recordId) || [];
    const targetProfile =
      profiles.get(brandConfig.recordId) ||
      ({ id: brandConfig.recordId, entityType: "brand", name: brandConfig.brandName, fields: {} });

    brands.push(
      buildBrandGovernancePublishRow({
        brandConfig,
        sources,
        facts,
        targetProfile,
        mode,
        applyTimestamp,
      })
    );
  }

  const eligible = brands.filter((b) => b.eligibleForBatchApply);
  const blocked = brands.filter((b) => !b.eligibleForBatchApply);
  const splitOut = brands.filter((b) => b.splitOutRecommended);

  const batchApplyCommand = `npm run choice-legacy-batch-governance-publish -- --batch ${batchName} --apply --approve-choice-legacy-batch-governance-publish`;

  return {
    governancePublishVersion: GOVERNANCE_PUBLISH_VERSION,
    batchName,
    batchDisplayName: batch.displayName,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified: false,
    sourceTable: BRAND_TABLE,
    expectedPosture: {
      validationStatus: EXPECTED_VALIDATION_STATUS,
      usagePermission: EXPECTED_USAGE_PERMISSION,
      externalDisplayStatus: EXPECTED_EXTERNAL_DISPLAY,
      displayLabel: EXPECTED_DISPLAY_LABEL,
      sourceBasis: EXPECTED_SOURCE_BASIS,
      companyValidated: false,
      companyValidationDate: null,
    },
    brands,
    summary: {
      totalBrands: brands.length,
      eligibleForGovernancePublish: eligible.length,
      blocked: blocked.length,
      skipped: 0,
      splitOutRecommended: splitOut.length,
      proposedChipSummary: EXPECTED_DISPLAY_LABEL,
      proposedSourceBasisSummary: EXPECTED_SOURCE_BASIS,
    },
    batchApplyCommand,
    postApplyVerificationCommands: POST_APPLY_VERIFY_COMMANDS,
    doesNotDo: [
      "Rebuild Explorer content or overwrite Brand Setup content fields",
      "Approve more facts or change Human Review Status",
      "Set Company Validated or Company Validation Date",
      "Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema",
      "Publish Source-Informed / Reviewed Sources posture for official Choice company materials",
      "Downgrade stronger live governance",
    ],
  };
}

export async function applyChoiceLegacyBatchGovernancePublish(
  report,
  { brandFilter = null, applyPatch } = {}
) {
  if (typeof applyPatch !== "function") {
    throw new Error("applyPatch function is required for apply mode");
  }

  const applied = [];
  const skipped = [];
  const errors = [];

  let brands = report.brands || [];
  if (brandFilter) {
    brands = brands.filter(
      (b) => b.brandKey === brandFilter || b.recordId === brandFilter
    );
  }

  for (const brand of brands) {
    if (!brand.eligibleForBatchApply) {
      skipped.push({
        brand: brand.brandName,
        recordId: brand.recordId,
        reason: brand.blockers[0] || "not_eligible_for_batch_apply",
        blockers: brand.blockers,
      });
      continue;
    }

    const patch = brand.publishPlan?.patch;
    if (!patch || !Object.keys(patch).length) {
      skipped.push({
        brand: brand.brandName,
        recordId: brand.recordId,
        reason: brand.publishPlan?.writeReason || "no_changes",
      });
      continue;
    }

    try {
      const result = await applyPatch({
        table: BRAND_TABLE,
        recordId: brand.recordId,
        patch,
      });
      applied.push({
        brand: brand.brandName,
        recordId: brand.recordId,
        fieldCount: result.fieldCount ?? Object.keys(patch).length,
        patch,
      });
    } catch (err) {
      errors.push({
        brand: brand.brandName,
        recordId: brand.recordId,
        message: err.message || String(err),
      });
    }
    await new Promise((r) => setTimeout(r, 250));
  }

  return { applied, skipped, errors };
}

export function buildChoiceLegacyBatchGovernancePublishMarkdown(report) {
  const s = report.summary;
  const lines = [
    "# Choice Legacy Mini-Batch Governance Publish v1",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `Target table: **${report.sourceTable}**`,
    "",
    "## Executive summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Total brands | ${s.totalBrands} |`,
    `| Eligible for governance publish | ${s.eligibleForGovernancePublish} |`,
    `| Blocked | ${s.blocked} |`,
    `| Skipped (apply) | ${s.skipped} |`,
    `| Split-out recommended | ${s.splitOutRecommended} |`,
    "",
    `**Proposed chip:** ${s.proposedChipSummary} · **Source Basis:** ${s.proposedSourceBasisSummary}`,
    "",
    "### Expected posture",
    "",
    "| Field | Value |",
    "|-------|-------|",
    `| Validation Status | ${report.expectedPosture.validationStatus} |`,
    `| Usage Permission | ${report.expectedPosture.usagePermission} |`,
    `| External Display Status | ${report.expectedPosture.externalDisplayStatus} |`,
    `| Explorer chip | ${report.expectedPosture.displayLabel} |`,
    `| Source Basis | ${report.expectedPosture.sourceBasis} |`,
    `| Company Validated | unchanged (false) |`,
    "",
    "### Batch apply command",
    "",
    "```bash",
    report.batchApplyCommand,
    "```",
    "",
    "### Post-apply verification",
    "",
    "```bash",
    ...report.postApplyVerificationCommands,
    "```",
    "",
    "## Brands",
    "",
  ];

  for (const brand of report.brands) {
    lines.push(`### ${brand.brandName}`, "");
    lines.push(
      `- Record: \`${brand.recordId}\``,
      `- Approved sources: **${brand.approvedSourceCount}** · Approved facts: **${brand.approvedFactCount}** · Pending facts: **${brand.pendingFactCount}**`,
      `- Company-controlled sources: **${brand.allCompanyControlledSources ? "yes" : "no"}**`,
      `- Readiness: **${brand.readinessStatus}** · Change class: **${brand.changeClass || "n/a"}**`,
      `- Eligible for batch apply: **${brand.eligibleForBatchApply ? "yes" : "no"}**`,
      `- Split out: **${brand.splitOutRecommended ? "yes" : "no"}**${brand.splitOutReason ? ` (${brand.splitOutReason})` : ""}`,
      `- Expected chip: **${brand.expectedExplorerChip}** · Source Basis: **${brand.expectedSourceBasis}**`,
      ""
    );

    if (brand.proposedGovernanceFields) {
      lines.push("**Proposed governance fields:**", "");
      lines.push("| Field | Value |", "|-------|-------|");
      for (const [k, v] of Object.entries(brand.proposedGovernanceFields)) {
        lines.push(`| ${k} | ${v == null ? "—" : JSON.stringify(v)} |`);
      }
      lines.push("");
    }

    if (brand.expectedDisplaySubtitle) {
      lines.push(`**Expected subtitle:** \`${brand.expectedDisplaySubtitle}\``, "");
    }

    if (brand.publishPlan?.fieldUpdates?.length) {
      lines.push("**Field diff (would update):**", "");
      lines.push("| Field | From | To |", "|-------|------|-----|");
      for (const row of brand.publishPlan.fieldUpdates) {
        lines.push(
          `| \`${row.field}\` | ${row.from == null ? "—" : JSON.stringify(row.from)} | ${JSON.stringify(row.to)} |`
        );
      }
      lines.push("");
    } else if (brand.publishPlan?.writeReason === "no_changes") {
      lines.push("**Field diff:** no changes (live Setup already matches proposal).", "");
    }

    if (brand.blockers.length) {
      lines.push("**Blockers:**");
      for (const b of brand.blockers) lines.push(`- ${b}`);
      lines.push("");
    }
    if (brand.warnings.length) {
      lines.push("**Warnings:**");
      for (const w of brand.warnings) lines.push(`- ${w}`);
      lines.push("");
    }
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

export { MINI_BATCH_EXTRACT_BRANDS };
