/**
 * Brand Explorer Brand Asset Registry Discovery + Approval Queue Writer v31B.
 *
 * Generalizes Tribute-only asset registry workflow for expansion brands.
 * Discovers image candidates, audits presentation row usage, flags wrong-brand
 * imagery, and stages metadata-only registry records — never auto-approves or
 * materializes images into presentation rows.
 *
 * @see docs/data-intelligence/brand-explorer-brand-asset-registry-discovery-writer-v31B.md
 */
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import {
  resolveBrandTarget,
  getBrandTargetResolverContext,
} from "./brand-explorer-brand-target-resolver.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import {
  ASSET_STATUS,
  ASSET_TYPE,
  SOURCE_BASIS,
} from "./brand-asset-pr-package-governance.js";
import {
  applyRegistryRecords,
  buildRegistryDedupeKey,
  listRegistryAssetsForBrand,
  MAP_BRAND_ASSET,
} from "./brand-asset-registry-workflow.js";
import {
  DISCOVERY_BRAND_CONFIG,
  assessPresentationRowImageGovernance,
  detectWrongBrandSignageRisk,
  listVisualPresentationRows,
} from "./brand-explorer-brand-asset-image-governance.js";

export const WRITER_VERSION = "31B";
export const REPORT_JSON_NAME = "brand-explorer-brand-asset-registry-discovery-writer.json";
export const REPORT_MD_NAME = "brand-explorer-brand-asset-registry-discovery-writer.md";
export const DOC_MD_NAME = "brand-explorer-brand-asset-registry-discovery-writer-v31B.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31B-brand-asset-registry-candidate-creation";
export const APPLY_FLAG_QUEUE = "--create-pending-image-review-queue";
export const APPLY_FLAG_NO_MATERIALIZE = "--confirm-no-unapproved-image-materialization";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "radisson",
  "ascend",
  "radisson-blu",
  "kimpton",
  "curio-collection",
]);

/** Re-export discovery brand configs (single source: image-governance module). */
export {
  DISCOVERY_BRAND_CONFIG,
  getDiscoveryBrandConfig,
} from "./brand-explorer-brand-asset-image-governance.js";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.md",
  "reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.json",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.md",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "lib/partner-intelligence/brand-asset-registry-workflow.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "live Radisson Individuals presentation rows",
  "live Brand Asset Registry records",
  "live Source Library records",
  "live Partner Facts",
  "Tribute Brand Asset Registry records (structure reference)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-brand-asset-registry-discovery-writer.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "scripts/brand-explorer-brand-asset-registry-discovery-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function resolveDiscoveryBrand(brandArg) {
  const slug = nz(brandArg).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected — v31B targets expansion brands only`);
  }
  const config = DISCOVERY_BRAND_CONFIG[slug];
  if (!config) {
    throw new Error(
      `Unknown discovery brand: ${brandArg}. Known: ${Object.keys(DISCOVERY_BRAND_CONFIG).join(", ")}`
    );
  }
  return { ...config };
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function inferAssetTypeForSlot(slotKey) {
  if (slotKey === "overview.hero") return ASSET_TYPE.HERO;
  if (slotKey === "footprint.openings") return ASSET_TYPE.PR_IMAGE;
  if (/materials\.gallery/.test(slotKey)) return ASSET_TYPE.EXTERIOR;
  if (/overview\.scenario|valueOwners\.scenario/.test(slotKey)) return ASSET_TYPE.LIFESTYLE;
  return ASSET_TYPE.EXTERIOR;
}

function inferSourceBasis(url, brandConfig) {
  const u = nz(url).toLowerCase();
  if (!u) return SOURCE_BASIS.UNKNOWN;
  if (brandConfig.officialDomains?.some((d) => u.includes(d))) return SOURCE_BASIS.COMPANY_MATERIALS;
  return SOURCE_BASIS.UNKNOWN;
}

function isOfficialUrl(url, brandConfig) {
  const u = nz(url).toLowerCase();
  return Boolean(brandConfig.officialDomains?.some((d) => u.includes(d)));
}

function buildStagedCandidate(partial, stagingRunId) {
  return {
    ...partial,
    stagingRunId,
    explorerUsePermission: partial.explorerUsePermission || "Candidate Only",
    usageReviewStatus: partial.usageReviewStatus || "Pending Review",
    metadataOnly: true,
    safeMetadataOnly: true,
  };
}

export function auditCurrentImageUsage(brand, brandConfig, registryAssets) {
  const rows = listVisualPresentationRows(brand);
  return rows.map((row) => {
    const assessment = assessPresentationRowImageGovernance(row, brandConfig, registryAssets);
    const humanSignageReviewRequired =
      Boolean(row.imageUrl) &&
      /footprint\.openings/.test(row.slotKey) &&
      !assessment?.registryApproved;
    const wrongBrandFromCopy = assessment?.wrongBrandRisk || null;
    const wrongBrandRisk =
      wrongBrandFromCopy ||
      (humanSignageReviewRequired
        ? {
            markerId: "human_visual_signage_review",
            severity: "high",
            excerpt: nz(row.title),
            reason:
              "Opening example image materialized without approved registry — founder must verify signage (e.g. no Quality Inn / unrelated Choice brand imagery).",
          }
        : null);
    return {
      presentationRowId: row.recordId,
      slot: row.slotKey,
      title: nz(row.title),
      imageUrl: nz(row.imageUrl) || null,
      sourceUrl: nz(row.summaryUrl) || null,
      registryRecordId: assessment?.registryRecordId || null,
      registryApproved: assessment?.registryApproved || false,
      brandMatched: !wrongBrandRisk,
      wrongBrandRisk,
      humanSignageReviewRequired,
      recommendation: wrongBrandRisk
        ? "replace_and_queue_review"
        : assessment?.recommendation || "pending_image_review",
      pendingImageReview: assessment?.pendingImageReview ?? true,
    };
  });
}

export function discoverImageCandidates({
  brand,
  brandConfig,
  registryAssets,
  liveState,
  imageUsageAudit,
  stagingRunId,
}) {
  const candidates = [];
  const approvedSources = (liveState?.sources || []).filter(isApprovedExplorerSource);

  for (const source of approvedSources) {
    const pageUrl = nz(source.sourceUrl);
    if (!pageUrl || !isOfficialUrl(pageUrl, brandConfig)) continue;
    candidates.push(
      buildStagedCandidate(
        {
          assetName: `${brandConfig.name} — source-backed visual candidate (${nz(source.sourceTitle) || source.id})`,
          assetType: ASSET_TYPE.PRESS_LINK,
          assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
          sourceBasis: inferSourceBasis(pageUrl, brandConfig),
          sourceUrl: null,
          sourcePageUrl: pageUrl,
          recommendedExplorerSlot: "footprint.openings",
          isPrimaryCandidate: false,
          sourceNotes:
            "Approved Source Library page — pending explicit image URL extraction and founder review. v31B metadata-only.",
          reviewNotes: "Needs Founder Review — do not materialize to presentation until approved.",
        },
        stagingRunId
      )
    );
  }

  if (brandConfig.pressKitUrl) {
    candidates.push(
      buildStagedCandidate(
        {
          assetName: `${brandConfig.name} — official press kit (opening/gallery candidate pool)`,
          assetType: ASSET_TYPE.PRESS_LINK,
          assetStatus: ASSET_STATUS.CANDIDATE,
          sourceBasis: SOURCE_BASIS.COMPANY_MATERIALS,
          sourceUrl: null,
          sourcePageUrl: brandConfig.pressKitUrl,
          recommendedExplorerSlot: "footprint.openings",
          isPrimaryCandidate: true,
          sourceNotes:
            "Choice/Radisson Individuals official press materials — select property-matched images after founder review.",
          reviewNotes: "Pending Image Review — link property-specific assets before Explorer materialization.",
        },
        stagingRunId
      )
    );
  }

  for (const row of imageUsageAudit) {
    if (row.wrongBrandRisk && row.imageUrl) {
      candidates.push(
        buildStagedCandidate(
          {
            assetName: `DO NOT USE — materialized wrong-brand image (${row.title || row.slot})`,
            assetType: inferAssetTypeForSlot(row.slot),
            assetStatus: ASSET_STATUS.DO_NOT_USE,
            sourceBasis: inferSourceBasis(row.imageUrl, brandConfig),
            sourceUrl: row.imageUrl,
            sourcePageUrl: row.sourceUrl || brandConfig.pressKitUrl || null,
            recommendedExplorerSlot: row.slot,
            isPrimaryCandidate: false,
            sourceNotes: `Guard row for presentation ${row.presentationRowId} — remove from active Explorer after review.`,
            reviewNotes: row.wrongBrandRisk.reason,
            doNotUseReason: `${row.wrongBrandRisk.reason} (${row.wrongBrandRisk.markerId})`,
            explorerUsePermission: "Do Not Use",
            usageReviewStatus: "Blocked",
          },
          stagingRunId
        )
      );
    } else if (row.imageUrl && isOfficialUrl(row.imageUrl, brandConfig) && !row.registryApproved) {
      candidates.push(
        buildStagedCandidate(
          {
            assetName: `${brandConfig.name} — existing row image pending review (${row.title || row.slot})`,
            assetType: inferAssetTypeForSlot(row.slot),
            assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
            sourceBasis: inferSourceBasis(row.imageUrl, brandConfig),
            sourceUrl: row.imageUrl,
            sourcePageUrl: row.sourceUrl || brandConfig.consumerUrl,
            recommendedExplorerSlot: row.slot,
            isPrimaryCandidate: /footprint\.openings/.test(row.slot),
            sourceNotes: `Tied to presentation row ${row.presentationRowId} — requires Brand Asset Registry approval before active-profile use.`,
            reviewNotes: "Pending Image Review — source-supported URL on row; founder must confirm brand/property match.",
          },
          stagingRunId
        )
      );
    } else if (!row.imageUrl && /footprint\.openings|materials\.gallery/.test(row.slot)) {
      candidates.push(
        buildStagedCandidate(
          {
            assetName: `${brandConfig.name} — ${row.slot} placeholder (${row.title || "untitled"})`,
            assetType: inferAssetTypeForSlot(row.slot),
            assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
            sourceBasis: SOURCE_BASIS.COMPANY_MATERIALS,
            sourceUrl: null,
            sourcePageUrl: brandConfig.pressKitUrl || brandConfig.consumerUrl,
            recommendedExplorerSlot: row.slot,
            isPrimaryCandidate: false,
            sourceNotes: `Presentation row ${row.presentationRowId} lacks image — queue candidate discovery from official sources only.`,
            reviewNotes: "Pending Image Review — no image materialization in v31B.",
          },
          stagingRunId
        )
      );
    }
  }

  const seen = new Set();
  return candidates.filter((c) => {
    const key = buildRegistryDedupeKey(c, brandConfig.recordId);
    if (seen.has(key)) return false;
    seen.add(key);
    const exists = (registryAssets || []).some(
      (r) =>
        buildRegistryDedupeKey(
          {
            assetType: r.assetType,
            sourceUrl: r.sourceUrl,
            assetName: r.assetName,
            recommendedExplorerSlot: r.recommendedExplorerSlot,
          },
          brandConfig.recordId
        ) === key
    );
    return !exists;
  });
}

export function buildApplyCommand({ brand = "radisson-individuals-by-choice" } = {}) {
  return [
    "npm run brand-explorer-brand-asset-registry-discovery-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_QUEUE,
    APPLY_FLAG_NO_MATERIALIZE,
  ].join(" ");
}

export async function buildBrandExplorerBrandAssetRegistryDiscoveryWriterReport({
  brandArg = "radisson-individuals-by-choice",
  apply = false,
  approveBatch = false,
  createQueue = false,
  noMaterialize = false,
} = {}) {
  const brandConfig = resolveDiscoveryBrand(brandArg);
  const ctx = await getBrandTargetResolverContext();
  const resolved = await resolveBrandTarget(brandArg, ctx);
  const recordId = resolved.recordId || brandConfig.recordId;
  if (!recordId) {
    throw new Error(`Could not resolve record ID for ${brandArg}`);
  }
  brandConfig.recordId = recordId;

  const brandBasicsBefore = await fetchBrandBasics(recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const liveState = await fetchLiveState(recordId);
  const brand = await fetchBrandApiShape(recordId);
  if (!brand) throw new Error(`Could not load brand API shape for ${recordId}`);

  const registryAssets = await listRegistryAssetsForBrand(recordId).catch(() => []);
  const imageUsageAudit = auditCurrentImageUsage(brand, brandConfig, registryAssets);
  const stagingRunId = `v31B-discovery-${brandConfig.slug}-${Date.now()}`;
  const candidateAssets = discoverImageCandidates({
    brand,
    brandConfig,
    registryAssets,
    liveState,
    imageUsageAudit,
    stagingRunId,
  });

  const missingRegistry = imageUsageAudit.filter(
    (r) => (r.imageUrl || /footprint\.openings/.test(r.slot)) && !r.registryRecordId
  );
  const wrongBrandRisks = imageUsageAudit.filter((r) => r.wrongBrandRisk);
  const presentationRowsBlocked = imageUsageAudit.filter(
    (r) =>
      r.wrongBrandRisk ||
      (r.imageUrl && !r.registryApproved) ||
      (!r.imageUrl && /footprint\.openings/.test(r.slot))
  );

  const imageApprovalQueue = imageUsageAudit
    .filter((r) => r.pendingImageReview || r.wrongBrandRisk)
    .map((r) => ({
      presentationRowId: r.presentationRowId,
      slot: r.slot,
      title: r.title,
      status: r.wrongBrandRisk ? "wrong_brand_image_review" : "pending_image_review",
      linkedCandidateAssets: candidateAssets
        .filter((c) => c.recommendedExplorerSlot === r.slot)
        .map((c) => c.assetName),
      materializeAfterApproval: false,
    }));

  const linkStrategy = imageUsageAudit.map((r) => ({
    presentationRowId: r.presentationRowId,
    slot: r.slot,
    currentRecommendation: r.recommendation,
    approvedAssetRecordId: r.registryApproved ? r.registryRecordId : null,
    futureAssetCandidates: candidateAssets
      .filter((c) => c.recommendedExplorerSlot === r.slot && c.explorerUsePermission !== "Do Not Use")
      .map((c) => ({ assetName: c.assetName, sourcePageUrl: c.sourcePageUrl, assetStatus: c.assetStatus })),
  }));

  const applyBlockers = [];
  if (candidateAssets.some((c) => detectWrongBrandSignageRisk(c.assetName, brandConfig))) {
    applyBlockers.push("candidate_copy_safety");
  }
  if (candidateAssets.some((c) => c.assetStatus === ASSET_STATUS.APPROVED_EXPLORER)) {
    applyBlockers.push("auto_approval_not_allowed");
  }

  const hasCandidates = candidateAssets.length > 0;
  const applyGatesReady = apply && approveBatch && createQueue && noMaterialize;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasCandidates;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    applyResults = await applyRegistryRecords({
      brandRecordId: recordId,
      parentCompany: brandConfig.parentCompany,
      stagedAssets: candidateAssets,
      stagingRunId,
    });
    airtableModified = applyResults.recordsCreated > 0;
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(recordId));
  } else if (apply) {
    applyResults = { blocked: true, blockers: applyBlockers, recordsCreated: 0 };
  }

  const dryRunClean = applyBlockers.length === 0 && hasCandidates;

  const report = {
    writerVersion: WRITER_VERSION,
    v31BWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brand: { ...brandConfig, resolution: resolved.resolution },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    imageUsageAudit,
    missingRegistryRecords: missingRegistry,
    wrongBrandImageRisks: wrongBrandRisks,
    candidateAssetsToCreate: candidateAssets.map((c) => ({
      assetName: c.assetName,
      assetType: c.assetType,
      assetStatus: c.assetStatus,
      intendedSlot: c.recommendedExplorerSlot,
      sourcePageUrl: c.sourcePageUrl,
      imageUrl: c.sourceUrl || null,
      approvalStatus: "Pending Image Review",
      reviewStatus: c.usageReviewStatus,
      explorerUsePermission: c.explorerUsePermission,
      brandMatchNotes: c.reviewNotes,
      useNotes: c.sourceNotes,
      doNotUse: c.assetStatus === ASSET_STATUS.DO_NOT_USE,
      dedupeKey: buildRegistryDedupeKey(c, recordId),
    })),
    imageApprovalQueue,
    presentationRowsBlockedByImageReview: presentationRowsBlocked,
    linkStrategy,
    registryAssetsFound: registryAssets.length,
    approvedSourcesCount: (liveState.sources || []).filter(isApprovedExplorerSource).length,
    applyBlockers,
    dryRunClean,
    canApply,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    airtableModified,
    applyResults,
    exactDryRunCommand: `npm run brand-explorer-brand-asset-registry-discovery-writer -- --brand ${brandConfig.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: brandConfig.slug }) : null,
    governanceNote:
      "v31B never sets Approved For Explorer, never attaches images to presentation rows, never modifies Company Validated.",
    registryFieldMap: MAP_BRAND_ASSET,
  };
  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Brand Asset Registry Discovery v${report.writerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}**`);
  lines.push(`- v31B exists: **yes**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Registry assets found: **${report.registryAssetsFound}**`);
  lines.push(`- Candidates to create: **${report.candidateAssetsToCreate.length}**`);
  lines.push(`- Wrong-brand risks: **${report.wrongBrandImageRisks.length}**`);
  lines.push(`- Pending image queue: **${report.imageApprovalQueue.length}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Image usage audit");
  for (const row of report.imageUsageAudit.slice(0, 20)) {
    lines.push(
      `- \`${row.slot}\` ${row.title || "(no title)"} — registry: ${row.registryRecordId || "none"} — ${row.recommendation}${row.wrongBrandRisk ? " ⚠ wrong-brand" : ""}`
    );
  }
  lines.push("");
  lines.push("## Apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none)");
  return lines.join("\n");
}
