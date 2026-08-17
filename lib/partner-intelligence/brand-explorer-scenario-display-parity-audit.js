/**
 * Brand Explorer Scenario Display Parity Audit v31H.
 *
 * Compares how a single overview.scenario.* slot is stored, exposed by API,
 * and rendered for two brands. Read-only — no Airtable writes.
 *
 * @see docs/data-intelligence/brand-explorer-scenario-display-parity-audit-v31H.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  MAP_BRAND_ASSET,
  listRegistryAssetsForBrand,
} from "./brand-asset-registry-workflow.js";
import {
  assessPresentationRowImageGovernance,
  DISCOVERY_BRAND_CONFIG,
  findRegistryAssetForPresentationRow,
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  classifyRegistryAsset,
} from "./brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";
import {
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import {
  detectInternalUiLanguage,
  findInternalLanguageInRow,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  getBrandTargetResolverContext,
  resolveBrandTarget,
} from "./brand-explorer-brand-target-resolver.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

export const AUDIT_VERSION = "31H";
export const REPORT_JSON_NAME = "brand-explorer-scenario-display-parity-audit.json";
export const REPORT_MD_NAME = "brand-explorer-scenario-display-parity-audit.md";
export const DOC_MD_NAME = "brand-explorer-scenario-display-parity-audit-v31H.md";

export const DEFAULT_LEFT = "radisson";
export const DEFAULT_RIGHT = "radisson-individuals-by-choice";
export const DEFAULT_SLOT = "overview.scenario.1";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const PRIOR_REPORTS = [
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.json",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json",
  "reports/brand-explorer-radisson-individuals-asset-registry-normalization-writer.json",
  "reports/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.json",
  "reports/brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.json",
];

const FILES_READ = [
  "AGENTS.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "lib/partner-intelligence/brand-explorer-openings-ui-quarantine-governance.js",
  ...PRIOR_REPORTS.map((p) => p.replace(".json", ".md")),
  ...PRIOR_REPORTS,
  "live Brand Explorer Presentation rows (left + right)",
  "live API responses (left + right)",
  "live Brand Asset Registry rows (left + right)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-scenario-display-parity-audit.js",
  "scripts/brand-explorer-scenario-display-parity-audit.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const FRONTEND_SCENARIO_RENDERING = {
  component: "brand-explorer-atelier-from-api.js",
  section: "Where This Brand Creates the Most Value (Overview tab)",
  slotPattern: "overview.scenario.{1,2,3}",
  imageLogic:
    "explorerFirstBlock(brand, 'overview.scenario.N').imageUrl — if present renders <img>, else scenario-card__visual--empty placeholder with text 'Image'",
  sameComponentForAllBrands: true,
  activeRegistryOnlyLogic: false,
  expansionBrandSpecificBranch: false,
  externalDisplayStatusFilter: "api/brand-library.js normalizeBrandExplorerPresentationRecords skips Do Not Display / Internal Only before blocks[]",
  goldDetailNote:
    "brand-explorer-gold-detail.js does not render overview.scenario cards directly; atelier-from-api.js is the owner-facing scenario grid",
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function targetFields(target) {
  return {
    recordId: target?.recordId || target?.resolution?.resolvedRecordId || "",
    name: target?.name || target?.resolution?.resolvedBrandName || "",
    slug: target?.slug || target?.resolution?.resolvedSlug || "",
    resolutionSource: target?.resolution?.resolutionSource || null,
    error: target?.resolution?.error || null,
  };
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function listPresentationRowsRaw(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, PRESENTATION_TABLE)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function firstAttachmentUrl(fields) {
  const att = fields?.Image;
  if (!Array.isArray(att) || !att[0]?.url) return null;
  return nz(att[0].url);
}

function normalizeAirtableRow(rec, slotKey) {
  const f = rec.fields || {};
  const sk = nz(f["Slot Key"]);
  if (sk !== slotKey) return null;
  const externalDisplayStatus = nz(f["External Display Status"]);
  const activeRaw = f.Active;
  const inactive =
    activeRaw === false ||
    String(activeRaw).toLowerCase() === "no" ||
    String(activeRaw).toLowerCase() === "false" ||
    activeRaw === 0;
  return {
    recordId: rec.id,
    slotKey: sk,
    title: nz(f.Title),
    body: nz(f.Body),
    subtitle: nz(f.Subtitle),
    hasImageAttachment: Array.isArray(f.Image) && f.Image.length > 0,
    imageUrl: firstAttachmentUrl(f),
    sourceUrl: nz(f["Source URL"]) || null,
    sourcePageUrl: nz(f["Source Page URL"]) || null,
    externalDisplayStatus,
    active: !inactive,
    sortOrder: f["Sort Order"] ?? null,
    tags: f.Tags || f["Case Summary Tags"] || null,
    badges: f.Badges || null,
    quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(externalDisplayStatus),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    filteredFromApi:
      inactive ||
      HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(externalDisplayStatus),
  };
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

function apiBlockForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.find((b) => nz(b?.slotKey) === slotKey) || null;
}

function classifyImageGovernance({ row, apiBlock, registryAssets, brandConfig, brandTarget }) {
  const mergedRow = {
    recordId: row?.recordId || apiBlock?.recordId,
    slotKey: row?.slotKey || apiBlock?.slotKey,
    title: row?.title || apiBlock?.title,
    body: row?.body || apiBlock?.body,
    imageUrl: apiBlock?.imageUrl || row?.imageUrl || null,
    summaryUrl: apiBlock?.summaryUrl || row?.sourcePageUrl || null,
  };
  const assessment = brandConfig
    ? assessPresentationRowImageGovernance(mergedRow, brandConfig, registryAssets)
    : null;
  const registryMatch = findRegistryAssetForPresentationRow(registryAssets, mergedRow);
  const registryClass = registryMatch
    ? classifyRegistryAsset({
        assetStatus: registryMatch.assetStatus,
        explorerUsePermission: registryMatch.explorerUsePermission,
        usageReviewStatus: registryMatch.usageReviewStatus,
        assetName: registryMatch.assetName,
        doNotUseReason: registryMatch.doNotUseReason,
      })
    : null;

  const hasImage = Boolean(nz(mergedRow.imageUrl));
  let displayClassification = "text_only_eligible";
  if (!hasImage) displayClassification = "missing_image";
  else if (registryClass === "Do Not Use") displayClassification = "do_not_use";
  else if (registryClass === "Approved" || assessment?.registryApproved)
    displayClassification = "approved_image";
  else if (assessment?.pendingImageReview) displayClassification = "pending_image_review";
  else if (!registryMatch) displayClassification = "pending_image_review";
  else if (registryClass === "source_reference_only") displayClassification = "source_reference_only";

  const isExpansion = nz(brandTarget?.resolution?.resolutionSource) === "expansion_backlog";
  const activeProfileBlocker =
    isExpansion &&
    hasImage &&
    !assessment?.registryApproved &&
    !/^materials\.gallery/.test(mergedRow.slotKey);

  return {
    assessment,
    registryMatch: registryMatch
      ? {
          id: registryMatch.id,
          assetName: registryMatch.assetName,
          assetStatus: registryMatch.assetStatus,
          explorerUsePermission: registryMatch.explorerUsePermission,
          usageReviewStatus: registryMatch.usageReviewStatus,
          sourceUrl: registryMatch.sourceUrl || null,
          sourcePageUrl: registryMatch.sourcePageUrl || null,
          recommendedExplorerSlot: registryMatch.recommendedExplorerSlot || null,
          approved: isRegistryAssetApprovedForExplorer(registryMatch),
          classification: registryClass,
        }
      : null,
    displayClassification,
    activeProfileBlocker,
    expansionBrandGovernance: isExpansion,
  };
}

function frontendRenderingProjection(apiBlock) {
  const imgUrl = nz(apiBlock?.imageUrl);
  const hasImage = Boolean(imgUrl);
  return {
    component: "scenario-card scenario-card--visual",
    section: "Where This Brand Creates the Most Value",
    rendersImage: hasImage,
    rendersBlankPlaceholder: !hasImage,
    placeholderClass: hasImage ? null : "scenario-card__visual--empty",
    placeholderText: hasImage ? null : "Image",
    titleRendered: Boolean(nz(apiBlock?.title)),
    bodyRendered: Boolean(nz(apiBlock?.body)),
    htmlPattern: hasImage
      ? '<div class="scenario-card__visual"><img src="..." /></div>'
      : '<div class="scenario-card__visual scenario-card__visual--empty">Image</div>',
  };
}

function diffFields(left, right, fields) {
  const diffs = [];
  for (const key of fields) {
    const l = left?.[key] ?? null;
    const r = right?.[key] ?? null;
    const same = JSON.stringify(l) === JSON.stringify(r);
    diffs.push({ field: key, left: l, right: r, same });
  }
  return diffs;
}

function inferRootCause({ airtableComparison, apiComparison, frontendComparison, governanceComparison }) {
  const causes = [];
  if (!airtableComparison.bothRowsExist) {
    causes.push({ layer: "data", reason: "One brand missing overview.scenario.1 presentation row" });
  }
  if (airtableComparison.diffs.some((d) => d.field === "imageUrl" && !d.same)) {
    causes.push({ layer: "data", reason: "Image attachment / imageUrl differs between brands" });
  }
  if (airtableComparison.diffs.some((d) => d.field === "externalDisplayStatus" && !d.same)) {
    causes.push({ layer: "data", reason: "External Display Status differs — API filter may exclude one brand" });
  }
  if (apiComparison.leftInBlocks !== apiComparison.rightInBlocks) {
    causes.push({ layer: "api", reason: "Slot present in API blocks for one brand only" });
  }
  if (
    apiComparison.leftBlock?.imageUrl !== apiComparison.rightBlock?.imageUrl &&
    (apiComparison.leftBlock?.imageUrl || apiComparison.rightBlock?.imageUrl)
  ) {
    causes.push({ layer: "api", reason: "API imageUrl differs — mirrors Airtable attachment state" });
  }
  if (
    frontendComparison.left.rendersBlankPlaceholder !==
    frontendComparison.right.rendersBlankPlaceholder
  ) {
    if (frontendComparison.left.rendersBlankPlaceholder || frontendComparison.right.rendersBlankPlaceholder) {
      causes.push({
        layer: "data",
        reason:
          "Frontend uses identical scenario-card logic; blank placeholder appears when imageUrl is empty — not a brand-specific render branch",
      });
    }
  }
  if (
    governanceComparison.left.displayClassification !==
    governanceComparison.right.displayClassification
  ) {
    causes.push({ layer: "image_governance", reason: "Image governance classification differs" });
  }
  if (
    governanceComparison.left.expansionBrandGovernance !==
    governanceComparison.right.expansionBrandGovernance
  ) {
    causes.push({
      layer: "image_governance",
      reason: "Radisson Individuals is expansion_backlog — stricter image governance applies; Radisson by Choice uses active_registry path",
    });
  }
  if (
    governanceComparison.left.registryMatch?.id !== governanceComparison.right.registryMatch?.id &&
    (!governanceComparison.left.registryMatch || !governanceComparison.right.registryMatch)
  ) {
    causes.push({ layer: "registry_mapping", reason: "Brand Asset Registry linkage differs or missing on one side" });
  }
  return causes;
}

function buildRecommendation({ rootCause, left, right, governanceComparison, frontendComparison }) {
  const recs = [];
  const rightNoImage = !nz(right?.apiBlock?.imageUrl);
  const leftHasImage = Boolean(nz(left?.apiBlock?.imageUrl));

  if (rightNoImage && leftHasImage) {
    recs.push({
      priority: "P1",
      action: "restore_or_assign_scenario_image",
      detail:
        "Radisson Individuals overview.scenario.1 needs an approved scenario image (registry row + presentation attachment) to match Radisson display parity — or explicitly accept text-only card until founder approves image.",
      alternative: "text_only_until_approval",
    });
  }
  if (right?.airtableRow?.quarantined) {
    recs.push({
      priority: "P2",
      action: "review_quarantine_status",
      detail:
        "If overview.scenario.1 is quarantined (Do Not Display), it will not appear in API blocks — verify External Display Status is intentional.",
    });
  }
  if (
    frontendComparison.left.rendersBlankPlaceholder !==
    frontendComparison.right.rendersBlankPlaceholder
  ) {
    recs.push({
      priority: "P3",
      action: "optional_ui_text_only_mode",
      detail:
        "Optional frontend patch: hide scenario-card__visual--empty shell when imageUrl missing and card is text-only eligible — applies to both brands uniformly (not Radisson-specific logic).",
      scope: "frontend",
    });
  }
  if (governanceComparison.right.expansionBrandGovernance) {
    recs.push({
      priority: "P2",
      action: "registry_then_approval",
      detail:
        "Create/link Brand Asset Registry asset for overview.scenario.1, then founder-approve before v31E materialization — expansion brands require approved registry for active-profile evidence.",
    });
  }
  if (recs.length === 0) {
    recs.push({
      priority: "info",
      action: "no_action",
      detail: "No display parity gap detected for this slot — differences are intentional or absent.",
    });
  }
  return recs;
}

export function v31hAuditExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-scenario-display-parity-audit.js")
  );
}

export async function buildBrandExplorerScenarioDisplayParityAuditReport({
  leftArg = DEFAULT_LEFT,
  rightArg = DEFAULT_RIGHT,
  slotKey = DEFAULT_SLOT,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const ctx = await getBrandTargetResolverContext({ refresh: true });
  const leftTarget = await resolveBrandTarget(leftArg, ctx);
  const rightTarget = await resolveBrandTarget(rightArg, ctx);
  const left = targetFields(leftTarget);
  const right = targetFields(rightTarget);
  if (left.error) throw new Error(`Left brand resolve failed: ${left.error}`);
  if (right.error) throw new Error(`Right brand resolve failed: ${right.error}`);
  if (!left.recordId) throw new Error(`Left brand resolve failed: no recordId for ${leftArg}`);
  if (!right.recordId) throw new Error(`Right brand resolve failed: no recordId for ${rightArg}`);

  const leftBasics = await fetchBrandBasics(left.recordId);
  const rightBasics = await fetchBrandBasics(right.recordId);
  const companyValidatedLeft = companyValidatedSnapshot(leftBasics);
  const companyValidatedRight = companyValidatedSnapshot(rightBasics);

  const [leftPresRaw, rightPresRaw] = await Promise.all([
    listPresentationRowsRaw(baseId, apiKey, left.recordId, left.name),
    listPresentationRowsRaw(baseId, apiKey, right.recordId, right.name),
  ]);

  const leftAirtableRow =
    leftPresRaw.map((r) => normalizeAirtableRow(r, slotKey)).find(Boolean) || null;
  const rightAirtableRow =
    rightPresRaw.map((r) => normalizeAirtableRow(r, slotKey)).find(Boolean) || null;

  const [leftApi, rightApi, leftRegistry, rightRegistry] = await Promise.all([
    fetchBrandApiShape(left.recordId),
    fetchBrandApiShape(right.recordId),
    listRegistryAssetsForBrand(left.recordId).catch(() => []),
    listRegistryAssetsForBrand(right.recordId).catch(() => []),
  ]);

  const leftApiBlock = apiBlockForSlot(leftApi, slotKey);
  const rightApiBlock = apiBlockForSlot(rightApi, slotKey);

  const leftBrandConfig =
    getDiscoveryBrandConfig(left.slug) || { name: left.name, allowedSiblingMentions: [] };
  const rightBrandConfig =
    getDiscoveryBrandConfig(right.slug) || { name: right.name, allowedSiblingMentions: [] };

  const compareFields = [
    "recordId",
    "slotKey",
    "title",
    "body",
    "subtitle",
    "hasImageAttachment",
    "imageUrl",
    "sourceUrl",
    "sourcePageUrl",
    "externalDisplayStatus",
    "active",
    "sortOrder",
    "quarantined",
    "visibleInExplorer",
    "filteredFromApi",
  ];

  const airtableComparison = {
    left: leftAirtableRow,
    right: rightAirtableRow,
    bothRowsExist: Boolean(leftAirtableRow && rightAirtableRow),
    diffs: diffFields(leftAirtableRow, rightAirtableRow, compareFields),
    intentionalDifferences: ["title", "body", "recordId", "sortOrder"],
  };

  const apiComparison = {
    leftBlock: leftApiBlock,
    rightBlock: rightApiBlock,
    leftInBlocks: Boolean(leftApiBlock),
    rightInBlocks: Boolean(rightApiBlock),
    leftTotalBlocks: leftApi?.brandExplorer?.blocks?.length ?? 0,
    rightTotalBlocks: rightApi?.brandExplorer?.blocks?.length ?? 0,
    section: "Where This Brand Creates the Most Value (Overview tab scenario grid)",
    leftExposesImageUrl: Boolean(nz(leftApiBlock?.imageUrl)),
    rightExposesImageUrl: Boolean(nz(rightApiBlock?.imageUrl)),
    leftExposesTitle: Boolean(nz(leftApiBlock?.title)),
    rightExposesTitle: Boolean(nz(rightApiBlock?.title)),
    leftExposesBody: Boolean(nz(leftApiBlock?.body)),
    rightExposesBody: Boolean(nz(rightApiBlock?.body)),
    leftFilteredReason: leftAirtableRow?.filteredFromApi
      ? leftAirtableRow.quarantined
        ? "Do Not Display / Internal Only"
        : "inactive"
      : null,
    rightFilteredReason: rightAirtableRow?.filteredFromApi
      ? rightAirtableRow.quarantined
        ? "Do Not Display / Internal Only"
        : "inactive"
      : null,
    apiTransformNotes: [
      "brand-library.js maps Image attachment → block.imageUrl",
      "Do Not Display / Internal Only rows excluded from blocks[]",
      "Inactive rows excluded from blocks[]",
      "No brand-specific scenario transform — same normalizeBrandExplorerPresentationRecords for all brands",
    ],
  };

  const leftFrontend = frontendRenderingProjection(leftApiBlock);
  const rightFrontend = frontendRenderingProjection(rightApiBlock);

  const frontendComparison = {
    left: leftFrontend,
    right: rightFrontend,
    sameComponent: true,
    parityGap:
      leftFrontend.rendersBlankPlaceholder !== rightFrontend.rendersBlankPlaceholder ||
      leftFrontend.rendersImage !== rightFrontend.rendersImage,
    staticAnalysis: FRONTEND_SCENARIO_RENDERING,
  };

  const leftGovernance = classifyImageGovernance({
    row: leftAirtableRow,
    apiBlock: leftApiBlock,
    registryAssets: leftRegistry,
    brandConfig: leftBrandConfig,
    brandTarget: leftTarget,
  });
  const rightGovernance = classifyImageGovernance({
    row: rightAirtableRow,
    apiBlock: rightApiBlock,
    registryAssets: rightRegistry,
    brandConfig: rightBrandConfig,
    brandTarget: rightTarget,
  });

  const governanceComparison = {
    left: leftGovernance,
    right: rightGovernance,
    registrySlotMatches: {
      left: leftRegistry.filter((a) => nz(a.recommendedExplorerSlot) === slotKey),
      right: rightRegistry.filter((a) => nz(a.recommendedExplorerSlot) === slotKey),
    },
  };

  const internalLanguage = {
    left: leftAirtableRow
      ? findInternalLanguageInRow({ fields: { Title: leftAirtableRow.title, Body: leftAirtableRow.body } })
      : [],
    right: rightAirtableRow
      ? findInternalLanguageInRow({
          fields: { Title: rightAirtableRow.title, Body: rightAirtableRow.body },
        })
      : [],
  };

  const rootCause = inferRootCause({
    airtableComparison,
    apiComparison,
    frontendComparison,
    governanceComparison,
  });

  const recommendation = buildRecommendation({
    rootCause,
    left: { airtableRow: leftAirtableRow, apiBlock: leftApiBlock },
    right: { airtableRow: rightAirtableRow, apiBlock: rightApiBlock },
    governanceComparison,
    frontendComparison,
  });

  const finalQaRight = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: right.slug,
  }).catch((err) => ({ error: err.message }));
  const completeRight = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: right.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));

  const report = {
    auditVersion: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    v31hAuditExists: v31hAuditExists(),
    slotKey,
    leftBrand: {
      input: leftArg,
      slug: left.slug,
      recordId: left.recordId,
      name: left.name,
      resolutionSource: left.resolutionSource,
    },
    rightBrand: {
      input: rightArg,
      slug: right.slug,
      recordId: right.recordId,
      name: right.name,
      resolutionSource: right.resolutionSource,
    },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    companyValidatedLeft,
    companyValidatedRight,
    companyValidatedUntouched: true,
    airtableModified: false,
    imagesApproved: false,
    airtableRowComparison: airtableComparison,
    apiComparison,
    frontendRenderingComparison: frontendComparison,
    imageGovernanceComparison: governanceComparison,
    internalLanguageAudit: internalLanguage,
    rootCauseMap: rootCause,
    recommendedFix: recommendation,
    rightBrandReadiness: {
      finalQa: finalQaRight?.brandReports?.[0]?.scores || null,
      completeBuild: (completeRight?.brandReports || []).find((b) => b.slug === right.slug) || null,
    },
    displayParitySummary: {
      shouldDisplayIdentically:
        Boolean(leftApiBlock?.imageUrl) === Boolean(rightApiBlock?.imageUrl) &&
        Boolean(leftApiBlock) === Boolean(rightApiBlock),
      differenceIsIntentional:
        nz(leftAirtableRow?.title) !== nz(rightAirtableRow?.title) ||
        nz(leftAirtableRow?.body) !== nz(rightAirtableRow?.body),
      unexplainedDisplayGap:
        leftFrontend.rendersImage !== rightFrontend.rendersImage &&
        leftAirtableRow?.active === rightAirtableRow?.active &&
        leftAirtableRow?.quarantined === rightAirtableRow?.quarantined,
    },
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Scenario Display Parity Audit v31H`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Slot: **${report.slotKey}**`,
    `- Left: **${report.leftBrand.name}** (\`${report.leftBrand.slug}\`)`,
    `- Right: **${report.rightBrand.name}** (\`${report.rightBrand.slug}\`)`,
    `- v31H exists: **${report.v31hAuditExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}** (audit only)`,
    `- Company Validated untouched: **yes**`,
    `- Airtable modified: **no**`,
    "",
    "## 1. Airtable row comparison",
    "",
  ];

  for (const side of ["left", "right"]) {
    const row = report.airtableRowComparison[side];
    const brand = report[side === "left" ? "leftBrand" : "rightBrand"];
    lines.push(`### ${brand.name}`);
    if (!row) {
      lines.push("- Row not found");
      continue;
    }
    lines.push(`- Record: \`${row.recordId}\``);
    lines.push(`- Title: ${row.title}`);
    lines.push(`- Image: ${row.hasImageAttachment ? "attached" : "none"}`);
    lines.push(`- External Display Status: ${row.externalDisplayStatus || "—"}`);
    lines.push(`- Quarantined: ${row.quarantined}`);
    lines.push(`- Registry (slot): ${report.imageGovernanceComparison.registrySlotMatches[side].length} asset(s)`);
    lines.push(
      `- Governance: **${report.imageGovernanceComparison[side].displayClassification}**`
    );
  }

  lines.push("", "## 2. API comparison", "");
  lines.push(
    `- Left in blocks: **${report.apiComparison.leftInBlocks}** (imageUrl: ${report.apiComparison.leftExposesImageUrl})`
  );
  lines.push(
    `- Right in blocks: **${report.apiComparison.rightInBlocks}** (imageUrl: ${report.apiComparison.rightExposesImageUrl})`
  );

  lines.push("", "## 3. Frontend rendering", "");
  lines.push(
    `- Left renders image: **${report.frontendRenderingComparison.left.rendersImage}**`
  );
  lines.push(
    `- Right renders image: **${report.frontendRenderingComparison.right.rendersImage}**`
  );
  lines.push(
    `- Right blank placeholder: **${report.frontendRenderingComparison.right.rendersBlankPlaceholder}**`
  );
  lines.push(`- Same component: **yes** (atelier scenario-card--visual)`);

  lines.push("", "## 4. Root cause", "");
  for (const c of report.rootCauseMap) {
    lines.push(`- **${c.layer}**: ${c.reason}`);
  }

  lines.push("", "## 5. Recommended fix", "");
  for (const r of report.recommendedFix) {
    lines.push(`- [${r.priority}] ${r.action}: ${r.detail}`);
  }

  return lines.join("\n");
}
