/**
 * Brand Explorer WoodSpring Source Stewardship + Profile Build Readiness v33A.
 *
 * WoodSpring-only: source stewardship, presentation/registry audit, registry candidate
 * creates (metadata only), and required-section build plan. No presentation copy,
 * image field, or image approval changes.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-source-registry-readiness-writer-v33A.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerSources, patchPartnerSource } from "./airtable-source.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import {
  buildRegistryDedupeKey,
  listRegistryAssetsForBrand,
  MAP_BRAND_ASSET,
  validateRegistryWritePayload,
  BRAND_ASSET_REGISTRY_TABLE,
} from "./brand-asset-registry-workflow.js";
import {
  ASSET_STATUS,
  ASSET_TYPE,
  SOURCE_BASIS,
} from "./brand-asset-pr-package-governance.js";
import {
  auditCurrentImageUsage,
  discoverImageCandidates,
} from "./brand-explorer-brand-asset-registry-discovery-writer.js";
import {
  detectWrongBrandSignageRisk,
  getDiscoveryBrandConfig,
  normalizeUrlKey,
} from "./brand-explorer-brand-asset-image-governance.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  classifyMomentumSourceType,
  followsTributeMomentumRules,
  isMomentumInappropriatePropertyListing,
  momentumEvidenceSourceRank,
} from "./brand-explorer-momentum-link-label.js";
import { MAP_VISUAL_SLOT, VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import {
  classifySourceUrl,
  isBlockedSourceUrl,
} from "./brand-explorer-choice-extended-stay-source-capture-writer.js";
import {
  isDoNotUseRecord,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { REGISTRY_COMPLETENESS_FIELDS } from "./brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.js";
import { TRIBUTE_RECORD_ID } from "./tribute-portfolio-brand-package.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import {
  resolveBrandTarget,
  getBrandTargetResolverContext,
} from "./brand-explorer-brand-target-resolver.js";
import {
  auditEverhomeSourceRecord,
  shouldStewardApproveSource,
} from "./brand-explorer-everhome-source-registry-normalization-writer.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33A";
export const STAGING_RUN_ID = "v33A-woodspring-source-registry-readiness";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-source-registry-readiness-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-source-registry-readiness-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-source-registry-readiness-writer-v33A.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33A-woodspring-source-registry-readiness";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_IMAGE_APPROVAL = "--confirm-no-image-approval";
export const APPLY_FLAG_NO_PRESENTATION_COPY = "--confirm-no-presentation-copy-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export const TARGET_BRAND = Object.freeze({
  slug: "woodspring-suites",
  recordId: "recsOd51NzRPYsMko",
  name: "WoodSpring Suites",
});

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "everhome-suites",
  "suburban-studios",
]);

const EVERHOME_RECORD_ID = "recqkkrsevi4r9ibj";

const WOODSPRING_VISUAL_SLOT_RE =
  /^(footprint\.openings|materials\.gallery\.\d|overview\.hero|overview\.scenario\.\d|valueOwners\.scenario\.\d)$/;

const INTERNAL_LANGUAGE_PATTERNS = [
  { id: "fdd", re: /\bfdd\b/i },
  { id: "item_19", re: /\bitem\s*19\b/i },
  { id: "franchise_disclosure", re: /\bfranchise disclosure\b/i },
  { id: "confirm_fees", re: /\bconfirm fees\b/i },
  { id: "confirm_flag", re: /\bconfirm flag\b/i },
  { id: "performance_representation", re: /\bperformance representation\b/i },
  { id: "active_property_page", re: /\bactive property page\b/i },
  { id: "consumer_path", re: /\bconsumer path\b/i },
  { id: "census", re: /\bcensus\b/i },
  { id: "metadata", re: /\bmetadata\b/i },
  { id: "source_data", re: /\bsource data\b/i },
  { id: "internal", re: /\binternal\b/i },
  { id: "extraction", re: /\bextraction\b/i },
  { id: "source_capture", re: /\bsource[- ]capture\b/i },
  { id: "booking_path", re: /\bbooking path\b/i },
];

const PRESENTATION_SECTIONS = [
  { key: "overview", match: (s) => /^overview\./.test(s) && !/^overview\.(scenario|featured_application)/.test(s) },
  { key: "overview.featured_application", match: (s) => s === "overview.featured_application" },
  { key: "overview.scenario", match: (s) => /^overview\.scenario\./.test(s) },
  { key: "valueOwners.scenario", match: (s) => /^valueOwners\.scenario\./.test(s) },
  { key: "footprint.openings", match: (s) => s === "footprint.openings" },
  { key: "footprint.momentum", match: (s) => s === "footprint.momentum" || /^footprint\.momentum\./.test(s) },
  { key: "portfolio_mix", match: (s) => s === "footprint.portfolio_mix" },
  { key: "portfolio_context", match: (s) => s === "portfolio_context" || s === "footprint.portfolio_context" },
  { key: "standard_detail", match: (s) => /^standards\./.test(s) },
  { key: "demand_scenario", match: (s) => /^demand\./.test(s) || s === "demand_scenario" },
  { key: "loyalty", match: (s) => /^loyalty\./.test(s) },
  { key: "geographic_footprint", match: (s) => /^footprint\.(geo|growth)/.test(s) },
  { key: "materials.gallery", match: (s) => /^materials\.gallery\./.test(s) },
];

const REQUIRED_SECTION_PLAN = [
  { label: "Openings / Examples / Properties", keys: ["footprint.openings"] },
  { label: "Recent Momentum", keys: ["footprint.momentum"] },
  { label: "Portfolio Mix", keys: ["portfolio_mix"] },
  { label: "Portfolio Context", keys: ["portfolio_context"] },
  { label: "Standard Detail / Where Available", keys: ["standard_detail"] },
  { label: "Demand Scenario View", keys: ["demand_scenario"] },
  { label: "Loyalty Program", keys: ["loyalty"] },
  { label: "Geographic Footprint", keys: ["geographic_footprint"] },
  { label: "Overview / Featured Application", keys: ["overview.featured_application"] },
  { label: "Gallery / visual completeness", keys: ["materials.gallery", "overview.scenario"] },
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-choice-extended-stay-batch-readiness-audit.json",
  "reports/brand-explorer-choice-extended-stay-source-capture-writer.json",
  "reports/brand-explorer-everhome-final-fact-formatting-cleanup-writer.json",
  "reports/brand-explorer-everhome-final-gate-backfill-writer.json",
  "reports/brand-explorer-everhome-existing-image-approval-recognition-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "docs/brand-explorer-presentation-slots.md",
  "live WoodSpring Brand Setup / Source Library / Presentation / Registry / Facts / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-source-registry-readiness-writer.js",
  "scripts/brand-explorer-woodspring-source-registry-readiness-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v33aWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-woodspring-source-registry-readiness-writer.js")
  );
}

function validateSourcePatch(fields) {
  const errors = [];
  if (!VAL_PARTNER_SOURCE_SELECTS.approvedForExplorerUse.includes(fields[MAP_PARTNER_SOURCE.approvedForExplorerUse])) {
    errors.push("invalid approvedForExplorerUse");
  }
  if (!VAL_PARTNER_SOURCE_SELECTS.status.includes(fields[MAP_PARTNER_SOURCE.status])) {
    errors.push("invalid status");
  }
  if (fields[MAP_PARTNER_SOURCE.approvedForExplorerUse] === "Yes") {
    const notes = fields[MAP_PARTNER_SOURCE.permissionVisibilityNotes] || "";
    if (/\bcompany validated\b/i.test(notes)) errors.push("implies_company_validation");
    if (/implies?\s+company\s+(review|validation)/i.test(notes)) {
      errors.push("implies_company_validation");
    }
  }
  return errors;
}

export function buildWoodspringSourceStewardshipPatch(audit) {
  const today = new Date().toISOString().slice(0, 10);
  const permissionNote =
    audit.classification === "trade"
      ? "Public Source / Trade Source — AI-assisted Explorer evidence. Not company validation."
      : "AI-Assisted / Company Materials — AI-assisted Explorer evidence. Not company validation.";

  const evidenceNote = [
    `v33A evidenceUseCase: ${audit.evidenceUseCases.join(", ") || "General"}`,
    `momentumAppropriate: ${audit.momentumAppropriate ? "yes" : "no"}`,
    `openingsAppropriate: ${audit.openingsAppropriate ? "yes" : "no"}`,
    `stewardedBy: v33A`,
  ].join(" | ");

  return {
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "Yes",
    [MAP_PARTNER_SOURCE.status]: "Approved",
    [MAP_PARTNER_SOURCE.sourceQuality]: audit.classification === "trade" ? "Medium" : "High",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.verifiedSource]: "Yes",
    [MAP_PARTNER_SOURCE.permissionVisibilityNotes]: permissionNote,
    [MAP_PARTNER_SOURCE.lastReviewed]: today,
    [MAP_PARTNER_SOURCE.notes]: evidenceNote,
  };
}

function listWoodspringVisualRows(brand) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => WOODSPRING_VISUAL_SLOT_RE.test(nz(b?.slotKey)));
}

function inferAssetTypeForSlot(slotKey) {
  if (slotKey === "overview.hero") return ASSET_TYPE.HERO;
  if (slotKey === "footprint.openings") return ASSET_TYPE.PR_IMAGE;
  if (/materials\.gallery/.test(slotKey)) return ASSET_TYPE.EXTERIOR;
  if (/overview\.scenario|valueOwners\.scenario/.test(slotKey)) return ASSET_TYPE.LIFESTYLE;
  return ASSET_TYPE.EXTERIOR;
}

function inferExplorerSection(slotKey, assetType) {
  if (slotKey === "footprint.openings") return VISUAL_SLOT.RECENT_OPENINGS;
  if (/materials\.gallery/.test(slotKey)) return VISUAL_SLOT.GALLERY;
  if (slotKey === "overview.hero") return VISUAL_SLOT.HERO;
  if (/valueOwners\.scenario/.test(slotKey)) return VISUAL_SLOT.VALUE_DRIVER;
  if (/overview\.scenario/.test(slotKey)) return "Value Scenarios";
  if (assetType === ASSET_TYPE.PRESS_LINK) return VISUAL_SLOT.PR_LINK;
  return "Brand Explorer Presentation";
}

function inferSourceBasis(url, brandConfig) {
  const u = nz(url).toLowerCase();
  if (!u) return SOURCE_BASIS.COMPANY_MATERIALS;
  if (brandConfig?.officialDomains?.some((d) => u.includes(d))) return SOURCE_BASIS.COMPANY_MATERIALS;
  return SOURCE_BASIS.THIRD_PARTY;
}

function extractUrlFromText(text) {
  const m = nz(text).match(/https?:\/\/[^\s<>"')]+/i);
  return m ? m[0].replace(/[.,;)]+$/, "") : "";
}

function parsePropertyFromTitle(title) {
  const t = nz(title);
  if (!t) return "";
  return t.replace(/^WoodSpring Suites\s*[-–—]\s*/i, "").trim() || t;
}

export function buildWoodspringRegistryStagedAsset({ row, brandConfig, stagingRunId, wrongBrandRisk }) {
  const slotKey = nz(row.slotKey);
  const rawImageUrl = nz(row.imageUrl);
  const imageUrl = rawImageUrl && !isTemporaryAirtableUrl(rawImageUrl) ? rawImageUrl : null;
  const sourcePageUrl =
    nz(row.summaryUrl) || extractUrlFromText(row.body) || brandConfig.consumerUrl;
  const durablePageUrl =
    sourcePageUrl && !isTemporaryAirtableUrl(sourcePageUrl) ? sourcePageUrl : brandConfig.consumerUrl;
  const assetType = inferAssetTypeForSlot(slotKey);
  const propertyName = parsePropertyFromTitle(row.title);

  if (wrongBrandRisk && rawImageUrl) {
    return {
      assetName: `DO NOT USE — WoodSpring — ${propertyName || slotKey} — Wrong-Brand Image`,
      assetType,
      assetStatus: ASSET_STATUS.DO_NOT_USE,
      sourceBasis: inferSourceBasis(rawImageUrl, brandConfig),
      sourceUrl: imageUrl,
      sourcePageUrl: durablePageUrl,
      recommendedExplorerSlot: slotKey,
      isPrimaryCandidate: false,
      explorerUsePermission: "Do Not Use",
      usageReviewStatus: "Blocked",
      sourceNotes: `v33A guard — presentation row ${row.recordId} (${slotKey})`,
      reviewNotes: wrongBrandRisk.reason,
      doNotUseReason: `${wrongBrandRisk.reason} (${wrongBrandRisk.markerId})`,
      stagingRunId,
      explorerSection: inferExplorerSection(slotKey, assetType),
      slotPurpose: `WoodSpring ${slotKey} visual`,
      relatedPropertyName: propertyName,
      validationStatus: "Do Not Use",
      validationNotes: "v33A — wrong-brand risk flagged; founder review required.",
      brandConfirmed: "Unknown",
      propertyConfirmed: "Unknown",
      calaRelevant: "No",
    };
  }

  const tempUrl = rawImageUrl && isTemporaryAirtableUrl(rawImageUrl);
  return {
    assetName: `WoodSpring Suites — ${propertyName || slotKey} — ${assetType}`,
    assetType,
    assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    sourceBasis: inferSourceBasis(durablePageUrl, brandConfig),
    sourceUrl: imageUrl,
    sourcePageUrl: durablePageUrl,
    recommendedExplorerSlot: slotKey,
    isPrimaryCandidate: slotKey === "footprint.openings",
    explorerUsePermission: "Candidate Only",
    usageReviewStatus: "Pending Review",
    sourceNotes: `v33A linked — presentation row ${row.recordId}. Pending founder image review.`,
    reviewNotes: tempUrl
      ? "Pending Image Review — temporary Airtable attachment on presentation row; durable source required before approval."
      : "Pending Image Review — v33A metadata-only registry link; not approved for Explorer.",
    stagingRunId,
    explorerSection: inferExplorerSection(slotKey, assetType),
    slotPurpose: `WoodSpring ${slotKey} visual candidate`,
    relatedPropertyName: propertyName,
    validationStatus: "Needs Usage Review",
    validationNotes: "v33A registry candidate — no automatic image approval.",
    brandConfirmed: "Yes",
    propertyConfirmed: imageUrl ? "Unknown" : "Unknown",
    calaRelevant: "No",
  };
}

function mapStagedToRegistryFields(staged, brandRecordId, parentCompany) {
  const fields = {
    [MAP_BRAND_ASSET.assetName]: staged.assetName,
    [MAP_BRAND_ASSET.brand]: [brandRecordId],
    [MAP_BRAND_ASSET.brandRecordId]: brandRecordId,
    [MAP_BRAND_ASSET.parentCompany]: parentCompany,
    [MAP_BRAND_ASSET.assetType]: staged.assetType,
    [MAP_BRAND_ASSET.assetStatus]: staged.assetStatus,
    [MAP_BRAND_ASSET.sourceBasis]: staged.sourceBasis,
    [MAP_BRAND_ASSET.usageReviewStatus]: staged.usageReviewStatus,
    [MAP_BRAND_ASSET.explorerUsePermission]: staged.explorerUsePermission,
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: staged.recommendedExplorerSlot,
    [MAP_BRAND_ASSET.isPrimaryCandidate]: Boolean(staged.isPrimaryCandidate),
    [MAP_BRAND_ASSET.sourceNotes]: staged.sourceNotes,
    [MAP_BRAND_ASSET.reviewNotes]: staged.reviewNotes,
    [MAP_BRAND_ASSET.stagingRunId]: staged.stagingRunId,
    [MAP_BRAND_ASSET.companyValidated]: false,
    [MAP_VISUAL_SLOT.explorerSection]: staged.explorerSection,
    [MAP_VISUAL_SLOT.slotPurpose]: staged.slotPurpose,
    [MAP_VISUAL_SLOT.relatedPropertyName]: staged.relatedPropertyName,
    [MAP_VISUAL_SLOT.validationStatus]: staged.validationStatus,
    [MAP_VISUAL_SLOT.validationNotes]: staged.validationNotes,
    [MAP_VISUAL_SLOT.brandConfirmed]: staged.brandConfirmed,
    [MAP_VISUAL_SLOT.propertyConfirmed]: staged.propertyConfirmed,
    [MAP_VISUAL_SLOT.calaRelevant]: staged.calaRelevant,
  };
  if (staged.sourceUrl && !isTemporaryAirtableUrl(staged.sourceUrl)) {
    fields[MAP_BRAND_ASSET.sourceUrl] = staged.sourceUrl;
  }
  if (staged.sourcePageUrl) fields[MAP_BRAND_ASSET.sourcePageUrl] = staged.sourcePageUrl;
  if (staged.doNotUseReason) fields[MAP_BRAND_ASSET.doNotUseReason] = staged.doNotUseReason;
  return fields;
}

function auditPresentationSection(blocks) {
  const inventory = {};
  for (const def of PRESENTATION_SECTIONS) {
    const rows = blocks.filter((b) => def.match(nz(b.slotKey)));
    const thin = rows.filter((b) => wordCount(`${b.title} ${b.body}`) < 12).length;
    const hidden = rows.filter((b) => b.visibleInExplorer === false).length;
    const withImages = rows.filter((b) => nz(b.imageUrl)).length;
    const tempUrls = rows.filter((b) => isTemporaryAirtableUrl(b.imageUrl)).length;
    inventory[def.key] = {
      rowsPresent: rows.length,
      rowsMissing: 0,
      rowsThin: thin,
      rowsHidden: hidden,
      rowsWithImages: withImages,
      rowsWithLoadingImages: withImages,
      rowsWithTempUrls: tempUrls,
      rowIds: rows.map((r) => r.recordId),
    };
  }
  return inventory;
}

function scanInternalLanguage(blocks) {
  const findings = [];
  for (const block of blocks) {
    const text = `${nz(block.title)}\n${nz(block.body)}`;
    for (const pat of INTERNAL_LANGUAGE_PATTERNS) {
      const match = text.match(pat.re);
      if (match) {
        findings.push({
          recordId: block.recordId,
          slotKey: block.slotKey,
          patternId: pat.id,
          phrase: match[0],
          cleanupRecommendation: "v33B — presentation cleanup / internal-language removal",
        });
      }
    }
  }
  return findings;
}

function classifyImagePreservation(row, brandConfig, registryAssets) {
  const slotKey = nz(row.slotKey);
  const imageUrl = nz(row.imageUrl);
  const hasImage = Boolean(imageUrl);
  const temp = hasImage && isTemporaryAirtableUrl(imageUrl);
  const wrongBrand = detectWrongBrandSignageRisk(`${row.title} ${row.body}`, brandConfig);
  const linked = registryAssets.find(
    (r) =>
      nz(r.recommendedExplorerSlot) === slotKey ||
      nz(r.sourceNotes).includes(row.recordId)
  );

  let classification = "preserve_working_image";
  if (wrongBrand) classification = "wrong_brand_or_unsupported";
  else if (temp) classification = "needs_durable_source";
  else if (hasImage && !linked) classification = "needs_registry_linkage";
  else if (!hasImage && /scenario|gallery|openings/.test(slotKey)) classification = "needs_replacement_later";

  return {
    recordId: row.recordId,
    slotKey,
    imageFieldPresent: hasImage,
    imageUrlExposedInApi: hasImage,
    imageLoads: hasImage && !temp ? "assumed_working_if_visible" : hasImage ? "temp_url_risk" : "no_image",
    urlTemporaryExpired: temp,
    registryLinkage: linked?.id || null,
    wrongBrandRisk: wrongBrand,
    classification,
    sourceSupport: extractUrlFromText(`${row.title}\n${row.body}`) || nz(row.summaryUrl),
  };
}

function buildResolverConfigAudit(resolved, discoveryConfig) {
  const configRecordId = discoveryConfig?.recordId ?? null;
  const liveRecordId = resolved.recordId || TARGET_BRAND.recordId;
  return {
    displayName: TARGET_BRAND.name,
    slug: TARGET_BRAND.slug,
    recordId: liveRecordId,
    brandSetupRecordId: liveRecordId,
    discoveryConfigPresent: Boolean(discoveryConfig),
    configRecordIdStillNull: configRecordId == null,
    recommendedConfigPatch: configRecordId == null ? liveRecordId : null,
    configPatchFile:
      configRecordId == null
        ? "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js (DISCOVERY_BRAND_CONFIG.woodspring-suites.recordId)"
        : null,
    slugResolutionSource: resolved.resolution?.resolutionSource || null,
    recordIdResolutionSource: resolved.resolution?.resolutionSource || null,
    sameV31nExpansionScoringPath:
      resolved.resolution?.resolutionSource === "expansion_backlog",
    completeBuildSafeBySlug: true,
    completeBuildSafeByRecordId: true,
  };
}

function classifyRequiredSection(label, inventory, sourceReady) {
  const inv = inventory || {};
  if (label.includes("Openings")) {
    return (inv["footprint.openings"]?.rowsPresent || 0) >= 3
      ? "ready"
      : "needs_openings_build";
  }
  if (label.includes("Momentum")) {
    return (inv["footprint.momentum"]?.rowsPresent || 0) >= 3
      ? "ready"
      : "needs_momentum_build";
  }
  if (label.includes("Featured Application")) {
    return (inv["overview.featured_application"]?.rowsPresent || 0) > 0
      ? "needs_copy_backfill"
      : "needs_copy_backfill";
  }
  if (label.includes("Standard Detail")) {
    return (inv.standard_detail?.rowsPresent || 0) >= 3
      ? "needs_copy_backfill"
      : "needs_copy_backfill";
  }
  if (label.includes("Portfolio Mix")) {
    return (inv.portfolio_mix?.rowsPresent || 0) >= 3 ? "needs_copy_backfill" : "needs_copy_backfill";
  }
  if (label.includes("Gallery")) {
    const scenarioTemp = inv["overview.scenario"]?.rowsWithTempUrls || 0;
    return scenarioTemp > 0 ? "needs_registry_alignment" : "needs_image_review";
  }
  if (!sourceReady) return "needs_source_stewardship";
  return "needs_copy_backfill";
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-source-registry-readiness-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_IMAGE_APPROVAL,
    APPLY_FLAG_NO_PRESENTATION_COPY,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
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

async function fetchAllBrandSources(brandRecordId) {
  const all = [];
  let offset = "";
  do {
    const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset || "";
  } while (offset);
  return all;
}

async function registryAirtableFetch(url, apiKey, init = {}) {
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

function registryAirtableUrl(baseId, recordId) {
  const table = encodeURIComponent(
    process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID || BRAND_ASSET_REGISTRY_TABLE
  );
  if (recordId) {
    return `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(recordId)}`;
  }
  return `https://api.airtable.com/v0/${baseId}/${table}`;
}

async function createRegistryBatch(registryCreates, stagingRunId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const created = [];
  for (const item of registryCreates) {
    const validation = validateRegistryWritePayload(item.fields);
    if (!validation.valid) {
      throw new Error(`Registry validation failed: ${JSON.stringify(validation.errors)}`);
    }
    const { res, json } = await registryAirtableFetch(registryAirtableUrl(baseId), apiKey, {
      method: "POST",
      body: JSON.stringify({ fields: item.fields, typecast: true }),
    });
    if (!res.ok) throw new Error(json.error?.message || `Registry create failed: ${res.status}`);
    created.push({
      recordId: json.id,
      assetName: item.staged.assetName,
      slotKey: item.slotKey,
      stagingRunId,
    });
    await new Promise((r) => setTimeout(r, 220));
  }
  return created;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Source + Registry Readiness v33A");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v33A exists: **${report.v33aWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Resolver / config");
  lines.push(`- Config recordId null: **${report.resolverConfigFindings.configRecordIdStillNull ? "yes" : "no"}**`);
  if (report.resolverConfigFindings.recommendedConfigPatch) {
    lines.push(`- Recommended code patch: \`${report.resolverConfigFindings.recommendedConfigPatch}\` in ${report.resolverConfigFindings.configPatchFile}`);
  }
  lines.push("");
  lines.push("## Source stewardship");
  lines.push(`- Sources audited: **${report.sourceStewardshipAudit.length}**`);
  lines.push(`- Proposed source updates: **${report.sourceRowsToUpdate.length}**`);
  lines.push("");
  lines.push("## Registry");
  lines.push(`- Existing registry: **${report.registryAudit.total}**`);
  lines.push(`- Proposed creates: **${report.registryProposedCreates.length}**`);
  lines.push("");
  lines.push("## Required sections");
  for (const [k, v] of Object.entries(report.requiredSectionReadiness || {})) {
    lines.push(`- ${k}: **${v.classification}**`);
  }
  lines.push("");
  lines.push(`**Next writer:** ${report.recommendedNextWriter}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("## Apply command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}

export async function buildBrandExplorerWoodspringSourceRegistryReadinessWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noImageFieldChanges = false,
  noImageApproval = false,
  noPresentationCopy = false,
  woodspringOnly = false,
} = {}) {
  const slug = nz(brandArg).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Protected brand cannot be modified by v33A: ${slug}`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33A is WoodSpring-only. Requested: ${brandArg}`);
  }

  const ctx = await getBrandTargetResolverContext();
  const resolved = await resolveBrandTarget(brandArg, ctx);
  const recordId = resolved.recordId || TARGET_BRAND.recordId;
  const discoveryConfig = getDiscoveryBrandConfig(TARGET_BRAND.slug);
  const brandConfig = { ...discoveryConfig, recordId, slug: TARGET_BRAND.slug, name: TARGET_BRAND.name };

  const brandBasicsBefore = await fetchBrandBasics(recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const liveState = await fetchLiveState(recordId);
  const brand = await fetchBrandApiShape(recordId);
  if (!brand) throw new Error(`Could not load WoodSpring API shape for ${recordId}`);

  const sources = await fetchAllBrandSources(recordId);
  const sourceStewardshipAudit = sources.map(auditEverhomeSourceRecord);

  const sourceRowsToUpdate = [];
  for (const audit of sourceStewardshipAudit) {
    if (!shouldStewardApproveSource(audit)) continue;
    const fields = buildWoodspringSourceStewardshipPatch(audit);
    const errors = validateSourcePatch(fields);
    if (errors.length) continue;
    sourceRowsToUpdate.push({
      recordId: audit.recordId,
      sourceTitle: audit.sourceTitle,
      fields,
    });
  }

  const registryAssetsRaw = await listRegistryAssetsForBrand(recordId).catch(() => []);
  const registryAudit = {
    total: registryAssetsRaw.length,
    approved: registryAssetsRaw.filter((r) => r.explorerUsePermission === "Approved For Explorer").length,
    pending: registryAssetsRaw.filter((r) => r.usageReviewStatus === "Pending Review").length,
    confirmedZeroAtStart: registryAssetsRaw.length === 0,
    tributeReferenceCount: (await listRegistryAssetsForBrand(TRIBUTE_RECORD_ID).catch(() => [])).length,
    everhomeReferenceCount: (await listRegistryAssetsForBrand(EVERHOME_RECORD_ID).catch(() => [])).length,
  };

  const allBlocks = brand.brandExplorer?.blocks || [];
  const presentationInventory = auditPresentationSection(allBlocks);
  const internalLanguageFindings = scanInternalLanguage(allBlocks);
  const visualRows = listWoodspringVisualRows(brand);
  const imagePreservationFindings = visualRows.map((row) =>
    classifyImagePreservation(row, brandConfig, registryAssetsRaw)
  );

  const wrongBrandRisks = [];
  for (const row of visualRows) {
    const wrong = detectWrongBrandSignageRisk(`${row.title} ${row.body}`, brandConfig);
    if (wrong) wrongBrandRisks.push({ presentationRowId: row.recordId, slotKey: row.slotKey, ...wrong });
    if (nz(row.imageUrl) && isTemporaryAirtableUrl(row.imageUrl)) {
      wrongBrandRisks.push({
        presentationRowId: row.recordId,
        slotKey: row.slotKey,
        markerId: "temporary_airtable_url",
        severity: "critical",
        reason: "Temporary Airtable attachment URL on presentation row",
      });
    }
  }

  const stagingRunId = STAGING_RUN_ID;
  const registryProposedCreates = [];
  const existingKeys = new Set(
    registryAssetsRaw.map((r) =>
      buildRegistryDedupeKey(
        {
          assetType: r.assetType,
          sourceUrl: r.sourceUrl,
          assetName: r.assetName,
          recommendedExplorerSlot: r.recommendedExplorerSlot,
        },
        recordId
      )
    )
  );

  for (const row of visualRows) {
    const wrongRisk = wrongBrandRisks.find((w) => w.presentationRowId === row.recordId) || null;
    const staged = buildWoodspringRegistryStagedAsset({
      row,
      brandConfig,
      stagingRunId,
      wrongBrandRisk: wrongRisk?.markerId === "temporary_airtable_url" ? null : wrongRisk,
    });
    const dedupeKey = buildRegistryDedupeKey(staged, recordId);
    if (existingKeys.has(dedupeKey)) continue;
    registryProposedCreates.push({
      presentationRowId: row.recordId,
      slotKey: row.slotKey,
      staged,
      fields: mapStagedToRegistryFields(
        staged,
        recordId,
        brandConfig.parentCompany || "Choice Hotels International"
      ),
      dedupeKey,
    });
    existingKeys.add(dedupeKey);
  }

  const imageUsageAudit = auditCurrentImageUsage(brand, brandConfig, registryAssetsRaw);
  const discoveryCandidates = discoverImageCandidates({
    brand,
    brandConfig,
    registryAssets: registryAssetsRaw,
    liveState,
    imageUsageAudit,
    stagingRunId,
  });
  for (const staged of discoveryCandidates) {
    const dedupeKey = buildRegistryDedupeKey(staged, recordId);
    if (existingKeys.has(dedupeKey)) continue;
    const enriched = {
      ...staged,
      explorerSection: inferExplorerSection(staged.recommendedExplorerSlot, staged.assetType),
      slotPurpose: "WoodSpring official source reference",
      validationStatus: "Needs Usage Review",
      validationNotes: "v33A discovery candidate — pending founder review.",
      brandConfirmed: "Yes",
      propertyConfirmed: "Unknown",
      calaRelevant: "No",
      explorerUsePermission: "Candidate Only",
      usageReviewStatus: "Pending Review",
      assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    };
    registryProposedCreates.push({
      presentationRowId: null,
      slotKey: staged.recommendedExplorerSlot,
      staged: enriched,
      fields: mapStagedToRegistryFields(
        enriched,
        recordId,
        brandConfig.parentCompany || "Choice Hotels International"
      ),
      dedupeKey,
      fromDiscovery: true,
    });
    existingKeys.add(dedupeKey);
  }

  const sourceReady =
    sourceStewardshipAudit.filter((s) => s.approvedExplorer).length +
      sourceRowsToUpdate.length >=
    3;

  const requiredSectionReadiness = {};
  for (const item of REQUIRED_SECTION_PLAN) {
    requiredSectionReadiness[item.label] = {
      classification: classifyRequiredSection(item.label, presentationInventory, sourceReady),
    };
  }

  const contractReport = await buildBrandExplorerRequiredSectionPopulationContractReport({
    brandIdOrName: recordId,
  }).catch(() => null);
  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({ brandIdOrName: slug }).catch(
    () => null
  );
  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const visualReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: recordId,
  }).catch(() => null);

  const applyBlockers = [];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noImageFieldChanges) applyBlockers.push("missing_confirm_no_image_field_changes");
    if (!noImageApproval) applyBlockers.push("missing_confirm_no_image_approval");
    if (!noPresentationCopy) applyBlockers.push("missing_confirm_no_presentation_copy_changes");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  for (const update of sourceRowsToUpdate) {
    if (/\bcompany validated\b/i.test(update.fields[MAP_PARTNER_SOURCE.permissionVisibilityNotes] || "")) {
      applyBlockers.push(`company_validation_implied:${update.recordId}`);
    }
  }
  for (const create of registryProposedCreates) {
    if (create.staged.explorerUsePermission === "Approved For Explorer") {
      applyBlockers.push("image_approval_blocked");
    }
    if (create.staged.sourcePageUrl && isTemporaryAirtableUrl(create.staged.sourcePageUrl)) {
      applyBlockers.push(`temporary_url_registry:${create.slotKey}`);
    }
  }

  const hasWork = sourceRowsToUpdate.length > 0 || registryProposedCreates.length > 0;
  const dryRunClean = applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  const applyResults = { sourceUpdates: [], registryCreates: [], errors: [] };

  const canApply =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFieldChanges &&
    noImageApproval &&
    noPresentationCopy &&
    woodspringOnly &&
    applyBlockers.length === 0;

  if (canApply && hasWork) {
    for (const update of sourceRowsToUpdate) {
      try {
        await patchPartnerSource(update.recordId, update.fields);
        applyResults.sourceUpdates.push(update.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 180));
      } catch (err) {
        applyResults.errors.push({ type: "source", recordId: update.recordId, message: err.message });
      }
    }
    if (registryProposedCreates.length) {
      try {
        applyResults.registryCreates = await createRegistryBatch(registryProposedCreates, stagingRunId);
        if (applyResults.registryCreates.length) airtableModified = true;
      } catch (err) {
        applyResults.errors.push({ type: "registry", message: err.message });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const buildSequence = [
    { id: "v33B", name: "Presentation Cleanup + Required Section Backfill", when: "after v33A source stewardship apply" },
    { id: "v33C", name: "Openings / Momentum Build", when: "after v33B copy cleanup" },
    { id: "v33D", name: "Existing Image Recognition / Registry Approval", when: "after openings/momentum or parallel once durable sources exist" },
    { id: "v33E", name: "Final Fact Stewardship + Active Profile Finalization", when: "after v33B–D gates pass" },
  ];

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33aWriterExists: v33aWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: { ...TARGET_BRAND, recordId },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    resolverConfigFindings: buildResolverConfigAudit(resolved, discoveryConfig),
    sourceStewardshipAudit,
    sourceRowsToUpdate,
    presentationInventoryFindings: presentationInventory,
    imagePreservationFindings,
    registryAudit,
    registryProposedCreates,
    internalLanguageFindings,
    wrongBrandRiskFindings: wrongBrandRisks,
    requiredSectionReadiness,
    buildSequenceRecommendation: buildSequence,
    recommendedNextWriter: "v33B — WoodSpring presentation cleanup + required section backfill",
    contractReadinessScore: contractReport?.readinessScore ?? null,
    expectedFinalQaResult: finalQaReport?.scores
      ? `${finalQaReport.scores.overallActiveProfileReadiness} (${finalQaReport.scores.overallNumeric})`
      : "unavailable",
    expectedCompleteBuildResult: completeBuildReport?.brandResults?.[0]?.readyForActiveProfile
      ? "ready band (post-stewardship projection)"
      : `blocked (readyForActiveProfile: ${completeBuildReport?.brandResults?.[0]?.readyForActiveProfile ?? "unknown"})`,
    expectedVisualDefectResult: visualReport?.defectCounts
      ? `${visualReport.defectCounts.total} defects (critical ${visualReport.defectCounts.critical}, high ${visualReport.defectCounts.high})`
      : "unavailable",
    applyBlockers,
    safetyBlockers: applyBlockers,
    dryRunClean,
    airtableModified,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    applyResults,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand: `npm run brand-explorer-woodspring-source-registry-readiness-writer -- --brand ${TARGET_BRAND.slug} --dry-run`,
    applyGuardrails: {
      woodspringOnly: true,
      noPresentationCopyChanges: true,
      noImageFieldChanges: true,
      noImageApproval: true,
      noCompanyValidationClaims: true,
    },
  };

  report.markdown = buildMarkdown(report);
  return report;
}
