/**
 * Brand Explorer Everhome Source Stewardship + Brand Asset Registry Normalization v32C.
 *
 * Everhome-only: stewards Source Library governance and creates/links pending Brand Asset
 * Registry records for existing visual/presentation assets. No presentation copy changes,
 * image approvals, activation, or Company Validated changes.
 *
 * @see docs/data-intelligence/brand-explorer-everhome-source-registry-normalization-writer-v32C.md
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

export const WRITER_VERSION = "v32C";
export const REPORT_JSON_NAME = "brand-explorer-everhome-source-registry-normalization-writer.json";
export const REPORT_MD_NAME = "brand-explorer-everhome-source-registry-normalization-writer.md";
export const DOC_MD_NAME = "brand-explorer-everhome-source-registry-normalization-writer-v32C.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v32C-everhome-source-registry-normalization";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_PRESENTATION = "--confirm-no-presentation-copy-changes";
export const APPLY_FLAG_NO_IMAGE_APPROVAL = "--confirm-no-image-approval";
export const APPLY_FLAG_EVERHOME_ONLY = "--confirm-everhome-only";

export const TARGET_BRAND = Object.freeze({
  slug: "everhome-suites",
  recordId: "recqkkrsevi4r9ibj",
  name: "Everhome Suites",
});

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "woodspring-suites",
  "suburban-studios",
  "tribute-portfolio",
  "radisson-individuals-by-choice",
]);

const RADISSON_INDIVIDUALS_RECORD_ID = "recRyvM8OmLlDj9G7";

const EVERHOME_VISUAL_SLOT_RE =
  /^(footprint\.openings|materials\.gallery\.\d|overview\.hero|overview\.scenario\.\d|valueOwners\.scenario\.\d)$/;

const INTERNAL_LANGUAGE_PATTERNS = [
  { id: "fdd", re: /\bfdd\b/i, family: "internal" },
  { id: "item_19", re: /\bitem\s*19\b/i, family: "internal" },
  { id: "franchise_disclosure", re: /\bfranchise disclosure\b/i, family: "internal" },
  { id: "confirm_fees", re: /\bconfirm fees\b/i, family: "governance" },
  { id: "confirm_flag", re: /\bconfirm flag\b/i, family: "governance" },
  { id: "performance_representation", re: /\bperformance representation\b/i, family: "governance" },
  { id: "active_property_page", re: /\bactive property page\b/i, family: "source_capture" },
  { id: "consumer_path", re: /\bconsumer path\b/i, family: "source_capture" },
  { id: "census", re: /\bcensus\b/i, family: "internal" },
  { id: "metadata", re: /\bmetadata\b/i, family: "internal" },
  { id: "source_data", re: /\bsource data\b/i, family: "internal" },
  { id: "internal", re: /\binternal\b/i, family: "internal" },
  { id: "extraction", re: /\bextraction\b/i, family: "source_capture" },
  { id: "source_capture", re: /\bsource[- ]capture\b/i, family: "source_capture" },
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-choice-extended-stay-batch-readiness-audit.json",
  "reports/brand-explorer-choice-extended-stay-source-capture-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.json",
  "reports/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.json",
  "reports/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.json",
  "reports/brand-explorer-radisson-individuals-asset-registry-normalization-writer.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-momentum-link-label.js",
  "live Everhome Brand Setup / Source Library / Presentation / Registry",
  "Tribute + Radisson Individuals registry reference rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-everhome-source-registry-normalization-writer.js",
  "scripts/brand-explorer-everhome-source-registry-normalization-writer.mjs",
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

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v32cWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-everhome-source-registry-normalization-writer.js")
  );
}

export function resolveEverhomeBrand(brandArg) {
  const slug = nz(brandArg).toLowerCase() || TARGET_BRAND.slug;
  if (slug !== TARGET_BRAND.slug) {
    throw new Error(
      `v32C is Everhome-only. Requested: ${brandArg}. Use ${TARGET_BRAND.slug} only.`
    );
  }
  if (PROTECTED_BRAND_SLUGS.includes(slug) && slug !== TARGET_BRAND.slug) {
    throw new Error(`Protected brand ${slug} cannot be modified by v32C`);
  }
  const config = getDiscoveryBrandConfig(slug);
  return { ...TARGET_BRAND, ...config, slug };
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

function parseEvidenceUseCases(notes) {
  const m = nz(notes).match(/v32B evidenceUseCase:\s*([^|]+)/i);
  if (!m) return [];
  return m[1]
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function inferStandardsLoyaltyPortfolio(classification, notes, evidenceCases) {
  const cases = evidenceCases.length ? evidenceCases : [];
  return {
    standards: cases.includes("Standards") || /development|brand page|standards/i.test(notes),
    loyalty: cases.includes("Loyalty") || /loyalty|choice privileges/i.test(notes),
    portfolioContext:
      cases.includes("Portfolio Context") ||
      classification.classification === "official",
  };
}

export function auditEverhomeSourceRecord(source) {
  const url = nz(source.sourceUrl);
  const durable = !isBlockedSourceUrl(url).blocked;
  const classification = classifySourceUrl(url, source.sourceTitle);
  const momentumType = classifyMomentumSourceType(url);
  const evidenceUseCases = parseEvidenceUseCases(source.notes);
  const suitability = inferStandardsLoyaltyPortfolio(classification, source.notes, evidenceUseCases);
  const fddContent =
    classification.internalOnly ||
    /\bfdd\b|item\s*19|franchise disclosure/i.test(
      `${source.sourceTitle} ${source.notes} ${url}`
    );

  return {
    recordId: source.id,
    sourceTitle: source.sourceTitle,
    sourceUrl: url,
    sourceType: source.sourceType,
    sourceRegion: source.region,
    sourceStatus: source.status,
    approvedForExplorerUse: source.approvedForExplorerUse,
    usagePermission: source.permissionVisibilityNotes,
    confidenceLevel: source.sourceQuality,
    externalDisplayStatus: source.visibility,
    evidenceUseCases: evidenceUseCases.length
      ? evidenceUseCases
      : classification.momentumAppropriate
        ? ["Momentum"]
        : classification.openingsAppropriate
          ? ["Openings"]
          : [],
    durable,
    classification: classification.classification,
    category: classification.category,
    momentumAppropriate: classification.momentumAppropriate && !fddContent,
    openingsAppropriate: classification.openingsAppropriate && !fddContent,
    standardsAppropriate: suitability.standards && !fddContent,
    loyaltyAppropriate: suitability.loyalty && !fddContent,
    portfolioContextAppropriate: suitability.portfolioContext && !fddContent,
    internalOnly: fddContent || !durable,
    fddOrItem19: fddContent,
    momentumSourceType: momentumType,
    approvedExplorer: isApprovedExplorerSource(source),
  };
}

export function shouldStewardApproveSource(audit) {
  if (audit.internalOnly || audit.fddOrItem19) return false;
  if (!audit.durable || !audit.sourceUrl) return false;
  if (audit.approvedExplorer) return false;
  return ["official", "trade", "property"].includes(audit.classification);
}

export function buildSourceStewardshipPatch(audit) {
  const today = new Date().toISOString().slice(0, 10);
  const permissionNote =
    audit.classification === "trade"
      ? "Public Source / Trade Source — AI-assisted Explorer evidence. Not company validation."
      : "AI-Assisted / Company Materials — AI-assisted Explorer evidence. Not company validation.";

  const evidenceNote = [
    `v32C evidenceUseCase: ${audit.evidenceUseCases.join(", ") || "General"}`,
    `momentumAppropriate: ${audit.momentumAppropriate ? "yes" : "no"}`,
    `openingsAppropriate: ${audit.openingsAppropriate ? "yes" : "no"}`,
    `stewardedBy: v32C`,
  ].join(" | ");

  const fields = {
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "Yes",
    [MAP_PARTNER_SOURCE.status]: "Approved",
    [MAP_PARTNER_SOURCE.sourceQuality]:
      audit.classification === "trade" ? "Medium" : "High",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.verifiedSource]: "Yes",
    [MAP_PARTNER_SOURCE.permissionVisibilityNotes]: permissionNote,
    [MAP_PARTNER_SOURCE.lastReviewed]: today,
    [MAP_PARTNER_SOURCE.notes]: evidenceNote,
  };

  if (audit.fddOrItem19) {
    fields[MAP_PARTNER_SOURCE.confidentialityNotes] = "internal_research_only — not owner-facing";
  }

  return fields;
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

export function listEverhomeVisualPresentationRows(brand) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => EVERHOME_VISUAL_SLOT_RE.test(nz(b?.slotKey)));
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
  if (brandConfig.officialDomains?.some((d) => u.includes(d))) return SOURCE_BASIS.COMPANY_MATERIALS;
  return SOURCE_BASIS.THIRD_PARTY;
}

function extractUrlFromText(text) {
  const m = nz(text).match(/https?:\/\/[^\s<>"')]+/i);
  return m ? m[0].replace(/[.,;)]+$/, "") : "";
}

function parsePropertyFromTitle(title) {
  const t = nz(title);
  if (!t) return "";
  const cleaned = t.replace(/^Everhome Suites\s*[-–—]\s*/i, "").trim();
  return cleaned || t;
}

export function buildEverhomeRegistryStagedAsset({
  row,
  brandConfig,
  stagingRunId,
  wrongBrandRisk,
}) {
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
      assetName: `DO NOT USE — Everhome — ${propertyName || slotKey} — Wrong-Brand Image`,
      assetType,
      assetStatus: ASSET_STATUS.DO_NOT_USE,
      sourceBasis: inferSourceBasis(rawImageUrl, brandConfig),
      sourceUrl: imageUrl,
      sourcePageUrl: durablePageUrl,
      recommendedExplorerSlot: slotKey,
      isPrimaryCandidate: false,
      explorerUsePermission: "Do Not Use",
      usageReviewStatus: "Blocked",
      sourceNotes: `v32C guard — presentation row ${row.recordId} (${slotKey})`,
      reviewNotes: wrongBrandRisk.reason,
      doNotUseReason: `${wrongBrandRisk.reason} (${wrongBrandRisk.markerId})`,
      stagingRunId,
      explorerSection: inferExplorerSection(slotKey, assetType),
      slotPurpose: `Everhome ${slotKey} visual`,
      relatedPropertyName: propertyName,
      validationStatus: "Do Not Use",
      validationNotes: "v32C — wrong-brand risk flagged; founder review required.",
      brandConfirmed: "Unknown",
      propertyConfirmed: "Unknown",
      calaRelevant: "No",
    };
  }

  return {
    assetName: `Everhome Suites — ${propertyName || slotKey} — ${assetType}`,
    assetType,
    assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    sourceBasis: inferSourceBasis(durablePageUrl, brandConfig),
    sourceUrl: imageUrl,
    sourcePageUrl: durablePageUrl,
    recommendedExplorerSlot: slotKey,
    isPrimaryCandidate: slotKey === "footprint.openings",
    explorerUsePermission: "Candidate Only",
    usageReviewStatus: "Pending Review",
    sourceNotes: `v32C linked — presentation row ${row.recordId}. Pending founder image review.`,
    reviewNotes: rawImageUrl && isTemporaryAirtableUrl(rawImageUrl)
      ? "Pending Image Review — presentation uses temporary Airtable attachment; replace with durable source before approval."
      : "Pending Image Review — v32C metadata-only registry link; not approved for Explorer.",
    stagingRunId,
    explorerSection: inferExplorerSection(slotKey, assetType),
    slotPurpose: `Everhome ${slotKey} visual candidate`,
    relatedPropertyName: propertyName,
    validationStatus: "Needs Usage Review",
    validationNotes: "v32C registry candidate — no automatic image approval.",
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
  if (staged.sourceUrl) fields[MAP_BRAND_ASSET.sourceUrl] = staged.sourceUrl;
  if (staged.sourcePageUrl) fields[MAP_BRAND_ASSET.sourcePageUrl] = staged.sourcePageUrl;
  if (staged.doNotUseReason) fields[MAP_BRAND_ASSET.doNotUseReason] = staged.doNotUseReason;
  return fields;
}

function auditRegistryRecord(rec) {
  const fieldMap = {
    brand: rec.brandId,
    assetName: rec.assetName,
    assetType: rec.assetType,
    explorerSection: rec.explorerSection,
    recommendedExplorerSlot: rec.recommendedExplorerSlot,
    relatedPropertyName: rec.relatedPropertyName,
    countryRegion: rec.countryRegion,
    sourceUrl: rec.sourceUrl,
    sourcePageUrl: rec.sourcePageUrl,
    sourceBasis: rec.sourceBasis,
    attachmentUrl: rec.sourceUrl,
    assetStatus: rec.assetStatus,
    explorerUsePermission: rec.explorerUsePermission,
    usageReviewStatus: rec.usageReviewStatus,
    validationStatus: rec.validationStatus,
    validationNotes: rec.validationNotes,
    slotPurpose: rec.slotPurpose,
    calaRelevant: rec.calaRelevant,
    brandConfirmed: rec.brandConfirmed,
    propertyConfirmed: rec.propertyConfirmed,
    reviewNotes: rec.reviewNotes,
    sourceNotes: rec.sourceNotes,
    stagingRunId: rec.stagingRunId,
    doNotUseReason: rec.doNotUseReason,
  };

  const missing = REGISTRY_COMPLETENESS_FIELDS.filter((f) => {
    const val = fieldMap[f.key];
    if (val == null) return f.required;
    if (Array.isArray(val)) return val.length === 0 && f.required;
    return nz(val) === "" && f.required;
  }).map((f) => f.key);

  return {
    recordId: rec.id,
    assetName: rec.assetName,
    assetStatus: rec.assetStatus,
    explorerUsePermission: rec.explorerUsePermission,
    usageReviewStatus: rec.usageReviewStatus,
    recommendedExplorerSlot: rec.recommendedExplorerSlot,
    doNotUse: isDoNotUseRecord(rec),
    missingFields: missing,
    incomplete: missing.length > 0,
  };
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
          family: pat.family,
          phrase: match[0],
          recommendedCleanupWriter: "v32D — presentation backfill / internal-language cleanup writer",
        });
      }
    }
  }
  return findings;
}

function auditMomentumRows(blocks) {
  const momentumSlots = blocks.filter((b) => /momentum|recent/i.test(nz(b.slotKey)));
  return momentumSlots.map((row) => {
    const url = extractUrlFromText(`${row.title}\n${row.body}`) || nz(row.summaryUrl);
    const tribute = followsTributeMomentumRules(url);
    const momentumType = classifyMomentumSourceType(url);
    const propertyListing = isMomentumInappropriatePropertyListing(url);
    return {
      recordId: row.recordId,
      slotKey: row.slotKey,
      title: nz(row.title),
      sourceUrl: url,
      momentumSourceType: momentumType,
      tributeMomentumOk: tribute.ok,
      propertyListingUsedAsMomentum: propertyListing,
      rank: momentumEvidenceSourceRank(url),
      issue: !tribute.ok
        ? tribute.reason || "momentum_source_hierarchy_violation"
        : propertyListing
          ? "property_listing_should_be_openings_not_momentum"
          : null,
      recommendedWriter: "v32E — openings/momentum rebuild writer",
    };
  });
}

function auditOpeningsRows(blocks, sources) {
  const openingRows = blocks.filter((b) => /footprint\.openings|opening/i.test(nz(b.slotKey)));
  const sourceByUrl = new Map(
    sources
      .filter((s) => nz(s.sourceUrl))
      .map((s) => [normalizeUrlKey(s.sourceUrl), s])
  );

  return openingRows.map((row) => {
    const url = extractUrlFromText(`${row.title}\n${row.body}`) || nz(row.summaryUrl);
    const durable = url ? !isBlockedSourceUrl(url).blocked : false;
    const linkedSource = url ? sourceByUrl.get(normalizeUrlKey(url)) : null;
    return {
      recordId: row.recordId,
      slotKey: row.slotKey,
      title: nz(row.title),
      sourceUrl: url,
      durableSource: durable,
      hasImage: Boolean(nz(row.imageUrl)),
      linkedSourceId: linkedSource?.id || null,
      linkedSourceApproved: linkedSource ? isApprovedExplorerSource(linkedSource) : false,
      registryLinkRecommended: true,
      issue: !durable ? "missing_durable_property_source" : !nz(row.imageUrl) ? "missing_image" : null,
      recommendedWriter: "v32E — openings/momentum rebuild writer",
    };
  });
}

function detectRegistryDuplicates(registryAssets) {
  const byUrl = new Map();
  const duplicates = [];
  for (const rec of registryAssets) {
    const key = normalizeUrlKey(rec.sourceUrl || rec.sourcePageUrl);
    if (!key) continue;
    if (byUrl.has(key)) {
      duplicates.push({
        recordId: rec.id,
        duplicateOf: byUrl.get(key),
        url: key,
        canonicalRecommendation: byUrl.get(key),
      });
    } else {
      byUrl.set(key, rec.id);
    }
  }
  return duplicates;
}

function detectWrongBrandRisks(visualRows, brandConfig) {
  const risks = [];
  for (const row of visualRows) {
    const wrongBrand = detectWrongBrandSignageRisk(`${row.title} ${row.body}`, brandConfig);
    if (wrongBrand) {
      risks.push({
        presentationRowId: row.recordId,
        slotKey: row.slotKey,
        imageUrl: nz(row.imageUrl),
        ...wrongBrand,
      });
    }
    if (nz(row.imageUrl) && isTemporaryAirtableUrl(row.imageUrl)) {
      risks.push({
        presentationRowId: row.recordId,
        slotKey: row.slotKey,
        imageUrl: row.imageUrl,
        markerId: "temporary_airtable_url",
        severity: "critical",
        reason: "Temporary Airtable attachment URL on presentation row",
      });
    }
  }
  return risks;
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-everhome-source-registry-normalization-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_PRESENTATION,
    APPLY_FLAG_NO_IMAGE_APPROVAL,
    APPLY_FLAG_EVERHOME_ONLY,
  ].join(" ");
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

async function createEverhomeRegistryBatch(registryCreates, stagingRunId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const created = [];
  const validationFailed = [];

  for (const item of registryCreates) {
    const validation = validateRegistryWritePayload(item.fields);
    if (!validation.valid) {
      validationFailed.push({ assetName: item.staged.assetName, errors: validation.errors });
      continue;
    }
    const url = registryAirtableUrl(baseId);
    const { res, json } = await registryAirtableFetch(url, apiKey, {
      method: "POST",
      body: JSON.stringify({ fields: item.fields, typecast: true }),
    });
    if (!res.ok) {
      throw new Error(json.error?.message || `Registry create failed: ${res.status}`);
    }
    created.push({
      recordId: json.id,
      assetName: item.staged.assetName,
      slotKey: item.slotKey,
      stagingRunId,
    });
    await new Promise((r) => setTimeout(r, 220));
  }

  if (validationFailed.length) {
    throw new Error(`Registry validation failed: ${JSON.stringify(validationFailed)}`);
  }

  return created;
}

export async function buildBrandExplorerEverhomeSourceRegistryNormalizationWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noPresentationCopy = false,
  noImageApproval = false,
  everhomeOnly = false,
} = {}) {
  const brandConfig = resolveEverhomeBrand(brandArg);
  const ctx = await getBrandTargetResolverContext();
  const resolved = await resolveBrandTarget(brandArg, ctx);
  const recordId = resolved.recordId || TARGET_BRAND.recordId;
  brandConfig.recordId = recordId;

  const brandBasicsBefore = await fetchBrandBasics(recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const liveState = await fetchLiveState(recordId);
  const brand = await fetchBrandApiShape(recordId);
  if (!brand) throw new Error(`Could not load Everhome API shape for ${recordId}`);

  const sources = await fetchAllBrandSources(recordId);
  const sourceStewardshipAudit = sources.map(auditEverhomeSourceRecord);

  const proposedSourceUpdates = [];
  for (const audit of sourceStewardshipAudit) {
    if (!shouldStewardApproveSource(audit)) continue;
    const fields = buildSourceStewardshipPatch(audit);
    const errors = validateSourcePatch(fields);
    if (errors.length) continue;
    proposedSourceUpdates.push({
      recordId: audit.recordId,
      before: {
        approvedForExplorerUse: audit.approvedForExplorerUse,
        sourceStatus: audit.sourceStatus,
      },
      fields,
    });
  }

  const registryAssetsRaw = await listRegistryAssetsForBrand(recordId).catch(() => []);
  const registryExtended = registryAssetsRaw;

  const registryAudit = {
    total: registryExtended.length,
    approved: registryExtended.filter((r) => r.explorerUsePermission === "Approved For Explorer").length,
    pending: registryExtended.filter(
      (r) =>
        r.explorerUsePermission === "Candidate Only" ||
        r.usageReviewStatus === "Pending Review" ||
        r.assetStatus === ASSET_STATUS.NEEDS_USAGE_REVIEW
    ).length,
    doNotUse: registryExtended.filter((r) => isDoNotUseRecord(r)).length,
    perRecord: registryExtended.map(auditRegistryRecord),
    confirmedZeroAtStart: registryExtended.length === 0,
  };

  const tributeRegistryCount = (await listRegistryAssetsForBrand(TRIBUTE_RECORD_ID).catch(() => [])).length;
  const radissonRegistryCount = (
    await listRegistryAssetsForBrand(RADISSON_INDIVIDUALS_RECORD_ID).catch(() => [])
  ).length;

  const visualRows = listEverhomeVisualPresentationRows(brand);
  const imageUsageAudit = auditCurrentImageUsage(brand, brandConfig, registryAssetsRaw);
  const wrongBrandRisks = detectWrongBrandRisks(visualRows, brandConfig);
  const stagingRunId = `v32C-everhome-${Date.now()}`;

  const registryCreates = [];
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
    const wrongRisk =
      wrongBrandRisks.find((w) => w.presentationRowId === row.recordId) || null;
    const staged = buildEverhomeRegistryStagedAsset({
      row,
      brandConfig,
      stagingRunId,
      wrongBrandRisk: wrongRisk,
    });
    const dedupeKey = buildRegistryDedupeKey(staged, recordId);
    if (existingKeys.has(dedupeKey)) continue;
    registryCreates.push({
      action: "create",
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

  const discoveryCandidates = discoverImageCandidates({
    brand,
    brandConfig,
    registryAssets: registryAssetsRaw,
    liveState: {
      ...liveState,
      sources: sources.map((s) => ({
        ...s,
        approvedForExplorerUse:
          proposedSourceUpdates.find((u) => u.recordId === s.id)?.fields[
            MAP_PARTNER_SOURCE.approvedForExplorerUse
          ] || s.approvedForExplorerUse,
      })),
    },
    imageUsageAudit,
    stagingRunId,
  });

  for (const staged of discoveryCandidates) {
    const dedupeKey = buildRegistryDedupeKey(staged, recordId);
    if (existingKeys.has(dedupeKey)) continue;
    const enriched = {
      ...staged,
      explorerSection: inferExplorerSection(staged.recommendedExplorerSlot, staged.assetType),
      slotPurpose: "Everhome official source reference",
      validationStatus: "Needs Usage Review",
      validationNotes: "v32C discovery candidate — pending founder review.",
      brandConfirmed: "Yes",
      propertyConfirmed: "Unknown",
      calaRelevant: "No",
    };
    registryCreates.push({
      action: "create",
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

  const allBlocks = brand.brandExplorer?.blocks || [];
  const internalLanguageFindings = scanInternalLanguage(allBlocks);
  const momentumFindings = auditMomentumRows(allBlocks);
  const openingsFindings = auditOpeningsRows(allBlocks, sources);

  const duplicateFindings = detectRegistryDuplicates(registryExtended);

  const applyBlockers = [];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noPresentationCopy) applyBlockers.push("missing_confirm_no_presentation_copy_changes");
    if (!noImageApproval) applyBlockers.push("missing_confirm_no_image_approval");
    if (!everhomeOnly) applyBlockers.push("missing_confirm_everhome_only");
  }

  for (const update of proposedSourceUpdates) {
    if (update.fields[MAP_PARTNER_SOURCE.approvedForExplorerUse] === "Yes") {
      const audit = sourceStewardshipAudit.find((a) => a.recordId === update.recordId);
      if (audit?.fddOrItem19) applyBlockers.push(`fdd_owner_facing_blocked:${update.recordId}`);
    }
  }

  for (const create of registryCreates) {
    const pageUrl = create.staged.sourcePageUrl;
    if (pageUrl && isTemporaryAirtableUrl(pageUrl)) {
      applyBlockers.push(`temporary_url_registry:${create.presentationRowId || create.slotKey}`);
    }
    if (create.staged.explorerUsePermission === "Approved For Explorer") {
      applyBlockers.push("image_approval_blocked");
    }
  }

  const dryRunClean = applyBlockers.length === 0 && (proposedSourceUpdates.length > 0 || registryCreates.length > 0);

  let airtableModified = false;
  const applyResults = { sourceUpdates: [], registryCreates: [], errors: [] };

  const canApply =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noPresentationCopy &&
    noImageApproval &&
    everhomeOnly &&
    applyBlockers.length === 0;

  if (canApply) {
    for (const update of proposedSourceUpdates) {
      try {
        const patched = await patchPartnerSource(update.recordId, update.fields);
        applyResults.sourceUpdates.push({ recordId: update.recordId, ok: true, id: patched.id });
        airtableModified = true;
      } catch (err) {
        applyResults.errors.push({ type: "source_patch", recordId: update.recordId, message: err.message });
      }
    }

    if (registryCreates.length) {
      try {
        const created = await createEverhomeRegistryBatch(registryCreates, stagingRunId);
        applyResults.registryCreates = created;
        if (created.length) airtableModified = true;
      } catch (err) {
        applyResults.errors.push({ type: "registry_create", message: err.message });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const report = {
    writerVersion: WRITER_VERSION,
    v32cWriterExists: v32cWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: {
      slug: TARGET_BRAND.slug,
      recordId,
      displayName: TARGET_BRAND.name,
    },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    sourceStewardshipAudit,
    proposedSourceUpdates,
    registryAudit: {
      ...registryAudit,
      referenceCounts: {
        tributePortfolio: tributeRegistryCount,
        radissonIndividuals: radissonRegistryCount,
      },
    },
    registryCreates,
    registryUpdates: [],
    momentumFindings,
    openingsFindings,
    internalLanguageFindings,
    wrongBrandRiskFindings: wrongBrandRisks,
    duplicateFindings,
    imageUsageAudit,
    applyBlockers,
    dryRunClean,
    airtableModified,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    applyResults,
    expectedFinalQaImpact:
      "Should reduce source_stewardship_needed defects; registry gaps partially addressed pending founder image review.",
    expectedCompleteBuildImpact:
      "Registry row count increases; active-profile still blocked until v32D/E/F image approval and copy cleanup.",
    recommendedNextWriter: "v32D — Everhome presentation backfill writer",
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: TARGET_BRAND.slug }) : null,
    exactDryRunCommand: `npm run brand-explorer-everhome-source-registry-normalization-writer -- --brand ${TARGET_BRAND.slug} --dry-run`,
    applyGuardrails: {
      everhomeOnly: true,
      noPresentationCopyChanges: true,
      noImageApproval: true,
      noCompanyValidationClaims: true,
      fddInternalNotOwnerFacing: true,
    },
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Everhome Source + Registry Normalization v32C");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32C exists: **${report.v32cWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Source stewardship");
  lines.push(`- Sources audited: **${report.sourceStewardshipAudit.length}**`);
  lines.push(`- Proposed source updates: **${report.proposedSourceUpdates.length}**`);
  lines.push("");
  lines.push("## Brand Asset Registry");
  lines.push(`- Existing registry rows: **${report.registryAudit.total}**`);
  lines.push(`- Registry creates proposed: **${report.registryCreates.length}**`);
  lines.push(`- Reference — Tribute: ${report.registryAudit.referenceCounts.tributePortfolio}, Radisson Individuals: ${report.registryAudit.referenceCounts.radissonIndividuals}`);
  lines.push("");
  lines.push("## Findings");
  lines.push(`- Internal-language hits: **${report.internalLanguageFindings.length}**`);
  lines.push(`- Momentum rows reviewed: **${report.momentumFindings.length}**`);
  lines.push(`- Openings rows reviewed: **${report.openingsFindings.length}**`);
  lines.push(`- Wrong-brand / temp URL risks: **${report.wrongBrandRiskFindings.length}**`);
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
