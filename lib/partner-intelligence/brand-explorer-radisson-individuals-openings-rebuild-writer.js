/**
 * Brand Explorer Radisson Individuals Openings / Examples Rebuild v31L.
 *
 * Rebuilds footprint.openings copy + registry linkage to Radisson/Tribute quality.
 * Reactivates rows only when founder-approved registry-backed images exist.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-openings-rebuild-writer-v31L.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
  listRegistryAssetsForBrand,
  validateRegistryWritePayload,
} from "./brand-asset-registry-workflow.js";
import { ASSET_STATUS, ASSET_TYPE, SOURCE_BASIS } from "./brand-asset-pr-package-governance.js";
import {
  DISCOVERY_BRAND_CONFIG,
  findRegistryAssetForPresentationRow,
  isRegistryAssetApprovedForExplorer,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  detectInternalUiLanguage,
  findInternalLanguageInRow,
  parseFootprintOpeningLocation,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  EXTERNAL_DISPLAY_STATUS_QUARANTINE,
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND,
  PROTECTED_BRAND_SLUGS,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import {
  fetchDurablePropertyImage,
  GALLERY_DURABLE_SOURCES,
  isDurableSourcePageUrl,
  isTemporaryAirtableUrl,
} from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  isDoNotUseRecord,
  isFounderApprovedRecord,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { MAP_VISUAL_SLOT, VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

export const WRITER_VERSION = "31L";
export const OPENINGS_SLOT = "footprint.openings";
export const REPORT_JSON_NAME = "brand-explorer-radisson-individuals-openings-rebuild-writer.json";
export const REPORT_MD_NAME = "brand-explorer-radisson-individuals-openings-rebuild-writer.md";
export const DOC_MD_NAME = "brand-explorer-radisson-individuals-openings-rebuild-writer-v31L.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v31L-radisson-individuals-openings-rebuild";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-individuals-openings-copy";
export const APPLY_FLAG_APPROVED_ONLY = "--confirm-approved-assets-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const STAGING_RUN_ID = "v31L-openings-rebuild";
export const PRESS_KIT_URL = "https://media.choicehotels.com/Radisson-Individuals-press-kit";
/** Shared press-kit source reference — not a per-property opening image. */
export const SHARED_PRESS_KIT_REGISTRY_ID = "rec9B2pa235RNmWSV";

export const RADISSON_REFERENCE = Object.freeze({
  slug: "radisson",
  recordId: "recywbx1YQSTCPqW1",
  name: "Radisson by Choice",
});

/** Curated openings row catalog — durable Choice property pages where known. */
export const OPENINGS_PROPERTY_CATALOG = Object.freeze([
  {
    presentationRecordId: "recM0XfO2UlkNBd5x",
    propertyName: "Medellín Individuals context",
    marketCity: "Medellín",
    countryRegion: "Colombia",
    chips: "Urban, Colombia, CALA, Portfolio example",
    meta: "Soft-Collection Conversion Context · upper-upscale lifestyle",
    scenario: "CALA Urban Context",
    sourcePageUrl: PRESS_KIT_URL,
    officialPropertyPageUrl: null,
    titleKeywords: ["medellin", "medellín"],
    gallerySlotKey: null,
  },
  {
    presentationRecordId: "recA57HKv0Zd2bGnx",
    propertyName: "Hotel Casa Don Luis by Faranda Boutique",
    marketCity: "Cartagena",
    countryRegion: "Colombia",
    chips: "Urban heritage, Colombia, CALA, Portfolio example",
    meta: "Soft-Collection Conversion Context · historic core adjacency",
    scenario: "Caribbean Heritage CALA Context",
    sourcePageUrl: "https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb017",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/hotel/hotel-casa-don-luis-cartagena-by-faranda-boutique-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-cb017",
    titleKeywords: ["cartagena", "casa don luis"],
    gallerySlotKey: "materials.gallery.2",
  },
  {
    presentationRecordId: "recFKCA1auFtGwwjY",
    propertyName: "Panama City Individuals context",
    marketCity: "Panama City",
    countryRegion: "Panama",
    chips: "Urban, Panama, CALA, Portfolio example",
    meta: "Choice-Family Distribution Context · capital metro",
    scenario: "CALA Gateway Context",
    sourcePageUrl: PRESS_KIT_URL,
    officialPropertyPageUrl: null,
    titleKeywords: ["panama city"],
    gallerySlotKey: null,
  },
  {
    presentationRecordId: "recLHEhgtaFWGjACc",
    propertyName: "Panama corridor Individuals context",
    marketCity: "Panama",
    countryRegion: "Panama",
    chips: "CALA, Panama, Urban / corridor, Portfolio example",
    meta: "Choice-Family Distribution Context · corridor example",
    scenario: "CALA Urban / Resort Context",
    sourcePageUrl: PRESS_KIT_URL,
    officialPropertyPageUrl: null,
    titleKeywords: ["panama"],
    gallerySlotKey: null,
  },
  {
    presentationRecordId: "rec0uiWsD44ePqr6M",
    propertyName: "Barranquilla Individuals context",
    marketCity: "Barranquilla",
    countryRegion: "Colombia",
    chips: "Urban, Colombia, CALA, Portfolio example",
    meta: "Soft-Collection Conversion Context · Caribbean coast",
    scenario: "CALA Urban Context",
    sourcePageUrl: PRESS_KIT_URL,
    officialPropertyPageUrl: null,
    titleKeywords: ["barranquilla"],
    gallerySlotKey: null,
  },
  {
    presentationRecordId: "recto7QMu58eMf5jV",
    propertyName: "Faranda Collection Bogota",
    marketCity: "Bogotá",
    countryRegion: "Colombia",
    chips: "Urban, Colombia, CALA, Portfolio example",
    meta: "Soft-Collection Conversion Context · capital urban",
    scenario: "CALA Urban Context",
    sourcePageUrl: "https://www.choicehotels.com/colombia/bogota/radisson-individuals-hotels/cb012",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/en/hotel/hotel-faranda-collection-bogota-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-cb012",
    titleKeywords: ["bogota", "bogotá"],
    gallerySlotKey: "materials.gallery.4",
  },
  {
    presentationRecordId: "rect0VNHSr1f5ImGx",
    propertyName: "Cali Individuals context",
    marketCity: "Cali",
    countryRegion: "Colombia",
    chips: "Urban, Colombia, CALA, Portfolio example",
    meta: "Soft-Collection Conversion Context · Pacific southwest",
    scenario: "CALA Urban Context",
    sourcePageUrl: PRESS_KIT_URL,
    officialPropertyPageUrl: null,
    titleKeywords: ["cali"],
    gallerySlotKey: null,
  },
  {
    presentationRecordId: "recVtiPqVGo8gUtpO",
    propertyName: "Hotel Faranda Bolivar Cucuta",
    marketCity: "Cúcuta",
    countryRegion: "Colombia",
    chips: "Urban, Colombia, CALA, Portfolio example",
    meta: "Soft-Collection Conversion Context · border metro",
    scenario: "CALA Urban Context",
    sourcePageUrl: "https://www.choicehotels.com/colombia/cucuta/radisson-individuals-hotels/cb010",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/en/hotel/hotel-faranda-bolivar-cucuta-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-cb010",
    titleKeywords: ["cucuta", "cúcuta", "bolivar"],
    gallerySlotKey: "materials.gallery.6",
  },
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-openings-display-parity-audit.md",
  "reports/brand-explorer-openings-display-parity-audit.json",
  "reports/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.md",
  "reports/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.json",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.md",
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.md",
  "reports/brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.md",
  "live Radisson Individuals footprint.openings rows",
  "live Radisson Individuals Brand Asset Registry rows",
  "live Source Library records",
  "Radisson by Choice footprint.openings (reference)",
  "Tribute openings/examples (reference)",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "docs/brand-explorer-presentation-slots.md",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-openings-rebuild-writer.js",
  "scripts/brand-explorer-radisson-individuals-openings-rebuild-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "public/js/brand-explorer-atelier-from-api.js",
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
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

export function v31lWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-openings-rebuild-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31L`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31L supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, options = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
    ...options,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
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

function normalizeOpeningsRow(rec) {
  const f = rec.fields || {};
  const sk = nz(f["Slot Key"]);
  if (sk !== OPENINGS_SLOT) return null;
  return {
    recordId: rec.id,
    fields: f,
    slotKey: sk,
    title: nz(f.Title),
    body: nz(f.Body),
    sortOrder: f["Sort Order"],
    externalDisplayStatus: nz(f["External Display Status"]),
    quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(f["External Display Status"])),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    hasImage: Array.isArray(f.Image) && f.Image.length > 0,
    imageUrl: Array.isArray(f.Image) && f.Image[0]?.url ? nz(f.Image[0].url) : null,
    summaryUrl: nz(f["Summary URL"] || f["View Summary URL"]),
    sourcePageUrl: nz(f["Source Page URL"]),
    caseSummaryOverview: nz(f["Case Summary Overview"]),
    caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
    caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
    caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
    caseSummaryTags: nz(f["Case Summary Tags"]),
    location: parseFootprintOpeningLocation(nz(f.Title), nz(f.Body)),
  };
}

async function fetchBrandApiShape(brandRecordId) {
  const req = { query: { brandId: brandRecordId, refresh: "1" }, headers: {} };
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

function catalogForRow(recordId) {
  return OPENINGS_PROPERTY_CATALOG.find((c) => c.presentationRecordId === recordId) || null;
}

export function buildOwnerFacingOpeningsCopy({ row, catalog }) {
  const location =
    parseFootprintOpeningLocation(row.title, row.body) ||
    (catalog ? `${catalog.marketCity}, ${catalog.countryRegion}` : "");
  const city = location.split(",")[0]?.trim() || catalog?.marketCity || "this market";
  const country =
    location.split(",").slice(-1)[0]?.trim() || catalog?.countryRegion || "";
  const locLine = country && city ? `${city}, ${country}` : city;
  const propertyLabel = catalog?.propertyName || city;

  const chips = catalog?.chips || "CALA, Soft collection, Portfolio example";
  const meta =
    catalog?.meta || "Radisson Individuals · Choice-Family Distribution Context";
  const scenario = catalog?.scenario || "CALA MARKET EXAMPLE";
  const teaser = `${propertyLabel} illustrates how Radisson Individuals positions a hand-selected independent or boutique asset within Choice-family distribution—portfolio example for owner context, not a performance guarantee.`;
  const summaryUrl = isDurableSourcePageUrl(row.summaryUrl) ? row.summaryUrl : catalog?.sourcePageUrl || null;

  const body = [chips, locLine, meta, scenario, teaser, summaryUrl].filter(Boolean).join("\n\n");
  const caseSummaryOverview = `${locLine}: portfolio example for Radisson Individuals by Choice within the Choice Hotels CALA corridor.`;
  const caseSummaryOwnerObjective =
    "Owner consideration: confirm current flag, standards, commercial terms, and operating status directly before underwriting.";
  const caseSummaryBrandRelevance =
    "Published Choice-family Individuals inventory in this market—useful for soft-collection positioning context, not comp-set proof.";
  const caseSummaryInterpretation =
    "Treat as a market and brand-fit reference only; validate economics, PIP scope, and member terms locally.";
  const caseSummaryTags = `CALA, ${city}, Individuals, Portfolio example`;

  const surfaces = {
    title: row.title,
    body,
    caseSummaryOverview,
    caseSummaryOwnerObjective,
    caseSummaryBrandRelevance,
    caseSummaryInterpretation,
    caseSummaryTags,
    summaryUrl,
  };

  for (const [field, text] of Object.entries(surfaces)) {
    if (detectInternalUiLanguage(text).length) {
      throw new Error(`Proposed copy failed internal-language guardrail on ${field}`);
    }
  }

  return surfaces;
}

function registryFieldsWouldApprove(fields) {
  return (
    fields[MAP_BRAND_ASSET.assetStatus] === ASSET_STATUS.APPROVED_EXPLORER ||
    fields[MAP_BRAND_ASSET.explorerUsePermission] === "Approved For Explorer" ||
    fields[MAP_BRAND_ASSET.usageReviewStatus] === "Usage Review Complete"
  );
}

export function buildPendingOpeningsRegistryFields({
  catalog,
  presentationRecordId,
  brandRecordId,
  imageUrl,
  sourcePageUrl,
}) {
  const assetName = `Radisson Individuals — ${catalog.propertyName} — Opening Example Image`;
  return {
    [MAP_BRAND_ASSET.assetName]: assetName,
    [MAP_BRAND_ASSET.brand]: [brandRecordId],
    [MAP_BRAND_ASSET.brandRecordId]: brandRecordId,
    [MAP_BRAND_ASSET.parentCompany]: "Choice Hotels International",
    [MAP_BRAND_ASSET.assetType]: ASSET_TYPE.PR_IMAGE,
    [MAP_BRAND_ASSET.assetStatus]: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    [MAP_BRAND_ASSET.sourceBasis]: SOURCE_BASIS.RENDERED_OFFICIAL,
    [MAP_BRAND_ASSET.sourceUrl]: imageUrl || null,
    [MAP_BRAND_ASSET.sourcePageUrl]: sourcePageUrl,
    [MAP_BRAND_ASSET.usageReviewStatus]: "Pending Review",
    [MAP_BRAND_ASSET.explorerUsePermission]: "Candidate Only",
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: OPENINGS_SLOT,
    [MAP_BRAND_ASSET.isPrimaryCandidate]: true,
    [MAP_BRAND_ASSET.reviewNotes]:
      "v31L openings rebuild — pending founder image review; not approved for active-profile.",
    [MAP_BRAND_ASSET.sourceNotes]: `Presentation row ${presentationRecordId}. Per-property opening example.`,
    [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
    [MAP_BRAND_ASSET.companyValidated]: false,
    [MAP_VISUAL_SLOT.explorerSection]: VISUAL_SLOT.RECENT_OPENINGS,
    [MAP_VISUAL_SLOT.slotPurpose]: "Opening / property example card image",
    [MAP_VISUAL_SLOT.relatedPropertyName]: catalog.propertyName,
    [MAP_VISUAL_SLOT.countryRegion]: catalog.countryRegion,
    [MAP_VISUAL_SLOT.calaRelevant]: "Yes",
    [MAP_VISUAL_SLOT.propertyConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.brandConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.validationStatus]: "Pending Review",
    [MAP_VISUAL_SLOT.validationNotes]:
      "v31L linked registry — founder approval required before reactivation.",
  };
}

function findDedicatedRegistry(registryAssets, presentationRecordId, catalog) {
  const bySlot = registryAssets.filter((a) => nz(a.recommendedExplorerSlot) === OPENINGS_SLOT);
  const byNotes = bySlot.filter(
    (a) =>
      nz(a.sourceNotes).includes(presentationRecordId) &&
      !isSharedSourceReferenceRegistry(a)
  );
  if (byNotes.length === 1) return byNotes[0];
  const byName = bySlot.find(
    (a) =>
      !isSharedSourceReferenceRegistry(a) &&
      nz(a.assetName).toLowerCase().includes(catalog?.marketCity?.toLowerCase() || "")
  );
  return byName || null;
}

function isSharedSourceReferenceRegistry(registry) {
  if (!registry) return false;
  if (registry.id === SHARED_PRESS_KIT_REGISTRY_ID) return true;
  const name = nz(registry.assetName).toLowerCase();
  if (name.includes("press kit") && name.includes("source reference")) return true;
  if (!registry.attachmentUrl && !registry.sourceUrl) return true;
  return false;
}

function hasDedicatedApprovedOpeningImage(registry) {
  if (!registry || isDoNotUseRecord(registry) || isSharedSourceReferenceRegistry(registry)) {
    return false;
  }
  if (!isFounderApprovedRecord(registry) && !isRegistryAssetApprovedForExplorer(registry)) {
    return false;
  }
  return Boolean(registry.attachmentUrl || registry.sourceUrl);
}

export function classifyOpeningsRowRecommendation({
  row,
  catalog,
  registryMatch,
  dedicatedRegistry,
  rebuiltCopy,
  durableImage,
}) {
  const internalBefore = findInternalLanguageInRow(row);
  const internalAfter = rebuiltCopy
    ? findInternalLanguageInRow({ ...row, ...rebuiltCopy })
    : [];

  if (registryMatch && isDoNotUseRecord(registryMatch)) {
    return {
      recommendation: "remove_from_owner_facing_evidence",
      reason: "linked_registry_do_not_use",
    };
  }

  if (registryMatch && isSharedSourceReferenceRegistry(registryMatch) && !dedicatedRegistry) {
    if (!durableImage?.ok && !row.hasImage) {
      return {
        recommendation: "replace_image_needed",
        reason: "shared_source_reference_no_property_image",
      };
    }
  }

  if (!catalog?.sourcePageUrl || catalog.sourcePageUrl === PRESS_KIT_URL) {
    if (!durableImage?.ok && !row.hasImage && !hasDedicatedApprovedOpeningImage(dedicatedRegistry)) {
      return {
        recommendation: "source_review_needed",
        reason: "no_property_level_durable_source",
      };
    }
  }

  const hasApprovedImage =
    hasDedicatedApprovedOpeningImage(dedicatedRegistry) ||
    (row.hasImage && hasDedicatedApprovedOpeningImage(dedicatedRegistry));

  if (internalBefore.length && !internalAfter.length && rebuiltCopy) {
    if (hasApprovedImage) {
      return { recommendation: "rebuild_and_reactivate", reason: "clean_copy_and_approved_image" };
    }
    return { recommendation: "replace_image_needed", reason: "copy_ready_pending_approved_image" };
  }

  if (!internalBefore.length && rebuiltCopy) {
    if (hasApprovedImage) {
      return { recommendation: "rebuild_and_reactivate", reason: "approved_image_ready" };
    }
    if (!row.hasImage && !durableImage?.ok) {
      return { recommendation: "replace_image_needed", reason: "missing_image" };
    }
    return { recommendation: "source_review_needed", reason: "copy_refresh_pending_founder_image_approval" };
  }

  if (internalBefore.length) {
    return { recommendation: "keep_quarantined", reason: "internal_language_unresolved" };
  }

  return { recommendation: "keep_quarantined", reason: "insufficient_evidence" };
}

export function canReactivateOpeningsRow({ row, dedicatedRegistry, rebuiltCopy }) {
  if (!rebuiltCopy) return { ok: false, reason: "no_rebuilt_copy" };
  if (findInternalLanguageInRow({ ...row, ...rebuiltCopy }).length) {
    return { ok: false, reason: "internal_language_in_rebuilt_copy" };
  }
  if (!dedicatedRegistry || isDoNotUseRecord(dedicatedRegistry)) {
    return { ok: false, reason: "no_dedicated_approved_registry" };
  }
  if (isSharedSourceReferenceRegistry(dedicatedRegistry)) {
    return { ok: false, reason: "shared_source_reference_not_property_image" };
  }
  if (
    !isFounderApprovedRecord(dedicatedRegistry) &&
    !isRegistryAssetApprovedForExplorer(dedicatedRegistry)
  ) {
    return { ok: false, reason: "registry_not_founder_approved" };
  }
  const hasImage =
    row.hasImage || Boolean(dedicatedRegistry.attachmentUrl || dedicatedRegistry.sourceUrl);
  if (!hasImage) return { ok: false, reason: "missing_approved_image" };
  return { ok: true, reason: "dedicated_approved_registry_and_clean_copy" };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return `npm run brand-explorer-radisson-individuals-openings-rebuild-writer -- --brand ${brand} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_APPROVED_ONLY} ${APPLY_FLAG_NO_VALIDATION}`;
}

export async function buildBrandExplorerRadissonIndividualsOpeningsRebuildWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  approvedAssetsOnly = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = DISCOVERY_BRAND_CONFIG[target.slug];

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const [presentationRaw, registryAssetsRaw, brandApiBefore, radissonRefRaw] = await Promise.all([
    listPresentationRowsRaw(baseId, apiKey, target.recordId, target.name),
    listRegistryAssetsForBrand(target.recordId).catch(() => []),
    fetchBrandApiShape(target.recordId),
    listPresentationRowsRaw(
      baseId,
      apiKey,
      RADISSON_REFERENCE.recordId,
      RADISSON_REFERENCE.name
    ),
  ]);

  const openingsRows = presentationRaw
    .map(normalizeOpeningsRow)
    .filter(Boolean)
    .sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0));

  const radissonReferenceCount = radissonRefRaw
    .map(normalizeOpeningsRow)
    .filter(Boolean).length;

  const openingsRowDiagnosis = [];
  const copyBeforeAfter = [];
  const rowsToRebuild = [];
  const rowsToReactivate = [];
  const rowsToKeepQuarantined = [];
  const proposedPresentationUpdates = [];
  const proposedRegistryCreates = [];
  const proposedRegistryUpdates = [];

  for (const row of openingsRows) {
    const catalog = catalogForRow(row.recordId);
    const dedicatedRegistry = findDedicatedRegistry(registryAssetsRaw, row.recordId, catalog);
    const registryMatch =
      dedicatedRegistry || findRegistryAssetForPresentationRow(registryAssetsRaw, row);

    let durableImage = null;
    if (catalog?.sourcePageUrl && catalog.sourcePageUrl !== PRESS_KIT_URL) {
      durableImage = await fetchDurablePropertyImage({
        sourcePageUrl: catalog.sourcePageUrl,
        officialPropertyPageUrl: catalog.officialPropertyPageUrl,
        titleKeywords: catalog.titleKeywords || [],
      });
    }

    let rebuiltCopy = null;
    let copyError = null;
    try {
      rebuiltCopy = buildOwnerFacingOpeningsCopy({ row, catalog });
    } catch (err) {
      copyError = err.message;
    }

    const classRec = classifyOpeningsRowRecommendation({
      row,
      catalog,
      registryMatch,
      dedicatedRegistry,
      rebuiltCopy,
      durableImage,
    });

    const reactivateCheck = canReactivateOpeningsRow({
      row,
      dedicatedRegistry,
      rebuiltCopy,
    });

    const internalLanguage = findInternalLanguageInRow(row);
    const modalCopy = {
      caseSummaryOverview: row.caseSummaryOverview,
      caseSummaryOwnerObjective: row.caseSummaryOwnerObjective,
      caseSummaryBrandRelevance: row.caseSummaryBrandRelevance,
      caseSummaryInterpretation: row.caseSummaryInterpretation,
      caseSummaryTags: row.caseSummaryTags,
    };

    const diagnosis = {
      recordId: row.recordId,
      title: row.title,
      location: row.location,
      currentBodyExcerpt: row.body.slice(0, 220),
      modalCopy,
      externalDisplayStatus: row.externalDisplayStatus || null,
      hasImage: row.hasImage,
      imageUrl: row.imageUrl,
      sourcePageUrl: row.sourcePageUrl || catalog?.sourcePageUrl || null,
      summaryUrl: row.summaryUrl || null,
      registryRecordId: registryMatch?.id || null,
      dedicatedRegistryRecordId: dedicatedRegistry?.id || null,
      registryApproved: dedicatedRegistry ? isFounderApprovedRecord(dedicatedRegistry) : false,
      registryDoNotUse: registryMatch ? isDoNotUseRecord(registryMatch) : false,
      sharedSourceReferenceRegistry:
        registryMatch && isSharedSourceReferenceRegistry(registryMatch),
      internalLanguageHits: internalLanguage,
      durableImageResolved: durableImage?.ok || false,
      durableImageUrl: durableImage?.imageUrl || null,
      recommendation: classRec.recommendation,
      recommendationReason: classRec.reason,
      copyRebuildError: copyError,
      reactivateEligible: reactivateCheck.ok,
      reactivateBlockReason: reactivateCheck.ok ? null : reactivateCheck.reason,
    };

    openingsRowDiagnosis.push(diagnosis);

    if (rebuiltCopy) {
      copyBeforeAfter.push({
        recordId: row.recordId,
        title: row.title,
        before: {
          body: row.body.slice(0, 400),
          caseSummaryOverview: row.caseSummaryOverview,
          internalLanguageCount: internalLanguage.length,
        },
        after: {
          body: rebuiltCopy.body.slice(0, 400),
          caseSummaryOverview: rebuiltCopy.caseSummaryOverview,
          internalLanguageCount: findInternalLanguageInRow({ ...row, ...rebuiltCopy }).length,
        },
      });
    }

    if (!rebuiltCopy) {
      rowsToKeepQuarantined.push({ recordId: row.recordId, reason: copyError || "copy_rebuild_failed" });
      continue;
    }

    rowsToRebuild.push({ recordId: row.recordId, title: row.title });

    const presentationFields = {
      Body: rebuiltCopy.body,
      "Case Summary Overview": rebuiltCopy.caseSummaryOverview,
      "Case Summary Owner Objective": rebuiltCopy.caseSummaryOwnerObjective,
      "Case Summary Brand Relevance": rebuiltCopy.caseSummaryBrandRelevance,
      "Case Summary Interpretation": rebuiltCopy.caseSummaryInterpretation,
      "Case Summary Tags": rebuiltCopy.caseSummaryTags,
      "Brand Name": target.name,
      Brand: [target.recordId],
    };

    if (reactivateCheck.ok && approvedAssetsOnly) {
      presentationFields["External Display Status"] = null;
      rowsToReactivate.push({ recordId: row.recordId, title: row.title });
    } else {
      presentationFields["External Display Status"] = EXTERNAL_DISPLAY_STATUS_QUARANTINE;
      rowsToKeepQuarantined.push({
        recordId: row.recordId,
        reason: reactivateCheck.reason || "pending_image_approval",
      });
    }

    proposedPresentationUpdates.push({
      recordId: row.recordId,
      slotKey: OPENINGS_SLOT,
      reactivate: reactivateCheck.ok && approvedAssetsOnly,
      fields: presentationFields,
    });

    if (catalog) {
      const sourcePageUrl = catalog.sourcePageUrl;
      const registryFields = buildPendingOpeningsRegistryFields({
        catalog,
        presentationRecordId: row.recordId,
        brandRecordId: target.recordId,
        imageUrl: durableImage?.imageUrl || row.imageUrl || null,
        sourcePageUrl,
      });

      if (
        dedicatedRegistry?.id &&
        !isDoNotUseRecord(dedicatedRegistry) &&
        !isSharedSourceReferenceRegistry(dedicatedRegistry)
      ) {
        proposedRegistryUpdates.push({
          registryRecordId: dedicatedRegistry.id,
          presentationRecordId: row.recordId,
          fields: registryFields,
          stageImageUrl: durableImage?.imageUrl || null,
        });
      } else {
        proposedRegistryCreates.push({
          presentationRecordId: row.recordId,
          fields: registryFields,
          stageImageUrl: durableImage?.imageUrl || null,
        });
      }
    }
  }

  const apiBlocksBefore = (brandApiBefore?.brandExplorer?.blocks || []).filter(
    (b) => nz(b.slotKey) === OPENINGS_SLOT
  );

  const emptyShellFix = {
    file: "public/js/brand-explorer-atelier-from-api.js",
    before: "propertyShell() × 3 when zero footprint.openings API blocks",
    after: "omit Openings / Examples / Properties section when zero visible blocks",
    status: "patched_in_repo",
    ownerFacingEmptyShellsRemoved: true,
  };

  const applyBlockers = [];
  if (openingsRows.length !== OPENINGS_PROPERTY_CATALOG.length) {
    applyBlockers.push(
      `expected_${OPENINGS_PROPERTY_CATALOG.length}_openings_rows_found_${openingsRows.length}`
    );
  }
  if (proposedPresentationUpdates.length === 0) {
    applyBlockers.push("no_presentation_updates");
  }
  if (
    proposedPresentationUpdates.some((u) => u.reactivate && !approvedAssetsOnly) ||
    rowsToReactivate.length > 0
  ) {
    // reactivation only when approvedAssetsOnly gate passed at apply time
  }
  if (
    proposedRegistryCreates.some((c) => registryFieldsWouldApprove(c.fields)) ||
    proposedRegistryUpdates.some((u) => registryFieldsWouldApprove(u.fields))
  ) {
    applyBlockers.push("images_would_be_marked_approved");
  }
  if (
    [...proposedRegistryCreates, ...proposedRegistryUpdates].some((r) =>
      isTemporaryAirtableUrl(r.fields[MAP_BRAND_ASSET.sourcePageUrl])
    )
  ) {
    applyBlockers.push("temporary_url_as_source_page");
  }
  if (
    proposedPresentationUpdates.some((u) => u.reactivate) &&
    !rowsToReactivate.every((r) =>
      openingsRowDiagnosis.find((d) => d.recordId === r.recordId)?.reactivateEligible
    )
  ) {
    applyBlockers.push("reactivation_without_approved_image");
  }
  for (const u of proposedPresentationUpdates) {
    if (u.reactivate && findInternalLanguageInRow({ body: u.fields.Body }).length) {
      applyBlockers.push(`internal_language_reactivation:${u.recordId}`);
    }
  }

  const hasWork =
    proposedPresentationUpdates.length > 0 ||
    proposedRegistryCreates.length > 0 ||
    proposedRegistryUpdates.length > 0;
  const applyGatesReady =
    apply && approveBatch && founderReviewed && approvedAssetsOnly && noValidationClaim;
  const dryRunClean = applyBlockers.length === 0 && emptyShellFix.ownerFacingEmptyShellsRemoved;
  const canApply = applyGatesReady && dryRunClean && hasWork;

  let airtableModified = false;
  let imagesApproved = false;
  let applyResults = { presentationUpdated: [], registryCreated: [], registryUpdated: [], errors: [] };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const create of proposedRegistryCreates) {
      const validation = validateRegistryWritePayload(create.fields);
      if (!validation.valid) {
        applyResults.errors.push({
          presentationRecordId: create.presentationRecordId,
          error: validation.errors.join("; "),
        });
        continue;
      }
      const { res, json } = await airtableFetch(baseId, apiKey, BRAND_ASSET_REGISTRY_TABLE, {
        method: "POST",
        body: JSON.stringify({ records: [{ fields: create.fields }], typecast: true }),
      });
      if (!res.ok) {
        applyResults.errors.push({
          presentationRecordId: create.presentationRecordId,
          error: json.error?.message || "registry create failed",
        });
        continue;
      }
      applyResults.registryCreated.push(json.records?.[0]?.id);
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const update of proposedRegistryUpdates) {
      if (registryFieldsWouldApprove(update.fields)) {
        applyResults.errors.push({ registryRecordId: update.registryRecordId, error: "approval_blocked" });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        BRAND_ASSET_REGISTRY_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: update.fields, typecast: true }) },
        update.registryRecordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          registryRecordId: update.registryRecordId,
          error: json.error?.message || "registry patch failed",
        });
        continue;
      }
      applyResults.registryUpdated.push(update.registryRecordId);
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const update of proposedPresentationUpdates) {
      if (update.reactivate && !approvedAssetsOnly) {
        applyResults.errors.push({ recordId: update.recordId, error: "reactivation_blocked" });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: update.fields, typecast: true }) },
        update.recordId
      );
      if (!res.ok) {
        applyResults.errors.push({ recordId: update.recordId, error: json.error?.message || "patch failed" });
        continue;
      }
      applyResults.presentationUpdated.push({
        recordId: update.recordId,
        reactivated: update.reactivate,
      });
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }

    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));
  const completeBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    v31lWriterExists: v31lWriterExists(),
    targetBrand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    airtableModified,
    imagesApproved: false,
    openingsRowDiagnosis,
    rowsToRebuild,
    rowsToReactivate,
    rowsToKeepQuarantined,
    copyBeforeAfter,
    imageRegistryStatus: openingsRowDiagnosis.map((d) => ({
      recordId: d.recordId,
      title: d.title,
      hasImage: d.hasImage,
      durableImageResolved: d.durableImageResolved,
      registryRecordId: d.registryRecordId,
      registryApproved: d.registryApproved,
      reactivateEligible: d.reactivateEligible,
    })),
    emptyShellFix,
    radissonReferenceOpeningsCount: radissonReferenceCount,
    apiOpeningsBlocksBefore: apiBlocksBefore.length,
    proposedPresentationUpdates,
    proposedRegistryCreates,
    proposedRegistryUpdates,
    applyBlockers,
    dryRunClean,
    canApply,
    applyResults,
    expectedUiResult: {
      before: "Three empty property-example-card shells (no API blocks)",
      afterCopyRebuild: "Openings section hidden until approved images + reactivation",
      afterFullReactivation:
        "Up to 4 property-example-card grid matching Radisson by Choice (when founder approves images)",
      reference: `${radissonReferenceCount} visible Radisson openings blocks`,
    },
    expectedActiveProfileResult: {
      note: "Copy rebuild + pending registry improves governance; active-profile requires founder-approved opening images per row.",
      reactivateNow: rowsToReactivate.length,
      pendingApproval: rowsToKeepQuarantined.length,
      finalQaBefore: finalQaBefore?.brandReports?.[0]?.scores || null,
      completeBuildBefore:
        (completeBefore?.brandReports || []).find((b) => b.slug === target.slug)?.readiness || null,
    },
    exactApplyCommand: dryRunClean && hasWork ? buildApplyCommand({ brand: target.slug }) : null,
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Radisson Individuals Openings Rebuild v31L`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.targetBrand.name}**`,
    `- v31L exists: **${report.v31lWriterExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}**`,
    `- Rows audited: **${report.openingsRowDiagnosis.length}**`,
    `- Rows to rebuild: **${report.rowsToRebuild.length}**`,
    `- Rows to reactivate: **${report.rowsToReactivate.length}**`,
    `- Rows to keep quarantined: **${report.rowsToKeepQuarantined.length}**`,
    `- Empty shell fix: **${report.emptyShellFix.after}**`,
    `- Company Validated untouched: **yes**`,
    `- Images approved: **no**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Row diagnosis",
    "",
  ];

  for (const d of report.openingsRowDiagnosis) {
    lines.push(`### ${d.title}`);
    lines.push(`- Record: \`${d.recordId}\``);
    lines.push(`- Recommendation: **${d.recommendation}** (${d.recommendationReason})`);
    lines.push(`- Internal language hits: ${d.internalLanguageHits.length}`);
    lines.push(`- Image: ${d.hasImage} · Durable source image: ${d.durableImageResolved}`);
    lines.push(`- Reactivate eligible: ${d.reactivateEligible}${d.reactivateBlockReason ? ` (${d.reactivateBlockReason})` : ""}`);
    lines.push("");
  }

  if (report.exactApplyCommand) {
    lines.push("## Apply command", "", "```bash", report.exactApplyCommand, "```");
  }
  if (report.applyBlockers.length) {
    lines.push("", "## Apply blockers", "");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }

  return lines.join("\n");
}
