/**
 * Brand Explorer Design Hotels Draft Cleanup v35E.
 *
 * Hides duplicate footprint.openings rows, applies CALA property labels,
 * promotes Stage 1 registry candidates, and links presentation ↔ registry.
 *
 * Draft-state only — no active-profile approval, no Company Validated changes.
 */
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
  VAL_ASSET_STATUS,
  VAL_ASSET_TYPE,
  VAL_EXPLORER_USE_PERMISSION,
  VAL_USAGE_REVIEW_STATUS,
  applyRegistryRecords,
  listRegistryAssetsForBrand,
  mapStagedAssetToRegistryFields,
} from "./brand-asset-registry-workflow.js";
import {
  ASSET_STATUS,
  ASSET_TYPE,
  SOURCE_BASIS,
} from "./brand-asset-pr-package-governance.js";
import {
  findRegistryAssetForPresentationRow,
  isRegistryAssetApprovedForExplorer,
  normalizeUrlKey,
} from "./brand-explorer-brand-asset-image-governance.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { DESIGN_HOTELS_PROPERTY_CATALOG } from "./brand-explorer-lifestyle-affiliation-property-catalog.js";
import {
  buildCalaPropertyOpeningCopy,
  CALA_SECTION_LABEL_DEFAULT,
  countCalaPropertyExamples,
  propertyExampleTitlePassesGovernance,
  selectPropertyExamplesWithGeographicFallback,
  shouldBlockUsFallbackForBrand,
} from "./brand-explorer-cala-property-example-rules.js";
import {
  evaluateAllFactoryRules,
} from "./brand-explorer-active-profile-factory-rules.js";
import {
  evaluateFounderVisualReview,
} from "./brand-explorer-active-profile-staged-apply.js";
import { MAP_VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const V35E_VERSION = "v35E";
export const STAGING_RUN_ID = "v35E-design-hotels-draft-cleanup";
export const STAGE1_REGISTRY_STAGING_RUN_ID = "v34D-active-profile-draft-apply";

export const TARGET_BRAND = Object.freeze({
  slug: "design-hotels",
  recordId: "rec02zPClpWUTCyXM",
  name: "Design Hotels",
});

/** Duplicate rows from second draft-apply pass — hide only, never delete. */
export const DUPLICATE_OPENING_RECORD_IDS = Object.freeze([
  "recdD7rNZzhj8YpQH",
  "rec0o2XPGiafhcwLJ",
  "recXNKKIqeI7Vvi0C",
]);

export const CALA_PROPERTY_NAMES = Object.freeze([
  "Wake BioHotel",
  "Condesa DF",
  "Carlota",
]);

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v35E-design-hotels-draft-cleanup";
export const APPLY_FLAG_FOUNDER_ASSETS = "--founder-approved-design-hotels-draft-assets";
export const APPLY_FLAG_CALA_FIRST = "--confirm-cala-property-examples-first";
export const APPLY_FLAG_NO_US_FALLBACK = "--confirm-no-us-fallback-when-three-cala-examples-exist";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_ACTIVE_APPROVAL = "--confirm-no-active-profile-approval";
export const APPLY_FLAG_NO_SUMMARY_URL = "--confirm-no-summary-url-field";
export const APPLY_FLAG_DESIGN_HOTELS_ONLY = "--confirm-design-hotels-only";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const HIDE_DISPLAY = "Do Not Display";
const PRESENTATION_REGISTRY_LINK_FIELD = "Brand Asset Registry";
const PRESENTATION_SUPPORTS_REGISTRY_LINK =
  process.env.BRAND_EXPLORER_PRESENTATION_REGISTRY_LINK === "1";

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
  "Image",
  "Scenario Image",
  "Images",
  "Attachments",
  "Ready for Active Profile",
  "Active Profile Approved",
]);

const REGISTRY_REVIEW_NOTES =
  "v35E founder-approved draft assets — official public source imagery aligned for Explorer; AI-assisted registry promotion; not a Design Hotels company validation claim.";
const REGISTRY_VALIDATION_NOTES =
  "Official public source page imagery — usage review complete for Explorer draft slot; preserve independent/design-led affiliation framing.";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: Boolean(fields["Company Validated"]),
    companyValidationDate: fields["Company Validation Date"] || null,
  };
}

function parseCliFlags(argv = process.argv.slice(2)) {
  const args = argv.filter((a) => !a.startsWith("--") || a === "--apply");
  return {
    brandArg: args.find((a) => a !== "--apply" && !a.startsWith("--")) || TARGET_BRAND.slug,
    apply: argv.includes("--apply"),
    approveCleanup: argv.includes(APPLY_FLAG_APPROVE),
    founderApprovedAssets: argv.includes(APPLY_FLAG_FOUNDER_ASSETS),
    confirmCalaFirst: argv.includes(APPLY_FLAG_CALA_FIRST),
    confirmNoUsFallback: argv.includes(APPLY_FLAG_NO_US_FALLBACK),
    confirmNoValidation: argv.includes(APPLY_FLAG_NO_VALIDATION),
    confirmNoActiveApproval: argv.includes(APPLY_FLAG_NO_ACTIVE_APPROVAL),
    confirmNoSummaryUrl: argv.includes(APPLY_FLAG_NO_SUMMARY_URL),
    confirmDesignHotelsOnly: argv.includes(APPLY_FLAG_DESIGN_HOTELS_ONLY),
  };
}

export function buildApplyCommand() {
  return [
    "npm run brand-explorer-design-hotels-draft-cleanup --",
    `--brand ${TARGET_BRAND.slug}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER_ASSETS,
    APPLY_FLAG_CALA_FIRST,
    APPLY_FLAG_NO_US_FALLBACK,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_ACTIVE_APPROVAL,
    APPLY_FLAG_NO_SUMMARY_URL,
    APPLY_FLAG_DESIGN_HOTELS_ONLY,
  ].join(" ");
}

export function validateV35eRegistryPromotionPayload(fields) {
  const errors = [];
  if (fields[MAP_BRAND_ASSET.companyValidated]) errors.push("Company Validated must not be set");
  if (fields[MAP_BRAND_ASSET.companyValidationDate]) {
    errors.push("Company Validation Date must not be set");
  }
  if (fields[MAP_BRAND_ASSET.attachment]) errors.push("Attachment must not be set");
  if (
    fields[MAP_BRAND_ASSET.assetStatus] &&
    !VAL_ASSET_STATUS.includes(fields[MAP_BRAND_ASSET.assetStatus])
  ) {
    errors.push(`Invalid Asset Status: ${fields[MAP_BRAND_ASSET.assetStatus]}`);
  }
  if (
    fields[MAP_BRAND_ASSET.explorerUsePermission] &&
    !VAL_EXPLORER_USE_PERMISSION.includes(fields[MAP_BRAND_ASSET.explorerUsePermission])
  ) {
    errors.push(`Invalid Explorer Use Permission: ${fields[MAP_BRAND_ASSET.explorerUsePermission]}`);
  }
  if (
    fields[MAP_BRAND_ASSET.usageReviewStatus] &&
    !VAL_USAGE_REVIEW_STATUS.includes(fields[MAP_BRAND_ASSET.usageReviewStatus])
  ) {
    errors.push(`Invalid Usage Review Status: ${fields[MAP_BRAND_ASSET.usageReviewStatus]}`);
  }
  return { valid: errors.length === 0, errors };
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
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

async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    rows.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);

  return rows.map((rec) => {
    const f = rec.fields || {};
    const imageAtt = f.Image?.[0] || f["Scenario Image"]?.[0];
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      meta: nz(f.Meta),
      chips: nz(f.Chips),
      externalDisplayStatus: nz(f["External Display Status"]),
      visible: !/do not display|internal only/i.test(nz(f["External Display Status"])),
      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",
      registryLinkIds: Array.isArray(f[PRESENTATION_REGISTRY_LINK_FIELD])
        ? f[PRESENTATION_REGISTRY_LINK_FIELD]
        : [],
    };
  });
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

function catalogEntryForOpeningRow(row) {
  const titleLead = nz(row.title).split("—")[0].trim().toLowerCase();
  return DESIGN_HOTELS_PROPERTY_CATALOG.find((c) =>
    titleLead.includes(nz(c.propertyName).toLowerCase()) ||
    nz(c.propertyName).toLowerCase().includes(titleLead)
  );
}

function isStage1RegistryCandidate(asset) {
  if (isRegistryAssetApprovedForExplorer(asset)) return false;
  if (nz(asset.stagingRunId) === STAGE1_REGISTRY_STAGING_RUN_ID) return true;
  if (/v34D-active-profile-draft-apply/i.test(nz(asset.stagingRunId))) return true;
  if (/v34B generic factory draft/i.test(nz(asset.sourceNotes))) return true;
  if (/v34B generic factory draft/i.test(nz(asset.reviewNotes))) return true;
  return (
    nz(asset.explorerUsePermission) === "Candidate Only" &&
    /needs usage review|pending review|not reviewed/i.test(
      [asset.assetStatus, asset.usageReviewStatus].filter(Boolean).join(" ")
    )
  );
}

function resolveRegistryForRow(registryAssets, row) {
  const titleLead = nz(row.title).split("—")[0].trim().toLowerCase();

  if (row.slotKey === "footprint.openings" && titleLead) {
    const byProperty = registryAssets.find(
      (asset) =>
        nz(asset.recommendedExplorerSlot) === row.slotKey &&
        nz(asset.assetName).toLowerCase().includes(titleLead)
    );
    if (byProperty) return byProperty;
  }

  if (/^overview\.scenario\.\d+$/.test(row.slotKey)) {
    const bySlot = registryAssets.find(
      (asset) => nz(asset.recommendedExplorerSlot) === row.slotKey
    );
    if (bySlot) return bySlot;
  }

  const direct = findRegistryAssetForPresentationRow(registryAssets, row);
  if (direct) return direct;

  return (
    registryAssets.find((asset) => {
      if (nz(asset.recommendedExplorerSlot) !== row.slotKey) return false;
      if (row.slotKey.startsWith("materials.gallery.")) {
        return (
          nz(asset.recommendedExplorerSlot) === row.slotKey ||
          nz(asset.assetName).includes(row.slotKey)
        );
      }
      return false;
    }) || null
  );
}

function buildScenarioRegistryStaged(row, brandConfig) {
  const officialPage = brandConfig.consumerUrl || "https://www.designhotels.com/";
  return {
    assetName: `${brandConfig.name} — ${row.slotKey}`,
    assetType: ASSET_TYPE.PR_IMAGE,
    assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    sourceBasis: SOURCE_BASIS.RENDERED_OFFICIAL,
    sourceUrl: officialPage,
    sourcePageUrl: officialPage,
    usageReviewStatus: "Pending Review",
    explorerUsePermission: "Candidate Only",
    recommendedExplorerSlot: row.slotKey,
    isPrimaryCandidate: false,
    sourceNotes: `v35E scenario registry backfill — official public source imagery for ${row.slotKey}`,
    reviewNotes: "v35E scenario registry backfill — AI-assisted draft traceability; not a company validation claim.",
  };
}

function planScenarioRegistryCreates(visualRows, registryAssets, brandConfig) {
  const creates = [];
  for (const row of visualRows.filter((r) => /^overview\.scenario\.\d+$/.test(r.slotKey))) {
    if (resolveRegistryForRow(registryAssets, row)) continue;
    creates.push({
      presentationRecordId: row.recordId,
      slotKey: row.slotKey,
      staged: buildScenarioRegistryStaged(row, brandConfig),
    });
  }
  return creates;
}

export function buildRegistryPromotionFields(asset, row, brandConfig) {
  const catalog = row.slotKey === "footprint.openings" ? catalogEntryForOpeningRow(row) : null;
  const linkedNote = `Linked presentation row ${row.recordId} (${row.slotKey}). ${REGISTRY_VALIDATION_NOTES}`;
  const existingNotes = nz(asset?.sourceNotes);
  const sourceNotes = existingNotes.includes(row.recordId)
    ? existingNotes
    : existingNotes
      ? `${existingNotes} ${linkedNote}`
      : linkedNote;

  const fields = {
    [MAP_BRAND_ASSET.assetStatus]: ASSET_STATUS.APPROVED_EXPLORER,
    [MAP_BRAND_ASSET.explorerUsePermission]: "Approved For Explorer",
    [MAP_BRAND_ASSET.usageReviewStatus]: "Usage Review Complete",
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: row.slotKey,
    [MAP_BRAND_ASSET.sourceNotes]: sourceNotes,
    [MAP_BRAND_ASSET.reviewNotes]: REGISTRY_REVIEW_NOTES,
    [MAP_VISUAL_SLOT.validationStatus]: "Valid for Slot",
    [MAP_VISUAL_SLOT.validationNotes]: REGISTRY_VALIDATION_NOTES,
    [MAP_VISUAL_SLOT.brandConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.sourcePageConfirmsContext]: "Yes",
    [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
    [MAP_BRAND_ASSET.companyValidated]: false,
  };

  if (catalog) {
    fields[MAP_VISUAL_SLOT.relatedPropertyName] = catalog.propertyName;
    fields[MAP_VISUAL_SLOT.countryRegion] = catalog.stateRegion || catalog.marketCity;
    fields[MAP_VISUAL_SLOT.calaRelevant] = "Yes";
    fields[MAP_VISUAL_SLOT.propertyConfirmed] = "Yes";
    fields[MAP_VISUAL_SLOT.slotPurpose] = `Design Hotels CALA property example — ${catalog.propertyName}`;
  } else if (row.slotKey.startsWith("materials.gallery.")) {
    fields[MAP_VISUAL_SLOT.calaRelevant] = "Yes";
    fields[MAP_VISUAL_SLOT.propertyConfirmed] = "Unknown";
    fields[MAP_VISUAL_SLOT.slotPurpose] = `Design Hotels gallery — ${row.slotKey}`;
  } else if (row.slotKey.startsWith("overview.scenario.")) {
    fields[MAP_VISUAL_SLOT.calaRelevant] = "Unknown";
    fields[MAP_VISUAL_SLOT.propertyConfirmed] = "Unknown";
    fields[MAP_VISUAL_SLOT.slotPurpose] = `Design Hotels scenario card — ${row.slotKey}`;
    const officialPage = brandConfig?.consumerUrl || "https://www.designhotels.com/";
    if (!nz(asset?.sourcePageUrl)) {
      fields[MAP_BRAND_ASSET.sourcePageUrl] = officialPage;
    }
    if (!nz(asset?.sourceUrl) || isTemporaryAirtableUrl(asset?.sourceUrl)) {
      fields[MAP_BRAND_ASSET.sourceUrl] = officialPage;
      fields[MAP_BRAND_ASSET.sourceBasis] = SOURCE_BASIS.RENDERED_OFFICIAL;
    }
  }

  if (catalog?.sourcePageUrl && !nz(asset?.sourcePageUrl)) {
    fields[MAP_BRAND_ASSET.sourcePageUrl] = catalog.sourcePageUrl;
  }
  if (!nz(asset?.sourceBasis)) {
    fields[MAP_BRAND_ASSET.sourceBasis] = SOURCE_BASIS.RENDERED_OFFICIAL;
  }
  if (!nz(asset?.assetType)) {
    fields[MAP_BRAND_ASSET.assetType] = ASSET_TYPE.PR_IMAGE;
  }

  return fields;
}

function auditOpeningsRows(presentationRows) {
  const openings = presentationRows.filter((r) => r.slotKey === "footprint.openings");
  const visible = openings.filter((r) => r.visible);
  const hidden = openings.filter((r) => !r.visible);
  const duplicatesByProperty = {};
  for (const row of visible) {
    const key = nz(row.title).split("—")[0].trim().toLowerCase();
    duplicatesByProperty[key] = duplicatesByProperty[key] || [];
    duplicatesByProperty[key].push(row);
  }
  return {
    total: openings.length,
    visibleCount: visible.length,
    hiddenCount: hidden.length,
    visible,
    hidden,
    duplicatesByProperty,
  };
}

const DUPLICATE_SET = new Set(DUPLICATE_OPENING_RECORD_IDS);

function pickCanonicalOpeningRow(presentationRows, catalog) {
  const matches = presentationRows.filter(
    (r) =>
      r.slotKey === "footprint.openings" &&
      r.visible &&
      nz(r.title).toLowerCase().includes(nz(catalog.propertyName).toLowerCase())
  );
  return matches.find((r) => !DUPLICATE_SET.has(r.recordId)) || matches[0] || null;
}

function planOpeningCleanup(presentationRows, brandConfig) {
  const calaSelection = selectPropertyExamplesWithGeographicFallback(DESIGN_HOTELS_PROPERTY_CATALOG, {
    minimum: brandConfig.propertyExampleMinimum || 3,
  });
  const hidePatches = [];
  const calaLabelPatches = [];
  const sectionLabel = brandConfig.propertyExampleSectionLabel || CALA_SECTION_LABEL_DEFAULT;

  for (const recordId of DUPLICATE_OPENING_RECORD_IDS) {
    const row = presentationRows.find((r) => r.recordId === recordId);
    if (!row) continue;
    if (row.visible) {
      hidePatches.push({
        recordId,
        slotKey: row.slotKey,
        action: "hide_duplicate",
        fields: { "External Display Status": HIDE_DISPLAY },
        reason: "v35E duplicate opening row from second draft-apply pass",
      });
    }
  }

  for (const catalog of calaSelection.selected) {
    const keep = pickCanonicalOpeningRow(presentationRows, catalog);
    if (!keep) continue;
    const copy = buildCalaPropertyOpeningCopy(catalog, { sectionLabel });
    calaLabelPatches.push({
      recordId: keep.recordId,
      slotKey: keep.slotKey,
      propertyName: catalog.propertyName,
      fields: {
        Title: copy.title,
        Body: copy.body ? `${copy.body}\n\n${sectionLabel}` : sectionLabel,
      },
      sectionLabel,
      reason: "v35E CALA property example label — no U.S. fallback",
    });
    const matches = presentationRows.filter(
      (r) =>
        r.slotKey === "footprint.openings" &&
        r.visible &&
        nz(r.title).toLowerCase().includes(nz(catalog.propertyName).toLowerCase())
    );
    for (const dup of matches) {
      if (dup.recordId === keep.recordId) continue;
      if (!hidePatches.some((p) => p.recordId === dup.recordId)) {
        hidePatches.push({
          recordId: dup.recordId,
          slotKey: dup.slotKey,
          action: "hide_extra_duplicate",
          fields: { "External Display Status": HIDE_DISPLAY },
          reason: `extra visible duplicate for ${catalog.propertyName}`,
        });
      }
    }
  }

  return {
    calaSelection,
    hidePatches,
    calaLabelPatches,
    sectionLabel,
  };
}

function planRegistryPromotion(registryAssets, presentationRows, brandConfig, options = {}) {
  const hideRecordIds = new Set(options.hideRecordIds || []);
  const visualRows = presentationRows.filter(
    (r) =>
      r.visible &&
      !hideRecordIds.has(r.recordId) &&
      (/^materials\.gallery\.\d+$/.test(r.slotKey) ||
        r.slotKey === "footprint.openings" ||
        /^overview\.scenario\.\d+$/.test(r.slotKey)) &&
      r.imageUrl
  );

  const candidates = registryAssets.filter(isStage1RegistryCandidate);
  const promotions = [];
  const presentationLinks = [];
  const linkAudit = [];

  for (const row of visualRows) {
    const registry = resolveRegistryForRow(candidates, row) || resolveRegistryForRow(registryAssets, row);
    if (!registry) {
      linkAudit.push({
        presentationRecordId: row.recordId,
        slotKey: row.slotKey,
        status: "missing_registry_match",
      });
      continue;
    }

    const promotionFields = buildRegistryPromotionFields(registry, row, brandConfig);
    const validation = validateV35eRegistryPromotionPayload(promotionFields);
    if (!validation.valid) {
      linkAudit.push({
        presentationRecordId: row.recordId,
        registryRecordId: registry.id,
        slotKey: row.slotKey,
        status: "registry_validation_failed",
        errors: validation.errors,
      });
      continue;
    }

    promotions.push({
      registryRecordId: registry.id,
      presentationRecordId: row.recordId,
      slotKey: row.slotKey,
      fields: promotionFields,
      before: {
        explorerUsePermission: registry.explorerUsePermission,
        usageReviewStatus: registry.usageReviewStatus,
        assetStatus: registry.assetStatus,
      },
    });

    const needsLink =
      !row.registryLinkIds?.includes(registry.id) || row.registryLinkIds.length === 0;
    if (needsLink && PRESENTATION_SUPPORTS_REGISTRY_LINK) {
      presentationLinks.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        fields: { [PRESENTATION_REGISTRY_LINK_FIELD]: [registry.id] },
      });
    }

    linkAudit.push({
      presentationRecordId: row.recordId,
      registryRecordId: registry.id,
      slotKey: row.slotKey,
      status: needsLink
        ? PRESENTATION_SUPPORTS_REGISTRY_LINK
          ? "link_proposed"
          : "registry_only_traceability"
        : "already_linked",
      registryApprovedAfter: true,
    });
  }

  return {
    stage1Candidates: candidates.length,
    promotions,
    presentationLinks,
    linkAudit,
  };
}

function projectPostCleanupState(presentationRows, registryAssets, plans) {
  const rows = presentationRows.map((r) => ({ ...r }));
  for (const patch of plans.openingCleanup.hidePatches) {
    const row = rows.find((x) => x.recordId === patch.recordId);
    if (row) {
      row.visible = false;
      row.externalDisplayStatus = HIDE_DISPLAY;
    }
  }
  for (const patch of plans.openingCleanup.calaLabelPatches) {
    const row = rows.find((x) => x.recordId === patch.recordId);
    if (row) {
      row.title = patch.fields.Title;
      row.body = patch.fields.Body;
    }
  }
  for (const link of plans.registryPlan.presentationLinks) {
    const row = rows.find((x) => x.recordId === link.recordId);
    if (row) row.registryLinkIds = link.fields[PRESENTATION_REGISTRY_LINK_FIELD];
  }

  const registries = registryAssets.map((a) => ({ ...a }));
  for (const promo of plans.registryPlan.promotions) {
    const asset = registries.find((x) => x.id === promo.registryRecordId);
    if (asset) {
      asset.explorerUsePermission = "Approved For Explorer";
      asset.usageReviewStatus = "Usage Review Complete";
      asset.assetStatus = ASSET_STATUS.APPROVED_EXPLORER;
      asset.validationStatus = "Valid for Slot";
      if (promo.fields?.[MAP_BRAND_ASSET.sourcePageUrl]) {
        asset.sourcePageUrl = promo.fields[MAP_BRAND_ASSET.sourcePageUrl];
      }
      if (promo.fields?.[MAP_BRAND_ASSET.sourceUrl]) {
        asset.sourceUrl = promo.fields[MAP_BRAND_ASSET.sourceUrl];
      }
      if (promo.fields?.[MAP_BRAND_ASSET.sourceBasis]) {
        asset.sourceBasis = promo.fields[MAP_BRAND_ASSET.sourceBasis];
      }
    }
  }
  return { rows, registries };
}

function projectBrandApiForCleanup(brandApi, projectedRows) {
  if (!brandApi?.brandExplorer?.blocks) return brandApi;
  const visibleById = new Map(
    projectedRows.filter((r) => r.visible).map((r) => [r.recordId, r])
  );
  const blocks = brandApi.brandExplorer.blocks
    .filter((b) => visibleById.has(b.recordId))
    .map((b) => {
      const row = visibleById.get(b.recordId);
      return row
        ? {
            ...b,
            title: row.title || b.title,
            body: row.body || b.body,
            meta: row.meta || b.meta,
          }
        : b;
    });
  return {
    ...brandApi,
    brandExplorer: {
      ...brandApi.brandExplorer,
      blocks,
    },
  };
}

function validateApplyBlockers({
  flags,
  brandConfig,
  openingAudit,
  openingCleanup,
  registryPlan,
  companyValidatedBefore,
  brandBasics,
}) {
  const blockers = [];

  if (flags.apply) {
    if (!flags.approveCleanup) blockers.push("missing_approve_brand_explorer_v35E_design_hotels_draft_cleanup");
    if (!flags.founderApprovedAssets) blockers.push("missing_founder_approved_design_hotels_draft_assets");
    if (!flags.confirmCalaFirst) blockers.push("missing_confirm_cala_property_examples_first");
    if (!flags.confirmNoUsFallback) {
      blockers.push("missing_confirm_no_us_fallback_when_three_cala_examples_exist");
    }
    if (!flags.confirmNoValidation) blockers.push("missing_confirm_no_company_validation_claim");
    if (!flags.confirmNoActiveApproval) blockers.push("missing_confirm_no_active_profile_approval");
    if (!flags.confirmNoSummaryUrl) blockers.push("missing_confirm_no_summary_url_field");
    if (!flags.confirmDesignHotelsOnly) blockers.push("missing_confirm_design_hotels_only");
  }

  if (shouldBlockUsFallbackForBrand(brandConfig, DESIGN_HOTELS_PROPERTY_CATALOG)) {
    if (openingCleanup.calaSelection.usFallbackUsed) {
      blockers.push("us_fallback_would_be_used_despite_three_cala_examples");
    }
    if (openingCleanup.calaLabelPatches.some((p) => /u\.s\. property example/i.test(p.fields.Title))) {
      blockers.push("us_property_label_would_be_applied");
    }
  }

  if (countCalaPropertyExamples(DESIGN_HOTELS_PROPERTY_CATALOG) < 3) {
    blockers.push("fewer_than_three_cala_examples_in_catalog");
  }

  const stillVisibleDuplicates = DUPLICATE_OPENING_RECORD_IDS.filter((id) => {
    const row = openingAudit.visible.find((r) => r.recordId === id);
    return row && !openingCleanup.hidePatches.some((p) => p.recordId === id);
  });
  if (stillVisibleDuplicates.length && !flags.apply) {
    blockers.push(`duplicate_openings_still_visible:${stillVisibleDuplicates.join(",")}`);
  }

  if (flags.apply && !flags.founderApprovedAssets && registryPlan.promotions.length) {
    blockers.push("registry_promotion_blocked_without_founder_gate");
  }

  const cv = companyValidatedSnapshot(brandBasics);
  if (cv.companyValidated !== companyValidatedBefore.companyValidated) {
    blockers.push("company_validated_would_change");
  }

  if (openingAudit.visibleCount > 3 && openingCleanup.hidePatches.length === 0) {
    blockers.push("duplicate_property_rows_would_remain_visible");
  }

  return blockers;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Design Hotels Draft Cleanup ${V35E_VERSION}`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}**`);
  lines.push("");
  lines.push("## 1. Opening rows before/after");
  lines.push(`- Before visible: **${report.openingRowsBefore.visibleCount}** / total ${report.openingRowsBefore.total}`);
  lines.push(`- After (projected) visible: **${report.openingRowsAfter.visibleCount}**`);
  lines.push("");
  lines.push("## 2. Duplicate rows hidden");
  for (const patch of report.duplicateRowsHidden) {
    lines.push(`- \`${patch.recordId}\` — ${patch.reason}`);
  }
  if (!report.duplicateRowsHidden.length) lines.push("- (none proposed)");
  lines.push("");
  lines.push("## 3. Visible CALA property examples");
  for (const row of report.visibleCalaPropertyExamples) {
    lines.push(`- **${row.propertyName}** (\`${row.recordId}\`) — ${row.title}`);
  }
  lines.push("");
  lines.push("## 4. Section label before/after");
  lines.push(`- Before: ${report.sectionLabelBefore || "(mixed / U.S. labels)"}`);
  lines.push(`- After: **${report.sectionLabelAfter}**`);
  lines.push("");
  lines.push("## 5. Registry candidates promoted");
  lines.push(`- Stage 1 candidates found: ${report.registryCandidatesFound}`);
  lines.push(`- Scenario registry backfill creates: ${report.scenarioRegistryCreates?.length ?? 0}`);
  lines.push(`- Promotions planned: **${report.registryPromotions.length}**`);
  for (const promo of report.registryPromotions) {
    lines.push(`- \`${promo.registryRecordId}\` → ${promo.slotKey} (presentation \`${promo.presentationRecordId}\`)`);
  }
  lines.push("");
  lines.push("## 6. Presentation–registry link audit");
  for (const item of report.presentationRegistryLinkAudit) {
    lines.push(`- ${item.slotKey} \`${item.presentationRecordId}\` ↔ \`${item.registryRecordId || "?"}\` — ${item.status}`);
  }
  lines.push("");
  lines.push("## 7. Company Validated untouched");
  lines.push(`- Before: ${report.companyValidatedBefore.companyValidated}`);
  lines.push(`- After (projected): ${report.companyValidatedAfter.companyValidated}`);
  lines.push(`- Untouched: **${report.companyValidatedUntouched ? "yes" : "NO"}**`);
  lines.push("");
  lines.push("## 8. Active-profile approval untouched");
  lines.push(`- Active profile approved: **${report.activeProfileApproved}** (must remain false)`);
  lines.push("");
  lines.push("## 9. Expected founder visual review");
  lines.push(`- Pass (projected): **${report.expectedFounderReview.pass ? "yes" : "no"}**`);
  for (const check of report.expectedFounderReview.checks || []) {
    lines.push(`- ${check.pass ? "PASS" : "FAIL"} — ${check.label} (${check.detail})`);
  }
  lines.push("");
  lines.push("## 10. Expected Final QA");
  lines.push(`- Readiness: **${report.expectedFinalQa?.scores?.overallActiveProfileReadiness || "unknown"}**`);
  lines.push(`- Defects: ${report.expectedFinalQa?.defectCounts?.total ?? "n/a"}`);
  lines.push("");
  lines.push("## 11. Apply command");
  if (report.exactApplyCommand) {
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  } else {
    lines.push("- Blocked — resolve apply blockers first");
  }
  if (report.applyBlockers.length) {
    lines.push("");
    lines.push("## Apply blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }
  return lines.join("\n");
}

export async function buildDesignHotelsDraftCleanupV35EReport(options = {}) {
  const flags = options.flags || parseCliFlags(options.argv);
  const brand = TARGET_BRAND;
  const brandConfig = getActiveProfileBrandConfig(brand.slug);
  if (!brandConfig) throw new Error(`Missing brand config: ${brand.slug}`);
  if (flags.brandArg !== brand.slug && flags.brandArg !== brand.recordId) {
    throw new Error(`v35E supports Design Hotels only; got: ${flags.brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const [brandBasics, presentationRows, registryAssetsRaw, brandApi] = await Promise.all([
    fetchBrandBasics(brand.recordId),
    listPresentationRows(baseId, apiKey, brand.recordId, brand.name),
    listRegistryAssetsForBrand(brand.recordId).catch(() => []),
    fetchBrandApiShape(brand.recordId),
  ]);

  const registryAssets = registryAssetsRaw;
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);

  const openingRowsBefore = auditOpeningsRows(presentationRows);
  const openingCleanup = planOpeningCleanup(presentationRows, brandConfig);
  const hideRecordIds = openingCleanup.hidePatches.map((p) => p.recordId);
  const visualRowsAfterHide = presentationRows.filter(
    (r) => r.visible && !hideRecordIds.includes(r.recordId)
  );
  const scenarioRegistryCreates = planScenarioRegistryCreates(
    visualRowsAfterHide,
    registryAssets,
    brandConfig
  );
  const projectedScenarioRegistry = scenarioRegistryCreates.map((create, idx) => ({
    id: `v35e-scenario-draft-${idx + 1}`,
    assetName: create.staged.assetName,
    recommendedExplorerSlot: create.slotKey,
    explorerUsePermission: "Approved For Explorer",
    usageReviewStatus: "Usage Review Complete",
    assetStatus: ASSET_STATUS.APPROVED_EXPLORER,
    validationStatus: "Valid for Slot",
    stagingRunId: STAGING_RUN_ID,
    sourceUrl: create.staged.sourceUrl,
    sourcePageUrl: create.staged.sourcePageUrl,
    sourceNotes: create.staged.sourceNotes,
    reviewNotes: create.staged.reviewNotes,
  }));
  const registryAssetsForPlan = [...registryAssets, ...projectedScenarioRegistry];
  const registryPlan = planRegistryPromotion(registryAssetsForPlan, presentationRows, brandConfig, {
    hideRecordIds,
  });

  const plans = { openingCleanup, registryPlan, scenarioRegistryCreates };
  const projected = projectPostCleanupState(presentationRows, [...registryAssets, ...projectedScenarioRegistry], plans);
  const openingRowsAfter = auditOpeningsRows(projected.rows);

  const visibleCalaPropertyExamples = openingCleanup.calaLabelPatches.map((p) => ({
    recordId: p.recordId,
    propertyName: p.propertyName,
    title: p.fields.Title,
  }));

  const applyBlockers = validateApplyBlockers({
    flags,
    brandConfig,
    openingAudit: openingRowsBefore,
    openingCleanup,
    registryPlan,
    companyValidatedBefore,
    brandBasics,
  });

  const dryRunClean = applyBlockers.length === 0;
  const canApply = flags.apply && dryRunClean;

  const applyResults = {
    presentationHidden: [],
    presentationRelabeled: [],
    presentationLinked: [],
    registryPromoted: [],
    errors: [],
  };

  if (canApply) {
    for (const patch of [...openingCleanup.hidePatches, ...openingCleanup.calaLabelPatches]) {
      for (const key of Object.keys(patch.fields)) {
        if (BLOCKED_PRESENTATION_FIELDS.has(key)) {
          applyResults.errors.push({ recordId: patch.recordId, message: `blocked_field:${key}` });
        }
      }
    }

    if (!applyResults.errors.length && scenarioRegistryCreates.length) {
      try {
        const parentCompany =
          nz(brandBasics?.fields?.["Parent Company"]) || brandConfig.parentCompany;
        const scenarioApply = await applyRegistryRecords({
          brandRecordId: brand.recordId,
          parentCompany,
          stagedAssets: scenarioRegistryCreates.map((c) => c.staged),
          stagingRunId: STAGING_RUN_ID,
        });
        applyResults.scenarioRegistryCreated = (scenarioApply.created || []).map((r) => r.recordId);
        await new Promise((r) => setTimeout(r, 400));
      } catch (err) {
        applyResults.errors.push({ stage: "scenario_registry_create", message: err.message });
      }
    }

    let registryAssetsLive = registryAssets;
    if (canApply && applyResults.scenarioRegistryCreated?.length) {
      registryAssetsLive = await listRegistryAssetsForBrand(brand.recordId);
      const livePlan = planRegistryPromotion(registryAssetsLive, presentationRows, brandConfig, {
        hideRecordIds,
      });
      registryPlan.promotions = livePlan.promotions;
      registryPlan.presentationLinks = livePlan.presentationLinks;
    }

    if (!applyResults.errors.length) {
      for (const patch of openingCleanup.hidePatches) {
        try {
          const { res, json } = await airtableFetch(
            baseId,
            apiKey,
            PRESENTATION_TABLE,
            { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
            patch.recordId
          );
          if (!res.ok) throw new Error(json.error?.message || `Hide failed: ${res.status}`);
          applyResults.presentationHidden.push(patch.recordId);
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({ recordId: patch.recordId, message: err.message });
        }
      }

      for (const patch of openingCleanup.calaLabelPatches) {
        try {
          const { res, json } = await airtableFetch(
            baseId,
            apiKey,
            PRESENTATION_TABLE,
            { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
            patch.recordId
          );
          if (!res.ok) throw new Error(json.error?.message || `Relabel failed: ${res.status}`);
          applyResults.presentationRelabeled.push(patch.recordId);
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({ recordId: patch.recordId, message: err.message });
        }
      }

      for (const promo of registryPlan.promotions) {
        try {
          const { res, json } = await airtableFetch(
            baseId,
            apiKey,
            BRAND_ASSET_REGISTRY_TABLE,
            { method: "PATCH", body: JSON.stringify({ fields: promo.fields, typecast: true }) },
            promo.registryRecordId
          );
          if (!res.ok) throw new Error(json.error?.message || `Registry PATCH failed: ${res.status}`);
          applyResults.registryPromoted.push(promo.registryRecordId);
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({ recordId: promo.registryRecordId, message: err.message });
        }
      }

      for (const link of registryPlan.presentationLinks) {
        try {
          const { res, json } = await airtableFetch(
            baseId,
            apiKey,
            PRESENTATION_TABLE,
            { method: "PATCH", body: JSON.stringify({ fields: link.fields, typecast: true }) },
            link.recordId
          );
          if (!res.ok) {
            if (/unknown field name/i.test(json.error?.message || "")) {
              applyResults.presentationLinkSkipped = applyResults.presentationLinkSkipped || [];
              applyResults.presentationLinkSkipped.push({
                recordId: link.recordId,
                reason: "presentation_registry_link_field_unavailable",
              });
              continue;
            }
            throw new Error(json.error?.message || `Link failed: ${res.status}`);
          }
          applyResults.presentationLinked.push(link.recordId);
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({ recordId: link.recordId, message: err.message });
        }
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(brand.recordId) : brandBasics;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  const companyValidatedUntouched =
    companyValidatedBefore.companyValidated === companyValidatedAfter.companyValidated &&
    companyValidatedBefore.companyValidationDate === companyValidatedAfter.companyValidationDate;

  const projectedBrandApi = projectBrandApiForCleanup(brandApi, projected.rows);
  const factoryRulesProjected = evaluateAllFactoryRules({
    brandApi: projectedBrandApi,
    presentationRows: projected.rows,
    registryAssets: projected.registries,
    brandConfig,
  });
  const expectedFounderReview = evaluateFounderVisualReview({
    factoryRules: factoryRulesProjected,
    brandBasics: brandBasicsAfter,
    companyValidatedBefore: companyValidatedBefore.companyValidated,
  });

  const expectedFinalQa = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: brand.slug,
  }).catch(() => null);
  const expectedCompleteBuild = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: brand.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const expectedVisualDefects = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: brand.slug,
  }).catch(() => null);

  const report = {
    version: V35E_VERSION,
    stagingRunId: STAGING_RUN_ID,
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand,
    flags,
    calaPropertyRule: {
      version: "v35E",
      calaCount: countCalaPropertyExamples(DESIGN_HOTELS_PROPERTY_CATALOG),
      tierUsed: openingCleanup.calaSelection.tierUsed,
      usFallbackBlocked: shouldBlockUsFallbackForBrand(brandConfig, DESIGN_HOTELS_PROPERTY_CATALOG),
      usFallbackUsed: openingCleanup.calaSelection.usFallbackUsed,
    },
    openingRowsBefore,
    openingRowsAfter,
    duplicateRowsHidden: openingCleanup.hidePatches,
    visibleCalaPropertyExamples,
    sectionLabelBefore: openingRowsBefore.visible[0]?.meta || null,
    sectionLabelAfter: openingCleanup.sectionLabel,
    registryCandidatesFound: registryPlan.stage1Candidates,
    scenarioRegistryCreates,
    registryPromotions: registryPlan.promotions,
    presentationRegistryLinkAudit: registryPlan.linkAudit,
    presentationLinkPatches: registryPlan.presentationLinks,
    calaLabelPatches: openingCleanup.calaLabelPatches,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    activeProfileApproved: false,
    expectedFounderReview,
    factoryRulesProjected,
    expectedFinalQa: expectedFinalQa
      ? {
          scores: expectedFinalQa.scores,
          defectCounts: expectedFinalQa.defectCounts,
        }
      : null,
    expectedCompleteBuild: expectedCompleteBuild
      ? { readyForActiveProfile: expectedCompleteBuild.readyForActiveProfile, readinessBand: expectedCompleteBuild.brandResults?.[0]?.readinessBand }
      : null,
    expectedVisualDefects: expectedVisualDefects
      ? { defectCounts: expectedVisualDefects.defectCounts, comparableToCurio: expectedVisualDefects.comparableToCurio }
      : null,
    applyBlockers,
    dryRunClean,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    applyResults: canApply ? applyResults : null,
    airtableModified: canApply && applyResults.errors.length === 0,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export { parseCliFlags, buildMarkdown };
