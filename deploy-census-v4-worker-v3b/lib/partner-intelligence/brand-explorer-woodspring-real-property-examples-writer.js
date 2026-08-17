/**
 * Brand Explorer WoodSpring Real Property Examples Rebuild v33C-R1.
 *
 * Replaces generic footprint.openings platform cards with U.S. WoodSpring
 * property examples, aligns registry + images, and hides leftover platform rows.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-real-property-examples-writer-v33C-R1.md
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
} from "./brand-asset-registry-workflow.js";
import { ASSET_STATUS, ASSET_TYPE, SOURCE_BASIS } from "./brand-asset-pr-package-governance.js";
import {
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  buildWoodspringRegistryStagedAsset,
  TARGET_BRAND,
} from "./brand-explorer-woodspring-source-registry-readiness-writer.js";
import {
  isDoNotUseRecord,
  normalizeRegistryRecordExtended,
  parsePresentationRowIdFromNotes,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import {
  extractOgImageFromHtml,
  fetchDurablePropertyImage,
  isChoiceAccessDeniedHtml,
  isTemporaryAirtableUrl,
} from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { validateV32gR1RegistryWritePayload } from "./brand-explorer-everhome-existing-image-approval-recognition-writer.js";
import {
  buildFounderApprovedRegistryPatch,
  detectWoodspringWrongBrandRisk,
} from "./brand-explorer-woodspring-visual-completion-writer.js";
import { MAP_VISUAL_SLOT, VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33C-R1";
export const STAGING_RUN_ID = "v33C-R1-woodspring-real-property-examples";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-real-property-examples-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-real-property-examples-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-real-property-examples-writer-v33C-R1.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33C-R1-woodspring-real-property-examples";
export const APPLY_FLAG_FOUNDER = "--founder-approved-woodspring-property-example-images";
export const APPLY_FLAG_OFFICIAL_ONLY = "--confirm-official-source-images-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_SUMMARY_URL = "--confirm-no-summary-url-field";
export const APPLY_FLAG_NO_OTHER_SECTIONS =
  "--confirm-no-momentum-gallery-proof-standard-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export { TARGET_BRAND };
export const PROTECTED_BRAND_SLUGS = Object.freeze(["everhome-suites", "suburban-studios"]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";
const HIDE_DISPLAY_STATUS = "Do Not Display";

const GENERIC_OPENING_TITLE_RE =
  /footprint example|portfolio discovery|development prototype|platform example|extended-stay platform/i;
const METADATA_STYLE_RE =
  /listed on choicehotels|active property page|consumer site|consumer path|property listing page|source data|metadata|census|extracted from|booking path|source[- ]?capture/i;
const BLOCKED_OWNER_FACING_RE =
  /\bfdd\b|\bitem\s*19\b|franchise disclosure|recent opening|newly opened|\beverhome\b|\bsuburban\b/i;
const PERFORMANCE_RE =
  /\b(roi|irr|cap rate|noi|revpar|adr forecast|guaranteed returns?|published %)\b/i;
const CHOICE_LOGO_IMAGE_RE =
  /choice[-_]?hotels?[-_]?social|choice[-_]?logo|logo[-_]?mark|image-choice-logo|ws-suites-logo/i;

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
]);

const PROTECTED_SLOTS = new Set([
  MOMENTUM_SLOT,
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "overview.proof.1",
  "overview.proof.2",
  "overview.proof.3",
  "overview.proof.4",
  "overview.proof.5",
  "overview.proof.6",
  "overview.proof_operator",
  "standards.last_reviewed",
  "standards.source_confidence",
  "standards.conversion",
]);

/** U.S. property examples — official Choice property URLs from sitemap extract. */
export const WOODSPRING_PROPERTY_CATALOG = Object.freeze([
  {
    presentationRecordId: "recI3cbO8mOhEpo1W",
    propertyName: "WoodSpring Suites Orlando",
    marketCity: "Orlando",
    stateRegion: "Florida",
    chips: "Extended-Stay, Florida, Kitchen-Equipped Suite",
    meta: "U.S. Property Example · Weekly-stay suite model",
    scenario: "PROPERTY EXAMPLE / MARKET FIT",
    teaser:
      "A WoodSpring property example that helps owners evaluate longer-stay demand, simple-suite positioning, and market fit within the Choice platform.",
    sourcePageUrl: "https://www.choicehotels.com/florida/orlando/woodspring-hotels/flf21",
    titleKeywords: ["orlando", "woodspring"],
    fallbackImagePreference: ["hero", "suite"],
  },
  {
    presentationRecordId: "recpNB0KoPq6y3Mhs",
    propertyName: "WoodSpring Suites Charlotte",
    marketCity: "Charlotte",
    stateRegion: "North Carolina",
    chips: "Extended-Stay, North Carolina, Weekly Demand",
    meta: "U.S. Property Example · Economy extended-stay positioning",
    scenario: "PROPERTY EXAMPLE / COMPETITIVE CONTEXT",
    teaser:
      "A U.S. WoodSpring example for owners comparing economy extended-stay positioning, operating simplicity, and local demand drivers.",
    sourcePageUrl: "https://www.choicehotels.com/north-carolina/charlotte/woodspring-hotels/ncb10",
    titleKeywords: ["charlotte", "woodspring"],
    fallbackImagePreference: ["kitchen", "suite"],
  },
  {
    presentationRecordId: "rec4Eqp9lwXSP7UQE",
    propertyName: "WoodSpring Suites Raleigh",
    marketCity: "Raleigh",
    stateRegion: "North Carolina",
    chips: "Extended-Stay, North Carolina, Kitchen-Equipped Suite",
    meta: "U.S. Property Example · Extended-stay supply reference",
    scenario: "PROPERTY EXAMPLE / SUITE MODEL",
    teaser:
      "A property-level reference point for owners assessing WoodSpring's kitchen-equipped suite model and competitive extended-stay supply.",
    sourcePageUrl: "https://www.choicehotels.com/north-carolina/raleigh/woodspring-hotels/nc936",
    titleKeywords: ["raleigh", "woodspring"],
    fallbackImagePreference: ["pet", "clean"],
  },
]);

export const HIDDEN_GENERIC_OPENING = Object.freeze({
  presentationRecordId: "recdC5lflCPCtjbkr",
  previousTitle: "Choice Extended-Stay Platform Example",
  reason: "generic_platform_example_replaced_by_property_cards",
});

const KNOWN_OFFICIAL_WOODSPRING_BRAND_IMAGES = Object.freeze([
  {
    imageUrl:
      "https://www-media.woodspring.com/v1/media/images/wsweb/WS_WebHero_1280x500+%281%29.jpg",
    sourcePageUrl: "https://www.woodspring.com/",
    kind: "hero",
    label: "woodspring_web_hero",
  },
  {
    imageUrl:
      "https://www-media.woodspring.com/v1/media/images/wsweb/general/Suite_Dark_1280x500.jpg",
    sourcePageUrl: "https://www.woodspring.com/",
    kind: "suite",
    label: "woodspring_suite_dark",
  },
  {
    imageUrl:
      "https://www-media.woodspring.com/v1/media/images/wsweb/general/Kitchen_Dark_1280x500.jpg",
    sourcePageUrl: "https://www.woodspring.com/",
    kind: "kitchen",
    label: "woodspring_kitchen_dark",
  },
  {
    imageUrl:
      "https://www-media.woodspring.com/v1/media/images/wsweb/offers/Pet-Friendly/WoodSpring_Hotels_Pet-Friendly_Hotels_Extended_Stay_Hotels_Module_OPT_1280x500.jpg",
    sourcePageUrl: "https://www.woodspring.com/",
    kind: "pet",
    label: "woodspring_pet_friendly",
  },
  {
    imageUrl:
      "https://www-media.woodspring.com/v1/media/images/wsweb/WS_Simply-Clean_1280x500.png",
    sourcePageUrl: "https://www.woodspring.com/",
    kind: "clean",
    label: "woodspring_simply_clean",
  },
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/independent-census-choice-property-url-extract-cala-2026-05-20.csv",
  "reports/brand-explorer-woodspring-openings-momentum-build-writer.json",
  "reports/brand-explorer-woodspring-visual-registry-recovery-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "live WoodSpring footprint.openings / registry / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-real-property-examples-writer.js",
  "scripts/brand-explorer-woodspring-real-property-examples-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "public/js/brand-explorer-atelier-from-api.js",
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FOUNDER_REVIEW_NOTES =
  "v33C-R1 founder-approved — official WoodSpring property example image for Explorer openings.";
const FOUNDER_SOURCE_NOTES_SUFFIX =
  "Founder-approved official WoodSpring property example; durable property source page on file.";

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

export function v33cR1WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-woodspring-real-property-examples-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v33C-R1`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33C-R1 supports WoodSpring Suites only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

function extractUrlFromText(text) {
  const m = nz(text).match(/https?:\/\/[^\s<>"')]+/i);
  return m ? m[0].replace(/[.,;)]+$/, "") : "";
}

export function buildOpeningBody({ chips, location, meta, scenario, teaser, sourceUrl }) {
  return [chips, location, meta, scenario, teaser, sourceUrl].filter(Boolean).join("\n\n");
}

export function buildPropertyOpeningTitle(catalog) {
  return `WoodSpring Suites ${catalog.marketCity} — U.S. Property Example`;
}

export function buildPropertyOpeningCopy(catalog) {
  const title = buildPropertyOpeningTitle(catalog);
  const location = `${catalog.marketCity}, ${catalog.stateRegion}`;
  const body = buildOpeningBody({
    chips: catalog.chips,
    location,
    meta: catalog.meta,
    scenario: catalog.scenario,
    teaser: catalog.teaser,
    sourceUrl: catalog.sourcePageUrl,
  });
  return { title, body, location };
}

export function classifyOpeningExampleType(row) {
  const title = nz(row.title);
  if (GENERIC_OPENING_TITLE_RE.test(title)) return "generic_platform_example";
  if (/u\.s\. property example|property example/i.test(title)) return "property_example";
  return "unknown";
}

export function classifyOpeningRecommendation(row) {
  const type = classifyOpeningExampleType(row);
  if (row.recordId === HIDDEN_GENERIC_OPENING.presentationRecordId) {
    return { action: "hide", reason: "generic_platform_card" };
  }
  const catalog = WOODSPRING_PROPERTY_CATALOG.find(
    (c) => c.presentationRecordId === row.recordId
  );
  if (catalog) return { action: "rewrite_as_property_example", reason: "mapped_property_catalog" };
  if (type === "generic_platform_example") return { action: "hide", reason: "unmapped_generic_card" };
  if (type === "property_example") return { action: "keep", reason: "already_property_example" };
  return { action: "review", reason: "unclassified_opening_row" };
}

export function isDisallowedPropertyImageUrl(url) {
  const u = nz(url).toLowerCase();
  if (!u) return true;
  if (/\beverhome\b|\bsuburban\b/i.test(u)) return true;
  if (CHOICE_LOGO_IMAGE_RE.test(u)) return true;
  if (detectWoodspringWrongBrandRisk(u, getDiscoveryBrandConfig(TARGET_BRAND.slug))) return true;
  return false;
}

function validateOpeningCopy(text) {
  const errors = [];
  if (METADATA_STYLE_RE.test(text)) errors.push("metadata_language");
  if (BLOCKED_OWNER_FACING_RE.test(text)) errors.push("blocked_owner_facing");
  if (PERFORMANCE_RE.test(text)) errors.push("performance_claim");
  if (scanCopySafety(text).length) errors.push(`copy_safety:${scanCopySafety(text).join(",")}`);
  return errors;
}

function validatePresentationPatch(fields, { allowImage = false } = {}) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
    if (!allowImage && (key === "Image" || key === "Scenario Image")) {
      errors.push(`blocked_image_field:${key}`);
    }
  }
  const combined = `${fields.Title || ""}\n${fields.Body || ""}`;
  errors.push(...validateOpeningCopy(combined));
  const url = extractUrlFromText(fields.Body);
  if (url && isTemporaryAirtableUrl(url)) errors.push("temporary_source_url_in_body");
  return errors;
}

function validateVisibilityPatch(fields) {
  const keys = Object.keys(fields);
  if (keys.length !== 1 || fields["External Display Status"] !== HIDE_DISPLAY_STATUS) {
    return ["visibility_patch_must_only_hide"];
  }
  return [];
}

function normalizeUrlKey(url) {
  return nz(url).split("?")[0].toLowerCase();
}

export async function discoverWoodspringOfficialBrandImages() {
  const discovered = [...KNOWN_OFFICIAL_WOODSPRING_BRAND_IMAGES];
  const seen = new Set(discovered.map((img) => normalizeUrlKey(img.imageUrl)));
  const pageUrl = "https://www.woodspring.com/";
  try {
    const res = await fetch(pageUrl, {
      headers: { "User-Agent": "DealalityBrandExplorer/1.0" },
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) return discovered;
    const html = await res.text();
    const matches = [...html.matchAll(/https?:\/\/[^\s"'<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^\s"'<>]*)?/gi)];
    for (const match of matches) {
      const imageUrl = match[0];
      const key = normalizeUrlKey(imageUrl);
      if (seen.has(key) || isDisallowedPropertyImageUrl(imageUrl)) continue;
      seen.add(key);
      const label = imageUrl.toLowerCase();
      discovered.push({
        imageUrl,
        sourcePageUrl: pageUrl,
        label,
        kind: label.includes("kitchen")
          ? "kitchen"
          : label.includes("suite")
            ? "suite"
            : label.includes("hero")
              ? "hero"
              : label.includes("pet")
                ? "pet"
                : label.includes("clean")
                  ? "clean"
                  : "general",
      });
    }
  } catch {
    // discovery is best-effort; known official URLs remain
  }
  return discovered;
}

function pickFallbackBrandImage(catalog, brandImages, usedKeys) {
  const prefs = catalog.fallbackImagePreference || [];
  for (const pref of prefs) {
    const candidate = brandImages.find(
      (img) => img.kind === pref && !usedKeys.has(normalizeUrlKey(img.imageUrl))
    );
    if (candidate) return candidate;
  }
  return (
    brandImages.find((img) => !usedKeys.has(normalizeUrlKey(img.imageUrl))) || null
  );
}

async function resolvePropertyImagePlan(catalog, brandImages, usedImageKeys) {
  const propertyFetch = await fetchDurablePropertyImage({
    sourcePageUrl: catalog.sourcePageUrl,
    titleKeywords: catalog.titleKeywords,
  });

  if (propertyFetch.ok && !isDisallowedPropertyImageUrl(propertyFetch.imageUrl)) {
    return {
      ok: true,
      imageUrl: propertyFetch.imageUrl,
      imageSourcePageUrl: catalog.sourcePageUrl,
      imageSource: "property_page_og_image",
      imageHonestyNote: null,
      usesChoiceLogo: false,
    };
  }

  const fallback = pickFallbackBrandImage(catalog, brandImages, usedImageKeys);
  if (!fallback) {
    return {
      ok: false,
      error: propertyFetch.error || "no_safe_fallback_image",
      imageSource: "unresolved",
    };
  }

  return {
    ok: true,
    imageUrl: fallback.imageUrl,
    imageSourcePageUrl: fallback.sourcePageUrl,
    imageSource: "official_woodspring_brand_image",
    imageHonestyNote:
      "Property consumer-page image unavailable from official source; official WoodSpring brand visual used for property example context.",
    usesChoiceLogo: false,
  };
}

function findRegistryForRow(registryAssets, row) {
  if (row.registryLink) {
    const byPresentationLink = registryAssets.find((a) => a.id === row.registryLink);
    if (byPresentationLink) return byPresentationLink;
  }
  const byNotes = registryAssets.find(
    (a) =>
      parsePresentationRowIdFromNotes(a.sourceNotes) === row.recordId ||
      nz(a.sourceNotes).includes(row.recordId)
  );
  if (byNotes) return byNotes;
  return (
    registryAssets.find(
      (a) =>
        nz(a.recommendedExplorerSlot) === OPENINGS_SLOT &&
        nz(a.sourceNotes).includes(row.recordId)
    ) || null
  );
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
  if (staged.sourcePageUrl && !isTemporaryAirtableUrl(staged.sourcePageUrl)) {
    fields[MAP_BRAND_ASSET.sourcePageUrl] = staged.sourcePageUrl;
  }
  return fields;
}

function buildRegistryFieldsForProperty({
  catalog,
  row,
  brandConfig,
  parentCompany,
  imagePlan,
}) {
  const assetName = `${catalog.propertyName} — U.S. Property Example — PR / Opening Image`;
  const validationNotes = imagePlan.imageHonestyNote
    ? `${FOUNDER_REVIEW_NOTES} ${imagePlan.imageHonestyNote}`
    : FOUNDER_REVIEW_NOTES;
  const staged = buildWoodspringRegistryStagedAsset({
    row: {
      ...row,
      title: buildPropertyOpeningTitle(catalog),
      imageUrl: imagePlan.imageUrl,
      body: buildPropertyOpeningCopy(catalog).body,
    },
    brandConfig,
    stagingRunId: STAGING_RUN_ID,
    wrongBrandRisk: null,
  });
  staged.assetName = assetName;
  staged.assetStatus = ASSET_STATUS.APPROVED_EXPLORER;
  staged.explorerUsePermission = "Approved For Explorer";
  staged.usageReviewStatus = "Usage Review Complete";
  staged.validationStatus = "Valid for Slot";
  staged.validationNotes = validationNotes;
  staged.reviewNotes = FOUNDER_REVIEW_NOTES;
  staged.sourceNotes = `Linked presentation row ${row.recordId} (${OPENINGS_SLOT}). ${FOUNDER_SOURCE_NOTES_SUFFIX}`;
  staged.stagingRunId = STAGING_RUN_ID;
  staged.relatedPropertyName = catalog.propertyName;
  staged.propertyConfirmed = "Yes";
  staged.calaRelevant = "No";
  staged.slotPurpose = `WoodSpring ${OPENINGS_SLOT} — U.S. property example image`;
  staged.explorerSection = VISUAL_SLOT.RECENT_OPENINGS;
  staged.sourcePageUrl = catalog.sourcePageUrl;
  staged.sourceUrl = imagePlan.imageUrl;
  staged.sourceBasis = SOURCE_BASIS.RENDERED_OFFICIAL;
  return mapStagedToRegistryFields(staged, TARGET_BRAND.recordId, parentCompany);
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
    const imageAtt = f.Image?.[0];
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      sortOrder: f["Sort Order"],
      externalDisplayStatus: nz(f["External Display Status"]),
      visible:
        nz(f["External Display Status"]).toLowerCase() !== HIDE_DISPLAY_STATUS.toLowerCase(),
      hasImage: Array.isArray(f.Image) && f.Image.length > 0,
      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",
      summaryUrl: nz(f["Summary URL"] || f["View Summary URL"]),
      sourcePageUrl: nz(f["Source Page URL"]),
      registryLink: Array.isArray(f["Brand Asset Registry"]) ? f["Brand Asset Registry"][0] : null,
    };
  });
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

function registryTableName() {
  return process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID || BRAND_ASSET_REGISTRY_TABLE;
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

function auditCalaAvailability() {
  const csvPath = path.join(ROOT, "reports/independent-census-choice-property-url-extract-cala-2026-05-20.csv");
  if (!fs.existsSync(csvPath)) {
    return {
      calaPropertyExamplesAvailable: false,
      note: "CALA census extract not found; defaulting to U.S. property fallback.",
    };
  }
  const text = fs.readFileSync(csvPath, "utf8");
  const calaWoodspring = text
    .split("\n")
    .filter((line) => /woodspring/i.test(line) && !/excluded_non_cala/i.test(line));
  return {
    calaPropertyExamplesAvailable: calaWoodspring.length > 0,
    calaWoodspringUrlCount: calaWoodspring.length,
    note:
      calaWoodspring.length > 0
        ? "CALA WoodSpring property URLs found in extract."
        : "No CALA WoodSpring property URLs in approved extract; U.S. property examples required.",
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-real-property-examples-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_OFFICIAL_ONLY,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_SUMMARY_URL,
    APPLY_FLAG_NO_OTHER_SECTIONS,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Real Property Examples v33C-R1");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## CALA availability");
  lines.push(`- ${report.calaAvailability.note}`);
  lines.push("");
  lines.push("## Current openings audit");
  for (const row of report.currentOpeningsAudit) {
    lines.push(
      `- \`${row.recordId}\` **${row.title}** — type: ${row.exampleType}; image: ${row.imageStatus}; action: ${row.recommendedAction}`
    );
  }
  lines.push("");
  lines.push("## Selected property examples");
  for (const ex of report.selectedPropertyExamples) {
    lines.push(
      `- **${ex.propertyName}** — ${ex.marketCity}, ${ex.stateRegion}; source: ${ex.sourcePageUrl}; image: ${ex.imageSource}`
    );
  }
  lines.push("");
  lines.push("## Before / after");
  for (const row of report.beforeAfterOpenings) {
    lines.push(`- \`${row.recordId}\`: ${row.beforeTitle} → ${row.afterTitle || "(hidden)"}`);
  }
  lines.push("");
  lines.push("## Registry");
  lines.push(`- Patches: **${report.registryPatches.length}**`);
  lines.push(`- Creates: **${report.registryCreates.length}**`);
  lines.push(`- Hidden rows: **${report.rowsHidden.length}**`);
  lines.push(`- Choice-logo property image used: **${report.choiceLogoPropertyImageUsed ? "yes" : "no"}**`);
  lines.push(`- Generic cards remain visible: **${report.genericCardsRemainVisible ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Expected QA");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Visual defects: ${report.expectedVisualDefectResult}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  if (report.applyBlockers.length) {
    lines.push("");
    lines.push("## Apply blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }
  return lines.join("\n");
}

export async function buildBrandExplorerWoodspringRealPropertyExamplesWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderApproved = false,
  officialImagesOnly = false,
  noValidationClaim = false,
  noSummaryUrl = false,
  noOtherSectionChanges = false,
  woodspringOnly = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = getDiscoveryBrandConfig(target.slug);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const parentCompany = nz(brandBasicsBefore?.fields?.["Parent Company"]) || "Choice Hotels International";

  const [presentationRows, registryAssetsRaw, brandApiBefore, brandImages] = await Promise.all([
    listPresentationRows(baseId, apiKey, target.recordId, target.name),
    listRegistryAssetsForBrand(target.recordId).catch(() => []),
    fetchBrandApiShape(target.recordId),
    discoverWoodspringOfficialBrandImages(),
  ]);

  const registryAssets = registryAssetsRaw.map(normalizeRegistryRecordExtended);
  const openingsRows = presentationRows.filter((r) => r.slotKey === OPENINGS_SLOT);
  const calaAvailability = auditCalaAvailability();

  const currentOpeningsAudit = openingsRows.map((row) => {
    const rec = classifyOpeningRecommendation(row);
    return {
      recordId: row.recordId,
      title: row.title,
      marketLocation: extractUrlFromText(row.body) || row.sourcePageUrl || "(from body/title)",
      bodyPreview: nz(row.body).slice(0, 180),
      sourceLink: extractUrlFromText(row.body),
      imageStatus: row.hasImage
        ? isTemporaryAirtableUrl(row.imageUrl)
          ? "temporary_airtable"
          : "present"
        : "missing",
      registryLink: findRegistryForRow(registryAssets, row)?.id || row.registryLink || null,
      exampleType: classifyOpeningExampleType(row),
      recommendedAction: rec.action,
      recommendedReason: rec.reason,
    };
  });

  const presentationPatches = [];
  const presentationImagePatches = [];
  const visibilityPatches = [];
  const registryPatches = [];
  const registryCreates = [];
  const beforeAfterOpenings = [];
  const selectedPropertyExamples = [];
  const imageSourceMapping = [];
  const safetyBlockers = [];
  const usedImageKeys = new Set();

  for (const catalog of WOODSPRING_PROPERTY_CATALOG) {
    const row = openingsRows.find((r) => r.recordId === catalog.presentationRecordId);
    if (!row) {
      safetyBlockers.push(`missing_opening_row:${catalog.presentationRecordId}`);
      continue;
    }

    const copy = buildPropertyOpeningCopy(catalog);
    const imagePlan = await resolvePropertyImagePlan(catalog, brandImages, usedImageKeys);
    if (!imagePlan.ok) {
      safetyBlockers.push(`no_safe_image:${catalog.presentationRecordId}:${imagePlan.error}`);
      continue;
    }
    if (isDisallowedPropertyImageUrl(imagePlan.imageUrl)) {
      safetyBlockers.push(`disallowed_image:${catalog.presentationRecordId}`);
      continue;
    }
    usedImageKeys.add(normalizeUrlKey(imagePlan.imageUrl));

    selectedPropertyExamples.push({
      ...catalog,
      imageUrl: imagePlan.imageUrl,
      imageSource: imagePlan.imageSource,
      imageSourcePageUrl: imagePlan.imageSourcePageUrl,
      imageHonestyNote: imagePlan.imageHonestyNote,
    });
    imageSourceMapping.push({
      presentationRecordId: catalog.presentationRecordId,
      propertyName: catalog.propertyName,
      propertySourcePageUrl: catalog.sourcePageUrl,
      imageUrl: imagePlan.imageUrl,
      imageSource: imagePlan.imageSource,
      imageSourcePageUrl: imagePlan.imageSourcePageUrl,
      usesChoiceLogo: false,
    });

    beforeAfterOpenings.push({
      recordId: row.recordId,
      beforeTitle: row.title,
      beforeBodyPreview: nz(row.body).slice(0, 160),
      afterTitle: copy.title,
      afterBodyPreview: nz(copy.body).slice(0, 160),
      action: "rewrite",
    });

    const fields = { Title: copy.title, Body: copy.body };
    const copyErrors = validatePresentationPatch(fields);
    if (copyErrors.length) {
      safetyBlockers.push(`copy_validation:${row.recordId}:${copyErrors.join(";")}`);
    } else if (row.title !== copy.title || row.body !== copy.body) {
      presentationPatches.push({
        recordId: row.recordId,
        slotKey: OPENINGS_SLOT,
        fields,
        reason: "rewrite_generic_to_property_example",
      });
    }

    const imageFields = { Image: [{ url: imagePlan.imageUrl }] };
    const imageErrors = validatePresentationPatch(imageFields, { allowImage: true });
    if (imageErrors.length) {
      safetyBlockers.push(`image_validation:${row.recordId}:${imageErrors.join(";")}`);
    } else if (normalizeUrlKey(row.imageUrl) !== normalizeUrlKey(imagePlan.imageUrl)) {
      presentationImagePatches.push({
        recordId: row.recordId,
        slotKey: OPENINGS_SLOT,
        fields: imageFields,
        reason: "align_property_example_image",
        imageSource: imagePlan.imageSource,
      });
    }

    const registryAsset = findRegistryForRow(registryAssets, row);
    const registryFields = buildRegistryFieldsForProperty({
      catalog,
      row,
      brandConfig,
      parentCompany,
      imagePlan,
    });
    const validation = validateV32gR1RegistryWritePayload({
      [MAP_BRAND_ASSET.assetName]: registryFields[MAP_BRAND_ASSET.assetName],
      [MAP_BRAND_ASSET.brandRecordId]: target.recordId,
      ...registryFields,
    });
    if (!validation.valid) {
      safetyBlockers.push(`registry_validation:${row.recordId}:${validation.errors.join(";")}`);
    } else if (registryAsset && !isDoNotUseRecord(registryAsset)) {
      const patchFields = {
        ...buildFounderApprovedRegistryPatch({
          asset: registryAsset,
          row: { ...row, title: copy.title, body: copy.body },
          brandConfig,
          materializationUrl: imagePlan.imageUrl,
        }),
        [MAP_BRAND_ASSET.assetName]: registryFields[MAP_BRAND_ASSET.assetName],
        [MAP_BRAND_ASSET.sourcePageUrl]: catalog.sourcePageUrl,
        [MAP_BRAND_ASSET.sourceUrl]: imagePlan.imageUrl,
        [MAP_VISUAL_SLOT.relatedPropertyName]: catalog.propertyName,
        [MAP_VISUAL_SLOT.propertyConfirmed]: "Yes",
        [MAP_VISUAL_SLOT.calaRelevant]: "No",
        [MAP_VISUAL_SLOT.validationNotes]: registryFields[MAP_VISUAL_SLOT.validationNotes],
        [MAP_VISUAL_SLOT.slotPurpose]: registryFields[MAP_VISUAL_SLOT.slotPurpose],
        [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
        [MAP_BRAND_ASSET.reviewNotes]: FOUNDER_REVIEW_NOTES,
      };
      registryPatches.push({
        recordId: registryAsset.id,
        presentationRecordId: row.recordId,
        fields: patchFields,
      });
    } else {
      registryCreates.push({
        presentationRecordId: row.recordId,
        fields: registryFields,
      });
    }
  }

  const rowsHidden = [];
  const hideRow = openingsRows.find((r) => r.recordId === HIDDEN_GENERIC_OPENING.presentationRecordId);
  if (hideRow) {
    beforeAfterOpenings.push({
      recordId: hideRow.recordId,
      beforeTitle: hideRow.title,
      beforeBodyPreview: nz(hideRow.body).slice(0, 160),
      afterTitle: null,
      afterBodyPreview: null,
      action: "hide",
    });
    if (hideRow.visible) {
      const hideFields = { "External Display Status": HIDE_DISPLAY_STATUS };
      const hideErrors = validateVisibilityPatch(hideFields);
      if (hideErrors.length) safetyBlockers.push(`hide_validation:${hideRow.recordId}`);
      else {
        visibilityPatches.push({
          recordId: hideRow.recordId,
          slotKey: OPENINGS_SLOT,
          fields: hideFields,
          reason: HIDDEN_GENERIC_OPENING.reason,
        });
        rowsHidden.push({
          recordId: hideRow.recordId,
          previousTitle: hideRow.title,
          reason: HIDDEN_GENERIC_OPENING.reason,
        });
      }
    }
  } else {
    safetyBlockers.push(`missing_platform_row:${HIDDEN_GENERIC_OPENING.presentationRecordId}`);
  }

  const projectedVisibleTitles = WOODSPRING_PROPERTY_CATALOG.map((catalog) => {
    const patch = presentationPatches.find((p) => p.recordId === catalog.presentationRecordId);
    return patch?.fields?.Title || buildPropertyOpeningTitle(catalog);
  });
  const genericCardsRemainVisible = projectedVisibleTitles.some((title) =>
    GENERIC_OPENING_TITLE_RE.test(nz(title))
  );
  if (genericCardsRemainVisible) {
    safetyBlockers.push("generic_platform_cards_remain_visible");
  }

  const choiceLogoPropertyImageUsed = imageSourceMapping.some((m) =>
    CHOICE_LOGO_IMAGE_RE.test(nz(m.imageUrl))
  );
  if (choiceLogoPropertyImageUsed) {
    safetyBlockers.push("choice_logo_used_as_property_image");
  }

  const protectedSlotTouched = [...presentationPatches, ...presentationImagePatches, ...visibilityPatches].some(
    (p) => PROTECTED_SLOTS.has(p.slotKey) && p.slotKey !== OPENINGS_SLOT
  );
  if (protectedSlotTouched) safetyBlockers.push("protected_slot_touched");

  const applyBlockers = [...safetyBlockers];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!founderApproved) applyBlockers.push("missing_founder_approved_woodspring_property_example_images");
    if (!officialImagesOnly) applyBlockers.push("missing_confirm_official_source_images_only");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noSummaryUrl) applyBlockers.push("missing_confirm_no_summary_url_field");
    if (!noOtherSectionChanges) {
      applyBlockers.push("missing_confirm_no_momentum_gallery_proof_standard_changes");
    }
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  const hasWork =
    presentationPatches.length > 0 ||
    presentationImagePatches.length > 0 ||
    visibilityPatches.length > 0 ||
    registryPatches.length > 0 ||
    registryCreates.length > 0;

  const dryRunClean =
    safetyBlockers.length === 0 &&
    hasWork &&
    applyBlockers.filter((b) => b.startsWith("missing_")).length === 0;

  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const visualDefectReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.recordId,
  }).catch(() => null);

  const expectedFinalQaResult = dryRunClean
    ? `projected_ready_from_${finalQaReport?.summary?.overallStatus || "unknown"}_after_property_example_rebuild`
    : finalQaReport?.summary?.overallStatus || "unknown";
  const expectedCompleteBuildResult = dryRunClean
    ? `projected_improvement; standards may still block until v33F`
    : completeBuildReport?.readyForActiveProfile
      ? "ready"
      : completeBuildReport?.blockers?.join("; ") || "blocked";
  const expectedVisualDefectResult = dryRunClean
    ? `projected_reduction_from_${visualDefectReport?.summary?.defectCount ?? "unknown"}_after_distinct_property_images`
    : visualDefectReport?.summary?.defectCount != null
      ? `${visualDefectReport.summary.defectCount} defects`
      : "unknown";

  let airtableModified = false;
  const applyResults = {
    presentationUpdated: [],
    presentationImagesUpdated: [],
    rowsHidden: [],
    registryPatched: [],
    registryCreated: [],
    errors: [],
  };

  const founderGatesReady =
    approveBatch &&
    founderApproved &&
    officialImagesOnly &&
    noValidationClaim &&
    noSummaryUrl &&
    noOtherSectionChanges &&
    woodspringOnly;

  const canApply = apply && founderGatesReady && safetyBlockers.length === 0;

  if (canApply) {
    for (const patch of presentationPatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
        applyResults.presentationUpdated.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }

    for (const patch of visibilityPatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Hide PATCH failed: ${res.status}`);
        applyResults.rowsHidden.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }

    for (const patch of registryPatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          registryTableName(),
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Registry PATCH failed: ${res.status}`);
        applyResults.registryPatched.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }

    for (const create of registryCreates) {
      try {
        const { res, json } = await airtableFetch(baseId, apiKey, registryTableName(), {
          method: "POST",
          body: JSON.stringify({ fields: create.fields, typecast: true }),
        });
        if (!res.ok) throw new Error(json.error?.message || `Registry POST failed: ${res.status}`);
        applyResults.registryCreated.push(json.id);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ presentationRecordId: create.presentationRecordId, message: err.message });
      }
    }

    for (const patch of presentationImagePatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Image PATCH failed: ${res.status}`);
        applyResults.presentationImagesUpdated.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
  }

  const brandBasicsAfter =
    canApply && airtableModified ? await fetchBrandBasics(target.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33cR1WriterExists: v33cR1WriterExists(),
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    calaAvailability,
    usFallbackRationale: calaAvailability.calaPropertyExamplesAvailable
      ? null
      : "No official CALA WoodSpring property examples in approved extract; using U.S. Choice property URLs with official WoodSpring visuals where property-page images are unavailable.",
    currentOpeningsAudit,
    selectedPropertyExamples,
    beforeAfterOpenings,
    imageSourceMapping,
    presentationPatches,
    presentationImagePatches,
    visibilityPatches,
    registryPatches,
    registryCreates,
    rowsHidden,
    choiceLogoPropertyImageUsed,
    genericCardsRemainVisible,
    codePatches: [
      {
        file: "public/js/brand-explorer-atelier-from-api.js",
        change: 'WoodSpring openings section hint → "Curated U.S. examples · Not a full directory"',
      },
    ],
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    dryRunClean,
    applyBlockers,
    applyResults,
    expectedFinalQaResult,
    expectedCompleteBuildResult,
    expectedVisualDefectResult,
    remainingBlockers: dryRunClean ? ["v33F standard detail governance if not yet applied"] : applyBlockers,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
    exactDryRunCommand: `npm run brand-explorer-woodspring-real-property-examples-writer -- --brand ${target.slug} --dry-run`,
    airtableModified,
  };

  report.markdown = buildMarkdown(report);
  return report;
}
