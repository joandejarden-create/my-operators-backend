/**
 * Brand Explorer Radisson Individuals Durable Gallery Source + Registry Repair v31J.
 *
 * Restores materials.gallery.1–6 from durable official Choice property pages and repairs
 * Brand Asset Registry metadata (Source Page URL vs image attachment semantics).
 * Never approves images, touches openings/quarantined rows, or modifies Company Validated.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-durable-gallery-source-repair-writer-v31J.md
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
import { ASSET_STATUS, SOURCE_BASIS } from "./brand-asset-pr-package-governance.js";
import {
  DISCOVERY_BRAND_CONFIG,
  detectWrongBrandSignageRisk,
  galleryPendingReviewBlocksActiveProfile,
  isGalleryImageSlot,
  isRegistryAssetApprovedForExplorer,
  normalizeUrlKey,
} from "./brand-explorer-brand-asset-image-governance.js";
import { MAP_VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import {
  classifyRegistryAsset,
} from "./brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";
import {
  isDoNotUseRecord,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import {
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND as SUPPRESSION_TARGET,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import { uploadFileBytesToAirtable } from "../dealality/airtable-upload-attachment.js";

export const WRITER_VERSION = "31J";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-durable-gallery-source-repair-writer-v31J.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31J-durable-gallery-source-repair";
export const APPLY_FLAG_RESTORE = "--restore-gallery-images-from-durable-sources";
export const APPLY_FLAG_NO_IMAGE_APPROVAL = "--confirm-no-image-approval";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const TARGET_BRAND = SUPPRESSION_TARGET;
export const STAGING_RUN_ID = "v31J-durable-gallery-source-repair";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "radisson",
  "ascend",
  "radisson-blu",
  "kimpton",
  "curio-collection",
  "suburban-studios",
  "woodspring-suites",
  "everhome-suites",
  ...WAVE1_EXPANSION_SLUGS.filter((s) => s !== TARGET_BRAND.slug),
]);

/** Curated durable source pages — evidence from Choice sitemap census + official listings. */
export const GALLERY_DURABLE_SOURCES = Object.freeze([
  {
    slotKey: "materials.gallery.1",
    presentationRecordId: "recbuxkGK4Uh6Hq0y",
    registryRecordId: "rec0tjE3JvH7pyP5J",
    propertyName: "Hotel Bambito By Faranda Boutique",
    marketCity: "Cerro Punta",
    countryRegion: "Panama",
    calaRelevant: "Yes",
    sourcePageUrl: "https://www.choicehotels.com/colon/cerro-punta/radisson-individuals-hotels/pn007",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/en/hotel/hotel-bambito-by-faranda-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-pn007",
    evidenceSource: "choice-sitemap-pn007 + faranda-official-pn007",
    titleKeywords: ["bambito"],
  },
  {
    slotKey: "materials.gallery.2",
    presentationRecordId: "recEOOCEnmy48hRiy",
    registryRecordId: "recXlmzJTtHbKIxJs",
    propertyName: "Hotel Casa Don Luis by Faranda Boutique",
    marketCity: "Cartagena",
    countryRegion: "Colombia",
    calaRelevant: "Yes",
    sourcePageUrl:
      "https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb017",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/hotel/hotel-casa-don-luis-cartagena-by-faranda-boutique-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-cb017",
    evidenceSource: "choice-official-listing-cb017 + faranda-official-cb017",
    titleKeywords: ["casa don luis", "don luis"],
  },
  {
    slotKey: "materials.gallery.3",
    presentationRecordId: "recmuusPraVbFUxyK",
    registryRecordId: "recm6isRdCaotG3aU",
    propertyName: "Hotel Faranda Guayacanes",
    marketCity: "Chitré",
    countryRegion: "Panama",
    calaRelevant: "Yes",
    sourcePageUrl:
      "https://www.choicehotels.com/herrera/chitre/radisson-individuals-hotels/pn009",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/en/hotel/hotel-faranda-guayacanes-chitre-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-pn009",
    evidenceSource: "choice-sitemap-pn009 + faranda-official-pn009",
    titleKeywords: ["guayacanes"],
  },
  {
    slotKey: "materials.gallery.4",
    presentationRecordId: "recZWoVDGYpJdTBP1",
    registryRecordId: "rec3fCxZEdL7lwR6e",
    propertyName: "Faranda Collection Bogota",
    marketCity: "Bogotá",
    countryRegion: "Colombia",
    calaRelevant: "Yes",
    sourcePageUrl:
      "https://www.choicehotels.com/colombia/bogota/radisson-individuals-hotels/cb012",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/en/hotel/hotel-faranda-collection-bogota-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-cb012",
    evidenceSource: "choice-sitemap-cb012 + faranda-official-cb012",
    titleKeywords: ["bogota", "bogotá", "collection bogota"],
  },
  {
    slotKey: "materials.gallery.5",
    presentationRecordId: "rectMcyJ3FaVqG1ly",
    registryRecordId: "recLVkWOnyPszJYGt",
    propertyName: "Hotel Casa La Factoria by Faranda Boutique",
    marketCity: "Cartagena",
    countryRegion: "Colombia",
    calaRelevant: "Yes",
    sourcePageUrl:
      "https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb018",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/hotel/hotel-casa-la-factoria-cartagena-by-faranda-boutique-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-cb018",
    evidenceSource: "choice-official-listing-cb018 + faranda-official-cb018",
    titleKeywords: ["factoria", "factoría", "casa la factoria"],
  },
  {
    slotKey: "materials.gallery.6",
    presentationRecordId: "reco6kIWyBsLOwohj",
    registryRecordId: "recdxfVt2sISpG95w",
    propertyName: "Hotel Faranda Bolivar Cucuta",
    marketCity: "Cúcuta",
    countryRegion: "Colombia",
    calaRelevant: "Yes",
    sourcePageUrl:
      "https://www.choicehotels.com/colombia/cucuta/radisson-individuals-hotels/cb010",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/en/hotel/hotel-faranda-bolivar-cucuta-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-cb010",
    evidenceSource: "choice-sitemap-cb010 + faranda-official-cb010",
    titleKeywords: ["bolivar", "bolívar", "cucuta", "cúcuta"],
  },
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const IMAGE_FIELD = "Image";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.md",
  "reports/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.json",
  "reports/brand-explorer-radisson-individuals-gallery-restore-writer.md",
  "reports/brand-explorer-radisson-individuals-gallery-restore-writer.json",
  "reports/brand-explorer-radisson-individuals-asset-registry-normalization-writer.md",
  "reports/brand-explorer-radisson-individuals-asset-registry-normalization-writer.json",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.md",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.json",
  "live Radisson Individuals presentation rows",
  "live Radisson Individuals Brand Asset Registry rows",
  "live Source Library records",
  "Tribute Portfolio Brand Asset Registry rows (schema reference)",
  "api/brand-library.js",
  "docs/brand-explorer-presentation-slots.md",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js",
  "scripts/brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.mjs",
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

export function isTemporaryAirtableUrl(url) {
  const u = nz(url).toLowerCase();
  return u.includes("v5.airtableusercontent.com") || u.includes("airtableusercontent.com");
}

export function isDurableSourcePageUrl(url) {
  const u = nz(url);
  if (!u || !/^https?:\/\//i.test(u)) return false;
  if (isTemporaryAirtableUrl(u)) return false;
  try {
    const host = new URL(u).hostname.toLowerCase();
    return (
      host.endsWith("choicehotels.com") ||
      host.endsWith("radissonhotels.com") ||
      host.endsWith("radisson.com")
    );
  } catch {
    return false;
  }
}

export function extractChoicePropertyUrlFromText(text) {
  const matches = nz(text).match(
    /https?:\/\/(?:www\.)?choicehotels\.com\/[^\s)\]"']+\/radisson-individuals-hotels\/[a-z0-9]+/gi
  );
  return matches?.[0] || null;
}

function decodeMetaContent(value) {
  return nz(value)
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

export function extractOgImageFromHtml(html) {
  const raw = String(html || "");
  const re1 = /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i;
  const re2 = /<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i;
  const m = raw.match(re1) || raw.match(re2);
  return m ? decodeMetaContent(m[1]) : null;
}

export function pageSupportsProperty(html, titleKeywords = []) {
  const haystack = String(html || "").toLowerCase();
  if (!titleKeywords.length) return true;
  return titleKeywords.some((k) => haystack.includes(k.toLowerCase()));
}

export function isChoiceAccessDeniedHtml(html) {
  return /access denied/i.test(String(html || ""));
}

export async function fetchDurablePropertyImage({
  sourcePageUrl,
  officialPropertyPageUrl = null,
  titleKeywords = [],
}) {
  const attempts = [
    { url: sourcePageUrl, label: "choice_consumer_page" },
    { url: officialPropertyPageUrl, label: "faranda_official_property_page" },
  ].filter((a) => a.url);

  const errors = [];
  for (const attempt of attempts) {
    try {
      const res = await fetch(attempt.url, {
        headers: { "User-Agent": "Dealality-BrandExplorer/1.0" },
        redirect: "follow",
      });
      if (!res.ok) {
        errors.push(`${attempt.label}:http_${res.status}`);
        continue;
      }
      const html = await res.text();
      if (isChoiceAccessDeniedHtml(html)) {
        errors.push(`${attempt.label}:access_denied`);
        continue;
      }
      if (
        attempt.label === "choice_consumer_page" &&
        !pageSupportsProperty(html, titleKeywords)
      ) {
        errors.push(`${attempt.label}:property_keyword_missing`);
      }
      const imageUrl = extractOgImageFromHtml(html);
      if (!imageUrl) {
        errors.push(`${attempt.label}:no_og_image`);
        continue;
      }
      if (isTemporaryAirtableUrl(imageUrl)) {
        errors.push(`${attempt.label}:temporary_image_url`);
        continue;
      }
      if (
        attempt.label === "faranda_official_property_page" ||
        pageSupportsProperty(html, titleKeywords) ||
        imageUrl
      ) {
        return {
          ok: true,
          sourcePageUrl,
          imageUrl,
          resolvedFrom: `${attempt.label}:og:image`,
          imageFetchSource: attempt.label,
          officialPropertyPageUrl: officialPropertyPageUrl || null,
        };
      }
    } catch (err) {
      errors.push(`${attempt.label}:${err.message}`);
    }
  }

  return {
    ok: false,
    error: errors.join("; ") || "no_durable_image_resolved",
    sourcePageUrl,
    imageUrl: null,
  };
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
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

function normalizePresentationRow(rec) {
  const f = rec.fields || {};
  return {
    recordId: rec.id,
    fields: f,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    body: nz(f.Body),
    externalDisplayStatus: nz(f["External Display Status"]),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(f["External Display Status"])),
    hasImage: Array.isArray(f.Image) && f.Image.length > 0,
    imageUrl: Array.isArray(f.Image) && f.Image[0]?.url ? nz(f.Image[0].url) : null,
  };
}

function classifyUrlStatus(url) {
  if (!url) return "missing";
  if (isTemporaryAirtableUrl(url)) return "temporary_expired";
  if (isDurableSourcePageUrl(url)) return "durable";
  return "other";
}

function registryFieldsWouldApprove(fields) {
  return (
    fields[MAP_BRAND_ASSET.assetStatus] === ASSET_STATUS.APPROVED_EXPLORER ||
    fields[MAP_BRAND_ASSET.explorerUsePermission] === "Approved For Explorer" ||
    fields[MAP_BRAND_ASSET.usageReviewStatus] === "Usage Review Complete"
  );
}

export function buildPendingGalleryRegistryFields({
  curated,
  sourcePageUrl,
  imageUrl,
  presentationRecordId,
  brandRecordId,
  brandName,
}) {
  const assetName = `Radisson Individuals — ${curated.propertyName} — Gallery Image`;
  return {
    [MAP_BRAND_ASSET.assetName]: assetName,
    [MAP_BRAND_ASSET.assetType]: "Exterior / Property",
    [MAP_BRAND_ASSET.assetStatus]: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    [MAP_BRAND_ASSET.sourceBasis]: SOURCE_BASIS.RENDERED_OFFICIAL,
    [MAP_BRAND_ASSET.sourceUrl]: imageUrl || null,
    [MAP_BRAND_ASSET.sourcePageUrl]: sourcePageUrl,
    [MAP_BRAND_ASSET.usageReviewStatus]: "Pending Review",
    [MAP_BRAND_ASSET.explorerUsePermission]: "Candidate Only",
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: curated.slotKey,
    [MAP_BRAND_ASSET.isPrimaryCandidate]: true,
    [MAP_BRAND_ASSET.reviewNotes]:
      "v31J durable gallery repair — pending image review; Source Page URL is durable Choice property page; not approved for active-profile.",
    [MAP_BRAND_ASSET.sourceNotes]: `Durable source: ${curated.evidenceSource}. Presentation row ${presentationRecordId}.`,
    [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
    [MAP_VISUAL_SLOT.explorerSection]: "Image Gallery",
    [MAP_VISUAL_SLOT.slotPurpose]: "Gallery property image — pending founder review",
    [MAP_VISUAL_SLOT.relatedPropertyName]: curated.propertyName,
    [MAP_VISUAL_SLOT.countryRegion]: curated.countryRegion,
    [MAP_VISUAL_SLOT.calaRelevant]: curated.calaRelevant,
    [MAP_VISUAL_SLOT.propertyConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.brandConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.validationStatus]: "Pending Review",
    [MAP_VISUAL_SLOT.validationNotes]:
      "v31J restored from durable Choice property page og:image — human signage review still required.",
    [MAP_BRAND_ASSET.brandRecordId]: brandRecordId,
  };
}

export function classifyGalleryRepairEligibility({
  row,
  registry,
  curated,
  brandConfig,
}) {
  if (!row || !isGalleryImageSlot(row.slotKey)) {
    return { eligible: false, reason: "not_gallery_slot" };
  }
  if (row.quarantined) {
    return { eligible: false, reason: "quarantined_presentation_row" };
  }
  if (registry && isDoNotUseRecord(registry)) {
    return { eligible: false, reason: "do_not_use_registry_asset" };
  }
  const wrongBrand = detectWrongBrandSignageRisk(
    [row.title, curated?.propertyName, curated?.sourcePageUrl].filter(Boolean).join("\n"),
    brandConfig
  );
  if (wrongBrand) {
    return { eligible: false, reason: wrongBrand.reason, wrongBrandRisk: wrongBrand };
  }
  if (!curated?.sourcePageUrl || !isDurableSourcePageUrl(curated.sourcePageUrl)) {
    return { eligible: false, reason: "missing_durable_source_page" };
  }
  return {
    eligible: true,
    reason: "gallery_cleared_by_v31d_restore_from_durable_source",
    resetApprovalToPending: Boolean(registry && isRegistryAssetApprovedForExplorer(registry)),
  };
}

export function v31jWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31J`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31J supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-durable-gallery-source-repair-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_RESTORE,
    APPLY_FLAG_NO_IMAGE_APPROVAL,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

async function downloadImageBuffer(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": "Dealality-BrandExplorer/1.0" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Image download failed ${res.status} for ${url}`);
  const contentType = nz(res.headers.get("content-type")) || "image/jpeg";
  const buffer = Buffer.from(await res.arrayBuffer());
  return { buffer, contentType };
}

function filenameFromImageUrl(imageUrl, slotKey) {
  try {
    const pathname = new URL(imageUrl).pathname;
    const base = pathname.split("/").pop() || "gallery-image";
    const ext = base.includes(".") ? "" : ".jpg";
    return `${slotKey.replace(/\./g, "-")}-${base}${ext}`.slice(0, 120);
  } catch {
    return `${slotKey.replace(/\./g, "-")}-gallery.jpg`;
  }
}

async function materializePresentationImage({ baseId, apiKey, recordId, imageUrl, slotKey, dryRun }) {
  if (dryRun) {
    return { strategy: "content-api-upload", materialized: false, projectedUrl: imageUrl, dryRunOnly: true };
  }
  const { buffer, contentType } = await downloadImageBuffer(imageUrl);
  await uploadFileBytesToAirtable({
    baseId,
    recordId,
    fieldName: IMAGE_FIELD,
    buffer,
    contentType,
    filename: filenameFromImageUrl(imageUrl, slotKey),
    apiKey,
  });
  await new Promise((r) => setTimeout(r, 400));
  const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {}, recordId);
  if (!res.ok) throw new Error(json.error?.message || `Reread failed ${recordId}`);
  const url =
    Array.isArray(json.fields?.Image) && json.fields.Image[0]?.url
      ? nz(json.fields.Image[0].url)
      : null;
  return {
    strategy: "content-api-upload",
    materialized: Boolean(url),
    attachmentUrl: url,
    bytesUploaded: buffer.length,
  };
}

async function materializeRegistryAttachment({
  baseId,
  apiKey,
  recordId,
  imageUrl,
  slotKey,
  dryRun,
}) {
  if (dryRun) {
    return { materialized: false, projectedUrl: imageUrl, dryRunOnly: true };
  }
  const { buffer, contentType } = await downloadImageBuffer(imageUrl);
  await uploadFileBytesToAirtable({
    baseId,
    recordId,
    fieldName: MAP_BRAND_ASSET.attachment,
    buffer,
    contentType,
    filename: filenameFromImageUrl(imageUrl, `${slotKey}-registry`),
    apiKey,
  });
  await new Promise((r) => setTimeout(r, 400));
  const { res, json } = await airtableFetch(
    baseId,
    apiKey,
    BRAND_ASSET_REGISTRY_TABLE,
    {},
    recordId
  );
  if (!res.ok) throw new Error(json.error?.message || `Registry reread failed ${recordId}`);
  const att = json.fields?.[MAP_BRAND_ASSET.attachment];
  const url = Array.isArray(att) && att[0]?.url ? nz(att[0].url) : null;
  return { materialized: Boolean(url), attachmentUrl: url, bytesUploaded: buffer.length };
}

export async function buildBrandExplorerRadissonIndividualsDurableGallerySourceRepairWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  restoreFromDurable = false,
  noImageApproval = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = DISCOVERY_BRAND_CONFIG[target.slug];

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const presentationRaw = await listPresentationRowsRaw(
    baseId,
    apiKey,
    target.recordId,
    target.name
  );
  const allRows = presentationRaw.map(normalizePresentationRow);
  const galleryRows = allRows.filter((r) => isGalleryImageSlot(r.slotKey));
  const registryAssetsRaw = await listRegistryAssetsForBrand(target.recordId).catch(() => []);

  const galleryImageDiagnosis = [];
  const durableSourceFindings = [];
  const registryFieldRepairPlan = [];
  const proposedPresentationUpdates = [];
  const proposedRegistryUpdates = [];
  const imagesNotRestored = [];
  const v31eCompatibility = [];

  for (const curated of GALLERY_DURABLE_SOURCES) {
    const row =
      galleryRows.find((r) => r.recordId === curated.presentationRecordId) ||
      galleryRows.find((r) => r.slotKey === curated.slotKey);
    const registry =
      registryAssetsRaw.find((a) => a.id === curated.registryRecordId) ||
      registryAssetsRaw.find((a) => nz(a.recommendedExplorerSlot) === curated.slotKey);

    const bodySourceUrl = row ? extractChoicePropertyUrlFromText(row.body) : null;
    const resolvedSourcePageUrl =
      (bodySourceUrl && isDurableSourcePageUrl(bodySourceUrl) ? bodySourceUrl : null) ||
      curated.sourcePageUrl;

    const currentImageUrl = row?.imageUrl || null;
    const currentSourceUrl = registry?.sourceUrl || null;
    const currentSourcePageUrl = registry?.sourcePageUrl || null;

    const diagnosis = {
      presentationRecordId: row?.recordId || curated.presentationRecordId,
      slotKey: curated.slotKey,
      title: row?.title || curated.propertyName,
      hasImageAttachment: Boolean(row?.hasImage),
      currentImageUrl,
      imageUrlStatus: classifyUrlStatus(currentImageUrl),
      imageUrlExpiredOrTemporary: isTemporaryAirtableUrl(currentImageUrl),
      linkedRegistryRecordId: registry?.id || curated.registryRecordId,
      registryApprovalStatus: registry?.assetStatus || null,
      registryApproved: registry ? isRegistryAssetApprovedForExplorer(registry) : false,
      registrySourceUrl: currentSourceUrl || null,
      registrySourcePageUrl: currentSourcePageUrl || null,
      registrySourceUrlStatus: classifyUrlStatus(currentSourceUrl),
      relatedPropertyName: curated.propertyName,
      intendedSlot: curated.slotKey,
      shouldRestore: false,
      restoreBlockReason: null,
    };

    const eligibility = classifyGalleryRepairEligibility({
      row: row || { slotKey: curated.slotKey, quarantined: false, title: curated.propertyName },
      registry,
      curated: { ...curated, sourcePageUrl: resolvedSourcePageUrl },
      brandConfig,
    });

    let durableImage = null;
    if (eligibility.eligible) {
      durableImage = await fetchDurablePropertyImage({
        sourcePageUrl: resolvedSourcePageUrl,
        officialPropertyPageUrl: curated.officialPropertyPageUrl,
        titleKeywords: curated.titleKeywords,
      });
      durableSourceFindings.push({
        slotKey: curated.slotKey,
        propertyName: curated.propertyName,
        sourcePageUrl: resolvedSourcePageUrl,
        evidenceSource: curated.evidenceSource,
        bodySourceUrl,
        imageResolved: durableImage.ok,
        imageUrl: durableImage.imageUrl || null,
        resolvedFrom: durableImage.resolvedFrom || null,
        imageFetchSource: durableImage.imageFetchSource || null,
        officialPropertyPageUrl: curated.officialPropertyPageUrl || null,
        error: durableImage.error || null,
      });

      if (durableImage.ok) {
        diagnosis.shouldRestore = true;
      } else {
        diagnosis.shouldRestore = false;
        diagnosis.restoreBlockReason = durableImage.error;
        imagesNotRestored.push({
          slotKey: curated.slotKey,
          reason: durableImage.error,
        });
      }
    } else {
      diagnosis.shouldRestore = false;
      diagnosis.restoreBlockReason = eligibility.reason;
      durableSourceFindings.push({
        slotKey: curated.slotKey,
        propertyName: curated.propertyName,
        sourcePageUrl: resolvedSourcePageUrl,
        evidenceSource: curated.evidenceSource,
        bodySourceUrl,
        imageResolved: false,
        error: eligibility.reason,
      });
      imagesNotRestored.push({ slotKey: curated.slotKey, reason: eligibility.reason });
    }

    galleryImageDiagnosis.push(diagnosis);

    if (!diagnosis.shouldRestore || !durableImage?.ok) continue;

    const registryFields = buildPendingGalleryRegistryFields({
      curated,
      sourcePageUrl: resolvedSourcePageUrl,
      imageUrl: durableImage.imageUrl,
      presentationRecordId: row?.recordId || curated.presentationRecordId,
      brandRecordId: target.recordId,
      brandName: target.name,
    });

    const missingBefore = [];
    for (const [key, label] of [
      ["sourcePageUrl", "Source Page URL"],
      ["explorerSection", "Explorer Section"],
      ["relatedPropertyName", "Related Property Name"],
      ["countryRegion", "Country / Region"],
      ["validationStatus", "Visual Slot Validation Status"],
    ]) {
      if (!nz(registry?.[key])) missingBefore.push(label);
    }

    registryFieldRepairPlan.push({
      registryRecordId: registry?.id || curated.registryRecordId,
      slotKey: curated.slotKey,
      assetName: registryFields[MAP_BRAND_ASSET.assetName],
      fieldsToPopulate: Object.keys(registryFields),
      missingBefore,
      resetApprovalToPending: eligibility.resetApprovalToPending,
      sourcePageUrl: resolvedSourcePageUrl,
      imageUrl: durableImage.imageUrl,
      v31eClassificationAfter: "Pending Image Review",
    });

    proposedRegistryUpdates.push({
      registryRecordId: registry?.id || curated.registryRecordId,
      slotKey: curated.slotKey,
      fields: registryFields,
      uploadAttachmentFrom: durableImage.imageUrl,
    });

    proposedPresentationUpdates.push({
      action: "restore_gallery_image_from_durable_source",
      recordId: row?.recordId || curated.presentationRecordId,
      slotKey: curated.slotKey,
      sourcePageUrl: resolvedSourcePageUrl,
      imageUrl: durableImage.imageUrl,
      pendingImageReview: true,
    });

    v31eCompatibility.push({
      registryRecordId: registry?.id || curated.registryRecordId,
      slotKey: curated.slotKey,
      assetType: "image_asset",
      classification: classifyRegistryAsset({
        assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
        explorerUsePermission: "Candidate Only",
        usageReviewStatus: "Pending Review",
        assetName: registryFields[MAP_BRAND_ASSET.assetName],
      }),
      materializableAfterApproval: true,
      sourceReferenceOnly: false,
      pendingGalleryReview: true,
      durableSourcePageUrl: resolvedSourcePageUrl,
    });
  }

  const applyBlockers = [];
  if (proposedPresentationUpdates.length === 0) {
    applyBlockers.push("no_gallery_images_eligible_for_durable_restore");
  }
  if (
    proposedRegistryUpdates.some((u) => registryFieldsWouldApprove(u.fields)) ||
    proposedPresentationUpdates.some((u) => u.approved)
  ) {
    applyBlockers.push("images_would_be_marked_approved");
  }
  if (
    proposedRegistryUpdates.some((u) =>
      isTemporaryAirtableUrl(u.fields[MAP_BRAND_ASSET.sourcePageUrl])
    )
  ) {
    applyBlockers.push("temporary_url_would_be_written_as_source_page_url");
  }
  if (
    proposedPresentationUpdates.some((u) => {
      const row = allRows.find((r) => r.recordId === u.recordId);
      return row?.quarantined;
    })
  ) {
    applyBlockers.push("quarantined_rows_would_be_touched");
  }
  const wouldRestoreDoNotUse = proposedPresentationUpdates.some((u) => {
    const reg = registryAssetsRaw.find(
      (a) => nz(a.recommendedExplorerSlot) === u.slotKey && isDoNotUseRecord(a)
    );
    return Boolean(reg);
  });
  if (wouldRestoreDoNotUse) applyBlockers.push("do_not_use_images_would_be_restored");

  const brandApi = await fetchBrandApiShape(target.slug);
  const galleryBlocksActiveProfile = galleryPendingReviewBlocksActiveProfile(
    brandApi,
    registryAssetsRaw,
    brandConfig,
    { slug: target.slug, resolution: { resolutionSource: "expansion_backlog" } }
  );

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));
  const qaBrand = finalQaBefore?.brandReports?.[0] || {};

  const completeBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));
  const completeBrand =
    (completeBefore?.brandReports || []).find((b) => b.slug === target.slug) || {};

  const hasWork =
    proposedPresentationUpdates.length > 0 || proposedRegistryUpdates.length > 0;
  const applyGatesReady =
    apply && approveBatch && restoreFromDurable && noImageApproval && noValidationClaim;
  const dryRunClean = applyBlockers.length === 0;
  const canApply = applyGatesReady && dryRunClean && hasWork;

  let airtableModified = false;
  let imagesApproved = false;
  let imagesRestored = false;
  let applyResults = {
    presentationUpdated: [],
    registryUpdated: [],
    errors: [],
    imagesApproved: false,
    companyValidatedChanged: false,
  };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const regUpdate of proposedRegistryUpdates) {
      if (registryFieldsWouldApprove(regUpdate.fields)) {
        applyResults.errors.push({
          registryRecordId: regUpdate.registryRecordId,
          error: "blocked_registry_approval_fields",
        });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        BRAND_ASSET_REGISTRY_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: regUpdate.fields, typecast: true }) },
        regUpdate.registryRecordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          registryRecordId: regUpdate.registryRecordId,
          error: json.error?.message || `PATCH failed ${res.status}`,
        });
        continue;
      }
      applyResults.registryUpdated.push(regUpdate.registryRecordId);
      airtableModified = true;

      if (regUpdate.uploadAttachmentFrom) {
        try {
          await materializeRegistryAttachment({
            baseId,
            apiKey,
            recordId: regUpdate.registryRecordId,
            imageUrl: regUpdate.uploadAttachmentFrom,
            slotKey: regUpdate.slotKey,
            dryRun: false,
          });
        } catch (err) {
          applyResults.errors.push({
            registryRecordId: regUpdate.registryRecordId,
            error: `registry_attachment_upload: ${err.message}`,
          });
        }
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const update of proposedPresentationUpdates) {
      const row = allRows.find((r) => r.recordId === update.recordId);
      if (row?.quarantined) {
        applyResults.errors.push({ recordId: update.recordId, error: "quarantined_row_blocked" });
        continue;
      }
      try {
        const mat = await materializePresentationImage({
          baseId,
          apiKey,
          recordId: update.recordId,
          imageUrl: update.imageUrl,
          slotKey: update.slotKey,
          dryRun: false,
        });
        applyResults.presentationUpdated.push({
          recordId: update.recordId,
          slotKey: update.slotKey,
          materialized: mat.materialized,
          attachmentUrl: mat.attachmentUrl || null,
        });
        if (mat.materialized) {
          imagesRestored = true;
          airtableModified = true;
        }
      } catch (err) {
        applyResults.errors.push({
          recordId: update.recordId,
          error: err.message,
        });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
    applyResults.companyValidatedChanged =
      JSON.stringify(companyValidatedBefore) !== JSON.stringify(companyValidatedAfter);
    applyResults.imagesApproved = false;
    imagesApproved = false;
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const expectedUiResult =
    proposedPresentationUpdates.length > 0
      ? `Materials gallery shows up to ${proposedPresentationUpdates.length} property images sourced from durable Choice pages; draft/internal profile renders gallery cards with pending-review posture.`
      : "Gallery remains blank until durable sources resolve.";

  const expectedActiveProfileResult = {
    readyForActiveProfile: false,
    blockedByPendingGalleryImages:
      galleryBlocksActiveProfile || proposedPresentationUpdates.length > 0,
    note:
      "Gallery images remain pending review after v31J — active-profile blocked until founder approves registry assets and v31E materializes.",
  };

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: apply && canApply ? "apply" : "dry-run",
    v31jWriterExists: v31jWriterExists(),
    targetBrand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    imagesApproved,
    imagesRestored,
    imagesMaterialized: imagesRestored,
    airtableModified,
    dryRunClean,
    applyBlockers,
    galleryImageDiagnosis,
    durableSourceFindings,
    registryFieldRepairPlan,
    rowsToUpdate: proposedPresentationUpdates,
    registryAssetsToUpdate: proposedRegistryUpdates,
    imagesNotRestored,
    v31eCompatibility,
    currentReadinessDiagnosis: {
      finalQaScore: qaBrand.score ?? null,
      finalQaReadiness: qaBrand.readiness ?? null,
      activeProfileReady: completeBrand.readyForTargetQuality ?? false,
      activeProfileBlockers: completeBrand.blockers ?? [],
    },
    expectedUiResult,
    expectedActiveProfileResult,
    exactApplyCommand: dryRunClean && hasWork ? buildApplyCommand({ brand: target.slug }) : null,
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Radisson Individuals Durable Gallery Source + Registry Repair v31J`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.targetBrand.name}**`,
    `- v31J exists: **${report.v31jWriterExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Gallery image diagnosis",
    `- Slots audited: **${report.galleryImageDiagnosis.length}**`,
    `- Should restore: **${report.galleryImageDiagnosis.filter((d) => d.shouldRestore).length}**`,
    `- Expired/temporary image URLs: **${report.galleryImageDiagnosis.filter((d) => d.imageUrlExpiredOrTemporary).length}**`,
    "",
  ];

  for (const d of report.galleryImageDiagnosis) {
    lines.push(
      `- \`${d.slotKey}\` ${d.title.slice(0, 50)} — image: ${d.hasImageAttachment ? "attached" : "blank"}, url: ${d.imageUrlStatus}, restore: ${d.shouldRestore ? "yes" : `no (${d.restoreBlockReason})`}`
    );
  }

  lines.push(
    "",
    "## Durable source page findings",
    `- Resolved: **${report.durableSourceFindings.filter((f) => f.imageResolved).length}** / ${report.durableSourceFindings.length}`,
    ""
  );
  for (const f of report.durableSourceFindings) {
    lines.push(
      `- \`${f.slotKey}\` ${f.propertyName} — ${f.sourcePageUrl} — image: ${f.imageResolved ? "resolved" : f.error}`
    );
  }

  lines.push(
    "",
    "## Registry field repair plan",
    `- Assets to update: **${report.registryFieldRepairPlan.length}**`,
    ""
  );
  for (const p of report.registryFieldRepairPlan) {
    lines.push(
      `- \`${p.slotKey}\` ${p.registryRecordId} — missing before: ${p.missingBefore.join(", ") || "none"} — reset approval: ${p.resetApprovalToPending ? "yes" : "no"}`
    );
  }

  lines.push(
    "",
    "## Apply summary",
    `- Presentation rows to update: **${report.rowsToUpdate.length}**`,
    `- Registry assets to update: **${report.registryAssetsToUpdate.length}**`,
    `- Images approved: **no**`,
    `- Images restored/materialized: **${report.imagesRestored ? "yes" : "no (dry-run or blocked)"}**`,
    `- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`,
    report.applyBlockers.length ? `- Blockers: ${report.applyBlockers.join(", ")}` : "",
    "",
    "## Expected UI result",
    report.expectedUiResult,
    "",
    "## Expected active-profile result",
    `- Ready: **${report.expectedActiveProfileResult.readyForActiveProfile ? "yes" : "no"}**`,
    `- Note: ${report.expectedActiveProfileResult.note}`,
    "",
    "## Current readiness",
    `- Final QA: **${report.currentReadinessDiagnosis.finalQaScore ?? "—"}** (${report.currentReadinessDiagnosis.finalQaReadiness ?? "—"})`,
    `- Active-profile ready: **${report.currentReadinessDiagnosis.activeProfileReady ? "yes" : "no"}**`,
  );

  if (report.exactApplyCommand) {
    lines.push("", "## Exact apply command", "```bash", report.exactApplyCommand, "```");
  }

  return lines.filter(Boolean).join("\n");
}
