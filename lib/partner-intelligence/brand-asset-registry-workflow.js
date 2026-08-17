/**
 * Brand Asset Registry / Approval Workflow v1 — staging & governance layer.
 *
 * Inspects whether a Brand Asset Registry exists, proposes minimum schema,
 * stages asset candidates from Brand Asset & PR Package Governance output,
 * and produces a report-only approval plan. Does NOT download images,
 * overwrite Brand Setup media fields, or auto-approve Explorer use.
 *
 * @see docs/data-intelligence/brand-asset-registry-workflow-v1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  ASSET_STATUS,
  ASSET_TYPE,
  SOURCE_BASIS,
  BRAND_ASSET_PILOT_CONFIG,
  buildBrandAssetPrPackageGovernanceReport,
} from "./brand-asset-pr-package-governance.js";
import { PARTNER_INTELLIGENCE_LINKS, PARTNER_INTELLIGENCE_TABLES } from "../../api/lib/partner-intelligence-field-map.js";

export { BRAND_ASSET_PILOT_CONFIG };

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const WORKFLOW_VERSION = "2";
export const REPORT_JSON_NAME = "brand-asset-registry-workflow.json";
export const REPORT_MD_NAME = "brand-asset-registry-workflow.md";
export const GOVERNANCE_REPORT_JSON = "brand-asset-pr-package-governance.json";

export const BRAND_ASSET_REGISTRY_TABLE =
  process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE ||
  "Partner Intelligence - Brand Asset Registry";

/** Field names — central mapping for registry table. */
export const MAP_BRAND_ASSET = {
  assetName: "Asset Name",
  brand: "Brand",
  brandRecordId: "Brand Record ID",
  parentCompany: "Parent Company",
  assetType: "Asset Type",
  assetStatus: "Asset Status",
  sourceBasis: "Source Basis",
  sourceUrl: "Source URL",
  sourcePageUrl: "Source Page URL",
  localFilePath: "Local File Path",
  attachment: "Attachment",
  usageReviewStatus: "Usage Review Status",
  explorerUsePermission: "Explorer Use Permission",
  recommendedExplorerSlot: "Recommended Explorer Slot",
  isPrimaryCandidate: "Is Primary Candidate",
  sourceNotes: "Source Notes",
  reviewNotes: "Review Notes",
  reviewedBy: "Reviewed By",
  lastReviewedDate: "Last Reviewed Date",
  companyValidated: "Company Validated",
  companyValidationDate: "Company Validation Date",
  doNotUseReason: "Do Not Use Reason",
  sourceLibraryLink: "Source Library Link",
  stagingRunId: "Staging Run ID",
};

export const VAL_ASSET_STATUS = Object.values(ASSET_STATUS);
export const VAL_ASSET_TYPE = Object.values(ASSET_TYPE);
export const VAL_SOURCE_BASIS = Object.values(SOURCE_BASIS);

export const VAL_USAGE_REVIEW_STATUS = [
  "Not Reviewed",
  "Pending Review",
  "Usage Review Complete",
  "Blocked",
];

export const VAL_EXPLORER_USE_PERMISSION = [
  "Not Reviewed",
  "Candidate Only",
  "Approved For Explorer",
  "Internal Only",
  "Do Not Use",
];

export const VAL_EXPLORER_SLOTS = [
  "Brand Setup — Logo",
  "Brand Setup — Explorer Hero",
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "overview.hero",
  "footprint.openings",
  "PR / Recent Openings",
  "Source Library Reference",
  "None",
];

const PRESENTATION_TABLES = [
  "Brand Setup - Brand Explorer Presentation",
  "Brand Setup - Brand Explorer Materials",
  "Brand Setup - Brand Explorer Footprint",
];

const SOURCE_LIBRARY_ASSET_FIELDS = [
  "Source URL",
  "Source File",
  "Local File Path",
  "Approved for Explorer Use?",
  "File Type",
];

const BRAND_SETUP_MEDIA_FIELDS = ["Logo", "Explorer Hero Data Source", "Explorer Hero Verification"];

const DEFAULT_REGISTRY_TABLE_ID = "tblwNaf9DZt8Lth4t";

function getRegistryTableName() {
  return (
    process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID ||
    BRAND_ASSET_REGISTRY_TABLE
  );
}

function registryAirtableUrl(baseId, recordId) {
  const table = encodeURIComponent(getRegistryTableName());
  if (recordId) {
    return `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(recordId)}`;
  }
  return `https://api.airtable.com/v0/${baseId}/${table}`;
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

export function buildRegistryDedupeKey(staged, brandRecordId) {
  const sourceKey = nz(staged.sourceUrl) || nz(staged.assetName);
  return [
    brandRecordId,
    nz(staged.assetType),
    sourceKey,
    nz(staged.recommendedExplorerSlot),
  ].join("|");
}

export function normalizeRegistryAssetRecord(record) {
  const f = record.fields || {};
  const brandLinks = f[MAP_BRAND_ASSET.brand];
  return {
    id: record.id,
    assetName: nz(f[MAP_BRAND_ASSET.assetName]),
    brandRecordId: nz(f[MAP_BRAND_ASSET.brandRecordId]),
    brandId: Array.isArray(brandLinks) && brandLinks.length ? brandLinks[0] : null,
    parentCompany: nz(f[MAP_BRAND_ASSET.parentCompany]),
    assetType: nz(f[MAP_BRAND_ASSET.assetType]),
    assetStatus: nz(f[MAP_BRAND_ASSET.assetStatus]),
    sourceBasis: nz(f[MAP_BRAND_ASSET.sourceBasis]),
    sourceUrl: nz(f[MAP_BRAND_ASSET.sourceUrl]),
    sourcePageUrl: nz(f[MAP_BRAND_ASSET.sourcePageUrl]),
    localFilePath: nz(f[MAP_BRAND_ASSET.localFilePath]),
    usageReviewStatus: nz(f[MAP_BRAND_ASSET.usageReviewStatus]),
    explorerUsePermission: nz(f[MAP_BRAND_ASSET.explorerUsePermission]),
    recommendedExplorerSlot: nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]),
    isPrimaryCandidate: Boolean(f[MAP_BRAND_ASSET.isPrimaryCandidate]),
    stagingRunId: nz(f[MAP_BRAND_ASSET.stagingRunId]),
    doNotUseReason: nz(f[MAP_BRAND_ASSET.doNotUseReason]),
    createdTime: record.createdTime || null,
  };
}

export async function listRegistryAssetsForBrand(brandRecordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const formula = `{${MAP_BRAND_ASSET.brandRecordId}}='${brandRecordId.replace(/'/g, "\\'")}'`;
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const url = `${registryAirtableUrl(baseId)}?${params.toString()}`;
    const { res, json } = await registryAirtableFetch(url, apiKey);
    if (!res.ok) {
      throw new Error(json.error?.message || `Airtable list registry failed: ${res.status}`);
    }
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);

  return records.map(normalizeRegistryAssetRecord);
}

export function validateRegistryWritePayload(fields) {
  const errors = [];
  if (!nz(fields[MAP_BRAND_ASSET.assetName])) errors.push("Asset Name required");
  if (!nz(fields[MAP_BRAND_ASSET.brandRecordId])) errors.push("Brand Record ID required");
  if (!VAL_ASSET_TYPE.includes(fields[MAP_BRAND_ASSET.assetType])) {
    errors.push(`Invalid Asset Type: ${fields[MAP_BRAND_ASSET.assetType]}`);
  }
  if (!VAL_ASSET_STATUS.includes(fields[MAP_BRAND_ASSET.assetStatus])) {
    errors.push(`Invalid Asset Status: ${fields[MAP_BRAND_ASSET.assetStatus]}`);
  }
  if (fields[MAP_BRAND_ASSET.assetStatus] === ASSET_STATUS.APPROVED_EXPLORER) {
    errors.push("Approved For Explorer Use is not allowed in v2 writer");
  }
  if (fields[MAP_BRAND_ASSET.explorerUsePermission] === "Approved For Explorer") {
    errors.push("Explorer Use Permission Approved For Explorer is not allowed in v2 writer");
  }
  if (fields[MAP_BRAND_ASSET.attachment]) {
    errors.push("Attachment must not be set in v2 metadata-only writer");
  }
  if (fields[MAP_BRAND_ASSET.companyValidated]) {
    errors.push("Company Validated must not be set");
  }
  if (fields[MAP_BRAND_ASSET.companyValidationDate]) {
    errors.push("Company Validation Date must not be set");
  }
  return { valid: errors.length === 0, errors };
}

export function mapStagedAssetToRegistryFields(staged, { brandRecordId, parentCompany, stagingRunId }) {
  const fields = {
    [MAP_BRAND_ASSET.assetName]: staged.assetName,
    [MAP_BRAND_ASSET.brand]: [brandRecordId],
    [MAP_BRAND_ASSET.brandRecordId]: brandRecordId,
    [MAP_BRAND_ASSET.parentCompany]: parentCompany,
    [MAP_BRAND_ASSET.assetType]: staged.assetType,
    [MAP_BRAND_ASSET.assetStatus]: staged.assetStatus,
    [MAP_BRAND_ASSET.sourceBasis]: staged.sourceBasis,
    [MAP_BRAND_ASSET.usageReviewStatus]: staged.usageReviewStatus || "Not Reviewed",
    [MAP_BRAND_ASSET.explorerUsePermission]: staged.explorerUsePermission || "Candidate Only",
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: staged.recommendedExplorerSlot,
    [MAP_BRAND_ASSET.isPrimaryCandidate]: Boolean(staged.isPrimaryCandidate),
    [MAP_BRAND_ASSET.sourceNotes]: staged.sourceNotes || "",
    [MAP_BRAND_ASSET.stagingRunId]: stagingRunId,
    [MAP_BRAND_ASSET.companyValidated]: false,
  };
  if (staged.sourceUrl) fields[MAP_BRAND_ASSET.sourceUrl] = staged.sourceUrl;
  if (staged.sourcePageUrl) fields[MAP_BRAND_ASSET.sourcePageUrl] = staged.sourcePageUrl;
  if (staged.localFilePath) fields[MAP_BRAND_ASSET.localFilePath] = staged.localFilePath;
  if (staged.reviewNotes) fields[MAP_BRAND_ASSET.reviewNotes] = staged.reviewNotes;
  if (staged.doNotUseReason) fields[MAP_BRAND_ASSET.doNotUseReason] = staged.doNotUseReason;
  return fields;
}

async function createRegistryRecordsBatch(baseId, apiKey, recordsFields) {
  const url = registryAirtableUrl(baseId);
  const { res, json } = await registryAirtableFetch(url, apiKey, {
    method: "POST",
    body: JSON.stringify({
      records: recordsFields.map((fields) => ({ fields })),
      typecast: true,
    }),
  });
  if (!res.ok) {
    throw new Error(json.error?.message || `Airtable create registry batch failed: ${res.status}`);
  }
  return (json.records || []).map(normalizeRegistryAssetRecord);
}

export async function applyRegistryRecords({
  brandRecordId,
  parentCompany,
  stagedAssets,
  stagingRunId,
}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const existing = await listRegistryAssetsForBrand(brandRecordId);
  const existingKeys = new Set(
    existing.map((r) =>
      buildRegistryDedupeKey(
        {
          assetType: r.assetType,
          sourceUrl: r.sourceUrl,
          assetName: r.assetName,
          recommendedExplorerSlot: r.recommendedExplorerSlot,
        },
        brandRecordId
      )
    )
  );

  const proposed = [];
  const skippedDuplicates = [];
  const validationFailed = [];

  for (const staged of stagedAssets) {
    const dedupeKey = buildRegistryDedupeKey(staged, brandRecordId);
    if (existingKeys.has(dedupeKey)) {
      skippedDuplicates.push({ assetName: staged.assetName, dedupeKey, reason: "existing-registry-record" });
      continue;
    }
    const fields = mapStagedAssetToRegistryFields(staged, {
      brandRecordId,
      parentCompany,
      stagingRunId,
    });
    const validation = validateRegistryWritePayload(fields);
    if (!validation.valid) {
      validationFailed.push({ assetName: staged.assetName, errors: validation.errors });
      continue;
    }
    proposed.push({ staged, fields, dedupeKey });
    existingKeys.add(dedupeKey);
  }

  if (validationFailed.length) {
    throw new Error(
      `Registry write validation failed: ${JSON.stringify(validationFailed)}`
    );
  }

  const created = [];
  const BATCH = 10;
  for (let i = 0; i < proposed.length; i += BATCH) {
    const batch = proposed.slice(i, i + BATCH);
    const createdBatch = await createRegistryRecordsBatch(
      baseId,
      apiKey,
      batch.map((p) => p.fields)
    );
    created.push(
      ...createdBatch.map((rec, idx) => ({
        recordId: rec.id,
        assetName: rec.assetName,
        assetType: rec.assetType,
        assetStatus: rec.assetStatus,
        dedupeKey: batch[idx].dedupeKey,
      }))
    );
  }

  return {
    existingRecordsFound: existing.length,
    recordsProposed: proposed.length,
    recordsCreated: created.length,
    recordsSkippedDuplicates: skippedDuplicates,
    created,
    validationFailed,
  };
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function singleSelect(name, optionNames, description) {
  const field = { name, type: "singleSelect", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}

function dateField(name, description) {
  const field = {
    name,
    type: "date",
    options: { dateFormat: { name: "iso" } },
  };
  if (description) field.description = description;
  return field;
}

function linkField(name, linkedTableId, description) {
  const field = {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
  if (description) field.description = description;
  return field;
}

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

export function buildProposedRegistrySchema({ brandBasicsId, sourceLibraryId } = {}) {
  const fields = [
    { name: MAP_BRAND_ASSET.assetName, type: "singleLineText", description: "Human-readable asset label." },
    linkField(MAP_BRAND_ASSET.brand, brandBasicsId, "Link to Brand Setup - Brand Basics."),
    { name: MAP_BRAND_ASSET.brandRecordId, type: "singleLineText", description: "rec… id for scripting." },
    { name: MAP_BRAND_ASSET.parentCompany, type: "singleLineText" },
    singleSelect(MAP_BRAND_ASSET.assetType, VAL_ASSET_TYPE),
    singleSelect(MAP_BRAND_ASSET.assetStatus, VAL_ASSET_STATUS),
    singleSelect(MAP_BRAND_ASSET.sourceBasis, VAL_SOURCE_BASIS),
    { name: MAP_BRAND_ASSET.sourceUrl, type: "url" },
    { name: MAP_BRAND_ASSET.sourcePageUrl, type: "url" },
    { name: MAP_BRAND_ASSET.localFilePath, type: "singleLineText" },
    { name: MAP_BRAND_ASSET.attachment, type: "multipleAttachments", description: "Populated only after download + rights review." },
    singleSelect(MAP_BRAND_ASSET.usageReviewStatus, VAL_USAGE_REVIEW_STATUS),
    singleSelect(MAP_BRAND_ASSET.explorerUsePermission, VAL_EXPLORER_USE_PERMISSION),
    singleSelect(MAP_BRAND_ASSET.recommendedExplorerSlot, VAL_EXPLORER_SLOTS),
    { name: MAP_BRAND_ASSET.isPrimaryCandidate, type: "checkbox", options: { icon: "check", color: "greenBright" } },
    { name: MAP_BRAND_ASSET.sourceNotes, type: "multilineText" },
    { name: MAP_BRAND_ASSET.reviewNotes, type: "multilineText" },
    { name: MAP_BRAND_ASSET.reviewedBy, type: "singleCollaborator" },
    dateField(MAP_BRAND_ASSET.lastReviewedDate),
    { name: MAP_BRAND_ASSET.companyValidated, type: "checkbox", options: { icon: "check", color: "blueBright" } },
    dateField(MAP_BRAND_ASSET.companyValidationDate, "Set only from direct company confirmation — never from PI extraction."),
    { name: MAP_BRAND_ASSET.doNotUseReason, type: "multilineText" },
    { name: MAP_BRAND_ASSET.stagingRunId, type: "singleLineText" },
  ];
  if (sourceLibraryId) {
    fields.push(
      linkField(
        MAP_BRAND_ASSET.sourceLibraryLink,
        sourceLibraryId,
        "Optional link to PI Source Library row when asset derives from a registered source."
      )
    );
  }
  return {
    tableName: BRAND_ASSET_REGISTRY_TABLE,
    description:
      "Governed brand visual assets (logo, hero, property images, PR) — staging and approval workflow. Does not replace Brand Setup fields until explicitly promoted.",
    fields,
    enums: {
      assetStatus: VAL_ASSET_STATUS,
      assetType: VAL_ASSET_TYPE,
      sourceBasis: VAL_SOURCE_BASIS,
      usageReviewStatus: VAL_USAGE_REVIEW_STATUS,
      explorerUsePermission: VAL_EXPLORER_USE_PERMISSION,
      recommendedExplorerSlot: VAL_EXPLORER_SLOTS,
    },
  };
}

export async function inspectExistingAssetInfrastructure(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    throw new Error(`Meta list tables failed ${res.status}: ${JSON.stringify(json)}`);
  }
  const tables = json.tables || [];

  const registryTable = tables.find((t) => t.name === BRAND_ASSET_REGISTRY_TABLE);
  const sourceLibrary = tables.find((t) => t.name === PARTNER_INTELLIGENCE_TABLES.sourceLibrary);
  const brandBasics = tables.find((t) => t.name === PARTNER_INTELLIGENCE_LINKS.brandBasics);
  const presentationTables = PRESENTATION_TABLES.map((name) => {
    const t = tables.find((x) => x.name === name);
    if (!t) return { name, exists: false, imageFields: [] };
    const imageFields = (t.fields || [])
      .filter((f) => /image|photo|attachment|logo|hero/i.test(f.name))
      .map((f) => ({ name: f.name, type: f.type }));
    return { name, exists: true, tableId: t.id, imageFields };
  });

  const brandBasicsMedia = brandBasics
    ? BRAND_SETUP_MEDIA_FIELDS.map((fname) => {
        const f = (brandBasics.fields || []).find((x) => x.name === fname);
        return { field: fname, exists: Boolean(f), type: f?.type || null };
      })
    : [];

  const sourceLibraryAssetCapability = sourceLibrary
    ? {
        exists: true,
        tableId: sourceLibrary.id,
        relevantFields: SOURCE_LIBRARY_ASSET_FIELDS.map((fname) => {
          const f = (sourceLibrary.fields || []).find((x) => x.name === fname);
          return { field: fname, exists: Boolean(f), type: f?.type || null };
        }),
        suitableForAssetGovernance: false,
        reason:
          "Source Library tracks document/source provenance for text extraction. It lacks asset-type, usage-review, Explorer-slot, and rights-governance fields required for visual asset approval workflow.",
      }
    : { exists: false, suitableForAssetGovernance: false, reason: "Source Library table not found." };

  return {
    registryTable: registryTable
      ? {
          exists: true,
          tableId: registryTable.id,
          fieldCount: (registryTable.fields || []).length,
          fields: (registryTable.fields || []).map((f) => f.name),
        }
      : { exists: false },
    sourceLibrary: sourceLibraryAssetCapability,
    brandBasicsMedia,
    presentationTables,
    brandBasicsId: brandBasics?.id || null,
    sourceLibraryId: sourceLibrary?.id || null,
    newRegistryRecommended: !registryTable?.id,
  };
}

function loadGovernanceReport() {
  const p = path.join(ROOT, "reports", GOVERNANCE_REPORT_JSON);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function isFaviconUrl(url) {
  return /favicon|cropped-tributefavicon|placeholder|apple57x57|profile_placeholder/i.test(url);
}

function isTributeLogoUrl(url) {
  return /tribute.*\.(svg|png)/i.test(url) && !/ritz|sheraton|westin|bonvoy-logo|marriott\.svg|header-logo/i.test(url);
}

function isGenericMarriottProperty(url) {
  return /mlewh-overwater|marriott-renditions\/MLEWH/i.test(url);
}

function pickPreferredLogo(candidates) {
  const tributeSpecific = candidates.find(
    (c) => c.url.includes("tribute-portfolio.svg") || c.url.includes("tribute-black.svg")
  );
  if (tributeSpecific) return tributeSpecific;
  const consumer = candidates.find(
    (c) =>
      c.url.includes("tribute-portfolio.marriott.com") &&
      c.url.endsWith(".svg") &&
      !isFaviconUrl(c.url)
  );
  return consumer || candidates.find((c) => isTributeLogoUrl(c.url)) || candidates[0];
}

function pickPreferredHero(candidates) {
  const consumerHero = candidates.find(
    (c) =>
      c.url.includes("trbcl.") &&
      c.url.includes("2560x1024") &&
      c.sourceBasis === SOURCE_BASIS.MARRIOTT_CONTROLLED
  );
  if (consumerHero) return consumerHero;
  const devHero = candidates.find((c) => c.assetTypeGuess === ASSET_TYPE.HERO || c.url.includes("hero-image"));
  return devHero || candidates.find((c) => c.url.includes("trbcl.")) || null;
}

function pickPropertyCandidates(candidates, limit = 6, excludeUrls = []) {
  const exclude = new Set(excludeUrls.map((u) => u.replace(/\?.*$/, "")));
  const tributeFirst = candidates.filter(
    (c) =>
      c.url.includes("tribute-portfolio.marriott.com") &&
      c.url.includes("trbcl.") &&
      !isFaviconUrl(c.url) &&
      !exclude.has(c.url.replace(/\?.*$/, ""))
  );
  const fallback = candidates.filter(
    (c) =>
      !isFaviconUrl(c.url) &&
      !isGenericMarriottProperty(c.url) &&
      !/hero-image|\.svg$/i.test(c.url) &&
      !/hotel-development\.marriott\.com\/resourcefiles\/home-hero-slider/i.test(c.url) &&
      !exclude.has(c.url.replace(/\?.*$/, ""))
  );

  const pool = [...tributeFirst, ...fallback];
  const scored = pool.map((c) => {
    let score = 0;
    if (c.url.includes("trbcl.")) score += 10;
    if (c.url.includes("tribute-portfolio.marriott.com")) score += 5;
    if (c.url.includes("2560x1024") || c.url.includes("1536x614")) score += 3;
    if (c.sourceBasis === SOURCE_BASIS.MARRIOTT_CONTROLLED) score += 2;
    return { ...c, score };
  }).sort((a, b) => b.score - a.score);

  const seen = new Set();
  const out = [];
  for (const c of scored) {
    const key = c.url.replace(/\?.*$/, "");
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(c);
    if (out.length >= limit) break;
  }
  return out;
}

function buildStagedAssetRecord(partial, stagingRunId) {
  return {
    ...partial,
    stagingRunId,
    explorerUsePermission: partial.explorerUsePermission || "Candidate Only",
    usageReviewStatus: partial.usageReviewStatus || "Not Reviewed",
    companyValidated: false,
    companyValidationDate: null,
    attachment: null,
    metadataOnly: true,
    safeMetadataOnly: partial.safeMetadataOnly !== false,
    needsUsageReview: partial.needsUsageReview !== false,
    requiresFutureTooling: Boolean(partial.requiresFutureTooling),
  };
}

export function stageTributeAssetCandidates(governanceReport, stagingRunId) {
  const brand = governanceReport.brand;
  const logoCandidates = governanceReport.officialCandidates?.imageLogo || [];
  const heroProperty = governanceReport.officialCandidates?.heroProperty || [];
  const localPdfs = (governanceReport.localAssets?.pdfs || []).filter((p) => p.tributeSpecific);
  const prCandidate = governanceReport.prRecentOpeningCandidates?.[0] || governanceReport.currentStatus?.prRecentOpenings?.pressHubProbe;

  const preferredLogo = pickPreferredLogo(logoCandidates);
  const preferredHero = pickPreferredHero(heroProperty);
  const propertyCandidates = pickPropertyCandidates(
    heroProperty,
    6,
    preferredHero ? [preferredHero.url] : []
  );
  const fddPdf = localPdfs[0];

  const staged = [];

  staged.push(
    buildStagedAssetRecord(
      {
        assetName: "Brand Setup — existing logo (unconfirmed)",
        assetType: ASSET_TYPE.LOGO,
        assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
        sourceBasis: SOURCE_BASIS.COMPANY_MATERIALS,
        sourceUrl: null,
        sourcePageUrl: governanceReport.brand?.consumerUrl || "https://tribute-portfolio.marriott.com/",
        localFilePath: null,
        recommendedExplorerSlot: "Brand Setup — Logo",
        isPrimaryCandidate: false,
        sourceNotes: governanceReport.currentStatus?.logo?.note || "Existing Brand Setup logo — confirm source/rights before Explorer use.",
        doNotUseReason: null,
        needsUsageReview: true,
        safeMetadataOnly: true,
      },
      stagingRunId
    )
  );

  if (preferredLogo) {
    staged.push(
      buildStagedAssetRecord(
        {
          assetName: "Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate)",
          assetType: ASSET_TYPE.LOGO,
          assetStatus: ASSET_STATUS.CANDIDATE,
          sourceBasis: preferredLogo.sourceBasis,
          sourceUrl: preferredLogo.url,
          sourcePageUrl: "https://tribute-portfolio.marriott.com/",
          localFilePath: preferredLogo.localCapturePath || null,
          recommendedExplorerSlot: "Brand Setup — Logo",
          isPrimaryCandidate: true,
          sourceNotes: `Official Marriott-controlled logo candidate from ${preferredLogo.label}. Confirm matches Brand Setup attachment before promotion.`,
          needsUsageReview: true,
        },
        stagingRunId
      )
    );
  }

  if (preferredHero) {
    staged.push(
      buildStagedAssetRecord(
        {
          assetName: "Tribute Portfolio hero — consumer property wide (preferred)",
          assetType: ASSET_TYPE.HERO,
          assetStatus: ASSET_STATUS.CANDIDATE,
          sourceBasis: preferredHero.sourceBasis,
          sourceUrl: preferredHero.url,
          sourcePageUrl: "https://tribute-portfolio.marriott.com/",
          localFilePath: preferredHero.localCapturePath || null,
          recommendedExplorerSlot: "Brand Setup — Explorer Hero",
          isPrimaryCandidate: true,
          sourceNotes:
            "Preferred hero candidate. Current Brand Setup hero is Mock/Demo — do NOT overwrite until usage review + staged promotion workflow.",
          reviewNotes: "Mock/Demo hero must remain until explicit promotion gate.",
          needsUsageReview: true,
        },
        stagingRunId
      )
    );
  }

  const gallerySlots = [
    "materials.gallery.1",
    "materials.gallery.2",
    "materials.gallery.3",
    "materials.gallery.4",
    "materials.gallery.5",
    "materials.gallery.6",
  ];
  propertyCandidates.slice(0, 6).forEach((c, i) => {
    staged.push(
      buildStagedAssetRecord(
        {
          assetName: `Tribute property/design image ${i + 1}`,
          assetType: ASSET_TYPE.EXTERIOR,
          assetStatus: ASSET_STATUS.CANDIDATE,
          sourceBasis: c.sourceBasis,
          sourceUrl: c.url,
          sourcePageUrl: "https://tribute-portfolio.marriott.com/",
          localFilePath: c.localCapturePath || null,
          recommendedExplorerSlot: gallerySlots[i] || "materials.gallery.1",
          isPrimaryCandidate: i === 0,
          sourceNotes: `Property/design candidate from ${c.label}. Usage review required before Explorer gallery slot.`,
          needsUsageReview: true,
        },
        stagingRunId
      )
    );
  });

  if (fddPdf) {
    staged.push(
      buildStagedAssetRecord(
        {
          assetName: "2026 Tribute Portfolio FDD (reference)",
          assetType: ASSET_TYPE.PDF,
          assetStatus: ASSET_STATUS.SOURCE_CONFIRMED,
          sourceBasis: SOURCE_BASIS.LOCAL_REFERENCE,
          sourceUrl: null,
          sourcePageUrl: null,
          localFilePath: fddPdf.relativePath,
          recommendedExplorerSlot: "Source Library Reference",
          isPrimaryCandidate: true,
          sourceNotes: "FDD already registered in PI Source Library — text/factual reference only; not Explorer hero/logo.",
          explorerUsePermission: "Internal Only",
          needsUsageReview: false,
          safeMetadataOnly: true,
        },
        stagingRunId
      )
    );
  }

  if (prCandidate) {
    staged.push(
      buildStagedAssetRecord(
        {
          assetName: "Marriott newsroom — Tribute Portfolio PR/openings (placeholder)",
          assetType: ASSET_TYPE.PRESS_LINK,
          assetStatus: ASSET_STATUS.DO_NOT_USE,
          sourceBasis: SOURCE_BASIS.RENDERED_OFFICIAL,
          sourceUrl: prCandidate.url,
          sourcePageUrl: prCandidate.url,
          localFilePath: null,
          recommendedExplorerSlot: "PR / Recent Openings",
          isPrimaryCandidate: true,
          sourceNotes: prCandidate.note || "JS-shell — provenance only.",
          doNotUseReason: "news.marriott.com requires Rendered Source Capture v1 before PR links or press imagery can be governed.",
          explorerUsePermission: "Do Not Use",
          usageReviewStatus: "Blocked",
          needsUsageReview: true,
          requiresFutureTooling: true,
        },
        stagingRunId
      )
    );
  }

  staged.push(
    buildStagedAssetRecord(
      {
        assetName: "Brand Setup — Mock/Demo hero (do not replace)",
        assetType: ASSET_TYPE.HERO,
        assetStatus: ASSET_STATUS.MOCK_DEMO,
        sourceBasis: SOURCE_BASIS.UNKNOWN,
        sourceUrl: null,
        sourcePageUrl: null,
        localFilePath: null,
        recommendedExplorerSlot: "Brand Setup — Explorer Hero",
        isPrimaryCandidate: false,
        sourceNotes: governanceReport.currentStatus?.hero?.note || "Mock/Demo — must not be overwritten without staged approval.",
        explorerUsePermission: "Do Not Use",
        doNotUseReason: "Mock/Demo placeholder — not brand-verified.",
        needsUsageReview: false,
        safeMetadataOnly: true,
      },
      stagingRunId
    )
  );

  return {
    brandRecordId: brand.recordId,
    brandName: brand.name,
    parentCompany: brand.parentCompany,
    stagingRunId,
    totalStaged: staged.length,
    stagedAssets: staged,
    summary: {
      preferredLogo: preferredLogo?.url || null,
      preferredHero: preferredHero?.url || null,
      propertyCount: propertyCandidates.length,
      fddReference: fddPdf?.relativePath || null,
      prPlaceholder: prCandidate?.url || null,
    },
  };
}

async function applyRegistrySchema(baseId, token, infrastructure) {
  const schema = buildProposedRegistrySchema({
    brandBasicsId: infrastructure.brandBasicsId,
    sourceLibraryId: infrastructure.sourceLibraryId,
  });
  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);
  const tables = listJson.tables || [];
  let table = tables.find((t) => t.name === schema.tableName);
  const result = { createdTable: false, fieldsCreated: [], fieldsSkipped: [], tableId: table?.id || null };

  if (!table) {
    const primary = schema.fields.find((f) => f.name === MAP_BRAND_ASSET.assetName);
    const rest = schema.fields.filter((f) => f.name !== MAP_BRAND_ASSET.assetName);
    const { res, json } = await metaFetch(baseId, token, "/tables", {
      method: "POST",
      body: JSON.stringify({
        name: schema.tableName,
        description: schema.description,
        fields: [primary, ...rest],
      }),
    });
    if (!res.ok) throw new Error(`Create registry table failed: ${JSON.stringify(json)}`);
    result.createdTable = true;
    result.tableId = json.id;
    result.fieldsCreated = (json.fields || []).map((f) => f.name);
    table = json;
  } else {
    const existing = new Set((table.fields || []).map((f) => f.name));
    for (const fieldSpec of schema.fields) {
      if (existing.has(fieldSpec.name)) {
        result.fieldsSkipped.push(fieldSpec.name);
        continue;
      }
      const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
        method: "POST",
        body: JSON.stringify(fieldSpec),
      });
      if (!res.ok) throw new Error(`Create field ${fieldSpec.name} failed: ${JSON.stringify(json)}`);
      result.fieldsCreated.push(fieldSpec.name);
    }
  }
  return result;
}

export async function buildBrandAssetRegistryWorkflowReport({
  brandKey = "tribute-portfolio",
  probeUrls = false,
  applySchema = false,
  schemaApproved = false,
  applyRecords = false,
  recordsApproved = false,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) {
    return { error: "AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required." };
  }

  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey];
  if (!pilot) {
    return { error: `Unknown brand key: ${brandKey}. Known: ${Object.keys(BRAND_ASSET_PILOT_CONFIG).join(", ")}` };
  }

  let mode = "dry-run";
  if (applySchema && schemaApproved) mode = "schema-apply";
  if (applyRecords && recordsApproved) mode = "records-apply";

  const stagingRunId = `brand-asset-staging-${brandKey}-${Date.now()}`;
  let airtableModified = false;
  let brandSetupMediaUntouched = true;

  const infrastructure = await inspectExistingAssetInfrastructure(baseId, token);
  const proposedSchema = buildProposedRegistrySchema({
    brandBasicsId: infrastructure.brandBasicsId,
    sourceLibraryId: infrastructure.sourceLibraryId,
  });

  let governanceReport = loadGovernanceReport();
  let governanceSource = "cached-report";
  if (!governanceReport || governanceReport.brand?.key !== brandKey) {
    governanceReport = await buildBrandAssetPrPackageGovernanceReport({ brandKey, probeUrls });
    governanceSource = probeUrls ? "live-governance-run" : "live-governance-run-no-probe";
  }

  if (governanceReport.error) {
    return { error: governanceReport.error };
  }

  const staging = stageTributeAssetCandidates(governanceReport, stagingRunId);

  let schemaApplyResult = null;
  if (applySchema && schemaApproved) {
    schemaApplyResult = await applyRegistrySchema(baseId, token, infrastructure);
    airtableModified = schemaApplyResult.createdTable || schemaApplyResult.fieldsCreated.length > 0;
  }

  let existingRegistryRecords = [];
  if (infrastructure.registryTable.exists) {
    try {
      existingRegistryRecords = await listRegistryAssetsForBrand(pilot.recordId);
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[brand-asset-registry-workflow] list existing records:", err.message);
      }
    }
  }

  const proposedDedupe = staging.stagedAssets.map((a) => ({
    assetName: a.assetName,
    assetType: a.assetType,
    assetStatus: a.assetStatus,
    dedupeKey: buildRegistryDedupeKey(a, pilot.recordId),
    recommendedExplorerSlot: a.recommendedExplorerSlot,
    sourceUrl: a.sourceUrl || null,
  }));

  const existingDedupeKeys = new Set(
    existingRegistryRecords.map((r) =>
      buildRegistryDedupeKey(
        {
          assetType: r.assetType,
          sourceUrl: r.sourceUrl,
          assetName: r.assetName,
          recommendedExplorerSlot: r.recommendedExplorerSlot,
        },
        pilot.recordId
      )
    )
  );

  const wouldCreate = proposedDedupe.filter((p) => !existingDedupeKeys.has(p.dedupeKey));
  const wouldSkip = proposedDedupe.filter((p) => existingDedupeKeys.has(p.dedupeKey));

  let recordWriterResult = {
    registryTableStatus: infrastructure.registryTable.exists
      ? { exists: true, tableId: infrastructure.registryTable.tableId || DEFAULT_REGISTRY_TABLE_ID }
      : { exists: false },
    existingRecordsFound: existingRegistryRecords.length,
    recordsProposed: wouldCreate.length,
    recordsCreated: 0,
    recordsSkippedDuplicates: wouldSkip.map((p) => ({
      assetName: p.assetName,
      dedupeKey: p.dedupeKey,
      reason: "existing-registry-record",
    })),
    created: [],
    brandSetupMediaUntouched: true,
  };

  if (applyRecords && recordsApproved) {
    if (!infrastructure.registryTable.exists) {
      return { error: "Brand Asset Registry table not found. Run schema apply first." };
    }
    const applyResult = await applyRegistryRecords({
      brandRecordId: pilot.recordId,
      parentCompany: pilot.parentCompany,
      stagedAssets: staging.stagedAssets,
      stagingRunId,
    });
    recordWriterResult = {
      ...recordWriterResult,
      existingRecordsFound: applyResult.existingRecordsFound,
      recordsProposed: applyResult.recordsProposed,
      recordsCreated: applyResult.recordsCreated,
      recordsSkippedDuplicates: applyResult.recordsSkippedDuplicates,
      created: applyResult.created,
      brandSetupMediaUntouched: true,
    };
    airtableModified = applyResult.recordsCreated > 0;
    brandSetupMediaUntouched = true;
    existingRegistryRecords = await listRegistryAssetsForBrand(pilot.recordId);
  }

  const metadataOnlySafe = staging.stagedAssets.filter((a) => a.safeMetadataOnly && !a.requiresFutureTooling);
  const needsUsageReview = staging.stagedAssets.filter((a) => a.needsUsageReview);
  const requiresFutureTooling = staging.stagedAssets.filter((a) => a.requiresFutureTooling);
  const doNotUseRecords = staging.stagedAssets.filter(
    (a) => a.assetStatus === ASSET_STATUS.DO_NOT_USE || a.assetStatus === ASSET_STATUS.MOCK_DEMO
  );

  const schemaApplyCommand =
    "npm run brand-asset-registry-workflow -- --brand tribute-portfolio --apply --approve-brand-asset-registry-schema";
  const recordsApplyCommand =
    "npm run brand-asset-registry-workflow -- --brand tribute-portfolio --apply --approve-brand-asset-registry-records";
  const nextCommandAfterRecords =
    "npm run brand-asset-registry-workflow -- --brand tribute-portfolio --dry-run";

  return {
    workflowVersion: WORKFLOW_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified,
    brandSetupMediaUntouched,
    brand: governanceReport.brand,
    governedProfileStatus: governanceReport.governedProfileStatus,
    governanceReportSource: governanceSource,
    existingInfrastructure: infrastructure,
    sourceLibraryAssessment: {
      canSupportAssetGovernance: false,
      recommendation: "Use dedicated Brand Asset Registry table — Source Library is for document/source provenance, not visual asset approval workflow.",
      linkStrategy: "Optional Source Library Link field on registry rows when asset derives from a registered source.",
    },
    newRegistryRecommended: infrastructure.newRegistryRecommended,
    proposedSchema: {
      tableName: proposedSchema.tableName,
      fieldCount: proposedSchema.fields.length,
      fields: proposedSchema.fields.map((f) => f.name),
      enums: proposedSchema.enums,
    },
    schemaApplyResult,
    recordWriter: recordWriterResult,
    staging,
    approvalPlan: {
      metadataOnlySafe: metadataOnlySafe.map((a) => ({
        assetName: a.assetName,
        assetType: a.assetType,
        assetStatus: a.assetStatus,
        reason: a.sourceNotes,
      })),
      needsUsageReview: needsUsageReview.map((a) => ({
        assetName: a.assetName,
        assetType: a.assetType,
        sourceUrl: a.sourceUrl,
        recommendedExplorerSlot: a.recommendedExplorerSlot,
      })),
      doNotUse: doNotUseRecords.map((a) => ({
        assetName: a.assetName,
        assetType: a.assetType,
        assetStatus: a.assetStatus,
        reason: a.doNotUseReason || a.sourceNotes,
      })),
      requiresFutureTooling: requiresFutureTooling.map((a) => ({
        assetName: a.assetName,
        assetType: a.assetType,
        reason: a.doNotUseReason || a.sourceNotes,
      })),
      autoApproveBlocked: true,
      autoApproveReason: "v2 never marks assets Approved For Explorer Use automatically.",
    },
    visualParityGap: governanceReport.visualParityGap,
    renderedSourceCaptureNeeded: governanceReport.renderedSourceCaptureNeeded,
    remainingWorkBeforePromotion: [
      "Human usage review on logo, hero, and gallery candidates in registry",
      "Confirm tribute-black.svg matches Brand Setup logo attachment",
      "Approve hero replacement only after rights review — Mock/Demo hero stays until promotion gate",
      "Rendered Source Capture v1 for Marriott newsroom PR",
      "Future v3: asset download + Explorer hero/logo promotion writer",
    ],
    doesNotDo: [
      "Download images or attach binary files",
      "Overwrite Brand Setup logo, hero, image, or attachment fields",
      "Replace Mock/Demo hero",
      "Mark assets Approved For Explorer Use automatically",
      "Set Company Validated or Company Validation Date",
      "Imply Marriott validated assets or profile",
    ],
    schemaApplyCommand: infrastructure.newRegistryRecommended ? schemaApplyCommand : null,
    recordsApplyCommand:
      infrastructure.registryTable.exists && recordWriterResult.recordsProposed > 0
        ? recordsApplyCommand
        : infrastructure.registryTable.exists
          ? null
          : recordsApplyCommand,
    nextCommandAfterRecords,
    nextCommand:
      recordWriterResult.recordsProposed > 0 && mode === "dry-run"
        ? recordsApplyCommand
        : nextCommandAfterRecords,
  };
}

export function buildBrandAssetRegistryWorkflowMarkdown(report) {
  if (report.error) {
    return `# Brand Asset Registry / Approval Workflow v2\n\nError: ${report.error}\n`;
  }

  const rw = report.recordWriter || {};

  const lines = [
    "# Brand Asset Registry / Approval Workflow v2",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `Brand Setup media untouched: **${report.brandSetupMediaUntouched ? "yes" : "no"}**`,
    `Brand: ${report.brand.name} \`${report.brand.recordId}\``,
    `Governance report source: ${report.governanceReportSource}`,
    `Text/governance Platform Ready: **${report.governedProfileStatus?.textGovernancePlatformReady ? "yes" : "no"}**`,
    "",
    "## 1. Registry table status",
    "",
    `- Table exists: **${rw.registryTableStatus?.exists ? "yes" : "no"}**${rw.registryTableStatus?.tableId ? ` (\`${rw.registryTableStatus.tableId}\`)` : ""}`,
    `- Existing records for brand: **${rw.existingRecordsFound ?? 0}**`,
    "",
    "## 2. Record writer (v2)",
    "",
    `| Metric | Count |`,
    `|--------|-------|`,
    `| Records proposed | ${rw.recordsProposed ?? 0} |`,
    `| Records created | ${rw.recordsCreated ?? 0} |`,
    `| Records skipped (duplicates) | ${rw.recordsSkippedDuplicates?.length ?? 0} |`,
    "",
    ...(rw.created?.length
      ? ["### Created", "", ...rw.created.map((c) => `- \`${c.recordId}\` — ${c.assetName} (${c.assetType})`)]
      : []),
    "",
    ...(rw.recordsSkippedDuplicates?.length
      ? [
          "### Skipped duplicates",
          "",
          ...rw.recordsSkippedDuplicates.map((s) => `- ${s.assetName} — \`${s.dedupeKey}\``),
        ]
      : []),
    "",
    "## 3. Existing asset/media schema",
    "",
    `- Brand Asset Registry table: **${report.existingInfrastructure.registryTable.exists ? "exists" : "missing"}**`,
    `- Source Library suitable for asset governance: **no** — ${report.sourceLibraryAssessment.recommendation}`,
    `- Brand Setup media fields: ${report.existingInfrastructure.brandBasicsMedia.map((f) => `\`${f.field}\` (${f.exists ? "yes" : "no"})`).join(", ")}`,
    "",
    "### Presentation tables (image slots — not scanned for assets in v1)",
    "",
    ...report.existingInfrastructure.presentationTables.map(
      (t) =>
        `- ${t.name}: ${t.exists ? `exists (${t.imageFields.length} image-related fields)` : "not found"}`
    ),
    "",
    "## 4. Staged Tribute asset candidates",
    "",
    `Staging run: \`${report.staging.stagingRunId}\` · **${report.staging.totalStaged}** records`,
    "",
    "| Asset | Type | Status | Primary | Slot |",
    "|-------|------|--------|---------|------|",
    ...report.staging.stagedAssets.map(
      (a) =>
        `| ${a.assetName} | ${a.assetType} | ${a.assetStatus} | ${a.isPrimaryCandidate ? "yes" : "no"} | ${a.recommendedExplorerSlot} |`
    ),
    "",
    "## 5. Needs usage review",
    "",
    ...report.approvalPlan.needsUsageReview.map(
      (a) => `- **${a.assetName}** → ${a.recommendedExplorerSlot}${a.sourceUrl ? ` — \`${a.sourceUrl}\`` : ""}`
    ),
    "",
    "## 6. Do Not Use / Mock-Demo",
    "",
    ...(report.approvalPlan.doNotUse || []).map(
      (a) => `- **${a.assetName}** (${a.assetStatus}) — ${a.reason}`
    ),
    "",
    "## 7. Requires future tooling",
    "",
    ...report.approvalPlan.requiresFutureTooling.map((a) => `- **${a.assetName}** — ${a.reason}`),
    `- Rendered Source Capture v1: **${report.renderedSourceCaptureNeeded ? "yes" : "no"}**`,
    "",
    "## 8. Records apply command",
    "",
    report.recordsApplyCommand
      ? `\`\`\`bash\n${report.recordsApplyCommand}\n\`\`\``
      : "_No new records to write — registry is up to date._",
    "",
    "## 9. Next command",
    "",
    `\`\`\`bash\n${report.nextCommand}\n\`\`\``,
    "",
    "## 10. Remaining work before hero/logo promotion",
    "",
    ...(report.remainingWorkBeforePromotion || []).map((w) => `- ${w}`),
    "",
    "## 11. Visual parity gap (Kimpton / Radisson Blu)",
    "",
    `- Target: ${report.visualParityGap?.kimptonRadissonTarget || "—"}`,
    `- Tribute now: ${report.visualParityGap?.tributeCurrent || "—"}`,
    "",
    "**Remaining:**",
    ...(report.visualParityGap?.remainingWork || []).map((w) => `- ${w}`),
    "",
    "## Does not do",
    "",
    ...report.doesNotDo.map((d) => `- ${d}`),
  ];

  return `${lines.join("\n")}\n`;
}
