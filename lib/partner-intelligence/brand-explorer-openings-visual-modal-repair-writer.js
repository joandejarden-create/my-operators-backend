/**
 * Brand Explorer Openings / Examples Visual + Modal Repair Writer v25C-3D.
 *
 * Repairs Tribute Portfolio footprint.openings rows: hero images, premium card copy,
 * and Case Summary modal fields. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-openings-visual-modal-repair-writer-v25C-3D.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { uploadFileBytesToAirtable } from "../dealality/airtable-upload-attachment.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  OPENINGS_SLOT,
  REPORT_JSON_NAME as REVIEW_PACKAGE_JSON,
} from "./brand-explorer-openings-momentum-row-review-package.js";

export const WRITER_VERSION = "25C-3D";
export const REPORT_JSON_NAME = "brand-explorer-openings-visual-modal-repair-writer.json";
export const REPORT_MD_NAME = "brand-explorer-openings-visual-modal-repair-writer.md";
export const DOC_MD_NAME = "brand-explorer-openings-visual-modal-repair-writer-v25C-3D.md";

export const APPLY_FLAG_BATCH = "--approve-brand-explorer-v25C-3D-openings-visual-modal-repair";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-openings-ui-copy";
export const APPLY_FLAG_IMAGE = "--approve-brand-explorer-v25C-3D-image-render-repair";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const IMAGE_FIELD = "Image";
const CASA_NIZUC_MARSHA = "CUNAN";

const MARSHA_UI_RE = /\bMARSHA\b/i;
const CONSUMER_SITE_LISTING_RE = /consumer-site listing/i;

const FORBIDDEN_UI_PATTERNS = [MARSHA_UI_RE, CONSUMER_SITE_LISTING_RE];

const GOVERNANCE_LABELS = [
  "Founder-reviewed UI copy package",
  "Source-grounded from official Marriott/Tribute metadata",
  "Not company-validated",
  "Not Marriott-validated",
];

/** Founder-reviewed repair payloads — premium card + modal copy; no internal metadata in UI fields. */
export const OPENINGS_REPAIR_PACKAGES = [
  {
    marsha: "CUNAN",
    title: "Casa Nizuc, a Tribute Portfolio Resort",
    sort: 0,
    classification: "Future Opening Example",
    imageUrl:
      "https://cache.marriott.com/is/image/marriotts7prod/tx-cunan-casanizuc-from-aldea-42311:Wide-Hor?wid=1336&fit=constrain",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/cunan-casa-nizuc-a-tribute-portfolio-resort/overview/",
    tags: "Resort, Mexico, CALA, Riviera Maya",
    location: "Cancún, Quintana Roo, Mexico",
    meta: "Future Resort Example",
    scenario: "Riviera Maya Leisure Example",
    teaser:
      "Cancún-area resort on the official Tribute consumer map—useful when owners evaluate Marriott affiliation for an independent-character leisure asset before any opening is confirmed.",
    caseSummaryOverview:
      "Cancún-area resort listed on Marriott's Tribute Portfolio consumer site as a future example—leisure positioning on the Riviera Maya corridor.",
    caseSummaryBrandRelevance:
      "Useful when the owner story is an independent-character resort or boutique leisure asset seeking Marriott distribution and Bonvoy without a rigid full-service prototype.",
    caseSummaryOwnerObjective:
      "Shows how Tribute can frame a pre-opening leisure asset as a portfolio example—not a confirmed opening—for affiliation evaluation.",
    caseSummaryInterpretation:
      "Treat published listing timing as illustrative metadata; validate ramp, fees, and PIP scope in the deal model before underwriting from this example.",
    caseSummaryTags: "Resort, Mexico, CALA, Riviera Maya, Future example",
    futureOpeningExampleOnly: true,
  },
  {
    marsha: "BGITY",
    title: "Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort",
    sort: 1,
    classification: "Opening Example",
    imageUrl:
      "https://cache.marriott.com/is/image/marriotts7prod/tx-bgity-colorful-garden-villas-14044:Wide-Hor?wid=1336&fit=constrain",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/bgity-crystal-cove-barbados-a-tribute-portfolio-all-inclusive-resort/overview/",
    tags: "Resort, All-Inclusive, Barbados, CALA",
    location: "St. James, Barbados",
    meta: "Caribbean All-Inclusive Resort",
    scenario: "Barbados Resort Example",
    teaser:
      "Barbados all-inclusive resort operating under Tribute Portfolio—illustrates how the collection can anchor a Caribbean leisure asset with resort-scale positioning within Marriott's network.",
    caseSummaryOverview:
      "St. James, Barbados: all-inclusive resort operating under Tribute Portfolio on Marriott's official property pages—resort-scale leisure positioning in the Caribbean.",
    caseSummaryBrandRelevance:
      "Useful when owners evaluate all-inclusive or resort-scale leisure assets that need Marriott distribution while preserving independent resort character.",
    caseSummaryOwnerObjective:
      "Shows Tribute anchoring a Caribbean leisure asset with full resort programming—not a urban lifestyle or conversion-only play.",
    caseSummaryInterpretation:
      "Compare all-inclusive operating complexity, seasonality, and fee stack against urban Tribute examples before using this as a performance proxy.",
    caseSummaryTags: "Resort, All-Inclusive, Barbados, CALA, Caribbean",
  },
  {
    marsha: "SJUTX",
    title: "Hotel Rumbao, a Tribute Portfolio Hotel",
    sort: 2,
    classification: "Opening Example",
    imageUrl:
      "https://cache.marriott.com/is/image/marriotts7prod/tx-sjutx-sjutx-exterior-view-001-26118:Wide-Hor?wid=1336&fit=constrain",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/sjutx-hotel-rumbao-a-tribute-portfolio-hotel/overview/",
    tags: "Urban, Puerto Rico, CALA, Old San Juan",
    location: "San Juan, Puerto Rico",
    meta: "Historic City Lifestyle Hotel",
    scenario: "Old San Juan Urban Example",
    teaser:
      "Old San Juan lifestyle hotel under Tribute—relevant for urban conversion or repositioning deals where owners want local character with Marriott systems and Bonvoy participation.",
    caseSummaryOverview:
      "Old San Juan urban lifestyle hotel under Tribute Portfolio—design-forward positioning in a heritage city core within Marriott's CALA footprint.",
    caseSummaryBrandRelevance:
      "Illustrates Tribute for urban conversion or repositioning where owners want local character, Bonvoy participation, and Marriott commercial infrastructure.",
    caseSummaryOwnerObjective:
      "Reference for heritage or urban lifestyle assets in CALA gateway cities—not a resort or airport-capture play.",
    caseSummaryInterpretation:
      "Validate ADR, operating complexity, and PIP scope for historic urban cores before modeling from this example.",
    caseSummaryTags: "Urban, Puerto Rico, CALA, Old San Juan, Lifestyle",
  },
  {
    marsha: "LIMTX",
    title: "Humano, Lima, a Tribute Portfolio Hotel",
    sort: 3,
    classification: "Urban Example",
    imageUrl:
      "https://cache.marriott.com/is/image/marriotts7prod/tx-limtx-limtx-full-facade-1-31405:Wide-Ver?wid=377&fit=constrain",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/limtx-humano-lima-a-tribute-portfolio-hotel/overview/",
    tags: "Urban, Peru, South America, Waterfront",
    location: "Lima, Peru",
    meta: "Waterfront Urban Lifestyle Hotel",
    scenario: "South America Urban Example",
    teaser:
      "Malecón waterfront hotel in Lima showing Tribute's South America urban footprint—useful when owners compare lifestyle urban affiliation options within Marriott.",
    caseSummaryOverview:
      "Lima Malecón waterfront urban hotel under Tribute Portfolio—lifestyle positioning on Peru's Pacific coast within Marriott's South America map.",
    caseSummaryBrandRelevance:
      "Useful when owners compare urban lifestyle affiliation options in South America secondary and capital-city corridors.",
    caseSummaryOwnerObjective:
      "Shows Tribute as a waterfront urban lifestyle flag—not resort-scale or all-inclusive—within Marriott's commercial stack.",
    caseSummaryInterpretation:
      "Model waterfront demand mix, seasonality, and operating complexity separately from Caribbean resort examples.",
    caseSummaryTags: "Urban, Peru, South America, Waterfront, Lifestyle",
  },
  {
    marsha: "MDETX",
    title: "Loma, Medellin, a Tribute Portfolio Hotel",
    sort: 4,
    classification: "Urban Example",
    imageUrl:
      "https://cache.marriott.com/is/image/marriotts7prod/tx-mdetx-mdetx-exterior-6-31958:Wide-Hor?wid=1336&fit=constrain",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/mdetx-loma-medellin-a-tribute-portfolio-hotel/overview/",
    tags: "Urban, Colombia, South America",
    location: "Medellín, Colombia",
    meta: "Andean Urban Lifestyle Hotel",
    scenario: "Andean Urban Example",
    teaser:
      "Medellín urban hotel under Tribute—reference for Andean secondary-city lifestyle positioning where owners want independent design sensibility inside Marriott's commercial stack.",
    caseSummaryOverview:
      "Medellín urban lifestyle hotel under Tribute Portfolio—independent design sensibility in an Andean secondary city within Marriott's network.",
    caseSummaryBrandRelevance:
      "Reference for Andean urban lifestyle deals where owners want collection positioning without a rigid full-service box.",
    caseSummaryOwnerObjective:
      "Illustrates Tribute in a design-forward secondary city—not a resort, airport, or all-inclusive anchor.",
    caseSummaryInterpretation:
      "Compare secondary-city demand drivers, fee economics, and ramp timing before using Medellín as a underwriting proxy.",
    caseSummaryTags: "Urban, Colombia, South America, Andean, Lifestyle",
  },
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-openings-momentum-row-creation-writer.md",
  "reports/brand-explorer-openings-momentum-row-creation-writer.json",
  "reports/brand-explorer-openings-momentum-row-review-package.md",
  "reports/brand-explorer-openings-momentum-row-review-package.json",
  "reports/brand-explorer-openings-momentum-source-capture-completion.md",
  "reports/brand-explorer-openings-momentum-source-capture-completion.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-required-section-population-contract.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "fixtures/brand-explorer-presentation-radisson-footprint-openings.json",
  "live Tribute Brand Explorer Presentation rows",
  "live Tribute Brand Asset Registry records",
  "live Curio/Radisson/Ascend/Kimpton footprint.openings rows",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
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
      ...(init.body ? { "Content-Type": "application/json" } : {}),
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function listByFormula(baseId, apiKey, tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (formula) params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function readJsonIfExists(relPath) {
  const full = path.join(ROOT, relPath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return TRIBUTE_RECORD_ID;
  }
  return nz(raw);
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function buildOpeningsBody(pkg) {
  return [pkg.tags, pkg.location, pkg.meta, pkg.scenario, pkg.teaser, pkg.sourceUrl]
    .filter(Boolean)
    .join("\n\n");
}

function buildRepairFields(pkg) {
  return {
    Body: buildOpeningsBody(pkg),
    "Case Summary Overview": pkg.caseSummaryOverview,
    "Case Summary Brand Relevance": pkg.caseSummaryBrandRelevance,
    "Case Summary Owner Objective": pkg.caseSummaryOwnerObjective,
    "Case Summary Interpretation": pkg.caseSummaryInterpretation,
    "Case Summary Tags": pkg.caseSummaryTags,
  };
}

function uiFieldsContainForbidden(text) {
  if (!nz(text)) return false;
  return FORBIDDEN_UI_PATTERNS.some((re) => re.test(text));
}

function collectUiFacingText(pkg, fields) {
  return [
    pkg.title,
    fields.Body,
    fields["Case Summary Overview"],
    fields["Case Summary Brand Relevance"],
    fields["Case Summary Owner Objective"],
    fields["Case Summary Interpretation"],
    fields["Case Summary Tags"],
    pkg.meta,
    pkg.scenario,
    pkg.teaser,
  ]
    .filter(Boolean)
    .join("\n");
}

function parseFootprintOpeningParas(bodyRaw) {
  const paras = String(bodyRaw || "")
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  let summaryHref = "";
  if (paras.length && /^https?:\/\//i.test(paras[paras.length - 1])) {
    summaryHref = paras[paras.length - 1];
    paras.pop();
  }
  let chips = "";
  let loc = "";
  let asset = "";
  let scenario = "";
  let situation = "";
  let why = "";
  let takeaway = "";
  if (paras.length >= 6) {
    [chips, loc, asset, situation, why, takeaway] = paras;
  } else if (paras.length === 5) {
    [chips, loc, asset, scenario, situation] = paras;
  } else if (paras.length === 4) {
    [chips, loc, asset, situation] = paras;
  }
  return { summaryHref, chips, loc, asset, scenario, situation, why, takeaway };
}

function simulateFootprintModal(fields, imageUrl) {
  const body = fields.Body || "";
  const p = parseFootprintOpeningParas(body);
  const overview = nz(fields["Case Summary Overview"]) || nz(p.situation);
  const relevance = nz(fields["Case Summary Brand Relevance"]) || nz(p.why);
  const suggests = nz(fields["Case Summary Owner Objective"]) || nz(p.asset);
  const dealalityTakeaway = nz(fields["Case Summary Interpretation"]) || nz(p.takeaway);
  const ext = p.summaryHref || "";
  return {
    overview: overview || "—",
    relevance: relevance || "—",
    suggests: suggests || "—",
    dealalityTakeaway: dealalityTakeaway || "—",
    externalUrl: ext || "",
    imageUrl: nz(imageUrl),
    modalComplete:
      overview && relevance && suggests && dealalityTakeaway && overview !== "—" && relevance !== "—" && suggests !== "—" && dealalityTakeaway !== "—",
    cardHasImage: /^https?:\/\//i.test(nz(imageUrl)),
  };
}

function firstAttachmentUrl(fields) {
  const image = fields?.Image;
  if (!Array.isArray(image) || !image.length) return "";
  for (const att of image) {
    const u = nz(att?.url);
    if (u) return u;
  }
  return "";
}

function attachmentMaterialized(url) {
  return /airtableusercontent\.com/i.test(nz(url));
}

function filenameFromImageUrl(url, marsha) {
  const base = nz(url).split("?")[0].split("/").pop() || `${marsha}-cover.jpg`;
  return base.includes(".") ? base : `${base}.jpg`;
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
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

async function downloadImageBuffer(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": "Dealality-BrandExplorer/1.0" },
    redirect: "follow",
  });
  if (!res.ok) {
    throw new Error(`Image download failed ${res.status} for ${url}`);
  }
  const contentType = nz(res.headers.get("content-type")) || "image/jpeg";
  const ab = await res.arrayBuffer();
  const buffer = Buffer.from(ab);
  return { buffer, contentType };
}

async function materializePresentationImage({
  baseId,
  apiKey,
  recordId,
  imageUrl,
  marsha,
  dryRun,
}) {
  if (dryRun) {
    return {
      strategy: "content-api-upload",
      materialized: false,
      projectedUrl: imageUrl,
      dryRunOnly: true,
    };
  }
  const { buffer, contentType } = await downloadImageBuffer(imageUrl);
  const filename = filenameFromImageUrl(imageUrl, marsha);
  await uploadFileBytesToAirtable({
    baseId,
    recordId,
    fieldName: IMAGE_FIELD,
    buffer,
    contentType,
    filename,
    apiKey,
  });
  await new Promise((r) => setTimeout(r, 400));
  const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {}, recordId);
  if (!res.ok) {
    throw new Error(json.error?.message || `Reread failed ${recordId}`);
  }
  const url = firstAttachmentUrl(json.fields || {});
  return {
    strategy: "content-api-upload",
    materialized: Boolean(url) && attachmentMaterialized(url),
    attachmentUrl: url,
    bytesUploaded: buffer.length,
  };
}

function diagnoseImageRootCause(liveRow, apiBlock) {
  const airtableImageCount = Array.isArray(liveRow?.fields?.Image) ? liveRow.fields.Image.length : 0;
  const apiImageUrl = nz(apiBlock?.imageUrl);
  if (airtableImageCount === 0 && !apiImageUrl) {
    return {
      category: "attachment_not_materialized_from_external_url",
      detail:
        "v25C-3C wrote Image: [{ url: marriott CDN }] but Airtable Attachment count is 0; brand-library firstAttachmentUrlFromFields returns empty imageUrl.",
      repairPath: "content_api_byte_upload_to_presentation_image_field",
    };
  }
  if (airtableImageCount > 0 && !apiImageUrl) {
    return {
      category: "api_transform_omission",
      detail: "Airtable has attachments but API imageUrl is empty — inspect brand-library firstAttachmentUrlFromFields.",
      repairPath: "api_mapping_repair",
    };
  }
  if (apiImageUrl && !attachmentMaterialized(apiImageUrl)) {
    return {
      category: "non_airtable_cdn_url",
      detail: "imageUrl is external CDN, not airtableusercontent.com — may break or be blocked in UI.",
      repairPath: "materialize_via_content_api",
    };
  }
  return {
    category: "unknown_or_ok",
    detail: airtableImageCount > 0 ? "Image present" : "No image",
    repairPath: airtableImageCount > 0 ? "none" : "content_api_byte_upload_to_presentation_image_field",
  };
}

function findMetadataLeakage(fields, body) {
  const leaks = [];
  const p = parseFootprintOpeningParas(body);
  const candidates = [
    { field: "Body", value: body },
    { field: "Body.metaLine", value: p.asset },
    { field: "Body.scenarioLine", value: p.scenario },
    { field: "Body.teaserLine", value: p.situation },
    { field: "Case Summary Overview", value: fields["Case Summary Overview"] },
    { field: "Case Summary Brand Relevance", value: fields["Case Summary Brand Relevance"] },
    { field: "Case Summary Owner Objective", value: fields["Case Summary Owner Objective"] },
    { field: "Case Summary Interpretation", value: fields["Case Summary Interpretation"] },
    { field: "Case Summary Tags", value: fields["Case Summary Tags"] },
    { field: "Title", value: fields.Title },
  ];
  for (const c of candidates) {
    if (uiFieldsContainForbidden(c.value)) {
      leaks.push({
        field: c.field,
        snippet: nz(c.value).slice(0, 120),
        marsha: MARSHA_UI_RE.test(c.value),
        consumerSiteListing: CONSUMER_SITE_LISTING_RE.test(c.value),
      });
    }
  }
  return leaks;
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-openings-visual-modal-repair-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_BATCH} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_IMAGE}`;
}

export async function buildBrandExplorerOpeningsVisualModalRepairWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  imageRepairApproved = false,
} = {}) {
  const brandRecordId = normalizeBrandInput(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-3D pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const brandApi = await fetchBrandApiShape(brandRecordId);
  const apiOpeningsBlocks = (brandApi?.brandExplorer?.blocks || []).filter(
    (b) => nz(b.slotKey) === OPENINGS_SLOT
  );

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );

  const liveOpenings = presentationRaw
    .map((rec) => ({
      recordId: rec.id,
      fields: rec.fields || {},
      slotKey: nz(rec.fields?.["Slot Key"]),
      title: nz(rec.fields?.Title),
      sortOrder: rec.fields?.["Sort Order"],
    }))
    .filter((r) => r.slotKey === OPENINGS_SLOT);

  const loyaltySnapshot = presentationRaw
    .map((rec) => ({
      recordId: rec.id,
      slotKey: nz(rec.fields?.["Slot Key"]),
      title: nz(rec.fields?.Title),
    }))
    .filter((r) => r.slotKey.startsWith("loyalty."));

  const momentumSnapshot = presentationRaw
    .map((rec) => ({
      recordId: rec.id,
      slotKey: nz(rec.fields?.["Slot Key"]),
      title: nz(rec.fields?.Title),
    }))
    .filter((r) => r.slotKey === "footprint.momentum");

  const geographicSnapshot = presentationRaw
    .map((rec) => ({
      recordId: rec.id,
      slotKey: nz(rec.fields?.["Slot Key"]),
      title: nz(rec.fields?.Title),
    }))
    .filter(
      (r) =>
        r.slotKey.startsWith("footprint.") &&
        r.slotKey !== OPENINGS_SLOT &&
        r.slotKey !== "footprint.momentum"
    );

  const duplicateOpenings =
    liveOpenings.length > OPENINGS_REPAIR_PACKAGES.length
      ? {
          liveCount: liveOpenings.length,
          expectedCount: OPENINGS_REPAIR_PACKAGES.length,
          recordIds: liveOpenings.map((r) => r.recordId),
        }
      : null;

  const imageDiagnosisByRow = [];
  const metadataLeakageBefore = [];
  const modalMissingBefore = [];
  const proposedCleanedMetadata = [];
  const proposedModalCopy = [];
  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const applyBlockers = [];

  if (duplicateOpenings) {
    applyBlockers.push(`duplicate_openings_rows:${duplicateOpenings.liveCount}`);
  }

  for (const pkg of OPENINGS_REPAIR_PACKAGES) {
    const live = liveOpenings.find(
      (r) => nz(r.title) === nz(pkg.title) && Number(r.sortOrder ?? -1) === Number(pkg.sort)
    );
    const apiBlock = apiOpeningsBlocks.find((b) => nz(b.title) === nz(pkg.title)) || null;
    const repairFields = buildRepairFields(pkg);

    if (!live) {
      rowsWouldCreate.push({ marsha: pkg.marsha, title: pkg.title, reason: "missing_live_row" });
      applyBlockers.push(`missing_openings_row:${pkg.marsha}`);
      continue;
    }

    const currentBody = nz(live.fields.Body);
    const imageRoot = diagnoseImageRootCause(live, apiBlock);
    const leaks = findMetadataLeakage(live.fields, currentBody);
    const currentModal = simulateFootprintModal(live.fields, firstAttachmentUrl(live.fields) || apiBlock?.imageUrl);

    imageDiagnosisByRow.push({
      marsha: pkg.marsha,
      title: pkg.title,
      recordId: live.recordId,
      airtableImageCount: Array.isArray(live.fields.Image) ? live.fields.Image.length : 0,
      apiImageUrl: nz(apiBlock?.imageUrl),
      proposedSourceImageUrl: pkg.imageUrl,
      rootCause: imageRoot.category,
      rootCauseDetail: imageRoot.detail,
      proposedImageRepair: imageRoot.repairPath,
      referencePattern:
        "Curio/Radisson openings use materialized airtableusercontent.com attachments + Case Summary columns",
    });

    if (leaks.length) {
      metadataLeakageBefore.push({
        marsha: pkg.marsha,
        recordId: live.recordId,
        leaks,
      });
    }

    const missingModalFields = [];
    if (currentModal.relevance === "—") missingModalFields.push("Why It Is Relevant");
    if (currentModal.dealalityTakeaway === "—") missingModalFields.push("Dealality Takeaway");
    if (currentModal.suggests === "—" || uiFieldsContainForbidden(currentModal.suggests)) {
      missingModalFields.push("What It Suggests About The Brand");
    }
    if (!currentModal.cardHasImage) missingModalFields.push("Card hero image");

    if (missingModalFields.length) {
      modalMissingBefore.push({
        marsha: pkg.marsha,
        recordId: live.recordId,
        missingModalFields,
        currentModal,
      });
    }

    const afterModal = simulateFootprintModal(repairFields, pkg.imageUrl);
    proposedCleanedMetadata.push({
      marsha: pkg.marsha,
      title: pkg.title,
      beforeMetaLine: parseFootprintOpeningParas(currentBody).asset,
      afterMetaLine: pkg.meta,
      beforeScenarioLine: parseFootprintOpeningParas(currentBody).scenario,
      afterScenarioLine: pkg.scenario,
    });

    proposedModalCopy.push({
      marsha: pkg.marsha,
      title: pkg.title,
      propertyOverview: repairFields["Case Summary Overview"],
      whyItIsRelevant: repairFields["Case Summary Brand Relevance"],
      whatItSuggests: repairFields["Case Summary Owner Objective"],
      dealalityTakeaway: repairFields["Case Summary Interpretation"],
      similarPropertyTypes: repairFields["Case Summary Tags"],
      externalSourceLink: pkg.sourceUrl,
      projectedModalComplete: afterModal.modalComplete,
    });

    const uiText = collectUiFacingText(pkg, repairFields);
    if (uiFieldsContainForbidden(uiText)) {
      applyBlockers.push(`forbidden_ui_copy_in_proposed_repair:${pkg.marsha}`);
    }
    if (!afterModal.modalComplete) {
      applyBlockers.push(`modal_incomplete_after_repair:${pkg.marsha}`);
    }

    if (pkg.marsha === CASA_NIZUC_MARSHA) {
      const casaBody = nz(repairFields.Body).toLowerCase();
      if (
        /opened|debuted|now open|completed opening/i.test(casaBody) &&
        !/before any opening|future example|pre-opening/i.test(casaBody)
      ) {
        applyBlockers.push("casa_nizuc_completed_opening_claim_blocked");
      }
    }

    const needsCopyRepair =
      currentBody !== repairFields.Body ||
      nz(live.fields["Case Summary Brand Relevance"]) !== repairFields["Case Summary Brand Relevance"] ||
      leaks.length > 0;
    const needsImageRepair = !currentModal.cardHasImage;

    if (needsCopyRepair || needsImageRepair) {
      rowsWouldUpdate.push({
        marsha: pkg.marsha,
        recordId: live.recordId,
        title: pkg.title,
        action: "update",
        needsCopyRepair,
        needsImageRepair,
        fields: repairFields,
        imageUrl: pkg.imageUrl,
      });
    }
  }

  for (const row of rowsWouldUpdate) {
    if (row.needsImageRepair) {
      const projected = simulateFootprintModal(row.fields, row.imageUrl);
      if (!projected.cardHasImage && !imageRepairApproved) {
        /* dry-run: image repair is projected via content API */
      }
    }
  }

  const proposedUiClean = rowsWouldUpdate.every((r) => {
    const pkg = OPENINGS_REPAIR_PACKAGES.find((p) => p.marsha === r.marsha);
    return pkg && !uiFieldsContainForbidden(collectUiFacingText(pkg, r.fields));
  });

  const allMarshaRemovedFromUi =
    metadataLeakageBefore.some((m) => m.leaks.some((l) => l.marsha)) && proposedUiClean;

  const allConsumerSiteListingRemoved =
    metadataLeakageBefore.some((m) => m.leaks.some((l) => l.consumerSiteListing)) && proposedUiClean;

  const projectedAllCardsHaveImages = rowsWouldUpdate.every((r) => hasVal(r.imageUrl));

  const projectedModalComplete = proposedModalCopy.every((m) => m.projectedModalComplete);

  if (!projectedAllCardsHaveImages) {
    applyBlockers.push("opening_card_missing_image_after_repair");
  }
  if (!projectedModalComplete) {
    applyBlockers.push("modal_required_fields_incomplete_after_repair");
  }

  const applyGatesReady = apply && approveBatch && founderReviewed && imageRepairApproved;
  const canApply = applyGatesReady && applyBlockers.length === 0 && rowsWouldUpdate.length > 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;
  const imageRepairResults = [];

  if (canApply) {
    const updated = [];
    const errors = [];
    for (const row of rowsWouldUpdate) {
      try {
        if (row.needsCopyRepair) {
          const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
            method: "PATCH",
            body: JSON.stringify({ fields: row.fields, typecast: true }),
          }, row.recordId);
          if (!res.ok) {
            errors.push({ recordId: row.recordId, step: "copy_patch", message: json.error?.message || res.status });
            continue;
          }
        }
        if (row.needsImageRepair) {
          const imgResult = await materializePresentationImage({
            baseId,
            apiKey,
            recordId: row.recordId,
            imageUrl: row.imageUrl,
            marsha: row.marsha,
            dryRun: false,
          });
          imageRepairResults.push({ recordId: row.recordId, marsha: row.marsha, ...imgResult });
          if (!imgResult.materialized) {
            errors.push({
              recordId: row.recordId,
              step: "image_materialize",
              message: "Content API upload did not materialize attachment on reread",
            });
            applyBlockers.push(`image_not_materialized:${row.marsha}`);
          }
        }
        updated.push({ recordId: row.recordId, marsha: row.marsha, title: row.title });
        await new Promise((r) => setTimeout(r, 280));
      } catch (err) {
        errors.push({ recordId: row.recordId, step: "apply", message: err?.message || String(err) });
      }
    }
    airtableModified = updated.length > 0 && errors.length === 0;
    applyResults = { updated, errors, imageRepairResults };

    const brandBasicsAfter = await fetchBrandBasics(brandRecordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: [...new Set(applyBlockers)] };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const casaNizucPkg = OPENINGS_REPAIR_PACKAGES.find((p) => p.marsha === CASA_NIZUC_MARSHA);
  const casaNizucRemainsFutureExample =
    casaNizucPkg?.futureOpeningExampleOnly === true &&
    !/now open|debuted|completed opening/i.test(buildOpeningsBody(casaNizucPkg));

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C3DWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: "tribute-portfolio",
    },
    marriottValidationImplied: false,
    governanceLabels: [...GOVERNANCE_LABELS],
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-openings-visual-modal-repair-writer.js",
      "scripts/brand-explorer-openings-visual-modal-repair-writer.mjs",
      "docs/data-intelligence/brand-explorer-openings-visual-modal-repair-writer-v25C-3D.md",
      "reports/brand-explorer-openings-visual-modal-repair-writer.md",
      "reports/brand-explorer-openings-visual-modal-repair-writer.json",
      "package.json",
    ],
    rootCauseOfMissingImages:
      "Marriott CDN URLs were stored as external URL attachments during v25C-3C but did not materialize into Airtable Image attachments; brand-library.js exposes imageUrl only from materialized attachments, so the UI renders empty hero images.",
    imageDiagnosisByRow,
    metadataLeakageBefore,
    proposedCleanedMetadata,
    modalMissingBefore,
    proposedModalCopy,
    rowsWouldUpdate,
    rowsWouldCreate,
    duplicateOpeningsDetected: Boolean(duplicateOpenings),
    duplicateOpenings,
    marshaCodesRemovedFromUiFields: metadataLeakageBefore.length > 0,
    marshaCodesRemovedFromUiFieldsAfterRepair: allMarshaRemovedFromUi || metadataLeakageBefore.length === 0,
    consumerSiteListingRemovedFromUiFields: metadataLeakageBefore.some((m) =>
      m.leaks.some((l) => l.consumerSiteListing)
    ),
    consumerSiteListingRemovedFromUiFieldsAfterRepair:
      allConsumerSiteListingRemoved || !metadataLeakageBefore.some((m) => m.leaks.some((l) => l.consumerSiteListing)),
    allOpeningCardsWillHaveImagesAfterRepair: projectedAllCardsHaveImages,
    modalRequiredFieldsCompleteAfterRepair: projectedModalComplete,
    casaNizucRemainsFutureExample,
    loyaltyRowsUntouched: true,
    loyaltyRowsSnapshot: loyaltySnapshot,
    momentumRowsUntouched: true,
    momentumRowsSnapshot: momentumSnapshot,
    geographicFootprintRowsUntouched: true,
    geographicFootprintRowsSnapshot: geographicSnapshot,
    brandBasicsUntouched: true,
    registryAssetsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      imageRepairApproved,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers: [...new Set(applyBlockers)],
    applyResults,
    imageRepairStrategy: "content_api_byte_upload_to_presentation_image_field",
    exactApplyCommand: buildApplyCommand(),
    idempotentAfterApply: rowsWouldUpdate.length === 0,
    doesNotDo: [
      "Create new footprint.openings rows (updates existing v25C-3C rows only)",
      "Modify loyalty or momentum rows",
      "Modify geographic footprint region rows",
      "Change Brand Basics or Company Validated",
      "Modify Brand Asset Registry records",
      "Write governance labels into presentation Body copy",
      "Imply Marriott validated anything",
      "Describe Casa Nizuc as a completed opening",
    ],
  };
}

export function buildBrandExplorerOpeningsVisualModalRepairWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Openings Visual + Modal Repair Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- v25C-3D exists: **${report.v25C3DWriterExists ? "yes" : "no"}**`,
    "",
    "## Summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Rows would update | ${report.rowsWouldUpdate.length} |`,
    `| Rows would create | ${report.rowsWouldCreate.length} |`,
    `| Root cause (images) | ${report.rootCauseOfMissingImages} |`,
    `| MARSHA removed from UI | ${report.marshaCodesRemovedFromUiFieldsAfterRepair ? "yes" : "pending repair"} |`,
    `| Consumer-site listing removed | ${report.consumerSiteListingRemovedFromUiFieldsAfterRepair ? "yes" : "pending repair"} |`,
    `| All cards will have images | ${report.allOpeningCardsWillHaveImagesAfterRepair ? "yes" : "no"} |`,
    `| Modal fields complete | ${report.modalRequiredFieldsCompleteAfterRepair ? "yes" : "no"} |`,
    `| Casa Nizuc future example only | ${report.casaNizucRemainsFutureExample ? "yes" : "no"} |`,
    `| Loyalty untouched | ${report.loyaltyRowsUntouched ? "yes" : "no"} |`,
    `| Momentum untouched | ${report.momentumRowsUntouched ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Image diagnosis by row",
    "",
  ];

  for (const d of report.imageDiagnosisByRow) {
    lines.push(
      `### ${d.title}`,
      "",
      `- Record: \`${d.recordId}\``,
      `- Airtable Image count: **${d.airtableImageCount}**`,
      `- API imageUrl: ${d.apiImageUrl ? `\`${d.apiImageUrl.slice(0, 60)}…\`` : "**empty**"}`,
      `- Root cause: **${d.rootCause}**`,
      `- Repair: **${d.proposedImageRepair}**`,
      ""
    );
  }

  if (report.metadataLeakageBefore?.length) {
    lines.push("## UI metadata leakage (before repair)", "");
    for (const m of report.metadataLeakageBefore) {
      lines.push(`### ${m.marsha}`, "");
      for (const l of m.leaks) {
        lines.push(`- \`${l.field}\`: ${l.snippet}`);
      }
      lines.push("");
    }
  }

  if (report.proposedCleanedMetadata?.length) {
    lines.push("## Proposed cleaned card metadata", "");
    for (const m of report.proposedCleanedMetadata) {
      lines.push(
        `- **${m.title}**: meta \`${m.beforeMetaLine}\` → **${m.afterMetaLine}**; scenario \`${m.beforeScenarioLine}\` → **${m.afterScenarioLine}**`
      );
    }
    lines.push("");
  }

  if (report.proposedModalCopy?.length) {
    lines.push("## Proposed modal copy", "");
    for (const m of report.proposedModalCopy) {
      lines.push(
        `### ${m.title}`,
        "",
        `- Property overview: ${m.propertyOverview}`,
        `- Why it is relevant: ${m.whyItIsRelevant}`,
        `- What it suggests: ${m.whatItSuggests}`,
        `- Dealality takeaway: ${m.dealalityTakeaway}`,
        `- Tags: ${m.similarPropertyTypes}`,
        `- External link: ${m.externalSourceLink}`,
        ""
      );
    }
  }

  if (report.applyBlockers?.length) {
    lines.push("## Apply blockers", "");
    for (const b of report.applyBlockers) {
      lines.push(`- ${b}`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command", "", "```bash", report.exactApplyCommand, "```", "");

  if (report.applyResults) {
    lines.push(
      "## Apply results",
      "",
      `- Updated: ${report.applyResults.updated?.length || 0}`,
      `- Errors: ${report.applyResults.errors?.length || 0}`,
      `- Blocked: ${report.applyResults.blocked ? "yes" : "no"}`,
      ""
    );
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) {
    lines.push(`- ${item}`);
  }

  return lines.join("\n");
}
