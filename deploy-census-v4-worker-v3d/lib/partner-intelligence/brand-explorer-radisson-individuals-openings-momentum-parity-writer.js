/**
 * Brand Explorer Radisson Individuals Openings Completion +
 * Momentum Tribute-Parity Writer v31M.
 *
 * Completes footprint.openings to 3 visible rows where gates pass;
 * rewrites footprint.momentum to Tribute-quality editorial standards.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-openings-momentum-parity-writer-v31M.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { uploadFileBytesToAirtable } from "../dealality/airtable-upload-attachment.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME as TRIBUTE_BRAND_NAME } from "./tribute-portfolio-brand-package.js";
import {
  POLISHED_MOMENTUM_ROWS as TRIBUTE_MOMENTUM_REFERENCE,
  POLISHED_OPENINGS_CARDS as TRIBUTE_OPENINGS_REFERENCE,
  MOMENTUM_SLOT as TRIBUTE_MOMENTUM_SLOT,
} from "./brand-explorer-openings-momentum-row-review-package.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import { findRegistryAssetForPresentationRow } from "./brand-explorer-brand-asset-image-governance.js";
import {
  classifyRegistryAsset,
  assessReactivationEligibility,
} from "./brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";
import {
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
  OPENINGS_SLOT,
  OPENINGS_PROPERTY_CATALOG,
  PRESS_KIT_URL,
  buildOwnerFacingOpeningsCopy,
  canReactivateOpeningsRow,
} from "./brand-explorer-radisson-individuals-openings-rebuild-writer.js";
import {
  classifyMomentumSourceUrl,
  proposedLinkLabelForSource,
  legacyFrontendLinkLabel,
  assessTitleQuality,
  assessBodyQuality,
  assessLinkLabelQuality,
  MOMENTUM_SLOT,
} from "./brand-explorer-radisson-individuals-momentum-editorial-repair-writer.js";
import {
  fetchDurablePropertyImage,
  isDurableSourcePageUrl,
} from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  isDoNotUseRecord,
  isFounderApprovedRecord,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

export const WRITER_VERSION = "31M";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-openings-momentum-parity-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-openings-momentum-parity-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-openings-momentum-parity-writer-v31M.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31M-openings-momentum-parity";
export const APPLY_FLAG_APPROVED_ONLY = "--confirm-approved-assets-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-individuals-momentum-copy";

export const RADISSON_PRESS_KIT = PRESS_KIT_URL;

/** Tribute-parity momentum packages — differentiated insights + specific link labels. */
export const MOMENTUM_PARITY_PACKAGES = Object.freeze([
  {
    recordId: "rec0an5blfW4FtMfE",
    sort: 0,
    dateLine: "2024",
    polishedTitle: "Radisson Individuals Expands Across CALA",
    polishedSummary:
      "Choice Hotels highlighted Radisson Individuals growth across Colombia and Panama—illustrating how the hand-selected soft collection scales within Choice Privileges distribution while preserving each hotel's local identity.",
    sourceUrl: RADISSON_PRESS_KIT,
    proposedLinkLabel: "View Choice Hotels Press Kit",
    ownerImplication:
      "Portfolio-scale CALA signal for owners comparing soft-brand breadth inside Choice Hotels.",
  },
  {
    recordId: "recb0WzRRu6jrev4c",
    sort: 1,
    dateLine: "2024",
    polishedTitle: "Colombia Urban and Heritage Markets Add Individuals Properties",
    polishedSummary:
      "Medellín lifestyle and Cartagena heritage examples show how Individuals positions boutique and independent hotels inside Choice's upper-upscale CALA footprint—each retaining property-specific character rather than a uniform prototype.",
    sourceUrl: "https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb017",
    proposedLinkLabel: "View Cartagena Property Listing",
    ownerImplication:
      "Dual-market Colombia reference (urban lifestyle + heritage leisure) for conversion underwriting context.",
  },
  {
    recordId: "recpIgmBNBEMXVEda",
    sort: 2,
    dateLine: "2024",
    polishedTitle: "Panama Capital Corridor Extends Individuals Reach",
    polishedSummary:
      "Panama City and regional Panama examples extend Choice's hand-selected upper-upscale presence in Central America—useful when owners weigh gateway-corridor affiliation where distribution leverage matters alongside local differentiation.",
    sourceUrl: "https://www.choicehotels.com/panama/panama-city/radisson-individuals-hotels/pa006",
    proposedLinkLabel: "View Panama City Property Listing",
    ownerImplication:
      "Gateway CALA corridor example for capital-market owners evaluating Individuals distribution fit.",
  },
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const IMAGE_FIELD = "Image";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-complete-build-orchestrator.md",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-radisson-individuals-openings-rebuild-writer.md",
  "reports/brand-explorer-radisson-individuals-openings-rebuild-writer.json",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.md",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.json",
  "live Radisson Individuals footprint.openings rows",
  "live Radisson Individuals footprint.momentum rows",
  "live Radisson Individuals Brand Asset Registry rows",
  "live API response for Radisson Individuals",
  "Tribute Portfolio footprint.openings rows",
  "Tribute Portfolio footprint.momentum rows",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "docs/brand-explorer-presentation-slots.md",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-openings-momentum-parity-writer.js",
  "scripts/brand-explorer-radisson-individuals-openings-momentum-parity-writer.mjs",
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

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
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

function parseParagraphs(body) {
  return normalizeBody(body)
    .split(/\n\n+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function firstHttp(paragraphs) {
  return paragraphs.find((p) => /^https?:\/\//i.test(p)) || "";
}

export function openingIsCompleteRow(row) {
  const title = nz(row?.title);
  const image = nz(row?.imageUrl);
  const paras = parseParagraphs(row?.body);
  const textParas = paras.filter((p) => !/^https?:\/\//i.test(p));
  const location = textParas[1] || "";
  const summary = textParas[3] || textParas[4] || textParas[0] || "";
  const url = nz(row?.summaryUrl) || firstHttp(paras);
  return {
    complete: [title, image, location, summary, url].every(hasVal),
    missing: [
      !title && "title",
      !image && "imageUrl",
      !location && "location",
      !summary && "summary",
      !url && "sourceUrl",
    ].filter(Boolean),
  };
}

export function momentumIsCompleteRow(row) {
  const title = nz(row?.title);
  const paras = parseParagraphs(row?.body);
  const date = paras[0] || "";
  const summary = paras.filter((p) => !/^https?:\/\//i.test(p)).slice(1).join(" ");
  const source = firstHttp(paras);
  return {
    complete: [title, date, summary, source].every(hasVal),
    missing: [
      !title && "title",
      !date && "dateLine",
      !summary && "summary",
      !source && "sourceUrl",
    ].filter(Boolean),
  };
}

function parseMomentumBody(body) {
  const paras = parseParagraphs(body);
  const dateLine = paras[0] || "";
  let sourceUrl = "";
  const descParts = [];
  for (let i = 1; i < paras.length; i++) {
    if (/^https?:\/\//i.test(paras[i])) sourceUrl = paras[i];
    else descParts.push(paras[i]);
  }
  return { dateLine, description: descParts.join("\n\n"), sourceUrl };
}

function buildMomentumBody(pkg) {
  return normalizeBody([pkg.dateLine, pkg.polishedSummary, pkg.sourceUrl].join("\n\n"));
}

export function v31mWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-openings-momentum-parity-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31M`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31M supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

function catalogForRow(recordId) {
  return OPENINGS_PROPERTY_CATALOG.find((c) => c.presentationRecordId === recordId) || null;
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
    body: normalizeBody(f.Body),
    sortOrder: f["Sort Order"],
    active: f.Active !== false,
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

function normalizeOpeningsRow(rec) {
  const row = normalizePresentationRow(rec);
  if (row.slotKey !== OPENINGS_SLOT) return null;
  return row;
}

function normalizeMomentumRow(rec) {
  const row = normalizePresentationRow(rec);
  if (row.slotKey !== MOMENTUM_SLOT) return null;
  return row;
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

function findDedicatedRegistry(registryAssets, presentationRecordId, catalog) {
  const byNotes = registryAssets.filter(
    (a) =>
      nz(a.sourceNotes).includes(presentationRecordId) &&
      !isSharedSourceReferenceRegistry(a)
  );
  if (byNotes.length) {
    const v31l = byNotes.find((a) => /v31L/i.test(nz(a.stagingRunId)));
    return v31l || byNotes[byNotes.length - 1];
  }
  const bySlot = registryAssets.filter((a) => nz(a.recommendedExplorerSlot) === OPENINGS_SLOT);
  return (
    bySlot.find(
      (a) =>
        !isSharedSourceReferenceRegistry(a) &&
        nz(a.assetName).toLowerCase().includes(catalog?.marketCity?.toLowerCase() || "")
    ) || null
  );
}

function isRegistrySharedAcrossOpenings(registryId, openingsRows, registryAssets) {
  if (!registryId) return false;
  let count = 0;
  for (const row of openingsRows) {
    const dedicated = findDedicatedRegistry(registryAssets, row.recordId, catalogForRow(row.recordId));
    if (dedicated?.id === registryId) count += 1;
  }
  return count > 1;
}

function isSharedSourceReferenceRegistry(registry) {
  if (!registry) return false;
  const name = nz(registry.assetName).toLowerCase();
  if (name.includes("press kit") && name.includes("source reference")) return true;
  if (!registry.attachmentUrl && !registry.sourceUrl) return true;
  return false;
}

function registryAssetImageUrl(asset) {
  return nz(asset?.attachmentUrl || asset?.sourceUrl);
}

export function classifyOpeningRowStatus({
  row,
  apiBlock,
  dedicatedRegistry,
  registryMatch,
  completeCheck,
  reactivateCheck,
  v31eReactivation,
  durableImage,
}) {
  if (completeCheck.complete && row.visibleInExplorer && apiBlock) {
    return { classification: "complete_visible", reason: "visible_in_api_with_complete_fields" };
  }
  if (registryMatch && isDoNotUseRecord(registryMatch)) {
    return { classification: "keep_quarantined_wrong_image_risk", reason: "linked_do_not_use_registry" };
  }
  if (
    dedicatedRegistry &&
    (isFounderApprovedRecord(dedicatedRegistry) || classifyRegistryAsset(dedicatedRegistry) === "Approved")
  ) {
    if (row.quarantined && reactivateCheck.ok && !row.hasImage) {
      return { classification: "needs_image_materialization", reason: "approved_registry_pending_presentation_image" };
    }
    if (row.quarantined && reactivateCheck.ok && row.hasImage) {
      return { classification: "ready_to_reactivate", reason: reactivateCheck.reason };
    }
  }
  if (v31eReactivation?.eligible && reactivateCheck.ok) {
    if (!row.hasImage && registryAssetImageUrl(dedicatedRegistry)) {
      return { classification: "needs_image_materialization", reason: "approved_registry_pending_presentation_image" };
    }
    return { classification: "ready_to_reactivate", reason: reactivateCheck.reason };
  }
  if (dedicatedRegistry && !isFounderApprovedRecord(dedicatedRegistry) && classifyRegistryAsset(dedicatedRegistry) !== "Approved") {
    return { classification: "needs_founder_image_approval", reason: "registry_pending_founder_review" };
  }
  if (!row.hasImage && durableImage?.ok && !dedicatedRegistry) {
    return { classification: "needs_founder_image_approval", reason: "durable_image_staged_pending_registry_approval" };
  }
  if (!isDurableSourcePageUrl(row.summaryUrl || row.sourcePageUrl || catalogForRow(row.recordId)?.sourcePageUrl)) {
    if (!completeCheck.complete) {
      return { classification: "needs_source_page_url", reason: "missing_durable_source_page" };
    }
  }
  if (completeCheck.complete && !row.visibleInExplorer) {
    return { classification: "keep_quarantined_incomplete", reason: "complete_but_quarantined_from_api" };
  }
  return { classification: "keep_quarantined_incomplete", reason: reactivateCheck.reason || "gates_not_met" };
}

function scoreOpeningCandidate(row, status, catalog, durableImage) {
  let score = 0;
  if (status.classification === "ready_to_reactivate") score += 200;
  if (status.classification === "needs_image_materialization") score += 150;
  if (status.classification === "complete_visible") score += 100;
  if (row.hasImage) score += 80;
  if (durableImage?.ok) score += 50;
  if (catalog?.sourcePageUrl && catalog.sourcePageUrl !== PRESS_KIT_URL) score += 40;
  if (!findInternalLanguageInRow(row).length) score += 30;
  if (status.classification === "needs_founder_image_approval") score += 10;
  return score;
}

function compareMomentumToTribute(radissonRows, tributeRows) {
  const tributeParsed = tributeRows.map((r) => {
    const p = parseMomentumBody(r.body);
    return {
      recordId: r.recordId,
      title: r.title,
      dateLine: p.dateLine,
      sourceUrl: p.sourceUrl,
      linkLabel: legacyFrontendLinkLabel(p.sourceUrl),
      bodyExcerpt: p.description.slice(0, 120),
    };
  });

  const radissonParsed = radissonRows.map((r) => {
    const p = parseMomentumBody(r.body);
    return {
      recordId: r.recordId,
      title: r.title,
      dateLine: p.dateLine,
      sourceUrl: p.sourceUrl,
      linkLabel: legacyFrontendLinkLabel(p.sourceUrl),
      bodyExcerpt: p.description.slice(0, 120),
    };
  });

  const uniqueRadissonUrls = new Set(radissonParsed.map((r) => r.sourceUrl).filter(Boolean));
  const uniqueTributeUrls = new Set(tributeParsed.map((r) => r.sourceUrl).filter(Boolean));
  const uniqueRadissonDates = new Set(radissonParsed.map((r) => r.dateLine).filter(Boolean));
  const uniqueTributeDates = new Set(tributeParsed.map((r) => r.dateLine).filter(Boolean));

  const sameGenericAnnouncement =
    uniqueRadissonUrls.size === 1 && uniqueRadissonUrls.has(RADISSON_PRESS_KIT);

  const repetitiveLinkLabel =
    radissonParsed.filter((r) => /view choice hotels announcement/i.test(r.linkLabel)).length >= 2;

  return {
    tributeRowCount: tributeRows.length,
    radissonRowCount: radissonRows.length,
    tributeReferencePackages: TRIBUTE_MOMENTUM_REFERENCE.length,
    tributeUniqueYears: [...uniqueTributeDates],
    radissonUniqueYears: [...uniqueRadissonDates],
    tributeUniqueSourceUrls: uniqueTributeUrls.size,
    radissonUniqueSourceUrls: uniqueRadissonUrls.size,
    tributeUsesVariedPropertyLinks: uniqueTributeUrls.size >= 4,
    radissonUsesSinglePressKit: sameGenericAnnouncement,
    repetitiveGenericLinkLabel: repetitiveLinkLabel,
    tributeLinkLabelExamples: [...new Set(tributeParsed.map((r) => r.linkLabel))].slice(0, 6),
    tributeTitleStyle: "Proper-case insight-led headlines with property/market specificity",
    radissonGapSummary: sameGenericAnnouncement
      ? "All momentum rows share one press-kit URL — feels repetitive vs Tribute property-specific links."
      : "Source URLs partially differentiated.",
    editorialGap:
      repetitiveLinkLabel
        ? 'Frontend falls back to generic "View Choice Hotels announcement" on multiple rows.'
        : "Link labels acceptable.",
    yearSupportNote:
      uniqueTributeDates.size > 1
        ? "Tribute uses varied month/year labels from consumer-site metadata; Radisson Individuals has 2024 press-kit support only — differentiated copy per row is required to avoid repetition."
        : "Year labels comparable.",
    tributeRowsSample: tributeParsed.slice(0, 3),
    radissonRowsSample: radissonParsed,
  };
}

async function downloadImageBuffer(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Image download failed ${res.status}`);
  const buffer = Buffer.from(await res.arrayBuffer());
  const contentType = res.headers.get("content-type") || "image/jpeg";
  return { buffer, contentType };
}

async function materializePresentationImage({ baseId, apiKey, recordId, imageUrl, slotKey }) {
  const { buffer, contentType } = await downloadImageBuffer(imageUrl);
  const filename = `${slotKey.replace(/\./g, "-")}-opening.jpg`.slice(0, 120);
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
  if (!res.ok) throw new Error(json.error?.message || `Reread failed ${recordId}`);
  const url =
    Array.isArray(json.fields?.Image) && json.fields.Image[0]?.url
      ? nz(json.fields.Image[0].url)
      : null;
  return { materialized: Boolean(url), attachmentUrl: url };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return `npm run brand-explorer-radisson-individuals-openings-momentum-parity-writer -- --brand ${brand} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_APPROVED_ONLY} ${APPLY_FLAG_NO_VALIDATION} ${APPLY_FLAG_FOUNDER}`;
}

export async function buildBrandExplorerRadissonIndividualsOpeningsMomentumParityWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  approvedAssetsOnly = false,
  noValidationClaim = false,
  founderReviewed = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const [presentationRaw, registryAssetsRaw, tributeRaw, brandApi] = await Promise.all([
    listPresentationRowsRaw(baseId, apiKey, target.recordId, target.name),
    listRegistryAssetsForBrand(target.recordId).catch(() => []),
    listPresentationRowsRaw(baseId, apiKey, TRIBUTE_RECORD_ID, TRIBUTE_BRAND_NAME),
    fetchBrandApiShape(target.recordId),
  ]);

  const openingsRows = presentationRaw.map(normalizeOpeningsRow).filter(Boolean);
  const momentumRows = presentationRaw.map(normalizeMomentumRow).filter(Boolean);
  const tributeMomentum = tributeRaw.map(normalizeMomentumRow).filter(Boolean);
  const tributeOpenings = tributeRaw
    .map(normalizeOpeningsRow)
    .filter(Boolean);

  const apiOpeningBlocks = (brandApi?.brandExplorer?.blocks || []).filter(
    (b) => nz(b.slotKey) === OPENINGS_SLOT
  );
  const apiBlockByRecordId = new Map(
    apiOpeningBlocks.map((b) => [nz(b.recordId || b.id), b])
  );

  const openingsCompletionAudit = [];
  const rowsProposedForReactivation = [];
  const rowsKeptHidden = [];
  const proposedOpeningUpdates = [];
  const proposedMomentumUpdates = [];
  const momentumCopyBeforeAfter = [];
  const momentumLinkBeforeAfter = [];

  let visibleCount = 0;
  let completeCount = 0;
  let blockedByImageApproval = 0;
  let blockedByMaterialization = 0;
  let blockedByDisplayStatus = 0;
  let blockedBySourceMismatch = 0;

  const candidateScores = [];

  for (const row of openingsRows) {
    const catalog = catalogForRow(row.recordId);
    const dedicatedRegistry = findDedicatedRegistry(registryAssetsRaw, row.recordId, catalog);
    const registryMatch =
      dedicatedRegistry || findRegistryAssetForPresentationRow(registryAssetsRaw, row);
    const sharedRegistryRisk =
      registryMatch?.id &&
      isRegistrySharedAcrossOpenings(registryMatch.id, openingsRows, registryAssetsRaw);

    let durableImage = null;
    if (catalog?.sourcePageUrl && catalog.sourcePageUrl !== PRESS_KIT_URL) {
      durableImage = await fetchDurablePropertyImage({
        sourcePageUrl: catalog.sourcePageUrl,
        officialPropertyPageUrl: catalog.officialPropertyPageUrl,
        titleKeywords: catalog.titleKeywords || [],
      });
    }

    let rebuiltCopy = null;
    try {
      rebuiltCopy = buildOwnerFacingOpeningsCopy({ row, catalog });
    } catch {
      rebuiltCopy = null;
    }

    const reactivateCheck = canReactivateOpeningsRow({
      row,
      dedicatedRegistry:
        dedicatedRegistry && !sharedRegistryRisk ? dedicatedRegistry : null,
      rebuiltCopy,
    });

    const v31eReactivation =
      dedicatedRegistry && !sharedRegistryRisk
        ? assessReactivationEligibility(row, dedicatedRegistry, { brandName: target.name })
        : { eligible: false, blockReason: sharedRegistryRisk ? "shared_registry_across_rows" : "no_dedicated_registry" };

    const completeCheck = openingIsCompleteRow(row);
    const apiBlock = apiBlockByRecordId.get(row.recordId);
    const visibleInApi = Boolean(apiBlock);

    if (visibleInApi) visibleCount += 1;
    if (completeCheck.complete) completeCount += 1;
    if (row.quarantined) blockedByDisplayStatus += 1;
    if (
      !dedicatedRegistry ||
      (!isFounderApprovedRecord(dedicatedRegistry) && classifyRegistryAsset(dedicatedRegistry) !== "Approved")
    ) {
      if (!row.hasImage) blockedByImageApproval += 1;
    }
    if (
      (v31eReactivation.eligible || reactivateCheck.ok) &&
      !row.hasImage &&
      registryAssetImageUrl(dedicatedRegistry)
    ) {
      blockedByMaterialization += 1;
    }
    if (registryMatch && isDoNotUseRecord(registryMatch)) blockedBySourceMismatch += 1;

    const status = classifyOpeningRowStatus({
      row,
      apiBlock,
      dedicatedRegistry,
      registryMatch,
      completeCheck,
      reactivateCheck,
      v31eReactivation,
      durableImage,
    });

    const audit = {
      recordId: row.recordId,
      title: row.title,
      property: catalog?.propertyName || row.location || row.title,
      externalDisplayStatus: row.externalDisplayStatus || null,
      active: row.active,
      visibleInApi,
      hasImage: row.hasImage,
      imageMaterialized: row.hasImage,
      registryLinked: Boolean(registryMatch?.id),
      registryRecordId: dedicatedRegistry?.id || registryMatch?.id || null,
      registryApprovalStatus: dedicatedRegistry
        ? classifyRegistryAsset(dedicatedRegistry)
        : registryMatch
          ? classifyRegistryAsset(registryMatch)
          : "none",
      sharedRegistryRisk,
      sourcePageUrl: row.sourcePageUrl || catalog?.sourcePageUrl || null,
      completeRow: completeCheck.complete,
      completeMissingFields: completeCheck.missing,
      classification: status.classification,
      classificationReason: status.reason,
      reactivateEligible: reactivateCheck.ok || v31eReactivation.eligible,
      durableImageResolved: durableImage?.ok || false,
    };

    openingsCompletionAudit.push(audit);
    candidateScores.push({
      recordId: row.recordId,
      title: row.title,
      score: scoreOpeningCandidate(row, status, catalog, durableImage),
      classification: status.classification,
    });

    const shouldReactivate =
      (status.classification === "ready_to_reactivate" ||
        status.classification === "needs_image_materialization") &&
      reactivateCheck.ok &&
      v31eReactivation.eligible;

    if (shouldReactivate && rebuiltCopy) {
      rowsProposedForReactivation.push({
        recordId: row.recordId,
        title: row.title,
        classification: status.classification,
        registryRecordId: dedicatedRegistry?.id,
      });

      const fields = {
        Body: rebuiltCopy.body,
        "Case Summary Overview": rebuiltCopy.caseSummaryOverview,
        "Case Summary Owner Objective": rebuiltCopy.caseSummaryOwnerObjective,
        "Case Summary Brand Relevance": rebuiltCopy.caseSummaryBrandRelevance,
        "Case Summary Interpretation": rebuiltCopy.caseSummaryInterpretation,
        "Case Summary Tags": rebuiltCopy.caseSummaryTags,
        "External Display Status": null,
        "Source Page URL": catalog?.sourcePageUrl || row.sourcePageUrl || null,
        "Summary URL": rebuiltCopy.summaryUrl || catalog?.sourcePageUrl || null,
        "Brand Name": target.name,
        Brand: [target.recordId],
      };

      proposedOpeningUpdates.push({
        recordId: row.recordId,
        fields,
        materializeImageUrl: !row.hasImage ? registryAssetImageUrl(dedicatedRegistry) || durableImage?.imageUrl : null,
        reactivate: true,
      });
    } else {
      rowsKeptHidden.push({
        recordId: row.recordId,
        title: row.title,
        classification: status.classification,
        reason: status.reason,
      });
    }
  }

  const alreadyCompleteVisible = openingsCompletionAudit.filter(
    (a) => a.classification === "complete_visible"
  );
  const neededAdditional = Math.max(0, 3 - alreadyCompleteVisible.length);
  const rankedCandidates = candidateScores
    .filter((c) => !alreadyCompleteVisible.some((v) => v.recordId === c.recordId))
    .sort((a, b) => b.score - a.score);
  const bestTwoAdditional = rankedCandidates.slice(0, 2).map((c) => ({
    ...c,
    inReactivationProposal: rowsProposedForReactivation.some((r) => r.recordId === c.recordId),
  }));

  const whyOnlyOneShowing = {
    visibleRowsCount: visibleCount,
    completeRowsCount: completeCount,
    requiredMinimum: 3,
    rowsBlockedByImageApproval: blockedByImageApproval,
    rowsBlockedByMaterialization: blockedByMaterialization,
    rowsBlockedByExternalDisplayStatus: blockedByDisplayStatus,
    rowsBlockedBySourceOrImageMismatch: blockedBySourceMismatch,
    summary:
      visibleCount <= 1
        ? `${visibleCount} visible / ${completeCount} complete — remaining rows quarantined (Do Not Display) and/or missing founder-approved materialized images.`
        : `${visibleCount} visible openings blocks in API.`,
    completeVisibleRecordIds: alreadyCompleteVisible.map((a) => a.recordId),
  };

  const momentumParityComparison = compareMomentumToTribute(momentumRows, tributeMomentum);

  for (const pkg of MOMENTUM_PARITY_PACKAGES) {
    const live =
      momentumRows.find((r) => r.recordId === pkg.recordId) ||
      momentumRows.find((r) => Number(r.sortOrder ?? -1) === Number(pkg.sort));

    const parsedBefore = live ? parseMomentumBody(live.body) : { dateLine: "", description: "", sourceUrl: "" };
    const sourceClass = classifyMomentumSourceUrl(pkg.sourceUrl);
    const proposedLinkLabel = pkg.proposedLinkLabel || proposedLinkLabelForSource(sourceClass);
    const proposedBody = buildMomentumBody(pkg);

    momentumCopyBeforeAfter.push({
      recordId: live?.recordId || pkg.recordId,
      titleBefore: live?.title || "",
      titleAfter: pkg.polishedTitle,
      bodyBefore: parsedBefore.description || live?.body || "",
      bodyAfter: pkg.polishedSummary,
      dateLineBefore: parsedBefore.dateLine,
      dateLineAfter: pkg.dateLine,
    });

    momentumLinkBeforeAfter.push({
      recordId: live?.recordId || pkg.recordId,
      sourceUrlBefore: parsedBefore.sourceUrl,
      sourceUrlAfter: pkg.sourceUrl,
      linkLabelBefore: legacyFrontendLinkLabel(parsedBefore.sourceUrl),
      linkLabelAfter: proposedLinkLabel,
      ownerImplication: pkg.ownerImplication,
    });

    proposedMomentumUpdates.push({
      recordId: live?.recordId || pkg.recordId,
      fields: {
        Title: pkg.polishedTitle,
        Body: proposedBody,
        "Brand Name": target.name,
        Brand: [target.recordId],
      },
    });
  }

  const applyBlockers = [];
  if (momentumRows.length !== MOMENTUM_PARITY_PACKAGES.length) {
    applyBlockers.push(
      `momentum_row_count_mismatch:live=${momentumRows.length},expected=${MOMENTUM_PARITY_PACKAGES.length}`
    );
  }

  for (const u of proposedOpeningUpdates) {
    if (u.reactivate && apply && !approvedAssetsOnly) {
      applyBlockers.push(`reactivation_requires_${APPLY_FLAG_APPROVED_ONLY}`);
    }
    if (findInternalLanguageInRow({ body: u.fields.Body }).length) {
      applyBlockers.push(`internal_language_opening:${u.recordId}`);
    }
    if (u.materializeImageUrl && apply && !approvedAssetsOnly) {
      applyBlockers.push(`materialization_requires_approved_assets:${u.recordId}`);
    }
    const audit = openingsCompletionAudit.find((a) => a.recordId === u.recordId);
    if (u.reactivate && audit && !audit.reactivateEligible) {
      applyBlockers.push(`forced_reactivation_blocked:${u.recordId}`);
    }
  }

  for (const pkg of MOMENTUM_PARITY_PACKAGES) {
    if (findInternalLanguageInRow({ body: pkg.polishedSummary, title: pkg.polishedTitle }).length) {
      applyBlockers.push(`internal_language_momentum:${pkg.recordId}`);
    }
  }

  const hasWork = proposedOpeningUpdates.length > 0 || proposedMomentumUpdates.length > 0;
  const applyGatesReady =
    apply && approveBatch && approvedAssetsOnly && noValidationClaim && founderReviewed;
  const dryRunClean = applyBlockers.length === 0;
  const canApply = applyGatesReady && dryRunClean && hasWork;

  let airtableModified = false;
  let imagesApproved = false;
  let applyResults = {
    openingsUpdated: [],
    momentumUpdated: [],
    imagesMaterialized: [],
    errors: [],
  };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const update of proposedMomentumUpdates) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: update.fields, typecast: true }) },
        update.recordId
      );
      if (!res.ok) {
        applyResults.errors.push({ recordId: update.recordId, error: json.error?.message || "momentum patch failed" });
        continue;
      }
      applyResults.momentumUpdated.push(update.recordId);
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const update of proposedOpeningUpdates) {
      if (update.materializeImageUrl) {
        try {
          const mat = await materializePresentationImage({
            baseId,
            apiKey,
            recordId: update.recordId,
            imageUrl: update.materializeImageUrl,
            slotKey: OPENINGS_SLOT,
          });
          if (mat.materialized) applyResults.imagesMaterialized.push(update.recordId);
        } catch (err) {
          applyResults.errors.push({ recordId: update.recordId, error: `materialize:${err.message}` });
          continue;
        }
      }

      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: update.fields, typecast: true }) },
        update.recordId
      );
      if (!res.ok) {
        applyResults.errors.push({ recordId: update.recordId, error: json.error?.message || "opening patch failed" });
        continue;
      }
      applyResults.openingsUpdated.push(update.recordId);
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
  const completeBuildBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    v31mWriterExists: v31mWriterExists(),
    targetBrand: target,
    tributeReference: { recordId: TRIBUTE_RECORD_ID, name: TRIBUTE_BRAND_NAME },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    airtableModified,
    imagesApproved,
    whyOnlyOneOpeningShowing: whyOnlyOneShowing,
    openingsCompletionAudit,
    rowsProposedForReactivation,
    rowsKeptHidden,
    bestTwoAdditionalCandidates: bestTwoAdditional,
    neededAdditionalOpeningsToReachThree: neededAdditional,
    momentumParityComparison,
    momentumCopyBeforeAfter,
    momentumLinkBeforeAfter,
    tributeOpeningsReferenceCount: tributeOpenings.length,
    tributeMomentumReferenceCount: tributeMomentum.length,
    proposedOpeningUpdates,
    proposedMomentumUpdates,
    applyBlockers,
    dryRunClean,
    canApply,
    applyResults,
    expectedUiResult: {
      openingsBefore: `${visibleCount} visible / ${completeCount} complete (need 3)`,
      openingsAfterApply: `${alreadyCompleteVisible.length + rowsProposedForReactivation.length} projected visible if gates pass`,
      momentumAfter: "Three insight-led momentum rows with differentiated link labels (press kit + property listings)",
      tributeParity: "Momentum copy/link pattern aligned to Tribute editorial standards",
    },
    expectedActiveProfileResult: {
      note:
        rowsProposedForReactivation.length >= neededAdditional
          ? "Openings section may reach 3 complete rows after apply."
          : "Active-profile blocked until founder approves opening registry images for 2+ additional rows.",
      finalQaBefore: finalQaBefore?.brandReports?.[0]?.scores || null,
      completeBuildBefore:
        (completeBuildBefore?.brandReports || []).find((b) => b.slug === target.slug)?.readiness ||
        completeBuildBefore?.summary ||
        null,
    },
    exactApplyCommand: dryRunClean && hasWork ? buildApplyCommand({ brand: target.slug }) : null,
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Radisson Individuals Openings + Momentum Parity v31M`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.targetBrand.name}**`,
    `- v31M exists: **${report.v31mWriterExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    `- Images approved: **no**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Why only one opening shows",
    "",
    `- Visible in API: **${report.whyOnlyOneOpeningShowing.visibleRowsCount}**`,
    `- Complete rows: **${report.whyOnlyOneOpeningShowing.completeRowsCount}** / 3 required`,
    `- Blocked by image approval: **${report.whyOnlyOneOpeningShowing.rowsBlockedByImageApproval}**`,
    `- Blocked by materialization: **${report.whyOnlyOneOpeningShowing.rowsBlockedByMaterialization}**`,
    `- Blocked by External Display Status: **${report.whyOnlyOneOpeningShowing.rowsBlockedByExternalDisplayStatus}**`,
    `- ${report.whyOnlyOneOpeningShowing.summary}`,
    "",
    "## Openings completion audit",
    "",
  ];

  for (const a of report.openingsCompletionAudit) {
    lines.push(`### ${a.title}`);
    lines.push(`- Record: \`${a.recordId}\``);
    lines.push(`- Classification: **${a.classification}** (${a.classificationReason})`);
    lines.push(`- Visible in API: ${a.visibleInApi} · Complete: ${a.completeRow} · Image: ${a.hasImage}`);
    lines.push(`- Registry: ${a.registryRecordId || "none"} (${a.registryApprovalStatus})`);
    lines.push("");
  }

  lines.push("## Rows proposed for reactivation", "");
  if (!report.rowsProposedForReactivation.length) {
    lines.push("- none (founder-approved images required)");
  } else {
    for (const r of report.rowsProposedForReactivation) {
      lines.push(`- \`${r.recordId}\` — ${r.title}`);
    }
  }

  lines.push("", "## Momentum Tribute parity", "");
  lines.push(`- Tribute rows: ${report.momentumParityComparison.tributeRowCount}`);
  lines.push(`- Radisson uses single press kit: ${report.momentumParityComparison.radissonUsesSinglePressKit}`);
  lines.push(`- Gap: ${report.momentumParityComparison.radissonGapSummary}`);
  lines.push(`- Year note: ${report.momentumParityComparison.yearSupportNote}`);

  if (report.exactApplyCommand) {
    lines.push("", "## Apply command", "", "```bash", report.exactApplyCommand, "```");
  }
  if (report.applyBlockers.length) {
    lines.push("", "## Apply blockers", "", ...report.applyBlockers.map((b) => `- ${b}`));
  }
  return lines.join("\n");
}
