/**
 * Brand Explorer Everhome Presentation Cleanup + Internal-Language Removal v32D.
 *
 * Copy-only presentation repairs for Everhome Suites. Preserves working images,
 * visibility, approvals, and Company Validated. No registry or source approval changes.
 *
 * @see docs/data-intelligence/brand-explorer-everhome-presentation-cleanup-writer-v32D.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import {
  isRegistryAssetApprovedForExplorer,
} from "./brand-explorer-brand-asset-image-governance.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";

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
  { id: "booking_path", re: /\bbooking path\b/i, family: "source_capture" },
  { id: "verify_fdd", re: /\bverify with (the )?fdd\b/i, family: "internal" },
];

export const WRITER_VERSION = "v32D";
export const REPORT_JSON_NAME = "brand-explorer-everhome-presentation-cleanup-writer.json";
export const REPORT_MD_NAME = "brand-explorer-everhome-presentation-cleanup-writer.md";
export const DOC_MD_NAME = "brand-explorer-everhome-presentation-cleanup-writer-v32D.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v32D-everhome-presentation-cleanup";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_APPROVALS = "--confirm-no-image-or-registry-approval-changes";
export const APPLY_FLAG_NO_VISIBILITY = "--confirm-no-visibility-changes";
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

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const MIN_FEATURED_WORDS = 25;
const MIN_SCENARIO_WORDS = 15;
const MIN_OVERVIEW_SCENARIO_WORDS = 22;
const MIN_GALLERY_WORDS = 12;

const BLOCKED_PATCH_FIELDS = new Set([
  "Image",
  "Images",
  "Attachments",
  "Scenario Image",
  "External Display Status",
  "Company Validated",
  "Company Validation Date",
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-everhome-source-registry-normalization-writer.json",
  "reports/brand-explorer-choice-extended-stay-batch-readiness-audit.json",
  "reports/brand-explorer-choice-extended-stay-source-capture-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "live Everhome presentation / sources / registry",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-everhome-presentation-cleanup-writer.js",
  "scripts/brand-explorer-everhome-presentation-cleanup-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

/** Ordered sanitization — specific phrases before broad FDD replacement. */
const SANITIZE_REPLACEMENTS = [
  { re: /\bowner should confirm in (the )?fdd\b/gi, replace: "owners should validate during commercial model review" },
  { re: /\bverify with (the )?fdd\b/gi, replace: "confirm during owner diligence" },
  { re: /\bconfirm in (the )?fdd\b/gi, replace: "confirm during owner diligence" },
  { re: /\bfranchise disclosure document\b/gi, replace: "commercial model review materials" },
  { re: /\bfranchise disclosure\b/gi, replace: "commercial model review" },
  { re: /\bitem\s*19\b/gi, replace: "operating economics review" },
  { re: /\bperformance representation\b/gi, replace: "operating performance considerations" },
  { re: /\bconfirm fees\b/gi, replace: "fee structure diligence" },
  { re: /\bconfirm flag\b/gi, replace: "brand participation diligence" },
  { re: /\bactive property page\b/gi, replace: "official property positioning" },
  { re: /\bconsumer path\b/gi, replace: "guest booking channels" },
  { re: /\bbooking path\b/gi, replace: "distribution channels" },
  { re: /\bsource[- ]capture\b/gi, replace: "reference review" },
  { re: /\binternal extraction\b/gi, replace: "reference review" },
  { re: /\bsource data\b/gi, replace: "reference materials" },
  { re: /\bconsumer site\b/gi, replace: "public brand materials" },
  { re: /\bbrand site\b/gi, replace: "official brand materials" },
  { re: /\bcensus\b/gi, replace: "portfolio footprint reference" },
  { re: /\bmetadata\b/gi, replace: "profile details" },
  { re: /\bextraction\b/gi, replace: "reference review" },
  { re: /\bfdd\b/gi, replace: "owner diligence materials" },
];

/** Slot-specific backfill when copy remains thin after sanitization. */
export const EVERHOME_BACKFILL = Object.freeze({
  "overview.featured_application": {
    title: "Extended-Stay Development Fit",
    body:
      "Purpose-built extended-stay suites for owners underwriting weekly and monthly stay demand—Everhome fits when assets need residential-style guestrooms, limited public space, and Choice Privileges distribution while aligning to extended-stay operating economics and conversion-friendly development standards.",
  },
  "overview.scenario.1": {
    title: "New-Build Extended-Stay",
    body:
      "Ground-up extended-stay development in suburban and highway-corridor markets—Everhome suits owners targeting project housing, training rotations, and relocation demand with residential-style suites and lean public-space requirements.",
  },
  "overview.scenario.2": {
    title: "Conversion From Independent Extended Stay",
    body:
      "Mature extended-stay or all-suites properties needing affiliation lift—Everhome competes when room modules already support kitchenettes and owners want Choice scale without full-service F&B intensity.",
  },
  "overview.scenario.3": {
    title: "Corporate & Project Housing Corridor",
    body:
      "Markets with employer-driven weekly demand near industrial parks, hospitals, or training campuses—Everhome works when ADR supports extended-stay housekeeping models within Choice extended-stay standards.",
  },
  "valueOwners.scenario.1": {
    title: "New-Build Extended-Stay Development",
    body:
      "Owners developing purpose-built extended-stay product—Everhome fits when underwriting emphasizes residential guestrooms, limited F&B, and Choice Privileges participation without upscale public-space mandates.",
  },
  "valueOwners.scenario.2": {
    title: "Conversion / Repositioning",
    body:
      "Independent or legacy extended-stay assets seeking affiliation—Everhome suits when kitchenette-ready room modules and lean operating models align to Choice extended-stay positioning.",
  },
  "valueOwners.scenario.3": {
    title: "Weekly Corporate Demand",
    body:
      "Corridors with project crews, insurance housing, or training rotations—Everhome works when weekly billing and extended-stay guest expectations match residential suite design.",
  },
  "valueOwners.scenario.4": {
    title: "Third-Party Operator–Led",
    body:
      "Assets run by extended-stay operators who understand weekly billing—Everhome suits sponsors who need recognizable Choice affiliation while keeping operating complexity aligned to extended-stay norms.",
  },
  "loyalty.ecosystem": {
    title: "Choice Privileges Participation",
    body:
      "Everhome participates in Choice Privileges, connecting extended-stay guests to enterprise and transient demand across the Choice network. Owners should evaluate loyalty contribution, channel mix, and booking economics during commercial model review—not as a performance guarantee.",
  },
  "loyalty.proof": {
    title: "Loyalty Demand Capture",
    body:
      "Choice Privileges supports extended-stay demand capture through enterprise accounts, direct channels, and partner bookings. Owners should assess loyalty mix and distribution reach during underwriting—not as a forecast of property-level performance.",
  },
  "loyalty.redeem": {
    title: "Redemption & Channel Mix",
    body:
      "Extended-stay owners should understand how Choice Privileges redemption and channel participation affect net booking economics. Validate redemption patterns and corporate contract mix during owner diligence.",
  },
  "loyalty.kpi.mix": {
    title: "Loyalty Mix Considerations",
    body:
      "Loyalty-driven bookings can supplement extended-stay demand but vary by market and operator. Owners should review channel and loyalty mix assumptions during commercial model review without treating program scale as property-level guidance.",
  },
  "standards.intro": {
    title: "Brand Standards Overview",
    body:
      "Everhome standards emphasize residential-style suites, limited public space, and extended-stay operating models suited to weekly and monthly guests. Owners should review brand participation requirements and conversion specifications during diligence.",
  },
  "economics.intro": {
    title: "Economics & Owner Considerations",
    body:
      "Extended-stay underwriting should focus on weekly rate positioning, housekeeping intensity, and conversion scope—not on disclosed fee tables in research materials. Owners should complete commercial model review with qualified advisors.",
  },
  "economics.fee": {
    title: "Fee Structure Diligence",
    body:
      "Owners should validate franchise and operating fee components during commercial model review. Dealality does not present specific fee amounts or performance representations—confirm economics with Choice development counsel.",
  },
  "economics.fee.join": {
    title: "Initial Investment Considerations",
    body:
      "Joining costs vary by conversion scope, market, and prototype alignment. Owners should review development and conversion estimates during diligence without relying on generic disclosure excerpts.",
  },
  "economics.fee.operate": {
    title: "Operating Cost Considerations",
    body:
      "Extended-stay operating economics depend on housekeeping model, utility recovery, and weekly-rate positioning. Owners should validate operating assumptions during commercial model review.",
  },
  "economics.kpi.fee_stack": {
    title: "Fee Stack Diligence",
    body:
      "Owners should map franchise, marketing, and technology fees during underwriting. Dealality summarizes considerations only—confirm fee stack details with Choice development representatives.",
  },
  "commercial.theme": {
    title: "Commercial Positioning",
    body:
      "Everhome targets extended-stay demand with residential-style suites and Choice Privileges distribution. Owners should align commercial positioning to weekly-stay corridors and conversion economics during diligence.",
  },
  "portfolio_context": {
    title: "0",
    body:
      "Everhome Suites sits in Choice's extended-stay portfolio alongside WoodSpring and Suburban Studios—positioned for residential-style extended-stay development with Choice Privileges participation and conversion-friendly standards.",
  },
  "demand_scenario": {
    title: "Extended-Stay Demand Context",
    body:
      "Everhome targets weekly and monthly stay demand from project housing, relocation, training rotations, and insurance housing. Owners should validate local extended-stay supply, weekly-rate benchmarks, and operating model fit during market diligence.",
  },
  "materials.gallery.1": {
    body:
      "Everhome prototype reference—residential-style suite layout suited to extended-stay weekly demand and limited public-space operating models.",
  },
  "materials.gallery.2": {
    body:
      "Extended-stay guestroom example illustrating kitchenette-ready residential positioning within Everhome brand standards.",
  },
  "materials.gallery.3": {
    body:
      "Property exterior reference for Everhome extended-stay development—confirm signage and prototype alignment during owner diligence.",
  },
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
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

function inferSection(slotKey) {
  const sk = nz(slotKey);
  if (!sk) return "other";
  if (sk.startsWith("overview.")) return "overview";
  if (sk.startsWith("valueOwners.")) return "valueOwners";
  if (sk.startsWith("footprint.openings")) return "footprint.openings";
  if (sk.startsWith("footprint.momentum")) return "footprint.momentum";
  if (sk.startsWith("footprint.")) return "geographic_footprint";
  if (sk.startsWith("materials.gallery")) return "materials.gallery";
  if (sk.startsWith("loyalty.")) return "loyalty";
  if (sk.startsWith("economics.")) return "economics";
  if (sk.startsWith("standards.")) return "standard_detail";
  if (sk === "portfolio_mix" || sk.startsWith("portfolio_mix")) return "portfolio_mix";
  if (sk === "portfolio_context" || sk.startsWith("overview.portfolio")) return "portfolio_context";
  if (sk === "demand_scenario" || sk.includes("demand")) return "demand_scenario";
  return sk.split(".")[0] || "other";
}

export function v32dWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-everhome-presentation-cleanup-writer.js")
  );
}

export function resolveEverhomeBrand(brandArg) {
  const slug = nz(brandArg).toLowerCase() || TARGET_BRAND.slug;
  if (slug !== TARGET_BRAND.slug) {
    throw new Error(`v32D is Everhome-only. Requested: ${brandArg}`);
  }
  return TARGET_BRAND;
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

function firstAttachmentUrl(fields) {
  for (const key of ["Image", "Images", "Scenario Image", "Attachments"]) {
    const att = fields?.[key];
    if (Array.isArray(att) && att[0]?.url) return nz(att[0].url);
  }
  return "";
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

export async function listEverhomePresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const listUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const listRes = await fetch(listUrl, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const listJson = await listRes.json().catch(() => ({}));
    if (!listRes.ok) throw new Error(listJson.error?.message || `List failed: ${listRes.status}`);
    records.push(...(listJson.records || []));
    offset = listJson.offset || "";
  } while (offset);

  return records.map((rec) => {
    const f = rec.fields || {};
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      sortOrder: f["Sort Order"],
      active: f.Active,
      externalDisplayStatus: nz(f["External Display Status"]),
      imageUrl: firstAttachmentUrl(f),
      summaryUrl: nz(f["Summary URL"]),
      hasImage: Boolean(firstAttachmentUrl(f)),
      rawFields: f,
    };
  });
}

function scanInternalLanguage(text, recordId, slotKey) {
  const findings = [];
  const combined = nz(text);
  for (const pat of INTERNAL_LANGUAGE_PATTERNS) {
    const m = combined.match(pat.re);
    if (m) {
      findings.push({
        recordId,
        slotKey,
        patternId: pat.id,
        family: pat.family,
        phrase: m[0],
      });
    }
  }
  return findings;
}

export function sanitizeEverhomeCopy(text) {
  let out = nz(text);
  if (!out) return out;
  for (const rule of SANITIZE_REPLACEMENTS) {
    out = out.replace(rule.re, rule.replace);
  }
  return out
    .split("\n")
    .map((line) => line.replace(/[ \t]{2,}/g, " ").trim())
    .join("\n")
    .trim();
}

function isThinCopy(slotKey, title, body) {
  const wc = wordCount(body);
  if (slotKey === "overview.featured_application") return wc < MIN_FEATURED_WORDS;
  if (slotKey.startsWith("overview.scenario")) return wc < MIN_OVERVIEW_SCENARIO_WORDS;
  if (slotKey.startsWith("valueOwners.scenario")) return wc < MIN_SCENARIO_WORDS;
  if (slotKey.startsWith("materials.gallery")) return wc < MIN_GALLERY_WORDS;
  if (!body && title) return true;
  if (!body && !title) return true;
  return wc < 10 && /loyalty|economics|standards|portfolio|demand/i.test(slotKey);
}

function findRegistryForRow(registryAssets, presentationRowId) {
  return (registryAssets || []).filter((a) =>
    nz(a.sourceNotes).includes(presentationRowId)
  );
}

function buildImageGovernanceReport(row, apiBlock, registryMatches) {
  const apiImage = nz(apiBlock?.imageUrl);
  const imageLoading = Boolean(apiImage) && !isTemporaryAirtableUrl(apiImage);
  const tempAttachment = isTemporaryAirtableUrl(row.imageUrl);
  const approved = registryMatches.some((r) => isRegistryAssetApprovedForExplorer(r));
  const pending = registryMatches.some(
    (r) =>
      r.explorerUsePermission === "Candidate Only" ||
      r.usageReviewStatus === "Pending Review" ||
      r.assetStatus === "Needs Usage Review"
  );

  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    imageLoading,
    hasPresentationImage: row.hasImage,
    tempAirtableAttachment: tempAttachment,
    registryLinked: registryMatches.length > 0,
    registryRecordIds: registryMatches.map((r) => r.id),
    registryApproved: approved,
    pendingImageReview: pending && !approved,
    durableSourceUrl: row.summaryUrl && !isTemporaryAirtableUrl(row.summaryUrl) ? row.summaryUrl : null,
    recommendation: imageLoading
      ? approved
        ? "ok_for_v32F_recognition"
        : pending
          ? "v32F_materialization_after_founder_approval"
          : "registry_linkage_present_pending_review"
      : row.hasImage
        ? "image_present_verify_loading"
        : "no_image_v32E_or_v32F",
  };
}

function presentationPatchFields({ slotKey, title, body, sort, brandRecordId, brandName }) {
  return {
    "Slot Key": slotKey,
    Title: title || "",
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort ?? 0,
  };
}

function validatePatchFields(fields) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PATCH_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
  }
  const safety = scanCopySafety(`${fields.Title || ""}\n${fields.Body || ""}`);
  if (safety.length) errors.push(`copy_safety:${safety.join(",")}`);
  if (/\bfdd\b|\bitem\s*19\b|franchise disclosure/i.test(`${fields.Title}\n${fields.Body}`)) {
    errors.push("internal_language_remains");
  }
  return errors;
}

function proposeRowUpdate(row, brandRecordId, brandName) {
  const isMomentumOpening =
    row.slotKey === "footprint.momentum" ||
    row.slotKey === "footprint.openings" ||
    row.slotKey.startsWith("footprint.momentum.");

  const beforeTitle = row.title;
  const beforeBody = row.body;

  const internalBefore = scanInternalLanguage(
    `${beforeTitle}\n${beforeBody}`,
    row.recordId,
    row.slotKey
  );

  if (isMomentumOpening && internalBefore.length === 0) {
    return null;
  }

  let proposedTitle = sanitizeEverhomeCopy(beforeTitle);
  let proposedBody = sanitizeEverhomeCopy(beforeBody);

  const backfill = EVERHOME_BACKFILL[row.slotKey];
  const needsBackfill =
    backfill &&
    (isThinCopy(row.slotKey, proposedTitle, proposedBody) ||
      internalBefore.length > 0 ||
      (backfill.title && !proposedTitle));

  if (needsBackfill) {
    if (backfill.title) proposedTitle = backfill.title;
    if (backfill.body) proposedBody = backfill.body;
  }

  if (proposedTitle === beforeTitle && proposedBody === beforeBody) {
    return null;
  }

  const fields = presentationPatchFields({
    slotKey: row.slotKey,
    title: proposedTitle,
    body: proposedBody,
    sort: row.sortOrder ?? 0,
    brandRecordId,
    brandName,
  });

  const validationErrors = validatePatchFields(fields);
  const internalAfter = scanInternalLanguage(
    `${proposedTitle}\n${proposedBody}`,
    row.recordId,
    row.slotKey
  );

  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    section: inferSection(row.slotKey),
    before: { title: beforeTitle, body: beforeBody },
    after: { title: proposedTitle, body: proposedBody },
    internalLanguageBefore: internalBefore,
    internalLanguageAfter: internalAfter,
    fields,
    validationErrors,
    fixReason: internalBefore.length ? "internal_language_cleanup" : "thin_copy_backfill",
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-everhome-presentation-cleanup-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_APPROVALS,
    APPLY_FLAG_NO_VISIBILITY,
    APPLY_FLAG_EVERHOME_ONLY,
  ].join(" ");
}

export async function buildBrandExplorerEverhomePresentationCleanupWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noImageFields = false,
  noApprovals = false,
  noVisibility = false,
  everhomeOnly = false,
} = {}) {
  const target = resolveEverhomeBrand(brandArg);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const brandApi = await fetchBrandApiShape(target.recordId);
  if (!brandApi) throw new Error("Could not load Everhome brand API shape");

  const presentationRows = await listEverhomePresentationRows(
    baseId,
    apiKey,
    target.recordId,
    target.name
  );
  const registryAssets = await listRegistryAssetsForBrand(target.recordId).catch(() => []);
  const apiBlocks = brandApi.brandExplorer?.blocks || [];
  const apiBlockById = new Map(apiBlocks.map((b) => [b.recordId, b]));

  const presentationInventory = presentationRows.map((row) => {
    const apiBlock = apiBlockById.get(row.recordId);
    const registryMatches = findRegistryForRow(registryAssets, row.recordId);
    const internalHits = scanInternalLanguage(
      `${row.title}\n${row.body}`,
      row.recordId,
      row.slotKey
    );
    const ownerFacing = !["Do Not Display", "Internal Only"].includes(row.externalDisplayStatus);
    const thin = isThinCopy(row.slotKey, row.title, row.body);
    return {
      recordId: row.recordId,
      slotKey: row.slotKey,
      section: inferSection(row.slotKey),
      title: row.title,
      body: row.body,
      externalDisplayStatus: row.externalDisplayStatus || null,
      ownerFacing,
      imageStatus: row.hasImage ? "has_attachment" : "no_attachment",
      imageLoading: Boolean(apiBlock?.imageUrl),
      sourceUrl: row.summaryUrl || null,
      registryLinkage: registryMatches.map((r) => r.id),
      defectHints: [
        ...(internalHits.length ? ["internal_language"] : []),
        ...(thin ? ["thin_copy"] : []),
        ...(row.hasImage && !registryMatches.length ? ["missing_registry_linkage"] : []),
      ],
      thin,
      internalLanguage: internalHits.length > 0,
    };
  });

  const proposedUpdates = [];
  const rowsLeftUnchanged = [];

  for (const row of presentationRows) {
    const proposal = proposeRowUpdate(row, target.recordId, target.name);
    if (!proposal || proposal.validationErrors.length) {
      rowsLeftUnchanged.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: proposal?.validationErrors?.length
          ? proposal.validationErrors.join("; ")
          : "no_copy_change_needed",
      });
      continue;
    }
    proposedUpdates.push(proposal);
  }

  const imageGovernanceReport = presentationRows.map((row) =>
    buildImageGovernanceReport(row, apiBlockById.get(row.recordId), findRegistryForRow(registryAssets, row.recordId))
  );

  const internalLanguageBefore = presentationInventory.flatMap((r) =>
    scanInternalLanguage(`${presentationRows.find((p) => p.recordId === r.recordId)?.title}\n${presentationRows.find((p) => p.recordId === r.recordId)?.body}`, r.recordId, r.slotKey)
  );

  const internalLanguageAfter = proposedUpdates.flatMap((u) => u.internalLanguageAfter);

  const applyBlockers = [];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noImageFields) applyBlockers.push("missing_confirm_no_image_field_changes");
    if (!noApprovals) applyBlockers.push("missing_confirm_no_image_or_registry_approval_changes");
    if (!noVisibility) applyBlockers.push("missing_confirm_no_visibility_changes");
    if (!everhomeOnly) applyBlockers.push("missing_confirm_everhome_only");
  }

  for (const update of proposedUpdates) {
    if (update.validationErrors.length) {
      applyBlockers.push(`validation_failed:${update.recordId}`);
    }
  }

  const dryRunClean =
    applyBlockers.length === 0 && proposedUpdates.length > 0;

  let airtableModified = false;
  const applyResults = { updated: [], errors: [] };
  let imageFieldsChanged = false;
  let visibilityChanged = false;
  let approvalsChanged = false;

  const canApply =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFields &&
    noApprovals &&
    noVisibility &&
    everhomeOnly &&
    applyBlockers.length === 0;

  if (canApply) {
    for (const update of proposedUpdates) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          {
            method: "PATCH",
            body: JSON.stringify({ fields: update.fields, typecast: true }),
          },
          update.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
        applyResults.updated.push({ recordId: update.recordId, slotKey: update.slotKey });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: update.recordId, message: err.message });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(target.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const report = {
    writerVersion: WRITER_VERSION,
    v32dWriterExists: v32dWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    presentationInventory,
    workingImagePreservation: {
      policy: "no_image_field_changes",
      imagesLoadingInExplorer: presentationInventory.filter((r) => r.imageLoading || r.imageStatus === "has_attachment").length,
      rowsWithImages: imageGovernanceReport.filter((r) => r.hasPresentationImage).length,
      tempAirtableAttachments: imageGovernanceReport.filter((r) => r.tempAirtableAttachment).length,
      note: "Working images preserved — registry linkage reported only; temporary Airtable URLs may still render in Explorer.",
    },
    internalLanguageFindings: {
      before: internalLanguageBefore,
      afterProjected: [
        ...internalLanguageAfter,
        ...presentationInventory
          .filter((r) => !proposedUpdates.some((u) => u.recordId === r.recordId))
          .flatMap((r) => {
            const row = presentationRows.find((p) => p.recordId === r.recordId);
            return scanInternalLanguage(`${row?.title}\n${row?.body}`, r.recordId, r.slotKey);
          }),
      ],
    },
    copyBeforeAfter: proposedUpdates.map((u) => ({
      recordId: u.recordId,
      slotKey: u.slotKey,
      before: u.before,
      after: u.after,
      fixReason: u.fixReason,
    })),
    rowsUpdated: proposedUpdates.map((u) => ({
      recordId: u.recordId,
      slotKey: u.slotKey,
      fixReason: u.fixReason,
    })),
    rowsLeftUnchanged,
    imageGovernanceReport,
    imageFieldsChanged,
    openingsMomentumVisibilityChanged: visibilityChanged,
    imageOrRegistryApprovalsChanged: approvalsChanged,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    airtableModified,
    applyBlockers,
    dryRunClean,
    applyResults,
    expectedFinalQaImpact:
      "Should reduce internal_or_governance_language and thin_copy defects; presentation quality score may improve modestly.",
    expectedCompleteBuildImpact:
      "Copy layer improved; active-profile still blocked pending image approval (v32F) and momentum/openings parity (v32E).",
    recommendedNextWriter: "v32E — Everhome openings/momentum rebuild writer",
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand: `npm run brand-explorer-everhome-presentation-cleanup-writer -- --brand ${TARGET_BRAND.slug} --dry-run`,
    applyGuardrails: {
      copyOnly: true,
      noImageFields: true,
      noVisibilityChanges: true,
      noApprovalChanges: true,
      everhomeOnly: true,
    },
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Everhome Presentation Cleanup v32D");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32D exists: **${report.v32dWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Image fields changed: **${report.imageFieldsChanged ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Summary");
  lines.push(`- Presentation rows inventoried: **${report.presentationInventory.length}**`);
  lines.push(`- Rows proposed for update: **${report.rowsUpdated.length}**`);
  lines.push(`- Internal-language hits (before): **${report.internalLanguageFindings.before.length}**`);
  lines.push(`- Images loading in Explorer: **${report.workingImagePreservation.imagesLoadingInExplorer}**`);
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
