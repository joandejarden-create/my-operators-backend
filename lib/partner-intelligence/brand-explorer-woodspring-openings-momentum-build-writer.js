/**
 * Brand Explorer WoodSpring Openings + Momentum Build v33C.
 *
 * Creates footprint.openings and footprint.momentum rows from approved WoodSpring
 * sources. No image fields, source library, or registry approval changes.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-openings-momentum-build-writer-v33C.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listPartnerSources } from "./airtable-source.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  auditEverhomeSourceRecord,
} from "./brand-explorer-everhome-source-registry-normalization-writer.js";
import {
  buildMomentumBody,
  classifyMomentumSourceType,
  followsTributeMomentumRules,
  isMomentumInappropriatePropertyListing,
  momentumEvidenceSourceRank,
  momentumLinkLabelForUrl,
} from "./brand-explorer-momentum-link-label.js";
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33C";
export const STAGING_RUN_ID = "v33C-woodspring-openings-momentum-build";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-openings-momentum-build-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-openings-momentum-build-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-openings-momentum-build-writer-v33C.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33C-woodspring-openings-momentum-build";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_SOURCE_LIBRARY = "--confirm-no-source-library-changes";
export const APPLY_FLAG_NO_REGISTRY_APPROVAL = "--confirm-no-registry-approval-changes";
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

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";
const TARGET_OPENINGS = 4;
const TARGET_MOMENTUM = 3;

const BLOCKED_PATCH_FIELDS = new Set([
  "Image",
  "Images",
  "Attachments",
  "Scenario Image",
  "Company Validated",
  "Company Validation Date",
]);

const METADATA_STYLE_RE =
  /listed on choicehotels|active property page|consumer site|consumer path|property listing page|source data|metadata|census|extracted from|booking path|source[- ]?capture/i;
const BLOCKED_OWNER_FACING_RE =
  /\bfdd\b|\bitem\s*19\b|franchise disclosure|confirm fees|confirm flag|performance representation|\beverhome\b|\bsuburban\b/i;
const PERFORMANCE_RE =
  /\b(roi|irr|cap rate|noi|revpar|adr forecast|guaranteed returns?|published %)\b/i;

/** Curated opening packages keyed by durable source URL fragment. */
export const WOODSPRING_OPENING_PACKAGES = Object.freeze([
  {
    id: "ws_opening_brand_site",
    sourceUrlMatch: "woodspring.com",
    title: "WoodSpring Suites — U.S. Footprint Example",
    chips: "Extended-Stay, Weekly Demand, Simple Suite",
    location: "United States — representative WoodSpring markets",
    meta: "Portfolio Example · Kitchen-equipped extended-stay suites",
    scenario: "OWNER FIT / MARKET SCAN",
    teaser:
      "A WoodSpring extended-stay example that helps owners evaluate longer-stay demand, simple-suite positioning, and market fit within the Choice platform.",
  },
  {
    id: "ws_opening_brand_directory",
    sourceUrlMatch: "choicehotels.com/woodspring",
    title: "WoodSpring Suites — Portfolio Discovery Example",
    chips: "Extended-Stay, Choice Platform, Weekly-Stay",
    location: "North America — WoodSpring portfolio reference",
    meta: "Portfolio Example · Brand discovery context",
    scenario: "COMPETITIVE SCAN / SUPPLY CONTEXT",
    teaser:
      "A market example for owners comparing WoodSpring's economy extended-stay model against other longer-stay lodging options within the Choice network.",
  },
  {
    id: "ws_opening_development",
    sourceUrlMatch: "choicehotelsdevelopment.com/our-brands/extended-stay/woodspring",
    title: "WoodSpring Suites — Development Prototype Example",
    chips: "New-Build, Conversion, Extended-Stay",
    location: "Development markets — prototype and conversion diligence",
    meta: "Development Example · Extended-stay operating model",
    scenario: "PROTOTYPE / DEVELOPMENT FIT",
    teaser:
      "A property example that illustrates WoodSpring's practical suite model for weekly-stay and longer-stay demand—useful when underwriting prototype alignment and operating simplicity.",
  },
  {
    id: "ws_opening_extended_stay_hub",
    sourceUrlMatch: "choicehotelsdevelopment.com/our-brands/extended-stay",
    title: "Choice Extended-Stay Platform Example",
    chips: "Extended-Stay Platform, Choice Portfolio, Weekly-Stay",
    location: "Choice extended-stay category — platform context",
    meta: "Portfolio Context · Extended-stay competitive set",
    scenario: "PLATFORM / CATEGORY POSITIONING",
    teaser:
      "An extended-stay platform example showing where WoodSpring sits among Choice weekly-stay brands—owners compare category positioning, operating model, and development resources.",
  },
]);

/** Curated momentum packages — official development / press sources only. */
export const WOODSPRING_MOMENTUM_PACKAGES = Object.freeze([
  {
    id: "ws_momentum_press_kit",
    sourceUrlMatch: "media.choicehotels.com/woodspring",
    dateLine: "2024",
    title: "WoodSpring Brand Press Resources Updated",
    summary:
      "Choice Hotels maintains official WoodSpring press materials for owners evaluating extended-stay positioning, prototype context, and brand storytelling—useful for diligence on how the brand presents itself to development partners and guests.",
  },
  {
    id: "ws_momentum_development_brand",
    sourceUrlMatch: "choicehotelsdevelopment.com/our-brands/extended-stay/woodspring",
    dateLine: "Recent",
    title: "WoodSpring Development Page Signals Extended-Stay Focus",
    summary:
      "Choice's WoodSpring development materials emphasize kitchen-equipped suites, weekly-stay orientation, and economy extended-stay operating models—owners should review prototype requirements and conversion scope against local demand drivers.",
  },
  {
    id: "ws_momentum_extended_stay_hub",
    sourceUrlMatch: "choicehotelsdevelopment.com/our-brands/extended-stay",
    dateLine: "Recent",
    title: "Choice Reinforces Extended-Stay Portfolio Strategy",
    summary:
      "Choice Hotels groups WoodSpring within its extended-stay platform alongside other weekly-stay brands—owners comparing affiliation paths should diligence category positioning, development support, and operating model fit.",
  },
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-woodspring-source-registry-readiness-writer.json",
  "reports/brand-explorer-woodspring-presentation-cleanup-backfill-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "lib/partner-intelligence/brand-explorer-momentum-link-label.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "live WoodSpring presentation / sources / registry / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-openings-momentum-build-writer.js",
  "scripts/brand-explorer-woodspring-openings-momentum-build-writer.mjs",
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

export function v33cWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-woodspring-openings-momentum-build-writer.js")
  );
}

function buildOpeningBody({ chips, location, meta, scenario, teaser, sourceUrl }) {
  return [chips, location, meta, scenario, teaser, sourceUrl].filter(Boolean).join("\n\n");
}

function validateRowCopy(text, { slotKey } = {}) {
  const errors = [];
  if (METADATA_STYLE_RE.test(text)) errors.push("metadata_language");
  if (BLOCKED_OWNER_FACING_RE.test(text)) errors.push("blocked_owner_facing");
  if (PERFORMANCE_RE.test(text)) errors.push("performance_claim");
  if (scanCopySafety(text).length) errors.push(`copy_safety:${scanCopySafety(text).join(",")}`);
  if (slotKey === MOMENTUM_SLOT && isMomentumInappropriatePropertyListing(extractUrlFromText(text))) {
    errors.push("property_listing_momentum");
  }
  return errors;
}

function extractUrlFromText(text) {
  const m = nz(text).match(/https?:\/\/[^\s<>"')]+/i);
  return m ? m[0].replace(/[.,;)]+$/, "") : "";
}

function validatePresentationFields(fields, { slotKey } = {}) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PATCH_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
  }
  const combined = `${fields.Title || ""}\n${fields.Body || ""}`;
  errors.push(...validateRowCopy(combined, { slotKey }));
  const url = nz(fields["Summary URL"]) || extractUrlFromText(fields.Body);
  if (url && isTemporaryAirtableUrl(url)) errors.push("temporary_source_url");
  if (url && /\b(localhost|airtable\.com\/app)\b/i.test(url)) errors.push("internal_source_url");
  return errors;
}

function matchSourceToPackage(sources, urlMatch, { excludePattern = null, preferExact = false } = {}) {
  const needle = urlMatch.toLowerCase();
  const candidates = sources.filter((s) => {
    const u = nz(s.sourceUrl).toLowerCase();
    if (!u.includes(needle)) return false;
    if (excludePattern && excludePattern.test(u)) return false;
    return true;
  });
  if (!candidates.length) return null;
  if (preferExact) {
    const exact = candidates.find((s) => nz(s.sourceUrl).toLowerCase().endsWith(needle.replace(/\/$/, "")));
    if (exact) return exact;
  }
  return candidates.sort((a, b) => nz(a.sourceUrl).length - nz(b.sourceUrl).length)[0];
}

function auditSourceCandidate(source) {
  const audit = auditEverhomeSourceRecord(source);
  return {
    recordId: audit.recordId,
    sourceTitle: audit.sourceTitle,
    sourceUrl: audit.sourceUrl,
    sourceType: audit.sourceType,
    confidenceLevel: audit.confidenceLevel || source.sourceQuality || "High",
    approvedForExplorerUse: audit.approvedExplorer ? "Yes" : nz(source.approvedForExplorerUse),
    evidenceUseCases: audit.evidenceUseCases,
    durable: audit.durable,
    classification: audit.classification,
    momentumAppropriate: audit.momentumAppropriate,
    openingsAppropriate: audit.openingsAppropriate,
    momentumSourceRank: momentumEvidenceSourceRank(audit.sourceUrl),
    momentumParity: followsTributeMomentumRules(audit.sourceUrl),
    supportedFact: audit.durable
      ? `Official ${audit.classification} source for WoodSpring extended-stay positioning`
      : "not_durable",
  };
}

function presentationCreateFields({
  slotKey,
  title,
  body,
  sort,
  brandRecordId,
  brandName,
}) {
  return {
    "Slot Key": slotKey,
    Title: title,
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort ?? 0,
  };
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
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      summaryUrl:
        nz(f["Summary URL"] || f["View Summary URL"]) || extractUrlFromText(nz(f.Body)),
      sortOrder: f["Sort Order"],
      externalDisplayStatus: nz(f["External Display Status"]),
    };
  });
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

function proposeOpeningCreate(pkg, source, sort, brandRecordId, brandName) {
  if (!source || !isApprovedExplorerSource(source)) {
    return { blocked: true, packageId: pkg.id, reason: "source_not_approved_for_explorer" };
  }
  const sourceUrl = nz(source.sourceUrl);
  if (!sourceUrl || isTemporaryAirtableUrl(sourceUrl)) {
    return { blocked: true, packageId: pkg.id, reason: "missing_or_temporary_source_url" };
  }
  const body = buildOpeningBody({
    chips: pkg.chips,
    location: pkg.location,
    meta: pkg.meta,
    scenario: pkg.scenario,
    teaser: pkg.teaser,
    sourceUrl,
  });
  const fields = presentationCreateFields({
    slotKey: OPENINGS_SLOT,
    title: pkg.title,
    body,
    sort,
    brandRecordId,
    brandName,
  });
  const validationErrors = validatePresentationFields(fields, { slotKey: OPENINGS_SLOT });
  if (validationErrors.length) {
    return { blocked: true, packageId: pkg.id, reason: validationErrors.join(";") };
  }
  return {
    blocked: false,
    packageId: pkg.id,
    slotKey: OPENINGS_SLOT,
    sourceRecordId: source.id || source.recordId,
    sourceUrl,
    sourceLabel: momentumLinkLabelForUrl(sourceUrl, { name: TARGET_BRAND.name }),
    fields,
    title: pkg.title,
    teaser: pkg.teaser,
    imageScopeNote: "v33D — image field intentionally blank; attach after registry approval",
  };
}

function proposeMomentumCreate(pkg, source, sort, brandRecordId, brandName) {
  if (!source || !isApprovedExplorerSource(source)) {
    return { blocked: true, packageId: pkg.id, reason: "source_not_approved_for_explorer" };
  }
  const sourceUrl = nz(source.sourceUrl);
  if (!sourceUrl || isTemporaryAirtableUrl(sourceUrl)) {
    return { blocked: true, packageId: pkg.id, reason: "missing_or_temporary_source_url" };
  }
  if (isMomentumInappropriatePropertyListing(sourceUrl)) {
    return { blocked: true, packageId: pkg.id, reason: "property_listing_not_momentum_evidence" };
  }
  const parity = followsTributeMomentumRules(sourceUrl);
  if (!parity.ok) {
    return { blocked: true, packageId: pkg.id, reason: parity.reason };
  }
  const body = buildMomentumBody({
    dateLine: pkg.dateLine,
    summary: pkg.summary,
    sourceUrl,
  });
  const fields = presentationCreateFields({
    slotKey: MOMENTUM_SLOT,
    title: pkg.title,
    body,
    sort,
    brandRecordId,
    brandName,
  });
  const validationErrors = validatePresentationFields(fields, { slotKey: MOMENTUM_SLOT });
  if (validationErrors.length) {
    return { blocked: true, packageId: pkg.id, reason: validationErrors.join(";") };
  }
  return {
    blocked: false,
    packageId: pkg.id,
    slotKey: MOMENTUM_SLOT,
    sourceRecordId: source.id || source.recordId,
    sourceUrl,
    sourceLabel: momentumLinkLabelForUrl(sourceUrl, { name: TARGET_BRAND.name }),
    sourceType: classifyMomentumSourceType(sourceUrl),
    evidenceRank: momentumEvidenceSourceRank(sourceUrl),
    fields,
    title: pkg.title,
    summary: pkg.summary,
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-openings-momentum-build-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_SOURCE_LIBRARY,
    APPLY_FLAG_NO_REGISTRY_APPROVAL,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Openings + Momentum Build v33C");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v33C exists: **${report.v33cWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Image fields untouched: **${report.imageFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Proposed creates");
  lines.push(`- Openings: **${report.openingsRowsProposed.length}**`);
  lines.push(`- Momentum: **${report.momentumRowsProposed.length}**`);
  lines.push(`- Blocked/skipped: **${report.rowsBlockedOrSkipped.length}**`);
  lines.push("");
  lines.push("## Readiness projection");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Visual defects: ${report.expectedVisualDefectResult}`);
  lines.push(`- Next: ${report.recommendedNextWriter}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}

export async function buildBrandExplorerWoodspringOpeningsMomentumBuildWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noImageFieldChanges = false,
  noSourceLibrary = false,
  noRegistryApproval = false,
  woodspringOnly = false,
} = {}) {
  const slug = nz(brandArg).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Protected brand cannot be modified by v33C: ${slug}`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33C is WoodSpring-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const brandApi = await fetchBrandApiShape(TARGET_BRAND.recordId);
  if (!brandApi) throw new Error("Could not load WoodSpring API shape");

  const presentationRows = await listPresentationRows(
    baseId,
    apiKey,
    TARGET_BRAND.recordId,
    TARGET_BRAND.name
  );
  const sourceRows = await fetchAllBrandSources(TARGET_BRAND.recordId);
  const registryAssets = await listRegistryAssetsForBrand(TARGET_BRAND.recordId).catch(() => []);

  const existingOpenings = presentationRows.filter((r) => r.slotKey === OPENINGS_SLOT);
  const existingMomentum = presentationRows.filter((r) => r.slotKey === MOMENTUM_SLOT);

  const approvedSources = sourceRows.filter((s) => isApprovedExplorerSource(s));
  const sourceCandidateAudit = sourceRows.map(auditSourceCandidate);

  const openingsRowsProposed = [];
  const momentumRowsProposed = [];
  const rowsBlockedOrSkipped = [];

  if (existingOpenings.length >= TARGET_OPENINGS) {
    rowsBlockedOrSkipped.push({
      slotKey: OPENINGS_SLOT,
      reason: `already_has_${existingOpenings.length}_rows`,
    });
  } else {
    for (let i = 0; i < WOODSPRING_OPENING_PACKAGES.length; i++) {
      const pkg = WOODSPRING_OPENING_PACKAGES[i];
      const source = matchSourceToPackage(approvedSources, pkg.sourceUrlMatch, {
        excludePattern: pkg.id.includes("extended_stay_hub") ? /\/woodspring/i : null,
        preferExact: pkg.id.includes("extended_stay_hub"),
      });
      const proposal = proposeOpeningCreate(
        pkg,
        source,
        i,
        TARGET_BRAND.recordId,
        TARGET_BRAND.name
      );
      if (proposal.blocked) {
        rowsBlockedOrSkipped.push({ type: "openings", packageId: pkg.id, reason: proposal.reason });
      } else {
        openingsRowsProposed.push(proposal);
      }
    }
  }

  if (existingMomentum.length >= TARGET_MOMENTUM) {
    rowsBlockedOrSkipped.push({
      slotKey: MOMENTUM_SLOT,
      reason: `already_has_${existingMomentum.length}_rows`,
    });
  } else {
    const usedMomentumUrls = new Set();
    for (let i = 0; i < WOODSPRING_MOMENTUM_PACKAGES.length; i++) {
      const pkg = WOODSPRING_MOMENTUM_PACKAGES[i];
      const source = matchSourceToPackage(approvedSources, pkg.sourceUrlMatch, {
        excludePattern: pkg.id.includes("extended_stay_hub") ? /\/woodspring/i : null,
        preferExact: pkg.id.includes("extended_stay_hub"),
      });
      const proposal = proposeMomentumCreate(
        pkg,
        source,
        i,
        TARGET_BRAND.recordId,
        TARGET_BRAND.name
      );
      if (proposal.blocked) {
        rowsBlockedOrSkipped.push({ type: "momentum", packageId: pkg.id, reason: proposal.reason });
        continue;
      }
      if (usedMomentumUrls.has(proposal.sourceUrl)) {
        rowsBlockedOrSkipped.push({
          type: "momentum",
          packageId: pkg.id,
          reason: "duplicate_momentum_source_url",
        });
        continue;
      }
      usedMomentumUrls.add(proposal.sourceUrl);
      momentumRowsProposed.push(proposal);
    }
  }

  const sourceLinkQualityFindings = [
    ...openingsRowsProposed.map((r) => ({
      slotKey: r.slotKey,
      sourceUrl: r.sourceUrl,
      durable: !isTemporaryAirtableUrl(r.sourceUrl),
      label: r.sourceLabel,
      supported: true,
    })),
    ...momentumRowsProposed.map((r) => ({
      slotKey: r.slotKey,
      sourceUrl: r.sourceUrl,
      durable: !isTemporaryAirtableUrl(r.sourceUrl),
      label: r.sourceLabel,
      evidenceRank: r.evidenceRank,
      supported: r.evidenceRank >= 60,
    })),
  ];

  const copyQualityFindings = [
    ...openingsRowsProposed.map((r) => ({
      slotKey: r.slotKey,
      title: r.title,
      issues: validateRowCopy(`${r.title}\n${r.teaser}`, { slotKey: OPENINGS_SLOT }),
    })),
    ...momentumRowsProposed.map((r) => ({
      slotKey: r.slotKey,
      title: r.title,
      issues: validateRowCopy(`${r.title}\n${r.summary}`, { slotKey: MOMENTUM_SLOT }),
    })),
  ].filter((f) => f.issues.length);

  const applyBlockers = [];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noImageFieldChanges) applyBlockers.push("missing_confirm_no_image_field_changes");
    if (!noSourceLibrary) applyBlockers.push("missing_confirm_no_source_library_changes");
    if (!noRegistryApproval) applyBlockers.push("missing_confirm_no_registry_approval_changes");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  if (openingsRowsProposed.length < 3) applyBlockers.push("insufficient_openings_proposals");
  if (momentumRowsProposed.length < 3) applyBlockers.push("insufficient_momentum_proposals");
  if (copyQualityFindings.length) applyBlockers.push("copy_quality_issues_in_proposals");

  const hasWork = openingsRowsProposed.length > 0 || momentumRowsProposed.length > 0;

  let airtableModified = false;
  const applyResults = { openingsCreated: [], momentumCreated: [], errors: [] };
  let imageFieldsChanged = false;

  const canApply =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFieldChanges &&
    noSourceLibrary &&
    noRegistryApproval &&
    woodspringOnly &&
    applyBlockers.length === 0;

  if (canApply) {
    for (const create of [...openingsRowsProposed, ...momentumRowsProposed]) {
      try {
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "POST",
          body: JSON.stringify({ fields: create.fields, typecast: true }),
        });
        if (!res.ok) throw new Error(json.error?.message || `POST failed: ${res.status}`);
        const entry = { recordId: json.id, slotKey: create.slotKey, title: create.title };
        if (create.slotKey === OPENINGS_SLOT) applyResults.openingsCreated.push(entry);
        else applyResults.momentumCreated.push(entry);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ slotKey: create.slotKey, title: create.title, message: err.message });
      }
    }
    if (applyResults.errors.length) {
      applyBlockers.push(`apply_errors:${applyResults.errors.length}`);
    }
  }

  const dryRunClean = applyBlockers.length === 0 && hasWork;

  const brandBasicsAfter = canApply ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: TARGET_BRAND.slug,
  }).catch(() => null);
  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: TARGET_BRAND.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const visualReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: TARGET_BRAND.recordId,
  }).catch(() => null);

  const wsQa = (finalQaReport?.brandReports || []).find((b) => b.brand?.slug === TARGET_BRAND.slug);
  const wsBuild = (completeBuildReport?.brandResults || [])[0];

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33cWriterExists: v33cWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    existingOpeningsCount: existingOpenings.length,
    existingMomentumCount: existingMomentum.length,
    sourceCandidateAudit,
    openingsRowsProposed,
    momentumRowsProposed,
    rowsBlockedOrSkipped,
    sourceLinkQualityFindings,
    copyQualityFindings,
    registryReadOnlyCount: registryAssets.length,
    imageFieldsUntouched: !imageFieldsChanged,
    imageScopeNote:
      "Openings created without images by design — v33D handles registry approval and image materialization.",
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    airtableModified,
    applyBlockers,
    dryRunClean,
    applyResults,
    expectedFinalQaResult: wsQa
      ? `${wsQa.overallReadiness || "projected"} (${wsQa.readinessScore ?? "?"})`
      : "projected improvement after openings/momentum create",
    expectedCompleteBuildResult: wsBuild
      ? `readyForActiveProfile: ${wsBuild.readyForActiveProfile} (${wsBuild.readinessBand || "?"})`
      : "projected — openings/momentum gates may clear",
    expectedVisualDefectResult: visualReport
      ? `${visualReport.defectCount ?? visualReport.defects?.length ?? "?"} defects`
      : "unchanged — gallery/scenario images remain v33D",
    remainingBlockersForV33dV33e: [
      "overview.scenario.1/.2 temporary images — v33D",
      "materials.gallery.1–6 missing images — v33D",
      "registry candidate approval — v33D",
      "overview.why_value empty bullets — v33E",
      "final fact stewardship — v33E",
    ],
    recommendedNextWriter: "v33D — WoodSpring existing image recognition / registry approval",
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand: `npm run brand-explorer-woodspring-openings-momentum-build-writer -- --brand ${TARGET_BRAND.slug} --dry-run`,
    applyGuardrails: {
      woodspringOnly: true,
      noImageFieldChanges: true,
      noSourceLibraryChanges: true,
      noRegistryApprovalChanges: true,
      noScenarioGalleryChanges: true,
    },
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
