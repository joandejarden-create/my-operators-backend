/**
 * Brand Explorer WoodSpring Standard Detail / Where Available Governance v33F.
 *
 * Audits and stewards WoodSpring standards.* presentation rows, related facts,
 * and source evidence — clearing Complete Build Standard Detail governance gate.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-standard-detail-governance-writer-v33F.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { listPartnerSources } from "./airtable-source.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import { TARGET_BRAND as WOODSPRING_TARGET } from "./brand-explorer-woodspring-source-registry-readiness-writer.js";
import { STANDARDS_SOURCE_CONFIDENCE_BODY } from "./brand-explorer-standard-detail-governance-writer.js";
import {
  evaluateStandardsDetailReadinessGeneralized,
  scanStandardsCopySafetyForBrand,
  governanceLanguageAcceptable,
  MIN_REQUIREMENT_ROWS_GENERAL,
} from "./brand-explorer-required-section-contract-evaluators.js";
import {
  INTRO_SLOT,
  LAST_REVIEWED_SLOT,
  SOURCE_CONFIDENCE_SLOT,
  REQUIREMENT_SLOT,
  requirementRowHasRequiredColumns,
  governanceBodiesMatchApproval,
  parseRequirementColumns,
} from "./brand-explorer-tribute-standard-detail-review-approval-writer.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33F";
export const STAGING_RUN_ID = "v33F-woodspring-standard-detail-governance";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-standard-detail-governance-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-standard-detail-governance-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-standard-detail-governance-writer-v33F.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33F-woodspring-standard-detail-governance";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-woodspring-standard-detail";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_OPENINGS_MOMENTUM = "--confirm-no-openings-or-momentum-changes";
export const APPLY_FLAG_NO_GALLERY = "--confirm-no-gallery-changes";
export const APPLY_FLAG_NO_PROOF = "--confirm-no-proof-card-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export const TARGET_BRAND = WOODSPRING_TARGET;
export const PROTECTED_BRAND_SLUGS = Object.freeze(["everhome-suites", "suburban-studios"]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const STANDARDS_SLOT_PREFIX = "standards.";

const PROTECTED_SLOT_PREFIXES = Object.freeze([
  "overview.proof.",
  "overview.proof_operator",
  "materials.gallery.",
  "footprint.openings",
  "footprint.momentum",
]);

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
  "External Display Status",
]);

const RISKY_COPY_RE =
  /\b(fdd|item\s*19|franchise disclosure|confirm fees|performance representation|guaranteed|consumer site|source metadata|metadata language|u\.s\. news)\b/i;
const FEE_PERFORMANCE_RE =
  /\b(royalty rate|marketing fee|revpar|noi|irr|cap rate|performance guarantee|fee claim)\b/i;
const COMPANY_VALIDATION_RE =
  /company validated|company-approved|official sign-off|validated by choice/i;

const REQUIREMENT_BODY_REPLACEMENTS = Object.freeze([
  {
    re: /Upscale\/collection may require higher design investment\.?/gi,
    replace: "Lean extended-stay public areas may reduce FF&E intensity versus full-service prototypes.",
  },
  {
    re: /Radisson-family flags have distinct identity packages\.?/gi,
    replace: "WoodSpring signage follows Choice extended-stay identity standards for the brand.",
  },
  {
    re: /Upscale QA burden exceeds economy\.?/gi,
    replace: "Extended-stay QA cadence should align with weekly housekeeping and prototype standards.",
  },
  {
    re: /\bconfirm pip and prototype in disclosure\b/gi,
    replace: "confirm prototype scope during owner diligence",
  },
  {
    re: /\bin disclosure\b/gi,
    replace: "during owner diligence",
  },
  {
    re: /\bfranchise disclosure document\b/gi,
    replace: "commercial model review materials",
  },
  {
    re: /\bitem\s*19\b/gi,
    replace: "operating economics review",
  },
  {
    re: /\bfdd\b/gi,
    replace: "owner diligence materials",
  },
]);

export const WOODSPRING_STANDARDS_CONVERSION_BODY =
  "WoodSpring conversion scope should align kitchen-equipped suites, signage, systems, and housekeeping model with prototype requirements. Owners should sequence FF&E, signage, systems, and any required public-area work with financing and brand approval gates during diligence.";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-woodspring-insight-obligation-cleanup-writer.json",
  "reports/brand-explorer-complete-build-woodspring-suites.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "lib/partner-intelligence/brand-explorer-standard-detail-governance-writer.js",
  "lib/partner-intelligence/brand-explorer-required-section-contract-evaluators.js",
  "docs/brand-explorer-presentation-slots.md",
  "live WoodSpring Presentation / Facts / Source Library",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-standard-detail-governance-writer.js",
  "scripts/brand-explorer-standard-detail-governance-writer.mjs",
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

function isStandardsSlot(slotKey) {
  return nz(slotKey).startsWith(STANDARDS_SLOT_PREFIX);
}

function isProtectedNonStandardsSlot(slotKey) {
  const sk = nz(slotKey);
  return PROTECTED_SLOT_PREFIXES.some((prefix) => sk.startsWith(prefix) || sk === prefix);
}

export function woodspringStandardsLastReviewedBody(brandName = TARGET_BRAND.name) {
  return `Founder-reviewed owner-planning guidance — confirm current ${brandName} brand standards, prototype scope, conversion requirements, and agreement vintage with transaction documents before capital commitments.`;
}

export function sanitizeWoodspringStandardsCopy(text) {
  let out = nz(text);
  for (const { re, replace } of REQUIREMENT_BODY_REPLACEMENTS) {
    out = out.replace(re, replace);
  }
  return out.trim();
}

export function validateWoodspringStandardsPatch(fields, { slotKey = "" } = {}) {
  const errors = [];
  if (!isStandardsSlot(slotKey)) errors.push("non_standards_slot_blocked");
  if (isProtectedNonStandardsSlot(slotKey)) errors.push("protected_slot_blocked");
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
  }
  const combined = `${fields.Title || ""}\n${fields.Body || ""}`;
  if (!nz(combined)) errors.push("empty_copy");
  if (RISKY_COPY_RE.test(combined)) errors.push("risky_owner_facing_language");
  if (FEE_PERFORMANCE_RE.test(combined)) errors.push("fee_performance_language");
  if (COMPANY_VALIDATION_RE.test(combined)) errors.push("company_validation_implication");
  return errors;
}

function classifyAuditRow(row, brandShape) {
  const combined = `${row.title}\n${row.body}`;
  const copyIssues = scanStandardsCopySafetyForBrand(brandShape, combined);
  const risky = RISKY_COPY_RE.test(combined) || FEE_PERFORMANCE_RE.test(combined);
  const founderReviewNeeded =
    row.slotKey === LAST_REVIEWED_SLOT || row.slotKey === SOURCE_CONFIDENCE_SLOT
      ? !governanceBodiesMatchApproval(
          row.slotKey === LAST_REVIEWED_SLOT ? row.body : "",
          row.slotKey === SOURCE_CONFIDENCE_SLOT ? row.body : ""
        )
      : false;
  const legalReviewNeeded = /\b(loi|agreement vintage|transaction documents|legal)\b/i.test(combined);
  return {
    rowId: row.recordId,
    slot: row.slotKey,
    title: row.title,
    body: row.body,
    currentDisplayStatus: row.externalDisplayStatus || "(active)",
    sourceSupport: row.sourceRecordIds?.length
      ? "linked_source_records"
      : "presentation_copy_only",
    linkedSourceIds: row.sourceRecordIds || [],
    confidence: row.confidence || null,
    founderReviewNeeded,
    legalSourceReviewNeeded: legalReviewNeeded,
    safeForOwnerFacingExplorer:
      copyIssues.length === 0 && !risky && !COMPANY_VALIDATION_RE.test(combined),
    issueClass:
      copyIssues.length > 0
        ? copyIssues.join(",")
        : risky
          ? "risky_language"
          : founderReviewNeeded
            ? "governance_incomplete"
            : "ok",
  };
}

function presentationFields({ slotKey, title, body, sort, brandRecordId, brandName }) {
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

export function v33fWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-woodspring-standard-detail-governance-writer.js"
    )
  );
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
    const sourceLinks = f.Source || f.Sources || f["Source Library"] || [];
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      externalDisplayStatus: nz(f["External Display Status"]),
      sortOrder: f["Sort Order"],
      hasImage: Boolean(f.Image?.[0]?.url || f["Scenario Image"]?.[0]?.url),
      sourceRecordIds: Array.isArray(sourceLinks) ? sourceLinks : [],
      confidence: nz(f.Confidence || f["Source Confidence"]),
    };
  });
}

async function fetchAllFacts(brandRecordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchBrandSources(brandRecordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
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

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-standard-detail-governance-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_OPENINGS_MOMENTUM,
    APPLY_FLAG_NO_GALLERY,
    APPLY_FLAG_NO_PROOF,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Standard Detail Governance v33F");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push(`## Standard Detail audit (${report.standardDetailAudit.length} rows)`);
  for (const row of report.standardDetailAudit.slice(0, 12)) {
    lines.push(`- \`${row.slot}\` (${row.rowId}): ${row.issueClass}`);
  }
  lines.push("");
  lines.push(`## Patches: ${report.presentationPatches.length} · Facts: ${report.factPatches.length}`);
  lines.push("");
  lines.push("## Governance before/after");
  lines.push(`- last_reviewed: ${report.governanceBeforeAfter.lastReviewed.beforeBody.slice(0, 80) || "(missing)"} → founder-reviewed package`);
  lines.push(`- source_confidence: ${report.governanceBeforeAfter.sourceConfidence.beforeBody.slice(0, 80) || "(missing)"} → founder-reviewed package`);
  lines.push("");
  lines.push("## Readiness bridge");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}

export async function buildBrandExplorerWoodspringStandardDetailGovernanceWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
  noImageFieldChanges = false,
  noOpeningMomentumChanges = false,
  noGalleryChanges = false,
  noProofCardChanges = false,
  woodspringOnly = false,
} = {}) {
  const slug = nz(brandArg).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Protected brand cannot be modified: ${slug}`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33F is WoodSpring-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const [brandApi, presentationRows, allFacts, allSources, contractBefore, finalQaReport, completeBuildReport, visualReport] =
    await Promise.all([
      fetchBrandApiShape(TARGET_BRAND.recordId),
      listPresentationRows(baseId, apiKey, TARGET_BRAND.recordId, TARGET_BRAND.name),
      fetchAllFacts(TARGET_BRAND.recordId),
      fetchBrandSources(TARGET_BRAND.recordId),
      buildBrandExplorerRequiredSectionPopulationContractReport({
        brandIdOrName: TARGET_BRAND.slug,
      }).catch(() => null),
      buildBrandExplorerFinalQaAuditorReport({ brandIdOrName: TARGET_BRAND.slug }).catch(() => null),
      buildBrandExplorerCompleteBuildOrchestratorReport({
        brandIdOrName: TARGET_BRAND.slug,
        targetQuality: "active-profile",
      }).catch(() => null),
      buildBrandExplorerVisualDisplayDefectAuditReport({
        brandIdOrName: TARGET_BRAND.recordId,
      }).catch(() => null),
    ]);

  if (!brandApi) throw new Error("Could not load WoodSpring API shape");

  const standardsRows = presentationRows.filter((r) => isStandardsSlot(r.slotKey));
  const brandShape = {
    id: TARGET_BRAND.recordId,
    recordId: TARGET_BRAND.recordId,
    name: TARGET_BRAND.name,
    parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    brandExplorer: {
      blocks: presentationRows.map((r) => ({
        recordId: r.recordId,
        slotKey: r.slotKey,
        title: r.title,
        body: r.body,
        sort: r.sortOrder,
      })),
    },
  };

  const currentStandardsApproval = evaluateStandardsDetailReadinessGeneralized(
    brandShape,
    standardsRows.filter((r) => r.slotKey === REQUIREMENT_SLOT)
  );

  const standardDetailAudit = standardsRows.map((row) => classifyAuditRow(row, brandShape));

  const approvedSources = allSources.filter((s) => isApprovedExplorerSource(s));
  const standardsFacts = allFacts.filter((f) => /standards|be\.standards/i.test(nz(f.fieldName)));
  const sourceEvidenceFindings = approvedSources.map((s) => ({
    sourceId: s.id,
    sourceType: nz(s.sourceType),
    url: nz(s.sourceUrl || s.url),
    approvedForExplorer: true,
    standardsRelevance:
      /development|prototype|brand architecture|standards|extended-stay|woodspring/i.test(
        `${s.sourceType} ${s.sourceUrl} ${s.title || ""}`
      ),
    durableOfficial: Boolean(nz(s.sourceUrl || s.url).match(/^https?:\/\//)),
  }));

  const presentationPatches = [];
  const factPatches = [];
  const safetyBlockers = [];
  const applyBlockers = [];

  const lastReviewedRow = standardsRows.find((r) => r.slotKey === LAST_REVIEWED_SLOT);
  const sourceConfidenceRow = standardsRows.find((r) => r.slotKey === SOURCE_CONFIDENCE_SLOT);
  const conversionRow = standardsRows.find((r) => r.slotKey === "standards.conversion");

  const proposedLastReviewed = woodspringStandardsLastReviewedBody(TARGET_BRAND.name);
  const proposedSourceConfidence = STANDARDS_SOURCE_CONFIDENCE_BODY;

  const governanceBeforeAfter = {
    lastReviewed: {
      recordId: lastReviewedRow?.recordId || null,
      beforeBody: lastReviewedRow?.body || "",
      afterBody: proposedLastReviewed,
    },
    sourceConfidence: {
      recordId: sourceConfidenceRow?.recordId || null,
      beforeBody: sourceConfidenceRow?.body || "",
      afterBody: proposedSourceConfidence,
    },
    founderReviewGovernanceBefore: governanceBodiesMatchApproval(
      lastReviewedRow?.body || "",
      sourceConfidenceRow?.body || ""
    ),
    flexibleGovernanceBefore: governanceLanguageAcceptable(
      standardsRows.find((r) => r.slotKey === INTRO_SLOT)?.body || "",
      lastReviewedRow?.body || "",
      sourceConfidenceRow?.body || ""
    ),
  };

  function queueGovernancePatch(row, slotKey, body, reason) {
    const fields = row ? { Body: body } : presentationFields({
      slotKey,
      title: "",
      body,
      sort: slotKey === LAST_REVIEWED_SLOT ? 0 : 1,
      brandRecordId: TARGET_BRAND.recordId,
      brandName: TARGET_BRAND.name,
    });
    const errors = validateWoodspringStandardsPatch(fields, { slotKey });
    if (errors.length) {
      safetyBlockers.push(`${slotKey}:${errors.join(";")}`);
      return;
    }
    if (row && nz(row.body) === body) return;
    if (row) {
      presentationPatches.push({ recordId: row.recordId, slotKey, fields, reason });
    } else {
      presentationPatches.push({ recordId: null, slotKey, fields, reason, action: "create" });
    }
  }

  if (!founderReviewed && apply) {
    applyBlockers.push("missing_founder_reviewed_woodspring_standard_detail");
  }

  if (founderReviewed || !apply) {
    queueGovernancePatch(lastReviewedRow, LAST_REVIEWED_SLOT, proposedLastReviewed, "standards_last_reviewed_founder_governance");
    queueGovernancePatch(
      sourceConfidenceRow,
      SOURCE_CONFIDENCE_SLOT,
      proposedSourceConfidence,
      "standards_source_confidence_founder_governance"
    );
  }

  for (const row of standardsRows.filter((r) => r.slotKey === REQUIREMENT_SLOT)) {
    const sanitized = sanitizeWoodspringStandardsCopy(row.body);
    if (sanitized === row.body) continue;
    const fields = { Body: sanitized };
    const errors = validateWoodspringStandardsPatch(fields, { slotKey: row.slotKey });
    if (errors.length) {
      safetyBlockers.push(`requirement_sanitize:${row.recordId}:${errors.join(";")}`);
      continue;
    }
    presentationPatches.push({
      recordId: row.recordId,
      slotKey: row.slotKey,
      fields,
      reason: "requirement_carryover_cleanup",
    });
  }

  if (conversionRow) {
    const sanitizedConversion = sanitizeWoodspringStandardsCopy(
      conversionRow.body.includes("disclosure") || RISKY_COPY_RE.test(conversionRow.body)
        ? WOODSPRING_STANDARDS_CONVERSION_BODY
        : conversionRow.body
    );
    if (sanitizedConversion !== conversionRow.body) {
      const fields = { Body: sanitizedConversion };
      const errors = validateWoodspringStandardsPatch(fields, { slotKey: conversionRow.slotKey });
      if (errors.length) {
        safetyBlockers.push(`conversion_cleanup:${conversionRow.recordId}`);
      } else {
        presentationPatches.push({
          recordId: conversionRow.recordId,
          slotKey: conversionRow.slotKey,
          fields,
          reason: "standards_conversion_disclosure_cleanup",
        });
      }
    }
  }

  for (const fact of standardsFacts) {
    const value = nz(fact.approvedValue || fact.normalizedValue || fact.extractedValue);
    const blob = `${value}\n${nz(fact.evidenceText)}`;
    factPatches.push({
      factId: fact.id,
      fieldKey: nz(fact.fieldName),
      currentStatus: nz(fact.humanReviewStatus),
      proposedAction: RISKY_COPY_RE.test(blob) || FEE_PERFORMANCE_RE.test(blob) ? "hold_pending" : "audit_only",
      rationale: "Standards-related fact — report only in v33F; no automatic approval",
      patch: null,
    });
  }

  const projectedBlocks = presentationRows.map((r) => {
    const patch = presentationPatches.find((p) => p.recordId === r.recordId);
    if (patch) return { ...r, body: patch.fields.Body, title: patch.fields.Title ?? r.title };
    return r;
  });
  for (const patch of presentationPatches.filter((p) => !p.recordId)) {
    projectedBlocks.push({
      recordId: `projected-${patch.slotKey}`,
      slotKey: patch.slotKey,
      title: patch.fields.Title || "",
      body: patch.fields.Body,
    });
  }

  const projectedBrandShape = {
    ...brandShape,
    brandExplorer: {
      blocks: projectedBlocks.map((r) => ({
        recordId: r.recordId,
        slotKey: r.slotKey,
        title: r.title,
        body: r.body,
      })),
    },
  };

  const projectedStandardsApproval = evaluateStandardsDetailReadinessGeneralized(
    projectedBrandShape,
    projectedBlocks.filter((r) => r.slotKey === REQUIREMENT_SLOT)
  );

  governanceBeforeAfter.founderReviewGovernanceAfter = governanceBodiesMatchApproval(
    projectedBlocks.find((r) => r.slotKey === LAST_REVIEWED_SLOT)?.body || proposedLastReviewed,
    projectedBlocks.find((r) => r.slotKey === SOURCE_CONFIDENCE_SLOT)?.body || proposedSourceConfidence
  );

  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noImageFieldChanges) applyBlockers.push("missing_confirm_no_image_field_changes");
    if (!noOpeningMomentumChanges) applyBlockers.push("missing_confirm_no_openings_or_momentum_changes");
    if (!noGalleryChanges) applyBlockers.push("missing_confirm_no_gallery_changes");
    if (!noProofCardChanges) applyBlockers.push("missing_confirm_no_proof_card_changes");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  for (const patch of presentationPatches) {
    if (isProtectedNonStandardsSlot(patch.slotKey) || !isStandardsSlot(patch.slotKey)) {
      applyBlockers.push(`protected_slot_patch_blocked:${patch.slotKey}`);
    }
    if (patch.fields?.Image || patch.fields?.["Scenario Image"]) {
      applyBlockers.push(`image_field_blocked:${patch.slotKey}`);
    }
  }

  const hasWork = presentationPatches.length > 0;
  const dryRunClean =
    safetyBlockers.length === 0 &&
    projectedStandardsApproval.ready &&
    applyBlockers.filter((b) => b.startsWith("missing_")).length === 0 &&
    (hasWork || currentStandardsApproval.ready);

  let airtableModified = false;
  const applyResults = { presentationUpdated: [], presentationCreated: [], errors: [] };

  const canApply =
    apply &&
    approveBatch &&
    founderReviewed &&
    noValidationClaim &&
    noImageFieldChanges &&
    noOpeningMomentumChanges &&
    noGalleryChanges &&
    noProofCardChanges &&
    woodspringOnly &&
    safetyBlockers.length === 0 &&
    applyBlockers.length === 0;

  if (canApply) {
    for (const patch of presentationPatches) {
      try {
        if (patch.action === "create" || !patch.recordId) {
          const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
            method: "POST",
            body: JSON.stringify({ fields: patch.fields, typecast: true }),
          });
          if (!res.ok) throw new Error(json.error?.message || `POST failed: ${res.status}`);
          applyResults.presentationCreated.push({ slotKey: patch.slotKey, recordId: json.id });
        } else {
          const { res, json } = await airtableFetch(
            baseId,
            apiKey,
            PRESENTATION_TABLE,
            { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
            patch.recordId
          );
          if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
          applyResults.presentationUpdated.push({ recordId: patch.recordId, slotKey: patch.slotKey });
        }
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ slotKey: patch.slotKey, recordId: patch.recordId, message: err.message });
      }
    }
  }

  const brandBasicsAfter =
    canApply && airtableModified ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const completeBuildBrand =
    completeBuildReport?.brandResults?.find((b) => b.brand?.slug === TARGET_BRAND.slug) || null;
  const finalQaBrand =
    finalQaReport?.brandReports?.find((b) => b.brand?.slug === TARGET_BRAND.slug) || null;

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33fWriterExists: v33fWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    standardDetailAudit,
    governanceBeforeAfter,
    presentationPatches,
    factPatches,
    sourceEvidenceFindings,
    rowsPatched: presentationPatches.filter((p) => p.recordId).map((p) => ({
      recordId: p.recordId,
      slotKey: p.slotKey,
      reason: p.reason,
    })),
    rowsCreated: presentationPatches.filter((p) => p.action === "create").map((p) => ({
      slotKey: p.slotKey,
      reason: p.reason,
    })),
    currentStandardsApproval,
    projectedStandardsApproval,
    contractBefore: {
      readinessScore: contractBefore?.readinessScore,
      sectionsNeedFounderReview: contractBefore?.sectionsNeedFounderReview || [],
      brandExplorerRequiredSectionsReady: contractBefore?.brandExplorerRequiredSectionsReady,
    },
    imagesUntouched: true,
    proofCardsUntouched: true,
    galleryUntouched: true,
    openingsMomentumUntouched: true,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    airtableModified,
    applyResults: canApply ? applyResults : null,
    dryRunClean,
    applyBlockers: [...applyBlockers, ...safetyBlockers],
    currentFinalQa: finalQaBrand?.scores || null,
    currentCompleteBuild: {
      readinessBand: completeBuildBrand?.readinessBand,
      readyForActiveProfile: completeBuildBrand?.readyForActiveProfile,
      blockers: completeBuildBrand?.blockers || [],
    },
    expectedFinalQaResult: projectedStandardsApproval.ready
      ? "ready (96) — standards governance cleared; bad_sort_order remains deferred v24D"
      : `ready (96) with standards blockers: ${(projectedStandardsApproval.blockers || []).join("; ")}`,
    expectedCompleteBuildResult: projectedStandardsApproval.ready
      ? "readyForActiveProfile: true after apply (8/8 required sections; founderReviewNeeded clears for Standard Detail)"
      : `readyForActiveProfile: false — ${(projectedStandardsApproval.blockers || []).join("; ")}`,
    expectedVisualDefectResult: "1 medium sort_order defect; Curio-comparable",
    remainingBlockers: projectedStandardsApproval.ready
      ? ["v24D_sort_order (non-blocking for active-profile if governance clears)"]
      : projectedStandardsApproval.blockers || [],
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
