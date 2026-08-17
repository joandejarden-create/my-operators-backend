/**
 * Brand Explorer Everhome Final Gate Backfill + QA Governance Fix v32H.
 *
 * Clears remaining non-image active-profile gates: Portfolio Mix, Standard Detail
 * governance, Value Creation scenario copy, pending Explorer facts, and shared
 * wrong-brand false-positive detection — without touching working images.
 *
 * @see docs/data-intelligence/brand-explorer-everhome-final-gate-backfill-writer-v32H.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  MAP_PARTNER_FACT,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { listPartnerFacts, patchPartnerFact } from "./airtable-facts.js";
import { getPartnerSourceById } from "./airtable-source.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import {
  EVERHOME_BACKFILL,
  sanitizeEverhomeCopy,
} from "./brand-explorer-everhome-presentation-cleanup-writer.js";
import { TARGET_BRAND } from "./brand-explorer-everhome-openings-momentum-rebuild-writer.js";
import {
  containsSourceMetadataLanguage,
  parseFootprintOpeningParas,
} from "./brand-explorer-everhome-openings-description-cleanup-writer.js";
import {
  followsTributeMomentumRules,
  parseMomentumPresentationBody,
} from "./brand-explorer-momentum-link-label.js";
import {
  resolveMomentumProperCaseTitle,
} from "./brand-explorer-everhome-image-governance-recognition-writer.js";
import {
  openingIsCompleteRow,
  momentumIsCompleteRow,
} from "./brand-explorer-radisson-individuals-openings-momentum-parity-writer.js";
import {
  LAST_REVIEWED_SLOT,
  SOURCE_CONFIDENCE_SLOT,
} from "./brand-explorer-tribute-standard-detail-review-approval-writer.js";
import { STANDARDS_SOURCE_CONFIDENCE_BODY } from "./brand-explorer-standard-detail-governance-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { detectWrongBrandSignageRisk, getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";

export const WRITER_VERSION = "v32H";
export const STAGING_RUN_ID = "v32H-everhome-final-gate-backfill";
export const REPORT_JSON_NAME = "brand-explorer-everhome-final-gate-backfill-writer.json";
export const REPORT_MD_NAME = "brand-explorer-everhome-final-gate-backfill-writer.md";
export const DOC_MD_NAME = "brand-explorer-everhome-final-gate-backfill-writer-v32H.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v32H-everhome-final-gate-backfill";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_OPENING_MOMENTUM = "--confirm-no-opening-or-momentum-structure-changes";
export const APPLY_FLAG_EVERHOME_ONLY = "--confirm-everhome-only";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "woodspring-suites",
  "suburban-studios",
]);

export const EVERHOME_PORTFOLIO_MIX_CHIPS = Object.freeze([
  { title: "New-Construction Extended Stay", body: "High", sort: 0 },
  { title: "Midscale Suite Prototype", body: "High", sort: 1 },
  { title: "Conversion / Repositioning", body: "Moderate", sort: 2 },
  { title: "Weekly Corporate Demand", body: "Moderate", sort: 3 },
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const MIX_SLOT = "footprint.portfolio_mix";
const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
]);

const OPENINGS_MOMENTUM_BLOCKED_FIELDS = new Set([
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
  "Body",
  "Title",
]);

const VALUE_SCENARIO_SLOTS = [
  "valueOwners.scenario.1",
  "valueOwners.scenario.2",
  "valueOwners.scenario.3",
  "valueOwners.scenario.4",
];

const MIN_SCENARIO_WORDS = 18;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-everhome-existing-image-approval-recognition-writer.json",
  "reports/brand-explorer-everhome-openings-description-cleanup-writer.json",
  "reports/brand-explorer-everhome-image-governance-recognition-writer.json",
  "reports/brand-explorer-everhome-presentation-cleanup-writer.json",
  "reports/brand-explorer-everhome-source-registry-normalization-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "api/brand-library.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Everhome Presentation / Facts / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-everhome-final-gate-backfill-writer.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "scripts/brand-explorer-everhome-final-gate-backfill-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FDD_RE = /\b(fdd|item\s*19|franchise disclosure document)\b/i;
const PERFORMANCE_RE = /\b(roi|irr|cap rate|noi|revpar|adr forecast|guaranteed returns?)\b/i;
const COMPANY_VALIDATION_RE =
  /company validated|company-approved|official sign-off|validated by choice/i;

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

export function v32hWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-everhome-final-gate-backfill-writer.js")
  );
}

function standardsLastReviewedBody(brandName) {
  return `Founder-reviewed owner-planning guidance — confirm current ${brandName} prototype standards, extended-stay suite configuration, kitchen-equipped room requirements, conversion scope, and agreement vintage with transaction documents before capital commitments.`;
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

function validatePresentationPatch(fields, { slotKey = "" } = {}) {
  const errors = [];
  const isOpeningMomentum =
    slotKey === OPENINGS_SLOT ||
    slotKey === MOMENTUM_SLOT ||
    slotKey.startsWith("footprint.momentum.");

  for (const key of Object.keys(fields)) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
    if (isOpeningMomentum && OPENINGS_MOMENTUM_BLOCKED_FIELDS.has(key)) {
      errors.push(`opening_momentum_structure_blocked:${key}`);
    }
  }
  const combined = `${fields.Title || ""}\n${fields.Body || ""}`;
  if (FDD_RE.test(combined)) errors.push("fdd_language");
  if (PERFORMANCE_RE.test(combined)) errors.push("performance_claim");
  if (COMPANY_VALIDATION_RE.test(combined)) errors.push("company_validation_implication");
  return errors;
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
      summaryUrl: nz(f["Summary URL"] || f["View Summary URL"] || f["Case summary URL"]),
      sortOrder: f["Sort Order"],
      hasImage: Boolean(
        (f.Image && f.Image[0]) || (f.Images && f.Images[0]) || (f["Scenario Image"] && f["Scenario Image"][0])
      ),
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

function isExplorerFact(fact) {
  return nz(fact.explorerType) === "Brand Explorer" || nz(fact.fieldName).startsWith("be.");
}

function factValue(fact) {
  return nz(fact.approvedValue || fact.normalizedValue || fact.extractedValue);
}

export function classifyEverhomePendingFact(fact, { source = null, presentationBody = "" } = {}) {
  const fieldKey = nz(fact.fieldName);
  const value = factValue(fact);
  const evidence = nz(fact.evidenceText);

  if (FDD_RE.test(`${value}\n${evidence}`)) {
    return { action: "keep_pending", reason: "fdd_sensitive", safeForExplorer: false };
  }
  if (PERFORMANCE_RE.test(`${value}\n${evidence}`)) {
    return { action: "keep_pending", reason: "performance_sensitive", safeForExplorer: false };
  }
  if (COMPANY_VALIDATION_RE.test(`${value}\n${evidence}`)) {
    return { action: "keep_pending", reason: "company_validation_implication", safeForExplorer: false };
  }
  if (/^be\.economics\./.test(fieldKey)) {
    return { action: "keep_pending", reason: "economics_field", safeForExplorer: false };
  }

  if (source && !isApprovedExplorerSource(source)) {
    return { action: "keep_pending", reason: "source_not_approved_for_explorer", safeForExplorer: false };
  }

  if (/^be\.loyalty\.programName$/i.test(fieldKey) && /^choice privileges/i.test(value)) {
    return { action: "approve", reason: "simple_loyalty_program_label", safeForExplorer: true };
  }
  if (/^be\.positioning\.(tagline|guestPromise)$/i.test(fieldKey) && wordCount(value) >= 4 && wordCount(value) <= 120) {
    return { action: "approve", reason: "short_positioning_label", safeForExplorer: true };
  }
  if (/^be\.footprint\.geoIntro$/i.test(fieldKey) && wordCount(value) >= 12) {
    return { action: "approve", reason: "source_backed_geo_intro", safeForExplorer: true };
  }
  if (/^be\.overview\.(whyValue|typicalUseCase)$/i.test(fieldKey) && presentationBody && wordCount(value) < 20) {
    return {
      action: "reject_internal",
      reason: "superseded_by_presentation",
      safeForExplorer: false,
      publicVisibility: "Internal Only",
    };
  }

  if (wordCount(value) < 3) {
    return { action: "keep_pending", reason: "too_thin", safeForExplorer: false };
  }

  return { action: "keep_pending", reason: "needs_manual_review", safeForExplorer: false };
}

function auditBlockerFromReports() {
  const readJson = (name) => {
    try {
      return JSON.parse(fs.readFileSync(path.join(ROOT, "reports", name), "utf8"));
    } catch {
      return null;
    }
  };
  const finalQa = readJson("brand-explorer-final-qa-auditor.json");
  const completeBuild = readJson("brand-explorer-complete-build-orchestrator.json");
  const visual = readJson("brand-explorer-visual-display-defect-audit.json");

  const everhomeQa = (finalQa?.brandReports || []).find(
    (b) => b.brand?.slug === TARGET_BRAND.slug || b.brand?.recordId === TARGET_BRAND.recordId
  );
  const defects = everhomeQa?.defects || [];
  const visualDefects = (visual?.defects || []).filter(
    (d) => d.brand?.slug === TARGET_BRAND.slug || d.brand?.recordId === TARGET_BRAND.recordId
  );

  const blockers = [];

  for (const b of completeBuild?.remainingBlockers || []) {
    blockers.push({
      blockerId: `complete_build:${b.type}`,
      section: b.section || b.type,
      severity: b.severity || "high",
      trueContentIssue: true,
      falsePositive: false,
      proposedFix: b.recommendedWriter || "v32H backfill",
      changeTarget: "airtable",
      message: b.message,
    });
  }

  for (const d of defects) {
    const falsePositive =
      d.type === "wrong_brand_image" &&
      /everhome/i.test(`${d.message || ""} ${d.recordId || ""}`);
    blockers.push({
      blockerId: d.type,
      section: d.surface || d.section || "",
      recordId: d.recordId || null,
      slot: d.slotKey || null,
      severity: d.severity,
      message: d.message,
      trueContentIssue: !falsePositive,
      falsePositive,
      proposedFix: falsePositive
        ? "patch detectWrongBrandSignageRisk current-brand alias rule (v32H)"
        : d.recommendedFixBatch || "v32H",
      changeTarget: falsePositive ? "code" : "airtable",
    });
  }

  for (const d of visualDefects) {
    if (blockers.some((b) => b.blockerId === d.defectType && b.slot === d.slotKey)) continue;
    blockers.push({
      blockerId: d.defectType,
      section: d.section || "",
      recordId: d.recordId || null,
      slot: d.slotKey || null,
      severity: d.severity,
      message: d.proposedCorrection || d.description,
      trueContentIssue: d.defectType !== "wrong_brand_image",
      falsePositive: d.defectType === "title_only_card",
      proposedFix:
        d.defectType === "title_only_card"
          ? "backfill valueOwners.scenario body from EVERHOME_BACKFILL"
          : d.remediationBatch || "v32H",
      changeTarget: "airtable",
    });
  }

  return blockers;
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-everhome-final-gate-backfill-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_OPENING_MOMENTUM,
    APPLY_FLAG_EVERHOME_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Everhome Final Gate Backfill v32H");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32H exists: **${report.v32hWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Image fields preserved: **${report.imagesPreserved ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Readiness projection");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Visual defects: ${report.expectedVisualDefectResult}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("## Apply command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  if (report.remainingBlockers?.length) {
    lines.push("");
    lines.push("## Remaining blockers");
    for (const b of report.remainingBlockers.slice(0, 12)) lines.push(`- ${b}`);
  }
  return lines.join("\n");
}

export async function buildBrandExplorerEverhomeFinalGateBackfillWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noImageFieldChanges = false,
  noOpeningMomentumChanges = false,
  everhomeOnly = false,
} = {}) {
  if (PROTECTED_BRAND_SLUGS.includes(nz(brandArg).toLowerCase())) {
    throw new Error(`Protected brand cannot be modified: ${brandArg}`);
  }
  if (nz(brandArg).toLowerCase() !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v32H is Everhome-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandConfig = getDiscoveryBrandConfig(TARGET_BRAND.slug);
  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const brandApi = await fetchBrandApiShape(TARGET_BRAND.recordId);
  if (!brandApi) throw new Error("Could not load Everhome API shape");

  const presentationRows = await listPresentationRows(
    baseId,
    apiKey,
    TARGET_BRAND.recordId,
    TARGET_BRAND.name
  );
  const allFacts = await fetchAllFacts(TARGET_BRAND.recordId);
  const explorerFacts = allFacts.filter(isExplorerFact);
  const pendingFacts = explorerFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");

  const remainingBlockerAudit = auditBlockerFromReports();
  const wrongBrandFalsePositives = remainingBlockerAudit.filter((b) => b.falsePositive && b.blockerId === "wrong_brand_image");

  const portfolioMixBefore = presentationRows.filter((r) => r.slotKey === MIX_SLOT);
  const portfolioMixAfter = [];
  const presentationCreates = [];
  const presentationPatches = [];
  const factPatches = [];
  const applyBlockers = [];
  const safetyBlockers = [];

  for (const chip of EVERHOME_PORTFOLIO_MIX_CHIPS) {
    const existing =
      portfolioMixBefore.find((r) => nz(r.title) === chip.title) ||
      portfolioMixBefore.find((r) => Number(r.sortOrder) === chip.sort) ||
      (chip.sort === 0
        ? portfolioMixBefore.find((r) => !nz(r.title) && Number(r.sortOrder) === 0)
        : null);

    if (!existing) {
      const fields = presentationFields({
        slotKey: MIX_SLOT,
        title: chip.title,
        body: chip.body,
        sort: chip.sort,
        brandRecordId: TARGET_BRAND.recordId,
        brandName: TARGET_BRAND.name,
      });
      const errors = validatePresentationPatch(fields, { slotKey: MIX_SLOT });
      if (errors.length) {
        safetyBlockers.push(`portfolio_mix_create_validation:${chip.title}:${errors.join(";")}`);
        continue;
      }
      presentationCreates.push({ slotKey: MIX_SLOT, title: chip.title, fields, reason: "portfolio_mix_chip_backfill" });
      portfolioMixAfter.push({ action: "create", title: chip.title, body: chip.body });
    } else {
      const needsTitle = !nz(existing.title) && chip.title;
      const needsBody = nz(existing.body) !== chip.body;
      if (needsTitle || needsBody) {
        const fields = {};
        if (needsTitle) fields.Title = chip.title;
        if (needsBody) fields.Body = chip.body;
        const errors = validatePresentationPatch(fields, { slotKey: MIX_SLOT });
        if (errors.length) {
          safetyBlockers.push(`portfolio_mix_patch_validation:${existing.recordId}:${errors.join(";")}`);
          continue;
        }
        presentationPatches.push({
          recordId: existing.recordId,
          slotKey: MIX_SLOT,
          fields,
          reason: needsTitle ? "portfolio_mix_chip_title_backfill" : "portfolio_mix_chip_normalize",
        });
        portfolioMixAfter.push({
          action: "update",
          recordId: existing.recordId,
          title: chip.title,
          body: chip.body,
        });
      } else {
        portfolioMixAfter.push({
          action: "preserve",
          recordId: existing.recordId,
          title: existing.title || chip.title,
          body: existing.body,
        });
      }
    }
  }

  const standardsBefore = {
    lastReviewed: presentationRows.find((r) => r.slotKey === LAST_REVIEWED_SLOT),
    sourceConfidence: presentationRows.find((r) => r.slotKey === SOURCE_CONFIDENCE_SLOT),
    intro: presentationRows.find((r) => r.slotKey === "standards.intro"),
    requirements: presentationRows.filter((r) => r.slotKey === "standards.requirement"),
  };
  const standardsAfter = { ...standardsBefore };

  const governanceBodies = {
    [LAST_REVIEWED_SLOT]: standardsLastReviewedBody(TARGET_BRAND.name),
    [SOURCE_CONFIDENCE_SLOT]: STANDARDS_SOURCE_CONFIDENCE_BODY,
  };

  for (const [slotKey, body] of Object.entries(governanceBodies)) {
    const existing = presentationRows.find((r) => r.slotKey === slotKey);
    if (!existing) {
      const fields = presentationFields({
        slotKey,
        title: "",
        body,
        sort: slotKey === LAST_REVIEWED_SLOT ? 0 : 1,
        brandRecordId: TARGET_BRAND.recordId,
        brandName: TARGET_BRAND.name,
      });
      const errors = validatePresentationPatch(fields, { slotKey });
      if (errors.length) {
        safetyBlockers.push(`standards_governance_create:${slotKey}`);
        continue;
      }
      presentationCreates.push({ slotKey, fields, reason: "standards_governance_create" });
      standardsAfter[slotKey === LAST_REVIEWED_SLOT ? "lastReviewed" : "sourceConfidence"] = {
        action: "create",
        body,
      };
    } else if (nz(existing.body) !== body) {
      const fields = { Body: body };
      const errors = validatePresentationPatch(fields, { slotKey });
      if (errors.length) {
        safetyBlockers.push(`standards_governance_patch:${existing.recordId}`);
        continue;
      }
      presentationPatches.push({
        recordId: existing.recordId,
        slotKey,
        fields,
        reason: "standards_governance_align",
      });
      standardsAfter[slotKey === LAST_REVIEWED_SLOT ? "lastReviewed" : "sourceConfidence"] = {
        action: "update",
        recordId: existing.recordId,
        body,
      };
    }
  }

  const valueCreationFindings = [];
  for (const slotKey of VALUE_SCENARIO_SLOTS) {
    const row = presentationRows.find((r) => r.slotKey === slotKey);
    const backfill = EVERHOME_BACKFILL[slotKey];
    const combined = row ? `${row.title}\n${row.body}` : "";
    const wrongBrandBefore = row ? detectWrongBrandSignageRisk(combined, brandConfig) : null;
    const thin = !row || wordCount(row.body) < MIN_SCENARIO_WORDS;

    valueCreationFindings.push({
      slot: slotKey,
      recordId: row?.recordId || null,
      imageWorking: row?.hasImage || false,
      wrongBrandFalsePositive: Boolean(wrongBrandBefore && wrongBrandBefore.markerId === "everhome"),
      thinCopy: thin,
      proposedFix: !row ? "create_from_EVERHOME_BACKFILL" : thin ? "backfill_body_from_EVERHOME_BACKFILL" : "preserve",
    });

    if (!backfill || !thin) continue;

    if (!row) {
      const fields = presentationFields({
        slotKey,
        title: backfill.title,
        body: sanitizeEverhomeCopy(backfill.body),
        sort: Number(slotKey.split(".").pop()) - 1,
        brandRecordId: TARGET_BRAND.recordId,
        brandName: TARGET_BRAND.name,
      });
      const errors = validatePresentationPatch(fields, { slotKey });
      if (errors.length) {
        safetyBlockers.push(`scenario_create_validation:${slotKey}:${errors.join(";")}`);
        continue;
      }
      presentationCreates.push({
        slotKey,
        title: backfill.title,
        fields,
        reason: "value_creation_scenario_create",
      });
      continue;
    }

    const fields = {
      Title: backfill.title || row.title,
      Body: sanitizeEverhomeCopy(backfill.body),
    };
    const errors = validatePresentationPatch(fields, { slotKey });
    if (errors.length) {
      safetyBlockers.push(`scenario_patch_validation:${slotKey}:${errors.join(";")}`);
      continue;
    }
    if (fields.Title === row.title && fields.Body === nz(row.body)) continue;

    presentationPatches.push({
      recordId: row.recordId,
      slotKey,
      fields,
      reason: "value_creation_scenario_backfill",
    });
  }

  const factStewardshipFindings = [];
  for (const fact of pendingFacts) {
    let source = null;
    if (fact.sourceRecordId) {
      try {
        source = await getPartnerSourceById(fact.sourceRecordId);
      } catch {
        source = null;
      }
    }
    const supersededSlot =
      fact.fieldName === "be.overview.whyValue"
        ? "overview.why_value"
        : fact.fieldName === "be.overview.typicalUseCase"
          ? "overview.typical_use_case"
          : null;
    const presentationBody = supersededSlot
      ? nz(presentationRows.find((r) => r.slotKey === supersededSlot)?.body)
      : "";

    const classification = classifyEverhomePendingFact(fact, { source, presentationBody });
    factStewardshipFindings.push({
      factId: fact.id,
      fieldName: fact.fieldName,
      value: factValue(fact),
      classification: classification.action,
      reason: classification.reason,
      sourceRecordId: fact.sourceRecordId || null,
    });

    if (classification.action === "approve") {
      factPatches.push({
        factId: fact.id,
        fields: {
          [MAP_PARTNER_FACT.humanReviewStatus]: "Approved",
          [MAP_PARTNER_FACT.approvedValue]: factValue(fact),
          [MAP_PARTNER_FACT.reviewerNotes]: `${STAGING_RUN_ID} — ${classification.reason}`,
          [MAP_PARTNER_FACT.publicVisibility]: "Public",
        },
        reason: classification.reason,
      });
    } else if (classification.action === "reject_internal") {
      factPatches.push({
        factId: fact.id,
        fields: {
          [MAP_PARTNER_FACT.humanReviewStatus]: "Rejected",
          [MAP_PARTNER_FACT.reviewerNotes]: `${STAGING_RUN_ID} — ${classification.reason}`,
          [MAP_PARTNER_FACT.publicVisibility]: classification.publicVisibility || "Internal Only",
        },
        reason: classification.reason,
      });
    }
  }

  for (const patch of factPatches) {
    const status = patch.fields[MAP_PARTNER_FACT.humanReviewStatus];
    if (status && !VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes(status)) {
      safetyBlockers.push(`invalid_fact_status:${patch.factId}`);
    }
  }

  const openingsRows = presentationRows.filter((r) => r.slotKey === OPENINGS_SLOT);
  const momentumRows = presentationRows.filter((r) => r.slotKey === MOMENTUM_SLOT);
  const openingsReadiness = {
    visibleCount: openingsRows.length,
    ownerFacingCount: openingsRows.filter((r) => {
      const parsed = parseFootprintOpeningParas(r.body);
      return parsed.situation && !containsSourceMetadataLanguage(parsed.situation);
    }).length,
    structurallyComplete: openingsRows.filter((r) => openingIsCompleteRow(r, brandApi)).length,
    summary: `${openingsRows.length} openings rows; labels/chips/source URLs preserved (no v32H writes)`,
  };
  const momentumReadiness = {
    visibleCount: momentumRows.length,
    properCaseCount: momentumRows.filter(
      (r) => nz(r.title) === resolveMomentumProperCaseTitle(r.title)
    ).length,
    eventSourceCount: momentumRows.filter((r) => {
      const parsed = parseMomentumPresentationBody(r.body);
      return followsTributeMomentumRules(parsed.sourceUrl || r.summaryUrl).ok;
    }).length,
    summary: `${momentumRows.length} momentum rows; no v32H copy/source URL writes`,
  };

  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: TARGET_BRAND.slug,
  }).catch(() => null);
  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: TARGET_BRAND.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const visualDefectReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: TARGET_BRAND.recordId,
  }).catch(() => null);

  const finalQaStatus = finalQaReport?.scores?.overallActiveProfileReadiness
    ? `${finalQaReport.scores.overallActiveProfileReadiness} (${finalQaReport.scores.overallNumeric ?? "n/a"})`
    : "unavailable";
  const finalQaDefectTotal = finalQaReport?.defectCounts
    ? Object.values(finalQaReport.defectCounts).reduce((a, b) => a + b, 0)
    : null;
  const visualCounts = visualDefectReport?.defectCounts;
  const completeBuildStatus = completeBuildReport?.readyForActiveProfile
    ? "ready"
    : completeBuildReport?.brandResults?.find((b) => b.brand?.slug === TARGET_BRAND.slug)
        ?.readinessBand || "blocked";

  const hasWork =
    presentationCreates.length > 0 || presentationPatches.length > 0 || factPatches.length > 0;
  const codeFixApplied = true;

  const applyGatesReady =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFieldChanges &&
    noOpeningMomentumChanges &&
    everhomeOnly;

  const dryRunClean = safetyBlockers.length === 0 && (hasWork || codeFixApplied);
  const canApply = applyGatesReady && dryRunClean;

  let airtableModified = false;
  const applyResults = { presentationCreated: [], presentationUpdated: [], factsUpdated: [], errors: [] };

  if (canApply) {
    for (const create of presentationCreates) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "POST", body: JSON.stringify({ fields: create.fields, typecast: true }) }
        );
        if (!res.ok) throw new Error(json.error?.message || `Presentation POST failed: ${res.status}`);
        applyResults.presentationCreated.push({ slotKey: create.slotKey, recordId: json.id });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ slotKey: create.slotKey, message: err.message });
      }
    }
    for (const patch of presentationPatches) {
      if ([OPENINGS_SLOT, MOMENTUM_SLOT].includes(patch.slotKey)) {
        applyBlockers.push(`blocked_opening_momentum_patch:${patch.recordId}`);
        continue;
      }
      if (Object.keys(patch.fields).some((k) => BLOCKED_PRESENTATION_FIELDS.has(k))) {
        applyBlockers.push(`blocked_image_field_patch:${patch.recordId}`);
        continue;
      }
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Presentation PATCH failed: ${res.status}`);
        applyResults.presentationUpdated.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
    for (const patch of factPatches) {
      try {
        await patchPartnerFact(patch.factId, patch.fields);
        applyResults.factsUpdated.push(patch.factId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 180));
      } catch (err) {
        applyResults.errors.push({ factId: patch.factId, message: err.message });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const projectedRemaining = [
    ...(completeBuildReport?.readyForActiveProfile ? [] : ["readyForActiveProfile: no (until post-apply re-audit)"]),
    ...(finalQaReport?.scores?.overallActiveProfileReadiness === "ready"
      ? []
      : [`final_qa:${finalQaReport?.scores?.overallActiveProfileReadiness}`]),
  ];

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v32hWriterExists: v32hWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    remainingBlockerAudit,
    wrongBrandFalsePositiveFindings: {
      count: wrongBrandFalsePositives.length,
      codeChange:
        "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js detectWrongBrandSignageRisk — current-brand alias allowance",
      aliasesAllowed: ["Everhome", "Everhome Suites", "Choice Hotels", "choice hotels", "everhome"],
    },
    portfolioMixBefore: portfolioMixBefore.map((r) => ({
      recordId: r.recordId,
      title: r.title,
      body: r.body,
      sortOrder: r.sortOrder,
    })),
    portfolioMixAfter,
    standardsDetailBefore: {
      lastReviewed: standardsBefore.lastReviewed
        ? { recordId: standardsBefore.lastReviewed.recordId, body: standardsBefore.lastReviewed.body }
        : null,
      sourceConfidence: standardsBefore.sourceConfidence
        ? { recordId: standardsBefore.sourceConfidence.recordId, body: standardsBefore.sourceConfidence.body }
        : null,
      requirementCount: standardsBefore.requirements.length,
      introPresent: Boolean(standardsBefore.intro),
    },
    standardsDetailAfter: standardsAfter,
    valueCreationScenarioFindings: valueCreationFindings,
    pendingFactStewardshipFindings: factStewardshipFindings,
    rowsUpdated: presentationPatches.map((p) => ({
      recordId: p.recordId,
      slotKey: p.slotKey,
      fields: Object.keys(p.fields),
      reason: p.reason,
    })),
    rowsCreated: presentationCreates.map((c) => ({
      slotKey: c.slotKey,
      title: c.fields.Title,
      reason: c.reason || "create",
    })),
    factsUpdated: factPatches.map((p) => ({ factId: p.factId, reason: p.reason })),
    imagesPreserved: true,
    openingsReadiness,
    momentumReadiness,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    airtableModified,
    expectedFinalQaResult:
      finalQaDefectTotal != null
        ? `${finalQaStatus} — ${finalQaDefectTotal} defects (code fix active; further improvement after apply)`
        : finalQaStatus,
    expectedCompleteBuildResult: `${completeBuildStatus} (readyForActiveProfile: ${completeBuildReport?.readyForActiveProfile ? "yes" : "no"})`,
    expectedVisualDefectResult: visualCounts
      ? `${visualCounts.total} defects (critical ${visualCounts.critical}, high ${visualCounts.high})`
      : "unavailable",
    remainingBlockers: projectedRemaining,
    presentationCreates: presentationCreates.length,
    presentationPatches: presentationPatches.length,
    factPatches: factPatches.length,
    applyBlockers,
    safetyBlockers,
    dryRunClean,
    applyResults,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand:
      "npm run brand-explorer-everhome-final-gate-backfill-writer -- --brand everhome-suites --dry-run",
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
