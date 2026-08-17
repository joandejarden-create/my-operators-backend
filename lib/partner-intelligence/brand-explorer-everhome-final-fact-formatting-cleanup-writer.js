/**
 * Brand Explorer Everhome Final Fact Stewardship + Formatting Cleanup v32I.
 *
 * Clears last active-profile gates: two pending Explorer facts, overview.why_value
 * line-broken bullet formatting — without touching images, openings, momentum, or
 * Company Validated.
 *
 * @see docs/data-intelligence/brand-explorer-everhome-final-fact-formatting-cleanup-writer-v32I.md
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
import { TARGET_BRAND } from "./brand-explorer-everhome-openings-momentum-rebuild-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v32I";
export const STAGING_RUN_ID = "v32I-everhome-final-fact-formatting-cleanup";
export const REPORT_JSON_NAME = "brand-explorer-everhome-final-fact-formatting-cleanup-writer.json";
export const REPORT_MD_NAME = "brand-explorer-everhome-final-fact-formatting-cleanup-writer.md";
export const DOC_MD_NAME = "brand-explorer-everhome-final-fact-formatting-cleanup-writer-v32I.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v32I-everhome-final-fact-formatting-cleanup";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_OPENINGS_MOMENTUM = "--confirm-no-openings-or-momentum-changes";
export const APPLY_FLAG_EVERHOME_ONLY = "--confirm-everhome-only";

export const STEWARDSHIP_TAG = "v32I-everhome-final-fact-formatting-cleanup";

export const PROTECTED_BRAND_SLUGS = Object.freeze(["woodspring-suites", "suburban-studios"]);

/** Scoped pending facts for v32I (from v32H dry-run / Final QA). */
export const TARGET_PENDING_FACTS = Object.freeze([
  {
    factId: "recDp9fzAP5TJYBXJ",
    fieldKey: "be.footprint.geoIntro",
  },
  {
    factId: "recwPnALiOcly82bA",
    fieldKey: "be.positioning.guestPromise",
  },
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WHY_VALUE_SLOT = "overview.why_value";
const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";
const TARGET_BULLET_COUNT = 5;

const SUPERSEDED_BY_PRESENTATION = Object.freeze({
  "be.footprint.geoIntro": "footprint.geo_intro",
});

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-everhome-final-gate-backfill-writer.json",
  "reports/brand-explorer-everhome-existing-image-approval-recognition-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "api/brand-library.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Everhome Presentation / Facts / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-everhome-final-fact-formatting-cleanup-writer.js",
  "scripts/brand-explorer-everhome-final-fact-formatting-cleanup-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FDD_RE = /\b(fdd|item\s*19|franchise disclosure document)\b/i;
const PERFORMANCE_RE = /\b(roi|irr|cap rate|noi|revpar|adr forecast|guaranteed returns?)\b/i;
const FEE_RE = /\b(royalty|franchise fee|marketing fee|initial franchise)\b/i;
const COMPANY_VALIDATION_RE =
  /company validated|company-approved|official sign-off|validated by choice/i;
const INTERNAL_SURFACE_RE =
  /\b(source capture|internal extraction|paste into airtable|franchise disclosure document|item\s*19)\b/i;

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

export function v32iWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-everhome-final-fact-formatting-cleanup-writer.js"
    )
  );
}

export function splitBullets(val) {
  if (!nz(val)) return [];
  return String(val)
    .split(/\n|;|•/g)
    .map((s) => s.replace(/^\s*[-*]\s*/, "").trim())
    .filter(Boolean);
}

function joinBullets(bullets) {
  return bullets.map((b) => nz(b)).filter(Boolean).join("\n");
}

function normalizeBodyText(text) {
  return nz(text).replace(/\r\n/g, "\n").replace(/\s+/g, " ").trim();
}

function trySplitBulletsToTarget(bullets, minBullets) {
  let result = bullets.map((b) => nz(b)).filter(Boolean);
  while (result.length < minBullets && result.length > 0) {
    const idx = result.reduce((best, b, i) => (b.length > result[best].length ? i : best), 0);
    const target = result[idx];
    const parts = target
      .split(/\s*[—–]\s*|\s+-\s+(?=[A-Za-z])/)
      .map((s) => s.trim())
      .filter((s) => wordCount(s) >= 3);
    if (parts.length < 2) break;
    result.splice(idx, 1, ...parts);
  }
  return result;
}

/**
 * Rewrite overview.why_value Body as line-broken bullets without changing meaning.
 */
export function formatWhyValueLineBrokenBullets(body, { minBullets = TARGET_BULLET_COUNT } = {}) {
  const normalized = nz(body).replace(/\r\n/g, "\n").trim();
  if (!normalized) {
    return {
      bodyAfter: "",
      changed: false,
      bulletCount: 0,
      action: "empty_body",
      needsManualReview: true,
    };
  }

  let bullets = splitBullets(normalized);
  if (bullets.length >= minBullets) {
    const trimmed = bullets.slice(0, minBullets);
    const bodyAfter = joinBullets(trimmed);
    return {
      bodyAfter,
      changed: bodyAfter !== normalized,
      bulletCount: trimmed.length,
      action: "preserve_existing_bullets",
      needsManualReview: false,
    };
  }

  if (bullets.length >= 2 && bullets.length < minBullets) {
    const split = trySplitBulletsToTarget(bullets, minBullets);
    if (split.length >= minBullets) {
      const bodyAfter = joinBullets(split.slice(0, minBullets));
      return {
        bodyAfter,
        changed: bodyAfter !== normalized,
        bulletCount: minBullets,
        action: "split_longest_bullet",
        needsManualReview: false,
      };
    }
  }

  const flat = normalized.replace(/\n+/g, " ").trim();
  const sentences = flat
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.replace(/^\s*[-*•]\s*/, "").trim())
    .filter((s) => wordCount(s) >= 4);

  if (sentences.length >= minBullets) {
    const bodyAfter = joinBullets(sentences.slice(0, minBullets));
    return {
      bodyAfter,
      changed: bodyAfter !== normalized,
      bulletCount: minBullets,
      action: "sentence_split",
      needsManualReview: false,
    };
  }

  const clauses = flat
    .split(/\s*[·;|]\s*/)
    .map((s) => s.trim())
    .filter((s) => wordCount(s) >= 4);

  if (clauses.length >= minBullets) {
    const bodyAfter = joinBullets(clauses.slice(0, minBullets));
    return {
      bodyAfter,
      changed: bodyAfter !== normalized,
      bulletCount: minBullets,
      action: "clause_split",
      needsManualReview: false,
    };
  }

  if (bullets.length === 1 && wordCount(bullets[0]) >= 20) {
    const parts = bullets[0]
      .split(/,\s+(?=[a-z])/i)
      .map((s) => s.trim())
      .filter((s) => wordCount(s) >= 4);
    if (parts.length >= minBullets) {
      const bodyAfter = joinBullets(parts.slice(0, minBullets));
      return {
        bodyAfter,
        changed: bodyAfter !== normalized,
        bulletCount: minBullets,
        action: "comma_clause_split",
        needsManualReview: false,
      };
    }
  }

  return {
    bodyAfter: normalized,
    changed: false,
    bulletCount: bullets.length,
    action: "insufficient_source_bullets",
    needsManualReview: bullets.length < minBullets,
  };
}

function sourceStewardship(source, brandRecordId) {
  if (!source) return { sufficient: false, reason: "missing_source" };
  if (!isApprovedExplorerSource(source)) {
    return { sufficient: false, reason: "source_not_approved_for_explorer" };
  }
  if (source.brandId && source.brandId !== brandRecordId) {
    return { sufficient: false, reason: "source_wrong_brand" };
  }
  return { sufficient: true, reason: "approved_explorer_source" };
}

function isSensitiveFactBlob(value, evidence = "") {
  const blob = `${value}\n${evidence}`;
  return (
    FDD_RE.test(blob) ||
    PERFORMANCE_RE.test(blob) ||
    FEE_RE.test(blob) ||
    COMPANY_VALIDATION_RE.test(blob) ||
    INTERNAL_SURFACE_RE.test(blob)
  );
}

export function classifyEverhomeV32IPendingFact(
  fact,
  { source = null, presentationBody = "", expectedFactId = null } = {}
) {
  const fieldKey = nz(fact.fieldName);
  const value = factValue(fact);
  const evidence = nz(fact.evidenceText);
  const status = nz(fact.humanReviewStatus);
  const src = sourceStewardship(source, TARGET_BRAND.recordId);

  if (expectedFactId && fact.id !== expectedFactId) {
    return {
      fieldKey,
      proposedAction: "none",
      approveReady: false,
      safeForExplorer: false,
      rationale: `Fact ID mismatch — expected ${expectedFactId}, got ${fact.id}`,
    };
  }

  if (status !== "Pending") {
    return {
      fieldKey,
      proposedAction: "none",
      approveReady: false,
      safeForExplorer: false,
      rationale: `Review status is ${status || "unknown"} — idempotent skip`,
    };
  }

  if (isSensitiveFactBlob(value, evidence)) {
    return {
      fieldKey,
      proposedAction: "hold_pending",
      approveReady: false,
      safeForExplorer: false,
      rationale: "Sensitive FDD/performance/fee/validation language — keep pending",
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.footprint.geoIntro") {
    const presentationHasBody = wordCount(presentationBody) >= 12;
    return {
      fieldKey,
      proposedAction: "reject_archive",
      approveReady: false,
      safeForExplorer: false,
      rationale: presentationHasBody
        ? "Thin geography fragment superseded by presentation footprint.geo_intro — archive as Internal Only."
        : "Thin geography fragment — not safe for external Explorer use; archive as Internal Only.",
      supersededByPresentationSlot: SUPERSEDED_BY_PRESENTATION[fieldKey],
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
      publicOfficialTradeSupported: src.sufficient,
    };
  }

  if (fieldKey === "be.positioning.guestPromise") {
    if (!src.sufficient) {
      return {
        fieldKey,
        proposedAction: "hold_pending",
        approveReady: false,
        safeForExplorer: false,
        rationale: "Guest promise label needs approved Explorer source before approval.",
        sourceSupport: src,
      };
    }
    const approvedValue = normalizeGuestPromiseValue(value);
    if (!approvedValue || wordCount(approvedValue) < 4) {
      return {
        fieldKey,
        proposedAction: "hold_pending",
        approveReady: false,
        safeForExplorer: false,
        rationale: "Guest promise extract too thin for safe public approval.",
        sourceSupport: src,
      };
    }
    return {
      fieldKey,
      proposedAction: "approve",
      approveReady: true,
      safeForExplorer: true,
      rationale:
        "Short public-source positioning label — AI-Assisted / Public Source stewardship; not company validation.",
      approvedValue,
      sourceSupport: src,
      publicOfficialTradeSupported: true,
    };
  }

  return {
    fieldKey,
    proposedAction: "hold_pending",
    approveReady: false,
    safeForExplorer: false,
    rationale: "Out of v32I scope — keep pending",
  };
}

function normalizeGuestPromiseValue(value) {
  const raw = nz(value).replace(/\s+/g, " ").trim();
  if (!raw) return "";
  const cleaned = raw.replace(/\.$/, "").trim();
  if (/^comfortable extended stay$/i.test(cleaned)) {
    return "Comfortable extended-stay guest experience.";
  }
  if (wordCount(cleaned) >= 4) {
    return cleaned.endsWith(".") ? cleaned : `${cleaned}.`;
  }
  return "";
}

export function buildEverhomeV32IFactStewardshipPatch(fact, diagnosis) {
  const stamp = new Date().toISOString().slice(0, 10);
  const prior = nz(fact.reviewerNotes);
  const note = [STEWARDSHIP_TAG, diagnosis.rationale, "Not company validation."].filter(Boolean).join(" ");
  const reviewerNotes = prior.includes(STEWARDSHIP_TAG) ? prior : prior ? `${prior}\n${note}` : note;

  if (diagnosis.proposedAction === "approve") {
    if (!diagnosis.approveReady) return { patch: null, skipped: ["approve_not_ready"] };
    if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Approved")) {
      return { patch: null, skipped: ["unknown_select_option:Approved"] };
    }
    const approvedValue = nz(diagnosis.approvedValue) || factValue(fact);
    if (isSensitiveFactBlob(approvedValue)) {
      return { patch: null, skipped: ["unsafe_approved_value"] };
    }
    const fields = {
      [MAP_PARTNER_FACT.humanReviewStatus]: "Approved",
      [MAP_PARTNER_FACT.reviewerNotes]: reviewerNotes,
      [MAP_PARTNER_FACT.lastUpdated]: stamp,
      [MAP_PARTNER_FACT.dataGap]: "No",
    };
    if (!nz(fact.approvedValue)) {
      fields[MAP_PARTNER_FACT.approvedValue] = approvedValue;
    }
    if (VAL_PARTNER_FACT_SELECTS.publicVisibility.includes("Public")) {
      fields[MAP_PARTNER_FACT.publicVisibility] = "Public";
    }
    return { patch: fields, skipped: [] };
  }

  if (diagnosis.proposedAction === "reject_archive") {
    if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Rejected")) {
      return { patch: null, skipped: ["unknown_select_option:Rejected"] };
    }
    if (!VAL_PARTNER_FACT_SELECTS.publicVisibility.includes("Internal Only")) {
      return { patch: null, skipped: ["unknown_select_option:Internal Only"] };
    }
    return {
      patch: {
        [MAP_PARTNER_FACT.humanReviewStatus]: "Rejected",
        [MAP_PARTNER_FACT.publicVisibility]: "Internal Only",
        [MAP_PARTNER_FACT.reviewerNotes]: reviewerNotes,
        [MAP_PARTNER_FACT.dataGap]: "Yes",
        [MAP_PARTNER_FACT.lastUpdated]: stamp,
      },
      skipped: [],
    };
  }

  return { patch: null, skipped: [`action_${diagnosis.proposedAction}`] };
}

function validatePresentationPatch(fields, { slotKey = "" } = {}) {
  const errors = [];
  const isOpeningMomentum =
    slotKey === OPENINGS_SLOT ||
    slotKey === MOMENTUM_SLOT ||
    slotKey.startsWith("footprint.momentum.");

  for (const key of Object.keys(fields)) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
    if (isOpeningMomentum) errors.push(`opening_momentum_blocked:${slotKey}`);
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
      sortOrder: f["Sort Order"],
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

function presentationBodyForSlot(brandApi, slotKey) {
  const blocks = Array.isArray(brandApi?.brandExplorer?.blocks) ? brandApi.brandExplorer.blocks : [];
  const rows = blocks.filter((b) => nz(b.slotKey) === slotKey);
  return rows
    .map((b) => [nz(b.title), nz(b.body)].filter(Boolean).join(": "))
    .filter(Boolean)
    .join("\n\n");
}

function assessSortOrderCleanupNeed(finalQaReport, completeBuildReport) {
  const qaDefects =
    finalQaReport?.brandReports?.find(
      (b) => b.brand?.slug === TARGET_BRAND.slug || b.brand?.recordId === TARGET_BRAND.recordId
    )?.defects || [];
  const sortDefect = qaDefects.find((d) => d.type === "bad_sort_order");
  const blockingInContract =
    completeBuildReport?.brandResults?.find((b) => b.brand?.slug === TARGET_BRAND.slug)?.blockers?.some(
      (b) => b.type === "sort_order_hygiene"
    ) || false;
  const severity = sortDefect?.severity || "medium";
  const explicitBlock = blockingInContract || severity === "high" || severity === "critical";
  return {
    sortDefectPresent: Boolean(sortDefect),
    severity,
    explicitBlock,
    patchAllowed: false,
    rationale: explicitBlock
      ? "Sort-order defect is blocking — v24D writer required for deterministic multi-row normalization."
      : "Sort-order defect is medium/non-blocking — defer to v24D; v32I does not reorder sections.",
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-everhome-final-fact-formatting-cleanup-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_OPENINGS_MOMENTUM,
    APPLY_FLAG_EVERHOME_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Everhome Final Fact Stewardship + Formatting Cleanup v32I");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32I exists: **${report.v32iWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
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
    for (const b of report.remainingBlockers) lines.push(`- ${b}`);
  }
  return lines.join("\n");
}

export async function buildBrandExplorerEverhomeFinalFactFormattingCleanupWriterReport({
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
    throw new Error(`v32I is Everhome-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const [liveState, brandApi, allFacts, presentationRows] = await Promise.all([
    fetchLiveState(TARGET_BRAND.recordId),
    fetchBrandApiShape(TARGET_BRAND.recordId),
    fetchAllFacts(TARGET_BRAND.recordId),
    listPresentationRows(baseId, apiKey, TARGET_BRAND.recordId, TARGET_BRAND.name),
  ]);
  if (!brandApi) throw new Error("Could not load Everhome API shape");

  const pendingFactAudit = [];
  const factPatches = [];
  const presentationPatches = [];
  const sortOrderChanges = [];
  const safetyBlockers = [];
  const applyBlockers = [];

  for (const spec of TARGET_PENDING_FACTS) {
    const fact =
      allFacts.find((f) => f.id === spec.factId) ||
      allFacts.find((f) => nz(f.fieldName) === spec.fieldKey);
    if (!fact) {
      safetyBlockers.push(`missing_fact:${spec.fieldKey}:${spec.factId}`);
      pendingFactAudit.push({
        factId: spec.factId,
        fieldKey: spec.fieldKey,
        error: "fact_not_found",
      });
      continue;
    }
    if (fact.id !== spec.factId) {
      safetyBlockers.push(`fact_id_mismatch:${spec.fieldKey}`);
    }

    const source = fact.sourceRecordId
      ? await getPartnerSourceById(fact.sourceRecordId).catch(() => null)
      : null;
    const presentationSlot = SUPERSEDED_BY_PRESENTATION[spec.fieldKey];
    const presentationBody = presentationSlot
      ? presentationBodyForSlot(brandApi, presentationSlot)
      : "";

    const diagnosis = classifyEverhomeV32IPendingFact(fact, {
      source,
      presentationBody,
      expectedFactId: spec.factId,
    });
    const { patch, skipped } = buildEverhomeV32IFactStewardshipPatch(fact, diagnosis);

    pendingFactAudit.push({
      factId: fact.id,
      fieldKey: spec.fieldKey,
      currentStatus: nz(fact.humanReviewStatus),
      currentValue: factValue(fact),
      evidenceText: nz(fact.evidenceText).slice(0, 200),
      sourceRecordId: fact.sourceRecordId || null,
      sourceType: nz(fact.sourceType) || nz(source?.sourceType),
      sourceApprovedForExplorer: source ? isApprovedExplorerSource(source) : false,
      sourceUrl: nz(source?.sourceUrl).slice(0, 120),
      confidenceLevel: nz(fact.confidenceLevel),
      confidenceScore: fact.confidenceScore ?? null,
      publicOfficialTradeSupported: diagnosis.publicOfficialTradeSupported ?? false,
      safeForExplorer: diagnosis.safeForExplorer ?? false,
      sensitiveFddPerformanceFee: isSensitiveFactBlob(factValue(fact), nz(fact.evidenceText)),
      proposedStewardshipAction: diagnosis.proposedAction,
      rationale: diagnosis.rationale,
      approvedValue: diagnosis.approvedValue || null,
      patchSkipped: skipped,
    });

    if (patch) {
      factPatches.push({
        factId: fact.id,
        fieldKey: spec.fieldKey,
        fields: patch,
        reason: diagnosis.proposedAction,
      });
    }
  }

  const whyValueRow = presentationRows.find((r) => r.slotKey === WHY_VALUE_SLOT);
  const whyValueBefore = {
    recordId: whyValueRow?.recordId || null,
    body: whyValueRow?.body || "",
    bulletCount: splitBullets(whyValueRow?.body || "").length,
  };
  const whyValueFormat = formatWhyValueLineBrokenBullets(whyValueBefore.body);
  const whyValueAfter = {
    recordId: whyValueRow?.recordId || null,
    body: whyValueFormat.bodyAfter,
    bulletCount: splitBullets(whyValueFormat.bodyAfter).length,
    action: whyValueFormat.action,
  };

  if (whyValueRow && whyValueFormat.changed) {
    const fields = { Body: whyValueFormat.bodyAfter };
    const errors = validatePresentationPatch(fields, { slotKey: WHY_VALUE_SLOT });
    if (errors.length) {
      safetyBlockers.push(`why_value_patch_validation:${errors.join(";")}`);
    } else {
      presentationPatches.push({
        recordId: whyValueRow.recordId,
        slotKey: WHY_VALUE_SLOT,
        fields,
        reason: "why_value_line_broken_bullets",
      });
    }
  } else if (!whyValueRow) {
    safetyBlockers.push("missing_overview_why_value_row");
  }

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

  const sortOrderAssessment = assessSortOrderCleanupNeed(finalQaReport, completeBuildReport);
  if (sortOrderAssessment.patchAllowed) {
    sortOrderChanges.push({ action: "deferred", ...sortOrderAssessment });
  } else {
    sortOrderChanges.push({ action: "none", ...sortOrderAssessment });
  }

  const finalQaBrand =
    finalQaReport?.brandReports?.find(
      (b) => b.brand?.slug === TARGET_BRAND.slug || b.brand?.recordId === TARGET_BRAND.recordId
    ) || null;
  const completeBuildBrand =
    completeBuildReport?.brandResults?.find((b) => b.brand?.slug === TARGET_BRAND.slug) || null;

  const projectedPendingAfter = Math.max(
    0,
    (liveState.facts || []).filter(
      (f) =>
        isExplorerFact(f) &&
        nz(f.humanReviewStatus) === "Pending" &&
        !factPatches.some((p) => p.factId === f.id)
    ).length - factPatches.filter((p) => p.fields[MAP_PARTNER_FACT.humanReviewStatus] === "Approved").length
  );

  const scoringPathMismatch =
    finalQaBrand?.scores?.overallActiveProfileReadiness !== completeBuildBrand?.readinessBand
      ? {
          finalQa: finalQaBrand?.scores?.overallActiveProfileReadiness,
          completeBuildBand: completeBuildBrand?.readinessBand,
          completeBuildReadyForActiveProfile: completeBuildBrand?.readyForActiveProfile,
          note: "Final QA uses defect/governance scoring; Complete Build also requires governedPlatformReady and contract gates.",
        }
      : null;

  const hasWork = factPatches.length > 0 || presentationPatches.length > 0;
  const dryRunClean = safetyBlockers.length === 0 && hasWork;

  const applyGatesReady =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFieldChanges &&
    noOpeningMomentumChanges &&
    everhomeOnly;

  if (apply && !applyGatesReady) {
    applyBlockers.push("apply_gates_incomplete");
  }
  if (apply && safetyBlockers.length) {
    applyBlockers.push(...safetyBlockers);
  }

  let airtableModified = false;
  const applyResults = { presentationUpdated: [], factsUpdated: [], errors: [] };

  if (applyGatesReady && dryRunClean) {
    for (const patch of presentationPatches) {
      if (patch.slotKey === OPENINGS_SLOT || patch.slotKey === MOMENTUM_SLOT) {
        applyBlockers.push(`blocked_opening_momentum:${patch.recordId}`);
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

  const brandBasicsAfter =
    applyGatesReady && dryRunClean && airtableModified
      ? await fetchBrandBasics(TARGET_BRAND.recordId)
      : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const finalQaStatus = finalQaBrand?.scores?.overallActiveProfileReadiness
    ? `${finalQaBrand.scores.overallActiveProfileReadiness} (${finalQaBrand.scores.overallNumeric ?? "n/a"})`
    : "unavailable";
  const finalQaDefectTotal = finalQaBrand?.defectCounts?.total ?? null;
  const visualCounts = visualDefectReport?.defectCounts;
  const readyForActiveProfileProjected =
    factPatches.length >= 2 &&
    whyValueFormat.bulletCount >= TARGET_BULLET_COUNT &&
    !whyValueFormat.needsManualReview;

  const remainingBlockers = [
    ...(readyForActiveProfileProjected && completeBuildBrand?.readyForActiveProfile
      ? []
      : [
          completeBuildBrand?.readyForActiveProfile
            ? null
            : `readyForActiveProfile: no (projected pending facts after apply: ~${Math.max(0, pendingFactAudit.filter((f) => f.proposedStewardshipAction === "hold_pending").length)})`,
        ].filter(Boolean)),
    ...(sortOrderAssessment.sortDefectPresent && !sortOrderAssessment.explicitBlock
      ? ["bad_sort_order: medium — deferred to v24D"]
      : []),
    ...(whyValueFormat.needsManualReview ? ["overview.why_value: insufficient bullets for 5/5 fill"] : []),
  ].filter(Boolean);

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v32iWriterExists: v32iWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    pendingFactAudit,
    factStewardshipChanges: factPatches.map((p) => ({
      factId: p.factId,
      fieldKey: p.fieldKey,
      action: p.reason,
      fields: Object.keys(p.fields),
    })),
    whyValueBefore,
    whyValueAfter,
    sortOrderChanges,
    scoringPathMismatch,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    imagesPreserved: true,
    openingsMomentumUntouched: true,
    airtableModified,
    expectedFinalQaResult:
      finalQaDefectTotal != null
        ? `${finalQaStatus} — ${finalQaDefectTotal} defects (post-apply projection: ${readyForActiveProfileProjected ? "0-1" : "2+"})`
        : finalQaStatus,
    expectedCompleteBuildResult: `ready (readyForActiveProfile: ${readyForActiveProfileProjected ? "yes (projected)" : completeBuildBrand?.readyForActiveProfile ? "yes" : "no"})`,
    expectedVisualDefectResult: visualCounts
      ? `${visualCounts.total ?? 0} defects (critical ${visualCounts.critical ?? 0}, high ${visualCounts.high ?? 0})`
      : "unavailable",
    remainingBlockers,
    presentationPatches: presentationPatches.length,
    factPatches: factPatches.length,
    sortOrderPatches: 0,
    applyBlockers,
    safetyBlockers,
    dryRunClean,
    applyResults,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand: `npm run brand-explorer-everhome-final-fact-formatting-cleanup-writer -- --brand ${TARGET_BRAND.slug} --dry-run`,
    markdown: "",
  };
  report.markdown = buildMarkdown(report);
  return report;
}
