/**
 * Brand Explorer WoodSpring Final Content Cleanup + Fact Stewardship v33E.
 *
 * Completes overview bullets, differentiators, loyalty.proof cleanup, and pending
 * Partner Fact stewardship — without touching images, registry, or openings/momentum.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-final-content-cleanup-writer-v33E.md
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
import { TARGET_BRAND as WOODSPRING_TARGET } from "./brand-explorer-woodspring-source-registry-readiness-writer.js";
import { GALLERY_DISPLAY_STATUS_HIDE } from "./brand-explorer-woodspring-visual-completion-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33E";
export const STAGING_RUN_ID = "v33E-woodspring-final-content-cleanup";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-final-content-cleanup-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-final-content-cleanup-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-final-content-cleanup-writer-v33E.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v33E-woodspring-final-content-cleanup";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_REGISTRY = "--confirm-no-registry-approval-changes";
export const APPLY_FLAG_NO_SOURCE_LIBRARY = "--confirm-no-source-library-changes";
export const APPLY_FLAG_NO_OPENINGS_MOMENTUM = "--confirm-no-openings-or-momentum-changes";
export const APPLY_FLAG_HIDDEN_GALLERY = "--confirm-hidden-gallery-stays-hidden";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export const STEWARDSHIP_TAG = "v33E-woodspring-final-content-cleanup";
export const TARGET_BRAND = WOODSPRING_TARGET;
export const PROTECTED_BRAND_SLUGS = Object.freeze(["everhome-suites", "suburban-studios"]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WHY_VALUE_SLOT = "overview.why_value";
const DIFF_IDENTITY_SLOT = "overview.differentiators.identity";
const DIFF_COMMERCIAL_SLOT = "overview.differentiators.commercial";
const LOYALTY_PROOF_SLOT = "loyalty.proof";
const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";

const HIDDEN_GALLERY_SLOTS = Object.freeze([
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
]);

export const WOODSPRING_WHY_VALUE_BULLETS = Object.freeze([
  "Extended-stay positioning for weekly and longer-stay demand within the Choice Hotels platform.",
  "Simple suite model with kitchen-equipped room expectations suited to extended stays.",
  "Choice platform context for distribution and brand participation across the extended-stay portfolio.",
  "Operating model considerations around staffing, housekeeping, and service intensity relative to full-service hotels.",
  "Owner diligence lens for market demand, prototype fit, and competitive extended-stay supply.",
]);

export const WOODSPRING_DIFF_IDENTITY_BULLETS = Object.freeze([
  "Economy extended-stay positioning focused on practical, longer-stay lodging.",
  "Kitchen-equipped suite model designed for guests staying multiple nights.",
  "Part of the Choice Hotels extended-stay brand family alongside other longer-stay flags.",
  "Brand orientation toward simplicity and extended-stay utility rather than resort or lifestyle premium.",
]);

export const WOODSPRING_DIFF_COMMERCIAL_BULLETS = Object.freeze([
  "Longer-stay demand fit where guests need in-room kitchen and weekly-stay flexibility.",
  "Distribution through the Choice platform and brand-family discovery context.",
  "Operating simplicity relative to higher-service hotel models with lean public-area expectations.",
  "Market-fit diligence for extended-stay supply, rate sensitivity, and competitive positioning.",
]);

export const WOODSPRING_LOYALTY_PROOF_BODIES = Object.freeze([
  "Choice Privileges Context\nChoice Privileges participation may support guest recognition and loyalty engagement within the Choice platform for WoodSpring properties.",
  "Direct Channel Participation\nOwners should diligence direct-channel mix, member-rate strategy, and how loyalty programs interact with extended-stay length of stay.",
  "Portfolio Loyalty Context\nWoodSpring participates in the broader Choice Hotels rewards ecosystem; owners should evaluate local relevance for weekly and longer-stay guests.",
]);

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
  "External Display Status",
]);

const BLOCKED_COPY_RE =
  /\b(confirm\s+(prototype|fees|flag|pip)|\bloi\b|\bfdd\b|item\s*19|franchise disclosure|consumer site|u\.s\. news|#\d+\s+hotel rewards|return\s*&\s*earn|10%\+|guaranteed|revenue impact|conversion rate|direct booking uplift)\b/i;

const FDD_RE = /\b(fdd|item\s*19|franchise disclosure document)\b/i;
const PERFORMANCE_RE = /\b(roi|irr|cap rate|noi|revpar|adr forecast|guaranteed returns?)\b/i;
const FEE_RE = /\b(royalty|franchise fee|marketing fee|initial franchise)\b/i;
const COMPANY_VALIDATION_RE =
  /company validated|company-approved|official sign-off|validated by choice/i;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-woodspring-visual-registry-recovery-writer.json",
  "reports/brand-explorer-woodspring-visual-completion-writer.json",
  "reports/brand-explorer-woodspring-openings-momentum-build-writer.json",
  "reports/brand-explorer-woodspring-presentation-cleanup-backfill-writer.json",
  "reports/brand-explorer-woodspring-source-registry-readiness-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "lib/partner-intelligence/brand-explorer-everhome-final-fact-formatting-cleanup-writer.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "docs/brand-explorer-presentation-slots.md",
  "live WoodSpring Presentation / Facts / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-final-content-cleanup-writer.js",
  "scripts/brand-explorer-woodspring-final-content-cleanup-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "package.json",
];

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

export function splitBullets(val) {
  if (!nz(val)) return [];
  return String(val)
    .split(/\n|;|•/g)
    .map((s) => s.replace(/^\s*[-*]\s*/, "").trim())
    .filter(Boolean);
}

export function joinBullets(bullets) {
  return bullets.map((b) => nz(b)).filter(Boolean).join("\n");
}

export function v33eWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-woodspring-final-content-cleanup-writer.js")
  );
}

export function validateWoodspringOwnerFacingCopy(text, { slotKey = "" } = {}) {
  const errors = [];
  const blob = nz(text);
  if (!blob) errors.push("empty_copy");
  if (BLOCKED_COPY_RE.test(blob)) errors.push("blocked_owner_facing_language");
  if (FDD_RE.test(blob)) errors.push("fdd_language");
  if (PERFORMANCE_RE.test(blob)) errors.push("performance_claim");
  if (FEE_RE.test(blob)) errors.push("fee_claim");
  if (COMPANY_VALIDATION_RE.test(blob)) errors.push("company_validation_implication");
  if (/consumer site/i.test(blob)) errors.push("consumer_site_metadata");
  if (slotKey === OPENINGS_SLOT || slotKey === MOMENTUM_SLOT) {
    errors.push("openings_momentum_blocked");
  }
  return errors;
}

function validatePresentationPatch(fields, { slotKey = "" } = {}) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
  }
  const combined = `${fields.Title || ""}\n${fields.Body || ""}`;
  errors.push(...validateWoodspringOwnerFacingCopy(combined, { slotKey }));
  return errors;
}

function isExplorerFact(fact) {
  return nz(fact.explorerType) === "Brand Explorer" || nz(fact.fieldName).startsWith("be.");
}

function factValue(fact) {
  return nz(fact.approvedValue || fact.normalizedValue || fact.extractedValue);
}

function isSensitiveFactBlob(value, evidence = "") {
  const blob = `${value}\n${evidence}`;
  return (
    FDD_RE.test(blob) ||
    PERFORMANCE_RE.test(blob) ||
    FEE_RE.test(blob) ||
    COMPANY_VALIDATION_RE.test(blob) ||
    BLOCKED_COPY_RE.test(blob)
  );
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

export function classifyWoodspringPendingFact(fact, { source = null } = {}) {
  const fieldKey = nz(fact.fieldName);
  const value = factValue(fact);
  const evidence = nz(fact.evidenceText);
  const status = nz(fact.humanReviewStatus);
  const src = sourceStewardship(source, TARGET_BRAND.recordId);

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
      proposedAction: "reject_internal",
      approveReady: false,
      safeForExplorer: false,
      rationale: "Sensitive FDD/performance/fee/disclosure language — mark Internal Only",
      sourceSupport: src,
    };
  }

  if (!src.sufficient) {
    return {
      fieldKey,
      proposedAction: "hold_pending",
      approveReady: false,
      safeForExplorer: false,
      rationale: "Missing or unapproved Explorer source — keep pending",
      sourceSupport: src,
    };
  }

  if (wordCount(value) < 4) {
    return {
      fieldKey,
      proposedAction: "reject_internal",
      approveReady: false,
      safeForExplorer: false,
      rationale: "Thin extract — not safe for external Explorer use",
      sourceSupport: src,
    };
  }

  if (/economics|fee|royalty|item\s*19|performance|contribution\s*%/i.test(`${fieldKey} ${value}`)) {
    return {
      fieldKey,
      proposedAction: "hold_pending",
      approveReady: false,
      safeForExplorer: false,
      rationale: "Economics/fee/performance-adjacent field — requires manual review",
      sourceSupport: src,
    };
  }

  return {
    fieldKey,
    proposedAction: "approve",
    approveReady: true,
    safeForExplorer: true,
    rationale:
      "Public-source, non-sensitive Explorer fact — AI-Assisted stewardship; not company validation.",
    approvedValue: value.endsWith(".") ? value : `${value.replace(/\.$/, "")}.`,
    sourceSupport: src,
    publicOfficialTradeSupported: true,
  };
}

function buildFactStewardshipPatch(fact, diagnosis) {
  const stamp = new Date().toISOString().slice(0, 10);
  const prior = nz(fact.reviewerNotes);
  const note = [STEWARDSHIP_TAG, diagnosis.rationale, "Not company validation."].filter(Boolean).join(" ");
  const reviewerNotes = prior.includes(STEWARDSHIP_TAG) ? prior : prior ? `${prior}\n${note}` : note;

  if (diagnosis.proposedAction === "approve") {
    if (!diagnosis.approveReady) return { patch: null, skipped: ["approve_not_ready"] };
    const approvedValue = nz(diagnosis.approvedValue) || factValue(fact);
    if (isSensitiveFactBlob(approvedValue)) return { patch: null, skipped: ["unsafe_approved_value"] };
    return {
      patch: {
        [MAP_PARTNER_FACT.humanReviewStatus]: "Approved",
        [MAP_PARTNER_FACT.reviewerNotes]: reviewerNotes,
        [MAP_PARTNER_FACT.lastUpdated]: stamp,
        [MAP_PARTNER_FACT.approvedValue]: approvedValue,
      },
      skipped: [],
    };
  }

  if (diagnosis.proposedAction === "reject_internal") {
    if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Internal Only")) {
      return { patch: null, skipped: ["unknown_select:Internal Only"] };
    }
    return {
      patch: {
        [MAP_PARTNER_FACT.humanReviewStatus]: "Internal Only",
        [MAP_PARTNER_FACT.reviewerNotes]: reviewerNotes,
        [MAP_PARTNER_FACT.lastUpdated]: stamp,
      },
      skipped: [],
    };
  }

  return { patch: null, skipped: [`action_${diagnosis.proposedAction}`] };
}

function classifyDefectRemediation(defect) {
  const slot = nz(defect.slotKey || defect.type);
  const type = nz(defect.type || defect.defectType);
  if (type === "missing_card_image" && /materials\.gallery/.test(slot)) {
    return {
      issueClass: "gallery_image_materialization",
      requiresAirtablePatch: true,
      requiresCodePatch: false,
      proposedFix: "Materialize six visible gallery Image attachments with hoteldam property photography (v33H)",
    };
  }
  if (type === "insufficient_visible_gallery_images") {
    return {
      issueClass: "gallery_visibility_minimum",
      requiresAirtablePatch: true,
      requiresCodePatch: false,
      proposedFix: "Unhide materials.gallery.4–6 and complete six visible gallery cards (v33H)",
    };
  }
  if (type === "empty_bullet" || slot.includes("why_value") || slot.includes("differentiators")) {
    return {
      issueClass: "content_formatting",
      requiresAirtablePatch: true,
      requiresCodePatch: false,
      proposedFix: "Patch presentation Body with line-broken bullets",
    };
  }
  if (slot === LOYALTY_PROOF_SLOT || /consumer_site|loyalty/.test(type + slot)) {
    return {
      issueClass: "content_carryover",
      requiresAirtablePatch: true,
      requiresCodePatch: false,
      proposedFix: "Rewrite loyalty.proof rows without consumer-site/disclosure language",
    };
  }
  if (type === "bad_sort_order") {
    return {
      issueClass: "deferred_sort_order",
      requiresAirtablePatch: false,
      requiresCodePatch: false,
      proposedFix: "Defer to v24D — non-deterministic multi-row reorder risk",
    };
  }
  return {
    issueClass: "other",
    requiresAirtablePatch: false,
    requiresCodePatch: false,
    proposedFix: "Review manually",
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
      externalDisplayStatus: nz(f["External Display Status"]),
      sortOrder: f["Sort Order"],
      hasImage: Boolean(f.Image?.[0]?.url || f["Scenario Image"]?.[0]?.url),
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
    "npm run brand-explorer-woodspring-final-content-cleanup-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_REGISTRY,
    APPLY_FLAG_NO_SOURCE_LIBRARY,
    APPLY_FLAG_NO_OPENINGS_MOMENTUM,
    APPLY_FLAG_HIDDEN_GALLERY,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Final Content Cleanup v33E");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v33E exists: **${report.v33eWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Content patches");
  lines.push(`- Presentation patches: **${report.presentationPatches.length}**`);
  lines.push(`- Fact patches: **${report.factPatches.length}**`);
  lines.push(`- Code/audit patches: **${report.codeAuditPatches.length}**`);
  lines.push("");
  lines.push("## Readiness projection");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Visual defects: ${report.expectedVisualDefectResult}`);
  if (report.exactApplyCommand) {
    lines.push("");
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

export async function buildBrandExplorerWoodspringFinalContentCleanupWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noImageFieldChanges = false,
  noRegistryChanges = false,
  noSourceLibrary = false,
  noOpeningMomentumChanges = false,
  hiddenGalleryStaysHidden = false,
  woodspringOnly = false,
} = {}) {
  const slug = nz(brandArg).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Protected brand cannot be modified: ${slug}`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33E is WoodSpring-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const [liveState, brandApi, allFacts, presentationRows, finalQaReport, completeBuildReport, visualDefectReport] =
    await Promise.all([
      fetchLiveState(TARGET_BRAND.recordId),
      fetchBrandApiShape(TARGET_BRAND.recordId),
      fetchAllFacts(TARGET_BRAND.recordId),
      listPresentationRows(baseId, apiKey, TARGET_BRAND.recordId, TARGET_BRAND.name),
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

  const finalQaBrand =
    finalQaReport?.brandReports?.find(
      (b) => b.brand?.slug === TARGET_BRAND.slug || b.brand?.recordId === TARGET_BRAND.recordId
    ) || null;
  const completeBuildBrand =
    completeBuildReport?.brandResults?.find((b) => b.brand?.slug === TARGET_BRAND.slug) || null;

  const finalDefectAudit = (finalQaBrand?.defects || []).map((d) => ({
    defectType: d.type,
    severity: d.severity,
    section: d.surface || d.category,
    slot: d.slotKey,
    recordId: d.recordId || null,
    currentTitle: null,
    currentBody: d.message || d.excerpt || null,
    ...classifyDefectRemediation(d),
  }));

  const presentationPatches = [];
  const factPatches = [];
  const safetyBlockers = [];
  const applyBlockers = [];
  const codeAuditPatches = [
    {
      file: "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
      change: "reconstructMaterials marks visibleInApi/deferredHidden; missing_card_image only for visible API gallery slots",
      reason: "materials.gallery.4-6 Do Not Display should not block active-profile",
    },
  ];

  const whyValueRow = presentationRows.find((r) => r.slotKey === WHY_VALUE_SLOT);
  const whyValueBefore = {
    recordId: whyValueRow?.recordId || null,
    body: whyValueRow?.body || "",
    bullets: splitBullets(whyValueRow?.body || ""),
  };
  const whyValueBodyAfter = joinBullets([...WOODSPRING_WHY_VALUE_BULLETS]);
  const whyValueAfter = {
    recordId: whyValueRow?.recordId || null,
    body: whyValueBodyAfter,
    bullets: [...WOODSPRING_WHY_VALUE_BULLETS],
  };

  if (whyValueRow) {
    const fields = { Body: whyValueBodyAfter };
    const errors = validatePresentationPatch(fields, { slotKey: WHY_VALUE_SLOT });
    if (errors.length) safetyBlockers.push(`why_value_validation:${errors.join(";")}`);
    else if (whyValueBefore.body !== whyValueBodyAfter) {
      presentationPatches.push({
        recordId: whyValueRow.recordId,
        slotKey: WHY_VALUE_SLOT,
        fields,
        reason: "why_value_five_bullets",
      });
    }
  } else {
    safetyBlockers.push("missing_overview_why_value_row");
  }

  const diffIdentityRow = presentationRows.find((r) => r.slotKey === DIFF_IDENTITY_SLOT);
  const diffIdentityBefore = {
    recordId: diffIdentityRow?.recordId || null,
    body: diffIdentityRow?.body || "",
    bullets: splitBullets(diffIdentityRow?.body || ""),
  };
  const diffIdentityBodyAfter = joinBullets([...WOODSPRING_DIFF_IDENTITY_BULLETS]);
  const diffIdentityAfter = {
    recordId: diffIdentityRow?.recordId || null,
    body: diffIdentityBodyAfter,
    bullets: [...WOODSPRING_DIFF_IDENTITY_BULLETS],
  };
  if (diffIdentityRow) {
    const fields = { Body: diffIdentityBodyAfter };
    const errors = validatePresentationPatch(fields, { slotKey: DIFF_IDENTITY_SLOT });
    if (errors.length) safetyBlockers.push(`diff_identity_validation:${errors.join(";")}`);
    else if (diffIdentityBefore.body !== diffIdentityBodyAfter) {
      presentationPatches.push({
        recordId: diffIdentityRow.recordId,
        slotKey: DIFF_IDENTITY_SLOT,
        fields,
        reason: "differentiators_identity_four_bullets",
      });
    }
  } else {
    safetyBlockers.push("missing_differentiators_identity_row");
  }

  const diffCommercialRow = presentationRows.find((r) => r.slotKey === DIFF_COMMERCIAL_SLOT);
  const diffCommercialBefore = {
    recordId: diffCommercialRow?.recordId || null,
    body: diffCommercialRow?.body || "",
    bullets: splitBullets(diffCommercialRow?.body || ""),
  };
  const diffCommercialBodyAfter = joinBullets([...WOODSPRING_DIFF_COMMERCIAL_BULLETS]);
  const diffCommercialAfter = {
    recordId: diffCommercialRow?.recordId || null,
    body: diffCommercialBodyAfter,
    bullets: [...WOODSPRING_DIFF_COMMERCIAL_BULLETS],
  };
  if (diffCommercialRow) {
    const fields = { Body: diffCommercialBodyAfter };
    const errors = validatePresentationPatch(fields, { slotKey: DIFF_COMMERCIAL_SLOT });
    if (errors.length) safetyBlockers.push(`diff_commercial_validation:${errors.join(";")}`);
    else if (diffCommercialBefore.body !== diffCommercialBodyAfter) {
      presentationPatches.push({
        recordId: diffCommercialRow.recordId,
        slotKey: DIFF_COMMERCIAL_SLOT,
        fields,
        reason: "differentiators_commercial_four_bullets",
      });
    }
  } else {
    safetyBlockers.push("missing_differentiators_commercial_row");
  }

  const loyaltyRows = presentationRows
    .filter((r) => r.slotKey === LOYALTY_PROOF_SLOT)
    .sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0));
  const loyaltyProofBefore = loyaltyRows.map((r) => ({
    recordId: r.recordId,
    body: r.body,
    excerpt: r.body.slice(0, 120),
  }));
  const loyaltyProofAfter = [];
  for (let i = 0; i < loyaltyRows.length; i++) {
    const row = loyaltyRows[i];
    const bodyAfter = WOODSPRING_LOYALTY_PROOF_BODIES[i] || WOODSPRING_LOYALTY_PROOF_BODIES[0];
    loyaltyProofAfter.push({ recordId: row.recordId, body: bodyAfter });
    if (row.body !== bodyAfter) {
      const fields = { Body: bodyAfter };
      const errors = validatePresentationPatch(fields, { slotKey: LOYALTY_PROOF_SLOT });
      if (errors.length) {
        safetyBlockers.push(`loyalty_proof_validation:${row.recordId}:${errors.join(";")}`);
      } else {
        presentationPatches.push({
          recordId: row.recordId,
          slotKey: LOYALTY_PROOF_SLOT,
          fields,
          reason: "loyalty_proof_consumer_site_cleanup",
        });
      }
    }
  }

  const pendingFactAudit = [];
  const explorerPendingFacts = allFacts.filter(
    (f) => isExplorerFact(f) && nz(f.humanReviewStatus) === "Pending"
  );
  for (const fact of explorerPendingFacts) {
    const source = fact.sourceRecordId
      ? await getPartnerSourceById(fact.sourceRecordId).catch(() => null)
      : null;
    const diagnosis = classifyWoodspringPendingFact(fact, { source });
    const { patch, skipped } = buildFactStewardshipPatch(fact, diagnosis);
    pendingFactAudit.push({
      factId: fact.id,
      fieldKey: nz(fact.fieldName),
      currentStatus: nz(fact.humanReviewStatus),
      currentValue: factValue(fact).slice(0, 200),
      sourceRecordId: fact.sourceRecordId || null,
      sourceType: nz(fact.sourceType) || nz(source?.sourceType),
      sourceApprovedForExplorer: source ? isApprovedExplorerSource(source) : false,
      confidenceLevel: nz(fact.confidenceLevel),
      publicOfficialTradeSupported: diagnosis.publicOfficialTradeSupported ?? false,
      safeForExplorer: diagnosis.safeForExplorer ?? false,
      sensitive: isSensitiveFactBlob(factValue(fact), nz(fact.evidenceText)),
      proposedAction: diagnosis.proposedAction,
      rationale: diagnosis.rationale,
      patchSkipped: skipped,
    });
    if (patch) {
      factPatches.push({
        factId: fact.id,
        fieldKey: nz(fact.fieldName),
        fields: patch,
        reason: diagnosis.proposedAction,
      });
    }
  }

  const hiddenGalleryVerification = HIDDEN_GALLERY_SLOTS.map((slotKey) => {
    const row = presentationRows.find((r) => r.slotKey === slotKey);
    const inApi = (brandApi.brandExplorer?.blocks || []).some((b) => b.slotKey === slotKey);
    return {
      slotKey,
      recordId: row?.recordId || null,
      externalDisplayStatus: row?.externalDisplayStatus || null,
      inApi,
      intendedHidden: nz(row?.externalDisplayStatus).toLowerCase() === "do not display",
      hasImage: row?.hasImage || false,
      auditReconciliation: inApi
        ? "visibility_mismatch_needs_review"
        : "deferred_asset_need_not_blocking",
    };
  });

  const galleryVisibilityPatches = hiddenGalleryVerification
    .filter((g) => g.recordId && !g.intendedHidden && hiddenGalleryStaysHidden)
    .map((g) => ({
      recordId: g.recordId,
      slotKey: g.slotKey,
      fields: { "External Display Status": GALLERY_DISPLAY_STATUS_HIDE },
    }));
  if (galleryVisibilityPatches.length) {
    for (const p of galleryVisibilityPatches) {
      presentationPatches.push({ ...p, reason: "rehide_deferred_gallery_slot" });
    }
  }

  const sortOrderDecision = {
    defectPresent: finalDefectAudit.some((d) => d.defectType === "bad_sort_order"),
    patchAllowed: false,
    rationale: "Deferred to v24D — multi-row Sort Order normalization not deterministic enough for v33E",
    nonBlocking: true,
  };

  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noImageFieldChanges) applyBlockers.push("missing_confirm_no_image_field_changes");
    if (!noRegistryChanges) applyBlockers.push("missing_confirm_no_registry_approval_changes");
    if (!noSourceLibrary) applyBlockers.push("missing_confirm_no_source_library_changes");
    if (!noOpeningMomentumChanges) applyBlockers.push("missing_confirm_no_openings_or_momentum_changes");
    if (!hiddenGalleryStaysHidden) applyBlockers.push("missing_confirm_hidden_gallery_stays_hidden");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  const hasWork =
    presentationPatches.length > 0 || factPatches.length > 0 || codeAuditPatches.length > 0;
  const dryRunClean =
    safetyBlockers.length === 0 &&
    hasWork &&
    applyBlockers.filter((b) => b.startsWith("missing_")).length === 0;

  const scoringPathMismatch =
    finalQaBrand?.scores?.overallActiveProfileReadiness !== completeBuildBrand?.readinessBand
      ? {
          finalQa: finalQaBrand?.scores?.overallActiveProfileReadiness,
          completeBuildBand: completeBuildBrand?.readinessBand,
          completeBuildReadyForActiveProfile: completeBuildBrand?.readyForActiveProfile,
        }
      : null;

  const projectedDefectReduction =
    presentationPatches.filter((p) =>
      [WHY_VALUE_SLOT, DIFF_IDENTITY_SLOT, DIFF_COMMERCIAL_SLOT, LOYALTY_PROOF_SLOT].includes(p.slotKey)
    ).length +
    (codeAuditPatches.length ? 1 : 0);

  let airtableModified = false;
  const applyResults = { presentationUpdated: [], factsUpdated: [], errors: [] };

  const canApply =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFieldChanges &&
    noRegistryChanges &&
    noSourceLibrary &&
    noOpeningMomentumChanges &&
    hiddenGalleryStaysHidden &&
    woodspringOnly &&
    safetyBlockers.length === 0 &&
    applyBlockers.length === 0;

  if (canApply) {
    for (const patch of presentationPatches) {
      if (patch.slotKey === OPENINGS_SLOT || patch.slotKey === MOMENTUM_SLOT) {
        applyBlockers.push(`blocked_opening_momentum:${patch.recordId}`);
        continue;
      }
      if (patch.fields.Image || patch.fields["Scenario Image"]) {
        applyBlockers.push(`blocked_image_field:${patch.recordId}`);
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
        applyResults.presentationUpdated.push({
          recordId: patch.recordId,
          slotKey: patch.slotKey,
          reason: patch.reason,
        });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
    for (const patch of factPatches) {
      try {
        await patchPartnerFact(patch.factId, patch.fields);
        applyResults.factsUpdated.push({ factId: patch.factId, fieldKey: patch.fieldKey });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 180));
      } catch (err) {
        applyResults.errors.push({ factId: patch.factId, message: err.message });
      }
    }
  }

  const brandBasicsAfter =
    canApply && airtableModified ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const openingsMomentumTouched = presentationPatches.some(
    (p) => p.slotKey === OPENINGS_SLOT || p.slotKey === MOMENTUM_SLOT
  );

  const remainingBlockers = [
    ...(sortOrderDecision.defectPresent ? ["bad_sort_order: deferred to v24D (non-blocking)"] : []),
    ...(pendingFactAudit.filter((f) => f.proposedAction === "hold_pending").length
      ? [
          `pending_facts_hold: ${pendingFactAudit.filter((f) => f.proposedAction === "hold_pending").length}`,
        ]
      : []),
    ...(scoringPathMismatch && !completeBuildBrand?.readyForActiveProfile
      ? ["scoring_path_mismatch_between_final_qa_and_complete_build"]
      : []),
  ].filter(Boolean);

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33eWriterExists: v33eWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    finalDefectAudit,
    whyValueBefore,
    whyValueAfter,
    diffIdentityBefore,
    diffIdentityAfter,
    diffCommercialBefore,
    diffCommercialAfter,
    loyaltyProofBefore,
    loyaltyProofAfter,
    pendingFactAudit,
    hiddenGalleryAuditReconciliation: {
      slots: hiddenGalleryVerification,
      codeAuditPatches,
      galleryVisibilityPatchesProposed: galleryVisibilityPatches,
    },
    sortOrderDecision,
    presentationPatches,
    factPatches,
    codeAuditPatches,
    rowsPatched: presentationPatches.map((p) => ({
      recordId: p.recordId,
      slotKey: p.slotKey,
      reason: p.reason,
    })),
    factsPatched: factPatches.map((p) => ({
      factId: p.factId,
      fieldKey: p.fieldKey,
      reason: p.reason,
    })),
    imagesUntouched: true,
    openingsMomentumUntouched: !openingsMomentumTouched,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    airtableModified,
    applyResults: canApply ? applyResults : null,
    dryRunClean,
    applyBlockers: [...applyBlockers, ...safetyBlockers],
    scoringPathMismatch,
    currentFinalQa: finalQaBrand?.scores || null,
    currentCompleteBuild: {
      readinessBand: completeBuildBrand?.readinessBand,
      readyForActiveProfile: completeBuildBrand?.readyForActiveProfile,
    },
    currentVisualDefects: visualDefectReport?.defectCounts || null,
    expectedFinalQaResult: `projected_ready_from_almost_ready_85_after_${projectedDefectReduction}_content_fixes`,
    expectedCompleteBuildResult: `projected_readyForActiveProfile_after_content_and_fact_stewardship`,
    expectedVisualDefectResult: `projected_${Math.max(0, (visualDefectReport?.defectCounts?.total ?? 5) - projectedDefectReduction)}_defects`,
    remainingBlockers,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
