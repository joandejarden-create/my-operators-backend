/**
 * Brand Explorer Slot Completion Writer v20B.
 *
 * Gated writer for v20A-approved Batch 1 presentation slots only.
 * Creates/updates Brand Setup - Brand Explorer Presentation rows — never images,
 * Brand Basics, sourceLinks, Company Validated, or excluded slot families.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const WRITER_VERSION = "20B";
export const REPORT_JSON_NAME = "brand-explorer-slot-completion-writer.json";
export const REPORT_MD_NAME = "brand-explorer-slot-completion-writer.md";
export const DOC_MD_NAME = "brand-explorer-slot-completion-writer-v20B.md";
export const RECONCILIATION_JSON_NAME = "brand-explorer-slot-completion-reconciliation.json";
export const RECONCILIATION_MD_NAME = "brand-explorer-slot-completion-reconciliation.md";
export const REMAINING_PLAN_JSON_NAME = "brand-explorer-slot-completion-remaining-plan.json";
export const REMAINING_PLAN_MD_NAME = "brand-explorer-slot-completion-remaining-plan.md";
export const V20A_REPORT_PATH = "reports/brand-explorer-slot-completion-review-package.json";
export const COVERAGE_AUDIT_PATH = "reports/brand-explorer-presentation-slot-coverage-audit.json";
export const MANIFEST_PATH = "reports/brand-explorer-slot-standard-manifest.json";
export const REQUIRED_APPLY_FLAG = "--approve-brand-explorer-slot-completion-v20B";

const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const DEFAULT_BRAND_NAME = "Tribute Portfolio";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const PRESENTATION_WRITE_FIELDS = {
  slotKey: "Slot Key",
  title: "Title",
  body: "Body",
  brand: "Brand",
  brandName: "Brand Name",
  active: "Active",
  sortOrder: "Sort Order",
};

const APPLY_BLOCKLIST_KEYS = new Set([
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
  "materials.caseStudy",
  "footprint.momentum",
  "footprint.openings",
  "overview.proof_operator",
  "standards.last_reviewed",
  "standards.requirement",
]);

const APPLY_BLOCKLIST_PATTERNS = [
  /^economics\./i,
  /^overview\.proof\./i,
  /^materials\.gallery\./i,
  /openings/i,
];

const FORBIDDEN_COPY_PATTERNS = [
  /profile caveats/i,
  /company-validated/i,
  /marriott-validated/i,
  /Illustrative mechanics only/i,
  /(Radisson Blu by Choice|Kimpton Hotels|Curio Collection by Hilton):/i,
];

const TAB_ORDER = [
  "Commercial Engine",
  "Operating Model",
  "Value to Owners",
  "Loyalty Program",
  "Footprint & Growth",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeBodyText(v) {
  return nz(v)
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function normalizeTitle(v) {
  return nz(v);
}

function normalizeActive(v) {
  if (v === true || v === "true" || v === 1 || v === "1" || v === "Yes" || v === "yes") return true;
  if (v === false || v === "false" || v === 0 || v === "0" || v === "No" || v === "no") return false;
  return Boolean(v);
}

function normalizeSortOrder(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeBrandIds(brandField) {
  if (!brandField) return [];
  const arr = Array.isArray(brandField) ? brandField : [brandField];
  return arr.map((x) => String(x).trim()).filter(Boolean).sort();
}

function arraysEqual(a, b) {
  if (a.length !== b.length) return false;
  return a.every((val, idx) => val === b[idx]);
}

function short(v, max = 280) {
  const s = nz(v).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

function escapeFormulaValue(v) {
  return String(v).replace(/'/g, "\\'");
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return DEFAULT_BRAND_ID;
  }
  return nz(raw) || DEFAULT_BRAND_ID;
}

function readJsonFromRepo(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

function isApplyBlockedSlot(slotKey) {
  const key = nz(slotKey);
  if (APPLY_BLOCKLIST_KEYS.has(key)) return true;
  return APPLY_BLOCKLIST_PATTERNS.some((rx) => rx.test(key));
}

function detectCriticalWordingRisk(slotKey, title, body) {
  const combined = `${title}\n${body}`;
  const risks = [];
  for (const rx of FORBIDDEN_COPY_PATTERNS) {
    if (rx.test(combined)) risks.push(`forbidden pattern: ${rx}`);
  }
  if (/Selective presence/i.test(body)) risks.push("deprecated selective-presence template");
  if (/helps owners lift/i.test(body)) risks.push("generic uplift framing");
  if (/\bguarantee\b/i.test(body) && !/not a guarantee/i.test(body)) {
    risks.push("guarantee language");
  }
  return risks;
}

function scoreFromPresentKeys(presentCount, totalRequiredKeys) {
  const total = totalRequiredKeys || 110;
  return Math.max(0, Math.round((presentCount / total) * 80));
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

async function fetchAirtableTableSchemas(baseId, apiKey) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Schema fetch failed: ${res.status}`);
  const byName = new Map((json.tables || []).map((t) => [t.name, t]));
  return { byName };
}

function normalizePresentationRows(records) {
  return (records || [])
    .map((rec) => {
      const f = rec.fields || {};
      const brandField = f[PRESENTATION_WRITE_FIELDS.brand] || f.Brand || [];
      return {
        recordId: rec.id,
        slotKey: nz(f[PRESENTATION_WRITE_FIELDS.slotKey] || f.slot_key),
        title: nz(f[PRESENTATION_WRITE_FIELDS.title]),
        body: nz(f[PRESENTATION_WRITE_FIELDS.body]),
        brandIds: normalizeBrandIds(brandField),
        brandName: nz(f[PRESENTATION_WRITE_FIELDS.brandName]),
        active: f[PRESENTATION_WRITE_FIELDS.active],
        sortOrder: f[PRESENTATION_WRITE_FIELDS.sortOrder],
        imageAttachmentCount: Array.isArray(f.Image) ? f.Image.length : 0,
      };
    })
    .filter((r) => r.slotKey);
}

function buildProposedRowState(slotKey, review, brandRecordId, brandName, sortOrder) {
  const proposedTitle = nz(review.proposedTitle);
  const proposedBody = nz(review.proposedBody);
  return {
    slotKey,
    title: proposedTitle,
    body: proposedBody,
    brandIds: [brandRecordId],
    brandName,
    active: true,
    sortOrder,
    titleOwnedByV20B: Boolean(proposedTitle),
  };
}

function compareLiveToProposed(live, proposed) {
  const differingFields = [];
  const integrityIssues = [];

  if (normalizeTitle(live.slotKey) !== normalizeTitle(proposed.slotKey)) {
    integrityIssues.push("Slot Key");
  }

  const liveBrandIds = live.brandIds || [];
  if (!arraysEqual(liveBrandIds, proposed.brandIds)) {
    differingFields.push("Brand");
    if (!liveBrandIds.includes(proposed.brandIds[0])) {
      integrityIssues.push("Brand");
    }
  }

  if (normalizeTitle(live.brandName) !== normalizeTitle(proposed.brandName)) {
    differingFields.push("Brand Name");
  }

  if (normalizeBodyText(live.body) !== normalizeBodyText(proposed.body)) {
    differingFields.push("Body");
  }

  if (normalizeActive(live.active) !== normalizeActive(proposed.active)) {
    differingFields.push("Active");
  }

  if (normalizeSortOrder(live.sortOrder) !== normalizeSortOrder(proposed.sortOrder)) {
    differingFields.push("Sort Order");
  }

  if (proposed.titleOwnedByV20B) {
    if (normalizeTitle(live.title) !== normalizeTitle(proposed.title)) {
      differingFields.push("Title");
    }
  } else if (normalizeTitle(live.title)) {
    integrityIssues.push("Title (live-only, not v20B-owned)");
  }

  let status;
  if (integrityIssues.length > 0) {
    status = "unexpected_difference";
  } else if (differingFields.length === 0) {
    status = "matched";
  } else {
    status = "would_update";
  }

  return { status, differingFields, integrityIssues };
}

function classifyRemainingSlot(slotKey, manifestRow, blankSlots) {
  if (blankSlots.has(slotKey) || manifestRow?.classification === "not_applicable_to_tribute") {
    return "intentionally_blank";
  }
  if (
    manifestRow?.classification === "media_optional" ||
    /^materials\.gallery\./i.test(slotKey) ||
    /^overview\.scenario\.[1-3]$/i.test(slotKey) ||
    slotKey === "overview.hero"
  ) {
    return "media_required";
  }
  if (
    manifestRow?.classification === "requires_source_evidence" ||
    manifestRow?.classification === "source_material" ||
    manifestRow?.needsSourceBackedEvidence ||
    /^economics\./i.test(slotKey) ||
    ["loyalty.earn", "loyalty.redeem", "loyalty.elite", "loyalty.proof", "materials.caseStudy", "standards.last_reviewed", "standards.requirement", "overview.proof_operator"].includes(slotKey) ||
    /^overview\.proof\./i.test(slotKey) ||
    /^loyalty\.kpi\./i.test(slotKey)
  ) {
    return "evidence_required";
  }
  if (
    manifestRow?.classification === "soft_brand_required" ||
    manifestRow?.classification === "tab_required" ||
    manifestRow?.classification === "core_required" ||
    manifestRow?.requiredForCompletedBrandParity
  ) {
    return "manual_review_required";
  }
  if (
    manifestRow?.classification === "candidate_for_tribute_completion" ||
    manifestRow?.aiDraftHumanReviewAcceptable
  ) {
    return "safe_editorial_future_batch";
  }
  return "manual_review_required";
}

function buildRemainingSlotPlan() {
  const coverage = readJsonFromRepo(COVERAGE_AUDIT_PATH);
  const manifest = readJsonFromRepo(MANIFEST_PATH);
  const blankSlots = new Set(manifest?.slotsShouldRemainBlankForTribute || []);
  const manifestBySlot = new Map((manifest?.slotStandardManifestRows || []).map((r) => [r.slotKey, r]));

  const missingSlots = (coverage?.tributeSlotKeysMissing || []).slice();
  const grouped = {
    evidence_required: [],
    media_required: [],
    manual_review_required: [],
    safe_editorial_future_batch: [],
    intentionally_blank: [],
  };

  for (const slotKey of missingSlots) {
    const bucket = classifyRemainingSlot(slotKey, manifestBySlot.get(slotKey), blankSlots);
    grouped[bucket].push(slotKey);
  }

  return {
    generatedAt: new Date().toISOString(),
    sourceReports: [COVERAGE_AUDIT_PATH, MANIFEST_PATH],
    slotCoverageScore: coverage?.slotCoverageScore ?? null,
    manifestScore: manifest?.revisedRealisticTributeCompletionScore ?? null,
    requiredSlotsStillMissing: manifest?.requiredSlotsTributeMissing || [],
    requiredSlotsStillMissingCount: (manifest?.requiredSlotsTributeMissing || []).length,
    tributeSlotKeysMissingCount: missingSlots.length,
    remainingSlotsGrouped: grouped,
    remainingSlotsGroupedCounts: Object.fromEntries(
      Object.entries(grouped).map(([k, v]) => [k, v.length])
    ),
    v21Recommendation: buildV21Recommendation(grouped, manifest),
  };
}

function buildV21Recommendation(grouped, manifest) {
  const requiredMissing = manifest?.requiredSlotsTributeMissing || [];
  const editorialSafe = grouped.safe_editorial_future_batch.length;
  const evidenceCount = grouped.evidence_required.length;
  const mediaCount = grouped.media_required.length;

  if (requiredMissing.some((k) => /^overview\.(bestAt|differentiators|why_value|owner_experience)/.test(k) || k === "insight.similar" || k === "hero.benefit_zones")) {
    return {
      target: "remaining_editorial_slots",
      rationale:
        "Post-v20B, the highest-leverage gap is Overview / soft-brand editorial slots (bestAt, differentiators, why_value, insight.similar) that are safe for AI draft + human review and do not require source evidence or media.",
      priorityOrder: ["safe_editorial_future_batch", "manual_review_required", "evidence_required", "media_required"],
    };
  }
  if (editorialSafe >= 10) {
    return {
      target: "remaining_editorial_slots",
      rationale: `${editorialSafe} missing slots are classified safe for a future editorial batch before tackling evidence-heavy economics/loyalty mechanics.`,
      priorityOrder: ["safe_editorial_future_batch", "manual_review_required", "evidence_required", "media_required"],
    };
  }
  if (evidenceCount > mediaCount) {
    return {
      target: "evidence_required_slots",
      rationale: `${evidenceCount} missing slots require approved source evidence (economics, loyalty mechanics, proof blocks) before any writer pass.`,
      priorityOrder: ["evidence_required", "manual_review_required", "safe_editorial_future_batch", "media_required"],
    };
  }
  return {
    target: "media_required_slots",
    rationale: `${mediaCount} missing slots are media/image-dependent; pair with approved asset pipeline before writer.`,
    priorityOrder: ["media_required", "safe_editorial_future_batch", "manual_review_required", "evidence_required"],
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
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function assertExternalCopy(text, slotKey) {
  const risks = detectCriticalWordingRisk(slotKey, "", text);
  if (risks.length) {
    throw new Error(`Copy guardrail violated for ${slotKey}: ${risks.join("; ")}`);
  }
}

function loadV20AReport() {
  const report = readJsonFromRepo(V20A_REPORT_PATH);
  if (!report) {
    throw new Error(`Missing v20A review package: ${V20A_REPORT_PATH}. Run brand-explorer-slot-completion-review-package first.`);
  }
  return report;
}

function tabFromSlot(slotKey) {
  if (slotKey.startsWith("commercial.")) return "Commercial Engine";
  if (slotKey.startsWith("operations.")) return "Operating Model";
  if (slotKey.startsWith("valueOwners.")) return "Value to Owners";
  if (slotKey.startsWith("loyalty.")) return "Loyalty Program";
  if (slotKey.startsWith("footprint.")) return "Footprint & Growth";
  return "Unknown";
}

export async function buildBrandExplorerSlotCompletionWriterReport(options = {}) {
  const brandIdOrName = normalizeBrandInput(options.brandIdOrName);
  const apply = Boolean(options.apply);
  const applyApproved = Boolean(options.applyApproved);
  const applyMode = apply && applyApproved;

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const v20a = loadV20AReport();
  if (!v20a.v20BWriterSafeToBuild) {
    throw new Error("v20A review package reports v20B writer is not safe to build — regenerate review package.");
  }
  if (!v20a.batch1CriticalWordingRisksClear) {
    throw new Error("v20A batch1CriticalWordingRisksClear is false — apply blocked.");
  }
  if (v20a.proposedCopyReferenceBrandLanguagePresent) {
    throw new Error("v20A proposed copy still contains reference-brand language — apply blocked.");
  }

  const targetSlotKeys = (v20a.v20BApplyBatchAfterReview || []).slice();
  const reviewRowsBySlot = new Map(
    (v20a.reviewPackageRows || []).map((r) => [r.slotKey, r])
  );

  const brandRecordId = v20a.brand?.recordId || brandIdOrName || DEFAULT_BRAND_ID;
  const brandName = v20a.brand?.name || DEFAULT_BRAND_NAME;

  const excludedSlotsExplicit = [];
  const leakedExcludedSlots = [];
  for (const slotKey of targetSlotKeys) {
    if (isApplyBlockedSlot(slotKey)) leakedExcludedSlots.push(slotKey);
  }

  const applyBlockers = [];
  if (apply && !applyApproved) {
    applyBlockers.push(`--apply requires ${REQUIRED_APPLY_FLAG}`);
  }
  if (leakedExcludedSlots.length) {
    applyBlockers.push(`Excluded slot(s) in target list: ${leakedExcludedSlots.join(", ")}`);
  }
  if (!targetSlotKeys.length) {
    applyBlockers.push("v20BApplyBatchAfterReview is empty");
  }

  const tributeBrand = await fetchBrandApiShape(brandRecordId);
  if (!tributeBrand) throw new Error(`Unable to read brand: ${brandRecordId}`);

  const { byName: schemaByName } = await fetchAirtableTableSchemas(baseId, apiKey);
  const presentationTable = schemaByName.get(PRESENTATION_TABLE);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);
  const presentationBySlot = new Map(presentationRows.map((r) => [r.slotKey, r]));

  const preflightRows = [];
  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const rowsMatched = [];
  const rowsUnexpectedDifference = [];
  const postApplyReconciliation = [];
  const wordingRisksBySlot = [];

  targetSlotKeys.forEach((slotKey, index) => {
    const review = reviewRowsBySlot.get(slotKey);
    if (!review) {
      applyBlockers.push(`Missing v20A review row for ${slotKey}`);
      return;
    }
    if (!review.safeForBatch1 || review.movedToSourceEvidenceRequired) {
      applyBlockers.push(`Slot not safe for Batch 1: ${slotKey}`);
    }

    const proposedTitle = nz(review.proposedTitle);
    const proposedBody = nz(review.proposedBody);
    if (!proposedBody) {
      applyBlockers.push(`Empty proposed body for ${slotKey}`);
    }

    const risks = detectCriticalWordingRisk(slotKey, proposedTitle, proposedBody);
    if (risks.length) {
      wordingRisksBySlot.push({ slotKey, risks });
      applyBlockers.push(`Wording risk on ${slotKey}: ${risks.join("; ")}`);
    }

    try {
      assertExternalCopy(proposedBody, slotKey);
      if (proposedTitle) assertExternalCopy(proposedTitle, slotKey);
    } catch (err) {
      applyBlockers.push(err.message);
    }

    const existing = presentationBySlot.get(slotKey);
    const sortOrder = index * 10;
    const proposedState = buildProposedRowState(slotKey, review, brandRecordId, brandName, sortOrder);

    let action = "create";
    let reconciliationStatus = "would_create";
    let differingFields = [];
    let integrityIssues = [];

    if (existing?.recordId) {
      const comparison = compareLiveToProposed(existing, proposedState);
      reconciliationStatus = comparison.status;
      differingFields = comparison.differingFields;
      integrityIssues = comparison.integrityIssues;
      if (comparison.status === "matched") {
        action = "matched";
      } else if (comparison.status === "unexpected_difference") {
        action = "unexpected_difference";
      } else {
        action = "update";
      }
    }

    const writableFields = [
      PRESENTATION_WRITE_FIELDS.slotKey,
      PRESENTATION_WRITE_FIELDS.body,
      PRESENTATION_WRITE_FIELDS.brand,
      PRESENTATION_WRITE_FIELDS.brandName,
      PRESENTATION_WRITE_FIELDS.active,
      PRESENTATION_WRITE_FIELDS.sortOrder,
    ];
    if (proposedState.titleOwnedByV20B) writableFields.push(PRESENTATION_WRITE_FIELDS.title);

    const preflight = {
      slotKey,
      tab: review.tab || tabFromSlot(slotKey),
      rowExists: Boolean(existing?.recordId),
      action,
      reconciliationStatus,
      differingFields,
      integrityIssues,
      recordId: existing?.recordId || null,
      currentTitle: existing?.title || "",
      proposedTitle,
      currentBody: existing?.body || "",
      proposedBody,
      writableFields,
      riskStatus: risks.length ? "blocked" : "clear",
      reviewStatus: review.reviewStatus,
      sourceBasis: review.sourceBasis,
      imageAttachmentsOnRow: existing?.imageAttachmentCount || 0,
      imagesWillRemainUntouched: true,
      sortOrder,
    };
    preflightRows.push(preflight);

    postApplyReconciliation.push({
      slotKey,
      recordId: existing?.recordId || null,
      currentTitle: existing?.title || "",
      proposedTitle,
      currentBody: existing?.body || "",
      proposedBody,
      status: reconciliationStatus,
      differingFields,
      integrityIssues,
    });

    const targetRow = {
      slotKey,
      tab: preflight.tab,
      action,
      reconciliationStatus,
      differingFields,
      recordId: existing?.recordId || null,
      proposedTitle,
      proposedBody,
      sortOrder,
      fieldsPreview: {
        [PRESENTATION_WRITE_FIELDS.slotKey]: slotKey,
        [PRESENTATION_WRITE_FIELDS.title]: proposedTitle,
        [PRESENTATION_WRITE_FIELDS.body]: proposedBody,
        [PRESENTATION_WRITE_FIELDS.brand]: [brandRecordId],
        [PRESENTATION_WRITE_FIELDS.brandName]: brandName,
        [PRESENTATION_WRITE_FIELDS.active]: true,
        [PRESENTATION_WRITE_FIELDS.sortOrder]: sortOrder,
      },
    };

    if (action === "create") rowsWouldCreate.push(targetRow);
    else if (action === "update") rowsWouldUpdate.push(targetRow);
    else if (action === "matched") rowsMatched.push(targetRow);
    else if (action === "unexpected_difference") rowsUnexpectedDifference.push(targetRow);
  });

  const slotsTargetedByTab = {};
  for (const tab of TAB_ORDER) slotsTargetedByTab[tab] = [];
  for (const row of preflightRows) {
    if (!slotsTargetedByTab[row.tab]) slotsTargetedByTab[row.tab] = [];
    slotsTargetedByTab[row.tab].push(row.slotKey);
  }

  const applyResult = { created: [], updated: [], skippedMatched: [], errors: [], blocked: false };
  if (applyMode && applyBlockers.length === 0) {
    for (const target of [...rowsWouldCreate, ...rowsWouldUpdate]) {
      if (target.action === "create") {
        const fields = { ...target.fieldsPreview };
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "POST",
          body: JSON.stringify({ fields, typecast: true }),
        });
        if (!res.ok) {
          applyResult.errors.push({ slotKey: target.slotKey, message: json.error?.message || res.status });
        } else {
          applyResult.created.push({ recordId: json.id, slotKey: target.slotKey });
        }
      } else if (target.recordId) {
        const patchFields = {
          [PRESENTATION_WRITE_FIELDS.body]: target.proposedBody,
          [PRESENTATION_WRITE_FIELDS.active]: true,
          [PRESENTATION_WRITE_FIELDS.sortOrder]: target.sortOrder,
        };
        if (target.proposedTitle) {
          patchFields[PRESENTATION_WRITE_FIELDS.title] = target.proposedTitle;
        }
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          {
            method: "PATCH",
            body: JSON.stringify({ fields: patchFields, typecast: true }),
          },
          target.recordId
        );
        if (!res.ok) {
          applyResult.errors.push({ slotKey: target.slotKey, message: json.error?.message || res.status });
        } else {
          applyResult.updated.push({ recordId: target.recordId, slotKey: target.slotKey });
        }
      }
    }
    for (const target of rowsMatched) {
      applyResult.skippedMatched.push({ recordId: target.recordId, slotKey: target.slotKey });
    }
  } else if (applyMode && applyBlockers.length > 0) {
    applyResult.blocked = true;
  }

  const remainingSlotPlan = buildRemainingSlotPlan();

  const alreadyHas = Array.isArray(v20a.v19Source?.alreadyHasRequiredSlots)
    ? v20a.v19Source.alreadyHasRequiredSlots.length
    : 9;
  const totalRequired = v20a.v19Source?.totalRequiredSlots || 110;
  const projectedPresent = alreadyHas + targetSlotKeys.length;
  const projectedScore = scoreFromPresentKeys(projectedPresent, totalRequired);
  const stillMissing = (v20a.v18Baseline?.totalMissingRequiredCandidateSlots || 101) - targetSlotKeys.length;
  const comparableAfterApply = projectedScore >= 85 && stillMissing === 0;

  const mode = applyMode && !applyResult.blocked && applyResult.errors.length === 0 ? "apply" : "dry-run";
  const airtableModified =
    applyMode && !applyResult.blocked && (applyResult.created.length > 0 || applyResult.updated.length > 0);

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: applyMode && applyBlockers.length === 0 && !applyResult.blocked ? mode : "dry-run",
    airtableModified,
    imagesUntouched: true,
    brandBasicsUntouched: true,
    sourceLinksUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    filesRead: [
      "AGENTS.md",
      V20A_REPORT_PATH,
      "reports/brand-explorer-slot-completion-review-package.md",
      "lib/partner-intelligence/brand-explorer-slot-completion-review-package.js",
      "reports/brand-explorer-slot-completion-planner.md",
      "reports/brand-explorer-slot-standard-manifest.md",
      MANIFEST_PATH,
      COVERAGE_AUDIT_PATH,
      "reports/brand-explorer-presentation-slot-coverage-audit.md",
      "docs/brand-explorer-presentation-slots.md",
      "docs/data-intelligence/brand-explorer-slot-completion-writer-v20B.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-slot-completion-writer.js",
      "scripts/brand-explorer-slot-completion-writer.mjs",
      "docs/data-intelligence/brand-explorer-slot-completion-writer-v20B.md",
      "reports/brand-explorer-slot-completion-writer.md",
      "reports/brand-explorer-slot-completion-writer.json",
      "package.json",
    ],
    v20BWriterExists: true,
    brand: { recordId: brandRecordId, name: brandName },
    v20ASource: {
      v20BWriterSafeToBuild: v20a.v20BWriterSafeToBuild,
      batch1CriticalWordingRisksClear: v20a.batch1CriticalWordingRisksClear,
      safeBatch1SlotCount: v20a.safeBatch1SlotCount,
    },
    slotsTargetedCount: targetSlotKeys.length,
    slotsTargeted: targetSlotKeys,
    slotsTargetedByTab,
    slotsExplicitlyExcluded: [
      ...APPLY_BLOCKLIST_KEYS,
      "economics.*",
      "overview.proof.*",
      "materials.gallery.*",
      "*openings*",
    ],
    excludedSlotsExplicit,
    leakedExcludedSlotsIntoTargetList: leakedExcludedSlots,
    preflightRows,
    postApplyReconciliation,
    rowsWouldCreate,
    rowsWouldUpdate,
    rowsMatched,
    rowsUnexpectedDifference,
    matchedSlotCount: rowsMatched.length,
    trueWouldUpdateCount: rowsWouldUpdate.length,
    unexpectedDifferenceCount: rowsUnexpectedDifference.length,
    v20BIdempotent: rowsWouldCreate.length === 0 && rowsWouldUpdate.length === 0 && rowsUnexpectedDifference.length === 0,
    idempotencyNote:
      "Dry-run classifies existing rows as matched when all v20B-owned writable fields equal proposed values; only true field diffs count as would_update.",
    remainingSlotPlan,
    wordingRisksBySlot,
    wordingRisksRemain: wordingRisksBySlot.length > 0,
    applyBlockers: [...new Set(applyBlockers)],
    applyGatesRequired: ["--apply", REQUIRED_APPLY_FLAG],
    applyResult,
    projectedScoreAfterApply: projectedScore,
    tributeCompletedBrandComparableAfterApply: comparableAfterApply,
    exactApplyCommand:
      "npm run brand-explorer-slot-completion-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-slot-completion-v20B",
    exactDryRunCommand:
      "npm run brand-explorer-slot-completion-writer -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerSlotCompletionWriterMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Slot Completion Writer v20B");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Scope");
  lines.push(`- Slots targeted: **${report.slotsTargetedCount}**`);
  lines.push(`- Rows would create: **${report.rowsWouldCreate.length}**`);
  lines.push(`- Rows would update: **${report.trueWouldUpdateCount}**`);
  lines.push(`- Rows matched (no-op): **${report.matchedSlotCount}**`);
  lines.push(`- Unexpected differences: **${report.unexpectedDifferenceCount}**`);
  lines.push(`- v20B idempotent on dry-run: **${report.v20BIdempotent ? "yes" : "no"}**`);
  lines.push(`- Leaked excluded slots: **${report.leakedExcludedSlotsIntoTargetList.length}**`);
  lines.push(`- Wording risks remain: **${report.wordingRisksRemain ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Score projection");
  lines.push(`- Projected score after apply: **${report.projectedScoreAfterApply}/100**`);
  lines.push(
    `- Completed-brand comparable after apply: **${report.tributeCompletedBrandComparableAfterApply ? "yes" : "no"}**`
  );
  if (report.remainingSlotPlan) {
    lines.push("");
    lines.push("## Post-v20B remaining slots (summary)");
    lines.push(`- Slot coverage score: **${report.remainingSlotPlan.slotCoverageScore}/100**`);
    lines.push(`- Manifest score: **${report.remainingSlotPlan.manifestScore}/100**`);
    lines.push(`- Required slots still missing: **${report.remainingSlotPlan.requiredSlotsStillMissingCount}**`);
    lines.push(`- v21 should target: **${report.remainingSlotPlan.v21Recommendation?.target || "TBD"}**`);
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Brand Basics untouched: **${report.brandBasicsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Marriott validation implied: **${report.marriottValidationImplied ? "yes" : "no"}**`);
  lines.push("");
  if (report.applyBlockers.length) {
    lines.push("## Apply blockers");
    report.applyBlockers.forEach((b) => lines.push(`- ${b}`));
    lines.push("");
  }
  lines.push("## Reconciliation (sample)");
  (report.postApplyReconciliation || []).slice(0, 20).forEach((row) => {
    lines.push(
      `- \`${row.slotKey}\` · ${row.status} · record \`${row.recordId || "—"}\`${row.differingFields?.length ? ` · differs: ${row.differingFields.join(", ")}` : ""}`
    );
  });
  if ((report.postApplyReconciliation || []).length > 20) {
    lines.push(`- …${report.postApplyReconciliation.length - 20} more in reconciliation JSON`);
  }
  lines.push("");
  lines.push("## Apply command (gated)");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}

export function buildBrandExplorerSlotCompletionReconciliationMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Slot Completion Reconciliation v20B");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Summary");
  lines.push(`- Matched (no-op): **${report.matchedSlotCount}**`);
  lines.push(`- Would update: **${report.trueWouldUpdateCount}**`);
  lines.push(`- Would create: **${report.rowsWouldCreate.length}**`);
  lines.push(`- Unexpected differences: **${report.unexpectedDifferenceCount}**`);
  lines.push(`- Idempotent: **${report.v20BIdempotent ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Reconciliation table");
  lines.push("");
  lines.push("| Slot Key | Record ID | Status | Differing fields |");
  lines.push("|---|---|---|---|");
  for (const row of report.postApplyReconciliation || []) {
    lines.push(
      `| \`${row.slotKey}\` | \`${row.recordId || "—"}\` | ${row.status} | ${row.differingFields?.length ? row.differingFields.join(", ") : "—"} |`
    );
  }
  lines.push("");
  if (report.rowsUnexpectedDifference?.length) {
    lines.push("## Unexpected differences");
    for (const row of report.rowsUnexpectedDifference) {
      lines.push(
        `- \`${row.slotKey}\` · integrity: ${(row.integrityIssues || row.differingFields || []).join(", ") || "see JSON"}`
      );
    }
    lines.push("");
  }
  return lines.join("\n");
}

export function buildBrandExplorerSlotCompletionRemainingPlanMarkdown(plan) {
  const lines = [];
  lines.push("# Brand Explorer Slot Completion Remaining Plan (post-v20B)");
  lines.push("");
  lines.push(`Generated: ${plan.generatedAt}`);
  lines.push("");
  lines.push("## Scores");
  lines.push(`- Slot coverage score: **${plan.slotCoverageScore}/100**`);
  lines.push(`- Manifest score: **${plan.manifestScore}/100**`);
  lines.push(`- Required slots still missing: **${plan.requiredSlotsStillMissingCount}**`);
  lines.push(`- Total missing slot keys: **${plan.tributeSlotKeysMissingCount}**`);
  lines.push("");
  lines.push("## v21 recommendation");
  lines.push(`- Target: **${plan.v21Recommendation?.target}**`);
  lines.push(`- Rationale: ${plan.v21Recommendation?.rationale}`);
  lines.push("");
  for (const [bucket, slots] of Object.entries(plan.remainingSlotsGrouped || {})) {
    lines.push(`## ${bucket} (${slots.length})`);
    if (!slots.length) {
      lines.push("- (none)");
    } else {
      slots.forEach((slotKey) => lines.push(`- \`${slotKey}\``));
    }
    lines.push("");
  }
  return lines.join("\n");
}
