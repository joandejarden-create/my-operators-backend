/**
 * Brand Explorer Tribute Standard Detail Review Approval State Writer v25C-5C.
 *
 * Updates governance/review metadata for existing Tribute standards presentation rows.
 * Does not rewrite standards table copy unless a safety blocker is found.
 *
 * @see docs/data-intelligence/brand-explorer-tribute-standard-detail-review-approval-writer-v25C-5C.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  STANDARDS_INTRO_BODY,
  TRIBUTE_REQUIREMENT_ROWS,
} from "./brand-explorer-tribute-standard-detail-table-writer.js";

export const WRITER_VERSION = "25C-5C";
export const REPORT_JSON_NAME = "brand-explorer-tribute-standard-detail-review-approval-writer.json";
export const REPORT_MD_NAME = "brand-explorer-tribute-standard-detail-review-approval-writer.md";
export const DOC_MD_NAME = "brand-explorer-tribute-standard-detail-review-approval-writer-v25C-5C.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v25C-5C-standard-detail-review-state";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-standard-detail-owner-planning-copy";
export const APPLY_FLAG_NO_LEGAL = "--confirm-not-legal-or-company-validation";

export const REQUIREMENT_SLOT = "standards.requirement";
export const INTRO_SLOT = "standards.intro";
export const LAST_REVIEWED_SLOT = "standards.last_reviewed";
export const SOURCE_CONFIDENCE_SLOT = "standards.source_confidence";
export const MIN_REQUIREMENT_ROWS = 7;

export const GOVERNANCE_TARGET_SLOTS = [LAST_REVIEWED_SLOT, SOURCE_CONFIDENCE_SLOT];

export const STANDARDS_LAST_REVIEWED_BODY =
  "Founder-reviewed 2026-07-09 — confirm current Tribute design standards, PIP scope, and agreement vintage with transaction documents before capital commitments.";

export const STANDARDS_SOURCE_CONFIDENCE_BODY =
  "Founder-Reviewed · Owner-Planning Guidance · Not Company Validated · Legal/Transaction Confirmation Required";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const CURIO_HILTON_COPY_RE =
  /curio collection|exactly like nothing else|hilton honors|hilton pms|hilton crs|tapestry collection by hilton|waldorf astoria|hilton garden inn|doubletree by hilton|canopy by hilton|signia by hilton|embassy suites|hampton by hilton|tru by hilton|spark by hilton|homewood suites|livsmart studios|radisson blu|choice privileges|alpha brand studios|kimpton hotels|ihg one rewards/i;

const RAW_FDD_LEGAL_RE =
  /item\s*19|franchise disclosure document|\bfdd\b|§\s*\d|hereinafter|pursuant to the agreement|exhibit\s+[a-z]\b|whereas\b/i;

const MARRIOTT_VALIDATION_RE =
  /marriott\s+validated|validated\s+by\s+marriott|company-validated\s+standards|company validated standards/i;

const COMPANY_VALIDATION_RE = /company.validated(?!\s*·\s*not)/i;

const PROTECTED_SLOT_PREFIXES = [
  "loyalty.",
  "footprint.openings",
  "footprint.momentum",
  "footprint.portfolio_mix",
  "footprint.region.",
  "overview.portfolio_context",
  "commercial.demand",
  "branded.",
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-tribute-standard-detail-table-writer.md",
  "reports/brand-explorer-tribute-standard-detail-table-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "lib/partner-intelligence/brand-explorer-tribute-standard-detail-table-writer.js",
  "live Tribute standards.intro row",
  "live Tribute standards.requirement rows",
  "live Curio/Kimpton/Radisson/Ascend standards.requirement rows",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

function mergedSlotBody(brand, slotKey) {
  return blocksForSlot(brand, slotKey)
    .map((b) => nz(b.body))
    .filter(Boolean)
    .join("\n\n");
}

export function parseRequirementColumns(body) {
  const out = { typical: "", owner: "", status: "", notes: "" };
  for (const line of normalizeBody(body).split("\n")) {
    const t = line.trim();
    if (/^Typical consideration:/i.test(t)) {
      out.typical = t.replace(/^Typical consideration:\s*/i, "").trim();
    } else if (/^Owner planning consideration:/i.test(t)) {
      out.owner = t.replace(/^Owner planning consideration:\s*/i, "").trim();
    } else if (/^Typical status:/i.test(t)) {
      out.status = t.replace(/^Typical status:\s*/i, "").trim();
    } else if (/^Notes to confirm:/i.test(t)) {
      out.notes = t.replace(/^Notes to confirm:\s*/i, "").trim();
    }
  }
  return out;
}

export function requirementRowHasRequiredColumns(row) {
  const title = nz(row?.title);
  const body = nz(row?.body);
  const cols = parseRequirementColumns(body);
  return Boolean(title && cols.typical && cols.owner && cols.status && cols.notes);
}

export function scanCopySafety(text) {
  const combined = nz(text);
  const issues = [];
  if (CURIO_HILTON_COPY_RE.test(combined)) issues.push("reference_brand_copy");
  if (RAW_FDD_LEGAL_RE.test(combined)) issues.push("raw_fdd_legal_fragment");
  if (MARRIOTT_VALIDATION_RE.test(combined)) issues.push("marriott_validation_language");
  if (COMPANY_VALIDATION_RE.test(combined) && !/not company validated/i.test(combined)) {
    issues.push("company_validation_language");
  }
  return issues;
}

export function governanceBodiesMatchApproval(bodyLastReviewed, bodySourceConfidence) {
  const lr = normalizeBody(bodyLastReviewed);
  const sc = normalizeBody(bodySourceConfidence);
  return (
    /founder-reviewed/i.test(lr) &&
    /founder-reviewed/i.test(sc) &&
    /owner-planning guidance/i.test(sc) &&
    /(?:not company validated|no company sign-off)/i.test(sc) &&
    /legal\/transaction confirmation required/i.test(sc)
  );
}

export function evaluateStandardsDetailApprovalState(brand, requirementRows = null) {
  const rows = requirementRows || blocksForSlot(brand, REQUIREMENT_SLOT);
  const introBody = mergedSlotBody(brand, INTRO_SLOT);
  const lastReviewedBody = mergedSlotBody(brand, LAST_REVIEWED_SLOT);
  const sourceConfidenceBody = mergedSlotBody(brand, SOURCE_CONFIDENCE_SLOT);

  const completeRows = rows.filter(requirementRowHasRequiredColumns);
  const copyTexts = [
    introBody,
    lastReviewedBody,
    sourceConfidenceBody,
    ...rows.map((r) => `${r.title}\n${r.body}`),
  ];
  const copySafetyIssues = [...new Set(copyTexts.flatMap((t) => scanCopySafety(t)))];

  const governanceApproved = governanceBodiesMatchApproval(lastReviewedBody, sourceConfidenceBody);

  const blockers = [];
  if (rows.length < MIN_REQUIREMENT_ROWS) {
    blockers.push(`insufficient_requirement_rows:${rows.length}<${MIN_REQUIREMENT_ROWS}`);
  }
  if (completeRows.length < MIN_REQUIREMENT_ROWS) {
    blockers.push(`incomplete_requirement_columns:${completeRows.length}<${MIN_REQUIREMENT_ROWS}`);
  }
  if (!hasVal(introBody)) blockers.push("missing_standards_intro");
  if (!governanceApproved) blockers.push("governance_review_state_incomplete");
  if (copySafetyIssues.length) blockers.push(`copy_safety:${copySafetyIssues.join(",")}`);

  return {
    requirementRowCount: rows.length,
    completeRequirementRowCount: completeRows.length,
    introPresent: hasVal(introBody),
    governanceLastReviewedPresent: hasVal(lastReviewedBody),
    governanceSourceConfidencePresent: hasVal(sourceConfidenceBody),
    founderReviewGovernancePresent: governanceApproved,
    copySafetyPassed: copySafetyIssues.length === 0,
    copySafetyIssues,
    ready: blockers.length === 0,
    blockers,
  };
}

function isProtectedNonGovernanceSlot(slotKey) {
  const key = nz(slotKey);
  if (GOVERNANCE_TARGET_SLOTS.includes(key)) return false;
  if (key === REQUIREMENT_SLOT || key === INTRO_SLOT) return false;
  if (key.startsWith("standards.")) return true;
  return PROTECTED_SLOT_PREFIXES.some((prefix) => key === prefix || key.startsWith(prefix));
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
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

function presentationFieldsForGovernance(slotKey, body, sort, brandRecordId) {
  return {
    "Slot Key": slotKey,
    Title: "",
    Body: body,
    "Brand Name": BRAND_NAME,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort,
  };
}

function bodiesMatch(a, b) {
  return normalizeBody(a) === normalizeBody(b);
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-tribute-standard-detail-review-approval-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_LEGAL}`;
}

export async function buildBrandExplorerTributeStandardDetailReviewApprovalWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noLegalOrCompanyConfirmed = false,
} = {}) {
  const brandRecordId =
    nz(brandIdOrName).toLowerCase() === "tribute-portfolio" || !nz(brandIdOrName)
      ? TRIBUTE_RECORD_ID
      : nz(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-5C pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );

  const presentationRows = presentationRaw.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    active: rec.fields?.Active,
  }));

  const requirementRowsLive = presentationRows.filter((r) => r.slotKey === REQUIREMENT_SLOT);
  const introRowLive = presentationRows.find((r) => r.slotKey === INTRO_SLOT);
  const lastReviewedLive = presentationRows.find((r) => r.slotKey === LAST_REVIEWED_SLOT);
  const sourceConfidenceLive = presentationRows.find((r) => r.slotKey === SOURCE_CONFIDENCE_SLOT);

  const brandShape = {
    brandExplorer: {
      blocks: presentationRows
        .filter((r) => r.active !== false)
        .map((r) => ({
          recordId: r.recordId,
          slotKey: r.slotKey,
          title: r.title,
          body: r.body,
          sort: r.sortOrder,
        })),
    },
  };

  const currentApprovalState = evaluateStandardsDetailApprovalState(
    brandShape,
    brandShape.brandExplorer.blocks.filter((b) => b.slotKey === REQUIREMENT_SLOT)
  );

  const copySafetyFindings = [];
  for (const row of requirementRowsLive) {
    const issues = scanCopySafety(`${row.title}\n${row.body}`);
    if (issues.length) {
      copySafetyFindings.push({ recordId: row.recordId, title: row.title, issues });
    }
  }
  if (introRowLive) {
    const issues = scanCopySafety(introRowLive.body);
    if (issues.length) {
      copySafetyFindings.push({ recordId: introRowLive.recordId, slotKey: INTRO_SLOT, issues });
    }
  }

  const expectedRequirementTitles = new Set(
    TRIBUTE_REQUIREMENT_ROWS.map((r) => r.title.toLowerCase())
  );
  const missingRequirementTitles = TRIBUTE_REQUIREMENT_ROWS.map((r) => r.title).filter(
    (title) => !requirementRowsLive.some((r) => nz(r.title).toLowerCase() === title.toLowerCase())
  );

  const governancePlans = [
    {
      slotKey: LAST_REVIEWED_SLOT,
      recordId: lastReviewedLive?.recordId || null,
      action: lastReviewedLive ? "update" : "create",
      sort: 0,
      proposedBody: STANDARDS_LAST_REVIEWED_BODY,
      currentBody: lastReviewedLive?.body || "",
      fields: presentationFieldsForGovernance(
        LAST_REVIEWED_SLOT,
        STANDARDS_LAST_REVIEWED_BODY,
        0,
        brandRecordId
      ),
    },
    {
      slotKey: SOURCE_CONFIDENCE_SLOT,
      recordId: sourceConfidenceLive?.recordId || null,
      action: sourceConfidenceLive ? "update" : "create",
      sort: 0,
      proposedBody: STANDARDS_SOURCE_CONFIDENCE_BODY,
      currentBody: sourceConfidenceLive?.body || "",
      fields: presentationFieldsForGovernance(
        SOURCE_CONFIDENCE_SLOT,
        STANDARDS_SOURCE_CONFIDENCE_BODY,
        0,
        brandRecordId
      ),
    },
  ];

  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const rowsMatched = [];
  const copyChangesRequired = [];

  for (const plan of governancePlans) {
    const safetyIssues = scanCopySafety(plan.proposedBody);
    if (safetyIssues.length) {
      throw new Error(`Proposed governance copy failed safety scan: ${plan.slotKey}`);
    }
    if (plan.recordId && bodiesMatch(plan.currentBody, plan.proposedBody)) {
      rowsMatched.push({ recordId: plan.recordId, slotKey: plan.slotKey, action: "matched" });
      continue;
    }
    if (plan.recordId) {
      rowsWouldUpdate.push({
        recordId: plan.recordId,
        slotKey: plan.slotKey,
        action: "update",
        currentBody: plan.currentBody,
        proposedBody: plan.proposedBody,
        fields: {
          "Slot Key": plan.slotKey,
          Title: "",
          Body: plan.proposedBody,
          "Brand Name": BRAND_NAME,
          Active: true,
          "Sort Order": plan.sort,
        },
      });
    } else {
      rowsWouldCreate.push({
        slotKey: plan.slotKey,
        action: "create",
        proposedBody: plan.proposedBody,
        fields: plan.fields,
      });
    }
  }

  const applyBlockers = [];
  if (requirementRowsLive.length < MIN_REQUIREMENT_ROWS) {
    applyBlockers.push(`insufficient_requirement_rows:${requirementRowsLive.length}`);
  }
  if (missingRequirementTitles.length) {
    applyBlockers.push(`missing_requirement_titles:${missingRequirementTitles.join("|")}`);
  }
  for (const row of requirementRowsLive) {
    if (!requirementRowHasRequiredColumns(row)) {
      applyBlockers.push(`missing_required_columns:${row.title || row.recordId}`);
    }
  }
  if (copySafetyFindings.length) {
    applyBlockers.push(
      `copy_safety_blocker:${copySafetyFindings.map((f) => f.title || f.slotKey).join(",")}`
    );
  }
  const proposedGovernanceSafety = scanCopySafety(
    `${STANDARDS_LAST_REVIEWED_BODY}\n${STANDARDS_SOURCE_CONFIDENCE_BODY}`
  );
  if (proposedGovernanceSafety.length) {
    applyBlockers.push(`proposed_governance_unsafe:${proposedGovernanceSafety.join(",")}`);
  }

  const projectedApprovalState = evaluateStandardsDetailApprovalState({
    brandExplorer: {
      blocks: [
        ...brandShape.brandExplorer.blocks.filter(
          (b) => !GOVERNANCE_TARGET_SLOTS.includes(b.slotKey)
        ),
        {
          slotKey: LAST_REVIEWED_SLOT,
          title: "",
          body: STANDARDS_LAST_REVIEWED_BODY,
        },
        {
          slotKey: SOURCE_CONFIDENCE_SLOT,
          title: "",
          body: STANDARDS_SOURCE_CONFIDENCE_BODY,
        },
      ],
    },
  });

  const applyGatesReady =
    apply && approveBatch && founderReviewed && noLegalOrCompanyConfirmed;
  const hasWork = rowsWouldCreate.length > 0 || rowsWouldUpdate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const created = [];
    const updated = [];
    const errors = [];

    for (const row of rowsWouldUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: row.fields, typecast: true }),
        },
        row.recordId
      );
      if (!res.ok) {
        errors.push({
          action: "update",
          slotKey: row.slotKey,
          recordId: row.recordId,
          message: json.error?.message || res.status,
        });
      } else {
        updated.push({ recordId: json.id, slotKey: row.slotKey });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const row of rowsWouldCreate) {
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "POST",
        body: JSON.stringify({ fields: row.fields, typecast: true }),
      });
      if (!res.ok) {
        errors.push({
          action: "create",
          slotKey: row.slotKey,
          message: json.error?.message || res.status,
        });
      } else {
        created.push({ recordId: json.id, slotKey: row.slotKey });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    airtableModified = (created.length > 0 || updated.length > 0) && errors.length === 0;
    applyResults = { created, updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(brandRecordId));
  } else if (apply) {
    applyResults = { created: [], updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C5CWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: { name: BRAND_NAME, recordId: brandRecordId, slug: "tribute-portfolio" },
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-tribute-standard-detail-review-approval-writer.js",
      "scripts/brand-explorer-tribute-standard-detail-review-approval-writer.mjs",
      "docs/data-intelligence/brand-explorer-tribute-standard-detail-review-approval-writer-v25C-5C.md",
      "reports/brand-explorer-tribute-standard-detail-review-approval-writer.md",
      "reports/brand-explorer-tribute-standard-detail-review-approval-writer.json",
      "package.json",
      "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
    ],
    currentStandardDetailReadiness: {
      contractStatusBeforeApply: currentApprovalState.ready ? "ready" : "partial_not_approved",
      ...currentApprovalState,
    },
    existingStandardsRowsInspected: {
      intro: introRowLive
        ? {
            recordId: introRowLive.recordId,
            bodyPreview: introRowLive.body.slice(0, 120),
            matchesExpectedIntro: bodiesMatch(introRowLive.body, STANDARDS_INTRO_BODY),
          }
        : null,
      requirementRows: requirementRowsLive.map((r) => ({
        recordId: r.recordId,
        title: r.title,
        sortOrder: r.sortOrder,
        columnsComplete: requirementRowHasRequiredColumns(r),
        columns: parseRequirementColumns(r.body),
      })),
      governanceRows: {
        lastReviewed: lastReviewedLive
          ? { recordId: lastReviewedLive.recordId, body: lastReviewedLive.body }
          : null,
        sourceConfidence: sourceConfidenceLive
          ? { recordId: sourceConfidenceLive.recordId, body: sourceConfidenceLive.body }
          : null,
      },
      otherStandardsSlots: presentationRows
        .filter((r) => r.slotKey.startsWith("standards.") && !["standards.intro", "standards.requirement", ...GOVERNANCE_TARGET_SLOTS].includes(r.slotKey))
        .map((r) => ({ recordId: r.recordId, slotKey: r.slotKey })),
    },
    copySafetyFindings,
    governanceFieldsInspected: {
      targetSlots: GOVERNANCE_TARGET_SLOTS,
      referencePattern:
        "Active brands expose standards.last_reviewed (+ optional standards.source_confidence) as UI meta above the owner table",
      proposedLastReviewed: STANDARDS_LAST_REVIEWED_BODY,
      proposedSourceConfidence: STANDARDS_SOURCE_CONFIDENCE_BODY,
    },
    rowsWouldCreate: rowsWouldCreate.map((r) => ({
      slotKey: r.slotKey,
      proposedBody: r.proposedBody,
      action: r.action,
    })),
    rowsWouldUpdate: rowsWouldUpdate.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      currentBody: r.currentBody,
      proposedBody: r.proposedBody,
      action: r.action,
    })),
    rowsMatched,
    exactProposedGovernanceUpdates: governancePlans.map((p) => ({
      slotKey: p.slotKey,
      action: p.action,
      recordId: p.recordId,
      body: p.proposedBody,
      fields: p.fields,
    })),
    copyChangesRequired,
    rawFddLegalCopyExcluded: !RAW_FDD_LEGAL_RE.test(
      `${STANDARDS_LAST_REVIEWED_BODY}${STANDARDS_SOURCE_CONFIDENCE_BODY}`
    ),
    referenceBrandCopyExcluded: !CURIO_HILTON_COPY_RE.test(
      `${STANDARDS_LAST_REVIEWED_BODY}${STANDARDS_SOURCE_CONFIDENCE_BODY}`
    ),
    projectedStandardDetailReadyAfterApply: projectedApprovalState.ready,
    projectedApprovalState,
    requirementRowCount: requirementRowsLive.length,
    loyaltyRowsUntouched: true,
    openingsRowsUntouched: true,
    momentumRowsUntouched: true,
    demandRowsUntouched: true,
    portfolioMixContextRowsUntouched: true,
    geographicFootprintRowsUntouched: true,
    brandedResidencesUntouched: true,
    standardsRequirementCopyUntouched: copyChangesRequired.length === 0,
    standardsIntroCopyUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      noLegalOrCompanyConfirmed,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers: [...new Set(applyBlockers)],
    applyResults,
    exactApplyCommand: buildApplyCommand("tribute-portfolio"),
    idempotentAfterApply:
      rowsWouldCreate.length === 0 && rowsWouldUpdate.length === 0 && projectedApprovalState.ready,
    doesNotDo: [
      "Create or modify standards.requirement table rows",
      "Rewrite standards.intro or requirement copy unless a safety blocker is found",
      "Publish raw FDD/legal fragments",
      "Mark Company Validated or change Company Validation Date",
      "Imply Marriott validated standards",
      "Modify loyalty, openings, momentum, demand, portfolio mix/context, or geographic footprint rows",
    ],
  };
}

export function buildBrandExplorerTributeStandardDetailReviewApprovalWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Tribute Standard Detail Review Approval Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Writer exists: **${report.v25C5CWriterExists ? "yes" : "no"}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    "",
    "## Summary",
    "",
    `- Requirement rows inspected: **${report.requirementRowCount}**`,
    `- Current readiness: **${report.currentStandardDetailReadiness.contractStatusBeforeApply}**`,
    `- Ready after apply: **${report.projectedStandardDetailReadyAfterApply ? "yes" : "no"}**`,
    `- Governance rows to create: **${report.rowsWouldCreate.length}**`,
    `- Governance rows to update: **${report.rowsWouldUpdate.length}**`,
    `- Copy changes required: **${report.copyChangesRequired.length > 0 ? "yes" : "no"}**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Apply command",
    "",
    "```bash",
    report.exactApplyCommand,
    "```",
  ];
  return lines.join("\n");
}
