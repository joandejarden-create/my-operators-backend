/**
 * Brand Explorer Copy Carryover Cleanup Writer v26A.
 *
 * Removes cross-brand carryover, internal capture labels, and false validation
 * phrasing from Tribute Portfolio presentation copy. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-copy-carryover-cleanup-writer-v26A.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";

export const WRITER_VERSION = "26A";
export const REPORT_JSON_NAME = "brand-explorer-copy-carryover-cleanup-writer.json";
export const REPORT_MD_NAME = "brand-explorer-copy-carryover-cleanup-writer.md";
export const DOC_MD_NAME = "brand-explorer-copy-carryover-cleanup-writer-v26A.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v26A-copy-carryover-cleanup";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-final-copy-cleanup";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

export const ALLOWED_SLOT_KEYS = new Set([
  "insight.similar",
  "footprint.momentum_label",
  "footprint.region.am",
  "footprint.region.cala",
  "footprint.region.eu",
  "footprint.region.mea",
  "footprint.region.apac",
  "materials.file",
  "standards.last_reviewed",
  "standards.source_confidence",
]);

export const INSIGHT_SIMILAR_PEER_REPLACEMENT = {
  title: "Design Hotels",
  body: "(Marriott · independent-character collection · experiential design benchmark for diligence)",
};

export const STANDARDS_SOURCE_CONFIDENCE_BODY_CLEAN =
  "Founder-reviewed · Owner-planning guidance · No company sign-off · Legal/transaction confirmation required";

export const MOMENTUM_LABEL_BODY_CLEAN = "Recent momentum — documented openings when available";

const CRITICAL_CARRYOVER_RE = /\bcurio collection\b|\bhilton honors\b|\bchoice privileges\b|\bradisson rewards\b/i;
const QA_VALIDATION_RE = /company validated|marriott validated/i;
const QA_INTERNAL_RE =
  /\bconsumer site\b|\bconsumer brand site\b|\bbrand site\b|\bsource-backed\b|\bapproved facts\b|\bno performance guarantee\b|\bconfirm scale claims\b/i;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.md",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "live Tribute Brand Explorer Presentation rows",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
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

export function sanitizeCarryoverText(text) {
  let out = normalizeBody(text);
  if (!out) return out;

  out = out.replace(/\bconsumer brand site\b/gi, "public brand website");
  out = out.replace(/\bconsumer site\b/gi, "public brand page");
  out = out.replace(/\bbrand site\b/gi, "public website");
  out = out.replace(/\bsource-backed\b/gi, "documented");
  out = out.replace(/\bNot Company Validated\b/g, "No company sign-off");
  out = out.replace(/\bNot company-validated\b/gi, "No company sign-off");
  out = out.replace(/\bcompany validated\b/gi, "company sign-off");
  out = out.replace(/\bmarriott validated\b/gi, "Marriott sign-off");
  out = out.replace(
    /Official Tribute Portfolio consumer brand site/gi,
    "Tribute Portfolio — Public brand website"
  );
  out = out.replace(/Website Capture · Marriott-controlled reference ·/gi, "Official Marriott reference ·");
  return out.trim();
}

export function detectCarryoverIssues({ slotKey, title, body }) {
  const combined = `${nz(title)}\n${nz(body)}`;
  const issues = [];

  if (CRITICAL_CARRYOVER_RE.test(combined)) {
    issues.push({
      patternId: "critical_carryover",
      severity: "critical",
      message: "Cross-brand carryover language detected",
    });
  }
  if (QA_VALIDATION_RE.test(combined)) {
    issues.push({
      patternId: "validation_language",
      severity: "critical",
      message: "Copy contains company/Marriott validation phrasing",
    });
  }
  if (QA_INTERNAL_RE.test(combined)) {
    issues.push({
      patternId: "internal_capture_language",
      severity: "medium",
      message: "UI-facing internal/source-capture language detected",
    });
  }
  if (slotKey === "insight.similar" && /\bcurio collection\b/i.test(combined)) {
    issues.push({
      patternId: "curio_brand",
      severity: "critical",
      message: "Curio Collection peer card on Tribute profile",
    });
  }
  return issues;
}

export function proposeCleanCopy(row) {
  const slotKey = nz(row.slotKey);
  const currentTitle = nz(row.title);
  const currentBody = normalizeBody(row.body);
  let proposedTitle = currentTitle;
  let proposedBody = currentBody;
  let fixReason = "";

  if (slotKey === "insight.similar" && /\bcurio collection\b/i.test(`${currentTitle}\n${currentBody}`)) {
    proposedTitle = INSIGHT_SIMILAR_PEER_REPLACEMENT.title;
    proposedBody = INSIGHT_SIMILAR_PEER_REPLACEMENT.body;
    fixReason = "replace_curio_peer_with_design_hotels";
  } else if (slotKey === "standards.source_confidence") {
    proposedBody = STANDARDS_SOURCE_CONFIDENCE_BODY_CLEAN;
    fixReason = "standards_source_confidence_validation_safe_wording";
  } else if (slotKey === "footprint.momentum_label") {
    proposedBody = MOMENTUM_LABEL_BODY_CLEAN;
    fixReason = "remove_governance_phrase_from_momentum_label";
  } else if (slotKey === "materials.file" && /consumer|brand site|website capture/i.test(`${currentTitle}\n${currentBody}`)) {
    proposedTitle = proposedTitle.replace(
      /Tribute Portfolio — Official Tribute Portfolio consumer brand site/i,
      "Tribute Portfolio — Public brand website"
    );
    if (!proposedTitle || /consumer brand site/i.test(proposedTitle)) {
      proposedTitle = "Tribute Portfolio — Public brand website";
    }
    proposedBody = sanitizeCarryoverText(currentBody);
    if (!proposedBody) {
      proposedBody = "Official Marriott reference · https://tribute-portfolio.marriott.com/";
    }
    fixReason = "materials_file_owner_facing_label";
  } else {
    proposedTitle = sanitizeCarryoverText(currentTitle);
    proposedBody = sanitizeCarryoverText(currentBody);
    fixReason = "sanitize_internal_capture_and_validation_phrasing";
  }

  return {
    slotKey,
    recordId: row.recordId,
    fixReason,
    currentTitle,
    currentBody,
    proposedTitle,
    proposedBody,
    currentIssues: detectCarryoverIssues({ slotKey, title: currentTitle, body: currentBody }),
    proposedIssues: detectCarryoverIssues({ slotKey, title: proposedTitle, body: proposedBody }),
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

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-copy-carryover-cleanup-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_VALIDATION}`;
}

export async function buildBrandExplorerCopyCarryoverCleanupWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
} = {}) {
  const brandRecordId =
    nz(brandIdOrName).toLowerCase() === "tribute-portfolio" || !nz(brandIdOrName)
      ? TRIBUTE_RECORD_ID
      : nz(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v26A pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
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

  const targetRows = presentationRows.filter((r) => ALLOWED_SLOT_KEYS.has(r.slotKey));
  const proposals = targetRows.map((row) => proposeCleanCopy(row));
  const rowsWouldUpdate = [];
  const applyBlockers = [];

  for (const proposal of proposals) {
    const unchanged =
      nz(proposal.currentTitle) === nz(proposal.proposedTitle) &&
      normalizeBody(proposal.currentBody) === normalizeBody(proposal.proposedBody);
    if (unchanged) continue;
    if (proposal.proposedIssues.some((i) => i.severity === "critical")) {
      applyBlockers.push(`${proposal.slotKey}:proposed_copy_still_critical:${proposal.recordId}`);
      continue;
    }
    rowsWouldUpdate.push({
      recordId: proposal.recordId,
      slotKey: proposal.slotKey,
      fixReason: proposal.fixReason,
      currentTitle: proposal.currentTitle,
      currentBody: proposal.currentBody,
      proposedTitle: proposal.proposedTitle,
      proposedBody: proposal.proposedBody,
      fields: {
        "Slot Key": proposal.slotKey,
        Title: proposal.proposedTitle,
        Body: proposal.proposedBody,
        "Brand Name": BRAND_NAME,
        Active: true,
      },
    });
  }

  const defectsBefore = targetRows.flatMap((row) =>
    detectCarryoverIssues(row).map((issue) => ({
      ...issue,
      slotKey: row.slotKey,
      recordId: row.recordId,
    }))
  );

  const projectedDefectsAfter = targetRows.map((row) => {
    const proposal = proposeCleanCopy(row);
    return {
      slotKey: row.slotKey,
      recordId: row.recordId,
      issues: proposal.proposedIssues,
    };
  });

  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const hasWork = rowsWouldUpdate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
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
        updated.push({ recordId: json.id, slotKey: row.slotKey, fixReason: row.fixReason });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = updated.length > 0 && errors.length === 0;
    applyResults = { updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(brandRecordId));
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v26AWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: { name: BRAND_NAME, recordId: brandRecordId, slug: "tribute-portfolio" },
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-copy-carryover-cleanup-writer.js",
      "scripts/brand-explorer-copy-carryover-cleanup-writer.mjs",
      "docs/data-intelligence/brand-explorer-copy-carryover-cleanup-writer-v26A.md",
      "reports/brand-explorer-copy-carryover-cleanup-writer.md",
      "reports/brand-explorer-copy-carryover-cleanup-writer.json",
      "package.json",
    ],
    allowedSlotKeys: [...ALLOWED_SLOT_KEYS],
    rowsInspected: targetRows.length,
    defectsBefore,
    projectedDefectsAfter,
    proposals: proposals.filter(
      (p) =>
        nz(p.currentTitle) !== nz(p.proposedTitle) ||
        normalizeBody(p.currentBody) !== normalizeBody(p.proposedBody)
    ),
    rowsWouldUpdate,
    rowsWouldCreate: [],
    projectedCriticalDefectsAfterApply: projectedDefectsAfter.reduce(
      (sum, row) => sum + row.issues.filter((i) => i.severity === "critical").length,
      0
    ),
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    imagesUntouched: true,
    sortOrderUntouched: true,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      noValidationClaim,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers: [...new Set(applyBlockers)],
    applyResults,
    exactApplyCommand: buildApplyCommand("tribute-portfolio"),
    idempotentAfterApply: rowsWouldUpdate.length === 0,
    doesNotDo: [
      "Set Company Validated or Company Validation Date",
      "Imply Marriott or company validation",
      "Modify loyalty, openings, momentum event rows, demand, or standards table requirement copy",
      "Change images or Sort Order",
    ],
  };
}

export function buildBrandExplorerCopyCarryoverCleanupWriterMarkdown(report) {
  return [
    `# Brand Explorer Copy Carryover Cleanup Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- Rows inspected: **${report.rowsInspected}**`,
    `- Defects before: **${report.defectsBefore.length}**`,
    `- Rows would update: **${report.rowsWouldUpdate.length}**`,
    `- Projected critical defects after apply: **${report.projectedCriticalDefectsAfterApply}**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    "",
    "## Proposed updates",
    ...(report.proposals.length
      ? report.proposals.map(
          (p) =>
            `- **${p.slotKey}** (\`${p.recordId}\`) · ${p.fixReason}\n  - title: ${p.proposedTitle || "(empty)"}\n  - body: ${p.proposedBody.slice(0, 180)}${p.proposedBody.length > 180 ? "…" : ""}`
        )
      : ["- (none — idempotent)"]),
    "",
    "## Exact apply command",
    "```bash",
    report.exactApplyCommand,
    "```",
  ].join("\n");
}
