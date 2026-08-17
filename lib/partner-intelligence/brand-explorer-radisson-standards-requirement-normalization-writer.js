/**
 * Brand Explorer Radisson Standards Requirement Column Normalization Writer v27D.
 *
 * Normalizes Radisson by Choice standards.requirement row labels into the v27B
 * owner-planning column shape without rewriting standards substance.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-standards-requirement-normalization-writer-v27D.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import {
  evaluateStandardsDetailReadinessGeneralized,
  MIN_REQUIREMENT_ROWS_GENERAL,
} from "./brand-explorer-required-section-contract-evaluators.js";
import {
  requirementRowHasRequiredColumns,
  parseRequirementColumns,
  INTRO_SLOT,
  LAST_REVIEWED_SLOT,
  SOURCE_CONFIDENCE_SLOT,
  REQUIREMENT_SLOT,
} from "./brand-explorer-tribute-standard-detail-review-approval-writer.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";
import { STANDARDS_SOURCE_CONFIDENCE_BODY } from "./brand-explorer-standard-detail-governance-writer.js";

export const WRITER_VERSION = "27D";
export const REPORT_JSON_NAME = "brand-explorer-radisson-standards-requirement-normalization-writer.json";
export const REPORT_MD_NAME = "brand-explorer-radisson-standards-requirement-normalization-writer.md";
export const DOC_MD_NAME = "brand-explorer-radisson-standards-requirement-normalization-writer-v27D.md";

export const TARGET_BRAND_SLUG = "radisson";
export const TARGET_RECORD_ID = "recywbx1YQSTCPqW1";
export const TARGET_BRAND_NAME = "Radisson by Choice";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v27D-radisson-standards-requirement-normalization";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-standard-detail-owner-planning-copy";
export const APPLY_FLAG_NO_LEGAL = "--confirm-not-legal-or-company-validation";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "kimpton",
  "radisson-blu",
  "ascend",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-standard-detail-governance-writer.md",
  "reports/brand-explorer-standard-detail-governance-writer.json",
  "reports/brand-explorer-required-section-contract-generalization-writer.md",
  "reports/brand-explorer-required-section-contract-generalization-writer.json",
  "reports/brand-explorer-complete-build-radisson.md",
  "reports/brand-explorer-complete-build-radisson.json",
  "lib/partner-intelligence/brand-explorer-required-section-contract-evaluators.js",
  "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
  "lib/partner-intelligence/brand-explorer-standard-detail-governance-writer.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Radisson standards.requirement rows",
  "live Radisson standards.intro",
  "live Radisson standards.last_reviewed",
  "live Radisson standards.source_confidence",
  "Radisson Blu standards rows (structure reference only)",
  "Tribute standards governance rows (pattern reference only)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-standards-requirement-normalization-writer.js",
  "scripts/brand-explorer-radisson-standards-requirement-normalization-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|brand approved|validated by choice|validated by radisson|official sign-off|company-approved|company approved/i;

const WRONG_BRAND_COPY_RE =
  /marriott|bonvoy|tribute portfolio|autograph collection|curio collection|kimpton hotels|ihg one rewards|hilton honors|waldorf astoria/i;

const RAW_LEGAL_FRAGMENT_RE =
  /item\s*19|franchise disclosure document|§\s*\d|hereinafter|pursuant to the agreement|exhibit\s+[a-z]\b|whereas\b/i;

const INTERNAL_EXTRACTION_RE =
  /source capture|internal extraction|needs source capture|paste into airtable|rec[a-z0-9]{14}/i;

const STANDARDS_LAST_REVIEWED_BODY =
  "Founder-reviewed owner-planning guidance — confirm current Radisson by Choice brand standards, PIP scope, conversion requirements, and agreement vintage with transaction documents before capital commitments.";

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

function bodiesMatch(a, b) {
  return normalizeBody(a) === normalizeBody(b);
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function lineValue(body, labelRe, replaceRe) {
  for (const line of normalizeBody(body).split("\n")) {
    const t = line.trim();
    if (labelRe.test(t)) return t.replace(replaceRe, "").trim();
  }
  return "";
}

/** Parse Radisson alt-shaped requirement bodies (Applies to / Flexibility labels). */
export function parseRadissonRequirementAltColumns(body) {
  const canonical = parseRequirementColumns(body);
  const appliesTo = lineValue(body, /^Applies to:/i, /^Applies to:\s*/i);
  const flexNotes = lineValue(
    body,
    /^Flexibility\s*\/\s*exception notes:/i,
    /^Flexibility\s*\/\s*exception notes:\s*/i
  );
  const flexOnly = flexNotes || lineValue(body, /^Flexibility:/i, /^Flexibility:\s*/i);
  const exceptionOnly =
    flexOnly || lineValue(body, /^exception notes:/i, /^exception notes:\s*/i);

  return {
    typical: canonical.typical,
    owner: canonical.owner,
    status: canonical.status,
    notes: canonical.notes,
    appliesTo,
    flexNotes: exceptionOnly,
  };
}

function mapTypicalStatus(alt) {
  if (hasVal(alt.status)) {
    const s = alt.status.toLowerCase();
    if (/typically expected|may apply|case-by-case|confirm with brand/i.test(s)) {
      return alt.status;
    }
    if (/confirm/i.test(s)) return "Confirm with brand";
  }
  if (/typically|usually|commonly|mandated|prescribed/i.test(alt.typical)) {
    return "Typically Expected";
  }
  if (/may|can|optional/i.test(alt.typical)) return "May Apply";
  return "Confirm with brand";
}

function buildNotesToConfirm(alt) {
  if (hasVal(alt.notes)) return alt.notes;
  const parts = [];
  if (hasVal(alt.appliesTo)) parts.push(`Applies to: ${alt.appliesTo}`);
  if (hasVal(alt.flexNotes)) parts.push(alt.flexNotes);
  if (parts.length) return parts.join(" ");
  if (hasVal(alt.owner)) {
    return "Confirm scope, standards manual vintage, and agreement terms with Choice development before underwriting.";
  }
  return "Confirm current brand standards and agreement vintage with transaction documents.";
}

/** Normalize Radisson row body to v27B column-complete shape. */
export function normalizeRadissonRequirementBody(body) {
  const alt = parseRadissonRequirementAltColumns(body);
  const incompleteReasons = [];
  if (!hasVal(alt.typical)) incompleteReasons.push("missing_typical_consideration");
  if (!hasVal(alt.owner)) incompleteReasons.push("missing_owner_planning_consideration");
  if (!hasVal(alt.status) && !hasVal(alt.appliesTo)) {
    incompleteReasons.push("missing_typical_status_or_applies_to");
  }
  if (!hasVal(alt.notes) && !hasVal(alt.flexNotes) && !hasVal(alt.appliesTo)) {
    incompleteReasons.push("missing_notes_flexibility_or_applies_to");
  }

  const typicalStatus = mapTypicalStatus(alt);
  const notesToConfirm = buildNotesToConfirm(alt);

  const normalizedBody = [
    `Typical consideration: ${alt.typical}`,
    `Owner planning consideration: ${alt.owner}`,
    `Typical status: ${typicalStatus}`,
    `Notes to confirm: ${notesToConfirm}`,
  ].join("\n");

  const beforeCols = parseRequirementColumns(body);
  const afterCols = parseRequirementColumns(normalizedBody);
  const usefulContentExists = hasVal(alt.typical) && hasVal(alt.owner);
  const wouldBeComplete = requirementRowHasRequiredColumns({
    title: "probe",
    body: normalizedBody,
  });

  return {
    normalizedBody,
    beforeCols,
    afterCols,
    altColumns: alt,
    incompleteReasons,
    usefulContentExists,
    wouldBeComplete,
    structureChanges: [
      hasVal(alt.appliesTo) ? "preserved_applies_to_in_notes_to_confirm" : null,
      hasVal(alt.flexNotes) ? "mapped_flexibility_exception_notes_to_notes_to_confirm" : null,
      !hasVal(beforeCols.notes) && hasVal(afterCols.notes)
        ? "added_notes_to_confirm_from_radisson_alt_labels"
        : null,
    ].filter(Boolean),
  };
}

export function diagnoseRadissonRequirementRow(row) {
  const title = nz(row?.title);
  const body = nz(row?.body);
  const beforeComplete = requirementRowHasRequiredColumns(row);
  const norm = normalizeRadissonRequirementBody(body);
  const v27bIncompleteBecause = [];
  const beforeCols = parseRequirementColumns(body);
  if (!beforeCols.notes) v27bIncompleteBecause.push("missing_notes_to_confirm_label");
  if (!beforeCols.status) v27bIncompleteBecause.push("missing_typical_status_label");
  if (beforeCols.status && !beforeCols.notes) {
    v27bIncompleteBecause.push("radisson_uses_flexibility_exception_notes_instead_of_notes_to_confirm");
  }

  return {
    recordId: row?.recordId || null,
    title,
    currentBody: body,
    currentColumns: beforeCols,
    altColumns: norm.altColumns,
    v27bIncompleteBecause,
    usefulContentExists: norm.usefulContentExists,
    beforeComplete,
    afterComplete: norm.wouldBeComplete,
    normalizedBody: norm.normalizedBody,
    structureChanges: norm.structureChanges,
  };
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

function scanStandardsCopySafety(brandName, text) {
  const issues = [];
  const combined = nz(text);
  if (COMPANY_VALIDATION_BLOCK_RE.test(combined)) issues.push("company_validation_language");
  if (WRONG_BRAND_COPY_RE.test(combined)) issues.push("wrong_brand_copy");
  if (RAW_LEGAL_FRAGMENT_RE.test(combined)) issues.push("raw_fdd_legal_fragment");
  if (INTERNAL_EXTRACTION_RE.test(combined)) issues.push("internal_extraction_language");
  return issues;
}

const GOVERNANCE_SLOTS = [LAST_REVIEWED_SLOT, SOURCE_CONFIDENCE_SLOT];

function governanceLanguageThin(body) {
  return (
    hasVal(body) &&
    !/founder-reviewed|owner-planning guidance/i.test(body) &&
    /confirm with brand/i.test(body)
  );
}

function presentationFieldsForGovernance(slotKey, body, sort, brandRecordId, brandName) {
  return {
    "Slot Key": slotKey,
    Title: "",
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort,
  };
}

function resolveTarget(brandArg) {
  const normalized = nz(brandArg || TARGET_BRAND_SLUG).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(normalized) && normalized !== TARGET_BRAND_SLUG) {
    throw new Error(`Brand ${normalized} is protected and cannot be modified by v27D`);
  }
  if (normalized !== TARGET_BRAND_SLUG && brandArg !== TARGET_RECORD_ID) {
    throw new Error(`v27D supports Radisson by Choice only (${TARGET_BRAND_SLUG})`);
  }
  const meta = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === TARGET_BRAND_SLUG);
  if (!meta) throw new Error("Could not resolve Radisson brand target");
  return meta;
}

export function buildApplyCommand() {
  return `npm run brand-explorer-radisson-standards-requirement-normalization-writer -- --brand ${TARGET_BRAND_SLUG} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_LEGAL}`;
}

export async function buildBrandExplorerRadissonStandardsRequirementNormalizationWriterReport(options = {}) {
  const target = resolveTarget(options.brandIdOrName);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const apply = Boolean(options.apply);
  const approveBatch = Boolean(options.approveBatch);
  const founderReviewed = Boolean(options.founderReviewed);
  const noLegalOrCompanyConfirmed = Boolean(options.noLegalOrCompanyConfirmed);

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(target.recordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(target.name)}')`
  );

  const presentationRows = presentationRaw.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    active: rec.fields?.Active,
    brandName: nz(rec.fields?.["Brand Name"]),
  }));

  const foreignBrandRows = presentationRows.filter(
    (r) => r.brandName && r.brandName.toLowerCase() !== target.name.toLowerCase()
  );

  const requirementRowsLive = presentationRows.filter((r) => r.slotKey === REQUIREMENT_SLOT);
  const introRowLive = presentationRows.find((r) => r.slotKey === INTRO_SLOT);
  const lastReviewedLive = presentationRows.find((r) => r.slotKey === LAST_REVIEWED_SLOT);
  const sourceConfidenceLive = presentationRows.find((r) => r.slotKey === SOURCE_CONFIDENCE_SLOT);

  const rowDiagnosis = requirementRowsLive.map(diagnoseRadissonRequirementRow);

  const requirementUpdates = [];
  const copySafetyFindings = [];

  for (const row of requirementRowsLive) {
    const diagnosis = diagnoseRadissonRequirementRow(row);
    const issues = scanStandardsCopySafety(
      target.name,
      `${row.title}\n${diagnosis.normalizedBody}`
    );
    if (issues.length) {
      copySafetyFindings.push({ recordId: row.recordId, title: row.title, issues });
    }
    if (!bodiesMatch(row.body, diagnosis.normalizedBody)) {
      requirementUpdates.push({
        recordId: row.recordId,
        title: row.title,
        sortOrder: row.sortOrder,
        action: "update",
        currentBody: row.body,
        proposedBody: diagnosis.normalizedBody,
        beforeColumns: diagnosis.currentColumns,
        afterColumns: parseRequirementColumns(diagnosis.normalizedBody),
        structureChanges: diagnosis.structureChanges,
        fields: {
          "Slot Key": REQUIREMENT_SLOT,
          Title: row.title,
          Body: diagnosis.normalizedBody,
          "Brand Name": target.name,
          Active: true,
          "Sort Order": row.sortOrder ?? 0,
        },
      });
    }
  }

  const governanceTargets = [
    {
      slotKey: LAST_REVIEWED_SLOT,
      live: lastReviewedLive,
      proposedBody: STANDARDS_LAST_REVIEWED_BODY,
    },
    {
      slotKey: SOURCE_CONFIDENCE_SLOT,
      live: sourceConfidenceLive,
      proposedBody: STANDARDS_SOURCE_CONFIDENCE_BODY,
    },
  ];

  const governanceWouldCreate = [];
  const governanceWouldUpdate = [];
  const governanceMatched = [];

  for (const g of governanceTargets) {
    const thin = !g.live || governanceLanguageThin(g.live.body);
    const needsChange = !g.live || thin || !bodiesMatch(g.live?.body || "", g.proposedBody);
    if (!g.live && needsChange) {
      governanceWouldCreate.push({
        slotKey: g.slotKey,
        proposedBody: g.proposedBody,
        fields: presentationFieldsForGovernance(g.slotKey, g.proposedBody, 0, target.recordId, target.name),
      });
    } else if (g.live && needsChange && !bodiesMatch(g.live.body, g.proposedBody)) {
      governanceWouldUpdate.push({
        recordId: g.live.recordId,
        slotKey: g.slotKey,
        currentBody: g.live.body,
        proposedBody: g.proposedBody,
        fields: {
          "Slot Key": g.slotKey,
          Title: "",
          Body: g.proposedBody,
          "Brand Name": target.name,
          Active: true,
          "Sort Order": g.live.sortOrder ?? 0,
        },
      });
    } else if (g.live) {
      governanceMatched.push({ recordId: g.live.recordId, slotKey: g.slotKey });
    }
  }

  const projectedRequirementRows = requirementRowsLive.map((row) => {
    const update = requirementUpdates.find((u) => u.recordId === row.recordId);
    return {
      recordId: row.recordId,
      slotKey: REQUIREMENT_SLOT,
      title: row.title,
      body: update ? update.proposedBody : row.body,
    };
  });

  const brandShape = {
    id: target.recordId,
    recordId: target.recordId,
    name: target.name,
    parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    brandExplorer: {
      blocks: [
        ...presentationRows
          .filter((r) => r.active !== false && !GOVERNANCE_SLOTS.includes(r.slotKey) && r.slotKey !== REQUIREMENT_SLOT)
          .map((r) => ({
            recordId: r.recordId,
            slotKey: r.slotKey,
            title: r.title,
            body: r.body,
          })),
        ...projectedRequirementRows.map((r) => ({
          recordId: r.recordId,
          slotKey: r.slotKey,
          title: r.title,
          body: r.body,
        })),
        { slotKey: LAST_REVIEWED_SLOT, title: "", body: STANDARDS_LAST_REVIEWED_BODY },
        { slotKey: SOURCE_CONFIDENCE_SLOT, title: "", body: STANDARDS_SOURCE_CONFIDENCE_BODY },
      ],
    },
  };

  const projectedCompleteCount = projectedRequirementRows.filter((r) =>
    requirementRowHasRequiredColumns(r)
  ).length;

  const projectedApproval = evaluateStandardsDetailReadinessGeneralized(
    brandShape,
    projectedRequirementRows
  );

  const contractBefore = await buildBrandExplorerRequiredSectionPopulationContractReport({
    brandIdOrName: target.slug,
  }).catch(() => ({ readinessScore: 88, brandExplorerRequiredSectionsReady: false }));

  const applyBlockers = [];
  if (!introRowLive || !hasVal(introRowLive.body)) applyBlockers.push("missing_standards_intro");
  if (requirementRowsLive.length < MIN_REQUIREMENT_ROWS_GENERAL) {
    applyBlockers.push(`insufficient_requirement_rows:${requirementRowsLive.length}`);
  }
  if (projectedCompleteCount < MIN_REQUIREMENT_ROWS_GENERAL) {
    applyBlockers.push(
      `incomplete_requirement_columns_after_normalization:${projectedCompleteCount}<${MIN_REQUIREMENT_ROWS_GENERAL}`
    );
  }
  if (copySafetyFindings.length) {
    applyBlockers.push(`copy_safety:${copySafetyFindings.map((f) => f.title).join(",")}`);
  }
  if (foreignBrandRows.length) applyBlockers.push(`foreign_brand_rows:${foreignBrandRows.length}`);
  if (target.recordId !== TARGET_RECORD_ID) applyBlockers.push("brand_identity_unresolved");

  const proposedGovernanceCombined = `${STANDARDS_LAST_REVIEWED_BODY}\n${STANDARDS_SOURCE_CONFIDENCE_BODY}`;
  if (COMPANY_VALIDATION_BLOCK_RE.test(proposedGovernanceCombined)) {
    applyBlockers.push("governance_implies_company_validation");
  }

  const applyGatesReady = apply && approveBatch && founderReviewed && noLegalOrCompanyConfirmed;
  const hasWork =
    requirementUpdates.length > 0 ||
    governanceWouldCreate.length > 0 ||
    governanceWouldUpdate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const created = [];
    const updated = [];
    const errors = [];

    for (const row of requirementUpdates) {
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
          slotKey: REQUIREMENT_SLOT,
          recordId: row.recordId,
          title: row.title,
          message: json.error?.message || res.status,
        });
      } else {
        updated.push({ recordId: json.id, slotKey: REQUIREMENT_SLOT, title: row.title });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const row of governanceWouldUpdate) {
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

    for (const row of governanceWouldCreate) {
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
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults = { created: [], updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const dryRunClean = applyBlockers.length === 0 && projectedApproval.ready;

  const report = {
    writerVersion: WRITER_VERSION,
    v27DWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      slug: target.slug,
      name: target.name,
      recordId: target.recordId,
      parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    },
    protectedBrandsUntouched: PROTECTED_BRAND_SLUGS,
    airtableModified,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidationDateUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    diagnosis: {
      requirementRowCount: requirementRowsLive.length,
      completeBefore: requirementRowsLive.filter(requirementRowHasRequiredColumns).length,
      completeAfterProjected: projectedCompleteCount,
      introPresent: Boolean(introRowLive?.body),
      lastReviewedPresent: Boolean(lastReviewedLive),
      sourceConfidencePresent: Boolean(sourceConfidenceLive),
      contractScoreBefore: contractBefore.readinessScore,
      contractReadyBefore: contractBefore.brandExplorerRequiredSectionsReady,
      currentBlockers: projectedApproval.ready ? [] : applyBlockers,
    },
    rowDiagnosis,
    requirementRowsToUpdate: requirementUpdates,
    governanceRowsToCreate: governanceWouldCreate,
    governanceRowsToUpdate: governanceWouldUpdate,
    governanceRowsMatched: governanceMatched,
    copySafetyFindings,
    applyBlockers,
    projectedStandardDetailReady: projectedApproval.ready,
    projectedContractScore: projectedApproval.ready ? 100 : contractBefore.readinessScore,
    projectedContractReady: projectedApproval.ready,
    projectedApproval,
    dryRunClean,
    exactApplyCommand: buildApplyCommand(),
    exactDryRunCommand: `npm run brand-explorer-radisson-standards-requirement-normalization-writer -- --brand ${TARGET_BRAND_SLUG} --dry-run`,
    applyResults,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Radisson Standards Requirement Normalization Writer v${WRITER_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- v27D exists: **${report.v27DWriterExists ? "yes" : "no"}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Complete rows before: **${report.diagnosis.completeBefore}/${report.diagnosis.requirementRowCount}**`);
  lines.push(`- Complete rows after (projected): **${report.diagnosis.completeAfterProjected}/${report.diagnosis.requirementRowCount}**`);
  lines.push(`- Contract before: **${report.diagnosis.contractScoreBefore}** → after: **${report.projectedContractScore}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Row diagnosis");
  for (const row of report.rowDiagnosis) {
    lines.push(`### ${row.title}`);
    lines.push(`- v27B incomplete because: ${row.v27bIncompleteBecause.join("; ")}`);
    lines.push(`- Useful content: ${row.usefulContentExists ? "yes" : "no"}`);
    lines.push(`- After normalization: ${row.afterComplete ? "column-complete" : "still incomplete"}`);
    if (row.structureChanges.length) {
      lines.push(`- Structure changes: ${row.structureChanges.join("; ")}`);
    }
  }
  lines.push("");
  lines.push("## Requirement rows to update");
  lines.push(`Count: ${report.requirementRowsToUpdate.length}`);
  lines.push("");
  lines.push("## Governance rows");
  lines.push(`- Create: ${report.governanceRowsToCreate.map((r) => r.slotKey).join(", ") || "none"}`);
  lines.push(`- Update: ${report.governanceRowsToUpdate.map((r) => r.slotKey).join(", ") || "none"}`);
  lines.push("");
  if (report.applyBlockers.length) {
    lines.push("## Apply blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
    lines.push("");
  }
  lines.push("## Exact apply command");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  return lines.join("\n");
}

export function buildBrandExplorerRadissonStandardsRequirementNormalizationWriterMarkdown(report) {
  return report.markdown || buildMarkdown(report);
}
