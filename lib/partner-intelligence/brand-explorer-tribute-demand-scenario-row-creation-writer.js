/**
 * Brand Explorer Tribute Demand Scenario Row Creation Writer v25C-5B.
 *
 * Creates/updates commercial.demand presentation rows for Tribute Portfolio from
 * the v25C-5A founder-review package. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-tribute-demand-scenario-row-creation-writer-v25C-5B.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  DEMAND_SLOT,
  DEMAND_MINIMUM,
  DEMAND_TARGET,
  REPORT_JSON_NAME as REVIEW_PACKAGE_JSON,
  buildFlattenedDemandRowTargets,
  demandIsComplete,
} from "./brand-explorer-tribute-demand-scenario-row-review-package.js";

export const WRITER_VERSION = "25C-5B";
export const REPORT_JSON_NAME = "brand-explorer-tribute-demand-scenario-row-creation-writer.json";
export const REPORT_MD_NAME = "brand-explorer-tribute-demand-scenario-row-creation-writer.md";
export const DOC_MD_NAME = "brand-explorer-tribute-demand-scenario-row-creation-writer-v25C-5B.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v25C-5B-demand-scenario-rows";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-demand-scenario-row-copy";
export const APPLY_FLAG_CREATE = "--approve-brand-explorer-v25C-5B-row-create";

export const TARGET_SLOT = DEMAND_SLOT;
export const EXPECTED_ROW_COUNT = 7;
export const EXISTING_UPDATE_RECORD_ID = "recKbyC6vr05rrnzS";
export const EXISTING_UPDATE_TITLE_BEFORE = "Resort & leisure conversion";
export const EXISTING_UPDATE_TITLE_AFTER = "Resort / Leisure Repositioning";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FORBIDDEN_REFERENCE_TITLES = new Set([
  "Gateway Urban",
  "Regional & Secondary Upscale",
  "Corporate-Led Urban",
  "Resort / Coastal Leisure",
  "Conversion / Repositioning",
  "Pure Economy / Highway",
  "Independent / Soft-Brand Conversion",
  "Heritage & Experiential Repositioning",
]);

const FORBIDDEN_MARRIOTT_VALIDATION = /marriott\s+validated|validated\s+by\s+marriott|company-validated\s+demand/i;
const UNSUPPORTED_STATISTICS =
  /\b\d{1,3}(?:\.\d+)?%\b|\b\d[\d,]*\s*(?:million|billion|thousand|m\+|k\+)\b|\$\s?\d[\d,]+/i;

const PROTECTED_SLOT_PREFIXES = [
  "loyalty.",
  "standards.",
  "footprint.openings",
  "footprint.momentum",
  "footprint.portfolio_mix",
  "overview.portfolio_context",
];

const ALLOWED_BODY_PILLS = new Set(["Strong", "Moderate–strong", "Not a fit"]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-tribute-demand-scenario-row-review-package.md",
  "reports/brand-explorer-tribute-demand-scenario-row-review-package.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "lib/partner-intelligence/brand-explorer-tribute-demand-scenario-row-review-package.js",
  "live Tribute Brand Explorer Presentation rows",
  "live Tribute Partner Facts",
  "live Tribute Source Library records",
  "live Curio/Kimpton/Radisson/Ascend commercial.demand rows",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function normalizeTitle(v) {
  return nz(v);
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
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

function readJsonIfExists(relPath) {
  const full = path.join(ROOT, relPath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

function normalizePresentationRows(records) {
  return (records || [])
    .map((rec) => {
      const f = rec.fields || {};
      return {
        recordId: rec.id,
        slotKey: nz(f["Slot Key"] || f.slot_key),
        title: nz(f.Title),
        body: nz(f.Body),
        brandName: nz(f["Brand Name"]),
        active: f.Active,
        sortOrder: f["Sort Order"],
        caseSummaryOverview: nz(f["Case Summary Overview"]),
        caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
        caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
        imageCount: Array.isArray(f.Image) ? f.Image.length : 0,
      };
    })
    .filter((r) => r.slotKey);
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return TRIBUTE_RECORD_ID;
  }
  return nz(raw);
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function copiesReferenceBrandTitle(title) {
  return FORBIDDEN_REFERENCE_TITLES.has(nz(title));
}

function containsUnsupportedStatistics(text) {
  return UNSUPPORTED_STATISTICS.test(nz(text));
}

function impliesMarriottValidation(text) {
  return FORBIDDEN_MARRIOTT_VALIDATION.test(nz(text));
}

function isProtectedSlot(slotKey) {
  const key = nz(slotKey);
  return PROTECTED_SLOT_PREFIXES.some((prefix) => key === prefix || key.startsWith(prefix));
}

function fieldsMatch(a, b) {
  const keys = [
    "Slot Key",
    "Title",
    "Body",
    "Case Summary Overview",
    "Case Summary Owner Objective",
    "Case Summary Interpretation",
    "Sort Order",
    "Active",
  ];
  return keys.every((k) => normalizeBody(a[k]) === normalizeBody(b[k]));
}

function findLiveMatchByTitleSort(planned, liveRows) {
  return liveRows.find(
    (live) =>
      normalizeTitle(live.title) === normalizeTitle(planned.title) &&
      Number(live.sortOrder ?? -1) === Number(planned.sort)
  );
}

function validateDemandRow(planned) {
  const f = planned.fields || {};
  const missing = [];
  if (!normalizeTitle(f.Title)) missing.push("title");
  if (!normalizeBody(f.Body)) missing.push("body");
  if (!ALLOWED_BODY_PILLS.has(normalizeBody(f.Body))) missing.push("invalid_body_pill");
  if (!normalizeBody(f["Case Summary Overview"])) missing.push("description");
  if (!normalizeBody(f["Case Summary Owner Objective"])) missing.push("owner_implication");
  if (!normalizeBody(f["Case Summary Interpretation"])) missing.push("demand_logic");
  if (nz(f["Slot Key"]) !== DEMAND_SLOT) missing.push("slot_key");

  const combined = [
    f.Title,
    f.Body,
    f["Case Summary Overview"],
    f["Case Summary Owner Objective"],
    f["Case Summary Interpretation"],
  ].join(" ");

  const violations = [];
  if (copiesReferenceBrandTitle(f.Title)) violations.push("copies_reference_brand_title");
  if (containsUnsupportedStatistics(combined)) violations.push("unsupported_statistics");
  if (impliesMarriottValidation(combined)) violations.push("implies_marriott_validation");

  return { missing, violations };
}

function rowContractCompleteFromFields(fields) {
  return demandIsComplete({
    title: fields.Title,
    body: fields.Body,
    caseSummaryOwnerObjective: fields["Case Summary Owner Objective"],
    caseSummaryInterpretation: fields["Case Summary Interpretation"],
  });
}

function buildUpdateFields(planned) {
  const f = { ...planned.fields };
  delete f.Brand;
  return f;
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-tribute-demand-scenario-row-creation-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_CREATE}`;
}

export async function buildBrandExplorerTributeDemandScenarioRowCreationWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  createApproved = false,
} = {}) {
  const brandRecordId = normalizeBrandInput(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-5B pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const reviewPackage = readJsonIfExists(`reports/${REVIEW_PACKAGE_JSON}`);
  if (!reviewPackage?.v25C5AReviewPackageExists) {
    throw new Error(
      "v25C-5A review package missing — run brand-explorer-tribute-demand-scenario-row-review-package first"
    );
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const targetRows = buildFlattenedDemandRowTargets(brandRecordId, BRAND_NAME);
  if (targetRows.length !== EXPECTED_ROW_COUNT) {
    throw new Error(`Expected ${EXPECTED_ROW_COUNT} target rows, got ${targetRows.length}`);
  }

  const updateTarget = targetRows.find((r) => r.updatesExistingRow);
  const createTargets = targetRows.filter((r) => !r.updatesExistingRow);

  if (!updateTarget || createTargets.length !== 6) {
    throw new Error("v25C-5A package must include 1 update row and 6 create rows");
  }

  const applyBlockers = [];
  const rowValidation = [];

  for (const planned of targetRows) {
    const v = validateDemandRow(planned);
    rowValidation.push({
      title: planned.title,
      sort: planned.sort,
      updatesExistingRow: Boolean(planned.updatesExistingRow),
      missing: v.missing,
      violations: v.violations,
      contractComplete: rowContractCompleteFromFields(planned.fields),
    });
    if (v.missing.length) {
      applyBlockers.push(`missing_fields:${planned.title}:${v.missing.join(",")}`);
    }
    if (v.violations.length) {
      applyBlockers.push(`violations:${planned.title}:${v.violations.join(",")}`);
    }
  }

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);

  const demandLive = presentationRows.filter((r) => r.slotKey === DEMAND_SLOT);
  const protectedRowsSnapshot = presentationRows
    .filter((r) => isProtectedSlot(r.slotKey))
    .map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title,
      sortOrder: r.sortOrder,
    }));

  const existingUpdateRow = demandLive.find((r) => r.recordId === EXISTING_UPDATE_RECORD_ID);
  if (!existingUpdateRow) {
    applyBlockers.push(`existing_update_row_not_found:${EXISTING_UPDATE_RECORD_ID}`);
  }

  const resortLeisureDuplicates = demandLive.filter(
    (r) =>
      normalizeTitle(r.title) === normalizeTitle(EXISTING_UPDATE_TITLE_AFTER) &&
      r.recordId !== EXISTING_UPDATE_RECORD_ID
  );
  if (resortLeisureDuplicates.length > 0) {
    applyBlockers.push(
      `resort_leisure_duplicate_would_exist:${resortLeisureDuplicates.map((r) => r.recordId).join(",")}`
    );
  }

  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const rowsMatched = [];
  const exactCreatePayloads = [];
  const exactUpdatePayloads = [];

  const updateFields = buildUpdateFields(updateTarget);
  if (existingUpdateRow) {
    const liveFields = {
      "Slot Key": existingUpdateRow.slotKey,
      Title: existingUpdateRow.title,
      Body: existingUpdateRow.body,
      "Case Summary Overview": existingUpdateRow.caseSummaryOverview,
      "Case Summary Owner Objective": existingUpdateRow.caseSummaryOwnerObjective,
      "Case Summary Interpretation": existingUpdateRow.caseSummaryInterpretation,
      "Sort Order": existingUpdateRow.sortOrder,
      Active: existingUpdateRow.active ?? true,
    };
    if (fieldsMatch(liveFields, updateFields)) {
      rowsMatched.push({
        recordId: EXISTING_UPDATE_RECORD_ID,
        title: existingUpdateRow.title,
        action: "matched_update",
      });
    } else {
      rowsWouldUpdate.push({
        recordId: EXISTING_UPDATE_RECORD_ID,
        action: "update",
        currentTitle: existingUpdateRow.title,
        proposedTitle: updateTarget.title,
        currentBody: existingUpdateRow.body,
        proposedBody: updateTarget.body,
        fields: updateFields,
      });
      exactUpdatePayloads.push({
        table: PRESENTATION_TABLE,
        recordId: EXISTING_UPDATE_RECORD_ID,
        fields: updateFields,
      });
    }
  }

  for (const planned of createTargets) {
    const match = findLiveMatchByTitleSort(planned, demandLive);
    if (match) {
      const liveFields = {
        "Slot Key": match.slotKey,
        Title: match.title,
        Body: match.body,
        "Case Summary Overview": match.caseSummaryOverview,
        "Case Summary Owner Objective": match.caseSummaryOwnerObjective,
        "Case Summary Interpretation": match.caseSummaryInterpretation,
        "Sort Order": match.sortOrder,
        Active: match.active ?? true,
      };
      if (fieldsMatch(liveFields, planned.fields)) {
        rowsMatched.push({
          recordId: match.recordId,
          title: match.title,
          sort: planned.sort,
          action: "matched_create",
        });
      } else {
        rowsWouldUpdate.push({
          recordId: match.recordId,
          action: "update_existing_create_target",
          currentTitle: match.title,
          proposedTitle: planned.title,
          fields: buildUpdateFields(planned),
          note: "Existing row matches title/sort but fields differ — will patch to v25C-5A copy",
        });
        exactUpdatePayloads.push({
          table: PRESENTATION_TABLE,
          recordId: match.recordId,
          fields: buildUpdateFields(planned),
        });
      }
      continue;
    }

    rowsWouldCreate.push({
      slotKey: planned.slotKey,
      title: planned.title,
      body: planned.body,
      sort: planned.sort,
      themeKey: planned.themeKey,
      action: "create",
      fields: planned.fields,
    });
    exactCreatePayloads.push({
      table: PRESENTATION_TABLE,
      fields: planned.fields,
    });
  }

  const projectedDemandCount =
    demandLive.length + rowsWouldCreate.length - (rowsWouldUpdate.some((r) => r.action === "update_existing_create_target") ? 0 : 0);

  const projectedCompleteRows = targetRows.filter((r) => rowContractCompleteFromFields(r.fields)).length;
  const demandMeetsMinimumAfterApply = projectedCompleteRows >= DEMAND_MINIMUM;
  const demandMeetsTargetParityAfterApply = projectedCompleteRows >= DEMAND_TARGET;

  if (!demandMeetsMinimumAfterApply) {
    applyBlockers.push(`fewer_than_${DEMAND_MINIMUM}_complete_rows_after_apply`);
  }

  if (projectedDemandCount > EXPECTED_ROW_COUNT && rowsWouldCreate.length > 0) {
    const extra = projectedDemandCount - EXPECTED_ROW_COUNT;
    if (extra > 0 && demandLive.length >= EXPECTED_ROW_COUNT) {
      applyBlockers.push(`demand_row_count_exceeds_target:${projectedDemandCount}>${EXPECTED_ROW_COUNT}`);
    }
  }

  const resortLeisureUpdatedNotDuplicated =
    Boolean(existingUpdateRow) &&
    resortLeisureDuplicates.length === 0 &&
    (rowsWouldUpdate.some((r) => r.recordId === EXISTING_UPDATE_RECORD_ID) ||
      rowsMatched.some((r) => r.recordId === EXISTING_UPDATE_RECORD_ID));

  if (
    rowsWouldCreate.some(
      (r) => normalizeTitle(r.title) === normalizeTitle(EXISTING_UPDATE_TITLE_AFTER)
    )
  ) {
    applyBlockers.push("resort_leisure_would_be_created_as_duplicate");
  }

  const applyGatesReady = apply && approveBatch && founderReviewed && createApproved;
  const canApply =
    applyGatesReady &&
    applyBlockers.length === 0 &&
    (rowsWouldCreate.length > 0 || rowsWouldUpdate.length > 0);

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
          recordId: row.recordId,
          title: row.proposedTitle || row.currentTitle,
          message: json.error?.message || res.status,
        });
      } else {
        updated.push({
          recordId: json.id,
          title: row.proposedTitle || row.currentTitle,
          action: row.action,
        });
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
          title: row.title,
          message: json.error?.message || res.status,
        });
      } else {
        created.push({
          recordId: json.id,
          title: row.title,
          sort: row.sort,
        });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    airtableModified = created.length > 0 || updated.length > 0;
    applyResults = { created, updated, errors };

    const brandBasicsAfter = await fetchBrandBasics(brandRecordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  } else if (apply && applyBlockers.length > 0) {
    applyResults = { created: [], updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const unsupportedStatisticsExcluded = rowValidation.every((r) => !r.violations.includes("unsupported_statistics"));
  const referenceBrandCopyExcluded = rowValidation.every(
    (r) => !r.violations.includes("copies_reference_brand_title")
  );

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C5BWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: "tribute-portfolio",
    },
    sourcePackage: REVIEW_PACKAGE_JSON,
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-tribute-demand-scenario-row-creation-writer.js",
      "scripts/brand-explorer-tribute-demand-scenario-row-creation-writer.mjs",
      "docs/data-intelligence/brand-explorer-tribute-demand-scenario-row-creation-writer-v25C-5B.md",
      "reports/brand-explorer-tribute-demand-scenario-row-creation-writer.md",
      "reports/brand-explorer-tribute-demand-scenario-row-creation-writer.json",
      "package.json",
    ],
    targetSlot: TARGET_SLOT,
    expectedRowCount: EXPECTED_ROW_COUNT,
    existingDemandRowsFound: demandLive.map((r) => ({
      recordId: r.recordId,
      title: r.title,
      body: r.body,
      sortOrder: r.sortOrder,
      contractComplete: demandIsComplete({
        title: r.title,
        body: r.body,
        caseSummaryOwnerObjective: r.caseSummaryOwnerObjective,
        caseSummaryInterpretation: r.caseSummaryInterpretation,
      }),
    })),
    existingDemandRowCount: demandLive.length,
    existingContractCompleteCount: demandLive.filter((r) =>
      demandIsComplete({
        title: r.title,
        body: r.body,
        caseSummaryOwnerObjective: r.caseSummaryOwnerObjective,
        caseSummaryInterpretation: r.caseSummaryInterpretation,
      })
    ).length,
    rowValidation,
    rowsWouldCreate,
    rowsWouldUpdate,
    rowsMatched,
    exactCreatePayloads,
    exactUpdatePayloads,
    exactProposedPayloads: {
      create: exactCreatePayloads,
      update: exactUpdatePayloads,
    },
    resortLeisureUpdatedNotDuplicated,
    existingUpdateRecordId: EXISTING_UPDATE_RECORD_ID,
    existingUpdateTitleBefore: EXISTING_UPDATE_TITLE_BEFORE,
    existingUpdateTitleAfter: EXISTING_UPDATE_TITLE_AFTER,
    demandMeetsMinimumAfterApply,
    demandMeetsTargetParityAfterApply,
    projectedCompleteRowsAfterApply: projectedCompleteRows,
    projectedDemandRowCountAfterApply: demandLive.length + rowsWouldCreate.length,
    unsupportedStatisticsExcluded,
    referenceBrandCopyExcluded,
    loyaltyRowsUntouched: true,
    openingsRowsUntouched: true,
    momentumRowsUntouched: true,
    standardsRowsUntouched: true,
    portfolioMixContextRowsUntouched: true,
    brandedResidencesUntouched: true,
    imagesUntouched: true,
    protectedRowsSnapshot,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      createApproved,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers: [...new Set(applyBlockers)],
    applyResults,
    exactApplyCommand: buildApplyCommand(),
    idempotentAfterApply:
      rowsWouldCreate.length === 0 &&
      rowsWouldUpdate.length === 0 &&
      resortLeisureDuplicates.length === 0,
    doesNotDo: [
      "Modify loyalty, openings, momentum, standards, portfolio mix/context, or branded residences fields",
      "Change images on any presentation row",
      "Change Company Validated or Company Validation Date",
      "Copy Curio/Kimpton/Radisson/Ascend demand scenario titles",
      "Use unsupported market statistics",
      "Imply Marriott or company validation",
      "Create duplicate Resort / Leisure Repositioning row",
    ],
  };
}

export function buildBrandExplorerTributeDemandScenarioRowCreationWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Tribute Demand Scenario Row Creation Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Writer exists: **${report.v25C5BWriterExists ? "yes" : "no"}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- Source package: \`${report.sourcePackage}\``,
    "",
    "## Summary",
    "",
    `- Existing demand rows: **${report.existingDemandRowCount}** (${report.existingContractCompleteCount} contract-complete)`,
    `- Rows would create: **${report.rowsWouldCreate.length}**`,
    `- Rows would update: **${report.rowsWouldUpdate.length}**`,
    `- Rows matched (idempotent): **${report.rowsMatched.length}**`,
    `- Resort / Leisure updated not duplicated: **${report.resortLeisureUpdatedNotDuplicated ? "yes" : "no"}**`,
    `- Meets minimum (3+) after apply: **${report.demandMeetsMinimumAfterApply ? "yes" : "no"}**`,
    `- Meets target parity (6+) after apply: **${report.demandMeetsTargetParityAfterApply ? "yes" : "no"}**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Rows to update",
    "",
  ];

  if (report.rowsWouldUpdate.length === 0) {
    lines.push("- None (all matched)");
  } else {
    for (const r of report.rowsWouldUpdate) {
      lines.push(`- \`${r.recordId}\` · ${r.currentTitle || r.proposedTitle} → ${r.proposedTitle || r.currentTitle}`);
    }
  }

  lines.push("", "## Rows to create", "");
  if (report.rowsWouldCreate.length === 0) {
    lines.push("- None (all matched)");
  } else {
    for (const r of report.rowsWouldCreate) {
      lines.push(`- **${r.title}** · ${r.body} · sort ${r.sort}`);
    }
  }

  lines.push("", "## Apply command", "", "```bash", report.exactApplyCommand, "```");
  return lines.join("\n");
}
