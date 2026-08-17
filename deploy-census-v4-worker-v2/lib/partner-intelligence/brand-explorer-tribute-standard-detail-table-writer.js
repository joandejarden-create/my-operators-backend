/**
 * Brand Explorer Tribute Standard Detail Table Writer v25C-4B.
 *
 * Populates Tribute Portfolio standards.requirement owner planning table
 * (7 rows) plus standards.intro. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-tribute-standard-detail-table-writer-v25C-4B.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";

export const WRITER_VERSION = "25C-4B";
export const REPORT_JSON_NAME = "brand-explorer-tribute-standard-detail-table-writer.json";
export const REPORT_MD_NAME = "brand-explorer-tribute-standard-detail-table-writer.md";
export const DOC_MD_NAME = "brand-explorer-tribute-standard-detail-table-writer-v25C-4B.md";

export const APPLY_FLAG_BATCH = "--approve-brand-explorer-v25C-4B-tribute-standard-detail";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-tribute-standard-detail-copy";
export const APPLY_FLAG_NO_LEGAL = "--confirm-no-legal-or-curio-copy";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const REQUIREMENT_SLOT = "standards.requirement";
const INTRO_SLOT = "standards.intro";
const MIN_REQUIREMENT_ROWS = 7;

const PLACEHOLDER_RE =
  /no owner planning checklist is published in brand explorer presentation/i;

const CURIO_HILTON_COPY_RE =
  /curio collection|exactly like nothing else|hilton honors|hilton pms|hilton crs|tapestry collection by hilton|waldorf astoria|hilton garden inn|doubletree by hilton|canopy by hilton|signia by hilton|embassy suites|hampton by hilton|tru by hilton|spark by hilton|homewood suites|livsmart studios/i;

const RAW_FDD_LEGAL_RE =
  /item\s*19|franchise disclosure document|\bfdd\b|§\s*\d|hereinafter|pursuant to the agreement|exhibit\s+[a-z]\b|whereas\b/i;

const PROTECTED_SLOT_KEYS = new Set([
  "footprint.momentum",
  "footprint.openings",
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
  "valueOwners.scenario.1",
  "valueOwners.scenario.2",
  "valueOwners.scenario.3",
  "valueOwners.scenario.4",
]);

const PROTECTED_SLOT_PREFIXES = [
  /^loyalty\./i,
  /^footprint\.momentum/i,
  /^footprint\.openings/i,
  /^valueOwners\.scenario\./i,
];

const GOVERNANCE_LABELS = [
  "Founder-reviewed owner planning checklist",
  "Planning language only — not legal/FDD publication",
  "Not company-validated",
  "Not Marriott-validated",
];

export const STANDARDS_INTRO_BODY =
  "Tribute Portfolio standards vary by asset type, conversion path, market, operating model, and agreement vintage. Use this table as an owner planning checklist — confirm every requirement with current brand disclosure, design standards, and transaction documents before making capital commitments.";

function buildRequirementBody({ typical, owner, status, notes }) {
  return [
    `Typical consideration: ${typical}`,
    `Owner planning consideration: ${owner}`,
    `Typical status: ${status}`,
    `Notes to confirm: ${notes}`,
  ].join("\n");
}

export const TRIBUTE_REQUIREMENT_ROWS = [
  {
    title: "Breakfast / Morning Meal",
    sort: 10,
    typical:
      "Morning meal requirements may depend on asset type, market, operating model, and approved brand program.",
    owner:
      "Confirm whether the asset needs breakfast, café, restaurant, or other approved morning offering before underwriting staffing and FF&E.",
    status: "May Apply",
    notes:
      "Confirm current Tribute standards, market expectations, and brand approval requirements.",
  },
  {
    title: "F&B / Bar / Local Programming",
    sort: 11,
    typical:
      "Tribute properties often rely on local character, social spaces, and destination-relevant programming rather than a purely standardized prototype.",
    owner:
      "Confirm restaurant, bar, event, and local programming expectations early because they can materially affect capex, staffing, and operator scope.",
    status: "Typically Expected",
    notes:
      "Confirm whether the property's concept, operator, and market support the required experience level.",
  },
  {
    title: "Lobby / Public Space",
    sort: 12,
    typical:
      "Public areas should support independent lifestyle positioning, arrival experience, and locally relevant guest touchpoints.",
    owner:
      "Confirm lobby layout, FF&E, brand identity moments, and conversion flexibility before finalizing design scope.",
    status: "Typically Expected",
    notes: "Historic or adaptive-reuse assets may need project-specific design review.",
  },
  {
    title: "Guestroom Standards",
    sort: 13,
    typical:
      "Guestrooms should align with Tribute's lifestyle positioning while meeting Marriott system, quality, and guest-experience expectations.",
    owner:
      "Confirm bedding, bathroom, FF&E, technology, and room-mix implications before locking renovation budgets.",
    status: "Typically Expected",
    notes: "Conversion projects may need project-specific brand and design review.",
  },
  {
    title: "Signage / Exterior Identity",
    sort: 14,
    typical:
      "Exterior identity should balance Tribute affiliation with the property's independent character and local context.",
    owner:
      "Confirm monument signage, façade treatment, preservation limits, and permitting before committing to exterior scope.",
    status: "Typically Expected",
    notes: "Historic, urban, or mixed-use assets may require additional local approvals.",
  },
  {
    title: "Technology / Systems",
    sort: 15,
    typical:
      "Participation in Marriott systems can require PMS, CRS, loyalty, reporting, Wi-Fi, and distribution readiness.",
    owner:
      "Confirm system cutover timing, training, integration costs, and operating responsibilities with the brand and operator.",
    status: "Typically Expected",
    notes: "Technology scope may vary by agreement, market, and conversion timing.",
  },
  {
    title: "Training / QA / Brand Standards",
    sort: 16,
    typical:
      "Brand onboarding, training, quality assurance, and standards compliance should be planned before opening or conversion.",
    owner:
      "Confirm inspection timing, remediation responsibilities, operating procedures, and ongoing QA cadence.",
    status: "Typically Expected",
    notes:
      "Confirm current Marriott/Tribute review process, required training, and post-opening compliance obligations.",
  },
].map((row) => ({
  ...row,
  slotKey: REQUIREMENT_SLOT,
  body: buildRequirementBody(row),
}));

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-required-section-source-capture-package.md",
  "reports/tribute-portfolio-targeted-extract.md",
  "reports/tribute-portfolio-targeted-extract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Source Library records",
  "live Tribute Partner Facts",
  "live Tribute Brand Explorer Presentation rows",
  "live Curio/Kimpton/Radisson/Ascend standards.requirement rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-tribute-standard-detail-table-writer.js",
  "scripts/brand-explorer-tribute-standard-detail-table-writer.mjs",
  "docs/data-intelligence/brand-explorer-tribute-standard-detail-table-writer-v25C-4B.md",
  "reports/brand-explorer-tribute-standard-detail-table-writer.md",
  "reports/brand-explorer-tribute-standard-detail-table-writer.json",
  "package.json",
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

function isProtectedSlot(slotKey) {
  const key = nz(slotKey);
  if (PROTECTED_SLOT_KEYS.has(key)) return true;
  return PROTECTED_SLOT_PREFIXES.some((rx) => rx.test(key));
}

function containsForbiddenCopy(text) {
  return CURIO_HILTON_COPY_RE.test(text) || RAW_FDD_LEGAL_RE.test(text);
}

function parseRequirementColumns(body) {
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

function rowHasRequiredColumns(body) {
  const cols = parseRequirementColumns(body);
  return Boolean(cols.typical && cols.owner && cols.status && cols.notes);
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

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-tribute-standard-detail-table-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_BATCH} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_LEGAL}`;
}

function presentationFieldsForRow(slotKey, title, body, sort, brandRecordId) {
  return {
    "Slot Key": slotKey,
    Title: title,
    Body: body,
    "Brand Name": BRAND_NAME,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort,
  };
}

export async function buildBrandExplorerTributeStandardDetailTableWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noLegalOrCurioConfirmed = false,
} = {}) {
  const brandRecordId =
    nz(brandIdOrName).toLowerCase() === "tribute-portfolio" || !nz(brandIdOrName)
      ? TRIBUTE_RECORD_ID
      : nz(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-4B pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
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

  const standardsRequirementRows = presentationRows.filter((r) => r.slotKey === REQUIREMENT_SLOT);
  const standardsIntroRow = presentationRows.find((r) => r.slotKey === INTRO_SLOT);
  const standardsQuestionsRows = presentationRows.filter((r) => r.slotKey === "standards.questions");
  const legacyStandardsRows = presentationRows.filter((r) => /^standards\./i.test(r.slotKey));

  const placeholderWouldRender = standardsRequirementRows.length === 0;

  const standardDetailDiagnosis = {
    placeholderWouldRender,
    placeholderReason:
      standardsRequirementRows.length === 0
        ? "explorerCardRowsForSlot(brand, 'standards.requirement') returns 0 rows — renderStandardsOwnerConsiderations shows be-atelier-placeholder"
        : "standards.requirement rows exist",
    standardsRequirementRowCount: standardsRequirementRows.length,
    standardsIntroPresent: Boolean(standardsIntroRow),
    standardsQuestionsPresent: standardsQuestionsRows.length > 0,
    legacyStandardsSlots: legacyStandardsRows.map((r) => r.slotKey),
    referenceStructure:
      "Curio uses 7+ standards.requirement rows; Title = Requirement Area; Body = Typical consideration / Owner planning / Typical status / Notes to confirm (newline-separated)",
  };

  const introPlan = {
    slotKey: INTRO_SLOT,
    recordId: standardsIntroRow?.recordId || null,
    action: standardsIntroRow ? "update" : "create",
    fields: presentationFieldsForRow(INTRO_SLOT, "", STANDARDS_INTRO_BODY, 1, brandRecordId),
    needsUpdate:
      !standardsIntroRow || normalizeBody(standardsIntroRow.body) !== STANDARDS_INTRO_BODY,
  };

  const requirementPlans = TRIBUTE_REQUIREMENT_ROWS.map((pkg) => {
    const live = standardsRequirementRows.find(
      (r) => nz(r.title).toLowerCase() === pkg.title.toLowerCase()
    );
    return {
      slotKey: REQUIREMENT_SLOT,
      title: pkg.title,
      recordId: live?.recordId || null,
      action: live ? "update" : "create",
      proposedBody: pkg.body,
      currentBody: live?.body || "",
      fields: presentationFieldsForRow(REQUIREMENT_SLOT, pkg.title, pkg.body, pkg.sort, brandRecordId),
      needsUpdate:
        !live ||
        normalizeBody(live.body) !== pkg.body ||
        Number(live.sortOrder ?? -1) !== pkg.sort,
    };
  });

  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const applyBlockers = [];

  if (introPlan.needsUpdate) {
    if (introPlan.action === "create") rowsWouldCreate.push(introPlan);
    else rowsWouldUpdate.push(introPlan);
  }

  for (const plan of requirementPlans) {
    if (!plan.needsUpdate) continue;
    if (plan.action === "create") rowsWouldCreate.push(plan);
    else rowsWouldUpdate.push(plan);
  }

  const proposedTablePayload = {
    intro: introPlan.fields,
    requirements: requirementPlans.map((p) => ({
      requirementArea: p.title,
      title: p.title,
      sort: p.fields["Sort Order"],
      body: p.proposedBody,
      columns: parseRequirementColumns(p.proposedBody),
    })),
  };

  const allProposedCopy = [
    STANDARDS_INTRO_BODY,
    ...requirementPlans.map((p) => p.proposedBody),
  ].join("\n");

  const curioHiltonExcluded = !CURIO_HILTON_COPY_RE.test(allProposedCopy);
  const rawFddLegalExcluded = !RAW_FDD_LEGAL_RE.test(allProposedCopy);

  if (!curioHiltonExcluded) applyBlockers.push("curio_hilton_language_in_proposed_copy");
  if (!rawFddLegalExcluded) applyBlockers.push("raw_fdd_legal_fragment_in_proposed_copy");
  if (containsForbiddenCopy(allProposedCopy)) applyBlockers.push("forbidden_copy_pattern");

  for (const row of requirementPlans) {
    if (!rowHasRequiredColumns(row.proposedBody)) {
      applyBlockers.push(`missing_required_columns:${row.title}`);
    }
  }

  const finalRequirementCount = requirementPlans.length;
  if (finalRequirementCount < MIN_REQUIREMENT_ROWS) {
    applyBlockers.push(`insufficient_requirement_rows:${finalRequirementCount}<${MIN_REQUIREMENT_ROWS}`);
  }

  if (placeholderWouldRender && rowsWouldCreate.filter((r) => r.slotKey === REQUIREMENT_SLOT).length < MIN_REQUIREMENT_ROWS) {
    applyBlockers.push("placeholder_would_remain");
  }

  const applyGatesReady =
    apply && approveBatch && founderReviewed && noLegalOrCurioConfirmed;
  const hasWork = rowsWouldUpdate.length > 0 || rowsWouldCreate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const updated = [];
    const created = [];
    const errors = [];

    const allOps = [
      ...rowsWouldUpdate.map((r) => ({ ...r, method: "PATCH" })),
      ...rowsWouldCreate.map((r) => ({ ...r, method: "POST" })),
    ];

    for (const row of allOps) {
      const slotKey = row.slotKey || row.fields?.["Slot Key"];
      if (isProtectedSlot(slotKey)) {
        errors.push({ slotKey, message: "protected_slot_blocked" });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: row.method,
          body: JSON.stringify({ fields: row.fields, typecast: true }),
        },
        row.method === "PATCH" ? row.recordId : ""
      );
      if (!res.ok) {
        errors.push({
          recordId: row.recordId,
          slotKey,
          message: json.error?.message || res.status,
        });
      } else if (row.method === "PATCH") {
        updated.push({ recordId: row.recordId, slotKey, title: row.title });
      } else {
        created.push({ recordId: json.id, slotKey, title: row.title });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    airtableModified = (updated.length > 0 || created.length > 0) && errors.length === 0;
    applyResults = { updated, created, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(brandRecordId));
  } else if (apply) {
    applyResults = { updated: [], created: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C4BWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: { name: BRAND_NAME, recordId: brandRecordId, slug: "tribute-portfolio" },
    marriottValidationImplied: false,
    governanceLabels: [...GOVERNANCE_LABELS],
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    standardDetailDiagnosis,
    placeholderRenderingCause: standardDetailDiagnosis.placeholderReason,
    rowsWouldCreate: rowsWouldCreate.map((r) => ({
      slotKey: r.slotKey,
      title: r.title || r.fields?.Title,
      sort: r.fields?.["Sort Order"],
      action: r.action,
    })),
    rowsWouldUpdate: rowsWouldUpdate.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title || r.fields?.Title,
      action: r.action,
    })),
    proposedTablePayload,
    curioHiltonLanguageExcluded: curioHiltonExcluded,
    rawFddLegalFragmentsExcluded: rawFddLegalExcluded,
    requirementRowCount: finalRequirementCount,
    loyaltyRowsUntouched: true,
    openingsRowsUntouched: true,
    momentumRowsUntouched: true,
    valueScenarioRowsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      noLegalOrCurioConfirmed,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand("tribute-portfolio"),
    idempotentAfterApply: !hasWork,
    doesNotDo: [
      "Copy Curio/Hilton requirement language",
      "Publish raw FDD or legal excerpts",
      "Modify loyalty, openings, momentum, or value scenario rows",
      "Change Company Validated or imply Marriott validation",
    ],
  };
}

export function buildBrandExplorerTributeStandardDetailTableWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Tribute Standard Detail Table Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- v25C-4B exists: **${report.v25C4BWriterExists ? "yes" : "no"}**`,
    "",
    "## Diagnosis",
    "",
    `- Placeholder would render: **${report.standardDetailDiagnosis.placeholderWouldRender ? "yes" : "no"}**`,
    `- Cause: ${report.placeholderRenderingCause}`,
    `- Current standards.requirement rows: **${report.standardDetailDiagnosis.standardsRequirementRowCount}**`,
    "",
    "## Proposed table (7 rows)",
    "",
    "| Requirement Area | Typical Status |",
    "|------------------|----------------|",
  ];

  for (const row of report.proposedTablePayload.requirements) {
    lines.push(`| ${row.requirementArea} | ${row.columns.status} |`);
  }

  lines.push(
    "",
    "## Summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Rows would create | ${report.rowsWouldCreate.length} |`,
    `| Rows would update | ${report.rowsWouldUpdate.length} |`,
    `| Curio/Hilton language excluded | ${report.curioHiltonLanguageExcluded ? "yes" : "no"} |`,
    `| Raw FDD/legal excluded | ${report.rawFddLegalFragmentsExcluded ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Exact apply command",
    "",
    "```bash",
    report.exactApplyCommand,
    "```",
    ""
  );

  if (report.applyBlockers?.length) {
    lines.push("## Apply blockers", "");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
    lines.push("");
  }

  return lines.join("\n");
}
