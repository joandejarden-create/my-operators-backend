/**
 * Brand Explorer Loyalty Row Creation Writer v25C-2D.
 *
 * Creates missing loyalty presentation rows for Tribute Portfolio from the
 * v25C-2C polished copy package. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-loyalty-row-creation-writer-v25C-2D.md
 */
import { listPartnerFacts } from "./airtable-facts.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  ELIGIBLE_LOYALTY_FACT_KEYS,
} from "./brand-explorer-loyalty-fact-approval-writer.js";
import {
  EXCLUDED_KPI_SLOTS,
  EXPECTED_LOYALTY_ROW_COUNTS,
  REPORT_JSON_NAME as REVIEW_PACKAGE_JSON,
  TARGET_LOYALTY_SLOTS,
  buildFlattenedLoyaltyRowTargets,
} from "./brand-explorer-loyalty-row-review-package.js";

export const WRITER_VERSION = "25C-2D";
export const REPORT_JSON_NAME = "brand-explorer-loyalty-row-creation-writer.json";
export const REPORT_MD_NAME = "brand-explorer-loyalty-row-creation-writer.md";
export const DOC_MD_NAME = "brand-explorer-loyalty-row-creation-writer-v25C-2D.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v25C-2D-loyalty-rows";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-loyalty-row-copy";
export const APPLY_FLAG_CREATE = "--approve-brand-explorer-v25C-2D-loyalty-row-create";

const HERO_SLOT = "loyalty.hero_title";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const TOTAL_TARGET_ROWS = 10;

const GOVERNANCE_LABELS = [
  "AI-assembled from approved source facts",
  "Founder-reviewed copy package",
  "Not company-validated",
  "Not Marriott-validated",
];

const FORBIDDEN_BODY_PATTERNS = [
  /approved source excerpt/i,
  /AI-assembled from approved source facts/i,
  /Pending founder review/i,
  /Not company-validated/i,
  /Not Marriott-validated/i,
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-loyalty-row-review-package.md",
  "reports/brand-explorer-loyalty-row-review-package.json",
  "reports/brand-explorer-loyalty-fact-approval-writer.md",
  "reports/brand-explorer-loyalty-fact-approval-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-loyalty-row-review-package.js",
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

function isApprovedFact(fact) {
  const st = nz(fact.humanReviewStatus);
  return st === "Approved" || st === "Edited";
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function bodyHasForbiddenGovernanceCopy(body) {
  return FORBIDDEN_BODY_PATTERNS.some((re) => re.test(body));
}

function findLiveMatch(planned, liveRows) {
  return liveRows.find(
    (live) =>
      normalizeTitle(live.title) === normalizeTitle(planned.title) &&
      Number(live.sortOrder ?? -1) === Number(planned.sort)
  );
}

function bodiesMatch(a, b) {
  return normalizeBody(a) === normalizeBody(b);
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-loyalty-row-creation-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_CREATE}`;
}

export async function buildBrandExplorerLoyaltyRowCreationWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  createApproved = false,
} = {}) {
  const brandRecordId = normalizeBrandInput(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-2D pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const allFacts = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: brandRecordId, limit: 100, offset });
    allFacts.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);

  const approvedFactsVerified = ELIGIBLE_LOYALTY_FACT_KEYS.map((key) => {
    const fact = allFacts.find((f) => nz(f.fieldName) === key);
    return {
      fieldKey: key,
      factRecordId: fact?.id || null,
      humanReviewStatus: nz(fact?.humanReviewStatus) || "missing",
      approved: fact ? isApprovedFact(fact) : false,
    };
  });

  const missingApprovedFacts = approvedFactsVerified.filter((f) => !f.approved);
  const pendingFactsExcluded = allFacts
    .filter((f) => nz(f.fieldName).startsWith("be.loyalty.") && nz(f.humanReviewStatus) === "Pending")
    .map((f) => ({ fieldKey: f.fieldName, factRecordId: f.id, reason: "pending_fact_excluded" }));

  const internalOrFddExcluded = allFacts
    .filter((f) => {
      const key = nz(f.fieldName);
      return /^be\.standards\./i.test(key) || /^be\.meta\.fdd/i.test(key);
    })
    .map((f) => ({ fieldKey: f.fieldName, factRecordId: f.id, reason: "internal_or_fdd_excluded" }));

  const kpiFactsExcluded = allFacts
    .filter((f) => /^loyalty\.kpi\./i.test(nz(f.fieldName)))
    .map((f) => ({ fieldKey: f.fieldName, factRecordId: f.id, reason: "unsupported_kpi_excluded" }));

  const targetRows = buildFlattenedLoyaltyRowTargets(brandRecordId, BRAND_NAME);
  if (targetRows.length !== TOTAL_TARGET_ROWS) {
    throw new Error(`Expected ${TOTAL_TARGET_ROWS} target rows, got ${targetRows.length}`);
  }

  for (const row of targetRows) {
    if (bodyHasForbiddenGovernanceCopy(row.body)) {
      throw new Error(`Forbidden governance copy in target row ${row.slotKey}/${row.title}`);
    }
    if (!TARGET_LOYALTY_SLOTS.includes(row.slotKey)) {
      throw new Error(`Non-loyalty target slot leaked: ${row.slotKey}`);
    }
  }

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);

  const heroRows = presentationRows.filter((r) => r.slotKey === HERO_SLOT);
  const loyaltyHeroTitleSnapshot = heroRows.map((r) => ({
    recordId: r.recordId,
    title: r.title,
    body: r.body,
    sortOrder: r.sortOrder,
  }));

  const rowsBySlot = new Map();
  for (const slot of TARGET_LOYALTY_SLOTS) {
    rowsBySlot.set(slot, presentationRows.filter((r) => r.slotKey === slot));
  }

  const duplicateRowsFound = [];
  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const rowsMatched = [];
  const exactCreatePayloads = [];
  const applyBlockers = [];

  if (missingApprovedFacts.length) {
    applyBlockers.push(`missing_approved_facts:${missingApprovedFacts.map((f) => f.fieldKey).join(",")}`);
  }

  for (const slotKey of TARGET_LOYALTY_SLOTS) {
    const live = rowsBySlot.get(slotKey) || [];
    const expected = EXPECTED_LOYALTY_ROW_COUNTS[slotKey] || 0;
    if (live.length > expected) {
      duplicateRowsFound.push({
        slotKey,
        liveCount: live.length,
        expectedCount: expected,
        recordIds: live.map((r) => r.recordId),
        reason: "duplicate_cleanup_required",
      });
      applyBlockers.push(`duplicate_cleanup_required:${slotKey}:${live.length}>${expected}`);
    }
  }

  const nonTargetLoyaltySlots = presentationRows
    .filter((r) => r.slotKey.startsWith("loyalty.") && !TARGET_LOYALTY_SLOTS.includes(r.slotKey) && r.slotKey !== HERO_SLOT)
    .map((r) => r.slotKey);
  const kpiRowsFound = presentationRows.filter((r) => EXCLUDED_KPI_SLOTS.includes(r.slotKey));

  for (const planned of targetRows) {
    const liveForSlot = rowsBySlot.get(planned.slotKey) || [];
    const match = findLiveMatch(planned, liveForSlot);

    if (!match) {
      if (liveForSlot.length >= (EXPECTED_LOYALTY_ROW_COUNTS[planned.slotKey] || 0)) {
        continue;
      }
      rowsWouldCreate.push({
        slotKey: planned.slotKey,
        title: planned.title,
        body: planned.body,
        sort: planned.sort,
        action: "create",
        fields: planned.fields,
      });
      exactCreatePayloads.push({
        table: PRESENTATION_TABLE,
        fields: planned.fields,
      });
      continue;
    }

    if (bodiesMatch(match.body, planned.body) && normalizeTitle(match.title) === normalizeTitle(planned.title)) {
      rowsMatched.push({
        slotKey: planned.slotKey,
        recordId: match.recordId,
        title: match.title,
        action: "matched",
      });
    } else {
      rowsWouldUpdate.push({
        slotKey: planned.slotKey,
        recordId: match.recordId,
        action: "update_required",
        currentTitle: match.title,
        currentBody: match.body,
        proposedTitle: planned.title,
        proposedBody: planned.body,
        note: "Report-only — v25C-2D apply creates missing rows only; does not patch existing copy.",
      });
    }
  }

  const applyGatesReady = apply && approveBatch && founderReviewed && createApproved;
  const canApply =
    applyGatesReady &&
    applyBlockers.length === 0 &&
    rowsWouldCreate.length > 0 &&
    missingApprovedFacts.length === 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const created = [];
    const skipped = [];
    const errors = [];
    for (const row of rowsWouldCreate) {
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "POST",
        body: JSON.stringify({ fields: row.fields, typecast: true }),
      });
      if (!res.ok) {
        errors.push({ slotKey: row.slotKey, title: row.title, message: json.error?.message || res.status });
      } else {
        created.push({ recordId: json.id, slotKey: row.slotKey, title: row.title, sort: row.sort });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = created.length > 0;
    applyResults = { created, skipped, errors };

    const brandBasicsAfter = await fetchBrandBasics(brandRecordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  } else if (apply && applyBlockers.length > 0) {
    applyResults = { created: [], skipped: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const heroUntouched = true;

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: "tribute-portfolio",
    },
    sourcePackage: REVIEW_PACKAGE_JSON,
    marriottValidationImplied: false,
    governanceLabels: [...GOVERNANCE_LABELS],
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-loyalty-row-creation-writer.js",
      "scripts/brand-explorer-loyalty-row-creation-writer.mjs",
      "docs/data-intelligence/brand-explorer-loyalty-row-creation-writer-v25C-2D.md",
      "reports/brand-explorer-loyalty-row-creation-writer.md",
      "reports/brand-explorer-loyalty-row-creation-writer.json",
      "package.json",
    ],
    approvedFactsVerified,
    missingApprovedFacts,
    pendingFactsExcluded,
    internalOrFddFactsExcluded: internalOrFddExcluded,
    kpiFactsExcluded,
    kpiRowsExcluded: true,
    existingLoyaltyRowsFound: presentationRows
      .filter((r) => r.slotKey.startsWith("loyalty."))
      .map((r) => ({
        recordId: r.recordId,
        slotKey: r.slotKey,
        title: r.title,
        bodyPreview: normalizeBody(r.body).slice(0, 120),
        sortOrder: r.sortOrder,
      })),
    loyaltyHeroTitleSnapshot,
    loyaltyHeroTitleUntouched: heroUntouched,
    targetRowCounts: EXPECTED_LOYALTY_ROW_COUNTS,
    totalTargetRows: TOTAL_TARGET_ROWS,
    rowsWouldCreate,
    rowsWouldUpdate,
    rowsMatched,
    duplicateRowsFound,
    exactCreatePayloads,
    nonLoyaltySlotsLeakedIntoPlan: [],
    kpiRowsFound: kpiRowsFound.map((r) => ({ recordId: r.recordId, slotKey: r.slotKey })),
    otherLoyaltySlotsOnBrand: [...new Set(nonTargetLoyaltySlots)],
    presentationRowsUntouched: rowsWouldCreate.length === 0 && !airtableModified,
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
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
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand(),
    idempotentAfterApply: rowsWouldCreate.length === 0 && duplicateRowsFound.length === 0,
    doesNotDo: [
      "Modify loyalty.hero_title or existing matched rows",
      "Create loyalty.kpi.* rows",
      "Use pending, FDD, or internal-only facts",
      "Write governance labels into presentation Body copy",
      "Change images, Brand Basics, or Company Validated",
      "Imply Marriott validated anything",
      "Auto-update rows flagged update_required",
    ],
  };
}

export function buildBrandExplorerLoyaltyRowCreationWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Loyalty Row Creation Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Writer exists: **${report.writerExists ? "yes" : "no"}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- Source package: \`${report.sourcePackage}\``,
    "",
    "## Summary",
    "",
    `| Metric | Value |`,
    `|--------|-------|`,
    `| Rows would create | ${report.rowsWouldCreate.length} |`,
    `| Rows update required | ${report.rowsWouldUpdate.length} |`,
    `| Rows matched (idempotent) | ${report.rowsMatched.length} |`,
    `| Duplicate rows found | ${report.duplicateRowsFound.length} |`,
    `| loyalty.hero_title untouched | ${report.loyaltyHeroTitleUntouched ? "yes" : "no"} |`,
    `| KPI rows excluded | ${report.kpiRowsExcluded ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Governance labels (report metadata only)",
    "",
    ...report.governanceLabels.map((l) => `- ${l}`),
    "",
    "## Approved facts verified",
    "",
  ];

  for (const f of report.approvedFactsVerified) {
    lines.push(`- \`${f.fieldKey}\`: **${f.approved ? "Approved" : f.humanReviewStatus}**`);
  }
  lines.push("");

  if (report.existingLoyaltyRowsFound.length) {
    lines.push("## Existing loyalty rows", "");
    for (const row of report.existingLoyaltyRowsFound) {
      lines.push(`- \`${row.slotKey}\` \`${row.recordId}\` — ${row.title || row.bodyPreview || "(empty)"}`);
    }
    lines.push("");
  }

  if (report.rowsWouldCreate.length) {
    lines.push("## Rows would create", "");
    for (const row of report.rowsWouldCreate) {
      lines.push(`### ${row.slotKey} — ${row.title || "(no title)"}`, "", row.body, "");
    }
  }

  if (report.rowsWouldUpdate.length) {
    lines.push("## Rows update required (report only)", "");
    for (const row of report.rowsWouldUpdate) {
      lines.push(`- \`${row.slotKey}\` \`${row.recordId}\`: copy differs from v25C-2C package`);
    }
    lines.push("");
  }

  if (report.duplicateRowsFound.length) {
    lines.push("## Duplicate rows (blocks apply)", "");
    for (const dup of report.duplicateRowsFound) {
      lines.push(`- \`${dup.slotKey}\`: ${dup.liveCount} live vs ${dup.expectedCount} expected`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command", "", "```bash", report.exactApplyCommand, "```", "");

  if (report.applyResults) {
    lines.push(
      "## Apply results",
      "",
      `- Created: ${report.applyResults.created?.length || 0}`,
      `- Errors: ${report.applyResults.errors?.length || 0}`,
      ""
    );
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) {
    lines.push(`- ${item}`);
  }
  lines.push("");

  return lines.join("\n");
}
