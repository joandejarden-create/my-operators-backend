/**
 * Brand Explorer Kimpton Portfolio Context + Sort-Order Idempotency Repair v30A-R1.
 *
 * Resolves remaining Kimpton almost_ready blockers after v30A:
 * - Portfolio Context visual defect (IHG ladder mapping in audit layer)
 * - v30A sort-order non-idempotency (false-positive 13 updates on re-dry-run)
 *
 * No fact stewardship, no Company Validated changes, no image materialization.
 *
 * @see docs/data-intelligence/brand-explorer-kimpton-context-sort-idempotency-repair-writer-v30A-R1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import {
  diagnosePortfolioLadderMapping,
  readAtelierFrontendSource,
} from "./brand-explorer-portfolio-ladder-mapping.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import {
  BRAND_REPAIR_CONFIG,
  isLikelyWriterBatchSortOrder,
} from "./brand-explorer-ihg-family-active-profile-repair-writer.js";

export const WRITER_VERSION = "30A-R1";
export const REPORT_JSON_NAME =
  "brand-explorer-kimpton-context-sort-idempotency-repair-writer.json";
export const REPORT_MD_NAME = "brand-explorer-kimpton-context-sort-idempotency-repair-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-kimpton-context-sort-idempotency-repair-writer-v30A-R1.md";

export const TARGET_BRANDS = Object.freeze([
  {
    slug: "kimpton",
    recordId: "recCKuXCmGvxHPfb3",
    name: "Kimpton Hotels",
    parentCompany: "IHG Hotels & Resorts",
  },
]);

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v30A-R1-kimpton-context-sort-repair";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-kimpton-context-copy";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "ascend",
  "radisson",
  "radisson-blu",
  ...WAVE1_EXPANSION_SLUGS,
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const PORTFOLIO_CONTEXT_SLOT = "overview.portfolio_context";
const MIN_PORTFOLIO_CONTEXT_WORDS = 20;

const SORT_REPAIR_SLOT_KEYS = new Set(
  BRAND_REPAIR_CONFIG.kimpton?.sortOrderRepairSlotKeys || []
);
const DUPLICATE_BLOCKER_SLOTS = new Set([PORTFOLIO_CONTEXT_SLOT, ...SORT_REPAIR_SLOT_KEYS]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-ihg-family-active-profile-repair-writer.md",
  "reports/brand-explorer-ihg-family-active-profile-repair-writer.json",
  "reports/brand-explorer-complete-build-kimpton.md",
  "reports/brand-explorer-complete-build-kimpton.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "docs/brand-explorer-presentation-slots.md",
  "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js",
  "lib/partner-intelligence/brand-explorer-ihg-family-active-profile-repair-writer.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "live Kimpton Brand Explorer Presentation rows",
  "live Kimpton Brand Basics / parent company fields",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-kimpton-context-sort-idempotency-repair-writer.js",
  "scripts/brand-explorer-kimpton-context-sort-idempotency-repair-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-ihg-family-active-profile-repair-writer.js",
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|validated by ihg|company-approved|company approved|official sign-off/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
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

async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, PRESENTATION_TABLE)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    active: rec.fields?.Active,
    brandLinks: rec.fields?.Brand || [],
  }));
}

function buildBrandShapeFromPresentation(brandBasics, presentationRows, target) {
  const parent =
    nz(brandBasics?.fields?.["Parent Company"]) ||
    nz(brandBasics?.fields?.parent_company) ||
    target.parentCompany;
  return {
    name: target.name,
    parentCompany: parent,
    brandExplorer: {
      blocks: presentationRows.map((r) => ({
        slotKey: r.slotKey,
        recordId: r.recordId,
        title: r.title,
        body: r.body,
      })),
    },
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

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || "kimpton").toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v30A-R1`);
  }
  const meta = TARGET_BRANDS.find((b) => b.slug === slug);
  if (!meta) throw new Error(`v30A-R1 supports Kimpton only; got: ${slug}`);
  return meta;
}

function v30aWriterExists() {
  const p = path.join(
    ROOT,
    "lib/partner-intelligence/brand-explorer-ihg-family-active-profile-repair-writer.js"
  );
  return fs.existsSync(p);
}

function v30aR1WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-kimpton-context-sort-idempotency-repair-writer.js"
    )
  );
}

/** Legacy v30A logic — re-proposes sort=0 even when already 0. */
function simulateLegacyV30aSortRepairs(existingRows, brandSlug) {
  const config = BRAND_REPAIR_CONFIG[brandSlug];
  const explicitKeys = new Set(config?.sortOrderRepairSlotKeys || []);
  const repairs = [];
  for (const live of existingRows) {
    const matchesExplicit = explicitKeys.has(live.slotKey);
    const matchesBatchDrift = isLikelyWriterBatchSortOrder(live.sortOrder);
    if (!matchesExplicit && !matchesBatchDrift) continue;
    repairs.push({
      recordId: live.recordId,
      slotKey: live.slotKey,
      title: live.title,
      currentSortOrder: live.sortOrder,
      proposedSortOrder: 0,
      reason: matchesBatchDrift ? "batch_sort_drift" : "explicit_slot_key_legacy",
    });
  }
  return repairs;
}

/** Idempotent v30A logic — skip when sort already at target. */
function simulateIdempotentSortRepairs(existingRows, brandSlug) {
  const targetSort = 0;
  return simulateLegacyV30aSortRepairs(existingRows, brandSlug).filter((row) => {
    const current = row.currentSortOrder == null ? null : Number(row.currentSortOrder);
    return current !== targetSort;
  });
}

function auditDuplicateRows(existingRows, brandRecordId) {
  const bySlot = new Map();
  for (const row of existingRows) {
    if (!bySlot.has(row.slotKey)) bySlot.set(row.slotKey, []);
    bySlot.get(row.slotKey).push(row);
  }

  const findings = [];
  for (const [slotKey, rows] of bySlot.entries()) {
    if (rows.length <= 1) continue;
    const titleGroups = new Map();
    for (const row of rows) {
      const key = nz(row.title).toLowerCase() || "(empty title)";
      if (!titleGroups.has(key)) titleGroups.set(key, []);
      titleGroups.get(key).push(row);
    }
    for (const [titleKey, group] of titleGroups.entries()) {
      if (group.length > 1) {
        findings.push({
          slotKey,
          duplicateType: "duplicate_slot_title",
          title: titleKey,
          recordIds: group.map((r) => r.recordId),
          recommendation: "human_review_required",
          safeToDeactivate: false,
        });
      }
    }
    if (slotKey === "standards.requirement" && rows.length > 1 && titleGroups.size === rows.length) {
      findings.push({
        slotKey,
        duplicateType: "intentional_multi_row_slot",
        rowCount: rows.length,
        recordIds: rows.map((r) => r.recordId),
        titles: rows.map((r) => r.title),
        sortOrders: rows.map((r) => r.sortOrder),
        recommendation: "leave_alone",
        safeToDeactivate: false,
        note: "Distinct requirement titles — not duplicate rows.",
      });
    }
  }

  const wrongBrand = existingRows.filter(
    (r) =>
      Array.isArray(r.brandLinks) &&
      r.brandLinks.length > 0 &&
      !r.brandLinks.includes(brandRecordId)
  );
  if (wrongBrand.length) {
    findings.push({
      duplicateType: "wrong_brand_link",
      recordIds: wrongBrand.map((r) => r.recordId),
      recommendation: "human_review_required",
      safeToDeactivate: false,
    });
  }

  const orphanedInactive = existingRows.filter((r) => r.active === false);
  if (orphanedInactive.length) {
    findings.push({
      duplicateType: "inactive_rows",
      count: orphanedInactive.length,
      recordIds: orphanedInactive.map((r) => r.recordId),
      recommendation: "leave_alone",
    });
  }

  return findings;
}

function diagnosePortfolioContext(brandApi, frontendSource) {
  const ladder = diagnosePortfolioLadderMapping(brandApi, frontendSource);
  const ctxRow = (brandApi?.brandExplorer?.blocks || []).find(
    (b) => nz(b.slotKey) === PORTFOLIO_CONTEXT_SLOT
  );
  const body = nz(ctxRow?.body);
  const isMappingGap =
    ladder.portfolioContextRowExists &&
    ladder.narrativeRenders &&
    (ladder.rootCause === "frontend_ihg_ladder_mapping_missing" ||
      ladder.rootCause === "ihg_sibling_labels_not_rendering" ||
      ladder.rootCause === "generic_ladder_fallback_still_rendering");
  const isAuditorOnlyFix =
    ladder.portfolioContextRowExists &&
    ladder.narrativeRenders &&
    wordCount(body) >= MIN_PORTFOLIO_CONTEXT_WORDS &&
    !COMPANY_VALIDATION_BLOCK_RE.test(body);
  return {
    portfolioContextRecordId: ladder.portfolioContextRecordId,
    portfolioContextTitle: ladder.portfolioContextTitle,
    portfolioContextBodyPreview: ladder.portfolioContextBodyPreview,
    portfolioContextWordCount: wordCount(body),
    portfolioContextTierIndex: ladder.portfolioContextTierIndex,
    narrativeRenders: ladder.narrativeRenders,
    rootCause: ladder.rootCause,
    ihgSiblingLabelsRender: ladder.ihgSiblingLabelsRender,
    frontendIhgMappingPresent: ladder.frontendIhgMappingPresent,
    usesGenericScaleLabels: ladder.usesGenericScaleLabels,
    issueClass: isMappingGap
      ? "audit_mapping_gap"
      : !ladder.portfolioContextRowExists
        ? "missing_row"
        : !ladder.narrativeRenders
          ? "empty_body"
          : "resolved",
    repairStrategy: isAuditorOnlyFix
      ? "fix_audit_and_ladder_mapping_only_no_airtable_rewrite"
      : !ladder.portfolioContextRowExists
        ? "create_portfolio_context_row"
        : wordCount(body) < MIN_PORTFOLIO_CONTEXT_WORDS
          ? "backfill_ihg_owner_facing_context"
          : "fix_audit_and_ladder_mapping_only_no_airtable_rewrite",
    ladderCells: ladder.ladderCells,
    copyIsGood: isAuditorOnlyFix,
  };
}

function proposePortfolioContextRepair(portfolioDiagnosis, target, existingRows) {
  if (portfolioDiagnosis.repairStrategy !== "backfill_ihg_owner_facing_context") return null;
  const live = existingRows.find((r) => r.slotKey === PORTFOLIO_CONTEXT_SLOT);
  if (!live) return null;
  const proposedBody =
    "Luxury & lifestyle flagship within IHG—Kimpton sits with InterContinental, Regent, and Six Senses at the experiential apex; above Hotel Indigo, voco, and Crowne Plaza upscale tiers—not midscale Holiday Inn Express, avid, or limited-service formats.";
  if (COMPANY_VALIDATION_BLOCK_RE.test(proposedBody)) return null;
  return {
    action: "update",
    recordId: live.recordId,
    slotKey: PORTFOLIO_CONTEXT_SLOT,
    fixReason: "portfolio_context_body_backfill",
    currentTitle: live.title,
    currentBody: live.body,
    proposedTitle: live.title || "3",
    proposedBody,
    fields: {
      "Slot Key": PORTFOLIO_CONTEXT_SLOT,
      Title: live.title || "3",
      Body: proposedBody,
      "Brand Name": target.name,
      Brand: [target.recordId],
      Active: true,
      "Sort Order": live.sortOrder ?? 0,
    },
  };
}

export function buildApplyCommand({ brandSlug = "kimpton" } = {}) {
  return [
    "npm run brand-explorer-kimpton-context-sort-idempotency-repair-writer --",
    `--brand ${brandSlug}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildKimptonContextSortIdempotencyRepairReport({
  brandArg = "kimpton",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const presentationRows = await listPresentationRows(
    baseId,
    apiKey,
    target.recordId,
    target.name
  );
  const brandApiFetched = await fetchBrandApiShape(target.recordId);
  const brandApi =
    brandApiFetched ||
    buildBrandShapeFromPresentation(brandBasicsBefore, presentationRows, target);
  let frontendSource = "";
  try {
    frontendSource = readAtelierFrontendSource();
  } catch {
    frontendSource = "";
  }

  const portfolioDiagnosis = diagnosePortfolioContext(brandApi, frontendSource);
  const legacySortRepairs = simulateLegacyV30aSortRepairs(presentationRows, target.slug);
  const idempotentSortRepairs = simulateIdempotentSortRepairs(presentationRows, target.slug);
  const duplicateFindings = auditDuplicateRows(presentationRows, target.recordId);

  const visualBefore = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const qaBrand = finalQaBefore?.brandReports?.[0] || {};

  const portfolioContextUpdate = proposePortfolioContextRepair(
    portfolioDiagnosis,
    target,
    presentationRows
  );
  const rowsWouldUpdate = [
    ...(portfolioContextUpdate ? [portfolioContextUpdate] : []),
    ...idempotentSortRepairs.map((row) => ({
      action: "update",
      recordId: row.recordId,
      slotKey: row.slotKey,
      fixReason: "normalize_writer_batch_sort_order",
      currentTitle: row.title,
      currentBody: presentationRows.find((r) => r.recordId === row.recordId)?.body || "",
      proposedTitle: row.title,
      proposedBody: presentationRows.find((r) => r.recordId === row.recordId)?.body || "",
      fields: {
        "Slot Key": row.slotKey,
        Title: row.title || "",
        Body: presentationRows.find((r) => r.recordId === row.recordId)?.body || "",
        "Brand Name": target.name,
        Brand: [target.recordId],
        Active: true,
        "Sort Order": row.proposedSortOrder,
      },
    })),
  ];
  const rowsWouldCreate = [];

  const humanDecisionDuplicates = duplicateFindings.filter(
    (f) =>
      f.recommendation === "human_review_required" &&
      f.slotKey &&
      DUPLICATE_BLOCKER_SLOTS.has(f.slotKey)
  );
  const applyBlockers = [];
  if (humanDecisionDuplicates.length) {
    applyBlockers.push(`duplicate_rows_need_human_decision:${humanDecisionDuplicates.length}`);
  }
  if (portfolioContextUpdate && portfolioDiagnosis.copyIsGood) {
    applyBlockers.push("would_replace_good_kimpton_copy");
  }
  if (
    portfolioDiagnosis.issueClass === "audit_mapping_gap" &&
    !portfolioDiagnosis.ihgSiblingLabelsRender &&
    portfolioDiagnosis.repairStrategy.includes("no_airtable")
  ) {
    const mappingFile = path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js"
    );
    const mappingSrc = fs.readFileSync(mappingFile, "utf8");
    if (!/isIhgParent/.test(mappingSrc) || !/ihgSiblingLabelsRender/.test(mappingSrc)) {
      applyBlockers.push("ihg_ladder_mapping_code_not_deployed");
    }
  }
  if (idempotentSortRepairs.length > 0) {
    applyBlockers.push(`sort_order_drift_still_present:${idempotentSortRepairs.length}`);
  }

  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const hasAirtableWork = rowsWouldCreate.length > 0 || rowsWouldUpdate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply && hasAirtableWork) {
    const created = [];
    const updated = [];
    const errors = [];
    for (const row of rowsWouldUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: row.fields, typecast: true }) },
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
        updated.push({ recordId: row.recordId, slotKey: row.slotKey, fixReason: row.fixReason });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = updated.length > 0 && errors.length === 0;
    applyResults = { created, updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults = { created: [], updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const visualDefectsBefore = visualBefore?.defectCounts ||
    qaBrand.defectCounts || { total: 0, critical: 0, high: 0 };
  const pendingFacts = qaBrand.pendingFactsCount ?? 44;
  const projectedHigh = Math.max(
    0,
    (visualDefectsBefore.high || 0) -
      (portfolioDiagnosis.issueClass === "audit_mapping_gap" ? 1 : 0)
  );
  const projectedOverall = Math.min(
    99,
    Math.max(
      (qaBrand.scores?.overallNumeric || 81) + (portfolioDiagnosis.issueClass === "audit_mapping_gap" ? 4 : 0),
      81
    )
  );
  const expectedReadiness =
    pendingFacts > 0 ? "almost_ready" : projectedOverall >= 90 ? "ready" : "almost_ready";

  const mappingCodeDeployed =
    fs.existsSync(path.join(ROOT, "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js")) &&
    /isIhgParent/.test(
      fs.readFileSync(
        path.join(ROOT, "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js"),
        "utf8"
      )
    );

  const dryRunClean = applyBlockers.length === 0;

  const report = {
    writerVersion: WRITER_VERSION,
    v30AWriterExists: v30aWriterExists(),
    v30AR1WriterExists: v30aR1WriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_no_airtable_changes") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    portfolioContextDiagnosis: portfolioDiagnosis,
    portfolioContextRepairPlan: {
      strategy: portfolioDiagnosis.repairStrategy,
      airtableUpdateNeeded: Boolean(portfolioContextUpdate),
      mappingCodeRepairNeeded: portfolioDiagnosis.issueClass === "audit_mapping_gap",
      mappingCodeDeployed,
      note: portfolioDiagnosis.copyIsGood
        ? "Kimpton overview.portfolio_context copy is owner-facing IHG luxury/lifestyle context — do not rewrite."
        : "Portfolio context row needs backfill.",
    },
    sortOrderDriftDiagnosis: {
      legacyV30aWouldUpdateCount: legacySortRepairs.length,
      idempotentWouldUpdateCount: idempotentSortRepairs.length,
      rootCause:
        legacySortRepairs.length > 0 && idempotentSortRepairs.length === 0
          ? "v30A_proposeSortOrderRepairs_non_idempotent_explicit_key_match"
          : idempotentSortRepairs.length > 0
            ? "live_sort_order_still_drifted"
            : "no_sort_drift",
      legacyRepairs: legacySortRepairs,
      idempotentRepairs: idempotentSortRepairs,
      batchDriftRows: presentationRows.filter((r) => isLikelyWriterBatchSortOrder(r.sortOrder)),
    },
    duplicateRowFindings: duplicateFindings,
    rowsWouldCreate,
    rowsWouldUpdate,
    beforeAfterCopy: rowsWouldUpdate.map((row) => ({
      action: row.action,
      recordId: row.recordId,
      slotKey: row.slotKey,
      fixReason: row.fixReason,
      before: { title: row.currentTitle || "", body: row.currentBody || "" },
      after: { title: row.proposedTitle || "", body: row.proposedBody || "" },
    })),
    applyBlockers,
    dryRunClean,
    canApply,
    hasAirtableWork,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    ihgValidationImplied: false,
    airtableModified,
    applyResults,
    expectedFinalQaAfterApply: {
      overallNumeric: projectedOverall,
      overallActiveProfileReadiness: expectedReadiness,
      highDefectsAfter: projectedHigh,
      pendingFactsAfter: pendingFacts,
      portfolioContextDefectCleared: portfolioDiagnosis.issueClass === "audit_mapping_gap",
    },
    expectedBlockersRemainingAfterApply: [
      pendingFacts > 0 ? `${pendingFacts} pending facts — v30B stewardship required` : null,
      projectedHigh > 0 ? `${projectedHigh} high visual defects may remain` : null,
    ].filter(Boolean),
    exactDryRunCommand: `npm run brand-explorer-kimpton-context-sort-idempotency-repair-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brandSlug: target.slug }) : null,
    codeRepairsIncluded: [
      "brand-explorer-portfolio-ladder-mapping.js — IHG parent ladder + sibling simulation",
      "brand-explorer-visual-display-defect-audit.js — IHG portfolio context defect gate",
      "brand-explorer-ihg-family-active-profile-repair-writer.js — idempotent sort repair skip when sort=0",
    ],
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(
    `# Brand Explorer Kimpton Context + Sort Idempotency Repair v${report.writerVersion}`
  );
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.slug}\`)`);
  lines.push(`- v30A exists: **${report.v30AWriterExists ? "yes" : "no"}**`);
  lines.push(`- v30A-R1 exists: **${report.v30AR1WriterExists ? "yes" : "no"}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`
  );
  lines.push("");

  lines.push("## Portfolio Context diagnosis");
  const pc = report.portfolioContextDiagnosis;
  lines.push(`- Record: \`${pc.portfolioContextRecordId || "—"}\``);
  lines.push(`- Issue class: **${pc.issueClass}**`);
  lines.push(`- Root cause: **${pc.rootCause}**`);
  lines.push(`- Repair strategy: **${pc.repairStrategy}**`);
  lines.push(`- Copy is good: **${pc.copyIsGood ? "yes" : "no"}**`);
  lines.push(`- Body preview: ${pc.portfolioContextBodyPreview}`);
  lines.push("");

  lines.push("## Sort-order drift diagnosis");
  const sd = report.sortOrderDriftDiagnosis;
  lines.push(`- Root cause: **${sd.rootCause}**`);
  lines.push(`- Legacy v30A would update: **${sd.legacyV30aWouldUpdateCount}** rows`);
  lines.push(`- Idempotent would update: **${sd.idempotentWouldUpdateCount}** rows`);
  lines.push("");

  lines.push("## Duplicate row findings");
  if (!report.duplicateRowFindings.length) lines.push("- None");
  for (const f of report.duplicateRowFindings) {
    lines.push(`- ${f.slotKey || f.duplicateType}: ${f.recommendation} (${f.rowCount || f.recordIds?.length || 0} rows)`);
  }
  lines.push("");

  lines.push("## Rows to update/create");
  lines.push(`- Create: ${report.rowsWouldCreate.length}`);
  lines.push(`- Update: ${report.rowsWouldUpdate.length}`);
  lines.push("");

  lines.push("## Expected Final QA after apply");
  lines.push(
    `- Overall: **${report.expectedFinalQaAfterApply.overallNumeric}** (${report.expectedFinalQaAfterApply.overallActiveProfileReadiness})`
  );
  lines.push("");

  lines.push("## Blockers remaining after apply");
  for (const b of report.expectedBlockersRemainingAfterApply) lines.push(`- ${b}`);
  if (!report.expectedBlockersRemainingAfterApply.length) lines.push("- None");
  lines.push("");

  lines.push("## Code repairs included");
  for (const c of report.codeRepairsIncluded) lines.push(`- ${c}`);
  lines.push("");

  lines.push("## Exact apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none — dry-run not clean)");
  return lines.join("\n");
}
