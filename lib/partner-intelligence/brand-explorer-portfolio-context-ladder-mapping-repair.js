/**
 * Brand Explorer Portfolio Context Ladder Mapping Repair v25C-4D.
 *
 * Repairs Marriott sibling ladder display mapping for Tribute Portfolio.
 * Dry-run by default; minimal Airtable updates only when tier title/body missing.
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";
import { PORTFOLIO_CONTEXT_TIER } from "./brand-explorer-tribute-visible-content-repair-writer.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  ATELIER_JS_PATH,
  diagnosePortfolioLadderMapping,
  frontendMappingRepairNeeded,
  GENERIC_LADDER_FALLBACK_LABELS,
  hasVal,
  nz,
  readAtelierFrontendSource,
} from "./brand-explorer-portfolio-ladder-mapping.js";

export const WRITER_VERSION = "25C-4D";
export const REPORT_JSON_NAME = "brand-explorer-portfolio-context-ladder-mapping-repair.json";
export const REPORT_MD_NAME = "brand-explorer-portfolio-context-ladder-mapping-repair.md";
export const DOC_MD_NAME = "brand-explorer-portfolio-context-ladder-mapping-repair-v25C-4D.md";

export const APPLY_FLAG = "--approve-brand-explorer-v25C-4D-portfolio-context-ladder-mapping";
export const APPLY_FLAG_OWNER_PLANNING =
  "--confirm-marriott-context-is-owner-planning-not-company-validated";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const PORTFOLIO_CONTEXT_SLOT = "overview.portfolio_context";

const PROTECTED_SLOT_KEYS = new Set([
  "footprint.portfolio_mix",
  "footprint.momentum",
  "footprint.openings",
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
  "standards.requirement",
  "standards.intro",
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-portfolio-mix-context-normalization-writer.md",
  "reports/brand-explorer-portfolio-mix-context-normalization-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  ATELIER_JS_PATH,
  "public/js/brand-explorer-gold-detail.js",
  "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js",
  "live Tribute Brand Explorer Presentation rows",
  "live Curio/Kimpton/Radisson/Ascend Portfolio Context reference rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js",
  "lib/partner-intelligence/brand-explorer-portfolio-context-ladder-mapping-repair.js",
  "scripts/brand-explorer-portfolio-context-ladder-mapping-repair.mjs",
  "docs/data-intelligence/brand-explorer-portfolio-context-ladder-mapping-repair-v25C-4D.md",
  "reports/brand-explorer-portfolio-context-ladder-mapping-repair.md",
  "reports/brand-explorer-portfolio-context-ladder-mapping-repair.json",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "package.json",
];

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

export function buildApplyCommand({ brandSlug = "tribute-portfolio" } = {}) {
  return `npm run brand-explorer-portfolio-context-ladder-mapping-repair -- --brand ${brandSlug} --apply ${APPLY_FLAG} ${APPLY_FLAG_OWNER_PLANNING}`;
}

function buildMinimalContextUpdatePlan(ctxRow, brandRecordId, brandName) {
  if (!ctxRow) return null;
  const needsTitle = nz(ctxRow.title) !== PORTFOLIO_CONTEXT_TIER;
  const needsBody = !hasVal(ctxRow.body);
  if (!needsTitle && !needsBody) return null;
  const fields = {
    "Slot Key": PORTFOLIO_CONTEXT_SLOT,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
  };
  if (needsTitle) fields.Title = PORTFOLIO_CONTEXT_TIER;
  if (needsBody) {
    fields.Body = nz(ctxRow.body);
  }
  return {
    recordId: ctxRow.recordId,
    slotKey: PORTFOLIO_CONTEXT_SLOT,
    action: "update",
    fields,
    reason: needsTitle ? "tier_title_alignment" : "body_backfill_required",
  };
}

export async function buildBrandExplorerPortfolioContextLadderMappingRepairReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  ownerPlanningConfirmed = false,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(TRIBUTE_RECORD_ID);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const brand = await fetchBrandApiShape(TRIBUTE_RECORD_ID);
  if (!brand) throw new Error(`Brand not found: ${TRIBUTE_RECORD_ID}`);

  const frontendSource = readAtelierFrontendSource();
  const diagnosis = diagnosePortfolioLadderMapping(brand, frontendSource);
  const frontendRepairs = frontendMappingRepairNeeded(frontendSource);

  const ctxRow = (brand.brandExplorer?.blocks || []).find(
    (b) => nz(b.slotKey) === PORTFOLIO_CONTEXT_SLOT
  );

  const dataPlan = buildMinimalContextUpdatePlan(
    ctxRow
      ? {
          recordId: ctxRow.recordId,
          title: ctxRow.title,
          body: ctxRow.body,
        }
      : null,
    TRIBUTE_RECORD_ID,
    BRAND_NAME
  );

  const rowsWouldUpdate = dataPlan ? [dataPlan] : [];
  const rowsWouldCreate = [];

  const applyBlockers = [];
  if (!diagnosis.narrativeRenders) applyBlockers.push("portfolio_context_body_does_not_render");
  if (!diagnosis.frontendMarriottMappingPresent) applyBlockers.push("frontend_marriott_mapping_missing");
  if (diagnosis.usesGenericScaleLabels) applyBlockers.push("generic_lower_mid_upscale_ladder_still_present");
  if (!diagnosis.tributeHighlightedInLadder) applyBlockers.push("tribute_not_highlighted_in_ladder");
  if (!diagnosis.marriottSiblingLabelsRender) applyBlockers.push("marriott_sibling_labels_do_not_render");
  if (frontendRepairs.length > 0) {
    applyBlockers.push(`frontend_repairs_still_needed:${frontendRepairs.join(",")}`);
  }

  const applyGatesReady = apply && approveBatch && ownerPlanningConfirmed;
  const hasAirtableWork = rowsWouldUpdate.length > 0 || rowsWouldCreate.length > 0;
  const mappingReady = applyBlockers.length === 0;
  const canApply = applyGatesReady && mappingReady && (hasAirtableWork || mappingReady);

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = { ...companyValidatedBefore };

  if (canApply && hasAirtableWork) {
    const updated = [];
    const errors = [];
    for (const row of rowsWouldUpdate) {
      if (PROTECTED_SLOT_KEYS.has(row.slotKey)) {
        errors.push({ slotKey: row.slotKey, message: "protected_slot_blocked" });
        continue;
      }
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
        errors.push({ recordId: row.recordId, message: json.error?.message || res.status });
      } else {
        updated.push({ recordId: row.recordId, slotKey: row.slotKey });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = updated.length > 0 && errors.length === 0;
    applyResults = { updated, created: [], errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(TRIBUTE_RECORD_ID));
  } else if (apply) {
    applyResults = {
      updated: [],
      created: [],
      errors: [],
      blocked: !canApply,
      blockers: applyBlockers,
      note: hasAirtableWork ? "blocked" : "mapping_repair_is_frontend_only_no_airtable_rows_needed",
    };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const rootCauseAfterV25C4C =
    diagnosis.rootCause === "resolved_after_v25C_4D"
      ? "v25C-4C populated overview.portfolio_context and added partial Marriott frontend mapping; v25C-4D audit still flagged Marriott because visual-display-defect-audit only treated Hilton/Choice as static-ladder parents and Marriott tier-2 peers were not soft-collection focused."
      : diagnosis.rootCause;

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C4DWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? (airtableModified ? "apply" : "apply_no_airtable_changes") : "apply_blocked") : "dry-run",
    brand: { name: BRAND_NAME, recordId: TRIBUTE_RECORD_ID, slug: "tribute-portfolio" },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    rootCause: rootCauseAfterV25C4C,
    rootCauseDetail:
      "Data row overview.portfolio_context exists with tier Title=2 and founder-reviewed Body. API exposes it via brand.brandExplorer.blocks. Remaining defect was display-mapping: audit omitted Marriott from usesParentStaticLadder and frontend tier-2 peer list needed soft-collection owner-planning labels (Autograph Collection, Design Hotels, etc.).",
    dataExists: diagnosis.portfolioContextRowExists,
    apiExposesPortfolioContext: diagnosis.apiExposesPortfolioContext,
    frontendMappingIncomplete: frontendRepairs.length > 0,
    frontendRepairsNeeded: frontendRepairs,
    portfolioContextDiagnosis: diagnosis,
    proposedCodeRepair: {
      atelierJs: [
        "Refine MARRIOTT_PORTFOLIO_TIER_BRANDS tier 2 to soft-collection peers (Autograph Collection, Tribute Portfolio, Design Hotels, Moxy Hotels, Element Hotels).",
        "Update tier label to Lifestyle & Soft Collections.",
        "Add Marriott owner-planning hint copy on Portfolio Context section.",
      ],
      visualDefectAudit: [
        "Treat Marriott International as usesParentStaticLadder when frontend MARRIOTT_PORTFOLIO_TIER_BRANDS is present.",
      ],
    },
    proposedDataRepair: dataPlan,
    rowsWouldUpdate: rowsWouldUpdate.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      reason: r.reason,
    })),
    rowsWouldCreate,
    genericLadderLabelsBlocked: GENERIC_LADDER_FALLBACK_LABELS,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    loyaltyRowsUntouched: true,
    openingsRowsUntouched: true,
    momentumRowsUntouched: true,
    portfolioMixRowsUntouched: true,
    standardsRowsUntouched: true,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      ownerPlanningConfirmed,
      ready: applyGatesReady,
      canApply,
      mappingReady,
    },
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand({ brandSlug: "tribute-portfolio" }),
    idempotentAfterApply: rowsWouldUpdate.length === 0 && mappingReady,
  };
}

export function buildBrandExplorerPortfolioContextLadderMappingRepairMarkdown(report) {
  const d = report.portfolioContextDiagnosis || {};
  const lines = [
    `# Brand Explorer Portfolio Context Ladder Mapping Repair v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}**`,
    "",
    "## Root cause",
    "",
    `**${report.rootCause}**`,
    "",
    report.rootCauseDetail,
    "",
    "## Diagnosis",
    "",
    `| Data exists | ${report.dataExists ? "yes" : "no"} |`,
    `| API exposes portfolio context | ${report.apiExposesPortfolioContext ? "yes" : "no"} |`,
    `| Narrative renders | ${d.narrativeRenders ? "yes" : "no"} |`,
    `| Frontend Marriott mapping | ${d.frontendMarriottMappingPresent ? "yes" : "no"} |`,
    `| Tribute highlighted | ${d.tributeHighlightedInLadder ? "yes" : "no"} |`,
    `| Marriott sibling labels | ${d.marriottSiblingLabelsRender ? "yes" : "no"} |`,
    `| Generic scale labels | ${d.usesGenericScaleLabels ? "yes" : "no"} |`,
    "",
    "## Ladder simulation",
    "",
  ];

  for (const cell of d.ladderCells || []) {
    lines.push(
      `- Tier ${cell.tierIndex}${cell.active ? " **(active)**" : ""}: ${cell.label}`
    );
  }

  lines.push(
    "",
    "## Summary",
    "",
    `| Rows would update | ${report.rowsWouldUpdate.length} |`,
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
