/**
 * Brand Explorer Kimpton Portfolio Context Gate Reconciliation Writer v30A-R2.
 *
 * Reconciles portfolio-context detection across ladder mapping, visual defect audit,
 * Final QA, and complete-build orchestrator when Kimpton Basics uses
 * "InterContinental Hotels Group" as Parent Company (not only "IHG Hotels & Resorts").
 *
 * Code-only reconciliation by default — no Airtable writes unless row content is missing.
 *
 * @see docs/data-intelligence/brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer-v30A-R2.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import {
  evaluatePortfolioContextGate,
  readAtelierFrontendSource,
} from "./brand-explorer-portfolio-ladder-mapping.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

export const WRITER_VERSION = "30A-R2";
export const REPORT_JSON_NAME =
  "brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer-v30A-R2.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v30A-R2-kimpton-portfolio-context-gate-reconciliation";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const TARGET_BRANDS = Object.freeze([
  {
    slug: "kimpton",
    recordId: "recCKuXCmGvxHPfb3",
    name: "Kimpton Hotels",
    parentCompany: "IHG Hotels & Resorts",
  },
]);

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "ascend",
  "radisson",
  "radisson-blu",
  ...WAVE1_EXPANSION_SLUGS,
]);

const PORTFOLIO_CONTEXT_SLOT = "overview.portfolio_context";
const MIN_PORTFOLIO_CONTEXT_WORDS = 20;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-kimpton-context-sort-idempotency-repair-writer.md",
  "reports/brand-explorer-kimpton-context-sort-idempotency-repair-writer.json",
  "reports/brand-explorer-ihg-family-active-profile-repair-writer.md",
  "reports/brand-explorer-ihg-family-active-profile-repair-writer.json",
  "reports/brand-explorer-ihg-family-pending-fact-stewardship-writer.md",
  "reports/brand-explorer-ihg-family-pending-fact-stewardship-writer.json",
  "reports/brand-explorer-kimpton-branded-residences-conflict-writer.md",
  "reports/brand-explorer-kimpton-branded-residences-conflict-writer.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-kimpton.md",
  "reports/brand-explorer-complete-build-kimpton.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "live Kimpton API response",
  "live Kimpton presentation rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer.js",
  "scripts/brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|validated by ihg|company-approved|company approved|official sign-off/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || "kimpton").toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v30A-R2`);
  }
  const meta = TARGET_BRANDS.find((b) => b.slug === slug);
  if (!meta) throw new Error(`v30A-R2 supports Kimpton only; got: ${slug}`);
  return meta;
}

export function v30aR2WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer.js"
    )
  );
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

function extractPortfolioContextRow(brand) {
  const row = (brand?.brandExplorer?.blocks || []).find(
    (b) => nz(b.slotKey) === PORTFOLIO_CONTEXT_SLOT
  );
  return row || null;
}

function gateComparisonSummary({ gate, visualReport, finalQaReport, completeBuildReport }) {
  const visualDefect = (visualReport?.defects || []).find(
    (d) => d.defectType === "missing_peer_portfolio_context"
  );
  const qaBrand = finalQaReport?.brandReports?.[0] || {};
  const qaDefect = (qaBrand.defects || []).find((d) => d.type === "missing_peer_portfolio_context");
  const completeBlocker = (completeBuildReport?.blockers || []).find(
    (b) => /portfolio context/i.test(nz(b.section)) || /missing_peer/i.test(nz(b.message))
  );
  return {
    ladderMapping: {
      parentCompany: gate.parentCompany,
      portfolioContextRowExists: gate.portfolioContextRowExists,
      narrativeRenders: gate.narrativeRenders,
      ihgSiblingLabelsRender: gate.ihgSiblingLabelsRender,
      parentPortfolioReady: gate.parentPortfolioReady,
      portfolioContextReady: gate.portfolioContextReady,
      missingPeerDefect: gate.missingPeerDefect,
      rootCause: gate.rootCause,
    },
    visualDefectAudit: {
      defectCount: visualReport?.defectCounts?.total ?? null,
      highDefectCount: visualReport?.defectCounts?.high ?? null,
      missingPeerPresent: Boolean(visualDefect),
      ihgLadderMappingReady:
        visualReport?.tributeVisibleModel?.sections?.portfolioContext?.ihgLadderMappingReady,
      usesParentStaticLadder:
        visualReport?.tributeVisibleModel?.sections?.portfolioContext?.usesParentStaticLadder,
    },
    finalQaAuditor: {
      overallNumeric: qaBrand.scores?.overallNumeric ?? null,
      readiness: qaBrand.scores?.overallActiveProfileReadiness ?? null,
      highDefectCount: qaBrand.defectCounts?.high ?? null,
      missingPeerPresent: Boolean(qaDefect),
    },
    completeBuild: {
      readyForActiveProfile: completeBuildReport?.readyForActiveProfile ?? null,
      readinessBand: completeBuildReport?.readinessBand ?? null,
      portfolioContextBlocker: Boolean(completeBlocker),
      blockerMessage: completeBlocker?.message || null,
    },
    gatesAgree:
      gate.missingPeerDefect === Boolean(visualDefect) &&
      gate.missingPeerDefect === Boolean(qaDefect) &&
      gate.missingPeerDefect === Boolean(completeBlocker),
  };
}

export function buildApplyCommand({ brandSlug = "kimpton" } = {}) {
  return [
    "npm run brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer --",
    `--brand ${brandSlug}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildKimptonPortfolioContextGateReconciliationReport({
  brandArg = "kimpton",
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const brandApi = await fetchBrandApiShape(target.recordId);
  if (!brandApi) throw new Error(`Could not load Kimpton API shape for ${target.recordId}`);

  let frontendSource = "";
  try {
    frontendSource = readAtelierFrontendSource();
  } catch {
    frontendSource = "";
  }

  const ctxRow = extractPortfolioContextRow(brandApi);
  const gate = evaluatePortfolioContextGate(brandApi, frontendSource);
  const body = nz(ctxRow?.body);
  const copyIsGood =
    Boolean(ctxRow) &&
    wordCount(body) >= MIN_PORTFOLIO_CONTEXT_WORDS &&
    !COMPANY_VALIDATION_BLOCK_RE.test(body);

  const visualReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.recordId,
  }).catch(() => null);
  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const completeBuildOrchestrator = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const completeBuildReport = completeBuildOrchestrator?.brandResults?.[0] || null;

  const gateComparison = gateComparisonSummary({
    gate,
    visualReport,
    finalQaReport,
    completeBuildReport,
  });

  const rootCause =
    gate.portfolioContextRowExists && copyIsGood && !gate.parentPortfolioReady
      ? "audit_parent_company_mismatch_intercontinental_hotels_group_not_recognized_as_ihg"
      : !gate.portfolioContextRowExists
        ? "missing_portfolio_context_row"
        : !gate.narrativeRenders
          ? "portfolio_context_body_empty"
          : gate.missingPeerDefect
            ? "portfolio_ladder_mapping_not_ready"
            : "resolved_after_v30A_R2";

  const issueClass =
    rootCause === "audit_parent_company_mismatch_intercontinental_hotels_group_not_recognized_as_ihg"
      ? "audit_logic_inconsistency"
      : !gate.portfolioContextRowExists
        ? "missing_row"
        : gate.missingPeerDefect
          ? "mapping_gap"
          : "resolved";

  const mappingSrc = fs.readFileSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-portfolio-ladder-mapping.js"),
    "utf8"
  );
  const codeFixDeployed =
    /intercontinental hotels group/.test(mappingSrc) &&
    /evaluatePortfolioContextGate/.test(mappingSrc);

  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const applyBlockers = [];

  if (!copyIsGood && !ctxRow) {
    applyBlockers.push("portfolio_context_row_genuinely_missing_needs_human_copy");
  }
  if (copyIsGood && rowsWouldCreate.length === 0 && rowsWouldUpdate.length === 0) {
    applyBlockers.push("no_airtable_changes_needed_code_reconciliation_only");
  }
  if (!codeFixDeployed) {
    applyBlockers.push("shared_ihg_parent_detection_not_deployed");
  }
  if (!gateComparison.gatesAgree && codeFixDeployed) {
    applyBlockers.push("final_qa_and_complete_build_still_disagree_after_code_fix");
  }

  const applyGatesReady = apply && approveBatch && noValidationClaim;
  const canApply = applyGatesReady && applyBlockers.length === 0;
  const dryRunClean = applyBlockers.filter((b) => b !== "no_airtable_changes_needed_code_reconciliation_only").length === 0;

  let airtableModified = false;
  let companyValidatedAfter = companyValidatedBefore;
  const applyResults = apply
    ? {
        created: [],
        updated: [],
        errors: [],
        blocked: !canApply,
        blockers: applyBlockers,
        note: "v30A-R2 is code-reconciliation only — no Airtable presentation writes.",
      }
    : null;

  const qaAfter = gateComparison.finalQaAuditor;
  const expectedFinalQa = {
    overallNumeric: gate.missingPeerDefect ? qaAfter.overallNumeric : Math.min(99, (qaAfter.overallNumeric || 96) + 4),
    overallActiveProfileReadiness: gate.missingPeerDefect ? qaAfter.readiness : "ready",
    highDefectsAfter: gate.missingPeerDefect ? 1 : 0,
    portfolioContextDefectCleared: !gate.missingPeerDefect,
  };
  const expectedCompleteBuild = {
    readyForActiveProfile: !gate.missingPeerDefect,
    readinessBand: gate.missingPeerDefect ? "almost_ready" : "ready",
    portfolioContextBlocker: gate.missingPeerDefect,
  };

  const report = {
    writerVersion: WRITER_VERSION,
    v30AR2WriterExists: v30aR2WriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply_no_op" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    rootCause,
    issueClass,
    rowContentMissing: !gate.portfolioContextRowExists || !gate.narrativeRenders,
    auditLogicStale: issueClass === "audit_logic_inconsistency",
    portfolioContextDiagnosis: {
      recordId: gate.portfolioContextRecordId,
      slotKey: PORTFOLIO_CONTEXT_SLOT,
      title: gate.portfolioContextTitle,
      bodyPreview: gate.portfolioContextBodyPreview,
      wordCount: wordCount(body),
      parentCompanyFromApi: nz(brandApi.parentCompany),
      copyIsGood,
      tierIndex: gate.portfolioContextTierIndex,
      ladderCells: gate.ladderCells,
    },
    gateComparison,
    codeRepairs: [
      "isIhgParent() recognizes InterContinental Hotels Group (aligns with atelier isIhgParentCompanyKey)",
      "evaluatePortfolioContextGate() shared across visual audit reconstruct + defect gate",
      "Final QA visual audit call prefers recordId over slug",
    ],
    codeFixDeployed,
    rowsWouldCreate,
    rowsWouldUpdate,
    applyBlockers,
    dryRunClean,
    canApply,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched: true,
    ihgValidationImplied: false,
    airtableModified,
    applyResults,
    expectedFinalQaAfterFix: expectedFinalQa,
    expectedCompleteBuildAfterFix: expectedCompleteBuild,
    exactDryRunCommand: `npm run brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brandSlug: target.slug }) : null,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Kimpton Portfolio Context Gate Reconciliation v${report.writerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.slug}\`)`);
  lines.push(`- v30A-R2 exists: **${report.v30AR2WriterExists ? "yes" : "no"}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Root cause: **${report.rootCause}**`);
  lines.push(`- Issue class: **${report.issueClass}**`);
  lines.push(`- Row content missing: **${report.rowContentMissing ? "yes" : "no"}**`);
  lines.push(`- Audit logic stale: **${report.auditLogicStale ? "yes" : "no"}**`);
  lines.push(`- Code fix deployed: **${report.codeFixDeployed ? "yes" : "no"}**`);
  lines.push(`- Gates agree: **${report.gateComparison.gatesAgree ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");

  lines.push("## Portfolio context row");
  const pc = report.portfolioContextDiagnosis;
  lines.push(`- Record: \`${pc.recordId || "—"}\``);
  lines.push(`- Parent (API): **${pc.parentCompanyFromApi}**`);
  lines.push(`- Copy is good: **${pc.copyIsGood ? "yes" : "no"}**`);
  lines.push(`- Body preview: ${pc.bodyPreview}`);
  lines.push("");

  lines.push("## Gate comparison");
  lines.push(`- Ladder parentPortfolioReady: **${report.gateComparison.ladderMapping.parentPortfolioReady}**`);
  lines.push(`- Visual missing_peer: **${report.gateComparison.visualDefectAudit.missingPeerPresent}**`);
  lines.push(`- Final QA missing_peer: **${report.gateComparison.finalQaAuditor.missingPeerPresent}**`);
  lines.push(`- Complete Build blocker: **${report.gateComparison.completeBuild.portfolioContextBlocker}**`);
  lines.push("");

  lines.push("## Expected after fix");
  lines.push(
    `- Final QA: **${report.expectedFinalQaAfterFix.overallNumeric}** (${report.expectedFinalQaAfterFix.overallActiveProfileReadiness})`
  );
  lines.push(
    `- Complete Build ready: **${report.expectedCompleteBuildAfterFix.readyForActiveProfile ? "yes" : "no"}**`
  );
  lines.push("");

  lines.push("## Code repairs");
  for (const c of report.codeRepairs) lines.push(`- ${c}`);
  lines.push("");

  lines.push("## Apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none — dry-run not clean or no Airtable work)");
  return lines.join("\n");
}
