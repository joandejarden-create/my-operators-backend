/**
 * Brand Explorer v36D — Action Router orchestrator.
 *
 * Loads v36C remediation planner output, routes actions, builds dry-run patch plans.
 * No Airtable writes by default.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { routeBatchActions } from "./brand-explorer-action-router.js";
import { executeBatchRemediationPlans } from "./brand-explorer-remediation-executor.js";
import {
  parseV36DApplyMode,
  parseV36DGuardFlags,
  enforceApplyGate,
  V36D_APPLY_MODES,
  APPLY_GATE_ENFORCER_VERSION,
} from "./brand-explorer-apply-gate-enforcer.js";

export const V36D_VERSION = "v36D";
export const REPORT_JSON = "brand-explorer-v36d-action-router.json";
export const REPORT_MD = "brand-explorer-v36d-action-router.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const DEFAULT_TEST_BRANDS = Object.freeze([
  "design-hotels",
  "small-luxury-hotels-of-the-world",
  "tribute-portfolio",
  "woodspring-suites",
  "everhome-suites",
]);

function loadV36CReport(rootDir = ROOT) {
  const p = path.join(rootDir, "reports", "brand-explorer-v36c-remediation-planner.json");
  if (!fs.existsSync(p)) {
    throw new Error(
      `Missing ${p} — run npm run brand-explorer-v36c-remediation-planner -- --dry-run first`
    );
  }
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function filterBrandResults(v36cReport, brands) {
  if (!brands?.length) return v36cReport;
  return {
    ...v36cReport,
    brandResults: (v36cReport.brandResults || []).filter((b) => brands.includes(b.brandSlug)),
    regressionChecks: (v36cReport.regressionChecks || []).filter((r) => brands.includes(r.brandSlug)),
  };
}

function buildBatchTable(routes) {
  return routes.map((r) => ({
    brand: r.brand,
    brandSlug: r.brandSlug,
    currentState: r.currentState,
    recommendedAction: r.recommendedAction,
    allowedCommand: r.allowedCommand,
    blockedCommand: r.blockedCommand,
    blockersRemaining: r.blockersRemaining,
    founderReviewRequired: r.founderReviewRequired,
    estimatedGenericFixCoverage: r.estimatedGenericFixCoverage,
    requiresBrandSpecificConfig: r.requiresBrandSpecificConfig,
    requiresCodePatch: r.requiresCodePatch,
    safeToApplyNow: r.safeToApplyNow,
  }));
}

function markdownBatchTable(batch) {
  const lines = [
    "# Brand Explorer v36D Action Router",
    "",
    "## Batch action table",
    "",
    "| Brand | State | Action | Safe to apply now | Founder review | Generic fix % | Brand config | Code patch |",
    "|-------|-------|--------|-------------------|----------------|---------------|--------------|------------|",
  ];
  for (const row of batch) {
    lines.push(
      `| ${row.brand} | ${row.currentState} | **${row.recommendedAction}** | ${row.safeToApplyNow ? "yes" : "no"} | ${row.founderReviewRequired ? "yes" : "no"} | ${row.estimatedGenericFixCoverage}% | ${row.requiresBrandSpecificConfig ? "yes" : "no"} | ${row.requiresCodePatch ? "yes" : "no"} |`
    );
  }
  lines.push("");
  lines.push("## Apply gate");
  lines.push("");
  lines.push("- Default: **dry-run** — no Airtable writes");
  lines.push("- Future apply modes: `--apply-draft`, `--apply-remediation`, `--apply-approved` with confirm flags");
  lines.push("- Never mutates Company Validated");
  lines.push("- Never auto-approves active profile");
  return lines.join("\n");
}

function writeDesignHotelsPlanMd(plan) {
  const lines = [
    "# Design Hotels Remediation Apply Plan (v36D)",
    "",
    `- Action: **${plan.action}**`,
    `- Mode: **${plan.mode}** — apply blocked in v36D`,
    `- Active approval: **NOT RECOMMENDED**`,
    `- Calibrated score: ${plan.calibratedScore}`,
    `- Patch items: ${plan.patchPlan?.summary?.total ?? 0}`,
    "",
    "## Patch summary",
    "",
    "```json",
    JSON.stringify(plan.patchPlan?.summary || {}, null, 2),
    "```",
    "",
    "## Checklist",
    "",
  ];
  for (const c of plan.addressChecklist || []) {
    lines.push(
      `- **${c.issueId}** — \`${c.patchType}\` @ ${c.stage} — generic=${c.safeForGenericApply} founder=${c.requiresFounderApproval} code=${c.requiresCodePatch}`
    );
  }
  lines.push("");
  lines.push("## Must address");
  lines.push("- property example row-level image matching");
  lines.push("- modal placeholders on Wake BioHotel, Condesa DF, Carlota");
  lines.push("- wrong affiliation model language in 3 rows");
  lines.push("- standards table owner-readiness");
  lines.push("- loyalty tab coverage");
  lines.push("- economics / fee affiliation fit");
  lines.push("- source/internal language if still present");
  lines.push("- fallback risk where renderer masks missing rows");
  lines.push("");
  lines.push("## Allowed command (future)");
  lines.push("```");
  lines.push(plan.allowedCommand || "(none)");
  lines.push("```");
  return lines.join("\n");
}

function writeSlhApplyDraftMd(plan) {
  const lines = [
    "# SLH Apply-Draft Plan (v36D)",
    "",
    `- Action: **${plan.action}**`,
    `- Mode: **${plan.mode}** — apply blocked unless future --apply-draft gate`,
    "",
    "## Draft plan summary",
    "",
    "```json",
    JSON.stringify(plan.draftPlanSummary || {}, null, 2),
    "```",
    "",
    "## Confirmations",
    "",
  ];
  for (const c of plan.confirmations || []) lines.push(`- ${c}`);
  lines.push("");
  lines.push("## Allowed command (future)");
  lines.push("```");
  lines.push(plan.allowedCommand || "(none)");
  lines.push("```");
  lines.push("");
  lines.push("> Do not apply in v36D. Post-draft founder visual review required.");
  return lines.join("\n");
}

function writeTributeMd(plan) {
  return [
    "# Tribute Portfolio Remediation Plan (v36D)",
    "",
    `- Action: **${plan.action}**`,
    `- Patch items: ${plan.patchPlan?.summary?.total ?? 0}`,
    "",
    "## Benchmark",
    "",
    "```json",
    JSON.stringify(plan.benchmark || {}, null, 2),
    "```",
    "",
    "## Patch summary",
    "",
    "```json",
    JSON.stringify(plan.patchPlan?.summary || {}, null, 2),
    "```",
  ].join("\n");
}

function writeWoodspringMd(plan) {
  return [
    "# WoodSpring Suites Remediation Plan (v36D)",
    "",
    `- Action: **${plan.action}**`,
    `- Patch items: ${plan.patchPlan?.summary?.total ?? 0}`,
    "",
    "## Prior known issues",
    "",
    ...(plan.woodspringPriorIssues || []).map((i) => `- ${i}`),
    "",
    plan.note || "",
    "",
    "## Patch summary",
    "",
    "```json",
    JSON.stringify(plan.patchPlan?.summary || {}, null, 2),
    "```",
  ].join("\n");
}

function writeEverhomeMd(plan) {
  return [
    "# Everhome Exception Investigation (v36D)",
    "",
    `- Action: **${plan.action}**`,
    `- Previous readyForActiveProfile: **${plan.previousReadyForActiveProfile}**`,
    `- v36C calibrated score: ${plan.v36cCalibratedScore} (${plan.enforcementBand})`,
    `- Regression verdict: ${plan.regressionVerdict}`,
    "",
    "## Recommendation",
    "",
    `**${plan.recommendation}** — ${plan.rationale}`,
    "",
    "## Determination",
    "",
    "```json",
    JSON.stringify(plan.determination || {}, null, 2),
    "```",
    "",
    "## Blockers",
    "",
    "```json",
    JSON.stringify(plan.blockers || {}, null, 2),
    "```",
    "",
    `## Next`,
    "",
    plan.recommendedNext || "",
  ].join("\n");
}

export async function runV36DActionRouter({
  brands = DEFAULT_TEST_BRANDS,
  dryRun = true,
  argv = [],
  rootDir = ROOT,
} = {}) {
  const mode = dryRun ? V36D_APPLY_MODES.DRY_RUN : parseV36DApplyMode(argv);
  const guardFlags = parseV36DGuardFlags(argv);
  const v36cFull = loadV36CReport(rootDir);
  const v36cReport = filterBrandResults(v36cFull, brands);

  const routes = routeBatchActions(v36cReport);
  const plans = executeBatchRemediationPlans(v36cReport, routes);
  const batchTable = buildBatchTable(routes);

  const gateEnforcement = routes.map((r) => ({
    brandSlug: r.brandSlug,
    recommendedAction: r.recommendedAction,
    gate: enforceApplyGate({
      mode,
      guardFlags,
      recommendedAction: r.recommendedAction,
      companyValidatedTouchAttempted: false,
      activeProfileApprovalAttempted: mode === V36D_APPLY_MODES.APPLY_APPROVED,
    }),
  }));

  // Hard stop: never execute writes in this orchestrator regardless of flags
  const airtableWritesExecuted = false;
  if (mode !== V36D_APPLY_MODES.DRY_RUN) {
    // Document that apply modes are recognized but not executed in v36D
  }

  const bySlug = (slug) => plans.find((p) => p.brandSlug === slug);
  const routeBySlug = (slug) => routes.find((r) => r.brandSlug === slug);

  return {
    version: V36D_VERSION,
    applyGateEnforcerVersion: APPLY_GATE_ENFORCER_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified: airtableWritesExecuted,
    airtableWritesExecuted,
    sourceReport: "reports/brand-explorer-v36c-remediation-planner.json",
    brands,
    batchTable,
    routes,
    plans,
    gateEnforcement,
    designHotelsPlan: bySlug("design-hotels"),
    slhApplyDraftPlan: bySlug("small-luxury-hotels-of-the-world"),
    tributePlan: bySlug("tribute-portfolio"),
    woodspringPlan: bySlug("woodspring-suites"),
    everhomeInvestigation: bySlug("everhome-suites"),
    expectedRouting: {
      "design-hotels": "remediation_apply",
      "small-luxury-hotels-of-the-world": "apply_draft",
      "tribute-portfolio": "remediation_apply",
      "woodspring-suites": "remediation_apply",
      "everhome-suites": "investigate_exception",
    },
    routingMatch: {
      "design-hotels": routeBySlug("design-hotels")?.recommendedAction === "remediation_apply",
      "small-luxury-hotels-of-the-world":
        routeBySlug("small-luxury-hotels-of-the-world")?.recommendedAction === "apply_draft",
      "tribute-portfolio": routeBySlug("tribute-portfolio")?.recommendedAction === "remediation_apply",
      "woodspring-suites": routeBySlug("woodspring-suites")?.recommendedAction === "remediation_apply",
      "everhome-suites":
        routeBySlug("everhome-suites")?.recommendedAction === "investigate_exception",
    },
  };
}

export function writeV36DReports(report, rootDir = ROOT) {
  const reportsDir = path.join(rootDir, "reports");
  const docsDir = path.join(rootDir, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const mdPath = path.join(reportsDir, REPORT_MD);
  let md = markdownBatchTable(report.batchTable);
  md += `\n\nGenerated: ${report.generatedAt}\nMode: **${report.mode}**\nAirtable writes: **${report.airtableWritesExecuted}**\n`;
  md += `\n## Routing match vs expected\n\n`;
  for (const [slug, ok] of Object.entries(report.routingMatch || {})) {
    md += `- \`${slug}\`: expected \`${report.expectedRouting[slug]}\` → ${ok ? "MATCH" : "MISMATCH"}\n`;
  }
  fs.writeFileSync(mdPath, md);

  const docPath = path.join(docsDir, "brand-explorer-v36d-action-router.md");
  fs.writeFileSync(
    docPath,
    [
      "# Brand Explorer v36D Action Router",
      "",
      "Generic action router + remediation executor (dry-run by default).",
      "",
      "## Modules",
      "",
      "| Module | Path |",
      "|--------|------|",
      "| Action router | `lib/partner-intelligence/brand-explorer-action-router.js` |",
      "| Remediation executor | `lib/partner-intelligence/brand-explorer-remediation-executor.js` |",
      "| Patch builder | `lib/partner-intelligence/brand-explorer-remediation-patch-builder.js` |",
      "| Apply gate enforcer | `lib/partner-intelligence/brand-explorer-apply-gate-enforcer.js` |",
      "",
      "## Run",
      "",
      "```bash",
      "npm run brand-explorer-v36d-action-router -- --brands design-hotels,small-luxury-hotels-of-the-world,tribute-portfolio,woodspring-suites,everhome-suites --dry-run",
      "```",
      "",
      "Requires prior: `reports/brand-explorer-v36c-remediation-planner.json`",
      "",
      "See `reports/brand-explorer-v36d-action-router.md`.",
      "",
    ].join("\n")
  );

  if (report.designHotelsPlan) {
    fs.writeFileSync(
      path.join(reportsDir, "brand-explorer-v36d-design-hotels-remediation-plan.md"),
      writeDesignHotelsPlanMd(report.designHotelsPlan)
    );
  }
  if (report.slhApplyDraftPlan) {
    fs.writeFileSync(
      path.join(reportsDir, "brand-explorer-v36d-slh-apply-draft-plan.md"),
      writeSlhApplyDraftMd(report.slhApplyDraftPlan)
    );
  }
  if (report.tributePlan) {
    fs.writeFileSync(
      path.join(reportsDir, "brand-explorer-v36d-tribute-remediation-plan.md"),
      writeTributeMd(report.tributePlan)
    );
  }
  if (report.woodspringPlan) {
    fs.writeFileSync(
      path.join(reportsDir, "brand-explorer-v36d-woodspring-remediation-plan.md"),
      writeWoodspringMd(report.woodspringPlan)
    );
  }
  if (report.everhomeInvestigation) {
    fs.writeFileSync(
      path.join(reportsDir, "brand-explorer-v36d-everhome-exception-investigation.md"),
      writeEverhomeMd(report.everhomeInvestigation)
    );
  }

  return { jsonPath, mdPath, docPath };
}
