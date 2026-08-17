/**
 * Brand Explorer v36C — remediation planner orchestrator (read-only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { validateBrandV36BContracts } from "./brand-explorer-v36b-contract-validation.js";
import { DEFAULT_TEST_BRANDS } from "./brand-explorer-v36b-contract-validation.js";
import { detectDraftState, draftStateBlocksApplyDraft } from "./brand-explorer-draft-state-detector.js";
import {
  enforceExternalOwnerReadiness,
  enforcePresentationPlan,
} from "./brand-explorer-contract-enforcement.js";
import {
  buildRemediationPlan,
  recommendApplyGate,
  buildDesignHotelsRemediationPlan,
  buildSlhNextAction,
  buildTributeBenchmark,
  buildRegressionCheck,
  remediationPlanMarkdown,
} from "./brand-explorer-remediation-planner.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";

export const V36C_VERSION = "v36C";
export const REPORT_JSON = "brand-explorer-v36c-remediation-planner.json";
export const REPORT_MD = "brand-explorer-v36c-remediation-planner.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function readCompleteBuildReadyFromReport(brandSlug, rootDir = ROOT) {
  const reportPath = path.join(rootDir, "reports", `brand-explorer-complete-build-${brandSlug}.json`);
  if (!fs.existsSync(reportPath)) return null;
  try {
    const data = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    return data.readyForActiveProfile ?? null;
  } catch {
    return null;
  }
}

const REMEDIATION_FILE_KEYS = Object.freeze({
  "design-hotels": "design-hotels",
  "small-luxury-hotels-of-the-world": "slh",
  "tribute-portfolio": "tribute-portfolio",
});

export async function runBrandV36CRemediation(brandSlug, { dryRun = true } = {}) {
  const v36b = await validateBrandV36BContracts(brandSlug, { dryRun });
  const loadCtx = await loadBrandFactoryContext(brandSlug).catch(() => null);
  const brandConfig = getActiveProfileBrandConfig(brandSlug);

  const draftState = detectDraftState({
    brandSlug,
    presentationRows: v36b.presentationRows,
    brandApi: v36b.brandApi,
    assetPack: v36b.factoryReport?.assetPack,
    draftPlan: v36b.factoryReport?.draftPlan,
    factoryRules: v36b.factoryReport?.factoryRules,
    renderContract: v36b.renderContract,
    approvedSourcesCount: v36b.factoryReport?.approvedSourcesCount,
    completeBuildReport: loadCtx?.completeBuildReport,
    brandBasics: loadCtx?.brandBasics,
  });

  if (draftStateBlocksApplyDraft(draftState)) {
    draftState.readyForApplyDraft = false;
  }

  const enforcement = enforceExternalOwnerReadiness({
    brandSlug,
    brandConfig,
    presentationRows: v36b.presentationRows,
    brandApi: v36b.brandApi,
    renderContract: v36b.renderContract,
    factoryRules: v36b.factoryReport?.factoryRules,
    knowledgePack: v36b.knowledgePack,
    approvedSourcesCount: v36b.factoryReport?.approvedSourcesCount,
  });

  const planEnforcement = enforcePresentationPlan(v36b.presentationPlan);

  const remediationPlan = buildRemediationPlan({
    brandSlug,
    brandName: brandConfig?.name,
    enforcement,
    draftState,
    factoryRules: v36b.factoryReport?.factoryRules,
    renderContract: v36b.renderContract,
    presentationPlan: v36b.presentationPlan,
  });

  const applyGate = recommendApplyGate({
    draftState,
    enforcement,
    factoryRules: v36b.factoryReport?.factoryRules,
    renderContract: v36b.renderContract,
    remediationPlan,
  });

  return {
    brandSlug,
    brandName: brandConfig?.name,
    dryRun,
    draftState,
    enforcement,
    planEnforcement,
    remediationPlan,
    applyGate,
    renderContract: v36b.renderContract,
    presentationPlan: v36b.presentationPlan,
    factoryRules: v36b.factoryReport?.factoryRules,
    draftPlan: v36b.factoryReport?.draftPlan,
    v36bScoreComparison: {
      v36b: v36b.externalOwnerScore?.numericScore,
      v36cCalibrated: enforcement.numericScore,
      v36bBand: v36b.externalOwnerScore?.pass ? "pass" : "fail",
      v36cBand: enforcement.band,
    },
    knowledgePackSummary: {
      visualAssetCoverage: v36b.knowledgePack?.visualAssetCoverage,
      tabCoverage: v36b.knowledgePack?.tabCoverage,
    },
    factoryReportSummary: v36b.factoryReportSummary,
    completeBuildReady:
      readCompleteBuildReadyFromReport(brandSlug) ??
      loadCtx?.completeBuildReport?.brandResults?.find((b) => b.brand?.slug === brandSlug)
        ?.readyForActiveProfile ??
      null,
  };
}

export async function runV36CRemediationPlanner({ brands = DEFAULT_TEST_BRANDS, dryRun = true } = {}) {
  const brandResults = [];
  for (const slug of brands) {
    brandResults.push(await runBrandV36CRemediation(slug, { dryRun }));
  }

  const designHotels = brandResults.find((b) => b.brandSlug === "design-hotels");
  const slh = brandResults.find((b) => b.brandSlug === "small-luxury-hotels-of-the-world");
  const tribute = brandResults.find((b) => b.brandSlug === "tribute-portfolio");
  const woodspring = brandResults.find((b) => b.brandSlug === "woodspring-suites");
  const everhome = brandResults.find((b) => b.brandSlug === "everhome-suites");

  const report = {
    version: V36C_VERSION,
    generatedAt: new Date().toISOString(),
    mode: dryRun ? "dry-run" : "live",
    airtableModified: false,
    brands: brandResults.map((b) => b.brandSlug),
    brandResults,
    designHotelsRemediation: designHotels
      ? {
          markdown: buildDesignHotelsRemediationPlan(
            designHotels.remediationPlan,
            designHotels.enforcement,
            designHotels.draftState
          ),
          plan: designHotels.remediationPlan,
          applyGate: designHotels.applyGate,
        }
      : null,
    slhNextAction: slh
      ? buildSlhNextAction({
          draftState: slh.draftState,
          enforcement: slh.enforcement,
          remediationPlan: slh.remediationPlan,
          factoryRules: slh.factoryRules,
          draftPlan: slh.draftPlan,
        })
      : null,
    tributeBenchmark: tribute
      ? buildTributeBenchmark({
          enforcement: tribute.enforcement,
          draftState: tribute.draftState,
          renderContract: tribute.renderContract,
          remediationPlan: tribute.remediationPlan,
          factoryRules: tribute.factoryRules,
        })
      : null,
    regressionChecks: [
      woodspring
        ? buildRegressionCheck("woodspring-suites", {
            enforcement: woodspring.enforcement,
            draftState: woodspring.draftState,
            completeBuildReady: woodspring.completeBuildReady,
            remediationPlan: woodspring.remediationPlan,
          })
        : null,
      everhome
        ? buildRegressionCheck("everhome-suites", {
            enforcement: everhome.enforcement,
            draftState: everhome.draftState,
            completeBuildReady: everhome.completeBuildReady,
            remediationPlan: everhome.remediationPlan,
          })
        : null,
    ].filter(Boolean),
  };

  return report;
}

export function writeV36CReports(report, rootDir = ROOT) {
  const reportsDir = path.join(rootDir, "reports");
  const docsDir = path.join(rootDir, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const mdLines = [
    "# Brand Explorer v36C Remediation Planner",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** — no Airtable writes`,
    "",
    "## Brand summary",
    "",
  ];

  for (const b of report.brandResults) {
    mdLines.push(`### ${b.brandName} (\`${b.brandSlug}\`)`);
    mdLines.push(`- Draft state: **${b.draftState.primaryState}**`);
    mdLines.push(`- Calibrated score: **${b.enforcement.numericScore}/100** (${b.enforcement.band})`);
    mdLines.push(`- v36B → v36C score: ${b.v36bScoreComparison.v36b} → ${b.v36bScoreComparison.v36cCalibrated}`);
    mdLines.push(`- Remediation items: ${b.remediationPlan.itemCount}`);
    mdLines.push(`- Apply gate: **${b.applyGate.recommendation}**`);
    mdLines.push("");
  }

  if (report.slhNextAction) {
    mdLines.push("## SLH next action");
    mdLines.push(`- **${report.slhNextAction.nextStep}**`);
    mdLines.push(`- ${report.slhNextAction.reasons.join("; ")}`);
    mdLines.push("");
  }

  if (report.tributeBenchmark) {
    mdLines.push("## Tribute benchmark");
    mdLines.push(`- Close: ${report.tributeBenchmark.isClose}`);
    mdLines.push(`- Blocks active approval: ${report.tributeBenchmark.blocksActiveApproval.join(", ") || "none"}`);
    mdLines.push("");
  }

  mdLines.push("## Regression (WoodSpring / Everhome)");
  for (const rc of report.regressionChecks) {
    mdLines.push(`- **${rc.brandSlug}**: ${rc.verdict} — ${rc.explanation}`);
  }

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, mdLines.join("\n"));

  const docPath = path.join(docsDir, "brand-explorer-v36c-remediation-planner.md");
  fs.writeFileSync(
    docPath,
    `# Brand Explorer v36C Remediation Planner\n\nContract enforcement + remediation planning (read-only).\n\nSee \`reports/${REPORT_MD}\`.\n`
  );

  if (report.designHotelsRemediation?.markdown) {
    fs.writeFileSync(
      path.join(reportsDir, "brand-explorer-remediation-design-hotels-v36c.md"),
      report.designHotelsRemediation.markdown
    );
  }

  if (report.slhNextAction) {
    fs.writeFileSync(
      path.join(reportsDir, "brand-explorer-remediation-slh-v36c.md"),
      remediationPlanMarkdown(
        report.brandResults.find((b) => b.brandSlug === "small-luxury-hotels-of-the-world")?.remediationPlan || {
          itemCount: 0,
          ownerVisibleCount: 0,
          items: [],
        },
        `SLH Next Action: ${report.slhNextAction.nextStep}`
      ) +
        `\n\n## Decision\n\n- Next step: **${report.slhNextAction.nextStep}**\n- Apply-draft permitted: ${report.slhNextAction.applyDraftPermitted}\n- Active approval blocked: ${report.slhNextAction.activeApprovalBlocked}\n`
    );
  }

  const tributeResult = report.brandResults.find((b) => b.brandSlug === "tribute-portfolio");
  if (tributeResult && report.tributeBenchmark) {
    fs.writeFileSync(
      path.join(reportsDir, "brand-explorer-remediation-tribute-v36c.md"),
      `# Tribute Portfolio Benchmark (v36C)\n\n- Close: ${report.tributeBenchmark.isClose}\n- Lifestyle benchmark: ${report.tributeBenchmark.canServeAsLifestyleBenchmark}\n- Generic remediation viable: ${report.tributeBenchmark.genericRemediationViable}\n\n## Passes\n${report.tributeBenchmark.passes.map((p) => `- ${p}`).join("\n")}\n\n## Blocks active approval\n${report.tributeBenchmark.blocksActiveApproval.map((p) => `- ${p}`).join("\n")}\n`
    );
  }

  return { jsonPath, mdPath, docPath };
}
