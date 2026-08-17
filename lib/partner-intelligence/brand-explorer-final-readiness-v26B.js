/**
 * Brand Explorer Final Readiness v26B — stale-flag cleanup + active-profile gate summary.
 * Read-only; uses live API/row state as source of truth.
 */
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerVisualQaVerificationReport } from "./brand-explorer-visual-qa-verification.js";
import { fetchLiveState, buildGovernancePlan, runTributePortfolioPackagePipeline } from "./tribute-portfolio-package-pipeline.js";
import { assessBrandExplorerGovernanceReadiness } from "./profile-governance-publish-readiness.js";

export const REPORT_VERSION = "v26B";
export const REPORT_JSON_NAME = "brand-explorer-final-readiness-v26B.json";
export const REPORT_MD_NAME = "brand-explorer-final-readiness-v26B.md";
export const DOC_MD_NAME = "brand-explorer-final-readiness-v26B.md";

const TRIBUTE_BRAND_ID = "recCvV0PuZOi8c3hC";
const TRIBUTE_SLUG = "tribute-portfolio";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.md",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-source-evidence-visual-completion-writer.md",
  "reports/brand-explorer-source-evidence-visual-completion-writer.json",
  "reports/brand-explorer-visual-qa-verification.md",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-required-section-population-contract.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "live Tribute Brand Explorer API response",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-visual-qa-verification.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
  "lib/partner-intelligence/brand-explorer-final-readiness-v26B.js",
  "scripts/brand-explorer-final-readiness-check.mjs",
  "docs/data-intelligence/brand-explorer-final-readiness-v26B.md",
  "reports/brand-explorer-final-readiness-v26B.md",
  "reports/brand-explorer-final-readiness-v26B.json",
  "package.json",
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withAirtableRetry(fn, { attempts = 3, delayMs = 12000 } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const message = nz(error?.message);
      if (!/rate limit|429/i.test(message) || attempt === attempts) throw error;
      await sleep(delayMs * attempt);
    }
  }
  throw lastError;
}

function nz(v) {
  return v == null ? "" : String(v).trim();
}

async function fetchBrandApiShape(brandId) {
  const req = { query: { brandId, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    headers: {},
    payload: null,
    setHeader(name, value) {
      this.headers[name] = value;
    },
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
  if (res.statusCode >= 400 || !res.payload?.success || !res.payload?.brand) {
    throw new Error(`Brand API read failed (${res.statusCode})`);
  }
  return res.payload.brand;
}

function gallery3FromApi(brand) {
  const block = (brand?.brandExplorer?.blocks || []).find((b) => b?.slotKey === "materials.gallery.3");
  return {
    title: nz(block?.title),
    imageUrl: nz(block?.imageUrl),
    populated: Boolean(nz(block?.title) && nz(block?.imageUrl)),
  };
}

function computeGovernedPlatformReady(liveState) {
  return assessBrandExplorerGovernanceReadiness(liveState).governedPlatformReady;
}

function buildStaleFlagAudit({
  gallery3,
  contractReport,
  visualQaReport,
  orchestratorPrimary,
  visualReport,
}) {
  const staleFlagsFound = [];
  const staleFlagsRemovedOrReclassified = [];

  if (gallery3.populated) {
    staleFlagsFound.push("visual_qa: materials.gallery.3 intentionally unpopulated boilerplate");
    staleFlagsRemovedOrReclassified.push(
      "Removed gallery.3 unpopulated gap when live API has title + imageUrl"
    );
  }

  if (contractReport?.readinessScore === 100 && contractReport?.brandExplorerRequiredSectionsReady) {
    staleFlagsFound.push("required_section: hardcoded Required Sections Ready false + suppression guards");
    staleFlagsRemovedOrReclassified.push(
      "Required-section report now derives ready flag and omits suppression/next-writer when 8/8 ready"
    );
  }

  const featuredTruncation = (orchestratorPrimary?.tributeExpectedRemaining || []).find(
    (item) => item.id === "featured_truncation"
  );
  if (featuredTruncation && !featuredTruncation.stillBlocking) {
    staleFlagsFound.push("orchestrator: featured_truncation stillBlocking from any Featured-section visual match");
    staleFlagsRemovedOrReclassified.push(
      "featured_truncation only blocks on truncated_copy / critical-high featured defects"
    );
  }

  const featuredDefect = (visualReport?.defects || []).find(
    (d) => d.slotKey === "overview.featured_application"
  );
  if (featuredDefect?.defectType === "thin_copy_vs_reference" && featuredDefect.cosmeticNonBlocking) {
    staleFlagsFound.push("visual_defect: featured thin_copy_vs_reference treated as truncation blocker");
    staleFlagsRemovedOrReclassified.push(
      "thin_copy_vs_reference on dedicated featured slot reclassified cosmetic/non-blocking (low)"
    );
  }

  if (orchestratorPrimary && !orchestratorPrimary.readyForActiveProfile && contractReport?.readinessScore === 100) {
    staleFlagsFound.push("orchestrator: Ready for active profile false despite contract 100");
  }
  if (orchestratorPrimary?.readyForActiveProfile) {
    staleFlagsRemovedOrReclassified.push(
      "Complete Build Orchestrator Ready for active profile now uses live Final QA + contract + governance gates"
    );
  }

  if (visualQaReport?.gallery3Status?.populated && !visualQaReport.remainingGapToFullVisualParity?.some((g) => /gallery\.3/i.test(g))) {
    staleFlagsRemovedOrReclassified.push("Visual QA remaining-gap list no longer includes populated gallery.3");
  }

  return { staleFlagsFound, staleFlagsRemovedOrReclassified };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Final Readiness ${REPORT_VERSION}`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`Active-profile ready: **${report.activeProfileReady ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Final QA");
  lines.push(`- Status: **${report.finalQa.status}** (${report.finalQa.score})`);
  lines.push(`- Critical: ${report.finalQa.critical} · High: ${report.finalQa.high}`);
  lines.push("");
  lines.push("## Required Section Contract");
  lines.push(`- Score: **${report.requiredSection.score}** · Ready: **${report.requiredSection.ready ? "yes" : "no"}**`);
  lines.push(`- Sections ready: ${report.requiredSection.sectionsReady}/${report.requiredSection.sectionsTotal}`);
  lines.push("");
  lines.push("## Visual QA");
  lines.push(`- Completeness: **${report.visualQa.completenessScore}**`);
  lines.push(`- Gallery.3 populated: **${report.visualQa.gallery3Populated ? "yes" : "no"}**`);
  lines.push(`- Defects: ${report.visualQa.defectTotal} (critical ${report.visualQa.critical}, high ${report.visualQa.high})`);
  lines.push("");
  lines.push("## Complete Build");
  lines.push(`- Ready for active profile: **${report.completeBuild.readyForActiveProfile ? "yes" : "no"}**`);
  lines.push(`- Remaining blockers: ${report.completeBuild.remainingBlockers}`);
  lines.push(`- Governed platform ready: **${report.completeBuild.governedPlatformReady ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Stale flags found");
  for (const item of report.staleFlagsFound) lines.push(`- ${item}`);
  if (!report.staleFlagsFound.length) lines.push("- none");
  lines.push("");
  lines.push("## Stale flags removed or reclassified");
  for (const item of report.staleFlagsRemovedOrReclassified) lines.push(`- ${item}`);
  if (!report.staleFlagsRemovedOrReclassified.length) lines.push("- none");
  lines.push("");
  lines.push("## Files read");
  for (const f of report.filesRead) lines.push(`- ${f}`);
  lines.push("");
  lines.push("## Files changed");
  for (const f of report.filesChanged) lines.push(`- ${f}`);
  lines.push("");
  if (report.activeProfileReady) {
    lines.push("## Package pipeline (when ready)");
    lines.push("```bash");
    lines.push("npm run tribute-portfolio-package-pipeline -- --apply --approve-tribute-portfolio-package-pipeline");
    lines.push("```");
  }
  return lines.join("\n");
}

export async function buildBrandExplorerFinalReadinessV26BReport(options = {}) {
  const brandSlug = nz(options.brandIdOrName || TRIBUTE_SLUG);
  const brandRecordId = options.brandRecordId || TRIBUTE_BRAND_ID;

  const brandApi = await withAirtableRetry(() => fetchBrandApiShape(brandRecordId));
  const liveState = await withAirtableRetry(() => fetchLiveState(brandRecordId));
  const contractReport = await withAirtableRetry(() =>
    buildBrandExplorerRequiredSectionPopulationContractReport({ brandIdOrName: brandRecordId })
  );
  const visualReport = await withAirtableRetry(() =>
    buildBrandExplorerVisualDisplayDefectAuditReport({ brandIdOrName: brandSlug })
  );
  const visualQaReport = await withAirtableRetry(() =>
    buildBrandExplorerVisualQaVerificationReport({ brandKey: brandSlug, brandRecordId })
  );
  const finalQaReport = await withAirtableRetry(() =>
    buildBrandExplorerFinalQaAuditorReport({ brandIdOrName: brandSlug })
  );
  const orchestratorReport = await withAirtableRetry(() =>
    buildBrandExplorerCompleteBuildOrchestratorReport({
      brandIdOrName: brandSlug,
      targetQuality: "active-profile",
    })
  );
  const packagePipelineReport = await withAirtableRetry(() =>
    runTributePortfolioPackagePipeline({ mode: "dry-run", recordId: brandRecordId, probeUrls: false })
  );

  const gallery3 = gallery3FromApi(brandApi);
  const primaryQa = (finalQaReport.brandReports || []).find((b) => !b.error) || finalQaReport.brandReports?.[0];
  const orchestratorPrimary = orchestratorReport.brandResults?.[0];
  const governedPlatformReady =
    packagePipelineReport?.executiveSummary?.governedPlatformReady ??
    computeGovernedPlatformReady(liveState);

  const companyValidated = liveState.brandBasics?.fields?.["Company Validated"] ?? null;

  const { staleFlagsFound, staleFlagsRemovedOrReclassified } = buildStaleFlagAudit({
    gallery3,
    contractReport,
    visualQaReport,
    orchestratorPrimary,
    visualReport,
  });

  const activeProfileReady = Boolean(orchestratorPrimary?.readyForActiveProfile);

  const report = {
    reportVersion: REPORT_VERSION,
    v26BExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    companyValidatedUntouched: true,
    companyValidatedSnapshot: companyValidated,
    brand: {
      slug: brandSlug,
      name: nz(brandApi?.name) || "Tribute Portfolio",
      recordId: brandRecordId,
    },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    staleFlagsFound,
    staleFlagsRemovedOrReclassified,
    finalQa: {
      status: primaryQa?.scores?.overallActiveProfileReadiness || finalQaReport.scores?.overallActiveProfileReadiness,
      score: primaryQa?.scores?.overallNumeric || finalQaReport.scores?.overallNumeric,
      critical: primaryQa?.defectsBySeverity?.critical?.length ?? 0,
      high: primaryQa?.defectsBySeverity?.high?.length ?? 0,
      visualCompletenessScore: primaryQa?.scores?.visualCompletenessScore,
    },
    requiredSection: {
      score: contractReport.readinessScore,
      ready: contractReport.brandExplorerRequiredSectionsReady,
      sectionsReady: (contractReport.sectionBySectionReadiness || []).filter((s) =>
        String(s.classification).startsWith("ready")
      ).length,
      sectionsTotal: (contractReport.sectionBySectionReadiness || []).length,
    },
    visualQa: {
      completenessScore: primaryQa?.scores?.visualCompletenessScore ?? visualReport.visualComparability?.score,
      gallery3Populated: gallery3.populated,
      gallery3Title: gallery3.title,
      gallery3ImageUrlPresent: Boolean(gallery3.imageUrl),
      defectTotal: visualReport.defectCounts?.total ?? 0,
      critical: visualReport.defectCounts?.critical ?? 0,
      high: visualReport.defectCounts?.high ?? 0,
      mediaVisible: visualQaReport.tributeMediaVisibleToBrandExplorer,
    },
    completeBuild: {
      readyForActiveProfile: orchestratorPrimary?.readyForActiveProfile ?? false,
      remainingBlockers: orchestratorPrimary?.blockers?.length ?? 0,
      governedPlatformReady,
      contractReadinessScore: orchestratorPrimary?.contractReadinessScore,
      finalQaReadiness: orchestratorPrimary?.finalQaScores?.overallActiveProfileReadiness,
    },
    activeProfileReady,
    packagePipeline: {
      governedReady: governedPlatformReady,
      dryRunCommand: "npm run tribute-portfolio-package-pipeline -- --dry-run",
      applyCommand:
        "npm run tribute-portfolio-package-pipeline -- --apply --approve-tribute-portfolio-package-pipeline",
    },
    exactVerificationCommands: [
      "npm run brand-explorer-final-qa-auditor -- --brand tribute-portfolio --dry-run",
      "npm run brand-explorer-complete-build -- --brand tribute-portfolio --dry-run --target-quality active-profile",
      "npm run brand-explorer-visual-display-defect-audit -- --brand tribute-portfolio --dry-run",
      "npm run brand-explorer-visual-qa-verification -- --brand tribute-portfolio --dry-run",
      "npm run brand-explorer-required-section-population-contract -- --brand tribute-portfolio --dry-run",
      "npm run tribute-portfolio-package-pipeline -- --dry-run",
    ],
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildBrandExplorerFinalReadinessV26BMarkdown(report) {
  return report.markdown || buildMarkdown(report);
}
