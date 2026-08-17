/**
 * Choice legacy mini-batch consolidated status report (read-only).
 */
import { fetchBrandSources } from "./choice-legacy-brand-source-package.js";
import {
  buildBatchPipelineCommands,
  getBatchDefinition,
  getBatchExtractBrandConfigs,
} from "./choice-legacy-batch-config.js";

export function buildMiniBatchStatusReport({
  batchName,
  sourcePackageReport,
  urlCaptureReport = null,
  governanceReport = null,
}) {
  const batch = getBatchDefinition(batchName);
  const governanceById = new Map(
    (governanceReport?.brands || [])
      .filter((b) => b.recordId)
      .map((b) => [b.recordId, b])
  );

  const urlByBrand = new Map();
  for (const row of urlCaptureReport?.urls || urlCaptureReport?.brands || []) {
    const key = row.brandKey || row.key;
    if (!urlByBrand.has(key)) urlByBrand.set(key, []);
    urlByBrand.get(key).push(row);
  }

  const brands = (sourcePackageReport?.brands || []).map((row) => {
    const gov = governanceById.get(row.recordId) || null;
    const urlRows = urlByBrand.get(row.key) || [];
    const extractConfig = getBatchExtractBrandConfigs(batchName).find((b) => b.key === row.key);

    return {
      brandName: row.brandName,
      brandKey: row.key,
      recordId: row.recordId,
      explorerStatus: row.explorerActive ? "Active" : "Not Active",
      profileStatus: row.profileStatus,
      governanceStatus: row.governanceStatus || gov?.governance?.liveValidationStatus || null,
      platformReady: gov?.profileStatus === "Platform Ready",
      localPdfCandidates: row.localPdf?.localFilePath ? [row.localPdf.localFilePath] : [],
      primaryPdfRecommendation: row.localPdf?.localFilePath || null,
      consumerUrl: row.consumerUrl,
      consumerUrlConfidence: row.consumerUrlConfidence,
      pressKitUrl: row.pressKitUrl,
      pressKitConfidence: row.pressKitConfidence,
      developmentUrl: row.developmentUrl,
      developmentJsShellRisk: row.developmentJsShellRisk,
      developmentRecommendation: row.developmentRecommendation,
      regionalCaveats: row.regionalCaveats || [],
      rhgCaveats: row.rhgLocalWarnings || [],
      duplicateCheck: row.duplicateCheck,
      readyForPdfRegistration: row.readyForPdfRegistration,
      readyForUrlCapture: row.readyForUrlCapture,
      readyForBatchSourceStewardship: (extractConfig?.allowlistedSourceIds?.length || 0) > 0,
      readyForExtraction: false,
      readyForFactStewardship: false,
      readyForGovernancePublish: false,
      splitOutRecommended: row.splitOutRecommended,
      splitOutReason: row.splitOutReason,
      urlCaptureRows: urlRows,
      piSourceCount: row.piSourceCount,
      approvedSourceCount: row.approvedSourceCount,
    };
  });

  const pipeline = buildBatchPipelineCommands(batchName);

  return {
    generatedAt: new Date().toISOString(),
    batchName,
    batchDisplayName: batch.displayName,
    mode: "dry_run",
    airtableModified: false,
    brands,
    summary: {
      totalBrands: brands.length,
      readyForPdfRegistration: brands.filter((b) => b.readyForPdfRegistration).length,
      readyForUrlCapture: brands.filter((b) => b.readyForUrlCapture).length,
      splitOutRecommended: brands.filter((b) => b.splitOutRecommended).length,
      platformReady: brands.filter((b) => b.platformReady).length,
    },
    batchApplyCommands: pipeline,
    perBrandFallbackCommands: brands.map(
      (b) =>
        `npm run choice-legacy-brand-source-package-batch -- --batch ${batchName} --dry-run --brand ${b.brandKey}`
    ),
  };
}

export async function enrichMiniBatchStatusWithLiveSources(statusReport, batchName) {
  for (const brand of statusReport.brands) {
    const sources = await fetchBrandSources(brand.recordId);
    brand.liveSourceCount = sources.length;
    brand.liveApprovedSources = sources.filter((s) => s.approvedForExplorerUse === "Yes").length;
    const extractConfig = getBatchExtractBrandConfigs(batchName).find((b) => b.key === brand.brandKey);
    const hasAllowlist = (extractConfig?.allowlistedSourceIds?.length || 0) > 0;
    brand.readyForBatchSourceStewardship = hasAllowlist || brand.liveSourceCount >= 3;
    brand.readyForExtraction = brand.liveApprovedSources >= 1 && hasAllowlist;
  }
  return statusReport;
}

export function buildMiniBatchStatusMarkdown(report) {
  const lines = [
    `# ${report.batchDisplayName} — Status`,
    "",
    `Generated: ${report.generatedAt}`,
    `Batch: **${report.batchName}**`,
    "",
    "## Summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Brands | ${report.summary.totalBrands} |`,
    `| Ready for PDF registration | ${report.summary.readyForPdfRegistration} |`,
    `| Ready for URL capture | ${report.summary.readyForUrlCapture} |`,
    `| Split-out recommended | ${report.summary.splitOutRecommended} |`,
    `| Already Platform Ready | ${report.summary.platformReady} |`,
    "",
    "## Batch apply commands (do not run until dry-run reviewed)",
    "",
    "```bash",
    report.batchApplyCommands.sourcePackageApply,
    report.batchApplyCommands.urlCaptureApply,
    report.batchApplyCommands.stewardshipApply,
    report.batchApplyCommands.extractApply,
    report.batchApplyCommands.factStewardshipApply,
    report.batchApplyCommands.governancePublishApply,
    "```",
    "",
    "## Brands",
    "",
  ];

  for (const b of report.brands) {
    lines.push(`### ${b.brandName}`, "");
    lines.push(
      `- Record: \`${b.recordId}\``,
      `- Explorer: **${b.explorerStatus}** · Profile: **${b.profileStatus}**`,
      `- Governance: **${b.governanceStatus || "—"}**`,
      `- Primary PDF: \`${b.primaryPdfRecommendation || "—"}\``,
      `- Consumer URL: ${b.consumerUrl} (**${b.consumerUrlConfidence}**)`,
      `- Press kit: ${b.pressKitUrl || "—"} (**${b.pressKitConfidence}**)`,
      `- Development: ${b.developmentUrl} · JS-shell risk **${b.developmentJsShellRisk}**`,
      `- Duplicate: ${b.duplicateCheck ? `\`${b.duplicateCheck.sourceId}\`` : "none"}`,
      `- Ready — PDF reg: **${b.readyForPdfRegistration}** · URL capture: **${b.readyForUrlCapture}** · stewardship: **${b.readyForBatchSourceStewardship}**`,
      `- Split out: **${b.splitOutRecommended ? "yes" : "no"}**${b.splitOutReason ? ` (${b.splitOutReason})` : ""}`
    );
    if (b.regionalCaveats?.length) {
      lines.push("- Caveats:");
      for (const c of b.regionalCaveats) lines.push(`  - ${c}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}
