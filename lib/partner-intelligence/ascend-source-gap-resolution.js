/**
 * Ascend Hotel Collection — source gap resolution (dry-run default).
 * Prepares Ascend for Choice Legacy Batch Pipeline without registering sources.
 * @see docs/data-intelligence/choice-legacy-batch-pipeline-v1.md
 */
import fs from "fs";
import path from "path";
import {
  CHOICE_LEGACY_BRANDS,
  COMPANY_FOLDER,
  fetchBrandSources,
  planChoiceLegacyBrandPackage,
  scanLocalBrandFiles,
} from "./choice-legacy-brand-source-package.js";
import {
  classifyDevelopmentPageRecommendation,
  probeDevelopmentUrlFromFixture,
  probeDevelopmentUrlLive,
} from "./choice-legacy-brand-source-package-batch.js";
import {
  downloadUrlWithFallback,
  estimateReadableTextLength,
} from "./choice-legacy-batch-url-capture.js";
import { buildPipelineApplyCommand } from "./choice-legacy-batch-pipeline.js";
import { readLocalSourceText } from "./extract-source-text.js";
import { resolveReferenceRoot } from "./reference-material-paths.js";

export const ASCEND_RECORD_ID = "reclkgOzvAcBheUSo";
export const REPORT_JSON_NAME = "ascend-source-gap-resolution.json";
export const REPORT_MD_NAME = "ascend-source-gap-resolution.md";

const FDD_STEM = "35768-202604-08";
const DEFAULT_FDD_DIR =
  process.env.CHOICE_FDD_DIR ||
  "G:\\My Drive\\Dealality™\\Platform Design & Build\\Brand Reference Material\\Choice Hotels International\\FDDs";

export function getAscendBrandConfig() {
  const brand =
    CHOICE_LEGACY_BRANDS.find((b) => b.key === "ascend-hotel-collection") ||
    CHOICE_LEGACY_BRANDS.find((b) => b.recordId === ASCEND_RECORD_ID);
  if (!brand) {
    throw new Error("Ascend Hotel Collection not found in CHOICE_LEGACY_BRANDS");
  }
  return brand;
}

export function scanAscendFddOnDisk(fddDir = DEFAULT_FDD_DIR) {
  const candidates = [];
  if (!fs.existsSync(fddDir)) {
    return { fddDir, exists: false, candidates: [], recommended: null };
  }
  for (const name of fs.readdirSync(fddDir)) {
    if (!/\.pdf$/i.test(name)) continue;
    if (!/ascend|35768/i.test(name)) continue;
    const fullPath = path.join(fddDir, name);
    const st = fs.statSync(fullPath);
    candidates.push({
      filename: name,
      absolutePath: fullPath,
      sizeBytes: st.size,
      stem: FDD_STEM,
    });
  }
  const recommended =
    candidates.find((c) => c.filename.includes(FDD_STEM)) || candidates[0] || null;
  return { fddDir, exists: true, candidates, recommended };
}

export function scanBroaderAscendReferenceFiles(referenceRoot = resolveReferenceRoot()) {
  const hits = [];
  const companyDir = path.join(referenceRoot, COMPANY_FOLDER);
  if (!fs.existsSync(companyDir)) return hits;

  function walk(abs, relParts) {
    if (!fs.existsSync(abs)) return;
    for (const name of fs.readdirSync(abs)) {
      if (name.startsWith(".")) continue;
      const childAbs = path.join(abs, name);
      const rel = [...relParts, name].join("/");
      if (fs.statSync(childAbs).isDirectory()) {
        if (/ascend/i.test(name) || /ascend/i.test(rel)) walk(childAbs, [...relParts, name]);
        else if (relParts.length < 3) walk(childAbs, [...relParts, name]);
        continue;
      }
      if (!/ascend/i.test(name) && !/ascend/i.test(rel)) continue;
      if (!/\.(pdf|html|htm|txt|md)$/i.test(name)) continue;
      let textLength = null;
      try {
        const doc = readLocalSourceText(`${COMPANY_FOLDER}/${rel}`);
        textLength = String(doc.text || "").length;
      } catch {
        textLength = 0;
      }
      hits.push({
        relativePath: `${COMPANY_FOLDER}/${rel}`,
        filename: name,
        textLength,
        readable: (textLength || 0) > 0,
      });
    }
  }
  walk(companyDir, []);
  return hits;
}

export async function probeAscendUrl(url, slot) {
  if (!url) {
    return { slot, url: null, status: "missing", error: "no_url" };
  }
  try {
    const dl = await downloadUrlWithFallback(url);
    const ext = dl.ext || (/\.pdf/i.test(url) ? ".pdf" : ".html");
    const readableTextLength = estimateReadableTextLength(dl.buf, dl.contentType, ext);
    return {
      slot,
      url,
      status: dl.httpStatus >= 200 && dl.httpStatus < 400 ? "reachable" : "failed",
      httpStatus: dl.httpStatus,
      contentType: dl.contentType,
      bytes: dl.buf?.length ?? 0,
      readableTextLength,
      finalUrl: dl.finalUrl || url,
      error: null,
    };
  } catch (err) {
    return { slot, url, status: "failed", error: err.message || String(err) };
  }
}

export async function probeAscendDevelopmentPage(developmentUrl) {
  const fixture = probeDevelopmentUrlFromFixture(developmentUrl);
  let live = { probed: false };
  try {
    live = await probeDevelopmentUrlLive(developmentUrl);
  } catch (err) {
    live = { probed: true, error: err.message || String(err) };
  }
  const recommendation = classifyDevelopmentPageRecommendation(fixture, live, false);
  return {
    url: developmentUrl,
    fixture,
    live: {
      probed: live.probed,
      httpStatus: live.httpStatus ?? null,
      htmlLength: live.htmlLength ?? null,
      extractableTextLength: live.extractableTextLength ?? null,
      likelyJsShell: live.likelyJsShell ?? null,
      jsShellMarkers: live.jsShellMarkers || [],
      error: live.error || null,
    },
    jsShellRisk: recommendation.risk,
    recommendation: recommendation.recommendation,
    provenanceOnly: recommendation.recommendation?.includes("provenance") ?? true,
  };
}

export function buildAscendSourcePackageRecommendation({
  localScan,
  fddScan,
  broaderScan,
  packagePlan,
  urlProbes,
  developmentProbe,
}) {
  const localPdfs = [
    ...(localScan.found || []).filter((f) => f.ext === ".pdf"),
    ...(broaderScan || []).filter((f) => f.readable && /\.pdf$/i.test(f.filename)),
  ];
  const primaryPdf =
    localPdfs.find((f) => /brochure--ascend/i.test(f.relativePath || f.filename)) ||
    localPdfs.find((f) => /one.?pager/i.test(f.relativePath || f.filename)) ||
    localPdfs.find((f) => /gtm deck/i.test(f.relativePath || f.filename)) ||
    localPdfs[0];

  const consumerOk =
    (urlProbes.consumer?.readableTextLength ?? 0) >= 500 ||
    (urlProbes.consumer?.status === "failed" && packagePlan.proposedP0?.some((p) => p.role === "p0_consumer_page" && p.confidence === "verified"));
  const pressOk =
    (urlProbes.press?.readableTextLength ?? 0) >= 200 ||
    (urlProbes.press?.status === "failed" && packagePlan.proposedP0?.some((p) => p.role === "p0_press_kit" && p.confidence === "verified"));

  const hasFdd = Boolean(fddScan.recommended);
  const hasLocalPdf = Boolean(primaryPdf);

  let recommendation;
  if (hasLocalPdf) {
    recommendation = "register_local_brochure_plus_consumer_press";
  } else if (consumerOk && pressOk) {
    recommendation = "proceed_consumer_plus_press_no_local_pdf";
  } else if (hasFdd) {
    recommendation = "optional_fdd_secondary_consumer_press_primary";
  } else {
    recommendation = "hold_until_official_sources_captured";
  }

  const pipelineReady =
    hasLocalPdf &&
    packagePlan.proposedP0?.some((p) => p.role === "p0_press_kit" && p.confidence === "verified");

  return {
    recommendation,
    holdReason: pipelineReady ? null : "register_local_pdf_and_url_capture_first",
    canProceedWithoutLocalPdf: consumerOk && pressOk && !hasLocalPdf,
    pipelineReady,
    primaryPath: hasLocalPdf
      ? `local PDF (${primaryPdf.relativePath}) + consumer + press; development provenance-only`
      : consumerOk && pressOk
        ? "consumer_page + press_kit (development page provenance-only)"
        : hasFdd
          ? "register FDD as secondary; still capture consumer + press"
          : "hold",
    primaryPdfRecommendation: primaryPdf?.relativePath || null,
    optionalFdd: hasFdd
      ? {
          stem: FDD_STEM,
          path: fddScan.recommended?.absolutePath || null,
          note: "Use for Item 19 / fee facts only — not primary positioning",
        }
      : null,
    localPdfGap: !hasLocalPdf,
    priorAuditMissReason: hasLocalPdf
      ? "Prior scan used folder names Ascend Hotel Collection / Ascend; files live under Ascend Collection"
      : null,
    miniBatchRecommendation: "mini-batch-3-single-brand",
    miniBatchNote:
      "Process Ascend as sole brand in mini-batch-3 via choice-legacy-batch-pipeline",
  };
}

export async function buildAscendSourceGapResolutionReport({
  governanceRow = null,
  probeUrls = true,
  probeDevelopment = true,
} = {}) {
  const brand = getAscendBrandConfig();
  const localScan = scanLocalBrandFiles(brand.referenceFolderCandidates);
  const fddScan = scanAscendFddOnDisk();
  const broaderScan = scanBroaderAscendReferenceFiles();
  const existingSources = await fetchBrandSources(brand.recordId);
  const packagePlan = planChoiceLegacyBrandPackage(brand, {
    existingSources,
    governanceRow,
  });

  let urlProbes = {};
  if (probeUrls) {
    urlProbes.consumer = await probeAscendUrl(brand.consumerPage?.url, "consumer");
    urlProbes.press = await probeAscendUrl(brand.pressKit?.url, "press");
  }

  let developmentProbe = null;
  if (probeDevelopment && brand.developmentPage?.url) {
    developmentProbe = await probeAscendDevelopmentPage(brand.developmentPage.url);
  }

  const duplicateChecks = {
    existingSourceCount: existingSources.length,
    sources: existingSources.map((s) => ({
      sourceId: s.id,
      sourceTitle: s.sourceTitle,
      sourceUrl: s.sourceUrl,
      localFilePath: s.localFilePath,
      status: s.status,
    })),
    proposedDuplicates: (packagePlan.proposedP0 || []).map((p) => ({
      role: p.role,
      url: p.sourceUrl,
      registrationStatus: p.registrationStatus,
      duplicate: p.duplicate || null,
      alreadyRegistered: p.alreadyRegistered || false,
    })),
  };

  const sourceRecommendation = buildAscendSourcePackageRecommendation({
    localScan,
    fddScan,
    broaderScan,
    packagePlan,
    urlProbes,
    developmentProbe,
  });

  const nextApplyCommand = sourceRecommendation.pipelineReady
    ? buildPipelineApplyCommand("mini-batch-3")
    : null;

  return {
    resolutionVersion: "1",
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    brandName: brand.brandName,
    brandKey: brand.key,
    recordId: brand.recordId,
    explorerStatus: packagePlan.explorerActive ? "Active" : "Not Active",
    profileStatus: governanceRow?.profileStatus || "Active — Evidence Package Needed",
    governanceStatus: governanceRow?.governance?.liveValidationStatus || null,
    localFiles: {
      brandFolderScan: localScan,
      localPdfCount: (localScan.found || []).filter((f) => f.ext === ".pdf").length,
      broaderAscendHits: broaderScan,
      fddOnDisk: fddScan,
    },
    urls: {
      consumer: {
        url: brand.consumerPage?.url,
        confidence: brand.consumerPage?.confidence,
        probe: urlProbes.consumer || null,
      },
      pressKit: {
        url: brand.pressKit?.url,
        confidence: brand.pressKit?.confidence,
        probe: urlProbes.press || null,
      },
      development: developmentProbe || {
        url: brand.developmentPage?.url,
        confidence: brand.developmentPage?.confidence,
      },
    },
    duplicateChecks,
    packagePlan: {
      missingSourceTypes: packagePlan.missingSourceTypes,
      jsShellRisk: packagePlan.jsShellRisk,
      proposedP0: packagePlan.proposedP0,
      recommendedCaptureCommands: packagePlan.recommendedCaptureCommands,
    },
    sourcePackageRecommendation: sourceRecommendation,
    canProceedThroughPipeline: sourceRecommendation.pipelineReady,
    processAloneOrBatch: sourceRecommendation.miniBatchRecommendation,
    nextSteps: [
      "npm run choice-legacy-batch-pipeline -- --batch mini-batch-3 --dry-run",
      sourceRecommendation.pipelineReady
        ? "npm run choice-legacy-batch-pipeline -- --batch mini-batch-3 --apply --approve-choice-legacy-batch-pipeline"
        : "Review pipeline dry-run before apply",
    ],
    nextApplyCommand,
    perStageFallback: packagePlan.recommendedCaptureCommands,
    doesNotDo: [
      "Rebuild Brand Explorer content",
      "Overwrite Brand Setup fields",
      "Register or approve sources in dry-run",
      "Approve facts or publish governance",
      "Set Company Validated or Company Validation Date",
    ],
  };
}

export function buildAscendSourceGapResolutionMarkdown(report) {
  const rec = report.sourcePackageRecommendation;
  const lines = [
    `# Ascend Hotel Collection — Source Gap Resolution`,
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    "",
    "## Summary",
    "",
    `| Field | Value |`,
    `|-------|-------|`,
    `| Brand record | \`${report.recordId}\` |`,
    `| Explorer | ${report.explorerStatus} |`,
    `| Profile status | ${report.profileStatus} |`,
    `| Local PDFs in brand folder | ${report.localFiles.localPdfCount} |`,
    `| FDD on disk | ${report.localFiles.fddOnDisk.recommended ? "yes" : "no"} |`,
    `| Can proceed without local PDF | **${rec.canProceedWithoutLocalPdf ? "yes" : "no"}** |`,
    `| Pipeline ready | **${report.canProceedThroughPipeline ? "yes" : "no"}** |`,
    `| Recommendation | **${rec.recommendation}** |`,
    `| Process as | **${report.processAloneOrBatch}** |`,
    "",
    "## URL probes",
    "",
    `### Consumer — ${report.urls.consumer.url}`,
    "",
    report.urls.consumer.probe
      ? `- HTTP ${report.urls.consumer.probe.httpStatus} · ${report.urls.consumer.probe.bytes} bytes · readable text ${report.urls.consumer.probe.readableTextLength}`
      : "- Not probed",
    "",
    `### Press kit — ${report.urls.pressKit.url}`,
    "",
    report.urls.pressKit.probe
      ? `- HTTP ${report.urls.pressKit.probe.httpStatus} · ${report.urls.pressKit.probe.bytes} bytes · readable text ${report.urls.pressKit.probe.readableTextLength}`
      : "- Not probed",
    "",
    `### Development — ${report.urls.development.url || report.urls.development.url}`,
    "",
    `- JS-shell risk: **${report.urls.development.jsShellRisk || report.packagePlan.jsShellRisk?.risk || "—"}**`,
    `- Recommendation: ${report.urls.development.recommendation || report.packagePlan.jsShellRisk?.reason || "provenance-only"}`,
    "",
    "## Local files",
    "",
    report.localFiles.brandFolderScan.scannedFolders.length
      ? report.localFiles.brandFolderScan.scannedFolders.map((f) => `- Scanned: \`${f}\``).join("\n")
      : "- No brand folders found on disk",
    "",
    report.localFiles.broaderAscendHits.length
      ? report.localFiles.broaderAscendHits.map((f) => `- \`${f.relativePath}\` (${f.textLength} chars)`).join("\n")
      : "- No broader Ascend-named files under Choice reference root",
    "",
    report.localFiles.fddOnDisk.recommended
      ? `- FDD candidate: \`${report.localFiles.fddOnDisk.recommended.filename}\``
      : "- No Ascend FDD PDF found on disk",
    "",
    "## Duplicate checks",
    "",
    `- Existing PI sources: **${report.duplicateChecks.existingSourceCount}**`,
    ...(report.duplicateChecks.proposedDuplicates || []).map(
      (d) => `- ${d.role}: duplicate=${d.duplicate?.isDuplicate ? "yes" : "no"}`
    ),
    "",
    "## Next apply command",
    "",
    report.nextApplyCommand
      ? "```bash\n" + report.nextApplyCommand + "\n```"
      : "_Not ready — complete mini-batch-3 manifest and dry-run first._",
    "",
    "### Prerequisites",
    "",
    ...report.nextSteps.map((s) => `- ${s}`),
    "",
    "## Does not do",
    "",
    ...report.doesNotDo.map((d) => `- ${d}`),
  ];
  return lines.join("\n");
}
