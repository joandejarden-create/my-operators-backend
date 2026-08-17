/**
 * Choice legacy brands — mini-batch source package (batch-config driven).
 * Dry-run default; local PDF register only on explicit --apply.
 * @see docs/data-intelligence/choice-legacy-brand-mini-batch-1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createPartnerSource } from "./airtable-source.js";
import {
  CHOICE_LEGACY_BRANDS,
  COMPANY_FOLDER,
  buildCaptureCommand,
  buildLocalSourceFields,
  fetchBrandSources,
  planChoiceLegacyBrandPackage,
} from "./choice-legacy-brand-source-package.js";
import {
  DEFAULT_BATCH_NAME,
  getBatchBrandConfigs,
  getBatchDefinition,
  getBatchPrimaryPdf,
  getBatchReportFiles,
  isRhgContaminatedLocalPath,
} from "./choice-legacy-batch-config.js";
import { resolveLocalSourceAbsolutePath } from "./reference-material-paths.js";
import { readLocalSourceText } from "./extract-source-text.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..");
const FIXTURE_DEV_TEXT_DIR = path.join(ROOT, "fixtures", "choice-dev-site-text");

export const BATCH_VERSION = "1.1";

// Backward-compatible mini-batch 1 report names
export {
  DEFAULT_BATCH_NAME,
  MINI_BATCH_KEYS,
  MINI_BATCH_PRIMARY_PDF,
  BATCH_NAME,
} from "./choice-legacy-batch-config.js";

export const REPORT_JSON_NAME = "choice-legacy-brand-mini-batch-1.json";
export const REPORT_MD_NAME = "choice-legacy-brand-mini-batch-1.md";

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeUrl(u) {
  return nz(u).toLowerCase().replace(/\/+$/, "");
}

function urlToFixtureSlug(developmentUrl) {
  try {
    const u = new URL(developmentUrl);
    const slug = u.pathname.replace(/^\/+/, "").replace(/\//g, "__");
    return slug ? `${slug}.txt` : null;
  } catch {
    return null;
  }
}

function stripFixtureHeader(text) {
  return nz(text)
    .split("\n")
    .filter((line) => !line.startsWith("#"))
    .join("\n")
    .trim();
}

export function probeDevelopmentUrlFromFixture(developmentUrl) {
  const slug = urlToFixtureSlug(developmentUrl);
  if (!slug) {
    return { fixtureFound: false, fixturePath: null, fixtureTextLength: 0, fixtureExtractable: false };
  }
  const fixturePath = path.join(FIXTURE_DEV_TEXT_DIR, slug);
  if (!fs.existsSync(fixturePath)) {
    return { fixtureFound: false, fixturePath: slug, fixtureTextLength: 0, fixtureExtractable: false };
  }
  const raw = fs.readFileSync(fixturePath, "utf8");
  const text = stripFixtureHeader(raw);
  return {
    fixtureFound: true,
    fixturePath: `fixtures/choice-dev-site-text/${slug}`,
    fixtureTextLength: text.length,
    fixtureExtractable: text.length >= 400,
    fixturePreview: text.slice(0, 280),
  };
}

const JS_SHELL_MARKERS = [
  "webruntime-app",
  "lwc://",
  "salesforce",
  "<lightning-out",
  "aura.prod",
];

export async function probeDevelopmentUrlLive(developmentUrl) {
  if (!developmentUrl || !/^https?:\/\//i.test(developmentUrl)) {
    return {
      probed: false,
      httpStatus: null,
      htmlLength: 0,
      extractableTextLength: 0,
      jsShellMarkers: [],
      likelyJsShell: false,
      error: "invalid_url",
    };
  }
  try {
    const res = await fetch(developmentUrl, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Accept: "text/html,*/*",
      },
      redirect: "follow",
    });
    const html = await res.text();
    const lower = html.toLowerCase();
    const markers = JS_SHELL_MARKERS.filter((m) => lower.includes(m.toLowerCase()));
    const stripped = html
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    const likelyJsShell = markers.length > 0 || stripped.length < 200;
    return {
      probed: true,
      httpStatus: res.status,
      htmlLength: html.length,
      extractableTextLength: stripped.length,
      jsShellMarkers: markers,
      likelyJsShell,
      textPreview: stripped.slice(0, 280),
      error: null,
    };
  } catch (err) {
    return {
      probed: false,
      httpStatus: null,
      htmlLength: 0,
      extractableTextLength: 0,
      jsShellMarkers: [],
      likelyJsShell: null,
      error: err.message || String(err),
    };
  }
}

export function classifyDevelopmentPageRecommendation(fixtureProbe, liveProbe, hasLocalPdf) {
  const fixtureOk = fixtureProbe.fixtureExtractable;
  const liveShell = liveProbe?.likelyJsShell === true;
  const liveThin = liveProbe?.probed && liveProbe.extractableTextLength < 200;

  let risk = "low";
  let recommendation = "capture_and_extract_candidate";

  if (liveShell || (liveProbe?.probed && liveThin && !fixtureOk)) {
    risk = "high";
    recommendation = hasLocalPdf
      ? "provenance_only_prefer_local_pdf"
      : "blocked_acquire_pdf_first";
  } else if (liveShell === null && !fixtureOk) {
    risk = "medium";
    recommendation = hasLocalPdf
      ? "provenance_only_prefer_local_pdf"
      : "capture_then_verify_extract_preview";
  } else if (fixtureOk && (liveShell || liveThin)) {
    risk = "medium";
    recommendation = hasLocalPdf
      ? "provenance_only_prefer_local_pdf"
      : "fixture_extractable_live_shell_verify_on_capture";
  } else if (fixtureOk) {
    risk = "low";
    recommendation = hasLocalPdf
      ? "provenance_url_plus_pdf_primary"
      : "capture_development_page_primary";
  }

  return { risk, recommendation };
}

function findDuplicateSource(existingSources, { url, localFilePath, sourceTitle }) {
  const titleKey = nz(sourceTitle).toLowerCase();
  for (const s of existingSources) {
    if (url && normalizeUrl(s.sourceUrl) === normalizeUrl(url)) {
      return { sourceId: s.id, matchType: "source_url", sourceTitle: s.sourceTitle };
    }
    if (localFilePath && nz(s.localFilePath).toLowerCase() === nz(localFilePath).toLowerCase()) {
      return { sourceId: s.id, matchType: "local_file_path", sourceTitle: s.sourceTitle };
    }
    if (titleKey && nz(s.sourceTitle).toLowerCase() === titleKey) {
      return { sourceId: s.id, matchType: "source_title", sourceTitle: s.sourceTitle };
    }
  }
  return null;
}

export function assessLocalPdf(primaryPdf) {
  const issues = [];
  let resolution = null;
  let textPreview = null;
  let textLength = 0;
  let readable = false;

  try {
    resolution = resolveLocalSourceAbsolutePath(primaryPdf.localFilePath);
    const stat = fs.statSync(resolution.absolutePath);
    resolution.sizeBytes = stat.size;
    const doc = readLocalSourceText(primaryPdf.localFilePath);
    const text = nz(doc.text);
    textLength = text.length;
    readable = textLength > 0;
    textPreview = text.slice(0, 400);
  } catch (err) {
    issues.push(err.message || String(err));
  }

  return {
    localFilePath: primaryPdf.localFilePath,
    sourceTitle: primaryPdf.sourceTitle,
    resolution,
    textLength,
    textPreview,
    readable,
    issues,
  };
}

export function buildMiniBatchPdfRegistrationRow(brandConfig, pdfAssessment, existingSources, batchName) {
  const batch = getBatchDefinition(batchName);
  const duplicate = findDuplicateSource(existingSources, {
    localFilePath: pdfAssessment.localFilePath,
    sourceTitle: pdfAssessment.sourceTitle,
  });

  const spec = {
    sourceTitle: pdfAssessment.sourceTitle,
    sourceType: "Development Brochure",
    sourceOrigin: "Brand Provided",
    sourceQuality: "High",
    region: "CALA",
    localFilePath: pdfAssessment.localFilePath,
    note: `${batch.batchNote} — primary development PDF. Text length ${pdfAssessment.textLength}. Not auto-approved.`,
  };

  const validation = buildLocalSourceFields(spec, brandConfig.recordId);

  let registrationStatus = "ready_to_register_local";
  if (duplicate) registrationStatus = "skip_already_registered";
  else if (pdfAssessment.issues.length) registrationStatus = "blocked_file_missing";
  else if (!pdfAssessment.readable) registrationStatus = "blocked_unreadable";
  else if (!validation.ok) registrationStatus = "blocked_validation";

  return {
    role: "p0_development_pdf",
    duplicate,
    alreadyRegistered: Boolean(duplicate),
    registrationStatus,
    registrationReady: registrationStatus === "ready_to_register_local",
    proposedFields: validation.fields,
    validation,
    pdfAssessment,
    spec,
  };
}

export function buildRecommendedP0Package(brandConfig, pdfRow, devProbe) {
  const items = [
    {
      slot: "consumer_brand_page",
      sourceType: "Brand Page",
      url: brandConfig.consumerPage.url,
      register: "capture_after_dry_run_review",
      priority: "P0",
    },
    {
      slot: "development_pdf_local",
      sourceType: "Development Brochure",
      localFilePath: pdfRow.pdfAssessment.localFilePath,
      register: pdfRow.registrationReady ? "apply_local_pdf" : pdfRow.registrationStatus,
      priority: "P0",
    },
    {
      slot: "press_kit",
      sourceType: "Press Release",
      url: brandConfig.pressKit.url,
      register: "capture_after_dry_run_review",
      priority: "P0",
    },
    {
      slot: "development_page",
      sourceType: "Development Page",
      url: brandConfig.developmentPage.url,
      register: devProbe.recommendation.recommendation,
      priority: "P0_provenance",
      note: "URL provenance; PDF is primary extraction evidence when local PDF registered",
    },
  ];
  return items;
}

export function buildMissingMaterials(brandRow, pdfRow) {
  const missing = [];
  if (!pdfRow.registrationReady && !pdfRow.alreadyRegistered) {
    missing.push("primary local development PDF registration");
  }
  missing.push("consumer page URL capture");
  missing.push("press kit URL capture");
  if (brandRow.developmentProbe.recommendation.recommendation.includes("blocked")) {
    missing.push("development page blocked until PDF available");
  }
  return missing;
}

export function buildUrlCaptureCommands(brandConfig) {
  const brandId = brandConfig.recordId;
  const brandName = brandConfig.brandName;
  return {
    consumer: buildCaptureCommand(
      {
        sourceUrl: brandConfig.consumerPage.url,
        sourceType: "Brand Page",
        label: "Choice consumer brand page",
        sourceTitle: `${brandName} — Choice consumer brand page`,
      },
      brandId
    ),
    pressKit: buildCaptureCommand(
      {
        sourceUrl: brandConfig.pressKit.url,
        sourceType: "Press Release",
        label: "Choice press kit / media center",
        sourceTitle: `${brandName} — Choice press kit / media center`,
      },
      brandId
    ),
    development: buildCaptureCommand(
      {
        sourceUrl: brandConfig.developmentPage.url,
        sourceType: "Development Page",
        label: "Choice development brand page (provenance)",
        sourceTitle: `${brandName} — Choice development brand page`,
      },
      brandId
    ),
  };
}

export async function planMiniBatchBrand(
  brandConfig,
  { governanceRow = null, probeLive = true, batchName = DEFAULT_BATCH_NAME } = {}
) {
  const batch = getBatchDefinition(batchName);
  const existingSources = await fetchBrandSources(brandConfig.recordId);
  const packagePlan = planChoiceLegacyBrandPackage(brandConfig, { existingSources, governanceRow });

  const urlOnlyMode = Boolean(batch.urlOnlyMode);
  const primaryPdfSpec = getBatchPrimaryPdf(batchName, brandConfig.key);

  let pdfAssessment;
  let pdfRow;
  if (urlOnlyMode && !primaryPdfSpec) {
    pdfAssessment = {
      localFilePath: null,
      sourceTitle: `${brandConfig.brandName} — no local development PDF (URL-only batch)`,
      textLength: 0,
      readable: false,
      issues: ["no_local_pdf_url_only_batch"],
      resolution: null,
    };
    pdfRow = {
      role: "p0_development_pdf",
      duplicate: null,
      alreadyRegistered: false,
      registrationStatus: "skip_no_local_pdf",
      registrationReady: false,
      proposedFields: null,
      validation: { ok: false, errors: ["url_only_batch_no_local_pdf"] },
      pdfAssessment,
      spec: null,
    };
  } else {
    if (!primaryPdfSpec) {
      throw new Error(`No primary PDF configured for ${batchName}:${brandConfig.key}`);
    }
    pdfAssessment = assessLocalPdf(primaryPdfSpec);
    pdfRow = buildMiniBatchPdfRegistrationRow(
      brandConfig,
      pdfAssessment,
      existingSources,
      batchName
    );
  }

  const fixtureProbe = probeDevelopmentUrlFromFixture(brandConfig.developmentPage.url);
  const liveProbe = probeLive
    ? await probeDevelopmentUrlLive(brandConfig.developmentPage.url)
    : { probed: false, likelyJsShell: null };
  const recommendation = classifyDevelopmentPageRecommendation(
    fixtureProbe,
    liveProbe,
    pdfAssessment.readable
  );

  const developmentProbe = {
    url: brandConfig.developmentPage.url,
    fixture: fixtureProbe,
    live: liveProbe,
    recommendation,
  };

  const urlCommands = buildUrlCaptureCommands(brandConfig);
  const recommendedP0 = buildRecommendedP0Package(brandConfig, pdfRow, developmentProbe);
  const missingMaterials = buildMissingMaterials({ developmentProbe }, pdfRow);

  const pressKitConfidence = brandConfig.pressKit?.confidence || "unknown";
  const pressKitReady = Boolean(brandConfig.pressKit?.url) && pressKitConfidence === "verified";
  const rhgLocalWarnings = (primaryPdfSpec?.excludedLocalCandidates || []).map((p) => ({
    path: p,
    reason: "excluded_rhg_global_primary_candidate",
  }));
  if (pdfAssessment.localFilePath && isRhgContaminatedLocalPath(pdfAssessment.localFilePath)) {
    rhgLocalWarnings.push({
      path: pdfAssessment.localFilePath,
      reason: "primary_pdf_matches_rhg_exclusion_pattern",
    });
  }

  const splitOutRecommended = urlOnlyMode && !primaryPdfSpec
    ? !pressKitReady && !brandConfig.consumerPage?.url
    : !pdfAssessment.readable ||
      rhgLocalWarnings.some((w) => w.reason.includes("primary_pdf")) ||
      (batch.disallowRhgGlobal && !pressKitReady && !brandConfig.pressKit?.url);

  return {
    key: brandConfig.key,
    brandName: brandConfig.brandName,
    recordId: brandConfig.recordId,
    recordIdConfirmed: brandConfig.recordId,
    batchName,
    profileStatus: governanceRow?.profileStatus || "Active — Evidence Package Needed",
    governanceStatus: governanceRow?.governance?.liveValidationStatus || null,
    explorerActive: packagePlan.explorerActive,
    profileCompleteness: packagePlan.profileCompleteness,
    piSourceCount: packagePlan.piSourceCount,
    approvedSourceCount: packagePlan.approvedSourceCount,
    regionalCaveats: brandConfig.regionalCaveats || [],
    rhgGlobalDisallowed: Boolean(batch.disallowRhgGlobal || brandConfig.regionalCaveats?.length),
    rhgLocalWarnings,
    consumerUrlConfidence: brandConfig.consumerPage?.confidence || "unknown",
    pressKitConfidence,
    pressKitReady,
    primaryPdfNote: primaryPdfSpec?.note || (urlOnlyMode ? "URL-only batch — consumer + press primary" : null),
    excludedLocalCandidates: primaryPdfSpec?.excludedLocalCandidates || [],
    localPdf: pdfAssessment,
    proposedSourceTitle: pdfAssessment.sourceTitle,
    proposedSourceLibraryFields: pdfRow.proposedFields,
    duplicateCheck: pdfRow.duplicate,
    pdfRegistration: pdfRow,
    consumerUrl: brandConfig.consumerPage.url,
    pressKitUrl: brandConfig.pressKit.url,
    developmentUrl: brandConfig.developmentPage.url,
    developmentJsShellRisk: recommendation.risk,
    developmentProbe,
    developmentRecommendation: recommendation.recommendation,
    recommendedP0,
    missingMaterials,
    urlCaptureCommands: urlCommands,
    readyForUrlCapture: pressKitReady || Boolean(brandConfig.consumerPage?.url),
    readyForBatchStewardship: false,
    readyForBatchProcessing:
      urlOnlyMode && !primaryPdfSpec
        ? Boolean(brandConfig.consumerPage?.url && pressKitReady)
        : pdfRow.registrationReady || pdfRow.alreadyRegistered,
    splitOutRecommended,
    splitOutReason: splitOutRecommended
      ? !pressKitReady && !brandConfig.pressKit?.url
        ? "press_kit_uncertain_or_missing"
        : !pdfAssessment.readable && !urlOnlyMode
          ? "primary_pdf_unreadable"
          : "rhg_or_caveat_review"
      : null,
    exactNextCommands: [
      pdfRow.registrationReady
        ? `npm run choice-legacy-brand-source-package-batch -- --batch ${batchName} --apply --approve-choice-legacy-batch-source-register --brand ${brandConfig.key}`
        : null,
      urlCommands.consumer,
      pressKitReady ? urlCommands.pressKit : null,
      developmentProbe.recommendation.recommendation.startsWith("provenance")
        ? `${urlCommands.development}  # provenance only; do not rely on extract`
        : urlCommands.development,
    ].filter(Boolean),
    blocked: pdfRow.registrationStatus.startsWith("blocked"),
    readyForPdfRegistration: pdfRow.registrationReady,
    needsUrlCapture: true,
  };
}

export async function buildChoiceLegacyMiniBatchReport({
  governanceReport = null,
  probeLive = true,
  brandFilter = null,
  batchName = DEFAULT_BATCH_NAME,
} = {}) {
  const batch = getBatchDefinition(batchName);
  const governanceById = new Map();
  if (governanceReport?.brands) {
    for (const row of governanceReport.brands) {
      if (row.recordId) governanceById.set(row.recordId, row);
    }
  }

  const brands = getBatchBrandConfigs(batchName, brandFilter);

  const rows = [];
  for (const brandConfig of brands) {
    rows.push(
      await planMiniBatchBrand(brandConfig, {
        governanceRow: governanceById.get(brandConfig.recordId) || null,
        probeLive,
        batchName,
      })
    );
  }

  const summary = {
    totalBrands: rows.length,
    readyForPdfRegistration: rows.filter((r) => r.readyForPdfRegistration).length,
    needingUrlCapture: rows.filter((r) => r.needsUrlCapture).length,
    jsShellDevelopmentRisk: rows.filter((r) =>
      ["medium", "high"].includes(r.developmentJsShellRisk)
    ).length,
    blocked: rows.filter((r) => r.blocked).length,
    alreadyRegisteredPdf: rows.filter((r) => r.pdfRegistration.alreadyRegistered).length,
    splitOutRecommended: rows.filter((r) => r.splitOutRecommended).length,
    pressKitUncertain: rows.filter((r) => !r.pressKitReady).length,
  };

  const recommendedOrder = [
    "1. Dry-run review this report",
    "2. Apply local PDF registration per brand (explicit approval flag)",
    "3. Capture consumer + press kit URLs with --apply --register (one brand at a time)",
    "4. Optional: capture development URL as provenance only (not primary extract)",
    "5. npm run steward-partner-intelligence -- --entity-type brand --target-rec-id … --dry-run",
    "6. Do NOT extract or publish governance until sources stewarded",
  ];

  return {
    batchVersion: BATCH_VERSION,
    batchName: batch.batchName,
    batchDisplayName: batch.displayName,
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    companyFolder: COMPANY_FOLDER,
    brands: rows,
    summary,
    recommendedOrder,
    recommendedNextCommands: rows.flatMap((r) => r.exactNextCommands).slice(0, 16),
    batchApplyCommands: {
      sourcePackageApply: `npm run choice-legacy-brand-source-package-batch -- --batch ${batchName} --apply --approve-choice-legacy-batch-source-register`,
      urlCaptureApply: `npm run choice-legacy-batch-url-capture -- --batch ${batchName} --apply --approve-choice-legacy-batch-url-capture`,
    },
    doesNotDo: [
      "Rebuild Brand Explorer content",
      "Overwrite Brand Setup fields",
      "Extract facts",
      "Approve sources or facts",
      "Publish governance",
      "Set Company Validated",
      "Register uncertain URLs without capture",
      "Auto-approve registered sources",
    ],
  };
}

export async function applyMiniBatchLocalPdfRegistrations(report, { brandFilter = null } = {}) {
  const applied = [];
  const skipped = [];
  const errors = [];

  for (const brandRow of report.brands) {
    if (brandFilter && brandRow.key !== brandFilter && brandRow.recordId !== brandFilter) continue;
    const pdf = brandRow.pdfRegistration;
    if (pdf.registrationStatus === "skip_already_registered") {
      skipped.push({
        brand: brandRow.brandName,
        reason: "already_registered",
        sourceId: pdf.duplicate?.sourceId,
      });
      continue;
    }
    if (pdf.registrationStatus !== "ready_to_register_local") {
      skipped.push({ brand: brandRow.brandName, reason: pdf.registrationStatus });
      continue;
    }
    try {
      const created = await createPartnerSource(pdf.proposedFields);
      applied.push({
        brand: brandRow.brandName,
        recordId: brandRow.recordId,
        sourceId: created.id,
        localFilePath: pdf.pdfAssessment.localFilePath,
      });
    } catch (err) {
      errors.push({ brand: brandRow.brandName, message: err.message || String(err) });
    }
  }

  return { applied, skipped, errors };
}

export function buildChoiceLegacyMiniBatchMarkdown(report) {
  const s = report.summary;
  const title = report.batchDisplayName || report.batchName || "Choice Legacy Mini-Batch";
  const lines = [
    `# ${title} — Source Package`,
    "",
    `Generated: ${report.generatedAt}`,
    `Batch: **${report.batchName}**`,
    `Mode: **${report.mode}**`,
    `Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Executive summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Brands in batch | ${s.totalBrands} |`,
    `| Ready for PDF registration | ${s.readyForPdfRegistration} |`,
    `| Already registered (PDF duplicate) | ${s.alreadyRegisteredPdf} |`,
    `| Needing URL capture | ${s.needingUrlCapture} |`,
    `| Press kit uncertain/missing | ${s.pressKitUncertain ?? 0} |`,
    `| JS-shell / dev-page risk (medium+) | ${s.jsShellDevelopmentRisk} |`,
    `| Split-out recommended | ${s.splitOutRecommended ?? 0} |`,
    `| Blocked | ${s.blocked} |`,
    "",
    "## Recommended order of operations",
    "",
  ];

  for (const step of report.recommendedOrder) lines.push(`- ${step}`);
  lines.push("", "## Brands", "");

  for (const row of report.brands) {
    lines.push(`### ${row.brandName}`, "");
    lines.push(
      `- Record ID: \`${row.recordId}\` (confirmed)`,
      `- Profile status: **${row.profileStatus}**`,
      `- Governance: **${row.governanceStatus || "—"}**`,
      `- Explorer active: **${row.explorerActive ? "yes" : "no"}**`,
      `- Profile completeness: **${row.profileCompleteness}**`,
      `- PI sources: **${row.approvedSourceCount}/${row.piSourceCount}** approved`,
      `- Local PDF: \`${row.localPdf.localFilePath}\``,
      `- PDF readable: **${row.localPdf.readable ? "yes" : "no"}** · text length **${row.localPdf.textLength}**`,
      `- Proposed title: **${row.proposedSourceTitle}**`,
      `- PDF registration: **${row.pdfRegistration.registrationStatus}**`,
      `- Duplicate: ${
        row.duplicateCheck
          ? "`" + row.duplicateCheck.sourceId + "` (" + row.duplicateCheck.matchType + ")"
          : "none"
      }`,
      `- Consumer URL: ${row.consumerUrl} (${row.consumerUrlConfidence})`,
      `- Press kit URL: ${row.pressKitUrl || "—"} (${row.pressKitConfidence})`,
      `- Development URL: ${row.developmentUrl}`,
      `- Dev JS-shell risk: **${row.developmentJsShellRisk}** · recommendation: **${row.developmentRecommendation}**`,
      `- Ready for PDF registration: **${row.readyForPdfRegistration ? "yes" : "no"}**`,
      `- Ready for URL capture: **${row.readyForUrlCapture ? "yes" : "partial"}**`,
      `- Split out: **${row.splitOutRecommended ? "yes" : "no"}**${row.splitOutReason ? ` (${row.splitOutReason})` : ""}`
    );
    if (row.regionalCaveats?.length) {
      lines.push("- Regional caveats:");
      for (const c of row.regionalCaveats) lines.push(`  - ${c}`);
    }
    if (row.rhgLocalWarnings?.length) {
      lines.push("- RHG/global exclusions:");
      for (const w of row.rhgLocalWarnings) lines.push(`  - \`${w.path}\` — ${w.reason}`);
    }
    if (row.developmentProbe.fixture.fixtureFound) {
      lines.push(
        `- Fixture extractable text: **${row.developmentProbe.fixture.fixtureTextLength}** chars (\`${row.developmentProbe.fixture.fixturePath}\`)`
      );
    }
    if (row.developmentProbe.live.probed) {
      lines.push(
        `- Live probe: HTTP ${row.developmentProbe.live.httpStatus}; stripped text **${row.developmentProbe.live.extractableTextLength}**; likely JS shell: **${row.developmentProbe.live.likelyJsShell ? "yes" : "no"}**`
      );
    }
    lines.push("", "#### Recommended P0 package", "");
    for (const item of row.recommendedP0) {
      lines.push(`- **${item.slot}** (${item.sourceType}): ${item.url || item.localFilePath} → ${item.register}`);
    }
    if (row.missingMaterials.length) {
      lines.push("", `- Missing: ${row.missingMaterials.join("; ")}`);
    }
    lines.push("", "#### Exact next commands", "", "```bash");
    for (const cmd of row.exactNextCommands) lines.push(cmd);
    lines.push("```", "");
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) lines.push(`- ${item}`);
  lines.push("");

  return lines.join("\n");
}
