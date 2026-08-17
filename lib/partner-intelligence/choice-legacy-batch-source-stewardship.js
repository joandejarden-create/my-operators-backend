/**
 * Choice legacy mini-batch 1 — batch source stewardship (dry-run default).
 * Comfort, Everhome, Quality — shared Choice-controlled source pattern.
 * @see docs/data-intelligence/choice-legacy-batch-source-stewardship-v1.md
 */
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { patchPartnerSource } from "./airtable-source.js";
import {
  DEFAULT_BATCH_NAME,
  getBatchDefinition,
  getBatchBrandConfigs,
  getBatchPrimaryPdf,
} from "./choice-legacy-batch-config.js";
import { CHOICE_LEGACY_BRANDS, fetchBrandSources } from "./choice-legacy-brand-source-package.js";
import { readLocalSourceText } from "./extract-source-text.js";
import { resolveLocalSourceAbsolutePath } from "./reference-material-paths.js";
import {
  buildSafeSourcePatch,
  isLinkedToTarget,
  sourceSnapshot,
} from "./stewardship-package.js";

export const STEWARDSHIP_VERSION = "1.1";
export const REPORT_JSON_NAME = "choice-legacy-batch-source-stewardship.json";
export const REPORT_MD_NAME = "choice-legacy-batch-source-stewardship.md";

/** Minimum stripped text length to approve extraction on HTML captures. */
export const MIN_READABLE_TEXT_FOR_EXTRACTION = 200;

/** stewardship-package.js does not write this field; batch module adds it when eligible. */
export const EXTRACTION_FIELD_SUPPORTED = true;
export const EXTRACTION_FIELD_NOTE =
  "stewardship-package.js buildSafeSourcePatch does not set Approved for Extraction?; this batch workflow adds it for eligible local PDFs and official readable Choice consumer/press captures.";

const CHOICE_URL_PATTERNS = [
  /^https?:\/\/(www\.)?choicehotels\.com\//i,
  /^https?:\/\/media\.choicehotels\.com\//i,
  /^https?:\/\/(www\.)?choicehotelsdevelopment\.com\//i,
];

const BLOCKED_URL_PATTERNS = [
  /radissonhotels\.(net|com)/i,
  /radissonhotelgroup/i,
  /\brhg\b/i,
  /showpad\.com/i,
];

const BLOCKED_ORIGINS = new Set(["Other"]);

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeUrl(u) {
  return nz(u).toLowerCase().replace(/\/+$/, "");
}

function urlMatchesExpected(sourceUrl, expectedUrl) {
  if (!expectedUrl) return false;
  return normalizeUrl(sourceUrl) === normalizeUrl(expectedUrl);
}

function isOfficialChoiceConsumerUrl(url) {
  if (!url) return false;
  return /^https?:\/\/(www\.)?choicehotels\.com\//i.test(url) && !/choicehotelsdevelopment/i.test(url);
}

function isOfficialChoicePressUrl(url) {
  if (!url) return false;
  return /^https?:\/\/media\.choicehotels\.com\//i.test(url);
}

export function getStewardshipBatchBrandConfigs(batchName = DEFAULT_BATCH_NAME, brandFilter = null) {
  return getBatchBrandConfigs(batchName, brandFilter);
}

export function classifyChoiceLegacySource(source, brandKey, batchName = DEFAULT_BATCH_NAME) {
  const primary = getBatchPrimaryPdf(batchName, brandKey);
  const localPath = nz(source.localFilePath).toLowerCase();
  const url = nz(source.sourceUrl).toLowerCase();
  const type = nz(source.sourceType);

  if (primary && localPath && localPath === primary.localFilePath.toLowerCase()) {
    return "mini_batch_primary_pdf";
  }
  if (localPath && primary && localPath.endsWith(pathBasename(primary.localFilePath).toLowerCase())) {
    return "mini_batch_primary_pdf";
  }
  if (type === "Press Release" || /media\.choicehotels\.com/i.test(url)) {
    return "press_kit";
  }
  if (
    type === "Brand Page" ||
    type === "Website Capture" ||
    /^https?:\/\/(www\.)?choicehotels\.com\//i.test(url)
  ) {
    return "consumer_page";
  }
  if (type === "Development Page" || /choicehotelsdevelopment\.com/i.test(url)) {
    return "development_provenance";
  }
  if (type === "Development Brochure" && localPath) {
    return "local_development_pdf";
  }
  return "other";
}

function pathBasename(p) {
  const parts = nz(p).replace(/\\/g, "/").split("/");
  return parts[parts.length - 1] || "";
}

function isChoiceControlledUrl(url) {
  if (!url) return false;
  return CHOICE_URL_PATTERNS.some((re) => re.test(url));
}

function isBlockedUrl(url) {
  if (!url) return false;
  return BLOCKED_URL_PATTERNS.some((re) => re.test(url));
}

function assessLocalFileReadable(localFilePath) {
  if (!localFilePath) return { readable: false, textLength: 0, error: "no_local_path" };
  try {
    resolveLocalSourceAbsolutePath(localFilePath);
    const doc = readLocalSourceText(localFilePath);
    const textLength = nz(doc.text).length;
    return { readable: textLength > 0, textLength, error: null };
  } catch (err) {
    return { readable: false, textLength: 0, error: err.message || String(err) };
  }
}

export function isSourceFullyApproved(source) {
  return (
    nz(source.status) === "Approved" &&
    nz(source.approvedForExplorerUse) === "Yes" &&
    nz(source.approvedForExtraction) === "Yes"
  );
}

export function isSourcePartiallyApproved(source) {
  return (
    nz(source.approvedForExplorerUse) === "Yes" ||
    nz(source.status) === "Approved" ||
    nz(source.approvedForExtraction) === "Yes"
  );
}

/**
 * @returns {{ eligible: boolean, recommendation: string, blockers: string[], approveExtraction: boolean, role: string }}
 */
export function assessChoiceLegacySourceEligibility(source, brandConfig) {
  const blockers = [];
  const role = classifyChoiceLegacySource(source, brandConfig.key, brandConfig.batchName);

  if (!isLinkedToTarget("brand", brandConfig.recordId, source)) {
    blockers.push("not_linked_to_target_brand");
  }
  if (nz(source.status) === "Stale") blockers.push("source_stale");
  if (nz(source.status) === "Rejected") blockers.push("source_rejected");
  if (BLOCKED_ORIGINS.has(nz(source.sourceOrigin))) blockers.push("blocked_source_origin");

  const url = nz(source.sourceUrl);
  if (url && isBlockedUrl(url)) blockers.push("blocked_rhg_or_third_party_url");
  if (url && !isChoiceControlledUrl(url) && !nz(source.localFilePath)) {
    blockers.push("url_not_choice_controlled");
  }

  if (isSourceFullyApproved(source)) {
    return {
      eligible: false,
      recommendation: "no_op_already_approved",
      blockers: ["already_fully_approved"],
      approveExtraction: false,
      role,
    };
  }

  if (role === "development_provenance") {
    blockers.push("development_js_shell_provenance_only");
    return {
      eligible: false,
      recommendation: "skip_provenance_only_not_for_batch_approval",
      blockers,
      approveExtraction: false,
      role,
    };
  }

  if (role === "mini_batch_primary_pdf" || role === "local_development_pdf") {
    const primary = getBatchPrimaryPdf(brandConfig.batchName || DEFAULT_BATCH_NAME, brandConfig.key);
    const expectedPath = primary?.localFilePath?.toLowerCase();
    const actualPath = nz(source.localFilePath).toLowerCase();
    if (expectedPath && actualPath !== expectedPath && !actualPath.endsWith(pathBasename(expectedPath).toLowerCase())) {
      blockers.push("local_pdf_path_mismatch");
    }
    const fileCheck = assessLocalFileReadable(source.localFilePath);
    if (!fileCheck.readable) blockers.push(`local_pdf_unreadable:${fileCheck.error || "empty"}`);
    const quality = nz(source.sourceQuality);
    if (quality === "Low") blockers.push("source_quality_low");

    if (blockers.length) {
      return {
        eligible: false,
        recommendation: "blocked",
        blockers,
        approveExtraction: false,
        role,
      };
    }
    return {
      eligible: true,
      recommendation: "approve_explorer_use_status_and_extraction",
      blockers: [],
      approveExtraction: true,
      role,
    };
  }

  if (role === "consumer_page" || role === "press_kit") {
    if (!url) blockers.push("missing_verified_url");
    else if (!isChoiceControlledUrl(url)) blockers.push("url_not_choice_verified");

    if (role === "consumer_page") {
      const expected = brandConfig.consumerPage?.url;
      if (expected && !urlMatchesExpected(url, expected)) {
        blockers.push("consumer_url_mismatch");
      }
      if (url && !isOfficialChoiceConsumerUrl(url)) {
        blockers.push("not_official_choice_consumer_domain");
      }
    }

    if (role === "press_kit") {
      const expected = brandConfig.pressKit?.url;
      if (expected && !urlMatchesExpected(url, expected)) {
        blockers.push("press_url_mismatch");
      }
      if (url && !isOfficialChoicePressUrl(url)) {
        blockers.push("press_not_company_media_domain");
      }
    }

    const fileCheck = source.localFilePath ? assessLocalFileReadable(source.localFilePath) : null;
    const extractionReadable =
      Boolean(fileCheck?.readable) && fileCheck.textLength >= MIN_READABLE_TEXT_FOR_EXTRACTION;

    if (blockers.length) {
      return {
        eligible: false,
        recommendation: "blocked",
        blockers,
        approveExtraction: false,
        role,
      };
    }

    if (extractionReadable) {
      return {
        eligible: true,
        recommendation: "approve_explorer_use_status_and_extraction",
        blockers: [],
        approveExtraction: true,
        role,
      };
    }

    return {
      eligible: true,
      recommendation: "approve_explorer_use_and_status_no_extraction",
      blockers: [],
      approveExtraction: false,
      role,
    };
  }

  blockers.push("unclassified_or_unsupported_source");
  return {
    eligible: false,
    recommendation: "manual_review",
    blockers,
    approveExtraction: false,
    role,
  };
}

export function buildChoiceLegacyBatchSourcePatch(source, brandConfig, eligibility) {
  const base = buildSafeSourcePatch(source, "brand", brandConfig.recordId, {
    approvedSourceIds: new Set([source.id]),
    allowWrites: true,
    allowQualityBump: false,
    allowStatusAdvance: true,
  });

  const patch = base.patch ? { ...base.patch } : {};
  const applied = [...(base.applied || [])];
  const skipped = [...(base.skipped || [])];
  const extractionNotes = [];

  if (!eligibility.eligible) {
    return { patch: null, applied, skipped, extractionNotes };
  }

  if (
    eligibility.approveExtraction &&
    EXTRACTION_FIELD_SUPPORTED &&
    nz(source.approvedForExtraction) !== "Yes"
  ) {
    if (!VAL_PARTNER_SOURCE_SELECTS.approvedForExtraction.includes("Yes")) {
      skipped.push("unknown_select_option:approvedForExtraction");
    } else {
      patch[MAP_PARTNER_SOURCE.approvedForExtraction] = "Yes";
      applied.push("Approved for Extraction? → Yes");
      extractionNotes.push(EXTRACTION_FIELD_NOTE);
    }
  }

  if (!Object.keys(patch).length) {
    return { patch: null, applied, skipped: skipped.length ? skipped : ["no_changes_needed"], extractionNotes };
  }

  return { patch, applied, skipped, extractionNotes };
}

export function assessBrandSourceRow(source, brandConfig, batchName = DEFAULT_BATCH_NAME) {
  const enriched = { ...brandConfig, batchName: brandConfig.batchName || batchName };
  const eligibility = assessChoiceLegacySourceEligibility(source, enriched);
  const patchPlan = buildChoiceLegacyBatchSourcePatch(source, enriched, eligibility);
  const fileCheck = source.localFilePath ? assessLocalFileReadable(source.localFilePath) : null;

  return {
    ...sourceSnapshot(source),
    sourceId: source.id,
    localFilePath: source.localFilePath || null,
    sourceUrl: source.sourceUrl || null,
    approvedForExtraction: source.approvedForExtraction ?? null,
    role: eligibility.role,
    recommendation: eligibility.recommendation,
    blockers: eligibility.blockers,
    eligibleForBatchApproval: eligibility.eligible,
    approveExtractionRecommended: eligibility.approveExtraction,
    localFileReadable: fileCheck?.readable ?? null,
    localFileTextLength: fileCheck?.textLength ?? null,
    applyPlan: {
      wouldApply: Boolean(patchPlan.patch),
      previewPatch: patchPlan.patch,
      appliedLabels: patchPlan.applied,
      skipped: patchPlan.skipped,
      extractionNotes: patchPlan.extractionNotes,
    },
  };
}

export async function buildChoiceLegacyBatchStewardshipReport({
  brandFilter = null,
  batchName = DEFAULT_BATCH_NAME,
} = {}) {
  const batch = getBatchDefinition(batchName);
  const brandConfigs = getStewardshipBatchBrandConfigs(batchName, brandFilter);
  const brands = [];

  for (const brandConfig of brandConfigs) {
    const sources = await fetchBrandSources(brandConfig.recordId);
    const sourceRows = sources.map((s) => assessBrandSourceRow(s, brandConfig, batchName));
    const eligible = sourceRows.filter((r) => r.eligibleForBatchApproval);

    brands.push({
      key: brandConfig.key,
      brandName: brandConfig.brandName,
      recordId: brandConfig.recordId,
      batchName,
      expectedPrimaryPdf: getBatchPrimaryPdf(batchName, brandConfig.key)?.localFilePath || null,
      consumerUrl: brandConfig.consumerPage?.url || null,
      pressKitUrl: brandConfig.pressKit?.url || null,
      developmentUrl: brandConfig.developmentPage?.url || null,
      developmentNote: "provenance_only_js_shell_risk",
      sourcesFound: sources.length,
      sourceRows,
      eligibleSourceIds: eligible.map((r) => r.sourceId),
      eligibleCount: eligible.length,
      skippedCount: sourceRows.filter((r) => !r.eligibleForBatchApproval).length,
    });
  }

  const allEligible = brands.flatMap((b) =>
    b.sourceRows.filter((r) => r.eligibleForBatchApproval).map((r) => ({
      brandKey: b.key,
      brandName: b.brandName,
      recordId: b.recordId,
      sourceId: r.sourceId,
      sourceTitle: r.sourceTitle,
      role: r.role,
    }))
  );

  const allSkipped = brands.flatMap((b) =>
    b.sourceRows
      .filter((r) => !r.eligibleForBatchApproval)
      .map((r) => ({
        brandKey: b.key,
        brandName: b.brandName,
        sourceId: r.sourceId,
        sourceTitle: r.sourceTitle,
        recommendation: r.recommendation,
        blockers: r.blockers,
      }))
  );

  const batchApplyCommand = `npm run choice-legacy-batch-source-stewardship -- --batch ${batchName} --apply --approve-choice-legacy-batch-stewardship`;

  const perBrandFallbackCommands = brands.map(
    (b) =>
      `npm run choice-legacy-batch-source-stewardship -- --batch ${batchName} --apply --approve-choice-legacy-batch-stewardship --brand ${b.key}`
  );

  const nextAfterApproval = [
    {
      brand: batchName,
      recordId: null,
      command: batchApplyCommand,
      note:
        "Approve official Choice consumer/press sources (Extraction Yes when readable). Do not extract facts until founder approves per brand.",
    },
  ];

  return {
    stewardshipVersion: STEWARDSHIP_VERSION,
    batchName,
    batchDisplayName: batch.displayName,
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    extractionFieldNote: EXTRACTION_FIELD_NOTE,
    summary: {
      totalBrands: brands.length,
      totalSourcesFound: brands.reduce((n, b) => n + b.sourcesFound, 0),
      sourcesEligibleForApproval: allEligible.length,
      sourcesSkipped: allSkipped.length,
      brandsWithEligibleSources: brands.filter((b) => b.eligibleCount > 0).length,
      blockers: [...new Set(allSkipped.flatMap((s) => s.blockers))],
    },
    batchApplyCommand,
    perBrandFallbackCommands,
    recommendedNextAfterApproval: nextAfterApproval,
    eligibleSources: allEligible,
    skippedSources: allSkipped,
    brands,
    doesNotDo: [
      "Rebuild Explorer content or overwrite Brand Setup fields",
      "Approve or extract facts",
      "Publish governance or set Company Validated",
      "Approve development-page sources (JS-shell provenance only)",
      "Approve RHG/global or uncertain third-party sources",
    ],
  };
}

export async function applyChoiceLegacyBatchStewardship(report, { brandFilter = null } = {}) {
  const applied = [];
  const skipped = [];
  const errors = [];
  const batchName = report.batchName || DEFAULT_BATCH_NAME;

  const eligible = report.eligibleSources || [];
  const toApply = brandFilter
    ? eligible.filter((e) => e.brandKey === brandFilter || e.recordId === brandFilter)
    : eligible;

  if (!toApply.length) {
    return {
      applied,
      skipped,
      errors: [{ message: "no_eligible_sources_for_apply" }],
      rejected: true,
    };
  }

  for (const item of toApply) {
    const brand = report.brands.find((b) => b.key === item.brandKey);
    const row = brand?.sourceRows.find((r) => r.sourceId === item.sourceId);
    if (!row?.applyPlan?.previewPatch) {
      skipped.push({ sourceId: item.sourceId, brand: item.brandName, reason: "no_patch_preview" });
      continue;
    }

    const brandConfig = getBatchBrandConfigs(batchName, item.brandKey)[0];
    const sources = await fetchBrandSources(brandConfig.recordId);
    const source = sources.find((s) => s.id === item.sourceId);
    if (!source) {
      skipped.push({ sourceId: item.sourceId, brand: item.brandName, reason: "source_not_found" });
      continue;
    }

    const enriched = { ...brandConfig, batchName };
    const eligibility = assessChoiceLegacySourceEligibility(source, enriched);
    const patchResult = buildChoiceLegacyBatchSourcePatch(source, enriched, eligibility);
    if (!patchResult.patch) {
      skipped.push({
        sourceId: item.sourceId,
        brand: item.brandName,
        reason: patchResult.skipped.join("; ") || "patch_blocked",
      });
      continue;
    }

    try {
      await patchPartnerSource(item.sourceId, patchResult.patch);
      applied.push({
        brand: item.brandName,
        recordId: item.recordId,
        sourceId: item.sourceId,
        sourceTitle: item.sourceTitle,
        patch: patchResult.patch,
        appliedLabels: patchResult.applied,
      });
    } catch (err) {
      errors.push({
        sourceId: item.sourceId,
        brand: item.brandName,
        message: err.message || String(err),
      });
    }
    await new Promise((r) => setTimeout(r, 220));
  }

  return { applied, skipped, errors, rejected: false };
}

export function buildChoiceLegacyBatchStewardshipMarkdown(report) {
  const s = report.summary;
  const lines = [
    "# Choice Legacy Batch Source Stewardship v1",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "> " + report.extractionFieldNote,
    "",
    "## Executive summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Brands | ${s.totalBrands} |`,
    `| Sources found | ${s.totalSourcesFound} |`,
    `| Eligible for batch approval | ${s.sourcesEligibleForApproval} |`,
    `| Skipped | ${s.sourcesSkipped} |`,
    "",
    "### Blockers (aggregate)",
    "",
    ...(s.blockers.length ? s.blockers.map((b) => `- ${b}`) : ["- none"]),
    "",
    "### Batch apply command",
    "",
    "```bash",
    report.batchApplyCommand,
    "```",
    "",
    "### Per-brand fallback",
    "",
  ];

  for (const cmd of report.perBrandFallbackCommands) {
    lines.push("```bash", cmd, "```", "");
  }

  lines.push("### Next after batch approval", "");
  for (const n of report.recommendedNextAfterApproval) {
    lines.push(`- **${n.brand}**: ${n.note}`);
    lines.push("  ```bash", n.command, "  ```");
  }
  lines.push("", "## Brands", "");

  for (const brand of report.brands) {
    lines.push(`### ${brand.brandName}`, "");
    lines.push(
      `- Record: \`${brand.recordId}\``,
      `- Sources found: **${brand.sourcesFound}**`,
      `- Eligible: **${brand.eligibleCount}** · Skipped: **${brand.skippedCount}**`,
      `- Expected primary PDF: \`${brand.expectedPrimaryPdf}\``
    );
    lines.push("", "| Source ID | Title | Type | Status | Explorer Use | Extraction | Role | Eligible | Recommendation |",
      "|-----------|-------|------|--------|--------------|------------|------|----------|----------------|");
    for (const row of brand.sourceRows) {
      lines.push(
        `| \`${row.sourceId}\` | ${row.sourceTitle} | ${row.sourceType} | ${row.status} | ${row.approvedForExplorerUse} | ${row.approvedForExtraction ?? "—"} | ${row.role} | ${row.eligibleForBatchApproval ? "yes" : "no"} | ${row.recommendation} |`
      );
      if (row.blockers.length) {
        lines.push(`| | blockers: ${row.blockers.join("; ")} | | | | | | | |`);
      }
    }
    lines.push("");
  }

  if (report.applyResult) {
    lines.push("## Apply result", "");
    lines.push(`- Applied: **${report.applyResult.applied?.length ?? 0}**`);
    lines.push(`- Skipped: **${report.applyResult.skipped?.length ?? 0}**`);
    lines.push(`- Errors: **${report.applyResult.errors?.length ?? 0}**`, "");
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) lines.push(`- ${item}`);
  lines.push("");

  return lines.join("\n");
}
