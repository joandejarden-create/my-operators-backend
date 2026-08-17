/**
 * Choice legacy mini-batch 1 — batch URL capture (consumer + press only).
 * Dry-run default; download + register on explicit --apply.
 * @see docs/data-intelligence/choice-legacy-batch-url-capture-v1.md
 */
import fs from "fs";
import path from "path";
import { MAP_PARTNER_SOURCE } from "../../api/lib/partner-intelligence-field-map.js";
import { createPartnerSource } from "./airtable-source.js";
import {
  DEFAULT_BATCH_NAME,
  getBatchBrandConfigs,
  getBatchDefinition,
} from "./choice-legacy-batch-config.js";
import { CHOICE_LEGACY_BRANDS, COMPANY_FOLDER, fetchBrandSources } from "./choice-legacy-brand-source-package.js";
import {
  appendCaptureLog,
  buildReferenceMaterialPaths,
  ensureReferenceDirectory,
  resolveReferenceRoot,
  writeCaptureReadme,
} from "./reference-material-paths.js";

export const CAPTURE_VERSION = "1";
export const REPORT_JSON_NAME = "choice-legacy-batch-url-capture.json";
export const REPORT_MD_NAME = "choice-legacy-batch-url-capture.md";

export const BROWSER_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

const DOWNLOAD_USER_AGENTS = [
  "DealalityReferenceCapture/1.0 (+https://dealality.com)",
  BROWSER_USER_AGENT,
];

/** v1 — consumer + press only; no development URLs. */
export const URL_SLOT_SPECS = {
  consumer_page: {
    slot: "consumer_page",
    typeKey: "website-capture",
    sourceType: "Brand Page",
    titleSuffix: "Choice consumer brand page",
    label: "Choice consumer brand page",
  },
  press_kit: {
    slot: "press_kit",
    typeKey: "media-kit",
    sourceType: "Press Release",
    titleSuffix: "Choice press kit / media center",
    label: "Choice press kit / media center",
  },
};

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeUrl(u) {
  return nz(u).toLowerCase().replace(/\/+$/, "");
}

export function getBatchUrlCaptureBrands(batchName = DEFAULT_BATCH_NAME, brandFilter = null) {
  return getBatchBrandConfigs(batchName, brandFilter);
}

export function buildUrlTargetsForBrand(brandConfig) {
  const targets = [];
  const consumerUrl = brandConfig.consumerPage?.url;
  const pressUrl = brandConfig.pressKit?.url;

  if (consumerUrl) {
    const spec = URL_SLOT_SPECS.consumer_page;
    targets.push({
      ...spec,
      sourceUrl: consumerUrl,
      sourceTitle: `${brandConfig.brandName} — ${spec.titleSuffix}`,
    });
  }
  if (pressUrl) {
    const spec = URL_SLOT_SPECS.press_kit;
    targets.push({
      ...spec,
      sourceUrl: pressUrl,
      sourceTitle: `${brandConfig.brandName} — ${spec.titleSuffix}`,
    });
  }
  return targets;
}

export function findDuplicateSource(existingSources, { url, sourceTitle }) {
  const titleKey = nz(sourceTitle).toLowerCase();
  for (const s of existingSources) {
    if (url && normalizeUrl(s.sourceUrl) === normalizeUrl(url)) {
      return {
        isDuplicate: true,
        sourceId: s.id,
        matchType: "source_url",
        existingTitle: s.sourceTitle,
        existingStatus: s.status,
      };
    }
    if (titleKey && nz(s.sourceTitle).toLowerCase() === titleKey) {
      return {
        isDuplicate: true,
        sourceId: s.id,
        matchType: "source_title",
        existingTitle: s.sourceTitle,
        existingStatus: s.status,
      };
    }
  }
  return { isDuplicate: false, sourceId: null, matchType: null, existingTitle: null, existingStatus: null };
}

function stripHtmlText(html) {
  return nz(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function estimateReadableTextLength(buf, contentType, ext) {
  const ct = nz(contentType).toLowerCase();
  const extension = nz(ext).toLowerCase();
  if (ct.includes("html") || extension === ".html" || extension === ".htm") {
    return stripHtmlText(buf.toString("utf8")).length;
  }
  if (ct.includes("pdf") || extension === ".pdf") {
    const text = buf.toString("latin1");
    const matches = text.match(/\(([^()]{4,120})\)/g) || [];
    const joined = matches.join(" ");
    return joined.length > 80 ? joined.length : Math.floor(buf.length / 4);
  }
  return nz(buf.toString("utf8")).replace(/\s+/g, " ").trim().length;
}

/**
 * Download with Dealality UA then browser UA fallback (Choice capture fix).
 * @returns {{ buf: Buffer, contentType: string, ext: string, finalUrl: string, httpStatus: number, userAgentUsed: string }}
 */
export async function downloadUrlWithFallback(targetUrl) {
  let lastErr;
  for (const userAgent of DOWNLOAD_USER_AGENTS) {
    try {
      const res = await fetch(targetUrl, {
        headers: {
          "User-Agent": userAgent,
          Accept: "application/pdf,text/html,*/*",
        },
        redirect: "follow",
      });
      const finalUrl = res.url || targetUrl;
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${targetUrl}`);
      }
      const buf = Buffer.from(await res.arrayBuffer());
      const ct = (res.headers.get("content-type") || "").toLowerCase();
      let ext = ".html";
      if (ct.includes("pdf")) ext = ".pdf";
      else if (ct.includes("json")) ext = ".json";
      else if (!ct.includes("html") && buf.slice(0, 4).toString() === "%PDF") ext = ".pdf";
      else {
        try {
          const fromUrl = path.extname(new URL(finalUrl).pathname);
          if (fromUrl) ext = fromUrl;
        } catch {
          /* keep default */
        }
      }
      return {
        buf,
        contentType: ct,
        ext,
        finalUrl,
        httpStatus: res.status,
        userAgentUsed: userAgent,
      };
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr;
}

export function buildUrlRegistrationFields(target, brandRecordId, paths) {
  return {
    [MAP_PARTNER_SOURCE.sourceTitle]: target.sourceTitle,
    [MAP_PARTNER_SOURCE.profileType]: "Brand",
    [MAP_PARTNER_SOURCE.sourceUrl]: target.sourceUrl,
    [MAP_PARTNER_SOURCE.localFilePath]: paths.relativePath,
    [MAP_PARTNER_SOURCE.sourceType]: target.sourceType,
    [MAP_PARTNER_SOURCE.sourceOrigin]: paths.typeMeta.origin,
    [MAP_PARTNER_SOURCE.sourceQuality]: "Medium",
    [MAP_PARTNER_SOURCE.status]: "Captured",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.verifiedSource]: "No",
    [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
    [MAP_PARTNER_SOURCE.captureDate]: new Date().toISOString().slice(0, 10),
    [MAP_PARTNER_SOURCE.notes]: `Captured via choice-legacy-batch-url-capture v${CAPTURE_VERSION} (${target.slot}); not auto-approved.`,
    [MAP_PARTNER_SOURCE.brand]: [brandRecordId],
  };
}

export function buildUrlCapturePaths(target, brandName, ext) {
  const paths = buildReferenceMaterialPaths({
    companyFolder: COMPANY_FOLDER,
    brandName: undefined,
    typeKey: target.typeKey,
    title: target.sourceTitle,
    ext,
  });
  return paths;
}

export async function probeUrlTarget(target, brandConfig, existingSources, { fetchContent = true } = {}) {
  const duplicate = findDuplicateSource(existingSources, {
    url: target.sourceUrl,
    sourceTitle: target.sourceTitle,
  });

  const row = {
    brand: brandConfig.brandName,
    brandKey: brandConfig.key,
    brandRecordId: brandConfig.recordId,
    slot: target.slot,
    sourceUrl: target.sourceUrl,
    sourceTitle: target.sourceTitle,
    type: target.sourceType,
    typeKey: target.typeKey,
    mode: "dry_run",
    status: duplicate.isDuplicate ? "skip_duplicate" : "ready_to_capture",
    finalUrl: null,
    httpStatus: null,
    contentType: null,
    bytes: null,
    readableTextLength: null,
    duplicateCheck: duplicate,
    sourceLibraryRowId: duplicate.sourceId || null,
    warnings: [],
    error: null,
    localFilePath: null,
    proposedRegistrationFields: null,
  };

  if (duplicate.isDuplicate) {
    row.warnings.push(`duplicate_${duplicate.matchType}`);
    return row;
  }

  if (!fetchContent) return row;

  try {
    const download = await downloadUrlWithFallback(target.sourceUrl);
    row.finalUrl = download.finalUrl;
    row.httpStatus = download.httpStatus;
    row.contentType = download.contentType;
    row.bytes = download.buf.length;
    row.readableTextLength = estimateReadableTextLength(
      download.buf,
      download.contentType,
      download.ext
    );

    if (row.readableTextLength < 100) {
      row.warnings.push("thin_readable_text");
    }
    if (download.userAgentUsed === BROWSER_USER_AGENT) {
      row.warnings.push("browser_user_agent_fallback_used");
    }

    const paths = buildUrlCapturePaths(target, brandConfig.brandName, download.ext);
    row.localFilePath = paths.relativePath;
    row.proposedRegistrationFields = buildUrlRegistrationFields(
      target,
      brandConfig.recordId,
      paths
    );
  } catch (err) {
    row.status = "failed";
    row.error = err.message || String(err);
    row.warnings.push("fetch_failed");
  }

  return row;
}

export async function applyUrlCaptureRow(row, target, brandConfig) {
  if (row.status === "skip_duplicate") {
    return {
      ...row,
      mode: "apply",
      applyStatus: "skipped_duplicate",
      sourceLibraryRowId: row.duplicateCheck.sourceId,
    };
  }

  try {
    const download = await downloadUrlWithFallback(target.sourceUrl);
    let paths = buildUrlCapturePaths(target, brandConfig.brandName, download.ext);
    if (download.ext !== ".pdf" && !paths.fileName.endsWith(download.ext)) {
      paths = buildReferenceMaterialPaths({
        companyFolder: COMPANY_FOLDER,
        typeKey: target.typeKey,
        title: target.sourceTitle,
        ext: download.ext,
      });
    }

    ensureReferenceDirectory(paths.absoluteDir);
    const companyDir = path.join(resolveReferenceRoot(), paths.companyFolder);
    writeCaptureReadme(COMPANY_FOLDER, companyDir);
    fs.writeFileSync(paths.absoluteFile, download.buf);

    appendCaptureLog(COMPANY_FOLDER, {
      url: target.sourceUrl,
      relativePath: paths.relativePath,
      typeKey: target.typeKey,
      brand: brandConfig.brandName,
      title: target.sourceTitle,
      batch: "choice-legacy-batch-url-capture",
    });

    const fields = buildUrlRegistrationFields(target, brandConfig.recordId, paths);
    const created = await createPartnerSource(fields);

    return {
      ...row,
      mode: "apply",
      applyStatus: "captured",
      status: "captured",
      finalUrl: download.finalUrl,
      httpStatus: download.httpStatus,
      contentType: download.contentType,
      bytes: download.buf.length,
      readableTextLength: estimateReadableTextLength(
        download.buf,
        download.contentType,
        download.ext
      ),
      localFilePath: paths.relativePath,
      sourceLibraryRowId: created.id,
      proposedRegistrationFields: fields,
    };
  } catch (err) {
    return {
      ...row,
      mode: "apply",
      applyStatus: "failed",
      status: "failed",
      error: err.message || String(err),
      warnings: [...(row.warnings || []), "apply_failed"],
    };
  }
}

export async function buildChoiceLegacyBatchUrlCaptureReport({
  brandFilter = null,
  probeUrls = true,
  batchName = DEFAULT_BATCH_NAME,
} = {}) {
  const batch = getBatchDefinition(batchName);
  const brands = getBatchUrlCaptureBrands(batchName, brandFilter);
  const urls = [];

  for (const brandConfig of brands) {
    const existingSources = await fetchBrandSources(brandConfig.recordId);
    const targets = buildUrlTargetsForBrand(brandConfig);
    for (const target of targets) {
      urls.push(await probeUrlTarget(target, brandConfig, existingSources, { fetchContent: probeUrls }));
    }
  }

  const ready = urls.filter((u) => u.status === "ready_to_capture");
  const duplicates = urls.filter((u) => u.status === "skip_duplicate");
  const failed = urls.filter((u) => u.status === "failed");
  const warnings = [...new Set(urls.flatMap((u) => u.warnings || []))];

  const batchApplyCommand = `npm run choice-legacy-batch-url-capture -- --batch ${batchName} --apply --approve-choice-legacy-batch-url-capture`;

  const nextRecommendedCommand = `npm run choice-legacy-batch-source-stewardship -- --batch ${batchName} --dry-run`;

  return {
    captureVersion: CAPTURE_VERSION,
    batchName,
    batchDisplayName: batch.displayName,
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    companyFolder: COMPANY_FOLDER,
    brandsIncluded: brands.map((b) => ({
      key: b.key,
      brandName: b.brandName,
      recordId: b.recordId,
      existingPdfSourceNote: "local PDF already registered; URL capture is consumer + press only",
    })),
    urls,
    summary: {
      totalUrlsPlanned: urls.length,
      readyToCapture: ready.length,
      captured: 0,
      skippedDuplicates: duplicates.length,
      failed: failed.length,
      warnings,
    },
    batchApplyCommand,
    nextRecommendedCommand,
    perBrandFallbackCommands: brands.map(
      (b) =>
        `npm run choice-legacy-batch-url-capture -- --batch ${batchName} --apply --approve-choice-legacy-batch-url-capture --brand ${b.key}`
    ),
    doesNotDo: [
      "Rebuild Brand Explorer content or overwrite Brand Setup fields",
      "Capture development URLs (JS-shell provenance only — excluded from v1)",
      "Extract or approve facts",
      "Publish governance or set Company Validated / Company Validation Date",
      "Auto-approve URL sources (Explorer Use and Extraction remain No)",
      "Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema",
    ],
  };
}

export async function applyChoiceLegacyBatchUrlCapture(
  report,
  { brandFilter = null, batchName = DEFAULT_BATCH_NAME } = {}
) {
  const captured = [];
  const skippedDuplicates = [];
  const failed = [];

  const brandMap = new Map(
    getBatchUrlCaptureBrands(batchName, brandFilter).map((b) => [b.key, b])
  );

  for (const row of report.urls) {
    if (brandFilter && row.brandKey !== brandFilter && row.brandRecordId !== brandFilter) {
      continue;
    }

    const brandConfig = brandMap.get(row.brandKey);
    if (!brandConfig) continue;

    const targets = buildUrlTargetsForBrand(brandConfig);
    const target = targets.find((t) => t.slot === row.slot && t.sourceUrl === row.sourceUrl);
    if (!target) {
      failed.push({ ...row, error: "target_spec_not_found" });
      continue;
    }

    if (row.status === "skip_duplicate") {
      skippedDuplicates.push(row);
      continue;
    }

    const existingSources = await fetchBrandSources(brandConfig.recordId);
    const freshDuplicate = findDuplicateSource(existingSources, {
      url: target.sourceUrl,
      sourceTitle: target.sourceTitle,
    });
    if (freshDuplicate.isDuplicate) {
      skippedDuplicates.push({
        ...row,
        status: "skip_duplicate",
        duplicateCheck: freshDuplicate,
        sourceLibraryRowId: freshDuplicate.sourceId,
      });
      continue;
    }

    const result = await applyUrlCaptureRow(row, target, brandConfig);
    if (result.applyStatus === "captured") captured.push(result);
    else if (result.applyStatus === "skipped_duplicate") skippedDuplicates.push(result);
    else failed.push(result);

    await new Promise((r) => setTimeout(r, 280));
  }

  return { captured, skippedDuplicates, failed };
}

export function buildChoiceLegacyBatchUrlCaptureMarkdown(report) {
  const s = report.summary;
  const lines = [
    "# Choice Legacy Mini-Batch URL Capture v1",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Executive summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Total URLs planned | ${s.totalUrlsPlanned} |`,
    `| Ready to capture | ${s.readyToCapture} |`,
    `| Captured | ${s.captured} |`,
    `| Skipped duplicates | ${s.skippedDuplicates} |`,
    `| Failed | ${s.failed} |`,
    "",
    "### Warnings (aggregate)",
    "",
    ...(s.warnings.length ? s.warnings.map((w) => `- ${w}`) : ["- none"]),
    "",
    "### Batch apply command",
    "",
    "```bash",
    report.batchApplyCommand,
    "```",
    "",
    "### Next recommended command",
    "",
    "```bash",
    report.nextRecommendedCommand,
    "```",
    "",
    "## URLs",
    "",
    "| Brand | Slot | URL | Status | HTTP | Bytes | Text len | Duplicate | Source ID |",
    "|-------|------|-----|--------|------|------:|---------:|-----------|-----------|",
  ];

  for (const row of report.urls) {
    const dup = row.duplicateCheck?.isDuplicate
      ? `yes (${row.duplicateCheck.matchType})`
      : "no";
    lines.push(
      `| ${row.brand} | ${row.slot} | ${row.sourceUrl} | ${row.status} | ${row.httpStatus ?? "—"} | ${row.bytes ?? "—"} | ${row.readableTextLength ?? "—"} | ${dup} | ${row.sourceLibraryRowId ? "`" + row.sourceLibraryRowId + "`" : "—"} |`
    );
    if (row.warnings?.length) {
      lines.push(`| | warnings: ${row.warnings.join("; ")} | | | | | | | |`);
    }
    if (row.error) {
      lines.push(`| | error: ${row.error} | | | | | | | |`);
    }
  }

  lines.push("", "## Per-URL detail", "");

  for (const row of report.urls) {
    lines.push(`### ${row.brand} — ${row.slot}`, "");
    lines.push(
      `- Brand record: \`${row.brandRecordId}\``,
      `- Source URL: ${row.sourceUrl}`,
      `- Source title: **${row.sourceTitle}**`,
      `- Type: **${row.type}**`,
      `- Status: **${row.status}**`,
      `- Final URL: ${row.finalUrl || "—"}`,
      `- HTTP status: ${row.httpStatus ?? "—"}`,
      `- Content type: ${row.contentType || "—"}`,
      `- Bytes: ${row.bytes ?? "—"}`,
      `- Readable text length: ${row.readableTextLength ?? "—"}`,
      `- Duplicate: ${row.duplicateCheck?.isDuplicate ? `yes (\`${row.duplicateCheck.sourceId}\` · ${row.duplicateCheck.matchType})` : "no"}`,
      `- Source Library row: ${row.sourceLibraryRowId ? `\`${row.sourceLibraryRowId}\`` : "—"}`
    );
    if (row.localFilePath) lines.push(`- Local file path: \`${row.localFilePath}\``);
    if (row.warnings?.length) lines.push(`- Warnings: ${row.warnings.join("; ")}`);
    if (row.error) lines.push(`- Error: ${row.error}`);
    lines.push("");
  }

  if (report.applyResult) {
    const ar = report.applyResult;
    lines.push(
      "## Apply result",
      "",
      `- Captured: **${ar.captured?.length ?? 0}**`,
      `- Skipped duplicates: **${ar.skippedDuplicates?.length ?? 0}**`,
      `- Failed: **${ar.failed?.length ?? 0}**`,
      ""
    );
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) lines.push(`- ${item}`);
  lines.push("");

  return lines.join("\n");
}
