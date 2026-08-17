/**
 * Rendered field-by-field completeness audit for Brand Explorer.
 * Loads live Brand Library payload + atelier HTML — not row-existence alone.
 * Sync evaluation lives in brand-explorer-rendered-field-completeness-evaluate.js
 * (safe for OS/factory imports without circular deps).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import {
  BENCHMARK_BRANDS,
  PROTECTED_BRANDS,
  TARGET_BRANDS,
} from "./brand-explorer-rendered-field-completeness-inventory.js";
import {
  evaluateRenderedFieldCompletenessFromPayload,
} from "./brand-explorer-rendered-field-completeness-evaluate.js";

export {
  evaluateRenderedFieldCompletenessFromPayload,
  evaluateRenderedFieldCompletenessForTest,
} from "./brand-explorer-rendered-field-completeness-evaluate.js";

export const AUDIT_VERSION = "rendered-field-completeness-v1";
export const REPORT_JSON = "brand-explorer-rendered-field-completeness-audit.json";
export const REPORT_MD = "brand-explorer-rendered-field-completeness-audit.md";

const BRAND_REPORT_MD = Object.freeze({
  "hotel-indigo": "brand-explorer-rendered-field-completeness-hotel-indigo.md",
  "mgallery-collection": "brand-explorer-rendered-field-completeness-mgallery.md",
  "small-luxury-hotels-of-the-world": "brand-explorer-rendered-field-completeness-slh.md",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

async function fetchBrandApi(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } = await import(
    "./brand-explorer-factory-preview-candidates.js"
  );
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  const brandId = identity?.recordId || slug;
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`Brand API failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function buildBenchmarkDelta(targetFindings, benchmarkSummaries) {
  const targetFail = targetFindings.filter((f) => f.status !== "pass");
  const weaker = targetFail.map((f) => `${f.sectionName} · ${f.componentLabel} (${f.status})`);
  return {
    benchmarksCompared: benchmarkSummaries.map((b) => b.brandSlug),
    missingOrWeakerVsBenchmark: weaker.slice(0, 40),
    benchmarkPassRates: benchmarkSummaries.map((b) => ({
      brandSlug: b.brandSlug,
      pass: b.summary?.totalPass,
      fail: b.summary?.totalFail,
    })),
  };
}

export async function auditBrandRenderedFieldCompleteness(brandSlug, { includeHtmlScans = true } = {}) {
  if (PROTECTED_BRANDS.includes(brandSlug) && !BENCHMARK_BRANDS.includes(brandSlug)) {
    throw new Error(`Protected brand ${brandSlug}`);
  }
  const brand = await fetchBrandApi(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug);
  const rows = ctx.presentationRows || [];
  const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: false });
  return evaluateRenderedFieldCompletenessFromPayload(brand, rows, html, brandSlug, {
    includeHtmlScans,
  });
}

export async function runRenderedFieldCompletenessAudit({
  brands = TARGET_BRANDS,
  includeBenchmarks = true,
} = {}) {
  for (const b of brands) {
    if (!TARGET_BRANDS.includes(b)) {
      throw new Error(`Audit targets only: ${TARGET_BRANDS.join(", ")}`);
    }
  }

  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await auditBrandRenderedFieldCompleteness(brandSlug));
  }

  let benchmarkSummaries = [];
  if (includeBenchmarks) {
    for (const slug of ["kimpton", "design-hotels", "radisson-individuals-by-choice"]) {
      try {
        const b = await auditBrandRenderedFieldCompleteness(slug);
        benchmarkSummaries.push({
          brandSlug: slug,
          summary: b.summary,
          releaseQualityDecision: b.releaseQualityDecision,
        });
      } catch (err) {
        benchmarkSummaries.push({ brandSlug: slug, error: err.message });
      }
    }
  }

  for (const b of brandResults) {
    b.benchmarkDelta = buildBenchmarkDelta(b.findings, benchmarkSummaries);
  }

  const unresolved = brandResults.filter((b) => b.patchPlanComplete !== true);

  return {
    version: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    brandResults,
    benchmarkSummaries,
    summary: {
      brandsAudited: brandResults.length,
      totalFail: brandResults.reduce((n, b) => n + b.summary.totalFail, 0),
      totalPass: brandResults.reduce((n, b) => n + b.summary.totalPass, 0),
      decisions: Object.fromEntries(brandResults.map((b) => [b.brandSlug, b.releaseQualityDecision])),
      unresolvedWithoutPatchPlan: unresolved.map((b) => b.brandSlug),
      auditComplete: brandResults.every((b) => b.auditComplete === true),
      patchPlanComplete: unresolved.length === 0,
      auditPass: brandResults.every((b) => b.auditPass === true),
    },
    auditComplete: brandResults.every((b) => b.auditComplete === true),
    patchPlanComplete: unresolved.length === 0,
    auditPass: brandResults.every((b) => b.auditPass === true),
  };
}

function brandTableMd(b) {
  const lines = [
    `# Rendered Field Completeness — ${b.brandName}`,
    "",
    `Slug: \`${b.brandSlug}\``,
    `Live state: **${b.liveState.displayState}**`,
    `Release-quality decision: **${b.releaseQualityDecision}**`,
    `auditComplete: **${b.auditComplete}** · patchPlanComplete: **${b.patchPlanComplete}** · auditPass: **${b.auditPass}** (failFindings=${b.failFindings})`,
    "",
    "## Counts",
    "",
    `- Visible fields audited: **${b.summary.totalVisibleFieldsAudited}**`,
    `- Pass: **${b.summary.totalPass}**`,
    `- Fail: **${b.summary.totalFail}**`,
    `- Suppression needed: **${b.summary.totalSuppressionNeeded}**`,
    `- Rewrite needed: **${b.summary.totalRewriteNeeded}**`,
    `- Metric handling defects: **${b.summary.totalMetricHandlingDefects}**`,
    `- Image distinctiveness defects: **${b.summary.totalImageDistinctivenessDefects}**`,
    `- Missing modal/body defects: **${b.summary.totalMissingModalBodyDefects}**`,
    "",
    "## Field table",
    "",
    "| Brand | Tab | Section | Component | Field | Current Rendered Value | Status | Required Fix | Record ID | Proposed Patch |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
  ];
  for (const f of b.findings) {
    const val = String(f.currentRenderedValue || "")
      .replace(/\|/g, "/")
      .replace(/\n/g, " ")
      .slice(0, 120);
    const patch = f.proposedPatch
      ? `${f.proposedPatch.action} ${f.proposedPatch.slotKey || ""}`.trim()
      : "—";
    lines.push(
      `| ${b.brandSlug} | ${f.tabName} | ${f.sectionName} | ${f.componentLabel} | ${f.fieldName} | ${val} | ${f.status} | ${f.recommendedAction} | ${f.sourceRecordId || "—"} | ${patch} |`
    );
  }
  lines.push("", "## Benchmark delta", "");
  for (const w of b.benchmarkDelta?.missingOrWeakerVsBenchmark || []) {
    lines.push(`- ${w}`);
  }
  lines.push("", "## Patch plan", "");
  for (const p of b.patchPlan || []) {
    lines.push(`- **${p.action}** \`${p.slotKey}\` → ${String(p.fields?.Body || "").slice(0, 100)}…`);
  }
  lines.push("");
  return lines.join("\n");
}

export function writeRenderedFieldCompletenessReports(report) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# Brand Explorer Rendered Field Completeness Audit`,
    "",
    `Generated: ${report.generatedAt}`,
    `auditComplete: **${report.auditComplete}**`,
    `patchPlanComplete: **${report.patchPlanComplete}** (every fail has a proposed fix/handling)`,
    `auditPass: **${report.auditPass}** (requires failFindings = 0 after remediation)`,
    "",
    "## Summary",
    "",
    `- Brands audited: **${report.summary.brandsAudited}**`,
    `- Total pass findings: **${report.summary.totalPass}**`,
    `- Total fail findings: **${report.summary.totalFail}**`,
    `- Unresolved without patch plan: ${report.summary.unresolvedWithoutPatchPlan.join(", ") || "—"}`,
    "",
  ];
  for (const b of report.brandResults) {
    md.push(`### ${b.brandSlug}`);
    md.push(`- Decision: **${b.releaseQualityDecision}**`);
    md.push(`- auditPass: **${b.auditPass}** · patchPlanComplete: **${b.patchPlanComplete}**`);
    md.push(`- Pass/Fail: ${b.summary.totalPass}/${b.summary.totalFail}`);
    md.push(`- Patch plan items: ${b.patchPlan.length}`);
    md.push("");
  }
  fs.writeFileSync(mdPath, md.join("\n"));

  const brandPaths = {};
  for (const b of report.brandResults) {
    const name = BRAND_REPORT_MD[b.brandSlug];
    if (!name) continue;
    const p = path.join(reportsDir, name);
    fs.writeFileSync(p, brandTableMd(b));
    brandPaths[b.brandSlug] = p;
  }
  return { jsonPath, mdPath, brandPaths };
}
