/**
 * Section Pattern Parity — live audit + report writers.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import {
  SECTION_PATTERN_PARITY_VERSION,
  SECTION_PATTERN_AUDIT_DEFAULT_BRANDS,
  resolveSectionPatternBrandIdentity,
  evaluateSectionPatternParity,
} from "./brand-explorer-section-pattern-parity.js";

export { SECTION_PATTERN_PARITY_VERSION, SECTION_PATTERN_AUDIT_DEFAULT_BRANDS };

export const REPORT_JSON = "brand-explorer-section-pattern-parity-audit.json";
export const REPORT_MD = "brand-explorer-section-pattern-parity-audit.md";
export const REPORT_MOMENTUM_MD = "brand-explorer-section-pattern-parity-recent-momentum.md";
export const REPORT_GEO_MD = "brand-explorer-section-pattern-parity-geographic-footprint.md";
export const DOC_MD = "brand-explorer-section-pattern-parity.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const SLUG_ALIASES = Object.freeze({
  comfort: "comfort-inn-suites",
  curio: "curio-collection",
  mgallery: "mgallery-collection",
  indigo: "hotel-indigo",
  slh: "small-luxury-hotels-of-the-world",
  tribute: "tribute-portfolio",
  "radisson-individuals": "radisson-individuals-by-choice",
  country: "country-inn-suites",
  quality: "quality-inn",
  suburban: "suburban-studios",
  woodspring: "woodspring-suites",
  blu: "radisson-blu",
  red: "radisson-red",
});

export function resolveSectionPatternBrandList(rawList) {
  if (!rawList?.length) return [...SECTION_PATTERN_AUDIT_DEFAULT_BRANDS];
  const out = [];
  for (const raw of rawList) {
    const key = String(raw).trim().toLowerCase();
    const mapped = SLUG_ALIASES[key] || key;
    // Lane 2 full-build drafts may be audited during founder minor cleanup.
    // Default cohort still excludes them; explicit --brands is required.
    out.push(mapped);
  }
  return [...new Set(out)];
}

async function fetchBrandApi(slug) {
  const identity = resolveSectionPatternBrandIdentity(slug);
  const lookupId = identity.recordId || slug;
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
  await getBrandLibraryBrandById({ query: { brandId: lookupId }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${slug} (${lookupId})`);
  return res.payload.brand;
}

function tableRow(brandSlug, sectionResult) {
  return {
    brand: brandSlug,
    section: sectionResult.section,
    currentPattern: sectionResult.currentPattern,
    expectedPattern: sectionResult.expectedPattern,
    status: sectionResult.status,
    failureReason: sectionResult.failureReason || "",
    proposedPatch: sectionResult.proposedPatch || "",
    benchmarkReference: sectionResult.benchmarkReference || sectionResult.expectedPattern,
  };
}

export async function auditSectionPatternParityBrand(brandSlug) {
  const brand = await fetchBrandApi(brandSlug);
  const rows = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const html = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: true,
  });
  const evaluation = evaluateSectionPatternParity({
    brandSlug,
    brandName: brand.name,
    presentationRows: rows,
    html,
  });
  return {
    brandSlug,
    brandName: brand.name || brandSlug,
    recordId: brand.id || brand.recordId || null,
    pass: evaluation.pass === true,
    gates: evaluation.gates,
    sections: evaluation.sections,
    findings: evaluation.findings,
    tableRows: Object.values(evaluation.sections).map((s) => tableRow(brandSlug, s)),
  };
}

export async function runSectionPatternParityAudit({ brands = null, dryRun = true } = {}) {
  const brandList = resolveSectionPatternBrandList(brands);
  const results = [];
  for (const slug of brandList) {
    console.log(`Auditing section pattern parity: ${slug}`);
    const result = await auditSectionPatternParityBrand(slug);
    results.push(result);
    console.log(
      `  ${result.pass ? "PASS" : "FAIL"} · momentum=${result.sections.recent_momentum.status} · geo=${result.sections.geographic_footprint.status}`
    );
  }

  const failing = results.filter((r) => !r.pass);
  const report = {
    version: SECTION_PATTERN_PARITY_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: dryRun !== false,
    purpose:
      "Section pattern parity audit for Recent Momentum, Geographic Footprint, Portfolio Context, Growth Priorities",
    brands: brandList,
    brandResults: results,
    summary: {
      brandsAudited: results.length,
      pass: results.filter((r) => r.pass).length,
      fail: failing.length,
      recentMomentumFail: results.filter((r) => !r.sections.recent_momentum.pass).length,
      geographicFootprintFail: results.filter((r) => !r.sections.geographic_footprint.pass).length,
      failingSlugs: failing.map((r) => r.brandSlug),
    },
    guardrails: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      releaseFieldsUntouched: true,
      publicRestoreUntouched: true,
      trueIncompleteUntouched: true,
    },
  };
  return report;
}

function mdEscape(s) {
  return nz(s).replace(/\|/g, "\\|").replace(/\n/g, " ");
}

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function renderSectionTable(rows) {
  const lines = [
    "| Brand | Section | Current Pattern | Expected Pattern | Status | Failure Reason | Proposed Patch | Benchmark Reference |",
    "| --- | --- | --- | --- | --- | --- | --- | --- |",
  ];
  for (const r of rows) {
    lines.push(
      `| ${mdEscape(r.brand)} | ${mdEscape(r.section)} | ${mdEscape(r.currentPattern)} | ${mdEscape(r.expectedPattern)} | **${mdEscape(r.status)}** | ${mdEscape(r.failureReason)} | ${mdEscape(r.proposedPatch)} | ${mdEscape(r.benchmarkReference)} |`
    );
  }
  return lines.join("\n");
}

export function writeSectionPatternParityReports(report) {
  const allRows = report.brandResults.flatMap((b) => b.tableRows);
  const momentumRows = allRows.filter((r) => r.section === "recent_momentum");
  const geoRows = allRows.filter((r) => r.section === "geographic_footprint");

  const jsonPath = path.join(ROOT, "reports", REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# Brand Explorer Section Pattern Parity Audit`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    `Dry-run: ${report.dryRun}`,
    ``,
    `## Summary`,
    ``,
    `- Brands audited: ${report.summary.brandsAudited}`,
    `- Pass: ${report.summary.pass}`,
    `- Fail: ${report.summary.fail}`,
    `- Recent Momentum fails: ${report.summary.recentMomentumFail}`,
    `- Geographic Footprint fails: ${report.summary.geographicFootprintFail}`,
    `- Failing slugs: ${report.summary.failingSlugs.join(", ") || "(none)"}`,
    ``,
    `A section does **not** pass merely because text is non-empty. It must match benchmark product pattern.`,
    ``,
    `## All sections`,
    ``,
    renderSectionTable(allRows),
    ``,
    `## Guardrails`,
    ``,
    `- No Company Validated / Source Library / Registry / release / public-restore writes in this audit`,
    `- True-incomplete brands excluded`,
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(ROOT, "reports", REPORT_MD), md);

  fs.writeFileSync(
    path.join(ROOT, "reports", REPORT_MOMENTUM_MD),
    [
      `# Section Pattern Parity — Recent Momentum`,
      ``,
      `Generated: ${report.generatedAt}`,
      ``,
      renderSectionTable(momentumRows),
      ``,
    ].join("\n")
  );

  fs.writeFileSync(
    path.join(ROOT, "reports", REPORT_GEO_MD),
    [
      `# Section Pattern Parity — Geographic Footprint`,
      ``,
      `Generated: ${report.generatedAt}`,
      ``,
      renderSectionTable(geoRows),
      ``,
    ].join("\n")
  );

  const doc = [
    `# Brand Explorer Section Pattern Parity`,
    ``,
    `Permanent Tab Factory gate: **section_pattern_parity**.`,
    ``,
    `A section does not pass simply because it has non-empty text. It must match the established Brand Explorer product pattern used by benchmark profiles (Tribute, Kimpton, Radisson Individuals, Design Hotels).`,
    ``,
    `## Mandatory gates`,
    ``,
    `- \`recent_momentum_pattern_pass\``,
    `- \`geographic_footprint_pattern_pass\``,
    `- \`portfolio_context_pattern_pass\``,
    `- \`growth_priorities_pattern_pass\``,
    ``,
    `A brand cannot become \`founder_review_ready\` or \`active_profile_ready\` unless rendered completeness, no-empty, provenance, image uniqueness, image role-match, **section pattern parity**, and golden content all pass.`,
    ``,
    `## Commands`,
    ``,
    "```bash",
    `npm run brand-explorer-section-pattern-parity-audit -- --dry-run`,
    `npm run brand-explorer-section-pattern-parity-remediation -- --brands <failing-slugs> --dry-run`,
    `npm run brand-explorer-section-pattern-parity-remediation -- --brands <failing-slugs> --apply \\`,
    `  --approve-section-pattern-parity-remediation \\`,
    `  --confirm-no-company-validation-changes \\`,
    `  --confirm-no-source-library-status-changes \\`,
    `  --confirm-no-registry-approval-changes \\`,
    `  --confirm-no-release-field-changes \\`,
    `  --confirm-no-public-restore-fields \\`,
    `  --confirm-section-pattern-only \\`,
    `  --confirm-benchmark-pattern-aligned`,
    `npm run test:brand-explorer-section-pattern-parity -- --brands <slugs>`,
    "```",
    ``,
    `Latest audit: ${report.generatedAt} · pass=${report.summary.pass} fail=${report.summary.fail}`,
    ``,
    `Failing: ${report.summary.failingSlugs.join(", ") || "(none)"}`,
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(ROOT, "docs", "data-intelligence", DOC_MD), doc);

  return {
    jsonPath,
    mdPath: path.join(ROOT, "reports", REPORT_MD),
    momentumPath: path.join(ROOT, "reports", REPORT_MOMENTUM_MD),
    geoPath: path.join(ROOT, "reports", REPORT_GEO_MD),
    docPath: path.join(ROOT, "docs", "data-intelligence", DOC_MD),
  };
}
