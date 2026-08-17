/**
 * Brand Explorer Tab Factory audit — tab-by-tab, field-by-field on live rendered payload.
 * auditPass = failFindings === 0 (patch plan is not a pass).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import {
  TAB_FACTORY_BENCHMARK_BRANDS,
  TAB_FACTORY_PROTECTED_BRANDS,
  TAB_FACTORY_TARGET_BRANDS,
  TAB_FACTORY_VERSION,
  assertTabContractsCoverInventory,
} from "./brand-explorer-tab-contracts.js";
import {
  evaluateTabFactoryFromPayload,
  evaluateTabFactoryForTest,
} from "./brand-explorer-tab-factory-evaluate.js";
import { WAVE12_SLUGS } from "./brand-explorer-wave12-factory-plan.js";
import { WAVE13_STAGE4_APPROVED_SLUGS } from "./brand-explorer-wave13-factory-plan.js";
import { WAVE14_STAGE4_APPROVED_SLUGS } from "./brand-explorer-wave14-factory-plan.js";
import { WAVE15_STAGE4_APPROVED_SLUGS } from "./brand-explorer-wave15-factory-plan.js";
import { WAVE16A_STAGE2A_APPROVED_SLUGS } from "./brand-explorer-wave16a-factory-plan.js";

export {
  evaluateTabFactoryFromPayload,
  evaluateTabFactoryForTest,
} from "./brand-explorer-tab-factory-evaluate.js";

export const AUDIT_VERSION = TAB_FACTORY_VERSION;
export const REPORT_JSON = "brand-explorer-tab-factory-audit.json";
export const REPORT_MD = "brand-explorer-tab-factory-audit.md";

const WAVE12_AUDIT_ALLOWLIST = new Set(WAVE12_SLUGS);
const WAVE13_AUDIT_ALLOWLIST = new Set(WAVE13_STAGE4_APPROVED_SLUGS);
const WAVE14_AUDIT_ALLOWLIST = new Set(WAVE14_STAGE4_APPROVED_SLUGS);
const WAVE15_AUDIT_ALLOWLIST = new Set(WAVE15_STAGE4_APPROVED_SLUGS);
const WAVE16A_STAGE2A_AUDIT_ALLOWLIST = new Set(WAVE16A_STAGE2A_APPROVED_SLUGS);

const BRAND_MD = Object.freeze({
  "hotel-indigo": "brand-explorer-tab-factory-hotel-indigo.md",
  "mgallery-collection": "brand-explorer-tab-factory-mgallery.md",
  "small-luxury-hotels-of-the-world": "brand-explorer-tab-factory-slh.md",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

async function resolveAuditBrandId(slug) {
  const { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } = await import(
    "./brand-explorer-factory-preview-candidates.js"
  );
  if (FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug]?.recordId) {
    return FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug].recordId;
  }
  if (WAVE16A_STAGE2A_AUDIT_ALLOWLIST.has(slug)) {
    const { WAVE16A_IDENTITIES } = await import("./brand-explorer-wave16a-factory-plan.js");
    if (WAVE16A_IDENTITIES[slug]?.recordId) return WAVE16A_IDENTITIES[slug].recordId;
  }
  return slug;
}

async function fetchBrandApi(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const brandId = await resolveAuditBrandId(slug);
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
  await getBrandLibraryBrandById(
    { query: { brandId, beInternalPreview: "1", factoryPreview: "1" }, headers: {} },
    res
  );
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`Brand API failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

export async function auditBrandTabFactory(brandSlug) {
  if (TAB_FACTORY_PROTECTED_BRANDS.includes(brandSlug) && !TAB_FACTORY_BENCHMARK_BRANDS.includes(brandSlug)) {
    throw new Error(`Protected brand ${brandSlug}`);
  }
  const brandId = await resolveAuditBrandId(brandSlug);
  const brand = await fetchBrandApi(brandSlug);
  const ctx = await loadBrandFactoryContext(brandId);
  const rows = ctx.presentationRows || [];
  const html = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: WAVE16A_STAGE2A_AUDIT_ALLOWLIST.has(brandSlug),
  });
  return evaluateTabFactoryFromPayload({
    brand,
    rows,
    html,
    brandSlug,
    brandConfig: ctx.brandConfig || ctx.activeProfileConfig,
    registryAssets: ctx.registryAssets || [],
  });
}

export async function runTabFactoryAudit({
  brands = TAB_FACTORY_TARGET_BRANDS,
  includeBenchmarks = false,
} = {}) {
  const cover = assertTabContractsCoverInventory();
  if (!cover.ok) {
    throw new Error(`Tab contracts missing inventory fields: ${cover.missing.join(", ")}`);
  }

  for (const b of brands) {
    if (
      !TAB_FACTORY_TARGET_BRANDS.includes(b) &&
      !TAB_FACTORY_BENCHMARK_BRANDS.includes(b) &&
      !WAVE12_AUDIT_ALLOWLIST.has(b) &&
      !WAVE13_AUDIT_ALLOWLIST.has(b) &&
      !WAVE14_AUDIT_ALLOWLIST.has(b) &&
      !WAVE15_AUDIT_ALLOWLIST.has(b) &&
      !WAVE16A_STAGE2A_AUDIT_ALLOWLIST.has(b)
    ) {
      throw new Error(`Unsupported brand for tab factory audit: ${b}`);
    }
  }

  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await auditBrandTabFactory(brandSlug));
  }

  let benchmarkSummaries = [];
  if (includeBenchmarks) {
    for (const slug of TAB_FACTORY_BENCHMARK_BRANDS.slice(0, 3)) {
      try {
        const b = await auditBrandTabFactory(slug);
        benchmarkSummaries.push({
          brandSlug: slug,
          auditPass: b.auditPass,
          failFindings: b.failFindings,
          gates: b.gates,
        });
      } catch (err) {
        benchmarkSummaries.push({ brandSlug: slug, error: err.message });
      }
    }
  }

  return {
    version: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    brandResults,
    benchmarkSummaries,
    summary: {
      brandsAudited: brandResults.length,
      totalFailFindings: brandResults.reduce((n, b) => n + b.failFindings, 0),
      totalEmptyRenderFails: brandResults.reduce((n, b) => n + b.emptyRenderFailFindings, 0),
      auditComplete: true,
      patchPlanComplete: brandResults.every((b) => b.patchPlanComplete === true),
      auditPass: brandResults.every((b) => b.auditPass === true),
    },
    auditComplete: true,
    patchPlanComplete: brandResults.every((b) => b.patchPlanComplete === true),
    auditPass: brandResults.every((b) => b.auditPass === true),
  };
}

function brandMd(b) {
  const lines = [
    `# Tab Factory — ${b.brandName}`,
    "",
    `Slug: \`${b.brandSlug}\``,
    `Live: **${b.liveState?.displayState}**`,
    `auditComplete: **${b.auditComplete}** · patchPlanComplete: **${b.patchPlanComplete}** · auditPass: **${b.auditPass}**`,
    `failFindings: **${b.failFindings}** · emptyRenderFails: **${b.emptyRenderFailFindings}**`,
    "",
    "## Gates",
    "",
  ];
  for (const [k, v] of Object.entries(b.gates || {})) {
    lines.push(`- ${k}: **${v}**`);
  }
  lines.push("", "## Provenance", "");
  lines.push(`Pass: **${b.provenance?.pass}** · failures: ${(b.provenance?.failures || []).join(", ") || "—"}`);
  lines.push("", "## Field findings (fails)", "");
  lines.push(
    "| Tab | Section | Component | Field | Value | Status | Action | Record ID | Patch |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"
  );
  for (const f of (b.findings || []).filter((x) => x.status !== "pass")) {
    const val = String(f.currentRenderedValue || "")
      .replace(/\|/g, "/")
      .replace(/\n/g, " ")
      .slice(0, 80);
    const patch = f.proposedPatch
      ? `${f.proposedPatch.action || "PATCH"} ${f.proposedPatch.slotKey || ""}`
      : "—";
    lines.push(
      `| ${f.tabName} | ${f.sectionName} | ${f.componentLabel} | ${f.fieldName} | ${val} | ${f.status} | ${f.recommendedAction} | ${f.sourceRecordId || "—"} | ${patch} |`
    );
  }
  lines.push("", "## Empty render scan", "");
  for (const e of b.emptyScan?.findings || []) {
    lines.push(`- ${e.id}: ${e.detail}`);
  }
  lines.push("");
  return lines.join("\n");
}

export function writeTabFactoryAuditReports(report) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# Brand Explorer Tab Factory Audit`,
    "",
    `Generated: ${report.generatedAt}`,
    `auditComplete: **${report.auditComplete}**`,
    `patchPlanComplete: **${report.patchPlanComplete}**`,
    `auditPass: **${report.auditPass}** (requires failFindings = 0)`,
    "",
    `- Brands: **${report.summary.brandsAudited}**`,
    `- Field failFindings: **${report.summary.totalFailFindings}**`,
    `- Empty-render fails: **${report.summary.totalEmptyRenderFails}**`,
    "",
  ];
  for (const b of report.brandResults) {
    md.push(`### ${b.brandSlug}`);
    md.push(`- auditPass: **${b.auditPass}** · fails: ${b.failFindings} · empty: ${b.emptyRenderFailFindings}`);
    md.push(`- gates: ${Object.entries(b.gates || {}).map(([k, v]) => `${k}=${v}`).join(", ")}`);
    md.push("");
  }
  fs.writeFileSync(mdPath, md.join("\n"));

  const brandPaths = {};
  for (const b of report.brandResults) {
    const name = BRAND_MD[b.brandSlug];
    if (!name) continue;
    const p = path.join(reportsDir, name);
    fs.writeFileSync(p, brandMd(b));
    brandPaths[b.brandSlug] = p;
  }
  return { jsonPath, mdPath, brandPaths };
}
