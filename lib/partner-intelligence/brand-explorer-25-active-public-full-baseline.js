/**
 * Brand Explorer — 25 Active/Live public-full baseline (Tapestry promotion).
 *
 * Extends the protected 24 baseline after intentional Tapestry Active promotion.
 * Does not delete or overwrite the 24 freeze artifacts.
 *
 * Read-only freeze/report helpers. No Brand Status / CV / Source / Registry writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACTIVE_UNIVERSE_SOURCE,
  loadActiveUniverse,
  NON_ACTIVE_STATUS_CONFLICT_PROBES,
} from "./brand-explorer-active-universe.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import {
  BASELINE_VERSION as BASELINE_VERSION_24,
  EXPECTED_ACTIVE_COUNT as EXPECTED_ACTIVE_COUNT_24,
  REPORT_JSON as REPORT_JSON_24,
  ROOT,
} from "./brand-explorer-24-active-public-full-baseline.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const BASELINE_VERSION_25 = "25-active-public-full-baseline-v1";
export const EXPECTED_ACTIVE_COUNT_25 = 25;
export const REPORT_JSON_25 = "brand-explorer-25-active-public-full-baseline.json";
export const REPORT_MD_25 = "brand-explorer-25-active-public-full-baseline.md";
export const DOCS_MD_25 = "brand-explorer-25-active-public-full-baseline.md";

/** Brand that expands 24 → 25 when intentionally promoted. */
export const BASELINE_25_ADDED_SLUG = "tapestry-collection-by-hilton";
export const BASELINE_25_ADDED_RECORD_ID = "reccXxMHEh7NNRhIE";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
}

async function fetchBrand(id) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: id }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

/**
 * Build a 25-brand baseline snapshot from live Active/Live universe.
 * Requires Tapestry Active/Live and totalCount === 25.
 */
export async function build25ActivePublicFullBaseline({ requireReports = false } = {}) {
  const universe = await loadActiveUniverse({ includeDetails: true });
  const frozen24Path = path.join(REPORTS_DIR, REPORT_JSON_24);
  const frozen24 = fs.existsSync(frozen24Path)
    ? JSON.parse(fs.readFileSync(frozen24Path, "utf8"))
    : null;

  const brands = (universe.brands || []).map((b) => ({
    slug: b.slug,
    name: b.name,
    recordId: b.recordId,
    brandStatus: b.status,
    shouldRenderFullProfile: b.publicFull === true,
    publicFullProfile: b.publicFull === true,
    presentationRowCount: b.presentationRowCount || 0,
    displayState: b.displayState || null,
    addedInBaseline25: b.slug === BASELINE_25_ADDED_SLUG,
  }));

  const tapestry = brands.find((b) => b.slug === BASELINE_25_ADDED_SLUG);
  const tapestryLive = tapestry || (await fetchBrand(BASELINE_25_ADDED_RECORD_ID));
  const tapestryStatus =
    tapestry?.brandStatus ||
    nz(tapestryLive?.brandStatus || tapestryLive?.status);

  const excluded = [];
  for (const probe of NON_ACTIVE_STATUS_CONFLICT_PROBES) {
    if (probe.slug === BASELINE_25_ADDED_SLUG) continue; // intentionally included when Active
    const fetched = await fetchBrand(probe.recordId);
    const status = nz(fetched?.brandStatus || fetched?.status);
    excluded.push({
      slug: probe.slug,
      name: probe.name,
      recordId: probe.recordId,
      brandStatus: status,
      isActiveLive: isBrandStatusActive(status),
      includedInBaseline: false,
    });
  }

  const activeCount = universe.totalCount;
  const tapestryIncluded = Boolean(tapestry) && isBrandStatusActive(tapestryStatus);
  const frozen =
    activeCount === EXPECTED_ACTIVE_COUNT_25 &&
    tapestryIncluded &&
    brands.every((b) => isBrandStatusActive(b.brandStatus));

  const freezeDecision = frozen
    ? "frozen_25_active_public_full_baseline"
    : "not_frozen_25_prerequisites_incomplete";

  return {
    version: BASELINE_VERSION_25,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    airtableWrites: false,
    predecessorBaseline: {
      version: BASELINE_VERSION_24,
      expectedCount: EXPECTED_ACTIVE_COUNT_24,
      report: REPORT_JSON_24,
      present: Boolean(frozen24),
    },
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_25,
    activeCount,
    frozen,
    freezeDecision,
    brands,
    excludedNonActive: excluded,
    tapestry: {
      slug: BASELINE_25_ADDED_SLUG,
      recordId: BASELINE_25_ADDED_RECORD_ID,
      brandStatus: tapestryStatus,
      included: tapestryIncluded,
      shouldRenderFullProfile: tapestry?.shouldRenderFullProfile === true,
    },
    summary: {
      frozen,
      freezeDecision,
      activeCount,
      expected: EXPECTED_ACTIVE_COUNT_25,
      tapestryIncluded,
      requireReports,
    },
  };
}

export function write25ActivePublicFullBaselineReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON_25);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD_25);
  const docsPath = path.join(DOCS_DIR, DOCS_MD_25);

  const lines = [
    `# Brand Explorer — 25 Active/Live Public-Full Baseline`,
    ``,
    `> Version \`${report.version}\` · Generated \`${report.generatedAt}\``,
    `> Predecessor: 24 baseline (\`${report.predecessorBaseline?.version}\`)`,
    ``,
    `| Metric | Value |`,
    `|---|---|`,
    `| Expected Active/Live | **${report.expectedActiveCount}** |`,
    `| Live count | **${report.activeCount}** |`,
    `| Frozen | **${report.frozen}** |`,
    `| Decision | \`${report.freezeDecision}\` |`,
    `| Tapestry included | **${report.tapestry?.included}** (status: ${report.tapestry?.brandStatus || "—"}) |`,
    ``,
    `## Brands (${(report.brands || []).length})`,
    ``,
  ];
  for (const b of report.brands || []) {
    lines.push(
      `- ${b.addedInBaseline25 ? "**[+25]** " : ""}\`${b.slug}\` — ${b.name} (${b.brandStatus}) full=${b.shouldRenderFullProfile}`
    );
  }
  lines.push(``);
  lines.push(`## Still excluded`);
  lines.push(``);
  for (const ex of report.excludedNonActive || []) {
    lines.push(`- \`${ex.slug}\` — ${ex.brandStatus} (activeLive=${ex.isActiveLive})`);
  }
  lines.push(``);

  const md = `${lines.join("\n")}\n`;
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  return { jsonPath, mdPath, docsPath };
}

// Aliases expected by tapestry factory promotion orchestrator
export {
  BASELINE_VERSION_25 as BASELINE_VERSION,
  EXPECTED_ACTIVE_COUNT_25 as EXPECTED_ACTIVE_COUNT,
};
