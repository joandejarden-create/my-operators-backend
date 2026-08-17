#!/usr/bin/env node
/**
 * Read-only: Active/Live brands vs Footprint Metrics display gate
 * ("Portfolio data being verified." / hidden tables).
 *
 * Quiet sequential brand-detail fetches to avoid Airtable 429s.
 *
 * Usage:
 *   node scripts/audit-brand-explorer-active-footprint-display-gate.mjs
 *   node scripts/audit-brand-explorer-active-footprint-display-gate.mjs --delay-ms=900
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { getBrandLibraryBrandById } from "../api/brand-library.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const REPORT_JSON = path.join(ROOT, "reports", "brand-explorer-active-footprint-display-gate.json");
const REPORT_MD = path.join(ROOT, "reports", "brand-explorer-active-footprint-display-gate.md");

function argVal(name, fallback) {
  const hit = process.argv.find((a) => a.startsWith(`${name}=`));
  if (!hit) return fallback;
  return hit.slice(name.length + 1);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function loadCensusMetricsHelper() {
  const code = fs.readFileSync(path.join(ROOT, "public/js/brand-explorer-census-metrics.js"), "utf8");
  const sandbox = { console, URL, URLSearchParams };
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox.BrandExplorerCensusMetrics;
}

/** Mirror public/js/brand-explorer-census-metrics.js isDemoOrMockFootprintBrand (not exported). */
function isDemoOrMockFootprintBrand(brand) {
  const norm = (v) =>
    String(v == null ? "" : v)
      .trim()
      .toLowerCase();
  const hasVal = (v) => v != null && v !== "";
  const unverifiedVer = (t) =>
    /unverified|draft|placeholder|demo|sample|pending|tbd|not verified|under review/.test(t);
  const unverifiedSrc = (t) => /placeholder|demo|sample|draft|unverified|tbd|mock/.test(t);
  if (hasVal(brand.explorerHeroVerification)) {
    const t = norm(brand.explorerHeroVerification);
    if (t === "verified by brand") return true;
    if (unverifiedVer(t)) return true;
  }
  if (hasVal(brand.explorerHeroDataSource)) {
    const t = norm(brand.explorerHeroDataSource);
    if (t === "live airtable / brand setup data") return true;
    if (unverifiedSrc(t)) return true;
  }
  return false;
}

function mockRes() {
  const out = { statusCode: 200, payload: null };
  return {
    setHeader() {},
    status(code) {
      out.statusCode = code;
      return this;
    },
    json(payload) {
      out.payload = payload;
      return this;
    },
    getOut() {
      return out;
    },
  };
}

async function fetchBrandDetail(brandId) {
  const res = mockRes();
  await getBrandLibraryBrandById(
    { method: "GET", query: { brandId: String(brandId), refresh: "1" }, headers: {} },
    res
  );
  const out = res.getOut();
  if (out.statusCode !== 200 || !out.payload?.success || !out.payload?.brand) {
    const err = out.payload?.error || `HTTP ${out.statusCode}`;
    throw new Error(err);
  }
  return out.payload.brand;
}

function classifyRow(brand, M) {
  const trust = M.footprintTrustModel(brand);
  const disp = M.footprintDisplayModel(brand);
  const heroV = brand.explorerHeroVerification || "";
  const heroD = brand.explorerHeroDataSource || "";
  const demoMock = isDemoOrMockFootprintBrand(brand);
  const cs = brand.censusSummary;
  const asOf = brand.footprint?.formValues?.figuresAsOf || brand.footprint?.verification?.figuresAsOf || "";
  const status = brand.footprint?.verification?.status || "";
  const mvpHotels = brand.footprint?.totalExistingHotels ?? null;
  const censusHotels = cs?.metrics?.totalOpenHotels ?? null;

  let gateClass = "ok_show";
  if (!disp.showVerifiedMetrics) {
    gateClass = "hidden_unverified";
  } else if (demoMock && trust.sourceUsed === "census") {
    gateClass = "show_via_census_but_demo_hero";
  } else if (demoMock && trust.sourceUsed === "mvp-footprint") {
    gateClass = "show_via_mvp_but_demo_hero";
  } else if (disp.isUnverifiedFallback) {
    gateClass = "show_with_unverified_banner";
  }

  return {
    recordId: brand.id || brand.recordId || null,
    name: brand.name || brand.brandName || "",
    slug: brand.slug || "",
    gateClass,
    showVerifiedMetrics: !!disp.showVerifiedMetrics,
    sourceUsed: trust.sourceUsed,
    displaySourceLabel: disp.displaySourceLabel || trust.displaySourceLabel || null,
    metricsBanner: disp.metricsBanner || null,
    demoMockHero: !!demoMock,
    explorerHeroVerification: heroV || null,
    explorerHeroDataSource: heroD || null,
    figuresAsOf: asOf || null,
    footprintDataStatus: status || null,
    mvpOpenHotels: mvpHotels,
    censusAvailable: cs?.available === true,
    censusFallbackRecommended: cs?.fallbackRecommended === true,
    censusOpenHotels: censusHotels,
    censusWarnings: Array.isArray(cs?.warnings) ? cs.warnings.slice(0, 5) : [],
    unlockHints: buildUnlockHints({
      show: disp.showVerifiedMetrics,
      demoMock,
      trust,
      cs,
      asOf,
      status,
    }),
  };
}

function buildUnlockHints({ show, demoMock, trust, cs, asOf, status }) {
  if (show && !demoMock) return [];
  const hints = [];
  if (!show) {
    if (!(cs && cs.available === true && cs.fallbackRecommended === false)) {
      hints.push("Ensure censusSummary is available (alias coverage + BRAND_EXPLORER_CENSUS_METRICS on)");
    }
    if (!asOf) hints.push("Set Brand Footprint Figures as of");
    if (!status || status === "Placeholder" || status === "Needs Review") {
      hints.push("Set Footprint Data Status to Verified or Estimated (when numbers are real)");
    }
  }
  if (demoMock) {
    hints.push("Replace Demo/Mock Explorer Hero Verification + Data Source labels");
  }
  if (trust.sourceUsed === "unverified") {
    hints.push(`Current trust source: ${trust.sourceUsed}`);
  }
  return hints;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer — Active/Live Footprint Display Gate Audit");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Universe: Brand Status Active/Live (${report.activeCount})`);
  lines.push("Read-only. No Airtable writes.");
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`| Metric | Count |`);
  lines.push(`| --- | ---: |`);
  for (const [k, v] of Object.entries(report.summary)) {
    lines.push(`| ${k} | ${v} |`);
  }
  lines.push("");
  lines.push("## Hidden tables (`Portfolio data being verified.`)");
  lines.push("");
  const hidden = report.rows.filter((r) => r.gateClass === "hidden_unverified");
  if (!hidden.length) {
    lines.push("_None — every Active/Live brand currently shows Footprint Metrics tables._");
  } else {
    lines.push("| Brand | Slug | Trust | Demo/Mock hero | Census open | Figures as of | Unlock |");
    lines.push("| --- | --- | --- | --- | ---: | --- | --- |");
    for (const r of hidden) {
      lines.push(
        `| ${r.name} | \`${r.slug}\` | ${r.sourceUsed} | ${r.demoMockHero ? "yes" : "no"} | ${r.censusOpenHotels ?? "—"} | ${r.figuresAsOf || "—"} | ${(r.unlockHints || []).join("; ") || "—"} |`
      );
    }
  }
  lines.push("");
  lines.push("## Demo/Mock hero but tables still show");
  lines.push("");
  const demoShow = report.rows.filter((r) => r.demoMockHero && r.showVerifiedMetrics);
  if (!demoShow.length) {
    lines.push("_None._");
  } else {
    lines.push(
      "These match Marriott’s labeling pattern: Demo/Mock hero fields, but tables render via census or MVP trust. If census drops, they can flip to hidden."
    );
    lines.push("");
    lines.push("| Brand | Slug | Trust source | Census open | MVP open | Figures as of |");
    lines.push("| --- | --- | --- | ---: | ---: | --- |");
    for (const r of demoShow.sort((a, b) => a.name.localeCompare(b.name))) {
      lines.push(
        `| ${r.name} | \`${r.slug}\` | ${r.sourceUsed} | ${r.censusOpenHotels ?? "—"} | ${r.mvpOpenHotels ?? "—"} | ${r.figuresAsOf || "—"} |`
      );
    }
  }
  lines.push("");
  lines.push("## Fetch errors");
  lines.push("");
  if (!report.errors.length) lines.push("_None._");
  else {
    for (const e of report.errors) {
      lines.push(`- **${e.name || e.slug || e.recordId}**: ${e.error}`);
    }
  }
  lines.push("");
  return lines.join("\n");
}

async function main() {
  const delayMs = Math.max(250, Number(argVal("--delay-ms", "850")) || 850);
  const M = loadCensusMetricsHelper();

  console.log(`[footprint-gate] loading Active/Live universe…`);
  const universe = await loadActiveUniverse({ includeDetails: false });
  const activeList = Array.isArray(universe?.brands) ? universe.brands : [];
  console.log(`[footprint-gate] active count=${activeList.length} (universe.totalCount=${universe.totalCount}); delayMs=${delayMs}`);

  const rows = [];
  const errors = [];
  for (let i = 0; i < activeList.length; i++) {
    const entry = activeList[i];
    const recordId = entry.recordId || entry.id || entry.brandId;
    const name = entry.name || entry.brandName || entry.slug || recordId;
    const slug = entry.slug || "";
    process.stdout.write(`[${i + 1}/${activeList.length}] ${name}… `);
    try {
      const brand = await fetchBrandDetail(recordId);
      const row = classifyRow(brand, M);
      if (!row.recordId) row.recordId = recordId;
      if (!row.slug) row.slug = slug || brand.slug || "";
      rows.push(row);
      console.log(`${row.gateClass} (${row.sourceUsed}, show=${row.showVerifiedMetrics})`);
    } catch (err) {
      const msg = err?.message || String(err);
      console.log(`ERROR ${msg}`);
      errors.push({ recordId, name, slug, error: msg });
      if (/429|rate/i.test(msg)) {
        console.log(`[footprint-gate] rate limit — backing off 8s`);
        await sleep(8000);
      }
    }
    if (i < activeList.length - 1) await sleep(delayMs);
  }

  const summary = {
    activeCount: activeList.length,
    audited: rows.length,
    fetchErrors: errors.length,
    hidden_unverified: rows.filter((r) => r.gateClass === "hidden_unverified").length,
    show_via_census_but_demo_hero: rows.filter((r) => r.gateClass === "show_via_census_but_demo_hero").length,
    show_via_mvp_but_demo_hero: rows.filter((r) => r.gateClass === "show_via_mvp_but_demo_hero").length,
    show_with_unverified_banner: rows.filter((r) => r.gateClass === "show_with_unverified_banner").length,
    ok_show: rows.filter((r) => r.gateClass === "ok_show").length,
    demoMockHeroTotal: rows.filter((r) => r.demoMockHero).length,
    censusBackedShow: rows.filter((r) => r.showVerifiedMetrics && r.sourceUsed === "census").length,
    mvpBackedShow: rows.filter((r) => r.showVerifiedMetrics && r.sourceUsed === "mvp-footprint").length,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    activeCount: activeList.length,
    delayMs,
    summary,
    hiddenBrands: rows.filter((r) => r.gateClass === "hidden_unverified"),
    demoMockButShowing: rows.filter((r) => r.demoMockHero && r.showVerifiedMetrics),
    rows: rows.sort((a, b) => String(a.name).localeCompare(String(b.name))),
    errors,
  };

  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(REPORT_MD, toMarkdown(report));
  console.log(`[footprint-gate] wrote ${REPORT_JSON}`);
  console.log(`[footprint-gate] wrote ${REPORT_MD}`);
  console.log(`[footprint-gate] summary`, summary);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
