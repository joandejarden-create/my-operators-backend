/**
 * Audit Active/Live + future (Wave 12 factory) brands against the
 * Kimpton / Curio / Design Hotels scenario owner-value bar.
 *
 * Read-only. No Airtable writes.
 *
 *   node scripts/brand-explorer-scenario-owner-value-bar-audit.mjs --dry-run
 *   node scripts/brand-explorer-scenario-owner-value-bar-audit.mjs --brands kimpton-hotels,curio-collection
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { WAVE12_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave12-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { evaluateScenarioOwnerValueBar } from "../lib/partner-intelligence/brand-explorer-scenario-owner-value-bar.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1]
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean);
  }
  return null;
}

async function main() {
  const argv = process.argv.slice(2);
  const brandFilter = parseBrands(argv);

  const universe = await loadActiveUniverse({ includeDetails: false });
  const activeBrands = universe.brands.map((b) => ({
    slug: b.slug,
    name: b.name || b.brandName,
    recordId: b.recordId,
    cohort: "active",
  }));

  const futureBrands = WAVE12_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    return {
      slug,
      name: id?.name || slug,
      recordId: id?.recordId || null,
      cohort: "future_wave12",
    };
  }).filter((b) => b.recordId);

  const bySlug = new Map();
  for (const b of [...activeBrands, ...futureBrands]) {
    if (brandFilter && !brandFilter.includes(b.slug)) continue;
    const prev = bySlug.get(b.slug);
    if (prev) {
      prev.cohort = prev.cohort === "active" || b.cohort === "active" ? "active" : b.cohort;
      if (b.cohort === "future_wave12") prev.alsoFutureWave12 = true;
    } else {
      bySlug.set(b.slug, { ...b, alsoFutureWave12: b.cohort === "future_wave12" && activeBrands.some((a) => a.slug === b.slug) });
    }
  }

  const targets = [...bySlug.values()].sort((a, b) => a.slug.localeCompare(b.slug));
  const results = [];

  for (const brand of targets) {
    try {
      const fetch = await listPresentationRowsLight(brand.recordId, brand.name);
      const evalResult = evaluateScenarioOwnerValueBar(fetch.rows || [], { brandSlug: brand.slug });
      results.push({
        ...brand,
        pass: evalResult.pass,
        failures: evalResult.failures,
        checks: evalResult.checks,
        scenarios: evalResult.scenarios.map((s) => ({
          slotKey: s.slotKey,
          title: s.title,
          wordCount: s.wordCount,
          titleIsProperCase: s.titleIsProperCase,
          hasOwnerValueCues: s.hasOwnerValueCues,
          hasDiligenceCloser: s.hasDiligenceCloser,
          hasImage: Boolean(s.imageUrl),
        })),
      });
      process.stdout.write(evalResult.pass ? "." : "x");
    } catch (err) {
      results.push({
        ...brand,
        pass: false,
        failures: [`fetch_error:${err.message}`],
        checks: {},
        scenarios: [],
      });
      process.stdout.write("E");
    }
    await new Promise((r) => setTimeout(r, 120));
  }
  process.stdout.write("\n");

  const passing = results.filter((r) => r.pass);
  const failing = results.filter((r) => !r.pass);
  const failureTallies = {};
  for (const r of failing) {
    for (const f of r.failures || []) {
      const key = String(f).replace(/:\d+$/, "").replace(/distinct_\d+_of_\d+/, "distinct_dupe");
      failureTallies[key] = (failureTallies[key] || 0) + 1;
    }
  }

  const report = {
    version: "scenario-owner-value-bar-audit-v1",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    airtableWrites: false,
    goldReferences: ["Kimpton Hotels", "Curio Collection by Hilton", "Design Hotels"],
    bar: "Proper Case titles · unique images · distinct owner-value bodies · no identical diligence closer",
    counts: {
      audited: results.length,
      pass: passing.length,
      fail: failing.length,
      active: results.filter((r) => r.cohort === "active").length,
      futureWave12: results.filter((r) => r.cohort === "future_wave12" || r.alsoFutureWave12).length,
    },
    failureTallies,
    passingSlugs: passing.map((r) => r.slug),
    failingSlugs: failing.map((r) => r.slug),
    brands: results,
  };

  fs.mkdirSync(REPORTS, { recursive: true });
  const jsonPath = path.join(REPORTS, "brand-explorer-scenario-owner-value-bar-audit.json");
  const mdPath = path.join(REPORTS, "brand-explorer-scenario-owner-value-bar-audit.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const md = [
    `# Brand Explorer — Scenario Owner-Value Bar Audit`,
    ``,
    `- Generated: ${report.generatedAt}`,
    `- Gold bar: Kimpton · Curio · Design Hotels`,
    `- Audited: **${report.counts.audited}** · Pass: **${report.counts.pass}** · Fail: **${report.counts.fail}**`,
    `- Airtable writes: none`,
    ``,
    `## Failure tallies`,
    ``,
    ...Object.entries(failureTallies)
      .sort((a, b) => b[1] - a[1])
      .map(([k, n]) => `- \`${k}\`: ${n}`),
    ``,
    `## Passing (${passing.length})`,
    ``,
    passing.length ? passing.map((r) => `- ${r.name} (\`${r.slug}\`)`).join("\n") : "- none",
    ``,
    `## Failing (${failing.length})`,
    ``,
    ...failing.map((r) => {
      const titles = (r.scenarios || []).map((s) => s.title || "(blank)").join(" · ");
      return [
        `### ${r.name} (\`${r.slug}\`) — ${r.cohort}`,
        `- Failures: ${(r.failures || []).join(", ") || "none"}`,
        `- Titles: ${titles || "(none)"}`,
        ``,
      ].join("\n");
    }),
    `## Next steps`,
    ``,
    `1. Wave 12 / future: refresh scenario seeds to Proper Case + owner-value closers, then dry-run \`npm run brand-explorer-wave12-scenario-owner-value -- --dry-run\`.`,
    `2. Active brands failing title case / diligence pad: mechanical remediation (Title/Body only).`,
    `3. Active brands failing images or weak owner-value cues: editorial content packages before apply.`,
    `4. Apply only after founder review of dry-run patches.`,
    ``,
  ].join("\n");
  fs.writeFileSync(mdPath, md, "utf8");

  console.log(`Wrote ${mdPath}`);
  console.log(`pass=${passing.length}/${results.length} fail=${failing.length}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
