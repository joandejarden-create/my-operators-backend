/**
 * Audit Active/Live brands — Value Creation Scenarios (valueOwners.scenario.1–4).
 * Gold bar: Ascend-style — 4 Proper Case titles + short paragraphs (~28–55 words).
 *
 *   node scripts/brand-explorer-value-creation-scenarios-audit.mjs --dry-run
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import {
  evaluateValueCreationScenariosBar,
  VALUE_CREATION_SCENARIO_SLOTS,
} from "../lib/partner-intelligence/brand-explorer-value-creation-scenarios-bar.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

async function main() {
  const universe = await loadActiveUniverse({ includeDetails: false });
  const results = [];

  for (const brand of universe.brands) {
    const fetch = await listPresentationRowsLight(brand.recordId, brand.name || brand.brandName);
    const rows = fetch.rows || [];
    const evalResult = evaluateValueCreationScenariosBar(rows, {
      brandSlug: brand.slug,
      brandName: brand.name || brand.brandName,
    });
    results.push({
      slug: brand.slug,
      name: brand.name || brand.brandName,
      recordId: brand.recordId,
      pass: evalResult.pass,
      failures: evalResult.failures,
      checks: evalResult.checks,
      scenarios: evalResult.scenarios.map((s) => ({
        slotKey: s.slotKey,
        title: s.title,
        wordCount: s.wordCount,
        hasBody: Boolean(s.body),
        recordId: s.recordId,
      })),
    });
  }

  const passing = results.filter((r) => r.pass);
  const failing = results.filter((r) => !r.pass);
  const tallies = {};
  for (const r of failing) {
    for (const f of r.failures) {
      const key = String(f).replace(/:\d+$/, "").replace(/_overview\.scenario\.\d+/, "");
      const k = String(f).replace(/valueOwners\.scenario\.\d+/, "N");
      tallies[k] = (tallies[k] || 0) + 1;
    }
  }

  const report = {
    version: "value-creation-scenarios-audit-v1",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    goldReference: "Ascend Hotel Collection (4 short owner-value paragraphs)",
    counts: { audited: results.length, pass: passing.length, fail: failing.length },
    failureTallies: tallies,
    passingSlugs: passing.map((r) => r.slug),
    failingSlugs: failing.map((r) => r.slug),
    brands: results,
  };

  fs.mkdirSync(REPORTS, { recursive: true });
  const jsonPath = path.join(REPORTS, "brand-explorer-value-creation-scenarios-audit.json");
  const mdPath = path.join(REPORTS, "brand-explorer-value-creation-scenarios-audit.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# Brand Explorer — Value Creation Scenarios Audit`,
    ``,
    `- Generated: ${report.generatedAt}`,
    `- Gold bar: Ascend — 4 Proper Case titles + short paragraphs (${VALUE_CREATION_SCENARIO_SLOTS.join(", ")})`,
    `- Audited: **${report.counts.audited}** · Pass: **${report.counts.pass}** · Fail: **${report.counts.fail}**`,
    ``,
    `## Failure tallies`,
    ``,
    ...Object.entries(tallies)
      .sort((a, b) => b[1] - a[1])
      .map(([k, n]) => `- \`${k}\`: ${n}`),
    ``,
    `## Passing (${passing.length})`,
    ``,
    ...passing.map((r) => `- ${r.name} (\`${r.slug}\`)`),
    ``,
    `## Failing (${failing.length})`,
    ``,
    ...failing.map((r) => {
      const titles = r.scenarios.map((s) => s.title || "(blank title)").join(" · ");
      const words = r.scenarios.map((s) => s.wordCount).join("/");
      return [
        `### ${r.name} (\`${r.slug}\`)`,
        `- Failures: ${r.failures.join(", ")}`,
        `- Titles: ${titles}`,
        `- Words: ${words}`,
        ``,
      ].join("\n");
    }),
  ].join("\n");
  fs.writeFileSync(mdPath, md);
  console.log(`Wrote ${mdPath}`);
  console.log(`pass=${passing.length}/${results.length} fail=${failing.length}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
