#!/usr/bin/env node
/**
 * Clean An & Casino / Unknown City on DR Hotel Property Census rows.
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import { runAnCasinoCityCleanup } from "../lib/independent-census/census-city-an-casino-cleanup-apply.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

function toMarkdown(report) {
  return [
    `# City cleanup — An & Casino / Unknown (DR)`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Apply executed:** ${report.apply_executed}`,
    `**Airtable writes:** ${report.airtable_writes}`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Bad-city targets | ${report.targets_found} |`,
    `| Proposals | ${report.proposal_count} |`,
    `| Failed | ${report.failed_count} |`,
    `| Patched | ${report.patched_count} |`,
    ``,
    `| Name | City before → after | State |`,
    `| --- | --- | --- |`,
    ...(report.proposals || []).map(
      (p) =>
        `| ${p.property_name} | ${p.city_before} → **${p.city_after}** | ${p.state_after || ""} |`
    ),
    ``,
  ].join("\n");
}

async function main() {
  const args = parseArgs();
  const envCheck = checkIntakeApplyEnv();
  if (args.apply && !(args.allConfirmsOk && envCheck.allOk)) {
    console.error("Apply blocked — missing confirms/env", {
      confirms: Object.entries(args.confirms)
        .filter(([, v]) => !v)
        .map(([k]) => k),
      env: envCheck.missing,
    });
    process.exit(1);
  }

  const report = await runAnCasinoCityCleanup({
    doWrite: args.apply,
    confirms: args.confirms,
    allConfirmsOk: args.allConfirmsOk,
    env: process.env,
  });

  const out = `reports/census-city-an-casino-cleanup-${args.apply ? "applied" : "dry-run"}.json`;
  mkdirSync(dirname(join(root, out)), { recursive: true });
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  writeFileSync(
    join(root, "docs/data-intelligence/census-city-an-casino-cleanup.md"),
    toMarkdown(report)
  );

  console.log(
    JSON.stringify(
      {
        ok: report.status !== "blocked",
        status: report.status,
        output: out,
        targets_found: report.targets_found,
        proposal_count: report.proposal_count,
        failed_count: report.failed_count,
        patched_count: report.patched_count,
        airtable_writes: report.airtable_writes,
        proposals: (report.proposals || []).map((p) => ({
          n: p.property_name,
          city: `${p.city_before} → ${p.city_after}`,
          state: p.state_after,
          brand: p.brand_after,
        })),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
