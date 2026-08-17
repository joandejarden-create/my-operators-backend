#!/usr/bin/env node
/**
 * Dominican Republic State / Region backfill (Hotel Property Census).
 * Default: dry-run. Live requires --apply --enable-production-writes + confirms + env.
 *
 *   node scripts/census-dr-state-region-backfill.mjs [--scope blank_only|all|osm_intake_only]
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import { runDominicanRepublicStateRegionBackfill } from "../lib/independent-census/dr-state-region-backfill.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function parseArgs(argv = process.argv.slice(2)) {
  const get = (name, fb = "") => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : fb;
  };
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    scope: get("--scope", "blank_only"),
    output: get("--output", ""),
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    overwriteExisting: argv.includes("--overwrite-existing"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

function toMarkdown(report) {
  const byProvince = new Map();
  for (const p of report.proposals || []) {
    byProvince.set(p.state_after, (byProvince.get(p.state_after) || 0) + 1);
  }
  return [
    `# Dominican Republic State / Region backfill`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Map version:** ${report.map_version}`,
    `**Scope:** ${report.scope}`,
    `**Apply executed:** ${report.apply_executed}`,
    `**Airtable writes:** ${report.airtable_writes}`,
    ``,
    `## Counts`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| DR rows scanned | ${report.input_count} |`,
    `| High proposals | ${report.proposal_count} |`,
    `| Steward (no High map) | ${report.steward_count} |`,
    `| Skipped | ${report.skipped_count} |`,
    `| Patched | ${report.patched_count} |`,
    ``,
    `## Province breakdown (proposals)`,
    ``,
    `| State / Region | Count |`,
    `| --- | ---: |`,
    ...[...byProvince.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([p, c]) => `| ${p} | ${c} |`),
    ``,
    `## Proposal sample`,
    ``,
    `| Name | City | State | OSM |`,
    `| --- | --- | --- | --- |`,
    ...(report.proposals || [])
      .slice(0, 30)
      .map(
        (p) =>
          `| ${p.property_name} | ${p.city_before} | ${p.state_after} | ${p.osm_intake} |`
      ),
    ``,
    `## Steward sample (need city cleanup / manual province)`,
    ``,
    `| Name | City | Reason |`,
    `| --- | --- | --- |`,
    ...(report.steward_sample || []).map(
      (s) => `| ${s.property_name} | ${s.city} | ${s.reason} |`
    ),
    ``,
  ].join("\n");
}

async function main() {
  const args = parseArgs();
  if (!["blank_only", "all", "osm_intake_only"].includes(args.scope)) {
    throw new Error("--scope must be blank_only|all|osm_intake_only");
  }
  const envCheck = checkIntakeApplyEnv();
  if (args.apply && !(args.allConfirmsOk && envCheck.allOk)) {
    console.error("Apply blocked — missing confirms/env");
    console.error(
      "confirms",
      Object.entries(args.confirms)
        .filter(([, v]) => !v)
        .map(([k]) => k)
    );
    console.error("env", envCheck.missing);
    process.exit(1);
  }

  const report = await runDominicanRepublicStateRegionBackfill({
    scope: args.scope,
    overwriteExisting: args.overwriteExisting,
    doWrite: args.apply,
    confirms: args.confirms,
    allConfirmsOk: args.allConfirmsOk,
    env: process.env,
  });

  const out =
    args.output ||
    `reports/census-dr-state-region-backfill-${args.scope}-${args.apply ? "applied" : "dry-run"}.json`;
  mkdirSync(dirname(join(root, out)), { recursive: true });
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  writeFileSync(
    join(root, "docs/data-intelligence/census-dr-state-region-backfill.md"),
    toMarkdown(report)
  );

  console.log(
    JSON.stringify(
      {
        ok: report.status !== "blocked",
        status: report.status,
        output: out,
        input_count: report.input_count,
        proposal_count: report.proposal_count,
        steward_count: report.steward_count,
        patched_count: report.patched_count,
        airtable_writes: report.airtable_writes,
      },
      null,
      2
    )
  );
  if (report.status === "blocked" || report.errors?.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
