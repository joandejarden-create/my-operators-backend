#!/usr/bin/env node
/**
 * Remediate intake-applied HPC rows: Family / Brand Family / State / hostels.
 *
 * Default dry-run. Live:
 *   --apply --enable-production-writes + all intake confirms + env flags
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import { runIntakeFormatRemediation } from "../lib/independent-census/intake-format-remediation.js";

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
    appliedPath: get(
      "--applied",
      "reports/census-intake-autopilot-apply-osm-dominican-republic-hotel-focused-2026-08-07-url-enriched-applied.json"
    ),
    bundlePath: get(
      "--bundle",
      "reports/census-intake-autopilot-approval-bundle-osm-dominican-republic-hotel-focused-2026-08-07-url-enriched-no_hr.json"
    ),
    output: get("--output", ""),
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

function toMarkdown(report) {
  return [
    `# Census intake format remediation`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Version:** ${report.version}`,
    `**Apply executed:** ${report.apply_executed}`,
    `**Airtable writes:** ${report.airtable_writes}`,
    ``,
    `## Counts`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Records found | ${report.records_found} |`,
    `| Patches | ${report.patch_count} |`,
    `| Deletes (hostels) | ${report.delete_count} |`,
    `| No-op | ${report.noop_count} |`,
    ``,
    `## Patch sample`,
    ``,
    `| Name | Before Family | After Family | Before BF | After BF | Clear State |`,
    `| --- | --- | --- | --- | --- | --- |`,
    ...(report.proposals || [])
      .filter((p) => !p.delete_record && Object.keys(p.patch).length)
      .slice(0, 25)
      .map((p) => {
        const afterFam =
          p.patch["Family / Source Family"] === null
            ? "(clear)"
            : p.patch["Family / Source Family"] ?? p.before.family;
        const afterBf =
          p.patch["Brand Family"] === null
            ? "(clear)"
            : p.patch["Brand Family"] ?? p.before.brand_family;
        return `| ${p.property_name} | ${p.before.family} | ${afterFam} | ${p.before.brand_family} | ${afterBf} | ${p.patch["State / Region"] === null} |`;
      }),
    ``,
    `## Deletes`,
    ``,
    ...(report.proposals || [])
      .filter((p) => p.delete_record)
      .map((p) => `- ${p.property_name} (\`${p.identity_key}\`) — hostel/hostal out of scope`),
    ``,
  ].join("\n");
}

async function main() {
  const args = parseArgs();
  const envCheck = checkIntakeApplyEnv();
  if (args.apply && !(args.allConfirmsOk && envCheck.allOk)) {
    console.error("Apply blocked — missing confirms/env");
    console.error("confirms missing", Object.entries(args.confirms).filter(([, v]) => !v).map(([k]) => k));
    console.error("env missing", envCheck.missing);
    process.exit(1);
  }

  const keys = new Set();
  if (existsSync(join(root, args.bundlePath))) {
    const bundle = JSON.parse(readFileSync(join(root, args.bundlePath), "utf8"));
    for (const i of bundle.inserts || []) {
      const k = i.fields?.["Property Identity Key"];
      if (k) keys.add(k);
    }
  }
  if (existsSync(join(root, args.appliedPath))) {
    const applied = JSON.parse(readFileSync(join(root, args.appliedPath), "utf8"));
    for (const p of applied.writable_preview || []) {
      if (p.identity_key) keys.add(p.identity_key);
    }
  }

  const report = await runIntakeFormatRemediation({
    identityKeys: [...keys],
    doWrite: args.apply,
    confirms: args.confirms,
    allConfirmsOk: args.allConfirmsOk,
    env: process.env,
  });

  const out =
    args.output ||
    `reports/census-intake-format-remediation-${args.apply ? "applied" : "dry-run"}.json`;
  mkdirSync(dirname(join(root, out)), { recursive: true });
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  const mdPath = `docs/data-intelligence/census-intake-format-remediation.md`;
  writeFileSync(join(root, mdPath), toMarkdown(report));

  console.log(
    JSON.stringify(
      {
        ok: report.status !== "blocked",
        status: report.status,
        output: out,
        records_found: report.records_found,
        patch_count: report.patch_count,
        delete_count: report.delete_count,
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
