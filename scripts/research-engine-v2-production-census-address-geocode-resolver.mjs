/**
 * Production Census address-first geocode resolver — dry-run (no Airtable apply in this task).
 *
 *   npm run research-engine-v2:production-census-address-geocode-resolver -- --dry-run
 *   GEOCODING_PROVIDER=google npm run research-engine-v2:production-census-address-geocode-resolver -- --dry-run --geocode-limit=40
 *
 * Apply is disabled unless all confirm flags are present (founder-approved later).
 * Does not invoke Webhound. Does not patch Brand Explorer.
 */

import { mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import "dotenv/config";
import {
  parseAddressGeocodeArgs,
  runAddressGeocodeDryRun,
  finalizeNextStep,
  renderAddressGeocodeMarkdown,
  STATUS,
  APPLY_CONFIRM_FLAGS,
} from "../lib/research-engine-v2/production-census-address-geocode-resolver.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

function renderDurableDoc(report) {
  const s = report.summary || {};
  const g = report.geocoding || {};
  return `# Production Census Address-First Geocode Resolver

**Status:** \`${report.status}\`  
**Contract:** \`${report.version}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** false (dry-run only)

## Executive summary

Address-first Census coordinate lane: confirm official hotel address, use official lat/lng when present, otherwise geocode property name + street address via \`GEOCODING_PROVIDER\` (mapbox | google | none). Webhound remains closed.

| Metric | Value |
| --- | ---: |
| Scanned | ${s.total_records_scanned ?? "—"} |
| Active missing coordinates | ${s.active_brand_missing_coordinates ?? "—"} |
| Official address found | ${s.records_with_official_address_found ?? "—"} |
| Official coordinates | ${s.records_with_official_coordinates_found ?? "—"} |
| Geocoder requests | ${s.records_sent_to_geocoder ?? "—"} |
| Proposed | ${s.records_proposed_for_coordinate_update ?? "—"} |
| Provider | ${g.provider ?? "—"} |
| Est. cost (USD) | ${s.geocoding_cost_estimate?.estimated_usd ?? "—"} |
| Webhound writes | 0 |

## Provider / terms

${(g.terms_warnings || []).map((w) => `- ${w}`).join("\n")}

## Schema v1.1.3

Supporting provenance fields are **not** in the live schema. Recommended add (separate task): Address Confidence, Address Source URL, Coordinate Source Type, Coordinate Confidence, Geocode Provider, Geocode Method, Geocode Reviewed Date. Dry-run report captures these until approved.

## Commands

\`\`\`bash
npm run research-engine-v2:production-census-address-geocode-resolver -- --dry-run
GEOCODING_PROVIDER=mapbox MAPBOX_PERMANENT_GEOCODING=1 npm run research-engine-v2:production-census-address-geocode-resolver -- --dry-run
\`\`\`

## Next step

${report.next_step}
`;
}

function runBrandExplorerGates() {
  const cmds = [
    ["npm", ["run", "brand-explorer-active-universe-source-of-truth", "--", "--dry-run"]],
    ["npm", ["run", "brand-explorer-global-active-semantic-audit", "--", "--dry-run", "--fresh"]],
    ["node", ["scripts/brand-explorer-quiet-sequential-pvql.mjs"]],
    ["npm", ["run", "test:brand-explorer-recent-momentum-evidence-quality"]],
    ["npm", ["run", "test:brand-explorer-mandatory-release-gates"]],
  ];
  const results = [];
  for (const [cmd, args] of cmds) {
    console.log(`[addr-geocode] BE gate: ${cmd} ${args.join(" ")}`);
    const r = spawnSync(cmd, args, {
      cwd: ROOT,
      encoding: "utf8",
      shell: true,
      env: process.env,
      maxBuffer: 20 * 1024 * 1024,
    });
    const pass = r.status === 0;
    results.push({
      command: `${cmd} ${args.join(" ")}`,
      exit_code: r.status,
      pass,
      stderr_tail: String(r.stderr || "")
        .split(/\r?\n/)
        .filter(Boolean)
        .slice(-8)
        .join("\n"),
      stdout_tail: String(r.stdout || "")
        .split(/\r?\n/)
        .filter(Boolean)
        .slice(-12)
        .join("\n"),
    });
    if (!pass) {
      console.error(`[addr-geocode] BE gate FAILED: ${cmd} ${args.join(" ")} exit=${r.status}`);
    }
  }
  return {
    touched: false,
    writes: 0,
    gates_run: true,
    all_pass: results.every((x) => x.pass),
    results,
  };
}

async function main() {
  const args = parseAddressGeocodeArgs();
  if (args.apply) {
    console.error(
      "[addr-geocode] --apply is not enabled in this task. Dry-run only. After founder approval, re-run with all confirm flags:",
      APPLY_CONFIRM_FLAGS.join(" ")
    );
    process.exit(2);
  }

  mkdirSync(REPORTS, { recursive: true });
  mkdirSync(DOCS, { recursive: true });

  console.log(
    `[addr-geocode] dry-run fetch-limit=${args.fetchLimit} geocode-limit=${args.geocodeLimit} provider=${args.providerOverride || process.env.GEOCODING_PROVIDER || "auto"}`
  );

  let report = await runAddressGeocodeDryRun(args);
  report = finalizeNextStep(report);

  // Brand Explorer gates (Census-only lane should leave BE untouched / PASS)
  if (args.skipBeGates) {
    report.brand_explorer_safety = {
      touched: false,
      writes: 0,
      gates_run: false,
      skipped: true,
      note: "Passed --skip-be-gates; run gates separately before founder apply review.",
    };
  } else {
    try {
      report.brand_explorer_safety = runBrandExplorerGates();
    } catch (err) {
      report.brand_explorer_safety = {
        touched: false,
        writes: 0,
        gates_run: true,
        all_pass: false,
        error: err?.message || String(err),
      };
    }
  }

  const dryJson = join(REPORTS, "production-census-address-geocode-resolver-dry-run.json");
  const dryMd = join(REPORTS, "production-census-address-geocode-resolver-dry-run.md");
  writeJson(dryJson, report);
  writeMd(dryMd, renderAddressGeocodeMarkdown(report));
  writeMd(join(DOCS, "production-census-address-geocode-resolver.md"), renderDurableDoc(report));

  console.log(`[addr-geocode] status=${report.status}`);
  console.log(
    `[addr-geocode] proposed=${report.summary.records_proposed_for_coordinate_update} blocked=${report.summary.records_blocked} geocoded=${report.summary.records_sent_to_geocoder} provider=${report.geocoding.provider}`
  );
  console.log(
    `[addr-geocode] BE gates all_pass=${report.brand_explorer_safety?.all_pass}`
  );

  const okStatuses = new Set([STATUS.READY, STATUS.NEEDS_PROVIDER_OR_TERMS, STATUS.NEEDS_SCHEMA_V113]);
  // Exit 0 for reviewable statuses; 2 for blocked source quality
  process.exit(report.status === STATUS.BLOCKED_SOURCE_QUALITY ? 2 : okStatuses.has(report.status) ? 0 : 2);
}

main().catch((err) => {
  console.error("[addr-geocode] fatal:", err);
  process.exit(1);
});
