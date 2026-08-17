/**
 * Peru MINCETUR steward insert apply — DRY-RUN by default.
 *
 * Live writes require:
 *   --enable-production-writes
 *   --confirm-peru-mincetur-steward-insert
 *   --confirm-no-owner-operator-writes
 *   --confirm-hotel-property-census-only
 *   --confirm-no-legacy-census-writes
 *
 * Usage:
 *   npm run census:peru-mincetur-steward-insert-apply -- --pack reports/.../peru-mincetur-steward-review-pack-….json
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  PERU_MINCETUR_STEWARD_APPLY_VERSION,
  parsePeruMinceturStewardApplyArgs,
  runPeruMinceturStewardInsertApply,
} from "../lib/research-engine-v2/peru-mincetur-steward-insert-apply.js";
import { PERU_MINCETUR_REQUIRED_APPLY_CONFIRMS } from "../lib/research-engine-v2/peru-mincetur-steward-review-pack.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function resolvePath(p) {
  if (!p) return "";
  return p.startsWith("/") || /^[A-Za-z]:/.test(p) ? p : join(process.cwd(), p);
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
}

function renderMarkdown(report) {
  const s = report.summary || {};
  const preview = report.writable_preview || [];
  return [
    `# Peru MINCETUR Steward Insert Apply`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Generated:** ${report.generated_at}`,
    `**Version:** \`${PERU_MINCETUR_STEWARD_APPLY_VERSION}\``,
    `**Dry-run:** ${report.dry_run}`,
    `**Airtable writes:** ${report.airtable_writes}`,
    `**Owner Name writes:** ${report.ownership_writes}`,
    `**Legacy Hotel Census writes:** ${report.legacy_hotel_census_writes}`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Proposed (pilot slice) | ${s.proposed_in_pack_slice ?? 0} |`,
    `| Prepared OK | ${s.prepared_ok ?? 0} |`,
    `| Invalid | ${s.invalid ?? 0} |`,
    `| Writable after re-dedupe | ${s.writable_after_rededupe ?? 0} |`,
    `| Blocked duplicates | ${s.blocked_duplicates ?? 0} |`,
    `| Created | ${s.created ?? 0} |`,
    ``,
    `## Required confirms`,
    ``,
    ...PERU_MINCETUR_REQUIRED_APPLY_CONFIRMS.map((c) => `- \`${c}\``),
    ``,
    `## Writable preview (dry-run)`,
    ``,
    `| Identity | Name | City | Rooms | Official URL | RUC signal |`,
    `| --- | --- | --- | ---: | --- | --- |`,
    ...preview.map(
      (r) =>
        `| ${r.identity_key || ""} | ${r.property_name || ""} | ${r.city || ""} | ${r.rooms ?? ""} | ${r.official_property_url || ""} | ${r.ownership_signal_ruc || ""} |`
    ),
    ``,
    `## Notes`,
    ``,
    `${report.note || ""}`,
    ``,
    report.blocked_reason ? `**Blocked:** ${report.blocked_reason}` : "",
    ``,
  ].join("\n");
}

const args = parsePeruMinceturStewardApplyArgs();
if (args.help) {
  console.log(`Peru MINCETUR steward insert apply

Dry-run (default):
  npm run census:peru-mincetur-steward-insert-apply -- --pack <steward-pack.json>

Live (gated):
  npm run census:peru-mincetur-steward-insert-apply -- --pack <steward-pack.json> \\
    --enable-production-writes \\
    --confirm-peru-mincetur-steward-insert \\
    --confirm-no-owner-operator-writes \\
    --confirm-hotel-property-census-only \\
    --confirm-no-legacy-census-writes

Options:
  --pack PATH         Steward review pack or approval bundle JSON
  --pilot-limit N     Cap inserts (default 25)
`);
  process.exit(0);
}

if (!args.packPath) {
  console.error("Missing --pack <steward-review-pack.json>");
  process.exit(1);
}

const packPath = resolvePath(args.packPath);
if (!existsSync(packPath)) {
  console.error(`Not found: ${packPath}`);
  process.exit(1);
}

const pack = JSON.parse(readFileSync(packPath, "utf8"));
console.log("=== Peru MINCETUR steward insert apply ===\n");
console.log(`Pack: ${packPath}`);
console.log(`Mode: ${args.enableProductionWrites ? "LIVE (gated)" : "DRY-RUN"}`);
console.log(`Pilot limit: ${args.pilotLimit}`);

const result = await runPeruMinceturStewardInsertApply({
  args,
  pack,
  allowLiveWrite: true,
});

const report = {
  generated_at: new Date().toISOString(),
  pack_path: packPath,
  ...result,
};

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });
const ts = stamp();
const suffix = report.dry_run ? "dry-run" : "applied";
const jsonPath = join(REPORTS, `peru-mincetur-steward-insert-apply-${suffix}-${ts}.json`);
const mdPath = join(REPORTS, `peru-mincetur-steward-insert-apply-${suffix}-${ts}.md`);
const docPath = join(DOCS, "production-census-peru-mincetur-steward-insert-apply.md");
const md = renderMarkdown(report);
writeFileSync(jsonPath, JSON.stringify(report, null, 2));
writeFileSync(mdPath, md);
writeFileSync(docPath, md);

console.log(`\nStatus: ${report.status}`);
if (report.blocked_reason) console.log(`Blocked: ${report.blocked_reason}`);
console.log(`Writable: ${report.summary?.writable_after_rededupe ?? 0}`);
console.log(`Created:  ${report.summary?.created ?? 0}`);
console.log(`\n  wrote: ${jsonPath}`);
console.log(`  wrote: ${docPath}`);
if (report.dry_run) {
  console.log("✓ Dry-run only — no Airtable writes.");
} else {
  console.log("✓ Apply path finished — verify created_record_ids in report.");
}

if (report.status === "peru_mincetur_steward_insert_blocked") process.exitCode = 2;
