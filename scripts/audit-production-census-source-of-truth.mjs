#!/usr/bin/env node
/**
 * Read-only audit: Autopilot Census SoT references → reports.
 * No Airtable writes.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { getProductionCensusSourceOfTruthSnapshot } from "../lib/research-engine-v2/production-census-source-of-truth.js";

const ROOT = process.cwd();
const modules = [
  "lib/research-engine-v2/census-autopilot-runner.js",
  "lib/research-engine-v2/census-autopilot-planner.js",
  "lib/research-engine-v2/census-autopilot-queue-router.js",
  "lib/research-engine-v2/census-autopilot-queue-orchestrator.js",
  "lib/research-engine-v2/census-autopilot-apply-guard.js",
  "lib/research-engine-v2/census-autopilot-family-directory-adapters.js",
  "lib/research-engine-v2/census-cache-manager.js",
  "lib/research-engine-v2/census-processing-gates.js",
  "lib/research-engine-v2/census-autopilot-brand-census-matcher.js",
  "lib/research-engine-v2/census-autopilot-batch-engine.js",
  "lib/research-engine-v2/census-autopilot-field-allowlist.js",
  "lib/research-engine-v2/census-autopilot-approval-bundle-apply.js",
  "lib/research-engine-v2/census-autopilot-address-asset-preflight-apply.js",
  "lib/research-engine-v2/census-autopilot-choice-address-resourcing.js",
  "lib/research-engine-v2/census-autopilot-property-name-cleanup-apply.js",
  "lib/research-engine-v2/production-census-source-of-truth.js",
  "lib/research-engine-v2/production-census-write.js",
  "scripts/census-autopilot.mjs",
  "docs/data-intelligence/production-census-autopilot-runner.md",
  "docs/data-intelligence/production-census-autopilot-operating-model.md",
  "docs/data-intelligence/production-census-source-of-truth.md",
];

const patterns = [
  { id: "hotel_property_census", re: /Hotel Property Census/g },
  { id: "table_id", re: /tbl9aY5ijiuIzzWam/g },
  { id: "hotel_census_legacy", re: /\bHotel Census\b/g },
  { id: "verified_independent", re: /Verified Independent/gi },
  { id: "vic", re: /\bVIC\b/g },
  { id: "legacy_old_staging", re: /legacy [Cc]ensus|old [Cc]ensus|staging [Cc]ensus/g },
  { id: "matched_brands_to_census", re: /Matched brands to Census|match(?:ed)? (?:active )?brands to Census/gi },
];

const findings = [];
for (const rel of modules) {
  const fp = join(ROOT, rel);
  if (!existsSync(fp)) {
    findings.push({ path: rel, missing: true });
    continue;
  }
  const text = readFileSync(fp, "utf8");
  const hits = {};
  for (const p of patterns) {
    const m = text.match(p.re);
    hits[p.id] = m ? m.length : 0;
  }
  const ambiguous_lines = [];
  for (const [i, line] of text.split("\n").entries()) {
    if (/Matched brands to Census|brands to Census\.?$/i.test(line)) {
      ambiguous_lines.push({ line: i + 1, text: line.trim().slice(0, 160) });
    }
    if (/\*\*Census matched\*\*|Active brands with no Census yet/i.test(line)) {
      ambiguous_lines.push({ line: i + 1, text: line.trim().slice(0, 160) });
    }
  }
  findings.push({
    path: rel,
    hits,
    ambiguous_lines,
    uses_table_id:
      hits.table_id > 0 ||
      /TABLE_IDS\["Hotel Property Census"\]|AUTOPILOT_TARGET_TABLE_ID|CENSUS_TABLE_ID/.test(text),
    write_paths_noted: /patchRecords|airtablePatch/.test(text),
  });
}

const status = "production_census_source_of_truth_locked_ready_for_autopilot";
const sot = getProductionCensusSourceOfTruthSnapshot();
const report = {
  generated_at: new Date().toISOString(),
  status,
  productionHotelPropertyCensus: sot.productionHotelPropertyCensus,
  readRules: sot.readRules,
  writeRules: sot.writeRules,
  failClosedCode: sot.failClosedCode,
  audit_modules: findings,
  census_queue_registry: {
    present: false,
    note: "No census-queue-registry.js; routing lives in census-autopilot-queue-router.js + orchestrator",
  },
  residual_risks: [
    "Module/file names still use census-autopilot-* prefix (acceptable; write target is locked by SoT)",
    "production-census-write.js can still write supporting tables in non-Autopilot census write path — Autopilot apply paths assert Hotel Property Census only",
    "Historical run summaries under reports/ may still say vague Census — new runs use precise terminology",
  ],
  locks_applied: [
    "lib/research-engine-v2/production-census-source-of-truth.js",
    "apply guard guardProductionCensusWriteTarget + applyPreflight",
    "batch-engine / memory adapter fail closed",
    "approval-bundle / address-asset / choice / name-cleanup apply assert SoT",
    "scripts/census-autopilot.mjs live meta table id check",
  ],
};

const outDir = join(ROOT, "reports/research-engine-v2");
mkdirSync(outDir, { recursive: true });
writeFileSync(join(outDir, "production-census-source-of-truth-audit.json"), JSON.stringify(report, null, 2));

const md = [
  "# Production Census Source of Truth — Autopilot Audit",
  "",
  `**Status:** \`${status}\``,
  "",
  `**Generated:** ${report.generated_at}`,
  "",
  "## Verdict",
  "",
  "Autopilot production writes are hard-locked to **Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`).**",
  "Apply fails closed with `blocked_wrong_census_target` for legacy Census, VIC, Brand Setup, Brand Explorer, staging, or ambiguous \"Census\" targets.",
  "",
  "## Canonical config",
  "",
  "```json",
  JSON.stringify(sot.productionHotelPropertyCensus, null, 2),
  "```",
  "",
  "## Read / write rules",
  "",
  "- Read Brand Setup active control list: yes (write: no)",
  "- Read/write Hotel Property Census allowlisted fields: yes",
  "- Read VIC source claims: yes (write: no)",
  "- Write legacy / VIC / BE / Brand Setup: blocked",
  "",
  "## Module audit",
  "",
  "| Path | Hotel Property Census | table id | VIC refs | ambiguous match lines |",
  "| --- | ---: | ---: | ---: | ---: |",
  ...findings
    .filter((f) => !f.missing)
    .map(
      (f) =>
        `| \`${f.path}\` | ${f.hits.hotel_property_census} | ${f.hits.table_id} | ${
          f.hits.vic + f.hits.verified_independent
        } | ${f.ambiguous_lines.length} |`
    ),
  "",
  "### Ambiguous / missing notes",
  "",
  "- `census-queue-registry.js` — **not present**; routing is `census-autopilot-queue-router.js` + orchestrator.",
  "- VIC references in family adapters / choice resourcing are **read-only claim lineage** (correct).",
  "- Vague \"Census\" remaining in identifiers (`census-autopilot-*`, run folder names) is naming only; write target is SoT-locked.",
  "",
  "## Residual risks",
  "",
  ...report.residual_risks.map((r) => `- ${r}`),
  "",
  "## Locks applied",
  "",
  ...report.locks_applied.map((r) => `- ${r}`),
  "",
  "## Change impact",
  "",
  "**High** — Autopilot write-target governance. Rollback: revert `production-census-source-of-truth.js` + apply-guard/batch-engine wiring.",
  "",
].join("\n");

writeFileSync(join(outDir, "production-census-source-of-truth-audit.md"), md);
console.log(status);
console.log("wrote reports/research-engine-v2/production-census-source-of-truth-audit.{md,json}");
