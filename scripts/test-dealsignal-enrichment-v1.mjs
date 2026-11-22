#!/usr/bin/env node
/**
 * DealSignal Enrichment V1 gate — CUSTOMER_BLOCKED, idempotent IDs, no Surfe/schema automation.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/dealsignal-enrichment-v1");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

const required = [
  "FOUNDER_REPORT.md",
  "IC_HANDOFF.md",
  "ENRICHMENT_RESEARCH_PACKAGE.json",
  "PERSISTENCE_LEDGER_DRY_RUN.json",
  "PERSISTENCE_LEDGER_FIRST_APPLY.json",
  "PERSISTENCE_LEDGER_IDEMPOTENT.json",
  "EXA_BUDGET_LEDGER.json",
  "BASELINE_AIRTABLE_AUDIT.json",
];
for (const f of required) {
  assert(fs.existsSync(path.join(OUT, f)), `missing ${f}`);
}

const pkg = JSON.parse(fs.readFileSync(path.join(OUT, "ENRICHMENT_RESEARCH_PACKAGE.json"), "utf8"));
assert(pkg.publicationState === "CUSTOMER_BLOCKED", "package must be CUSTOMER_BLOCKED");

const first = JSON.parse(fs.readFileSync(path.join(OUT, "PERSISTENCE_LEDGER_FIRST_APPLY.json"), "utf8"));
assert(first.publicationState === "CUSTOMER_BLOCKED", "first apply CUSTOMER_BLOCKED");
assert(first.summary.creates === 6, `expected 6 creates, got ${first.summary.creates}`);
assert(first.principalBctMctUpdates === 0, "must not update principal BCT/MCT rows");
assert(first.schemaChanges === 0, "no schema changes");
assert(first.surfeCalls === 0, "no Surfe");

const idem = JSON.parse(fs.readFileSync(path.join(OUT, "PERSISTENCE_LEDGER_IDEMPOTENT.json"), "utf8"));
assert(idem.summary.noops === 6, `expected 6 NOOPs, got ${idem.summary.noops}`);
assert(idem.summary.creates === 0, "idempotent re-apply must not create");

const expectedIds = [
  "find_ds_enrich_v1_lakeville_inn_mn_bct_ownership_2026",
  "find_ds_enrich_v1_shorebreak_huntington_bct_ownership_2026",
  "find_ds_enrich_v1_courtyard_denver_bct_ownership_2026",
  "find_ds_enrich_v1_hilton_trinidad_mct_ownership_2026",
  "find_ds_enrich_v1_embassy_one_bengaluru_mct_ownership_2026",
  "find_ds_enrich_v1_ahip_aimbridge_mct_ownership_2026",
];
const created = first.operations.filter((o) => o.action === "CREATE").map((o) => o.id).sort();
assert(
  JSON.stringify(created) === JSON.stringify([...expectedIds].sort()),
  "CREATE finding IDs mismatch expected enrichment set",
);

const runSrc = fs.readFileSync(path.join(ROOT, "scripts/dealsignal-enrichment-v1-run.mjs"), "utf8");
assert(!/surfe/i.test(runSrc), "enrichment runner must not call Surfe");
assert(/CUSTOMER_BLOCKED/.test(runSrc), "runner must enforce CUSTOMER_BLOCKED");
assert(/Finding Type.*=.*OTHER|findingType.*=.*"OTHER"|COMMERCIAL_SIGNAL_OTHER/.test(runSrc), "enrichment must write OTHER findings");
assert(/--apply/.test(runSrc), "runner must gate writes behind --apply");

const budget = JSON.parse(fs.readFileSync(path.join(OUT, "EXA_BUDGET_LEDGER.json"), "utf8"));
const spent =
  budget.incrementalSpendUsd ??
  budget.spentUsd ??
  budget.totalSpentUsd ??
  budget.summary?.spentUsd ??
  null;
if (typeof spent === "number") {
  assert(spent <= 5.01, `Exa spend ${spent} exceeds $5 ceiling`);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      creates: first.summary.creates,
      noops: idem.summary.noops,
      publicationState: "CUSTOMER_BLOCKED",
      surfe: false,
      schemaChanges: 0,
    },
    null,
    2,
  ),
);
