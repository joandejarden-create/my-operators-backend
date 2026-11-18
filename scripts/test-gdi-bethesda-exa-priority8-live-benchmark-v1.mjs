#!/usr/bin/env node
/**
 * Offline gates for Bethesda Exa priority-8 live benchmark package.
 * Does not spend. Does not write Airtable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { quoteExaAgentRun, EXA_AGENT_EFFORT } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-bethesda-exa-priority8-live-benchmark-v1");
const SCRIPT = path.join(ROOT, "scripts/gdi-bethesda-exa-priority8-live-benchmark-v1.mjs");

const REQUIRED_REPORTS = [
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  "EXA_SPEND_LEDGER.json",
  "EIGHT_SEGMENT_EXECUTION_SCORECARD.md",
  "NEW_OPPORTUNITIES_AND_EVIDENCE.json",
  "COMMERCIAL_QUALIFICATION_RESULTS.md",
  "CONTACT_VERIFICATION.md",
  "OPPORTUNITY_DRAWER_QUALITY.md",
  "DEDUPLICATION_AND_WEEKLY_CHANGE.md",
  "AIRTABLE_RESEARCH_WRITE_DRY_RUN.json",
  "DISCOVERY_GAPS_AND_NEXT_IMPROVEMENTS.md",
  "FOUNDER_NEXT_DECISIONS.md",
  "TEST_RESULTS.md",
];

const PRIORITY = [
  "corporate_meetings_incentives",
  "corporate_accounts_scaling",
  "projects_infrastructure",
  "education_scientific",
  "relocation_extended_stay",
  "government_public_sector",
  "preferred_lodging_partnerships",
  "professional_event_planners",
];

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("ok:", msg);
  }
}

assert(fs.existsSync(SCRIPT), "benchmark script exists");
const src = fs.readFileSync(SCRIPT, "utf8");
assert(src.includes("gdi-bethesda-exa-priority8-benchmark-v1"), "authorization id in script");
assert(src.includes("HARD_CEILING_USD = 54"), "hard ceiling 54");
assert(src.includes("--authorize-paid"), "authorize-paid gate");
assert(src.includes("forceRerun: true"), "forceRerun for genuine retrieval");
assert(src.includes("apply: false"), "persistence dry-run only");
assert(!/authorize-publish|APPLY_RESEARCH_PERSISTENCE\s*=\s*true/i.test(src), "no publish/apply unlock");

for (const id of PRIORITY) {
  assert(src.includes(`"${id}"`) || src.includes(`id: "${id}"`), `priority segment ${id}`);
}

const quote = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM });
assert(quote.bounded === true, "medium quote bounded");
assert(Number(quote.maxUsd) > 0 && Number(quote.maxUsd) <= 54, "unit quote fits ceiling");

const chainSrc = fs.readFileSync(
  path.join(
    ROOT,
    "lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js",
  ),
  "utf8",
);
assert(chainSrc.includes("forceRerun: opts.forceRerun === true"), "chain forwards forceRerun");
assert(chainSrc.includes("stateRoot: opts.stateRoot"), "chain forwards stateRoot");

if (fs.existsSync(path.join(OUT, "EXA_SPEND_LEDGER.json"))) {
  const ledger = JSON.parse(fs.readFileSync(path.join(OUT, "EXA_SPEND_LEDGER.json"), "utf8"));
  assert(Number(ledger.spentUsd || 0) <= 54 + 1e-9, "ledger spent within $54");
  assert(ledger.authorizationId === "gdi-bethesda-exa-priority8-benchmark-v1", "ledger auth id");
  for (const name of REQUIRED_REPORTS) {
    assert(fs.existsSync(path.join(OUT, name)), `report ${name}`);
  }
  const acceptance = JSON.parse(
    fs.readFileSync(path.join(OUT, "BENCHMARK_ACCEPTANCE.json"), "utf8"),
  );
  assert(acceptance.airtableProductionWrites === 0, "acceptance: zero Airtable writes");
  assert(acceptance.publicationChanged === false, "acceptance: no publication change");
} else {
  console.log("note: reports not generated yet — live run required for report presence gates");
}

if (failed) {
  console.error(`\n${failed} assertion(s) failed`);
  process.exit(1);
}
console.log("\nAll offline priority-8 benchmark gates passed.");
