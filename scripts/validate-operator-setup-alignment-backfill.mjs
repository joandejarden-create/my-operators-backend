#!/usr/bin/env node
/**
 * Validate Phase 5C operator backfill plans against live Airtable options.
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getLiveOperatorAlignmentOptions } from "../lib/operator-alignment-airtable-options-loader.js";
import {
  loadActiveOperatorBackfillPlans,
  validateOperatorProposal,
} from "../lib/operator-alignment-operator-backfill-plans.js";
import { OPERATOR_BACKFILL_PRIORITY_FIELDS } from "../lib/operator-alignment-operator-field-map.js";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function fail(msg) {
  console.error("FAIL:", msg);
  failed += 1;
}
function ok(msg) {
  console.log("ok:", msg);
}

async function main() {
  const live = await getLiveOperatorAlignmentOptions({ refresh: true });
  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const plans = loadActiveOperatorBackfillPlans(live, candidates);

  for (const [, plan] of plans) {
    for (const [field, prop] of Object.entries(plan.fields)) {
      if (field === "Last Updated Date") continue;
      const v = validateOperatorProposal(field, prop.value, live);
      if (!v.ok) fail(`${plan.companyName} ${field}: ${(v.warnings || []).join("; ")}`);
    }
  }
  ok(`all ${plans.size} operator plans validate against live options`);

  const top10 = candidates.slice(0, 10);
  for (const c of top10) {
    const plan = plans.get(c.operatorId);
    if (!plan) {
      fail("missing plan for " + c.companyName);
      continue;
    }
    for (const field of OPERATOR_BACKFILL_PRIORITY_FIELDS) {
      const prop = plan.fields[field];
      if (!prop?.value || (Array.isArray(prop.value) && !prop.value.length)) {
        fail(`${c.companyName} missing priority field ${field}`);
      }
    }
    const svc = plan.fields["Offered Services"]?.value;
    if (!Array.isArray(svc) || svc.length < 4) {
      fail(`${c.companyName} Offered Services too thin (${svc?.length || 0})`);
    }
  }
  ok("top active operators have priority field proposals");

  const svcSets = new Set();
  for (const c of candidates) {
    const s = JSON.stringify(plans.get(c.operatorId)?.fields["Offered Services"]?.value || []);
    svcSets.add(s);
  }
  if (svcSets.size < 3) fail("Offered Services not differentiated enough across operators");
  else ok(`Offered Services differentiation: ${svcSets.size} unique bundles`);

  const ocs = fs.readFileSync(path.join(ROOT, "lib/operator-capability-snapshot-build.js"), "utf8");
  ok(!ocs.includes("OPERATOR_MATCH_WEIGHTS"), "OCS unchanged");

  console.log(failed ? `\n${failed} failure(s)` : "\nAll operator backfill validation checks passed.");
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
