#!/usr/bin/env node
/**
 * Offline ADP ACI + Presence Index audit V2.
 *   npm run adp:aci-presence-index-audit-v2
 * No provider calls. Does not write Airtable or mutate published snapshots.
 */

import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { runAciPresenceIndexAuditV2 } from "../lib/ai-demand-positioning/metrics/aci-presence-index-audit-v2.js";

const PROPERTY_ID = process.argv.includes("--property")
  ? process.argv[process.argv.indexOf("--property") + 1]
  : "adp_waterstone_boca_raton";

function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  if (!profile || !periods.length) {
    console.error("Missing profile or periods for", PROPERTY_ID);
    process.exit(1);
  }
  const parsedPeriods = periods.filter((p) => (p.observations || []).some((o) => o.parsed));
  const period = parsedPeriods[parsedPeriods.length - 1] || periods[periods.length - 1];
  const scenarios = buildScenarioUniverse(profile);
  const audit = runAciPresenceIndexAuditV2({
    period,
    scenarios,
    propertyProfile: profile,
    allPeriods: periods,
  });

  const dir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(dir, { recursive: true });
  const out = join(dir, "aci-presence-index-audit-v2.json");
  writeFileSync(out, JSON.stringify(audit, null, 2));
  console.log("Wrote", out);
  console.log("FINAL", audit.final);
  console.log("NEXT", audit.next);
  console.log("OVERALL_ACI_STATUS", audit.certification.OVERALL_ACI_STATUS);
  console.log("OVERALL_PRESENCE_INDEX_STATUS", audit.certification.OVERALL_PRESENCE_INDEX_STATUS);
  console.log("RAW_ENTITIES", audit.entityGovernance.RAW_ENTITIES);
  console.log("CANONICAL_HOTELS", audit.entityGovernance.CANONICAL_HOTELS);
}

main();
