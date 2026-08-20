#!/usr/bin/env node
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { runPresenceIndexV2Audit } from "../lib/ai-demand-positioning/metrics/presence-index-v2-audit.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";

function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  const parsed = periods.filter((p) => (p.observations || []).some((o) => o.parsed));
  const period = parsed[parsed.length - 1];
  const scenarios = buildScenarioUniverse(profile);
  const audit = runPresenceIndexV2Audit({ period, scenarios, propertyProfile: profile, allPeriods: periods });
  const dir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(dir, { recursive: true });
  const out = join(dir, "presence-index-v2-core-stability-v1.json");
  writeFileSync(out, JSON.stringify(audit, null, 2));
  console.log("Wrote", out);
  console.log("FINAL", audit.final);
  console.log("NEXT", audit.next);
  console.log("OVERALL_PRESENCE", audit.certification.OVERALL_STATUS);
  console.log("OVERALL_ACI", audit.aciProgress.OVERALL_ACI_STATUS);
}

main();
