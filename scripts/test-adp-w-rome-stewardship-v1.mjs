/**
 * W Rome ADP stewardship — peer pack ADEQUATE, scenarios certified, no Rome prod switches.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandPortfolioPeerSet } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-peer-set-v1.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import {
  propertyCoreGovernanceReady,
  stabilizedCoreIdsForProperty,
} from "../lib/ai-demand-positioning/metrics/property-core-governance-data.js";
import { buildWRomePreflight } from "../lib/ai-demand-positioning/execution/w-rome-baseline-period-001-v1.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ADP = "adp_w_rome";

let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (e) {
    failed += 1;
    console.error(`FAIL ${name}: ${e.message}`);
  }
}

check("peer_pack_adequate_five", () => {
  const peers = getBrandPortfolioPeerSet(ADP);
  assert.ok(peers);
  assert.equal(peers.included.length, 5);
  assert.equal(peers.adequacy.status, "ADEQUATE");
});

check("profile_census_and_comp_set", () => {
  const p = loadPropertyProfile(ADP);
  assert.equal(p.censusRecordId, "rece0or38cxo3Fymb");
  assert.equal(p.stewardship.peerPackStatus, "CERTIFIED");
  assert.ok(p.declaredCompSet.length >= 5);
});

check("scenarios_no_us_bleed", () => {
  const p = loadPropertyProfile(ADP);
  const sc = buildScenarioUniverse(p);
  assert.ok(sc.length >= 15);
  for (const s of sc) {
    assert.ok(!/\b(bethesda|nih|dmv|renaissance|times square|manhattan|new york)\b/i.test(s.query || ""));
  }
});

check("core_governance_ready", () => {
  assert.equal(propertyCoreGovernanceReady(ADP), true);
  assert.ok(stabilizedCoreIdsForProperty(ADP, TRAVELER_INTENTS.LEISURE).length >= 4);
});

check("preflight_pass", () => {
  const pf = buildWRomePreflight();
  assert.equal(pf.PREFLIGHT, "PASS");
  assert.equal(pf.blockers.length, 0);
});

check("no_rome_if_switches_in_peer_module", () => {
  const src = fs.readFileSync(
    path.join(ROOT, "lib/ai-demand-positioning/brand-portfolio/brand-portfolio-peer-set-v1.js"),
    "utf8"
  );
  assert.ok(!/if\s*\(\s*country\s*===\s*['\"]Italy['\"]/i.test(src));
  assert.ok(!/if\s*\(\s*city\s*===\s*['\"]Rome['\"]/i.test(src));
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
