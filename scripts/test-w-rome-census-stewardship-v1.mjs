/**
 * W Rome census stewardship gates — provisional→canonical bind, apply clear, no hardcodes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveCanonicalHotelId,
  isAirtableRecordId,
} from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { getCensusRecordIdForAdpProperty } from "../lib/ai-demand-positioning/census-link-registry.js";
import {
  proposeHotelOnboardSeed,
  compareOnboardSeedProposals,
} from "../lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js";
import { INSERT_ADDRESS_FIELDS } from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import {
  ADP_JEV_FORBIDDEN,
  describeAdpJevShadowAdapter,
} from "../lib/ai-demand-positioning/adp-jev-shadow.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const CANONICAL = "rece0or38cxo3Fymb";
const PROVISIONAL = "gdi_hotel_w_rome";
const ADP = "adp_w_rome";
const NOW = "2026-09-25T12:00:00.000Z";

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

check("canonical_hpc_id_format", () => {
  assert.equal(isAirtableRecordId(CANONICAL), true);
});

check("provisional_resolves_to_canonical", () => {
  assert.equal(resolveCanonicalHotelId(PROVISIONAL), CANONICAL);
  assert.equal(resolveCanonicalHotelId(ADP), CANONICAL);
  assert.equal(resolveCanonicalHotelId(CANONICAL), CANONICAL);
});

check("adp_census_link_ready", () => {
  assert.equal(getCensusRecordIdForAdpProperty(ADP), CANONICAL);
});

check("canonical_config_exists", () => {
  const p = path.join(ROOT, "config/group-demand-intelligence/hotels", `${CANONICAL}.json`);
  assert.equal(fs.existsSync(p), true);
  const cfg = JSON.parse(fs.readFileSync(p, "utf8"));
  assert.equal(cfg.hotelId, CANONICAL);
  assert.equal(cfg.aliases.censusRecordId, CANONICAL);
});

check("provisional_is_redirect_only", () => {
  const cfg = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "config/group-demand-intelligence/hotels", `${PROVISIONAL}.json`),
      "utf8"
    )
  );
  assert.equal(cfg.redirectTo, CANONICAL);
});

check("seed_uses_canonical_hotel_id_from_provisional_alias", () => {
  const p = proposeHotelOnboardSeed(PROVISIONAL, { now: NOW });
  assert.equal(p.hotelId, CANONICAL);
  assert.ok(p.fits.every((f) => f.hotelId === CANONICAL));
  assert.ok(p.targets.every((t) => t.hotelId === CANONICAL));
});

check("provisional_vs_canonical_seed_100_overlap", () => {
  const a = proposeHotelOnboardSeed(PROVISIONAL, { now: NOW });
  const b = proposeHotelOnboardSeed(CANONICAL, { now: NOW });
  const c = compareOnboardSeedProposals(a, b);
  assert.equal(c.fitOverlapPct, 100);
  assert.equal(c.targetOverlapPct, 100);
  assert.equal(a.totals.fits, 7);
  assert.equal(a.totals.targets, 14);
});

check("postal_code_allowlisted_for_insert", () => {
  assert.ok(INSERT_ADDRESS_FIELDS.includes("Postal Code"));
});

check("write_result_present", () => {
  const wr = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "reports/group-demand-intelligence/w-rome-census-stewardship-v1/WRITE_RESULT.json"
      ),
      "utf8"
    )
  );
  assert.equal(wr.recordId, CANONICAL);
  assert.equal(wr.mismatchCount, 0);
});

check("no_w_rome_if_hotelid_switch_in_steward_lib", () => {
  const src = fs.readFileSync(
    path.join(ROOT, "lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js"),
    "utf8"
  );
  assert.ok(!/hotelId\s*===\s*['\"]gdi_hotel_w_rome['\"]/.test(src));
  assert.ok(!/if\s*\(\s*['\"]Italy['\"]\s*\)/.test(src));
});

check("jev_cannot_set_canonical_fields_contract", () => {
  assert.ok(ADP_JEV_FORBIDDEN.includes("canonical_identity"));
  assert.ok(ADP_JEV_FORBIDDEN.includes("finding_truth"));
  assert.equal(describeAdpJevShadowAdapter().apply, false);
});

check("apply_gate_cleared_in_phase0_report", () => {
  const phase = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "reports/group-demand-intelligence/cross-market-replication-v1/PHASE0_2_SEED_AND_LOCAL_PLAN.json"
      ),
      "utf8"
    )
  );
  assert.equal(phase.phase0.censusStatus, "RESOLVED");
  assert.equal(phase.phase0.blockingForApply, false);
  assert.equal(phase.phase0.canonicalHotelId, CANONICAL);
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
