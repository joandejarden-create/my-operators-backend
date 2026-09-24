/**
 * Replication gate repairs V1 — regression tests (no live Airtable / Surfe / Jev apply).
 */
import assert from "node:assert/strict";
import {
  resolveAdpCanonicalBaseId,
  resolveGdiCanonicalBaseId,
  reportCanonicalBaseEnv,
  classifyBaseConfig,
  CANONICAL_INTELLIGENCE_BASE_ID,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
} from "../lib/group-demand-intelligence/canonical-airtable-base.js";
import { getAdpPersistencePolicy } from "../lib/ai-demand-positioning/adp-persistence-policy.js";
import {
  isPilotReferenceLogicEnabled,
  isPilotReferenceHotel,
} from "../lib/group-demand-intelligence/pilot-path-policy.js";
import {
  proposeHotelOnboardSeed,
  compareOnboardSeedProposals,
  assessWeeklyReadinessFromSeed,
} from "../lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js";
import { textHasPilotGeoBleed } from "../lib/group-demand-intelligence/research-coverage/portable-seed-templates.js";
import { describeAdpJevShadowIntegrationPoints } from "../lib/group-demand-intelligence/research-coverage/seed-jev-shadow.js";
import { getGdiOpportunitiesAirtableBaseId } from "../lib/decision-outcomes/airtable-base.js";

const RENAISSANCE = "recG66DQJKP2c0UNh";
const BETHESDA = "recLuxvwwxID7U2B8";
const NOW = "2026-09-24T12:00:00.000Z";

let passed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}: ${err.message}`);
    process.exitCode = 1;
  }
}

check("adp_accepts_intelligence_canonical_chain", () => {
  const r = resolveAdpCanonicalBaseId({
    ADP_AIRTABLE_BASE_ID: "",
    AIRTABLE_INTELLIGENCE_BASE_ID: CANONICAL_INTELLIGENCE_BASE_ID,
    AIRTABLE_BASE_ID: "appSOMEOTHER",
  });
  assert.equal(r.baseId, CANONICAL_INTELLIGENCE_BASE_ID);
  assert.equal(r.source, "AIRTABLE_INTELLIGENCE_BASE_ID");
});

check("adp_fail_closed_missing_explicit", () => {
  assert.throws(
    () =>
      resolveAdpCanonicalBaseId({
        ADP_AIRTABLE_BASE_ID: "",
        AIRTABLE_INTELLIGENCE_BASE_ID: "",
        AIRTABLE_GDI_BASE_ID: "",
        AIRTABLE_BASE_ID: "appSOMEOTHER",
      }),
    /ADP_CANONICAL_BASE_MISSING/
  );
});

check("adp_accepts_verified_canonical_fallback", () => {
  const r = resolveAdpCanonicalBaseId({
    ADP_AIRTABLE_BASE_ID: "",
    AIRTABLE_BASE_ID: CANONICAL_INTELLIGENCE_BASE_ID,
  });
  assert.equal(r.baseId, CANONICAL_INTELLIGENCE_BASE_ID);
  assert.equal(r.status, "SET_CANONICAL");
});

check("adp_explicit_canonical", () => {
  const r = resolveAdpCanonicalBaseId({
    ADP_AIRTABLE_BASE_ID: CANONICAL_INTELLIGENCE_BASE_ID,
  });
  assert.equal(r.status, "SET_CANONICAL");
});

check("gdi_fail_closed_noncanonical_fallback", () => {
  assert.throws(
    () =>
      resolveGdiCanonicalBaseId({
        AIRTABLE_GDI_BASE_ID: "",
        GDI_OPPORTUNITIES_AIRTABLE_BASE_ID: "",
        AIRTABLE_INTELLIGENCE_BASE_ID: "",
        ADP_AIRTABLE_BASE_ID: "",
        AIRTABLE_BASE_ID: "appSOMEOTHER",
      }),
    /GDI_CANONICAL_BASE_MISSING/
  );
});

check("gdi_loose_getter_rejects_noncanonical_silent_fallback", () => {
  const prev = { ...process.env };
  try {
    delete process.env.AIRTABLE_GDI_BASE_ID;
    delete process.env.GDI_OPPORTUNITIES_AIRTABLE_BASE_ID;
    delete process.env.AIRTABLE_INTELLIGENCE_BASE_ID;
    delete process.env.AIRTABLE_DECISION_OUTCOME_BASE_ID;
    delete process.env.DECISION_OUTCOMES_AIRTABLE_BASE_ID;
    delete process.env.ADP_AIRTABLE_BASE_ID;
    delete process.env.GDI_ALLOW_GENERIC_AIRTABLE_BASE_FALLBACK;
    process.env.AIRTABLE_BASE_ID = "appSOMEOTHER";
    assert.equal(getGdiOpportunitiesAirtableBaseId(), "");
  } finally {
    Object.assign(process.env, prev);
  }
});

check("legacy_mvp_write_rejected", () => {
  assert.throws(
    () =>
      assertNotLegacyMvpCanonicalBase(LEGACY_DEAL_CAPTURE_MVP_BASE_ID, {
        surface: "test",
      }),
    /WRONG_CANONICAL_AIRTABLE_BASE/
  );
});

check("classify_base_config", () => {
  assert.equal(classifyBaseConfig(CANONICAL_INTELLIGENCE_BASE_ID), "SET_CANONICAL");
  assert.equal(classifyBaseConfig(""), "MISSING");
  assert.equal(classifyBaseConfig("appX"), "SET_NONCANONICAL");
});

check("adp_persistence_policy_filesystem_primary", () => {
  const p = getAdpPersistencePolicy({ ADP_HISTORY_WRITES_ENABLED: "" });
  assert.equal(p.PRIMARY_SOT, "FILESYSTEM_SNAPSHOT");
  assert.equal(p.LIVE_OVERLAY, "AIRTABLE_PUBLISHED_REPORTS");
  assert.equal(p.HISTORY_AIRTABLE, "DISABLED");
  assert.equal(p.secondHotelHistoryRequired, false);
});

check("pilot_logic_disabled_for_renaissance_by_default", () => {
  assert.equal(isPilotReferenceHotel(RENAISSANCE), false);
  assert.equal(isPilotReferenceLogicEnabled(RENAISSANCE), false);
});

check("pilot_logic_available_for_bethesda_reference", () => {
  assert.equal(isPilotReferenceHotel(BETHESDA), true);
  assert.equal(isPilotReferenceLogicEnabled(BETHESDA), true);
});

check("generic_fit_and_target_seed_renaissance", () => {
  const p = proposeHotelOnboardSeed(RENAISSANCE, { now: NOW });
  assert.ok(p.fits.length >= 6, `expected fits, got ${p.fits.length}`);
  assert.ok(p.targets.length >= 6, `expected targets, got ${p.targets.length}`);
  assert.equal(p.pilotLogicEnabled, false);
  assert.equal(p.quality.baselineCreatesWeeklyNew, false);
  assert.equal(p.quality.hotelNameSwitches, false);
  for (const f of p.fits) {
    assert.equal(f.hotelId, RENAISSANCE);
    assert.equal(textHasPilotGeoBleed(f.fitRationale), false);
  }
  for (const t of p.targets) {
    assert.equal(t.hotelId, RENAISSANCE);
    assert.ok(t.targetId);
    assert.ok(t.targetType);
    assert.ok(t.status);
    assert.notEqual(t.status, "NEW");
  }
});

check("bethesda_logic_not_invoked_in_renaissance_seed", () => {
  const p = proposeHotelOnboardSeed(RENAISSANCE, { now: NOW });
  const blob = JSON.stringify(p.generators) + JSON.stringify(p.fits);
  assert.equal(textHasPilotGeoBleed(blob), false);
  assert.ok(!blob.toLowerCase().includes("nih.gov"));
  assert.ok(!blob.toLowerCase().includes("pooks hill"));
});

check("deterministic_recreate", () => {
  const a = proposeHotelOnboardSeed(RENAISSANCE, { now: NOW });
  const b = proposeHotelOnboardSeed(RENAISSANCE, { now: NOW });
  const c = compareOnboardSeedProposals(a, b);
  assert.equal(c.deterministic, true);
  assert.equal(c.fitOverlapPct, 100);
  assert.equal(c.targetOverlapPct, 100);
  assert.equal(c.manualHotelSpecificInjection, false);
});

check("weekly_ready_structurally", () => {
  const p = proposeHotelOnboardSeed(RENAISSANCE, { now: NOW });
  const w = assessWeeklyReadinessFromSeed(p);
  assert.equal(w.status, "WEEKLY_READY");
});

check("jev_shadow_cannot_affect_seed_output_contract", () => {
  const before = proposeHotelOnboardSeed(RENAISSANCE, { now: NOW });
  const prep = describeAdpJevShadowIntegrationPoints();
  assert.equal(prep.productionAdpBehaviorChanged, false);
  const after = proposeHotelOnboardSeed(RENAISSANCE, { now: NOW });
  assert.deepEqual(
    before.fits.map((f) => f.fitId),
    after.fits.map((f) => f.fitId)
  );
});

check("env_report_shape", () => {
  const r = reportCanonicalBaseEnv({
    ADP_AIRTABLE_BASE_ID: CANONICAL_INTELLIGENCE_BASE_ID,
    AIRTABLE_GDI_BASE_ID: CANONICAL_INTELLIGENCE_BASE_ID,
  });
  assert.equal(r.adpResolve.ok, true);
  assert.equal(r.gdiResolve.ok, true);
});

console.log(`\n${passed} checks passed`);
