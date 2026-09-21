#!/usr/bin/env node
/**
 * GDI Discovery Hygiene V3 — offline fixture + Wave 2 closure + regression gates.
 * Does not modify V11/V12 WHO tests. No provider calls.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  DISCOVERY_HYGIENE_V3,
  ACTIONABILITY_V3,
  qualifyOpportunityV3,
  applyDiscoveryHygieneV3,
  assertUnknownNotOpenRegression,
  classifyOpenSourcingEvidence,
  classifyOverflowThesis,
  classifyHotelDemandThesis,
  classifyEventSemanticType,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const SUBJECT = {
  hotelId: "recCEpdskZeUBvQwG",
  name: "Hotel Caribe by Faranda Grand, Cartagena",
};
const AS_OF = "2026-02-01";

check("unknown_never_maps_to_open", () => {
  const r = assertUnknownNotOpenRegression();
  assert.equal(r.ok, true, JSON.stringify(r));
});

check("offline_fixture_matrix", () => {
  const fix = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/evals/gdi-discovery-hygiene-v3-offline-fixtures.json"
      ),
      "utf8"
    )
  );
  assert.equal(fix.cases.length >= 11, true);
  for (const c of fix.cases) {
    const q = qualifyOpportunityV3(SUBJECT.hotelId, c.opp, {
      nowDate: AS_OF,
      subjectHotel: SUBJECT,
    });
    assert.equal(
      q.actionability,
      c.expect,
      `${c.id}: expected ${c.expect} got ${q.actionability} (${q.failureClass}) notes=${(q.notes || []).join("|")}`
    );
    if (c.failureClass) {
      assert.equal(
        q.failureClass,
        c.failureClass,
        `${c.id}: failureClass ${q.failureClass}`
      );
    }
    if (c.opportunityType && q.actionability === ACTIONABILITY_V3.TRUE_ACTIONABLE) {
      assert.equal(
        q.opportunityType,
        c.opportunityType,
        `${c.id}: opportunityType ${q.opportunityType}`
      );
    }
  }
});

check("sales_thesis_room_block_language_not_overflow", () => {
  const q = classifyOverflowThesis({
    title: "Health Business Summit",
    roomDemandStatus: "VERIFIED_HOUSING_PROGRAM",
    opportunityType: "OVERFLOW_HOUSING",
    whyNow: "allowing for planning and room block arrangements",
    hotelOpportunityThesis: "well-positioned to accommodate attendees",
    venueSourcingStatus: "UNKNOWN",
  });
  assert.equal(q.ok, false, JSON.stringify(q));
});

check("verified_housing_label_alone_not_strong_demand", () => {
  const d = classifyHotelDemandThesis({
    title: "Health Business Summit",
    roomDemandStatus: "VERIFIED_HOUSING_PROGRAM",
    whyNow: "room block arrangements",
    hotelOpportunityThesis: "likely needs rooms",
  });
  assert.notEqual(d.strength, "STRONG");
});

check("non_event_plan_semantic", () => {
  assert.equal(
    classifyEventSemanticType({
      title: "Action Plan San Pedro Garza García 2026-2027",
      semanticTypeHint: "PLAN",
    }),
    "PLAN"
  );
});

check("wave2_reprocess_precision_and_retention", () => {
  const freeze = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/evals/gdi-wave2-discovery-hygiene-v3.json"
      ),
      "utf8"
    )
  );
  assert.equal(freeze.version, DISCOVERY_HYGIENE_V3);
  assert.equal(freeze.metrics.discoveryPass, true);
  assert.equal(freeze.metrics.manualTrueRetained, 3);
  assert.equal(freeze.metrics.minPrecisionPct >= 90, true);
  for (const h of freeze.hotels) {
    assert.equal(h.precisionPct >= 90, true, h.name);
    assert.equal(h.falseActionable, 0, h.name);
  }
});

check("wave2_live_reprocess_matches_freeze", () => {
  const overlays = {};
  const fix = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/evals/gdi-wave2-discovery-v3-invalid-fixtures.json"
      ),
      "utf8"
    )
  );
  for (const row of [...fix.invalidFixtures, ...fix.validRetentionFixtures]) {
    overlays[row.opportunityId] = row.evidenceOverlay || {};
  }
  const hotels = [
    ["recESHsNsWUFYZrxR", "JW Marriott Hotel Santo Domingo"],
    ["recCEpdskZeUBvQwG", "Hotel Caribe by Faranda Grand, Cartagena"],
    ["recD17Kxn6BcJjGFh", "The Westin Monterrey Valle"],
  ];
  const manual = new Set([
    "gdi_opp_international_congress_of_radiology_icr_2026_4",
    "gdi_opp_weef_ifees_gedc_2026_0",
    "gdi_opp_xviii_international_symposium_on_biosafety_and_b_5",
  ]);
  let retained = 0;
  for (const [hotelId, name] of hotels) {
    const qualified = JSON.parse(
      fs.readFileSync(
        path.join(
          ROOT,
          `data/group-demand-intelligence/hotels/${hotelId}/wave2-qualified.json`
        ),
        "utf8"
      )
    );
    const result = applyDiscoveryHygieneV3(hotelId, qualified.opportunities || [], {
      nowDate: AS_OF,
      subjectHotel: { hotelId, name },
      evidenceOverlays: overlays,
    });
    const falseAmong = result.trueActionable.filter(
      (r) => !manual.has(r.id || r.opportunityId)
    );
    assert.equal(falseAmong.length, 0, `${name} false ${falseAmong.map((r) => r.id)}`);
    retained += result.trueActionable.filter((r) =>
      manual.has(r.id || r.opportunityId)
    ).length;
  }
  assert.equal(retained, 3);
});

check("prior_cohort_no_invalid_promotion_no_true_destruction", () => {
  const files = [
    "data/group-demand-intelligence/evals/now-now-noho-discovery-hygiene-v2.json",
    "data/group-demand-intelligence/evals/cambridge-beaches-discovery-hygiene-v2.json",
    "data/group-demand-intelligence/evals/jw-monterrey-discovery-hygiene-v2.json",
    "data/group-demand-intelligence/evals/st-regis-mexico-city-wave1-hygiene.json",
    "data/group-demand-intelligence/evals/st-regis-cap-cana-wave1-hygiene.json",
    "data/group-demand-intelligence/evals/hotel-phillips-kansas-city-wave1-hygiene.json",
  ];
  let newFalse = 0;
  let trueToInvalid = 0;
  for (const rel of files) {
    const j = JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
    const hotelId = j.hotel?.hotelId || "unknown";
    const name = j.hotel?.name || rel;
    const priorTrue = (j.rows || []).filter(
      (r) => r.manualActionable === "TRUE_ACTIONABLE" || r.hygieneState === "VALID_ACTIONABLE"
    );
    const priorInvalid = (j.rows || []).filter((r) => r.hygieneState === "INVALID");
    const rTrue = applyDiscoveryHygieneV3(hotelId, priorTrue, {
      nowDate: AS_OF,
      subjectHotel: { hotelId, name },
    });
    trueToInvalid += rTrue.rows.filter(
      (r) => r.actionabilityV3 === ACTIONABILITY_V3.INVALID
    ).length;
    const rInv = applyDiscoveryHygieneV3(hotelId, priorInvalid.slice(0, 40), {
      nowDate: AS_OF,
      subjectHotel: { hotelId, name },
    });
    newFalse += rInv.trueActionable.length;
  }
  assert.equal(newFalse, 0, `new false actionable from prior INVALID: ${newFalse}`);
  assert.equal(trueToInvalid, 0, `prior TRUE destroyed to INVALID: ${trueToInvalid}`);
});

check("open_sourcing_unknown_status", () => {
  const o = classifyOpenSourcingEvidence({ venueSourcingStatus: "UNKNOWN" });
  assert.equal(o.open, false);
  assert.equal(o.status, "UNKNOWN");
});

if (failed) {
  console.error(`\n${failed} failing checks`);
  process.exit(1);
}
console.log(`\nAll discovery-hygiene-v3 checks passed (${DISCOVERY_HYGIENE_V3})`);
