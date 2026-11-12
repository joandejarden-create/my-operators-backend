#!/usr/bin/env node
/**
 * Regression: Sales Ready must not pass with thin / unconfirmed hotel fit.
 * Bethesda false-eligibility cases + hotel-agnostic synthetic fixtures.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assessSalesReadyHotelFitGate,
  assessAccountDevelopmentHotelFitGate,
  HOTEL_FIT_GATE_REASON,
} from "../lib/group-demand-intelligence/sales-ready-hotel-fit-gate-v1.js";
import {
  loadHotelOpportunityDoc,
  BETHESDA_HOTEL_ID,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ALIGNMENT_SUMMARY = path.join(
  ROOT,
  "reports/gdi-bethesda-maturity-alignment-approval-v1/00-alignment-summary.json",
);
const ALIGNMENT_MANIFEST = path.join(
  ROOT,
  "reports/gdi-bethesda-maturity-alignment-approval-v1/01-corrected-maturity-approval-manifest.json",
);

function loadUnionOpp(id) {
  const doc = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID);
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    doc.opportunities || [],
  );
  return union.opportunities.find((o) => o.id === id);
}

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}:`, err.message || err);
  }
}

check("loudoun_hotel_fit_blocks_sales_ready", () => {
  const o = loadUnionOpp("gdi_opp_loudoun_college_showcase_2027");
  assert.ok(o, "loudoun present");
  const gate = assessSalesReadyHotelFitGate(o);
  assert.equal(gate.ok, false);
  assert.equal(gate.reason, HOTEL_FIT_GATE_REASON.CONDITIONAL_STRETCH);
  assert.equal(assessAccountDevelopmentHotelFitGate(o).ok, false);
});

check("nado_2028_hotel_fit_blocks_sales_ready", () => {
  const o = loadUnionOpp("gdi_opp_nado_ddaa_washcon_2028");
  assert.ok(o, "nado present");
  const gate = assessSalesReadyHotelFitGate(o);
  assert.equal(gate.ok, false);
  assert.equal(gate.reason, HOTEL_FIT_GATE_REASON.HOST_UNCONFIRMED);
  assert.equal(assessAccountDevelopmentHotelFitGate(o).ok, false);
});

check("synthetic_destination_unconfirmed_blocks_sr", () => {
  const gate = assessSalesReadyHotelFitGate({
    title: "Future association meeting",
    hotelFitHonesty: "DESTINATION_UNCONFIRMED",
    hotelFitNote: "Host hotel not yet named",
  });
  assert.equal(gate.ok, false);
  assert.equal(gate.reason, HOTEL_FIT_GATE_REASON.HOST_UNCONFIRMED);
});

check("synthetic_out_of_market_blocks_sr", () => {
  const gate = assessSalesReadyHotelFitGate({
    hotelFitHonesty: "OUT_OF_MARKET_HOST_RESEARCH_GATED",
    hotelFitNote: "Salt Lake City host cycle",
  });
  assert.equal(gate.ok, false);
  assert.equal(gate.reason, HOTEL_FIT_GATE_REASON.DESTINATION_OUTSIDE);
});

check("partnership_watch_not_ad", () => {
  const gate = assessAccountDevelopmentHotelFitGate({
    commercialMaturity: "PARTNERSHIP_WATCH",
    title: "Planner partnership",
  });
  assert.equal(gate.ok, false);
});

check("alignment_report_engine_counts", () => {
  assert.ok(
    fs.existsSync(ALIGNMENT_SUMMARY),
    "run npm run gdi:bethesda-maturity-alignment-approval first",
  );
  const summary = JSON.parse(fs.readFileSync(ALIGNMENT_SUMMARY, "utf8"));
  assert.equal(summary.engineAfterCorrection.salesReadyEligible, 0);
  assert.equal(summary.engineAfterCorrection.accountDevelopmentEligible, 14);
  assert.equal(summary.actionableNowCount, 3);
  assert.equal(summary.writeApplied, false);
  assert.equal(summary.publicationAuthorized, false);
});

check("alignment_manifest_rejects_loudoun_nado_sr", () => {
  const pkg = JSON.parse(fs.readFileSync(ALIGNMENT_MANIFEST, "utf8"));
  assert.ok(
    pkg.rejectedPromotions.some(
      (r) =>
        r.id === "gdi_opp_loudoun_college_showcase_2027" &&
        r.decision === "REJECT_SALES_READY_PROMOTION",
    ),
  );
  assert.ok(
    pkg.rejectedPromotions.some(
      (r) =>
        r.id === "gdi_opp_nado_ddaa_washcon_2028" &&
        r.decision === "REJECT_SALES_READY_PROMOTION",
    ),
  );
  for (const p of pkg.proposedPromotions) {
    assert.equal(p.field, "commercialMaturity");
    assert.equal(p.to, "ACCOUNT_DEVELOPMENT");
    assert.ok("priorityFieldUnchanged" in p);
    assert.equal(p.publicationReady, false);
    assert.equal(p.salesReadyImplied, false);
  }
});

try {
  const { evaluateSharedCommercialEligibility } = await import(
    "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js"
  );
  const { evaluateAccountDevelopmentEligibility } = await import(
    "../lib/group-demand-intelligence/account-development-eligibility-v1.js"
  );
  check("eligibility_dynamic_loudoun_nado_not_sales_ready", () => {
    for (const id of [
      "gdi_opp_loudoun_college_showcase_2027",
      "gdi_opp_nado_ddaa_washcon_2028",
    ]) {
      const o = loadUnionOpp(id);
      assert.equal(evaluateSharedCommercialEligibility(o).salesReady, false, id);
      assert.equal(
        evaluateAccountDevelopmentEligibility(o).accountDevelopment,
        false,
        id,
      );
    }
  });
} catch (err) {
  failed += 1;
  console.error("FAIL eligibility_dynamic_import:", err.message || err);
}

if (failed) {
  console.error(`\n${failed} gate(s) failed`);
  process.exit(1);
}
console.log("\nAll sales-ready hotel-fit / maturity alignment gates passed.");
