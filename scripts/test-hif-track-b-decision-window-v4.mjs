#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildDecisionWindowV4Dossiers,
  summarizeWindowClasses,
  WINDOW_CLASS,
  DRH_NEAR_FRANCHISE,
  DRH_NEAR_HMA,
  DRH_HISTORICAL_HIGHGATE,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-decision-window-v4-dossiers.js";
import { contractPersistAllowed } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";

const d = buildDecisionWindowV4Dossiers();
assert.equal(DRH_NEAR_FRANCHISE.length, 2);
assert.equal(DRH_NEAR_HMA.length, 6);
assert.equal(DRH_HISTORICAL_HIGHGATE.length, 3);

const westin = d.find((x) => x.findingId.includes("westin_boston") && x.agreementType === "FRANCHISE");
assert.ok(westin);
assert.equal(westin.expirationYear, 2026);
assert.equal(westin.expirationMonth, 12);
assert.equal(westin.expirationDate, null);
assert.equal(westin.milestoneDate, null);
assert.equal(westin.windowClass, WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW);

const desert = d.find((x) => x.findingId.includes("desert_ridge"));
assert.equal(desert.persistEligible, false);
assert.equal(desert.windowClass, WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW);
assert.ok(desert.trackAPayload.priorTrackBContractId.includes("tb_gsca_rhp"));

const aple = d.find((x) => x.findingId.includes("aple_flexibility"));
assert.equal(aple.persistEligible, false);

const eCount = d.filter((x) => x.windowClass === WINDOW_CLASS.E_VERIFIED_OPEN_OPPORTUNITY).length;
assert.equal(eCount, 0);

const auditLeak = d.filter((x) => String(x.findingId || "").includes("tb_gsca_") || String(x.contractId || "").includes("tb_gsca_"));
assert.equal(auditLeak.length, 0);

const persistable = d.filter((x) => contractPersistAllowed(x));
assert.ok(persistable.length >= 10);
for (const x of persistable) {
  assert.ok(String(x.findingId).includes("tb_v4_"));
  if (x.datePrecision === "MONTH" || x.datePrecision === "YEAR_ONLY") {
    assert.equal(x.expirationDate, null);
  }
}

const ids = new Set(d.map((x) => x.findingId));
assert.equal(ids.size, d.length);

const byClass = summarizeWindowClasses(d);
console.log(
  JSON.stringify(
    {
      ok: true,
      dossiers: d.length,
      persistEligible: persistable.length,
      windowClasses: byClass,
      verifiedOpenE: eCount,
    },
    null,
    2,
  ),
);
