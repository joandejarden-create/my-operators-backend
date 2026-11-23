#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildGlobalSecCoverageGapDossiers,
  APLE_MARRIOTT_EXIT_JAN2026,
  RHP_HMA_ROWS,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-sec-coverage-gap-dossiers-v1.js";
import { contractPersistAllowed } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";

const d = buildGlobalSecCoverageGapDossiers();
assert.equal(APLE_MARRIOTT_EXIT_JAN2026.length, 9);
assert.equal(RHP_HMA_ROWS.length, 9);
assert.equal(d.length, 19); // 9 APLE transitions + 1 portfolio + 9 RHP

const aple = d.filter((x) => x.findingId.includes("aple_") && x.findingId.includes("marriott_exit"));
assert.equal(aple.length, 9);
for (const x of aple) {
  assert.equal(x.agreementType, "MANAGEMENT");
  assert.equal(x.contractStatus, "TERMINATED_VERIFIED");
  assert.equal(x.milestoneDate, null);
  assert.equal(contractPersistAllowed(x), true);
}

const desert = d.find((x) => x.hotelName.includes("Desert Ridge"));
assert.ok(desert);
assert.equal(desert.expirationYear, 2027);
assert.equal(desert.expirationDate, null);
assert.equal(desert.milestoneDate, null);
assert.equal(desert.datePrecision, "YEAR_ONLY");
assert.equal(desert.trackA, "DECISION_WINDOW_YEAR_ONLY_WITH_AUTO_RENEWAL");
assert.equal(contractPersistAllowed(desert), true);

const portfolio = d.find((x) => x.findingId.includes("variable_hma"));
assert.equal(portfolio.persistEligible, false);
assert.equal(contractPersistAllowed(portfolio), false);

const ids = new Set(d.map((x) => x.findingId));
assert.equal(ids.size, d.length);

const persistable = d.filter((x) => contractPersistAllowed(x));
assert.equal(persistable.length, 18);

console.log(
  JSON.stringify(
    {
      ok: true,
      dossiers: d.length,
      persistEligible: persistable.length,
      apleTransitions: aple.length,
      rhpNearTerm: d.filter((x) => x.trackA === "DECISION_WINDOW_YEAR_ONLY_WITH_AUTO_RENEWAL").length,
    },
    null,
    2,
  ),
);
