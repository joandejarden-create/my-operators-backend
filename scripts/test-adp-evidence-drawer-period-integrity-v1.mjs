#!/usr/bin/env node
/**
 * test:adp-evidence-drawer-period-integrity-v1
 */
import assert from "assert";
import { getPublishedEvidenceResponse } from "../lib/ai-demand-positioning/published-read-service.js";
import { loadLatestCustomerPeriod } from "../lib/ai-demand-positioning/data-model.js";

const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE_ID = "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";

const customer = loadLatestCustomerPeriod(PROPERTY_ID);
assert.equal(customer?.periodId, CORE_ID);

const missing = await getPublishedEvidenceResponse(PROPERTY_ID, {
  type: "missing",
  intent: "business",
  limit: 100,
});
assert.equal(missing.ok, true);
assert.equal(missing.periodId, CORE_ID);
assert.equal(missing.total, 16);
assert.equal(missing.evidence?.length, 16);

const present = await getPublishedEvidenceResponse(PROPERTY_ID, {
  type: "present",
  intent: "business",
  limit: 5,
});
assert.equal(present.ok, true);
assert.equal(present.periodId, CORE_ID);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-evidence-drawer-period-integrity-v1",
      businessMissing: missing.total,
      periodId: missing.periodId,
    },
    null,
    2
  )
);
