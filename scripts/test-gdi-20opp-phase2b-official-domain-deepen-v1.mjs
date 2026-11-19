#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  PHASE2B_CASES,
  PHASE2B_ID,
  MAX_PAID_USD,
  assertZeroPaidProviders,
} from "../lib/group-demand-intelligence/experiments/gdi-20opp-phase2b-official-domain-deepen-v1.js";
import { DENIED_METERED_PROVIDERS } from "../lib/group-demand-intelligence/adaptive-public-research-controller-v1.js";

const billing = assertZeroPaidProviders();
assert.equal(PHASE2B_CASES.length, 2);
assert.equal(MAX_PAID_USD, 0);
assert.equal(billing.ok, true);
assert.equal(billing.publicFetchCostUsd, 0);
assert.ok(DENIED_METERED_PROVIDERS.includes("exa"));
assert.deepEqual(
  PHASE2B_CASES.map((c) => c.opportunityId).sort(),
  ["gdi_lmc_00562bf81b1e1957", "gdi_opp_2027_ddaa_annual_conference_8"].sort(),
);
assert.equal(PHASE2B_ID, "gdi_20opp_phase2b_official_domain_deepen_v1");

console.log(
  JSON.stringify(
    {
      ok: true,
      cases: PHASE2B_CASES.map((c) => c.opportunityId),
      maxPaidUsd: MAX_PAID_USD,
      billing,
    },
    null,
    2,
  ),
);
