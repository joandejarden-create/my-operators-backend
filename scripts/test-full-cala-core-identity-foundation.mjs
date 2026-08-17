import test from "node:test";
import assert from "node:assert/strict";
import {
  HOLD_RESOLUTION,
  STATE_REGION_NOT_APPLICABLE,
  reconcileHoldLedger,
} from "../lib/research-engine-v2/full-cala-core-identity-foundation-closure-v1.js";
import { CENSUS_TABLE_ID } from "../lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js";

test("production table locked", () => {
  assert.equal(CENSUS_TABLE_ID, "tbl9aY5ijiuIzzWam");
});

test("Aruba is State/Region not applicable", () => {
  assert.ok(STATE_REGION_NOT_APPLICABLE.has("Aruba"));
  assert.ok(STATE_REGION_NOT_APPLICABLE.has("Bermuda"));
});

test("reconcileHoldLedger separates active from resolved", () => {
  const censusLookup = {
    byNameCountry: new Map([
      ["grand hotel|brazil", [{ id: "rec1" }]],
    ]),
    byHbx: new Map([[999, { id: "rec2" }]]),
  };
  const holds = {
    by_candidate_id: {
      a: { country: "Brazil", class: "weak_identity_hold", reason: "x" },
      b: { country: "Brazil", class: "non_hotel_reject", reason: "y" },
      c: { country: "Mexico", class: "weak_identity_hold", reason: "z" },
    },
  };
  const universe = [
    {
      candidate_id: "a",
      property_name: "Grand Hotel",
      country: "Brazil",
      external_ids: {},
    },
    {
      candidate_id: "c",
      property_name: "Unknown Inn",
      country: "Mexico",
      external_ids: { hbx_code: 999 },
    },
  ];
  const applied = { hbx_codes: [], candidate_ids: [] };
  const r = reconcileHoldLedger({ censusLookup, applied, universe, holds });
  assert.equal(r.tallies[HOLD_RESOLUTION.RESOLVED_EXISTING_MATCH] >= 1, true);
  assert.equal(r.tallies[HOLD_RESOLUTION.INVALID], 1);
  assert.ok(r.active_count >= 0);
  assert.ok(!r.active.b);
});
