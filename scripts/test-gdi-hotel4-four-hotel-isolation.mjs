#!/usr/bin/env node
/**
 * Four-hotel isolation — Bethesda, Renaissance, W Rome, Hilton Times Square.
 */
import assert from "node:assert/strict";
import {
  issueGdiShareCapability,
  verifyGdiShareCapability,
  revokeGdiShareCapability,
} from "../lib/group-demand-intelligence/index.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";

const HOTELS = [
  { id: "recLuxvwwxID7U2B8", label: "Bethesda" },
  { id: "recG66DQJKP2c0UNh", label: "Renaissance" },
  { id: "rece0or38cxo3Fymb", label: "WRome" },
  { id: "rec35fExUxCClpOP6", label: "HiltonTS" },
];

process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET = "1";

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

const issued = {};
for (const h of HOTELS) {
  issued[h.id] = issueGdiShareCapability({ hotelId: h.id, label: `hotel4-iso-${h.label}` });
}

check("tokens_unique", () => {
  assert.equal(new Set(HOTELS.map((h) => issued[h.id].token)).size, 4);
});

check("self_verify", () => {
  for (const h of HOTELS) {
    assert.equal(
      verifyGdiShareCapability(issued[h.id].token, {
        expectedHotelId: h.id,
        requiredSurface: "brief",
      }).ok,
      true
    );
  }
});

check("cross_hotel_token_blocked", () => {
  for (const a of HOTELS) {
    for (const b of HOTELS) {
      if (a.id === b.id) continue;
      assert.equal(
        verifyGdiShareCapability(issued[a.id].token, { expectedHotelId: b.id }).ok,
        false,
        `${a.label}->${b.label}`
      );
    }
  }
});

check("configs_bound_to_distinct_hpc", () => {
  const census = new Set();
  for (const h of HOTELS) {
    const cfg = loadHotelDemandConfig(h.id);
    assert.ok(cfg, `missing config ${h.id}`);
    const cid = cfg.aliases?.censusRecordId || h.id;
    assert.ok(!census.has(cid), `duplicate census ${cid}`);
    census.add(cid);
  }
  assert.equal(census.size, 4);
});

const bags = {};
for (const h of HOTELS) {
  bags[h.id] = await loadOpportunitiesCanonical(h.id);
}

check("opportunity_hotelId_isolation", () => {
  let leaks = 0;
  for (const h of HOTELS) {
    for (const o of bags[h.id].opportunities || []) {
      const oid = o.hotelId || o.hotel_id;
      if (oid && oid !== h.id) {
        leaks += 1;
        console.error(`LEAK opp ${o.id} hotelId=${oid} bag=${h.id}`);
      }
    }
  }
  assert.equal(leaks, 0);
});

check("no_cross_bag_opportunity_id_collision_across_hotels", () => {
  const seen = new Map();
  let collisions = 0;
  for (const h of HOTELS) {
    for (const o of bags[h.id].opportunities || []) {
      if (!o.id) continue;
      if (seen.has(o.id) && seen.get(o.id) !== h.id) {
        collisions += 1;
        console.error(`ID collision ${o.id} in ${seen.get(o.id)} and ${h.id}`);
      }
      seen.set(o.id, h.id);
    }
  }
  assert.equal(collisions, 0);
});

check("hilton_bag_has_no_bethesda_nih_bleed", () => {
  const blob = JSON.stringify(bags["rec35fExUxCClpOP6"].opportunities || []).toLowerCase();
  assert.equal(/\bnatcher\b|\bpooks hill\b|\bwalter reed\b/.test(blob), false);
});

for (const h of HOTELS) {
  revokeGdiShareCapability(issued[h.id].tokenId, "hotel4-iso");
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
