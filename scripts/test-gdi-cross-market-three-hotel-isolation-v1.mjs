/**
 * Three-hotel share/auth isolation matrix — Bethesda, Renaissance, W Rome (canonical).
 */
import assert from "node:assert/strict";
import {
  issueGdiShareCapability,
  verifyGdiShareCapability,
  revokeGdiShareCapability,
} from "../lib/group-demand-intelligence/index.js";

const BETHESDA = "recLuxvwwxID7U2B8";
const RENAISSANCE = "recG66DQJKP2c0UNh";
const W_ROME = "rece0or38cxo3Fymb";
const HOTELS = [BETHESDA, RENAISSANCE, W_ROME];

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
  issued[h] = issueGdiShareCapability({ hotelId: h, label: `cross-market-${h}` });
}

check("each_hotel_token_verifies_for_self", () => {
  for (const h of HOTELS) {
    const ok = verifyGdiShareCapability(issued[h].token, {
      expectedHotelId: h,
      requiredSurface: "brief",
    });
    assert.equal(ok.ok, true, `${h} should verify`);
  }
});

check("bethesda_token_cannot_fetch_renaissance_or_w_rome", () => {
  assert.equal(
    verifyGdiShareCapability(issued[BETHESDA].token, { expectedHotelId: RENAISSANCE }).ok,
    false
  );
  assert.equal(
    verifyGdiShareCapability(issued[BETHESDA].token, { expectedHotelId: W_ROME }).ok,
    false
  );
});

check("renaissance_token_cannot_fetch_bethesda_or_w_rome", () => {
  assert.equal(
    verifyGdiShareCapability(issued[RENAISSANCE].token, { expectedHotelId: BETHESDA }).ok,
    false
  );
  assert.equal(
    verifyGdiShareCapability(issued[RENAISSANCE].token, { expectedHotelId: W_ROME }).ok,
    false
  );
});

check("w_rome_token_cannot_fetch_bethesda_or_renaissance", () => {
  assert.equal(
    verifyGdiShareCapability(issued[W_ROME].token, { expectedHotelId: BETHESDA }).ok,
    false
  );
  assert.equal(
    verifyGdiShareCapability(issued[W_ROME].token, { expectedHotelId: RENAISSANCE }).ok,
    false
  );
});

check("tokens_are_not_reused_across_hotels", () => {
  const tokens = HOTELS.map((h) => issued[h].token);
  assert.equal(new Set(tokens).size, 3);
  const ids = HOTELS.map((h) => issued[h].tokenId);
  assert.equal(new Set(ids).size, 3);
});

for (const h of HOTELS) {
  revokeGdiShareCapability(issued[h].tokenId, "cross-market-test");
}

check("revoked_tokens_fail", () => {
  for (const h of HOTELS) {
    assert.equal(verifyGdiShareCapability(issued[h].token).ok, false);
  }
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
