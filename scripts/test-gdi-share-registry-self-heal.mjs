#!/usr/bin/env node
/**
 * GDI share registry self-heal — valid HMAC tokens reseed missing registry rows
 * so Railway redeploys do not orphan client share URLs.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-share-heal-"));
process.env.GDI_SHARE_REGISTRY_DIR = tmp;
process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET = "1";
delete process.env.GDI_SHARE_SIGNATURE_SELF_HEAL;

const {
  issueGdiShareCapability,
  verifyGdiShareCapability,
  revokeGdiShareCapability,
  readGdiShareRegistry,
} = await import(
  "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js"
);

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

check("missing_registry_row_self_heals_on_verify", () => {
  const issued = issueGdiShareCapability({
    hotelId: "recLuxvwwxID7U2B8",
    label: "self-heal-test",
    expiresAt: "2099-12-31",
  });
  const regPath = path.join(tmp, "active-tokens.json");
  const reg = JSON.parse(fs.readFileSync(regPath, "utf8"));
  delete reg.tokens[issued.tokenId];
  fs.writeFileSync(regPath, JSON.stringify(reg, null, 2));
  assert.equal(readGdiShareRegistry().tokens[issued.tokenId], undefined);

  const verified = verifyGdiShareCapability(issued.token);
  assert.equal(verified.ok, true);
  assert.equal(verified.healed, true);
  assert.equal(verified.claims.tid, issued.tokenId);
  assert.equal(readGdiShareRegistry().tokens[issued.tokenId].status, "ACTIVE");
});

check("revoked_row_is_not_resurrected", () => {
  const issued = issueGdiShareCapability({
    hotelId: "recLuxvwwxID7U2B8",
    label: "revoke-test",
  });
  revokeGdiShareCapability(issued.tokenId, "test");
  const verified = verifyGdiShareCapability(issued.token);
  assert.equal(verified.ok, false);
  assert.equal(verified.code, "SHARE_REVOKED");
});

check("self_heal_can_be_disabled", () => {
  process.env.GDI_SHARE_SIGNATURE_SELF_HEAL = "0";
  const issued = issueGdiShareCapability({
    hotelId: "recLuxvwwxID7U2B8",
    label: "no-heal",
  });
  const regPath = path.join(tmp, "active-tokens.json");
  const reg = JSON.parse(fs.readFileSync(regPath, "utf8"));
  delete reg.tokens[issued.tokenId];
  fs.writeFileSync(regPath, JSON.stringify(reg, null, 2));
  const verified = verifyGdiShareCapability(issued.token);
  assert.equal(verified.ok, false);
  assert.equal(verified.code, "SHARE_UNKNOWN");
  delete process.env.GDI_SHARE_SIGNATURE_SELF_HEAL;
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI share self-heal checks passed.");
