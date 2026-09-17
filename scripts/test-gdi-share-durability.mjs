#!/usr/bin/env node
/**
 * Unit/regression: share durability — previous keys, durable revoke, self-heal,
 * production assert, capabilities.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-share-durability-"));

process.env.GDI_SHARE_CAPABILITY_SECRET =
  process.env.GDI_SHARE_CAPABILITY_SECRET ||
  "unit-test-gdi-share-secret-min-32-chars-xx";
process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET = "0";
process.env.GDI_SHARE_REGISTRY_DIR = tmp;
process.env.GDI_SHARE_SIGNATURE_SELF_HEAL = "1";
delete process.env.NODE_ENV;
delete process.env.RAILWAY_ENVIRONMENT;

const mod = await import(
  "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js"
);

let failed = 0;
async function check(name, fn) {
  try {
    await fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

await check("issue_and_verify", async () => {
  const issued = mod.issueGdiShareCapability({
    hotelId: "recTESTHOTEL00001",
    label: "durability-test",
    expiresAt: "2099-12-31",
  });
  const v = mod.verifyGdiShareCapability(issued.token);
  assert.equal(v.ok, true);
  assert.ok(v.capabilities.includes(mod.GDI_SHARE_CAPABILITY.CAN_VALIDATE));
});

await check("previous_secret_verifies", async () => {
  const issued = mod.issueGdiShareCapability({
    hotelId: "recTESTHOTEL00002",
    label: "prev-key",
  });
  const oldSecret = process.env.GDI_SHARE_CAPABILITY_SECRET;
  process.env.GDI_SHARE_CAPABILITY_SECRET =
    "rotated-gdi-share-secret-min-32-chars-yy";
  process.env.GDI_SHARE_CAPABILITY_SECRET_PREVIOUS = oldSecret;
  // Re-import not needed — getGdiShareVerificationSecrets reads env each call
  const v = mod.verifyGdiShareCapability(issued.token);
  assert.equal(v.ok, true, v.code);
  process.env.GDI_SHARE_CAPABILITY_SECRET = oldSecret;
  delete process.env.GDI_SHARE_CAPABILITY_SECRET_PREVIOUS;
});

await check("durable_revoke_blocks_self_heal", async () => {
  const issued = mod.issueGdiShareCapability({
    hotelId: "recTESTHOTEL00003",
    label: "revoke-test",
  });
  mod.revokeGdiShareCapability(issued.tokenId, "unit_test");
  // Wipe registry row but keep durable revoke
  const regPath = path.join(tmp, "active-tokens.json");
  const reg = JSON.parse(fs.readFileSync(regPath, "utf8"));
  delete reg.tokens[issued.tokenId];
  fs.writeFileSync(regPath, JSON.stringify(reg, null, 2));
  const v = mod.verifyGdiShareCapability(issued.token, { allowSelfHeal: true });
  assert.equal(v.ok, false);
  assert.equal(v.code, "SHARE_REVOKED");
});

await check("self_heal_missing_active_row", async () => {
  const issued = mod.issueGdiShareCapability({
    hotelId: "recTESTHOTEL00004",
    label: "heal-test",
  });
  const regPath = path.join(tmp, "active-tokens.json");
  const reg = JSON.parse(fs.readFileSync(regPath, "utf8"));
  delete reg.tokens[issued.tokenId];
  fs.writeFileSync(regPath, JSON.stringify(reg, null, 2));
  const v = mod.verifyGdiShareCapability(issued.token, { allowSelfHeal: true });
  assert.equal(v.ok, true);
  assert.equal(v.healed, true);
});

await check("legacy_bethesda_capabilities", async () => {
  const caps = mod.resolveGdiShareCapabilities({
    surfaces: ["brief", "opportunities", "opportunity_detail", "summary"],
    mode: "read_only",
  });
  assert.ok(caps.includes(mod.GDI_SHARE_CAPABILITY.CAN_VALIDATE));
});

await check("production_assert_fails_without_secret", async () => {
  const prev = process.env.GDI_SHARE_CAPABILITY_SECRET;
  const prevNode = process.env.NODE_ENV;
  process.env.NODE_ENV = "production";
  delete process.env.GDI_SHARE_CAPABILITY_SECRET;
  process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET = "0";
  assert.throws(() => mod.assertGdiShareProductionConfig(), /GDI_SHARE_CAPABILITY_SECRET/);
  process.env.GDI_SHARE_CAPABILITY_SECRET = prev;
  process.env.NODE_ENV = prevNode;
});

await check("customer_message_no_internals", async () => {
  const msg = mod.customerMessageForShareCode("SHARE_BAD_SIGNATURE");
  assert.ok(/Dealality access link/i.test(msg));
  assert.ok(!/HMAC|registry|signature/i.test(msg));
});

fs.rmSync(tmp, { recursive: true, force: true });

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nPASS test-gdi-share-durability");
