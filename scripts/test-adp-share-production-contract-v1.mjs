#!/usr/bin/env node
import assert from "node:assert";
import { createHmac } from "node:crypto";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const secret = "production-share-contract-test-secret-32";
process.env.ADP_SHARE_REGISTRY_DIR = mkdtempSync(join(tmpdir(), "adp-share-"));
process.env.ADP_SHARE_CAPABILITY_SECRET = secret;
delete process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET;

const {
  issueShareCapability,
  verifyShareCapability,
  revokeShareCapability,
  readShareRegistry,
  productionShareSigningAllowed,
  evaluateExternalShareReadiness,
  EXTERNAL_SHARE_STATUS,
  ADP_SHARE_DEV_FALLBACK_SECRET,
  ADP_SHARE_TOKEN_PREFIX,
} = await import("../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js");

function b64url(buf) {
  return Buffer.from(buf).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

const issued = issueShareCapability({
  propertyId: "adp_wyndham_san_diego_bayside",
  reportScope: "current_published",
});
const ok = verifyShareCapability(issued.token, { requiredSurface: "report" });
assert.strictEqual(ok.ok, true);
assert.strictEqual(ok.claims.propertyId, "adp_wyndham_san_diego_bayside");
assert.strictEqual(ok.claims.exp, null);

assert.ok(issued.token.startsWith(ADP_SHARE_TOKEN_PREFIX));
const raw = issued.token.slice(ADP_SHARE_TOKEN_PREFIX.length);
const [body, sig] = raw.split(".");

const flippedBody = `${body.slice(0, -1)}${body.endsWith("a") ? "b" : "a"}`;
const tamperedPayload = verifyShareCapability(`${ADP_SHARE_TOKEN_PREFIX}${flippedBody}.${sig}`);
assert.strictEqual(tamperedPayload.ok, false);
assert.strictEqual(tamperedPayload.code, "SHARE_BAD_SIGNATURE");

const flippedSig = `${sig.slice(0, -1)}${sig.endsWith("a") ? "b" : "a"}`;
const tamperedSig = verifyShareCapability(`${ADP_SHARE_TOKEN_PREFIX}${body}.${flippedSig}`);
assert.strictEqual(tamperedSig.ok, false);
assert.strictEqual(tamperedSig.code, "SHARE_BAD_SIGNATURE");

const wrongProperty = verifyShareCapability(issued.token, {
  expectedPropertyId: "adp_mayflower_hotel_washington_dc",
  requiredSurface: "report",
});
assert.strictEqual(wrongProperty.ok, false);
assert.strictEqual(wrongProperty.code, "SHARE_PROPERTY_SCOPE");

const missingSig = verifyShareCapability(`${ADP_SHARE_TOKEN_PREFIX}${body}`);
assert.strictEqual(missingSig.ok, false);
assert.strictEqual(missingSig.code, "SHARE_MALFORMED");

const unsigned = verifyShareCapability("");
assert.strictEqual(unsigned.ok, false);
assert.strictEqual(unsigned.code, "SHARE_MISSING");

const versionPayload = { ...ok.claims, v: 2 };
const versionBody = b64url(Buffer.from(JSON.stringify(versionPayload), "utf8"));
const versionSig = b64url(createHmac("sha256", secret).update(versionBody).digest());
const wrongVersion = verifyShareCapability(`${ADP_SHARE_TOKEN_PREFIX}${versionBody}.${versionSig}`);
assert.strictEqual(wrongVersion.ok, false);
assert.strictEqual(wrongVersion.code, "SHARE_VERSION");

const admin = verifyShareCapability(issued.token, { requiredSurface: "admin" });
assert.strictEqual(admin.ok, false);
assert.strictEqual(admin.code, "SHARE_SURFACE");

const expired = issueShareCapability({
  propertyId: "adp_wyndham_san_diego_bayside",
  expiresAt: new Date(Date.now() - 60_000).toISOString(),
});
const expiredCheck = verifyShareCapability(expired.token);
assert.strictEqual(expiredCheck.ok, false);
assert.strictEqual(expiredCheck.code, "SHARE_EXPIRED");

const registryFile = join(process.env.ADP_SHARE_REGISTRY_DIR, "active-tokens.json");
const missingRow = issueShareCapability({
  propertyId: "adp_westin_tampa_waterside",
  reportScope: "current_published",
});
const regDoc = JSON.parse(readFileSync(registryFile, "utf8"));
delete regDoc.tokens[missingRow.tokenId];
writeFileSync(registryFile, JSON.stringify(regDoc));
const healed = verifyShareCapability(missingRow.token, { requiredSurface: "report" });
assert.strictEqual(healed.ok, true, "valid signature must reseed a missing registry row");
assert.strictEqual(readShareRegistry().tokens[missingRow.tokenId].status, "ACTIVE");

process.env.ADP_SHARE_SIGNATURE_SELF_HEAL = "0";
const blockedHeal = issueShareCapability({ propertyId: "adp_westin_tampa_waterside" });
const regDoc2 = JSON.parse(readFileSync(registryFile, "utf8"));
delete regDoc2.tokens[blockedHeal.tokenId];
writeFileSync(registryFile, JSON.stringify(regDoc2));
const unknown = verifyShareCapability(blockedHeal.token);
assert.strictEqual(unknown.ok, false);
assert.strictEqual(unknown.code, "SHARE_UNKNOWN");
delete process.env.ADP_SHARE_SIGNATURE_SELF_HEAL;

const revoked = issueShareCapability({ propertyId: "adp_westin_tampa_waterside" });
revokeShareCapability(revoked.tokenId, "test_revoke");
const regDoc3 = JSON.parse(readFileSync(registryFile, "utf8"));
delete regDoc3.tokens[revoked.tokenId];
writeFileSync(registryFile, JSON.stringify(regDoc3));
const resurrect = verifyShareCapability(revoked.token);
assert.strictEqual(resurrect.ok, false);
assert.strictEqual(resurrect.code, "SHARE_REVOKED");

delete process.env.ADP_SHARE_CAPABILITY_SECRET;
process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET = "1";
const blocked = productionShareSigningAllowed("https://my-operators-backend-production.up.railway.app");
assert.strictEqual(blocked.ok, false);
assert.strictEqual(blocked.code, "PRODUCTION_SECRET_REQUIRED");
const localOk = productionShareSigningAllowed("http://localhost:8080");
assert.strictEqual(localOk.ok, true);
assert.notStrictEqual(ADP_SHARE_DEV_FALLBACK_SECRET, secret);

process.env.ADP_SHARE_CAPABILITY_SECRET = secret;
delete process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET;
const allowed = productionShareSigningAllowed("https://my-operators-backend-production.up.railway.app");
assert.strictEqual(allowed.ok, true);

const localOnly = evaluateExternalShareReadiness({
  signed: true,
  productionStatus: 401,
  propertyResolved: true,
  periodResolved: true,
  surfacesResolved: true,
  loginRequired: false,
});
assert.strictEqual(localOnly.status, EXTERNAL_SHARE_STATUS.EXTERNAL_SHARE_FAILED);

const ready = evaluateExternalShareReadiness({
  signed: true,
  productionStatus: 200,
  propertyResolved: true,
  periodResolved: true,
  surfacesResolved: true,
  loginRequired: false,
});
assert.strictEqual(ready.status, EXTERNAL_SHARE_STATUS.READY);

console.log("adp share production contract ok");
