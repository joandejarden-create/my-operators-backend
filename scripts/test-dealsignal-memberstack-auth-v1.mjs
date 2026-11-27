#!/usr/bin/env node
/**
 * Static + unit certification for DealSignal Memberstack auth hardening.
 * No paid calls. Does not mutate opportunity data.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const htmlPath = path.join(ROOT, "public", "deal-opportunities.html");
const verifyPath = path.join(ROOT, "lib", "memberstack", "verify-token.js");

const results = [];
function pass(name, detail = "") {
  results.push({ name, status: "PASS", detail });
  console.log(`PASS  ${name}${detail ? " — " + detail : ""}`);
}
function fail(name, err) {
  results.push({ name, status: "FAIL", detail: String(err?.stack || err) });
  console.error(`FAIL  ${name}\n`, err);
}

function testHtmlGuards() {
  const html = fs.readFileSync(htmlPath, "utf8");
  assert.match(html, /function isValidMsJwt/);
  assert.match(html, /function pickBearerJwt/);
  assert.match(html, /function isAuthFailure/);
  assert.match(html, /function failAuthLoad/);
  assert.match(html, /Do not retry the same rejected token/);
  assert.doesNotMatch(html, /localStorage\.getItem\("_ms-mem"\)/);
  assert.match(html, /Intentionally never read localStorage `_ms-mem`/);
  assert.match(html, /const isLocal = host === "localhost"/);
  assert.match(html, /if \(isLocal\)/);
  pass("DealSignal HTML rejects _ms-mem Bearer and stops auth retries");
}

function testVerifyTokenSandboxSecretSelection() {
  const src = fs.readFileSync(verifyPath, "utf8");
  assert.match(src, /candidateAdminSecrets/);
  assert.match(src, /peekJwtPayloadUnsafe/);
  assert.match(src, /isSandboxMemberstackId/);
  assert.match(src, /MEMBERSTACK_TEST_SECRET_KEY|MEMBERSTACK_SANDBOX_SECRET_KEY/);
  assert.match(src, /verifyViaRestApiWithSecret/);
  assert.match(src, /assertSecretMatchesMemberId/);
  assert.match(src, /NODE_ENV === "production"/);
  assert.match(src, /MEMBERSTACK_ENV_MISMATCH/);
  pass("verify-token env-matches secrets; production live-only");
}

function testDevAuthBypassFailClosed() {
  const mw = fs.readFileSync(path.join(ROOT, "middleware", "memberstackAuth.js"), "utf8");
  assert.match(mw, /function isDevAuthBypassAllowed/);
  assert.match(mw, /function isLoopbackRemoteAddress/);
  assert.match(mw, /NODE_ENV === "production"/);
  assert.match(mw, /DEALALITY_ALLOW_DEV_AUTH_BYPASS/);
  assert.match(mw, /isDevAuthBypassAllowed\(req\)/);
  // Must not interpolate raw Bearer JWT into logs
  assert.doesNotMatch(mw, /console\.\w+\([^)]*\$\{[^}]*token/);
  assert.doesNotMatch(mw, /console\.\w+\(\s*token\s*[,)]/);
  assert.match(mw, /msg\.slice\(0,\s*120\)/);
  pass("DEV_AUTH_BYPASS fail-closed + no credential logging");
}

async function main() {
  try {
    testHtmlGuards();
  } catch (e) {
    fail("DealSignal HTML rejects _ms-mem Bearer and stops auth retries", e);
  }
  try {
    testVerifyTokenSandboxSecretSelection();
  } catch (e) {
    fail("verify-token env-matches secrets; production live-only", e);
  }
  try {
    testDevAuthBypassFailClosed();
  } catch (e) {
    fail("DEV_AUTH_BYPASS fail-closed + no credential logging", e);
  }

  const failed = results.filter((r) => r.status === "FAIL");
  const outDir = path.join(ROOT, "reports", "dealsignal-auth");
  fs.mkdirSync(outDir, { recursive: true });
  const report = {
    version: "test-dealsignal-memberstack-auth-v1",
    generatedAt: new Date().toISOString(),
    summary: { total: results.length, passed: results.length - failed.length, failed: failed.length },
    results,
  };
  fs.writeFileSync(path.join(outDir, "CERTIFICATION.json"), JSON.stringify(report, null, 2));
  console.log(`\n${report.summary.passed}/${report.summary.total} passed`);
  if (failed.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
