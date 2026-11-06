#!/usr/bin/env node
/**
 * Preflight for GitHub Actions production release workflow.
 * Does not deploy. Validates inputs, clean checkout, and main ancestry.
 *
 *   node scripts/dealality-production-release-preflight-v1.mjs \
 *     --product AVA --sha <40hex> --authorization-phrase I_AUTHORIZE_PRODUCTION_RELEASE
 */
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertApprovedShaFormat,
  assertAuthorizationPhrase,
  assertProduct,
  AUTHORIZATION_PHRASE,
} from "../lib/dealality-production-release/release-channel-v1.js";

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

const ROOT = path.resolve(
  arg("--repo-root") || path.join(path.dirname(fileURLToPath(import.meta.url)), "..")
);

function fail(code, message, extra = {}) {
  console.error(JSON.stringify({ ok: false, code, message, ...extra }, null, 2));
  process.exit(2);
}

function sh(cmd) {
  const r = spawnSync(cmd, {
    cwd: ROOT,
    encoding: "utf8",
    shell: true,
  });
  if (r.status !== 0) {
    return { ok: false, stdout: String(r.stdout || "").trim(), stderr: String(r.stderr || "").trim() };
  }
  return { ok: true, stdout: String(r.stdout || "").trim() };
}

const productGate = assertProduct(arg("--product"));
if (!productGate.ok) fail(productGate.code, productGate.message);

const shaGate = assertApprovedShaFormat(arg("--sha"));
if (!shaGate.ok) fail(shaGate.code, shaGate.message);

const phraseGate = assertAuthorizationPhrase(arg("--authorization-phrase"));
if (!phraseGate.ok) fail(phraseGate.code, phraseGate.message);

const head = sh("git rev-parse HEAD");
if (!head.ok) fail("GIT_HEAD_FAILED", head.stderr || "cannot read HEAD");
const headSha = head.stdout.toLowerCase();
if (headSha !== shaGate.sha) {
  fail("SHA_MISMATCH", `HEAD ${headSha} != approved ${shaGate.sha}`);
}

const dirty = sh("git status --porcelain");
if (!dirty.ok) fail("GIT_STATUS_FAILED", dirty.stderr || "cannot read status");
if (dirty.stdout) {
  fail("DIRTY_TREE", "working tree must be clean for production release", {
    dirty: dirty.stdout.split("\n").slice(0, 20),
  });
}

// Ensure approved SHA is on main ancestry (fetch origin/main if present).
sh("git fetch origin main --depth=200");
const mergeBase = sh(`git merge-base --is-ancestor ${shaGate.sha} origin/main`);
const tip = sh("git rev-parse origin/main");
const tipSha = tip.ok ? tip.stdout.toLowerCase() : "";
const onMainAncestry =
  mergeBase.ok || (tipSha && tipSha === shaGate.sha);
if (!onMainAncestry) {
  // Also allow when origin/main IS the approved sha after shallow fetch quirks
  const contains = sh(`git branch -r --contains ${shaGate.sha}`);
  const listed = String(contains.stdout || "");
  if (!listed.includes("origin/main")) {
    fail(
      "SHA_NOT_ON_MAIN",
      `approved SHA ${shaGate.sha} is not an ancestor of origin/main`,
      { originMain: tipSha || null }
    );
  }
}

const out = {
  ok: true,
  contract: "DEALALITY_PRODUCTION_RELEASE_PREFLIGHT_V1",
  product: productGate.product,
  approvedSha: shaGate.sha,
  headSha,
  originMain: tipSha || null,
  authorizationPhraseAccepted: true,
  authorizationPhrase: AUTHORIZATION_PHRASE,
  cleanCheckout: true,
  channel: "github-actions",
};
console.log(JSON.stringify(out, null, 2));
