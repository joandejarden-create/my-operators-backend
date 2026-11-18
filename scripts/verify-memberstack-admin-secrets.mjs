#!/usr/bin/env node
/**
 * Read-only: verify Memberstack Admin API secrets without printing key material.
 *
 * Exit 0 when LIVE (and TEST when set) authenticate against Admin API.
 * Exit 1 when any configured secret returns validation/invalid-secret-key.
 *
 * Usage (from repo root, with .env loaded by the shell or dotenv):
 *   node --import dotenv/config scripts/verify-memberstack-admin-secrets.mjs
 */
import { memberstackSecretEnvironment } from "../lib/memberstack/environment.js";

async function probe(label, secret) {
  if (!secret) {
    return { label, present: false, validAuth: false, reason: "missing" };
  }
  const res = await fetch("https://admin.memberstack.com/members/mem_probe_secret_validity", {
    headers: { "X-API-KEY": secret, Accept: "application/json" },
  });
  const body = await res.json().catch(() => ({}));
  const invalid =
    res.status === 401 && String(body?.code || "").includes("invalid-secret-key");
  return {
    label,
    present: true,
    env: memberstackSecretEnvironment(secret),
    prefixClass: secret.startsWith("sk_sb_")
      ? "sk_sb"
      : secret.startsWith("sk_")
        ? "sk_live"
        : "other",
    status: res.status,
    code: body?.code || null,
    validAuth: !invalid,
    reason: invalid ? "secret_invalid" : res.status === 404 ? "auth_ok_member_missing" : "checked",
  };
}

const live = await probe("MEMBERSTACK_SECRET_KEY", (process.env.MEMBERSTACK_SECRET_KEY || "").trim());
const test = await probe(
  "MEMBERSTACK_TEST_SECRET_KEY",
  (
    process.env.MEMBERSTACK_TEST_SECRET_KEY ||
    process.env.MEMBERSTACK_SANDBOX_SECRET_KEY ||
    ""
  ).trim()
);

const report = {
  live,
  test,
  ok: live.validAuth === true && (test.present ? test.validAuth === true : true),
  note:
    "Localhost Test Mode (mem_sb_) needs a valid MEMBERSTACK_TEST_SECRET_KEY so JWT email can be enriched via member_lookup. Refresh keys from the Memberstack dashboard (Test + Live) into Integration Controller .env, then restart :8080.",
};

console.log(JSON.stringify(report, null, 2));
process.exit(report.ok ? 0 : 1);
