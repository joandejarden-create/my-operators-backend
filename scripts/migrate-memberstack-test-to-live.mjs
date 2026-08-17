#!/usr/bin/env node
/**
 * Recreate a Memberstack member in Live from Test Mode and relink Airtable Users.
 *
 * Memberstack has separate Test and Live databases — there is no built-in "transfer".
 * This script:
 *   1. Reads the Test member (sk_sb_…)
 *   2. Creates or finds the Live member (sk_…)
 *   3. Patches Live custom fields from Airtable (identity hints only)
 *   4. Updates Airtable Unique Webflow ID / Slug to the Live mem_… id
 *      (does NOT change Workspace Access, Company Profile, or User Type)
 *
 * Usage:
 *   MEMBERSTACK_TEST_SECRET_KEY=sk_sb_... MEMBERSTACK_SECRET_KEY=sk_... \
 *     node scripts/migrate-memberstack-test-to-live.mjs --email you@example.com
 *
 *   Add --execute to write (default is dry-run).
 *
 * After --execute:
 *   - User must verify email again in Live (new verification email).
 *   - User needs a new password (script prints a one-time temp if member was created).
 *   - Webflow must use the Live MEMBERSTACK_APP_ID.
 */
import "../load-env.js";
import axios from "axios";
import Airtable from "airtable";
import { randomBytes } from "crypto";
import { memberstackSecretEnvironment } from "../lib/memberstack/environment.js";
import { buildMemberstackCustomFields } from "../lib/memberstack/memberstack-custom-fields.js";
import { USERS_SIGNUP } from "../lib/signup-airtable-upsert.js";

const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(
  /\/$/,
  ""
);

const USERS_TABLE = process.env.AIRTABLE_INTAKE_USERS_TABLE || USERS_SIGNUP.table;
const MS_ID_FIELD =
  process.env.AIRTABLE_INTAKE_USERS_UNIQUE_WEBFLOW_ID_FIELD || USERS_SIGNUP.uniqueWebflowId;
const SLUG_FIELD = process.env.AIRTABLE_USERS_SLUG_FIELD || "fldEgbHu5MvfyrxgE";

function parseArgs(argv) {
  const out = { email: null, execute: false, help: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--email" && argv[i + 1]) out.email = String(argv[++i]).trim().toLowerCase();
    else if (a === "--execute") out.execute = true;
    else if (a === "--help" || a === "-h") out.help = true;
  }
  return out;
}

function headersForKey(secretKey) {
  return { "X-API-KEY": secretKey.trim(), "Content-Type": "application/json" };
}

async function getMemberByEmail(email, secretKey) {
  const res = await axios.get(`${BASE}/members/${encodeURIComponent(email)}`, {
    headers: headersForKey(secretKey),
    validateStatus: (s) => s === 200 || s === 404,
  });
  if (res.status === 404) return null;
  if (res.status !== 200) {
    const err = new Error(`Memberstack GET failed: ${res.status}`);
    err.response = res.data;
    throw err;
  }
  return res.data?.data || res.data?.member || null;
}

async function createLiveMember(email, testMember, liveSecret, pendingPlanId) {
  const tempPassword = randomBytes(18).toString("base64url").slice(0, 22);
  const cf =
    testMember?.customFields && typeof testMember.customFields === "object"
      ? { ...testMember.customFields }
      : {};
  const payload = {
    email,
    password: tempPassword,
    customFields: cf,
  };
  if (pendingPlanId) payload.plans = [{ planId: pendingPlanId }];

  const res = await axios.post(`${BASE}/members`, payload, {
    headers: headersForKey(liveSecret),
    validateStatus: () => true,
  });
  if (res.status < 200 || res.status >= 300 || !res.data?.data?.id) {
    const err = new Error(`Live member create failed: ${res.status}`);
    err.response = res.data;
    throw err;
  }
  return { member: res.data.data, tempPassword };
}

async function patchLiveCustomFields(memberId, customFields, liveSecret) {
  const res = await axios.patch(
    `${BASE}/members/${memberId}`,
    { customFields },
    { headers: headersForKey(liveSecret), validateStatus: () => true }
  );
  return res.status >= 200 && res.status < 300;
}

async function findAirtableUser(email) {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  }
  const esc = email.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  const base = new Airtable({ apiKey }).base(baseId);
  const rows = await base(USERS_TABLE)
    .select({ filterByFormula: `{Email} = '${esc}'`, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help || !args.email) {
    console.log(`Usage:
  node scripts/migrate-memberstack-test-to-live.mjs --email <email> [--execute]

Env (any one pattern):
  MEMBERSTACK_TEST_SECRET_KEY=sk_sb_…     read Test member
  MEMBERSTACK_LIVE_SECRET_KEY=sk_…        create/link Live (recommended with --execute)
  MEMBERSTACK_SECRET_KEY                  fallback: sk_sb_ for Test read, sk_ for Live if live key unset
  MEMBERSTACK_SIGNUP_PENDING_PLAN_ID      Live plan id (pln_…), not Test plan id
  AIRTABLE_API_KEY, AIRTABLE_BASE_ID

Default: dry-run. Pass --execute to apply.`);
    process.exit(args.help ? 0 : 1);
  }

  const secretFallback = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
  const testKeyExplicit = (process.env.MEMBERSTACK_TEST_SECRET_KEY || "").trim();
  const liveKeyExplicit = (process.env.MEMBERSTACK_LIVE_SECRET_KEY || "").trim();

  const testKey =
    testKeyExplicit ||
    (memberstackSecretEnvironment(secretFallback) === "sandbox" ? secretFallback : "");
  const liveKey =
    liveKeyExplicit ||
    (memberstackSecretEnvironment(secretFallback) === "live" ? secretFallback : "");

  if (!testKey) {
    console.error(
      "Need Test API key: set MEMBERSTACK_TEST_SECRET_KEY=sk_sb_… or MEMBERSTACK_SECRET_KEY=sk_sb_…"
    );
    process.exit(1);
  }
  if (memberstackSecretEnvironment(testKey) !== "sandbox") {
    console.warn("Warning: Test key should be sk_sb_… (sandbox)");
  }
  if (args.execute && !liveKey) {
    console.error(
      "Need Live API key for --execute: set MEMBERSTACK_LIVE_SECRET_KEY=sk_… (not sk_sb_)"
    );
    process.exit(1);
  }
  if (liveKey && memberstackSecretEnvironment(liveKey) !== "live") {
    console.error("Live key must be sk_… (not sk_sb_)");
    process.exit(1);
  }

  const pendingPlanId = (process.env.MEMBERSTACK_SIGNUP_PENDING_PLAN_ID || "").trim();
  const email = args.email;

  console.log("Mode:", args.execute ? "EXECUTE" : "DRY-RUN");
  console.log("Email:", email);

  const testMember = await getMemberByEmail(email, testKey);
  if (!testMember?.id) {
    console.error("No Test Mode member for this email. Check Test dashboard or email spelling.");
    process.exit(1);
  }
  console.log("Test member:", testMember.id, "verified:", testMember.verified ?? testMember.auth?.verified);

  let liveMember = null;
  let tempPassword = null;
  if (liveKey) {
    liveMember = await getMemberByEmail(email, liveKey);
  } else {
    console.log("Live member: skipped (no Live key in env — set MEMBERSTACK_LIVE_SECRET_KEY to check)");
  }
  if (liveMember?.id) {
    console.log("Live member already exists:", liveMember.id);
  } else if (liveKey) {
    console.log("Live member: will create");
    if (args.execute) {
      const created = await createLiveMember(email, testMember, liveKey, pendingPlanId);
      liveMember = created.member;
      tempPassword = created.tempPassword;
      console.log("Created Live member:", liveMember.id);
      console.log("TEMP PASSWORD (share securely; user should change):", tempPassword);
    }
  }

  const airtableRow = await findAirtableUser(email);
  if (!airtableRow) {
    console.warn("No Airtable Users row for email — skip Airtable link");
  } else {
    const oldMs = airtableRow.fields?.[MS_ID_FIELD] || airtableRow.fields?.["Unique Webflow ID"];
    console.log("Airtable Users:", airtableRow.id, "current mem id:", oldMs || "(empty)");
    if (args.execute && liveMember?.id && liveKey) {
      const patch = { [MS_ID_FIELD]: liveMember.id, [SLUG_FIELD]: liveMember.id };
      const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
        process.env.AIRTABLE_BASE_ID
      );
      await base(USERS_TABLE).update(airtableRow.id, patch, { typecast: true });
      console.log("Airtable linked to Live member id");

      const f = airtableRow.fields;
      const body = {
        email,
        firstName: f[USERS_SIGNUP.firstName] || f["First Name"] || "",
        lastName: f[USERS_SIGNUP.lastName] || f["Last Name"] || "",
        companyName: f["Company Name"] || "",
        phone: f["Phone Number"] || "",
        companyType: f["Auth Role Hint"] || f["User Type"] || "",
      };
      const companyProfileId = Array.isArray(f["Company Profile"])
        ? f["Company Profile"][0]
        : "";
      const customFields = buildMemberstackCustomFields(body, {
        airtableRecordId: airtableRow.id,
        companyProfileId,
      });
      const ok = await patchLiveCustomFields(liveMember.id, customFields, liveKey);
      console.log("Live custom fields patch:", ok ? "ok" : "failed");
    }
  }

  if (!args.execute) {
    console.log("\nDry-run complete. Re-run with --execute to apply.");
    process.exit(0);
  }

  console.log("\nManual follow-up:");
  console.log("  1. User verifies email in Live (new link).");
  console.log("  2. User logs in with temp password or password reset.");
  console.log("  3. Webflow + Railway use Live MEMBERSTACK_APP_ID.");
  console.log("  4. Airtable Workspace Access unchanged — still controls /api/me.");
  console.log("  5. Optional: delete Test member in Memberstack dashboard.");
}

main().catch((err) => {
  console.error(err.response || err.message || err);
  process.exit(1);
});
