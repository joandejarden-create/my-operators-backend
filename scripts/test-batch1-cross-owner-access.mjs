#!/usr/bin/env node
/**
 * Batch 1 cross-owner access validation.
 *
 * Uses live Airtable data + dealRecordAllowedForUser + requireDealRecordAccess middleware.
 * Optional HTTP checks when env JWTs are set:
 *   BATCH1_OWNER_A_JWT  — Owner A Memberstack eyJ… session
 *   BATCH1_OWNER_B_JWT  — Owner B Memberstack eyJ… session
 *
 * Usage:
 *   node scripts/test-batch1-cross-owner-access.mjs
 *   node scripts/test-batch1-cross-owner-access.mjs --base-url http://localhost:8080
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";
import { dealRecordAllowedForUser } from "../lib/dealality/deal-record-access.js";
import { requireDealRecordAccess } from "../middleware/requireDealRecordAccess.js";
import { INTAKE_DEALS_USER_LINK_NAME } from "../api/schemas/intake-deal-fields.js";
import { readAirtableField } from "../lib/airtable-utils.js";

const USERS_TABLE = "tbl6shiyz2wdUqE5F";
const DEALS_TABLE = "tblbvSxjiIhXzW6XW";
const EMAIL_FIELD = "fldBl7IXEscwkMhnZ";

const OWNER_A_EMAIL = process.env.BATCH1_OWNER_A_EMAIL || "dealalitydemo@dealality.com";
const OWNER_B_EMAIL = process.env.BATCH1_OWNER_B_EMAIL || "justinboutwell@gmail.com";

const baseUrl = (process.argv.includes("--base-url")
  ? process.argv[process.argv.indexOf("--base-url") + 1]
  : process.env.SMOKE_BASE_URL || "http://localhost:8080"
).replace(/\/$/, "");

let passed = 0;
let failed = 0;
let skipped = 0;

function ok(cond, msg) {
  if (cond) {
    passed += 1;
    console.log("PASS:", msg);
  } else {
    failed += 1;
    console.error("FAIL:", msg);
  }
}

function skip(msg) {
  skipped += 1;
  console.warn("SKIP:", msg);
}

function mockRes() {
  const res = { statusCode: 200, body: null };
  res.status = (code) => {
    res.statusCode = code;
    return res;
  };
  res.json = (body) => {
    res.body = body;
    return res;
  };
  return res;
}

function escapeFormula(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function findUserByEmail(base, email) {
  const lit = escapeFormula(email.toLowerCase());
  const rows = await base(USERS_TABLE)
    .select({ filterByFormula: `LOWER({${EMAIL_FIELD}}) = '${lit}'`, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

async function resolveOwner(email) {
  const result = await resolveDealalityUser({ email: email.toLowerCase() });
  if (!result.found) return null;
  return result;
}

async function findExclusiveDealForUser(base, userRecordId, otherUserRecordId) {
  let found = null;
  await new Promise((resolve, reject) => {
    base(DEALS_TABLE)
      .select({ pageSize: 100 })
      .eachPage(
        (records, next) => {
          if (found) return;
          for (const rec of records) {
            const dealUserIds =
              readAirtableField(rec.fields, INTAKE_DEALS_USER_LINK_NAME) || rec.fields?.Users || [];
            const ids = Array.isArray(dealUserIds) ? dealUserIds : [];
            if (ids.includes(userRecordId) && !ids.includes(otherUserRecordId)) {
              found = rec;
              resolve();
              return;
            }
          }
          next();
        },
        (err) => {
          if (err) reject(err);
          else resolve();
        }
      );
  });
  return found;
}

async function runRequireDealRecordAccess(dealalityUser, dealRecordId) {
  const req = {
    params: { recordId: dealRecordId },
    dealalityUser: {
      isAdmin: dealalityUser.isAdmin,
      isOwner: dealalityUser.isOwner,
      companyId: dealalityUser.companyId,
      companyIds: dealalityUser.companyIds || [],
      userRecordId: dealalityUser.userRecordId,
    },
  };
  const res = mockRes();
  let nextCalled = false;
  await requireDealRecordAccess(req, res, () => {
    nextCalled = true;
  });
  return { statusCode: res.statusCode, body: res.body, nextCalled };
}

async function httpRequest(path, token, options = {}) {
  const headers = { Accept: "application/json", ...(options.headers || {}) };
  if (token) headers.Authorization = `Bearer ${token}`;
  const res = await fetch(`${baseUrl}${path}`, {
    method: options.method || "GET",
    headers,
    body: options.body,
  });
  const text = await res.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { raw: text.slice(0, 200) };
  }
  return { status: res.status, body };
}

const report = {
  ownerA: null,
  ownerB: null,
  ownerADealId: null,
  ownerBDealId: null,
  routesTested: [],
  httpResults: [],
};

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
  process.exit(1);
}

const base = new Airtable({ apiKey }).base(baseId);

console.log("=== Batch 1 cross-owner access validation ===\n");

const ownerA = await resolveOwner(OWNER_A_EMAIL);
const ownerB = await resolveOwner(OWNER_B_EMAIL);

if (!ownerA) {
  console.error(`Could not resolve Owner A: ${OWNER_A_EMAIL}`);
  process.exit(1);
}
if (!ownerB) {
  console.error(`Could not resolve Owner B: ${OWNER_B_EMAIL}`);
  process.exit(1);
}

report.ownerA = {
  email: ownerA.email,
  userRecordId: ownerA.userRecordId,
  memberstackId: ownerA.memberstackId,
  isOwner: ownerA.isOwner,
  workspaceAccess: ownerA.workspaceAccess,
};
report.ownerB = {
  email: ownerB.email,
  userRecordId: ownerB.userRecordId,
  memberstackId: ownerB.memberstackId,
  isOwner: ownerB.isOwner,
  workspaceAccess: ownerB.workspaceAccess,
};

console.log("Owner A:", report.ownerA.email, report.ownerA.userRecordId);
console.log("Owner B:", report.ownerB.email, report.ownerB.userRecordId);

ok(ownerA.isOwner === true, "Owner A has isOwner");
ok(ownerB.isOwner === true, "Owner B has isOwner");
ok(ownerA.userRecordId !== ownerB.userRecordId, "Owner A and B are different Users rows");

const dealA = await findExclusiveDealForUser(base, ownerA.userRecordId, ownerB.userRecordId);
const dealB = await findExclusiveDealForUser(base, ownerB.userRecordId, ownerA.userRecordId);

if (!dealA) {
  console.error("No deal found exclusively linked to Owner A");
  process.exit(1);
}

report.ownerADealId = dealA.id;
report.ownerBDealId = dealB?.id || null;

console.log("\nDeal A (Owner A only):", dealA.id);
if (dealB) console.log("Deal B (Owner B only):", dealB.id);

// --- dealRecordAllowedForUser with live deal fields ---
ok(
  dealRecordAllowedForUser(dealA.fields, {
    isOwner: true,
    isAdmin: false,
    userRecordId: ownerA.userRecordId,
    companyId: ownerA.companyId,
    companyIds: ownerA.companyIds || [],
  }) === true,
  "dealRecordAllowedForUser: Owner A → own deal"
);

ok(
  dealRecordAllowedForUser(dealA.fields, {
    isOwner: true,
    isAdmin: false,
    userRecordId: ownerB.userRecordId,
    companyId: ownerB.companyId,
    companyIds: ownerB.companyIds || [],
  }) === false,
  "dealRecordAllowedForUser: Owner B → Owner A deal (deny)"
);

// --- requireDealRecordAccess middleware (live Airtable fetch) ---
const allowA = await runRequireDealRecordAccess(ownerA, dealA.id);
report.routesTested.push({ route: "middleware requireDealRecordAccess (Owner A own)", status: allowA.statusCode });
ok(allowA.nextCalled && allowA.statusCode === 200, "requireDealRecordAccess: Owner A → own deal → next()");

const denyB = await runRequireDealRecordAccess(ownerB, dealA.id);
report.routesTested.push({ route: "middleware requireDealRecordAccess (Owner B → A deal)", status: denyB.statusCode });
ok(
  !denyB.nextCalled && denyB.statusCode === 403 && denyB.body?.error === "forbidden",
  "requireDealRecordAccess: Owner B → Owner A deal → 403 forbidden"
);

// All Batch 1 deal-scoped routes share myDealsDealAuth → requireDealRecordAccess
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const serverJs = fs.readFileSync(path.join(__dirname, "..", "server.js"), "utf8");
const securedRouteNeedles = [
  'app.get("/api/my-deals/:recordId", ...myDealsDealAuth',
  'app.get("/api/my-deals/:recordId/outreach-setup", ...myDealsDealAuth',
  'app.get("/api/my-deals/:recordId/match-score-breakdown", ...myDealsDealAuth',
  'app.get("/api/my-deals/:recordId/alternative-brands", ...myDealsDealAuth',
  'app.get("/api/franchise-application/:dealId", mapParamDealIdToRecordId, ...myDealsDealAuth',
  'app.post("/api/ai/deal-readiness-review", mapBodyDealIdToRecordId, ...myDealsDealAuth',
  'app.post("/api/ai/brand-alignment-snapshot", mapBodyDealIdToRecordId, ...myDealsDealAuth',
  '"/api/ai/operator-capability-snapshot"',
];
for (const needle of securedRouteNeedles) {
  const label = needle.includes("operator-capability")
    ? "POST /api/ai/operator-capability-snapshot uses myDealsDealAuth"
    : `server.js mounts ${needle.split("(")[0].replace("app.", "")}`;
  ok(serverJs.includes(needle), label);
  report.routesTested.push({ route: label, status: "wired" });
}

// Symmetric check if we have Owner B deal
if (dealB) {
  const allowB = await runRequireDealRecordAccess(ownerB, dealB.id);
  ok(allowB.nextCalled, "requireDealRecordAccess: Owner B → own deal → next()");
  const denyA = await runRequireDealRecordAccess(ownerA, dealB.id);
  ok(!denyA.nextCalled && denyA.statusCode === 403, "requireDealRecordAccess: Owner A → Owner B deal → 403");
}

// --- Optional HTTP with JWT env vars ---
const jwtA = process.env.BATCH1_OWNER_A_JWT || "";
const jwtB = process.env.BATCH1_OWNER_B_JWT || "";
const dealId = dealA.id;

const httpRoutes = [
  { method: "GET", path: `/api/my-deals/${dealId}`, owner: "A", expectOwnerA: 200, expectOwnerB: 403 },
  { method: "GET", path: `/api/my-deals/${dealId}/outreach-setup`, owner: "cross", expectOwnerB: 403 },
  { method: "GET", path: `/api/my-deals/${dealId}/match-score-breakdown?brand=Test`, owner: "cross", expectOwnerB: 403 },
  { method: "GET", path: `/api/my-deals/${dealId}/alternative-brands`, owner: "cross", expectOwnerB: 403 },
  { method: "GET", path: `/api/franchise-application/${dealId}`, owner: "cross", expectOwnerB: 403 },
  {
    method: "POST",
    path: "/api/ai/deal-readiness-review",
    owner: "cross",
    expectOwnerB: 403,
    body: JSON.stringify({ dealId }),
    headers: { "Content-Type": "application/json" },
  },
  {
    method: "POST",
    path: "/api/ai/brand-alignment-snapshot",
    owner: "cross",
    expectOwnerB: 403,
    body: JSON.stringify({ dealId }),
    headers: { "Content-Type": "application/json" },
  },
  {
    method: "POST",
    path: "/api/ai/operator-capability-snapshot",
    owner: "cross",
    expectOwnerB: 403,
    body: JSON.stringify({ dealId }),
    headers: { "Content-Type": "application/json" },
  },
];

console.log("\n--- HTTP (optional JWT) ---");

if (!jwtA && !jwtB) {
  skip("HTTP route checks — set BATCH1_OWNER_A_JWT and BATCH1_OWNER_B_JWT for live JWT tests");
} else {
  // Health: server reachable
  try {
    const ping = await fetch(`${baseUrl}/api/me`, { headers: { Accept: "application/json" } });
    ok(ping.status === 401 || ping.status === 403, `Server reachable at ${baseUrl} (unauth /api/me → ${ping.status})`);
  } catch (err) {
    fail(`Server not reachable at ${baseUrl}: ${err.message}`);
  }

  if (jwtA) {
    const own = await httpRequest(`/api/my-deals/${dealId}`, jwtA);
    report.httpResults.push({ path: `/api/my-deals/${dealId}`, token: "A", status: own.status });
    ok(own.status === 200, `HTTP Owner A GET own deal → 200 (got ${own.status})`);
  } else {
    skip("Owner A JWT not set — skip Owner A HTTP 200 check");
  }

  if (jwtB) {
    const blocked = await httpRequest(`/api/my-deals/${dealId}`, jwtB);
    report.httpResults.push({ path: `/api/my-deals/${dealId}`, token: "B", status: blocked.status });
    ok(blocked.status === 403, `HTTP Owner B GET Owner A deal → 403 (got ${blocked.status})`);

    for (const r of httpRoutes.filter((x) => x.owner === "cross")) {
      const result = await httpRequest(r.path, jwtB, {
        method: r.method,
        body: r.body,
        headers: r.headers,
      });
      report.httpResults.push({ path: r.path, method: r.method, token: "B", status: result.status });
      report.routesTested.push({ route: `${r.method} ${r.path}`, token: "B", status: result.status });
      ok(
        result.status === 403,
        `HTTP Owner B ${r.method} ${r.path} → 403 (got ${result.status})`
      );
    }
  } else {
    skip("Owner B JWT not set — skip Owner B HTTP 403 checks");
  }
}

console.log("\n=== Summary ===");
console.log(`Passed: ${passed}, Failed: ${failed}, Skipped: ${skipped}`);
console.log("\nReport JSON:");
console.log(JSON.stringify(report, null, 2));

process.exit(failed > 0 ? 1 : 0);
