#!/usr/bin/env node
/**
 * Batch 2A final validation — listAll admin gate, cross-owner denial, optional HTTP JWT.
 *
 * Optional env:
 *   BATCH1_OWNER_A_JWT, BATCH1_OWNER_B_JWT — Memberstack session JWTs
 *   BATCH2A_ADMIN_JWT — admin user JWT for listAll 200 check
 *   SMOKE_BASE_URL — default http://localhost:8080
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";
import { assertOwnerDealAccess } from "../lib/dealality/owner-deal-id-access.js";
import { requireDealRecordAccess } from "../middleware/requireDealRecordAccess.js";
import { requireTargetListRecordAccess } from "../middleware/requireTargetListRecordAccess.js";
import { requireOwnerBdrActivityAccess, requireOwnerBdrDealMetaAccess } from "../middleware/requireOwnerBdrActivityAccess.js";
import { INTAKE_DEALS_USER_LINK_NAME } from "../api/schemas/intake-deal-fields.js";
import { readAirtableField } from "../lib/airtable-utils.js";
import { TARGET_LIST_TABLE } from "../api/schemas/target-list-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OWNER_A_EMAIL = process.env.BATCH1_OWNER_A_EMAIL || "dealalitydemo@dealality.com";
const OWNER_B_EMAIL = process.env.BATCH1_OWNER_B_EMAIL || "justinboutwell@gmail.com";
const baseUrl = (process.env.SMOKE_BASE_URL || "http://localhost:8080").replace(/\/$/, "");

const USERS_TABLE = "tbl6shiyz2wdUqE5F";
const DEALS_TABLE = "tblbvSxjiIhXzW6XW";
const EMAIL_FIELD = "fldBl7IXEscwkMhnZ";

let passed = 0;
let failed = 0;
let skipped = 0;

const report = {
  ownerA: null,
  ownerB: null,
  ownerADealId: null,
  routesTested: [],
  httpResults: [],
  skipped: [],
};

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
  report.skipped.push(msg);
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

function dealalityUserCtx(u) {
  return {
    isAdmin: u.isAdmin,
    isOwner: u.isOwner,
    companyId: u.companyId,
    companyIds: u.companyIds || [],
    userRecordId: u.userRecordId,
    workspaceAccess: u.workspaceAccess || [],
  };
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
        (err) => (err ? reject(err) : resolve())
      );
  });
  return found;
}

async function findTargetForDeal(baseId, apiKey, dealId) {
  const table = encodeURIComponent(TARGET_LIST_TABLE);
  const url = `https://api.airtable.com/v0/${baseId}/${table}?maxRecords=20`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const data = await res.json();
  for (const rec of data.records || []) {
    const dealIds = rec.fields?.Deal_ID;
    if (Array.isArray(dealIds) && dealIds.includes(dealId)) return rec.id;
  }
  return null;
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
    body = { raw: text.slice(0, 120) };
  }
  return { status: res.status, body };
}

async function runRequireDealAccess(user, dealId) {
  const req = { params: { recordId: dealId }, dealalityUser: dealalityUserCtx(user) };
  const res = mockRes();
  let nextCalled = false;
  await requireDealRecordAccess(req, res, () => {
    nextCalled = true;
  });
  return { statusCode: res.statusCode, body: res.body, nextCalled };
}

async function runTargetListAccess(user, targetId) {
  const req = { params: { targetId }, dealalityUser: dealalityUserCtx(user) };
  const res = mockRes();
  let nextCalled = false;
  await requireTargetListRecordAccess(req, res, () => {
    nextCalled = true;
  });
  return { statusCode: res.statusCode, body: res.body, nextCalled };
}

async function runBdrActivityAccess(user, dealId) {
  const req = {
    headers: { Authorization: "Bearer test" },
    query: { dealId },
    dealalityUser: dealalityUserCtx(user),
  };
  const res = mockRes();
  let nextCalled = false;
  await requireOwnerBdrActivityAccess(req, res, () => {
    nextCalled = true;
  });
  return { statusCode: res.statusCode, body: res.body, nextCalled };
}

async function runBdrDealMetaAccess(user, dealId) {
  const req = {
    headers: { Authorization: "Bearer test" },
    query: { ids: dealId },
    dealalityUser: dealalityUserCtx(user),
  };
  const res = mockRes();
  let nextCalled = false;
  await requireOwnerBdrDealMetaAccess(req, res, () => {
    nextCalled = true;
  });
  return { statusCode: res.statusCode, body: res.body, nextCalled, query: req.query };
}

async function runListAllAdminCheck(user) {
  const req = {
    headers: {},
    query: { all: "1" },
    dealalityUser: dealalityUserCtx(user),
  };
  const res = mockRes();
  let nextCalled = false;
  const u = req.dealalityUser;
  if (!u?.isAdmin) {
    res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden",
      message: "Admin access required for listAll.",
    });
    return { statusCode: res.statusCode, body: res.body, nextCalled: false };
  }
  nextCalled = true;
  return { statusCode: 200, body: null, nextCalled };
}

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
  process.exit(1);
}

const base = new Airtable({ apiKey }).base(baseId);

console.log("=== Batch 2A final validation ===\n");

const ownerA = await resolveDealalityUser({ email: OWNER_A_EMAIL.toLowerCase() });
const ownerB = await resolveDealalityUser({ email: OWNER_B_EMAIL.toLowerCase() });
if (!ownerA.found || !ownerB.found) {
  console.error("Could not resolve owner test users");
  process.exit(1);
}

report.ownerA = {
  email: ownerA.email,
  userRecordId: ownerA.userRecordId,
  isAdmin: ownerA.isAdmin,
  workspaceAccess: ownerA.workspaceAccess,
};
report.ownerB = {
  email: ownerB.email,
  userRecordId: ownerB.userRecordId,
  isAdmin: ownerB.isAdmin,
  workspaceAccess: ownerB.workspaceAccess,
};

console.log("Owner A:", report.ownerA.email, report.ownerA.userRecordId, "isAdmin:", report.ownerA.isAdmin);
console.log("Owner B:", report.ownerB.email, report.ownerB.userRecordId, "isAdmin:", report.ownerB.isAdmin);

const dealA = await findExclusiveDealForUser(base, ownerA.userRecordId, ownerB.userRecordId);
if (!dealA) {
  console.error("No deal exclusively linked to Owner A");
  process.exit(1);
}
report.ownerADealId = dealA.id;
console.log("Owner A exclusive deal:", dealA.id);

const serverJs = fs.readFileSync(path.join(__dirname, "..", "server.js"), "utf8");
ok(serverJs.includes("gateOwnerBdrListAll"), "server.js wires gateOwnerBdrListAll on BDR router");

// --- 1. listAll admin-only (middleware) ---
console.log("\n--- listAll admin gate ---");
const listAllOwnerB = await runListAllAdminCheck(ownerB);
report.routesTested.push({ route: "GET ?all=1 gate (Owner B middleware)", status: listAllOwnerB.statusCode });
ok(
  !listAllOwnerB.nextCalled && listAllOwnerB.statusCode === 403,
  "Non-admin Owner B → listAll gate → 403"
);

const listAllOwnerA = await runListAllAdminCheck(ownerA);
ok(!listAllOwnerA.nextCalled && listAllOwnerA.statusCode === 403, "Non-admin Owner A → listAll admin check → 403");

const mockAdmin = { ...ownerA, isAdmin: true };
const listAllAdmin = await runListAllAdminCheck(mockAdmin);
ok(listAllAdmin.nextCalled, "Synthetic isAdmin user → listAll admin check → pass");

// --- 2. Cross-owner Batch 2A (middleware + assertOwnerDealAccess) ---
console.log("\n--- Cross-owner Batch 2A (middleware) ---");

const allowTargetListA = await runRequireDealAccess(ownerA, dealA.id);
report.routesTested.push({ route: "GET target-list/:dealId (Owner A)", status: allowTargetListA.statusCode });
ok(allowTargetListA.nextCalled, "Owner A → target-list deal access → allow");

const denyTargetListB = await runRequireDealAccess(ownerB, dealA.id);
report.routesTested.push({ route: "GET target-list/:dealId (Owner B → A deal)", status: denyTargetListB.statusCode });
ok(!denyTargetListB.nextCalled && denyTargetListB.statusCode === 403, "Owner B → target-list deal access → 403");

const denyBdrDealA = await assertOwnerDealAccess(ownerB, dealA.id);
report.routesTested.push({ route: "BDR by-deals dealId filter (Owner B → A)", status: denyBdrDealA.status });
ok(!denyBdrDealA.ok && denyBdrDealA.status === 403, "Owner B → assertOwnerDealAccess Owner A deal → 403");

const denyActivityB = await runBdrActivityAccess(ownerB, dealA.id);
report.routesTested.push({ route: "BDR activity dealId (Owner B → A)", status: denyActivityB.statusCode });
ok(!denyActivityB.nextCalled && denyActivityB.statusCode === 403, "Owner B → BDR activity dealId → 403");

const denyMetaB = await runBdrDealMetaAccess(ownerB, dealA.id);
report.routesTested.push({
  route: "BDR deal-meta ids (Owner B → A)",
  status: denyMetaB.statusCode,
  filteredIds: denyMetaB.query?.ids,
});
ok(
  denyMetaB.nextCalled && String(denyMetaB.query?.ids || "") === "",
  "Owner B → BDR deal-meta → filtered to empty ids (no leak)"
);

const targetId = await findTargetForDeal(baseId, apiKey, dealA.id);
if (targetId) {
  const denyTargetB = await runTargetListAccess(ownerB, targetId);
  report.routesTested.push({ route: `PATCH/DELETE target-list/${targetId} (Owner B)`, status: denyTargetB.statusCode });
  ok(!denyTargetB.nextCalled && denyTargetB.statusCode === 403, "Owner B → target record access → 403");
} else {
  skip("No Target List row for Owner A deal — targetId record access not tested");
}

const denyAttachPostB = await runRequireDealAccess(ownerB, dealA.id);
report.routesTested.push({ route: "POST attachments (Owner B → A deal)", status: denyAttachPostB.statusCode });
ok(!denyAttachPostB.nextCalled && denyAttachPostB.statusCode === 403, "Owner B → attachment upload deal → 403");

const denyAttachGetB = await runRequireDealAccess(ownerB, dealA.id);
report.routesTested.push({ route: "GET attachments (Owner B → A deal)", status: denyAttachGetB.statusCode });
ok(!denyAttachGetB.nextCalled && denyAttachGetB.statusCode === 403, "Owner B → attachment download deal → 403");

ok(serverJs.includes('app.get("/api/target-list/:dealId", mapTargetListDealParam, ...myDealsDealAuth'), "target-list GET secured");
ok(serverJs.includes('app.post("/api/brand-deal-requests/by-deals", ...myDealsAuth'), "BDR by-deals secured");
ok(serverJs.includes('gateOwnerBdrActivity'), "BDR activity gate wired");
ok(serverJs.includes('gateOwnerBdrDealMeta'), "BDR deal-meta gate wired");
ok(serverJs.includes('app.post("/api/my-deals/:recordId/attachments", ...myDealsDealAuth'), "attachment POST secured");
ok(serverJs.includes('app.get("/api/my-deals/:recordId/attachments/:filename", ...myDealsDealAuth'), "attachment GET secured");

// --- Optional HTTP ---
console.log("\n--- HTTP (optional JWT) ---");
const jwtA = process.env.BATCH1_OWNER_A_JWT || "";
const jwtB = process.env.BATCH1_OWNER_B_JWT || "";
const jwtAdmin = process.env.BATCH2A_ADMIN_JWT || "";

try {
  const ping = await fetch(`${baseUrl}/api/brand-deal-requests?all=1`);
  ok(ping.status === 401, `Unauth listAll → 401 (got ${ping.status})`);
  report.httpResults.push({ path: "?all=1", token: "none", status: ping.status });
} catch (err) {
  failed += 1;
  console.error("FAIL: Server not reachable:", err.message);
}

if (jwtB) {
  const listAllHttpB = await httpRequest("/api/brand-deal-requests?all=1", jwtB);
  report.httpResults.push({ path: "?all=1", token: "B", status: listAllHttpB.status });
  ok(listAllHttpB.status === 403, `HTTP Owner B listAll → 403 (got ${listAllHttpB.status})`);

  const routes = [
    { method: "GET", path: `/api/target-list/${dealA.id}` },
    { method: "POST", path: "/api/brand-deal-requests/by-deals", body: JSON.stringify({ dealIds: [dealA.id] }), headers: { "Content-Type": "application/json" } },
    { method: "GET", path: `/api/brand-deal-requests/activity?dealId=${dealA.id}` },
    { method: "GET", path: `/api/brand-deal-requests/deal-meta?ids=${dealA.id}` },
    { method: "POST", path: `/api/my-deals/${dealA.id}/attachments` },
    { method: "GET", path: `/api/my-deals/${dealA.id}/attachments/test.pdf` },
  ];

  for (const r of routes) {
    const result = await httpRequest(r.path, jwtB, {
      method: r.method,
      body: r.body,
      headers: r.headers,
    });
    report.httpResults.push({ path: r.path, method: r.method, token: "B", status: result.status });
    report.routesTested.push({ route: `HTTP ${r.method} ${r.path}`, token: "B", status: result.status });
    const pass =
      r.path.includes("by-deals")
        ? result.status === 200 &&
          result.body?.success === true &&
          Array.isArray(result.body?.contacted) &&
          result.body.contacted.length === 0
        : r.path.includes("deal-meta")
          ? result.status === 200 &&
            result.body?.success === true &&
            Array.isArray(result.body?.deals) &&
            result.body.deals.length === 0
          : result.status === 403;
    const label =
      r.path.includes("by-deals") || r.path.includes("deal-meta")
        ? "200 empty scoped result"
        : "403";
    ok(pass, `HTTP Owner B ${r.method} ${r.path} → ${label} (got ${result.status})`);
  }

  if (jwtA) {
    const ownTarget = await httpRequest(`/api/target-list/${dealA.id}`, jwtA);
    report.httpResults.push({ path: `/api/target-list/${dealA.id}`, token: "A", status: ownTarget.status });
    ok(ownTarget.status === 200, `HTTP Owner A target-list own deal → 200 (got ${ownTarget.status})`);
  }
} else {
  skip("BATCH1_OWNER_B_JWT not set — HTTP cross-owner and listAll 403 checks skipped");
}

if (jwtAdmin) {
  const listAllAdmin = await httpRequest("/api/brand-deal-requests?all=1", jwtAdmin);
  report.httpResults.push({ path: "?all=1", token: "admin", status: listAllAdmin.status });
  ok(listAllAdmin.status === 200, `HTTP admin listAll → 200 (got ${listAllAdmin.status})`);
} else {
  skip("BATCH2A_ADMIN_JWT not set — HTTP admin listAll 200 check skipped (middleware admin pass confirmed above)");
}

console.log("\n=== Summary ===");
console.log(`Passed: ${passed}, Failed: ${failed}, Skipped: ${skipped}`);
console.log("\nReport JSON:");
console.log(JSON.stringify(report, null, 2));

process.exit(failed > 0 ? 1 : 0);
