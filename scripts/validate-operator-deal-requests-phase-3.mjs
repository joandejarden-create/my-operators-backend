#!/usr/bin/env node
/**
 * Phase 3 validation — owner-side Operator Deal Request creation.
 * Run: node scripts/validate-operator-deal-requests-phase-3.mjs
 */
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  isOdrStatusActive,
  ownerOutreachStatusLabel,
  ODR_DEFAULT_CREATE_STATUS,
  getOdrAirtableBase,
} from "../lib/dealality/odr-owner-create.js";
import { DEALS_TABLE } from "../api/schemas/deal-setup-fields.js";
import {
  operatingCompanyNamesMatch,
} from "../lib/dealality/resolve-operator-scope.js";
import { requireOwnerOdrCreateAccess } from "../middleware/requireOwnerOdrCreateAccess.js";
import {
  createMyDealsOperatorRequest,
  listMyDealsOperatorRequestsByDeals,
} from "../api/my-deals-operator-requests.js";
import { listOperatorDealRequests } from "../api/operator-deal-requests.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("PASS:", msg);
}

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    status(code) {
      out.statusCode = code;
      return this;
    },
    json(payload) {
      out.body = payload;
      return out;
    },
    _out: out,
  };
}

function mockReq(user, params, body, dealFields) {
  return {
    params: params || {},
    body: body || {},
    dealalityUser: user,
    dealRecordFields: dealFields,
  };
}

// Static wiring
const serverJs = readFileSync(join(root, "server.js"), "utf8");
const osJs = readFileSync(join(root, "public/js/operator-strategy-my-deals.js"), "utf8");

if (serverJs.includes('app.post("/api/my-deals/:recordId/operator-requests"')) {
  pass("create route registered under my-deals");
} else fail("missing POST /api/my-deals/:recordId/operator-requests");

if (serverJs.includes('app.post("/api/my-deals/operator-requests/by-deals"')) {
  pass("by-deals route registered under my-deals");
} else fail("missing POST /api/my-deals/operator-requests/by-deals");

if (serverJs.includes("requireOwnerOdrCreateAccess") && serverJs.includes("ownerOdrDealAuth")) {
  pass("owner ODR auth stack wired");
} else fail("owner ODR auth stack missing");

if (!serverJs.includes('app.post("/api/operator-deal-requests"')) {
  pass("no unauthenticated POST on operator-deal-requests");
} else fail("POST /api/operator-deal-requests should not exist for owner create");

// Middleware role gates
for (const [role, user, expectNext] of [
  ["owner", { isOwner: true, role: "owner" }, true],
  ["admin", { isAdmin: true, role: "admin" }, true],
  ["brand", { isBrand: true, role: "brand" }, false],
  ["operator", { isOperator: true, role: "operator" }, false],
]) {
  let nextCalled = false;
  const res = mockRes();
  requireOwnerOdrCreateAccess(mockReq(user), res, () => {
    nextCalled = true;
  });
  if (nextCalled === expectNext) pass(`requireOwnerOdrCreateAccess ${role}`);
  else fail(`requireOwnerOdrCreateAccess ${role} expected next=${expectNext}`);
}

// Status helpers
if (ODR_DEFAULT_CREATE_STATUS === "Sent / Awaiting Response") {
  pass("default create status");
} else fail("wrong default create status");

if (isOdrStatusActive("Sent / Awaiting Response") && !isOdrStatusActive("Declined")) {
  pass("active vs terminal status detection");
} else fail("status active/terminal logic");

if (ownerOutreachStatusLabel("Operator Viewed") === "Viewed") {
  pass("owner outreach label mapping");
} else fail("owner outreach label mapping");

if (operatingCompanyNamesMatch("GHL Hoteles", "ghl hoteles")) {
  pass("case-insensitive company match");
} else fail("company name normalization");

// UI wiring
const uiChecks = [
  "operator-requests/by-deals",
  "operator-requests",
  "outreachStatusLabel",
  "Contact Operator",
  "odrActive",
];
for (const c of uiChecks) {
  if (osJs.includes(c)) pass(`operator-strategy UI: ${c}`);
  else fail(`operator-strategy UI missing: ${c}`);
}

// Live API tests (when env configured)
const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;

async function liveTests() {
  if (!apiKey || !baseId) {
    console.log("SKIP: live tests (AIRTABLE_* not set)");
    return;
  }

  const USER_ID = "rec1JNteylErKCkyj";
  const MASTER_ID = "reciI2tYQBfMoMK9G";
  const COMPANY = "GHL Hoteles (GHL Holding)";

  const base = getOdrAirtableBase();
  const deals = await base(DEALS_TABLE).select({ maxRecords: 1 }).all();
  if (!deals.length) {
    console.log("SKIP: no deals for live tests");
    return;
  }
  const dealId = deals[0].id;

  const adminUser = {
    isOwner: false,
    isAdmin: true,
    isBrand: false,
    isOperator: false,
    role: "admin",
  };

  const operatorUser = {
    isOwner: false,
    isAdmin: false,
    isBrand: false,
    isOperator: true,
    role: "operator",
    userRecordId: USER_ID,
  };

  const brandUser = { isBrand: true, isOwner: false, isOperator: false, isAdmin: false, role: "brand" };

  // Operator create forbidden
  let opRes = mockRes();
  await createMyDealsOperatorRequest(
    mockReq(operatorUser, { recordId: dealId }, { operatorSetupId: MASTER_ID }),
    opRes
  );
  if (opRes._out.statusCode === 403) pass("live: operator create 403");
  else fail("live: operator create should 403");

  // Brand create forbidden
  let brRes = mockRes();
  await createMyDealsOperatorRequest(
    mockReq(brandUser, { recordId: dealId }, { operatorSetupId: MASTER_ID }),
    brRes
  );
  if (brRes._out.statusCode === 403) pass("live: brand create 403");
  else fail("live: brand create should 403");

  // Admin create (may duplicate)
  let crRes = mockRes();
  await createMyDealsOperatorRequest(
    mockReq(
      adminUser,
      { recordId: dealId },
      {
        operatorSetupId: MASTER_ID,
        alignmentScore: 55,
        alignmentBand: "Moderate",
        dataConfidence: "Operator-provided",
        ownerNotes: "Phase 3 validation",
      },
      deals[0].fields
    ),
    crRes
  );
  if ([200, 201].includes(crRes._out.statusCode) && crRes._out.body?.success) {
    pass("live: admin create or duplicate return");
  } else {
    fail("live: admin create failed " + crRes._out.statusCode + " " + JSON.stringify(crRes._out.body));
  }

  const requestId = crRes._out.body?.requestId;
  if (crRes._out.body?.alreadyExists) pass("live: active duplicate returns existing");
  if (crRes._out.body?.created === false && crRes._out.body?.alreadyExists) {
    pass("live: duplicate created=false");
  }

  // Second create duplicate
  let dupRes = mockRes();
  await createMyDealsOperatorRequest(
    mockReq(adminUser, { recordId: dealId }, { operatorSetupId: MASTER_ID }, deals[0].fields),
    dupRes
  );
  if (dupRes._out.statusCode === 200 && dupRes._out.body?.alreadyExists && dupRes._out.body?.created === false) {
    pass("live: second create returns active duplicate");
  } else fail("live: duplicate prevention");

  // by-deals owner read
  let byRes = mockRes();
  await listMyDealsOperatorRequestsByDeals(
    mockReq(adminUser, {}, { dealIds: [dealId] }),
    byRes
  );
  if (byRes._out.statusCode === 200 && Array.isArray(byRes._out.body?.contacted)) {
    pass("live: by-deals returns contacted array");
    const hit = byRes._out.body.contacted.find((c) => c.dealId === dealId);
    if (hit && operatingCompanyNamesMatch(hit.operatingCompanyName, COMPANY)) {
      pass("live: by-deals includes seeded company");
    }
  } else fail("live: by-deals failed");

  // Operator scoped list sees row
  let listRes = mockRes();
  await listOperatorDealRequests(
    { dealalityUser: operatorUser, query: {}, operatorScope: null },
    listRes
  );
  if (listRes._out.statusCode === 200) {
    const found = (listRes._out.body?.requests || []).some(
      (r) => r.dealId === dealId && operatingCompanyNamesMatch(r.operatingCompanyName, COMPANY)
    );
    if (found) pass("live: operator workspace list includes ODR");
    else fail("live: operator list missing ODR (check user Master link)");
  } else fail("live: operator list failed");

  console.log("Live test requestId:", requestId || "(duplicate)");
}

await liveTests();

if (!process.exitCode) {
  console.log("\nPhase 3 operator deal requests validation passed.");
}
