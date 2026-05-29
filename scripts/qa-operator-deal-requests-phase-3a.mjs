#!/usr/bin/env node
/**
 * Phase 3A stabilization QA — owner ODR create, duplicate, terminal, activity log, auth.
 * Run: node scripts/qa-operator-deal-requests-phase-3a.mjs
 */
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  getOdrAirtableBase,
  findOdrRowsForDeal,
  ownerOutreachStatusLabel,
  isOdrStatusActive,
} from "../lib/dealality/odr-owner-create.js";
import { MAP_ODR_AIRTABLE } from "../api/operator-deal-requests-fields.js";
import {
  createMyDealsOperatorRequest,
  listMyDealsOperatorRequestsByDeals,
} from "../api/my-deals-operator-requests.js";
import { listOperatorDealRequests } from "../api/operator-deal-requests.js";
import { DEALS_TABLE } from "../api/schemas/deal-setup-fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const DEAL_ID = process.env.ODR_QA_DEAL_ID || "rec6JMTqtSUn1ygtd";
const MASTER_ID = "reciI2tYQBfMoMK9G";
const COMPANY = "GHL Hoteles (GHL Holding)";
const USER_ID = "rec1JNteylErKCkyj";
const ACTIVITY_LOG_TABLE =
  process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";

const results = [];

function record(name, ok, detail) {
  results.push({ name, ok, detail });
  console.log(`${ok ? "PASS" : "FAIL"}: ${name}${detail ? " — " + detail : ""}`);
  if (!ok) process.exitCode = 1;
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

// --- Static: auth wiring ---
const serverJs = readFileSync(join(root, "server.js"), "utf8");
const createRoute =
  'app.post("/api/my-deals/:recordId/operator-requests"';
const byDealsRoute =
  'app.post("/api/my-deals/operator-requests/by-deals"';

record(
  "owner create route requires memberstackAuth",
  serverJs.includes(createRoute) &&
    serverJs.indexOf("memberstackAuth") <
      serverJs.indexOf(createRoute) &&
    serverJs.includes("ownerOdrDealAuth"),
  "ownerOdrDealAuth stack"
);

record(
  "by-deals route requires memberstackAuth",
  serverJs.includes(byDealsRoute) && serverJs.includes("ownerOdrAuth"),
  "ownerOdrAuth stack"
);

record(
  "no POST /api/operator-deal-requests for owner create",
  !serverJs.includes('app.post("/api/operator-deal-requests"'),
  "operator workspace POST absent"
);

// --- UI label coverage ---
const statusCases = [
  [null, "Not contacted"],
  ["Sent / Awaiting Response", "Request sent"],
  ["Operator Viewed", "Viewed"],
  ["More Info Requested", "More info requested"],
  ["Accepted", "Accepted"],
  ["Declined", "Declined"],
  ["Archived", "Archived"],
  ["Pre-LOI", "Pre-LOI"],
];

for (const [raw, expected] of statusCases) {
  const label = ownerOutreachStatusLabel(raw);
  record(`UI label: ${expected}`, label === expected, `got "${label}"`);
}

const osJs = readFileSync(join(root, "public/js/operator-strategy-my-deals.js"), "utf8");
record(
  "UI merges odrActive for duplicate disable",
  osJs.includes("row.odrActive") && osJs.includes("refreshOperatorRequestsAndRender"),
  "contact + refresh"
);

if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
  console.log("\nSKIP: live Airtable tests (AIRTABLE_* not set)");
  printSummary();
  process.exit(process.exitCode || 0);
}

const base = getOdrAirtableBase();
let dealFields;
try {
  dealFields = (await base(DEALS_TABLE).find(DEAL_ID)).fields;
} catch (e) {
  record("load QA deal", false, e.message);
  printSummary();
  process.exit(1);
}

const adminUser = {
  isAdmin: true,
  isOwner: false,
  isBrand: false,
  isOperator: false,
  role: "admin",
};
const brandUser = { isBrand: true, isOwner: false, isOperator: false, isAdmin: false, role: "brand" };
const operatorUser = {
  isOperator: true,
  isOwner: false,
  isBrand: false,
  isAdmin: false,
  role: "operator",
  userRecordId: USER_ID,
};

async function countActivityLogsForDealSince(dealId, sinceMs) {
  const rows = await base(ACTIVITY_LOG_TABLE)
    .select({
      maxRecords: 50,
      sort: [{ field: "Created At", direction: "desc" }],
    })
    .all();
  const bufferMs = 3000;
  return rows.filter((r) => {
    const deals = r.fields?.Deal;
    const dealMatch = Array.isArray(deals)
      ? deals.includes(dealId)
      : deals === dealId;
    if (!dealMatch) return false;
    const created = r.fields?.["Created At"];
    const t = created ? new Date(created).getTime() : 0;
    if (t < sinceMs - bufferMs) return false;
    const action = String(r.fields?.Action || "");
    return /request sent|operator contact/i.test(action);
  }).length;
}

// Brand / operator 403
let brRes = mockRes();
await createMyDealsOperatorRequest(
  mockReq(brandUser, { recordId: DEAL_ID }, { operatorSetupId: MASTER_ID }),
  brRes
);
record("brand create 403", brRes._out.statusCode === 403);

let opRes = mockRes();
await createMyDealsOperatorRequest(
  mockReq(operatorUser, { recordId: DEAL_ID }, { operatorSetupId: MASTER_ID }),
  opRes
);
record("operator create 403", opRes._out.statusCode === 403);

// Row count before duplicate attempt
const beforeRows = await findOdrRowsForDeal(base, DEAL_ID);
const ghlBefore = beforeRows.filter((r) =>
  String(r.fields?.[MAP_ODR_AIRTABLE.operatingCompanyName] || "")
    .toLowerCase()
    .includes("ghl")
);
const countBefore = ghlBefore.length;

const since = Date.now();
const activityBefore = await countActivityLogsForDealSince(DEAL_ID, since);

// Active duplicate — should not create row or activity
let dupRes = mockRes();
await createMyDealsOperatorRequest(
  mockReq(adminUser, { recordId: DEAL_ID }, { operatorSetupId: MASTER_ID }, dealFields),
  dupRes
);
const countAfterDup = (
  await findOdrRowsForDeal(base, DEAL_ID)
).filter((r) =>
  String(r.fields?.[MAP_ODR_AIRTABLE.operatingCompanyName] || "")
    .toLowerCase()
    .includes("ghl")
).length;

record(
  "active duplicate returns alreadyExists",
  dupRes._out.statusCode === 200 &&
    dupRes._out.body?.alreadyExists === true &&
    dupRes._out.body?.created === false,
  JSON.stringify({ status: dupRes._out.statusCode, body: dupRes._out.body })
);

record(
  "active duplicate does not create Airtable row",
  countAfterDup === countBefore,
  `before=${countBefore} after=${countAfterDup}`
);

const activityAfterDup = await countActivityLogsForDealSince(DEAL_ID, since);
record(
  "duplicate return does not write activity log",
  activityAfterDup === activityBefore,
  `before=${activityBefore} after=${activityAfterDup}`
);

// Terminal re-outreach: archive latest active, then create new
const latest = ghlBefore[0] || beforeRows.find((r) =>
  String(r.fields?.[MAP_ODR_AIRTABLE.operatingCompanyName] || "")
    .toLowerCase()
    .includes("ghl")
);
if (latest && isOdrStatusActive(latest.fields?.[MAP_ODR_AIRTABLE.status])) {
  await base(MAP_ODR_AIRTABLE.table).update(latest.id, {
    [MAP_ODR_AIRTABLE.status]: "Declined",
  });
  record("terminal setup", true, `set ${latest.id} to Declined for re-outreach test`);
} else {
  record("terminal setup", !!latest, latest ? "row already terminal" : "no GHL row");
}

const countBeforeTerminal = (
  await findOdrRowsForDeal(base, DEAL_ID)
).filter((r) =>
  String(r.fields?.[MAP_ODR_AIRTABLE.operatingCompanyName] || "")
    .toLowerCase()
    .includes("ghl")
).length;

const sinceCreate = Date.now();
let termRes = mockRes();
await createMyDealsOperatorRequest(
  mockReq(
    adminUser,
    { recordId: DEAL_ID },
    { operatorSetupId: MASTER_ID, ownerNotes: "Phase 3A terminal re-outreach QA" },
    dealFields
  ),
  termRes
);

const countAfterTerminal = (
  await findOdrRowsForDeal(base, DEAL_ID)
).filter((r) =>
  String(r.fields?.[MAP_ODR_AIRTABLE.operatingCompanyName] || "")
    .toLowerCase()
    .includes("ghl")
).length;

record(
  "terminal status allows new create",
  termRes._out.statusCode === 201 && termRes._out.body?.created === true,
  `status=${termRes._out.statusCode}`
);

record(
  "terminal re-outreach creates new row",
  countAfterTerminal === countBeforeTerminal + 1,
  `before=${countBeforeTerminal} after=${countAfterTerminal}`
);

const activityAfterCreate = await countActivityLogsForDealSince(DEAL_ID, sinceCreate);
record(
  "successful create writes activity log",
  termRes._out.statusCode === 201 && activityAfterCreate >= 1,
  `new logs=${activityAfterCreate}`
);

// by-deals + operator list
let byRes = mockRes();
await listMyDealsOperatorRequestsByDeals(
  mockReq(adminUser, {}, { dealIds: [DEAL_ID] }),
  byRes
);
const contacted = byRes._out.body?.contacted || [];
const hit = contacted.find(
  (c) => c.dealId === DEAL_ID && c.operatingCompanyName?.toLowerCase().includes("ghl")
);
record(
  "by-deals returns owner-accessible ODR",
  byRes._out.statusCode === 200 && !!hit,
  hit ? `status=${hit.status} label=${hit.outreachStatusLabel}` : "missing"
);

let listRes = mockRes();
await listOperatorDealRequests(
  { dealalityUser: operatorUser, query: {}, operatorScope: null },
  listRes
);
const opFound = (listRes._out.body?.requests || []).some(
  (r) => r.dealId === DEAL_ID && r.operatingCompanyName?.toLowerCase().includes("ghl")
);
record(
  "operator My Operator Deals includes owner-created row",
  listRes._out.statusCode === 200 && opFound,
  opFound ? "scoped list ok" : "not found — check Users→Master link"
);

function printSummary() {
  const passed = results.filter((r) => r.ok).length;
  const failed = results.filter((r) => !r.ok).length;
  console.log(`\nPhase 3A QA: ${passed} passed, ${failed} failed`);
}

printSummary();
