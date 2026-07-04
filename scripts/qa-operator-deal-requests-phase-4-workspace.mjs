#!/usr/bin/env node
/**
 * Phase 4 — My Operator Deals workspace QA.
 * Run: node scripts/qa-operator-deal-requests-phase-4-workspace.mjs
 */
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  getOperatorDealMetaBatch,
  getOperatorDealActivity,
  listOperatorDealRequests,
  updateOperatorDealRequest,
} from "../api/operator-deal-requests.js";
import { requireOperatorDealsAccess } from "../middleware/requireOperatorDealsAccess.js";
import { deriveOperatorNextAction } from "../lib/deal-workspace-pipeline.js";
import { enrichWorkspaceRow } from "../lib/deal-workspace-pipeline.js";

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

function mockReq(user, params, query, body) {
  return {
    params: params || {},
    query: query || {},
    body: body || {},
    dealalityUser: user,
  };
}

const serverJs = readFileSync(join(root, "server.js"), "utf8");
const oddJs = readFileSync(join(root, "public/operator-development-dashboard.js"), "utf8");
const oddHtml = readFileSync(join(root, "public/operator-development-dashboard.html"), "utf8");

if (serverJs.includes('app.get("/api/operator-deal-requests/deal-meta"')) {
  pass("deal-meta route registered");
} else fail("missing GET /api/operator-deal-requests/deal-meta");

if (oddHtml.includes("oddTableBody") && oddHtml.includes("odd-workspace-table")) {
  pass("workspace table scaffold in HTML");
} else fail("missing table HTML");

const uiChecks = [
  "renderWorkspaceTable",
  "loadDealMeta",
  "operator-deal-requests/deal-meta",
  "Operator Viewed",
  "More Info Requested",
  "patchRequest",
  "oddModalBackdrop",
];
for (const c of uiChecks) {
  if (oddJs.includes(c)) pass("UI wiring: " + c);
  else fail("UI missing: " + c);
}

if (typeof deriveOperatorNextAction === "function") {
  const label = deriveOperatorNextAction({ status: "New" });
  if (label === "Review owner request") pass("deriveOperatorNextAction");
  else fail("deriveOperatorNextAction wrong label: " + label);
} else fail("deriveOperatorNextAction missing");

for (const [role, user, expectNext] of [
  ["operator", { isOperator: true, role: "operator" }, true],
  ["admin", { isAdmin: true, role: "admin" }, true],
  ["owner", { isOwner: true, role: "owner" }, false],
  ["brand", { isBrand: true, role: "brand" }, false],
]) {
  let nextCalled = false;
  const res = mockRes();
  requireOperatorDealsAccess(mockReq(user), res, () => {
    nextCalled = true;
  });
  if (nextCalled === expectNext) pass("requireOperatorDealsAccess " + role);
  else fail("requireOperatorDealsAccess " + role);
}

const OPERATOR_USER = {
  isOperator: true,
  isOwner: false,
  isBrand: false,
  isAdmin: false,
  role: "operator",
  userRecordId: "rec1JNteylErKCkyj",
};

const ADMIN_USER = {
  isAdmin: true,
  isOperator: false,
  isOwner: false,
  isBrand: false,
  role: "admin",
};

async function liveTests() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.log("SKIP: live tests (AIRTABLE_* not set)");
    return;
  }

  let listRes = mockRes();
  await listOperatorDealRequests(mockReq(OPERATOR_USER, {}, {}), listRes);
  if (listRes._out.statusCode !== 200) {
    fail("operator list failed");
    return;
  }
  const requests = listRes._out.body?.requests || [];
  if (requests.length) pass("operator list has " + requests.length + " row(s)");
  else console.log("WARN: no ODR rows for live table tests");

  const dealId = requests[0]?.dealId;
  const requestId = requests[0]?.id;

  if (dealId) {
    let metaRes = mockRes();
    await getOperatorDealMetaBatch(mockReq(OPERATOR_USER, {}, { ids: dealId }), metaRes);
    if (metaRes._out.statusCode === 200 && metaRes._out.body?.deals?.length >= 1) {
      pass("deal-meta returns scoped deal");
    } else fail("deal-meta scoped fetch");

    let leakRes = mockRes();
    await getOperatorDealMetaBatch(mockReq(OPERATOR_USER, {}, { ids: "recXXXXXXXXXXXXXX" }), leakRes);
    const leaked = (leakRes._out.body?.deals || []).filter((d) => d.dealId === "recXXXXXXXXXXXXXX");
    if (leakRes._out.statusCode === 200 && leaked.length === 0) {
      pass("deal-meta does not leak unrelated deal id");
    } else fail("deal-meta leak check");

    let actRes = mockRes();
    await getOperatorDealActivity(mockReq(OPERATOR_USER, {}, { dealIds: dealId }), actRes);
    if (actRes._out.statusCode === 200) {
      const withName = (actRes._out.body?.entries || []).some((e) => e.dealId === dealId);
      if (withName) pass("activity log returns entries for deal");
      const named = (actRes._out.body?.entries || []).find((e) => e.dealId === dealId && e.dealName);
      if (named) pass("activity log includes dealName when meta available");
      else console.log("WARN: activity dealName not set (deal meta may be empty)");
    } else fail("activity fetch");
  }

  if (requestId && dealId) {
    const bucketBefore = enrichWorkspaceRow(requests[0]).workspaceBucket;

    let viewRes = mockRes();
    await updateOperatorDealRequest(
      mockReq(OPERATOR_USER, { requestId }, {}, { status: "Operator Viewed" }),
      viewRes
    );
    if (viewRes._out.statusCode === 200 && viewRes._out.body?.request?.status === "Operator Viewed") {
      pass("PATCH Mark viewed");
    } else fail("PATCH Mark viewed");

    let infoRes = mockRes();
    await updateOperatorDealRequest(
      mockReq(OPERATOR_USER, { requestId }, {}, {
        status: "More Info Requested",
        responseNotes: "Phase 4 QA — please clarify project timeline.",
      }),
      infoRes
    );
    if (infoRes._out.statusCode === 200 && infoRes._out.body?.request?.status === "More Info Requested") {
      pass("PATCH Request more info");
    } else fail("PATCH Request more info");

    let intRes = mockRes();
    await updateOperatorDealRequest(
      mockReq(OPERATOR_USER, { requestId }, {}, { status: "Accepted" }),
      intRes
    );
    if (intRes._out.statusCode === 200 && intRes._out.body?.request?.status === "Accepted") {
      pass("PATCH Interested");
    } else fail("PATCH Interested");

    let notesRes = mockRes();
    await updateOperatorDealRequest(
      mockReq(OPERATOR_USER, { requestId }, {}, {
        operatorInternalNotes: "Phase 4 QA internal note",
        nextFollowupDate: "2026-06-15",
        nextFollowupHeader: "Follow up with owner",
        nextFollowupNotes: "Confirm data gaps on operating model.",
        scheduledBy: "operator",
      }),
      notesRes
    );
    if (notesRes._out.statusCode === 200 && notesRes._out.body?.request?.operatorInternalNotes) {
      pass("PATCH notes/follow-up");
    } else fail("PATCH notes/follow-up");

    let revRes = mockRes();
    await updateOperatorDealRequest(
      mockReq(OPERATOR_USER, { requestId }, {}, { status: "Revisit Later" }),
      revRes
    );
    if (revRes._out.statusCode === 200 && revRes._out.body?.request?.status === "Revisit Later") {
      pass("PATCH Revisit later");
    } else fail("PATCH Revisit later");

    let decRes = mockRes();
    await updateOperatorDealRequest(
      mockReq(OPERATOR_USER, { requestId }, {}, {
        status: "Declined",
        responseNotes: "Phase 4 QA decline",
      }),
      decRes
    );
    if (decRes._out.statusCode === 200 && decRes._out.body?.request?.status === "Declined") {
      pass("PATCH Decline");
    } else fail("PATCH Decline");

    const enriched = enrichWorkspaceRow(decRes._out.body.request);
    if (enriched.workspaceBucket === "archived") pass("declined row maps to archived bucket");
    else fail("bucket after decline");

    if (bucketBefore) pass("tab bucket helper available (was " + bucketBefore + ")");
  }

  let adminMeta = mockRes();
  await getOperatorDealMetaBatch(mockReq(ADMIN_USER, {}, { ids: dealId || "rec6JMTqtSUn1ygtd" }), adminMeta);
  if (adminMeta._out.statusCode === 200) pass("admin deal-meta access");
  else fail("admin deal-meta");
}

await liveTests();

if (!process.exitCode) {
  console.log("\nPhase 4 operator workspace validation passed.");
}
