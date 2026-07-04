#!/usr/bin/env node
/**
 * Phase 4A — demo-readiness: clean GHL demo ODR, activity Action options, workflow QA.
 * Run: node scripts/qa-operator-deal-requests-phase-4a-demo.mjs
 * Apply Airtable schema: node scripts/qa-operator-deal-requests-phase-4a-demo.mjs --apply-schema
 */
import "../load-env.js";
import {
  getOdrAirtableBase,
  findOdrRowsForDeal,
  createOdrRow,
  ODR_DEFAULT_CREATE_STATUS,
} from "../lib/dealality/odr-owner-create.js";
import { MAP_ODR_AIRTABLE } from "../api/operator-deal-requests-fields.js";
import {
  listOperatorDealRequests,
  updateOperatorDealRequest,
  getOperatorDealActivity,
} from "../api/operator-deal-requests.js";
import { requireOperatorDealsAccess } from "../middleware/requireOperatorDealsAccess.js";
import { enrichWorkspaceRow } from "../lib/deal-workspace-pipeline.js";

const APPLY_SCHEMA = process.argv.includes("--apply-schema");
const DEAL_ID = process.env.ODR_DEMO_DEAL_ID || "rec6JMTqtSUn1ygtd";
const MASTER_ID = "reciI2tYQBfMoMK9G";
const COMPANY = "GHL Hoteles (GHL Holding)";
const ACTIVITY_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";

const REQUIRED_ACTIVITY_ACTIONS = [
  "Request Sent",
  "Opportunity reviewed",
  "Information requested",
  "Marked interested",
  "Declined",
  "Revisit Later",
  "Notes updated",
  "Follow-up updated",
  "Follow-up scheduled",
  "Operator Viewed",
  "Viewed",
  "Operator contacted",
];

const OPERATOR_USER = {
  isOperator: true,
  isOwner: false,
  isBrand: false,
  isAdmin: false,
  role: "operator",
  userRecordId: "rec1JNteylErKCkyj",
};

const DEMO_FIELDS = {
  alignmentScore: 72,
  alignmentBand: "Moderate",
  dataConfidence: "Operator-provided",
  ownerNotes: "Demo owner request — review alignment signals and data gaps for this operating opportunity.",
};

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
  return { params: params || {}, query: query || {}, body: body || {}, dealalityUser: user };
}

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

async function ensureActivityActionOptions(token, baseId) {
  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${listRes.status}`);

  const activityTable = (listJson.tables || []).find((t) => t.name === ACTIVITY_TABLE);
  if (!activityTable) throw new Error(`Activity table not found: ${ACTIVITY_TABLE}`);

  const actionField = (activityTable.fields || []).find((f) => f.name === "Action");
  if (!actionField) {
    console.log("WARN: Action field missing on Deal Activity Log — activity may use free text elsewhere");
    return { skipped: true };
  }

  if (actionField.type !== "singleSelect") {
    console.log("INFO: Action field is not singleSelect — option ensure skipped");
    return { skipped: true, type: actionField.type };
  }

  const existingChoices = actionField.options?.choices || [];
  const byName = new Map(existingChoices.map((c) => [c.name, c]));
  const mergedNames = existingChoices.map((c) => c.name);
  for (const name of REQUIRED_ACTIVITY_ACTIONS) {
    if (!mergedNames.includes(name)) mergedNames.push(name);
  }

  const missing = REQUIRED_ACTIVITY_ACTIONS.filter((n) => !byName.has(n));
  if (!missing.length) {
    pass("activity Action select options already include Phase 4 labels");
    return { updated: false, choices: mergedNames.length };
  }

  console.log("Missing Action options:", missing.join(", "));

  if (!APPLY_SCHEMA) {
    console.log("Re-run with --apply-schema to merge Action select options");
    return { dryRun: true, missing };
  }

  const choicesPayload = mergedNames.map((name) => {
    const ex = byName.get(name);
    if (ex) return { id: ex.id, name: ex.name, ...(ex.color ? { color: ex.color } : {}) };
    return { name };
  });

  const { res: patchRes, json: patchJson } = await metaFetch(
    baseId,
    token,
    `/tables/${activityTable.id}/fields/${actionField.id}`,
    {
      method: "PATCH",
      body: JSON.stringify({
        options: { choices: choicesPayload },
      }),
    },
  );

  if (!patchRes.ok) {
    console.warn(
      "WARN: Could not patch Action field via Meta API (" +
        patchRes.status +
        "). Add options manually in Airtable or grant schema.bases:write. Fallback labels will be used.",
    );
    return { updated: false, missing, patchError: patchRes.status };
  }

  pass("activity Action select options updated (" + missing.length + " added)");
  return { updated: true, added: missing };
}

async function archiveGhlRows(base) {
  const rows = await findOdrRowsForDeal(base, DEAL_ID);
  const ghlRows = rows.filter((r) =>
    String(r.fields?.[MAP_ODR_AIRTABLE.operatingCompanyName] || "")
      .trim()
      .toLowerCase() === COMPANY.toLowerCase(),
  );
  const active = ghlRows.filter(
    (r) => String(r.fields?.[MAP_ODR_AIRTABLE.status] || "") !== "Archived",
  );
  if (!active.length) return { archived: 0 };

  for (let i = 0; i < active.length; i += 10) {
    const batch = active.slice(i, i + 10).map((r) => ({
      id: r.id,
      fields: { [MAP_ODR_AIRTABLE.status]: "Archived" },
    }));
    await base(MAP_ODR_AIRTABLE.table).update(batch);
  }
  return { archived: active.length };
}

async function createDemoRow(base) {
  const now = new Date().toISOString();
  const futureFollowUp = new Date();
  futureFollowUp.setDate(futureFollowUp.getDate() + 30);
  const followUpIso = futureFollowUp.toISOString().slice(0, 10);

  const record = await createOdrRow(base, {
    dealId: DEAL_ID,
    operatingCompanyName: COMPANY,
    operatorSetupId: MASTER_ID,
    status: ODR_DEFAULT_CREATE_STATUS,
    alignmentScore: DEMO_FIELDS.alignmentScore,
    alignmentBand: DEMO_FIELDS.alignmentBand,
    dataConfidence: DEMO_FIELDS.dataConfidence,
    ownerNotes: DEMO_FIELDS.ownerNotes,
  });

  await base(MAP_ODR_AIRTABLE.table).update(record.id, {
    [MAP_ODR_AIRTABLE.nextFollowupDate]: followUpIso,
    [MAP_ODR_AIRTABLE.responseNotes]: "",
    [MAP_ODR_AIRTABLE.requestSentAt]: now,
    [MAP_ODR_AIRTABLE.lastUpdated]: now,
  });

  return record.id;
}

async function resetDemoRow(base, requestId) {
  const now = new Date().toISOString();
  const futureFollowUp = new Date();
  futureFollowUp.setDate(futureFollowUp.getDate() + 30);
  await base(MAP_ODR_AIRTABLE.table).update(requestId, {
    [MAP_ODR_AIRTABLE.status]: ODR_DEFAULT_CREATE_STATUS,
    [MAP_ODR_AIRTABLE.alignmentScore]: DEMO_FIELDS.alignmentScore,
    [MAP_ODR_AIRTABLE.alignmentBand]: DEMO_FIELDS.alignmentBand,
    [MAP_ODR_AIRTABLE.dataConfidence]: DEMO_FIELDS.dataConfidence,
    [MAP_ODR_AIRTABLE.ownerNotes]: DEMO_FIELDS.ownerNotes,
    [MAP_ODR_AIRTABLE.responseNotes]: "",
    [MAP_ODR_AIRTABLE.responseDate]: null,
    [MAP_ODR_AIRTABLE.requestSentAt]: now,
    [MAP_ODR_AIRTABLE.lastUpdated]: now,
    [MAP_ODR_AIRTABLE.nextFollowupDate]: futureFollowUp.toISOString().slice(0, 10),
    [MAP_ODR_AIRTABLE.nextFollowupHeader]: "",
    [MAP_ODR_AIRTABLE.nextFollowupNotesExternal]: null,
  });
}

async function fetchLatestActivity(dealId) {
  const res = mockRes();
  await getOperatorDealActivity(
    mockReq(OPERATOR_USER, {}, { dealIds: dealId, operator: COMPANY }),
    res,
  );
  return (res._out.body?.entries || [])[0] || null;
}

async function patchAndExpect(requestId, body, expectStatus, expectBucket) {
  const res = mockRes();
  await updateOperatorDealRequest(mockReq(OPERATOR_USER, { requestId }, {}, body), res);
  if (res._out.statusCode !== 200 || res._out.body?.request?.status !== expectStatus) {
    fail("PATCH " + expectStatus + " got " + res._out.statusCode + " " + JSON.stringify(res._out.body));
    return null;
  }
  const row = res._out.body.request;
  const bucket = enrichWorkspaceRow(row).workspaceBucket;
  if (expectBucket && bucket !== expectBucket) {
    fail("bucket for " + expectStatus + " expected " + expectBucket + " got " + bucket);
  } else {
    pass("PATCH " + expectStatus + " → bucket " + bucket);
  }
  return row;
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.log("SKIP: AIRTABLE_* not set");
    return;
  }

  await ensureActivityActionOptions(process.env.AIRTABLE_API_KEY, process.env.AIRTABLE_BASE_ID);

  const base = getOdrAirtableBase();
  const archived = await archiveGhlRows(base);
  console.log("Archived " + archived.archived + " prior GHL row(s) for demo reset");

  const demoId = await createDemoRow(base);
  pass("demo ODR created: " + demoId);

  let listRes = mockRes();
  await listOperatorDealRequests(mockReq(OPERATOR_USER, {}, {}), listRes);
  const demoRow = (listRes._out.body?.requests || []).find((r) => r.id === demoId);
  if (!demoRow) fail("demo row not in operator list");
  else pass("demo row visible to operator list");

  const newBucket = enrichWorkspaceRow(demoRow).workspaceBucket;
  if (newBucket === "new") pass("demo row in New opportunities bucket");
  else fail("demo row bucket expected new, got " + newBucket);

  if (demoRow.alignmentScore === DEMO_FIELDS.alignmentScore) pass("alignment score populated");
  else fail("alignment score missing");

  if (demoRow.alignmentBand === DEMO_FIELDS.alignmentBand) pass("alignment band populated");
  else fail("alignment band missing");

  if (demoRow.dataConfidence === DEMO_FIELDS.dataConfidence) pass("data confidence populated");
  else fail("data confidence missing");

  await patchAndExpect(demoId, { status: "Operator Viewed" }, "Operator Viewed", "active-review");
  const actView = await fetchLatestActivity(DEAL_ID);
  if (actView && /opportunity reviewed|brand viewed|operator viewed|notes updated|request sent/i.test(actView.action || "")) {
    pass("activity log: Mark viewed (or fallback)");
  } else fail("activity log missing viewed action");

  await patchAndExpect(
    demoId,
    { status: "More Info Requested", responseNotes: "Demo — please clarify opening timeline." },
    "More Info Requested",
    "awaiting-info",
  );
  const actInfo = await fetchLatestActivity(DEAL_ID);
  if (actInfo && /information requested|notes updated|other/i.test(actInfo.action || "")) {
    pass("activity log: Information requested (or fallback)");
  } else fail("activity log missing Information requested (got " + (actInfo?.action || "none") + ")");

  await patchAndExpect(demoId, { status: "Accepted" }, "Accepted", "awaiting-info");
  const actInt = await fetchLatestActivity(DEAL_ID);
  if (actInt && /marked interested|accepted/i.test(actInt.action || "")) {
    pass("activity log: Marked interested (or Accepted fallback)");
  } else fail("activity log missing Marked interested");

  await patchAndExpect(
    demoId,
    {
      operatorInternalNotes: "Demo internal note for operator team.",
      nextFollowupDate: "2026-07-15",
      nextFollowupHeader: "Owner follow-up",
      nextFollowupNotes: "Confirm data gaps on operating model.",
      scheduledBy: "operator",
    },
    "Accepted",
    "awaiting-info",
  );
  const actNotes = await fetchLatestActivity(DEAL_ID);
  if (actNotes && /follow-up|notes updated/i.test(actNotes.action || "")) {
    pass("activity log: Notes/follow-up action written");
  } else fail("activity log missing follow-up/notes action");

  await patchAndExpect(demoId, { status: "Revisit Later" }, "Revisit Later", "advanced");
  const actRev = await fetchLatestActivity(DEAL_ID);
  if (actRev && /revisit|other|notes updated/i.test(actRev.action || "")) {
    pass("activity log: Revisit Later (or fallback)");
  } else fail("activity log missing Revisit Later");

  await patchAndExpect(
    demoId,
    { status: "Declined", responseNotes: "Demo decline — not pursuing at this time." },
    "Declined",
    "archived",
  );
  const actDec = await fetchLatestActivity(DEAL_ID);
  if (actDec && /declined/i.test(actDec.action || "")) {
    pass("activity log: Declined");
  } else fail("activity log missing Declined");

  await resetDemoRow(base, demoId);
  pass("demo row reset to Sent / Awaiting Response for live demo");

  listRes = mockRes();
  await listOperatorDealRequests(mockReq(OPERATOR_USER, {}, {}), listRes);
  const resetRow = (listRes._out.body?.requests || []).find((r) => r.id === demoId);
  if (resetRow && resetRow.status === ODR_DEFAULT_CREATE_STATUS) {
    pass("final demo state: " + ODR_DEFAULT_CREATE_STATUS);
  } else fail("final demo reset failed");

  for (const [role, user, expectNext] of [
    ["owner", { isOwner: true, role: "owner" }, false],
    ["brand", { isBrand: true, role: "brand" }, false],
  ]) {
    let nextCalled = false;
    const res = mockRes();
    requireOperatorDealsAccess(mockReq(user), res, () => {
      nextCalled = true;
    });
    if (nextCalled === expectNext) pass("403 gate: " + role);
    else fail("403 gate: " + role);
  }

  console.log("\nDemo record id:", demoId);
  console.log("Deal:", DEAL_ID, "| Company:", COMPANY);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
