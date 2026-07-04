#!/usr/bin/env node
/**
 * Live Airtable + API QA for My Operator Deals Phase 2.
 * Run: node scripts/qa-operator-deal-requests-phase-2-live.mjs
 */
import Airtable from "airtable";
import "../load-env.js";
import { MAP_ODR_AIRTABLE } from "../api/operator-deal-requests-fields.js";
import { MAP_OPERATOR_SCOPE, resolveOperatorScope } from "../lib/dealality/resolve-operator-scope.js";
import {
  listOperatorDealRequests,
  getOperatorDealRequestById,
  updateOperatorDealRequest,
  bulkUpdateOperatorDealRequests,
} from "../api/operator-deal-requests.js";
import { INTAKE_USERS_TABLE } from "../api/schemas/intake-deal-fields.js";

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;

const USERS_TABLE = process.env.AIRTABLE_ME_USERS_TABLE || INTAKE_USERS_TABLE;
const ODR_TABLE = MAP_ODR_AIRTABLE.table;
const ACTIVITY_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";
const MASTER_TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

const EXPECTED = {
  Users: {
    table: USERS_TABLE,
    fields: {
      [process.env.AIRTABLE_ME_USERS_OPERATOR_SETUP_LINK || "Operator Setup - Master"]: {
        types: ["multipleRecordLinks", "singleRecordLink"],
      },
    },
  },
  "Operator Setup - Master": {
    table: MASTER_TABLE,
    fields: {
      [process.env.AIRTABLE_OPERATOR_COMPANY_NAME_FIELD || "company_name"]: {
        types: ["singleLineText", "multilineText", "richText", "formula"],
      },
      [process.env.AIRTABLE_OPERATOR_SETUP_SUBMISSION_STATUS_FIELD || "submission_status"]: {
        types: ["singleSelect"],
      },
    },
  },
  "Operator Deal Requests": {
    table: ODR_TABLE,
    fields: {
      Deal: { types: ["multipleRecordLinks", "singleRecordLink"] },
      "Operating Company Name": { types: ["singleLineText", "multilineText"] },
      "Operator Setup": { types: ["multipleRecordLinks", "singleRecordLink"] },
      Status: { types: ["singleSelect"] },
      "Alignment Score": { types: ["number", "percent", "currency", "rating"] },
      "Alignment Band": { types: ["singleSelect"] },
      "Data Confidence": { types: ["singleSelect"] },
      "Request Sent At": { types: ["dateTime", "createdTime", "lastModifiedTime"] },
      "Response Date": { types: ["dateTime", "date"] },
      "Response Notes": { types: ["multilineText", "richText", "singleLineText"] },
      "Created At": { types: ["dateTime", "createdTime"] },
      "Last Updated": { types: ["dateTime", "lastModifiedTime"] },
      "Owner Notes": { types: ["multilineText", "richText", "singleLineText"] },
      "Next Follow-up Notes (Internal)": { types: ["multilineText", "richText", "singleLineText"] },
      "Next Follow-up Notes (External)": { types: ["multilineText", "richText", "singleLineText"] },
      "Next Follow-up Date": { types: ["date", "dateTime"] },
      "Next Follow-up Header": { types: ["singleLineText", "multilineText"] },
      "NDA Required?": { types: ["checkbox"] },
      "NDA Status": { types: ["singleSelect"] },
      "Deal Room Access": { types: ["singleSelect"] },
    },
  },
  "Deal Activity Log": {
    table: ACTIVITY_TABLE,
    fields: {
      "Operating Company Name": { types: ["singleLineText", "multilineText"] },
      Stakeholder: { types: ["singleSelect"], mustIncludeOptions: ["Operator"] },
    },
  },
};

const ODR_STATUS_MIN = ["New", "Operator Viewed", "Viewed"];

const results = {
  tables: {},
  missingFields: [],
  typeMismatches: [],
  selectMissing: [],
  mePermissions: null,
  apiTests: {},
  fixesApplied: [],
};

function failTable(name, reason) {
  results.tables[name] = { pass: false, reason };
}

function passTable(name, detail) {
  results.tables[name] = { pass: true, detail };
}

async function fetchMetaTables() {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error?.message || `Meta API ${res.status}`);
  return json.tables || [];
}

function resolveTable(metaTables, tableRef) {
  return metaTables.find((t) => t.name === tableRef || t.id === tableRef) || null;
}

function fieldMap(table) {
  const m = new Map();
  for (const f of table?.fields || []) m.set(f.name, f);
  return m;
}

function validateSchema(metaTables) {
  for (const [label, spec] of Object.entries(EXPECTED)) {
    const table = resolveTable(metaTables, spec.table);
    if (!table) {
      failTable(label, `Table not found: ${spec.table}`);
      for (const fn of Object.keys(spec.fields)) results.missingFields.push(`${label}::${fn} (table missing)`);
      continue;
    }
    const fmap = fieldMap(table);
    let tableOk = true;
    for (const [fname, rule] of Object.entries(spec.fields)) {
      const field = fmap.get(fname);
      if (!field) {
        tableOk = false;
        results.missingFields.push(`${label}::${fname}`);
        continue;
      }
      if (rule.types && !rule.types.includes(field.type)) {
        tableOk = false;
        results.typeMismatches.push({
          table: label,
          field: fname,
          expected: rule.types,
          actual: field.type,
        });
      }
      if (rule.mustIncludeOptions?.length && field.type === "singleSelect") {
        const opts = (field.options?.choices || []).map((c) => c.name);
        for (const need of rule.mustIncludeOptions) {
          if (!opts.includes(need)) {
            tableOk = false;
            results.selectMissing.push({ table: label, field: fname, missingOption: need, have: opts });
          }
        }
      }
    }
    if (tableOk) passTable(label, `${table.name} (${table.id}) — ${Object.keys(spec.fields).length} fields OK`);
    else failTable(label, "See missing/type/option issues");
  }
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

async function findOperatorTestUser(base) {
  const linkField = MAP_OPERATOR_SCOPE.usersOperatorSetupLink;
  const usersTable = USERS_TABLE;
  let records = [];
  try {
    records = await base(usersTable)
      .select({ maxRecords: 50, fields: [linkField] })
      .all();
  } catch (e) {
    return { error: e.message };
  }
  for (const rec of records) {
    const links = rec.fields?.[linkField];
    const ids = Array.isArray(links) ? links.filter((id) => typeof id === "string" && id.startsWith("rec")) : [];
    if (!ids.length) continue;
    const scope = await resolveOperatorScope({
      userRecordId: rec.id,
      isAdmin: false,
      isOperator: true,
    });
    if (scope.mappingStatus === "ok" && scope.allowedOperatingCompanyNames?.length) {
      return { userRecordId: rec.id, scope, masterIds: ids };
    }
  }
  return { error: "No Users row with active Operator Setup - Master scope found" };
}

async function findOdrForCompany(base, companyName) {
  const formula = `{Operating Company Name} = '${String(companyName).replace(/'/g, "\\'")}'`;
  try {
    const rows = await base(ODR_TABLE).select({ filterByFormula: formula, maxRecords: 5 }).all();
    return rows;
  } catch (e) {
    return { error: e.message };
  }
}

async function seedTestOdr(base, companyName, masterId) {
  const deals = await base(process.env.AIRTABLE_TABLE_DEALS || "Deals").select({ maxRecords: 1 }).all();
  if (!deals.length) return { error: "No Deals row available to seed ODR test record" };
  const now = new Date().toISOString();
  const fields = {
    Deal: [deals[0].id],
    [MAP_ODR_AIRTABLE.operatingCompanyName]: companyName,
    [MAP_ODR_AIRTABLE.status]: "New",
    [MAP_ODR_AIRTABLE.requestSentAt]: now,
    [MAP_ODR_AIRTABLE.createdAt]: now,
    [MAP_ODR_AIRTABLE.lastUpdated]: now,
  };
  if (masterId) fields[MAP_ODR_AIRTABLE.operatorSetup] = [masterId];
  const [created] = await base(ODR_TABLE).create([{ fields }]);
  results.fixesApplied.push(`Seeded test ODR record ${created.id} for ${companyName}`);
  return created;
}

async function testApiBehavior(base, testUser) {
  const scope = testUser.scope;
  const company = scope.primaryOperatingCompanyName || scope.allowedOperatingCompanyNames[0];

  const listReq = {
    dealalityUser: { userRecordId: testUser.userRecordId, isAdmin: false, isOperator: true },
    query: {},
  };
  const listRes = mockRes();
  await listOperatorDealRequests(listReq, listRes);
  results.apiTests.scopedGet = {
    pass: listRes._out.statusCode === 200 && listRes._out.body?.success === true,
    status: listRes._out.statusCode,
    count: listRes._out.body?.requests?.length ?? null,
    mappingStatus: listRes._out.body?.meta?.mappingStatus,
  };

  let odrRows = await findOdrForCompany(base, company);
  if (odrRows?.error) {
    results.apiTests.scopedGet.note = odrRows.error;
    odrRows = [];
  }
  let seeded = null;
  if (!odrRows.length) {
    seeded = await seedTestOdr(base, company, testUser.masterIds[0]);
    if (seeded?.error) {
      results.apiTests.seed = { pass: false, error: seeded.error };
      return;
    }
    odrRows = [seeded];
  }

  const requestId = odrRows[0].id;

  const tamperReq = {
    dealalityUser: { userRecordId: testUser.userRecordId, isAdmin: false, isOperator: true },
    query: { operator: "__NOT_IN_ALLOW_LIST__" },
  };
  const tamperRes = mockRes();
  await listOperatorDealRequests(tamperReq, tamperRes);
  results.apiTests.tamperedOperatorQuery = {
    pass: tamperRes._out.statusCode === 200 && (tamperRes._out.body?.requests?.length ?? 0) === 0,
    count: tamperRes._out.body?.requests?.length ?? null,
  };

  const getReq = {
    params: { requestId },
    dealalityUser: { userRecordId: testUser.userRecordId, isAdmin: false, isOperator: true },
  };
  const getRes = mockRes();
  await getOperatorDealRequestById(getReq, getRes);
  results.apiTests.getById = {
    pass: getRes._out.statusCode === 200 && getRes._out.body?.request?.id === requestId,
    status: getRes._out.statusCode,
  };

  const patchReq = {
    params: { requestId },
    body: { status: "Operator Viewed", appendOperatorInternalNotes: "Phase 2 QA patch" },
    dealalityUser: { userRecordId: testUser.userRecordId, isAdmin: false, isOperator: true },
  };
  const patchRes = mockRes();
  await updateOperatorDealRequest(patchReq, patchRes);
  results.apiTests.patch = {
    pass: patchRes._out.statusCode === 200 && patchRes._out.body?.request?.status === "Operator Viewed",
    status: patchRes._out.statusCode,
    error: patchRes._out.body?.error,
  };

  const bulkReq = {
    body: { updates: [{ requestId, status: "Viewed" }] },
    dealalityUser: { userRecordId: testUser.userRecordId, isAdmin: false, isOperator: true },
  };
  const bulkRes = mockRes();
  await bulkUpdateOperatorDealRequests(bulkReq, bulkRes);
  results.apiTests.bulkUpdate = {
    pass: bulkRes._out.statusCode === 200 && bulkRes._out.body?.success === true,
    status: bulkRes._out.statusCode,
    error: bulkRes._out.body?.error,
  };

  const forbiddenReq = {
    body: { updates: [{ requestId, status: "Viewed" }] },
    dealalityUser: { userRecordId: testUser.userRecordId, isAdmin: false, isOperator: true },
  };
  const otherCompany = "__FORBIDDEN_CO__";
  const fakeId = requestId;
  const crossScopeReq = {
    params: { requestId: fakeId },
    dealalityUser: {
      userRecordId: testUser.userRecordId,
      isAdmin: false,
      isOperator: true,
    },
  };
  await updateOperatorDealRequest(crossScopeReq, mockRes());

  if (seeded?.id) {
    try {
      await base(ODR_TABLE).destroy([seeded.id]);
      results.fixesApplied.push(`Cleaned up seeded ODR ${seeded.id}`);
    } catch (e) {
      results.fixesApplied.push(`Could not clean seeded ODR: ${e.message}`);
    }
  }

  results.apiTests.meEquivalent = {
    pass: scope.mappingStatus === "ok",
    mappingStatus: scope.mappingStatus,
    allowedOperatingCompanyNames: scope.allowedOperatingCompanyNames,
    primaryOperatingCompanyName: scope.primaryOperatingCompanyName,
    allowedOperatorSetupIds: scope.allowedOperatorSetupIds,
    warnings: scope.warnings,
  };
}

async function main() {
  if (!apiKey || !baseId) {
    console.error("FAIL: AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");
    process.exit(1);
  }

  console.log("=== My Operator Deals Phase 2 — Live Airtable QA ===\n");

  const metaTables = await fetchMetaTables();
  validateSchema(metaTables);

  console.log("1. Schema by table");
  for (const [name, r] of Object.entries(results.tables)) {
    console.log(`   ${r.pass ? "PASS" : "FAIL"} — ${name}${r.detail ? `: ${r.detail}` : ""}${r.reason ? `: ${r.reason}` : ""}`);
  }

  if (results.missingFields.length) {
    console.log("\n2. Missing fields");
    for (const m of results.missingFields) console.log(`   - ${m}`);
  } else console.log("\n2. Missing fields: none");

  if (results.typeMismatches.length) {
    console.log("\n3. Type mismatches");
    for (const t of results.typeMismatches) {
      console.log(`   - ${t.table}::${t.field} expected one of [${t.expected.join(", ")}] got ${t.actual}`);
    }
  } else console.log("\n3. Type mismatches: none");

  if (results.selectMissing.length) {
    console.log("\n4. Select options missing");
    for (const s of results.selectMissing) {
      console.log(`   - ${s.table}::${s.field} missing "${s.missingOption}" (have: ${s.have.join(", ")})`);
    }
  } else console.log("\n4. Select options missing: none");

  const base = new Airtable({ apiKey }).base(baseId);
  const testUser = await findOperatorTestUser(base);
  if (testUser.error) {
    results.mePermissions = { pass: false, error: testUser.error };
    console.log("\n5. /api/me operator permissions: SKIP —", testUser.error);
  } else {
    results.mePermissions = {
      pass: testUser.scope.mappingStatus === "ok",
      ...testUser.scope,
      userRecordId: testUser.userRecordId,
    };
    console.log("\n5. /api/me operator permissions (via resolveOperatorScope)");
    console.log(`   ${results.mePermissions.pass ? "PASS" : "FAIL"} — mappingStatus=${testUser.scope.mappingStatus}`);
    console.log(`   allowedOperatingCompanyNames: ${JSON.stringify(testUser.scope.allowedOperatingCompanyNames)}`);
    console.log(`   primaryOperatingCompanyName: ${testUser.scope.primaryOperatingCompanyName || "null"}`);
    if (testUser.scope.warnings?.length) console.log(`   warnings: ${testUser.scope.warnings.join(", ")}`);

    await testApiBehavior(base, testUser);
    console.log("\n6. Scoped API behavior");
    for (const [k, v] of Object.entries(results.apiTests)) {
      const ok = v.pass === true;
      console.log(`   ${ok ? "PASS" : "FAIL"} — ${k}${v.status != null ? ` (HTTP ${v.status})` : ""}${v.error ? ` error=${v.error}` : ""}`);
    }
  }

  if (results.fixesApplied.length) {
    console.log("\n7. Fixes applied during QA");
    for (const f of results.fixesApplied) console.log(`   - ${f}`);
  } else {
    console.log("\n7. Fixes applied: none");
  }

  const schemaFail = Object.values(results.tables).some((t) => !t.pass);
  const apiFail = Object.values(results.apiTests).some((v) => v.pass === false);
  const meFail = results.mePermissions && results.mePermissions.pass === false;

  console.log("\n=== Summary ===");
  if (schemaFail || apiFail || meFail) {
    console.log("OVERALL: FAIL");
    process.exit(1);
  }
  console.log("OVERALL: PASS");
}

main().catch((e) => {
  console.error("QA script error:", e.message);
  process.exit(1);
});
