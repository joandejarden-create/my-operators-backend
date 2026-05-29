#!/usr/bin/env node
/**
 * Security / wiring checks for Operator Deal Requests Phase 2 scoping.
 * Run: node scripts/validate-operator-deal-requests-scoping.mjs
 */

import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("PASS:", msg);
}

const serverJs = readFileSync(join(root, "server.js"), "utf8");
const serverUpload = readFileSync(join(root, "server.upload-ready.js"), "utf8");
const odrApi = readFileSync(join(root, "api/operator-deal-requests.js"), "utf8");
const meApi = readFileSync(join(root, "api/me.js"), "utf8");
const scopeLib = readFileSync(join(root, "lib/dealality/resolve-operator-scope.js"), "utf8");
const middleware = readFileSync(join(root, "middleware/requireOperatorDealsAccess.js"), "utf8");
const dashboardJs = readFileSync(join(root, "public/operator-development-dashboard.js"), "utf8");
const dashboardHtml = readFileSync(join(root, "public/operator-development-dashboard.html"), "utf8");
const envExample = readFileSync(join(root, ".env.example"), "utf8");

for (const file of [serverJs, serverUpload]) {
  const routes = [
    'app.get("/api/operator-deal-requests/deal-meta"',
    'app.get("/api/operator-deal-requests/activity"',
    'app.get("/api/operator-deal-requests"',
    'app.get("/api/operator-deal-requests/:requestId"',
    'app.patch("/api/operator-deal-requests/:requestId"',
    'app.post("/api/operator-deal-requests/bulk-update"',
  ];
  for (const route of routes) {
    if (!file.includes(route)) {
      fail(`Missing route registration: ${route}`);
    } else {
      const idx = file.indexOf(route);
      const slice = file.slice(Math.max(0, idx - 120), idx + route.length + 80);
      if (!slice.includes("operatorDealsAuth")) {
        fail(`${route} must use operatorDealsAuth middleware`);
      } else {
        pass(`${route} uses operatorDealsAuth`);
      }
    }
  }
}

if (serverJs.includes("requireOperatorDealsAccess")) {
  pass("operatorDealsAuth includes requireOperatorDealsAccess");
} else {
  fail("operatorDealsAuth must include requireOperatorDealsAccess");
}

if (middleware.includes("isAdmin") && middleware.includes("isOperator") && middleware.includes("403")) {
  pass("requireOperatorDealsAccess gates operator/admin");
} else {
  fail("requireOperatorDealsAccess missing role gate");
}

if (odrApi.includes("!scope.isAdmin && wantsAll")) {
  pass("list ignores ?all=1 for non-admin");
} else {
  fail("list must ignore ?all=1 for non-admin operators");
}

if (
  odrApi.includes("assertOperatorRequestRecordAccess") &&
  odrApi.includes("bulkUpdateOperatorDealRequests") &&
  odrApi.includes("existingRecords.length !== requestIds.length")
) {
  pass("bulk-update verifies row access before update");
} else {
  fail("bulk-update must verify all requestIds and row scope");
}

if (odrApi.includes("unsupported_action") && odrApi.includes("Phase 2")) {
  pass("PATCH rejects proposal/deal room in Phase 2");
} else {
  fail("PATCH must reject proposal/deal room actions");
}

if (scopeLib.includes('AIRTABLE_ME_USERS_OPERATOR_SETUP_LINK || "Operator Setup - Master"')) {
  pass("scope uses Operator Setup - Master via env default");
} else {
  fail("scope must default to Operator Setup - Master link field");
}

if (scopeLib.includes("AIRTABLE_OPERATOR_SETUP_ACTIVE_STATUS_VALUES") && scopeLib.includes('"Active"')) {
  pass("active status filter defaults to Active");
} else {
  fail("active status filter env missing");
}

if (scopeLib.includes("return match ? [match] : []")) {
  pass("out-of-scope ?operator= returns empty filter (no data leak)");
} else {
  fail("resolveEffectiveCompanyFilter must return [] for invalid operator query");
}

const meFields = [
  "allowedOperatingCompanyNames",
  "allowedOperatorSetupIds",
  "primaryOperatingCompanyName",
  "operatorMappingStatus",
  "resolveOperatorScope",
];
for (const f of meFields) {
  if (!meApi.includes(f)) fail(`/api/me missing ${f}`);
  else pass(`/api/me includes ${f}`);
}

if (odrApi.includes("MAP_ODR_AIRTABLE.operatingCompanyName") && odrApi.includes('"Operator"')) {
  pass("activity log uses operating company field + Operator stakeholder");
} else {
  fail("activity log must use Operating Company Name and Operator stakeholder");
}

if (dashboardJs.includes("/api/me") && dashboardJs.includes("authFetch")) {
  pass("dashboard loads /api/me with authFetch");
} else {
  fail("dashboard must call /api/me via authFetch");
}

if (dashboardJs.includes("oddCompanyFilter") && dashboardJs.includes("allowedOperatingCompanyNames")) {
  pass("dashboard company dropdown from /api/me allow-list");
} else {
  fail("dashboard must wire company filter from allowedOperatingCompanyNames");
}

if (dashboardHtml.includes("dealality-memberstack-auth.js")) {
  pass("dashboard HTML loads memberstack auth");
} else {
  fail("dashboard HTML must load dealality-memberstack-auth.js");
}

const envKeys = [
  "AIRTABLE_ME_USERS_OPERATOR_SETUP_LINK",
  "AIRTABLE_OPERATOR_SETUP_MASTER_TABLE",
  "AIRTABLE_OPERATOR_COMPANY_NAME_FIELD",
  "AIRTABLE_OPERATOR_SETUP_ACTIVE_STATUS_VALUES",
];
for (const k of envKeys) {
  if (!envExample.includes(k)) fail(`.env.example missing ${k}`);
  else pass(`.env.example includes ${k}`);
}

if (serverJs.includes('import { getMe } from "./api/me.js"')) {
  pass("server.js imports getMe (boot-safe)");
} else {
  fail("server.js missing import { getMe } from ./api/me.js");
}

if (!process.exitCode) {
  console.log("\nAll operator deal requests scoping checks passed.");
}
