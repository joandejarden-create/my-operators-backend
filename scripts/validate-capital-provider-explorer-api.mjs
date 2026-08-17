#!/usr/bin/env node
/**
 * Validate Capital Provider Explorer API against live Airtable data.
 *   node scripts/validate-capital-provider-explorer-api.mjs
 */
import "../load-env.js";
import {
  listCapitalProvidersApi,
  getCapitalProviderDetailApi,
} from "../api/capital-provider-explorer.js";
import {
  INTERNAL_PROVIDER_AT_FIELDS,
  INTERNAL_CRITERIA_AT_FIELDS,
} from "../api/lib/capital-provider-explorer-field-map.js";

const INTERNAL_API_KEYS = new Set([
  "internal",
  "internalCreditBoxNotes",
  "pricingGuidance",
  "leverageGuidance",
  "riskLimits",
  "internalNotes",
  "internalCriteriaNotes",
  "relationshipSensitivity",
  "creditBoxNotes",
  "leverageRanges",
  "pricingGuidance",
  "dealDeclinePatterns",
  "contacts",
]);

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function mockReq(query = {}, user = null) {
  return { query, dealalityUser: user };
}

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    status(code) {
      out.statusCode = code;
      return this;
    },
    json(body) {
      out.body = body;
      return this;
    },
    out,
  };
}

async function callList(query = {}) {
  const req = mockReq(query);
  const res = mockRes();
  await listCapitalProvidersApi(req, res);
  return res.out;
}

async function callDetail(id) {
  const req = mockReq({ id });
  const res = mockRes();
  await getCapitalProviderDetailApi(req, res);
  return res.out;
}

function collectKeys(obj, prefix = "", keys = new Set()) {
  if (!obj || typeof obj !== "object") return keys;
  if (Array.isArray(obj)) {
    obj.forEach((item) => collectKeys(item, prefix, keys));
    return keys;
  }
  for (const [k, v] of Object.entries(obj)) {
    keys.add(k);
    if (v && typeof v === "object") collectKeys(v, `${prefix}${k}.`, keys);
  }
  return keys;
}

function assertNoInternalKeys(payload, label) {
  const keys = collectKeys(payload);
  for (const key of keys) {
    if (INTERNAL_API_KEYS.has(key)) {
      assert(false, `${label} exposes internal key: ${key}`);
    }
  }
  for (const atField of INTERNAL_PROVIDER_AT_FIELDS) {
    if (keys.has(atField)) {
      assert(false, `${label} exposes internal Airtable field name: ${atField}`);
    }
  }
  for (const atField of INTERNAL_CRITERIA_AT_FIELDS) {
    if (keys.has(atField)) {
      assert(false, `${label} exposes internal criteria field: ${atField}`);
    }
  }
}

async function main() {
  console.log("=== Capital Provider Explorer API validation ===\n");

  const list = await callList();
  assert(list.statusCode === 200, "list returns 200");
  assert(list.body?.ok === true, "list ok=true");
  assert(list.body?.source === "airtable", "list source=airtable");
  assert(Array.isArray(list.body?.providers), "list providers array");

  const providers = list.body.providers || [];
  assert(providers.length === 12, `list returns 12 providers (got ${providers.length})`);

  const names = providers.map((p) => p.name);
  assert(names.includes("IDB Invest"), "IDB Invest appears in list");
  assert(names.includes("IFC"), "IFC appears in list");
  assert(
    names.some((n) => String(n).includes("CIBC")),
    "CIBC Caribbean / FirstCaribbean appears in list"
  );

  const cibc = providers.find((p) => String(p.name).includes("CIBC"));
  if (cibc) {
    assert(
      cibc.profileStatus === "Needs Review" || cibc.sourceConfidence === "Needs Verification",
      "CIBC carries Needs Review or Needs Verification"
    );
  }

  assertNoInternalKeys(list.body, "list response");

  const senior = await callList({ loanProduct: "Senior Debt" });
  assert(senior.statusCode === 200, "loanProduct filter returns 200");
  const seniorNames = (senior.body.providers || []).map((p) => p.name);
  assert(seniorNames.length > 0, "loanProduct=Senior Debt returns providers");
  assert(
    seniorNames.every((n) =>
      (senior.body.providers.find((p) => p.name === n)?.loanProductsOffered || []).some(
        (lp) => String(lp).toLowerCase() === "senior debt"
      )
    ),
    "loanProduct filter matches Senior Debt"
  );

  const caribbean = await callList({ region: "Caribbean" });
  assert(caribbean.statusCode === 200, "region filter returns 200");
  assert((caribbean.body.providers || []).length > 0, "region=Caribbean returns providers");

  const geo = await callList({ geography: "Mexico" });
  assert(geo.statusCode === 200, "geography filter returns 200");
  assert((geo.body.providers || []).length > 0, "geography=Mexico returns providers");

  const search = await callList({ search: "IDB" });
  assert(search.statusCode === 200, "search filter returns 200");
  assert(
    (search.body.providers || []).some((p) => String(p.name).includes("IDB")),
    "search=IDB returns IDB Invest"
  );

  const idb = providers.find((p) => p.name === "IDB Invest");
  assert(idb?.id, "IDB Invest has Airtable record id");

  const detail = await callDetail(idb.id);
  assert(detail.statusCode === 200, "detail returns 200");
  assert(detail.body?.ok === true, "detail ok=true");
  assert(detail.body?.provider?.name === "IDB Invest", "detail provider is IDB Invest");
  assert(
    (detail.body?.sourceReferences || []).length >= 1,
    "IDB Invest has linked source references"
  );
  assertNoInternalKeys(detail.body, "detail response");

  const withCriteria = [];
  for (const p of providers.slice(0, 6)) {
    const d = await callDetail(p.id);
    if ((d.body?.criteria || []).length > 0) withCriteria.push(p.name);
  }
  assert(withCriteria.length >= 1, "at least one provider has linked criteria");

  const ifc = providers.find((p) => p.name === "IFC");
  if (ifc) {
    const ifcDetail = await callDetail(ifc.id);
    assert(
      (ifcDetail.body?.requiredDocuments || []).length >= 1,
      "IFC has required document records where available"
    );
    for (const doc of ifcDetail.body.requiredDocuments || []) {
      const vis = doc.visibilityLevel || "Owner Visible";
      assert(
        vis === "Owner Visible" || !vis,
        `IFC document visibility owner-safe (${doc.documentRequirementName})`
      );
    }
  }

  console.log("\n--- Summary ---");
  if (failed) {
    console.error(`\n${failed} validation check(s) failed.`);
    process.exit(1);
  }
  console.log("\nAll validation checks passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
