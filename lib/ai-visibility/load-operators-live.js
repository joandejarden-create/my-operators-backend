/**
 * Read-only Operator Setup - Master loader for AI Visibility.
 * Uses governed fields only: company_name, Operator Aliases, Operator Website,
 * submission_status, Record Purpose. Never writes Airtable.
 */

import { createHash } from "crypto";
import {
  fetchAllRecordsRest,
  NEW_BASE_MASTER_TABLE,
} from "../../api/lib/operator-setup-new-base-read.js";
import { normalizeMatchKey } from "./normalize-entities.js";
import { parseDomain } from "./extract-citations.js";

const OPERATOR_FIELDS = Object.freeze({
  companyName: "company_name",
  aliases: "Operator Aliases",
  website: "Operator Website",
  parent: "Operator Parent Company",
  submissionStatus: "submission_status",
  recordPurpose: "Record Purpose",
});

function cellToString(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return cellToString(v[0]);
  if (typeof v === "object" && v.name != null) return String(v.name).trim();
  return String(v).trim();
}

/** Split governed Operator Aliases (semicolon/comma separated). */
export function parseOperatorAliases(raw) {
  const s = cellToString(raw);
  if (!s) return [];
  return s
    .split(/[;|,]/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function isActiveSubmission(status) {
  return cellToString(status).toLowerCase() === "active";
}

function isTestFixturePurpose(purpose) {
  const p = cellToString(purpose).toLowerCase();
  return p === "test fixture" || p.includes("test fixture");
}

/**
 * @param {{ fetchMasterRecords?: Function, activeOnly?: boolean, excludeTestFixtures?: boolean }} [deps]
 */
export async function loadLiveOperatorEntities(deps = {}) {
  const activeOnly = deps.activeOnly !== false;
  const excludeTestFixtures = deps.excludeTestFixtures !== false;
  const fetchMasterRecords =
    deps.fetchMasterRecords ||
    (async () => fetchAllRecordsRest(NEW_BASE_MASTER_TABLE));

  const records = await fetchMasterRecords();
  const operators = [];
  const skipped = [];

  for (const rec of records || []) {
    const f = rec.fields || {};
    const name = cellToString(f[OPERATOR_FIELDS.companyName]);
    if (!name) {
      skipped.push({ id: rec.id, reason: "missing_company_name" });
      continue;
    }
    const purpose = cellToString(f[OPERATOR_FIELDS.recordPurpose]);
    const status = cellToString(f[OPERATOR_FIELDS.submissionStatus]);
    if (excludeTestFixtures && isTestFixturePurpose(purpose)) {
      skipped.push({ id: rec.id, name, reason: "test_fixture" });
      continue;
    }
    if (activeOnly && !isActiveSubmission(status)) {
      skipped.push({ id: rec.id, name, reason: "not_active", status });
      continue;
    }

    const aliases = parseOperatorAliases(f[OPERATOR_FIELDS.aliases]);
    const website = cellToString(f[OPERATOR_FIELDS.website]) || null;
    const domain = website ? parseDomain(website) : null;

    operators.push({
      id: rec.id,
      name,
      entityType: "operator",
      aliases,
      firstPartyDomains: domain ? [domain] : [],
      parentCompany: cellToString(f[OPERATOR_FIELDS.parent]) || null,
      isParentCompanyLabel: false,
      submissionStatus: status || null,
      recordPurpose: purpose || null,
      website,
      sourceSystem: "operator_setup_master",
    });
  }

  operators.sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" }));

  const fingerprint = createHash("sha256")
    .update(
      operators
        .map((o) => `${o.id}|${o.name}|${(o.aliases || []).slice().sort().join(",")}`)
        .join("\n")
    )
    .digest("hex")
    .slice(0, 16);

  return {
    entities: operators,
    meta: {
      table: NEW_BASE_MASTER_TABLE,
      eligibility: activeOnly
        ? "submission_status=Active (exclude Test Fixture)"
        : "all non-test masters",
      operatorCount: operators.length,
      aliasField: OPERATOR_FIELDS.aliases,
      websiteField: OPERATOR_FIELDS.website,
      skippedSample: skipped.slice(0, 20),
      fingerprint,
      airtableWrites: 0,
      note:
        "Operator alias SSOT is weaker than Brand Alias Mapping; unresolved mentions stay Unresolved.",
    },
  };
}

export function selectOperatorsByCanonicalNames(entities, requestedNames) {
  const byKey = new Map(
    (entities || []).map((e) => [normalizeMatchKey(e.name), e])
  );
  const selected = [];
  const missing = [];
  for (const name of requestedNames || []) {
    const hit = byKey.get(normalizeMatchKey(name));
    if (hit) selected.push(hit);
    else missing.push(name);
  }
  return { selected, missing };
}

export { OPERATOR_FIELDS };
