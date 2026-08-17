/**
 * Audit connectivity and schema for the internal GTM Owner Targets Airtable base.
 *
 * Usage:
 *   AIRTABLE_GTM_BASE_ID=appKZuK006BWIVjNW node scripts/audit-gtm-owner-target-base.mjs
 *
 * Report: reports/gtm-owner-target-base-audit.json
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
  MAP_GTM_PROPERTIES,
} from "../lib/gtm-owner-target/field-map.js";
import {
  getGtmApiKey,
  getGtmBaseId,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import { fetchAllGtmProperties, groupAirtablePropertiesByOwner } from "../lib/gtm-owner-target/properties-read.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT = join(__dirname, "..", "reports", "gtm-owner-target-base-audit.json");

async function metaFetch(baseId, token, metaPath) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  return { res, json };
}

async function resolveGtmBaseId(token) {
  const configured = getGtmBaseId();
  if (configured) return configured;

  const listRes = await fetch("https://api.airtable.com/v0/meta/bases", {
    headers: { Authorization: `Bearer ${token}` },
  });
  const listJson = await listRes.json();
  if (!listRes.ok) throw new Error(`Cannot list bases: ${JSON.stringify(listJson)}`);

  for (const base of listJson.bases || []) {
    const { res: tRes, json: tJson } = await metaFetch(base.id, token, "/tables");
    if (!tRes.ok) continue;
    const hasProperties = (tJson.tables || []).some((t) => t.name === GTM_OWNER_TARGET_TABLES.properties);
    if (hasProperties && /owner targets/i.test(base.name)) return base.id;
  }
  return "";
}

function missingFields(tableFields, requiredNames) {
  const existing = new Set((tableFields || []).map((f) => f.name));
  return requiredNames.filter((name) => !existing.has(name));
}

async function main() {
  const token = getGtmApiKey();
  if (!token) throw new Error("Set AIRTABLE_GTM_API_KEY, AIRTABLE_PAT, or AIRTABLE_API_KEY");

  let baseId = getGtmBaseId();
  if (!baseId) {
    baseId = await resolveGtmBaseId(token);
  }
  if (!baseId) {
    throw new Error("Set AIRTABLE_GTM_BASE_ID (Owner Targets Table base, e.g. appKZuK006BWIVjNW)");
  }
  assertNotProductBase(baseId);

  const report = {
    generatedAt: new Date().toISOString(),
    baseId,
    tokenSource: process.env.AIRTABLE_GTM_API_KEY
      ? "AIRTABLE_GTM_API_KEY"
      : process.env.AIRTABLE_PAT
        ? "AIRTABLE_PAT"
        : "AIRTABLE_API_KEY",
    tables: {},
    propertiesSummary: null,
    ownerTargetsTableExists: false,
    recommendations: [],
  };

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    report.metaError = json;
    report.recommendations.push(
      "Token cannot read GTM base metadata. Add this base to your Personal Access Token scopes."
    );
    mkdirSync(dirname(REPORT), { recursive: true });
    writeFileSync(REPORT, JSON.stringify(report, null, 2));
    console.error("Meta API failed:", res.status, JSON.stringify(json));
    process.exit(1);
  }

  for (const table of json.tables || []) {
    report.tables[table.name] = {
      id: table.id,
      fieldCount: (table.fields || []).length,
      fields: (table.fields || []).map((f) => f.name),
    };
    if (table.name === GTM_OWNER_TARGET_TABLES.ownerTargets) {
      report.ownerTargetsTableExists = true;
      report.ownerTargetsMissingFields = missingFields(table.fields, [
        MAP_GTM_OWNER_TARGET.ownerName,
        MAP_GTM_OWNER_TARGET.outreachStatus,
        MAP_GTM_OWNER_TARGET.pitchStatus,
        MAP_GTM_OWNER_TARGET.priorityTier,
      ]);
    }
    if (table.name === GTM_OWNER_TARGET_TABLES.properties) {
      report.propertiesMissingFields = missingFields(table.fields, [
        MAP_GTM_PROPERTIES.buildingName,
        MAP_GTM_PROPERTIES.trueOwner,
        MAP_GTM_PROPERTIES.rbaGla,
      ]);
    }
  }

  if (!report.tables[GTM_OWNER_TARGET_TABLES.properties]) {
    report.recommendations.push(
      `Missing CoStar table "${GTM_OWNER_TARGET_TABLES.properties}" in GTM base.`
    );
  }
  if (!report.ownerTargetsTableExists) {
    report.recommendations.push(
      `Create rollup table via: node scripts/ensure-gtm-owner-target-base.mjs --apply`
    );
  }

  process.env.AIRTABLE_GTM_BASE_ID = baseId;
  try {
    const { tableName, records } = await fetchAllGtmProperties();
    const groups = groupAirtablePropertiesByOwner(records);
    report.propertiesSummary = {
      tableName,
      propertyCount: records.length,
      distinctTrueOwners: groups.length,
      topOwners: groups
        .map((g) => ({ ownerName: g.ownerName, propertyCount: g.properties.length }))
        .sort((a, b) => b.propertyCount - a.propertyCount)
        .slice(0, 15),
    };
    report.dataApiOk = true;
  } catch (err) {
    report.dataApiOk = false;
    report.dataApiError = err.message || String(err);
    report.recommendations.push("Data API read failed — verify token has data.records:read on GTM base.");
  }

  mkdirSync(dirname(REPORT), { recursive: true });
  writeFileSync(REPORT, JSON.stringify(report, null, 2));

  console.log("GTM base audit:", baseId);
  console.log("Token:", report.tokenSource);
  console.log("Tables:", Object.keys(report.tables).join(", "));
  if (report.propertiesSummary) {
    console.log(
      `Properties: ${report.propertiesSummary.propertyCount} rows, ${report.propertiesSummary.distinctTrueOwners} True Owners`
    );
  }
  if (report.recommendations.length) {
    console.log("\nRecommendations:");
    report.recommendations.forEach((r) => console.log(" -", r));
  }
  console.log("\nWrote", REPORT);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
