#!/usr/bin/env node
/**
 * Seed missing Family / Source Family select options via typecast
 * (Meta API field PATCH is blocked on this base/token).
 *
 * Seeds: Club Med, SuperClubs (and any other required options still missing).
 *
 *   node scripts/seed-hotel-property-census-source-family-options.mjs --dry-run
 *   node scripts/seed-hotel-property-census-source-family-options.mjs --apply \
 *     --enable-production-writes + intake confirms + env flags
 */
import "../load-env.js";
import {
  CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS,
} from "../lib/independent-census/intake-census-field-normalize.js";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";

const PRIORITY = ["Club Med", "SuperClubs"];

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function metaChoices(baseId, token) {
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json.error || json));
  const table = (json.tables || []).find(
    (t) => t.id === PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID
  );
  const field = (table?.fields || []).find((f) => f.name === "Family / Source Family");
  return (field?.options?.choices || []).map((c) => c.name);
}

async function findSeedRecord(baseId, token) {
  // Prefer an OSM DR intake row so we don't touch unrelated census
  const formula =
    "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
  const params = new URLSearchParams({
    filterByFormula: formula,
    maxRecords: "1",
  });
  params.append("fields[]", "Family / Source Family");
  params.append("fields[]", "Property Name");
  params.append("fields[]", "Property Identity Key");
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID}?${params}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json.error || json));
  return json.records?.[0] || null;
}

async function patchFamily(baseId, token, recordId, familyValue) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        typecast: true,
        records: [
          {
            id: recordId,
            fields: { "Family / Source Family": familyValue },
          },
        ],
      }),
    }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json.error || json));
  return json.records?.[0];
}

async function main() {
  const args = parseArgs();
  const envCheck = checkIntakeApplyEnv();
  if (args.apply && !(args.allConfirmsOk && envCheck.allOk)) {
    console.error("Apply blocked", {
      confirms: Object.entries(args.confirms)
        .filter(([, v]) => !v)
        .map(([k]) => k),
      env: envCheck.missing,
    });
    process.exit(1);
  }

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    console.error("Wrong write target");
    process.exit(1);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const existing = await metaChoices(baseId, token);
  const have = new Set(existing);
  const missingAll = CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS.filter((n) => !have.has(n));
  const toSeed = [
    ...PRIORITY.filter((n) => !have.has(n)),
    ...missingAll.filter((n) => !PRIORITY.includes(n)),
  ];

  const seed = await findSeedRecord(baseId, token);
  const originalFamily = seed?.fields?.["Family / Source Family"] || null;

  const report = {
    status: args.apply ? "applied" : "dry_run",
    existing_count: existing.length,
    missing: toSeed,
    priority: PRIORITY,
    seed_record_id: seed?.id || null,
    seed_property: seed?.fields?.["Property Name"] || null,
    original_family: originalFamily,
    airtable_writes: false,
    seeded: [],
  };

  if (!toSeed.length) {
    console.log(JSON.stringify({ ...report, status: "already_complete" }, null, 2));
    return;
  }
  if (!seed) {
    console.error("No OSM DR seed record found to typecast against");
    process.exit(1);
  }
  if (!args.apply) {
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  for (const name of toSeed) {
    await patchFamily(baseId, token, seed.id, name);
    report.seeded.push(name);
  }
  // Restore original family (or clear if it was blank)
  await patchFamily(baseId, token, seed.id, originalFamily);
  report.airtable_writes = true;
  report.restored_family = originalFamily;

  const after = await metaChoices(baseId, token);
  report.after_has_club_med = after.includes("Club Med");
  report.after_has_superclubs = after.includes("SuperClubs");
  report.after_missing = CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS.filter(
    (n) => !after.includes(n)
  );

  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
