#!/usr/bin/env node
/**
 * Ensure Hotel Property Census geography fields:
 *   Continent (singleSelect), Sub-Continent (singleSelect),
 *   Market (singleLineText), Submarket (singleLineText)
 *
 *   node scripts/ensure-hotel-property-census-market-geography-fields.mjs --dry-run
 *   node scripts/ensure-hotel-property-census-market-geography-fields.mjs --apply \
 *     --confirm-write-to-production-census \
 *     --confirm-schema-only \
 *     --confirm-no-brand-explorer-writes
 *
 * Requires ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE=1 for --apply.
 * Creates fields on Hotel Property Census only (tbl9aY5ijiuIzzWam).
 */
import "../load-env.js";
import {
  CONTINENT_OPTIONS,
  SUB_CONTINENT_OPTIONS,
  CENSUS_GEO_FIELDS,
} from "../lib/research-engine-v2/census-region-market-map.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;
const TABLE_ID = PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const TABLE_NAME = productionHotelPropertyCensus.tableName;

function hasFlag(name) {
  return process.argv.includes(name);
}

function chooseColor(i) {
  const colors = [
    "blueLight2",
    "cyanLight2",
    "tealLight2",
    "greenLight2",
    "yellowLight2",
    "orangeLight2",
    "redLight2",
    "pinkLight2",
    "purpleLight2",
    "grayLight2",
  ];
  return colors[i % colors.length];
}

function selectField(name, optionNames) {
  return {
    name,
    type: "singleSelect",
    options: {
      choices: optionNames.map((n, i) => ({ name: n, color: chooseColor(i) })),
    },
  };
}

function textField(name) {
  return { name, type: "singleLineText" };
}

const DESIRED = [
  selectField(CENSUS_GEO_FIELDS.continent, CONTINENT_OPTIONS),
  selectField(CENSUS_GEO_FIELDS.subContinent, SUB_CONTINENT_OPTIONS),
  textField(CENSUS_GEO_FIELDS.market),
  textField(CENSUS_GEO_FIELDS.submarket),
];

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
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

async function main() {
  const token = process.env.AIRTABLE_PAT || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) {
    console.error("AIRTABLE_PAT and AIRTABLE_BASE_ID_ALT required");
    process.exit(1);
  }

  const confirmsOk =
    hasFlag("--confirm-write-to-production-census") &&
    hasFlag("--confirm-schema-only") &&
    hasFlag("--confirm-no-brand-explorer-writes");
  const allow = process.env.ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE === "1";

  console.log(
    `[ensure-hpc-geo] mode=${APPLY ? "apply" : "dry-run"} table=${TABLE_NAME} (${TABLE_ID}) allow=${allow}`
  );

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    console.error("Failed to load base schema:", res.status, json);
    process.exit(1);
  }

  const table = (json.tables || []).find(
    (t) => t.id === TABLE_ID || t.name === TABLE_NAME
  );
  if (!table) {
    console.error("Hotel Property Census not found");
    process.exit(1);
  }
  if (table.id !== TABLE_ID) {
    console.error(`Refusing wrong table id ${table.id} (expected ${TABLE_ID})`);
    process.exit(1);
  }

  const existing = new Map((table.fields || []).map((f) => [f.name, f]));
  const toCreate = [];
  const already = [];
  for (const field of DESIRED) {
    if (existing.has(field.name)) {
      already.push({
        name: field.name,
        type: existing.get(field.name).type,
        id: existing.get(field.name).id,
      });
    } else {
      toCreate.push(field);
    }
  }

  const report = {
    table: TABLE_NAME,
    table_id: TABLE_ID,
    already_present: already,
    to_create: toCreate.map((f) => ({ name: f.name, type: f.type })),
    created: [],
    errors: [],
  };

  if (DRY || !APPLY) {
    console.log(JSON.stringify({ dry_run: true, ...report }, null, 2));
    return;
  }

  if (!allow) {
    console.error("Set ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE=1 for apply");
    process.exit(1);
  }
  if (!confirmsOk) {
    console.error(
      "Apply requires --confirm-write-to-production-census --confirm-schema-only --confirm-no-brand-explorer-writes"
    );
    process.exit(1);
  }

  for (const field of toCreate) {
    const { res: cRes, json: cJson } = await metaFetch(
      baseId,
      token,
      `/tables/${TABLE_ID}/fields`,
      { method: "POST", body: JSON.stringify(field) }
    );
    if (!cRes.ok) {
      console.error(`FAIL create ${field.name}:`, cRes.status, cJson);
      report.errors.push({ field: field.name, error: cJson });
      continue;
    }
    console.log(`  created ${field.name} (${cJson.type}) id=${cJson.id}`);
    report.created.push({ name: field.name, type: cJson.type, id: cJson.id });
  }

  console.log(
    JSON.stringify(
      {
        apply: true,
        created: report.created.length,
        already: report.already_present.length,
        errors: report.errors.length,
        report,
      },
      null,
      2
    )
  );
  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[ensure-hpc-geo] FAILED", err);
  process.exit(1);
});
