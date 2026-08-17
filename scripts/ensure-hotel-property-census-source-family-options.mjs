#!/usr/bin/env node
/**
 * Ensure Hotel Property Census Family / Source Family select options
 * include global + regional chain families (never rely on "Other").
 *
 *   node scripts/ensure-hotel-property-census-source-family-options.mjs --dry-run
 *   node scripts/ensure-hotel-property-census-source-family-options.mjs --apply \
 *     --confirm-write-to-production-census \
 *     --confirm-schema-only \
 *     --confirm-no-brand-explorer-writes
 *
 * Requires ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE=1 for --apply.
 */
import "../load-env.js";
import {
  CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS,
  CENSUS_REGIONAL_SOURCE_FAMILY_BY_BRAND,
} from "../lib/independent-census/intake-census-field-normalize.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;
const FIELD_NAME = "Family / Source Family";
const TABLE_ID = PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

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
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(
      `Airtable meta ${res.status}: ${JSON.stringify(json.error || json)}`
    );
  }
  return json;
}

async function main() {
  if (
    APPLY &&
    !(
      hasFlag("--confirm-write-to-production-census") &&
      hasFlag("--confirm-schema-only") &&
      hasFlag("--confirm-no-brand-explorer-writes")
    )
  ) {
    console.error(
      "Apply blocked — need --confirm-write-to-production-census --confirm-schema-only --confirm-no-brand-explorer-writes"
    );
    process.exit(1);
  }
  if (APPLY && String(process.env.ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE || "") !== "1") {
    console.error("Apply blocked — set ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE=1");
    process.exit(1);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  if (!token || !baseId) {
    console.error("Missing Airtable credentials");
    process.exit(1);
  }

  const tables = await metaFetch(baseId, token, "/tables");
  const table = (tables.tables || []).find((t) => t.id === TABLE_ID);
  if (!table) throw new Error(`Table not found: ${TABLE_ID}`);
  const field = (table.fields || []).find((f) => f.name === FIELD_NAME);
  if (!field || field.type !== "singleSelect") {
    throw new Error(`${FIELD_NAME} missing or not singleSelect`);
  }

  const existing = (field.options?.choices || []).map((c) => c.name);
  const existingSet = new Set(existing);
  const missing = CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS.filter(
    (n) => !existingSet.has(n)
  );

  const report = {
    table: productionHotelPropertyCensus.tableName,
    table_id: TABLE_ID,
    field: FIELD_NAME,
    existing_choices: existing,
    required_options: [...CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS],
    missing_to_add: missing,
    regional_brand_map: CENSUS_REGIONAL_SOURCE_FAMILY_BY_BRAND,
    apply: APPLY && !DRY,
    airtable_writes: false,
  };

  if (!missing.length) {
    console.log(JSON.stringify({ ...report, status: "already_complete" }, null, 2));
    return;
  }

  if (DRY || !APPLY) {
    console.log(JSON.stringify({ ...report, status: "dry_run" }, null, 2));
    return;
  }

  // Airtable meta: PATCH field with type + full choices (preserve ids for existing)
  const mergedChoices = [
    ...(field.options?.choices || []).map((c) => ({
      id: c.id,
      name: c.name,
      ...(c.color ? { color: c.color } : {}),
    })),
    ...missing.map((name) => ({ name })),
  ];

  try {
    await metaFetch(baseId, token, `/tables/${TABLE_ID}/fields/${field.id}`, {
      method: "PATCH",
      body: JSON.stringify({
        type: "singleSelect",
        options: { choices: mergedChoices },
      }),
    });
  } catch (err) {
    console.log(
      JSON.stringify(
        {
          ...report,
          status: "meta_patch_blocked_use_typecast_remediation",
          error: err?.message || String(err),
          fallback:
            "npm run census:family-other-remediation -- --apply --enable-production-writes + confirms (typecast:true creates options)",
          missing_to_add_manually_or_via_typecast: missing,
        },
        null,
        2
      )
    );
    process.exit(0);
  }

  console.log(
    JSON.stringify(
      {
        ...report,
        status: "applied",
        airtable_writes: true,
        added: missing,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
