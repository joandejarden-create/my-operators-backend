#!/usr/bin/env node
/**
 * Add country submarket options to Demand Anchors + Travel Infrastructure.
 *   node scripts/ensure-radar-submarket-options.mjs --dry-run
 *   node scripts/ensure-radar-submarket-options.mjs --apply
 */
import "../load-env.js";
import { ALL_SUBMARKET_OPTIONS, SUBMARKET_FIELD_NAME } from "../lib/radar-submarket.js";
import { DEMAND_ANCHORS_TABLE } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { TRAVEL_INFRASTRUCTURE_TABLE } from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import { getDemandAnchorsBaseId } from "../lib/demand-anchors/demand-anchors-base.js";

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;

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

function mergeChoices(existingChoices, targetNames) {
  const byName = new Map();
  for (const c of existingChoices || []) {
    if (c?.name) byName.set(c.name, { ...c });
  }
  for (const name of targetNames) {
    if (!byName.has(name)) byName.set(name, { name });
  }
  return [...byName.values()];
}

async function patchSubmarketField(baseId, token, table, fieldName) {
  const subField = (table.fields || []).find((f) => f.name === fieldName);
  if (!subField) {
    console.warn(`  skip: ${table.name} has no ${fieldName} field`);
    return { skipped: true };
  }
  const existing = subField.options?.choices || [];
  const merged = mergeChoices(existing, ALL_SUBMARKET_OPTIONS);
  const added = merged.length - existing.length;
  if (added <= 0) {
    console.log(`  ${table.name}: submarket options already complete (${existing.length})`);
    return { skipped: true };
  }
  if (DRY) {
    console.log(`[dry-run] ${table.name}: would add ${added} submarket option(s) (${merged.length} total)`);
    return { dry: true, added };
  }
  const { res, json } = await metaFetch(
    baseId,
    token,
    `/tables/${table.id}/fields/${subField.id}`,
    {
      method: "PATCH",
      body: JSON.stringify({
        type: "singleSelect",
        options: { choices: merged },
      }),
    }
  );
  if (!res.ok) {
    console.error(`  FAIL ${table.name}:`, res.status, JSON.stringify(json));
    console.warn(
      `  Hint: Meta API field PATCH may be blocked on this base/token. ` +
        `Run npm run backfill:built-country-submarkets:apply — writes use typecast:true and auto-create select options.`
    );
    return { failed: true };
  }
  console.log(`  ${table.name}: added ${added} submarket option(s) (${merged.length} total)`);
  return { updated: true, added };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = getDemandAnchorsBaseId();
  if (!token || !baseId) {
    console.error("AIRTABLE_API_KEY and base ID required");
    process.exit(1);
  }
  console.log(APPLY ? "=== APPLY ===" : "=== DRY RUN ===");
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    console.error("Failed to load base schema:", res.status, json);
    process.exit(1);
  }
  const tables = json.tables || [];
  const da = tables.find((t) => t.name === DEMAND_ANCHORS_TABLE);
  const ti = tables.find((t) => t.name === TRAVEL_INFRASTRUCTURE_TABLE);
  if (!da) console.warn("Demand Anchors table not found");
  if (!ti) console.warn("Travel Infrastructure table not found");
  if (da) await patchSubmarketField(baseId, token, da, SUBMARKET_FIELD_NAME);
  if (ti) await patchSubmarketField(baseId, token, ti, SUBMARKET_FIELD_NAME);
  console.log("Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
