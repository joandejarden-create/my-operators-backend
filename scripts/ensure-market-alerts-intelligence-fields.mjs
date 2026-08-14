/**
 * Ensure MarketAlerts intelligence fields (Actionable Intelligence V1).
 *
 *   node scripts/ensure-market-alerts-intelligence-fields.mjs
 *   node scripts/ensure-market-alerts-intelligence-fields.mjs --apply
 */
import "../load-env.js";
import { buildMarketAlertsIntelligenceFieldSpecs } from "../api/lib/market-alerts-intelligence-map.js";

const APPLY = process.argv.includes("--apply");
const TABLE_NAME = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";

async function metaFetch(baseId, token, path, init = {}) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  return { res, json: text ? JSON.parse(text) : {} };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  console.log(`Mode: ${APPLY ? "apply" : "dry-run"}`);
  console.log(`Table: ${TABLE_NAME}`);

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(JSON.stringify(json));

  const table = (json.tables || []).find((t) => t.name === TABLE_NAME);
  if (!table) throw new Error(`Table not found: ${TABLE_NAME}`);

  const existing = new Set((table.fields || []).map((f) => f.name));
  const specs = buildMarketAlertsIntelligenceFieldSpecs();
  let wouldCreate = 0;
  let created = 0;

  for (const field of specs) {
    if (existing.has(field.name)) {
      console.log(`Skip (exists): ${field.name}`);
      continue;
    }
    wouldCreate += 1;
    console.log(`${APPLY ? "Creating" : "[dry-run] Would create"}: ${field.name} (${field.type})`);
    if (!APPLY) continue;
    const createdRes = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(field),
    });
    if (!createdRes.res.ok) {
      throw new Error(`Create ${field.name} failed: ${JSON.stringify(createdRes.json)}`);
    }
    created += 1;
    existing.add(field.name);
    console.log(`  → ${createdRes.json.id}`);
  }

  console.log(
    `\nSummary: specs=${specs.length} existing=${existing.size} wouldCreate=${wouldCreate} created=${created}`
  );
  if (!APPLY) console.log("Re-run with --apply to write fields.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
