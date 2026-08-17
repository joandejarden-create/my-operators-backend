#!/usr/bin/env node
/**
 * Ensure Conversion Financing Package fields on Capital Setup - Deal Financing Needs.
 *
 *   node scripts/ensure-conversion-financing-package-schema.mjs --dry-run
 *   node scripts/ensure-conversion-financing-package-schema.mjs --apply
 */
import "../load-env.js";
import { CFP_TABLE } from "../lib/capital-setup/conversion-financing-package-field-map.js";
import { buildConversionFinancingPackageFieldSpecs } from "../lib/capital-setup/conversion-financing-package-airtable-fields.js";

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;

async function metaFetch(baseId, token, pathSuffix, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${pathSuffix}`;
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

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  console.log(DRY ? "=== DRY RUN ===" : "=== APPLY ===");
  console.log("Table:", CFP_TABLE);

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed ${res.status}: ${JSON.stringify(json)}`);

  const table = (json.tables || []).find((t) => t.name === CFP_TABLE);
  if (!table) {
    console.error(`Table not found: ${CFP_TABLE}`);
    console.error("Run scripts/ensure-capital-setup-schema.mjs --apply first.");
    process.exit(1);
  }

  const existing = new Set((table.fields || []).map((f) => f.name));
  const specs = buildConversionFinancingPackageFieldSpecs();
  const failures = [];
  let created = 0;
  let skipped = 0;

  for (const spec of specs) {
    if (existing.has(spec.name)) {
      console.log(`  skip (exists): ${spec.name}`);
      skipped += 1;
      continue;
    }
    if (DRY) {
      console.log(`  [dry-run] would create: ${spec.name}`);
      created += 1;
      continue;
    }
    const body = {
      name: spec.name,
      type: spec.type,
      ...(spec.options ? { options: spec.options } : {}),
      ...(spec.description ? { description: spec.description } : {}),
    };
    const cr = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    if (!cr.res.ok) {
      console.error(`  FAIL ${spec.name}:`, cr.res.status, JSON.stringify(cr.json));
      failures.push({ field: spec.name, status: cr.res.status, error: cr.json });
      continue;
    }
    console.log(`  created: ${spec.name}`);
    created += 1;
    await sleep(220);
  }

  console.log(`\nSummary: ${created} would create/created, ${skipped} skipped, ${failures.length} failures`);
  if (failures.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[ensure-conversion-financing-package-schema]", err);
  process.exit(1);
});
