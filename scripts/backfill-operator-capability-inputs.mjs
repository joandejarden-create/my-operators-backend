#!/usr/bin/env node
/**
 * Backfill P0 Operator Capability fields from legacy deal inputs (conservative; Needs Review when uncertain).
 *
 *   node scripts/backfill-operator-capability-inputs.mjs
 *   node scripts/backfill-operator-capability-inputs.mjs --dry-run
 *   node scripts/backfill-operator-capability-inputs.mjs --limit 50
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  DEALS_TABLE,
  LOCATION_PROPERTY_TABLE,
  STRATEGIC_INTENT_TABLE,
  LOCATION_LINK_FIELD,
  STRATEGIC_INTENT_LINK_FIELD,
} from "../api/schemas/deal-setup-fields.js";
import { fetchDealWithMergedLinkedRecords } from "../api/my-deals.js";
import { inferOperatorCapabilityBackfill } from "../lib/operator-capability-backfill.js";
import { strVal } from "../lib/operator-capability-inputs.js";

function parseArgs() {
  return {
    dryRun: process.argv.includes("--dry-run"),
    limit: (() => {
      const i = process.argv.indexOf("--limit");
      if (i < 0) return 0;
      const n = Number(process.argv[i + 1]);
      return Number.isFinite(n) && n > 0 ? n : 0;
    })(),
    force: process.argv.includes("--force"),
  };
}

function isEmpty(val) {
  if (val == null) return true;
  if (Array.isArray(val)) return val.length === 0;
  return strVal(val) === "";
}

function pickPatch(existing, patch, force) {
  const out = {};
  for (const [k, v] of Object.entries(patch)) {
    if (v == null || v === "") continue;
    if (!force && !isEmpty(existing[k])) continue;
    out[k] = v;
  }
  return out;
}

async function main() {
  const { dryRun, limit, force } = parseArgs();
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const report = {
    dryRun,
    force,
    processed: 0,
    dealsPatched: 0,
    locationPatched: 0,
    siPatched: 0,
    skippedNoInference: 0,
    uncertain: 0,
    errors: [],
    samples: [],
  };

  const rows = [];
  await base(DEALS_TABLE)
    .select({ pageSize: 100 })
    .eachPage((page, next) => {
      rows.push(...page);
      next();
    });

  const slice = limit > 0 ? rows.slice(0, limit) : rows;
  console.log(`Deals to process: ${slice.length} of ${rows.length}`);

  for (const row of slice) {
    report.processed += 1;
    try {
      const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, row.id);
      const merged = full?.deal?.fields || {};
      const inf = inferOperatorCapabilityBackfill(merged);
      if (inf.uncertain) report.uncertain += 1;

      const dealsExisting = merged;
      const locId = Array.isArray(merged[LOCATION_LINK_FIELD])
        ? merged[LOCATION_LINK_FIELD][0]
        : "";
      const siId = Array.isArray(merged[STRATEGIC_INTENT_LINK_FIELD])
        ? merged[STRATEGIC_INTENT_LINK_FIELD][0]
        : "";

      const dealsPatch = pickPatch(dealsExisting, inf.dealsPatch, force);
      const locationPatch = pickPatch(merged, inf.locationPatch, force);
      const siPatch = pickPatch(merged, inf.siPatch, force);

      const hasWork =
        Object.keys(dealsPatch).length ||
        Object.keys(locationPatch).length ||
        Object.keys(siPatch).length;

      if (!hasWork) {
        report.skippedNoInference += 1;
        continue;
      }

      if (report.samples.length < 8) {
        report.samples.push({
          dealId: row.id,
          property: strVal(merged["Property Name"]),
          dealsPatch,
          locationPatch,
          siPatch,
          notes: inf.notes.slice(0, 6),
          uncertain: inf.uncertain,
        });
      }

      if (dryRun) {
        if (Object.keys(dealsPatch).length) report.dealsPatched += 1;
        if (Object.keys(locationPatch).length) report.locationPatched += 1;
        if (Object.keys(siPatch).length) report.siPatched += 1;
        continue;
      }

      if (Object.keys(dealsPatch).length) {
        await base(DEALS_TABLE).update(row.id, dealsPatch);
        report.dealsPatched += 1;
      }
      if (locId && Object.keys(locationPatch).length) {
        await base(LOCATION_PROPERTY_TABLE).update(locId, locationPatch);
        report.locationPatched += 1;
      }
      if (siId && Object.keys(siPatch).length) {
        await base(STRATEGIC_INTENT_TABLE).update(siId, siPatch);
        report.siPatched += 1;
      }
    } catch (e) {
      report.errors.push({ dealId: row.id, message: e.message || String(e) });
    }
  }

  console.log(JSON.stringify(report, null, 2));
  if (report.errors.length) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
