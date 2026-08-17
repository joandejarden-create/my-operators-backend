#!/usr/bin/env node
/**
 * Backfill Property ID (MARSHA) for CALA Design Hotels census rows from
 * designhotels.com Marriott links and Marriott country sitemaps.
 *
 *   node scripts/apply-design-hotels-census-property-id-backfill.mjs --audit
 *   node scripts/apply-design-hotels-census-property-id-backfill.mjs --dry-run
 *   node scripts/apply-design-hotels-census-property-id-backfill.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  auditDesignHotelsPropertyIdCoverage,
  planDesignHotelsCensusPropertyIdBackfill,
} from "../lib/hotel-census/plan-design-hotels-census-property-id-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const REPORT = join("reports", "design-hotels-census-property-id-backfill-plan.json");
const LOG = join("reports", "design-hotels-census-property-id-backfill-apply-log.json");
const STEWARD = join("reports", "design-hotels-census-property-id-steward-review.json");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const eq = args.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    audit: args.includes("--audit"),
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    fetchDelayMs: Number(get("--fetch-delay-ms") || 600),
    recordId: get("--record-id"),
  };
}

async function main() {
  const opts = parseArgs();

  if (opts.audit || (!opts.apply && !opts.dryRun)) {
    const audit = await auditDesignHotelsPropertyIdCoverage();
    console.log("=== Design Hotels CALA Property ID coverage ===\n");
    console.log("Total rows:", audit.total);
    console.log("With Property ID:", audit.withPropertyId);
    console.log("Missing Property ID:", audit.missingPropertyId);
    if (!opts.apply && !opts.dryRun) {
      console.log("\nRun --dry-run or --apply to backfill MARSHA codes.");
      return;
    }
  }

  if (!opts.apply && !opts.dryRun) return;

  console.log(`\n=== Plan (${opts.apply && !opts.dryRun ? "LIVE" : "DRY RUN"}) ===\n`);

  const plan = await planDesignHotelsCensusPropertyIdBackfill({
    fetchDelayMs: opts.fetchDelayMs,
    recordIds: opts.recordId ? [opts.recordId] : undefined,
    onProgress: (msg) => console.log(" ", msg),
  });

  mkdirSync("reports", { recursive: true });
  writeFileSync(REPORT, JSON.stringify(plan, null, 2), "utf8");
  writeFileSync(
    STEWARD,
    JSON.stringify(
      {
        generatedAt: plan.generatedAt,
        stewardReview: plan.stewardReview,
        unresolved: plan.unresolved,
      },
      null,
      2
    ),
    "utf8"
  );

  console.log("\nScanned:", plan.censusRowsScanned);
  console.log("Marriott sitemap hotels:", plan.marriottSitemapHotels);
  console.log("Ready to apply:", plan.readyToApply);
  console.log("Steward review:", plan.stewardReview.length);
  console.log("Unresolved:", plan.unresolved.length);
  console.log("Report:", REPORT);
  console.log("Steward:", STEWARD);

  for (const row of plan.planRows) {
    console.log(
      `  ${row.marshaCode} | ${row.censusName} | ${row.matchPath} (${row.matchScore?.toFixed?.(2) ?? "-"})`
    );
  }

  if (plan.stewardReview.length) {
    console.log("\nSteward review (not auto-applied):");
    for (const row of plan.stewardReview) {
      console.log(`  ${row.censusName} → ${row.marshaCode} (${row.matchScore?.toFixed?.(2)})`);
    }
  }

  if (plan.unresolved.length) {
    console.log("\nUnresolved:");
    for (const row of plan.unresolved) {
      console.log(`  ${row.censusName}`);
    }
  }

  if (opts.dryRun) return;

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  /** @type {object[]} */
  const logRows = [];
  let updated = 0;
  /** @type {{ id: string, fields: Record<string, unknown> }[]} */
  let batch = [];

  for (const row of plan.planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    logRows.push({ ...row, action: "update" });
    if (batch.length >= BATCH) {
      await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      console.log(`Updated ${updated}/${plan.planRows.length}`);
      batch = [];
    }
  }
  if (batch.length) {
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  const postAudit = await auditDesignHotelsPropertyIdCoverage();
  writeFileSync(
    LOG,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        updated,
        postAudit,
        logRows,
      },
      null,
      2
    ),
    "utf8"
  );

  console.log(`\nApplied ${updated} update(s).`);
  console.log(`Property ID coverage: ${postAudit.withPropertyId}/${postAudit.total}`);
  console.log("Log:", LOG);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
