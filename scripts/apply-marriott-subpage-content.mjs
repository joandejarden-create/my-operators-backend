#!/usr/bin/env node
/**
 * Apply marriott subpage-derived amenities (and description if blank) for one hotel.
 *   node scripts/apply-marriott-subpage-content.mjs --url="https://.../pujac.../overview/" --apply
 *   node scripts/apply-marriott-subpage-content.mjs --marsha=PUJAC --record-id=rec54jkFM0zveGu7P --apply
 */
import "../load-env.js";
import Airtable from "airtable";
import { marshaFromMarriottWebsite } from "../lib/marriott-brand-directory-extract.js";
import { marriottOverviewUrlFromWebsite } from "../lib/marriott-hotel-content-fetch.js";
import { fetchMarriottSubpageContent } from "../lib/marriott-subpage-content-fetch.js";
import { planMarriottCensusContentBackfill } from "../lib/hotel-census/plan-marriott-census-content-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const eq = args.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    url: get("--url"),
    marsha: get("--marsha"),
    recordId: get("--record-id"),
    apply: args.includes("--apply"),
  };
}

async function main() {
  const opts = parseArgs();
  const overviewUrl = marriottOverviewUrlFromWebsite(opts.url || "") || opts.url || "";
  const marsha =
    String(opts.marsha || "").trim().toUpperCase() ||
    marshaFromMarriottWebsite(overviewUrl);

  if (!overviewUrl && !marsha) {
    console.error("Provide --url (marriott overview) or --marsha=PUJAC");
    process.exit(1);
  }

  console.log("MARSHA:", marsha || "(resolve from census)");
  console.log("Fetching subpages…");

  const content = await fetchMarriottSubpageContent(overviewUrl || marsha, { marshaCode: marsha });
  console.log("Subpages:", content.subpages.map((s) => `${s.subpage}:${s.status}`).join(", "));
  if (content.parseErrors.length) console.log("Parse notes:", content.parseErrors.join("; "));

  console.log("\n--- Extracted (subpage fallback) ---");
  console.log("Description:", content.description || "(empty — census may already have Bazaarvoice text)");
  console.log("Amenities:", content.amenities.length ? content.amenities.join(", ") : "(empty)");

  if (!content.description && !content.amenitiesText) {
    console.error("\nNo content extracted from subpages.");
    process.exit(1);
  }

  const contentRows = [
    {
      marshaCode: marsha,
      description: content.description,
      amenitiesText: content.amenitiesText,
      website: overviewUrl,
      source: content.source,
    },
  ];

  const planOpts = opts.recordId ? { recordIds: [opts.recordId] } : { marshaCodes: marsha ? [marsha] : [] };
  const plan = await planMarriottCensusContentBackfill({ ...planOpts, contentRows });

  console.log("\nCensus match ready:", plan.readyToApply);
  if (plan.planRows[0]) {
    console.log("Hotel:", plan.planRows[0].censusName, plan.planRows[0].censusRecordId);
    console.log("Apply fields:", Object.keys(plan.planRows[0].applyFields).join(", "));
    for (const [field, value] of Object.entries(plan.planRows[0].applyFields)) {
      const preview = String(value).length > 120 ? `${String(value).slice(0, 120)}…` : value;
      console.log(`  ${field}:`, preview);
    }
  } else if (plan.skipped[0]) {
    console.log("Skipped:", plan.skipped[0]);
  }

  if (!opts.apply) {
    console.log("\nRun with --apply to write to Hotel Census (fill-blank only).");
    return;
  }

  if (!plan.planRows.length) {
    console.error("No census row to update.");
    process.exit(1);
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  await base(HOTEL_CENSUS_TABLE).update(
    [{ id: plan.planRows[0].censusRecordId, fields: plan.planRows[0].applyFields }],
    { typecast: true }
  );
  console.log("\nUpdated census record:", plan.planRows[0].censusRecordId);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
