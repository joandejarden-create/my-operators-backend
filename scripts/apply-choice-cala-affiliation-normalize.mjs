#!/usr/bin/env node
/**
 * Normalize Choice Hotels CALA Hotel Census Affiliation → Brand Alias Mapping canonical names.
 *
 *   node scripts/apply-choice-cala-affiliation-normalize.mjs --dry-run
 *   node scripts/apply-choice-cala-affiliation-normalize.mjs --apply
 *
 * Uses AIRTABLE_API_KEY + AIRTABLE_BASE_ID_ALT (load-env.js).
 * Writes reports/choice-cala-affiliation-normalize-plan.json
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { planChoiceAffiliationNormalize } from "../lib/hotel-census/plan-choice-affiliation-normalize.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY;
const PLAN_JSON = join("reports", "choice-cala-affiliation-normalize-plan.json");
const LOG_CSV = join("reports", "choice-cala-affiliation-normalize-applies.csv");

async function main() {
  mkdirSync("reports", { recursive: true });

  console.log("Planning Choice CALA Affiliation normalize (Brand Alias Mapping)…");
  const plan = await planChoiceAffiliationNormalize({ calaOnly: true });

  const report = {
    ...plan,
    apply: APPLY,
    dryRun: DRY_RUN,
  };

  writeFileSync(PLAN_JSON, JSON.stringify(report, null, 2));

  console.log(`\nChoice Parent census rows: ${plan.censusRowsWithChoiceParent}`);
  console.log(`CALA Choice rows scanned: ${plan.calaChoiceRowsScanned}`);
  console.log(`Ready to apply: ${plan.readyToApply}`);
  console.log(`Already canonical: ${plan.alreadyCanonical}`);
  console.log(`Steward review: ${plan.stewardReviewCount}`);
  console.log(`Protected blocked: ${plan.protectedBlockedCount}`);
  console.log(`Non-CALA skipped: ${plan.skippedNonCalaCount}`);

  console.log("\nAffiliation inventory (CALA Choice Parent):");
  for (const [aff, n] of Object.entries(plan.affiliationInventory).sort(
    (a, b) => b[1] - a[1]
  )) {
    console.log(`  ${n}\t${aff}`);
  }

  console.log("\nBefore → after counts (ready):");
  const transitions = Object.entries(plan.beforeAfterCounts).sort((a, b) => b[1] - a[1]);
  if (!transitions.length) {
    console.log("  (none — affiliation already canonical or only parent fills)");
  }
  for (const [t, n] of transitions) {
    console.log(`  ${n}\t${t}`);
  }

  const parentFillOnly = plan.planRows.filter((r) => r.changeType === "parent_fill_only");
  if (parentFillOnly.length) {
    console.log(`\nParent Company fill-only (already canonical Affiliation): ${parentFillOnly.length}`);
  }

  if (plan.aliasContext.conflicts.length) {
    console.log("\nAlias map conflicts (excluded from safe apply):");
    for (const c of plan.aliasContext.conflicts) {
      console.log(`  ${c.affiliationOrAlias} → [${c.canonicals.join(" | ")}]`);
    }
  }

  if (plan.stewardReview.length) {
    console.log("\nSteward review sample (up to 20):");
    for (const row of plan.stewardReview.slice(0, 20)) {
      console.log(
        `  ${row.censusName} | ${row.country} | Aff="${row.currentAffiliation}" | ${row.reason}`
      );
    }
    if (plan.stewardReview.length > 20) {
      console.log(`  … +${plan.stewardReview.length - 20} more (see plan JSON)`);
    }
  }

  console.log(`\nPlan written: ${PLAN_JSON}`);

  if (DRY_RUN) {
    console.log("Dry-run only — pass --apply to write Airtable (typecast:true).");
    return;
  }

  if (!plan.planRows.length) {
    console.log("Nothing to apply.");
    return;
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
  }
  const base = new Airtable({ apiKey }).base(baseId);

  if (!existsSync(LOG_CSV)) {
    appendFileSync(
      LOG_CSV,
      "appliedAt,censusRecordId,censusName,country,changeType,currentAffiliation,canonicalAffiliation,fieldsJson\n"
    );
  }

  let updated = 0;
  let failed = 0;

  for (const row of plan.planRows) {
    try {
      await base(HOTEL_CENSUS_TABLE).update(row.censusRecordId, row.applyFields, {
        typecast: true,
      });
      updated += 1;
      const name = String(row.censusName || "").replace(/"/g, '""');
      appendFileSync(
        LOG_CSV,
        `${new Date().toISOString()},${row.censusRecordId},"${name}",${row.country || ""},${row.changeType},${JSON.stringify(row.currentAffiliation)},${JSON.stringify(row.canonicalAffiliation || "")},"${JSON.stringify(row.applyFields).replace(/"/g, '""')}"\n`
      );
      console.log(
        "Updated",
        row.censusRecordId,
        row.changeType,
        row.currentAffiliation,
        "→",
        row.applyFields[CENSUS_FIELDS.affiliation] || "(parent only)",
        row.censusName
      );
    } catch (err) {
      failed += 1;
      console.error("FAIL", row.censusRecordId, err?.message || err);
    }
  }

  console.log(`\nApplied: ${updated}  Failed: ${failed}`);
  if (failed) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
