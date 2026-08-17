/**
 * Classify GTM Companies by CALA hotel footprint (via Properties True Owner / operator roles).
 *
 * Usage:
 *   node scripts/sync-gtm-company-cala-flags.mjs
 *   node scripts/sync-gtm-company-cala-flags.mjs --apply
 *   node scripts/ensure-gtm-costar-companies-table.mjs --apply   # add fields first
 *
 * Reports:
 *   reports/gtm-company-cala-flags.json
 *   reports/gtm-company-cala-flags.csv
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  GTM_COMPANY_TABLE,
  MAP_GTM_COMPANY,
  VAL_GTM_COMPANY_CALA_HOTELS,
} from "../lib/gtm-owner-target/company-field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import {
  fetchAllGtmProperties,
  groupAirtablePropertiesByOwner,
} from "../lib/gtm-owner-target/properties-read.js";
import { COMPANY_PROFILE_ENRICHMENTS } from "../lib/gtm-owner-target/company-profile-enrichments.js";
import {
  buildCompanyCalaMatchContext,
  classifyCompanyCalaFootprint,
} from "../lib/gtm-owner-target/company-cala-match.js";
import { summarizePropertyFootprint } from "../lib/gtm-owner-target/cala-footprint.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_JSON = join(__dirname, "..", "reports", "gtm-company-cala-flags.json");
const REPORT_CSV = join(__dirname, "..", "reports", "gtm-company-cala-flags.csv");
const APPLY = process.argv.includes("--apply");

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

async function updateBatchesWithRetry(base, tableName, updates, { batchSize = 5, retries = 4 } = {}) {
  let updated = 0;
  const batches = chunk(updates, batchSize);
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    let lastError;
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        await base(tableName).update(batch, { typecast: true });
        updated += batch.length;
        lastError = null;
        break;
      } catch (err) {
        lastError = err;
        if (attempt < retries) {
          await new Promise((resolve) => setTimeout(resolve, attempt * 2000));
        }
      }
    }
    if (lastError) throw lastError;
    if ((i + 1) % 20 === 0) {
      console.log(`  …${updated}/${updates.length} company records updated`);
    }
  }
  return updated;
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const [{ records: propertyRecords }, companyRecords] = await Promise.all([
    fetchAllGtmProperties(),
    base(GTM_COMPANY_TABLE)
      .select({
        fields: [
          MAP_GTM_COMPANY.company,
          MAP_GTM_COMPANY.hqCity,
          MAP_GTM_COMPANY.hqCountry,
        ],
      })
      .all(),
  ]);

  const ownerGroups = groupAirtablePropertiesByOwner(propertyRecords);
  const propertyFootprint = summarizePropertyFootprint(propertyRecords.map((r) => r.row));
  const companyNames = companyRecords.map((r) => String(r.fields[MAP_GTM_COMPANY.company] || ""));
  const context = buildCompanyCalaMatchContext({
    ownerGroups,
    companyNames,
    profileEnrichments: COMPANY_PROFILE_ENRICHMENTS,
    propertyRecords,
  });

  /** @type {object[]} */
  const rows = [];
  const summary = {
    yes: 0,
    no: 0,
    unknown: 0,
    matchTypes: {},
  };

  /** @type {{ id: string, fields: Record<string, unknown> }[]} */
  const updates = [];

  for (const rec of companyRecords) {
    const companyName = String(rec.fields[MAP_GTM_COMPANY.company] || "").trim();
    const result = classifyCompanyCalaFootprint(companyName, context);
    summary[result.calaHotels]++;
    summary.matchTypes[result.matchType] = (summary.matchTypes[result.matchType] || 0) + 1;

    const row = {
      companyId: rec.id,
      company: companyName,
      hqCity: rec.fields[MAP_GTM_COMPANY.hqCity] || "",
      hqCountry: rec.fields[MAP_GTM_COMPANY.hqCountry] || "",
      calaHotels: result.calaHotels,
      calaPropertyCount: result.calaPropertyCount,
      totalPropertyCount: result.totalPropertyCount,
      calaCountriesSummary: result.calaCountriesSummary,
      matchedOwnerName: result.matchedOwnerName || "",
      matchType: result.matchType,
      calaOnly: result.calaOnly,
    };
    rows.push(row);

    if (!VAL_GTM_COMPANY_CALA_HOTELS.includes(result.calaHotels)) continue;

    updates.push({
      id: rec.id,
      fields: {
        [MAP_GTM_COMPANY.calaHotels]: result.calaHotels,
        [MAP_GTM_COMPANY.calaPropertyCount]: result.calaPropertyCount,
        [MAP_GTM_COMPANY.calaCountriesSummary]: result.calaCountriesSummary || null,
        [MAP_GTM_COMPANY.matchedOwnerName]: result.matchedOwnerName || null,
        [MAP_GTM_COMPANY.calaMatchType]: result.matchType,
      },
    });
  }

  rows.sort((a, b) => {
    const rank = { yes: 0, no: 1, unknown: 2 };
    return (
      (rank[a.calaHotels] ?? 9) - (rank[b.calaHotels] ?? 9) ||
      String(a.company).localeCompare(String(b.company))
    );
  });

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    propertyScope: {
      totalProperties: propertyFootprint.totalPropertyCount,
      calaProperties: propertyFootprint.calaPropertyCount,
      distinctTrueOwners: ownerGroups.length,
      ownersWithCalaHotels: ownerGroups.filter((g) =>
        summarizePropertyFootprint(g.properties).hasCalaHotels
      ).length,
    },
    companySummary: {
      totalCompanies: companyRecords.length,
      calaYes: summary.yes,
      calaNo: summary.no,
      calaUnknown: summary.unknown,
      matchTypes: summary.matchTypes,
    },
    rows,
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  const csvColumns = [
    "company",
    "hqCity",
    "hqCountry",
    "calaHotels",
    "calaPropertyCount",
    "calaCountriesSummary",
    "matchedOwnerName",
    "matchType",
  ];
  writeFileSync(
    REPORT_CSV,
    [csvColumns.join(","), ...rows.map((r) => csvColumns.map((c) => csvEscape(r[c])).join(","))].join(
      "\n"
    )
  );

  console.log("GTM Company CALA footprint classification");
  console.log(
    `  Properties: ${propertyFootprint.totalPropertyCount} total, ${propertyFootprint.calaPropertyCount} CALA`
  );
  console.log(
    `  True Owners (from properties): ${ownerGroups.length}, ${report.propertyScope.ownersWithCalaHotels} with CALA hotels`
  );
  console.log(`  Companies: ${companyRecords.length}`);
  console.log(`    CALA yes: ${summary.yes}`);
  console.log(`    CALA no:  ${summary.no}`);
  console.log(`    unknown:  ${summary.unknown}`);
  console.log("  Match types:", JSON.stringify(summary.matchTypes));

  console.log("\nSample CALA companies:");
  for (const row of rows.filter((r) => r.calaHotels === "yes").slice(0, 10)) {
    console.log(
      `  ${row.company} — ${row.calaPropertyCount} CALA props (${row.calaCountriesSummary}) [${row.matchType}]`
    );
  }

  if (!APPLY) {
    console.log(`\nDry-run. Wrote ${REPORT_JSON}`);
    console.log(`Wrote ${REPORT_CSV}`);
    console.log("Add fields: node scripts/ensure-gtm-costar-companies-table.mjs --apply");
    console.log("Then write: node scripts/sync-gtm-company-cala-flags.mjs --apply");
    return;
  }

  const updated = await updateBatchesWithRetry(base, GTM_COMPANY_TABLE, updates);

  console.log(`\nApplied ${updated} company updates.`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_CSV}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
