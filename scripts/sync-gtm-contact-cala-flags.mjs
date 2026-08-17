/**
 * Classify GTM Contacts by CALA hotel footprint (via Owner Target / Properties linkage).
 *
 * Usage:
 *   node scripts/sync-gtm-contact-cala-flags.mjs
 *   node scripts/sync-gtm-contact-cala-flags.mjs --apply
 *   node scripts/ensure-gtm-costar-contacts-table.mjs --apply   # add fields first
 *
 * Reports:
 *   reports/gtm-contact-cala-flags.json
 *   reports/gtm-contact-cala-flags.csv
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  GTM_CONTACT_TABLE,
  MAP_GTM_CONTACT,
  VAL_GTM_CONTACT_CALA_HOTEL,
} from "../lib/gtm-owner-target/contact-field-map.js";
import { GTM_COMPANY_TABLE, MAP_GTM_COMPANY } from "../lib/gtm-owner-target/company-field-map.js";
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
  buildContactCalaMatchContext,
  classifyContactCalaFootprint,
} from "../lib/gtm-owner-target/contact-cala-match.js";
import { summarizePropertyFootprint } from "../lib/gtm-owner-target/cala-footprint.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_JSON = join(__dirname, "..", "reports", "gtm-contact-cala-flags.json");
const REPORT_CSV = join(__dirname, "..", "reports", "gtm-contact-cala-flags.csv");
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

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const [{ records: propertyRecords }, companyRecords, contactRecords] = await Promise.all([
    fetchAllGtmProperties(),
    base(GTM_COMPANY_TABLE).select({ fields: [MAP_GTM_COMPANY.company] }).all(),
    base(GTM_CONTACT_TABLE)
      .select({
        fields: [
          MAP_GTM_CONTACT.name,
          MAP_GTM_CONTACT.email,
          MAP_GTM_CONTACT.company,
          MAP_GTM_CONTACT.title,
        ],
      })
      .all(),
  ]);

  const ownerGroups = groupAirtablePropertiesByOwner(propertyRecords);
  const propertyFootprint = summarizePropertyFootprint(propertyRecords.map((r) => r.row));
  const context = buildContactCalaMatchContext({
    ownerGroups,
    companyNames: companyRecords.map((r) => String(r.fields[MAP_GTM_COMPANY.company] || "")),
    profileEnrichments: COMPANY_PROFILE_ENRICHMENTS,
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

  for (const rec of contactRecords) {
    const contact = {
      name: rec.fields[MAP_GTM_CONTACT.name],
      email: rec.fields[MAP_GTM_CONTACT.email],
      company: rec.fields[MAP_GTM_CONTACT.company],
      title: rec.fields[MAP_GTM_CONTACT.title],
    };
    const result = classifyContactCalaFootprint(contact, context);
    summary[result.calaHotelContact]++;
    summary.matchTypes[result.matchType] = (summary.matchTypes[result.matchType] || 0) + 1;

    const row = {
      contactId: rec.id,
      name: contact.name || "",
      email: contact.email || "",
      company: contact.company || "",
      title: contact.title || "",
      calaHotelContact: result.calaHotelContact,
      calaPropertyCount: result.calaPropertyCount,
      totalPropertyCount: result.totalPropertyCount,
      calaCountriesSummary: result.calaCountriesSummary,
      matchedOwnerName: result.matchedOwnerName || "",
      matchType: result.matchType,
      calaOnly: result.calaOnly,
    };
    rows.push(row);

    if (!VAL_GTM_CONTACT_CALA_HOTEL.includes(result.calaHotelContact)) continue;

    const fields = {
      [MAP_GTM_CONTACT.calaHotelContact]: result.calaHotelContact,
      [MAP_GTM_CONTACT.calaPropertyCount]: result.calaPropertyCount,
      [MAP_GTM_CONTACT.calaCountriesSummary]: result.calaCountriesSummary || null,
      [MAP_GTM_CONTACT.matchedOwnerName]: result.matchedOwnerName || null,
      [MAP_GTM_CONTACT.calaMatchType]: result.matchType,
    };
    updates.push({ id: rec.id, fields });
  }

  rows.sort((a, b) => {
    const rank = { yes: 0, no: 1, unknown: 2 };
    return (
      (rank[a.calaHotelContact] ?? 9) - (rank[b.calaHotelContact] ?? 9) ||
      String(a.company).localeCompare(String(b.company)) ||
      String(a.name).localeCompare(String(b.name))
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
    contactSummary: {
      totalContacts: contactRecords.length,
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
    "name",
    "email",
    "company",
    "title",
    "calaHotelContact",
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

  console.log("GTM Contact CALA footprint classification");
  console.log(
    `  Properties: ${propertyFootprint.totalPropertyCount} total, ${propertyFootprint.calaPropertyCount} CALA`
  );
  console.log(
    `  Owner Targets (from properties): ${ownerGroups.length}, ${report.propertyScope.ownersWithCalaHotels} with CALA hotels`
  );
  console.log(`  Contacts: ${contactRecords.length}`);
  console.log(`    CALA yes: ${summary.yes}`);
  console.log(`    CALA no:  ${summary.no}`);
  console.log(`    unknown:  ${summary.unknown}`);
  console.log("  Match types:", JSON.stringify(summary.matchTypes));

  console.log("\nSample CALA contacts:");
  for (const row of rows.filter((r) => r.calaHotelContact === "yes").slice(0, 10)) {
    console.log(`  ${row.name} @ ${row.company} — ${row.calaPropertyCount} CALA props (${row.calaCountriesSummary})`);
  }

  if (!APPLY) {
    console.log(`\nDry-run. Wrote ${REPORT_JSON}`);
    console.log(`Wrote ${REPORT_CSV}`);
    console.log("Add fields: node scripts/ensure-gtm-costar-contacts-table.mjs --apply");
    console.log("Then write: node scripts/sync-gtm-contact-cala-flags.mjs --apply");
    return;
  }

  let updated = 0;
  for (const batch of chunk(updates, 10)) {
    await base(GTM_CONTACT_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  console.log(`\nApplied ${updated} contact updates.`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_CSV}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
