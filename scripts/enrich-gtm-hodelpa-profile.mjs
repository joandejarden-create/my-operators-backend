/**
 * One-off: enrich Hodelpa Hotels from CoStar company profile screenshot.
 * Internal GTM base only.
 */
import "../load-env.js";
import { GTM_COMPANY_TABLE, MAP_GTM_COMPANY } from "../lib/gtm-owner-target/company-field-map.js";
import { MAP_GTM_OWNER_TARGET } from "../lib/gtm-owner-target/field-map.js";
import { GTM_CONTACT_TABLE, MAP_GTM_CONTACT } from "../lib/gtm-owner-target/contact-field-map.js";
import { getGtmAirtableBase, assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";
import { companyDedupeKey } from "../lib/gtm-owner-target/company-to-airtable.js";

const OWNER_TARGET_ID = "recgYY2ewSAPx5OFP";
const JOINT_OWNER_TARGET_ID = "recVPQ00VX7DjRD9A";
const CONTACT_IDS = {
  angel: "rec7dkYOjmPJzO11B",
  rita: "recKyNArdLSp9Hgnd",
};

const COMPANY_FIELDS = {
  [MAP_GTM_COMPANY.company]: "Hodelpa Hotels",
  [MAP_GTM_COMPANY.companyOverview]:
    "Leading hospitality company and the largest hotel chain in the Dominican Republic. Founded in 1990, Hodelpa operates 1,600+ rooms across Santiago, Santo Domingo, Puerto Plata, and Juan Dolio.",
  [MAP_GTM_COMPANY.specialty]: "Hotel Operation, Owner",
  [MAP_GTM_COMPANY.hqMarket]: "Santo Domingo",
  [MAP_GTM_COMPANY.hqCity]: "Santo Domingo",
  [MAP_GTM_COMPANY.hqCountry]: "Dominican Republic",
  [MAP_GTM_COMPANY.website]: "https://hodelpa.com",
  [MAP_GTM_COMPANY.employees]: 328,
  [MAP_GTM_COMPANY.locations]: 1,
  [MAP_GTM_COMPANY.ownedProperties]: 9,
  [MAP_GTM_COMPANY.operatedProperties]: 9,
  [MAP_GTM_COMPANY.companyDedupeKey]: companyDedupeKey({
    Company: "Hodelpa Hotels",
    "HQ City": "Santo Domingo",
    "HQ Country": "Dominican Republic",
  }),
  [MAP_GTM_COMPANY.sourceFile]: "costar_profile_manual_2026-06-25",
  [MAP_GTM_COMPANY.internalNotes]: [
    "Established 1990.",
    "Leading hospitality company and largest hotel chain in the Dominican Republic.",
    "1,600+ rooms across Santiago, Santo Domingo, Puerto Plata, and Juan Dolio.",
    "HQ: 12 C. Angel Severo Cabral, 10148 Santo Domingo, Dominican Republic.",
    "Company phone: 00 1 809-683-1000.",
    "Developed: 2 properties (194,429 SF).",
    "Operated: 9 properties (945,743 SF; 1,474 rooms).",
    "Source: CoStar company profile (manual enrichment).",
  ].join("\n"),
};

const OWNER_TARGET_FIELDS = {
  [MAP_GTM_OWNER_TARGET.primaryContactName]: "Angel Hernandez Rojas",
  [MAP_GTM_OWNER_TARGET.primaryContactEmail]: "ARojas@hodelpa.com",
  [MAP_GTM_OWNER_TARGET.primaryContactPhone]: "00 1 809-683-1000",
  [MAP_GTM_OWNER_TARGET.contactPath]: "linkedin",
  [MAP_GTM_OWNER_TARGET.pitchAngle]:
    "Dominican Republic's largest hotel chain (est. 1990) — integrated owner-operator with 9 assets and ~1,474 rooms across Santiago, Santo Domingo, Puerto Plata, and Juan Dolio. Strong fit for portfolio-level brand/operator evaluation and multi-asset deal workspace on Dealality.",
  [MAP_GTM_OWNER_TARGET.internalNotes]: [
    "CoStar company profile enriched 2026-06-25.",
    "Executive President: Angel Hernandez Rojas (ARojas@hodelpa.com).",
    "CFO: Rita Claudia Arias (RArias@hodelpa.com) — use for financial / portfolio discussions.",
    "CoStar stats: 328 employees; 9 owned/operated properties; 945,743 SF; 1,474 rooms.",
    "Note: Hodelpa Garden Court is co-owned with Comercial Canabacoa S.A. (separate owner target).",
  ].join("\n"),
};

const JOINT_OWNER_NOTES =
  "CoStar profile note (2026-06-25): Hodelpa Hotels is co-owner alongside Comercial Canabacoa S.A. Primary Hodelpa portfolio target: Owner Target 'Hodelpa Hotels' (8 properties). Key contact: Angel Hernandez Rojas, Executive President.";

const CONTACT_NOTE_APPEND =
  "CoStar company profile (2026-06-25): Hodelpa is the largest hotel chain in the Dominican Republic (est. 1990), 1,600+ rooms, HQ Santo Domingo.";

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const company = await base(GTM_COMPANY_TABLE).create([{ fields: COMPANY_FIELDS }], { typecast: true });
  console.log("Created CoStar Companies:", company[0].id);

  await base("Owner Targets").update([
    { id: OWNER_TARGET_ID, fields: OWNER_TARGET_FIELDS },
    {
      id: JOINT_OWNER_TARGET_ID,
      fields: { [MAP_GTM_OWNER_TARGET.internalNotes]: JOINT_OWNER_NOTES },
    },
  ]);
  console.log("Updated Owner Targets:", OWNER_TARGET_ID, JOINT_OWNER_TARGET_ID);

  for (const [label, id] of Object.entries(CONTACT_IDS)) {
    const rec = await base(GTM_CONTACT_TABLE).find(id);
    const existing = String(rec.fields[MAP_GTM_CONTACT.internalNotes] || "").trim();
    const notes = existing ? `${existing}\n\n${CONTACT_NOTE_APPEND}` : CONTACT_NOTE_APPEND;
    await base(GTM_CONTACT_TABLE).update([{ id, fields: { [MAP_GTM_CONTACT.internalNotes]: notes } }]);
    console.log("Updated contact:", label, id);
  }

  console.log("\nHodelpa enrichment complete.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
