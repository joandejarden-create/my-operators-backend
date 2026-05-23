/**
 * Populate Brand Setup - Deal Terms from fixtures/choice-fdd-text (Item 17 initial term / renewal summary)
 * plus tiered PIP estimates in scripts/lib/choice-deal-terms-profiles.mjs.
 *
 *   node scripts/apply-choice-deal-terms-batch.mjs --dry-run
 *   node scripts/apply-choice-deal-terms-batch.mjs --overwrite
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  CHOICE_DEAL_TERMS_BRANDS,
  CHOICE_DEAL_TERMS_FDD_FILE,
  CHOICE_DEAL_PIP_CONVERSION_USD,
} from "./lib/choice-deal-terms-profiles.mjs";
import { readFddText, parseChoiceFddDealTerms } from "./lib/parse-choice-fdd-item17-deal-terms.mjs";
import { pickBasis } from "./lib/fee-structure-basis-normalize.mjs";
import { FDD_FIELD_DISCLAIMER } from "../lib/external-owner-copy.mjs";

const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_DEAL = "Brand Setup - Deal Terms";
const LINK_FIELD = "Brand Setup - Deal Terms";

const SKIP_FIELD = new Set([
  "Brand",
  "Brand Name",
  "BrandIDLookup",
  "Record_ID",
  "Deal_Terms_ID",
  "User_Record_ID",
]);

const SELECT_COLS = [
  "Duration - Typical Minimum Initial Term",
  "Duration - Typical Renewal Option",
  "Quantity - Typical Renewal Notice Period",
  "Renewal Structure",
  "Renewal Notice Responsibility",
  "Mandatory PIP at Renewal",
  "Mandatory PIP for Conversions",
  "Mandatory PIP at Renewal",
  "Mandatory PIP for Conversions",
  "Conversion - Typical max time allowed for completion -Duration",
  "Renewal - Typical max time allowed for completion -Duration",
];

function parseArgs(argv) {
  const args = argv.slice(2);
  const bi = args.indexOf("--brand");
  return {
    dryRun: args.includes("--dry-run"),
    overwrite: args.includes("--overwrite"),
    brandFilter: bi >= 0 ? String(args[bi + 1] || "").trim() : "",
  };
}

async function getTableFieldChoices(baseId, apiKey, tableName, fieldName) {
  const r = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (!r.ok) throw new Error(`Meta API ${r.status}`);
  const j = await r.json();
  const t = j.tables.find((x) => x.name === tableName);
  const f = t?.fields?.find((x) => x.name === fieldName);
  return (f?.options?.choices || []).map((c) => c.name);
}

async function findBasicsByName(base, brandName) {
  const esc = String(brandName).replace(/"/g, '\\"');
  const rows = await base(TABLE_BASICS)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

function dealRecordIdFromBasics(basicsRow) {
  const link = basicsRow.fields[LINK_FIELD];
  if (Array.isArray(link) && link[0]) return link[0];
  return null;
}

async function updateWithPruning(base, recordId, fields) {
  let payload = { ...fields };
  for (let attempt = 0; attempt < 25; attempt++) {
    if (!Object.keys(payload).length) return;
    try {
      await base(TABLE_DEAL).update(recordId, payload, { typecast: true });
      return;
    } catch (err) {
      const msg = String(err.message || err);
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const m = msg.match(/Unknown field name: "([^"]+)"/);
        if (m && Object.hasOwn(payload, m[1])) {
          delete payload[m[1]];
          continue;
        }
      }
      if (err.error === "INVALID_VALUE_FOR_COLUMN") {
        const m = msg.match(/Field "([^"]+)"/);
        if (m && Object.hasOwn(payload, m[1])) {
          console.warn(`  Skip invalid: ${m[1]}`);
          delete payload[m[1]];
          continue;
        }
      }
      throw err;
    }
  }
}

async function main() {
  const { dryRun, overwrite, brandFilter } = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const base = new Airtable({ apiKey }).base(baseId);

  const choices = {};
  for (const col of SELECT_COLS) {
    choices[col] = await getTableFieldChoices(baseId, apiKey, TABLE_DEAL, col);
  }

  let brands = CHOICE_DEAL_TERMS_BRANDS;
  if (brandFilter) brands = [brandFilter];

  for (const brandName of brands) {
    const fddFile = CHOICE_DEAL_TERMS_FDD_FILE[brandName];
    const text = fddFile ? readFddText(fddFile) : null;
    const parsed = text ? parseChoiceFddDealTerms(text) : { initialYears: null, noRenewal: false };
    const years = parsed.initialYears ?? 20;

    const pipUsd = CHOICE_DEAL_PIP_CONVERSION_USD[brandName];
    if (pipUsd == null) console.warn(`No PIP default for ${brandName}`);

    const basics = await findBasicsByName(base, brandName);
    if (!basics) {
      console.warn(`Skip ${brandName}: no Brand Basics row`);
      continue;
    }
    const dealId = dealRecordIdFromBasics(basics);
    if (!dealId) {
      console.warn(`Skip ${brandName}: no linked Deal Terms on Brand Basics`);
      continue;
    }

    const fddNote = fddFile ? FDD_FIELD_DISCLAIMER : "";

    const renewalStructure = parsed.noRenewal
      ? pickBasis(choices["Renewal Structure"], "Other")
      : pickBasis(choices["Renewal Structure"], "Renewal by Mutual Agreement Only");

    const fields = {
      "Quantity - Typical Minimum Initial Term": "1",
      "Length - Typical Minimum Initial Term": String(years),
      "Duration - Typical Minimum Initial Term": pickBasis(
        choices["Duration - Typical Minimum Initial Term"],
        "Year(s)"
      ),

      "Renewal Structure": renewalStructure,
      "Renewal Notice Responsibility": pickBasis(
        choices["Renewal Notice Responsibility"],
        "Mutual"
      ),
      "Typical Renewal Conditions": parsed.noRenewal
        ? `No contractual renewal right after the ${years}-year initial term; continued operation may require a new franchise agreement on then-current terms and fees. ${fddNote}`
        : `Renewal terms vary; ${fddNote}`,

      "Length - Typical Renewal Notice Period": "12",
      "Quantity - Typical Renewal Notice Period": pickBasis(
        choices["Quantity - Typical Renewal Notice Period"],
        "Month(s)"
      ),

      "Performance Test Requirement": pickBasis(choices["Performance Test Requirement"], "Yes"),

      "Typical Cure Period for Performance Test Failure":
        "See FDD Item 17(g): 10 days (fee/report defaults); 30 days (other material defaults).",

      "Typical QA": pickBasis(choices["Typical QA"], "Yes"),
      "Mandatory PIP at Renewal": pickBasis(choices["Mandatory PIP at Renewal"], "Yes"),
      "Mandatory PIP for Conversions": pickBasis(choices["Mandatory PIP for Conversions"], "Yes"),
      "Typical Mandatory PIP for Conversions ($/room)": pipUsd ?? 6500,

      "Conversion - Typical max time allowed for completion": "24",
      "Conversion - Typical max time allowed for completion -Duration": pickBasis(
        choices["Conversion - Typical max time allowed for completion -Duration"],
        "Month(s)"
      ),
      "Renewal - Typical max time allowed for completion": "24",
      "Renewal - Typical max time allowed for completion -Duration": pickBasis(
        choices["Renewal - Typical max time allowed for completion -Duration"],
        "Month(s)"
      ),
    };

    if (!parsed.noRenewal) {
      fields["Quantity - Typical Renewal Option"] = "1";
      fields["Length - Typical Renewal Option"] = "10";
      fields["Duration - Typical Renewal Option"] = pickBasis(
        choices["Duration - Typical Renewal Option"],
        "Year(s)"
      );
    }

    for (const k of Object.keys(fields)) {
      if (fields[k] === null || fields[k] === undefined) delete fields[k];
    }

    if (!overwrite && !dryRun) {
      const existing = await base(TABLE_DEAL).find(dealId);
      for (const k of Object.keys(fields)) {
        if (SKIP_FIELD.has(k)) continue;
        const cur = existing.get(k);
        if (cur !== undefined && cur !== null && cur !== "") delete fields[k];
      }
    }

    for (const k of Object.keys(fields)) {
      if (SKIP_FIELD.has(k)) delete fields[k];
    }

    if (dryRun) {
      console.log(`[dry-run] ${brandName}`, {
        dealId,
        years,
        noRenewal: parsed.noRenewal,
        keys: Object.keys(fields).length,
      });
      continue;
    }

    await updateWithPruning(base, dealId, fields);
    console.log(`Updated Deal Terms ${brandName} (${dealId}) fields: ${Object.keys(fields).length}`);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
