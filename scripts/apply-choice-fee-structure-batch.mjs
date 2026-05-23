/**
 * Populate Brand Setup - Fee Structure from fixtures/choice-fdd-text (Choice FDD Item 6)
 * plus tier metadata in scripts/lib/choice-fee-structure-profiles.mjs.
 *
 * Note: Airtable table used by the app is "Brand Setup - Fee Structure". A table named
 * "Brand Setup - Project Fee Structure" is not readable with the current API token in this repo.
 *
 *   node scripts/apply-choice-fee-structure-batch.mjs --dry-run
 *   node scripts/apply-choice-fee-structure-batch.mjs --overwrite
 *   node scripts/apply-choice-fee-structure-batch.mjs --brand "Comfort Inn & Suites" --overwrite
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  CHOICE_FEE_TARGET_BRANDS,
  CHOICE_FEE_FDD_FILE,
  CHOICE_FEE_TIER,
  CHOICE_FEE_OVERRIDES,
} from "./lib/choice-fee-structure-profiles.mjs";
import { readFddText, parseChoiceFddItem6Fees } from "./lib/parse-choice-fdd-item6-fees.mjs";
import { pickBasis } from "./lib/fee-structure-basis-normalize.mjs";
import { FDD_FIELD_DISCLAIMER } from "../lib/external-owner-copy.mjs";

const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_FEE = "Brand Setup - Fee Structure";
const LINK_FIELD = "Brand Setup - Fee Structure";

const BASIS_COLS = [
  "Basis - Typical Application Fee",
  "Basis - Typical Royalty Fee Range",
  "Basis - Typical Marketing Fee Range",
  "Basis - Typical Tech",
  "Basis - Typical Loyalty Program Fee",
  "Basis - Typical Reservation / Distribution Fee",
  "Basis - Typical Training Fee",
];

const SKIP_FIELD = new Set([
  "Brand",
  "Brand Name",
  "BrandIDLookup",
  "Record_ID",
  "Fee_Structure_ID",
  "User_Record_ID",
  "DELETE>>>>",
]);

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

async function feeRecordIdForBrand(base, basicsRow) {
  const link = basicsRow.fields[LINK_FIELD];
  if (Array.isArray(link) && link[0]) return link[0];
  return null;
}

function pctToDecimal(p) {
  if (p == null || Number.isNaN(p)) return null;
  return Math.round(p * 1000) / 100000;
}

async function updateWithPruning(base, recordId, fields) {
  let payload = { ...fields };
  for (let attempt = 0; attempt < 25; attempt++) {
    if (!Object.keys(payload).length) return;
    try {
      await base(TABLE_FEE).update(recordId, payload, { typecast: true });
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
          console.warn(`  Skip invalid/read-only: ${m[1]}`);
          delete payload[m[1]];
          continue;
        }
      }
      throw err;
    }
  }
}

function mergeParsed(brandName, parsed) {
  const o = CHOICE_FEE_OVERRIDES[brandName];
  if (!o) return { ...parsed };
  return {
    ...parsed,
    royaltyMin: o.royaltyMin ?? parsed.royaltyMin,
    royaltyMax: o.royaltyMax ?? parsed.royaltyMax,
    marketingReservationPct: o.marketingReservationPct ?? parsed.marketingReservationPct,
    loyaltyMin: o.loyaltyMin ?? parsed.loyaltyMin,
    loyaltyMax: o.loyaltyMax ?? parsed.loyaltyMax,
    techPerRoomMonthly: o.techPerRoomMonthly ?? parsed.techPerRoomMonthly,
  };
}

async function main() {
  const { dryRun, overwrite, brandFilter } = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const base = new Airtable({ apiKey }).base(baseId);

  const basisChoices = {};
  for (const col of BASIS_COLS) {
    basisChoices[col] = await getTableFieldChoices(baseId, apiKey, TABLE_FEE, col);
  }

  const feeBandChoices = await getTableFieldChoices(baseId, apiKey, TABLE_FEE, "Fee Positioning Band");
  const pipChoices = await getTableFieldChoices(baseId, apiKey, TABLE_FEE, "Typical CapEx / PIP Intensity Band");
  const earlyTermChoices = await getTableFieldChoices(
    baseId,
    apiKey,
    TABLE_FEE,
    "Typical Owner Early-Termination Rights (without cause)"
  );
  const termStructChoices = await getTableFieldChoices(
    baseId,
    apiKey,
    TABLE_FEE,
    "Typical Termination Fee Structure (if any)"
  );
  const perfTermChoices = await getTableFieldChoices(
    baseId,
    apiKey,
    TABLE_FEE,
    "Who Can Exercise Termination Right After Failed Test?"
  );
  const keyMoneyChoices = await getTableFieldChoices(baseId, apiKey, TABLE_FEE, "Key Money / Co-Investment");
  const capReimbChoices = await getTableFieldChoices(
    baseId,
    apiKey,
    TABLE_FEE,
    "Do Agreements Typically Cap Operator Reimbursable Expenses?"
  );
  const auditChoices = await getTableFieldChoices(
    baseId,
    apiKey,
    TABLE_FEE,
    "Do You Usually Require Audit Rights for Owner Books / Operator Systems?"
  );

  let brands = CHOICE_FEE_TARGET_BRANDS;
  if (brandFilter) brands = [brandFilter];

  for (const brandName of brands) {
    const tier = CHOICE_FEE_TIER[brandName];
    if (!tier) {
      console.warn(`No tier metadata for: ${brandName}`);
      continue;
    }
    const fddFile = CHOICE_FEE_FDD_FILE[brandName];
    const text = fddFile ? readFddText(fddFile) : null;
    let parsed = text ? parseChoiceFddItem6Fees(text) : {};
    parsed = mergeParsed(brandName, parsed);

    const roy = parsed.royaltyMin;
    const royMax = parsed.royaltyMax ?? parsed.royaltyMin;
    const mr = parsed.marketingReservationPct;
    let lMin = parsed.loyaltyMin;
    let lMax = parsed.loyaltyMax;
    if (lMin == null && lMax == null) {
      lMin = 4.5;
      lMax = 5.5;
    }
    const tech = parsed.techPerRoomMonthly ?? tier.techFallback;

    const basics = await findBasicsByName(base, brandName);
    if (!basics) {
      console.warn(`Skip ${brandName}: no Brand Basics row`);
      continue;
    }
    const feeId = await feeRecordIdForBrand(base, basics);
    if (!feeId) {
      console.warn(`Skip ${brandName}: no linked Fee Structure on Brand Basics`);
      continue;
    }

    const fddNote = fddFile ? FDD_FIELD_DISCLAIMER : "";

    const fields = {
      "Min - Typical Application Fee": tier.appMin,
      "Max - Typical Application Fee": tier.appMax,
      "Basis - Typical Application Fee": pickBasis(
        basisChoices["Basis - Typical Application Fee"],
        "Base + Per Room Over Threshold"
      ),
      "Application Fee Per Unit Over Threshold": tier.appPerRoom,
      "Application Fee Threshold (Units)": tier.appThresholdRooms,
      "Additional Notes - Typical Application Fee": fddNote,

      "Min - Typical Royalty Fee Range": roy != null ? pctToDecimal(roy) : null,
      "Max - Typical Royalty Fee Range": royMax != null ? pctToDecimal(royMax) : null,
      "Basis - Typical Royalty Fee Range": pickBasis(
        basisChoices["Basis - Typical Royalty Fee Range"],
        "% of Rooms Revenue"
      ),
      "Additional Notes - Typical Royalty Fee Range": fddNote,

      "Min - Typical Marketing Fee Range": mr != null ? pctToDecimal(mr) : null,
      "Max - Typical Marketing Fee Range": mr != null ? pctToDecimal(mr) : null,
      "Basis - Typical Marketing Fee Range": pickBasis(
        basisChoices["Basis - Typical Marketing Fee Range"],
        "% of Rooms Revenue"
      ),
      "Additional Notes - Typical Marketing Fee Range":
        "Choice Item 6 lists a combined Marketing and Reservation Fee; the same percentage is recorded here. Reservation / distribution line is left blank unless separately stated in the FDD.",

      "Min - Typical Tech": tech,
      "Max - Typical Tech": tech,
      "Basis - Typical Tech": pickBasis(basisChoices["Basis - Typical Tech"], "Per Room / Month"),
      "Additional Notes - Typical Tech": fddNote,

      "Min - Typical Loyalty Program Fee": lMin != null ? pctToDecimal(lMin) : null,
      "Max - Typical Loyalty Program Fee": lMax != null ? pctToDecimal(lMax) : null,
      "Basis - Typical Loyalty Program Fee": pickBasis(
        basisChoices["Basis - Typical Loyalty Program Fee"],
        "% of Gross Revenue"
      ),
      "Additional Notes - Typical Loyalty Program Fee": fddNote,

      "Min - Typical Training Fee": 3345,
      "Max - Typical Training Fee": 5295,
      "Basis - Typical Training Fee": pickBasis(basisChoices["Basis - Typical Training Fee"], "One-Time"),
      "Additional Notes - Typical Training Fee":
        "Immersion + HOST pre-opening training (Choice Item 5 template); excludes travel. Confirm current tuition in FDD Item 11.",

      "Typical Incentives Offered":
        "Negotiated case-by-case (conversions, multi-unit, strategic markets). Confirm active Choice development incentives at signing.",

      "Typical Owner Early-Termination Rights (without cause)": pickBasis(earlyTermChoices, "Sometimes"),
      "Early-Termination Notes":
        "Cure periods and liquidated damages vary by agreement generation—review FDD Item 17 and franchise agreement.",

      "Typical Termination Fee Structure (if any)": pickBasis(termStructChoices, "Case-by-Case"),
      "Typical Termination Fee Structure (if any) Text":
        "Liquidated damages and unrealized-fee formulas are agreement-specific; model using FDD Item 17 and legal review.",

      "Who Can Exercise Termination Right After Failed Test?": pickBasis(perfTermChoices, "Mutual"),

      "Key Money / Co-Investment": pickBasis(keyMoneyChoices, "Only In Select Deals"),
      "Typical Expectations for Owner-Funded Reserves":
        "FF&E reserves, PIP reserves, and replacement cycles per brand standards and PIP schedule—underwrite from FDD Item 7 and architecture.",

      "Do Agreements Typically Cap Operator Reimbursable Expenses?": pickBasis(capReimbChoices, "No"),
      "Do You Usually Require Audit Rights for Owner Books / Operator Systems?": pickBasis(auditChoices, "Yes"),

      "Fee Positioning Band": pickBasis(feeBandChoices, tier.feeBand),
      "Typical CapEx / PIP Intensity Band": pickBasis(pipChoices, tier.pipBand),
    };

    for (const k of Object.keys(fields)) {
      if (fields[k] === null || fields[k] === undefined) delete fields[k];
    }

    if (!overwrite && !dryRun) {
      const existing = await base(TABLE_FEE).find(feeId);
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
      console.log(`[dry-run] ${brandName}`, { feeId, keys: Object.keys(fields).length, roy, mr, tech, lMin, lMax });
      continue;
    }

    await updateWithPruning(base, feeId, fields);
    console.log(`Updated Fee Structure ${brandName} (${feeId}) fields: ${Object.keys(fields).length}`);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
