#!/usr/bin/env node
/**
 * Phase 3A.8 — read Brand Footprint operating presence + new-build counts for cohort.
 * Distinguishes OPERATING_PRESENCE from Region Offered (development consideration).
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const discovery = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, "..", "data/ai-visibility/phase3a8-eligibility-data-discovery.json"),
    "utf8"
  )
);

const FP_FIELDS = [
  "Brand Name",
  "AM Existing Hotel",
  "CALA Existing Hotel",
  "EU Existing Hotel",
  "MEA Existing Hotel",
  "APAC Existing Hotel",
  "Total Existing Hotels",
  "AM Pipeline Hotel",
  "CALA Pipeline Hotel",
  "EU Pipeline Hotel",
  "Total New Build Hotel",
  "Total Conversion Hotel",
  "AM New Build Hotel",
  "CALA New Build Hotel",
  "EU New Build Hotel",
  "New Build Experience (New build %)",
  "Conversion Experience (Conversion %)",
  "Specific Markets/Cities",
  "Number of Markets Operated In",
  "Footprint Data Status",
  "Footprint Data Source",
  "Footprint Figures As Of",
];

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const footprintIds = [
  ...new Set(discovery.rows.flatMap((r) => r.footprintLinkIds || [])),
];
const formula = `OR(${footprintIds.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
const recs = await base("Brand Setup - Brand Footprint")
  .select({ filterByFormula: formula, fields: FP_FIELDS })
  .all();
const byId = new Map(recs.map((r) => [r.id, r]));

function presence(n) {
  if (n == null) return "UNKNOWN";
  if (n > 0) return "PRESENT";
  return "ABSENT";
}

const rows = discovery.rows.map((b) => {
  const fpId = (b.footprintLinkIds || [])[0] || null;
  const r = fpId ? byId.get(fpId) : null;
  const f = r?.fields || {};
  const cala = num(f["CALA Existing Hotel"]);
  const am = num(f["AM Existing Hotel"]);
  const eu = num(f["EU Existing Hotel"]);
  const totalNb = num(f["Total New Build Hotel"]);
  const totalConv = num(f["Total Conversion Hotel"]);
  const calaNb = num(f["CALA New Build Hotel"]);
  const cities = f["Specific Markets/Cities"];
  const cityStr = cities == null ? null : String(cities);
  const mexicoHint =
    cityStr && /mexico|canc[uú]n|mexico city|cdmx|guadalajara|monterrey|los cabos|tulum/i.test(cityStr)
      ? true
      : cityStr
        ? false
        : null;

  return {
    brandId: b.brandId,
    brandName: b.brandName,
    footprintId: fpId,
    footprintFound: Boolean(r),
    footprintDataStatus: f["Footprint Data Status"] || null,
    footprintDataSource: f["Footprint Data Source"] || null,
    operatingPresence: {
      NORTH_AMERICA: presence(am),
      CALA: presence(cala),
      EUROPE: presence(eu),
      GLOBAL: presence(num(f["Total Existing Hotels"])),
      counts: {
        am,
        cala,
        eu,
        total: num(f["Total Existing Hotels"]),
        calaPipeline: num(f["CALA Pipeline Hotel"]),
      },
    },
    newBuildFootprint: {
      totalNewBuildHotel: totalNb,
      totalConversionHotel: totalConv,
      calaNewBuildHotel: calaNb,
      newBuildExperiencePct: f["New Build Experience (New build %)"] ?? null,
      conversionExperiencePct: f["Conversion Experience (Conversion %)"] ?? null,
    },
    mexicoCityHint: mexicoHint,
    specificMarketsCities: cityStr ? cityStr.slice(0, 240) : null,
  };
});

const out = {
  generatedAt: new Date().toISOString(),
  AIRTABLE_WRITES: 0,
  LIVE_PROVIDER_CALLS: 0,
  NOTE:
    "Operating presence from Brand Footprint Existing Hotel counts. Not development eligibility. Mexico country not a footprint column — city-string hint only.",
  rows,
};

const outPath = path.join(
  __dirname,
  "..",
  "data/ai-visibility/phase3a8-footprint-operating-presence.json"
);
fs.writeFileSync(outPath, JSON.stringify(out, null, 2), "utf8");
console.log(JSON.stringify(out, null, 2));
