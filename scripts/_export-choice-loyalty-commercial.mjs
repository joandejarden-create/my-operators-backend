/**
 * Export Loyalty & Commercial linked rows for Choice CHI brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "fixtures", "choice-loyalty-commercial-export.json");

const FIELDS = [
  "Brand Name",
  "Typical Loyalty Program Name",
  "Typical % of Rooms from Loyalty (est.)",
  "Typical Direct Booking % (est.)",
  "Typical OTA Reliance % (est.)",
  "Total Global Members (Approx. Millions)",
  "Regional Members - NA (Millions)",
  "Regional Members - CALA (Millions)",
  "Regional Members - EU (Millions)",
  "Regional Members - MEA (Millions)",
  "Regional Members - APAC (Millions)",
  "Loyalty Program Cost per Stay (Approximate)",
  "OTA Commission (Typical % of Reservation)",
  "CRS Usage (% of bookings flowing through)",
  "Distribution Cost (Per Reservation)",
  "Website/App Conv. Rates (%)",
  "Avg. Cost of Cust. Acquisition",
];

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const basics = await base("Brand Setup - Brand Basics").select({ maxRecords: 500 }).all();
const choice = basics.filter((r) =>
  String(r.get("Parent Company") || "").includes("Choice Hotels International")
);
const lcRows = await base("Brand Setup - Loyalty & Commercial").select({ maxRecords: 500 }).all();
const byName = new Map();
for (const r of lcRows) {
  const n = r.get("Brand Name");
  if (n) byName.set(String(n).trim(), r);
}

const out = choice
  .sort((a, b) => String(a.get("Brand Name")).localeCompare(String(b.get("Brand Name"))))
  .map((b) => {
    const name = b.get("Brand Name");
    const lc = byName.get(String(name).trim());
    const o = { basicsId: b.id, brandName: name, lcRecordId: lc?.id || null };
    if (lc) {
      for (const f of FIELDS) {
        let v = lc.get(f);
        if (typeof v === "number" && f.includes("%") && v >= 0 && v <= 1) v = Math.round(v * 10000) / 100;
        o[f] = v ?? null;
      }
    }
    return o;
  });

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(out, null, 2), "utf8");
console.log(`Exported ${out.length} brands → ${OUT}`);
