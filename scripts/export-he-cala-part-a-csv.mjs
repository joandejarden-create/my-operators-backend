/**
 * Export Hotel Equities (CALA) form inventory as Perplexity "Part A" CSV.
 *
 *   node scripts/export-he-cala-part-a-csv.mjs
 *   node scripts/export-he-cala-part-a-csv.mjs ./scripts/he-cala-part-a.csv
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const INVENTORY = path.join(ROOT, "scripts", "he-cala-form-inventory.json");
const DEFAULT_OUT = path.join(ROOT, "scripts", "he-cala-part-a.csv");

const SOURCE_URL = "https://www.hotelequities.com/cala.htm";
const SOURCE_DATE = "2026-05-27";

const MULTI_SELECT = new Set([
  "chainScalesSupported",
  "propertyTypes",
  "additionalExperience",
  "activeCountries",
  "activeMarkets",
  "regions",
  "serviceModelsSupported",
  "managementStructuresSupported",
  "brandFamiliesOperated",
  "offeredServices",
  "brands",
]);

const SINGLE_SELECT_SUFFIX = new Set([
  "primaryServiceModel",
  "marketPresenceType",
  "companySize",
  "emergencyResponse",
  "businessContinuity",
  "support24x7",
  "sustainabilityPrograms",
  "esgReporting",
  "carbonTracking",
  "ownerReportingLevel",
  "governanceCadence",
  "preferredContactMethod",
  "fbCapabilityLevel",
  "revenueManagementCapability",
  "salesPlatform",
  "newBuildOpeningExperience",
  "conversionReflagExperience",
  "preOpeningSupportCapability",
  "dataConfidenceLevel",
  "sourceType",
]);

const NUMBER_FIELDS = new Set([
  "yearEstablished",
  "yearsInBusiness",
  "numberOfBrands",
  "numberOfMarkets",
  "totalProperties",
  "totalRooms",
  "minimumKeyCount",
  "geo_cala_total_hotels",
  "geo_na_total_hotels",
  "geo_total_total_hotels",
]);

function inferValueType(fieldName) {
  if (fieldName === "website" || fieldName.endsWith("Url") || fieldName.endsWith("_url")) return "url";
  if (fieldName.includes("Date") || fieldName === "figuresAsOf" || fieldName === "portfolioMetricsAsOf")
    return "date";
  if (MULTI_SELECT.has(fieldName)) return "multi_select";
  if (SINGLE_SELECT_SUFFIX.has(fieldName) || fieldName.startsWith("cap_signal_") || fieldName.startsWith("cap_kpi_") || fieldName.startsWith("brand_signal_") || fieldName.startsWith("infra_kpi_") || fieldName.startsWith("tr_signal_"))
    return "single_select";
  if (NUMBER_FIELDS.has(fieldName)) return "number";
  return "text";
}

function inferConfidence(verdict, isEmpty, value) {
  if (!value || value === "(empty)" || /^Unknown$/i.test(value)) return "Unknown";
  if (verdict === "Keep") return "Verified";
  if (verdict === "Update") return "High";
  if (verdict === "Change") return "High";
  if (verdict === "Review") return "Inferred";
  return isEmpty ? "Unknown" : "Medium";
}

function csvEscape(cell) {
  const s = cell == null ? "" : String(cell);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function rowToCsv(cols) {
  return cols.map(csvEscape).join(",");
}

const inventory = JSON.parse(fs.readFileSync(INVENTORY, "utf8"));
const outPath = process.argv[2] ? path.resolve(process.cwd(), process.argv[2]) : DEFAULT_OUT;

const header = [
  "field_id",
  "airtable_field_name",
  "value",
  "value_type",
  "allowed_options_if_any",
  "confidence",
  "source_url",
  "source_date",
  "notes",
];

const lines = [rowToCsv(header)];

for (const row of inventory) {
  const fieldId = row.fieldName;
  const value =
    row.suggestedCopyPaste && String(row.suggestedCopyPaste).trim()
      ? String(row.suggestedCopyPaste).trim()
      : row.isEmpty
        ? "Unknown"
        : String(row.rawValue || row.current || "").trim();

  const notes = [
    `tab=${row.tab}`,
    `verdict=${row.verdict}`,
    row.isEmpty ? "was_empty" : "had_value",
  ].join("; ");

  lines.push(
    rowToCsv([
      fieldId,
      fieldId,
      value,
      inferValueType(fieldId),
      "",
      inferConfidence(row.verdict, row.isEmpty, value),
      SOURCE_URL,
      SOURCE_DATE,
      notes,
    ])
  );
}

fs.writeFileSync(outPath, lines.join("\n") + "\n", "utf8");
console.log(`Wrote ${inventory.length} data rows to ${outPath}`);
