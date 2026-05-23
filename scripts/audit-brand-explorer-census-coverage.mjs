/**
 * QA: compare MVP Brand Footprint totals vs Platform census summary per active brand.
 * Read-only. Writes reports/brand-explorer-census-coverage.csv
 *
 * Usage: node scripts/audit-brand-explorer-census-coverage.mjs
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { buildBrandCensusSummary } from "../lib/hotel-census/build-brand-census-summary.js";
import { resetGovernanceFieldCache } from "../lib/hotel-census/census-governance.js";
import {
  FOOTPRINT_VERIFICATION_AIRTABLE,
  readFootprintVerificationFromFields,
} from "../lib/brand-footprint-verification.js";
import {
  brandFootprintTrustInput,
  displaySourceRecommendation,
} from "../lib/brand-explorer-footprint-trust.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_PATH = join(__dirname, "..", "reports", "brand-explorer-census-coverage.csv");

const BASICS_TABLE = "Brand Setup - Brand Basics";
const FOOTPRINT_TABLE = "Brand Setup - Brand Footprint";
const BRAND_NAME_FIELD = "Brand Name";
const PARENT_FIELD = "Parent Company";
const STATUS_FIELD = "Brand Status";

const STANDARD_REGIONS = ["AM", "CALA", "EU", "MEA", "APAC"];

function parseNum(v) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : 0;
}

function mvpExistingHotelsFromFootprint(fields) {
  if (!fields) return null;
  let sum = 0;
  let any = false;
  for (const region of STANDARD_REGIONS) {
    const candidates = [
      `${region} - Existing Hotel`,
      `${region} - Existing Hotels`,
      `${region} Total Distribution Hotel`,
    ];
    for (const col of candidates) {
      const v = fields[col];
      if (v != null && v !== "") {
        sum += parseNum(v);
        any = true;
        break;
      }
    }
  }
  return any ? sum : null;
}

function csvEscape(val) {
  const s = val == null ? "" : String(val);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function loadFootprintByBrandName(mvpBase, brandName) {
  const esc = (brandName || "").replace(/"/g, '\\"');
  const recs = await mvpBase(FOOTPRINT_TABLE)
    .select({
      filterByFormula: `{${BRAND_NAME_FIELD}} = "${esc}"`,
      maxRecords: 1,
    })
    .all();
  return recs[0]?.fields || null;
}

function footprintForTrust(fpFields) {
  const hotels = mvpExistingHotelsFromFootprint(fpFields);
  const verification = readFootprintVerificationFromFields(fpFields);
  return {
    totalExistingHotels: hotels,
    regionalDistribution: {},
    verification: verification || undefined,
  };
}

async function main() {
  const mvpKey = process.env.AIRTABLE_API_KEY;
  const mvpBaseId = process.env.AIRTABLE_BASE_ID;
  if (!mvpKey || !mvpBaseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  }
  if (!process.env.AIRTABLE_BASE_ID_ALT) {
    throw new Error("Set AIRTABLE_BASE_ID_ALT");
  }

  resetGovernanceFieldCache();

  const mvpBase = new Airtable({ apiKey: mvpKey }).base(mvpBaseId);
  const brands = await mvpBase(BASICS_TABLE)
    .select({
      filterByFormula: "FIND('Active', {Brand Status}) > 0",
      fields: [BRAND_NAME_FIELD, PARENT_FIELD, STATUS_FIELD],
    })
    .all();

  const rows = [];
  const headers = [
    "brandName",
    "parentCompany",
    "mvpExistingHotels",
    "censusOpenHotels",
    "difference",
    "censusOpenKeys",
    "countryCount",
    "aliasUsed",
    "fallbackRecommended",
    "Footprint Data Status",
    "Footprint Data Source",
    "Footprint Figures As Of",
    "Display Source Recommendation",
    "warnings",
  ];

  console.log(`Auditing ${brands.length} active brands...\n`);

  for (const rec of brands) {
    const f = rec.fields || {};
    const brandName = (f[BRAND_NAME_FIELD] || "").toString().trim();
    if (!brandName) continue;

    const parentCompany = (f[PARENT_FIELD] || "").toString().trim();
    const fpFields = await loadFootprintByBrandName(mvpBase, brandName);
    const mvpHotels = mvpExistingHotelsFromFootprint(fpFields);
    const verification = readFootprintVerificationFromFields(fpFields);

    const summary = await buildBrandCensusSummary(brandName, parentCompany || null);
    const censusHotels =
      summary.available && summary.metrics ? summary.metrics.totalOpenHotels : null;
    const censusKeys =
      summary.available && summary.metrics ? summary.metrics.totalOpenKeys : null;
    const countryCount =
      summary.available && summary.metrics ? summary.metrics.countryCount : null;

    const diff =
      mvpHotels != null && censusHotels != null ? censusHotels - mvpHotels : "";

    const brandForTrust = brandFootprintTrustInput({
      name: brandName,
      parentCompany,
      footprint: footprintForTrust(fpFields),
      censusSummary: summary,
    });
    const displayRec = displaySourceRecommendation(brandForTrust);

    const line = {
      brandName,
      parentCompany,
      mvpExistingHotels: mvpHotels ?? "",
      censusOpenHotels: censusHotels ?? "",
      difference: diff,
      censusOpenKeys: censusKeys ?? "",
      countryCount: countryCount ?? "",
      aliasUsed: summary.alias?.usedAliasTable ? "yes" : "no",
      fallbackRecommended: summary.fallbackRecommended ? "yes" : "no",
      "Footprint Data Status":
        verification?.status ??
        (fpFields?.[FOOTPRINT_VERIFICATION_AIRTABLE.status] != null
          ? String(fpFields[FOOTPRINT_VERIFICATION_AIRTABLE.status]).trim()
          : ""),
      "Footprint Data Source":
        verification?.source ??
        (fpFields?.[FOOTPRINT_VERIFICATION_AIRTABLE.source] != null
          ? String(fpFields[FOOTPRINT_VERIFICATION_AIRTABLE.source]).trim()
          : ""),
      "Footprint Figures As Of":
        verification?.figuresAsOf ??
        (fpFields?.[FOOTPRINT_VERIFICATION_AIRTABLE.figuresAsOf] != null
          ? String(fpFields[FOOTPRINT_VERIFICATION_AIRTABLE.figuresAsOf]).trim()
          : ""),
      "Display Source Recommendation": displayRec,
      warnings: (summary.warnings || []).join("; "),
    };
    rows.push(line);

    const diffNum = typeof diff === "number" ? diff : null;
    const flag = diffNum != null && Math.abs(diffNum) >= 10 ? " ***" : "";
    console.log(
      `${brandName} | MVP ${mvpHotels ?? "—"} | Census ${censusHotels ?? "—"} | Δ ${diff}${flag} | display ${displayRec} | fallback ${line.fallbackRecommended}`
    );
  }

  mkdirSync(dirname(REPORT_PATH), { recursive: true });
  const csv = [
    headers.join(","),
    ...rows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")),
  ].join("\n");
  writeFileSync(REPORT_PATH, csv + "\n", "utf8");

  const major = rows.filter((r) => {
    const d = Number(r.difference);
    return (
      Number.isFinite(d) &&
      Math.abs(d) >= 10 &&
      r.fallbackRecommended === "no" &&
      r["Display Source Recommendation"] === "Census"
    );
  });

  console.log(`\nWrote ${REPORT_PATH}`);
  console.log(`Brands with |difference| >= 10 (census display, no fallback): ${major.length}`);
  major.slice(0, 25).forEach((r) => {
    console.log(
      `  ${r.brandName}: MVP ${r.mvpExistingHotels} vs Census ${r.censusOpenHotels} (Δ ${r.difference})`
    );
  });
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
