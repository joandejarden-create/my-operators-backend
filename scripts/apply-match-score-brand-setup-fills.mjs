#!/usr/bin/env node
/**
 * Apply founder-filled Match Score Brand Setup worksheet (A/B sources only).
 *
 * Default: --dry-run (no Airtable writes).
 * Live write: --apply after founder approval.
 *
 * Usage:
 *   node scripts/apply-match-score-brand-setup-fills.mjs --dry-run
 *   node scripts/apply-match-score-brand-setup-fills.mjs --worksheet reports/match-score-brand-setup-founder-worksheet.json --dry-run
 *   node scripts/apply-match-score-brand-setup-fills.mjs --worksheet … --apply
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const BASICS = "Brand Setup - Brand Basics";
const PROJECT_FIT = "Brand Setup - Project Fit";
const STANDARDS = "Brand Setup - Brand Standards";
const FEE = "Brand Setup - Fee Structure";
const OP = "Brand Setup - Operational Support";

/** Map worksheet fieldKey → { table, airtableField, valueKind } */
const FIELD_WRITE_MAP = Object.freeze({
  hotelChainScale: { table: BASICS, airtableField: "Hotel Chain Scale", valueKind: "string" },
  hotelServiceModel: { table: BASICS, airtableField: "Hotel Service Model", valueKind: "string" },
  marketsToAvoid: { table: BASICS, airtableField: "Markets to Avoid or Saturated", valueKind: "multiSelectOrString" },
  priorityMarkets: { table: PROJECT_FIT, airtableField: "Priority Markets", valueKind: "multiSelectOrString" },
  softCollectionBrand: { table: PROJECT_FIT, airtableField: "Soft/Collection Brand", valueKind: "string" },
  acceptableProjectType: { table: PROJECT_FIT, airtableField: "Acceptable Project Type", valueKind: "multiSelectOrString" },
  acceptableBuildingTypes: { table: PROJECT_FIT, airtableField: "Acceptable Building Types", valueKind: "multiSelectOrString" },
  acceptableProjectStages: { table: PROJECT_FIT, airtableField: "Acceptable Project Stages", valueKind: "multiSelectOrString" },
  roomCountRange: { table: PROJECT_FIT, airtableField: null, valueKind: "roomRangeJson" },
  acceptableAgreementsType: { table: PROJECT_FIT, airtableField: "Acceptable Agreements Type", valueKind: "multiSelectOrString" },
  additionalAmenities: { table: STANDARDS, airtableField: "Additional Amenities", valueKind: "multiSelectOrString" },
  fbParkingStandards: { table: STANDARDS, airtableField: null, valueKind: "fbParkingJson" },
  feeRoyaltyRange: { table: FEE, airtableField: null, valueKind: "royaltyJson" },
  feeMarketingOrLoyalty: { table: FEE, airtableField: null, valueKind: "mktLoyaltyJson" },
  incentiveTypes: { table: OP, airtableField: "Incentive Types", valueKind: "multiSelectOrString" },
  willingToNegotiateIncentives: { table: OP, airtableField: "Willing to Negotiate Incentives", valueKind: "string" },
});

function parseArgs(argv) {
  const apply = argv.includes("--apply");
  const dryRun = !apply || argv.includes("--dry-run");
  let worksheet = path.join(ROOT, "reports", "match-score-brand-setup-founder-worksheet.json");
  const wi = argv.indexOf("--worksheet");
  if (wi >= 0 && argv[wi + 1]) worksheet = path.resolve(process.cwd(), argv[wi + 1]);
  return { apply: apply && !argv.includes("--dry-run"), dryRun: !apply || argv.includes("--dry-run"), worksheet };
}

async function atFetch(baseId, apiKey, urlPath, { method = "GET", body } = {}) {
  const url = `https://api.airtable.com/v0/${baseId}/${urlPath}`;
  const headers = { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" };
  const res = await fetch(url, {
    method,
    headers,
    body: body != null ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) {
    const msg = data.error?.message || data.error?.type || res.statusText || String(res.status);
    throw new Error(msg);
  }
  return data;
}

function parseProposedValue(kind, raw) {
  const s = String(raw ?? "").trim();
  if (!s) return null;
  if (kind === "string") return s;
  if (kind === "multiSelectOrString") {
    if (s.startsWith("[")) {
      try {
        const arr = JSON.parse(s);
        return Array.isArray(arr) ? arr.map((x) => String(x).trim()).filter(Boolean) : [s];
      } catch {
        /* fall through */
      }
    }
    return s.split(/\s*\|\s*/).map((x) => x.trim()).filter(Boolean);
  }
  if (kind === "roomRangeJson" || kind === "fbParkingJson" || kind === "royaltyJson" || kind === "mktLoyaltyJson") {
    try {
      return JSON.parse(s);
    } catch {
      throw new Error(`proposedValue for ${kind} must be JSON object, got: ${s.slice(0, 80)}`);
    }
  }
  return s;
}

function fieldsFromParsed(fieldKey, kind, parsed) {
  if (kind === "roomRangeJson") {
    const min = parsed.min ?? parsed["Min - Room Count"];
    const max = parsed.max ?? parsed["Max - Room Count"];
    if (min == null || max == null) throw new Error("roomCountRange needs {min,max}");
    return { "Min - Room Count": Number(min), "Max - Room Count": Number(max) };
  }
  if (kind === "fbParkingJson") {
    const out = {};
    if (parsed.fb != null) out["F&B Outlets Required"] = String(parsed.fb);
    if (parsed.parking != null) out["Parking Required"] = String(parsed.parking);
    if (!Object.keys(out).length) throw new Error("fbParkingStandards needs {fb?,parking?}");
    return out;
  }
  if (kind === "royaltyJson") {
    const out = {};
    if (parsed.min != null) out["Min - Typical Royalty Fee Range"] = Number(parsed.min);
    if (parsed.max != null) out["Max - Typical Royalty Fee Range"] = Number(parsed.max);
    if (parsed.basis != null) out["Basis - Typical Royalty Fee Range"] = String(parsed.basis);
    if (!Object.keys(out).length) throw new Error("feeRoyaltyRange needs {min?,max?,basis?}");
    return out;
  }
  if (kind === "mktLoyaltyJson") {
    const out = {};
    if (parsed.marketingMin != null) out["Min - Typical Marketing Fee Range"] = Number(parsed.marketingMin);
    if (parsed.marketingMax != null) out["Max - Typical Marketing Fee Range"] = Number(parsed.marketingMax);
    if (parsed.loyaltyMin != null) out["Min - Typical Loyalty Program Fee"] = Number(parsed.loyaltyMin);
    if (parsed.loyaltyMax != null) out["Max - Typical Loyalty Program Fee"] = Number(parsed.loyaltyMax);
    if (!Object.keys(out).length) throw new Error("feeMarketingOrLoyalty needs marketing/loyalty min/max");
    return out;
  }
  const map = FIELD_WRITE_MAP[fieldKey];
  if (!map?.airtableField) throw new Error(`No airtableField for ${fieldKey}`);
  return { [map.airtableField]: parsed };
}

function getLinkedId(basicsFields, tableName) {
  const names = [tableName, tableName.replace("Brand Setup - ", "")];
  for (const name of names) {
    const link = basicsFields[name];
    if (Array.isArray(link) && link[0] && String(link[0]).startsWith("rec")) return String(link[0]);
    if (typeof link === "string" && link.startsWith("rec")) return link;
  }
  return null;
}

async function loadBasics(baseId, apiKey, recordId) {
  return atFetch(baseId, apiKey, `${encodeURIComponent(BASICS)}/${encodeURIComponent(recordId)}`);
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  if (!fs.existsSync(opts.worksheet)) {
    throw new Error(`Worksheet not found: ${opts.worksheet}. Run npm run audit-match-score-brand-setup-gaps first.`);
  }
  const worksheet = JSON.parse(fs.readFileSync(opts.worksheet, "utf8"));
  const rows = (worksheet.rows || []).filter((r) => String(r.proposedValue || "").trim() !== "");

  const fillLog = {
    generatedAt: new Date().toISOString(),
    dryRun: opts.dryRun,
    worksheetPath: opts.worksheet,
    fillPolicy: "A_founder_or_B_existing_docs_only",
    candidateRows: rows.length,
    skippedEmptyProposed: (worksheet.rows || []).length - rows.length,
    patches: [],
    errors: [],
  };

  if (rows.length === 0) {
    console.log("No rows with proposedValue — nothing to apply. Founder must fill worksheet from A/B sources.");
    const out = path.join(ROOT, "reports", "match-score-brand-setup-fills-dry-run.json");
    fs.writeFileSync(out, JSON.stringify(fillLog, null, 2), "utf8");
    console.log(`Wrote ${out}`);
    process.exit(0);
  }

  for (const row of rows) {
    const sourceType = String(row.sourceType || "").trim().toUpperCase();
    if (sourceType !== "A" && sourceType !== "B") {
      fillLog.errors.push({
        brand: row.brandName,
        fieldKey: row.fieldKey,
        error: "sourceType must be A or B before apply",
      });
      continue;
    }
    const map = FIELD_WRITE_MAP[row.fieldKey];
    if (!map) {
      fillLog.errors.push({ brand: row.brandName, fieldKey: row.fieldKey, error: "Unknown fieldKey" });
      continue;
    }
    try {
      const parsed = parseProposedValue(map.valueKind, row.proposedValue);
      const fields = fieldsFromParsed(row.fieldKey, map.valueKind, parsed);
      const basics = await loadBasics(baseId, apiKey, row.brandRecordId);
      const basicsFields = basics.fields || {};
      let targetRecordId = row.brandRecordId;
      let targetTable = map.table;
      if (map.table !== BASICS) {
        const linked = getLinkedId(basicsFields, map.table);
        if (!linked) {
          fillLog.errors.push({
            brand: row.brandName,
            fieldKey: row.fieldKey,
            error: `No linked ${map.table} record on Brand Basics — create/link in Airtable first`,
          });
          continue;
        }
        targetRecordId = linked;
      }
      const patch = {
        brandName: row.brandName,
        fieldKey: row.fieldKey,
        sourceType,
        sourceRef: row.sourceRef || "",
        table: targetTable,
        recordId: targetRecordId,
        fields,
      };
      fillLog.patches.push(patch);
      if (!opts.dryRun) {
        await atFetch(baseId, apiKey, `${encodeURIComponent(targetTable)}/${encodeURIComponent(targetRecordId)}`, {
          method: "PATCH",
          body: { fields },
        });
        patch.applied = true;
      }
    } catch (err) {
      fillLog.errors.push({
        brand: row.brandName,
        fieldKey: row.fieldKey,
        error: err?.message || String(err),
      });
    }
  }

  const outName = opts.dryRun ? "match-score-brand-setup-fills-dry-run.json" : "match-score-brand-setup-fills-apply.json";
  const out = path.join(ROOT, "reports", outName);
  fs.writeFileSync(out, JSON.stringify(fillLog, null, 2), "utf8");
  console.log(`mode=${opts.dryRun ? "dry-run" : "APPLY"} patches=${fillLog.patches.length} errors=${fillLog.errors.length}`);
  console.log(`Wrote ${out}`);
  if (fillLog.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
