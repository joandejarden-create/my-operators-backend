/**
 * Export curated GTM owner strike list from classification report or live Airtable.
 *
 * Usage:
 *   node scripts/export-gtm-owner-strike-list.mjs
 *   node scripts/export-gtm-owner-strike-list.mjs --from-airtable
 *   node scripts/export-gtm-owner-strike-list.mjs --include-needs-contact
 *
 * Reports:
 *   reports/gtm-owner-strike-list.json
 *   reports/gtm-owner-strike-list.csv
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
  VAL_GTM_ICP_STRIKE_ELIGIBLE,
} from "../lib/gtm-owner-target/field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLASSIFICATION_JSON = join(__dirname, "..", "reports", "gtm-owner-target-icp-classification.json");
const REPORT_JSON = join(__dirname, "..", "reports", "gtm-owner-strike-list.json");
const REPORT_CSV = join(__dirname, "..", "reports", "gtm-owner-strike-list.csv");

const FROM_AIRTABLE = process.argv.includes("--from-airtable");
const INCLUDE_NEEDS_CONTACT = process.argv.includes("--include-needs-contact");

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function tierRank(tier) {
  if (tier === "A") return 1;
  if (tier === "B") return 2;
  return 3;
}

function sortStrikeRows(rows) {
  return rows.slice().sort(
    (a, b) =>
      tierRank(a.priorityTier) - tierRank(b.priorityTier) ||
      (b.calaPropertyCount || 0) - (a.calaPropertyCount || 0) ||
      String(a.ownerName || "").localeCompare(String(b.ownerName || ""))
  );
}

function mapRow(row) {
  return {
    id: row.id,
    ownerName: row.ownerName,
    priorityTier: row.priorityTier,
    icpSegment: row.icpSegment,
    calaPropertyCount: row.calaPropertyCount,
    propertyCount: row.propertyCount,
    countriesSummary: row.countriesSummary,
    primaryContactName: row.primaryContactName,
    primaryContactEmail: row.primaryContactEmail,
    hasVerifiedContact: row.hasVerifiedContact,
    dealTrigger: row.dealTrigger,
    pitchAngle: row.pitchAngle || "",
    outreachStatus: row.outreachStatus || "",
    nextAction: row.nextAction || "",
    icpClassificationNotes: row.icpClassificationNotes || "",
    strikeList: row.strikeList,
  };
}

async function loadFromClassificationReport() {
  if (!existsSync(CLASSIFICATION_JSON)) {
    throw new Error(
      `Missing ${CLASSIFICATION_JSON}. Run: node scripts/classify-gtm-owner-target-icp.mjs`
    );
  }
  const data = JSON.parse(readFileSync(CLASSIFICATION_JSON, "utf8"));
  const strikeRows = (data.rows || []).filter((r) => r.strikeList);
  const nearMiss = (data.rows || []).filter(
    (r) =>
      !r.strikeList &&
      VAL_GTM_ICP_STRIKE_ELIGIBLE.includes(r.icpSegment) &&
      (r.priorityTier === "A" || r.priorityTier === "B") &&
      (r.calaPropertyCount || 0) >= 3
  );
  return {
    source: "classification_report",
    generatedAt: data.generatedAt,
    strikeRows: sortStrikeRows(strikeRows.map(mapRow)),
    nearMissRows: sortStrikeRows(nearMiss.map(mapRow)),
  };
}

async function loadFromAirtable() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const fields = [
    MAP_GTM_OWNER_TARGET.ownerName,
    MAP_GTM_OWNER_TARGET.priorityTier,
    MAP_GTM_OWNER_TARGET.icpSegment,
    MAP_GTM_OWNER_TARGET.strikeList,
    MAP_GTM_OWNER_TARGET.calaPropertyCount,
    MAP_GTM_OWNER_TARGET.propertyCount,
    MAP_GTM_OWNER_TARGET.countriesSummary,
    MAP_GTM_OWNER_TARGET.primaryContactName,
    MAP_GTM_OWNER_TARGET.primaryContactEmail,
    MAP_GTM_OWNER_TARGET.dealTrigger,
    MAP_GTM_OWNER_TARGET.pitchAngle,
    MAP_GTM_OWNER_TARGET.outreachStatus,
    MAP_GTM_OWNER_TARGET.nextAction,
    MAP_GTM_OWNER_TARGET.icpClassificationNotes,
  ];

  const records = await base(GTM_OWNER_TARGET_TABLES.ownerTargets).select({ fields }).all();
  const allRows = records.map((rec) =>
    mapRow({
      id: rec.id,
      ownerName: rec.fields[MAP_GTM_OWNER_TARGET.ownerName],
      priorityTier: rec.fields[MAP_GTM_OWNER_TARGET.priorityTier],
      icpSegment: rec.fields[MAP_GTM_OWNER_TARGET.icpSegment],
      strikeList: Boolean(rec.fields[MAP_GTM_OWNER_TARGET.strikeList]),
      calaPropertyCount: rec.fields[MAP_GTM_OWNER_TARGET.calaPropertyCount],
      propertyCount: rec.fields[MAP_GTM_OWNER_TARGET.propertyCount],
      countriesSummary: rec.fields[MAP_GTM_OWNER_TARGET.countriesSummary],
      primaryContactName: rec.fields[MAP_GTM_OWNER_TARGET.primaryContactName],
      primaryContactEmail: rec.fields[MAP_GTM_OWNER_TARGET.primaryContactEmail],
      dealTrigger: rec.fields[MAP_GTM_OWNER_TARGET.dealTrigger],
      pitchAngle: rec.fields[MAP_GTM_OWNER_TARGET.pitchAngle],
      outreachStatus: rec.fields[MAP_GTM_OWNER_TARGET.outreachStatus],
      nextAction: rec.fields[MAP_GTM_OWNER_TARGET.nextAction],
      icpClassificationNotes: rec.fields[MAP_GTM_OWNER_TARGET.icpClassificationNotes],
      hasVerifiedContact: false,
    })
  );

  const strikeRows = sortStrikeRows(allRows.filter((r) => r.strikeList));
  const nearMiss = sortStrikeRows(
    allRows.filter(
      (r) =>
        !r.strikeList &&
        VAL_GTM_ICP_STRIKE_ELIGIBLE.includes(r.icpSegment) &&
        (r.priorityTier === "A" || r.priorityTier === "B") &&
        (r.calaPropertyCount || 0) >= 3
    )
  );

  return {
    source: "airtable",
    generatedAt: new Date().toISOString(),
    baseId,
    strikeRows,
    nearMissRows: nearMiss,
  };
}

async function main() {
  const payload = FROM_AIRTABLE ? await loadFromAirtable() : await loadFromClassificationReport();

  const exportRows = INCLUDE_NEEDS_CONTACT
    ? sortStrikeRows([...payload.strikeRows, ...payload.nearMissRows])
    : payload.strikeRows;

  const report = {
    generatedAt: new Date().toISOString(),
    source: payload.source,
    baseId: payload.baseId || null,
    classificationGeneratedAt: payload.generatedAt || null,
    includeNeedsContact: INCLUDE_NEEDS_CONTACT,
    summary: {
      strikeListCount: payload.strikeRows.length,
      nearMissCount: payload.nearMissRows.length,
      exportedCount: exportRows.length,
    },
    strikeList: payload.strikeRows,
    nearMiss: payload.nearMissRows,
    exported: exportRows,
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  const csvHeaders = [
    "ownerName",
    "priorityTier",
    "icpSegment",
    "calaPropertyCount",
    "propertyCount",
    "countriesSummary",
    "primaryContactName",
    "primaryContactEmail",
    "dealTrigger",
    "outreachStatus",
    "nextAction",
    "icpClassificationNotes",
  ];
  const csvLines = [
    csvHeaders.join(","),
    ...exportRows.map((r) => csvHeaders.map((h) => csvEscape(r[h])).join(",")),
  ];
  writeFileSync(REPORT_CSV, csvLines.join("\n"));

  console.log(`Strike list: ${payload.strikeRows.length} qualified`);
  console.log(`Near-miss (needs contact): ${payload.nearMissRows.length}`);
  console.log(`Exported: ${exportRows.length} rows`);
  console.log("Wrote", REPORT_JSON);
  console.log("Wrote", REPORT_CSV);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
