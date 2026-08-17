/**
 * Populate Brand Standards F&B / Meeting requirement fields for active brands.
 *
 *   node scripts/populate-active-brand-standards-fb-meeting.mjs --dry-run
 *   node scripts/populate-active-brand-standards-fb-meeting.mjs --apply
 *   node scripts/populate-active-brand-standards-fb-meeting.mjs --apply --fill-blanks-only
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import {
  buildBrandStandardsFbMeetingProfile,
  brandStandardsFbMeetingProfileToAirtableFields,
} from "../lib/brand-explorer/active-brand-standards-fb-meeting-profiles.js";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const STANDARDS_TABLE = "Brand Setup - Brand Standards";
const REPORT_PATH = path.join("reports", "active-brand-standards-fb-meeting-population.json");

const fillBlanksOnly = process.argv.includes("--fill-blanks-only");
const apply = process.argv.includes("--apply");
const dryRun = !apply;

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const basicsRows = await base(BASICS_TABLE)
  .select({
    filterByFormula: "OR({Brand Status}='Active', {Brand Status}='Live')",
    fields: [
      "Brand Name",
      "Parent Company",
      "Hotel Chain Scale",
      "Brand Model",
      "Hotel Service Model",
      "Brand Architecture",
      "Brand Setup - Brand Standards",
    ],
    sort: [{ field: "Brand Name", direction: "asc" }],
  })
  .all();

const stdRows = await base(STANDARDS_TABLE)
  .select({
    fields: [
      "Brand Name",
      "Brand",
      "F&B Outlets Required",
      "Meeting Space Required",
      "Typical Number of F&B Outlets",
      "Typical Number of Meeting Rooms",
    ],
  })
  .all();

const stdByBrandId = new Map();
const stdByName = new Map();
for (const row of stdRows) {
  stdByName.set(String(row.get("Brand Name") || "").trim(), row);
  for (const id of row.get("Brand") || []) stdByBrandId.set(id, row);
}

const planned = [];
const skipped = [];
const missingStandards = [];

for (const basics of basicsRows) {
  const name = basics.get("Brand Name");
  const std =
    stdByBrandId.get(basics.id) ||
    stdByName.get(String(name || "").trim()) ||
    null;
  if (!std) {
    missingStandards.push({ basicsId: basics.id, name });
    continue;
  }

  const profile = buildBrandStandardsFbMeetingProfile({
    name,
    parentCompany: basics.get("Parent Company"),
    chainScale: basics.get("Hotel Chain Scale"),
    brandModel: basics.get("Brand Model"),
    serviceModel: basics.get("Hotel Service Model"),
    architecture: basics.get("Brand Architecture"),
  });

  const targetFields = brandStandardsFbMeetingProfileToAirtableFields(profile);
  const current = {
    "F&B Outlets Required": std.get("F&B Outlets Required") || null,
    "Meeting Space Required": std.get("Meeting Space Required") || null,
    "Typical Number of F&B Outlets": std.get("Typical Number of F&B Outlets") ?? null,
    "Typical Number of Meeting Rooms": std.get("Typical Number of Meeting Rooms") ?? null,
  };

  const blankFb = !current["F&B Outlets Required"];
  const blankMeeting = !current["Meeting Space Required"];
  const hasBlank = blankFb || blankMeeting;

  const patch = {};
  for (const [field, value] of Object.entries(targetFields)) {
    const cur = current[field];
    const isScalarBlank = cur === null || cur === "" || cur === undefined;
    if (fillBlanksOnly) {
      if ((field === "F&B Outlets Required" && blankFb) || (field === "Meeting Space Required" && blankMeeting)) {
        patch[field] = value;
      } else if (
        (field === "Typical Number of F&B Outlets" && blankFb && isScalarBlank) ||
        (field === "Typical Number of Meeting Rooms" && blankMeeting && isScalarBlank)
      ) {
        patch[field] = value;
      }
    } else if (cur !== value) {
      patch[field] = value;
    }
  }

  if (!Object.keys(patch).length) {
    skipped.push({ standardsId: std.id, name, current, target: targetFields });
    continue;
  }

  planned.push({
    standardsId: std.id,
    basicsId: basics.id,
    name,
    chainScale: basics.get("Hotel Chain Scale"),
    serviceModel: basics.get("Hotel Service Model"),
    brandModel: basics.get("Brand Model"),
    architecture: basics.get("Brand Architecture"),
    current,
    target: targetFields,
    patch,
    reason: hasBlank ? "blank-field" : "profile-drift",
  });
}

const report = {
  generatedAt: new Date().toISOString(),
  mode: dryRun ? "dry-run" : "apply",
  fillBlanksOnly,
  activeBrandCount: basicsRows.length,
  updateCount: planned.length,
  skippedCount: skipped.length,
  missingStandardsCount: missingStandards.length,
  planned,
  missingStandards,
};

fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");

console.log(
  JSON.stringify(
    {
      mode: report.mode,
      fillBlanksOnly,
      activeBrandCount: report.activeBrandCount,
      updateCount: report.updateCount,
      skippedCount: report.skippedCount,
      missingStandardsCount: report.missingStandardsCount,
      report: REPORT_PATH,
    },
    null,
    2
  )
);

for (const item of planned) {
  console.log(
    `${item.name}: F&B ${item.current["F&B Outlets Required"] || "(blank)"} -> ${item.patch["F&B Outlets Required"] || item.current["F&B Outlets Required"]}; Meeting ${item.current["Meeting Space Required"] || "(blank)"} -> ${item.patch["Meeting Space Required"] || item.current["Meeting Space Required"]}`
  );
}

if (missingStandards.length) {
  console.error("Missing Brand Standards rows:", missingStandards.map((m) => m.name).join(", "));
  process.exitCode = 1;
}

if (!apply) {
  console.log("Dry run only. Re-run with --apply to write Brand Standards values.");
  process.exit(0);
}

const results = { updated: [], failed: [] };
for (const item of planned) {
  try {
    await base(STANDARDS_TABLE).update(item.standardsId, item.patch, { typecast: true });
    results.updated.push({ standardsId: item.standardsId, name: item.name, patch: item.patch });
  } catch (err) {
    results.failed.push({
      standardsId: item.standardsId,
      name: item.name,
      error: err?.message || String(err),
    });
  }
}

report.applyResults = results;
fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");

console.log(
  JSON.stringify({ updated: results.updated.length, failed: results.failed.length, report: REPORT_PATH }, null, 2)
);

if (results.failed.length) process.exit(1);
