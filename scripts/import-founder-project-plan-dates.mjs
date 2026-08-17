/**
 * Import Start / End dates from Founder Project Plan Excel into Airtable.
 *
 *   node scripts/import-founder-project-plan-dates.mjs --dry-run
 *   node scripts/import-founder-project-plan-dates.mjs --execute
 *   node scripts/import-founder-project-plan-dates.mjs --dry-run --report reports/founder-plan-date-import.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import XLSX from "xlsx";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const DEFAULT_XLSX =
  "g:\\My Drive\\Dealality™\\Dealality™ - Founder Project Plan.xlsx";
const TABLE_ID = "tblpCg0QZ0kIPXihE";
const SHEET_NAME = "Project schedule";
const HEADER_ROW_INDEX = 4;
const DATA_START_ROW = 7;

const MAP = {
  phase: 1,
  workstream: 2,
  task: 3,
  objective: 4,
  deliverables: 5,
  assignedTo: 6,
  progress: 7,
  status: 8,
  start: 9,
  end: 10,
  duration: 11,
};

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;
const XLSX_PATH = argValue("--xlsx", DEFAULT_XLSX);
const REPORT_PATH = path.resolve(ROOT, argValue("--report", "reports/founder-plan-date-import.json"));

function argValue(flag, fallback = "") {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] || fallback;
}

function norm(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[’']/g, "'");
}

function excelSerialToIso(value) {
  if (value == null || value === "") return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Date.parse(trimmed);
    if (!Number.isNaN(parsed)) return new Date(parsed).toISOString().slice(0, 10);
    return null;
  }
  if (typeof value === "number" && value > 30000 && value < 80000) {
    const d = XLSX.SSF.parse_date_code(value);
    if (!d) return null;
    return `${d.y}-${String(d.m).padStart(2, "0")}-${String(d.d).padStart(2, "0")}`;
  }
  return null;
}

function isPlaceholderRow(row) {
  const phase = String(row[MAP.phase] ?? "").trim();
  const workstream = String(row[MAP.workstream] ?? "").trim();
  const task = String(row[MAP.task] ?? "").trim();
  if (!phase && !workstream && !task) return true;
  if (phase === "__" || workstream === "__" || task === "__") return true;
  if (/do not delete this row/i.test(String(row[0] ?? "") + workstream + task)) return true;
  return false;
}

function buildMatchKeys(row) {
  const phase = norm(row.phase);
  const workstream = norm(row.workstream);
  const task = norm(row.task);
  const objective = norm(row.objective);
  const keys = [];
  if (phase && workstream && task && objective) keys.push(`p|${phase}|w|${workstream}|t|${task}|o|${objective}`);
  if (phase && task && objective) keys.push(`p|${phase}|t|${task}|o|${objective}`);
  if (workstream && task && objective) keys.push(`w|${workstream}|t|${task}|o|${objective}`);
  if (phase && workstream && task) keys.push(`p|${phase}|w|${workstream}|t|${task}`);
  if (workstream && task) keys.push(`w|${workstream}|t|${task}`);
  if (task && objective) keys.push(`t|${task}|o|${objective}`);
  if (task) keys.push(`t|${task}`);
  return keys;
}

function buildAirtableMatchKeys(record) {
  const f = record.fields || {};
  const row = {
    phase: f.Phase,
    workstream: f.Workstream,
    task: f.Task,
    objective: f["Task Objective/Description"],
  };
  return buildMatchKeys(row);
}

function parseExcelRows(filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Excel file not found: ${filePath}`);
  }
  const wb = XLSX.readFile(filePath);
  const sheet = wb.Sheets[SHEET_NAME];
  if (!sheet) {
    throw new Error(`Sheet "${SHEET_NAME}" not found. Available: ${wb.SheetNames.join(", ")}`);
  }
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: null });
  const parsed = [];
  for (let i = DATA_START_ROW; i < rows.length; i++) {
    const raw = rows[i] || [];
    if (isPlaceholderRow(raw)) continue;

    const start = excelSerialToIso(raw[MAP.start]);
    const end = excelSerialToIso(raw[MAP.end]);
    const phase = String(raw[MAP.phase] ?? "").trim();
    const workstream = String(raw[MAP.workstream] ?? "").trim();
    const task = String(raw[MAP.task] ?? "").trim();
    const objective = String(raw[MAP.objective] ?? "").trim();

    if (!task && !workstream && !objective) continue;
    if (!start && !end) continue;

    parsed.push({
      excelRow: i + 1,
      phase,
      workstream,
      task,
      objective,
      start,
      end,
      status: String(raw[MAP.status] ?? "").trim(),
    });
  }
  return parsed;
}

async function fetchAllAirtableRecords(baseId, apiKey) {
  const records = [];
  let offset;
  do {
    const url = `https://api.airtable.com/v0/${baseId}/${TABLE_ID}?pageSize=100${offset ? `&offset=${offset}` : ""}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json();
    if (!res.ok) throw new Error(`Airtable list failed (${res.status}): ${JSON.stringify(json)}`);
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
}

function indexAirtableRecords(records) {
  const index = new Map();
  for (const rec of records) {
    for (const key of buildAirtableMatchKeys(rec)) {
      if (!index.has(key)) index.set(key, []);
      index.get(key).push(rec);
    }
  }
  return index;
}

function matchExcelRow(row, index) {
  const keys = buildMatchKeys(row);
  for (const key of keys) {
    const hits = index.get(key);
    if (hits?.length === 1) return { record: hits[0], matchKey: key, strategy: key.split("|")[0] };
    if (hits?.length > 1) return { ambiguous: hits, matchKey: key };
  }
  return null;
}

async function main() {
  const apiKey = (process.env.AIRTABLE_TOKEN || process.env.AIRTABLE_PAT || "").trim();
  const baseId = (process.env.AIRTABLE_GTM_BASE_ID || process.env.AIRTABLE_BASE_ID || "").trim();
  if (!apiKey) throw new Error("Set AIRTABLE_TOKEN or AIRTABLE_PAT.");
  if (!baseId) throw new Error("Set AIRTABLE_GTM_BASE_ID.");

  const excelRows = parseExcelRows(XLSX_PATH);
  const airtableRecords = await fetchAllAirtableRecords(baseId, apiKey);
  const index = indexAirtableRecords(airtableRecords);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    xlsxPath: XLSX_PATH,
    sheet: SHEET_NAME,
    excelRowsWithDates: excelRows.length,
    airtableRecords: airtableRecords.length,
    matched: [],
    unmatched: [],
    ambiguous: [],
    skippedAlreadySet: [],
    updates: [],
    errors: [],
  };

  const updatesByRecordId = new Map();

  for (const row of excelRows) {
    const result = matchExcelRow(row, index);
    if (!result) {
      report.unmatched.push(row);
      continue;
    }
    if (result.ambiguous) {
      report.ambiguous.push({
        excelRow: row.excelRow,
        task: row.task,
        workstream: row.workstream,
        matchKey: result.matchKey,
        recordIds: result.ambiguous.map((r) => r.id),
      });
      continue;
    }

    const rec = result.record;
    const existingStart = rec.fields.Start || null;
    const existingEnd = rec.fields.End || null;
    const patch = {};
    if (row.start) patch.Start = row.start;
    if (row.end) patch.End = row.end;

    if (!Object.keys(patch).length) continue;

    const unchanged =
      (patch.Start == null || patch.Start === existingStart) &&
      (patch.End == null || patch.End === existingEnd);
    if (unchanged && existingStart && existingEnd) {
      report.skippedAlreadySet.push({ recordId: rec.id, excelRow: row.excelRow, task: row.task });
      continue;
    }

    report.matched.push({
      recordId: rec.id,
      excelRow: row.excelRow,
      matchKey: result.matchKey,
      task: row.task,
      workstream: row.workstream,
      phase: row.phase,
      start: row.start,
      end: row.end,
      before: { Start: existingStart, End: existingEnd },
      after: patch,
    });

    updatesByRecordId.set(rec.id, {
      id: rec.id,
      fields: { ...(updatesByRecordId.get(rec.id)?.fields || {}), ...patch },
      meta: row,
    });
  }

  report.updates = [...updatesByRecordId.values()].map((u) => ({
    id: u.id,
    fields: u.fields,
    excelRow: u.meta.excelRow,
    task: u.meta.task,
  }));

  if (!DRY_RUN && updatesByRecordId.size) {
    const base = new Airtable({ apiKey }).base(baseId);
    const updates = [...updatesByRecordId.values()].map((u) => ({ id: u.id, fields: u.fields }));
    for (let i = 0; i < updates.length; i += 10) {
      const batch = updates.slice(i, i + 10);
      try {
        await base(TABLE_ID).update(batch);
      } catch (err) {
        report.errors.push({
          batchStart: i,
          message: err.message || String(err),
          recordIds: batch.map((b) => b.id),
        });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nFounder Project Plan date import (${report.mode})`);
  console.log(`Excel rows with dates: ${report.excelRowsWithDates}`);
  console.log(`Airtable records: ${report.airtableRecords}`);
  console.log(`Matched updates: ${report.updates.length}`);
  console.log(`Unmatched excel rows: ${report.unmatched.length}`);
  console.log(`Ambiguous matches: ${report.ambiguous.length}`);
  console.log(`Errors: ${report.errors.length}`);
  console.log(`Report: ${REPORT_PATH}`);

  if (report.unmatched.length) {
    console.log("\nUnmatched sample (first 10):");
    for (const row of report.unmatched.slice(0, 10)) {
      console.log(`  row ${row.excelRow}: [${row.phase}] ${row.workstream} / ${row.task?.slice(0, 50)}`);
    }
  }
  if (report.ambiguous.length) {
    console.log("\nAmbiguous sample:");
    for (const row of report.ambiguous.slice(0, 5)) {
      console.log(`  row ${row.excelRow}: ${row.task} -> ${row.recordIds.length} records`);
    }
  }
  if (report.updates.length) {
    console.log("\nUpdate sample (first 5):");
    for (const u of report.updates.slice(0, 5)) {
      console.log(`  ${u.id} row ${u.excelRow}: Start=${u.fields.Start} End=${u.fields.End} | ${u.task?.slice(0, 50)}`);
    }
  }

  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[import-founder-project-plan-dates]", err.message || err);
  process.exit(1);
});
