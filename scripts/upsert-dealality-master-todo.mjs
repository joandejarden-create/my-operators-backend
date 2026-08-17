/**
 * Upsert Dealality master to-do seed tasks into Airtable.
 *
 *   node scripts/upsert-dealality-master-todo.mjs --dry-run
 *   node scripts/upsert-dealality-master-todo.mjs --execute
 *   node scripts/upsert-dealality-master-todo.mjs --execute --table-id tblpCg0QZ0kIPXihE
 *   node scripts/upsert-dealality-master-todo.mjs --execute --force-update-completed
 *   node scripts/upsert-dealality-master-todo.mjs --dry-run --only mt-26,mt-27,mt-28
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  buildMatchKey,
  fetchAllRecords,
  fetchAllTables,
  getGtmConfig,
} from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import {
  COMPLETED_STATUS_VALUES,
  MAP_MASTER_TODO,
  MASTER_TODO_DEFAULT_TABLE_ID,
  MASTER_TODO_SOURCE_VALUE,
  STATUS_WRITE_MAP,
  mapPriorityForWrite,
} from "../lib/dealality-master-todo/master-todo-field-map.js";
import { MASTER_TODO_SEED } from "../lib/dealality-master-todo/master-todo-seed.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function argValue(flag, fallback = "") {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] || fallback;
}

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;
const FORCE_COMPLETED = process.argv.includes("--force-update-completed");
const ONLY_SEED_IDS = new Set(
  argValue("--only", "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
);

const TABLE_ID = argValue("--table-id", MASTER_TODO_DEFAULT_TABLE_ID);
const REPORT_PATH = path.resolve(
  ROOT,
  argValue("--report", "reports/dealality-master-todo-upsert-report.json")
);

function isCompletedStatus(status) {
  return COMPLETED_STATUS_VALUES.has(String(status || "").trim().toLowerCase());
}

function mapStatusForWrite(seedStatus, existingFieldNames) {
  const mapped = STATUS_WRITE_MAP[seedStatus] || seedStatus;
  return mapped;
}

/**
 * Build Airtable fields payload from seed row.
 * @param {import('../lib/dealality-master-todo/master-todo-seed.js').MASTER_TODO_SEED[0]} seed
 * @param {Set<string>} existingFieldNames lowercased
 */
export function seedToAirtableFields(seed, existingFieldNames) {
  const has = (name) => existingFieldNames.has(name.toLowerCase());
  const F = MAP_MASTER_TODO;
  const fields = {};

  fields[F.task] = seed.taskName;
  fields[F.workstream] = seed.workstream;
  fields[F.status] = mapStatusForWrite(seed.status);
  fields[F.priority] = mapPriorityForWrite(seed.priority);
  fields[F.phase] = seed.phase;
  if (seed.owner) fields[F.owner] = seed.owner;
  else fields[F.owner] = "Joan D.";
  if (seed.description) fields[F.description] = seed.description;
  if (seed.nextAction) fields[F.nextAction] = seed.nextAction;
  if (seed.progress) fields[F.progress] = seed.progress;

  if (has(F.source)) fields[F.source] = MASTER_TODO_SOURCE_VALUE;
  if (has(F.relatedArea) && seed.relatedArea) fields[F.relatedArea] = seed.relatedArea;

  if (!has(F.source)) {
    const tag = `[Master To-Do | ${MASTER_TODO_SOURCE_VALUE}]`;
    fields[F.description] = `${tag}\n\n${fields[F.description] || ""}`.trim();
  }

  fields[F.successMetric] = `Seed ID: ${seed.id}`;

  return fields;
}

function indexExistingRecords(records) {
  const byMatch = new Map();
  const bySeedId = new Map();
  const bySource = [];
  for (const rec of records) {
    const f = rec.fields || {};
    const task = f[MAP_MASTER_TODO.task] || "";
    const ws = f[MAP_MASTER_TODO.workstream] || "";
    const key = buildMatchKey(task, ws);
    if (!byMatch.has(key)) byMatch.set(key, []);
    byMatch.get(key).push(rec);

    const sm = String(f[MAP_MASTER_TODO.successMetric] || "");
    const seedMatch = sm.match(/Seed ID:\s*(mt-\d+)/i);
    if (seedMatch?.[1] && !bySeedId.has(seedMatch[1])) {
      bySeedId.set(seedMatch[1], rec);
    }

    const source = f[MAP_MASTER_TODO.source] || "";
    const desc = f[MAP_MASTER_TODO.description] || "";
    if (
      source === MASTER_TODO_SOURCE_VALUE ||
      String(desc).includes(MASTER_TODO_SOURCE_VALUE) ||
      String(f[MAP_MASTER_TODO.successMetric] || "").startsWith("Seed ID:")
    ) {
      bySource.push(rec);
    }
  }
  return { byMatch, bySeedId, bySource };
}

function findMatch(seed, byMatch, bySeedId) {
  const byId = bySeedId?.get(seed.id);
  if (byId) return { record: byId, confidence: "high", matchKey: `seed:${seed.id}` };

  const key = buildMatchKey(seed.taskName, seed.workstream);
  const hits = byMatch.get(key) || [];
  if (hits.length === 1) return { record: hits[0], confidence: "high", matchKey: key };
  if (hits.length > 1) return { ambiguous: hits, confidence: "low", matchKey: key };

  const nameOnlyKey = buildMatchKey(seed.taskName, "");
  const nameHits = (byMatch.get(nameOnlyKey) || []).filter(
    (r) => !String(r.fields?.[MAP_MASTER_TODO.workstream] || "").trim()
  );
  if (nameHits.length === 1) return { record: nameHits[0], confidence: "medium", matchKey: nameOnlyKey };
  if (nameHits.length > 1) return { ambiguous: nameHits, confidence: "low", matchKey: nameOnlyKey };

  return null;
}

function diffFields(before, after) {
  const changes = {};
  for (const [k, v] of Object.entries(after)) {
    const prev = before[k];
    if (JSON.stringify(prev) !== JSON.stringify(v)) {
      changes[k] = { before: prev ?? null, after: v };
    }
  }
  return changes;
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const tables = await fetchAllTables(baseId, token);
  const tableMeta = tables.find((t) => t.id === TABLE_ID);
  const schemaFieldNames = new Set(
    (tableMeta?.fields || []).map((f) => f.name.toLowerCase())
  );

  const existing = await fetchAllRecords(baseId, token, TABLE_ID);
  const existingFieldNames = new Set(schemaFieldNames);

  const { byMatch, bySeedId } = indexExistingRecords(existing);
  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    baseId,
    tableId: TABLE_ID,
    existingRecordCount: existing.length,
    seedCount: MASTER_TODO_SEED.length,
    toCreate: [],
    toUpdate: [],
    skipped: [],
    possibleDuplicates: [],
    errors: [],
    created: [],
    updated: [],
  };

  const creates = [];
  const updates = [];

  for (const seed of MASTER_TODO_SEED) {
    if (ONLY_SEED_IDS.size && !ONLY_SEED_IDS.has(seed.id)) {
      report.skipped.push({
        seedId: seed.id,
        taskName: seed.taskName,
        reason: "excluded by --only filter",
      });
      continue;
    }
    const fields = seedToAirtableFields(seed, existingFieldNames);
    const match = findMatch(seed, byMatch, bySeedId);

    if (!match) {
      report.toCreate.push({ seedId: seed.id, taskName: seed.taskName, fields });
      if (!DRY_RUN) creates.push({ fields });
      continue;
    }

    if (match.ambiguous) {
      report.possibleDuplicates.push({
        seedId: seed.id,
        taskName: seed.taskName,
        matchKey: match.matchKey,
        recordIds: match.ambiguous.map((r) => r.id),
        action: "skip_update",
      });
      continue;
    }

    const rec = match.record;
    const currentStatus = rec.fields?.[MAP_MASTER_TODO.status];
    if (isCompletedStatus(currentStatus) && !FORCE_COMPLETED) {
      report.skipped.push({
        seedId: seed.id,
        recordId: rec.id,
        taskName: seed.taskName,
        reason: "existing record is completed; use --force-update-completed to override",
        confidence: match.confidence,
      });
      continue;
    }

    // Preserve Airtable task/workstream labels when matched by Seed ID
    // (live titles often diverge from seed short names).
    if (String(match.matchKey || "").startsWith("seed:")) {
      delete fields[MAP_MASTER_TODO.task];
      delete fields[MAP_MASTER_TODO.workstream];
    }

    const changes = diffFields(rec.fields || {}, fields);
    if (!Object.keys(changes).length) {
      report.skipped.push({
        seedId: seed.id,
        recordId: rec.id,
        taskName: seed.taskName,
        reason: "no field changes",
        confidence: match.confidence,
      });
      continue;
    }

    report.toUpdate.push({
      seedId: seed.id,
      recordId: rec.id,
      taskName: seed.taskName,
      confidence: match.confidence,
      changes,
      fields,
    });
    if (!DRY_RUN) updates.push({ id: rec.id, fields });
  }

  if (!DRY_RUN) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < creates.length; i += 10) {
      const batch = creates.slice(i, i + 10);
      try {
        const created = await base(TABLE_ID).create(batch, { typecast: true });
        report.created.push(
          ...created.map((r) => ({
            id: r.id,
            task: r.fields?.[MAP_MASTER_TODO.task],
          }))
        );
      } catch (err) {
        report.errors.push({ action: "create", message: err.message || String(err) });
      }
    }
    for (let i = 0; i < updates.length; i += 10) {
      const batch = updates.slice(i, i + 10);
      try {
        const updated = await base(TABLE_ID).update(batch, { typecast: true });
        report.updated.push(
          ...updated.map((r) => ({
            id: r.id,
            task: r.fields?.[MAP_MASTER_TODO.task],
          }))
        );
      } catch (err) {
        report.errors.push({ action: "update", message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nDealality master to-do upsert (${report.mode})`);
  console.log(`Table: ${TABLE_ID}`);
  console.log(`Existing records: ${report.existingRecordCount}`);
  console.log(`Proposed creates: ${report.toCreate.length}`);
  console.log(`Proposed updates: ${report.toUpdate.length}`);
  console.log(`Skipped: ${report.skipped.length}`);
  console.log(`Possible duplicates: ${report.possibleDuplicates.length}`);
  if (!DRY_RUN) {
    console.log(`Created: ${report.created.length}`);
    console.log(`Updated: ${report.updated.length}`);
    console.log(`Errors: ${report.errors.length}`);
  }
  console.log(`Report: ${REPORT_PATH}`);

  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[upsert-dealality-master-todo]", err.message || err);
  process.exit(1);
});
