#!/usr/bin/env node
/**
 * Migrate file-store shortlist → Airtable Operator Fit - Shortlist.
 *
 *   node scripts/operator-fit-shortlist-migrate-to-airtable.mjs --dry-run
 *   node scripts/operator-fit-shortlist-migrate-to-airtable.mjs --apply --approve-shortlist-migrate
 */
import "../load-env.js";
import { readFileSync, writeFileSync, copyFileSync, existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { airtableFetchJson } from "../api/lib/operator-setup-new-base-read.js";
import {
  OPERATOR_SHORTLIST_TABLE,
  map_operatorShortlistFields as F,
} from "../lib/operator-fit/shortlist.js";
import { loadShortlistStore, getShortlistStorePath } from "../lib/operator-fit/shortlist-store.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APPLY = process.argv.includes("--apply");
const APPROVED = process.argv.includes("--approve-shortlist-migrate");
const DRY = !APPLY;

function enc(s) {
  return encodeURIComponent(s);
}

async function listShortlistIds() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const out = [];
  let offset;
  do {
    let url = `https://api.airtable.com/v0/${baseId}/${enc(OPERATOR_SHORTLIST_TABLE)}?pageSize=100&fields%5B%5D=${enc(F.shortlistId)}`;
    if (offset) url += `&offset=${enc(offset)}`;
    const { ok, status, json } = await airtableFetchJson(url);
    if (!ok) throw new Error(`List shortlist failed ${status}`);
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function createRecord(fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const { ok, status, json } = await airtableFetchJson(
    `https://api.airtable.com/v0/${baseId}/${enc(OPERATOR_SHORTLIST_TABLE)}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields, typecast: true }),
    }
  );
  if (!ok) throw new Error(`CREATE shortlist: ${status} ${JSON.stringify(json)}`);
  return json;
}

async function main() {
  if (APPLY && !APPROVED) throw new Error("Refusing --apply without --approve-shortlist-migrate");
  const storePath = getShortlistStorePath();
  if (!existsSync(storePath)) throw new Error(`Missing file store: ${storePath}`);

  const backupDir = join(ROOT, "data", "operator-fit", "backups");
  mkdirSync(backupDir, { recursive: true });
  const backupPath = join(backupDir, `shortlist-store-backup-${Date.now()}.json`);
  copyFileSync(storePath, backupPath);

  const store = loadShortlistStore(storePath);
  const existing = await listShortlistIds();
  const existingIds = new Set(
    existing.map((r) => r.fields?.[F.shortlistId]).filter(Boolean)
  );

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY ? "dry-run" : "apply",
    table: OPERATOR_SHORTLIST_TABLE,
    backupPath,
    fileStorePath: storePath,
    notOdr: true,
    odrCreated: false,
    rows: [],
    errors: [],
  };

  for (const rec of store.records || []) {
    const row = {
      fileId: rec.id,
      dealId: rec.dealId,
      operatorId: rec.operatorId,
      operatorName: rec.operatorName,
      lifecycle: rec.snapshot?.lifecycle,
      snapshotValid: Boolean(rec.snapshot?.capturedAt && rec.snapshot?.alignment != null),
      duplicateInAirtable: existingIds.has(rec.id),
      action: null,
      airtableRecordId: null,
    };

    if (!row.snapshotValid) {
      row.action = "skip_invalid_snapshot";
      report.rows.push(row);
      continue;
    }
    if (row.duplicateInAirtable) {
      row.action = "skip_duplicate";
      report.rows.push(row);
      continue;
    }

    const fields = { ...(rec.airtableFields || {}) };
    // Ensure Operator link only when rec… master id
    if (rec.operatorId && String(rec.operatorId).startsWith("rec")) {
      fields[F.operator] = [rec.operatorId];
    } else {
      delete fields[F.operator];
    }
    fields[F.shortlistId] = rec.id;
    fields[F.snapshotJson] = JSON.stringify(rec.snapshot);

    row.action = DRY ? "would_create" : "create";
    if (!DRY) {
      try {
        const created = await createRecord(fields);
        row.airtableRecordId = created.id;
        existingIds.add(rec.id);
      } catch (err) {
        row.action = "error";
        row.error = err.message;
        report.errors.push({ id: rec.id, error: err.message });
      }
    }
    report.rows.push(row);
  }

  writeFileSync(
    join(ROOT, "reports", "operator-fit-shortlist-migration.json"),
    JSON.stringify(report, null, 2)
  );
  writeFileSync(
    join(ROOT, "reports", "operator-fit-shortlist-migration.md"),
    [
      "# Operator Fit Shortlist — File → Airtable Migration",
      "",
      `Mode: **${report.mode}** · ${report.generatedAt}`,
      "",
      `- Table: \`${OPERATOR_SHORTLIST_TABLE}\``,
      `- Backup: \`${backupPath}\``,
      `- ODR created: **no**`,
      "",
      "| File ID | Deal | Operator | Snapshot OK | Action | Airtable ID |",
      "| ------- | ---- | -------- | ----------- | ------ | ----------- |",
      ...report.rows.map(
        (r) =>
          `| ${r.fileId} | ${r.dealId} | ${r.operatorName} | ${r.snapshotValid} | ${r.action} | ${r.airtableRecordId || "—"} |`
      ),
      "",
      `Errors: ${report.errors.length}`,
      "",
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        migrated: report.rows.filter((r) => r.action === "create" || r.action === "would_create").length,
        skipped: report.rows.filter((r) => String(r.action).startsWith("skip")).length,
        errors: report.errors.length,
        backupPath,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
