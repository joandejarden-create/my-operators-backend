#!/usr/bin/env node
/**
 * Backfill empty fields on all Operator Setup - Leadership Team Members rows.
 *
 *   node scripts/backfill-operator-leadership-team-members.mjs
 *   node scripts/backfill-operator-leadership-team-members.mjs --apply
 *   node scripts/backfill-operator-leadership-team-members.mjs --operator-id recXXX --apply
 *   node scripts/backfill-operator-leadership-team-members.mjs --apply --overwrite
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";
import {
  NEW_BASE_MASTER_TABLE,
  NEW_BASE_LEADERSHIP_TABLE,
  fetchAllRecordsRest,
  rowsLinkedToMaster,
} from "../api/lib/operator-setup-new-base-read.js";
import { normalizeWhitespace } from "../api/lib/operator-leadership-member-map.js";
import {
  MAP_LEADERSHIP_MEMBER,
  buildLeadershipMemberBackfillPatch,
  mergeLeadershipBackfill,
  validatePatchOptions,
} from "../lib/operator-leadership-member-backfill.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function parseArgs() {
  const args = process.argv.slice(2);
  let operatorId = "";
  let apply = false;
  let overwrite = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--operator-id" && args[i + 1]) operatorId = args[++i];
    if (args[i] === "--apply") apply = true;
    if (args[i] === "--overwrite") overwrite = true;
  }
  return { operatorId, apply, overwrite };
}

function masterIdFromRow(fields) {
  const op = fields?.Operator;
  return Array.isArray(op) && op[0] ? op[0] : "";
}

function masterLabel(fields) {
  return normalizeWhitespace(fields?.company_name || fields?.Company || fields?.operator_name || "");
}

async function main() {
  const { operatorId, apply, overwrite } = parseArgs();

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const [leadershipRows, masterRows] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_LEADERSHIP_TABLE),
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE).catch(() => []),
  ]);

  const masterById = new Map((masterRows || []).map((r) => [r.id, r.fields || {}]));

  let rows = leadershipRows || [];
  if (operatorId) {
    rows = rowsLinkedToMaster(rows, operatorId);
    if (!rows.length) {
      console.error("No leadership rows for master", operatorId);
      process.exit(1);
    }
  }

  const byMaster = new Map();
  for (const rec of rows) {
    const mid = masterIdFromRow(rec.fields);
    if (!byMaster.has(mid)) byMaster.set(mid, []);
    byMaster.get(mid).push(rec);
  }

  const updates = [];
  const skipped = [];

  for (const [, group] of byMaster) {
    const sorted = [...group].sort(
      (a, b) =>
        Number(a.fields?.[MAP_LEADERSHIP_MEMBER.displayOrder] || 0) -
        Number(b.fields?.[MAP_LEADERSHIP_MEMBER.displayOrder] || 0)
    );
    sorted.forEach((rec, idx) => {
      const f = rec.fields || {};
      const mid = masterIdFromRow(f);
      const mFields = masterById.get(mid) || {};
      const patch = buildLeadershipMemberBackfillPatch(f, {
        displayOrder: idx + 1,
        operatorLabel: masterLabel(mFields),
      });
      const merged = mergeLeadershipBackfill(f, patch, { overwrite });
      const warnings = validatePatchOptions(merged);
      if (warnings.length) {
        skipped.push({ id: rec.id, name: f.name, warnings });
        return;
      }
      if (!Object.keys(merged).length) return;
      updates.push({
        id: rec.id,
        name: f[MAP_LEADERSHIP_MEMBER.name] || f.name || rec.id,
        masterId: mid,
        fields: merged,
        fieldCount: Object.keys(merged).length,
      });
    });
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const reportPath = path.join(
    ROOT,
    "reports",
    `leadership-team-members-backfill-${stamp}.json`
  );
  const report = {
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    overwrite,
    operatorFilter: operatorId || null,
    totalRows: rows.length,
    updateCount: updates.length,
    skippedCount: skipped.length,
    updates: updates.map((u) => ({
      id: u.id,
      name: u.name,
      masterId: u.masterId,
      fieldCount: u.fieldCount,
      fields: Object.keys(u.fields),
    })),
    skipped,
  };
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));

  console.log(`Leadership rows scanned: ${rows.length}`);
  console.log(`Rows with fields to ${apply ? "patch" : "fill"}: ${updates.length}`);
  if (skipped.length) console.log(`Skipped (validation): ${skipped.length}`);
  console.log(`Report: ${reportPath}`);

  if (!apply) {
    console.log("\nDry run — pass --apply to write to Airtable.");
    updates.slice(0, 5).forEach((u) => {
      console.log(`  ${u.name} (${u.id}): +${u.fieldCount} fields → ${Object.keys(u.fields).join(", ")}`);
    });
    if (updates.length > 5) console.log(`  … and ${updates.length - 5} more`);
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
  let written = 0;
  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10);
    await base(NEW_BASE_LEADERSHIP_TABLE).update(
      chunk.map((u) => ({ id: u.id, fields: u.fields })),
      { typecast: true }
    );
    written += chunk.length;
    process.stdout.write(`\rPatched ${written}/${updates.length}…`);
  }
  console.log(`\nDone. Updated ${written} leadership row(s).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
