#!/usr/bin/env node
/**
 * Fill Operator Setup Profile companyHistory with verified narratives only.
 * Clears invented staging histories.
 *
 *   node scripts/normalize-operator-setup-histories.mjs --dry-run
 *   node scripts/normalize-operator-setup-histories.mjs --apply --approve-normalize-operator-setup-histories
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { resolveOperatorHistory } from "../lib/partner-intelligence/operator-setup-histories-registry.js";
import { getOperatorFactoryQueueEntry } from "../lib/partner-intelligence/operator-explorer-factory-queue.js";
import { getOperatorQualityBaselineEntry } from "../lib/partner-intelligence/operator-explorer-quality-baseline.js";
import { upsertOperatorOneToOneTable } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MASTER_TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";
const PROFILE_TABLE = "Operator Setup - Profile & Positioning";

function parseArgs(argv) {
  const out = { apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-normalize-operator-setup-histories") out.approve = true;
  }
  return out;
}

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID");
  return new Airtable({ apiKey }).base(baseId);
}

function norm(s) {
  return String(s || "")
    .trim()
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n");
}

async function listAllMasters(base) {
  const rows = await base(MASTER_TABLE).select({ fields: ["company_name"], pageSize: 100 }).all();
  return rows.map((r) => ({
    recordId: r.id,
    companyName: String(r.fields?.company_name || "").trim(),
  }));
}

/** @returns {Promise<Map<string, { profileId: string, companyHistory: string|null }>>} */
async function indexProfilesByMaster(base) {
  const map = new Map();
  const rows = await base(PROFILE_TABLE)
    .select({ fields: ["Operator", "companyHistory"], pageSize: 100 })
    .all();
  for (const r of rows) {
    const ops = r.fields?.Operator;
    if (!Array.isArray(ops)) continue;
    const raw = r.fields?.companyHistory;
    const companyHistory = raw == null || String(raw).trim() === "" ? null : norm(raw);
    for (const masterId of ops) {
      if (!masterId || map.has(masterId)) continue;
      map.set(masterId, { profileId: r.id, companyHistory });
    }
  }
  return map;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Apply requires --approve-normalize-operator-setup-histories");
    process.exit(1);
  }

  console.log(`[normalize-histories] dryRun=${!args.apply} field=companyHistory`);
  const base = getBase();
  const masters = await listAllMasters(base);
  const profilesByMaster = await indexProfilesByMaster(base);

  const results = [];
  let alreadyCorrect = 0;
  let wouldUpdate = 0;
  let updated = 0;
  let wouldClear = 0;
  let cleared = 0;
  let leaveEmpty = 0;
  let needsManual = 0;
  let noProfile = 0;

  for (const m of masters) {
    const baseline = getOperatorQualityBaselineEntry(m.recordId);
    const queued = getOperatorFactoryQueueEntry(m.recordId);
    const slug = baseline?.slug || queued?.slug || null;
    const spec = resolveOperatorHistory({ slug, companyName: m.companyName });
    const profile = profilesByMaster.get(m.recordId) || null;

    const row = {
      recordId: m.recordId,
      companyName: m.companyName,
      slug,
      profileId: profile?.profileId || null,
      currentLen: profile?.companyHistory?.length ?? 0,
      targetLen: spec?.companyHistory?.length ?? 0,
      sourceNote: spec?.sourceNote || null,
      action: "noop",
      targetPreview: spec?.companyHistory ? spec.companyHistory.slice(0, 120) : null,
    };

    if (!spec) {
      row.action = "needs_manual_history";
      needsManual += 1;
      console.log(`needs_manual             ${m.companyName}`);
      results.push(row);
      continue;
    }
    if (!profile) {
      row.action = "skip_no_profile";
      noProfile += 1;
      console.log(`skip_no_profile          ${m.companyName}`);
      results.push(row);
      continue;
    }

    const current = profile.companyHistory;
    const target = spec.companyHistory;

    if (target == null) {
      if (current == null) {
        row.action = "leave_empty";
        leaveEmpty += 1;
        console.log(`leave_empty              ${m.companyName}`);
        results.push(row);
        continue;
      }
      if (!args.apply) {
        row.action = "would_clear";
        wouldClear += 1;
        console.log(`would_clear              ${m.companyName} (${current.length} chars)`);
        results.push(row);
        continue;
      }
      await upsertOperatorOneToOneTable(
        PROFILE_TABLE,
        m.recordId,
        { companyHistory: "" },
        `history-normalize-${slug || m.recordId}`
      );
      row.action = "cleared";
      cleared += 1;
      console.log(`cleared                  ${m.companyName}`);
      results.push(row);
      continue;
    }

    if (norm(current) === norm(target)) {
      row.action = "already_correct";
      alreadyCorrect += 1;
      console.log(`already_correct          ${m.companyName} (${target.length} chars)`);
      results.push(row);
      continue;
    }

    if (!args.apply) {
      row.action = "would_update";
      wouldUpdate += 1;
      console.log(
        `would_update             ${m.companyName}: ${current?.length ?? 0} → ${target.length} chars`
      );
      results.push(row);
      continue;
    }

    await upsertOperatorOneToOneTable(
      PROFILE_TABLE,
      m.recordId,
      { companyHistory: target },
      `history-normalize-${slug || m.recordId}`
    );
    row.action = "updated";
    updated += 1;
    console.log(`updated                  ${m.companyName} (${target.length} chars)`);
    results.push(row);
  }

  const summary = {
    masters: masters.length,
    alreadyCorrect,
    wouldUpdate,
    updated,
    wouldClear,
    cleared,
    leaveEmpty,
    needsManual,
    noProfile,
  };
  const outPath = path.join(ROOT, "reports", "normalize-operator-setup-histories.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify({ summary, results }, null, 2));
  console.log(`Wrote ${outPath}`);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
