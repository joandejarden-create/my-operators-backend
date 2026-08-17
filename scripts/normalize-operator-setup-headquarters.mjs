#!/usr/bin/env node
/**
 * Normalize Operator Setup Profile `headquarters` to City, Country for all Masters.
 *
 *   node scripts/normalize-operator-setup-headquarters.mjs --dry-run
 *   node scripts/normalize-operator-setup-headquarters.mjs --apply --approve-normalize-operator-setup-headquarters
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  resolveOperatorHeadquarters,
  isValidOperatorHeadquartersFormat,
  OPERATOR_SETUP_HQ_FORMAT,
} from "../lib/partner-intelligence/operator-setup-headquarters-registry.js";
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
    else if (a === "--approve-normalize-operator-setup-headquarters") out.approve = true;
  }
  return out;
}

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID");
  return new Airtable({ apiKey }).base(baseId);
}

async function listAllMasters(base) {
  const rows = await base(MASTER_TABLE).select({ fields: ["company_name"], pageSize: 100 }).all();
  return rows.map((r) => ({
    recordId: r.id,
    companyName: String(r.fields?.company_name || "").trim(),
  }));
}

/** @returns {Promise<Map<string, { profileId: string, headquarters: string }>>} */
async function indexProfilesByMaster(base) {
  /** @type {Map<string, { profileId: string, headquarters: string }>} */
  const map = new Map();
  const rows = await base(PROFILE_TABLE)
    .select({ fields: ["Operator", "headquarters"], pageSize: 100 })
    .all();
  for (const r of rows) {
    const ops = r.fields?.Operator;
    if (!Array.isArray(ops)) continue;
    const headquarters = String(r.fields?.headquarters || "").trim();
    for (const masterId of ops) {
      if (!masterId) continue;
      // Prefer first linked row if duplicates exist
      if (!map.has(masterId)) {
        map.set(masterId, { profileId: r.id, headquarters });
      }
    }
  }
  return map;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Apply requires --approve-normalize-operator-setup-headquarters");
    process.exit(1);
  }

  console.log(`[normalize-hq] dryRun=${!args.apply} format=${OPERATOR_SETUP_HQ_FORMAT}`);
  const base = getBase();
  const masters = await listAllMasters(base);
  const profilesByMaster = await indexProfilesByMaster(base);
  const results = [];

  for (const m of masters) {
    const baseline = getOperatorQualityBaselineEntry(m.recordId);
    const queued = getOperatorFactoryQueueEntry(m.recordId);
    const slug = baseline?.slug || queued?.slug || null;
    const spec = resolveOperatorHeadquarters({
      slug,
      recordId: m.recordId,
      companyName: m.companyName,
    });
    const profile = profilesByMaster.get(m.recordId) || null;
    const current = profile?.headquarters || "";
    const valid = isValidOperatorHeadquartersFormat(current);
    const target = spec?.headquarters || null;

    const row = {
      recordId: m.recordId,
      companyName: m.companyName,
      slug,
      profileId: profile?.profileId || null,
      currentHeadquarters: current || null,
      currentValid: valid,
      targetHeadquarters: target,
      sourceNote: spec?.sourceNote || null,
      action: "noop",
    };

    if (!profile) {
      row.action = "skip_no_profile";
    } else if (!target) {
      row.action = valid ? "ok_unmapped_but_valid" : "needs_manual_hq_city";
    } else if (current === target && valid) {
      row.action = "already_correct";
    } else if (!args.apply) {
      row.action = "would_update";
    } else {
      const res = await upsertOperatorOneToOneTable(
        PROFILE_TABLE,
        m.recordId,
        { headquarters: target },
        `hq-normalize-${slug || m.recordId}`
      );
      row.profileId = res.recordId || row.profileId;
      row.action = "updated";
    }
    results.push(row);
    console.log(
      `${row.action.padEnd(24)} ${m.companyName || m.recordId}: "${current || "∅"}" → ${target || "(no registry)"}`
    );
  }

  const report = {
    version: "normalize-operator-setup-headquarters-v1",
    dryRun: !args.apply,
    formatRule: OPERATOR_SETUP_HQ_FORMAT,
    generatedAt: new Date().toISOString(),
    summary: {
      masters: results.length,
      alreadyCorrect: results.filter((r) => r.action === "already_correct").length,
      wouldUpdate: results.filter((r) => r.action === "would_update").length,
      updated: results.filter((r) => r.action === "updated").length,
      needsManual: results.filter((r) => r.action === "needs_manual_hq_city").length,
      noProfile: results.filter((r) => r.action === "skip_no_profile").length,
      okUnmapped: results.filter((r) => r.action === "ok_unmapped_but_valid").length,
    },
    results,
  };

  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "normalize-operator-setup-headquarters.json");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  console.log("Wrote", jsonPath);
  console.log(JSON.stringify(report.summary, null, 2));
  if (report.summary.needsManual > 0) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
