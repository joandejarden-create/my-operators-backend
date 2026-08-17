#!/usr/bin/env node
/**
 * Smoke detail API for brand-managed Core 5, then promote submission_status → Active.
 *
 *   node scripts/promote-operator-explorer-brand-managed-active.mjs --dry-run
 *   node scripts/promote-operator-explorer-brand-managed-active.mjs --apply --approve-promote-operator-brand-managed-active
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import getThirdPartyOperatorDetail from "../api/third-party-operator-detail.js";
import { getOperatorFactoryQueueEntry } from "../lib/partner-intelligence/operator-explorer-factory-queue.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MASTER_TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

const CORE5_SLUGS = [
  "marriott-international-managed",
  "ihg-managed",
  "hilton-managed",
  "accor-managed",
  "minor-hotels-managed",
];

function parseArgs(argv) {
  const out = { apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-promote-operator-brand-managed-active") out.approve = true;
  }
  return out;
}

function mockRes() {
  const state = { statusCode: 200, body: null };
  return {
    status(code) {
      state.statusCode = code;
      return this;
    },
    json(payload) {
      state.body = payload;
      return this;
    },
    get state() {
      return state;
    },
  };
}

async function smokeDetail(recordId) {
  const req = { params: { recordId } };
  const res = mockRes();
  await getThirdPartyOperatorDetail(req, res);
  const { statusCode, body } = res.state;
  return {
    statusCode,
    success: Boolean(body?.success),
    error: body?.error || null,
    companyName: body?.operator?.prefill?.companyName || body?.operator?.fields?.company_name || null,
    hasPrefill: Boolean(body?.operator?.prefill),
  };
}

async function patchMasterSubmissionStatus(recordId, status) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(MASTER_TABLE)}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields: { submission_status: status }, typecast: true }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error((data?.error?.message || data?.error?.type || res.statusText) + " (submission_status patch)");
  }
  return data;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Apply requires --approve-promote-operator-brand-managed-active");
    process.exit(1);
  }
  if (!process.env.AIRTABLE_BASE_ID || !process.env.AIRTABLE_API_KEY) {
    console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
    process.exit(1);
  }

  const results = [];
  for (const slug of CORE5_SLUGS) {
    const q = getOperatorFactoryQueueEntry(slug);
    if (!q?.recordId) {
      results.push({ slug, ok: false, error: "missing queue recordId" });
      continue;
    }
    const smoke = await smokeDetail(q.recordId);
    const row = {
      slug,
      recordId: q.recordId,
      companyName: q.companyName,
      smoke,
      wouldPromote: smoke.success,
      promoted: false,
    };
    if (args.apply && smoke.success) {
      await patchMasterSubmissionStatus(q.recordId, "Active");
      row.promoted = true;
      row.submission_status = "Active";
    } else if (args.apply && !smoke.success) {
      row.error = "Skipped promote — detail smoke failed";
    } else if (smoke.success) {
      row.submission_status = "would_set_Active";
    }
    results.push(row);
    console.log(
      `${slug}: smoke=${smoke.success} status=${smoke.statusCode}` +
        (row.promoted ? " promoted=Active" : args.apply ? " not_promoted" : smoke.success ? " would_promote" : "")
    );
  }

  const report = {
    version: "promote-operator-explorer-brand-managed-active-v1",
    dryRun: !args.apply,
    generatedAt: new Date().toISOString(),
    summary: {
      total: results.length,
      smokePass: results.filter((r) => r.smoke?.success).length,
      smokeFail: results.filter((r) => !r.smoke?.success).length,
      promoted: results.filter((r) => r.promoted).length,
    },
    results,
  };

  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "promote-operator-explorer-brand-managed-active.json");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  console.log(`Wrote ${jsonPath}`);
  console.log(JSON.stringify(report.summary, null, 2));

  if (report.summary.smokeFail > 0) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
