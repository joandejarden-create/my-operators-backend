/**
 * Apply Arbor Lodging (CALA) Owner Engagement & Reporting content to:
 *   - Operator Setup - Commercial Fit & Terms (value cards + legacy JSON mirror)
 *   - Operator Setup - Engagement & Reporting (child table rows)
 *
 *   node scripts/apply-arbor-cala-engagement.mjs
 *   node scripts/apply-arbor-cala-engagement.mjs --dry-run
 *   node scripts/apply-arbor-cala-engagement.mjs [masterRecordId] [path-to-fixture.json]
 *
 * Default master: recF5Z87OAqFgndoq
 * Default fixture: fixtures/operator-engagement-explorer-arbor-cala.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import {
  NEW_BASE_COMMERCIAL_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorEngagementReportingRows } from "../api/lib/operator-setup-new-base-writer.js";
import { applyEngagementReportingToLegacyPrefill } from "../api/lib/operator-engagement-reporting-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recF5Z87OAqFgndoq";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-engagement-explorer-arbor-cala.json");

const JSON_COMMERCIAL_KEYS = [
  "ov_strategic_owner_value_json",
  "ov_engagement_cadence_json",
  "ov_controls_governance_json",
  "ov_reports_received_json",
  "ov_owner_tools_json",
  "ov_lifecycle_support_json",
];

function parseArgs(argv) {
  const flags = new Set(argv.slice(2).filter((a) => a.startsWith("--")));
  const pos = argv.slice(2).filter((a) => !a.startsWith("--"));
  return {
    masterId: (pos[0] || "").trim() || DEFAULT_MASTER,
    fixturePath: (pos[1] || "").trim() || DEFAULT_FIXTURE,
    dryRun: flags.has("--dry-run"),
  };
}

function enc(s) {
  return encodeURIComponent(s);
}

function buildCommercialPatch(commercialRaw, engagementReporting) {
  const patch = { ...(commercialRaw || {}) };
  const prefill = {};
  applyEngagementReportingToLegacyPrefill(prefill, engagementReporting);
  for (const key of JSON_COMMERCIAL_KEYS) {
    if (prefill[key] != null && prefill[key] !== "") patch[key] = prefill[key];
  }
  for (const [key, value] of Object.entries(commercialRaw || {})) {
    if (value == null || value === "") continue;
    patch[key] = value;
  }
  return patch;
}

async function patchCommercialRecord(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(NEW_BASE_COMMERCIAL_TABLE)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) {
    const msg = json?.error?.message || JSON.stringify(json);
    throw new Error(`Commercial PATCH failed (${status}): ${msg}`);
  }
  return json;
}

function countEngagement(er) {
  return {
    strategicOwnerValue: (er.strategicOwnerValue || []).length,
    engagementCadence: (er.engagementCadence || []).length,
    controlsGovernance: (er.controlsGovernance || []).length,
    reportsReceived: (er.reportsReceived || []).length,
    ownerTools: (er.ownerTools || []).length,
    lifecycleSupport: (er.lifecycleSupport || []).length,
    ownerValueCards: (er.ownerValueCards || []).length,
    optionalClusters: (er.optionalClusters || []).length,
    engagementSignals: (er.engagementSignals || []).length,
  };
}

async function main() {
  const { masterId, fixturePath, dryRun } = parseArgs(process.argv);
  if (!fs.existsSync(fixturePath)) {
    throw new Error(`Missing fixture: ${fixturePath}`);
  }
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const engagementReporting = fixture.engagementReporting;
  if (!engagementReporting || typeof engagementReporting !== "object") {
    throw new Error("Fixture missing engagementReporting object");
  }

  const commRows = await fetchRecordsLinkedToMaster(NEW_BASE_COMMERCIAL_TABLE, masterId);
  if (!commRows.length) {
    throw new Error(`No Commercial row linked to Master ${masterId}`);
  }
  const commId = commRows[0].id;
  const commPatch = buildCommercialPatch(fixture.commercialFields || {}, engagementReporting);
  if (!Object.keys(commPatch).length) {
    throw new Error("Fixture has no commercial fields to patch");
  }

  const publicFixture = path.join(ROOT, "public", "fixtures", "operator-engagement-explorer-arbor-cala.json");
  const summary = {
    masterId,
    fixturePath,
    operatorName: fixture._meta?.operatorName || null,
    commercialRecordId: commId,
    commercialFieldCount: Object.keys(commPatch).length,
    engagementRowCounts: countEngagement(engagementReporting),
    dryRun,
  };

  if (dryRun) {
    console.log(JSON.stringify({ ...summary, commercialPatchKeys: Object.keys(commPatch) }, null, 2));
    return;
  }

  await patchCommercialRecord(commId, commPatch);
  const erRes = await replaceOperatorEngagementReportingRows(
    masterId,
    { engagementReporting },
    randomUUID()
  );

  fs.mkdirSync(path.dirname(publicFixture), { recursive: true });
  fs.copyFileSync(fixturePath, publicFixture);

  console.log(
    JSON.stringify(
      {
        ...summary,
        publicFixture,
        airtable: erRes,
        sampleStrategicTitle: engagementReporting.strategicOwnerValue?.[0]?.title || null,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
