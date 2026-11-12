#!/usr/bin/env node
/**
 * Bethesda SC integrity restore — DRY RUN ONLY.
 * Loads verified 59-row backup bodies for the five missing gdi_sc_* IDs
 * and writes an approval manifest. Does NOT write opportunities.json.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  VERIFIED_59_ROW_SHA256,
  dryRunMergeProtectedBodies,
  loadHotelOpportunityDoc,
  sha256File,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-bethesda-sc-integrity-repair-v1");
const DEFAULT_SOURCE =
  "C:/Dev/dealality/authorized-backups/primary-working-state-e2ae6c7d3-20261111/files/data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json";

const sourcePath = process.env.BETHESDA_VERIFIED_59_SOURCE || DEFAULT_SOURCE;
if (!fs.existsSync(sourcePath)) {
  console.error(JSON.stringify({ ok: false, error: "SOURCE_ABSENT", sourcePath }, null, 2));
  process.exit(2);
}
const sourceSha = sha256File(sourcePath);
if (sourceSha !== VERIFIED_59_ROW_SHA256) {
  console.error(
    JSON.stringify(
      { ok: false, error: "SOURCE_SHA_MISMATCH", sourceSha, expected: VERIFIED_59_ROW_SHA256 },
      null,
      2
    )
  );
  process.exit(2);
}

const sourceDoc = JSON.parse(fs.readFileSync(sourcePath, "utf8"));
const sourceRows = sourceDoc.opportunities || [];
const bodies = BETHESDA_PROTECTED_SCALING_IDS.map((id) => {
  const row = sourceRows.find((r) => (r.id || r.opportunityId) === id);
  if (!row) throw new Error(`Missing body in verified source: ${id}`);
  return row;
});

const dry = dryRunMergeProtectedBodies({
  hotelId: BETHESDA_HOTEL_ID,
  recoveredBodies: bodies,
  protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  root: ROOT,
});

fs.mkdirSync(OUT, { recursive: true });
const bodiesDir = path.join(OUT, "01-recovered-bodies");
fs.mkdirSync(bodiesDir, { recursive: true });
for (const body of bodies) {
  fs.writeFileSync(
    path.join(bodiesDir, `${body.id}.json`),
    JSON.stringify(body, null, 2)
  );
}

const current = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root: ROOT });
const summary = {
  version: "gdi-bethesda-sc-integrity-restore-dry-run-v1",
  status: "AWAITING_FOUNDER_APPROVAL_BEFORE_AUTHORITATIVE_WRITE",
  writeAuthorized: false,
  hotelId: BETHESDA_HOTEL_ID,
  sourcePath,
  sourceSha256: sourceSha,
  currentSha256: current.sha256,
  currentCount: current.count,
  dryRun: {
    ok: dry.ok,
    code: dry.code,
    proposedCount: dry.proposedCount,
    added: dry.added,
    missingStill: dry.missingStill,
  },
  protectedIds: BETHESDA_PROTECTED_SCALING_IDS,
  recommendation:
    "APPROVE merge-add of five recovered gdi_sc_* bodies into current store (preserve current 54 bodies). Do NOT byte-replace with fd438549 unless founder explicitly accepts discarding post-backup mutations on shared IDs.",
};

fs.writeFileSync(
  path.join(OUT, "05-restore-dry-run-summary.json"),
  JSON.stringify(summary, null, 2)
);

console.log(JSON.stringify(summary, null, 2));
if (!dry.ok) process.exitCode = 2;
