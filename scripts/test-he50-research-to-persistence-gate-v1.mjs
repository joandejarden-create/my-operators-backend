#!/usr/bin/env node
/**
 * HE Research-to-Persistence gate.
 *
 * Fails when a hotel that was successfully CREATEd in HE50 controlled apply
 * is still classified as CREATE READY / hpcId:null in the completeness matrix,
 * or when the standing allowlist omits the canonical HPC IDs.
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import {
  HE50_CANONICAL_HPC_BY_PILOT_KEY,
  HE50_HPC_TABLE_ID,
  HE50_CANONICAL_BASE_ID,
  HE50_HOTEL_CENSUS_TABLE_ID_RETIRED,
  HE50_TABS,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import { EIGHT_HOTEL_COMPLETENESS } from "../lib/hotel-intelligence/he-5-0-full-e2e/completeness-matrix-v1.js";
import { HE50_PILOT_HOTEL_ALLOWLIST } from "../lib/hotel-intelligence/he-5-0-full-e2e/standing-authorization-v1.js";
import { SEVEN_HOTEL_FREE_IDENTITY_PACKETS } from "../lib/hotel-intelligence/he-5-0-full-e2e/free-research-findings-v1.js";

const ROOT = process.cwd();
const APPLY = join(
  ROOT,
  "reports/hotel-explorer/he-5-0-pilot/HE50-CONTROLLED-APPLY-RESULT-V1.json",
);
const STANDING = join(
  ROOT,
  "reports/hotel-explorer/he-5-0-pilot/HE50-STANDING-AUTH-FOUNDER-REPORT-V1.md",
);

assert.equal(HE50_TABS.length, 12);
assert.equal(Object.keys(HE50_CANONICAL_HPC_BY_PILOT_KEY).length, 8);
assert.equal(EIGHT_HOTEL_COMPLETENESS.length, 8);

assert.ok(existsSync(APPLY), "controlled apply result required");
assert.ok(existsSync(STANDING), "standing auth founder report required");

const apply = JSON.parse(readFileSync(APPLY, "utf8"));
const created = [];
for (const op of apply.operations || apply.results || apply.mutations || []) {
  const id = op.recordId || op.result?.recordId || op.airtableRecordId;
  const action = String(op.action || op.class || op.classification || "");
  if (id && /CREATE|create/i.test(action + JSON.stringify(op))) {
    created.push(id);
  }
}
// Fallback: parse known CREATE IDs from standing report text
const standingMd = readFileSync(STANDING, "utf8");
for (const id of Object.values(HE50_CANONICAL_HPC_BY_PILOT_KEY)) {
  assert.match(standingMd, new RegExp(id), `standing report must include ${id}`);
}

for (const [pilotKey, hpcId] of Object.entries(HE50_CANONICAL_HPC_BY_PILOT_KEY)) {
  const row = EIGHT_HOTEL_COMPLETENESS.find((h) => h.pilotKey === pilotKey);
  assert.ok(row, `completeness row missing for ${pilotKey}`);
  assert.equal(
    row.hpcId,
    hpcId,
    `${pilotKey} must not be null/CREATE READY — canonical HPC is ${hpcId}`,
  );
  assert.doesNotMatch(
    String(row.readinessNote || ""),
    /CREATE READY/i,
    `${pilotKey} readinessNote must not say CREATE READY after persistence`,
  );
  assert.ok(
    HE50_PILOT_HOTEL_ALLOWLIST.includes(hpcId),
    `standing allowlist must include HPC ${hpcId}`,
  );
}

// Never route to legacy Hotel Census table as runtime SoT
const matrixSrc = readFileSync(
  join(ROOT, "lib/hotel-intelligence/he-5-0-full-e2e/completeness-matrix-v1.js"),
  "utf8",
);
assert.doesNotMatch(matrixSrc, /tblgj2qEwxjTcg6q0/);
assert.equal(HE50_HPC_TABLE_ID, "tbl9aY5ijiuIzzWam");
assert.equal(HE50_CANONICAL_BASE_ID, "appCCUsuGsE1ifoLk");
assert.equal(HE50_HOTEL_CENSUS_TABLE_ID_RETIRED, "tblgj2qEwxjTcg6q0");

// Radar / founder report must not be the only identity source — gate requires matrix sync
const nullIds = EIGHT_HOTEL_COMPLETENESS.filter((h) => !h.hpcId);
assert.equal(
  nullIds.length,
  0,
  `persisted HPC hotels still null in matrix: ${nullIds.map((h) => h.pilotKey).join(",")}`,
);

for (const p of SEVEN_HOTEL_FREE_IDENTITY_PACKETS) {
  const expected = HE50_CANONICAL_HPC_BY_PILOT_KEY[p.pilotKey];
  if (!expected) continue;
  assert.equal(
    p.hpcRecordId,
    expected,
    `free-research packet ${p.pilotKey} must carry canonical HPC ${expected}`,
  );
  assert.notEqual(
    p.identityOutcome,
    "NEW_HPC_CANDIDATE",
    `${p.pilotKey} must not remain NEW_HPC_CANDIDATE after CREATE success`,
  );
  assert.doesNotMatch(
    String(p.proposedHpcAction || ""),
    /CREATE_AFTER_FOUNDER_APPROVAL/i,
    `${p.pilotKey} must not propose CREATE after persistence`,
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:he50-research-to-persistence-gate-v1",
      tabCount: HE50_TABS.length,
      hotels: EIGHT_HOTEL_COMPLETENESS.map((h) => ({
        pilotKey: h.pilotKey,
        hpcId: h.hpcId,
      })),
      certifications: {
        EIGHT_HPC_IDENTITIES_MATRIX_SYNCED: true,
        NO_STALE_CREATE_READY: true,
        FREE_RESEARCH_PACKETS_SYNCED: true,
        LEGACY_CENSUS_NOT_RUNTIME_SOT: true,
      },
    },
    null,
    2,
  ),
);
