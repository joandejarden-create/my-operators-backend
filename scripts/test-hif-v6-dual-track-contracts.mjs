#!/usr/bin/env node
/**
 * Gate: HIF V6 dual-track shared contract + source inventories (design-time).
 */
import {
  assertSharedContractIntegrity,
  buildV6ExecutionGate,
  COMMERCIAL_SIGNAL_CLASS,
  DISCOVERY_TRACK,
  PARALLEL_TRACK_ORCHESTRATION_V6,
  SHARED_IDENTITY_KIND,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-shared-research-contract.js";
import { trackAInventorySummary } from "../lib/hotel-intelligence/sec-edgar/hif-v6-track-a-source-inventory.js";
import { trackBInventorySummary } from "../lib/hotel-intelligence/sec-edgar/hif-v6-track-b-source-inventory.js";

const errors = [];

const gateBlocked = buildV6ExecutionGate({ merged: false });
if (gateBlocked.v6WaveExecutionAuthorized) errors.push("BLOCKED_GATE_MUST_DENY_WAVES");
if (!gateBlocked.designAndInventoryAllowedWhileBlocked) errors.push("DESIGN_MUST_BE_ALLOWED");

const gateMerged = buildV6ExecutionGate({ merged: true, mergeSha: "deadbeef" });
if (!gateMerged.v6WaveExecutionAuthorized) errors.push("MERGED_GATE_MUST_AUTHORIZE");

const integrity = assertSharedContractIntegrity(gateBlocked);
if (!integrity.ok) errors.push(...integrity.errors);

for (const k of [
  "VERIFIED_CONTRACTUAL_OPPORTUNITY",
  "VERIFIED_COMMERCIAL_SIGNAL",
  "RESEARCH_CANDIDATE",
  "HISTORICAL_CLOSED_OR_SUPERSEDED",
  "HOLD",
]) {
  if (!COMMERCIAL_SIGNAL_CLASS[k]) errors.push(`MISSING_SIGNAL_CLASS_${k}`);
}

if (!DISCOVERY_TRACK.TRACK_A_COMMERCIAL_CONTRACT_PROPERTY) errors.push("MISSING_TRACK_A");
if (!DISCOVERY_TRACK.TRACK_B_GOVERNMENT_REGULATORY_PUBLIC) errors.push("MISSING_TRACK_B");
if (Object.keys(SHARED_IDENTITY_KIND).length < 10) errors.push("IDENTITY_KINDS_TOO_FEW");

if (PARALLEL_TRACK_ORCHESTRATION_V6.separateUnregisteredWorktrees !== false) {
  errors.push("MUST_NOT_CREATE_UNREGISTERED_WORKTREES");
}

const trackA = trackAInventorySummary();
const trackB = trackBInventorySummary();
if (trackA.familyCount < 10) errors.push("TRACK_A_INVENTORY_TOO_THIN");
if (trackB.sourceCount < 15) errors.push("TRACK_B_INVENTORY_TOO_THIN");
if (trackA.waveExecutionAuthorized || trackB.waveExecutionAuthorized) {
  errors.push("INVENTORIES_MUST_NOT_AUTHORIZE_WAVES_PRE_MERGE");
}
if (!trackB.initialWaveIds.includes("BR_CVM_OPEN_DATA")) errors.push("MISSING_CVM_WAVE");
if (!trackB.initialWaveIds.includes("JP_EDINET")) errors.push("MISSING_EDINET_WAVE");
if (!trackB.initialWaveIds.includes("UK_COMPANIES_HOUSE")) errors.push("MISSING_CH_WAVE");

if (errors.length) {
  console.error("FAIL", errors);
  process.exit(1);
}
console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "AWAITING_PR_503_MERGE",
      trackAFamilies: trackA.familyCount,
      trackBSources: trackB.sourceCount,
      trackBInitialWave: trackB.initialWaveIds,
    },
    null,
    2
  )
);
