#!/usr/bin/env node
/**
 * CLI — hotel-wide finalization stage ($0 by default).
 * Automatically prepares/certifies a Qualified candidate package when eligible
 * (use --no-write-candidate-package only for isolated diagnostics).
 * Never activates STAGED/CURRENT pointers.
 *
 *   node scripts/gdi-hotel-wide-finalization-stage-v1.mjs --hotel rec35fExUxCClpOP6
 *   node scripts/gdi-hotel-wide-finalization-stage-v1.mjs --hotel rec35fExUxCClpOP6 --no-write-candidate-package
 */
import { runHotelWideFinalizationStage } from "../lib/group-demand-intelligence/workflow/gdi-hotel-wide-finalization-stage-v1.js";

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1];
  return fallback;
}

const hotelId = arg("--hotel") || arg("--hotel-id");
if (!hotelId) {
  console.error(
    "Usage: --hotel <hotelId> [--no-write-candidate-package] [--run-id <id>]",
  );
  process.exit(2);
}

const result = await runHotelWideFinalizationStage({
  hotelId,
  hotelName: arg("--hotel-name"),
  runId: arg("--run-id"),
  // Production default: write candidate. Opt out only for isolated diagnostics.
  writeCandidatePackage: !process.argv.includes("--no-write-candidate-package"),
  agentAuthorized: false,
  paidSpendUsd: 0,
});

console.log(
  JSON.stringify(
    {
      ok: result.ok,
      hotelId: result.hotelId,
      runId: result.runId,
      completionOutcome: result.completionOutcome,
      hotelInventoryReconciled: result.hotelInventoryReconciled,
      researchGapsRemain: result.researchGapsRemain,
      qualifiedCandidateCertified: result.qualifiedCandidateCertified,
      dispositions: result.manifest?.dispositions,
      stagedCandidate: result.manifest?.stagedCandidate,
      pointersUnchanged: result.pointersUnchanged,
      manifestPath: result.manifestPath,
      blockers: result.manifest?.blockers || [],
    },
    null,
    2,
  ),
);
process.exit(result.ok ? 0 : 2);
