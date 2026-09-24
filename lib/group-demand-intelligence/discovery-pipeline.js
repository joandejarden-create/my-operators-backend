/**
 * Production discovery pipeline: Native blind → completeness gate → Parallel gaps.
 * Outputs seedCandidates compatible with runGroupDemandResearch / buildOpportunity.
 */

import { runNativeBlindDiscovery } from "./native-blind-discovery.js";
import {
  evaluateNativeDiscoveryCompleteness,
  NATIVE_DISCOVERY_DECISION,
} from "./discovery-completeness-gate.js";
import { runParallelDiscoveryEscalation } from "./parallel-discovery-escalate.js";
import { dedupeDiscoveryCandidates } from "./candidate-dedupe.js";
import { mapWebhoundOpportunityUniverse } from "./webhound-opportunity-import.js";
import { recordProviderSpend } from "./cost-ledger.js";

export const GDI_DISCOVERY_PIPELINE_VERSION = "gdi_discovery_pipeline_v1";

/**
 * Run blind discovery and return orchestrator-ready seedCandidates.
 */
export async function runGdiBlindDiscoveryPipeline({
  hotelId,
  profile = null,
  config = null,
  dryRun = false,
  skipParallel = false,
  nativeOpts = {},
  ledger = null,
} = {}) {
  const native = await runNativeBlindDiscovery({
    hotelId,
    profile,
    config,
    dryRun,
    ...nativeOpts,
  });

  if (dryRun) {
    return {
      version: GDI_DISCOVERY_PIPELINE_VERSION,
      hotelId,
      native,
      gate: null,
      parallel: { skipped: true, reason: "dry_run" },
      seedCandidates: [],
      discoveryMeta: { mode: "DRY_RUN" },
    };
  }

  const dedupedNative = dedupeDiscoveryCandidates(native.rows || []);
  const gate = evaluateNativeDiscoveryCompleteness({
    candidates: dedupedNative.candidates,
    contract: native.contract,
  });

  let parallel = { skipped: true, reason: "native_sufficient", rows: [] };
  if (
    !skipParallel &&
    gate.decision === NATIVE_DISCOVERY_DECISION.ESCALATE_PARALLEL
  ) {
    parallel = await runParallelDiscoveryEscalation({
      contract: native.contract,
      gate,
      existingCandidates: dedupedNative.candidates,
      hotelId,
    });
  }

  const mergedRows = [
    ...dedupedNative.candidates,
    ...(parallel.rows || []).map((r) => ({
      ...r,
      researchProvider: r.researchProvider || "parallel",
    })),
  ];
  const dedupedAll = dedupeDiscoveryCandidates(mergedRows);

  // Reuse Webhound row→candidate mapper (field-compatible)
  const mapped = mapWebhoundOpportunityUniverse(dedupedAll.candidates, hotelId);
  // Fix discoverySource labels for native/parallel rows
  const seedCandidates = mapped.candidates.map((c, i) => {
    const src = dedupedAll.candidates[i] || {};
    const provider = src.researchProvider || c.researchProvider || "native";
    return {
      ...c,
      researchProvider: provider,
      discoverySource:
        provider === "parallel"
          ? "gdi_parallel_escalation_v1"
          : "gdi_native_blind_discovery_v1",
    };
  });

  if (ledger) {
    const serpUsd = Number(native.ledger?.serpCharged || 0) * 0.01; // rough ledger placeholder
    if (serpUsd > 0) {
      recordProviderSpend(ledger, "serpapi", serpUsd, {
        note: "native_blind_discovery_serp_estimate",
        queries: native.ledger?.serpQueries,
      });
    }
    if (parallel.costUsd > 0) {
      recordProviderSpend(ledger, "parallel", parallel.costUsd, {
        note: "gdi_parallel_escalation",
        runId: parallel.runId || null,
      });
    }
  }

  return {
    version: GDI_DISCOVERY_PIPELINE_VERSION,
    hotelId,
    native: {
      ...native,
      rows: dedupedNative.candidates,
      duplicatesRemoved: dedupedNative.duplicatesRemoved,
    },
    gate,
    parallel,
    seedCandidates,
    rejected: mapped.rejected,
    discoveryMeta: {
      mode: "NATIVE_BLIND",
      nativeCandidateCount: dedupedNative.candidates.length,
      parallelCandidateCount: (parallel.rows || []).length,
      mergedCandidateCount: seedCandidates.length,
      duplicatesRemoved: dedupedAll.duplicatesRemoved,
      gateDecision: gate.decision,
      escalateGaps: gate.escalateGaps,
      webhoundUsed: false,
    },
  };
}
