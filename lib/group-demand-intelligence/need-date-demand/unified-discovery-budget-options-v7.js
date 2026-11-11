/**
 * Three live campaign options + budget reconciliation for Unified Discovery V7.
 * Planning only — no live spend.
 */

import {
  V7_BUDGET_RECONCILIATION,
  V7_PROVIDER_STRATEGY,
  WATERSTONE_V7_HOTEL,
} from "./unified-discovery-engine-v7-contract.js";
import { buildWaterstoneApifyBenchmarkPlanV1 } from "../research/apify/apify-waterstone-benchmark-plan-v1.js";

export function buildUnifiedDiscoveryBudgetOptionsV7() {
  const apifyPlan = buildWaterstoneApifyBenchmarkPlanV1();
  const recon = V7_BUDGET_RECONCILIATION;

  const optionA = {
    id: "OPTION_A_DISCOVERY_VALIDATION",
    label: "Discovery Validation",
    purpose: "Prove Apify adds incremental candidates vs saved Exa baseline at modest cost",
    providerStrategy: V7_PROVIDER_STRATEGY.APIFY_PLUS_EXA,
    tracks: {
      A: "Reuse prior/saved Exa artifacts as baseline (no new Exa-only track spend)",
      B: "Lean Apify discovery + light Exa/ORS qualify on net-new only",
    },
    workflowCoverage: "PRIMARY_APIFY workflows (#7,#8,#13,#14) + wedding mandatory",
    actorRuns: [
      "solidcode/eventbrite-scraper (max 40, Aug–Oct 2027)",
      "compass/crawler-google-places (max 30, planners/venues)",
      "dionysus_way/the-knot-marketplace-scraper---wedding-vendor-leads (max 25)",
      "filip_cicvarek/meetup-scraper (max 25)",
    ],
    exaDiscoveryCalls: "0–2 bounded (only if baseline gap)",
    exaDeepResearchCalls: "4–6 on Apify net-new only",
    adaptiveFollowUpReserveUsd: 3,
    estimatedUsd: {
      apify: 0.98,
      exa: 12,
      aggregate: 15.98,
      maximumCeiling: 18,
    },
    commercialQualification: "Same V6 accessibility + buyer gates; no standard relaxation",
    stopConditions: [
      "Apify or Exa budget hard-stop",
      "Zero net-new after Wave B lean → stop before Deep Research",
      "Publication always blocked",
    ],
    reusePriorArtifacts: true,
    note: "Does NOT prove Apify incremental qualified yield vs live Exa-only control",
  };

  const optionB = {
    id: "OPTION_B_COMMERCIAL_DEPTH_BENCHMARK",
    label: "Commercial Depth Benchmark (recommended for actionable yield)",
    purpose: "Apify + Exa with substantial ORS research, buyer qualification, source verification",
    providerStrategy: V7_PROVIDER_STRATEGY.ADAPTIVE_MULTI_PROVIDER,
    tracks: {
      A: "Not run (single-track depth focus)",
      B: "Full Apify five-family discovery + Exa/ORS at V6 COMMERCIAL_DEPTH parity",
    },
    workflowCoverage: "All 14 evaluated; PRIMARY + SUPPLEMENTAL pursued; EXA_PRIMARY via Exa",
    actorRuns: apifyPlan.tracks.B.proposedRuns.map(
      (r) => `${r.actorId} (max ${r.maxItems}, ${r.track})`,
    ),
    exaDiscoveryCalls: "6–10 adaptive",
    exaDeepResearchCalls: "10–14 + Wave E blocker reserve",
    adaptiveFollowUpReserveUsd: 6,
    estimatedUsd: {
      apify: recon.apifyV1.apifyPortionFullUsd,
      exa: 34,
      aggregate: 36,
      maximumCeiling: 40,
      alignsWith: "V6 COMMERCIAL_DEPTH $36 — NOT additive to Apify V1 $34",
    },
    commercialQualification: "Full Wave C–F; wedding mandatory; relationship vs dated tracks separate",
    stopConditions: [
      "Aggregate $40 hard stop",
      "Per-provider isolation: Apify ceiling separate from Exa ceiling",
      "No identical query retries",
      "AES/WIO/URA not re-burned without new evidence",
    ],
    reusePriorArtifacts: true,
    note: "~$34 Apify V1 FULL and ~$36 V6 COMMERCIAL_DEPTH are the SAME planning envelope for Track B depth — reconcile to $36 ceiling",
  };

  const optionC = {
    id: "OPTION_C_FULL_COMPARATIVE_BENCHMARK",
    label: "Full Comparative Benchmark",
    purpose: "Measure incremental qualified yield attributable to Apify vs Exa-only",
    providerStrategy: V7_PROVIDER_STRATEGY.APIFY_PLUS_EXA,
    tracks: {
      A: "Live EXA_ONLY control at comparable investigation depth (~$28)",
      B: "APIFY_PLUS_EXA at same ORS/Deep Research depth (~$34–36)",
    },
    workflowCoverage: "Identical objectives across tracks; separate cost ledgers",
    actorRuns: "Track B only — same as OPTION B actor set",
    exaDiscoveryCalls: "Track A: 8–12 · Track B: 6–10 (after Apify)",
    exaDeepResearchCalls: "Parity target: similar Deep Research count per promising candidate",
    adaptiveFollowUpReserveUsd: 8,
    estimatedUsd: {
      trackA_exaOnly: recon.apifyV1.exaOnlyControlUsd,
      trackB_apifyPlusExa: 36,
      aggregateBothTracks: 64,
      maximumCeiling: 70,
      researchDepthParityRequired: true,
    },
    commercialQualification: "Identical gates; report Apify-only / Exa-only / both intersections",
    stopConditions: [
      "$70 combined hard stop",
      "Halt a track if depth parity cannot be maintained within budget",
      "Do not declare winner on raw row count",
    ],
    reusePriorArtifacts: true,
    note: "$28 Exa-only + ~$36 Track B ≈ $64 — this is ADDITIVE because two live tracks run. Do not confuse with single-track $34/$36.",
  };

  return {
    version: "gdi-unified-discovery-budget-options-v7",
    hotel: WATERSTONE_V7_HOTEL,
    reconciliation: recon,
    options: [optionA, optionB, optionC],
    recommendedForActionableYield: optionB.id,
    recommendedForApifyAttributionProof: optionC.id,
    recommendedForCheapValidation: optionA.id,
  };
}
