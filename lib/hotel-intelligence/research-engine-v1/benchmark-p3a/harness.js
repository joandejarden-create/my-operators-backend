/**
 * Provider-neutral P3A benchmark harness (dry-run / mock only).
 * Independent methods cannot consume one another's newly discovered results.
 */

import {
  BENCHMARK_ARM,
  BENCHMARK_CONTRACT_ID,
  BENCHMARK_CONTRACT_VERSION,
  BENCHMARK_MODE,
  BENCHMARK_PHASE,
  CLAIM_ACCEPTANCE,
  EVIDENCE_LAYER,
  P3A_POLICY,
  writeGuaranteesP3A,
  assertP3APaidBudgetZero,
} from "./contract.js";
import {
  loadP3ASixHotelCohort,
  buildIndependentDiscoverySeed,
  buildIncrementalProductionSeed,
} from "./cohort.js";
import { buildProviderCapabilityMatrix } from "./provider-capability-matrix.js";
import { buildClaimEnvelope, rejectInferredContact } from "./claim-schema.js";
import { scoreClaims, computeCostEfficiency } from "./quality-rubric.js";
import { blindClaimPackets, adjudicateBlindPackets, revealAdjudication } from "./adjudication.js";
import { createBenchmarkCostLedger } from "./cost-ledger.js";
import {
  assertBenchmarkArmAuthorized,
  createP3AProviderGateway,
  proposedMinimumPaidBenchmarkBudget,
} from "./authorization-gates.js";
import { createBenchmarkStagingStore } from "./staging-store.js";
import { createMockProviderAdapter } from "../provider-interface.js";

const DEFAULT_ARMS = [
  BENCHMARK_ARM.MOCK,
  BENCHMARK_ARM.DEALALITY_NATIVE,
  BENCHMARK_ARM.EXA,
  BENCHMARK_ARM.PARALLEL_SEARCH,
  BENCHMARK_ARM.PARALLEL_TASK,
  BENCHMARK_ARM.SERPAPI,
  BENCHMARK_ARM.HYBRID_WATERFALL,
];

/**
 * Isolated discovery bags — no cross-arm reads within the same run.
 */
export function createIsolatedDiscoveryBags(armIds) {
  const bags = new Map();
  for (const id of armIds) bags.set(id, { discovered: [], sealed: false });
  return {
    append(armId, claim) {
      const b = bags.get(armId);
      if (!b) throw new Error("unknown_arm_bag");
      if (b.sealed) throw new Error("bag_sealed");
      b.discovered.push(claim);
    },
    readOwn(armId) {
      return [...(bags.get(armId)?.discovered || [])];
    },
    /** Attempting to read another arm's discoveries must fail. */
    readOther(armId, otherArmId) {
      if (armId === otherArmId) return this.readOwn(armId);
      return {
        ok: false,
        error: "CROSS_ARM_LEAKAGE_FORBIDDEN",
        armId,
        otherArmId,
      };
    },
    sealAll() {
      for (const b of bags.values()) b.sealed = true;
    },
    snapshot() {
      const out = {};
      for (const [k, v] of bags) out[k] = { count: v.discovered.length };
      return out;
    },
  };
}

function mockDiscover(seed, armId) {
  // Deterministic offline facts for harness regression — NOT verified research.
  const claims = [
    buildClaimEnvelope({
      claimType: "PROPERTY_IDENTITY",
      value: seed.propertyName,
      supportingSource: {
        url: seed.officialPropertyUrl || "https://example.invalid/property",
        publisherClass: "OFFICIAL_OR_FIXTURE",
        publishedAt: null,
        excerpt: "fixture identity",
      },
      independentVerification: { ok: true, method: "MANIFEST_HPC_ID", note: "seed" },
      acceptanceDecision: CLAIM_ACCEPTANCE.ACCEPTED,
      graphMapping: { stagedOnly: true, productionWriteForbidden: true },
    }),
  ];
  if (armId === BENCHMARK_ARM.MOCK) {
    // Demonstrate UNKNOWN without evidence
    claims.push(
      buildClaimEnvelope({
        claimType: "OWNER_ENTITY",
        value: "UNSPECIFIED_OWNER",
        supportingSource: { url: null },
        acceptanceDecision: CLAIM_ACCEPTANCE.UNKNOWN,
      })
    );
  }
  return claims;
}

/**
 * Run P3A dry-run benchmark foundation.
 */
export async function runP3ABenchmarkDryRun(opts = {}) {
  const paidGate = assertP3APaidBudgetZero(opts);
  if (!paidGate.ok) return { ok: false, ...paidGate };

  const cohort = loadP3ASixHotelCohort(opts);
  if (!cohort.ok) return { ok: false, error: "COHORT_INVALID", cohort };

  const arms = opts.arms || DEFAULT_ARMS;
  const modes = opts.modes || [
    BENCHMARK_MODE.INDEPENDENT_DISCOVERY,
    BENCHMARK_MODE.INCREMENTAL_PRODUCTION,
  ];
  const priorByHotel = opts.priorSnapshots || {};
  const ledger = createBenchmarkCostLedger({
    runUsdCap: 0,
    perHotelUsdCap: 0,
    allowPaid: false,
  });
  const gateway = createP3AProviderGateway({
    adapters: { MOCK: createMockProviderAdapter({ costUsd: 0 }) },
  });
  const store = createBenchmarkStagingStore({
    runId: opts.runId,
    memoryOnly: opts.memoryOnly !== false,
    root: opts.artifactRoot,
  });
  const capability = buildProviderCapabilityMatrix({ checkEnv: opts.checkEnv === true });

  const armResults = [];
  const leakageViolations = [];
  const authorization = [];

  for (const mode of modes) {
    const bags = createIsolatedDiscoveryBags(arms);

    for (const hotel of cohort.hotels) {
      const seed =
        mode === BENCHMARK_MODE.INCREMENTAL_PRODUCTION
          ? buildIncrementalProductionSeed(hotel, priorByHotel[hotel.hpcId])
          : buildIndependentDiscoverySeed(hotel);

      for (const armId of arms) {
        const auth = assertBenchmarkArmAuthorized(armId, {
          paidBudgetUsd: 0,
          paidNetworkCall: false,
        });
        authorization.push({ armId, mode, hpcId: hotel.hpcId, ...auth });

        const paidArm = armId !== BENCHMARK_ARM.MOCK && armId !== BENCHMARK_ARM.DEALALITY_NATIVE;
        if (paidArm) {
          // Feasibility-only: refuse live paid call; still record adapter readiness
          ledger.recordUsage({
            provider: armId,
            operation: "feasibility_probe",
            hotelId: hotel.hpcId,
            armId,
            mode,
            costUsd: 0,
            estimatedUsd: null,
            estimatedUsdStatus: "UNKNOWN",
          });
          armResults.push({
            armId,
            hpcId: hotel.hpcId,
            mode,
            status: "REFUSED_PAID_P3A",
            claims: [],
            auth,
          });
          continue;
        }

        // Native/mock offline path only — must go through P1 execution gateway
        const gw = await gateway.call({
          provider: "MOCK",
          operation: "benchmark_dry_discover",
          hotelId: hotel.hpcId,
          runId: store.runId,
        });

        const claims = mockDiscover(seed, armId).map(rejectInferredContact);
        for (const c of claims) bags.append(armId, c);

        // Leakage audit: each other arm must not be readable
        for (const other of arms) {
          if (other === armId) continue;
          const leak = bags.readOther(armId, other);
          if (leak?.ok !== false) {
            leakageViolations.push({ armId, other, mode, hpcId: hotel.hpcId });
          }
        }

        ledger.recordUsage({
          provider: armId,
          operation: "mock_discover",
          hotelId: hotel.hpcId,
          armId,
          mode,
          costUsd: 0,
          searchRequests: 0,
          credits: 0,
        });

        const score = scoreClaims(claims);
        const efficiency = computeCostEfficiency({
          costUsd: 0,
          costUsdStatus: "ZERO_AUTHORIZED",
          verifiedUsefulFacts: score.acceptedCount,
          verifiedDecisionMakers: claims.filter(
            (c) =>
              c[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.claimType === "DECISION_MAKER" &&
              c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] === CLAIM_ACCEPTANCE.ACCEPTED
          ).length,
          hotelCount: 1,
        });

        armResults.push({
          armId,
          hpcId: hotel.hpcId,
          mode,
          status: "DRY_RUN_OK",
          seed,
          claims,
          score,
          efficiency,
          gatewayResultOk: gw?.ok !== false,
          auth,
        });
      }
    }
    bags.sealAll();
  }

  const executable = armResults.filter((r) => r.status === "DRY_RUN_OK");
  const blinded = blindClaimPackets(executable, { includeRevealMap: false });
  const adjudicated = adjudicateBlindPackets(blinded.packets);
  const revealed = revealAdjudication(adjudicated, blinded._revealMapInternal);

  const graphWriteGate = store.assertNoProductionGraphWrite({
    productionAirtableWrite: opts.productionAirtableWrite === true,
    canonicalGraphWrite: false,
  });

  const report = {
    ok:
      cohort.ok &&
      leakageViolations.length === 0 &&
      graphWriteGate.ok &&
      paidGate.ok,
    phase: BENCHMARK_PHASE,
    contract: BENCHMARK_CONTRACT_ID,
    contract_version: BENCHMARK_CONTRACT_VERSION,
    guarantees: writeGuaranteesP3A(),
    policy: P3A_POLICY,
    runId: store.runId,
    cohort: {
      cohortId: cohort.cohortId,
      hotels: cohort.hotels.map((h) => ({
        order: h.order,
        hpcId: h.hpcId,
        name: h.name,
        dualHouse: h.dualHouse,
      })),
      fullPilotDistinctHotels: cohort.fullPilotDistinctHotels,
      fullPilotFounderExperiences: cohort.fullPilotFounderExperiences,
    },
    capabilityMatrix: capability,
    authorizationSummary: {
      refusedPaidArms: authorization.filter((a) => !a.ok).length,
      allowedOfflineArms: authorization.filter((a) => a.ok).length,
    },
    leakageViolations,
    armResultsCount: armResults.length,
    dryRunExecuted: executable.length,
    adjudication: {
      blindedPacketCount: blinded.packets.length,
      rows: revealed.rows,
    },
    costLedger: ledger.snapshot(),
    sharedBudgetBypassPossible: ledger.canParallelBypassSharedBudget(),
    proposedP3BBudget: proposedMinimumPaidBenchmarkBudget(),
    staging: {
      ...store.policy,
      writes: store.listWrites(),
      graphWriteGate,
    },
    p3bExecutionPlan: buildP3BExecutionPlan(),
  };

  store.writeJson("manifest.json", {
    runId: store.runId,
    phase: BENCHMARK_PHASE,
    contract: BENCHMARK_CONTRACT_ID,
    paid_budget_usd: 0,
  });
  store.writeJson("scoreboard.json", report.adjudication);
  store.writeJson("cost-ledger.json", report.costLedger);
  store.writeJson("capability-matrix.json", capability);

  return report;
}

export function buildP3BExecutionPlan() {
  return {
    phase: "P3B",
    status: "PROPOSED_NOT_AUTHORIZED",
    steps: [
      "Founder authorize M1 envelope (hard cap $5 USD) and confirm SerpAPI/native USD rates or keep UNKNOWN+count caps",
      "Enable paid providers only inside execution gateway with shared budget ledger",
      "Run Mode A then Mode B on 3-hotel mini-pilot subset (spread: Bethesda, Wyndham or Renaissance, Ritz CDMX)",
      "Blind adjudication + scoreboard; no production HI graph writes",
      "Review cost/verified-fact; amend M2 ($40) only after M1 review",
      "Surfe/PDL remain forbidden",
    ],
    stopConditions: [
      "Shared or per-hotel USD cap hit",
      "Unknown pricing under enforced USD cap",
      "Cross-arm leakage detected",
      "Attempted Surfe/PDL/inferred-email promotion",
      "Production Airtable canonical write attempted",
    ],
  };
}
