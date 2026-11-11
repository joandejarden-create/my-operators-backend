/**
 * Full E2E stage: opportunity research maturation.
 *
 * Insert after paid_multisegment / candidate normalization and before
 * assessed publication. Researcher adapter must route to existing providers
 * and return evidence patches — never publish.
 */

import fs from "node:fs";
import path from "node:path";
import {
  matureOpportunityQueue,
  evaluateOpportunity,
} from "./opportunity-research-supervisor-v1.js";
import { HeuristicResearchPlanner } from "./research-planner-v1.js";
import {
  segmentCoverageGate,
  hotelCompletionGate,
} from "./segment-coverage-controller-v1.js";
import {
  SUPERVISOR_EXPECTED_SEGMENTS,
  SUPERVISOR_VERSION,
  canonicalizeSegmentId,
  DEFAULT_MINIMUM_DEPTH,
} from "./research-constants-v1.js";
import { createMaturationResearcherRouter } from "./maturation-researcher-router-v1.js";
import { createDirectPublicExecutor } from "./direct-public-executor-v1.js";
import { createParallelMaturationExecutor } from "./parallel-maturation-executor-v1.js";

/**
 * Default researcher: memory-only / no paid calls.
 * Real Full E2E injects Parallel/contact adapters via options.researcher.
 */
export function createNoopPublicResearcher() {
  return {
    async execute({ action }) {
      return {
        newMaterialEvidence: false,
        provider: "noop_public",
        sourceFamilies: ["none"],
        costUsd: 0,
        disposition: "NO_OP",
        notes: `No-op researcher skipped strategy ${action?.strategy} — inject live adapter for paid/public execution`,
      };
    },
  };
}

function envFlag(name) {
  return String(process.env[name] || "").trim() === "1";
}

/**
 * Resolve live vs noop researcher.
 *
 * - Default: noop (safe for dry Full E2E).
 * - GDI_MATURATION_JEV_ROUTER=1 / useJevRouter → Jev route + executors when live.
 * - GDI_MATURATION_LIVE=1 / useLiveResearchers → inject real direct_public (+ Parallel unless paidBlocked).
 * Jev remains routing-only — never completion authority.
 */
export function resolveMaturationResearcher(options = {}) {
  if (options.researcher) return options.researcher;
  const useLive =
    options.useLiveResearchers === true || envFlag("GDI_MATURATION_LIVE");
  const useJev =
    options.useJevRouter === true || envFlag("GDI_MATURATION_JEV_ROUTER");

  if (!useLive && !useJev) return createNoopPublicResearcher();

  const directPublic =
    options.directPublicExecutor ||
    createDirectPublicExecutor(options.directPublicDefaults || {});
  const parallel =
    options.parallelExecutor ||
    (options.paidBlocked === true
      ? null
      : createParallelMaturationExecutor(options.parallelDefaults || {}));

  const router = createMaturationResearcherRouter({
    directPublic,
    parallel,
    jevDecideFn: options.jevDecideFn,
    planner: options.planner,
  });

  return {
    async execute(args = {}) {
      return router.execute({
        ...args,
        options: {
          ...(args.options || {}),
          jevEnabled: useJev ? args.options?.jevEnabled !== false : false,
          forceJev: useJev ? true : Boolean(args.options?.forceJev),
          paidBlocked: options.paidBlocked ?? args.options?.paidBlocked,
          hotelId: options.hotelId ?? args.options?.hotelId,
          hotelConfig: options.hotelConfig ?? args.options?.hotelConfig,
          authorizationRef:
            options.authorizationRef ?? args.options?.authorizationRef,
          remainingBudgetUsd:
            options.remainingBudgetUsd ??
            args.options?.remainingBudgetUsd ??
            options.supervisor?.remainingBudgetUsd,
        },
      });
    },
  };
}

export async function runOpportunityMaturationStage({
  hotel,
  expectedSegments = SUPERVISOR_EXPECTED_SEGMENTS,
  candidates = [],
  segmentRuns = [],
  researcher,
  persistOpportunity,
  options = {},
  delivery = {},
  artifactDir = null,
} = {}) {
  const planner = options.planner || new HeuristicResearchPlanner();
  const activeResearcher =
    researcher ||
    resolveMaturationResearcher({ ...options, researcher: options.researcher });

  const matured = await matureOpportunityQueue({
    opportunities: candidates.map((c) => ({
      ...c,
      hotel: c.hotel || hotel,
      segment: canonicalizeSegmentId(c.segment || c.segmentId),
    })),
    planner,
    researcher: activeResearcher,
    persist: persistOpportunity,
    options: options.supervisor || options,
    onProgress: options.onProgress,
  });

  const terminatedBySegment = new Map();
  for (const item of matured) {
    const segment = canonicalizeSegmentId(item.segment);
    terminatedBySegment.set(segment, (terminatedBySegment.get(segment) || 0) + 1);
  }

  const normalizedSegmentRuns = (segmentRuns.length
    ? segmentRuns
    : expectedSegments.map((segment) => ({
        segment,
        applicable: true,
        independentTasks: 0,
        sourceFamilies: [],
        candidatesDiscovered: 0,
        candidatesTerminated: 0,
      }))
  ).map((run) => {
    const segment = canonicalizeSegmentId(run.segment || run.segmentId);
    const discovered =
      run.candidatesDiscovered ??
      matured.filter((m) => canonicalizeSegmentId(m.segment) === segment).length;
    return {
      ...run,
      segment,
      candidatesDiscovered: discovered,
      candidatesTerminated:
        run.candidatesTerminated ?? terminatedBySegment.get(segment) ?? 0,
    };
  });

  const coverage = segmentCoverageGate({
    expectedSegments,
    segmentRuns: normalizedSegmentRuns,
    minimumDepth: options.minimumDepth || {},
  });

  const completion = hotelCompletionGate({
    coverage,
    opportunities: matured,
    minHotelFacingProspects: options.minHotelFacingProspects ?? 10,
    requireProductionDelivery: options.requireProductionDelivery ?? true,
    delivery,
  });

  const artifact = {
    version: SUPERVISOR_VERSION,
    at: new Date().toISOString(),
    hotelId: hotel?.canonicalHotelId || hotel?.hotelId || hotel?.id || null,
    maturedCount: matured.length,
    coverage,
    completion,
    evaluations: matured.map((m) => ({
      id: m.id,
      segment: m.segment,
      researchStatus: m.researchStatus,
      stopReason: m.stopReason || null,
      prospectMaturity: m.prospectMaturity || null,
      attempts: (m.researchAttempts || []).length,
      evaluation: evaluateOpportunity(m, options.supervisor || options),
    })),
    defaults: DEFAULT_MINIMUM_DEPTH,
    note: "Maturation is advisory for publication — eligibility/Ready gates unchanged",
  };

  if (artifactDir) {
    fs.mkdirSync(artifactDir, { recursive: true });
    fs.writeFileSync(
      path.join(artifactDir, "opportunity-maturation.json"),
      JSON.stringify(artifact, null, 2) + "\n"
    );
    fs.writeFileSync(
      path.join(artifactDir, "matured-opportunities.json"),
      JSON.stringify(matured, null, 2) + "\n"
    );
    fs.writeFileSync(
      path.join(artifactDir, "segment-coverage.json"),
      JSON.stringify(coverage, null, 2) + "\n"
    );
  }

  return { matured, coverage, completion, artifact };
}
