/**
 * Hotel Research Orchestrator v1
 *
 * Owns the complete hotel research queue: prioritize → mature → handoff →
 * next candidate → segment gaps → resume checkpoint.
 *
 * Batch size = checkpoint / resource cadence, NOT hotel-job completion.
 * Full E2E evaluator remains the only COMPLETE authority.
 */

import fs from "node:fs";
import path from "node:path";
import {
  RESEARCH_TERMINAL,
  STOP_REASONS,
  RESUMABLE_STOP_REASONS,
  HARD_TERMINAL_STOP_REASONS,
  ORCHESTRATOR_STOP_REASONS,
  ORCHESTRATOR_RESUMABLE_CONTINUATION,
  SUPERVISOR_EXPECTED_SEGMENTS,
  SEGMENT_TERMINAL_OK,
  canonicalizeSegmentId,
} from "./research-constants-v1.js";
import {
  matureOpportunity,
  evaluateOpportunity,
  hasNamedBuyer,
  isActionableContactRoute,
  isHotelFacingAdvisory,
  classifyExhaustionStop,
  estimateParallelFollowupExpectedValue,
} from "./opportunity-research-supervisor-v1.js";
import { resolveMaturationResearcher } from "./opportunity-maturation-stage-v1.js";
import {
  HeuristicResearchPlanner,
  hasTriedParallelFollowup,
  hasCompletedParallelFollowup,
  publicPathExhaustedForBlocker,
  remainingPublicStrategiesForBlocker,
} from "./research-planner-v1.js";
import {
  segmentCoverageGate,
  hotelCompletionGate,
} from "./segment-coverage-controller-v1.js";
import { handoffMaturedOpportunityToPublication } from "./maturation-publication-handoff-v1.js";
import { assessSegmentResearchSufficiency } from "./segment-research-sufficiency-v1.js";

/**
 * Repair false RESEARCH_EXHAUSTED from earlier canaries when Parallel was never tried.
 */
export function reclassifyFalseResearchExhausted(opportunity = {}, { paidBlocked = true } = {}) {
  const blockerKey =
    (opportunity.researchAttempts || []).slice(-1)[0]?.blocker || "future_demand";
  const prior = opportunity.stopReason;
  // Parallel follow-up is terminal only when public strategies are also exhausted.
  // Never reopen exploratory/publishable commercial dispositions.
  if (hasCompletedParallelFollowup(opportunity, blockerKey)) {
    if (
      opportunity.researchStatus === RESEARCH_TERMINAL.EXPLORATORY_PROSPECT ||
      opportunity.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT
    ) {
      return opportunity;
    }
    const remPublic = remainingPublicStrategiesForBlocker(
      opportunity,
      blockerKey
    );
    if (remPublic.length > 0) {
      return {
        ...opportunity,
        stopReason: null,
        researchStatus: RESEARCH_TERMINAL.IN_PROGRESS,
        reopenStrategies: [
          ...new Set([...(opportunity.reopenStrategies || []), ...remPublic]),
        ],
        notes: [
          ...(opportunity.notes || []),
          `reclassified_continue_public_after_parallel:${remPublic.join(",")}`,
        ],
      };
    }
    if (
      HARD_TERMINAL_STOP_REASONS.includes(prior) &&
      prior !== STOP_REASONS.RESEARCH_EXHAUSTED
    ) {
      return opportunity;
    }
    return {
      ...opportunity,
      stopReason: STOP_REASONS.NO_FUTURE_DEMAND_EVIDENCE,
      researchStatus: RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON,
      notes: [
        ...(opportunity.notes || []),
        "reclassified_paid_followup_after_completed_parallel",
      ],
    };
  }
  if (
    prior !== STOP_REASONS.RESEARCH_EXHAUSTED &&
    prior !== STOP_REASONS.NO_FUTURE_DEMAND_EVIDENCE
  ) {
    return opportunity;
  }
  const nextReason = classifyExhaustionStop({
    opportunity,
    blockerKey,
    paidBlocked,
    parallelAvailable: true,
  });
  if (!nextReason || nextReason === prior) {
    return opportunity;
  }
  // Only soften false terminals → resumable/provider states
  const soften = new Set([
    STOP_REASONS.PUBLIC_PATH_EXHAUSTED,
    STOP_REASONS.PAID_FOLLOWUP_AVAILABLE,
    STOP_REASONS.PROVIDER_BLOCKED,
    STOP_REASONS.CONTACT_RESEARCH_PENDING,
  ]);
  if (!soften.has(nextReason)) {
    return opportunity;
  }
  return {
    ...opportunity,
    stopReason: nextReason,
    researchStatus: RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON,
    notes: [
      ...(opportunity.notes || []),
      `reclassified_from_${prior}_to_${nextReason}`,
    ],
  };
}

export const ORCHESTRATOR_VERSION = "gdi-hotel-research-orchestrator-v1";
export const ORCHESTRATOR_STATE_FILENAME = "orchestrator-state-v1.json";

function now() {
  return new Date().toISOString();
}

function oppId(o = {}) {
  return String(o.id || o.opportunityId || "").trim();
}

function dedupeOpportunities(list = []) {
  const byId = new Map();
  for (const o of list) {
    const id = oppId(o);
    if (!id) continue;
    const prev = byId.get(id);
    if (!prev) {
      byId.set(id, o);
      continue;
    }
    // Prefer richer researchAttempts / newer updatedAt
    const prevN = (prev.researchAttempts || []).length;
    const nextN = (o.researchAttempts || []).length;
    byId.set(id, nextN >= prevN ? { ...prev, ...o } : { ...o, ...prev });
  }
  return [...byId.values()];
}

export function scoreOrchestratorCandidate(raw = {}, options = {}) {
  const segment = canonicalizeSegmentId(
    raw.segment || raw.researchSegment || raw.demandSegment
  );
  const ev = evaluateOpportunity({ ...raw, segment });
  const facing = isHotelFacingAdvisory(raw);
  const named = hasNamedBuyer(raw.contact || {});
  const route = isActionableContactRoute(raw.contact || {});
  const blockers = ev.blockers.filter((b) => b.recoverable).map((b) => b.key);
  const hard = Boolean(ev.hardStop);
  const terminal = HARD_TERMINAL_STOP_REASONS.includes(raw.stopReason);
  const resumable = RESUMABLE_STOP_REASONS.includes(raw.stopReason);
  const publishable =
    raw.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT;
  const parallelDone = hasCompletedParallelFollowup(
    raw,
    blockers[0] ||
      (raw.researchAttempts || []).slice(-1)[0]?.blocker ||
      "future_demand"
  );

  let band = "C";
  if (parallelDone || terminal) band = "C";
  else if (!facing && !hard && !terminal && blockers.length <= 4) band = "A";
  else if (facing && route && !named) band = "B";
  else if (resumable && raw.stopReason === STOP_REASONS.PAID_FOLLOWUP_AVAILABLE)
    band = "A";
  else if (resumable && raw.stopReason === STOP_REASONS.PUBLIC_PATH_EXHAUSTED)
    band = "A";

  const identity =
    Boolean(raw.organizationName) &&
    Boolean(raw.officialUrl || raw.officialDomain || (raw.sources || []).length);
  let score = 0;
  if (band === "A") score += 50;
  if (band === "B") score += 40;
  if (identity) score += 15;
  if (blockers.length && blockers.length <= 2) score += 10;
  if (raw.stopReason === STOP_REASONS.PAID_FOLLOWUP_AVAILABLE && !parallelDone)
    score += 12;
  if (publishable) score -= 30; // already advisory done unless handoff pending
  if (facing && named) score -= 40;
  if (terminal) score -= 100;
  if (hard) score -= 100;
  // Parallel follow-up done is not terminal when public strategies remain.
  const publicStillOpen =
    parallelDone &&
    !publicPathExhaustedForBlocker(
      raw,
      blockers[0] ||
        (raw.researchAttempts || []).slice(-1)[0]?.blocker ||
        "future_demand"
    );
  if (parallelDone && !publicStillOpen) score -= 200;
  if (publicStillOpen) {
    band = "A";
    score += 40;
  }
  if (options.processedIds?.has(oppId(raw))) score -= 200;

  return {
    id: oppId(raw),
    organizationName: raw.organizationName || raw.company || raw.title,
    segment,
    band,
    score,
    facing,
    namedBuyer: named,
    actionableRoute: route,
    blockers,
    hardStop: hard,
    stopReason: raw.stopReason || null,
    researchStatus: raw.researchStatus || null,
    opportunity: { ...raw, segment },
  };
}

export function selectNextOrchestratorCandidate(scored = [], options = {}) {
  const exclude = options.excludeIds || new Set();
  const eligible = scored
    .filter((s) => s.id && !exclude.has(s.id) && s.score > -50)
    .filter((s) => !HARD_TERMINAL_STOP_REASONS.includes(s.stopReason))
    // Already dispositioned / stopped opportunities must not consume batch slots —
    // otherwise waves spin on NO_FUTURE/DUPLICATE and never reach !next rediscovery.
    .filter((s) => {
      const st = s.researchStatus;
      if (st === RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON) return false;
      if (st === RESEARCH_TERMINAL.EXPLORATORY_PROSPECT) return false;
      if (
        st === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT &&
        options.includePublishableForHandoff !== true
      ) {
        return false;
      }
      return true;
    })
    .filter((s) => {
      const o = s.opportunity || {};
      const blockerKey =
        s.blockers?.[0] ||
        (o.researchAttempts || []).slice(-1)[0]?.blocker ||
        "future_demand";
      // Parallel done only blocks selection when all relevant public strategies
      // are also exhausted — otherwise continue unpaid public research.
      if (!hasCompletedParallelFollowup(o, blockerKey)) return true;
      return remainingPublicStrategiesForBlocker(o, blockerKey).length > 0;
    })
    .sort((a, b) => b.score - a.score || a.id.localeCompare(b.id));
  return eligible[0] || null;
}

export function defaultOrchestratorStateDir(hotelId, root = process.cwd()) {
  return path.join(
    root,
    "data/group-demand-intelligence/hotels",
    String(hotelId),
    "research-orchestrator"
  );
}

export function loadOrchestratorState(stateDir) {
  const p = path.join(stateDir, ORCHESTRATOR_STATE_FILENAME);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function saveOrchestratorState(stateDir, state) {
  fs.mkdirSync(stateDir, { recursive: true });
  const p = path.join(stateDir, ORCHESTRATOR_STATE_FILENAME);
  const tmp = `${p}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2) + "\n");
  fs.renameSync(tmp, p);
  return p;
}

export function acquireOrchestratorLease(state, { owner, leaseMs = 30 * 60_000, force = false } = {}) {
  const nowMs = Date.now();
  const held =
    state.lease?.owner &&
    state.lease.owner !== owner &&
    !state.lease.releasedAt &&
    state.lease.expiresAtMs &&
    state.lease.expiresAtMs > nowMs;
  if (held && !force) {
    return { ok: false, reason: ORCHESTRATOR_STOP_REASONS.LEASE_HELD_BY_OTHER, lease: state.lease };
  }
  state.lease = {
    owner,
    acquiredAt: now(),
    expiresAtMs: nowMs + leaseMs,
    releasedAt: null,
  };
  return { ok: true, lease: state.lease };
}

export function buildSegmentRunsFromCorpus(opportunities = [], expectedSegments = SUPERVISOR_EXPECTED_SEGMENTS) {
  return expectedSegments.map((segment) => {
    const inSeg = opportunities.filter(
      (o) => canonicalizeSegmentId(o.segment || o.researchSegment) === segment
    );
    const families = new Set();
    let followUps = 0;
    let strategies = new Set();
    const discoverySources = new Set();
    for (const o of inSeg) {
      if (o.discoverySource) discoverySources.add(o.discoverySource);
      for (const e of o.evidence || []) {
        if (e.sourceFamily) families.add(e.sourceFamily);
      }
      for (const a of o.researchAttempts || []) {
        followUps += 1;
        if (a.strategy) strategies.add(a.strategy);
        for (const f of a.sourceFamilies || []) families.add(f);
      }
    }
    const terminated = inSeg.filter(
      (o) =>
        HARD_TERMINAL_STOP_REASONS.includes(o.stopReason) ||
        o.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT ||
        o.researchStatus === RESEARCH_TERMINAL.EXPLORATORY_PROSPECT ||
        o.customerVisible === true
    ).length;
    // Distinct discovery sources / waves — not "any candidate ⇒ 1 task"
    const independentTasks =
      discoverySources.size > 0
        ? discoverySources.size
        : inSeg.length > 0
          ? 1
          : 0;
    return {
      segment,
      applicable: true,
      independentTasks,
      sourceFamilies: [...families],
      candidatesDiscovered: inSeg.length,
      candidatesTerminated: terminated,
      followUpPasses: followUps > inSeg.length ? 1 : 0,
      strategiesTried: strategies.size,
      furtherResearchUnlikely: false,
      stoppingReason: null,
    };
  });
}

/**
 * @param {object} args
 */
export async function runHotelResearchOrchestrator({
  hotelId,
  hotel = {},
  opportunities = [],
  expectedSegments = SUPERVISOR_EXPECTED_SEGMENTS,
  options = {},
  persistOpportunity,
  persistCanonical,
  onProgress,
} = {}) {
  const stateDir =
    options.stateDir || defaultOrchestratorStateDir(hotelId, options.root);
  const owner = options.leaseOwner || `orch_${process.pid}_${Date.now()}`;
  const batchSize = Math.max(1, Number(options.batchSize || 5));
  const maxCandidatesPerRun = Number(
    options.maxCandidatesPerRun ?? options.executionLimit ?? 25
  );
  const paidBlocked = options.paidBlocked === true;
  const authorizePublication = options.authorizePublication === true;
  const maxPaidTasks = Number(options.maxPaidTasks ?? 2);
  let paidTasksUsed = Number(options.paidTasksUsed || 0);
  const remainingBudgetUsd = Number(options.remainingBudgetUsd ?? Infinity);

  let state =
    options.resume === false
      ? null
      : loadOrchestratorState(stateDir) || null;

  if (!state) {
    state = {
      version: ORCHESTRATOR_VERSION,
      hotelId,
      createdAt: now(),
      queuedIds: [],
      activeId: null,
      completedIds: [],
      pausedIds: [],
      discoveredIds: [],
      processedThisRun: [],
      handoffs: [],
      segmentCoverage: null,
      nextRecommendedAction: null,
      overallStopReason: null,
      remainingBudgetUsd,
      paidTasksUsed: 0,
      checkpoint: null,
      lease: null,
      history: [],
    };
  }

  state.updatedAt = now();
  const lease = acquireOrchestratorLease(state, {
    owner,
    leaseMs: options.leaseMs,
    force: options.forceLease === true,
  });
  if (!lease.ok) {
    state.overallStopReason = lease.reason;
    saveOrchestratorState(stateDir, state);
    return {
      status: "BLOCKED",
      stopReason: lease.reason,
      state,
      matured: [],
      handoffs: [],
    };
  }
  // Clear stale stop from a prior blocked acquire once lease is held
  if (
    state.overallStopReason === ORCHESTRATOR_STOP_REASONS.LEASE_HELD_BY_OTHER ||
    state.overallStopReason === ORCHESTRATOR_STOP_REASONS.EXPLICIT_PAUSE
  ) {
    state.overallStopReason = null;
  }

  const corpus = dedupeOpportunities(opportunities).map((o) =>
    reclassifyFalseResearchExhausted(o, { paidBlocked })
  );
  const byId = new Map(corpus.map((o) => [oppId(o), o]));
  // Seed queue with all non-hard-terminal ids not completed
  const completed = new Set(state.completedIds || []);
  const paused = new Set(state.pausedIds || []);
  const priorCompleted = new Set(completed);
  for (const o of corpus) {
    const id = oppId(o);
    if (!id) continue;
    const blockerKey =
      (o.researchAttempts || []).slice(-1)[0]?.blocker || "future_demand";
    // Parallel follow-up alone is not exhaustion when public strategies remain.
    // Never reopen already dispositioned exploratory/publishable prospects.
    const alreadyDispositioned =
      o.researchStatus === RESEARCH_TERMINAL.EXPLORATORY_PROSPECT ||
      o.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT;
    if (alreadyDispositioned) {
      completed.add(id);
      paused.delete(id);
      continue;
    }
    if (hasCompletedParallelFollowup(o, blockerKey)) {
      const remPublic = remainingPublicStrategiesForBlocker(o, blockerKey);
      if (remPublic.length === 0 && publicPathExhaustedForBlocker(o, blockerKey)) {
        completed.add(id);
        paused.delete(id);
        byId.set(id, {
          ...o,
          stopReason: STOP_REASONS.NO_FUTURE_DEMAND_EVIDENCE,
          researchStatus: RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON,
        });
        continue;
      }
      // Reopen for remaining public strategies (do not invent Parallel spend).
      completed.delete(id);
      paused.delete(id);
      const reopened = {
        ...o,
        stopReason: null,
        researchStatus: RESEARCH_TERMINAL.IN_PROGRESS,
        reopenStrategies: [
          ...new Set([...(o.reopenStrategies || []), ...remPublic]),
        ],
        notes: [
          ...(o.notes || []),
          `orchestrator_continue_public_after_parallel:${remPublic.join(",")}`,
        ],
      };
      byId.set(id, reopened);
      if (!state.queuedIds.includes(id)) state.queuedIds.push(id);
      continue;
    }
    // Softened reclassification → reopen for paid/provider retry once.
    // Never reopen priorCompleted after Parallel already completed.
    if (
      RESUMABLE_STOP_REASONS.includes(o.stopReason) &&
      completed.has(id)
    ) {
      completed.delete(id);
      paused.add(id);
    }
    if (completed.has(id)) continue;
    if (HARD_TERMINAL_STOP_REASONS.includes(o.stopReason)) {
      completed.add(id);
      continue;
    }
    if (!state.queuedIds.includes(id) && !paused.has(id)) {
      state.queuedIds.push(id);
    }
  }
  state.completedIds = [...completed];
  state.pausedIds = [...paused];
  state.queuedIds = state.queuedIds.filter(
    (id) => !completed.has(id) && !paused.has(id)
  );

  const planner = options.planner || new HeuristicResearchPlanner();
  const researcher =
    options.researcher ||
    resolveMaturationResearcher({
      useLiveResearchers: options.useLiveResearchers !== false,
      useJevRouter: options.useJevRouter !== false,
      paidBlocked,
      hotelId,
      hotelConfig: hotel,
      authorizationRef: options.authorizationRef,
      remainingBudgetUsd,
      parallelDefaults: options.parallelDefaults,
      directPublicDefaults: options.directPublicDefaults,
      jevDecideFn: options.jevDecideFn,
    });

  const matured = [];
  const handoffs = [];
  let processed = 0;
  let batches = 0;
  const paidEscalationAttempted = new Set();
  // Multi-round rediscovery: queue drain ≠ segment complete.
  // Cap rounds so zero-yield rediscovery cannot loop forever.
  let segmentDiscoveryRounds = 0;
  const maxSegmentDiscoveryRounds = Math.max(
    1,
    Number(options.maxSegmentDiscoveryRounds ?? 3)
  );

  const persistOne = async (s) => {
    byId.set(oppId(s), s);
    if (persistOpportunity) await persistOpportunity(s);
  };

  while (processed < maxCandidatesPerRun) {
    // Refresh scores from live byId map
    const scored = [...byId.values()].map((o) =>
      scoreOrchestratorCandidate(o, { processedIds: completed })
    );
    const exclude = new Set([...completed, ...paused]);
    // Re-open paused paid-followup at most once per run (never if Parallel already completed)
    if (!paidBlocked) {
      for (const id of [...paused]) {
        if (paidEscalationAttempted.has(id)) continue;
        const o = byId.get(id);
        const blockerKey =
          (o?.researchAttempts || []).slice(-1)[0]?.blocker || "future_demand";
        if (hasCompletedParallelFollowup(o, blockerKey)) {
          completed.add(id);
          paused.delete(id);
          continue;
        }
        if (
          o?.stopReason === STOP_REASONS.PAID_FOLLOWUP_AVAILABLE ||
          o?.stopReason === STOP_REASONS.PUBLIC_PATH_EXHAUSTED ||
          o?.stopReason === STOP_REASONS.PROVIDER_BLOCKED
        ) {
          exclude.delete(id);
        }
      }
    }

    const next = selectNextOrchestratorCandidate(scored, {
      excludeIds: exclude,
      includePublishableForHandoff: authorizePublication,
    });

    // Hard guard: never re-mature durable completed terminals (incl. Parallel-done)
    if (
      next &&
      completed.has(next.id) &&
      !paidEscalationAttempted.has(next.id)
    ) {
      const o = byId.get(next.id) || next.opportunity;
      const blockerKey =
        next.blockers?.[0] ||
        (o?.researchAttempts || []).slice(-1)[0]?.blocker ||
        "future_demand";
      if (
        HARD_TERMINAL_STOP_REASONS.includes(o?.stopReason) ||
        (hasCompletedParallelFollowup(o, blockerKey) &&
          remainingPublicStrategiesForBlocker(o, blockerKey).length === 0)
      ) {
        exclude.add(next.id);
        paused.delete(next.id);
        continue;
      }
    }

    if (!next) {
      // Segment gap discovery signal (orchestrator records need; does not invent Parallel discovery here unless opts)
      const segmentRuns = buildSegmentRunsFromCorpus(
        [...byId.values()],
        expectedSegments
      );
      const coverage = segmentCoverageGate({
        expectedSegments,
        segmentRuns,
      });
      state.segmentCoverage = coverage;
      if (
        !coverage.pass &&
        options.runSegmentDiscovery === true &&
        typeof options.segmentDiscoveryFn === "function" &&
        segmentDiscoveryRounds < maxSegmentDiscoveryRounds
      ) {
        segmentDiscoveryRounds += 1;
        const discovery = await options.segmentDiscoveryFn({
          hotelId,
          hotel,
          coverage,
          remainingBudgetUsd: remainingBudgetUsd,
          paidBlocked,
          existingOpportunities: [...byId.values()],
          discoveryRound: segmentDiscoveryRounds,
          shallowSegments: (coverage.rows || [])
            .filter(
              (r) =>
                r.status === "INSUFFICIENT_RESEARCH_DEPTH" ||
                r.status === "PENDING" ||
                r.status === "IN_PROGRESS"
            )
            .map((r) => r.segment),
        });
        const added = dedupeOpportunities(discovery?.candidates || []);
        let inserted = 0;
        for (const c of added) {
          const id = oppId(c);
          if (!id || byId.has(id)) continue;
          byId.set(id, c);
          state.discoveredIds.push(id);
          state.queuedIds.push(id);
          inserted += 1;
        }
        state.history.push({
          at: now(),
          kind: "segment_discovery",
          round: segmentDiscoveryRounds,
          added: added.map(oppId).filter(Boolean),
          inserted,
          mode: discovery?.mode || null,
          note: discovery?.note || null,
        });
        if (inserted > 0) {
          continue;
        }
        // Zero insert this round — try another round only if rounds remain;
        // otherwise fall through to DISCOVERY_INCOMPLETE (not false COMPLETE).
      }

      state.overallStopReason = coverage.pass
        ? ORCHESTRATOR_STOP_REASONS.QUEUE_DRAINED
        : ORCHESTRATOR_STOP_REASONS.DISCOVERY_INCOMPLETE;
      state.nextRecommendedAction = coverage.pass
        ? null
        : {
            kind: "SEGMENT_DISCOVERY",
            reason: "coverage_incomplete",
            shallowSegments: (coverage.rows || [])
              .filter((r) => r.status === "INSUFFICIENT_RESEARCH_DEPTH")
              .map((r) => r.segment),
          };
      break;
    }

    if (remainingBudgetUsd <= 0 && !paidBlocked) {
      state.overallStopReason = ORCHESTRATOR_STOP_REASONS.BUDGET_BLOCKED;
      break;
    }

    state.activeId = next.id;
    state.history.push({
      at: now(),
      kind: "select",
      id: next.id,
      band: next.band,
      score: next.score,
    });

    // Rehydrate stop for paid escalation OR remaining public strategies
    const remPublicAtSelect = remainingPublicStrategiesForBlocker(
      next.opportunity,
      next.blockers[0] || "future_demand"
    );
    const reopenPaid =
      !paidBlocked &&
      (next.stopReason === STOP_REASONS.PUBLIC_PATH_EXHAUSTED ||
        next.stopReason === STOP_REASONS.PAID_FOLLOWUP_AVAILABLE ||
        next.stopReason === STOP_REASONS.PROVIDER_BLOCKED);
    const reopenPublic =
      remPublicAtSelect.length > 0 &&
      (next.stopReason === STOP_REASONS.PUBLIC_PATH_EXHAUSTED ||
        next.stopReason === STOP_REASONS.PAID_FOLLOWUP_AVAILABLE ||
        next.stopReason === STOP_REASONS.PROVIDER_BLOCKED ||
        next.opportunity.researchStatus === RESEARCH_TERMINAL.IN_PROGRESS);
    const reopen = reopenPaid || reopenPublic;

    let opportunity = {
      ...next.opportunity,
      hotel: next.opportunity.hotel || hotel,
      stopReason: reopen ? null : next.opportunity.stopReason,
      researchStatus: reopen
        ? RESEARCH_TERMINAL.IN_PROGRESS
        : next.opportunity.researchStatus,
      reopenStrategies: reopenPublic
        ? [
            ...new Set([
              ...(next.opportunity.reopenStrategies || []),
              ...remPublicAtSelect,
            ]),
          ]
        : next.opportunity.reopenStrategies,
      notes: reopen
        ? [
            ...(next.opportunity.notes || []),
            reopenPaid
              ? `orchestrator_reopen_paid:${next.stopReason}`
              : `orchestrator_reopen_public:${remPublicAtSelect.join(",")}`,
          ]
        : next.opportunity.notes,
    };

    // EV gate before spending paid on PUBLIC_PATH_EXHAUSTED / PAID_FOLLOWUP_AVAILABLE
    if (reopenPaid) {
      paidEscalationAttempted.add(next.id);
      const blockerKey = next.blockers[0] || "future_demand";
      const ev = estimateParallelFollowupExpectedValue({
        opportunity,
        blockerKey,
        remainingBudgetUsd,
      });
      if (!ev.justified) {
        // Do not convert EV miss into a commercial rejection — keep resumable.
        if (ev.reason === "PUBLIC_NOT_EXHAUSTED") {
          opportunity.stopReason = null;
          opportunity.researchStatus = RESEARCH_TERMINAL.IN_PROGRESS;
          // Fall through to matureOpportunity for remaining public work.
        } else if (
          ev.reason === "BUDGET_INSUFFICIENT" ||
          ev.reason === "PARALLEL_ALREADY_TRIED"
        ) {
          opportunity.stopReason = STOP_REASONS.PAID_FOLLOWUP_AVAILABLE;
          opportunity.researchStatus =
            RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
          opportunity.stopDetail = { parallelEv: ev };
          await persistOne(opportunity);
          paused.add(next.id);
          completed.delete(next.id);
          matured.push(opportunity);
          processed += 1;
          continue;
        } else {
          opportunity.stopReason = STOP_REASONS.PAID_FOLLOWUP_AVAILABLE;
          opportunity.researchStatus =
            RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
          opportunity.stopDetail = { parallelEv: ev };
          await persistOne(opportunity);
          paused.add(next.id);
          completed.delete(next.id);
          matured.push(opportunity);
          processed += 1;
          continue;
        }
      }
      if (paidTasksUsed >= maxPaidTasks) {
        const remPublic = remainingPublicStrategiesForBlocker(
          opportunity,
          blockerKey
        );
        if (remPublic.length > 0) {
          // Paid cap reached but public strategies remain — continue unpaid.
          opportunity.stopReason = null;
          opportunity.researchStatus = RESEARCH_TERMINAL.IN_PROGRESS;
          opportunity.reopenStrategies = [
            ...new Set([...(opportunity.reopenStrategies || []), ...remPublic]),
          ];
          // Fall through to matureOpportunity with paidBlocked forced below.
        } else if (hasCompletedParallelFollowup(opportunity, blockerKey)) {
          opportunity.stopReason = STOP_REASONS.NO_FUTURE_DEMAND_EVIDENCE;
          opportunity.researchStatus =
            RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
          completed.add(next.id);
          paused.delete(next.id);
          await persistOne(opportunity);
          matured.push(opportunity);
          processed += 1;
          byId.set(next.id, opportunity);
          state.completedIds = [...completed];
          state.pausedIds = [...paused];
          state.queuedIds = state.queuedIds.filter(
            (id) => !completed.has(id) && !paused.has(id)
          );
          continue;
        } else {
          opportunity.stopReason = STOP_REASONS.PAID_FOLLOWUP_AVAILABLE;
          opportunity.researchStatus =
            RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
          paused.add(next.id);
          completed.delete(next.id);
          await persistOne(opportunity);
          matured.push(opportunity);
          processed += 1;
          byId.set(next.id, opportunity);
          state.completedIds = [...completed];
          state.pausedIds = [...paused];
          state.queuedIds = state.queuedIds.filter(
            (id) => !completed.has(id) && !paused.has(id)
          );
          state.nextRecommendedAction = {
            kind: "CONTINUE_QUEUE_PUBLIC",
            reason: "maxPaidTasks_reached_skip_paid_candidate",
            opportunityId: next.id,
          };
          continue;
        }
      }
      // Seed note so supervisor forces Parallel on first iteration
      opportunity.notes = [
        ...(opportunity.notes || []),
        `escalate_parallel_followup:${blockerKey}:${ev.reason}`,
      ];
    }

    const forcePaidBlockedForPublic =
      paidTasksUsed >= maxPaidTasks &&
      remainingPublicStrategiesForBlocker(
        opportunity,
        next.blockers[0] || "future_demand"
      ).length > 0;
    const result = await matureOpportunity({
      opportunity,
      planner,
      researcher,
      persist: persistOne,
      options: {
        ...(options.supervisor || {}),
        paidBlocked: paidBlocked || forcePaidBlockedForPublic,
        remainingBudgetUsd,
        maxPaidCostUsd: options.maxPaidCostUsd ?? remainingBudgetUsd,
        parallelAvailable: !(paidBlocked || forcePaidBlockedForPublic),
        hotelId,
        hotelConfig: hotel,
        authorizationRef: options.authorizationRef,
        deepenNamedBuyer: true,
      },
    });

    matured.push(result);
    processed += 1;
    state.processedThisRun.push(next.id);
    // Count only NEW paid Parallel attempts from this maturation (not historical)
    const priorParallelPaid = new Set(
      (next.opportunity?.researchAttempts || [])
        .filter(
          (a) =>
            a.provider === "parallel" &&
            !a.reusedProviderRun &&
            Number(a.costUsd) > 0 &&
            a.providerRunId
        )
        .map((a) => a.providerRunId)
    );
    const newPaidParallel = (result.researchAttempts || []).filter(
      (a) =>
        a.provider === "parallel" &&
        !a.reusedProviderRun &&
        Number(a.costUsd) > 0 &&
        a.providerRunId &&
        !priorParallelPaid.has(a.providerRunId)
    ).length;
    paidTasksUsed += newPaidParallel;
    state.paidTasksUsed = paidTasksUsed;

    // Classify pause vs complete — never mark IN_PROGRESS with remaining
    // public strategies as completed (premature Parallel-done trap).
    const resultBlocker =
      next.blockers[0] ||
      (result.researchAttempts || []).slice(-1)[0]?.blocker ||
      "future_demand";
    const stillHasPublic =
      remainingPublicStrategiesForBlocker(result, resultBlocker).length > 0;
    if (
      result.researchStatus === RESEARCH_TERMINAL.IN_PROGRESS ||
      (stillHasPublic &&
        result.researchStatus !== RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT &&
        result.researchStatus !== RESEARCH_TERMINAL.EXPLORATORY_PROSPECT &&
        !HARD_TERMINAL_STOP_REASONS.includes(result.stopReason))
    ) {
      completed.delete(next.id);
      paused.delete(next.id);
      if (!state.queuedIds.includes(next.id)) state.queuedIds.push(next.id);
    } else if (RESUMABLE_STOP_REASONS.includes(result.stopReason)) {
      paused.add(next.id);
      completed.delete(next.id);
    } else if (
      result.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT ||
      HARD_TERMINAL_STOP_REASONS.includes(result.stopReason) ||
      result.researchStatus === RESEARCH_TERMINAL.EXPLORATORY_PROSPECT
    ) {
      completed.add(next.id);
      paused.delete(next.id);
    } else {
      completed.add(next.id);
    }

    // Publication handoff for advisory publishable
    if (result.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT) {
      const handoff = handoffMaturedOpportunityToPublication({
        matured: result,
        prior: next.opportunity,
        authorizePublication,
        authorizedBy: options.authorizedBy || owner,
      });
      handoffs.push({
        opportunityId: next.id,
        ...handoff,
        patchedId: oppId(handoff.patched),
      });
      state.handoffs.push({
        at: now(),
        opportunityId: next.id,
        handoffStatus: handoff.handoffStatus,
        blockingGate: handoff.blockingGate,
      });
      if (
        handoff.handoffStatus === "PROMOTED_HOTEL_FACING" &&
        handoff.patched
      ) {
        await persistOne(handoff.patched);
        if (persistCanonical) {
          await persistCanonical(handoff.patched, { handoff });
        }
      }
    }

    byId.set(next.id, result);
    state.activeId = null;
    state.completedIds = [...completed];
    state.pausedIds = [...paused];
    state.queuedIds = state.queuedIds.filter(
      (id) => !completed.has(id) && !paused.has(id)
    );
    state.checkpoint = {
      at: now(),
      processed,
      batches,
      activeId: null,
      remainingQueued: state.queuedIds.length,
      paused: state.pausedIds.length,
    };

    if (onProgress) {
      await onProgress({
        processed,
        result,
        state,
        next,
      });
    }

    // Batch = checkpoint cadence, not completion
    if (processed % batchSize === 0) {
      batches += 1;
      state.nextRecommendedAction = {
        kind: "CHECKPOINT_CONTINUE",
        reason: "batch_checkpoint",
        batchSize,
        processed,
        remainingQueued: state.queuedIds.length + state.pausedIds.length,
      };
      saveOrchestratorState(stateDir, state);
      if (options.pauseAfterBatch === true) {
        state.overallStopReason = ORCHESTRATOR_STOP_REASONS.CHECKPOINT_CONTINUE;
        break;
      }
    }
  }

  if (!state.overallStopReason) {
    const remaining =
      state.queuedIds.length +
      state.pausedIds.filter((id) => {
        const o = byId.get(id);
        return o && RESUMABLE_STOP_REASONS.includes(o.stopReason);
      }).length;
    if (processed >= maxCandidatesPerRun && remaining > 0) {
      // Soft checkpoint — caller should continue without force-lease
      state.overallStopReason = ORCHESTRATOR_STOP_REASONS.CHECKPOINT_CONTINUE;
      state.nextRecommendedAction = {
        kind: "CONTINUE_QUEUE",
        reason: "checkpoint_window_complete",
        remainingQueued: remaining,
      };
    } else if (processed >= maxCandidatesPerRun && remaining === 0) {
      state.overallStopReason = ORCHESTRATOR_STOP_REASONS.QUEUE_DRAINED;
    }
  }

  const segmentRuns = buildSegmentRunsFromCorpus(
    [...byId.values()],
    expectedSegments
  );
  // Enrich sufficiency signals
  for (const run of segmentRuns) {
    const suf = assessSegmentResearchSufficiency(run);
    run.sufficiency = suf;
    if (!suf.sufficient) {
      run.statusHint = "INSUFFICIENT_RESEARCH_DEPTH";
      run.stoppingReason = suf.reason;
    }
  }
  const coverage = segmentCoverageGate({ expectedSegments, segmentRuns });
  state.segmentCoverage = coverage;
  if (
    !coverage.pass &&
    state.overallStopReason === ORCHESTRATOR_STOP_REASONS.QUEUE_DRAINED
  ) {
    state.overallStopReason = ORCHESTRATOR_STOP_REASONS.DISCOVERY_INCOMPLETE;
    state.nextRecommendedAction = {
      kind: "SEGMENT_DISCOVERY",
      reason: "queue_drained_but_coverage_incomplete",
      shallowSegments: (coverage.rows || [])
        .filter((r) => r.status === "INSUFFICIENT_RESEARCH_DEPTH")
        .map((r) => r.segment),
    };
  } else if (
    !coverage.pass &&
    (state.overallStopReason ===
      ORCHESTRATOR_STOP_REASONS.NO_JUSTIFIED_WORK_REMAINING ||
      !state.overallStopReason)
  ) {
    // Prefer discovery incomplete over "no work" when segments are shallow
    if (
      state.queuedIds.length === 0 &&
      state.pausedIds.length === 0
    ) {
      state.overallStopReason = ORCHESTRATOR_STOP_REASONS.DISCOVERY_INCOMPLETE;
    }
  }

  const allOps = [...byId.values()];
  const hotelGate = hotelCompletionGate({
    coverage,
    opportunities: allOps.filter(
      (o) =>
        o.customerVisible === true ||
        o.prospectHotelFacing === true ||
        o.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT
    ),
    minHotelFacingProspects: options.minHotelFacingProspects ?? 10,
    requireProductionDelivery: options.requireProductionDelivery !== false,
    delivery: options.delivery || {},
  });

  // Demand Segment Research Supervisor — independent durable segment ownership.
  // Opt-in via options.runSegmentSupervisor !== false (default on when stateDir set).
  let segmentSupervision = null;
  if (options.runSegmentSupervisor !== false) {
    try {
      const { coordinateHotelSegmentSupervision } = await import(
        "./demand-segment-research-supervisor-v1.js"
      );
      segmentSupervision = coordinateHotelSegmentSupervision({
        hotelId,
        opportunities: allOps,
        expectedSegments,
        stateDir:
          options.segmentStateDir ||
          path.join(
            options.root || process.cwd(),
            "data",
            "group-demand-intelligence",
            "hotels",
            hotelId,
            "segment-research-state"
          ),
        remainingBudgetUsd,
        paidBlocked,
      });
      state.segmentSupervision = {
        hotelAssessment: segmentSupervision.hotelAssessment,
        coverageRows: segmentSupervision.coverageRows,
        reopenCandidates: segmentSupervision.reopenPlan?.length || 0,
        schedule: (segmentSupervision.schedule || []).slice(0, 8),
      };
      // Prefer supervisor coverage when it is stricter than corpus heuristic
      if (
        segmentSupervision.coverage &&
        !segmentSupervision.coverage.pass &&
        state.overallStopReason === ORCHESTRATOR_STOP_REASONS.QUEUE_DRAINED
      ) {
        state.overallStopReason =
          ORCHESTRATOR_STOP_REASONS.DISCOVERY_INCOMPLETE;
        state.nextRecommendedAction = {
          kind: "SEGMENT_SUPERVISOR_REDISCOVERY",
          reason: "segment_supervisor_coverage_incomplete",
          shallowSegments: (segmentSupervision.coverageRows || [])
            .filter((r) => !SEGMENT_TERMINAL_OK.includes(r.status))
            .map((r) => r.segment),
        };
      }
    } catch (err) {
      state.history.push({
        at: now(),
        kind: "segment_supervisor_error",
        error: err?.message || String(err),
      });
    }
  }

  state.updatedAt = now();
  state.lease = { ...state.lease, releasedAt: now() };
  saveOrchestratorState(stateDir, state);

  const drained =
    state.overallStopReason === ORCHESTRATOR_STOP_REASONS.QUEUE_DRAINED ||
    state.overallStopReason ===
      ORCHESTRATOR_STOP_REASONS.NO_JUSTIFIED_WORK_REMAINING;

  return {
    status: drained
      ? "QUEUE_DRAINED"
      : ORCHESTRATOR_RESUMABLE_CONTINUATION.includes(state.overallStopReason)
        ? "CHECKPOINT_CONTINUE"
        : "PAUSED",
    stopReason: state.overallStopReason,
    version: ORCHESTRATOR_VERSION,
    hotelId,
    processed,
    matured,
    handoffs,
    state,
    coverage,
    hotelGate,
    segmentSupervision,
    opportunities: allOps,
    metrics: {
      corpusSize: corpus.length,
      facing:
        allOps.filter((o) => o.customerVisible || o.prospectHotelFacing)
          .length,
      advisoryPublishable: allOps.filter(
        (o) => o.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT
      ).length,
      completed: state.completedIds.length,
      paused: state.pausedIds.length,
      queued: state.queuedIds.length,
      discovered: state.discoveredIds.length,
      paidTasksUsed,
      segmentDiscoveryRounds,
    },
  };
}

export function isOrchestratorHotelComplete(result) {
  // Never claim Full E2E COMPLETE — only queue drained + research coverage hint
  return (
    result?.stopReason ===
      ORCHESTRATOR_STOP_REASONS.NO_JUSTIFIED_WORK_REMAINING &&
    result?.coverage?.pass === true
  );
}
