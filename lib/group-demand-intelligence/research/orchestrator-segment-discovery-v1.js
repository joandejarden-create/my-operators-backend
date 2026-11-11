/**
 * Bounded segment rediscovery for Hotel Research Orchestrator.
 * Prefer Event-to-Demand (source-grounded) then optional Parallel replay.
 * No hotel-specific fork — market packs + hotel config drive locality.
 */

import {
  runParallelDiscoveryStage,
  DISCOVERY_MODE,
} from "../workflow/parallel-discovery-adapter-v1.js";
import { canonicalizeSegmentId } from "./research-constants-v1.js";
import { runEventToDemandDiscovery } from "./event-to-demand/event-to-demand-discovery-v1.js";

/**
 * @param {object} args
 * @param {string} args.hotelId
 * @param {object} [args.hotel]
 * @param {object} args.coverage - segmentCoverageGate result
 * @param {object[]} [args.existingOpportunities]
 * @param {number} [args.remainingBudgetUsd]
 * @param {boolean} [args.authorizeLivePaid]
 * @param {string} [args.outDir]
 * @returns {Promise<{ candidates: object[], mode: string, shallowSegments: string[], note?: string }>}
 */
export async function runOrchestratorSegmentDiscovery({
  hotelId,
  hotel = {},
  coverage,
  existingOpportunities = [],
  remainingBudgetUsd = 0,
  authorizeLivePaid = false,
  outDir,
  maxNewCandidates = 12,
} = {}) {
  const shallowSegments = (coverage?.rows || [])
    .filter(
      (r) =>
        r.status === "INSUFFICIENT_RESEARCH_DEPTH" ||
        r.status === "PENDING" ||
        r.status === "IN_PROGRESS"
    )
    .map((r) => canonicalizeSegmentId(r.segment))
    .filter(Boolean);

  if (!shallowSegments.length) {
    return {
      candidates: [],
      mode: "noop",
      shallowSegments: [],
      note: "no_shallow_segments",
    };
  }

  // 1) Event-to-Demand (preferred — verified events → mechanisms → orgs)
  let e2d = null;
  try {
    e2d = await runEventToDemandDiscovery({
      hotelId,
      hotel,
      coverage,
      existingOpportunities,
      remainingBudgetUsd,
      authorizeLivePaid: false, // paid Parallel remains separate under hotel auth
      // Prefer live calendars with seed merge so rediscovery is not seed-only.
      // Held-out recall audits use discoveryMode:"live" explicitly.
      discoveryMode:
        process.env.GDI_E2D_DISCOVERY_MODE ||
        (authorizeLivePaid ? "seed_and_live" : "seed_and_live"),
      outDir: outDir ? `${outDir}/event-to-demand` : null,
      maxNewCandidates,
    });
  } catch (err) {
    e2d = {
      candidates: [],
      note: "event_to_demand_failed",
      error: err?.message || String(err),
    };
  }

  if ((e2d.candidates || []).length) {
    return {
      candidates: e2d.candidates,
      mode: `event_to_demand:${e2d.mode || "seed_public"}`,
      shallowSegments,
      eventToDemand: {
        eventsConsidered: e2d.eventsConsidered,
        breadth: e2d.breadth,
        metrics: e2d.metrics,
        signals: (e2d.signals || []).length,
        rejected: (e2d.rejected || []).length,
      },
      note: e2d.note,
    };
  }

  // 2) Fallback: existing Parallel discovery adapter (replay / optional live)
  const canLive =
    authorizeLivePaid === true &&
    Number(remainingBudgetUsd) > 0.05 &&
    String(process.env.GDI_PARALLEL_ENABLED || "").trim() === "1";

  const mode = canLive ? DISCOVERY_MODE.LIVE : DISCOVERY_MODE.REPLAY;

  try {
    const stage = await runParallelDiscoveryStage(hotelId, {
      mode,
      authorizePaidDiscovery: canLive,
      spendCapUsd: canLive
        ? Math.min(0.25, Number(remainingBudgetUsd) || 0)
        : 0,
      outDir,
      hotel,
      segments: shallowSegments,
    });

    const raw = [
      ...(stage?.candidates || []),
      ...(stage?.normalizedCandidates || []),
      ...(stage?.cards || []),
    ];
    const candidates = [];
    const seen = new Set();
    for (const c of raw) {
      const id = c?.id || c?.opportunityId;
      if (!id || seen.has(id)) continue;
      seen.add(id);
      const seg = canonicalizeSegmentId(
        c.segment || c.researchSegment || shallowSegments[0]
      );
      candidates.push({
        ...c,
        id,
        hotelId,
        segment: seg || c.segment,
        researchStatus: c.researchStatus || "IN_PROGRESS",
        notes: [
          ...(c.notes || []),
          `orchestrator_segment_discovery:${mode}`,
        ],
      });
      if (candidates.length >= maxNewCandidates) break;
    }

    return {
      candidates,
      mode,
      shallowSegments,
      eventToDemand: e2d
        ? {
            eventsConsidered: e2d.eventsConsidered,
            note: e2d.note,
            signals: (e2d.signals || []).length,
          }
        : null,
      stageStatus: stage?.status || stage?.mode || null,
      note:
        candidates.length === 0
          ? e2d?.note || "honest_zero_yield_or_replay_empty"
          : `added_up_to_${candidates.length}`,
    };
  } catch (err) {
    return {
      candidates: [],
      mode,
      shallowSegments,
      eventToDemand: e2d,
      note: "discovery_failed",
      error: err?.message || String(err),
    };
  }
}
