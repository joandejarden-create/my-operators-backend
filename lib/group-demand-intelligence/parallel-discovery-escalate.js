/**
 * Gated Parallel escalation for GDI discovery gaps.
 * Prefer targeted gap-fill over full hotel redo.
 */

import { createParallelClient } from "../hotel-intelligence/research/providers/parallel/client.js";
import {
  renderDiscoveryContractBrief,
} from "./discovery-contract.js";
import { buildParallelEscalationGaps } from "./discovery-completeness-gate.js";
import { mapNativeCandidateRow } from "./native-blind-discovery.js";

export const PARALLEL_GDI_ESCALATION_VERSION = "gdi_parallel_escalation_v1";

function parallelEnabled() {
  const v = String(process.env.GDI_PARALLEL_ENABLED || process.env.HOTEL_INTELLIGENCE_PARALLEL_ENABLED || "0");
  return v === "1" || v === "true";
}

function hasParallelKey() {
  return Boolean(
    String(process.env.PARALLEL_KEY || process.env.PARALLEL_API_KEY || "").trim()
  );
}

/**
 * @returns {{ skipped: boolean, reason?: string, rows?: object[], costUsd?: number, raw?: object }}
 */
export async function runParallelDiscoveryEscalation({
  contract,
  gate,
  existingCandidates = [],
  hotelId,
  processor = process.env.GDI_PARALLEL_PROCESSOR || "base",
  timeoutMs = Number(process.env.GDI_PARALLEL_TIMEOUT_MS || 180000),
} = {}) {
  if (!parallelEnabled()) {
    return {
      skipped: true,
      reason: "GDI_PARALLEL_ENABLED_not_set",
      rows: [],
      costUsd: 0,
    };
  }
  if (!hasParallelKey()) {
    return {
      skipped: true,
      reason: "parallel_key_missing",
      rows: [],
      costUsd: 0,
    };
  }

  const gaps = buildParallelEscalationGaps(gate, existingCandidates);
  if (!gaps.length) {
    return { skipped: true, reason: "no_escalate_gaps", rows: [], costUsd: 0 };
  }

  const brief = renderDiscoveryContractBrief(contract, { gaps });
  const existingTitles = existingCandidates
    .slice(0, 25)
    .map((c) => `- ${c.title || c.eventName}`)
    .join("\n");

  const taskPrompt = `${brief}

EXISTING NATIVE CANDIDATES (do not discard; fill gaps / add only if material):
${existingTitles || "(none)"}

Return JSON object:
{ "candidates": [ { "eventName", "organization", "startDate", "endDate", "location", "venue", "venueStatus", "sourcingStatus", "attendance", "peakRoomEstimate", "roomDemandEvidence", "housingEvidence", "officialSource", "supportingSources", "whyRelevantToHotel", "whyNow", "whoClues", "opportunityType", "demandTerritoryFit" } ] }
Only include opportunities supported by sources. Prefer first-party URLs.`;

  const client = createParallelClient();
  const started = Date.now();
  let runId = null;
  let result = null;
  let costUsd = 0;

  try {
    const created = await client.createTaskRun({
      processor,
      input: taskPrompt,
      task_spec: {
        output_schema: {
          type: "json",
        },
      },
    });
    runId = created.structured?.run_id || created.structured?.id || created.structured?.run?.id;
    if (!runId) {
      return {
        skipped: false,
        ok: false,
        reason: "parallel_run_id_missing",
        rows: [],
        costUsd: 0,
        raw: created.structured,
      };
    }
    result = await client.pollUntilComplete(runId, { timeoutMs });
    const structured = result.structured || {};
    costUsd = Number(
      structured.cost?.total ||
        structured.usage?.cost_usd ||
        structured.run?.cost ||
        0
    );
    let output = structured.output || structured.result || structured;
    if (typeof output === "string") {
      try {
        output = JSON.parse(output);
      } catch {
        output = { raw: output };
      }
    }
    const list = Array.isArray(output?.candidates)
      ? output.candidates
      : Array.isArray(output)
        ? output
        : [];
    const rows = [];
    for (let i = 0; i < list.length; i += 1) {
      const mapped = mapNativeCandidateRow(
        { ...list[i], researchProvider: "parallel" },
        hotelId,
        `par_${i}`
      );
      if (mapped) {
        mapped.researchProvider = "parallel";
        mapped.discoverySource = "gdi_parallel_escalation_v1";
        rows.push(mapped);
      }
    }
    return {
      skipped: false,
      ok: true,
      runId,
      gaps,
      rows,
      costUsd,
      latencyMs: Date.now() - started,
      rawPreview: {
        candidateCount: rows.length,
        status: structured.status || null,
      },
    };
  } catch (err) {
    return {
      skipped: false,
      ok: false,
      reason: String(err?.code || err?.message || err).slice(0, 300),
      runId,
      gaps,
      rows: [],
      costUsd,
      latencyMs: Date.now() - started,
    };
  }
}
