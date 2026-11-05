/**
 * Exa Agent provider adapter V1 — Scaling Companies.
 *
 * Default: mock / fail-closed. Live HTTP requires explicit dual authorization:
 *   opts.authorizeExaAgentLive === true AND process.env.GDI_EXA_AGENT_LIVE === "1"
 *
 * Uses raw HTTPS to api.exa.ai/agent/runs (exa-js not required / not installed).
 * Does not invent search-style parameters for Agent.
 */
import { randomBytes } from "node:crypto";
import {
  buildExaAgentQuery,
  getExaAgentOutputSchema,
  EXA_AGENT_DISCOVERY_CONTRACT_V1,
  EXA_AGENT_DISCOVERY_CONTRACT_V2,
  EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE,
  EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1,
} from "./exa-agent-contracts-v1.js";

import { quoteBenchmarkCall } from "../benchmark-provider-pricing-v1.js";

export const EXA_AGENT_PROVIDER_ADAPTER_V1 =
  "gdi-scaling-exa-agent-provider-adapter-v1";

export const EXA_AGENT_EFFORT = Object.freeze({
  MINIMAL: "minimal",
  LOW: "low",
  MEDIUM: "medium",
  HIGH: "high",
  XHIGH: "xhigh",
  AUTO: "auto",
  MAX: "max",
});

const FIXED_EFFORT_USD = Object.freeze({
  minimal: 0.012,
  low: 0.025,
  medium: 0.1,
  high: 0.5,
  xhigh: 1.0,
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function isExaAgentLiveAuthorized(opts = {}) {
  return (
    opts.authorizeExaAgentLive === true &&
    String(process.env.GDI_EXA_AGENT_LIVE || "") === "1"
  );
}

/**
 * Prefer fixed effort for bounded reservation quotes.
 * auto/max require budget.maxCostDollars and are treated as ceiling-only.
 */
export function quoteExaAgentRun({
  effort = EXA_AGENT_EFFORT.MEDIUM,
  maxCostDollars = null,
} = {}) {
  const e = String(effort || "").toLowerCase();
  if (FIXED_EFFORT_USD[e] != null) {
    return quoteBenchmarkCall({
      provider: "exa",
      operation: "agent_run",
      params: { effort: e },
    });
  }
  if (e === "auto" || e === "max" || e === "ultra") {
    const ceiling = Number(maxCostDollars);
    if (!Number.isFinite(ceiling) || ceiling < 1 || ceiling > 100) {
      return {
        bounded: false,
        maxUsd: null,
        reason: "EXA_AGENT_AUTO_REQUIRES_MAX_COST_DOLLARS_1_TO_100",
        asOf: "2026-10-28",
      };
    }
    return {
      bounded: true,
      maxUsd: Number(ceiling.toFixed(6)),
      assumption: `${e} metered run capped at budget.maxCostDollars=${ceiling}; ACU=$0.10 search=$0.005 enrichment separate`,
      sources: ["https://exa.ai/docs/reference/agent-api-guide"],
      asOf: "2026-10-28",
      isCeilingNotFixedPrice: true,
    };
  }
  return {
    bounded: false,
    maxUsd: null,
    reason: "EXA_AGENT_EFFORT_UNKNOWN",
    asOf: "2026-10-28",
  };
}

export function buildExaAgentCreatePayload({
  mode,
  contextBlock = "",
  effort = EXA_AGENT_EFFORT.MEDIUM,
  maxCostDollars = null,
  inputData = null,
  exclusion = null,
  previousRunId = null,
} = {}) {
  const query = buildExaAgentQuery({ mode, contextBlock });
  const outputSchema = getExaAgentOutputSchema(mode);
  const body = {
    query,
    effort,
    outputSchema,
  };
  if (previousRunId) body.previousRunId = previousRunId;
  if (inputData || exclusion) {
    body.input = {};
    if (Array.isArray(inputData) && inputData.length) body.input.data = inputData;
    if (Array.isArray(exclusion) && exclusion.length) {
      // Exa Agent expects exclusion items as records/objects, not bare strings.
      body.input.exclusion = exclusion.map((item) => {
        if (item && typeof item === "object" && !Array.isArray(item)) return item;
        const s = String(item || "").trim();
        return s ? { id: s, name: s } : null;
      }).filter(Boolean);
    }
  }
  const e = String(effort).toLowerCase();
  if ((e === "auto" || e === "max" || e === "ultra") && maxCostDollars != null) {
    body.budget = { maxCostDollars: Number(maxCostDollars) };
  }
  return body;
}

function mockRunId() {
  return `agent_run_mock_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
}

/**
 * Execute Agent research. Live path refuses unless dual-authorized.
 * Mock path returns fixture structured output without HTTP.
 */
export async function runExaAgentResearch({
  mode,
  contextBlock = "",
  effort = EXA_AGENT_EFFORT.MEDIUM,
  maxCostDollars = null,
  inputData = null,
  exclusion = null,
  previousRunId = null,
  mockStructured = null,
  mockStatus = "completed",
  authorizeExaAgentLive = false,
  fetchImpl = null,
  pollUntilFinished = true,
  pollIntervalMs = 4000,
  maxPolls = 60,
  /** When set, use this create body instead of Scaling contract builder (Events path). */
  createPayloadOverride = null,
  /** When set, stamp this contract id (e.g. gdi-exa-events-discovery-v1). */
  contractIdOverride = null,
  /** Hard block live HTTP even if env flags set — used by zero-paid FullE2E mocks. */
  forceMockOnly = false,
} = {}) {
  const quote = quoteExaAgentRun({ effort, maxCostDollars });
  const payload =
    createPayloadOverride && typeof createPayloadOverride === "object"
      ? createPayloadOverride
      : buildExaAgentCreatePayload({
          mode,
          contextBlock,
          effort,
          maxCostDollars,
          inputData,
          exclusion,
          previousRunId,
        });
  const contractId =
    text(contractIdOverride) ||
    (mode === "discovery"
      ? EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE
      : EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1);

  const liveOk =
    !forceMockOnly && isExaAgentLiveAuthorized({ authorizeExaAgentLive });

  const baseMeta = {
    version: EXA_AGENT_PROVIDER_ADAPTER_V1,
    contractId,
    mode,
    effort,
    quote,
    liveAuthorized: liveOk,
    forceMockOnly: forceMockOnly === true,
    payloadOverrideUsed: Boolean(createPayloadOverride),
  };

  if (!liveOk) {
    if (mockStructured == null && mockStatus === "completed") {
      return {
        ...baseMeta,
        status: "refused_live_not_authorized",
        ok: false,
        runId: null,
        structured: null,
        text: null,
        grounding: null,
        costDollars: null,
        note: forceMockOnly
          ? "forceMockOnly=true — live Agent HTTP disabled for this run."
          : "Zero-paid default. Pass authorizeExaAgentLive + GDI_EXA_AGENT_LIVE=1 for live Agent calls.",
      };
    }
    // Mock / fixture path — never hits api.exa.ai
    const runId = mockRunId();
    if (mockStatus === "failed") {
      return {
        ...baseMeta,
        ok: false,
        status: "failed",
        runId,
        structured: null,
        error: { code: "MOCK_FAILED", message: "Simulated Agent failure" },
        costDollars: { total: 0 },
      };
    }
    if (mockStatus === "cancelled") {
      return {
        ...baseMeta,
        ok: false,
        status: "cancelled",
        runId,
        structured: null,
        costDollars: { total: 0 },
      };
    }
    if (mockStructured && typeof mockStructured !== "object") {
      return {
        ...baseMeta,
        ok: false,
        status: "failed",
        runId,
        structured: null,
        error: { code: "MOCK_MALFORMED", message: "mockStructured must be object" },
      };
    }
    // Simulate async Agent shape: create → completed poll result
    return {
      ...baseMeta,
      ok: true,
      status: "completed",
      runId,
      structured: mockStructured,
      text: null,
      grounding: mockStructured?.grounding || null,
      costDollars: { total: 0, mocked: true },
      stopReason: "schema_satisfied",
      provider: "exa_agent_mock",
      asyncShape: {
        createStatus: "running",
        polledStatus: "completed",
        note: "Mock mimics create+poll without network",
      },
    };
  }

  if (forceMockOnly) {
    return {
      ...baseMeta,
      ok: false,
      status: "refused_force_mock_only",
      structured: null,
      note: "Live path unreachable when forceMockOnly is set",
    };
  }

  if (!quote.bounded) {
    return {
      ...baseMeta,
      ok: false,
      status: "refused_unbounded_quote",
      reason: quote.reason,
      structured: null,
    };
  }

  const key = process.env.EXA_API_KEY || process.env.EXA_KEY;
  if (!key) {
    return {
      ...baseMeta,
      ok: false,
      status: "credential_absent",
      structured: null,
    };
  }

  const fetchFn = fetchImpl || globalThis.fetch;
  if (typeof fetchFn !== "function") {
    return {
      ...baseMeta,
      ok: false,
      status: "fetch_unavailable",
      structured: null,
    };
  }

  let created = {};
  let createRes;
  try {
    createRes = await fetchFn("https://api.exa.ai/agent/runs", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": key,
        Authorization: `Bearer ${key}`,
      },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    return {
      ...baseMeta,
      ok: false,
      status: "create_fetch_failed",
      error: String(err?.cause?.code || err?.code || err?.message || err),
      structured: null,
    };
  }
  try {
    created = await createRes.json();
  } catch {
    return {
      ...baseMeta,
      ok: false,
      status: "create_parse_failed",
      httpStatus: createRes.status,
      structured: null,
    };
  }
  if (!createRes.ok) {
    return {
      ...baseMeta,
      ok: false,
      status: `create_http_${createRes.status}`,
      error: created?.error || created,
      structured: null,
    };
  }
  const runId = created.id || created.runId;
  if (!runId) {
    return {
      ...baseMeta,
      ok: false,
      status: "create_missing_run_id",
      structured: null,
      raw: created,
    };
  }

  let finished = created;
  if (pollUntilFinished && !["completed", "failed", "cancelled"].includes(created.status)) {
    for (let i = 0; i < maxPolls; i += 1) {
      await new Promise((r) => setTimeout(r, pollIntervalMs));
      let pollRes;
      try {
        pollRes = await fetchFn(`https://api.exa.ai/agent/runs/${runId}`, {
          method: "GET",
          headers: { "x-api-key": key, Authorization: `Bearer ${key}` },
        });
      } catch (err) {
        // Transient poll timeout — retry until maxPolls; do not throw the whole wave.
        if (i === maxPolls - 1) {
          return {
            ...baseMeta,
            ok: false,
            status: "poll_fetch_failed",
            runId,
            error: String(err?.cause?.code || err?.code || err?.message || err),
            costDollars: finished?.costDollars || null,
            structured: null,
          };
        }
        continue;
      }
      try {
        finished = await pollRes.json();
      } catch {
        if (i === maxPolls - 1) {
          return {
            ...baseMeta,
            ok: false,
            status: "poll_parse_failed",
            runId,
            structured: null,
          };
        }
        continue;
      }
      if (["completed", "failed", "cancelled"].includes(finished.status)) break;
    }
  }

  if (finished.status !== "completed") {
    return {
      ...baseMeta,
      ok: false,
      status: finished.status || "not_completed",
      runId,
      error: finished.error || null,
      stopReason: finished.stopReason || null,
      costDollars: finished.costDollars || null,
      structured: null,
    };
  }

  return {
    ...baseMeta,
    ok: true,
    status: "completed",
    runId,
    structured: finished.output?.structured ?? null,
    text: finished.output?.text ?? null,
    grounding: finished.output?.grounding ?? null,
    costDollars: finished.costDollars || null,
    stopReason: finished.stopReason || null,
    usage: finished.usage || null,
    provider: "exa_agent",
  };
}

export async function cancelExaAgentRun(runId, { authorizeExaAgentLive = false, fetchImpl = null } = {}) {
  if (!isExaAgentLiveAuthorized({ authorizeExaAgentLive })) {
    return { ok: false, status: "refused_live_not_authorized" };
  }
  const key = process.env.EXA_API_KEY || process.env.EXA_KEY;
  if (!key || !text(runId)) return { ok: false, status: "invalid" };
  const fetchFn = fetchImpl || globalThis.fetch;
  const res = await fetchFn(`https://api.exa.ai/agent/runs/${runId}/cancel`, {
    method: "POST",
    headers: { "x-api-key": key, Authorization: `Bearer ${key}` },
  });
  return { ok: res.ok, status: res.status, runId };
}
