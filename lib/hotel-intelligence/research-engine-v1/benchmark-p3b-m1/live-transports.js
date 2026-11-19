/**
 * Live Exa / Parallel transports for P3B-M1 — wired to existing Dealality clients
 * but hard-gated: no network unless founder paid auth + explicit live network flag.
 * Search hits are discovery candidates only. Parallel Task supports create + poll/resume.
 */

import { quoteBenchmarkCall } from "../../../group-demand-intelligence/research/benchmark-provider-pricing-v1.js";
import { extractExaEvidence } from "../../../group-demand-intelligence/research/exa-benchmark-adapter-v1.js";
import { resolveParallelKey } from "../../research/providers/parallel/resolve-parallel-key.js";
import { PROVIDER_IDS } from "../provider-interface.js";
import { P3B_M1_FOUNDER_PAID_AUTH_ID } from "./contract.js";
import {
  toDiscoveryCandidate,
  extractParallelTaskDiscovery,
} from "./discovery-candidates.js";

const EXA_SEARCH_URL = "https://api.exa.ai/search";
const EXA_CONTENTS_URL = "https://api.exa.ai/contents";
const PARALLEL_API_BASE = "https://api.parallel.ai/v1";

/** Conservative Parallel Search unit (list ~$1/1k) — reserve for requested max, not returned count. */
export const PARALLEL_SEARCH_UNIT_USD = 0.001;
/** Optional model/orchestration headroom per call when unknown extras may apply. */
export const CONSERVATIVE_ORCHESTRATION_HEADROOM_USD = 0.002;

export const LIVE_TRANSPORT_GATE = Object.freeze({
  paid_authorized: false,
  live_network_default: false,
  note: "P3B_M1 proposed $5 NOT AUTHORIZED — transports refuse until founder auth + LIVE_NETWORK=1",
  expected_cost_not_max_guarantee: true,
});

export function probeAccountPricingPresence(env = process.env) {
  return {
    EXA: {
      keyPresent: Boolean(env.EXA_API_KEY && String(env.EXA_API_KEY).trim()),
      listPriceSource: "benchmark-provider-pricing-v1 exa lite/contents",
      accountBalanceChecked: false,
      accountBalanceStatus: "NOT_QUERIED_CREDENTIAL_SAFE",
      planEffectivePricingStatus: "LIST_PRICE_ASSUMPTION_ONLY",
    },
    PARALLEL: {
      keyPresent: Boolean(resolveParallelKey(env)),
      listPriceSource: "public Parallel fast ~$1/1k + core task $0.025",
      accountBalanceChecked: false,
      accountBalanceStatus: "NOT_QUERIED_CREDENTIAL_SAFE",
      planEffectivePricingStatus: "LIST_PRICE_ASSUMPTION_ONLY",
    },
    credentialsExposed: false,
    note: "List-price assumptions are NOT a guarantee of maximum charges",
  };
}

function assertLiveAllowed(opts = {}) {
  const auth = opts.founderAuth || {};
  const liveNetwork =
    opts.liveNetworkEnabled === true ||
    String(opts.env?.HE_INTEL_P3B_M1_LIVE_NETWORK || "0") === "1";
  const paidOk =
    auth.ok === true &&
    String(auth.authId || "") === P3B_M1_FOUNDER_PAID_AUTH_ID;
  if (!paidOk || !liveNetwork) {
    const err = new Error("P3B_M1_LIVE_NETWORK_DISABLED");
    err.code = "LIVE_TRANSPORT_BLOCKED";
    err.paidOk = paidOk;
    err.liveNetwork = liveNetwork;
    throw err;
  }
}

export function createExaLiveTransport(opts = {}) {
  const env = opts.env || process.env;
  const fetchImpl = opts.fetchImpl || null;
  return async function exaLiveTransport(req = {}) {
    assertLiveAllowed({ ...opts, env });
    const key = env.EXA_API_KEY && String(env.EXA_API_KEY).trim();
    if (!key) {
      const err = new Error("EXA_CREDENTIAL_ABSENT");
      err.code = "CREDENTIAL_ABSENT";
      throw err;
    }
    const fetchFn = fetchImpl || globalThis.fetch;
    if (typeof fetchFn !== "function") {
      const err = new Error("FETCH_UNAVAILABLE");
      err.code = "LIVE_TRANSPORT_BLOCKED";
      throw err;
    }

    // Inline contents on search is an extra billable surface — refuse unless separately budgeted
    if (req.params?.includeContents === true || req.params?.contents) {
      const err = new Error("EXA_INLINE_CONTENTS_REQUIRES_SEPARATE_BOUNDED_OPERATION");
      err.code = "COST_UNBOUNDED";
      throw err;
    }

    const op = req.operation === "contents" ? "contents" : "search";
    const params =
      op === "contents"
        ? {
            urls: (req.params?.urls || []).slice(0, 5),
            text: true,
            highlights: req.params?.highlights === true,
          }
        : {
            query:
              req.params?.query ||
              `${req.hotelName || ""} hotel owner ownership investor`.trim(),
            type: "lite",
            numResults: Math.min(10, req.params?.numResults || 8),
          };
    const quote = quoteBenchmarkCall({ provider: "exa", operation: op, params });
    if (!quote.bounded) {
      const err = new Error("EXA_COST_UNBOUNDED");
      err.code = "COST_UNBOUNDED";
      throw err;
    }
    const controller = new AbortController();
    const timeoutMs = Number(req.timeoutMs || opts.timeoutMs || 15000);
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const endpoint = op === "contents" ? EXA_CONTENTS_URL : EXA_SEARCH_URL;
    try {
      const response = await fetchFn(endpoint, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": key,
        },
        body: JSON.stringify(
          op === "contents"
            ? params
            : {
                query: params.query,
                type: "lite",
                numResults: params.numResults,
                // No contents payload — keeps search quote bounded
              }
        ),
        signal: controller.signal,
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        const err = new Error(`exa_http_${response.status}`);
        err.code = response.status === 429 ? "PROVIDER_RATE_LIMITED" : "PROVIDER_HTTP_ERROR";
        err.dispatched = true;
        err.ambiguous = response.status >= 500;
        throw err;
      }
      const extracted = extractExaEvidence(body, op);
      const reported = Number(body?.costDollars?.total);
      // Conservative settle: never under-report vs reserved quote
      const cost = Number.isFinite(reported)
        ? Math.max(reported, 0)
        : quote.maxUsd;
      if (Number.isFinite(reported) && reported > quote.maxUsd + 1e-9) {
        const err = new Error("EXA_ACTUAL_EXCEEDED_RESERVATION");
        err.code = "BILLING_UNKNOWN";
        err.ambiguous = true;
        err.dispatched = true;
        err.actualUsd = reported;
        throw err;
      }
      const facts = (extracted.candidates || []).map((c) =>
        toDiscoveryCandidate(
          { url: c.url, title: c.title, excerpt: c.excerpt },
          { hotelName: req.hotelName }
        )
      );
      return {
        ok: true,
        provider: PROVIDER_IDS.EXA,
        operation: req.operation || op,
        facts,
        discovery_candidates: facts,
        extracted,
        cost_usd: cost,
        cost_usd_status: Number.isFinite(reported) ? "REPORTED" : "ESTIMATED",
        provider_cost_usd: cost,
        model_token_cost_usd: 0,
        orchestration_cost_usd: 0,
        credits: 0,
        source_attribution: facts.map((f) => f.url).filter(Boolean),
        ownership_claims_verified: false,
      };
    } catch (err) {
      if (err?.name === "AbortError") {
        const timeout = new Error("exa_timeout");
        timeout.code = "PROVIDER_TIMEOUT";
        timeout.dispatched = true;
        timeout.ambiguous = true;
        throw timeout;
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  };
}

async function parallelFetch(fetchFn, key, path, { method = "GET", body, signal } = {}) {
  const response = await fetchFn(`${PARALLEL_API_BASE}${path}`, {
    method,
    headers: {
      "x-api-key": key,
      "content-type": "application/json",
    },
    body: body != null ? JSON.stringify(body) : undefined,
    signal,
  });
  const json = await response.json().catch(() => ({}));
  return { response, json };
}

export function createParallelLiveTransport(opts = {}) {
  const env = opts.env || process.env;
  const fetchImpl = opts.fetchImpl || null;
  const mode = opts.mode === "task" ? "task" : "search";

  return async function parallelLiveTransport(req = {}) {
    assertLiveAllowed({ ...opts, env });
    const key = resolveParallelKey(env);
    if (!key) {
      const err = new Error("PARALLEL_CREDENTIAL_ABSENT");
      err.code = "CREDENTIAL_ABSENT";
      throw err;
    }
    const fetchFn = fetchImpl || globalThis.fetch;
    if (typeof fetchFn !== "function") {
      const err = new Error("FETCH_UNAVAILABLE");
      err.code = "LIVE_TRANSPORT_BLOCKED";
      throw err;
    }

    const controller = new AbortController();
    const timeoutMs = Number(req.timeoutMs || opts.timeoutMs || 30000);
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      if (mode === "search") {
        const maxResults = Math.min(6, Number(req.params?.searches || req.params?.max_results || 6) || 6);
        const query =
          req.params?.query ||
          `${req.hotelName || ""} hotel owner ownership`.trim();
        const { response, json: body } = await parallelFetch(
          fetchFn,
          key,
          "/search",
          {
            method: "POST",
            body: {
              objective: query,
              processor: "base",
              max_results: maxResults,
            },
            signal: controller.signal,
          }
        );
        if (!response.ok) {
          const err = new Error(`parallel_search_http_${response.status}`);
          err.code = "PROVIDER_HTTP_ERROR";
          err.dispatched = true;
          err.ambiguous = response.status >= 500;
          throw err;
        }
        const results = Array.isArray(body?.results) ? body.results : [];
        const facts = results.slice(0, maxResults).map((r) =>
          toDiscoveryCandidate(
            {
              url: r.url,
              title: r.title,
              excerpt: String(r.excerpts?.[0] || r.excerpt || "").slice(0, 400),
            },
            { hotelName: req.hotelName }
          )
        );
        // Bill for requested max, not returned count (conservative)
        const cost = Number((maxResults * PARALLEL_SEARCH_UNIT_USD).toFixed(6));
        return {
          ok: true,
          provider: PROVIDER_IDS.PARALLEL_SEARCH,
          operation: req.operation || "search_fast",
          facts,
          discovery_candidates: facts,
          cost_usd: cost,
          cost_usd_status: "ESTIMATED",
          provider_cost_usd: cost,
          model_token_cost_usd: 0,
          orchestration_cost_usd: 0,
          credits: 0,
          source_attribution: facts.map((f) => f.url).filter(Boolean),
          ownership_claims_verified: false,
        };
      }

      // Task path — resume existing run_id instead of creating a duplicate paid task
      const processor = String(req.params?.processor || "core");
      if (processor !== "core" && processor !== "pro") {
        const err = new Error("PARALLEL_PROCESSOR_UNBOUNDED");
        err.code = "COST_UNBOUNDED";
        throw err;
      }
      const usd = processor === "pro" ? 0.1 : 0.025;
      const hotel = req.hotelName || "hotel";
      const existingRunId =
        req.params?.provider_run_id ||
        req.params?.run_id ||
        req.provider_run_id ||
        null;

      let runId = existingRunId;
      let created = false;
      if (!runId) {
        const { response, json: body } = await parallelFetch(
          fetchFn,
          key,
          "/tasks/runs",
          {
            method: "POST",
            body: {
              processor,
              input: `Identify the ultimate owner / controlling investor, legal ownership chain, named ownership decision-makers, and verified contact pathways for ${hotel}. Distinguish legal titleholder from economic owner. Cite sources. Do not invent contacts.`,
            },
            signal: controller.signal,
          }
        );
        if (!response.ok) {
          const err = new Error(`parallel_task_http_${response.status}`);
          err.code = "PROVIDER_HTTP_ERROR";
          err.dispatched = true;
          err.ambiguous = response.status >= 500;
          throw err;
        }
        runId = body?.run_id || body?.id || null;
        created = true;
        if (!runId) {
          const err = new Error("PARALLEL_TASK_MISSING_RUN_ID");
          err.code = "AMBIGUOUS_PROVIDER_OUTCOME";
          err.dispatched = true;
          err.ambiguous = true;
          throw err;
        }
      }

      // Poll status / result — distinguish create vs complete
      const poll = await parallelFetch(
        fetchFn,
        key,
        `/tasks/runs/${encodeURIComponent(runId)}`,
        { method: "GET", signal: controller.signal }
      );
      const status = String(
        poll.json?.status || poll.json?.run?.status || "unknown"
      ).toLowerCase();

      if (!poll.response.ok) {
        const err = new Error(`parallel_task_status_http_${poll.response.status}`);
        err.code = "PROVIDER_HTTP_ERROR";
        err.dispatched = true;
        err.ambiguous = true;
        throw err;
      }

      const terminalOk = ["completed", "succeeded", "success", "done"].includes(status);
      const terminalFail = ["failed", "error", "cancelled", "canceled"].includes(status);

      if (!terminalOk && !terminalFail) {
        // Still running — not a successful research outcome; do not create another task
        return {
          ok: false,
          provider: PROVIDER_IDS.PARALLEL_TASK,
          operation: req.operation || "task_core",
          facts: [],
          parallel_run: { run_id: runId, status, created },
          provider_run_id: runId,
          cost_usd: usd,
          cost_usd_status: "ESTIMATED",
          provider_cost_usd: usd,
          model_token_cost_usd: 0,
          orchestration_cost_usd: 0,
          credits: 0,
          task_created_only: true,
          task_completed: false,
          research_success: false,
          note: created
            ? "Task created; awaiting completion — not a successful ownership outcome"
            : "Resumed existing task; still in progress",
        };
      }

      if (terminalFail) {
        return {
          ok: false,
          provider: PROVIDER_IDS.PARALLEL_TASK,
          operation: req.operation || "task_core",
          facts: [],
          parallel_run: { run_id: runId, status, created },
          provider_run_id: runId,
          cost_usd: usd,
          cost_usd_status: "ESTIMATED",
          provider_cost_usd: usd,
          model_token_cost_usd: 0,
          orchestration_cost_usd: 0,
          credits: 0,
          task_created_only: false,
          task_completed: true,
          research_success: false,
        };
      }

      // Fetch result
      const resultResp = await parallelFetch(
        fetchFn,
        key,
        `/tasks/runs/${encodeURIComponent(runId)}/result`,
        { method: "GET", signal: controller.signal }
      );
      const output = resultResp.json?.output || resultResp.json?.result || resultResp.json || {};
      const extracted = extractParallelTaskDiscovery(output, { hotelName: hotel });

      return {
        ok: extracted.candidates.length > 0,
        provider: PROVIDER_IDS.PARALLEL_TASK,
        operation: req.operation || "task_core",
        facts: extracted.candidates,
        discovery_candidates: extracted.candidates,
        parallel_run: { run_id: runId, status, created },
        provider_run_id: runId,
        cost_usd: usd,
        cost_usd_status: "ESTIMATED",
        provider_cost_usd: usd,
        model_token_cost_usd: 0,
        orchestration_cost_usd: 0,
        credits: 0,
        source_attribution: extracted.candidates.map((c) => c.url).filter(Boolean),
        task_created_only: false,
        task_completed: true,
        ownership_claims_verified: false,
        research_success: extracted.candidates.length > 0,
        rawTextExcerpt: extracted.rawTextExcerpt,
        note: "Completed task output held as discovery candidates pending independent acceptance",
      };
    } catch (err) {
      if (err?.name === "AbortError") {
        const timeout = new Error("parallel_timeout");
        timeout.code = "PROVIDER_TIMEOUT";
        timeout.dispatched = true;
        timeout.ambiguous = true;
        throw timeout;
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  };
}

export function buildLiveCapableTransports(opts = {}) {
  const founderAuth = opts.founderAuth || { ok: false };
  const liveNetworkEnabled = opts.liveNetworkEnabled === true;
  const common = { founderAuth, liveNetworkEnabled, env: opts.env, fetchImpl: opts.fetchImpl };
  return {
    exa: createExaLiveTransport(common),
    parallelSearch: createParallelLiveTransport({ ...common, mode: "search" }),
    parallelTask: createParallelLiveTransport({ ...common, mode: "task" }),
    gate: {
      ...LIVE_TRANSPORT_GATE,
      paid_authorized: founderAuth.ok === true,
      live_network_enabled: liveNetworkEnabled,
    },
    pricingPresence: probeAccountPricingPresence(opts.env || process.env),
  };
}
