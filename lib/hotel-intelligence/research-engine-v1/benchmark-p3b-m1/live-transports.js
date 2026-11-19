/**
 * Live Exa / Parallel transports for P3B-M1 — wired to existing Dealality clients
 * but hard-gated: no network unless founder paid auth + explicit live network flag.
 * Never bypasses the P1 execution gateway (transports are adapter internals only).
 */

import { quoteBenchmarkCall } from "../../../group-demand-intelligence/research/benchmark-provider-pricing-v1.js";
import { extractExaEvidence } from "../../../group-demand-intelligence/research/exa-benchmark-adapter-v1.js";
import { resolveParallelKey } from "../../research/providers/parallel/resolve-parallel-key.js";
import { PROVIDER_IDS } from "../provider-interface.js";
import { P3B_M1_FOUNDER_PAID_AUTH_ID } from "./contract.js";

const EXA_SEARCH_URL = "https://api.exa.ai/search";
const EXA_CONTENTS_URL = "https://api.exa.ai/contents";
const PARALLEL_API_BASE = "https://api.parallel.ai/v1";

export const LIVE_TRANSPORT_GATE = Object.freeze({
  paid_authorized: false,
  live_network_default: false,
  note: "P3B_M1 proposed $5 NOT AUTHORIZED — transports refuse until founder auth + LIVE_NETWORK=1",
});

/**
 * Presence-only plan/pricing probe — never returns credential values.
 */
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

function mapExaToFacts(extracted, hotelName) {
  const facts = [];
  for (const c of extracted?.candidates || []) {
    if (!c?.url) continue;
    facts.push({
      claimType: "OWNERSHIP_STRUCTURE_EDGE",
      value: c.title || c.excerpt || "exa_candidate",
      url: c.url,
      excerpt: c.excerpt || null,
      hotelHint: hotelName || null,
      isSeedFact: false,
      verified: false,
      note: "URL_AND_CONTENT_RETRIEVED_NOT_OWNERSHIP_VERIFIED",
    });
  }
  return facts;
}

/**
 * Build Exa transport that uses existing HTTPS surface (not CLI).
 * Inject fetchImpl for tests; live path still requires assertLiveAllowed.
 */
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
    const op = req.operation === "contents" ? "contents" : "search";
    const params =
      op === "contents"
        ? {
            urls: (req.params?.urls || []).slice(0, 5),
            text: true,
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
                contents: { text: true },
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
      const cost = Number.isFinite(reported) ? reported : quote.maxUsd;
      return {
        ok: true,
        provider: PROVIDER_IDS.EXA,
        operation: req.operation || op,
        facts: mapExaToFacts(extracted, req.hotelName),
        extracted,
        cost_usd: cost,
        cost_usd_status: Number.isFinite(reported) ? "REPORTED" : "ESTIMATED",
        provider_cost_usd: cost,
        model_token_cost_usd: 0,
        orchestration_cost_usd: 0,
        credits: 0,
        source_attribution: (extracted.candidates || [])
          .map((c) => c.url)
          .filter(Boolean),
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

/**
 * Parallel Search/Task transport — uses Parallel HTTP API when live-allowed.
 */
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
        const query =
          req.params?.query ||
          `${req.hotelName || ""} hotel owner ownership`.trim();
        const response = await fetchFn(`${PARALLEL_API_BASE}/search`, {
          method: "POST",
          headers: {
            "x-api-key": key,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            objective: query,
            processor: "base",
            max_results: Math.min(6, Number(req.params?.searches || 6) || 6),
          }),
          signal: controller.signal,
        });
        const body = await response.json().catch(() => ({}));
        if (!response.ok) {
          const err = new Error(`parallel_search_http_${response.status}`);
          err.code = "PROVIDER_HTTP_ERROR";
          err.dispatched = true;
          err.ambiguous = response.status >= 500;
          throw err;
        }
        const results = Array.isArray(body?.results) ? body.results : [];
        const facts = results.slice(0, 6).map((r) => ({
          claimType: "OWNERSHIP_STRUCTURE_EDGE",
          value: r.title || r.url || "parallel_search_hit",
          url: r.url || null,
          excerpt: String(r.excerpts?.[0] || r.excerpt || "").slice(0, 400),
          isSeedFact: false,
          verified: false,
          note: "URL_RETRIEVED_NOT_OWNERSHIP_VERIFIED",
        }));
        const n = Math.max(1, facts.length || 1);
        const cost = Number((n * 0.001).toFixed(6));
        return {
          ok: true,
          provider: PROVIDER_IDS.PARALLEL_SEARCH,
          operation: req.operation || "search_fast",
          facts,
          cost_usd: cost,
          cost_usd_status: "ESTIMATED",
          provider_cost_usd: cost,
          model_token_cost_usd: 0,
          orchestration_cost_usd: 0,
          credits: 0,
          source_attribution: facts.map((f) => f.url).filter(Boolean),
        };
      }

      // Task path
      const processor = String(req.params?.processor || "core");
      const usd = processor === "pro" ? 0.1 : 0.025;
      const hotel = req.hotelName || "hotel";
      const response = await fetchFn(`${PARALLEL_API_BASE}/tasks/runs`, {
        method: "POST",
        headers: {
          "x-api-key": key,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          processor,
          input: `Identify the ultimate owner / controlling investor, legal ownership chain, named ownership decision-makers, and verified contact pathways for ${hotel}. Distinguish legal titleholder from economic owner. Cite sources. Do not invent contacts.`,
        }),
        signal: controller.signal,
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        const err = new Error(`parallel_task_http_${response.status}`);
        err.code = "PROVIDER_HTTP_ERROR";
        err.dispatched = true;
        err.ambiguous = response.status >= 500;
        throw err;
      }
      return {
        ok: true,
        provider: PROVIDER_IDS.PARALLEL_TASK,
        operation: req.operation || "task_core",
        facts: [],
        parallel_run: {
          run_id: body?.run_id || body?.id || null,
          status: body?.status || "created",
        },
        cost_usd: usd,
        cost_usd_status: "ESTIMATED",
        provider_cost_usd: usd,
        model_token_cost_usd: 0,
        orchestration_cost_usd: 0,
        credits: 0,
        source_attribution: [],
        note: "Task created; ownership claims require result poll + evidence adjudication",
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

/**
 * Build adapter transports: live-capable but default-blocked.
 * For verification without spend, inject fetchImpl mocks.
 */
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
