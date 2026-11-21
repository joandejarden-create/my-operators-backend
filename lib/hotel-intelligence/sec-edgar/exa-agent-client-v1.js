/**
 * Exa Agent client — optional deep verification (not high-volume discovery).
 * Auth: EXA_API_KEY / EXA_KEY via x-api-key (same as Search client).
 * Docs: POST/GET https://api.exa.ai/agent/runs — async poll until terminal.
 *
 * Pricing (2026 Exa pricing page): fixed effort
 *   minimal $0.012 · low $0.025 · medium $0.10 · high $0.50 · xhigh $1.00
 *   auto/ultra metered (avoid in this pilot unless explicitly authorized)
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export const EXA_AGENT_CLIENT_V1 = "exa-agent-client-v1";
export const EXA_AGENT_RUNS_URL = "https://api.exa.ai/agent/runs";

export const EXA_AGENT_EFFORT_COST_USD = Object.freeze({
  minimal: 0.012,
  low: 0.025,
  medium: 0.1,
  high: 0.5,
  xhigh: 1.0,
  // Metered — reserve conservatively; actual from usage when reported
  auto: 0.5,
  ultra: 2.0,
});

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function loadExaAgentConfig(env = process.env) {
  const apiKey = String(env.EXA_API_KEY || env.EXA_KEY || "").trim();
  return {
    enabled: Boolean(apiKey),
    apiKeyPresent: Boolean(apiKey),
    apiKeyLen: apiKey.length,
    cacheDir:
      String(env.EXA_AGENT_CACHE_DIR || "").trim() ||
      path.join(process.cwd(), "data", "sec-edgar", "exa-agent-cache"),
    timeoutMs: Math.max(10000, Number(env.EXA_AGENT_TIMEOUT_MS || 120000) || 120000),
    pollIntervalMs: Math.max(2000, Number(env.EXA_AGENT_POLL_MS || 4000) || 4000),
    maxPolls: Math.max(5, Number(env.EXA_AGENT_MAX_POLLS || 45) || 45),
  };
}

export function createExaAgentClient(opts = {}) {
  const env = opts.env || process.env;
  const config = { ...loadExaAgentConfig(env), ...opts };
  const apiKey = String(env.EXA_API_KEY || env.EXA_KEY || "").trim();
  const budget = opts.budget || null;
  const stats = { create: 0, get: 0, cacheHits: 0, errors: 0, status429: 0, status402: 0 };

  function ensureCache() {
    fs.mkdirSync(config.cacheDir, { recursive: true });
  }

  function cachePath(kind, payload) {
    const h = crypto.createHash("sha1").update(JSON.stringify({ kind, payload })).digest("hex");
    return path.join(config.cacheDir, `${kind}_${h}.json`);
  }

  function readCache(kind, payload) {
    if (opts.skipCache) return null;
    const p = cachePath(kind, payload);
    if (!fs.existsSync(p)) return null;
    try {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      stats.cacheHits += 1;
      return raw;
    } catch {
      return null;
    }
  }

  function writeCache(kind, payload, result) {
    if (opts.skipCacheWrite) return;
    ensureCache();
    fs.writeFileSync(
      cachePath(kind, payload),
      JSON.stringify({ cachedAt: Date.now(), kind, payload, result }, null, 2),
      "utf8",
    );
  }

  function headers() {
    return {
      "content-type": "application/json",
      "x-api-key": apiKey,
      Authorization: `Bearer ${apiKey}`,
    };
  }

  async function createRun(params = {}) {
    if (!apiKey) return { ok: false, error: "EXA_API_KEY_ABSENT", status: 0 };
    const effort = String(params.effort || "low");
    const reserve = EXA_AGENT_EFFORT_COST_USD[effort] ?? EXA_AGENT_EFFORT_COST_USD.low;
    const caseId = params.caseId || null;
    const body = {
      query: String(params.query || "").trim(),
      effort,
    };
    if (params.systemPrompt) body.systemPrompt = String(params.systemPrompt);
    if (params.outputSchema) body.outputSchema = params.outputSchema;
    if (params.previousRunId) body.previousRunId = params.previousRunId;
    if (!body.query) return { ok: false, error: "QUERY_REQUIRED", status: 0 };

    const cached = readCache("create_run", body);
    if (cached?.result?.ok && cached.result.run?.status === "completed") {
      return { ...cached.result, fromCache: true, caseId };
    }

    if (budget) {
      const gate = budget.canSpend(reserve, caseId);
      if (!gate.ok) return { ok: false, error: gate.error, status: 0, budget: gate, reserveUsd: reserve };
    }

    stats.create += 1;
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), config.timeoutMs);
    try {
      const res = await fetch(EXA_AGENT_RUNS_URL, {
        method: "POST",
        headers: headers(),
        body: JSON.stringify(body),
        signal: ac.signal,
      });
      clearTimeout(timer);
      const json = await res.json().catch(() => ({}));
      if (res.status === 401 || res.status === 403) {
        stats.errors += 1;
        return { ok: false, status: res.status, error: "EXA_AGENT_AUTH_FAILED", body: json };
      }
      if (res.status === 402) {
        stats.status402 += 1;
        return { ok: false, status: 402, error: "EXA_AGENT_BUDGET_EXCEEDED", body: json };
      }
      if (res.status === 429) {
        stats.status429 += 1;
        return { ok: false, status: 429, error: "EXA_AGENT_CONCURRENCY_LIMIT", body: json };
      }
      if (!res.ok) {
        stats.errors += 1;
        return { ok: false, status: res.status, error: `EXA_AGENT_HTTP_${res.status}`, body: json };
      }
      return {
        ok: true,
        status: res.status,
        run: json,
        runId: json.id || json.runId || null,
        reserveUsd: reserve,
        effort,
        fromCache: false,
      };
    } catch (err) {
      clearTimeout(timer);
      stats.errors += 1;
      return { ok: false, status: 0, error: "EXA_AGENT_CREATE_FAILED", message: String(err?.message || err) };
    }
  }

  async function getRun(runId) {
    if (!apiKey) return { ok: false, error: "EXA_API_KEY_ABSENT", status: 0 };
    if (!runId) return { ok: false, error: "RUN_ID_REQUIRED", status: 0 };
    stats.get += 1;
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), config.timeoutMs);
    try {
      const res = await fetch(`${EXA_AGENT_RUNS_URL}/${encodeURIComponent(runId)}`, {
        method: "GET",
        headers: headers(),
        signal: ac.signal,
      });
      clearTimeout(timer);
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        stats.errors += 1;
        return { ok: false, status: res.status, error: `EXA_AGENT_HTTP_${res.status}`, body: json };
      }
      return { ok: true, status: res.status, run: json };
    } catch (err) {
      clearTimeout(timer);
      stats.errors += 1;
      return { ok: false, status: 0, error: "EXA_AGENT_GET_FAILED", message: String(err?.message || err) };
    }
  }

  async function pollUntilFinished(runId, pollOpts = {}) {
    const maxPolls = pollOpts.maxPolls ?? config.maxPolls;
    const interval = pollOpts.pollIntervalMs ?? config.pollIntervalMs;
    let last = null;
    for (let i = 0; i < maxPolls; i++) {
      last = await getRun(runId);
      if (!last.ok) return last;
      const st = String(last.run?.status || "").toLowerCase();
      if (["completed", "failed", "cancelled", "canceled", "error"].includes(st)) {
        return last;
      }
      await sleep(interval);
    }
    return {
      ok: false,
      error: "EXA_AGENT_POLL_TIMEOUT",
      runId,
      last,
    };
  }

  /**
   * Create + poll. Records budget on completion using reported cost or effort reserve.
   */
  async function research(params = {}) {
    const caseId = params.caseId || null;
    const createPayload = {
      query: params.query,
      systemPrompt: params.systemPrompt,
      effort: params.effort || "low",
      outputSchema: params.outputSchema,
      caseId,
    };
    const created = await createRun(createPayload);
    if (!created.ok) return created;
    if (created.fromCache) {
      const run = created.run || {};
      return {
        ok: String(run.status || "completed").toLowerCase() === "completed",
        status: 200,
        runId: created.runId || run.id || null,
        effort: createPayload.effort,
        run,
        structured: run?.output?.structured || null,
        text: run?.output?.text || null,
        citations: run?.citations || run?.output?.citations || [],
        spentUsd: 0,
        fromCache: true,
        error: null,
      };
    }

    const runId = created.runId;
    if (!runId) return { ok: false, error: "EXA_AGENT_NO_RUN_ID", created };

    const finished = await pollUntilFinished(runId, params);
    if (!finished.ok) return { ...finished, created };

    const st = String(finished.run?.status || "").toLowerCase();
    const usageCost =
      Number(finished.run?.costDollars?.total) ||
      Number(finished.run?.usage?.costDollars) ||
      Number(finished.run?.cost) ||
      null;
    const usd = Number.isFinite(usageCost) ? usageCost : created.reserveUsd;

    if (budget && st === "completed") {
      budget.record({
        provider: "EXA_AGENT",
        operation: "agent_run",
        usd,
        caseId,
        fingerprint: budget.fingerprint?.("EXA_AGENT", "agent_run", {
          query: createPayload.query,
          effort: createPayload.effort,
          outputSchema: createPayload.outputSchema,
        }),
        note: `runId=${runId};effort=${created.effort};status=${st}`,
      });
    }

    const result = {
      ok: st === "completed",
      status: finished.status,
      runId,
      effort: created.effort,
      run: finished.run,
      structured: finished.run?.output?.structured || finished.run?.outputStructured || null,
      text: finished.run?.output?.text || finished.run?.outputText || null,
      citations: finished.run?.citations || finished.run?.output?.citations || [],
      spentUsd: usd,
      error: st !== "completed" ? finished.run?.error || st : null,
    };

    if (result.ok) writeCache("create_run", createPayload, { ok: true, run: finished.run, runId });
    return result;
  }

  return {
    version: EXA_AGENT_CLIENT_V1,
    config: { ...config, apiKey: undefined },
    stats: () => ({ ...stats }),
    createRun,
    getRun,
    pollUntilFinished,
    research,
    effortCostUsd: EXA_AGENT_EFFORT_COST_USD,
  };
}

export default { createExaAgentClient, loadExaAgentConfig, EXA_AGENT_EFFORT_COST_USD };
