/**
 * Parallel Task API client (server-side only).
 * Docs: https://docs.parallel.ai/task-api/task-quickstart
 * Never log API keys.
 */

import { resolveParallelKey } from "./resolve-parallel-key.js";

export const PARALLEL_API_BASE = "https://api.parallel.ai/v1";
export const PARALLEL_DEFAULT_TIMEOUT_MS = 3_600_000;
export const PARALLEL_POLL_INTERVAL_MS = 5_000;

function redact(text, secret) {
  let safe = String(text || "");
  if (secret) safe = safe.split(secret).join("[REDACTED]");
  return safe.slice(0, 800);
}

export function createParallelClient(opts = {}) {
  const env = opts.env || process.env;
  const apiKey = opts.api_key || resolveParallelKey(env);
  const fetchImpl = opts.fetchImpl || globalThis.fetch;
  const baseUrl = String(opts.baseUrl || PARALLEL_API_BASE).replace(/\/$/, "");

  if (!apiKey) {
    const err = new Error("parallel_key_missing");
    err.code = "parallel_key_missing";
    err.customer_safe = "Parallel research is not configured.";
    throw err;
  }

  async function request(method, path, body, extraHeaders = {}) {
    const url = `${baseUrl}${path}`;
    const started = Date.now();
    let res;
    let text = "";
    try {
      res = await fetchImpl(url, {
        method,
        headers: {
          "x-api-key": apiKey,
          "Content-Type": "application/json",
          ...extraHeaders,
        },
        body: body != null ? JSON.stringify(body) : undefined,
      });
      text = await res.text();
    } catch (err) {
      const e = new Error("parallel_network_error");
      e.code = "parallel_network_error";
      e.customer_safe = "Research provider network error.";
      e.cause = err;
      throw e;
    }

    let json = null;
    if (text) {
      try {
        json = JSON.parse(text);
      } catch {
        json = { raw_text: text };
      }
    }

    if (!res.ok) {
      const e = new Error(`parallel_http_${res.status}`);
      e.code = "parallel_http_error";
      e.status = res.status;
      e.customer_safe = "Research provider returned an error.";
      e.response = json;
      e.response_preview = redact(text, apiKey);
      throw e;
    }

    return {
      json,
      latency_ms: Date.now() - started,
      status: res.status,
    };
  }

  return {
    async createTaskRun(payload = {}) {
      const { json, latency_ms } = await request("POST", "/tasks/runs", payload);
      return { structured: json, latency_ms };
    },

    async getRunStatus(runId) {
      const { json, latency_ms } = await request("GET", `/tasks/runs/${encodeURIComponent(runId)}`);
      return { structured: json, latency_ms };
    },

    async getRunResult(runId, { timeoutMs = PARALLEL_DEFAULT_TIMEOUT_MS } = {}) {
      const started = Date.now();
      const { json, latency_ms } = await request(
        "GET",
        `/tasks/runs/${encodeURIComponent(runId)}/result`,
        null,
        timeoutMs ? { "parallel-timeout-ms": String(timeoutMs) } : {}
      );
      return {
        structured: json,
        latency_ms,
        total_wait_ms: Date.now() - started,
        blocking: true,
      };
    },

    async pollUntilComplete(runId, opts = {}) {
      const timeoutMs = Number(opts.timeoutMs || PARALLEL_DEFAULT_TIMEOUT_MS);
      const intervalMs = Number(opts.intervalMs || PARALLEL_POLL_INTERVAL_MS);
      const started = Date.now();
      let last = null;

      while (Date.now() - started < timeoutMs) {
        const statusResp = await this.getRunStatus(runId);
        last = statusResp.structured;
        const status = String(last?.status || last?.run?.status || "").toLowerCase();
        if (status === "completed") {
          return this.getRunResult(runId, { timeoutMs: Math.max(30_000, timeoutMs - (Date.now() - started)) });
        }
        if (status === "failed") {
          const err = new Error("parallel_task_failed");
          err.code = "parallel_task_failed";
          err.customer_safe = "Research provider task failed.";
          err.run = last;
          throw err;
        }
        await new Promise((r) => setTimeout(r, intervalMs));
      }

      const err = new Error("parallel_task_timeout");
      err.code = "parallel_task_timeout";
      err.customer_safe = "Research timed out.";
      err.last_status = last;
      throw err;
    },
  };
}
