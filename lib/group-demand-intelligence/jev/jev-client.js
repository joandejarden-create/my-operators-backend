/**
 * TypeSafe System One HTTP client for GDI (normalized).
 * Contract mirrors Market Alerts Jev benchmarks — do not invent endpoints.
 */

import crypto from "crypto";
import {
  getJevApiKey,
  getJevEndpoint,
  getJevModel,
  getJevTimeoutMs,
  DEFAULT_CIRCUIT_BREAKER_ERRORS,
} from "./jev-config.js";

let consecutiveErrors = 0;
let circuitOpen = false;

export function resetJevCircuitBreaker() {
  consecutiveErrors = 0;
  circuitOpen = false;
}

export function getJevCircuitState() {
  return {
    open: circuitOpen,
    consecutiveErrors,
    threshold: DEFAULT_CIRCUIT_BREAKER_ERRORS,
  };
}

function openCircuitIfNeeded() {
  consecutiveErrors += 1;
  if (consecutiveErrors >= DEFAULT_CIRCUIT_BREAKER_ERRORS) {
    circuitOpen = true;
  }
}

function noteSuccess() {
  consecutiveErrors = 0;
  circuitOpen = false;
}

/**
 * @param {{ state: object, questions: object, model?: string, timeoutMs?: number }} opts
 */
export async function callSystemOne({
  state,
  questions,
  model = getJevModel(),
  timeoutMs = getJevTimeoutMs(),
} = {}) {
  if (circuitOpen) {
    const err = new Error("jev_circuit_open");
    err.code = "jev_circuit_open";
    throw err;
  }
  const key = getJevApiKey();
  if (!key) {
    const err = new Error("jev_api_key_missing");
    err.code = "jev_api_key_missing";
    throw err;
  }
  const endpoint = getJevEndpoint();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const t0 = Date.now();
  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${key}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ model, state, questions }),
      signal: controller.signal,
    });
    const text = await res.text();
    const latencyMs = Date.now() - t0;
    let body;
    try {
      body = text ? JSON.parse(text) : {};
    } catch {
      body = { raw: text };
    }
    if (res.status === 429 || res.status === 529) {
      openCircuitIfNeeded();
      const err = new Error(`jev_rate_limited_${res.status}`);
      err.code = "jev_rate_limited";
      err.status = res.status;
      err.latencyMs = latencyMs;
      throw err;
    }
    if (!res.ok) {
      openCircuitIfNeeded();
      const err = new Error(`jev_http_${res.status}`);
      err.code = "jev_http_error";
      err.status = res.status;
      err.latencyMs = latencyMs;
      err.bodyPreview = String(text || "").slice(0, 300);
      throw err;
    }
    noteSuccess();
    return {
      model: body.model || model,
      answers: body.answers || {},
      usage: body.usage || {},
      latencyMs,
      providerRequestId:
        body.id || body.request_id || body.requestId || null,
      rawCost:
        typeof body.usage?.cost === "number" ? body.usage.cost : null,
      inputTokens: body.usage?.input_tokens || null,
    };
  } catch (err) {
    if (err && err.name === "AbortError") {
      openCircuitIfNeeded();
      const e = new Error("jev_timeout");
      e.code = "jev_timeout";
      e.latencyMs = Date.now() - t0;
      throw e;
    }
    if (err && !err.code) openCircuitIfNeeded();
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

export function hashJevInput(state, decisionType) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify({ decisionType, state }))
    .digest("hex")
    .slice(0, 24);
}

export function createDecisionId({ runId, decisionType, inputHash } = {}) {
  const base = `${runId || "norun"}|${decisionType || "x"}|${inputHash || crypto.randomBytes(6).toString("hex")}`;
  return `gdi_jev_${crypto.createHash("sha256").update(base).digest("hex").slice(0, 16)}`;
}
