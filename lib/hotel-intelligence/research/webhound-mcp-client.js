/**
 * Packet 2.6C-R2 — Hosted Webhound MCP Streamable HTTP client (server-side only).
 * Mirrors WebhoundAI/dify-plugin-webhound WebhoundMCPClient.
 * Never logs WEBHOUND_KEY. Never call from browser.
 */

import crypto from "node:crypto";

export const WEBHOUND_MCP_ENDPOINT = "https://api.webhound.ai/api/v2/mcp";
export const WEBHOUND_MCP_PROTOCOL_VERSION = "2025-06-18";

function redact(text, secret) {
  let safe = String(text || "");
  if (secret) safe = safe.split(secret).join("[REDACTED]");
  return safe.slice(0, 500);
}

function joinTextBlocks(blocks) {
  const parts = [];
  for (const block of blocks || []) {
    if (block && block.type === "text" && typeof block.text === "string" && block.text) {
      parts.push(block.text);
    }
  }
  return parts.join("\n");
}

function parseSseEnvelope(body) {
  const candidates = [];
  let eventData = [];
  const flush = () => {
    if (!eventData.length) return;
    const raw = eventData.join("\n").trim();
    eventData = [];
    if (!raw || raw === "[DONE]") return;
    try {
      const decoded = JSON.parse(raw);
      if (decoded && typeof decoded === "object") candidates.push(decoded);
    } catch {
      /* ignore partial SSE frames */
    }
  };
  for (const line of String(body || "").split(/\r?\n/)) {
    if (!line) flush();
    else if (line.startsWith("data:")) eventData.push(line.slice(5).trimStart());
  }
  flush();
  for (let i = candidates.length - 1; i >= 0; i -= 1) {
    if ("result" in candidates[i] || "error" in candidates[i]) return candidates[i];
  }
  const err = new Error("webhound_mcp_incomplete_sse");
  err.code = "webhound_mcp_incomplete_sse";
  throw err;
}

function parseJsonRpcResponse(res, text) {
  const ct = String(res.headers.get("content-type") || "").toLowerCase();
  if (ct.includes("text/event-stream")) return parseSseEnvelope(text);
  let payload;
  try {
    payload = JSON.parse(text);
  } catch {
    const err = new Error("webhound_mcp_invalid_json");
    err.code = "webhound_mcp_invalid_json";
    throw err;
  }
  if (!payload || typeof payload !== "object") {
    const err = new Error("webhound_mcp_invalid_envelope");
    err.code = "webhound_mcp_invalid_envelope";
    throw err;
  }
  return payload;
}

export function resolveWebhoundKey(env = process.env) {
  return String(env.WEBHOUND_KEY || env.WEBHOUND_API_KEY || "").trim();
}

/**
 * @param {object} opts
 * @param {string} [opts.env]
 * @param {string} [opts.webhound_key]
 * @param {typeof fetch} [opts.fetchImpl]
 * @param {string} [opts.endpoint] — only for tests with injected fetch
 */
export function createWebhoundMcpClient(opts = {}) {
  const env = opts.env || process.env;
  const key = String(opts.webhound_key || resolveWebhoundKey(env)).trim();
  const endpoint = opts.endpoint || WEBHOUND_MCP_ENDPOINT;
  const fetchImpl = opts.fetchImpl || globalThis.fetch;

  if (!key) {
    const err = new Error("webhound_key_missing");
    err.code = "webhound_key_missing";
    err.customer_safe = "Live research is not configured.";
    throw err;
  }
  if (endpoint !== WEBHOUND_MCP_ENDPOINT && !opts.fetchImpl && !opts.allow_endpoint_override) {
    const err = new Error("webhound_endpoint_override_forbidden");
    err.code = "webhound_endpoint_override_forbidden";
    throw err;
  }

  async function callTool(toolName, arguments_ = {}, timeoutMs = 110000) {
    const requestId = crypto.randomUUID();
    const payload = {
      jsonrpc: "2.0",
      id: requestId,
      method: "tools/call",
      params: { name: toolName, arguments: { ...(arguments_ || {}) } },
    };
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    let res;
    let text;
    try {
      res = await fetchImpl(endpoint, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${key}`,
          "Content-Type": "application/json",
          Accept: "application/json, text/event-stream",
          "MCP-Protocol-Version": WEBHOUND_MCP_PROTOCOL_VERSION,
          "User-Agent": "dealality-hotel-intelligence-research/2.6c-r2",
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
        redirect: "manual",
      });
      text = await res.text();
    } catch (e) {
      const err = new Error(redact(e?.message || "webhound_unreachable", key));
      err.code = "webhound_unreachable";
      err.customer_safe = "Research provider could not be reached.";
      throw err;
    } finally {
      clearTimeout(timer);
    }

    if ([301, 302, 303, 307, 308].includes(res.status)) {
      const err = new Error("webhound_mcp_unexpected_redirect");
      err.code = "webhound_mcp_unexpected_redirect";
      err.customer_safe = "Research provider rejected the request.";
      throw err;
    }
    if (res.status === 401 || res.status === 403) {
      const err = new Error("webhound_key_rejected");
      err.code = "webhound_key_rejected";
      err.customer_safe = "Live research is not configured.";
      throw err;
    }
    if (res.status === 429) {
      const err = new Error("webhound_rate_limited");
      err.code = "webhound_rate_limited";
      err.customer_safe = "Research provider is busy. Try again shortly.";
      throw err;
    }
    if (res.status >= 500) {
      const err = new Error(`webhound_mcp_http_${res.status}`);
      err.code = "webhound_mcp_unavailable";
      err.customer_safe = "Research provider is temporarily unavailable.";
      throw err;
    }
    if (res.status >= 400) {
      const err = new Error(redact(text || `http_${res.status}`, key));
      err.code = "webhound_mcp_http_error";
      err.customer_safe = "Research could not be started.";
      throw err;
    }

    const envelope = parseJsonRpcResponse(res, text);
    if (envelope.id != null && envelope.id !== requestId) {
      const err = new Error("webhound_mcp_id_mismatch");
      err.code = "webhound_mcp_id_mismatch";
      throw err;
    }
    if (envelope.error) {
      const err = new Error(redact(envelope.error.message || "mcp_error", key));
      err.code = "webhound_mcp_error";
      err.customer_safe = "Research provider returned an error.";
      throw err;
    }
    const result = envelope.result;
    if (!result || typeof result !== "object") {
      const err = new Error("webhound_mcp_empty_result");
      err.code = "webhound_mcp_empty_result";
      throw err;
    }
    const content = Array.isArray(result.content) ? result.content : [];
    const summary = joinTextBlocks(content);
    if (result.isError) {
      const err = new Error(redact(summary || "tool_rejected", key));
      err.code = "webhound_tool_rejected";
      err.customer_safe = "Research could not be completed.";
      throw err;
    }
    const structured =
      result.structuredContent && typeof result.structuredContent === "object"
        ? { ...result.structuredContent }
        : { content };
    return { summary: summary || `${toolName} completed.`, structured };
  }

  return {
    callTool,
    startReport(args) {
      return callTool("webhound_start_report", args);
    },
    watch(sessionId) {
      return callTool("webhound_watch", { session_id: sessionId });
    },
    wait(sessionId, timeoutSeconds = 25) {
      return callTool("webhound_wait", {
        session_id: sessionId,
        timeout_seconds: timeoutSeconds,
      });
    },
    getEvidencePack(sessionId) {
      return callTool("webhound_get_evidence_pack", { session_id: sessionId });
    },
    getOutput(sessionId) {
      return callTool("webhound_get_output", { session_id: sessionId });
    },
    getSources(sessionId) {
      return callTool("webhound_get_sources", { session_id: sessionId });
    },
    getClaims(sessionId) {
      return callTool("webhound_get_claims", { session_id: sessionId });
    },
    account() {
      return callTool("webhound_account", {});
    },
  };
}
