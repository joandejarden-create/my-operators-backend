/**
 * Exa HTTPS adapter for the benchmark evaluation. The CLI is not used.
 * A call requires the evaluation flag, a credential, and an open ledger reservation.
 */

import {
  assertBenchmarkEvaluationEnabled,
  readBenchmarkFlags,
  readProviderCredential,
} from "./benchmark-provider-flags-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import { getBenchmarkReservation } from "./benchmark-cohort-a-budget-v1.js";

const SEARCH_URL = "https://api.exa.ai/search";
const CONTENTS_URL = "https://api.exa.ai/contents";

function fail(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function actualFromBody(body, reservedUsd) {
  const reported = Number(body?.costDollars?.total);
  if (Number.isFinite(reported) && reported >= 0 && reported <= reservedUsd + 1e-9) {
    return reported;
  }
  return reservedUsd;
}

function excerpt(value) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, 400);
}

export function extractExaEvidence(body, operation) {
  const results = Array.isArray(body?.results) ? body.results : [];
  const first = results[0] || {};
  const quote = [first.text, ...(first.highlights || [])].filter(Boolean).join("\n");
  const candidates = results.slice(0, 8).map((row) => ({
    url: row?.url || null,
    title: row?.title || null,
    excerpt: excerpt([row?.text, ...(row?.highlights || [])].filter(Boolean).join(" ")),
  })).filter((row) => row.url);
  return {
    sourceUrl: first.url || null,
    title: first.title || null,
    evidenceQuote: quote,
    language: null,
    resultCount: results.length,
    extractionOk: Boolean(first.url && quote.trim()),
    operation,
    candidates,
  };
}

export async function executeExaBenchmarkCall({
  operation,
  params = {},
  reservation,
  ledgerPath,
  env = process.env,
  fetchImpl = null,
  timeoutMs = 15000,
} = {}) {
  assertBenchmarkEvaluationEnabled(env);
  const quote = quoteBenchmarkCall({ provider: "exa", operation, params });
  if (!quote.bounded) throw fail(quote.reason, "exa_call_unpriced");
  const key = readProviderCredential("exa", env);
  if (!key) throw fail("CREDENTIAL_ABSENT", "exa_credential_absent");
  const flags = readBenchmarkFlags(env);
  const fetchFn = fetchImpl || (flags.liveEnabled ? globalThis.fetch : null);
  if (!fetchFn) throw fail("LIVE_NETWORK_DISABLED", "exa_live_network_disabled");
  const held = getBenchmarkReservation(reservation?.reservationId, { ledgerPath, env });
  if (!held || held.status !== "reserved" || held.provider !== "exa") {
    throw fail("RESERVATION_REQUIRED", "exa_reservation_required");
  }
  if (quote.maxUsd > Number(held.reservedUsd) + 1e-9) {
    throw fail("RESERVATION_TOO_SMALL", "exa_reservation_below_quote");
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const endpoint = operation === "contents" ? CONTENTS_URL : SEARCH_URL;
  const payload =
    operation === "contents"
      ? {
          urls: params.urls,
          text: params.text !== false,
          highlights: params.highlights === true,
        }
      : {
          query: params.query,
          type: "lite",
          numResults: params.numResults || 10,
          contents: { text: true },
        };
  try {
    const response = await fetchFn(endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": key,
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    const body = await response.json().catch(() => ({}));
    if (response.status === 429) {
      const err = fail("PROVIDER_RATE_LIMITED", "exa_rate_limited");
      err.dispatched = true;
      err.actualUsd = actualFromBody(body, held.reservedUsd);
      throw err;
    }
    if (!response.ok) {
      const err = fail("PROVIDER_HTTP_ERROR", `exa_http_${response.status}`);
      err.dispatched = true;
      err.actualUsd = actualFromBody(body, held.reservedUsd);
      throw err;
    }
    const extracted = extractExaEvidence(body, operation);
    return {
      provider: "exa",
      operation,
      requestId: body.requestId || null,
      extracted,
      actualUsd: actualFromBody(body, held.reservedUsd),
      estimatedUsd: quote.maxUsd,
      reservedUsd: held.reservedUsd,
    };
  } catch (err) {
    if (err?.code) throw err;
    if (err?.name === "AbortError") {
      const timeout = fail("PROVIDER_TIMEOUT", "exa_timeout");
      timeout.dispatched = true;
      timeout.actualUsd = held.reservedUsd;
      throw timeout;
    }
    const wrapped = fail("PROVIDER_REQUEST_FAILED", "exa_request_failed");
    wrapped.dispatched = true;
    wrapped.actualUsd = held.reservedUsd;
    throw wrapped;
  } finally {
    clearTimeout(timer);
  }
}
