/**
 * Firecrawl HTTPS adapter. Search, map, scrape, and parse use the v2 API.
 * The Firecrawl CLI is not required and is not invoked.
 */

import {
  assertBenchmarkEvaluationEnabled,
  readBenchmarkFlags,
  readProviderCredential,
} from "./benchmark-provider-flags-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import { getBenchmarkReservation } from "./benchmark-cohort-a-budget-v1.js";
import fs from "node:fs";
import path from "node:path";

const API_ROOT = "https://api.firecrawl.dev/v2";

function fail(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function excerpt(value) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, 400);
}

export function extractFirecrawlEvidence(body, operation) {
  if (operation === "search") {
    const rows = Array.isArray(body?.data) ? body.data : [];
    const first = rows[0] || {};
    const quote = [first.title, first.description, first.markdown].filter(Boolean).join("\n");
    const candidates = rows.slice(0, 8).map((row) => ({
      url: row?.url || null,
      title: row?.title || null,
      excerpt: excerpt([row?.title, row?.description, row?.markdown].filter(Boolean).join(" ")),
    })).filter((row) => row.url);
    return {
      sourceUrl: first.url || null,
      title: first.title || null,
      evidenceQuote: quote,
      language: first.language || null,
      resultCount: rows.length,
      extractionOk: Boolean(first.url && quote.trim()),
      operation,
      candidates,
    };
  }
  if (operation === "map") {
    const links = body?.links || body?.data?.links || [];
    const quote = Array.isArray(links) ? links.slice(0, 20).join("\n") : "";
    return {
      sourceUrl: links[0] || null,
      title: null,
      evidenceQuote: quote,
      language: null,
      resultCount: Array.isArray(links) ? links.length : 0,
      extractionOk: Array.isArray(links) && links.length > 0,
      operation,
    };
  }
  const data = body?.data || {};
  const quote = data.markdown || data.content || "";
  return {
    sourceUrl: data.metadata?.sourceURL || data.metadata?.url || null,
    title: data.metadata?.title || null,
    evidenceQuote: quote,
    language: data.metadata?.language || null,
    resultCount: quote ? 1 : 0,
    extractionOk: Boolean(quote.trim()),
    operation,
  };
}

export async function executeFirecrawlBenchmarkCall({
  operation,
  params = {},
  reservation,
  ledgerPath,
  env = process.env,
  fetchImpl = null,
  timeoutMs = 20000,
} = {}) {
  assertBenchmarkEvaluationEnabled(env);
  const quote = quoteBenchmarkCall({ provider: "firecrawl", operation, params });
  if (!quote.bounded) throw fail(quote.reason, "firecrawl_call_unpriced");
  const key = readProviderCredential("firecrawl", env);
  if (!key) throw fail("CREDENTIAL_ABSENT", "firecrawl_credential_absent");
  const flags = readBenchmarkFlags(env);
  const fetchFn = fetchImpl || (flags.liveEnabled ? globalThis.fetch : null);
  if (!fetchFn) throw fail("LIVE_NETWORK_DISABLED", "firecrawl_live_network_disabled");
  const held = getBenchmarkReservation(reservation?.reservationId, { ledgerPath, env });
  if (!held || held.status !== "reserved" || held.provider !== "firecrawl") {
    throw fail("RESERVATION_REQUIRED", "firecrawl_reservation_required");
  }
  if (quote.maxUsd > Number(held.reservedUsd) + 1e-9) {
    throw fail("RESERVATION_TOO_SMALL", "firecrawl_reservation_below_quote");
  }

  const payload =
    operation === "search"
      ? { query: params.query, limit: params.limit || 10 }
      : operation === "map"
        ? { url: params.url }
        : operation === "parse"
          ? null
          : { url: params.url, formats: ["markdown"] };

  let bodyInit = JSON.stringify(payload);
  const headers = {
    authorization: `Bearer ${key}`,
  };
  if (operation === "parse") {
    const filePath = String(params.filePath || "");
    if (!fs.existsSync(filePath)) throw fail("PARSE_FILE_MISSING", "firecrawl_parse_file_missing");
    const bytes = fs.readFileSync(filePath);
    if (bytes.length > 50 * 1024 * 1024) {
      throw fail("PARSE_FILE_TOO_LARGE", "firecrawl_parse_file_too_large");
    }
    const form = new FormData();
    form.append("file", new Blob([bytes]), path.basename(filePath));
    const parseOptions = { formats: ["markdown"] };
    if (path.extname(filePath).toLowerCase() === ".pdf") {
      parseOptions.parsers = [{ type: "pdf", mode: "auto", maxPages: params.maxPages }];
    }
    form.append("options", JSON.stringify(parseOptions));
    bodyInit = form;
  } else {
    headers["content-type"] = "application/json";
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetchFn(`${API_ROOT}/${operation}`, {
      method: "POST",
      headers,
      body: bodyInit,
      signal: controller.signal,
    });
    const body = await response.json().catch(() => ({}));
    if (response.status === 429) {
      const err = fail("PROVIDER_RATE_LIMITED", "firecrawl_rate_limited");
      err.dispatched = true;
      err.actualUsd = held.reservedUsd;
      throw err;
    }
    if (!response.ok || body.success === false) {
      const err = fail("PROVIDER_HTTP_ERROR", `firecrawl_http_${response.status}`);
      err.dispatched = true;
      err.actualUsd = held.reservedUsd;
      throw err;
    }
    const extracted = extractFirecrawlEvidence(body, operation);
    if (operation === "parse") {
      extracted.citation = `file:${path.basename(String(params.filePath || ""))}`;
    } else if (extracted.sourceUrl) {
      extracted.citation = extracted.sourceUrl;
    }
    return {
      provider: "firecrawl",
      operation,
      requestId: body.id || null,
      extracted,
      actualUsd: held.reservedUsd,
      estimatedUsd: quote.maxUsd,
      reservedUsd: held.reservedUsd,
    };
  } catch (err) {
    if (err?.code) throw err;
    if (err?.name === "AbortError") {
      const timeout = fail("PROVIDER_TIMEOUT", "firecrawl_timeout");
      timeout.dispatched = true;
      timeout.actualUsd = held.reservedUsd;
      throw timeout;
    }
    const wrapped = fail("PROVIDER_REQUEST_FAILED", "firecrawl_request_failed");
    wrapped.dispatched = true;
    wrapped.actualUsd = held.reservedUsd;
    throw wrapped;
  } finally {
    clearTimeout(timer);
  }
}
