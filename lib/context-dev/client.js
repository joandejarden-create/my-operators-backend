/**
 * Context.dev server-side wrapper — single entry point for all Context.dev calls.
 * Never import this from browser/public code. Never log or serialize the API key.
 *
 * Env: CONTEXT_DEV_API_KEY (required)
 * Docs: https://docs.context.dev
 */

import "dotenv/config";
import ContextDev, {
  APIError,
  RateLimitError,
  ContextDevError,
} from "context.dev";

export const CONTEXT_DEV_CLIENT_VERSION = "context-dev-client-v1";
export const CONTEXT_DEV_BASE_URL = "https://api.context.dev/v1";

/** Docs links for endpoints this wrapper exposes. */
export const CONTEXT_DEV_ENDPOINTS = Object.freeze({
  extract: {
    method: "POST",
    path: "/web/extract",
    credits: 10,
    docs: "https://docs.context.dev/api-reference/web-extraction/extract",
    why: "Structured ownership, executives, and published business emails from an org/hotel domain",
  },
  search: {
    method: "POST",
    path: "/web/search",
    credits: "1 per 10 results",
    docs: "https://docs.context.dev/api-reference/web-scraping/search",
    why: "Discover sourced ownership / leadership evidence URLs before scrape or extract",
  },
  scrapeMarkdown: {
    method: "GET",
    path: "/web/scrape/markdown",
    credits: 1,
    docs: "https://docs.context.dev/api-reference/web-scraping/markdown",
    why: "Clean Markdown for a known evidence page (governance, press, contact)",
  },
  brandRetrieve: {
    method: "POST",
    path: "/brand/retrieve",
    credits: 10,
    docs: "https://docs.context.dev/guides/get-brand-data",
    why: "Company / brand identity details when a domain or company identifier is known",
  },
});

/** Default JSON Schema for Dealality ownership / contact extraction. */
export const OWNERSHIP_CONTACT_EXTRACT_SCHEMA = Object.freeze({
  type: "object",
  properties: {
    company_legal_or_trade_name: {
      type: "string",
      description: "Primary company or hotel-owner organization name as stated on the site.",
    },
    company_description: {
      type: "string",
      description: "Short description of what the organization does, if stated.",
    },
    ownership_or_portfolio_claims: {
      type: "array",
      description: "Stated ownership, portfolio, or hotel-operation claims with source wording.",
      items: {
        type: "object",
        properties: {
          claim: { type: "string" },
          property_or_asset_name: { type: "string" },
          source_page_url: { type: "string" },
        },
        required: ["claim"],
        additionalProperties: false,
      },
    },
    executives: {
      type: "array",
      description: "Named executives or board members with titles as published.",
      items: {
        type: "object",
        properties: {
          full_name: { type: "string" },
          title: { type: "string" },
          role_category: {
            type: "string",
            description: "board | operating_executive | development_asset | other",
          },
          source_page_url: { type: "string" },
        },
        required: ["full_name", "title"],
        additionalProperties: false,
      },
    },
    business_emails: {
      type: "array",
      description: "Publicly published organization or role mailbox emails (not inferred).",
      items: {
        type: "object",
        properties: {
          email: { type: "string" },
          label: { type: "string" },
          source_page_url: { type: "string" },
        },
        required: ["email"],
        additionalProperties: false,
      },
    },
    business_phones: {
      type: "array",
      description: "Publicly published organization phones.",
      items: {
        type: "object",
        properties: {
          phone: { type: "string" },
          label: { type: "string" },
          source_page_url: { type: "string" },
        },
        required: ["phone"],
        additionalProperties: false,
      },
    },
    contact_page_urls: {
      type: "array",
      items: { type: "string" },
      description: "Contact or investor-relations page URLs found on the site.",
    },
  },
  required: ["company_legal_or_trade_name", "executives"],
  additionalProperties: false,
});

function getApiKey() {
  const key = String(process.env.CONTEXT_DEV_API_KEY || "").trim();
  if (!key) {
    throw new Error("CONTEXT_DEV_API_KEY missing from environment (server-side .env)");
  }
  return key;
}

export function isContextDevConfigured(env = process.env) {
  return Boolean(String(env.CONTEXT_DEV_API_KEY || "").trim());
}

export function redactSecrets(value) {
  if (value == null) return value;
  const key = process.env.CONTEXT_DEV_API_KEY;
  if (typeof value === "string") {
    return key && value.includes(key) ? "[REDACTED]" : value;
  }
  if (Array.isArray(value)) return value.map(redactSecrets);
  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (/authorization|api[_-]?key|bearer|secret|password|ctxt_secret/i.test(k)) {
        out[k] = "[REDACTED]";
      } else {
        out[k] = redactSecrets(v);
      }
    }
    return out;
  }
  return value;
}

export function safeErrorMessage(err) {
  let msg = err?.message || String(err || "unknown_error");
  const key = process.env.CONTEXT_DEV_API_KEY;
  if (key) msg = msg.split(key).join("[REDACTED]");
  return msg.replace(/ctxt_secret_[a-zA-Z0-9]+/g, "[REDACTED]");
}

let _client = null;

/**
 * Shared SDK client. Reads CONTEXT_DEV_API_KEY from the environment.
 * @param {{ maxRetries?: number, timeout?: number, forceNew?: boolean }} [opts]
 */
export function getContextDevClient(opts = {}) {
  if (_client && !opts.forceNew) return _client;
  const apiKey = getApiKey();
  _client = new ContextDev({
    apiKey,
    baseURL: process.env.CONTEXT_DEV_BASE_URL || CONTEXT_DEV_BASE_URL,
    // SDK retries timeouts/5xx by default; keep bounded.
    maxRetries: opts.maxRetries ?? 0,
    timeout: opts.timeout ?? 120_000,
  });
  return _client;
}

/**
 * Classify SDK / HTTP errors for callers (no key leakage).
 */
export function classifyContextDevError(err) {
  if (err instanceof RateLimitError || err?.status === 429) {
    const retryAfter = err?.headers?.["retry-after"] || err?.headers?.get?.("retry-after") || null;
    return {
      class: "RATE_LIMITED",
      status: 429,
      retry_after: retryAfter,
      message: safeErrorMessage(err),
      retryable: true,
    };
  }
  const status = err?.status ?? err?.statusCode ?? null;
  if (status === 408 || (status >= 500 && status < 600)) {
    return {
      class: "TRANSIENT_HTTP",
      status,
      message: safeErrorMessage(err),
      retryable: true,
    };
  }
  if (status >= 400 && status < 500) {
    return {
      class: "VALIDATION_OR_CLIENT",
      status,
      message: safeErrorMessage(err),
      retryable: false,
    };
  }
  if (err instanceof APIError || err instanceof ContextDevError) {
    return {
      class: "API_ERROR",
      status,
      message: safeErrorMessage(err),
      retryable: false,
    };
  }
  return {
    class: "UNKNOWN",
    status,
    message: safeErrorMessage(err),
    retryable: false,
  };
}

async function withContextDev(fn) {
  try {
    const data = await fn(getContextDevClient());
    return { ok: true, data, error: null };
  } catch (err) {
    return { ok: false, data: null, error: classifyContextDevError(err) };
  }
}

/**
 * POST /web/extract — structured ownership / contact extraction (10 credits).
 * @param {{ url: string, schema?: object, instructions?: string, maxPages?: number, maxDepth?: number, factCheck?: boolean, maxAgeMs?: number }} params
 */
export async function contextDevExtract(params = {}) {
  const url = String(params.url || "").trim();
  if (!url.startsWith("http")) {
    return {
      ok: false,
      data: null,
      error: { class: "VALIDATION_OR_CLIENT", status: null, message: "url_required", retryable: false },
    };
  }
  return withContextDev((client) =>
    client.web.extract({
      url,
      schema: params.schema || OWNERSHIP_CONTACT_EXTRACT_SCHEMA,
      instructions: params.instructions,
      maxPages: params.maxPages ?? 5,
      maxDepth: params.maxDepth,
      factCheck: params.factCheck ?? true,
      maxAgeMs: params.maxAgeMs,
      followSubdomains: params.followSubdomains === true,
    })
  );
}

/**
 * POST /web/search — discover evidence URLs (1 credit / 10 results).
 */
export async function contextDevSearch(params = {}) {
  const query = String(params.query || "").trim();
  if (!query) {
    return {
      ok: false,
      data: null,
      error: { class: "VALIDATION_OR_CLIENT", status: null, message: "query_required", retryable: false },
    };
  }
  return withContextDev((client) =>
    client.web.search({
      query,
      numResults: params.numResults ?? 10,
      country: params.country,
      // /web/search rejects maxAgeMs (INPUT_VALIDATION_ERROR) — do not pass it.
      includeDomains: params.includeDomains,
      excludeDomains: params.excludeDomains,
    })
  );
}

/**
 * GET /web/scrape/markdown — one URL → Markdown (1 credit).
 */
export async function contextDevScrapeMarkdown(params = {}) {
  const url = String(params.url || "").trim();
  if (!url.startsWith("http")) {
    return {
      ok: false,
      data: null,
      error: { class: "VALIDATION_OR_CLIENT", status: null, message: "url_required", retryable: false },
    };
  }
  return withContextDev((client) =>
    client.web.webScrapeMd({
      url,
      useMainContentOnly: params.useMainContentOnly !== false,
      maxAgeMs: params.maxAgeMs,
    })
  );
}

/**
 * POST /brand/retrieve — company/brand details (10 credits).
 */
export async function contextDevBrandRetrieve(params = {}) {
  return withContextDev((client) => {
    if (!client.brand?.retrieve) {
      throw new Error("context.dev SDK brand.retrieve unavailable in this version");
    }
    return client.brand.retrieve(params);
  });
}

/**
 * Dealality-oriented helper: extract ownership + executives + public business contacts
 * from a known first-party organization domain.
 */
export async function extractOwnerOrgIntelligence({
  url,
  instructions = null,
  maxPages = 5,
  factCheck = true,
  maxAgeMs = undefined,
} = {}) {
  const result = await contextDevExtract({
    url,
    schema: OWNERSHIP_CONTACT_EXTRACT_SCHEMA,
    instructions:
      instructions ||
      "Extract only facts published on this organization's first-party pages. Prefer gobierno corporativo / about / team / contact pages. Do not invent emails or titles. Distinguish board members from operating executives. Include source page URLs when available.",
    maxPages,
    maxDepth: 2,
    factCheck,
    maxAgeMs,
  });
  return {
    ...result,
    endpoint: CONTEXT_DEV_ENDPOINTS.extract,
    purpose: "owner_org_ownership_executives_contacts",
  };
}
