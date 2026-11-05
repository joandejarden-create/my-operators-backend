/**
 * Conservative list-price ceilings for the Cohort A evaluation.
 * Unknown or unbounded operations are refused before a reservation.
 *
 * Exa: https://exa.ai/docs/admin/pricing as of 2026-10-24.
 * Lite search is $4 / 1k requests for up to 10 results.
 * Contents is $1 / 1k pages per content type. Summaries are disabled here.
 *
 * Firecrawl: https://www.firecrawl.dev/pricing and https://docs.firecrawl.dev/billing
 * as of 2026-10-24. Search is 2 credits per 10 results. Scrape and map are 1 credit
 * at the markdown / single-call rate. Dollar conversion uses the Hobby overage
 * ceiling of $5 per 1,000 credits ($0.005). Included-plan credits can cost less.
 * This reservation uses the higher figure. Parse is 1 credit per PDF page when
 * maxPages is 1–10, and 1 credit for one HTML file. Crawl, interact, agent,
 * summaries, and JSON extraction are unbounded and are refused.
 */

import path from "node:path";

export const BENCHMARK_PRICE_AS_OF = "2026-10-24";

export const EXA_LITE_SEARCH_USD = 0.004;
export const EXA_CONTENT_TYPE_USD = 0.001;
export const FIRECRAWL_CREDIT_USD = 0.005;

const EXA_SOURCES = Object.freeze([
  "https://exa.ai/docs/admin/pricing",
]);
const FIRECRAWL_SOURCES = Object.freeze([
  "https://docs.firecrawl.dev/billing",
  "https://www.firecrawl.dev/pricing",
]);

function bounded(maxUsd, assumption, sources) {
  return {
    bounded: true,
    maxUsd: Number(maxUsd.toFixed(6)),
    assumption,
    sources,
    asOf: BENCHMARK_PRICE_AS_OF,
  };
}

function unbounded(reason) {
  return {
    bounded: false,
    maxUsd: null,
    reason,
    asOf: BENCHMARK_PRICE_AS_OF,
  };
}

function integerInRange(value, fallback, min, max) {
  const n = value == null ? fallback : Number(value);
  if (!Number.isInteger(n) || n < min || n > max) return null;
  return n;
}

export function quoteBenchmarkCall({ provider, operation, params = {} } = {}) {
  if (provider === "exa" && operation === "search") {
    if ((params.type || "lite") !== "lite") return unbounded("EXA_SEARCH_TYPE_NOT_BOUNDED");
    if (params.summary || params.livecrawl) return unbounded("EXA_OPTIONAL_CHARGE_NOT_BOUNDED");
    const n = integerInRange(params.numResults, 10, 1, 10);
    if (n == null) return unbounded("EXA_RESULT_COUNT_UNBOUNDED");
    return bounded(
      EXA_LITE_SEARCH_USD,
      "lite search, at most 10 results, cached text only, no summary",
      EXA_SOURCES
    );
  }

  if (provider === "exa" && operation === "contents") {
    const urls = Array.isArray(params.urls) ? params.urls : null;
    if (!urls || urls.length < 1 || urls.length > 5) {
      return unbounded("EXA_CONTENTS_URL_COUNT_UNBOUNDED");
    }
    if (params.summary) return unbounded("EXA_SUMMARY_DISABLED");
    const types = [];
    if (params.text !== false) types.push("text");
    if (params.highlights === true) types.push("highlights");
    if (!types.length) return unbounded("EXA_CONTENTS_TYPE_UNBOUNDED");
    return bounded(
      EXA_CONTENT_TYPE_USD * urls.length * types.length,
      `contents ${urls.length} url(s), types ${types.join("+")}`,
      EXA_SOURCES
    );
  }

  if (provider === "firecrawl" && operation === "search") {
    if (params.scrapeOptions || params.scrape === true) {
      return unbounded("FIRECRAWL_SEARCH_SCRAPE_UNBOUNDED");
    }
    const n = integerInRange(params.limit, 10, 1, 10);
    if (n == null) return unbounded("FIRECRAWL_RESULT_COUNT_UNBOUNDED");
    const credits = 2 * Math.ceil(n / 10);
    return bounded(
      FIRECRAWL_CREDIT_USD * credits,
      "search without page scrape, Hobby overage credit price",
      FIRECRAWL_SOURCES
    );
  }

  if (provider === "firecrawl" && operation === "scrape") {
    const formats = params.formats || ["markdown"];
    if (!Array.isArray(formats) || formats.length !== 1 || formats[0] !== "markdown") {
      return unbounded("FIRECRAWL_SCRAPE_OPTIONS_UNBOUNDED");
    }
    if (!params.url || !/^https:\/\//i.test(String(params.url))) {
      return unbounded("FIRECRAWL_SCRAPE_URL_REQUIRED");
    }
    return bounded(
      FIRECRAWL_CREDIT_USD,
      "one https page, markdown only, Hobby overage credit price",
      FIRECRAWL_SOURCES
    );
  }

  if (provider === "firecrawl" && operation === "parse") {
    const formats = params.formats || ["markdown"];
    if (!Array.isArray(formats) || formats.length !== 1 || formats[0] !== "markdown") {
      return unbounded("FIRECRAWL_PARSE_FORMAT_UNBOUNDED");
    }
    if (params.summary || params.question || params.redactPII || params.mode === "ocr") {
      return unbounded("FIRECRAWL_PARSE_OPTION_UNBOUNDED");
    }
    const filePath = String(params.filePath || "");
    const ext = path.extname(filePath).toLowerCase();
    if (ext === ".html" || ext === ".htm") {
      return bounded(
        FIRECRAWL_CREDIT_USD,
        "one local HTML file, markdown only, 1 credit",
        FIRECRAWL_SOURCES
      );
    }
    if (ext === ".pdf") {
      const pages = integerInRange(params.maxPages, null, 1, 10);
      if (pages == null) return unbounded("FIRECRAWL_PARSE_PAGE_COUNT_UNBOUNDED");
      return bounded(
        FIRECRAWL_CREDIT_USD * pages,
        `local PDF, markdown only, 1 credit per page up to ${pages}`,
        FIRECRAWL_SOURCES
      );
    }
    return unbounded("FIRECRAWL_PARSE_TYPE_UNBOUNDED");
  }

  if (provider === "firecrawl" && operation === "map") {
    if (!params.url || !/^https:\/\//i.test(String(params.url))) {
      return unbounded("FIRECRAWL_MAP_URL_REQUIRED");
    }
    return bounded(
      FIRECRAWL_CREDIT_USD,
      "one map call, Hobby overage credit price",
      FIRECRAWL_SOURCES
    );
  }

  // Exa Agent fixed efforts (docs 2026-10-28). auto/max/ultra are ceiling-quoted
  // in exa-agent-provider-adapter-v1 (require budget.maxCostDollars).
  if (provider === "exa" && operation === "agent_run") {
    const effort = String(params.effort || "").toLowerCase();
    const fixed = {
      minimal: 0.012,
      low: 0.025,
      medium: 0.1,
      high: 0.5,
      xhigh: 1.0,
    };
    if (fixed[effort] != null) {
      return bounded(
        fixed[effort],
        `exa agent fixed effort=${effort} (contact enrichment not included)`,
        ["https://exa.ai/docs/reference/agent-api-guide", ...EXA_SOURCES],
      );
    }
    return unbounded("EXA_AGENT_METERED_OR_UNKNOWN_EFFORT_USE_ADAPTER_QUOTE");
  }

  return unbounded("UNPRICED_OPERATION");
}
