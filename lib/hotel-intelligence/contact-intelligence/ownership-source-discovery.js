/**
 * Provider-neutral ownership source discovery.
 * Context.dev = SERP + scrape. Parallel = Task API research (different request format;
 * logical queries preserved). Does not rewrite the ownership reader.
 */
import { ownershipQueries, ownershipSearchResults, rankOwnershipSourcesDetailed } from "./ownership-research-planning.js";
import { createOwnershipStageTrace, pushStageEvent } from "./ownership-research-stage-trace.js";
import { CONTEXT_DEV_CREDIT_COSTS } from "../../context-dev/credit-ledger.js";

export const OWNERSHIP_DISCOVERY_PROVIDERS = Object.freeze({
  CONTEXT_DEV: "context_dev",
  PARALLEL: "parallel",
});

export const OWNERSHIP_DISCOVERY_VERSION = "ownership-source-discovery-v1";

function nz(v) {
  return String(v == null ? "" : v).trim();
}

/**
 * @param {object} args
 * @param {object} args.hotel - identity only
 * @param {string[]} [args.queries] - logical queries (defaults to ownershipQueries)
 * @param {string} args.provider - context_dev | parallel
 * @param {object} [args.budget]
 * @param {object} [args.trace] - stage trace (mutated)
 * @param {object} [args.deps] - injectable search/scrape/parallel for offline
 */
export async function discoverOwnershipSources({
  hotel = {},
  queries = null,
  provider,
  budget = {},
  trace = null,
  deps = {},
} = {}) {
  const logicalQueries = Array.isArray(queries) && queries.length ? queries : ownershipQueries(hotel);
  const stageTrace = trace || createOwnershipStageTrace(hotel);
  const providerId = String(provider || "").toLowerCase();

  if (providerId === OWNERSHIP_DISCOVERY_PROVIDERS.CONTEXT_DEV) {
    return discoverViaContextDev({ hotel, logicalQueries, budget, stageTrace, deps });
  }
  if (providerId === OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL) {
    return discoverViaParallel({ hotel, logicalQueries, budget, stageTrace, deps });
  }
  throw new Error(`OWNERSHIP_DISCOVERY_UNKNOWN_PROVIDER:${provider}`);
}

async function discoverViaContextDev({ hotel, logicalQueries, budget, stageTrace, deps }) {
  const searchFn = deps.contextDevSearch || deps.search;
  const scrapeFn = deps.contextDevScrapeMarkdown || deps.scrape;
  const maxCredits = Number(budget.context_dev_max ?? budget.max_credits ?? 4);
  let spent = 0;
  const calls = [];
  const allRaw = [];
  const documents = [];
  const searches = [];

  if (!searchFn || !scrapeFn) {
    return finalizeDiscovery({
      provider: OWNERSHIP_DISCOVERY_PROVIDERS.CONTEXT_DEV,
      hotel,
      logicalQueries,
      queries_executed: [],
      request_parameters: { numResults: 10 },
      raw_results: [],
      ranked: [],
      rejected: [],
      documents: [],
      cost: { context_dev_credits: 0, parallel_usd: 0 },
      stop_reason: "PROVIDER_DEPS_MISSING",
      stageTrace,
      calls,
      format_note: "Context.dev search+scrape; queries are exact SERP strings",
    });
  }

  const queryCap = Math.min(logicalQueries.length, Number(budget.max_queries ?? 2));
  for (const q of logicalQueries.slice(0, queryCap)) {
    const searchCost = CONTEXT_DEV_CREDIT_COSTS.search_per_10_results;
    if (spent + searchCost > maxCredits) {
      pushStageEvent(stageTrace, {
        kind: "budget",
        stage: "BUDGET",
        remaining_budget: maxCredits - spent,
        note: "search_skipped_budget",
      });
      break;
    }
    spent += searchCost;
    let searchResult;
    try {
      searchResult = await searchFn({ query: q, numResults: 10 });
    } catch (err) {
      calls.push({ kind: "search", query: q, ok: false, error: String(err?.message || err) });
      continue;
    }
    const mapped = searchResult?.ok !== false ? ownershipSearchResults(searchResult.data ?? searchResult) : null;
    calls.push({
      kind: "search",
      query: q,
      ok: mapped !== null,
      result_count: mapped?.length ?? 0,
    });
    pushStageEvent(stageTrace, {
      kind: "search",
      stage: "OWNERSHIP_SOURCE_RETRIEVAL",
      provider: OWNERSHIP_DISCOVERY_PROVIDERS.CONTEXT_DEV,
      query: q,
      request_parameters: { numResults: 10 },
      raw_result_count: mapped?.length ?? 0,
      ok: mapped !== null,
    });
    if (!mapped) continue;
    allRaw.push(...mapped.map((r) => ({ ...r, query: q })));
    searches.push({ query: q, results: mapped });
  }

  const detail = rankOwnershipSourcesDetailed(allRaw, hotel);
  pushStageEvent(stageTrace, {
    kind: "search",
    stage: "RESULT_RANKING_AND_FILTERING",
    provider: OWNERSHIP_DISCOVERY_PROVIDERS.CONTEXT_DEV,
    candidate_urls_before_filter: detail.candidate_urls_before_filter,
    urls_removed: (detail.rejected || []).map((r) => ({
      url: r.url,
      reject_reason: r.reject_reason,
      score: r.score,
    })),
    urls_kept: (detail.ranked || []).map((r) => ({ url: r.url, score: r.score, rank_reasons: r.rank_reasons })),
  });

  const docsPerHotel = Number(budget.max_documents ?? 2);
  for (const u of (detail.ranked || []).slice(0, docsPerHotel)) {
    const scrapeCost = CONTEXT_DEV_CREDIT_COSTS.scrape_markdown;
    if (spent + scrapeCost > maxCredits) break;
    spent += scrapeCost;
    let scraped;
    try {
      scraped = await scrapeFn({ url: u.url });
    } catch (err) {
      documents.push({ url: u.url, ok: false, characters: 0, error: String(err?.message || err), markdown: "" });
      pushStageEvent(stageTrace, {
        kind: "fetch",
        stage: "PAGE_FETCHING_AND_DOCUMENT_PARSING",
        url: u.url,
        ok: false,
        attempted: true,
        characters: 0,
        error: String(err?.message || err),
      });
      continue;
    }
    const md = String(
      typeof scraped?.data === "string"
        ? scraped.data
        : scraped?.data?.markdown || scraped?.data?.content || scraped?.markdown || ""
    );
    const ok = scraped?.ok !== false && Boolean(md);
    documents.push({ url: u.url, ok, characters: md.length, markdown: md, title: u.title || null });
    pushStageEvent(stageTrace, {
      kind: "fetch",
      stage: "PAGE_FETCHING_AND_DOCUMENT_PARSING",
      url: u.url,
      ok,
      attempted: true,
      characters: md.length,
    });
    calls.push({ kind: "scrape", url: u.url, ok, characters: md.length });
  }

  let stop_reason = "DISCOVERY_COMPLETE";
  if (!allRaw.length) stop_reason = "NO_SEARCH_RESULT";
  else if (!(detail.ranked || []).length) stop_reason = "RESULT_FILTERED";
  else if (!documents.some((d) => d.ok && d.characters > 0)) stop_reason = "FETCH_FAILED";

  return finalizeDiscovery({
    provider: OWNERSHIP_DISCOVERY_PROVIDERS.CONTEXT_DEV,
    hotel,
    logicalQueries,
    queries_executed: searches.map((s) => s.query),
    request_parameters: { numResults: 10, max_credits: maxCredits },
    raw_results: allRaw,
    ranked: detail.ranked || [],
    rejected: detail.rejected || [],
    documents,
    cost: { context_dev_credits: spent, parallel_usd: 0 },
    stop_reason,
    stageTrace,
    calls,
    format_note: "Context.dev: logical queries === executed SERP query strings",
  });
}

async function discoverViaParallel({ hotel, logicalQueries, budget, stageTrace, deps }) {
  const parallelFn = deps.parallelDiscover || deps.runParallelOwnershipTask;
  const maxUsd = Number(budget.parallel_usd_max ?? budget.max_usd ?? 0.1);
  const processor = budget.parallel_processor || "pro";

  // Parallel Task API does not take SERP query strings; record logical queries for parity.
  const request_parameters = {
    api: "Task API POST /v1/tasks/runs",
    processor,
    blind_mode: true,
    template_id: "FULL_HOTEL_INTELLIGENCE",
    hotel_seed_fields: ["hotel_id", "hotel_name", "city", "country", "language"],
    logical_queries_recorded_not_executed_as_serp: logicalQueries,
    format_difference:
      "Parallel receives a compiled hotel research task (seed + objective), not per-query SERP strings. Logical ownershipQueries are preserved for side-by-side comparison.",
  };

  pushStageEvent(stageTrace, {
    kind: "search",
    stage: "OWNERSHIP_SOURCE_RETRIEVAL",
    provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
    query: logicalQueries[0] || null,
    request_parameters,
    note: "parallel_task_not_serp",
  });

  if (!parallelFn) {
    return finalizeDiscovery({
      provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
      hotel,
      logicalQueries,
      queries_executed: [],
      request_parameters,
      raw_results: [],
      ranked: [],
      rejected: [],
      documents: [],
      cost: { context_dev_credits: 0, parallel_usd: 0 },
      stop_reason: "PROVIDER_DEPS_MISSING",
      stageTrace,
      calls: [],
      format_note: request_parameters.format_difference,
      parallel_artifact: null,
    });
  }

  if (maxUsd < 0.01) {
    return finalizeDiscovery({
      provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
      hotel,
      logicalQueries,
      queries_executed: [],
      request_parameters,
      raw_results: [],
      ranked: [],
      rejected: [],
      documents: [],
      cost: { context_dev_credits: 0, parallel_usd: 0 },
      stop_reason: "BUDGET_EXHAUSTED",
      stageTrace,
      calls: [],
      format_note: request_parameters.format_difference,
      parallel_artifact: null,
    });
  }

  const hotelSeed = {
    hotel_id: hotel.hotel_id,
    hotel_name: hotel.hotel_name,
    city: hotel.city,
    country: hotel.country,
    language: hotel.language,
    aliases: hotel.aliases || [],
  };

  let artifact;
  try {
    artifact = await parallelFn({
      hotel_seed: hotelSeed,
      logical_queries: logicalQueries,
      processor,
      budget_usd: maxUsd,
    });
  } catch (err) {
    return finalizeDiscovery({
      provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
      hotel,
      logicalQueries,
      queries_executed: [],
      request_parameters,
      raw_results: [],
      ranked: [],
      rejected: [],
      documents: [],
      cost: { context_dev_credits: 0, parallel_usd: 0 },
      stop_reason: "PARALLEL_TASK_FAILED",
      stageTrace,
      calls: [{ kind: "parallel_task", ok: false, error: String(err?.message || err) }],
      format_note: request_parameters.format_difference,
      parallel_artifact: null,
    });
  }

  const costUsd = Number(artifact?.cost_usd ?? artifact?.budget?.spent_usd ?? 0.1);
  const sourceDocs = extractParallelDocuments(artifact);
  const raw_results = sourceDocs.map((d) => ({
    title: d.title || "",
    url: d.url,
    snippet: String(d.passage || d.markdown || "").slice(0, 240),
    provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
  }));
  const detail = rankOwnershipSourcesDetailed(raw_results, hotel);

  pushStageEvent(stageTrace, {
    kind: "search",
    stage: "RESULT_RANKING_AND_FILTERING",
    provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
    candidate_urls_before_filter: detail.candidate_urls_before_filter,
    urls_removed: (detail.rejected || []).map((r) => ({
      url: r.url,
      reject_reason: r.reject_reason,
      score: r.score,
    })),
    urls_kept: (detail.ranked || []).map((r) => ({ url: r.url, score: r.score })),
  });

  const documents = sourceDocs.map((d) => ({
    url: d.url,
    ok: Boolean(d.markdown || d.passage),
    characters: String(d.markdown || d.passage || "").length,
    markdown: d.markdown || d.passage || "",
    title: d.title || null,
    provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
  }));

  for (const d of documents) {
    pushStageEvent(stageTrace, {
      kind: "fetch",
      stage: "PAGE_FETCHING_AND_DOCUMENT_PARSING",
      url: d.url,
      ok: d.ok,
      attempted: true,
      characters: d.characters,
      provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
      note: "parallel_embedded_passage_not_fresh_scrape",
    });
  }

  let stop_reason = "DISCOVERY_COMPLETE";
  if (!raw_results.length && !artifact?.ownership_claims?.length) stop_reason = "NO_SEARCH_RESULT";
  else if (!documents.some((d) => d.ok)) stop_reason = "NO_OWNERSHIP_PASSAGE";

  return finalizeDiscovery({
    provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
    hotel,
    logicalQueries,
    queries_executed: [], // not SERP-executed
    request_parameters,
    raw_results,
    ranked: detail.ranked || [],
    rejected: detail.rejected || [],
    documents,
    cost: { context_dev_credits: 0, parallel_usd: costUsd },
    stop_reason,
    stageTrace,
    calls: [{ kind: "parallel_task", ok: true, cost_usd: costUsd }],
    format_note: request_parameters.format_difference,
    parallel_artifact: sanitizeParallelArtifact(artifact),
  });
}

function extractParallelDocuments(artifact = {}) {
  const docs = [];
  const seen = new Set();
  const push = (url, passage, title) => {
    const u = nz(url) || `parallel://claim/${docs.length + 1}`;
    if (seen.has(u) && !passage) return;
    seen.add(u);
    docs.push({
      url: u,
      title: title || "",
      passage: nz(passage),
      markdown: nz(passage),
    });
  };

  for (const c of artifact.ownership_claims || artifact.claims || []) {
    push(c.source_url || c.url, c.supporting_passage || c.evidence_span || c.excerpt, c.named_entity_or_person);
  }
  for (const s of artifact.sources || artifact.candidate_documents || []) {
    push(s.url || s.source_url, s.passage || s.excerpt || s.snippet, s.title);
  }
  // If Parallel returned only a free-text evidence blob
  if (!docs.length && artifact.evidence_pack_text) {
    push("parallel://evidence_pack", String(artifact.evidence_pack_text).slice(0, 8000), "parallel_evidence_pack");
  }
  return docs;
}

function sanitizeParallelArtifact(artifact) {
  if (!artifact || typeof artifact !== "object") return null;
  const clone = JSON.parse(JSON.stringify(artifact));
  delete clone.email;
  delete clone.phone;
  delete clone.raw_contact;
  if (Array.isArray(clone.people)) {
    clone.people = clone.people.map((p) => ({
      name: p.name || p.display_name || null,
      role: p.role || p.title || null,
      organization: p.organization || null,
      // strip contact values
    }));
  }
  return clone;
}

function finalizeDiscovery(row) {
  return {
    version: OWNERSHIP_DISCOVERY_VERSION,
    provider: row.provider,
    hotel: {
      hotel_id: row.hotel.hotel_id || null,
      hotel_name: row.hotel.hotel_name || null,
      city: row.hotel.city || null,
      country: row.hotel.country || null,
      language: row.hotel.language || null,
    },
    queries_logical: row.logicalQueries,
    queries_executed: row.queries_executed,
    request_parameters: row.request_parameters,
    raw_result_count: (row.raw_results || []).length,
    raw_results: (row.raw_results || []).map((r) => ({
      title: r.title || "",
      url: r.url,
      snippet: String(r.snippet || "").slice(0, 240),
    })),
    ranked: row.ranked,
    rejected: row.rejected,
    documents: row.documents,
    cost: row.cost,
    stop_reason: row.stop_reason,
    stage_trace: row.stageTrace,
    calls: row.calls,
    format_note: row.format_note || null,
    parallel_artifact: row.parallel_artifact || null,
    enrichment_authorized: false,
    publication_authorized: false,
    canonical_writes: false,
  };
}

/**
 * Live Parallel discover fn for comparison runner only.
 * Requires authorized + confirm_spend; never auto-promotes to canonical.
 */
export function createLiveParallelDiscoverFn(opts = {}) {
  return async function parallelDiscover({ hotel_seed, logical_queries, processor, budget_usd }) {
    const { createParallelProvider } = await import("../research/providers/parallel/provider.js");
    const provider = createParallelProvider({
      env: opts.env || process.env,
      force_enabled: opts.force_enabled === true,
    });
    if (!provider.isAvailable() && opts.force_enabled !== true) {
      const err = new Error("PARALLEL_UNAVAILABLE");
      err.code = "PARALLEL_UNAVAILABLE";
      throw err;
    }
    const started = await provider.start({
      template_id: "FULL_HOTEL_INTELLIGENCE",
      hotel_seed: {
        hotel_id: hotel_seed.hotel_id,
        hotel_name: hotel_seed.hotel_name,
        city: hotel_seed.city,
        country: hotel_seed.country,
        language: hotel_seed.language,
        aliases: hotel_seed.aliases || [],
      },
      unresolved_questions: (logical_queries || []).slice(0, 4).map((q) => ({ question: q })),
      known_gaps: (logical_queries || []).slice(0, 4),
      budget_usd: Number(budget_usd || 0.1),
      parallel_processor: processor || "pro",
      blind_mode: true,
      include_contacts: false,
      authorized: opts.authorized === true,
      confirm_spend: opts.confirm_spend === true,
      explicit_user_action: opts.confirm_spend === true,
      request_id: opts.request_id || `ownership-discovery-${hotel_seed.hotel_id || "anon"}`,
    });
    const collected = await provider.collectCompleted(started.provider_run_id, {
      hotel_id: hotel_seed.hotel_id,
      hotel_name: hotel_seed.hotel_name,
      city: hotel_seed.city,
      country: hotel_seed.country,
      template_id: "FULL_HOTEL_INTELLIGENCE",
      compiled_job: started.raw_artifact?.compiled_job || null,
    });
    const norm = collected.normalized || {};
    const cost_usd = Number(
      collected.provider_cost_usd ?? budget_usd ?? 0.1
    );
    return {
      cost_usd,
      ownership_claims: (norm.ownership_claims || norm.claims || []).map((c) => ({
        named_entity_or_person: c.subject || c.named_entity_or_person || c.name,
        relationship: c.relationship || c.party_role || c.role,
        supporting_passage: c.evidence_span || c.excerpt || c.supporting_passage,
        source_url: c.source_url || c.url,
        publication_date: c.source_date || c.publication_date,
        currentness: c.currentness,
      })),
      sources: (norm.sources || []).map((s) => ({
        url: s.url || s.source_url,
        title: s.title,
        passage: s.excerpt || s.passage || s.snippet,
      })),
      evidence_pack_text: norm.evidence_pack_text || null,
      provider_run_id: collected.provider_run_id,
      status: collected.status,
      error: collected.error || null,
    };
  };
}
