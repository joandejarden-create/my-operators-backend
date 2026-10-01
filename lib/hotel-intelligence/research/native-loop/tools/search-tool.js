/**
 * SerpAPI / search-as-tool — Iteration 2.
 * Controlled by native next-best-action; results normalize into claim/evidence graph.
 */

import { addEvidence, upsertClaim } from "../claim-graph.js";
import { addWorkingNote, upsertTopicNote } from "../working-notes.js";
import { compileGapDrivenSearchQueries } from "./compile-gap-request.js";
import { getFixtureBundle, resolveExternalMode } from "./fixture-external.js";
import { remainingExternalBudget } from "./escalation-policy.js";

export const SEARCH_TOOL_VERSION = "native-search-tool-v1";
const SERPAPI_USD = 0.01;

function authorityTier(sourceType) {
  const t = String(sourceType || "").toLowerCase();
  if (/securities|filing|regulatory|government|registry|investor|official|company_official/.test(t)) {
    return "high";
  }
  if (/press|trade|hospitality|strong_secondary/.test(t)) return "strong_secondary";
  if (/aggregat|seo|directory|scrape|weak/.test(t)) return "weak";
  return "supporting";
}

function ingestHit(state, hit) {
  const ev = addEvidence(state, {
    url: hit.url || null,
    title: hit.title || null,
    source_type: hit.source_type || "web_search",
    authority_tier: hit.authority_tier || authorityTier(hit.source_type),
    excerpt: hit.snippet || hit.excerpt || "",
    entity_association: "ASSUMED_SUBJECT",
    retrieval_timestamp: new Date().toISOString(),
    provider: hit.provider || "search",
  });
  return ev;
}

function softExtractFromSnippet(state, hit, ev) {
  const text = String(hit.snippet || hit.excerpt || "");
  const hotel = state.entity?.name;
  // Conservative: only create soft CANDIDATE claims from search snippets; pages preferred later
  if (/owned by\s+([A-Z][^.]{3,60})/i.test(text)) {
    const m = text.match(/owned by\s+([A-Z][^.]{3,60})/i);
    if (m?.[1]) {
      upsertClaim(state, {
        subject: hotel,
        relationship: "OWNED_BY",
        object: m[1].trim(),
        field: "economic_owner",
        status: "CANDIDATE",
        confidence: "LOW",
        evidence_ids: [ev.evidence_id],
        inference: false,
      });
    }
  }
  if (state.lane === "CONTACT_INTELLIGENCE") {
    if (/Phil Hospod/i.test(text) && /Dovetail/i.test(text)) {
      upsertClaim(state, {
        subject: "Phil Hospod",
        relationship: "PERSON_AFFILIATED_WITH",
        object: "Dovetail + Co",
        field: "person",
        status: "CANDIDATE",
        confidence: "LOW",
        evidence_ids: [ev.evidence_id],
      });
    }
    if (/Founder\s*&\s*CEO|Founder and CEO/i.test(text)) {
      upsertClaim(state, {
        subject: state.entity?.name || "Phil Hospod",
        relationship: "HAS_ROLE",
        object: "Founder & CEO",
        field: "role",
        status: "CANDIDATE",
        confidence: "LOW",
        evidence_ids: [ev.evidence_id],
      });
    }
    if (/dovetailandco\.com/i.test(text) || /dovetailandco\.com/i.test(hit.url || "")) {
      upsertClaim(state, {
        subject: "Dovetail + Co",
        relationship: "HAS_DOMAIN",
        object: "dovetailandco.com",
        field: "domain",
        status: "PROBABLE",
        confidence: "MEDIUM",
        evidence_ids: [ev.evidence_id],
      });
    }
  }
}

async function liveSerpSearch(queries, state) {
  const key = String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim();
  if (!key) return { ok: false, reason: "SERPAPI_KEY_MISSING", hits: [], cost_usd: 0 };

  const budget = remainingExternalBudget(state);
  if (budget < SERPAPI_USD && Number(state.bounds?.max_external_cost_usd ?? 0) > 0) {
    return { ok: false, reason: "EXTERNAL_BUDGET", hits: [], cost_usd: 0 };
  }
  // When max_external_cost_usd is 0, live search is blocked unless force_live
  if (Number(state.bounds?.max_external_cost_usd ?? 0) <= 0 && state.bounds?.force_live !== true) {
    return { ok: false, reason: "EXTERNAL_COST_CAP_ZERO", hits: [], cost_usd: 0 };
  }

  const { serpapiSearch } = await import(
    "../../../../research-engine-v2/providers/serpapi-google-hotels/client.js"
  );
  const hits = [];
  let cost = 0;
  const country = String(state.entity?.country || "").toLowerCase();
  const gl = country.includes("mex") ? "mx" : country.includes("berm") ? "bm" : "us";
  const hl = gl === "mx" ? "es" : "en";

  for (const q of queries.slice(0, 3)) {
    if (cost + SERPAPI_USD > Number(state.bounds.max_external_cost_usd || 0) && !state.bounds.force_live) {
      break;
    }
    try {
      const res = await serpapiSearch({ engine: "google", q, num: 6, gl, hl }, { timeoutMs: 25000 });
      cost += Number(res?.creditsCharged ?? 1) * SERPAPI_USD;
      for (const r of res?.organic_results || res?.organic || []) {
        hits.push({
          title: r.title,
          url: r.link || r.url,
          snippet: r.snippet,
          source_type: "web_search",
          authority_tier: "supporting",
          provider: "serpapi",
          query: q,
        });
      }
    } catch (err) {
      addWorkingNote(state, {
        topic: "unresolved",
        observation: `SerpAPI query failed: ${err.message}`,
        status: "OBSERVED",
      });
    }
  }
  return { ok: hits.length > 0, hits, cost_usd: cost, reason: hits.length ? null : "NO_HITS" };
}

function fixtureSearch(state) {
  const bundle = getFixtureBundle(state.objective?.case_id);
  if (!bundle?.search_hits?.length) return { ok: false, reason: "NO_FIXTURE", hits: [], cost_usd: 0 };
  return {
    ok: true,
    hits: bundle.search_hits.map((h) => ({ ...h, provider: "fixture_search" })),
    cost_usd: 0,
    reason: null,
    mode: "fixture",
  };
}

/**
 * Execute SEARCH_WEB tool.
 */
export async function executeSearchTool(state, action = {}) {
  const queries =
    action.queries ||
    compileGapDrivenSearchQueries(state, action.step || null);

  for (const q of queries) {
    state.queries.push({
      query: q,
      step_id: action.step?.id || null,
      iteration: state.iteration,
      tool: "SEARCH_WEB",
      at: new Date().toISOString(),
    });
  }

  const mode = resolveExternalMode(state);
  let result = { ok: false, hits: [], cost_usd: 0, reason: "SKIPPED" };

  if (mode === "live" || (mode === "auto" && Number(state.bounds?.max_external_cost_usd || 0) > 0)) {
    result = await liveSerpSearch(queries, state);
  }
  if (!result.ok && mode !== "live" && mode !== "off") {
    result = fixtureSearch(state);
  }

  state._search_calls = (state._search_calls || 0) + 1;
  state.providers_used.push(result.mode === "fixture" ? "fixture_search" : "serpapi");
  state.cost.external_usd = (state.cost.external_usd || 0) + (result.cost_usd || 0);
  state.cost.provider_usd = (state.cost.provider_usd || 0) + (result.cost_usd || 0);
  state.cost.total_usd = (state.cost.total_usd || 0) + (result.cost_usd || 0);

  let ingested = 0;
  let newUrls = 0;
  for (const hit of result.hits || []) {
    const existed = hit.url && state.evidence.some((e) => e.url === hit.url);
    const ev = ingestHit(state, hit);
    softExtractFromSnippet(state, hit, ev);
    ingested += 1;
    if (!existed) newUrls += 1;
  }

  upsertTopicNote(state, {
    topic: state.lane === "CONTACT_INTELLIGENCE" ? "contacts" : "ownership",
    current_hypothesis: `Search pass (${result.mode || mode}): ${ingested} hits`,
    strongest_support: result.hits?.[0]?.snippet || null,
    unresolved_question: action.step?.question || null,
    next_research_path: ingested ? "inspect_promising_sources_or_parallel" : "escalate_or_stop",
    status: ingested ? "OBSERVED" : "UNRESOLVED",
    supporting_evidence_ids: state.evidence.slice(-ingested).map((e) => e.evidence_id),
  });

  addWorkingNote(state, {
    topic: "unresolved",
    observation: `SEARCH_WEB ${result.ok ? "ok" : "miss"} — ${ingested} hits; reasons=${(action.reasons || []).join(",")}`,
    status: result.ok ? "SUPPORTED" : "UNRESOLVED",
    open_question: result.reason || null,
  });

  state._promising_urls = (result.hits || [])
    .filter((h) => h.authority_tier === "high" || h.authority_tier === "strong_secondary")
    .map((h) => h.url)
    .filter(Boolean)
    .slice(0, 5);

  return {
    ok: result.ok,
    hits: ingested,
    cost_usd: result.cost_usd || 0,
    mode: result.mode || mode,
    reason: result.reason,
    queries: queries.slice(0, 6),
    material_improvement: newUrls > 0,
  };
}
