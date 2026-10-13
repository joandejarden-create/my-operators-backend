/**
 * Ownership research Stage C — bounded multilingual search.
 * Prefer page fetch over snippet-only OWNED_BY. Do not inflate search volume.
 */

import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../room-count-research/fetch.js";
import { classifySourceUrl, isFetchEligibleUrl } from "../../room-count-research/trust.js";
import {
  createOwnershipEntity,
  createOwnershipRelationship,
  createRelationshipEvidence,
} from "../schemas.js";
import { scoreOwnershipEvidence } from "../confidence-map.js";
import { normalizeEntityName } from "../ids.js";

import {
  extractOwnershipClaimsFromText,
  dedupeClaims,
} from "./extract-phrases.js";
import { isPlausibleLegalEntityName } from "./entity-name-guard.js";
import { looksLikeMajorBrandName } from "./claim-context.js";

import {
  buildOwnershipQueries,
  buildEntityDrivenOwnershipQueries,
} from "./entity-driven-search.js";

export { buildOwnershipQueries };

export const STAGE_C_VERSION = "ownership-research-stage-c-v3";

function inferLang(country) {
  const c = String(country || "").toLowerCase();
  if (/(brazil|brasil)/.test(c)) return "pt";
  if (
    /(mexico|spain|colombia|peru|chile|argentina|ecuador|venezuela|panama|costa rica|guatemala|honduras|nicaragua|el salvador|bolivia|dominican|puerto rico|cuba)/.test(
      c
    )
  ) {
    return "es";
  }
  return "en";
}

/**
 * @param {object} ctx
 */
export async function runStageC(ctx) {
  const { hotelId, hotel, ownershipRepository: repo, env } = ctx;
  const notes = [];
  const metrics = {
    searches: 0,
    organic_results: 0,
    fetch_candidates: 0,
    pages_fetched: 0,
    pages_ok: 0,
    snippet_claims: 0,
    page_claims: 0,
    fetch_failed: 0,
  };
  const maxSearches = Math.min(
    5,
    Math.max(
      0,
      Number(
        ctx.maxSearches ??
          env?.OWNERSHIP_RESEARCH_MAX_SEARCHES ??
          3
      )
    )
  );
  const maxFetches = Math.min(
    6,
    Math.max(
      1,
      Number(ctx.maxPageFetches ?? env?.OWNERSHIP_RESEARCH_MAX_FETCHES ?? 4)
    )
  );
  const allowSerpapi =
    ctx.allowSerpapi !== false &&
    String(env?.OWNERSHIP_RESEARCH_USE_SERPAPI || env?.ROOM_COUNT_RESEARCH_USE_SERPAPI || "1").trim() !==
      "0" &&
    Boolean(String(env?.SERPAPI_KEY || env?.SERPAPI_API_KEY || "").trim());

  if (!allowSerpapi) {
    return {
      stage: "C",
      stop: true,
      stop_reason: "stage_c_serpapi_unavailable_or_disabled",
      candidates: [],
      notes: ["serpapi_disabled"],
      metrics,
    };
  }

  const queries = buildOwnershipQueries(hotel, { maxQueries: maxSearches });
  const registered = ctx.registeredEntities || [];
  for (const ent of registered.slice(0, 2)) {
    const entityQ = buildEntityDrivenOwnershipQueries(hotel, ent, {
      maxQueries: 2,
    });
    queries.push(...entityQ);
  }
  const uniqueQueries = [...new Set(queries)].slice(0, maxSearches + 2);
  /** @type {Array<{url:string, title?:string, snippet?:string, rank:number}>} */
  const organicUrls = [];
  /** @type {Array<object>} */
  const discoveries = [];

  for (const q of uniqueQueries) {
    metrics.searches += 1;
    try {
      const res = await serpapiSearch(
        {
          engine: "google",
          q,
          num: 5,
          hl: "en",
          gl: "us",
        },
        { timeoutMs: 60000, env }
      );
      if (!res?.ok) {
        notes.push(`serpapi_error:${res?.error?.message || "failed"}`);
        continue;
      }
      const organic = Array.isArray(res.data?.organic_results)
        ? res.data.organic_results
        : [];
      metrics.organic_results += organic.length;
      let rank = 0;
      for (const row of organic) {
        rank += 1;
        const url = row.link || row.url;
        if (!url) continue;
        organicUrls.push({
          url,
          title: row.title || "",
          snippet: row.snippet || "",
          rank,
        });
        // Snippet extraction is lead-only; OWNED_BY from snippets stay capped / prefer page
        const before = discoveries.length;
        extractClaims(`${row.title || ""} ${row.snippet || ""}`, url, discoveries, {
          snippet_only: true,
          page_backed: false,
        });
        metrics.snippet_claims += discoveries.length - before;
      }
    } catch (err) {
      notes.push(`serpapi_error:${String(err?.message || err).slice(0, 80)}`);
    }
  }

  // Prefer fetching organic results (ownership-relevant hosts) — not only snippet hits
  const fetchTargets = selectFetchTargetsFromOrganic(
    organicUrls,
    discoveries,
    maxFetches
  );
  metrics.fetch_candidates = fetchTargets.length;
  notes.push(`fetch_targets:${fetchTargets.length}`);

  for (const t of fetchTargets) {
    metrics.pages_fetched += 1;
    const page = await fetchResearchPage(t.url, { env });
    if (!page?.ok) {
      metrics.fetch_failed += 1;
      notes.push(`fetch_failed:${String(t.url).slice(0, 80)}`);
      continue;
    }
    metrics.pages_ok += 1;
    const text = htmlToSearchableText(page.text || "");
    const before = discoveries.length;
    extractClaims(text.slice(0, 50000), t.url, discoveries, {
      snippet_only: false,
      page_backed: true,
    });
    metrics.page_claims += discoveries.length - before;
  }

  const candidates = [];
  for (const d of preferPageBacked(dedupe(discoveries)).slice(0, 8)) {
    if (
      d.relationship_type === "OWNED_BY" &&
      looksLikeMajorBrandName(d.name) &&
      !/\b(international|group|grupo|hospitality)\b/i.test(d.name)
    ) {
      notes.push(`skipped_brand_owned_by:${d.name}`);
      continue;
    }
    if (
      d.relationship_type === "OWNED_BY" &&
      looksLikeSameAsHotel(d.name, hotel)
    ) {
      notes.push(`skipped_self_hotel_owned_by:${d.name}`);
      continue;
    }
    candidates.push(await resolveDiscovery(repo, d));
  }

  return {
    stage: "C",
    stop: true,
    stop_reason: "stage_c_complete",
    candidates,
    notes,
    metrics,
  };
}

export async function stageCCandidates(repo, hotelId, candidates, runId) {
  const staged = { relationships: [], evidence: [], review: [] };
  for (const c of candidates || []) {
    if (c.ambiguity || !c.entity) {
      staged.review.push({
        issue_type: "weak_ownership_candidate",
        summary: `Stage C unresolved: ${c.name}`,
      });
      continue;
    }
    // Do not stage snippet-only OWNED_BY as graph edges — review queue only
    if (
      c.relationship_type === "OWNED_BY" &&
      c.snippet_only &&
      !c.page_backed
    ) {
      staged.review.push({
        issue_type: "weak_ownership_candidate",
        summary: `Snippet-only OWNED_BY (not staged): ${c.name}`,
      });
      continue;
    }
    if (c.scored.confidence < 0.7) {
      staged.review.push({
        issue_type: "weak_ownership_candidate",
        summary: `Low confidence Stage C claim: ${c.name}`,
      });
    }
    const rel = await repo.upsertRelationship(
      createOwnershipRelationship({
        subject_hotel_id: hotelId,
        relationship_type: c.relationship_type || "OWNED_BY",
        object_entity_id: c.entity.entity_id,
        confidence: c.scored.confidence,
        verification_status:
          c.scored.confidence < 0.7
            ? "needs_review"
            : c.scored.verification_status,
        research_run_id: runId,
      })
    );
    staged.relationships.push(rel);
    staged.evidence.push(
      await repo.addEvidence(
        createRelationshipEvidence({
          relationship_id: rel.relationship_id,
          source: "web_search",
          source_type: mapSourceType(c.url),
          source_url: c.url || null,
          extracted_claim: c.quote || c.name,
          confidence: c.scored.confidence,
          source_authority: c.scored.source_authority ?? c.scored.confidence,
          extraction_method: "stage_c",
          researcher: STAGE_C_VERSION,
          notes: c.scored.explanation || null,
        })
      )
    );
  }
  return staged;
}

function extractClaims(text, url, out, flags = {}) {
  for (const claim of extractOwnershipClaimsFromText(text, { url })) {
    if (!isPlausibleLegalEntityName(claim.name)) continue;
    out.push({
      ...claim,
      snippet_only: Boolean(flags.snippet_only),
      page_backed: flags.page_backed !== false && !flags.snippet_only,
    });
  }
}

function looksLikeSameAsHotel(entityName, hotel) {
  const a = normalizeEntityName(entityName);
  const b = normalizeEntityName(
    hotel?.hotel_name || hotel?.name || hotel?.official_name || ""
  );
  if (!a || !b) return false;
  return a === b || a.includes(b) || b.includes(a);
}

/**
 * Select URLs from organic results first (ownership-relevant), then fill from claim URLs.
 */
function selectFetchTargetsFromOrganic(organicUrls, discoveries, maxFetches) {
  const seen = new Set();
  const out = [];

  const scored = organicUrls
    .map((row) => ({
      ...row,
      score: ownershipFetchPriority(row),
    }))
    .filter((row) => row.score > 0 && isFetchEligibleUrl(row.url))
    .sort((a, b) => b.score - a.score || a.rank - b.rank);

  for (const row of scored) {
    if (seen.has(row.url)) continue;
    seen.add(row.url);
    out.push(row);
    if (out.length >= maxFetches) return out;
  }

  // Fill remaining slots from discovery URLs that look fetchable
  for (const d of discoveries) {
    const url = d.url;
    if (!url || seen.has(url)) continue;
    if (!isFetchEligibleUrl(url)) continue;
    seen.add(url);
    out.push({ url, title: "", snippet: "", rank: 99 });
    if (out.length >= maxFetches) break;
  }
  return out;
}

function ownershipFetchPriority(row) {
  const blob = `${row.url} ${row.title} ${row.snippet}`.toLowerCase();
  // Skip pure OTA (already filtered by isFetchEligibleUrl for directories)
  let score = 1;
  if (
    /\b(owner|owned|propiedad|propriet|adquir|acquisition|investor|desarroll|developer|incorporadora|operado|managed by|operated by)\b/i.test(
      blob
    )
  ) {
    score += 3;
  }
  if (/\b(prnewswire|businesswire|globenewswire|newsroom|press)\b/i.test(blob)) {
    score += 2;
  }
  if (/\b(hotel|resort|hospitality|grupo|group|capital|inversiones)\b/i.test(blob)) {
    score += 1;
  }
  if (/\b(tripadvisor|booking\.com|expedia|yelp|facebook)\b/i.test(blob)) {
    score -= 5;
  }
  return score;
}

function preferPageBacked(list) {
  return [...list].sort((a, b) => {
    const ap = a.page_backed ? 1 : 0;
    const bp = b.page_backed ? 1 : 0;
    if (bp !== ap) return bp - ap;
    return (b.relationship_explicitness || 0) - (a.relationship_explicitness || 0);
  });
}

function dedupe(list) {
  // Prefer page-backed when deduping same name+rel
  const byKey = new Map();
  for (const c of list || []) {
    const key = `${c.relationship_type}|${normalizeEntityName(c.name)}`;
    const prev = byKey.get(key);
    if (!prev) {
      byKey.set(key, c);
      continue;
    }
    if (c.page_backed && !prev.page_backed) byKey.set(key, c);
    else if (
      c.page_backed === prev.page_backed &&
      (c.relationship_explicitness || 0) > (prev.relationship_explicitness || 0)
    ) {
      byKey.set(key, c);
    }
  }
  return [...byKey.values()];
}

async function resolveDiscovery(repo, d) {
  const sourceType = mapSourceType(d.url);
  const scored = scoreOwnershipEvidence(sourceType, {
    relationshipType: d.relationship_type,
    relationshipExplicitness: d.relationship_explicitness,
    entityName: d.name,
    propertyIdentityMatch: 0.55,
    temporalRelevance: 0.65,
    corroborationCount: 0,
    pageBacked: Boolean(d.page_backed),
    snippetOnly: Boolean(d.snippet_only) && !d.page_backed,
    completeness: d.page_backed ? 0.7 : 0.45,
  });
  const hits = await repo.findByNormalizedName(d.name);
  let entity = null;
  let ambiguity = false;
  if (hits.length === 1) entity = hits[0];
  else if (hits.length > 1) ambiguity = true;
  else {
    entity = await repo.upsertEntity(
      createOwnershipEntity({
        legal_name: d.name,
        display_name: d.name,
        entity_type: "company",
      })
    );
  }
  return { ...d, entity, ambiguity, scored };
}

function mapSourceType(url) {
  const cat = classifySourceUrl(url);
  if (/Official|Tourism|Convention/i.test(cat)) return "first_party";
  if (/Press|News/i.test(cat)) return "trade_press";
  if (/Government/i.test(cat)) return "government";
  return "web_search";
}
