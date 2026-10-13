/**
 * Stage Economic Owner — entity-driven web research (P1.5).
 * Targets SPONSORED_BY / CONTROLLED_BY / acquisition / portfolio evidence.
 * Separate from exact PropCo OWNED_BY path.
 */

import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../room-count-research/fetch.js";
import { classifySourceUrl, isFetchEligibleUrl } from "../../room-count-research/trust.js";
import {
  createOwnershipRelationship,
  createRelationshipEvidence,
} from "../schemas.js";
import { scoreOwnershipEvidence } from "../confidence-map.js";
import { resolveOrCreateEntity } from "./entity-resolution.js";
import { isPlausibleLegalEntityName } from "./entity-name-guard.js";
import {
  extractEconomicOwnershipClaims,
  detectPortfolioHotelListing,
} from "./economic-extract-phrases.js";
import {
  buildEntityDrivenOwnershipQueries,
  buildEconomicOwnerQueries,
} from "./entity-driven-search.js";
import { extractOwnershipClaimsFromText, dedupeClaims } from "./extract-phrases.js";

export const STAGE_ECONOMIC_VERSION = "ownership-research-stage-economic-v1";

/**
 * @param {object} ctx
 * @param {{ registeredEntities?: object[] }} [extra]
 */
export async function runStageEconomicOwner(ctx, extra = {}) {
  const { hotelId, hotel, ownershipRepository: repo, env } = ctx;
  const notes = [];
  const metrics = {
    searches: 0,
    pages_fetched: 0,
    entity_driven_queries: 0,
    economic_claims: 0,
    portfolio_hits: 0,
  };
  const candidates = [];
  const registeredEntities = extra.registeredEntities || [];

  const allowSerpapi =
    String(env?.OWNERSHIP_RESEARCH_USE_SERPAPI || env?.ROOM_COUNT_RESEARCH_USE_SERPAPI || "1").trim() !==
      "0" &&
    Boolean(String(env?.SERPAPI_KEY || env?.SERPAPI_API_KEY || "").trim());

  if (!allowSerpapi) {
    notes.push("economic_stage_skipped:serpapi_disabled");
    return { stage: "economic", stop: false, candidates, metrics, notes };
  }

  const maxSearches = Math.min(
    6,
    Number(env?.OWNERSHIP_ECONOMIC_MAX_SEARCHES || 4) || 4
  );
  const maxFetches = Math.min(
    4,
    Number(env?.OWNERSHIP_ECONOMIC_MAX_FETCHES || 3) || 3
  );

  /** @type {string[]} */
  const queries = [];
  for (const ent of registeredEntities.slice(0, 2)) {
    const q = buildEntityDrivenOwnershipQueries(hotel, ent, { maxQueries: 3 });
    metrics.entity_driven_queries += q.length;
    queries.push(...q);
  }
  if (queries.length < maxSearches) {
    queries.push(
      ...buildEconomicOwnerQueries(hotel, {
        maxQueries: maxSearches - queries.length,
      })
    );
  }

  const hotelName = String(hotel.hotel_name || hotel.name || "").trim();
  const seenUrls = new Set();
  let fetches = 0;

  for (const query of queries.slice(0, maxSearches)) {
    metrics.searches += 1;
    let serp;
    try {
      serp = await serpapiSearch(
        {
          engine: "google",
          q: query,
          num: 5,
          hl: "en",
          gl: "us",
        },
        { timeoutMs: 60000, env }
      );
    } catch (err) {
      notes.push(`economic_serp_failed:${query.slice(0, 40)}`);
      continue;
    }
    if (!serp?.ok) {
      notes.push(`economic_serp_error:${serp?.error?.message || "failed"}`);
      continue;
    }

    for (const item of serp.data?.organic_results || []) {
      const url = item?.link || item?.url;
      if (!url || seenUrls.has(url) || fetches >= maxFetches) continue;
      if (!isFetchEligibleUrl(url)) continue;

      const trust = classifySourceUrl(url);
      if (trust.tier === "blocked" || trust.tier === "ota_snippet_only") continue;

      seenUrls.add(url);
      fetches += 1;
      metrics.pages_fetched += 1;

      let text = "";
      try {
        const page = await fetchResearchPage(url, { env, timeoutMs: 15000 });
        text = htmlToSearchableText(page?.html || page?.body || "");
      } catch {
        text = String(item.snippet || "");
      }

      if (!text.trim()) continue;

      const economicClaims = extractEconomicOwnershipClaims(text, {
        url,
        hotelName,
        registeredCompany: registeredEntities[0]?.legal_name,
      });

      // Also run standard extract but remap institutional OWNED_BY → SPONSORED_BY
      const standard = extractOwnershipClaimsFromText(text, { url, hotelName });
      for (const c of standard) {
        if (c.relationship_type === "OWNED_BY") continue; // economic stage skips PropCo
        if (["SPONSORED_BY", "CONTROLLED_BY", "DEVELOPED_BY"].includes(c.relationship_type)) {
          economicClaims.push({ ...c, economic_not_propco: true });
        }
      }

      const portfolio = detectPortfolioHotelListing(text, hotelName, url);
      if (portfolio) {
        metrics.portfolio_hits += 1;
        // Try to infer owner from domain / page title
        const host = tryHostBrand(url);
        if (host) {
          economicClaims.push({ ...portfolio, name: host });
        }
      }

      for (const claim of dedupeClaims(economicClaims)) {
        if (!claim.name || !isPlausibleLegalEntityName(claim.name, { allowBrandAsEntity: true })) {
          continue;
        }
        metrics.economic_claims += 1;
        candidates.push({
          ...claim,
          source_url: url,
          source_type: trust.tier === "government" ? "government" : "web",
          query,
        });
      }
    }
  }

  return {
    stage: "economic",
    stop: false,
    candidates,
    metrics,
    notes,
  };
}

/**
 * Persist economic owner candidates with conservative caps.
 */
export async function stageEconomicCandidates(repo, hotelId, candidates, runId) {
  const staged = { relationships: [], evidence: [], review: [], entities: [] };

  for (const c of candidates || []) {
    const resolved = await resolveOrCreateEntity(repo, {
      legal_name: c.name,
      display_name: c.name,
    });
    if (resolved.ambiguity || !resolved.entity) {
      staged.review.push({
        issue_type: "entity_ambiguous",
        summary: `Economic owner ambiguous: ${c.name}`,
      });
      continue;
    }
    staged.entities.push(resolved.entity);

    const relType = c.relationship_type || "SPONSORED_BY";
    const scored = scoreOwnershipEvidence(c.source_type || "web", {
      relationshipType: relType,
      relationshipExplicitness: c.relationship_explicitness || 0.7,
      entityName: c.name,
      propertyIdentityMatch: c.claim_kind === "portfolio_hotel_listing" ? 0.75 : 0.65,
      pageBacked: Boolean(c.source_url),
      snippetOnly: !c.source_url,
    });

    // Economic claims never reach VERIFIED without manual review
    let confidence = scored.confidence;
    let verification = scored.verification_status;
    if (verification === "verified") {
      verification = "high";
      confidence = Math.min(confidence, 0.89);
    }
    if (c.economic_not_propco && relType !== "OWNED_BY") {
      // Cap SPONSORED_BY from web at HIGH max
      if (confidence > 0.84) {
        confidence = 0.84;
        verification = "high";
      }
    }

    const rel = await repo.upsertRelationship(
      createOwnershipRelationship({
        subject_hotel_id: hotelId,
        relationship_type: relType,
        object_entity_id: resolved.entity.entity_id,
        confidence,
        verification_status: verification,
        research_run_id: runId,
      })
    );
    staged.relationships.push(rel);
    staged.evidence.push(
      await repo.addEvidence(
        createRelationshipEvidence({
          relationship_id: rel.relationship_id,
          source: c.source_url ? new URL(c.source_url).hostname : "web_search",
          source_type: c.source_type || "web",
          source_url: c.source_url || null,
          extracted_claim: c.quote || `${relType}: ${c.name}`,
          confidence,
          source_authority: scored.source_authority,
          extraction_method: "stage_economic",
          researcher: STAGE_ECONOMIC_VERSION,
          notes: c.claim_kind || "economic_owner",
        })
      )
    );
  }
  return staged;
}

function tryHostBrand(url) {
  try {
    const host = new URL(url).hostname.replace(/^www\./, "");
    const base = host.split(".")[0];
    if (!base || base.length < 3) return null;
    return base
      .split("-")
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join(" ");
  } catch {
    return null;
  }
}
