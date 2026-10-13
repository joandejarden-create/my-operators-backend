/**
 * Ownership research Stage B — official site + first-party heuristics.
 * Source authority ≠ claim strength. Brand ≠ OWNED_BY.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../room-count-research/fetch.js";
import { classifySourceUrl, SOURCE_CATEGORIES } from "../../room-count-research/trust.js";
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

export const STAGE_B_VERSION = "ownership-research-stage-b-v2";

/**
 * @param {object} ctx
 */
export async function runStageB(ctx) {
  const { hotelId, hotel, ownershipRepository: repo, env } = ctx;
  const website = normalizeWebsite(hotel?.website || hotel?.official_website);
  const notes = [];
  const candidates = [];

  if (!website) {
    return {
      stage: "B",
      stop: false,
      stop_reason: null,
      candidates,
      notes: ["no_official_website"],
    };
  }

  const fetched = await fetchResearchPage(website, { env });
  if (!fetched?.ok) {
    notes.push(`official_site_fetch_failed:${fetched?.error || "unknown"}`);
    return { stage: "B", stop: false, stop_reason: null, candidates, notes };
  }

  const text = htmlToSearchableText(fetched.text || "");
  const category = classifySourceUrl(website, { hotelWebsite: website });
  notes.push(`official_site_category:${category}`);

  for (const claim of extractOwnershipClaimsFromText(text, {
    url: website,
    hotelName: hotel?.hotel_name || hotel?.name,
  })) {
    if (!isPlausibleLegalEntityName(claim.name)) {
      notes.push(`rejected_implausible_entity:${claim.name.slice(0, 60)}`);
      continue;
    }
    if (
      claim.relationship_type === "OWNED_BY" &&
      (looksLikeBrandOnly(claim.name, hotel) || looksLikeMajorBrandName(claim.name))
    ) {
      notes.push(`skipped_brandlike_owner_claim:${claim.name}`);
      continue;
    }
    candidates.push({ ...claim, page_backed: true, snippet_only: false });
  }

  const stagedCandidates = [];
  for (const c of dedupeClaims(candidates).slice(0, 5)) {
    stagedCandidates.push(await resolveCandidate(repo, hotelId, c, category));
  }

  const owned = stagedCandidates.filter(
    (c) => c.relationship_type === "OWNED_BY" && c.entity && !c.ambiguity
  );
  const highOwned = owned.filter(
    (c) =>
      c.scored.verification_status === "verified" ||
      c.scored.verification_status === "high"
  );

  return {
    stage: "B",
    stop: highOwned.length >= 1,
    stop_reason: highOwned.length ? "stage_b_high_owned_by" : null,
    candidates: stagedCandidates,
    notes,
  };
}

export async function stageBCandidates(repo, hotelId, candidates, runId) {
  const staged = { relationships: [], evidence: [], review: [] };
  for (const c of candidates || []) {
    if (c.ambiguity || !c.entity) {
      staged.review.push({
        issue_type: "entity_ambiguous",
        summary: `Stage B ambiguous: ${c.name}`,
      });
      continue;
    }
    const rel = await repo.upsertRelationship(
      createOwnershipRelationship({
        subject_hotel_id: hotelId,
        relationship_type: c.relationship_type,
        object_entity_id: c.entity.entity_id,
        confidence: c.scored.confidence,
        verification_status: c.scored.verification_status,
        research_run_id: runId,
      })
    );
    staged.relationships.push(rel);
    staged.evidence.push(
      await repo.addEvidence(
        createRelationshipEvidence({
          relationship_id: rel.relationship_id,
          source: "official_site",
          source_type: "first_party",
          source_url: c.url || null,
          extracted_claim: c.quote || c.name,
          confidence: c.scored.confidence,
          source_authority: c.scored.source_authority ?? c.scored.confidence,
          extraction_method: "stage_b",
          researcher: STAGE_B_VERSION,
          notes: c.scored.explanation || null,
        })
      )
    );
  }
  return staged;
}

async function resolveCandidate(repo, hotelId, c, category) {
  const sourceType =
    category === SOURCE_CATEGORIES.OFFICIAL_HOTEL ||
    category === SOURCE_CATEGORIES.OFFICIAL_OWNER
      ? "first_party"
      : category === SOURCE_CATEGORIES.OFFICIAL_OPERATOR
        ? "first_party"
        : "other";
  const scored = scoreOwnershipEvidence(sourceType, {
    relationshipType: c.relationship_type,
    relationshipExplicitness: c.relationship_explicitness,
    entityName: c.name,
    propertyIdentityMatch: 0.7,
    temporalRelevance: 0.75,
    corroborationCount: 0,
    pageBacked: true,
    snippetOnly: false,
    completeness: 0.8,
  });
  const hits = await repo.findByNormalizedName(c.name);
  let entity = null;
  let ambiguity = false;
  if (hits.length === 1) entity = hits[0];
  else if (hits.length > 1) ambiguity = true;
  else {
    entity = await repo.upsertEntity(
      createOwnershipEntity({
        legal_name: c.name,
        display_name: c.name,
        entity_type: "company",
      })
    );
  }
  return {
    ...c,
    hotelId,
    entity,
    ambiguity,
    scored,
  };
}

function normalizeWebsite(url) {
  const s = String(url || "").trim();
  if (!s) return null;
  if (/^https?:\/\//i.test(s)) return s;
  return `https://${s}`;
}

function looksLikeBrandOnly(name, hotel) {
  const brand = normalizeEntityName(hotel?.brand_name || "");
  const n = normalizeEntityName(name);
  if (!brand || !n) return false;
  return n === brand || n.includes(brand) || brand.includes(n);
}
