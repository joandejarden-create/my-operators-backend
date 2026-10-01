/**
 * Ownership research Stage A — existing Dealality intelligence only.
 * Never treats brand as OWNED_BY. Never uses CoStar.
 */

import { MAP_CENSUS_FIELDS } from "../../map_hotel_intelligence_fields.js";
import {
  createOwnershipEntity,
  createOwnershipRelationship,
  createRelationshipEvidence,
} from "../schemas.js";
import { scoreOwnershipEvidence, CENSUS_FLAT_FIELD_CONFIDENCE_CAP } from "../confidence-map.js";
import { normalizeEntityName } from "../ids.js";
import { interpretOwnershipSignal } from "./source-semantics.js";

export const STAGE_A_VERSION = "ownership-research-stage-a-v1";

/**
 * Cap flat Census Owner/Operator/Developer so they never enter as VERIFIED/HIGH.
 * @param {object} scored
 * @param {{ candidateOnly?: boolean }} [opts]
 */
export function applyCensusFlatFieldCap(scored, opts = {}) {
  const confidence = Math.min(
    Number(scored?.confidence) || 0,
    CENSUS_FLAT_FIELD_CONFIDENCE_CAP
  );
  return {
    ...scored,
    confidence,
    verification_status: "needs_review",
    auto_accept: false,
    candidate_only: opts.candidateOnly !== false,
    tier_label: "needs_review",
    explanation:
      scored?.explanation ||
      "census_flat_field_candidate_not_graph_truth",
  };
}

/**
 * @param {object} ctx
 * @returns {Promise<object>}
 */
export async function runStageA(ctx) {
  const {
    hotelId,
    hotel,
    censusRecord,
    ownershipRepository: repo,
  } = ctx;

  const candidates = [];
  const notes = [];
  const existing = await repo.listRelationshipsForHotel(hotelId, {
    currentOnly: true,
  });

  if (existing.length) {
    notes.push(`existing_relationships:${existing.length}`);
    const strongOwned = existing.find(
      (r) =>
        r.relationship_type === "OWNED_BY" &&
        (r.verification_status === "verified" ||
          r.verification_status === "high")
    );
    if (strongOwned) {
      return {
        stage: "A",
        stop: true,
        stop_reason: "existing_verified_or_high_owned_by",
        candidates,
        existing,
        notes,
      };
    }
  }

  const fields = censusRecord?.fields || {};
  const ownerName = String(fields[MAP_CENSUS_FIELDS.ownerName] || fields["Owner Name"] || "").trim();
  const operatorName = String(
    fields[MAP_CENSUS_FIELDS.operatorName] ||
      fields["Operator / Management Company"] ||
      ""
  ).trim();
  const developerName = String(
    fields[MAP_CENSUS_FIELDS.developerName] || fields["Developer Name"] || ""
  ).trim();
  const brandName = String(
    hotel?.brand_name ||
      fields[MAP_CENSUS_FIELDS.brandName] ||
      fields["Current Brand"] ||
      ""
  ).trim();

  // Brand is BRANDED_BY context only — never OWNED_BY from Stage A brand field.
  if (brandName) {
    notes.push("brand_present_not_used_as_owner");
  }

  if (ownerName) {
    notes.push("census_owner_name_as_candidate_only");
    candidates.push(
      await draftCandidate(repo, {
        hotelId,
        name: ownerName,
        relationship_type: "OWNED_BY",
        source: "dealality_census",
        source_type: "dealality_census",
        claim: `Census Owner Name (candidate lead, not verified): ${ownerName}`,
        method: "stage_a",
        completeness: 0.5,
        census_flat_field: true,
      })
    );
  }

  if (operatorName) {
    const sameAsOwner =
      ownerName &&
      normalizeEntityName(operatorName) === normalizeEntityName(ownerName);
    notes.push("census_operator_as_operated_by_only");
    candidates.push(
      await draftCandidate(repo, {
        hotelId,
        name: operatorName,
        relationship_type: "OPERATED_BY",
        source: "dealality_census",
        source_type: "dealality_census",
        claim: `Census Operator / Management Company (candidate lead): ${operatorName}`,
        method: "stage_a",
        completeness: 0.5,
        warn_operator_owner: sameAsOwner,
        census_flat_field: true,
      })
    );
  }

  if (developerName) {
    notes.push("census_developer_as_developed_by_only");
    candidates.push(
      await draftCandidate(repo, {
        hotelId,
        name: developerName,
        relationship_type: "DEVELOPED_BY",
        source: "dealality_census",
        source_type: "dealality_census",
        claim: `Census Developer Name (candidate lead): ${developerName}`,
        method: "stage_a",
        completeness: 0.5,
        census_flat_field: true,
      })
    );
  }

  // ownership_signal sidecar — interpret with documented semantics (P1C).
  // Do NOT auto-OWNED_BY from registered lodging entity fields.
  const signal =
    censusRecord?.ownership_signal ||
    hotel?.ownership_signal ||
    fields.ownership_signal ||
    null;
  if (signal && typeof signal === "object") {
    notes.push("ownership_signal_present");
    const interpreted = interpretOwnershipSignal(signal);
    notes.push(...interpreted.notes);
    if (interpreted.legal_representative) {
      notes.push("legal_representative_not_staged_as_owner");
    }
    if (interpreted.entity_name) {
      if (interpreted.allowed_relationship === "OWNED_BY") {
        candidates.push(
          await draftCandidate(repo, {
            hotelId,
            name: interpreted.entity_name,
            relationship_type: "OWNED_BY",
            source: "government_signal",
            source_type: "government",
            claim: `ownership_signal (semantics-capped): ${interpreted.entity_name}`,
            method: "stage_a",
            completeness: 0.55,
            identifiers: interpreted.identifiers,
            max_verification_cap: interpreted.max_verification,
          })
        );
      } else {
        // Entity lead only — resolve into graph entity without OWNED_BY edge
        notes.push("ownership_signal_entity_resolution_only");
        candidates.push(
          await draftCandidate(repo, {
            hotelId,
            name: interpreted.entity_name,
            relationship_type: "OPERATED_BY", // placeholder type rejected below
            source: "government_signal",
            source_type: "government",
            claim: `registered lodging entity (not PropCo): ${interpreted.entity_name}`,
            method: "stage_a",
            completeness: 0.5,
            identifiers: interpreted.identifiers,
            entity_resolution_only: true,
          })
        );
      }
    }
  }

  return {
    stage: "A",
    stop: false,
    stop_reason: null,
    candidates,
    existing,
    notes,
  };
}

/**
 * Resolve-or-create entity by exact identifier or exact normalized name (no fuzzy merge).
 * Multiple name hits → leave unresolved candidate for review.
 */
async function draftCandidate(repo, spec) {
  let scored = scoreOwnershipEvidence(spec.source_type, {
    completeness: spec.completeness,
    relationshipType: spec.relationship_type,
    relationshipExplicitness: spec.census_flat_field ? 0.35 : 0.55,
    entityName: spec.name,
    pageBacked: false,
    snippetOnly: true,
    propertyIdentityMatch: 0.5,
  });
  if (spec.census_flat_field || spec.source_type === "dealality_census") {
    scored = applyCensusFlatFieldCap(scored, { candidateOnly: true });
  }
  let entity = null;
  let ambiguity = false;

  if (spec.identifiers?.length) {
    for (const id of spec.identifiers) {
      const hits = await repo.findByIdentifier(id.kind, id.value);
      if (hits.length === 1) {
        entity = hits[0];
        break;
      }
      if (hits.length > 1) {
        ambiguity = true;
      }
    }
  }

  if (!entity) {
    const hits = await repo.findByNormalizedName(spec.name);
    if (hits.length === 1) entity = hits[0];
    else if (hits.length > 1) ambiguity = true;
  }

  if (!entity && !ambiguity) {
    entity = await repo.upsertEntity(
      createOwnershipEntity({
        legal_name: spec.name,
        display_name: spec.name,
        entity_type: "company",
        identifiers: spec.identifiers || [],
      })
    );
  }

  return {
    ambiguity,
    warn_operator_owner: Boolean(spec.warn_operator_owner),
    candidate_only: Boolean(scored.candidate_only),
    entity_resolution_only: Boolean(spec.entity_resolution_only),
    entity,
    name: spec.name,
    relationship_type: spec.relationship_type,
    scored,
    evidence_draft: {
      source: spec.source,
      source_type: spec.source_type,
      extracted_claim: spec.claim,
      extraction_method: spec.method,
      confidence: scored.confidence,
      source_authority: scored.confidence,
      notes: scored.candidate_only
        ? "census_flat_field_candidate_not_graph_truth"
        : spec.entity_resolution_only
          ? "registered_entity_not_propco"
          : null,
    },
  };
}

/**
 * Persist Stage A candidates that resolved to a single entity.
 */
export async function stageCandidates(repo, hotelId, candidates, runId) {
  const staged = { relationships: [], evidence: [], review: [], entities: [] };
  for (const c of candidates || []) {
    if (c.ambiguity || !c.entity) {
      staged.review.push({
        issue_type: "entity_ambiguous",
        summary: `Ambiguous entity for ${c.relationship_type}: ${c.name}`,
      });
      continue;
    }
    staged.entities.push(c.entity);
    if (c.entity_resolution_only) {
      staged.review.push({
        issue_type: "weak_ownership_candidate",
        summary: `Registered lodging entity (not PropCo OWNED_BY): ${c.name}`,
      });
      continue;
    }
    if (c.warn_operator_owner) {
      staged.review.push({
        issue_type: "operator_owner_confusion",
        summary: `Operator name matches owner name for ${c.name}`,
      });
    }
    const rel = await repo.upsertRelationship(
      createOwnershipRelationship({
        subject_hotel_id: hotelId,
        relationship_type: c.relationship_type,
        object_entity_id: c.entity.entity_id,
        confidence: c.scored.confidence,
        verification_status: c.scored.verification_status || "needs_review",
        research_run_id: runId,
      })
    );
    staged.relationships.push(rel);
    const ev = await repo.addEvidence(
      createRelationshipEvidence({
        relationship_id: rel.relationship_id,
        ...c.evidence_draft,
        researcher: STAGE_A_VERSION,
      })
    );
    staged.evidence.push(ev);
  }
  return staged;
}
