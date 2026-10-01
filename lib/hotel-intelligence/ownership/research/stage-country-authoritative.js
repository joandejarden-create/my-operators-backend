/**
 * Stage Country — authoritative ownership evidence (P1).
 * Runs after Stage A, before Stage B/C.
 * Does not write Census Owner Name.
 */

import { createOwnershipRelationship, createRelationshipEvidence } from "../schemas.js";
import { scoreOwnershipEvidence } from "../confidence-map.js";
import { resolveOrCreateEntity } from "./entity-resolution.js";
import { lookupCountryOwnershipEvidence } from "./adapters/index.js";
import {
  searchLeiByLegalName,
  getDirectParentLei,
} from "./gleif-client.js";
import { FIELD_SEMANTICS } from "./source-semantics.js";
import { isPlausibleLegalEntityName } from "./entity-name-guard.js";

export const STAGE_COUNTRY_VERSION = "ownership-research-stage-country-v1";

/**
 * @param {object} ctx
 */
export async function runStageCountry(ctx) {
  const { hotelId, hotel, ownershipRepository: repo, env } = ctx;
  const notes = [];
  const metrics = {
    adapter: null,
    entity_candidates: 0,
    relationships_staged: 0,
    gleif_lookups: 0,
    gleif_parents: 0,
  };

  const lookup = await lookupCountryOwnershipEvidence(
    { ...hotel, hotel_id: hotelId },
    { env }
  );
  notes.push(...(lookup.notes || []));
  metrics.adapter = lookup.metrics?.provider || lookup.country || null;

  const stagedCandidates = [];
  for (const cand of lookup.candidates || []) {
    if (!cand.entity_candidate?.legal_name) continue;
    if (
      cand.supported_relationship === "OWNED_BY" &&
      !isPlausibleLegalEntityName(cand.entity_candidate.legal_name)
    ) {
      notes.push(
        `rejected_implausible_country_entity:${cand.entity_candidate.legal_name.slice(0, 40)}`
      );
      continue;
    }

    const resolved = await resolveOrCreateEntity(repo, {
      legal_name: cand.entity_candidate.legal_name,
      display_name: cand.entity_candidate.display_name,
      jurisdiction: cand.entity_candidate.jurisdiction,
      identifiers: cand.entity_candidate.identifiers,
    });
    metrics.entity_candidates += 1;

    if (resolved.ambiguity || !resolved.entity) {
      stagedCandidates.push({
        ...cand,
        ambiguity: true,
        entity: null,
        resolve_notes: resolved.notes,
      });
      continue;
    }

    stagedCandidates.push({
      ...cand,
      ambiguity: false,
      entity: resolved.entity,
      match_method: resolved.match_method,
      resolve_notes: resolved.notes,
    });
  }

  return {
    stage: "country",
    stop: false,
    stop_reason: null,
    candidates: stagedCandidates,
    adapter_result: lookup,
    metrics,
    notes,
  };
}

/**
 * Persist country-stage candidates. Entity-resolution-only candidates
 * do not create hotel relationships unless supported_relationship is set.
 */
export async function stageCountryCandidates(repo, hotelId, candidates, runId) {
  const staged = { relationships: [], evidence: [], review: [], entities: [] };
  for (const c of candidates || []) {
    if (c.ambiguity || !c.entity) {
      staged.review.push({
        issue_type: "entity_ambiguous",
        summary: `Country adapter ambiguous: ${c.entity_candidate?.legal_name || c.name}`,
      });
      continue;
    }
    staged.entities.push(c.entity);

    if (!c.supported_relationship) {
      staged.review.push({
        issue_type: "weak_ownership_candidate",
        summary: `Registered entity (no OWNED_BY): ${c.entity.legal_name} via ${c.source_provider}`,
      });
      continue;
    }

    const maxVer = c.max_verification || "needs_review";
    const scored = scoreOwnershipEvidence("government", {
      relationshipType: c.supported_relationship,
      relationshipExplicitness: maxVer === "needs_review" ? 0.55 : 0.8,
      entityName: c.entity.legal_name,
      propertyIdentityMatch: 0.65,
      pageBacked: true,
      completeness: 0.7,
    });

    // Cap verification to adapter max
    let verification = scored.verification_status;
    let confidence = scored.confidence;
    if (maxVer === "needs_review") {
      verification = "needs_review";
      confidence = Math.min(confidence, 0.69);
    } else if (maxVer === "probable") {
      if (verification === "verified" || verification === "high") {
        verification = "probable";
        confidence = Math.min(confidence, 0.84);
      }
    }

    const rel = await repo.upsertRelationship(
      createOwnershipRelationship({
        subject_hotel_id: hotelId,
        relationship_type: c.supported_relationship,
        object_entity_id: c.entity.entity_id,
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
          source: c.source_provider,
          source_type: "government",
          source_url: c.source_url || null,
          extracted_claim: c.extracted_claim || c.source_semantics,
          confidence,
          source_authority: c.source_authority || scored.source_authority,
          extraction_method: "stage_country",
          researcher: STAGE_COUNTRY_VERSION,
          notes: c.source_semantics,
        })
      )
    );
  }
  return staged;
}

/**
 * GLEIF enrichment for entities already linked (or country-resolved).
 * Creates CONTROLLED_BY / SPONSORED_BY on the *company*, never invents hotel OWNED_BY.
 *
 * For hotel graph: if hotel has OWNED_BY → PropCo, and GLEIF finds PropCo parent,
 * we can add CONTROLLED_BY from hotel context only when PropCo edge exists — actually
 * ontology is hotel→CONTROLLED_BY→parent. Prefer hotel→OWNED_BY→PropCo and
 * separately store parent on entity; also add hotel→CONTROLLED_BY→parent as economic owner.
 */
export async function enrichEntitiesWithGleif(repo, hotelId, entities, runId, env) {
  const staged = { relationships: [], evidence: [], review: [], metrics: { lookups: 0, parents: 0 } };
  const enabled =
    String(env?.OWNERSHIP_GLEIF_ENABLE || process.env.OWNERSHIP_GLEIF_ENABLE || "1").trim() !==
    "0";
  if (!enabled) {
    return staged;
  }

  const seen = new Set();
  for (const ent of entities || []) {
    if (!ent?.entity_id || seen.has(ent.entity_id)) continue;
    seen.add(ent.entity_id);

    staged.metrics.lookups += 1;
    let lei = ent.lei || ent.identifiers?.find((i) => i.kind === "lei")?.value;
    if (!lei) {
      const search = await searchLeiByLegalName(ent.legal_name, {
        jurisdiction: ent.country,
        env,
      });
      if (search.ok && search.records.length === 1) {
        lei = search.records[0].lei;
        await repo.upsertEntity({
          ...ent,
          lei,
          identifiers: mergeLei(ent.identifiers, lei),
        });
      } else if (search.ok && search.records.length > 1) {
        staged.review.push({
          issue_type: "entity_ambiguous",
          summary: `GLEIF ambiguous for ${ent.legal_name}`,
        });
        continue;
      } else {
        continue;
      }
    }

    const parentRes = await getDirectParentLei(lei, { env });
    if (!parentRes.ok || !parentRes.parent?.legal_name) continue;
    staged.metrics.parents += 1;

    const parentResolved = await resolveOrCreateEntity(repo, {
      legal_name: parentRes.parent.legal_name,
      jurisdiction: parentRes.parent.jurisdiction,
      identifiers: parentRes.parent_lei
        ? [{ kind: "lei", value: parentRes.parent_lei }]
        : [],
      lei: parentRes.parent_lei,
    });
    if (!parentResolved.entity || parentResolved.ambiguity) continue;

    const sem = FIELD_SEMANTICS["gleif.direct_parent"];
    const scored = scoreOwnershipEvidence("corporate_registry", {
      relationshipType: "CONTROLLED_BY",
      relationshipExplicitness: 0.9,
      entityName: parentResolved.entity.legal_name,
      propertyIdentityMatch: 0.7,
      pageBacked: true,
      completeness: 0.85,
    });

    // Cap HIGH (not VERIFIED) — GLEIF proves company parent, hotel link is indirect
    let confidence = Math.min(scored.confidence, 0.9);
    let verification =
      confidence >= 0.85 ? "high" : scored.verification_status;

    const rel = await repo.upsertRelationship(
      createOwnershipRelationship({
        subject_hotel_id: hotelId,
        relationship_type: "CONTROLLED_BY",
        object_entity_id: parentResolved.entity.entity_id,
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
          source: "gleif",
          source_type: "corporate_registry",
          source_url: `https://search.gleif.org/#/record/${lei}`,
          extracted_claim: `GLEIF direct parent of ${ent.legal_name}: ${parentResolved.entity.legal_name}`,
          confidence,
          source_authority: 0.93,
          extraction_method: "gleif",
          researcher: STAGE_COUNTRY_VERSION,
          notes: sem.notes,
        })
      )
    );
  }
  return staged;
}

function mergeLei(identifiers, lei) {
  const list = Array.isArray(identifiers) ? [...identifiers] : [];
  if (!list.some((i) => i.kind === "lei" && i.value === lei)) {
    list.push({ kind: "lei", value: lei });
  }
  return list;
}
