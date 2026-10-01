/**
 * GLEIF corporate graph enrichment (P1.5).
 * Entity→parent on corporate graph; hotel economic link only with separate evidence.
 */

import { createOwnershipRelationship, createRelationshipEvidence } from "../schemas.js";
import { scoreOwnershipEvidence } from "../confidence-map.js";
import { resolveOrCreateEntity } from "./entity-resolution.js";
import { searchLeiByLegalName, getDirectParentLei } from "./gleif-client.js";
import { FIELD_SEMANTICS } from "./source-semantics.js";

export const GLEIF_CORPORATE_VERSION = "ownership-gleif-corporate-v1";

/**
 * Enrich registered company entities with GLEIF parent (entity→entity CONTROLLED_BY).
 * Does NOT create hotel→parent unless explicit economic evidence exists elsewhere.
 *
 * @param {object} repo
 * @param {string} hotelId — for audit trail on research run only
 * @param {object[]} registeredEntities
 * @param {string} runId
 * @param {object} env
 */
export async function enrichCorporateGraphWithGleif(
  repo,
  hotelId,
  registeredEntities,
  runId,
  env
) {
  const staged = {
    entity_relationships: [],
    evidence: [],
    review: [],
    metrics: { lookups: 0, lei_matches: 0, parents: 0 },
    enriched_entities: [],
  };

  const enabled =
    String(env?.OWNERSHIP_GLEIF_ENABLE || process.env.OWNERSHIP_GLEIF_ENABLE || "1").trim() !==
    "0";
  if (!enabled) return staged;

  const seen = new Set();
  for (const ent of registeredEntities || []) {
    if (!ent?.entity_id || seen.has(ent.entity_id)) continue;
    seen.add(ent.entity_id);

    staged.metrics.lookups += 1;
    let lei = ent.lei || ent.identifiers?.find((i) => i.kind === "lei")?.value;

    if (!lei) {
      const jurisdiction =
        ent.country ||
        ent.jurisdiction ||
        ent.identifiers?.find((i) => i.country)?.country;
      const search = await searchLeiByLegalName(ent.legal_name, {
        jurisdiction: jurisdiction?.length === 2 ? jurisdiction : null,
        env,
      });
      if (search.ok && search.records.length === 1) {
        lei = search.records[0].lei;
        staged.metrics.lei_matches += 1;
        const updated = await repo.upsertEntity({
          ...ent,
          lei,
          identifiers: mergeId(ent.identifiers, { kind: "lei", value: lei }),
        });
        staged.enriched_entities.push(updated);
      } else if (search.ok && search.records.length > 1) {
        staged.review.push({
          issue_type: "entity_ambiguous",
          summary: `GLEIF ambiguous for registered company ${ent.legal_name}`,
        });
        continue;
      } else {
        continue;
      }
    } else {
      staged.metrics.lei_matches += 1;
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

    // Entity → entity corporate parent (NOT hotel ownership)
    const corpRel = await repo.upsertRelationship(
      createOwnershipRelationship({
        subject_entity_id: ent.entity_id,
        relationship_type: "CONTROLLED_BY",
        object_entity_id: parentResolved.entity.entity_id,
        confidence: 0.88,
        verification_status: "high",
        research_run_id: runId,
      })
    );
    staged.entity_relationships.push(corpRel);
    staged.evidence.push(
      await repo.addEvidence(
        createRelationshipEvidence({
          relationship_id: corpRel.relationship_id,
          source: "gleif",
          source_type: "corporate_registry",
          source_url: `https://search.gleif.org/#/record/${lei}`,
          extracted_claim: `GLEIF: ${ent.legal_name} direct parent ${parentResolved.entity.legal_name}`,
          confidence: 0.88,
          source_authority: 0.93,
          extraction_method: "gleif_corporate_graph",
          researcher: GLEIF_CORPORATE_VERSION,
          notes: FIELD_SEMANTICS["gleif.direct_parent"].notes,
        })
      )
    );
  }

  return staged;
}

/**
 * Optional: if hotel has registered company with GLEIF parent AND no economic owner yet,
 * stage a PROBABLE SPONSORED_BY with explicit caveat — only at needs_review/probable max.
 * Disabled by default; requires OWNERSHIP_GLEIF_INFER_ECONOMIC=1
 */
export async function inferEconomicOwnerFromCorporateChain(
  repo,
  hotelId,
  registeredEntityId,
  runId,
  env
) {
  const out = { relationships: [], evidence: [], review: [] };
  if (
    String(env?.OWNERSHIP_GLEIF_INFER_ECONOMIC || "0").trim() !== "1"
  ) {
    return out;
  }

  const corpRels = await repo.listRelationshipsForEntity?.(registeredEntityId);
  if (!corpRels?.length) return out;

  const parentRel = corpRels.find((r) => r.relationship_type === "CONTROLLED_BY");
  if (!parentRel) return out;

  const parent = await repo.getEntity(parentRel.object_entity_id);
  if (!parent) return out;

  out.review.push({
    issue_type: "weak_ownership_candidate",
    summary: `Corporate chain inference only: ${parent.legal_name} is GLEIF parent of registered company — NOT verified hotel economic owner`,
  });

  const scored = scoreOwnershipEvidence("corporate_registry", {
    relationshipType: "SPONSORED_BY",
    relationshipExplicitness: 0.45,
    entityName: parent.legal_name,
    propertyIdentityMatch: 0.4,
    pageBacked: true,
  });

  const rel = await repo.upsertRelationship(
    createOwnershipRelationship({
      subject_hotel_id: hotelId,
      relationship_type: "SPONSORED_BY",
      object_entity_id: parent.entity_id,
      confidence: Math.min(scored.confidence, 0.55),
      verification_status: "needs_review",
      research_run_id: runId,
    })
  );
  out.relationships.push(rel);
  return out;
}

function mergeId(list, id) {
  const out = Array.isArray(list) ? [...list] : [];
  const key = `${id.kind}|${id.value}`;
  if (!out.some((x) => `${x.kind}|${x.value}` === key)) out.push(id);
  return out;
}
