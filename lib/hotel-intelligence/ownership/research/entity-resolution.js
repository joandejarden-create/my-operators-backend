/**
 * Conservative corporate entity resolution (P1D).
 * Identifier match first. Name similarity alone never auto-merges.
 */

import { normalizeEntityName } from "../ids.js";
import {
  createOwnershipEntity,
  createEntityAlias,
} from "../schemas.js";

export const ENTITY_RESOLUTION_VERSION = "ownership-entity-resolution-v1";

/**
 * @param {object} repo
 * @param {{
 *   legal_name: string,
 *   display_name?: string,
 *   jurisdiction?: string|null,
 *   identifiers?: {kind:string,value:string,country?:string}[],
 *   entity_type?: string,
 *   lei?: string|null,
 * }} candidate
 */
export async function resolveOrCreateEntity(repo, candidate) {
  const name = String(candidate.legal_name || "").trim();
  const identifiers = Array.isArray(candidate.identifiers)
    ? candidate.identifiers.filter((i) => i?.kind && i?.value)
    : [];
  const notes = [];

  // 1) Exact identifier match
  for (const id of identifiers) {
    const hits = await repo.findByIdentifier(id.kind, id.value);
    if (hits.length === 1) {
      notes.push(`matched_identifier:${id.kind}`);
      const hit = hits[0];
      const existingIds = Array.isArray(hit.identifiers) ? hit.identifiers : [];
      const merged = mergeIdentifiers(existingIds, identifiers);
      const next = {
        ...hit,
        identifiers: merged,
        website: candidate.website || hit.website || null,
        domains: mergeDomains(hit.domains, candidate.domains),
      };
      await repo.upsertEntity(next);
      if (candidate.display_name) {
        await addEntityAliasIfDistinct(
          repo,
          hit.entity_id,
          candidate.display_name,
          "entity_resolution"
        );
      }
      return {
        entity: next,
        ambiguity: false,
        created: false,
        match_method: `identifier:${id.kind}`,
        notes,
      };
    }
    if (hits.length > 1) {
      notes.push(`ambiguous_identifier:${id.kind}`);
      return {
        entity: null,
        ambiguity: true,
        created: false,
        match_method: `identifier_ambiguous:${id.kind}`,
        notes,
      };
    }
  }

  // 2) LEI field
  if (candidate.lei) {
    const hits = await repo.findByIdentifier("lei", candidate.lei);
    if (hits.length === 1) {
      return {
        entity: hits[0],
        ambiguity: false,
        created: false,
        match_method: "lei",
        notes: [...notes, "matched_lei"],
      };
    }
  }

  // 3) Exact normalized legal name — only if single hit AND (identifier empty OR jurisdiction aligns)
  const norm = normalizeEntityName(name);
  if (norm) {
    const hits = await repo.findByNormalizedName(name);
    if (hits.length === 1) {
      const hit = hits[0];
      // Refuse merge when both sides have conflicting strong identifiers
      const conflict = conflictingIdentifiers(hit, identifiers);
      if (conflict) {
        notes.push(`name_match_blocked_identifier_conflict:${conflict}`);
        // Fall through to create separate entity
      } else {
        // Attach new identifiers / alias if needed
        if (identifiers.length) {
          const existingIds = Array.isArray(hit.identifiers) ? hit.identifiers : [];
          const merged = mergeIdentifiers(existingIds, identifiers);
          await repo.upsertEntity({ ...hit, identifiers: merged });
        }
        notes.push("matched_exact_normalized_name");
        return {
          entity: hit,
          ambiguity: false,
          created: false,
          match_method: "exact_normalized_name",
          notes,
        };
      }
    } else if (hits.length > 1) {
      notes.push("ambiguous_normalized_name");
      return {
        entity: null,
        ambiguity: true,
        created: false,
        match_method: "name_ambiguous",
        notes,
      };
    }
  }

  // 4) Create new entity — never fuzzy-merge
  const entity = await repo.upsertEntity(
    createOwnershipEntity({
      legal_name: name,
      display_name: candidate.display_name || name,
      entity_type: candidate.entity_type || "company",
      country: candidate.jurisdiction || null,
      identifiers,
      lei: candidate.lei || null,
    })
  );
  notes.push("created_new_entity");
  return {
    entity,
    ambiguity: false,
    created: true,
    match_method: "create",
    notes,
  };
}

/**
 * Register an alias without merging entities.
 */
export async function addEntityAliasIfDistinct(repo, entityId, aliasName, source) {
  const name = String(aliasName || "").trim();
  if (!name) return null;
  const entity = await repo.getEntity(entityId);
  if (!entity) return null;
  if (normalizeEntityName(entity.legal_name) === normalizeEntityName(name)) {
    return null;
  }
  return repo.addAlias(
    createEntityAlias({
      entity_id: entityId,
      alias: name,
      source: source || "entity_resolution",
    })
  );
}

function conflictingIdentifiers(entity, incoming) {
  const existing = Array.isArray(entity?.identifiers) ? entity.identifiers : [];
  for (const id of incoming) {
    const sameKind = existing.find(
      (e) =>
        String(e.kind).toLowerCase() === String(id.kind).toLowerCase() &&
        String(e.value).replace(/\W/g, "").toLowerCase() !==
          String(id.value).replace(/\W/g, "").toLowerCase()
    );
    if (sameKind) return id.kind;
  }
  return null;
}

function mergeIdentifiers(existing, incoming) {
  const out = [...existing];
  const seen = new Set(
    existing.map(
      (e) =>
        `${String(e.kind).toLowerCase()}|${String(e.value).replace(/\W/g, "").toLowerCase()}`
    )
  );
  for (const id of incoming) {
    const key = `${String(id.kind).toLowerCase()}|${String(id.value).replace(/\W/g, "").toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(id);
  }
  return out;
}

function mergeDomains(existing, incoming) {
  const out = Array.isArray(existing) ? [...existing] : [];
  const seen = new Set(out.map((d) => String(d).toLowerCase()));
  for (const d of incoming || []) {
    const k = String(d || "").trim().toLowerCase();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(d);
  }
  return out;
}
