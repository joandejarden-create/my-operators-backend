/**
 * In-memory OwnershipRepository — tests and injectable service double.
 * Lookup-only name/identifier APIs never merge entities.
 */

import { normalizeEntityName } from "./ids.js";
import {
  validateOwnershipEntity,
  validateEntityAlias,
  validateOwnershipRelationship,
  validateRelationshipEvidence,
  validateResearchRun,
  validateIntelligenceObservation,
  validateResearchDossier,
  assertValid,
} from "./validate.js";
import { FORBIDDEN_EVIDENCE_SOURCES } from "./ontology.js";
import { OWNERSHIP_REPOSITORY_VERSION } from "./repository.js";

function clone(value) {
  return structuredClone(value);
}

/**
 * @param {object} [opts]
 */
export function createMemoryOwnershipRepository(opts = {}) {
  /** @type {Map<string, object>} */
  const entities = opts.entities || new Map();
  /** @type {Map<string, object>} */
  const aliases = opts.aliases || new Map();
  /** @type {Map<string, object>} */
  const relationships = opts.relationships || new Map();
  /** @type {Map<string, object[]>} */
  const evidenceByRel = opts.evidenceByRel || new Map();
  /** @type {Map<string, object>} */
  const runs = opts.runs || new Map();
  /** @type {Map<string, object>} */
  const observations = opts.observations || new Map();
  /** @type {Map<string, object>} */
  const dossiers = opts.dossiers || new Map();

  return {
    version: OWNERSHIP_REPOSITORY_VERSION,
    kind: "memory",

    async getEntity(entityId) {
      const id = String(entityId || "").trim();
      const row = entities.get(id);
      return row ? clone(row) : null;
    },

    async upsertEntity(entity) {
      const row = { ...entity, updated_at: new Date().toISOString() };
      assertValid(validateOwnershipEntity(row), "upsertEntity");
      entities.set(row.entity_id, clone(row));
      return clone(row);
    },

    async addAlias(alias) {
      const row = { ...alias };
      assertValid(validateEntityAlias(row), "addAlias");
      if (!entities.has(row.entity_id)) {
        throw new Error("addAlias: entity_not_found");
      }
      aliases.set(row.alias_id, clone(row));
      return clone(row);
    },

    async listAliases(entityId) {
      const id = String(entityId || "").trim();
      return [...aliases.values()]
        .filter((a) => a.entity_id === id)
        .map(clone);
    },

    /**
     * Lookup candidates only — never merges.
     */
    async findByIdentifier(kind, value) {
      const k = String(kind || "").trim().toLowerCase();
      const v = String(value || "").trim().toLowerCase();
      if (!k || !v) return [];
      const out = [];
      for (const ent of entities.values()) {
        if (k === "lei" && String(ent.lei || "").trim().toLowerCase() === v) {
          out.push(clone(ent));
          continue;
        }
        for (const id of ent.identifiers || []) {
          if (
            String(id.kind || "").trim().toLowerCase() === k &&
            String(id.value || "").trim().toLowerCase() === v
          ) {
            out.push(clone(ent));
            break;
          }
        }
      }
      return out;
    },

    /**
     * Lookup candidates by normalized legal/display/alias name — never merges.
     */
    async findByNormalizedName(name) {
      const needle = normalizeEntityName(name);
      if (!needle) return [];
      const out = [];
      const seen = new Set();
      for (const ent of entities.values()) {
        const legal = normalizeEntityName(ent.legal_name);
        const display = normalizeEntityName(ent.display_name);
        if (legal === needle || display === needle) {
          seen.add(ent.entity_id);
          out.push(clone(ent));
        }
      }
      for (const alias of aliases.values()) {
        if (alias.normalized_alias === needle && !seen.has(alias.entity_id)) {
          const ent = entities.get(alias.entity_id);
          if (ent) {
            seen.add(ent.entity_id);
            out.push(clone(ent));
          }
        }
      }
      return out;
    },

    async getRelationship(relationshipId) {
      const id = String(relationshipId || "").trim();
      const row = relationships.get(id);
      return row ? clone(row) : null;
    },

    async upsertRelationship(rel) {
      const row = { ...rel, updated_at: new Date().toISOString() };
      assertValid(validateOwnershipRelationship(row), "upsertRelationship");
      if (!entities.has(row.object_entity_id)) {
        throw new Error("upsertRelationship: object_entity_not_found");
      }
      if (row.subject_entity_id && !entities.has(row.subject_entity_id)) {
        throw new Error("upsertRelationship: subject_entity_not_found");
      }
      relationships.set(row.relationship_id, clone(row));
      return clone(row);
    },

    async listRelationshipsForHotel(hotelId, listOpts = {}) {
      const hid = String(hotelId || "").trim();
      let rows = [...relationships.values()].filter(
        (r) => r.subject_hotel_id === hid
      );
      if (listOpts.currentOnly) {
        rows = rows.filter((r) => r.is_current !== false);
      }
      if (listOpts.relationship_type) {
        rows = rows.filter(
          (r) => r.relationship_type === listOpts.relationship_type
        );
      }
      return rows.map(clone);
    },

    async listRelationshipsForEntity(entityId, listOpts = {}) {
      const eid = String(entityId || "").trim();
      let rows = [...relationships.values()].filter(
        (r) =>
          r.object_entity_id === eid || r.subject_entity_id === eid
      );
      if (listOpts.currentOnly) {
        rows = rows.filter((r) => r.is_current !== false);
      }
      return rows.map(clone);
    },

    async addEvidence(ev) {
      const row = { ...ev };
      assertValid(validateRelationshipEvidence(row), "addEvidence");
      if (FORBIDDEN_EVIDENCE_SOURCES.includes(row.source)) {
        throw new Error("addEvidence: forbidden_evidence_source");
      }
      if (!relationships.has(row.relationship_id)) {
        throw new Error("addEvidence: relationship_not_found");
      }
      const list = evidenceByRel.get(row.relationship_id) || [];
      list.push(clone(row));
      evidenceByRel.set(row.relationship_id, list);
      return clone(row);
    },

    async listEvidence(relationshipId) {
      const id = String(relationshipId || "").trim();
      return (evidenceByRel.get(id) || []).map(clone);
    },

    async createResearchRun(run) {
      const row = { ...run };
      assertValid(validateResearchRun(row), "createResearchRun");
      runs.set(row.run_id, clone(row));
      return clone(row);
    },

    async updateResearchRun(run) {
      const row = { ...run };
      assertValid(validateResearchRun(row), "updateResearchRun");
      if (!runs.has(row.run_id)) {
        throw new Error("updateResearchRun: run_not_found");
      }
      runs.set(row.run_id, clone(row));
      return clone(row);
    },

    async getResearchRun(runId) {
      const id = String(runId || "").trim();
      const row = runs.get(id);
      return row ? clone(row) : null;
    },

    async addObservation(obs) {
      const row = { ...obs };
      assertValid(validateIntelligenceObservation(row), "addObservation");
      observations.set(row.observation_id, clone(row));
      return clone(row);
    },

    async listObservationsForHotel(hotelId) {
      const hid = String(hotelId || "").trim();
      return [...observations.values()]
        .filter(
          (o) =>
            (o.subject_type === "hotel" && o.subject_id === hid) ||
            (o.related_hotel_ids || []).includes(hid)
        )
        .map(clone);
    },

    async listObservationsForSubject(subjectType, subjectId) {
      const t = String(subjectType || "").trim();
      const id = String(subjectId || "").trim();
      return [...observations.values()]
        .filter((o) => o.subject_type === t && o.subject_id === id)
        .map(clone);
    },

    async upsertResearchDossier(dossier) {
      const row = { ...dossier, updated_at: new Date().toISOString() };
      assertValid(validateResearchDossier(row), "upsertResearchDossier");
      dossiers.set(row.dossier_id, clone(row));
      return clone(row);
    },

    async getResearchDossier(dossierId) {
      const id = String(dossierId || "").trim();
      const row = dossiers.get(id);
      return row ? clone(row) : null;
    },

    async listResearchDossiersForHotel(hotelId) {
      const hid = String(hotelId || "").trim();
      return [...dossiers.values()].filter((d) => d.hotel_id === hid).map(clone);
    },

    /** Test/debug snapshot — not part of public MCP contract. */
    _debugSnapshot() {
      return {
        entities: [...entities.values()].map(clone),
        aliases: [...aliases.values()].map(clone),
        relationships: [...relationships.values()].map(clone),
        evidence: [...evidenceByRel.entries()].flatMap(([, rows]) =>
          rows.map(clone)
        ),
        runs: [...runs.values()].map(clone),
        observations: [...observations.values()].map(clone),
        dossiers: [...dossiers.values()].map(clone),
      };
    },
  };
}
