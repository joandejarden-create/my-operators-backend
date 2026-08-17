/**
 * Golden Set scoring entity index — expands Phase 2C universe with
 * human-reviewed Golden Set subject entities (no Airtable writes).
 * Does not invent aliases; runtime overlay still applies separately.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildAiVisibilityEntityIndex } from "../entity-index.js";
import { loadRuntimeAliasOverlay } from "../runtime-alias-overlay.js";
import { normalizeMatchKey } from "../normalize-entities.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.resolve(__dirname, "../../../fixtures/ai-visibility");
const PHASE2C_UNIVERSE_PATH = path.join(FIXTURES, "phase2c-entity-universe.json");
const CANDIDATES_PATH = path.join(FIXTURES, "ai-intelligence-golden-set-v2-candidates.json");
const V2_PATH = path.join(FIXTURES, "ai-intelligence-golden-set-v2.json");
const EXPANDED_UNIVERSE_PATH = path.join(FIXTURES, "golden-set-v2-entity-universe.json");

/**
 * Collect unique brand entities from Golden Set fixtures.
 */
export function collectGoldenSetSubjectEntities() {
  const byId = new Map();

  const ingest = (id, name, source) => {
    if (!id || !name) return;
    const existing = byId.get(id);
    if (!existing) {
      byId.set(id, {
        id,
        name: String(name).trim(),
        entityType: "brand",
        aliases: [],
        firstPartyDomains: [],
        parentCompany: null,
        isParentCompanyLabel: false,
        sourceSystem: source,
      });
      return;
    }
    // Prefer longer canonical display name when conflicting
    if (String(name).trim().length > existing.name.length) {
      existing.name = String(name).trim();
    }
  };

  if (fs.existsSync(PHASE2C_UNIVERSE_PATH)) {
    const uni = JSON.parse(fs.readFileSync(PHASE2C_UNIVERSE_PATH, "utf8"));
    for (const e of uni.entities || []) {
      ingest(e.id, e.name, "phase2c_entity_universe");
      const row = byId.get(e.id);
      if (row && Array.isArray(e.aliases)) {
        row.aliases = [...new Set([...(row.aliases || []), ...e.aliases])];
      }
      if (row && e.parentCompany) row.parentCompany = e.parentCompany;
      if (row && e.firstPartyDomains) {
        row.firstPartyDomains = [...new Set([...(row.firstPartyDomains || []), ...e.firstPartyDomains])];
      }
    }
  }

  if (fs.existsSync(CANDIDATES_PATH)) {
    const cand = JSON.parse(fs.readFileSync(CANDIDATES_PATH, "utf8"));
    for (const c of cand.cases || []) {
      ingest(c.canonicalEntityId, c.candidateEntity, "golden_set_v2_candidates");
    }
  }

  if (fs.existsSync(V2_PATH)) {
    const v2 = JSON.parse(fs.readFileSync(V2_PATH, "utf8"));
    for (const c of v2.cases || []) {
      ingest(
        c.canonicalEntityId,
        c.candidateEntity || c.entityName,
        "golden_set_v2"
      );
    }
  }

  return [...byId.values()].sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Persist expanded universe fixture for offline tests (labels unchanged).
 */
export function materializeGoldenSetEntityUniverse(options = {}) {
  const entities = collectGoldenSetSubjectEntities();
  const doc = {
    version: "ai_intelligence_golden_set_v2_entity_universe_v1",
    note:
      "Subject entities from Phase 2C + Golden Set v2 human-reviewed cases. Not LLM-invented. Aliases via runtime overlay.",
    entityCount: entities.length,
    entities,
  };
  if (options.write !== false) {
    fs.writeFileSync(EXPANDED_UNIVERSE_PATH, JSON.stringify(doc, null, 2), "utf8");
  }
  return { ...doc, path: EXPANDED_UNIVERSE_PATH };
}

/**
 * Build alias index for Golden Set scoring / hardening.
 */
export function buildGoldenSetScoringEntityIndex(options = {}) {
  let entities = collectGoldenSetSubjectEntities();
  if (fs.existsSync(EXPANDED_UNIVERSE_PATH)) {
    const expanded = JSON.parse(fs.readFileSync(EXPANDED_UNIVERSE_PATH, "utf8"));
    const byId = new Map(entities.map((e) => [e.id, e]));
    for (const e of expanded.entities || []) {
      if (!byId.has(e.id)) byId.set(e.id, e);
    }
    entities = [...byId.values()];
  }

  const overlay = options.runtimeOverlay || loadRuntimeAliasOverlay();
  const brands = entities.filter((e) => e.entityType === "brand");
  const operators = entities.filter((e) => e.entityType === "operator");

  return buildAiVisibilityEntityIndex({
    brands,
    operators,
    applyOverlay: options.applyOverlay !== false,
    runtimeOverlay: overlay,
  });
}

export function entityInScoringUniverse(canonicalName, index) {
  const key = normalizeMatchKey(canonicalName);
  return (index?.entities || []).some((e) => normalizeMatchKey(e.name) === key);
}

export { EXPANDED_UNIVERSE_PATH, PHASE2C_UNIVERSE_PATH };
