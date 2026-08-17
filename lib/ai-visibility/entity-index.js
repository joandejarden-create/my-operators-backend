/**
 * Unified read-only AI Visibility entity index.
 * Deterministic ordering + version/hash for reproducibility.
 * Phase 2B: optional runtime alias overlay (not Airtable writes).
 */

import { createHash } from "crypto";
import { buildEntityAliasIndex } from "./normalize-entities.js";
import { loadLiveBrandEntities } from "./load-brands-live.js";
import { loadLiveOperatorEntities } from "./load-operators-live.js";
import {
  loadRuntimeAliasOverlay,
  applyRuntimeAliasOverlay,
} from "./runtime-alias-overlay.js";

export const ENTITY_INDEX_VERSION = "ai_visibility_entity_index_v1";

function cloneEntity(e) {
  return {
    entityType: e.entityType,
    canonicalEntityId: e.id,
    id: e.id,
    canonicalName: e.name,
    name: e.name,
    aliases: [...(e.aliases || [])],
    parentEntityId: e.parentEntityId || null,
    parentCompany: e.parentCompany || null,
    firstPartyDomains: [...(e.firstPartyDomains || [])],
    sourceSystem: e.sourceSystem || null,
    isParentCompanyLabel: Boolean(e.isParentCompanyLabel),
    brandStatus: e.brandStatus || null,
    submissionStatus: e.submissionStatus || null,
  };
}

/**
 * @param {{ brands?: object[], operators?: object[], runtimeOverlay?: object|null, applyOverlay?: boolean }} input
 */
export function buildAiVisibilityEntityIndex(input = {}) {
  let brands = (input.brands || []).map(cloneEntity);
  let operators = (input.operators || []).map(cloneEntity);
  let overlayMeta = null;

  if (input.applyOverlay !== false) {
    const overlay = input.runtimeOverlay || loadRuntimeAliasOverlay();
    if (overlay.loaded) {
      const merged = applyRuntimeAliasOverlay([...brands, ...operators], overlay);
      brands = merged.entities.filter((e) => e.entityType === "brand");
      operators = merged.entities.filter((e) => e.entityType === "operator");
      overlayMeta = {
        overlayId: overlay.overlayId,
        applied: merged.applied,
        skipped: merged.skipped,
        founderReviewOnly: merged.founderReviewOnly,
        airtableWrites: 0,
      };
    }
  }

  const seen = new Set();
  const entities = [];
  for (const e of [...brands, ...operators]) {
    const key = `${e.entityType}:${e.id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    entities.push(e);
  }

  entities.sort((a, b) => {
    if (a.entityType !== b.entityType) {
      return a.entityType.localeCompare(b.entityType);
    }
    return a.name.localeCompare(b.name, undefined, { sensitivity: "base" });
  });

  const fingerprint = createHash("sha256")
    .update(
      entities
        .map(
          (e) =>
            `${e.entityType}|${e.id}|${e.name}|${e.aliases.slice().sort().join(",")}`
        )
        .join("\n")
    )
    .digest("hex")
    .slice(0, 16);

  const aliasIndex = buildEntityAliasIndex(entities);

  return {
    version: ENTITY_INDEX_VERSION,
    fingerprint,
    entities,
    brands: entities.filter((e) => e.entityType === "brand"),
    operators: entities.filter((e) => e.entityType === "operator"),
    aliasIndex,
    overlayMeta,
    meta: {
      brandCount: brands.length,
      operatorCount: operators.length,
      entityCount: entities.length,
      runtimeOverlayApplied: Boolean(overlayMeta),
    },
  };
}

/**
 * Load live brands+operators and build index.
 */
export async function buildLiveAiVisibilityEntityIndex(opts = {}) {
  const brandResult = await loadLiveBrandEntities(opts.brandDeps || {});
  const operatorResult = await loadLiveOperatorEntities(opts.operatorDeps || {});
  const index = buildAiVisibilityEntityIndex({
    brands: brandResult.entities,
    operators: operatorResult.entities,
    applyOverlay: opts.applyOverlay !== false,
    runtimeOverlay: opts.runtimeOverlay,
  });
  return {
    index,
    brandMeta: brandResult.meta,
    operatorMeta: operatorResult.meta,
  };
}

/**
 * Build index from fixture universe JSON ({ entities: [...] }).
 * For unit tests only — never mix into live runs.
 * Default: overlay off so Phase 1/2A fixtures stay stable unless opted in.
 */
export function buildFixtureAiVisibilityEntityIndex(fixtureUniverse, opts = {}) {
  const entities = fixtureUniverse?.entities || fixtureUniverse || [];
  return buildAiVisibilityEntityIndex({
    brands: entities.filter((e) => e.entityType === "brand"),
    operators: entities.filter((e) => e.entityType === "operator"),
    applyOverlay: opts.applyOverlay === true,
    runtimeOverlay: opts.runtimeOverlay,
  });
}
