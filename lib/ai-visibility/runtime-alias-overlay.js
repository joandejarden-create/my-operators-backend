/**
 * Runtime alias overlay for AI Visibility Phase 2B.
 * Separate from Airtable Brand Alias Mapping / Operator Aliases SSOT.
 * Never writes Airtable.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { normalizeMatchKey } from "./normalize-entities.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_OVERLAY_PATH = path.resolve(
  __dirname,
  "../../fixtures/ai-visibility/runtime-alias-overlay-phase2b.json"
);

export const RUNTIME_ALIAS_OVERLAY_VERSION = "ai_visibility_runtime_alias_overlay_v1";

/**
 * @param {string} [overlayPath]
 */
export function loadRuntimeAliasOverlay(overlayPath = DEFAULT_OVERLAY_PATH) {
  const abs = path.resolve(overlayPath);
  if (!fs.existsSync(abs)) {
    return {
      version: RUNTIME_ALIAS_OVERLAY_VERSION,
      overlayId: null,
      aliases: [],
      founderReviewOnly: [],
      blockedBareParents: [],
      path: abs,
      loaded: false,
    };
  }
  const raw = JSON.parse(fs.readFileSync(abs, "utf8"));
  return {
    version: RUNTIME_ALIAS_OVERLAY_VERSION,
    overlayId: raw.overlayId || null,
    aliases: Array.isArray(raw.aliases) ? raw.aliases : [],
    aliasesRejected: Array.isArray(raw.aliasesRejected) ? raw.aliasesRejected : [],
    founderReviewOnly: Array.isArray(raw.founderReviewOnly) ? raw.founderReviewOnly : [],
    blockedBareParents: Array.isArray(raw.blockedBareParents) ? raw.blockedBareParents : [],
    path: abs,
    loaded: true,
    airtableWrites: 0,
  };
}

/**
 * Merge approved runtime aliases onto entity objects (clone; no mutation of SSOT source).
 * Skips blocked bare parents and aliases that would collide across entities.
 * @param {object[]} entities
 * @param {ReturnType<typeof loadRuntimeAliasOverlay>} overlay
 */
export function applyRuntimeAliasOverlay(entities, overlay) {
  const list = (entities || []).map((e) => ({
    ...e,
    aliases: [...(e.aliases || [])],
  }));
  const byId = new Map(list.map((e) => [e.id, e]));
  const blocked = new Set(
    (overlay?.blockedBareParents || []).map((x) => normalizeMatchKey(x))
  );
  const applied = [];
  const skipped = [];

  for (const row of overlay?.aliases || []) {
    if (row.reviewStatus !== "approved_for_runtime_test") {
      skipped.push({ alias: row.alias, reason: "not_approved_for_runtime_test" });
      continue;
    }
    const aliasKey = normalizeMatchKey(row.alias);
    if (!aliasKey) {
      skipped.push({ alias: row.alias, reason: "empty_alias" });
      continue;
    }
    if (blocked.has(aliasKey)) {
      skipped.push({ alias: row.alias, reason: "blocked_bare_parent" });
      continue;
    }
    const entity = byId.get(row.canonicalEntityId);
    if (!entity) {
      skipped.push({ alias: row.alias, reason: "canonical_entity_missing_from_index" });
      continue;
    }
    if (entity.entityType !== row.entityType) {
      skipped.push({ alias: row.alias, reason: "entity_type_mismatch" });
      continue;
    }
    // Collision: another entity already owns this alias/name key
    const collision = list.find(
      (e) =>
        e.id !== entity.id &&
        (normalizeMatchKey(e.name) === aliasKey ||
          (e.aliases || []).some((a) => normalizeMatchKey(a) === aliasKey))
    );
    if (collision) {
      skipped.push({
        alias: row.alias,
        reason: "collision_with_other_entity",
        otherEntityId: collision.id,
      });
      continue;
    }
    if (!entity.aliases.some((a) => normalizeMatchKey(a) === aliasKey)) {
      entity.aliases.push(row.alias);
    }
    applied.push({
      alias: row.alias,
      canonicalEntityId: entity.id,
      canonicalName: entity.name,
      entityType: entity.entityType,
      evidenceCount: row.evidenceCount ?? null,
      reviewStatus: row.reviewStatus,
      reason: row.reason || null,
    });
  }

  return {
    entities: list,
    applied,
    skipped,
    founderReviewOnly: overlay?.founderReviewOnly || [],
    overlayVersion: RUNTIME_ALIAS_OVERLAY_VERSION,
    airtableWrites: 0,
  };
}
