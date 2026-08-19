/**
 * Peer-set governance (MVP): versioned config — no Airtable Peer Set table yet.
 * Global peer set + regional eligibility / override.
 * Membership is founder/admin governed only (no AI-generated membership).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const PEER_SET_CONFIG_VERSION = "ai_visibility_peer_sets_v1";

/** Historical monitoring peer set (2026-08-13 batches). Do not relabel history to v2. */
export const PEER_SET_ID_V1 = "peers_upper_upscale_brands_global_v1";

/** Showcase owner-decision cohort (Wave-1 / live Brand AI comparative reads). Frozen. */
export const PEER_SET_ID_V2 = "peers_uu_collection_lifestyle_owner_decision_v2";

/** Longitudinal multi-parent measurement cohort = frozen v2 + Radisson. Do not mutate v2. */
export const PEER_SET_ID_V3 = "peers_uu_collection_lifestyle_owner_decision_v3";

/** Full showcase portfolio monitoring universe (19 brands across 4 parents). Comparative rank stays on v2. */
export const PEER_SET_ID_V4 = "peers_showcase_portfolio_monitoring_v4";

/** Internal benchmark expansion = frozen v2 + 7 founder-approved INTERNAL_BENCHMARK_ONLY brands. Pilot cohort. */
export const PEER_SET_ID_V5 = "peers_uu_collection_lifestyle_owner_decision_v5";

/** Alias for presence-index pilot reads — do not use for live comparative rank (stays v2). */
export const PEER_SET_ID_INTERNAL_BENCHMARK = PEER_SET_ID_V5;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_PATH = path.join(
  __dirname,
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "peer-sets-v1.json"
);

/**
 * @param {string} [filePath]
 */
export function loadPeerSetConfig(filePath = DEFAULT_PATH) {
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return {
    ...raw,
    configVersion: raw.configVersion || PEER_SET_CONFIG_VERSION,
  };
}

/**
 * Resolve effective entity IDs for a peer set + commercial region.
 * Starts from Global entityIds, applies region override include/exclude.
 *
 * @param {{ peerSetId: string, commercialRegion?: string|null }} args
 * @param {object} [config]
 */
export function resolvePeerSetMembership(args, config) {
  const cfg = config || loadPeerSetConfig();
  const set = (cfg.peerSets || []).find((p) => p.peerSetId === args.peerSetId);
  if (!set) {
    return {
      ok: false,
      error: `peer_set_not_found:${args.peerSetId}`,
      entityIds: [],
      peerSetVersion: null,
    };
  }

  const base = [...(set.entityIds || [])];
  const region = args.commercialRegion || null;
  let entityIds = base;
  let overrideApplied = null;

  if (region && set.regionalOverrides?.[region]) {
    const ov = set.regionalOverrides[region];
    overrideApplied = region;
    const exclude = new Set(ov.excludeEntityIds || []);
    entityIds = base.filter((id) => !exclude.has(id));
    for (const id of ov.includeEntityIds || []) {
      if (!entityIds.includes(id)) entityIds.push(id);
    }
  }

  return {
    ok: true,
    peerSetId: set.peerSetId,
    name: set.name,
    entityType: set.entityType,
    geographyScope: "region",
    commercialRegion: region || "Global",
    peerSetVersion: set.version || cfg.configVersion,
    entityIds,
    overrideApplied,
    baseCount: base.length,
    effectiveCount: entityIds.length,
  };
}

/**
 * Governed brandId → brandName map from peer-set members (when present).
 * @param {string} peerSetId
 * @param {object} [config]
 * @returns {Record<string, string>}
 */
export function peerSetBrandNamesById(peerSetId, config) {
  const cfg = config || loadPeerSetConfig();
  const set = (cfg.peerSets || []).find((p) => p.peerSetId === peerSetId);
  const out = {};
  for (const m of set?.members || []) {
    if (m?.brandId && m?.brandName) out[String(m.brandId)] = String(m.brandName);
  }
  return out;
}

/**
 * Diff v1 vs v2 brand peer sets (IDs only).
 */
export function diffBrandPeerSetVersions(config) {
  const cfg = config || loadPeerSetConfig();
  const v1 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V1 }, cfg);
  const v2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, cfg);
  if (!v1.ok || !v2.ok) {
    return { ok: false, v1, v2 };
  }
  const s1 = new Set(v1.entityIds);
  const s2 = new Set(v2.entityIds);
  return {
    ok: true,
    V1_PRESERVED: true,
    V1_COUNT: v1.entityIds.length,
    V2_COUNT: v2.entityIds.length,
    ADDED: v2.entityIds.filter((id) => !s1.has(id)),
    REMOVED: v1.entityIds.filter((id) => !s2.has(id)),
    UNCHANGED: v1.entityIds.filter((id) => s2.has(id)),
    RANK_COMPARABILITY_RULE:
      "v1 vs v2 peer-set versions are NON_COMPARABLE for Competitive Position / rank trends",
  };
}
