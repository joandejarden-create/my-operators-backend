/**
 * Dealality-wide canonical hotel identity resolver (HPC-only).
 *
 * Reuses:
 * - adp-gdi-canonical-identity (alias / ADP / GDI keys → HPC id)
 * - hpc-live-identity-collision-v1 (live name/alias/geo collision)
 * - property-census-identity-v1 (RECORD_ID ownership — HPC-first)
 *
 * Does not create a parallel hotel database.
 * Does not call legacy Hotel Census unless explicit migration opt-in is passed
 * through to ownership resolution (default: blocked).
 */

import { resolveCanonicalHotelId } from "./adp-gdi-canonical-identity.js";
import {
  performLiveHpcIdentityCollisionCheck,
  HPC_LIVE_OUTCOME,
} from "./hpc-live-identity-collision-v1.js";
import { fetchHpcRecord } from "./hpc-shared-upsert-v1.js";

export const DEALALITY_HPC_IDENTITY_RESOLVER_V1 = "DEALALITY_HPC_IDENTITY_RESOLVER_V1";
export const HPC_CANONICAL_TABLE_ID = "tbl9aY5ijiuIzzWam";
export const HPC_CANONICAL_TABLE_NAME = "Hotel Property Census";
export const HPC_CANONICAL_BASE_ID = "appCCUsuGsE1ifoLk";

export const RESOLVER_MATCH = Object.freeze({
  EXACT_HPC_ID: "EXACT_HPC_ID",
  ALIAS_MAP: "ALIAS_MAP",
  LIVE_COLLISION_VERIFIED: "LIVE_COLLISION_VERIFIED",
  LIVE_COLLISION_AMBIGUOUS: "LIVE_COLLISION_AMBIGUOUS",
  PROPOSED_NEW: "PROPOSED_NEW",
  UNRESOLVED: "UNRESOLVED",
  LOOKUP_UNAVAILABLE: "LOOKUP_UNAVAILABLE",
});

/**
 * Resolve a hotel key or identity packet to a canonical HPC id + evidence.
 *
 * @param {object} input
 * @param {string} [input.hotelKey] ADP / GDI / Airtable / alias key
 * @param {string} [input.hpcId] Known HPC id
 * @param {string} [input.hotelName]
 * @param {string[]} [input.aliases]
 * @param {string} [input.city]
 * @param {string} [input.country]
 * @param {string} [input.brand]
 * @param {boolean} [input.liveLookup=true]
 */
export async function resolveDealalityHotelIdentity(input = {}) {
  const startedAt = new Date().toISOString();
  const hotelKey = String(input.hotelKey || input.hpcId || "").trim();
  const evidence = [];

  if (hotelKey) {
    const fromAlias = resolveCanonicalHotelId(hotelKey);
    if (fromAlias && /^rec[A-Za-z0-9]{14}$/.test(fromAlias)) {
      evidence.push({
        method: RESOLVER_MATCH.ALIAS_MAP,
        canonicalHotelId: fromAlias,
        inputKey: hotelKey,
      });
      let record = null;
      try {
        record = await fetchHpcRecord(fromAlias);
      } catch {
        /* optional live confirm */
      }
      return {
        ok: true,
        contract: DEALALITY_HPC_IDENTITY_RESOLVER_V1,
        matchStatus: hotelKey === fromAlias ? RESOLVER_MATCH.EXACT_HPC_ID : RESOLVER_MATCH.ALIAS_MAP,
        canonicalHotelId: fromAlias,
        canonicalTableId: HPC_CANONICAL_TABLE_ID,
        confidence: hotelKey === fromAlias ? "HIGH" : "MEDIUM",
        record: record || null,
        candidates: [],
        evidence,
        legacyHotelCensusRuntimeDependency: false,
        startedAt,
        completedAt: new Date().toISOString(),
      };
    }
  }

  if (input.liveLookup === false || !input.hotelName) {
    return {
      ok: false,
      contract: DEALALITY_HPC_IDENTITY_RESOLVER_V1,
      matchStatus: RESOLVER_MATCH.UNRESOLVED,
      canonicalHotelId: null,
      canonicalTableId: HPC_CANONICAL_TABLE_ID,
      confidence: "NONE",
      record: null,
      candidates: [],
      evidence,
      legacyHotelCensusRuntimeDependency: false,
      startedAt,
      completedAt: new Date().toISOString(),
    };
  }

  let collision;
  try {
    collision = await performLiveHpcIdentityCollisionCheck({
      hotelName: input.hotelName,
      aliases: input.aliases || [],
      city: input.city,
      country: input.country,
      brand: input.brand,
      knownHpcId: input.hpcId || null,
    });
  } catch (err) {
    return {
      ok: false,
      contract: DEALALITY_HPC_IDENTITY_RESOLVER_V1,
      matchStatus: RESOLVER_MATCH.LOOKUP_UNAVAILABLE,
      canonicalHotelId: null,
      canonicalTableId: HPC_CANONICAL_TABLE_ID,
      confidence: "NONE",
      error: err?.message || String(err),
      record: null,
      candidates: [],
      evidence,
      legacyHotelCensusRuntimeDependency: false,
      startedAt,
      completedAt: new Date().toISOString(),
    };
  }

  evidence.push({
    method: "HPC_LIVE_IDENTITY_COLLISION_V1",
    outcome: collision?.outcome,
    hpcRecordId: collision?.hpcRecordId || null,
  });

  if (collision?.outcome === HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED && collision.hpcRecordId) {
    return {
      ok: true,
      contract: DEALALITY_HPC_IDENTITY_RESOLVER_V1,
      matchStatus: RESOLVER_MATCH.LIVE_COLLISION_VERIFIED,
      canonicalHotelId: collision.hpcRecordId,
      canonicalTableId: HPC_CANONICAL_TABLE_ID,
      confidence: "HIGH",
      record: collision.verifiedRecord || null,
      candidates: collision.candidates || [],
      evidence,
      legacyHotelCensusRuntimeDependency: false,
      startedAt,
      completedAt: new Date().toISOString(),
    };
  }

  if (collision?.outcome === HPC_LIVE_OUTCOME.HPC_IDENTITY_COLLISION_REQUIRES_REVIEW) {
    return {
      ok: false,
      contract: DEALALITY_HPC_IDENTITY_RESOLVER_V1,
      matchStatus: RESOLVER_MATCH.LIVE_COLLISION_AMBIGUOUS,
      canonicalHotelId: null,
      canonicalTableId: HPC_CANONICAL_TABLE_ID,
      confidence: "LOW",
      record: null,
      candidates: collision.candidates || [],
      evidence,
      legacyHotelCensusRuntimeDependency: false,
      startedAt,
      completedAt: new Date().toISOString(),
    };
  }

  if (collision?.outcome === HPC_LIVE_OUTCOME.HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT) {
    return {
      ok: true,
      contract: DEALALITY_HPC_IDENTITY_RESOLVER_V1,
      matchStatus: RESOLVER_MATCH.PROPOSED_NEW,
      canonicalHotelId: null,
      canonicalTableId: HPC_CANONICAL_TABLE_ID,
      confidence: "MEDIUM",
      proposeCreate: true,
      record: null,
      candidates: [],
      evidence,
      legacyHotelCensusRuntimeDependency: false,
      startedAt,
      completedAt: new Date().toISOString(),
    };
  }

  return {
    ok: false,
    contract: DEALALITY_HPC_IDENTITY_RESOLVER_V1,
    matchStatus: RESOLVER_MATCH.UNRESOLVED,
    canonicalHotelId: null,
    canonicalTableId: HPC_CANONICAL_TABLE_ID,
    confidence: "NONE",
    record: null,
    candidates: collision?.candidates || [],
    evidence,
    legacyHotelCensusRuntimeDependency: false,
    startedAt,
    completedAt: new Date().toISOString(),
  };
}
