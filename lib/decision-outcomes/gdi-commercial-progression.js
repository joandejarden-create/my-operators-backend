import {
  listDecisionSummaries,
  loadDecision,
  loadEvents,
} from "./persistence.js";
import { PRODUCT_MODULE } from "./types.js";
import { projectCommercialProgression } from "./projection.js";
import {
  getCachedProgressionMap,
  setCachedProgressionMap,
  invalidateGdiHotelReadCache,
} from "../group-demand-intelligence/read-cache.js";

/**
 * @returns {Promise<Map<string, object>>} subjectId → commercialProgression DTO
 */
export async function loadGdiCommercialProgressionBySubject(hotelId) {
  const hotel = String(hotelId || "").trim();
  const map = new Map();
  if (!hotel) return map;

  const cached = getCachedProgressionMap(hotel);
  if (cached) return cached;

  const summaries = (await listDecisionSummaries(hotel)).filter(
    (d) => d.productModule === PRODUCT_MODULE.GDI
  );

  // Parallelize per-decision loads (was sequential N+1).
  await Promise.all(
    summaries.map(async (s) => {
      const decision = await loadDecision(hotel, s.decisionId);
      if (!decision?.subjectId) return;
      if (String(decision.hotelId) !== hotel) return;
      const events = await loadEvents(hotel, s.decisionId);
      map.set(
        String(decision.subjectId),
        projectCommercialProgression(decision, events)
      );
    })
  );

  setCachedProgressionMap(hotel, map);
  return map;
}

/** Customer-safe fields only (no raw event IDs / causal jargon). */
export function toCustomerCommercialProgressionDto(progression) {
  if (!progression) return null;
  return {
    currentStatusLabel: progression.currentStatusLabel || null,
    compactStatusLabel: progression.compactStatusLabel || null,
    funnelStageLabel: progression.funnelStageLabel || null,
    latestActionLabel: progression.latestActionLabel || null,
    latestActionDate: progression.latestActionDate || null,
    latestOutcomeLabel: progression.latestOutcomeLabel || null,
    latestOutcomeDate: progression.latestOutcomeDate || null,
  };
}

/** Call after GDI validation/action/outcome writes for this hotel. */
export function invalidateGdiCommercialProgressionCache(hotelId) {
  invalidateGdiHotelReadCache(hotelId);
}
