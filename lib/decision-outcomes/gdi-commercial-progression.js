/**
 * Load customer-safe commercial progression overlays keyed by opportunity subjectId.
 */

import {
  listDecisionSummaries,
  loadDecision,
  loadEvents,
} from "./persistence.js";
import { PRODUCT_MODULE } from "./types.js";
import { projectCommercialProgression } from "./projection.js";

/**
 * @returns {Promise<Map<string, object>>} subjectId → commercialProgression DTO
 */
export async function loadGdiCommercialProgressionBySubject(hotelId) {
  const map = new Map();
  const hotel = String(hotelId || "").trim();
  if (!hotel) return map;

  const summaries = (await listDecisionSummaries(hotel)).filter(
    (d) => d.productModule === PRODUCT_MODULE.GDI
  );

  for (const s of summaries) {
    const decision = await loadDecision(hotel, s.decisionId);
    if (!decision?.subjectId) continue;
    if (String(decision.hotelId) !== hotel) continue;
    const events = await loadEvents(hotel, s.decisionId);
    map.set(
      String(decision.subjectId),
      projectCommercialProgression(decision, events)
    );
  }
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
