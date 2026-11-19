/**
 * Idempotent Market Alerts ↔ HPC hotel association planner.
 * Default: dry-run only. Apply requires MARKET_ALERTS_HOTEL_LINK_APPLY=1.
 * Does not create article rows — only proposes Tag merges (`hpc:<recId>`).
 */
import {
  classifyHotelArticleRelevance,
  HE_NEWS_VISIBLE_MATCHES,
  planHpcHotelAssociation,
  buildHotelNewsIdentity,
  HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
} from "./hotel-news-from-market-alerts-v1.js";

export const MARKET_ALERTS_HOTEL_ASSOC_SYNC_V1 = "market-alerts-hotel-association-sync-v1";

/**
 * @param {Array<{ id: string, fields: object }>} alertRecords
 * @param {Array<object>} hotels — HPC identity objects
 * @param {{ apply?: boolean }} [opts]
 */
export function planMarketAlertsHotelAssociations(alertRecords = [], hotels = [], opts = {}) {
  const applyAuthorized =
    opts.apply === true && String(process.env.MARKET_ALERTS_HOTEL_LINK_APPLY || "") === "1";
  const identities = hotels.map((h) => buildHotelNewsIdentity(h)).filter((h) => h.hpcId && h.name);
  const plans = [];
  const seenPair = new Set();

  for (const rec of alertRecords) {
    const fields = rec.fields || {};
    const tags = fields.Tags || fields.tags || [];
    for (const identity of identities) {
      const verdict = classifyHotelArticleRelevance(identity, fields);
      if (!HE_NEWS_VISIBLE_MATCHES.has(verdict.classification)) continue;
      const pairKey = `${rec.id}|${identity.hpcId}`;
      if (seenPair.has(pairKey)) continue;
      seenPair.add(pairKey);
      const tagPlan = planHpcHotelAssociation(tags, identity.hpcId);
      plans.push({
        alertId: rec.id,
        hpcId: identity.hpcId,
        hotelName: identity.name,
        classification: verdict.classification,
        reasons: verdict.reasons,
        tagPlan,
        applyAuthorized,
        wouldWrite: Boolean(tagPlan.changed && applyAuthorized),
      });
    }
  }

  return {
    version: MARKET_ALERTS_HOTEL_ASSOC_SYNC_V1,
    newsVersion: HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
    applyAuthorized,
    planned: plans.length,
    wouldWriteCount: plans.filter((p) => p.wouldWrite).length,
    alreadyAssociated: plans.filter((p) => p.tagPlan && p.tagPlan.reason === "already_associated")
      .length,
    plans,
  };
}
