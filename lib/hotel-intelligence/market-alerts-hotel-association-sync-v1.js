/**
 * Idempotent Market Alerts ↔ HPC hotel association planner.
 * Default: dry-run only. Apply requires MARKET_ALERTS_HOTEL_LINK_APPLY=1.
 * Does not create article rows — only proposes Tag merges (`hpc:<recId>`)
 * and optional designed multiline `HPC Hotel Record IDs` merges (multi-hotel).
 */
import {
  classifyHotelArticleRelevance,
  HE_NEWS_VISIBLE_MATCHES,
  planHpcHotelAssociation,
  planHpcAssocFieldMerge,
  buildHotelNewsIdentity,
  HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
  MAP_HOTEL_ASSOC,
} from "./hotel-news-from-market-alerts-v1.js";
import { proposedHpcHotelRecordIdsFieldSchema } from "./market-alerts-evidence-citations-v1.js";

export const MARKET_ALERTS_HOTEL_ASSOC_SYNC_V1 = "market-alerts-hotel-association-sync-v1";

/**
 * @param {Array<{ id: string, fields: object }>} alertRecords
 * @param {Array<object>} hotels — HPC identity objects
 * @param {{ apply?: boolean, assocFieldAvailable?: boolean }} [opts]
 */
export function planMarketAlertsHotelAssociations(alertRecords = [], hotels = [], opts = {}) {
  const applyAuthorized =
    opts.apply === true && String(process.env.MARKET_ALERTS_HOTEL_LINK_APPLY || "") === "1";
  const identities = hotels.map((h) => buildHotelNewsIdentity(h)).filter((h) => h.hpcId && h.name);
  const plans = [];
  const seenPair = new Set();
  const assocFieldAvailable = opts.assocFieldAvailable === true;

  for (const rec of alertRecords) {
    const fields = rec.fields || {};
    const tags = fields.Tags || fields.tags || [];
    const assocRaw =
      fields[MAP_HOTEL_ASSOC.hpcHotelRecordIds] || fields.hpcHotelRecordIds || "";
    for (const identity of identities) {
      const verdict = classifyHotelArticleRelevance(identity, fields);
      if (!HE_NEWS_VISIBLE_MATCHES.has(verdict.classification)) continue;
      const pairKey = `${rec.id}|${identity.hpcId}`;
      if (seenPair.has(pairKey)) continue;
      seenPair.add(pairKey);
      const tagPlan = planHpcHotelAssociation(tags, identity.hpcId);
      const fieldPlan = planHpcAssocFieldMerge(assocRaw, identity.hpcId);
      const wouldWriteTags = Boolean(tagPlan.changed && applyAuthorized);
      const wouldWriteField = Boolean(
        assocFieldAvailable && fieldPlan.changed && applyAuthorized
      );
      plans.push({
        alertId: rec.id,
        hpcId: identity.hpcId,
        hotelName: identity.name,
        classification: verdict.classification,
        reasons: verdict.reasons,
        tagPlan,
        assocFieldPlan: fieldPlan,
        assocFieldName: MAP_HOTEL_ASSOC.hpcHotelRecordIds,
        assocFieldAvailable,
        applyAuthorized,
        wouldWrite: wouldWriteTags || wouldWriteField,
        wouldWriteTags,
        wouldWriteField,
        createsArticleDuplicate: false,
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
    multiHotelCapable: true,
    proposedFieldSchema: proposedHpcHotelRecordIdsFieldSchema(),
    plans,
  };
}
