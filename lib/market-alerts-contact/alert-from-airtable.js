/**
 * Map MarketAlerts Airtable records → alert objects for stakeholder identification.
 */

import { MAP_ALERT } from "../../api/lib/market-alerts-rss-airtable.js";
import { MAP_INTEL } from "../../api/lib/market-alerts-intelligence-map.js";

export function alertFromAirtableRecord(rec) {
  const f = rec.fields || {};
  const actionable =
    !!f[MAP_INTEL.actionableOwner] ||
    !!f[MAP_INTEL.actionableBrand] ||
    !!f[MAP_INTEL.actionableOperator];
  const worthReviewing =
    !!f[MAP_INTEL.worthReviewingOwner] ||
    !!f[MAP_INTEL.worthReviewingBrand] ||
    !!f[MAP_INTEL.worthReviewingOperator] ||
    actionable;
  return {
    id: rec.id,
    alertId: rec.id,
    title: f[MAP_ALERT.title] || "",
    summary: f[MAP_ALERT.summary] || "",
    sourceUrl: f[MAP_ALERT.sourceUrl] || "",
    category: f[MAP_ALERT.category] || "",
    eventType: f[MAP_INTEL.eventType] || "",
    hotelProject: f[MAP_INTEL.hotelProject] || "",
    ownerDeveloper: f[MAP_INTEL.ownerDeveloper] || "",
    brandInvolved: f[MAP_INTEL.brandInvolved] || "",
    operatorInvolved: f[MAP_INTEL.operatorInvolved] || "",
    actionable,
    worthReviewing,
    intelligence: {
      eventType: f[MAP_INTEL.eventType] || null,
      signalType:
        f[MAP_INTEL.signalTypeOwner] ||
        f[MAP_INTEL.signalTypeBrand] ||
        f[MAP_INTEL.signalTypeOperator] ||
        null,
      actionable,
      worthReviewing,
      whatChanged: f[MAP_INTEL.whatChanged] || "",
      entities: {
        ownerDeveloper: f[MAP_INTEL.ownerDeveloper] || null,
        hotelProject: f[MAP_INTEL.hotelProject] || null,
        brandInvolved: f[MAP_INTEL.brandInvolved] || null,
        operatorInvolved: f[MAP_INTEL.operatorInvolved] || null,
      },
    },
  };
}

export async function loadMarketAlertById(alertId) {
  const Airtable = (await import("airtable")).default;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  if (!apiKey || !baseId || !alertId) return null;
  try {
    const base = new Airtable({ apiKey }).base(baseId);
    const rec = await base(table).find(alertId);
    return alertFromAirtableRecord(rec);
  } catch {
    return null;
  }
}
