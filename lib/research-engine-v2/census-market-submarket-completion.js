/**
 * Market + Submarket completion for Hotel Property Census (commercial fields).
 */

import { isDirtyStateRegionValue } from "./census-city-to-state-map.js";
import { resolveCommercialMarket } from "./census-commercial-market-map.js";
import { resolveCommercialSubmarket } from "./census-submarket-map.js";
import { isIncorrectCanonicalPropertyName } from "./universal-hotel-record-inspector.js";

export const MARKET_SUBMARKET_COMPLETION_VERSION =
  "census-market-submarket-completion-v1";

function isBlank(v) {
  return v == null || !String(v).trim();
}

/**
 * Build High Market/Submarket patch for one Census record.
 */
export function completeMarketSubmarketForRecord(record = {}) {
  const f = record.fields || {};
  const patch = {};
  const blockers = [];
  const backlog = [];

  const city = String(f.City || "").trim();
  const country = String(f.Country || "").trim();
  const state = String(f["State / Region"] || "").trim();

  if (!city || !country) {
    return {
      ok: false,
      reason: "missing_city_or_country",
      patch: {},
      blockers: [{ field: "Market", reason: "missing_city_or_country" }],
      backlog: [],
    };
  }

  if (isDirtyStateRegionValue(state)) {
    blockers.push({
      field: "State / Region",
      reason: "dirty_state_blocks_commercial_confidence",
      current: state,
    });
  }

  const nameCheck = isIncorrectCanonicalPropertyName(f);
  if (nameCheck.incorrect) {
    blockers.push({
      field: "Canonical Property Name",
      reason: nameCheck.reason,
    });
  }

  if (isBlank(f.Market)) {
    const m = resolveCommercialMarket({ city, country });
    if (m.ok && m.market) {
      patch.Market = m.market;
    } else {
      blockers.push({ field: "Market", reason: m.reason || "market_mapping_backlog" });
      backlog.push({
        type: "market_mapping_backlog",
        record_id: record.id || null,
        city,
        country,
        reason: m.reason,
      });
    }
  }

  const market = patch.Market || f.Market;
  if (isBlank(f.Submarket) && market) {
    const sub = resolveCommercialSubmarket({
      market,
      city,
      address: f.Address,
      propertyName: f["Canonical Property Name"] || f["Property Name"],
    });
    if (sub.ok && sub.submarket) {
      patch.Submarket = sub.submarket;
    } else {
      backlog.push({
        type: "submarket_mapping_backlog",
        record_id: record.id || null,
        city,
        country,
        market,
        reason: sub.reason,
      });
    }
  } else if (isBlank(f.Submarket) && !market) {
    backlog.push({
      type: "submarket_mapping_backlog",
      record_id: record.id || null,
      city,
      country,
      reason: "market_missing",
    });
  }

  return {
    ok: Object.keys(patch).length > 0,
    version: MARKET_SUBMARKET_COMPLETION_VERSION,
    patch,
    blockers,
    backlog,
  };
}
