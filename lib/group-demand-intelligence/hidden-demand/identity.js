/**
 * Stable hidden-demand / generator / hotel-match identity.
 */

import { createHash } from "node:crypto";
import {
  HIDDEN_DEMAND_ID_PREFIX,
  DEMAND_GENERATOR_ID_PREFIX,
  HOTEL_MATCH_ID_PREFIX,
} from "./constants.js";

function clean(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function computeDemandGeneratorId({ name, marketKey, year } = {}) {
  const seed = `${clean(marketKey)}|${clean(name)}|${clean(year)}`;
  return `${DEMAND_GENERATOR_ID_PREFIX}${createHash("sha256").update(seed).digest("hex").slice(0, 14)}`;
}

export function computeHiddenDemandId({
  organizationName,
  projectName,
  demandGeneratorId,
  family,
  timingKey,
} = {}) {
  const seed = [
    clean(demandGeneratorId),
    clean(organizationName),
    clean(projectName),
    clean(family),
    clean(timingKey),
  ].join("|");
  return `${HIDDEN_DEMAND_ID_PREFIX}${createHash("sha256").update(seed).digest("hex").slice(0, 14)}`;
}

export function computeHotelMatchId({ hotelId, hiddenDemandId } = {}) {
  const seed = `${clean(hotelId)}|${clean(hiddenDemandId)}`;
  return `${HOTEL_MATCH_ID_PREFIX}${createHash("sha256").update(seed).digest("hex").slice(0, 14)}`;
}

export function computeHotelOpportunityId({ hotelId, hiddenDemandId } = {}) {
  const seed = `gdi_hdopp|${clean(hotelId)}|${clean(hiddenDemandId)}`;
  return `gdi_opp_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}
