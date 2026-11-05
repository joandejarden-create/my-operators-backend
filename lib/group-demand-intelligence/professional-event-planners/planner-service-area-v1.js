/**
 * Planner service-area logic — office distance ≠ destination production evidence.
 * Fail closed: never claim destination activity without evidence.
 */
import { text } from "./professional-event-planner-contract-v1.js";

export const PLANNER_SERVICE_AREA_V1 = "gdi-planner-service-area-v1";

export const PLANNER_GEO_SCOPE = Object.freeze({
  OFFICE_IN_MARKET: "OFFICE_IN_MARKET",
  PRODUCES_IN_DESTINATION: "PRODUCES_IN_DESTINATION",
  BOTH: "BOTH",
  OFFICE_ONLY_UNVERIFIED_DESTINATION: "OFFICE_ONLY_UNVERIFIED_DESTINATION",
  OUT_OF_MARKET: "OUT_OF_MARKET",
  UNKNOWN: "UNKNOWN",
});

export const PLANNER_GEO_SCOPE_LABEL = Object.freeze({
  [PLANNER_GEO_SCOPE.OFFICE_IN_MARKET]: "Office based in hotel market",
  [PLANNER_GEO_SCOPE.PRODUCES_IN_DESTINATION]:
    "Verified event production in hotel destination",
  [PLANNER_GEO_SCOPE.BOTH]:
    "Office in market and verified destination production",
  [PLANNER_GEO_SCOPE.OFFICE_ONLY_UNVERIFIED_DESTINATION]:
    "Office nearby — destination production not yet verified",
  [PLANNER_GEO_SCOPE.OUT_OF_MARKET]: "Outside hotel market / destination",
  [PLANNER_GEO_SCOPE.UNKNOWN]: "Service area not yet classified",
});

/**
 * Configurable research catchments — provisional bands for office location only.
 * Destination production is evidence-based, not radius-based.
 */
export const DEFAULT_PLANNER_CATCHMENT_POLICY_MI = Object.freeze({
  coreOfficeMaxMi: 15,
  secondaryOfficeMaxMi: 40,
  extendedOfficeMaxMi: 120,
  note: "Office proximity is secondary to verified destination event history.",
});

export function classifyPlannerGeoScope({
  officeInMarket = null,
  destinationProductionVerified = false,
  outOfMarket = false,
} = {}) {
  if (outOfMarket && !destinationProductionVerified) {
    return PLANNER_GEO_SCOPE.OUT_OF_MARKET;
  }
  if (officeInMarket === true && destinationProductionVerified) {
    return PLANNER_GEO_SCOPE.BOTH;
  }
  if (destinationProductionVerified) {
    return PLANNER_GEO_SCOPE.PRODUCES_IN_DESTINATION;
  }
  if (officeInMarket === true) {
    return PLANNER_GEO_SCOPE.OFFICE_ONLY_UNVERIFIED_DESTINATION;
  }
  if (officeInMarket === false && !destinationProductionVerified) {
    return PLANNER_GEO_SCOPE.OUT_OF_MARKET;
  }
  return PLANNER_GEO_SCOPE.UNKNOWN;
}

export function buildPlannerServiceAreaEvidence({
  planner = {},
  hotel = {},
  destinationEvidence = [],
} = {}) {
  const officeLocation =
    text(planner.officeLocation || planner.city || planner.officeCity) || null;
  const operatingMarkets = Array.isArray(planner.operatingMarkets)
    ? planner.operatingMarkets.map(text).filter(Boolean)
    : text(planner.operatingMarkets)
      ? [text(planner.operatingMarkets)]
      : [];
  const hotelMarket = text(
    hotel.market || hotel.city || hotel.destination || hotel.name,
  );
  const evidence = (destinationEvidence || [])
    .map((e) => ({
      label: text(e.label || e.eventName || e.venue),
      destination: text(e.destination || e.city),
      sourceUrl: text(e.sourceUrl || e.url) || null,
      verified: e.verified !== false,
    }))
    .filter((e) => e.label || e.destination);

  const destinationProductionVerified = evidence.some(
    (e) => e.verified && (e.label || e.destination),
  );

  const fold = (s) =>
    String(s || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");

  let officeInMarket = null;
  if (officeLocation && hotelMarket) {
    const officeLc = fold(officeLocation);
    const marketLc = fold(hotelMarket);
    const marketToken = marketLc.split(/[,\s]/)[0] || "";
    const officeToken = officeLc.split(/[,\s]/)[0] || "";
    const cdmxAlias =
      /ciudad de mexico|mexico city|cdmx|polanco|chapultepec/.test(officeLc) &&
      /ciudad de mexico|mexico city|cdmx|mexico/.test(marketLc);
    officeInMarket =
      cdmxAlias ||
      (marketToken && officeLc.includes(marketToken)) ||
      (officeToken && marketLc.includes(officeToken)) ||
      operatingMarkets.some((m) => {
        const mf = fold(m);
        return (
          (marketToken && mf.includes(marketToken)) ||
          /ciudad de mexico|mexico city|cdmx/.test(mf)
        );
      });
  }

  const geoScope = classifyPlannerGeoScope({
    officeInMarket,
    destinationProductionVerified,
    outOfMarket: officeInMarket === false && !destinationProductionVerified,
  });

  return {
    version: PLANNER_SERVICE_AREA_V1,
    officeLocation,
    operatingMarkets,
    hotelMarket: hotelMarket || null,
    destinationEvidence: evidence,
    destinationProductionVerified,
    officeInMarket,
    geoScope,
    geoScopeLabel: PLANNER_GEO_SCOPE_LABEL[geoScope] || geoScope,
    note: destinationProductionVerified
      ? null
      : "Do not claim the planner regularly operates in this destination without verified event evidence.",
  };
}
