/**
 * Brand & Portfolio customer publication meta — single source for delivery/cache tokens.
 * Do not require founder/customer to append ?v= manually.
 */

export const BPP_CUSTOMER_PUBLICATION_VERSION = "bpp-customer-v1.1-20260902-p2";
export const BPP_ASSET_CACHE_TOKEN = "adp-v79-20260903-visual-restore";
export const BPP_KPI_CONTRACT_VERSION = "ADP_BRAND_PORTFOLIO_KPI_CONTRACT_V1_1";
export const BPP_METRICS_VERSION = "ADP_BRAND_PORTFOLIO_METRICS_V1_1";
export const BPP_CUSTOMER_PUBLISHED_PACK =
  "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_CUSTOMER_PUBLISHED_V1.json";

/** Deployable SoT (Railway excludes reports/ by default). Prefer this path at read time. */
export const BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE =
  "config/client-share/bpp-customer-published-v1.json";

export const BPP_CUSTOMER_PUBLISHED_PACK_CANDIDATES = Object.freeze([
  BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE,
  BPP_CUSTOMER_PUBLISHED_PACK,
]);

/** Optional share-route / alias → canonical propertyId */
export const BPP_PROPERTY_ID_ALIASES = Object.freeze({
  waterstone: "adp_waterstone_boca_raton",
  "waterstone-boca": "adp_waterstone_boca_raton",
  "waterstone-boca-raton": "adp_waterstone_boca_raton",
  phillips: "adp_hotel_phillips_kansas_city",
  "hotel-phillips": "adp_hotel_phillips_kansas_city",
  renaissance: "adp_renaissance_times_square",
  "renaissance-ts": "adp_renaissance_times_square",
  cambridge: "adp_cambridge_beaches_bermuda",
  noho: "adp_now_now_noho",
  "now-now-noho": "adp_now_now_noho",
});

export function resolveCanonicalBppPropertyId(propertyId) {
  const raw = String(propertyId || "").trim();
  if (!raw) return null;
  if (BPP_PROPERTY_ID_ALIASES[raw]) return BPP_PROPERTY_ID_ALIASES[raw];
  const lower = raw.toLowerCase();
  if (BPP_PROPERTY_ID_ALIASES[lower]) return BPP_PROPERTY_ID_ALIASES[lower];
  return raw;
}

export function buildBppReportCacheKey({ propertyId, publicationVersion, payloadHash }) {
  return [
    "adp-bpp-report",
    resolveCanonicalBppPropertyId(propertyId) || propertyId || "unknown",
    publicationVersion || "none",
    payloadHash || "none",
  ].join("::");
}
