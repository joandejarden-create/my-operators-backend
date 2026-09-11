/**
 * ADP current_published share + local/external analytical parity doctrine.
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

export const CURRENT_PUBLISHED_SHARE_ALWAYS_RESOLVES_LATEST_GOVERNED_PUBLISHED_EDITION =
  "CURRENT_PUBLISHED_SHARE_ALWAYS_RESOLVES_LATEST_GOVERNED_PUBLISHED_EDITION";

export const LOCAL_OWNER_SHARE_PRINT_MUST_RESOLVE_SAME_PUBLISHED_ANALYTICAL_STATE =
  "LOCAL_OWNER_SHARE_PRINT_MUST_RESOLVE_SAME_PUBLISHED_ANALYTICAL_STATE";

export const SAME_PROPERTY_SAME_PERIOD_SAME_PUBLISHED_SNAPSHOT_SAME_RENDERED_RESULT =
  "SAME_PROPERTY_SAME_PERIOD_SAME_PUBLISHED_SNAPSHOT_SAME_RENDERED_RESULT";

export const ADP_CURRENT_PUBLISHED_SHARE_RESOLVER_CANONICAL =
  "ADP_CURRENT_PUBLISHED_SHARE_RESOLVER_CANONICAL";

export const ADP_EXISTING_SHARE_URL_AUTO_UPDATES_CURRENT_PUBLISHED =
  "ADP_EXISTING_SHARE_URL_AUTO_UPDATES_CURRENT_PUBLISHED";

export const ADP_LOCAL_EXTERNAL_ANALYTICAL_PARITY = "ADP_LOCAL_EXTERNAL_ANALYTICAL_PARITY";

export const ADP_LOCAL_EXTERNAL_RENDERER_PARITY = "ADP_LOCAL_EXTERNAL_RENDERER_PARITY";

export const ADP_BPP_LOCAL_EXTERNAL_PARITY = "ADP_BPP_LOCAL_EXTERNAL_PARITY";

export const ADP_EXTERNAL_SHARE_ASSET_CACHE_INVALIDATION =
  "ADP_EXTERNAL_SHARE_ASSET_CACHE_INVALIDATION";

export const ADP_EXISTING_SHARE_URL_LATEST_EDITION_PASS =
  "ADP_EXISTING_SHARE_URL_LATEST_EDITION_PASS";

export const PRODUCTION_VISUAL_OUTPUT_MATCHES_VERIFIED_LOCAL =
  "PRODUCTION_VISUAL_OUTPUT_MATCHES_VERIFIED_LOCAL";

export const ADP_ANALYTICAL_FINGERPRINT_LOCAL_EXTERNAL_PARITY =
  "ADP_ANALYTICAL_FINGERPRINT_LOCAL_EXTERNAL_PARITY";

/** Canonical analytical SoT for published Existing Hotel ADP customer surfaces. */
export const ADP_CURRENT_PUBLISHED_SOT = Object.freeze({
  storageRuntime: "data/ai-demand-positioning/published/<propertyId>/",
  storageSeedFallback: "fixtures/ai-demand-positioning/published/<propertyId>/",
  manifest: "manifest.json (latestPeriodId, reportFile, evidenceFile)",
  report: "report-<periodId>.json → payload",
  evidence: "evidence-<periodId>.json",
  bppPackDeployable: "config/client-share/bpp-customer-published-v1.json",
  bppPackReportsFallback: "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_CUSTOMER_PUBLISHED_V1.json",
  shareRegistry: "config/client-share/adp-share-registry/active-tokens.json",
  resolvers: Object.freeze({
    loadPublishedReport: "lib/ai-demand-positioning/published-snapshot.js#loadPublishedReport",
    getPublishedOwnerReport: "lib/ai-demand-positioning/published-read-service.js#getPublishedOwnerReport",
    enrichPayloadOptionalMetrics:
      "lib/ai-demand-positioning/published-read-service.js#enrichPayloadOptionalMetrics",
    resolveBrandPortfolioPosition: "api/ai-demand-positioning.js#resolveBrandPortfolioPosition",
    verifyShareCapability:
      "lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js#verifyShareCapability",
    listPublishedPropertyIds: "lib/ai-demand-positioning/published-snapshot.js#listPublishedPropertyIds",
    resolveGovernedUniverse:
      "lib/ai-demand-positioning/client-readiness/resolve-governed-adp-property-universe-v1.js#resolveGovernedAdpPropertyUniverseV1",
  }),
  surfaces: Object.freeze([
    "owner dashboard /owner-ai-demand.html",
    "signed share /owner-ai-demand-share.html?share=",
    "print/PDF (same payload + print CSS)",
  ]),
  reportScopeAllowed: "current_published",
  note:
    "Share tokens bind propertyId + surfaces; analytical content always comes from current Live published snapshot for that property — not a frozen edition id on the token.",
});
