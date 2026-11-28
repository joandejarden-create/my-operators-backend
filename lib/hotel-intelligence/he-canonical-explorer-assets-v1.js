/**
 * Canonical Hotel Explorer asset contract shared by:
 * - Opportunity Radar (`deal-capture-radar-with-ranked-list.html`)
 * - Founder preview (`hotel-explorer-preview.html`)
 * - Future HE hosts
 *
 * Radar must load the FULL set — not only hotel-explorer.js + HDP embed.
 * Missing HotelIntelligenceWorkspace (and related CSS/JS) yields a 12-icon
 * shell with empty/reduced HI tabs.
 */

export const HE_CANONICAL_EXPLORER_ASSETS_V1 = "he-canonical-explorer-assets-v1";

/** CSS required for full 12-tab visual/animation parity (relative /css/…). */
export const HE_CANONICAL_CSS = Object.freeze([
  "/css/hotel-detail-panel.css",
  "/css/hotel-ownership-intelligence.css",
  "/css/hotel-intelligence-workspace.css",
  "/css/hotel-explorer.css",
  "/css/dealality-wave-loader.css",
  "/css/hotel-contact-intelligence.css",
  "/css/admin-ai-demand-table.css",
  "/css/brand-alignment-snapshot.css",
  "/css/dealality-report-print-chrome.css",
  "/css/hotel-intelligence-dossier.css",
  "/css/hotel-intelligence-research-center.css",
]);

/**
 * JS load order for canonical Explorer (relative /js/… except noted).
 * hotel-explorer.js MUST follow hotel-detail-panel.js so PRIMARY open delegates.
 */
export const HE_CANONICAL_JS = Object.freeze([
  "/js/dealality-chain-scale-theme.js",
  "/js/dealality-hotel-property-merge.js",
  "/js/he-measurement-prefs-v1.js",
  "/js/he-owner-portfolio-operator-view-v1.js",
  "/js/dealality-wave-loader.js",
  "/js/he-loading-ux-v1.js",
  "/js/he-async-section-v1.js",
  "/js/he-ownership-control-network-v1.js",
  "/js/he-guest-sentiment-feature-flag-v1.js",
  "/js/he-customer-source-date-v1.js",
  "/js/hilton-amenity-icons.js",
  "/js/hotel-detail-panel.js",
  "/js/hotel-ownership-intelligence.js",
  "/js/hotel-contact-intelligence.js",
  "/js/he-customer-supports-label-v1.js",
  "/js/hotel-intelligence-workspace.js",
  "/js/dealality-sortable-table.js",
  "/js/hotel-explorer-rooms-meeting-space.js",
  "/js/dealality-book-shell.js",
  "/js/dealality-report-print-chrome.js",
  "/js/hotel-intelligence-dossier.js",
  "/js/hotel-intelligence-research-center.js",
  "/js/hotel-intelligence-golden-demo-route.js",
  "/js/hotel-explorer.js",
]);

/**
 * All registered Explorer tab ids (including preserved-but-disabled Guest Sentiment).
 * Customer nav uses HE_ACTIVE_TAB_IDS when Guest Sentiment is founder-disabled.
 */
export const HE_ALL_TAB_IDS = Object.freeze([
  "hotel",
  "rooms",
  "ownership",
  "contacts",
  "organization",
  "relationships",
  "submarket",
  "area",
  "demand",
  "access",
  "sentiment",
  "sources",
]);

/** Preserved feature tabs that may be hidden by founder config (code retained). */
export const HE_PRESERVED_DISABLED_TAB_IDS = Object.freeze(["sentiment"]);

/**
 * Active customer-facing tabs when HE_GUEST_SENTIMENT_ENABLED=false (default).
 * Length 11 — Guest Sentiment remains registered in HE_ALL_TAB_IDS.
 */
export const HE_ACTIVE_TAB_IDS = Object.freeze(
  HE_ALL_TAB_IDS.filter((id) => !HE_PRESERVED_DISABLED_TAB_IDS.includes(id)),
);

/**
 * @deprecated Prefer HE_ACTIVE_TAB_IDS for customer nav / HE_ALL_TAB_IDS for registry.
 * Kept as alias of HE_ALL_TAB_IDS so asset loaders still see the preserved 12-tab contract.
 */
export const HE_CANONICAL_TAB_IDS = HE_ALL_TAB_IDS;

/** Tabs rendered via HotelDetailPanel.embed (location intelligence). */
export const HE_RADAR_EMBED_TAB_IDS = Object.freeze([
  "hotel",
  "submarket",
  "area",
  "demand",
  "access",
]);

/** Tabs that require HotelIntelligenceWorkspace (or dedicated modules). */
export const HE_HI_WORKSPACE_TAB_IDS = Object.freeze([
  "rooms",
  "ownership",
  "contacts",
  "organization",
  "relationships",
  "sentiment",
  "sources",
]);

/** HI workspace tabs shown when Guest Sentiment is founder-disabled. */
export const HE_HI_WORKSPACE_ACTIVE_TAB_IDS = Object.freeze(
  HE_HI_WORKSPACE_TAB_IDS.filter((id) => !HE_PRESERVED_DISABLED_TAB_IDS.includes(id)),
);

export function assertHtmlLoadsCanonicalExplorerAssets(html, { label = "host" } = {}) {
  const missingCss = HE_CANONICAL_CSS.filter((href) => {
    const base = href.replace("/css/", "").replace(/\.css$/, "");
    return !new RegExp(`${base}\\.css`, "i").test(html);
  });
  const missingJs = HE_CANONICAL_JS.filter((src) => {
    const base = src.replace("/js/", "").replace(/\.js$/, "");
    return !new RegExp(`${base}\\.js`, "i").test(html);
  });
  const hdpBeforeHe =
    /hotel-detail-panel\.js[\s\S]*hotel-explorer\.js/i.test(html) &&
    /hotel-intelligence-workspace\.js[\s\S]*hotel-explorer\.js/i.test(html);

  return {
    label,
    ok: missingCss.length === 0 && missingJs.length === 0 && hdpBeforeHe,
    missingCss,
    missingJs,
    hdpBeforeHe,
    workspaceBeforeExplorer: /hotel-intelligence-workspace\.js[\s\S]*hotel-explorer\.js/i.test(
      html,
    ),
  };
}
