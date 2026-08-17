/**
 * Marriott Webhound pattern learner — converts discovery findings into adapter catalog.
 *
 * CRITICAL: Webhound output is NEVER Census source of truth.
 * Findings must be revalidated against underlying official URLs before any write.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const WEBHOUND_PATTERN_LEARNER_VERSION =
  "marriott-webhound-pattern-learner-v1";

export const PATTERN_SOURCE_CLASS = Object.freeze({
  OFFICIAL_MARRIOTT: "official_marriott",
  OFFICIAL_BRAND: "official_brand",
  OFFICIAL_HOTEL_MICROSITE: "official_hotel_microsite",
  OFFICIAL_NEWSROOM: "official_newsroom",
  OFFICIAL_FACTSHEET: "official_factsheet",
  SECONDARY: "secondary",
  REJECTED: "rejected",
});

/** Seed patterns already known from Dealality code / prior learning (not Webhound SoT). */
export const SEED_MARRIOTT_PATTERNS = Object.freeze([
  {
    pattern_id: "marriott_country_hotel_sitemap",
    source_type: "official_parent_directory",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_MARRIOTT,
    url_template:
      "https://www.marriott.com/en-us/hotel-sitemap/{country-slug}-hotel-sitemap",
    marsha_keying: "URL path /hotels/{MARSHA}-",
    fields: ["Official Property URL", "Property Name", "Brand"],
    bot_risk: "medium",
    autopilot_safe: true,
    webhound_as_sot: false,
    adapter_module: "census-autopilot-marriott-discovery-adapter.js",
    notes: "Identity/URL discovery only — not Level 2 address/phone/rooms.",
  },
  {
    pattern_id: "marriott_hqv_graphql_coords",
    source_type: "official_catalog_api",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_MARRIOTT,
    url_template:
      "https://www.marriott.com/mi/query/phoenixShopHQVPropertyInfoCall",
    marsha_keying: "GraphQL variables.propertyId = MARSHA",
    fields: ["Latitude", "Longitude", "Canonical Property Name"],
    bot_risk: "high",
    autopilot_safe: true,
    webhound_as_sot: false,
    adapter_module: "marriott-hqv-coordinate-client.js",
    notes:
      "Requires optional graphql-operation-signature; never invent coords on block.",
  },
  {
    pattern_id: "marriott_overview_html_jsonld",
    source_type: "official_property_page",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_MARRIOTT,
    url_template:
      "https://www.marriott.com/en-us/hotels/{marsha}-{slug}/overview/",
    marsha_keying: "URL MARSHA segment",
    fields: ["Address", "Phone", "Rooms / Keys"],
    bot_risk: "very_high",
    autopilot_safe: false,
    webhound_as_sot: false,
    adapter_module: "marriott-official-metadata-adapter.js",
    notes: "Often Akamai-blocked; classify blocked, do not fatal-stop Autopilot.",
  },
  {
    pattern_id: "marriott_linked_hotel_microsite_jsonld",
    source_type: "official_linked_hotel_website",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_HOTEL_MICROSITE,
    url_template: "{official_hotel_domain}/*",
    marsha_keying: "via Marriott page link when available",
    fields: ["Address", "Phone", "Rooms / Keys"],
    bot_risk: "low",
    autopilot_safe: true,
    webhound_as_sot: false,
    adapter_module: "marriott-linked-hotel-site-adapter.js",
    notes: "Only when Marriott/official brand links the hotel site; revalidate JSON-LD.",
  },
  {
    pattern_id: "marriott_official_factsheet_pdf",
    source_type: "official_factsheet",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_FACTSHEET,
    url_template: "{factsheet_pdf_url}",
    marsha_keying: "property name + city match",
    fields: ["Rooms / Keys", "Address", "Phone"],
    bot_risk: "low",
    autopilot_safe: true,
    webhound_as_sot: false,
    adapter_module: "marriott-factsheet-adapter.js",
    notes: "Exact property + exact room count required; no meeting-room confusion.",
  },
  {
    pattern_id: "marriott_newsroom_press_rooms_only",
    source_type: "official_press_release",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_NEWSROOM,
    url_template: "https://news.marriott.com/*",
    marsha_keying: "exact property name match",
    fields: ["Rooms / Keys"],
    bot_risk: "low",
    autopilot_safe: true,
    webhound_as_sot: false,
    adapter_module: "marriott-rooms-source-adapter.js",
    notes: "Rooms only when exact property and count are clear.",
  },
]);

export const REJECTED_PATTERNS = Object.freeze([
  {
    pattern_id: "ota_booking_expedia",
    source_class: PATTERN_SOURCE_CLASS.REJECTED,
    reason: "OTA not allowed as Census SoT",
  },
  {
    pattern_id: "google_maps_address_phone",
    source_class: PATTERN_SOURCE_CLASS.REJECTED,
    reason: "Google Maps forbidden for address/phone",
  },
  {
    pattern_id: "mapbox_as_address_phone",
    source_class: PATTERN_SOURCE_CLASS.REJECTED,
    reason: "Mapbox only for coordinates after High Address",
  },
  {
    pattern_id: "webhound_direct_census_write",
    source_class: PATTERN_SOURCE_CLASS.REJECTED,
    reason: "Webhound never final source of truth",
  },
]);

/**
 * Assert a finding cannot be used as Census SoT.
 * @param {object} finding
 */
export function assertWebhoundNotCensusSot(finding = {}) {
  if (finding.webhound_as_sot === true || finding.use_as_census_sot === true) {
    return {
      ok: false,
      reason: "webhound_as_census_sot_forbidden",
      safe_for_autopilot_extraction: false,
    };
  }
  if (finding.source === "webhound" && finding.direct_write === true) {
    return {
      ok: false,
      reason: "webhound_direct_write_forbidden",
      safe_for_autopilot_extraction: false,
    };
  }
  return {
    ok: true,
    webhound_role: "pattern_discovery_only",
    requires_underlying_source_revalidation: true,
    safe_for_autopilot_extraction: Boolean(finding.autopilot_safe),
  };
}

/**
 * Normalize a Webhound-discovered pattern into catalog row.
 * Never marks webhound_as_sot true.
 */
export function normalizeDiscoveredPattern(raw = {}) {
  const url = String(raw.discovered_url || raw.url || "").trim();
  const domain = (() => {
    try {
      return new URL(url).hostname;
    } catch {
      return String(raw.source_domain || "").trim() || null;
    }
  })();
  const rejected =
    /booking\.com|expedia\.|hotels\.com|tripadvisor\.|google\.(com|co)|maps\.google|kayak\.|trivago\./i.test(
      url || domain || ""
    );

  const finding = {
    pattern_id: raw.pattern_id || `discovered_${domain || "unknown"}`,
    discovered_url: url || null,
    source_domain: domain,
    source_type: raw.source_type || "unknown",
    source_class: rejected
      ? PATTERN_SOURCE_CLASS.REJECTED
      : raw.source_class || PATTERN_SOURCE_CLASS.SECONDARY,
    field_candidates: raw.field_candidates || raw.fields || [],
    confidence: raw.confidence || "Medium",
    recommended_adapter_change: raw.recommended_adapter_change || null,
    autopilot_safe: rejected ? false : Boolean(raw.autopilot_safe),
    webhound_as_sot: false,
    bot_risk: raw.bot_risk || "unknown",
    marsha_keying: raw.marsha_keying || null,
    notes: raw.notes || null,
    rejected,
    reject_reason: rejected ? "ota_or_google_forbidden" : null,
  };
  finding.sot_guard = assertWebhoundNotCensusSot(finding);
  return finding;
}

/**
 * Merge seed + Webhound discoveries into learning catalog.
 * @param {object[]} discovered
 * @param {object} [opts]
 */
/** Patterns confirmed by Webhound research — still never Census SoT. */
export const WEBHOUND_CONFIRMED_PATTERNS = Object.freeze([
  {
    pattern_id: "marriott_dam_factsheet_pdf",
    source_type: "official_factsheet",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_FACTSHEET,
    url_template:
      "https://www.marriott.com/content/dam/marriott-digital/{brandPrefix}/{region}/hws/{marshaFirst}/{marsha}/en_us/document/assets/{prefix}-{marsha}-fact-sheet-{id}.pdf",
    marsha_keying: "MARSHA in DAM path",
    fields: ["Rooms / Keys", "Address", "Phone"],
    bot_risk: "low",
    autopilot_safe: true,
    webhound_as_sot: false,
    adapter_module: "marriott-dam-factsheet-discovery.js",
    notes:
      "Webhound-confirmed: SJULU/GYECY DAM PDFs extract rooms+address/phone after revalidation. Seed examples may be outside current Census; expand index for in-Census MARSHAs.",
  },
  {
    pattern_id: "marriott_hotel_sitemap_next_data",
    source_type: "official_parent_directory",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_MARRIOTT,
    url_template:
      "https://www.marriott.com/en-us/hotel-sitemap/{country-slug}-hotel-sitemap",
    marsha_keying: "__NEXT_DATA__ property list + URL /hotels/{MARSHA}-",
    fields: ["Official Property URL", "Property Name", "Brand"],
    bot_risk: "low",
    autopilot_safe: true,
    webhound_as_sot: false,
    adapter_module: "census-autopilot-marriott-discovery-adapter.js",
    notes: "Working for enumeration; not Level 2 address/phone/rooms.",
  },
  {
    pattern_id: "marriott_newsroom_press_rooms",
    source_type: "official_press_release",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_NEWSROOM,
    url_template: "https://news.marriott.com/news/{YYYY}/{MM}/{DD}/{slug}",
    marsha_keying: "property name in body",
    fields: ["Rooms / Keys"],
    bot_risk: "low",
    autopilot_safe: true,
    webhound_as_sot: false,
    adapter_module: "marriott-rooms-source-adapter.js",
    notes: "Sparse but exact guestroom counts; low Akamai risk vs HWS overview.",
  },
  {
    pattern_id: "marriott_modules_microsite",
    source_type: "official_linked_hotel_website",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_HOTEL_MICROSITE,
    url_template: "https://modules.marriott.com/{property-collection-slug}/",
    marsha_keying: "MARSHA in RFP / meeting links",
    fields: ["Address", "Phone", "Rooms / Keys"],
    bot_risk: "low",
    autopilot_safe: true,
    webhound_as_sot: false,
    adapter_module: "marriott-linked-hotel-site-adapter.js",
    notes:
      "Property-cluster microsites; rooms may be aggregated — require property-specific evidence before write.",
  },
  {
    pattern_id: "marriott_overview_html_akamai",
    source_type: "official_property_page",
    source_class: PATTERN_SOURCE_CLASS.OFFICIAL_MARRIOTT,
    url_template:
      "https://www.marriott.com/en-us/hotels/{marsha}-{slug}/overview/",
    marsha_keying: "URL MARSHA segment",
    fields: ["Address", "Phone"],
    bot_risk: "very_high",
    autopilot_safe: false,
    webhound_as_sot: false,
    adapter_module: "marriott-official-metadata-adapter.js",
    notes:
      "Webhound: no __NEXT_DATA__/JSON-LD on overview; address/phone in GETTING HERE HTML when not blocked. Classify blocked, not fatal.",
  },
]);

export function buildMarriottPatternLearningCatalog(discovered = [], opts = {}) {
  const normalized = (discovered || []).map(normalizeDiscoveredPattern);
  const accepted = [
    ...WEBHOUND_CONFIRMED_PATTERNS,
    ...normalized.filter((p) => !p.rejected && p.sot_guard.ok),
  ];
  const rejected = [
    ...REJECTED_PATTERNS,
    ...normalized.filter((p) => p.rejected || !p.sot_guard.ok),
  ];
  const repeatable = accepted.filter((p) => p.autopilot_safe);

  const catalog = {
    version: WEBHOUND_PATTERN_LEARNER_VERSION,
    generated_at: new Date().toISOString(),
    webhound_session_id: opts.webhoundSessionId || null,
    webhound_as_census_sot: false,
    seed_patterns: SEED_MARRIOTT_PATTERNS,
    webhound_confirmed_patterns: WEBHOUND_CONFIRMED_PATTERNS,
    discovered_patterns: normalized,
    accepted_for_adapter_learning: accepted,
    rejected_patterns: rejected,
    repeatable_adapter_candidates: repeatable,
    adapter_modules: [
      "marriott-source-pattern-discovery.js",
      "marriott-webhound-pattern-learner.js",
      "marriott-official-metadata-adapter.js",
      "marriott-factsheet-adapter.js",
      "marriott-linked-hotel-site-adapter.js",
      "marriott-rooms-source-adapter.js",
    ],
  };

  if (opts.writePath) {
    fs.mkdirSync(path.dirname(opts.writePath), { recursive: true });
    fs.writeFileSync(opts.writePath, `${JSON.stringify(catalog, null, 2)}\n`, "utf8");
  }
  return catalog;
}

/**
 * Parse loose Webhound markdown/JSON into discovered pattern stubs.
 * @param {string} text
 */
export function extractPatternsFromWebhoundText(text = "") {
  const raw = String(text || "");
  const urls = [...raw.matchAll(/https?:\/\/[^\s\)\"\'<>]+/gi)].map((m) =>
    m[0].replace(/[.,;]+$/, "")
  );
  const unique = [...new Set(urls)].slice(0, 80);
  return unique.map((url) => {
    let source_class = PATTERN_SOURCE_CLASS.SECONDARY;
    let source_type = "discovered_url";
    let autopilot_safe = false;
    if (/news\.marriott\.com/i.test(url)) {
      source_class = PATTERN_SOURCE_CLASS.OFFICIAL_NEWSROOM;
      source_type = "official_press_release";
      autopilot_safe = true;
    } else if (/modules\.marriott\.com/i.test(url)) {
      source_class = PATTERN_SOURCE_CLASS.OFFICIAL_HOTEL_MICROSITE;
      source_type = "official_linked_hotel_website";
      autopilot_safe = true;
    } else if (/marriott\.com.*fact-sheet|\.pdf($|\?)/i.test(url) && /marriott/i.test(url)) {
      source_class = PATTERN_SOURCE_CLASS.OFFICIAL_FACTSHEET;
      source_type = "official_factsheet";
      autopilot_safe = true;
    } else if (/marriott\.com/i.test(url)) {
      source_class = PATTERN_SOURCE_CLASS.OFFICIAL_MARRIOTT;
      source_type = /hotel-sitemap/i.test(url)
        ? "official_parent_directory"
        : /\/hotels\//i.test(url)
          ? "official_property_page"
          : "official_marriott";
      autopilot_safe = /hotel-sitemap|content\/dam/i.test(url);
    } else if (/\.pdf($|\?)/i.test(url)) {
      source_class = PATTERN_SOURCE_CLASS.OFFICIAL_FACTSHEET;
      source_type = "official_factsheet";
      autopilot_safe = /marriott|sheraton|westin|ritz|st-regis/i.test(url);
    }
    return normalizeDiscoveredPattern({
      discovered_url: url,
      source_class,
      source_type,
      autopilot_safe,
      confidence: "Medium",
      notes: "Parsed from Webhound report text — requires underlying revalidation",
    });
  });
}

export function defaultCatalogPath() {
  return path.join(
    ROOT,
    "reports/research-engine-v2/marriott-webhound-pattern-catalog.json"
  );
}
