/**
 * Known-chain Official Property URL enrichment — report-only.
 *
 * Combines:
 * - Brand official domain roots (matching / discovery leads)
 * - Optional Google Places websiteUri proposals (when host matches brand domain)
 *
 * Never scrapes OTAs. Never writes Airtable.
 */

import {
  KNOWN_CHAIN_DOMAINS,
  KNOWN_CHAIN_NAME_ALIASES,
} from "./brand-exclusion-audit.js";
import { websiteHost } from "./match-current-census.js";
import {
  evaluateIntakeAutopilotGate,
  isDeniedWebsite,
} from "./intake-autopilot-gates.js";
import { getDrOsmStewardCatalogEntry } from "./dr-osm-steward-official-url-catalog.js";

export const KNOWN_CHAIN_URL_ENRICHMENT_VERSION =
  "known-chain-official-url-enrichment-v1";

/**
 * Brand label → preferred official site roots (host only).
 * Built from KNOWN_CHAIN_DOMAINS + DR-relevant regional chains.
 * Property-level paths are never invented — only domain corroboration / search leads.
 */
export const KNOWN_CHAIN_BRAND_HOSTS = Object.freeze({
  RIU: ["riu.com"],
  Barceló: ["barcelo.com", "barcelobavarocaribe.com"],
  Meliá: ["melia.com"],
  "Dreams (Hyatt Inclusive Collection)": [
    "dreamsresorts.com",
    "hyatt.com",
    "hyattinclusivecollection.com",
  ],
  "Secrets (Hyatt Inclusive Collection)": [
    "secretsresorts.com",
    "hyatt.com",
    "hyattinclusivecollection.com",
  ],
  "Breathless (Hyatt Inclusive Collection)": [
    "breathlessresorts.com",
    "hyatt.com",
    "hyattinclusivecollection.com",
  ],
  "Hyatt Zilara": ["hyatt.com", "hyattinclusivecollection.com"],
  "Bahía Príncipe": ["bahia-principe.com"],
  Occidental: ["occidentalhotels.com", "barcelo.com"],
  Catalonia: ["cataloniahotels.com"],
  "Club Med": ["clubmed.com"],
  Wyndham: [
    "wyndhamhotels.com",
    "vivaresortsbywyndham.com",
    "puntacanawyndhamalltra.com",
  ],
  "Wyndham Alltra": ["wyndhamhotels.com", "puntacanawyndhamalltra.com"],
  Marriott: ["marriott.com"],
  Westin: ["marriott.com"],
  Sheraton: ["marriott.com"],
  "Four Points by Sheraton": ["marriott.com"],
  "Renaissance Hotels": ["marriott.com"],
  "Autograph Collection": ["marriott.com"],
  "Hilton Hotels & Resorts": ["hilton.com"],
  Hilton: ["hilton.com"],
  "Crowne Plaza": ["ihg.com"],
  InterContinental: ["ihg.com"],
  "Holiday Inn": ["ihg.com"],
  "Quality Inn": ["choicehotels.com"],
  "Be Live": ["belivehotels.com"],
  Hodelpa: ["hodelpa.com"],
  "Amhsa Marina Hotels": [
    "amhsamarina.com",
    "grandparadiseplayadorada.com",
  ],
  "Breezes (SuperClubs)": ["breezes.com", "superclubs.com"],
  Iberostar: ["iberostar.com"],
  Excellence: ["excellenceresorts.com"],
  "Excellence Resorts": ["excellenceresorts.com"],
  "Hard Rock Hotels": ["hardrockhotels.com", "hardrock.com"],
  "Starfish Resorts": ["starfishresorts.com", "hotelcasahemingway.com"],
});

/** Tokens too generic to corroborate a property-level URL path alone. */
const URL_NAME_STOPWORDS = Object.freeze(
  new Set([
    "hotel",
    "hotels",
    "resort",
    "resorts",
    "spa",
    "the",
    "and",
    "by",
    "collection",
    "all",
    "inclusive",
    "grand",
    "deluxe",
    "adults",
    "only",
    "club",
    "suites",
    "villas",
    "beach",
    "reef",
    "palace",
    "royal",
    "experience",
    "riu",
    "barcelo",
    "melia",
    "bahia",
    "principe",
    "dreams",
    "secrets",
    "breathless",
    "occidental",
    "catalonia",
    "hodelpa",
    "wyndham",
    "marriott",
    "hilton",
    "iberostar",
    "excellence",
    "hyatt",
    "crowne",
    "plaza",
    "holiday",
    "inn",
    "westin",
    "sheraton",
    "intercontinental",
    "quality",
    "live",
    "amhsa",
    "marina",
    "hard",
    "rock",
    "viva",
    "alltra",
    "starfish",
    "allegro",
    "luxury",
    "tangerine",
  ])
);

/**
 * Strip tracking params before proposing Official Property URL.
 * Unwraps known ad redirect wrappers (e.g. Adform → brand site).
 * @param {string} url
 */
export function sanitizeOfficialUrlCandidate(url) {
  let raw = String(url || "").trim();
  if (!raw) return "";
  try {
    // Adform click wrappers embed the brand URL in cpdir= (often with `;` separators)
    const cpdirMatch = raw.match(/[?&;]cpdir=([^&;#]+)/i);
    if (cpdirMatch?.[1] && /adform\.net/i.test(raw)) {
      raw = decodeURIComponent(cpdirMatch[1]);
    }
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    const drop = [
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_content",
      "utm_term",
      "mb",
      "scid",
      "src",
      "SEO_id",
      "cm_mmc",
      "partner",
    ];
    for (const k of drop) u.searchParams.delete(k);
    const qs = u.searchParams.toString();
    return `${u.origin}${u.pathname}${qs ? `?${qs}` : ""}${u.hash || ""}`;
  } catch {
    return raw;
  }
}

/**
 * Distinctive name/city tokens used to corroborate a property URL path.
 * @param {string} text
 */
export function distinctiveUrlNameTokens(text) {
  return String(text || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 4 && !URL_NAME_STOPWORDS.has(t));
}

/**
 * True when property name and/or city tokens appear in the candidate URL path.
 * Prevents promoting wrong Google websiteUri matches (e.g. Barceló Puerto Plata → Santo Domingo).
 * @param {string} propertyName
 * @param {string} url
 * @param {string} [city]
 */
export function propertyUrlNameCorroborates(propertyName, url, city = "") {
  const rawUrl = String(url || "").trim();
  if (!rawUrl) return false;
  let hay = "";
  try {
    const u = new URL(/^https?:\/\//i.test(rawUrl) ? rawUrl : `https://${rawUrl}`);
    hay = `${u.hostname}${u.pathname}`.toLowerCase();
  } catch {
    return false;
  }

  const pathParts = new Set(
    hay
      .normalize("NFD")
      .replace(/\p{M}/gu, "")
      .split(/[^a-z0-9]+/)
      .filter((p) => p.length >= 3)
  );
  // Marriott / marketing often glue place names: puntacana, playadorada
  if (pathParts.has("puntacana")) {
    pathParts.add("punta");
    pathParts.add("cana");
  }
  if (pathParts.has("playadorada")) {
    pathParts.add("playa");
    pathParts.add("dorada");
  }

  const nameTokens = distinctiveUrlNameTokens(propertyName);
  const cityTokens = distinctiveUrlNameTokens(city).filter(
    (t) => !["unknown", "playas", "republic", "dominicana"].includes(t)
  );

  const hits = (tokens) =>
    tokens.filter((t) => {
      if (pathParts.has(t)) return true;
      // Allow longer tokens as hyphenated segment substrings (gran-almirante)
      if (t.length >= 6) {
        for (const p of pathParts) {
          if (p.includes(t) || t.includes(p) && p.length >= 6) return true;
        }
      }
      return false;
    });

  const nameHits = hits(nameTokens);
  const cityHits = hits(cityTokens);

  // Strong unique name tokens (e.g. Sensimar, Turquesa) must appear when present
  const strong = nameTokens.filter((t) => t.length >= 7);
  if (strong.length && !strong.every((t) => nameHits.includes(t))) {
    return false;
  }

  if (nameTokens.length >= 2) {
    return (
      nameHits.length >= 2 ||
      (nameHits.length >= 1 && nameHits.some((t) => t.length >= 6) && cityHits.length >= 1)
    );
  }
  if (nameTokens.length === 1) {
    return nameHits.length === 1;
  }
  // Generic OSM brand-only names (e.g. "Sheraton") — require city in path
  return cityHits.length >= 1;
}

/** Directory / brand search lead templates (not Official Property URLs). */
export const KNOWN_CHAIN_SEARCH_LEAD_TEMPLATES = Object.freeze({
  RIU: "https://www.riu.com/en/search?q={query}",
  Barceló: "https://www.barcelo.com/en-us/hotels/?q={query}",
  Meliá: "https://www.melia.com/en/hotels?q={query}",
  "Dreams (Hyatt Inclusive Collection)":
    "https://www.dreamsresorts.com/search?q={query}",
  "Bahía Príncipe": "https://www.bahia-principe.com/en/hotels/?q={query}",
  Occidental: "https://www.occidentalhotels.com/en/hotels?q={query}",
  Catalonia: "https://www.cataloniahotels.com/en/hotels?q={query}",
  "Club Med": "https://www.clubmed.com/s?q={query}",
  Wyndham: "https://www.wyndhamhotels.com/search?query={query}",
  Marriott: "https://www.marriott.com/search/default.mi?propertyName={query}",
  "Hilton Hotels & Resorts":
    "https://www.hilton.com/en/search/?query={query}",
  "Crowne Plaza": "https://www.ihg.com/crowneplaza/hotels/us/en/find-hotels/hotel/list?qDest={query}",
  InterContinental:
    "https://www.ihg.com/intercontinental/hotels/us/en/find-hotels/hotel/list?qDest={query}",
  "Holiday Inn":
    "https://www.ihg.com/holidayinn/hotels/us/en/find-hotels/hotel/list?qDest={query}",
  "Be Live": "https://www.belivehotels.com/en/hotels?q={query}",
  Hodelpa: "https://www.hodelpa.com/?s={query}",
});

function hostMatchesBrand(host, brandHosts) {
  if (!host) return false;
  return brandHosts.some((d) => host === d || host.endsWith(`.${d}`));
}

/**
 * Resolve preferred hosts for a brand label.
 * @param {string} brandLabel
 */
export function resolveBrandOfficialHosts(brandLabel) {
  const label = String(brandLabel || "").trim();
  if (!label) return [];
  if (KNOWN_CHAIN_BRAND_HOSTS[label]) {
    return [...KNOWN_CHAIN_BRAND_HOSTS[label]];
  }
  const lower = label.toLowerCase();
  for (const [key, hosts] of Object.entries(KNOWN_CHAIN_BRAND_HOSTS)) {
    if (key.toLowerCase() === lower || lower.includes(key.toLowerCase())) {
      return [...hosts];
    }
  }
  // Fall back: invert KNOWN_CHAIN_DOMAINS by matching label substring
  const hosts = [];
  for (const [domain, mappedLabel] of Object.entries(KNOWN_CHAIN_DOMAINS)) {
    if (
      mappedLabel.toLowerCase().includes(lower) ||
      lower.includes(mappedLabel.toLowerCase().split(" ")[0])
    ) {
      hosts.push(domain);
    }
  }
  for (const [alias, mapped] of KNOWN_CHAIN_NAME_ALIASES) {
    if (lower.includes(alias) || alias.includes(lower)) {
      for (const [domain, mappedLabel] of Object.entries(KNOWN_CHAIN_DOMAINS)) {
        if (mappedLabel === mapped) hosts.push(domain);
      }
    }
  }
  return [...new Set(hosts)];
}

/**
 * Build a brand-directory search lead (not an Official Property URL).
 * @param {string} brandLabel
 * @param {string} propertyName
 */
export function buildKnownChainSearchLead(brandLabel, propertyName) {
  const tpl =
    KNOWN_CHAIN_SEARCH_LEAD_TEMPLATES[brandLabel] ||
    Object.entries(KNOWN_CHAIN_SEARCH_LEAD_TEMPLATES).find(([k]) =>
      brandLabel?.toLowerCase().includes(k.toLowerCase())
    )?.[1];
  if (!tpl) return "";
  const q = encodeURIComponent(String(propertyName || "").trim());
  return tpl.replace(/\{query\}/g, q);
}

/**
 * Enrich one steward row using Google Places lookup result (optional) + brand hosts.
 * @param {object} row — intake plan row
 * @param {object|null} googleResult — from google-places-hotel-url-lookup
 */
export function enrichKnownChainOfficialUrl(row, googleResult = null) {
  const brand = String(row.current_brand || "").trim();
  const name = String(row.property_name || "").trim();
  const city = String(
    row.city || row.payload?.City || row.sanitized_payload_preview?.City || ""
  ).trim();
  const brandHosts = resolveBrandOfficialHosts(brand);
  const searchLead = buildKnownChainSearchLead(brand, name);
  const catalog = getDrOsmStewardCatalogEntry(row.source_record_id);

  const googleUrlRaw = String(
    googleResult?.suggested_official_property_url ||
      googleResult?.place?.google_website_uri ||
      googleResult?.place?.googleWebsiteUri ||
      ""
  ).trim();
  const googleUrl = sanitizeOfficialUrlCandidate(googleUrlRaw);
  const googleHost = websiteHost(googleUrl) || "";
  const googleOnBrandDomain =
    Boolean(googleUrl) &&
    !isDeniedWebsite(googleUrl) &&
    hostMatchesBrand(googleHost, brandHosts);
  const nameOk = propertyUrlNameCorroborates(name, googleUrl, city);

  let proposedUrl = "";
  let proposalSource = "";
  let confidence = "none";
  let requiresSteward = true;
  let proposedCity = "";
  const reasons = [];

  // Curated DR OSM steward catalog (named properties only) — preferred High path.
  const catalogUrl = sanitizeOfficialUrlCandidate(catalog?.url || "");
  const catalogHosts = [
    ...(catalog?.brand_hosts || []),
    ...brandHosts,
  ];
  if (
    catalogUrl &&
    !isDeniedWebsite(catalogUrl) &&
    hostMatchesBrand(websiteHost(catalogUrl) || "", catalogHosts)
  ) {
    proposedUrl = catalogUrl;
    proposalSource = "catalog_official_url";
    confidence = "high";
    requiresSteward = false;
    proposedCity = String(catalog.city || "").trim();
    reasons.push("dr_osm_steward_official_url_catalog");
    if (proposedCity) reasons.push("catalog_city_hint");
  } else if (catalog && String(catalog.city || "").trim()) {
    // City-only catalog (clears known_brand_missing_city when URL already present).
    proposedCity = String(catalog.city || "").trim();
    proposalSource = "catalog_city_only";
    confidence = "high";
    requiresSteward = false;
    reasons.push("dr_osm_steward_city_catalog");
  } else if (googleOnBrandDomain && nameOk) {
    proposedUrl = googleUrl;
    proposalSource = "google_places_website_on_brand_domain";
    confidence = "high";
    requiresSteward = false;
    reasons.push("google_website_host_matches_known_chain_domain");
    reasons.push("property_name_or_city_corroborates_url_path");
    if (googleResult?.match_confidence === "High") {
      reasons.push("google_match_confidence_high");
    } else {
      reasons.push("google_match_confidence_lifted_by_name_corroboration");
    }
  } else if (
    googleOnBrandDomain &&
    googleResult?.website_proposal?.propose_as_official_url
  ) {
    proposedUrl = googleUrl;
    proposalSource = "google_places_website_on_brand_domain";
    confidence =
      googleResult.match_confidence === "High" ? "high" : "medium";
    requiresSteward =
      confidence !== "high" ||
      Boolean(googleResult.website_proposal?.requires_steward);
    reasons.push("google_website_host_matches_known_chain_domain");
    if (!nameOk) reasons.push("name_city_path_not_corroborated");
  } else if (
    googleResult?.website_proposal?.propose_as_official_url &&
    googleUrl &&
    !isDeniedWebsite(googleUrl)
  ) {
    proposedUrl = googleUrl;
    proposalSource = "google_places_website_off_brand_domain";
    confidence = nameOk ? "medium" : "medium";
    requiresSteward = true;
    reasons.push("google_website_usable_but_host_not_in_brand_host_map");
    if (nameOk) reasons.push("property_name_or_city_corroborates_url_path");
  } else if (googleUrl && isDeniedWebsite(googleUrl)) {
    reasons.push("google_website_denylisted");
  } else if (googleOnBrandDomain && !nameOk) {
    reasons.push("google_website_on_brand_but_name_city_path_mismatch");
  } else if (
    googleUrl &&
    googleResult?.website_proposal &&
    !googleResult.website_proposal.propose_as_official_url
  ) {
    reasons.push(
      googleResult.website_proposal.reason || "google_website_not_proposed"
    );
  } else if (!googleUrl && googleResult?.status === "matched") {
    reasons.push("google_matched_but_no_website_uri");
  } else if (!googleResult || googleResult.status === "no_match") {
    reasons.push("no_google_website_proposal");
  }

  if (!brandHosts.length) {
    reasons.push("brand_hosts_unmapped");
  }
  if (searchLead) {
    reasons.push("brand_search_lead_available");
  }

  return {
    version: KNOWN_CHAIN_URL_ENRICHMENT_VERSION,
    source_record_id: row.source_record_id || "",
    property_name: name,
    current_brand: brand,
    intake_class: row.intake_class || "",
    brand_official_hosts: brandHosts,
    brand_search_lead_url: searchLead,
    google_status: googleResult?.status || "not_run",
    google_place_id: googleResult?.place?.google_place_id || "",
    google_website_uri: googleUrl || googleUrlRaw || "",
    google_match_confidence: googleResult?.match_confidence || "",
    proposed_official_property_url: proposedUrl,
    proposed_city: proposedCity,
    proposal_source: proposalSource,
    proposal_confidence: confidence,
    requires_steward: requiresSteward,
    apply_as_official_url_candidate: Boolean(proposedUrl),
    reasons,
    policy: {
      airtable_write: false,
      brand_homepage_alone_not_official_url: true,
      search_lead_not_official_url: true,
      google_places_product_use: "restricted_refresh_required",
      name_path_corroboration_required_for_low_google_match: true,
    },
  };
}

/**
 * Simulate re-gate after applying proposed Official Property URL into payload.
 * @param {object} row
 * @param {object} enrichment
 */
export function simulateIntakeGateWithProposedUrl(row, enrichment) {
  const payload = {
    ...(row.payload || row.sanitized_payload_preview || {}),
  };
  if (enrichment?.apply_as_official_url_candidate && enrichment.proposed_official_property_url) {
    payload["Official Property URL"] = enrichment.proposed_official_property_url;
  }
  if (enrichment?.proposed_city) {
    payload.City = enrichment.proposed_city;
  }
  const gate = evaluateIntakeAutopilotGate({
    lane: row.lane,
    intake_class: row.intake_class,
    hpc_recommended_action: row.hpc_recommended_action || "likely_new_candidate",
    quality_score: row.quality_score,
    wikidata_match_confidence: row.wikidata_match_confidence || "",
    sanitized_payload_preview: payload,
  });
  return {
    simulated: true,
    prior_decision: row.decision,
    new_decision: gate.decision,
    decision_changed: gate.decision !== row.decision,
    new_reasons: gate.reasons,
    new_human_review_required: gate.human_review_required,
    production_writable_insert: gate.production_writable_insert,
  };
}

/**
 * Batch enrich missing-URL steward rows.
 * @param {object[]} rows
 * @param {Map<string, object>|object} googleBySourceId
 */
export function runKnownChainOfficialUrlEnrichmentBatch(
  rows,
  googleBySourceId = new Map()
) {
  const map =
    googleBySourceId instanceof Map
      ? googleBySourceId
      : new Map(Object.entries(googleBySourceId || {}));

  const enrichments = [];
  for (const row of rows) {
    const gid = String(row.source_record_id || "");
    const google = map.get(gid) || null;
    const enrichment = enrichKnownChainOfficialUrl(row, google);
    const sim = simulateIntakeGateWithProposedUrl(row, enrichment);
    enrichments.push({ ...enrichment, re_gate: sim });
  }

  const proposed = enrichments.filter((e) => e.apply_as_official_url_candidate);
  const decisionLift = enrichments.filter(
    (e) => e.re_gate?.decision_changed && e.re_gate?.new_decision === "auto_insert"
  );

  return {
    version: KNOWN_CHAIN_URL_ENRICHMENT_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry_run_report_only",
    airtable_write: false,
    input_count: rows.length,
    proposed_official_url_count: proposed.length,
    high_confidence_count: proposed.filter((e) => e.proposal_confidence === "high")
      .length,
    brand_search_leads: enrichments.filter((e) => e.brand_search_lead_url).length,
    brand_hosts_unmapped: enrichments.filter((e) =>
      e.reasons.includes("brand_hosts_unmapped")
    ).length,
    simulated_auto_insert_lift: decisionLift.length,
    enrichments,
  };
}
