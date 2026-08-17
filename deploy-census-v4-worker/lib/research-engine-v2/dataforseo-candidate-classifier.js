/**
 * Classify DataForSEO SERP/Maps hits into Census discovery candidates.
 * Candidate-only — never Census writes. DataForSEO is never source of truth.
 *
 * v2: prefer brand-official / hotel-official / tourism / owner / factsheet;
 * reject affiliate mirrors and OTA/UGC; Travel Weekly = trusted secondary only.
 */

export const DATAFORSEO_CANDIDATE_CLASSIFIER_VERSION =
  "dataforseo-candidate-classifier-v2";

/** Domains never accepted as official hotel / rooms evidence SoT. */
export const REJECTED_SOURCE_HOSTS = Object.freeze([
  "booking.com",
  "expedia.com",
  "hotels.com",
  "hoteles.com",
  "tripadvisor.com",
  "tripadvisor.co",
  "agoda.com",
  "kayak.com",
  "trivago.com",
  "hotelscombined.com",
  "orbitz.com",
  "travelocity.com",
  "priceline.com",
  "makemytrip.com",
  "tripadvisor.in",
  "facebook.com",
  "instagram.com",
  "twitter.com",
  "x.com",
  "youtube.com",
  "wikipedia.org",
  "reddit.com",
  "yelp.com",
  "airbnb.com",
  "vrbo.com",
  "rooms.aero",
  "tripadvisor.es",
  "tripadvisor.com.mx",
  "tripadvisor.com.br",
  "tripadvisor.com.co",
  "despegar.com",
  "despegar.com.mx",
  "despegar.com.co",
  "google.com",
  "google.com.mx",
  "google.com.br",
  "bing.com",
]);

/** Affiliate / mirror host patterns (not official brand property sites). */
const REJECTED_HOST_SUFFIX_RE =
  /(^|\.)([a-z0-9-]+-)?hotels?\.(com|net|org)$/i;

/** Generic travel blogs / soft UGC — reject as official. */
const REJECTED_BLOG_HOST_RE =
  /(blogspot\.|wordpress\.com|medium\.com|substack\.com|travelblog|hotelblog)/i;

export const BRAND_OFFICIAL_HOST_HINTS = Object.freeze([
  "marriott.com",
  "hilton.com",
  "ihg.com",
  "hyatt.com",
  "accor.com",
  "all.accor.com",
  "choicehotels.com",
  "wyndhamhotels.com",
  "radissonhotels.com",
  "bestwestern.com",
  "melia.com",
  "riu.com",
  "barcelo.com",
  "minorhotels.com",
  "fourseasons.com",
  "rosewoodhotels.com",
  "preferredhotels.com",
  "designhotels.com",
  "kimptonhotels.com",
  "sixsenses.com",
  "sofitel.com",
  "fairmont.com",
  "novotel.com",
  "ibis.com",
  "pullmanhotels.com",
  "mgallery.com",
  "swissotel.com",
  "movenpick.com",
]);

/** Trusted secondary / verification only — never treat as official. */
export const TRUSTED_SECONDARY_HOSTS = Object.freeze([
  "travelweekly.com",
  "northstartravelmedia.com",
  "northstarmeetingsgroup.com",
  "hotelnewsnow.com",
  "costar.com",
  "hospitalitynet.org",
]);

/** Tourism registry / board / destination authority host hints. */
const TOURISM_HOST_HINTS = [
  "gob.mx",
  "sectur.gob.mx",
  "datatur.sectur.gob.mx",
  "mincit.gov.co",
  "fontur.com.co",
  "rnt.gov.co",
  "colombia.travel",
  "gob.do",
  "mitur.gob.do",
  "godominicanrepublic.com",
  "visitpanama.com",
  "atp.gob.pa",
  "visitcostarica.com",
  "ict.go.cr",
  "embratur.gov.br",
  "turismo.gob.pe",
  "sernatur.cl",
  "argentina.travel",
  "visitmexico.com",
  "convention",
  "cvb.",
  "visit",
];

export const SOURCE_TIER = Object.freeze({
  brand_official: "brand_official",
  hotel_official: "hotel_official",
  tourism_registry: "tourism_registry",
  tourism_board: "tourism_board",
  convention_bureau: "convention_bureau",
  owner_developer: "owner_developer",
  factsheet_pdf: "factsheet_pdf",
  official_press: "official_press",
  hospitality_trade_secondary: "hospitality_trade_secondary",
  google_maps_local: "google_maps_local",
  rejected: "rejected",
});

function hostFromUrl(url) {
  try {
    return new URL(String(url || "")).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

export function isBrandOfficialHost(host) {
  if (!host) return false;
  return BRAND_OFFICIAL_HOST_HINTS.some(
    (d) => host === d || host.endsWith(`.${d}`)
  );
}

export function isTrustedSecondaryHost(host) {
  if (!host) return false;
  return TRUSTED_SECONDARY_HOSTS.some(
    (d) => host === d || host.endsWith(`.${d}`)
  );
}

function isTourismHost(host) {
  if (!host) return false;
  const h = host.toLowerCase();
  return TOURISM_HOST_HINTS.some(
    (d) => h === d || h.endsWith(`.${d}`) || h.includes(d)
  );
}

function isRejectedHost(host) {
  if (!host) return false;
  if (REJECTED_SOURCE_HOSTS.some((d) => host === d || host.endsWith(`.${d}`))) {
    return true;
  }
  if (REJECTED_HOST_SUFFIX_RE.test(host) && !isBrandOfficialHost(host)) {
    return true;
  }
  if (REJECTED_BLOG_HOST_RE.test(host)) return true;
  return false;
}

function textHay(item) {
  return `${item.title || ""} ${item.description || ""} ${item.url || ""}`.toLowerCase();
}

function looksOwnerDeveloper(host, hay) {
  if (!host) return false;
  return (
    /desarroll|developer|inmobiliaria|grupo|holdings?|properties|propiedades|owner/i.test(
      host
    ) ||
    /owner|developer|desarrollador|inmobiliaria/i.test(hay)
  );
}

function looksOfficialPress(url, hay) {
  return (
    /\/(press|newsroom|media|noticias|prensa)\b/i.test(url || "") ||
    /\b(press release|comunicado|newsroom)\b/i.test(hay)
  );
}

function looksFactsheet(url, hay) {
  return (
    /\.pdf(\?|$)/i.test(url || "") ||
    /fact-?sheet|factsheet|ficha t[eé]cnica|media kit|brand standards/i.test(
      hay
    ) ||
    /fact-?sheet|factsheet|ficha/i.test(url || "")
  );
}

/**
 * Soft name match — reject when title shares no meaningful tokens with hotel name.
 * @param {string} hotelName
 * @param {string} title
 */
export function nameMatchScore(hotelName, title) {
  const name = String(hotelName || "").toLowerCase();
  const t = String(title || "").toLowerCase();
  if (!name || !t) return { ok: true, hits: 0, tokens: 0 };
  const stop = new Set([
    "hotel",
    "hotels",
    "the",
    "and",
    "de",
    "del",
    "la",
    "el",
    "los",
    "las",
    "by",
    "a",
    "member",
    "of",
  ]);
  const tokens = name
    .split(/[^a-z0-9áéíóúñü]+/i)
    .map((x) => x.toLowerCase())
    .filter((x) => x.length > 3 && !stop.has(x))
    .slice(0, 5);
  if (!tokens.length) return { ok: true, hits: 0, tokens: 0 };
  const hits = tokens.filter((tok) => t.includes(tok)).length;
  return { ok: hits > 0, hits, tokens: tokens.length };
}

/**
 * @param {object} item — organic or maps item
 * @param {{ hotelName?: string, city?: string }} [ctx]
 */
export function classifySerpOrMapsItem(item, ctx = {}) {
  const url = String(item.url || "").trim();
  const host = hostFromUrl(url) || String(item.domain || "")
    .replace(/^www\./i, "")
    .toLowerCase();
  const hay = textHay(item);
  const hasMapsSignals = Boolean(
    item.place_id || item.phone || item.address || (item.latitude != null && item.longitude != null)
  );

  const nameCheck = nameMatchScore(ctx.hotelName, item.title);
  if (ctx.hotelName && item.title && !nameCheck.ok && !hasMapsSignals) {
    return {
      status: "rejected",
      reason: "title_name_mismatch",
      host,
      url: url || null,
      categories: [],
      source_tier: SOURCE_TIER.rejected,
      write_eligible: false,
      name_match: nameCheck,
    };
  }

  /** @type {string[]} */
  const categories = [];
  let source_tier = null;

  // Rejected host URL with no Maps contact/geo payload → hard reject
  if (host && isRejectedHost(host) && !hasMapsSignals) {
    return {
      status: "rejected",
      reason: rejectReasonForHost(host),
      host,
      url: url || null,
      categories: [],
      source_tier: SOURCE_TIER.rejected,
      write_eligible: false,
      name_match: nameCheck,
    };
  }

  // Maps row whose website points at OTA/affiliate: keep address/phone/geo only
  if (host && isRejectedHost(host) && hasMapsSignals) {
    if (item.address) categories.push("address_candidate");
    if (item.phone) categories.push("phone_candidate");
    if (item.latitude != null && item.longitude != null) {
      categories.push("lat_long_candidate");
    }
    categories.push("google_maps_local_candidate");
    return finalize({
      status: "useful",
      reason: "maps_payload_kept_rejected_website",
      host,
      url: null,
      categories,
      source_tier: SOURCE_TIER.google_maps_local,
      write_eligible: false,
      item: { ...item, url: null },
      nameCheck,
    });
  }

  // Trusted secondary (Travel Weekly / Northstar) — never official
  if (isTrustedSecondaryHost(host)) {
    categories.push("trusted_secondary_verification_candidate");
    if (
      /rooms?|keys|habitaciones|cuartos|guest rooms|number of rooms|room count|quartos/i.test(
        hay
      ) ||
      looksFactsheet(url, hay)
    ) {
      categories.push("rooms_evidence_page_candidate");
    }
    return finalize({
      status: "secondary",
      reason: "trusted_secondary_not_official",
      host,
      url,
      categories,
      source_tier: SOURCE_TIER.hospitality_trade_secondary,
      write_eligible: false,
      item,
      nameCheck,
    });
  }

  const brandOfficial = isBrandOfficialHost(host);
  const tourism = isTourismHost(host);
  const factsheet = looksFactsheet(url, hay);
  const press = looksOfficialPress(url, hay);
  const ownerDev = looksOwnerDeveloper(host, hay);

  if (brandOfficial) {
    categories.push("official_hotel_url_candidate");
    source_tier = SOURCE_TIER.brand_official;
  } else if (
    url &&
    host &&
    !isRejectedHost(host) &&
    !/google\.(com|[a-z.]+)$/i.test(host) &&
    (/hotel|resort|inn|suites|hostel|boutique/i.test(host) ||
      /\/(hotels?|properties|property)\//i.test(url) ||
      /hoteldetail|overview|en-us\/hotels/i.test(url))
  ) {
    // Independent hotel official site candidate — stricter than v1
    categories.push("official_hotel_url_candidate");
    source_tier = SOURCE_TIER.hotel_official;
  }

  if (tourism) {
    categories.push("tourism_registry_candidate");
    if (/rnt|registro|habitacion|cuartos|rooms|keys|keys?/i.test(hay)) {
      categories.push("rooms_evidence_page_candidate");
    }
    source_tier =
      source_tier ||
      (/rnt|registro/i.test(hay)
        ? SOURCE_TIER.tourism_registry
        : /convention|cvb|visit/i.test(host)
          ? SOURCE_TIER.convention_bureau
          : SOURCE_TIER.tourism_board);
  }

  if (ownerDev && url && !isRejectedHost(host)) {
    categories.push("owner_developer_candidate");
    source_tier = source_tier || SOURCE_TIER.owner_developer;
  }

  if (factsheet && url && !isRejectedHost(host)) {
    categories.push("rooms_evidence_page_candidate");
    categories.push("factsheet_pdf_candidate");
    source_tier = source_tier || SOURCE_TIER.factsheet_pdf;
  }

  if (press && url && !isRejectedHost(host)) {
    categories.push("official_press_candidate");
    source_tier = source_tier || SOURCE_TIER.official_press;
  }

  if (
    !factsheet &&
    /rooms?|keys|habitaciones|cuartos|guest rooms|number of rooms|room count|quartos|apartamentos|n[uú]mero de habitaciones/i.test(
      hay
    ) &&
    url &&
    !isRejectedHost(host) &&
    (brandOfficial || tourism || source_tier === SOURCE_TIER.hotel_official)
  ) {
    categories.push("rooms_evidence_page_candidate");
  }

  if (hasMapsSignals) {
    categories.push("google_maps_local_candidate");
    source_tier = source_tier || SOURCE_TIER.google_maps_local;
  }
  if (item.address) categories.push("address_candidate");
  if (item.phone) categories.push("phone_candidate");
  if (item.latitude != null && item.longitude != null) {
    categories.push("lat_long_candidate");
  }
  if (item.url && hasMapsSignals && !isRejectedHost(host) && !categories.includes("official_hotel_url_candidate")) {
    // Maps website candidate — not auto-official unless brand host
    if (brandOfficial) {
      categories.push("official_hotel_url_candidate");
      source_tier = SOURCE_TIER.brand_official;
    } else {
      categories.push("maps_website_candidate");
    }
  }

  if (!categories.length) {
    return {
      status: "rejected",
      reason: "no_useful_candidate_signal",
      host,
      url: url || null,
      categories: [],
      source_tier: SOURCE_TIER.rejected,
      write_eligible: false,
      name_match: nameCheck,
    };
  }

  // Prefer / useful only when preferred tier present
  const preferred =
    source_tier === SOURCE_TIER.brand_official ||
    source_tier === SOURCE_TIER.hotel_official ||
    source_tier === SOURCE_TIER.tourism_registry ||
    source_tier === SOURCE_TIER.tourism_board ||
    source_tier === SOURCE_TIER.convention_bureau ||
    source_tier === SOURCE_TIER.owner_developer ||
    source_tier === SOURCE_TIER.factsheet_pdf ||
    source_tier === SOURCE_TIER.official_press ||
    source_tier === SOURCE_TIER.google_maps_local;

  if (!preferred) {
    return {
      status: "rejected",
      reason: "weak_source_downgraded",
      host,
      url: url || null,
      categories: [],
      source_tier: SOURCE_TIER.rejected,
      write_eligible: false,
      name_match: nameCheck,
    };
  }

  return finalize({
    status: "useful",
    reason: null,
    host,
    url,
    categories,
    source_tier,
    write_eligible: false,
    item,
    nameCheck,
  });
}

function rejectReasonForHost(host) {
  if (/hoteles\.com|hotels\.com|rooms\.aero/i.test(host)) {
    return "rejected_affiliate_mirror";
  }
  if (REJECTED_HOST_SUFFIX_RE.test(host)) {
    return "rejected_affiliate_mirror";
  }
  if (REJECTED_BLOG_HOST_RE.test(host)) {
    return "rejected_generic_travel_blog";
  }
  return "rejected_ota_or_ugc_host";
}

function finalize({
  status,
  reason,
  host,
  url,
  categories,
  source_tier,
  write_eligible,
  item,
  nameCheck,
}) {
  return {
    status,
    reason,
    host,
    url: url || null,
    categories: [...new Set(categories)],
    source_tier,
    write_eligible: Boolean(write_eligible),
    storage_policy_flag: "candidate_only_no_census_write",
    match_confidence: estimateMatchConfidence(nameCheck, source_tier, item),
    address: item.address || null,
    phone: item.phone || null,
    latitude: item.latitude ?? null,
    longitude: item.longitude ?? null,
    place_id: item.place_id || null,
    title: item.title || null,
    name_match: nameCheck || null,
  };
}

function estimateMatchConfidence(nameCheck, source_tier, item) {
  let score = 0.4;
  if (nameCheck?.tokens) {
    score += 0.4 * (nameCheck.hits / Math.max(nameCheck.tokens, 1));
  }
  if (source_tier === SOURCE_TIER.brand_official) score += 0.25;
  else if (source_tier === SOURCE_TIER.hotel_official) score += 0.15;
  else if (source_tier === SOURCE_TIER.tourism_registry) score += 0.2;
  else if (source_tier === SOURCE_TIER.google_maps_local) score += 0.1;
  if (item?.place_id) score += 0.05;
  return Math.min(1, Math.round(score * 100) / 100);
}

/**
 * Summarize classified candidates for a record.
 * @param {object[]} classified
 */
export function summarizeClassifiedCandidates(classified = []) {
  const useful = classified.filter((c) => c.status === "useful");
  const secondary = classified.filter((c) => c.status === "secondary");
  const rejected = classified.filter((c) => c.status === "rejected");
  const has = (cat, pool = useful) =>
    pool.filter((c) => c.categories.includes(cat));
  const tierCounts = {};
  for (const c of [...useful, ...secondary]) {
    const t = c.source_tier || "unknown";
    tierCounts[t] = (tierCounts[t] || 0) + 1;
  }
  return {
    useful_count: useful.length,
    secondary_count: secondary.length,
    rejected_count: rejected.length,
    official_hotel_urls: has("official_hotel_url_candidate"),
    rooms_evidence_pages: has("rooms_evidence_page_candidate").concat(
      has("rooms_evidence_page_candidate", secondary)
    ),
    address_candidates: has("address_candidate"),
    phone_candidates: has("phone_candidate"),
    google_maps_candidates: has("google_maps_local_candidate"),
    lat_long_candidates: has("lat_long_candidate"),
    tourism_registry_candidates: has("tourism_registry_candidate"),
    trusted_secondary_candidates: secondary,
    source_tier_counts: tierCounts,
    rejected_reasons: rejected.reduce((acc, r) => {
      acc[r.reason] = (acc[r.reason] || 0) + 1;
      return acc;
    }, {}),
  };
}

/**
 * Rough precision among URL candidates that claim official.
 * Prefer brand_official / tourism over hotel_official heuristics.
 * @param {object[]} usefulOfficial
 */
export function estimateOfficialUrlPrecision(usefulOfficial = []) {
  if (!usefulOfficial.length) {
    return { precision_estimate: null, n: 0, brand_official: 0, hotel_official: 0 };
  }
  const brand = usefulOfficial.filter(
    (c) => c.source_tier === SOURCE_TIER.brand_official
  ).length;
  const hotel = usefulOfficial.filter(
    (c) => c.source_tier === SOURCE_TIER.hotel_official
  ).length;
  // Weight brand_official as high-confidence; hotel_official as 0.55
  const weighted = brand * 1 + hotel * 0.55;
  return {
    precision_estimate: Math.round((weighted / usefulOfficial.length) * 1000) / 1000,
    n: usefulOfficial.length,
    brand_official: brand,
    hotel_official: hotel,
  };
}
