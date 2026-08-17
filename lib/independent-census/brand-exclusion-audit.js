/**
 * Brand-exclusion audit for independent census discovery.
 *
 * Classifies OSM (or other) candidates into branded Autopilot route vs
 * independent-unaffiliated lane using Active/Live Brand Setup dictionary +
 * official brand domains. Read-only; never writes Airtable / Brand Setup.
 */

import {
  buildCanonicalBrandDictionary,
  familyFromOfficialUrl,
  lookupCanonicalBrand,
} from "../research-engine-v2/census-brand-canonical-dictionary.js";

export const BRAND_EXCLUSION_AUDIT_VERSION = "independent-census-brand-exclusion-v1";

export const ROUTE_BUCKETS = Object.freeze({
  BRANDED_ACTIVE: "route_branded_active_setup",
  BRANDED_SOFT: "route_branded_soft_collection",
  BRANDED_DOMAIN: "route_branded_official_domain",
  /** Major chain detected but not Active/Live Brand Setup — not independent lane. */
  KNOWN_CHAIN_HOLD: "route_known_chain_not_active",
  POSSIBLE_BRANDED: "steward_possible_branded",
  INDEPENDENT_CANDIDATE: "independent_unaffiliated_candidate",
  WEAK_IDENTITY: "hold_weak_identity",
});

/**
 * Extended CALA / Caribbean chain signals for independent-lane exclusion.
 * Broader than Active/Live Brand Setup — prevents RIU / Barceló / Melía / Hyatt
 * Inclusive Collection / etc. from falsely entering the unaffiliated pool.
 * Does NOT activate Brand Explorer / Autopilot Active scope.
 */
export const KNOWN_CHAIN_NAME_ALIASES = Object.freeze([
  // Hyatt Inclusive / related
  ["secrets", "Secrets (Hyatt Inclusive Collection)"],
  ["dreams resorts", "Dreams (Hyatt Inclusive Collection)"],
  ["dreams ", "Dreams (Hyatt Inclusive Collection)"],
  ["breathless", "Breathless (Hyatt Inclusive Collection)"],
  ["hyatt zilara", "Hyatt Zilara"],
  ["hyatt ziva", "Hyatt Ziva"],
  ["hyatt", "Hyatt"],
  // Spanish / regional majors common in DR
  ["barceló", "Barceló"],
  ["barcelo", "Barceló"],
  ["meliá", "Meliá"],
  ["melia", "Meliá"],
  ["riu palace", "RIU"],
  ["riu ", "RIU"],
  ["hotel riu", "RIU"],
  ["iberostar", "Iberostar"],
  ["bahia principe", "Bahía Príncipe"],
  ["bahía príncipe", "Bahía Príncipe"],
  ["grand bahia", "Bahía Príncipe"],
  ["excellence ", "Excellence Resorts"],
  ["occidental", "Occidental"],
  ["catalonia ", "Catalonia"],
  ["club med", "Club Med"],
  ["hard rock", "Hard Rock Hotels"],
  ["majestic ", "Majestic Resorts"],
  ["hodelpa", "Hodelpa"],
  ["be live", "Be Live"],
  ["wyndham", "Wyndham"],
  ["alltra", "Wyndham Alltra"],
  ["viva dominicus", "Viva by Wyndham"],
  ["marriott", "Marriott"],
  ["sheraton", "Sheraton"],
  ["westin", "Westin"],
  ["hilton", "Hilton"],
  ["hampton", "Hampton by Hilton"],
  ["crowne plaza", "Crowne Plaza"],
  ["holiday inn", "Holiday Inn"],
  ["intercontinental", "InterContinental"],
  ["radisson", "Radisson"],
  ["accor", "Accor"],
  ["ibis ", "ibis"],
  ["novotel", "Novotel"],
  ["sofitel", "Sofitel"],
  ["mercure", "Mercure"],
  ["pullman", "Pullman"],
  // Additional DR / Caribbean majors seen in 2026-08-07 pilot
  ["nickelodeon", "Nickelodeon Hotels (Karisma)"],
  ["karisma", "Karisma Hotels"],
  ["lopesan", "Lopesan"],
  ["sirenis", "Sirenis Hotels & Resorts"],
  ["renaissance", "Renaissance Hotels"],
  ["starfish", "Starfish Resorts"],
  ["amhsa", "Amhsa Marina Hotels"],
  ["breezes", "Breezes (SuperClubs)"],
  ["blau hotels", "Blau Hotels"],
  ["blau ", "Blau Hotels"],
]);

export const KNOWN_CHAIN_DOMAINS = Object.freeze({
  "hyatt.com": "Hyatt",
  "secretsresorts.com": "Secrets (Hyatt Inclusive Collection)",
  "dreamsresorts.com": "Dreams (Hyatt Inclusive Collection)",
  "breathlessresorts.com": "Breathless (Hyatt Inclusive Collection)",
  "barcelo.com": "Barceló",
  "barcelobavarocaribe.com": "Barceló",
  "melia.com": "Meliá",
  "riu.com": "RIU",
  "iberostar.com": "Iberostar",
  "bahia-principe.com": "Bahía Príncipe",
  "excellenceresorts.com": "Excellence Resorts",
  "occidentalhotels.com": "Occidental",
  "cataloniahotels.com": "Catalonia",
  "clubmed.com": "Club Med",
  "hardrockhotels.com": "Hard Rock Hotels",
  "wyndhamhotels.com": "Wyndham",
  "marriott.com": "Marriott",
  "hilton.com": "Hilton",
  "ihg.com": "IHG",
  "choicehotels.com": "Choice",
  "radissonhotels.com": "Choice",
  "accor.com": "Accor",
  "all.accor.com": "Accor",
  "karismahotels.com": "Karisma Hotels",
  "lopesan.com": "Lopesan",
  "sirenishotels.com": "Sirenis Hotels & Resorts",
});

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Build longest-first alias patterns for property-name scanning.
 * @param {object} dictionary
 */
export function buildBrandNameMatchers(dictionary) {
  /** @type {Array<{ alias: string, canonical: string, soft: boolean, pattern: RegExp }>} */
  const matchers = [];
  const seen = new Set();

  const pushAlias = (alias, canonical, soft) => {
    const a = norm(alias);
    if (!a || a.length < 3) return;
    if (seen.has(a)) return;
    // Skip ultra-generic tokens that create false positives in Spanish names
    if (["hotel", "hotels", "inn", "resort", "collection"].includes(a)) return;
    seen.add(a);
    matchers.push({
      alias: a,
      canonical,
      soft: Boolean(soft),
      pattern: new RegExp(`(?:^|[^a-z0-9])${escapeRegExp(a)}(?:[^a-z0-9]|$)`, "i"),
    });
  };

  for (const entry of dictionary.brands || []) {
    const soft = Boolean(entry.soft_brand_collection);
    pushAlias(entry.canonical_brand_name, entry.canonical_brand_name, soft);
    for (const alias of entry.accepted_aliases || []) {
      pushAlias(alias, entry.canonical_brand_name, soft);
    }
  }

  for (const [alias, canonical] of dictionary.alias_to_canonical || []) {
    const entry =
      dictionary.by_canonical_norm?.get(norm(canonical)) ||
      dictionary.by_canonical_norm?.get(norm(canonical).replace(/[^a-z0-9]/g, ""));
    pushAlias(alias, canonical, Boolean(entry?.soft_brand_collection));
  }

  matchers.sort((a, b) => b.alias.length - a.alias.length);
  return matchers;
}

/**
 * Scan hotel name / brand / website for Active Brand signals.
 * @param {{ rawHotelName?: string, rawBrand?: string, rawWebsite?: string, qualityScore?: number, missingFields?: string[] }} candidate
 * @param {{ dictionary?: object, matchers?: ReturnType<typeof buildBrandNameMatchers>, minQualityForIndependent?: number }} [opts]
 */
export function classifyCandidateBrandRoute(candidate, opts = {}) {
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary({ region: "CALA" });
  const matchers = opts.matchers || buildBrandNameMatchers(dictionary);
  const minQuality = opts.minQualityForIndependent ?? 40;

  const name = String(candidate.rawHotelName || "");
  const brand = String(candidate.rawBrand || "");
  const website = String(candidate.rawWebsite || "");
  const quality =
    typeof candidate.qualityScore === "number" ? candidate.qualityScore : null;
  const missing = Array.isArray(candidate.missingFields)
    ? candidate.missingFields
    : [];

  const signals = [];

  if (brand.trim()) {
    const hit = lookupCanonicalBrand(brand, dictionary, {
      propertyName: name,
      sourceUrl: website,
    });
    if (hit.ok && hit.canonical) {
      signals.push({
        kind: "osm_brand_tag",
        canonical: hit.canonical,
        match: hit.match,
        soft: Boolean(hit.entry?.soft_brand_collection),
      });
    } else if (brand.trim().length >= 3) {
      signals.push({
        kind: "osm_brand_tag_unresolved",
        raw: brand.trim(),
      });
    }
  }

  const domainFamily = familyFromOfficialUrl(website);
  if (domainFamily) {
    signals.push({
      kind: "official_brand_domain",
      family: domainFamily,
      website,
    });
  }

  const host = (() => {
    try {
      const raw = String(website || "").trim();
      if (!raw) return "";
      const withProto = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
      return new URL(withProto).hostname.replace(/^www\./i, "").toLowerCase();
    } catch {
      return "";
    }
  })();
  if (host) {
    for (const [domain, label] of Object.entries(KNOWN_CHAIN_DOMAINS)) {
      if (host === domain || host.endsWith(`.${domain}`)) {
        signals.push({
          kind: "known_chain_domain",
          canonical: label,
          family: label,
          website,
        });
        break;
      }
    }
  }

  for (const m of matchers) {
    if (m.pattern.test(name)) {
      signals.push({
        kind: "name_alias_match",
        alias: m.alias,
        canonical: m.canonical,
        soft: m.soft,
      });
      break;
    }
  }

  const nameNorm = norm(name);
  for (const [alias, label] of KNOWN_CHAIN_NAME_ALIASES) {
    const a = norm(alias).trim();
    if (!a || a.length < 3) continue;
    // Word-boundary only — avoid "ibis" matching inside "Ibiscus"
    const re = new RegExp(`(?:^|[^a-z0-9])${escapeRegExp(a)}(?:[^a-z0-9]|$)`, "i");
    if (re.test(nameNorm) || re.test(name)) {
      signals.push({
        kind: "known_chain_name",
        alias: a,
        canonical: label,
      });
      break;
    }
  }

  const activeHit = signals.find(
    (s) =>
      (s.kind === "osm_brand_tag" || s.kind === "name_alias_match") &&
      s.canonical &&
      !s.soft
  );
  const softHit = signals.find(
    (s) =>
      (s.kind === "osm_brand_tag" || s.kind === "name_alias_match") && s.soft
  );
  const domainHit = signals.find((s) => s.kind === "official_brand_domain");
  const knownChainHit = signals.find(
    (s) => s.kind === "known_chain_name" || s.kind === "known_chain_domain"
  );
  const unresolvedBrand = signals.find((s) => s.kind === "osm_brand_tag_unresolved");

  let route = ROUTE_BUCKETS.INDEPENDENT_CANDIDATE;
  let reason = "no_active_brand_signal";
  let matchedBrand = null;
  let matchedFamily = null;

  if (activeHit) {
    route = ROUTE_BUCKETS.BRANDED_ACTIVE;
    reason = activeHit.kind;
    matchedBrand = activeHit.canonical;
  } else if (softHit) {
    route = ROUTE_BUCKETS.BRANDED_SOFT;
    reason = softHit.kind;
    matchedBrand = softHit.canonical;
  } else if (domainHit) {
    route = ROUTE_BUCKETS.BRANDED_DOMAIN;
    reason = "official_brand_domain";
    matchedFamily = domainHit.family;
  } else if (knownChainHit) {
    route = ROUTE_BUCKETS.KNOWN_CHAIN_HOLD;
    reason = knownChainHit.kind;
    matchedBrand = knownChainHit.canonical;
    matchedFamily = knownChainHit.family || knownChainHit.canonical;
  } else if (unresolvedBrand) {
    const rawN = norm(unresolvedBrand.raw);
    const knownFromTag = KNOWN_CHAIN_NAME_ALIASES.find(([alias]) => {
      const a = norm(alias).trim();
      return a && (rawN === a || rawN.includes(a) || a.includes(rawN));
    });
    if (knownFromTag) {
      route = ROUTE_BUCKETS.KNOWN_CHAIN_HOLD;
      reason = "osm_brand_tag_known_chain";
      matchedBrand = knownFromTag[1];
    } else {
      route = ROUTE_BUCKETS.POSSIBLE_BRANDED;
      reason = "osm_brand_tag_not_in_active_dictionary";
      matchedBrand = unresolvedBrand.raw;
    }
  } else if (
    missing.includes("missingName") ||
    (quality != null && quality < minQuality)
  ) {
    route = ROUTE_BUCKETS.WEAK_IDENTITY;
    reason = missing.includes("missingName")
      ? "missing_name"
      : "below_min_quality";
  }

  return {
    route,
    reason,
    matched_brand: matchedBrand,
    matched_family: matchedFamily,
    signals,
    independent_lane_eligible:
      route === ROUTE_BUCKETS.INDEPENDENT_CANDIDATE,
  };
}

/**
 * @param {object[]} candidates
 * @param {{ region?: string, minQualityForIndependent?: number, matchRowsBySourceId?: Map<string, object> }} [opts]
 */
export function auditBrandExclusion(candidates, opts = {}) {
  const dictionary = buildCanonicalBrandDictionary({
    region: opts.region || "CALA",
  });
  const matchers = buildBrandNameMatchers(dictionary);
  const matchMap = opts.matchRowsBySourceId || new Map();

  const counts = Object.fromEntries(
    Object.values(ROUTE_BUCKETS).map((k) => [k, 0])
  );
  const byBrand = new Map();
  const rows = [];

  for (const c of candidates) {
    const classification = classifyCandidateBrandRoute(c, {
      dictionary,
      matchers,
      minQualityForIndependent: opts.minQualityForIndependent,
    });
    counts[classification.route] = (counts[classification.route] || 0) + 1;
    if (classification.matched_brand) {
      byBrand.set(
        classification.matched_brand,
        (byBrand.get(classification.matched_brand) || 0) + 1
      );
    }

    const sourceId = String(c.sourceRecordId || "");
    const match = matchMap.get(sourceId) || null;

    rows.push({
      sourceRecordId: sourceId,
      rawHotelName: c.rawHotelName || "",
      rawCity: c.rawCity || "",
      rawCountry: c.rawCountry || "",
      rawBrand: c.rawBrand || "",
      rawWebsite: c.rawWebsite || "",
      qualityScore: c.qualityScore ?? null,
      qualityTier: c.qualityTier || "",
      missingFields: (c.missingFields || []).join("|"),
      route: classification.route,
      reason: classification.reason,
      matchedBrand: classification.matched_brand || "",
      matchedFamily: classification.matched_family || "",
      independentLaneEligible: classification.independent_lane_eligible,
      censusMatchConfidence: match?.matchConfidence || "",
      censusRecommendedAction: match?.recommendedAction || "",
      matchedCensusName: match?.matchedCensusName || "",
      signalKinds: classification.signals.map((s) => s.kind).join("|"),
    });
  }

  const independent = rows.filter((r) => r.independentLaneEligible);
  const withWebsite = independent.filter((r) => String(r.rawWebsite || "").trim());
  const withCity = independent.filter((r) => String(r.rawCity || "").trim());
  const likelyNew = independent.filter(
    (r) => r.censusRecommendedAction === "likely_new_candidate"
  );
  const likelyExisting = independent.filter(
    (r) => r.censusRecommendedAction === "likely_existing"
  );
  const highQualityIndependent = independent.filter(
    (r) => (r.qualityScore ?? 0) >= 70
  );

  const topBrands = [...byBrand.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 25)
    .map(([brand, count]) => ({ brand, count }));

  /** Failure / learning taxonomy for batch-learning loop */
  const taxonomy = {
    branded_route_to_autopilot:
      (counts[ROUTE_BUCKETS.BRANDED_ACTIVE] || 0) +
      (counts[ROUTE_BUCKETS.BRANDED_SOFT] || 0) +
      (counts[ROUTE_BUCKETS.BRANDED_DOMAIN] || 0),
    known_chain_hold_not_active: counts[ROUTE_BUCKETS.KNOWN_CHAIN_HOLD] || 0,
    steward_possible_branded: counts[ROUTE_BUCKETS.POSSIBLE_BRANDED] || 0,
    weak_identity_hold: counts[ROUTE_BUCKETS.WEAK_IDENTITY] || 0,
    independent_total: independent.length,
    independent_missing_city: independent.length - withCity.length,
    independent_missing_website: independent.length - withWebsite.length,
    independent_likely_already_in_legacy_census: likelyExisting.length,
    independent_likely_new_vs_legacy: likelyNew.length,
    independent_high_quality: highQualityIndependent.length,
    independent_promote_ready_l1_proxy: independent.filter(
      (r) =>
        String(r.rawHotelName || "").trim() &&
        String(r.rawCountry || "").trim() &&
        String(r.rawWebsite || "").trim() &&
        (r.qualityScore ?? 0) >= 55 &&
        r.censusRecommendedAction !== "likely_existing"
    ).length,
  };

  return {
    version: BRAND_EXCLUSION_AUDIT_VERSION,
    generated_at: new Date().toISOString(),
    brand_setup_read_only: true,
    airtable_writes: false,
    active_brand_count: dictionary.active_brand_count,
    candidate_count: candidates.length,
    route_counts: counts,
    taxonomy,
    top_matched_brands: topBrands,
    rows,
    independent_sample: independent.slice(0, 40).map((r) => ({
      name: r.rawHotelName,
      city: r.rawCity,
      website: r.rawWebsite,
      quality: r.qualityScore,
      censusAction: r.censusRecommendedAction,
    })),
    promote_ready_sample: rows
      .filter(
        (r) =>
          r.independentLaneEligible &&
          String(r.rawWebsite || "").trim() &&
          (r.qualityScore ?? 0) >= 55 &&
          r.censusRecommendedAction !== "likely_existing"
      )
      .slice(0, 30)
      .map((r) => ({
        name: r.rawHotelName,
        city: r.rawCity,
        website: r.rawWebsite,
        quality: r.qualityScore,
      })),
  };
}
