/**
 * ADP_ATTRIBUTE_SEMANTIC_CONFLICT_V1
 * Fail-closed semantic consistency checks for Reality Gap attribute inventories.
 * Distinguishes verified facts from inferred / conflicting / leakage.
 */

export const ADP_ATTRIBUTE_SEMANTIC_CONFLICT_V1 =
  "ADP_ATTRIBUTE_SEMANTIC_CONFLICT_V1";

/** Mutually exclusive pairs — both present → conflict */
export const MUTUALLY_EXCLUSIVE_ATTRIBUTE_PAIRS_V1 = Object.freeze([
  Object.freeze(["full_service", "select_service"]),
  Object.freeze(["hilton_honors", "marriott_bonvoy"]),
  Object.freeze(["hilton_honors", "hyatt_world_of_hyatt"]),
  Object.freeze(["marriott_bonvoy", "hyatt_world_of_hyatt"]),
  Object.freeze(["hilton_honors", "wyndham_rewards"]),
  Object.freeze(["marriott_bonvoy", "wyndham_rewards"]),
  Object.freeze(["hyatt_world_of_hyatt", "wyndham_rewards"]),
  Object.freeze(["sheraton", "luxury_collection"]),
  Object.freeze(["sheraton", "st_regis"]),
  Object.freeze(["sheraton", "jw_marriott"]),
  Object.freeze(["curio_collection", "marriott_bonvoy"]),
  Object.freeze(["curio_collection", "autograph"]),
  Object.freeze(["luxury_collection", "hilton_honors"]),
  Object.freeze(["luxury_collection", "curio_collection"]),
]);

/** Brand → allowed loyalty / soft-brand attributes */
export const BRAND_LOYALTY_ALLOWLIST_V1 = Object.freeze({
  "Autograph Collection": Object.freeze(["marriott_bonvoy", "soft_brand"]),
  Sheraton: Object.freeze(["marriott_bonvoy", "sheraton"]),
  "Luxury Collection": Object.freeze(["marriott_bonvoy", "luxury_collection"]),
  Westin: Object.freeze(["marriott_bonvoy", "westin", "heavenly_spa", "westin_club"]),
  "JW Marriott": Object.freeze(["marriott_bonvoy", "jw_marriott"]),
  "St. Regis": Object.freeze(["marriott_bonvoy", "st_regis"]),
  "AC Hotel": Object.freeze(["marriott_bonvoy"]),
  Renaissance: Object.freeze(["marriott_bonvoy"]),
  Marriott: Object.freeze(["marriott_bonvoy", "marriott_hotels", "m_club"]),
  "W Hotels": Object.freeze(["marriott_bonvoy"]),
  Curio: Object.freeze(["hilton_honors", "curio_collection", "soft_brand"]),
  "Curio Collection": Object.freeze(["hilton_honors", "curio_collection", "soft_brand"]),
  Hilton: Object.freeze(["hilton_honors"]),
  Hyatt: Object.freeze(["hyatt_world_of_hyatt"]),
  YOTEL: Object.freeze(["yotel_brand", "select_service", "cabin_rooms"]),
  Wyndham: Object.freeze(["wyndham_rewards"]),
  Nobu: Object.freeze(["nobu_brand", "nobu_restaurant"]),
});

/**
 * Build a text blob for enrichment WITHOUT confusable exclusions
 * (those describe other hotels and must not seed attributes).
 */
export function buildAttributeEnrichmentBlob(profile) {
  const exclude = new Set(
    (profile?.identityConfusableExclusions || []).map((s) =>
      String(s || "").toLowerCase()
    )
  );
  const parts = [
    profile?.name,
    profile?.displayName,
    profile?.brand,
    profile?.affiliation,
    profile?.parentCompany,
    profile?.portfolioLoyaltyIdentity,
    profile?.propertyType,
    profile?.chainScale,
    profile?.positioning?.primary,
    ...(profile?.positioning?.differentiators || []),
    profile?.locationContextNote,
    ...(profile?.identityAliases || []),
    ...(profile?.commonShorthand || []),
    ...(profile?.attributes || []),
  ]
    .filter(Boolean)
    .map((s) => String(s));
  // Drop any part that is exactly a confusable exclusion string
  const filtered = parts.filter((p) => !exclude.has(p.toLowerCase()));
  return filtered.join(" ").toLowerCase();
}

/**
 * @param {{ propertyId: string, profile: object, attributes?: string[] }} input
 * @returns {{ conflicts: object[], warnings: object[], pass: boolean }}
 */
export function detectAttributeSemanticConflictsV1(input) {
  const profile = input.profile || {};
  const attrs = [
    ...new Set(
      (input.attributes || profile.attributes || []).map((a) => String(a || "").trim())
    ),
  ].filter(Boolean);
  const attrSet = new Set(attrs);
  const conflicts = [];
  const warnings = [];
  const brand = String(profile.brand || "").trim();
  const affiliation = String(profile.affiliation || "").toLowerCase();
  const parent = String(profile.parentCompany || "").toLowerCase();

  for (const [a, b] of MUTUALLY_EXCLUSIVE_ATTRIBUTE_PAIRS_V1) {
    if (attrSet.has(a) && attrSet.has(b)) {
      conflicts.push({
        type: "MUTUALLY_EXCLUSIVE_ATTRIBUTES",
        severity: "HIGH",
        attributes: [a, b],
        detail: `Both ${a} and ${b} present`,
      });
    }
  }

  // Hilton Honors on non-Hilton properties
  if (attrSet.has("hilton_honors")) {
    const hiltonFamily =
      /hilton|curio|embassy suites|garden inn|hampton|doubletree|waldorf/i.test(
        `${brand} ${affiliation} ${parent}`
      );
    if (!hiltonFamily) {
      conflicts.push({
        type: "INCORRECT_LOYALTY_AFFILIATION",
        severity: "HIGH",
        attributes: ["hilton_honors"],
        detail: `hilton_honors present but brand/affiliation is not Hilton family (brand=${brand || "—"})`,
      });
    }
  }

  // Marriott Bonvoy on non-Marriott
  if (attrSet.has("marriott_bonvoy")) {
    const marriottFamily =
      /marriott|bonvoy|sheraton|westin|renaissance|autograph|st\.?\s*regis|jw |w hotels|ac hotel|luxury collection|tribute|design hotels/i.test(
        `${brand} ${affiliation} ${parent}`
      );
    if (!marriottFamily) {
      conflicts.push({
        type: "INCORRECT_LOYALTY_AFFILIATION",
        severity: "HIGH",
        attributes: ["marriott_bonvoy"],
        detail: `marriott_bonvoy present but brand/affiliation is not Marriott family (brand=${brand || "—"})`,
      });
    }
  }

  // Hyatt loyalty on non-Hyatt
  if (attrSet.has("hyatt_world_of_hyatt")) {
    const hyattFamily = /hyatt|world of hyatt/i.test(
      `${brand} ${affiliation} ${parent}`
    );
    if (!hyattFamily) {
      conflicts.push({
        type: "INCORRECT_LOYALTY_AFFILIATION",
        severity: "HIGH",
        attributes: ["hyatt_world_of_hyatt"],
        detail: `hyatt_world_of_hyatt present but brand/affiliation is not Hyatt (brand=${brand || "—"})`,
      });
    }
  }

  // Luxury Collection brand attribute only for Luxury Collection properties
  if (attrSet.has("luxury_collection")) {
    const isLc =
      /luxury collection/i.test(brand) ||
      /luxury collection/i.test(String(profile.name || ""));
    if (!isLc) {
      conflicts.push({
        type: "INCORRECT_BRAND_AFFILIATION",
        severity: "HIGH",
        attributes: ["luxury_collection"],
        detail: `luxury_collection attribute on non–Luxury Collection property (brand=${brand || "—"})`,
      });
    }
  }

  // Confusable exclusion leakage into attributes (heuristic)
  const confusables = profile.identityConfusableExclusions || [];
  for (const c of confusables) {
    const cl = String(c || "").toLowerCase();
    if (/luxury collection/.test(cl) && attrSet.has("luxury_collection") && !/luxury collection/i.test(brand)) {
      conflicts.push({
        type: "CROSS_HOTEL_ATTRIBUTE_LEAKAGE",
        severity: "HIGH",
        attributes: ["luxury_collection"],
        detail: `luxury_collection likely leaked from identityConfusableExclusions: "${c}"`,
      });
    }
    if (/embassy/.test(cl) && attrSet.has("hilton_honors") && !/hilton|curio/i.test(brand)) {
      warnings.push({
        type: "POSSIBLE_TEXT_FALSE_POSITIVE",
        severity: "MEDIUM",
        attributes: ["hilton_honors"],
        detail: `Confusable text mentions Embassy; hilton_honors may be regex false positive`,
      });
    }
  }

  // Service class vs propertyType
  if (attrSet.has("full_service") && /select.?service|limited.?service|cabin|hostel|sleeper/i.test(String(profile.propertyType || ""))) {
    conflicts.push({
      type: "SERVICE_CLASSIFICATION_CONFLICT",
      severity: "HIGH",
      attributes: ["full_service"],
      detail: `full_service conflicts with propertyType=${profile.propertyType}`,
    });
  }

  // Synonym duplication (warning only — collapse handles coverage)
  const synonymGroups = [
    ["meeting_space", "meetings", "meeting_rooms"],
    ["spa", "full_service_spa"],
    ["fitness_center", "fitness_center_24hr", "wellness_fitness", "wellness"],
    ["upscale", "upper_upscale"],
    ["ballroom", "large_ballroom"],
    ["on_site_restaurant", "restaurant"],
  ];
  for (const group of synonymGroups) {
    const present = group.filter((id) => attrSet.has(id));
    if (present.length > 2) {
      warnings.push({
        type: "SYNONYM_DUPLICATION",
        severity: "LOW",
        attributes: present,
        detail: `Multiple synonym attributes present (coverage collapse keeps first)`,
      });
    }
  }

  return {
    gate: ADP_ATTRIBUTE_SEMANTIC_CONFLICT_V1,
    propertyId: input.propertyId || profile.propertyId || null,
    attributeCount: attrs.length,
    conflicts,
    warnings,
    pass: conflicts.length === 0,
  };
}

/**
 * Verified corrections for known P0 anomalies (source-backed).
 * Does not invent attributes — only removes false positives / adds verified loyalty when defined.
 */
export const VERIFIED_ATTRIBUTE_CORRECTIONS_V1 = Object.freeze([
  {
    propertyId: "adp_mayflower_hotel_washington_dc",
    remove: Object.freeze(["hilton_honors"]),
    add: Object.freeze([]),
    reason:
      "Autograph Collection / Marriott Bonvoy property. hilton_honors was a false positive from 'Embassy' in locationContextNote matching enrichment regex /embassy/.",
    sources: Object.freeze([
      "https://www.marriott.com/en-us/hotels/wasak-the-mayflower-hotel-autograph-collection/overview/",
    ]),
    factState: "VERIFIED_ABSENT_FOR_HILTON_HONORS",
  },
  {
    propertyId: "adp_sheraton_mallorca_arabella_golf",
    remove: Object.freeze(["luxury_collection"]),
    add: Object.freeze([]),
    reason:
      "Sheraton brand (PMISI), not Luxury Collection. luxury_collection leaked from identityConfusableExclusions naming Castillo Hotel Son Vida, a Luxury Collection Hotel.",
    sources: Object.freeze([
      "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
    ]),
    factState: "VERIFIED_ABSENT_FOR_LUXURY_COLLECTION",
  },
  {
    propertyId: "adp_yotel_geneva_lake",
    remove: Object.freeze(["full_service"]),
    add: Object.freeze([]),
    reason:
      "propertyType=select_service; YOTEL cabin-product positioning. full_service + select_service mutually exclusive. Keep select_service + cabin_rooms.",
    sources: Object.freeze([
      "https://www.yotel.com/en/hotels/yotel-geneva-lake",
      "https://helpcenter.yotel.com/hc/en-gb/articles/6791172278685-What-amenities-do-YOTEL-rooms-have",
    ]),
    factState: "VERIFIED_SELECT_SERVICE",
  },
  {
    propertyId: "adp_now_now_noho",
    remove: Object.freeze(["hyatt_world_of_hyatt", "full_service"]),
    add: Object.freeze(["select_service"]),
    reason:
      "Public first-party SoT (staynownow.com / Dovetail + Co) describes an independent sleeper-cabin boutique hostel — not World of Hyatt and not full-service. Remove unsupported Hyatt loyalty + full_service; add select_service. Canonical brand/affiliation remapped to Independent / Dovetail + Co under ADP_REALITY_GAP_FINAL_CERTIFICATION_V2 (management ≠ loyalty).",
    sources: Object.freeze([
      "https://www.staynownow.com/",
      "https://www.hospitalitynet.org/announcement/41012378/now-now-noho-opens-on-new-yorks-historic-bowery",
      "https://www.dovetailandco.com/now-now-noho",
    ]),
    factState: "VERIFIED_ABSENT_HYATT_LOYALTY_AND_FULL_SERVICE",
    identityFollowUp: false,
    identityRemappedV2: true,
  },
  {
    propertyId: "adp_wyndham_san_diego_bayside",
    remove: Object.freeze([]),
    add: Object.freeze(["wyndham_rewards", "concierge"]),
    reason:
      "Official Wyndham San Diego Bayside earns Wyndham Rewards; concierge listed on property amenities. Recognition sparsity is Core-keyword/depth limitation — not removal of verified amenities.",
    sources: Object.freeze([
      "https://www.wyndhamhotels.com/wyndham/san-diego-california/wyndham-san-diego-bayside/services-amenities",
      "https://www.wyndhamsandiegobay.com/about-us",
      "https://www.wyndhamsandiegobay.com/special-offers",
    ]),
    factState: "VERIFIED_PRESENT_WYNDHAM_REWARDS",
  },
]);
