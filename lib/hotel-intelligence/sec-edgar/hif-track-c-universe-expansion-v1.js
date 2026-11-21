/**
 * HIF Track C — US hotel universe expansion & global identity reconciliation.
 * HPC is the sole canonical hotel inventory. CUSTOMER_BLOCKED.
 * No operator inference from brand. No invented rooms/addresses.
 */

export const HIF_TRACK_C_UNIVERSE_EXPANSION_V1 = "hif-track-c-universe-expansion-v1";

export const TRACK_C_STANDING_AUTH = Object.freeze({
  stream: "TRACK_C",
  grantedAt: "2026-11-21",
  source: "FOUNDER_TRACK_C_OBJECTIVE_US_UNIVERSE_EXPANSION",
  customerBlocked: true,
  allowDiscoveryInventoryPersist: true,
  allowDiscoveryFindingsPersist: true,
  allowNonDestructiveHpcEnrichment: true,
  allowControlledHpcCreateViaAuthorizedPathway: true,
  maxHpcCreateBatch: 25,
  forbidOperatorInferenceFromBrand: true,
  forbidInventedRoomCounts: true,
  forbidInventedAddresses: true,
  preserveHistoricalBrandIdentities: true,
  preserveCalaRecords: true,
  worldwideEligibilityUnrestricted: true,
});

export const MATCH_CLASS = Object.freeze({
  MATCHED_EXISTING_HPC: "MATCHED_EXISTING_HPC",
  CREATE_CANDIDATE: "CREATE_CANDIDATE",
  CREATE_READY: "CREATE_READY",
  AMBIGUOUS: "AMBIGUOUS",
  UNMATCHED_VERIFIED: "UNMATCHED_VERIFIED",
  UNCERTAIN: "UNCERTAIN",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
});

export const HANDOFF_TRACK = Object.freeze({
  TRACK_A_COMMERCIAL: "TRACK_A_COMMERCIAL",
  TRACK_B_CONTRACTUAL: "TRACK_B_CONTRACTUAL",
  NONE: "NONE",
});

/** Priority country labels as stored on HPC Country. */
export const PRIORITY_COUNTRY_LABELS = Object.freeze({
  US: "United States",
  MX: "Mexico",
  BR: "Brazil",
  UK: "United Kingdom",
  CA: "Canada",
});

export const CALA_COUNTRY_LABELS = Object.freeze([
  "Mexico",
  "Brazil",
  "Argentina",
  "Colombia",
  "Costa Rica",
  "Peru",
  "Chile",
  "Dominican Republic",
  "Panama",
  "Ecuador",
  "Belize",
  "Jamaica",
  "Uruguay",
  "Cuba",
  "Puerto Rico",
  "Bolivia",
  "Guatemala",
  "Barbados",
  "Nicaragua",
  "Honduras",
  "El Salvador",
  "Trinidad and Tobago",
  "Bahamas",
  "Paraguay",
  "Venezuela",
  "Suriname",
  "Guyana",
  "Haiti",
  "Saint Lucia",
  "Antigua and Barbuda",
  "Dominica",
  "Grenada",
  "Saint Kitts and Nevis",
  "Saint Vincent and the Grenadines",
  "Aruba",
  "Curaçao",
  "Cayman Islands",
  "British Virgin Islands",
  "U.S. Virgin Islands",
  "Turks and Caicos Islands",
  "Martinique",
  "Guadeloupe",
  "French Guiana",
  "Anguilla",
  "Montserrat",
  "Sint Eustatius",
  "Bonaire, Sint Eustatius and Saba",
  "Saba",
  "Saint-Martin (French part)",
  "Sint Maarten (Dutch part)",
]);

export function normalizeHotelKey(name = "") {
  return String(name)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\b(the|hotel|inn|suites?|by\s+\w+)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeCountry(value = "") {
  const v = String(value || "").trim();
  if (!v) return "";
  const lower = v.toLowerCase();
  if (lower === "us" || lower === "usa" || lower === "u.s." || lower === "u.s.a.") {
    return PRIORITY_COUNTRY_LABELS.US;
  }
  if (lower === "uk" || lower === "gb" || lower === "great britain" || lower === "england") {
    return PRIORITY_COUNTRY_LABELS.UK;
  }
  if (lower === "ca" || lower === "can") return PRIORITY_COUNTRY_LABELS.CA;
  if (lower === "mx" || lower === "mex") return PRIORITY_COUNTRY_LABELS.MX;
  if (lower === "br" || lower === "bra") return PRIORITY_COUNTRY_LABELS.BR;
  return v;
}

export function isUsCountry(value = "") {
  return normalizeCountry(value) === PRIORITY_COUNTRY_LABELS.US;
}

export function isCalaCountry(value = "") {
  const n = normalizeCountry(value);
  return CALA_COUNTRY_LABELS.includes(n);
}

/**
 * Classify SEC/discovery identity vs live HPC collision outcome.
 */
export function classifySecToHpcMatch({
  collisionOutcome = null,
  identityClass = null,
  operatingStatus = null,
  hasExactAddress = false,
  hasOfficialBrandSource = false,
} = {}) {
  const outcome = String(collisionOutcome || "");
  if (outcome.includes("EXISTING_RECORD_VERIFIED") || outcome === "MATCHED") {
    return MATCH_CLASS.MATCHED_EXISTING_HPC;
  }
  if (outcome.includes("COLLISION") || outcome.includes("REQUIRES_REVIEW") || identityClass === "AMBIGUOUS_PROPERTY") {
    return MATCH_CLASS.AMBIGUOUS;
  }
  if (outcome.includes("LOOKUP_UNAVAILABLE")) return MATCH_CLASS.UNCERTAIN;
  if (String(operatingStatus || "").toUpperCase().includes("HISTORICAL") && !hasOfficialBrandSource) {
    return MATCH_CLASS.HISTORICAL_ONLY;
  }
  if (
    outcome.includes("NEW_RECORD") ||
    outcome.includes("VERIFIED_ABSENT") ||
    outcome === "ABSENT"
  ) {
    if (hasExactAddress && hasOfficialBrandSource) return MATCH_CLASS.CREATE_READY;
    if (identityClass === "VERIFIED_PROPERTY" || identityClass === "PROBABLE_PROPERTY") {
      return MATCH_CLASS.UNMATCHED_VERIFIED;
    }
    return MATCH_CLASS.CREATE_CANDIDATE;
  }
  if (identityClass === "VERIFIED_PROPERTY") return MATCH_CLASS.UNMATCHED_VERIFIED;
  return MATCH_CLASS.RESEARCH_REQUIRED;
}

/**
 * Commercial/contractual handoff routing — never invent opportunity classes.
 */
export function routeHandoff({
  hasFranchiseOrManagementSignal = false,
  hasContractualAgreementEvidence = false,
} = {}) {
  const handoffs = [];
  if (hasFranchiseOrManagementSignal) handoffs.push(HANDOFF_TRACK.TRACK_A_COMMERCIAL);
  if (hasContractualAgreementEvidence) handoffs.push(HANDOFF_TRACK.TRACK_B_CONTRACTUAL);
  if (!handoffs.length) handoffs.push(HANDOFF_TRACK.NONE);
  return handoffs;
}

/**
 * Build Discovery Inventory payload for Track C (CUSTOMER_BLOCKED).
 */
export function buildTrackCDiscoveryHotel(hotel = {}) {
  const country = normalizeCountry(hotel.country || hotel.physicalCountry || "");
  const discoveryPropertyId =
    hotel.discoveryPropertyId ||
    `disc_tc_${normalizeHotelKey(hotel.hotelName || hotel.name)
      .replace(/\s+/g, "_")
      .slice(0, 48)}_${(hotel.stateRegion || hotel.state || "xx").toString().toLowerCase().slice(0, 2)}`;

  return {
    discoveryPropertyId,
    hotelName: hotel.hotelName || hotel.name,
    hotelNameNormalized: normalizeHotelKey(hotel.hotelName || hotel.name),
    hotelNameRaw: hotel.hotelNameRaw || hotel.hotelName || hotel.name,
    city: hotel.city || null,
    stateProvince: hotel.stateRegion || hotel.state || hotel.stateProvince || null,
    physicalCountry: country || null,
    address: hotel.address || hotel.streetAddress || null,
    currentBrand: hotel.currentBrand || hotel.brand || null,
    historicalBrand: hotel.historicalBrand || null,
    brandFamily: hotel.brandFamily || null,
    identityClass: hotel.identityClass || "VERIFIED_PROPERTY",
    evidenceConfidence: hotel.evidenceConfidence || "HIGH",
    hpcMatchStatus: hotel.hpcMatchStatus || "DISCOVERED_NOT_IN_HPC",
    hpcRecordId: hotel.hpcRecordId || null,
    sourceUrl: hotel.sourceUrl || hotel.officialHotelBrandSource || hotel.historicalSecCmbsSource,
    discoveryMethod: hotel.discoveryMethod || "TRACK_C_UNIVERSE_EXPANSION",
    notes:
      hotel.notes ||
      `CUSTOMER_BLOCKED | Track C | match=${hotel.matchClass || "PENDING"} | operator_not_inferred_from_brand`,
    verified: hotel.verified !== false,
    evidenceSupported: Boolean(hotel.sourceUrl || hotel.officialHotelBrandSource || hotel.historicalSecCmbsSource),
    destination: "HI Hotel Discovery Inventory",
    operatorName: hotel.operatorName || null,
    roomCount: hotel.roomCount || null,
    operatingStatus: hotel.operatingStatus || hotel.currentOperationEvidence || null,
    matchClass: hotel.matchClass || null,
    handoffs: hotel.handoffs || [HANDOFF_TRACK.NONE],
  };
}

/**
 * Map CREATE_READY founder-review row → Track C discovery + HPC field payload.
 * Does not invent rooms or operators.
 */
export function mapCreateReadyRowToTrackC(row = {}) {
  const fields = row.intendedHpcFields || {};
  const street =
    fields["Street Address"] ||
    fields.Address ||
    (row.fullAddress ? String(row.fullAddress).split(",")[0].trim() : null);
  const hotel = buildTrackCDiscoveryHotel({
    discoveryPropertyId: `disc_tc_${row.candidateId || normalizeHotelKey(row.hotelName).replace(/\s+/g, "_")}`,
    hotelName: row.hotelName || fields["Property Name"],
    city: fields.City,
    stateRegion: fields["State / Region"],
    country: fields.Country || "United States",
    address: street,
    currentBrand: fields["Current Brand"] || row.brand,
    brandFamily: inferBrandFamilyFromLabel(fields["Current Brand"] || row.brand),
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    sourceUrl: row.officialHotelBrandSource || row.historicalSecCmbsSource,
    officialHotelBrandSource: row.officialHotelBrandSource,
    historicalSecCmbsSource: row.historicalSecCmbsSource,
    operatingStatus: row.currentOperationEvidence || "OPERATING",
    matchClass: MATCH_CLASS.CREATE_READY,
    discoveryMethod: "TRACK_C_SEC_CREATE_READY_PILOT",
    handoffs: routeHandoff({
      hasFranchiseOrManagementSignal: Boolean(row.relevantContractMilestone),
      hasContractualAgreementEvidence: Boolean(row.historicalSecCmbsSource),
    }),
    notes: `CUSTOMER_BLOCKED | Track C CREATE_READY | candidate=${row.candidateId || ""} | milestone=${row.relevantContractMilestone?.meaning || ""}`,
  });

  const hpcFields = {
    "Property Name": fields["Property Name"] || row.hotelName,
    "Canonical Property Name": fields["Property Name"] || row.hotelName,
    Country: normalizeCountry(fields.Country || "United States"),
    City: fields.City || null,
    "State / Region": fields["State / Region"] || null,
    Address: street || null,
    "Current Brand": fields["Current Brand"] || row.brand || null,
    "Affiliation Status": fields["Affiliation Status"] || "Branded",
    "Family / Source Family":
      fields["Family / Source Family"] && !/^[A-Z_]+$/.test(fields["Family / Source Family"])
        ? fields["Family / Source Family"]
        : inferFamilySelect(fields["Current Brand"] || row.brand),
  };
  if (row.officialHotelBrandSource) {
    hpcFields["Official Property URL"] = row.officialHotelBrandSource;
  }
  for (const k of Object.keys(hpcFields)) {
    if (hpcFields[k] == null || hpcFields[k] === "") delete hpcFields[k];
  }

  return {
    candidateId: row.candidateId,
    hotel,
    hpcFields,
    officialHotelBrandSource: row.officialHotelBrandSource || null,
    historicalSecCmbsSource: row.historicalSecCmbsSource || null,
    recommendation: row.recommendation || "CREATE",
  };
}

export function inferBrandFamilyFromLabel(brand = "") {
  const b = String(brand || "").toLowerCase();
  if (!b) return "UNKNOWN";
  if (/marriott|courtyard|residence inn|fairfield|springhill|ac hotel|delta hotel|autograph|tribute|moxy|aloft|element|towneplace|protea/.test(b)) {
    return "MARRIOTT";
  }
  if (/hilton|hampton|garden inn|embassy|homewood|home2|tru |canopy|curio|tempo|spark by hilton/.test(b)) {
    return "HILTON";
  }
  if (/hyatt|andaz|thompson|unbound/.test(b)) return "HYATT";
  if (/ihg|holiday inn|crowne|candlewood|staybridge|voco|intercontinental|kimpton|avid /.test(b)) return "IHG";
  if (/wyndham|ramada|days inn|super 8|la quinta|wingate|trademark|dolce/.test(b)) return "WYNDHAM";
  if (/choice|comfort|quality|clarion|econo|sleep inn|ascend|cambria|radisson/.test(b)) return "CHOICE";
  if (/accor|mercure|novotel|ibis|sofitel|pullman|fairmont|raffles/.test(b)) return "ACCOR";
  return "OTHER";
}

export function inferFamilySelect(brand = "") {
  const family = inferBrandFamilyFromLabel(brand);
  // HPC "Family / Source Family" select options (Title Case — not ALLCAPS enums)
  const map = {
    MARRIOTT: "Marriott",
    HILTON: "Hilton",
    HYATT: "Hyatt",
    IHG: "IHG",
    WYNDHAM: "Wyndham",
    CHOICE: "Choice",
    ACCOR: "Accor",
  };
  return map[family] || "Other";
}

/**
 * Aggregate HPC audit rows into country / state / city / brand / operator / status.
 */
export function aggregateHpcAudit(records = []) {
  const byCountry = new Map();
  const byState = new Map();
  const byCity = new Map();
  const byBrand = new Map();
  const byOperator = new Map();
  const byStatus = new Map();
  let verifiedIdentity = 0;
  let unresolvedIdentity = 0;

  for (const r of records) {
    const f = r.fields || r;
    const country = f.Country || "(empty)";
    const state = f["State / Region"] || "(empty)";
    const city = f.City || "(empty)";
    const brand = f["Current Brand"] || "(empty)";
    const operator =
      f["Operator / Management Company"] || f["Management Company"] || "(empty)";
    const status = f["Production Use Status"] || f["Enrichment Status"] || "(empty)";
    const name = f["Property Name"] || f["Canonical Property Name"] || "";

    byCountry.set(country, (byCountry.get(country) || 0) + 1);
    byState.set(`${country}|${state}`, (byState.get(`${country}|${state}`) || 0) + 1);
    byCity.set(`${country}|${city}`, (byCity.get(`${country}|${city}`) || 0) + 1);
    byBrand.set(brand, (byBrand.get(brand) || 0) + 1);
    byOperator.set(operator, (byOperator.get(operator) || 0) + 1);
    byStatus.set(status, (byStatus.get(status) || 0) + 1);

    if (name && country && country !== "(empty)") verifiedIdentity += 1;
    else unresolvedIdentity += 1;
  }

  const sortMap = (m) => [...m.entries()].sort((a, b) => b[1] - a[1]);

  return {
    scanned: records.length,
    verifiedIdentity,
    unresolvedIdentity,
    byCountry: sortMap(byCountry),
    byStateTop100: sortMap(byState).slice(0, 100),
    byCityTop100: sortMap(byCity).slice(0, 100),
    byBrandTop100: sortMap(byBrand).slice(0, 100),
    byOperatorTop50: sortMap(byOperator).slice(0, 50),
    byStatus: sortMap(byStatus),
    priorityCountries: {
      US: byCountry.get(PRIORITY_COUNTRY_LABELS.US) || 0,
      Mexico: byCountry.get(PRIORITY_COUNTRY_LABELS.MX) || 0,
      Brazil: byCountry.get(PRIORITY_COUNTRY_LABELS.BR) || 0,
      UK: byCountry.get(PRIORITY_COUNTRY_LABELS.UK) || 0,
      Canada: byCountry.get(PRIORITY_COUNTRY_LABELS.CA) || 0,
    },
    calaTotal: [...byCountry.entries()]
      .filter(([c]) => isCalaCountry(c))
      .reduce((n, [, v]) => n + v, 0),
  };
}

/**
 * Deduplicate discovered hotels by normalized name + city + state + country.
 */
export function dedupeDiscoveries(hotels = []) {
  const seen = new Map();
  const out = [];
  for (const h of hotels) {
    const key = [
      normalizeHotelKey(h.hotelName || h.name || ""),
      String(h.city || "").toLowerCase().trim(),
      String(h.stateRegion || h.state || h.stateProvince || "")
        .toLowerCase()
        .trim(),
      normalizeCountry(h.country || h.physicalCountry || ""),
    ].join("|");
    if (!key.replace(/\|/g, "")) continue;
    if (seen.has(key)) continue;
    seen.set(key, true);
    out.push(h);
  }
  return out;
}

/**
 * Scorecard helpers for SEC→HPC reconciliation.
 */
export function summarizeMatchClasses(rows = []) {
  const counts = Object.fromEntries(Object.values(MATCH_CLASS).map((k) => [k, 0]));
  for (const r of rows) {
    const c = r.matchClass || MATCH_CLASS.UNCERTAIN;
    counts[c] = (counts[c] || 0) + 1;
  }
  const total = rows.length || 1;
  const matched = counts[MATCH_CLASS.MATCHED_EXISTING_HPC] || 0;
  return {
    counts,
    distinctHotels: rows.length,
    matchedExistingHpc: matched,
    unmatchedVerified:
      (counts[MATCH_CLASS.UNMATCHED_VERIFIED] || 0) +
      (counts[MATCH_CLASS.CREATE_READY] || 0) +
      (counts[MATCH_CLASS.CREATE_CANDIDATE] || 0),
    uncertain:
      (counts[MATCH_CLASS.UNCERTAIN] || 0) +
      (counts[MATCH_CLASS.AMBIGUOUS] || 0) +
      (counts[MATCH_CLASS.RESEARCH_REQUIRED] || 0),
    matchRate: Number((matched / total).toFixed(4)),
  };
}

/**
 * Aimbridge public portfolio seed (operator disclosed; addresses require corroboration).
 * Operator is disclosed by Aimbridge — not inferred from brand.
 */
export const AIMBRIDGE_PUBLIC_US_PORTFOLIO_SEED = Object.freeze([
  {
    hotelName: "Residence Inn Scottsdale North",
    city: "Scottsdale",
    stateRegion: "AZ",
    country: "United States",
    currentBrand: "Residence Inn",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://context.aimbridgehospitality.com/llms.txt",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
  {
    hotelName: "Hyatt Place Mystic",
    city: "Mystic",
    stateRegion: "CT",
    country: "United States",
    currentBrand: "Hyatt Place",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://context.aimbridgehospitality.com/llms.txt",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
  {
    hotelName: "Courtyard Oceanside Fort Lauderdale Beach",
    city: "Fort Lauderdale",
    stateRegion: "FL",
    country: "United States",
    currentBrand: "Courtyard",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://context.aimbridgehospitality.com/llms.txt",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
  {
    hotelName: "AC Hotel Atlanta Downtown",
    city: "Atlanta",
    stateRegion: "GA",
    country: "United States",
    currentBrand: "AC Hotels",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://context.aimbridgehospitality.com/llms.txt",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
  {
    hotelName: "Hilton New York Fashion District",
    city: "New York",
    stateRegion: "NY",
    country: "United States",
    currentBrand: "Hilton",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://context.aimbridgehospitality.com/llms.txt",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
  {
    hotelName: "voco Sandpiper All-Inclusive Resort",
    city: "Port St. Lucie",
    stateRegion: "FL",
    country: "United States",
    currentBrand: "voco",
    operatorName: "Aimbridge Hospitality",
    roomCount: 335,
    sourceUrl:
      "https://www.aimbridgehospitality.com/news/aimbridge-welcomes-voco-sandpiper-all-inclusive-resort-to-its-expanding-portfolio/",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    notes: "CUSTOMER_BLOCKED | Track C | rooms from Aimbridge/IHG press (335-key)",
  },
  {
    hotelName: "Delta Hotels Orlando Celebration",
    city: "Kissimmee",
    stateRegion: "FL",
    country: "United States",
    currentBrand: "Delta Hotels",
    operatorName: "Aimbridge Hospitality",
    roomCount: 718,
    sourceUrl:
      "https://www.aimbridgehospitality.com/news/aimbridge-hospitality-strengthens-portfolio-with-718-key-delta-marriott-orlando-celebration/",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    notes: "CUSTOMER_BLOCKED | Track C | rooms from Aimbridge press (718-key); franchise through 2042",
  },
  {
    hotelName: "AC Hotel Dallas Downtown",
    city: "Dallas",
    stateRegion: "TX",
    country: "United States",
    currentBrand: "AC Hotels",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://context.aimbridgehospitality.com/llms.txt",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
]);

/** UK Aimbridge EMEA public additions (Lodging Magazine 2025) — names only until address corroboration. */
export const AIMBRIDGE_PUBLIC_UK_PORTFOLIO_SEED = Object.freeze([
  {
    hotelName: "Crowne Plaza Newcastle",
    city: "Newcastle",
    country: "United Kingdom",
    currentBrand: "Crowne Plaza",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://lodgingmagazine.com/aimbridge-hospitality-expands-portfolio-implements-company-updates/",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
  {
    hotelName: "Hilton Garden Inn Birmingham Airport",
    city: "Birmingham",
    country: "United Kingdom",
    currentBrand: "Hilton Garden Inn",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://lodgingmagazine.com/aimbridge-hospitality-expands-portfolio-implements-company-updates/",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
  {
    hotelName: "Mercure Cardiff",
    city: "Cardiff",
    country: "United Kingdom",
    currentBrand: "Mercure",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://lodgingmagazine.com/aimbridge-hospitality-expands-portfolio-implements-company-updates/",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
  {
    hotelName: "Mercure Nottingham",
    city: "Nottingham",
    country: "United Kingdom",
    currentBrand: "Mercure",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://lodgingmagazine.com/aimbridge-hospitality-expands-portfolio-implements-company-updates/",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
  },
]);

export function buildCalaPreservationCheck(priorityCountries = {}, calaTotal = 0, priorCalaTotal = null) {
  return {
    calaTotal,
    priorCalaTotal,
    preserved:
      priorCalaTotal == null ? true : calaTotal >= priorCalaTotal,
    mexico: priorityCountries.Mexico ?? null,
    brazil: priorityCountries.Brazil ?? null,
    note: "Track C must not delete or overwrite CALA HPC records",
  };
}
