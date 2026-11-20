/**
 * V4 US HPC pilot selection — deterministic criteria + scoring.
 * Does not create HPC hotels. Does not force CREATE_READY to hit a count target.
 */

export const CONTRACT_TIMING_V4_US_PILOT_SELECTION_V1 =
  "contract-timing-v4-us-pilot-selection-v1";

export const PILOT_DISPOSITION = Object.freeze({
  LINK_EXISTING: "LINK_EXISTING",
  CREATE_READY: "CREATE_READY",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
  REJECT_AMBIGUOUS: "REJECT_AMBIGUOUS",
});

export const US_STATE_CODES = Object.freeze(
  new Set([
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC", "PR",
    "VI", "GU", "AS", "MP",
  ]),
);

const HOTEL_BRAND_HINT =
  /\b(marriott|hilton|hyatt|ihg|holiday\s*inn|hampton|courtyard|residence\s*inn|springhill|fairfield|sheraton|westin|w\s+hotel|renaissance|autograph|tribute|curio|doubletree|embassy|homewood|home2|hilton\s+garden|tru\s+by|canopy|tempo|spark|motto|signia|waldorf|conrad|intercontinental|crowne\s*plaza|hotel\s+indigo|kimpton|voco|even\s+hotels|avid|candlewood|staybridge|choice|comfort|quality|sleep\s+inn|cambria|ascend|radisson|best\s+western|wyndham|la\s+quinta|days\s+inn|super\s+8|ramada|motel\s+6|extended\s+stay|ac\s+hotel|aloft|element|moxy|edition|st\s*\.?\s*regis|ritz|four\s+points|le\s+m[eé]ridien|delta\s+hotels|towneplace|protea)\b/i;

const NON_HOTEL_REJECT =
  /\b(rv\s+resort|manufactured\s+housing|mobile\s+home|self[\s-]?storage|office\s+building|shopping\s+center|industrial|warehouse|apartments?\b|multifamily|student\s+housing)\b/i;

const GENERIC_NAME =
  /^(the\s+)?(hotel|inn|motel|resort|property|collateral|mortgaged\s+property)\b/i;

/** Street / parcel labels mistaken for hotel names in some CMBS columns. */
const STREET_ADDRESS_NAME = /^\d{1,6}\s+\w+/;

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&ndash;/g, "-")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function propertyKey(f) {
  return [norm(f.hotelName), norm(f.city), norm(f.stateRegion || f.state || "")].join("|");
}

/**
 * US detection: explicit US country, or US/territory state code, or snippet ZIP pattern with state.
 */
export function isUnitedStatesHotel(f) {
  const country = String(f.country || f.source?.country || "").toLowerCase();
  if (country && /united states|usa|u\.s\.a|u\.s\b/.test(country)) return true;
  if (country && country.length > 1 && !/united states|usa|u\.s/.test(country)) return false;
  const st = String(f.stateRegion || f.state || "")
    .trim()
    .toUpperCase()
    .replace(/\./g, "");
  if (US_STATE_CODES.has(st)) return true;
  if (/^[A-Z]{2}$/.test(st) && !US_STATE_CODES.has(st)) return false;
  // Snippet often has "| City | ST | ZIP |"
  const snip = String(f.snippet || "");
  const m = snip.match(/\|\s*([A-Z]{2})\s*\|\s*\d{5}\b/);
  if (m && US_STATE_CODES.has(m[1])) return true;
  return false;
}

export function extractAddressHint(f) {
  const snip = String(f.snippet || "");
  // Common CMBS: Name | address | City | ST | ZIP
  const parts = snip.split("|").map((p) => p.trim());
  for (let i = 0; i < parts.length - 2; i++) {
    const maybeAddr = parts[i];
    const maybeCity = parts[i + 1];
    const maybeSt = parts[i + 2];
    if (
      /\d/.test(maybeAddr) &&
      /[A-Za-z]/.test(maybeAddr) &&
      maybeAddr.length >= 8 &&
      maybeAddr.length <= 80 &&
      String(maybeCity).toLowerCase() === String(f.city || "").toLowerCase() &&
      US_STATE_CODES.has(String(maybeSt).toUpperCase())
    ) {
      return maybeAddr.replace(/&amp;/g, "&");
    }
  }
  if (f.address) return String(f.address);
  return null;
}

/**
 * Score components (0–100 total). Deterministic; higher = better pilot fitness.
 */
export function scoreUsPilotCandidate(hotelAgg) {
  const reasons = [];
  const penalties = [];
  let score = 0;
  const name = String(hotelAgg.hotelName || "");
  const city = String(hotelAgg.city || "");
  const state = String(hotelAgg.stateRegion || "");
  const blob = `${name} ${city} ${hotelAgg.snippetSample || ""}`;

  // Identity (max 30)
  if (name.length >= 8 && city) {
    score += 12;
    reasons.push("name_and_city");
  }
  if (state && US_STATE_CODES.has(state.toUpperCase())) {
    score += 8;
    reasons.push("us_state");
  }
  const brandFromFields =
    HOTEL_BRAND_HINT.test(String(hotelAgg.brandLabel || "")) ||
    (hotelAgg.brandFamily &&
      hotelAgg.brandFamily !== "UNKNOWN" &&
      hotelAgg.brandFamily !== "OTHER");
  // Prefer structured brand fields; name-only brand match must include hotel lexeme
  // to avoid "Hampton" substring in street addresses.
  const brandFromName =
    HOTEL_BRAND_HINT.test(name) && /\b(hotel|inn|suites|resort|lodge|motel)\b/i.test(name);
  if (brandFromFields || brandFromName) {
    score += 10;
    reasons.push("brand_hint");
  } else if (/\b(hotel|inn|suites|resort|lodge)\b/i.test(name)) {
    score += 5;
    reasons.push("hotel_lexeme");
  }

  // Geography distinctiveness (max 20)
  if (hotelAgg.addressHint) {
    score += 14;
    reasons.push("address_from_evidence");
  } else if (city && state) {
    score += 6;
    reasons.push("city_state_only");
  }
  if (city.length >= 3 && !/^(various|n\/a|multiple|portfolio)$/i.test(city)) {
    score += 4;
    reasons.push("distinctive_city");
  } else {
    penalties.push("weak_city");
  }

  // Contract milestone (max 20)
  if (hotelAgg.milestoneCount >= 1) {
    score += 12;
    reasons.push("has_contract_milestone");
  }
  if (hotelAgg.futureMilestoneCount >= 1) {
    score += 8;
    reasons.push("future_milestone_2027_2040");
  } else if (hotelAgg.recentishMilestoneCount >= 1) {
    score += 3;
    reasons.push("post_2018_milestone");
  }

  // Alias / brand history (max 10)
  if (hotelAgg.brandFamily && hotelAgg.brandFamily !== "UNKNOWN") {
    score += 5;
    reasons.push("brand_family");
  }
  if (hotelAgg.aliasHints?.length) {
    score += 5;
    reasons.push("alias_evidence");
  }

  // Ownership / management (max 10)
  if (hotelAgg.hasCounterparty) {
    score += 6;
    reasons.push("counterparty");
  }
  if (hotelAgg.hasOwnerOrManagerMention) {
    score += 4;
    reasons.push("owner_manager_mention");
  }

  // HE enrichment potential (max 10)
  if (HOTEL_BRAND_HINT.test(name) && hotelAgg.addressHint && hotelAgg.milestoneCount >= 1) {
    score += 10;
    reasons.push("he_enrichment_potential");
  } else if (HOTEL_BRAND_HINT.test(name) && city && state) {
    score += 5;
    reasons.push("he_enrichment_partial");
  }

  // Hard penalties
  if (NON_HOTEL_REJECT.test(blob)) {
    score -= 40;
    penalties.push("non_hotel_asset_class");
  }
  if (GENERIC_NAME.test(name) || name.length < 6) {
    score -= 25;
    penalties.push("generic_or_short_name");
  }
  if (STREET_ADDRESS_NAME.test(name.trim())) {
    score -= 45;
    penalties.push("street_address_as_name");
  }
  if (hotelAgg.portfolioWide) {
    score -= 30;
    penalties.push("portfolio_wide");
  }
  if (hotelAgg.loanOnly) {
    score -= 40;
    penalties.push("loan_maturity_only");
  }
  if (!hotelAgg.hotelAttributed) {
    score -= 20;
    penalties.push("not_hotel_attributed");
  }

  score = Math.max(0, Math.min(100, score));
  return { score, reasons, penalties };
}

/**
 * Pre-HPC disposition from evidence quality only.
 */
export function classifyPreHpc(hotelAgg, scored) {
  if (
    scored.penalties.includes("non_hotel_asset_class") ||
    scored.penalties.includes("generic_or_short_name") ||
    scored.penalties.includes("street_address_as_name") ||
    scored.penalties.includes("portfolio_wide") ||
    scored.penalties.includes("loan_maturity_only") ||
    scored.score < 25
  ) {
    return PILOT_DISPOSITION.REJECT_AMBIGUOUS;
  }
  const strongIdentity =
    scored.reasons.includes("name_and_city") &&
    scored.reasons.includes("us_state") &&
    scored.reasons.includes("has_contract_milestone") &&
    (scored.reasons.includes("brand_hint") || scored.reasons.includes("hotel_lexeme"));
  const strongGeo = scored.reasons.includes("address_from_evidence");
  if (strongIdentity && strongGeo && scored.score >= 55) {
    // CREATE_READY only after live collision confirms absence
    return "CREATE_READY_PENDING_COLLISION";
  }
  if (strongIdentity && scored.score >= 40) {
    return PILOT_DISPOSITION.RESEARCH_REQUIRED;
  }
  return PILOT_DISPOSITION.RESEARCH_REQUIRED;
}

/**
 * Map live collision outcome → final disposition. Never invent CREATE_READY.
 */
export function classifyAfterCollision(preDisposition, collision) {
  if (preDisposition === PILOT_DISPOSITION.REJECT_AMBIGUOUS) {
    return PILOT_DISPOSITION.REJECT_AMBIGUOUS;
  }
  const outcome = collision?.outcome || collision?.liveOutcome || null;
  if (!outcome || outcome === "HPC_LIVE_LOOKUP_UNAVAILABLE") {
    return PILOT_DISPOSITION.RESEARCH_REQUIRED;
  }
  if (outcome === "HPC_EXISTING_RECORD_VERIFIED") {
    return PILOT_DISPOSITION.LINK_EXISTING;
  }
  if (outcome === "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW") {
    return PILOT_DISPOSITION.REJECT_AMBIGUOUS;
  }
  if (outcome === "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT") {
    if (preDisposition === "CREATE_READY_PENDING_COLLISION") {
      return PILOT_DISPOSITION.CREATE_READY;
    }
    // Evidence not strong enough even if absent from HPC
    return PILOT_DISPOSITION.RESEARCH_REQUIRED;
  }
  return PILOT_DISPOSITION.RESEARCH_REQUIRED;
}

function yearOf(d) {
  const y = Number(String(d || "").slice(0, 4));
  return Number.isFinite(y) ? y : null;
}

/**
 * Aggregate finding rows into hotel-level candidates.
 */
export function aggregateFindingsToHotels(findings) {
  const map = new Map();
  for (const f of findings || []) {
    if (!f?.hotelName) continue;
    if (!isUnitedStatesHotel(f)) continue;
    const key = propertyKey(f);
    if (!map.has(key)) {
      map.set(key, {
        propertyKey: key,
        hotelName: f.hotelName,
        city: f.city || null,
        stateRegion: f.stateRegion || f.state || null,
        country: "United States",
        brandFamily: f.brandFamily || null,
        brandLabel: f.brandLabel || null,
        milestoneCount: 0,
        futureMilestoneCount: 0,
        recentishMilestoneCount: 0,
        findingIds: [],
        accessions: new Set(),
        waves: new Set(),
        addressHint: null,
        snippetSample: f.snippet || "",
        hasCounterparty: false,
        hasOwnerOrManagerMention: false,
        aliasHints: [],
        portfolioWide: false,
        loanOnly: true,
        hotelAttributed: false,
        milestones: [],
      });
    }
    const h = map.get(key);
    h.findingIds.push(f.findingId);
    if (f.source?.accessionNumber) h.accessions.add(f.source.accessionNumber);
    if (f.wave) h.waves.add(f.wave);
    else if (f.discoveryProvider === "SEC_API_IO" && f.currentStatus) h.waves.add("V3");
    else if (f.discoveryProvider) h.waves.add("V2");
    else h.waves.add("V1");

    const isContract =
      f.isHotelContractDate &&
      !f.isLoanMaturity &&
      f.primaryDate &&
      /FRANCHISE|MANAGEMENT|LICENSE|LEASE|EXPIRATION|COMMENCEMENT|RENEWAL|TERMINATION/i.test(
        String(f.dateMeaning || f.agreementType || ""),
      );
    if (isContract) {
      h.milestoneCount += 1;
      h.loanOnly = false;
      const y = yearOf(f.primaryDate);
      if (y != null && y >= 2027 && y <= 2040) h.futureMilestoneCount += 1;
      if (y != null && y >= 2018) h.recentishMilestoneCount += 1;
      h.milestones.push({
        findingId: f.findingId,
        primaryDate: f.primaryDate,
        dateMeaning: f.dateMeaning,
        currentStatus: f.currentStatus || null,
      });
    }
    if (f.hotelAttributed) h.hotelAttributed = true;
    if (f.portfolioWide) h.portfolioWide = true;
    if (f.counterparty) h.hasCounterparty = true;
    if (/owner|manager|management\s+company|franchisor/i.test(String(f.snippet || ""))) {
      h.hasOwnerOrManagerMention = true;
    }
    if (/formerly|f\/k\/a|aka|a\/k\/a|previously/i.test(String(f.snippet || f.notes || ""))) {
      h.aliasHints.push("historical_alias_language");
    }
    const addr = extractAddressHint(f);
    if (addr && !h.addressHint) h.addressHint = addr;
    if (!h.brandFamily && f.brandFamily) h.brandFamily = f.brandFamily;
    if (!h.brandLabel && f.brandLabel) h.brandLabel = f.brandLabel;
    if (!h.snippetSample && f.snippet) h.snippetSample = f.snippet;
  }

  return [...map.values()].map((h) => ({
    ...h,
    accessions: [...h.accessions],
    waves: [...h.waves],
  }));
}

/**
 * Rank and classify without padding CREATE_READY.
 * @param {object} opts
 * @param {number} [opts.targetMin=25]
 * @param {number} [opts.targetMax=50]
 * @param {number} [opts.collisionCheckCap=50] — max live HPC checks
 */
export function selectUsPilotCandidates(hotels, opts = {}) {
  const targetMin = opts.targetMin ?? 25;
  const targetMax = opts.targetMax ?? 50;
  const scored = hotels.map((h) => {
    const s = scoreUsPilotCandidate(h);
    const pre = classifyPreHpc(h, s);
    return {
      ...h,
      score: s.score,
      scoreReasons: s.reasons,
      scorePenalties: s.penalties,
      preDisposition: pre,
      disposition: pre === "CREATE_READY_PENDING_COLLISION" ? pre : pre,
      collision: null,
    };
  });

  scored.sort(
    (a, b) =>
      b.score - a.score ||
      String(a.hotelName).localeCompare(String(b.hotelName)) ||
      String(a.city).localeCompare(String(b.city)),
  );

  const eligibleForCollision = scored.filter(
    (r) =>
      r.preDisposition === "CREATE_READY_PENDING_COLLISION" ||
      (r.preDisposition === PILOT_DISPOSITION.RESEARCH_REQUIRED && r.score >= 45),
  );

  return {
    version: CONTRACT_TIMING_V4_US_PILOT_SELECTION_V1,
    targetMin,
    targetMax,
    totals: {
      usHotels: scored.length,
      rejectAmbiguous: scored.filter((r) => r.disposition === PILOT_DISPOSITION.REJECT_AMBIGUOUS)
        .length,
      researchRequired: scored.filter(
        (r) =>
          r.disposition === PILOT_DISPOSITION.RESEARCH_REQUIRED ||
          r.disposition === "CREATE_READY_PENDING_COLLISION",
      ).length,
      createReadyPendingCollision: scored.filter(
        (r) => r.preDisposition === "CREATE_READY_PENDING_COLLISION",
      ).length,
    },
    note: "CREATE_READY requires live HPC collision = verified absent. Do not pad to targetMax.",
    collisionCheckQueue: eligibleForCollision.slice(0, opts.collisionCheckCap ?? 50),
    ranked: scored,
  };
}

export default {
  aggregateFindingsToHotels,
  selectUsPilotCandidates,
  scoreUsPilotCandidate,
  classifyPreHpc,
  classifyAfterCollision,
  isUnitedStatesHotel,
  PILOT_DISPOSITION,
};
