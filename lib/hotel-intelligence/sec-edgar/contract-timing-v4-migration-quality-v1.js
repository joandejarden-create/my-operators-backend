/**
 * Quality-gated migration cohort from V1–V3 contract findings.
 * Stable keys ≠ verified agreement identities.
 */

import crypto from "node:crypto";

export const MIGRATION_QUALITY_V1 = "contract-timing-v4-migration-quality-v1";

export const QUALITY_CLASS = Object.freeze({
  VERIFIED: "VERIFIED",
  RESEARCH_ONLY: "RESEARCH_ONLY",
  CONFLICTED: "CONFLICTED",
  REJECTED: "REJECTED",
});

const NON_HOTEL =
  /\b(rv\s+resort|manufactured\s+housing|mobile\s+home|self[\s-]?storage|office\s+building|shopping\s+center|industrial|warehouse|apartments?\b|multifamily|student\s+housing|casino\s+and\s+(?!hotel)|parking\s+garage)\b/i;

const STREET_NAME = /^\d{1,6}\s+\w+/;
const HOTEL_LEXEME = /\b(hotel|inn|suites|resort|lodge|motel|hyatt|marriott|hilton|hampton|courtyard|residence\s+inn|home2|homewood|fairfield|embassy|doubletree|crowne|holiday\s+inn|sheraton|westin|radisson|wyndham|best\s+western)\b/i;
const FRANCHISE_Wording =
  /\b(franchise\s+(agreement|expiration|expir)|franchisor|franchisee)\b/i;
const MANAGEMENT_Wording = /\b(management\s+(agreement|expiration|expir)|hotel\s+manager)\b/i;
const US_STATES = new Set([
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
  "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
  "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
  "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC", "PR",
]);

function sha(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 16);
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function sourceUrlOf(f) {
  return (
    f?.source?.documentUrl ||
    f?.source?.filingUrl ||
    f?.sourceUrl ||
    f?.primarySourceUrl ||
    f?.url ||
    null
  );
}

export function accessionOf(f) {
  return f?.source?.accessionNumber || f?.secAccession || f?.accessionNumber || null;
}

export function normalizeAgreementType(f) {
  const raw = String(f.agreementType || f.agreementFamily || f.dateMeaning || "").toUpperCase();
  if (/MANAGEMENT/.test(raw)) return "MANAGEMENT";
  if (/LEASE/.test(raw)) return "LEASE";
  if (/LICENSE/.test(raw)) return "LICENSE";
  if (/FRANCHISE/.test(raw)) return "FRANCHISE";
  const snip = String(f.snippet || "");
  if (MANAGEMENT_Wording.test(snip)) return "MANAGEMENT";
  if (FRANCHISE_Wording.test(snip)) return "FRANCHISE";
  return "OTHER";
}

export function agreementKey(f) {
  return [
    norm(f.hotelName),
    norm(f.city),
    norm(f.stateRegion || f.state),
    normalizeAgreementType(f),
    norm(f.brandFamily || f.brandLabel || ""),
  ].join("|");
}

export function milestoneKey(f) {
  return [agreementKey(f), f.dateMeaning || "", f.primaryDate || ""].join("|");
}

export function hotelIdentityKey(f) {
  return [norm(f.hotelName), norm(f.stateRegion || f.state)].join("|");
}

/**
 * Classify a single finding before agreement aggregation.
 */
export function classifyFindingQuality(f) {
  const reasons = [];
  const name = String(f.hotelName || "");
  const city = String(f.city || "");
  const state = String(f.stateRegion || f.state || "")
    .trim()
    .toUpperCase();
  const snip = String(f.snippet || "");
  const blob = `${name} ${city} ${snip}`;

  if (!f.isHotelContractDate || f.isLoanMaturity) {
    return { class: QUALITY_CLASS.REJECTED, reasons: ["loan_or_non_contract"] };
  }
  if (!name || !f.hotelAttributed || f.portfolioWide) {
    return { class: QUALITY_CLASS.REJECTED, reasons: ["unattributed_or_portfolio"] };
  }
  if (NON_HOTEL.test(blob) || /manufactured\s+housing/i.test(snip)) {
    return { class: QUALITY_CLASS.REJECTED, reasons: ["non_hotel_asset"] };
  }
  if (STREET_NAME.test(name.trim()) && !HOTEL_LEXEME.test(name)) {
    return { class: QUALITY_CLASS.REJECTED, reasons: ["street_address_as_name"] };
  }
  if (!f.primaryDate) {
    return { class: QUALITY_CLASS.REJECTED, reasons: ["missing_date"] };
  }

  const url = sourceUrlOf(f);
  const acc = accessionOf(f);
  const hasProvenance = Boolean(url || acc);
  const hasGeo = Boolean(city) && US_STATES.has(state);
  const hotelish = HOTEL_LEXEME.test(name) || HOTEL_LEXEME.test(snip);
  const type = normalizeAgreementType(f);
  // Exact contractual wording in snippet (not merely dateMeaning enum label)
  const wordingInSnippet =
    type === "FRANCHISE"
      ? FRANCHISE_Wording.test(snip)
      : type === "MANAGEMENT"
        ? MANAGEMENT_Wording.test(snip)
        : type === "LEASE"
          ? /\blease\s+(agreement|expiration|expir)\b/i.test(snip)
          : type === "LICENSE"
            ? /\blicen[cs]e\s+(agreement|expiration|expir)\b/i.test(snip)
            : false;
  const cmbsFranchiseColumn =
    /franchise[_\s-]*expir/i.test(String(f.dateMeaning || "")) &&
    /\|\s*[A-Z]{2}\s*\|\s*\d{5}\b/.test(snip) &&
    /hotel|inn|suites|resort|lodge|motel/i.test(snip);

  // County-as-city (e.g. Bellagio city=Clark) → conflict risk
  if (/^(clark|cook|harris|maricopa|king|los\s+angeles)$/i.test(city) && /hotel|casino/i.test(name)) {
    reasons.push("city_may_be_county");
  }
  if (!hasGeo) reasons.push("missing_or_invalid_city_state");
  if (!hasProvenance) reasons.push("missing_source_provenance");
  if (!hotelish) reasons.push("weak_hotel_lexeme");
  if (!wordingInSnippet && !cmbsFranchiseColumn) {
    reasons.push("agreement_type_wording_not_in_snippet");
  }
  if (type === "OTHER") reasons.push("agreement_type_other");
  // Casino/complex without clear franchise wording stays RESEARCH_ONLY / CONFLICTED
  if (/\bcasino\b/i.test(name) && !wordingInSnippet) {
    reasons.push("casino_complex_franchise_unverified");
  }

  if (reasons.includes("city_may_be_county")) {
    return { class: QUALITY_CLASS.CONFLICTED, reasons, agreementType: type, url, accession: acc };
  }
  if (
    hasGeo &&
    hasProvenance &&
    hotelish &&
    (wordingInSnippet || cmbsFranchiseColumn) &&
    type !== "OTHER" &&
    !reasons.includes("casino_complex_franchise_unverified") &&
    reasons.filter((r) => r !== "agreement_type_wording_not_in_snippet").length === 0
  ) {
    return {
      class: QUALITY_CLASS.VERIFIED,
      reasons: wordingInSnippet
        ? ["identity_geo_provenance_wording_ok"]
        : ["identity_geo_provenance_cmbs_franchise_column"],
      agreementType: type,
      url,
      accession: acc,
    };
  }
  return {
    class: QUALITY_CLASS.RESEARCH_ONLY,
    reasons: reasons.length ? reasons : ["insufficient_certainty"],
    agreementType: type,
    url,
    accession: acc,
  };
}

/**
 * Build quality-gated migration structures from findings.
 */
export function buildQualityGatedMigration(findings) {
  const contractFindings = (findings || []).filter(
    (f) => f.isHotelContractDate && !f.isLoanMaturity && f.primaryDate && f.hotelName,
  );

  const findingClasses = [];
  const byClass = {
    VERIFIED: 0,
    RESEARCH_ONLY: 0,
    CONFLICTED: 0,
    REJECTED: 0,
  };

  const agreements = new Map();
  const milestones = new Map();
  const excluded = [];
  const quarantined = [];

  // Detect hotel identity collisions (same name+state, conflicting cities)
  const cityByHotel = new Map();
  for (const f of contractFindings) {
    const hk = hotelIdentityKey(f);
    const city = norm(f.city);
    if (!cityByHotel.has(hk)) cityByHotel.set(hk, new Set());
    if (city) cityByHotel.get(hk).add(city);
  }

  for (const f of contractFindings) {
    const q = classifyFindingQuality(f);
    findingClasses.push({
      findingId: f.findingId,
      hotelName: f.hotelName,
      city: f.city,
      stateRegion: f.stateRegion,
      quality: q.class,
      reasons: q.reasons,
      wave: f.wave,
    });
    byClass[q.class] = (byClass[q.class] || 0) + 1;

    if (q.class === QUALITY_CLASS.REJECTED) {
      excluded.push({
        findingId: f.findingId,
        hotelName: f.hotelName,
        reasons: q.reasons,
        wave: f.wave,
      });
      continue;
    }

    const hk = hotelIdentityKey(f);
    const cities = cityByHotel.get(hk);
    const multiCity = cities && cities.size > 1;
    let quality = q.class;
    const reasons = [...q.reasons];
    if (multiCity) {
      quality = QUALITY_CLASS.CONFLICTED;
      reasons.push("multi_city_identity_collision");
    }

    if (quality === QUALITY_CLASS.CONFLICTED) {
      quarantined.push({
        findingId: f.findingId,
        hotelName: f.hotelName,
        city: f.city,
        stateRegion: f.stateRegion,
        reasons,
      });
    }

    const ak = agreementKey(f);
    if (!agreements.has(ak)) {
      agreements.set(ak, {
        contractId: `ctr_${sha(ak)}`,
        hotelName: f.hotelName,
        city: f.city,
        stateRegion: f.stateRegion,
        agreementType: normalizeAgreementType(f),
        brandFamily: f.brandFamily || null,
        quality,
        reasons,
        findingIds: [],
        waves: new Set(),
        sourceUrls: new Set(),
        sourceAccessions: new Set(),
        evidenceSnippets: [],
      });
    }
    const a = agreements.get(ak);
    a.findingIds.push(f.findingId);
    a.waves.add(f.wave);
    // Escalate quality: VERIFIED < RESEARCH_ONLY < CONFLICTED for agreement rollup
    const rank = { VERIFIED: 1, RESEARCH_ONLY: 2, CONFLICTED: 3, REJECTED: 4 };
    if (rank[quality] > rank[a.quality]) {
      a.quality = quality;
      a.reasons = reasons;
    }
    const url = sourceUrlOf(f);
    const acc = accessionOf(f);
    if (url) a.sourceUrls.add(url);
    if (acc) a.sourceAccessions.add(acc);
    if (f.snippet && a.evidenceSnippets.length < 2) {
      a.evidenceSnippets.push(String(f.snippet).slice(0, 240));
    }

    const mk = milestoneKey(f);
    if (!milestones.has(mk)) {
      milestones.set(mk, {
        milestoneId: `ms_${sha(mk)}`,
        contractId: a.contractId,
        primaryDate: f.primaryDate,
        dateMeaning: f.dateMeaning,
        currentStatus: f.currentStatus || "EXPIRED_STATUS_UNCONFIRMED",
        historicalApplicability: f.historicalApplicability || null,
        quality,
        publicationState: "CUSTOMER_BLOCKED",
        findingIds: [f.findingId],
        waves: new Set([f.wave]),
        sourceUrls: new Set(url ? [url] : []),
        sourceAccessions: new Set(acc ? [acc] : []),
      });
    } else {
      const m = milestones.get(mk);
      m.findingIds.push(f.findingId);
      m.waves.add(f.wave);
      if (url) m.sourceUrls.add(url);
      if (acc) m.sourceAccessions.add(acc);
      if (rank[quality] > rank[m.quality]) m.quality = quality;
    }
  }

  const serializeA = (a) => ({
    ...a,
    waves: [...a.waves],
    sourceUrls: [...a.sourceUrls],
    sourceAccessions: [...a.sourceAccessions],
  });
  const serializeM = (m) => ({
    ...m,
    waves: [...m.waves],
    sourceUrls: [...m.sourceUrls],
    sourceAccessions: [...m.sourceAccessions],
  });

  const agreementList = [...agreements.values()].map(serializeA);
  const milestoneList = [...milestones.values()].map(serializeM);

  const agreementByQuality = agreementList.reduce((acc, a) => {
    acc[a.quality] = (acc[a.quality] || 0) + 1;
    return acc;
  }, {});

  const physicalHotels = new Set(
    agreementList
      .filter((a) => a.quality !== QUALITY_CLASS.REJECTED)
      .map((a) => [norm(a.hotelName), norm(a.city), norm(a.stateRegion)].join("|")),
  );

  return {
    version: MIGRATION_QUALITY_V1,
    input: {
      findingsLoaded: findings.length,
      contractFindings: contractFindings.length,
      reportedTotalHint: "57+19+405=481 reported finding total — not automatic unique agreements",
    },
    findingQualityCounts: byClass,
    agreementQualityCounts: agreementByQuality,
    unique: {
      physicalHotels: physicalHotels.size,
      agreements: agreementList.length,
      milestones: milestoneList.length,
      excludedFindings: excluded.length,
      quarantinedFindings: quarantined.length,
    },
    agreements: agreementList,
    milestones: milestoneList,
    excluded: excluded.slice(0, 50),
    quarantined: quarantined.slice(0, 50),
    samples: {
      rejected: excluded.slice(0, 8),
      conflicted: agreementList.filter((a) => a.quality === QUALITY_CLASS.CONFLICTED).slice(0, 8),
      researchOnly: agreementList.filter((a) => a.quality === QUALITY_CLASS.RESEARCH_ONLY).slice(0, 8),
      verified: agreementList.filter((a) => a.quality === QUALITY_CLASS.VERIFIED).slice(0, 8),
    },
    note: "Stable keys are reconciliation aids only. VERIFIED ≠ authorized Airtable persist until Research Mode pilot auth.",
  };
}

export default {
  buildQualityGatedMigration,
  classifyFindingQuality,
  normalizeAgreementType,
  QUALITY_CLASS,
};
