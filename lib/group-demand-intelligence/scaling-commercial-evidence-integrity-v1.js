/**
 * Scaling Companies — claim-to-source evidence integrity + commercial requalification.
 * Free/saved evidence only. Does not invent growth or contacts.
 */

import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  applyScalingTwoContactStandard,
  classifyScalingContactCoverage,
  extractPrimaryPersonName,
  SCALING_CONTACT_COVERAGE,
} from "./scaling-two-contact-standard-v1.js";

export const SCALING_EVIDENCE_INTEGRITY_V1 =
  "gdi-scaling-commercial-evidence-integrity-v1";

export const SCALING_COMMERCIAL_DISPOSITION = Object.freeze({
  PUBLISHABLE_SCALING_ACCOUNT: "PUBLISHABLE_SCALING_ACCOUNT",
  RESEARCH_CONTINUATION_REQUIRED: "RESEARCH_CONTINUATION_REQUIRED",
  HOLD_INSUFFICIENT_EVIDENCE: "HOLD_INSUFFICIENT_EVIDENCE",
  BETTER_FIT_OTHER_SEGMENT: "BETTER_FIT_OTHER_SEGMENT",
  DUPLICATE: "DUPLICATE",
  EXCLUDE: "EXCLUDE",
  HOLD_NOT_HOTEL_BUYER: "HOLD_NOT_HOTEL_BUYER",
});

const NEWS_HOST_RE =
  /prnewswire|commercialobserver|selectflorida|therealdeal|sfbwmag|miamiherald|cbs12|citybiz|floridatrend|palmbeachpost|techhubsouthflorida|tradeandindustrydev|bizjournals|globenewswire|businesswire|einpresswire|henselphelps/i;

const COMPETITOR_OR_SUPPLIER_RE =
  /\b(hotel|resort|motel|hostel|lodging|dmc|destination management|tour operator|attraction|theme park|cruise line|airbnb|vrbo|short[- ]term rental|event venue only)\b/i;

const WORKFORCE_HOUSING_RE =
  /\b(workforce housing|affordable housing|multifamily|apartment community|student housing|senior living)\b/i;

const RESEARCH_DATE_AS_EVIDENCE_RE =
  /evidence dated\s+20\d{2}-\d{2}-\d{2}|verified\s+20\d{2}-\d{2}-\d{2}|as of\s+research/i;

const STATUS_QUO_NOT_GROWTH_RE =
  /\b(operates its|operates a|headquartered at|world headquarters|corporate headquarters at|has offices in|is based in)\b/i;

const EXPANSION_SIGNAL_RE =
  /\b(expand|expansion|relocat|open(ed|ing)?|new (hq|office|facility|campus|plant)|hir(e|ing)|invest(ment|ing)|grow(th|ing)|leas(e|ing)|move[sd]?\s+(hq|headquarters)|groundbreak|top[- ]?out|announce[sd]?)\b/i;

/**
 * US city / metro tokens used in URL paths and titles for claim↔source geography checks.
 * Shared gate — not hotel-specific. Keep hyphenated slug forms.
 */
const US_CITY_GEO_TOKENS = Object.freeze([
  "atlanta",
  "austin",
  "baltimore",
  "boston",
  "boca-raton",
  "charlotte",
  "chicago",
  "clearwater",
  "dallas",
  "denver",
  "detroit",
  "durham",
  "fort-lauderdale",
  "houston",
  "jacksonville",
  "los-angeles",
  "memphis",
  "miami",
  "minneapolis",
  "nashville",
  "new-york",
  "norfolk",
  "orlando",
  "philadelphia",
  "phoenix",
  "pittsburgh",
  "portland",
  "raleigh",
  "richmond",
  "san-diego",
  "san-francisco",
  "savannah",
  "seattle",
  "st-petersburg",
  "tampa",
  "washington",
]);

/** Metro aliases: hotel city label → acceptable evidence city tokens. */
const HOTEL_CITY_GEO_ALIASES = Object.freeze({
  tampa: ["tampa", "clearwater", "st-petersburg", "st petersburg", "tampa-bay", "tampa bay"],
  "boca raton": ["boca-raton", "boca raton", "boca", "deerfield-beach", "deerfield beach"],
  miami: ["miami", "miami-beach", "miami beach", "coral-gables", "brickell"],
  orlando: ["orlando", "kissimmee", "lake-buena-vista"],
  "mexico city": ["mexico-city", "mexico city", "cdmx", "ciudad-de-mexico"],
  bethesda: ["bethesda", "washington", "dc", "chevy-chase", "rockville"],
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normalizeGeoToken(s) {
  return text(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function hotelCityAliases(hotelCity) {
  const key = text(hotelCity).toLowerCase();
  const norm = normalizeGeoToken(hotelCity);
  const aliases = new Set([key, norm, text(hotelCity).toLowerCase()]);
  for (const [city, list] of Object.entries(HOTEL_CITY_GEO_ALIASES)) {
    if (key.includes(city) || norm.includes(normalizeGeoToken(city))) {
      for (const a of list) aliases.add(a);
    }
  }
  return aliases;
}

function extractCityTokensFromUrlAndTitle(url, title = "") {
  const found = new Set();
  let path = "";
  try {
    path = new URL(String(url || "")).pathname || "";
  } catch {
    path = String(url || "");
  }
  const blob = `${path} ${title}`.toLowerCase();
  for (const token of US_CITY_GEO_TOKENS) {
    const re = new RegExp(
      `(?:^|[^a-z0-9])${token.replace(/-/g, "[-\\s]?")}(?:[^a-z0-9]|$)`,
      "i",
    );
    if (re.test(blob)) found.add(token);
  }
  return [...found];
}

function growthEvidenceRefs(opportunity = {}) {
  const refs = [];
  for (const s of opportunity.sources || []) {
    if (!s?.url) continue;
    const type = text(s.type || s.kind || "").toLowerCase();
    if (
      /growth|expansion|press|news|project/i.test(type) ||
      type === "" ||
      type === "source"
    ) {
      refs.push({ url: s.url, title: s.title || s.label || "" });
    }
  }
  if (opportunity.sourceUrl || opportunity.growthSourceUrl) {
    refs.push({
      url: opportunity.growthSourceUrl || opportunity.sourceUrl,
      title: opportunity.growthEvidenceTitle || "",
    });
  }
  // Prefer explicit growth URLs; de-dupe
  const seen = new Set();
  return refs.filter((r) => {
    const u = text(r.url);
    if (!u || seen.has(u)) return false;
    seen.add(u);
    return true;
  });
}

function hostOf(url) {
  try {
    return new URL(String(url || "")).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function sourceUrls(opportunity = {}) {
  const urls = [];
  for (const s of opportunity.sources || []) {
    if (s?.url) urls.push(s.url);
  }
  for (const e of opportunity.evidence || []) {
    if (e?.sourceUrl || e?.url) urls.push(e.sourceUrl || e.url);
  }
  if (opportunity.sourceUrl) urls.push(opportunity.sourceUrl);
  if (opportunity.contactUrl) urls.push(opportunity.contactUrl);
  return [...new Set(urls.map((u) => text(u)).filter(Boolean))];
}

function orgTokens(name) {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 3 && !/^(the|and|inc|llc|corp|group|company|co|ltd)$/i.test(t));
}

/**
 * Build claim-to-source mismatches for one Scaling opportunity (offline).
 */
export function auditScalingClaimToSource(opportunity = {}, hotel = {}) {
  const o = opportunity || {};
  const findings = [];
  const urls = sourceUrls(o);
  const hosts = urls.map(hostOf).filter(Boolean);
  const org = text(o.organizationName || o.company);
  const tokens = orgTokens(org);
  const what = text(o.whatIsHappening || o.datedGrowthEvidence || o.summaryWhat);
  const local = text(o.localConnection || o.localPresence);
  const hotelCity = text(hotel.city || hotel.marketCity || "");
  const contactHost = hostOf(o.contactUrl);

  // Wrong-org contact host (contractor / news publisher as "official" contact)
  if (contactHost && NEWS_HOST_RE.test(contactHost)) {
    findings.push({
      code: "CONTACT_HOST_NOT_ORG_OWNED",
      severity: "HIGH",
      detail: `contactUrl host ${contactHost} is a publisher/contractor, not the opportunity organization.`,
      urls: [o.contactUrl].filter(Boolean),
    });
  }

  // Research verification date presented as expansion date
  if (RESEARCH_DATE_AS_EVIDENCE_RE.test(what) || RESEARCH_DATE_AS_EVIDENCE_RE.test(local)) {
    findings.push({
      code: "RESEARCH_DATE_AS_GROWTH_DATE",
      severity: "HIGH",
      detail:
        "Narrative uses research/verification date language as if it were the expansion event date.",
    });
  }

  // Status-quo HQ description without expansion verbs
  if (
    STATUS_QUO_NOT_GROWTH_RE.test(what) &&
    !EXPANSION_SIGNAL_RE.test(what) &&
    !EXPANSION_SIGNAL_RE.test(local)
  ) {
    findings.push({
      code: "STATUS_QUO_NOT_EXPANSION",
      severity: "HIGH",
      detail:
        "Activity narrative describes ongoing HQ/presence without a specific expansion/relocation/hiring event.",
    });
  }

  // Missing growth date when claiming dated activity
  if (!text(o.growthEvidenceDate) && /20\d{2}/.test(what) === false && EXPANSION_SIGNAL_RE.test(what)) {
    findings.push({
      code: "MISSING_GROWTH_EVIDENCE_DATE",
      severity: "MEDIUM",
      detail: "Expansion-like claim lacks growthEvidenceDate and no year in narrative.",
    });
  }

  // Geography: hotel city absent from local connection when both present
  if (
    hotelCity &&
    local &&
    !local.toLowerCase().includes(hotelCity.toLowerCase()) &&
    !what.toLowerCase().includes(hotelCity.toLowerCase())
  ) {
    // Soft — many Boca opportunities use "Boca Raton" while hotel is Waterstone Boca
    findings.push({
      code: "GEOGRAPHY_LABEL_WEAK",
      severity: "LOW",
      detail: `Hotel city "${hotelCity}" not explicitly present in localConnection/whatIsHappening.`,
    });
  }

  // Geography: narrative claims hotel-market activity, but growth evidence URL/title
  // names a different US city (e.g. Tampa claim + Durham manufacturing project URL).
  // Offline / URL+title only — no live fetch. Shared rule for every hotel.
  if (hotelCity) {
    const claimBlob = `${what} ${local}`.toLowerCase();
    const aliases = hotelCityAliases(hotelCity);
    const claimMentionsHotelCity = [...aliases].some((a) => {
      const spaced = String(a).replace(/-/g, " ");
      return claimBlob.includes(spaced) || claimBlob.includes(String(a));
    });
    if (claimMentionsHotelCity) {
      const growthRefs = growthEvidenceRefs(o);
      const contradicting = [];
      for (const ref of growthRefs) {
        const citiesInEvidence = extractCityTokensFromUrlAndTitle(
          ref.url,
          ref.title,
        );
        const foreign = citiesInEvidence.filter((c) => {
          const cNorm = normalizeGeoToken(c);
          return ![...aliases].some(
            (a) =>
              normalizeGeoToken(a) === cNorm ||
              normalizeGeoToken(a).includes(cNorm) ||
              cNorm.includes(normalizeGeoToken(a)),
          );
        });
        // Only fail when evidence names a foreign city AND does not also name the hotel city
        const alsoHotel = citiesInEvidence.some((c) =>
          [...aliases].some(
            (a) =>
              normalizeGeoToken(a) === normalizeGeoToken(c) ||
              normalizeGeoToken(c).includes(normalizeGeoToken(a)),
          ),
        );
        if (foreign.length && !alsoHotel) {
          contradicting.push({
            url: ref.url,
            foreignCities: foreign,
            evidenceCities: citiesInEvidence,
          });
        }
      }
      if (contradicting.length) {
        findings.push({
          code: "SOURCE_GEOGRAPHY_CONTRADICTS_CLAIM",
          severity: "HIGH",
          detail: `Claim references hotel market "${hotelCity}" but growth evidence URL/title names a different city (${contradicting
            .flatMap((c) => c.foreignCities)
            .join(", ")}).`,
          urls: contradicting.map((c) => c.url),
        });
      }
    }
  }

  // Org name tokens absent from all source hosts + titles (weak identity link)
  const blob = `${urls.join(" ")} ${text(o.title)} ${what}`.toLowerCase();
  if (tokens.length && !tokens.some((t) => blob.includes(t))) {
    findings.push({
      code: "ORG_TOKEN_ABSENT_FROM_SOURCES",
      severity: "HIGH",
      detail: "Organization name tokens do not appear in source URLs or activity narrative.",
    });
  }

  // Competitor / supplier classification — check identity fields only.
  // Do NOT scan proposedHotelUse / demand prose (those legitimately say "hotel").
  const identityBlob = `${org} ${text(o.industry)} ${text(o.companyType)} ${text(o.businessModel)} ${text(o.title)}`;
  const activityBlob = `${what} ${local}`;
  if (
    COMPETITOR_OR_SUPPLIER_RE.test(identityBlob) &&
    !/corporate travel|workplace|headquarters|office/i.test(identityBlob)
  ) {
    findings.push({
      code: "POSSIBLE_COMPETITOR_OR_SUPPLIER",
      severity: "HIGH",
      detail:
        "Organization identity suggests lodging competitor, attraction, DMC, or intermediary — not a Scaling Corporate Account buyer.",
    });
  }

  if (
    WORKFORCE_HOUSING_RE.test(`${identityBlob} ${activityBlob}`) &&
    !/corporate|headquarters|office expansion|relocat/i.test(activityBlob)
  ) {
    findings.push({
      code: "WORKFORCE_HOUSING_NOT_CORPORATE_TRAVEL",
      severity: "MEDIUM",
      detail:
        "Signal may be real-estate / workforce housing rather than corporate hotel demand.",
    });
  }

  // Orphan secondary (named person not in primary) — structural contact defect
  const primary = extractPrimaryPersonName(o);
  const secName = text(o.secondaryContact?.name);
  if (!primary && secName && looksLikePersonName(secName)) {
    findings.push({
      code: "NAMED_PERSON_ORPHANED_IN_SECONDARY",
      severity: "MEDIUM",
      detail: `Named contactable person "${secName}" stored only as secondary; primary is organization-route.`,
    });
  }

  // No usable sources
  if (!urls.length) {
    findings.push({
      code: "NO_SOURCE_URLS",
      severity: "HIGH",
      detail: "Opportunity has no source URLs for claim verification.",
    });
  }

  const high = findings.filter((f) => f.severity === "HIGH").length;
  const medium = findings.filter((f) => f.severity === "MEDIUM").length;

  return {
    version: SCALING_EVIDENCE_INTEGRITY_V1,
    opportunityId: o.id || o.opportunityId || null,
    organizationName: org || null,
    sourceUrlCount: urls.length,
    sourceHosts: [...new Set(hosts)],
    findings,
    highSeverityCount: high,
    mediumSeverityCount: medium,
    evidenceIntegrityPass: high === 0,
  };
}

/**
 * Map evidence audit + existing commercial flags → requalification disposition.
 */
export function requalifyScalingOpportunity(opportunity = {}, evidenceAudit = {}, opts = {}) {
  const o = opportunity || {};
  const existing = text(
    o.scalingCommercialDisposition || o.internalDisposition || o.disposition || "",
  );

  if (/HOLD_NOT_HOTEL_BUYER|NOT_HOTEL_BUYER|LANYARD/i.test(existing) || opts.forceHoldNotBuyer) {
    return {
      disposition: SCALING_COMMERCIAL_DISPOSITION.HOLD_NOT_HOTEL_BUYER,
      reason: "Existing or forced not-hotel-buyer hold.",
      customerFacingEligible: false,
    };
  }

  const codes = new Set((evidenceAudit.findings || []).map((f) => f.code));
  if (codes.has("POSSIBLE_COMPETITOR_OR_SUPPLIER")) {
    return {
      disposition: SCALING_COMMERCIAL_DISPOSITION.BETTER_FIT_OTHER_SEGMENT,
      reason: "Competitor/supplier/intermediary classification risk.",
      customerFacingEligible: false,
      alternateSegmentHint: "review_other_gdi_segment",
    };
  }
  if (
    codes.has("STATUS_QUO_NOT_EXPANSION") ||
    codes.has("NO_SOURCE_URLS") ||
    codes.has("SOURCE_GEOGRAPHY_CONTRADICTS_CLAIM")
  ) {
    return {
      disposition: SCALING_COMMERCIAL_DISPOSITION.HOLD_INSUFFICIENT_EVIDENCE,
      reason: codes.has("SOURCE_GEOGRAPHY_CONTRADICTS_CLAIM")
        ? "Growth evidence geography contradicts the claimed hotel-market expansion."
        : "Insufficient expansion evidence or missing sources.",
      customerFacingEligible: false,
    };
  }
  if (codes.has("CONTACT_HOST_NOT_ORG_OWNED") && codes.has("ORG_TOKEN_ABSENT_FROM_SOURCES")) {
    return {
      disposition: SCALING_COMMERCIAL_DISPOSITION.HOLD_INSUFFICIENT_EVIDENCE,
      reason: "Contact and identity sources do not support the claimed organization.",
      customerFacingEligible: false,
    };
  }
  if (codes.has("RESEARCH_DATE_AS_GROWTH_DATE") || codes.has("CONTACT_HOST_NOT_ORG_OWNED")) {
    return {
      disposition: SCALING_COMMERCIAL_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED,
      reason: "Material claim/contact defects require focused free research before confident publication.",
      customerFacingEligible: o.customerVisible !== false && !opts.stripFacingOnContinuation,
      keepFacingWithDisclosure: true,
    };
  }
  if ((evidenceAudit.highSeverityCount || 0) > 0) {
    return {
      disposition: SCALING_COMMERCIAL_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED,
      reason: "High-severity evidence findings remain.",
      customerFacingEligible: o.customerVisible !== false,
      keepFacingWithDisclosure: true,
    };
  }

  return {
    disposition: SCALING_COMMERCIAL_DISPOSITION.PUBLISHABLE_SCALING_ACCOUNT,
    reason: "No high-severity evidence defects; organization remains eligible Scaling account-development prospect.",
    customerFacingEligible: true,
  };
}

/**
 * Full offline assessment: evidence + promotion + coverage classification.
 */
export function assessScalingOpportunityCommercialQuality(opportunity = {}, hotel = {}) {
  const evidence = auditScalingClaimToSource(opportunity, hotel);
  const requal = requalifyScalingOpportunity(opportunity, evidence);
  const promoted = applyScalingTwoContactStandard({
    ...opportunity,
    researchSegment: opportunity.researchSegment || "scaling_companies",
  });
  const coverage = classifyScalingContactCoverage(promoted, {
    commerciallyIneligible:
      requal.disposition === SCALING_COMMERCIAL_DISPOSITION.HOLD_NOT_HOTEL_BUYER ||
      requal.disposition === SCALING_COMMERCIAL_DISPOSITION.EXCLUDE,
  });

  const primaryName = extractPrimaryPersonName(promoted);
  const secondaryName =
    looksLikePersonName(promoted.secondaryContact?.name || "")
      ? text(promoted.secondaryContact.name)
      : null;

  return {
    version: SCALING_EVIDENCE_INTEGRITY_V1,
    opportunityId: opportunity.id || null,
    organizationName: opportunity.organizationName || null,
    hotelId: hotel.hotelId || hotel.id || opportunity.hotelId || null,
    evidence,
    requalification: requal,
    contactCoverageAfterPromotion: coverage,
    primaryNamed: primaryName,
    secondaryNamed: secondaryName,
    promotedFromSecondary: Boolean(promoted.promotedNamedContactFromSecondary),
    stagedOpportunity: promoted,
    customerFacingBefore: opportunity.customerVisible !== false,
    customerFacingAfterRecommended:
      requal.customerFacingEligible !== false &&
      requal.disposition !== SCALING_COMMERCIAL_DISPOSITION.HOLD_INSUFFICIENT_EVIDENCE &&
      requal.disposition !== SCALING_COMMERCIAL_DISPOSITION.EXCLUDE &&
      requal.disposition !== SCALING_COMMERCIAL_DISPOSITION.HOLD_NOT_HOTEL_BUYER &&
      requal.disposition !== SCALING_COMMERCIAL_DISPOSITION.BETTER_FIT_OTHER_SEGMENT,
  };
}

export { SCALING_CONTACT_COVERAGE };
