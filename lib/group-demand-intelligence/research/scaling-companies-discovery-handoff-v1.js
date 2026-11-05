/**
 * Scaling-companies discovery → research-candidate handoff V1.
 *
 * Closes the hotel-generic runner gap: discovery hits are NOT opportunities.
 * Pipeline (reuses Bethesda live patterns + canonical modules):
 *   RAW_DISCOVERY_HIT → IDENTITY_VERIFIED → MECHANISM_SCREENED →
 *   RESEARCH_ELIGIBLE → (ORS + public WHO) → QUALIFIED_ACCOUNT_DEVELOPMENT /
 *   PUBLICATION_READY / EXCLUDED
 *
 * Hotels may start with zero existing scaling inventory.
 * Never writes unqualified hits to customer-facing storage.
 */

import { createHash } from "node:crypto";
import {
  parseStageAHitIdentity,
  isWeakOrGenericCandidate,
} from "./bethesda-full-spectrum-stage-a-live-v1.js";
import {
  qualifyV5DiscoveryCandidate,
  DEMAND_MECHANISM_CLASS,
  DISCOVERY_PATHWAY,
} from "./autonomous-demand-discovery-v5.js";
import {
  qualifyScalingCompanyCandidate,
  buildScalingCompanyOpportunityCard,
  findExistingScalingCompanyMatches,
  resolveScalingCompaniesResearchCity,
  SCALING_COMPANIES_RESEARCH_VERSION,
} from "../workflow/scaling-companies-research-v1.js";
import { applyScalingSalesPresentation } from "../scaling-companies-sales-presentation-v1.js";
import { applyScalingTwoContactStandard } from "../scaling-two-contact-standard-v1.js";
import {
  auditScalingClaimToSource,
  requalifyScalingOpportunity,
} from "../scaling-commercial-evidence-integrity-v1.js";
import { runPublicBuyerIntelligencePass } from "./commercial-depth-buyer-intelligence-v1.js";
import {
  extractPublicEvidenceFromText,
  selectPreferredPublicEmail,
} from "./direct-public-executor-v1.js";
import { fetchOfficialPage } from "../official-page-fetch.js";
import { runDeepResearchOnExistingGdiOpportunity } from "./deep-research-gdi-integration-v1.js";
import {
  evaluateProspectPublicationEligibility,
  hasUsableContactRoute,
} from "../prospect-publication-eligibility-v1.js";
import {
  evaluateScalingPublicationQualification,
  applyScalingPublicationGateToEligibility,
} from "../scaling-publication-qualification-v1.js";
import {
  attachSecondaryNamedContact,
  looksLikePersonName,
} from "../secondary-named-contact-v1.js";
import { buildHotelSpecificFitAssessment } from "../scaling-commercial-intelligence-brief-v1.js";

/** Deep-research result shape is `{ after: { opportunity } }` (not top-level). */
function opportunityFromDeepResearchResult(deep, fallback = null) {
  return (
    deep?.after?.opportunity ||
    deep?.opportunity ||
    deep?.matured ||
    fallback
  );
}

export const SCALING_DISCOVERY_HANDOFF_V1 =
  "gdi-scaling-companies-discovery-handoff-v1";

export const FUNNEL_STAGE = Object.freeze({
  RAW_DISCOVERY_HIT: "RAW_DISCOVERY_HIT",
  IDENTITY_VERIFIED: "IDENTITY_VERIFIED",
  MECHANISM_SCREENED: "MECHANISM_SCREENED",
  RESEARCH_ELIGIBLE: "RESEARCH_ELIGIBLE",
  GENUINELY_NET_NEW: "GENUINELY_NET_NEW",
  EXISTING_MATCH: "EXISTING_MATCH",
  RESEARCHED_BY_ORS: "RESEARCHED_BY_ORS",
  QUALIFIED_ACCOUNT_DEVELOPMENT: "QUALIFIED_ACCOUNT_DEVELOPMENT",
  PUBLICATION_READY: "PUBLICATION_READY",
  EXCLUDED: "EXCLUDED",
});

const FALSE_POSITIVE_URL =
  /crunchbase|builtin\.com\/company|linkedin\.com\/(company|posts)|glassdoor|indeed\.com|levels\.fyi|zoominfo|apollo\.io|rocketreach|\/jobs?\/|\/careers?\/|yummyjobs|breakroom\.cc|aveliving\.com\/blog|exa\.ai\/library|passkey|onpeak|housing.?guide|cvent\.com\/venues|retreatsandvenues|thebocaraton\.com|teneohg\.com|experienceepic\.com|kirklanddmc|dudmc\.com|creamofthecropevents|bocabydesign\.com|offsite\.com\/(destinations|planning)|blucorporatehousing|izealstay|grimaldivacation|wezoo\.com|insightglobal\.com|careers\.ey\.com|hospitalitynet\.org|wwashow\.org\/speaker|myboca\.us\/m\/newsflash|bdb\.org\/services/i;

const NEWS_OR_DIRECTORY_HOST =
  /^(www\.)?(sfbwmag|miamiherald|cbs12|commercialsearch|commercialobserver|therealdeal|palmbeachpost|floridatrend|prnewswire|citybiz|selectflorida|techhubsouthflorida|thepalmbeaches|bdb\.org)\b/i;

const WEAK_ORG =
  /^(manager|corporate|company|hotel|travel|lodging|housing|venue|events?|dmc|planner|guide|destination|challenge|intern|goal of|news flash|con|florida|larry|dan|around the state|global tech|two companies|beach club|meeting room|furnished|new hotel|pebb|brokerage|what happens|island hospitality|palm\s*beach\s*challenge|your destination|kirkland|mdvip celebrates|tandemly|servicenow)\b/i;

const COMPETITOR_OR_VENDOR_ORG =
  /^(the\s+boca\s+raton|boca\s+raton\s+conference|corporate\s+(events|hospitality|housing|relocations)|florida\s*&\s*miami\s*dmc|dmc\s+south|palm\s+beach\s+executive\s+retreat|leadership\s+coaching|tech\s+company|mystery\s+tech)\b/i;

const NEWS_PUBLISHER_EMAIL =
  /@(commercialobserver|therealdeal|bizjournals|prnewswire|miamiherald|palmbeachpost|floridatrend|cbs12|citybiz|sfbwmag|techcrunch|reuters|bloomberg)\./i;

export function isNewsPublisherContactEmail(email, sourceUrl = "") {
  const e = text(email).toLowerCase();
  if (!e || !e.includes("@")) return false;
  if (NEWS_PUBLISHER_EMAIL.test(e)) return true;
  try {
    const host = new URL(sourceUrl).hostname.replace(/^www\./i, "").toLowerCase();
    const domain = e.split("@")[1] || "";
    if (host && domain && (host === domain || host.endsWith(`.${domain}`) || domain.endsWith(host))) {
      if (NEWS_OR_DIRECTORY_HOST.test(host) || NEWS_OR_DIRECTORY_HOST.test(domain)) {
        return true;
      }
    }
  } catch {
    /* ignore */
  }
  return false;
}

/**
 * Extract a real organization from announcement-style discovery titles.
 * Prefer named companies over publisher hostnames.
 */
export function resolveScalingOrganizationFromHit(hit = {}) {
  const title = text(hit.title || hit.organizationName || "");
  const excerpt = text(hit.excerpt || hit.note || "");
  const url = hit.url || hit.sourceUrl || "";
  const blob = `${title} ${excerpt} ${url}`;

  const namedPatterns = [
    /\b(D[- ]?Wave(?:\s+Quantum(?:\s+Inc\.?)?)?)\b/i,
    /\b(DigitalBridge)\b/i,
    /\b(Hair\s+Club)\b/i,
    /\b(ServiceNow)\b/i,
    /\b(MDVIP)\b/i,
    /\b(MPLT\s+Healthcare)\b/i,
    /\b(Munck\s+Wilson\s+Mandala)\b/i,
    /\b(Cresa)\b/i,
    /\b(WSHC\+?B|WSH\s+Law|Weiss\s+Serota)\b/i,
    /\b(The\s+GEO\s+Group)\b/i,
    /\b(CP\s+Group)\b/i,
    /\b(Innovative(?:[-_]?Solutions)?)\b/i,
    /\b(eHouse)\b/i,
    /\b(FAU(?:'s)?(?:\s+Tech\s+Runway)?)\b/i,
    /\b(Brookfield)\b/i,
    /\b(Bridge\s+Industrial)\b/i,
    /\b(GEO\s+Group)\b/i,
  ];
  for (const re of namedPatterns) {
    const m = blob.match(re);
    if (m?.[1]) {
      let name = text(m[1]).replace(/[-_]/g, " ");
      if (/^WSHC/i.test(name)) name = "WSHC+B";
      if (/^Innovative/i.test(name)) name = "Innovative";
      if (/^FAU/i.test(name)) name = "FAU Tech Runway";
      if (/^D[- ]?Wave/i.test(name)) name = "D-Wave Quantum";
      if (/^GEO\s+Group/i.test(name)) name = "The GEO Group";
      return { organizationName: name, method: "named_entity_in_title" };
    }
  }

  const announce = title.match(
    /^(.{3,80}?)\s+(?:Announces|Secures|Signs|Opens|Expands|Celebrates|Employs|Continues|Unites|Relocat(?:es|ion)|Moves|Chooses)\b/i,
  );
  if (
    announce?.[1] &&
    !WEAK_ORG.test(announce[1]) &&
    !COMPETITOR_OR_VENDOR_ORG.test(announce[1]) &&
    !/^(tech|mystery)\s+company$/i.test(announce[1])
  ) {
    return {
      organizationName: text(announce[1]).replace(/\s+'s$/i, ""),
      method: "announcement_subject",
    };
  }

  const relocation = title.match(
    /(?:Relocation|HQ|Headquarters|Office)\s+of\s+(.{3,60}?)(?:\s+to\b|\s+in\b|$)/i,
  );
  if (relocation?.[1] && !/pioneer|firm|company|tenant/i.test(relocation[1])) {
    return { organizationName: text(relocation[1]), method: "relocation_object" };
  }

  const prompts = title.match(/Prompts\s+(.{3,50}?)\s+to\s+Move/i);
  if (prompts?.[1]) {
    return { organizationName: text(prompts[1]), method: "prompts_move" };
  }

  // Fall back to Stage-A parser, but reject news-host orgs
  const parsed = parseStageAHitIdentity(
    { title, excerpt, url },
    "scaling_companies",
  );
  let org = parsed.organizationName;
  if (org && NEWS_OR_DIRECTORY_HOST.test(url.replace(/^https?:\/\//i, ""))) {
    // Hostname leaf as org is a false positive for news pages
    try {
      const host = new URL(url).hostname.replace(/^www\./i, "");
      const leaf = host.split(".")[0].toUpperCase();
      if (org.replace(/\s+/g, "").toUpperCase() === leaf || org.length <= 12) {
        org = null;
      }
    } catch {
      /* ignore */
    }
  }
  if (org && (WEAK_ORG.test(org) || COMPETITOR_OR_VENDOR_ORG.test(org))) {
    org = null;
  }
  if (org && org.length > 70) {
    // Truncated full headline used as org — try first clause
    const clause = org.split(/\s+(?:Announces|Opens|Signs|Expands|Celebrates)\b/i)[0];
    org = clause && clause.length >= 3 && clause.length < 70 ? clause : null;
  }
  return {
    organizationName: org || null,
    method: org ? "stage_a_fallback" : "unresolved",
    programName: parsed.programName || title.slice(0, 160),
    eventYear: parsed.eventYear,
    demandSignalSummary: parsed.demandSignalSummary,
  };
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function orgKey(name) {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 12);
}

export function marketLocalTerms(hotel = {}) {
  const terms = new Set();
  const add = (v) => {
    const t = text(v).toLowerCase();
    if (t && t.length >= 3) terms.add(t);
  };
  add(hotel.city);
  add(resolveScalingCompaniesResearchCity(hotel));
  add(hotel.displayName);
  for (const a of hotel.demandTerritory?.includes || []) {
    add(String(a).split(/\s+where\b|\s*\/\s*/)[0]);
  }
  add(hotel.demandTerritory?.label);
  for (const a of hotel.commercialPriorities?.demandAnchorFocus || []) {
    String(a)
      .split(/[/,·|]/)
      .map((s) => s.trim())
      .filter((s) => s.length >= 3 && s.length <= 40)
      .forEach(add);
  }
  return [...terms].slice(0, 32);
}

export function inferLocalConnectionForHotel(hit = {}, parsed = {}, hotel = {}) {
  const terms = marketLocalTerms(hotel);
  // Never concatenate internal ops notes (ORS qualification instructions) into
  // customer-facing local-connection prose — prefer clean title / program name.
  const excerptRaw = text(hit.excerpt || "");
  const excerptIsOps =
    /Requires qualification|before publication|DISCOVERY_HIT|HOLD_CONTACT|REVIEWED_PACKAGE/i.test(
      excerptRaw,
    );
  const excerpt = excerptIsOps ? "" : excerptRaw;
  const title = text(hit.title || parsed.programName || "");
  const blobForMatch = `${title} ${excerpt} ${parsed.programName || ""} ${hit.url || parsed.sourceUrl || ""}`;
  const lowerBlob = blobForMatch.toLowerCase();
  const compactBlob = lowerBlob.replace(/[^a-z0-9]+/g, "");
  const customerBlob = text(`${title}${excerpt ? ` — ${excerpt}` : ""}`).slice(0, 280);
  for (const t of terms) {
    if (t.length < 4) continue;
    if (lowerBlob.includes(t)) {
      return customerBlob || title.slice(0, 280) || null;
    }
    // Match "boca raton" against URL path "boca-raton" / "bocaraton"
    const compactTerm = t.replace(/[^a-z0-9]+/g, "");
    if (compactTerm.length >= 6 && compactBlob.includes(compactTerm)) {
      return customerBlob || title.slice(0, 280) || null;
    }
  }
  return null;
}

export function inferHotelUseForHotel(hit = {}, parsed = {}, hotel = {}) {
  const blob =
    `${hit.title || ""} ${hit.excerpt || ""} ${parsed.programName || ""} ${hit.url || parsed.sourceUrl || ""}`.toLowerCase();
  const hotelName = text(hotel.displayName || hotel.name || "the hotel");
  const city = resolveScalingCompaniesResearchCity(hotel) || text(hotel.city) || "the market";
  if (/training|onboarding|kickoff|offsite|workshop|retreat|leadership meeting/i.test(blob)) {
    return `Training / onboarding / offsite lodging and meeting space at ${hotelName} for visiting or newly hired staff near ${city}`;
  }
  if (/office|headquarters|hq|expand|opened|opening|reloc|campus|leasing|innovation/i.test(blob)) {
    return `Visiting leadership, project staff, and client meetings needing lodging/meeting space near ${city} operations — evaluate ${hotelName} fit`;
  }
  if (/contract|project|mobiliz|implementation|construction|headquarters/i.test(blob)) {
    return `Project-team lodging and intermittent working sessions for mobilized staff near ${city}; ${hotelName} as a candidate property`;
  }
  if (/meeting|hotel|lodging|rooms|gathering|hospitality/i.test(blob)) {
    return `Corporate lodging and meeting demand associated with documented local activity — ${hotelName} only if property fit holds`;
  }
  return null;
}

function hotelFitRationale(orgName, localConnection, hotel) {
  if (!localConnection) return null;
  const fit = buildHotelSpecificFitAssessment(
    { organizationName: orgName, localConnection },
    hotel,
  );
  if (fit?.whyThisHotel) return fit.whyThisHotel;
  const name = text(hotel.displayName || "Hotel");
  return `${name} is in the same commercial market as documented ${orgName} activity. No confirmed room block from public research alone.`;
}

/**
 * Normalize saved reconciliation rows or Exa hits into a common hit shape.
 */
export function normalizeDiscoveryHits(rawHits = []) {
  return (rawHits || [])
    .map((h) => {
      const note = text(h.note || "");
      const excerptRaw = text(h.excerpt || "");
      // Never promote internal reconciliation notes into discovery excerpts.
      const noteIsOps =
        /Requires qualification|before publication|DISCOVERY_HIT|HOLD_CONTACT|REVIEWED_PACKAGE/i.test(
          note,
        );
      const excerptIsOps =
        /Requires qualification|before publication|DISCOVERY_HIT|HOLD_CONTACT|REVIEWED_PACKAGE/i.test(
          excerptRaw,
        );
      return {
        title: text(h.title || h.organizationName || ""),
        excerpt: excerptIsOps ? "" : excerptRaw || (noteIsOps ? "" : note),
        url: h.url || h.sourceUrl || null,
        publisher: h.publisher || null,
        pathway: h.pathway || DISCOVERY_PATHWAY.TRIGGER_LED,
        researchQuestion: h.researchQuestion || null,
        independent: h.independent === true,
      };
    })
    .filter((h) => h.url || h.title);
}

/**
 * Assess one discovery hit through identity + mechanism screens.
 */
export function assessScalingDiscoveryHit(hit, hotel, existingOpportunities = []) {
  const stagePath = [FUNNEL_STAGE.RAW_DISCOVERY_HIT];
  const url = hit.url || "";

  if (FALSE_POSITIVE_URL.test(url)) {
    return {
      stage: FUNNEL_STAGE.EXCLUDED,
      stagePath,
      candidateId: `sc_${hashKey(url || hit.title)}`,
      organizationName: null,
      sourceUrl: url || null,
      rejectReasons: ["directory_hiring_aggregator_or_generic_guide"],
      countsAsGenuinelyNetNew: false,
      researchEligible: false,
    };
  }

  const resolved = resolveScalingOrganizationFromHit(hit);
  const parsed = {
    organizationName: resolved.organizationName,
    programName: resolved.programName || hit.title,
    eventYear: resolved.eventYear || null,
    demandSignalSummary:
      resolved.demandSignalSummary || hit.excerpt || hit.title,
    sourceUrl: url || null,
    identityMethod: resolved.method,
  };
  if (
    !parsed.organizationName ||
    WEAK_ORG.test(parsed.organizationName) ||
    COMPETITOR_OR_VENDOR_ORG.test(parsed.organizationName) ||
    isWeakOrGenericCandidate(parsed)
  ) {
    return {
      stage: FUNNEL_STAGE.EXCLUDED,
      stagePath,
      candidateId: `sc_${hashKey(url || hit.title)}`,
      organizationName: parsed.organizationName,
      sourceUrl: url || null,
      rejectReasons: ["weak_or_generic_identity"],
      identityMethod: resolved.method,
      countsAsGenuinelyNetNew: false,
      researchEligible: false,
    };
  }
  stagePath.push(FUNNEL_STAGE.IDENTITY_VERIFIED);

  const matches = findExistingScalingCompanyMatches(
    existingOpportunities,
    parsed.organizationName,
    null,
  );
  if (matches.length) {
    return {
      stage: FUNNEL_STAGE.EXISTING_MATCH,
      stagePath: [...stagePath, FUNNEL_STAGE.EXISTING_MATCH],
      candidateId: `sc_${hashKey(`${parsed.organizationName}::${url}`)}`,
      organizationName: parsed.organizationName,
      sourceUrl: url || null,
      existingIds: matches.map((m) => m.id).filter(Boolean),
      countsAsGenuinelyNetNew: false,
      researchEligible: false,
      rejectReasons: ["already_in_hotel_scaling_inventory"],
    };
  }

  const localConnection = inferLocalConnectionForHotel(hit, parsed, hotel);
  const hotelUse = inferHotelUseForHotel(hit, parsed, hotel);
  const fit = hotelFitRationale(parsed.organizationName, localConnection, hotel);

  const draft = {
    pathway: hit.pathway,
    segment: "scaling_companies",
    organizationName: parsed.organizationName,
    programName: parsed.programName || hit.title,
    demandMechanism: parsed.programName || hit.title,
    demandSignalSummary: parsed.demandSignalSummary || hit.excerpt,
    sourceUrl: url,
    localConnection,
    datedGrowthEvidence: hit.excerpt || hit.title,
    growthEvidenceDate: parsed.eventYear ? String(parsed.eventYear) : null,
    proposedHotelUse: hotelUse,
    hotelFitRationale: fit,
    whyThisHotel: fit,
  };

  const hotelCfg = {
    city: hotel.city || resolveScalingCompaniesResearchCity(hotel),
    displayName: hotel.displayName,
    demandTerritory: hotel.demandTerritory,
    capabilityProfile: hotel.capabilityProfile,
    commercialPriorities: hotel.commercialPriorities,
    hotelId: hotel.hotelId,
  };

  const v5 = qualifyV5DiscoveryCandidate(
    {
      pathway: hit.pathway,
      segment: "scaling_companies",
      organizationName: parsed.organizationName,
      programName: draft.programName,
      demandMechanism: draft.demandMechanism,
      demandSignalSummary: draft.demandSignalSummary,
      sourceUrl: url,
      eventYear: parsed.eventYear,
    },
    hotel,
    null,
  );
  const scalingQ = qualifyScalingCompanyCandidate(draft, hotelCfg);
  const mechanismOk = Boolean(localConnection && hotelUse && (scalingQ.ok || scalingQ.failReasons?.length <= 2));
  if (mechanismOk) stagePath.push(FUNNEL_STAGE.MECHANISM_SCREENED);

  const researchEligible =
    Boolean(localConnection) &&
    Boolean(hotelUse) &&
    Boolean(fit) &&
    !scalingQ.failReasons?.includes("DIRECTORY_ONLY") &&
    !scalingQ.failReasons?.includes("FUNDING_ONLY") &&
    !scalingQ.failReasons?.includes("HIRING_PAGE_ONLY");

  if (researchEligible) stagePath.push(FUNNEL_STAGE.RESEARCH_ELIGIBLE);

  const genuinelyNetNew = researchEligible && matches.length === 0;
  if (genuinelyNetNew) stagePath.push(FUNNEL_STAGE.GENUINELY_NET_NEW);

  const accepted = v5.accepted === true && scalingQ.ok === true && researchEligible;

  return {
    stage: researchEligible
      ? FUNNEL_STAGE.RESEARCH_ELIGIBLE
      : mechanismOk
        ? FUNNEL_STAGE.MECHANISM_SCREENED
        : FUNNEL_STAGE.IDENTITY_VERIFIED,
    stagePath,
    candidateId: `sc_${hashKey(`${parsed.organizationName}::${url}`)}`,
    pathway: hit.pathway,
    organizationName: parsed.organizationName,
    programName: draft.programName,
    sourceUrl: url || null,
    excerpt: hit.excerpt || null,
    localConnection,
    proposedHotelUse: hotelUse,
    hotelFitRationale: fit,
    datedGrowthEvidence: draft.datedGrowthEvidence,
    growthEvidenceDate: draft.growthEvidenceDate,
    mechanismClass: accepted
      ? DEMAND_MECHANISM_CLASS.INFERRED_DEMAND
      : DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
    scalingQualifyOk: scalingQ.ok === true,
    scalingFailReasons: scalingQ.failReasons || [],
    v5Accepted: v5.accepted === true,
    v5Reject: v5.rejectReasons || [],
    accepted,
    researchEligible,
    countsAsGenuinelyNetNew: genuinelyNetNew,
    draft,
    rejectReasons: researchEligible
      ? []
      : [
          ...(!localConnection ? ["missing_local_connection"] : []),
          ...(!hotelUse ? ["missing_hotel_use_thesis"] : []),
          ...(scalingQ.failReasons || []),
          ...(v5.rejectReasons || []),
        ],
  };
}

export function assessDiscoveryHitInventory(
  rawHits = [],
  hotel = {},
  existingOpportunities = [],
) {
  const hits = normalizeDiscoveryHits(rawHits);
  const assessed = hits.map((h) =>
    assessScalingDiscoveryHit(h, hotel, existingOpportunities),
  );

  // Dedupe by org — keep strongest research-eligible row
  const byOrg = new Map();
  for (const row of assessed) {
    const k = orgKey(row.organizationName);
    if (!k) continue;
    const prev = byOrg.get(k);
    const score = (r) =>
      Number(r.countsAsGenuinelyNetNew) * 4 +
      Number(r.researchEligible) * 2 +
      Number(r.accepted);
    if (!prev || score(row) > score(prev)) {
      if (prev) {
        prev.stage = FUNNEL_STAGE.EXCLUDED;
        prev.rejectReasons = [...(prev.rejectReasons || []), "duplicate_org"];
        prev.countsAsGenuinelyNetNew = false;
        prev.researchEligible = false;
      }
      byOrg.set(k, row);
    } else {
      row.stage = FUNNEL_STAGE.EXCLUDED;
      row.rejectReasons = [...(row.rejectReasons || []), "duplicate_org"];
      row.countsAsGenuinelyNetNew = false;
      row.researchEligible = false;
    }
  }

  const uniqueEligible = [...byOrg.values()]
    .filter((r) => r.researchEligible)
    .sort((a, b) => {
      const rank = (r) =>
        Number(r.accepted) * 8 +
        Number(r.scalingQualifyOk) * 4 +
        Number(Boolean(r.localConnection)) * 2 +
        Number(Boolean(r.proposedHotelUse));
      return rank(b) - rank(a);
    });

  const funnel = {
    RAW_DISCOVERY_HIT: hits.length,
    IDENTITY_VERIFIED: assessed.filter((a) =>
      a.stagePath?.includes(FUNNEL_STAGE.IDENTITY_VERIFIED),
    ).length,
    MECHANISM_SCREENED: assessed.filter((a) =>
      a.stagePath?.includes(FUNNEL_STAGE.MECHANISM_SCREENED),
    ).length,
    RESEARCH_ELIGIBLE: uniqueEligible.length,
    GENUINELY_NET_NEW: uniqueEligible.filter((a) => a.countsAsGenuinelyNetNew).length,
    EXISTING_MATCH: assessed.filter((a) => a.stage === FUNNEL_STAGE.EXISTING_MATCH)
      .length,
    EXCLUDED: assessed.filter((a) => a.stage === FUNNEL_STAGE.EXCLUDED).length,
  };

  return {
    version: SCALING_DISCOVERY_HANDOFF_V1,
    hits: hits.length,
    assessed,
    uniqueEligible,
    shortlist: uniqueEligible.slice(0, 10),
    funnel,
  };
}

async function fetchPageText(url, { allowRender = false } = {}) {
  if (!url) return { text: "", provider: null, url: null };
  try {
    const page = await fetchOfficialPage(url, {
      allowRender: false,
      timeoutMs: 12000,
    });
    let blob = page?.text || page?.searchableText || page?.bodyText || "";
    if ((!blob || blob.length < 80) && allowRender) {
      const rendered = await fetchOfficialPage(url, {
        allowRender: true,
        timeoutMs: 20000,
      });
      blob = rendered?.text || rendered?.searchableText || rendered?.bodyText || "";
    }
    if (blob && blob.length >= 80) {
      return {
        text: String(blob).slice(0, 12000),
        provider: "direct_public",
        url,
      };
    }
  } catch {
    /* offline / blocked */
  }
  return { text: "", provider: null, url };
}

/** Known official hosts for Waterstone shortlist orgs (and similar). */
const OFFICIAL_ORG_HOSTS = Object.freeze({
  "cp group": "https://cpgcre.com",
  cpgcre: "https://cpgcre.com",
  mdvip: "https://www.mdvip.com",
  "wshc+b": "https://www.wsh-law.com",
  "wsh law": "https://www.wsh-law.com",
  "weiss serota": "https://www.wsh-law.com",
  // "Innovative" Boca HQ2 is NOT innovativesolutions.com (Grain Processing, Iowa) —
  // leave unmapped until identity host is confirmed.
  "hair club": "https://www.hairclub.com",
  cresa: "https://www.cresa.com",
  "mplt healthcare": "https://www.mplthealthcare.com",
  mplt: "https://www.mplthealthcare.com",
});

const NEWS_HOST =
  /commercialobserver|prnewswire|selectflorida|therealdeal|sfbwmag|miamiherald|cbs12|citybiz|floridatrend|palmbeachpost|techhubsouthflorida/i;

/**
 * Candidate official contact URLs beyond the discovery hit (news pages lack routes).
 */
export function resolveOfficialContactCandidateUrls(
  organizationName = "",
  sourceUrl = "",
) {
  const urls = [];
  const add = (u) => {
    const t = text(u);
    if (t && /^https?:\/\//i.test(t) && !urls.includes(t)) urls.push(t);
  };
  const orgKey = orgKeyName(organizationName);
  if (OFFICIAL_ORG_HOSTS[orgKey]) add(OFFICIAL_ORG_HOSTS[orgKey]);
  try {
    const u = new URL(sourceUrl);
    const host = u.hostname.replace(/^www\./i, "");
    if (!NEWS_HOST.test(host) && !/linkedin|facebook|twitter/i.test(host)) {
      add(`${u.protocol}//${u.hostname}`);
    }
  } catch {
    /* ignore */
  }
  const bases = [...urls];
  for (const base of bases) {
    const root = base.replace(/\/$/, "");
    add(`${root}/contact`);
    add(`${root}/contact-us`);
    add(`${root}/contact/`);
    add(`${root}/about/contact`);
    // Named-person research — team/leadership pages (not only /contact)
    add(`${root}/about`);
    add(`${root}/about-us`);
    add(`${root}/about/`);
    add(`${root}/team`);
    add(`${root}/our-team`);
    add(`${root}/people`);
    add(`${root}/leadership`);
    add(`${root}/who-we-are`);
  }
  return urls.slice(0, 14);
}

function orgKeyName(name) {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9+]+/g, " ")
    .trim();
}

function pickBestPhone(phones = []) {
  const list = (phones || [])
    .map((p) => text(p))
    .filter((p) => p.replace(/\D/g, "").length >= 10);
  const us = list.find((p) => {
    const d = p.replace(/\D/g, "");
    return d.length === 10 || (d.length === 11 && d.startsWith("1"));
  });
  return us || list[0] || null;
}

function registrableDomain(hostOrEmail = "") {
  const raw = String(hostOrEmail || "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0]
    .split("@")
    .pop();
  const parts = String(raw || "").split(".").filter(Boolean);
  if (parts.length >= 2) return parts.slice(-2).join(".");
  return raw || "";
}

/**
 * Reject emails whose domain clearly does not belong to the official host we fetched
 * (e.g. grainprocessing.com on a mis-mapped innovativesolutions.com page).
 */
export function emailMatchesOfficialHost(email, pageUrl = "", organizationName = "") {
  const em = text(email).toLowerCase();
  if (!em.includes("@")) return false;
  const emailDom = registrableDomain(em);
  if (!emailDom) return false;
  if (isNewsPublisherContactEmail(em, pageUrl)) return false;
  try {
    const pageDom = registrableDomain(new URL(pageUrl).hostname);
    if (pageDom && emailDom !== pageDom) {
      // Allow only when page is clearly the org's own host family
      return false;
    }
  } catch {
    /* ignore */
  }
  const orgHost = OFFICIAL_ORG_HOSTS[orgKeyName(organizationName)];
  if (orgHost) {
    try {
      const expected = registrableDomain(new URL(orgHost).hostname);
      if (expected && emailDom !== expected) return false;
    } catch {
      /* ignore */
    }
  }
  return true;
}

/**
 * Mature a research-eligible shortlist with page fetch + public WHO + ORS.
 * When `researcher` is provided (paid FullE2E), uses live longform; otherwise offline corpus ORS.
 * Does not write Airtable.
 */
export async function matureScalingResearchShortlist({
  shortlist = [],
  hotel = {},
  maxCandidates = 10,
  runOrs = true,
  researcher = null,
  existingOpportunities = [],
  /** When a live researcher is provided, paid Exa/Firecrawl must not default to blocked. */
  remainingBudgetUsd = null,
} = {}) {
  const hotelId = hotel.hotelId;
  const hotelCfg = {
    city: hotel.city || resolveScalingCompaniesResearchCity(hotel),
    displayName: hotel.displayName,
    demandTerritory: hotel.demandTerritory,
    capabilityProfile: hotel.capabilityProfile,
    commercialPriorities: hotel.commercialPriorities,
    hotelId,
  };
  const results = [];
  const corpus = {};

  // Discovery V2 pre-ORS screening — REQUIRED default before ORS.
  // Missing / unloadable module must fail closed (never silent legacy fallback).
  let preOrsMeta = null;
  let orderedShortlist = shortlist.slice();
  let screenScalingDiscoveryBatch;
  let PRE_ORS_DISPOSITION;
  try {
    ({
      screenScalingDiscoveryBatch,
      PRE_ORS_DISPOSITION,
    } = await import("./scaling-companies-pre-ors-screening-v2.js"));
  } catch (err) {
    const fail = new Error(
      `PRE_ORS_V2_REQUIRED: scaling-companies-pre-ors-screening-v2.js failed to load (${err?.message || err}). Refusing to continue with legacy shortlist order.`,
    );
    fail.code = "PRE_ORS_V2_REQUIRED";
    fail.cause = err;
    throw fail;
  }
  if (typeof screenScalingDiscoveryBatch !== "function" || !PRE_ORS_DISPOSITION) {
    const fail = new Error(
      "PRE_ORS_V2_REQUIRED: Discovery V2 exports missing — refusing lower-quality screening.",
    );
    fail.code = "PRE_ORS_V2_REQUIRED";
    throw fail;
  }
  const batch = screenScalingDiscoveryBatch({
    candidates: shortlist,
    hotel,
    existingOpportunities,
    maxOrs: maxCandidates,
  });
  preOrsMeta = {
    available: true,
    required: true,
    version: batch.version,
    byDisposition: batch.byDisposition,
    metrics: batch.metrics,
    orsQueueOrgs: (batch.orsQueue || []).map((s) => s.organizationName),
  };
  const redirected = (batch.screens || []).filter(
    (s) => s.disposition === PRE_ORS_DISPOSITION.BETTER_FIT_OTHER_SEGMENT,
  );
  const findScreen = (cand) => {
    const k = orgKeyName(cand.organizationName);
    const name = text(cand.organizationName).toLowerCase();
    return (
      (batch.screens || []).find((s) => s.orgKey === k) ||
      (batch.screens || []).find(
        (s) => text(s.organizationName).toLowerCase() === name,
      ) ||
      null
    );
  };
  // Prefer V2-ranked eligible orgs; keep unknown-identity rows for light research.
  const ranked = [];
  const deferred = [];
  for (const cand of shortlist) {
    const screen = findScreen(cand);
    if (screen?.orsEligible) ranked.push({ ...cand, preOrsScreen: screen });
    else if (
      screen?.disposition === PRE_ORS_DISPOSITION.NEEDS_IDENTITY_RESEARCH ||
      screen?.disposition === PRE_ORS_DISPOSITION.NEEDS_EXPANSION_EVIDENCE
    ) {
      deferred.push({ ...cand, preOrsScreen: screen });
    } else if (screen) {
      results.push({
        candidateId: cand.candidateId || hashKey(cand.organizationName || cand.sourceUrl),
        organizationName: cand.organizationName,
        sourceUrl: cand.sourceUrl,
        countsAsGenuinelyNetNew: false,
        preOrsScreen: screen,
        ors: { attempted: false, ok: false, skippedReason: screen.disposition },
        publicationAuditLabel: "EXCLUDE",
        hotelFacingEligible: false,
        opportunityPayload: null,
        salesBrief: null,
        whoStatus: "SKIPPED_PRE_ORS",
        alternateSegment: screen.alternateSegment || null,
      });
    } else {
      deferred.push(cand);
    }
  }
  orderedShortlist = [...ranked, ...deferred];
  preOrsMeta.redirectedCount = redirected.length;
  preOrsMeta.skippedBeforeOrs = results.length;

  for (const cand of orderedShortlist.slice(0, maxCandidates)) {
    const page = await fetchPageText(cand.sourceUrl);
    let pageText = page.text || cand.excerpt || "";
    if (pageText && cand.sourceUrl) corpus[cand.sourceUrl] = pageText;

    let contactEmail = null;
    let contactPhone = null;
    let contactUrl = null;
    let contactPagesExamined = [];
    let extractedRejected = [];
    let namedStakeholders = [];
    let whoStatus = "NOT_RUN";
    const evidencePages = [];

    const absorbExtraction = (extracted, pageUrl, { isOfficialContactPage = false } = {}) => {
      if (!extracted || extracted.charCount < 40) return;
      contactPagesExamined.push({
        url: pageUrl,
        emails: extracted.emails || [],
        phones: extracted.phones || [],
        preferredEmail: extracted.preferredEmail || null,
        hasContactLanguage: extracted.hasContactLanguage === true,
        charCount: extracted.charCount,
        isOfficialContactPage,
      });
      let pref =
        extracted.preferredEmail || selectPreferredPublicEmail(extracted.emails);
      if (pref && isNewsPublisherContactEmail(pref, pageUrl)) {
        extractedRejected.push({
          value: pref,
          reason: "news_publisher_or_press_mailbox",
          pageUrl,
        });
        pref = null;
      }
      if (pref && !emailMatchesOfficialHost(pref, pageUrl, cand.organizationName)) {
        extractedRejected.push({
          value: pref,
          reason: "email_domain_mismatch_vs_official_host",
          pageUrl,
        });
        pref = null;
      }
      // Prefer official contact-page routes over news-page residue
      if (pref) {
        if (!contactEmail || isOfficialContactPage) contactEmail = pref;
      }
      const phone = pickBestPhone(extracted.phones);
      if (phone) {
        if (!contactPhone || isOfficialContactPage) contactPhone = phone;
      }
      if (
        extracted.hasContactLanguage &&
        pageUrl &&
        /contact/i.test(pageUrl)
      ) {
        contactUrl = pageUrl;
      }
      evidencePages.push({
        url: pageUrl,
        text: extracted.excerpt || pageText.slice(0, 4000),
        quote: extracted.excerpt,
        statement: extracted.excerpt,
        excerpt: extracted.excerpt,
      });
    };

    if (pageText.length >= 40) {
      absorbExtraction(
        extractPublicEvidenceFromText(pageText, cand.sourceUrl),
        cand.sourceUrl,
        { isOfficialContactPage: false },
      );
    }

    // Official contact pages — discovery news URLs rarely carry org-owned routes
    const contactCandidates = resolveOfficialContactCandidateUrls(
      cand.organizationName,
      cand.sourceUrl,
    );
    // Scan contact + team/leadership pages. Do not stop after the first org
    // route — early exit previously left namedStakeholders empty/single and
    // parked the only person in secondaryContact.
    let teamPagesScanned = 0;
    for (const contactPageUrl of contactCandidates) {
      if (contactPageUrl === cand.sourceUrl) continue;
      const isTeamPath =
        /\/(about|about-us|team|our-team|people|leadership|who-we-are)(\/|$)/i.test(
          contactPageUrl,
        );
      const isContactPath = /contact/i.test(contactPageUrl);
      // Cap team fetches once we already have two named people from prior pages
      if (isTeamPath && teamPagesScanned >= 3 && namedStakeholders.length >= 2) {
        continue;
      }
      const contactPage = await fetchPageText(contactPageUrl, {
        allowRender: /cresa|mdvip/i.test(contactPageUrl),
      });
      if (contactPage.text.length < 80) continue;
      corpus[contactPageUrl] = contactPage.text;
      if (isTeamPath) teamPagesScanned += 1;
      absorbExtraction(
        extractPublicEvidenceFromText(contactPage.text, contactPageUrl),
        contactPageUrl,
        { isOfficialContactPage: isContactPath },
      );
      // Incremental WHO pass so namedStakeholders length guides team-page budget
      if (evidencePages.length) {
        const interim = runPublicBuyerIntelligencePass({
          evidence: evidencePages,
          organizationName: cand.organizationName,
          existingContactEmail: contactEmail,
          segmentId: "scaling_companies",
        });
        namedStakeholders = interim.namedStakeholders || namedStakeholders;
      }
    }

    if (evidencePages.length) {
      const buyer = runPublicBuyerIntelligencePass({
        evidence: evidencePages,
        organizationName: cand.organizationName,
        existingContactEmail: contactEmail,
        segmentId: "scaling_companies",
      });
      whoStatus = buyer.whoResearchStatus;
      namedStakeholders = buyer.namedStakeholders || [];
      const buyerRoute = buyer.contactOwnership?.route || null;
      if (
        !contactEmail &&
        buyerRoute &&
        !isNewsPublisherContactEmail(buyerRoute, cand.sourceUrl)
      ) {
        contactEmail = buyerRoute;
      }
      if (!contactPhone && buyer.contactOwnership?.phone) {
        contactPhone = buyer.contactOwnership.phone;
      }
    } else {
      whoStatus = "ZERO_PAGES_SCANNED";
    }

    // Enrich local/use from page when missing
    let localConnection = cand.localConnection;
    let hotelUse = cand.proposedHotelUse;
    if (!localConnection && pageText) {
      localConnection = inferLocalConnectionForHotel(
        { title: cand.programName, excerpt: pageText.slice(0, 500), url: cand.sourceUrl },
        { organizationName: cand.organizationName, programName: cand.programName },
        hotel,
      );
    }
    if (!hotelUse && pageText) {
      hotelUse = inferHotelUseForHotel(
        { title: cand.programName, excerpt: pageText.slice(0, 500) },
        { programName: cand.programName },
        hotel,
      );
    }
    const fit =
      cand.hotelFitRationale ||
      hotelFitRationale(cand.organizationName, localConnection, hotel);

    const routeHint = contactEmail || contactPhone || contactUrl;
    const cardInput = {
      organizationName: cand.organizationName,
      localConnection,
      datedGrowthEvidence:
        cand.datedGrowthEvidence || cand.excerpt || pageText.slice(0, 400),
      growthEvidenceDate: cand.growthEvidenceDate,
      proposedHotelUse: hotelUse,
      hotelFitRationale: fit,
      whyThisHotel: fit,
      sourceUrl: cand.sourceUrl,
      sources: [
        cand.sourceUrl
          ? { url: cand.sourceUrl, type: "growth_evidence", title: "Growth evidence" }
          : null,
        ...contactPagesExamined
          .filter((p) => p.url && p.url !== cand.sourceUrl)
          .map((p) => ({
            url: p.url,
            type: "contact_page",
            title: "Official contact page",
          })),
      ].filter(Boolean),
      contactEmail,
      contactPhone,
      contactUrl,
      website: contactUrl || OFFICIAL_ORG_HOSTS[orgKeyName(cand.organizationName)] || null,
      contactName:
        namedStakeholders.find((s) => looksLikePersonName(s?.name))?.name || null,
      contactRole:
        namedStakeholders.find((s) => looksLikePersonName(s?.name))?.title ||
        "workplace_operations",
      establishedFacts: [
        cand.programName ? `Program/activity signal: ${cand.programName}` : null,
        localConnection ? `Local connection: ${localConnection}` : null,
      ].filter(Boolean),
      inferredHotelDemand: [
        hotelUse ||
          "Potential lodging/meeting demand inferred from expansion/project signal — not a verified room block",
      ],
      whyNow:
        "Establish corporate relationship while local growth/project activity is observable",
      recommendedAction: routeHint
        ? `Contact ${namedStakeholders.find((s) => looksLikePersonName(s?.name))?.name || "workplace/travel/meetings owner"} via ${contactEmail || contactPhone || contactUrl} to introduce ${hotel.displayName} for visiting staff lodging and meetings tied to ${cand.organizationName}'s local activity; ask how overnight and meeting needs are currently handled.`
        : `Identify workplace operations / travel / meetings owner at ${cand.organizationName} and introduce ${hotel.displayName} for visiting-staff lodging near ${hotelCfg.city}; ask how current lodging is arranged.`,
    };

    const card = buildScalingCompanyOpportunityCard(cardInput, {
      hotelId,
      hotelConfig: hotelCfg,
    });

    let ors = { attempted: false, ok: false, offline: true };
    let maturedOpp = card.ok ? card.opportunity : null;
    // Research-first: UNKNOWN / expansion-deferred candidates keep a continuation
    // payload even when the opportunity card gate fails — do not silent-drop.
    if (
      !maturedOpp &&
      (cand.preOrsScreen?.disposition ===
        PRE_ORS_DISPOSITION.NEEDS_IDENTITY_RESEARCH ||
        cand.preOrsScreen?.disposition ===
          PRE_ORS_DISPOSITION.NEEDS_EXPANSION_EVIDENCE)
    ) {
      maturedOpp = {
        id: `gdi_sc_rc_${hashKey(`${cand.organizationName}|${cand.sourceUrl || ""}`)}`,
        organizationName: cand.organizationName,
        researchSegment: "scaling_companies",
        hotelId,
        whatIsHappening:
          cardInput.datedGrowthEvidence || cand.excerpt || cand.programName || null,
        localConnection: localConnection || null,
        proposedHotelUse: hotelUse || null,
        hotelFitRationale: fit || null,
        sourceUrl: cand.sourceUrl || null,
        sources: cardInput.sources || [],
        namedStakeholders: (namedStakeholders || []).filter((s) =>
          looksLikePersonName(s?.name),
        ),
        contactEmail: contactEmail || null,
        contactPhone: contactPhone || null,
        contactUrl: contactUrl || null,
        preserveOpportunity: true,
        researchContinuation: true,
        scalingCommercialDisposition: "RESEARCH_CONTINUATION",
      };
    }

    // Research-first: NEEDS_IDENTITY / expansion-deferred rows may still receive
    // ORS / longform when a researcher or corpus is available — do not ORS-starve
    // continuation candidates solely because the card gate failed.
    const continuationOrsEligible =
      cand.preOrsScreen?.disposition ===
        PRE_ORS_DISPOSITION.NEEDS_IDENTITY_RESEARCH ||
      cand.preOrsScreen?.disposition ===
        PRE_ORS_DISPOSITION.NEEDS_EXPANSION_EVIDENCE;
    const canOrs =
      runOrs &&
      maturedOpp &&
      (card.ok || continuationOrsEligible) &&
      (researcher || Object.keys(corpus).length > 0);
    if (canOrs) {
      const useLive = Boolean(researcher);
      try {
        // run-050 addendum: Scaling previously omitted paidBlocked:false, so live
        // researchers still starved Exa/Firecrawl (deep-research defaults paidBlocked=true).
        const liveBudget =
          remainingBudgetUsd == null
            ? useLive
              ? 1
              : 0
            : Number(remainingBudgetUsd);
        const deep = await runDeepResearchOnExistingGdiOpportunity({
          opportunity: {
            ...maturedOpp,
            hotelId,
            hotelName: hotel.displayName,
            researchSegment: "scaling_companies",
          },
          researcher: useLive ? researcher : null,
          corpus: Object.keys(corpus).length ? corpus : undefined,
          offlineLongform: !useLive,
          options: {
            authorizeSurfe: false,
            authorizePdl: false,
            paidBlocked: !(useLive && liveBudget > 0.05),
            remainingBudgetUsd: Math.max(0, liveBudget),
            maxIterations: 6,
            executeLeadFollowUps: true,
            maxLeadFollowUpsPerOpportunity: 4,
            allowReplan: true,
            allowVoiContinue: true,
            allowResearchActionContract: true,
          },
        });
        ors = {
          attempted: true,
          ok: deep?.ok !== false,
          offline: !useLive,
          live: useLive,
          error: deep?.error || null,
          stopReason:
            deep?.after?.orsStopReason ||
            deep?.stopReason ||
            deep?.stoppingReason ||
            null,
        };
        maturedOpp = opportunityFromDeepResearchResult(deep, maturedOpp);
        // Prefer ORS-refined contact when present and not a news-publisher mailbox
        const orsEmail =
          maturedOpp?.contactEmail ||
          maturedOpp?.primaryContact?.email ||
          null;
        if (
          orsEmail &&
          !isNewsPublisherContactEmail(orsEmail, cand.sourceUrl)
        ) {
          contactEmail = orsEmail;
        }
        if (
          Array.isArray(maturedOpp?.namedStakeholders) &&
          maturedOpp.namedStakeholders.length
        ) {
          namedStakeholders = maturedOpp.namedStakeholders;
        }
      } catch (err) {
        ors = {
          attempted: true,
          ok: false,
          offline: !useLive,
          live: useLive,
          error: err.message,
        };
      }
    }

    if (maturedOpp && (contactEmail || contactPhone || contactUrl)) {
      maturedOpp = {
        ...maturedOpp,
        contactEmail: contactEmail || maturedOpp.contactEmail || null,
        contactPhone: contactPhone || maturedOpp.contactPhone || null,
        contactUrl: contactUrl || maturedOpp.contactUrl || null,
        primaryContactPhone: contactPhone || maturedOpp.primaryContactPhone || null,
        functionalContactEmail: contactEmail || maturedOpp.functionalContactEmail || null,
        contactName:
          namedStakeholders[0]?.name || maturedOpp.contactName || null,
        contactRole:
          namedStakeholders[0]?.title ||
          maturedOpp.contactRole ||
          "workplace_operations",
        localConnection: localConnection || maturedOpp.localConnection,
        proposedHotelUse: hotelUse || maturedOpp.proposedHotelUse,
        hotelFitRationale: fit || maturedOpp.hotelFitRationale,
        namedStakeholders,
        organizationName: cand.organizationName || maturedOpp.organizationName,
      };
      // Named secondary / referral — does not overwrite primary; never blocks publication
      maturedOpp = attachSecondaryNamedContact(maturedOpp, {
        candidates: namedStakeholders,
      });
    } else if (maturedOpp) {
      maturedOpp = attachSecondaryNamedContact(
        {
          ...maturedOpp,
          namedStakeholders,
          organizationName: cand.organizationName || maturedOpp.organizationName,
        },
        { candidates: namedStakeholders },
      );
    }

    // Final hotel-sales presentation choke point BEFORE publication payload:
    // scrub ORS/internal language, rebuild sales narratives, gate secondary
    // without usable route. Missing secondary never blocks.
    if (maturedOpp) {
      maturedOpp = applyScalingTwoContactStandard(
        applyScalingSalesPresentation(maturedOpp, {
          hotelDisplayName: hotel.displayName || hotelCfg.displayName,
          hotel: hotel || hotelCfg,
          applyCommercialIntelligenceBrief: true,
        }),
        { force: true, candidates: namedStakeholders },
      );
      // Claim-to-source integrity — hold/continue when high-severity defects;
      // two named people are never a hard publication gate.
      const evidenceAudit = auditScalingClaimToSource(maturedOpp, {
        hotelId,
        city: hotelCfg.city || hotel.city,
      });
      const requal = requalifyScalingOpportunity(maturedOpp, evidenceAudit);
      maturedOpp = {
        ...maturedOpp,
        scalingEvidenceIntegrityVersion: evidenceAudit.version,
        scalingEvidenceFindings: evidenceAudit.findings,
        scalingEvidenceIntegrityPass: evidenceAudit.evidenceIntegrityPass,
        scalingCommercialDisposition:
          requal.disposition || maturedOpp.scalingCommercialDisposition || null,
      };
      if (
        requal.customerFacingEligible === false &&
        /HOLD_|EXCLUDE|BETTER_FIT/i.test(requal.disposition || "")
      ) {
        maturedOpp.customerVisible = false;
        maturedOpp.hotelFacingEligible = false;
      }
    }

    const baseElig = maturedOpp
      ? evaluateProspectPublicationEligibility(maturedOpp, { hotelId })
      : { hotelFacing: false };
    const elig = maturedOpp
      ? applyScalingPublicationGateToEligibility(baseElig, maturedOpp)
      : baseElig;
    const scalingQual = maturedOpp
      ? evaluateScalingPublicationQualification(maturedOpp)
      : null;
    const routeCheck = maturedOpp
      ? hasUsableContactRoute(maturedOpp)
      : { ok: false, reason: "no_opportunity" };

    let pubLabel = "EXCLUDE";
    // Parity with Bethesda/Wyndham: email OR phone OR functional contact URL
    // (not email-only). News-publisher mailboxes remain rejected upstream.
    // Founder rule: PASS_PUBLICATION_READY requires one named relevant buyer
    // + demand thesis — org route alone is ACCOUNT_DEVELOPMENT / buyer research.
    const usableOrgContact = routeCheck.ok === true;
    if (card.ok && usableOrgContact && localConnection && hotelUse) {
      pubLabel = "HOLD_ACCOUNT_DEVELOPMENT_INTERNAL";
      if (
        elig.hotelFacing &&
        ors.ok &&
        scalingQual?.publicationQualified === true
      ) {
        pubLabel = "PASS_PUBLICATION_READY_INTERNAL";
      } else if (
        scalingQual?.status === "RESEARCH_CONTINUATION_BUYER" ||
        scalingQual?.gaps?.includes("ONE_NAMED_RELEVANT_BUYER")
      ) {
        pubLabel = "HOLD_BUYER_RESEARCH_CONTINUATION";
      }
    } else if (card.ok && !usableOrgContact) pubLabel = "HOLD_CONTACT";
    else if (card.ok && !localConnection) pubLabel = "HOLD_HOTEL_FIT";
    else if (!card.ok) {
      if (
        maturedOpp &&
        (cand.preOrsScreen?.disposition ===
          PRE_ORS_DISPOSITION.NEEDS_IDENTITY_RESEARCH ||
          cand.preOrsScreen?.disposition ===
            PRE_ORS_DISPOSITION.NEEDS_EXPANSION_EVIDENCE)
      ) {
        pubLabel = "HOLD_RESEARCH_CONTINUATION";
      } else {
        pubLabel = "EXCLUDE";
      }
    }

    const stage =
      card.ok || pubLabel === "HOLD_RESEARCH_CONTINUATION"
        ? pubLabel === "PASS_PUBLICATION_READY_INTERNAL"
          ? FUNNEL_STAGE.PUBLICATION_READY
          : FUNNEL_STAGE.QUALIFIED_ACCOUNT_DEVELOPMENT
        : FUNNEL_STAGE.EXCLUDED;

    results.push({
      candidateId: cand.candidateId,
      organizationName: cand.organizationName,
      sourceUrl: cand.sourceUrl,
      stage,
      researchEligible: true,
      countsAsGenuinelyNetNew: cand.countsAsGenuinelyNetNew === true && card.ok === true,
      scalingCardOk: card.ok === true,
      failReasons: card.failReasons || cand.scalingFailReasons || [],
      pageProvider: page.provider,
      pageChars: pageText.length,
      whoStatus,
      namedStakeholders,
      contactEmail,
      contactPhone,
      contactUrl,
      contactRouteReason: routeCheck.reason || null,
      contactPagesExamined,
      extractedRejected,
      ors,
      publicationAuditLabel: pubLabel,
      hotelFacingEligible:
        elig.hotelFacing === true && scalingQual?.publicationQualified === true,
      scalingPublicationQualified: scalingQual?.publicationQualified === true,
      scalingQualificationStatus: scalingQual?.status || null,
      researchContinuation: scalingQual?.researchContinuation === true,
      opportunityPayload:
        maturedOpp &&
        (card.ok || pubLabel === "HOLD_RESEARCH_CONTINUATION")
        ? {
            ...maturedOpp,
            researchSegment: "scaling_companies",
            segment: "Scaling Companies / Emerging Corporate Accounts",
            customerVisible: false,
            prospectHotelFacing: false,
            prospectPublicationBasis: null,
            customerFacingState: "INTERNAL_ONLY",
            publicationAuditLabel: pubLabel,
            scalingPublicationQualification: scalingQual || null,
            researchVersion: SCALING_COMPANIES_RESEARCH_VERSION,
            discoveryHandoffVersion: SCALING_DISCOVERY_HANDOFF_V1,
          }
        : null,
      salesBrief:
        maturedOpp &&
        (card.ok || pubLabel === "HOLD_RESEARCH_CONTINUATION")
        ? {
            organization: cand.organizationName,
            activity: localConnection,
            whyThisHotel: fit,
            contactRoute: contactEmail || contactPhone || contactUrl,
            contactRouteClass: routeCheck.reason || null,
            recommendedOutreach: cardInput.recommendedAction,
            uncertainties: [
              "Travel volume not verified",
              "Hotel-selection decision-maker may be unconfirmed",
              whoStatus === "ZERO_PAGES_SCANNED"
                ? "WHO did not scan usable page text"
                : null,
              contactEmail && /^(info|contact|hello|hq|customerservice)@/i.test(contactEmail)
                ? "General/functional org route — account intro routing, not verified hotel buyer"
                : null,
            ].filter(Boolean),
            publicationReadiness: pubLabel,
          }
        : null,
    });
  }

  return {
    version: SCALING_DISCOVERY_HANDOFF_V1,
    matured: results,
    preOrsScreening: preOrsMeta,
    funnel: {
      RESEARCHED_BY_ORS: results.filter((r) => r.ors?.attempted).length,
      QUALIFIED_ACCOUNT_DEVELOPMENT: results.filter(
        (r) => r.scalingCardOk && r.publicationAuditLabel !== "EXCLUDE",
      ).length,
      PUBLICATION_READY: results.filter(
        (r) => r.publicationAuditLabel === "PASS_PUBLICATION_READY_INTERNAL",
      ).length,
      withContact: results.filter((r) => {
        if (!r.opportunityPayload) return false;
        return hasUsableContactRoute(r.opportunityPayload).ok === true;
      }).length,
      buyerIntelRun: results.filter((r) => r.whoStatus && r.whoStatus !== "NOT_RUN")
        .length,
      PRE_ORS_SKIPPED: results.filter((r) => r.whoStatus === "SKIPPED_PRE_ORS").length,
    },
  };
}

/**
 * End-to-end: assess hits → shortlist → mature (saved-evidence / offline).
 */
export async function runScalingDiscoveryHandoffPipeline({
  rawHits = [],
  hotel = {},
  existingOpportunities = [],
  maxCandidates = 10,
  runOrs = true,
  researcher = null,
  remainingBudgetUsd = null,
} = {}) {
  const assessed = assessDiscoveryHitInventory(
    rawHits,
    hotel,
    existingOpportunities,
  );
  const matured = await matureScalingResearchShortlist({
    shortlist: assessed.shortlist,
    hotel,
    maxCandidates,
    runOrs,
    researcher,
    existingOpportunities,
    remainingBudgetUsd,
  });
  return {
    version: SCALING_DISCOVERY_HANDOFF_V1,
    assessment: assessed,
    maturation: matured,
    preOrsScreening: matured.preOrsScreening || null,
    honestMetrics: {
      rawDiscoveryHits: assessed.funnel.RAW_DISCOVERY_HIT,
      identityVerified: assessed.funnel.IDENTITY_VERIFIED,
      mechanismScreened: assessed.funnel.MECHANISM_SCREENED,
      researchEligible: assessed.funnel.RESEARCH_ELIGIBLE,
      genuinelyNetNew: assessed.funnel.GENUINELY_NET_NEW,
      existingMatch: assessed.funnel.EXISTING_MATCH,
      excluded: assessed.funnel.EXCLUDED,
      researchedByOrs: matured.funnel.RESEARCHED_BY_ORS,
      qualifiedAccountDevelopment: matured.funnel.QUALIFIED_ACCOUNT_DEVELOPMENT,
      publicationReadyInternal: matured.funnel.PUBLICATION_READY,
      withUsableContact: matured.funnel.withContact,
      preOrsSkipped: matured.funnel.PRE_ORS_SKIPPED || 0,
      note: "RAW_DISCOVERY_HIT must never be reported as opportunity count or NET_NEW. Pre-ORS V2 redirects unsuitable entities before paid ORS when the module is present.",
    },
  };
}
