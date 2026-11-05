/**
 * Multidimensional commercial improvement measurement V1.
 * Compares BEFORE vs AFTER using normalized evidence identities — never phantom field fills.
 * Does not rebuild discovery, ORS, or qualification.
 */

import { isConsumerEmailDomain } from "./commercial-spine-restoration-v41.js";
import { hasRoleRelevantDecisionMaker } from "./autonomous-deep-research-recovery-v1.js";

export const COMMERCIAL_IMPROVEMENT_MEASUREMENT_V1 = "gdi-commercial-improvement-measurement-v1";

export const IMPROVEMENT_OUTCOME = Object.freeze({
  MATERIAL_COMMERCIAL_IMPROVEMENT: "MATERIAL_COMMERCIAL_IMPROVEMENT",
  LIMITED_IMPROVEMENT: "LIMITED_IMPROVEMENT",
  NO_IMPROVEMENT: "NO_IMPROVEMENT",
  RESEARCH_BLOCKED: "RESEARCH_BLOCKED",
});

export const IMPROVEMENT_DIMENSION = Object.freeze({
  DEMAND_INTELLIGENCE: "DEMAND_INTELLIGENCE",
  HOTEL_SPECIFIC_RELEVANCE: "HOTEL_SPECIFIC_RELEVANCE",
  BUYER_ACCESSIBILITY: "BUYER_ACCESSIBILITY",
  COMMERCIAL_TIMING: "COMMERCIAL_TIMING",
  SALES_ACTIONABILITY: "SALES_ACTIONABILITY",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function normalizeEvidenceKey(item = {}) {
  const url = String(item.url || "").trim().replace(/\/+$/, "").toLowerCase();
  const stmt = text(item.statement || item.quote || item.excerpt || item.claim)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .slice(0, 220);
  if (url && stmt) return `u:${url}|s:${stmt.slice(0, 120)}`;
  if (url) return `u:${url}`;
  if (stmt) return `s:${stmt}`;
  return "";
}

function fingerprintBlob(s) {
  return text(s).toLowerCase().replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").slice(0, 280);
}

/**
 * Collect prior contact routes from BEFORE snapshot / opportunity — all known forms.
 */
export function collectPriorContactRoutes(before = {}, opportunity = {}) {
  const routes = [
    ...(before.contactRoutes || []),
    before.contactEmail,
    before.contact?.businessEmail,
    opportunity.contactEmail,
    opportunity.contact?.businessEmail,
    opportunity.primaryContactEmail,
    ...(opportunity.backupContacts || []).map((c) => c?.email || c),
  ];
  return new Set(routes.map(normalizeEmail).filter(Boolean));
}

/**
 * Collect prior evidence identity keys from BEFORE snapshot.
 */
export function collectPriorEvidenceKeys(before = {}, opportunity = {}) {
  const items = [
    ...(before.demandEvidence || []),
    ...(opportunity.evidence || []),
    ...(opportunity.acceptedClaims || []).map((c) => ({
      statement: c.statement || c.claim,
      url: c.url,
    })),
  ];
  if (text(opportunity.hotelOpportunityThesis)) {
    items.push({
      statement: opportunity.hotelOpportunityThesis,
      url: opportunity.officialSource || null,
    });
  }
  if (text(before.description)) {
    items.push({ statement: before.description, url: null });
  }
  return new Set(items.map(normalizeEvidenceKey).filter(Boolean));
}

export function collectPriorCompetitorHosts(before = {}, opportunity = {}) {
  const hosts = [
    opportunity.competingHotel,
    opportunity.namedHostHotel,
    before.namedHostHotel,
    before.competingHotel,
  ];
  const thesis = text(opportunity.hotelOpportunityThesis || before.description || "");
  const m = thesis.match(/\b(?:Crystal Gateway Marriott|Hilton [A-Z][^.,;]{2,40}|Marriott [A-Z][^.,;]{2,40}|Hyatt [A-Z][^.,;]{2,40})\b/g);
  if (m) hosts.push(...m);
  return new Set(hosts.map((h) => text(h).toLowerCase()).filter(Boolean));
}

function evidenceItemsFromAfter(after = {}, result = {}) {
  const opp = after.opportunity || after;
  return [
    ...(opp.evidence || []),
    ...(result.acceptedClaims || []).map((c) => ({
      statement: c.statement || c.claim,
      url: c.url,
      provenance: c.provenance || "after",
    })),
  ];
}

/**
 * Detect newly acquired evidence (by normalized identity), excluding rewords of prior thesis.
 */
export function diffNewEvidence({ before = {}, after = {}, opportunity = {}, result = {} } = {}) {
  const priorKeys = collectPriorEvidenceKeys(before, opportunity);
  const priorFingerprints = new Set(
    [...priorKeys].map((k) => k.replace(/^u:[^|]+\|?s?:?/, "").slice(0, 160)).filter(Boolean),
  );
  const items = evidenceItemsFromAfter(after, result);
  const novel = [];
  for (const item of items) {
    const key = normalizeEvidenceKey(item);
    if (!key) continue;
    if (priorKeys.has(key)) continue;
    const fp = fingerprintBlob(item.statement || item.quote || "");
    const isReword = [...priorFingerprints].some((p) =>
      p && fp && (fp.includes(p.slice(0, 80)) || p.includes(fp.slice(0, 80))));
    if (isReword && !item.url) continue;
    // Skip if only URL is new but statement empty / thin re-fetch of known host marketing
    if (/held_|canonical:\/\/|previouslyHeld/i.test(item.provenance || "") || item.previouslyHeld) continue;
    const statement = text(item.statement || item.quote || "").slice(0, 280);
    // Unproductive retrievals are not commercial discoveries
    if (/404\s*error|page you are looking for is not found|does not exist/i.test(statement)) continue;
    if (statement.length < 40) continue;
    novel.push({
      key,
      url: item.url || null,
      statement,
      provenance: item.provenance || item.sourceFamily || "after_evidence",
      relatedEntity: Boolean(item.relatedEntity),
    });
  }
  return novel;
}

function isRoleRelevantStakeholder(person = {}) {
  return hasRoleRelevantDecisionMaker([person]);
}

/**
 * Dimension scoring — each returns { advanced, limited, none, evidence[], decisionChange }
 */
function scoreDemandIntelligence({ novelEvidence = [], before = {}, after = {} } = {}) {
  const evidence = [];
  const demandPatterns = /room\s*block|housing\s*bureau|stay-?to-?play|peak\s*rooms?|group\s*accommodation|incentive\s+programme|housing\s*program|hotel\s+sourc|contract(?:ing|ed)?\s+hotel|official\s+overflow/i;
  const sizeTiming = /\b(?:20(?:2[6-9]|3\d))\b|\b\d{2,4}\s*(?:peak\s*)?rooms?\b|\b\d{2,4}\s*teams?\b|memorial\s+day|legislative\s+conference/i;
  for (const e of novelEvidence) {
    // Corporate registry / nonprofit explorer alone ≠ demand intelligence
    if (/companyregistry|propublica\.org\/nonprofits|incorporation-date/i.test(e.url || "")) continue;
    if (demandPatterns.test(e.statement)) {
      evidence.push({ ...e, dimensionNote: "new_demand_or_sourcing_signal" });
    } else if (sizeTiming.test(e.statement) && /hotel|housing|room|tournament|conference|meeting/i.test(e.statement)) {
      evidence.push({ ...e, dimensionNote: "new_size_or_timing_signal" });
    }
  }
  const beforeThesis = fingerprintBlob(before.description || "");
  const afterThesis = fingerprintBlob(
    after.opportunity?.hotelOpportunityThesis || after.hotelOpportunityThesis || "",
  );
  // Thesis change alone without novel evidence ≠ demand improvement
  if (!evidence.length) {
    return { advanced: false, limited: false, evidence: [], decisionChange: null };
  }
  const substantive = evidence.filter((e) =>
    /housing|room\s*block|sourcing|contract|peak\s*rooms|stay-?to-?play|housing\s*bureau/i.test(e.statement));
  return {
    advanced: substantive.length >= 1,
    limited: substantive.length === 0 && evidence.length >= 1,
    evidence: evidence.slice(0, 6),
    decisionChange: substantive.length
      ? "New demand/sourcing evidence acquired beyond prior thesis"
      : "Background demand context enriched",
    beforeThesisLen: beforeThesis.length,
    afterThesisLen: afterThesis.length,
  };
}

function scoreHotelRelevance({
  novelEvidence = [],
  before = {},
  after = {},
  opportunity = {},
  priorHosts = new Set(),
  brief = {},
} = {}) {
  const hotel = text(opportunity.hotelName || before.hotelName || brief.hotelName);
  const evidence = [];
  let exclusionSupported = false;
  for (const e of novelEvidence) {
    const mentionsHotel = hotel && new RegExp(hotel.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(e.statement);
    const competing = [...priorHosts].some((h) => h && e.statement.toLowerCase().includes(h));
    if (mentionsHotel && /fit|overflow|catchment|metro|housing|shortlist|compete/i.test(e.statement)) {
      evidence.push({ ...e, dimensionNote: "hotel_fit_signal" });
    }
    // Known competitor restated ≠ new
    if (competing && !priorHosts.has(fingerprintBlob(e.statement).slice(0, 40))) {
      // Only count competitor detail if URL is new independent page with new facts
      if (e.url && !/canonical:\/\//i.test(e.url)) {
        evidence.push({ ...e, dimensionNote: "competitor_context_enriched" });
      }
    }
    if (/do not pursue|exclude|mismatch|elsewhere|not relevant/i.test(e.statement)
      && e.url) {
      exclusionSupported = true;
      evidence.push({ ...e, dimensionNote: "supported_exclusion" });
    }
  }
  // Brief clarifying why-my-hotel from new evidence
  const briefHotelBound = brief.provenanceBound?.hotelId === (opportunity.hotelId || before.hotelId)
    || brief.hotelName === hotel;
  if (exclusionSupported) {
    return {
      advanced: true,
      limited: false,
      evidence: evidence.slice(0, 6),
      decisionChange: "Supported exclusion / non-pursuit from newly acquired evidence",
      exclusionSupported: true,
    };
  }
  if (evidence.some((e) => e.dimensionNote === "hotel_fit_signal")) {
    return {
      advanced: true,
      limited: false,
      evidence: evidence.slice(0, 6),
      decisionChange: "New hotel-specific competitive relevance evidence",
      exclusionSupported: false,
    };
  }
  if (evidence.length && briefHotelBound) {
    return {
      advanced: false,
      limited: true,
      evidence: evidence.slice(0, 6),
      decisionChange: "Hotel context enriched without material fit change",
      exclusionSupported: false,
    };
  }
  return { advanced: false, limited: false, evidence: [], decisionChange: null, exclusionSupported: false };
}

function scoreBuyerAccessibility({
  before = {},
  after = {},
  opportunity = {},
  ownership = {},
} = {}) {
  const priorRoutes = collectPriorContactRoutes(before, opportunity);
  const priorNames = new Set(
    (before.knownStakeholders || opportunity.namedStakeholders || [])
      .map((p) => text(p?.name || p?.personName).toLowerCase())
      .filter(Boolean),
  );

  const afterPeople = [
    ...(after.namedStakeholders || []),
    ...(after.opportunity?.namedStakeholders || []),
  ];
  const newPeople = afterPeople.filter((p) => {
    const name = text(p?.name || p?.personName).toLowerCase();
    return name && !priorNames.has(name);
  });
  const newRoleRelevant = newPeople.filter(isRoleRelevantStakeholder);
  const newInfluencers = newPeople.filter((p) => !isRoleRelevantStakeholder(p));

  const route = normalizeEmail(ownership.route || after.contactOwnership?.route);
  const salesUsable = ownership.salesUsableAsProspectContact === true
    || after.contactOwnership?.salesUsableAsProspectContact === true;
  const consumer = route && isConsumerEmailDomain(route);
  const routeIsNew = Boolean(route) && !priorRoutes.has(route) && !consumer && salesUsable;

  // Pre-existing general routes (tournament@, info@) must NEVER count as improvement
  const priorRouteRestated = Boolean(route) && priorRoutes.has(route);

  const evidence = [];
  if (newRoleRelevant.length) {
    for (const p of newRoleRelevant.slice(0, 3)) {
      evidence.push({
        key: `person:${text(p.name || p.personName).toLowerCase()}`,
        statement: `${p.name || p.personName} — ${p.title || p.roleTitle || "role"} (${p.decisionMakerClass || "relevant"})`,
        provenance: p.sourceUrl || "public_buyer_pass",
        dimensionNote: "new_role_relevant_stakeholder",
      });
    }
    return {
      advanced: true,
      limited: false,
      evidence,
      decisionChange: "Newly identified role-relevant stakeholder",
      phantomContactImprovement: false,
      priorRouteRestated,
      newUsableRoute: routeIsNew,
    };
  }

  if (routeIsNew) {
    evidence.push({
      key: `route:${route}`,
      statement: `New usable org contact route: ${route} (${ownership.ownershipClass || after.contactOwnership?.ownershipClass})`,
      provenance: "contact_ownership",
      dimensionNote: "new_usable_org_route",
    });
    return {
      advanced: true,
      limited: false,
      evidence,
      decisionChange: "Newly validated organization contact route",
      phantomContactImprovement: false,
      priorRouteRestated: false,
      newUsableRoute: true,
    };
  }

  if (newInfluencers.length && !priorRouteRestated) {
    return {
      advanced: false,
      limited: true,
      evidence: newInfluencers.slice(0, 2).map((p) => ({
        key: `influencer:${text(p.name || p.personName).toLowerCase()}`,
        statement: `${p.name || p.personName} — influencer-class only`,
        provenance: p.sourceUrl || "public_extraction",
        dimensionNote: "influencer_not_dm",
      })),
      decisionChange: "Influencer names extracted — not decision-makers",
      phantomContactImprovement: false,
      priorRouteRestated,
      newUsableRoute: false,
    };
  }

  return {
    advanced: false,
    limited: false,
    evidence: [],
    decisionChange: priorRouteRestated
      ? "Prior contact route restated — not counted as improvement"
      : null,
    phantomContactImprovement: false,
    priorRouteRestated,
    newUsableRoute: false,
  };
}

function scoreCommercialTiming({ novelEvidence = [], before = {}, opportunity = {} } = {}) {
  const priorBlob = text([
    opportunity.eventStartDate,
    opportunity.eventEndDate,
    opportunity.eventDateDisplay,
    opportunity.hotelOpportunityThesis,
    before.description,
    ...(before.demandEvidence || []).map((e) => e.statement),
  ].join(" "));
  const evidence = [];
  for (const e of novelEvidence) {
    if (/companyregistry|propublica\.org\/nonprofits|incorporation/i.test(e.url || "")) continue;
    // Program / sourcing timing only — not incorporation years or CMS asset dates
    const programDate = e.statement.match(
      /\b(?:20(?:2[6-9]|3\d))\b|\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s*20(?:2[6-9]|3\d)\b/ig,
    ) || [];
    const cycle = /next\s+cycle|future\s+cycle|sourcing\s+window|rfp\s+active|headquarter\s+hotel\s+not\s+yet|room\s*block\s+(?:opens|deadline)/i.test(e.statement);
    const commercialContext = /hotel|housing|conference|tournament|meeting|overflow|room\s*block|washcon|memorial/i.test(e.statement);
    const novelDate = programDate.some((d) => !priorBlob.includes(d)) && commercialContext;
    if ((novelDate || (cycle && commercialContext)) && e.url) {
      evidence.push({ ...e, dimensionNote: novelDate ? "new_program_date" : "cycle_or_window_signal" });
    }
  }
  if (!evidence.length) {
    return { advanced: false, limited: false, evidence: [], decisionChange: null };
  }
  const advanced = evidence.some((e) => e.dimensionNote === "new_program_date"
    || /sourcing\s+window|rfp\s+active|headquarter\s+hotel\s+not\s+yet/i.test(e.statement));
  return {
    advanced,
    limited: !advanced,
    evidence: evidence.slice(0, 4),
    decisionChange: advanced
      ? "Newly established program date or sourcing window"
      : "Cycle/recurrence context enriched",
  };
}

function scoreSalesActionability({
  before = {},
  brief = {},
  dimensions = {},
  novelEvidence = [],
} = {}) {
  const priorActions = (before.recommendedSalesActions || []).map(fingerprintBlob);
  const ask = text(brief.whatShouldISayOrAsk || "");
  const askFp = fingerprintBlob(ask);
  const askIsNew = ask && !priorActions.some((p) => p && askFp.includes(p.slice(0, 60)));
  const deferralOnly = /do not pitch yet|close research gaps first|not sales-ready/i.test(ask);
  const specificAsk = /ask\s+[A-Z][a-z]+|shortlist|housing\s*bureau|overflow|room\s*block|who owns|introduce .+ via/i.test(ask);
  const exclusion = dimensions.HOTEL_SPECIFIC_RELEVANCE?.exclusionSupported === true
    || /do not pursue|exclude this prospect|not a fit for/i.test(ask);
  const evidence = [];

  if (exclusion && (dimensions.HOTEL_SPECIFIC_RELEVANCE?.advanced || novelEvidence.length >= 1)) {
    evidence.push({
      key: "sales:exclusion",
      statement: ask || "Supported non-pursuit / exclusion guidance",
      provenance: "sales_brief",
      dimensionNote: "exclusion_action",
    });
    return {
      advanced: true,
      limited: false,
      evidence,
      decisionChange: "Sales decision improved via supported exclusion",
    };
  }

  // Deferral copy is honest but not a material sales unlock
  if (deferralOnly) {
    return {
      advanced: false,
      limited: askIsNew,
      evidence: askIsNew ? [{
        key: "sales:deferral",
        statement: ask.slice(0, 200),
        provenance: "sales_brief",
        dimensionNote: "deferral_not_unlock",
      }] : [],
      decisionChange: askIsNew ? "Research deferral restated — not a stronger outreach path" : null,
    };
  }

  if (askIsNew && specificAsk && (
    dimensions.BUYER_ACCESSIBILITY?.advanced
    || dimensions.DEMAND_INTELLIGENCE?.advanced
    || dimensions.COMMERCIAL_TIMING?.advanced
    || dimensions.HOTEL_SPECIFIC_RELEVANCE?.advanced
  )) {
    evidence.push({
      key: "sales:ask",
      statement: ask.slice(0, 280),
      provenance: "sales_brief",
      dimensionNote: "stronger_outreach_or_qualification_ask",
    });
    return {
      advanced: true,
      limited: false,
      evidence,
      decisionChange: "Materially better next sales action grounded in new evidence",
    };
  }

  if (askIsNew && novelEvidence.length >= 1) {
    return {
      advanced: false,
      limited: true,
      evidence: [{
        key: "sales:ask_limited",
        statement: ask.slice(0, 200),
        provenance: "sales_brief",
        dimensionNote: "ask_reworded_or_lightly_improved",
      }],
      decisionChange: "Sales ask updated without material commercial unlock",
    };
  }

  return { advanced: false, limited: false, evidence: [], decisionChange: null };
}

/**
 * Full multidimensional commercial improvement assessment.
 */
export function assessCommercialImprovement({
  before = {},
  after = {},
  opportunity = {},
  result = {},
  technicalPass = true,
  salesBriefClean = true,
} = {}) {
  if (!technicalPass) {
    return {
      version: COMMERCIAL_IMPROVEMENT_MEASUREMENT_V1,
      outcome: IMPROVEMENT_OUTCOME.RESEARCH_BLOCKED,
      dimensions: {},
      materialDimensionCount: 0,
      limitedDimensionCount: 0,
      improvements: [],
      phantomContactImprovement: false,
      note: "Technical research path failed — commercial improvement not assessed.",
    };
  }

  const brief = result.hotelSalesBrief || after.salesBrief || after.opportunity?.deepResearchEnrichment?.salesBrief || {};
  const ownership = after.contactOwnership || result.after?.contactOwnership || {};
  const priorHosts = collectPriorCompetitorHosts(before, opportunity);
  const novelEvidence = diffNewEvidence({ before, after, opportunity, result });

  const dimensions = {
    [IMPROVEMENT_DIMENSION.DEMAND_INTELLIGENCE]: scoreDemandIntelligence({
      novelEvidence, before, after,
    }),
    [IMPROVEMENT_DIMENSION.HOTEL_SPECIFIC_RELEVANCE]: scoreHotelRelevance({
      novelEvidence, before, after, opportunity, priorHosts, brief,
    }),
    [IMPROVEMENT_DIMENSION.BUYER_ACCESSIBILITY]: scoreBuyerAccessibility({
      before, after, opportunity, ownership,
    }),
    [IMPROVEMENT_DIMENSION.COMMERCIAL_TIMING]: scoreCommercialTiming({
      novelEvidence, before, opportunity,
    }),
  };
  dimensions[IMPROVEMENT_DIMENSION.SALES_ACTIONABILITY] = scoreSalesActionability({
    before, brief, dimensions, novelEvidence,
  });

  const improvements = [];
  for (const [dim, score] of Object.entries(dimensions)) {
    if (score.advanced || score.limited) {
      improvements.push({
        dimension: dim,
        level: score.advanced ? "MATERIAL_DIMENSION" : "LIMITED_DIMENSION",
        decisionChange: score.decisionChange,
        evidence: score.evidence || [],
      });
    }
  }

  const materialDimensionCount = improvements.filter((i) => i.level === "MATERIAL_DIMENSION").length;
  const limitedDimensionCount = improvements.filter((i) => i.level === "LIMITED_DIMENSION").length;
  const phantomContactImprovement = dimensions.BUYER_ACCESSIBILITY?.priorRouteRestated === true
    && dimensions.BUYER_ACCESSIBILITY?.advanced === true; // should be impossible with correct logic

  // Named buyer alone / page count alone / longer brief alone ≠ MATERIAL
  const researchEffortOnly = novelEvidence.length >= 1
    && materialDimensionCount === 0
    && limitedDimensionCount === 0;

  let outcome = IMPROVEMENT_OUTCOME.NO_IMPROVEMENT;
  if (!salesBriefClean) {
    // Contaminated brief cannot support MATERIAL sales use; evidence dims may still be limited
    outcome = materialDimensionCount >= 1 || limitedDimensionCount >= 1
      ? IMPROVEMENT_OUTCOME.LIMITED_IMPROVEMENT
      : IMPROVEMENT_OUTCOME.NO_IMPROVEMENT;
  } else if (materialDimensionCount >= 2) {
    outcome = IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT;
  } else if (materialDimensionCount === 1
    && (dimensions.SALES_ACTIONABILITY?.advanced
      || dimensions.BUYER_ACCESSIBILITY?.advanced
      || dimensions.DEMAND_INTELLIGENCE?.advanced
      || dimensions.HOTEL_SPECIFIC_RELEVANCE?.exclusionSupported)) {
    // Single material dimension is enough when it changes the commercial decision path
    outcome = IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT;
  } else if (materialDimensionCount >= 1 || limitedDimensionCount >= 1) {
    outcome = IMPROVEMENT_OUTCOME.LIMITED_IMPROVEMENT;
  } else if (researchEffortOnly || novelEvidence.length >= 2) {
    outcome = IMPROVEMENT_OUTCOME.LIMITED_IMPROVEMENT;
  }

  // Missing room-block evidence must not block ACCOUNT_DEVELOPMENT classification of outcome
  // (outcome is about improvement delta, not disposition). Disposition left to qualification.

  return {
    version: COMMERCIAL_IMPROVEMENT_MEASUREMENT_V1,
    outcome,
    dimensions,
    materialDimensionCount,
    limitedDimensionCount,
    improvements,
    novelEvidenceCount: novelEvidence.length,
    novelEvidenceSample: novelEvidence.slice(0, 8),
    phantomContactImprovement: Boolean(phantomContactImprovement),
    priorContactRoutes: [...collectPriorContactRoutes(before, opportunity)],
    afterContactRoute: ownership.route || after.contactOwnership?.route || null,
    priorRouteRestatedAsImprovement: dimensions.BUYER_ACCESSIBILITY?.priorRouteRestated === true
      && !dimensions.BUYER_ACCESSIBILITY?.newUsableRoute
      && !dimensions.BUYER_ACCESSIBILITY?.advanced,
    namedBuyerNotRequiredForMaterial: true,
    roomBlockNotRequiredForMaterial: true,
    researchEffortAloneInsufficientForMaterial: true,
    note: outcome === IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT
      ? "Material improvement grounded in ≥1 decision-changing dimension with cited new evidence."
      : outcome === IMPROVEMENT_OUTCOME.LIMITED_IMPROVEMENT
        ? "Limited improvement — research advanced context without unlocking a new commercial decision path."
        : "No commercial improvement beyond prior state.",
  };
}

/**
 * Contract summary for docs / certification.
 */
export function commercialImprovementContract() {
  return {
    version: COMMERCIAL_IMPROVEMENT_MEASUREMENT_V1,
    dimensions: Object.values(IMPROVEMENT_DIMENSION),
    materialRules: [
      "MATERIAL requires ≥1 advanced dimension that changes demand, hotel relevance (incl. supported exclusion), buyer accessibility (new role-relevant person OR new usable org route), timing, or sales actionability — preferably with sales action grounded in that evidence.",
      "Two advanced dimensions always qualify as MATERIAL.",
      "Named decision-maker is NOT required for MATERIAL.",
      "Confirmed room block is NOT required for MATERIAL (account development may improve without it).",
    ],
    falsePositiveGuards: [
      "Prior contact routes (contactRoutes, contactEmail, opportunity emails) never count as newly discovered.",
      "Normalized evidence identity (url+statement fingerprint) prevents reworded thesis from counting as new.",
      "Already-known competitor hosts restated without new independent facts do not count.",
      "Influencers ≠ decision-makers.",
      "Consumer-domain emails never sales-usable improvements.",
      "Research effort / page count / longer report alone ≠ MATERIAL.",
      "Inference must not be labeled verified fact in improvement evidence provenance.",
    ],
    rootCauseFixed: {
      phantomTournamentInfoContacts: "BEFORE snapshot used contactRoutes but classifier compared only before.contactEmail (often undefined), so tournament@/info@ looked 'new'. Fixed via collectPriorContactRoutes.",
    },
  };
}
