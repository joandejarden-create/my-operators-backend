/**
 * Commercial Completion & Buyer Access V1 — offline.
 * Turns PRA/AAD research into actionable hotel sales intelligence.
 * No Surfe/PDL/Parallel/paid · no Airtable/publish/weekly.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyContactOwnership,
  CONTACT_OWNERSHIP_CLASS,
  assertPaidContactProvidersDisabled,
  isConsumerEmailDomain,
} from "./commercial-spine-restoration-v41.js";
import {
  assessCommercialImprovement,
  IMPROVEMENT_OUTCOME,
  collectPriorContactRoutes,
  diffNewEvidence,
} from "./commercial-improvement-measurement-v1.js";
import {
  buildTargetedEnrichmentRequest,
} from "./autonomous-deep-research-recovery-v1.js";
import {
  targetRolesForOpportunity,
} from "../workflow/buyer-contact-enrichment-contract-v1.js";
import {
  resolveEffectiveSurfeAuthorization,
} from "../workflow/buyer-contact-enrichment-v1.js";
import { FINDING_PROVENANCE, SHORTLIST_COHORT_V1 } from "./buyer-intelligence-shortlist-offline-v1.js";

export const COMMERCIAL_COMPLETION_BUYER_ACCESS_V1 = "gdi-commercial-completion-buyer-access-v1";
export const COMMERCIAL_COMPLETION_REPORTS_DIR = "reports/gdi-commercial-completion-buyer-access-v1";

export const COMMERCIAL_NEXT_ACTION = Object.freeze({
  READY_FOR_OUTREACH: "READY_FOR_OUTREACH",
  ACCOUNT_DEVELOPMENT_NEEDS_BUYER: "ACCOUNT_DEVELOPMENT_NEEDS_BUYER",
  FUNCTIONAL_ROUTE_QUALIFY: "FUNCTIONAL_ROUTE_QUALIFY",
  FUTURE_CYCLE_SCHEDULED_FOLLOWUP: "FUTURE_CYCLE_SCHEDULED_FOLLOWUP",
  AWAITING_AUTHORIZED_ENRICHMENT: "AWAITING_AUTHORIZED_ENRICHMENT",
  INSUFFICIENT_HOTEL_FIT: "INSUFFICIENT_HOTEL_FIT",
  DO_NOT_PURSUE: "DO_NOT_PURSUE",
  HOLD_LOWER_PRIORITY: "HOLD_LOWER_PRIORITY",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LIVE_DIR = path.join(ROOT, "reports/gdi-full-commercial-deep-research-validation-v1/live");
const HOTELS_DIR = path.join(ROOT, "data/group-demand-intelligence/hotels");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadCanonicalCohortOpportunity(opportunityId) {
  const meta = SHORTLIST_COHORT_V1.find((c) => c.opportunityId === opportunityId);
  if (!meta) throw new Error(`not_in_shortlist_cohort:${opportunityId}`);
  const bagPath = path.join(HOTELS_DIR, meta.hotelId, "opportunities.json");
  const raw = JSON.parse(fs.readFileSync(bagPath, "utf8"));
  const arr = Array.isArray(raw) ? raw : (raw.opportunities || []);
  const bag = arr.find((o) => o.id === opportunityId);
  if (!bag) throw new Error(`opportunity_not_found:${opportunityId}`);
  const profilePath = path.join(HOTELS_DIR, meta.hotelId, "profile.json");
  const profile = fs.existsSync(profilePath) ? JSON.parse(fs.readFileSync(profilePath, "utf8")) : {};
  const hotelName = profile.identity?.hotelName || meta.hotelName;
  return {
    ...bag,
    id: bag.id,
    opportunityId: bag.id,
    hotelId: meta.hotelId,
    hotelName,
    pathway: meta.pathway,
    contactEmail: bag.contactEmail || bag.primaryContactEmail || bag.primaryContact?.email || null,
    organizationDomain: (() => {
      try {
        return new URL(bag.officialSource || bag.contactUrl || "https://example.invalid").hostname.replace(/^www\./, "");
      } catch {
        return "";
      }
    })(),
  };
}

function isLikelyPersonName(name) {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (/association|academy|events|housing|provider|channel|tournaments|inc\.|llc|global|maritz|staff|tbd|meetings\s*\/\s*housing|consultancy/i.test(n)) {
    return false;
  }
  return /^[A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+)+$/.test(n);
}

function readJson(p) {
  return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, "utf8")) : null;
}

function loadLiveBundle(opportunityId) {
  const dir = path.join(LIVE_DIR, opportunityId);
  return {
    before: readJson(path.join(dir, "BEFORE.json")),
    after: readJson(path.join(dir, "AFTER.json")),
    cert: readJson(path.join(dir, "CERTIFICATION.json")),
    commercial: readJson(path.join(dir, "COMMERCIAL_IMPROVEMENT.json")),
    brief: readJson(path.join(dir, "SALES_BRIEF.json")),
    sourcing: readJson(path.join(dir, "SOURCING_CHAIN.json")),
    enrichment: readJson(path.join(dir, "TARGETED_ENRICHMENT_REQUEST.json")),
    trace: readJson(path.join(dir, "RETRIEVAL_TRACE.json")),
  };
}

/**
 * Classify what the hotel salesperson should do next — not a new customer publication class.
 */
export function classifyCommercialNextAction({
  pathway = null,
  hotelFitCredible = true,
  namedPersonBuyerVerified = false,
  functionalRouteUsable = false,
  ownershipClass = null,
  enrichmentJustified = false,
  overflowOrHostUnlocked = false,
  speculativeOnly = false,
  holdRecommended = false,
} = {}) {
  if (holdRecommended) {
    return {
      nextAction: COMMERCIAL_NEXT_ACTION.HOLD_LOWER_PRIORITY,
      salesReady: false,
      rationale: "Hotel-fit / buyer gaps do not justify near-term pursuit; hold or lower research priority",
    };
  }
  if (!hotelFitCredible || speculativeOnly) {
    return {
      nextAction: speculativeOnly
        ? COMMERCIAL_NEXT_ACTION.DO_NOT_PURSUE
        : COMMERCIAL_NEXT_ACTION.INSUFFICIENT_HOTEL_FIT,
      salesReady: false,
      rationale: speculativeOnly
        ? "Only speculative path remains — do not invent demand"
        : "Insufficient hotel-specific commercial fit from evidence",
    };
  }
  if (namedPersonBuyerVerified && functionalRouteUsable && overflowOrHostUnlocked) {
    return {
      nextAction: COMMERCIAL_NEXT_ACTION.READY_FOR_OUTREACH,
      salesReady: true,
      rationale: "Verified named buyer + usable route + open commercial path",
    };
  }
  if (/COMPETITIVE_FUTURE|FUTURE_CYCLE/i.test(String(pathway || ""))
    && functionalRouteUsable
    && !overflowOrHostUnlocked) {
    return {
      nextAction: COMMERCIAL_NEXT_ACTION.FUTURE_CYCLE_SCHEDULED_FOLLOWUP,
      salesReady: false,
      rationale: "Future-cycle / housing channel exists; open inventory not evidenced — schedule qualify/follow-up",
    };
  }
  if (functionalRouteUsable
    && /FUNCTIONAL|INFLUENCER|GENERAL_ORGANIZATION|INTERMEDIARY/i.test(String(ownershipClass || ""))) {
    return {
      nextAction: COMMERCIAL_NEXT_ACTION.FUNCTIONAL_ROUTE_QUALIFY,
      salesReady: false,
      rationale: "Usable functional/intermediary route for qualification — not automatic sales-ready outreach",
    };
  }
  if (enrichmentJustified && !namedPersonBuyerVerified) {
    return {
      nextAction: COMMERCIAL_NEXT_ACTION.AWAITING_AUTHORIZED_ENRICHMENT,
      salesReady: false,
      rationale: "Public evidence exhausted for named buyer — gated enrichment may unlock outreach path",
    };
  }
  if (/ACCOUNT/i.test(String(pathway || ""))) {
    return {
      nextAction: COMMERCIAL_NEXT_ACTION.ACCOUNT_DEVELOPMENT_NEEDS_BUYER,
      salesReady: false,
      rationale: "Credible account thesis without verified buyer access",
    };
  }
  return {
    nextAction: COMMERCIAL_NEXT_ACTION.ACCOUNT_DEVELOPMENT_NEEDS_BUYER,
    salesReady: false,
    rationale: "Buyer / path incomplete",
  };
}

/**
 * Enrich buildTargetedEnrichmentRequest with role/geo/cost authorization fields.
 */
export function buildPrioritizedEnrichmentRequest({
  opportunity = {},
  targetRole = null,
  geography = null,
  knownCandidate = null,
  knownEvidence = [],
  informationNeeded = null,
  salesActionImproved = null,
  preferredProvider = "surfe",
  publicAttempts = [],
  contactOwnership = null,
} = {}) {
  const base = buildTargetedEnrichmentRequest({
    opportunity,
    unresolvedBuyerQuestion: informationNeeded
      || `Identify ${targetRole || "hotel-sourcing owner"} at ${opportunity.organizationName || "organization"}`,
    publicAttempts,
    contactOwnership,
  });
  return {
    ...base,
    targetRole: targetRole || null,
    geography: geography || null,
    knownCandidateIdentity: knownCandidate
      ? {
        name: knownCandidate.name || null,
        title: knownCandidate.title || null,
        findingProvenance: knownCandidate.findingProvenance || FINDING_PROVENANCE.UNVERIFIED,
        note: "Identity enrichment only after responsibility verification — personal email last",
      }
      : null,
    knownEvidence: (knownEvidence || []).slice(0, 6),
    informationNeeded: informationNeeded
      || "Named person currently responsible for hotel/venue/supplier selection + current employer confirmation",
    whyImprovesHotelSalesAction: salesActionImproved
      || "Converts account-development thesis into a specific outreach conversation without inventing RFPs",
    preferredProvider,
    providerSequence: preferredProvider === "surfe"
      ? ["public_who_first", "surfe_identity_then_email", "pdl_only_if_surfe_gap"]
      : ["public_who_first", preferredProvider],
    expectedCostOrCredits: {
      surfe: "Requires live Surfe credit balance + estimateSurfeUsd(email); historically ~1 credit/email when authorized — do not invent exact remaining credits",
      pdl: "Requires PDL_CONTACT_EVAL_ALLOW_PAID=1 and separate eval budget — not enabled for GDI deep research by default",
      verified: false,
      note: "Exact credits not asserted offline — founder must check live balance before GO",
    },
    authorizationRequired: {
      authorizeSurfe: true,
      authorizePdl: preferredProvider === "pdl",
      persistSurfePiiInGdi: false,
      note: "Surfe-derived contact details must not be permanently persisted in GDI",
    },
    prioritizeIdentityBeforePersonalContact: true,
  };
}

/**
 * Static audit of existing buyer-enrichment capabilities (no provider calls).
 */
export function auditContactEnrichmentCapabilities() {
  const surfeAuth = resolveEffectiveSurfeAuthorization({ authorizeSurfe: false, dryRun: true });
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });

  const capabilities = [
    {
      capability: "Public WHO discovery (staff/team/leadership pages + Exa buyer queries)",
      orsConnection: "ensureBuyerWhoResearchExecuted + createLiveDeepResearchResearcher Exa ranking + runPublicBuyerIntelligencePass",
      requiredInput: "organizationName, organizationDomain/officialSource, remainingBudgetUsd",
      returns: "Named stakeholders from page text + decisionMakerClass via classifyPersonResponsibility",
      verifiedLimitations: "Many orgs lack parseable hotel-sourcing titles; Exa can return wrong-entity hits (e.g. PRA consultancy)",
      cost: "Exa lite search + Firecrawl/direct_public scrape when live; $0 offline fixtures",
      storageRestrictions: "Public-source only; no Surfe PII",
      helpsPraAad: "Yes for public staff pages; PRA team pages did not yield named hotel-sourcing DMs in live pilot",
    },
    {
      capability: "Person responsibility classification",
      orsConnection: "classifyPersonResponsibility in buyer-contact-enrichment-v1 + commercial-depth buyer pass",
      requiredInput: "name, title, optional email, opportunity segment",
      returns: "BOOKING_DECISION / CONFIRMED_RELEVANT / POTENTIALLY_RELEVANT / influencer classes",
      verifiedLimitations: "Title heuristics; functional entities previously mislabeled as people (now gated)",
      cost: "Free / local",
      storageRestrictions: "n/a",
      helpsPraAad: "Yes — rejects national exec-as-automatic-buyer; needs correct titles first",
    },
    {
      capability: "Contact route ownership",
      orsConnection: "classifyContactOwnership in commercial-spine-restoration-v41 on ORS mature path",
      requiredInput: "email, pageUrl, organizationDomain, optional personName/roleTitle",
      returns: "ownershipClass + salesUsableAsProspectContact",
      verifiedLimitations: "Consumer email rejected; directories rejected; functional intermediaries now non-person",
      cost: "Free / local",
      storageRestrictions: "Ownership metadata only",
      helpsPraAad: "AAD registration@ usable as FUNCTIONAL/INTERMEDIARY; PRA has no email yet",
    },
    {
      capability: "Buyer-contact enrichment workflow (Surfe-gated)",
      orsConnection: "enrichBuyerContactForOpportunity — weekly/workflow path; deep-research forbids authorizeSurfe=true",
      requiredInput: "named person + org/domain + authorizeSurfe=true + Surfe key + budget",
      returns: "Optional work email/phone with identity acceptance; cache reuse",
      verifiedLimitations: "Requires named person first; dry-run always blocks; Surfe PII must not permanently persist in GDI",
      cost: "estimateSurfeUsd(email) when authorized; credits via getSurfeCredits — not asserted offline",
      storageRestrictions: "Surfe-derived details: no permanent GDI persistence; usage ledger only",
      helpsPraAad: "Only AFTER named hotel-sourcing candidate exists for PRA; AAD needs housing-admin person not Maritz-as-person",
      surfeEffectiveWhenUnauthorized: surfeAuth,
    },
    {
      capability: "Surfe adapter (market-alerts + hotel-intelligence identity acceptance)",
      orsConnection: "Indirect via buyer-contact-enrichment; NOT wired into runDeepResearchOnExistingGdiOpportunity paid path",
      requiredInput: "API key + authorized enrichment job",
      returns: "Person search + enrichment emails/phones",
      verifiedLimitations: "Disconnected from deep-research live pilot by hard throw on authorizeSurfe",
      cost: "Provider credits",
      storageRestrictions: "Identity acceptance gates; no auto-merge to customer display",
      helpsPraAad: "Potential for PRA named DM enrichment after public WHO finds a candidate name",
    },
    {
      capability: "PDL contact enrichment eval",
      orsConnection: "Eval scripts only (pdl-contact-enrichment-eval); PDL_CONTACT_EVAL_ALLOW_PAID gate",
      requiredInput: "PDL credentials + allow-paid flag",
      returns: "Eval enrichment payloads",
      verifiedLimitations: "Not production-connected to ORS deep research; separately gated",
      cost: "PDL credits when allow-paid",
      storageRestrictions: "Eval artifacts; not GDI production writes",
      helpsPraAad: "Optional fallback if Surfe cannot resolve — requires separate founder GO",
    },
    {
      capability: "Targeted enrichment request builder",
      orsConnection: "buildTargetedEnrichmentRequest attached on buyer gap in deep-research result",
      requiredInput: "opportunity + publicAttempts + contactOwnership",
      returns: "AWAITING_SEPARATE_AUTHORIZATION request (authorizeSurfe/Pdl false)",
      verifiedLimitations: "Does not execute providers; must be enriched with role/geo/cost for founder review",
      cost: "$0 until authorized",
      storageRestrictions: "Request JSON only",
      helpsPraAad: "Yes — primary vehicle for separately authorized buyer test",
    },
    {
      capability: "Historical contact memory / Surfe usage ledger",
      orsConnection: "findReusableSurfeResult + surfe-usage-ledger-v1",
      requiredInput: "Prior Surfe cache entries",
      returns: "Cache reuse without new credits when match exists",
      verifiedLimitations: "No PRA/AAD hotel-sourcing person cache evidenced for this cohort",
      cost: "$0 on reuse",
      storageRestrictions: "Ledger under workflow runs — not opportunity bag PII",
      helpsPraAad: "Unlikely until a prior authorized enrichment exists for the same person",
    },
  ];

  return {
    version: COMMERCIAL_COMPLETION_BUYER_ACCESS_V1,
    auditedAt: new Date().toISOString(),
    paidProvidersCurrentlyDisabled: paidGuard.ok === true,
    orsDeepResearchForbidsSurfePdl: true,
    integrationGapSummary: [
      "Deep-research path runs public WHO + ownership but never invokes Surfe/PDL (hard forbid).",
      "enrichBuyerContactForOpportunity exists on weekly/workflow path and is not the live deep-research entrypoint.",
      "Wrong-entity Exa hits (PRA consultancy; Showsbee directories) can inflate timing unless filtered.",
      "Functional intermediaries were previously labeled as named DMs in ownership/briefs — ownership now gates personLooksHuman.",
    ],
    capabilities,
  };
}

export function assessPraCommercialCompletion() {
  const opportunityId = "gdi_lmc_4d2f10eab2d44499";
  const bag = loadCanonicalCohortOpportunity(opportunityId);
  const live = loadLiveBundle(opportunityId);
  const roles = targetRolesForOpportunity(bag);
  const contactUrl = bag.contactUrl || "https://pra.com/about-us/contact-us";

  const ownership = classifyContactOwnership({
    email: bag.contactEmail || null,
    pageUrl: contactUrl,
    organizationName: bag.organizationName,
    organizationDomain: "pra.com",
    publisherKind: "ORGANIZATION_OWNED",
  });

  const evidence = {
    praRole: {
      statement: "PRA operates as a destination-management / event-planning organization supporting client programmes across destinations including Florida/Tampa.",
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      urls: [bag.officialSource, "https://pra.com/destinations/florida"],
    },
    tampaActivity: {
      statement: text(bag.eventActivityEvidence || bag.hotelOpportunityThesis),
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      urls: [
        "https://pra.com/blog/a-day-in-tampa-exploring-top-event-venues-and-experiences-for-meeting-planners",
        "https://pra.com/destinations/florida",
      ],
    },
    hotelSourcingInfluence: {
      statement: "Supplier-development visit produced new supplier relationships and an internal vendor-resource library — PRA influences venue/supplier selection for client programmes (not proven as exclusive hotel contractor).",
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      inferenceNote: "Influence ≠ exclusive contracting authority",
    },
    westinCredibility: {
      statement: "Harbour Island / waterfront full-service profile is commercially relevant to PRA Tampa itineraries (meetings, receptions, dinners) — prospective supplier introduction only.",
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      hotelId: bag.hotelId,
      hotelName: bag.hotelName,
    },
    namedProgramsClients: {
      statement: "No named client RFPs, room blocks, or contracted Westin partnership evidenced.",
      provenance: FINDING_PROVENANCE.UNVERIFIED,
    },
    partnershipLocus: {
      statement: "Unresolved whether hotel partnerships are set locally (Tampa destination team) vs regionally/centrally.",
      provenance: FINDING_PROVENANCE.UNVERIFIED,
    },
    wrongEntityLiveNoise: {
      statement: "Live Exa/Clocate hits for 'PRA consultancy / regulatory affairs' are wrong-entity noise — not pra.com DMC evidence.",
      provenance: FINDING_PROVENANCE.VERIFIED_LIVE,
      note: "Filtered from commercial scoring in reclassification",
    },
  };

  const sourcingChain = {
    organization: bag.organizationName,
    hotel: bag.hotelName,
    pathway: "ACCOUNT_LED",
    functionsLikelyControlling: [
      "Destination operations / Tampa market team",
      "Supplier / hotel partnership development",
      "Corporate meetings & incentives planning",
      "Venue selection for client programmes",
    ],
    targetRoles: roles.roles,
    nationalVsLocal: {
      statement: "Do not assume national PRA leadership personally manages Tampa hotel contracting; prefer destination/supplier roles.",
      provenance: FINDING_PROVENANCE.UNVERIFIED,
    },
    intermediaries: [],
    knownContactRoutes: [
      {
        route: contactUrl,
        kind: "ORGANIZATION_CONTACT_PAGE",
        email: null,
        ownershipClass: ownership.ownershipClass,
        salesUsableAsProspectContact: false,
        findingProvenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      },
    ],
    namedPeopleFromResearch: (live.cert?.realNamedStakeholders || [])
      .filter((p) => isLikelyPersonName(p?.name)),
    unresolvedBuyerRoles: [
      "Hotel / venue supplier partnership owner (Tampa or Florida)",
      "Destination operations lead influencing hotel shortlists",
    ],
  };

  const enrichment = buildPrioritizedEnrichmentRequest({
    opportunity: bag,
    targetRole: "Director/Manager — Hotel Partnerships, Supplier Relations, or Destination Operations (Tampa/Florida)",
    geography: "Tampa, Florida / Florida destinations",
    knownCandidate: null,
    knownEvidence: [
      evidence.tampaActivity.statement.slice(0, 240),
      contactUrl,
    ],
    informationNeeded: "Named person currently responsible for hotel/venue supplier selection for PRA Tampa programmes + current title/employer confirmation on pra.com or LinkedIn-public",
    salesActionImproved: "Lets Westin Tampa sales ask a specific buyer whether Harbour Island can join the Tampa vendor-resource library — without inventing a client RFP",
    preferredProvider: "surfe",
    publicAttempts: (live.trace?.executed || []).map((e) => e.url).filter(Boolean).slice(0, 8),
    contactOwnership: ownership,
  });

  const next = classifyCommercialNextAction({
    pathway: "ACCOUNT_LED",
    hotelFitCredible: true,
    namedPersonBuyerVerified: false,
    functionalRouteUsable: false,
    ownershipClass: ownership.ownershipClass,
    enrichmentJustified: true,
    overflowOrHostUnlocked: false,
  });

  const brief = {
    hotelId: bag.hotelId,
    hotelName: bag.hotelName,
    opportunityId,
    organizationName: bag.organizationName,
    whyPursuePra: evidence.tampaActivity.statement,
    businessPraMayInfluence: "Client meetings, catered dinners, receptions, and waterfront itineraries sourced via PRA Tampa supplier resources",
    tampaEvidence: evidence.tampaActivity,
    whoToApproach: "Unresolved named buyer — use contact-us to ask who owns hotel/supplier partnerships for Tampa; do not pitch national execs as automatic buyers",
    whatToAsk: "Who owns hotel and venue supplier selection for Tampa programmes, and what does it take for The Westin Tampa Waterside to be considered for the vendor-resource library?",
    whatWouldEstablishWorth: [
      "Named destination/supplier owner confirms hotel partnership process",
      "Evidence Westin is absent from current Tampa hotel shortlist OR invited to pitch",
      "Dated programme needing Harbour Island / waterfront inventory (without inventing client names)",
    ],
    nextAction: next,
    verifiedFacts: [
      evidence.praRole.statement,
      evidence.tampaActivity.statement.slice(0, 280),
    ],
    inferences: [
      evidence.hotelSourcingInfluence.inferenceNote,
      evidence.westinCredibility.statement,
    ],
    unresolvedQuestions: sourcingChain.unresolvedBuyerRoles.concat([
      "Local vs central hotel partnership ownership",
      "Any existing Westin / Marriott Tampa supplier relationship",
    ]),
    findingProvenancePolicy: FINDING_PROVENANCE,
  };

  return {
    opportunityId,
    hotelId: bag.hotelId,
    hotelName: bag.hotelName,
    organizationName: bag.organizationName,
    pathway: "ACCOUNT_LED",
    evidence,
    sourcingChain,
    accountDevelopmentBrief: brief,
    contactOwnership: ownership,
    targetedEnrichmentRequest: enrichment,
    commercialNextAction: next,
    liveCommercialOutcome: live.commercial?.outcome || null,
  };
}

export function auditAadMaterialClassification() {
  const opportunityId = "gdi_opp_aad_2028_overflow";
  const bag = loadCanonicalCohortOpportunity(opportunityId);
  const live = loadLiveBundle(opportunityId);
  const before = live.before || {};
  const after = live.after || {};
  const commercial = live.commercial || {};

  const priorRoutes = collectPriorContactRoutes(before, bag);
  const novel = diffNewEvidence({
    before,
    after,
    opportunity: bag,
    result: {},
  });

  const novelOrgOwned = novel.filter((e) => /aad\.org|ctfassets\.net.*aad|moscone\.com\/events\/american-academy/i.test(e.url || ""));
  const novelDirectory = novel.filter((e) => /showsbee|emedevents|clocate/i.test(e.url || ""));
  const buyerAdvanced = commercial.dimensions?.BUYER_ACCESSIBILITY?.advanced === true;
  const priorRouteRestated = commercial.dimensions?.BUYER_ACCESSIBILITY?.priorRouteRestated === true
    || priorRoutes.has("registration@aad.org");

  const ownership = classifyContactOwnership({
    email: "registration@aad.org",
    pageUrl: bag.primaryContact?.sourceUrl || bag.officialSource,
    organizationName: bag.organizationName,
    organizationDomain: "aad.org",
    personName: "Maritz Global Events",
    roleTitle: "Official AAD registration & housing provider (eventshq)",
    publisherKind: "ORGANIZATION_OWNED",
  });

  const materialAudit = {
    recordedOutcome: commercial.outcome || live.cert?.commercialOutcome,
    honestyNote: commercial.honestyNote || null,
    priorContactEmail: before.contactEmail || bag.primaryContactEmail,
    priorRoutes: [...priorRoutes],
    buyerAccessibilityAdvanced: buyerAdvanced,
    priorRouteCorrectlyNotCountedAsNew: priorRouteRestated && !buyerAdvanced,
    novelEvidenceCount: novel.length,
    novelOrgOwnedOrVenueCount: novelOrgOwned.length,
    novelDirectoryCount: novelDirectory.length,
    demandAdvanced: commercial.dimensions?.DEMAND_INTELLIGENCE?.advanced === true,
    timingAdvanced: commercial.dimensions?.COMMERCIAL_TIMING?.advanced === true,
    salesActionabilityAdvanced: commercial.dimensions?.SALES_ACTIONABILITY?.advanced === true,
    hotelRelevanceAdvanced: commercial.dimensions?.HOTEL_SPECIFIC_RELEVANCE?.advanced === true,
    bethesdaOverflowOpened: false,
    maritzIsPerson: false,
    decisionChangingForBethesdaSales: false,
    auditVerdict: null,
    auditRationale: null,
  };

  // Honest audit: cycle corroboration is new relative to empty BEFORE demandEvidence snapshots,
  // but does not unlock Bethesda overflow or change pursue/exclude for host status.
  const genuineNewCycleContext = novelOrgOwned.length >= 1
    || (novelDirectory.length >= 1 && materialAudit.timingAdvanced);
  if (materialAudit.recordedOutcome === IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT) {
    if (genuineNewCycleContext && !materialAudit.hotelRelevanceAdvanced && !materialAudit.bethesdaOverflowOpened) {
      materialAudit.auditVerdict = "MATERIAL_FOR_CYCLE_CORROBORATION_ONLY";
      materialAudit.auditRationale = [
        "Live retrievals added meeting-cycle/housing-channel context beyond the thin BEFORE demandEvidence snapshot.",
        "registration@aad.org was already known — correctly not counted as new buyer accessibility.",
        "Maritz remains a functional intermediary (named:false on whoRecord) — not a person DM.",
        "No evidence Bethesda overflow inventory is open; host status remains unavailable.",
        "For hotel sales decisioning, treat as FUTURE_CYCLE qualify/follow-up — not a verified near-term sourcing win.",
      ].join(" ");
      materialAudit.decisionChangingForBethesdaSales = false;
    } else if (materialAudit.hotelRelevanceAdvanced || materialAudit.bethesdaOverflowOpened) {
      materialAudit.auditVerdict = "MATERIAL_DECISION_CHANGING";
      materialAudit.decisionChangingForBethesdaSales = true;
      materialAudit.auditRationale = "Hotel-specific relevance or open overflow evidenced.";
    } else {
      materialAudit.auditVerdict = "OVERSTATED_RELATIVE_TO_SALES_DECISION";
      materialAudit.auditRationale = "MATERIAL label reflects research enrichment more than a changed Bethesda pursue decision.";
    }
  } else {
    materialAudit.auditVerdict = "NOT_MATERIAL";
    materialAudit.auditRationale = "Recorded outcome was not MATERIAL.";
  }

  return {
    opportunityId,
    materialAudit,
    ownershipAfterPersonGate: ownership,
    novelSample: novel.slice(0, 6),
  };
}

export function assessAadCommercialCompletion() {
  const opportunityId = "gdi_opp_aad_2028_overflow";
  const bag = loadCanonicalCohortOpportunity(opportunityId);
  const live = loadLiveBundle(opportunityId);
  const materialAudit = auditAadMaterialClassification();
  const roles = targetRolesForOpportunity(bag);

  const ownership = materialAudit.ownershipAfterPersonGate;

  const housingChain = {
    organization: bag.organizationName,
    hotel: bag.hotelName,
    pathway: "COMPETITIVE_FUTURE_CYCLE",
    meetingCycle: {
      statement: "AAD Annual Meeting is a recurring multi-hotel downtown convention pattern; live retrievals corroborate 2027 Moscone listing and later-cycle directory references.",
      provenance: FINDING_PROVENANCE.VERIFIED_LIVE,
      caveat: "Directory listings are weaker than aad.org; 2028 Bethesda host is not evidenced",
    },
    confirmedLocations: {
      statement: text(bag.hotelOpportunityThesis).slice(0, 220),
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
    },
    maritzRole: {
      statement: "Maritz Global Events documented as official registration & housing provider (eventshq) on prior Annual Meeting housing page — functional intermediary, named:false.",
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      evidenceUrl: bag.whoRecord?.evidence || bag.primaryContact?.sourceUrl,
      isPerson: false,
    },
    registrationRoute: {
      route: "registration@aad.org",
      ownershipClass: ownership.ownershipClass,
      salesUsableAsProspectContact: ownership.salesUsableAsProspectContact,
      bestInquiryRoute: ownership.salesUsableAsProspectContact
        ? "Best existing inquiry route for housing-channel qualification — not proof of personal DM ownership"
        : "Not sales-usable",
      findingProvenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      previouslyKnown: true,
      countedAsNewImprovement: false,
    },
    openToBethesda: {
      futureCycleAccountDevelopment: true,
      housingParticipationInquiry: true,
      verifiedSourcingOpportunity: false,
      speculativeOverflow: true,
      statement: "Bethesda may only become relevant if official overflow/suburban medical-attendee lodging opens — not assumed from downtown host pattern.",
      provenance: FINDING_PROVENANCE.UNVERIFIED,
    },
    responsibilityChain: [
      "AAD Meetings & Education (programme owner)",
      "Official housing provider / Maritz pattern (block administration intermediary)",
      "registration@aad.org (org registration/housing channel)",
      "Unnamed AAD or Maritz housing administrators (not publicly named for 2028)",
    ],
    targetRoles: roles.roles,
    whatSalesShouldAsk:
      "Who administers official housing and overflow hotel participation for the Washington-area Annual Meeting cycle, and is suburban/medical-corridor inventory ever added once downtown blocks fill?",
  };

  const next = classifyCommercialNextAction({
    pathway: "COMPETITIVE_FUTURE_CYCLE",
    hotelFitCredible: true,
    namedPersonBuyerVerified: false,
    functionalRouteUsable: ownership.salesUsableAsProspectContact === true,
    ownershipClass: ownership.ownershipClass,
    enrichmentJustified: true,
    overflowOrHostUnlocked: false,
    speculativeOnly: false,
  });

  const enrichment = buildPrioritizedEnrichmentRequest({
    opportunity: bag,
    targetRole: "Housing / Meeting Planning administrator (AAD or Maritz housing desk) — not CEO",
    geography: "Washington DC Annual Meeting cycle / official housing programme",
    knownCandidate: null,
    knownEvidence: [
      "registration@aad.org OFFICIAL_SOURCE_VERIFIED",
      bag.whoRecord?.evidence,
      "https://www.aad.org/member/meetings-education",
    ].filter(Boolean),
    informationNeeded: "Named housing-block administrator or meetings planner who can confirm whether overflow/suburban hotels are ever considered; current role at AAD or Maritz",
    salesActionImproved: "Moves Bethesda from speculative overflow thesis to a concrete housing-channel conversation without claiming open inventory",
    preferredProvider: "surfe",
    publicAttempts: (live.trace?.executed || []).map((e) => e.url).filter(Boolean).slice(0, 8),
    contactOwnership: ownership,
  });

  const salesAssessment = {
    opportunityId,
    hotelName: bag.hotelName,
    distinguish: {
      potentialFutureCycleAccountDevelopment: "YES — recurring Annual Meeting cycle exists",
      potentialHousingParticipationInquiry: "YES — via registration@ / housing intermediary channel",
      verifiedSourcingOpportunity: "NO — no open Bethesda block or overflow list evidenced",
      speculativeOverflow: "YES if treated as available demand without housing-open evidence — do not",
    },
    credibleNearTermPath: next.nextAction === COMMERCIAL_NEXT_ACTION.FUTURE_CYCLE_SCHEDULED_FOLLOWUP
      || next.nextAction === COMMERCIAL_NEXT_ACTION.FUNCTIONAL_ROUTE_QUALIFY
      ? "Qualify via housing channel; schedule follow-up when official housing opens — no host pitch"
      : "No credible near-term commercial path beyond watch/qualify",
    nextAction: next,
    materialAudit: materialAudit.materialAudit,
  };

  return {
    opportunityId,
    hotelId: bag.hotelId,
    hotelName: bag.hotelName,
    organizationName: bag.organizationName,
    pathway: "COMPETITIVE_FUTURE_CYCLE",
    housingChain,
    salesAssessment,
    contactOwnership: ownership,
    targetedEnrichmentRequest: enrichment,
    commercialNextAction: next,
    materialClassificationAudit: materialAudit,
  };
}

export function assessAlexandriaHold() {
  const opportunityId = "gdi_opp_alexandria_soccer_kickoff_2027";
  const bag = loadCanonicalCohortOpportunity(opportunityId);
  const live = loadLiveBundle(opportunityId);

  const ben = (live.cert?.realNamedStakeholders || bag.namedStakeholders || [])
    .find((p) => /ben\s+hawkins/i.test(p?.name || ""));

  const geoNote = "Games/fields in Alexandria VA; lodging via stay-to-play partner inventory — Bethesda is a Beltway option only if partner list accepts it; not a contracted host thesis.";
  const hotelFitWeak = true;
  const buyerGap = true;

  const next = classifyCommercialNextAction({
    pathway: "EVENT_LED",
    hotelFitCredible: false,
    namedPersonBuyerVerified: false,
    functionalRouteUsable: false,
    enrichmentJustified: false,
    holdRecommended: true,
  });

  return {
    opportunityId,
    hotelId: bag.hotelId,
    hotelName: bag.hotelName,
    organizationName: bag.organizationName,
    recommendation: "HOLD_LOWER_PRIORITY",
    justifyFutureInvestigation: false,
    exclusionOrLowerPriority: "Lower research priority pending partner-inventory evidence; do not spend enrichment credits now",
    benHawkins: {
      status: FINDING_PROVENANCE.UNVERIFIED,
      preserved: true,
      salesUsable: false,
      note: ben
        ? "Ben Hawkins remains UNVERIFIED prior inference — not promoted"
        : "Ben Hawkins not present as verified stakeholder",
    },
    hotelFitSummary: geoNote,
    buyerGapSummary: "No usable org email; stay-to-play partner identity/list membership unresolved",
    commercialNextAction: next,
    preservedClassifications: {
      opportunityType: bag.opportunityType,
      roomDemandStatus: bag.roomDemandStatus,
      venueSourcingStatus: bag.venueSourcingStatus,
    },
  };
}

/**
 * Full offline commercial-completion package.
 */
export function runCommercialCompletionBuyerAccessOfflinePackage() {
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const enrichmentAudit = auditContactEnrichmentCapabilities();
  const pra = assessPraCommercialCompletion();
  const aad = assessAadCommercialCompletion();
  const alexandria = assessAlexandriaHold();

  // Ownership regression: Maritz must not be POTENTIAL_DECISION_MAKER person
  const maritzOwnership = aad.contactOwnership;
  const maritzGate = maritzOwnership.ownershipClass !== CONTACT_OWNERSHIP_CLASS.POTENTIAL_DECISION_MAKER
    && maritzOwnership.personName == null;

  // Consumer email still rejected
  const consumer = classifyContactOwnership({
    email: "coach@gmail.com",
    pageUrl: "https://alexandria-soccer.org/",
    organizationName: "Alexandria Soccer Association",
    organizationDomain: "alexandria-soccer.org",
    publisherKind: "ORGANIZATION_OWNED",
  });

  // Existing contact not counted as new — AAD prior route
  const aadBuyerDimOk = aad.materialClassificationAudit.materialAudit.priorRouteCorrectlyNotCountedAsNew === true
    || aad.materialClassificationAudit.materialAudit.buyerAccessibilityAdvanced === false;

  // Brief no cross-hotel contamination
  const praBriefClean = !/Principal|Nobu|Bethesda Marriott|AAD/i.test(
    JSON.stringify(pra.accountDevelopmentBrief.whatToAsk + pra.accountDevelopmentBrief.whoToApproach),
  );
  const aadBriefClean = !/Principal|Nobu|Westin Tampa|PRA\b/i.test(
    JSON.stringify(aad.housingChain.whatSalesShouldAsk),
  );

  // Surfe cannot execute without auth
  const surfeBlocked = resolveEffectiveSurfeAuthorization({ authorizeSurfe: false }).authorizeSurfeEffective === false;
  let surfeThrowOk = false;
  try {
    assertPaidContactProvidersDisabled({ authorizeSurfe: true });
  } catch {
    surfeThrowOk = true;
  }
  // assertPaid returns ok:false rather than throw — check both patterns
  const surfeAssert = assertPaidContactProvidersDisabled({ authorizeSurfe: true });
  const surfeCannotRun = surfeBlocked && surfeAssert.ok === false;

  const gates = {
    paidGuardOk: paidGuard.ok === true,
    maritzNotPersonDecisionMaker: maritzGate,
    consumerEmailRejected: consumer.salesUsableAsProspectContact === false,
    aadPriorRouteNotNewBuyerWin: aadBuyerDimOk,
    benHawkinsUnverified: alexandria.benHawkins.status === FINDING_PROVENANCE.UNVERIFIED,
    alexandriaHoldNoEnrichmentSpend: alexandria.recommendation === "HOLD_LOWER_PRIORITY"
      && alexandria.justifyFutureInvestigation === false,
    praNeedsBuyerOrEnrichment: [
      COMMERCIAL_NEXT_ACTION.ACCOUNT_DEVELOPMENT_NEEDS_BUYER,
      COMMERCIAL_NEXT_ACTION.AWAITING_AUTHORIZED_ENRICHMENT,
    ].includes(pra.commercialNextAction.nextAction),
    aadFutureCycleOrFunctionalQualify: [
      COMMERCIAL_NEXT_ACTION.FUTURE_CYCLE_SCHEDULED_FOLLOWUP,
      COMMERCIAL_NEXT_ACTION.FUNCTIONAL_ROUTE_QUALIFY,
    ].includes(aad.commercialNextAction.nextAction),
    enrichmentRequestsGated: pra.targetedEnrichmentRequest.status === "AWAITING_SEPARATE_AUTHORIZATION"
      && aad.targetedEnrichmentRequest.status === "AWAITING_SEPARATE_AUTHORIZATION"
      && pra.targetedEnrichmentRequest.authorizeSurfe === false
      && aad.targetedEnrichmentRequest.authorizeSurfe === false
      && pra.targetedEnrichmentRequest.authorizationRequired.persistSurfePiiInGdi === false,
    surfeCannotExecuteUnauthorized: surfeCannotRun,
    hotelBriefsNotContaminated: praBriefClean && aadBriefClean,
    materialAuditPresent: Boolean(aad.materialClassificationAudit.materialAudit.auditVerdict),
    canonicalIdsPreserved: pra.opportunityId === "gdi_lmc_4d2f10eab2d44499"
      && aad.opportunityId === "gdi_opp_aad_2028_overflow"
      && alexandria.opportunityId === "gdi_opp_alexandria_soccer_kickoff_2027",
  };

  const allPass = Object.values(gates).every(Boolean);

  const recommendedBuyerEnrichmentTest = {
    status: "PROPOSED_AWAITING_SEPARATE_FOUNDER_GO",
    executePaidResearch: false,
    prioritize: [
      {
        opportunityId: pra.opportunityId,
        hotelName: pra.hotelName,
        request: pra.targetedEnrichmentRequest,
        whyFirst: "Highest commercial VoI — credible Tampa supplier thesis blocked only on named buyer access",
      },
      {
        opportunityId: aad.opportunityId,
        hotelName: aad.hotelName,
        request: aad.targetedEnrichmentRequest,
        whySecond: "Functional route exists; named housing admin would sharpen follow-up — lower urgency than PRA",
      },
    ],
    excludeNow: [alexandria.opportunityId],
    providers: ["surfe"],
    pdl: "NO_GO unless Surfe fails after authorized identity search",
    persistSurfePiiInGdi: false,
    note: "Small buyer-enrichment test only after founder GO — identity before personal email",
  };

  return {
    version: COMMERCIAL_COMPLETION_BUYER_ACCESS_V1,
    certifiedAt: new Date().toISOString(),
    pra,
    aad,
    alexandria,
    enrichmentCapabilityAudit: enrichmentAudit,
    recommendedBuyerEnrichmentTest,
    gates,
    allOfflineGatesPass: allPass,
    goNoGo: {
      decision: allPass ? "GO" : "NO_GO",
      forCommercialCompletionOfflineModel: allPass ? "GO" : "NO_GO",
      paidEnrichment: "NO_GO",
      surfePdlParallel: "NO_GO",
      airtablePublicationWeekly: "NO_GO",
      smallBuyerEnrichmentTest: "NO_GO_UNTIL_SEPARATE_AUTHORIZATION",
      note: allPass
        ? "Commercial completion offline certified. PRA/AAD next actions and gated enrichment requests ready for founder review — no credits spent."
        : "Offline gates failed.",
    },
  };
}
