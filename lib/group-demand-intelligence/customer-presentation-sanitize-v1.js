/**
 * Shared customer-presentation sanitization for list + detail paths.
 * Hotel-agnostic: no Mayflower/San Diego-only branches.
 *
 * - Assessment-status prose is not a sales summary (belongs in fit caveats).
 * - Provider brand names never reach customer-facing copy.
 * - Withheld overall scores never fall back to stale flat numerics.
 * - Scaling-company ops notes / NAMED_DIRECT-on-org-email / secondary
 *   contactability are normalized via scaling-companies-sales-presentation-v1.
 */

import {
  applyScalingSalesPresentation,
  scrubInternalResearchLanguage,
} from "./scaling-companies-sales-presentation-v1.js";
import { applyUnifiedWhoToContactStandard } from "./unified-who-to-contact-v1.js";
import { attachUniversalTileContactPreview } from "./universal-tile-contact-preview-v1.js";
import {
  applyPortfolioCommercialIntegrity,
  recalibrateEvidenceConfidence,
} from "./research/portfolio-commercial-integrity-v1.js";
import { applyHotelFitPresentationHonesty } from "./hotel-fit-presentation-honesty-v1.js";
import {
  isScalingCompaniesSegment,
  resolveSegmentAuthoritativePresentation,
} from "./scaling-drawer-presentation-contract-v1.js";
import { buildScalingDrawerNormalizedModel } from "./scaling-drawer-normalized-model-v1.js";
import { buildEventsDrawerNormalizedModel } from "./events-drawer-normalized-model-v1.js";
import {
  attachEventsOpportunityOverview,
  isAssociationConferenceEventsOpportunity,
} from "./events-opportunity-overview-v1.js";
import {
  buildSharedTenSectionDrawerDto,
  resolveOpportunityDemandSegmentId,
} from "./demand-segments/shared-ten-section-drawer-contract-v1.js";
import {
  applyPreferredLodgingPresentation,
  isPreferredLodgingPartnershipOpportunity,
} from "./preferred-lodging-partnership/index.js";
import {
  applyProfessionalEventPlannerPresentation,
  isProfessionalEventPlannerOpportunity,
} from "./professional-event-planners/index.js";
import { attachNeedDateMatchToOpportunity } from "./need-date-demand/need-period-drawer-component-v1.js";
import { attachOpportunityRevenueStreamIcons } from "./opportunity-revenue-stream-icons-v1.js";
import { attachContactResearchCompletionGate } from "./contact-research-completion-gate-v1.js";
import { applyCanonicalCustomerFacingTitle } from "./demand-segments/universal-opportunity-identity-v1.js";

export const CUSTOMER_PRESENTATION_SANITIZE_VERSION = "gdi-customer-presentation-sanitize-v18";

function text(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.replace(/\s+/g, " ").trim();
  if (typeof v === "number" || typeof v === "boolean") return String(v).trim();
  if (Array.isArray(v)) {
    return v.map(text).filter(Boolean).join(" · ").replace(/\s+/g, " ").trim();
  }
  if (typeof v === "object") {
    // Never emit "[object Object]" into customer copy.
    const nested =
      v.claim ||
      v.text ||
      v.body ||
      v.summary ||
      v.thesis ||
      v.label ||
      v.title ||
      v.note ||
      null;
    if (nested != null && nested !== v) return text(nested);
    return "";
  }
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^\[object Object\]$/i, "");
}

/**
 * run-052 Issue 5 (RC-05): detect webpage scrape / marketing markup leaking
 * into customer Account/occasion narratives.
 */
export function isContaminatedWebScrapeNarrative(raw) {
  const t = String(raw || "");
  if (!t) return false;
  return (
    /#{1,6}\s+\S/.test(t) ||
    /##\s*MAF/i.test(t) ||
    /--\s*Days\s*--/i.test(t) ||
    /Days\s*--\s*Hours\s*--\s*Minutes/i.test(t) ||
    (/cookies?\b/i.test(t) && /accept|consent|pol[ií]tica|utilizamos/i.test(t)) ||
    /¿POR QUÉ EXISTE/i.test(t) ||
    /skip to (?:main )?content/i.test(t) ||
    /men[uú] principal|navegaci[oó]n/i.test(t)
  );
}

/**
 * Build a customer-safe occasion narrative without inventing commercial facts.
 * Contaminated scrapes are truncated to the lead before markup, else title.
 */
export function scrubCustomerOccasionNarrative(raw, meta = {}) {
  const title = text(meta.title);
  const org = text(meta.organizationName || meta.companyName);
  const fallback = title || org || null;
  let t = String(raw == null ? "" : raw)
    .replace(/\r\n/g, "\n")
    .trim();
  if (!t) return fallback;

  if (isContaminatedWebScrapeNarrative(t)) {
    const beforeHeading = t.split(/\n?#{1,6}\s+/)[0] || "";
    let lead = beforeHeading
      .replace(/--\s*Days\s*--[\s\S]*$/i, "")
      .replace(/\s+/g, " ")
      .trim();
    // Drop trailing promotional crumbs that survived the heading cut.
    lead = lead
      .replace(/\s*[|·•]\s*$/g, "")
      .replace(/\s{2,}/g, " ")
      .trim();
    if (lead.length > 280) {
      lead = `${lead.slice(0, 277).replace(/\s+\S*$/, "").trim()}…`;
    }
    if (
      !lead ||
      lead.length < 12 ||
      isContaminatedWebScrapeNarrative(lead)
    ) {
      return fallback;
    }
    return lead;
  }

  return t.replace(/\s+/g, " ").trim();
}

/**
 * Derive current customer presentation actions / whyThisHotel from the
 * segment-authoritative gold brief so legacy salesBrief / recommendedAction
 * cannot override corrected research (run-052 Issue 2).
 *
 * Phase 1 Scaling drawer standardization: Scaling Companies must prefer
 * commercialIntelligenceBrief and must NOT let a contaminating
 * associationEventsGoldBrief win presentation SoT.
 */
export function applyAuthoritativeGoldBriefPresentation(opportunity = {}) {
  // Segment-aware SoT (Scaling vs Association) — shared contract.
  let next = resolveSegmentAuthoritativePresentation(opportunity);

  // Scrub nav/cookie pollution from titles (portfolio RC-05).
  if (isContaminatedWebScrapeNarrative(next.title) || /skip to(?: main)? content/i.test(text(next.title))) {
    const cleanedTitle = scrubCustomerOccasionNarrative(next.title, {
      title: null,
      organizationName: next.organizationName || next.companyName,
    });
    if (cleanedTitle && cleanedTitle !== text(next.title)) {
      if (!next.researchProvenance || typeof next.researchProvenance !== "object") {
        next.researchProvenance = {};
      }
      next.researchProvenance.rawTitle = String(next.title);
      next.title = cleanedTitle;
    } else if (/skip to(?: main)? content/i.test(text(next.title))) {
      next.title = text(next.title)
        .replace(/\bSkip to(?: main)? content\b/gi, " ")
        .replace(/\s+/g, " ")
        .trim();
    }
  }

  // run-052 Issue 5 (RC-05): never let scraped webpage copy become Account/occasion.
  const narrativeCandidates = [
    next.summaryWhat,
    next.whatIsHappening,
    next.salesBriefActivity,
    next.eventHistoryAndActivity,
    next.associationEventsGoldBrief?.eventHistoryAndActivity,
    next.salesBrief?.companyAndLocalActivity,
  ];
  const dirtyCandidate = narrativeCandidates.find((v) =>
    isContaminatedWebScrapeNarrative(v),
  );
  if (dirtyCandidate || narrativeCandidates.some((v) => isContaminatedWebScrapeNarrative(v))) {
    if (!next.researchProvenance || typeof next.researchProvenance !== "object") {
      next.researchProvenance = {};
    }
    if (!next.researchProvenance.rawWhatIsHappening) {
      const preserve =
        narrativeCandidates.find((v) => isContaminatedWebScrapeNarrative(v)) ||
        next.whatIsHappening;
      if (text(preserve)) {
        next.researchProvenance.rawWhatIsHappening = String(preserve);
      }
    }
    const cleaned = scrubCustomerOccasionNarrative(
      dirtyCandidate || next.summaryWhat || next.whatIsHappening || next.title,
      {
        title: next.title,
        organizationName: next.organizationName || next.companyName,
      },
    );
    if (cleaned) {
      next.summaryWhat = cleaned;
      next.whatIsHappening = cleaned;
      next.salesBriefActivity = cleaned;
      next.eventHistoryAndActivity = cleaned;
      if (next.associationEventsGoldBrief && typeof next.associationEventsGoldBrief === "object") {
        next.associationEventsGoldBrief = {
          ...next.associationEventsGoldBrief,
          eventHistoryAndActivity: cleaned,
        };
      }
      if (next.salesBrief && typeof next.salesBrief === "object") {
        const companyAct = text(next.salesBrief.companyAndLocalActivity);
        if (!companyAct || isContaminatedWebScrapeNarrative(companyAct)) {
          next.salesBrief = {
            ...next.salesBrief,
            companyAndLocalActivity: cleaned,
          };
        }
      }
      next.occasionNarrativeScrubbed = true;
    }
  }

  return next;
}

/** Top-level flags / IDs that must never reach customer list/detail/share DTOs. */
export const CUSTOMER_INTERNAL_PROVIDER_KEYS = Object.freeze([
  "webhoundUsed",
  "ampfyUsed",
  "surfeUsed",
  "parallelUsed",
  "providerRunId",
  "enrichmentId",
  "creditsConsumed",
  "providerCostUsd",
  "surfeRequestId",
  "surfeEnrichmentEligible",
  "surfeEnriched",
  "surfeEnrichmentChannel",
  "surfeEnrichmentStatus",
  "webhound",
  "webhoundRunId",
  "webhoundTaskId",
  "webhoundInternal",
  "webhoundCalls",
  "contactResearchAudit",
  "parallelTaskId",
  "researchProvider",
  "researchLevel",
  // Internal ORS / supervisor dossiers — never customer-facing (also avoids
  // `[object Object]` stringification leaks from nested verifiedFindings).
  "supervisorV2",
  "orsSupervisorState",
  "maturationMemory",
  "researchMemoryInternal",
  "bethesdaWinThesis",
]);

/** Keys stripped from evidence / contact nested objects (URLs and titles stay). */
export const CUSTOMER_EVIDENCE_INTERNAL_KEYS = Object.freeze([
  "researchProvider",
  "researchLevel",
  "providerRunId",
  "enrichmentId",
  "creditsConsumed",
  "providerCostUsd",
  "surfeRequestId",
  "surfeEnrichmentEligible",
  "surfeEnriched",
  "surfeEnrichmentChannel",
  "surfeEnrichmentStatus",
  "webhound",
  "webhoundRunId",
  "webhoundTaskId",
  "webhoundInternal",
  "webhoundUsed",
  "requestId",
  "provider",
  "enrichmentProvider",
]);

function normalizeProseKey(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Reject hotel-thesis fallbacks that merely repeat the occasion summary. */
function distinctFromOccasion(candidate, occasion) {
  const a = normalizeProseKey(candidate);
  const b = normalizeProseKey(occasion);
  if (!a) return null;
  if (b && a === b) return null;
  if (b && a.length > 48 && b.includes(a.slice(0, Math.min(96, a.length)))) {
    return null;
  }
  return candidate;
}

export function isAssessmentStatusProse(text) {
  const t = String(text || "");
  return (
    /dimension-level hotel-fit assessment/i.test(t) ||
    /overall numeric (?:score )?(provisional\/)?withheld/i.test(t) ||
    /weights renormalized/i.test(t) ||
    /applicable dimensions still lacking evidence/i.test(t) ||
    /^provisional hotel-fit score\b/i.test(t)
  );
}

export function scrubProviderBrandNames(text) {
  if (text == null || text === "") return text == null ? null : "";
  return String(text)
    .replace(/\bpreviously verified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bverified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bvia Surfe\b/gi, "from prior verified enrichment")
    .replace(/\bSurfe only if\b/gi, "Paid enrichment only if")
    .replace(/\bSurfe\b/gi, "enrichment provider")
    .replace(/\bWebhound\b/gi, "research provider")
    .replace(/\bwebhound\b/gi, "research provider")
    .replace(/\bsurfe[_-]?enrich\w*/gi, "")
    .replace(/\bApollo\.io\b/gi, "enrichment provider")
    .replace(/\bZoomInfo\b/gi, "enrichment provider")
    .replace(/\bParallel\.ai\b/gi, "research provider")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Map internal channel-source enums to customer-safe labels.
 * Preserves first-party / verified meaning; drops provider brand tokens.
 */
export function scrubChannelSourceLabel(value) {
  if (value == null || value === "") return value == null ? null : "";
  let s = String(value).trim();
  if (!s) return "";
  s = s
    .replace(/\bWEBHOUND[_-]?/gi, "")
    .replace(/\bSURFE[_-]?/gi, "")
    .replace(/\bPARALLEL[_-]?/gi, "")
    .replace(/\bAPOLLO[_-]?/gi, "")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!s) return "VERIFIED_PUBLIC";
  if (/^FIRST_PARTY$/i.test(s)) return "FIRST_PARTY";
  if (/^OFFICIAL/i.test(s)) return s.toUpperCase();
  return s;
}

function stripInternalKeys(obj, keys) {
  if (!obj || typeof obj !== "object") return obj;
  const next = { ...obj };
  for (const key of keys) delete next[key];
  return next;
}

function scrubEvidenceItem(item) {
  if (!item || typeof item !== "object") return item;
  const next = stripInternalKeys(item, CUSTOMER_EVIDENCE_INTERNAL_KEYS);
  if (next.sourceTitle) next.sourceTitle = scrubProviderBrandNames(next.sourceTitle);
  if (next.value != null && typeof next.value === "string") {
    next.value = scrubProviderBrandNames(next.value);
  }
  if (next.extractedText) next.extractedText = scrubProviderBrandNames(next.extractedText);
  return next;
}

function scrubEvidenceList(list) {
  if (!Array.isArray(list)) return list || null;
  return list.map(scrubEvidenceItem);
}

function salesFacingField(primary, fallback) {
  const clean = (v) => {
    const s = scrubProviderBrandNames(text(v));
    if (!s || /^\[object Object\]$/i.test(s)) return null;
    return s;
  };
  const a = clean(primary);
  if (a && !isAssessmentStatusProse(a)) return a;
  const b = clean(fallback);
  if (b && !isAssessmentStatusProse(b)) return b;
  return null;
}

/**
 * Prefer structured assessment over a stale flat Hotel Fit column.
 * When overall numeric is withheld, never surface a leftover score.
 * Uses the shared assessment contract when a structured assessment is present.
 */
export function reconcileHotelFitScoreForDisplay(opportunity = {}) {
  const structured =
    opportunity.hotelFitAssessment && typeof opportunity.hotelFitAssessment === "object"
      ? opportunity.hotelFitAssessment
      : null;
  if (structured) {
    // Lazy import avoided — contract lives next to assessment; inline mirror of
    // withheld precedence so display cannot revive stale flats when assessment says no.
    const basis = structured.calculationBasis || opportunity.hotelFitCalculationBasis || {};
    const status = String(
      structured.overallStatus || opportunity.hotelFitOverallStatus || ""
    ).toLowerCase();
    const rationale = String(
      structured.summary ||
        opportunity.hotelFitRationale ||
        opportunity.hotelFitScoreNote ||
        ""
    );
    const withheldExplicit =
      structured.numericScoreEmitted === false ||
      structured.scored === false ||
      basis.numericScoreEmitted === false ||
      status === "insufficient_evidence" ||
      /overall numeric withheld|withheld — insufficient|not scored — evidence/i.test(
        rationale
      ) ||
      /overall numeric withheld/i.test(String(basis.reason || ""));
    if (withheldExplicit) return null;
    if (structured.numericScoreEmitted === true && structured.hotelFitScore != null) {
      const n = Number(structured.hotelFitScore);
      return Number.isFinite(n) ? n : null;
    }
    if (structured.scored === true) {
      const raw = structured.numericScore ?? structured.hotelFitScore;
      if (raw != null) {
        const n = Number(raw);
        return Number.isFinite(n) ? n : null;
      }
    }
    // Version-only structured object must not revive flat score.
    if (structured.scoringVersion || structured.version) {
      if (structured.numericScoreEmitted !== true && structured.scored !== true) {
        return null;
      }
    }
  }

  const basis = opportunity.hotelFitCalculationBasis || {};
  const status = String(opportunity.hotelFitOverallStatus || "").toLowerCase();
  const rationale = String(opportunity.hotelFitRationale || opportunity.hotelFitScoreNote || "");
  const withheldExplicit =
    basis.numericScoreEmitted === false ||
    status === "insufficient_evidence" ||
    /overall numeric withheld|withheld — insufficient|not scored — evidence/i.test(rationale) ||
    /overall numeric withheld/i.test(String(basis.reason || ""));
  if (withheldExplicit) {
    return null;
  }
  const raw = opportunity.hotelFitScore;
  if (raw == null || raw === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function scrubContactNotes(contact) {
  if (!contact || typeof contact !== "object") return contact || null;
  let next = stripInternalKeys(contact, CUSTOMER_EVIDENCE_INTERNAL_KEYS);
  for (const key of [
    "whyThisContact",
    "notes",
    "verificationNotes",
    "enrichmentNotes",
    "sourceNotes",
    "relevanceEvidence",
  ]) {
    if (next[key]) next[key] = scrubProviderBrandNames(next[key]);
  }
  for (const key of [
    "phoneSource",
    "emailSource",
    "channelSource",
    "sourceType",
    "phoneSourceType",
    "emailSourceType",
  ]) {
    if (next[key]) next[key] = scrubChannelSourceLabel(next[key]);
  }
  // Never expose provider/cost metadata on customer contact objects
  delete next.provider;
  delete next.enrichmentProvider;
  delete next.providerCostUsd;
  delete next.surfeRequestId;
  delete next.surfeEnrichmentEligible;
  delete next.surfeEnriched;
  delete next.surfeEnrichmentChannel;
  delete next.surfeEnrichmentStatus;
  delete next.webhound;
  delete next.webhoundRunId;
  delete next.webhoundTaskId;
  delete next.webhoundInternal;
  delete next.webhoundUsed;
  delete next.requestId;
  delete next.enrichmentId;
  delete next.providerRunId;
  delete next.creditsConsumed;
  delete next.confidence;
  delete next._rank;
  return next;
}

function scrubContactList(list) {
  if (!Array.isArray(list)) return list || null;
  return list.map(scrubContactNotes);
}

/**
 * Apply presentation sanitization to one opportunity (list or detail).
 * Does not invent fit scores, contacts, or prose.
 */
export function sanitizeOpportunityForCustomerPresentation(opp = {}) {
  // Segment 8 subtypes: Preferred Lodging (venues) + Professional Event Planners
  // — normalize identity + conditional intelligence before shared drawer attach.
  let seed = opp;
  if (isPreferredLodgingPartnershipOpportunity(opp)) {
    seed = applyPreferredLodgingPresentation(opp, {
      hotelId: opp.hotelId || null,
      name: opp.hotelDisplayName || opp.hotelName || null,
      lat: opp.hotelLat,
      lng: opp.hotelLng,
    });
  } else if (isProfessionalEventPlannerOpportunity(opp)) {
    seed = applyProfessionalEventPlannerPresentation(opp, {
      hotelId: opp.hotelId || null,
      name: opp.hotelDisplayName || opp.hotelName || null,
      market: opp.hotelMarket || opp.venueCity || null,
      city: opp.hotelCity || opp.venueCity || null,
    });
  }
  // Shared integrity: cross-hotel quarantine, honest evidence confidence, contact gates.
  // Hotel context comes from the opportunity's own hotelId (non-mutating for other hotels
  // beyond scrubbing foreign theses that name a different property).
  const integrityHotel = {
    hotelId: seed.hotelId || null,
    displayName: seed.hotelDisplayName || seed.hotelName || null,
  };
  let integrityApplied = applyPortfolioCommercialIntegrity(seed, integrityHotel);
  // run-052 Issue 2: derive current presentation actions from segment gold brief.
  integrityApplied = applyAuthoritativeGoldBriefPresentation(integrityApplied);
  // run-052 Issue 6: honest hotel-fit dimension / facilities presentation.
  integrityApplied = applyHotelFitPresentationHonesty(integrityApplied);
  const evidenceRecal = recalibrateEvidenceConfidence(integrityApplied);

  // Scaling-company sales presentation first (scrub ORS notes, fix contact path,
  // gate secondary on usable route). Then universal two-contact channel honesty.
  // When a commercial-intelligence brief is already persisted, do not rebuild
  // generic template copy that would re-promote scraped localConnection dumps.
  const scalingSeg = isScalingCompaniesSegment(integrityApplied);
  const hasCommercialBrief = Boolean(
    integrityApplied.salesBrief || integrityApplied.commercialIntelligenceBrief,
  );
  // Scaling: activity-first fields must beat industry-boilerplate summaryWhat /
  // companyAndLocalActivity leads ("A technology / AI company. …").
  const scalingActivityLead = scalingSeg
    ? text(
        integrityApplied.tileCustomerSummary ||
          integrityApplied.activityHeadline ||
          integrityApplied.growthHeadline ||
          integrityApplied.whatIsHappening ||
          "",
      )
    : "";
  const industryBoilerplateLead =
    /^(?:an?\s+)?(?:technology\s*\/\s*ai|technology|fintech|software|saas|ai)\s+company\./i;
  const summaryLooksLikeIndustryLead = industryBoilerplateLead.test(
    text(integrityApplied.summaryWhat),
  );
  const presented = hasCommercialBrief
    ? {
        ...integrityApplied,
        // Prefer already-scrubbed summaryWhat; never re-promote raw scrape via
        // whatIsHappening → summaryWhat fallback (RC-05).
        // Scaling: prefer CIB activity over legacy salesBrief when both exist.
        whatIsHappening:
          (scalingSeg && scalingActivityLead
            ? scalingActivityLead
            : null) ||
          integrityApplied.whatIsHappening ||
          (scalingSeg && summaryLooksLikeIndustryLead
            ? null
            : integrityApplied.summaryWhat) ||
          (scalingSeg
            ? integrityApplied.commercialIntelligenceBrief?.companyAndLocalActivity
            : null) ||
          integrityApplied.salesBrief?.companyAndLocalActivity ||
          integrityApplied.commercialIntelligenceBrief?.companyAndLocalActivity ||
          null,
        summaryWhat:
          (scalingSeg && scalingActivityLead
            ? scalingActivityLead
            : null) ||
          (scalingSeg && summaryLooksLikeIndustryLead
            ? integrityApplied.whatIsHappening
            : null) ||
          integrityApplied.summaryWhat ||
          integrityApplied.whatIsHappening ||
          (scalingSeg
            ? integrityApplied.commercialIntelligenceBrief?.companyAndLocalActivity
            : null) ||
          integrityApplied.salesBrief?.companyAndLocalActivity ||
          null,
        whyThisHotel:
          integrityApplied.whyThisHotel ||
          (scalingSeg
            ? integrityApplied.commercialIntelligenceBrief?.whyThisHotel
            : null) ||
          integrityApplied.salesBrief?.whyThisHotel ||
          integrityApplied.summaryWhyHotel ||
          null,
        localConnection: null,
        localPresence: null,
      }
    : applyScalingSalesPresentation(integrityApplied);
  // Universal Who to Contact V2: EVERY demand segment hydrates CIB/saved contacts
  // into the shared Scaling gold-standard two-contact presentation (Surfe click-only).
  // Do not gate on Events-only — Nobu education/government/social/etc. lost contacts
  // when applyScalingTwoContactStandard early-returned without force.
  const withContacts = applyUnifiedWhoToContactStandard(presented, {
    force: true,
  });
  // Universal one-name tile contact preview (all segments). Events path already
  // attaches via unified who-to-contact; Scaling path attaches here.
  const withPreview = withContacts.tileContactPreview
    ? withContacts
    : attachUniversalTileContactPreview(withContacts);
  const base = attachContactResearchCompletionGate(withPreview);

  const summaryWhatRaw = salesFacingField(base.summaryWhat, base.summaryWhyMatters);
  const summaryWhat = scrubCustomerOccasionNarrative(summaryWhatRaw || base.summaryWhat, {
    title: base.title,
    organizationName: base.organizationName || base.companyName,
  });
  // Never fall back hotel-fit thesis fields to the occasion summary — that made
  // "Why this hotel" repeat Account/Occasion and hid missing property rationale.
  const hotelOpportunityThesis = distinctFromOccasion(
    salesFacingField(base.hotelOpportunityThesis, null),
    summaryWhat || base.summaryWhat
  );
  const summaryWhyHotel = distinctFromOccasion(
    salesFacingField(base.summaryWhyHotel, null),
    summaryWhat || base.summaryWhat
  );
  const whyThisHotel = distinctFromOccasion(
    salesFacingField(base.whyThisHotel, hotelOpportunityThesis || summaryWhyHotel),
    summaryWhat || base.summaryWhat
  );

  // Assessment-status prose may live in fitExplanation/fitRationale; keep it only
  // for structured caveats (hotelFitRationale / score note), never as sales thesis.
  // When overall is provisional/numeric, drop stale "withheld" fitExplanation copy.
  const fitExplanation = salesFacingField(base.fitExplanation, null);
  const fitRationale = salesFacingField(base.fitRationale, null);
  const summaryWhyMatters = salesFacingField(base.summaryWhyMatters, null);

  const whyHotelDemandRaw = salesFacingField(base.whyHotelDemand, null);
  const whyHotelDemand = distinctFromOccasion(whyHotelDemandRaw, summaryWhat || base.summaryWhat);

  const garbageContactName =
    /^(Riviera Maya|Meetings Content Managers|Mexico City|Cancun|Cancún|CDMX)$/i;
  let primaryContact = scrubContactNotes(base.primaryContact);
  if (primaryContact?.name && garbageContactName.test(String(primaryContact.name).trim())) {
    primaryContact = { ...primaryContact, name: null };
  }
  let contactName = base.contactName || null;
  if (contactName && garbageContactName.test(String(contactName).trim())) {
    contactName = null;
  }

  const occasionClean =
    summaryWhat ||
    scrubCustomerOccasionNarrative(base.whatIsHappening, {
      title: base.title,
      organizationName: base.organizationName || base.companyName,
    }) ||
    null;

  let out = {
    ...base,
    summaryWhat: occasionClean,
    whatIsHappening: occasionClean,
    salesBriefActivity: occasionClean || base.salesBriefActivity || null,
    eventHistoryAndActivity: occasionClean || base.eventHistoryAndActivity || null,
    summaryWhyMatters,
    hotelOpportunityThesis,
    summaryWhyHotel,
    whyThisHotel,
    whyHotelDemand: whyHotelDemand || null,
    hotelFitScore: reconcileHotelFitScoreForDisplay(base),
    fitExplanation,
    fitRationale,
    hotelFitRationale:
      scrubInternalResearchLanguage(
        scrubProviderBrandNames(base.hotelFitRationale) || base.hotelFitRationale,
      ) || null,
    localPresence: scrubInternalResearchLanguage(base.localPresence) || null,
    localConnection: scrubInternalResearchLanguage(base.localConnection) || null,
    datedGrowthEvidence: scrubInternalResearchLanguage(base.datedGrowthEvidence) || null,
    evidenceConfidence: evidenceRecal.evidenceConfidence,
    evidenceConfidenceExplanation:
      scrubProviderBrandNames(
        evidenceRecal.evidenceConfidenceExplanation ||
          base.evidenceConfidenceExplanation,
      ) ||
      evidenceRecal.evidenceConfidenceExplanation ||
      null,
    recommendedAction:
      scrubInternalResearchLanguage(
        scrubProviderBrandNames(base.recommendedAction) || base.recommendedAction,
      ) || null,
    // run-052: suggestedAction must never diverge to a dirtier legacy Contact-<buyer> string
    suggestedAction:
      scrubInternalResearchLanguage(
        scrubProviderBrandNames(
          base.recommendedAction || base.suggestedAction || base.salesActionRecommended,
        ) ||
          base.recommendedAction ||
          base.suggestedAction ||
          base.salesActionRecommended,
      ) || null,
    whyNow: scrubProviderBrandNames(base.whyNow) || base.whyNow || null,
    whoPrimaryReason: scrubProviderBrandNames(base.whoPrimaryReason) || base.whoPrimaryReason || null,
    contactName,
    primaryContact,
    secondaryContact: scrubContactNotes(base.secondaryContact),
    backupContacts: scrubContactList(base.backupContacts),
    contacts: scrubContactList(base.contacts),
    evidence: scrubEvidenceList(base.evidence),
    // Research taxonomy — never customer-facing card chrome on Scaling.
    // Association drawers still use venueSourcingStatusLabel in Why-this-hotel.
    evidenceStatusLabel: null,
    venueSourcingStatusLabel: isScalingCompaniesSegment(base)
      ? null
      : base.venueSourcingStatusLabel || null,
    prospectCustomerLabel:
      base.prospectCustomerLabel ||
      (String(base.prospectReviewSection || "").toUpperCase() === "EXPLORATORY"
        ? "Potential opportunity"
        : base.prospectCustomerLabel) ||
      null,
    customerPresentationSanitizeVersion: CUSTOMER_PRESENTATION_SANITIZE_VERSION,
  };

  // Drop internal provider flag/ID keys (key names themselves match leak scanners).
  for (const key of CUSTOMER_INTERNAL_PROVIDER_KEYS) {
    delete out[key];
  }
  // Raw scrape excerpts stay in research stores — never customer DTOs.
  if (out.researchProvenance && typeof out.researchProvenance === "object") {
    const { rawWhatIsHappening: _rawDrop, ...restProv } = out.researchProvenance;
    out.researchProvenance = Object.keys(restProv).length ? restProv : undefined;
    if (!out.researchProvenance) delete out.researchProvenance;
  }

  // Channel ownership labels on the opportunity root (legacy phone/email provenance).
  for (const key of [
    "phoneSource",
    "emailSource",
    "channelSource",
    "contactPhoneSource",
    "contactEmailSource",
  ]) {
    if (out[key]) out[key] = scrubChannelSourceLabel(out[key]);
  }

  // Strict Scaling drawer model — UI must render these 10 sections only.
  if (isScalingCompaniesSegment(out)) {
    out.scalingDrawerModel = buildScalingDrawerNormalizedModel(out, {
      hotelDisplayName: out.hotelDisplayName || out.hotelName || null,
      hotelCity: out.hotelCity || out.marketCity || null,
      hotelAddress: out.hotelAddress || null,
    });
    if (out.scalingDrawerModel?.sections?.recommended_next_step?.body) {
      out.recommendedAction =
        out.scalingDrawerModel.sections.recommended_next_step.body;
      out.suggestedAction = out.recommendedAction;
    }
  } else {
    delete out.scalingDrawerModel;
  }

  // Shared twelve-segment drawers (non-Scaling / non-Events): attach Scaling
  // sections 2–10 model + segment-specific Section 1 overviewModel.
  const demandSegmentId = resolveOpportunityDemandSegmentId(out);
  const isSharedNonLegacySegment =
    demandSegmentId &&
    demandSegmentId !== "corporate_accounts_scaling" &&
    demandSegmentId !== "associations_conferences_events" &&
    !isScalingCompaniesSegment(out) &&
    !isAssociationConferenceEventsOpportunity(out);
  if (isSharedNonLegacySegment) {
    out.demandSegmentId = demandSegmentId;
    const shared = buildSharedTenSectionDrawerDto(out, {
      hotelDisplayName: out.hotelDisplayName || out.hotelName || null,
      hotelCity: out.hotelCity || out.marketCity || null,
      hotelAddress: out.hotelAddress || null,
      hotelId: out.hotelId || null,
      force: true,
    });
    out.sharedTenSectionDrawer = shared;
    out.segmentOverviewModel = shared.overviewModel || null;
    out.scalingDrawerModel = shared.drawerModel || null;
    // Preferred Lodging: keep subtype classification on the type label / tile line.
    if (out.preferredLodgingPartnershipV1 === true) {
      out.segmentDisplayLabel =
        out.classificationLine ||
        "Preferred Lodging Partnership · Social Groups & Special Events";
      out.opportunityTypeLabel = "Preferred Lodging Partnership";
    } else {
      out.segmentDisplayLabel =
        shared.segmentLabel || out.segmentDisplayLabel || null;
      out.opportunityTypeLabel =
        shared.segmentLabel || out.opportunityTypeLabel || null;
    }
    if (shared.drawerModel?.sections?.recommended_next_step?.body) {
      out.recommendedAction =
        shared.drawerModel.sections.recommended_next_step.body;
      out.suggestedAction = out.recommendedAction;
    }
  }

  // Association / Conference / Events — structured overview + canonical sections 2–10.
  // Section 1 stays Events grid; never attach Scaling narrative overview model.
  if (isAssociationConferenceEventsOpportunity(out) && !isScalingCompaniesSegment(out)) {
    const withOverview = attachEventsOpportunityOverview(out);
    out.eventsOpportunityOverview = withOverview.eventsOpportunityOverview;
    out.displayTitle =
      withOverview.eventsOpportunityOverview?.drawerHeaderTitle || out.displayTitle;
    out.drawerHeaderTitle =
      withOverview.eventsOpportunityOverview?.drawerHeaderTitle || out.drawerHeaderTitle;
    out.eventTypeLabel =
      withOverview.eventTypeLabel ||
      withOverview.eventsOpportunityOverview?.eventTypeLabel ||
      out.eventTypeLabel;
    out.venueSourcingStatusLabel =
      withOverview.venueSourcingStatusLabel ||
      withOverview.eventsOpportunityOverview?.hotelSourcing ||
      out.venueSourcingStatusLabel;
    out.hotelSourcingStatusLabel =
      withOverview.hotelSourcingStatusLabel ||
      withOverview.eventsOpportunityOverview?.hotelSourcing ||
      out.hotelSourcingStatusLabel;
    out.hotelSourcingStatus =
      withOverview.hotelSourcingStatus ||
      withOverview.eventsOpportunityOverview?.hotelSourcing ||
      out.hotelSourcingStatus;
    if (withOverview.commercialIntelligenceBrief) {
      out.commercialIntelligenceBrief = withOverview.commercialIntelligenceBrief;
    }
    if (withOverview.associationEventsGoldBrief) {
      out.associationEventsGoldBrief = withOverview.associationEventsGoldBrief;
    }
    delete out.scalingDrawerModel;
    out.eventsDrawerModel = buildEventsDrawerNormalizedModel(out, {
      hotelDisplayName: out.hotelDisplayName || out.hotelName || null,
      hotelCity: out.hotelCity || out.marketCity || null,
      hotelId: out.hotelId || null,
      force: true,
    });
    if (out.eventsDrawerModel?.sections?.recommended_next_step?.body) {
      out.recommendedAction =
        out.eventsDrawerModel.sections.recommended_next_step.body;
      out.suggestedAction = out.recommendedAction;
    }
  }

  // Catch-all: every customer-facing opportunity must leave sanitize with a
  // universal ten-section drawer model (Events or Scaling/shared). Never leave
  // rows dependent on the deleted legacy Account/occasion renderer.
  if (
    !out.eventsDrawerModel?.sections &&
    !out.scalingDrawerModel?.sections &&
    !out.sharedTenSectionDrawer?.sections
  ) {
    const segId =
      resolveOpportunityDemandSegmentId(out) ||
      (isAssociationConferenceEventsOpportunity(out)
        ? "associations_conferences_events"
        : null);
    if (
      segId === "associations_conferences_events" ||
      isAssociationConferenceEventsOpportunity(out)
    ) {
      const withOverview = attachEventsOpportunityOverview(out);
      out.eventsOpportunityOverview =
        withOverview.eventsOpportunityOverview || out.eventsOpportunityOverview;
      out.eventsDrawerModel = buildEventsDrawerNormalizedModel(out, {
        hotelDisplayName: out.hotelDisplayName || out.hotelName || null,
        hotelCity: out.hotelCity || out.marketCity || null,
        hotelId: out.hotelId || null,
        force: true,
      });
      delete out.scalingDrawerModel;
    } else {
      const shared = buildSharedTenSectionDrawerDto(out, {
        hotelDisplayName: out.hotelDisplayName || out.hotelName || null,
        hotelCity: out.hotelCity || out.marketCity || null,
        hotelId: out.hotelId || null,
        force: true,
        segmentId: segId || "corporate_accounts_scaling",
      });
      out.sharedTenSectionDrawer = shared;
      out.segmentOverviewModel = shared.overviewModel || null;
      out.scalingDrawerModel = shared.drawerModel || null;
      out.demandSegmentId = shared.segmentId || out.demandSegmentId;
    }
  }

  // Commercial consistency: Fully Placed / closed venue must not recommend
  // open lodging pursuit as if sourcing were contestable.
  const sourcingBlob = `${out.venueSourcingStatusLabel || ""} ${out.hotelSourcingStatusLabel || ""} ${out.hotelSourcingStatus || ""} ${out.opportunityType || ""} ${out.eventsOpportunityOverview?.hotelSourcing || ""}`;
  const sourcingClosed =
    /fully\s*placed|venue\s*confirmed|closed_disqualified|closed\s*\/\s*disqualified/i.test(
      sourcingBlob,
    );
  if (sourcingClosed) {
    const action = String(out.recommendedAction || "");
    const potential = String(
      out.eventsDrawerModel?.sections?.potential_business?.body ||
        out.commercialIntelligenceBrief?.potentialBusinessForThisHotel ||
        "",
    );
    const contradictsOpenPursuit =
      /approach the organizer|hotel sourcing remains open|ask about lodging|credible purs|pursue now|win room block/i.test(
        `${action} ${potential}`,
      );
    const hotelLabel =
      out.hotelDisplayName || out.hotelName || "this hotel";
    const closedNext =
      /no current pursuit|retain as historical|do not pursue as an open/i.test(
        action,
      )
        ? action ||
          `Official hotel sourcing for this edition appears closed / fully placed. Do not pursue as an open room-block RFP for ${hotelLabel}. If a distinct secondary mechanism exists (sponsor dinner, offsite meeting, future edition), qualify that pathway separately with evidence — do not invent overflow demand.`
        : `Official hotel sourcing for this edition appears closed / fully placed. Do not pursue as an open room-block RFP for ${hotelLabel}. If a distinct secondary mechanism exists (sponsor dinner, offsite meeting, future edition), qualify that pathway separately with evidence — do not invent overflow demand.`;
    if (contradictsOpenPursuit || !/do not pursue as an open|no current pursuit|retain as historical/i.test(action)) {
      out.recommendedAction = closedNext;
      out.suggestedAction = closedNext;
    }
    if (out.eventsDrawerModel?.sections?.recommended_next_step) {
      out.eventsDrawerModel = {
        ...out.eventsDrawerModel,
        sections: {
          ...out.eventsDrawerModel.sections,
          recommended_next_step: {
            ...out.eventsDrawerModel.sections.recommended_next_step,
            body: out.recommendedAction || closedNext,
          },
        },
      };
    }
    if (
      out.eventsDrawerModel?.sections?.potential_business?.body &&
      /credible purs|hotel sourcing remains open|approach the organizer/i.test(
        out.eventsDrawerModel.sections.potential_business.body,
      )
    ) {
      out.eventsDrawerModel = {
        ...out.eventsDrawerModel,
        sections: {
          ...out.eventsDrawerModel.sections,
          potential_business: {
            ...out.eventsDrawerModel.sections.potential_business,
            body: `Hotel sourcing for this edition appears closed / fully placed. Do not treat ${hotelLabel} as an open room-block contender for the primary housing programme. Any secondary meeting/catering pathway requires separate evidence.`,
          },
        },
      };
    }
  }

  // Strip internal research engines from customer detail payloads (keep CIB + drawer model).
  for (const key of [
    "exaAgentEvidencePack",
    "exaAgentClaimToSourceMap",
    "exaAgentDeepResearchRaw",
    "unresolvedResearchTasks",
    "orsTrace",
    "orsInvestigationLog",
    "providerDebug",
    "adaptiveResearchLedger",
  ]) {
    if (key in out) delete out[key];
  }

  // Conditional Hotel Need-Date Match block (does not replace ten sections).
  if (
    Array.isArray(out.needDateAssociations) &&
    out.needDateAssociations.length > 0
  ) {
    out = attachNeedDateMatchToOpportunity(out);
  }

  // Canonical revenue-stream class + tile icon states (presentation only).
  // Derives from persisted class or thesis fields — never from demand segment alone.
  out = attachOpportunityRevenueStreamIcons(out);

  // Final hotel-thesis honesty: never ship quarantine boilerplate as Why this hotel.
  const hotelLabel =
    out.hotelDisplayName ||
    out.hotelName ||
    (out.hotelId === "recgMYovrrZDJMqzX"
      ? "Waterstone Resort & Marina, Curio Collection by Hilton"
      : "This hotel");
  const scrubThesis = (v) => {
    const t = text(v);
    if (!t) return null;
    if (/prior (?:CIB|brief) referenced another hotel/i.test(t)) {
      return `${hotelLabel} sits in its verified market with guestroom and meeting inventory relevant to qualified demand — confirm geography, product fit, and sourcing accessibility before pitching.`;
    }
    return t;
  };
  out.whyThisHotel = scrubThesis(out.whyThisHotel) || out.whyThisHotel;
  out.summaryWhyHotel = scrubThesis(out.summaryWhyHotel) || out.summaryWhyHotel;
  if (out.commercialIntelligenceBrief?.whyThisHotel) {
    out.commercialIntelligenceBrief = {
      ...out.commercialIntelligenceBrief,
      whyThisHotel:
        scrubThesis(out.commercialIntelligenceBrief.whyThisHotel) ||
        out.commercialIntelligenceBrief.whyThisHotel,
    };
  }
  const whySec = out.eventsDrawerModel?.sections?.why_this_hotel;
  if (whySec) {
    const body = scrubThesis(whySec.body) || whySec.body || "";
    const advantages = Array.isArray(whySec.advantages) ? whySec.advantages : [];
    out.eventsDrawerModel = {
      ...out.eventsDrawerModel,
      sections: {
        ...out.eventsDrawerModel.sections,
        why_this_hotel: {
          ...whySec,
          body:
            body ||
            advantages.filter(Boolean).slice(0, 2).join(" ") ||
            `${hotelLabel} fit depends on verified event geography and sourcing accessibility — confirm before pitching contracted space.`,
        },
      },
    };
  }

  // Final title gate, then remove provider-only keys at every nesting level.
  return stripInternalProviderKeys(applyCanonicalCustomerFacingTitle(out));
}

function stripInternalProviderKeys(value, seen = new Map()) {
  if (typeof value === "string") return scrubProviderBrandNames(value);
  if (!value || typeof value !== "object") return value;
  if (seen.has(value)) return seen.get(value);
  const denied = new Set([
    ...CUSTOMER_INTERNAL_PROVIDER_KEYS,
    ...CUSTOMER_EVIDENCE_INTERNAL_KEYS,
  ]);
  if (Array.isArray(value)) {
    const next = [];
    seen.set(value, next);
    for (const item of value) {
      if (typeof item === "string" && /^(webhound|surfe|parallel|ampfy)$/i.test(item.trim())) continue;
      next.push(stripInternalProviderKeys(item, seen));
    }
    return next;
  }
  const next = {};
  seen.set(value, next);
  for (const key of Object.keys(value)) {
    if (
      denied.has(key) ||
      /webhound|surfe[_-]?enrich|providerRunId|enrichmentId|creditsConsumed|parallel\.ai/i.test(key)
    ) {
      continue;
    }
    next[key] = stripInternalProviderKeys(value[key], seen);
  }
  return next;
}
