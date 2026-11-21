/**
 * Commercial target discovery — conversion & management-contract opportunities.
 *
 * Optimizes for verified hotel targets + reliable contractual timing (not legal-term depth).
 * Two projections over shared canonical research identities:
 *   1) Brand Conversion Targets
 *   2) Hotel Management Contract Targets
 *
 * LEASE is a supported agreement classification retained when encountered incidentally
 * (ground / operating / other hotel leases), but Lease Intelligence is deferred:
 * no third prospect list, no dedicated lease discovery spend, no dual-list inflation.
 *
 * Never inflates counts with opening years, loan maturities, or unsupported dates.
 */

import crypto from "node:crypto";
import { auditContractDateObservation, AUDITED_DATE_MEANING } from "./contract-date-meaning-audit-v1.js";
import { assertDateEligibleForDecisionWindow } from "./contract-date-semantic-gate-v1.js";
import { calculateDecisionWindows } from "./contract-terms-decision-windows-v1.js";
import { classifyRenewalOutcome } from "./contract-terms-renewal-classifier-v1.js";
import { IDENTITY_LAYER } from "./contract-intelligence-identity-layers-v1.js";

export const COMMERCIAL_TARGET_DISCOVERY_V1 = "commercial-target-discovery-v1";

export const OPPORTUNITY_TIER = Object.freeze({
  TIER_A_VERIFIED_DECISION_WINDOW: "TIER_A_VERIFIED_DECISION_WINDOW",
  TIER_B_VERIFIED_CONTRACT_EXPIRATION: "TIER_B_VERIFIED_CONTRACT_EXPIRATION",
  TIER_C_CONVERSION_RESEARCH_CANDIDATE: "TIER_C_CONVERSION_RESEARCH_CANDIDATE",
  TIER_D_FURTHER_RESEARCH_REQUIRED: "TIER_D_FURTHER_RESEARCH_REQUIRED",
  CLOSED_SUPERSEDED: "CLOSED_SUPERSEDED",
  HOLD_QUARANTINE: "HOLD_QUARANTINE",
});

/**
 * Research-facing contractual date classes (no arbitrary horizon).
 * Application filters may slice these; research eligibility must not.
 */
export const CONTRACT_DATE_CLASS = Object.freeze({
  EXPLICIT_CONTRACT_EXPIRATION: "EXPLICIT_CONTRACT_EXPIRATION",
  DERIVED_FROM_VERIFIED_CONTRACT_TERMS: "DERIVED_FROM_VERIFIED_CONTRACT_TERMS",
  EXECUTED_TERMINATION_DATE: "EXECUTED_TERMINATION_DATE",
  EXECUTED_EXTENSION_DATE: "EXECUTED_EXTENSION_DATE",
  HISTORICAL_EXPIRATION_STATUS_UNKNOWN: "HISTORICAL_EXPIRATION_STATUS_UNKNOWN",
  CURRENT_CONTRACT_EXPIRATION_VERIFIED: "CURRENT_CONTRACT_EXPIRATION_VERIFIED",
  NON_CONTRACTUAL_PROPERTY_DATE: "NON_CONTRACTUAL_PROPERTY_DATE",
  UNRESOLVED_DATE_MEANING: "UNRESOLVED_DATE_MEANING",
});

/** Optional app-filter buckets — never used to exclude research eligibility. */
export const APP_DATE_FILTER = Object.freeze({
  ALL: "ALL",
  HISTORICAL_EXPIRATIONS: "HISTORICAL_EXPIRATIONS",
  FUTURE_EXPIRATIONS: "FUTURE_EXPIRATIONS",
  NEXT_12_MONTHS: "NEXT_12_MONTHS",
  NEXT_24_MONTHS: "NEXT_24_MONTHS",
  NEXT_36_MONTHS: "NEXT_36_MONTHS",
  NEXT_60_MONTHS: "NEXT_60_MONTHS",
  UNKNOWN_EXPIRATION: "UNKNOWN_EXPIRATION",
});

export const PROSPECT_LIST = Object.freeze({
  BRAND_CONVERSION: "BRAND_CONVERSION",
  MANAGEMENT_CONTRACT: "MANAGEMENT_CONTRACT",
  // No LEASE prospect list in V5 — deferred capability only.
});

/** Supported agreement classifications (research inventory). Dual lists use franchise/management only. */
export const AGREEMENT_TYPE = Object.freeze({
  FRANCHISE: "FRANCHISE",
  MANAGEMENT: "MANAGEMENT",
  LICENSE: "LICENSE",
  LEASE: "LEASE",
  OTHER: "OTHER",
  UNKNOWN: "UNKNOWN",
});

/**
 * Deferred commercial capability. Classify + retain incidental lease evidence;
 * do not project to Brand/Management lists or create a third list until reassessed.
 */
export const DEFERRED_LEASE_INTELLIGENCE = Object.freeze({
  status: "DEFERRED",
  supportedClassification: AGREEMENT_TYPE.LEASE,
  thirdProspectList: false,
  dedicatedDiscoveryAuthorized: false,
  paidResearchAuthorized: false,
  retainIncidentalEvidence: true,
  extensibleTo: ["GROUND_LEASE", "OPERATING_LEASE", "OTHER_HOTEL_LEASE"],
  reassessmentTrigger: "AFTER_BRAND_AND_MANAGEMENT_COMMERCIAL_VALUE_ESTABLISHED",
});

export const NOTICE_STATUS = Object.freeze({
  VERIFIED: "VERIFIED",
  CALCULATED: "CALCULATED",
  NOTICE_NOT_DISCLOSED: "NOTICE_NOT_DISCLOSED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

export const OPPORTUNITY_CHANGE = Object.freeze({
  NEW: "NEW",
  UPDATED: "UPDATED",
  UNCHANGED: "UNCHANGED",
  SUPERSEDED: "SUPERSEDED",
  CONFLICTED: "CONFLICTED",
  CLOSED: "CLOSED",
});

export const HISTORICAL_APPLICABILITY = Object.freeze({
  CURRENTLY_CORROBORATED: "CURRENTLY_CORROBORATED",
  HISTORICALLY_DISCLOSED_ONLY: "HISTORICALLY_DISCLOSED_ONLY",
  SUPERSEDED_OR_RENEWED: "SUPERSEDED_OR_RENEWED",
  TERMINATED: "TERMINATED",
  STATUS_UNCONFIRMED: "STATUS_UNCONFIRMED",
  REQUIRES_FOLLOW_UP: "REQUIRES_FOLLOW_UP",
});

const FRANCHISE_LIKE = new Set([AGREEMENT_TYPE.FRANCHISE, AGREEMENT_TYPE.LICENSE]);
const MANAGEMENT_LIKE = new Set([AGREEMENT_TYPE.MANAGEMENT]);
const DEFERRED_FROM_PROSPECT_LISTS = new Set([AGREEMENT_TYPE.LEASE]);

function stableId(parts) {
  return crypto.createHash("sha1").update(parts.filter(Boolean).join("|")).digest("hex").slice(0, 16);
}

function monthsBetween(fromIso, toIso) {
  const a = new Date(fromIso);
  const b = new Date(toIso);
  if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return null;
  return (b.getUTCFullYear() - a.getUTCFullYear()) * 12 + (b.getUTCMonth() - a.getUTCMonth());
}

function ymd(d = new Date()) {
  return new Date(d).toISOString().slice(0, 10);
}

/**
 * Classify a research observation into commercial opportunity tiers.
 * Fail-closed on date meaning / hotel identity.
 */
function mapContractDateClass({ audit, monthsToExpiration, historicalApplicability, gateOk }) {
  if (!gateOk || !audit) {
    if (audit?.auditedMeaning === AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT) {
      return CONTRACT_DATE_CLASS.NON_CONTRACTUAL_PROPERTY_DATE;
    }
    if (audit?.auditedMeaning === AUDITED_DATE_MEANING.LOAN_MATURITY) {
      return CONTRACT_DATE_CLASS.NON_CONTRACTUAL_PROPERTY_DATE;
    }
    return CONTRACT_DATE_CLASS.UNRESOLVED_DATE_MEANING;
  }
  if (historicalApplicability === HISTORICAL_APPLICABILITY.TERMINATED) {
    return CONTRACT_DATE_CLASS.EXECUTED_TERMINATION_DATE;
  }
  if (historicalApplicability === HISTORICAL_APPLICABILITY.SUPERSEDED_OR_RENEWED) {
    return CONTRACT_DATE_CLASS.EXECUTED_EXTENSION_DATE;
  }
  if (historicalApplicability === HISTORICAL_APPLICABILITY.CURRENTLY_CORROBORATED) {
    return CONTRACT_DATE_CLASS.CURRENT_CONTRACT_EXPIRATION_VERIFIED;
  }
  if (monthsToExpiration != null && monthsToExpiration < 0) {
    return CONTRACT_DATE_CLASS.HISTORICAL_EXPIRATION_STATUS_UNKNOWN;
  }
  return CONTRACT_DATE_CLASS.EXPLICIT_CONTRACT_EXPIRATION;
}

/**
 * Classify a research observation into commercial opportunity tiers.
 * Fail-closed on date meaning / hotel identity.
 *
 * IMPORTANT: horizonMonths is metadata for optional *application* filters only.
 * It must NEVER exclude historical or far-future dates from research eligibility
 * or dual prospect list membership.
 */
export function classifyCommercialOpportunity(obs = {}, opts = {}) {
  const asOf = opts.asOf || ymd();
  /** @deprecated for eligibility — retained only as optional near-term outreach hint */
  const horizonMonths = opts.horizonMonths != null ? Number(opts.horizonMonths) : 36;

  const hotelVerified = Boolean(obs.hotelVerified ?? obs.hotelName);
  const hotelName = obs.hotelName || null;
  const agreementType = String(obs.agreementType || "UNKNOWN").toUpperCase();
  const evidenceSnippet = obs.evidenceSnippet || obs.snippet || "";
  const primaryDate = obs.primaryDate || obs.expirationDate || null;
  const noticeDays = Number.isFinite(obs.renewalNoticeDays) ? Number(obs.renewalNoticeDays) : null;
  const explicitNoticeDeadline = obs.noticeDeadline || null;
  const sourceUrl = obs.sourceUrl || (obs.sourceUrls || [])[0] || null;

  const audit =
    obs.dateAudit ||
    (primaryDate
      ? auditContractDateObservation({
          primaryDate,
          dateMeaning: obs.dateMeaning || null,
          evidenceSnippet,
          hotelName,
          sourceUrls: sourceUrl ? [sourceUrl] : obs.sourceUrls || [],
        })
      : null);

  // Quarantine non-contract dates / uncertain hotels first
  if (!hotelVerified || !hotelName) {
    return holdResult("hotel_identity_uncertain", { audit, hotelName, agreementType, sourceUrl });
  }
  if (obs.portfolioLevelOnly && !obs.hotelAttributed) {
    return holdResult("portfolio_not_hotel_attributed", { audit, hotelName, agreementType, sourceUrl });
  }
  if (
    audit &&
    (audit.auditedMeaning === AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT ||
      audit.auditedMeaning === AUDITED_DATE_MEANING.LOAN_MATURITY)
  ) {
    return holdResult(`non_contract_date:${audit.auditedMeaning}`, {
      audit,
      hotelName,
      agreementType,
      sourceUrl,
      layer: IDENTITY_LAYER.HOTEL_PROPERTY_FACT,
      preserveAsHotelFact: audit.auditedMeaning === AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT,
    });
  }
  if (obs.conflictingContractEvidence) {
    return holdResult("conflicting_contractual_information", { audit, hotelName, agreementType, sourceUrl });
  }

  const renewal = classifyRenewalOutcome({
    originalText: evidenceSnippet,
    laterText: obs.laterEvidenceText || "",
    scheduledExpiration: primaryDate,
    asOf,
    sameBrandObserved: Boolean(obs.sameBrandObserved),
    expirationStillCited: Boolean(obs.expirationStillCited),
  });

  let historicalApplicability = HISTORICAL_APPLICABILITY.STATUS_UNCONFIRMED;
  if (
    renewal.renewalStatusClass === "RENEWAL_EXECUTED_VERIFIED" ||
    renewal.renewalStatusClass === "EXTENSION_EXECUTED_VERIFIED" ||
    renewal.renewalStatusClass === "REPLACED_OR_REFLAGGED_VERIFIED"
  ) {
    historicalApplicability = HISTORICAL_APPLICABILITY.SUPERSEDED_OR_RENEWED;
  } else if (renewal.renewalStatusClass === "TERMINATED_VERIFIED") {
    historicalApplicability = HISTORICAL_APPLICABILITY.TERMINATED;
  } else if (renewal.renewalStatusClass === "CURRENT_TERMS_CORROBORATED") {
    historicalApplicability = HISTORICAL_APPLICABILITY.CURRENTLY_CORROBORATED;
  } else if (primaryDate) {
    historicalApplicability = HISTORICAL_APPLICABILITY.HISTORICALLY_DISCLOSED_ONLY;
  }

  // Closed / superseded — preserve hotel + historical contract; not a HOLD quarantine
  if (
    historicalApplicability === HISTORICAL_APPLICABILITY.SUPERSEDED_OR_RENEWED ||
    historicalApplicability === HISTORICAL_APPLICABILITY.TERMINATED
  ) {
    return {
      version: COMMERCIAL_TARGET_DISCOVERY_V1,
      tier: OPPORTUNITY_TIER.CLOSED_SUPERSEDED,
      holdReason: null,
      researchEligible: true,
      nearTermOutreachPriority: false,
      activeTargeting: false,
      changeClass: OPPORTUNITY_CHANGE.CLOSED,
      hotelName,
      agreementType,
      primaryDate,
      verifiedEndDate: primaryDate,
      renewalStatusClass: renewal.renewalStatusClass,
      historicalApplicability,
      contractDateClass:
        historicalApplicability === HISTORICAL_APPLICABILITY.TERMINATED
          ? CONTRACT_DATE_CLASS.EXECUTED_TERMINATION_DATE
          : CONTRACT_DATE_CLASS.EXECUTED_EXTENSION_DATE,
      dateAudit: audit,
      country: obs.country || null,
      region: obs.region || null,
      publicationState: "CUSTOMER_BLOCKED",
      hpcCopyAuthorized: false,
    };
  }

  const gate = primaryDate
    ? assertDateEligibleForDecisionWindow({
        audit,
        hotelAttributed: obs.hotelAttributed !== false,
        portfolioLevelOnly: Boolean(obs.portfolioLevelOnly),
      })
    : { ok: false, code: "NO_EXPIRATION_DATE", reason: "No hotel-specific expiration disclosed" };

  const windows = gate.ok
    ? calculateDecisionWindows({
        scheduledExpiration: primaryDate,
        renewalNoticeDays: noticeDays,
        explicitNoticeDeadline,
        renewalOptionsCount: obs.renewalOptionsCount || 0,
        terminationWithoutCause: Boolean(obs.terminationWithoutCause),
        performanceTest: Boolean(obs.performanceTest),
        changeOfControl: Boolean(obs.changeOfControl),
        dateAudit: audit,
        hotelAttributed: true,
        hotelName,
        evidenceSnippet,
      })
    : null;

  const noticeWindow = windows?.windows?.find((w) => w.kind === "RENEWAL_NOTICE_DEADLINE") || null;
  let noticeStatus = NOTICE_STATUS.NOTICE_NOT_DISCLOSED;
  if (explicitNoticeDeadline) noticeStatus = NOTICE_STATUS.VERIFIED;
  else if (noticeWindow?.date && noticeDays != null) noticeStatus = NOTICE_STATUS.CALCULATED;
  else if (!gate.ok) noticeStatus = NOTICE_STATUS.NOT_APPLICABLE;

  const monthsToExpiration = primaryDate && gate.ok ? monthsBetween(asOf, primaryDate) : null;
  const monthsToNotice =
    noticeWindow?.date && gate.ok ? monthsBetween(asOf, noticeWindow.date) : null;
  const noticeDeadlinePassed =
    monthsToNotice != null ? monthsToNotice < 0 : null;
  const withinHorizon =
    (monthsToExpiration != null && monthsToExpiration >= 0 && monthsToExpiration <= horizonMonths) ||
    (monthsToNotice != null && monthsToNotice >= 0 && monthsToNotice <= horizonMonths);

  let tier = OPPORTUNITY_TIER.TIER_C_CONVERSION_RESEARCH_CANDIDATE;
  if (gate.ok && primaryDate && noticeWindow?.date && !windows?.blocked) {
    tier = OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW;
  } else if (gate.ok && primaryDate && !windows?.blocked) {
    tier = OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION;
  } else if (
    audit &&
    (audit.auditedMeaning === AUDITED_DATE_MEANING.UNRESOLVED_DATE_MEANING ||
      gate.code === "UNRESOLVED_DATE_MEANING" ||
      gate.code === "NON_CONTRACT_DATE_MEANING")
  ) {
    return holdResult(`date_gate:${gate.code || audit.auditedMeaning}`, {
      audit,
      hotelName,
      agreementType,
      sourceUrl,
    });
  }

  // Past expiration without renewal evidence → still research-eligible Tier B
  const expiredUnknownRenewal =
    monthsToExpiration != null &&
    monthsToExpiration < 0 &&
    historicalApplicability !== HISTORICAL_APPLICABILITY.CURRENTLY_CORROBORATED;

  const researchId = `ct_${stableId([
    hotelName,
    agreementType,
    primaryDate || "nodate",
    sourceUrl || obs.accession || "",
  ])}`;

  const histApp = expiredUnknownRenewal
    ? HISTORICAL_APPLICABILITY.REQUIRES_FOLLOW_UP
    : historicalApplicability;

  const contractDateClass = mapContractDateClass({
    audit,
    monthsToExpiration,
    historicalApplicability: histApp,
    gateOk: gate.ok,
  });

  // Research eligibility is NEVER gated by 36-month (or any) horizon.
  const researchEligible =
    tier === OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW ||
    tier === OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION ||
    tier === OPPORTUNITY_TIER.TIER_C_CONVERSION_RESEARCH_CANDIDATE ||
    tier === OPPORTUNITY_TIER.TIER_D_FURTHER_RESEARCH_REQUIRED;

  // Optional BD hint only — application filter metadata
  const nearTermOutreachPriority =
    researchEligible &&
    Boolean(withinHorizon) &&
    (tier === OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW ||
      (tier === OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION && !expiredUnknownRenewal));

  return {
    version: COMMERCIAL_TARGET_DISCOVERY_V1,
    researchId,
    tier,
    researchEligible,
    nearTermOutreachPriority,
    /** @deprecated prefer researchEligible + nearTermOutreachPriority; kept for compat */
    activeTargeting: nearTermOutreachPriority,
    changeClass: OPPORTUNITY_CHANGE.NEW,
    hotelName,
    location: obs.location || null,
    country: obs.country || null,
    region: obs.region || null,
    brand: obs.brand || null,
    owner: obs.owner || null,
    managementCompany: obs.managementCompany || null,
    agreementType,
    verifiedEndDate: gate.ok ? primaryDate : null,
    historicalExpirationDate:
      gate.ok && monthsToExpiration != null && monthsToExpiration < 0 ? primaryDate : null,
    futureExpirationDate:
      gate.ok && monthsToExpiration != null && monthsToExpiration >= 0 ? primaryDate : null,
    latestCorroboratedExpirationDate: gate.ok ? primaryDate : null,
    noticePeriodDays: noticeDays,
    noticeStatus,
    noticeDeadline: noticeWindow?.date || explicitNoticeDeadline || null,
    noticeDeadlinePassed,
    monthsToExpiration,
    monthsToNoticeDeadline: monthsToNotice,
    /** App-filter metadata only — not a research eligibility gate */
    within36Months: Boolean(withinHorizon),
    appDateFilterHints: buildAppDateFilterHints(monthsToExpiration, gate.ok ? primaryDate : null),
    renewalStatusClass: renewal.renewalStatusClass,
    historicalApplicability: histApp,
    contractDateClass,
    decisionWindows: windows,
    dateSemanticGate: gate,
    dateAudit: audit,
    confidence: audit?.confidence || obs.confidence || "MEDIUM",
    evidenceSnippet: evidenceSnippet ? String(evidenceSnippet).slice(0, 400) : null,
    sourceUrl,
    accession: obs.accession || null,
    cik: obs.cik || null,
    issuerName: obs.issuerName || null,
    recommendedNextAction: recommendNextAction({
      tier,
      expiredUnknownRenewal,
      noticeStatus,
      owner: obs.owner,
      managementCompany: obs.managementCompany,
      brand: obs.brand,
    }),
    publicationState: "CUSTOMER_BLOCKED",
    hpcCopyAuthorized: false,
  };
}

function buildAppDateFilterHints(monthsToExpiration, primaryDate) {
  const hints = [APP_DATE_FILTER.ALL];
  if (!primaryDate) {
    hints.push(APP_DATE_FILTER.UNKNOWN_EXPIRATION);
    return hints;
  }
  if (monthsToExpiration == null) {
    hints.push(APP_DATE_FILTER.UNKNOWN_EXPIRATION);
    return hints;
  }
  if (monthsToExpiration < 0) hints.push(APP_DATE_FILTER.HISTORICAL_EXPIRATIONS);
  if (monthsToExpiration >= 0) hints.push(APP_DATE_FILTER.FUTURE_EXPIRATIONS);
  if (monthsToExpiration >= 0 && monthsToExpiration <= 12) hints.push(APP_DATE_FILTER.NEXT_12_MONTHS);
  if (monthsToExpiration >= 0 && monthsToExpiration <= 24) hints.push(APP_DATE_FILTER.NEXT_24_MONTHS);
  if (monthsToExpiration >= 0 && monthsToExpiration <= 36) hints.push(APP_DATE_FILTER.NEXT_36_MONTHS);
  if (monthsToExpiration >= 0 && monthsToExpiration <= 60) hints.push(APP_DATE_FILTER.NEXT_60_MONTHS);
  return hints;
}

function holdResult(reason, extra = {}) {
  return {
    version: COMMERCIAL_TARGET_DISCOVERY_V1,
    tier: OPPORTUNITY_TIER.HOLD_QUARANTINE,
    holdReason: reason,
    researchEligible: false,
    nearTermOutreachPriority: false,
    activeTargeting: false,
    changeClass: OPPORTUNITY_CHANGE.CONFLICTED,
    publicationState: "CUSTOMER_BLOCKED",
    hpcCopyAuthorized: false,
    ...extra,
  };
}

function recommendNextAction({ tier, expiredUnknownRenewal, noticeStatus, owner, managementCompany, brand }) {
  if (expiredUnknownRenewal) return "VERIFY_RENEWAL_OR_SUCCESSOR_STATUS";
  if (tier === OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW) {
    return owner || managementCompany
      ? "PRIORITIZE_OWNER_ASSET_MANAGER_OUTREACH_RESEARCH"
      : "ENRICH_OWNER_OPERATOR_THEN_TARGET";
  }
  if (tier === OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION) {
    return noticeStatus === NOTICE_STATUS.NOTICE_NOT_DISCLOSED
      ? "SEARCH_NOTICE_PROVISIONS_THEN_OWNER_ENRICHMENT"
      : "ENRICH_OWNER_OPERATOR_CONTACTS";
  }
  if (!brand && !owner) return "CONFIRM_HOTEL_IDENTITY_AND_COMMERCIAL_COUNTERPARTIES";
  return "RESEARCH_CONTRACT_END_DATE_OR_CHANGE_EVIDENCE";
}

/**
 * Project shared opportunity records onto the two commercial prospect lists.
 * Same hotel may appear on both when agreement types warrant — no duplicated research identity.
 * LEASE rows are retained as incidental research evidence only (no third list; no dual-list inflation).
 */
export function projectDualProspectLists(opportunities = [], opts = {}) {
  const asOf = opts.asOf || ymd();
  const includeClosed = opts.includeClosed === true;
  const brandConversion = [];
  const managementContract = [];
  const closedArchive = [];
  const incidentalLeaseRetained = [];

  for (const opp of opportunities) {
    if (!opp || opp.tier === OPPORTUNITY_TIER.HOLD_QUARANTINE) continue;

    const base = {
      researchId: opp.researchId,
      hotel: opp.hotelName,
      location: opp.location,
      country: opp.country || null,
      region: opp.region || null,
      brand: opp.brand,
      owner: opp.owner,
      managementCompany: opp.managementCompany,
      agreementType: opp.agreementType,
      verifiedEndDate: opp.verifiedEndDate,
      historicalExpirationDate: opp.historicalExpirationDate || null,
      futureExpirationDate: opp.futureExpirationDate || null,
      latestCorroboratedExpirationDate: opp.latestCorroboratedExpirationDate || opp.verifiedEndDate || null,
      noticePeriodDays: opp.noticePeriodDays,
      noticeStatus: opp.noticeStatus,
      noticeDeadline: opp.noticeDeadline,
      renewalStatus: opp.renewalStatusClass,
      historicalApplicability: opp.historicalApplicability,
      contractDateClass: opp.contractDateClass || null,
      monthsToDecisionWindow:
        opp.monthsToNoticeDeadline != null && opp.monthsToNoticeDeadline >= 0
          ? opp.monthsToNoticeDeadline
          : opp.monthsToExpiration,
      opportunityTier: opp.tier,
      confidence: opp.confidence,
      evidenceUrl: opp.sourceUrl,
      verificationDate: asOf,
      changeClass: opp.changeClass,
      recommendedNextAction: opp.recommendedNextAction,
      publicationState: "CUSTOMER_BLOCKED",
      researchEligible: opp.researchEligible !== false,
      nearTermOutreachPriority: Boolean(opp.nearTermOutreachPriority),
      activeTargeting: Boolean(opp.activeTargeting),
      within36Months: Boolean(opp.within36Months),
      appDateFilterHints: opp.appDateFilterHints || [APP_DATE_FILTER.ALL],
    };

    // Deferred Lease Intelligence: classify/retain only — never a third commercial list.
    if (DEFERRED_FROM_PROSPECT_LISTS.has(String(opp.agreementType || "").toUpperCase())) {
      incidentalLeaseRetained.push({
        ...base,
        prospectList: null,
        leaseIntelligenceStatus: DEFERRED_LEASE_INTELLIGENCE.status,
        retentionMode: "INCIDENTAL_EVIDENCE_ONLY",
        decisionMakerFocus: "deferred_until_lease_capability",
      });
      continue;
    }

    if (
      opp.tier === OPPORTUNITY_TIER.CLOSED_SUPERSEDED ||
      opp.changeClass === OPPORTUNITY_CHANGE.CLOSED
    ) {
      closedArchive.push({ ...base, prospectList: "CLOSED_SUPERSEDED_ARCHIVE" });
      if (!includeClosed) continue;
    }

    if (FRANCHISE_LIKE.has(opp.agreementType) || opp.listHints?.includes(PROSPECT_LIST.BRAND_CONVERSION)) {
      brandConversion.push({
        ...base,
        prospectList: PROSPECT_LIST.BRAND_CONVERSION,
        decisionMakerFocus: "hotel_owner_and_brand_counterparties",
      });
    }
    if (
      MANAGEMENT_LIKE.has(opp.agreementType) ||
      opp.listHints?.includes(PROSPECT_LIST.MANAGEMENT_CONTRACT)
    ) {
      managementContract.push({
        ...base,
        prospectList: PROSPECT_LIST.MANAGEMENT_CONTRACT,
        decisionMakerFocus: "owner_and_asset_management_counterparties",
      });
    }
    // Tier C/D without clear type → both potential-target categories when signals present
    // (LEASE already returned above — never inflated here.)
    if (
      (opp.tier === OPPORTUNITY_TIER.TIER_C_CONVERSION_RESEARCH_CANDIDATE ||
        opp.tier === OPPORTUNITY_TIER.TIER_D_FURTHER_RESEARCH_REQUIRED) &&
      !FRANCHISE_LIKE.has(opp.agreementType) &&
      !MANAGEMENT_LIKE.has(opp.agreementType) &&
      !DEFERRED_FROM_PROSPECT_LISTS.has(String(opp.agreementType || "").toUpperCase())
    ) {
      if (opp.conversionSignals?.brand || opp.brand || opp.listHints?.includes(PROSPECT_LIST.BRAND_CONVERSION)) {
        brandConversion.push({
          ...base,
          prospectList: PROSPECT_LIST.BRAND_CONVERSION,
          potentialTargetWithoutVerifiedDate: true,
          decisionMakerFocus: "hotel_owner_and_brand_counterparties",
        });
      }
      if (
        opp.conversionSignals?.management ||
        opp.managementCompany ||
        opp.listHints?.includes(PROSPECT_LIST.MANAGEMENT_CONTRACT)
      ) {
        managementContract.push({
          ...base,
          prospectList: PROSPECT_LIST.MANAGEMENT_CONTRACT,
          potentialTargetWithoutVerifiedDate: true,
          decisionMakerFocus: "owner_and_asset_management_counterparties",
        });
      }
    }
  }

  return {
    version: COMMERCIAL_TARGET_DISCOVERY_V1,
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    brandConversionTargets: brandConversion,
    managementContractTargets: managementContract,
    closedSupersededArchive: closedArchive,
    incidentalLeaseRetained,
    deferredLeaseIntelligence: DEFERRED_LEASE_INTELLIGENCE,
    sharedResearchIdentities: [...new Set(opportunities.map((o) => o.researchId).filter(Boolean))],
    note:
      "Two projections over shared research identities. Historical and future dates retained; 36-month horizon is app-filter metadata only. LEASE retained incidentally only — no third prospect list.",
    globalCoverage: true,
    researchHorizonFilter: "NONE",
  };
}

/**
 * Filter helpers for BD workflows (research-only).
 */
/**
 * Application-layer filters only. Never deletes research inventory.
 */
export function filterProspectList(list = [], filters = {}) {
  return list.filter((row) => {
    if (filters.brand && !new RegExp(filters.brand, "i").test(row.brand || "")) return false;
    if (filters.owner && !new RegExp(filters.owner, "i").test(row.owner || "")) return false;
    if (filters.managementCompany && !new RegExp(filters.managementCompany, "i").test(row.managementCompany || ""))
      return false;
    if (filters.geography && !new RegExp(filters.geography, "i").test(row.location || row.country || row.region || ""))
      return false;
    if (filters.country && !new RegExp(filters.country, "i").test(row.country || row.location || "")) return false;
    if (filters.region && !new RegExp(filters.region, "i").test(row.region || row.location || "")) return false;
    if (filters.agreementType && String(row.agreementType).toUpperCase() !== String(filters.agreementType).toUpperCase())
      return false;
    if (filters.expirationYear && String(row.verifiedEndDate || "").slice(0, 4) !== String(filters.expirationYear))
      return false;
    if (filters.tier && row.opportunityTier !== filters.tier) return false;
    if (filters.activeOnly && !(row.activeTargeting || row.nearTermOutreachPriority)) return false;
    if (filters.nearTermOnly && !row.nearTermOutreachPriority) return false;
    if (filters.requireVerifiedDate && !row.verifiedEndDate) return false;
    if (filters.researchEligibleOnly && row.researchEligible === false) return false;

    const dateFilter = filters.dateFilter || filters.appDateFilter || null;
    if (dateFilter && dateFilter !== APP_DATE_FILTER.ALL) {
      const hints = row.appDateFilterHints || [];
      const months = row.monthsToDecisionWindow;
      if (dateFilter === APP_DATE_FILTER.UNKNOWN_EXPIRATION && row.verifiedEndDate) return false;
      if (dateFilter === APP_DATE_FILTER.HISTORICAL_EXPIRATIONS) {
        if (!(months != null && months < 0) && !hints.includes(APP_DATE_FILTER.HISTORICAL_EXPIRATIONS)) return false;
      }
      if (dateFilter === APP_DATE_FILTER.FUTURE_EXPIRATIONS) {
        if (!(months != null && months >= 0) && !hints.includes(APP_DATE_FILTER.FUTURE_EXPIRATIONS)) return false;
      }
      if (dateFilter === APP_DATE_FILTER.NEXT_12_MONTHS) {
        if (!(months != null && months >= 0 && months <= 12)) return false;
      }
      if (dateFilter === APP_DATE_FILTER.NEXT_24_MONTHS) {
        if (!(months != null && months >= 0 && months <= 24)) return false;
      }
      if (dateFilter === APP_DATE_FILTER.NEXT_36_MONTHS) {
        if (!(months != null && months >= 0 && months <= 36)) return false;
      }
      if (dateFilter === APP_DATE_FILTER.NEXT_60_MONTHS) {
        if (!(months != null && months >= 0 && months <= 60)) return false;
      }
      if (filters.customDateFrom || filters.customDateTo) {
        const d = row.verifiedEndDate;
        if (!d) return false;
        if (filters.customDateFrom && d < filters.customDateFrom) return false;
        if (filters.customDateTo && d > filters.customDateTo) return false;
      }
    }
    return true;
  });
}

export function summarizeCommercialInventory(opportunities = [], projections = null) {
  const byTier = {};
  for (const t of Object.values(OPPORTUNITY_TIER)) byTier[t] = 0;
  let verifiedExpirations = 0;
  let historicalExpirations = 0;
  let futureExpirations = 0;
  let unknownExpiration = 0;
  let noticeVerified = 0;
  let nearTermPriority = 0;
  let researchEligible = 0;
  let hold = 0;
  let renewalsClosed = 0;
  const hotels = new Set();
  const rejectedFalseExpirations = [];
  const byCountry = {};
  const byAgreementType = {};

  for (const o of opportunities) {
    byTier[o.tier] = (byTier[o.tier] || 0) + 1;
    if (o.hotelName) hotels.add(String(o.hotelName).toLowerCase());
    byAgreementType[o.agreementType || "UNKNOWN"] = (byAgreementType[o.agreementType || "UNKNOWN"] || 0) + 1;
    const c = o.country || "UNKNOWN";
    byCountry[c] = (byCountry[c] || 0) + 1;
    if (o.tier === OPPORTUNITY_TIER.HOLD_QUARANTINE) {
      hold += 1;
      if (/OPENING|LOAN|non_contract/i.test(o.holdReason || "")) {
        rejectedFalseExpirations.push({
          hotelName: o.hotelName || null,
          reason: o.holdReason,
          auditedMeaning: o.dateAudit?.auditedMeaning || null,
        });
      }
    }
    if (o.researchEligible) researchEligible += 1;
    if (o.verifiedEndDate) {
      verifiedExpirations += 1;
      if (o.monthsToExpiration != null && o.monthsToExpiration < 0) historicalExpirations += 1;
      if (o.monthsToExpiration != null && o.monthsToExpiration >= 0) futureExpirations += 1;
    } else if (o.tier !== OPPORTUNITY_TIER.HOLD_QUARANTINE) {
      unknownExpiration += 1;
    }
    if (o.noticeStatus === NOTICE_STATUS.VERIFIED || o.noticeStatus === NOTICE_STATUS.CALCULATED) {
      noticeVerified += 1;
    }
    if (o.nearTermOutreachPriority || (o.activeTargeting && o.within36Months)) nearTermPriority += 1;
    if (
      o.changeClass === OPPORTUNITY_CHANGE.CLOSED ||
      o.tier === OPPORTUNITY_TIER.CLOSED_SUPERSEDED
    ) {
      renewalsClosed += 1;
    }
  }

  const proj = projections || projectDualProspectLists(opportunities);
  const brandHotels = new Set(proj.brandConversionTargets.map((t) => String(t.hotel || "").toLowerCase()));
  const mgmtHotels = new Set(proj.managementContractTargets.map((t) => String(t.hotel || "").toLowerCase()));

  return {
    version: COMMERCIAL_TARGET_DISCOVERY_V1,
    opportunityCount: opportunities.length,
    distinctHotels: hotels.size,
    byTier,
    byAgreementType,
    byCountry,
    verifiedExpirations,
    historicalContractExpirations: historicalExpirations,
    futureContractExpirations: futureExpirations,
    unknownExpirationCount: unknownExpiration,
    noticePeriodsDiscovered: noticeVerified,
    /** App-filter near-term hint count — NOT research eligibility */
    nearTermOutreachPriorityCount: nearTermPriority,
    /** @deprecated alias of nearTermOutreachPriorityCount */
    validFutureDecisionWindows: nearTermPriority,
    researchEligibleCount: researchEligible,
    holdQuarantine: hold,
    renewalsOrTerminationsClosed: renewalsClosed,
    brandConversionListCount: proj.brandConversionTargets.length,
    brandConversionDistinctHotels: brandHotels.size,
    managementContractListCount: proj.managementContractTargets.length,
    managementContractDistinctHotels: mgmtHotels.size,
    closedSupersededArchiveCount: (proj.closedSupersededArchive || []).length,
    rejectedFalseExpirations: rejectedFalseExpirations.length,
    rejectedFalseExpirationSamples: rejectedFalseExpirations.slice(0, 25),
    researchHorizonFilter: "NONE",
    geographicResearchRestriction: "NONE",
    publicationState: "CUSTOMER_BLOCKED",
  };
}

export default {
  classifyCommercialOpportunity,
  projectDualProspectLists,
  filterProspectList,
  summarizeCommercialInventory,
  OPPORTUNITY_TIER,
  PROSPECT_LIST,
  AGREEMENT_TYPE,
  DEFERRED_LEASE_INTELLIGENCE,
  NOTICE_STATUS,
  OPPORTUNITY_CHANGE,
  HISTORICAL_APPLICABILITY,
  CONTRACT_DATE_CLASS,
  APP_DATE_FILTER,
};
