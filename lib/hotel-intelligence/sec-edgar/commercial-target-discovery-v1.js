/**
 * Commercial target discovery — conversion & management-contract opportunities.
 *
 * Optimizes for verified hotel targets + reliable contractual timing (not legal-term depth).
 * Two projections over shared canonical research identities:
 *   1) Brand Conversion Targets
 *   2) Hotel Management Contract Targets
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
  HOLD_QUARANTINE: "HOLD_QUARANTINE",
});

export const PROSPECT_LIST = Object.freeze({
  BRAND_CONVERSION: "BRAND_CONVERSION",
  MANAGEMENT_CONTRACT: "MANAGEMENT_CONTRACT",
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

const FRANCHISE_LIKE = new Set(["FRANCHISE", "LICENSE"]);
const MANAGEMENT_LIKE = new Set(["MANAGEMENT"]);

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
export function classifyCommercialOpportunity(obs = {}, opts = {}) {
  const asOf = opts.asOf || ymd();
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

  // Closed / not active if renewed or superseded with verified successor status
  if (
    historicalApplicability === HISTORICAL_APPLICABILITY.SUPERSEDED_OR_RENEWED ||
    historicalApplicability === HISTORICAL_APPLICABILITY.TERMINATED
  ) {
    return {
      version: COMMERCIAL_TARGET_DISCOVERY_V1,
      tier: OPPORTUNITY_TIER.HOLD_QUARANTINE,
      holdReason: `agreement_${historicalApplicability.toLowerCase()}`,
      activeTargeting: false,
      changeClass: OPPORTUNITY_CHANGE.CLOSED,
      hotelName,
      agreementType,
      primaryDate,
      renewalStatusClass: renewal.renewalStatusClass,
      historicalApplicability,
      dateAudit: audit,
      publicationState: "CUSTOMER_BLOCKED",
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

  // Past expiration without renewal evidence → keep as follow-up, not active window
  const expiredUnknownRenewal =
    monthsToExpiration != null && monthsToExpiration < 0 && historicalApplicability !== HISTORICAL_APPLICABILITY.CURRENTLY_CORROBORATED;

  const researchId = `ct_${stableId([
    hotelName,
    agreementType,
    primaryDate || "nodate",
    sourceUrl || obs.accession || "",
  ])}`;

  return {
    version: COMMERCIAL_TARGET_DISCOVERY_V1,
    researchId,
    tier,
    activeTargeting:
      (tier === OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW ||
        tier === OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION) &&
      !expiredUnknownRenewal &&
      withinHorizon !== false,
    changeClass: OPPORTUNITY_CHANGE.NEW,
    hotelName,
    location: obs.location || null,
    brand: obs.brand || null,
    owner: obs.owner || null,
    managementCompany: obs.managementCompany || null,
    agreementType,
    verifiedEndDate: gate.ok ? primaryDate : null,
    noticePeriodDays: noticeDays,
    noticeStatus,
    noticeDeadline: noticeWindow?.date || explicitNoticeDeadline || null,
    noticeDeadlinePassed,
    monthsToExpiration,
    monthsToNoticeDeadline: monthsToNotice,
    within36Months: Boolean(withinHorizon),
    renewalStatusClass: renewal.renewalStatusClass,
    historicalApplicability: expiredUnknownRenewal
      ? HISTORICAL_APPLICABILITY.REQUIRES_FOLLOW_UP
      : historicalApplicability,
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

function holdResult(reason, extra = {}) {
  return {
    version: COMMERCIAL_TARGET_DISCOVERY_V1,
    tier: OPPORTUNITY_TIER.HOLD_QUARANTINE,
    holdReason: reason,
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
 */
export function projectDualProspectLists(opportunities = [], opts = {}) {
  const asOf = opts.asOf || ymd();
  const brandConversion = [];
  const managementContract = [];

  for (const opp of opportunities) {
    if (!opp || opp.tier === OPPORTUNITY_TIER.HOLD_QUARANTINE) continue;
    // Closed / superseded stay out of active lists
    if (opp.changeClass === OPPORTUNITY_CHANGE.CLOSED) continue;

    const base = {
      researchId: opp.researchId,
      hotel: opp.hotelName,
      location: opp.location,
      brand: opp.brand,
      owner: opp.owner,
      managementCompany: opp.managementCompany,
      agreementType: opp.agreementType,
      verifiedEndDate: opp.verifiedEndDate,
      noticePeriodDays: opp.noticePeriodDays,
      noticeStatus: opp.noticeStatus,
      noticeDeadline: opp.noticeDeadline,
      renewalStatus: opp.renewalStatusClass,
      historicalApplicability: opp.historicalApplicability,
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
      activeTargeting: Boolean(opp.activeTargeting),
    };

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
    // Tier C without clear type → both potential-target categories when conversion signals present
    if (
      opp.tier === OPPORTUNITY_TIER.TIER_C_CONVERSION_RESEARCH_CANDIDATE &&
      !FRANCHISE_LIKE.has(opp.agreementType) &&
      !MANAGEMENT_LIKE.has(opp.agreementType)
    ) {
      if (opp.conversionSignals?.brand || opp.brand) {
        brandConversion.push({
          ...base,
          prospectList: PROSPECT_LIST.BRAND_CONVERSION,
          potentialTargetWithoutVerifiedDate: true,
          decisionMakerFocus: "hotel_owner_and_brand_counterparties",
        });
      }
      if (opp.conversionSignals?.management || opp.managementCompany) {
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
    sharedResearchIdentities: [...new Set(opportunities.map((o) => o.researchId).filter(Boolean))],
    note: "Two projections over shared research identities — not independent hotel databases",
  };
}

/**
 * Filter helpers for BD workflows (research-only).
 */
export function filterProspectList(list = [], filters = {}) {
  return list.filter((row) => {
    if (filters.brand && !new RegExp(filters.brand, "i").test(row.brand || "")) return false;
    if (filters.owner && !new RegExp(filters.owner, "i").test(row.owner || "")) return false;
    if (filters.managementCompany && !new RegExp(filters.managementCompany, "i").test(row.managementCompany || ""))
      return false;
    if (filters.geography && !new RegExp(filters.geography, "i").test(row.location || "")) return false;
    if (filters.agreementType && String(row.agreementType).toUpperCase() !== String(filters.agreementType).toUpperCase())
      return false;
    if (filters.expirationYear && String(row.verifiedEndDate || "").slice(0, 4) !== String(filters.expirationYear))
      return false;
    if (filters.tier && row.opportunityTier !== filters.tier) return false;
    if (filters.activeOnly && !row.activeTargeting) return false;
    if (filters.requireVerifiedDate && !row.verifiedEndDate) return false;
    return true;
  });
}

export function summarizeCommercialInventory(opportunities = [], projections = null) {
  const byTier = {};
  for (const t of Object.values(OPPORTUNITY_TIER)) byTier[t] = 0;
  let verifiedExpirations = 0;
  let noticeVerified = 0;
  let futureWindows = 0;
  let hold = 0;
  let renewalsClosed = 0;
  const rejectedFalseExpirations = [];

  for (const o of opportunities) {
    byTier[o.tier] = (byTier[o.tier] || 0) + 1;
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
    if (o.verifiedEndDate) verifiedExpirations += 1;
    if (o.noticeStatus === NOTICE_STATUS.VERIFIED || o.noticeStatus === NOTICE_STATUS.CALCULATED) {
      noticeVerified += 1;
    }
    if (o.activeTargeting && o.within36Months) futureWindows += 1;
    if (o.changeClass === OPPORTUNITY_CHANGE.CLOSED) renewalsClosed += 1;
  }

  const proj = projections || projectDualProspectLists(opportunities);
  return {
    version: COMMERCIAL_TARGET_DISCOVERY_V1,
    opportunityCount: opportunities.length,
    byTier,
    verifiedExpirations,
    noticePeriodsDiscovered: noticeVerified,
    validFutureDecisionWindows: futureWindows,
    holdQuarantine: hold,
    renewalsOrTerminationsClosed: renewalsClosed,
    brandConversionListCount: proj.brandConversionTargets.length,
    managementContractListCount: proj.managementContractTargets.length,
    rejectedFalseExpirations: rejectedFalseExpirations.length,
    rejectedFalseExpirationSamples: rejectedFalseExpirations.slice(0, 25),
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
  NOTICE_STATUS,
  OPPORTUNITY_CHANGE,
  HISTORICAL_APPLICABILITY,
};
