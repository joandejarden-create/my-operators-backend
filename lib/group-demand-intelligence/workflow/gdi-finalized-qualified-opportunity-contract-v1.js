/**
 * GDI Finalized Qualified Opportunity (FQO) contract V1
 *
 * Founder-approved product policy: local STAGED presentation eligibility.
 * Sales Ready is NOT required for FQO. Person-specific outreach channel IS.
 *
 * Lifecycle (presentation):
 *   RESEARCH → VERIFIED/QUALIFIED → FINALIZED STAGED → FOUNDER REVIEW → CURRENT
 *
 * Does not write Airtable, activate STAGED/CURRENT pointers, or spend providers.
 */
import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import { isPlausibleBuyerPersonName } from "./gdi-original-full-e2e-execution-restore-v1.js";
import { evaluateContactCompletenessForFinalization } from "../presentation/gdi-verified-commercial-route-v1.js";
import {
  evaluateNamedContactCoverageStandard,
  NAMED_CONTACT_COVERAGE,
} from "./gdi-named-contact-coverage-standard-v1.js";
import { isHistoricalOnlyCustomerExclusion } from "../commercial-inventory/gdi-unified-commercial-inventory-contract-v1.js";

const FQO_JUNK_NAME_RE =
  /\b(headquarters|enquire|beyond|colonial|dames|climate|monitoring|android|once|member\s*select|learning\s*futures|free\s*univ|booth|onpeak)\b/i;

/** Stricter than generic looksLikePersonName — rejects desks, orgs, and junk scrapes. */
function fqoLooksLikePersonName(name) {
  const n = text(name);
  if (!n || FQO_JUNK_NAME_RE.test(n)) return false;
  return isPlausibleBuyerPersonName(n) && looksLikePersonName(n);
}

export const GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1 =
  "gdi-finalized-qualified-opportunity-contract-v1";

export const FQO_STATUS = Object.freeze({
  FINALIZED_QUALIFIED: "FINALIZED_QUALIFIED",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  FOUNDER_HELD: "FOUNDER_HELD",
  FOUNDER_REJECTED: "FOUNDER_REJECTED",
  INELIGIBLE: "INELIGIBLE",
});

export const FQO_GATE = Object.freeze({
  EVIDENCE_BACKED_DEMAND: "EVIDENCE_BACKED_DEMAND",
  HOTEL_GEO_COMMERCIAL_FIT: "HOTEL_GEO_COMMERCIAL_FIT",
  FUTURE_SOURCING_PATHWAY: "FUTURE_SOURCING_PATHWAY",
  NAMED_COMMERCIAL_PERSON: "NAMED_COMMERCIAL_PERSON",
  PERSON_SPECIFIC_CHANNEL: "PERSON_SPECIFIC_CHANNEL",
  HOTEL_SPECIFIC_ACTION: "HOTEL_SPECIFIC_ACTION",
  COMPETITIVE_INCUMBENT_PATHWAY: "COMPETITIVE_INCUMBENT_PATHWAY",
});

const FUNCTIONAL_ORG_MAILBOX_RE =
  /^(info|contact|hello|press|pr|media|sales|support|admin|office|enquiries|inquiries|events?|meetings?|groups?|housing|lodging|reservations?|booking|noreply|no-reply|team|general|help|group[-_.]|comcoor|pcchair|paleo)@/i;

const PLACEHOLDER_RE =
  /CONTACT_RESEARCH_REQUIRED|TODO_RESEARCH|PLACEHOLDER|TBD_CONTACT|BUYER NOT YET|ROLE NOT YET/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

/**
 * Person-specific professional channel (email / direct phone / person LinkedIn).
 * Generic org inboxes and switchboards are NOT substitutes.
 */
export function evaluatePersonSpecificOutreachChannel(opportunity = {}, opts = {}) {
  const primary =
    opportunity.primaryContact && typeof opportunity.primaryContact === "object"
      ? opportunity.primaryContact
      : {
          name: opportunity.primaryContactName || opportunity.contactName,
          email: opportunity.primaryContactEmail || opportunity.contactEmail,
          phone: opportunity.primaryContactPhone || opportunity.contactPhone,
          linkedinUrl:
            opportunity.primaryContactLinkedin ||
            opportunity.linkedinUrl ||
            opportunity.contactLinkedin,
          emailType: opportunity.primaryContactEmailType,
        };

  const name = text(primary.name);
  const email = text(primary.email);
  const phone = text(primary.phone || primary.mobile || primary.directPhone);
  const linkedin = text(
    primary.linkedinUrl || primary.linkedin || primary.profileUrl,
  );
  const emailType = upper(primary.emailType || primary.emailClassification);

  const channels = [];
  const rejected = [];

  if (email && /@/.test(email)) {
    if (/%[0-9a-f]{2}|\s/i.test(email)) {
      rejected.push({
        kind: "EMAIL",
        value: email,
        reason: "MALFORMED_OR_ENCODED_EMAIL",
      });
    } else if (
      FUNCTIONAL_ORG_MAILBOX_RE.test(email) ||
      emailType === "GENERIC_ORGANIZATION" ||
      emailType === "ORG_FUNCTIONAL" ||
      emailType === "SWITCHBOARD"
    ) {
      rejected.push({
        kind: "EMAIL",
        value: email,
        reason: "GENERIC_OR_FUNCTIONAL_ORGANIZATION_INBOX",
      });
    } else {
      channels.push({
        kind: "DIRECT_WORK_EMAIL",
        value: email,
        provenance: text(
          primary.emailSource ||
            primary.emailProvenance ||
            primary.sourceUrl ||
            opportunity.contactChannelProvenance,
        ),
      });
    }
  }

  const contactKind = upper(
    primary.contactKind || primary.kind || opportunity.primaryContactKind || "",
  );
  const phoneType = upper(primary.phoneType || "");
  if (phone && /\d{7,}/.test(phone.replace(/\D/g, ""))) {
    // Org office / functional desks are not person-specific direct lines.
    if (
      contactKind === "FUNCTIONAL" ||
      phoneType === "SWITCHBOARD" ||
      phoneType === "ORGANIZATION" ||
      /headquarters|housing desk|main office/i.test(text(primary.role || primary.title || ""))
    ) {
      rejected.push({
        kind: "PHONE",
        value: phone,
        reason: "ORGANIZATION_OR_FUNCTIONAL_PHONE",
      });
    } else {
      channels.push({
        kind: "DIRECT_PROFESSIONAL_PHONE",
        value: phone,
        provenance: text(primary.phoneSource || primary.sourceUrl || ""),
      });
    }
  }

  if (/linkedin\.com\/in\//i.test(linkedin)) {
    channels.push({
      kind: "PERSON_LINKEDIN",
      value: linkedin,
      provenance: text(primary.linkedinSource || primary.sourceUrl || ""),
    });
  }

  const ok = channels.length > 0;
  return {
    ok,
    name: name || null,
    channels,
    rejected,
    reason: ok
      ? "At least one verified person-specific professional channel"
      : rejected.length
        ? "Only generic organizational routes present — not sufficient for FQO"
        : "No person-specific email, direct phone, or person LinkedIn",
  };
}

function collectEvidenceUrls(opp = {}) {
  return [
    ...(Array.isArray(opp.sourceUrls) ? opp.sourceUrls : []),
    ...(Array.isArray(opp.seedUrls) ? opp.seedUrls : []),
    ...(Array.isArray(opp.investigatedUrls) ? opp.investigatedUrls : []),
    ...(Array.isArray(opp.evidence)
      ? opp.evidence.map((e) => e?.url || e?.sourceUrl).filter(Boolean)
      : []),
    opp.primaryContact?.sourceUrl,
    opp.locationEvidenceUrl,
  ]
    .map(text)
    .filter((u) => /^https?:\/\//i.test(u));
}

function isCompetitive(opp = {}) {
  const scope = upper(opp.marketScope || opp.inventoryScope || "");
  return (
    scope === "COMPETITIVE" ||
    opp.competitiveMarketScopeEligible === true ||
    Boolean(opp.competitiveContext?.incumbentHotel) ||
    Boolean(opp.competitiveContext?.label)
  );
}

function hasFuturePathway(opp = {}) {
  if (opp.futureDemandPathway === true) return true;
  if (opp.accountDevelopmentReactivated === true) return true;
  const state = upper(
    opp.futureCycleEvidenceState ||
      opp.commercialActionabilityStateV1 ||
      opp.commercialTemporalDisposition ||
      "",
  );
  if (
    /CURRENT_FUTURE|FUTURE_CYCLE|OPEN_SOURCING|ACTIVE|ENTERING_WINDOW|WATCH_FUTURE/.test(
      state,
    )
  ) {
    return true;
  }
  if (isHistoricalOnlyCustomerExclusion(opp)) return false;
  const start = Date.parse(
    opp.eventStartDate || opp.needDates || opp.needDateStart || opp.eventDate || "",
  );
  if (Number.isFinite(start)) {
    const asOf = Date.parse(opp._asOf || Date.now());
    if (start >= asOf - 7 * 86400000) return true;
  }
  // Explicit next-cycle / overflow pathway stamps
  const pathway = upper(
    opp.competitiveContext?.pathway ||
      opp.opportunityPathway ||
      opp.sourcingPathway ||
      "",
  );
  if (/OVERFLOW|FUTURE_CYCLE|NEXT_CYCLE|OPEN_HOUSING|STAY.?TO.?PLAY/.test(pathway)) {
    return true;
  }
  // Soft pass when not historical and thesis claims future hotel demand
  const thesis = text(
    opp.hotelOpportunityThesis || opp.whyThisHotel || opp.whatIsHappening,
  );
  if (
    !isHistoricalOnlyCustomerExclusion(opp) &&
    /\b(202[6-9]|2030|next cycle|future|upcoming|overflow|sourcing)\b/i.test(thesis)
  ) {
    return true;
  }
  return false;
}

function hotelFitOk(opp = {}) {
  const thesis = text(
    opp.hotelOpportunityThesis ||
      opp.whyThisHotel ||
      opp.summaryWhyMatters ||
      opp.potentialBusinessForThisHotel ||
      opp.whatIsHappening,
  );
  if (thesis.length >= 40) return { ok: true, thesis };
  if (opp.hotelMarketScopeEligible === true && thesis.length >= 20) {
    return { ok: true, thesis };
  }
  return { ok: false, thesis };
}

function commercialActionOk(opp = {}) {
  const action = text(
    opp.recommendedNextStep ||
      opp.recommendedSalesOrMarketingAction ||
      opp.whyActNow ||
      opp.cardWhyNowLine ||
      opp.customerNarrative,
  );
  if (action.length >= 20) return { ok: true, action };
  const thesis = text(opp.hotelOpportunityThesis || opp.whyThisHotel || "");
  if (thesis.length >= 80) return { ok: true, action: thesis.slice(0, 200) };
  return { ok: false, action };
}

/**
 * Deterministic FQO evaluation. Never requires salesReady.
 */
export function evaluateFinalizedQualifiedOpportunity(opportunity = {}, opts = {}) {
  const opp = opportunity && typeof opportunity === "object" ? opportunity : {};
  const nameFn =
    typeof opts.looksLikePersonName === "function"
      ? opts.looksLikePersonName
      : fqoLooksLikePersonName;

  const founderDisposition = upper(
    opp.founderDisposition ||
      opp.founderReviewDisposition ||
      opp.publicationHoldReason ||
      "",
  );
  if (/REJECT|DECLIN/.test(founderDisposition) || opp.founderRejected === true) {
    return {
      contractVersion: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
      status: FQO_STATUS.FOUNDER_REJECTED,
      finalizedQualified: false,
      salesReadyRequired: false,
      salesReady: opp.salesReady === true,
      gates: {},
      reasons: ["FOUNDER_REJECTED"],
      bindingFounderHold: true,
    };
  }
  if (
    /HOLD|PAUSE|DEFER/.test(founderDisposition) ||
    opp.founderHold === true ||
    opp.founderHeld === true
  ) {
    return {
      contractVersion: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
      status: FQO_STATUS.FOUNDER_HELD,
      finalizedQualified: false,
      salesReadyRequired: false,
      salesReady: opp.salesReady === true,
      gates: {},
      reasons: ["FOUNDER_HELD"],
      bindingFounderHold: true,
    };
  }

  const gates = {};
  const reasons = [];

  // 1. Evidence-backed real demand
  const demand = text(
    opp.organizationName ||
      opp.organization ||
      opp.programmeName ||
      opp.demandGenerator ||
      opp.title,
  );
  const urls = collectEvidenceUrls(opp);
  const demandOk = Boolean(demand) && urls.length > 0;
  gates[FQO_GATE.EVIDENCE_BACKED_DEMAND] = {
    ok: demandOk,
    demand: demand || null,
    evidenceUrlCount: urls.length,
  };
  if (!demandOk) {
    reasons.push(
      !demand ? "MISSING_DEMAND_GENERATOR" : "MISSING_EVIDENCE_URLS",
    );
  }

  // 2. Hotel geography / commercial fit
  const fit = hotelFitOk(opp);
  gates[FQO_GATE.HOTEL_GEO_COMMERCIAL_FIT] = {
    ok: fit.ok,
    thesisLength: fit.thesis.length,
  };
  if (!fit.ok) reasons.push("MISSING_HOTEL_FIT_THESIS");

  // 3. Future sourcing / booking / next-cycle pathway
  const futureOk =
    hasFuturePathway(opp) && !isHistoricalOnlyCustomerExclusion(opp);
  gates[FQO_GATE.FUTURE_SOURCING_PATHWAY] = { ok: futureOk };
  if (!futureOk) reasons.push("NO_VALID_FUTURE_SOURCING_PATHWAY");

  // 4. Named commercially relevant person
  const contactEval = evaluateContactCompletenessForFinalization(opp, {
    looksLikePersonName: nameFn,
    allowOrgRouteOnly: false,
  });
  const coverage = evaluateNamedContactCoverageStandard(opp);
  const primaryName = text(
    opp.primaryContact?.name || opp.primaryContactName || opp.contactName,
  );
  const namedOk =
    contactEval.complete === true &&
    contactEval.mode === "NAMED_PERSON" &&
    nameFn(primaryName) &&
    coverage.namedCount >= 1;
  gates[FQO_GATE.NAMED_COMMERCIAL_PERSON] = {
    ok: namedOk,
    name: primaryName || null,
    role: text(opp.primaryContact?.role || opp.primaryContact?.title),
    coverage: coverage.coverage,
  };
  if (!namedOk) reasons.push("NAMED_COMMERCIAL_PERSON_REQUIRED");

  // 5. Person-specific professional channel
  const channelEval = evaluatePersonSpecificOutreachChannel(opp, opts);
  gates[FQO_GATE.PERSON_SPECIFIC_CHANNEL] = {
    ok: channelEval.ok,
    channels: channelEval.channels,
    rejected: channelEval.rejected,
    reason: channelEval.reason,
  };
  if (!channelEval.ok) reasons.push("PERSON_SPECIFIC_CHANNEL_REQUIRED");

  // 6. Hotel-specific recommended commercial action
  const action = commercialActionOk(opp);
  gates[FQO_GATE.HOTEL_SPECIFIC_ACTION] = {
    ok: action.ok,
    actionLength: text(action.action).length,
  };
  if (!action.ok) reasons.push("MISSING_HOTEL_SPECIFIC_COMMERCIAL_ACTION");

  // Competitive extras
  const competitive = isCompetitive(opp);
  if (competitive) {
    const incumbent = text(
      opp.competitiveContext?.incumbentHotel ||
        opp.incumbentHotel ||
        opp.competitorHotel,
    );
    const pathway = text(
      opp.competitiveContext?.pathway ||
        opp.opportunityPathway ||
        opp.sourcingPathway,
    );
    const competitiveOk =
      Boolean(incumbent) &&
      (Boolean(pathway) || hasFuturePathway(opp));
    gates[FQO_GATE.COMPETITIVE_INCUMBENT_PATHWAY] = {
      ok: competitiveOk,
      incumbentHotel: incumbent || null,
      pathway: pathway || null,
    };
    if (!competitiveOk) {
      reasons.push("COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED");
    }
  }

  if (PLACEHOLDER_RE.test(JSON.stringify({ title: opp.title, action: action.action, thesis: fit.thesis, name: primaryName }))) {
    reasons.push("INTERNAL_PLACEHOLDER_PRESENT");
    gates.PLACEHOLDER_FREE = { ok: false };
  } else {
    gates.PLACEHOLDER_FREE = { ok: true };
  }

  const allGateOk = Object.values(gates).every((g) => g && g.ok === true);
  const finalizedQualified = allGateOk && reasons.length === 0;

  return {
    contractVersion: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    status: finalizedQualified
      ? FQO_STATUS.FINALIZED_QUALIFIED
      : reasons.some((r) => /HISTORICAL|INELIGIBLE|WRONG/.test(r))
        ? FQO_STATUS.INELIGIBLE
        : FQO_STATUS.RESEARCH_CONTINUATION,
    finalizedQualified,
    salesReadyRequired: false,
    salesReady: opp.salesReady === true,
    salesReadyWasNotBlocking: true,
    competitive,
    marketScope: competitive ? "COMPETITIVE" : "HOTEL",
    priorityBand: upper(opp.priority || opp.priorityBand || "") || null,
    gates,
    reasons,
    evidenceUrls: urls.slice(0, 8),
    namedCoverage: coverage.coverage || NAMED_CONTACT_COVERAGE.ZERO,
    secondaryPresent: coverage.namedCount >= 2,
    bindingFounderHold: false,
  };
}

/**
 * Filter opportunities to FQO-pass only (for NEW local STAGED candidacy).
 * Does not mutate inputs. Does not rewrite historical packages.
 */
export function selectFinalizedQualifiedOpportunities(opportunities = [], opts = {}) {
  const rows = Array.isArray(opportunities) ? opportunities : [];
  const evaluated = rows.map((o) => ({
    opportunity: o,
    evaluation: evaluateFinalizedQualifiedOpportunity(o, opts),
  }));
  const qualified = evaluated
    .filter((r) => r.evaluation.finalizedQualified === true)
    .map((r) => ({
      ...r.opportunity,
      finalizedQualifiedOpportunityV1: {
        status: r.evaluation.status,
        contractVersion: r.evaluation.contractVersion,
        salesReadyRequired: false,
        salesReady: r.evaluation.salesReady,
        evaluatedAt: new Date().toISOString(),
      },
    }));
  return {
    contractVersion: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    count: qualified.length,
    opportunities: qualified,
    evaluations: evaluated.map((r) => ({
      id: r.opportunity.id || r.opportunity.opportunityId || null,
      ...r.evaluation,
    })),
  };
}

/**
 * Assert package candidates meet FQO (for staging certification).
 */
export function assertFinalizedQualifiedPackage(opportunities = [], opts = {}) {
  const selected = selectFinalizedQualifiedOpportunities(opportunities, opts);
  const blockers = selected.evaluations
    .filter((e) => e.finalizedQualified !== true)
    .map((e) => ({
      code: "NOT_FINALIZED_QUALIFIED",
      opportunityId: e.id,
      reasons: e.reasons,
      status: e.status,
    }));
  const requireAll = opts.requireAll !== false;
  return {
    ok: requireAll ? blockers.length === 0 && selected.count === (opportunities || []).length : selected.count > 0,
    contractVersion: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    qualifiedCount: selected.count,
    total: (opportunities || []).length,
    blockers,
  };
}

export default {
  GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
  FQO_STATUS,
  FQO_GATE,
  evaluatePersonSpecificOutreachChannel,
  evaluateFinalizedQualifiedOpportunity,
  selectFinalizedQualifiedOpportunities,
  assertFinalizedQualifiedPackage,
};
