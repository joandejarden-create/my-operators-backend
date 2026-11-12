/**
 * Canonical GDI Opportunity Quality Contract V1
 *
 * Reusable, hotel-agnostic validation composing existing GDI modules.
 * Separates:
 *   RESEARCH_RECORD_INTEGRITY — structural honesty + provenance
 *   COMMERCIAL_OPPORTUNITY_CERTIFICATION — actionable commercial readiness
 *
 * Does not invent contacts, demand, or titles. Does not write stores.
 */

import {
  classifyOpportunityTitleDefects,
  hasMeetingCredentialContamination,
  customerSafeOpportunityTitle,
  TITLE_DEFECT,
} from "./opportunity-title-quality-v1.js";
import { extractContactSnapshot } from "./research/bethesda-88-contact-quality-completion-v1.js";
import { isGenericOrganizationEmail } from "./contact-display-surface-v1.js";
import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "./account-development-eligibility-v1.js";
import { classifyCommercialDemandThesis } from "./commercial-demand-thesis-standard-v1.js";
import { evaluateCommercialDemandTypeQuality } from "./commercial-demand-type-v1.js";
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";

export const GDI_OPPORTUNITY_QUALITY_CONTRACT_V1 =
  "gdi-opportunity-quality-contract-v1";

export const RESEARCH_INTEGRITY = Object.freeze({
  PASS: "RESEARCH_INTEGRITY_PASS",
  FAIL: "RESEARCH_INTEGRITY_FAIL",
  QUARANTINED: "RESEARCH_INTEGRITY_QUARANTINED",
});

export const COMMERCIAL_CERT = Object.freeze({
  SALES_READY_ELIGIBLE: "SALES_READY_ELIGIBLE",
  ACCOUNT_DEVELOPMENT_ELIGIBLE: "ACCOUNT_DEVELOPMENT_ELIGIBLE",
  PARTNERSHIP_WATCH: "PARTNERSHIP_WATCH",
  OVERFLOW_WATCH: "OVERFLOW_WATCH",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  DISQUALIFIED: "DISQUALIFIED",
  LINKED_RESEARCH: "LINKED_RESEARCH",
  QUARANTINED: "QUARANTINED",
});

export const QUALITY_AXIS = Object.freeze({
  IDENTITY: "IDENTITY",
  TITLE: "TITLE",
  CATEGORY: "CATEGORY",
  DESCRIPTION: "DESCRIPTION",
  DEMAND: "DEMAND",
  HOTEL_FIT: "HOTEL_FIT",
  CONTACT: "CONTACT",
  SALES_ACTION: "SALES_ACTION",
  MATURITY: "MATURITY",
  PUBLICATION: "PUBLICATION",
  COMMERCIAL_DEMAND_TYPE: "COMMERCIAL_DEMAND_TYPE",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function len(v) {
  return text(v).length;
}

function axis(result, code, ok, detail = null, blockers = []) {
  return { axis: code, ok: Boolean(ok), detail, blockers: blockers.filter(Boolean) };
}

function descriptionBody(opp = {}) {
  return text(
    opp.whatIsHappening ||
      opp.hotelOpportunityThesis ||
      opp.summaryWhat ||
      opp.tileCustomerSummary ||
      opp.description ||
      opp.overview ||
      ""
  );
}

function categoryOf(opp = {}) {
  return text(
    opp.category ||
      opp.demandCategory ||
      opp.segment ||
      opp.workflowCategory ||
      opp.gdiCategory ||
      ""
  );
}

function salesActionOf(opp = {}) {
  return text(
    opp.recommendedNextAction ||
      opp.nextSalesAction ||
      opp.salesAction ||
      opp.outreachRecommendation ||
      opp.nextAction ||
      ""
  );
}

function maturityHint(opp = {}) {
  const m = text(
    opp.commercialMaturity ||
      opp.priorityPillLabel ||
      opp.priority ||
      opp.historicalRecoveryDisposition ||
      ""
  )
    .toUpperCase()
    .replace(/\s+/g, "_");
  if (/DISQUALIFIED/.test(m)) return COMMERCIAL_CERT.DISQUALIFIED;
  if (/SALES_READY/.test(m)) return COMMERCIAL_CERT.SALES_READY_ELIGIBLE;
  if (/ACCOUNT_DEVELOPMENT/.test(m)) return COMMERCIAL_CERT.ACCOUNT_DEVELOPMENT_ELIGIBLE;
  if (/PARTNERSHIP/.test(m)) return COMMERCIAL_CERT.PARTNERSHIP_WATCH;
  if (/OVERFLOW/.test(m)) return COMMERCIAL_CERT.OVERFLOW_WATCH;
  if (/QUARANTINE|TITLE_SOURCE_INVALID/.test(m)) return COMMERCIAL_CERT.QUARANTINED;
  return COMMERCIAL_CERT.RESEARCH_CONTINUATION;
}

/**
 * Evaluate one opportunity against the shared quality contract.
 *
 * @param {object} opportunity
 * @param {object} [ctx]
 * @param {string} [ctx.familyRole] PRIMARY|LINKED_RESEARCH|QUARANTINED|STANDALONE
 * @param {string} [ctx.familyId]
 * @param {boolean} [ctx.historical]
 */
export function evaluateOpportunityQualityContract(opportunity = {}, ctx = {}) {
  const opp = opportunity && typeof opportunity === "object" ? opportunity : {};
  const id = text(opp.id || opp.opportunityId);
  const title = text(opp.title || opp.opportunityName || opp.name);
  const org = text(opp.organizationName || opp.company || opp.accountName);
  const familyRole = text(ctx.familyRole || opp.commercialEventFamilyRole || "").toUpperCase();
  const contact = extractContactSnapshot(opp);
  const titleDefects = classifyOpportunityTitleDefects(title, {
    organizationName: org,
  });
  const safeTitle = customerSafeOpportunityTitle(opp);
  const desc = descriptionBody(opp);
  const category = categoryOf(opp);
  const salesAction = salesActionOf(opp);
  const hotelFitText = text(
    opp.hotelFitRationale ||
      opp.whyThisHotel ||
      opp.localConnection ||
      opp.hotelFitNote ||
      ""
  );

  let thesis = null;
  try {
    thesis = classifyCommercialDemandThesis(opp);
  } catch {
    thesis = { thesisStandard: "UNKNOWN", error: true };
  }

  let sr = null;
  let ad = null;
  try {
    sr = evaluateSharedCommercialEligibility(opp);
  } catch (err) {
    sr = { salesReady: false, status: "ERROR", error: err?.message || String(err) };
  }
  try {
    ad = evaluateAccountDevelopmentEligibility(opp);
  } catch (err) {
    ad = {
      accountDevelopment: false,
      status: "ERROR",
      error: err?.message || String(err),
    };
  }

  let customerReady = null;
  try {
    customerReady = isGdiCustomerOpportunityReady(opp);
  } catch (err) {
    customerReady = { ok: false, error: err?.message || String(err) };
  }

  const axes = [];

  // IDENTITY
  const identityBlockers = [];
  if (!id) identityBlockers.push("MISSING_ID");
  if (!org) identityBlockers.push("MISSING_ORGANIZATION");
  if (!title && !safeTitle) identityBlockers.push("MISSING_TITLE");
  if (familyRole === "QUARANTINED" || titleDefects.includes(TITLE_DEFECT.TITLE_SOURCE_INVALID)) {
    identityBlockers.push("TITLE_SOURCE_INVALID_OR_QUARANTINED");
  }
  axes.push(
    axis(
      null,
      QUALITY_AXIS.IDENTITY,
      identityBlockers.length === 0 ||
        (id &&
          org &&
          !identityBlockers.includes("MISSING_ID") &&
          !identityBlockers.includes("MISSING_ORGANIZATION")),
      { id, org, familyRole: familyRole || "STANDALONE", familyId: ctx.familyId || null },
      identityBlockers
    )
  );

  // TITLE
  const titleOk =
    Boolean(safeTitle) &&
    !hasMeetingCredentialContamination(title) &&
    !titleDefects.includes(TITLE_DEFECT.TITLE_SOURCE_INVALID) &&
    !titleDefects.includes(TITLE_DEFECT.MEETING_CREDENTIALS) &&
    !titleDefects.includes(TITLE_DEFECT.SOURCE_NAV_CONTAMINATION);
  axes.push(
    axis(null, QUALITY_AXIS.TITLE, titleOk, {
      title: hasMeetingCredentialContamination(title)
        ? "[REDACTED]"
        : title.slice(0, 120),
      safeTitle: safeTitle || null,
      defects: titleDefects,
    }, titleOk ? [] : titleDefects)
  );

  // CATEGORY
  const categoryBad =
    !category ||
    /^(other|unclassified|unknown|n\/a|misc)$/i.test(category) ||
    /closed\s*\/\s*disqualified/i.test(category);
  axes.push(
    axis(null, QUALITY_AXIS.CATEGORY, !categoryBad, { category: category || null }, categoryBad
      ? ["CATEGORY_MISSING_OR_FALLBACK"]
      : [])
  );

  // DESCRIPTION
  const descBlockers = [];
  if (len(desc) < 40) descBlockers.push("DESCRIPTION_TOO_SHORT");
  if (hasMeetingCredentialContamination(desc)) descBlockers.push("DESCRIPTION_CREDENTIAL_LEAK");
  if (/lorem ipsum|as an ai|i cannot|placeholder/i.test(desc)) {
    descBlockers.push("DESCRIPTION_FILLER");
  }
  axes.push(
    axis(null, QUALITY_AXIS.DESCRIPTION, descBlockers.length === 0, {
      length: len(desc),
    }, descBlockers)
  );

  // DEMAND
  const demandBlockers = [];
  const thesisStd = text(thesis?.thesisStandard || thesis?.standard || "");
  if (/SPECULATIVE|UNKNOWN|INSUFFICIENT/i.test(thesisStd)) {
    demandBlockers.push(`DEMAND_THESIS:${thesisStd || "UNKNOWN"}`);
  }
  if (len(desc) < 60 && !opp.sources?.length && !opp.officialSource) {
    demandBlockers.push("WEAK_DEMAND_EVIDENCE");
  }
  axes.push(
    axis(null, QUALITY_AXIS.DEMAND, demandBlockers.length === 0, {
      thesisStandard: thesisStd || null,
      demandClass:
        opp.demandStatus ||
        opp.demandEvidenceClass ||
        opp.demandConfidence ||
        null,
    }, demandBlockers)
  );

  // COMMERCIAL DEMAND TYPE (Guest Rooms / Local Catering / Both)
  let demandTypeEval = null;
  try {
    demandTypeEval = evaluateCommercialDemandTypeQuality(opp, {
      familyRole,
      historical: ctx.historical === true,
    });
  } catch (err) {
    demandTypeEval = {
      ok: false,
      blockers: ["DEMAND_TYPE_EVAL_ERROR"],
      detail: { error: err?.message || String(err) },
    };
  }
  axes.push(
    axis(
      null,
      QUALITY_AXIS.COMMERCIAL_DEMAND_TYPE,
      demandTypeEval?.ok !== false,
      {
        commercialDemandType: demandTypeEval?.commercialDemandType || null,
        revenueStreamClass: demandTypeEval?.revenueStreamClass || null,
        actionable: demandTypeEval?.actionable === true,
        ...(demandTypeEval?.detail || {}),
      },
      demandTypeEval?.blockers || [],
    ),
  );

  // HOTEL FIT
  const fitBlockers = [];
  const fitHonesty = text(opp.hotelFitHonesty || opp.hotelFitQuality || "");
  const priorityUpper = text(opp.priority).toUpperCase();
  const destinationOutside =
    /DESTINATION_OUTSIDE|OUT_OF_MARKET|CATEGORY_WATCH_NO_SINGLE_BUYER/i.test(
      fitHonesty
    ) ||
    text(opp.demandTerritoryFit).toUpperCase() === "OUTSIDE" ||
    priorityUpper === "DISQUALIFIED";
  if (/DISQUALIFIED/i.test(fitHonesty) && !destinationOutside) {
    fitBlockers.push("HOTEL_FIT_DISQUALIFIED");
  }
  if (
    len(hotelFitText) < 24 &&
    !/OVERFLOW|PARTNERSHIP|DISQUALIFIED|DESTINATION_OUTSIDE|CATEGORY_WATCH/i.test(
      fitHonesty
    )
  ) {
    fitBlockers.push("HOTEL_FIT_RATIONALE_THIN");
  }
  if (/OVERFLOW_WATCH_COMPETITOR_HOST|OUT_OF_MARKET|DESTINATION_OUTSIDE/i.test(fitHonesty)) {
    // Honest overflow/out-of-market is OK for integrity; commercial cert handles maturity.
  }
  axes.push(
    axis(null, QUALITY_AXIS.HOTEL_FIT, !fitBlockers.includes("HOTEL_FIT_DISQUALIFIED"), {
      hotelFitHonesty: fitHonesty || null,
      rationaleLength: len(hotelFitText),
      destinationOutside,
    }, fitBlockers)
  );

  // CONTACT
  // Named contacts are NOT required for disqualified / destination-outside /
  // category-watch records — require an honest research outcome or org route instead.
  const contactBlockers = [];
  const namedOk =
    contact.named === true && looksLikePersonName(contact.name || contact.displayName);
  const contactExempt =
    destinationOutside ||
    priorityUpper === "DISQUALIFIED" ||
    /EXHAUSTED_|NOT_APPLICABLE_CATEGORY_WATCH|RESOLVED_NAMED_BUT_DESTINATION_OUTSIDE/i.test(
      text(opp.contactResearchStatus)
    );
  if (!namedOk && !contactExempt) contactBlockers.push("NAMED_CONTACT_MISSING");
  if (!contact.hasDirectEmail && !contact.hasOrgRoute && !contactExempt) {
    contactBlockers.push("NO_OUTREACH_ROUTE");
  }
  if (
    contact.hasDirectEmail &&
    isGenericOrganizationEmail(contact.email)
  ) {
    contactBlockers.push("ORG_INBOX_LABELED_AS_DIRECT");
  }
  const contactAxisOk = contactExempt
    ? contactBlockers.length === 0
    : namedOk && (contact.hasDirectEmail || contact.hasOrgRoute);
  axes.push(
    axis(null, QUALITY_AXIS.CONTACT, contactAxisOk, {
      named: namedOk,
      name: namedOk ? contact.name : null,
      coverage: contact.coverage,
      hasDirectEmail: contact.hasDirectEmail === true,
      hasOrgRoute: contact.hasOrgRoute === true,
      contactExempt,
      contactResearchStatus: text(opp.contactResearchStatus) || null,
    }, contactBlockers)
  );

  // SALES ACTION
  const actionBlockers = [];
  if (len(salesAction) < 24) actionBlockers.push("SALES_ACTION_MISSING_OR_THIN");
  axes.push(
    axis(null, QUALITY_AXIS.SALES_ACTION, actionBlockers.length === 0, {
      length: len(salesAction),
    }, actionBlockers)
  );

  // MATURITY / engines
  const storedMaturity = maturityHint(opp);
  let recommendedCommercial = COMMERCIAL_CERT.RESEARCH_CONTINUATION;
  if (familyRole === "QUARANTINED" || titleDefects.includes(TITLE_DEFECT.TITLE_SOURCE_INVALID)) {
    recommendedCommercial = COMMERCIAL_CERT.QUARANTINED;
  } else if (familyRole === "LINKED_RESEARCH") {
    recommendedCommercial = COMMERCIAL_CERT.LINKED_RESEARCH;
  } else if (storedMaturity === COMMERCIAL_CERT.DISQUALIFIED || /DISQUALIFIED/i.test(fitHonesty)) {
    recommendedCommercial = COMMERCIAL_CERT.DISQUALIFIED;
  } else if (sr?.salesReady === true) {
    recommendedCommercial = COMMERCIAL_CERT.SALES_READY_ELIGIBLE;
  } else if (ad?.accountDevelopment === true) {
    recommendedCommercial = COMMERCIAL_CERT.ACCOUNT_DEVELOPMENT_ELIGIBLE;
  } else if (storedMaturity === COMMERCIAL_CERT.PARTNERSHIP_WATCH) {
    recommendedCommercial = COMMERCIAL_CERT.PARTNERSHIP_WATCH;
  } else if (
    storedMaturity === COMMERCIAL_CERT.OVERFLOW_WATCH ||
    /OVERFLOW/i.test(fitHonesty)
  ) {
    recommendedCommercial = COMMERCIAL_CERT.OVERFLOW_WATCH;
  }

  axes.push(
    axis(null, QUALITY_AXIS.MATURITY, true, {
      storedMaturity,
      recommendedCommercial,
      salesReadyEligible: sr?.salesReady === true,
      accountDevelopmentEligible: ad?.accountDevelopment === true,
      srStatus: sr?.status || null,
      adStatus: ad?.status || null,
    }, [])
  );

  // PUBLICATION
  const pubOk = customerReady?.ok === true && recommendedCommercial === COMMERCIAL_CERT.SALES_READY_ELIGIBLE;
  axes.push(
    axis(null, QUALITY_AXIS.PUBLICATION, pubOk, {
      customerReady: customerReady?.ok === true,
      eligibleForPublicationReview: pubOk,
      autoPublishAuthorized: false,
    }, pubOk ? [] : ["NOT_ELIGIBLE_FOR_CUSTOMER_PUBLICATION"])
  );

  const criticalIntegrityFail =
    familyRole === "QUARANTINED" ||
    titleDefects.includes(TITLE_DEFECT.TITLE_SOURCE_INVALID) ||
    titleDefects.includes(TITLE_DEFECT.MEETING_CREDENTIALS) ||
    !id;

  let researchIntegrity = RESEARCH_INTEGRITY.PASS;
  if (familyRole === "QUARANTINED" || titleDefects.includes(TITLE_DEFECT.TITLE_SOURCE_INVALID)) {
    researchIntegrity = RESEARCH_INTEGRITY.QUARANTINED;
  } else if (criticalIntegrityFail || !id || !org) {
    researchIntegrity = RESEARCH_INTEGRITY.FAIL;
  } else {
    // Integrity allows honest unknowns (missing contact/description) if identity+title OK.
    const integrityAxesOk = axes
      .filter((a) =>
        [QUALITY_AXIS.IDENTITY, QUALITY_AXIS.TITLE].includes(a.axis)
      )
      .every((a) => a.ok);
    researchIntegrity = integrityAxesOk
      ? RESEARCH_INTEGRITY.PASS
      : RESEARCH_INTEGRITY.FAIL;
  }

  const allBlockers = axes.flatMap((a) =>
    (a.blockers || []).map((b) => `${a.axis}:${b}`)
  );

  const nextActions = [];
  if (!namedOk) nextActions.push("BOUNDED_NAMED_CONTACT_RESEARCH");
  if (!contact.hasDirectEmail && !contact.hasOrgRoute) {
    nextActions.push("RECOVER_ORG_OR_PERSON_OUTREACH_ROUTE");
  }
  if (!titleOk) nextActions.push("REPAIR_TITLE_FROM_SOURCE");
  if (descBlockers.length) nextActions.push("STRENGTHEN_SOURCE_GROUNDED_DESCRIPTION");
  if (demandBlockers.length) nextActions.push("CONTINUE_DEMAND_EVIDENCE_RESEARCH");
  if (fitBlockers.includes("HOTEL_FIT_RATIONALE_THIN")) {
    nextActions.push("DOCUMENT_BETHESDA_HOTEL_FIT_OR_OVERFLOW_HONESTY");
  }
  if (actionBlockers.length) nextActions.push("WRITE_CONCRETE_SALES_NEXT_ACTION");
  if (recommendedCommercial === COMMERCIAL_CERT.QUARANTINED) {
    nextActions.push("KEEP_QUARANTINED_OUT_OF_ACTIONABLE_LIST");
  }
  if (recommendedCommercial === COMMERCIAL_CERT.LINKED_RESEARCH) {
    nextActions.push("PRESENT_UNDER_PRIMARY_EVENT_FAMILY_ONLY");
  }

  return {
    version: GDI_OPPORTUNITY_QUALITY_CONTRACT_V1,
    id,
    historical: ctx.historical === true,
    familyRole: familyRole || "STANDALONE",
    familyId: ctx.familyId || null,
    researchIntegrity,
    commercialCertification: recommendedCommercial,
    axes,
    blockers: allBlockers,
    nextResearchActions: [...new Set(nextActions)],
    contactCoverage: contact.coverage,
    namedRelevantPrimary: namedOk,
    hasDirectPersonEmail: contact.hasDirectEmail === true,
    hasOrgRoute: contact.hasOrgRoute === true,
    titleOk,
    categoryOk: !categoryBad,
    descriptionOk: descBlockers.length === 0,
    eligibleForCustomerPublicationReview: pubOk,
    engines: {
      salesReady: sr,
      accountDevelopment: ad,
      demandThesis: thesis,
      customerReady,
    },
    // Never auto-mutate product status from this evaluation alone.
    productStatusMutationAuthorized: false,
    opportunitiesJsonWriteAuthorized: false,
  };
}

/**
 * Batch-evaluate opportunities with optional family role map.
 */
export function evaluateOpportunityQualityBatch(opportunities = [], opts = {}) {
  const familyById = opts.familyById instanceof Map ? opts.familyById : new Map();
  const historicalIds = new Set(opts.historicalIds || []);
  const rows = [];
  for (const opp of opportunities || []) {
    const id = text(opp?.id || opp?.opportunityId);
    const fam = familyById.get(id) || {};
    rows.push(
      evaluateOpportunityQualityContract(opp, {
        historical: historicalIds.has(id),
        familyRole: fam.role || fam.familyRole || null,
        familyId: fam.familyId || null,
      })
    );
  }
  const summary = {
    version: GDI_OPPORTUNITY_QUALITY_CONTRACT_V1,
    count: rows.length,
    researchIntegrity: {},
    commercialCertification: {},
    namedRelevantPrimary: rows.filter((r) => r.namedRelevantPrimary).length,
    directPersonEmails: rows.filter((r) => r.hasDirectPersonEmail).length,
    orgRouteCoverage: rows.filter((r) => r.hasOrgRoute).length,
    titleOk: rows.filter((r) => r.titleOk).length,
    categoryOk: rows.filter((r) => r.categoryOk).length,
    descriptionOk: rows.filter((r) => r.descriptionOk).length,
    eligibleForCustomerPublicationReview: rows.filter(
      (r) => r.eligibleForCustomerPublicationReview
    ).length,
  };
  for (const r of rows) {
    summary.researchIntegrity[r.researchIntegrity] =
      (summary.researchIntegrity[r.researchIntegrity] || 0) + 1;
    summary.commercialCertification[r.commercialCertification] =
      (summary.commercialCertification[r.commercialCertification] || 0) + 1;
  }
  return { summary, rows };
}

/**
 * Lifecycle quality gate for finalize / refresh / publication review.
 * Fail-closed for customer publication; research continuation always allowed.
 */
export function evaluateOpportunityQualityGate(opportunity = {}, ctx = {}) {
  const evaluation = evaluateOpportunityQualityContract(opportunity, ctx);
  const blockPublication =
    evaluation.eligibleForCustomerPublicationReview !== true ||
    evaluation.commercialCertification === COMMERCIAL_CERT.QUARANTINED ||
    evaluation.researchIntegrity === RESEARCH_INTEGRITY.QUARANTINED ||
    evaluation.researchIntegrity === RESEARCH_INTEGRITY.FAIL;

  return {
    version: GDI_OPPORTUNITY_QUALITY_CONTRACT_V1,
    okForResearchContinuation: true,
    okForCustomerPublication: !blockPublication,
    mustRemainResearchContinuation:
      evaluation.commercialCertification === COMMERCIAL_CERT.RESEARCH_CONTINUATION ||
      evaluation.commercialCertification === COMMERCIAL_CERT.LINKED_RESEARCH,
    evaluation,
  };
}
