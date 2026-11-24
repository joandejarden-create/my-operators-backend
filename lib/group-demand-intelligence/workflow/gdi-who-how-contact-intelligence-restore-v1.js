/**
 * Restore existing GDI WHO + HOW contact intelligence into canonical Full E2E.
 *
 * DO NOT replace historical modules — connect them:
 *   WHO  → person ID / verification / ranking / primary+secondary
 *   HOW  → public-first email / phone / LinkedIn channel discovery
 *
 * Separate completeness gates (never conflate):
 *   WHO_COMPLETE | WHO_SECONDARY_COMPLETE | HOW_COMPLETE | SALES_READY
 *
 * Findymail / Surfe remain optional and off by default.
 */

import {
  evaluateNamedContactCoverageStandard,
  SECONDARY_CONTACT_RESEARCH_STATUS,
} from "./gdi-named-contact-coverage-standard-v1.js";
import { runPublicContactDiscoveryWorkflow } from "../presentation/gdi-public-contact-discovery-workflow-v1.js";
import { assessPublicContactVerification } from "../presentation/gdi-public-contact-verification-v1.js";
import { PUBLIC_CONTACT_RESEARCH_STATUS } from "../presentation/gdi-public-contact-discovery-v1.js";

export const GDI_WHO_HOW_CONTACT_INTELLIGENCE_RESTORE_V1 =
  "gdi-who-how-contact-intelligence-restore-v1";

/** Canonical Full E2E stage order for contact intelligence (after deep research). */
export const WHO_HOW_FULL_E2E_SEQUENCE = Object.freeze([
  "DEMAND_DISCOVERY",
  "DEEP_RESEARCH",
  "WHO_IDENTIFICATION",
  "PERSON_VERIFICATION",
  "BUYER_ALTERNATIVE_PERSON_RESEARCH",
  "COMMERCIAL_CONTACT_RANKING",
  "SECONDARY_PERSON_RESEARCH",
  "HOW_CONTACT_CHANNEL_DISCOVERY",
  "CONTACT_VERIFICATION",
  "COMMERCIAL_QUALIFICATION",
  "FINALIZED_STAGED",
]);

export const WHO_HOW_REQUIRED_WORKFLOWS = Object.freeze([
  "new_hotel_full_e2e",
  "existing_hotel_full_e2e",
  "portfolio_full_e2e",
  "leisure_demand_full_e2e",
  "resume_full_e2e",
  "replay_full_e2e",
]);

export const WHO_COMPLETENESS = Object.freeze({
  WHO_COMPLETE: "WHO_COMPLETE",
  WHO_INCOMPLETE: "WHO_INCOMPLETE",
});

export const WHO_SECONDARY_COMPLETENESS = Object.freeze({
  WHO_SECONDARY_COMPLETE: "WHO_SECONDARY_COMPLETE",
  WHO_SECONDARY_INCOMPLETE: "WHO_SECONDARY_INCOMPLETE",
});

export const HOW_COMPLETENESS = Object.freeze({
  HOW_COMPLETE: "HOW_COMPLETE",
  HOW_INCOMPLETE: "HOW_INCOMPLETE",
  HOW_SKIPPED_WHO_INCOMPLETE: "HOW_SKIPPED_WHO_INCOMPLETE",
});

export const HOW_CONTACT_RESEARCH_STATUS = Object.freeze({
  CHANNELS_VERIFIED: "HOW_CHANNELS_VERIFIED",
  PARTIAL_CHANNELS: "HOW_PARTIAL_CHANNELS",
  ATTEMPTED_NO_CHANNEL: "HOW_ATTEMPTED_NO_CHANNEL",
  NOT_ATTEMPTED: "HOW_NOT_ATTEMPTED",
  SKIPPED_WHO_INCOMPLETE: "HOW_SKIPPED_WHO_INCOMPLETE",
});

/**
 * Historical function inventory — recovery map (not replacements).
 * status: active | wired_here | bypassed_prior | optional_paid_off
 */
export const WHO_HOW_FUNCTION_INVENTORY = Object.freeze({
  who: Object.freeze([
    {
      fn: "ensureBuyerWhoResearchExecuted",
      path: "research/autonomous-deep-research-recovery-v1.js",
      role: "Buyer-WHO identification after deep research",
      status: "active",
    },
    {
      fn: "runRestoredContactInvestigationPassAsync",
      path: "workflow/gdi-original-full-e2e-execution-restore-v1.js",
      role: "Classify people, official-source follow-up, secondary attempt",
      status: "active",
    },
    {
      fn: "evaluateNamedContactCoverageStandard",
      path: "workflow/gdi-named-contact-coverage-standard-v1.js",
      role: "Min-1 / target-2 named coverage + finalization disposition",
      status: "active",
    },
    {
      fn: "discover-who-v9 / who-discovery-apply",
      path: "contact-candidate/native-who-v3/*",
      role: "Native WHO discovery + apply",
      status: "active_library",
    },
    {
      fn: "opportunity-who-resolution-v1",
      path: "opportunity-who-resolution-v1.js",
      role: "Opportunity WHO resolution",
      status: "active_library",
    },
    {
      fn: "gdi-opportunity-contact-lineage-reuse-v1",
      path: "workflow/gdi-opportunity-contact-lineage-reuse-v1.js",
      role: "Identity reuse across opportunities",
      status: "active_library",
    },
    {
      fn: "secondary-named-contact-v1 / scaling-two-contact-standard-v1",
      path: "secondary-named-contact-v1.js",
      role: "Primary/secondary ranking presentation",
      status: "active_library",
    },
  ]),
  how: Object.freeze([
    {
      fn: "runPublicContactDiscoveryWorkflow",
      path: "presentation/gdi-public-contact-discovery-workflow-v1.js",
      role: "Public-first HOW after named person (no Surfe/FullEnrich auto)",
      status: "wired_here",
      priorBypass:
        "original-full-e2e-research-chain ran CONTACT_RESEARCH (WHO) but never invoked HOW workflow",
    },
    {
      fn: "discoverPublicContactChannels",
      path: "presentation/gdi-public-contact-discovery-v1.js",
      role: "Public channel discovery from URLs + optional paid search",
      status: "active_via_workflow",
    },
    {
      fn: "recoverPublicContactChannels",
      path: "presentation/gdi-public-contact-channel-hierarchy-v1.js",
      role: "Extract email/phone/LinkedIn from authoritative pages",
      status: "active_via_workflow",
    },
    {
      fn: "assessPublicContactVerification",
      path: "presentation/gdi-public-contact-verification-v1.js",
      role: "Verify channel ownership + status",
      status: "active",
    },
    {
      fn: "splitWhoHow",
      path: "contact-completeness-v1.js",
      role: "WHO/HOW field split helper",
      status: "active",
    },
    {
      fn: "buyer-contact-enrichment-v1",
      path: "workflow/buyer-contact-enrichment-v1.js",
      role: "Optional Surfe AFTER public HOW exhausted",
      status: "optional_paid_off",
    },
    {
      fn: "Findymail / Surfe / FullEnrich providers",
      path: "gdi-surfe-contact-provider-v1.js / gdi-fullenrich-contact-provider-v1.js",
      role: "Paid enrichment — never WHO engine; on-demand only",
      status: "optional_paid_off",
    },
  ]),
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function now() {
  return new Date().toISOString();
}

function hasLinkedIn(opportunity = {}) {
  const c = opportunity.primaryContact || {};
  const url = text(c.linkedinUrl || c.linkedIn || opportunity.linkedinUrl);
  return /^https?:\/\/([a-z0-9-]+\.)?linkedin\.com\//i.test(url) ? url : null;
}

/**
 * Assess separate WHO / HOW / Sales Ready gates for one opportunity.
 * Missing email never makes WHO incomplete.
 */
export function assessWhoHowCompletenessGates(opportunity = {}, opts = {}) {
  const coverage = evaluateNamedContactCoverageStandard(opportunity, opts);
  const whoComplete = coverage.namedCount >= 1;
  const whoSecondaryComplete = coverage.namedCount >= 2;

  const verification = assessPublicContactVerification(opportunity);
  const linkedin = hasLinkedIn(opportunity);
  const channelFound = Boolean(
    verification.hasVerifiedDirectEmail ||
      verification.hasVerifiedDirectPhone ||
      verification.hasVerifiedOrgEmailRoute ||
      verification.hasVerifiedOrgPhoneRoute ||
      linkedin,
  );

  const howStatusRaw = text(
    opportunity.howContactResearchStatus ||
      opportunity.publicContactResearchStatus ||
      "",
  );
  const explicitNotAttempted =
    howStatusRaw === HOW_CONTACT_RESEARCH_STATUS.NOT_ATTEMPTED ||
    howStatusRaw === "NOT_ATTEMPTED" ||
    opportunity.howContactResearchAttempted === false;
  const howAttempted =
    !explicitNotAttempted &&
    (opportunity.howContactResearchAttempted === true ||
      howStatusRaw === HOW_CONTACT_RESEARCH_STATUS.CHANNELS_VERIFIED ||
      howStatusRaw === HOW_CONTACT_RESEARCH_STATUS.PARTIAL_CHANNELS ||
      howStatusRaw === HOW_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_CHANNEL ||
      /PUBLIC_CHANNELS_VERIFIED|PARTIAL_PUBLIC|PUBLIC_SEARCH_EXHAUSTED|REUSED_EXISTING/i.test(
        howStatusRaw,
      ));

  let howResearchStatus = HOW_CONTACT_RESEARCH_STATUS.NOT_ATTEMPTED;
  if (!whoComplete) {
    howResearchStatus = HOW_CONTACT_RESEARCH_STATUS.SKIPPED_WHO_INCOMPLETE;
  } else if (
    verification.hasVerifiedDirectEmail ||
    verification.hasVerifiedDirectPhone ||
    linkedin
  ) {
    howResearchStatus = HOW_CONTACT_RESEARCH_STATUS.CHANNELS_VERIFIED;
  } else if (
    verification.hasVerifiedOrgEmailRoute ||
    verification.hasVerifiedOrgPhoneRoute
  ) {
    howResearchStatus = HOW_CONTACT_RESEARCH_STATUS.PARTIAL_CHANNELS;
  } else if (howAttempted) {
    howResearchStatus = HOW_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_CHANNEL;
  }

  // HOW_COMPLETE = researched to terminal public standard (found OR exhausted attempt).
  // Not finding email does not block WHO; finding a person does not auto-complete HOW.
  const howComplete =
    whoComplete &&
    (howResearchStatus === HOW_CONTACT_RESEARCH_STATUS.CHANNELS_VERIFIED ||
      howResearchStatus === HOW_CONTACT_RESEARCH_STATUS.PARTIAL_CHANNELS ||
      howResearchStatus === HOW_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_CHANNEL);

  const salesReady =
    opportunity.salesReady === true ||
    opportunity.commercialDisposition === "SALES_READY" ||
    opportunity.eligibilityStatus === "SALES_READY";

  return {
    version: GDI_WHO_HOW_CONTACT_INTELLIGENCE_RESTORE_V1,
    who: whoComplete
      ? WHO_COMPLETENESS.WHO_COMPLETE
      : WHO_COMPLETENESS.WHO_INCOMPLETE,
    whoSecondary: whoSecondaryComplete
      ? WHO_SECONDARY_COMPLETENESS.WHO_SECONDARY_COMPLETE
      : WHO_SECONDARY_COMPLETENESS.WHO_SECONDARY_INCOMPLETE,
    how: !whoComplete
      ? HOW_COMPLETENESS.HOW_SKIPPED_WHO_INCOMPLETE
      : howComplete
        ? HOW_COMPLETENESS.HOW_COMPLETE
        : HOW_COMPLETENESS.HOW_INCOMPLETE,
    whoComplete,
    whoSecondaryComplete,
    howComplete,
    howResearchStatus,
    channelFound,
    channels: {
      email:
        verification.hasVerifiedDirectEmail ||
        verification.hasVerifiedOrgEmailRoute ||
        false,
      emailDirect: verification.hasVerifiedDirectEmail === true,
      phone:
        verification.hasVerifiedDirectPhone ||
        verification.hasVerifiedOrgPhoneRoute ||
        false,
      phoneDirect: verification.hasVerifiedDirectPhone === true,
      linkedin: Boolean(linkedin),
      linkedinUrl: linkedin,
    },
    salesReady: salesReady === true,
    namedCount: coverage.namedCount,
    secondaryResearchStatus: coverage.secondaryResearchStatus,
    coverage,
    verification,
  };
}

/**
 * Run existing public-first HOW after WHO. Never activates Surfe/Findymail.
 */
export async function runRestoredHowContactChannelDiscovery(
  opportunity = {},
  opts = {},
) {
  const gatesBefore = assessWhoHowCompletenessGates(opportunity, opts);
  if (!gatesBefore.whoComplete) {
    const next = {
      ...opportunity,
      howContactResearchAttempted: false,
      howContactResearchStatus:
        HOW_CONTACT_RESEARCH_STATUS.SKIPPED_WHO_INCOMPLETE,
      howCompleteness: HOW_COMPLETENESS.HOW_SKIPPED_WHO_INCOMPLETE,
      whoHowCompleteness: {
        ...gatesBefore,
        at: now(),
      },
    };
    return {
      opportunity: next,
      executed: false,
      skipped: true,
      reason: "WHO_INCOMPLETE",
      researchStatus: HOW_CONTACT_RESEARCH_STATUS.SKIPPED_WHO_INCOMPLETE,
      workflow: null,
      providers: {
        findymail: false,
        surfe: false,
        paidPublicSearch: false,
      },
    };
  }

  if (opts.authorizeFindymail === true || opts.authorizeSurfe === true) {
    throw new Error(
      "HOW restore refuses Findymail/Surfe activation — public-first only in this stage",
    );
  }

  const authorizePaid = opts.authorizePaidPublicSearch === true;
  const workflow = await runPublicContactDiscoveryWorkflow({
    opportunity,
    hotelId: opts.hotelId || opportunity.hotelId || null,
    authorizePaidPublicSearch: authorizePaid,
    persist: false,
    founderAuthorizeLiveWrite: false,
    reuseCrossHotelCache: opts.reuseCrossHotelCache !== false,
    searchProvider: opts.searchProvider || null,
    fetchImpl: opts.fetchImpl || null,
    candidateUrls: opts.candidateUrls || null,
    skipDns: opts.skipDns === true,
    maxQueries: authorizePaid ? opts.maxQueries ?? 4 : 0,
    maxPages: opts.maxPages ?? 4,
    effort: opts.effort || undefined,
    env: opts.env,
  });

  const working = { ...(workflow.opportunity || opportunity) };
  const publicStatus = text(workflow.researchStatus);
  let howStatus = HOW_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_CHANNEL;
  if (
    publicStatus === PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_CHANNELS_VERIFIED ||
    publicStatus === PUBLIC_CONTACT_RESEARCH_STATUS.REUSED_EXISTING
  ) {
    howStatus = HOW_CONTACT_RESEARCH_STATUS.CHANNELS_VERIFIED;
  } else if (
    publicStatus === PUBLIC_CONTACT_RESEARCH_STATUS.PARTIAL_PUBLIC_CHANNELS
  ) {
    howStatus = HOW_CONTACT_RESEARCH_STATUS.PARTIAL_CHANNELS;
  } else if (
    publicStatus === PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_RESEARCH_INCOMPLETE &&
    !authorizePaid
  ) {
    // Public-first pass ran (URL recovery + optional cache); document terminal
    // attempt when paid search is intentionally off.
    howStatus = HOW_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_CHANNEL;
  }

  working.howContactResearchAttempted = true;
  working.howContactResearchStatus = howStatus;
  working.publicContactResearchStatus = publicStatus || howStatus;
  working.howContactResearchAt = now();
  working.howContactResearchMethods = workflow.methods || [];
  working.howContactResearchMetrics = workflow.metrics || null;
  working.howContactVerification = workflow.verification || null;
  working.howEnrichmentDecision = workflow.enrichmentDecision || null;

  const gates = assessWhoHowCompletenessGates(working, opts);
  working.howCompleteness = gates.how;
  working.whoCompleteness = gates.who;
  working.whoSecondaryCompleteness = gates.whoSecondary;
  working.whoHowCompleteness = { ...gates, at: now() };

  return {
    opportunity: working,
    executed: true,
    skipped: false,
    reason: howStatus,
    researchStatus: howStatus,
    publicResearchStatus: publicStatus || null,
    workflow,
    providers: {
      findymail: false,
      surfe: false,
      paidPublicSearch: authorizePaid,
      paidPublicSearchUsed: workflow.metrics?.paidPublicSearchUsed === true,
    },
  };
}

/**
 * Assert Full E2E stage list includes WHO then HOW (fail closed).
 */
export function assertWhoHowStagesInFullE2e(stages = [], opts = {}) {
  const list = Array.isArray(stages) ? stages : [];
  const contact = list.find((s) => s?.stage === "CONTACT_RESEARCH");
  const how = list.find((s) => s?.stage === "HOW_CONTACT_CHANNEL_DISCOVERY");
  const buyerWho = list.find((s) => s?.stage === "BUYER_WHO_RESEARCH");

  if (opts.requireBuyerWho !== false && !buyerWho && !contact) {
    return {
      ok: false,
      code: "WHO_STAGE_MISSING",
      reason: "Neither BUYER_WHO_RESEARCH nor CONTACT_RESEARCH recorded",
    };
  }
  if (!how) {
    return {
      ok: false,
      code: "HOW_STAGE_MISSING",
      reason:
        "HOW_CONTACT_CHANNEL_DISCOVERY stage missing — public HOW bypassed",
    };
  }
  if (opts.requireHowExecuted === true && how.executed !== true && how.skipped !== true) {
    return {
      ok: false,
      code: "HOW_NOT_EXECUTED",
      reason: how.reason || "HOW stage present but not executed",
      stage: how,
    };
  }

  const whoIdx = Math.max(
    list.findIndex((s) => s?.stage === "BUYER_WHO_RESEARCH"),
    list.findIndex((s) => s?.stage === "CONTACT_RESEARCH"),
  );
  const howIdx = list.findIndex(
    (s) => s?.stage === "HOW_CONTACT_CHANNEL_DISCOVERY",
  );
  if (whoIdx >= 0 && howIdx >= 0 && howIdx < whoIdx) {
    return {
      ok: false,
      code: "HOW_BEFORE_WHO",
      reason: "HOW must execute after WHO identification",
    };
  }

  return {
    ok: true,
    code: "WHO_HOW_STAGES_OK",
    buyerWhoPresent: Boolean(buyerWho),
    contactResearchPresent: Boolean(contact),
    howStage: how,
  };
}

/**
 * Annotate opportunity from evidence without inventing channels.
 */
export function annotateWhoHowCompletenessFromEvidence(opportunity = {}) {
  const gates = assessWhoHowCompletenessGates(opportunity);
  return {
    ...opportunity,
    whoCompleteness: gates.who,
    whoSecondaryCompleteness: gates.whoSecondary,
    howCompleteness: gates.how,
    howContactResearchStatus:
      opportunity.howContactResearchStatus || gates.howResearchStatus,
    whoHowCompleteness: { ...gates, at: now() },
  };
}

export default {
  GDI_WHO_HOW_CONTACT_INTELLIGENCE_RESTORE_V1,
  WHO_HOW_FULL_E2E_SEQUENCE,
  WHO_HOW_REQUIRED_WORKFLOWS,
  WHO_COMPLETENESS,
  WHO_SECONDARY_COMPLETENESS,
  HOW_COMPLETENESS,
  HOW_CONTACT_RESEARCH_STATUS,
  WHO_HOW_FUNCTION_INVENTORY,
  assessWhoHowCompletenessGates,
  runRestoredHowContactChannelDiscovery,
  assertWhoHowStagesInFullE2e,
  annotateWhoHowCompletenessFromEvidence,
};
