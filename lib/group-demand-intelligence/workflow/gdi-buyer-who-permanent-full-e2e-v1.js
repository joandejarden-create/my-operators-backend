/**
 * Permanent buyer-WHO Full E2E certification helpers V1.
 *
 * BUYER_WHO_RESEARCH is a mandatory stage of canonical Full E2E whenever
 * deep research runs for qualifying opportunities. This module documents
 * and asserts that contract for new hotels, existing hotels, portfolio,
 * leisure tracks, resume, and replay — without hotel-specific scripts.
 */

export const GDI_BUYER_WHO_PERMANENT_FULL_E2E_V1 =
  "gdi-buyer-who-permanent-full-e2e-v1";

/** Workflow shapes that must surface BUYER_WHO_RESEARCH after deep research. */
export const BUYER_WHO_REQUIRED_WORKFLOWS = Object.freeze([
  "new_hotel_full_e2e",
  "existing_hotel_full_e2e",
  "portfolio_full_e2e",
  "leisure_demand_full_e2e",
  "resume_full_e2e",
  "replay_full_e2e",
]);

/**
 * @param {Array<{ stage?: string, executed?: boolean, reason?: string, liveAuthorized?: boolean, researcherSource?: string }>} stages
 * @param {{ requireExecuted?: boolean, forbidLiveFixtureFallback?: boolean, workflow?: string }} [opts]
 */
export function assertBuyerWhoStagePresent(stages = [], opts = {}) {
  const list = Array.isArray(stages) ? stages : [];
  const who = [...list].reverse().find((s) => s?.stage === "BUYER_WHO_RESEARCH");
  if (!who) {
    return {
      ok: false,
      code: "BUYER_WHO_STAGE_MISSING",
      workflow: opts.workflow || null,
      reason: "BUYER_WHO_RESEARCH stage not recorded after deep research",
    };
  }
  if (opts.requireExecuted === true && who.executed !== true) {
    return {
      ok: false,
      code: "BUYER_WHO_NOT_EXECUTED",
      workflow: opts.workflow || null,
      reason: who.reason || "BUYER_WHO_RESEARCH did not execute",
      stage: who,
    };
  }
  if (
    opts.forbidLiveFixtureFallback === true &&
    who.liveAuthorized === true &&
    String(who.researcherSource || "") === "fixture"
  ) {
    return {
      ok: false,
      code: "LIVE_MODE_FIXTURE_FALLBACK",
      workflow: opts.workflow || null,
      reason: "Live-authorized buyer-WHO silently used fixture researcher",
      stage: who,
    };
  }
  return {
    ok: true,
    code: "BUYER_WHO_STAGE_OK",
    workflow: opts.workflow || null,
    stage: who,
  };
}

/**
 * Fail-closed finalization guard: named-buyer STAGED packages must not
 * promote org-route-only rows when buyer-WHO was required but incomplete.
 */
export function evaluateNamedBuyerFinalizationGate(opportunity = {}, opts = {}) {
  const looksLikePerson =
    typeof opts.looksLikePersonName === "function"
      ? opts.looksLikePersonName
      : (n) => {
          const s = String(n || "").replace(/\s+/g, " ").trim();
          return s.split(/\s+/).filter(Boolean).length >= 2 && s.length >= 5;
        };

  const name = String(
    opportunity.primaryContact?.name ||
      opportunity.contactName ||
      opportunity.namedStakeholders?.[0]?.name ||
      "",
  )
    .replace(/\s+/g, " ")
    .trim();

  const buyerWho = opportunity.buyerWhoFallback || null;
  const buyerWhoIncomplete =
    opportunity.buyerWhoResearchStatus === "INCOMPLETE" ||
    opportunity.buyerWhoRequiredIncomplete === true ||
    (buyerWho &&
      buyerWho.executed === true &&
      !/buyer_who_established|gap_closed|role_relevant/i.test(
        String(buyerWho.reason || ""),
      ) &&
      !looksLikePerson(name));

  if (looksLikePerson(name)) {
    return {
      pass: true,
      mode: "NAMED_PERSON",
      name,
      buyerWhoIncomplete: false,
    };
  }

  if (opts.requireNamedPerson === true || buyerWhoIncomplete) {
    return {
      pass: false,
      mode: "NAMED_BUYER_REQUIRED_INCOMPLETE",
      name: name || null,
      buyerWhoIncomplete: true,
      reason:
        "Buyer-WHO required but no verified named person — refuse silent org-route finalization",
    };
  }

  return {
    pass: false,
    mode: "CONTACT_INCOMPLETE",
    name: name || null,
    buyerWhoIncomplete: Boolean(buyerWhoIncomplete),
    reason: "No verified named buyer",
  };
}

/**
 * Rank candidate stakeholders for best-suggested contact.
 * Prefers role-relevant hotel/travel decision makers over generic executives.
 */
export function selectBestSuggestedBuyer(candidates = [], opts = {}) {
  const looksLikePerson =
    typeof opts.looksLikePersonName === "function"
      ? opts.looksLikePersonName
      : (n) => String(n || "").trim().split(/\s+/).length >= 2;
  const employerOk =
    typeof opts.employerAffiliationPlausible === "function"
      ? opts.employerAffiliationPlausible
      : () => true;
  const requireSourceUrl = opts.requireSourceUrl !== false;
  const organizationName = opts.organizationName || "";

  const scored = (candidates || [])
    .filter((c) => looksLikePerson(c?.name))
    .filter((c) => employerOk(c, organizationName))
    .filter((c) => !requireSourceUrl || (c.sourceUrl && /^https?:\/\//i.test(c.sourceUrl)))
    .map((c) => {
      const title = String(c.title || c.role || "");
      const blob = `${title} ${c.relevanceEvidence || ""} ${c.responsibilityClass || ""}`;
      let score = 0;
      if (/PROBABLE_RELEVANT_BUYER|BOOKING_DECISION|CONFIRMED_RELEVANT/i.test(
        `${c.buyerClassification || ""} ${c.responsibilityClass || ""} ${c.decisionMakerClass || ""}`,
      )) {
        score += 50;
      }
      if (/hotel|housing|lodging|sourcing|procurement|travel|meetings?|events?|venue|room.?block|MICE|DMC|partnership/i.test(blob)) {
        score += 30;
      }
      if (/director|manager|vp|head|lead|coordinator|planner/i.test(blob)) {
        score += 15;
      }
      if (/ceo|founder|intern|assistant|receptionist/i.test(blob)) {
        score -= 10;
      }
      if (c.sourceUrl && /^https?:\/\//i.test(c.sourceUrl)) score += 10;
      if (c.email && /@/.test(c.email)) score += 5;
      return { candidate: c, score };
    })
    .sort((a, b) => b.score - a.score);

  if (!scored.length) return { selected: null, score: 0, rejected: candidates?.length || 0 };
  return {
    selected: scored[0].candidate,
    score: scored[0].score,
    rejected: Math.max(0, (candidates?.length || 0) - 1),
    ranked: scored,
  };
}

/**
 * Apply selected buyer onto opportunity without inventing email.
 */
export function applyVerifiedBuyerToOpportunity(opportunity = {}, buyer = null, opts = {}) {
  if (!buyer?.name) {
    return {
      ...opportunity,
      buyerWhoResearchStatus: "INCOMPLETE",
      buyerWhoRequiredIncomplete: true,
    };
  }
  const role = buyer.title || buyer.role || null;
  const org =
    buyer.currentOrganization ||
    buyer.organization ||
    opportunity.organizationName ||
    null;
  const personIdentityKey =
    opts.personIdentityKey ||
    `pik_${String(buyer.name).toLowerCase().replace(/[^a-z0-9]+/g, "_").slice(0, 48)}_${String(org || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .slice(0, 24)}`;

  const primaryContact = {
    ...(opportunity.primaryContact && typeof opportunity.primaryContact === "object"
      ? opportunity.primaryContact
      : {}),
    name: buyer.name,
    role,
    title: role,
    email: buyer.email || null,
    emailVerificationStatus: buyer.email ? "PUBLIC_CANDIDATE" : "NO_EMAIL",
    phone: buyer.phone || null,
    phoneVerificationStatus: buyer.phone ? "PUBLIC_CANDIDATE" : "NO_PHONE",
    organization: org,
    relevanceEvidence:
      buyer.relevanceEvidence ||
      buyer.responsibilityRationale ||
      (role ? `${role} at ${org || opportunity.organizationName || "organization"}` : null),
    suggestedContactLabel: role
      ? `Suggested contact: ${buyer.name} (${role})`
      : `Suggested contact: ${buyer.name}`,
    sourceUrl: buyer.sourceUrl || null,
    verificationStatus: buyer.verificationStatus || "PUBLIC_SOURCE_VERIFIED",
    confidence: buyer.confidence || (buyer.sourceUrl ? "MEDIUM" : "LOW"),
    personIdentityKey,
    contactDataOrigin: buyer.contactDataOrigin || "PUBLIC_SOURCE",
  };

  const outreach = role
    ? `Research outreach to ${buyer.name} (${role}) at ${org || opportunity.organizationName || "the organization"}. Confirm lodging / meeting decision role, propose property-fit options, and request timing for a short commercial conversation.`
    : `Research outreach to ${buyer.name} at ${org || opportunity.organizationName || "the organization"}. Confirm lodging / meeting decision role.`;

  return {
    ...opportunity,
    primaryContact,
    personIdentityKey,
    contactProvenance: {
      ...(opportunity.contactProvenance || {}),
      source: "BUYER_WHO_RESEARCH",
      sourceUrl: buyer.sourceUrl || null,
      verifiedAt: new Date().toISOString(),
      personIdentityKey,
    },
    recommendedNextStep: opportunity.recommendedNextStep || outreach,
    recommendedAction: opportunity.recommendedAction || outreach,
    recommendedNextAction: opportunity.recommendedNextAction || outreach,
    tileContactPreview: {
      name: buyer.name,
      role,
      organization: org,
    },
    buyerWhoResearchStatus: "COMPLETE",
    buyerWhoRequiredIncomplete: false,
    namedStakeholders: [
      ...(opportunity.namedStakeholders || []),
      buyer,
    ],
  };
}

export default {
  GDI_BUYER_WHO_PERMANENT_FULL_E2E_V1,
  BUYER_WHO_REQUIRED_WORKFLOWS,
  assertBuyerWhoStagePresent,
  evaluateNamedBuyerFinalizationGate,
  selectBestSuggestedBuyer,
  applyVerifiedBuyerToOpportunity,
};
