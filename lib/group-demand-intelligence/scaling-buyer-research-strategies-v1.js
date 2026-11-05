/**
 * Universal named-buyer research continuation strategies for Scaling Corporate Accounts.
 * Used when activity + demand exist but ONE_NAMED_RELEVANT_BUYER (or attribution) fails.
 *
 * Research-led model: do not treat a single failed search as exhausted research.
 * Do not invent contacts. Prefer official bios → press leadership → workplace/facilities
 * → regional ops → HR/people → org ask-for route attached to a verified named person.
 */

export const SCALING_BUYER_RESEARCH_STRATEGIES_V1 =
  "scaling-buyer-research-strategies-v1";

/** Ordered strategies — advance index after each unsuccessful authorized attempt. */
export const BUYER_RESEARCH_STRATEGY_SEQUENCE = Object.freeze([
  {
    id: "OFFICIAL_LEADERSHIP_PAGES",
    label: "Official company leadership / team / about pages",
    queryHints: [
      "leadership",
      "executive team",
      "our team",
      "office managing partner",
      "regional director",
    ],
  },
  {
    id: "MEETINGS_INCENTIVES_SALES_OPS",
    label:
      "Meetings / incentives / sales operations / recognition program leadership (destination + city)",
    queryHints: [
      "meetings and incentives",
      "incentive travel",
      "sales operations",
      "sales enablement",
      "presidents club",
      "employee recognition",
      "eventos corporativos",
      "kickoff comercial",
      "director comercial",
    ],
  },
  {
    id: "WORKPLACE_FACILITIES_REAL_ESTATE",
    label: "Workplace Experience / Facilities / Real Estate / CRE leadership",
    queryHints: [
      "workplace experience",
      "facilities director",
      "corporate real estate",
      "office services",
      "head of facilities",
    ],
  },
  {
    id: "PEOPLE_HR_TRAINING",
    label: "People / HR / Talent / Learning & Development leadership",
    queryHints: [
      "chief people officer",
      "vp people",
      "director human resources",
      "learning and development",
      "office manager",
      "capacitación corporativa",
    ],
  },
  {
    id: "REGIONAL_OPS_EXPANSION",
    label: "Regional operations / market expansion / office managing leadership",
    queryHints: [
      "managing partner",
      "office managing partner",
      "vp operations",
      "market expansion",
      "regional operations",
    ],
  },
  {
    id: "PROCUREMENT_ADMIN_EA",
    label: "Procurement / administrative / executive assistant routes",
    queryHints: [
      "procurement",
      "strategic sourcing",
      "executive assistant",
      "office administrator",
    ],
  },
  {
    id: "PRESS_NAMED_THEN_VERIFY_AFFILIATION",
    label: "Press-named individuals — verify current employer before attaching",
    queryHints: [
      "press contact",
      "quoted in expansion announcement",
      "media contact",
    ],
    caution:
      "Press quote ≠ employer. Airport/landlord partners must not become company primaries.",
  },
  {
    id: "PUBLIC_PROFESSIONAL_PROFILES",
    label: "Public professional profiles cross-checked to company site",
    queryHints: ["linkedin current company", "title facilities OR workplace"],
    caution: "Confirm affiliation date; do not invent email from name patterns alone.",
  },
  {
    id: "ORG_ROUTE_ASK_FOR_NAMED",
    label: "Attach verified org mailbox / contact URL as ask-for for named person",
    queryHints: ["info@", "contact@", "hello@", "contact us page"],
    caution:
      "Org route alone is not a named buyer; pair with verified person. Generic info@ is NOT person misattribution.",
  },
]);

/**
 * @param {{ gaps?: string[], attempts?: Array<{ strategyId?: string }>, attributionInvalid?: boolean }} input
 * @returns {{ nextStrategy: object|null, remaining: object[], exhausted: boolean, note: string }}
 */
export function selectNextBuyerResearchStrategy(input = {}) {
  const gaps = Array.isArray(input.gaps) ? input.gaps : [];
  const attempts = Array.isArray(input.attempts) ? input.attempts : [];
  const tried = new Set(
    attempts.map((a) => String(a?.strategyId || "").trim()).filter(Boolean),
  );

  if (input.attributionInvalid === true || gaps.includes("INVALID_BUYER_CONTACT_ATTRIBUTION")) {
    const fix = {
      id: "CORRECT_EMPLOYER_EMAIL_ATTRIBUTION",
      label: "Correct employer/email ownership before deeper buyer search",
      queryHints: [
        "verify named person current employer",
        "detach emails belonging to different identities",
        "demote landlord/airport partners to referral only",
      ],
    };
    if (!tried.has(fix.id)) {
      return {
        nextStrategy: fix,
        remaining: BUYER_RESEARCH_STRATEGY_SEQUENCE.filter((s) => !tried.has(s.id)),
        exhausted: false,
        note: "Attribution integrity first — do not publish with wrong employer/email.",
      };
    }
  }

  const remaining = BUYER_RESEARCH_STRATEGY_SEQUENCE.filter((s) => !tried.has(s.id));
  if (!remaining.length) {
    return {
      nextStrategy: null,
      remaining: [],
      exhausted: true,
      note: "Free public strategy sequence exhausted — stage paid research proposal; keep RESEARCH_CONTINUATION; do not silent-withdraw.",
    };
  }
  return {
    nextStrategy: remaining[0],
    remaining: remaining.slice(1),
    exhausted: false,
    note: "Continue research — promising account without verified buyer stays in continuation.",
  };
}

/**
 * Attach next-strategy metadata onto a researchContinuation row (non-mutating helper).
 */
export function enrichResearchContinuationWithBuyerStrategy(row, opts = {}) {
  const picked = selectNextBuyerResearchStrategy({
    gaps: row?.gaps || opts.gaps || [],
    attempts: row?.buyerResearchAttempts || opts.attempts || [],
    attributionInvalid: (row?.gaps || []).includes("INVALID_BUYER_CONTACT_ATTRIBUTION"),
  });
  return {
    ...row,
    buyerResearchContinuation: true,
    nextBuyerResearchStrategy: picked.nextStrategy,
    buyerResearchStrategiesRemaining: picked.remaining.map((s) => s.id),
    buyerResearchExhausted: picked.exhausted,
    buyerResearchNote: picked.note,
    preserveOpportunity: true,
    doNotSilentWithdraw: true,
  };
}
