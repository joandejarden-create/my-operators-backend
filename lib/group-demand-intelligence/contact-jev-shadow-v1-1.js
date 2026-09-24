/**
 * GDI Contact Intelligence V1.1 — Jev SHADOW routing for contact research.
 *
 * Advisory only. Never changes production contact recovery path.
 * Does not declare person identity. Does not send full page content.
 */

import { decide } from "./jev/jev-decision-service.js";
import { JEV_DECISION_TYPE, CHOICES, isValidChoice } from "./jev/jev-types.js";
import {
  CONTACT_SOURCE_PATH,
  CONTACT_FOLLOWUP_TYPE,
  NAMED_PERSON_WORTH,
  FUNCTIONAL_SUFFICIENCY,
  STOP_CONTACT_RESEARCH,
  computeGdiContactRouting,
} from "./contact-source-recovery-v1-1.js";

/** Contact-specific decision types (also registered on JEV_DECISION_TYPE). */
export const CONTACT_JEV_DECISION = Object.freeze({
  CONTACT_SOURCE_PATH: "CONTACT_SOURCE_PATH",
  CONTACT_FOLLOWUP_TYPE: "CONTACT_FOLLOWUP_TYPE",
  NAMED_PERSON_WORTH_PURSUING: "NAMED_PERSON_WORTH_PURSUING",
  FUNCTIONAL_PATH_SUFFICIENT: "FUNCTIONAL_PATH_SUFFICIENT",
  STOP_CONTACT_RESEARCH: "STOP_CONTACT_RESEARCH",
});

/**
 * Structured context for Jev — no page bodies, no PII beyond role labels.
 */
export function buildContactJevContext(opportunity = {}, domainState = {}, gdiRoute = null) {
  const route = gdiRoute || computeGdiContactRouting(opportunity, domainState);
  return {
    opportunityType: opportunity.opportunityType || null,
    organizationType: route.orgFamily,
    programType: opportunity.demandFamily || opportunity.programType || null,
    currentContactTier: route.currentTier,
    knownOfficialDomain: domainState.host || opportunity.officialDomain || null,
    domainConfidence: domainState.confidence || null,
    sourceTypesChecked: (opportunity.sources || [])
      .map((s) => s.sourceType || s.type)
      .filter(Boolean)
      .slice(0, 8),
    rolesFound: [
      opportunity.primaryContactRole || opportunity.primaryContact?.role,
    ]
      .filter(Boolean)
      .slice(0, 5),
    functionalPathFound: route.currentTier === "FUNCTIONAL_CONTACT",
    missingContactFields: [
      !(opportunity.primaryContactName || opportunity.primaryContact?.name) && "name",
      !(opportunity.primaryContactEmail || opportunity.primaryContact?.email) && "email",
      !(opportunity.primaryContactPhone || opportunity.primaryContact?.phone) && "phone",
      !opportunity.officialSource && "officialSource",
    ].filter(Boolean),
    opportunityPriority: opportunity.priority || null,
    actionability: opportunity.bookingWindowStatus || opportunity.opportunityQualification || null,
    publicSourceStrength: domainState.confidence || "UNKNOWN",
    evidenceSummary: [
      `tier=${route.currentTier}`,
      `family=${route.orgFamily}`,
      `domain=${domainState.confidence || "none"}`,
      `gdiSourcePath=${route.sourcePath}`,
      `gdiFollowup=${route.followupType}`,
      `gdiStop=${route.stopContactResearch}`,
    ].join("; "),
    decisionHint: route.sourcePath,
  };
}

/**
 * Run all contact routing decision types in SHADOW.
 * Production path remains `gdiRoute` regardless of Jev output.
 */
export async function evaluateContactJevShadow({
  opportunity,
  domainState,
  gdiRoute,
  forceShadow = true,
} = {}) {
  const route = gdiRoute || computeGdiContactRouting(opportunity, domainState);
  const context = buildContactJevContext(opportunity, domainState, route);

  const specs = [
    {
      type: CONTACT_JEV_DECISION.CONTACT_SOURCE_PATH,
      existing: route.sourcePath,
      fallback: route.sourcePath,
    },
    {
      type: CONTACT_JEV_DECISION.CONTACT_FOLLOWUP_TYPE,
      existing: route.followupType,
      fallback: route.followupType,
    },
    {
      type: CONTACT_JEV_DECISION.NAMED_PERSON_WORTH_PURSUING,
      existing: route.namedPersonWorthPursuing,
      fallback: route.namedPersonWorthPursuing,
    },
    {
      type: CONTACT_JEV_DECISION.FUNCTIONAL_PATH_SUFFICIENT,
      existing: route.functionalPathSufficient,
      fallback: route.functionalPathSufficient,
    },
    {
      type: CONTACT_JEV_DECISION.STOP_CONTACT_RESEARCH,
      existing: route.stopContactResearch,
      fallback: route.stopContactResearch,
    },
  ];

  const decisions = [];
  let techFallbacks = 0;
  let policyFallbacks = 0;
  let calls = 0;

  for (const spec of specs) {
    calls += 1;
    const result = await decide({
      decisionType: spec.type,
      context,
      existingDecision: spec.existing,
      policyContext: {
        // Hard rules: never invent people; Surfe not in play here
        noValidSource: !domainState?.host && !opportunity.officialSource,
      },
      forceShadow,
      fallbackDecision: spec.fallback,
    });

    if (result.technicalFallback) techFallbacks += 1;
    if (result.policyFallback || result.lowConfidenceFallback) policyFallbacks += 1;

    const jevSelected = result.selected || result.effectiveDecision || spec.fallback;
    const agreement = String(jevSelected) === String(spec.existing);

    decisions.push({
      decisionType: spec.type,
      currentGdiRoute: spec.existing,
      jevRoute: jevSelected,
      jevConfidence: result.confidence ?? null,
      agreement,
      shadow: true,
      technicalFallback: Boolean(result.technicalFallback),
      policyFallback: Boolean(result.policyFallback || result.lowConfidenceFallback),
      decisionId: result.decisionId || null,
      evaluatedAt: new Date().toISOString(),
      // Never attach raw input/response
    });
  }

  return {
    shadow: true,
    calls,
    techFallbacks,
    policyFallbacks,
    decisions,
    gdiRoute: route,
    // Production always uses gdiRoute
    productionRoute: route,
  };
}

/**
 * After recovery, score whether Jev's suggested source path would have been better.
 * Heuristic: if Jev disagreed and actual recovery found named/functional via a path
 * matching Jev's suggestion, count as Jev-better; if GDI path found it, GDI-better.
 */
export function scoreJevRoutingOutcome({
  shadowEval,
  recoveryResult,
  actualSourceTypes = [],
} = {}) {
  const out = {
    CONTACT_SOURCE_PATH: { n: 0, jevBetter: 0, gdiBetter: 0, same: 0, unknown: 0, highConfWrong: 0 },
    CONTACT_FOLLOWUP_TYPE: { n: 0, jevBetter: 0, gdiBetter: 0, same: 0, unknown: 0, highConfWrong: 0 },
    NAMED_PERSON_WORTH_PURSUING: { n: 0, results: [] },
    FUNCTIONAL_PATH_SUFFICIENT: { n: 0, results: [] },
    STOP_CONTACT_RESEARCH: { n: 0, results: [] },
  };

  for (const d of shadowEval?.decisions || []) {
    if (d.decisionType === CONTACT_JEV_DECISION.CONTACT_SOURCE_PATH) {
      const bucket = out.CONTACT_SOURCE_PATH;
      bucket.n += 1;
      if (d.agreement) bucket.same += 1;
      else if (d.technicalFallback) bucket.unknown += 1;
      else {
        const jevMatched = actualSourceTypes.some((t) => sourceTypeMatchesPath(t, d.jevRoute));
        const gdiMatched = actualSourceTypes.some((t) =>
          sourceTypeMatchesPath(t, d.currentGdiRoute)
        );
        const improved =
          recoveryResult === "NAMED_CONTACT_FOUND" ||
          recoveryResult === "FUNCTIONAL_CONTACT_FOUND" ||
          recoveryResult === "BETTER_SOURCE_FOUND";
        if (improved && jevMatched && !gdiMatched) bucket.jevBetter += 1;
        else if (improved && gdiMatched) bucket.gdiBetter += 1;
        else if (d.jevConfidence != null && d.jevConfidence >= 0.75 && !improved) {
          bucket.highConfWrong += 1;
        } else bucket.unknown += 1;
      }
    } else if (d.decisionType === CONTACT_JEV_DECISION.CONTACT_FOLLOWUP_TYPE) {
      const bucket = out.CONTACT_FOLLOWUP_TYPE;
      bucket.n += 1;
      if (d.agreement) bucket.same += 1;
      else if (d.technicalFallback) bucket.unknown += 1;
      else bucket.unknown += 1;
    } else if (out[d.decisionType]) {
      out[d.decisionType].n += 1;
      out[d.decisionType].results.push({
        gdi: d.currentGdiRoute,
        jev: d.jevRoute,
        agreement: d.agreement,
      });
    }
  }

  return out;
}

function sourceTypeMatchesPath(sourceType, path) {
  const s = String(sourceType || "").toUpperCase();
  const p = String(path || "").toUpperCase();
  if (p === "OFFICIAL_STAFF") return /STAFF|DIRECTORY|LEADERSHIP/.test(s);
  if (p === "HOUSING") return /HOUSING/.test(s);
  if (p === "REGISTRATION") return /REGISTRATION/.test(s);
  if (p === "VENUE_SALES") return /VENUE/.test(s);
  if (p === "DEPARTMENT_PAGE") return /DEPARTMENT|ALUMNI/.test(s);
  if (p === "EVENT_PROGRAM_PAGE") return /EVENT|PROGRAM/.test(s);
  if (p === "PDF_PROSPECTUS") return /PDF/.test(s);
  if (p === "GENERAL_OFFICIAL_CONTACT") return /CONTACT|GENERAL/.test(s);
  return false;
}

// Re-export choice catalogs for tests
export const CONTACT_JEV_CHOICES = Object.freeze({
  CONTACT_SOURCE_PATH: Object.values(CONTACT_SOURCE_PATH),
  CONTACT_FOLLOWUP_TYPE: Object.values(CONTACT_FOLLOWUP_TYPE),
  NAMED_PERSON_WORTH_PURSUING: Object.values(NAMED_PERSON_WORTH),
  FUNCTIONAL_PATH_SUFFICIENT: Object.values(FUNCTIONAL_SUFFICIENCY),
  STOP_CONTACT_RESEARCH: Object.values(STOP_CONTACT_RESEARCH),
});

export { isValidChoice, CHOICES, JEV_DECISION_TYPE };
