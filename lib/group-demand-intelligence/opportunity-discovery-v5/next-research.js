/**
 * Next-best research + Jev advisory AFTER isGdiResearchLeadWorthPursuing() = true.
 * Jev does not control admission, truth, promotion, or thresholds.
 */

import { getGdiNextBestResearchAction } from "../discovery-expansion-v3/next-best-research.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { validateResearchLeadPage, applyPageEvidenceToLead } from "./page-validation.js";

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

/**
 * Deterministic next blocker for an admitted research lead.
 */
export function nextBlockerForLead(lead = {}) {
  if (!lead.eventStartDate && !lead.eventYear && !lead.futureCycleEvidenceState) {
    return {
      blocker: "FUTURE_CYCLE",
      question: "What is the next confirmed or expected cycle date / decision window?",
      worthOneMoreStep: true,
    };
  }
  if (!lead.lodgingEvidence) {
    return {
      blocker: "ROOM_BLOCK_OR_HOUSING",
      question: "Is there an official housing page, room block, or accommodation RFP?",
      worthOneMoreStep: true,
    };
  }
  if (!lead.buyerEntity && !lead.organizationContactUrl && !lead.functionalContactEmail) {
    return {
      blocker: "BUYER_OR_ORGANIZER",
      question: "Who buys or controls the hotel rooms (entity/function)?",
      worthOneMoreStep: true,
    };
  }
  if (!lead.agency && /association|congress|pharma|medical/i.test(`${lead.title} ${lead.organizationName}`)) {
    return {
      blocker: "AGENCY_OR_HOUSING_PARTNER",
      question: "Is there a named agency, DMC, or housing partner?",
      worthOneMoreStep: true,
    };
  }
  return {
    blocker: "NONE_HIGH_VALUE",
    question: null,
    worthOneMoreStep: false,
  };
}

/**
 * Jev advisory wrapper — recommendation only; never writes verified facts itself.
 */
export function jevAdviseResearchLead(lead = {}, ctx = {}) {
  const admitted = ctx.admitted === true;
  if (!admitted) {
    return {
      issued: false,
      reason: "NOT_ADMITTED_RESEARCH_LEAD",
      stopContinue: "STOP",
      note: "Jev must not advise before deterministic admission.",
    };
  }

  const blocker = nextBlockerForLead(lead);
  const nbr = getGdiNextBestResearchAction(lead, {
    marketPlace: (ctx.placeNames || [])[0],
  });

  if (!blocker.worthOneMoreStep && ctx.stepsTaken >= 2) {
    return {
      issued: true,
      stopContinue: "STOP",
      nextBlocker: blocker.blocker,
      nextSource: null,
      exactQuestion: null,
      oneMoreStepWorthwhile: false,
      note: "Low information-gain after two targeted steps.",
      wroteFacts: false,
      promoted: false,
    };
  }

  return {
    issued: true,
    stopContinue: blocker.worthOneMoreStep && (ctx.stepsTaken || 0) < 2 ? "CONTINUE" : "STOP",
    nextBlocker: blocker.blocker || nbr.blocker,
    nextSource: nbr.query || null,
    exactQuestion: blocker.question || nbr.exactQuestion,
    oneMoreStepWorthwhile: blocker.worthOneMoreStep && (ctx.stepsTaken || 0) < 2,
    note: "Advisory only — deterministic extractors apply any facts from fetched pages.",
    wroteFacts: false,
    promoted: false,
  };
}

/**
 * Run up to 2 targeted research steps on an admitted lead.
 * Third step only when one high-value blocker remains + predicted gain.
 */
export async function runTargetedResearchSteps(lead = {}, hotel = {}, budget = {}, opts = {}) {
  const maxSteps = opts.maxSteps ?? 2;
  const log = [];
  let current = { ...lead };
  let steps = 0;
  let blockerResolved = false;

  while (steps < maxSteps) {
    const advice = jevAdviseResearchLead(current, {
      admitted: true,
      stepsTaken: steps,
      placeNames: hotel.placeNames,
    });
    log.push({
      opportunityId: current.id,
      hotelKey: hotel.hotelKey,
      step: steps + 1,
      ...advice,
    });

    if (!advice.oneMoreStepWorthwhile || advice.stopContinue === "STOP") break;
    if (!hasSerp() || budget.queriesLeft <= 0) break;

    const action = getGdiNextBestResearchAction(current, {
      marketPlace: (hotel.placeNames || [])[0],
    });
    if (!action.query) break;

    budget.queriesLeft -= 1;
    budget.queriesRun += 1;
    let hit = null;
    try {
      const serp = await serpapiSearch({
        engine: "google",
        q: action.query,
        num: 3,
        hl: hotel.serpHlPrimary || "en",
        gl: hotel.serpGl || "us",
      });
      budget.costUsd += 0.05;
      hit = (serp?.data?.organic_results || [])[0] || null;
    } catch (err) {
      budget.errors.push(String(err?.message || err).slice(0, 160));
      budget.costUsd += 0.05;
      break;
    }

    if (!hit?.link) {
      steps += 1;
      continue;
    }

    const pageEv = await validateResearchLeadPage(
      { ...current, officialSource: hit.link },
      {}
    );
    budget.costUsd += 0.01; // page fetch approx
    const before = { ...current };
    current = applyPageEvidenceToLead(
      {
        ...current,
        sources: [
          ...(current.sources || []),
          { url: hit.link, kind: "targeted_research_v5", blocker: action.blocker },
        ],
      },
      pageEv
    );

    const resolved =
      (action.blocker === "LODGING" && current.lodgingEvidence && !before.lodgingEvidence) ||
      (action.blocker === "TIMING" &&
        (current.eventStartDate || current.eventYear) &&
        !(before.eventStartDate || before.eventYear)) ||
      ((action.blocker === "WHO" || action.blocker === "CONTACT_PATH") &&
        (current.organizationContactUrl || current.functionalContactEmail) &&
        !(before.organizationContactUrl || before.functionalContactEmail));

    if (resolved) blockerResolved = true;
    steps += 1;

    // Optional 3rd step
    if (steps >= maxSteps) {
      const still = nextBlockerForLead(current);
      if (
        still.worthOneMoreStep &&
        still.blocker !== "NONE_HIGH_VALUE" &&
        opts.allowThirdStep === true &&
        budget.queriesLeft > 0
      ) {
        // one more only
        continue;
      }
      break;
    }
  }

  // Cap at 2 unless allowThirdStep
  return {
    lead: current,
    stepsTaken: steps,
    jevLog: log,
    blockerResolved,
  };
}
