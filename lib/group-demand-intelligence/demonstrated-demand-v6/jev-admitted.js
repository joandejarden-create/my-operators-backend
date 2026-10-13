/**
 * Jev only on success-pattern admitted demonstrated-demand leads.
 * Depth default: page validation (1) + one targeted step (2). Third only if strong match.
 */

import { getGdiNextBestResearchAction } from "../discovery-expansion-v3/next-best-research.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  validateResearchLeadPage,
  applyPageEvidenceToLead,
} from "../opportunity-discovery-v5/page-validation.js";
import { nextBlockerForLead } from "../opportunity-discovery-v5/next-research.js";
import { SUCCESS_MATCH } from "./success-pattern-admission.js";

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

export const JEV_ACTION = Object.freeze({
  CONTINUE: "CONTINUE",
  SWITCH_SOURCE: "SWITCH_SOURCE",
  SWITCH_BLOCKER: "SWITCH_BLOCKER",
  STOP_LOW_INFORMATION_GAIN: "STOP_LOW_INFORMATION_GAIN",
  STOP_PUBLIC_DATA_CEILING: "STOP_PUBLIC_DATA_CEILING",
  SWITCH_LANGUAGE: "SWITCH_LANGUAGE",
  SWITCH_FEEDER_MARKET: "SWITCH_FEEDER_MARKET",
});

/**
 * Jev advisory — admitted demonstrated-demand leads only.
 */
export function jevAdviseAdmittedLead(lead = {}, ctx = {}) {
  if (ctx.admitted !== true || ctx.demonstratedDemand !== true) {
    return {
      issued: false,
      action: JEV_ACTION.STOP_LOW_INFORMATION_GAIN,
      reason: "NOT_ADMITTED_DEMONSTRATED_DEMAND",
      wroteFacts: false,
      promoted: false,
    };
  }

  const blocker = nextBlockerForLead(lead);
  const nbr = getGdiNextBestResearchAction(lead, {
    marketPlace: (ctx.placeNames || [])[0],
  });
  const depth = ctx.depth || 1;
  const match = ctx.successMatch || SUCCESS_MATCH.NONE;

  if (!blocker.worthOneMoreStep) {
    return {
      issued: true,
      action: JEV_ACTION.STOP_LOW_INFORMATION_GAIN,
      stopContinue: "STOP",
      nextBlocker: blocker.blocker,
      nextSource: null,
      exactQuestion: null,
      oneMoreStepWorthwhile: false,
      wroteFacts: false,
      promoted: false,
      note: "No high-value blocker remains.",
    };
  }

  // Depth-3 only when STRONG success match + one material blocker
  if (depth >= 2) {
    const allowThird =
      match === SUCCESS_MATCH.STRONG &&
      blocker.worthOneMoreStep &&
      blocker.blocker !== "NONE_HIGH_VALUE";
    if (!allowThird) {
      return {
        issued: true,
        action: JEV_ACTION.STOP_LOW_INFORMATION_GAIN,
        stopContinue: "STOP",
        nextBlocker: blocker.blocker,
        nextSource: nbr.query || null,
        exactQuestion: blocker.question,
        oneMoreStepWorthwhile: false,
        wroteFacts: false,
        promoted: false,
        note: "Depth-3 disabled unless STRONG success-pattern match.",
      };
    }
  }

  let action = JEV_ACTION.CONTINUE;
  if (ctx.priorBlocker && ctx.priorBlocker !== blocker.blocker) {
    action = JEV_ACTION.SWITCH_BLOCKER;
  } else if (nbr.query && depth >= 1) {
    action = JEV_ACTION.SWITCH_SOURCE;
  }

  return {
    issued: true,
    action,
    stopContinue: "CONTINUE",
    nextBlocker: blocker.blocker || nbr.blocker,
    nextSource: nbr.query || null,
    exactQuestion: blocker.question || nbr.exactQuestion,
    oneMoreStepWorthwhile: true,
    wroteFacts: false,
    promoted: false,
    note: "Advisory only — deterministic extractors apply page facts.",
  };
}

/**
 * Run bounded research on admitted lead. Default max depth 2.
 */
export async function researchAdmittedLead(lead = {}, hotel = {}, budget = {}, opts = {}) {
  const maxDepth = opts.maxDepth ?? 2;
  const allowThird = opts.allowThirdIfStrong === true;
  const match = opts.successMatch || SUCCESS_MATCH.NONE;
  const log = [];
  let current = { ...lead };
  let depth = opts.startingDepth ?? 1; // page already validated in comp mining
  let blockersResolved = 0;
  let classificationChanged = false;

  // Depth-1 already done via comp page validation — optional re-fetch contact/housing
  while (depth < maxDepth || (allowThird && match === SUCCESS_MATCH.STRONG && depth < 3)) {
    const advice = jevAdviseAdmittedLead(current, {
      admitted: true,
      demonstratedDemand: true,
      depth,
      successMatch: match,
      placeNames: hotel.placeNames || [hotel.market],
      priorBlocker: log[log.length - 1]?.nextBlocker,
    });
    log.push({
      opportunityId: current.id,
      hotelKey: hotel.hotelKey,
      depth: depth + 1,
      ...advice,
    });

    if (!advice.oneMoreStepWorthwhile || advice.stopContinue === "STOP") break;
    if (!hasSerp() || budget.queriesLeft <= 0) {
      log.push({
        opportunityId: current.id,
        hotelKey: hotel.hotelKey,
        depth: depth + 1,
        issued: true,
        action: JEV_ACTION.STOP_LOW_INFORMATION_GAIN,
        note: "Budget exhausted",
        wroteFacts: false,
        promoted: false,
      });
      break;
    }

    const action = getGdiNextBestResearchAction(current, {
      marketPlace: (hotel.placeNames || [hotel.market])[0],
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
        hl: hotel.serpHl || hotel.serpHlPrimary || "en",
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
      depth += 1;
      continue;
    }

    const before = { ...current };
    const pageEv = await validateResearchLeadPage(
      { ...current, officialSource: hit.link },
      {}
    );
    budget.costUsd += 0.01;
    current = applyPageEvidenceToLead(
      {
        ...current,
        sources: [
          ...(current.sources || []),
          { url: hit.link, kind: "demonstrated_demand_v6_jev", blocker: action.blocker },
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
    if (resolved) blockersResolved += 1;
    depth += 1;

    // Cap: do not routinely go to 3
    if (depth >= 2 && !(allowThird && match === SUCCESS_MATCH.STRONG)) break;
  }

  return {
    lead: current,
    depth,
    jevLog: log,
    blockersResolved,
    classificationChanged,
  };
}
