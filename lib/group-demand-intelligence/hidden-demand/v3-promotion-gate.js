/**
 * Customer promotion gate for Hidden Demand V3.
 * Directory listing alone is never enough.
 */

import {
  CANDIDATE_STATE,
  LODGING_PROOF,
  ADDRESSABILITY,
  HOTEL_OPPORTUNITY_LODGING,
} from "./v3-states.js";
import { mayBecomeHotelOpportunity, mayCustomerWatch } from "./v3-lodging-ladder.js";
import { cleanEntityDisplayName } from "./v3-entity-clean.js";

function isNoiseCompany(name = "") {
  const n = cleanEntityDisplayName(name).toLowerCase();
  if (!n || n.length < 4) return true;
  return /^(scan exhibitors|interested in exhibiting|supporters?\s*&\s*sponsors?|powered by|view all|see all|exhibitor login|become an exhibitor)/i.test(
    n
  );
}

/**
 * @returns {{ ok, customerState, reasons, watchAllowed }}
 */
export function passesCustomerPromotionGateV3(packet = {}) {
  const reasons = [];
  const fail = (r) => {
    reasons.push(r);
    return {
      ok: false,
      customerState: "INTERNAL_ONLY",
      reasons,
      watchAllowed: false,
    };
  };

  if (!packet.company && !packet.organizationName) return fail("missing_company");
  if (isNoiseCompany(packet.company || packet.organizationName)) {
    return fail("queue_noise_company");
  }
  if (!packet.futureTiming && !packet.year) return fail("no_future_timing");
  if (!packet.participationEvidence && !packet.participationRole) {
    return fail("no_participation");
  }
  if (!packet.teamSupported) return fail("no_traveling_team");
  if (!mayBecomeHotelOpportunity(packet.lodgingProof)) {
    if (mayCustomerWatch(packet.lodgingProof) && packet.teamSupported) {
      return {
        ok: false,
        customerState: "WATCH",
        reasons: ["lodging_plausible_watch_only"],
        watchAllowed: true,
      };
    }
    return fail(`lodging_insufficient_${packet.lodgingProof || "UNKNOWN"}`);
  }
  if (!packet.hotelMatched) return fail("no_hotel_fit");
  if (!packet.sourceChain?.length && !packet.sourceUrl) return fail("no_source_chain");
  if (packet.contactResearchAttempted === false) return fail("contact_not_attempted");

  const addr = packet.addressability || ADDRESSABILITY.NO_USABLE_PATH;
  if (addr === ADDRESSABILITY.NO_USABLE_PATH) {
    return {
      ok: false,
      customerState: "WATCH",
      reasons: ["strong_opp_contact_unresolved"],
      watchAllowed: true,
    };
  }

  // Named WHO must look like a person, not UI chrome
  const whoName = packet.who?.[0]?.name || packet.primaryContactName || "";
  if (whoName && /^(booth|press|event|new york|scan|interested)/i.test(whoName)) {
    return fail("who_is_ui_chrome");
  }

  const actionable =
    addr === ADDRESSABILITY.NAMED_PERSON &&
    Boolean(packet.timingTrigger) &&
    Boolean(packet.recommendedAction) &&
    Boolean(whoName) &&
    whoName.split(/\s+/).length >= 2;

  return {
    ok: true,
    customerState: actionable ? "ACTIONABLE_NOW" : "WATCH",
    reasons: actionable ? ["actionable"] : ["promotable_watch"],
    watchAllowed: true,
  };
}

export function mapContactDepthBucket(addressability, contacts = []) {
  if (addressability === ADDRESSABILITY.NAMED_PERSON) {
    if (contacts.some((c) => c.email)) return "NAMED_DIRECT";
    return "NAMED_PARTIAL";
  }
  if (addressability === ADDRESSABILITY.FUNCTIONAL_PATH) return "FUNCTIONAL";
  if (addressability === ADDRESSABILITY.COMPANY_PATH) return "ORG_PATH";
  return "NO_CONTACT";
}

export function shouldDowngradeV2CustomerRow(opp = {}, v3ByOrg = new Map()) {
  if (opp.opportunityType !== "HIDDEN_DEMAND" && opp.gdiVersion !== "gdi_hidden_demand_v2") {
    return { downgrade: false };
  }
  const org = String(opp.organizationName || "").toLowerCase();
  const v3 = v3ByOrg.get(org);
  if (!v3) {
    return {
      downgrade: true,
      to: CANDIDATE_STATE.MARKET_ENTITY,
      reason: "no_v3_qualification",
    };
  }
  if (
    v3.candidateState === CANDIDATE_STATE.HOTEL_OPPORTUNITY ||
    v3.candidateState === CANDIDATE_STATE.ACTIONABLE_NOW
  ) {
    return { downgrade: false };
  }
  if (v3.candidateState === CANDIDATE_STATE.LODGING_PLAUSIBLE) {
    return {
      downgrade: true,
      to: CANDIDATE_STATE.LODGING_PLAUSIBLE,
      reason: "lodging_plausible_not_opportunity",
      customerFacingState: "WATCH",
    };
  }
  return {
    downgrade: true,
    to: v3.candidateState || CANDIDATE_STATE.MARKET_HIDDEN_CANDIDATE,
    reason: `v3_state_${v3.candidateState}`,
    customerFacingState: "INTERNAL_ONLY",
  };
}

export {
  CANDIDATE_STATE,
  LODGING_PROOF,
  HOTEL_OPPORTUNITY_LODGING,
};
