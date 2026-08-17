/**
 * Why It Matters / Recommended Action templates (may/could language only).
 */
import { displayHotelLabel } from "./market-alerts-qualification-gate.js";

function hotelLabel(entities = {}) {
  const named = displayHotelLabel(entities, { preferGeneric: false });
  if (named) return named;
  if (entities.projectLabel) return entities.projectLabel;
  return displayHotelLabel(entities, { preferGeneric: true }) || "A hotel asset";
}

function roomsNote(entities = {}) {
  return entities.rooms ? ` (${entities.rooms} rooms)` : "";
}

function directionOverlay(audience, direction, hotel, signalType) {
  if (direction === "Rejected / Blocked") {
    if (audience === "brand") {
      return {
        whyItMatters: `The current proposal for ${hotel} has been rejected or blocked. Monitor only if the developer appeals or resubmits.`,
        recommendedAction: "Do not treat this as an active development opportunity unless an appeal or resubmission is reported.",
      };
    }
    if (audience === "operator") {
      return {
        whyItMatters: `The current hotel proposal has been rejected or blocked. This is not an open management opportunity.`,
        recommendedAction: "Monitor only if the developer appeals or resubmits a revised project.",
      };
    }
    if (audience === "owner") {
      return {
        whyItMatters: `A proposed hotel project was rejected or blocked at the current approval stage. Nearby supply impact is not committed.`,
        recommendedAction: "Treat as a future-supply watch item only if an appeal or resubmission appears.",
      };
    }
  }
  if (direction === "Challenged") {
    if (audience === "brand" && signalType === "Potential Development Opportunity") {
      return {
        whyItMatters: `The proposed hotel remains at an early stage and is facing regulatory or planning challenges. No brand has been publicly announced.`,
        recommendedAction: "Monitor the review before treating the project as an active development opportunity.",
      };
    }
    if (audience === "operator" && signalType === "Potential Management Opportunity") {
      return {
        whyItMatters: `The project is in an early development stage but is facing regulatory or environmental scrutiny. Operating structure is unresolved.`,
        recommendedAction: "Monitor the review before treating this as an active management opportunity.",
      };
    }
    if (audience === "owner") {
      return {
        whyItMatters: `A proposed hotel project is facing legal, environmental, or planning challenge. Delivery is not assured.`,
        recommendedAction: "Track the challenge outcome before incorporating the project into supply assumptions.",
      };
    }
  }
  if (
    direction === "Advancing" &&
    audience === "brand" &&
    signalType === "Potential Development Opportunity"
  ) {
    return {
      whyItMatters: `The proposed hotel is progressing through development and no brand has been publicly announced.`,
      recommendedAction: "Track planning and capital milestones; do not assume the owner is seeking a brand.",
    };
  }
  return null;
}

/**
 * @param {'owner'|'brand'|'operator'} audience
 * @param {{ eventType: string|null, whatChanged?: string|null, entities?: object, signalType?: string|null, decisionStage?: string|null }} ctx
 * @returns {{ whyItMatters: string|null, recommendedAction: string|null }}
 */
export function buildAudienceTemplates(audience, ctx = {}) {
  const eventType = ctx.eventType || null;
  const signalType = ctx.signalType || null;
  const entities = ctx.entities || {};
  const hotel = hotelLabel(entities);
  const rooms = roomsNote(entities);
  const direction = ctx.projectDirection || null;

  if (!eventType || !signalType) {
    return { whyItMatters: null, recommendedAction: null };
  }

  const directionCopy = directionOverlay(audience, direction, hotel, signalType);
  if (directionCopy) return directionCopy;

  if (audience === "brand" && signalType === "Competitive Brand Move") {
    return {
      whyItMatters: `A brand affiliation change was announced for ${hotel}${rooms}${entities.brandInvolved ? ` (${entities.brandInvolved})` : ""}. The affiliation decision appears already made; this is competitive intelligence, not an open lead.`,
      recommendedAction:
        "Track the competitive brand move, market positioning, and any pipeline or white-space implications in the corridor.",
    };
  }

  if (audience === "operator" && signalType === "Competitive Operator Move") {
    return {
      whyItMatters: `An operator or management structure was announced for ${hotel}${rooms}. The operating decision appears already made; this is competitive intelligence, not an open mandate.`,
      recommendedAction: "Record the operator/brand move for competitive intelligence and benchmark tracking.",
    };
  }

  if (audience === "operator" && signalType === "Management Agreement Announced") {
    return {
      whyItMatters: `A management agreement was announced for ${hotel}${rooms}. The operating structure appears confirmed rather than open for review.`,
      recommendedAction: "Record the management agreement for competitive intelligence.",
    };
  }

  if (audience === "owner") {
    return ownerTemplates(eventType, hotel, rooms);
  }
  if (audience === "brand") {
    return brandTemplates(eventType, hotel, rooms, entities);
  }
  if (audience === "operator") {
    return operatorTemplates(eventType, hotel, rooms, entities);
  }
  return { whyItMatters: null, recommendedAction: null };
}

function ownerTemplates(eventType, hotel, rooms) {
  const map = {
    "Hotel For Sale": {
      why: `${hotel}${rooms} is being marketed for sale. Nearby owners may want to watch pricing signals, buyer interest, and any competitive set impact.`,
      action:
        "Review the listing facts against your competitive set and decide whether to monitor, or whether a peer transaction could inform your hold/sell thinking.",
    },
    Acquisition: {
      why: `An ownership change was reported for ${hotel}${rooms}. That may shift competitive positioning, CapEx appetite, or brand strategy nearby.`,
      action: "Note the buyer profile and watch for follow-on brand, operator, or renovation moves.",
    },
    Sale: {
      why: `A hotel sale was reported for ${hotel}${rooms}. Completed sales can reset local pricing and buyer expectations.`,
      action: "Compare disclosed terms (if any) to your asset thesis and mark for competitive tracking.",
    },
    "Portfolio Acquisition": {
      why: "A multi-asset portfolio move was reported. Portfolio buyers may consolidate scale or reflag assets over time.",
      action: "Identify whether any assets sit in your markets and watch for subsequent single-asset actions.",
    },
    Distress: {
      why: `${hotel}${rooms} shows a distress signal. Distressed situations can change ownership, brand, or operator quickly.`,
      action: "Monitor for sale, receivership, or restructuring updates that could alter local competitive dynamics.",
    },
    "New Development": {
      why: `New supply was announced. Additional rooms could pressure occupancy or ADR in overlapping demand segments.`,
      action: "Estimate timing, scale, and segment overlap versus your asset(s), then decide whether deeper diligence is warranted.",
    },
    "Planning Approval": {
      why: "A project advanced through planning. Approvals can move speculative supply closer to delivery.",
      action: "Track entitlement status and planned keys; revisit if construction financing or groundbreaking follows.",
    },
    "Construction Start": {
      why: "Construction activity suggests supply is moving toward delivery rather than remaining conceptual.",
      action: "Update your supply timeline assumptions for the corridor and note opening window risk.",
    },
    "Site Acquisition": {
      why: "A development site appears to have been acquired for a hospitality project. If the project proceeds, it could add future supply — that is not yet certain.",
      action: "Track planning and project progression before incorporating it fully into supply assumptions.",
    },
    "Planning Application": {
      why: "A hotel planning or entitlement application was reported. The project may still be early and could change or stall.",
      action: "Track whether the application advances before treating it as committed competitive supply.",
    },
    "Development Proposal": {
      why: "A hotel or resort project has been proposed. It could add future supply if it advances, but it is not yet a committed opening.",
      action: "Track planning and project progression before incorporating it fully into supply assumptions.",
    },
    "Adaptive Reuse Proposal": {
      why: "A conversion-to-hotel proposal was reported. Adaptive reuse can add lodging inventory faster than ground-up builds if it proceeds.",
      action: "Watch planning progress and likely segment; do not assume the project will be built.",
    },
    Reflag: {
      why: `A reflag or affiliation change was reported for ${hotel}${rooms}. Brand changes can alter demand mix and rate posture.`,
      action: "Assess whether the new brand competes more directly with your product and adjust competitive set notes.",
    },
    Conversion: {
      why: "A conversion or adaptive reuse was reported. New hotel inventory from conversion can arrive faster than ground-up builds.",
      action: "Check likely segment and opening timing against your competitive set.",
    },
    "Brand Signing": {
      why: `A brand affiliation was announced for ${hotel}${rooms}. Brand support and distribution could change local competition.`,
      action: "Note the brand tier and watch for opening or renovation milestones.",
    },
    "Operator Appointment": {
      why: "An operator appointment can signal a push for performance improvement or a new ownership mandate.",
      action: "Watch for subsequent CapEx, reflag, or commercial strategy changes.",
    },
    "Operator Change": {
      why: "An operator change may precede repositioning or underwriting shifts.",
      action: "Update operator notes for the competitive set and monitor near-term commercial moves.",
    },
    Financing: {
      why: "New financing was reported. Capital events can fund CapEx, acquisitions, or stabilize ownership.",
      action: "Note lender/sponsor signals if disclosed and watch for related development or renovation news.",
    },
    Refinancing: {
      why: "Refinancing can extend hold periods or free capital for CapEx.",
      action: "Treat as a capital signal; revisit if a renovation or repositioning announcement follows.",
    },
    JV: {
      why: "A joint venture structure was reported. JV partners can change development or ownership capacity.",
      action: "Identify partner roles if disclosed and watch for follow-on project announcements.",
    },
    Recapitalization: {
      why: "Recapitalization can reset ownership economics without a full sale.",
      action: "Monitor whether brand, operator, or CapEx plans change after the recap.",
    },
    "Major Renovation": {
      why: `A major renovation was reported for ${hotel}${rooms}. Product upgrades may shift rate and competitive pressure.`,
      action: "Note scope/timing if available and compare to your own CapEx roadmap.",
    },
    Repositioning: {
      why: "A repositioning signal suggests product or demand-mix change that could affect nearby assets.",
      action: "Clarify target segment if disclosed and update competitive positioning notes.",
    },
  };

  const t = map[eventType];
  if (!t) {
    return {
      whyItMatters: `${hotel}${rooms}: this commercial signal may affect nearby competitive dynamics.`,
      recommendedAction: "Skim the source facts and decide whether deeper monitoring is useful for your market.",
    };
  }
  return { whyItMatters: t.why, recommendedAction: t.action };
}

function brandTemplates(eventType, hotel, rooms, entities) {
  if (eventType === "Hotel For Sale") {
    return {
      whyItMatters: null,
      recommendedAction: null,
    };
  }

  const map = {
    "New Development": {
      why: `${hotel}${rooms} appears to be advancing without a clear brand affiliation in the coverage. That could represent a development or conversion conversation later — not a confirmed opportunity.`,
      action: "If the market fits your growth priorities, you could research ownership and track branding milestones; do not assume the owner is seeking a brand.",
    },
    "Planning Approval": {
      why: "An unflagged or early-stage project advanced planning. Brand selection may still be open, or may already be decided off-market.",
      action: "Qualify market fit first; only then consider whether ownership outreach would be appropriate.",
    },
    "Construction Start": {
      why: "Construction progress on a potentially unflagged project can compress the window before a brand decision is locked.",
      action: "Confirm affiliation status from the source; if still unclear, treat as a watch item rather than a live pitch.",
    },
    "Site Acquisition": {
      why: "A developer appears to have secured a site for a planned hotel and no affiliation has been publicly announced. That could become a development conversation later — not a confirmed opportunity.",
      action: "Track the project as planning advances and assess whether it fits your development strategy. Do not assume the owner is seeking a brand.",
    },
    "Planning Application": {
      why: "A hotel planning application was reported with no publicly confirmed brand. Brand selection may still be open, or may already be decided off-market.",
      action: "Qualify market fit first; only then consider whether ownership outreach would be appropriate.",
    },
    "Development Proposal": {
      why: "A hotel project has been proposed and no brand has been publicly announced. Affiliation may still be unresolved.",
      action: "Track the project as it advances; do not treat a proposal as an open brand mandate.",
    },
    "Adaptive Reuse Proposal": {
      why: "A conversion-to-hotel proposal appears to be advancing without a publicly confirmed brand.",
      action: "Watch planning progress and only escalate if the market and product fit are clear.",
    },
    Acquisition: {
      why: `Ownership changed for ${hotel}${rooms}. New owners sometimes revisit brand strategy — that is possible, not confirmed.`,
      action: "Review whether the asset fits your conversion or soft-brand criteria before any outreach.",
    },
    Sale: {
      why: "A completed sale can precede brand review, but many buyers keep existing flags.",
      action: "Check current affiliation if known and only escalate if conversion criteria are clearly met.",
    },
    Distress: {
      why: "Distress can create conversion or reflag windows, but timing and control are uncertain.",
      action: "Monitor ownership/control updates; avoid assuming a brand search is underway.",
    },
    Reflag: {
      why: `A reflag was reported for ${hotel}${rooms}. That is a competitive brand change in-market.`,
      action: "Update competitive brand maps and assess share impact in the corridor.",
    },
    Conversion: {
      why: "A conversion was reported. Adaptive reuse projects can introduce branded inventory quickly.",
      action: "Note brand/segment if disclosed and compare to your white-space priorities.",
    },
    "Brand Signing": {
      why: `A brand affiliation was announced for ${hotel}${rooms}${entities.brandInvolved ? ` (${entities.brandInvolved})` : ""}. This reflects a completed affiliation decision rather than an open conversion lead.`,
      action: "Track the competitive brand move and market positioning; do not treat as an open affiliation opportunity.",
    },
    "Brand Exit": {
      why: "A brand exit can open white space, but a replacement flag may already be chosen.",
      action: "Confirm status from the source and watch for a replacement announcement.",
    },
  };

  const t = map[eventType];
  if (!t) {
    return {
      whyItMatters: "This signal could relate to brand footprint or conversion dynamics, depending on facts in the source.",
      recommendedAction: "Verify affiliation status before treating this as an actionable brand lead.",
    };
  }
  return { whyItMatters: t.why, recommendedAction: t.action };
}

function operatorTemplates(eventType, hotel, rooms) {
  if (eventType === "Hotel For Sale") {
    return { whyItMatters: null, recommendedAction: null };
  }

  const map = {
    "New Development": {
      why: `${hotel}${rooms} may still need an operator as the project advances — that is a possibility, not evidence of a search.`,
      action: "If the market and scale fit your platform, you could track ownership and opening timing; do not invent a management RFP.",
    },
    "Planning Approval": {
      why: "Early-stage projects sometimes select operators after entitlements. Selection may already be private.",
      action: "Qualify fit, then watch for management announcements rather than assuming an open search.",
    },
    "Construction Start": {
      why: "Construction can mean operator decisions are imminent or already made.",
      action: "Confirm operator status from coverage; keep as a watch item if still undisclosed.",
    },
    "Site Acquisition": {
      why: "A planned hotel site was reported and no operator has been publicly identified. That is a possibility, not evidence of a management search.",
      action: "Track operating structure as the development progresses. Do not claim the owner is seeking an operator unless the source says so.",
    },
    "Planning Application": {
      why: "A hotel planning filing was reported and no operator has been publicly identified.",
      action: "Track operating structure as planning advances; do not invent a management RFP.",
    },
    "Development Proposal": {
      why: "The project appears to be advancing without a publicly identified operator. That does not mean a search is underway.",
      action: "Track operating structure as the development progresses.",
    },
    "Adaptive Reuse Proposal": {
      why: "A conversion-to-hotel proposal was reported and no operator has been publicly identified.",
      action: "Watch whether an operator is named as planning advances.",
    },
    Acquisition: {
      why: `New ownership of ${hotel}${rooms} could eventually revisit third-party management — optional, not confirmed.`,
      action: "Assess whether the asset matches your operating sweet spot before any proactive outreach.",
    },
    Sale: {
      why: "Post-sale operator reviews happen in some deals, but continuity is common.",
      action: "Verify current operator if known; escalate only when fit and timing justify it.",
    },
    Distress: {
      why: "Distress or turnaround situations can create operator review windows under lender or new-owner pressure.",
      action: "Monitor control changes; treat as situational, not a guaranteed mandate.",
    },
    "Operator Appointment": {
      why: `An operator was appointed for ${hotel}${rooms}. That is a competitive platform signal.`,
      action: "Update competitor tracking; no further action required unless you are benchmarking mandates.",
    },
    "Operator Change": {
      why: "An operator change may signal performance or ownership mandate shifts.",
      action: "Note the incoming operator and watch for renovation or commercial strategy follow-through.",
    },
    "Operator Exit": {
      why: "An operator exit can open a management opportunity, or a successor may already be selected.",
      action: "Confirm succession status from the source before treating as an open mandate.",
    },
    "Management Agreement": {
      why: "A management agreement announcement clarifies who is operating the asset.",
      action: "Record for competitive intelligence.",
    },
    "Major Renovation": {
      why: "Major CapEx can accompany operator-led turnarounds or brand-standard upgrades.",
      action: "Watch whether operator or brand changes accompany the renovation.",
    },
    Repositioning: {
      why: "Repositioning often pairs with operating model changes.",
      action: "Clarify whether operator continuity is stated; otherwise keep as a watch item.",
    },
  };

  const t = map[eventType];
  if (!t) {
    return {
      whyItMatters: "This signal could relate to management or operating strategy, depending on source facts.",
      recommendedAction: "Verify operator status before treating this as an actionable management lead.",
    };
  }
  return { whyItMatters: t.why, recommendedAction: t.action };
}
