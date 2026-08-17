/**
 * Audience Worth Reviewing + Signal Type + Decision Stage rules (deterministic V1.1.1 + early signals).
 * Final qualification passes through applyQualificationGate().
 */
import {
  hasFutureHotelComponent,
  isHotelToNonHotelChangeOfUse,
} from "./market-alerts-change-of-use.js";

const EARLY_DEV_EVENTS = [
  "New Development",
  "Planning Approval",
  "Planning Application",
  "Construction Start",
  "Site Acquisition",
  "Development Proposal",
  "Adaptive Reuse Proposal",
];

function isSoftMarketNoise(text) {
  return /\b(revpar|occupancy|adr\b|loyalty|points?|rewards?|member(?:ship)?|workforce|staffing|wage|labor|travel trends?|tourism board|visitor arrivals|STR report|pipeline report|outlook|forecast|survey|study finds|ranking|best hotels? to stay)\b/i.test(
    text
  );
}

function isBrandedOpeningSoft(text, eventType) {
  if (
    [
      "Brand Signing",
      "Reflag",
      "Conversion",
      "Management Agreement",
      "Construction Start",
      "Site Acquisition",
      "Planning Application",
      "Development Proposal",
      "Adaptive Reuse Proposal",
      "New Development",
      "Planning Approval",
    ].includes(eventType || "")
  ) {
    return false;
  }
  if (/\b(breaks? ground|groundbreaking|under construction|construction start)\b/i.test(text)) {
    return false;
  }
  return /\b(opens?|opening|debuts?|unveils?|celebrates? (?:its )?opening)\b/i.test(text);
}

export function hasConfirmedPropertyBrand(text, entities) {
  if (entities?.brandInvolved) return true;
  const t = String(text || "");
  if (
    /\b(?:branded as|will operate as|operate as|to become|part of (?:the )?(?:marriott|hilton|ihg|hyatt|accor|wyndham|choice|radisson|omni)|second act as (?:jw )?marriott)\b/i.test(
      t
    )
  ) {
    return true;
  }
  return /\b((?:AC Hotel|Moxy|Autograph Collection|voco|Holiday Inn(?: Express|Resort)?|Crowne Plaza|Hyatt Regency|Hyatt Place|Hyatt Select|Signia(?: by Hilton)?|Garner|Noted Collection|Tapestry(?: Collection)?|Curio(?: Collection)?|Tribute(?: Portfolio)?|Fairfield(?: Inn)?|Courtyard(?: by Marriott)?|Residence Inn|SpringHill Suites|Element|Aloft|Westin|Sheraton|Four Points|St\.?\s*Regis|W Hotels|Ritz-Carlton|Waldorf Astoria|Conrad|Canopy|Tempo|Kimpton|InterContinental|Hotel Indigo|Andaz|Park Hyatt|Grand Hyatt|Sofitel|Novotel|Mercure|Fairmont|Pullman|Hard Rock|Virgin Hotels|My Place Hotels|Club Med|Hilton|JW Marriott|Omni)\b[^,.]{0,40}(?:Hotel|Resort|Downtown|Collection|Tampa|breaks ground|groundbreaking|development|project|tower|zoning)|(?:breaks ground|groundbreaking|development of|dual-brand)\b[^,.]{0,60}\b(?:AC Hotel|Moxy|Autograph|voco|Holiday Inn|Hyatt|Marriott|Hilton|IHG|Omni|JW Marriott))\b/i.test(
    t
  );
}

export function hasEvidentOperatorStructure(text, entities) {
  if (entities?.operatorInvolved) return true;
  return /\b(management agreement|managed by|will be managed by|operator appointed|mckibbon hospitality|aimbridge|highgate|white lodging|hotel equities|driftwood hospitality)\b/i.test(
    String(text || "")
  );
}

export function isUnflaggedDevelopment(text, eventType, entities) {
  if (!EARLY_DEV_EVENTS.includes(eventType)) {
    return false;
  }
  if (hasConfirmedPropertyBrand(text, entities)) return false;
  if (entities?.brandInvolved) return false;
  return !/\b(marriott|hilton|ihg|hyatt|wyndham|choice|radisson|accor|best western|intercontinental|holiday inn|moxy|ac hotel|autograph|voco|noted collection|tapestry|curio|tribute|signia|garner|fairfield|courtyard|residence inn|springhill|element|aloft|westin|sheraton|four points|st\.?\s*regis|waldorf|conrad|canopy|tempo|kimpton|andaz|park hyatt|grand hyatt|sofitel|novotel|mercure|fairmont|pullman|hard rock|virgin hotels|omni|jw marriott)\b/i.test(
    text
  );
}

function isConstructionOrDevelopmentFinancing(text, eventType) {
  if (eventType !== "Financing") return false;
  return /\b(credit approval|construction (?:loan|financing|debt)|development (?:financing|loan|equity|capital|funding|facility)|project (?:loan|financing)|debt package|hotel development)\b/i.test(
    text
  );
}

/**
 * @param {{ eventType: string|null, title?: string, summary?: string, entities?: object }} input
 */
export function resolveAudienceIntelligence(input = {}) {
  const eventType = input.eventType || null;
  const entities = input.entities || {};
  const text = `${input.title || ""} ${input.summary || ""}`.trim();

  const empty = () => ({
    worthReviewing: false,
    signalType: null,
    decisionStage: null,
  });

  const owner = empty();
  const brand = empty();
  const operator = empty();

  if (!eventType) {
    return { owner, brand, operator, treatment: "STANDARD" };
  }

  if (isSoftMarketNoise(text) && !["Hotel For Sale", "Acquisition", "Sale", "Distress"].includes(eventType)) {
    return { owner, brand, operator, treatment: "STANDARD" };
  }

  if (isBrandedOpeningSoft(text, eventType)) {
    return { owner, brand, operator, treatment: "STANDARD" };
  }

  if (isHotelToNonHotelChangeOfUse(text)) {
    owner.worthReviewing = true;
    owner.signalType = "Strategic Market Change";
    owner.decisionStage = "Active";

    if (
      hasFutureHotelComponent(text) &&
      isUnflaggedDevelopment(text, eventType, entities)
    ) {
      brand.worthReviewing = true;
      brand.signalType = "Potential Development Opportunity";
      brand.decisionStage = "Early";
    }

    const treatment = owner.worthReviewing || brand.worthReviewing || operator.worthReviewing
      ? "REVIEW"
      : "STANDARD";
    return { owner, brand, operator, treatment };
  }

  // --- Owner ---
  if (eventType === "Hotel For Sale") {
    owner.worthReviewing = true;
    owner.signalType = "Capital / Transaction Signal";
    owner.decisionStage = "Active";
  } else if (["Acquisition", "Sale", "Portfolio Acquisition"].includes(eventType)) {
    owner.worthReviewing = true;
    owner.signalType = "Capital / Transaction Signal";
    owner.decisionStage = "Likely Decided";
  } else if (eventType === "Distress") {
    owner.worthReviewing = true;
    owner.signalType = "Strategic Market Change";
    owner.decisionStage = "Active";
  } else if (eventType === "Site Acquisition") {
    owner.worthReviewing = true;
    owner.signalType = "Strategic Market Change";
    owner.decisionStage = "Early";
  } else if (
    ["New Development", "Construction Start", "Planning Approval", "Planning Application", "Development Proposal", "Adaptive Reuse Proposal"].includes(
      eventType
    )
  ) {
    owner.worthReviewing = true;
    owner.signalType = "New Competitive Supply";
    owner.decisionStage =
      eventType === "Construction Start"
        ? "Forming"
        : eventType === "Planning Approval"
          ? "Early"
          : "Early";
  } else if (["Reflag", "Conversion", "Major Renovation", "Repositioning"].includes(eventType)) {
    owner.worthReviewing = true;
    owner.signalType = "Repositioning Activity";
    owner.decisionStage = "Active";
  } else if (["Financing", "Refinancing", "JV", "Recapitalization"].includes(eventType)) {
    owner.worthReviewing = true;
    owner.signalType = "Capital / Transaction Signal";
    owner.decisionStage = "Forming";
  } else if (["Brand Signing", "Operator Appointment", "Operator Change"].includes(eventType)) {
    owner.worthReviewing = true;
    owner.signalType = "Competitive Change";
    owner.decisionStage = "Active";
  }

  // --- Brand ---
  if (["Brand Signing", "Reflag", "Conversion"].includes(eventType)) {
    brand.worthReviewing = true;
    brand.signalType = "Competitive Brand Move";
    brand.decisionStage = "Likely Decided";
  } else if (eventType === "Brand Exit") {
    brand.worthReviewing = true;
    brand.signalType = "Brand White-Space Signal";
    brand.decisionStage = "Active";
  } else if (isUnflaggedDevelopment(text, eventType, entities)) {
    brand.worthReviewing = true;
    brand.signalType = "Potential Development Opportunity";
    brand.decisionStage =
      eventType === "Planning Approval" || eventType === "Construction Start" ? (eventType === "Planning Approval" ? "Early" : "Forming") : "Early";
  } else if (isConstructionOrDevelopmentFinancing(text, eventType) && !hasConfirmedPropertyBrand(text, entities)) {
    brand.worthReviewing = true;
    brand.signalType = "Potential Development Opportunity";
    brand.decisionStage = "Forming";
  } else if (EARLY_DEV_EVENTS.includes(eventType) && hasConfirmedPropertyBrand(text, entities)) {
    brand.worthReviewing = true;
    brand.signalType = "Competitive Brand Move";
    brand.decisionStage = "Likely Decided";
  } else if (eventType === "Distress") {
    brand.worthReviewing = true;
    brand.signalType = "Potential Conversion Opportunity";
    brand.decisionStage = "Active";
  }

  // --- Operator ---
  if (eventType === "Management Agreement") {
    operator.worthReviewing = true;
    operator.signalType = "Management Agreement Announced";
    operator.decisionStage = "Likely Decided";
  } else if (["Operator Appointment", "Operator Change"].includes(eventType)) {
    operator.worthReviewing = true;
    operator.signalType = "Competitive Operator Move";
    operator.decisionStage = "Likely Decided";
  } else if (eventType === "Operator Exit") {
    operator.worthReviewing = true;
    operator.signalType = "Operator Review Signal";
    operator.decisionStage = "Active";
  } else if (isUnflaggedDevelopment(text, eventType, entities) && !hasEvidentOperatorStructure(text, entities)) {
    operator.worthReviewing = true;
    const earlyOpen = ["Site Acquisition", "Planning Application", "Development Proposal", "Adaptive Reuse Proposal"].includes(
      eventType
    );
    operator.signalType = earlyOpen ? "Potential Management Opportunity" : "New Development Opportunity";
    operator.decisionStage =
      eventType === "Planning Approval" ? "Early" : eventType === "Construction Start" ? "Forming" : "Early";
  } else if (isConstructionOrDevelopmentFinancing(text, eventType) && !hasEvidentOperatorStructure(text, entities)) {
    operator.worthReviewing = true;
    operator.signalType = "Potential Management Opportunity";
    operator.decisionStage = "Forming";
  } else if (EARLY_DEV_EVENTS.includes(eventType) && hasEvidentOperatorStructure(text, entities)) {
    operator.worthReviewing = true;
    operator.signalType = "Competitive Operator Move";
    operator.decisionStage = "Likely Decided";
  } else if (eventType === "Distress" || eventType === "Repositioning" || eventType === "Major Renovation") {
    operator.worthReviewing = true;
    operator.signalType = "Turnaround / Repositioning Opportunity";
    operator.decisionStage = "Active";
  }

  const anyReview = owner.worthReviewing || brand.worthReviewing || operator.worthReviewing;

  return {
    owner,
    brand,
    operator,
    treatment: anyReview ? "REVIEW" : "STANDARD",
  };
}

/**
 * Overlay Project Direction onto audience flags (internal). Does not add new Airtable signal types.
 * @param {object} audience
 * @param {string|null} projectDirection
 */
export function applyProjectDirectionToAudience(audience = {}, projectDirection = null) {
  const owner = { ...(audience.owner || {}) };
  const brand = { ...(audience.brand || {}) };
  const operator = { ...(audience.operator || {}) };

  if (projectDirection === "Rejected / Blocked") {
    if (brand.signalType === "Potential Development Opportunity") {
      brand.worthReviewing = false;
      brand.signalType = null;
      brand.decisionStage = null;
    }
    if (
      operator.signalType === "Potential Management Opportunity" ||
      operator.signalType === "New Development Opportunity"
    ) {
      operator.worthReviewing = false;
      operator.signalType = null;
      operator.decisionStage = null;
    }
    if (owner.worthReviewing) {
      owner.signalType = "Strategic Market Change";
      owner.decisionStage = "Early";
    }
  }

  const anyReview = !!(owner.worthReviewing || brand.worthReviewing || operator.worthReviewing);
  return {
    owner,
    brand,
    operator,
    treatment: anyReview ? "REVIEW" : "STANDARD",
  };
}
