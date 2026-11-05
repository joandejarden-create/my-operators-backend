/**
 * Scaling Companies drawer presentation contract V1
 *
 * Phase 1 (structure/layout/SoT only): ensure Scaling opportunities render the
 * corporate-account drawer and prefer commercialIntelligenceBrief over
 * Association Events gold briefs / association procurement boilerplate.
 *
 * Does NOT invent commercial conclusions or rewrite sealed research stores.
 * Contaminated association language is suppressed at presentation time with an
 * explicit research-gap fallback.
 */

export const SCALING_DRAWER_PRESENTATION_CONTRACT_V1 =
  "gdi-scaling-drawer-presentation-contract-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/** Association / event-procurement language that must not default on Scaling drawers. */
export const ASSOCIATION_PROCUREMENT_BOILERPLATE_RE =
  /association staff\s*vs\s*PCO|PCO vs housing|host\/overflow|meetings\s*\/\s*congress\s*\/\s*operations route|Identify the association meetings|event edition|scientific programme|housing provider|Peak rooms|Attendance:|Account \/ occasion/i;

export const ASSOCIATION_DEMAND_BOILERPLATE_RE =
  /\bdelegates?\b|\bspeakers?, or delegate lodging\b|mid-size meetings, speakers, or delegate lodging|not proof of an open room block(?!.*preferred-hotel)/i;

export function isScalingCompaniesSegment(opportunity = {}) {
  const id = text(opportunity.id || opportunity.opportunityId);
  const segEarly = text(opportunity.researchSegment || opportunity.segment).toLowerCase();
  // Mechanism-first: gdi_sc_* IDs reclassified into Association/Conference/Events
  // must not keep the Scaling drawer (Condesa incentive/event local preview).
  if (
    segEarly === "associations_government" ||
    segEarly.includes("association / conference") ||
    (opportunity.segmentReclassificationPreview === true &&
      segEarly.includes("assoc"))
  ) {
    return false;
  }
  if (/^gdi_sc_/i.test(id)) return true;
  const demandType = text(
    opportunity.demandProductType || opportunity.opportunityType || opportunity.detailLayout,
  ).toUpperCase();
  if (
    /CORPORATE_ACCOUNT/.test(demandType) &&
    !opportunity.associationEventsGoldBrief &&
    !/assoc|conference|congress/i.test(text(opportunity.segment || opportunity.researchSegment))
  ) {
    // Prefer explicit Scaling markers; still accept corporate-account product types
    // used by Scaling FullE2E when segment labels were omitted.
    if (
      /scaling|corporate account relationship|corporate account development/i.test(
        `${opportunity.segment || ""} ${opportunity.researchSegment || ""} ${opportunity.title || ""} ${demandType}`,
      )
    ) {
      return true;
    }
  }
  const seg = text(opportunity.researchSegment || opportunity.segment).toLowerCase();
  if (!seg) {
    return (
      String(opportunity.opportunityType || opportunity.detailLayout || "").toUpperCase() ===
        "CORPORATE_ACCOUNT_RELATIONSHIP" &&
      !opportunity.associationEventsGoldBrief
    );
  }
  if (seg === "scaling_companies" || seg.includes("scaling")) return true;
  if (/emerging corporate|corporate account/i.test(seg) && !/assoc/i.test(seg)) {
    return true;
  }
  return false;
}

export function isAssociationEventsSegment(opportunity = {}) {
  if (isScalingCompaniesSegment(opportunity)) return false;
  const seg = text(opportunity.researchSegment || opportunity.segment).toLowerCase();
  if (/assoc|associations_government|government/.test(seg)) return true;
  if (opportunity.associationEventsGoldBrief && !opportunity.commercialIntelligenceBrief) {
    return true;
  }
  return false;
}

export function hasAssociationProcurementBoilerplate(raw) {
  return ASSOCIATION_PROCUREMENT_BOILERPLATE_RE.test(text(raw));
}

export function hasAssociationDemandBoilerplate(raw) {
  return ASSOCIATION_DEMAND_BOILERPLATE_RE.test(text(raw));
}

function briefApproachText(brief) {
  if (!brief || brief.recommendedSalesApproach == null) return "";
  const a = brief.recommendedSalesApproach;
  if (typeof a === "string") return text(a);
  if (typeof a === "object") {
    return text(a.nextAction || a.openingApproach || "");
  }
  return "";
}

/**
 * Corporate research-gap action when Association procurement copy leaked onto Scaling.
 * Does not invent a named buyer or confirmed demand.
 */
export function buildScalingCorporateResearchGapAction(opportunity = {}) {
  const org =
    text(opportunity.organizationName || opportunity.company || opportunity.companyName) ||
    "this company";
  const hotel =
    text(
      opportunity.hotelDisplayName ||
        opportunity.hotelName ||
        opportunity.targetHotelName,
    ) || "this hotel";
  const city = text(opportunity.eventCity || opportunity.city || opportunity.market) || "the local market";
  return (
    `Identify who owns corporate travel, visiting-staff lodging, training, and local meetings for ${org} in ${city}. ` +
    `Introduce ${hotel} and ask whether a preferred-hotel, travel-management, or project-team lodging pattern already exists. ` +
    `Do not treat association/PCO/host-overflow sourcing questions as the default for this corporate account.`
  );
}

/**
 * Rebuild a concise Scaling why-this-hotel from CIB advantages/limitations when
 * top-level prose carries Association demand boilerplate.
 */
export function rebuildScalingWhyThisHotelFromBrief(opportunity = {}) {
  const cib = opportunity.commercialIntelligenceBrief;
  if (!cib || typeof cib !== "object") return null;
  const adv = Array.isArray(cib.whyThisHotelAdvantages)
    ? cib.whyThisHotelAdvantages.map(text).filter(Boolean)
    : [];
  const lim = Array.isArray(cib.whyThisHotelLimitations)
    ? cib.whyThisHotelLimitations.map(text).filter(Boolean)
    : [];
  const parts = [...adv.slice(0, 2), ...lim.slice(0, 2)];
  if (!parts.length) {
    const why = text(cib.whyThisHotel);
    if (why && !hasAssociationDemandBoilerplate(why) && !hasAssociationProcurementBoilerplate(why)) {
      return why;
    }
    return null;
  }
  return parts.join(" ");
}

/**
 * Ensure Scaling opportunities select the corporate-account drawer layout.
 * Does not reclassify Association Events records.
 */
export function ensureScalingDrawerLayoutFields(opportunity = {}) {
  if (!isScalingCompaniesSegment(opportunity)) return opportunity;
  const next = { ...opportunity };
  if (!next.opportunityType) {
    next.opportunityType = "CORPORATE_ACCOUNT_RELATIONSHIP";
  }
  if (!next.detailLayout) {
    next.detailLayout = "CORPORATE_ACCOUNT_RELATIONSHIP";
  }
  if (!text(next.opportunityTypeLabel)) {
    next.opportunityTypeLabel = "Corporate Account Relationship";
  }
  next.scalingDrawerPresentationContractVersion =
    SCALING_DRAWER_PRESENTATION_CONTRACT_V1;
  return next;
}

/**
 * Segment-aware gold-brief source of truth for customer presentation actions.
 * Scaling → commercialIntelligenceBrief first (ignore AEGB for presentation).
 * Association → associationEventsGoldBrief first.
 */
export function resolveSegmentAuthoritativePresentation(opportunity = {}) {
  const next = { ...opportunity };
  const scaling = isScalingCompaniesSegment(next);
  const association = isAssociationEventsSegment(next);

  const cibApproach = briefApproachText(next.commercialIntelligenceBrief);
  const aegbApproach = briefApproachText(next.associationEventsGoldBrief);
  const salesApproach = briefApproachText(next.salesBrief);

  let authoritative = "";
  let source = "top_level_fallback";

  if (scaling) {
    if (cibApproach && !hasAssociationProcurementBoilerplate(cibApproach)) {
      authoritative = cibApproach;
      source = "commercialIntelligenceBrief";
    } else if (salesApproach && !hasAssociationProcurementBoilerplate(salesApproach)) {
      authoritative = salesApproach;
      source = "salesBrief";
    } else if (
      text(next.recommendedAction) &&
      !hasAssociationProcurementBoilerplate(next.recommendedAction)
    ) {
      authoritative = text(next.recommendedAction);
      source = "top_level_clean";
    } else if (cibApproach || salesApproach || aegbApproach || text(next.recommendedAction)) {
      authoritative = buildScalingCorporateResearchGapAction(next);
      source = "scaling_association_boilerplate_suppressed";
      next.scalingAssociationBoilerplateSuppressed = true;
    }
  } else if (association) {
    if (aegbApproach) {
      authoritative = aegbApproach;
      source = "associationEventsGoldBrief";
    } else if (salesApproach) {
      authoritative = salesApproach;
      source = "salesBrief";
    } else if (cibApproach) {
      authoritative = cibApproach;
      source = "commercialIntelligenceBrief";
    }
  } else {
    // Ambiguous / other: prefer matching brief without inventing.
    if (aegbApproach && !cibApproach) {
      authoritative = aegbApproach;
      source = "associationEventsGoldBrief";
    } else if (cibApproach) {
      authoritative = cibApproach;
      source = "commercialIntelligenceBrief";
    } else if (salesApproach) {
      authoritative = salesApproach;
      source = "salesBrief";
    }
  }

  if (
    /^Contact\s+/i.test(authoritative) &&
    next.hasVerifiedNamedRelevantContact === false
  ) {
    authoritative = scaling
      ? buildScalingCorporateResearchGapAction(next)
      : "";
    if (scaling) source = "scaling_association_boilerplate_suppressed";
  }

  if (authoritative) {
    next.recommendedAction = authoritative;
    next.suggestedAction = authoritative;
    next.salesActionRecommended = authoritative;
  }
  next.presentationSourceOfTruth = source;

  // Why this hotel — segment-aware
  const cibWhy = text(next.commercialIntelligenceBrief?.whyThisHotel);
  const aegbWhy = text(next.associationEventsGoldBrief?.whyThisHotel);
  const salesWhy = text(next.salesBrief?.whyThisHotel);

  let why = "";
  if (scaling) {
    if (cibWhy && !hasAssociationDemandBoilerplate(cibWhy) && !hasAssociationProcurementBoilerplate(cibWhy)) {
      why = cibWhy;
    } else {
      why =
        rebuildScalingWhyThisHotelFromBrief(next) ||
        (salesWhy && !hasAssociationDemandBoilerplate(salesWhy) ? salesWhy : "") ||
        "";
    }
  } else if (association) {
    why = aegbWhy || salesWhy || cibWhy || "";
  } else {
    why = cibWhy || aegbWhy || salesWhy || "";
  }

  if (why) {
    next.whyThisHotel = why;
    next.summaryWhyHotel = why;
    if (next.salesBrief && typeof next.salesBrief === "object") {
      next.salesBrief = {
        ...next.salesBrief,
        whyThisHotel: why,
        recommendedSalesApproach:
          authoritative || next.salesBrief.recommendedSalesApproach,
      };
    }
    // Keep CIB as Scaling SoT surface; do not copy Association AEGB into CIB.
    if (
      scaling &&
      next.commercialIntelligenceBrief &&
      typeof next.commercialIntelligenceBrief === "object" &&
      authoritative
    ) {
      const cib = { ...next.commercialIntelligenceBrief };
      // Only replace string approaches that are association-contaminated.
      if (
        typeof cib.recommendedSalesApproach === "string" &&
        hasAssociationProcurementBoilerplate(cib.recommendedSalesApproach)
      ) {
        cib.recommendedSalesApproach = authoritative;
      }
      if (
        hasAssociationDemandBoilerplate(cib.whyThisHotel) ||
        hasAssociationProcurementBoilerplate(cib.whyThisHotel)
      ) {
        cib.whyThisHotel = why;
      }
      next.commercialIntelligenceBrief = cib;
    }
  }

  // Prefer CIB structured potential-business over Association "delegates" framing.
  if (scaling && next.commercialIntelligenceBrief?.potentialBusinessForThisHotel) {
    const pot = text(next.commercialIntelligenceBrief.potentialBusinessForThisHotel);
    if (pot && !hasAssociationProcurementBoilerplate(pot)) {
      next.potentialHotelBusiness = pot;
      next.summaryWhyMatters = next.summaryWhyMatters || pot;
    }
  }

  // Prefer CIB activity narrative for Scaling What-is-happening when present.
  if (scaling && next.commercialIntelligenceBrief?.companyAndLocalActivity) {
    const act = text(next.commercialIntelligenceBrief.companyAndLocalActivity);
    if (act && !hasAssociationProcurementBoilerplate(act)) {
      next.whatIsHappening = act;
      if (
        !text(next.summaryWhat) ||
        hasAssociationProcurementBoilerplate(next.summaryWhat)
      ) {
        next.summaryWhat = act;
      }
    }
  }

  // Prefer CIB whyActNow for Scaling timing.
  if (scaling && text(next.commercialIntelligenceBrief?.whyActNow)) {
    const wan = text(next.commercialIntelligenceBrief.whyActNow);
    if (!/OPEN[_ ]?ENDED/i.test(wan)) {
      next.whyEstablishContactNow = wan;
      next.whyNow = wan;
    }
  }

  return ensureScalingDrawerLayoutFields(next);
}
