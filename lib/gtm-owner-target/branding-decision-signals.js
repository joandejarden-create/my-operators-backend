/**
 * Branding-decision intent signals for GTM owner outreach.
 * Identifies hotels/owners likely approaching brand, operator, or development decisions.
 *
 * Data limits (explicit):
 * - CoStar exports do NOT include franchise contract expiry dates.
 * - `brand_renewal_window` is a heuristic (build/renov year + typical term) — medium/low confidence.
 * - Land purchase / pre-development requires news, permits, or company profile notes — partial coverage.
 *
 * Weights and thresholds: single source of truth — do not duplicate in UI/scripts.
 */
import {
  isNamedPersonEmail,
  isGenericMailboxEmail,
  isVerifiedPersonEmail,
  isReachableCorporateEmail,
  isNonPersonMailboxEmail,
} from "./registry-contact-verification.js";
import {
  isVerifiedPersonPhoneTier,
  pickPrimaryOutreachPhone,
  formatContactPhonesForDisplay,
} from "./registry-phone-verification.js";
import {
  isBrandDecisionEligibleProperty,
  isGenuineThirdPartyOperatorMismatch,
  isHouseBrandForOwner,
  isIntegratedOperatorOwner,
  isThirdPartyFranchiseBrand,
  classifyOwnerOutreachTrack,
  isBrandDecisionOutreachTrack,
  KNOWN_THIRD_PARTY_OPERATOR_RE,
} from "./branding-owner-context.js";
import { pickLeadProperty } from "./owner-lead-asset.js";

/** @typedef {"independent_unbranded" | "conversion_candidate" | "reflag_operator_mismatch" | "new_build" | "recent_open_branded" | "development_pipeline" | "brand_renewal_window" | "portfolio_mixed_brands" | "operator_rfp_proxy"} BrandingSignalId */

/** @typedef {"pre_decision" | "post_decision" | "uncertain"} BrandDecisionTiming */

/** @typedef {{
 *   id: BrandingSignalId,
 *   dealTrigger: string,
 *   label: string,
 *   weight: number,
 *   baseConfidence: "high" | "medium" | "low",
 * }} BrandingSignalDefinition */

export const MAP_BRANDING_DECISION_CONFIG = {
  /** Typical franchise/management agreement length for renewal-window heuristic (years). */
  typicalFranchiseTermYears: 20,
  /** Months before estimated renewal to flag `brand_renewal_window`. */
  renewalWindowMonthsAhead: 36,
  /** Property age (years) above which conversion/reflag messaging applies. */
  conversionAgeYears: 22,
  /** Recent renovation within this many years reduces conversion score. */
  recentRenovationYears: 8,
  /** Year built within this many years → new_build signal. */
  newBuildRecentYears: 3,
  /** Minimum owner intent score (0–100) for outreach-ready shortlist. */
  outreachReadyMinScore: 45,
  /** Bonus when verified owner contact exists. */
  verifiedContactBonus: 15,
  /** Bonus when primary email exists but not fully verified. */
  reachableContactBonus: 5,
  /** Bonus when VP1 direct business or VP2 mobile phone is verified. */
  verifiedPhoneBonus: 8,
};

/** @type {BrandingSignalDefinition[]} */
export const BRANDING_SIGNAL_DEFINITIONS = [
  {
    id: "independent_unbranded",
    dealTrigger: "independent_unbranded",
    label: "Independent / unbranded hotel",
    weight: 28,
    baseConfidence: "high",
  },
  {
    id: "conversion_candidate",
    dealTrigger: "conversion",
    label: "Aging product — conversion / repositioning candidate",
    weight: 22,
    baseConfidence: "medium",
  },
  {
    id: "reflag_operator_mismatch",
    dealTrigger: "reflag",
    label: "Owner ≠ operator — potential reflag or operator change",
    weight: 24,
    baseConfidence: "medium",
  },
  {
    id: "new_build",
    dealTrigger: "new_build",
    label: "Pre-brand development — unbranded new build or pipeline asset",
    weight: 30,
    baseConfidence: "high",
  },
  {
    id: "recent_open_branded",
    dealTrigger: "recent_open_branded",
    label: "Recently opened / UC with brand already chosen (late for brand RFP)",
    weight: 6,
    baseConfidence: "high",
  },
  {
    id: "development_pipeline",
    dealTrigger: "development_pipeline",
    label: "Owner development pipeline (CoStar profile / property type)",
    weight: 26,
    baseConfidence: "medium",
  },
  {
    id: "brand_renewal_window",
    dealTrigger: "brand_renewal_window",
    label: "Estimated brand/management renewal window (heuristic)",
    weight: 20,
    baseConfidence: "low",
  },
  {
    id: "portfolio_mixed_brands",
    dealTrigger: "portfolio_standardization",
    label: "Mixed brands across portfolio — standardization opportunity",
    weight: 18,
    baseConfidence: "medium",
  },
  {
    id: "operator_rfp_proxy",
    dealTrigger: "operator_rfp",
    label: "Third-party operator on asset — operator selection proxy",
    weight: 16,
    baseConfidence: "low",
  },
];

export const VAL_GTM_BRANDING_DEAL_TRIGGER = [
  "none_known",
  "conversion",
  "reflag",
  "operator_rfp",
  "new_build",
  "portfolio_standardization",
  "sale_process",
  "independent_unbranded",
  "brand_renewal_window",
  "development_pipeline",
  "recent_open_branded",
];

const INDEPENDENT_BRAND_RE =
  /^(independent|unbranded|unaffiliated|none|n\/a|na|null|boutique independent|no brand|non branded|non-branded|other)$/i;

const SOFT_COLLECTION_BRAND_RE =
  /\b(ascend|curio|tapestry|unbound|handwritten|independent|collection|individuals)\b/i;

const DEVELOPMENT_TYPE_RE =
  /\b(under construction|proposed|planned|development|pre-?development|site|pipeline|future)\b/i;

const DEVELOPMENT_NAME_RE =
  /\b(hotel project|proyecto hotel|hotel development|future hotel|planned hotel|new hotel)\b/i;

const THIRD_PARTY_OPERATOR_RE =
  /\b(management|gestion|operadora|operator|hospitality group|hotel management)\b/i;

/**
 * @param {string} brand
 */
export function isIndependentOrUnbranded(brand) {
  const b = String(brand || "").trim();
  if (!b) return true;
  return INDEPENDENT_BRAND_RE.test(b);
}

/**
 * Whether CoStar already shows a brand choice on a new/dev asset (usually too late for brand RFP).
 * @param {string} brand
 */
export function isBrandAlreadyChosen(brand) {
  const b = String(brand || "").trim();
  if (!b || isIndependentOrUnbranded(b)) return false;
  return true;
}

/**
 * @param {object} property
 * @returns {BrandDecisionTiming}
 */
export function classifyPropertyBrandDecisionTiming(property) {
  const brand = String(property.brandAffiliation || property.brand || "").trim();
  const propertyType = String(property.propertyType || "").trim();
  const buildingName = String(property.buildingName || "").trim();
  const yearBuilt = property.yearBuilt != null ? Number(property.yearBuilt) : null;
  const referenceYear = new Date().getFullYear();
  const cfg = MAP_BRANDING_DECISION_CONFIG;

  const isDevStatus =
    DEVELOPMENT_TYPE_RE.test(propertyType) ||
    DEVELOPMENT_NAME_RE.test(buildingName) ||
    DEVELOPMENT_TYPE_RE.test(String(property.builtRenovText || ""));

  const isRecentBuild =
    yearBuilt != null && referenceYear - yearBuilt <= cfg.newBuildRecentYears;

  if ((isDevStatus || isRecentBuild) && isBrandAlreadyChosen(brand)) {
    return "post_decision";
  }
  if ((isDevStatus || isRecentBuild) && isIndependentOrUnbranded(brand)) {
    return "pre_decision";
  }
  if (isIndependentOrUnbranded(brand)) return "pre_decision";
  if (brand) return "uncertain";
  return "uncertain";
}

/**
 * @param {object} property
 * @param {number} [referenceYear]
 * @param {{ ownerName?: string, icpSegment?: string }} [ownerContext]
 */
export function scorePropertyBrandingSignals(property, referenceYear = new Date().getFullYear(), ownerContext = {}) {
  const ownerName = ownerContext.ownerName || String(property.trueOwner || "").trim();
  const icpSegment = ownerContext.icpSegment || "";
  const brandDecisionEligible = isBrandDecisionEligibleProperty(property, { ownerName, icpSegment });
  const cfg = MAP_BRANDING_DECISION_CONFIG;
  const brand = String(property.brandAffiliation || property.brand || "").trim();
  const operator = String(property.hotelOperator || "").trim();
  const trueOwner = String(property.trueOwner || "").trim();
  const propertyType = String(property.propertyType || "").trim();
  const buildingName = String(property.buildingName || "").trim();
  const yearBuilt = property.yearBuilt != null ? Number(property.yearBuilt) : null;
  const yearRenovated = property.yearRenovated != null ? Number(property.yearRenovated) : null;
  const builtRenovText = String(property.builtRenovText || "").trim();

  /** @type {Array<{ id: BrandingSignalId, dealTrigger: string, label: string, weight: number, confidence: string, reason: string }>} */
  const signals = [];

  if (isIndependentOrUnbranded(brand)) {
    signals.push(signalHit("independent_unbranded", "No brand affiliation in CoStar"));
  } else if (SOFT_COLLECTION_BRAND_RE.test(brand)) {
    signals.push(
      signalHit("independent_unbranded", `Soft collection / independent-spirited brand: ${brand}`, "medium")
    );
  }

  const ageYears = yearBuilt ? referenceYear - yearBuilt : null;
  const renovAgeYears = yearRenovated ? referenceYear - yearRenovated : null;
  const recentlyRenovated = renovAgeYears != null && renovAgeYears <= cfg.recentRenovationYears;

  if (ageYears != null && ageYears >= cfg.conversionAgeYears && !recentlyRenovated) {
    signals.push(
      signalHit(
        "conversion_candidate",
        `Built ${yearBuilt} (${ageYears}y) without recent major renov`,
        isIndependentOrUnbranded(brand) ? "high" : "medium"
      )
    );
  }

  const isDevStatus =
    DEVELOPMENT_TYPE_RE.test(propertyType) ||
    DEVELOPMENT_NAME_RE.test(buildingName) ||
    DEVELOPMENT_TYPE_RE.test(builtRenovText);
  const isRecentBuild =
    yearBuilt != null && referenceYear - yearBuilt <= cfg.newBuildRecentYears;

  if (isDevStatus || isRecentBuild) {
    if (isBrandAlreadyChosen(brand)) {
      signals.push(
        signalHit(
          "recent_open_branded",
          `Brand already on record: ${brand} (${propertyType || yearBuilt || buildingName})`,
          "high",
          "post_decision"
        )
      );
    } else {
      signals.push(
        signalHit(
          "new_build",
          `Pre-brand development: ${propertyType || buildingName || `built ${yearBuilt}`}`,
          "high",
          "pre_decision"
        )
      );
    }
  }

  if (
    brand &&
    operator &&
    !isIndependentOrUnbranded(brand) &&
    isGenuineThirdPartyOperatorMismatch(trueOwner || ownerName, operator, brand, icpSegment)
  ) {
    signals.push(
      signalHit(
        "reflag_operator_mismatch",
        `Owner asset with third-party operator (${operator})`,
        KNOWN_THIRD_PARTY_OPERATOR_RE.test(operator) || isThirdPartyFranchiseBrand(brand, ownerName)
          ? "medium"
          : "low"
      )
    );
    if (KNOWN_THIRD_PARTY_OPERATOR_RE.test(operator)) {
      signals.push(signalHit("operator_rfp_proxy", `Operator field: ${operator}`, "low"));
    }
  }

  const anchorYear = yearRenovated || yearBuilt;
  const suppressHouseBrandRenewal =
    isIntegratedOperatorOwner(ownerName, icpSegment) && isHouseBrandForOwner(ownerName, brand);
  if (anchorYear && brand && !isIndependentOrUnbranded(brand) && !suppressHouseBrandRenewal) {
    const estimatedRenewalYear = anchorYear + cfg.typicalFranchiseTermYears;
    const monthsUntilRenewal = (estimatedRenewalYear - referenceYear) * 12;
    if (monthsUntilRenewal >= 0 && monthsUntilRenewal <= cfg.renewalWindowMonthsAhead) {
      signals.push(
        signalHit(
          "brand_renewal_window",
          `Heuristic renewal ~${estimatedRenewalYear} (${cfg.typicalFranchiseTermYears}y from ${anchorYear}) — NOT contract date`,
          "low"
        )
      );
    }
  }

  return {
    property,
    signals,
    propertyScore: signals.reduce((sum, s) => sum + s.weight, 0),
    primaryDealTrigger: pickPrimaryDealTrigger(signals),
    brandDecisionTiming: classifyPropertyBrandDecisionTiming(property),
    brandDecisionEligible,
    buildingName,
    brandAffiliation: brand,
    hotelOperator: operator,
    country: property.country || "",
    city: property.city || "",
  };
}

/**
 * @param {object[]} properties
 * @param {object} [options]
 * @param {number} [options.developmentPipelineCount] from company profile
 * @param {string} [options.ownerName]
 * @param {string} [options.icpSegment]
 */
export function scoreOwnerBrandingIntent(properties, options = {}) {
  const ownerContext = {
    ownerName: options.ownerName || "",
    icpSegment: options.icpSegment || "",
  };
  const calaProperties = (properties || []).filter((p) => p.country);
  const propertyScores = calaProperties.map((p) =>
    scorePropertyBrandingSignals(p, new Date().getFullYear(), ownerContext)
  );

  const eligiblePropertyScores = propertyScores.filter((p) => p.brandDecisionEligible);
  const scoringPool = eligiblePropertyScores.length ? eligiblePropertyScores : propertyScores;

  /** @type {Map<string, object>} */
  const signalRollup = new Map();
  for (const ps of scoringPool) {
    for (const sig of ps.signals) {
      const existing = signalRollup.get(sig.id);
      if (!existing) {
        signalRollup.set(sig.id, { ...sig, propertyCount: 1, examples: [ps.buildingName] });
      } else {
        existing.propertyCount++;
        if (existing.examples.length < 3 && ps.buildingName) {
          existing.examples.push(ps.buildingName);
        }
      }
    }
  }

  if (options.developmentPipelineCount > 0) {
    signalRollup.set("development_pipeline", {
      ...signalHit(
        "development_pipeline",
        `${options.developmentPipelineCount} asset(s) under development in CoStar company profile`,
        "medium"
      ),
      propertyCount: options.developmentPipelineCount,
      examples: [],
    });
  }

  const brands = new Set(
    scoringPool
      .map((p) => String(p.brandAffiliation || p.property?.brand || "").trim())
      .filter((b) => b && !isIndependentOrUnbranded(b))
  );
  const independentCount = scoringPool.filter((p) =>
    isIndependentOrUnbranded(p.brandAffiliation)
  ).length;

  if (brands.size >= 3) {
    signalRollup.set("portfolio_mixed_brands", {
      ...signalHit(
        "portfolio_mixed_brands",
        `${brands.size} distinct brands across CALA portfolio`,
        "medium"
      ),
      propertyCount: calaProperties.length,
      examples: [...brands].slice(0, 3),
    });
  }

  const signals = [...signalRollup.values()];
  const topProperties = scoringPool
    .filter((p) => p.signals.length > 0)
    .sort((a, b) => b.propertyScore - a.propertyScore)
    .slice(0, 5);

  let intentScore = signals.reduce((sum, s) => sum + s.weight, 0);
  if (independentCount >= 2) intentScore += 8;

  const primaryDealTrigger = pickPrimaryDealTrigger(signals);
  const confidence = resolveOverallConfidence(signals);
  const brandDecisionTiming = resolveOwnerBrandDecisionTiming(scoringPool, signals);
  const outreachTrack = classifyOwnerOutreachTrack(
    ownerContext.ownerName,
    ownerContext.icpSegment,
    propertyScores
  );

  return {
    ownerName: options.ownerName || "",
    intentScore: Math.min(100, intentScore),
    confidence,
    primaryDealTrigger,
    brandDecisionTiming,
    outreachTrack,
    brandDecisionEligiblePropertyCount: eligiblePropertyScores.length,
    houseBrandPropertyCount: propertyScores.length - eligiblePropertyScores.length,
    preDecisionPropertyCount: propertyScores.filter((p) => p.brandDecisionTiming === "pre_decision").length,
    postDecisionPropertyCount: propertyScores.filter((p) => p.brandDecisionTiming === "post_decision").length,
    signals,
    independentPropertyCount: independentCount,
    brandedPropertyCount: calaProperties.length - independentCount,
    distinctBrandCount: brands.size,
    topProperties: topProperties.map((p) => ({
      buildingName: p.buildingName,
      city: p.city,
      country: p.country,
      brandAffiliation: p.brandAffiliation,
      hotelOperator: p.hotelOperator,
      brandDecisionEligible: p.brandDecisionEligible,
      propertyScore: p.propertyScore,
      primaryDealTrigger: p.primaryDealTrigger,
      signalLabels: p.signals.map((s) => s.label),
    })),
  };
}

/**
 * Parse "N under development" from CoStar company profile internal notes.
 * @param {string} notes
 */
export function parseDevelopmentCountFromProfileNotes(notes) {
  const text = String(notes || "");
  const m = text.match(/(\d+)\s+(?:properties?\s+)?under development/i);
  if (m) return Number(m[1]);
  const m2 = text.match(/under development[^\d]*(\d+)/i);
  if (m2) return Number(m2[1]);
  return 0;
}

/**
 * @param {object} ownerRow
 * @param {object} brandingIntent from scoreOwnerBrandingIntent
 * @param {object} [contact]
 */
export function buildBrandingOutreachTarget(ownerRow, brandingIntent, contact = {}) {
  const cfg = MAP_BRANDING_DECISION_CONFIG;
  let outreachScore = brandingIntent.intentScore;
  const hasVerified = Boolean(contact.hasVerifiedContact);
  const email = String(contact.primaryContactEmail || contact.email || "").trim();
  const contactName = String(contact.primaryContactName || contact.name || "").trim();
  const title = String(contact.title || "").trim();
  const website = contact.website || "";
  const entityName = ownerRow.ownerName || "";

  const hasVerifiedPersonEmail =
    email &&
    isReachableCorporateEmail(email, website, entityName) &&
    isVerifiedPersonEmail(email, contactName);

  const businessPhone = String(contact.businessPhone || "").trim();
  const mobilePhone = String(contact.mobilePhone || "").trim();
  const phoneVerificationTier = String(contact.phoneVerificationTier || "").trim();
  const resolvedBusinessTier =
    contact.businessPhoneTier ||
    (phoneVerificationTier === "VP1" || phoneVerificationTier === "VP3" ? phoneVerificationTier : "");
  const resolvedMobileTier =
    contact.mobilePhoneTier || (phoneVerificationTier === "VP2" ? "VP2" : "");
  const hasVerifiedPersonPhone =
    isVerifiedPersonPhoneTier(resolvedMobileTier) || isVerifiedPersonPhoneTier(resolvedBusinessTier);
  const primaryPhone = pickPrimaryOutreachPhone({
    mobilePhone,
    mobileTier: resolvedMobileTier,
    businessPhone,
    businessTier: resolvedBusinessTier,
    phone: String(contact.primaryContactPhone || contact.phone || "").trim(),
  });
  const phonesDisplay = formatContactPhonesForDisplay({
    mobilePhone,
    mobilePhoneTier: resolvedMobileTier,
    businessPhone,
    businessPhoneTier: resolvedBusinessTier,
    phone: primaryPhone,
  });

  const hasReachableContact = hasVerified || hasVerifiedPersonEmail || hasVerifiedPersonPhone;
  if (hasVerified) outreachScore += cfg.verifiedContactBonus;
  else if (hasVerifiedPersonEmail) outreachScore += cfg.reachableContactBonus;
  if (hasVerifiedPersonPhone) outreachScore += cfg.verifiedPhoneBonus;

  const outreachReady =
    outreachScore >= cfg.outreachReadyMinScore &&
    (hasVerified || hasVerifiedPersonEmail || hasVerifiedPersonPhone) &&
    brandingIntent.primaryDealTrigger !== "none_known" &&
    brandingIntent.primaryDealTrigger !== "recent_open_branded" &&
    brandingIntent.brandDecisionTiming !== "post_decision" &&
    isBrandDecisionOutreachTrack(brandingIntent.outreachTrack || "asset_owner") &&
    (brandingIntent.brandDecisionEligiblePropertyCount || 0) > 0;

  return {
    ...brandingIntent,
    ownerTargetId: ownerRow.id || null,
    ownerName: ownerRow.ownerName || brandingIntent.ownerName,
    priorityTier: ownerRow.priorityTier || "C",
    icpSegment: ownerRow.icpSegment || "",
    calaPropertyCount: ownerRow.calaPropertyCount || 0,
    countriesSummary: ownerRow.countriesSummary || "",
    outreachScore: Math.min(100, outreachScore),
    outreachReady,
    dealalityFit: resolveDealalityFit(brandingIntent),
    contact: {
      name: contactName,
      email,
      phone: primaryPhone || "",
      businessPhone,
      mobilePhone,
      businessPhoneTier: resolvedBusinessTier,
      mobilePhoneTier: resolvedMobileTier,
      phoneVerificationTier,
      phonesDisplay,
      linkedIn: contact.linkedIn || "",
      verificationTier: contact.verificationTier || "",
      hasVerifiedContact: hasVerified,
      hasVerifiedPersonEmail,
      hasVerifiedPersonPhone,
      isNonPersonMailbox: email ? isNonPersonMailboxEmail(email) : false,
      isGenericMailbox: email ? isGenericMailboxEmail(email) : false,
    },
    pitchAngle: buildPitchAngle(brandingIntent),
    dataGaps: collectDataGaps(brandingIntent),
  };
}

function buildPitchAngle(intent) {
  const trigger = intent.primaryDealTrigger;
  const top =
    pickLeadProperty(intent.topProperties || [], intent.ownerName) ||
    intent.topProperties?.find((p) => p.brandDecisionEligible !== false) ||
    intent.topProperties?.[0];
  const asset = top?.buildingName ? `"${top.buildingName}"` : "portfolio assets";

  if (intent.outreachTrack === "integrated_operator_mixed") {
    return `Mixed portfolio — brand/franchise evaluation on third-party-flag and unbranded assets (lead: ${asset}).`;
  }
  if (intent.outreachTrack === "integrated_operator_house_brand_only") {
    return `House-brand operator — portfolio analytics only; not a third-party brand-decision target.`;
  }

  switch (trigger) {
    case "independent_unbranded":
      return `Independent CALA hotel owner — brand/franchise evaluation workspace for ${asset} and portfolio.`;
    case "conversion":
      return `Aging CALA asset(s) — conversion/repositioning and brand comparison on Dealality.`;
    case "reflag":
      return `Owner-operator separation on CALA assets — reflag/operator RFP decision support.`;
    case "new_build":
      return `Pre-brand development — brand/operator evaluation before flag commitment (${asset}).`;
    case "recent_open_branded":
      return `Brand likely already chosen on recent asset — prioritize other pre-decision properties in portfolio.`;
    case "development_pipeline":
      return `Active development pipeline — multi-project brand strategy and deal workspace.`;
    case "brand_renewal_window":
      return `Approaching estimated brand/management renewal window — evaluate alternatives before recommit.`;
    case "portfolio_standardization":
      return `Multi-brand portfolio — standardization and operator strategy across assets.`;
    case "operator_rfp":
      return `Third-party operated assets — operator selection and contract comparison.`;
    default:
      return "CALA hotel owner — brand and deal decision workspace on Dealality.";
  }
}

function collectDataGaps(intent) {
  /** @type {string[]} */
  const gaps = [];
  if (intent.signals.some((s) => s.id === "brand_renewal_window")) {
    gaps.push("Franchise expiry date unknown — renewal window is heuristic only");
  }
  if (!intent.signals.some((s) => s.id === "new_build" || s.id === "development_pipeline")) {
    gaps.push("Land purchase / pre-permit projects not in CoStar — add news/advisor signals");
  }
  return gaps;
}

function signalHit(id, reason, confidenceOverride, decisionTiming) {
  const def = BRANDING_SIGNAL_DEFINITIONS.find((d) => d.id === id);
  const timing =
    decisionTiming ||
    (id === "recent_open_branded"
      ? "post_decision"
      : ["new_build", "independent_unbranded", "development_pipeline", "brand_renewal_window"].includes(id)
        ? "pre_decision"
        : "uncertain");
  if (!def) {
    return {
      id,
      dealTrigger: "none_known",
      label: id,
      weight: 10,
      confidence: "low",
      decisionTiming: timing,
      reason,
    };
  }
  return {
    id: def.id,
    dealTrigger: def.dealTrigger,
    label: def.label,
    weight: def.weight,
    confidence: confidenceOverride || def.baseConfidence,
    decisionTiming: timing,
    reason,
  };
}

function pickPrimaryDealTrigger(signals) {
  if (!signals?.length) return "none_known";
  const preDecision = signals.filter(
    (s) => s.decisionTiming === "pre_decision" && s.dealTrigger !== "recent_open_branded"
  );
  const pool = preDecision.length ? preDecision : signals.filter((s) => s.dealTrigger !== "recent_open_branded");
  if (!pool.length) return "recent_open_branded";
  const sorted = [...pool].sort((a, b) => b.weight - a.weight);
  return sorted[0].dealTrigger || "none_known";
}

function resolveOwnerBrandDecisionTiming(propertyScores, ownerSignals) {
  const propTimings = new Set(propertyScores.map((p) => p.brandDecisionTiming));
  const hasPreSignal = ownerSignals.some((s) => s.decisionTiming === "pre_decision");
  const hasPostSignal = ownerSignals.some((s) => s.decisionTiming === "post_decision");

  if (propTimings.has("pre_decision") || hasPreSignal) {
    if (propTimings.has("post_decision") || hasPostSignal) return "mixed";
    return "pre_decision";
  }
  if (propTimings.has("post_decision") || hasPostSignal) return "post_decision";
  return "uncertain";
}

function resolveDealalityFit(intent) {
  if (intent.brandDecisionTiming === "post_decision") {
    return "late_for_brand_rfp";
  }
  if (intent.brandDecisionTiming === "pre_decision") {
    return "brand_decision_window";
  }
  if (intent.brandDecisionTiming === "mixed") {
    return "portfolio_mixed_timing";
  }
  if (intent.primaryDealTrigger === "conversion" || intent.primaryDealTrigger === "independent_unbranded") {
    return "brand_decision_window";
  }
  if (intent.primaryDealTrigger === "brand_renewal_window") {
    return "renewal_heuristic";
  }
  return "uncertain";
}

function resolveOverallConfidence(signals) {
  if (!signals.length) return "none";
  if (signals.some((s) => s.confidence === "high")) return "high";
  if (signals.some((s) => s.confidence === "medium")) return "medium";
  return "low";
}

function normalizeEntityKey(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Infer owner deal trigger from properties (respects manual override).
 * @param {object[]} properties
 * @param {object} [options]
 * @param {string} [options.existingDealTrigger]
 * @param {number} [options.developmentPipelineCount]
 * @param {string} [options.ownerName]
 */
export function inferOwnerDealTrigger(properties, options = {}) {
  if (options.existingDealTrigger && options.existingDealTrigger !== "none_known") {
    return options.existingDealTrigger;
  }
  const intent = scoreOwnerBrandingIntent(properties, {
    developmentPipelineCount: options.developmentPipelineCount,
    ownerName: options.ownerName,
    icpSegment: options.icpSegment,
  });
  return intent.primaryDealTrigger || "none_known";
}
