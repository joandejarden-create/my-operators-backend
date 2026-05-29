/**
 * Phase 5F — Company-level narrative packs from structured Operator Setup + scoring breakdown.
 * Prioritizes distinctive operator signals; neutral, owner-facing language only.
 */

import { normalizeOperatorAlignmentDealInputs } from "./operator-alignment-deal-normalize.js";
import { getOperatorSampleNarrativeMeta } from "./operator-alignment-operator-narrative-meta.js";

const BANNED_SUBSTRINGS = [
  "dealality recommends",
  "recommended operator",
  "best operator",
  "preferred operator",
  "the owner should select",
  "strongest path",
];

/** Rationales / lines that should not appear in WHAT SUPPORTS REVIEW */
const GENERIC_SUPPORT_PATTERNS = [
  /brand agreement is franchise/i,
  /evaluated separately from third-party/i,
  /fee\s*\/\s*commercial/i,
  /potential alignment on /i,
  /conditional alignment signal on /i,
  /limited alignment signal on /i,
  /market alignment is supported by documented country or city-level presence/i,
  /measures overlap between owner preferred brands/i,
  /operator profile theme \(.*\) is documented in sample metadata/i,
];

const BANNED_SUPPORT_PHRASES = [
  "fee / commercial assumptions may need validation",
  "validate open alignment factors before external sharing",
];

function toStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean).join(", ");
  return String(v).trim();
}

function toList(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean);
  const s = toStr(v);
  if (!s) return [];
  return s.split(/\s*,\s*/).map((x) => x.trim()).filter(Boolean);
}

function factorScore(f) {
  if (!f || f.score === "—" || f.score == null) return null;
  const n = Number(f.score);
  return Number.isNaN(n) ? null : n;
}

function listIncludes(arr, token) {
  const t = String(token || "").toLowerCase();
  return (arr || []).some((x) => String(x).toLowerCase().includes(t) || t.includes(String(x).toLowerCase()));
}

function joinList(items, max = 4) {
  const a = (items || []).filter(Boolean).slice(0, max);
  if (a.length <= 1) return a[0] || "";
  if (a.length === 2) return a[0] + " and " + a[1];
  return a.slice(0, -1).join(", ") + ", and " + a[a.length - 1];
}

function isGenericSupportLine(line) {
  const s = String(line || "").trim();
  if (!s) return true;
  const lower = s.toLowerCase();
  if (BANNED_SUPPORT_PHRASES.some((p) => lower.includes(p))) return true;
  return GENERIC_SUPPORT_PATTERNS.some((re) => re.test(s));
}

/**
 * @param {object} prefill
 * @param {object} [masterFields]
 */
export function extractOperatorStructuredProfile(prefill, masterFields = {}) {
  const p = prefill || {};
  const m = masterFields || {};
  return {
    activeCountries: toList(p.activeCountries),
    activeMarkets: toList(p.activeMarkets),
    marketPresenceType: toList(p.marketPresenceType),
    serviceModels: toList(p.serviceModelsSupported || p.serviceModels || p.service_models),
    chainScales: toList(p.chainScalesSupported || p.chainScale || p.chainScalesYouSupport),
    managementStructures: toList(p.managementStructuresSupported),
    offeredServices: toList(p.offeredServices),
    newBuildExperience: toStr(p.newBuildOpeningExperience),
    preOpeningCapability: toStr(p.preOpeningSupportCapability),
    ownerReporting: toStr(p.ownerReportingLevel),
    brandFamilies: toList(p.brandFamiliesOperated),
    fbCapability: toStr(p.fbCapabilityLevel),
    rmCapability: toStr(p.revenueManagementCapability),
    salesPlatform: toList(p.salesPlatform),
    governanceCadence: toStr(p.governanceCadence),
    dataConfidence: toStr(p.dataConfidenceLevel || m["Data Confidence Level"]),
    sourceType: toList(p.sourceType || m["Source Type"]),
  };
}

function marketLocationLabel(deal) {
  const city = deal.dealCity;
  const country = deal.dealCountry;
  if (city && country) return city + " / " + country;
  if (city) return city;
  if (country) return country;
  return "the deal market";
}

function hasExactMarketMatch(profile, deal) {
  if (deal.dealCity && listIncludes(profile.activeMarkets, deal.dealCity)) return true;
  if (deal.dealCountry && listIncludes(profile.activeCountries, deal.dealCountry)) {
    if (!deal.dealCity) return true;
    return listIncludes(profile.activeMarkets, deal.dealCity);
  }
  return false;
}

function hasCountryOnlyMatch(profile, deal) {
  return Boolean(deal.dealCountry && listIncludes(profile.activeCountries, deal.dealCountry));
}

function isResortOriented(profile) {
  const models = (profile.serviceModels || []).map((x) => String(x).toLowerCase());
  return models.some((m) => /resort|all-inclusive|luxury/i.test(m));
}

function isSelectServiceDeal(deal, profile) {
  const blob = [deal.dealScale, ...(deal.mustHaveOperatorServices || []), ...(profile.serviceModels || [])]
    .join(" ")
    .toLowerCase();
  return /select|upper midscale|focused service|airport/i.test(blob);
}

function collectSignalCandidates(profile, deal, breakdown, meta) {
  const candidates = [];
  const loc = marketLocationLabel(deal);

  const marketFootprint = joinList(
    profile.activeMarkets.length ? profile.activeMarkets : profile.activeCountries,
    3
  );

  const profileAccent =
    (meta && meta.label) ||
    (profile.serviceModels[0] ? profile.serviceModels[0] + " operating focus" : "") ||
    (profile.rmCapability && /advanced/i.test(profile.rmCapability) ? "commercial platform depth" : "");

  if (hasExactMarketMatch(profile, deal)) {
    const place =
      deal.dealCity && deal.dealCountry
        ? deal.dealCity + " and " + deal.dealCountry
        : deal.dealCity || deal.dealCountry;
    candidates.push({
      key: "marketExact",
      priority: 100,
      theme: place + " market presence",
      support:
        "Operator Setup documents " +
        marketFootprint +
        " for " +
        place +
        (profileAccent ? " (" + profileAccent + ")." : " alignment."),
    });
  } else if (hasCountryOnlyMatch(profile, deal)) {
    candidates.push({
      key: "marketCountry",
      priority: 72,
      theme: (deal.dealCountry || "regional") + " market presence",
      support:
        "Operator Setup documents " +
        marketFootprint +
        "; confirm " +
        (deal.dealCity || "city-level") +
        " operations within " +
        (deal.dealCountry || "the country") +
        ".",
    });
  } else if (profile.activeCountries.length || profile.activeMarkets.length) {
    candidates.push({
      key: "marketRegional",
      priority: 55,
      theme: "regional market coverage",
      support:
        "Regional presence is documented for " +
        joinList(profile.activeMarkets.length ? profile.activeMarkets : profile.activeCountries, 3) +
        "; confirm active operations for " +
        loc +
        ".",
      validation: "Confirm active market coverage in " + loc + ".",
    });
  }

  const mgmt = profile.managementStructures;
  if (mgmt.length && deal.operatingModel && /third-party/i.test(deal.operatingModel)) {
    const hasFull = listIncludes(mgmt, "full third-party");
    if (hasFull) {
      candidates.push({
        key: "management",
        priority: 82,
        theme: "third-party management scope",
        support:
          "Documented management structures (" +
          joinList(mgmt, 2) +
          ") align with the deal's third-party operating path.",
      });
    }
  }

  const offered = profile.offeredServices;
  const must = deal.mustHaveOperatorServices || [];
  if (offered.length && must.length) {
    const overlap = must.filter((m) =>
      offered.some((o) => o.toLowerCase() === m.toLowerCase() || o.toLowerCase().includes(m.toLowerCase()))
    );
    if (overlap.length >= 3) {
      const highlight = joinList(overlap, 2);
      const extra = offered.find(
        (o) => !overlap.some((x) => x.toLowerCase() === o.toLowerCase())
      );
      candidates.push({
        key: "services",
        priority: 80,
        theme: "must-have service overlap",
        support:
          "Offered Services emphasize " +
          highlight +
          (extra ? " plus " + extra : "") +
          " for this deal's must-haves.",
      });
    } else if (overlap.length >= 1) {
      candidates.push({
        key: "servicesPartial",
        priority: 62,
        theme: "partial service overlap",
        support:
          "Offered Services include " +
          joinList(overlap, 3) +
          "; confirm remaining must-have scope.",
        validation: "Confirm service delivery scope for owner-required must-have services.",
      });
    }
  }

  const cap = profile.preOpeningCapability;
  const needsPre =
    /yes/i.test(deal.preOpeningSupportNeeded || "") ||
    /new build/i.test(deal.dealProjectType || "") ||
    /pre-development|under construction/i.test(deal.openingTimeline || "");

  if (needsPre && cap) {
    if (/advanced|strong/i.test(cap)) {
      candidates.push({
        key: "preOpeningStrong",
        priority: 78,
        theme: "pre-opening support",
        support:
          "Pre-opening capability is documented as " +
          cap +
          " for a new-build / pre-development project.",
      });
    } else if (/standard|moderate/i.test(cap)) {
      candidates.push({
        key: "preOpeningModerate",
        priority: 65,
        theme: "pre-opening support",
        support: "Operator Setup documents " + cap + " pre-opening capability for this project stage.",
        validation: "Validate pre-opening support scope and staffing responsibilities.",
      });
    } else if (/limited|none documented/i.test(cap)) {
      candidates.push({
        key: "preOpeningLimited",
        priority: 92,
        theme: "pre-opening validation",
        validation: "Validate pre-opening support scope for a new-build / pre-development project (documented as " + cap + ").",
        weaken: "Alignment may weaken if pre-opening support is limited for a new-build project.",
      });
    }
  }

  if (profile.ownerReporting && deal.ownerReportingExpectations) {
    if (/institutional/i.test(profile.ownerReporting)) {
      candidates.push({
        key: "reportingInstitutional",
        priority: 72,
        theme: "institutional reporting",
        support: "Owner reporting is documented as " + profile.ownerReporting + ".",
        validation:
          /monthly/i.test(deal.ownerReportingExpectations)
            ? "Confirm institutional reporting cadence vs monthly owner expectation."
            : "Confirm institutional reporting cadence for this deal.",
      });
    } else if (/monthly/i.test(profile.ownerReporting) && /monthly/i.test(deal.ownerReportingExpectations)) {
      candidates.push({
        key: "reportingMonthly",
        priority: 68,
        theme: "owner reporting cadence",
        support: "Monthly owner reporting capability aligns with the deal's operating review expectation.",
      });
    }
  }

  if (/advanced centralized|advanced/i.test(profile.rmCapability)) {
    candidates.push({
      key: "commercial",
      priority: 76,
      theme: "revenue management capability",
      support:
        "Revenue management is documented as " +
        profile.rmCapability +
        (profile.salesPlatform.length ? " with " + joinList(profile.salesPlatform, 2) + " sales support." : "."),
    });
  } else if (/centralized/i.test(profile.rmCapability)) {
    candidates.push({
      key: "commercialModerate",
      priority: 58,
      theme: "commercial platform depth",
      support: "Centralized revenue management capability is documented in Operator Setup.",
    });
  }

  if (profile.fbCapability && /significant|lifestyle|full/i.test(profile.fbCapability)) {
    candidates.push({
      key: "fb",
      priority: 60,
      theme: "F&B operating capability",
      support: "F&B capability is documented as " + profile.fbCapability + ".",
    });
  }

  if (isResortOriented(profile) && isSelectServiceDeal(deal, profile)) {
    candidates.push({
      key: "resortMismatch",
      priority: 95,
      theme: "service model fit",
      validation:
        "Validate fit between resort/all-inclusive capabilities and this select-service project.",
      weaken:
        "Alignment may weaken if resort-oriented operating capabilities are less relevant to a select-service airport hotel.",
    });
  } else if (profile.serviceModels.length) {
    const selectModel = profile.serviceModels.some((m) => /select|upper midscale|focused/i.test(m));
    if (selectModel && isSelectServiceDeal(deal, profile)) {
      candidates.push({
        key: "selectModel",
        priority: 70,
        theme: "select-service capability",
        support:
          "Service models include " +
          joinList(
            profile.serviceModels.filter((m) => /select|upper midscale|focused/i.test(m)),
            2
          ) +
          " per Operator Setup.",
      });
    }
  }

  if (profile.brandFamilies.length && deal.dealPreferredBrands?.length) {
    const overlap = (deal.dealPreferredBrands || []).filter((b) =>
      profile.brandFamilies.some(
        (bf) =>
          String(bf).toLowerCase().includes(String(b).toLowerCase()) ||
          String(b).toLowerCase().includes(String(bf).toLowerCase())
      )
    );
    if (overlap.length) {
      candidates.push({
        key: "brandFamilies",
        priority: 64,
        theme: "brand family relevance",
        support: "Documented experience with " + joinList(overlap.length ? overlap : profile.brandFamilies, 2) + " brand families.",
      });
    }
  }

  if (
    deal.brandAgreementStructure &&
    /franchise/i.test(deal.brandAgreementStructure) &&
    deal.operatingModel &&
    /third-party/i.test(deal.operatingModel)
  ) {
    candidates.push({
      key: "franchiseSplit",
      priority: 45,
      validation: "Confirm brand/operator responsibility split under a franchise + third-party management path.",
    });
  }

  if (deal.dealCountry && !listIncludes(profile.activeCountries, deal.dealCountry)) {
    candidates.push({
      key: "marketGap",
      priority: 90,
      validation: "Confirm active market coverage in " + loc + ".",
      weaken:
        "Alignment may weaken if " +
        (deal.dealCountry || "target market") +
        " coverage reflects regional reach rather than active local operations.",
    });
  }

  const feeSc = factorScore(breakdown.feeCommercial);
  if (feeSc != null && feeSc < 80) {
    candidates.push({
      key: "feeCommercial",
      priority: 70,
      validation: "Confirm fee/commercial assumptions if relevant data is incomplete.",
    });
  }

  if (profile.dataConfidence === "Inferred") {
    candidates.push({
      key: "dataConfidence",
      priority: 50,
      validation:
        "Operator Setup data is documented as Inferred; confirm key profile fields before external sharing.",
    });
  }

  if (meta?.label && candidates.filter((c) => c.support).length < 2) {
    candidates.push({
      key: "meta",
      priority: 35,
      theme: meta.label.replace(/ specialist$| operator$| platform$/i, "").trim() || meta.label,
      support: "Operator profile reflects a " + meta.label + " operating focus in sample metadata.",
    });
  }

  return candidates.sort((a, b) => b.priority - a.priority);
}

function pickTopLines(candidates, field, max, { excludeGeneric = false } = {}) {
  const out = [];
  const seen = new Set();
  for (const c of candidates) {
    const line = c[field];
    if (!line) continue;
    const t = String(line).trim();
    if (!t || seen.has(t)) continue;
    if (excludeGeneric && isGenericSupportLine(t)) continue;
    seen.add(t);
    out.push(t);
    if (out.length >= max) break;
  }
  return out;
}

function pickLeadThemes(candidates, max = 3) {
  const themes = [];
  const seen = new Set();
  for (const c of candidates) {
    if (!c.theme || c.validation && !c.support) continue;
    const t = String(c.theme).trim();
    if (!t || seen.has(t)) continue;
    seen.add(t);
    themes.push(t);
    if (themes.length >= max) break;
  }
  return themes;
}

function buildOwnerFacingRationale(companyName, band, candidates) {
  const themes = pickLeadThemes(candidates, 3);
  const resortNote = candidates.find((c) => c.key === "resortMismatch");
  const preOpeningLimited = candidates.find((c) => c.key === "preOpeningLimited");

  let s =
    companyName +
    " currently shows " +
    (band || "alignment signals") +
    " based on available Operator Setup data.";

  if (themes.length) {
    s += " Stronger alignment signals appear around " + joinList(themes, 3) + ".";
  }

  if (resortNote) {
    s += " Resort-oriented operating coverage should be validated against the select-service airport profile.";
  } else if (preOpeningLimited) {
    s += " Pre-opening support scope should be validated for the new-build project timeline.";
  }

  s += " Before outreach, validate the highest-priority open items for this operator profile.";
  return s;
}

function pickKeyConsideration(candidates, profile, deal, breakdown) {
  const has = (key) => candidates.some((c) => c.key === key);
  const loc = marketLocationLabel(deal);

  if (has("resortMismatch")) {
    return "Validate fit between resort/all-inclusive capabilities and select-service project.";
  }
  if (has("preOpeningLimited") || has("preOpeningModerate")) {
    return "Validate pre-opening support scope.";
  }
  if (has("marketGap")) {
    return "Confirm active market coverage in " + loc + ".";
  }
  if (has("servicesPartial")) {
    return "Confirm service platform depth for must-have services.";
  }
  if (has("reportingInstitutional")) {
    return "Confirm institutional reporting cadence.";
  }
  if (has("commercial") && /advanced/i.test(profile.rmCapability)) {
    return "Confirm commercial platform depth for revenue management delivery.";
  }
  if (has("marketExact")) {
    return "Confirm active operations in " + loc + ".";
  }
  if (has("marketCountry")) {
    return "Confirm " + (deal.dealCity || "local") + " operations within " + (deal.dealCountry || "the market") + ".";
  }
  if (has("feeCommercial")) {
    return "Confirm fee/commercial assumptions.";
  }
  if (has("franchiseSplit")) {
    return "Confirm brand/operator responsibility split.";
  }
  if (has("marketRegional")) {
    return "Confirm active market coverage in " + loc + ".";
  }

  const stageScore = factorScore(breakdown.assetProjectStageFit);
  if (stageScore != null && stageScore < 65) {
    return "Validate pre-opening / project-stage fit for opening support scope.";
  }
  const svcScore = factorScore(breakdown.serviceOfferings);
  if (svcScore != null && svcScore < 70) {
    return "Confirm service platform depth for must-have services.";
  }
  if (hasExactMarketMatch(profile, deal) || hasCountryOnlyMatch(profile, deal)) {
    return "Confirm active operations in " + loc + ".";
  }
  return "Validate alignment signals against deal intake before outreach.";
}

function pickReviewStatus(band, candidates) {
  if (/insufficient/i.test(band)) return "Needs more operator profile data";
  const top = candidates[0];
  if (top?.key === "preOpeningLimited") return "Review pre-opening scope before outreach";
  if (top?.key === "resortMismatch") return "Review service model fit for project type";
  if (top?.key === "marketGap") return "Review market coverage before outreach";
  if (/strong/i.test(band) && candidates.some((c) => c.key === "marketExact")) {
    return "May merit review; confirm active market operations";
  }
  if (/moderate/i.test(band) && candidates.some((c) => c.key === "preOpeningLimited")) {
    return "Review pre-opening capability before outreach";
  }
  if (/conditional/i.test(band)) return "Review if owner confirms operating path";
  if (/limited/i.test(band)) return "Review if management structure aligns";
  if (/moderate|strong/i.test(band)) return "May merit review based on available Operator Setup data";
  return "Review if owner confirms operating path";
}

function buildFactorsReviewed(breakdown, candidates) {
  const labels = [];
  const keyToLabel = {
    marketExact: "Geography / market alignment",
    marketCountry: "Geography / market alignment",
    management: "Deal structure alignment",
    services: "Service platform alignment",
    preOpeningStrong: "Project / stage alignment",
    preOpeningLimited: "Project / stage alignment",
    commercial: "Service platform alignment",
    reportingInstitutional: "Owner reporting alignment",
    resortMismatch: "Service platform alignment",
    franchiseSplit: "Deal structure alignment",
  };
  for (const c of candidates) {
    const label = keyToLabel[c.key];
    if (label && labels.indexOf(label) < 0) labels.push(label);
    if (labels.length >= 6) break;
  }
  if (labels.length < 3) {
    const catalog = [
      { key: "geographyMarkets", label: "Geography / market alignment" },
      { key: "dealStructureAssignment", label: "Deal structure alignment" },
      { key: "serviceOfferings", label: "Service platform alignment" },
      { key: "assetProjectStageFit", label: "Project / stage alignment" },
    ];
    for (const { key, label } of catalog) {
      const sc = factorScore(breakdown[key]);
      if (sc != null && sc >= 50 && labels.indexOf(label) < 0) labels.push(label);
    }
  }
  return labels.slice(0, 6);
}

function buildOwnerQuestions(profile, deal, candidates) {
  const questions = [];
  const loc = marketLocationLabel(deal);
  if (profile.activeCountries.length || profile.activeMarkets.length) {
    questions.push("Does documented presence in " + loc + " reflect active operations for this deal?");
  }
  if (candidates.some((c) => /preOpening/i.test(c.key)) && profile.preOpeningCapability) {
    questions.push(
      "What pre-opening services would apply given documented " + profile.preOpeningCapability + " capability?"
    );
  }
  if (profile.offeredServices.length && (deal.mustHaveOperatorServices || []).length) {
    questions.push(
      "Which documented services (" +
        joinList(profile.offeredServices, 3) +
        ") would be in scope against deal must-haves?"
    );
  }
  if (profile.rmCapability && /advanced|centralized/i.test(profile.rmCapability)) {
    questions.push("How would " + profile.rmCapability + " revenue management support this project?");
  }
  if (isResortOriented(profile)) {
    questions.push("Is resort/all-inclusive operating experience relevant to this select-service project?");
  }
  if (profile.ownerReporting) {
    questions.push("How does " + profile.ownerReporting + " align with owner reporting expectations?");
  }
  return questions.slice(0, 5);
}

/**
 * @param {object} params
 */
export function buildOperatorNarrativePack(params) {
  const companyName = toStr(params.companyName) || "This operator";
  const profile = extractOperatorStructuredProfile(params.prefill, params.masterFields);
  const deal = normalizeOperatorAlignmentDealInputs(
    params.dealFields,
    params.locationData,
    params.mpData,
    params.siData
  );
  const breakdown = params.breakdownDetails || {};
  const meta = params.operatorId ? getOperatorSampleNarrativeMeta(params.operatorId) : null;
  const band = params.alignmentBand || "alignment signals";

  const candidates = collectSignalCandidates(profile, deal, breakdown, meta);

  const whatSupportsReview = pickTopLines(candidates, "support", 4, { excludeGeneric: true });
  const whatNeedsValidation = pickTopLines(candidates, "validation", 4);
  const whatCouldWeakenAlignment = pickTopLines(candidates, "weaken", 4);

  if (!whatCouldWeakenAlignment.length) {
    if (candidates.some((c) => c.key === "commercial") && !candidates.some((c) => c.key === "management")) {
      whatCouldWeakenAlignment.push(
        "Alignment may weaken if the owner requires full operating accountability beyond commercial support."
      );
    }
  }

  const ownerFacingRationale = buildOwnerFacingRationale(companyName, band, candidates);
  const keyConsideration = pickKeyConsideration(candidates, profile, deal, breakdown);
  const reviewStatusLabel = pickReviewStatus(band, candidates);
  const ownerQuestions = buildOwnerQuestions(profile, deal, candidates);
  const factorsReviewed = buildFactorsReviewed(breakdown, candidates);

  const alignmentSignals = [...whatSupportsReview.slice(0, 3), ...whatNeedsValidation.slice(0, 2)].slice(0, 5);
  const reviewConsiderations = whatNeedsValidation.slice(0, 4);

  return {
    ownerFacingRationale: sanitizeNarrativeText(ownerFacingRationale),
    whatSupportsReview: whatSupportsReview.map(sanitizeNarrativeText),
    whatNeedsValidation: whatNeedsValidation.map(sanitizeNarrativeText),
    whatCouldWeakenAlignment: whatCouldWeakenAlignment.map(sanitizeNarrativeText),
    ownerQuestions: ownerQuestions.map(sanitizeNarrativeText),
    keyConsideration: sanitizeNarrativeText(keyConsideration),
    reviewStatusLabel: sanitizeNarrativeText(reviewStatusLabel),
    factorsReviewed,
    alignmentSignals: alignmentSignals.map(sanitizeNarrativeText),
    reviewConsiderations: reviewConsiderations.map(sanitizeNarrativeText),
    operatorStructuredProfile: profile,
    sampleNarrativeMeta: meta,
  };
}

export function sanitizeNarrativeText(text) {
  let s = String(text || "").trim();
  for (const bad of BANNED_SUBSTRINGS) {
    if (s.toLowerCase().includes(bad)) {
      s = s.replace(new RegExp(bad, "gi"), "[removed]");
    }
  }
  return s.replace(/\s{2,}/g, " ").trim();
}

export { BANNED_SUBSTRINGS, BANNED_SUPPORT_PHRASES, GENERIC_SUPPORT_PATTERNS };
