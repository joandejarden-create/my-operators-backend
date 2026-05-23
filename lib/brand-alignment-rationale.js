/**
 * Business-readable brand review narratives for Brand Alignment Snapshot.
 * Separates Main Alignment Signals (executive) from Potential Alignment Signals (factor view, no methodology text).
 */

export const COMMON_QUESTIONS_BEFORE_OUTREACH = [
  "What level of brand standards / PIP scope is realistic for the owner?",
  "Is the owner seeking brand affiliation only, operator involvement, or both?",
  "How important are loyalty, distribution, and brand awareness relative to flexibility?",
  "Does the owner prefer soft brand, hard brand, or both?",
  "What level of local identity or design independence should be preserved?",
  "Are commercial incentives important to the owner, and what key money / incentive assumptions must be confirmed directly with the brand?",
  "What market/competitive presence should be reviewed before outreach?",
];

/** Owner-readable labels for executive summaries (not raw factor keys). */
const BUSINESS_SIGNAL_LABELS = {
  chainScaleProximity: "target positioning and chain scale",
  projectTypeCompatibility: "project type fit",
  projectStageCompatibility: "development stage timing",
  brandStandardsCompatibility: "standards and program expectations",
  agreementsTypeCompatibility: "deal structure",
  preferredBrand: "owner preference alignment",
  serviceModelAlignment: "operating model fit",
  buildingTypeCompatibility: "building type / asset form",
  roomRangeFitCompatibility: "room count",
  keyMoneyWillingnessCompatibility: "commercial incentive assumptions",
  incentiveAlignment: "commercial incentive assumptions",
  footprintRegionAlignment: "market and footprint context",
  feeStructureAlignment: "fee structure",
  sustainabilityAlignment: "sustainability / ESG inputs",
};

const FACTOR_ORDER = [
  "chainScaleProximity",
  "projectTypeCompatibility",
  "projectStageCompatibility",
  "brandStandardsCompatibility",
  "agreementsTypeCompatibility",
  "preferredBrand",
  "serviceModelAlignment",
  "buildingTypeCompatibility",
  "roomRangeFitCompatibility",
  "keyMoneyWillingnessCompatibility",
  "incentiveAlignment",
  "footprintRegionAlignment",
  "feeStructureAlignment",
  "sustainabilityAlignment",
];

const SIGNAL_LABELS = {
  chainScaleProximity: "Chain scale alignment",
  serviceModelAlignment: "Operator model compatibility",
  preferredBrand: "Owner priority alignment",
  projectTypeCompatibility: "Project type compatibility",
  buildingTypeCompatibility: "Building type compatibility",
  projectStageCompatibility: "Development stage alignment",
  brandStandardsCompatibility: "Standards / flexibility alignment",
  agreementsTypeCompatibility: "Deal structure alignment",
  incentiveAlignment: "Commercial incentive alignment",
  keyMoneyWillingnessCompatibility: "Commercial incentive alignment",
  roomRangeFitCompatibility: "Room range fit",
  sustainabilityAlignment: "Sustainability / ESG relevance",
  feeStructureAlignment: "Fee structure alignment",
  footprintRegionAlignment: "Market/region relevance",
};

function strVal(v) {
  if (v == null || v === "") return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  if (Array.isArray(v)) {
    return v
      .map((x) => (typeof x === "string" ? x : x && x.name ? String(x.name) : ""))
      .filter(Boolean)
      .join(", ");
  }
  if (typeof v === "object" && v.name) return String(v.name).trim();
  return String(v).trim();
}

function fieldPresent(fields, names) {
  for (const n of names) {
    const v = strVal(fields[n]);
    if (v) return v;
  }
  return "";
}

function joinNaturalList(items) {
  if (!items?.length) return "";
  if (items.length === 1) return items[0];
  if (items.length === 2) return items[0] + " and " + items[1];
  return items.slice(0, -1).join(", ") + ", and " + items[items.length - 1];
}

function capitalizeLead(text) {
  const t = String(text || "").trim();
  if (!t) return t;
  return t.charAt(0).toUpperCase() + t.slice(1);
}

export function signalAssessment(score) {
  if (score === "—" || score == null || score === "") return "Not enough data";
  const n = Number(score);
  if (!Number.isFinite(n)) return "Not enough data";
  if (n >= 85) return "Strong signal";
  if (n >= 65) return "Moderate signal";
  if (n >= 45) return "Needs validation";
  if (n >= 25) return "Limited signal";
  return "Not enough data";
}

function isConversionDeal(projectType) {
  const pt = String(projectType || "").toLowerCase();
  return /conversion|reposition|reflag|rebrand|affiliation change/i.test(pt);
}

function isMethodologyText(text) {
  const t = String(text || "");
  return (
    /Compares |Must match|Scores full|scores higher|one tier apart|Weight \d|calculation/i.test(t) ||
    /Closer alignment scores|Filter out brands without key money/i.test(t)
  );
}

export function classifyAlignmentFactors(breakdownNewDetails) {
  const strong = [];
  const weak = [];
  let missing = 0;
  for (const key of FACTOR_ORDER) {
    const row = breakdownNewDetails?.[key];
    if (!row || row.score == null || row.score === "") {
      missing += 1;
      continue;
    }
    const a = signalAssessment(row.score);
    if (a === "Strong signal" || a === "Moderate signal") strong.push(key);
    else weak.push(key);
  }
  return { strong, weak, missing };
}

function sortStrongFactorsByScore(breakdownNewDetails, strongKeys) {
  return [...strongKeys].sort((a, b) => {
    const sa = Number(breakdownNewDetails?.[a]?.score);
    const sb = Number(breakdownNewDetails?.[b]?.score);
    const na = Number.isFinite(sa) ? sa : -1;
    const nb = Number.isFinite(sb) ? sb : -1;
    return nb - na;
  });
}

/** Brand reference fields used for differentiated owner-facing copy. */
export function extractBrandProfile(brandName, brandData, parentCompany) {
  const basics = brandData?.brandBasics || {};
  const fit = brandData?.brandFit || {};
  const acceptableRaw = fit["Acceptable Project Type"];
  let acceptableProjectTypes = "";
  if (Array.isArray(acceptableRaw)) {
    acceptableProjectTypes = acceptableRaw
      .map((x) => (typeof x === "string" ? x : x?.name || ""))
      .filter(Boolean)
      .join(", ");
  } else {
    acceptableProjectTypes = strVal(acceptableRaw);
  }
  return {
    brandName: String(brandName || "").trim(),
    parentCompany: strVal(parentCompany || basics["Parent Company"]) || "",
    chainScale: strVal(basics["Hotel Chain Scale"] || basics["Chain Scale"]),
    serviceModel: strVal(basics["Hotel Service Model"]),
    brandModel: strVal(basics["Brand Model / Format"] || basics["Brand Model"]),
    softCollection: strVal(fit["Soft/Collection Brand"] || fit["Soft/Collection brand"]),
    acceptableProjectTypes,
    positioning: strVal(basics["Brand Positioning"] || basics["Target Guest Segment"] || basics["Positioning"]),
  };
}

/** Name-first pathway rules (overrides generic model/scale inference). */
const BRAND_PATHWAY_BY_NAME = [
  { test: /curio collection|tapestry by hilton|autograph collection|unbound collection|handpicked hotels|tribute portfolio/i, pathway: "collection" },
  { test: /radisson red|hyatt centric|joie de vivre|bunkhouse|andaz|moxy|edition hotels|motto by hilton|graduate hotels|mgallery|hotel indigo/i, pathway: "lifestyle" },
  { test: /^so$/i, pathway: "lifestyle" },
  { test: /ritz|st\.?\s*regis|waldorf|four seasons|peninsula|mandarin|raffles|sofitel|bulgari/i, pathway: "luxury" },
  { test: /canopy by hilton|cambria|homewood|home2|hampton|holiday inn express|courtyard|fairfield|aloft|four points/i, pathway: "upscale_hard" },
];

function inferBrandPathway(brandName, brandData, mergedFields) {
  const n = String(brandName || "").trim();
  const nl = n.toLowerCase();
  const profile = extractBrandProfile(brandName, brandData, "");
  const model = profile.brandModel.toLowerCase();
  const scale = profile.chainScale.toLowerCase();
  const softFlag = profile.softCollection.toLowerCase();

  for (const rule of BRAND_PATHWAY_BY_NAME) {
    if (rule.test.test(n)) return rule.pathway;
  }

  if (/^yes|true|soft|collection/i.test(softFlag) || /collection|soft brand|affiliation/i.test(model)) {
    if (/lifestyle|experiential|design/i.test(model) || /lifestyle|centric|joie|bunkhouse|red\b/i.test(nl)) {
      return "lifestyle";
    }
    return "collection";
  }
  if (/lifestyle|experiential|design-led|boutique/i.test(model) || /lifestyle|experiential/i.test(scale)) {
    return "lifestyle";
  }
  if (/luxury/i.test(scale) || /luxury/i.test(model)) return "luxury";
  if (/upper upscale|upscale|premium|midscale/i.test(scale)) return "upscale_hard";
  return "general_hard";
}

function brandIdentitySupportBullet(profile, pathway) {
  const { brandName, parentCompany, chainScale, serviceModel, brandModel } = profile;
  const parent = parentCompany ? ` (${parentCompany})` : "";
  const scaleClause = chainScale ? ` in the ${chainScale.toLowerCase()} chain-scale band` : "";
  const svcClause = serviceModel ? ` with a ${serviceModel.toLowerCase()} service model` : "";

  if (/curio collection/i.test(brandName)) {
    return (
      `${brandName}${parent} is Hilton's collection-style upper-upscale path${scaleClause}, where a differentiated property story may remain visible while using Hilton distribution and loyalty support.`
    );
  }
  if (/radisson red/i.test(brandName)) {
    return (
      `${brandName}${parent} is a lifestyle-oriented Radisson path${scaleClause}${svcClause}, which may merit review if the owner wants a more expressive, design-forward guest experience within the Radisson system.`
    );
  }
  if (/^so$/i.test(brandName.trim())) {
    return (
      `${brandName}${parent} is Accor's lifestyle-oriented upper-upscale path${scaleClause}, which may merit review where experiential positioning and F&B/public-space programming are part of the owner brief.`
    );
  }
  if (/hyatt centric/i.test(brandName)) {
    return (
      `${brandName}${parent} is Hyatt's lifestyle-oriented path${scaleClause}, which may merit review if the owner wants a locally rooted, design-aware guest experience under Hyatt's system.`
    );
  }
  if (/joie de vivre/i.test(brandName)) {
    return (
      `${brandName}${parent} is Hyatt's boutique-lifestyle path${scaleClause}, which may merit review where a distinctive local narrative and experiential positioning matter to the owner.`
    );
  }
  if (/bunkhouse/i.test(brandName)) {
    return (
      `${brandName}${parent} is Hyatt's experiential lifestyle path${scaleClause}, which may merit review if the owner is open to a more informal, design-led guest experience and programming mix.`
    );
  }

  switch (pathway) {
    case "collection":
      return (
        `${brandName}${parent} operates as a collection-style affiliation path${scaleClause}${svcClause}, which may merit review if the owner wants distribution support while preserving property identity.`
      );
    case "lifestyle":
      return (
        `${brandName}${parent} is positioned as a lifestyle-oriented path${scaleClause}${svcClause}, which may merit review where design, F&B, and experiential programming are important to the owner.`
      );
    case "luxury":
      return (
        `${brandName}${parent} sits in an upper-tier brand band${scaleClause}${svcClause}; capital, service, and approval expectations should be validated before outreach.`
      );
    case "upscale_hard":
      return (
        `${brandName}${parent} reflects a more defined upscale brand system${scaleClause}${svcClause}, which may merit review where standardized operating guidance and distribution support are priorities.`
      );
    default:
      return (
        `${brandName}${parent} may merit review on current deal inputs${scaleClause}${svcClause}; owner/advisor validation of standards, structure, and pathway fit still applies.`
      );
  }
}

function brandReviewHook(profile, pathway, tier, weakKeys) {
  const name = profile.brandName;
  const validateHint =
    weakKeys.includes("buildingTypeCompatibility") && weakKeys.includes("serviceModelAlignment")
      ? "building type and operating model"
      : weakKeys.includes("buildingTypeCompatibility")
        ? "building type / asset form"
        : weakKeys.includes("serviceModelAlignment")
          ? "operating model fit"
          : weakKeys.includes("preferredBrand")
            ? "owner priority alignment"
            : weakKeys.includes("chainScaleProximity")
              ? "positioning versus the brand's typical chain scale"
              : "brand standards and commercial structure";

  if (/curio collection/i.test(name)) {
    return `Hilton collection path may be relevant if the owner wants Curio's upper-upscale affiliation flexibility; confirm ${validateHint} before outreach`;
  }
  if (/radisson red/i.test(name)) {
    return `Radisson RED's lifestyle positioning may suit a more expressive upper-upscale product; validate ${validateHint} and F&B/public-space expectations before outreach`;
  }
  if (/^so$/i.test(name.trim())) {
    return `Accor's So lifestyle path may merit review for experiential upper-upscale positioning; confirm ${validateHint} and programming assumptions before outreach`;
  }
  if (/hyatt centric/i.test(name)) {
    return `Hyatt Centric may merit review for a locally rooted lifestyle path; validate ${validateHint} and design/F&B expectations before outreach`;
  }
  if (/joie de vivre/i.test(name)) {
    return `Joie de Vivre may merit review where boutique lifestyle storytelling matters; confirm ${validateHint} before outreach`;
  }
  if (/bunkhouse/i.test(name)) {
    return `Bunkhouse may merit review for an informal experiential path; validate ${validateHint} and programming fit before outreach`;
  }

  if (pathway === "collection") {
    return `${name} may be relevant as a collection-style path; confirm ${validateHint} before outreach`;
  }
  if (pathway === "lifestyle") {
    return `${name} may merit review on lifestyle-oriented positioning; validate ${validateHint} and programming expectations before outreach`;
  }
  if (tier === "Moderate Alignment Signal") {
    return `${name} shows moderate alignment on current inputs; validate ${validateHint} before outreach`;
  }
  if (tier === "Conditional Review Signal" || tier === "Lower Alignment Signal") {
    return `${name} shows conditional alignment; clarify ${validateHint} before outreach`;
  }
  return `${name} may merit review on current inputs; confirm ${validateHint} before outreach`;
}

function mainSignalBullet(factorKey, deal, source, preferredBrandNames, profile) {
  const brandName = profile.brandName;
  const pos = deal?.targetPositioning && deal.targetPositioning !== "—" ? deal.targetPositioning : "";
  const pt = deal?.projectType ? String(deal.projectType).trim() : "";
  const keys = deal?.keyCount != null && Number.isFinite(deal.keyCount) ? deal.keyCount : null;
  const brandScale = profile.chainScale;
  const acceptable = profile.acceptableProjectTypes;
  const isPreferred =
    source === "owner_preferred" ||
    (preferredBrandNames || []).some(
      (p) => String(p).trim().toLowerCase() === String(brandName || "").trim().toLowerCase()
    );

  switch (factorKey) {
    case "chainScaleProximity":
      if (pos && brandScale) {
        return `For ${brandName}, the deal's ${pos} target appears directionally aligned with the brand's ${brandScale} chain-scale tier.`;
      }
      return pos
        ? `For ${brandName}, target positioning (${pos}) appears directionally aligned with available brand chain-scale data.`
        : `For ${brandName}, target positioning appears directionally aligned with available brand chain-scale data.`;
    case "projectTypeCompatibility":
      if (pt && acceptable) {
        return `For ${brandName}, the ${pt} project type appears compatible with the brand's accepted project pathways (${acceptable}).`;
      }
      return pt
        ? `For ${brandName}, the ${pt} project type appears compatible with available brand project-fit data.`
        : `For ${brandName}, project type appears compatible with available brand reference data.`;
    case "projectStageCompatibility":
      return `For ${brandName}, development stage appears compatible with the brand's typical timing and pathway expectations.`;
    case "roomRangeFitCompatibility":
      return keys
        ? `For ${brandName}, ${keys} keys appear within a reviewable room-range band for available brand criteria.`
        : `For ${brandName}, room count appears within a reviewable range for available brand data.`;
    case "agreementsTypeCompatibility":
      return `For ${brandName}, deal structure appears compatible with available brand agreement-type data.`;
    case "preferredBrand":
      return isPreferred
        ? `Owner preference inputs explicitly support including ${brandName} in the review set.`
        : `Strategic-intent inputs appear to support reviewing ${brandName} in the current set.`;
    case "serviceModelAlignment":
      return profile.serviceModel
        ? `For ${brandName}, operating model inputs appear directionally compatible with the brand's ${profile.serviceModel} service model.`
        : `For ${brandName}, operating model appears directionally compatible with available brand inputs.`;
    case "buildingTypeCompatibility":
      return `For ${brandName}, building type / asset form appears compatible with available brand asset criteria.`;
    case "brandStandardsCompatibility":
      return `For ${brandName}, standards and program expectations appear reviewable, though PIP scope still needs owner/advisor confirmation.`;
    case "keyMoneyWillingnessCompatibility":
    case "incentiveAlignment":
      return `For ${brandName}, commercial incentive assumptions appear relevant; key money and fee terms should be confirmed with the brand.`;
    case "footprintRegionAlignment":
      return `For ${brandName}, market / region context appears directionally relevant for footprint and competitive review.`;
    case "feeStructureAlignment":
      return `For ${brandName}, fee-related inputs appear directionally compatible with available brand commercial data.`;
    case "sustainabilityAlignment":
      return `For ${brandName}, sustainability / ESG inputs appear directionally relevant where captured in deal data.`;
    default:
      return `For ${brandName}, a scored alignment factor appears directionally compatible on current inputs.`;
  }
}

function validationBullet(factorKey, deal, mergedFields) {
  switch (factorKey) {
    case "brandStandardsCompatibility":
      return "Brand standards and PIP expectations should be confirmed.";
    case "projectStageCompatibility":
      return "Development stage clarity should be confirmed before outreach.";
    case "chainScaleProximity":
      return "Target chain scale / positioning should be confirmed against the brand's typical band.";
    case "buildingTypeCompatibility":
      return "Building type / asset form requires validation.";
    case "serviceModelAlignment":
      return "Operating model compatibility should be clarified.";
    case "preferredBrand":
      return "Owner priority alignment is not fully supported by current inputs.";
    case "agreementsTypeCompatibility":
      return "Preferred deal structure should be validated against the brand's typical agreement types.";
    case "footprintRegionAlignment":
      return "Market and competitive presence should be reviewed.";
    case "keyMoneyWillingnessCompatibility":
    case "incentiveAlignment":
      return "Commercial incentive assumptions should be confirmed directly with the brand.";
    case "roomRangeFitCompatibility":
      return "Room count / room-range fit should be confirmed against brand criteria.";
    case "projectTypeCompatibility":
      return isConversionDeal(deal?.projectType)
        ? "Conversion pathway, PIP scope, and current brand status should be validated."
        : "Project type and development path should be confirmed with the owner.";
    case "feeStructureAlignment":
      return "Fee structure and commercial expectations should be confirmed.";
    default:
      return "Additional deal and brand inputs should be validated before outreach.";
  }
}

function ownerFacingPotentialExplanation(factorKey, assessment, deal, profile) {
  const strong = assessment === "Strong signal" || assessment === "Moderate signal";
  const bn = profile?.brandName || "this brand";
  switch (factorKey) {
    case "chainScaleProximity":
      return strong
        ? `The deal's positioning band appears directionally aligned with ${bn}'s chain scale tier${profile?.chainScale ? ` (${profile.chainScale})` : ""}.`
        : `Chain scale fit for ${bn} should be confirmed before relying on this signal.`;
    case "projectTypeCompatibility":
      return strong
        ? "The project's development path appears compatible with brand reference data for this pathway."
        : "Project type compatibility should be validated against the brand's accepted project types.";
    case "projectStageCompatibility":
      return strong
        ? "Development stage inputs appear compatible with the brand's typical timing expectations."
        : "Development stage clarity may affect how the brand views timing and risk.";
    case "brandStandardsCompatibility":
      return strong
        ? "Standards expectations appear directionally reviewable, but PIP and program detail still need confirmation."
        : "Brand standards and PIP expectations should be confirmed before outreach.";
    case "agreementsTypeCompatibility":
      return strong
        ? "Deal structure inputs appear compatible with available brand agreement data."
        : "Deal structure and agreement type fit should be validated.";
    case "preferredBrand":
      return strong
        ? "Owner strategic-intent inputs appear to support reviewing this brand in the set."
        : "Owner priority alignment should be confirmed with the owner.";
    case "serviceModelAlignment":
      return strong
        ? "Operating model inputs appear directionally compatible with brand reference data."
        : "Operating model compatibility should be clarified.";
    case "buildingTypeCompatibility":
      return strong
        ? "Building type appears compatible with available brand asset criteria."
        : "Building type / asset form should be validated.";
    case "roomRangeFitCompatibility":
      return strong
        ? "Room count appears within a reviewable range for this brand's reference data."
        : "Room range fit should be confirmed against brand criteria.";
    case "keyMoneyWillingnessCompatibility":
    case "incentiveAlignment":
      return strong
        ? "Incentive-related inputs appear relevant; commercial terms should still be confirmed with the brand."
        : "Key money / incentive expectations should be clarified before outreach.";
    case "footprintRegionAlignment":
      return strong
        ? "Market / region context appears directionally relevant for this brand's footprint."
        : "Market and competitive presence should be reviewed.";
    case "feeStructureAlignment":
      return strong
        ? "Fee-related inputs appear directionally compatible with available brand data."
        : "Fee structure assumptions should be confirmed.";
    case "sustainabilityAlignment":
      return strong
        ? "Sustainability / ESG inputs appear directionally relevant where captured."
        : "Sustainability / ESG alignment should be validated if relevant to the owner.";
    default:
      return strong
        ? "This factor appears directionally aligned on current inputs."
        : "This factor should be validated before outreach.";
  }
}

function buildPotentialAlignmentSignals(breakdownNewDetails, deal, profile) {
  const details = breakdownNewDetails && typeof breakdownNewDetails === "object" ? breakdownNewDetails : {};
  const out = [];
  for (const key of FACTOR_ORDER) {
    const row = details[key];
    if (!row || typeof row !== "object") continue;
    const assessment = signalAssessment(row.score);
    out.push({
      factorKey: key,
      label: SIGNAL_LABELS[key] || key,
      assessment,
      ownerExplanation: ownerFacingPotentialExplanation(key, assessment, deal, profile),
    });
    if (out.length >= 10) break;
  }
  return out;
}

function pathwayContextSentence(pathway, profile) {
  const b = profile.brandName || "This brand";
  const parent = profile.parentCompany ? ` under ${profile.parentCompany}` : "";
  if (/curio collection/i.test(b)) {
    return `${b}${parent} may be relevant if the owner wants Hilton distribution and loyalty while keeping a distinct property story and design narrative.`;
  }
  if (/radisson red/i.test(b)) {
    return `${b}${parent} may merit review if the owner wants a bolder lifestyle expression within Radisson's system rather than a traditional full-standard prototype path.`;
  }
  if (/^so$/i.test(b.trim())) {
    return `${b}${parent} may merit review where experiential, design-aware positioning and F&B programming are central to the owner's concept.`;
  }
  if (/hyatt centric|joie de vivre|bunkhouse/i.test(b)) {
    return `${b}${parent} may merit review if local character, design, and experiential programming are important alongside Hyatt system support.`;
  }
  switch (pathway) {
    case "collection":
      return `${b}${parent} may be relevant if the owner wants affiliation flexibility and distribution support while preserving differentiated property identity.`;
    case "lifestyle":
      return `${b}${parent} may merit review if the owner is pursuing a design-led or experiential positioning with defined F&B and public-space expectations.`;
    case "luxury":
      return `${b}${parent} may merit review where capital, service, and approval expectations appear directionally compatible with an upper-tier brand system.`;
    case "upscale_hard":
      return `${b}${parent} may merit review where the owner seeks a more defined brand system, distribution support, and standardized operating guidance.`;
    default:
      return `${b}${parent} may merit review as part of the owner/advisor screening set based on current deal and brand reference inputs.`;
  }
}

function buildBecauseClause(deal, strongKeys, source, brandName, preferredBrandNames) {
  const businessParts = [];
  const seen = new Set();
  for (const key of strongKeys || []) {
    const label = BUSINESS_SIGNAL_LABELS[key];
    if (!label || seen.has(label)) continue;
    seen.add(label);
    businessParts.push(label);
  }

  const isPreferred =
    source === "owner_preferred" ||
    (preferredBrandNames || []).some(
      (p) => String(p).trim().toLowerCase() === String(brandName || "").trim().toLowerCase()
    );
  if (isPreferred && !seen.has("owner preference alignment")) {
    businessParts.push("owner preference alignment");
  }

  if (businessParts.length) {
    return (
      joinNaturalList(businessParts.slice(0, 4)) +
      " appear directionally compatible with available brand reference data"
    );
  }

  const fallback = [];
  if (deal?.targetPositioning && deal.targetPositioning !== "—") {
    fallback.push(`the deal's ${deal.targetPositioning.toLowerCase()} positioning`);
  }
  if (deal?.projectType) fallback.push(`${String(deal.projectType).trim().toLowerCase()} project type`);
  if (deal?.keyCount != null && Number.isFinite(deal.keyCount)) fallback.push(`${deal.keyCount}-key scale`);
  if (!fallback.length) return "available deal and brand reference inputs appear directionally compatible";
  return fallback.join(", ") + " appear directionally compatible with available brand reference data";
}

/** What Supports Review — business bullets without per-brand factor repetition. */
function supportReviewBullet(factorKey, deal, source, preferredBrandNames, brandName) {
  const isPreferred =
    source === "owner_preferred" ||
    (preferredBrandNames || []).some(
      (p) => String(p).trim().toLowerCase() === String(brandName || "").trim().toLowerCase()
    );

  switch (factorKey) {
    case "chainScaleProximity":
      return "Target positioning appears directionally aligned with the brand's chain scale.";
    case "projectTypeCompatibility":
      return "Project type appears compatible with available brand reference data.";
    case "projectStageCompatibility":
      return "Development stage appears compatible with the brand's typical pathway expectations.";
    case "roomRangeFitCompatibility":
      return "Room count appears within a reviewable range for available brand criteria.";
    case "agreementsTypeCompatibility":
      return "Deal structure appears compatible with available brand pathway data.";
    case "preferredBrand":
      return isPreferred
        ? "Owner preference inputs support inclusion of this brand in the review set."
        : "Strategic-intent inputs appear to support reviewing this brand in the set.";
    case "serviceModelAlignment":
      return "Operating model compatibility appears directionally aligned on current inputs.";
    case "buildingTypeCompatibility":
      return "Building type / asset form appears compatible with available brand asset criteria.";
    case "brandStandardsCompatibility":
      return "Standards expectations appear reviewable, though PIP scope still needs confirmation.";
    case "keyMoneyWillingnessCompatibility":
    case "incentiveAlignment":
      return "Commercial incentive alignment appears relevant; key money / incentive assumptions should be confirmed with the brand.";
    case "footprintRegionAlignment":
      return "Market and competitive context appears directionally relevant for footprint review.";
    case "feeStructureAlignment":
      return "Fee-related inputs appear directionally compatible with available brand commercial data.";
    case "sustainabilityAlignment":
      return "Sustainability / ESG inputs appear directionally relevant where captured.";
    default:
      return null;
  }
}

function buildWhatSupportsReview(strongOrdered, deal, source, preferredBrandNames, brandName) {
  const bullets = [];
  const seen = new Set();
  const add = (text) => {
    const t = String(text || "").trim();
    const k = t.toLowerCase();
    if (!t || seen.has(k)) return;
    seen.add(k);
    bullets.push(t);
  };

  for (const key of strongOrdered) {
    add(supportReviewBullet(key, deal, source, preferredBrandNames, brandName));
    if (bullets.length >= 5) break;
  }
  if (
    source === "owner_preferred" &&
    bullets.length < 5 &&
    !strongOrdered.includes("preferredBrand")
  ) {
    add("Owner preference inputs support inclusion of this brand in the review set.");
  }
  if (bullets.length < 3) {
    add("Available deal and brand reference inputs appear sufficient to merit internal review.");
  }
  return bullets.slice(0, 5);
}

/** Short executive signals for page-2 table (identity + top business themes). */
function buildExecutiveMainSignals(profile, pathway, strongOrdered, deal, source, preferredBrandNames, brandName) {
  const out = [brandIdentitySupportBullet(profile, pathway)];
  const themes = [];
  for (const key of strongOrdered) {
    const label = BUSINESS_SIGNAL_LABELS[key];
    if (label && !themes.includes(label)) themes.push(label);
    if (themes.length >= 3) break;
  }
  if (themes.length) {
    out.push(
      `Primary alignment themes on current inputs: ${joinNaturalList(themes)}.`
    );
  }
  if (
    source === "owner_preferred" &&
    !strongOrdered.includes("preferredBrand")
  ) {
    out.push(`Owner preference inputs support including ${brandName} in the review set.`);
  }
  return out.slice(0, 4);
}

function validationBulletToTopic(bullet) {
  const b = String(bullet || "").toLowerCase();
  if (/brand standards|pip/i.test(b)) return "brand standards and PIP expectations";
  if (/building type|asset form/i.test(b)) return "building type / asset form";
  if (/operating model/i.test(b)) return "operating model compatibility";
  if (/chain scale|positioning/i.test(b)) return "target positioning and chain scale";
  if (/development stage/i.test(b)) return "development stage clarity";
  if (/owner priority/i.test(b)) return "owner priority alignment";
  if (/deal structure|agreement/i.test(b)) return "deal structure and agreement type";
  if (/market|competitive|footprint/i.test(b)) return "market and competitive presence";
  if (/commercial|incentive|key money/i.test(b)) return "commercial incentive assumptions";
  if (/room range|room count/i.test(b)) return "room count / room-range fit";
  if (/project type|conversion|pip scope/i.test(b)) return "project type and conversion pathway";
  if (/fee structure/i.test(b)) return "fee structure assumptions";
  if (/sustainability|esg/i.test(b)) return "sustainability / ESG alignment";
  if (/sufficient inputs|deal setup/i.test(b)) return "deal setup completeness";
  if (/soft vs\. hard/i.test(b)) return "soft vs. hard brand preference";
  return "";
}

function validationClosePhrase(validationBullets, pathway, mergedFields) {
  const topics = [];
  const seen = new Set();
  const add = (topic) => {
    const t = String(topic || "").trim();
    if (!t || seen.has(t)) return;
    seen.add(t);
    topics.push(t);
  };

  for (const bullet of validationBullets || []) {
    add(validationBulletToTopic(bullet));
  }

  const softHard = fieldPresent(mergedFields, [
    "Soft vs Hard Brand Preference",
    "Do you prefer a soft brand, hard brand, or are you open to both?",
  ]);
  if (!softHard) add("soft vs. hard brand preference");
  if (pathway === "collection" || pathway === "lifestyle") {
    add("design/story requirements");
  }
  add("commercial structure");

  return joinNaturalList(topics.slice(0, 4));
}

function buildAlignmentRationaleParagraph({
  brandName,
  tier,
  scoreAvailable,
  deal,
  mergedFields,
  source,
  preferredBrandNames,
  pathway,
  profile,
  validationBullets,
  strongKeys,
}) {
  if (!scoreAvailable) {
    return (
      `${brandName} is included in the review set, but alignment scoring is limited by incomplete deal or brand reference inputs. ` +
      "Additional validation is needed before this brand is used for controlled outreach planning."
    );
  }

  const because = buildBecauseClause(deal, strongKeys || [], source, brandName, preferredBrandNames);
  let text =
    `${brandName} currently shows a ${tier} because ${because}. ` +
    pathwayContextSentence(pathway, profile) +
    " ";
  const closeItems = validationClosePhrase(validationBullets, pathway, mergedFields);
  if (closeItems) {
    text += `Before outreach, the owner should validate ${closeItems}.`;
  } else {
    text += "Before outreach, the owner should validate brand standards, deal assumptions, and commercial structure with the owner/advisor.";
  }
  return text.trim();
}

function normalizeQuestionKey(q) {
  return String(q || "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isSharedClarificationQuestion(q) {
  const n = normalizeQuestionKey(q);
  if (!n) return true;
  return COMMON_QUESTIONS_BEFORE_OUTREACH.some((shared) => {
    const s = normalizeQuestionKey(shared);
    if (!s) return false;
    if (n === s) return true;
    if (n.length > 24 && s.length > 24 && (n.includes(s.slice(0, 40)) || s.includes(n.slice(0, 40)))) {
      return true;
    }
    return false;
  });
}

function ownerQuestionsForPathway(pathway, tier, deal, mergedFields, strongKeys, weakKeys) {
  const questions = [];
  const add = (q) => {
    const t = String(q || "").trim();
    if (!t || questions.includes(t) || isSharedClarificationQuestion(t)) return;
    questions.push(t);
  };

  if (pathway === "collection") {
    add("Does the project have a strong enough identity, story, or design concept for this affiliation path?");
    add("How much independent character does the owner want to preserve?");
    add("What brand standards are mandatory versus project-specific?");
  } else if (pathway === "lifestyle") {
    add("Does the owner want a more design-led or experiential positioning?");
    add("Are F&B, public space, and programming expectations realistic for this asset?");
    add("How much creative direction will the brand require versus owner-led concept development?");
  } else if (pathway === "luxury" || pathway === "upscale_hard") {
    add("Can the asset meet prototype, market, signage, and public-space expectations for this brand path?");
    add("Is the capital plan sufficient for the likely product and service expectations?");
    add("Does the market support the intended rate and positioning band?");
  } else {
    add("Does the owner want a more standardized brand system or greater operating flexibility?");
    add("What level of brand standards tolerance is realistic for this project?");
  }

  if (weakKeys.includes("keyMoneyWillingnessCompatibility") || weakKeys.includes("incentiveAlignment")) {
    add("Are commercial incentives important to the owner, and what terms would need to be confirmed?");
  }
  if (isConversionDeal(deal?.projectType)) {
    add("What PIP scope and conversion timeline are realistic before this brand is contacted?");
  }
  if (weakKeys.includes("serviceModelAlignment")) {
    add("Is the owner seeking brand affiliation only, operator involvement, or both?");
  }
  if (tier === "Conditional Review Signal" || tier === "Lower Alignment Signal") {
    add("What assumptions must be confirmed before this brand is included in controlled outreach?");
  }

  return questions.slice(0, 5);
}

function fitBoundariesForBrand(pathway, tier, deal, mergedFields, weakKeys) {
  const items = [];
  const add = (t) => {
    if (t && !items.includes(t)) items.push(t);
  };

  add("Alignment may weaken if required brand standards exceed the owner's capex tolerance.");
  add("Alignment may weaken if the owner wants more independence than the brand path allows.");
  add("Alignment may weaken if operating model expectations are not aligned.");
  add("Alignment may weaken if the brand's market presence or distribution value does not meet owner objectives.");
  add("Alignment may weaken if product/service expectations are higher than the project intends to support.");

  const capex = fieldPresent(mergedFields, ["PIP / CapEx Status", "CapEx Status", "Renovation Budget"]);
  if (capex && /low|minimal|tight/i.test(capex)) {
    add("Alignment may weaken if the owner prioritizes minimal capex over likely brand standards.");
  }
  if (pathway === "collection") {
    add("Alignment may weaken if the property lacks sufficient identity or story for a collection-style affiliation.");
  }
  if (weakKeys.includes("footprintRegionAlignment")) {
    add("Alignment may weaken if competitive or distribution assumptions do not hold in this market.");
  }
  if (tier === "Lower Alignment Signal" || tier === "Conditional Review Signal") {
    add("Alignment may weaken if core deal inputs remain incomplete or inconsistent with the brand's typical criteria.");
  }
  return items.slice(0, 5);
}

function buildKeyConsiderationSummary({ profile, pathway, tier, scoreAvailable, weakKeys }) {
  const n = profile.brandName;
  if (!scoreAvailable) {
    return `Alignment for ${n} is limited by missing or incomplete deal inputs; clarify chain scale, stage, and deal structure before outreach.`;
  }

  if (/curio collection/i.test(n)) {
    return (
      "Collection-style path may be relevant if the owner wants distribution support while preserving property identity; standards and PIP expectations should be confirmed."
    );
  }
  if (/radisson red/i.test(n)) {
    return (
      "Lifestyle positioning appears directionally aligned; validate F&B, public space, and programming expectations before outreach."
    );
  }
  if (/^so$/i.test(n.trim())) {
    return (
      "Lifestyle positioning appears directionally aligned; validate F&B, public space, and experiential programming before outreach."
    );
  }
  if (pathway === "collection") {
    return (
      "Collection-style path may be relevant if the owner wants affiliation flexibility while preserving property identity; standards and commercial assumptions should be confirmed."
    );
  }
  if (pathway === "lifestyle") {
    return (
      "Lifestyle positioning appears directionally aligned; validate F&B, public space, and programming expectations before outreach."
    );
  }
  if (pathway === "luxury") {
    return (
      "Upper-tier alignment may be positive, but product standards, capital plan, and approval requirements should be confirmed."
    );
  }
  if (tier === "Moderate Alignment Signal") {
    return (
      "Moderate alignment on current inputs; validate positioning, standards tolerance, and brand approval requirements before outreach."
    );
  }
  if (tier === "Conditional Review Signal" || tier === "Lower Alignment Signal") {
    return (
      "Conditional alignment on current inputs; clarify positioning, operating model, and commercial assumptions before outreach."
    );
  }
  return capitalizeLead(brandReviewHook(profile, pathway, tier, weakKeys)) + ".";
}

/**
 * Full business rationale package for one brand in the review set.
 */
export function buildBrandReviewContent({
  brandName,
  tier,
  scoreAvailable,
  score,
  breakdownNewDetails,
  deal,
  mergedFields,
  brandData,
  source,
  preferredBrandNames,
  parentCompany,
}) {
  const { strong, weak, missing } = classifyAlignmentFactors(breakdownNewDetails);
  const profile = extractBrandProfile(brandName, brandData, parentCompany);
  const pathway = inferBrandPathway(brandName, brandData, mergedFields);
  const strongOrdered = sortStrongFactorsByScore(breakdownNewDetails, strong);

  const mainAlignmentSignals = buildExecutiveMainSignals(
    profile,
    pathway,
    strongOrdered,
    deal,
    source,
    preferredBrandNames,
    brandName
  );

  const whatSupportsReview = buildWhatSupportsReview(
    strongOrdered,
    deal,
    source,
    preferredBrandNames,
    brandName
  );

  const whatNeedsValidation = [];
  const seenVal = new Set();
  for (const key of weak) {
    const bullet = validationBullet(key, deal, mergedFields);
    const k = bullet.toLowerCase();
    if (!seenVal.has(k)) {
      seenVal.add(k);
      whatNeedsValidation.push(bullet);
    }
    if (whatNeedsValidation.length >= 5) break;
  }
  if (missing >= 3 && whatNeedsValidation.length < 5) {
    whatNeedsValidation.push("Several scored factors lack sufficient inputs; deal setup completeness should be reviewed.");
  }
  const defaultValidation = [
    "Brand standards and PIP expectations should be confirmed.",
    "Building type / asset form requires validation.",
    "Operating model compatibility should be clarified.",
    "Owner priority alignment is not fully supported by current inputs.",
    "Market and competitive presence should be reviewed.",
    "Commercial incentive assumptions should be confirmed directly with the brand.",
  ];
  for (const d of defaultValidation) {
    const k = d.toLowerCase();
    if (!seenVal.has(k)) {
      seenVal.add(k);
      whatNeedsValidation.push(d);
    }
    if (whatNeedsValidation.length >= 5) break;
  }

  const alignmentFactorsReviewed = buildPotentialAlignmentSignals(breakdownNewDetails, deal, profile);

  const alignmentRationale = buildAlignmentRationaleParagraph({
    brandName,
    tier,
    scoreAvailable,
    deal,
    mergedFields,
    source,
    preferredBrandNames,
    pathway,
    profile,
    validationBullets: whatNeedsValidation,
    strongKeys: strongOrdered,
  });

  const ownerQuestionsThisBrandRaises = ownerQuestionsForPathway(
    pathway,
    tier,
    deal,
    mergedFields,
    strong,
    weak
  );

  const fitBoundariesWatchouts = fitBoundariesForBrand(pathway, tier, deal, mergedFields, weak);

  const keyConsideration = buildKeyConsiderationSummary({
    profile,
    pathway,
    tier,
    scoreAvailable,
    weakKeys: weak,
  });

  return {
    alignmentRationale,
    mainAlignmentSignals,
    alignmentFactorsReviewed,
    potentialAlignmentSignals: alignmentFactorsReviewed,
    whatSupportsReview,
    whatNeedsValidation: whatNeedsValidation.slice(0, 5),
    ownerQuestionsThisBrandRaises,
    whatCouldWeakenAlignment: fitBoundariesWatchouts,
    fitBoundariesWatchouts,
    keyConsideration,
    brandPathway: pathway,
    /** @deprecated use alignmentFactorsReviewed — technical factor view */
    signals: alignmentFactorsReviewed.map((s) => ({
      factorKey: s.factorKey,
      label: s.label,
      assessment: s.assessment,
      note: "",
    })),
  };
}
