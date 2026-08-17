/**
 * Brand Alignment Snapshot — deal-level brand alignment signals for owner/advisor review.
 * POST /api/ai/brand-alignment-snapshot
 * Uses server-side Match Score New (computeMatchScoreForDealBrand); does not expose recommended brand as final answer.
 */

import {
  fetchDealWithMergedLinkedRecords,
  fetchDealScoringContext,
  fetchDealBrandCacheForDeal,
  ensureDealBrandCacheFresh,
} from "./my-deals.js";
import { buildReadinessFromFields } from "./deal-readiness-review.js";
import { fetchTargetsForDeal } from "./target-list.js";
import { fetchBrandData } from "./match-score-server.js";
import {
  buildScoresIndexFromCacheRow,
  getBreakdownFromCacheIndex,
  getScoreFromCacheIndex,
} from "../lib/deal-brand-cache-snapshot.js";
import { buildBrandReviewContent, COMMON_QUESTIONS_BEFORE_OUTREACH } from "../lib/brand-alignment-rationale.js";
import {
  resolveProjectTypeKind,
  isConversionDealProjectType,
  normalizeProjectTypeLabel,
} from "../lib/project-type.js";
import { brandMatchTierFromScore } from "../lib/brand-match-scoring-weight-config.js";

const OUTPUT_STATUS = "Draft for validation";
const METHODOLOGY_NOTE =
  "Alignment scores are generated from current deal inputs and available brand reference data using Dealality's server-side brand matching logic. " +
  "Scores are intended to organize owner/advisor review and may change as deal inputs, owner priorities, brand criteria, or readiness information are updated. " +
  "An alignment signal does not indicate brand approval, brand interest, commercial terms, or final suitability.";

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

function normalizeBrandKey(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function tierFromScore(score) {
  return brandMatchTierFromScore(score);
}

function reviewStatusForBrand(tier, projectType) {
  const pt = String(projectType || "").toLowerCase();
  const statuses = [];
  if (tier === "Higher Alignment Signal" || tier === "Moderate Alignment Signal") {
    statuses.push("Review if owner confirms positioning");
  } else {
    statuses.push("Requires additional deal clarity");
  }
  if (/conversion|reposition|reflag|rebrand/i.test(pt)) {
    statuses.push("Review if capex/PIP assumptions are validated");
  }
  if (tier === "Moderate Alignment Signal") {
    statuses.push("Review if owner prioritizes loyalty/distribution");
  }
  return statuses[0];
}

function isConversionDeal(projectType) {
  return isConversionDealProjectType(projectType);
}

const EXECUTIVE_FIELD_LABELS = {
  Country: "Country / market confirmation",
  "Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?":
    "Prior brand / management agreement history",
  "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?":
    "Prior brand / management agreement history",
};

function humanizeFieldLabel(field) {
  const f = String(field || "").trim();
  if (!f) return "Additional clarification";
  if (EXECUTIVE_FIELD_LABELS[f]) return EXECUTIVE_FIELD_LABELS[f];
  const low = f.toLowerCase();
  if (
    (/agreeement/i.test(f) || /similar agreement pertaining/i.test(low)) &&
    /franchise|branded management|affiliation/i.test(low)
  ) {
    return "Prior brand / management agreement history";
  }
  if (/franchise|branded management|affiliation/i.test(low) && /proposed hotel/i.test(low)) {
    return "Prior brand / management agreement history";
  }
  if (/^country$/i.test(f)) return "Country / market confirmation";
  if (/^total number of rooms|^number of rooms|\/keys/i.test(low)) return "Key count / room program";
  if (/soft vs hard|soft brand.*hard/i.test(low)) return "Soft vs. hard brand preference";
  if (/chain scale|hotel chain scale|target chain/i.test(low)) return "Target chain scale / positioning";
  if (/stage of development|development stage|project stage/i.test(low)) return "Development stage";
  if (/project type/i.test(low)) return "Project type";
  if (/brand standards|standards tolerance/i.test(low)) return "Brand standards tolerance";
  if (/pip|capex|renovation/i.test(low)) return "PIP / CapEx assumptions";
  if (/\?/.test(f)) return "Owner confirmation item";
  return f.length > 52 ? f.slice(0, 49).trim() + "…" : f;
}

function formatClarificationLabel(text, deal) {
  const raw = String(text || "").trim();
  const label = humanizeFieldLabel(raw);
  if (
    deal &&
    /^country$/i.test(raw) &&
    (!deal.country || deal.country === "—") &&
    deal.market &&
    deal.market !== "—"
  ) {
    return "Country / market confirmation";
  }
  return label;
}

function formatMarketCountryLine(deal) {
  const country = deal?.country;
  const market = deal?.market;
  const countryMissing = !country || country === "—";
  if (countryMissing && market && market !== "—") {
    return market + " (country should be confirmed against the recorded market/city)";
  }
  if (market && market !== "—" && country && country !== "—") {
    return market + ", " + country;
  }
  if (country && country !== "—") return country;
  if (market && market !== "—") return market;
  return "the recorded market";
}

function describeProjectTypePhrase(projectType) {
  const pt = normalizeProjectTypeLabel(projectType) || String(projectType || "").trim();
  if (!pt) return "hospitality";
  const kind = resolveProjectTypeKind(pt);
  switch (kind) {
    case "new_build":
      return "new-build";
    case "conversion_reflag":
      return "conversion/reflag";
    case "renovation_repositioning":
      return "renovation/repositioning";
    case "existing_operating":
      return "existing operating-hotel";
    case "adaptive_reuse":
      return "adaptive-reuse";
    case "mixed_use":
      return "mixed-use hospitality";
    case "other_tbc":
      return "project-type to be confirmed";
    default:
      return pt.toLowerCase();
  }
}

function readinessStageKey(stage) {
  return String(stage || "")
    .trim()
    .toLowerCase();
}

function readinessStageFromPayload(readiness) {
  return String(readiness?.stage || readiness?.readinessStage || "").trim();
}

function readinessScoreFromPayload(readiness) {
  const n = Number(readiness?.score ?? readiness?.dealReadinessScore);
  return Number.isFinite(n) ? n : null;
}

/** Short clause for Paragraph 1 readiness posture. */
function readinessInterpretation(stage, readinessScore) {
  const sl = readinessStageKey(stage);
  if (sl === "discovery" || sl === "shaping") {
    return "requires more clarification";
  }
  if (sl === "advancing") {
    return "supports structured owner-advisor review";
  }
  if (sl.indexOf("ready for external") >= 0) {
    return "supports controlled external review after owner/advisor validation";
  }
  if (sl === "ready") {
    return "supports structured owner-advisor review";
  }
  const n = Number(readinessScore);
  if (Number.isFinite(n) && n >= 75) return "supports structured owner-advisor review";
  if (Number.isFinite(n) && n < 50) return "requires more clarification";
  return "supports early internal review with additional validation";
}

function alignmentPatternLabel(tierCounts, reportCount, limitedData, scored) {
  if (limitedData) return "limited by review-set breadth";
  const noScore = (scored || []).filter((b) => !b.scoreAvailable).length;
  if (reportCount > 0 && noScore >= Math.ceil(reportCount * 0.5)) {
    return "limited by missing inputs";
  }
  const h = tierCounts["Higher Alignment Signal"] || 0;
  const m = tierCounts["Moderate Alignment Signal"] || 0;
  const c = tierCounts["Conditional Review Signal"] || 0;
  const l = tierCounts["Lower Alignment Signal"] || 0;
  const n = Math.max(1, reportCount || 1);
  if (h + m >= Math.ceil(n * 0.6) && h >= 1) return "concentrated among higher-alignment signals";
  if (c + l >= Math.ceil(n * 0.6)) return "mostly conditional";
  if (h > 0 && (c > 0 || l > 0)) return "mixed";
  if (m > 0 && h === 0) return "moderate-tier centered";
  return "mixed";
}

function countTiers(scored) {
  const counts = {
    "Higher Alignment Signal": 0,
    "Moderate Alignment Signal": 0,
    "Conditional Review Signal": 0,
    "Lower Alignment Signal": 0,
  };
  for (const b of scored) {
    const t = b.tier || "Conditional Review Signal";
    if (counts[t] != null) counts[t] += 1;
    else counts["Conditional Review Signal"] += 1;
  }
  return counts;
}

function formatTierBreakdown(tierCounts) {
  const parts = [];
  if (tierCounts["Higher Alignment Signal"]) {
    parts.push(tierCounts["Higher Alignment Signal"] + " Higher Alignment Signal");
  }
  if (tierCounts["Moderate Alignment Signal"]) {
    parts.push(tierCounts["Moderate Alignment Signal"] + " Moderate Alignment Signal");
  }
  if (tierCounts["Conditional Review Signal"]) {
    parts.push(tierCounts["Conditional Review Signal"] + " Conditional Review Signal");
  }
  if (tierCounts["Lower Alignment Signal"]) {
    parts.push(tierCounts["Lower Alignment Signal"] + " Lower Alignment Signal");
  }
  return parts.join("; ");
}

function describeAlignmentPattern(tierCounts, reportCount, limitedData, scored) {
  const label = alignmentPatternLabel(tierCounts, reportCount, limitedData, scored);
  const tierLine = formatTierBreakdown(tierCounts);
  const map = {
    "limited by review-set breadth":
      "limited by a small review set and may not reflect the full owner pathway",
    "limited by missing inputs":
      "limited by missing or incomplete deal and brand inputs across much of the review set",
    "concentrated among higher-alignment signals":
      "concentrated among higher- and moderate-alignment signals",
    "mostly conditional":
      "mostly conditional or lower-alignment signals, with several brands requiring additional validation",
    mixed: "mixed across higher-alignment and conditional signals",
    "moderate-tier centered":
      "centered on moderate-alignment signals with limited higher-tier concentration",
  };
  let text = map[label] || "distributed across multiple alignment tiers";
  if (tierLine) text += " (" + tierLine + ")";
  return text;
}

function brandUniverseSourceLabel(brandEntries) {
  const sources = new Set((brandEntries || []).map((b) => b.source));
  const parts = [];
  if (sources.has("owner_preferred")) parts.push("owner-preferred brands");
  if (sources.has("target_list")) parts.push("deal pipeline / target list");
  if (sources.has("deal_brand_cache")) parts.push("deal brand cache candidates");
  return parts.length ? parts.join(", ") : "available deal inputs";
}

function brandFactorSignals(brand) {
  return brand.potentialAlignmentSignals || brand.signals || [];
}

function aggregateSignalTallies(scored, weakOnly) {
  const tallies = {};
  for (const brand of scored) {
    for (const sig of brandFactorSignals(brand)) {
      const weak =
        sig.assessment === "Not enough data" ||
        sig.assessment === "Needs validation" ||
        sig.assessment === "Limited signal";
      if (weakOnly && !weak) continue;
      if (!weakOnly && weak) continue;
      if (weakOnly && weak) {
        tallies[sig.label] = (tallies[sig.label] || 0) + 1;
      } else if (!weakOnly && (sig.assessment === "Strong signal" || sig.assessment === "Moderate signal")) {
        tallies[sig.label] = (tallies[sig.label] || 0) + 1;
      }
    }
  }
  return Object.entries(tallies)
    .sort((a, b) => b[1] - a[1])
    .map(([label]) => label);
}

function dominantAlignmentSignals(scored, limit = 5) {
  return aggregateSignalTallies(scored, false).slice(0, limit);
}

function signalLabelToWatchout(label) {
  const map = {
    "Chain scale alignment": "target chain scale / positioning",
    "Project type compatibility": "project type and conversion pathway",
    "Development stage alignment": "development stage clarity",
    "Standards / flexibility alignment": "brand standards and PIP expectations",
    "Deal structure alignment": "deal structure and agreement type",
    "Owner priority alignment": "owner priority alignment",
    "Operator model compatibility": "operating model",
    "Building type compatibility": "building type / asset form",
    "Incentive / key money relevance": "key money / incentive expectations",
    "Market/region relevance": "market / competitive presence",
    "Fee structure alignment": "fee structure assumptions",
    "Sustainability / ESG relevance": "sustainability / ESG positioning",
  };
  return map[label] || label.toLowerCase();
}

function aggregateCommonValidationFactors(scored, readiness, deal, mergedFields) {
  const fromSignals = aggregateSignalTallies(scored, true).map(signalLabelToWatchout);
  const fromReadiness = clarificationFromReadiness(readiness).map((t) =>
    formatClarificationLabel(t, deal)
  );
  const contextual = [];
  const softHard = fieldPresent(mergedFields || {}, [
    "Soft vs Hard Brand Preference",
    "Do you prefer a soft brand, hard brand, or are you open to both?",
  ]);
  if (!softHard) contextual.push("soft vs. hard brand preference");
  const localStory = fieldPresent(mergedFields || {}, [
    "Local identity / design story",
    "Is local identity important",
    "Design narrative",
  ]);
  if (!localStory && isConversionDeal(deal?.projectType)) {
    contextual.push("local identity / design story");
  }
  const merged = [];
  const seen = new Set();
  for (const x of [...fromSignals, ...fromReadiness, ...contextual]) {
    const k = String(x || "").toLowerCase();
    if (!x || seen.has(k)) continue;
    seen.add(k);
    merged.push(x);
  }
  return merged.slice(0, 5);
}

function topBrandNamesForSummary(scored, max = 6) {
  const tierOrder = [
    "Higher Alignment Signal",
    "Moderate Alignment Signal",
    "Conditional Review Signal",
    "Lower Alignment Signal",
  ];
  const picked = [];
  for (const tier of tierOrder) {
    for (const b of scored || []) {
      if (b.tier !== tier || picked.includes(b.brandName)) continue;
      if (!b.scoreAvailable) continue;
      picked.push(b.brandName);
      if (picked.length >= max) return picked;
    }
  }
  for (const b of scored || []) {
    if (picked.length >= max) break;
    if (!picked.includes(b.brandName)) picked.push(b.brandName);
  }
  return picked.slice(0, max);
}

function formatTopBrandsSentence(names) {
  const list = (names || []).filter(Boolean);
  if (!list.length) return "";
  if (list.length === 1) {
    return list[0] + " currently shows the strongest alignment signal in the review set";
  }
  const lead = joinNaturalList(list.slice(0, Math.min(3, list.length)));
  let s = lead + " currently show the strongest alignment signals in the review set";
  if (list.length > 3) {
    s += " (" + joinNaturalList(list.slice(3)) + " also appear in the review table)";
  }
  return s;
}

function capitalizeLead(text) {
  const t = String(text || "").trim();
  if (!t) return t;
  return t.charAt(0).toUpperCase() + t.slice(1);
}

function joinNaturalList(items) {
  if (!items?.length) return "";
  if (items.length === 1) return items[0];
  if (items.length === 2) return items[0] + " and " + items[1];
  return items.slice(0, -1).join(", ") + ", and " + items[items.length - 1];
}

function buildDealDescriptorParts(deal) {
  const parts = [];
  if (deal.keyCount != null && Number.isFinite(deal.keyCount)) {
    parts.push(deal.keyCount + "-key");
  }
  const pos = deal.targetPositioning && deal.targetPositioning !== "—" ? deal.targetPositioning : "";
  if (pos) parts.push(pos);
  const pt = deal.projectType && String(deal.projectType).trim();
  if (pt) parts.push(pt);
  return parts;
}

function buildSummaryParagraph1(deal, readiness) {
  const name = deal.name || "This deal";
  const marketLine = formatMarketCountryLine(deal);
  const descriptor = buildDealDescriptorParts(deal);
  const stage = readinessStageFromPayload(readiness);
  const interp = readinessInterpretation(stage, readinessScoreFromPayload(readiness));

  let s = "The current inputs describe " + name;
  if (descriptor.length) {
    s += " as a " + descriptor.join(", ") + " hospitality opportunity";
  } else {
    s += " as a hospitality opportunity";
  }
  s += " in " + marketLine + ".";
  if (stage) {
    s +=
      " The deal is currently in a " +
      stage +
      " readiness stage, which means the current information " +
      interp +
      " before controlled external outreach.";
  } else {
    s +=
      " Readiness stage is not fully recorded in the current inputs; owner/advisor validation should precede controlled external outreach.";
  }
  return s;
}

function buildSummaryParagraph2(universe, scored, tierCounts, helpers) {
  const inReport = scored.length;
  const totalInScope = universe.brandCount || inReport;
  const pattern = describeAlignmentPattern(tierCounts, inReport, universe.limitedData, scored);
  const topLead = formatTopBrandsSentence(helpers.topBrandNames);

  let s =
    "The current review set includes " +
    totalInScope +
    " brand" +
    (totalInScope === 1 ? "" : "s");
  if (inReport < totalInScope) {
    s += " (" + inReport + " shown in this snapshot table)";
  }
  s += ". Based on available deal and brand reference data, the alignment pattern is " + pattern + ".";
  if (topLead) {
    s += " " + capitalizeLead(topLead) + ".";
  } else {
    s +=
      " Leading brands cannot be ranked clearly because several entries lack sufficient scored alignment inputs.";
  }
  s +=
    " This should be interpreted as an internal screening view, not a final selection or indication of brand interest.";
  if (universe.limitedData) {
    s += " Fewer than three brands are in scope, which limits how representative this pattern may be.";
  }
  return s;
}

function buildSummaryParagraph3(dominantSignals, mergedFields, scoringPreferredCount) {
  if (!dominantSignals.length) {
    return (
      "Recurring alignment signals are limited across the review set because several brands lack sufficient scored factors. " +
      "Additional deal, owner-priority, and brand-reference inputs may be needed before pathway patterns are visible."
    );
  }
  let s =
    "The strongest recurring signals appear to be " +
    joinNaturalList(dominantSignals) +
    ". These signals suggest that the current review set is directionally aligned with the owner's stated positioning";
  if (scoringPreferredCount > 0) {
    s += " and owner-preferred brand pathways";
  }
  s += ", subject to owner/advisor confirmation.";
  return s;
}

function buildSummaryParagraph4(validationFactors) {
  if (!validationFactors.length) {
    return (
      "Several standard alignment checks remain lightly documented across the review set. " +
      "Owner/advisor review of deal setup completeness is still appropriate before controlled brand outreach."
    );
  }
  return (
    "Several important factors still require validation before the owner relies on the review set for outreach decisions, including " +
    joinNaturalList(validationFactors) +
    ". These items may affect how each brand views the opportunity and should be clarified before controlled brand outreach."
  );
}

function buildSummaryParagraph5() {
  return (
    "This snapshot should be used as an internal screening and discussion tool. " +
    "It organizes potential brand alignment signals and review considerations, but it does not determine final brand selection, commercial terms, brand interest, or brand approval."
  );
}

function buildExecutiveBrandAlignmentSummary(deal, readiness, universe, scored, mergedFields, preferredBrandCount) {
  const tierCounts = countTiers(scored);
  const inReport = scored.length;
  const topBrandNames = topBrandNamesForSummary(scored, 6);
  const dominantSignals = dominantAlignmentSignals(scored, 5);
  const commonValidationFactors = aggregateCommonValidationFactors(
    scored,
    readiness,
    deal,
    mergedFields
  );
  const patternLabel = alignmentPatternLabel(tierCounts, inReport, universe.limitedData, scored);
  const readinessInterp = readinessInterpretation(
    readinessStageFromPayload(readiness),
    readinessScoreFromPayload(readiness)
  );
  const helpers = { topBrandNames, alignmentPatternLabel: patternLabel };

  const paragraphs = [
    buildSummaryParagraph1(deal, readiness),
    buildSummaryParagraph2(universe, scored, tierCounts, helpers),
    buildSummaryParagraph3(dominantSignals, mergedFields, preferredBrandCount || 0),
    buildSummaryParagraph4(commonValidationFactors),
    buildSummaryParagraph5(),
  ];
  return {
    brandAlignmentSummaryParagraphs: paragraphs,
    brandAlignmentSummary: paragraphs.join("\n\n"),
    tierCounts,
    topBrandNames,
    topBrandCount: topBrandNames.length,
    dominantAlignmentSignals: dominantSignals,
    strongestRecurringSignals: dominantSignals.slice(0, 3),
    commonValidationFactors,
    commonLimitingFactors: commonValidationFactors,
    alignmentPatternLabel: patternLabel,
    brandUniverseSourceLabel: brandUniverseSourceLabel(universe.brands),
    readinessStage: readinessStageFromPayload(readiness),
    readinessInterpretation: readinessInterp,
  };
}

function buildNoBrandsSummary(deal, readiness, mergedFields) {
  const p1 = buildSummaryParagraph1(deal, readiness);
  const validationFactors = aggregateCommonValidationFactors([], readiness, deal, mergedFields);
  const p2 =
    "No brands are currently in the review set from owner-preferred selections, the deal pipeline, or deal brand cache candidates. " +
    "Add preferred brands or pipeline brands to generate brand-level alignment signals and tier distribution for this summary.";
  const p4 = validationFactors.length
    ? buildSummaryParagraph4(validationFactors)
    : "Deal readiness and setup inputs should be completed before a brand review set can support meaningful alignment screening.";
  const p5 = buildSummaryParagraph5();
  const paragraphs = [p1, p2, p4, p5];
  const readinessInterp = readinessInterpretation(
    readinessStageFromPayload(readiness),
    readinessScoreFromPayload(readiness)
  );
  return {
    brandAlignmentSummaryParagraphs: paragraphs,
    brandAlignmentSummary: paragraphs.join("\n\n"),
    tierCounts: countTiers([]),
    topBrandNames: [],
    topBrandCount: 0,
    dominantAlignmentSignals: [],
    strongestRecurringSignals: [],
    commonValidationFactors: validationFactors,
    commonLimitingFactors: validationFactors,
    alignmentPatternLabel: "no brands in scope",
    brandUniverseSourceLabel: "none (no brands in scope)",
    readinessStage: readinessStageFromPayload(readiness),
    readinessInterpretation: readinessInterp,
  };
}

function currentReviewStatusFromReadiness(stage) {
  const s = String(stage || "").trim();
  const sl = s.toLowerCase();
  if (sl === "discovery" || sl === "shaping") {
    return "Early brand review; additional deal clarity needed";
  }
  if (sl === "advancing") {
    return "Eligible for internal brand alignment review";
  }
  if (sl.indexOf("ready for external") >= 0) {
    return "Ready for controlled brand review after owner/advisor validation";
  }
  if (sl === "ready") {
    return "Ready for advanced owner/advisor brand review";
  }
  if (s) return "Current readiness stage: " + s + "; owner/advisor validation still applies";
  return "Brand alignment review pending deal readiness validation";
}

function buildPathwayView(fields, preferredNames) {
  const softHard = fieldPresent(fields, [
    "Soft vs Hard Brand Preference",
    "Do you prefer a soft brand, hard brand, or are you open to both?",
  ]);
  const projectType = fieldPresent(fields, ["Project Type"]);
  const rows = [];

  const add = (pathway, why, clarification) => {
    rows.push({ brandPathway: pathway, whyItMayMeritReview: why, clarificationNeeded: clarification });
  };

  add(
    "Soft brand / collection affiliation",
    "May be relevant when the owner seeks lighter standards, local identity, or affiliation flexibility.",
    softHard ? "Confirm owner preference: " + softHard : "Confirm soft vs hard brand preference with the owner."
  );
  add(
    "Flexible upscale hard brand",
    "May be relevant when positioning targets upscale or upper-midscale with defined brand standards.",
    "Validate chain scale, service model, and standards tolerance against the project."
  );
  add(
    "Lifestyle / design-led brand",
    "May be relevant when design narrative, guest experience, or lifestyle positioning is emphasized.",
    "Confirm design story, F&B/program assumptions, and brand creative requirements."
  );
  add(
    "Upper-upscale hard brand",
    "May be relevant when the project targets upper-upscale positioning and full brand system support.",
    "Validate fee structure, approval requirements, and capital expectations with the owner."
  );
  if (isConversionDeal(projectType)) {
    add(
      "Conversion-oriented brand path",
      "May be relevant for conversion or repositioning where PIP, standards, and reflag pathway matter.",
      "Validate conversion pathway, PIP range, and current brand status before outreach."
    );
  }
  if (preferredNames.length > 0) {
    add(
      "Owner-selected brand path",
      "Brands identified in strategic intent or the deal pipeline appear in the review set below.",
      "Confirm which owner-selected brands remain in scope for controlled review."
    );
  } else {
    add(
      "Owner-selected brand path",
      "Available when the owner identifies preferred brands or adds brands to the deal pipeline.",
      "Add preferred brands or pipeline brands to enable owner-selected pathway review."
    );
  }

  return rows;
}

function buildPrimaryReviewConsiderations() {
  return [
    "CapEx / PIP assumptions should be validated against owner tolerance and brand expectations.",
    "Brand standards tolerance and required program elements should be confirmed.",
    "Soft vs hard brand preference should be clarified with the owner.",
    "Owner identity / local story may affect lifestyle or collection pathways.",
    "Operating model (self-managed vs third-party) should be aligned before outreach.",
    "Loyalty / distribution importance should be confirmed if relevant to the owner.",
    "Deal structure preference (franchise, management, lease) should be validated.",
    "Market/competitive context should be reviewed against brand footprint and positioning.",
  ];
}

function clarificationFromReadiness(readiness) {
  const items = [];
  const seen = new Set();
  const push = (text) => {
    const t = String(text || "").trim();
    if (!t || seen.has(t.toLowerCase())) return;
    seen.add(t.toLowerCase());
    items.push(t);
  };
  (readiness.missingInformation || []).forEach((m) => {
    push((m && (m.label || m.field)) || "");
  });
  (readiness.weakInformation || []).forEach((w) => {
    push((w && (w.label || w.field)) || "");
  });
  (readiness.blockingIssues || []).forEach((b) => {
    push((b && (b.label || b.field || b.message)) || "");
  });
  return items.slice(0, 12);
}

/**
 * Build ordered brand universe: owner preferred → target list (non-deleted) → cache alternatives.
 */
function buildBrandUniverse({ preferredBrandNames, targets, cacheRow, maxBrands }) {
  const order = [];
  const notes = [];
  const seen = new Set();

  const add = (name, source) => {
    const display = String(name || "").trim();
    if (!display || display.toLowerCase() === "not specified") return;
    const key = normalizeBrandKey(display);
    if (seen.has(key)) return;
    seen.add(key);
    order.push({ brandName: display, source });
  };

  for (const n of preferredBrandNames || []) add(n, "owner_preferred");
  if (preferredBrandNames?.length) {
    notes.push("Owner-preferred brands from Strategic Intent included.");
  }

  for (const t of targets || []) {
    const status = String(t.status || "").trim();
    if (status.toLowerCase() === "deleted") continue;
    add(t.brandName, "target_list");
  }
  const targetCount = order.filter((b) => b.source === "target_list").length;
  if (targetCount) notes.push("Brands from deal target list / pipeline included.");

  const alts = cacheRow?.topAlternatives;
  if (Array.isArray(alts)) {
    for (const a of alts) {
      add(a.brand || a.brandName, "deal_brand_cache");
    }
    if (alts.length) notes.push("Additional brands from deal brand cache candidates included.");
  }

  const capped = order.slice(0, Math.max(1, Math.min(20, Number(maxBrands) || 8)));
  return {
    brands: capped,
    brandCount: order.length,
    notes,
    limitedData: order.length < 3,
    noBrands: order.length === 0,
  };
}

function dealMetaFromMerged(mergedFields, normalized, dealId) {
  const f = mergedFields || {};
  const n = normalized || {};
  const name =
    fieldPresent(f, ["Project Name", "Property Name", "Name"]) ||
    (n.propertyName && String(n.propertyName)) ||
    "Deal";
  const market =
    fieldPresent(f, ["City", "Market", "Submarket"]) ||
    (n.city && n.country ? `${n.city}, ${n.country}` : n.city || n.market || "");
  const country = fieldPresent(f, ["Country"]) || (n.country && String(n.country)) || "";
  const keyCountRaw = fieldPresent(f, ["Total Number of Rooms/Keys", "Number of Rooms/Keys"]);
  let keyCount = null;
  const kn = parseInt(String(keyCountRaw).replace(/[^\d]/g, ""), 10);
  if (Number.isFinite(kn)) keyCount = kn;
  const projectType = fieldPresent(f, ["Project Type"]) || (n.projectType && String(n.projectType)) || "";
  const targetPositioning = fieldPresent(f, [
    "Brand Positioning",
    "Target Chain Scale",
    "Hotel Chain Scale",
  ]);
  return { id: dealId, name, market: market || "—", country: country || "—", keyCount, projectType, targetPositioning };
}

export async function postBrandAlignmentSnapshot(req, res) {
  try {
    const dealId = req.body && typeof req.body.dealId === "string" ? req.body.dealId.trim() : "";
    const brandUniverseMode =
      (req.body && req.body.brandUniverse) || "owner_preferred_then_pipeline";
    const maxBrands = Math.min(20, Math.max(1, Number(req.body?.maxBrands) || 8));

    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid dealId (Airtable record id) is required" });
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const [full, scoringCtx, targets, preliminaryCache] = await Promise.all([
      fetchDealWithMergedLinkedRecords(baseId, apiKey, dealId),
      fetchDealScoringContext(baseId, apiKey, dealId),
      fetchTargetsForDeal(dealId),
      fetchDealBrandCacheForDeal(baseId, apiKey, dealId),
    ]);

    if (!full || !scoringCtx) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    const mergedFields = full.deal.fields || {};
    const readinessPayload = buildReadinessFromFields(mergedFields);
    const deal = dealMetaFromMerged(mergedFields, full.normalized, dealId);

    const universe = buildBrandUniverse({
      preferredBrandNames: scoringCtx.preferredBrandNames,
      targets,
      cacheRow: preliminaryCache,
      maxBrands,
    });

    if (universe.noBrands) {
      return res.json({
        success: true,
        limitedData: true,
        noBrands: true,
        deal,
        readiness: {
          score: readinessPayload.dealReadinessScore,
          stage: readinessPayload.readinessStage,
          validationItems: clarificationFromReadiness(readinessPayload)
            .map((t) => formatClarificationLabel(t, deal))
            .slice(0, 8),
        },
        brandUniverse: {
          source: brandUniverseMode,
          brandCount: 0,
          brandUniverseSourceLabel: "none (no brands in scope)",
          tierCounts: countTiers([]),
          notes: ["No owner-preferred, pipeline, or cache brands available for this deal."],
        },
        summary: {
          ...buildNoBrandsSummary(deal, readinessPayload, mergedFields),
          currentReviewStatus: currentReviewStatusFromReadiness(readinessPayload.readinessStage),
          pathwayView: buildPathwayView(mergedFields, []),
        },
        brands: [],
        primaryReviewConsiderations: buildPrimaryReviewConsiderations(),
        clarificationAreas: clarificationFromReadiness(readinessPayload)
          .map((t) => formatClarificationLabel(t, deal))
          .slice(0, 8),
        commonQuestionsToClarify: COMMON_QUESTIONS_BEFORE_OUTREACH,
        methodologyNote: METHODOLOGY_NOTE,
        outputStatus: OUTPUT_STATUS,
        generatedAt: new Date().toISOString(),
      });
    }

    const requiredBrandNames = universe.brands.map((b) => b.brandName).filter(Boolean);
    const cacheRow = await ensureDealBrandCacheFresh(baseId, apiKey, dealId, { requiredBrandNames });
    const scoreIndex = buildScoresIndexFromCacheRow(cacheRow);
    const scored = [];

    for (const entry of universe.brands) {
      const brandName = entry.brandName;
      const score = getScoreFromCacheIndex(scoreIndex, brandName);
      const breakdownNewDetails = getBreakdownFromCacheIndex(scoreIndex, brandName);
      let parentCompany = "—";

      const brandData = await fetchBrandData(baseId, apiKey, brandName);
      if (brandData?.brandBasics) {
        parentCompany = strVal(brandData.brandBasics["Parent Company"]) || "—";
      }

      const tier = tierFromScore(score);
      const scoreDisplay = Number.isFinite(Number(score)) ? Number(score) : null;
      const reviewContent = buildBrandReviewContent({
        brandName,
        tier,
        scoreAvailable: scoreDisplay != null,
        score: scoreDisplay,
        breakdownNewDetails,
        deal,
        mergedFields,
        brandData,
        source: entry.source,
        preferredBrandNames: scoringCtx.preferredBrandNames,
        parentCompany,
      });

      scored.push({
        brandId: brandData ? null : null,
        brandName,
        parentCompany,
        score: scoreDisplay,
        tier,
        reviewStatus: reviewStatusForBrand(tier, deal.projectType),
        source: entry.source,
        scoreAvailable: scoreDisplay != null,
        ...reviewContent,
        reviewConsiderations: reviewContent.whatNeedsValidation.slice(0, 6),
      });
    }

    scored.sort((a, b) => {
      const sa = Number.isFinite(Number(a.score)) ? Number(a.score) : -1;
      const sb = Number.isFinite(Number(b.score)) ? Number(b.score) : -1;
      return sb - sa;
    });

    const executiveSummary = buildExecutiveBrandAlignmentSummary(
      deal,
      readinessPayload,
      universe,
      scored,
      mergedFields,
      (scoringCtx.preferredBrandNames || []).length
    );

    const clarificationAreas = [
      ...clarificationFromReadiness(readinessPayload).map((t) => formatClarificationLabel(t, deal)),
      ...scored
        .filter((b) => !b.scoreAvailable)
        .map((b) => b.brandName + ": alignment signal requires additional deal or brand data."),
    ].slice(0, 12);

    res.json({
      success: true,
      limitedData: universe.limitedData,
      noBrands: false,
      deal,
      readiness: {
        score: readinessPayload.dealReadinessScore,
        stage: readinessPayload.readinessStage,
        validationItems: clarificationFromReadiness(readinessPayload)
          .map((t) => formatClarificationLabel(t, deal))
          .slice(0, 8),
      },
      brandUniverse: {
        source: brandUniverseMode,
        brandCount: universe.brandCount,
        brandUniverseSourceLabel: executiveSummary.brandUniverseSourceLabel,
        tierCounts: executiveSummary.tierCounts,
        notes: universe.notes,
      },
      summary: {
        ...executiveSummary,
        currentReviewStatus: currentReviewStatusFromReadiness(readinessPayload.readinessStage),
        pathwayView: buildPathwayView(mergedFields, scoringCtx.preferredBrandNames),
      },
      brands: scored,
      primaryReviewConsiderations: buildPrimaryReviewConsiderations(),
      clarificationAreas: clarificationAreas.slice(0, 8),
      commonQuestionsToClarify: COMMON_QUESTIONS_BEFORE_OUTREACH,
      methodologyNote: METHODOLOGY_NOTE,
      outputStatus: OUTPUT_STATUS,
      generatedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error("postBrandAlignmentSnapshot:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}
