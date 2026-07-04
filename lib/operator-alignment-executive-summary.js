/**
 * Operator Alignment Snapshot — executive summary (Brand Alignment Summary parity).
 * Neutral language only; no scoring changes.
 */

import { normalizeOperatorAlignmentDealInputs } from "./operator-alignment-deal-normalize.js";

const NOT_PROVIDED = "Not provided";

function joinNaturalList(items) {
  const list = (items || []).filter(Boolean);
  if (!list.length) return "";
  if (list.length === 1) return list[0];
  if (list.length === 2) return list[0] + " and " + list[1];
  return list.slice(0, -1).join(", ") + ", and " + list[list.length - 1];
}

function capitalizeLead(text) {
  const t = String(text || "").trim();
  if (!t) return t;
  return t.charAt(0).toUpperCase() + t.slice(1);
}

function formatMarketCountryLine(dealContext, deal) {
  const city =
    (deal && deal.dealCity) ||
    (dealContext.cityOrMarket && dealContext.cityOrMarket !== NOT_PROVIDED
      ? dealContext.cityOrMarket
      : "");
  const country =
    (deal && deal.dealCountry) ||
    (dealContext.country && dealContext.country !== NOT_PROVIDED ? dealContext.country : "");
  if (!city && !country) return "the stated market";
  if (!city) return country;
  if (!country) return city;
  if (String(city).toLowerCase().includes(String(country).toLowerCase())) return city;
  return city + ", " + country;
}

function formatChainScale(chainScale) {
  const s = String(chainScale || "").trim();
  if (!s || s === NOT_PROVIDED) return "";
  return s.toLowerCase().replace(/\s+/g, "-");
}

function formatProjectType(projectType) {
  const s = String(projectType || "").trim();
  if (!s || s === NOT_PROVIDED) return "";
  return s.toLowerCase();
}

function describeOperatingStructure(deal) {
  const parts = [];
  const brand = deal?.brandAgreementStructure;
  const op = deal?.operatingModel;
  if (brand) {
    parts.push("a " + String(brand).toLowerCase() + " brand path");
  }
  if (op) {
    if (parts.length) {
      parts.push("with a " + String(op).toLowerCase() + " operating model in scope");
    } else {
      parts.push("a " + String(op).toLowerCase() + " operating model in scope");
    }
  }
  if (!parts.length) {
    const legacy = deal?.legacyDealStructure;
    if (legacy) return "deal structure inputs referencing " + String(legacy).toLowerCase();
    return "operating path details that should be confirmed on the deal record";
  }
  return parts.join(" ");
}

function countAlignmentBands(companies) {
  const counts = { strong: 0, moderate: 0, conditional: 0, limited: 0, insufficient: 0 };
  for (const c of companies || []) {
    const b = String(c.alignmentBand || "");
    if (/strong/i.test(b)) counts.strong += 1;
    else if (/moderate/i.test(b)) counts.moderate += 1;
    else if (/conditional/i.test(b)) counts.conditional += 1;
    else if (/limited/i.test(b)) counts.limited += 1;
    else counts.insufficient += 1;
  }
  return counts;
}

function describeAlignmentPattern(counts, scoredCount, companiesAvailable) {
  if (!companiesAvailable || scoredCount === 0) {
    return "profile-level pathways only until company-level Operator Setup data is complete enough";
  }
  const strong = counts.strong || 0;
  const moderate = counts.moderate || 0;
  const conditional = counts.conditional || 0;
  if (strong >= Math.max(2, Math.ceil(scoredCount * 0.5))) {
    return "concentrated among Strong Alignment Signals";
  }
  if (strong + moderate >= Math.max(2, Math.ceil(scoredCount * 0.55))) {
    return "concentrated among Strong and Moderate Alignment Signals";
  }
  if (conditional + moderate + strong >= Math.ceil(scoredCount * 0.5)) {
    return "spread across Moderate and Conditional Alignment Signals with some Strong signals";
  }
  return "mixed across Moderate, Conditional, and Limited Alignment Signals";
}

function formatTopCompaniesSentence(companies, max = 3) {
  const pool = (companies || []).filter((c) => c.operatorName || c.companyName);
  const strong = pool.filter((c) => /strong/i.test(c.alignmentBand));
  const moderate = pool.filter((c) => /moderate/i.test(c.alignmentBand));
  const lead = strong.length >= 2 ? strong : strong.concat(moderate);
  const names = lead.slice(0, max).map((c) => c.operatorName || c.companyName);
  if (!names.length) return "";
  if (names.length === 1) {
    return (
      names[0] + " currently shows stronger alignment signals in the review set, based on available market, service platform, management structure, and project-stage inputs"
    );
  }
  return (
    joinNaturalList(names) +
    " currently show stronger alignment signals in the review set, based on available market, service platform, management structure, and pre-opening inputs"
  );
}

function aggregateRecurringSignals(companies) {
  const tallies = new Map();
  const rules = [
    [/market|geograph|mexico|cancún|cancun|presence in/i, "market presence"],
    [/management|third-party|franchise|operating path/i, "management structure overlap"],
    [/service|offered|must-have|platform/i, "service platform coverage"],
    [/pre-opening|preopening|opening support|new-build/i, "pre-opening support"],
    [/reporting|governance|institutional|monthly review/i, "owner reporting"],
    [/revenue|commercial|rm capability|sales support/i, "commercial platform capability"],
    [/select-service|chain-scale|chain scale|service model/i, "chain-scale and service model compatibility"],
  ];
  for (const c of (companies || []).slice(0, 8)) {
    const blob = []
      .concat(c.whatSupportsReview || [])
      .concat(c.factorsReviewed || [])
      .concat(c.alignmentSignals || [])
      .join(" ");
    for (const [re, label] of rules) {
      if (re.test(blob)) tallies.set(label, (tallies.get(label) || 0) + 1);
    }
  }
  const ordered = [...tallies.entries()].sort((a, b) => b[1] - a[1]).map(([label]) => label);
  if (ordered.length >= 2) return ordered.slice(0, 6);
  return [
    "market presence",
    "management structure overlap",
    "service platform coverage",
    "pre-opening support",
    "owner reporting",
    "chain-scale and service model compatibility",
  ].slice(0, 5);
}

function aggregateValidationThemes(companies, deal) {
  const tallies = new Map();
  const rules = [
    [/pre-opening|opening support scope/i, "pre-opening scope"],
    [/market coverage|active operations|mexico|cancún/i, "active market coverage"],
    [/brand\/operator|franchise|responsibility split/i, "brand/operator responsibility split"],
    [/fee|commercial/i, "fee/commercial assumptions"],
    [/service platform|must-have|service delivery/i, "service platform depth for must-have services"],
    [/reporting|governance|cadence|institutional/i, "reporting and governance expectations"],
    [/resort|all-inclusive|select-service/i, "resort vs. select-service fit"],
    [/inferred|operator setup data/i, "Operator Setup field completeness"],
  ];
  for (const c of (companies || []).slice(0, 8)) {
    const blob = []
      .concat(c.whatNeedsValidation || [])
      .concat(c.reviewConsiderations || [])
      .concat(c.keyConsideration || "")
      .join(" ");
    for (const [re, label] of rules) {
      if (re.test(blob)) tallies.set(label, (tallies.get(label) || 0) + 1);
    }
  }
  const fromCompanies = [...tallies.entries()].sort((a, b) => b[1] - a[1]).map(([l]) => l);
  const contextual = [];
  if (deal?.preOpeningSupportNeeded && /yes/i.test(deal.preOpeningSupportNeeded)) {
    contextual.push("pre-opening scope");
  }
  if (deal?.brandAgreementStructure && /franchise/i.test(deal.brandAgreementStructure)) {
    contextual.push("brand/operator responsibility split");
  }
  if (deal?.marketPresenceRequirement) contextual.push("active market coverage");
  if (deal?.mustHaveOperatorServices?.length) {
    contextual.push("service platform depth for must-have services");
  }
  if (deal?.ownerReportingExpectations) contextual.push("reporting and governance expectations");
  const merged = [];
  const seen = new Set();
  for (const x of [...fromCompanies, ...contextual]) {
    const k = x.toLowerCase();
    if (!x || seen.has(k)) continue;
    seen.add(k);
    merged.push(x);
  }
  return merged.slice(0, 5);
}

function buildParagraph1DealContext(dealContext, deal) {
  const name =
    dealContext.dealName && dealContext.dealName !== NOT_PROVIDED
      ? dealContext.dealName
      : "This deal";
  const marketLine = formatMarketCountryLine(dealContext, deal);
  const descriptor = [];
  if (dealContext.roomCount != null && Number.isFinite(dealContext.roomCount)) {
    descriptor.push(dealContext.roomCount + "-key");
  }
  const chain = formatChainScale(dealContext.chainScale);
  if (chain) descriptor.push(chain);
  const ptype = formatProjectType(dealContext.projectType);
  if (ptype) descriptor.push(ptype);

  let s = "The current inputs describe " + name;
  if (descriptor.length) {
    s += " as a " + descriptor.join(", ") + " hospitality opportunity";
  } else {
    s += " as a hospitality opportunity";
  }
  s += " in " + marketLine + ".";
  s += " The deal is currently structured around " + describeOperatingStructure(deal) + ".";
  return s;
}

function buildParagraph2ReviewSet(opts) {
  const {
    companiesAvailable,
    totalInSet,
    shownInTable,
    profilePathwayCount,
    tierCounts,
    scoredCount,
  } = opts;

  if (!companiesAvailable) {
    return (
      "The current snapshot shows " +
      (profilePathwayCount || 0) +
      " operator profile pathways for archetype-level review. Company-level alignment will appear once Operator Setup records are complete enough for this deal. " +
      "This should be interpreted as an internal screening view, not a final operator selection or indication of operator interest."
    );
  }

  let s =
    "The current company-level review set includes " +
    totalInSet +
    " operating " +
    (totalInSet === 1 ? "company" : "companies");
  if (shownInTable < totalInSet) {
    s += ", with " + shownInTable + " shown in this snapshot table and detail section";
  }
  s += ". Based on available deal and Operator Setup data, the alignment pattern is " + describeAlignmentPattern(tierCounts, scoredCount, true) + ".";
  s +=
    " This should be interpreted as an internal screening view, not a final operator selection or indication of operator availability.";
  return s;
}

function buildParagraph3TopCompanies(companies) {
  const lead = formatTopCompaniesSentence(companies, 3);
  if (lead) return capitalizeLead(lead) + ".";
  return (
    "Leading operators cannot be ranked clearly from the review set because several entries lack sufficient structured Operator Setup inputs for company-level comparison."
  );
}

function buildParagraph4RecurringSignals(companies) {
  const signals = aggregateRecurringSignals(companies);
  if (!signals.length) {
    return (
      "Recurring alignment signals are limited across the review set because several operators lack sufficient scored factors. " +
      "Additional deal and Operator Setup inputs may be needed before pathway patterns are visible."
    );
  }
  return (
    "The strongest recurring signals appear to be " +
    joinNaturalList(signals) +
    ", subject to owner/advisor confirmation against the deal's structured intake fields."
  );
}

function buildParagraph5Validation(companies, deal) {
  const themes = aggregateValidationThemes(companies, deal);
  if (!themes.length) {
    return (
      "Several standard alignment checks remain lightly documented across the review set. " +
      "Owner/advisor review of deal setup completeness is still appropriate before controlled operator outreach."
    );
  }
  return (
    "Several important factors still require validation before controlled operator outreach, including " +
    joinNaturalList(themes) +
    ". These items may affect how each operator views the opportunity and should be clarified before external discussions."
  );
}

function buildParagraph6InternalUse() {
  return (
    "This snapshot should be used as an internal screening and discussion tool. " +
    "It organizes potential operator alignment signals and review considerations, but it does not determine final operator selection, operator interest, approval, availability, or commercial terms."
  );
}

/**
 * @param {object} params
 * @param {object} params.dealContext — buildDealContextFromMerged
 * @param {object} params.dealFields
 * @param {object|null} params.locationData
 * @param {object|null} params.mpData
 * @param {object|null} params.siData
 * @param {boolean} params.companiesAvailable
 * @param {object[]} params.companiesForConsideration — ranked company rows
 * @param {number} [params.tableShownLimit=8]
 * @param {number} [params.profilePathwayCount=0]
 * @param {number} [params.activeOperatorRecords=0]
 */
export function buildOperatorAlignmentExecutiveSummary(params) {
  const deal = normalizeOperatorAlignmentDealInputs(
    params.dealFields,
    params.locationData,
    params.mpData,
    params.siData
  );
  const dealContext = params.dealContext || {};
  const companies = params.companiesForConsideration || [];
  const companiesAvailable = Boolean(params.companiesAvailable && companies.length);
  const tableShownLimit = params.tableShownLimit ?? 8;
  const tierCounts = countAlignmentBands(companies);
  const scoredCount = companies.filter((c) => !/insufficient/i.test(c.alignmentBand)).length;
  const totalInSet = companiesAvailable
    ? companies.length
    : params.activeOperatorRecords || 0;
  const shownInTable = companiesAvailable ? Math.min(tableShownLimit, companies.length) : 0;

  const paragraphs = [
    buildParagraph1DealContext(dealContext, deal),
    buildParagraph2ReviewSet({
      companiesAvailable,
      totalInSet,
      shownInTable,
      profilePathwayCount: params.profilePathwayCount || 0,
      tierCounts,
      scoredCount,
    }),
    companiesAvailable ? buildParagraph3TopCompanies(companies) : null,
    companiesAvailable ? buildParagraph4RecurringSignals(companies) : null,
    buildParagraph5Validation(companiesAvailable ? companies : [], deal),
    buildParagraph6InternalUse(),
  ].filter(Boolean);

  const wordCount = paragraphs.join(" ").split(/\s+/).filter(Boolean).length;

  return {
    operatorAlignmentSummaryParagraphs: paragraphs,
    operatorAlignmentSummary: paragraphs.join("\n\n"),
    tierCounts,
    topCompanyNames: companies
      .filter((c) => /strong|moderate/i.test(c.alignmentBand))
      .slice(0, 3)
      .map((c) => c.operatorName || c.companyName),
    alignmentPatternLabel: describeAlignmentPattern(tierCounts, scoredCount, companiesAvailable),
    recurringSignals: aggregateRecurringSignals(companies),
    validationThemes: aggregateValidationThemes(companies, deal),
    wordCount,
  };
}

export {
  joinNaturalList,
  countAlignmentBands,
  describeAlignmentPattern,
  aggregateRecurringSignals,
  aggregateValidationThemes,
};
