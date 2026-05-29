/**
 * Phase 5E — Factor scoring helpers for Operator Alignment (weights unchanged).
 */

import { OAS_DEAL_SI_FIELD_NAMES as SI } from "./operator-alignment-field-options.js";
import {
  scoringCanonicalize,
  scoringCanonicalOverlap,
  labelsToCanonicalSet,
  getCanonicalCategory,
} from "./operator-alignment-scoring-option-utils.js";
import { normalizeOptionKey } from "./operator-alignment-airtable-options-loader.js";

function toStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
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

function normKey(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function overlapScore(dealVals, operatorVals, partial = 35) {
  const d = new Set((dealVals || []).map((x) => normKey(x)).filter(Boolean));
  const o = new Set((operatorVals || []).map((x) => normKey(x)).filter(Boolean));
  if (d.size === 0 || o.size === 0) return null;
  let intersection = 0;
  for (const v of d) if (o.has(v)) intersection += 1;
  if (intersection === 0) return partial;
  const ratio = intersection / d.size;
  return Math.min(100, Math.max(0, Math.round((40 + ratio * 60) * 10) / 10));
}

function fuzzyOverlapScore(dealVals, operatorVals, partial = 40) {
  const d = (dealVals || []).map((x) => normKey(x)).filter(Boolean);
  const o = (operatorVals || []).map((x) => normKey(x)).filter(Boolean);
  if (d.length === 0 || o.length === 0) return null;

  let exactHits = 0;
  let partialHits = 0;
  for (const dv of d) {
    if (o.some((ov) => ov === dv)) {
      exactHits += 1;
      continue;
    }
    if (o.some((ov) => ov.includes(dv) || dv.includes(ov))) partialHits += 1;
  }
  if (exactHits === 0 && partialHits === 0) return partial;
  const ratio = (exactHits + partialHits * 0.5) / d.length;
  return Math.min(100, Math.max(0, Math.round((40 + ratio * 60) * 10) / 10));
}

const STRUCTURE_MATCH_HINTS = {
  "full third-party management": [
    "full third-party",
    "third-party management",
    "third party management",
    "management agreement",
    "operator management",
    "hotel management",
  ],
  "franchise with third-party operator": [
    "franchise with third-party",
    "franchise support",
    "third-party",
    "management",
    "franchise",
  ],
  "third-party managed": ["full third-party", "third-party", "management"],
  franchise: ["franchise", "third-party", "brand compliance", "brand support"],
};

function expandStructureTargets(values) {
  const out = new Set();
  for (const v of values || []) {
    const n = normKey(v);
    if (!n) continue;
    out.add(n);
    const hints = STRUCTURE_MATCH_HINTS[n];
    if (hints) for (const h of hints) out.add(h);
  }
  return [...out];
}

function operatorMatchesStructureTargets(targets, opStructures) {
  const dealC = scoringCanonicalize(targets, "si", SI.preferredManagementStructure);
  const opC = scoringCanonicalize(opStructures, "commercial", "Management Structures Supported");
  const d = new Set(dealC.canonicals || []);
  const o = new Set(opC.canonicals || []);
  if (d.size === 0 || o.size === 0) {
    const ops = (opStructures || []).map((x) => normKey(x)).filter(Boolean);
    if (ops.length === 0) return { exact: 0, partial: 0 };
    let exact = 0;
    let partial = 0;
    for (const t of targets) {
      const tk = normKey(t);
      if (!tk) continue;
      if (ops.some((o2) => o2 === tk)) exact += 1;
      else if (ops.some((o2) => o2.includes(tk) || tk.includes(o2))) partial += 1;
    }
    return { exact, partial };
  }
  let exact = 0;
  let partial = 0;
  for (const x of d) {
    if (o.has(x)) exact += 1;
    else if ([...o].some((y) => x.includes(y) || y.includes(x))) partial += 1;
  }
  return { exact, partial };
}

/**
 * Structure: brand agreement and operating model are separate dimensions.
 */
export function scoreDealStructureFactor(deal, opStructures) {
  const hasStructured = deal.hasStructuredStructure;
  const legacyOnly = toStr(deal.legacyDealStructure);
  const dealDisplay = [
    deal.brandAgreementStructure && "Brand agreement: " + deal.brandAgreementStructure,
    deal.operatingModel && "Operating model: " + deal.operatingModel,
    deal.preferredManagementStructure.length &&
      "Preferred management: " + deal.preferredManagementStructure.join("; "),
    deal.operatorScope.length && "Operator scope: " + deal.operatorScope.join("; "),
    !hasStructured && legacyOnly && "Legacy preferred structure: " + legacyOnly,
  ]
    .filter(Boolean)
    .join(" | ");

  const fieldSource = hasStructured
    ? "structured"
    : legacyOnly
      ? "legacy_mp"
      : "none";

  if (!hasStructured && !legacyOnly) {
    return {
      score: null,
      dealValue: dealDisplay || "—",
      fieldSource,
      missingDataClass: "excluded",
      rationale: null,
      note: "No deal structure inputs documented.",
    };
  }

  if (opStructures.length === 0) {
    return {
      score: null,
      dealValue: dealDisplay || "—",
      fieldSource,
      missingDataClass: "needs_validation",
      rationale:
        "Management structure requires validation because the operator profile does not yet document supported agreement structures.",
      note: "Operator management structures are not documented — excluded from weighted score; validation needed.",
    };
  }

  if (hasStructured) {
    const mgmtTargets = [...deal.preferredManagementStructure];
    if (deal.operatingModel && normKey(deal.operatingModel) === "third-party managed") {
      if (!mgmtTargets.some((x) => normKey(x) === "full third-party management")) {
        mgmtTargets.push("Full third-party management");
      }
    }
    const expanded = expandStructureTargets(mgmtTargets);
    const { exact, partial } = operatorMatchesStructureTargets(expanded, opStructures);
    let score = null;
    if (exact > 0) score = 100;
    else if (partial > 0) score = 72;
    else {
      const fuzzy = fuzzyOverlapScore(mgmtTargets, opStructures, 48);
      score = fuzzy != null ? fuzzy : 48;
    }

    const brandNote =
      normKey(deal.brandAgreementStructure) === "franchise"
        ? "Brand agreement is franchise; this is evaluated separately from third-party operating path."
        : "";

    const rationale =
      score >= 75
        ? "Management structure appears directionally aligned with the deal's third-party management path."
        : score >= 50
          ? "Management structure shows partial overlap — confirm franchise vs operator responsibilities."
          : "Management structure overlap is limited with documented operator structures — validate before shortlist.";

    return {
      score,
      dealValue: dealDisplay,
      fieldSource: "structured",
      missingDataClass: null,
      rationale: [rationale, brandNote].filter(Boolean).join(" "),
      note:
        "Compares operating model and preferred management structures to operator-supported structures (brand agreement is not treated as conflicting with third-party operations).",
    };
  }

  const lower = legacyOnly.toLowerCase();
  const exact = opStructures.some((s) => normKey(s) === normKey(legacyOnly));
  const partial = opStructures.some(
    (s) => normKey(s).includes(lower) || lower.includes(normKey(s))
  );
  const score = exact ? 100 : partial ? 65 : 20;

  return {
    score,
    dealValue: "Legacy preferred structure: " + legacyOnly,
    fieldSource: "legacy_mp",
    missingDataClass: score < 50 ? "data_gap" : null,
    rationale:
      score < 50
        ? "Legacy preferred deal structure shows limited overlap with operator-documented structures."
        : "Legacy preferred deal structure shows partial overlap with operator structures.",
    note: "Legacy MP Preferred Deal Structure fallback (structured fields not present).",
  };
}

function dealServiceRequirements(deal) {
  const must = [...deal.mustHaveOperatorServices];
  const req = [...deal.requiredOperatorServices];
  const scope = [...deal.operatorScope];
  const combined = [];
  const seen = new Set();
  for (const list of [must, req, scope]) {
    for (const v of list) {
      const k = normKey(v);
      if (!k || seen.has(k)) continue;
      seen.add(k);
      combined.push(v);
    }
  }
  return { must, req, combined, legacy: deal.legacyMustHaves || [] };
}

const SCOPE_TO_SERVICE = {
  "full management": "Full hotel management",
  "pre-opening support": "Pre-opening planning",
  "brand compliance support": "Brand compliance support",
  "owner reporting": "Owner reporting",
  "technical services coordination": "Technical services coordination",
  "commercial support": "Sales",
};

function mapScopeToServices(scopeList) {
  return (scopeList || [])
    .map((s) => SCOPE_TO_SERVICE[normKey(s)] || s)
    .filter(Boolean);
}

/**
 * Service offerings — structured multis first.
 */
export function scoreServiceOfferingsFactor(deal, opServices, opExtras = {}) {
  const svc = dealServiceRequirements(deal);
  const scopeMapped = mapScopeToServices(deal.operatorScope);
  const dealMust = svc.must.length ? svc.must : svc.combined.length ? svc.combined : [];
  const fieldSource = deal.hasStructuredServices
    ? "structured"
    : svc.legacy.length
      ? "legacy_must_haves"
      : "none";

  const dealDisplay = [
    dealMust.length && "Must-have services: " + dealMust.join(", "),
    svc.req.length && !svc.must.length && "Required services: " + svc.req.join(", "),
    scopeMapped.length && "From operator scope: " + scopeMapped.join(", "),
    !deal.hasStructuredServices && svc.legacy.length && "Legacy must-haves: " + svc.legacy.join(", "),
  ]
    .filter(Boolean)
    .join(" | ");

  const opAll = [
    ...opServices,
    ...toList(opExtras.revenueManagementCapability),
    ...toList(opExtras.preOpeningSupportCapability),
    ...toList(opExtras.serviceModelsSupported),
  ].filter(Boolean);

  if (dealMust.length === 0 && svc.legacy.length === 0) {
    return {
      score: opAll.length ? 75 : null,
      dealValue: dealDisplay || "—",
      fieldSource,
      missingDataClass: "excluded",
      rationale: null,
      note: "No owner service requirements documented.",
    };
  }

  const compareList =
    dealMust.length > 0
      ? [...dealMust, ...scopeMapped.filter((s) => !dealMust.some((m) => normKey(m) === normKey(s)))]
      : svc.legacy;

  if (opAll.length === 0) {
    return {
      score: null,
      dealValue: dealDisplay,
      fieldSource,
      missingDataClass: "needs_validation",
      rationale: "Required operator services cannot be validated until operator service offerings are documented.",
      note: "Operator services not documented — excluded from weighted score.",
    };
  }

  const overlap = scoringCanonicalOverlap(
    compareList,
    opAll,
    "si",
    SI.mustHaveOperatorServices,
    42
  );
  const score = overlap.score != null ? overlap.score : fuzzyOverlapScore(compareList, opAll, 42);
  const rationale =
    score != null && score >= 75
      ? "Documented operator services overlap with the deal's structured must-have service requirements."
      : score != null && score >= 50
        ? "Partial service overlap — validate pre-opening, revenue management, and owner reporting depth."
        : "Limited documented overlap with structured service requirements — validation recommended.";

  return {
    score,
    dealValue: dealDisplay,
    fieldSource,
    missingDataClass: score != null && score < 50 ? "data_gap" : null,
    rationale,
    note: "Compares structured Required/Must-Have Operator Services (and scope) to operator Offered Services and capability fields.",
  };
}

function listIncludesCountry(countries, country) {
  const c = getCanonicalCategory(country) || normalizeOptionKey(country);
  if (!c) return false;
  const canon = scoringCanonicalize(countries, "platform", "Active Countries");
  for (const x of canon.canonicals) {
    if (x === c || x.includes(c) || c.includes(x)) return true;
  }
  return (countries || []).some((x) => normalizeOptionKey(x) === normalizeOptionKey(country) || normalizeOptionKey(x).includes(normalizeOptionKey(country)));
}

function listIncludesMarket(markets, city, country) {
  const cityC = getCanonicalCategory(city) || normalizeOptionKey(city);
  const countryC = getCanonicalCategory(country) || normalizeOptionKey(country);
  const canon = scoringCanonicalize(markets, "platform", "Active Markets / Cities");
  for (const mk of canon.canonicals) {
    if (cityC && (mk === cityC || mk.includes(cityC) || cityC.includes(mk))) return "city";
    if (countryC && (mk.includes(countryC) || countryC.includes(mk))) return "country";
  }
  const cityK = normKey(city);
  const countryK = normKey(country);
  for (const m of markets || []) {
    const mk = normKey(m);
    if (cityK && (mk === cityK || mk.includes(cityK) || cityK.includes(mk))) return "city";
    if (countryK && (mk.includes(countryK) || countryK.includes(mk))) return "country";
  }
  return null;
}

/**
 * Geography — structured countries/markets + requirement tier.
 */
export function scoreGeographyFactor(deal, opMarkets, opCountries, opPresenceTypes) {
  const req = normKey(deal.marketPresenceRequirement);
  const dealDisplay = [
    deal.dealCountry && "Country: " + deal.dealCountry,
    deal.dealCity && "City: " + deal.dealCity,
    deal.marketPresenceRequirement && "Requirement: " + deal.marketPresenceRequirement,
  ]
    .filter(Boolean)
    .join("; ");

  const fieldSource = deal.marketPresenceRequirement
    ? "structured"
    : deal.dealCountry
      ? "location"
      : "none";

  if (!deal.dealCountry && opMarkets.length === 0 && opCountries.length === 0) {
    return {
      score: null,
      dealValue: dealDisplay || "—",
      fieldSource,
      missingDataClass: "excluded",
      rationale: null,
      note: "No geography inputs on deal or operator.",
    };
  }

  if (!deal.dealCountry) {
    return {
      score: opMarkets.length ? 60 : null,
      dealValue: dealDisplay || "—",
      fieldSource,
      missingDataClass: null,
      rationale: null,
      note: "Deal country not specified.",
    };
  }

  if (opCountries.length === 0 && opMarkets.length === 0) {
    return {
      score: null,
      dealValue: dealDisplay,
      fieldSource,
      missingDataClass: "needs_validation",
      rationale: "Active market coverage requires validation — operator countries/markets are not documented.",
      note: "Operator geography not documented — excluded from weighted score.",
    };
  }

  const inCountry = listIncludesCountry(opCountries, deal.dealCountry);
  const marketHit = listIncludesMarket(opMarkets, deal.dealCity, deal.dealCountry);
  const legacyCountry = opMarkets.some((m) =>
    normKey(m).includes(normKey(deal.dealCountry))
  );

  let score = 35;
  let rationale = "";

  if (inCountry && marketHit === "city") {
    score = 100;
    rationale = "Market alignment is supported by documented presence in the deal city/market.";
  } else if (inCountry || marketHit === "city") {
    score = 92;
    rationale = "Market alignment is supported by documented country or city-level presence.";
  } else if (inCountry || legacyCountry) {
    score = req.includes("active country") ? 88 : 80;
    rationale = "Market alignment is supported by documented " + deal.dealCountry + " presence.";
  } else if (req.includes("regional")) {
    score = 58;
    rationale = "Regional operator presence may be acceptable per deal requirement — confirm active country operations.";
  } else {
    score = 48;
    rationale = "Documented operator markets do not clearly include the deal country — validation recommended.";
  }

  return {
    score,
    dealValue: dealDisplay,
    fieldSource,
    missingDataClass: score < 55 && !inCountry && !legacyCountry ? "data_gap" : null,
    rationale,
    note: "Uses Market Presence Requirement with Active Countries / Active Markets when available.",
  };
}

/**
 * Pre-opening / stage fit.
 */
export function scoreAssetStageFactor(deal, opProject, opStages, opExtras = {}) {
  const dealDisplay = [
    deal.dealProjectType && "Project: " + deal.dealProjectType,
    deal.dealStage && "Stage: " + deal.dealStage,
    deal.openingTimeline && "Timeline: " + deal.openingTimeline,
    deal.preOpeningSupportNeeded && "Pre-opening needed: " + deal.preOpeningSupportNeeded,
  ]
    .filter(Boolean)
    .join("; ");

  const projectScore = fuzzyOverlapScore(
    [deal.dealProjectType, deal.dealBuildingType].filter(Boolean),
    opProject,
    30
  );
  const stageScore = fuzzyOverlapScore([deal.dealStage, deal.openingTimeline].filter(Boolean), opStages, 35);

  const preOpeningNeeded = normKey(deal.preOpeningSupportNeeded) === "yes";
  const opPre =
    toStr(opExtras.preOpeningSupportCapability) ||
    toStr(opExtras.newBuildOpeningExperience);
  const opServices = toList(opExtras.offeredServices);
  const hasPreOpeningSvc = opServices.some((s) =>
    /pre-opening|opening|transition/i.test(s)
  );

  let preScore = null;
  if (preOpeningNeeded) {
    if (/strong|advanced/i.test(opPre)) preScore = 95;
    else if (/moderate/i.test(opPre)) preScore = 78;
    else if (hasPreOpeningSvc || /limited/i.test(opPre)) preScore = 65;
    else if (!opPre && opServices.length === 0) preScore = null;
    else preScore = 50;
  }

  if (projectScore == null && stageScore == null && preScore == null) {
    return {
      score: null,
      dealValue: dealDisplay || "—",
      fieldSource: deal.openingTimeline || deal.preOpeningSupportNeeded ? "structured" : "location",
      missingDataClass: "excluded",
      rationale: null,
      note: "Insufficient asset/stage inputs.",
    };
  }

  const weightedParts = [];
  if (projectScore != null) weightedParts.push({ score: projectScore, w: 0.45 });
  if (stageScore != null) weightedParts.push({ score: stageScore, w: 0.25 });
  if (preScore != null) weightedParts.push({ score: preScore, w: 0.3 });
  const wSum = weightedParts.reduce((s, p) => s + p.w, 0);
  const score =
    wSum > 0
      ? Math.round(
          (weightedParts.reduce((s, p) => s + p.score * p.w, 0) / wSum) * 10
        ) / 10
      : null;

  const rationale =
    preOpeningNeeded && preScore != null && preScore >= 75
      ? "Pre-opening support may be relevant given the project's new-build/pre-development status."
      : preOpeningNeeded && preScore == null
        ? "Pre-opening support is required on the deal — operator pre-opening capability needs validation."
        : null;

  return {
    score,
    dealValue: dealDisplay,
    fieldSource: deal.preOpeningSupportNeeded || deal.openingTimeline ? "structured" : "location",
    missingDataClass: preOpeningNeeded && preScore == null ? "needs_validation" : null,
    rationale,
    note: "Combines project type, development stage/timeline, and pre-opening capability signals.",
  };
}

const REPORTING_ALIASES = {
  "monthly operating review": ["monthly", "monthly operating", "institutional reporting"],
  "institutional reporting": ["institutional", "investor", "lender"],
};

function scoreReportingMatch(dealExpectation, opReportingLevel, opGovernance, opReportingLegacy) {
  const exp = normKey(dealExpectation);
  if (!exp) return { score: null, missing: "excluded" };

  const op = [
    normKey(opReportingLevel),
    normKey(opGovernance),
    normKey(opReportingLegacy),
  ].filter(Boolean);

  if (op.length === 0) {
    return { score: null, missing: "needs_validation" };
  }

  const hints = REPORTING_ALIASES[exp] || [exp];
  const hit = op.some((o) => hints.some((h) => o.includes(h) || h.includes(o)));
  if (hit) return { score: 90, missing: null };
  if (op.some((o) => o.includes("monthly") || o.includes("quarterly"))) return { score: 72, missing: null };
  return { score: 55, missing: "data_gap" };
}

/**
 * Systems & reporting + owner relations (governance).
 */
export function scoreSystemsReportingFactor(deal, opSystems, opReportingLegacy, opReportingLevel, opGovernance) {
  const exp = deal.ownerReportingExpectations;
  const dealDisplay = exp ? "Owner reporting expectation: " + exp : "—";

  const { score: repScore, missing } = scoreReportingMatch(
    exp,
    opReportingLevel,
    opGovernance,
    opReportingLegacy
  );

  if (!exp) {
    if (opSystems.length === 0 && !opReportingLegacy) {
      return {
        score: null,
        dealValue: dealDisplay,
        fieldSource: "none",
        missingDataClass: "excluded",
        rationale: null,
        note: "No owner reporting expectation documented on deal.",
      };
    }
    const score = opSystems.length > 0 && opReportingLegacy ? 90 : 70;
    return {
      score,
      dealValue: dealDisplay,
      fieldSource: "legacy",
      missingDataClass: null,
      rationale: null,
      note: "Generic systems/reporting when deal expectation not specified.",
    };
  }

  if (repScore == null) {
    return {
      score: null,
      dealValue: dealDisplay,
      fieldSource: "structured",
      missingDataClass: missing,
      rationale:
        "Owner reporting capability appears relevant to the deal expectation — operator reporting level needs validation.",
      note: "Compares Owner Reporting Expectations to Owner Reporting Level / Governance Cadence.",
    };
  }

  const rationale =
    repScore >= 85
      ? "Owner reporting capability appears relevant to the deal's " + exp + " expectation."
      : "Owner reporting cadence may need validation against deal expectations.";

  return {
    score: repScore,
    dealValue: dealDisplay,
    fieldSource: "structured",
    missingDataClass: missing,
    rationale,
    note: "Compares structured owner reporting expectations to operator reporting/governance fields.",
  };
}

export function buildFactorMeta(result, operatorValue) {
  return {
    score: result.score,
    dealValue: result.dealValue,
    fieldSource: result.fieldSource,
    missingDataClass: result.missingDataClass,
    rationale: result.rationale,
    note: result.note,
    operatorValue,
    includedInDenominator: result.score != null && !Number.isNaN(Number(result.score)),
  };
}
