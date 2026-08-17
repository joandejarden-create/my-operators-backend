/**
 * Operator–Project Alignment factors (de-genericized).
 * Unknown → score 0 contribution, weight remains in denominator.
 * Table-stakes capability presence → never positive points.
 */

import { OPERATOR_PROJECT_FACTORS } from "./config.js";
import {
  isKnownPositive,
  isNotApplicable,
  listValue,
  scalarValue,
} from "./adapters/field-state.js";
import { isTableStakesToken } from "./adapters/operator-from-prefill.js";

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function overlapRatio(needles, haystack) {
  const n = (needles || []).map(norm).filter(Boolean);
  const h = (haystack || []).map(norm).filter(Boolean);
  if (!n.length || !h.length) return null;
  let hits = 0;
  for (const x of n) {
    if (h.some((y) => y === x || y.includes(x) || x.includes(y))) hits += 1;
  }
  return hits / n.length;
}

/**
 * Factor result shape.
 * @typedef {{
 *   key: string,
 *   label: string,
 *   weight: number,
 *   applicable: boolean,
 *   state: 'known'|'unknown'|'not_applicable'|'negative',
 *   score: number,
 *   evidenceClassHint: string|null,
 *   rationale: string,
 *   positiveEvidence: string[],
 *   negativeEvidence: string[],
 *   unknownNotes: string[],
 * }} FactorResult
 */

function knownScore(key, score, rationale, extras = {}) {
  const def = OPERATOR_PROJECT_FACTORS[key];
  return {
    key,
    label: def.label,
    weight: def.weight,
    applicable: true,
    state: "known",
    score: Math.max(0, Math.min(100, score)),
    evidenceClassHint: extras.evidenceClassHint || null,
    rationale,
    positiveEvidence: extras.positiveEvidence || [],
    negativeEvidence: extras.negativeEvidence || [],
    unknownNotes: extras.unknownNotes || [],
  };
}

function unknownFactor(key, note) {
  const def = OPERATOR_PROJECT_FACTORS[key];
  return {
    key,
    label: def.label,
    weight: def.weight,
    applicable: true,
    state: "unknown",
    score: 0,
    evidenceClassHint: null,
    rationale: note,
    positiveEvidence: [],
    negativeEvidence: [],
    unknownNotes: [note],
  };
}

function naFactor(key, note) {
  const def = OPERATOR_PROJECT_FACTORS[key];
  return {
    key,
    label: def.label,
    weight: def.weight,
    applicable: false,
    state: "not_applicable",
    score: 0,
    evidenceClassHint: null,
    rationale: note,
    positiveEvidence: [],
    negativeEvidence: [],
    unknownNotes: [],
  };
}

export function scoreGeographyFactor(project, operator) {
  const key = "geographyMarket";
  const country = scalarValue(project.geography?.country);
  const city = scalarValue(project.geography?.city);
  const opCountries = listValue(operator.geography?.countries);
  const opMarkets = listValue(operator.geography?.markets);
  if (!country && !city) return unknownFactor(key, "Project geography is unknown.");
  if (!opCountries.length && !opMarkets.length) {
    return unknownFactor(key, "Operator geographic coverage is unknown.");
  }
  const cityHit =
    city &&
    opMarkets.some((m) => norm(m).includes(norm(city)) || norm(city).includes(norm(m)));
  const countryHit =
    country &&
    (opCountries.some((c) => norm(c) === norm(country) || norm(c).includes(norm(country))) ||
      opMarkets.some((m) => norm(m).includes(norm(country))));
  if (cityHit) {
    return knownScore(key, 100, `Market-level presence aligns with ${city}.`, {
      positiveEvidence: [`Active market overlap: ${city}`],
    });
  }
  if (countryHit) {
    return knownScore(key, 78, `Country-level presence aligns with ${country}.`, {
      positiveEvidence: [`Active country: ${country}`],
    });
  }
  return knownScore(key, 12, "No documented geographic overlap with the project market.", {
    negativeEvidence: ["Geographic mismatch or no overlapping markets documented"],
  });
}

export function scoreSegmentFactor(project, operator) {
  const key = "segmentPositioning";
  const scale = scalarValue(project.hotelSegment);
  const opScales = listValue(operator.chainScales);
  if (!scale) return unknownFactor(key, "Project chain scale is unknown.");
  if (!opScales.length) return unknownFactor(key, "Operator chain-scale coverage is unknown.");
  if (opScales.some((s) => norm(s) === norm(scale))) {
    return knownScore(key, 100, `Exact chain-scale match (${scale}).`, {
      positiveEvidence: [`Supports ${scale}`],
    });
  }
  if (opScales.some((s) => norm(s).includes(norm(scale)) || norm(scale).includes(norm(s)))) {
    return knownScore(key, 62, `Partial chain-scale proximity to ${scale}.`, {
      positiveEvidence: ["Partial scale overlap"],
    });
  }
  return knownScore(key, 18, `Chain-scale coverage does not include ${scale}.`, {
    negativeEvidence: ["Scale mismatch"],
  });
}

export function scoreAssetDevelopmentFactor(project, operator) {
  const key = "assetDevelopmentExperience";
  const asset = scalarValue(project.assetType);
  const dev = scalarValue(project.developmentType);
  const opAssets = listValue(operator.assetExperience);
  const opSit = listValue(operator.developmentExperience);
  const comps = isKnownPositive(operator.comparables) ? operator.comparables.value : [];

  if (!asset && !dev && !comps.length) {
    return unknownFactor(key, "Insufficient project or comparable experience inputs.");
  }
  if (!opAssets.length && !opSit.length && !comps.length) {
    return unknownFactor(key, "Operator asset/development experience is unknown.");
  }

  let score = 0;
  const positive = [];
  const negative = [];

  // Direct comparable boost (project similarity > portfolio breadth)
  const relevantComps = (comps || []).filter((c) => {
    if (!c) return false;
    const hay = norm([c.hotelType, c.situation, c.region, c.assetType].filter(Boolean).join(" "));
    return (
      (dev && hay.includes(norm(dev).slice(0, 6))) ||
      (asset && hay.includes(norm(asset).slice(0, 6))) ||
      (dev && /turnaround|renovation|conversion|new build|resort|urban/i.test(hay))
    );
  });
  if (relevantComps.length) {
    score = Math.max(score, Math.min(100, 70 + relevantComps.length * 10));
    positive.push(
      `Directly comparable assignment(s): ${relevantComps
        .slice(0, 2)
        .map((c) => c.propertyName || c.name || "comparable")
        .join(", ")}`
    );
  }

  const assetRatio = asset ? overlapRatio([asset], opAssets) : null;
  const devNeedles = [];
  if (dev) {
    devNeedles.push(dev);
    if (/new build/i.test(dev)) devNeedles.push("New Build", "new-build");
    if (/conversion|reflag/i.test(dev)) devNeedles.push("Conversion", "Reflag");
    if (/renovation|reposition|turnaround/i.test(dev)) {
      devNeedles.push("Renovation", "Turnaround", "Repositioning");
    }
    if (/mixed/i.test(dev)) devNeedles.push("Mixed-Use", "Mixed use");
  }
  const sitRatio = devNeedles.length ? overlapRatio(devNeedles, opSit) : null;

  if (assetRatio != null) {
    score = Math.max(score, Math.round(35 + assetRatio * 50));
    if (assetRatio > 0) positive.push(`Asset-type overlap with ${asset}`);
    else negative.push(`Limited asset-type overlap with ${asset}`);
  }
  if (sitRatio != null) {
    score = Math.max(score, Math.round(30 + sitRatio * 55));
    if (sitRatio > 0) positive.push(`Development-situation overlap with ${dev}`);
    else negative.push(`Limited development-type overlap with ${dev}`);
  }

  // Breadth without relevance should not dominate — cap when only broad lists, no comps
  if (!relevantComps.length && opAssets.length >= 5 && (assetRatio == null || assetRatio === 0)) {
    score = Math.min(score, 40);
    negative.push("Broad portfolio lists without project-similar evidence");
  }

  if (!positive.length && !negative.length) {
    return unknownFactor(key, "Could not evaluate comparable asset/development experience.");
  }
  return knownScore(key, score || 20, "Comparable asset and development experience assessed.", {
    positiveEvidence: positive,
    negativeEvidence: negative,
  });
}

export function scoreProjectComplexityFactor(project, operator) {
  const key = "projectComplexity";
  const needs = [];
  if (project.mixedUse?.value) needs.push("mixed-use");
  if (project.brandedResidences?.value) needs.push("branded-residences");
  if (isKnownPositive(project.fbComplexity) && /complex|full-service|multi/i.test(scalarValue(project.fbComplexity))) {
    needs.push("complex-fb");
  }
  if (project.meetingGroupComplexity?.value) needs.push("meetings-groups");

  if (!needs.length) {
    return naFactor(key, "No elevated project-complexity flags on the deal.");
  }

  const hay = [
    ...listValue(operator.assetExperience),
    ...listValue(operator.developmentExperience),
    ...listValue(operator.specialistExperience?.differentiators),
    ...(isKnownPositive(operator.comparables)
      ? operator.comparables.value.map((c) => [c.hotelType, c.situation, c.services].join(" "))
      : []),
  ];
  if (!hay.length) {
    return unknownFactor(key, "Operator complexity experience is unknown for flagged project needs.");
  }

  let hits = 0;
  const positive = [];
  const missing = [];
  for (const n of needs) {
    const patterns =
      n === "mixed-use"
        ? [/mixed/i]
        : n === "branded-residences"
          ? [/residence/i]
          : n === "complex-fb"
            ? [/f&b|food|outlet|restaurant/i]
            : [/meeting|group|convention|banquet/i];
    if (hay.some((h) => patterns.some((p) => p.test(String(h))))) {
      hits += 1;
      positive.push(`Evidence related to ${n}`);
    } else {
      missing.push(n);
    }
  }
  const score = Math.round((hits / needs.length) * 100);
  return knownScore(key, score, "Project-complexity alignment assessed against specialist signals.", {
    positiveEvidence: positive,
    negativeEvidence: missing.map((m) => `No clear evidence for ${m}`),
    unknownNotes: missing.length ? [`Validate: ${missing.join(", ")}`] : [],
  });
}

export function scoreBrandExperienceFactor(project, operator) {
  const key = "brandExperience";
  const preferred = listValue(project.selectedOrEvaluatedBrands);
  if (!preferred.length) {
    return naFactor(key, "No preferred/evaluated brands on the project.");
  }
  const opBrands = [
    ...listValue(operator.brandsOperated),
    ...listValue(operator.brandFamilies),
  ];
  if (!opBrands.length) {
    return unknownFactor(key, "Operator brand portfolio is unknown.");
  }
  const ratio = overlapRatio(preferred, opBrands);
  if (ratio == null) return unknownFactor(key, "Could not compare brand portfolios.");
  if (ratio <= 0) {
    return knownScore(key, 15, "No overlap with preferred brands in documented portfolio.", {
      negativeEvidence: ["Preferred brands not found in operator portfolio"],
    });
  }
  return knownScore(key, Math.round(45 + ratio * 55), "Preferred-brand portfolio overlap found.", {
    positiveEvidence: [`Overlap ratio ${(ratio * 100).toFixed(0)}% of preferred brands`],
  });
}

export function scoreOwnershipGovernanceFactor(project, operator) {
  const key = "ownershipGovernance";
  const reportingReq = scalarValue(project.hardRequirements?.reporting);
  const controlReq = scalarValue(project.hardRequirements?.ownerControl);
  if (!reportingReq && !controlReq) {
    return naFactor(key, "No owner reporting/control requirements specified.");
  }
  const opReporting = scalarValue(operator.ownershipGovernance?.reportingLevel);
  if (!opReporting) {
    return unknownFactor(key, "Operator reporting/governance level is unknown.");
  }
  // Do not score generic "owner relations" narrative keywords as positive differentiation
  const institutional =
    /institutional|lender|board|audit|monthly package/i.test(reportingReq) ||
    /institutional|lender/i.test(controlReq);
  const opInstitutional = /institutional|lender|board|audit|monthly/i.test(opReporting);
  if (institutional && opInstitutional) {
    return knownScore(key, 88, "Reporting posture appears aligned with institutional expectations.", {
      positiveEvidence: [`Operator reporting level: ${opReporting}`],
    });
  }
  if (institutional && !opInstitutional) {
    return knownScore(key, 28, "Institutional/lender-grade reporting not clearly documented.", {
      negativeEvidence: ["Reporting level may not meet institutional requirement"],
      unknownNotes: ["Validate lender-grade reporting package"],
    });
  }
  return knownScore(key, 55, "Owner reporting fields present; depth requires validation.", {
    positiveEvidence: [`Operator reporting level: ${opReporting}`],
    unknownNotes: ["Confirm cadence, package contents, and audit rights"],
  });
}

export function scoreRegionalResourcesFactor(project, operator) {
  const key = "regionalResources";
  const resources = listValue(operator.regionalResources);
  const country = scalarValue(project.geography?.country);
  if (!resources.length) {
    // Presence of country ops is portfolio signal, not automatic regional bench proof
    const countries = listValue(operator.geography?.countries);
    if (country && countries.some((c) => norm(c) === norm(country))) {
      return unknownFactor(
        key,
        "Country presence documented, but regional team/resources are not confirmed."
      );
    }
    return unknownFactor(key, "Regional resources are unknown.");
  }
  return knownScore(key, 70, "Regional resource signals are documented.", {
    positiveEvidence: resources.slice(0, 3),
  });
}

export function scoreCommercialDifferentiatorFactor(project, operator) {
  const key = "commercialDifferentiator";
  const diffs = listValue(operator.specialistExperience?.differentiators);
  // Explicitly ignore table-stakes even if misclassified
  const clean = diffs.filter((d) => !isTableStakesToken(d));
  const tableStakes = listValue(operator.specialistExperience?.tableStakesClaimed);

  if (!clean.length) {
    if (tableStakes.length) {
      return knownScore(
        key,
        0,
        "Only table-stakes capability claims found — no positive differentiation awarded.",
        {
          negativeEvidence: [],
          unknownNotes: [
            "Table-stakes services recorded but not scored as differentiators",
          ],
        }
      );
    }
    return unknownFactor(key, "No project-specific commercial differentiators documented.");
  }

  // Positive only when differentiator relates to project needs
  const projectHay = [
    scalarValue(project.developmentType),
    scalarValue(project.assetType),
    scalarValue(project.fbComplexity),
    project.mixedUse?.value ? "mixed-use" : "",
    project.brandedResidences?.value ? "residences" : "",
    ...listValue(project.strategicPriorities),
  ]
    .filter(Boolean)
    .join(" ");

  const relevant = clean.filter((d) => {
    const dn = norm(d);
    return !projectHay || projectHay.split(/\s+/).some((t) => t.length > 3 && dn.includes(t));
  });

  if (!relevant.length) {
    return knownScore(key, 25, "Differentiators listed but weak project relevance.", {
      positiveEvidence: [],
      unknownNotes: ["Validate whether stated differentiators apply to this project"],
    });
  }
  return knownScore(key, Math.min(100, 55 + relevant.length * 15), "Project-relevant differentiators found.", {
    positiveEvidence: relevant.slice(0, 3),
  });
}

export function scoreAllOperatorProjectFactors(project, operator) {
  const factors = [
    scoreGeographyFactor(project, operator),
    scoreSegmentFactor(project, operator),
    scoreAssetDevelopmentFactor(project, operator),
    scoreProjectComplexityFactor(project, operator),
    scoreBrandExperienceFactor(project, operator),
    scoreOwnershipGovernanceFactor(project, operator),
    scoreRegionalResourcesFactor(project, operator),
    scoreCommercialDifferentiatorFactor(project, operator),
  ];
  return factors;
}

/**
 * Weighted average with unknowns contributing 0 but remaining in denominator.
 * Not-applicable factors are removed from both numerator and denominator.
 */
export function aggregateOperatorProjectAlignment(factors) {
  let weighted = 0;
  let denom = 0;
  let knownWeight = 0;
  let unknownWeight = 0;
  for (const f of factors) {
    if (!f.applicable || f.state === "not_applicable") continue;
    denom += f.weight;
    if (f.state === "unknown") {
      unknownWeight += f.weight;
      // score contributes 0
    } else {
      knownWeight += f.weight;
      weighted += f.score * f.weight;
    }
  }
  const raw = denom > 0 ? Math.round((weighted / denom) * 10) / 10 : 0;
  return {
    rawScore: raw,
    applicableWeight: denom,
    knownWeight,
    unknownWeight,
    factors,
  };
}
