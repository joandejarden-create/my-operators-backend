/**
 * Operator Fit v2.1 — Comparable Relevance Index (CRI).
 * Subcomponent of Asset/Development (weight 20). Not a 9th top-level factor.
 * Avoids large duplicated weight for geography / segment / brand.
 */

import { isKnownPositive, listValue, scalarValue } from "../adapters/field-state.js";
import {
  V21_CRI_MAX_COMPARABLES,
  V21_CRI_DIMINISHING,
  V21_CRI_FORMULATIONS,
  V21_DEFAULT_CRI_FORMULATION,
} from "./config.js";

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function hayOf(c) {
  return norm(
    [
      c.propertyName || c.name,
      c.hotelType,
      c.situation,
      c.developmentType,
      c.assetType,
      c.region,
      c.country,
      c.city,
      c.services,
      c.brand,
    ]
      .filter(Boolean)
      .join(" ")
  );
}

/** Evidence gate: verified / independent / referenced — not unsupported marketing. */
export function isComparableEvidenceSufficient(c) {
  if (!c) return false;
  if (c.verified === true || c.independent === true || c.referenced === true) return true;
  if (c.source && String(c.source).trim()) return true;
  // Calibration case studies with property name + country treated as detailed operator-provided
  if ((c.propertyName || c.name) && (c.country || c.region || c.situation)) return true;
  return false;
}

/**
 * Score one comparable 0–100 (CRI dimensions; geo/segment/brand minimal).
 */
export function scoreSingleComparableRelevance(comp, project) {
  const hay = hayOf(comp);
  const dev = norm(scalarValue(project.developmentType));
  const asset = norm(scalarValue(project.assetType));
  const dims = {};

  // Development type (25)
  if (dev && hay.includes(dev.slice(0, 6))) dims.development = 1;
  else if (
    (/new build/i.test(dev) && /new build|development|managed hotels|operating/i.test(hay)) ||
    (/conversion|reflag/i.test(dev) && /conversion|reflag/i.test(hay)) ||
    (/renovation|reposition|turnaround/i.test(dev) &&
      /renovation|reposition|turnaround/i.test(hay))
  ) {
    dims.development = 0.75;
  } else if (/operating|portfolio|managed/i.test(hay)) dims.development = 0.4;
  else dims.development = 0.15;

  // Asset type (20)
  if (asset && hay.includes(asset.slice(0, 6))) dims.asset = 1;
  else if (/mixed/i.test(asset) && /mixed/i.test(hay)) dims.asset = 1;
  else if (/resort/i.test(asset) && /resort|ocean|leisure|beach/i.test(hay)) dims.asset = 0.9;
  else if (/urban|high.?rise|city|mid.?rise/i.test(asset + " " + hay) && /urban|city|high.?rise|hotel/i.test(hay))
    dims.asset = 0.55;
  else if (/resort|ocean|leisure|beach/i.test(hay) && /mixed|urban|high.?rise/i.test(asset))
    dims.asset = 0.25;
  else dims.asset = 0.35;

  // Complexity: mixed-use / residences / F&B / meetings (20) — project-flagged only
  const needs = [];
  if (project.mixedUse?.value) needs.push([/mixed/i, "mixed"]);
  if (project.brandedResidences?.value) needs.push([/residence/i, "residences"]);
  const fbVal = scalarValue(project.fbComplexity);
  if (fbVal && /complex|full-service|multi/i.test(fbVal)) {
    needs.push([/f&b|food|outlet|restaurant/i, "fb"]);
  }
  if (project.meetingGroupComplexity?.value) {
    needs.push([/meeting|group|convention|banquet/i, "meetings"]);
  }
  if (!needs.length) dims.complexity = 0.5; // neutral when project has no elevated flags
  else {
    const hits = needs.filter(([re]) => re.test(hay)).length;
    dims.complexity = hits / needs.length;
  }

  // Recency (15)
  const cur = String(comp.currentOrHistorical || comp.currentness || "");
  if (/historical|past|former|closed/i.test(cur)) dims.recency = 0.35;
  else if (/current|operating|active/i.test(cur) || /operating/i.test(hay)) dims.recency = 0.9;
  else dims.recency = 0.65;

  // Evidence quality (20)
  if (comp.verified === true) dims.evidence = 1;
  else if (comp.independent === true || comp.referenced === true) dims.evidence = 0.85;
  else if (comp.source) dims.evidence = 0.7;
  else dims.evidence = 0.55;

  // Tiny explanatory geo/segment (max ~5% combined) — founder: avoid double-count
  const country = norm(scalarValue(project.geography?.country));
  dims.geoLight = country && hay.includes(country) ? 1 : /mexico|peru|dominican|dr\b/i.test(hay) ? 0.3 : 0.1;
  const scale = norm(scalarValue(project.hotelSegment));
  dims.segmentLight =
    scale && hay.includes(scale) ? 1 : /upscale|luxury|midscale|select/i.test(hay) ? 0.4 : 0.2;

  const weights = {
    development: 25,
    asset: 20,
    complexity: 20,
    recency: 15,
    evidence: 15,
    geoLight: 3,
    segmentLight: 2,
  };
  let num = 0;
  let den = 0;
  for (const [k, w] of Object.entries(weights)) {
    den += w;
    num += (dims[k] ?? 0) * w;
  }
  const score = den ? (num / den) * 100 : 0;
  return { score, dims, property: comp.propertyName || comp.name || "comparable", hay };
}

/**
 * @returns {{
 *   cri: number,
 *   criScore100: number,
 *   label: string,
 *   topComparables: object[],
 *   discardedCount: number,
 *   formulation: string,
 * }}
 */
export function calculateComparableRelevanceIndex(project, operator, formulationId = V21_DEFAULT_CRI_FORMULATION) {
  const comps = isKnownPositive(operator.comparables) ? operator.comparables.value || [] : [];
  const sufficient = comps.filter(isComparableEvidenceSufficient);
  const scored = sufficient
    .map((c) => ({ ...scoreSingleComparableRelevance(c, project), raw: c }))
    .sort((a, b) => b.score - a.score);

  const top = scored.slice(0, V21_CRI_MAX_COMPARABLES);
  let wSum = 0;
  let sSum = 0;
  top.forEach((t, i) => {
    const w = V21_CRI_DIMINISHING[i] || 0;
    wSum += w;
    sSum += t.score * w;
  });
  const criScore100 = wSum > 0 ? sSum / wSum : 0;
  const cri = criScore100 / 100;

  let label = "Limited Relevance";
  if (criScore100 >= 70) label = "High Relevance";
  else if (criScore100 >= 45) label = "Moderate Relevance";

  return {
    cri,
    criScore100: Math.round(criScore100 * 10) / 10,
    label,
    topComparables: top.map((t) => ({
      property: t.property,
      score: Math.round(t.score * 10) / 10,
      dims: t.dims,
    })),
    discardedCount: Math.max(0, scored.length - top.length),
    insufficientCount: comps.length - sufficient.length,
    formulation: formulationId,
    formulationMeta: V21_CRI_FORMULATIONS[formulationId] || V21_CRI_FORMULATIONS.moderate,
  };
}

/**
 * Development-mode depth 0–100 for project development type.
 */
export function scoreDevelopmentModeDepth(project, operator) {
  const dev = scalarValue(project.developmentType);
  if (!dev) {
    return { score: null, state: "not_applicable", label: "n/a", count: 0 };
  }
  const sits = listValue(operator.developmentExperience);
  const comps = isKnownPositive(operator.comparables) ? operator.comparables.value || [] : [];

  const matchMode = (text) => {
    const h = norm(text);
    if (/new build/i.test(dev)) return /new build|new-build|development/i.test(h);
    if (/conversion|reflag/i.test(dev)) return /conversion|reflag/i.test(h);
    if (/renovation|reposition|turnaround/i.test(dev)) {
      return /renovation|reposition|turnaround/i.test(h);
    }
    return h.includes(norm(dev).slice(0, 6));
  };

  const sitHits = sits.filter(matchMode).length;
  const compHits = comps.filter((c) => matchMode(hayOf(c)) && isComparableEvidenceSufficient(c)).length;
  const count = sitHits + compHits;

  let score;
  let label;
  if (count >= 3) {
    score = 90;
    label = "Repeated demonstrated experience";
  } else if (count === 2) {
    score = 75;
    label = "Some demonstrated experience";
  } else if (count === 1) {
    score = 60;
    label = "One documented example";
  } else if (sits.length || comps.length) {
    score = 28;
    label = "Operator-reported / weak mode overlap";
  } else {
    score = 0;
    label = "Unknown";
  }

  return { score, state: score === 0 && !sits.length && !comps.length ? "unknown" : "known", label, count };
}

export function blendAssetDevelopmentScore(modeDepth, criResult, formulationId = V21_DEFAULT_CRI_FORMULATION) {
  const f = V21_CRI_FORMULATIONS[formulationId] || V21_CRI_FORMULATIONS.moderate;
  const mode = modeDepth?.score;
  const cri100 = criResult?.criScore100 ?? 0;
  if (mode == null && !criResult?.topComparables?.length) {
    return { score: null, blend: null };
  }
  const modeScore = mode == null ? Math.max(20, cri100 * 0.5) : mode;
  const criW = f.criWeightInsideAsset;
  const score = Math.round(((1 - criW) * modeScore + criW * cri100) * 10) / 10;
  return {
    score: Math.max(0, Math.min(100, score)),
    blend: { modeScore, cri100, criWeight: criW, formulation: f.id },
  };
}
