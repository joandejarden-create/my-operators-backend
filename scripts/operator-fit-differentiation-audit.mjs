#!/usr/bin/env node
/**
 * AUDIT-ONLY — Operator Fit differentiation audit harness.
 * Does NOT change production scoring, routes, Airtable, or UI.
 *
 *   node scripts/operator-fit-differentiation-audit.mjs
 *   node scripts/operator-fit-comparable-relevance-simulation.mjs  (re-exports / thin wrapper)
 */
import "dotenv/config";
import { writeFileSync, readFileSync, existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { evaluateCandidate } from "../lib/operator-fit/evaluate-candidate.js";
import { classifyOperatorReadiness, READINESS_STATUS } from "../lib/operator-fit/readiness.js";
import { FIT_V2_SCENARIOS, FIT_V2_OPERATORS } from "../lib/operator-fit/fixtures/scenarios.js";
import { explainRankingDifference } from "../lib/operator-fit/ranking-difference.js";
import { buildOwnerCandidatePresentation } from "../lib/operator-fit/owner-presentation.js";
import {
  OPERATOR_PROJECT_FACTORS,
  PRIMARY_LAYER_WEIGHTS,
  EXECUTION_RISK,
  EVIDENCE_CONFIDENCE,
} from "../lib/operator-fit/config.js";
import {
  loadOperatorIntelligenceUniverse,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
} from "../lib/operator-intelligence/calibration-overlay.js";
import { OPERATOR_FIT_ENGINE_VERSION } from "../lib/operator-fit/feature-flag.js";
import { isKnownPositive, listValue, scalarValue } from "../lib/operator-fit/adapters/field-state.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const SF_ID = "reckyv9O0Y3auYpJJ";
const HG_ID = "recLjxtxIIVJaGbXK";

function mean(xs) {
  if (!xs.length) return null;
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}
function median(xs) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
function stdev(xs) {
  if (xs.length < 2) return 0;
  const m = mean(xs);
  return Math.sqrt(xs.reduce((a, x) => a + (x - m) ** 2, 0) / (xs.length - 1));
}
function bandOf(d) {
  if (d >= 70) return "Strong";
  if (d >= 55) return "Good";
  if (d >= 40) return "Potential";
  return "Limited";
}

function enrich(c, prefill) {
  const merged = { ...(prefill || {}), submission_status: "Active", companyName: c.companyName };
  const pf = c.platform?.fields || {};
  const cf = c.commercial?.fields || {};
  if (pf["Active Countries"]) merged.activeCountries = pf["Active Countries"];
  if (cf["Management Structures Supported"]) {
    merged.managementStructuresSupported = cf["Management Structures Supported"];
  }
  return merged;
}

async function buildProductionPrefills() {
  const universe = loadOperatorIntelligenceUniverse();
  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const univIds = new Set(
    (universe.operators || [])
      .map((o) => o.operatorId)
      .filter((id) => id && !String(id).startsWith("research_"))
  );
  let list = candidates.filter((c) => univIds.has(c.operatorId));
  if (list.length < 3) list = candidates.slice(0, 40);
  return list.map((c) => {
    const base = enrich(
      c,
      buildPrefillObjectFromNewBaseRows(c.master, c.profile, c.platform, c.commercial, c.governance)
    );
    const overlay = buildPrefillOverlayFromCohort(c.operatorId, universe);
    const merged = overlay ? mergePrefillWithCalibration(base, overlay) : { prefill: base };
    return { operatorId: c.operatorId, companyName: c.companyName, prefill: merged.prefill };
  });
}

function dealCContext(prior) {
  const deal = (prior.deals || []).find((d) => /Deal C/i.test(d.label));
  return {
    dealId: "recPILOT_pilot_deal_c",
    dealFields: { "Project Type": deal.projectType },
    locationData: {
      Country: deal.country,
      "Hotel Chain Scale": deal.chainScale,
      "Building Type": deal.buildingType,
    },
    mpData: {},
    siData: {
      "Operating Model": deal.operatingModel,
      "Preferred Management Structure": deal.preferredStructures || [],
      "Market Presence Requirement": "Active country operations required",
    },
    meta: {
      label: deal.label,
      country: deal.country,
      chainScale: deal.chainScale,
      buildingType: deal.buildingType,
      projectType: deal.projectType,
      preferredBrandCount: deal.preferredBrandCount,
      preferredStructures: deal.preferredStructures,
    },
  };
}

function pickEval(row) {
  if (!row) return null;
  const factors = (row.factorBreakdown || []).map((f) => ({
    key: f.key,
    label: f.label,
    weight: f.weight,
    score: f.score,
    state: f.state,
    applicable: f.applicable,
    rationale: f.rationale,
    positiveEvidence: f.positiveEvidence || [],
    negativeEvidence: f.negativeEvidence || [],
    unknownNotes: f.unknownNotes || [],
  }));
  return {
    candidateId: row.candidateId,
    operatorName: row.operatorName,
    eligibilityStatus: row.eligibilityStatus,
    eligibilityReasons: row.eligibilityReasons,
    eligibilityConditions: row.eligibilityConditions,
    eligibilityHardConflicts: row.eligibilityHardConflicts,
    eligibilityUnknowns: row.eligibilityUnknowns,
    factors,
    opProjectRaw: row.layers?.operatorProjectAlignment?.rawScore,
    knownWeight: row.layers?.operatorProjectAlignment?.knownWeight,
    applicableWeight: row.layers?.operatorProjectAlignment?.applicableWeight,
    unknownWeight: row.layers?.operatorProjectAlignment?.unknownWeight,
    structure: row.layers?.operatingStructureAlignment,
    brand: row.layers?.brandOperatorCompatibility,
    risk: row.layers?.executionRisk,
    evidence: row.layers?.evidenceConfidence,
    coverage: row.layers?.dataCoverage,
    rawBeforeRisk: row.rawBeforeRisk,
    rawOperatorAlignment: row.rawOperatorAlignment,
    displayedOperatorAlignment: row.displayedOperatorAlignment,
    evidenceConfidence: row.evidenceConfidence,
    dataCoveragePct: row.dataCoveragePct,
    confidenceCeilingApplied: row.confidenceCeilingApplied,
    executionRiskPenalty: row.executionRiskPenalty,
    whyItMatches: row.whyItMatches,
    potentialConcerns: row.potentialConcerns,
    unknowns: row.unknowns,
    validationQuestions: row.validationQuestions,
    rank: row.rank,
  };
}

function extractComparables(prefill, opEval) {
  const adapted = adaptOperatorFromPrefill(prefill?.prefill || {}, {
    operatorId: prefill?.operatorId,
    companyName: prefill?.companyName,
  });
  const comps = isKnownPositive(adapted.comparables) ? adapted.comparables.value || [] : [];
  const factor = (opEval.factors || []).find((f) => f.key === "assetDevelopmentExperience");
  return {
    count: comps.length,
    records: comps.map((c) => ({
      property: c.propertyName || c.name || c.hotel || "—",
      country: c.country || c.region || null,
      city: c.city || null,
      brand: c.brand || null,
      segment: c.segment || c.chainScale || c.hotelType || null,
      keyCount: c.keys || c.keyCount || c.rooms || null,
      developmentType: c.situation || c.developmentType || null,
      urbanResort: c.urbanResort || c.assetType || c.hotelType || null,
      mixedUse: c.mixedUse ?? null,
      residences: c.residences ?? c.brandedResidences ?? null,
      operatingStructure: c.operatingStructure || c.structure || null,
      comparabilityStrength: c.comparabilityStrength || c.strength || null,
      evidenceClass: c.verified
        ? "verified_project_level"
        : c.referenced
          ? "independently_referenced"
          : "detailed_operator_provided",
      currentness: c.currentOrHistorical || c.currentness || null,
      whyComparable: c.why || c.notes || null,
      consumedByScoring: true,
      scoringNote:
        "Consumed via assetDevelopmentExperience heuristic (relevant comps → min(100, 70+10*n)); no Comparability Strength enum in engine.",
      narrativeOnly: false,
      raw: c,
    })),
    assetFactorScore: factor?.score ?? null,
    assetPositive: factor?.positiveEvidence || [],
    assetNegative: factor?.negativeEvidence || [],
  };
}

/**
 * Audit-only Comparable Relevance Index (not production).
 * Documented weights per variant.
 */
function scoreComparableRelevance(comp, project, weights) {
  const hay = [
    comp.property,
    comp.country,
    comp.city,
    comp.brand,
    comp.segment,
    comp.developmentType,
    comp.urbanResort,
    String(comp.mixedUse),
    String(comp.residences),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  const dims = {};
  const country = String(project.country || "").toLowerCase();
  dims.geography = country && hay.includes(country) ? 1 : /latin|cala|caribbean|mexico|dr|dominican/i.test(hay) ? 0.4 : 0.1;
  const scale = String(project.scale || "").toLowerCase();
  dims.segment =
    scale && hay.includes(scale.replace(/\s+/g, " "))
      ? 1
      : /upper upscale|upscale|luxury|full.?service/i.test(hay)
        ? 0.55
        : 0.2;
  const dev = String(project.developmentType || "").toLowerCase();
  dims.development =
    /new build/i.test(dev) && /new build|development|managed hotels/i.test(hay)
      ? 1
      : /conversion|reflag|renovation|turnaround/i.test(hay)
        ? 0.35
        : 0.25;
  dims.asset =
    /mixed/i.test(String(project.buildingType || "")) && /mixed/i.test(hay)
      ? 1
      : /urban|high.?rise|city|portfolio/i.test(hay)
        ? 0.6
        : /resort|leisure|ocean|beach/i.test(hay)
          ? 0.25
          : 0.4;
  dims.urbanResort = /resort|ocean|leisure|beach/i.test(hay)
    ? project.buildingType && /resort/i.test(project.buildingType)
      ? 1
      : 0.2
    : 0.7;
  dims.brand = comp.brand ? 0.5 : 0.3;
  dims.keyCount = comp.keyCount ? 0.5 : 0.3;
  dims.complexity =
    /mixed|residence|f&b|meeting/i.test(hay) && /mixed/i.test(String(project.buildingType || ""))
      ? 0.9
      : 0.35;
  dims.recency = /historical|past|former/i.test(String(comp.currentness || "")) ? 0.4 : 0.75;
  const strengthMap = { high: 1, moderate: 0.65, limited: 0.35 };
  const cs = String(comp.comparabilityStrength || "").toLowerCase();
  dims.comparabilityStrength = strengthMap[cs] ?? 0.55;
  dims.evidence =
    comp.evidenceClass === "verified_project_level"
      ? 1
      : comp.evidenceClass === "independently_referenced"
        ? 0.85
        : 0.55;

  let totalW = 0;
  let score = 0;
  for (const [k, w] of Object.entries(weights)) {
    if (dims[k] == null) continue;
    totalW += w;
    score += dims[k] * w;
  }
  return { score: totalW ? score / totalW : 0, dims };
}

function simulateComparableVariants(sfComps, hgComps, project) {
  const variants = {
    A_conservative: {
      label: "Conservative",
      note: "Comparables create limited differentiation; capped influence.",
      weights: {
        geography: 18,
        segment: 12,
        development: 14,
        asset: 10,
        urbanResort: 8,
        brand: 6,
        keyCount: 4,
        complexity: 8,
        recency: 6,
        comparabilityStrength: 8,
        evidence: 6,
      },
      blendIntoAsset: 0.25, // replace 25% of asset factor with CRI*100
      multiCompBonus: 2,
    },
    B_moderate: {
      label: "Moderate",
      note: "Comparables important subcomponent of Asset/Development + complexity signal.",
      weights: {
        geography: 22,
        segment: 14,
        development: 16,
        asset: 12,
        urbanResort: 10,
        brand: 6,
        keyCount: 4,
        complexity: 10,
        recency: 6,
        comparabilityStrength: 10,
        evidence: 8,
      },
      blendIntoAsset: 0.55,
      multiCompBonus: 5,
    },
    C_strong: {
      label: "Strong",
      note: "Direct comparables become primary Asset/Development driver.",
      weights: {
        geography: 24,
        segment: 12,
        development: 18,
        asset: 14,
        urbanResort: 12,
        brand: 6,
        keyCount: 4,
        complexity: 12,
        recency: 8,
        comparabilityStrength: 12,
        evidence: 10,
      },
      blendIntoAsset: 0.85,
      multiCompBonus: 8,
    },
  };

  function opCri(comps, v) {
    if (!comps.records.length) {
      return { best: 0, avg: 0, n: 0, bestComp: null, details: [] };
    }
    const details = comps.records.map((c) => {
      const r = scoreComparableRelevance(c, project, v.weights);
      return { property: c.property, country: c.country, ...r };
    });
    details.sort((a, b) => b.score - a.score);
    const best = details[0].score;
    const avg = mean(details.map((d) => d.score));
    const n = details.length;
    const multi = Math.min(1, (n - 1) * 0.15);
    const combined = Math.min(1, best * 0.7 + avg * 0.2 + multi * 0.1);
    return { best, avg, n, combined, bestComp: details[0], details };
  }

  const out = {};
  for (const [id, v] of Object.entries(variants)) {
    const sf = opCri(sfComps, v);
    const hg = opCri(hgComps, v);
    // Simulated asset score = blend(currentAsset, CRI*100) + multi bonus
    const simAsset = (cri, currentAsset) => {
      const cri100 = (cri.combined || 0) * 100;
      const blended = (1 - v.blendIntoAsset) * currentAsset + v.blendIntoAsset * cri100;
      return Math.min(100, blended + Math.max(0, (cri.n || 0) - 1) * v.multiCompBonus);
    };
    const sfAsset = simAsset(sf, sfComps.assetFactorScore ?? 20);
    const hgAsset = simAsset(hg, hgComps.assetFactorScore ?? 20);
    // Recompute op-project raw with replaced asset score only (audit)
    const recompute = (assetScore) => {
      // Same known set as Deal C: geo78, seg100, assetX, complexity0, regional unk, commercial unk
      const weighted = 78 * 22 + 100 * 14 + assetScore * 20 + 0 * 12 + 0 * 6 + 0 * 6;
      const denom = 80;
      return Math.round((weighted / denom) * 10) / 10;
    };
    const sfOp = recompute(sfAsset);
    const hgOp = recompute(hgAsset);
    const layer = (op) => Math.round(((op * 70) / 85) * 10) / 10;
    const afterRisk = (primary) => Math.round((primary - 10) * 10) / 10;
    out[id] = {
      ...v,
      santaFe: {
        cri: sf,
        simulatedAsset: Math.round(sfAsset * 10) / 10,
        opProject: sfOp,
        primary: layer(sfOp),
        displayed: afterRisk(layer(sfOp)),
      },
      highgate: {
        cri: hg,
        simulatedAsset: Math.round(hgAsset * 10) / 10,
        opProject: hgOp,
        primary: layer(hgOp),
        displayed: afterRisk(layer(hgOp)),
      },
      differenceDisplayed:
        Math.round((afterRisk(layer(sfOp)) - afterRisk(layer(hgOp))) * 10) / 10,
      doubleCountRisk:
        "Asset factor already includes comparable boost; blending CRI atop current asset can double-count unless Asset is rebuilt around CRI.",
    };
  }
  return out;
}

function riskSensitivity(sf, hg) {
  const rebuild = (row, mode) => {
    const items = row.risk?.items || [];
    let penalty = 0;
    const kept = [];
    for (const it of items) {
      let pts = 0;
      if (mode === "current") pts = it.points;
      else if (mode === "confirmed_only") pts = it.kind === "confirmed_risk" ? it.points : 0;
      else if (mode === "unknown_zero") {
        pts = it.kind === "unknown_validation" ? 0 : it.points;
      } else if (mode === "potential_half") {
        if (it.kind === "unknown_validation") pts = 0;
        else if (it.kind === "potential_concern") pts = Math.round(it.points / 2);
        else pts = it.points;
      }
      if (pts) kept.push({ ...it, appliedPoints: pts });
      penalty += pts;
    }
    penalty = Math.min(penalty, EXECUTION_RISK.maxTotalPenaltyPoints);
    const displayed = Math.max(0, Math.round((row.rawBeforeRisk - penalty) * 10) / 10);
    return { penalty, displayed, band: bandOf(displayed), kept };
  };
  const modes = ["current", "confirmed_only", "unknown_zero", "potential_half"];
  const out = {};
  for (const m of modes) {
    out[m] = { santaFe: rebuild(sf, m), highgate: rebuild(hg, m) };
  }
  return out;
}

function distributionStats(scores) {
  const xs = scores.filter((x) => Number.isFinite(x));
  const bands = { Strong: 0, Good: 0, Potential: 0, Limited: 0 };
  for (const x of xs) bands[bandOf(x)] += 1;
  // exact ties among top pairs in same list: count duplicate displayed values
  const freq = new Map();
  for (const x of xs) {
    const k = Math.round(x * 10) / 10;
    freq.set(k, (freq.get(k) || 0) + 1);
  }
  let exactTieGroups = 0;
  let exactTieMembers = 0;
  for (const [, n] of freq) {
    if (n >= 2) {
      exactTieGroups += 1;
      exactTieMembers += n;
    }
  }
  let near1 = 0,
    near2 = 0,
    near3 = 0,
    near5 = 0;
  for (let i = 0; i < xs.length; i++) {
    for (let j = i + 1; j < xs.length; j++) {
      const d = Math.abs(xs[i] - xs[j]);
      if (d < 1) near1 += 1;
      if (d < 2) near2 += 1;
      if (d < 3) near3 += 1;
      if (d < 5) near5 += 1;
    }
  }
  return {
    n: xs.length,
    mean: xs.length ? Math.round(mean(xs) * 100) / 100 : null,
    median: xs.length ? Math.round(median(xs) * 100) / 100 : null,
    min: xs.length ? Math.min(...xs) : null,
    max: xs.length ? Math.max(...xs) : null,
    stdev: xs.length ? Math.round(stdev(xs) * 100) / 100 : null,
    bands,
    exactTieGroups,
    exactTieMembers,
    nearTiePairs: { within1: near1, within2: near2, within3: near3, within5: near5 },
  };
}

function candidateSetOptions(dealId, rows) {
  const rr = rows.filter((r) => r.readiness === READINESS_STATUS.RANKING_READY);
  const names = (list) => list.map((r) => ({ name: r.operatorName, displayed: r.displayed, band: bandOf(r.displayed) }));
  return {
    dealId,
    A_rankingReadyOnly: names(rr),
    B_rr_potential40: names(rr.filter((r) => r.displayed >= 40)),
    C_rr_good55: names(rr.filter((r) => r.displayed >= 55)),
    D_tiered: {
      leading: names(rr.filter((r) => r.displayed >= 55)),
      potential: names(rr.filter((r) => r.displayed >= 40 && r.displayed < 55)),
      additionalValidation: names(rr.filter((r) => r.displayed < 40)),
    },
  };
}

async function main() {
  mkdirSync(join(root, "reports"), { recursive: true });
  const priorPath = join(root, "reports/operator-fit-real-deal-shadow-review.json");
  const prior = existsSync(priorPath) ? JSON.parse(readFileSync(priorPath, "utf8")) : { deals: [] };
  const prefills = await buildProductionPrefills();
  const ctx = dealCContext(prior);

  const evaluated = evaluateOperatorFitForDeal({
    dealId: ctx.dealId,
    dealFields: ctx.dealFields,
    locationData: ctx.locationData,
    mpData: ctx.mpData,
    siData: ctx.siData,
    operatorPrefills: prefills,
  });

  const withReady = (evaluated.allEvaluated || []).map((row) => {
    const pref = prefills.find((p) => p.operatorId === row.candidateId);
    const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
      operatorId: row.candidateId,
      companyName: row.operatorName,
    });
    const ready = classifyOperatorReadiness(op, evaluated.project);
    return {
      ...row,
      readiness: ready.status,
      displayed: row.displayedOperatorAlignment,
      operatorName: row.operatorName,
    };
  });

  const top5 = evaluated.top5 || [];
  const sfRow = top5.find((r) => r.candidateId === SF_ID) || withReady.find((r) => r.candidateId === SF_ID);
  const hgRow = top5.find((r) => r.candidateId === HG_ID) || withReady.find((r) => r.candidateId === HG_ID);
  const sf = pickEval(sfRow);
  const hg = pickEval(hgRow);
  sf.readiness = withReady.find((r) => r.candidateId === SF_ID)?.readiness;
  hg.readiness = withReady.find((r) => r.candidateId === HG_ID)?.readiness;

  const sfPref = prefills.find((p) => p.operatorId === SF_ID);
  const hgPref = prefills.find((p) => p.operatorId === HG_ID);
  const sfComps = extractComparables(sfPref, sf);
  const hgComps = extractComparables(hgPref, hg);

  const projectMeta = {
    country: ctx.meta.country,
    scale: ctx.meta.chainScale,
    developmentType: evaluated.projectSummary?.developmentType || ctx.meta.projectType,
    buildingType: ctx.meta.buildingType,
  };

  const comparableSims = simulateComparableVariants(sfComps, hgComps, projectMeta);
  const riskSims = riskSensitivity(sf, hg);
  const rankingDiff = explainRankingDifference(sfRow, hgRow);

  // Side-by-side component table
  const factorKeys = Object.keys(OPERATOR_PROJECT_FACTORS);
  const sideBySide = {
    eligibilityStatus: { santaFe: sf.eligibilityStatus, highgate: hg.eligibilityStatus },
    readiness: { santaFe: sf.readiness, highgate: hg.readiness },
    factors: {},
    opProjectRaw: { santaFe: sf.opProjectRaw, highgate: hg.opProjectRaw, diff: roundDiff(sf.opProjectRaw, hg.opProjectRaw) },
    structureState: { santaFe: sf.structure?.state, highgate: hg.structure?.state },
    structureScore: { santaFe: sf.structure?.score, highgate: hg.structure?.score },
    brandCategory: { santaFe: sf.brand?.category, highgate: hg.brand?.category },
    brandNumeric: { santaFe: sf.brand?.numericForComposition, highgate: hg.brand?.numericForComposition },
    rawBeforeRisk: { santaFe: sf.rawBeforeRisk, highgate: hg.rawBeforeRisk, diff: roundDiff(sf.rawBeforeRisk, hg.rawBeforeRisk) },
    executionRisk: { santaFe: sf.executionRiskPenalty, highgate: hg.executionRiskPenalty, diff: roundDiff(sf.executionRiskPenalty, hg.executionRiskPenalty) },
    riskItems: { santaFe: sf.risk?.items, highgate: hg.risk?.items },
    evidenceStrength: { santaFe: sf.evidenceConfidence, highgate: hg.evidenceConfidence },
    coverage: { santaFe: sf.dataCoveragePct, highgate: hg.dataCoveragePct, diff: roundDiff(sf.dataCoveragePct, hg.dataCoveragePct) },
    ceiling: { santaFe: sf.confidenceCeilingApplied, highgate: hg.confidenceCeilingApplied },
    displayed: { santaFe: sf.displayedOperatorAlignment, highgate: hg.displayedOperatorAlignment, diff: roundDiff(sf.displayedOperatorAlignment, hg.displayedOperatorAlignment) },
    band: { santaFe: bandOf(sf.displayedOperatorAlignment), highgate: bandOf(hg.displayedOperatorAlignment) },
    rank: { santaFe: sf.rank, highgate: hg.rank },
    tieBreak: {
      rule: "candidateId localeCompare",
      santaFeId: SF_ID,
      highgateId: HG_ID,
      localeCompare: SF_ID.localeCompare(HG_ID),
      winner: SF_ID.localeCompare(HG_ID) < 0 ? "Santa Fe" : "Highgate",
    },
    why: { santaFe: sf.whyItMatches, highgate: hg.whyItMatches },
  };
  for (const k of factorKeys) {
    const a = sf.factors.find((f) => f.key === k);
    const b = hg.factors.find((f) => f.key === k);
    sideBySide.factors[k] = {
      santaFe: { score: a?.score, state: a?.state, applicable: a?.applicable, pos: a?.positiveEvidence, neg: a?.negativeEvidence },
      highgate: { score: b?.score, state: b?.state, applicable: b?.applicable, pos: b?.positiveEvidence, neg: b?.negativeEvidence },
      diff: roundDiff(a?.score, b?.score),
    };
  }

  // Compression / distribution across synthetic scenarios + fixture operators
  const synthScores = { opProject: [], preRisk: [], displayed: [], rrDisplayed: [] };
  const scenarioDist = [];
  for (const sc of FIT_V2_SCENARIOS) {
    const ops = (FIT_V2_OPERATORS || []).map((o) =>
      adaptOperatorFromPrefill(o.prefill || o, { operatorId: o.id || o.operatorId, companyName: o.name || o.companyName })
    );
    // Prefer evaluateOperatorFitForDeal with fixture-shaped prefills if available
    const fixturePrefills = (FIT_V2_OPERATORS || []).map((o) => ({
      operatorId: o.id || o.operatorId,
      companyName: o.name || o.companyName || o.id,
      prefill: o.prefill || o,
    }));
    let result;
    try {
      result = evaluateOperatorFitForDeal({
        dealId: `synth_${sc.id}`,
        dealFields: sc.dealFields,
        locationData: sc.locationData,
        mpData: sc.mpData || {},
        siData: sc.siData || {},
        operatorPrefills: fixturePrefills.length ? fixturePrefills : prefills.slice(0, 12),
      });
    } catch {
      result = evaluateOperatorFitForDeal({
        dealId: `synth_${sc.id}`,
        dealFields: sc.dealFields,
        locationData: sc.locationData,
        mpData: sc.mpData || {},
        siData: sc.siData || {},
        operatorPrefills: prefills.slice(0, 15),
      });
    }
    const rows = result.allEvaluated || [];
    for (const r of rows) {
      if (!/eligible/i.test(r.eligibilityStatus || "")) continue;
      synthScores.opProject.push(r.layers?.operatorProjectAlignment?.rawScore);
      synthScores.preRisk.push(r.rawBeforeRisk);
      synthScores.displayed.push(r.displayedOperatorAlignment);
    }
    scenarioDist.push({
      id: sc.id,
      eligible: rows.filter((r) => /eligible/i.test(r.eligibilityStatus || "")).length,
      displayedStats: distributionStats(
        rows.filter((r) => /eligible/i.test(r.eligibilityStatus || "")).map((r) => r.displayedOperatorAlignment)
      ),
    });
  }

  // Pilot deals A–F using production prefills where possible
  const pilotDefs = [];
  for (const deal of prior.deals || []) {
    pilotDefs.push({
      id: `pilot_${String(deal.label || "").toLowerCase().replace(/\s+/g, "_")}`,
      label: deal.label,
      dealFields: { "Project Type": deal.projectType },
      locationData: {
        Country: deal.country,
        "Hotel Chain Scale": deal.chainScale,
        "Building Type": deal.buildingType,
      },
      siData: {
        "Operating Model": deal.operatingModel,
        "Preferred Management Structure": deal.preferredStructures || [],
        "Market Presence Requirement": "Active country operations required",
      },
    });
  }
  const conversion = FIT_V2_SCENARIOS.find((s) => s.id === "select-service-conversion");
  const resort = FIT_V2_SCENARIOS.find((s) => s.id === "luxury-leisure-resort");
  const urban = FIT_V2_SCENARIOS.find((s) => s.id === "upper-upscale-urban-new-build");
  if (conversion) {
    pilotDefs.push({
      id: "pilot_conversion_mexico",
      label: "Deal D",
      dealFields: conversion.dealFields,
      locationData: conversion.locationData,
      siData: conversion.siData,
    });
  }
  if (resort) {
    pilotDefs.push({
      id: "pilot_resort_dr",
      label: "Deal E",
      dealFields: resort.dealFields,
      locationData: resort.locationData,
      siData: resort.siData,
    });
  }
  if (urban) {
    pilotDefs.push({
      id: "pilot_deal_f",
      label: "Deal F",
      dealFields: urban.dealFields,
      locationData: urban.locationData,
      siData: urban.siData,
    });
  }

  const pilotResults = [];
  const narrativeConsistency = [];
  const candidateSets = [];

  for (const def of pilotDefs) {
    const ev = evaluateOperatorFitForDeal({
      dealId: `recAUDIT_${def.id}`,
      dealFields: def.dealFields,
      locationData: def.locationData,
      mpData: {},
      siData: def.siData,
      operatorPrefills: prefills,
    });
    const rows = (ev.top5 || []).map((row) => {
      const pref = prefills.find((p) => p.operatorId === row.candidateId);
      const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
        operatorId: row.candidateId,
        companyName: row.operatorName,
      });
      const ready = classifyOperatorReadiness(op, ev.project);
      return {
        ...row,
        readiness: ready.status,
        displayed: row.displayedOperatorAlignment,
        operatorName: row.operatorName,
      };
    });
    const rr = rows.filter((r) => r.readiness === READINESS_STATUS.RANKING_READY);
    const allElig = (ev.allEvaluated || []).filter((r) => /eligible/i.test(r.eligibilityStatus || ""));
    for (const r of allElig) {
      synthScores.opProject.push(r.layers?.operatorProjectAlignment?.rawScore);
      synthScores.preRisk.push(r.rawBeforeRisk);
      synthScores.displayed.push(r.displayedOperatorAlignment);
    }
    for (const r of rr) synthScores.rrDisplayed.push(r.displayedOperatorAlignment);

    pilotResults.push({
      id: def.id,
      label: def.label,
      top: rows.map((r) => ({
        rank: r.rank,
        name: r.operatorName,
        displayed: r.displayed,
        readiness: r.readiness,
        why: r.whyItMatches,
        conf: r.evidenceConfidence,
      })),
      rrCount: rr.length,
      distEligible: distributionStats(allElig.map((r) => r.displayedOperatorAlignment)),
      distRR: distributionStats(rr.map((r) => r.displayedOperatorAlignment)),
    });

    candidateSets.push(candidateSetOptions(def.id, rows.length ? rows : withReady));

    // Narrative consistency for top2 when present
    if (rr.length >= 2) {
      const a = rr[0];
      const b = rr[1];
      const scoreDiff = Math.abs((a.displayed || 0) - (b.displayed || 0));
      const whyDiff =
        JSON.stringify(a.whyItMatches || []) !== JSON.stringify(b.whyItMatches || []);
      let cls = "A";
      if (scoreDiff < 0.05 && whyDiff) cls = "B";
      else if (scoreDiff >= 2 && !whyDiff) cls = "C";
      else if (scoreDiff >= 2 && whyDiff) cls = "A";
      else if (scoreDiff < 0.05 && !whyDiff) cls = "A";
      narrativeConsistency.push({
        dealId: def.id,
        label: def.label,
        a: a.operatorName,
        b: b.operatorName,
        scoreDiff: Math.round(scoreDiff * 10) / 10,
        whyA: a.whyItMatches,
        whyB: b.whyItMatches,
        classification: cls,
        note:
          cls === "B"
            ? "Narrative implies differentiation; scores tied"
            : cls === "C"
              ? "Score differs; narrative insufficiently explains"
              : "Narrative and score broadly consistent or both tied",
      });
    } else if (def.id.includes("deal_b") || /Deal B/i.test(def.label || "")) {
      narrativeConsistency.push({
        dealId: def.id,
        label: def.label,
        classification: "A",
        note: "Zero / thin universe — narrative should match scarcity",
        rrCount: rr.length,
      });
    }
  }

  const compression = {
    layerWeights: PRIMARY_LAYER_WEIGHTS,
    evidenceCeilings: EVIDENCE_CONFIDENCE,
    riskCaps: EXECUTION_RISK,
    opProject: distributionStats(synthScores.opProject.filter(Number.isFinite)),
    preRisk: distributionStats(synthScores.preRisk.filter(Number.isFinite)),
    displayed: distributionStats(synthScores.displayed.filter(Number.isFinite)),
    rankingReadyDisplayed: distributionStats(synthScores.rrDisplayed.filter(Number.isFinite)),
    scenarioDist,
    dealCExactTie: sideBySide.displayed.diff === 0,
  };

  const payload = {
    generatedAt: new Date().toISOString(),
    auditOnly: true,
    engineVersion: OPERATOR_FIT_ENGINE_VERSION,
    branch: "app-shell-left-nav",
    commit: "3c88c0b4e22a35052e450d00c5e2f1b9e417c040",
    flags: {
      OPERATOR_FIT_ENGINE_V2: "0",
      OPERATOR_FIT_INTERNAL_PILOT: "0",
    },
    dealC: {
      meta: ctx.meta,
      projectSummary: evaluated.projectSummary,
      santaFe: sf,
      highgate: hg,
      sideBySide,
      rankingDiff,
      comparables: { santaFe: sfComps, highgate: hgComps },
      comparableSims,
      riskSims,
      ownerPresentation: {
        santaFe: buildOwnerCandidatePresentation(sfRow, evaluated.project),
        highgate: buildOwnerCandidatePresentation(hgRow, evaluated.project),
      },
    },
    compression,
    pilotResults,
    candidateSets,
    narrativeConsistency,
    factorGranularityNotes: buildFactorGranularityNotes(),
  };

  const outJson = join(root, "reports/operator-fit-differentiation-audit.json");
  writeFileSync(outJson, JSON.stringify(payload, null, 2));
  console.log(
    JSON.stringify(
      {
        wrote: outJson,
        dealCTie: sideBySide.displayed.diff === 0,
        ranks: { sf: sf.rank, hg: hg.rank },
        displayed: sideBySide.displayed,
        tieBreak: sideBySide.tieBreak,
        criDiffB: comparableSims.B_moderate?.differenceDisplayed,
        riskConfirmedOnly: riskSims.confirmed_only,
        exactTieMembersDisplayed: compression.displayed.exactTieMembers,
        near1: compression.displayed.nearTiePairs.within1,
      },
      null,
      2
    )
  );
}

function roundDiff(a, b) {
  if (a == null || b == null) return null;
  return Math.round((Number(a) - Number(b)) * 1000) / 1000;
}

function buildFactorGranularityNotes() {
  return {
    geographyMarket: {
      inputStates: ["city hit", "country hit", "no overlap", "unknown"],
      scoreValues: [100, 78, 12, 0],
      distinctLevels: 4,
      binary: false,
      notes: "Market Presence type affects eligibility, not this factor score. Depth/count of properties not scored.",
    },
    segmentPositioning: {
      inputStates: ["exact", "partial", "mismatch", "unknown"],
      scoreValues: [100, 62, 18, 0],
      distinctLevels: 4,
      notes: "No repeated-experience multiplier; no current vs historical.",
    },
    assetDevelopmentExperience: {
      inputStates: ["relevant comps n", "asset ratio", "dev ratio", "breadth cap", "unknown"],
      scoreValues: ["min(100,70+10n)", "35+50*ratio", "30+55*ratio", "cap 40", 0],
      distinctLevels: "continuous-ish but comps often land on same 80 bucket",
      notes: "Comparability Strength enum unused. One vs many comps: +10 each after base 70.",
    },
    projectComplexity: {
      inputStates: ["N/A", "hit ratio of needs", "unknown"],
      scoreValues: ["removed", "0-100 by %", 0],
      distinctLevels: "need-count dependent",
    },
    brandExperience: {
      inputStates: ["N/A", "overlap ratio", "none", "unknown"],
      scoreValues: ["removed", "45+55*ratio", 15, 0],
      distinctLevels: 4,
    },
    ownershipGovernance: {
      inputStates: ["N/A", "institutional match", "mismatch", "present generic", "unknown"],
      scoreValues: ["removed", 88, 28, 55, 0],
      distinctLevels: 5,
    },
    regionalResources: {
      inputStates: ["documented list", "country without team (unknown)", "unknown"],
      scoreValues: [70, 0, 0],
      distinctLevels: 2,
      notes: "Effectively binary known(70) vs unknown(0).",
    },
    commercialDifferentiator: {
      inputStates: ["relevant diffs", "weak relevance", "table-stakes only", "unknown"],
      scoreValues: ["55+15*n", 25, 0, 0],
      distinctLevels: 4,
    },
  };
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
