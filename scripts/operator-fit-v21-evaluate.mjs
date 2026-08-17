#!/usr/bin/env node
/**
 * Operator Fit v2.1 — before/after evaluation (audit + shadow).
 * Does NOT enable owner pilot or change Airtable.
 *
 *   node scripts/operator-fit-v21-evaluate.mjs
 */
import "dotenv/config";
import { writeFileSync, readFileSync, existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { classifyOperatorReadiness, READINESS_STATUS } from "../lib/operator-fit/readiness.js";
import { FIT_V2_SCENARIOS, FIT_V2_OPERATORS } from "../lib/operator-fit/fixtures/scenarios.js";
import { buildOwnerTierPresentation } from "../lib/operator-fit/v21/owner-tiers.js";
import { V21_CRI_FORMULATIONS } from "../lib/operator-fit/v21/config.js";
import {
  loadOperatorIntelligenceUniverse,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
} from "../lib/operator-intelligence/calibration-overlay.js";
import {
  OPERATOR_FIT_ENGINE_VERSION,
  OPERATOR_FIT_ENGINE_VERSION_V21,
} from "../lib/operator-fit/feature-flag.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const SF = "reckyv9O0Y3auYpJJ";
const HG = "recLjxtxIIVJaGbXK";

function mean(xs) {
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
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
function band(d) {
  if (d >= 70) return "Strong";
  if (d >= 55) return "Good";
  if (d >= 40) return "Potential";
  return "Limited";
}
function dist(scores) {
  const xs = scores.filter(Number.isFinite);
  const bands = { Strong: 0, Good: 0, Potential: 0, Limited: 0 };
  for (const x of xs) bands[band(x)] += 1;
  const freq = new Map();
  for (const x of xs) {
    const k = Math.round(x * 10) / 10;
    freq.set(k, (freq.get(k) || 0) + 1);
  }
  let exactGroups = 0,
    exactMembers = 0;
  for (const n of freq.values()) {
    if (n >= 2) {
      exactGroups += 1;
      exactMembers += n;
    }
  }
  let near1 = 0,
    near3 = 0;
  for (let i = 0; i < xs.length; i++) {
    for (let j = i + 1; j < xs.length; j++) {
      const d = Math.abs(xs[i] - xs[j]);
      if (d < 1) near1 += 1;
      if (d < 3) near3 += 1;
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
    exactTieGroups: exactGroups,
    exactTieMembers: exactMembers,
    nearTiePairs: { within1: near1, within3: near3 },
  };
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

async function buildPrefills() {
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

function pick(row) {
  if (!row) return null;
  const factors = Object.fromEntries(
    (row.factorBreakdown || []).map((f) => [
      f.key,
      { score: f.score, state: f.state, applicable: f.applicable, v21: f.v21 || null },
    ])
  );
  return {
    name: row.operatorName,
    id: row.candidateId,
    elig: row.eligibilityStatus,
    readiness: row.readiness,
    factors,
    opProject: row.layers?.operatorProjectAlignment?.rawScore,
    structure: row.layers?.operatingStructureAlignment,
    brand: row.layers?.brandOperatorCompatibility,
    risk: row.executionRiskPenalty,
    riskItems: row.layers?.executionRisk?.items,
    rawBefore: row.rawBeforeRisk,
    raw: row.rawOperatorAlignment,
    displayed: row.displayedOperatorAlignment,
    coverage: row.dataCoveragePct,
    conf: row.evidenceConfidence,
    ceiling: row.confidenceCeilingApplied,
    band: band(row.displayedOperatorAlignment),
    why: row.whyItMatches,
    cri: row.layers?.v21?.cri || factors.assetDevelopmentExperience?.v21?.cri || null,
    methodology: row.methodology || row.featureVersion,
  };
}

function annotateReady(evaluated, prefills) {
  return (evaluated.top5 || []).map((row) => {
    const pref = prefills.find((p) => p.operatorId === row.candidateId);
    const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
      operatorId: row.candidateId,
      companyName: row.operatorName,
    });
    const ready = classifyOperatorReadiness(op, evaluated.project);
    return { ...row, readiness: ready.status };
  });
}

async function main() {
  mkdirSync(join(root, "reports"), { recursive: true });
  const prior = existsSync(join(root, "reports/operator-fit-real-deal-shadow-review.json"))
    ? JSON.parse(readFileSync(join(root, "reports/operator-fit-real-deal-shadow-review.json"), "utf8"))
    : { deals: [] };
  const prefills = await buildPrefills();

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
  for (const [id, label, scId] of [
    ["pilot_conversion_mexico", "Deal D", "select-service-conversion"],
    ["pilot_resort_dr", "Deal E", "luxury-leisure-resort"],
    ["pilot_deal_f", "Deal F", "upper-upscale-urban-new-build"],
  ]) {
    const sc = FIT_V2_SCENARIOS.find((s) => s.id === scId);
    if (sc) {
      pilotDefs.push({
        id,
        label,
        dealFields: sc.dealFields,
        locationData: sc.locationData,
        siData: sc.siData,
      });
    }
  }

  const pilotCompare = [];
  const allV2 = [];
  const allV21 = [];
  const rrV2 = [];
  const rrV21 = [];
  let dealCDetail = null;
  const candidateSets = [];

  for (const def of pilotDefs) {
    const v2 = evaluateOperatorFitForDeal({
      dealId: `recV2_${def.id}`,
      dealFields: def.dealFields,
      locationData: def.locationData,
      mpData: {},
      siData: def.siData,
      operatorPrefills: prefills,
      methodology: "v2",
    });
    const v21 = evaluateOperatorFitForDeal({
      dealId: `recV21_${def.id}`,
      dealFields: def.dealFields,
      locationData: def.locationData,
      mpData: {},
      siData: def.siData,
      operatorPrefills: prefills,
      methodology: "v21",
      criFormulation: "moderate",
    });

    const rows2 = annotateReady(v2, prefills);
    const rows21 = annotateReady(v21, prefills);
    const rr2 = rows2.filter((r) => r.readiness === READINESS_STATUS.RANKING_READY);
    const rr21 = rows21.filter((r) => r.readiness === READINESS_STATUS.RANKING_READY);

    for (const r of v2.allEvaluated || []) {
      if (/eligible/i.test(r.eligibilityStatus || "")) allV2.push(r.displayedOperatorAlignment);
    }
    for (const r of v21.allEvaluated || []) {
      if (/eligible/i.test(r.eligibilityStatus || "")) allV21.push(r.displayedOperatorAlignment);
    }
    for (const r of rr2) rrV2.push(r.displayedOperatorAlignment);
    for (const r of rr21) rrV21.push(r.displayedOperatorAlignment);

    const tiers = buildOwnerTierPresentation(
      rr21.map((r) => ({ ...r, readiness: r.readiness })),
      {}
    );
    candidateSets.push({
      dealId: def.id,
      label: def.label,
      counts: tiers.counts,
      tiers: Object.fromEntries(
        Object.entries(tiers.tiers).map(([k, list]) => [
          k,
          list.map((c) => ({
            name: c.operatorName,
            displayed: c.displayed,
            band: band(c.displayed),
            ownerOrdinal: c.ownerOrdinal,
            tieNote: c.tieNote || null,
            elig: c.eligibilityStatus,
          })),
        ])
      ),
    });

    const changes = rr21.map((r21) => {
      const r2 = rr2.find((x) => x.candidateId === r21.candidateId) ||
        rows2.find((x) => x.candidateId === r21.candidateId);
      const d2 = r2?.displayedOperatorAlignment;
      const d21 = r21.displayedOperatorAlignment;
      const risk2 = r2?.executionRiskPenalty;
      const risk21 = r21.executionRiskPenalty;
      const reasons = [];
      if (risk2 !== risk21) reasons.push(`risk ${risk2}→${risk21}`);
      const g2 = r2?.factorBreakdown?.find((f) => f.key === "geographyMarket")?.score;
      const g21 = r21.factorBreakdown?.find((f) => f.key === "geographyMarket")?.score;
      if (g2 !== g21) reasons.push(`geography ${g2}→${g21}`);
      const a2 = r2?.factorBreakdown?.find((f) => f.key === "assetDevelopmentExperience")?.score;
      const a21 = r21.factorBreakdown?.find((f) => f.key === "assetDevelopmentExperience")?.score;
      if (a2 !== a21) reasons.push(`asset/CRI ${a2}→${a21}`);
      if (!reasons.length && d2 === d21) reasons.push("unchanged");
      return {
        name: r21.operatorName,
        v2: d2,
        v21: d21,
        delta: d2 != null ? Math.round((d21 - d2) * 10) / 10 : null,
        reasons,
        readiness: r21.readiness,
      };
    });

    pilotCompare.push({
      id: def.id,
      label: def.label,
      v2Top: rr2.map((r) => ({ name: r.operatorName, displayed: r.displayedOperatorAlignment, risk: r.executionRiskPenalty })),
      v21Top: rr21.map((r) => ({ name: r.operatorName, displayed: r.displayedOperatorAlignment, risk: r.executionRiskPenalty })),
      changes,
    });

    if (def.id === "pilot_deal_c") {
      const sf2 = pick(rows2.find((r) => r.candidateId === SF) || v2.allEvaluated.find((r) => r.candidateId === SF));
      const hg2 = pick(rows2.find((r) => r.candidateId === HG) || v2.allEvaluated.find((r) => r.candidateId === HG));
      const sf21 = pick(rows21.find((r) => r.candidateId === SF) || v21.allEvaluated.find((r) => r.candidateId === SF));
      const hg21 = pick(rows21.find((r) => r.candidateId === HG) || v21.allEvaluated.find((r) => r.candidateId === HG));
      // attach readiness
      if (sf2) sf2.readiness = rows2.find((r) => r.candidateId === SF)?.readiness;
      if (hg2) hg2.readiness = rows2.find((r) => r.candidateId === HG)?.readiness;
      if (sf21) sf21.readiness = rows21.find((r) => r.candidateId === SF)?.readiness;
      if (hg21) hg21.readiness = rows21.find((r) => r.candidateId === HG)?.readiness;
      const tiersC = tiers;
      let dealCStatus = "Additional Validation Set Only";
      if (tiersC.counts.leading >= 2) dealCStatus = "Credible Leading Candidate Set";
      else if (tiersC.counts.leading === 1) dealCStatus = "Single Leading Candidate";
      else if (tiersC.counts.potential >= 1) dealCStatus = "Potential-Fit Set Only";
      else if (tiersC.counts.additional === 0 && tiersC.counts.leading === 0) dealCStatus = "Insufficient Universe";
      dealCDetail = {
        project: v21.projectSummary,
        santaFeV2: sf2,
        highgateV2: hg2,
        santaFeV21: sf21,
        highgateV21: hg21,
        tiedV2: sf2 && hg2 && sf2.displayed === hg2.displayed,
        tiedV21:
          sf21 &&
          hg21 &&
          Math.abs((sf21.displayed || 0) - (hg21.displayed || 0)) < 1,
        deltaV21:
          sf21 && hg21
            ? Math.round(((sf21.displayed || 0) - (hg21.displayed || 0)) * 10) / 10
            : null,
        tiers: tiersC,
        dealCStatus,
      };
    }
  }

  // CRI formulation comparison on Deal C
  const dealCDef = pilotDefs.find((d) => d.id === "pilot_deal_c");
  const criCompare = {};
  if (dealCDef) {
    for (const fid of Object.keys(V21_CRI_FORMULATIONS)) {
      const ev = evaluateOperatorFitForDeal({
        dealId: `recCRI_${fid}`,
        dealFields: dealCDef.dealFields,
        locationData: dealCDef.locationData,
        mpData: {},
        siData: dealCDef.siData,
        operatorPrefills: prefills,
        methodology: "v21",
        criFormulation: fid,
      });
      const rows = annotateReady(ev, prefills);
      const sf = rows.find((r) => r.candidateId === SF);
      const hg = rows.find((r) => r.candidateId === HG);
      criCompare[fid] = {
        santaFe: sf?.displayedOperatorAlignment,
        highgate: hg?.displayedOperatorAlignment,
        delta:
          sf && hg
            ? Math.round((sf.displayedOperatorAlignment - hg.displayedOperatorAlignment) * 10) / 10
            : null,
        sfAsset: sf?.factorBreakdown?.find((f) => f.key === "assetDevelopmentExperience")?.score,
        hgAsset: hg?.factorBreakdown?.find((f) => f.key === "assetDevelopmentExperience")?.score,
        sfCri: sf?.layers?.v21?.cri?.criScore100,
        hgCri: hg?.layers?.v21?.cri?.criScore100,
      };
    }
  }

  // Synthetic distribution
  const synthV2 = [];
  const synthV21 = [];
  const fixturePrefills = (FIT_V2_OPERATORS || []).map((o) => ({
    operatorId: o.id || o.operatorId,
    companyName: o.name || o.companyName || o.id,
    prefill: o.prefill || o,
  }));
  for (const sc of FIT_V2_SCENARIOS) {
    for (const methodology of ["v2", "v21"]) {
      const ev = evaluateOperatorFitForDeal({
        dealId: `synth_${methodology}_${sc.id}`,
        dealFields: sc.dealFields,
        locationData: sc.locationData,
        mpData: sc.mpData || {},
        siData: sc.siData || {},
        operatorPrefills: fixturePrefills.length ? fixturePrefills : prefills.slice(0, 12),
        methodology,
      });
      for (const r of ev.allEvaluated || []) {
        if (!/eligible/i.test(r.eligibilityStatus || "")) continue;
        (methodology === "v2" ? synthV2 : synthV21).push(r.displayedOperatorAlignment);
      }
    }
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    versions: { v2: OPERATOR_FIT_ENGINE_VERSION, v21: OPERATOR_FIT_ENGINE_VERSION_V21 },
    flags: {
      OPERATOR_FIT_ENGINE_V2: "0",
      OPERATOR_FIT_DIFFERENTIATION_V21: "0",
      note: "Evaluation forced methodology explicitly; env flag remains off",
    },
    dealCDetail,
    criFormulationComparison: criCompare,
    pilotCompare,
    candidateSets,
    distributions: {
      eligibleV2: dist(allV2),
      eligibleV21: dist(allV21),
      rrV2: dist(rrV2),
      rrV21: dist(rrV21),
      synthV2: dist(synthV2),
      synthV21: dist(synthV21),
    },
  };

  writeFileSync(join(root, "reports/operator-fit-v21-evaluation.json"), JSON.stringify(payload, null, 2));
  console.log(
    JSON.stringify(
      {
        wrote: "reports/operator-fit-v21-evaluation.json",
        dealCStatus: dealCDetail?.dealCStatus,
        sf: { v2: dealCDetail?.santaFeV2?.displayed, v21: dealCDetail?.santaFeV21?.displayed },
        hg: { v2: dealCDetail?.highgateV2?.displayed, v21: dealCDetail?.highgateV21?.displayed },
        tiedV21: dealCDetail?.tiedV21,
        delta: dealCDetail?.deltaV21,
        criModerate: criCompare.moderate,
        tiersC: dealCDetail?.tiers?.counts,
        dist: {
          v2: payload.distributions.eligibleV2,
          v21: payload.distributions.eligibleV21,
        },
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
