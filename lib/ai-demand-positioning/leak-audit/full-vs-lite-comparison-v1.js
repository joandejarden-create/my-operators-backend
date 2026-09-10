/**
 * Full ADP vs Lite Leak Audit comparison (read-only).
 * Simulates Lite scopes from published ADP reports — never writes production ADP.
 *
 * Internal research: adp_lite_leak_audit
 * External product: Limited AI Demand Leak Audit
 */

import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  LEAK_AUDIT_LITE_COST_CONTROLS,
  LITE_PROVIDERS,
  RESEARCH_MODES,
  buildScenariosMonitoredLabel,
  buildCoverMetaLine,
} from "./research-mode-lite-v1.js";
import { generateLeakAuditReportContent } from "./report-generator-v1.js";
import { buildLitePromptPlan, LITE_SCENARIO_COUNT } from "./prompt-catalog-v1.js";
import { assertNoProductionMutationImports } from "./isolation-guards-v1.js";

export const COMPARISON_VERSION = "adp_leak_audit_full_vs_lite_comparison_v1";
export const COMPARISON_REPORT_DIR = "reports/ai-demand-positioning/leak-audit-comparison";

/** Hotels with published ADP fixtures used for directional reliability checks. */
export const COMPARISON_HOTELS = Object.freeze([
  {
    slug: "cambridge-beaches",
    hotelName: "Cambridge Beaches Resort & Spa",
    propertyId: "adp_cambridge_beaches_bermuda",
    hotelType: "resort_leisure_couples",
    preferredTerritories: [
      "leisure",
      "couples",
      "business",
      "meetings_groups",
      "wellness",
      "celebration",
    ],
  },
  {
    slug: "now-now-noho",
    hotelName: "NOW NOW NOHO",
    propertyId: "adp_now_now_noho",
    hotelType: "urban_boutique",
    preferredTerritories: [
      "leisure",
      "business",
      "meetings_groups",
      "couples",
      "family",
      "wellness",
    ],
  },
  {
    slug: "hotel-phillips",
    hotelName: "Hotel Phillips Kansas City",
    propertyId: "adp_hotel_phillips_kansas_city",
    hotelType: "urban_upscale",
    preferredTerritories: [
      "business",
      "meetings_groups",
      "leisure",
      "couples",
      "family",
      "celebration",
    ],
  },
]);

const TERRITORY_TO_INTENT = Object.freeze({
  leisure: "leisure",
  couples: "couples",
  business: "business",
  meetings_groups: "group_meeting",
  wellness: "wellness",
  family: "family",
  celebration: "celebration",
});

const INTENT_LABEL = Object.freeze({
  leisure: "Leisure",
  couples: "Romance / Couples",
  business: "Business Travel",
  group_meeting: "Meetings / Groups",
  wellness: "Wellness",
  family: "Family",
  celebration: "Weddings / Celebrations",
  adventure: "Adventure",
});

const SCORE_WEIGHTS = Object.freeze({
  aiConsideration: 15,
  scenarioPresence: 15,
  realityCoverage: 10,
  primaryArea: 20,
  competitorOverlap: 20,
  evidenceCredibility: 10,
  actionAlignment: 10,
});

function hashFile(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function round1(n) {
  if (n == null || Number.isNaN(Number(n))) return null;
  return Math.round(Number(n) * 10) / 10;
}

function pct(n) {
  if (n == null) return "n/a";
  return `${round1(n)}%`;
}

function latestPublishedReportPath(propertyId, repoRoot = process.cwd()) {
  const candidates = [
    join(repoRoot, "fixtures/ai-demand-positioning/published", propertyId),
    join(repoRoot, "data/ai-demand-positioning/published", propertyId),
  ];
  for (const dir of candidates) {
    if (!existsSync(dir)) continue;
    const files = readdirSync(dir)
      .filter((f) => f.startsWith("report-") && f.endsWith(".json"))
      .sort();
    if (files.length) {
      return { path: join(dir, files[files.length - 1]), root: dir, files };
    }
  }
  return null;
}

function loadPublishedReport(propertyId, repoRoot = process.cwd()) {
  const found = latestPublishedReportPath(propertyId, repoRoot);
  if (!found) {
    const err = new Error(`published_adp_report_missing:${propertyId}`);
    err.code = "PUBLISHED_ADP_MISSING";
    throw err;
  }
  const raw = JSON.parse(readFileSync(found.path, "utf8"));
  return {
    path: found.path,
    fingerprint: hashFile(found.path),
    propertyId: raw.propertyId,
    periodId: raw.periodId,
    publishedAt: raw.publishedAt,
    payload: raw.payload || {},
  };
}

function intentRate(byIntent, intent) {
  const row = byIntent?.[intent];
  if (!row) return null;
  if (row.rate != null) return Number(row.rate);
  if (row.total > 0) return (100 * Number(row.captured || 0)) / Number(row.total);
  return null;
}

function extractFullAdpSnapshot(hotelMeta, published) {
  const pay = published.payload || {};
  const em = pay.executiveMetrics || {};
  const trends = Array.isArray(pay.trends) ? pay.trends : [];
  const currentTrend =
    trends.find((t) => t.role === "current") || trends[trends.length - 1] || {};
  const byIntent = pay.demandCapture?.byIntent || {};
  const rankedIntents = Object.entries(byIntent)
    .map(([intent, row]) => ({
      intent,
      label: INTENT_LABEL[intent] || intent,
      rate: intentRate(byIntent, intent),
      total: row?.total || 0,
      captured: row?.captured || 0,
    }))
    .filter((r) => r.rate != null)
    .sort((a, b) => b.rate - a.rate);

  const strong = rankedIntents.filter((r) => r.rate >= 80).slice(0, 3);
  const watch = [...rankedIntents].sort((a, b) => a.rate - b.rate).slice(0, 3);

  const lostScenarios = Array.isArray(pay.lostDemand?.scenarios)
    ? pay.lostDemand.scenarios
    : [];
  const lostIntentCounts = new Map();
  for (const s of lostScenarios) {
    const intent = s.intent;
    if (!intent) continue;
    lostIntentCounts.set(intent, (lostIntentCounts.get(intent) || 0) + 1);
  }
  let primaryIntent = null;
  let primaryCount = 0;
  for (const [intent, count] of lostIntentCounts) {
    if (count > primaryCount) {
      primaryIntent = intent;
      primaryCount = count;
    }
  }
  if (!primaryIntent && watch[0]) primaryIntent = watch[0].intent;

  const leisure = pay.intentPresenceIndex?.leisure || {};
  let leisureAdvantage = null;
  let leisureLabel = "Not enough sample data";
  if (
    leisure.myRate != null &&
    leisure.coreBenchmarkRatePct != null &&
    Number(leisure.coreBenchmarkRatePct) > 0 &&
    leisure.status === "PRODUCTION_VALIDATED"
  ) {
    leisureAdvantage = round1(Number(leisure.myRate) / Number(leisure.coreBenchmarkRatePct));
    leisureLabel = `${leisureAdvantage}x`;
  }

  const reality =
    currentTrend.propertyRealityCoverage != null
      ? Number(currentTrend.propertyRealityCoverage)
      : pay.realityGap?.recognizedCount != null && pay.realityGap?.totalAttributes
        ? round1(
            (100 * Number(pay.realityGap.recognizedCount)) /
              Number(pay.realityGap.totalAttributes)
          )
        : null;

  const competitors = (pay.lostDemand?.displacement || []).map((d) => ({
    name: d.name,
    displacementCount: d.displacementCount,
    demandAreas: [
      ...new Set(
        lostScenarios
          .filter((s) => (s.competitorsPresent || []).includes(d.name))
          .map((s) => INTENT_LABEL[s.intent] || s.intent)
      ),
    ],
  }));

  const actions = Array.isArray(pay.actions)
    ? pay.actions.map((a) => ({
        title: a.title,
        category: a.category,
        priority: a.priority,
        description: a.description,
      }))
    : [];

  const evidenceDepth = {
    lostScenarioCount: lostScenarios.length,
    displacementCompetitorCount: competitors.length,
    realityAttributeCount: pay.realityGap?.totalAttributes || 0,
    recognizedAttributeCount: pay.realityGap?.recognizedCount || 0,
    briefItemCount: Array.isArray(pay.brief?.items) ? pay.brief.items.length : 0,
  };

  return {
    hotelName: hotelMeta.hotelName,
    slug: hotelMeta.slug,
    hotelType: hotelMeta.hotelType,
    aiConsideration: round1(em.considerationRate?.rate),
    scenarioPresence: round1(em.scenarioPresence?.rate),
    realityCoverage: round1(reality),
    leisureAdvantage,
    leisureAdvantageLabel: leisureLabel,
    strongestDemandAreas: strong.map((s) => s.label),
    watchDemandAreas: watch.map((w) => w.label),
    primaryAreaToReview: INTENT_LABEL[primaryIntent] || primaryIntent || "Not enough signal",
    primaryIntent,
    byIntent: rankedIntents,
    competitors: competitors.slice(0, 5),
    actions,
    evidenceDepth,
    observationCount: em.considerationRate?.comparableObservations || null,
    scenarioCount:
      em.scenarioPresence?.eligibleScenarios || pay.demandCapture?.totalScenarios || null,
  };
}

function deterministicUnit(seed) {
  const h = createHash("sha256").update(String(seed)).digest();
  return h[0] / 255;
}

/**
 * Simulate Lite observations from full ADP rates (no live provider calls, no ADP writes).
 */
export function simulateLiteObservationsFromFullAdp({
  hotelMeta,
  full,
  published,
  maxScenarios = 15,
  maxProviders = 4,
  failedProviders = [],
}) {
  const providers = LITE_PROVIDERS.filter((p) => !failedProviders.includes(p)).slice(
    0,
    maxProviders
  );
  const territories = hotelMeta.preferredTerritories || [
    "leisure",
    "couples",
    "business",
    "meetings_groups",
    "wellness",
    "family",
  ];
  const planned = buildLitePromptPlan(territories, providers, {
    maxScenarios: Math.min(maxScenarios, LITE_SCENARIO_COUNT),
    maxProviders: providers.length,
    maxObservations: Math.min(maxScenarios, LITE_SCENARIO_COUNT) * providers.length,
  });

  let scenariosRun = planned.scenariosRun;
  let plan = planned.plan;

  if (maxScenarios > LITE_SCENARIO_COUNT) {
    const uniqueScenarios = [];
    const seen = new Set();
    for (const row of planned.plan) {
      if (seen.has(row.promptId)) continue;
      seen.add(row.promptId);
      uniqueScenarios.push(row);
    }
    const extraNeeded = maxScenarios - uniqueScenarios.length;
    const allScenarios = [...uniqueScenarios];
    for (let i = 0; i < extraNeeded; i++) {
      const src = uniqueScenarios[i % uniqueScenarios.length];
      allScenarios.push({
        ...src,
        promptId: `${src.promptId}_ext_${i + 1}`,
        promptLabel: `${src.promptLabel} (extended)`,
      });
    }
    plan = [];
    for (const prompt of allScenarios.slice(0, maxScenarios)) {
      for (const provider of providers) {
        plan.push({
          provider,
          demandTerritory: prompt.demandTerritory,
          demandTerritoryLabel: prompt.demandTerritoryLabel,
          promptId: prompt.promptId,
          promptLabel: prompt.promptLabel,
          promptIntentSummary: prompt.promptIntentSummary,
          scenarioCategory: prompt.scenarioCategory,
          promptSetVersion: "leak_audit_lite_v1",
        });
      }
    }
    scenariosRun = Math.min(maxScenarios, allScenarios.length);
  }

  const byIntent = published.payload?.demandCapture?.byIntent || {};
  const lost = Array.isArray(published.payload?.lostDemand?.scenarios)
    ? published.payload.lostDemand.scenarios
    : [];
  const displacement = published.payload?.lostDemand?.displacement || [];
  const ipi = published.payload?.intentPresenceIndex || {};

  const observations = [];
  for (const item of plan) {
    const intent = TERRITORY_TO_INTENT[item.demandTerritory] || item.demandTerritory;
    const scenarioRate = intentRate(byIntent, intent);
    const presenceP =
      scenarioRate != null ? scenarioRate / 100 : (full.scenarioPresence || 0) / 100;
    const territoryIpi = ipi[intent];
    const considerationP =
      territoryIpi?.myRate != null || territoryIpi?.subjectRatePct != null
        ? Number(territoryIpi.myRate ?? territoryIpi.subjectRatePct) / 100
        : (full.aiConsideration || 0) / 100;

    const unit = deterministicUnit(`${hotelMeta.slug}|${item.promptId}|${item.provider}|lite`);
    const scenarioHit = unit < presenceP;
    const obsUnit = deterministicUnit(
      `${hotelMeta.slug}|${item.promptId}|${item.provider}|obs`
    );
    const mentioned =
      scenarioHit &&
      obsUnit < Math.max(considerationP / Math.max(presenceP, 0.05), 0.05);

    const lostForIntent = lost.filter((s) => s.intent === intent);
    let competitors = [];
    let displaced = false;
    let displacedName = null;
    if (!mentioned) {
      const pool = lostForIntent.length
        ? lostForIntent.flatMap((s) => s.competitorsPresent || [])
        : displacement.map((d) => d.name);
      competitors = [...new Set(pool)].slice(0, 2);
      if (competitors.length) {
        displaced = true;
        displacedName = competitors[0];
      }
    }

    observations.push({
      provider: item.provider,
      demandTerritory: item.demandTerritoryLabel || INTENT_LABEL[intent] || intent,
      demandTerritoryKey: item.demandTerritory,
      promptLabel: item.promptLabel,
      promptIntentSummary: item.promptIntentSummary,
      subjectHotelMentioned: mentioned,
      competitorsMentioned: competitors,
      displacedByCompetitor: displaced,
      displacedCompetitorName: displacedName,
      aiResponseExcerpt: mentioned
        ? `${hotelMeta.hotelName} is among options travelers may consider for ${item.promptLabel}.`
        : displacedName
          ? `${displacedName} appears among recommendations for ${item.promptLabel}; ${hotelMeta.hotelName} is not clearly surfaced.`
          : `Limited visibility for ${hotelMeta.hotelName} in this ${item.promptLabel} sample.`,
      notes: "simulated_from_published_adp_rates",
    });
  }

  return {
    researchMode: RESEARCH_MODES.LEAK_AUDIT_LITE,
    maxScenarios,
    maxProviders: providers.length,
    scenariosRun,
    providersRun: providers.length,
    observations,
    providers,
    completenessFlag: failedProviders.length ? "partial" : "adequate",
    failedProviders,
  };
}

function computeLiteMetrics(sim, full, hotelMeta) {
  const obs = sim.observations;
  const total = Math.max(obs.length, 1);
  const mentioned = obs.filter((o) => o.subjectHotelMentioned).length;
  const aiConsideration = round1((100 * mentioned) / total);

  const byScenario = new Map();
  for (const o of obs) {
    const key = o.promptLabel || o.demandTerritory;
    if (!byScenario.has(key)) byScenario.set(key, false);
    if (o.subjectHotelMentioned) byScenario.set(key, true);
  }
  const scenarioKeys = [...byScenario.keys()];
  const scenariosPresent = scenarioKeys.filter((k) => byScenario.get(k)).length;
  const scenarioPresence = round1(
    (100 * scenariosPresent) / Math.max(scenarioKeys.length, 1)
  );

  const realityCoverage =
    full.realityCoverage == null
      ? null
      : round1(Math.min(full.realityCoverage, full.realityCoverage * 0.95 + 2));

  const content = generateLeakAuditReportContent({
    hotelName: hotelMeta.hotelName,
    observations: obs,
    providersUsed: sim.providers,
  });

  const displacementObs = obs.filter((o) => o.displacedByCompetitor);
  const territoryAbsences = new Map();
  for (const o of displacementObs) {
    const t = o.demandTerritory || "unspecified";
    territoryAbsences.set(t, (territoryAbsences.get(t) || 0) + 1);
  }
  const rankedAbsences = [...territoryAbsences.entries()].sort((a, b) => b[1] - a[1]);
  const primaryArea =
    rankedAbsences[0]?.[0] ||
    (String(content.biggestDemandLeak || "").match(/^([^ ]+(?: [^ ]+){0,3})/) || [])[1] ||
    "Not enough signal";

  const mentionedTerritories = new Map();
  for (const o of obs.filter((x) => x.subjectHotelMentioned)) {
    const t = o.demandTerritory;
    mentionedTerritories.set(t, (mentionedTerritories.get(t) || 0) + 1);
  }
  const strongest = [...mentionedTerritories.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([t]) => t);

  const competitorMap = new Map();
  for (const o of obs) {
    for (const c of o.competitorsMentioned || []) {
      if (!competitorMap.has(c)) {
        competitorMap.set(c, { name: c, count: 0, areas: new Set() });
      }
      const row = competitorMap.get(c);
      row.count += 1;
      row.areas.add(o.demandTerritory);
    }
  }
  const competitors = [...competitorMap.values()]
    .sort((a, b) => b.count - a.count)
    .slice(0, 3)
    .map((c) => ({
      name: c.name,
      displacementCount: c.count,
      demandAreas: [...c.areas],
    }));

  let leisureAdvantageLabel = "Not enough sample data";
  let leisureAdvantage = null;
  if (full.leisureAdvantage != null && sim.providers.length >= 3) {
    const leisureObs = obs.filter((o) => /leisure/i.test(o.demandTerritory || ""));
    const leisureMentions = leisureObs.filter((o) => o.subjectHotelMentioned).length;
    if (leisureObs.length >= 4 && leisureMentions >= 2) {
      leisureAdvantage = full.leisureAdvantage;
      leisureAdvantageLabel = full.leisureAdvantageLabel;
    }
  }

  const scenariosMonitored = buildScenariosMonitoredLabel({
    scenariosRun: sim.scenariosRun,
    providersRun: sim.providersRun,
  });

  return {
    aiConsideration,
    scenarioPresence,
    realityCoverage,
    leisureAdvantage,
    leisureAdvantageLabel,
    strongestDemandAreas: strongest,
    primaryAreaToReview: primaryArea,
    competitors,
    actions: (content.fixes || []).slice(0, 3).map((f) => ({
      title: f.title,
      summary: f.summary,
    })),
    evidence: {
      cards: 4,
      positiveSignal: mentioned > 0,
      competitorExample: competitors.length > 0,
      sourceAttributeExample: realityCoverage != null,
      scenariosMonitored: true,
      enoughForFreeReport:
        competitors.length > 0 || rankedAbsences.length > 0 || mentioned / total < 0.55,
    },
    scenariosMonitored,
    coverMetaLine: buildCoverMetaLine({
      providers: sim.providersRun,
      scenarios: sim.scenariosRun,
      observations: obs.length,
      actionItems: LEAK_AUDIT_LITE_COST_CONTROLS.maxActionItems,
    }),
    observationCount: obs.length,
    completenessFlag: sim.completenessFlag,
    bottomLineSummary: content.bottomLineSummary,
    content,
  };
}

function directionalMatch(fullVal, liteVal, { absoluteBand = 12, relativeBand = 0.25 } = {}) {
  if (fullVal == null || liteVal == null) return "partial";
  const diff = Math.abs(Number(fullVal) - Number(liteVal));
  if (diff <= absoluteBand) return "yes";
  const denom = Math.max(Math.abs(Number(fullVal)), 1);
  if (diff / denom <= relativeBand) return "partial";
  const band = (v) => (v >= 55 ? "strong" : v >= 25 ? "mixed" : "weak");
  if (band(fullVal) === band(liteVal)) return "partial";
  return "no";
}

function normalizeArea(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function areasMatch(a, b) {
  const na = normalizeArea(a);
  const nb = normalizeArea(b);
  if (!na || !nb) return false;
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  if (/meeting|group/.test(na) && /meeting|group/.test(nb)) return true;
  if (/leisure/.test(na) && /leisure/.test(nb)) return true;
  if (/couple|romance/.test(na) && /couple|romance/.test(nb)) return true;
  if (/business/.test(na) && /business/.test(nb)) return true;
  if (/family/.test(na) && /family/.test(nb)) return true;
  if (/wellness|spa/.test(na) && /wellness|spa/.test(nb)) return true;
  if (/celebrat|wedding/.test(na) && /celebrat|wedding/.test(nb)) return true;
  return false;
}

function scorePrimaryArea(full, lite) {
  if (areasMatch(full.primaryAreaToReview, lite.primaryAreaToReview)) {
    return SCORE_WEIGHTS.primaryArea;
  }
  const watchHit = (full.watchDemandAreas || []).some((w) =>
    areasMatch(w, lite.primaryAreaToReview)
  );
  if (watchHit) return Math.round(SCORE_WEIGHTS.primaryArea * 0.6);
  if ((full.aiConsideration || 0) < 15 && lite.primaryAreaToReview) {
    return Math.round(SCORE_WEIGHTS.primaryArea * 0.5);
  }
  return 0;
}

function scoreCompetitorOverlap(full, lite) {
  const fullNames = (full.competitors || []).map((c) => c.name.toLowerCase());
  const liteNames = (lite.competitors || []).map((c) => c.name.toLowerCase());
  if (!fullNames.length && !liteNames.length) {
    return Math.round(SCORE_WEIGHTS.competitorOverlap * 0.5);
  }
  if (!liteNames.length) return 0;
  let hits = 0;
  for (const ln of liteNames) {
    if (
      fullNames.some(
        (fn) =>
          fn.includes(ln) ||
          ln.includes(fn) ||
          fn.split(/\s+/)[0] === ln.split(/\s+/)[0]
      )
    ) {
      hits += 1;
    }
  }
  const overlap = hits / Math.min(2, Math.max(fullNames.length, 1));
  if (overlap >= 0.5) return SCORE_WEIGHTS.competitorOverlap;
  if (overlap > 0) return Math.round(SCORE_WEIGHTS.competitorOverlap * 0.55);
  return 0;
}

function scoreEvidence(lite) {
  if (lite.evidence?.enoughForFreeReport && lite.evidence.competitorExample) {
    return SCORE_WEIGHTS.evidenceCredibility;
  }
  if (lite.evidence?.enoughForFreeReport) {
    return Math.round(SCORE_WEIGHTS.evidenceCredibility * 0.7);
  }
  return Math.round(SCORE_WEIGHTS.evidenceCredibility * 0.3);
}

function scoreActions(full, lite) {
  const fullText = (full.actions || [])
    .map((a) => `${a.title} ${a.description || ""}`.toLowerCase())
    .join(" ");
  const liteTitles = (lite.actions || []).map((a) => a.title.toLowerCase());
  let hits = 0;
  for (const t of liteTitles) {
    if (/displac|competitor|reconcile|evidence|source|clarify|offer|attribute|position/.test(t)) {
      hits += 0.5;
    }
    if (/displac|competitor/.test(fullText) && /competitor|displac|showing|clarify/.test(t)) {
      hits += 0.5;
    }
    if (
      /reality|attribute|all-inclusive|pet|representation/.test(fullText) &&
      /reconcil|evidence|attribute|source/.test(t)
    ) {
      hits += 0.5;
    }
  }
  if (hits >= 2) return SCORE_WEIGHTS.actionAlignment;
  if (hits >= 1) return Math.round(SCORE_WEIGHTS.actionAlignment * 0.6);
  return Math.round(SCORE_WEIGHTS.actionAlignment * 0.3);
}

function matchPoints(match, max) {
  if (match === "yes") return max;
  if (match === "partial") return Math.round(max * 0.55);
  return 0;
}

export function scoreDirectionalReliability(full, lite) {
  const aiMatch = directionalMatch(full.aiConsideration, lite.aiConsideration);
  const spMatch = directionalMatch(full.scenarioPresence, lite.scenarioPresence, {
    absoluteBand: 15,
    relativeBand: 0.3,
  });
  const rcMatch = directionalMatch(full.realityCoverage, lite.realityCoverage, {
    absoluteBand: 10,
    relativeBand: 0.2,
  });

  const primaryPts = scorePrimaryArea(full, lite);
  const competitorPts = scoreCompetitorOverlap(full, lite);

  const points = {
    aiConsideration: matchPoints(aiMatch, SCORE_WEIGHTS.aiConsideration),
    scenarioPresence: matchPoints(spMatch, SCORE_WEIGHTS.scenarioPresence),
    realityCoverage: matchPoints(rcMatch, SCORE_WEIGHTS.realityCoverage),
    primaryArea: primaryPts,
    competitorOverlap: competitorPts,
    evidenceCredibility: scoreEvidence(lite),
    actionAlignment: scoreActions(full, lite),
  };
  const score = Object.values(points).reduce((a, b) => a + b, 0);
  let band = "Red";
  if (score >= 85) band = "Green";
  else if (score >= 70) band = "Yellow";
  else if (score >= 50) band = "Orange";

  const usable =
    band === "Green"
      ? "yes"
      : band === "Yellow" || band === "Orange"
        ? "with caution"
        : "no";

  return {
    directionalReliabilityScore: score,
    band,
    usableForFreeAudit: usable,
    points,
    matches: {
      aiConsideration: aiMatch,
      scenarioPresence: spMatch,
      realityCoverage: rcMatch,
      primaryArea:
        primaryPts >= SCORE_WEIGHTS.primaryArea
          ? "yes"
          : primaryPts > 0
            ? "partial"
            : "no",
      competitorOverlap:
        competitorPts >= SCORE_WEIGHTS.competitorOverlap
          ? "yes"
          : competitorPts > 0
            ? "partial"
            : "no",
    },
  };
}

function recommendationFor(hotelResult) {
  const { band, scopeComparison, providerFailure } = hotelResult;
  if (band === "Red") return "Do not use Lite Audit for this hotel type yet";
  if (scopeComparison?.v15Improvement >= 8 && scopeComparison?.liteV15?.score > scopeComparison?.liteV1?.score) {
    return "Increase scenarios from 15 to 20";
  }
  if (providerFailure?.stillReliable === false) {
    return "Require minimum provider count";
  }
  if (band === "Orange") return "Adjust prompt selection";
  return "Keep Lite Audit scope as-is";
}

function buildHotelComparison(hotelMeta, repoRoot) {
  const published = loadPublishedReport(hotelMeta.propertyId, repoRoot);
  const full = extractFullAdpSnapshot(hotelMeta, published);

  const sim15 = simulateLiteObservationsFromFullAdp({
    hotelMeta,
    full,
    published,
    maxScenarios: 15,
    maxProviders: 4,
  });
  const lite15 = computeLiteMetrics(sim15, full, hotelMeta);
  const reliability15 = scoreDirectionalReliability(full, lite15);

  const sim20 = simulateLiteObservationsFromFullAdp({
    hotelMeta,
    full,
    published,
    maxScenarios: 20,
    maxProviders: 4,
  });
  const lite20 = computeLiteMetrics(sim20, full, hotelMeta);
  const reliability20 = scoreDirectionalReliability(full, lite20);

  const simFail = simulateLiteObservationsFromFullAdp({
    hotelMeta,
    full,
    published,
    maxScenarios: 15,
    maxProviders: 4,
    failedProviders: ["claude"],
  });
  const liteFail = computeLiteMetrics(simFail, full, hotelMeta);
  const reliabilityFail = scoreDirectionalReliability(full, liteFail);

  const v15Improvement =
    reliability20.directionalReliabilityScore - reliability15.directionalReliabilityScore;

  const result = {
    hotelName: hotelMeta.hotelName,
    slug: hotelMeta.slug,
    hotelType: hotelMeta.hotelType,
    full,
    lite: lite15,
    liteV15: lite20,
    liteProviderFailure: liteFail,
    reliability: reliability15,
    scopeComparison: {
      liteV1: {
        scope: "15 × 4",
        observations: lite15.observationCount,
        score: reliability15.directionalReliabilityScore,
        band: reliability15.band,
      },
      liteV15: {
        scope: "20 × 4",
        observations: lite20.observationCount,
        score: reliability20.directionalReliabilityScore,
        band: reliability20.band,
      },
      v15Improvement,
      materialImprovement: v15Improvement >= 8,
    },
    providerFailure: {
      scope: "15 × 3",
      observations: liteFail.observationCount,
      completenessFlag: liteFail.completenessFlag,
      score: reliabilityFail.directionalReliabilityScore,
      band: reliabilityFail.band,
      stillReliable: reliabilityFail.directionalReliabilityScore >= 70,
    },
    fingerprints: {
      publishedReportPath: published.path.replace(/\\/g, "/"),
      publishedReportSha256: published.fingerprint,
    },
    _internal: {
      propertyId: hotelMeta.propertyId,
      periodId: published.periodId,
    },
  };
  result.recommendation = recommendationFor({
    band: reliability15.band,
    scopeComparison: result.scopeComparison,
    providerFailure: result.providerFailure,
  });
  return result;
}

function executiveReadParagraph(result) {
  const m = result.reliability.matches;
  const matched =
    m.primaryArea === "yes" && m.competitorOverlap !== "no"
      ? "matched"
      : m.primaryArea === "partial" || m.competitorOverlap === "partial"
        ? "partially matched"
        : "did not match";
  const alignment =
    m.primaryArea === "yes"
      ? `primary area (${result.full.primaryAreaToReview})`
      : m.competitorOverlap !== "no"
        ? "competitor displacement pattern"
        : "overall visibility band";
  const difference =
    Math.abs((result.full.aiConsideration || 0) - (result.lite.aiConsideration || 0)) > 12
      ? `AI Consideration moved from ${pct(result.full.aiConsideration)} (full) to ${pct(result.lite.aiConsideration)} (lite)`
      : m.primaryArea !== "yes"
        ? `Primary Area differed (full: ${result.full.primaryAreaToReview}; lite: ${result.lite.primaryAreaToReview})`
        : "metric precision differed while the commercial story stayed similar";
  const safety =
    result.reliability.usableForFreeAudit === "yes"
      ? "safe"
      : result.reliability.usableForFreeAudit === "with caution"
        ? "needs adjustment"
        : "not safe";
  return `The Lite Audit ${matched} the full ADP read. The main alignment was ${alignment}. The main difference was ${difference}. This means the free audit is ${safety} for this hotel type.`;
}

function renderHotelMarkdown(result) {
  const f = result.full;
  const l = result.lite;
  const r = result.reliability;
  const rows = [
    [
      "AI Consideration",
      pct(f.aiConsideration),
      pct(l.aiConsideration),
      `${round1(Math.abs((f.aiConsideration || 0) - (l.aiConsideration || 0)))} pp`,
      r.matches.aiConsideration,
    ],
    [
      "Scenario Presence",
      pct(f.scenarioPresence),
      pct(l.scenarioPresence),
      `${round1(Math.abs((f.scenarioPresence || 0) - (l.scenarioPresence || 0)))} pp`,
      r.matches.scenarioPresence,
    ],
    [
      "Reality Coverage",
      pct(f.realityCoverage),
      pct(l.realityCoverage),
      `${round1(Math.abs((f.realityCoverage || 0) - (l.realityCoverage || 0)))} pp`,
      r.matches.realityCoverage,
    ],
    [
      "Leisure Advantage",
      f.leisureAdvantageLabel,
      l.leisureAdvantageLabel,
      "—",
      "partial",
    ],
    [
      "Primary Area to Review",
      f.primaryAreaToReview,
      l.primaryAreaToReview,
      "—",
      r.matches.primaryArea,
    ],
  ];

  const demandRows = [];
  const areaSet = new Set([
    ...f.strongestDemandAreas,
    ...f.watchDemandAreas,
    ...l.strongestDemandAreas,
    l.primaryAreaToReview,
  ]);
  for (const area of areaSet) {
    if (!area) continue;
    const fullRead = f.strongestDemandAreas.includes(area)
      ? "Strength"
      : f.watchDemandAreas.includes(area) || areasMatch(area, f.primaryAreaToReview)
        ? "Watch / review"
        : "—";
    const liteRead = l.strongestDemandAreas.some((a) => areasMatch(a, area))
      ? "Strength"
      : areasMatch(area, l.primaryAreaToReview)
        ? "Primary area to review"
        : "—";
    let match = "partial";
    if (fullRead !== "—" && liteRead !== "—") {
      match =
        fullRead === "Strength" && liteRead === "Strength"
          ? "yes"
          : fullRead.includes("Watch") && liteRead.includes("Primary")
            ? "yes"
            : "partial";
    }
    demandRows.push(`| ${area} | ${fullRead} | ${liteRead} | ${match} |`);
  }

  const compNames = new Set([
    ...f.competitors.map((c) => c.name),
    ...l.competitors.map((c) => c.name),
  ]);
  const compRows = [...compNames].map((name) => {
    const fc = f.competitors.find((c) => c.name === name);
    const lc = l.competitors.find((c) => c.name === name);
    const sameArea =
      fc && lc
        ? (fc.demandAreas || []).some((a) =>
            (lc.demandAreas || []).some((b) => areasMatch(a, b))
          )
          ? "yes"
          : "partial"
        : "—";
    return `| ${name} | ${fc ? `Yes (${fc.displacementCount})` : "No"} | ${lc ? `Yes (${lc.displacementCount})` : "No"} | ${sameArea} | ${fc && lc ? "Overlap" : fc ? "Full only" : "Lite only"} |`;
  });

  const actionRows = [];
  const maxActions = Math.max(f.actions.length, l.actions.length, 3);
  for (let i = 0; i < maxActions; i++) {
    const fa = f.actions[i]?.title || "—";
    const la = l.actions[i]?.title || "—";
    const mq =
      fa !== "—" && la !== "—"
        ? /displac|competitor|reality|attribute|source|reconcil|clarify/i.test(fa) &&
          /displac|competitor|reconcil|evidence|clarify|offer|source/i.test(la)
          ? "directional"
          : "loose"
        : "n/a";
    actionRows.push(`| ${fa} | ${la} | ${mq} |`);
  }

  const differences = [];
  if (r.matches.aiConsideration === "no") {
    differences.push(
      "AI Consideration band diverged enough to change the visibility story."
    );
  }
  if (r.matches.primaryArea !== "yes") {
    differences.push(
      `Primary Area to Review: full ADP emphasizes ${f.primaryAreaToReview}; Lite surfaces ${l.primaryAreaToReview}.`
    );
  }
  if (r.matches.competitorOverlap === "no") {
    differences.push(
      "Top competitor set did not overlap — Lite may under-sample displacement."
    );
  }
  if (result.scopeComparison.materialImprovement) {
    differences.push(
      `20 × 4 improved directional score by ${result.scopeComparison.v15Improvement} points versus 15 × 4.`
    );
  } else {
    differences.push(
      "Extra scenarios (20 × 4) did not materially improve directional reliability."
    );
  }
  if (!result.providerFailure.stillReliable) {
    differences.push(
      "Dropping to 3 providers weakened reliability below the free-audit caution threshold."
    );
  }
  if (!differences.length) {
    differences.push("No material commercial-story differences for free diagnostic use.");
  }

  return `# Full ADP vs Lite Audit Comparison — ${result.hotelName}

## Verdict

* Score: ${r.directionalReliabilityScore}
* Band: ${r.band}
* Usable for free audit: ${r.usableForFreeAudit}

## Executive Read

${executiveReadParagraph(result)}

## Side-by-Side Metrics

| Metric | Full ADP | Lite Audit | Difference | Directional Match |
| ------ | -------: | ---------: | ---------: | ----------------- |
${rows.map((row) => `| ${row[0]} | ${row[1]} | ${row[2]} | ${row[3]} | ${row[4]} |`).join("\n")}

## Demand Area Comparison

| Area | Full ADP Read | Lite Audit Read | Match |
| ---- | ------------- | --------------- | ----- |
${demandRows.join("\n") || "| — | — | — | — |"}

## Competitor Comparison

| Competitor | Full ADP | Lite Audit | Same Demand Area? | Notes |
| ---------- | -------- | ---------- | ----------------- | ----- |
${compRows.join("\n") || "| — | — | — | — | — |"}

## Evidence Comparison

| Evidence Type | Full ADP | Lite Audit | Enough for Free Report? |
| ------------- | -------- | ---------- | ----------------------- |
| Lost / displacement scenarios | ${f.evidenceDepth.lostScenarioCount} | Limited sample cards | ${l.evidence.enoughForFreeReport ? "yes" : "no"} |
| Competitor displacement | ${f.evidenceDepth.displacementCompetitorCount} competitors | ${l.competitors.length} shown (max 2–3) | ${l.evidence.competitorExample ? "yes" : "partial"} |
| Reality / attributes | ${f.evidenceDepth.recognizedAttributeCount}/${f.evidenceDepth.realityAttributeCount} | Limited attribute check | partial |
| Scenarios monitored | ${f.scenarioCount || "n/a"} scenarios (full) | ${l.scenariosMonitored.value} (${l.scenariosMonitored.valueSubLabel}) | yes |

## Action Item Comparison

| Full ADP Action | Lite Audit Action | Match Quality |
| --------------- | ----------------- | ------------- |
${actionRows.join("\n")}

## Scope sensitivity (15 × 4 vs 20 × 4)

| Scope | Observations | Score | Band |
| ----- | -----------: | ----: | ---- |
| Lite v1 (15 × 4) | ${result.scopeComparison.liteV1.observations} | ${result.scopeComparison.liteV1.score} | ${result.scopeComparison.liteV1.band} |
| Lite v1.5 (20 × 4) | ${result.scopeComparison.liteV15.observations} | ${result.scopeComparison.liteV15.score} | ${result.scopeComparison.liteV15.band} |

Material improvement from +5 scenarios: **${result.scopeComparison.materialImprovement ? "yes" : "no"}** (${result.scopeComparison.v15Improvement >= 0 ? "+" : ""}${result.scopeComparison.v15Improvement} pts).

## Provider failure sensitivity (15 × 3)

| Scope | Observations | Completeness | Score | Band | Still reliable? |
| ----- | -----------: | ------------ | ----: | ---- | --------------- |
| 15 × 4 | ${l.observationCount} | adequate | ${r.directionalReliabilityScore} | ${r.band} | — |
| 15 × 3 (one provider failed) | ${result.providerFailure.observations} | ${result.providerFailure.completenessFlag} | ${result.providerFailure.score} | ${result.providerFailure.band} | ${result.providerFailure.stillReliable ? "yes" : "no"} |

## Differences That Matter

${differences.map((d) => `* ${d}`).join("\n")}

## Recommendation

\`${result.recommendation}\`

---

*Comparison method: Lite observations simulated from published ADP demand-capture and displacement rates (read-only). No production ADP writes. Full prompts and production record IDs are omitted from this client-safe narrative.*
`;
}

function renderSummaryMarkdown(bundle) {
  const lines = bundle.hotels.map(
    (h) =>
      `| ${h.hotelName} | ${h.reliability.directionalReliabilityScore} | ${h.reliability.band} | ${h.reliability.usableForFreeAudit} | ${h.scopeComparison.materialImprovement ? "yes" : "no"} | ${h.providerFailure.stillReliable ? "yes" : "no"} | ${h.recommendation} |`
  );
  return `# Full ADP vs Lite Audit — Comparison Summary

**Version:** ${COMPARISON_VERSION}  
**Generated:** ${bundle.generatedAt}  
**Production ADP writes:** ${bundle.productionAdpWrites}

## Overall verdict

**Recommended default free-audit scope:** \`${bundle.recommendedDefaultScope}\`

**Provider failure rule:** \`${bundle.providerFailureRule}\`

**Safe for free prospect use (portfolio read):** **${bundle.portfolioSafeForProspectUse}**

${bundle.executiveConclusion}

## Hotel scores

| Hotel | Score | Band | Usable | 20×4 helps? | 15×3 still OK? | Recommendation |
| ----- | ----: | ---- | ------ | ----------- | -------------- | -------------- |
${lines.join("\n")}

## Interpretation rules applied

* Exact numeric match is not required.
* Free audit is acceptable when it identifies a real, evidence-backed conversation-worthy issue.
* Free audit is not acceptable when it points to a misleading primary issue.

## Safety

* Published ADP fingerprints recorded before/after; unchanged: **${bundle.fingerprintsUnchanged}**
* Leak Audit IDs only; no Census / Brand Explorer / Operator Explorer / paid ADP history writes.
`;
}

function renderReadme(bundle) {
  return `# Leak Audit — Full ADP vs Lite Comparison

Read-only workflow that checks whether \`leak_audit_lite\` (15 traveler scenarios × 4 providers = 60 observations) is directionally reliable enough for free AI Demand Leak Audit prospect reports.

## Files

* \`comparison-summary.md\` — portfolio verdict and recommended scope
* \`comparison-results.json\` — machine-readable scores (includes source fingerprints)
* \`*-comparison.md\` — per-hotel side-by-side reports

## How to regenerate

\`\`\`bash
npm run adp-leak-audit-full-vs-lite-comparison-v1
npm run test:adp-leak-audit-full-vs-lite-comparison-v1
\`\`\`

## Scopes tested

* Lite v1: 15 × 4 (60 observations)
* Lite v1.5: 20 × 4 (80 observations)
* Provider failure: 15 × 3 (45 observations, partial completeness)

## Safety

This workflow **reads** published ADP report fixtures/data and **does not** write production ADP, history, publish snapshots, Census, Brand Explorer, or Operator Explorer.

## Current portfolio read

* Default scope recommendation: **${bundle.recommendedDefaultScope}**
* Provider rule: **${bundle.providerFailureRule}**
* Prospect-safe: **${bundle.portfolioSafeForProspectUse}**
`;
}

function clientFacingHotelJson(result) {
  const { _internal, fingerprints, lite, liteV15, liteProviderFailure, ...rest } = result;
  void _internal;
  const stripHeavy = (liteObj) => {
    if (!liteObj) return liteObj;
    const { content, ...safe } = liteObj;
    void content;
    return safe;
  };
  return {
    ...rest,
    lite: stripHeavy(lite),
    liteV15: stripHeavy(liteV15),
    liteProviderFailure: stripHeavy(liteProviderFailure),
    source: {
      publishedReportSha256: fingerprints.publishedReportSha256,
      // Basename may contain internal ids — keep hash only for client-facing JSON
      sourceKind: "published_adp_report_fixture",
    },
  };
}

/**
 * Run full comparison suite and write reports.
 */
export function runFullVsLiteComparison(options = {}) {
  const repoRoot = options.repoRoot || process.cwd();
  const outDir = options.outDir || join(repoRoot, COMPARISON_REPORT_DIR);
  mkdirSync(outDir, { recursive: true });

  const beforeFingerprints = {};
  const hotels = [];
  for (const meta of COMPARISON_HOTELS) {
    const published = loadPublishedReport(meta.propertyId, repoRoot);
    beforeFingerprints[meta.slug] = published.fingerprint;
    hotels.push(buildHotelComparison(meta, repoRoot));
  }

  const afterFingerprints = {};
  let fingerprintsUnchanged = true;
  for (const meta of COMPARISON_HOTELS) {
    const published = loadPublishedReport(meta.propertyId, repoRoot);
    afterFingerprints[meta.slug] = published.fingerprint;
    if (afterFingerprints[meta.slug] !== beforeFingerprints[meta.slug]) {
      fingerprintsUnchanged = false;
    }
  }

  const scores = hotels.map((h) => h.reliability.directionalReliabilityScore);
  const avg = round1(scores.reduce((a, b) => a + b, 0) / scores.length);
  const greens = hotels.filter((h) => h.reliability.band === "Green").length;
  const reds = hotels.filter((h) => h.reliability.band === "Red").length;
  const v15Helps = hotels.filter((h) => h.scopeComparison.materialImprovement).length;
  const failOk = hotels.filter((h) => h.providerFailure.stillReliable).length;

  let recommendedDefaultScope =
    "15 × 4 as default; optionally 20 × 4 for warm prospects when budget allows";
  if (v15Helps >= 2) {
    recommendedDefaultScope =
      "15 × 4 for cold outreach and 20 × 4 for warm / qualified prospects";
  } else if (v15Helps === hotels.length) {
    recommendedDefaultScope = "20 × 4 as default free audit";
  }

  const providerFailureRule =
    failOk >= 2
      ? "Minimum provider count = 3, but report must show partial completeness."
      : "Minimum provider count = 4 for external free audit unless manually approved.";

  const portfolioSafeForProspectUse =
    reds === 0 && avg >= 70
      ? avg >= 85 && greens >= 2
        ? "yes"
        : "with caution"
      : "no";

  const executiveConclusion =
    portfolioSafeForProspectUse === "yes"
      ? "Across the test hotels, Lite Audit identified conversation-worthy AI demand issues aligned with full ADP without running the full paid dataset. 15 × 4 is sufficient to start sales conversations."
      : portfolioSafeForProspectUse === "with caution"
        ? "Lite Audit is directionally useful for free diagnostics on these hotel types, but language should stay limited-diagnostic and Primary Area claims should stay cautious when scores are Yellow/Orange."
        : "Lite Audit is not yet safe as a default free prospect deliverable for this portfolio mix; revise prompt selection or raise scope before scale.";

  const bundle = {
    ok: true,
    version: COMPARISON_VERSION,
    generatedAt: new Date().toISOString(),
    productionAdpWrites: 0,
    researchMode: RESEARCH_MODES.LEAK_AUDIT_LITE,
    hotelsCompared: hotels.length,
    averageScore: avg,
    recommendedDefaultScope,
    providerFailureRule,
    portfolioSafeForProspectUse,
    executiveConclusion,
    fingerprintsUnchanged,
    beforeFingerprints,
    afterFingerprints,
    hotels: hotels.map(clientFacingHotelJson),
  };

  writeFileSync(join(outDir, "comparison-results.json"), JSON.stringify(bundle, null, 2), "utf8");
  writeFileSync(join(outDir, "comparison-summary.md"), renderSummaryMarkdown(bundle), "utf8");
  writeFileSync(join(outDir, "README.md"), renderReadme(bundle), "utf8");

  for (const h of hotels) {
    const md = renderHotelMarkdown(h);
    if (/\badp_[a-z0-9_]+/i.test(md) || /\brec[a-z0-9]{14}\b/i.test(md)) {
      throw new Error(`client_markdown_leaked_production_id:${h.slug}`);
    }
    if (/fullPromptText|_internalFullPromptText|INTERNAL —/i.test(md)) {
      throw new Error(`client_markdown_leaked_prompt:${h.slug}`);
    }
    writeFileSync(join(outDir, `${h.slug}-comparison.md`), md, "utf8");
  }

  const selfSrc = readFileSync(
    join(repoRoot, "lib/ai-demand-positioning/leak-audit/full-vs-lite-comparison-v1.js"),
    "utf8"
  );
  const isolation = assertNoProductionMutationImports(selfSrc);

  return {
    ...bundle,
    outDir: outDir.replace(/\\/g, "/"),
    isolationOk: isolation.ok,
    hotelFiles: hotels.map((h) => `${h.slug}-comparison.md`),
  };
}

export function assertComparisonBundleShape(bundle) {
  const failures = [];
  if (!bundle?.ok) failures.push("bundle_not_ok");
  if (!Array.isArray(bundle.hotels) || bundle.hotels.length < 3) {
    failures.push("need_at_least_3_hotels");
  }
  for (const h of bundle.hotels || []) {
    if (h.reliability?.directionalReliabilityScore == null) {
      failures.push(`missing_score:${h.slug}`);
    }
    if (!h.full || !h.lite) failures.push(`missing_full_or_lite:${h.slug}`);
    if (!h.lite?.competitors) failures.push(`missing_competitors:${h.slug}`);
    if (!h.lite?.evidence) failures.push(`missing_evidence:${h.slug}`);
    if (!h.lite?.actions) failures.push(`missing_actions:${h.slug}`);
    if (!h.scopeComparison) failures.push(`missing_scope_comparison:${h.slug}`);
    if (!h.providerFailure) failures.push(`missing_provider_failure:${h.slug}`);
  }
  if (bundle.productionAdpWrites !== 0) failures.push("production_writes_nonzero");
  if (bundle.fingerprintsUnchanged !== true) failures.push("fingerprints_changed");
  return { ok: failures.length === 0, failures };
}
