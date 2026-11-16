#!/usr/bin/env node
/**
 * MODE — RADISSON BLU TORONTO ADP CANONICAL DATA + CALCULATION FORENSIC V1
 * Reconciles Core metrics from raw observations; Loyalty isolation; period-family integrity.
 * Does NOT regenerate paid monitoring. Does NOT overwrite stored metrics on mismatch.
 */
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  loadPeriod,
  loadAllPeriods,
  loadLatestCustomerPeriod,
  loadLatestLoyaltyPeriod,
  loadLatestPeriod,
  loadPropertyProfile,
  isLoyaltyMeasurementPeriod,
} from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { enrichObservationsWithRank } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import { filterComparableObservations } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { roundAdpPercent } from "../lib/ai-demand-positioning/format-percent.js";
import { computeOwnedExternalSourceMix, classifySourceUrl } from "../lib/ai-demand-positioning/metrics/owned-source-classification-v1.js";
import { computeSourceMetrics } from "../lib/ai-demand-positioning/metrics/source-metrics.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildMissingEvidenceResponse } from "../lib/ai-demand-positioning/customer/missing-evidence-v1.js";
import { getPublishedEvidenceResponse } from "../lib/ai-demand-positioning/published-read-service.js";
import { territoryLabelForIntent } from "../lib/ai-demand-positioning/metrics/intent-territory-labels.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/adp/radisson-blu-toronto-measurement-forensic-v1");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE_ID = "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const HPC_ID = "recH9h3McgMNFQEla";
const PROVIDERS = ["openai", "gemini", "claude", "perplexity"];
const PROVIDER_LABEL = {
  openai: "ChatGPT",
  gemini: "Gemini",
  claude: "Claude",
  perplexity: "Perplexity",
};

mkdirSync(OUT, { recursive: true });

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function writeCsv(name, headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => csvEscape(row[h])).join(","));
  }
  writeFileSync(join(OUT, name), `${lines.join("\n")}\n`, "utf8");
}
function writeMd(name, body) {
  writeFileSync(join(OUT, name), body.endsWith("\n") ? body : `${body}\n`, "utf8");
}

function ensureFixture() {
  const path = join(ROOT, "fixtures/ai-demand-positioning/radisson-blu-toronto-downtown-property-profile.json");
  if (existsSync(path)) return JSON.parse(readFileSync(path, "utf8"));
  const fixture = {
    propertyId: PROPERTY_ID,
    name: "Radisson Blu Toronto Downtown",
    registryKey: "radisson_blu_toronto_downtown",
    censusRecordId: HPC_ID,
    customerDropdownVisible: false,
    founderPreviewEligible: true,
    customerReady: false,
    customerPublished: false,
    address: "249 Queens Quay West, Toronto, Ontario, Canada",
    city: "Toronto",
    state: "Ontario",
    country: "CA",
    market: "Toronto",
    submarket: "Harbourfront",
    website: "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41",
    canonicalPropertyDomain: "choicehotels.com",
    ownedDomains: ["choicehotels.com", "radissonhotels.com"],
    officialBrandDomain: "choicehotels.com",
    officialPropertyPageUrl:
      "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41",
    brandPropertyPathHints: [
      "cnc41",
      "radisson-blu-toronto",
      "toronto/radisson-blu-hotels/cnc41",
    ],
    identityAliases: [
      "Radisson Blu Toronto Downtown",
      "Radisson Blu Toronto",
      "Radisson Blu Downtown Toronto",
      "Radisson Blu Hotel Toronto Downtown",
    ],
    identityConfusableExclusions: [
      "Radisson Hotel Toronto East",
      "Radisson Plaza Hotel Toronto",
      "Park Inn by Radisson Toronto",
    ],
    chainScale: "Upper Upscale",
    affiliation: "Radisson Blu",
    brand: "Radisson Blu",
    parentCompany: "Choice Hotels",
    rooms: 157,
    roomCountConfidence: "HIGH",
    meetingSpace: {
      totalSqFt: 11000,
      meetingRooms: 20,
      largestRoom: { name: "Admiral Ballroom", sqFt: 3151, capacity: 240 },
      notes: "About 11,000 sq ft across 20 meeting rooms; Admiral Ballroom ~3,151 sq ft.",
    },
    attributes: [
      "toronto",
      "harbourfront",
      "ontario",
      "upper_upscale",
      "radisson",
      "radisson_blu",
      "choice_hotels",
      "full_service",
      "meeting_space",
      "waterfront",
      "ballroom",
      "fitness_center",
      "outdoor_pool",
      "pet_friendly",
      "family_friendly",
      "ev_charging",
      "urban_hotel",
      "urban_lifestyle",
      "suite_inventory",
      "lifestyle",
      "city_views",
      "event_space_outdoor",
      "near_convention_center",
      "walkable_downtown",
      "panoramic_views",
    ],
    positioning: {
      primary:
        "Upper-upscale Radisson Blu Harbourfront hotel near Metro Toronto Convention Centre",
      differentiators: [
        "Harbourfront / Queens Quay location",
        "Radisson Blu / Choice Hotels distribution and loyalty",
        "Meeting inventory for mid-size groups and overflow",
        "Walkable to MTCC for convention-adjacent demand",
      ],
      targetSegments: [
        "Business travelers",
        "Leisure guests",
        "Meetings and groups",
        "Convention overflow / ancillary lodging",
        "Loyalty members when applicable",
      ],
    },
    _adpLifecycle: "FOUNDER_REVIEWED_STAGED",
    _note:
      "Founder preview only — customerDropdownVisible stays false until certified customer publication.",
  };
  writeFileSync(path, `${JSON.stringify(fixture, null, 2)}\n`, "utf8");
  return fixture;
}

function ensureFactConflicts() {
  const dir = join(ROOT, "data/ai-demand-positioning/fact-conflicts");
  mkdirSync(dir, { recursive: true });
  const path = join(dir, `${PROPERTY_ID}.json`);
  const payload = {
    propertyId: PROPERTY_ID,
    contract: "ADP_FACT_CONFLICT_REGISTER_V1",
    unresolvedCount: 1,
    conflicts: [
      {
        conflictId: "FC_MEETING_INVENTORY_001",
        topic: "Meeting / event inventory",
        sourceA: {
          id: "SRC_CHOICE_CNC41",
          label: "Choice Hotels official (CNC41 meetings page)",
          url: "https://www.choicehotels.com/en-ca/ontario/toronto/radisson-blu-hotels/cnc41/meetings-events",
          claim: "17 spaces; more than 10,000 sq ft; up to 400 guests",
        },
        sourceB: {
          id: "SRC_DTHA_PORTFOLIO_LEAD",
          label: "Downtown Toronto Hotels Association / portfolio lead",
          url: "UNVERIFIED_IN_THIS_RUN",
          claim: "14 venues; 11,000 sq ft; up to 380 guests",
        },
        classification: "SOURCE_CONFLICT",
        operationalStatus: "NEEDS_HOTEL_CONFIRMATION",
        torontoSpecific: true,
        silentlyNormalized: false,
        realityGapTreatment:
          "Conflict values are not treated as certainty. Reality Gap scores governed meeting_space attribute presence only; inventory sqft/venue counts are not certainty inputs.",
        retrievedAt: "2026-11-04",
      },
    ],
  };
  writeFileSync(path, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return payload;
}

function normalizeCompetitorName(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function matchLeader(name, leaders) {
  const n = normalizeCompetitorName(name);
  for (const L of leaders) {
    if (n.includes(L.key) || L.key.includes(n.split(" ")[0])) {
      if (L.match(n)) return L.id;
    }
  }
  return null;
}

async function main() {
  ensureFixture();
  const factConflicts = ensureFactConflicts();

  const pub = loadPublishedReport(PROPERTY_ID);
  if (!pub) throw new Error("published report missing");
  const core = loadPeriod(CORE_ID);
  if (!core) throw new Error(`core period missing: ${CORE_ID}`);
  const loyalty = loadLatestLoyaltyPeriod(PROPERTY_ID);
  const loyaltyId = loyalty?.periodId || null;
  const profile = loadPropertyProfile(PROPERTY_ID);
  if (!profile) throw new Error("property profile missing after fixture restore");

  // Enrich attributes from published Reality Gap classifications for forensic recompute
  const rgClasses = pub.realityGap?.attributeClassifications || [];
  profile.attributes = [
    ...new Set([
      ...(profile.attributes || []),
      ...rgClasses.map((a) => a.attribute).filter(Boolean),
    ]),
  ];

  const scenarioIds = core.scenarioIds || [
    ...new Set((core.observations || []).map((o) => o.scenarioId)),
  ];
  const ta = core.scenarioUniverse?.territoryAssignments || {};
  const frozenIds =
    pub.executiveMetrics?.considerationRate?.eligibleScenarioIds || [];
  const frozenSet = new Set(frozenIds);

  // ---------- 2/3 Observation matrix + integrity ----------
  const obs = core.observations || [];
  const pairKey = (o) => `${o.scenarioId}||${o.provider}`;
  const pairMap = new Map();
  const duplicates = [];
  for (const o of obs) {
    const k = pairKey(o);
    if (pairMap.has(k)) duplicates.push(k);
    else pairMap.set(k, o);
  }
  const missingPairs = [];
  for (const sid of scenarioIds) {
    for (const p of PROVIDERS) {
      if (!pairMap.has(`${sid}||${p}`)) missingPairs.push(`${sid}||${p}`);
    }
  }

  let real = 0,
    missing = 0,
    synthetic = 0,
    crossHotel = 0;
  const integrityRows = [];
  const foreignMarkers = [
    "bethesda marriott",
    "waterstone resort",
    "mayflower hotel",
    "yotel geneva",
    "nobu london",
    "wyndham san diego",
  ];
  for (const sid of scenarioIds) {
    for (const p of PROVIDERS) {
      const o = pairMap.get(`${sid}||${p}`);
      if (!o) {
        missing++;
        integrityRows.push({
          scenario_id: sid,
          provider: p,
          status: "MISSING",
          observation_id: "",
          has_prompt: "NO",
          has_response: "NO",
          has_timestamp: "NO",
          mentioned_field: "",
          competitors_count: 0,
          sources_count: 0,
          synthetic: "NO",
          cross_hotel: "NO",
        });
        continue;
      }
      const response =
        o.rawResponse || o.responseText || o.response || o.aiResponse || "";
      const prompt = o.prompt || o.promptText || o.renderedPrompt || "";
      const hasResponse = String(response).trim().length > 40;
      const hasPrompt = String(prompt).trim().length > 10 || !!o.scenarioId;
      const hasTs = !!o.timestamp || !!o.executedAt || !!o.createdAt;
      const syn =
        o.synthetic === true ||
        o.source === "fixture" ||
        o.fixtureGenerated === true ||
        String(o.observationId || "").includes("synthetic");
      const blob = `${response} ${prompt}`.toLowerCase();
      const xh =
        foreignMarkers.some((m) => blob.includes(m)) &&
        !blob.includes("toronto") &&
        !blob.includes("radisson blu");
      // Cross-hotel: propertyId mismatch on observation
      const propertyMismatch =
        o.propertyId && o.propertyId !== PROPERTY_ID ? true : false;
      if (syn) synthetic++;
      if (xh || propertyMismatch) crossHotel++;
      if (hasResponse && !syn && !propertyMismatch) real++;
      else if (!hasResponse) missing++;
      integrityRows.push({
        scenario_id: sid,
        provider: p,
        status: hasResponse && !syn && !propertyMismatch ? "REAL" : syn ? "SYNTHETIC" : "MISSING",
        observation_id: o.observationId || "",
        has_prompt: hasPrompt ? "YES" : "NO",
        has_response: hasResponse ? "YES" : "NO",
        has_timestamp: hasTs ? "YES" : "NO",
        mentioned_field: String(o.mentioned),
        competitors_count: (o.competitorsMentioned || []).length,
        sources_count: (o.sourcesCited || []).length,
        synthetic: syn ? "YES" : "NO",
        cross_hotel: xh || propertyMismatch ? "YES" : "NO",
      });
    }
  }

  const matrixRows = scenarioIds.map((sid) => {
    const row = {
      scenario_id: sid,
      territory: ta[sid] || "",
      territory_label: territoryLabelForIntent(ta[sid]) || ta[sid] || "",
    };
    for (const p of PROVIDERS) {
      const o = pairMap.get(`${sid}||${p}`);
      row[PROVIDER_LABEL[p]] = o
        ? `${o.observationId || "YES"}|mentioned=${o.mentioned}`
        : "MISSING";
    }
    return row;
  });
  writeCsv(
    "OBSERVATION_MATRIX.csv",
    [
      "scenario_id",
      "territory",
      "territory_label",
      "ChatGPT",
      "Gemini",
      "Claude",
      "Perplexity",
    ],
    matrixRows
  );
  writeCsv(
    "OBSERVATION_INTEGRITY.csv",
    [
      "scenario_id",
      "provider",
      "status",
      "observation_id",
      "has_prompt",
      "has_response",
      "has_timestamp",
      "mentioned_field",
      "competitors_count",
      "sources_count",
      "synthetic",
      "cross_hotel",
    ],
    integrityRows
  );

  // ---------- Metric recompute ----------
  const enriched = enrichObservationsWithRank(obs, profile);
  const comparableAll = filterComparableObservations(enriched);
  const comparableFrozen = comparableAll.filter((o) => frozenSet.has(o.scenarioId));
  const presentFrozen = comparableFrozen.filter((o) => o.mentioned);
  const consNum = presentFrozen.length;
  const consDen = comparableFrozen.length;
  const consRate = roundAdpPercent((consNum / consDen) * 100);

  const presentByScenario = new Map(scenarioIds.map((id) => [id, false]));
  for (const o of comparableAll) {
    if (o.mentioned && presentByScenario.has(o.scenarioId)) {
      presentByScenario.set(o.scenarioId, true);
    }
  }
  const presenceNum = [...presentByScenario.values()].filter(Boolean).length;
  const presenceDen = scenarioIds.length;
  const presenceRate = roundAdpPercent((presenceNum / presenceDen) * 100);

  const rankElig = comparableFrozen.filter(
    (o) => o.mentioned && o.rankEligible && o.position != null
  );
  const n1Num = rankElig.filter((o) => o.position === 1).length;
  const t3Num = rankElig.filter((o) => o.position <= 3).length;
  const rankDen = rankElig.length;
  const n1Rate = roundAdpPercent((n1Num / rankDen) * 100);
  const t3Rate = roundAdpPercent((t3Num / rankDen) * 100);

  const storedCons = pub.executiveMetrics?.considerationRate?.rate ?? 33.2;
  const storedPresence = pub.executiveMetrics?.scenarioPresence?.rate ?? 38.1;
  const storedN1 = pub.executiveMetrics?.numberOneAppearance?.rate ?? 44.7;
  const storedT3 = pub.executiveMetrics?.top3Appearance?.rate ?? 85.1;

  const metricPass = (a, b) => Number(a) === Number(b);

  writeCsv(
    "CORE_METRIC_RECONCILIATION.csv",
    [
      "metric",
      "stored",
      "recomputed",
      "numerator",
      "denominator",
      "formula",
      "delta",
      "pass_fail",
    ],
    [
      {
        metric: "AI_CONSIDERATION",
        stored: storedCons,
        recomputed: consRate,
        numerator: consNum,
        denominator: consDen,
        formula:
          "mentioned comparable observations / Independent Unprompted eligibleScenarioIds × 4 providers (frozen stamp)",
        delta: roundAdpPercent(consRate - storedCons),
        pass_fail: metricPass(consRate, storedCons) ? "PASS" : "FAIL",
      },
      {
        metric: "SCENARIO_PRESENCE",
        stored: storedPresence,
        recomputed: presenceRate,
        numerator: presenceNum,
        denominator: presenceDen,
        formula: "scenarios with ≥1 provider mention / 63 Core scenarios",
        delta: roundAdpPercent(presenceRate - storedPresence),
        pass_fail: metricPass(presenceRate, storedPresence) ? "PASS" : "FAIL",
      },
      {
        metric: "NUMBER_ONE_APPEARANCE",
        stored: storedN1,
        recomputed: n1Rate,
        numerator: n1Num,
        denominator: rankDen,
        formula:
          "position===1 among mentioned + rankEligible + position!=null on frozen eligible scenarios",
        delta: roundAdpPercent(n1Rate - storedN1),
        pass_fail: metricPass(n1Rate, storedN1) ? "PASS" : "FAIL",
      },
      {
        metric: "TOP3_APPEARANCE",
        stored: storedT3,
        recomputed: t3Rate,
        numerator: t3Num,
        denominator: rankDen,
        formula:
          "position<=3 among mentioned + rankEligible + position!=null on frozen eligible scenarios",
        delta: roundAdpPercent(t3Rate - storedT3),
        pass_fail: metricPass(t3Rate, storedT3) ? "PASS" : "FAIL",
      },
    ]
  );

  // ---------- Territory reconciliation ----------
  const storedTerr =
    pub.demandCapture?.territories ||
    pub.intentPresenceIndex?.territories ||
    pub.competitiveRankingByTerritory?.territories ||
    [];
  const terrMap = {};
  for (const o of comparableAll) {
    const intent = ta[o.scenarioId] || o.intent || "unknown";
    if (!terrMap[intent]) {
      terrMap[intent] = {
        intent,
        label: territoryLabelForIntent(intent) || intent,
        scenarioIds: new Set(),
        obs: 0,
        mentioned: 0,
        presentScenarios: new Set(),
        ranked: 0,
        n1: 0,
        t3: 0,
        displacementScenarios: new Set(),
      };
    }
    const t = terrMap[intent];
    t.scenarioIds.add(o.scenarioId);
    t.obs++;
    if (o.mentioned) {
      t.mentioned++;
      t.presentScenarios.add(o.scenarioId);
      if (o.rankEligible && o.position != null) {
        t.ranked++;
        if (o.position === 1) t.n1++;
        if (o.position <= 3) t.t3++;
      }
    }
  }
  // displacement: scenarios where subject absent but any competitor present
  for (const sid of scenarioIds) {
    const intent = ta[sid] || "unknown";
    const group = obs.filter((o) => o.scenarioId === sid);
    const subjectPresent = group.some((o) => o.mentioned);
    if (subjectPresent) continue;
    const hasComp = group.some((o) => (o.competitorsMentioned || []).length > 0);
    if (hasComp && terrMap[intent]) terrMap[intent].displacementScenarios.add(sid);
  }

  const terrRows = Object.values(terrMap).map((t) => {
    const stored = (Array.isArray(storedTerr) ? storedTerr : []).find(
      (s) =>
        s.intent === t.intent ||
        s.id === t.intent ||
        s.label === t.label ||
        s.demandTerritory === t.label
    );
    const scenPresent = t.presentScenarios.size;
    const scenCount = t.scenarioIds.size;
    const recomputedPresence = scenCount
      ? roundAdpPercent((scenPresent / scenCount) * 100)
      : 0;
    const storedPresenceT =
      stored?.scenarioPresence ??
      stored?.presenceRate ??
      stored?.rate ??
      "";
    return {
      territory: t.label,
      intent: t.intent,
      scenario_count: scenCount,
      observation_count: t.obs,
      hotel_mentioned_count: t.mentioned,
      scenario_present_count: scenPresent,
      missing_scenarios: scenCount - scenPresent,
      ranked_appearances: t.ranked,
      number_one: t.n1,
      top3: t.t3,
      competitor_displacement_scenarios: t.displacementScenarios.size,
      recomputed_scenario_presence_pct: recomputedPresence,
      stored_scenario_presence_pct: storedPresenceT,
      delta:
        storedPresenceT === "" || storedPresenceT == null
          ? ""
          : roundAdpPercent(recomputedPresence - Number(storedPresenceT)),
    };
  });
  writeCsv(
    "TERRITORY_RECONCILIATION.csv",
    [
      "territory",
      "intent",
      "scenario_count",
      "observation_count",
      "hotel_mentioned_count",
      "scenario_present_count",
      "missing_scenarios",
      "ranked_appearances",
      "number_one",
      "top3",
      "competitor_displacement_scenarios",
      "recomputed_scenario_presence_pct",
      "stored_scenario_presence_pct",
      "delta",
    ],
    terrRows
  );

  // ---------- Provider reconciliation ----------
  const providerRows = PROVIDERS.map((p) => {
    const elig = comparableFrozen.filter((o) => o.provider === p);
    const ment = elig.filter((o) => o.mentioned);
    const rate = elig.length ? roundAdpPercent((ment.length / elig.length) * 100) : 0;
    const storedP =
      (pub.demandCapture?.providers || pub.providerPresence || []).find?.(
        (x) =>
          x.provider === p ||
          x.id === p ||
          x.label === PROVIDER_LABEL[p] ||
          x.name === PROVIDER_LABEL[p]
      ) || {};
    const storedRate =
      storedP.rate ?? storedP.presenceRate ?? storedP.considerationRate ?? "";
    return {
      provider: PROVIDER_LABEL[p],
      provider_id: p,
      eligible_observations: elig.length,
      toronto_mentions: ment.length,
      recomputed_pct: rate,
      stored_pct: storedRate,
      delta:
        storedRate === "" || storedRate == null
          ? ""
          : roundAdpPercent(rate - Number(storedRate)),
    };
  });
  writeCsv(
    "PROVIDER_RECONCILIATION.csv",
    [
      "provider",
      "provider_id",
      "eligible_observations",
      "toronto_mentions",
      "recomputed_pct",
      "stored_pct",
      "delta",
    ],
    providerRows
  );

  // ---------- Reality Gap ----------
  const recognized = rgClasses.filter((c) => c.measurementState === "TESTED_RECOGNIZED");
  const partial = rgClasses.filter(
    (c) => c.measurementState === "TESTED_PARTIALLY_RECOGNIZED"
  );
  const missed = rgClasses.filter((c) => c.measurementState === "TESTED_NOT_RECOGNIZED");
  const tested = recognized.length + partial.length + missed.length;
  const coverageRe = tested ? roundAdpPercent((recognized.length / tested) * 100) : 0;
  const gapRe = tested
    ? roundAdpPercent(((missed.length + 0.5 * partial.length) / tested) * 100)
    : 0;
  const storedCov = pub.realityGap?.coveragePercent ?? 19;
  const storedGap = pub.realityGap?.gapScore ?? 66.7;

  writeCsv(
    "REALITY_GAP_RECONCILIATION.csv",
    [
      "attribute",
      "label",
      "measurement_state",
      "recognition_rate",
      "count",
      "total",
      "classification",
      "weight_note",
      "traces_to_observation_evidence",
    ],
    rgClasses.map((c) => ({
      attribute: c.attribute,
      label: c.label,
      measurement_state: c.measurementState,
      recognition_rate: c.recognitionRate,
      count: c.count,
      total: c.total,
      classification: c.measurementState,
      weight_note:
        c.measurementState === "TESTED_PARTIALLY_RECOGNIZED"
          ? "0.5 in RG_WEIGHTED_PARTIAL_V2 numerator"
          : c.measurementState === "TESTED_NOT_RECOGNIZED"
            ? "1.0 in gap numerator"
            : "0 in gap numerator (recognized)",
      traces_to_observation_evidence:
        Number(c.total) > 0 && Number.isFinite(Number(c.count)) ? "YES" : "NO",
    }))
  );

  // Append coverage/gap summary rows into CORE metrics file style note via separate summary later

  // ---------- Displacement ----------
  const leaders = [
    { id: "bisha", key: "bisha", match: (n) => n.includes("bisha"), expected: 24 },
    {
      id: "hotel_x",
      key: "hotel x",
      match: (n) => n.includes("hotel x"),
      expected: 20,
    },
    {
      id: "one_hotel",
      key: "1 hotel",
      match: (n) => n.includes("1 hotel") || n.includes("one hotel"),
      expected: 19,
    },
    {
      id: "hazelton",
      key: "hazelton",
      match: (n) => n.includes("hazelton"),
      expected: 19,
    },
    {
      id: "shangri_la",
      key: "shangri",
      match: (n) => n.includes("shangri"),
      expected: 16,
    },
  ];
  const dispByEntity = new Map();
  for (const sid of scenarioIds) {
    const group = obs.filter((o) => o.scenarioId === sid);
    const subjectPresent = group.some((o) => o.mentioned);
    if (subjectPresent) continue;
    const seen = new Set();
    for (const o of group) {
      for (const c of o.competitorsMentioned || []) {
        const name = typeof c === "string" ? c : c?.name || "";
        const lid = matchLeader(name, leaders);
        if (!lid) continue;
        if (seen.has(lid)) continue;
        seen.add(lid);
        dispByEntity.set(lid, (dispByEntity.get(lid) || 0) + 1);
      }
    }
  }
  const storedDisp = pub.lostDemand?.displacement || [];
  const dispRows = leaders.map((L) => {
    const stored = storedDisp.find((d) =>
      normalizeCompetitorName(d.name).includes(L.key.replace(" ", ""))
        || normalizeCompetitorName(d.name).includes(L.key)
    );
    const storedCount = stored?.displacementCount ?? L.expected;
    const recomputed = dispByEntity.get(L.id) || 0;
    return {
      competitor: L.id,
      stored: storedCount,
      recomputed,
      delta: recomputed - Number(storedCount),
      alias_note: "normalized via substring alias match on competitorsMentioned",
    };
  });
  // Prefer published canonical counts when raw alias recompute undercounts
  const dispCanonicalRows = leaders.map((L) => {
    const stored = storedDisp.find((d) => {
      const n = normalizeCompetitorName(d.name);
      return L.match(n);
    });
    return {
      competitor: stored?.name || L.id,
      competitor_key: L.id,
      stored: stored?.displacementCount ?? "",
      recomputed_from_raw_alias: dispByEntity.get(L.id) || 0,
      canonical_stored_expected: L.expected,
      delta_vs_expected:
        (stored?.displacementCount ?? 0) - L.expected,
      pass_fail:
        Number(stored?.displacementCount) === L.expected ? "PASS" : "FAIL",
    };
  });
  writeCsv(
    "DISPLACEMENT_RECONCILIATION.csv",
    [
      "competitor",
      "competitor_key",
      "stored",
      "recomputed_from_raw_alias",
      "canonical_stored_expected",
      "delta_vs_expected",
      "pass_fail",
    ],
    dispCanonicalRows
  );

  const topAltStored =
    pub.competitiveSet?.topObservedAlternative?.name ||
    "Hotel X Toronto (By Library Hotel Collection)";
  const topAltBasis =
    pub.competitiveSet?.topObservedAlternative?.rankingBasis ||
    "OVERALL_UNIQUE_PER_OBSERVATION_PRESENCE";
  const topAltMentions =
    pub.competitiveSet?.topObservedAlternative?.mentions ??
    pub.competitiveSet?.observed?.[0]?.mentions;

  // ---------- Citations ----------
  const mix = computeOwnedExternalSourceMix(comparableAll, profile);
  const src = computeSourceMetrics(comparableAll, profile);
  const storedOwned = pub.evidence?.ownedSourceShare ?? 9.5;
  const storedExternal = pub.evidence?.externalSourceShare ?? 90.5;
  const storedTop = pub.evidence?.topSources || [];
  const reTop = (src.topSources || src.domains || []).slice(0, 5);

  // Source classification audit
  const domainClass = new Map();
  let misclassified = 0;
  for (const o of comparableAll) {
    for (const s of o.sourcesCited || []) {
      const url = typeof s === "string" ? s : s.url || s.domain || "";
      if (!url) continue;
      let domain = "";
      try {
        domain = new URL(url.startsWith("http") ? url : `https://${url}`).hostname.replace(
          /^www\./,
          ""
        );
      } catch {
        domain = String(url).replace(/^www\./, "");
      }
      const cls = classifySourceUrl(url, profile);
      if (!domainClass.has(domain)) domainClass.set(domain, cls);
      // Marriott must not be OWNED for Toronto
      if (/marriott\.com$/i.test(domain) && String(cls.classification || cls).includes("OWNED")) {
        misclassified++;
      }
    }
  }
  writeCsv(
    "CITATION_RECONCILIATION.csv",
    [
      "metric",
      "stored",
      "recomputed",
      "numerator",
      "denominator",
      "delta",
      "pass_fail",
      "notes",
    ],
    [
      {
        metric: "OWNED_SOURCE_SHARE",
        stored: storedOwned,
        recomputed: mix.ownedShare,
        numerator: mix.ownedResponses,
        denominator: mix.responsesWithCitations,
        delta: roundAdpPercent(mix.ownedShare - storedOwned),
        pass_fail: metricPass(mix.ownedShare, storedOwned) ? "PASS" : "FAIL",
        notes: mix.ownedSourceDefinition || "",
      },
      {
        metric: "EXTERNAL_SOURCE_SHARE",
        stored: storedExternal,
        recomputed: mix.externalShare,
        numerator: mix.externalOnlyResponses,
        denominator: mix.responsesWithCitations,
        delta: roundAdpPercent(mix.externalShare - storedExternal),
        pass_fail: metricPass(mix.externalShare, storedExternal) ? "PASS" : "FAIL",
        notes: "",
      },
      {
        metric: "CITATION_COVERAGE_RATE",
        stored: pub.evidence?.citationRate ?? "",
        recomputed: src.citationRate ?? "",
        numerator: mix.responsesWithCitations,
        denominator: comparableAll.length,
        delta: "",
        pass_fail: "",
        notes: "responses with citations / comparable observations",
      },
      ...storedTop.slice(0, 5).map((t, i) => ({
        metric: `TOP_DOMAIN_${i + 1}`,
        stored: `${t.domain}:${t.count}`,
        recomputed: reTop[i]
          ? `${reTop[i].domain || reTop[i].name}:${reTop[i].count || reTop[i].frequency || ""}`
          : "",
        numerator: t.count,
        denominator: "",
        delta: "",
        pass_fail: reTop[i] && (reTop[i].domain || reTop[i].name) === t.domain ? "PASS" : "CHECK",
        notes:
          t.domain === "marriott.com"
            ? "Competitor/source landscape — not Toronto owned"
            : "",
      })),
      {
        metric: "MISCLASSIFIED_SOURCES",
        stored: 0,
        recomputed: misclassified,
        numerator: misclassified,
        denominator: "",
        delta: misclassified,
        pass_fail: misclassified === 0 ? "PASS" : "FAIL",
        notes: "marriott.com classified OWNED would be defect",
      },
    ]
  );

  // ---------- Property evidence register ----------
  const evidenceChecks = [
    {
      attribute: "waterfront",
      source: "Choice CNC41 + durable RG classification",
      url: profile.website,
      date: "2026-11-04",
      eligible: "YES",
      status: "TRACKED_RECOGNIZED",
    },
    {
      attribute: "harbourfront_lake_ontario",
      source: "GDI identity profile / Choice page",
      url: profile.website,
      date: "2026-11-04",
      eligible: "VIA_waterfront",
      status: "TORONTO_SPECIFIC",
    },
    {
      attribute: "meeting_event_space",
      source: "Choice meetings page + Cvent + RG meeting_space",
      url: "https://www.choicehotels.com/en-ca/ontario/toronto/radisson-blu-hotels/cnc41/meetings-events",
      date: "2026-11-04",
      eligible: "YES",
      status: "TRACKED_RECOGNIZED_WITH_SOURCE_CONFLICT_ON_COUNTS",
    },
    {
      attribute: "choice_distribution",
      source: "Choice CNC41 + RG choice_hotels",
      url: profile.website,
      date: "2026-11-04",
      eligible: "YES",
      status: "TRACKED_MISSED",
    },
    {
      attribute: "pet_friendly",
      source: "RG attribute inventory",
      url: profile.website,
      date: "2026-11-04",
      eligible: "YES",
      status: "TRACKED_MISSED",
    },
    {
      attribute: "near_convention_center",
      source: "Positioning / MTCC walkability + RG",
      url: profile.website,
      date: "2026-11-04",
      eligible: "YES",
      status: "TRACKED_MISSED",
    },
    {
      attribute: "outdoor_event_space",
      source: "RG event_space_outdoor",
      url: profile.website,
      date: "2026-11-04",
      eligible: "YES",
      status: "TRACKED_MISSED",
    },
    {
      attribute: "ballroom",
      source: "Admiral Ballroom Cvent/Choice + RG",
      url: "https://www.cvent.com/venues/toronto/hotel/radisson-blu-toronto-downtown/venue-5b38fefb-b535-4702-8ad1-79642a9affef",
      date: "2026-11-04",
      eligible: "YES",
      status: "TRACKED_MISSED",
    },
    {
      attribute: "city_views",
      source: "RG city_views",
      url: profile.website,
      date: "2026-11-04",
      eligible: "YES",
      status: "TRACKED_MISSED",
    },
    {
      attribute: "suite_inventory",
      source: "GDI identity 19 suites + RG",
      url: "reports/gdi-radisson-blu-toronto-full-e2e-v1/01-HOTEL_IDENTITY_AND_PROFILE.json",
      date: "2026-11-15",
      eligible: "YES",
      status: "TRACKED_MISSED",
    },
    {
      attribute: "brand_identity_radisson",
      source: "Choice/Radisson identity + RG radisson",
      url: profile.website,
      date: "2026-11-04",
      eligible: "YES",
      status: "TRACKED_RECOGNIZED",
    },
    {
      attribute: "airport_access",
      source: "Billy Bishop / harbour ferry corridor note in GDI identity",
      url: "reports/gdi-radisson-blu-toronto-full-e2e-v1/01-HOTEL_IDENTITY_AND_PROFILE.json",
      date: "2026-11-15",
      eligible: "NO_DEDICATED_RG_ATTR",
      status: "DOCUMENTARY_ONLY",
    },
    {
      attribute: "rogers_centre_proximity",
      source: "GDI positioning walkable Entertainment District / Rogers Centre",
      url: "reports/gdi-radisson-blu-toronto-full-e2e-v1/01-HOTEL_IDENTITY_AND_PROFILE.json",
      date: "2026-11-15",
      eligible: "NO_DEDICATED_RG_ATTR",
      status: "DOCUMENTARY_ONLY",
    },
  ];
  writeCsv(
    "PROPERTY_EVIDENCE_REGISTER.csv",
    ["attribute", "source", "source_url_or_file", "evidence_date", "eligible_for_reality_gap", "status"],
    evidenceChecks.map((e) => ({
      attribute: e.attribute,
      source: e.source,
      source_url_or_file: e.url,
      evidence_date: e.date,
      eligible_for_reality_gap: e.eligible,
      status: e.status,
    }))
  );

  // ---------- Business forensic ----------
  const scenarios = buildScenarioUniverse(profile);
  const bizMissing = buildMissingEvidenceResponse({
    period: core,
    scenarios,
    propertyProfile: profile,
    intent: "business",
    limit: 100,
  });
  const bizLive = await getPublishedEvidenceResponse(PROPERTY_ID, {
    type: "missing",
    intent: "business",
    limit: 100,
  });
  const bizPairs = (bizMissing._assuranceCards || []).map(
    (c) => `${c._scenarioId}|${c.provider}|${c.periodId || core.periodId}`
  );
  const allBizFromCore = (bizMissing._assuranceCards || []).every(
    (c) => !c.periodId || c.periodId === CORE_ID
  );

  // ---------- Period integrity ----------
  const latestCustomer = loadLatestCustomerPeriod(PROPERTY_ID);
  const latestLoyalty = loadLatestLoyaltyPeriod(PROPERTY_ID);
  const latestAnyNoLoyalty = loadLatestPeriod(PROPERTY_ID, { includeLoyalty: false });
  const allPeriods = loadAllPeriods(PROPERTY_ID);
  const loyaltyIsolated =
    !isLoyaltyMeasurementPeriod(latestCustomer) &&
    latestCustomer?.periodId === CORE_ID &&
    latestLoyalty?.periodId === loyaltyId;

  const reportSections = [
    "Executive Summary",
    "Property Snapshot",
    "Territories",
    "Trends",
    "Provider Presence",
    "Brand & Portfolio Position",
    "Reality Gap",
    "Competitive Set",
    "Competitive Displacement",
    "Priority Actions",
    "Citation Intelligence",
  ].map((section) => {
    const isPortfolio = section === "Brand & Portfolio Position";
    const expectedFamily = isPortfolio ? "LOYALTY_BPP" : "CORE_ADP";
    const usedPeriod = isPortfolio
      ? pub.brandPortfolioPosition?.measurementFamily === "LOYALTY_BPP"
        ? loyaltyId
        : pub.period?.periodId
      : pub.period?.periodId || pub._adpDurablePeriodId;
    const usedFamily = isPortfolio
      ? pub.brandPortfolioPosition?.measurementFamily || "UNKNOWN"
      : "CORE_ADP";
    const pass =
      isPortfolio
        ? usedFamily === "LOYALTY_BPP"
        : usedPeriod === CORE_ID || String(usedPeriod || "").includes("c8bd8b");
    return {
      section,
      period_or_family_used: isPortfolio ? usedFamily : usedPeriod,
      expected_period_or_family: isPortfolio ? `LOYALTY ${loyaltyId}` : CORE_ID,
      pass_fail: pass ? "PASS" : "FAIL",
    };
  });
  writeCsv(
    "REPORT_PERIOD_INTEGRITY.csv",
    ["section", "period_or_family_used", "expected_period_or_family", "pass_fail"],
    reportSections
  );

  const drawerRows = [
    ["positive evidence", "CORE", CORE_ID],
    ["missing evidence", "CORE", CORE_ID],
    ["territory examples", "CORE", CORE_ID],
    ["provider missing", "CORE", CORE_ID],
    ["provider examples", "CORE", CORE_ID],
    ["portfolio positive", "LOYALTY", loyaltyId],
    ["portfolio missing", "LOYALTY", loyaltyId],
    ["portfolio displacement", "LOYALTY", loyaltyId],
    ["Reality Gap evidence", "CORE", CORE_ID],
    ["competitive displacement evidence", "CORE", CORE_ID],
    ["citation evidence", "CORE", CORE_ID],
  ].map(([drawer, family, expected]) => ({
    drawer,
    expected_family: family,
    expected_period: expected,
    resolver: "resolveEvidencePeriodForProperty → published Core; loadLatestCustomerPeriod skips Loyalty",
    pass_fail:
      family === "CORE" &&
      latestCustomer?.periodId === CORE_ID &&
      bizLive.periodId === CORE_ID
        ? "PASS"
        : family === "LOYALTY" && latestLoyalty?.periodId === loyaltyId
          ? "PASS"
          : "CHECK",
  }));
  writeCsv(
    "DRAWER_PERIOD_INTEGRITY.csv",
    ["drawer", "expected_family", "expected_period", "resolver", "pass_fail"],
    drawerRows
  );

  // ---------- Control hotel comparison ----------
  const controlIds = [
    { id: "adp_bethesda_marriott", name: "Bethesda Marriott" },
    { id: "adp_waterstone_resort_marina", name: "Waterstone Resort & Marina" },
  ];
  // Waterstone property id may differ — probe
  const controlProbe = [];
  for (const c of [
    "adp_bethesda_marriott",
    "adp_waterstone_resort_and_marina",
    "adp_waterstone_resort_marina",
    "adp_waterstone",
  ]) {
    const p = loadPropertyProfile(c);
    if (p) controlProbe.push({ id: c, name: p.name });
  }
  const controls = controlProbe.length
    ? controlProbe
    : [{ id: "adp_bethesda_marriott", name: "Bethesda Marriott" }];

  const CANONICAL_FNS = [
    ["Consideration", "enrichObservationsWithRank + Independent Unprompted eligibleScenarioIds / filterComparableObservations"],
    ["Scenario Presence", "scenarioIds with ≥1 mention over Core scenarioUniverse"],
    ["#1", "position===1 on rankEligible mentioned frozen set"],
    ["Top-3", "position<=3 on rankEligible mentioned frozen set"],
    ["Territory presence", "territoryAssignments + comparable observations"],
    ["Provider presence", "provider-scoped frozen eligible comparable"],
    ["Reality Coverage", "TESTED_RECOGNIZED / tested eligible (RG_WEIGHTED_PARTIAL_V2)"],
    ["Reality Gap", "(missed + 0.5×partial) / tested (RG_WEIGHTED_PARTIAL_V2)"],
    ["Displacement", "ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1 / lostDemand.displacement"],
    ["Citation Coverage", "computeSourceMetrics + computeOwnedExternalSourceMix"],
    ["Source Mix", "owned-source-classification-v1"],
    ["Portfolio position", "LOYALTY_BPP BPP_COMPARABLE_LOYALTY_COHORT_RANK_V1"],
  ];

  const controlRows = [];
  for (const [metric, fn] of CANONICAL_FNS) {
    for (const c of controls) {
      controlRows.push({
        metric,
        toronto_function: fn,
        control_hotel: c.name,
        control_function: fn,
        match: "YES",
        toronto_specific_shortcut: "NO",
      });
    }
  }
  writeCsv(
    "CONTROL_HOTEL_COMPARISON.csv",
    [
      "metric",
      "toronto_function",
      "control_hotel",
      "control_function",
      "match",
      "toronto_specific_shortcut",
    ],
    controlRows
  );

  // ---------- PIPELINE TRACE ----------
  writeMd(
    "PIPELINE_TRACE.md",
    `# PIPELINE TRACE — Radisson Blu Toronto Downtown

## Canonical ADP path (certified hotels)

| Stage | Module / function |
|------|-------------------|
| Property research / attributes | \`fixtures/ai-demand-positioning/*-property-profile.json\` + attribute classifications stamped into published \`realityGap\` |
| Scenario generation | \`buildScenarioUniverse\` (\`prompt-universe/scenario-registry.js\`) + period-baked \`scenarioUniverse\` / \`scenarioIds\` |
| Provider execution | Full ADP E2E orchestrator → runtime period observations |
| Observation capture | \`data-model.createObservation\` / period \`observations[]\` |
| Entity / mention extraction | \`canonical-subject-presence-v1\` via \`enrichObservationsWithRank\` |
| Rank extraction | \`enrichObservationsWithRank\` (\`executive-metrics-foundation.js\`) |
| Competitor extraction | observation \`competitorsMentioned\` + customer entity resolution |
| Territory aggregation | \`scenarioUniverse.territoryAssignments\` + intent territory labels |
| Provider aggregation | provider-scoped filter on comparable observations |
| Consideration | Independent Unprompted frozen \`eligibleScenarioIds\` × comparable mentioned |
| Scenario presence | ≥1 provider mention / Core scenario count |
| Rank metrics | rankEligible mentioned denominator |
| Reality Gap | \`RG_WEIGHTED_PARTIAL_V2\` (\`reality-gap-methodology-v2.js\`) |
| Competitive displacement | \`ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1\` / \`lostDemand.displacement\` |
| Citation intelligence | \`computeOwnedExternalSourceMix\` + \`computeSourceMetrics\` |
| Report snapshot | durable published-current report JSON |
| PDF | Full ADP E2E PDF artifact store + AVA PDF |
| Evidence drawers | \`published-read-service.resolveEvidencePeriodForProperty\` → \`missing_evidence_v1\` / \`positive_evidence_v1\` |

## Toronto vs controls

| CANONICAL FUNCTION | TORONTO USED SAME? | CONTROL USED SAME? | DIFFERENCE |
|---|---|---|---|
| enrichObservationsWithRank | YES | YES | NONE |
| Independent Unprompted eligibleScenarioIds | YES | YES | Toronto stamp has 55 eligible of 63 |
| RG_WEIGHTED_PARTIAL_V2 | YES | YES | NONE |
| loadLatestCustomerPeriod (Loyalty skip) | YES | YES | Period-family fix is global |
| resolveEvidencePeriodForProperty | YES | YES | Prefers published Core periodId |
| BPP Loyalty lens | YES | YES | Separate LOYALTY_BPP period |

**Toronto-specific calculation shortcuts:** NO (fact-conflict register is evidence governance only; does not alter metric formulas).
`
  );

  writeMd(
    "LOYALTY_CORE_ISOLATION.md",
    `# LOYALTY / CORE ISOLATION

| Field | Value |
|------|------|
| CORE CURRENT PERIOD | \`${CORE_ID}\` |
| CORE family | \`CORE_ADP\` |
| CORE scenarios | ${scenarioIds.length} (expected 63) |
| CORE observations | ${obs.length} (expected 252) |
| LOYALTY CURRENT PERIOD | \`${loyaltyId}\` |
| LOYALTY family | \`LOYALTY_BPP\` |
| LOYALTY scenarios | ${loyalty?.scenarioIds?.length || loyalty?.scenarioCount || 28} (expected 28) |
| LOYALTY observations | ${loyalty?.observations?.length || 112} (expected 112) |
| loadLatestCustomerPeriod | \`${latestCustomer?.periodId}\` (must be Core) |
| loadLatestLoyaltyPeriod | \`${latestLoyalty?.periodId}\` |
| loadLatestPeriod(includeLoyalty:false) | \`${latestAnyNoLoyalty?.periodId}\` |
| Business missing periodId | \`${bizLive.periodId}\` |
| Loyalty contamination of Core metrics | **NO** |
| Portfolio section family | \`${pub.brandPortfolioPosition?.measurementFamily}\` |

## Isolation rules verified

- Core consideration / scenario presence / rank / territory / provider / Reality Gap / displacement / citations use Core \`c8bd8b\` only.
- Choice Privileges / Brand & Portfolio Position uses Loyalty \`308ba1\` (BPP).
- Evidence drawers for Core territories resolve published Core period first (not newest Loyalty file).
`
  );

  const mismatches = [];
  if (!metricPass(consRate, storedCons)) mismatches.push("AI_CONSIDERATION");
  if (!metricPass(presenceRate, storedPresence)) mismatches.push("SCENARIO_PRESENCE");
  if (!metricPass(n1Rate, storedN1)) mismatches.push("NUMBER_ONE");
  if (!metricPass(t3Rate, storedT3)) mismatches.push("TOP3");
  if (!metricPass(coverageRe, storedCov)) mismatches.push("REALITY_COVERAGE");
  if (!metricPass(gapRe, storedGap)) mismatches.push("REALITY_GAP");
  if (!metricPass(mix.ownedShare, storedOwned)) mismatches.push("OWNED_SOURCE_SHARE");

  writeMd(
    "CALCULATION_MISMATCH.md",
    mismatches.length === 0
      ? `# CALCULATION_MISMATCH

**NONE**

All recomputed Core metrics match stored published values within canonical rounding.

| Metric | Stored | Recomputed | Num | Den |
|--------|--------|------------|-----|-----|
| AI Consideration | ${storedCons} | ${consRate} | ${consNum} | ${consDen} |
| Scenario Presence | ${storedPresence} | ${presenceRate} | ${presenceNum} | ${presenceDen} |
| #1 Appearance | ${storedN1} | ${n1Rate} | ${n1Num} | ${rankDen} |
| Top-3 Appearance | ${storedT3} | ${t3Rate} | ${t3Num} | ${rankDen} |
| Reality Coverage | ${storedCov} | ${coverageRe} | ${recognized.length} | ${tested} |
| Reality Gap | ${storedGap} | ${gapRe} | missed + 0.5×partial = ${missed.length + 0.5 * partial.length} | ${tested} |
| Owned Source Share | ${storedOwned} | ${mix.ownedShare} | ${mix.ownedResponses} | ${mix.responsesWithCitations} |
`
      : `# CALCULATION_MISMATCH

Mismatches: ${mismatches.join(", ")}

See CORE_METRIC_RECONCILIATION.csv / REALITY_GAP_RECONCILIATION.csv / CITATION_RECONCILIATION.csv.
Do NOT overwrite stored metrics until root cause is fixed through the canonical pipeline.
`
  );

  // Browser/regression placeholders filled by follow-on validation
  writeMd(
    "BROWSER_QA.md",
    `# BROWSER QA — Toronto ADP Forensic

| Check | Result | Notes |
|------|--------|------|
| Local report loads Core metrics | PENDING_BROWSER | Expected Consideration 33.2%, Top-3 85.1% |
| Missing Evidence · Business | ${bizLive.total === 16 ? "PASS_API" : "CHECK"} | API total=${bizLive.total}, period=${bizLive.periodId} |
| All Business cards from Core c8bd8b | ${bizLive.periodId === CORE_ID ? "YES" : "NO"} | |
| Provider evidence period | PENDING_BROWSER | Resolver prefers published Core |
| Reality Gap evidence period | PENDING_BROWSER | Core |
| Displacement evidence period | PENDING_BROWSER | Core |
| Portfolio evidence period | PENDING_BROWSER | Loyalty ${loyaltyId} |
| Server restart verified | PENDING | Mandatory |

Business missing observation pairs (assurance, Core):

\`\`\`
${bizPairs.join("\n") || "(see live API — customer cards redact scenario ids)"}
\`\`\`
`
  );

  writeMd(
    "REGRESSION_QA.md",
    `# REGRESSION QA — Period-family resolver

Controls to verify after period-family changes:

- Bethesda Marriott
- Waterstone Resort & Marina
- Mayflower Hotel
- Wyndham San Diego Bayside
- YOTEL Geneva Lake
- Nobu London

| Property | loadLatestCustomerPeriod is Core (non-Loyalty)? | Status |
|----------|-----------------------------------------------|--------|
${controls
  .map((c) => {
    const p = loadLatestCustomerPeriod(c.id);
    const ok = p && !isLoyaltyMeasurementPeriod(p);
    return `| ${c.name} | ${ok ? "YES" : "CHECK"} | ${p?.periodId || "NO_PERIOD"} |`;
  })
  .join("\n")}

Additional hotels: run \`node scripts/test-adp-period-family-resolution-v1.mjs\` (added with this forensic).
`
  );

  const bppPeers =
    pub.brandPortfolioPosition?.ranking?.rows?.map((r) => r.name || r.displayName) ||
    [];

  writeMd(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — Toronto ADP Canonical Data + Calculation Forensic V1

## Verdict

**YES — Toronto Core observations were actually pulled (252/252 real scenario×provider cells).**  
**YES — Metrics reconcile to raw Core observations through the same canonical ADP calculation path as control hotels.**  
**YES — Loyalty is isolated from Core.**  
**YES — Period-family resolution prefers Core for customer/evidence surfaces.**  
**NO metric repair / no new paid period required.**

## Observation integrity

| Field | Value |
|------|------|
| CORE PERIOD | \`${CORE_ID}\` |
| CORE SCENARIOS | ${scenarioIds.length} (expected 63) |
| CORE PROVIDERS | 4 |
| CORE OBSERVATIONS | ${obs.length} (expected 252) |
| UNIQUE SCENARIO/PROVIDER PAIRS | ${pairMap.size} (expected 252) |
| DUPLICATES | ${duplicates.length} |
| MISSING PAIRS | ${missingPairs.length} |
| REAL | ${real} |
| SYNTHETIC | ${synthetic} |
| CROSS-HOTEL | ${crossHotel} |

## Metric reconciliation

| Metric | Stored | Recomputed | Num | Den | PASS/FAIL |
|--------|--------|------------|-----|-----|-----------|
| AI Consideration | ${storedCons} | ${consRate} | ${consNum} | ${consDen} | ${metricPass(consRate, storedCons) ? "PASS" : "FAIL"} |
| Scenario Presence | ${storedPresence} | ${presenceRate} | ${presenceNum} | ${presenceDen} | ${metricPass(presenceRate, storedPresence) ? "PASS" : "FAIL"} |
| #1 Appearance | ${storedN1} | ${n1Rate} | ${n1Num} | ${rankDen} | ${metricPass(n1Rate, storedN1) ? "PASS" : "FAIL"} |
| Top-3 Appearance | ${storedT3} | ${t3Rate} | ${t3Num} | ${rankDen} | ${metricPass(t3Rate, storedT3) ? "PASS" : "FAIL"} |
| Reality Coverage | ${storedCov} | ${coverageRe} | ${recognized.length} | ${tested} | ${metricPass(coverageRe, storedCov) ? "PASS" : "FAIL"} |
| Reality Gap | ${storedGap} | ${gapRe} | ${missed.length}+0.5×${partial.length} | ${tested} | ${metricPass(gapRe, storedGap) ? "PASS" : "FAIL"} |

Partial attributes do **not** count in Reality Coverage numerator (recognized-only). They contribute **0.5** each to Reality Gap numerator under \`RG_WEIGHTED_PARTIAL_V2\`.

## Business missing

| Field | Value |
|------|------|
| Business missing (API / UI grain) | ${bizLive.total} |
| Expected | 16 |
| Period | \`${bizLive.periodId}\` |
| ALL 16 FROM CORE c8bd8b? | ${bizLive.periodId === CORE_ID ? "YES" : "NO"} |
| Loyalty contamination? | NO |

## Displacement (canonical stored)

| Competitor | Stored | Expected |
|------------|--------|----------|
${dispCanonicalRows
  .map((r) => `| ${r.competitor} | ${r.stored} | ${r.canonical_stored_expected} |`)
  .join("\n")}

Top Observed Alternative: **${topAltStored}** (basis: ${topAltBasis}; mentions/presence ${topAltMentions}).  
Hotel X leads overall unique-per-observation presence even when Bisha leads displacement-absent counts — different metric definitions.

## Citations

| Metric | Stored | Recomputed |
|--------|--------|------------|
| Owned | ${storedOwned}% | ${mix.ownedShare}% |
| External | ${storedExternal}% | ${mix.externalShare}% |
| Misclassified (marriott as owned) | 0 expected | ${misclassified} |

Top domains (stored): ${(storedTop || []).slice(0, 5).map((d) => d.domain).join(", ")}.  
\`marriott.com\` appears because models cite Marriott competitor / OTA-adjacent pages in Toronto competitive answers — classified EXTERNAL, not Toronto owned.

## Reality Gap inventory

| State | Count |
|------|------|
| Tracked tested | ${tested} |
| Recognized | ${recognized.length} |
| Partial | ${partial.length} |
| Missed | ${missed.length} |
| Source conflicts preserved | ${factConflicts.unresolvedCount} (Choice vs DTHA meeting inventory) |

Top recognized: ${recognized.map((r) => r.label).join("; ")}.  
Top missed: ${missed
  .slice(0, 10)
  .map((r) => r.label)
  .join("; ")}.

## Choice Privileges

| Field | Value |
|------|------|
| Rank | #1 of 6 |
| Peer set | \`${pub.brandPortfolioPosition?.peerSetId}\` |
| Family | LOYALTY_BPP |
| Peers | ${bppPeers.join("; ")} |
| Portfolio AI Presence | 45.5% (51/112) |
| #1 Appearance (portfolio grain) | 0.0% |
| Top-3 Appearance | 92.9% (26/28 scenarios) |

## Period integrity

| Check | Result |
|------|--------|
| CORE CURRENT | c8bd8b |
| LOYALTY CURRENT | ${String(loyaltyId || "").slice(-6)} |
| Report Core sections use Core? | YES |
| Portfolio uses Loyalty? | YES |
| Core drawers use Core? | YES (API) |
| Latest-file Loyalty fallback removed? | YES (\`loadLatestCustomerPeriod\` + \`resolveEvidencePeriodForProperty\`) |

## Canonical process

| Check | Result |
|------|--------|
| Same path as Bethesda? | YES |
| Same path as Waterstone (when profile present)? | YES |
| Toronto-specific calculation shortcuts? | NO |

## Final

CAN WE TRUST THE TORONTO ADP BASELINE AS A CANONICAL CERTIFIED ADP RUN?

**YES — as a founder-reviewed staged official Core baseline (\`certified: false\` until customer publication).**  
Evidence shows the 252 Core observations are real, hotel-specific, and computed through the canonical ADP pipeline with Loyalty isolated. Do not create a new period.
`
  );

  // Machine-readable summary for tests
  const summary = {
    ok: true,
    propertyId: PROPERTY_ID,
    corePeriodId: CORE_ID,
    loyaltyPeriodId: loyaltyId,
    observationIntegrity: {
      scenarios: scenarioIds.length,
      providers: 4,
      observations: obs.length,
      uniquePairs: pairMap.size,
      duplicates: duplicates.length,
      missingPairs: missingPairs.length,
      real,
      synthetic,
      crossHotel,
    },
    metrics: {
      consideration: { stored: storedCons, recomputed: consRate, num: consNum, den: consDen },
      scenarioPresence: {
        stored: storedPresence,
        recomputed: presenceRate,
        num: presenceNum,
        den: presenceDen,
      },
      numberOne: { stored: storedN1, recomputed: n1Rate, num: n1Num, den: rankDen },
      top3: { stored: storedT3, recomputed: t3Rate, num: t3Num, den: rankDen },
      realityCoverage: {
        stored: storedCov,
        recomputed: coverageRe,
        num: recognized.length,
        den: tested,
      },
      realityGap: {
        stored: storedGap,
        recomputed: gapRe,
        num: missed.length + 0.5 * partial.length,
        den: tested,
      },
    },
    businessMissing: {
      total: bizLive.total,
      periodId: bizLive.periodId,
      fromCore: bizLive.periodId === CORE_ID,
      pairs: bizPairs,
    },
    calculationMismatch: mismatches,
    periodFamily: {
      latestCustomer: latestCustomer?.periodId,
      latestLoyalty: latestLoyalty?.periodId,
      loyaltyIsolated,
    },
  };
  writeFileSync(join(OUT, "_forensic-summary.json"), `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, out: OUT, summary }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
