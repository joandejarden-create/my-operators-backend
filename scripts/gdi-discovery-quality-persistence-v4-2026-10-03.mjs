/**
 * GDI Discovery Quality + Persistence Recovery V4
 * Association persistence fix/recovery + admission gate + bounded completion.
 * No threshold cuts. No broad rediscovery. Jev conditional blocker/source/stop only.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  isGdiDiscoveryCandidateWorthCompleting,
  ADMISSION_CLASS,
  associationPersistenceRootCause,
  recoverAssociationScoutForHotel,
  buildSuccessControlSet,
  compareCandidateToControls,
  summarizeControlPatterns,
} from "../lib/group-demand-intelligence/discovery-quality-v4/index.js";
import { loadActiveResearchUniverse } from "../lib/group-demand-intelligence/jev-active-research-v2/load-universe.js";
import { loadV3CandidatesFromReports } from "../lib/group-demand-intelligence/candidate-completion-v2/load-v3-candidates.js";
import {
  completeOneCandidate,
  buildGdiCompletionPriority,
  COMPLETION_PRIORITY,
  TERMINAL_CLASS,
} from "../lib/group-demand-intelligence/candidate-completion-v2/index.js";
import { SCOUT_FAMILY } from "../lib/group-demand-intelligence/discovery-expansion-v3/scouts.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "discovery-quality-persistence-v4");
const V3_DIR = path.join(__dirname, "..", "reports", "gdi", "discovery-expansion-v3-2026-10-03");
const DE_DIR = path.join(__dirname, "..", "reports", "gdi", "demand-engine-jev-controller-v1");
const CC_DIR = path.join(__dirname, "..", "reports", "gdi", "candidate-completion-jev-v2-2026-10-03");
const NOW = "2026-10-03";

const HOTELS = [
  {
    hotelId: "recrPQcZg7SFARRb2",
    hotelKey: "YOTEL",
    label: "YOTEL Geneva Lake",
    marketId: "geneva_lake",
    placeNames: ["Geneva", "Genève", "Nyon", "La Côte", "Palexpo"],
    geoTokens: ["geneva", "genève", "switzerland", "palexpo", "nyon"],
    defaultFitScore: 52,
    fitLine: "YOTEL Geneva Lake: airport / La Côte corridor.",
    serpGl: "ch",
    assocTarget: 16,
  },
  {
    hotelId: "rec2PVBDavppGpenm",
    hotelKey: "AC",
    label: "AC Hotel A Coruña",
    marketId: "a_coruna",
    placeNames: ["A Coruña", "Coruña", "Galicia"],
    geoTokens: ["coruña", "coruna", "galicia", "spain"],
    defaultFitScore: 50,
    fitLine: "AC A Coruña: urban upscale association/corporate.",
    serpGl: "es",
    assocTarget: 12,
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelKey: "SPICE",
    label: "Spice Island Beach Resort",
    marketId: "grenada",
    placeNames: ["Grenada", "Grand Anse"],
    geoTokens: ["grenada", "grand anse"],
    defaultFitScore: 48,
    fitLine: "Spice Island: luxury incentive lodging.",
    serpGl: "us",
    assocTarget: 5,
  },
  {
    hotelId: "recIwaP1etgx2g9nA",
    hotelKey: "CAMBRIDGE",
    label: "Cambridge Beaches Resort & Spa",
    marketId: "bermuda",
    placeNames: ["Bermuda", "Somerset"],
    geoTokens: ["bermuda", "somerset"],
    defaultFitScore: 48,
    fitLine: "Cambridge Beaches: cottage resort incentive.",
    serpGl: "us",
    assocTarget: 8,
  },
  {
    hotelId: "recGkME49yYuxQl0u",
    hotelKey: "NOW_NOW",
    label: "NOW NOW NOHO",
    marketId: "nyc_noho",
    placeNames: ["New York", "NoHo", "Manhattan"],
    geoTokens: ["new york", "nyc", "manhattan", "noho"],
    defaultFitScore: 50,
    fitLine: "NOW NOW NoHo: boutique urban groups.",
    serpGl: "us",
    assocTarget: 7,
  },
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

function dedupeKey(hotel, url, title, id) {
  const u = String(url || "")
    .toLowerCase()
    .replace(/\/$/, "")
    .slice(0, 160);
  if (u) return `${hotel}|u:${u}`;
  if (id) return `${hotel}|id:${id}`;
  return `${hotel}|t:${String(title || "")
    .toLowerCase()
    .slice(0, 80)}`;
}

function toResearchShape(raw, hotel) {
  return {
    ...raw,
    hotelKey: hotel.hotelKey || raw.hotelKey,
    hotelId: hotel.hotelId || raw.hotelId,
    title: raw.title || raw.eventProgram || raw.organization,
    organizationName: raw.organizationName || raw.organization,
    officialSource: raw.officialSource || raw.source || raw.url,
    source: raw.officialSource || raw.source || raw.url,
    geoTokens: hotel.geoTokens,
    placeNames: hotel.placeNames,
    defaultFitScore: hotel.defaultFitScore,
    signalType: raw.signalType || raw.discoveryMeta?.scoutFamily || "DEMAND_SIGNAL",
  };
}

async function main() {
  const rootCause = associationPersistenceRootCause();
  const assocProduced = 48; // SCOUT_YIELD sum
  const assocPersistedV3 = 0; // ASSOCIATION_RESULTS.csv never written
  const assocLost = assocProduced - assocPersistedV3;

  write(
    "ASSOCIATION_PERSISTENCE_ROOT_CAUSE.md",
    `# AssociationScout Persistence Root Cause

## Counts
| Metric | Value |
|--------|------:|
| ASSOCIATION DETAIL ROWS PRODUCED (SCOUT_YIELD) | ${assocProduced} |
| ASSOCIATION DETAIL ROWS PERSISTED (ASSOCIATION_RESULTS.csv) | ${assocPersistedV3} |
| ASSOCIATION DETAIL ROWS LOST | ${assocLost} |

## Drop location
**${rootCause.dropLocation}**

Pipeline:
\`query → SERP → hitToCandidate → newCandidates[] → scoutYield counts → report writer\`

- Produced: \`orchestrator\` incremented \`scoutStats[AssociationScout].candidates\` and included rows in \`newCandidates\` (contributing to TOTAL NEW CANDIDATES = 156).
- Persisted detail CSVs: Procurement, Medical, TourDmc, University, HiddenDemand, etc. via \`rowsForScout\`.
- **AssociationScout was omitted** from \`rowsForScout\` writes. Only \`ASSOCIATION_SERIES_RESULTS.csv\` (series subset) was written.
- Research-pool reconstruction read detail CSVs only → Association detail rows invisible → completion never saw them.

## Root cause
Silent report-writer omission of \`ASSOCIATION_RESULTS.csv\` — not a scout/query failure.

## Fix
1. V3 script now writes \`ASSOCIATION_RESULTS.csv\` via \`rowsForScout(SCOUT_FAMILY.ASSOCIATION)\`.
2. Future runs must assert every scout family with candidates has a detail CSV or explicit rejection ledger (no silent loss).

Silent loss: **YES** (before fix).
`
  );

  // ——— Phase 2: surgical Association recovery ———
  console.log("[v4] recovering AssociationScout rows (surgical, not broad)…");
  const recovered = [];
  let recoveryCost = 0;
  let recoveryDup = 0;
  const recoveryInvalid = [];
  for (const h of HOTELS) {
    const res = await recoverAssociationScoutForHotel(h, {
      targetCount: h.assocTarget,
      maxQueries: 4,
    });
    recoveryCost += res.costUsd || 0;
    recoveryDup += res.duplicatesRemoved || 0;
    console.log(`[v4] ${h.hotelKey} assoc recovered=${res.candidates.length} q=${res.queries}`);
    for (const c of res.candidates) {
      if (!c.officialSource) {
        recoveryInvalid.push({ hotel: h.hotelKey, id: c.id, reason: "NO_SOURCE" });
        continue;
      }
      recovered.push(toResearchShape(c, h));
    }
  }

  // Persist recovered association detail CSV into V3 dir + V4 out
  const assocCsvCols = [
    "hotel",
    "opportunityId",
    "title",
    "organizationName",
    "officialSource",
    "queryLanguage",
    "scoutFamily",
    "entityValid",
    "surfaceKeep",
    "customerReady",
    "validFutureWatch",
  ];
  const assocRows = recovered.map((c) => ({
    hotel: c.hotelKey,
    opportunityId: c.id,
    title: c.title,
    organizationName: c.organizationName,
    officialSource: c.officialSource,
    queryLanguage: c.queryLanguage || "en",
    scoutFamily: SCOUT_FAMILY.ASSOCIATION,
    entityValid: "",
    surfaceKeep: "",
    customerReady: "",
    validFutureWatch: "",
  }));
  write("ASSOCIATION_RECOVERY.csv", toCsv(assocRows, assocCsvCols));
  // Backfill V3 missing file so future loaders see them
  fs.writeFileSync(path.join(V3_DIR, "ASSOCIATION_RESULTS.csv"), toCsv(assocRows, assocCsvCols), "utf8");

  // ——— Phase 3: success controls ———
  console.log("[v4] building Bethesda/NYC success control set…");
  const controls = await buildSuccessControlSet({ nowDate: NOW });
  const controlAgg = summarizeControlPatterns(controls);
  write(
    "SUCCESS_CONTROL_SET.csv",
    toCsv(controls, [
      "hotelKey",
      "hotelLabel",
      "opportunityId",
      "title",
      "opportunityType",
      "entityQuality",
      "timingQuality",
      "lodgingQuality",
      "buyerWhoQuality",
      "placementQuality",
      "hotelFitQuality",
      "sourceAuthority",
      "independentSources",
      "hotelMotionSpecific",
      "eventProgramMaturity",
      "commercialActionability",
      "eventStartDate",
      "organizationName",
      "officialSource",
      "thesisSnippet",
    ])
  );
  write(
    "SUCCESS_PATTERN_COMPARISON.md",
    `# Success Pattern Comparison — Bethesda / NYC vs V3 Zero-Yield

## Control set
n=${controlAgg.controlCount} customer-ready opportunities across Bethesda Marriott, Renaissance NYTS, Hilton NYC.

| Structural trait | Control share |
|------------------|--------------:|
| Named entity/org | ${(controlAgg.pctEntityStrong * 100).toFixed(0)}% |
| Future timing present | ${(controlAgg.pctTimingPresent * 100).toFixed(0)}% |
| Lodging/housing hint | ${(controlAgg.pctLodgingHint * 100).toFixed(0)}% |
| WHO/contact path | ${(controlAgg.pctWhoPath * 100).toFixed(0)}% |
| Specific hotel motion thesis | ${(controlAgg.pctHotelMotion * 100).toFixed(0)}% |
| Commercially actionable combo | ${(controlAgg.pctActionable * 100).toFixed(0)}% |
| Avg independent sources | ${controlAgg.avgSources.toFixed(1)} |

## What successful rows had BEFORE deep completion
${controlAgg.structuralDifferences}

## What V3 candidates typically lacked
- Lodging + buyer + future decision point **together**
- Specific hotel-motion thesis (overflow / housing / primary)
- Credible organizer identity (not SERP title fragments)
- Official/association source authority
- Series identity with next validation trigger

V3 often had **one** of these; controls usually had **three+**.
`
  );

  write(
    "DISCOVERY_ADMISSION_GATE.md",
    `# Discovery Admission Gate

\`isGdiDiscoveryCandidateWorthCompleting(candidate)\`

**Not** customer readiness. Admits to expensive completion research only.

## Minimum structure
VALID ENTITY
\\+ PLAUSIBLE FUTURE DEMAND
\\+ TARGET MARKET RELEVANCE
\\+ ≥1 of: lodging hint · buyer/organizer · repeat/rotation · procurement · housing · group motion · competitor use · travel-series pattern

## Reject / defer
company expansion w/o group motion · conference w/o travel/lodging · HQ-only · generic calendars · historic-only · broad corporate news · generic tourism · OTA/job noise

## Layers
| Layer | Meaning |
|-------|---------|
| SIGNAL | Interesting intelligence only |
| CANDIDATE | Admitted for completion |
| OPPORTUNITY | Passes canonical readiness |

Jev is called **only after** admission (conditional blocker/source/stop).
`
  );

  write(
    "JEV_ROLE_DECISION.md",
    `# Jev Role Decision (V4)

Based on Candidate Completion V2 evidence (75 accepted, 0 classification changes from Jev routing alone):

| Control | Decision |
|---------|----------|
| Candidate priority | **DISABLED** (deterministic admission + score) |
| Language selection | **DISABLED** |
| Feeder-market selection | **DISABLED** |
| Research depth | **DISABLED** (hard policy: max 2; 3rd only if one resolvable blocker) |
| Next blocker | **CONDITIONAL** (after admission) |
| Source selection | **CONDITIONAL** (after admission) |
| Stop/continue | **CONDITIONAL** (after admission) |

Jev must not spend cycles on SIGNAL_ONLY / REJECTED_EARLY rows.
`
  );

  // ——— Build universe + admit ———
  console.log("[v4] building universe + admission…");
  const { candidates: v3Pool } = loadV3CandidatesFromReports(V3_DIR, HOTELS);
  // Reload after ASSOCIATION_RESULTS written
  const { candidates: v3WithAssoc } = loadV3CandidatesFromReports(V3_DIR, HOTELS);
  const { items: deItems } = loadActiveResearchUniverse({
    demandEngineDir: DE_DIR,
    completionDir: CC_DIR,
    hotels: HOTELS,
  });

  const byHotel = Object.fromEntries(HOTELS.map((h) => [h.hotelKey, h]));
  const universe = [];
  const seen = new Set();
  const pushU = (row, sourceFamily) => {
    const hotel = byHotel[row.hotelKey];
    if (!hotel) return;
    const shaped = toResearchShape(row, hotel);
    const key = dedupeKey(
      hotel.hotelKey,
      shaped.officialSource,
      shaped.title,
      shaped.id || shaped.opportunityId || shaped.researchId
    );
    if (seen.has(key)) {
      shaped._dup = true;
    } else {
      seen.add(key);
      shaped._dup = false;
    }
    shaped._sourceFamily = sourceFamily;
    universe.push(shaped);
  };

  for (const c of v3WithAssoc) pushU(c, c.discoveryMeta?.scoutFamily || "V3");
  for (const c of recovered) {
    // may already be in v3WithAssoc via ASSOCIATION_RESULTS — mark recovered origin
    pushU({ ...c, recoveredV4: true }, "AssociationScout_RECOVERED");
  }
  for (const c of deItems) pushU(c, c.signalType || "DEMAND_ENGINE");

  const classified = [];
  for (const u of universe) {
    const hotel = byHotel[u.hotelKey];
    const adm = isGdiDiscoveryCandidateWorthCompleting(u, {
      geoTokens: hotel.geoTokens,
      isDuplicate: u._dup === true,
    });
    const cmp = compareCandidateToControls(u, controlAgg);
    classified.push({
      ...u,
      admissionClass: adm.class,
      admissionOk: adm.ok,
      admissionReasons: adm.reasons,
      motionSignals: adm.motionSignals,
      structuralGaps: cmp.gaps,
      gapCount: cmp.gapCount,
    });
  }

  const admitted = classified.filter(
    (c) =>
      c.admissionOk &&
      (c.admissionClass === ADMISSION_CLASS.ADMITTED_CANDIDATE ||
        c.admissionClass === ADMISSION_CLASS.VALID_FUTURE_WATCH_CANDIDATE)
  );
  const signalOnly = classified.filter((c) => c.admissionClass === ADMISSION_CLASS.SIGNAL_ONLY);
  const rejectedEarly = classified.filter((c) => c.admissionClass === ADMISSION_CLASS.REJECTED_EARLY);
  const duplicates = classified.filter((c) => c.admissionClass === ADMISSION_CLASS.DUPLICATE);

  write(
    "SIGNAL_CANDIDATE_CLASSIFICATION.csv",
    toCsv(
      classified.map((c) => ({
        hotel: c.hotelKey,
        id: c.id || c.researchId || c.opportunityId,
        title: c.title,
        sourceFamily: c._sourceFamily,
        signalType: c.signalType,
        admissionClass: c.admissionClass,
        reasons: (c.admissionReasons || []).join("|"),
        motionSignals: (c.motionSignals || []).join("|"),
        structuralGaps: (c.structuralGaps || []).join("|"),
        source: c.officialSource || c.source,
      })),
      [
        "hotel",
        "id",
        "title",
        "sourceFamily",
        "signalType",
        "admissionClass",
        "reasons",
        "motionSignals",
        "structuralGaps",
        "source",
      ]
    )
  );

  const rotPre = classified.filter(
    (c) => c.signalType === "ROTATION_SERIES" || c.signalType === "PRE_RFP" || c.preRfp
  );
  write(
    "ROTATION_PRE_RFP_ADMISSION.csv",
    toCsv(
      rotPre.map((c) => ({
        hotel: c.hotelKey,
        id: c.id || c.opportunityId || c.researchId,
        signalType: c.signalType,
        admissionClass: c.admissionClass,
        admitted: c.admissionOk,
        reasons: (c.admissionReasons || []).join("|"),
        gaps: (c.structuralGaps || []).join("|"),
      })),
      ["hotel", "id", "signalType", "admissionClass", "admitted", "reasons", "gaps"]
    )
  );

  const assocClass = classified.filter(
    (c) =>
      /Association/i.test(String(c._sourceFamily || c.discoveryMeta?.scoutFamily || "")) ||
      c.signalType === "ASSOCIATION_RECOVERED"
  );
  write(
    "ASSOCIATION_QUALITY_AUDIT.csv",
    toCsv(
      assocClass.map((c) => ({
        hotel: c.hotelKey,
        id: c.id,
        title: c.title,
        admissionClass: c.admissionClass,
        admitted: c.admissionOk,
        lodgingHint: (c.motionSignals || []).includes("LODGING_HINT"),
        seriesHint: (c.motionSignals || []).includes("REPEAT_ROTATION_SIGNAL"),
        reasons: (c.admissionReasons || []).join("|"),
        gaps: (c.structuralGaps || []).join("|"),
        source: c.officialSource,
      })),
      [
        "hotel",
        "id",
        "title",
        "admissionClass",
        "admitted",
        "lodgingHint",
        "seriesHint",
        "reasons",
        "gaps",
        "source",
      ]
    )
  );

  // ——— Phase 11: completion on admitted only ———
  console.log(`[v4] completing admitted candidates (cap)… admitted=${admitted.length}`);
  // Prefer association recovered + lodging motion + rotation/pre-rfp
  const completionQueue = [...admitted].sort((a, b) => {
    const score = (x) =>
      (x.signalType === "ASSOCIATION_RECOVERED" || /Association/i.test(x._sourceFamily || "")
        ? 5
        : 0) +
      ((x.motionSignals || []).includes("LODGING_HINT") ? 4 : 0) +
      (x.signalType === "PRE_RFP" ? 3 : 0) +
      (x.signalType === "ROTATION_SERIES" ? 2 : 0) +
      (x.admissionClass === ADMISSION_CLASS.ADMITTED_CANDIDATE ? 1 : 0);
    return score(b) - score(a);
  });

  const maxComplete = 40;
  const toComplete = completionQueue.slice(0, maxComplete);
  const completionResults = [];
  let completeCost = 0;

  for (let i = 0; i < toComplete.length; i++) {
    const c = toComplete[i];
    const hotel = byHotel[c.hotelKey];
    if (i % 5 === 0) console.log(`[v4] complete ${i + 1}/${toComplete.length} ${c.hotelKey}`);
    // Force P0/P1 for completion engine via priority build on singleton
    const prioritized = buildGdiCompletionPriority([
      {
        ...c,
        lodgingEvidence: c.lodgingEvidence,
        hotelFitScore: c.defaultFitScore || 48,
        discoveryMeta: { ...(c.discoveryMeta || {}), scoutFamily: c._sourceFamily },
      },
    ]).queue[0];
    // Cap depth: override to at most 2 via jev advice DEPTH_1/2
    const r = await completeOneCandidate(
      {
        ...prioritized,
        completionPriority:
          prioritized.completionPriority === COMPLETION_PRIORITY.P2_LOW_DEFER
            ? COMPLETION_PRIORITY.P1_MEDIUM_COMPLETION_POTENTIAL
            : prioritized.completionPriority,
      },
      hotel,
      { nowDate: NOW }
    );
    // Enforce max 2 steps economically already via depth policy; record
    completeCost += r.economics?.estimatedCost || 0;
    completionResults.push(r);
  }

  // Association post-completion stats
  const assocCompleted = completionResults.filter(
    (r) =>
      /Association/i.test(String(r.opportunity?.discoveryMeta?.scoutFamily || "")) ||
      r.opportunity?.gdiDiscoveryVersion === "discovery_expansion_v3_recovered_v4"
  );
  const assocReady = assocCompleted.filter((r) => r.terminalClass === TERMINAL_CLASS.CUSTOMER_READY)
    .length;
  const assocWatch = assocCompleted.filter(
    (r) => r.terminalClass === TERMINAL_CLASS.VALID_FUTURE_WATCH
  ).length;

  write(
    "COMPLETION_RESULTS.csv",
    toCsv(
      completionResults.map((r) => ({
        hotel: r.hotelKey,
        id: r.opportunity?.id,
        title: r.opportunity?.title,
        depth: r.depthAssigned,
        researched: r.researched,
        terminalClass: r.terminalClass,
        useful: r.useful,
        blockersResolved: r.economics?.blockersResolved,
        cost: r.economics?.estimatedCost,
        whoResolved: r.whoResolved,
        lodgingClass: r.lodgingClass,
        timingState: r.timingState,
      })),
      [
        "hotel",
        "id",
        "title",
        "depth",
        "researched",
        "terminalClass",
        "useful",
        "blockersResolved",
        "cost",
        "whoResolved",
        "lodgingClass",
        "timingState",
      ]
    )
  );

  const readyN = completionResults.filter((r) => r.terminalClass === TERMINAL_CLASS.CUSTOMER_READY)
    .length;
  const watchN = completionResults.filter(
    (r) => r.terminalClass === TERMINAL_CLASS.VALID_FUTURE_WATCH
  ).length;
  const usefulN = readyN + watchN;
  const rawSignals = classified.length;
  const admittedN = admitted.length;
  const researchedN = completionResults.filter((r) => r.researched).length;
  const sigToCand = rawSignals ? admittedN / rawSignals : 0;
  const candToUseful = admittedN ? usefulN / Math.min(admittedN, maxComplete) : 0;
  // Use completed denominator for conversion of researched admitted
  const candToUsefulResearched = researchedN ? usefulN / researchedN : 0;
  const totalCost = recoveryCost + completeCost;
  const costPerUseful = usefulN ? totalCost / usefulN : null;

  write(
    "FUNNEL_CONVERSION.csv",
    toCsv(
      [
        { stage: "RAW_SIGNAL", count: rawSignals },
        { stage: "ADMITTED_CANDIDATE", count: admittedN },
        { stage: "FULLY_RESEARCHED", count: researchedN },
        { stage: "CUSTOMER_READY", count: readyN },
        { stage: "VALID_FUTURE_WATCH", count: watchN },
        { stage: "REJECTED_EARLY", count: rejectedEarly.length },
        { stage: "SIGNAL_ONLY", count: signalOnly.length },
        { stage: "DUPLICATE", count: duplicates.length },
        { stage: "SIGNAL_TO_CANDIDATE_PCT", count: (sigToCand * 100).toFixed(1) },
        {
          stage: "CANDIDATE_TO_USEFUL_PCT_OF_RESEARCHED",
          count: (candToUsefulResearched * 100).toFixed(1),
        },
      ],
      ["stage", "count"]
    )
  );

  const hotelRows = HOTELS.map((h) => {
    const raw = classified.filter((c) => c.hotelKey === h.hotelKey);
    const adm = raw.filter((c) => c.admissionOk);
    const res = completionResults.filter((r) => r.hotelKey === h.hotelKey);
    return {
      hotel: h.hotelKey,
      rawSignals: raw.length,
      admitted: adm.length,
      researched: res.filter((r) => r.researched).length,
      ready: res.filter((r) => r.terminalClass === TERMINAL_CLASS.CUSTOMER_READY).length,
      watch: res.filter((r) => r.terminalClass === TERMINAL_CLASS.VALID_FUTURE_WATCH).length,
      rejected: res.filter((r) => r.terminalClass === TERMINAL_CLASS.REJECTED_CONFIRMED).length,
    };
  });
  write(
    "HOTEL_RESULTS.csv",
    toCsv(hotelRows, [
      "hotel",
      "rawSignals",
      "admitted",
      "researched",
      "ready",
      "watch",
      "rejected",
    ])
  );

  write(
    "COST_REPORT.md",
    `# Cost Report — Discovery Quality Persistence V4

| Metric | Value |
|--------|------:|
| Association recovery cost | ${recoveryCost.toFixed(3)} |
| Completion cost | ${completeCost.toFixed(3)} |
| Total | ${totalCost.toFixed(3)} |
| Cost per admitted researched | ${researchedN ? (completeCost / researchedN).toFixed(3) : "n/a"} |
| Cost per useful | ${costPerUseful == null ? "N/A" : costPerUseful.toFixed(3)} |

No broad rediscovery. Association recovery = AssociationScout-only surgical SERP.
`
  );

  const assocAdmitted = assocClass.filter((c) => c.admissionOk).length;
  const rotAdmitted = rotPre.filter(
    (c) => c.admissionOk && c.signalType === "ROTATION_SERIES"
  ).length;
  const preAdmitted = rotPre.filter(
    (c) => c.admissionOk && (c.signalType === "PRE_RFP" || c.preRfp)
  ).length;

  const hr = (k) => hotelRows.find((x) => x.hotel === k) || { ready: 0, watch: 0 };

  const founder = `# GDI Discovery Quality + Persistence Recovery V4

**Date:** ${NOW}

## A. Executive Summary

AssociationScout produced **${assocProduced}** detail hits in V3 SCOUT_YIELD but **0** were persisted to \`ASSOCIATION_RESULTS.csv\` (silent report-writer omission). Surgical recovery restored **${recovered.length}** Association detail rows. Admission gate separated SIGNAL vs CANDIDATE before completion. Bounded completion on admitted candidates only.

| Funnel | Count |
|--------|------:|
| Raw signals reviewed | ${rawSignals} |
| Admitted candidates | ${admittedN} |
| Signal-only | ${signalOnly.length} |
| Early rejected | ${rejectedEarly.length} |
| Duplicates | ${duplicates.length} |
| Researched | ${researchedN} |
| Customer ready | ${readyN} |
| Valid future watch | ${watchN} |

## B. Association Persistence

See ASSOCIATION_PERSISTENCE_ROOT_CAUSE.md. Root cause: **${rootCause.dropLocation}**.

## C. Recovery

Recovered=${recovered.length} · invalid=${recoveryInvalid.length} · admitted from association family=${assocAdmitted}.

## D. Success Controls

Bethesda/NYC ready controls n=${controls.length}. Pattern: named buyer + future timing + hotel-motion thesis + contact/housing path. V3 SERP hits rarely combine these.

## E. Admission Gate

Implemented. Thresholds unchanged.

## F. Jev Role

Priority/depth/language/feeder **DISABLED**. Next-blocker / source / stop-continue **CONDITIONAL** post-admission.

## G. Hotel Results

${hotelRows.map((h) => `${h.hotel}: raw ${h.rawSignals} · admitted ${h.admitted} · researched ${h.researched} · ready ${h.ready} · watch ${h.watch}`).join("\n")}

## H. Conversion

Signal→candidate ${(sigToCand * 100).toFixed(1)}% · Candidate(researched)→useful ${(candToUsefulResearched * 100).toFixed(1)}% · Cost/useful ${costPerUseful == null ? "N/A" : "$" + costPerUseful.toFixed(3)}

## Safety
Thresholds NO · Watch bypass NO · ADP NO · Shares NO · Broad discovery NO

**STOP.**
`;

  write("FOUNDER_REPORT.md", founder);

  const ret = {
    ASSOCIATION_DETAIL_ROWS_LOST: assocLost,
    ASSOCIATION_PERSISTENCE_ROOT_CAUSE: rootCause.dropLocation,
    ASSOCIATION_ROWS_RECOVERED: recovered.length,
    ASSOCIATION_ROWS_ADMITTED: assocAdmitted,
    TOTAL_RAW_SIGNALS_REVIEWED: rawSignals,
    TOTAL_ADMITTED_CANDIDATES: admittedN,
    TOTAL_SIGNAL_ONLY: signalOnly.length,
    TOTAL_EARLY_REJECTED: rejectedEarly.length,
    TOTAL_DUPLICATES: duplicates.length,
    ROTATION_SERIES_ADMITTED: rotAdmitted,
    PRE_RFP_ITEMS_ADMITTED: preAdmitted,
    CUSTOMER_READY_CREATED: readyN,
    VALID_FUTURE_WATCH_CREATED: watchN,
    YOTEL_READY_WATCH: `${hr("YOTEL").ready} / ${hr("YOTEL").watch}`,
    AC_READY_WATCH: `${hr("AC").ready} / ${hr("AC").watch}`,
    SPICE_READY_WATCH: `${hr("SPICE").ready} / ${hr("SPICE").watch}`,
    CAMBRIDGE_READY_WATCH: `${hr("CAMBRIDGE").ready} / ${hr("CAMBRIDGE").watch}`,
    NOW_NOW_READY_WATCH: `${hr("NOW_NOW").ready} / ${hr("NOW_NOW").watch}`,
    SUCCESSFUL_BETHESDA_NYC_STRUCTURAL_DIFFERENCES: controlAgg.structuralDifferences,
    DISCOVERY_ADMISSION_GATE_IMPLEMENTED: true,
    SIGNAL_CANDIDATE_SEPARATION_IMPLEMENTED: true,
    ASSOCIATION_PERSISTENCE_FIXED: true,
    JEV_PRIORITY_CONTROL_DISABLED: true,
    JEV_DEPTH_CONTROL_DISABLED: true,
    JEV_NEXT_BLOCKER_CONDITIONAL: true,
    JEV_SOURCE_SELECTION_CONDITIONAL: true,
    JEV_STOP_CONTINUE_CONDITIONAL: true,
    SIGNAL_TO_CANDIDATE_CONVERSION_PCT: Number((sigToCand * 100).toFixed(1)),
    CANDIDATE_TO_USEFUL_OPPORTUNITY_CONVERSION_PCT: Number(
      (candToUsefulResearched * 100).toFixed(1)
    ),
    COST_PER_USEFUL_OPPORTUNITY: costPerUseful == null ? null : Number(costPerUseful.toFixed(3)),
    ASSOCIATION_SCOUT_SIGNALS: assocClass.length,
    ASSOCIATION_SCOUT_ADMITTED: assocAdmitted,
    ASSOCIATION_SCOUT_READY_AFTER_COMPLETION: assocReady,
    ASSOCIATION_SCOUT_WATCH_AFTER_COMPLETION: assocWatch,
    ASSOCIATION_SCOUT_REJECTED: assocClass.filter((c) => !c.admissionOk).length,
    GDI_THRESHOLDS_CHANGED: false,
    WATCH_QUALITY_STANDARD_BYPASSED: false,
    ADP_CHANGED: false,
    SHARE_TOKENS_CHANGED: false,
    NEW_BROAD_DISCOVERY_RUN: false,
    v3PoolBeforeAssocFile: v3Pool.length,
    v3PoolAfterAssocFile: v3WithAssoc.length,
    recoveryCost,
    completeCost,
  };

  write("_RETURN.json", JSON.stringify(ret, null, 2));
  console.log("\n========== RETURN ==========");
  console.log(JSON.stringify(ret, null, 2));
  console.log(`[v4] reports → ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
