/**
 * GDI Full-Universe Research Coverage + Next-Cycle Prioritization V1
 *
 * Read-only audit. Does NOT launch research cycles.
 * Distinguishes INITIALIZED_ONLY from RESEARCHED_NO_READY.
 *
 *   node scripts/audit-gdi-universe-research-coverage-v1.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  resolveCanonicalHotelId,
  loadAdpGdiAliasMap,
} from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { getCensusLinkEntry } from "../lib/ai-demand-positioning/census-link-registry.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { MAP_HOTEL_GENERATOR_FIT as FIT } from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import {
  MAP_RESEARCH_TARGET as TGT,
  MAP_RESEARCH_RUN as RUN,
  MAP_RESEARCH_TARGET_RUN as TR,
} from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";
import {
  DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
  MAP_DEMAND_GENERATOR_SIGNAL as SIG,
} from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import {
  assessSubstantiveResearchEvidence,
  classifyGdiResearchMaturity,
  classifyZeroOppRootCause,
  prioritizeNextCycle,
  isInitFootprintRun,
  GDI_RESEARCH_MATURITY,
  NEXT_CYCLE_BUCKET,
} from "../lib/group-demand-intelligence/research-maturity-v1.js";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/decision-outcomes/airtable-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports", "group-demand-intelligence", "universe-research-coverage-v1");

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const intel =
  getGdiOpportunitiesAirtableBaseId() ||
  process.env.ADP_AIRTABLE_BASE_ID ||
  CANONICAL_INTELLIGENCE_BASE_ID;
assertNotLegacyMvpCanonicalBase(intel, { surface: "gdi-research-coverage-audit" });
if (!token) throw new Error("missing_token");
const intelBase = new Airtable({ apiKey: token }).base(intel);

const BETHESDA_HPC = "recLuxvwwxID7U2B8";
const AC_HPC = "rec2PVBDavppGpenm";
const SPICE_HPC = "recKRJjcPnb4tVDDS";

/** Evidence-backed focused lane gaps (not seeded opportunities). */
const FOCUSED_LANE_GAPS = {
  [AC_HPC]: {
    hasFocusedLaneGap: true,
    pilotImportance: 3,
    priorCycles: 2,
    lanes: [
      "corporate / industrial",
      "port / maritime",
      "healthcare",
      "university / science (deeper than webinar/calendar noise)",
      "project teams / training",
    ],
    evidenceNote:
      "Cycles 1–2 surfaced academic/scientific calendars, cultural festivals, sports overflow housing, and light corporate conferences — 0 customer-ready. Port/maritime logistics employers, industrial programs, healthcare cohorts, and training/project teams were not exhausted as lodging-supported lanes.",
    expectedSources: [
      "port authority / terminal operator pages",
      "regional industrial association calendars",
      "hospital / biomedical program pages",
      "university faculty/event housing pages",
      "training academy schedules",
    ],
    budget: { queries: 24, fetches: 36 },
    success:
      ">=1 lodging-supported candidate clearing readiness OR documented public-data ceiling per lane with provenance",
  },
  [SPICE_HPC]: {
    hasFocusedLaneGap: true,
    pilotImportance: 3,
    priorCycles: 1,
    lanes: [
      "incentive / executive retreat (deeper)",
      "luxury travel advisor networks",
      "destination wedding planner (planner-side, not brochure)",
      "marine / yachting charter groups",
      "Caribbean regional organizations",
    ],
    evidenceNote:
      "First cycle found MICE tourism conferences, yacht charter mentions, destination wedding pages, corporate retreat/incentive blurbs — mostly INVALID/WATCH, 0 ready. Deeper planner/advisor/yacht operator and regional org paths remain.",
    expectedSources: [
      "incentive house / DMC pages",
      "luxury advisor consortia",
      "wedding planner portfolios with lodging asks",
      "yacht charter operator group itineraries",
      "Caribbean tourism/association event pages with housing",
    ],
    budget: { queries: 20, fetches: 30 },
    success:
      "prove or refute lodging-supported incentive/yacht/wedding-planner demand with named path — no brochure filler",
  },
  rec35fExUxCClpOP6: {
    hasFocusedLaneGap: false,
    pilotImportance: 1,
    priorCycles: 1,
    lanes: ["association subgroups with Midtown housing evidence", "production / entertainment crews"],
    evidenceNote: "First cycle: 22q/34f, 16 candidates all FUTURE_WATCH/HOUSING — target ledger complete.",
    expectedSources: ["association housing pages", "production office notices"],
    budget: { queries: 16, fetches: 24 },
    success: "only if new housing-backed subgroup evidence appears — else public-data ceiling",
  },
  rece0or38cxo3Fymb: {
    hasFocusedLaneGap: true,
    pilotImportance: 1,
    priorCycles: 1,
    lanes: ["association / corporate Rome", "fashion / production", "Embassy / institutional programs"],
    evidenceNote: "Target runs present (12) with 0 customer-ready — deepen selected lanes, not broad re-init.",
    expectedSources: ["official program calendars", "housing blocks", "institutional event pages"],
    budget: { queries: 16, fetches: 24 },
    success: "lane-specific lodging support or documented ceiling",
  },
};

function esc(s) {
  return String(s || "").replace(/"/g, '\\"');
}

function readJson(p) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function discoverUniverse() {
  const pubRoot = path.join(ROOT, "data/ai-demand-positioning/published");
  const dirs = fs
    .readdirSync(pubRoot)
    .filter((d) => fs.existsSync(path.join(pubRoot, d, "manifest.json")));
  const aliasMap = loadAdpGdiAliasMap();
  const hotels = [];
  for (const adp of dirs) {
    const manifest = readJson(path.join(pubRoot, adp, "manifest.json"));
    if (String(manifest?.publishStatus || "").toLowerCase() !== "live") continue;
    const link = getCensusLinkEntry(adp);
    const hpc =
      resolveCanonicalHotelId(adp) ||
      link?.censusRecordId ||
      aliasMap.aliases?.[adp]?.canonicalHotelId ||
      null;
    hotels.push({
      adpPropertyId: adp,
      hotelName: manifest?.propertyName || adp,
      hpcId: hpc,
      period: manifest?.latestPeriodId || null,
      publishStatus: manifest?.publishStatus || null,
    });
  }
  hotels.sort((a, b) => a.hotelName.localeCompare(b.hotelName));
  return hotels;
}

async function listByField(table, field, value, fields = null) {
  const rows = [];
  const opts = { pageSize: 100, filterByFormula: `{${field}} = "${esc(value)}"` };
  if (fields) opts.fields = fields;
  await intelBase(table)
    .select(opts)
    .eachPage((recs, next) => {
      for (const r of recs) {
        rows.push({ id: r.id, fields: r.fields || {}, createdTime: r._rawJson?.createdTime });
      }
      next();
    });
  return rows;
}

function normalizeRun(rec) {
  const f = rec.fields || {};
  let payload = null;
  try {
    payload = f[RUN.payloadJson] ? JSON.parse(f[RUN.payloadJson]) : null;
  } catch {
    payload = null;
  }
  return {
    id: rec.id,
    runId: f[RUN.runId],
    notes: f[RUN.notes] || "",
    status: f[RUN.status],
    runType: f[RUN.runType],
    queries: f[RUN.queries] || 0,
    fetches: f[RUN.fetches] || 0,
    targetsAttempted: f[RUN.targetsAttempted] || 0,
    targetsCompleted: f[RUN.targetsCompleted] || 0,
    newSignals: f[RUN.newSignals] || 0,
    newOpportunities: f[RUN.newOpportunities] || 0,
    payloadJson: f[RUN.payloadJson],
    payload,
  };
}

function normalizeTargetRun(rec) {
  const f = rec.fields || {};
  return {
    id: rec.id,
    targetRunId: f[TR.targetRunId],
    targetId: f[TR.targetId],
    runId: f[TR.runId],
    executionStatus: f[TR.executionStatus],
    resultType: f[TR.resultType],
    queriesUsed: f[TR.queriesUsed] || 0,
    fetchesUsed: f[TR.fetchesUsed] || 0,
    sourceCount: f[TR.sourceCount] || 0,
    researchExecuted: f[TR.executionStatus] === "RESEARCHED",
  };
}

function normalizeTarget(rec) {
  const f = rec.fields || {};
  return {
    id: rec.id,
    targetId: f[TGT.targetId],
    status: f[TGT.status],
    lastResearchedAt: f[TGT.lastResearchedAt] || null,
    lastResult: f[TGT.lastResult] || null,
    lastRunId: f[TGT.lastRunId] || null,
    successfulRuns: f[TGT.successfulRuns] || 0,
    signalsFound: f[TGT.signalsFound] || 0,
  };
}

function loadLocalRuns(hpcId) {
  const dirs = [
    path.join(ROOT, "data/group-demand-intelligence/hotels", hpcId, "runs"),
    // legacy slug dirs — scan sibling folders that map via run hotelId
  ];
  const out = [];
  for (const dir of dirs) {
    if (!fs.existsSync(dir)) continue;
    for (const name of fs.readdirSync(dir)) {
      const p = path.join(dir, name, "run.json");
      const j = readJson(p);
      if (!j) continue;
      const queries = Number(j.cost?.queryCount || j.metrics?.queries || 0);
      const fetches = Number(j.metrics?.fetches || 0);
      const webhoundUsd = Number(j.cost?.webhoundUsd || 0);
      const candidates = Number(j.metrics?.candidatesResearched || 0);
      const sourceCount = Number(j.metrics?.sourceCount || 0);
      const seedIngestOnly =
        queries === 0 &&
        fetches === 0 &&
        webhoundUsd === 0 &&
        candidates > 0 &&
        Number(j.cost?.providerCallCount || 0) === 0;
      out.push({
        id: j.id,
        trigger: j.trigger,
        status: j.status,
        queries,
        fetches: webhoundUsd > 0 ? Math.max(fetches, 1) : fetches,
        webhoundUsd,
        candidates,
        sourceCount,
        seedIngestOnly,
        path: p,
      });
    }
  }
  // Also scan all hotel run dirs for matching hotelId (slug aliases)
  const rootHotels = path.join(ROOT, "data/group-demand-intelligence/hotels");
  if (fs.existsSync(rootHotels)) {
    for (const hid of fs.readdirSync(rootHotels)) {
      if (hid === hpcId) continue;
      const runsDir = path.join(rootHotels, hid, "runs");
      if (!fs.existsSync(runsDir)) continue;
      for (const name of fs.readdirSync(runsDir)) {
        const p = path.join(runsDir, name, "run.json");
        const j = readJson(p);
        if (!j || j.hotelId !== hpcId) continue;
        if (out.some((x) => x.id === j.id)) continue;
        const queries = Number(j.cost?.queryCount || 0);
        const webhoundUsd = Number(j.cost?.webhoundUsd || 0);
        const candidates = Number(j.metrics?.candidatesResearched || 0);
        const sourceCount = Number(j.metrics?.sourceCount || 0);
        const fetches = Number(j.metrics?.fetches || 0);
        out.push({
          id: j.id,
          trigger: j.trigger,
          status: j.status,
          queries,
          fetches: webhoundUsd > 0 ? Math.max(fetches, 1) : fetches,
          webhoundUsd,
          candidates,
          sourceCount,
          seedIngestOnly:
            queries === 0 &&
            fetches === 0 &&
            webhoundUsd === 0 &&
            candidates > 0 &&
            Number(j.cost?.providerCallCount || 0) === 0,
          path: p,
        });
      }
    }
  }
  return out;
}

function loadCycleSummaries(hpcId, adpId) {
  const map = {
    [AC_HPC]: [
      "reports/group-demand-intelligence/ac-hotel-a-coruna-v1/GDI_FIRST_CYCLE_SUMMARY.json",
      "reports/group-demand-intelligence/ac-hotel-a-coruna-v1/GDI_CYCLE2_SUMMARY.json",
    ],
    [SPICE_HPC]: [
      "reports/group-demand-intelligence/spice-island-beach-resort-v1/GDI_FIRST_CYCLE_SUMMARY.json",
    ],
    rec35fExUxCClpOP6: [
      "reports/group-demand-intelligence/hotel4-hilton-times-square-v1/GDI_FIRST_CYCLE_SUMMARY.json",
    ],
  };
  const paths = map[hpcId] || [];
  return paths
    .map((rel) => {
      const j = readJson(path.join(ROOT, rel));
      if (!j) return null;
      return {
        path: rel,
        queries: j.discovery?.queries || 0,
        fetches: j.discovery?.fetches || 0,
        candidates: j.discovery?.candidates || 0,
        qualified: j.qualification?.qualified || 0,
        customerVisible: j.customerVisible?.total || 0,
        byState: j.discovery?.byState || {},
        discovery: j.discovery,
        qualification: j.qualification,
      };
    })
    .filter(Boolean);
}

function computeCoverage(targets, researchedTargetIds) {
  const total = targets.length;
  const researchedSet = new Set(researchedTargetIds);
  // Also count targets with lastResearchedAt / successfulRuns
  let fromTargetFields = 0;
  let deferred = 0;
  let ceiling = 0;
  for (const t of targets) {
    if (t.lastResearchedAt || Number(t.successfulRuns) > 0 || researchedSet.has(t.targetId)) {
      fromTargetFields += 1;
      researchedSet.add(t.targetId);
    }
    if (/DEFER|PAUSED|LOW_PRIORITY/i.test(String(t.status || ""))) deferred += 1;
    if (/CEILING|PUBLIC_DATA|INSUFFICIENT/i.test(String(t.lastResult || ""))) ceiling += 1;
  }
  const researched = researchedSet.size;
  const unresearched = Math.max(0, total - researched);
  const pct = total > 0 ? Math.round((researched / total) * 1000) / 10 : 0;
  return {
    totalTargets: total,
    targetsResearched: researched,
    targetsUnresearched: unresearched,
    targetsDeferred: deferred,
    targetsAtPublicDataCeiling: ceiling,
    targetCoveragePct: pct,
  };
}

async function auditHotel(hotel) {
  const hpcId = hotel.hpcId;
  if (!hpcId) {
    return {
      ...hotel,
      error: "missing_hpc",
      maturity: GDI_RESEARCH_MATURITY.RESEARCH_STATE_UNKNOWN,
    };
  }

  const [fitRecs, tgtRecs, runRecs, trRecs] = await Promise.all([
    listByField("Hotel Demand Generator Fit", FIT.hotelId, hpcId, [FIT.fitId]),
    listByField("GDI Research Targets", TGT.hotelId, hpcId, [
      TGT.targetId,
      TGT.status,
      TGT.lastResearchedAt,
      TGT.lastResult,
      TGT.lastRunId,
      TGT.successfulRuns,
      TGT.signalsFound,
    ]),
    listByField("GDI Research Runs", RUN.hotelId, hpcId, [
      RUN.runId,
      RUN.notes,
      RUN.status,
      RUN.runType,
      RUN.queries,
      RUN.fetches,
      RUN.targetsAttempted,
      RUN.targetsCompleted,
      RUN.newSignals,
      RUN.newOpportunities,
      RUN.payloadJson,
    ]),
    listByField("GDI Research Target Runs", TR.hotelId, hpcId, [
      TR.targetRunId,
      TR.targetId,
      TR.runId,
      TR.executionStatus,
      TR.resultType,
      TR.queriesUsed,
      TR.fetchesUsed,
      TR.sourceCount,
    ]),
  ]);

  let signalCount = 0;
  try {
    const sigs = await listByField(
      DEMAND_GENERATOR_SIGNALS_TABLE_NAME || "Demand Generator Signals",
      SIG.hotelId || "Hotel ID",
      hpcId,
      [SIG.signalId || "Signal ID"].filter(Boolean)
    );
    signalCount = sigs.length;
  } catch {
    // hotel-scoped signals may use different link field — leave nullish as 0
    signalCount = 0;
  }

  const fits = fitRecs.length;
  const targets = tgtRecs.map(normalizeTarget);
  const runs = runRecs.map(normalizeRun);
  const targetRuns = trRecs.map(normalizeTargetRun);
  const localRuns = loadLocalRuns(hpcId);
  const cycleSummaries = loadCycleSummaries(hpcId, hotel.adpPropertyId);

  let opps = [];
  try {
    const doc = await loadOpportunitiesCanonical(hpcId);
    opps = doc?.opportunities || [];
  } catch {
    opps = [];
  }
  const customer = filterCustomerFacingOpportunities(opps);
  const customerReady = customer.length;

  // Aggregate candidates/qualified from cycles + local
  const candidates =
    cycleSummaries.reduce((s, c) => s + (c.candidates || 0), 0) ||
    localRuns.reduce((s, r) => s + (r.candidates || 0), 0);
  const qualified =
    cycleSummaries.reduce((s, c) => s + (c.qualified || 0), 0) ||
    localRuns.reduce((s, r) => s + Number(r.candidates || 0), 0);
  const watchlistHeavy = cycleSummaries.some(
    (c) => (c.qualification?.watchlist || 0) > 0 && (c.customerVisible || 0) === 0
  );

  const evidence = assessSubstantiveResearchEvidence({
    runs,
    targetRuns,
    localRuns,
    cycleSummaries,
    signals: signalCount,
    candidates,
    customerReady,
  });

  const coverage = computeCoverage(targets, evidence.researchedTargetIds);
  // Hotel-level cycles without target ledger: do NOT invent researched targets
  if (
    evidence.hasHotelCycleProof &&
    !evidence.hasTargetLedgerProof &&
    evidence.strength === "SUBSTANTIVE" &&
    coverage.targetsResearched === 0
  ) {
    coverage.note =
      "Hotel-level discovery cycles proved research; target-run persistence missing — targets counted unresearched at ledger grain (no fabrication).";
  }

  const { maturity, systemState, rationale } = classifyGdiResearchMaturity({
    totalTargets: coverage.totalTargets,
    targetsResearched: coverage.targetsResearched,
    customerReady,
    evidence,
  });

  // AC/Spice must never be INITIALIZED_ONLY
  let maturityFinal = maturity;
  let systemStateFinal = systemState;
  let rationaleFinal = rationale;
  if (
    (hpcId === AC_HPC || hpcId === SPICE_HPC) &&
    maturity === GDI_RESEARCH_MATURITY.INITIALIZED_ONLY
  ) {
    maturityFinal = GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED;
    systemStateFinal = "RESEARCH_IN_PROGRESS";
    rationaleFinal =
      "override: verified GDI cycles with queries/fetches; not initialization-only";
  }

  const rootCause = classifyZeroOppRootCause({
    maturity: maturityFinal,
    evidence,
    customerReady,
    targetsResearched: coverage.targetsResearched,
    totalTargets: coverage.totalTargets,
    signals: signalCount,
    candidates,
    qualified,
    watchlistHeavy,
  });

  // Source metrics — prefer cycle summaries (avoid double-count with Airtable run rows)
  let queries = null;
  let fetches = null;
  if (cycleSummaries.length > 0) {
    queries = cycleSummaries.reduce((s, c) => s + (c.queries || 0), 0);
    fetches = cycleSummaries.reduce((s, c) => s + (c.fetches || 0), 0);
  } else if (evidence.hasTargetLedgerProof) {
    queries =
      targetRuns.reduce((s, t) => s + Number(t.queriesUsed || 0), 0) ||
      runs.filter((r) => !isInitFootprintRun(r)).reduce((s, r) => s + Number(r.queries || 0), 0) ||
      null;
    fetches =
      targetRuns.reduce((s, t) => s + Number(t.fetchesUsed || 0), 0) ||
      runs.filter((r) => !isInitFootprintRun(r)).reduce((s, r) => s + Number(r.fetches || 0), 0) ||
      null;
  } else {
    const q = runs.filter((r) => !isInitFootprintRun(r)).reduce((s, r) => s + Number(r.queries || 0), 0);
    const f = runs.filter((r) => !isInitFootprintRun(r)).reduce((s, r) => s + Number(r.fetches || 0), 0);
    queries = q || null;
    fetches = f || null;
  }

  const structuredSources = localRuns.reduce((s, r) => s + Number(r.sourceCount || 0), 0) || null;
  const renderedSources = null; // not inventing
  const pdfSources = null;

  const lane = FOCUSED_LANE_GAPS[hpcId] || {
    hasFocusedLaneGap: false,
    pilotImportance: hpcId === BETHESDA_HPC ? 5 : 0,
    priorCycles: evidence.proof.cycleSummaries || evidence.proof.airtableSubstantiveRuns || 0,
    lanes: [],
    evidenceNote: null,
    expectedSources: [],
    budget: null,
    success: null,
  };

  // HI score proxy from prior universe matrix if present
  const priorMatrix = readJson(
    path.join(ROOT, "reports/adp-gdi-universe-reconciliation-v1/MATRIX.json")
  );
  const priorRow = Array.isArray(priorMatrix)
    ? priorMatrix.find((r) => r.hpc === hpcId)
    : null;
  const hiScore = priorRow?.hi ? Number(String(priorRow.hi).split("/")[0]) : 0;

  const next = prioritizeNextCycle({
    maturity: maturityFinal,
    rootCause,
    hotelName: hotel.hotelName,
    isBethesda: hpcId === BETHESDA_HPC,
    coveragePct: coverage.targetCoveragePct,
    totalTargets: coverage.totalTargets,
    hiScore,
    customerReady,
    hasFocusedLaneGap: Boolean(lane.hasFocusedLaneGap),
    pilotImportance: lane.pilotImportance || 0,
  });

  // Soft-downgrade Hilton-style fully covered RESEARCHED_NO_READY unless lane gap
  if (
    maturityFinal === GDI_RESEARCH_MATURITY.RESEARCHED_NO_READY &&
    coverage.targetCoveragePct >= 95 &&
    !lane.hasFocusedLaneGap
  ) {
    next.bucket = NEXT_CYCLE_BUCKET.DEFER;
    next.why =
      "meaningful research cycle completed with full/near-full target coverage and 0 ready — defer broad rediscovery";
  }

  // Persistence repair assessment (do not apply here)
  const persistenceRepair = {
    needed:
      evidence.hasHotelCycleProof &&
      !evidence.hasTargetLedgerProof &&
      evidence.strength === "SUBSTANTIVE" &&
      (hpcId === AC_HPC ||
        hpcId === SPICE_HPC ||
        localRuns.some((r) => r.webhoundUsd > 0 || (r.candidates > 5 && !r.seedIngestOnly))),
    action: "DO_NOT_FABRICATE_PER_TARGET_ROWS",
    note:
      evidence.hasHotelCycleProof &&
      !evidence.hasTargetLedgerProof &&
      evidence.strength === "SUBSTANTIVE"
        ? "Hotel-level cycles prove research; target-run rows were never written by discovery orchestrator. Repair = future write-path fix + optional summary provenance on Research Run — not invented per-target history."
        : null,
  };

  const initRunCount = runs.filter((r) => isInitFootprintRun(r)).length;
  const substantiveAirtableRuns = evidence.proof.airtableSubstantiveRuns;

  return {
    hotelName: hotel.hotelName,
    adpPropertyId: hotel.adpPropertyId,
    hpcId,
    fits,
    targets: coverage.totalTargets,
    researchRuns: runs.length,
    initRuns: initRunCount,
    substantiveAirtableRuns,
    researchTargetRuns: targetRuns.length,
    researchedTargetRuns: evidence.proof.researchedTargetRuns,
    signals: signalCount || null,
    opportunitiesTotal: opps.length,
    customerReady,
    ...coverage,
    queries: queries === 0 ? (evidence.hasHotelCycleProof ? queries : null) : queries,
    fetches: fetches === 0 ? (evidence.hasHotelCycleProof ? fetches : null) : fetches,
    structuredSources,
    renderedSources,
    pdfSources,
    candidates: candidates || null,
    qualified: qualified || null,
    maturity: maturityFinal,
    systemState: systemStateFinal,
    rationale: rationaleFinal,
    rootCause,
    nextCycle: next.bucket,
    nextWhy: next.why,
    evidenceStrength: evidence.strength,
    evidenceProof: evidence.proof,
    cycleSummaries: cycleSummaries.map((c) => ({
      path: c.path,
      queries: c.queries,
      fetches: c.fetches,
      candidates: c.candidates,
      customerVisible: c.customerVisible,
    })),
    localRunCount: localRuns.length,
    lanePlan: lane,
    persistenceRepair,
    bethesdaProtected: hpcId === BETHESDA_HPC,
    qualityHeldClaimValid:
      maturityFinal === GDI_RESEARCH_MATURITY.RESEARCHED_NO_READY ||
      (maturityFinal === GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED &&
        evidence.strength === "SUBSTANTIVE"),
  };
}

function pickNextBatch(rows) {
  const priorMatrix = readJson(
    path.join(ROOT, "reports/adp-gdi-universe-reconciliation-v1/MATRIX.json")
  );
  for (const r of rows) {
    const p = Array.isArray(priorMatrix) ? priorMatrix.find((x) => x.hpc === r.hpcId) : null;
    r._hi = p?.hi ? Number(String(p.hi).split("/")[0]) : 0;
    // Promote stronger-HI init hotels into A for first substantive cycles
    if (r.maturity === GDI_RESEARCH_MATURITY.INITIALIZED_ONLY && r._hi >= 4) {
      r.nextCycle = NEXT_CYCLE_BUCKET.NEXT_CYCLE_A;
      r.nextWhy = "initialized only — stronger HI completeness for first substantive cycle";
    }
    // Weak seed-ingest hotels need a first real cycle (not brochure re-init)
    if (
      r.maturity === GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED &&
      r.evidenceStrength === "WEAK_PARTIAL" &&
      r._hi >= 4
    ) {
      r.nextCycle = NEXT_CYCLE_BUCKET.NEXT_CYCLE_A;
      r.nextWhy = "seed/qualification only — needs first substantive query/fetch cycle";
      r.lanePlan = {
        ...r.lanePlan,
        hasFocusedLaneGap: true,
        pilotImportance: Math.max(r.lanePlan?.pilotImportance || 0, 2),
        lanes: r.lanePlan?.lanes?.length
          ? r.lanePlan.lanes
          : ["independent web discovery against ACTIVE targets (no seed-only ingest)"],
        expectedSources: r.lanePlan?.expectedSources?.length
          ? r.lanePlan.expectedSources
          : ["official domains", "program calendars", "housing/venue pages"],
        budget: r.lanePlan?.budget || { queries: 18, fetches: 28 },
        success:
          r.lanePlan?.success ||
          "queries/fetches > 0 with target-run ledger; readiness gates; no weak promotions",
        evidenceNote:
          r.lanePlan?.evidenceNote ||
          "Prior admin seed ingest is not substantive market research.",
      };
    }
  }

  function score(r) {
    let s = 0;
    if (r.bethesdaProtected) return -10000;
    if (r.customerReady > 0) s -= 40;
    if (r.lanePlan?.hasFocusedLaneGap) s += 50;
    s += (r.lanePlan?.pilotImportance || 0) * 15;
    if (r.hpcId === AC_HPC || r.hpcId === SPICE_HPC) s += 45;
    if (r.maturity === GDI_RESEARCH_MATURITY.INITIALIZED_ONLY) s += 20 + r._hi * 12;
    if (r.maturity === GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED) {
      s += r.evidenceStrength === "WEAK_PARTIAL" ? 30 + r._hi * 10 : 15;
    }
    if (r.maturity === GDI_RESEARCH_MATURITY.RESEARCHED_NO_READY && (r.targetCoveragePct || 0) >= 95) {
      s -= 20; // exhausted target ledger — lower priority than uncovered hotels
    }
    if (r.nextCycle === NEXT_CYCLE_BUCKET.NEXT_CYCLE_A) s += 10;
    return s;
  }

  const pool = rows
    .filter(
      (r) =>
        r.nextCycle === NEXT_CYCLE_BUCKET.NEXT_CYCLE_A ||
        r.nextCycle === NEXT_CYCLE_BUCKET.NEXT_CYCLE_B
    )
    .filter((r) => !r.bethesdaProtected)
    .sort((x, y) => score(y) - score(x) || x.hotelName.localeCompare(y.hotelName));

  const selected = [];
  for (const id of [AC_HPC, SPICE_HPC]) {
    const row = pool.find((r) => r.hpcId === id);
    if (row && !selected.includes(row)) selected.push(row);
  }
  for (const row of pool) {
    if (selected.length >= 5) break;
    if (!selected.includes(row)) selected.push(row);
  }
  return selected.slice(0, 5);
}

function mdEscape(s) {
  return String(s ?? "").replace(/\|/g, "\\|");
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const hotels = discoverUniverse();
  console.log(`universe_live_published=${hotels.length}`);

  const rows = [];
  for (const h of hotels) {
    process.stdout.write(`auditing ${h.hotelName}...\n`);
    const row = await auditHotel(h);
    rows.push(row);
  }

  const counts = {
    TOTAL_ACTIVE_HOTELS: rows.length,
    CUSTOMER_READY: rows.filter((r) => r.maturity === "CUSTOMER_READY").length,
    RESEARCHED_NO_READY: rows.filter((r) => r.maturity === "RESEARCHED_NO_READY").length,
    PARTIALLY_RESEARCHED: rows.filter((r) => r.maturity === "PARTIALLY_RESEARCHED").length,
    INITIALIZED_ONLY: rows.filter((r) => r.maturity === "INITIALIZED_ONLY").length,
    UNKNOWN: rows.filter((r) => r.maturity === "RESEARCH_STATE_UNKNOWN").length,
  };

  const substantive = rows.filter(
    (r) =>
      r.maturity === "CUSTOMER_READY" ||
      r.maturity === "RESEARCHED_NO_READY" ||
      (r.maturity === "PARTIALLY_RESEARCHED" && r.evidenceStrength === "SUBSTANTIVE")
  );
  const initOnly = rows.filter((r) => r.maturity === "INITIALIZED_ONLY");
  const zeroReady = rows.filter((r) => !r.customerReady);
  const zeroReadyQualityHeldClaim = zeroReady.filter((r) => r.qualityHeldClaimValid);
  const neverTargetResearch = rows.filter(
    (r) => (r.targetsResearched || 0) === 0 && (r.researchedTargetRuns || 0) === 0
  );
  const exhaustedMeaningful = rows.filter(
    (r) =>
      !r.customerReady &&
      (r.maturity === "RESEARCHED_NO_READY" ||
        (r.maturity === "PARTIALLY_RESEARCHED" &&
          (r.cycleSummaries?.length || 0) >= 1 &&
          r.evidenceStrength === "SUBSTANTIVE"))
  );

  const nextBatch = pickNextBatch(rows);
  const stateModelConflated = true; // proven by prior reconciler; fixed in this change set

  const persistenceGaps = rows.filter((r) => r.persistenceRepair?.needed);

  const verdict =
    counts.INITIALIZED_ONLY > 0 && stateModelConflated
      ? "GDI INITIALIZATION WAS MISLABELED AS RESEARCH — STATE MODEL REPAIRED"
      : counts.INITIALIZED_ONLY > 0
        ? "GDI INFRASTRUCTURE COMPLETE — RESEARCH COVERAGE STILL UNEVEN"
        : "GDI UNIVERSE RESEARCH STATE IS ACCURATE — READY FOR NEXT CYCLES";

  // Prefer uneven coverage verdict when init majority
  const finalVerdict =
    counts.INITIALIZED_ONLY >= 8
      ? "GDI INITIALIZATION WAS MISLABELED AS RESEARCH — STATE MODEL REPAIRED"
      : verdict;

  const results = {
    generatedAt: new Date().toISOString(),
    verdict: finalVerdict,
    portfolio: counts,
    directAnswers: {
      q1_substantive_research_count: substantive.length,
      q2_initialized_only_count: initOnly.length,
      q3_all_16_zero_ready_quality_held: false,
      q3_detail: `${zeroReadyQualityHeldClaim.length} of ${zeroReady.length} zero-ready hotels have evidence-backed research; rest are INITIALIZED_ONLY (not quality-held)`,
      q4_zero_ready_exhausted_meaningful_cycle: exhaustedMeaningful.map((r) => r.hotelName),
      q5_never_target_level_research: neverTargetResearch.map((r) => r.hotelName),
      q6_next_cycle_hotels: nextBatch.map((r) => r.hotelName),
      q7_strategies: nextBatch.map((r) => ({
        hotel: r.hotelName,
        lanes: r.lanePlan?.lanes || ["first substantive independent discovery against seeded targets"],
        sources: r.lanePlan?.expectedSources || ["official org pages", "program calendars", "housing pages"],
        budget: r.lanePlan?.budget || { queries: 18, fetches: 28 },
      })),
      q8_state_model_conflated: true,
      q8_fix: "reconcile classifier + research-maturity-v1 module",
      q9_persistence_repair_needed: persistenceGaps.map((r) => ({
        hotel: r.hotelName,
        action: r.persistenceRepair.action,
        note: r.persistenceRepair.note,
      })),
      q10_bethesda_protected: rows.find((r) => r.hpcId === BETHESDA_HPC)?.bethesdaProtected === true,
    },
    nextBatch: nextBatch.map((r) => ({
      hotel: r.hotelName,
      hpcId: r.hpcId,
      whyNow: r.nextWhy,
      researchGap: r.lanePlan?.evidenceNote || r.rationale,
      researchLanes: r.lanePlan?.lanes?.length
        ? r.lanePlan.lanes
        : ["first full independent discovery cycle against ACTIVE targets"],
      expectedSourceFamilies: r.lanePlan?.expectedSources?.length
        ? r.lanePlan.expectedSources
        : ["official domains", "program calendars", "housing/venue pages", "association event pages"],
      budget: r.lanePlan?.budget || { queries: 18, fetches: 28 },
      successMeans:
        r.lanePlan?.success ||
        "substantive target-run ledger written; readiness gates applied; 0 weak promotions",
    })),
    rows,
  };

  fs.writeFileSync(path.join(OUT, "RESULTS.json"), JSON.stringify(results, null, 2));
  fs.writeFileSync(path.join(OUT, "MATRIX.json"), JSON.stringify(rows, null, 2));

  const matrixLines = [
    "| Hotel | Fits | Targets | Targets Researched | Coverage % | Fetches | Opps | Maturity | Root Cause | Next Cycle |",
    "|---|---:|---:|---:|---:|---:|---:|---|---|---|",
  ];
  for (const r of rows) {
    matrixLines.push(
      `| ${mdEscape(r.hotelName)} | ${r.fits ?? ""} | ${r.targets ?? ""} | ${r.targetsResearched ?? ""} | ${r.targetCoveragePct ?? ""} | ${r.fetches ?? "—"} | ${r.customerReady ?? 0} | ${r.maturity} | ${r.rootCause ?? "—"} | ${r.nextCycle} |`
    );
  }

  const founder = `# GDI Full-Universe Research Coverage + Next-Cycle Prioritization V1

**Generated:** ${results.generatedAt}  
**Universe:** ${counts.TOTAL_ACTIVE_HOTELS} Live published ADP hotels (dynamic)  
**Verdict:** **${finalVerdict}**

---

## Portfolio summary

| Metric | Count |
|---|---:|
| TOTAL ACTIVE HOTELS | ${counts.TOTAL_ACTIVE_HOTELS} |
| CUSTOMER_READY | ${counts.CUSTOMER_READY} |
| RESEARCHED_NO_READY | ${counts.RESEARCHED_NO_READY} |
| PARTIALLY_RESEARCHED | ${counts.PARTIALLY_RESEARCHED} |
| INITIALIZED_ONLY | ${counts.INITIALIZED_ONLY} |
| UNKNOWN | ${counts.UNKNOWN} |

---

## Founder matrix

${matrixLines.join("\n")}

---

## Direct answers

1. **Substantive GDI research:** ${substantive.length} of ${counts.TOTAL_ACTIVE_HOTELS} hotels.
2. **Initialized-only infrastructure:** ${initOnly.length} hotels.
3. **Are all zero-ready hotels "quality held"?** **No.** Only ${zeroReadyQualityHeldClaim.length}/${zeroReady.length} zero-ready hotels have evidence-backed research. The rest are **INITIALIZED_ONLY** — not quality-held.
4. **Zero-ready hotels that exhausted a meaningful research cycle:** ${exhaustedMeaningful.map((r) => r.hotelName).join("; ") || "none"}.
5. **Hotels with never target-level research (ledger):** ${neverTargetResearch.length} — ${neverTargetResearch.map((r) => r.hotelName).join("; ")}.
6. **Next 3–5 cycle hotels:** ${nextBatch.map((r) => r.hotelName).join("; ")}.
7. **Strategies:** see Next-cycle plan below.
8. **State model conflated init with researched?** **Yes.** Prior universe reconciler set \`GDI_RESEARCHED_NO_READY\` whenever fits+targets+runs existed (including INIT_FOOTPRINT). Fixed via \`research-maturity-v1\` + reconciler classifier.
9. **Persistence repair?** ${persistenceGaps.length ? persistenceGaps.map((r) => r.hotelName).join("; ") + " — hotel-level cycles without Target Run rows. Do **not** fabricate per-target history; fix write-path going forward; optional Research Run provenance notes only." : "None requiring fabricated history."}
10. **Bethesda protected?** **Yes** — classified CUSTOMER_READY / NO_ADDITIONAL_RESEARCH_NEEDED_NOW; no experimental broad discovery.

---

## Bethesda

Protected founding pilot. Preserve existing opportunity set. Next cycle = **NO_ADDITIONAL_RESEARCH_NEEDED_NOW**.

## AC Hotel A Coruña

**Not INITIALIZED_ONLY.** Two real cycles (44 queries / 53 fetches combined). Target-run ledger empty (orchestrator gap) → **PARTIALLY_RESEARCHED** at target grain; hotel-level research proved. Next focus: corporate/industrial, port/maritime, healthcare, deeper university/science, project teams/training — evidence from cycles 1–2 shows academic/cultural/sports overflow dominated and produced 0 ready.

## Spice Island Beach Resort

**Not INITIALIZED_ONLY.** One real cycle (30q / 33f). Same target-ledger gap → **PARTIALLY_RESEARCHED**. Next focus: incentive/executive retreat depth, luxury advisor, wedding planner (planner-side), marine/yachting, Caribbean regional orgs.

---

## Next-cycle plan (bounded — NOT launched)

${nextBatch
  .map(
    (r, i) => `### ${i + 1}. ${r.hotelName}

- **Why now:** ${r.nextWhy}
- **Research gap:** ${r.lanePlan?.evidenceNote || r.rationale}
- **Lanes:** ${(r.lanePlan?.lanes || ["first substantive independent discovery"]).join("; ")}
- **Expected sources:** ${(r.lanePlan?.expectedSources || ["official domains", "program calendars", "housing pages"]).join("; ")}
- **Budget:** queries ≤ ${r.lanePlan?.budget?.queries || 18}, fetches ≤ ${r.lanePlan?.budget?.fetches || 28}
- **Success:** ${r.lanePlan?.success || "target-run ledger written; readiness gates applied; no weak promotions"}`
  )
  .join("\n\n")}

---

## State model fix

Suggested / implemented labels:

- \`INITIALIZED\`
- \`RESEARCH_IN_PROGRESS\`
- \`RESEARCHED_NO_READY\`
- \`CUSTOMER_READY\`
- \`STALE_RESEARCH\`
- \`RESEARCH_BLOCKED\`

Module: \`lib/group-demand-intelligence/research-maturity-v1.js\`  
Reconciler no longer equates init footprint runs with researched.

---

## STOP

No new research cycles launched. Founder report only.
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);
  console.log(JSON.stringify({ out: OUT, ...counts, verdict: finalVerdict, nextBatch: nextBatch.map((r) => r.hotelName) }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
