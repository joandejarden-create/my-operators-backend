/**
 * V5 Bethesda Stage A — three-pathway live discovery only.
 * Hard ceiling $2.00 · isolated ledger · no Stage B · Surfe/PDL/Parallel OFF.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";
import { RESEARCH_SEGMENT_LABEL } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  AUTONOMOUS_DEMAND_DISCOVERY_V5,
  DISCOVERY_PATHWAY,
  DEMAND_MECHANISM_CLASS,
  V5_INCREMENTAL_PRIORITY_SEGMENTS,
  PROPOSED_V5_STAGE_A_CEILING_USD,
  generateTriPathDiscoveryStrategies,
  qualifyV5DiscoveryCandidate,
  buildV5DiscoveryFunnelReport,
  buildSourceFamilyStrategy,
  loadHotelGdiConfig,
} from "./autonomous-demand-discovery-v5.js";
import { REPORTS_DIR as V5_REPORTS_DIR } from "./autonomous-demand-discovery-v5-bethesda.js";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  preflightLiveDeepResearchPricing,
  maybeExaSearch,
} from "./deep-research-gdi-live-certification-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
  assertBethesdaReconciliationGate,
  runBethesdaDatabaseReconciliationPreflight,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import {
  WEEKLY_FINDING_CLASS,
  buildBethesdaImmutableBaselineManifest,
} from "./bethesda-weekly-net-new-discovery-v1.js";
import {
  parseStageAHitIdentity,
  isWeakOrGenericCandidate,
} from "./bethesda-full-spectrum-stage-a-live-v1.js";

export const V5_STAGE_A_LIVE_V1 = "gdi-autonomous-demand-discovery-v5-stage-a-live";
export const V5_STAGE_A_CEILING_USD = PROPOSED_V5_STAGE_A_CEILING_USD; // 2.0

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LIVE_DIR = path.join(ROOT, V5_REPORTS_DIR, "live-stage-a");
const LEDGER_DIR = path.join(LIVE_DIR, "ledgers");
const STAGE_A_LEDGER = path.join(LEDGER_DIR, "v5-bethesda-three-pathway-discovery.json");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 12);
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}

function ledgerSpent(ledgerPath) {
  if (!fs.existsSync(ledgerPath)) return 0;
  return Number(JSON.parse(fs.readFileSync(ledgerPath, "utf8")).estimatedSpentUsd || 0);
}

function ledgerRemaining(ledgerPath, ceiling) {
  return Number((ceiling - ledgerSpent(ledgerPath)).toFixed(6));
}

/** Prefer underrepresented segments, then balance pathways. */
export function selectV5StageAStrategies(strategyPack, { maxStrategies = 54 } = {}) {
  const byKey = new Map();
  for (const s of strategyPack.strategies || []) {
    const key = `${s.pathway}::${s.questionFingerprint || s.researchQuestion}`;
    if (!byKey.has(key)) byKey.set(key, s);
  }
  const all = [...byKey.values()];
  const selected = [];
  const seen = new Set();
  const add = (s) => {
    if (!s || selected.length >= maxStrategies) return;
    const key = `${s.pathway}::${s.questionFingerprint || s.researchQuestion}`;
    if (seen.has(key)) return;
    seen.add(key);
    selected.push(s);
  };

  // Round 1: every pathway × every segment gets one if available
  for (const pathway of Object.values(DISCOVERY_PATHWAY)) {
    for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
      add(all.find((s) => s.pathway === pathway && s.segment === seg));
    }
  }
  // Round 2: underrepresented extras
  for (const seg of V5_INCREMENTAL_PRIORITY_SEGMENTS) {
    for (const pathway of Object.values(DISCOVERY_PATHWAY)) {
      for (const s of all.filter((x) => x.pathway === pathway && x.segment === seg)) add(s);
    }
  }
  for (const s of all) add(s);
  return selected.slice(0, maxStrategies);
}

export function preflightV5StageALive({ founderAuthorized = false } = {}) {
  const pricing = preflightLiveDeepResearchPricing();
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const recon = runBethesdaDatabaseReconciliationPreflight();
  const reconGate = assertBethesdaReconciliationGate(recon, {
    allowPaidResearch: founderAuthorized,
    founderAuthorizedPaid: founderAuthorized,
  });
  const baseline = buildBethesdaImmutableBaselineManifest({ reconciliation: recon });
  const hotel = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
  };
  const pack = generateTriPathDiscoveryStrategies(hotel);
  const selected = selectV5StageAStrategies(pack);

  let ledgerFresh = true;
  if (fs.existsSync(STAGE_A_LEDGER)) {
    const led = JSON.parse(fs.readFileSync(STAGE_A_LEDGER, "utf8"));
    if (Number(led.estimatedSpentUsd || 0) > 0) ledgerFresh = false;
  }

  const checks = {
    founderAuthorized: founderAuthorized === true,
    pricingOk: pricing.ok === true,
    paidContactGuardOk: paidGuard.ok === true,
    reconciliationGateOk: reconGate.ok === true,
    baselineImmutable: baseline.immutable === true,
    allElevenStrategies: pack.allElevenSegmentsCovered === true,
    threePathwaysPresent: Object.values(DISCOVERY_PATHWAY).every((p) => (pack.byPathway[p] || 0) > 0),
    selectedStrategyCount: selected.length,
    stageACeilingUsd: V5_STAGE_A_CEILING_USD,
    ledgerPath: STAGE_A_LEDGER.replace(/\\/g, "/"),
    ledgerFreshOrEmpty: ledgerFresh,
    reusePriorBalances: false,
    stageBDisabled: true,
    surfePdlParallelOff: true,
    airtableOff: true,
    weeklyOff: true,
    publicationOff: true,
  };

  const ok = checks.founderAuthorized
    && checks.pricingOk
    && checks.paidContactGuardOk
    && checks.reconciliationGateOk
    && checks.baselineImmutable
    && checks.allElevenStrategies
    && checks.threePathwaysPresent
    && checks.ledgerFreshOrEmpty;

  return {
    ok,
    checks,
    strategyCount: selected.length,
    byPathway: pack.byPathway,
    hotel,
    pack,
    selected,
    baseline,
  };
}

function buildStageBQuestions(c = {}) {
  return [
    `What is the specific dated programme or recurring demand mechanism for ${c.organizationName}?`,
    `Is demand ${c.mechanismClass || "UNVERIFIED"} — what evidence upgrades or falsifies it?`,
    `Could Bethesda Marriott (${c.fitClass || "fit TBD"}) compete — capacity, geography, timing?`,
    `Who controls hotel/venue/meeting sourcing at ${c.organizationName}?`,
    `What legitimate contact route reaches that buying process (named or functional)?`,
    `What should Bethesda sales do next without inventing an RFP or room block?`,
  ];
}

function scoreStageB(c = {}) {
  let score = 0;
  const reasons = [];
  if (!c.countsAsNetNewOpportunity) return { score: 0, reasons: ["not_net_new_opportunity"] };
  if (c.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND) {
    score += 28;
    reasons.push("verified_demand");
  } else if (c.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND) {
    score += 14;
    reasons.push("inferred_demand");
  } else {
    return { score: 0, reasons: ["unsupported_demand"] };
  }
  if (/CAPTURABLE_CORE/i.test(String(c.fitClass || ""))) {
    score += 22;
    reasons.push("core_fit");
  } else if (/CAPTURABLE/i.test(String(c.fitClass || ""))) {
    score += 14;
    reasons.push("dmv_or_account_fit");
  }
  if (c.pathway === DISCOVERY_PATHWAY.ORGANIZATION_LED || c.pathway === DISCOVERY_PATHWAY.TRIGGER_LED) {
    score += 6;
    reasons.push("non_event_pathway_diversity");
  }
  if (c.eventYear && Number(c.eventYear) >= 2026) {
    score += 8;
    reasons.push("forward_looking");
  }
  if (V5_INCREMENTAL_PRIORITY_SEGMENTS.includes(c.segment)) {
    score += 3;
    reasons.push("underrepresented_segment");
  }
  return { score, reasons };
}

/**
 * Execute V5 Stage A live discovery under founder GO.
 */
export async function executeV5StageALive({ founderAuthorized = false } = {}) {
  const pre = preflightV5StageALive({ founderAuthorized });
  if (!pre.ok) {
    return { executed: false, reason: "preflight_failed", preflight: pre };
  }

  fs.mkdirSync(LEDGER_DIR, { recursive: true });
  assertLiveDeepResearchLedger(STAGE_A_LEDGER);
  loadOrCreateLiveDeepResearchLedger(STAGE_A_LEDGER, {
    budgetUsd: V5_STAGE_A_CEILING_USD,
    version: `${V5_STAGE_A_LIVE_V1}-discovery-ledger`,
  });

  const { hotel, selected, baseline } = pre;
  const searchLog = [];
  const stopReasons = [];
  const rawHits = [];

  for (const strategy of selected) {
    const remaining = ledgerRemaining(STAGE_A_LEDGER, V5_STAGE_A_CEILING_USD);
    if (remaining < 0.004) {
      stopReasons.push("budget_exhausted");
      break;
    }
    const spentBefore = ledgerSpent(STAGE_A_LEDGER);
    const exa = await maybeExaSearch(strategy.researchQuestion, STAGE_A_LEDGER);
    const spentAfter = ledgerSpent(STAGE_A_LEDGER);
    const costUsd = Number((spentAfter - spentBefore).toFixed(6));
    const hits = (exa.hits || []).map((h) => ({
      ...h,
      pathway: strategy.pathway,
      segment: strategy.segment,
      researchQuestion: strategy.researchQuestion,
      sourceTypes: strategy.sourceTypes || [],
    }));
    searchLog.push({
      pathway: strategy.pathway,
      segment: strategy.segment,
      researchQuestion: strategy.researchQuestion,
      sourceTypes: strategy.sourceTypes || [],
      status: exa.status,
      costUsd,
      hitCount: hits.length,
    });
    rawHits.push(...hits);
  }

  const assessed = [];
  for (const hit of rawHits) {
    const parsed = parseStageAHitIdentity(hit, hit.segment);
    if (isWeakOrGenericCandidate(parsed)) {
      assessed.push({
        candidateId: `v5sa_${hashKey(hit.url || hit.title)}`,
        pathway: hit.pathway,
        segment: hit.segment,
        organizationName: parsed.organizationName,
        programName: parsed.programName,
        sourceUrl: hit.url,
        demandSignalSummary: parsed.demandSignalSummary,
        weeklyClass: WEEKLY_FINDING_CLASS.EXCLUDED,
        countsAsNetNewOpportunity: false,
        candidateKind: "EXCLUDE",
        rejectReasons: ["generic_or_weak_identity"],
        mechanismClass: DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
        falsePositiveCategory: "WEAK_IDENTITY",
      });
      continue;
    }

    const candidate = {
      pathway: hit.pathway,
      segment: hit.segment,
      organizationName: parsed.organizationName,
      programName: parsed.programName,
      demandMechanism: parsed.programName,
      triggerSummary: hit.pathway === DISCOVERY_PATHWAY.TRIGGER_LED
        ? `${parsed.programName || ""} ${parsed.demandSignalSummary || ""}`
        : null,
      demandSignalSummary: parsed.demandSignalSummary,
      sourceUrl: hit.url || hit.sourceUrl,
      eventYear: parsed.eventYear,
      researchQuestion: hit.researchQuestion,
      publisher: hit.publisher,
    };

    const q = qualifyV5DiscoveryCandidate(candidate, hotel, baseline);
    const row = {
      candidateId: `v5sa_${hashKey(`${hit.pathway}::${parsed.organizationName}::${parsed.programName}::${hit.url}`)}`,
      pathway: hit.pathway,
      segment: hit.segment,
      organizationName: parsed.organizationName,
      programName: parsed.programName,
      demandMechanism: q.mechanism?.demandMechanismSummary || parsed.programName,
      mechanismClass: q.mechanism?.mechanismClass || DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
      sourceUrl: hit.url,
      publisher: hit.publisher,
      demandSignalSummary: parsed.demandSignalSummary,
      eventYear: parsed.eventYear,
      geographyHint: parsed.geoHintRelevant ? "DMV_OR_LOCAL_HINT" : "GEO_UNVERIFIED",
      fitClass: q.fit?.fitClass || null,
      candidateKind: q.candidateKind,
      accepted: q.accepted,
      rejectReasons: q.rejectReasons || [],
      weeklyClass: q.weeklyClass,
      countsAsNetNewOpportunity: q.countsAsNetNewOpportunity === true,
      countsAsNetNew: q.countsAsNetNewOpportunity === true,
      deepResearchEligible: q.deepResearchEligible === true,
      stageBEligible: false,
      hotelFacingPublication: false,
      researchQuestion: hit.researchQuestion,
      sourceTypes: hit.sourceTypes || [],
      potentialRevenueStreams: q.mechanism?.potentialRevenueStreams || [],
      sourcingStatusHint: q.mechanism?.sourcingStatusHint || null,
      unansweredQuestions: buildStageBQuestions({
        organizationName: parsed.organizationName,
        fitClass: q.fit?.fitClass,
        mechanismClass: q.mechanism?.mechanismClass,
      }),
      falsePositiveCategory: (q.rejectReasons || [])[0] || null,
      underrepresentedSegment: V5_INCREMENTAL_PRIORITY_SEGMENTS.includes(hit.segment),
      verifiedVsInferred: q.mechanism?.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
        ? "VERIFIED_DEMAND"
        : (q.mechanism?.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND
          ? "INFERRED_DEMAND"
          : "UNSUPPORTED_DEMAND"),
    };

    const rank = scoreStageB(row);
    row.stageBScore = rank.score;
    row.stageBScoreReasons = rank.reasons;
    row.stageBEligible = row.countsAsNetNewOpportunity
      && row.deepResearchEligible
      && row.mechanismClass !== DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND
      && rank.score >= 40;
    row.recommendedStageBBudgetUsd = row.stageBEligible
      ? (rank.score >= 60 ? 0.45 : 0.35)
      : 0;
    row.stageBResearchQuestions = buildStageBQuestions(row);
    assessed.push(row);
  }

  // Dedupe by org+program+pathway
  const seen = new Set();
  const unique = [];
  for (const row of assessed) {
    const key = `${text(row.organizationName).toLowerCase()}::${text(row.programName).toLowerCase()}::${row.pathway}`;
    if (seen.has(key) && row.countsAsNetNewOpportunity) {
      row.weeklyClass = WEEKLY_FINDING_CLASS.DUPLICATE;
      row.countsAsNetNewOpportunity = false;
      row.countsAsNetNew = false;
      row.stageBEligible = false;
      row.rejectReasons = [...(row.rejectReasons || []), "duplicate_in_stage_a_batch"];
      row.falsePositiveCategory = "DUPLICATE";
    }
    if (row.organizationName) seen.add(key);
    unique.push(row);
  }

  const spent = ledgerSpent(STAGE_A_LEDGER);
  const netNew = unique.filter((r) => r.countsAsNetNewOpportunity);
  const stageBRanked = [...unique]
    .filter((r) => r.stageBEligible)
    .sort((a, b) => (b.stageBScore || 0) - (a.stageBScore || 0));

  const pathwaySpend = {};
  for (const p of Object.values(DISCOVERY_PATHWAY)) {
    pathwaySpend[p] = Number(
      searchLog.filter((s) => s.pathway === p).reduce((a, s) => a + Number(s.costUsd || 0), 0).toFixed(6),
    );
  }

  const funnel = buildV5DiscoveryFunnelReport({
    hits: rawHits,
    qualifications: unique.map((r) => ({
      pathway: r.pathway,
      segment: r.segment,
      countsAsNetNewOpportunity: r.countsAsNetNewOpportunity,
      mechanism: { mechanismClass: r.mechanismClass },
      fit: { fitClass: r.fitClass },
      deepResearchEligible: r.deepResearchEligible,
      normalized: { discoveryId: r.candidateId, organizationName: r.organizationName, segment: r.segment },
      candidateKind: r.candidateKind,
    })),
    pathwaySpendUsd: pathwaySpend,
    deepResearchedIds: [],
    buyerIdentifiedIds: [],
    contactableIds: [],
    hotelFacingIds: [],
    actionableNetNewIds: [],
  });

  // Enrich pathway funnel with Stage B eligible + false positives + named candidates
  const pathwayCompare = {};
  for (const p of Object.values(DISCOVERY_PATHWAY)) {
    const searches = searchLog.filter((s) => s.pathway === p);
    const rows = unique.filter((r) => r.pathway === p);
    const credible = rows.filter((r) =>
      r.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
      || r.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND);
    const net = rows.filter((r) => r.countsAsNetNewOpportunity);
    const stageB = rows.filter((r) => r.stageBEligible);
    const spendP = pathwaySpend[p] || 0;
    pathwayCompare[p] = {
      searchesPerformed: searches.length,
      rawResults: rows.length,
      validOrganizationsActivities: rows.filter((r) => r.organizationName && r.candidateKind !== "EXCLUDE").length,
      distinctNetNew: net.length,
      credibleDemandMechanisms: credible.length,
      bethesdaFit: rows.filter((r) => /CAPTURABLE/i.test(String(r.fitClass || ""))).length,
      stageBEligible: stageB.length,
      duplicates: rows.filter((r) => r.weeklyClass === WEEKLY_FINDING_CLASS.DUPLICATE).length,
      falsePositives: rows.filter((r) => r.falsePositiveCategory || (!r.accepted && r.rejectReasons?.length)).length,
      spendUsd: spendP,
      costPerCredibleNetNew: net.filter((r) =>
        r.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
        || r.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND).length
        ? Number((spendP / Math.max(1, net.filter((r) =>
          r.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
          || r.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND).length)).toFixed(4))
        : "UNDEFINED_ZERO_DENOMINATOR",
      costPerStageBEligible: stageB.length
        ? Number((spendP / stageB.length).toFixed(4))
        : "UNDEFINED_ZERO_DENOMINATOR",
      candidateNames: rows.filter((r) => r.organizationName).slice(0, 40).map((r) => ({
        organization: r.organizationName,
        program: r.programName,
        mechanism: r.mechanismClass,
        kind: r.candidateKind,
        netNew: r.countsAsNetNewOpportunity,
        stageB: r.stageBEligible,
        reject: r.rejectReasons,
        url: r.sourceUrl,
      })),
    };
  }

  const segmentCoverage = {};
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    const searches = searchLog.filter((s) => s.segment === seg);
    const rows = unique.filter((r) => r.segment === seg);
    segmentCoverage[seg] = {
      label: RESEARCH_SEGMENT_LABEL[seg] || seg,
      searchesPerformed: searches.length,
      pathways: [...new Set(searches.map((s) => s.pathway))],
      candidates: rows.length,
      netNew: rows.filter((r) => r.countsAsNetNewOpportunity).length,
      stageBEligible: rows.filter((r) => r.stageBEligible).length,
      candidateNames: rows.filter((r) => r.countsAsNetNewOpportunity || r.stageBEligible).map((r) => ({
        organization: r.organizationName,
        program: r.programName,
        pathway: r.pathway,
        mechanism: r.mechanismClass,
      })),
    };
  }

  // Write deliverables
  writeJson(path.join(LIVE_DIR, "01-provider-and-spending-ledger.json"), {
    ledgerPath: STAGE_A_LEDGER.replace(/\\/g, "/"),
    ceilingUsd: V5_STAGE_A_CEILING_USD,
    spentUsd: spent,
    remainingUsd: ledgerRemaining(STAGE_A_LEDGER, V5_STAGE_A_CEILING_USD),
    isolatedFromPriorAuthorizations: true,
    reusePriorBalances: false,
    providersUsed: ["exa"],
    providersForbidden: ["surfe", "pdl", "parallel"],
    stageBNotExecuted: true,
    pathwaySpendUsd: pathwaySpend,
    ledger: fs.existsSync(STAGE_A_LEDGER)
      ? JSON.parse(fs.readFileSync(STAGE_A_LEDGER, "utf8"))
      : null,
  });
  writeJson(path.join(LIVE_DIR, "02-three-pathway-execution-traces.json"), {
    strategiesSelected: selected.length,
    sourceFamilyStrategy: buildSourceFamilyStrategy(),
    searchLog,
    stopReasons,
  });
  writeJson(path.join(LIVE_DIR, "03-eleven-segment-coverage.json"), segmentCoverage);
  writeJson(path.join(LIVE_DIR, "04-complete-candidate-inventory.json"), {
    count: unique.length,
    candidates: unique,
  });
  writeJson(path.join(LIVE_DIR, "05-canonical-net-new-reconciliation.json"), {
    baselineDistinctOpportunities: baseline.counts?.distinctCanonicalOpportunities,
    byClass: Object.fromEntries(
      Object.values(WEEKLY_FINDING_CLASS).map((c) => [
        c,
        unique.filter((r) => r.weeklyClass === c).length,
      ]),
    ),
    netNewOpportunities: netNew.map((r) => ({
      candidateId: r.candidateId,
      organizationName: r.organizationName,
      programName: r.programName,
      pathway: r.pathway,
      segment: r.segment,
      mechanismClass: r.mechanismClass,
      sourceUrl: r.sourceUrl,
    })),
    note: "Account intelligence without demand mechanism is not counted as NET_NEW opportunity yield",
  });
  writeJson(path.join(LIVE_DIR, "06-demand-mechanism-evidence.json"), {
    byClass: {
      VERIFIED_DEMAND: unique.filter((r) => r.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND).length,
      INFERRED_DEMAND: unique.filter((r) => r.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND).length,
      UNSUPPORTED_DEMAND: unique.filter((r) => r.mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND).length,
    },
    items: unique.filter((r) => r.organizationName).map((r) => ({
      organizationName: r.organizationName,
      demandMechanism: r.demandMechanism,
      mechanismClass: r.mechanismClass,
      pathway: r.pathway,
      evidence: (r.demandSignalSummary || "").slice(0, 300),
      sourceUrl: r.sourceUrl,
    })),
  });
  writeJson(path.join(LIVE_DIR, "07-bethesda-hotel-fit-analysis.json"), {
    items: unique.map((r) => ({
      organizationName: r.organizationName,
      fitClass: r.fitClass,
      pathway: r.pathway,
      stageBEligible: r.stageBEligible,
      geographyHint: r.geographyHint,
    })),
  });
  writeJson(path.join(LIVE_DIR, "08-candidate-level-false-positive-audit.json"), {
    excluded: unique.filter((r) => !r.countsAsNetNewOpportunity).map((r) => ({
      organizationName: r.organizationName,
      programName: r.programName,
      pathway: r.pathway,
      falsePositiveCategory: r.falsePositiveCategory,
      rejectReasons: r.rejectReasons,
      segment: r.segment,
    })),
  });
  const orgTrigStronger = (pathwayCompare[DISCOVERY_PATHWAY.ORGANIZATION_LED].stageBEligible
    + pathwayCompare[DISCOVERY_PATHWAY.TRIGGER_LED].stageBEligible)
    > pathwayCompare[DISCOVERY_PATHWAY.EVENT_LED].stageBEligible;
  const orgTrigProducedAny = (pathwayCompare[DISCOVERY_PATHWAY.ORGANIZATION_LED].distinctNetNew
    + pathwayCompare[DISCOVERY_PATHWAY.TRIGGER_LED].distinctNetNew) > 0;

  writeJson(path.join(LIVE_DIR, "09-pathway-comparison-and-economics.json"), {
    pathwayCompare,
    funnelSpec: funnel,
    orgTriggerVsEvent: {
      orgNetNew: pathwayCompare[DISCOVERY_PATHWAY.ORGANIZATION_LED]?.distinctNetNew || 0,
      triggerNetNew: pathwayCompare[DISCOVERY_PATHWAY.TRIGGER_LED]?.distinctNetNew || 0,
      eventNetNew: pathwayCompare[DISCOVERY_PATHWAY.EVENT_LED]?.distinctNetNew || 0,
      orgStageB: pathwayCompare[DISCOVERY_PATHWAY.ORGANIZATION_LED]?.stageBEligible || 0,
      triggerStageB: pathwayCompare[DISCOVERY_PATHWAY.TRIGGER_LED]?.stageBEligible || 0,
      eventStageB: pathwayCompare[DISCOVERY_PATHWAY.EVENT_LED]?.stageBEligible || 0,
      orgTriggerProducedAnyNetNew: orgTrigProducedAny,
      orgTriggerStageBExceedsEvent: orgTrigStronger,
    },
  });

  writeJson(path.join(LIVE_DIR, "10-ranked-stage-b-shortlist.json"), {
    autoAdvanceToStageB: false,
    ranked: stageBRanked.slice(0, 15).map((r, i) => ({
      rank: i + 1,
      candidateId: r.candidateId,
      organizationName: r.organizationName,
      programName: r.programName,
      pathway: r.pathway,
      segment: r.segment,
      mechanismClass: r.mechanismClass,
      fitClass: r.fitClass,
      stageBScore: r.stageBScore,
      recommendedStageBBudgetUsd: r.recommendedStageBBudgetUsd,
      sourceUrl: r.sourceUrl,
      demandEvidence: (r.demandSignalSummary || "").slice(0, 400),
    })),
  });
  writeJson(path.join(LIVE_DIR, "11-stage-b-research-questions.json"), {
    byCandidate: stageBRanked.slice(0, 12).map((r) => ({
      candidateId: r.candidateId,
      organizationName: r.organizationName,
      pathway: r.pathway,
      questions: r.stageBResearchQuestions,
    })),
  });
  writeJson(path.join(LIVE_DIR, "12-buyer-contact-intelligence-gaps.json"), {
    note: "Stage A does not run paid buyer enrichment; gaps prepared for Stage B",
    gaps: stageBRanked.slice(0, 12).map((r) => ({
      organizationName: r.organizationName,
      missing: [
        "named sourcing decision-maker",
        "verified functional buyer route beyond page publisher",
        "confirmation of open sourcing window",
      ],
      pathway: r.pathway,
    })),
  });
  writeJson(path.join(LIVE_DIR, "13-recommended-stage-b-budgets.json"), {
    allocations: stageBRanked.slice(0, 10).map((r) => ({
      organizationName: r.organizationName,
      pathway: r.pathway,
      recommendedUsd: r.recommendedStageBBudgetUsd,
      score: r.stageBScore,
    })),
    sumRecommendedUsd: Number(
      stageBRanked.slice(0, 10).reduce((a, r) => a + Number(r.recommendedStageBBudgetUsd || 0), 0).toFixed(4),
    ),
    note: "Requires separate founder GO — Stage A remaining budget is not transferable",
  });

  const mostlyGeneric = netNew.length === 0
    || (orgTrigProducedAny === false
      && unique.filter((r) =>
        (r.pathway === DISCOVERY_PATHWAY.ORGANIZATION_LED || r.pathway === DISCOVERY_PATHWAY.TRIGGER_LED)
        && r.mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND).length
        > unique.filter((r) =>
          r.pathway === DISCOVERY_PATHWAY.ORGANIZATION_LED
          || r.pathway === DISCOVERY_PATHWAY.TRIGGER_LED).length * 0.7);

  let decision = "NO_GO_STAGE_B_INSUFFICIENT_QUALITY";
  if (stageBRanked.length >= 3) decision = "CONDITIONAL_GO_STAGE_B_AFTER_FOUNDER_REVIEW";
  else if (stageBRanked.length >= 1) decision = "CONDITIONAL_GO_LIMITED_STAGE_B_AFTER_REVIEW";
  if (mostlyGeneric && stageBRanked.length === 0) {
    decision = "NO_GO_DISCOVERY_FAILURE_GENERIC_PROFILES";
  }

  writeJson(path.join(LIVE_DIR, "14-commercial-go-nogo-assessment.json"), {
    decision,
    autoAdvance: false,
    stageBNotExecuted: true,
    stageASuccessNotDefinedByStrategyCount: true,
    strategiesExecuted: searchLog.filter((s) => s.status === "ok").length,
    genuineNetNewOpportunities: netNew.length,
    stageBEligible: stageBRanked.length,
    orgTriggerProducedCommerciallyStrongerAlternatives: orgTrigStronger,
    honestFailureIfGeneric: mostlyGeneric && stageBRanked.length === 0,
    rationale: [
      `Spent $${spent} of $${V5_STAGE_A_CEILING_USD}`,
      `NET_NEW opportunities (mechanism-gated): ${netNew.length}`,
      `Stage B eligible: ${stageBRanked.length}`,
      `Org-led NET_NEW: ${pathwayCompare.ORGANIZATION_LED.distinctNetNew}; Trigger-led: ${pathwayCompare.TRIGGER_LED.distinctNetNew}; Event-led: ${pathwayCompare.EVENT_LED.distinctNetNew}`,
    ],
    doNot: [
      "execute Stage B without new authorization",
      "count Stage A as hotel-facing actionable",
      "publish or write Airtable",
      "activate weekly production",
      "transfer remaining Stage A budget to Stage B",
    ],
  });

  const summary = {
    version: V5_STAGE_A_LIVE_V1,
    parentVersion: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    executed: true,
    completedAt: new Date().toISOString(),
    spentUsd: spent,
    ceilingUsd: V5_STAGE_A_CEILING_USD,
    strategiesAttempted: searchLog.length,
    strategiesOk: searchLog.filter((s) => s.status === "ok").length,
    grossHits: rawHits.length,
    assessedCandidates: unique.length,
    genuineNetNewOpportunities: netNew.length,
    stageBEligible: stageBRanked.length,
    byPathwayNetNew: {
      ORGANIZATION_LED: pathwayCompare.ORGANIZATION_LED.distinctNetNew,
      TRIGGER_LED: pathwayCompare.TRIGGER_LED.distinctNetNew,
      EVENT_LED: pathwayCompare.EVENT_LED.distinctNetNew,
    },
    stageBDecision: decision,
    stageBExecuted: false,
    hotelFacingActionableClaimed: 0,
    liveDir: LIVE_DIR.replace(/\\/g, "/"),
    stopReasons,
  };
  writeJson(path.join(LIVE_DIR, "SUMMARY.json"), summary);

  writeMd(path.join(LIVE_DIR, "DELIVERABLES_LIVE.md"), `# V5 Bethesda Three-Pathway Stage A — Live Discovery

**Spend:** $${spent} / $${V5_STAGE_A_CEILING_USD} · **Strategies OK:** ${summary.strategiesOk}/${summary.strategiesAttempted}  
**NET_NEW opportunities (mechanism-gated):** ${netNew.length} · **Stage B eligible:** ${stageBRanked.length}  
**By pathway NET_NEW:** org ${pathwayCompare.ORGANIZATION_LED.distinctNetNew} · trigger ${pathwayCompare.TRIGGER_LED.distinctNetNew} · event ${pathwayCompare.EVENT_LED.distinctNetNew}  
**Stage B:** NOT executed · Decision: **${decision}**  
**Hotel-facing actionable claimed:** 0

## Stage B shortlist

${stageBRanked.length
    ? stageBRanked.slice(0, 12).map((r, i) => `${i + 1}. **${r.organizationName}** — ${r.programName} (\`${r.pathway}\` / \`${r.segment}\`) · ${r.mechanismClass} · score ${r.stageBScore}`).join("\n")
    : "_None — insufficient commercially gated NET_NEW._"}

## Pathway economics (spend → Stage B)

| Pathway | Searches | NET_NEW | Stage B | Spend | $/StageB |
|---------|----------|---------|---------|-------|----------|
| ORGANIZATION_LED | ${pathwayCompare.ORGANIZATION_LED.searchesPerformed} | ${pathwayCompare.ORGANIZATION_LED.distinctNetNew} | ${pathwayCompare.ORGANIZATION_LED.stageBEligible} | $${pathwayCompare.ORGANIZATION_LED.spendUsd} | ${pathwayCompare.ORGANIZATION_LED.costPerStageBEligible} |
| TRIGGER_LED | ${pathwayCompare.TRIGGER_LED.searchesPerformed} | ${pathwayCompare.TRIGGER_LED.distinctNetNew} | ${pathwayCompare.TRIGGER_LED.stageBEligible} | $${pathwayCompare.TRIGGER_LED.spendUsd} | ${pathwayCompare.TRIGGER_LED.costPerStageBEligible} |
| EVENT_LED | ${pathwayCompare.EVENT_LED.searchesPerformed} | ${pathwayCompare.EVENT_LED.distinctNetNew} | ${pathwayCompare.EVENT_LED.stageBEligible} | $${pathwayCompare.EVENT_LED.spendUsd} | ${pathwayCompare.EVENT_LED.costPerStageBEligible} |

## NET_NEW names

${netNew.length
    ? netNew.map((r, i) => `${i + 1}. **${r.organizationName}** — ${r.programName} (\`${r.pathway}\`) · ${r.mechanismClass}`).join("\n")
    : "_None after mechanism + baseline gates._"}
`);

  return summary;
}
