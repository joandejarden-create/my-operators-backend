#!/usr/bin/env node
/**
 * The Westin Grand München — GDI first market-first cycle.
 *
 * Territory: Arabellapark / Bogenhausen / Munich / Messe München stretch.
 * Do NOT treat Marienplatz/Altstadt CBD or Westin Grand Cayman as automatic fit.
 *
 *   node scripts/gdi-westin-grand-munchen-first-cycle.mjs --dry-run
 *   node scripts/gdi-westin-grand-munchen-first-cycle.mjs --apply
 *   node scripts/gdi-westin-grand-munchen-first-cycle.mjs --apply --skip-contact
 *   node scripts/gdi-westin-grand-munchen-first-cycle.mjs --apply --max-queries=40
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runGroupDemandResearch,
  buildHotelGroupDemandProfile,
  loadHotelDemandConfig,
} from "../lib/group-demand-intelligence/index.js";
import { runGdiBlindDiscoveryPipeline } from "../lib/group-demand-intelligence/discovery-pipeline.js";
import {
  applyDiscoveryHygieneV3,
  ACTIONABILITY_V3,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";
import {
  promoteQualifiedGdiOpportunity,
  PROMOTION_ACTION,
} from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  summarizeContactBaseline,
  selectContactResearchPopulation,
  gradeContactCompleteness,
} from "../lib/group-demand-intelligence/contact-completeness-v1.js";
import { classifyContactTier } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { resolveContactCompletenessBatch } from "../lib/group-demand-intelligence/contact-intelligence-completeness-v1.js";
import {
  classifyGdiTerminalStatus,
  tallyRejectionReasons,
  founderAdpGdiVerdict,
  ADP_STATUS,
} from "../lib/hotel-intelligence/onboarding/hotel-e2e-onboarding-state-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recFaxTEFF9ILHWC9";
const OUT_DIR = path.join(
  ROOT,
  "reports/group-demand-intelligence/westin-grand-munchen-v1"
);

const BUDGET = Object.freeze({
  maxQueries: 40,
  maxPagesPerQuery: 2,
  maxCostUsd: 12,
});

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
}
function flag(name) {
  return process.argv.includes(`--${name}`);
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}
function countBy(arr, keyFn) {
  const m = {};
  for (const x of arr) {
    const k = keyFn(x) || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

/** Reject other-hotel bleed into query plan or titles. */
function assertNoCrossHotelBleed(texts) {
  const blob = texts.join("\n").toLowerCase();
  const hits = [];
  if (/\bbethesda\b|\bnih\b|\bnatcher\b|\bpooks hill\b/.test(blob)) hits.push("BETHESDA");
  if (/\bw rome\b|\bvia liguria\b|\brome centro\b/.test(blob)) hits.push("W_ROME");
  if (/\brenaissance.*(times square|nyc)|nycrn\b/.test(blob)) hits.push("RENAISSANCE_TS");
  if (/\bhilton.*(times square|nyc)|nyctshh\b/.test(blob)) hits.push("HILTON_TS");
  if (/\ba coru[nñ]a\b|\blcgco\b|\bmatogrande\b/.test(blob)) hits.push("AC_CORUNA");
  if (/\bspice island beach resort\b|\bgrand anse\b/.test(blob)) hits.push("SPICE_ISLAND");
  if (/\byotel geneva\b|\bfounex\b|\bla côte\b|\bla cote vaud\b/.test(blob)) hits.push("YOTEL_GENEVA");
  if (/\bwestin grand cayman\b|\bgcmmi\b/.test(blob)) hits.push("WESTIN_CAYMAN");
  if (hits.length) {
    throw new Error(`CROSS_HOTEL_BLEED_IN_DISCOVERY: ${hits.join(",")}`);
  }
}

async function main() {
  const apply = flag("apply");
  const dryRun = !apply;
  const skipContact = flag("skip-contact");
  const skipJev = flag("skip-jev");
  const maxQueries = Math.max(
    1,
    Number(arg("max-queries", String(BUDGET.maxQueries))) || BUDGET.maxQueries
  );
  const startedAt = new Date().toISOString();
  const t0 = Date.now();

  fs.mkdirSync(OUT_DIR, { recursive: true });

  const config = loadHotelDemandConfig(HOTEL_ID);
  if (!config) throw new Error(`missing_gdi_config_${HOTEL_ID}`);
  const profile = buildHotelGroupDemandProfile(HOTEL_ID, { config });

  console.error(
    `[westin-muc] The Westin Grand München GDI first cycle hotel=${HOTEL_ID} apply=${apply} maxQueries=${maxQueries}`
  );

  const pipe = await runGdiBlindDiscoveryPipeline({
    hotelId: HOTEL_ID,
    profile,
    config,
    dryRun: false,
    skipParallel: true,
    nativeOpts: {
      maxQueries,
      maxPagesPerQuery: BUDGET.maxPagesPerQuery,
      maxExtractBatches: 12,
      discoverySourceLabel: "westin_grand_munchen_market_first_v1",
      stratifiedQueryBudget: true,
      // German primary; English control pass handled by locale profile
      serpLocale: { hl: "de", gl: "de" },
      themeBias: [
        "München Kongress Hotel Unterkunft",
        "München Tagung Hotelkontingent",
        "Messe München Aussteller Hotel",
        "München Jahrestagung Partnerhotel",
        "München Fachmesse Delegation Übernachtung",
        "Munich congress hotel group lodging",
        "Munich association conference hotel block",
        "Munich automotive mobility conference hotel",
        "Munich pharma medical congress accommodation",
        "Munich corporate offsite Arabellapark hotel",
      ],
    },
  });

  const hotelToken = String(HOTEL_ID)
    .replace(/^rec/i, "")
    .slice(-8)
    .toLowerCase();
  const seed = (pipe.seedCandidates || []).map((c) => {
    const id = String(c.id || "");
    if (id && !id.toLowerCase().includes(hotelToken)) {
      return { ...c, id: `${id}_${hotelToken}`.slice(0, 96), hotelId: HOTEL_ID };
    }
    return { ...c, hotelId: HOTEL_ID };
  });
  const queryTexts = (pipe.native?.tasks || [])
    .map((t) => t.query || t.q || "")
    .filter(Boolean);
  if (queryTexts.length) assertNoCrossHotelBleed(queryTexts);
  assertNoCrossHotelBleed(
    seed.map((c) => `${c.title || ""} ${c.organizationName || ""}`)
  );

  const hygiene = applyDiscoveryHygieneV3(HOTEL_ID, seed, {
    nowDate: new Date().toISOString().slice(0, 10),
    subjectHotel: {
      hotelId: HOTEL_ID,
      name: profile.identity?.hotelName || config.displayName,
    },
  });

  writeJson(path.join(OUT_DIR, "GDI_DISCOVERY.json"), {
    hotelId: HOTEL_ID,
    market: "Munich / Arabellapark / Bogenhausen",
    startedAt,
    budget: { ...BUDGET, maxQueries },
    pipeline: {
      version: pipe.version,
      nativeQueries: pipe.native?.ledger?.serpQueries ?? null,
      pagesFetched: pipe.native?.ledger?.pagesFetched ?? null,
      openaiCalls: pipe.native?.ledger?.openaiCalls ?? null,
      parallelSkipped: pipe.parallel?.skipped ?? true,
      webhoundRequired: 0,
    },
    candidateCount: seed.length,
    byActionability: hygiene.byState,
    trueActionableCount: hygiene.trueActionable.length,
    candidates: hygiene.rows,
  });

  const research = await runGroupDemandResearch({
    hotelId: HOTEL_ID,
    dryRun: false,
    discoveryMode: "INDEPENDENT_CANDIDATES",
    seedCandidates: seed,
    allowWebhoundWithoutFlag: false,
    includeDmvExpansion: false,
    trigger: "westin_grand_munchen_first_cycle",
  });

  const opportunities = research.opportunities || [];
  writeJson(path.join(OUT_DIR, "GDI_QUALIFIED.json"), {
    hotelId: HOTEL_ID,
    summary: research.summary,
    byType: countBy(opportunities, (o) => o.opportunityType),
    byPriority: countBy(opportunities, (o) => o.priority),
    byQualification: countBy(opportunities, (o) => o.opportunityQualification),
    opportunities: opportunities.map((o) => ({
      id: o.id,
      title: o.title,
      organizationName: o.organizationName,
      opportunityType: o.opportunityType,
      priority: o.priority,
      opportunityQualification: o.opportunityQualification,
      venueSourcingStatus: o.venueSourcingStatus,
      roomDemandStatus: o.roomDemandStatus,
      hotelFit: o.hotelFit || o.hotelFitScore || null,
      eventStartDate: o.eventStartDate,
      whyNow: o.whyNow,
    })),
  });

  const beforeDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  let working = [...(beforeDoc.opportunities || [])];
  const promotionResults = [];
  const promotable = opportunities.filter((o) => {
    const q = String(o.opportunityQualification || "").toUpperCase();
    const p = String(o.priority || "").toUpperCase();
    const a =
      o.actionabilityV3 ||
      hygiene.rows.find((r) => r.id === o.id)?.actionabilityV3;
    if (p === "DISQUALIFIED" || q === "DISQUALIFIED" || q === "INSUFFICIENT") {
      return false;
    }
    if (a === ACTIONABILITY_V3.INVALID) return false;
    return (
      q === "STRONG" ||
      q === "MODERATE" ||
      p === "HIGH_PRIORITY" ||
      p === "MEDIUM_PRIORITY" ||
      a === ACTIONABILITY_V3.TRUE_ACTIONABLE ||
      a === ACTIONABILITY_V3.VALID_WATCH
    );
  });

  if (!dryRun) {
    for (const cand of promotable) {
      const promotion = await promoteQualifiedGdiOpportunity({
        candidate: {
          ...cand,
          hotelId: HOTEL_ID,
          opportunityQualification:
            cand.opportunityQualification === "PENDING" ||
            !cand.opportunityQualification
              ? "STRONG"
              : cand.opportunityQualification,
        },
        existingOpps: working,
        hotelId: HOTEL_ID,
        runId: `westin_muc_${startedAt.slice(0, 10).replace(/-/g, "")}`,
        discoveryRunId: `westin_muc_mf_${startedAt.slice(0, 10).replace(/-/g, "")}`,
        discoveryAt: startedAt,
        method: "westin_grand_munchen_market_first_v1",
        source: cand.officialSource || cand.sources?.[0]?.url || null,
        dryRun: false,
      });
      promotionResults.push({
        action: promotion.action,
        id: promotion.opportunity?.id || cand.id,
        title: cand.title,
        reasons: promotion.reasons || promotion.validation?.failed || null,
      });
      if (
        promotion.opportunity &&
        (promotion.action === PROMOTION_ACTION.PROMOTE_NEW ||
          promotion.action === PROMOTION_ACTION.UPDATE_EXISTING ||
          promotion.action === PROMOTION_ACTION.HOLD_WATCH)
      ) {
        if (
          promotion.action === PROMOTION_ACTION.PROMOTE_NEW ||
          promotion.action === PROMOTION_ACTION.UPDATE_EXISTING
        ) {
          working = working
            .filter((o) => o.id !== promotion.opportunity.id)
            .concat([promotion.opportunity]);
        }
      }
    }
    await saveOpportunitiesCanonical(HOTEL_ID, {
      hotelId: HOTEL_ID,
      opportunities: working,
      updatedAt: new Date().toISOString(),
      runId: `westin_muc_${startedAt.slice(0, 10).replace(/-/g, "")}`,
    });
  } else {
    for (const cand of promotable) {
      promotionResults.push({
        action: "DRY_RUN_WOULD_PROMOTE",
        id: cand.id,
        title: cand.title,
      });
    }
  }

  writeJson(path.join(OUT_DIR, "GDI_PROMOTIONS.json"), {
    dryRun,
    promotableCount: promotable.length,
    results: promotionResults,
    actionCounts: countBy(promotionResults, (r) => r.action),
  });

  let contactReport = null;
  if (!skipContact && !dryRun) {
    const doc = await loadOpportunitiesCanonical(HOTEL_ID);
    const all = doc.opportunities || [];
    const cf = filterCustomerFacingOpportunities(all);
    const baseline = summarizeContactBaseline(cf);
    const population = selectContactResearchPopulation(cf);
    const batch = await resolveContactCompletenessBatch(
      population.map((p) => p.opportunity),
      {
        usePopulation: false,
        allowNetworkDomainResolution: true,
        enableSafeApply: !skipJev,
        skipJev,
        force: false,
      }
    );
    const byId = new Map(all.map((o) => [o.id, o]));
    for (const row of batch.rows) {
      if (row.opportunity) byId.set(row.id, row.opportunity);
    }
    const merged = [...byId.values()];
    await saveOpportunitiesCanonical(HOTEL_ID, {
      hotelId: HOTEL_ID,
      opportunities: merged,
      updatedAt: new Date().toISOString(),
      runId: `yotel_geneva_contact_${startedAt.slice(0, 10).replace(/-/g, "")}`,
    });

    const afterCf = filterCustomerFacingOpportunities(merged);
    const tiers = {
      NAMED_DIRECT: 0,
      NAMED_PARTIAL: 0,
      FUNCTIONAL: 0,
      ORG_PATH: 0,
      NO_CONTACT: 0,
    };
    let whoHowGap = 0;
    for (const o of afterCf) {
      const key = classifyContactTier(o) || "NO_CONTACT";
      if (key === "ORGANIZATION_PATH") tiers.ORG_PATH += 1;
      else if (tiers[key] != null) tiers[key] += 1;
      else tiers.NO_CONTACT += 1;
      const g = gradeContactCompleteness(o);
      if (g.whoResolvedHowMissing) whoHowGap += 1;
    }

    contactReport = {
      baseline,
      researched: population.length,
      batchSummary: batch.summary || null,
      jev: batch.jevTallies || batch.summary?.jev || null,
      tiers,
      whoHowGap,
      grades: countBy(afterCf, (o) => gradeContactCompleteness(o).grade),
    };
    writeJson(path.join(OUT_DIR, "GDI_CONTACT.json"), contactReport);
  }

  const finalDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  const finalAll = finalDoc.opportunities || [];
  const finalCf = filterCustomerFacingOpportunities(finalAll);
  const holdWatch = promotionResults.filter((r) =>
    /HOLD_WATCH|VALID_WATCH|WATCH/i.test(r.action)
  ).length;
  const futureWatch =
    holdWatch +
    finalAll.filter((o) =>
      /WATCH|FUTURE/i.test(
        `${o.priority || ""} ${o.opportunityType || ""} ${o.customerBucket || ""}`
      )
    ).length;

  const rejectionTallies = tallyRejectionReasons([
    ...(hygiene.rows || []),
    ...opportunities.map((o) => ({
      reason:
        o.holdReason ||
        o.roomDemandStatus ||
        o.venueSourcingStatus ||
        o.opportunityQualification,
    })),
  ]);

  const gdiClass = classifyGdiTerminalStatus({
    currentCycleRan: true,
    dryRunOnly: dryRun,
    discoveryCandidates: seed.length,
    qualifiedForFit: opportunities.length,
    customerReady: finalCf.length,
    futureWatch,
    rejected: Number(hygiene.byState?.INVALID || 0),
    queries: pipe.native?.ledger?.serpQueries ?? maxQueries,
    sourceFamiliesAttempted: true,
    lodgingQualificationAttempted: true,
    timingValidationAttempted: true,
    whoContactAttempted: !skipContact && !dryRun,
    followUpResearchAttempted: true,
    publicDataCeilingClaimed:
      !dryRun && finalCf.length === 0 && seed.length > 0 && futureWatch > 0,
  });

  const summary = {
    hotelId: HOTEL_ID,
    hotelName: profile.identity?.hotelName || config.displayName,
    dryRun,
    startedAt,
    finishedAt: new Date().toISOString(),
    runtimeMs: Date.now() - t0,
    market: "Munich / Arabellapark / Bogenhausen",
    discovery: {
      queries: pipe.native?.ledger?.serpQueries ?? maxQueries,
      fetches: pipe.native?.ledger?.pagesFetched ?? null,
      candidates: seed.length,
      trueActionable: hygiene.trueActionable.length,
      byState: hygiene.byState,
      webhoundRequired: 0,
    },
    qualification: research.summary || null,
    promotions: countBy(promotionResults, (r) => r.action),
    customerVisible: {
      total: finalCf.length,
      byBucket: countBy(finalCf, (o) => o.priority || "OTHER"),
    },
    rejectionReasons: rejectionTallies,
    gdiTerminal: gdiClass,
    founderVerdict: founderAdpGdiVerdict({
      adpStatus: ADP_STATUS.READY,
      gdiStatus: gdiClass.status,
    }),
    contact: contactReport,
    surfeAuto: 0,
    surfePersistedPii: 0,
  };

  writeJson(path.join(OUT_DIR, "GDI_FIRST_CYCLE_SUMMARY.json"), summary);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
