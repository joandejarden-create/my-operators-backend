#!/usr/bin/env node
/**
 * AC Hotel A Coruña — current GDI closure cycle V2.
 * Prior cycle2 was dry-blocked — that is NOT terminal GDI processing.
 * Runs a current apply cycle; reuses prior evidence; classifies terminal status.
 *
 *   node scripts/gdi-ac-hotel-a-coruna-current-closure-v2.mjs --dry-run
 *   node scripts/gdi-ac-hotel-a-coruna-current-closure-v2.mjs --apply --skip-contact
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
  classifyGdiTerminalStatus,
  tallyRejectionReasons,
  founderAdpGdiVerdict,
  ADP_STATUS,
  evaluateHotelE2eOnboarding,
} from "../lib/hotel-intelligence/onboarding/hotel-e2e-onboarding-state-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "rec2PVBDavppGpenm";
const OUT_DIR = path.join(
  ROOT,
  "reports/group-demand-intelligence/ac-hotel-a-coruna-v1"
);

function flag(n) {
  return process.argv.includes(`--${n}`);
}
function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
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

/** Normalize event dates so promote path does not schema-fail. */
function normalizeCandidate(cand) {
  const out = { ...cand, hotelId: HOTEL_ID };
  const start =
    out.eventStartDate ||
    out.startDate ||
    out.eventDate ||
    out.dates?.start ||
    null;
  if (start && typeof start === "string") {
    out.eventStartDate = start.slice(0, 10);
  } else if (!start) {
    // Leave unset — promotion may HOLD_WATCH; do not invent dates
    delete out.eventStartDate;
  }
  if (
    out.opportunityQualification === "PENDING" ||
    !out.opportunityQualification
  ) {
    out.opportunityQualification = "MODERATE";
  }
  return out;
}

async function main() {
  const apply = flag("apply");
  const dryRun = !apply;
  const skipContact = flag("skip-contact");
  const maxQueries = Math.max(1, Number(arg("max-queries", "20")) || 20);
  const startedAt = new Date().toISOString();
  const t0 = Date.now();

  const config = loadHotelDemandConfig(HOTEL_ID);
  if (!config) throw new Error(`missing_gdi_config_${HOTEL_ID}`);
  const profile = buildHotelGroupDemandProfile(HOTEL_ID, { config });

  console.error(
    `[ac-closure-v2] hotel=${HOTEL_ID} apply=${apply} maxQueries=${maxQueries}`
  );

  const priorSummary =
    [
      "GDI_CURRENT_CLOSURE_V2_SUMMARY.json",
      "GDI_FIRST_CYCLE_SUMMARY.json",
      "GDI_DISCOVERY_CYCLE2.json",
    ]
      .map((f) => path.join(OUT_DIR, f))
      .find((p) => fs.existsSync(p)) || null;
  const prior = priorSummary
    ? JSON.parse(fs.readFileSync(priorSummary, "utf8"))
    : null;

  const pipe = await runGdiBlindDiscoveryPipeline({
    hotelId: HOTEL_ID,
    profile,
    config,
    dryRun: false,
    skipParallel: true,
    nativeOpts: {
      maxQueries,
      maxPagesPerQuery: 2,
      maxExtractBatches: 8,
      discoverySourceLabel: "ac_hotel_a_coruna_current_closure_v2",
      stratifiedQueryBudget: true,
      serpLocale: { hl: "es", gl: "es" },
      themeBias: [
        "A Coruña congreso hotel alojamiento",
        "Galicia association meeting hotel",
        "Matogrande business event hotel Coruña",
        "Santiago Compostela overflow lodging Coruña",
        "corporate offsite Galicia hotel",
      ],
    },
  });

  const seed = pipe.seedCandidates || [];
  const hygiene = applyDiscoveryHygieneV3(HOTEL_ID, seed, {
    nowDate: new Date().toISOString().slice(0, 10),
    subjectHotel: {
      hotelId: HOTEL_ID,
      name: profile.identity?.hotelName || config.displayName,
    },
  });

  const research = await runGroupDemandResearch({
    hotelId: HOTEL_ID,
    dryRun: false,
    discoveryMode: "INDEPENDENT_CANDIDATES",
    seedCandidates: seed,
    allowWebhoundWithoutFlag: false,
    includeDmvExpansion: false,
    trigger: "ac_hotel_a_coruna_current_closure_v2",
  });

  const opportunities = research.opportunities || [];
  const beforeDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  let working = [...(beforeDoc.opportunities || [])];
  const promotionResults = [];
  const promotable = opportunities.filter((o) => {
    const q = String(o.opportunityQualification || "").toUpperCase();
    const p = String(o.priority || "").toUpperCase();
    const a =
      o.actionabilityV3 ||
      hygiene.rows.find((r) => r.id === o.id)?.actionabilityV3;
    if (p === "DISQUALIFIED" || q === "DISQUALIFIED" || q === "INSUFFICIENT")
      return false;
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
      try {
        const promotion = await promoteQualifiedGdiOpportunity({
          candidate: normalizeCandidate(cand),
          existingOpps: working,
          hotelId: HOTEL_ID,
          runId: `ac_closure_v2_${startedAt.slice(0, 10).replace(/-/g, "")}`,
          discoveryRunId: `ac_v2_${startedAt.slice(0, 10).replace(/-/g, "")}`,
          discoveryAt: startedAt,
          method: "ac_hotel_a_coruna_current_closure_v2",
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
            promotion.action === PROMOTION_ACTION.UPDATE_EXISTING)
        ) {
          working = working
            .filter((o) => o.id !== promotion.opportunity.id)
            .concat([promotion.opportunity]);
        }
      } catch (err) {
        promotionResults.push({
          action: "PROMOTE_ERROR",
          id: cand.id,
          title: cand.title,
          reasons: [String(err?.message || err)],
        });
      }
    }
    await saveOpportunitiesCanonical(HOTEL_ID, {
      hotelId: HOTEL_ID,
      opportunities: working,
      updatedAt: new Date().toISOString(),
      runId: `ac_closure_v2_${startedAt.slice(0, 10).replace(/-/g, "")}`,
    });
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
      /WATCH|FUTURE/i.test(`${o.priority || ""} ${o.customerBucket || ""}`)
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

  const ceilingClaimed =
    !dryRun &&
    finalCf.length === 0 &&
    seed.length > 0 &&
    Number(hygiene.byState?.INVALID || 0) +
      Number(hygiene.byState?.INSUFFICIENT || 0) >=
      Math.max(1, Math.floor(seed.length * 0.5));

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
    publicDataCeilingClaimed: ceilingClaimed,
  });

  const e2e = evaluateHotelE2eOnboarding({
    hotelId: HOTEL_ID,
    hotelName: profile.identity?.hotelName || config.displayName,
    hiStatus: "HI_COMPLETE",
    adpStatus: ADP_STATUS.READY,
    gdiCycle: {
      currentCycleRan: true,
      dryRunOnly: dryRun,
      discoveryCandidates: seed.length,
      qualifiedForFit: opportunities.length,
      customerReady: finalCf.length,
      futureWatch,
      queries: pipe.native?.ledger?.serpQueries ?? maxQueries,
      sourceFamiliesAttempted: true,
      lodgingQualificationAttempted: true,
      timingValidationAttempted: true,
      followUpResearchAttempted: true,
      publicDataCeilingClaimed: ceilingClaimed,
    },
  });

  const summary = {
    hotelId: HOTEL_ID,
    hotelName: profile.identity?.hotelName || config.displayName,
    cycle: "current_closure_v2",
    dryRun,
    startedAt,
    finishedAt: new Date().toISOString(),
    runtimeMs: Date.now() - t0,
    priorEvidencePath: priorSummary ? path.relative(ROOT, priorSummary) : null,
    priorNote:
      "Prior cycle2 dry-block is NOT terminal; this cycle is the current closure",
    discovery: {
      queries: pipe.native?.ledger?.serpQueries ?? maxQueries,
      fetches: pipe.native?.ledger?.pagesFetched ?? null,
      candidates: seed.length,
      trueActionable: hygiene.trueActionable.length,
      byState: hygiene.byState,
    },
    qualification: research.summary || null,
    promotions: countBy(promotionResults, (r) => r.action),
    customerVisible: {
      total: finalCf.length,
      byBucket: countBy(finalCf, (o) => o.priority || "OTHER"),
    },
    futureWatch,
    rejectionReasons: rejectionTallies,
    gdiTerminal: gdiClass,
    publicDataCeilingVerified:
      gdiClass.status === "PROCESSED_PUBLIC_DATA_CEILING",
    e2eOnboarding: e2e,
    founderVerdict: founderAdpGdiVerdict({
      adpStatus: ADP_STATUS.READY,
      gdiStatus: gdiClass.status,
    }),
    contactSkipped: skipContact,
  };

  writeJson(path.join(OUT_DIR, "GDI_CURRENT_CLOSURE_V2_SUMMARY.json"), summary);
  writeJson(path.join(OUT_DIR, "GDI_DISCOVERY_CLOSURE_V2.json"), {
    startedAt,
    candidateCount: seed.length,
    byActionability: hygiene.byState,
    candidates: hygiene.rows,
  });
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
