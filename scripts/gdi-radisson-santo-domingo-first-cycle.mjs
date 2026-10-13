#!/usr/bin/env node
/**
 * AC Hotel A CoruÃ±a â€” GDI first cycle.
 *
 * Open-universe native discovery â†’ qualify â†’ promote â†’ contact completeness (Jev SAFE APPLY).
 * Webhound OFF. Surfe auto 0. No Bethesda/NYC/Rome/Spice Island target reuse.
 * Spanish-language SERP (hl=es, gl=es) for Galicia market sources.
 *
 *   node scripts/gdi-radisson-santo-domingo-first-cycle.mjs --dry-run
 *   node scripts/gdi-radisson-santo-domingo-first-cycle.mjs --apply
 *   node scripts/gdi-radisson-santo-domingo-first-cycle.mjs --apply --skip-contact
 *   node scripts/gdi-radisson-santo-domingo-first-cycle.mjs --apply --max-queries=40
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
  CONTACT_GRADE,
} from "../lib/group-demand-intelligence/contact-completeness-v1.js";
import { classifyContactTier } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { resolveContactCompletenessBatch } from "../lib/group-demand-intelligence/contact-intelligence-completeness-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recUOyzOXn2Zdp98I";
const OUT_DIR = path.join(
  ROOT,
  "reports/group-demand-intelligence/radisson-santo-domingo-v1"
);

const BUDGET = Object.freeze({
  maxQueries: 40,
  maxPagesPerQuery: 2,
  maxCostUsd: 10,
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
  if (/\bspice island beach resort\b|\bgrand anse\b/.test(blob)) hits.push("SPICE_ISLAND");
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
    `[radisson-sd] AC Hotel A CoruÃ±a GDI first cycle hotel=${HOTEL_ID} apply=${apply} maxQueries=${maxQueries}`
  );

  // --- Phase A: open-universe native discovery (Spanish/English SERP locale for Dominican Republic) ---
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
      discoverySourceLabel: "radisson_santo_domingo_open_universe_v1",
      stratifiedQueryBudget: true,
      // Dominican Republic market locale (Spanish primary).
      serpLocale: { hl: "es", gl: "do" },
    },
  });

  const seed = pipe.seedCandidates || [];
  const queryTexts = (pipe.native?.tasks || []).map((t) => t.query || t.q || "").filter(Boolean);
  if (queryTexts.length) assertNoCrossHotelBleed(queryTexts);
  assertNoCrossHotelBleed(seed.map((c) => `${c.title || ""} ${c.organizationName || ""}`));

  const hygiene = applyDiscoveryHygieneV3(HOTEL_ID, seed, {
    nowDate: new Date().toISOString().slice(0, 10),
    subjectHotel: {
      hotelId: HOTEL_ID,
      name: profile.identity?.hotelName || config.displayName,
    },
  });

  writeJson(path.join(OUT_DIR, "GDI_DISCOVERY.json"), {
    hotelId: HOTEL_ID,
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

  // --- Phase B: qualify via research orchestrator ---
  const research = await runGroupDemandResearch({
    hotelId: HOTEL_ID,
    dryRun: false,
    discoveryMode: "INDEPENDENT_CANDIDATES",
    seedCandidates: seed,
    allowWebhoundWithoutFlag: false,
    includeDmvExpansion: false,
    trigger: "radisson_sd_first_cycle",
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
      commercialMotion: o.commercialMotion || o.captureMotion || null,
      hotelFit: o.hotelFit || o.hotelFitScore || null,
      eventStartDate: o.eventStartDate,
      whyNow: o.whyNow,
      hotelOpportunityThesis: o.hotelOpportunityThesis,
    })),
  });

  // --- Phase C: promote qualified ---
  const beforeDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  let working = [...(beforeDoc.opportunities || [])];
  const promotionResults = [];
  const promotable = opportunities.filter((o) => {
    const q = String(o.opportunityQualification || "").toUpperCase();
    const p = String(o.priority || "").toUpperCase();
    const a = o.actionabilityV3 || hygiene.rows.find((r) => r.id === o.id)?.actionabilityV3;
    if (p === "DISQUALIFIED" || q === "DISQUALIFIED" || q === "INSUFFICIENT") return false;
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
          opportunityQualification:
            cand.opportunityQualification === "PENDING" || !cand.opportunityQualification
              ? "STRONG"
              : cand.opportunityQualification,
        },
        existingOpps: working,
        hotelId: HOTEL_ID,
        runId: `radisson_sd_${startedAt.slice(0, 10).replace(/-/g, "")}`,
        discoveryRunId: `radisson_sd_ou_${startedAt.slice(0, 10).replace(/-/g, "")}`,
        discoveryAt: startedAt,
        method: "radisson_santo_domingo_open_universe_v1",
        source: cand.officialSource || cand.sources?.[0]?.url || null,
        dryRun: false,
      });
      promotionResults.push({
        action: promotion.action,
        id: promotion.opportunity?.id || cand.id,
        title: cand.title,
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
    }
    await saveOpportunitiesCanonical(HOTEL_ID, {
      hotelId: HOTEL_ID,
      opportunities: working,
      updatedAt: new Date().toISOString(),
      runId: `radisson_sd_${startedAt.slice(0, 10).replace(/-/g, "")}`,
    });
  } else {
    for (const cand of promotable) {
      promotionResults.push({ action: "DRY_RUN_WOULD_PROMOTE", id: cand.id, title: cand.title });
    }
  }

  writeJson(path.join(OUT_DIR, "GDI_PROMOTIONS.json"), {
    dryRun,
    promotableCount: promotable.length,
    results: promotionResults,
    actionCounts: countBy(promotionResults, (r) => r.action),
  });

  // --- Phase D: contact completeness + Jev ---
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
      runId: `radisson_sd_contact_${startedAt.slice(0, 10).replace(/-/g, "")}`,
    });

    const afterCf = filterCustomerFacingOpportunities(merged);
    const tiers = { NAMED_DIRECT: 0, NAMED_PARTIAL: 0, FUNCTIONAL: 0, ORG_PATH: 0, NO_CONTACT: 0 };
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
  const finalCf = filterCustomerFacingOpportunities(finalDoc.opportunities || []);
  const byPri = countBy(finalCf, (o) => {
    const p = String(o.priority || "").toUpperCase();
    const t = String(o.opportunityType || "").toUpperCase();
    if (/ACTIONABLE|HIGH_PRIORITY/.test(p) && !/WATCH|HOUSING|OVERFLOW/.test(t)) return "ACTIONABLE";
    if (/HOUSING/.test(t) || /HOUSING/.test(p)) return "HOUSING";
    if (/OVERFLOW/.test(t) || /OVERFLOW/.test(p)) return "OVERFLOW";
    if (/FUTURE/.test(t) || /FUTURE_WATCH/.test(p)) return "FUTURE_WATCH";
    if (/WATCH/.test(t) || /WATCH|MEDIUM/.test(p)) return "WATCH";
    if (/HIGH/.test(p)) return "ACTIONABLE";
    return p || "OTHER";
  });

  const summary = {
    hotelId: HOTEL_ID,
    hotelName: profile.identity?.hotelName || config.displayName,
    dryRun,
    startedAt,
    finishedAt: new Date().toISOString(),
    runtimeMs: Date.now() - t0,
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
      byBucket: byPri,
    },
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

