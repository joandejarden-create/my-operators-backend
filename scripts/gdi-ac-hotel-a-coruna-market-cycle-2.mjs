#!/usr/bin/env node
/**
 * AC Hotel A Coruña — GDI market cycle 2 (hidden-demand emphasis).
 * Same quality gates as first cycle; Spanish SERP; no cross-hotel bleed.
 *
 *   node scripts/gdi-ac-hotel-a-coruna-market-cycle-2.mjs --apply
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
import { resolveContactCompletenessBatch } from "../lib/group-demand-intelligence/contact-intelligence-completeness-v1.js";
import { upsertResearchRun } from "../lib/group-demand-intelligence/research-coverage/airtable-stores.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "rec2PVBDavppGpenm";
const OUT_DIR = path.join(ROOT, "reports/group-demand-intelligence/ac-hotel-a-coruna-v1");

const BUDGET = Object.freeze({ maxQueries: 36, maxPagesPerQuery: 2, maxCostUsd: 10 });

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
function assertNoCrossHotelBleed(texts) {
  const blob = texts.join("\n").toLowerCase();
  const hits = [];
  if (/\bbethesda\b|\bnih\b|\bnatcher\b/.test(blob)) hits.push("BETHESDA");
  if (/\bw rome\b|\bvia liguria\b/.test(blob)) hits.push("W_ROME");
  if (/\brenaissance.*(times square|nyc)/.test(blob)) hits.push("RENAISSANCE_TS");
  if (/\bhilton.*(times square|nyc)/.test(blob)) hits.push("HILTON_TS");
  if (/\bspice island beach resort\b|\bgrand anse\b/.test(blob)) hits.push("SPICE_ISLAND");
  if (hits.length) throw new Error(`CROSS_HOTEL_BLEED_IN_DISCOVERY: ${hits.join(",")}`);
}

async function main() {
  const apply = flag("apply");
  const skipContact = flag("skip-contact");
  const maxQueries = Math.max(1, Number(arg("max-queries", String(BUDGET.maxQueries))) || BUDGET.maxQueries);
  const startedAt = new Date().toISOString();
  const t0 = Date.now();
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const config = loadHotelDemandConfig(HOTEL_ID);
  if (!config) throw new Error(`missing_gdi_config_${HOTEL_ID}`);
  const profile = buildHotelGroupDemandProfile(HOTEL_ID, { config });

  console.error(`[ac-coruna-c2] market cycle 2 hotel=${HOTEL_ID} apply=${apply} maxQueries=${maxQueries}`);

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
      discoverySourceLabel: "ac_hotel_a_coruna_market_cycle_2_hidden_demand_v1",
      stratifiedQueryBudget: true,
      serpLocale: { hl: "es", gl: "es" },
      // Prefer corporate / project / training / association motion over calendar festivals.
      themeBias: [
        "equipos proyecto corporativo galicia alojamiento",
        "formación in company a coruña hotel",
        "comités empresa galicia reunión hotel",
        "puerto a coruña grupo visita hotel",
        "universidad coruña congreso científico alojamiento",
        "producción audiovisual galicia rodaje hotel",
        "asociación profesional galicia asamblea hotel",
        "deportes a coruña equipo visitante hotel",
      ],
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

  writeJson(path.join(OUT_DIR, "GDI_DISCOVERY_CYCLE2.json"), {
    hotelId: HOTEL_ID,
    cycle: 2,
    startedAt,
    budget: { ...BUDGET, maxQueries },
    pipeline: {
      version: pipe.version,
      nativeQueries: pipe.native?.ledger?.serpQueries ?? null,
      pagesFetched: pipe.native?.ledger?.pagesFetched ?? null,
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
    trigger: "ac_coruna_market_cycle_2",
  });

  const opportunities = research.opportunities || [];
  writeJson(path.join(OUT_DIR, "GDI_QUALIFIED_CYCLE2.json"), {
    hotelId: HOTEL_ID,
    summary: research.summary,
    opportunities: opportunities.map((o) => ({
      id: o.id,
      title: o.title,
      organizationName: o.organizationName,
      opportunityType: o.opportunityType,
      priority: o.priority,
      opportunityQualification: o.opportunityQualification,
    })),
  });

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

  for (const opp of promotable) {
    const res = await promoteQualifiedGdiOpportunity(opp, {
      hotelId: HOTEL_ID,
      existing: working,
      dryRun: !apply,
    });
    promotionResults.push({ id: opp.id, title: opp.title, action: res.action, reasons: res.reasons });
    if (res.opportunity && apply) {
      const idx = working.findIndex((x) => x.id === res.opportunity.id);
      if (idx >= 0) working[idx] = res.opportunity;
      else working.push(res.opportunity);
    }
  }

  if (apply) {
    await saveOpportunitiesCanonical(HOTEL_ID, {
      hotelId: HOTEL_ID,
      opportunities: working,
      updatedAt: new Date().toISOString(),
      runId: `ac_coruna_cycle2_${startedAt.replace(/[:.]/g, "-")}`,
    });
  }

  writeJson(path.join(OUT_DIR, "GDI_PROMOTIONS_CYCLE2.json"), { hotelId: HOTEL_ID, promotionResults });

  const customerVisible = filterCustomerFacingOpportunities(working);
  let contact = { researched: 0, skipped: true };
  if (!skipContact && customerVisible.length && apply) {
    const population = selectContactResearchPopulation(customerVisible);
    const batch = await resolveContactCompletenessBatch(population, {
      hotelId: HOTEL_ID,
      surfeAuto: 0,
      jevPersonApply: false,
    });
    contact = {
      researched: batch.summary?.researched ?? 0,
      batchSummary: batch.summary,
      grades: countBy(batch.results || [], (r) => gradeContactCompleteness(r.opportunity || r)),
    };
  }

  const summary = {
    hotelId: HOTEL_ID,
    hotelName: "AC Hotel A Coruña",
    cycle: 2,
    dryRun: !apply,
    startedAt,
    finishedAt: new Date().toISOString(),
    runtimeMs: Date.now() - t0,
    discovery: {
      queries: pipe.native?.ledger?.serpQueries ?? queryTexts.length,
      fetches: pipe.native?.ledger?.pagesFetched ?? null,
      candidates: seed.length,
      trueActionable: hygiene.trueActionable.length,
      byState: hygiene.byState,
      webhoundRequired: 0,
    },
    qualification: research.summary || {},
    promotions: countBy(promotionResults, (p) => p.action),
    customerVisible: {
      total: customerVisible.length,
      byBucket: countBy(customerVisible, (o) => o.customerBucket || o.priority),
    },
    contact,
  };

  writeJson(path.join(OUT_DIR, "GDI_CYCLE2_SUMMARY.json"), summary);

  if (apply) {
    const runId = `grun_ac_market_cycle2_${startedAt.replace(/[^0-9]/g, "").slice(0, 14)}`;
    await upsertResearchRun(
      {
        runId,
        hotelId: HOTEL_ID,
        hotelName: "AC Hotel A Coruña",
        runType: "MANUAL",
        status: "COMPLETED",
        startedAt,
        completedAt: summary.finishedAt,
        queries: summary.discovery.queries || 0,
        fetches: summary.discovery.fetches || 0,
        targetsAttempted: summary.discovery.candidates || 0,
        newOpportunities: summary.customerVisible.total || 0,
        notes: `AC market cycle 2 hidden-demand. byState=${JSON.stringify(summary.discovery.byState)}`,
        payload: summary,
      },
      { dryRun: false }
    );
  }

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
