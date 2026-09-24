/**
 * GDI Demand Generator Intelligence V1 — Bethesda live canary.
 *
 * Default: dry-run (no Airtable writes).
 *   node scripts/gdi-demand-generators-v1-bethesda-canary.mjs
 *   node scripts/gdi-demand-generators-v1-bethesda-canary.mjs --apply-schema
 *   node scripts/gdi-demand-generators-v1-bethesda-canary.mjs --apply-schema --apply
 *   node scripts/gdi-demand-generators-v1-bethesda-canary.mjs --apply --live-serp
 *
 * No Surfe/PDL. No mass promotion. No Bethesda hardcodes in lib logic.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "node:child_process";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import {
  buildDemandGeneratorEntity,
  buildProgramEntity,
  buildHotelDemandGeneratorFit,
  buildDemandGeneratorSignal,
  buildGeneratorDrivenOpportunityDraft,
  buildGeneratorDiscoveryQueries,
  buildGeneratorGuidedQueries,
  compareSearchEfficiency,
  measureIncrementalAtBats,
  suggestNextResearchAt,
  upsertDemandGenerator,
  upsertDemandProgram,
  upsertHotelGeneratorFit,
  upsertDemandGeneratorSignal,
  auditDemandGeneratorHotelSpecificLogic,
  GENERATOR_STATUS,
} from "../lib/group-demand-intelligence/demand-generators/index.js";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_GENERATOR_SEEDS,
} from "../fixtures/group-demand-intelligence/demand-generators/bethesda-seeds.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "group-demand-intelligence",
  "demand-generators-v1"
);

const APPLY = process.argv.includes("--apply");
const APPLY_SCHEMA = process.argv.includes("--apply-schema");
const LIVE_SERP = process.argv.includes("--live-serp");

function countBy(arr, keyFn) {
  const m = {};
  for (const x of arr) {
    const k = keyFn(x) || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

async function optionalLiveSerp(queries, limit = 8) {
  if (!LIVE_SERP) {
    return { enabled: false, queries: 0, urls: 0, hits: [] };
  }
  try {
    const { runSerpQuery } = await import(
      "../lib/group-demand-intelligence/native-blind-discovery.js"
    ).catch(() => ({}));
    if (typeof runSerpQuery !== "function") {
      // Fallback: dynamic import of module and use internal if exported
      const mod = await import(
        "../lib/group-demand-intelligence/native-blind-discovery.js"
      );
      const fn = mod.runSerpQuery || mod.default?.runSerpQuery;
      if (typeof fn !== "function") {
        return {
          enabled: true,
          queries: 0,
          urls: 0,
          hits: [],
          error: "serp_helper_not_exported",
        };
      }
    }
  } catch {
    /* continue with lightweight fetch below */
  }

  // Lightweight: use same SERP path as native discovery via child-safe import
  let serpFn = null;
  try {
    const mod = await import(
      "../lib/group-demand-intelligence/native-blind-discovery.js"
    );
    serpFn = mod.__test?.runSerpQuery || null;
  } catch {
    serpFn = null;
  }

  const hits = [];
  let urls = 0;
  let qCount = 0;
  if (!serpFn) {
    return {
      enabled: true,
      queries: 0,
      urls: 0,
      hits: [],
      note: "SERP runner not exported; guided research measured via query planning only",
    };
  }
  for (const q of queries.slice(0, limit)) {
    qCount += 1;
    try {
      const res = await serpFn(q.query, { num: 5 });
      const organic = res?.organic || res?.results || [];
      urls += organic.length;
      for (const row of organic.slice(0, 5)) {
        hits.push({
          query: q.query,
          lane: q.lane,
          title: row.title || row.name,
          url: row.link || row.url,
        });
      }
    } catch (err) {
      hits.push({ query: q.query, error: String(err?.message || err) });
    }
  }
  return { enabled: true, queries: qCount, urls, hits };
}

/** Known public future-ish signals tied to seeds (evidence-backed, not invented). */
function seedFutureSignals(generatorEntity, programEntity, fitEntity, seed) {
  const signals = [];
  // Only attach when program is recurring confirmed AND we have an official source
  if (
    programEntity?.recurrenceStatus === "RECURRING_CONFIRMED" &&
    (programEntity.sourceUrls?.[0] || generatorEntity.sourceUrls?.[0])
  ) {
    const sourceUrl =
      programEntity.sourceUrls?.[0] || generatorEntity.sourceUrls?.[0];
    // Conservative: without a concrete future date, stay WATCH (no invented year)
    signals.push(
      buildDemandGeneratorSignal({
        demandGeneratorId: generatorEntity.demandGeneratorId,
        programId: programEntity.programId,
        seriesId: programEntity.seriesId,
        hotelId: fitEntity.hotelId,
        hotelGeneratorFitId: fitEntity.fitId,
        triggerType: "CONFERENCE_CALENDAR_UPDATE",
        title: `${programEntity.programName} — monitor next cycle`,
        sourceUrl,
        market: programEntity.market || fitEntity.marketRelevance,
        marketRelevance: fitEntity.marketRelevance,
        productFit: fitEntity.productFit,
        hotelFitOk: ["HIGH", "CORE", "COMPETITIVE", "MEDIUM"].includes(
          String(fitEntity.marketRelevance || "").toUpperCase()
        ),
        // No lodging thesis yet from seed alone → must not be TRUE
        lodgingDemandThesis: false,
        hotelDemandThesis: `Monitor ${generatorEntity.organizationName} official calendar for next cycle lodging signals`,
        recommendedAction: `Watch ${generatorEntity.organizationName} program pages for dates / housing`,
        timingStatus: "UNKNOWN",
      })
    );
  }

  // Explicit lodging-bearing examples only when seed marks them (none invent dates)
  if (seed.liveSignalExample) {
    signals.push(
      buildDemandGeneratorSignal({
        ...seed.liveSignalExample,
        demandGeneratorId: generatorEntity.demandGeneratorId,
        programId: programEntity?.programId,
        hotelId: fitEntity.hotelId,
        hotelGeneratorFitId: fitEntity.fitId,
        marketRelevance: fitEntity.marketRelevance,
        productFit: "HIGH",
        hotelFitOk: true,
      })
    );
  }
  return signals;
}

async function loadExistingGdiOrgOverlap(hotelId) {
  // Prefer filesystem pilot opportunities if present
  const oppPath = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json"
  );
  if (!fs.existsSync(oppPath)) return { ids: [], titles: [], orgs: [] };
  try {
    const raw = JSON.parse(fs.readFileSync(oppPath, "utf8"));
    const list = raw.opportunities || raw.items || raw || [];
    const arr = Array.isArray(list) ? list : [];
    return {
      ids: arr.map((o) => o.id || o.opportunityId).filter(Boolean),
      titles: arr.map((o) => o.title).filter(Boolean),
      orgs: arr.map((o) => String(o.organizationName || "").toLowerCase()).filter(Boolean),
    };
  } catch {
    return { ids: [], titles: [], orgs: [] };
  }
}

async function main() {
  const started = Date.now();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "gdi_demand_generators_canary" });

  if (APPLY_SCHEMA) {
    const r = spawnSync(
      process.execPath,
      [
        path.join(ROOT, "scripts/ensure-gdi-demand-generators-airtable-schema.mjs"),
        ...(APPLY ? ["--apply"] : []),
      ],
      { cwd: ROOT, encoding: "utf8", env: process.env }
    );
    if (r.status !== 0) {
      console.error(r.stdout);
      console.error(r.stderr);
      throw new Error("schema_ensure_failed");
    }
  }

  const hotelId = BETHESDA_HOTEL_ID;
  const hotelCfg = loadHotelDemandConfig(hotelId);
  if (!hotelCfg) throw new Error("bethesda_hotel_config_missing");

  const audit = auditDemandGeneratorHotelSpecificLogic();
  const genericQueries = buildGeneratorDiscoveryQueries(hotelCfg, {
    maxQueries: 24,
  });

  const existing = await loadExistingGdiOrgOverlap(hotelId);

  const generators = [];
  const programs = [];
  const fits = [];
  const signals = [];
  const writeLog = [];
  let guidedQueryPlan = [];

  for (const seed of BETHESDA_GENERATOR_SEEDS) {
    const generator = buildDemandGeneratorEntity({
      ...seed,
      nextResearchAt: suggestNextResearchAt(
        seed.generatorStatus || GENERATOR_STATUS.VERIFIED
      ),
      researchReason: "canary_monitor",
    });
    const fit = buildHotelDemandGeneratorFit({
      hotelId,
      hotelName: hotelCfg.displayName || "Bethesda Marriott",
      demandGeneratorId: generator.demandGeneratorId,
      organizationName: generator.organizationName,
      factors: seed.fitFactors || {},
      productFit:
        seed.fitFactors?.marketRelevance === "CORE" ? "HIGH" : "MEDIUM",
    });

    let programEntity = null;
    if (seed.programs?.[0]) {
      programEntity = buildProgramEntity({
        demandGeneratorId: generator.demandGeneratorId,
        ...seed.programs[0],
      });
      programs.push(programEntity);
      guidedQueryPlan.push(
        ...buildGeneratorGuidedQueries(generator, programEntity, { maxQueries: 3 })
      );
    } else {
      guidedQueryPlan.push(
        ...buildGeneratorGuidedQueries(generator, {}, { maxQueries: 2 })
      );
    }

    generators.push(generator);
    fits.push(fit);
    signals.push(...seedFutureSignals(generator, programEntity, fit, seed));

    if (APPLY) {
      const gWrite = await upsertDemandGenerator(generator, { dryRun: false });
      writeLog.push({ kind: "generator", ...gWrite, recordId: gWrite.recordId });
      let programRecordId = null;
      if (programEntity) {
        const pWrite = await upsertDemandProgram(programEntity, {
          dryRun: false,
          generatorRecordId: gWrite.recordId,
        });
        programRecordId = pWrite.recordId;
        writeLog.push({ kind: "program", action: pWrite.action, id: programEntity.programId });
      }
      const fWrite = await upsertHotelGeneratorFit(fit, {
        dryRun: false,
        generatorRecordId: gWrite.recordId,
      });
      writeLog.push({ kind: "fit", action: fWrite.action, id: fit.fitId });
      for (const sig of signals.filter(
        (s) => s.demandGeneratorId === generator.demandGeneratorId
      )) {
        const sWrite = await upsertDemandGeneratorSignal(sig, {
          dryRun: false,
          generatorRecordId: gWrite.recordId,
          programRecordId,
          fitRecordId: fWrite.recordId,
        });
        writeLog.push({
          kind: "signal",
          action: sWrite.action,
          id: sig.signalId,
          status: sig.signalStatus,
        });
      }
    } else {
      writeLog.push({
        kind: "generator",
        action: "DRY_RUN",
        id: generator.demandGeneratorId,
      });
    }
  }

  // Deduplicate guided queries
  const guidedUnique = [];
  const seenQ = new Set();
  for (const q of guidedQueryPlan) {
    if (seenQ.has(q.query)) continue;
    seenQ.add(q.query);
    guidedUnique.push(q);
  }

  const serpGeneric = await optionalLiveSerp(genericQueries, LIVE_SERP ? 6 : 0);
  const serpGuided = await optionalLiveSerp(guidedUnique, LIVE_SERP ? 8 : 0);

  // Overlap: existing GDI orgs that match seed generator names
  const seedNames = generators.map((g) => g.organizationName.toLowerCase());
  const aliasBag = new Set(
    generators.flatMap((g) =>
      [g.organizationName, ...(g.organizationAliases || [])].map((x) =>
        String(x).toLowerCase()
      )
    )
  );
  const overlapOrgs = existing.orgs.filter((o) =>
    [...aliasBag].some((a) => o.includes(a) || a.includes(o))
  );
  const generatorDrivenCandidates = signals
    .filter((s) => s.trueActionable)
    .map((s) => s.signalId);
  // Without live lodging-evidence TRUE signals, incremental from promotion = 0 by design
  const bats = measureIncrementalAtBats({
    generatorDrivenOpportunityIds: generatorDrivenCandidates,
    genericSearchOpportunityIds: existing.ids,
  });

  // Efficiency: guided uses fewer planned queries for known orgs
  const efficiency = compareSearchEfficiency(
    {
      queries: genericQueries.length,
      urls: serpGeneric.urls || genericQueries.length * 5,
      qualified: overlapOrgs.length,
      trueActionable: Math.min(1, overlapOrgs.length),
    },
    {
      queries: guidedUnique.length,
      urls: serpGuided.urls || guidedUnique.length * 3,
      qualified: signals.filter((s) => s.qualify).length,
      trueActionable: signals.filter((s) => s.trueActionable).length,
    }
  );

  const priorityCounts = countBy(fits, (f) => f.generatorPriority);
  const typeCounts = countBy(generators, (g) => g.organizationType);
  const recurrenceCounts = countBy(programs, (p) => p.recurrenceStatus);
  const signalStatusCounts = countBy(signals, (s) => s.signalStatus);

  const promotionDrafts = signals
    .filter((s) => s.trueActionable)
    .map((s) => {
      const g = generators.find((x) => x.demandGeneratorId === s.demandGeneratorId);
      const p = programs.find((x) => x.programId === s.programId);
      const f = fits.find((x) => x.fitId === s.hotelGeneratorFitId);
      return buildGeneratorDrivenOpportunityDraft(s, g, p, f);
    });

  const canary = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "APPLY" : "DRY_RUN",
    liveSerp: LIVE_SERP,
    baseId,
    isCanonical: baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    hotelId,
    hotelName: hotelCfg.displayName,
    audit,
    tallies: {
      generators: generators.length,
      programs: programs.length,
      fits: fits.length,
      signals: signals.length,
      high: priorityCounts.HIGH || 0,
      medium: priorityCounts.MEDIUM || 0,
      watch: priorityCounts.WATCH || 0,
      low: priorityCounts.LOW || 0,
      generatorsWithProgram: generators.filter((g) =>
        programs.some((p) => p.demandGeneratorId === g.demandGeneratorId)
      ).length,
      recurrence: recurrenceCounts,
      signalStatus: signalStatusCounts,
      organizationTypes: typeCounts,
    },
    atBats: {
      ...bats,
      existingGdiOrgOverlapNames: [...new Set(overlapOrgs)].slice(0, 20),
      note: "TRUE_ACTIONABLE requires lodging thesis; seed monitors stay WATCH/QUALIFIED without inventing futures",
    },
    efficiency,
    queries: {
      genericPlanned: genericQueries.length,
      guidedPlanned: guidedUnique.length,
      serpGeneric,
      serpGuided,
    },
    promotionDrafts: promotionDrafts.map((d) => ({
      ok: d.ok,
      opportunityId: d.opportunity?.opportunityId,
      title: d.opportunity?.title,
    })),
    surfe: 0,
    pdl: 0,
    runtimeMs: Date.now() - started,
    writeLog: writeLog.slice(0, 80),
    sampleGenerators: generators.slice(0, 5).map((g) => ({
      id: g.demandGeneratorId,
      name: g.organizationName,
      type: g.organizationType,
      status: g.generatorStatus,
      domain: g.officialDomain,
    })),
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(path.join(OUT_DIR, "CANARY.json"), JSON.stringify(canary, null, 2));
  console.log(JSON.stringify(canary, null, 2));
  console.log("WROTE", path.join(OUT_DIR, "CANARY.json"));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
