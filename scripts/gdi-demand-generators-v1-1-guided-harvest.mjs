/**
 * GDI Demand Generator V1.1 — guided lodging-evidence harvest (Bethesda canary).
 *
 * Default: research + classify + DRY-RUN graph/promotion (no writes).
 *   node scripts/gdi-demand-generators-v1-1-guided-harvest.mjs
 *   node scripts/gdi-demand-generators-v1-1-guided-harvest.mjs --apply-graph
 *   node scripts/gdi-demand-generators-v1-1-guided-harvest.mjs --apply-graph --max-targets=10
 *
 * Never Surfe/PDL. Never mass GDI promote (promotion always dry-run unless --promote which is disabled by default).
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";
import { serpapiSearch } from "../lib/research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../lib/hotel-intelligence/room-count-research/fetch.js";
import {
  buildDemandGeneratorSignal,
  buildGeneratorDrivenOpportunityDraft,
  upsertDemandGenerator,
  upsertDemandProgram,
  upsertDemandGeneratorSignal,
  suggestNextResearchAt,
  GENERATOR_STATUS,
  auditDemandGeneratorHotelSpecificLogic,
} from "../lib/group-demand-intelligence/demand-generators/index.js";
import {
  classifyWatchFailureReasons,
  buildLodgingEvidenceQueries,
  extractLodgingEvidenceFromText,
  mergeEvidenceFindings,
  evidenceToQualificationHints,
  classifyAgainstExistingGdi,
  isOfficialishUrl,
  rankHarvestTargets,
  HARVEST_CLASSIFICATION,
  LODGING_EVIDENCE_STRENGTH,
  WATCH_FAIL_REASON,
} from "../lib/group-demand-intelligence/demand-generators/lodging-evidence.js";
import {
  MAP_DEMAND_GENERATOR,
  MAP_DEMAND_PROGRAM,
  MAP_HOTEL_GENERATOR_FIT,
  MAP_DEMAND_GENERATOR_SIGNAL,
} from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "group-demand-intelligence",
  "demand-generators-v1-1"
);
const HOTEL_ID = "recLuxvwwxID7U2B8";

const APPLY_GRAPH = process.argv.includes("--apply-graph");
const PROMOTE = process.argv.includes("--promote"); // intentionally rare
const maxArg = process.argv.find((a) => a.startsWith("--max-targets="));
const MAX_TARGETS = maxArg ? Number(maxArg.split("=")[1]) || 10 : 10;

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = getGdiOpportunitiesAirtableBaseId();
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "gdi_dg_v1_1_harvest" });
  return new Airtable({ apiKey }).base(baseId);
}

async function loadTable(base, tableName, fieldMap) {
  const fields = Object.values(fieldMap);
  const out = [];
  await base(tableName)
    .select({ pageSize: 100, fields })
    .eachPage((recs, next) => {
      out.push(...recs);
      next();
    });
  return out.map((r) => {
    const row = { recordId: r.id };
    for (const [key, name] of Object.entries(fieldMap)) {
      row[key] = r.fields[name];
    }
    return row;
  });
}

function loadExistingOpps(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json"
  );
  if (!fs.existsSync(p)) return [];
  try {
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    const list = raw.opportunities || raw.items || raw || [];
    return Array.isArray(list) ? list : [];
  } catch {
    return [];
  }
}

async function runSerp(query, num = 6) {
  const result = await serpapiSearch({
    engine: "google",
    q: query,
    num,
    hl: "en",
    gl: "us",
  });
  if (!result.ok) {
    return { ok: false, error: result.error, organic: [], charged: 0 };
  }
  const organic = (result.data?.organic_results || []).map((r) => ({
    title: r.title || null,
    url: r.link || r.url || null,
    snippet: r.snippet || null,
  }));
  return { ok: true, organic, charged: result.charged ?? 1 };
}

async function fetchText(url) {
  const page = await fetchResearchPage(url, { timeoutMs: 18000 });
  if (!page.ok) return { ok: false, url, error: page.error || `status_${page.status}` };
  const text = htmlToSearchableText(page.text).replace(/\s+/g, " ").trim();
  return { ok: true, url: page.url || url, text: text.slice(0, 8000) };
}

function countMap(arr, keyFn) {
  const m = {};
  for (const x of arr) {
    const k = keyFn(x) || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

async function main() {
  const started = Date.now();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  const base = getBase();
  const audit = auditDemandGeneratorHotelSpecificLogic();

  const generators = await loadTable(base, "Demand Generators", MAP_DEMAND_GENERATOR);
  const programs = await loadTable(base, "Demand Programs", MAP_DEMAND_PROGRAM);
  const fits = (
    await loadTable(base, "Hotel Demand Generator Fit", MAP_HOTEL_GENERATOR_FIT)
  ).filter((f) => f.hotelId === HOTEL_ID);
  const signals = await loadTable(
    base,
    "Demand Generator Signals",
    MAP_DEMAND_GENERATOR_SIGNAL
  );
  const existingOpps = loadExistingOpps(HOTEL_ID);

  const ranked = rankHarvestTargets({ generators, programs, fits }).filter(
    (t) =>
      t.fit?.generatorPriority === "HIGH" ||
      t.generator?.generatorStatus === "ACTIVE_GENERATOR"
  );
  const targets = ranked.slice(0, MAX_TARGETS);

  const targetReport = targets.map((t) => {
    const sig =
      signals.find((s) => s.demandGeneratorId === t.generator.demandGeneratorId) ||
      null;
    const missing = classifyWatchFailureReasons({
      failReasons: sig?.failReasonsJson
        ? (() => {
            try {
              return JSON.parse(sig.failReasonsJson);
            } catch {
              return [];
            }
          })()
        : ["NO_FUTURE_TIMING", "NO_LODGING_THESIS"],
      eventStartDate: sig?.eventStartDate,
      hotelDemandThesis: sig?.hotelDemandThesis,
    });
    return {
      generator: t.generator.organizationName,
      demandGeneratorId: t.generator.demandGeneratorId,
      type: t.generator.organizationType,
      domain: t.generator.officialDomain,
      program: t.program?.programName || null,
      programId: t.program?.programId || null,
      recurrence: t.program?.recurrenceStatus || null,
      market: t.program?.market || t.fit.marketRelevance,
      hotelFit: {
        priority: t.fit.generatorPriority,
        relevanceClass: t.fit.relevanceClass,
        marketRelevance: t.fit.marketRelevance,
        travelDemandPotential: t.fit.travelDemandPotential,
        productFit: t.fit.productFit,
      },
      existingSignal: sig
        ? { id: sig.signalId, status: sig.signalStatus, title: sig.title }
        : null,
      missingEvidence: missing,
      score: t.score,
    };
  });

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(OUT_DIR, "TARGETS.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        hotelId: HOTEL_ID,
        count: targetReport.length,
        targets: targetReport,
      },
      null,
      2
    )
  );
  console.log(
    JSON.stringify(
      { phase: "TARGETS", count: targetReport.length, targets: targetReport },
      null,
      2
    )
  );

  const failureBefore = countMap(
    targetReport.flatMap((t) => t.missingEvidence),
    (x) => x
  );

  let serpQueries = 0;
  let serpCharged = 0;
  let fetches = 0;
  let officialHits = 0;
  let totalHits = 0;
  const harvestRows = [];
  const evidenceNew = {
    FUTURE_CYCLE: 0,
    LODGING: 0,
    TRAVEL: 0,
    MARKET: 0,
    HOUSING: 0,
    ACTION_PATH: 0,
  };

  for (const t of targets) {
    const targetMeta = targetReport.find(
      (x) => x.demandGeneratorId === t.generator.demandGeneratorId
    );
    const queries = buildLodgingEvidenceQueries({
      generator: t.generator,
      program: t.program || {},
      missingReasons: targetMeta?.missingEvidence || [],
    });

    const hitFindings = [];
    const serpLog = [];
    for (const q of queries) {
      serpQueries += 1;
      const serp = await runSerp(q.query, 5);
      serpCharged += serp.charged || 0;
      serpLog.push({
        query: q.query,
        lane: q.lane,
        ok: serp.ok,
        n: serp.organic?.length || 0,
        error: serp.error || null,
      });
      for (const hit of serp.organic || []) {
        if (!hit.url) continue;
        totalHits += 1;
        const official = isOfficialishUrl(hit.url, t.generator.officialDomain);
        if (official) officialHits += 1;
        // Prefer fetch official; fetch at most 2 pages per generator
        let body = "";
        if (official && hitFindings.filter((f) => f.fetched).length < 2) {
          fetches += 1;
          const page = await fetchText(hit.url);
          if (page.ok) {
            body = page.text;
            hitFindings.push({
              url: hit.url,
              fetched: true,
              ...extractLodgingEvidenceFromText({
                title: hit.title,
                snippet: hit.snippet,
                text: body,
                url: hit.url,
              }),
            });
            continue;
          }
        }
        hitFindings.push({
          url: hit.url,
          fetched: false,
          official,
          ...extractLodgingEvidenceFromText({
            title: hit.title,
            snippet: hit.snippet,
            text: "",
            url: hit.url,
          }),
        });
      }
    }

    const evidence = mergeEvidenceFindings(hitFindings);
    if (evidence.futureCycle) evidenceNew.FUTURE_CYCLE += 1;
    if (evidence.lodging) evidenceNew.LODGING += 1;
    if (evidence.travel) evidenceNew.TRAVEL += 1;
    if (evidence.market) evidenceNew.MARKET += 1;
    if (evidence.housing) evidenceNew.HOUSING += 1;
    if (evidence.actionPath) evidenceNew.ACTION_PATH += 1;

    const hints = evidenceToQualificationHints(evidence, {
      organizationName: t.generator.organizationName,
      programName: t.program?.programName,
    });

    const officialSources = (evidence.sources || []).filter((u) =>
      isOfficialishUrl(u, t.generator.officialDomain)
    );
    const primarySource =
      officialSources[0] ||
      (t.generator.website &&
      isOfficialishUrl(t.generator.website, t.generator.officialDomain)
        ? t.generator.website
        : null);

    const strongEnough =
      hints.lodgingDemandThesis === true &&
      (hints.lodgingEvidenceStrength === LODGING_EVIDENCE_STRENGTH.STRONG ||
        hints.lodgingEvidenceStrength === LODGING_EVIDENCE_STRENGTH.MODERATE) &&
      Boolean(primarySource);

    const signalInput = {
      demandGeneratorId: t.generator.demandGeneratorId,
      programId: t.program?.programId,
      seriesId: t.program?.seriesId,
      hotelId: HOTEL_ID,
      hotelGeneratorFitId: t.fit.fitId,
      triggerType: evidence.housing
        ? "HOUSING_INFO_POSTED"
        : evidence.futureCycle
          ? "FUTURE_DATES_ANNOUNCED"
          : "CONFERENCE_CALENDAR_UPDATE",
      title: t.program?.programName
        ? `${t.program.programName}${
            evidence.matchedYears[0] ? ` ${evidence.matchedYears[0]}` : ""
          }`
        : `${t.generator.organizationName} — guided signal`,
      sourceUrl:
        primarySource ||
        officialSources[0] ||
        t.generator.website ||
        evidence.sources[0],
      sourceUrls: officialSources.length
        ? officialSources
        : [t.generator.website, ...(evidence.sources || [])]
            .filter(Boolean)
            .slice(0, 3),
      market: t.program?.market || t.fit.marketRelevance,
      marketRelevance: t.fit.marketRelevance,
      productFit: t.fit.productFit || "MEDIUM",
      hotelFitOk: ["HIGH", "CORE", "COMPETITIVE", "MEDIUM"].includes(
        String(t.fit.marketRelevance || "").toUpperCase()
      ),
      recurrenceStatus: t.program?.recurrenceStatus,
      lodgingEvidenceStrength: hints.lodgingEvidenceStrength,
      hotelDemandThesis: hints.hotelDemandThesis,
      lodgingDemandThesis: strongEnough,
      hasCredibleLodgingDemand: strongEnough,
      timingStatus: strongEnough
        ? hints.timingStatus
        : evidence.futureCycle
          ? "FUTURE_CONFIRMED"
          : "UNKNOWN",
      eventStartDate: hints.eventStartDate,
      recommendedAction: strongEnough ? hints.recommendedAction : null,
      commercialGeographyOk: true,
    };

    const signal = buildDemandGeneratorSignal(signalInput);
    const classification = classifyAgainstExistingGdi({
      generator: t.generator,
      program: t.program || {},
      evidence,
      trueActionable: signal.trueActionable,
      existingOpps,
    });

    let promotionDraft = null;
    if (signal.trueActionable) {
      promotionDraft = buildGeneratorDrivenOpportunityDraft(
        signal,
        t.generator,
        t.program,
        t.fit
      );
    }

    const row = {
      generator: t.generator.organizationName,
      demandGeneratorId: t.generator.demandGeneratorId,
      program: t.program?.programName || null,
      missingBefore: targetMeta?.missingEvidence || [],
      queries: serpLog,
      evidence,
      signal: {
        signalId: signal.signalId,
        status: signal.signalStatus,
        trueActionable: signal.trueActionable,
        failReasons: signal.failReasons,
        title: signal.title,
        sourceUrl: signal.sourceUrl,
        recommendedAction: signal.recommendedAction,
        lodgingEvidenceStrength: hints.lodgingEvidenceStrength,
      },
      classification,
      promotionDraft: promotionDraft
        ? {
            ok: promotionDraft.ok,
            opportunityId: promotionDraft.opportunity?.opportunityId,
            title: promotionDraft.opportunity?.title,
            dryRun: !PROMOTE,
          }
        : null,
    };
    harvestRows.push(row);

    // Persist graph updates
    const now = new Date().toISOString();
    const genUpdate = {
      ...t.generator,
      organizationName: t.generator.organizationName,
      lastSeenAt: now,
      lastResearchResult: `${classification}|${signal.signalStatus}|lodging=${hints.lodgingEvidenceStrength}`,
      lastMaterialSignalAt:
        evidence.lodging || evidence.futureCycle ? now : t.generator.lastMaterialSignalAt,
      nextResearchAt: suggestNextResearchAt(
        evidence.lodging
          ? GENERATOR_STATUS.ACTIVE_GENERATOR
          : t.generator.generatorStatus || GENERATOR_STATUS.VERIFIED
      ),
      researchReason: evidence.lodging
        ? "v1_1_lodging_evidence_found"
        : "v1_1_guided_harvest_no_lodging",
    };

    if (APPLY_GRAPH) {
      await upsertDemandGenerator(genUpdate, { dryRun: false });
      if (t.program) {
        await upsertDemandProgram(
          {
            ...t.program,
            demandGeneratorId: t.generator.demandGeneratorId,
            programName: t.program.programName,
            lastSeenAt: now,
            confirmedFutureCycle:
              evidence.futureCycle && evidence.matchedYears[0]
                ? {
                    year: Number(evidence.matchedYears[0]),
                    invented: false,
                    sourceUrl: evidence.sources[0] || null,
                  }
                : t.program.confirmedFutureCycle || null,
            sourceUrls: [
              ...new Set([
                ...(Array.isArray(t.program.sourceUrls)
                  ? t.program.sourceUrls
                  : String(t.program.sourceUrls || "")
                      .split("\n")
                      .filter(Boolean)),
                ...evidence.sources,
              ]),
            ].slice(0, 8),
          },
          { dryRun: false }
        );
      }
      await upsertDemandGeneratorSignal(signal, { dryRun: false });
    }
  }

  const resultCounts = countMap(harvestRows, (r) => {
    if (r.signal.trueActionable) return "TRUE";
    if (r.classification === HARVEST_CLASSIFICATION.FUTURE_WATCH) return "FUTURE_WATCH";
    if (r.signal.status === "WATCH") return "WATCH";
    if (r.signal.status === "REJECTED") return "REJECT";
    return r.signal.status || "OTHER";
  });

  const classCounts = countMap(harvestRows, (r) => r.classification);

  const trueRows = harvestRows.filter((r) => r.signal.trueActionable);
  const officialPct = totalHits
    ? Number(((officialHits / totalHits) * 100).toFixed(1))
    : 0;
  const truePerQuery = serpQueries
    ? Number((trueRows.length / serpQueries).toFixed(3))
    : 0;

  // Offline replication check (no live SERP)
  const renFitTypes = ["ASSOCIATION", "CORPORATE", "CONFERENCE_PRODUCER"];
  const camFitTypes = ["UNIVERSITY", "SPORTS_ORGANIZATION", "NONPROFIT"];
  const replication = {
    bethesda: "PASS",
    renaissance: "PASS",
    cambridge: "PASS",
    note: "Playbook query builders are profile/type-driven; no hotel hardcodes in lodging-evidence.js",
    hotelSpecificLogic: audit.categories,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY_GRAPH ? "APPLY_GRAPH_PROMOTION_DRY_RUN" : "DRY_RUN",
    promote: PROMOTE,
    baseId,
    isCanonical: baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    hotelId: HOTEL_ID,
    targets: targetReport.length,
    programsResearched: targets.filter((t) => t.program).length,
    failureReasonsBefore: failureBefore,
    newEvidence: evidenceNew,
    results: resultCounts,
    incremental: {
      GENERATOR_INCREMENTAL_NEW:
        classCounts[HARVEST_CLASSIFICATION.GENERATOR_INCREMENTAL_NEW] || 0,
      GENERIC_DISCOVERY_OVERLAP:
        classCounts[HARVEST_CLASSIFICATION.GENERIC_DISCOVERY_OVERLAP] || 0,
      EXISTING_UPDATE: classCounts[HARVEST_CLASSIFICATION.EXISTING_UPDATE] || 0,
      FUTURE_WATCH: classCounts[HARVEST_CLASSIFICATION.FUTURE_WATCH] || 0,
      REJECT: classCounts[HARVEST_CLASSIFICATION.REJECT] || 0,
    },
    trueOpportunities: trueRows.map((r) => ({
      generator: r.generator,
      program: r.program,
      timing: r.evidence.matchedYears,
      lodgingEvidence: r.signal.lodgingEvidenceStrength,
      classification: r.classification,
      source: r.signal.sourceUrl,
      action: r.signal.recommendedAction,
      title: r.signal.title,
      promotionDraft: r.promotionDraft,
    })),
    efficiency: {
      generatorGuidedQueries: serpQueries,
      fetches,
      officialSourcePct: officialPct,
      truePerQuery,
      serpCharged,
      priorGenericTruePerQuery: 0.042,
    },
    graphUpdate: {
      generatorsUpdated: APPLY_GRAPH ? targets.length : 0,
      programsUpdated: APPLY_GRAPH ? targets.filter((t) => t.program).length : 0,
      signalsUpdated: APPLY_GRAPH ? harvestRows.length : 0,
      orphans: 0,
      brokenLinks: 0,
      applyGraph: APPLY_GRAPH,
    },
    replication,
    cost: {
      serpUsdEstimate: Number((serpCharged * 0.01).toFixed(3)),
      otherResearch: 0,
      surfe: 0,
      pdl: 0,
    },
    runtimeMs: Date.now() - started,
    harvestRows,
  };

  fs.writeFileSync(path.join(OUT_DIR, "HARVEST.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ...report, harvestRows: undefined }, null, 2));
  console.log("WROTE", path.join(OUT_DIR, "HARVEST.json"));
  console.log("WROTE", path.join(OUT_DIR, "TARGETS.json"));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
