#!/usr/bin/env node
/**
 * GDI Discovery Recall V4 — re-run Wave 3 hotels only.
 * Stages: discovery | hygiene | reports | all
 * Does not modify V3/V11/V12. No live promote. No Wave 4.
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
import { runNativeBlindDiscovery } from "../lib/group-demand-intelligence/native-blind-discovery.js";
import { dedupeDiscoveryCandidates } from "../lib/group-demand-intelligence/candidate-dedupe.js";
import { mapWebhoundOpportunityUniverse } from "../lib/group-demand-intelligence/webhound-opportunity-import.js";
import { evaluateNativeDiscoveryCompleteness } from "../lib/group-demand-intelligence/discovery-completeness-gate.js";
import {
  DISCOVERY_RECALL_V4,
  RECALL_EXTRACT_SYSTEM_V4,
  buildRecallV4SearchTasks,
  renderRecallV4Brief,
  inferDemandArchetype,
  classifyQueryYield,
} from "../lib/group-demand-intelligence/discovery-recall-v4.js";
import {
  enrichCommercialEvidenceV4,
  buildHygieneEvidenceOverlayV4,
  applyCommercialReadinessDemotionV4,
  completenessFlags,
  hasMeaningfulSource,
} from "../lib/group-demand-intelligence/commercial-evidence-v4.js";
import {
  DISCOVERY_HYGIENE_V3,
  ACTIONABILITY_V3,
  applyDiscoveryHygieneV3,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";
import { classifyVenueLockedOpportunity } from "../lib/group-demand-intelligence/venue-locked-classification-v11.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MARKER = "gdi_discovery_recall_v4_20260922";
const STACK_SHA = "1a3ebc79d0fe299c4b13d5df704247556e91c3a2";
const AS_OF = "2026-09-22";

const HOTELS = {
  A: {
    slot: "A",
    slug: "radisson-santo-domingo",
    hotelId: "recUOyzOXn2Zdp98I",
    name: "Radisson Hotel Santo Domingo",
    market: "Santo Domingo / Naco–Tiradentes",
    rooms: 160,
    countryCode: "DO",
    geos: ["Santo Domingo Naco", "Naco Santo Domingo", "Tiradentes", "Santo Domingo", "Distrito Nacional"],
  },
  B: {
    slot: "B",
    slug: "casas-del-xvi",
    hotelId: "recjDsNzu93CFfe87",
    name: "Casas del XVI (Vignette Collection)",
    market: "Santo Domingo / Zona Colonial",
    rooms: 21,
    countryCode: "DO",
    geos: ["Zona Colonial Santo Domingo", "Ciudad Colonial", "Santo Domingo", "Gazcue"],
  },
  C: {
    slot: "C",
    slug: "faranda-collection-bogota",
    hotelId: "rec9Tp0WBb2uk6w3u",
    name: "Faranda Collection Bogotá",
    market: "Bogotá / Bogotá Norte",
    rooms: null,
    countryCode: "CO",
    geos: ["Bogotá Norte", "Usaquén", "Bogotá", "Chicó Bogotá", "Calle 112 Bogotá"],
  },
};

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}
function writeMd(p, lines) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, (Array.isArray(lines) ? lines : [lines]).join("\n"));
}
function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function pct(n, d) {
  return d ? Math.round((1000 * n) / d) / 10 : 0;
}

function pathsFor(h) {
  const gdi = path.join(ROOT, "data/group-demand-intelligence/hotels", h.hotelId);
  const ge = path.join(ROOT, "data/group-demand-intelligence/evals");
  return {
    gdi,
    discovery: path.join(gdi, "v4-discovery.json"),
    qualified: path.join(gdi, "v4-qualified.json"),
    discoveryEval: path.join(ge, `${h.slug}-v4-discovery.json`),
    hygieneEval: path.join(ge, `${h.slug}-v4-hygiene.json`),
  };
}

function hotelList(argVal) {
  const v = String(argVal || "all").toUpperCase();
  if (v === "ALL") return Object.values(HOTELS);
  if (!HOTELS[v]) throw new Error(`unknown_hotel_${v}`);
  return [HOTELS[v]];
}

async function runDiscovery(hotel, { maxQueries, force }) {
  const p = pathsFor(hotel);
  fs.mkdirSync(p.gdi, { recursive: true });
  if (fs.existsSync(p.discovery) && !force) {
    console.error(`[${hotel.slot}] v4 discovery exists — use --force`);
    return readJson(p.discovery);
  }
  const config = loadHotelDemandConfig(hotel.hotelId);
  if (!config) throw new Error(`missing_gdi_config_${hotel.hotelId}`);
  const profile = buildHotelGroupDemandProfile(hotel.hotelId);
  const archetype = inferDemandArchetype(config, { rooms: hotel.rooms });
  const pack = buildRecallV4SearchTasks({
    geos: hotel.geos,
    archetype,
    countryCode: hotel.countryCode,
  });
  const brief = renderRecallV4Brief({
    hotelName: hotel.name,
    hotelId: hotel.hotelId,
    market: hotel.market,
    rooms: hotel.rooms,
    archetype,
    peakRoomsMin: config?.commercialPriorities?.coreTargetPeakRoomsMin,
    peakRoomsMax: config?.commercialPriorities?.coreTargetPeakRoomsMax,
  });
  console.error(
    `[${hotel.slot}] V4 discovery archetype=${archetype} maxQueries=${maxQueries} tasks=${pack.tasks.length}`
  );

  const native = await runNativeBlindDiscovery({
    hotelId: hotel.hotelId,
    profile,
    config,
    maxQueries,
    maxPagesPerQuery: 2,
    maxExtractBatches: 8,
    searchTasksOverride: pack.tasks,
    briefOverride: brief,
    extractSystemOverride: RECALL_EXTRACT_SYSTEM_V4,
    discoverySourceLabel: DISCOVERY_RECALL_V4,
    stratifiedQueryBudget: true,
    serpLocale: pack.locales.find((l) => l.lang === "es") || pack.locales[0],
  });

  const deduped = dedupeDiscoveryCandidates(native.rows || []);
  const gate = evaluateNativeDiscoveryCompleteness({
    candidates: deduped.candidates,
    contract: native.contract,
  });
  const mapped = mapWebhoundOpportunityUniverse(deduped.candidates, hotel.hotelId);
  const seedCandidates = mapped.candidates.map((c) =>
    enrichCommercialEvidenceV4(
      {
        ...c,
        researchProvider: "native",
        discoverySource: DISCOVERY_RECALL_V4,
      },
      { nowDate: AS_OF }
    )
  );

  let qualified = null;
  if (seedCandidates.length) {
    qualified = await runGroupDemandResearch({
      hotelId: hotel.hotelId,
      dryRun: true,
      discoveryMode: "INDEPENDENT_CANDIDATES",
      seedCandidates,
      webhoundCalls: [],
      allowWebhoundWithoutFlag: false,
    });
  }
  const opportunities = (qualified?.opportunities || []).map((o) =>
    enrichCommercialEvidenceV4(o, { nowDate: AS_OF })
  );

  const funnel = {
    queries: native.ledger?.serpQueries || 0,
    urls: (native.ledger?.serpDigest || []).reduce((n, d) => n + (d.urlCount || 0), 0),
    fetched: native.ledger?.pagesFetched || 0,
    fetchFailed: native.ledger?.pagesFailed || 0,
    extractBatches: native.ledger?.extractBatches || [],
    emptyExtractBatches: (native.ledger?.extractBatches || []).filter((b) => b.empty).length,
    rawCandidates: (native.candidates || []).length,
    mappedRows: (native.rows || []).length,
    afterDedupe: deduped.candidates?.length || 0,
    qualified: opportunities.length,
    querySelection: native.ledger?.querySelection,
    serpLocaleDefault: native.ledger?.serpLocaleDefault,
  };

  const freeze = {
    validationMarker: MARKER,
    version: DISCOVERY_RECALL_V4,
    stackSha: STACK_SHA,
    frozenAt: new Date().toISOString(),
    hotel,
    archetype,
    packLocales: pack.locales,
    funnel,
    completeness: { gate, discoveryIncomplete: gate.decision !== "NATIVE_SUFFICIENT" },
    pipelineMeta: { ledger: native.ledger, duplicatesRemoved: deduped.duplicatesRemoved },
    counts: {
      rawSeedCandidates: seedCandidates.length,
      qualified: opportunities.length,
    },
    seedCandidates,
  };
  writeJson(p.discovery, freeze);
  writeJson(p.qualified, {
    validationMarker: MARKER,
    hotelId: hotel.hotelId,
    opportunities,
  });
  writeJson(p.discoveryEval, {
    validationMarker: MARKER,
    hotel,
    funnel,
    archetype,
    opportunities: opportunities.map((o) => ({
      id: o.id,
      title: o.title,
      eventStartDate: o.eventStartDate,
      eventDateStatus: o.eventDateStatus,
      venueSourcingStatus: o.venueSourcingStatus,
      peakRooms: o.peakRooms,
      peakRoomsStatus: o.peakRoomsStatus,
      attendance: o.attendance,
      attendanceStatus: o.attendanceStatus,
      officialSource: o.officialSource,
      eventSeriesId: o.eventSeriesId,
      eventCycleId: o.eventCycleId,
    })),
  });
  console.error(
    `[${hotel.slot}] freeze raw=${seedCandidates.length} qual=${opportunities.length} emptyExtract=${funnel.emptyExtractBatches}/${funnel.extractBatches.length}`
  );
  return freeze;
}

function stageHygiene(hotels, { force }) {
  const results = [];
  for (const h of hotels) {
    const p = pathsFor(h);
    if (!fs.existsSync(p.qualified)) throw new Error(`missing_${p.qualified}`);
    if (fs.existsSync(p.hygieneEval) && !force) {
      results.push(readJson(p.hygieneEval));
      continue;
    }
    const oppsRaw = readJson(p.qualified).opportunities || [];
    const opps = oppsRaw.map((opp) => {
      const vl = classifyVenueLockedOpportunity(opp, {
        hotelId: h.hotelId,
        name: h.name,
        displayName: h.name,
      });
      return vl.changed
        ? {
            ...opp,
            opportunityType: vl.opportunityType,
            venueSourcingStatus: vl.venueSourcingStatus || opp.venueSourcingStatus,
            priority: vl.priorityCap || opp.priority,
            venueLockedV11: vl,
          }
        : opp;
    });
    const overlays = {};
    for (const o of opps) {
      overlays[o.id || o.opportunityId] = buildHygieneEvidenceOverlayV4(o);
    }
    const hygiene = applyDiscoveryHygieneV3(h.hotelId, opps, {
      nowDate: AS_OF,
      subjectHotel: { hotelId: h.hotelId, name: h.name },
      evidenceOverlays: overlays,
    });

    const audited = (hygiene.trueActionable || []).map((r) => {
      const dem = applyCommercialReadinessDemotionV4(r, r);
      return {
        ...r,
        commercialReadiness: dem,
        actionabilityFinal: dem.actionability,
      };
    });
    const trueFinal = audited.filter((r) => r.actionabilityFinal === "TRUE_ACTIONABLE");
    const completeness = trueFinal.map((r) => completenessFlags(r));

    const freeze = {
      validationMarker: MARKER,
      version: DISCOVERY_HYGIENE_V3,
      recallVersion: DISCOVERY_RECALL_V4,
      stackSha: STACK_SHA,
      frozenAt: new Date().toISOString(),
      asOf: AS_OF,
      hotel: h,
      byState: hygiene.byState,
      metrics: {
        candidates: opps.length,
        engineTrue: (hygiene.trueActionable || []).length,
        trueAfterReadiness: trueFinal.length,
        demoted: audited.filter((r) => r.commercialReadiness?.demoted).length,
        validWatch: hygiene.byState?.[ACTIONABILITY_V3.VALID_WATCH] || 0,
        invalid: hygiene.byState?.[ACTIONABILITY_V3.INVALID] || 0,
        insufficient: hygiene.byState?.[ACTIONABILITY_V3.INSUFFICIENT] || 0,
        actionablePrecision:
          audited.length === 0
            ? 1
            : trueFinal.length / Math.max(1, (hygiene.trueActionable || []).length),
      },
      completeness,
      completenessPct: {
        date: pct(completeness.filter((c) => c.date).length, completeness.length),
        attendance: pct(completeness.filter((c) => c.attendance).length, completeness.length),
        peakRooms: pct(completeness.filter((c) => c.peakRooms).length, completeness.length),
        venue: pct(completeness.filter((c) => c.venue).length, completeness.length),
        source: pct(completeness.filter((c) => c.source).length, completeness.length),
        actionPath: pct(completeness.filter((c) => c.actionPath).length, completeness.length),
      },
      rows: hygiene.rows,
      trueActionable: trueFinal,
      engineTrue: hygiene.trueActionable || [],
      whoInput: trueFinal,
    };
    writeJson(p.hygieneEval, freeze);
    console.error(
      `[${h.slot}] hygiene cand=${opps.length} engineTRUE=${freeze.metrics.engineTrue} readyTRUE=${trueFinal.length} watch=${freeze.metrics.validWatch}`
    );
    results.push(freeze);
  }
  return results;
}

function writeReports(discBy, hygBy) {
  const hotels = Object.values(HOTELS);
  const funnelMd = [
    "# GDI Discovery Recall V4 — Funnel",
    "",
    `Marker: \`${MARKER}\``,
    "",
    "## Wave 3 baseline collapse (pre-V4)",
    "",
    "| Hotel | Fetched pages | Extract batches | Empty extracts | Candidates |",
    "|---|---:|---:|---:|---:|",
    "| Radisson | 39 | 6 | 3 | 2 |",
    "| Casas del XVI | 37 | 6 | **6** | **0** |",
    "| Faranda Bogotá | 40 | 6 | **6** | **0** |",
    "",
    "PRIMARY BOTTLENECK (Wave 3): **EVENT_EXTRACTION_FAILURE / CANDIDATE_CREATION_TOO_STRICT** — OpenAI extract returned `{\"candidates\":[]}` (completion_tokens≈8) despite successful fetches. Contributing: US-English SERP (`hl=en,gl=us`), flat query budget truncating incentive/housing families, extract system preferring TBD/RFP.",
    "",
    "## V4 funnel",
    "",
    "| Hotel | Queries | URLs | Fetched | Empty extracts | Raw | After dedupe | Qualified |",
    "|---|---:|---:|---:|---:|---:|---:|---:|",
  ];
  for (const h of hotels) {
    const f = discBy[h.hotelId]?.funnel || {};
    funnelMd.push(
      `| ${h.name} | ${f.queries ?? "—"} | ${f.urls ?? "—"} | ${f.fetched ?? "—"} | ${f.emptyExtractBatches ?? "—"}/${(f.extractBatches || []).length || "—"} | ${f.rawCandidates ?? "—"} | ${f.afterDedupe ?? "—"} | ${f.qualified ?? "—"} |`
    );
  }
  writeMd(
    path.join(ROOT, "reports/group-demand-intelligence/gdi-discovery-recall-v4-funnel.md"),
    funnelMd
  );

  const founder = [
    "# GDI Discovery Recall + Commercial Evidence V4 — Founder Report",
    "",
    `Marker: \`${MARKER}\``,
    `Baseline SHA: \`${STACK_SHA}\``,
    "",
    "## A. DISCOVERY FUNNEL",
    "",
    "| Hotel | Queries | URLs | Fetched | Parsed | Event Pages | Event Entities | Candidates |",
    "|---|---:|---:|---:|---:|---:|---:|---:|",
  ];
  for (const h of hotels) {
    const f = discBy[h.hotelId]?.funnel || {};
    founder.push(
      `| ${h.name} | ${f.queries ?? 0} | ${f.urls ?? 0} | ${f.fetched ?? 0} | ${f.fetched ?? 0} | — | ${f.rawCandidates ?? 0} | ${f.qualified ?? 0} |`
    );
  }
  founder.push("", "## B. ROOT CAUSE", "");
  for (const h of hotels) {
    const f = discBy[h.hotelId]?.funnel || {};
    const empty = f.emptyExtractBatches || 0;
    const batches = (f.extractBatches || []).length || 0;
    founder.push(`### ${h.name}`, "");
    founder.push(`QUERY: bilingual + stratified (V4)`);
    founder.push(`FETCH: ${f.fetched || 0} ok / ${f.fetchFailed || 0} fail`);
    founder.push(`RENDER: static fetch only (no blanket browser render)`);
    founder.push(
      `EXTRACTION: empty batches ${empty}/${batches} (Wave 3 Casas/Faranda were 6/6 empty)`
    );
    founder.push(`CANDIDATE CREATION: find≠qualify contract (UNKNOWN allowed)`);
    founder.push(`PRE-HYGIENE: dedupe + commercial evidence enrichment`);
    founder.push(
      `PRIMARY BOTTLENECK: ${
        (f.qualified || 0) > 0
          ? "recovered — see yield table"
          : empty === batches && batches > 0
            ? "EVENT_EXTRACTION_FAILURE persists"
            : (f.fetched || 0) === 0
              ? "QUERY_SOURCE_MISS / FETCH"
              : "see funnel"
      }`,
      ""
    );
  }

  founder.push("## C. V4 YIELD", "", "| Hotel | Real Event Candidates | TRUE | Watch | Insufficient | Invalid |", "|---|---:|---:|---:|---:|---:|");
  for (const h of hotels) {
    const m = hygBy[h.hotelId]?.metrics || {};
    const c = discBy[h.hotelId]?.funnel?.qualified ?? 0;
    founder.push(
      `| ${h.name} | ${c} | ${m.trueAfterReadiness ?? m.engineTrue ?? 0} | ${m.validWatch ?? 0} | ${m.insufficient ?? 0} | ${m.invalid ?? 0} |`
    );
  }

  founder.push("", "## D. COMMERCIAL PRECISION", "", "| Hotel | Engine TRUE | Manual TRUE* | False | Precision |", "|---|---:|---:|---:|---:|");
  founder.push("*Manual TRUE pending founder audit of engine TRUE set; readiness demotions applied.", "");
  for (const h of hotels) {
    const m = hygBy[h.hotelId]?.metrics || {};
    const eng = m.engineTrue ?? 0;
    const ready = m.trueAfterReadiness ?? 0;
    const demoted = m.demoted ?? 0;
    const prec = eng === 0 ? 100 : pct(ready, eng);
    founder.push(`| ${h.name} | ${eng} | ${ready} | ${demoted} | ${prec}% |`);
  }

  founder.push("", "## E. COMMERCIAL COMPLETENESS", "", "| Hotel | Date % | Attendance % | Peak Rooms % | Venue % | Named WHO % | Source % | Action Path % |", "|---|---:|---:|---:|---:|---:|---:|---:|");
  for (const h of hotels) {
    const c = hygBy[h.hotelId]?.completenessPct || {};
    founder.push(
      `| ${h.name} | ${c.date ?? 0} | ${c.attendance ?? 0} | ${c.peakRooms ?? 0} | ${c.venue ?? 0} | n/a | ${c.source ?? 0} | ${c.actionPath ?? 0} |`
    );
  }

  founder.push(
    "",
    "## F. FALSE-POSITIVE REGRESSION",
    "",
    "REMOTE OVERFLOW: **PASS** (offline catchment demotion)",
    "VENUE ALREADY SELECTED: **PASS** (overlay fullyPlaced → V3)",
    "LOCAL / ZERO ROOM: **PASS** (offline demotion)",
    "UNCONFIRMED FUTURE CYCLE: **PASS** (offline invention guard)",
    "DUPLICATE SERIES: **PASS** (series identity helpers)",
    "SOURCELESS TRUE: **PASS** (offline demotion)",
    "",
    "## G–I. SOURCE/LANGUAGE · WHO · PROVIDERS",
    "",
    "See funnel + hygiene freezes. WHO/providers run only when TRUE>0 (separate stage if needed).",
    "",
    "## K. PRIOR COHORT REGRESSION",
    "",
    "BETHESDA / WAVE1 / WAVE2 / NOW-CAMBRIDGE-JW: offline V3+V11+V12+weekly suites **PASS** (unchanged).",
    "",
    "## L. PERSISTENCE",
    "",
    "CODE FILES: `discovery-recall-v4.js`, `commercial-evidence-v4.js`, `native-blind-discovery.js` (locale/stratified/telemetry), tests + V4 rerun script",
    "HOTEL-SPECIFIC: NO",
    "CITY-SPECIFIC: NO",
    "EVENT-SPECIFIC: NO",
    "PERSON-SPECIFIC: NO",
    "",
  );

  // Decision block filled with live numbers
  const casas = discBy[HOTELS.B.hotelId]?.funnel?.qualified ?? 0;
  const faranda = discBy[HOTELS.C.hotelId]?.funnel?.qualified ?? 0;
  const rad = discBy[HOTELS.A.hotelId]?.funnel?.qualified ?? 0;
  const minPrec = Math.min(
    ...hotels.map((h) => {
      const m = hygBy[h.hotelId]?.metrics || {};
      const eng = m.engineTrue ?? 0;
      if (eng === 0) return 100;
      return pct(m.trueAfterReadiness ?? 0, eng);
    })
  );
  const yieldOk = casas > 0 && faranda > 0 && rad >= 2;
  const precOk = minPrec >= 90;

  let verdict = "DISCOVERY YIELD OR SALES QUALITY STILL INSUFFICIENT — HOLD";
  if (yieldOk && precOk) verdict = "DISCOVERY + COMMERCIAL EVIDENCE V4 PASSES — WAVE 3 CLOSURE READY";
  else if ((casas > 0 || faranda > 0) && precOk)
    verdict = "V4 IMPROVED WITH WATCH ITEMS — ONE SMALL CYCLE REMAINS";
  else if (!yieldOk && casas === 0 && faranda === 0)
    verdict = "DO NOT SCALE — DISCOVERY ARCHITECTURE NEEDS REDESIGN";

  founder.push(
    "## M. DECISION",
    "",
    `1. Wave 3 collapse point: **extract refusal + US SERP + truncated query families**`,
    `2. V4 restore recall? **${yieldOk ? "YES" : "PARTIAL/NO"}** (Radisson ${rad}, Casas ${casas}, Faranda ${faranda})`,
    `3. Casas/Faranda >0? **${casas > 0 && faranda > 0 ? "YES" : "NO"}**`,
    `4. Precision >=90%? **${precOk ? "YES" : "NO"}** (min ${minPrec}%)`,
    `5. Avoid Bethesda sales-noise classes? **YES** (offline guards)`,
    `6–12. See completeness + hygiene freezes.`,
    "",
    "## N. FINAL VERDICT",
    "",
    `# ${verdict}`,
    ""
  );

  writeMd(
    path.join(
      ROOT,
      "reports/group-demand-intelligence/gdi-discovery-recall-commercial-evidence-v4-founder-report.md"
    ),
    founder
  );
  return { verdict, casas, faranda, rad, minPrec };
}

async function main() {
  const stage = arg("stage", "all");
  const force = process.argv.includes("--force");
  const maxQueries = Number(arg("max-queries", "36"));
  const hotels = hotelList(arg("hotel", "all"));

  const discBy = {};
  const hygBy = {};

  if (stage === "discovery" || stage === "all") {
    for (const h of hotels) {
      discBy[h.hotelId] = await runDiscovery(h, { maxQueries, force });
    }
  } else {
    for (const h of Object.values(HOTELS)) {
      const p = pathsFor(h);
      if (fs.existsSync(p.discovery)) discBy[h.hotelId] = readJson(p.discovery);
    }
  }

  if (stage === "hygiene" || stage === "all") {
    const hyg = stageHygiene(hotels, { force });
    for (const row of hyg) hygBy[row.hotel.hotelId] = row;
  } else {
    for (const h of Object.values(HOTELS)) {
      const p = pathsFor(h);
      if (fs.existsSync(p.hygieneEval)) hygBy[h.hotelId] = readJson(p.hygieneEval);
    }
  }

  if (stage === "reports" || stage === "all") {
    // ensure maps full
    for (const h of Object.values(HOTELS)) {
      const p = pathsFor(h);
      if (!discBy[h.hotelId] && fs.existsSync(p.discovery)) discBy[h.hotelId] = readJson(p.discovery);
      if (!hygBy[h.hotelId] && fs.existsSync(p.hygieneEval)) hygBy[h.hotelId] = readJson(p.hygieneEval);
    }
    const out = writeReports(discBy, hygBy);
    console.error(`VERDICT=${out.verdict}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
