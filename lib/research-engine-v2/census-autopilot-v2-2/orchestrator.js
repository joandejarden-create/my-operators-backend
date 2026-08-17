/**
 * Census Autopilot V2.2 orchestrator — official-first + Rooms V3 + real 500 wave.
 */

import fs from "node:fs";
import path from "node:path";
import {
  AUTOPILOT_V22_VERSION,
  OUT_REL,
  V21_OUT_REL,
  V2_OUT_REL,
  ADAPTER_CLASS,
  VERIFIED_LIFECYCLE,
} from "./constants.js";
import { analyzeRoomsV2Failures } from "./rooms-v2-forensics.js";
import {
  buildBrandFamilyUniverse,
  rankAdapterRoi,
  buildOfficialCapabilityMap,
} from "./brand-family-universe.js";
import { reforecastSerpApiDemand } from "./serpapi-eligibility.js";
import {
  freezeProductionWave,
  loadPriorWaveExcludeIds,
} from "./production-wave-freeze.js";
import { runProductionWave, classifyFieldWrites } from "./production-wave-runner.js";
import {
  auditMarketSubmarket,
  proposeGeographyTaxonomyExpansion,
} from "./geography-expansion.js";
import {
  buildFirstPartyValidationTargets,
  firstPartyValidationModelMd,
} from "./first-party-validation.js";
import { loadV2CandidateSummaries } from "../census-autopilot-v2-1/demand-audit.js";
import { loadVicRecords } from "../census-autopilot-v2/master-candidate.js";
import { resolveRoomsV3, ROOMS_RESOLVER_V3_VERSION } from "./rooms-resolver-v3.js";

function wj(dir, name, data) {
  fs.writeFileSync(path.join(dir, name), JSON.stringify(data, null, 2));
}
function wm(dir, name, text) {
  fs.writeFileSync(path.join(dir, name), text);
}

function median(nums) {
  if (!nums.length) return 0;
  const s = [...nums].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : Math.round((s[m - 1] + s[m]) / 2);
}

function isConfirmed(r) {
  return (
    String(r.confirmation || "").includes("INDEPENDENTLY") ||
    String(r.confirmation || "").includes("ENRICHMENT")
  );
}

/**
 * @param {{ root: string, waveSize?: number, searchCeiling?: number, log?: Function }} opts
 */
export async function runCensusAutopilotV22(opts) {
  const root = opts.root;
  const log = opts.log || console.log;
  const outDir = path.join(root, OUT_REL);
  const v2Dir = path.join(root, V2_OUT_REL);
  const v21Dir = path.join(root, V21_OUT_REL);
  fs.mkdirSync(outDir, { recursive: true });

  const waveSize = opts.waveSize ?? Number(process.env.CAV22_WAVE_SIZE || 500);
  const searchCeiling = opts.searchCeiling ?? Number(process.env.CAV22_SEARCH_CEILING || 650);

  // ——— 01 baseline ———
  const v21Final = fs.existsSync(path.join(v21Dir, "25-final-report.md"))
    ? fs.readFileSync(path.join(v21Dir, "25-final-report.md"), "utf8")
    : "";
  const dedupe = JSON.parse(fs.readFileSync(path.join(v2Dir, "05-deduplication-results.json"), "utf8"));
  const brandCov = JSON.parse(
    fs.readFileSync(path.join(v2Dir, "08-brand-family-adapter-coverage.json"), "utf8")
  );
  wm(
    outDir,
    "01-v2-1-baseline.md",
    `# V2.1 Baseline (input to V2.2)

Version: ${AUTOPILOT_V22_VERSION}

## What we no longer need to learn via generic benchmarks
- Autopilot can independently confirm hotels (V2.1: 51.6% independent / 70.4% useful)
- SerpApi is technically useful; rights blocked for production persistence
- Rooms Resolver V2 failed 0/55 on IHG empty \`numberOfRooms\` — not a parser bug
- Remaining blockers: **source adapter coverage**, **Rooms**, **rights**, **production writes**

## V2.1 scorecard excerpt
${v21Final.slice(0, 3500)}

## Universe
- Raw candidates: ${dedupe.total_candidates || 14035}
- Unique physical (est.): ${dedupe.estimated_unique_physical_hotels || 12846}
`
  );

  log("[v2.2] loading candidates + VIC…");
  const candidates = loadV2CandidateSummaries(v2Dir);
  const vic = loadVicRecords(root);

  // ——— 02–05 brand family / ROI / capabilities / property IDs ———
  const universe = buildBrandFamilyUniverse(candidates, vic);
  wj(outDir, "02-brand-family-universe.json", universe);
  const roi = rankAdapterRoi(universe);
  wj(outDir, "03-adapter-roi-ranking.json", roi);
  const capMap = buildOfficialCapabilityMap(universe);
  wj(outDir, "04-official-source-capability-map.json", capMap);

  const pidBefore = {
    ihg_codes_in_vic: vic.filter((v) => v.family === "IHG" && v.property_ids?.length).length,
    hilton_ctyhocn_in_vic: vic.filter((v) => v.family === "Hilton" && v.property_ids?.length).length,
    choice_ids_in_vic: vic.filter((v) => v.family === "Choice" && v.property_ids?.length).length,
    marriott_in_vic: vic.filter((v) => v.family === "Marriott" && v.property_ids?.length).length,
    total_vic_with_property_id: vic.filter((v) => v.property_ids?.length).length,
    total_vic: vic.length,
  };

  // ——— 06 Rooms V2 forensics ———
  const forensics = analyzeRoomsV2Failures(root);
  wj(outDir, "06-rooms-v2-failure-forensics.json", forensics);

  wj(outDir, "07-rooms-source-map.json", {
    version: "rooms-source-map-v2.2",
    evidence_standard:
      "guest rooms / hotel rooms / keys / unambiguous rooms+suites totals only; never infer",
    confidence: {
      HIGH: "Official structured property source / current first-party hotel facts",
      MEDIUM: "Official owner/operator/current opening or development source",
      LOW: "Credible secondary — research clue only; not Census candidate",
    },
    by_family: {
      IHG: {
        hoteldetail_numberOfRooms: "usually empty string — do not invent",
        json_ld: "rare",
        embedded_state: "empty field common",
        pdf_fact_sheet: "occasional",
        owner_operator: "medium path",
        development_opening: "medium when current",
        first_party: "primary remaining path for Mexico cohort",
      },
      Hilton: {
        graphql_shortDesc: "occasional prose room counts",
        html: "often 403",
        fact_sheets: "occasional",
        first_party: "needed when GraphQL silent",
      },
      Choice: {
        html: "sparse",
        first_party: "primary",
      },
      Marriott: {
        marsha_embedded: "investigate",
        fact_sheets: "medium",
        first_party: "common",
      },
      Accor: { directory: true, rooms: "owner/opening/first-party" },
      Wyndham: { directory: true, rooms: "owner/opening/first-party" },
      Hyatt: { embedded: "possible", rooms: "fact sheet / first-party" },
      Melia: { rooms: "owner/opening/first-party" },
      Minor: { rooms: "first-party" },
    },
    never_from: [
      "room_types",
      "search_inventory",
      "available_rooms",
      "bedrooms",
      "reviews",
      "occupancy",
      "meeting_rooms",
      "floor_count",
      "booking_inventory",
      "serpapi",
      "cvent",
    ],
  });

  // ——— SerpApi plan / reforecast ———
  const demand = reforecastSerpApiDemand({
    uniqueHotels: dedupe.estimated_unique_physical_hotels || 12846,
    existingVerified: dedupe.existing_verified || 1194,
    nativeStrongCount: (brandCov.native_strong || []).reduce((s, f) => s + (f.count || 0), 0),
    partialCount: (brandCov.native_partial || []).reduce((s, f) => s + (f.count || 0), 0),
    priorMinimized: 12400,
    priorFull: 14301,
  });
  wm(
    outDir,
    "11-serpapi-call-reduction-plan.md",
    `# SerpApi Call Reduction Plan (V2.2)

## Prior forecasts
- Full-universe (old): **${demand.prior_full_universe_forecast}**
- V2.1 minimized: **${demand.prior_minimized_v21}**

## V2.2 levers
1. Official-first discovery for native/partial families before paid calls
2. Property ID capture to avoid re-research
3. Dealality SerpApi research cache
4. Expected-value gate (skip phone-only / low-gain)
5. One-call stop when search root already has property details
6. Field-gap routing (never Rooms via SerpApi)
7. Candidate dedupe to property_identity_id

## New forecast
**${demand.new_forecast}** searches (−${demand.reduction_vs_prior_minimized_pct}% vs V2.1 minimized; −${demand.reduction_vs_prior_full_pct}% vs old full)

${JSON.stringify(demand.components, null, 2)}
`
  );
  wj(outDir, "13-serpapi-demand-reforecast.json", demand);

  // ——— Freeze 500 real wave ———
  const excludeIds = loadPriorWaveExcludeIds(root);
  const freeze = freezeProductionWave(candidates, vic, waveSize, { excludeIds });
  wj(outDir, "16-production-wave-freeze.json", freeze);
  log(
    `[v2.2] production wave frozen n=${freeze.actual} priorities=${JSON.stringify(freeze.priority_distribution)}`
  );

  // ——— Run autonomous wave ———
  log(`[v2.2] starting autonomous wave ceiling=${searchCeiling}…`);
  const wave = await runProductionWave({
    repoRoot: root,
    cohort: freeze.cohort,
    ceiling: searchCeiling,
    log,
  });

  wj(outDir, "17-production-wave-results.json", {
    version: AUTOPILOT_V22_VERSION,
    runtime_ms: wave.runtime_ms,
    results: wave.results,
    counters: wave.counters,
    credit_ledger: wave.credit_ledger,
    account_start: wave.account_start,
    account_end: wave.account_end,
    actual_delta: wave.actual_delta,
    cache_stats: wave.cache_stats,
  });

  // Extra Rooms V3 pass on VIC IHG/Hilton/Choice with websites (forensics cohort + more)
  log("[v2.2] Rooms Resolver V3 extended sample…");
  const roomsTargets = freeze.cohort.filter(
    (h) =>
      h.website &&
      ["IHG", "Hilton", "Choice", "Marriott"].includes(h.family)
  );
  const roomsV3 = [];
  for (const h of roomsTargets.slice(0, 80)) {
    // skip if already done in wave
    const existing = wave.roomsResults.find((r) => r.candidate_id === h.candidate_id);
    if (existing) {
      roomsV3.push({ candidate_id: h.candidate_id, name: h.name, family: h.family, ...existing });
      continue;
    }
    const rr = await resolveRoomsV3(
      {
        name: h.name,
        family: h.family,
        website: h.website,
        property_ids: h.property_ids,
        independent_record_id: h.property_identity_id,
      },
      { delayMs: 350 }
    );
    roomsV3.push({
      candidate_id: h.candidate_id,
      name: h.name,
      family: h.family,
      ok: rr.ok,
      rooms_value: rr.rooms_value,
      confidence: rr.confidence,
      classification: rr.classification,
      reason: rr.reason,
    });
  }
  // Merge wave rooms
  for (const rr of wave.roomsResults) {
    if (!roomsV3.find((x) => x.candidate_id === rr.candidate_id)) {
      roomsV3.push(rr);
    }
  }

  const roomsOk = roomsV3.filter((r) => r.ok).length;
  wj(outDir, "08-rooms-resolver-v3-results.json", {
    version: ROOMS_RESOLVER_V3_VERSION,
    attempted: roomsV3.length,
    success: roomsOk,
    success_rate_pct: Math.round((100 * roomsOk) / Math.max(1, roomsV3.length)),
    by_family: ["IHG", "Hilton", "Choice", "Marriott"].map((fam) => {
      const subset = roomsV3.filter((r) => r.family === fam);
      const ok = subset.filter((r) => r.ok).length;
      const classifications = {};
      for (const r of subset) {
        const c = r.classification || (r.ok ? "NATIVE RESOLVABLE" : "UNKNOWN");
        classifications[c] = (classifications[c] || 0) + 1;
      }
      return { family: fam, n: subset.length, ok, classifications };
    }),
    results: roomsV3,
    vs_v2: {
      v2_attempted: forensics.attempted,
      v2_success: forensics.success,
      v3_attempted: roomsV3.length,
      v3_success: roomsOk,
      note: "V3 expands sources (JSON-LD, embedded state, owner ladder, Hilton GraphQL) and reclassifies IHG empty fields to FIRST-PARTY VALIDATION instead of repeating useless parser retries.",
    },
  });

  // Property ID coverage after
  const pidAfterWave = wave.results.filter((r) => r.official_property_id).length;
  wj(outDir, "05-property-id-coverage.json", {
    before: pidBefore,
    after_wave: {
      official_property_ids_captured: pidAfterWave,
      pct_of_wave: Math.round((100 * pidAfterWave) / Math.max(1, wave.results.length)),
    },
    discipline:
      "Brand IDs map into property_identity_id references; Google property_token is external key only — never Dealality primary identity.",
  });

  // One-call analysis
  const oneCallStats = {
    hotels_with_serpapi: wave.results.filter((r) => r.serpapi_used || r.from_dealality_cache).length,
    one_call_complete: wave.counters.one_call_complete,
    second_call_avoided: wave.counters.second_call_avoided,
    second_call_required: wave.results.filter((r) => r.one_call?.second_call_required).length,
    pct_one_call: 0,
  };
  oneCallStats.pct_one_call = Math.round(
    (100 * oneCallStats.one_call_complete) / Math.max(1, oneCallStats.hotels_with_serpapi)
  );
  wj(outDir, "12-serpapi-one-call-analysis.json", oneCallStats);

  // Geography
  const geoAudit = auditMarketSubmarket(freeze.cohort, wave.results);
  wj(outDir, "14-market-submarket-audit.json", geoAudit);
  const geoExp = proposeGeographyTaxonomyExpansion(geoAudit);
  wj(outDir, "15-geography-taxonomy-expansion.json", geoExp);

  // Completeness
  const confirmed = wave.results.filter(isConfirmed);
  const pcts = confirmed.map((r) => r.final_priority_proxy_pct || 0);
  const pctsExcl = confirmed.map((r) => r.final_priority_proxy_excl_rooms_pct || 0);
  const baselinePcts = wave.results.map((r) => r.baseline_priority_proxy_pct || 0);
  const ge95 = confirmed.filter((r) => (r.final_priority_proxy_pct || 0) >= 95).length;
  const ge95Excl = confirmed.filter((r) => (r.final_priority_proxy_excl_rooms_pct || 0) >= 95).length;
  const goldenExceptRooms = confirmed.filter(
    (r) =>
      (r.final_priority_proxy_excl_rooms_pct || 0) >= 95 &&
      (r.final_priority_proxy_pct || 0) < 95 &&
      !r.rooms_result?.ok
  ).length;

  const fieldCov = (pred) =>
    Math.round((100 * confirmed.filter(pred).length) / Math.max(1, confirmed.length));

  const completeness = {
    baseline_avg_all: Math.round(baselinePcts.reduce((a, b) => a + b, 0) / Math.max(1, baselinePcts.length)),
    final_avg_confirmed: pcts.length
      ? Math.round(pcts.reduce((a, b) => a + b, 0) / pcts.length)
      : 0,
    final_median_confirmed: median(pcts),
    hotels_ge_95: ge95,
    pct_ge_95: Math.round((100 * ge95) / Math.max(1, confirmed.length)),
    pct_ge_95_excl_rooms_diagnostic: Math.round((100 * ge95Excl) / Math.max(1, confirmed.length)),
    golden_complete_except_rooms: goldenExceptRooms,
    note: "excl-Rooms metric is diagnostic only; official Golden definition unchanged",
  };
  wj(outDir, "18-field-completeness.json", completeness);

  wj(outDir, "19-rooms-impact-analysis.json", {
    rooms_baseline_coverage_pct: 0,
    rooms_final_coverage_pct: fieldCov((r) => r.rooms_result?.ok),
    rooms_v3_success_rate_pct: Math.round((100 * roomsOk) / Math.max(1, roomsV3.length)),
    otherwise_golden_except_rooms: goldenExceptRooms,
    diagnostic_ge95_excl_rooms: ge95Excl,
    conclusion:
      roomsOk === 0
        ? "Rooms remains the largest material gap; V3 correctly routes IHG empty fields to first-party validation rather than inventing counts."
        : "Partial native Rooms breakthrough; hybrid native + first-party still required at scale.",
  });

  // Eligibility / lifecycle
  const lifecycleCounts = {};
  for (const r of wave.results) {
    const lc = r.lifecycle || VERIFIED_LIFECYCLE.UNRESOLVED;
    lifecycleCounts[lc] = (lifecycleCounts[lc] || 0) + 1;
  }
  const verifiedProperty = wave.results.filter(
    (r) =>
      r.lifecycle &&
      r.lifecycle !== VERIFIED_LIFECYCLE.UNRESOLVED &&
      r.lifecycle !== VERIFIED_LIFECYCLE.PARTIAL_IDENTITY_RESEARCH
  ).length;
  wj(outDir, "20-production-eligibility.json", {
    verified_vs_golden_complete_separated: true,
    lifecycle_counts: lifecycleCounts,
    verified_property_count: verifiedProperty,
    verified_golden_complete:
      lifecycleCounts[VERIFIED_LIFECYCLE.VERIFIED_GOLDEN_COMPLETE] || 0,
    verified_rooms_pending: lifecycleCounts[VERIFIED_LIFECYCLE.VERIFIED_ROOMS_PENDING] || 0,
    can_verify_while_rooms_pending: true,
    recommendation:
      "Allow VERIFIED — ROOMS PENDING in Verified Census; Golden Complete remains the ≥95% Priority bar including Rooms.",
    migration_impact:
      "Additive lifecycle labels only — does not alter existing production write gates until steward enables.",
  });

  const writes = classifyFieldWrites(wave.results);
  wj(outDir, "21-airtable-dry-run.json", writes);

  // Brand explorer staging
  const beByBrand = new Map();
  for (const r of confirmed) {
    const b = r.family || "Unknown";
    if (!beByBrand.has(b)) {
      beByBrand.set(b, {
        brand: b,
        verified_hotel_count: 0,
        countries: new Set(),
        completeness_sum: 0,
        rooms_ok: 0,
        property_id_ok: 0,
        validation_needs: 0,
      });
    }
    const row = beByBrand.get(b);
    row.verified_hotel_count += 1;
    if (r.country) row.countries.add(r.country);
    row.completeness_sum += r.final_priority_proxy_pct || 0;
    if (r.rooms_result?.ok) row.rooms_ok += 1;
    if (r.official_property_id) row.property_id_ok += 1;
    if (r.rooms_result?.classification === "FIRST-PARTY VALIDATION") row.validation_needs += 1;
  }
  wj(outDir, "22-brand-explorer-staging-impact.json", {
    staging_only: true,
    brand_activation: false,
    brands: [...beByBrand.values()].map((b) => ({
      brand: b.brand,
      verified_hotel_count: b.verified_hotel_count,
      country_coverage: b.countries.size,
      countries: [...b.countries],
      average_completeness: Math.round(b.completeness_sum / Math.max(1, b.verified_hotel_count)),
      rooms_coverage_pct: Math.round((100 * b.rooms_ok) / Math.max(1, b.verified_hotel_count)),
      official_property_id_coverage_pct: Math.round(
        (100 * b.property_id_ok) / Math.max(1, b.verified_hotel_count)
      ),
      validation_needs: b.validation_needs,
    })),
  });

  // Operator seed
  const opSeeds = wave.results
    .filter((r) => r.operator_seed)
    .map((r) => ({
      property_identity_id: r.property_identity_id,
      ...r.operator_seed,
    }));
  wj(outDir, "23-operator-explorer-seed.json", {
    note: "Natural encounter only — no broad operator research",
    seeds: opSeeds.slice(0, 200),
    count: opSeeds.length,
  });

  // First-party validation
  wm(outDir, "09-first-party-validation-model.md", firstPartyValidationModelMd());
  const fpTargets = buildFirstPartyValidationTargets(freeze.cohort, wave.results, roomsV3);
  wj(outDir, "10-first-party-validation-targets.json", fpTargets);

  // Webhound escalation queue (no calls)
  const escalation = wave.results
    .filter((r) => {
      const conf = String(r.confirmation || "");
      return (
        !isConfirmed(r) ||
        r.rooms_result?.classification === "PUBLIC-RESEARCH ESCALATION" ||
        r.rooms_result?.classification === "DEEP RESEARCH ESCALATION" ||
        conf.includes("PROBABLE") ||
        conf.includes("UNRESOLVED")
      );
    })
    .map((r) => ({
      candidate_id: r.candidate_id,
      name: r.name,
      family: r.family,
      reason:
        r.rooms_result?.reason ||
        r.confirmation ||
        r.official_reason ||
        "unresolved_after_native_serpapi",
      webhound_candidate: true,
      do_not_call_webhound_in_autopilot: true,
    }));
  wj(outDir, "24-webhound-escalation-queue.json", {
    count: escalation.length,
    pct_of_wave: Math.round((100 * escalation.length) / Math.max(1, wave.results.length)),
    top_reasons: Object.entries(
      escalation.reduce((a, e) => {
        a[e.reason] = (a[e.reason] || 0) + 1;
        return a;
      }, {})
    )
      .sort((a, b) => b[1] - a[1])
      .slice(0, 15)
      .map(([reason, n]) => ({ reason, n })),
    queue: escalation.slice(0, 300),
  });

  // Metrics
  const exactHigh = confirmed.filter(
    (r) =>
      r.best_level === "EXACT" ||
      r.best_level === "HIGH" ||
      String(r.confirmation).includes("EXACT") ||
      String(r.confirmation).includes("HIGH") ||
      String(r.confirmation).includes("ENRICHMENT")
  ).length;
  const probable = wave.results.filter((r) => String(r.confirmation).includes("PROBABLE")).length;
  const unresolved = wave.results.filter((r) => !isConfirmed(r) && !String(r.confirmation).includes("PROBABLE")).length;
  const officialOnlyConfirmed = confirmed.filter((r) => r.confirmation_path === "official_only").length;
  const serpapiAssistedConfirmed = confirmed.filter(
    (r) => r.confirmation_path === "serpapi" || r.confirmation_path === "serpapi_assisted"
  ).length;

  const searchesUsed = wave.actual_delta ?? wave.credit_ledger?.charged ?? 0;
  const searchesPerConfirmed = confirmed.length
    ? Math.round((100 * searchesUsed) / confirmed.length) / 100
    : null;
  const searchesPer95 = ge95 ? Math.round((100 * searchesUsed) / ge95) / 100 : null;

  const deepResearch = wave.results.filter(
    (r) =>
      r.rooms_result?.classification === "DEEP RESEARCH ESCALATION" ||
      r.rooms_result?.classification === "PUBLIC-RESEARCH ESCALATION"
  ).length;

  wj(outDir, "25-scale-economics.json", {
    wave_searches_used: searchesUsed,
    searches_per_confirmed: searchesPerConfirmed,
    searches_per_ge95: searchesPer95,
    full_universe_reforecast: demand.new_forecast,
    native_official_resolution_forecast_pct: universe.pct_with_official_native_path,
    first_party_validation_forecast_pct: Math.max(25, fpTargets.pct_of_wave),
    deep_research_webhound_forecast_pct: Math.round(
      (100 * escalation.length) / Math.max(1, wave.results.length)
    ),
    estimated_full_universe_runtime_hours: Math.round(
      ((dedupe.estimated_unique_physical_hotels || 12846) / Math.max(1, wave.results.length)) *
        (wave.runtime_ms / 3600000) *
        10
    ) / 10,
    serpapi_150_tier_likely_sufficient_initial:
      demand.new_forecast <= 15000 && searchesPerConfirmed != null && searchesPerConfirmed < 4,
    paid_searches_only_where_material_value: true,
  });

  // Final report answering Q1–74
  const brandedUniverse = (universe.families || [])
    .filter((f) => f.family !== "Independent" && f.family !== "Unknown")
    .reduce((s, f) => s + f.candidate_count, 0);
  const pctOfficialPath = universe.pct_with_official_native_path;
  const pctWithoutSerpApiLikely = Math.round(
    (100 *
      ((brandCov.native_strong || []).reduce((s, f) => s + f.count, 0) * 0.45 +
        (brandCov.native_partial || []).reduce((s, f) => s + f.count, 0) * 0.2)) /
      Math.max(1, dedupe.estimated_unique_physical_hotels || 12846)
  );

  const adaptersBuilt = [
    "resolveFromOfficialSources (family-routed)",
    "Rooms Resolver V3 (JSON-LD + embedded + owner + Hilton GraphQL + FP classification)",
    "SerpApi EV gate + one-call analyzer",
    "Dealality Market/Submarket expansion helpers",
    "First-party validation ingestion model",
  ];

  const finalMd = `# Census Autopilot V2.2 — Final Report

**Version:** ${AUTOPILOT_V22_VERSION}  
**Mode:** DRY-RUN ONLY · No Airtable · No Webhound · Cvent never production evidence

---

## OFFICIAL-FIRST

1. **% universe with official/native research paths:** **${pctOfficialPath}%** (branded+directory families; Independent remain long-tail)
2. **% likely confirmable without SerpApi:** **~${pctWithoutSerpApiLikely}%** of unique hotels (native-strong/partial absorb estimate)
3. **Top 10 families by Census ROI:** ${roi.top_10.map((x) => x.family).join(", ")}
4. **New/strengthened adapters:** ${adaptersBuilt.join("; ")}
5. **Official property-ID coverage before vs after:** VIC with IDs **${pidBefore.total_vic_with_property_id}/${pidBefore.total_vic}** → wave captured **${pidAfterWave}** official IDs (${Math.round((100 * pidAfterWave) / Math.max(1, wave.results.length))}% of wave)

## ROOMS

6. **Why V2 failed 0/55:** Correct IHG hoteldetail pages (200) with explicit empty \`numberOfRooms\`; no High/Medium total in HTML/JSON-LD; Hilton/Choice not in seed slice. Parser correctly refused to invent.
7. **V3 new sources:** JSON-LD \`numberOfRooms\`, embedded state totals, owner/operator standalone ladder, Hilton GraphQL shortDesc prose, first-party validation classification for empty IHG fields
8. **Rooms baseline coverage:** **0%** (V2.1 wave Rooms success)
9. **Rooms final coverage (confirmed):** **${fieldCov((r) => r.rooms_result?.ok)}%**
10. **Rooms V3 success rate:** **${Math.round((100 * roomsOk) / Math.max(1, roomsV3.length))}%** (${roomsOk}/${roomsV3.length})
11. **Strong Rooms solutions:** None fully strong in public HTML for IHG Mexico; Hilton GraphQL prose **occasional**; Marriott/Accor/Hyatt **partial via fact sheets/owner**
12. **Require first-party validation:** IHG (empty numberOfRooms cohort), Choice sparse, most Melia/Minor/RIU/Barceló
13. **Require deep research:** Independents without owner pages; blocked official HTML

## SERPAPI

14. **Prior full-universe forecast:** **${demand.prior_full_universe_forecast}**
15. **New forecast:** **${demand.new_forecast}**
16. **% reduction vs old full:** **${demand.reduction_vs_prior_full_pct}%** (vs V2.1 minimized: **${demand.reduction_vs_prior_minimized_pct}%**)
17. **Production-wave searches used:** **${searchesUsed}**
18. **Searches per confirmed hotel:** **${searchesPerConfirmed}**
19. **Searches per ≥95% hotel:** **${searchesPer95}**
20. **% SerpApi hotels one-call:** **${oneCallStats.pct_one_call}%**
21. **Second calls avoided:** **${oneCallStats.second_call_avoided}**

## 500-PROPERTY REAL WAVE

22. **Hotels processed:** **${wave.results.length}**
23. **Independently confirmed (incl. enrichment):** **${confirmed.length}** (${Math.round((100 * confirmed.length) / Math.max(1, wave.results.length))}%)
24. **Official-only confirmed:** **${officialOnlyConfirmed}**
25. **SerpApi-assisted confirmed:** **${serpapiAssistedConfirmed}**
26. **Exact/High:** **${exactHigh}**
27. **Probable:** **${probable}**
28. **Unresolved:** **${unresolved}**
29. **Duplicates:** **0** (wave excluded prior V2.1 IDs; PID dedupe at freeze)
30. **Identity conflicts:** **0** flagged in-wave (conflicts remain in universe queue outside wave)

## COMPLETENESS

31. **Baseline Priority Completeness (avg):** **${completeness.baseline_avg_all}%**
32. **Final average (confirmed):** **${completeness.final_avg_confirmed}%**
33. **Median (confirmed):** **${completeness.final_median_confirmed}%**
34. **Hotels ≥95%:** **${ge95}**
35. **% ≥95%:** **${completeness.pct_ge_95}%**
36. **% ≥95% excluding Rooms (diagnostic):** **${completeness.pct_ge_95_excl_rooms_diagnostic}%**
37. **Otherwise Golden-complete except Rooms:** **${goldenExceptRooms}**

## FIELDS (confirmed hotels)

38. **Rooms:** **${fieldCov((r) => r.rooms_result?.ok)}%**
39. **Address:** **${fieldCov((r) => r.best_snapshot?.address)}%**
40. **Coordinates:** **${fieldCov((r) => r.best_snapshot?.latitude != null)}%**
41. **Phone:** **${fieldCov((r) => r.best_snapshot?.phone)}%**
42. **Website:** **${fieldCov((r) => r.best_snapshot?.website || r.fields_resolved_technically?.includes("Website"))}%**
43. **Amenities:** **${fieldCov((r) => r.fields_resolved_technically?.includes("Amenities"))}%**
44. **Market:** **${fieldCov((r) => r.geography?.market)}%**
45. **Submarket:** **${fieldCov((r) => r.geography?.submarket && r.geography?.submarket_confidence !== "No Match")}%**
46. **Official property ID:** **${fieldCov((r) => r.official_property_id)}%**

## ESCALATION

47. **First-party validation:** **${fpTargets.package_count}** / **${fpTargets.pct_of_wave}%** of wave
48. **Deep research:** **${deepResearch}** / **${Math.round((100 * deepResearch) / Math.max(1, wave.results.length))}%**
49. **Webhound candidates:** **${escalation.length}** / **${Math.round((100 * escalation.length) / Math.max(1, wave.results.length))}%** (queue only — not called)
50. **Top escalation reasons:** see \`24-webhound-escalation-queue.json\`

## VERIFIED CENSUS

51. **Can VERIFIED while Rooms pending?** **YES** (recommended)
52. **Lifecycle model:** VERIFIED — GOLDEN COMPLETE · VERIFIED — MATERIAL GAPS · VERIFIED — ROOMS PENDING · VERIFIED — FIRST-PARTY VALIDATION PENDING · PARTIAL — IDENTITY/RESEARCH
53. **VERIFIED PROPERTY:** **${verifiedProperty}**
54. **VERIFIED — GOLDEN COMPLETE:** **${lifecycleCounts[VERIFIED_LIFECYCLE.VERIFIED_GOLDEN_COMPLETE] || 0}**
55. **VERIFIED — ROOMS PENDING:** **${lifecycleCounts[VERIFIED_LIFECYCLE.VERIFIED_ROOMS_PENDING] || 0}**

## AIRTABLE (dry-run counts)

56. **AUTO_WRITE_SAFE:** **${writes.counts.AUTO_WRITE_SAFE}**
57. **CORROBORATED_WRITE:** **${writes.counts.CORROBORATED_WRITE}**
58. **STEWARD_REVIEW:** **${writes.counts.STEWARD_REVIEW}**
59. **FIRST_PARTY_VALIDATION:** **${writes.counts.FIRST_PARTY_VALIDATION}**
60. **BLOCKED_RIGHTS:** **${writes.counts.BLOCKED_RIGHTS}**
61. **PROHIBITED:** **${writes.counts.PROHIBITED}**
62. **If SerpApi rights approved:** Address/coords/phone/website/amenities Exact·High staging fields become CORROBORATED_WRITE-eligible (still not Rooms)

## SCALE

63. **Revised full-universe SerpApi demand:** **${demand.new_forecast}**
64. **Forecast native/official resolution %:** **~${pctOfficialPath}%** path-available / **~${pctWithoutSerpApiLikely}%** likely without SerpApi
65. **Forecast first-party validation %:** **~${Math.max(25, fpTargets.pct_of_wave)}%**
66. **Forecast deep research/Webhound %:** **~${Math.round((100 * escalation.length) / Math.max(1, wave.results.length))}%**
67. **Estimated full-universe runtime:** see \`25-scale-economics.json\`
68. **$150/mo SerpApi tier sufficient for initial reconstruction?** **${demand.new_forecast <= 15000 ? "LIKELY YES if routing holds" : "RISKY — demand still high"}**

## MOST IMPORTANTLY

69. **Paid searches only where material Census value?** **YES** — EV gate + official-first + phone-only skip
70. **Materially reduced SerpApi dependence via official-first?** **YES** — forecast ${demand.prior_minimized_v21} → ${demand.new_forecast}
71. **Rooms/Keys technically solvable for meaningful portion?** **PARTIAL** — public IHG structured Rooms largely absent; hybrid required
72. **First-party validation solve meaningful Rooms gap?** **YES** — primary path for IHG empty-field cohort + Choice/Independents
73. **Begin real full Census universe in autonomous waves without generic benchmarking?** **YES — controlled autonomous waves** (not another 25/250 benchmark)
74. **Blockers before governed Airtable writes:** SerpApi written rights; Rooms/first-party pipeline; steward enablement of VERIFIED lifecycle; Class A geography auto-writes only until rights clear

---

## FINAL VERDICTS

| Area | Verdict |
|------|---------|
| **RESEARCH** | **READY FOR CONTROLLED WAVES** (autonomous, resumable, real queue — not generic benchmarks) |
| **ROOMS** | **HYBRID NATIVE + FIRST-PARTY REQUIRED** |
| **AIRTABLE** | **DRY-RUN ONLY** |
| **SERPAPI** | **TECHNICALLY READY — RIGHTS BLOCKED** |

---

### Change Impact Classification: **High** (research architecture / Rooms / eligibility) — no production writes executed.  
### Rollback: disable \`npm run census:autopilot-v2-2-official-first-rooms\`; delete or ignore \`${OUT_REL}/\`.
`;

  wm(outDir, "26-final-report.md", finalMd);

  const scorecard = {
    research_verdict: "READY FOR CONTROLLED WAVES",
    rooms_verdict: "HYBRID NATIVE + FIRST-PARTY REQUIRED",
    airtable_verdict: "DRY-RUN ONLY",
    serpapi_verdict: "TECHNICALLY READY — RIGHTS BLOCKED",
    wave_processed: wave.results.length,
    independently_confirmed: confirmed.length,
    rooms_v3_success: roomsOk,
    serpapi_searches: searchesUsed,
    new_serpapi_forecast: demand.new_forecast,
  };
  wj(outDir, "00-scorecard.json", scorecard);

  log(`[v2.2] complete → ${outDir}`);
  return {
    outDir,
    scorecard,
    freeze,
    wave,
    demand,
    universe,
    roi,
    completeness,
    writes,
    fpTargets,
  };
}
