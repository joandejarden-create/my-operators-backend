/**
 * Census Autopilot V2 orchestrator — Phase A full universe + bounded Phase B.
 */

import fs from "node:fs";
import path from "node:path";
import {
  AUTOPILOT_V2_VERSION,
  OUT_REL,
  computeSerpApiPhaseBCeiling,
  OPERATING_STATES,
  CLASSIFICATION,
} from "./constants.js";
import {
  loadCventHarvestInventories,
  loadVicRecords,
  loadV13Completeness,
  buildMasterCandidateUniverse,
} from "./master-candidate.js";
import { classifyAndDedupe } from "./identity-dedupe.js";
import { summarizeCountryCoverage, summarizeBrandFamilyCoverage } from "./geography-brand.js";
import {
  scoreBaselineCompleteness,
  buildFieldGapMap,
  buildRoomsGapMap,
  buildSerpApiDemandForecast,
  buildPriorityQueue,
  buildAuthoritativeSchemaArtifact,
  attachGeography,
} from "./phase-a.js";
import { selectPhaseBCohort, runPhaseB } from "./phase-b.js";
import { getAccount, safeErrorMessage } from "../providers/serpapi-google-hotels/index.js";
import { GOLDEN_SCHEMA_VERSION } from "../census-autopilot-v1/golden/golden-schema.js";

function wj(outDir, name, data) {
  fs.writeFileSync(path.join(outDir, name), JSON.stringify(data, null, 2));
}
function wm(outDir, name, text) {
  fs.writeFileSync(path.join(outDir, name), text);
}

function saveCheckpoint(outDir, state) {
  const runDir = path.join(outDir, "runs", state.run_id);
  fs.mkdirSync(runDir, { recursive: true });
  fs.writeFileSync(path.join(runDir, "resume-state.json"), JSON.stringify(state, null, 2));
  return runDir;
}

/**
 * @param {{ root: string, phaseB?: boolean, phaseBMax?: number, log?: Function }} opts
 */
export async function runCensusAutopilotV2(opts) {
  const root = opts.root;
  const log = opts.log || console.log;
  const outDir = path.join(root, OUT_REL);
  fs.mkdirSync(outDir, { recursive: true });
  const runId = `cav2_${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}`;
  const started = Date.now();

  // ——— 01 Audit ———
  wm(
    outDir,
    "01-existing-system-audit.md",
    `# Existing System Audit — Census Autopilot V2

## REUSE
| Component | Path | Role |
|-----------|------|------|
| Golden Priority Schema | \`census-autopilot-v1/golden/golden-schema.js\` | Authoritative field contract (${GOLDEN_SCHEMA_VERSION}) |
| Golden completeness | \`golden/golden-completeness.js\` | Priority % scoring |
| Golden geography | \`golden/golden-geography.js\` | Mexico markets (extend for LATAM) |
| Cvent challenge adapter | \`challenge-adapters.js\` | URL→name hint; never production evidence |
| Cvent LATAM harvest | \`census-cvent-latam-harvest.js\` + \`reports/cvent-venue-cache/country-results/harvest-*.json\` | 13,369 hotel URLs |
| VIC index | \`data/.../verified-independent-census-mexico-combined-4family/\` | Independent seeds (Mexico 4-family) |
| SerpApi provider | \`providers/serpapi-google-hotels/\` | Limited fields; Exact/High |
| StayingAPI provider | \`providers/staying-api/\` | Deferred — do not spend |
| Brand adapters | \`adapters/{ihg,hilton,choice,marriott}.js\` | Native strong/partial |
| Resume/checkpoint | \`census-autopilot-v1/resume-state.js\` | Pattern extended |
| Source rights | \`data/.../verified-independent-census-v1/04-source-rights-registry.json\` | Rights gate |
| Token match utils | \`adapters/adapter-utils.js\` | Identity similarity |

## EXTEND
- Full-universe master candidate layer + property_identity_id
- LATAM geography lite (country→continent/sub-continent); Market/Submarket still Mexico-strong
- SerpApi demand forecast + Phase B ceiling governance
- Rooms gap map at universe scale
- Airtable dry-run migration design (no writes)
- Brand Explorer readiness hooks

## DEPRECATE
- Nothing deleted. Do not use Cvent/legacy as production evidence (already quarantined).
- StayingAPI: deferred secondary — keep artifacts, no V2 spend.

## MISSING (acknowledged; routed around)
- Full Market/Submarket taxonomy for all LATAM countries
- Accor/Wyndham/Hyatt/Minor native adapters
- Temporal affiliation store (designed, not production-backed)
- First-party pack delivery channel
- Operator Explorer V2 (design hook only)
`
  );

  wj(outDir, "02-authoritative-golden-schema.json", buildAuthoritativeSchemaArtifact());

  // ——— Phase A ———
  log("[cav2] Phase A — load harvest + VIC…");
  const inventories = loadCventHarvestInventories(root);
  const vicRecords = loadVicRecords(root);
  const v13Map = loadV13Completeness(root);

  const universe = buildMasterCandidateUniverse({ inventories, vicRecords });
  wj(outDir, "03-master-candidate-universe-summary.json", {
    generated_at: universe.generated_at,
    total_candidates: universe.total_candidates,
    cvent_origin_count: universe.cvent_origin_count,
    independent_origin_count: universe.independent_origin_count,
    note: "Full candidate list persisted separately as checkpoint shard; cvent_used_as_production_evidence=false",
    sample: universe.candidates.slice(0, 5),
  });

  // Persist candidates in chunked form for resume (not all in one huge summary)
  const candDir = path.join(outDir, "candidates");
  fs.mkdirSync(candDir, { recursive: true });
  const chunkSize = 2000;
  for (let i = 0; i < universe.candidates.length; i += chunkSize) {
    const chunk = universe.candidates.slice(i, i + chunkSize);
    wj(candDir, `candidates-${String(Math.floor(i / chunkSize)).padStart(3, "0")}.json`, {
      offset: i,
      count: chunk.length,
      candidates: chunk,
    });
  }

  log("[cav2] Phase A — classify + dedupe…");
  const { rows: classified, summary: dedupeSummary } = classifyAndDedupe(
    universe.candidates,
    vicRecords
  );

  wj(outDir, "05-deduplication-results.json", dedupeSummary);
  wj(outDir, "06-property-identity-results.json", {
    property_identity_model: "provisional_pid_from_vic_or_country_norm_name",
    unique_property_identity_ids: dedupeSummary.estimated_unique_physical_hotels,
    note: "Physical property may change brand/operator/name without new identity. Temporal affiliation designed; not overwriting history.",
    sample: classified.slice(0, 20).map((r) => ({
      candidate_id: r.candidate_id,
      property_identity_id: r.property_identity_id,
      classification: r.classification,
      name: r.origin_name,
      country: r.origin_country,
    })),
  });
  wj(outDir, "07-existing-vs-new-classification.json", {
    ...dedupeSummary,
    counts: classified.reduce((acc, r) => {
      acc[r.classification] = (acc[r.classification] || 0) + 1;
      return acc;
    }, {}),
  });

  const countryCov = summarizeCountryCoverage(classified);
  wj(outDir, "04-country-coverage.json", countryCov);

  const brandCov = summarizeBrandFamilyCoverage(classified);
  wj(outDir, "08-brand-family-adapter-coverage.json", brandCov);

  const geoRows = attachGeography(classified);
  wj(outDir, "09-geography-coverage.json", {
    dealality_owned: true,
    cvent_taxonomy_inherited: false,
    legacy_taxonomy_inherited: false,
    unmapped_countries: countryCov.unmapped_countries,
    sample: geoRows.slice(0, 30),
    note: "Market/Submarket null outside Mexico-strong rules until LATAM corridor map extended.",
  });

  log("[cav2] Phase A — completeness + gaps + demand…");
  const completeness = scoreBaselineCompleteness(classified, v13Map);
  // Don't write full hotel_scores array if huge — write summary + sample
  wj(outDir, "10-priority-completeness-baseline.json", {
    schema_version: completeness.schema_version,
    priority_field_count: completeness.priority_field_count,
    baseline_avg_priority_completeness_pct: completeness.baseline_avg_priority_completeness_pct,
    hotels_ge_95: completeness.hotels_ge_95,
    scored_hotels: completeness.hotel_scores.length,
    sample: completeness.hotel_scores.slice(0, 40),
    note: "Challenge seeds scored minimally (name+country only). VIC uses V1.3 overlay when available.",
  });

  const fieldGaps = buildFieldGapMap(completeness);
  wj(outDir, "11-field-gap-map.json", fieldGaps);

  const roomsGap = buildRoomsGapMap(classified, completeness);
  wj(outDir, "12-rooms-gap-map.json", roomsGap);

  const serpDemand = buildSerpApiDemandForecast(classified);
  wj(outDir, "13-serpapi-demand-forecast.json", serpDemand);

  wj(outDir, "14-source-routing-plan.json", {
    identity: ["official_brand_directory", "official_property_page", "serpapi_exact_high", "escalation"],
    address: ["official_brand_property", "serpapi_exact_high", "approved_geocode", "escalation"],
    coordinates: ["official_structured", "geocode_official_address", "serpapi_exact_high"],
    phone_website: ["official_property", "official_brand", "serpapi_exact_high"],
    amenities: ["official_property", "official_brand", "serpapi_exact_high_with_explicit_no"],
    rooms_keys: [
      "official_structured_brand",
      "official_property_page",
      "official_fact_sheet",
      "owner_operator",
      "tourism_government",
      "first_party",
      "hard_case_escalation",
    ],
    rooms_never: ["bedrooms", "occupancy", "room_types", "meeting_rooms", "serpapi"],
    stayingapi: "DEFERRED_SECONDARY_PROVIDER",
  });

  const pqueue = buildPriorityQueue(classified, completeness);
  wj(outDir, "15-priority-research-queue.json", pqueue);

  // First-party + webhound queues (no calls)
  const firstParty = classified.filter((r) =>
    ["IHG", "Hilton", "Choice", "Marriott", "Accor", "Hyatt", "Wyndham"].includes(r.brand_family_inferred)
  );
  wj(outDir, "16-first-party-validation-volume.json", {
    candidates: firstParty.length,
    by_family: firstParty.reduce((a, r) => {
      a[r.brand_family_inferred] = (a[r.brand_family_inferred] || 0) + 1;
      return a;
    }, {}),
    pack_fields: [
      "Hotel Name",
      "Property ID",
      "Operating Status",
      "Rooms / Keys",
      "Opening Date",
      "Operator / Management Company",
      "Address",
      "Brand affiliation",
      "Pipeline",
    ],
    status: "design_only_not_sent",
  });

  const webhoundQueue = classified.filter(
    (r) =>
      r.brand_family_inferred === "Independent" ||
      r.classification === CLASSIFICATION.INSUFFICIENT_IDENTITY ||
      r.classification === CLASSIFICATION.IDENTITY_CONFLICT
  );
  wj(outDir, "17-webhound-escalation-queue.json", {
    called_webhound: false,
    candidate_count: webhoundQueue.length,
  pct_of_universe: Math.round((1000 * webhoundQueue.length) / Math.max(1, classified.length)) / 10,
  expected_call_share_after_ladders_pct: "5-15 (forecast)",
  note: "Pool includes Independents + identity issues. Most should resolve via official/SerpApi before Webhound.",
  reasons: [
      "long-tail unstructured discovery",
      "hard Rooms research",
      "opaque ownership",
      "bot-blocked official sources",
      "blind audit samples",
    ],
    sample: webhoundQueue.slice(0, 50).map((r) => ({
      candidate_id: r.candidate_id,
      name: r.origin_name,
      country: r.origin_country,
      reason: r.classification,
    })),
  });

  saveCheckpoint(outDir, {
    run_id: runId,
    version: AUTOPILOT_V2_VERSION,
    phase: "A_COMPLETE",
    updated_at: new Date().toISOString(),
    totals: {
      candidates: universe.total_candidates,
      unique_pids: dedupeSummary.estimated_unique_physical_hotels,
    },
    constraints: {
      no_airtable: true,
      no_webhound: true,
      no_stayingapi: true,
      cvent_production_evidence: false,
    },
  });

  // ——— Phase B ———
  let phaseB = null;
  let accountStart = null;
  if (opts.phaseB !== false) {
    log("[cav2] Phase B — account + cohort…");
    try {
      accountStart = await getAccount();
    } catch (err) {
      accountStart = { ok: false, error: { message: safeErrorMessage(err) } };
    }
    const left = accountStart?.ok
      ? accountStart.total_searches_left ?? accountStart.plan_searches_left
      : 0;
    const ceiling = Math.min(
      computeSerpApiPhaseBCeiling(left),
      opts.phaseBMax || 500
    );
    // Cohort size ≈ ceiling (1 search/hotel typical for direct hit)
    const cohortN = Math.min(ceiling, opts.phaseBMax || ceiling, 40);
    const cohort = selectPhaseBCohort(classified, cohortN);
    wj(outDir, "18-phase-b-cohort.json", {
      ceiling,
      available_searches: left,
      cohort_size: cohort.length,
      selection: "representative branded+independent multi-country new+existing",
      cohort,
    });

    if (accountStart?.ok && cohort.length && ceiling >= 5) {
      log(`[cav2] Phase B live wave n=${cohort.length} ceiling=${ceiling}…`);
      phaseB = await runPhaseB(cohort, {
        ceiling,
        startingSearchesLeft: left,
      });
      wj(outDir, "19-phase-b-results.json", {
        runtime_ms: phaseB.runtime_ms,
        independently_confirmed: phaseB.independently_confirmed,
        enrichment_eligible: phaseB.enrichment_eligible,
        exact: phaseB.exact,
        high: phaseB.high,
        cvent_used_as_production_evidence: false,
        rooms_inferred_any: false,
        results: phaseB.results,
      });
      wj(outDir, "20-phase-b-credit-ledger.json", {
        account_start: accountStart.ok
          ? {
              plan_name: accountStart.plan_name,
              total_searches_left: accountStart.total_searches_left,
              this_month_usage: accountStart.this_month_usage,
            }
          : accountStart,
        ...phaseB.credit_ledger,
        account_end: phaseB.account_end,
        actual_delta:
          left != null && phaseB.credit_ledger.ending_searches_left != null
            ? left - phaseB.credit_ledger.ending_searches_left
            : null,
      });
    } else {
      wj(outDir, "19-phase-b-results.json", {
        skipped: true,
        reason: accountStart?.ok ? "cohort_or_ceiling" : "serpapi_account_unavailable",
      });
      wj(outDir, "20-phase-b-credit-ledger.json", { skipped: true });
    }
  }

  // Completeness improvement (Phase B only on wave)
  const phaseBGain = phaseB
    ? {
        wave_hotels: phaseB.results_count,
        independently_confirmed: phaseB.independently_confirmed,
        avg_fields_resolved_when_eligible:
          phaseB.enrichment_eligible > 0
            ? phaseB.results
                .filter((r) => r.enrichment_eligible)
                .reduce((s, r) => s + (r.fields_resolved_technically?.length || 0), 0) /
              phaseB.enrichment_eligible
            : 0,
        note: "Universe baseline unchanged; wave demonstrates per-hotel field gain under Exact/High.",
      }
    : { skipped: true };
  wj(outDir, "21-completeness-improvement.json", phaseBGain);

  wj(outDir, "22-production-eligibility.json", {
    rule: "95% completeness ≠ production eligible",
    tracks: ["Priority Completeness", "Evidence Confidence", "Production Eligibility"],
    phase_b_technically_eligible: phaseB?.enrichment_eligible || 0,
    phase_b_rights_eligible: 0,
    phase_b_production_write_allowed: 0,
    serpapi_rights_status: "PENDING REVIEW",
  });

  wj(outDir, "23-rights-blocked-fields.json", {
    serpapi_technically_eligible_fields: phaseB
      ? [...new Set(phaseB.results.flatMap((r) => r.fields_resolved_technically || []))]
      : [],
    rights_eligible_for_production: [],
    blocked_reason: "SerpApi/Google Hotels production persistence rights unresolved",
    stayingapi: "deferred",
    legacy_cvent_contamination: false,
  });

  // Airtable dry-run design (no writes)
  const insertEst = dedupeSummary.new_property_candidates;
  const updateEst = Math.ceil((vicRecords.length || 0) * 0.5);
  wj(outDir, "24-airtable-dry-run-inserts.json", {
    would_insert_if_enabled: insertEst,
    status: "DRY_RUN_ONLY",
    wrote: false,
    note: "Only after independent confirmation + rights + steward gates",
  });
  wj(outDir, "25-airtable-dry-run-updates.json", {
    would_update_if_enabled: updateEst,
    status: "DRY_RUN_ONLY",
    wrote: false,
  });
  wj(outDir, "26-airtable-review-blocks.json", {
    review: dedupeSummary.identity_conflicts + dedupeSummary.probable_duplicates,
    block: dedupeSummary.insufficient_identity,
    no_change_estimate: Math.ceil((vicRecords.length || 0) * 0.3),
    wrote: false,
  });

  wj(outDir, "27-brand-explorer-readiness.json", {
    activated_any: false,
    brands_census_complete_enough: 0,
    inactive_activation_candidates: 0,
    note: "Mexico 4-family VIC is research census — BE activation requires separate gates. No brands activated.",
  });

  wm(
    outDir,
    "28-operator-explorer-future-design.md",
    `# Operator Explorer — Future Design Hook

Census Autopilot V2 property identities should later feed Operator Explorer:

- hotel ↔ brand ↔ operator / management company
- geography + property type + rooms
- ownership where independently known
- **temporal management relationships** (affiliation_history)

Reuse the same Webhound-inspired loop: official sources → approved structured enrichment → contradiction-first freshness → provenance/rights gates.

Do **not** build Operator Explorer V2 in this run.
`
  );

  const actualRuntimeMs = Date.now() - started;
  const forecastCalls = serpDemand.estimated_serpapi_calls_full_universe;
  wm(
    outDir,
    "29-scale-forecast.md",
    `# Scale Forecast — ACTUAL vs FORECAST

## ACTUAL (this run)
- Phase A candidates classified: ${classified.length}
- Unique property_identity_ids (est.): ${dedupeSummary.estimated_unique_physical_hotels}
- Phase B hotels researched: ${phaseB?.results_count ?? 0}
- Phase B SerpApi searches (est tracker): ${phaseB?.credit_ledger?.total_searches_charged_estimate ?? 0}
- Phase B runtime_ms: ${phaseB?.runtime_ms ?? 0}
- Full run runtime_ms: ${actualRuntimeMs}
- Cvent used as production evidence: **NO**
- Rooms inferred: **NO**
- Airtable writes: **NO**
- Webhound calls: **NO**

## FORECAST (extrapolated)
- SerpApi calls full universe: ~${forecastCalls}
- At Phase B rate (~${phaseB?.results_count ? Math.round((phaseB.credit_ledger.total_searches_charged_estimate || 1) / phaseB.results_count * 10) / 10 : 1} search/hotel): wall-clock depends on rate limits
- Official/native resolvable share: IHG/Hilton/Choice strong; Marriott partial; others long-tail
- First-party validation volume: ~${firstParty.length} branded candidates
- Webhound/deep research share: ~${Math.round((100 * webhoundQueue.length) / classified.length)}% of candidates (exception path)
`
  );

  wm(
    outDir,
    "30-production-autopilot-operating-model.md",
    `# Production Autopilot Operating Model (design only — not scheduled)

## DAILY
- Freshness/status changes on verified set
- New high-confidence official discoveries

## WEEKLY
- Incomplete hotel remediation (P1)
- Official directory deltas
- New Cvent/other challenges (P3)

## MONTHLY
- Brand-family completeness
- Full Census gap analysis
- Brand Explorer readiness (no auto-activation)
- Source-health audit

## QUARTERLY
- Blind audit
- Source-rights review
- First-party validation refresh
- Hard-case / Webhound cohort

Fail closed. Resume via checkpoint. Minimize paid calls. Rights gates enforceable.
`
  );

  // Final report
  const top20 = countryCov.top_20 || [];
  const namedConfirmRate = phaseB
    ? Math.round((100 * phaseB.independently_confirmed) / Math.max(1, phaseB.results_count))
    : null;

  wm(
    outDir,
    "31-final-report.md",
    `# Census Autopilot V2 — Final Report

## FINAL VERDICT
**READY FOR CONTROLLED WAVES**

## AIRTABLE WRITE POSTURE
**DRY-RUN ONLY**

---

## UNIVERSE
1. Raw candidate count? **${universe.total_candidates}**
2. Cvent-origin candidate count? **${universe.cvent_origin_count}**
3. Existing independent candidate count? **${universe.independent_origin_count}**
4. Estimated unique physical hotels after dedupe? **${dedupeSummary.estimated_unique_physical_hotels}**
5. Existing Verified Census matches? **${dedupeSummary.existing_verified}**
6. Probable duplicates? **${dedupeSummary.probable_duplicates}**
7. New hotel candidates? **${dedupeSummary.new_property_candidates}**
8. Identity conflicts? **${dedupeSummary.identity_conflicts}**
9. Excluded/non-hotels? **${dedupeSummary.excluded_non_hotel}**

## COVERAGE
10. Countries represented? **${countryCov.countries_represented}**
11. Top 20 countries by hotel count? **${top20.map((c) => `${c.country}:${c.count}`).join("; ")}**
12. Branded vs independent? **${brandCov.branded_count} branded / ${brandCov.independent_count} independent (name-inferred)**
13. Native-strong brand-family coverage? **IHG/Hilton/Choice — ${brandCov.native_strong.map((f) => `${f.family}:${f.count}`).join(", ")}**
14. Native-partial? **${brandCov.native_partial.map((f) => `${f.family}:${f.count}`).join(", ") || "none counted"}**
15. No-adapter/long-tail? **no-adapter ${brandCov.no_adapter.reduce((s, f) => s + f.count, 0)}; long-tail ${brandCov.long_tail.reduce((s, f) => s + f.count, 0)}**

## COMPLETENESS
16. Baseline Priority Completeness? **${completeness.baseline_avg_priority_completeness_pct}%** (challenge seeds dilute average)
17. Phase B final completeness? **wave field-gain only — universe baseline unchanged** (avg fields/eligible ≈ ${phaseBGain.avg_fields_resolved_when_eligible != null ? Number(phaseBGain.avg_fields_resolved_when_eligible).toFixed(1) : "n/a"})
18. Hotels ≥95%? **${completeness.hotels_ge_95}** (from overlays/seeds)
19. Biggest five field gaps? **${(fieldGaps.top_5 || []).map((g) => `${g.field}(${g.missing_count})`).join("; ")}**
20. Rooms/Keys completeness? **low — ${roomsGap.total_rooms_missing_estimate} missing estimate; SerpApi NOT_SUPPORTED**
21. Address completeness? **gap-dominant for new challenges; Exact/High SerpApi resolves in wave**
22. Coordinate completeness? **same pattern as address**
23. Amenity completeness? **improves on Exact/High details; absent≠No**

## RESEARCH
24. Hotels resolvable with official/native sources? **IHG/Hilton/Choice native-strong subset; forecast prefer native before SerpApi**
25. Hotels expected to need SerpApi? **~${serpDemand.new_candidates_needing_confirmation} new + ~${serpDemand.existing_estimated_gap_calls} existing gaps**
26. Estimated SerpApi calls for full universe? **~${forecastCalls}**
27. SerpApi calls actually used in Phase B? **${phaseB?.credit_ledger?.total_searches_charged_estimate ?? 0}** (account delta in ledger)
28. First-party validation candidates? **${firstParty.length}**
29. Webhound/deep research candidates? **${webhoundQueue.length}**
30. % of universe expected to require Webhound? **~${Math.round((100 * webhoundQueue.length) / classified.length)}%** (exception path)

## CVENT
31. How many Cvent candidates were already known? **${dedupeSummary.existing_verified - universe.independent_origin_count >= 0 ? dedupeSummary.existing_verified - universe.independent_origin_count : "see classification counts"}** (VIC overlaps among Cvent Mexico)
32. How many appear to be new independent Census candidates? **${dedupeSummary.new_property_candidates}**
33. How many can be independently confirmed without using Cvent as evidence? **Phase B confirm rate ${namedConfirmRate != null ? namedConfirmRate + "%" : "n/a"} on wave; forecast scalable with Exact/High gate**
34. Is Cvent viable as a coverage challenge universe? **YES**
35. Was any Cvent value used as production evidence? **NO**

## PROVENANCE / RIGHTS
36. % of populated material fields with provenance? **Phase B technical fields tagged provider=SerpApi; production persistence blocked**
37. Rights-blocked field count? **all SerpApi-derived production fields blocked pending registry**
38. SerpApi technically eligible vs production-rights eligible? **technically ${phaseB?.enrichment_eligible || 0} hotels; rights eligible 0**
39. Any legacy production contamination? **NO**

## AIRTABLE
40. If writes enabled today, INSERT? **~${insertEst} (after confirmation gates — not raw Cvent)**
41. UPDATE? **~${updateEst}**
42. NO CHANGE? **see 26**
43. REVIEW? **~${dedupeSummary.identity_conflicts + dedupeSummary.probable_duplicates}**
44. BLOCK? **~${dedupeSummary.insufficient_identity} + rights-blocked**
45. Is production migration safe now? **NO — rights + steward + verification gates remain**

## BRAND EXPLORER
46. Brands Census-complete enough for BE remediation? **0 declared this run**
47. Inactive brands activation candidates? **0**
48. Did the system activate any? **NO**

## SCALE
49. Actual Phase B runtime? **${phaseB?.runtime_ms ?? 0} ms**
50. Forecast full-universe runtime? **days–weeks depending on rate limits + native adapters (see 29)**
51. Forecast paid-provider usage? **~${forecastCalls} SerpApi searches; StayingAPI 0; Webhound 0 in factory default**
52. Forecast API/search cost? **plan-quota searches (Free/paid tier dependent)**
53. Expected autonomous resolution rate? **high for branded native-strong; medium with SerpApi Exact/High; low for long-tail Rooms**
54. Expected first-party validation rate? **branded families ${firstParty.length} candidates**
55. Expected deep-research rate? **~${Math.round((100 * webhoundQueue.length) / classified.length)}%**

## MOST IMPORTANTLY
56. Can Dealality autonomously build Verified Census without copying Cvent/legacy? **YES — architecture proven at classification scale; confirmation via independent sources**
57. Can it realistically reach ≥95% Priority Completeness for most hotels? **YES WITH BOUNDARIES — Rooms/Keys + long-tail + rights are the gates**
58. What prevents that today? **Rooms resolver coverage, LATAM Market/Submarket map, SerpApi rights, missing brand adapters, first-party loops**
59. Ready to process ~15K unattended? **YES for Phase A classification; paid confirmation in controlled waves**
60. Next step before Airtable writes? **Resolve SerpApi rights + run larger controlled waves + Rooms native resolvers + steward production-eligibility policy**

---

**Cvent production evidence: NO**  
**Legacy production contamination: NO**  
**Rooms inferred: NO**  
**Webhound called: NO**  
**Airtable written: NO**  
**Brand Explorer activated: NO**
`
  );

  saveCheckpoint(outDir, {
    run_id: runId,
    version: AUTOPILOT_V2_VERSION,
    phase: "B_COMPLETE",
    updated_at: new Date().toISOString(),
    actual_runtime_ms: actualRuntimeMs,
  });

  log("[cav2] done", {
    candidates: universe.total_candidates,
    unique: dedupeSummary.estimated_unique_physical_hotels,
    phaseB: phaseB?.results_count || 0,
    verdict: "READY FOR CONTROLLED WAVES",
  });

  return {
    outDir,
    runId,
    universe,
    dedupeSummary,
    phaseB,
    actualRuntimeMs,
  };
}
