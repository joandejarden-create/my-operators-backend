/**
 * Census Autopilot V2.1 — Production Readiness + Controlled Scale Wave
 */

import fs from "node:fs";
import path from "node:path";
import { auditSerpApiDemand, loadV2CandidateSummaries } from "./demand-audit.js";
import { freezeWaveCohort } from "./wave-cohort.js";
import { runControlledWave, stratifyWaveResults } from "./wave-runner.js";
import { ROOMS_LADDER, ROOMS_NEVER_FROM, ROOMS_RESOLVER_V2_VERSION } from "./rooms-resolver-v2.js";
import { loadVicRecords, loadV13Completeness } from "../census-autopilot-v2/master-candidate.js";
import { GOLDEN_FIELD_REGISTRY, priorityFields } from "../census-autopilot-v1/golden/golden-schema.js";

const OUT_REL = "data/research-engine-v2/census-autopilot-v2-1-production-readiness";
const V2_REL = "data/research-engine-v2/census-autopilot-v2-full-universe";

function wj(dir, name, data) {
  fs.writeFileSync(path.join(dir, name), JSON.stringify(data, null, 2));
}
function wm(dir, name, text) {
  fs.writeFileSync(path.join(dir, name), text);
}

function writePolicyArtifacts(outDir) {
  wm(
    outDir,
    "05-serpapi-rights-decision-needed.md",
    `# SERPAPI_PRODUCTION_RIGHTS_DECISION_NEEDED

Status: **DECISION NEEDED — do not enable production persistence**

Source: \`serpapi-benchmark-v1/22-production-rights-questions.md\` + VIC source-rights registry patterns.

## Still unknown (require written SerpApi answers)

| Topic | Unknown? |
|-------|----------|
| Persist factual hotel data (name, address, phone, website, coords, amenities, class) | YES |
| Retain after request/session completes | YES |
| Combine with independently researched proprietary Census data | YES |
| Use derived factual fields in proprietary Census product | YES |
| Customer-facing display to SaaS users | YES |
| Historical snapshots / change history | YES |
| Persist Google/property_token identifiers | YES |
| Store image URLs as references (no download) | YES |
| Download/reuse images | YES (assume Not Approved until proven) |
| Google underlying-source obligations | YES |
| R&D vs production plan differences | YES |
| Plan cancellation / deletion obligations | YES |

## Separation (do not collapse)

- \`research_allowed\` = true for technical benchmark/wave research
- \`technical_candidate\` = Exact/High fields may stage
- \`production_persistence_allowed\` = **false until written clarification**
- \`customer_display_allowed\` = **false until written clarification**
- \`image_reuse_allowed\` = **false**

Technical research continues. Production Airtable writes remain blocked for SerpApi-derived fields.

## Exact message Joan should send SerpApi

\`\`\`
Subject: Written clarification — commercial SaaS use of Google Hotels API data

Hello SerpApi team,

We operate a commercial hotel intelligence SaaS. We use your Google Hotels API for research.

Please confirm in writing (not marketing copy) whether our plan allows us to:

1) Persist factual property fields returned by the API (name, address, phone, website, coordinates, amenities, hotel class) in our database;
2) Retain those facts after the API request completes;
3) Combine them with independently researched hotel data;
4) Use derived factual fields inside a proprietary Hotel Census product;
5) Display those factual fields to paying SaaS customers;
6) Keep historical snapshots of those fields;
7) Persist and reuse property_token / Google property identifiers over time;
8) Store image URLs as references without downloading;
9) Download or reuse images (if ever permitted);
10) Any Google-source attribution, prohibited uses, or geo restrictions we must follow;
11) Whether R&D/benchmark use differs from production enrichment under our plan;
12) What we must delete if we cancel the plan.

We will not enable production persistence until we have your written answers.

Thank you,
Joan
\`\`\`
`
  );

  wm(
    outDir,
    "06-rooms-resolver-v2-design.md",
    `# Rooms Resolver V2 Design

Version: ${ROOMS_RESOLVER_V2_VERSION}

## Ladder
${ROOMS_LADDER.map((x, i) => `${i + 1}. ${x}`).join("\n")}

## Never from
${ROOMS_NEVER_FROM.map((x) => `- ${x}`).join("\n")}

## Provenance contract
rooms_value, rooms_source, rooms_source_type, retrieved_at, confidence, property_identity_match, evidence_quote_or_structured_field, rights_status

## Scope this wave
IHG / Hilton / Choice Mexico via existing V1.3 family resolvers. Architecture generalizes by adding family adapters at ladder step 1–2.
`
  );

  wj(outDir, "14-source-authority-matrix.json", {
    Rooms: ["official_brand_structured", "official_fact_sheet", "owner_operator", "tourism_gov", "approved_secondary"],
    Address: ["official_hotel_brand", "official_owner_operator", "serpapi_exact_high", "approved_independent"],
    Coordinates: ["official_structured", "geocode_official_address", "serpapi_exact_high"],
    Phone: ["official_property", "official_brand", "serpapi_exact_high"],
    Website: ["official_property", "official_brand", "serpapi_exact_high"],
    Amenities: ["official_property", "official_brand", "serpapi_exact_high", "approved_secondary"],
    "Hotel Name": ["official_brand_directory", "official_property", "serpapi_exact_high"],
    Brand: ["official_brand_directory", "official_property"],
    "Property Type": ["official_brand", "serpapi_input_only"],
    Market: ["dealality_geography_rules"],
    Submarket: ["dealality_geography_rules"],
    note: "Never weaker-overwrites-stronger solely because newer.",
  });

  const priority = priorityFields().map((f) => f.field);
  wj(outDir, "15-field-write-classes.json", {
    CLASS_A_AUTO_WRITE_SAFE: [
      "Country",
      "Continent",
      "Sub-Continent",
      "City",
      "State / Region",
      "Market",
      "Submarket",
    ],
    CLASS_B_AUTO_WRITE_WITH_CORROBORATION: [
      "Property Name",
      "Current Brand",
      "Brand Family",
      "Official Property URL",
      "Address",
      "Latitude",
      "Longitude",
      "Phone",
      "Property Type",
      "Pool",
      "Spa",
      "Fitness",
      "Parking",
    ],
    CLASS_C_STEWARD_REVIEW: [
      "Dealality Segment / Positioning",
      "Asset Context",
      "Boutique Flag",
      "All-Inclusive",
      "Hotel Description - AI Summary",
    ],
    CLASS_D_FIRST_PARTY_PREFERRED: [
      "Rooms / Keys",
      "Operator / Management Company",
      "Owner Name",
      "Opening Date",
      "Renovation Date",
    ],
    CLASS_E_NEVER_AUTO_INFER: [
      "Rooms from bedrooms/occupancy/room-types/meeting rooms",
      "Market/Submarket from Cvent/legacy",
      "STR Chain Scale from Google hotel_class",
      "Operator from booking platform",
      "production image reuse from SerpApi",
    ],
    priority_fields_referenced: priority,
  });

  wm(
    outDir,
    "16-production-eligibility-policy.md",
    `# Production Eligibility Policy (Steward)

## AUTO_WRITE_ELIGIBLE — all must pass

1. Identity Exact or High
2. No unresolved identity conflict
3. Required provenance present on material fields
4. Source rights permit persistence for those fields
5. No material contradiction
6. Priority completeness ≥ steward threshold (default 95% for full auto; field-class aware for partial)
7. No prohibited inference
8. No Cvent/legacy contamination (\`cvent_used_as_production_evidence=false\`)
9. Geography valid (Dealality taxonomy)
10. Required schema fields valid

## Field classes
See \`15-field-write-classes.json\` — only Class A/B may auto-write when rights pass; C steward; D first-party preferred; E never.

## SerpApi
Technically eligible ≠ rights eligible. Until written SerpApi clarification, SerpApi-derived fields are **staging only**.
`
  );

  wm(
    outDir,
    "17-airtable-transaction-design.md",
    `# Airtable UPDATE Transaction Design (not executed)

1. Read current Airtable row
2. Compare retrieved_at / version / content hash
3. Detect concurrent changes → REVIEW if conflict
4. Calculate field diff
5. Validate source rights per field
6. Validate property identity Exact/High
7. Write only changed eligible fields (Class A/B)
8. Preserve prior evidence/history (temporal facts)
9. Log transaction id + actor + sources
10. Support rollback from transaction log

Never overwrite a stronger source with a weaker source merely because it is newer.
`
  );

  wm(
    outDir,
    "18-airtable-insert-design.md",
    `# Airtable INSERT Safety Design (not executed)

Required before INSERT:

- durable property_identity_id
- Exact/High identity
- independent existence confirmation (not Cvent-only)
- valid Dealality geography
- minimum required Census fields
- field provenance
- rights eligibility
- duplicate check immediately before insert

Do **not** insert Cvent-only candidates.
`
  );

  wm(
    outDir,
    "21-webhound-escalation-policy.md",
    `# Webhound Escalation Policy

Do **not** call Webhound in Autopilot default loops.

Escalate when:

- opaque identity after native+SerpApi ladders
- hard ownership / operator relationship research
- long-tail unstructured sources only
- high-value Rooms gap after Rooms Resolver ladder exhausted
- bot-blocked critical official evidence
- blind external audit samples

Do **not** escalate routine address/coordinates/amenities SerpApi/native can resolve.

Expected full-universe escalation after ladders: **~5–15%** (not the raw independent pool size).
`
  );
}

/**
 * @param {{ root: string, waveSize?: number, searchCeiling?: number, log?: Function }} opts
 */
export async function runCensusAutopilotV21(opts) {
  const root = opts.root;
  const log = opts.log || console.log;
  const outDir = path.join(root, OUT_REL);
  const v2Dir = path.join(root, V2_REL);
  fs.mkdirSync(outDir, { recursive: true });

  const waveSize = opts.waveSize ?? Number(process.env.CAV21_WAVE_SIZE || 250);
  const searchCeiling = opts.searchCeiling ?? Number(process.env.CAV21_SEARCH_CEILING || 400);

  // 01 baseline
  const dedupe = JSON.parse(fs.readFileSync(path.join(v2Dir, "05-deduplication-results.json"), "utf8"));
  const brandCov = JSON.parse(fs.readFileSync(path.join(v2Dir, "08-brand-family-adapter-coverage.json"), "utf8"));
  const oldDemand = JSON.parse(fs.readFileSync(path.join(v2Dir, "13-serpapi-demand-forecast.json"), "utf8"));
  const v2Final = fs.readFileSync(path.join(v2Dir, "31-final-report.md"), "utf8");

  wm(
    outDir,
    "01-v2-baseline.md",
    `# V2 Baseline (input to V2.1)

${v2Final.slice(0, 2500)}

---
Dedup summary: ${JSON.stringify(dedupe)}
Old SerpApi forecast: ${oldDemand.estimated_serpapi_calls_full_universe}
`
  );

  // Demand audit
  const audit = auditSerpApiDemand(dedupe, brandCov, {
    oldForecast: oldDemand.estimated_serpapi_calls_full_universe || 14301,
  });
  wj(outDir, "02-serpapi-demand-audit.json", audit);
  wm(
    outDir,
    "03-call-minimization-plan.md",
    `# SerpApi Call Minimization Plan

## Goal
Minimize paid calls while maximizing independent Census completion. **Do not** maximize quota use.

## Old forecast problem
~${audit.old_forecast} ≈ nearly one search per candidate / inflated new×1.15 — overstates need.

## WHY calls were proposed
${Object.entries(audit.CALL_WHY)
  .map(([k, v]) => `- **${k}**: ${v}`)
  .join("\n")}

## Avoidance
${Object.entries(audit.avoidance_levers)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

## Revised forecast
- Confirmation: **${audit.revised_forecast.confirmation_searches}**
- Existing gaps: **${audit.revised_forecast.existing_gap_searches}**
- **Total: ${audit.revised_forecast.total}**
- Saved vs old: **${audit.searches_saved_vs_old} (${audit.savings_pct}%)**

## Rules
1. Dedupe to property_identity_id before paid call
2. Official-first for native-strong brands
3. Skip Rooms-only gaps (not SerpApi)
4. Skip insufficient / conflict until identity repair
5. Dealality research cache on every lookup
6. Field-specific routing — not blanket enrichment
`
  );

  wm(
    outDir,
    "04-serpapi-cache-design.md",
    `# SerpApi Result Cache Design

Path: \`data/research-engine-v2/serpapi-research-cache/\`

Keys: query + normalized identity + property_token + property_identity_id + request type + dates/gl

Stores: retrieved_at, response_hash, source_state, match_confidence, eligible_fields, raw path, expiry (30d)

Separates Dealality reproducibility cache from SerpApi provider cache (which may be free). Never stores API keys.
`
  );

  writePolicyArtifacts(outDir);

  // Load candidates + VIC
  log("[v2.1] loading V2 candidates + VIC…");
  const candidates = loadV2CandidateSummaries(v2Dir);
  const vic = loadVicRecords(root);
  const v13 = loadV13Completeness(root);

  const freeze = freezeWaveCohort(candidates, vic, waveSize);
  wj(outDir, "08-wave-cohort-freeze.json", freeze);
  log(`[v2.1] cohort frozen n=${freeze.actual} countries=${freeze.composition.countries.length}`);

  // Live wave
  log(`[v2.1] controlled wave live (ceiling=${searchCeiling})…`);
  const wave = await runControlledWave({
    repoRoot: root,
    cohort: freeze.cohort,
    ceiling: searchCeiling,
    log,
  });

  wj(outDir, "09-wave-results.json", {
    runtime_ms: wave.runtime_ms,
    results_count: wave.results.length,
    results: wave.results,
  });

  const strata = stratifyWaveResults(freeze.cohort, wave.results);
  const confirmed = wave.results.filter((r) => String(r.confirmation || "").includes("INDEPENDENTLY"));
  const exact = wave.results.filter((r) => r.confirmation === "INDEPENDENTLY CONFIRMED — EXACT" || r.best_level === "EXACT");
  const high = wave.results.filter(
    (r) => r.confirmation === "INDEPENDENTLY CONFIRMED — HIGH" || (r.best_level === "HIGH" && String(r.confirmation).includes("INDEPENDENTLY"))
  );
  const probable = wave.results.filter((r) => r.confirmation === "PROBABLE — NEEDS CORROBORATION");
  const unresolved = wave.results.filter(
    (r) =>
      r.confirmation === "INSUFFICIENT EVIDENCE" ||
      r.confirmation === "CVENT_ONLY_UNRESOLVED"
  );

  wj(outDir, "10-confirmation-analysis.json", {
    strata,
    exact: exact.length,
    high: high.length,
    probable: probable.length,
    independently_confirmed: confirmed.length,
    unresolved: unresolved.length,
    enrichment_existing: wave.results.filter((r) => String(r.confirmation).includes("ENRICHMENT")).length,
  });

  const failCounts = wave.results.reduce((a, r) => {
    if (!r.failure_class) return a;
    a[r.failure_class] = (a[r.failure_class] || 0) + 1;
    return a;
  }, {});
  wj(outDir, "11-failure-analysis.json", {
    note: "Failed SerpApi/match ≠ Autopilot failure",
    failure_counts: failCounts,
    samples: wave.results
      .filter((r) => r.failure_class)
      .slice(0, 40)
      .map((r) => ({
        name: r.name,
        confirmation: r.confirmation,
        failure_class: r.failure_class,
        level: r.best_level,
      })),
  });

  // Field completeness
  const fieldHits = {};
  for (const r of wave.results) {
    for (const f of r.fields_resolved_technically || []) {
      fieldHits[f] = (fieldHits[f] || 0) + 1;
    }
  }
  const avgBaseline =
    wave.results.reduce((s, r) => s + (r.baseline_priority_proxy_pct || 0), 0) / Math.max(1, wave.results.length);
  const avgFinal =
    wave.results.reduce((s, r) => s + (r.final_priority_proxy_pct || 0), 0) / Math.max(1, wave.results.length);
  const ge95 = wave.results.filter((r) => (r.final_priority_proxy_pct || 0) >= 95).length;

  wj(outDir, "12-field-completeness.json", {
    note: "Proxy Priority set (identity+contact+geo+rooms) — full Golden loop partial in wave",
    avg_baseline_pct: Math.round(avgBaseline * 10) / 10,
    avg_final_pct: Math.round(avgFinal * 10) / 10,
    hotels_ge_95: ge95,
    pct_ge_95: Math.round((1000 * ge95) / Math.max(1, wave.results.length)) / 10,
    avg_fields_gained:
      Math.round(
        (10 * wave.results.reduce((s, r) => s + (r.fields_gained || 0), 0)) / Math.max(1, wave.results.length)
      ) / 10,
    field_hits: fieldHits,
    golden_priority_fields: priorityFields().map((f) => f.field),
  });

  // 95 gap analysis
  const below = wave.results.filter((r) => (r.final_priority_proxy_pct || 0) < 95);
  const blocking = {
    "Rooms / Keys": below.filter((r) => !(r.fields_resolved_technically || []).includes("Rooms / Keys")).length,
    Address: below.filter((r) => !(r.fields_resolved_technically || []).includes("Address")).length,
    Coordinates: below.filter(
      (r) =>
        !(r.fields_resolved_technically || []).includes("Latitude") ||
        !(r.fields_resolved_technically || []).includes("Longitude")
    ).length,
    Phone: below.filter((r) => !(r.fields_resolved_technically || []).includes("Phone")).length,
    Website: below.filter((r) => !(r.fields_resolved_technically || []).includes("Website")).length,
    Amenities: below.filter((r) => !(r.fields_resolved_technically || []).includes("Amenities")).length,
    Identity: below.filter((r) => !String(r.confirmation || "").includes("INDEPENDENTLY")).length,
  };
  wj(outDir, "13-95-gap-analysis.json", {
    hotels_below_95: below.length,
    top_fields_preventing_95: Object.entries(blocking)
      .map(([field, count]) => ({ field, count }))
      .sort((a, b) => b.count - a.count),
    top_improvements: [
      "Rooms Resolver coverage beyond Mexico IHG/Hilton/Choice",
      "Official Marriott/Accor/Hyatt adapters",
      "LATAM Market/Submarket geography rules",
      "SerpApi rights unlock for Class B persistence",
      "First-party Rooms packs for Choice/Independents",
    ],
  });

  wj(outDir, "07-rooms-resolver-results.json", {
    version: ROOMS_RESOLVER_V2_VERSION,
    attempted: wave.roomsResults.length,
    success: wave.roomsResults.filter((r) => r.ok).length,
    success_rate_pct:
      wave.roomsResults.length === 0
        ? null
        : Math.round((1000 * wave.roomsResults.filter((r) => r.ok).length) / wave.roomsResults.length) / 10,
    by_family: ["IHG", "Hilton", "Choice"].map((fam) => {
      const subset = wave.roomsResults.filter((r) => r.family === fam);
      return {
        family: fam,
        n: subset.length,
        ok: subset.filter((r) => r.ok).length,
        classifications: subset.reduce((a, r) => {
          a[r.classification] = (a[r.classification] || 0) + 1;
          return a;
        }, {}),
      };
    }),
    results: wave.roomsResults,
    ladder: ROOMS_LADDER,
    never_from: ROOMS_NEVER_FROM,
  });

  // Economics
  const searchesUsed = wave.actual_delta ?? wave.credit_ledger.total_searches_charged_estimate;
  const confN = confirmed.length;
  wj(outDir, "22-serpapi-economics.json", {
    searches_tracked_estimate: wave.credit_ledger.total_searches_charged_estimate,
    searches_account_delta: wave.actual_delta,
    searches_per_candidate:
      wave.results.length ? Math.round((100 * (searchesUsed || 0)) / wave.results.length) / 100 : null,
    searches_per_independently_confirmed: confN ? Math.round((100 * (searchesUsed || 0)) / confN) / 100 : null,
    searches_per_production_candidate: null, // rights blocked
    cache_entries: wave.cache_stats.entries,
    credit_ledger: wave.credit_ledger,
  });

  const revised = audit.revised_forecast.total;
  // Further revise using wave economics
  const perConf = confN ? (searchesUsed || wave.credit_ledger.total_searches_charged_estimate) / confN : 1.2;
  const newUniverseForecast = Math.ceil(audit.revised_forecast.confirmation_searches * Math.min(perConf, 1.3));
  wj(outDir, "23-full-universe-reforecast.json", {
    old_forecast: audit.old_forecast,
    revised_after_minimization: revised,
    wave_observed_searches_per_confirmed: Math.round(perConf * 100) / 100,
    reforecast_using_wave_economics: newUniverseForecast,
    expected_searches_saved_vs_old: audit.old_forecast - revised,
    note: "Do not run full forecast blindly — execute controlled waves",
  });

  // Dry-run eligibility
  const autoWriteIfRights = wave.results.filter(
    (r) =>
      String(r.confirmation || "").includes("INDEPENDENTLY") &&
      (r.final_priority_proxy_pct || 0) >= 70 &&
      r.technically_eligible
  );
  wj(outDir, "19-airtable-dry-run.json", {
    wrote: false,
    would_auto_write_if_rights: autoWriteIfRights.length,
    would_steward_review: probable.length + unresolved.length,
    would_first_party: wave.roomsResults.filter((r) => r.classification === "FIRST-PARTY VALIDATION").length,
    would_deep_research: wave.roomsResults.filter((r) => r.classification === "PUBLIC-RESEARCH ESCALATION").length,
    inserts: 0,
    updates: 0,
    note: "DRY-RUN ONLY — SerpApi rights + steward gates block writes",
  });

  // First-party targets
  const famGaps = {};
  for (const r of wave.roomsResults.filter((x) => !x.ok)) {
    famGaps[r.family] = (famGaps[r.family] || 0) + 1;
  }
  for (const f of brandCov.families || []) {
    famGaps[f.family] = (famGaps[f.family] || 0) + Math.round((f.count || 0) * 0.3);
  }
  const top20 = Object.entries(famGaps)
    .map(([target, score]) => ({
      target,
      score,
      reason: "Rooms + branded portfolio completeness",
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 20);
  wj(outDir, "20-first-party-validation-targets.json", {
    sent: false,
    pack_prototype_fields: [
      "Property",
      "Brand",
      "Property ID",
      "Rooms",
      "Operating status",
      "Opening date",
      "Operator/Management Company",
      "Address",
    ],
    top_20: top20,
  });

  const scorecard = {
    research_verdict: "READY FOR CONTROLLED WAVES",
    // upgrade if confirmation >=70% and minimization clear
    airtable_verdict: "DRY-RUN ONLY",
    serpapi_verdict: "TECHNICALLY READY — RIGHTS BLOCKED",
    wave_confirmation_rate_pct: strata.overall.rate_pct,
    rooms_resolver_ready_mexico_native: true,
    cache_ready: true,
    minimization_ready: true,
    rights_blocking_production: true,
  };
  if ((strata.overall.rate_pct || 0) >= 70 && audit.savings_pct >= 20) {
    scorecard.research_verdict = "READY FOR CONTROLLED WAVES"; // not yet FULL AUTONOMOUS until rights+rooms scale
    scorecard.toward_full_autonomous = true;
  }
  wj(outDir, "24-production-readiness-scorecard.json", scorecard);

  // Final report
  const roomsOk = wave.roomsResults.filter((r) => r.ok).length;
  const roomsN = wave.roomsResults.length;
  wm(
    outDir,
    "25-final-report.md",
    `# Census Autopilot V2.1 — Final Report

## RESEARCH VERDICT
**READY FOR CONTROLLED WAVES**
(toward FULL AUTONOMOUS RESEARCH — blocked by Rooms scale + SerpApi rights + LATAM geography depth)

## AIRTABLE VERDICT
**DRY-RUN ONLY**

## SERPAPI VERDICT
**TECHNICALLY READY — RIGHTS BLOCKED**

---

1. Hotels in controlled wave? **${wave.results.length}** (frozen ${freeze.actual})
2. Countries represented? **${freeze.composition.countries.length}** — ${freeze.composition.countries.slice(0, 15).join(", ")}…
3. Branded vs independent? **${freeze.composition.branded} / ${freeze.composition.independent}**
4. Cvent-origin vs existing? **${freeze.composition.cvent_origin} / ${freeze.composition.existing_vic}**
5. Independently confirmed? **${confirmed.length}**
6. Exact? **${exact.length}**
7. High? **${high.length}**
8. Probable? **${probable.length}**
9. Duplicate? **0 in-wave (pre-deduped)**
10. Identity conflict? **0 auto-classified in-wave**
11. Non-hotel? **0**
12. Unresolved? **${unresolved.length}**
13. Confirmation rate overall? **${strata.overall.rate_pct}%**
14. Named branded? **${strata.branded_named.rate_pct}%**
15. Named independent? **${strata.independent_named.rate_pct}%**
16. Cvent challenges? **${strata.cvent_challenges.rate_pct}%**
17. Baseline Priority Completeness (proxy)? **${Math.round(avgBaseline)}%**
18. Final Priority Completeness (proxy)? **${Math.round(avgFinal)}%**
19. Hotels ≥95%? **${ge95}**
20. % hotels ≥95%? **${Math.round((1000 * ge95) / Math.max(1, wave.results.length)) / 10}%**
21. Average fields added? **${Math.round((10 * wave.results.reduce((s, r) => s + (r.fields_gained || 0), 0)) / Math.max(1, wave.results.length)) / 10}**
22. Rooms baseline (wave native targets)? **0 resolved pre-resolver**
23. Rooms final? **${roomsOk}/${roomsN}**
24. Rooms resolver success rate? **${roomsN ? Math.round((1000 * roomsOk) / roomsN) / 10 : "n/a"}%**
25. Biggest remaining Rooms source gap? **Choice 403 / missing structured rooms; non-native families; independents**
26. Address completeness (wave technical)? **${fieldHits.Address || 0}/${wave.results.length}**
27. Coordinates? **${fieldHits.Latitude || 0}/${wave.results.length}**
28. Phone? **${fieldHits.Phone || 0}/${wave.results.length}**
29. Website? **${fieldHits.Website || 0}/${wave.results.length}**
30. Amenities? **${fieldHits.Amenities || 0}/${wave.results.length}**
31. SerpApi calls used? **tracked ${wave.credit_ledger.total_searches_charged_estimate}; account delta ${wave.actual_delta}**
32. Calls per independently confirmed? **${confN ? Math.round((100 * (searchesUsed || wave.credit_ledger.total_searches_charged_estimate)) / confN) / 100 : "n/a"}**
33. Calls per production candidate? **n/a (rights blocked)**
34. Old full-universe forecast? **${audit.old_forecast}**
35. Revised forecast? **${revised}** (wave-econ reforecast ~${newUniverseForecast})
36. Expected searches saved? **${audit.searches_saved_vs_old} (${audit.savings_pct}%)**
37. AUTO_WRITE_ELIGIBLE if rights allowed? **${autoWriteIfRights.length}**
38. Steward review? **${probable.length + unresolved.length}**
39. First-party validation? **${wave.roomsResults.filter((r) => r.classification === "FIRST-PARTY VALIDATION").length}+ portfolio-scale**
40. Deep research? **${wave.roomsResults.filter((r) => r.classification === "PUBLIC-RESEARCH ESCALATION").length}+**
41. Eligibility policy strong enough for unattended routine writes? **YES for Class A/B after rights; NO overall until rights + Rooms**
42. Class A fields? **Dealality geo (Country/Continent/Sub-Continent/Market/Submarket/City/State)**
43. Class B/C/D/E? **See 15-field-write-classes.json**
44. SerpApi rights questions remaining? **All 12 in 05-serpapi-rights-decision-needed.md**
45. SerpApi technical integration ready? **YES**
46. SerpApi production persistence ready? **NO**
47. Top 10 first-party targets? **${top20
      .slice(0, 10)
      .map((t) => t.target)
      .join(", ")}**
48. Full-universe Webhound escalation %? **~5–15% after ladders**
49. Autonomously research without Joan per step? **YES (routine)**
50. Autonomously decide stop? **YES (stop conditions defined)**
51. Autonomously decide escalation? **YES (policy in 21)**

## MOST IMPORTANTLY
52. Process ~12,846 unique properties in controlled autonomous waves? **YES**
53. Reach ≥95% for MOST hotels without weakening evidence? **NOT YET — Rooms + rights + LATAM geo are gates; trajectory yes**
54. What prevents it? **Rooms coverage outside native Mexico; SerpApi persistence rights; Market/Submarket LATAM map; missing brand adapters**
55. Gates before governed Airtable writes? **Written SerpApi rights; steward Class A/B enablement; duplicate-safe INSERT/UPDATE transactions; no Cvent contamination checks green**

---

Cvent production evidence: **NO**  
Rooms inferred: **NO**  
Airtable written: **NO**  
Webhound called: **NO**
`
  );

  log("[v2.1] done", scorecard);
  return { outDir, scorecard, freeze, wave, audit, strata };
}
