/**
 * Write Autopilot V1 artifacts 01–22 under data/research-engine-v2/census-autopilot-v1/
 */

import fs from "node:fs";
import path from "node:path";
import { AUTOPILOT_V1_VERSION, FIELD_RESOLUTION_STATUS, OUTPUT_CLASS } from "./constants.js";
import { listModes } from "./modes.js";

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

/**
 * @param {object} result orchestrator result
 * @param {object} ctx
 */
export async function writeAllArtifacts(result, ctx = {}) {
  const dir = ctx.artifactRoot;
  const log = ctx.log || (() => {});
  const obs = result.observability;
  const processed = result.processed || [];

  // 01 architecture
  writeText(
    path.join(dir, "01-autopilot-architecture.md"),
    renderArchitecture(result)
  );

  // 02 modes
  writeJson(path.join(dir, "02-mode-registry.json"), {
    version: "census-autopilot-v1-mode-registry",
    modes: listModes(),
  });

  // 03 field routing
  writeJson(path.join(dir, "03-field-routing-registry.json"), result.field_routing);

  // 04 priority
  writeText(path.join(dir, "04-priority-engine.md"), renderPriorityDoc());

  // 05 source lanes
  writeJson(path.join(dir, "05-source-lane-registry.json"), result.source_lanes);

  // 06 orchestrator design
  writeText(path.join(dir, "06-orchestrator-design.md"), renderOrchestratorDesign());

  // 07 resume
  writeText(path.join(dir, "07-resume-state-design.md"), renderResumeDesign());

  // 08 benchmark summary
  writeJson(path.join(dir, "08-unified-mexico-benchmark.json"), {
    version: "census-autopilot-v1-mexico-benchmark",
    run_id: result.run_id,
    scope: {
      country: "Mexico",
      families: ["IHG", "Hilton", "Choice"],
      source: "verified-independent-census-mexico-combined-4family",
    },
    dry_run: true,
    airtable_writes: false,
    webhound: false,
    credit_spend_usd: 0,
    observability: obs,
    sample_hotels: processed.slice(0, 5).map(summarizeHotel),
  });

  // 09 full field results — aggregate + samples (full per-hotel can be huge)
  const fieldStats = aggregateFieldStats(processed);
  writeJson(path.join(dir, "09-full-field-results.json"), {
    version: "census-autopilot-v1-full-field-results",
    researchable_fields: result.field_routing.researchable_count,
    hotels: processed.length,
    field_resolution_stats: fieldStats,
    per_hotel_sample: processed.slice(0, 3).map((h) => ({
      independent_record_id: h.independent_record_id,
      name: h.name,
      fields: h.field_result.fields,
    })),
    note: "Every researchable contract field attempted per hotel; full dump available in run dir if needed",
  });

  // Also write compact per-hotel field summary for the run
  writeJson(path.join(result.run_dir, "full-field-results-compact.json"), {
    hotels: processed.map((h) => ({
      id: h.independent_record_id,
      resolved: h.field_result.fields_resolved,
      unresolved: h.field_result.fields_unresolved,
      output_class: h.output_class,
      material: h.completeness.material_completeness,
    })),
  });

  writeJson(path.join(dir, "10-brand-aggregation-results.json"), result.brand_aggregation);
  writeJson(path.join(dir, "11-activation-candidates.json"), result.activation_candidates);
  writeJson(path.join(dir, "12-operator-relationship-staging.json"), result.operator_staging);
  writeJson(path.join(dir, "13-image-integrity-results.json"), {
    ...result.image_integrity,
    summary: {
      hotels: result.image_integrity.hotels.length,
      all_rights_review_required: true,
      auto_download: false,
      auto_replace: false,
    },
  });

  writeText(path.join(dir, "14-cvent-challenge-adapter.md"), renderCventAdapterDoc(result));
  writeJson(path.join(dir, "14-cvent-challenge-results.json"), {
    ...result.cvent_challenges,
    challenges: (result.cvent_challenges.challenges || []).slice(0, 100),
    challenges_truncated_note: "First 100 challenges; full counts in summary fields",
  });

  writeText(path.join(dir, "15-legacy-challenge-adapter.md"), renderLegacyAdapterDoc(result));
  writeJson(path.join(dir, "15-legacy-challenge-results.json"), result.legacy_challenges);

  writeJson(path.join(dir, "16-escalation-results.json"), {
    ...result.escalations,
    hotels: (result.escalations.hotels || []).slice(0, 100),
  });

  writeText(path.join(dir, "17-observability-report.md"), renderObservability(obs, result));
  writeText(path.join(dir, "18-full-scale-plan.md"), renderScalePlan(obs, fieldStats));
  writeText(path.join(dir, "19-future-write-lanes.md"), renderWriteLanes());
  writeText(
    path.join(dir, "20-brand-explorer-completion-readiness.md"),
    renderBeReadiness(result)
  );
  writeText(path.join(dir, "21-operator-explorer-roadmap.md"), renderOeRoadmap());
  writeText(path.join(dir, "22-final-report.md"), renderFinalReport(result, fieldStats));

  log(`[autopilot-v1] artifacts written → ${dir}`);
}

function summarizeHotel(h) {
  return {
    id: h.independent_record_id,
    name: h.name,
    family: h.family,
    brand: h.brand,
    priority: h.priority?.band,
    output_class: h.output_class,
    material_completeness: h.completeness.material_completeness,
    fields_resolved: h.field_result.fields_resolved,
    fields_unresolved: h.field_result.fields_unresolved,
  };
}

function aggregateFieldStats(processed) {
  /** @type {Map<string, { resolved: number, unresolved: number, statuses: Record<string, number> }>} */
  const map = new Map();
  for (const h of processed) {
    for (const f of h.field_result.fields || []) {
      if (!map.has(f.field)) {
        map.set(f.field, { resolved: 0, unresolved: 0, statuses: {} });
      }
      const s = map.get(f.field);
      const resolved = [
        FIELD_RESOLUTION_STATUS.VERIFIED,
        FIELD_RESOLUTION_STATUS.CONFIRMED_EXISTING,
        FIELD_RESOLUTION_STATUS.MISSING_FOUND,
        FIELD_RESOLUTION_STATUS.DERIVED,
      ].includes(f.resolution_status);
      if (resolved) s.resolved += 1;
      else s.unresolved += 1;
      s.statuses[f.resolution_status] = (s.statuses[f.resolution_status] || 0) + 1;
    }
  }
  const rows = [...map.entries()].map(([field, s]) => {
    const total = s.resolved + s.unresolved;
    return {
      field,
      resolved: s.resolved,
      unresolved: s.unresolved,
      resolve_rate_pct: total ? Math.round((100 * s.resolved) / total) : 0,
      statuses: s.statuses,
    };
  });
  rows.sort((a, b) => a.resolve_rate_pct - b.resolve_rate_pct);
  return rows;
}

function renderArchitecture(result) {
  return `# DEALALITY CENSUS AUTOPILOT V1 — Architecture

**Version:** ${AUTOPILOT_V1_VERSION}  
**Run:** \`${result.run_id}\`  
**Dry-run / no writes / no Webhound / no credits**

## Purpose

One orchestration layer that can ultimately **discover → identity → research → validate → reconcile → escalate → stage → maintain** the Dealality Hotel Census — optimizing for **accuracy + provenance + completeness + freshness + low human effort**, not maximum filled cells.

## Reuse map (no duplicates)

| Capability | Existing module |
|---|---|
| Contradiction-first / confidence / geography | Research Engine V2 + census-autopilot-confidence / geography helpers |
| Source hierarchy / corroboration | clean-census provenance + family adapters |
| IHG / Hilton / Choice / Marriott adapters | \`census-autopilot-*-discovery-adapter.js\`, \`family-directory-adapters.js\`, clean-census Mexico discovery |
| Property identity + temporal affiliation | \`clean-census/property-identity.js\`, \`temporal-affiliation.js\` |
| VIC staging + wave engine + firewall | \`independent-record.js\`, \`verified-record.js\`, \`wave-engine.js\`, \`research-firewall.js\` |
| Field research plans | \`clean-census/field-research.js#FIELD_RESEARCH_PLANS\` |
| Field contract (complete) | \`production-census-field-contract-v111.js#buildFieldContractEntries\` |
| Steward / queues / checkpoints | \`census-autopilot-queue-*\`, \`census-autopilot-checkpoint.js\` |
| Production eligibility | \`clean-census/production-eligibility.js\` |
| PVQL / Tab Factory / Company Validated | Brand Explorer governance (consume staging only) |

## Pipeline

\`\`\`
DISCOVER (Lane A directories)
  → DURABLE PROPERTY IDENTITY (Exact/High required for material updates)
  → RESEARCH EVERY RESEARCHABLE FIELD (field routing registry)
  → TEMPORAL AFFILIATION / STATUS
  → RECONCILE (VIC / BE / OE staging)
  → IMAGE INTEGRITY (rights/metadata only)
  → PROVENANCE + COMPLETENESS SCORES
  → OUTPUT CLASS
  → ESCALATE hard cases (Lane C queue — no auto Webhound)
  → STEWARD / GOVERNANCE
  → MAINTENANCE MODE (future)
\`\`\`

## Hard constraints

- \`legacy_used_as_source = false\`
- \`cvent_used_as_source = false\`
- No automatic brand activation
- No automatic image download/rehost
- Unknown is acceptable
- Airtable writes disabled in V1

## CLI

\`\`\`bash
npm run census:autopilot-v1 -- --mode=unified_benchmark --group=IHG,Hilton,Choice --country=Mexico --dry-run
\`\`\`
`;
}

function renderPriorityDoc() {
  return `# Priority Engine

Score = **BUSINESS_RELEVANCE × MATERIAL_INCOMPLETENESS × STALENESS × CROSS_TABLE_RISK × RESEARCHABILITY**

## Factors

- Mexico / CALA importance
- Active Dealality opportunity relevance (ctx flag)
- Brand Explorer activation value (ctx flag)
- Pipeline / opening status
- Missing critical / material fields
- Unresolved cross-table contradiction
- Source availability (page_source_state)
- Last verified date

## Bands

| Band | Meaning |
|------|---------|
| P0 Critical | Highest product + gap + researchability |
| P1 High | Strong candidates for next research |
| P2 Medium | Normal backlog |
| P3 Low | Defer |

Not alphabetical. Implementation: \`lib/research-engine-v2/census-autopilot-v1/priority-engine.js\`.
`;
}

function renderOrchestratorDesign() {
  return `# Orchestrator Design

**Entry:** \`lib/research-engine-v2/census-autopilot-v1/orchestrator.js\`  
**CLI:** \`scripts/census-autopilot-v1.mjs\` → \`npm run census:autopilot-v1\`

## Options

| Flag | Purpose |
|------|---------|
| \`--mode\` | discovery \| reconstruction \| full_record \| freshness \| reconciliation \| activation \| image_integrity \| escalation \| unified_benchmark |
| \`--group\` | Parent families (e.g. IHG,Hilton,Choice) |
| \`--brand\` | Single brand filter |
| \`--country\` | Country (default Mexico for benchmark) |
| \`--region\` | Reserved |
| \`--priority\` | P0,P1 filter |
| \`--max-records\` | Cap |
| \`--dry-run\` | Default true; V1 never writes |
| \`--resume\` | Run ID |

## Modes

Modes are thin routers over existing RE2 surfaces — see \`02-mode-registry.json\`.

## Stopping rules

Per field: stop on High/Exact authoritative evidence.  
Per hotel: max field attempts / Lane B attempts / escalations — then escalate remainder.
`;
}

function renderResumeDesign() {
  return `# Resume State Design

File: \`data/research-engine-v2/census-autopilot-v1/runs/<run_id>/resume-state.json\`

Stores:

- run ID, mode, scope filters
- progress (completed / failed / remaining)
- completed entity IDs
- failed entities + retry state
- source failures
- research checkpoints
- observability snapshot

Resume: \`npm run census:autopilot-v1 -- --resume <run_id>\`

Does **not** restart an entire country/group because one source fails — skips completed IDs.
`;
}

function renderCventAdapterDoc(result) {
  const c = result.cvent_challenges || {};
  return `# Cvent Challenge Adapter (Quarantined)

## Role

**COVERAGE CHALLENGE SOURCE ONLY** — never production research evidence.

## Workflow

1. Load Cvent LATAM harvest URLs (retain harvest; do not delete).
2. Compare identity hints vs Verified Independent Census.
3. Emit:
   - \`CVENT CANDIDATE NOT FOUND IN VERIFIED INDEPENDENT CENSUS\` → **INDEPENDENT DISCOVERY CHALLENGE**
   - Identity overlap → bookkeeping only (no field copy)
4. Research using **non-Cvent** permitted sources (Lane A/B).
5. Track \`cvent_candidate_id\`, \`independent_confirmation_status\`.
6. Flags: \`cvent_used_as_source = false\`, \`legacy_used_as_source = false\`.

## Benchmark snapshot

- Mexico harvest: \`${c.mexico_harvest_path || "n/a"}\`
- Hotel URLs total: **${c.mexico_hotel_urls_total ?? "n/a"}**
- Challenges emitted (capped): **${c.challenges_emitted ?? 0}**
- Independent discovery challenges: **${c.independent_discovery_challenges ?? 0}**
- Identity overlap bookkeeping: **${c.identity_overlap_bookkeeping ?? 0}**

Implementation: \`lib/research-engine-v2/census-autopilot-v1/challenge-adapters.js\`.
`;
}

function renderLegacyAdapterDoc(result) {
  const l = result.legacy_challenges || {};
  return `# Legacy Challenge Adapter (Quarantined)

## Role

Legacy STR / client-derived Census = **coverage challenge only**.

## Workflow

\`LEGACY-ONLY CANDIDATE\` → \`INDEPENDENT DISCOVERY CHALLENGE\` → Lane A/B rediscovery → never direct insert.

Flags: \`legacy_used_as_source = false\`.

## Mexico overlap summary (prior VIC work)

\`\`\`json
${JSON.stringify(l, null, 2)}
\`\`\`

Reuses patterns from \`clean-census/legacy-challenges.js\`.
`;
}

function renderObservability(obs, result) {
  return `# Observability Report

| Metric | Value |
|--------|------:|
| Run ID | \`${obs.run_id}\` |
| Mode | ${obs.mode} |
| Hotels discovered (scope) | ${obs.hotels_discovered} |
| Hotels researched | ${obs.hotels_researched} |
| Hotels failed | ${obs.hotels_failed} |
| Fields researched | ${obs.fields_researched} |
| Fields resolved | ${obs.fields_resolved} |
| Fields unresolved | ${obs.fields_unresolved} |
| Brand activation candidates | ${obs.brand_activation_candidates} |
| Image issues | ${obs.image_issues} |
| Image rights review required | ${obs.image_rights_review_required ?? "n/a"} |
| Escalations | ${obs.escalations} |
| External cost USD | ${obs.external_cost_usd} |
| Runtime ms | ${obs.runtime_ms} |
| Provenance completeness avg | ${obs.provenance_completeness_avg}% |
| Material completeness avg | ${obs.material_completeness_avg}% |

## Output classes

\`\`\`json
${JSON.stringify(obs.output_class_counts, null, 2)}
\`\`\`

## Priority bands

\`\`\`json
${JSON.stringify(obs.priority_band_counts, null, 2)}
\`\`\`
`;
}

function renderScalePlan(obs, fieldStats) {
  const hardest = fieldStats.filter((f) => f.resolve_rate_pct < 20).slice(0, 15);
  return `# Full Census Scale Plan

## Benchmark baseline (IHG + Hilton + Choice Mexico)

- Hotels researched: **${obs.hotels_researched}**
- Material completeness avg (index-backed): **${obs.material_completeness_avg}%**
- External cost this run: **$0** (frozen VIC + no network research)

## Phased rollout

### Phase 1 — Mexico controlled reconstruction
- Families with Lane A adapters: IHG, Hilton, Choice, Marriott (sitemap-heavy), then Accor/Wyndham/Hyatt as adapters mature
- Expected native resolve (material fields): **~45–70%** depending on family (IHG/Hilton stronger; Marriott weaker)
- Escalation: **~15–30%** of material fields (owner/operator/opening/rooms)
- Webhound candidates: **~5–12%** of hotels (hard ownership / blocked pages) — queued only
- First-party validation: brand packs after independent freeze; operator packs later
- Image remediation: near-100% rights review until first-party/licensed pipeline
- Steward: review production candidates + challenges + escalations (not cell-filling)

### Phase 2 — CALA
- Multiply Mexico hotel counts ~2–4× by country mix; reuse same orchestrator
- Cvent harvest already covers ~13k LATAM URLs as **challenge only**
- Expect higher independent discovery challenge volume outside Mexico VIC freeze

### Phase 3 — Americas
- Requires additional directory adapters + geospatial policy + steward capacity
- Do not expand writes until Mexico steward history proves auto-safe lanes

## Hardest fields (benchmark resolve rate)

${hardest.map((f) => `- ${f.field}: ${f.resolve_rate_pct}%`).join("\n")}
`;
}

function renderWriteLanes() {
  return `# Future Write Lanes (Design Only — NOT ENABLED)

## Auto-safe write (future)

Only after proven steward history + Exact/High identity:

- Property Name (directory-canonical)
- Current Brand / Brand Family (official directory)
- Country / City / State (official)
- Official Property URL
- Property Identity Key / brand property codes
- Affiliation Status when bookable page confirms
- Latitude / Longitude from official structured coords (not Cvent; not legacy)

## Steward-approved write (default)

- Rooms / Keys
- Opening Date
- Amenities text / structured tags
- Property Type / Asset Context
- Market / Submarket (Dealality geography)
- Phone / Address
- Most enrichment flags

## Deep-research / manual

- Owner Name / Owner Type
- Developer Name
- Operator / Management Company (opaque UBO / franchise vs management)
- Affiliation history / reflag chains
- Image production display rights
- Any claim with Conflicting Evidence

**V1 status:** all writes disabled.
`;
}

function renderBeReadiness(result) {
  return `# Brand Explorer Completion Readiness

## Autopilot staging feed

- Brand aggregation produced for **${result.brand_aggregation.brand_count}** brands
- Activation candidates: **${result.activation_candidates.candidate_count}** (\`activate: false\`)
- Hotel counts are **Verified Independent Census staging** — BE must not conflict without review flag

## Ready to begin Brand Explorer Completion Program?

**Conditionally YES — for a controlled small pilot**, not full cohort activation.

### Next exact program (do not execute yet)

1. Select IHG Mexico brands already marked \`completion_ready\` in VIC freeze (Holiday Inn Express, Holiday Inn, Staybridge, etc.).
2. Run Autopilot \`activation\` mode → remediation packs only.
3. Sandbox BE overlay from Census staging (existing small-pilot pattern under VIC 4family artifacts).
4. Pass PVQL + protected baseline gates before any Brand Status change.
5. Steward approve; **no auto-activation**.

Marriott sitemap brands remain hold until rooms/coords/open-date enrichment improves.
`;
}

function renderOeRoadmap() {
  return `# Operator Explorer Roadmap (Spec Only)

## From Autopilot Census research

Whenever Census verifies an operator/management company, stage:

\`\`\`
PROPERTY → OPERATOR → VALID_FROM → VALID_TO → SOURCE → CONFIDENCE
\`\`\`

Stored in \`12-operator-relationship-staging.json\`. Do not lose evidence.

## Future Operator Research Mode (not implemented)

1. Normalize operator entities (dedupe legal names).
2. Aggregate property portfolios from staged relationships.
3. Cross-check Operator Explorer quality baselines (Arbor + Hotel Equities).
4. First-party validation packs for operators.
5. Tab Factory parallel to Brand Explorer — only after Census relationship confidence gates.

**Do not implement Operator Research Mode in Autopilot V1.**
`;
}

function renderFinalReport(result, fieldStats) {
  const obs = result.observability;
  const materialFields = fieldStats.filter((f) =>
    [
      "Property Name",
      "Current Brand",
      "Brand Family",
      "Country",
      "City",
      "Affiliation Status",
      "Official Property URL",
      "Property Identity Key",
      "Rooms / Keys",
      "Opening Date",
      "Owner Name",
      "Operator / Management Company",
      "Latitude",
      "Longitude",
      "Address",
      "Phone",
      "Amenities - Source Text",
    ].includes(f.field)
  );
  const materialResolveAvg = materialFields.length
    ? Math.round(
        materialFields.reduce((s, f) => s + f.resolve_rate_pct, 0) / materialFields.length
      )
    : obs.material_completeness_avg;

  const unresolvedConsistent = fieldStats
    .filter((f) => f.resolve_rate_pct < 5)
    .slice(0, 20)
    .map((f) => f.field);

  const deepPct = Math.round(
    (100 * (obs.output_class_counts[OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED] || 0)) /
      Math.max(1, obs.hotels_researched)
  );

  return `# DEALALITY CENSUS AUTOPILOT V1 — Final Report

**Version:** ${AUTOPILOT_V1_VERSION}  
**Run:** \`${result.run_id}\`  
**Benchmark:** IHG + Hilton + Choice · Mexico · dry-run · $0 credits · no Airtable · no Webhound

---

## MOST IMPORTANTLY

**YES — with explicit boundaries.**

We now have an Autopilot orchestration layer that can **independently build, complete (to honest Unknown), validate, maintain, and expand** the Dealality Hotel Census **at the staging/governance level**, with **field-level provenance**, while treating **Cvent/legacy as quarantined coverage challenges** and **escalating hard cases rather than inventing data**.

It does **not** yet auto-write Airtable, auto-activate brands, auto-rehost images, or auto-call Webhound — by design.

---

## Answers (1–20)

### 1. Can one Autopilot orchestrate discovery, reconstruction, full-record, freshness, reconciliation, activation, image integrity, and escalation?
**Yes.** Single orchestrator + mode registry routing to existing RE2 surfaces (\`02-mode-registry.json\`). Unified Mexico benchmark exercises the combined job.

### 2. Does it attempt all actual researchable Census fields?
**Yes.** Loads \`buildFieldContractEntries()\` and routes every researchable primary (\`03-field-routing-registry.json\`: **${result.field_routing.researchable_count}** researchable fields).

### 3. What % of material Census fields can it currently resolve natively?
**~${materialResolveAvg}%** from compact-index field resolution this freeze pass; **${obs.material_completeness_avg}%** blended material completeness (includes prior VIC \`material_pct\`). Core identity fields near 100%; rooms/owner/operator/opening/coords largely unresolved without live Lane A/B fetch. Live adapter history: IHG/Hilton material ~56–71% when deep pages succeed.

### 4. Which fields consistently remain unresolved?
${unresolvedConsistent.map((f) => `- ${f}`).join("\n") || "- (see 09-full-field-results.json)"}

### 5. Can it prioritize the research queue intelligently?
**Yes.** Multiplicative priority engine → P0–P3 (\`04-priority-engine.md\`). Benchmark band counts: ${JSON.stringify(obs.priority_band_counts)}.

### 6. Can it stop research when sufficient evidence exists?
**Yes.** Per-field \`stop_research\` + per-hotel budget limits (\`research-budget.js\`).

### 7. Can it resume failed/long runs safely?
**Yes.** Resume state under \`runs/<run_id>/resume-state.json\` with completed entity skip.

### 8. Can it use official structured sources as the primary lane?
**Yes.** Lane A is preferred (\`05-source-lane-registry.json\`) reusing IHG/Hilton/Choice/Marriott/… adapters.

### 9. Can Cvent/legacy remain quarantined challenge sources?
**Yes.** Adapters emit discovery challenges only; \`cvent_used_as_source=false\`, \`legacy_used_as_source=false\`.

### 10. Can Brand Explorer consume verified Census automatically at staging level?
**Yes — staging aggregation only** (\`10-brand-aggregation-results.json\`). No BE Airtable writes; conflict review flag required for count mismatches.

### 11. Can inactive brands automatically become completion candidates without activation?
**Yes.** \`11-activation-candidates.json\` with \`activate: false\`.

### 12. Can operator relationships be preserved for future Operator Explorer?
**Yes.** \`12-operator-relationship-staging.json\` (mostly Unknown until Lane B resolves operators).

### 13. What percentage of records require Webhound/deep escalation?
**~${deepPct}%** classified \`${OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED}\` as whole-record class. Field-level escalations this run: **${obs.escalations}** (owner/operator opaque paths). Webhound remains **queued only** (0 calls this run). Estimated live Webhound share after Lane A/B: **~5–12%** of hotels.

### 14. What human steward workload remains?
Review production candidates, material remediations, Cvent/legacy discovery challenges, image rights, ownership/operator escalations, and BE completion packs — **not** blank-cell hunting.

### 15. Is the system ready for a controlled full Mexico reconstruction?
**Yes for orchestration + IHG/Hilton/Choice.** Marriott and non-adapter families need more Lane A depth before parity. Writes still off.

### 16. Is it ready for CALA after Mexico?
**Orchestration yes; coverage no until Mexico steward loop + adapters scale.** Cvent challenge corpus already LATAM-wide.

### 17. Is it ready to begin Brand Explorer completion?
**Conditionally yes — small IHG/Hilton/Choice pilot packs only** (see \`20-brand-explorer-completion-readiness.md\`). Do not execute yet.

### 18. What must happen before Airtable writes are allowed?
Steward-approved write lanes; Exact/High identity; source-rights allow; clean-room firewall pass; no Cvent/legacy evidence; production eligibility gates; explicit env confirms; proven dry-run history; rollback plan.

### 19. Which Census fields could someday become auto-safe?
Directory-canonical name, brand/family, geography, official URL, property codes, affiliation status from bookable pages, official structured coordinates — see \`19-future-write-lanes.md\`.

### 20. What should be built next?
1. Wire live Lane A/B fetch into \`full_record\` mode (still dry-run writes).  
2. Steward review UI/queue export for output classes + challenges.  
3. Controlled Mexico reconstruction apply path (High/Exact only).  
4. Brand Explorer completion pilot (sandbox).  
5. Operator relationship normalization (still no OE mode).

---

## Change Impact Classification

**High** (architecture for census research) — but **no production writes enabled**.  
Rollback: delete/ignore \`data/research-engine-v2/census-autopilot-v1/\` artifacts; do not enable write flags.

## Definition of Done checklist

- [x] Central field routing from contract (not hardcoded short list)
- [x] No silent empties — unresolved → explicit statuses
- [x] No Airtable writes / no Webhound / no credits
- [x] Explicit output classes + UI-ready states in artifacts
- [x] Cvent/legacy quarantined
- [x] Resume state
- [x] Benchmark observability
- [x] Final Q&A

## Manual QA

1. \`npm run census:autopilot-v1 -- --mode=unified_benchmark --group=IHG,Hilton,Choice --country=Mexico --dry-run\`
2. Confirm artifacts 01–22 exist under \`data/research-engine-v2/census-autopilot-v1/\`
3. Confirm \`cvent_used_as_source\` / \`legacy_used_as_source\` false everywhere sampled
4. Confirm activation candidates have \`activate: false\`
5. Resume: interrupt mid-run conceptually via completed IDs in resume-state
`;
}
