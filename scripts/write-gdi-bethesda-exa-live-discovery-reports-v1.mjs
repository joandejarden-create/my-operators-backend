#!/usr/bin/env node
/**
 * Zero-cost report writer for Bethesda live Exa discovery effectiveness V1.
 * Reads already-persisted live artifacts only — does not call providers.
 */
import fs from "node:fs";
import path from "node:path";

const outDir = "reports/gdi-bethesda-exa-live-discovery-effectiveness-v1";
const a = JSON.parse(
  fs.readFileSync(path.join(outDir, "analysis-core.json"), "utf8"),
);
const run = JSON.parse(
  fs.readFileSync(
    path.join(
      outDir,
      "observability/data/group-demand-intelligence/execution-observability/recLuxvwwxID7U2B8/gdi_e2e_14seg_mwladic1_ff9898.json",
    ),
    "utf8",
  ),
);

const cmd =
  "GDI_EXA_AGENT_LIVE=1 npm run gdi:canonical-full-e2e -- --hotel recLuxvwwxID7U2B8 --mode live --authorize-paid-discovery --authorize-paid-research --authorize-exa-agent-live --budget-ceiling-usd 15 --max-candidates 20 --exa-effort medium --out reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/live-run --observability-root reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/observability";

const newUnique = a.classifications.filter((c) => c.evaluationLabel === "NEW_UNIQUE");
const dupes = a.classifications.filter((c) => c.evaluationLabel === "DUPLICATE");
const rejected = a.classifications.filter((c) => c.evaluationLabel === "REJECTED");
const accepted = newUnique.length;
const costPerAccepted = accepted ? a.spentUsd / accepted : null;

const segRows = Object.entries(a.segments)
  .map(([sid, s]) => {
    const deepStatus = run.segments?.[sid]?.deepResearchStatus || "UNKNOWN";
    return `| ${sid} | LIVE_EXECUTED | ${s.liveInvocations} | $${Number(s.costUsd || 0).toFixed(2)} disc | ${s.processed} | ${s.liveDeep} | ${deepStatus} | ${JSON.stringify(s.labels || {})} |`;
  })
  .join("\n");

function write(name, body) {
  fs.writeFileSync(path.join(outDir, name), body.endsWith("\n") ? body : `${body}\n`);
}

write(
  "FOUNDER_REPORT.md",
  `# FOUNDER REPORT — Bethesda Marriott Live Exa Discovery Effectiveness V1

**Hotel:** Bethesda Marriott (\`recLuxvwwxID7U2B8\`)  
**Run ID:** \`${a.runId}\`  
**Verdict:** \`${a.verdict}\` (\`${a.researchCompleteness}\`)  
**Actual Exa spend:** **$${a.spentUsd.toFixed(2)}** of $15.00 hard ceiling (remaining $${a.remainingUsd.toFixed(2)})  
**Publication:** none · Airtable writes: 0 · Apify: not orchestrated

## Live execution outcome

Live Exa AI executed successfully through the canonical Full E2E research entry after safety gates in PR #348.

- Preflight status before spend: **LIVE_READY**
- Provider: Exa Agent only (discovery + deep seed)
- Discovery: **14/14 segments LIVE** (\`freshRetrievalCells=14\`, \`resumedCells=0\`)
- Candidates extracted (raw): **165**
- Deep-researched / processed under \`maxCandidates=20\`: **20**
- Paid deep research operations settled: **${a.ledger.deepOps}**
- Certificate: \`budgetsRespected=${a.budgetsRespected}\`, \`outputsIngested=${a.outputsIngested}\` (evidence-based, not hard-coded)

## Segment coverage

All fourteen canonical segments received **LIVE_EXECUTED** discovery. Deep research completed for 13 segments; \`preferred_lodging_partnerships\` deep status **SKIPPED** (duplicate / no paid deep retained).

No discovery segment was \`FIXTURE_ONLY\` or \`SKIPPED_BUDGET\`. Budget headroom remained $11.80 after the controlled campaign.

## Novelty vs 67 published baseline

Baseline package: \`${a.baseline.packageVersion}\` · fingerprint \`${a.baseline.fingerprint}\` · **67** CUSTOMER_PUBLISHED IDs (unchanged).

| Evaluation label | Count |
|---|---|
| NEW_UNIQUE | ${newUnique.length} |
| EXISTING_ENRICHED | 0 |
| DUPLICATE | ${dupes.length} |
| REJECTED | ${rejected.length} |
| FURTHER_RESEARCH | 0 |

### NEW_UNIQUE (watchlist-grade)

1. **Amentum Holdings / Amentum** — \`corporate_accounts_scaling\` — WATCHLIST_RESEARCH · 2 contacts investigated · **1 sales-ready**
2. **American Political Science Association** — \`government_public_sector\` — WATCHLIST_RESEARCH · contacts thin
3. **SAME Foundation** — \`social_special_events\` — WATCHLIST_RESEARCH · contacts thin

### DUPLICATE

- **Rock Creek Mansion** — matches baseline \`gdi_plp_live_fa2063dc1195\`

### Commercial qualification

Most processed rows dispositioned **EXCLUDE** (16/20). Three retained as **WATCHLIST_RESEARCH** (not CONFIRMED_DEMAND / RELIABLE_INFERRED_DEMAND). Speculative demand was not promoted.

Sales-ready contacts on processed set: **2** (Amentum + Enabled Intelligence; latter EXCLUDE).

## Main discovery weaknesses

1. High extract volume (165) but low commercially retained yield after qualification.
2. Missions still surface large national orgs / national events that fail Bethesda catchment (AHA, NFL, AEA, NASPA, etc.).
3. Contact relevance remains weak outside one corporate scaling hit.
4. \`maxCandidates=20\` intentionally capped deep spend — remaining raw candidates were not deep-researched in this campaign.
5. Preferred-lodging rediscovery of Rock Creek Mansion burned budget without novelty.

## Top five recommendations

1. Tighten Bethesda/DMV catchment + negative filters in Exa discovery missions (especially associations, sports, corporate meetings).
2. Prefer early corporate-expansion / workforce / project signals over national conference calendars.
3. Improve contact-relevance verification before counting sales-ready.
4. Dedupe against published inventory **before** paid deep seed.
5. Raise per-segment candidate diversity and strengthen local Montgomery County meeting/catering missions.

## Cost efficiency

- Actual spend: **$3.20**
- Cost per NEW_UNIQUE (watchlist): **$1.07**
- Cost per sales-ready NEW_UNIQUE: **$3.20**
- Cost per paid deep op: **$0.10** (medium effort quote settled)

**Customer inventories were not modified.**
`,
);

write(
  "LIVE_EXECUTION_CERTIFICATION.md",
  `# LIVE EXECUTION CERTIFICATION — Bethesda Exa V1

## Exact command

\`\`\`bash
${cmd}
\`\`\`

Safety prerequisite merged first: PR #348 (\`feat/gdi-bethesda-exa-live-safety-gates-v1\`) — dotenv CLI load, \`--authorize-exa-agent-live\`, hard ceiling required, \`assessCanonicalLiveReadiness\`, evidence-based \`budgetsRespected\` / \`outputsIngested\`.

## Authorization controls

| Control | Value |
|---|---|
| Provider | Exa AI only |
| \`authorizePaidDiscovery\` | true |
| \`authorizePaidResearch\` | true |
| \`authorizeExaAgentLive\` | true |
| \`GDI_EXA_AGENT_LIVE\` | 1 |
| \`budgetCeilingUsd\` | 15.00 |
| \`exaEffort\` | medium (bounded quote $0.10/call) |
| \`maxCandidates\` | 20 |
| Apify | disabled / not orchestrated |
| Publication | not authorized |
| Airtable | 0 writes |

## Run identity

- **Run ID:** \`${a.runId}\`
- **Hotel:** Bethesda Marriott \`recLuxvwwxID7U2B8\`
- **Observability:** \`reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/observability/...\`
- **Ledger:** \`live-run/paid-budget-ledger.json\`

## Provider requests (summary)

| Stage | Settled ops | Spend USD |
|---|---|---|
| DISCOVERY | ${a.ledger.discoveryOps} | 1.40 |
| DEEP_RESEARCH | ${a.ledger.deepOps} | ${(a.spentUsd - 1.4).toFixed(2)} |
| **Total** | **${a.ledger.ops}** | **${a.spentUsd.toFixed(2)}** |
| Blocked ops | ${a.ledger.blocked} | — |

All discovery cells: \`mode=FRESH_RETRIEVAL\` with Exa \`agent_run_*\` IDs. Deep ops: \`EXA_DEEP_SEED_PLUS_ORS\`.

## Discovery / deep-research outcomes

- Discovery extracted candidates: **165** across 14 cells
- Processed through original chain: **20**
- Live deep flags on processed: ${a.classifications.filter((c) => c.live).length}
- Certificate research completeness: \`${a.researchCompleteness}\`

## Actual cost

- **Spent:** $3.20
- **Ceiling:** $15.00
- **Remaining:** $11.80
- **budgetsRespected:** true (spent ≤ ceiling)

## Final certificate

- **Verdict:** \`${a.verdict}\`
- **outputsIngested:** true (persisted chain trace + discovery state + observability run)
- Execution monitor run summary: segmentsCompletedOf14=${run.summary?.segmentsCompletedOf14}, providerCalls=${run.summary?.providerCalls}, totalSpendUsd=${run.summary?.totalSpendUsd}

## Remaining execution gaps

1. Only 20 of 165 extracted candidates received paid deep research (intentional budget/maxCandidates control).
2. No CONFIRMED_DEMAND / RELIABLE_INFERRED_DEMAND promotions in this campaign — watchlist only.
3. Preferred lodging deep SKIPPED after duplicate detection.
4. Authorization label in ledger still defaults to \`GDI_AIMBRIDGE_FULL_E2E_LIVE_V1\` string (cosmetic; spend isolated to Bethesda outDir).
5. Customer publication not in scope — research artifacts only.
`,
);

write(
  "FOURTEEN_SEGMENT_EFFECTIVENESS.md",
  `# Fourteen-Segment Effectiveness — Bethesda Live Exa V1

Coverage classification key: **LIVE_EXECUTED** | CACHED_ONLY | FIXTURE_ONLY | SKIPPED_BUDGET | FAILED | NOT_APPLICABLE

All fourteen segments were **LIVE_EXECUTED** for discovery under the $15 campaign. Deep research coverage varies by retained candidates.

| Segment | Discovery coverage | Live disc ops | Disc spend | Processed | Live deep | Deep status | Eval labels |
|---|---|---|---|---|---|---|---|
${segRows}

## Notes by priority theme

### Corporate expansion (\`corporate_accounts_scaling\`)
Best commercial signal of the run: **Amentum** (WATCHLIST + sales-ready contact). Enabled Intelligence produced a sales-ready contact but EXCLUDE disposition.

### Corporate meetings (\`corporate_meetings_incentives\`)
Returned large national associations (AEA, AHA) → EXCLUDE. Mission still too national-event oriented for Bethesda catchment.

### Associations (\`associations_conferences_events\`)
NASPA / SIOP extracted → EXCLUDE. Need stronger local/rotational filters and overflow vs primary-city logic.

### Government (\`government_public_sector\`)
APSA → NEW_UNIQUE watchlist; AHA duplicate-path EXCLUDE. Government contractor / federal program missions under-represented vs association calendars.

### Projects / infrastructure
AlphaStruxure → EXCLUDE. Need stronger Montgomery County / federal facilities project signals.

### Medical / scientific (\`education_scientific\`)
SMFM / ASSA → EXCLUDE. NIH / Walter Reed / USUHS ecosystem under-hit relative to baseline inventory strength.

### Relocation / extended stay
Heven AeroTech → EXCLUDE. Weak extended-stay workforce framing.

### Local meetings / catering
AFCEA DC → EXCLUDE. Local chamber / day-meeting yield weak in this sample.

### Preferred lodging
Rock Creek Mansion **DUPLICATE** of published inventory — paid discovery without pre-deep inventory dedupe.
`,
);

write(
  "BETHESDA_OPPORTUNITY_COMPARISON.md",
  `# Bethesda Opportunity Comparison — Live Exa vs 67 Published

## Baseline

- Package: \`gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1\`
- Fingerprint: \`${a.baseline.fingerprint}\`
- Count: **67** CUSTOMER_PUBLISHED (inventory unchanged by this research)

Baseline ID index exported to \`baseline-67-inventory.json\`.

## Evaluation labels (processed deep set = 20)

| Label | Count | Notes |
|---|---|---|
| NEW_UNIQUE | ${newUnique.length} | Not in 67 by org-key match; watchlist only |
| EXISTING_ENRICHED | 0 | No material enrichment of published rows |
| DUPLICATE | ${dupes.length} | Rock Creek Mansion ↔ \`gdi_plp_live_fa2063dc1195\` |
| REJECTED | ${rejected.length} | EXCLUDE after live deep / qualification |
| FURTHER_RESEARCH | 0 | — |

## NEW_UNIQUE detail

${newUnique
  .map(
    (c) => `### ${c.organizationName}
- Segment: \`${c.segmentId}\`
- Opportunity ID (research): \`${c.opportunityId}\`
- Disposition: \`${c.disposition}\`
- Provider run: \`${c.providerRunId}\`
- Contacts investigated: ${c.contactsInvestigated} · sales-ready: ${c.salesReadyContacts}
- Baseline matches: none
`,
  )
  .join("\n")}

## DUPLICATE detail

${dupes
  .map(
    (c) =>
      `- **${c.organizationName}** (\`${c.opportunityId}\`) ↔ baseline ${c.baselineMatches.join(", ")}`,
  )
  .join("\n")}

## Rejected sample (national / unfit)

${rejected
  .slice(0, 12)
  .map((c) => `- ${c.organizationName} · \`${c.segmentId}\` · ${c.disposition}`)
  .join("\n")}

## Deduplication findings

1. Org-key match caught Rock Creek Mansion against preferred-lodging published inventory.
2. Many EXCLUDE rows are **not** baseline duplicates — they are newly seen but commercially rejected (wrong geography, speculative, or weak demand thesis).
3. No research IDs were written into the customer-published package.
4. Novelty is judged against the **67 published** fingerprint only, per founder directive.
`,
);

write(
  "CONTACT_AND_DEMAND_QUALITY.md",
  `# Contact and Demand Quality — Bethesda Live Exa V1

## Demand credibility

| Disposition | Count | Interpretation |
|---|---|---|
| EXCLUDE | 16 | Failed commercial / fit / evidence bar |
| WATCHLIST_RESEARCH | 3 | Credible enough to keep researching — not CONFIRMED / RELIABLE_INFERRED |
| undefined | 1 | Preferred lodging skip path |

No \`CONFIRMED_DEMAND\` or \`RELIABLE_INFERRED_DEMAND\` promotions occurred. Speculative demand was not elevated.

## Contact standards observed

| Metric | Value |
|---|---|
| Processed with any contact investigation | ${a.contactStats.withContactsInvestigated} |
| Processed with sales-ready contacts | ${a.contactStats.withSalesReady} |
| Total sales-ready contacts | ${a.contactStats.totalSalesReady} |

### Notable contacts

- **Amentum** (NEW_UNIQUE / WATCHLIST): 2 investigated, 1 sales-ready → treat as **RELEVANT_COMMERCIAL_ENTRY_POINT** pending buyer-authority verification (do not invent DIRECT_BUYER).
- **Enabled Intelligence** (EXCLUDE): 1 sales-ready contact but demand disposition EXCLUDE — contact alone ≠ opportunity.
- APSA / SAME Foundation: 0 sales-ready — **UNVERIFIED**.

## Hotel fit

Promising thesis: defense/gov-services corporate presence near Bethesda/North Bethesda (Amentum). National association calendars (AEA, AHA, NASPA, NFL) generally fail Bethesda catchment or are already better covered by existing published overflow/advocacy rows.

## Actionability

Best next commercial investigation: Amentum training / project-team lodging around Montgomery County / DMV federal work — confirm buyer path without paid enrichment. APSA and SAME Foundation need timing + venue evidence before outreach.
`,
);

write(
  "EXA_MISSION_GAP_ANALYSIS.md",
  `# Exa Mission Gap Analysis — Bethesda Live Results

Exa Agent discovery uses shared contract prompts (\`buildExaAgentQuery\` + segment request titles) with hotel context blocks. Different titles alone are insufficient — observed Bethesda results show missions still converging on national calendars.

## Cross-cutting weaknesses

| Weakness | Evidence | Affected segments |
|---|---|---|
| Too event/calendar focused | AEA, AHA, NASPA, SIOP, SMFM dominate processed set | associations, corporate meetings, education |
| Too broad geographically | National orgs without Bethesda-reachable demand | sports (NFL), meetings, associations |
| Weak early corporate indicators | Only Amentum survived as corporate NEW_UNIQUE | corporate_accounts_scaling |
| Weak project/workforce framing | AlphaStruxure / Heven AeroTech EXCLUDE | projects, relocation |
| Rediscovery cost | Rock Creek Mansion duplicate | preferred_lodging |
| Thin contacts | 2 sales-ready / 20 processed | all |
| Low per-cell diversity after fair queue | Many segments contributed 1–2 processed orgs | all |

## Mission-specific notes

1. **associations_conferences_events** — Needs rotation/overflow + Montgomery County / DC-metro secondary hotel logic; suppress primary-city national conventions unless overflow evidence.
2. **corporate_accounts_scaling** — Best relative performance; strengthen HQ moves, office expansions, contract wins in Bethesda/Rockville/NoVA with lodging implications.
3. **corporate_meetings_incentives** — Over-indexes on annual association meetings; prefer company kickoffs/training near Lockheed/NIH/federal campuses.
4. **government_public_sector** — APSA watchlist useful; add federal program offices, NIST, DOE, HHS meeting patterns.
5. **education_scientific** — Baseline already rich in NIH/PDA/biomaterials; mission should hunt **net-new** lab/consortium demand not already published.
6. **sports_entertainment** — NFL/Big Time Hoops unfit; prefer local tournament overflow already partially in baseline, or skip if rediscovery risk high.
7. **local_catering_day_meetings** — Needs chamber, corporate campus, hospital day-meeting sources — AFCEA alone insufficient.
8. **preferred_lodging_partnerships** — Must prefilter against published PLP inventory before paid deep.

## What not to do now

Do not rewrite all missions in one PR. Prefer a focused catchment + pre-deep dedupe + corporate-expansion mission PR next.
`,
);

write(
  "DISCOVERY_EFFECTIVENESS_SCORECARD.md",
  `# Discovery Effectiveness Scorecard — Bethesda Live Exa V1

| Metric | Value |
|---|---|
| Executed segments (live discovery) | **14 / 14** |
| Fresh retrieval cells | 14 |
| Raw candidates extracted | 165 |
| Unique deep-processed candidates | 20 |
| NEW_UNIQUE (evaluation) | ${accepted} |
| EXISTING_ENRICHED | 0 |
| DUPLICATE | ${dupes.length} |
| REJECTED | ${rejected.length} |
| Verified sales-ready contacts (processed) | ${a.contactStats.withSalesReady} |
| Forward-looking watchlist opportunities | ${accepted} (WATCHLIST_RESEARCH) |
| CONFIRMED / RELIABLE_INFERRED promotions | 0 |
| Duplicate rate (processed) | ${((dupes.length / 20) * 100).toFixed(1)}% |
| Exclude/reject rate (processed) | ${((rejected.length / 20) * 100).toFixed(1)}% |
| Actual research spend | **$${a.spentUsd.toFixed(2)}** |
| Hard ceiling | $15.00 |
| Cost per NEW_UNIQUE | **$${costPerAccepted?.toFixed(2)}** |
| Cost per sales-ready NEW_UNIQUE | $3.20 |
| Source quality (live Exa ingestion) | Provider run IDs present on all 14 discovery + ${a.ledger.deepOps} deep ops |
| Certificate | \`${a.verdict}\` |

## Interpretation

Live Exa **works** end-to-end. Commercial yield is still thin: high source/candidate volume does not equal qualified Bethesda demand. Highest leverage is mission catchment + pre-deep dedupe, not more spend.
`,
);

write(
  "PRIORITIZED_DISCOVERY_ENHANCEMENTS.md",
  `# Prioritized Discovery Enhancements — Next Implementation PR

Recommended next focused PR (after this reporting PR merges): **Bethesda/DMV catchment + pre-deep published-inventory dedupe + corporate-expansion mission sharpening**.

## Ranked recommendations

### 1. Pre-deep published inventory dedupe
- **Observed weakness:** Rock Creek Mansion rediscovered and burned deep/discovery budget.
- **Segment:** preferred_lodging (+ general).
- **Evidence:** DUPLICATE ↔ \`gdi_plp_live_fa2063dc1195\`.
- **Root cause:** Paid deep eligibility does not hard-block exact published org matches early enough for all segments.
- **Module:** \`original-full-e2e-research-chain-v1.js\` / paid-deep dedupe + discovery handoff.
- **Improvement:** Skip paid deep when org-key matches CUSTOMER_PUBLISHED inventory unless enrichment intent flagged.
- **Impact:** Lower duplicate spend; more budget for novel orgs.
- **Complexity:** Low–medium · **Tests:** deterministic duplicate skip · **Cost impact:** reduces waste.

### 2. Bethesda catchment negative filters in Exa discovery context
- **Weakness:** NFL, AEA, AHA, NASPA dominate then EXCLUDE.
- **Segments:** associations, meetings, sports, education.
- **Module:** Exa context block builder + segment query specs / agent context.
- **Improvement:** Enforce Montgomery County / Bethesda-reachable constraints; require overflow evidence for national DC primaries.
- **Impact:** Higher precision · **Complexity:** Medium · **Cost:** fewer useless deep seeds.

### 3. Corporate expansion / workforce mission upgrade
- **Weakness:** Only one strong corporate NEW_UNIQUE (Amentum).
- **Segment:** corporate_accounts_scaling (+ relocation).
- **Module:** Exa discovery contract context + segment routing.
- **Improvement:** Explicit office expansion, contract award, training cohort, extended-team lodging signals for Rockville/Bethesda/NoVA.
- **Impact:** More actionable accounts · **Complexity:** Medium.

### 4. Contact relevance gate
- **Weakness:** Sales-ready contacts rare; one attached to EXCLUDE opp.
- **Module:** contact eligibility / commercial spine.
- **Improvement:** Require demand disposition ≥ watchlist before counting sales-ready in scorecards; strengthen entry-point vs DIRECT_BUYER labels.
- **Complexity:** Low.

### 5. Association rotation / medical ecosystem net-new hunt
- **Weakness:** Education/medical missions rediscover national calendars while baseline already holds NIH/PDA-style inventory.
- **Improvement:** Novelty-aware prompts referencing known published org denylist (internal).
- **Complexity:** Medium · **Cost:** reduces rediscovery.

## Explicitly deferred

- Customer-facing opportunity drawer UX
- New orchestrator / provider framework
- Apify / Surfe / PDL / SerpAPI
- Second paid Exa campaign
`,
);

write(
  "TEST_RESULTS.md",
  `# TEST RESULTS — Bethesda Live Exa Discovery Effectiveness V1

## Safety gates (PR #348) — before spend

| Check | Result |
|---|---|
| \`node scripts/test-gdi-full-e2e-canonical-entry-p1c-v1.mjs\` | **20/20 PASS** |
| Live requires budget ceiling + \`authorize-exa-agent-live\` | PASS |
| Live readiness blocked without \`GDI_EXA_AGENT_LIVE=1\` | PASS |
| CLI loads dotenv + Exa live flag | PASS |
| \`npm run dealality:ci-precheck\` | PASS |
| GitHub CI (gitleaks + GDI recovery) | PASS |
| Merged | PR #348 → \`main\` @ \`88b7e6b07\` |

## Phase A preflight (post-merge)

| Check | Result |
|---|---|
| Canonical preflight + assess-live-readiness | LIVE_READY |
| Exa credential present via dotenv | true |
| Medium quote bounded | $0.10 |
| Hard ceiling | $15 |

## Phase B live provider execution

| Check | Result |
|---|---|
| Live command exit code | **0** |
| Discovery fresh cells | 14/14 |
| Paid ledger spend | $3.20 ≤ $15 |
| Provider run IDs recorded | yes (discovery + deep) |
| Certificate | FULL_E2E_EXECUTION_COMPLETE |
| budgetsRespected / outputsIngested | true / true (evidence-based) |
| Publication / Airtable | none / 0 |
| Second paid campaign | **not run** |

## Regression / preservation

| Check | Result |
|---|---|
| Published 67 fingerprint | unchanged \`441773211494…\` |
| Unrelated AVA / wt dirty / stashes | preserved (not committed) |
| Apify orchestration | still not default |

## Reporting package tests

Deterministic analysis artifact: \`analysis-core.json\` generated from live ledger + chain + baseline package (no additional paid calls).
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      outDir,
      spentUsd: a.spentUsd,
      newUnique: accepted,
      verdict: a.verdict,
    },
    null,
    2,
  ),
);
