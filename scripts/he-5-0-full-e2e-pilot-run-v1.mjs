/**
 * HE 5.0 Full E2E eight-hotel pilot runner (free-first, dry-run, STOP at founder gate).
 * Usage: node scripts/he-5-0-full-e2e-pilot-run-v1.mjs
 * Optional: --enable-paid (still refuses unbounded; default off)
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runHe50PilotSupervisor,
  defaultLedgerPath,
  BETHESDA_FIELD_GAP_MATRIX,
  summarizeFieldGapMatrix,
  HE50_TABS,
  PROVIDER_ROUTING,
  APPLY_GATE,
  summarizeReconciliation,
  summarizeCompleteness,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "reports/hotel-explorer/he-5-0-pilot");
fs.mkdirSync(outDir, { recursive: true });

const enablePaid = process.argv.includes("--enable-paid");
const ledgerPath = defaultLedgerPath(root);

const result = runHe50PilotSupervisor({ enablePaid, ledgerPath });

fs.writeFileSync(
  path.join(outDir, "HE50-PILOT-SUPERVISOR-RESULT.json"),
  JSON.stringify(result, null, 2),
  "utf8"
);
fs.writeFileSync(
  path.join(outDir, "HE50-FIELD-GAP-MATRIX.json"),
  JSON.stringify(
    { tabs: HE50_TABS, summary: summarizeFieldGapMatrix(), rows: BETHESDA_FIELD_GAP_MATRIX },
    null,
    2
  ),
  "utf8"
);
fs.writeFileSync(
  path.join(outDir, "HE50-AIRTABLE-DRY-RUN-PACKAGE.json"),
  JSON.stringify(result.dryRun, null, 2),
  "utf8"
);

const recon = summarizeReconciliation();
const completeness = summarizeCompleteness();
fs.writeFileSync(
  path.join(outDir, "HE50-MUTATION-RECONCILIATION-V1.json"),
  JSON.stringify(recon, null, 2),
  "utf8"
);
fs.writeFileSync(
  path.join(outDir, "HE50-COMPLETENESS-MATRIX-V1.json"),
  JSON.stringify(completeness, null, 2),
  "utf8"
);

const gap = result.gapSummary;
const agg = result.aggregate;

const inventoryMd = `# HE 5.0 — Existing Infrastructure Inventory (Phase 0)

**Generated:** ${new Date().toISOString()}  
**PR:** #463 · Branch \`feat/he-beth-phase3-sources-evidence-v1\`  
**Apply:** blocked (\`applyAuthorized: false\`)

## What already works

| Capability | Module / surface | Status |
| --- | --- | --- |
| HPC identity collision + Mullen cohort | \`hpc-population/*\`, \`he:mullen-pilot-hpc-identity-resolution\` | WORKS (read-only) |
| Shared budget reserve/settle | \`research-engine-v1/budget-ledger.js\` + M1 shared ledger | WORKS |
| HI graph contract + upsert planner | \`airtable-graph-*\` | WORKS (dry-run) |
| Bethesda Phase 3 evidence + stakeholder dry-runs | \`reports/hotel-explorer/phase3*\`, \`phase4-quality/*\` | WORKS (staged) |
| Demand Anchors / TI GET APIs | \`he-demand-access-read-v1\`, \`api/hotel-explorer-demand-access\` | WORKS (US sparse honest empty) |
| Guest Sentiment + Market Alerts news | HE-QL-038 surfaces | WORKS (rights-gated / empty news honest) |
| Founder Preview proposed vs live | \`api/hotel-explorer-founder-preview.js\` | WORKS |
| Provider capability matrix | P3A \`provider-capability-matrix.js\` | WORKS (no auto paid) |
| HE 12-tab shell | \`public/js/hotel-explorer.js\` | WORKS |

## Partial

| Capability | Gap |
| --- | --- |
| Bethesda HI graph | Staged dry-runs not applied |
| Year built / renovation on HPC | Verified free; not persisted |
| Seven-hotel HPC CREATE | Identity packets ready; not created |
| US Demand Anchors near Bethesda | Infrastructure connected; few anchors |
| Guest Reputation live | Staged founder-preview only |
| Market Alerts Bethesda news | Genuine absence |

## Missing / out of scope this pilot

- Production Airtable apply (founder gate)
- Automatic Surfe
- Unrestricted paid enrichment
- Per-hotel frontend coding
- Duplicate hotel databases

## Reuse rule

Do **not** recreate HPC population, budget ledgers, or HI graph planners — extend via \`lib/hotel-intelligence/he-5-0-full-e2e/\`.
`;

const mappingMd = `# HE 5.0 — Field-to-Airtable Gap Matrix (Phase 1)

**Hotel seed:** Bethesda Marriott (\`recLuxvwwxID7U2B8\`)  
**Tabs covered:** ${HE50_TABS.length}  
**Fields inventoried:** ${gap.totalFields}

## Classification totals

${Object.entries(gap.byClass)
  .map(([k, v]) => `- **${k}:** ${v}`)
  .join("\n")}

## By tab

| Tab | Fields | Verified live class | Research gaps |
| --- | ---: | ---: | ---: |
${gap.byTab
  .map((t) => `| ${t.tabId} | ${t.fields} | ${t.verified} | ${t.gaps} |`)
  .join("\n")}

## Machine JSON

\`reports/hotel-explorer/he-5-0-pilot/HE50-FIELD-GAP-MATRIX.json\`

## Paid research gate

Do **not** pay to rediscover EXISTING_VERIFIED facts. Paid only for remaining RESEARCH_REQUIRED / high-value UNRESOLVED after free pass, under shared $50 cap.
`;

const providerMd = `# HE 5.0 — Research Provider Routing (Phase 2)

## Preferred order

${PROVIDER_ROUTING.preferred_order.map((p, i) => `${i + 1}. \`${p}\``).join("\n")}

## Prohibited this pilot

${PROVIDER_ROUTING.prohibited_this_pilot.map((p) => `- ${p}`).join("\n")}

## Benchmark stance (this run)

| Provider | Used | Cost | Notes |
| --- | --- | ---: | --- |
| Existing Dealality / Airtable | Yes | $0 | Identity + Bethesda graph reuse |
| Official brand / hotel sites | Yes | $0 | Wave B min identity |
| MD SDAT / public registries | Yes (prior) | $0 | Bethesda PropCo |
| Exa / Parallel paid | **No** | $0 | Deferred — free pass sufficient for pilot STOP; $50 remains available |
| Surfe | **No** | $0 | Click-only / nonpersistent |

**Recommendation:** Keep free-first. Use Exa lite only for ownership/lender gaps if founder authorizes a bounded paid wave within remaining $50. Prefer organization-reusable queries over per-hotel scattershot.
`;

const scorecardRows = result.hotelScorecard
  .map(
    (r) =>
      `| ${r.hotel} | ${r.hpcIdentity} | ${r.newlyVerifiedFacts} | ${r.existingFactsReused} | ${r.proposedAirtableChanges} | $${r.researchSpendUsd.toFixed(2)} | ${r.remainingGaps.slice(0, 3).join("; ")} |`
  )
  .join("\n");

const founderMd = `# HE 5.0 — Eight-Hotel Research Pilot Founder Report

**Status:** ${APPLY_GATE.stopMessage}  
**Run ID:** \`${result.runId}\`  
**PR:** [#463](https://github.com/joandejarden-create/my-operators-backend/pull/463) (OPEN)  
**Paid spend confirmed:** $${agg.confirmedPaidSpendUsd.toFixed(2)} · **Reserved:** $${agg.reservedPaidSpendUsd.toFixed(2)} · **Remaining authorized:** $${agg.remainingBudgetUsd.toFixed(2)} of $50.00  
**Free spend:** $0.00  
**Airtable writes executed:** 0  
**Persistence classification:** \`PERSISTENCE_DESIGNED\` (not FULL_E2E_VALIDATED)

---

## 1. Infrastructure audit

See \`HE50-INFRASTRUCTURE-INVENTORY.md\`.

## 2. Field-to-Airtable mapping

See \`HE50-FIELD-GAP-MATRIX.md\` (${gap.totalFields} Bethesda-seed fields × 12 tabs).

## 3. Provider routing

See \`HE50-PROVIDER-ROUTING.md\`. Paid providers **not invoked** this wave (free-first complete).

## 4–10. Research findings (summary)

### Bethesda (Wave A)

- **Year built:** 1979 (Travel Weekly / CMBS / Cvent) — staged HPC null-fill.
- **Renovation:** SOURCE_CONFLICT — major reno ~2015 vs guestroom unveil press 2017; evidence-only dual dating.
- **Rooms:** 407 reused / corroborated.
- **Ownership:** Legal = Marbeth Acquisition LLC (SDAT); Economic sponsor = Mullen REC (staged MEDIUM); Operator/Brand = Marriott live; current lender / UBO unresolved.
- **CMBS note:** $39.8M loan terms → evidence only (no invented lender edge).
- **Demand / Access:** Infrastructure connected; US anchors sparse — honest empty.
- **Guest Sentiment:** Staged founder-preview; licensing-gated.
- **Market Alerts news:** Genuinely absent for Bethesda.
- **Contacts:** Public identity staged; Surfe not used.

### Seven candidates (Wave B)

| Hotel | Identity | HPC / Census | Rooms (free) | Action |
| --- | --- | --- | --- | --- |
| AC Hotel Newtown Square | NEW_HPC | — | 140 (conflict alt 146) | CREATE proposal |
| Carton House Fairmont | NEW_HPC | — | 169 HIGH | CREATE proposal |
| HGI Newtown Square Radnor | NEW_HPC | — | 138 MEDIUM | CREATE proposal |
| JW Marriott Costa Elena | MATCHED_HPC | \`recq9KWr8UE7z2549\` | 415 HIGH | UPDATE alias/rooms |
| Secrets Cap Cana | MATCHED_HPC | \`recd7XAM6tLjzQhaI\` | reuse HPC | Confirm not Dreams Cap Cana |
| Secrets Papagayo | NEW_HPC_CANDIDATE | — (legacy provenance \`recyX1wLsHBJYRPw7\` only) | 202 HIGH | HPC CREATE — no census runtime id |
| Secrets St. Lucia | NEW_HPC | — | 355 HIGH | CREATE proposal |

**Mullen holdings:** Not auto-verified. All non-Bethesda associations remain \`PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING\` (JW trade press claims flagged VERIFY_BEFORE_GRAPH).

## 11. Dry-run package

\`HE50-AIRTABLE-DRY-RUN-PACKAGE.json\` — ${result.dryRun.mutationCount} mutations · \`applyAuthorized: false\`.

## 12. Cost ledger

\`HE50-RESEARCH-SPEND-LEDGER.json\`

| Metric | Value |
| --- | ---: |
| Confirmed paid | $${agg.confirmedPaidSpendUsd.toFixed(2)} |
| Reserved | $${agg.reservedPaidSpendUsd.toFixed(2)} |
| Remaining of $50 | $${agg.remainingBudgetUsd.toFixed(2)} |
| Newly verified facts (agg) | ${agg.newlyVerifiedFactsTotal} |
| Cost per verified useful field | n/a (paid=$0) |

## 13. Expected post-apply HE completeness

| Hotel | After approved apply (expected) |
| --- | --- |
| Bethesda | Overview + rooms + ownership/contacts/sources substantially populated from staged graph; Demand/News remain thin until anchors/alerts exist |
| Matched HPC (JW, Cap Cana) | Identity + rooms/alias; HE deep-link available |
| Papagayo | After HPC CREATE: deep-link + min identity (legacy census id not canonical) |
| NEW_HPC ×5 (incl. Papagayo) | After CREATE: Founder Preview deep-links + min HPC fields only |

## 14. Automated tests

\`npm run test:he-5-0-full-e2e-pilot-v1\`

## 15. Preview links

${result.hotelScorecard
  .filter((r) => r.founderPreviewUrl)
  .map((r) => `- ${r.hotel}: ${r.founderPreviewUrl}`)
  .join("\n") || "- (none until HPC ids exist)"}

## 16. Required scorecard

| Hotel | HPC identity | Newly verified facts | Existing facts reused | Proposed Airtable changes | Research spend | Remaining gaps |
| --- | --- | ---: | ---: | ---: | ---: | --- |
${scorecardRows}

### Aggregate

- MATCHED_HPC: ${agg.matchedHpc} · MATCHED_HOTEL_CENSUS: ${agg.matchedCensus} · NEW_HPC_CANDIDATE: ${agg.newHpcCandidates}
- Proposed mutations: ${agg.proposedAirtableChangesTotal}
- Paid committed: $${(agg.confirmedPaidSpendUsd + agg.reservedPaidSpendUsd).toFixed(2)}

## Founder decisions required

1. Approve READY mutations only (${recon.counts.READY} READY · ${recon.counts.HOLD} HOLD · ${recon.counts.REJECT} REJECT) — see \`HE50-MUTATION-RECONCILIATION-V1.json\`.
2. Decide whether to spend any of remaining $50 on current-lender / UBO / Full HE gaps (not identity — Cap Cana + AC rooms resolved free).
3. Explicit production Airtable apply authorization (separate from this $50 research auth).
4. Steward review lowercase Cap Cana alias \`recFP9eslejVXbyuR\` (separate from Dreams).
5. Optional: strip HGI rooms on CREATE if HIGH-only keys policy preferred.

---

## ${APPLY_GATE.stopMessage}

Do not interpret successful research as permission to publish or persist.
`;

const reconTable = recon.rows
  .map(
    (r) =>
      `| ${r.op} | ${r.id} | ${r.hotel} | ${r.recommendation} | ${r.conflictStatus} |`
  )
  .join("\n");

const preApplyMd = `# HE 5.0 — Founder Pre-Apply Review Package

**Status:** ${APPLY_GATE.stopMessage}  
**Paid spend:** $0.00 confirmed · $0.00 reserved · **$50.00 remaining**  
**Airtable writes:** 0 · Persistence: \`PERSISTENCE_DESIGNED\` (not FULL_E2E_VALIDATED)

## 1. Conflict resolutions

| Conflict | Disposition |
| --- | --- |
| AC rooms 140 vs 146 | **RESOLVED → 140 HIGH** (opening press 86+50+4; discard OTA 146) |
| Secrets Cap Cana vs Dreams | **VERIFIED distinct** — HPC \`recd7XAM6tLjzQhaI\` Secrets @ 18.4844,-68.3943 vs Dreams \`reclS16CMUS6eJgpC\` |
| Secrets Papagayo | **HPC CREATE** rooms 202 — legacy census \`recyX1wLsHBJYRPw7\` provenance only (not runtime SoT) |
| Bethesda 2015 vs 2017 | **Dual events** — 2015 major reno near-complete; 2017 guestroom unveil press |
| Mullen portfolio | **Proposed listings ≠ verified holdings** (legacy Owner Website signal only) |

## 2. Mutation matrix (${recon.counts.READY} READY / ${recon.counts.HOLD} HOLD / ${recon.counts.REJECT} REJECT)

| # | Op id | Hotel | Rec | Conflict |
| --- | --- | --- | --- | --- |
${reconTable}

**REJECT:** \`M-UPD-secrets_cap_cana\` — identity already correct; no field UPDATE.  
**HOLD:** \`M-BETH-REF-STAKEHOLDER\` — prior package reference, not a standalone write.

## 3. Completeness (Min HPC ≠ Full HE)

See \`HE50-COMPLETENESS-MATRIX-V1.json\`. Only Bethesda approaches Full HE (still staged). Seven others are min-identity / HPC CREATE-or-UPDATE candidates. Legacy Hotel Census is not a runtime dependency (HE-QL-041).

## 4. Expected HE improvement after approved apply

- Bethesda: Year Built 1979 + renovation/CMBS evidence in Sources; prior stakeholder package still separate.
- 5× NEW_HPC (incl. Papagayo CREATE): Founder Preview deep-links with min Overview.
- Cap Cana / JW: identity already usable; JW rooms/alias after UPDATE.

## 5. Remaining Full HE work

Ownership verification, contacts, demand/TI population, Market Alerts discovery, reputation licensing, and browser validation after apply.

Machine packages: \`HE50-MUTATION-RECONCILIATION-V1.json\`, \`HE50-AIRTABLE-DRY-RUN-PACKAGE.json\`, \`HE50-IDENTITY-CONFLICT-PROBE-V1.json\`.
`;

fs.writeFileSync(path.join(outDir, "HE50-INFRASTRUCTURE-INVENTORY.md"), inventoryMd, "utf8");
fs.writeFileSync(path.join(outDir, "HE50-FIELD-GAP-MATRIX.md"), mappingMd, "utf8");
fs.writeFileSync(path.join(outDir, "HE50-PROVIDER-ROUTING.md"), providerMd, "utf8");
fs.writeFileSync(path.join(outDir, "HE50-FOUNDER-COMPLETION-REPORT.md"), founderMd, "utf8");
fs.writeFileSync(path.join(outDir, "HE50-FOUNDER-PRE-APPLY-REVIEW-V1.md"), preApplyMd, "utf8");

console.log(JSON.stringify({
  ok: true,
  stop: APPLY_GATE.stopMessage,
  outDir,
  confirmedPaidUsd: agg.confirmedPaidSpendUsd,
  remainingUsd: agg.remainingBudgetUsd,
  mutations: result.dryRun.mutationCount,
  reconciliation: recon.counts,
  hotels: agg.hotels,
}, null, 2));
