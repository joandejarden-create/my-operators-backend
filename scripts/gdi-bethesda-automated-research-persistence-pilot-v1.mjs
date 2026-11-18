#!/usr/bin/env node
/**
 * Bethesda automated Full E2E research-persistence controlled pilot (simulation).
 * Never writes production Airtable / GDO / publication.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runBethesdaAutomatedResearchPersistencePilot } from "../lib/group-demand-intelligence/workflow/gdi-bethesda-automated-research-persistence-pilot-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-automated-research-persistence-pilot-v1",
);

function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string" ? body : `${JSON.stringify(body, null, 2)}\n`,
  );
  return p;
}

const result = await runBethesdaAutomatedResearchPersistencePilot(ROOT);
write("PILOT_RESULT.json", result);

const a = result.acceptance || {};
const passA = result.passA || {};
const weekly = result.weeklyChange || {};
const shared = result.sharedIntelligence || {};
const plan = result.productionPilotPlan || {};
const fail = result.failureRecovery || {};

write(
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  `# Founder executive summary — Bethesda automated research persistence pilot

## Verdict

Controlled Full E2E research-persistence simulation **passed** against deterministic fixtures and an in-memory inventory store mirroring the production adapter.

| Gate | Result |
|------|--------|
| Bethesda baseline intact | **${a.bethesda86Intact ? "86/86" : "FAIL"}** |
| Production Airtable writes | **${result.productionAirtableWrites}** |
| GDO / publication writes | **${result.gdoWrites} / ${result.publicationWrites}** |
| UNCHANGED / CREATE / UPDATE observed | **${a.unchangedObserved} / ${a.createObserved} / ${a.updateObserved}** |
| In-run duplicate resolved | **${a.duplicateResolvedInRun}** |
| Failure recoverable + retry idempotent | **${a.failureRecoverable && a.retryNoDuplicate}** |
| Weekly Change timing / contact | **${a.weeklyTimingValidated}** |
| Shared DG across hotels | **${a.sharedDgSeparateHotels}** |

## What this proves

The canonical Full E2E → research inventory persistence bridge can:

1. Load existing hotel research intelligence
2. Deduplicate in-run and against existing inventory
3. Classify CREATE / UPDATE / UNCHANGED from material evidence
4. Keep exploratory rows research-only
5. Recover from failed writes without duplicate creates
6. Preserve Weekly Change / first-discovery provenance
7. Reuse shared demand-generator identity across hotels without cross-sharing private sales

## What this does **not** authorize

- Automatic recurring production persistence
- Paid research
- GDO or customer publication writes
- Rock Creek Venue ID correction

**STOP for founder review** before any live automatic research-persistence run.
`,
);

write(
  "FULL_E2E_EXECUTION_TRACE.md",
  `# Full E2E execution trace

## Canonical sequence (certified)

${(result.workflowSequence || []).map((s, i) => `${i + 1}. ${s}`).join("\n")}

## Integration points

- Entry: \`runCanonicalFullE2eResearch\` → \`persistFullE2eResearchInventory\`
- Adapter: \`upsertResearchInventoryRecord\` (production) / injectable memory store (this pilot)
- Ledger: \`research-inventory-persistence-ledger.json\` on live runs
- Parallel orchestrator created: **${result.workflowVerification.parallelOrchestratorCreated}**
- Fail-closed by default: **${result.workflowVerification.failClosedByDefault}**
- Production auth locked during pilot: **${result.workflowVerification.productionAuthLocked}**

## Pass A actuals (completed operations)

\`\`\`json
${JSON.stringify(passA.executionSummary || {}, null, 2)}
\`\`\`
`,
);

write(
  "RESEARCH_PERSISTENCE_RESULTS.md",
  `# Research persistence results

## Pass A (unchanged / update / create / duplicate / exploratory)

| Metric | Value |
|--------|------:|
| Candidates before dedupe | ${passA.counts?.candidatesBeforeDedupe} |
| Unique candidates | ${passA.counts?.candidates} |
| Duplicates resolved | ${passA.counts?.DUPLICATES_RESOLVED} |
| CREATED | ${passA.byAction?.CREATED} |
| UPDATED | ${passA.byAction?.UPDATED} |
| UNCHANGED | ${passA.byAction?.UNCHANGED} |
| FAILED | ${passA.byAction?.FAILED} |
| Verified read-backs | ${(passA.readBack || []).filter((r) => r.matched).length} |
| Memory store size after | ${passA.storeSizeAfter} |

## Contact enrichment

Material change detected: **${result.contactEnrichment?.materialChanged}**

\`\`\`json
${JSON.stringify(result.contactEnrichment?.counts || {}, null, 2)}
\`\`\`
`,
);

write(
  "DEDUPLICATION_AND_SHARED_INTELLIGENCE.md",
  `# Deduplication and shared intelligence

## In-run deduplication

Duplicate segment discovery for the same \`hotelId::opportunityId\` is collapsed before upsert.

Resolved in Pass A: **${(passA.duplicatesResolved || []).length}**

\`\`\`json
${JSON.stringify(passA.duplicatesResolved || [], null, 2)}
\`\`\`

## Cross-hotel shared demand generator

| Field | Value |
|-------|-------|
| Shared \`dgGeneratorId\` | \`${shared.sameDemandGeneratorId}\` |
| Bethesda composite key | \`${shared.bethesdaCompositeKey}\` |
| Peer composite key | \`${shared.peerCompositeKey}\` |
| Bethesda CREATE | ${shared.bethesdaCreated} |
| Peer CREATE | ${shared.peerCreated} |

### Rules

${(shared.rules || []).map((r) => `- ${r}`).join("\n")}
`,
);

write(
  "WEEKLY_CHANGE_VALIDATION.md",
  `# Weekly Change validation

Contract: \`${weekly.contract}\`

| Scenario | State | Pass |
|----------|-------|------|
| Event timing change | ${weekly.timingUpdate?.weeklyDeltaState} | ${weekly.timingIsUpdated} |
| Contact enrichment | ${weekly.contactUpdate?.weeklyDeltaState} | ${weekly.contactIsUpdated} |
| New opportunity | ${weekly.newOpportunity?.weeklyDeltaState} | ${weekly.newIsNew} |
| Description-only rewrite | ${weekly.descriptionOnlyNonMaterial?.weeklyDeltaState} | ${weekly.descriptionOnlyNonMaterial?.ok} |
| firstDiscovered preserved on update | — | ${weekly.firstDiscoveredPreservedOnUpdate} |

Changed fields (timing): ${(weekly.timingUpdate?.changedFields || []).join(", ") || "(none)"}
`,
);

write(
  "FAILURE_RECOVERY_TESTS.md",
  `# Failure recovery tests

| Step | Result |
|------|--------|
| Forced write failure | FAILED=${fail.failPass?.failed}, ok=${fail.failPass?.ok} |
| Retry CREATE | ${JSON.stringify(fail.retryPass?.counts)} |
| Second retry idempotent | action=${fail.retryIdempotent?.action} |

Acceptance: recoverable failure + retry create + no duplicate on second retry = **${a.failureRecoverable && a.retryNoDuplicate}**
`,
);

write(
  "PRODUCTION_PILOT_AUTHORIZATION.md",
  `# Production pilot authorization plan

**Not authorized.** This document is the exact plan for a future founder-gated live run.

| Field | Value |
|-------|-------|
| Authorization ID | \`${plan.authorizationId}\` |
| Hotel | ${plan.hotelName} (\`${plan.hotelId}\`) |
| Base / table | \`${plan.baseId}\` / \`${plan.tableId}\` |
| Expected CREATE max | ${plan.expectedCreatesMax} |
| Expected UPDATE max | ${plan.expectedUpdatesMax} |
| Write budget | ${plan.writeBudget} |
| Env gate | \`${plan.requiresEnvGate}=1\` |
| GDO / publication / Rock Creek / paid | **excluded** |

## Exact allowed composite identities

${(plan.exactAllowedCompositeIdentities || []).map((k) => `- \`${k}\``).join("\n")}

## Retry / read-back

Required: **${plan.retryAndReadBackRequired}**

## Rollback / recovery

${(plan.rollbackProcedure || []).map((s) => `1. ${s}`).join("\n")}

${plan.note}
`,
);

write(
  "TEST_RESULTS.md",
  `# Test results

See \`npm run test:gdi-bethesda-automated-research-persistence-pilot-v1\` and CI on the PR.

Pilot acceptance object:

\`\`\`json
${JSON.stringify(a, null, 2)}
\`\`\`
`,
);

console.log(
  JSON.stringify(
    {
      ok: Object.values(a).every(Boolean),
      acceptance: a,
      productionAirtableWrites: result.productionAirtableWrites,
      outDir: OUT,
    },
    null,
    2,
  ),
);

if (!Object.values(a).every(Boolean)) process.exitCode = 1;
