#!/usr/bin/env node
/**
 * All-hotel complete Airtable research persistence completion V1.
 * Dry-run / inventory / design only. No bulk production writes. No schema creates.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  GDI_COMPLETE_RESEARCH_PERSISTENCE_V1,
  COMPLETE_PERSISTENCE_CONTRACT,
  PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES,
  RESEARCH_LIFECYCLE_LAYER,
  discoverLocalHotelUniverse,
  loadLocalOpportunities,
  publicationState,
  planHotelCompletePersistence,
  assertNoBulkProductionApply,
  loadYotelForbiddenRecreateIds,
} from "../lib/group-demand-intelligence/workflow/gdi-complete-research-persistence-v1.js";
import {
  REFERENCE_HOTELS_V2,
  ROCK_CREEK_VENUE_IDENTITY,
} from "../lib/group-demand-intelligence/workflow/gdi-multi-hotel-airtable-reconciliation-v2.js";
import { YOTEL_LEGACY_DELETE_AUTH } from "../lib/group-demand-intelligence/workflow/gdi-yotel-legacy-gdo-deletion-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-all-hotel-airtable-persistence-completion-v1",
);

assertNoBulkProductionApply({
  apply: process.argv.includes("--apply"),
  authorizeProductionWrites: process.argv.includes("--authorize-production-writes"),
  createSchema: process.argv.includes("--create-schema"),
});

function write(name, content) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof content === "string" ? content : `${JSON.stringify(content, null, 2)}\n`,
  );
  return p;
}

function resolveHotelName(hotelId) {
  const ref = REFERENCE_HOTELS_V2.find((h) => h.hotelId === hotelId);
  if (ref?.name) return ref.name;
  const cfgPath = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${hotelId}.json`,
  );
  if (fs.existsSync(cfgPath)) {
    try {
      const j = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
      return j.hotelName || j.name || j.displayName || hotelId;
    } catch {
      /* ignore */
    }
  }
  return hotelId;
}

async function loadAirtable(hotelId) {
  try {
    const rows = await listOpportunitiesForHotel(hotelId);
    return { ok: true, rows: rows || [], error: null };
  } catch (err) {
    return {
      ok: false,
      rows: [],
      error: String(err?.code || err?.message || err),
    };
  }
}

function loadGitStatusSnapshot() {
  return {
    workspace: ROOT,
    expectedBranch: "feat/gdi-all-hotel-airtable-persistence-completion-v1",
    expectedHeadParent: "2fd417f5c374748eb272f4e0fb3efe10d905dc92",
    primaryCheckout: "C:\\Dev\\dealality\\my-operators-backend",
    adpIsolation: "C:\\Dev\\dealality\\_adp-atomic-delivery-deploy-wt (untouched)",
    note: "See GIT_WORKTREE_AND_BRANCH_STATUS.md for full diagnostic.",
  };
}

function buildMatrixMd(rows) {
  const lines = [
    "# All-hotel GDI inventory matrix",
    "",
    "Canonical local research = `data/group-demand-intelligence/hotels/{hotelId}/opportunities.json`.",
    "",
    "| Hotel ID | Name | FS opps | AT GDOs | Overlap | AT ok | Research pending schema | GDO create | GDO preserve | Conflicts | AT-only | Forbidden | Pub CURRENT |",
    "|----------|------|---------|---------|---------|-------|-------------------------|------------|--------------|-----------|---------|-----------|-------------|",
  ];
  for (const r of rows) {
    lines.push(
      `| \`${r.hotelId}\` | ${r.hotelName} | ${r.filesystemCount} | ${r.airtableGdoCount} | ${r.idOverlapCount ?? 0} | ${r.airtableOk ? "yes" : r.airtableError || "no"} | ${r.counts.researchInventoryPendingSchema} | ${r.counts.qualifiedGdoCreate} | ${r.counts.qualifiedGdoUnchanged} | ${r.counts.conflicts} | ${r.counts.airtableOnly} | ${r.counts.forbiddenBlocked} | ${r.publication?.current ? "yes" : "no"} |`,
    );
  }
  lines.push("");
  return lines.join("\n");
}

function buildHotelByHotelMd(plans) {
  const parts = ["# Hotel-by-hotel reconciliation", ""];
  for (const p of plans) {
    parts.push(`## ${p.hotelName} (\`${p.hotelId}\`)`);
    parts.push("");
    parts.push(`- Local research opportunities: **${p.filesystemCount}**`);
    parts.push(`- Airtable GDO rows: **${p.airtableGdoCount}** (${p.airtableOk ? "live read ok" : `read error: ${p.airtableError}`})`);
    parts.push(`- Research inventory pending schema: **${p.counts.researchInventoryPendingSchema}**`);
    parts.push(`- Qualified GDO CREATE candidates: **${p.counts.qualifiedGdoCreate}**`);
    parts.push(`- Qualified GDO unchanged: **${p.counts.qualifiedGdoUnchanged}**`);
    parts.push(`- Identity conflicts: **${p.counts.conflicts}**`);
    parts.push(`- Airtable-only GDOs: **${p.counts.airtableOnly}**`);
    if (p.counts.forbiddenBlocked) {
      parts.push(`- Forbidden YOTEL legacy recreate blocks: **${p.counts.forbiddenBlocked}**`);
    }
    if (p.conflicts?.length) {
      parts.push("");
      parts.push("Conflicts (quarantined — no auto-overwrite):");
      for (const c of p.conflicts.slice(0, 10)) {
        parts.push(
          `- \`${c.opportunityId}\`: ${c.reason} FS=\`${c.filesystem}\` AT=\`${c.airtable}\``,
        );
      }
      if (p.conflicts.length > 10) {
        parts.push(`- … +${p.conflicts.length - 10} more`);
      }
    }
    parts.push("");
  }
  return parts.join("\n");
}

async function main() {
  const forbidden = loadYotelForbiddenRecreateIds(ROOT);
  const universe = discoverLocalHotelUniverse(ROOT);
  const withResearch = universe.filter((h) => {
    const fsOpp = loadLocalOpportunities(ROOT, h.hotelId);
    return fsOpp.present && fsOpp.count > 0;
  });

  // Prefer hotels with opportunities.json; include REFERENCE hotels even if empty FS
  const hotelIds = new Set([
    ...withResearch.map((h) => h.hotelId),
    ...REFERENCE_HOTELS_V2.map((h) => h.hotelId),
  ]);
  // Drop provisional alias dirs that duplicate canonical rec* inventories
  const aliasDupes = new Set([
    "gdi_hotel_renaissance_times_square", // → recG66DQJKP2c0UNh
    "gdi_hotel_waterstone_boca_raton", // → recgMYovrrZDJMqzX
  ]);

  const plans = [];
  const researchDryRun = { version: GDI_COMPLETE_RESEARCH_PERSISTENCE_V1, hotels: [] };
  const gdoDryRun = { version: GDI_COMPLETE_RESEARCH_PERSISTENCE_V1, hotels: [] };

  for (const hotelId of [...hotelIds].sort()) {
    if (aliasDupes.has(hotelId)) continue;
    const hotelName = resolveHotelName(hotelId);
    const filesystem = loadLocalOpportunities(ROOT, hotelId);
    const pub = publicationState(ROOT, hotelId);
    const at = await loadAirtable(hotelId);
    const plan = planHotelCompletePersistence({
      hotel: { hotelId, hotelName },
      filesystem,
      airtableGdos: at.rows,
      forbiddenRecreateIds:
        hotelId === YOTEL_LEGACY_DELETE_AUTH.hotelId ? forbidden : new Set(),
    });
    const row = {
      hotelId,
      hotelName,
      filesystemCount: filesystem.count,
      airtableGdoCount: at.rows.length,
      idOverlapCount: plan.idOverlapCount,
      airtableOk: at.ok,
      airtableError: at.error,
      counts: plan.counts,
      publication: pub,
      conflicts: plan.conflicts,
      airtableOnly: plan.airtableOnly,
      researchCreates: plan.researchCreates,
      gdoCreates: plan.gdoCreates,
      gdoUnchanged: plan.gdoUnchanged,
      forbiddenBlocked: plan.forbiddenBlocked,
    };
    plans.push(row);
    researchDryRun.hotels.push({
      hotelId,
      hotelName,
      destination: PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES.name,
      schemaStatus: PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES.status,
      creates: plan.researchCreates,
      count: plan.researchCreates.length,
    });
    gdoDryRun.hotels.push({
      hotelId,
      hotelName,
      destination: COMPLETE_PERSISTENCE_CONTRACT.qualifiedGdoDestination.name,
      creates: plan.gdoCreates,
      unchanged: plan.gdoUnchanged,
      conflicts: plan.conflicts,
      createCount: plan.gdoCreates.length,
      unchangedCount: plan.gdoUnchanged.length,
    });
  }

  const totals = plans.reduce(
    (acc, p) => {
      acc.hotels += 1;
      acc.filesystemOpps += p.filesystemCount;
      acc.airtableGdos += p.airtableGdoCount;
      acc.researchPendingSchema += p.counts.researchInventoryPendingSchema;
      acc.qualifiedGdoCreate += p.counts.qualifiedGdoCreate;
      acc.qualifiedGdoUnchanged += p.counts.qualifiedGdoUnchanged;
      acc.idOverlap += p.idOverlapCount || 0;
      acc.commerciallyQualifiedFs += p.counts.commerciallyQualifiedFs || 0;
      acc.conflicts += p.counts.conflicts;
      acc.airtableOnly += p.counts.airtableOnly;
      acc.forbiddenBlocked += p.counts.forbiddenBlocked;
      return acc;
    },
    {
      hotels: 0,
      filesystemOpps: 0,
      airtableGdos: 0,
      researchPendingSchema: 0,
      qualifiedGdoCreate: 0,
      qualifiedGdoUnchanged: 0,
      idOverlap: 0,
      commerciallyQualifiedFs: 0,
      conflicts: 0,
      airtableOnly: 0,
      forbiddenBlocked: 0,
    },
  );

  researchDryRun.totals = {
    hotels: totals.hotels,
    researchCreatesPendingSchema: totals.researchPendingSchema,
  };
  gdoDryRun.totals = {
    hotels: totals.hotels,
    create: totals.qualifiedGdoCreate,
    unchanged: totals.qualifiedGdoUnchanged,
    conflicts: totals.conflicts,
  };

  // --- Write deliverables ---
  write("RESEARCH_PERSISTENCE_DRY_RUN.json", researchDryRun);
  write("QUALIFIED_GDO_PERSISTENCE_DRY_RUN.json", gdoDryRun);
  write("ALL_HOTEL_INVENTORY_MATRIX.md", buildMatrixMd(plans));
  write("HOTEL_BY_HOTEL_RECONCILIATION.md", buildHotelByHotelMd(plans));
  write("AGGREGATE_COUNTS.json", { totals, generatedAt: new Date().toISOString() });

  write(
    "GIT_WORKTREE_AND_BRANCH_STATUS.md",
    `# Git worktree and branch status

## Cursor workspace

- Path: \`${ROOT}\`
- Branch: \`feat/gdi-all-hotel-airtable-persistence-completion-v1\`
- HEAD base: \`2fd417f5c\` (PR #368 merge on \`origin/main\`)

## Primary checkout

- Path: \`C:\\Dev\\dealality\\my-operators-backend\`
- Branch: \`main\` @ \`2fd417f5c\` tracking \`origin/main\`
- Synchronized with merge of PR #368

## Changes inflation (~144k)

Not unmerged feature-branch source. Cursor Changes inflate from **untracked research artifacts** under this isolation worktree (Full E2E state dirs, association-events runs, scaling runner reports, tmp gitleaks). Short \`git status\` shows ~11–12 dirty/untracked paths, not 144k edited source lines.

Preserved WIP (do not discard):

- Modified: \`data/.../recUOyzOXn2Zdp98I/pursuits.json\`, Bethesda reconciliation JSON
- Untracked research state dirs for Radisson / Bethesda / YOTEL
- Untracked report dirs from prior research runs

## Completed feature branch

- \`feat/gdi-multi-hotel-airtable-reconciliation-v2\` work was squash-merged via PR #368
- Local retirement of that branch is safe when clean; remote tip may linger until pruned
- This isolation worktree remains required for active persistence-completion work + preserved WIP

## ADP

- \`C:\\Dev\\dealality\\_adp-atomic-delivery-deploy-wt\` remains isolated / untouched

## Safety

- No \`git clean\`, \`reset --hard\`, or forced WIP moves
- Obsolete clean deploy worktrees exist elsewhere; retirement deferred unless founder authorizes cleanup of confirmed-unused trees
`,
  );

  write(
    "AIRTABLE_COMPLETE_RESEARCH_STORAGE_DESIGN.md",
    `# Airtable complete research storage design

## Three distinct lifecycle layers

| Layer | Purpose | Destination |
|-------|---------|-------------|
| **A. Research inventory** | All valid non-duplicate research opportunities (exploratory, watchlist, continuation) | Proposed: **GDI Hotel Research Opportunities** (schema gate) |
| **B. Qualified GDO registry** | Commercially eligible hotel opportunities (demand + contact standards) | Existing: **Group Demand Opportunities** (\`tblRuReslJMwsfRQj\`) |
| **C. Customer publication** | Founder-authorized customer-visible packages | FS: \`bundled-publication-packages/{hotelId}/CURRENT.json\` |

These layers must never collapse into a single ambiguous status.

## Existing tables — correct uses

- **Demand Generators / Programs / Signals** — global identity authority; reuse; no hotel-private sales activity
- **Group Demand Opportunities** — qualified commercial registry only
- **Private Event Venues / Signals / Hotel Venue Fit** — venue identity + hotel fit
- **GDI Research Targets / Runs / Target Runs** — monitoring coverage & run provenance — **not** an opportunity registry
- **Hotel Commercial Profiles / Need Periods** — hotel commercial profile — **not** opportunity storage

## Gap

Existing Airtable model **cannot** durably retain research-only hotel opportunities without either:

1. Misusing GDO (forces unqualified research into commercial rows — **rejected**), or
2. Misusing Research Targets / Runs as opportunity registries — **rejected**, or
3. Adding a minimal **GDI Hotel Research Opportunities** table — **recommended**

Until schema approval, filesystem \`opportunities.json\` remains the operational research SoT while dry-run manifests enumerate every research row that must land in Airtable after approval.

## Proposed table (not created)

\`\`\`
${JSON.stringify(PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES, null, 2)}
\`\`\`

## Contract constants

Base: \`${COMPLETE_PERSISTENCE_CONTRACT.base.baseId}\` (${COMPLETE_PERSISTENCE_CONTRACT.base.name})
`,
  );

  write(
    "EXISTING_SCHEMA_REUSE_VS_MINIMAL_ADDITIONS.md",
    `# Existing schema reuse vs minimal additions

## Reuse as-is (no schema change)

| Asset | Role |
|-------|------|
| Group Demand Opportunities | Layer B — qualified GDO only |
| Demand Generators | Global org identity |
| Demand Programs / Signals | Program & signal authority |
| Private Event Venues (+ signals, hotel venue fit) | Venue identity |
| Hotel Demand Generator Fit | Hotel↔DG commercial fit when qualified |
| GDI Research Runs | Run provenance summaries (optional) |
| Commercial payload durability (PR #366) | Preserve thesis / contacts / evidence on GDO writes |

## Minimal addition (founder schema approval required)

| Asset | Why |
|-------|-----|
| **GDI Hotel Research Opportunities** | Layer A durable Airtable mirror for research-only / watchlist / exploratory hotel opportunities without implying GDO qualification |

## Explicitly rejected misuses

- Forcing research-only rows into GDO to match counts
- Research Targets as opportunity catch-all
- Research Runs as opportunity registry
- Hotel commercial profiles / need-period tables as opportunity store
- Creating a second competing GDI platform

## Aggregate dry-run (this run)

- Hotels inventoried: **${totals.hotels}**
- Local research opportunities: **${totals.filesystemOpps}**
- Research rows pending schema table: **${totals.researchPendingSchema}**
- Qualified GDO CREATE candidates: **${totals.qualifiedGdoCreate}**
- Qualified GDO already present / unchanged: **${totals.qualifiedGdoUnchanged}**
- Conflicts quarantined: **${totals.conflicts}**
`,
  );

  write(
    "RESEARCH_AND_GDO_SEPARATION_CONTRACT.md",
    `# Research and GDO separation contract

## Rules

1. **Research inventory** persists first (or independently) of commercial promotion.
2. **Qualified GDO** requires Confirmed/Reliable demand thesis **and** credible commercial entry point (existing Full E2E eligibility).
3. **Customer publication** remains independently founder-gated; persistence ≠ publication.
4. Research-only opportunities must survive future runs and become GDO-eligible later without losing earlier source research.
5. Idempotent identity: same \`opportunityId\` never forks into a new opportunity on rerun.
6. YOTEL legacy 108 GDO IDs must never be recreated (\`${forbidden.size}\` IDs loaded from delete manifest).
7. Hotel-private pursuits/feedback stay hotel-scoped; never copy into global Demand Generators.

## Classification API

\`classifyOpportunityLifecycleLayer(opp, hotel)\` → \`${Object.values(RESEARCH_LIFECYCLE_LAYER).join(" | ")}\`

## Status vocabulary (do not collapse)

| Status family | Examples |
|---------------|----------|
| Research lifecycle | exploratory, watchlist, continuation, research-only |
| Commercial qualification | RESEARCH_ONLY, GLOBAL_ORG_ONLY, CUSTOMER_ELIGIBLE_CANDIDATE |
| Customer publication | STAGED, CURRENT, unpublished |
`,
  );

  write(
    "GLOBAL_INTELLIGENCE_REUSE_CERTIFICATION.md",
    `# Global intelligence reuse certification

- Demand Generator / Program / Signal identity remains global and reusable across hotels.
- Hotel-specific opportunity rows and pursuits remain hotel-scoped.
- Cross-hotel shared org reuse is allowed; hotel-private commercial payload is not.
- Rock Creek venue identity correction is **not** authorized by this prompt:
  - Record \`${ROCK_CREEK_VENUE_IDENTITY.airtableRecordId}\`
  - Field: Venue ID → \`${ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId}\`
  - Submit on next explicit write allowlist only.
`,
  );

  write(
    "YOTEL_NO_LEGACY_RECREATION.md",
    `# YOTEL — no legacy 108 recreation

- Hotel: YOTEL Geneva Lake (\`${YOTEL_LEGACY_DELETE_AUTH.hotelId}\`)
- Legacy GDO deletions completed under prior authorization; live remaining expected **0**
- Forbidden recreate ID set size loaded from delete manifest: **${forbidden.size}**
- Current local research baseline: **42** opportunities (must land in research inventory after schema approval)
- Only independently qualified opportunities may enter GDO
- Dry-run planner blocks any attempt to recreate forbidden IDs
`,
  );

  write(
    "WEEKLY_CHANGE_AND_HISTORY_CERTIFICATION.md",
    `# Weekly Change and history certification

- Persistence must preserve \`firstDiscoveredAt\` / \`firstDiscoveredRunId\` / \`lastMaterialChangeAt\`
- No-change reruns must not invent new opportunity IDs
- Weekly Change remains deterministic from durable history fields
- Research inventory layer must carry the same discovery/history metadata as FS SoT
- GDO updates must use commercial payload preservation (PR #366) — no silent compacting of thesis/evidence/contacts
`,
  );

  write(
    "FULL_E2E_PERSISTENCE_INTEGRATION.md",
    `# Full E2E persistence integration

## Target pipeline

Discovery → Screening → Research → Identity Resolution → **Research Persistence** → Commercial Qualification → **GDO Persistence** → Read-Back → Weekly Change → Controlled Publication

## Adapter posture

- Extend existing Full E2E persistence adapter / eligibility — do not build a parallel engine
- Research persistence writes target proposed research table once schema-approved
- GDO writes remain gated by commercial eligibility
- Customer publication remains outside automatic persistence
- Production mutations remain fail-closed until exact allowlist authorization

## This package

- Implements classification + all-hotel dry-run manifests
- Does **not** enable bulk apply
- Schema create blocked (\`SCHEMA_CHANGES_REQUIRE_FOUNDER_APPROVAL\`)
`,
  );

  const batchPlan = {
    version: GDI_COMPLETE_RESEARCH_PERSISTENCE_V1,
    status: "AWAITING_FOUNDER_BATCH_AUTHORIZATION",
    prerequisites: [
      "Approve or reject proposed GDI Hotel Research Opportunities table",
      "If approved, create table with required fields in base appa2cE7FTRmIbB32",
      "Authorize research inventory backfill batches by hotel",
      "Separately authorize qualified GDO create batches (non-empty create sets only)",
      "Rock Creek Venue ID one-field fix on separate allowlist",
    ],
    hotels: plans.map((p) => ({
      hotelId: p.hotelId,
      hotelName: p.hotelName,
      researchInventoryCreates: p.counts.researchInventoryPendingSchema,
      qualifiedGdoCreates: p.counts.qualifiedGdoCreate,
      conflictsDeferred: p.counts.conflicts,
      recommendedBatchSize: 25,
      rateLimit: "Airtable 5 rps soft; backoff on 429",
    })),
    totals,
    rockCreekSeparateAllowlist: COMPLETE_PERSISTENCE_CONTRACT.rockCreekNextAllowlist,
  };
  write("PRODUCTION_BATCH_MIGRATION_PLAN.md", `# Production batch migration plan

**STOP — no bulk writes authorized by this package.**

## Prerequisites (founder)

1. Schema decision on **GDI Hotel Research Opportunities**
2. Exact per-hotel create/update allowlists
3. Confirm YOTEL legacy IDs remain blocked
4. Confirm customer publication packages untouched
5. Backup/recovery readiness (see ROLLBACK_AND_RECOVERY.md)

## Proposed batches

See machine-readable section below. Default batch size **25**. Research inventory and qualified GDO are **separate** authorization streams.

\`\`\`json
${JSON.stringify(batchPlan, null, 2)}
\`\`\`
`);

  write(
    "ROLLBACK_AND_RECOVERY.md",
    `# Rollback and recovery

- Research inventory creates (future): delete by \`opportunityId\` allowlist or restore from FS \`opportunities.json\` SoT
- Qualified GDO creates: delete only exact new record IDs from apply ledger; never broad hotel wipe
- YOTEL: off-git backup of deleted 108 remains under \`C:\\Dev\\dealality\\_secure-backups\\\` — do not recreate
- GDO updates: reverse from pre-write field snapshots in apply ledger
- Publication packages: never mutated by this persistence path
- Schema: if table created then abandoned, leave empty or archive; do not migrate research into GDO as cleanup
`,
  );

  write(
    "FOUNDER_APPROVALS_REQUIRED.md",
    `# Founder approvals required

| Gate | Status | Notes |
|------|--------|-------|
| Schema: create GDI Hotel Research Opportunities | **REQUIRED** | Blocks Airtable research-layer durability |
| Research inventory backfill (all hotels) | **REQUIRED after schema** | Exact hotel batches in PRODUCTION_BATCH_MIGRATION_PLAN |
| Qualified GDO creates (non-zero hotels) | **REQUIRED** | Separate from research inventory |
| Rock Creek Venue ID field correction | **REQUIRED (separate allowlist)** | \`recf35Q9B6jpX3Ivm\` → \`pev_53b689545914754d\` |
| Customer publication changes | **NOT requested** | Remains independently gated |
| Bulk production apply in this PR | **DENIED** | Fail-closed |

Prior Bethesda / YOTEL authorizations do **not** grant unrestricted write permission.
`,
  );

  write(
    "FOUNDER_EXECUTIVE_SUMMARY.md",
    `# Founder executive summary — all-hotel Airtable research persistence completion V1

**STOP FOR FOUNDER REVIEW — NO BULK PRODUCTION WRITES · NO SCHEMA CREATES**

## Verdict

The complete local research inventory across **${totals.hotels}** active GDI hotels (**${totals.filesystemOpps}** opportunities) is inventoried and classified into Research Inventory vs Qualified GDO vs Customer Publication.

**Durable Airtable storage for research-only opportunities requires a minimal new table** (\`GDI Hotel Research Opportunities\`). Forcing those rows into GDO would violate commercial qualification. Misusing Research Targets/Runs is rejected.

Until schema approval, FS \`opportunities.json\` remains the operational research SoT; this package provides exact dry-run manifests for every hotel.

## Aggregate counts

| Metric | Count |
|--------|-------|
| Hotels with research inventory | ${totals.hotels} |
| Local research opportunities | ${totals.filesystemOpps} |
| Live Airtable GDO rows (sum) | ${totals.airtableGdos} |
| Research → pending schema persist | ${totals.researchPendingSchema} |
| Qualified GDO CREATE candidates | ${totals.qualifiedGdoCreate} |
| Qualified GDO already present | ${totals.qualifiedGdoUnchanged} |
| Conflicts quarantined | ${totals.conflicts} |
| Airtable-only GDOs | ${totals.airtableOnly} |
| YOTEL legacy recreate blocks | ${totals.forbiddenBlocked} |

## Classification key

- **Already persisted (GDO):** unchanged qualified overlaps
- **Eligible to persist (GDO):** create candidates meeting commercial eligibility
- **Needs schema change:** research-only / watchlist inventory → proposed table
- **Deferred:** identity conflicts (e.g. org-name mismatches)
- **Research-only:** not GDO-eligible
- **Customer-published:** CURRENT packages only (unchanged by this work)

## YOTEL

Legacy 108 not recreated. Local 42 remain research baseline for inventory table after approval; only independently qualified subset may enter GDO.

## Git

Primary \`main\` @ \`2fd417f5c\` synchronized. Isolation worktree on focused completion branch. ADP untouched. Large Changes count = untracked research artifacts, not disposable source edits — WIP preserved.

## Next founder actions

1. Approve/reject proposed research inventory table schema
2. Authorize bounded migration batches from PRODUCTION_BATCH_MIGRATION_PLAN.md
3. Optionally authorize Rock Creek one-field Venue ID fix on separate allowlist
`,
  );

  write("INVENTORY_PLANS.json", {
    version: GDI_COMPLETE_RESEARCH_PERSISTENCE_V1,
    git: loadGitStatusSnapshot(),
    totals,
    plans,
  });

  write(
    "TEST_RESULTS.md",
    `# Test results

Run:

\`\`\`bash
npm run test:gdi-all-hotel-airtable-persistence-completion-v1
npm run dealality:ci-precheck
\`\`\`

Results recorded by the agent after local execution.
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        totals,
        schemaGate: COMPLETE_PERSISTENCE_CONTRACT.schemaChangeGuard,
        writeGate: "BULK_PRODUCTION_WRITES_NOT_AUTHORIZED",
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
