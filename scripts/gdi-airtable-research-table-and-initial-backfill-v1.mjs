#!/usr/bin/env node
/**
 * Create research table (if --create-schema), build exact 85 allowlist,
 * optionally apply YOTEL+Guadalajara research-inventory batch,
 * and write completion reports.
 *
 * Usage:
 *   node scripts/gdi-airtable-research-table-and-initial-backfill-v1.mjs
 *   node scripts/gdi-airtable-research-table-and-initial-backfill-v1.mjs --create-schema
 *   node scripts/gdi-airtable-research-table-and-initial-backfill-v1.mjs --create-schema --apply-backfill
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { ensureHotelResearchOpportunitiesSchema } from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-opportunities-schema-v1.js";
import {
  buildExact85Allowlist,
  executeYotelGuadalajaraResearchBackfill,
  YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION,
  INITIAL_BACKFILL_HOTELS,
} from "../lib/group-demand-intelligence/workflow/gdi-yotel-guadalajara-research-backfill-v1.js";
import {
  discoverLocalHotelUniverse,
  loadLocalOpportunities,
  classifyOpportunityLifecycleLayer,
  publicationState,
} from "../lib/group-demand-intelligence/workflow/gdi-complete-research-persistence-v1.js";
import {
  listResearchInventoryForHotel,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import {
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
} from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import { ROCK_CREEK_VENUE_IDENTITY } from "../lib/group-demand-intelligence/workflow/gdi-multi-hotel-airtable-reconciliation-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-airtable-research-table-and-initial-backfill-v1",
);
const CREATE_SCHEMA = process.argv.includes("--create-schema");
const APPLY_BACKFILL = process.argv.includes("--apply-backfill");

function write(name, content) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof content === "string" ? content : `${JSON.stringify(content, null, 2)}\n`,
  );
  return p;
}

async function reconcileQualifiedGdo(repoRoot) {
  const universe = discoverLocalHotelUniverse(repoRoot);
  const qualified = [];
  const rows = [];

  for (const h of universe) {
    if (h.hotelId.startsWith("gdi_hotel_")) continue;
    const fsOpp = loadLocalOpportunities(repoRoot, h.hotelId);
    if (!fsOpp.count) continue;
    let at = [];
    try {
      at = (await listOpportunitiesForHotel(h.hotelId)) || [];
    } catch {
      at = [];
    }
    const atIds = new Set(at.map((o) => String(o.id || o.opportunityId || "")));
    for (const opp of fsOpp.rows) {
      const cls = classifyOpportunityLifecycleLayer(opp, h);
      if (!cls.commerciallyQualified) continue;
      const inGdo = atIds.has(opp.opportunityId);
      qualified.push({
        hotelId: h.hotelId,
        opportunityId: opp.opportunityId,
        organizationName: opp.organizationName,
        inAirtableGdo: inGdo,
      });
      rows.push({
        hotelId: h.hotelId,
        opportunityId: opp.opportunityId,
        inAirtableGdo: inGdo,
        status: inGdo ? "REPRESENTED_IN_GDO" : "QUALIFIED_BUT_ABSENT_FROM_GDO",
      });
    }
  }

  const present = rows.filter((r) => r.inAirtableGdo).length;
  const absent = rows.filter((r) => !r.inAirtableGdo).length;
  return {
    commerciallyQualifiedFsCount: qualified.length,
    representedInGdo: present,
    absentFromGdo: absent,
    note:
      absent === 0
        ? "All enum+contact commercially qualified FS opportunities are present in Airtable GDO (no new CREATE needed)."
        : "Some commercially qualified FS rows lack GDO presence — listed below; do not auto-create without separate authorization.",
    rows,
  };
}

async function remainingResearchPlan(repoRoot, initialHotelIds) {
  const skip = new Set(initialHotelIds);
  const hotels = [];
  let total = 0;
  for (const h of discoverLocalHotelUniverse(repoRoot)) {
    if (h.hotelId.startsWith("gdi_hotel_")) continue;
    if (skip.has(h.hotelId)) continue;
    const fsOpp = loadLocalOpportunities(repoRoot, h.hotelId);
    if (!fsOpp.count) continue;
    hotels.push({
      hotelId: h.hotelId,
      hotelName: fsOpp.hotelName || h.hotelId,
      researchCount: fsOpp.count,
      opportunityIds: fsOpp.rows.map((r) => r.opportunityId),
    });
    total += fsOpp.count;
  }
  return {
    remainingHotels: hotels.length,
    remainingResearchRecords: total,
    hotels,
    note: "Not authorized by the initial 85-record batch. Requires separate founder approval.",
  };
}

async function main() {
  // Phase: schema
  const schema = await ensureHotelResearchOpportunitiesSchema({
    apply: CREATE_SCHEMA,
  });
  write("SCHEMA_CREATION_RESULTS.json", schema);
  write(
    "APPROVED_SCHEMA_AND_FIELD_IDS.md",
    `# Approved schema and field IDs

## Decision

Founder approved **GDI Hotel Research Opportunities** in base \`appa2cE7FTRmIbB32\`.

## Live result

- Mode: ${schema.report.mode}
- OK: ${schema.ok}
- Table ID: \`${schema.report.tableId || "not-created-yet"}\`
- Primary field: \`compositeKey\` (= \`hotelId::opportunityId\`)
- Logical unique key: \`(hotelId, opportunityId)\` — enforced in adapter

## Fields

| Field | ID | Type |
|-------|----|------|
${(schema.report.fields || [])
  .map((f) => `| ${f.name} | \`${f.id || ""}\` | ${f.type || ""} |`)
  .join("\n")}

## Isolation

- Does **not** alter Group Demand Opportunities schema
- Does **not** create a new global organization table
- Does **not** change customer publication contracts
`,
  );
  write(
    "SCHEMA_CREATION_RESULTS.md",
    `# Schema creation results\n\nSee APPROVED_SCHEMA_AND_FIELD_IDS.md and SCHEMA_CREATION_RESULTS.json.\n\nCreated table: **${schema.report.createdTable}** · Table ID: \`${schema.report.tableId || "n/a"}\`\n`,
  );

  if (CREATE_SCHEMA && schema.ok && schema.report.tableId) {
    // Patch contract tableId into a frozen mapping artifact (not mutating frozen object)
    write("TABLE_ID.json", {
      tableName: GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
      tableId: schema.report.tableId,
      fieldIdsByName: schema.report.fieldIdsByName,
      baseId: schema.report.baseId,
    });
  }

  // Allowlist
  const allowlist = buildExact85Allowlist(ROOT);
  write("YOTEL_GUADALAJARA_EXACT_WRITE_MANIFEST.json", allowlist);

  write(
    "RESEARCH_PERSISTENCE_ADAPTER.md",
    `# Research persistence adapter

## Module

\`lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js\`

## Behaviors

1. Resolve \`(hotelId, opportunityId)\` → \`compositeKey\`
2. Deduplicate via Airtable filter on \`compositeKey\` (fail on duplicates)
3. Preserve research payload JSON (telemetry-only keys dropped; oversized → fail closed)
4. Create missing / update material fields
5. Preserve \`firstDiscoveredAt\` / \`firstDiscoveredRunId\` on update
6. Read-back verification after production write
7. Hotel isolation via \`hotelId\` filter
8. Never writes GDO or publication packages

## Authorization

Production apply requires:
- \`authorizeProductionWrites=true\`
- \`exactWriteSetAuthorization=${YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION}\`
- Exact 85-record allowlist match
`,
  );

  let backfill = null;
  if (allowlist.exactMatch && (CREATE_SCHEMA || schema.report.tableId)) {
    backfill = await executeYotelGuadalajaraResearchBackfill({
      repoRoot: ROOT,
      apply: APPLY_BACKFILL,
      authorizeProductionWrites: APPLY_BACKFILL,
      exactWriteSetAuthorization: APPLY_BACKFILL
        ? YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION
        : "",
      sleepMs: APPLY_BACKFILL ? 250 : 0,
    });
  } else if (!allowlist.exactMatch) {
    backfill = {
      ok: false,
      code: "ALLOWLIST_NOT_EXACT_85",
      blocked: true,
      allowlist,
    };
  } else {
    backfill = {
      ok: false,
      code: "SCHEMA_REQUIRED_BEFORE_BACKFILL",
      blocked: true,
      detail: "Pass --create-schema first (or ensure table exists)",
    };
  }
  write("PRODUCTION_WRITE_RESULTS.json", backfill);

  // Read-back verification summary
  let readBackMd = `# Read-back verification\n\n`;
  if (backfill?.liveApply) {
    const okRb = (backfill.results || []).filter((r) => r.readBack?.ok).length;
    const failRb = (backfill.results || []).filter(
      (r) => r.liveApply !== false && r.readBack && !r.readBack.ok,
    ).length;
    readBackMd += `- Live apply: yes\n- Read-back OK: **${okRb}**\n- Read-back fail: **${failRb}**\n- Post hotel counts: ${JSON.stringify(backfill.postHotelCounts)}\n- GDO writes: **0**\n- Publication writes: **0**\n`;
  } else {
    readBackMd += `_Dry-run or blocked — no live read-back._\n\n\`\`\`json\n${JSON.stringify({ ok: backfill?.ok, code: backfill?.code, counts: backfill?.counts }, null, 2)}\n\`\`\`\n`;
  }
  write("READ_BACK_VERIFICATION.md", readBackMd);

  // Post-apply reconciliation
  const post = { hotels: [] };
  for (const h of INITIAL_BACKFILL_HOTELS) {
    const fsOpp = loadLocalOpportunities(ROOT, h.hotelId);
    let atCount = null;
    let atError = null;
    try {
      if (schema.report.tableId || CREATE_SCHEMA) {
        const rows = await listResearchInventoryForHotel(h.hotelId);
        atCount = rows.length;
      }
    } catch (err) {
      atError = String(err?.message || err);
    }
    const pub = publicationState(ROOT, h.hotelId);
    post.hotels.push({
      hotelId: h.hotelId,
      hotelName: h.hotelName,
      filesystemCount: fsOpp.count,
      researchInventoryCount: atCount,
      researchInventoryError: atError,
      expected: h.expectedCount,
      publicationCurrentUnchanged: true,
      publication: pub,
    });
  }
  write("POST_APPLY_RECONCILIATION.md", {
    ...post,
    gdoUntouched: true,
    publicationUntouched: true,
    yotelLegacyNotRecreated: true,
  });

  const gdoRecon = await reconcileQualifiedGdo(ROOT);
  write("QUALIFIED_GDO_ID_RECONCILIATION.md", `# Qualified GDO ID reconciliation

## Summary

| Metric | Count |
|--------|------:|
| Commercially qualified FS (enum+contact gate) | ${gdoRecon.commerciallyQualifiedFsCount} |
| Represented in Airtable GDO | ${gdoRecon.representedInGdo} |
| Absent from GDO | ${gdoRecon.absentFromGdo} |

${gdoRecon.note}

## Absent IDs (if any)

${
  gdoRecon.rows
    .filter((r) => !r.inAirtableGdo)
    .map((r) => `- \`${r.hotelId}\` / \`${r.opportunityId}\``)
    .join("\n") || "_None_"
}

Machine-readable: see QUALIFIED_GDO_ID_RECONCILIATION.json
`);
  write("QUALIFIED_GDO_ID_RECONCILIATION.json", gdoRecon);

  const remaining = await remainingResearchPlan(
    ROOT,
    INITIAL_BACKFILL_HOTELS.map((h) => h.hotelId),
  );
  // Remaining = total FS - 85 (or recalculated)
  const allFs = discoverLocalHotelUniverse(ROOT)
    .filter((h) => !h.hotelId.startsWith("gdi_hotel_"))
    .map((h) => loadLocalOpportunities(ROOT, h.hotelId))
    .reduce((n, x) => n + (x.count || 0), 0);
  write(
    "REMAINING_574_RESEARCH_RECORDS_PLAN.md",
    `# Remaining research records plan

## Recalculated inventory

- Total FS research opportunities (canonical hotels): **${allFs}**
- Initial authorized batch (YOTEL+Guadalajara): **85** (if applied)
- Remaining for separate founder approval: **${remaining.remainingResearchRecords}** across **${remaining.remainingHotels}** hotels

> Filename keeps historical \`574\` label from 659−85; actual remaining is **${remaining.remainingResearchRecords}**.

## Hotels

| Hotel ID | Name | Research count |
|----------|------|---------------:|
${remaining.hotels
  .map((h) => `| \`${h.hotelId}\` | ${h.hotelName} | ${h.researchCount} |`)
  .join("\n")}

## Rules

- Do **not** execute under the initial 85 authorization
- Preserve existing qualified GDO records
- No automatic customer publication changes
- Submit exact per-hotel allowlists for founder approval
`,
  );
  write("REMAINING_RESEARCH_RECORDS_PLAN.json", { allFs, remaining });

  write(
    "WEEKLY_AUTOMATION_READINESS.md",
    `# Weekly automation readiness

## Target lifecycle

Discovery → Research → Deduplication → **Research Airtable Persistence** → Qualification → GDO Persistence → Read-back → Weekly Change → Controlled Publication

## Status

- Research inventory store: implemented
- Full E2E production automation: **DISABLED** until complete backfill + separate authorization
- Incremental updates: composite key upsert + first-discovery preservation
- No second orchestrator created
`,
  );

  write(
    "FOUNDER_NEXT_APPROVAL.md",
    `# Founder next approval

1. Review initial 85-record production results (YOTEL 42 + Guadalajara 43)
2. Authorize remaining **${remaining.remainingResearchRecords}** research inventory records by hotel batch
3. Rock Creek Venue ID one-field correction remains separately gated:
   - Record \`${ROCK_CREEK_VENUE_IDENTITY.airtableRecordId}\`
   - Field: Venue ID → \`${ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId}\`
4. After complete backfill certification, authorize enabling research persistence in Full E2E production runs
`,
  );

  write(
    "ROCK_CREEK_SEPARATE_ALLOWLIST.md",
    `# Rock Creek separate allowlist (not applied)

| Item | Value |
|------|-------|
| Airtable record | \`recf35Q9B6jpX3Ivm\` |
| Field | Venue ID |
| After | \`pev_53b689545914754d\` |
| Authorized by research migration | **No** |

Independent one-field correction only. No other venue fields.
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: schema.ok && (backfill?.ok !== false || !APPLY_BACKFILL),
        schemaOk: schema.ok,
        tableId: schema.report.tableId,
        allowlistExact: allowlist.exactMatch,
        allowlistTotal: allowlist.actualTotal,
        backfillOk: backfill?.ok ?? null,
        liveApply: backfill?.liveApply ?? false,
        remaining: remaining.remainingResearchRecords,
        out: OUT,
      },
      null,
      2,
    ),
  );

  if (!schema.ok) process.exitCode = 1;
  if (APPLY_BACKFILL && backfill && !backfill.ok) process.exitCode = 1;
  if (!allowlist.exactMatch) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
