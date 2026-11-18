#!/usr/bin/env node
/**
 * YOTEL replacement + other-hotel reconciliation preparation V1.
 * Read-only. No Airtable deletions/writes. No paid research. No publication changes.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  GDI_OPPORTUNITIES_TABLE_NAME,
  MAP_GDI_OPPORTUNITY as F,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { getGdiOpportunitiesAirtableBaseId } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { escapeAirtableFormulaValue } from "../lib/airtable-utils.js";
import { airtableRequestTimeoutMs } from "../lib/http/with-timeout.js";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  loadFilesystemOpportunities,
  publicationPointers,
  planHotelOpportunitySync,
  ROCK_CREEK_VENUE_IDENTITY,
} from "../lib/group-demand-intelligence/workflow/gdi-multi-hotel-airtable-reconciliation-v2.js";
import {
  YOTEL_HOTEL,
  OTHER_HOTEL_STRATEGIES,
  assertNoMigrationMutation,
  assertYotelHotelScope,
  buildYotelDeleteManifest,
  buildYotel42TargetState,
  assessYotelDependencies,
  designYotelReplacementSequence,
  loadLocalYotelOpportunities,
  redactPiiFields,
  fingerprintRecord,
} from "../lib/group-demand-intelligence/workflow/gdi-yotel-replacement-migration-plan-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-yotel-replacement-other-hotel-reconciliation-v1",
);

function write(name, content) {
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof content === "string" ? content : JSON.stringify(content, null, 2),
  );
  return p;
}

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

async function fetchRawYotelGdos() {
  assertYotelHotelScope(YOTEL_HOTEL.hotelId);
  const apiKey = getToken();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  if (!apiKey || !baseId) {
    throw new Error("gdi_opportunities_airtable_not_configured");
  }
  const base = new Airtable({
    apiKey,
    requestTimeout: airtableRequestTimeoutMs(),
  }).base(baseId);
  const table = base(GDI_OPPORTUNITIES_TABLE_NAME);
  const formula = `{${F.hotelId}}='${escapeAirtableFormulaValue(YOTEL_HOTEL.hotelId)}'`;
  const rows = [];
  await table
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      rows.push(...records);
      next();
    });
  return rows.map((r) => ({
    id: r.id,
    createdTime: r._rawJson?.createdTime || null,
    fields: { ...(r.fields || {}) },
  }));
}

async function listDecisionsForYotelSubjects(opportunityIds) {
  // Best-effort: Decisions table may not be configured; never fail preparation.
  try {
    const mod = await import("../lib/decision-outcomes/airtable-store.js");
    const listFn =
      mod.listDecisionSummaries ||
      mod.listAllDecisionRecords ||
      null;
    if (typeof listFn !== "function") {
      return { ok: false, reason: "decision_list_unavailable", rows: [] };
    }
    const rows = await listFn(YOTEL_HOTEL.hotelId);
    const wanted = new Set(opportunityIds);
    return {
      ok: true,
      rows: (rows || []).filter((d) =>
        wanted.has(String(d.subjectId || d.opportunityId || "")),
      ),
    };
  } catch (err) {
    return {
      ok: false,
      reason: String(err?.code || err?.message || err),
      rows: [],
    };
  }
}

function snapshotLocalBaseline(local) {
  const ids = local.opportunities
    .map((o) => o.id || o.opportunityId)
    .filter(Boolean)
    .sort();
  return {
    path: local.path,
    present: local.present,
    count: local.opportunities.length,
    opportunityIds: ids,
    sha256OfIds: fingerprintRecord({
      id: "local-baseline",
      fields: { opportunityId: ids.join("|"), hotelId: YOTEL_HOTEL.hotelId },
    }),
  };
}

function mdHotelPlan(title, body) {
  return `# ${title}\n\n${body}\n`;
}

async function reconcileOtherHotel(key, strategy) {
  const hotel = {
    key,
    name: strategy.name,
    hotelId: strategy.hotelId,
    cohort: "OTHER",
    strategy: strategy.strategy,
  };
  const filesystem = loadFilesystemOpportunities(ROOT, hotel.hotelId);
  let airtableRows = [];
  let airtableError = null;
  try {
    airtableRows = await listOpportunitiesForHotel(hotel.hotelId);
  } catch (err) {
    airtableError = String(err?.code || err?.message || err);
  }
  const plan = planHotelOpportunitySync({
    hotel,
    filesystem,
    airtableRows,
  });
  return {
    hotel,
    strategy: strategy.strategy,
    filesystemCount: plan.filesystemCount,
    airtableCount: plan.airtableCount,
    counts: plan.counts,
    decisionCounts: plan.decisionCounts,
    syncOperations: plan.syncOperations,
    conflicts: plan.inventoryRows.filter((r) => r.syncDecision === "CONFLICT"),
    publication: publicationPointers(ROOT, hotel.hotelId),
    airtableError,
  };
}

async function main() {
  assertNoMigrationMutation({
    apply: false,
    delete: false,
    authorizeProductionWrites: false,
  });
  fs.mkdirSync(OUT, { recursive: true });

  const local = loadLocalYotelOpportunities(ROOT);
  const localSnapshot = snapshotLocalBaseline(local);
  const rawYotel = await fetchRawYotelGdos();

  // Sanity: every raw row hotelId must be YOTEL
  for (const rec of rawYotel) {
    assertYotelHotelScope(rec.fields?.hotelId);
  }

  const deleteManifest = buildYotelDeleteManifest({
    airtableRawRecords: rawYotel,
    priorReconciliationCount: 108,
  });
  const targetState = buildYotel42TargetState({
    localOpportunities: local.opportunities,
  });

  const decisions = await listDecisionsForYotelSubjects(
    deleteManifest.opportunityIdsExact,
  );
  const dependencyAudit = assessYotelDependencies({
    deleteManifest,
    decisionsBySubjectId: decisions.rows || [],
    publication: publicationPointers(ROOT, YOTEL_HOTEL.hotelId),
  });
  if (!decisions.ok) {
    dependencyAudit.decisionLookup = {
      ok: false,
      reason: decisions.reason,
      note: "Decision table lookup unavailable or failed; GDO decisionId fields still captured in delete manifest.",
    };
  } else {
    dependencyAudit.decisionLookup = { ok: true, matched: decisions.rows.length };
  }

  const sequence = designYotelReplacementSequence({
    deleteManifest,
    targetState,
  });

  const backup = {
    version: "gdi-yotel-gdo-full-backup-v1",
    generatedAt: new Date().toISOString(),
    hotel: YOTEL_HOTEL,
    baseId: getGdiOpportunitiesAirtableBaseId(),
    table: GDI_OPPORTUNITIES_TABLE_NAME,
    recordCount: rawYotel.length,
    piiRedacted: true,
    restorationNote:
      "Committed backup redacts email/phone-like values. Before production delete, regenerate an unredacted local backup outside git via the same script with GDI_YOTEL_BACKUP_UNREDACTED=1 if founder-approved for local recovery media.",
    records: rawYotel.map((r) => ({
      id: r.id,
      createdTime: r.createdTime,
      fields:
        process.env.GDI_YOTEL_BACKUP_UNREDACTED === "1"
          ? r.fields
          : redactPiiFields(r.fields),
      fingerprint: fingerprintRecord(r),
    })),
  };

  write("YOTEL_108_RECORD_DELETE_MANIFEST.json", deleteManifest);
  write("YOTEL_42_RECORD_TARGET_STATE.json", targetState);
  write("YOTEL_GDO_BACKUP_REDACTED.json", backup);
  write("YOTEL_LOCAL_BASELINE_SNAPSHOT.json", localSnapshot);
  write("YOTEL_DEPENDENCY_AND_FEEDBACK_AUDIT.json", dependencyAudit);
  write("MIGRATION_EXECUTION_SEQUENCE.json", sequence);

  // Other hotels
  const radisson = await reconcileOtherHotel(
    "radisson",
    OTHER_HOTEL_STRATEGIES.radisson,
  );
  const guad = await reconcileOtherHotel(
    "guadalajara",
    OTHER_HOTEL_STRATEGIES.guadalajara,
  );
  const wyndham = await reconcileOtherHotel(
    "wyndham",
    OTHER_HOTEL_STRATEGIES.wyndham,
  );
  const castillo = await reconcileOtherHotel(
    "castillo",
    OTHER_HOTEL_STRATEGIES.castillo,
  );
  write("OTHER_HOTELS_RECONCILIATION_SNAPSHOT.json", {
    radisson,
    guadalajara: guad,
    wyndham,
    castillo,
  });

  write(
    "YOTEL_FULL_BACKUP_AND_RECOVERY_PLAN.md",
    mdHotelPlan(
      "YOTEL full backup and recovery plan",
      [
        `- Hotel: \`${YOTEL_HOTEL.hotelId}\``,
        `- Backed-up GDO rows: **${backup.recordCount}**`,
        `- Artifact: \`YOTEL_GDO_BACKUP_REDACTED.json\` (PII redacted in git)`,
        `- Fingerprints: per-record sha256 over redacted field set`,
        `- Local baseline snapshot: \`YOTEL_LOCAL_BASELINE_SNAPSHOT.json\` (${localSnapshot.count} ids)`,
        "",
        "## Restoration feasibility",
        "1. Re-create each GDO from backup `fields` via Airtable create (or API upsert by opportunityId).",
        "2. Re-attach linked record IDs only if those shared records still exist (DG/Program/Signal are never deleted by this migration).",
        "3. Re-apply `decisionId` / `actionStatus` from backup when present.",
        "4. Verify read-back count and opportunityId set equal backup.",
        "5. Filesystem `opportunities.json` is independent and remains available even if Airtable restore is needed.",
        "",
        "## Deletion prerequisite",
        "Do **not** delete until founder authorizes the exact `airtableRecordIdsExact` list and an unredacted local recovery copy exists on founder-controlled media if contact fields must be preserved bit-for-bit.",
      ].join("\n"),
    ),
  );

  write(
    "YOTEL_QUALIFIED_GDO_REPLACEMENT_SET.md",
    mdHotelPlan(
      "YOTEL qualified GDO replacement set",
      [
        `- Local baseline records: **${targetState.actualLocalCount}** (expected 42)`,
        `- Qualified for GDO create: **${targetState.qualifiedGdoCount}**`,
        `- Remain research-store only: **${targetState.researchOnlyCount}**`,
        "",
        "### Qualified opportunity IDs",
        ...(targetState.qualifiedOpportunityIds.length
          ? targetState.qualifiedOpportunityIds.map((id) => `- \`${id}\``)
          : ["- _(none — commercial eligibility not met for any of the 42)_"]),
        "",
        "### Research-only IDs (filesystem authority; not GDO creates)",
        `Count: ${targetState.researchOnlyCount}. Full list in \`YOTEL_42_RECORD_TARGET_STATE.json\`.`,
        "",
        "**Important:** Replacing the obsolete 108 does **not** require inserting all 42 into GDO. Research-only rows stay on disk.",
      ].join("\n"),
    ),
  );

  write(
    "YOTEL_DEPENDENCY_AND_FEEDBACK_AUDIT.md",
    mdHotelPlan(
      "YOTEL dependency and feedback audit",
      [
        `- GDO rows with decisionId field: **${dependencyAudit.gdoRowsWithDecisionIdField.length}**`,
        `- Linked Decision records found via hotel lookup: **${dependencyAudit.linkedDecisionsFound.length}**`,
        `- Decision lookup: ${JSON.stringify(dependencyAudit.decisionLookup)}`,
        `- Shared DG-linked GDO rows: ${dependencyAudit.sharedIntelligence.demandGeneratorLinks}`,
        `- Publication STAGED/CURRENT must remain unchanged: **yes**`,
        "",
        "### Policy",
        "- Delete GDO rows only.",
        "- Do not delete Demand Generators / Programs / Signals / Venues.",
        "- Preserve Decision / Decision Event history independently.",
        "- Customer publication packages are out of scope.",
      ].join("\n"),
    ),
  );

  write(
    "RADISSON_RECONCILIATION.md",
    mdHotelPlan(
      "Radisson — reconcile (non-destructive)",
      [
        `- FS ${radisson.filesystemCount} / AT ${radisson.airtableCount}`,
        `- Counts: \`${JSON.stringify(radisson.counts)}\``,
        `- Decisions: \`${JSON.stringify(radisson.decisionCounts)}\``,
        `- Conflicts: ${radisson.conflicts.map((c) => "`" + c.opportunityId + "`").join(", ") || "_none_"}`,
        "",
        "Preserve shared identities. Do not bulk-create unqualified FS-only research.",
        "Investigate org-name conflicts manually. Preserve Airtable-only pending classification.",
      ].join("\n"),
    ),
  );

  write(
    "GUADALAJARA_RECONCILIATION.md",
    mdHotelPlan(
      "Guadalajara — reconcile (non-destructive)",
      [
        `- FS ${guad.filesystemCount} / AT ${guad.airtableCount}`,
        `- CREATE candidates: ${guad.decisionCounts?.CREATE || 0}`,
        `- DEFERRED: ${guad.decisionCounts?.DEFERRED || 0}`,
        "",
        "Preserve local inventory. Persist qualified opportunities only when separately authorized.",
        "No synthetic/provisional need-period promotion.",
      ].join("\n"),
    ),
  );

  write(
    "WYNDHAM_CASTILLO_PRESERVATION.md",
    mdHotelPlan(
      "Wyndham & Castillo — preserve",
      [
        `### Wyndham`,
        `- FS ${wyndham.filesystemCount} / AT ${wyndham.airtableCount}`,
        `- UNCHANGED: ${wyndham.decisionCounts?.UNCHANGED || 0}`,
        `- CREATE/UPDATE: ${(wyndham.decisionCounts?.CREATE || 0) + (wyndham.decisionCounts?.UPDATE || 0)}`,
        "",
        `### Castillo`,
        `- FS ${castillo.filesystemCount} / AT ${castillo.airtableCount}`,
        `- UNCHANGED: ${castillo.decisionCounts?.UNCHANGED || 0}`,
        `- CREATE/UPDATE: ${(castillo.decisionCounts?.CREATE || 0) + (castillo.decisionCounts?.UPDATE || 0)}`,
        "",
        "Do not rewrite unchanged records.",
      ].join("\n"),
    ),
  );

  write(
    "ROCK_CREEK_CORRECTION_PLAN.md",
    mdHotelPlan(
      "Rock Creek structured Venue ID correction (separate auth)",
      [
        `- Record: \`${ROCK_CREEK_VENUE_IDENTITY.airtableRecordId}\``,
        `- Authoritative canonical: \`${ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId}\``,
        `- Current structured (pilot): \`${ROCK_CREEK_VENUE_IDENTITY.structuredIdAfterPilot}\``,
        `- Proposed single-field UPDATE: Venue ID → \`${ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId}\``,
        `- Align payload if needed (already domain-canonical)`,
        "",
        "**Do not apply as part of YOTEL migration.** Requires separate founder authorization.",
      ].join("\n"),
    ),
  );

  write(
    "MIGRATION_EXECUTION_SEQUENCE.md",
    mdHotelPlan(
      "Migration execution sequence",
      [
        "Airtable has **no** atomic multi-record transactions. Use staged recoverable phases:",
        "",
        ...sequence.phases.map(
          (p) =>
            `### Phase ${p.phase} — ${p.name}\n- Writes: \`${p.writes}\`\n${(p.actions || []).map((a) => `  - ${a}`).join("\n")}`,
        ),
        "",
        `Proposed deletes: **${deleteManifest.counts.proposedDeletes}**`,
        `Qualified creates: **${targetState.qualifiedGdoCount}**`,
        `Research-only remain on filesystem: **${targetState.researchOnlyCount}**`,
      ].join("\n"),
    ),
  );

  write(
    "FOUNDER_PRODUCTION_AUTHORIZATION_REQUEST.md",
    mdHotelPlan(
      "Founder production authorization request — YOTEL replacement",
      [
        "This preparation phase did **not** delete or write Airtable records.",
        "",
        "## Requested authorization (when ready)",
        "1. Delete exactly the Airtable record IDs in `YOTEL_108_RECORD_DELETE_MANIFEST.json` → `airtableRecordIdsExact`.",
        `2. Create exactly the qualified GDO set (${targetState.qualifiedGdoCount} IDs) from \`YOTEL_42_RECORD_TARGET_STATE.json\`.`,
        "3. Leave research-only local rows on filesystem only.",
        "4. Do not touch other hotels, shared intelligence, or publication packages.",
        "5. Rock Creek correction remains a separate allowlist item.",
        "",
        "## Required flags (future apply runner)",
        "- `authorizeProductionWrites=true`",
        "- `exactWriteSetAuthorization=<hash-bound manifest token>`",
        "- `apply=true`",
        "- hotel allowlist: `recrPQcZg7SFARRb2` only",
        "",
        "**STOP — await founder signature on this request before any production mutation.**",
      ].join("\n"),
    ),
  );

  write(
    "FOUNDER_EXECUTIVE_SUMMARY.md",
    `# YOTEL replacement + other-hotel reconciliation — founder summary

**STOP FOR FOUNDER REVIEW — NO DELETIONS OR BULK WRITES EXECUTED**

## Directive applied

- **YOTEL:** replace obsolete Airtable GDO inventory with current local research baseline (controlled deletion + selective qualified recreate).
- **All other hotels:** non-destructive reconcile / preserve.
- **Rock Creek:** correction prepared, not applied with YOTEL.

## YOTEL verified counts

| Item | Count |
|------|------:|
| Airtable GDO (hotelId-scoped re-query) | ${deleteManifest.counts.proposedDeletes} |
| Matches prior 108 | ${deleteManifest.counts.countMatchesPrior} |
| Local baseline opportunities | ${targetState.actualLocalCount} |
| ID overlap FS∩AT | 0 (replacement, not merge) |
| Qualified GDO create candidates | ${targetState.qualifiedGdoCount} |
| Research-only (filesystem remains authority) | ${targetState.researchOnlyCount} |
| Out-of-scope rows rejected | ${deleteManifest.counts.rejectedOutOfScope} |

## Safety

- Exact hotel ID allowlist: \`${YOTEL_HOTEL.hotelId}\`
- Exact delete ID manifest committed
- Redacted full-field backup committed; restore procedure documented
- Shared DG/Program/Signal/Venue **not** in delete set
- Publication STAGED/CURRENT untouched
- Primary WIP preserved

## Other hotels

- Radisson: reconcile — FS ${radisson.filesystemCount} / AT ${radisson.airtableCount}; conflicts ${radisson.conflicts.length}
- Guadalajara: reconcile — FS ${guad.filesystemCount} / AT ${guad.airtableCount}; CREATE ${guad.decisionCounts?.CREATE || 0}
- Wyndham: preserve — FS ${wyndham.filesystemCount} / AT ${wyndham.airtableCount}; UNCHANGED ${wyndham.decisionCounts?.UNCHANGED || 0}
- Castillo: preserve — FS ${castillo.filesystemCount} / AT ${castillo.airtableCount}; UNCHANGED ${castillo.decisionCounts?.UNCHANGED || 0}

## Next

Review \`FOUNDER_PRODUCTION_AUTHORIZATION_REQUEST.md\` and exact manifests. No production migration until you authorize the delete/create allowlists.
`,
  );

  write(
    "README.md",
    `# YOTEL replacement + other-hotel reconciliation V1

Preparation package for founder-amended migration direction on PR #368.

- YOTEL = controlled GDO replacement
- Other hotels = non-destructive reconcile/preserve
- No production mutations in this phase

Regenerate:

\`\`\`bash
node scripts/gdi-yotel-replacement-other-hotel-reconciliation-v1.mjs
node scripts/test-gdi-yotel-replacement-migration-plan-v1.mjs
\`\`\`
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        yotelDeletes: deleteManifest.counts.proposedDeletes,
        countMatchesPrior108: deleteManifest.counts.countMatchesPrior,
        local42: targetState.actualLocalCount,
        qualifiedCreates: targetState.qualifiedGdoCount,
        researchOnly: targetState.researchOnlyCount,
        rejectedOutOfScope: deleteManifest.counts.rejectedOutOfScope,
        radisson: {
          fs: radisson.filesystemCount,
          at: radisson.airtableCount,
          conflicts: radisson.conflicts.length,
        },
        guadalajara: {
          fs: guad.filesystemCount,
          at: guad.airtableCount,
          create: guad.decisionCounts?.CREATE || 0,
        },
        wyndhamUnchanged: wyndham.decisionCounts?.UNCHANGED || 0,
        castilloUnchanged: castillo.decisionCounts?.UNCHANGED || 0,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
