#!/usr/bin/env node
/**
 * Execute founder-authorized YOTEL legacy GDO deletion (exact 108-ID allowlist).
 *
 * Required flags for mutation:
 *   --apply --authorize-yotel-legacy-delete --exact-auth=yotel-legacy-gdo-delete-108-v1
 *
 * Without --apply: preflight + off-git backup dry-run only.
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
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { escapeAirtableFormulaValue } from "../lib/airtable-utils.js";
import { airtableRequestTimeoutMs } from "../lib/http/with-timeout.js";
import {
  YOTEL_LEGACY_DELETE_AUTH,
  loadDeleteAllowlist,
  preflightYotelLegacyDeletion,
  chunkIds,
  buildDeletionLedgerSkeleton,
  applyBatchResultToLedger,
  remainingDeleteIds,
  fingerprintIds,
  assertAuthorizedYotelDelete,
  writeJson,
} from "../lib/group-demand-intelligence/workflow/gdi-yotel-legacy-gdo-deletion-v1.js";
import {
  loadFilesystemOpportunities,
  publicationPointers,
  planHotelOpportunitySync,
  ROCK_CREEK_VENUE_IDENTITY,
  REFERENCE_HOTELS_V2,
} from "../lib/group-demand-intelligence/workflow/gdi-multi-hotel-airtable-reconciliation-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const MANIFEST = path.join(
  ROOT,
  "reports/gdi-yotel-replacement-other-hotel-reconciliation-v1/YOTEL_108_RECORD_DELETE_MANIFEST.json",
);
const OUT = path.join(ROOT, "reports/gdi-yotel-legacy-cleanup-and-multi-hotel-sync-v1");
const SECURE_BACKUP_ROOT = path.join(
  "C:\\Dev\\dealality\\_secure-backups",
  "gdi-yotel-legacy-gdo-deletion-v1",
);

function parseArgs(argv) {
  const out = {
    apply: false,
    authorize: false,
    exactAuth: null,
  };
  for (const a of argv) {
    if (a === "--apply") out.apply = true;
    if (a === "--authorize-yotel-legacy-delete") out.authorize = true;
    if (a.startsWith("--exact-auth=")) out.exactAuth = a.slice("--exact-auth=".length);
  }
  return out;
}

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function gdoTable() {
  const apiKey = getToken();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  if (!apiKey || !baseId) throw new Error("gdi_opportunities_airtable_not_configured");
  return new Airtable({
    apiKey,
    requestTimeout: airtableRequestTimeoutMs(),
  }).base(baseId)(GDI_OPPORTUNITIES_TABLE_NAME);
}

async function fetchRawYotelGdos() {
  const table = gdoTable();
  const formula = `{${F.hotelId}}='${escapeAirtableFormulaValue(YOTEL_LEGACY_DELETE_AUTH.hotelId)}'`;
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

function countLocalYotel() {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    YOTEL_LEGACY_DELETE_AUTH.hotelId,
    "opportunities.json",
  );
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const ops = Array.isArray(j) ? j : j.opportunities || [];
  return { path: p, count: ops.length, opportunityIds: ops.map((o) => o.id || o.opportunityId).filter(Boolean) };
}

function writeMd(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body);
}

async function countHotelGdos(hotelId) {
  try {
    const rows = await listOpportunitiesForHotel(hotelId);
    return { ok: true, count: (rows || []).length };
  } catch (err) {
    return { ok: false, count: null, error: String(err?.message || err) };
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  fs.mkdirSync(OUT, { recursive: true });
  fs.mkdirSync(SECURE_BACKUP_ROOT, { recursive: true });

  const allowlist = loadDeleteAllowlist(MANIFEST);
  const local = countLocalYotel();
  const liveRaw = await fetchRawYotelGdos();
  const preflight = preflightYotelLegacyDeletion({
    allowlist,
    liveRawRecords: liveRaw,
    localOpportunityCount: local.count,
  });

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const backupPath = path.join(
    SECURE_BACKUP_ROOT,
    `yotel-gdo-unredacted-backup-${stamp}.json`,
  );
  const backupDoc = {
    version: "gdi-yotel-gdo-unredacted-backup-v1",
    generatedAt: new Date().toISOString(),
    hotel: YOTEL_LEGACY_DELETE_AUTH,
    recordCount: liveRaw.length,
    airtableRecordIds: liveRaw.map((r) => r.id).sort(),
    idFingerprint: fingerprintIds(liveRaw.map((r) => r.id)),
    records: liveRaw,
  };
  writeJson(backupPath, backupDoc);
  writeJson(path.join(SECURE_BACKUP_ROOT, "LATEST_BACKUP_POINTER.json"), {
    backupPath,
    generatedAt: backupDoc.generatedAt,
    recordCount: backupDoc.recordCount,
    idFingerprint: backupDoc.idFingerprint,
    recovery:
      "Re-create GDO rows from records[].fields via Airtable create; preserve opportunityId; do not recreate shared DG/Program/Signal/Venue.",
  });

  writeMd(
    "YOTEL_DELETE_PREFLIGHT.md",
    [
      "# YOTEL delete preflight",
      "",
      `- Hotel: \`${YOTEL_LEGACY_DELETE_AUTH.hotelId}\``,
      `- Live GDO count: **${preflight.liveCount}**`,
      `- Allowlist count: **${preflight.allowCount}**`,
      `- Local research count: **${local.count}**`,
      `- Preflight: **${preflight.code}**`,
      `- Hotel mismatches: ${preflight.hotelMismatches.length}`,
      `- Live not in allowlist: ${preflight.liveNotInAllowlist.length}`,
      `- Allowlist missing from live: ${preflight.allowlistMissingFromLive.length}`,
      `- Off-git backup: \`${backupPath}\``,
      "",
      preflight.ok
        ? "Preflight PASS — exact allowlist match."
        : "Preflight BLOCKED — do not delete.",
      "",
    ].join("\n"),
  );

  writeJson(path.join(OUT, "YOTEL_DELETE_PREFLIGHT.json"), {
    preflight,
    local,
    backupPath,
    backupFingerprint: backupDoc.idFingerprint,
  });

  if (!preflight.ok) {
    console.error(JSON.stringify({ ok: false, preflight }, null, 2));
    process.exit(2);
  }

  if (!args.apply) {
    console.log(
      JSON.stringify(
        {
          ok: true,
          mode: "DRY_RUN_PREFLIGHT_ONLY",
          preflight: preflight.code,
          backupPath,
          liveCount: preflight.liveCount,
        },
        null,
        2,
      ),
    );
    return;
  }

  assertAuthorizedYotelDelete({
    authorizeYotelLegacyDelete: args.authorize,
    exactWriteSetAuthorization: args.exactAuth,
    apply: args.apply,
  });

  const ledgerPath = path.join(OUT, "YOTEL_DELETION_LEDGER.json");
  let ledger = fs.existsSync(ledgerPath)
    ? JSON.parse(fs.readFileSync(ledgerPath, "utf8"))
    : buildDeletionLedgerSkeleton({ allowlist, preflight, backupPath });

  // Reconcile remaining against live after any prior partial run
  const stillLive = new Set((await fetchRawYotelGdos()).map((r) => r.id));
  const alreadyGone = allowlist.airtableRecordIdsExact.filter((id) => !stillLive.has(id));
  for (const id of alreadyGone) {
    if (!ledger.deletedIds.includes(id)) ledger.deletedIds.push(id);
  }

  const pending = remainingDeleteIds(
    allowlist.airtableRecordIdsExact,
    ledger.deletedIds,
  ).filter((id) => stillLive.has(id));

  const table = gdoTable();
  const batches = chunkIds(pending, 10);
  for (let i = 0; i < batches.length; i++) {
    const ids = batches[i];
    // Final hotel-scope check on each id before destroy
    for (const id of ids) {
      const rec = await table.find(id);
      const hotelId = String(rec.fields?.[F.hotelId] || "").trim();
      if (hotelId !== YOTEL_LEGACY_DELETE_AUTH.hotelId) {
        throw new Error(`cross_hotel_delete_blocked:${id}:${hotelId}`);
      }
      if (!allowlist.airtableRecordIdsExact.includes(id)) {
        throw new Error(`not_in_allowlist:${id}`);
      }
    }
    try {
      await table.destroy(ids);
      ledger = applyBatchResultToLedger(ledger, {
        ok: true,
        index: i,
        ids,
        at: new Date().toISOString(),
      });
    } catch (err) {
      ledger = applyBatchResultToLedger(ledger, {
        ok: false,
        index: i,
        ids,
        at: new Date().toISOString(),
        error: String(err?.message || err),
      });
      writeJson(ledgerPath, ledger);
      throw err;
    }
    writeJson(ledgerPath, ledger);
  }

  const after = await fetchRawYotelGdos();
  const afterIds = after.map((r) => r.id).sort();
  const deletedOk =
    after.length === 0 &&
    ledger.deletedIds.length === allowlist.airtableRecordIdsExact.length &&
    ledger.failed.length === 0;
  ledger.completed = deletedOk;
  ledger.completedAt = new Date().toISOString();
  ledger.postDeleteLiveCount = after.length;
  ledger.postDeleteLiveIds = afterIds;
  writeJson(ledgerPath, ledger);

  const localAfter = countLocalYotel();
  const otherHotels = {};
  for (const h of REFERENCE_HOTELS_V2.filter((x) => x.hotelId !== YOTEL_LEGACY_DELETE_AUTH.hotelId)) {
    otherHotels[h.key] = {
      hotelId: h.hotelId,
      name: h.name,
      ...(await countHotelGdos(h.hotelId)),
    };
  }

  const pub = publicationPointers(ROOT, YOTEL_LEGACY_DELETE_AUTH.hotelId);

  writeJson(path.join(OUT, "YOTEL_PRODUCTION_DELETE_RESULTS.json"), {
    ok: deletedOk,
    authorizationId: YOTEL_LEGACY_DELETE_AUTH.authorizationId,
    deletedCount: ledger.deletedIds.length,
    failedBatches: ledger.failed.length,
    postDeleteLiveCount: after.length,
    backupPath,
    deletedIds: ledger.deletedIds,
  });

  writeMd(
    "YOTEL_POST_DELETE_READBACK.md",
    [
      "# YOTEL post-delete read-back",
      "",
      `- Remaining YOTEL GDOs: **${after.length}**`,
      `- Deleted IDs recorded: **${ledger.deletedIds.length}**`,
      `- Failed batches: **${ledger.failed.length}**`,
      `- Success: **${deletedOk}**`,
      `- Remaining IDs: ${afterIds.length ? afterIds.map((id) => "`" + id + "`").join(", ") : "_(none)_"}`,
      "",
    ].join("\n"),
  );

  writeMd(
    "YOTEL_LOCAL_RESEARCH_INTEGRITY.md",
    [
      "# YOTEL local research integrity",
      "",
      `- Path: \`${localAfter.path}\``,
      `- Count before: ${local.count}`,
      `- Count after: **${localAfter.count}**`,
      `- Unchanged expected 42: **${localAfter.count === 42}**`,
      "",
    ].join("\n"),
  );

  writeMd(
    "YOTEL_BACKUP_AND_RECOVERY.md",
    [
      "# YOTEL backup and recovery",
      "",
      `- Off-git unredacted backup: \`${backupPath}\``,
      `- Pointer: \`C:\\\\Dev\\\\dealality\\\\_secure-backups\\\\gdi-yotel-legacy-gdo-deletion-v1\\\\LATEST_BACKUP_POINTER.json\``,
      `- Record count: ${backupDoc.recordCount}`,
      `- ID fingerprint: \`${backupDoc.idFingerprint}\``,
      `- Git retains redacted audit backup under replacement package; unredacted copy is outside Git.`,
      "",
      "## Recovery",
      "1. Load unredacted backup JSON.",
      "2. Recreate each GDO with original `opportunityId` + fields.",
      "3. Do not delete/recreate shared DG/Program/Signal/Venue records.",
      "4. Read-back verify opportunityId set.",
      "",
    ].join("\n"),
  );

  // Other-hotel status snapshots (no writes)
  const radisson = planHotelOpportunitySync({
    hotel: { key: "radisson", name: "Radisson Blu Toronto Downtown", hotelId: "recH9h3McgMNFQEla" },
    filesystem: loadFilesystemOpportunities(ROOT, "recH9h3McgMNFQEla"),
    airtableRows: await listOpportunitiesForHotel("recH9h3McgMNFQEla"),
  });
  const guad = planHotelOpportunitySync({
    hotel: { key: "guadalajara", name: "Sheraton Guadalajara Expo", hotelId: "recCAGqkERSXgiku4" },
    filesystem: loadFilesystemOpportunities(ROOT, "recCAGqkERSXgiku4"),
    airtableRows: await listOpportunitiesForHotel("recCAGqkERSXgiku4"),
  });
  const wyndham = planHotelOpportunitySync({
    hotel: { key: "wyndham", name: "Wyndham San Diego Bayside", hotelId: "recnPB3Q02NO8Ma1o" },
    filesystem: loadFilesystemOpportunities(ROOT, "recnPB3Q02NO8Ma1o"),
    airtableRows: await listOpportunitiesForHotel("recnPB3Q02NO8Ma1o"),
  });
  const castillo = planHotelOpportunitySync({
    hotel: { key: "castillo", name: "Castillo Hotel Son Vida", hotelId: "rec82D9zpB8fede1I" },
    filesystem: loadFilesystemOpportunities(ROOT, "rec82D9zpB8fede1I"),
    airtableRows: await listOpportunitiesForHotel("rec82D9zpB8fede1I"),
  });

  writeMd(
    "RADISSON_SYNC_CANDIDATES.md",
    [
      "# Radisson sync candidates (not executed)",
      "",
      `- FS ${radisson.filesystemCount} / AT ${radisson.airtableCount}`,
      `- CREATE ${radisson.decisionCounts.CREATE || 0} · UPDATE ${radisson.decisionCounts.UPDATE || 0} · UNCHANGED ${radisson.decisionCounts.UNCHANGED || 0} · DEFERRED ${radisson.decisionCounts.DEFERRED || 0} · CONFLICT ${radisson.decisionCounts.CONFLICT || 0}`,
      "",
      "### Eligible operations (require separate founder auth)",
      ...(radisson.syncOperations.length
        ? radisson.syncOperations.map(
            (o) =>
              `- \`${o.decision}\` \`${o.opportunityId}\` — ${o.reason}`,
          )
        : ["- _(none currently)_"]),
      "",
      "### Conflicts",
      ...radisson.inventoryRows
        .filter((r) => r.syncDecision === "CONFLICT")
        .map((r) => `- \`${r.opportunityId}\` — ${r.conflicts?.join(",")}`),
      "",
    ].join("\n"),
  );

  writeMd(
    "GUADALAJARA_SYNC_CANDIDATES.md",
    [
      "# Guadalajara sync candidates (not executed)",
      "",
      `- FS ${guad.filesystemCount} / AT ${guad.airtableCount}`,
      `- CREATE ${guad.decisionCounts.CREATE || 0} · DEFERRED ${guad.decisionCounts.DEFERRED || 0}`,
      "",
      guad.decisionCounts.CREATE
        ? "Qualified creates listed in planner syncOperations."
        : "**Zero** currently qualify for GDO persistence. Full 43 remain on filesystem research store.",
      "",
    ].join("\n"),
  );

  writeMd(
    "WYNDHAM_CASTILLO_PRESERVATION.md",
    [
      "# Wyndham & Castillo preservation",
      "",
      `- Wyndham: FS ${wyndham.filesystemCount} / AT ${wyndham.airtableCount} · UNCHANGED ${wyndham.decisionCounts.UNCHANGED || 0}`,
      `- Castillo: FS ${castillo.filesystemCount} / AT ${castillo.airtableCount} · UNCHANGED ${castillo.decisionCounts.UNCHANGED || 0}`,
      "",
      "No rewrites performed.",
      "",
    ].join("\n"),
  );

  writeMd(
    "ROCK_CREEK_REMEDIATION_STATUS.md",
    [
      "# Rock Creek remediation status",
      "",
      `- Record: \`${ROCK_CREEK_VENUE_IDENTITY.airtableRecordId}\``,
      `- Canonical: \`${ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId}\``,
      `- Structured (live after pilot): \`${ROCK_CREEK_VENUE_IDENTITY.structuredIdAfterPilot}\``,
      "",
      "**Not authorized by this YOTEL cleanup prompt.** Next eligible single-field UPDATE remains prepared but unexecuted.",
      "",
    ].join("\n"),
  );

  writeJson(path.join(OUT, "MULTI_HOTEL_RECONCILIATION_STATUS.json"), {
    yotelPostDeleteLiveCount: after.length,
    otherHotels,
    radisson: radisson.decisionCounts,
    guadalajara: guad.decisionCounts,
    wyndham: wyndham.decisionCounts,
    castillo: castillo.decisionCounts,
    publicationYotel: pub,
  });

  writeMd(
    "MULTI_HOTEL_RECONCILIATION_STATUS.md",
    [
      "# Multi-hotel reconciliation status",
      "",
      `- YOTEL remaining GDOs: **${after.length}**`,
      `- Other hotel GDO counts (post-YOTEL-delete):`,
      ...Object.entries(otherHotels).map(
        ([k, v]) => `  - ${k}: ${v.count}`,
      ),
      `- Publication STAGED/CURRENT present: staged=${Boolean(pub.staged)} current=${Boolean(pub.current)} (unchanged by deletion)`,
      "",
    ].join("\n"),
  );

  writeMd(
    "FULL_E2E_PERSISTENCE_READINESS.md",
    [
      "# Full E2E persistence readiness",
      "",
      "Existing contracts remain authoritative:",
      "- Persistence adapter + fail-closed production guard",
      "- Commercial payload durability + shared demand identity",
      "- Weekly Change durability",
      "- Multi-hotel dry-run planner",
      "- Exact allowlist write pattern (Bethesda pilot / YOTEL delete)",
      "",
      "General automatic production writes remain **fail-closed**.",
      "Next hotel-specific writes require founder exact allowlists.",
      "",
    ].join("\n"),
  );

  writeMd(
    "NEXT_FOUNDER_DECISIONS.md",
    [
      "# Next founder decisions",
      "",
      "1. Authorize Rock Creek structured Venue ID restore to `pev_53b689545914754d` (single field).",
      "2. Authorize any Radisson CONFLICT resolution + eligible CREATE/UPDATE allowlist (currently CREATE/UPDATE may be 0).",
      "3. Authorize Guadalajara GDO creates only if/when eligibility is met (currently 0).",
      "4. Do **not** enable unrestricted Full E2E production persistence.",
      "",
    ].join("\n"),
  );

  writeMd(
    "FOUNDER_EXECUTIVE_SUMMARY.md",
    [
      "# YOTEL legacy cleanup — founder summary",
      "",
      `**Deletion executed:** ${deletedOk}`,
      "",
      `| Metric | Value |`,
      `|---|---:|`,
      `| Deleted YOTEL GDOs | ${ledger.deletedIds.length} |`,
      `| Remaining YOTEL GDOs | ${after.length} |`,
      `| Local research preserved | ${localAfter.count} |`,
      `| Failed batches | ${ledger.failed.length} |`,
      "",
      `- Off-git backup: \`${backupPath}\``,
      `- Other hotels: not deleted`,
      `- Rock Creek: not applied`,
      `- General production writes: still fail-closed`,
      "",
    ].join("\n"),
  );

  console.log(
    JSON.stringify(
      {
        ok: deletedOk,
        deleted: ledger.deletedIds.length,
        remainingYotel: after.length,
        local42: localAfter.count,
        backupPath,
        failedBatches: ledger.failed.length,
      },
      null,
      2,
    ),
  );

  if (!deletedOk) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
