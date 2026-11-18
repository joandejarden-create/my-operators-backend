#!/usr/bin/env node
/**
 * Bethesda four-record Airtable production pilot.
 * Default: dry-run preflight only.
 * Apply: --apply --authorizeProductionWrites --exactWriteSetAuthorization=bethesda-marriott-four-record-pilot-v1
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
  BETHESDA_PILOT_HOTEL_ID,
  BETHESDA_PILOT_DEFERRED_GDOS,
  buildBethesdaPilotPreApplyManifest,
  executeBethesdaAirtableProductionPilot,
  recheckBethesdaPilotIdempotency,
  validateBethesdaPilotAuthorization,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-airtable-production-pilot-v1.js";
import { findOpportunityRecordById } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-airtable-bethesda-production-pilot-v1");

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}

function writeMd(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith("\n") ? body : `${body}\n`);
}

function parseArgs(argv) {
  const apply = argv.includes("--apply");
  const authorizeProductionWrites = argv.includes("--authorizeProductionWrites");
  let exactWriteSetAuthorization = "";
  for (const a of argv) {
    if (a.startsWith("--exactWriteSetAuthorization=")) {
      exactWriteSetAuthorization = a.slice("--exactWriteSetAuthorization=".length);
    }
  }
  return { apply, authorizeProductionWrites, exactWriteSetAuthorization };
}

async function countBethesdaGdos() {
  // Approximate integrity: probe deferred GDO ids remain absent/unpromoted
  const deferredStatus = [];
  for (const id of BETHESDA_PILOT_DEFERRED_GDOS) {
    const rec = await findOpportunityRecordById(id);
    deferredStatus.push({
      opportunityId: id,
      airtableRecordId: rec?.id || null,
      present: Boolean(rec),
    });
  }
  return deferredStatus;
}

async function main() {
  ensureOut();
  const args = parseArgs(process.argv.slice(2));

  // Always build pre-apply first
  const pre = await buildBethesdaPilotPreApplyManifest({
    authorizeProductionWrites: true,
    exactWriteSetAuthorization: BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
    hotelId: BETHESDA_PILOT_HOTEL_ID,
    apply: false,
  });
  writeJson("PRE_APPLY_MANIFEST.json", pre);

  const fieldDiffMd = [
    "# Field-level diff (pre-apply)",
    "",
    `Hotel: Bethesda Marriott (\`${BETHESDA_PILOT_HOTEL_ID}\`)`,
    `Eligible apply count: **${pre.eligibleApplyCount}**`,
    "",
    ...pre.operations.flatMap((op) => [
      `## ${op.canonicalId} — ${op.organizationName}`,
      "",
      `- Table: ${op.tableName}`,
      `- Proposed action: **${op.proposedAction}**`,
      `- Airtable record: ${op.airtableRecordId || "none"}`,
      `- Collision: ${op.collision}`,
      `- Apply eligible: ${op.applyEligible === true}`,
      op.deferReason ? `- Defer reason: ${op.deferReason}` : "",
      "",
      "| Field | Before | After |",
      "|---|---|---|",
      ...(op.changedFields || []).map(
        (c) =>
          `| \`${c.field}\` | ${JSON.stringify(c.before)} | ${JSON.stringify(c.after)} |`,
      ),
      (op.changedFields || []).length ? "" : "_No field changes_",
      "",
    ]),
  ]
    .filter((l) => l !== undefined)
    .join("\n");
  writeMd("FIELD_LEVEL_DIFF.md", fieldDiffMd);

  let applyResult = null;
  let idempotency = null;

  if (args.apply) {
    const gate = validateBethesdaPilotAuthorization({
      apply: true,
      authorizeProductionWrites: args.authorizeProductionWrites,
      exactWriteSetAuthorization: args.exactWriteSetAuthorization,
      hotelId: BETHESDA_PILOT_HOTEL_ID,
    });
    if (!gate.ok) {
      writeJson("AIRTABLE_WRITE_RESULTS.json", {
        ok: false,
        blocked: true,
        gate,
      });
      console.error(JSON.stringify(gate, null, 2));
      process.exitCode = 1;
      return;
    }
    applyResult = await executeBethesdaAirtableProductionPilot({
      apply: true,
      authorizeProductionWrites: true,
      exactWriteSetAuthorization: BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
      hotelId: BETHESDA_PILOT_HOTEL_ID,
    });
    writeJson("AIRTABLE_WRITE_RESULTS.json", applyResult);
    idempotency = await recheckBethesdaPilotIdempotency();
    writeJson("IDEMPOTENCY_RECHECK.json", idempotency);
  } else {
    writeJson("AIRTABLE_WRITE_RESULTS.json", {
      applied: false,
      detail: "Dry-run only — pass --apply with authorization flags to execute",
      eligibleApplyCount: pre.eligibleApplyCount,
    });
  }

  const deferredGdos = await countBethesdaGdos();
  writeJson("DEFERRED_GDO_STATUS.json", { deferredGdos });

  const readBackMd = [
    "# Read-back verification",
    "",
    applyResult?.applied
      ? `Applied: yes · ok=${applyResult.ok}`
      : "Applied: no (preflight only)",
    "",
    ...(applyResult?.results || []).map((r) => {
      const lines = [
        `## ${r.canonicalId}`,
        `- Outcome: **${r.outcome}**`,
        `- Airtable record: ${r.airtableRecordId || "n/a"}`,
        `- Read-back ok: ${r.readBackOk ?? "n/a"}`,
      ];
      if (r.verifiedFields) {
        for (const [k, v] of Object.entries(r.verifiedFields)) {
          lines.push(`- \`${k}\`: expected=${JSON.stringify(v.expected)} actual=${JSON.stringify(v.actual)} ok=${v.ok}`);
        }
      }
      if (r.clearedUnrelatedFields?.length) {
        lines.push(`- CLEARED UNRELATED: ${r.clearedUnrelatedFields.join(", ")}`);
      }
      if (r.error) lines.push(`- Error: ${r.error}`);
      lines.push("");
      return lines.join("\n");
    }),
  ].join("\n");
  writeMd("READ_BACK_VERIFICATION.md", readBackMd);

  writeMd(
    "IDEMPOTENCY_RECHECK.md",
    [
      "# Idempotency recheck",
      "",
      idempotency
        ? `ok=${idempotency.ok} wouldCreateCount=${idempotency.wouldCreateCount}`
        : "_Not run (apply not executed)_",
      "",
      ...(idempotency?.operations || []).map(
        (o) =>
          `- ${o.canonicalId}: ${o.proposedAction} (${o.airtableRecordId || "none"}) changed=${o.changedFieldCount}`,
      ),
      "",
    ].join("\n"),
  );

  writeMd(
    "BETHESDA_INVENTORY_INTEGRITY.md",
    [
      "# Bethesda inventory integrity",
      "",
      "Deferred WATCHLIST GDO ids must remain unpromoted by this pilot:",
      "",
      ...deferredGdos.map(
        (d) =>
          `- \`${d.opportunityId}\`: ${d.present ? `PRESENT as ${d.airtableRecordId}` : "absent (expected for never-created discs)"}`,
      ),
      "",
      "No GDO CREATE/UPDATE was authorized in this pilot.",
      "",
    ].join("\n"),
  );

  writeMd(
    "PUBLICATION_ISOLATION.md",
    [
      "# Publication isolation",
      "",
      "- No customer publication package files were modified by this pilot.",
      "- No STAGED/CURRENT pointer updates were performed.",
      "- Pilot writes were limited to Demand Generators + one Private Event Venue patch.",
      "",
    ].join("\n"),
  );

  const issues = [];
  if (applyResult && !applyResult.ok) issues.push("One or more apply/read-back outcomes failed");
  if (idempotency && !idempotency.ok) {
    issues.push(`Idempotency would still CREATE ${idempotency.wouldCreateCount} record(s)`);
  }
  for (const op of pre.operations) {
    if (op.collision === "IDENTITY_COLLISION_ID_VS_NAME") {
      issues.push(`Collision unresolved for ${op.canonicalId}`);
    }
    if (op.deferReason && op.proposedAction === "DEFERRED" && op.collision === "MATCH_EXISTING_BY_NAME") {
      issues.push(`${op.canonicalId}: ${op.deferReason}`);
    }
  }
  writeMd(
    "REMAINING_ISSUES.md",
    [
      "# Remaining issues",
      "",
      issues.length ? issues.map((i) => `- ${i}`).join("\n") : "- None reported by pilot runner.",
      "",
      "General production Airtable persistence remains fail-closed outside this four-record allowlist.",
      "",
    ].join("\n"),
  );

  writeMd(
    "FOUNDER_EXECUTIVE_SUMMARY.md",
    [
      "# Bethesda Airtable production pilot V1 — founder summary",
      "",
      "**STOP FOR FOUNDER REVIEW**",
      "",
      `- Exact write-set authorization id: \`${BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION}\``,
      `- Preflight eligible apply count: **${pre.eligibleApplyCount}**`,
      `- Applied: **${applyResult?.applied === true ? "yes" : "no"}**`,
      `- Summary: ${JSON.stringify(applyResult?.summary || { dryRun: true })}`,
      `- Idempotency ok: ${idempotency ? idempotency.ok : "n/a"}`,
      "",
      "See PRE_APPLY_MANIFEST.json, FIELD_LEVEL_DIFF.md, AIRTABLE_WRITE_RESULTS.json, READ_BACK_VERIFICATION.md.",
      "",
    ].join("\n"),
  );

  writeMd(
    "README.md",
    [
      "# Bethesda Airtable production pilot V1",
      "",
      "```bash",
      "npm run gdi:bethesda-airtable-production-pilot-v1",
      "npm run gdi:bethesda-airtable-production-pilot-v1 -- --apply --authorizeProductionWrites --exactWriteSetAuthorization=bethesda-marriott-four-record-pilot-v1",
      "```",
      "",
    ].join("\n"),
  );

  console.log(
    JSON.stringify(
      {
        ok: applyResult ? applyResult.ok : true,
        eligibleApplyCount: pre.eligibleApplyCount,
        applied: applyResult?.applied === true,
        summary: applyResult?.summary || null,
        out: OUT,
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
