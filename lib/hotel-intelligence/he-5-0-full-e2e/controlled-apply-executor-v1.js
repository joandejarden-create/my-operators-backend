/**
 * Controlled HE 5.0 pilot Airtable apply executor.
 * Read-before-write, duplicate-safe CREATE, evidence dedupe, read-back.
 * Blocks legacy Hotel Census. Default dryRun=true unless apply=true.
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import {
  applyHpcNullFill,
  fetchHpcRecord,
  fetchLiveHpcSchema,
} from "../../hotel-census/hpc-shared-upsert-v1.js";
import { performLiveHpcIdentityCollisionCheck } from "../../hotel-census/hpc-live-identity-collision-v1.js";
import { isFieldEmpty } from "../../hotel-census/hpc-full-onboarding-contract-v1.js";
import {
  assertLegacyHotelCensusAccess,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
} from "../hpc-population/legacy-hotel-census-access-guard-v1.js";
import { buildHe50DryRunMutationPackage } from "./dry-run-mutation-package.js";
import { classifyPilotMutationPackage, HE50_CONDITIONAL_APPROVAL_V1 } from "./conditional-approval-v1.js";
import {
  APPROVAL_CLASS,
  HE50_STANDING_APPLY_GATE,
  PERSISTENCE_STAGE,
  isLegacyHotelCensusDestination,
} from "./standing-authorization-v1.js";
import { HE50_HPC_TABLE_ID, HE50_PILOT_RUN_ID } from "./contract.js";

export const HE50_CONTROLLED_APPLY_V1 = "HE50_CONTROLLED_APPLY_V1";

const HPC_TABLE_NAME = "Hotel Property Census";
const EVIDENCE_TABLE_NAME = "HI Graph Evidence";

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function platformBaseId() {
  return (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
}

async function airtableFetch(pathOrUrl, opts = {}) {
  const token = airtableToken();
  const baseId = platformBaseId();
  if (!token || !baseId) {
    const err = new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
    err.code = "HPC_CONFIG_MISSING";
    throw err;
  }
  // Guard: never hit legacy census
  if (isLegacyHotelCensusDestination(pathOrUrl) || String(pathOrUrl).includes(LEGACY_HOTEL_CENSUS_TABLE_ID)) {
    assertLegacyHotelCensusAccess(pathOrUrl, {
      method: opts.method || "GET",
      workflow: "he50-controlled-apply",
      forceBlock: true,
    });
  }
  const url = pathOrUrl.startsWith("http")
    ? pathOrUrl
    : `https://api.airtable.com/v0/${baseId}/${pathOrUrl}`;
  const res = await fetch(url, {
    ...opts,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(
      `Airtable ${res.status}: ${json?.error?.message || JSON.stringify(json).slice(0, 200)}`
    );
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

async function findEvidenceById(evidenceId) {
  const esc = String(evidenceId || "").replace(/'/g, "\\'");
  const formula = `{Evidence ID}='${esc}'`;
  const q = new URLSearchParams({
    filterByFormula: formula,
    maxRecords: "1",
  });
  const j = await airtableFetch(
    `${encodeURIComponent(EVIDENCE_TABLE_NAME)}?${q}`
  );
  return (j.records || [])[0] || null;
}

async function executeOperation(op, { dryRun }) {
  if (op.kind === "HPC_NULL_FILL") {
    // Filter patch to still-empty fields after live read (also done inside applyHpcNullFill)
    const before = await fetchHpcRecord(op.recordId);
    if (!before) {
      return {
        ok: false,
        kind: op.kind,
        error: "record_not_found",
        recordId: op.recordId,
      };
    }
    const patch = {};
    for (const [k, v] of Object.entries(op.patch || {})) {
      if (v == null || v === "") continue;
      // Official Property URL: null-fill only; Rooms / Opening Date: null-fill only
      if (isFieldEmpty(before.fields?.[k])) patch[k] = v;
    }
    if (!Object.keys(patch).length) {
      return {
        ok: true,
        kind: op.kind,
        skipped: true,
        reason: "all_fields_already_populated",
        recordId: op.recordId,
        beforeSnapshot: pick(before.fields, Object.keys(op.patch || {})),
        stages: [PERSISTENCE_STAGE.RESEARCH_VERIFIED],
      };
    }
    const result = await applyHpcNullFill({
      recordId: op.recordId,
      patch,
      dryRun,
      reason: `he50_standing_auth:${op.mappedFrom || "null_fill"}`,
    });
    return {
      ...result,
      kind: op.kind,
      stages: dryRun
        ? [PERSISTENCE_STAGE.RESEARCH_VERIFIED]
        : result.ok
          ? [
              PERSISTENCE_STAGE.RESEARCH_VERIFIED,
              PERSISTENCE_STAGE.CANONICAL_PERSISTED,
              PERSISTENCE_STAGE.AIRTABLE_READBACK_VALIDATED,
            ]
          : [PERSISTENCE_STAGE.RESEARCH_VERIFIED],
    };
  }

  if (op.kind === "HPC_CREATE") {
    const collision = await performLiveHpcIdentityCollisionCheck({
      hotelName: op.collisionName,
      aliases: op.aliases || [],
      city: op.city,
      country: op.country,
      brand: op.brand,
    });
    if (
      collision?.outcome === "HPC_EXISTING_RECORD_VERIFIED" &&
      collision.hpcRecordId
    ) {
      return {
        ok: true,
        kind: op.kind,
        skipped: true,
        reason: "collision_matched_existing_hpc",
        recordId: collision.hpcRecordId,
        stages: [PERSISTENCE_STAGE.RESEARCH_VERIFIED],
      };
    }
    if (collision?.outcome === "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW") {
      return {
        ok: false,
        kind: op.kind,
        quarantined: true,
        reason: "ambiguous_collision",
        candidates: (collision.candidates || []).slice(0, 5),
      };
    }
    if (dryRun) {
      return {
        ok: true,
        kind: op.kind,
        dryRun: true,
        wouldCreate: op.fields,
        idempotencyKey: op.idempotencyKey,
        stages: [PERSISTENCE_STAGE.RESEARCH_VERIFIED],
      };
    }
    // Uncertain network: do not blind-retry CREATE — single attempt then re-collision
    let created;
    try {
      created = await airtableFetch(encodeURIComponent(HPC_TABLE_NAME), {
        method: "POST",
        body: JSON.stringify({ fields: op.fields, typecast: true }),
      });
    } catch (err) {
      // Reconcile whether record appeared
      const again = await performLiveHpcIdentityCollisionCheck({
        hotelName: op.collisionName,
        city: op.city,
        country: op.country,
        brand: op.brand,
      });
      if (again?.hpcRecordId) {
        return {
          ok: true,
          kind: op.kind,
          recordId: again.hpcRecordId,
          reconciledAfterError: true,
          error: err.message,
          stages: [
            PERSISTENCE_STAGE.CANONICAL_PERSISTED,
            PERSISTENCE_STAGE.AIRTABLE_READBACK_VALIDATED,
          ],
        };
      }
      throw err;
    }
    const id = created.id;
    const after = await fetchHpcRecord(id);
    return {
      ok: Boolean(after?.id),
      kind: op.kind,
      recordId: id,
      written: op.fields,
      readback: {
        name: after?.fields?.["Property Name"],
        rooms: after?.fields?.["Rooms / Keys"],
        brand: after?.fields?.["Current Brand"],
      },
      legacyProvenanceId: op.legacyProvenanceId || null,
      stages: [
        PERSISTENCE_STAGE.RESEARCH_VERIFIED,
        PERSISTENCE_STAGE.CANONICAL_PERSISTED,
        PERSISTENCE_STAGE.AIRTABLE_READBACK_VALIDATED,
      ],
    };
  }

  if (op.kind === "HI_GRAPH_EVIDENCE_CREATE") {
    const existing = await findEvidenceById(op.fields["Evidence ID"]);
    if (existing) {
      return {
        ok: true,
        kind: op.kind,
        skipped: true,
        reason: "evidence_id_already_exists",
        recordId: existing.id,
        stages: [PERSISTENCE_STAGE.RESEARCH_VERIFIED],
      };
    }
    if (dryRun) {
      return {
        ok: true,
        kind: op.kind,
        dryRun: true,
        wouldCreate: op.fields,
        stages: [PERSISTENCE_STAGE.RESEARCH_VERIFIED],
      };
    }
    const fields = { ...op.fields };
    if (!fields["Source URL"]) delete fields["Source URL"];
    const created = await airtableFetch(encodeURIComponent(EVIDENCE_TABLE_NAME), {
      method: "POST",
      body: JSON.stringify({ fields, typecast: true }),
    });
    return {
      ok: true,
      kind: op.kind,
      recordId: created.id,
      written: fields,
      stages: [
        PERSISTENCE_STAGE.RESEARCH_VERIFIED,
        PERSISTENCE_STAGE.CANONICAL_PERSISTED,
        PERSISTENCE_STAGE.AIRTABLE_READBACK_VALIDATED,
      ],
    };
  }

  return { ok: false, kind: op.kind, error: "unknown_operation_kind" };
}

function pick(fields, keys) {
  const out = {};
  for (const k of keys) out[k] = fields?.[k] ?? null;
  return out;
}

/**
 * @param {object} [opts]
 * @param {boolean} [opts.apply=false] When true, perform Airtable writes under standing auth.
 * @param {object[]} [opts.mutations] Optional override package
 * @param {string} [opts.outDir]
 */
export async function runHe50ControlledApply(opts = {}) {
  const dryRun = opts.apply !== true;
  const startedAt = new Date().toISOString();

  // Document that legacy census is refused (do not abort the pilot — classification already blocks).
  let legacyBlockedConfirmed = false;
  try {
    assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
      method: "PATCH",
      workflow: "he50-controlled-apply-preflight",
      forceBlock: true,
    });
  } catch (err) {
    legacyBlockedConfirmed = err?.code === "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED";
    if (!legacyBlockedConfirmed) throw err;
  }

  const pack = opts.mutations
    ? { mutations: opts.mutations }
    : buildHe50DryRunMutationPackage();
  const classifiedPack = classifyPilotMutationPackage(pack.mutations || []);

  let schema = null;
  try {
    schema = await fetchLiveHpcSchema();
  } catch (err) {
    schema = { error: err.message };
  }

  const results = [];
  for (const item of classifiedPack.classified) {
    if (!item.executable) {
      results.push({
        mutationId: item.mutationId,
        approvalClass: item.approvalClass,
        applied: false,
        skipped: true,
        quarantineReason: item.quarantineReason,
      });
      continue;
    }

    const opResults = [];
    let failed = false;
    for (const op of item.operations) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const r = await executeOperation(op, { dryRun });
        opResults.push(r);
        if (r.ok === false && !r.skipped) failed = true;
      } catch (err) {
        failed = true;
        opResults.push({
          ok: false,
          kind: op.kind,
          error: err.message,
          status: err.status,
        });
      }
    }

    results.push({
      mutationId: item.mutationId,
      approvalClass: item.approvalClass,
      pilotKey: item.pilotKey,
      applied: !dryRun && !failed && opResults.some((r) => r.ok && !r.skipped && !r.dryRun),
      dryRun,
      failed,
      opResults,
    });
  }

  const applied = results.filter((r) => r.applied);
  const quarantined = results.filter(
    (r) =>
      r.approvalClass === APPROVAL_CLASS.QUARANTINED_INSUFFICIENT_EVIDENCE ||
      r.approvalClass === APPROVAL_CLASS.BLOCKED_OUT_OF_SCOPE ||
      r.approvalClass === APPROVAL_CLASS.FOUNDER_APPROVAL_REQUIRED ||
      r.failed
  );
  const skipped = results.filter(
    (r) => r.approvalClass === APPROVAL_CLASS.SKIPPED_NO_CHANGE || r.opResults?.every?.((o) => o.skipped)
  );

  let reportStatus = HE50_STANDING_APPLY_GATE.reportStatuses.FULL;
  if (quarantined.length && applied.length) {
    reportStatus = HE50_STANDING_APPLY_GATE.reportStatuses.PARTIAL;
  } else if (!applied.length && quarantined.some((q) => q.approvalClass === APPROVAL_CLASS.FOUNDER_APPROVAL_REQUIRED)) {
    reportStatus = HE50_STANDING_APPLY_GATE.reportStatuses.OUT_OF_SCOPE;
  } else if (!applied.length && dryRun) {
    reportStatus = "DRY_RUN — STANDING AUTH CLASSIFICATION COMPLETE (no writes)";
  } else if (!applied.length && quarantined.length) {
    reportStatus = HE50_STANDING_APPLY_GATE.reportStatuses.PARTIAL;
  } else if (!applied.length && skipped.length === results.length) {
    // Idempotent re-run: every mutation already persisted / no-op — not a quarantine failure.
    reportStatus = HE50_STANDING_APPLY_GATE.reportStatuses.FULL;
  } else if (!applied.length) {
    reportStatus = HE50_STANDING_APPLY_GATE.reportStatuses.PARTIAL;
  }

  const report = {
    contract: HE50_CONTROLLED_APPLY_V1,
    approvalContract: HE50_CONDITIONAL_APPROVAL_V1,
    standingAuth: HE50_STANDING_APPLY_GATE,
    pilotRunId: HE50_PILOT_RUN_ID,
    startedAt,
    completedAt: new Date().toISOString(),
    dryRun,
    applyRequested: opts.apply === true,
    hpcTableId: HE50_HPC_TABLE_ID,
    schemaFieldCount: schema?.fieldCount ?? null,
    classificationSummary: classifiedPack.summary,
    results,
    counts: {
      total: results.length,
      applied: applied.length,
      quarantined: quarantined.length,
      skipped: skipped.length,
      createdHpc: results.filter((r) =>
        r.opResults?.some((o) => o.kind === "HPC_CREATE" && o.recordId && !o.skipped && !o.dryRun)
      ).length,
      updatedHpc: results.filter((r) =>
        r.opResults?.some((o) => o.kind === "HPC_NULL_FILL" && o.written && !o.dryRun)
      ).length,
      evidenceCreated: results.filter((r) =>
        r.opResults?.some(
          (o) => o.kind === "HI_GRAPH_EVIDENCE_CREATE" && o.recordId && !o.skipped && !o.dryRun
        )
      ).length,
    },
    reportStatus,
    legacyHotelCensusAccess: legacyBlockedConfirmed ? "BLOCKED_CONFIRMED" : "BLOCKED",
  };

  if (opts.outDir) {
    fs.mkdirSync(opts.outDir, { recursive: true });
    const name = dryRun
      ? "HE50-CONTROLLED-APPLY-DRY-RUN-V1.json"
      : "HE50-CONTROLLED-APPLY-RESULT-V1.json";
    fs.writeFileSync(path.join(opts.outDir, name), JSON.stringify(report, null, 2));
  }

  return report;
}
