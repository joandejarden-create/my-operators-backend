/**
 * GDI hotel store integrity guard V1.
 *
 * Prevents silent loss of richer local operational hotel stores
 * (e.g. Bethesda 59 → 54 after git checkout replaced local with committed snapshot).
 *
 * Does not write opportunities.json unless explicitly authorized.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const GDI_HOTEL_STORE_INTEGRITY_GUARD_V1 =
  "gdi-hotel-store-integrity-guard-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

/** Bethesda Scaling Companies IDs lost in the 59→54 regression (verified 2026-11-12). */
export const BETHESDA_PROTECTED_SCALING_IDS = Object.freeze([
  "gdi_sc_b36b66547864e8ce", // Matthews
  "gdi_sc_a5768a99d4e89875", // IMC Construction
  "gdi_sc_e7dd46732dd5bb7a", // BioHub Maryland
  "gdi_sc_5c5d41eb670e6c7b", // Miles & Stockbridge
  "gdi_sc_9d14aee7ff250b11", // REGENXBIO
]);

export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";
export const BETHESDA_EXPECTED_CANONICAL_COUNT = 59;
export const BETHESDA_EXPECTED_HISTORICAL_COUNT = 29;
export const BETHESDA_EXPECTED_UNION_COUNT = 88;

/** Authorized-backup source SHA used for the five-SC merge-add restore. */
export const VERIFIED_59_ROW_SHA256 =
  "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad";

/** Live store SHA after approved merge-add of five Scaling Companies IDs (PR #182). */
export const RESTORED_LIVE_59_ROW_SHA256 =
  "fd4539d9dd850c12a3093dd7b4e75039e5db67fadcd5e2c5ae5de09c68d00e44";

/** Live store SHA after founder-approved Woman's Club contact patch (2026-11-12). */
export const WOMANS_CLUB_CONTACT_PATCHED_59_ROW_SHA256 =
  "a298d6b8ac2e65a39a5ea8bd3a2bccbed55a7d024c5c6f3ea54622e2291a1f58";

/** Live store SHA after founder-approved 22 canonical title-only repairs (2026-11-12). */
export const TITLE_REPAIRED_59_ROW_SHA256 =
  "1279f40c2d1cdd2cb6efdc55bfa8397b44f65c58da3ca4365f2814c69e71c9c3";

/** Live store SHA after founder-approved 12 PASS canonical contact patches (2026-11-12). */
export const CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256 =
  "6a97feda920615bb53730a40968b65b0e6c6967f9310f4f83eda9b0b27e115b2";

/** Live store SHA after Wave 2 founder-approved four-ID contact patches (NADO×2, HRHR, NRC RIC; ACVNU deferred). */
export const WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256 =
  "838af4693fe214bb8aee4c79b93a260d24fafccd4654135c32e87bd73c8f0c12";

export const ACCEPTABLE_59_ROW_SHA256S = Object.freeze([
  VERIFIED_59_ROW_SHA256,
  RESTORED_LIVE_59_ROW_SHA256,
  WOMANS_CLUB_CONTACT_PATCHED_59_ROW_SHA256,
  TITLE_REPAIRED_59_ROW_SHA256,
  CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function hotelOpportunitiesPath(hotelId, { root = ROOT } = {}) {
  return path.join(
    root,
    "data/group-demand-intelligence/hotels",
    String(hotelId),
    "opportunities.json"
  );
}

export function sha256File(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

export function loadHotelOpportunityDoc(hotelId, { root = ROOT } = {}) {
  const p = hotelOpportunitiesPath(hotelId, { root });
  if (!fs.existsSync(p)) return { path: p, exists: false, opportunities: [] };
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  const opportunities = Array.isArray(doc)
    ? doc
    : Array.isArray(doc.opportunities)
      ? doc.opportunities
      : [];
  return {
    path: p,
    exists: true,
    doc,
    opportunities,
    ids: opportunities.map((r) => text(r.id || r.opportunityId)).filter(Boolean),
    sha256: sha256File(p),
    count: opportunities.length,
  };
}

export function assertNoProtectedIdLoss({
  hotelId,
  nextOpportunities = [],
  protectedIds = [],
} = {}) {
  const nextIds = new Set(
    (nextOpportunities || []).map((r) => text(r.id || r.opportunityId)).filter(Boolean)
  );
  const missing = (protectedIds || []).filter((id) => !nextIds.has(text(id)));
  if (missing.length) {
    return {
      ok: false,
      code: "PROTECTED_ID_LOSS_REFUSED",
      hotelId,
      missing,
    };
  }
  return { ok: true, code: "PROTECTED_IDS_PRESENT", hotelId };
}

export function assertCountNotDropped({
  hotelId,
  priorCount,
  nextCount,
  allowDrop = false,
} = {}) {
  if (allowDrop) return { ok: true, code: "DROP_EXPLICITLY_ALLOWED" };
  if (Number(nextCount) < Number(priorCount)) {
    return {
      ok: false,
      code: "OPPORTUNITY_COUNT_DROP_REFUSED",
      hotelId,
      priorCount,
      nextCount,
      dropped: Number(priorCount) - Number(nextCount),
    };
  }
  return { ok: true, code: "COUNT_OK", priorCount, nextCount };
}

/**
 * Backup a hotel opportunities.json before any authorized mutation.
 */
export function backupHotelOpportunityStore(
  hotelId,
  { root = ROOT, label = "pre-mutation" } = {}
) {
  const loaded = loadHotelOpportunityDoc(hotelId, { root });
  if (!loaded.exists) {
    return { ok: false, code: "STORE_ABSENT", path: loaded.path };
  }
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const backupDir = path.join(
    root,
    "data/group-demand-intelligence/hotel-store-backups",
    String(hotelId)
  );
  fs.mkdirSync(backupDir, { recursive: true });
  const backupPath = path.join(
    backupDir,
    `${label}-${stamp}-${loaded.sha256.slice(0, 12)}.json`
  );
  fs.copyFileSync(loaded.path, backupPath);
  return {
    ok: true,
    code: "BACKED_UP",
    hotelId,
    sourcePath: loaded.path,
    backupPath,
    sha256: loaded.sha256,
    count: loaded.count,
  };
}

/**
 * Dry-run merge: add missing protected bodies into current store without writing.
 */
export function dryRunMergeProtectedBodies({
  hotelId = BETHESDA_HOTEL_ID,
  recoveredBodies = [],
  protectedIds = BETHESDA_PROTECTED_SCALING_IDS,
  root = ROOT,
} = {}) {
  const loaded = loadHotelOpportunityDoc(hotelId, { root });
  if (!loaded.exists) {
    return { ok: false, code: "STORE_ABSENT", path: loaded.path };
  }
  const byId = new Map(
    loaded.opportunities.map((r) => [text(r.id || r.opportunityId), r])
  );
  const added = [];
  const skippedPresent = [];
  for (const body of recoveredBodies) {
    const id = text(body?.id || body?.opportunityId);
    if (!id) continue;
    if (!protectedIds.includes(id)) {
      return {
        ok: false,
        code: "BODY_NOT_ON_PROTECTED_ALLOWLIST",
        id,
      };
    }
    if (byId.has(id)) {
      skippedPresent.push(id);
      continue;
    }
    byId.set(id, body);
    added.push(id);
  }
  const missingStill = protectedIds.filter((id) => !byId.has(id));
  const opportunities = [...byId.values()];
  const countGate = assertCountNotDropped({
    hotelId,
    priorCount: loaded.count,
    nextCount: opportunities.length,
  });
  const idGate = assertNoProtectedIdLoss({
    hotelId,
    nextOpportunities: opportunities,
    protectedIds,
  });
  return {
    ok: countGate.ok && idGate.ok && missingStill.length === 0,
    code:
      missingStill.length === 0
        ? "DRY_RUN_MERGE_READY"
        : "DRY_RUN_MERGE_INCOMPLETE_MISSING_BODIES",
    hotelId,
    priorCount: loaded.count,
    priorSha256: loaded.sha256,
    proposedCount: opportunities.length,
    added,
    skippedPresent,
    missingStill,
    countGate,
    idGate,
    expectedCanonical: BETHESDA_EXPECTED_CANONICAL_COUNT,
    expectedUnion: BETHESDA_EXPECTED_UNION_COUNT,
    writeAuthorized: false,
    opportunities, // caller may persist only with explicit founder approval
  };
}

/**
 * Detect if a candidate/replacement store would drop IDs present locally.
 */
export function compareLocalVsCandidateStore({
  hotelId,
  candidateOpportunities = [],
  protectedIds = BETHESDA_PROTECTED_SCALING_IDS,
  root = ROOT,
} = {}) {
  const local = loadHotelOpportunityDoc(hotelId, { root });
  const candidateIds = new Set(
    (candidateOpportunities || [])
      .map((r) => text(r.id || r.opportunityId))
      .filter(Boolean)
  );
  const localOnly = (local.ids || []).filter((id) => !candidateIds.has(id));
  const protectedLost = protectedIds.filter((id) => local.ids.includes(id) && !candidateIds.has(id));
  const richerLocal =
    local.count > (candidateOpportunities || []).length || protectedLost.length > 0;
  return {
    hotelId,
    localCount: local.count,
    candidateCount: (candidateOpportunities || []).length,
    localOnlyIds: localOnly,
    protectedLost,
    richerLocal,
    refuseReplace: richerLocal,
    code: richerLocal ? "REFUSE_REPLACE_RICHER_LOCAL" : "CANDIDATE_OK",
  };
}

export function listTrackedHotelOpportunityRelPaths({ root = ROOT } = {}) {
  const hotelsDir = path.join(root, "data/group-demand-intelligence/hotels");
  if (!fs.existsSync(hotelsDir)) return [];
  return fs
    .readdirSync(hotelsDir, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => path.join("data/group-demand-intelligence/hotels", d.name, "opportunities.json"))
    .filter((rel) => fs.existsSync(path.join(root, rel)));
}

export const BETHESDA_SC_MERGE_ADD_APPROVAL_TOKEN =
  "FOUNDER_APPROVE_BETHESDA_SC_MERGE_ADD_5_IDS_20261112";

/**
 * Authorized merge-add of protected bodies. Fail-closed without exact approval token.
 * Preserves all current records; adds only missing allowlisted IDs.
 */
export function applyAuthorizedMergeAddProtectedBodies({
  hotelId = BETHESDA_HOTEL_ID,
  recoveredBodies = [],
  protectedIds = BETHESDA_PROTECTED_SCALING_IDS,
  approvalToken = null,
  expectPriorCount = 54,
  expectPriorAbsentIds = BETHESDA_PROTECTED_SCALING_IDS,
  root = ROOT,
  label = "bethesda-sc-merge-add",
} = {}) {
  if (approvalToken !== BETHESDA_SC_MERGE_ADD_APPROVAL_TOKEN) {
    return {
      ok: false,
      code: "APPROVAL_TOKEN_REQUIRED",
      writeApplied: false,
    };
  }

  const loaded = loadHotelOpportunityDoc(hotelId, { root });
  if (!loaded.exists) {
    return { ok: false, code: "STORE_ABSENT", writeApplied: false, path: loaded.path };
  }
  if (loaded.count !== Number(expectPriorCount)) {
    return {
      ok: false,
      code: "PRIOR_COUNT_MISMATCH",
      writeApplied: false,
      priorCount: loaded.count,
      expectPriorCount,
    };
  }
  const presentForbidden = (expectPriorAbsentIds || []).filter((id) =>
    loaded.ids.includes(text(id))
  );
  if (presentForbidden.length) {
    return {
      ok: false,
      code: "PROTECTED_IDS_ALREADY_PRESENT",
      writeApplied: false,
      presentForbidden,
    };
  }

  const dry = dryRunMergeProtectedBodies({
    hotelId,
    recoveredBodies,
    protectedIds,
    root,
  });
  if (!dry.ok) {
    return { ...dry, writeApplied: false };
  }
  if (dry.proposedCount !== BETHESDA_EXPECTED_CANONICAL_COUNT) {
    return {
      ok: false,
      code: "PROPOSED_COUNT_NOT_59",
      writeApplied: false,
      proposedCount: dry.proposedCount,
    };
  }
  if (dry.added.length !== protectedIds.length) {
    return {
      ok: false,
      code: "ADDED_COUNT_MISMATCH",
      writeApplied: false,
      added: dry.added,
    };
  }

  // Schema / identity validation for recovered bodies.
  for (const body of recoveredBodies) {
    const id = text(body?.id || body?.opportunityId);
    if (!protectedIds.includes(id)) {
      return { ok: false, code: "BODY_NOT_ALLOWLISTED", id, writeApplied: false };
    }
    if (!text(body.organizationName || body.companyName)) {
      return { ok: false, code: "BODY_MISSING_ORG", id, writeApplied: false };
    }
    if (text(body.hotelId) && text(body.hotelId) !== hotelId) {
      return {
        ok: false,
        code: "BODY_HOTEL_MISMATCH",
        id,
        hotelId: body.hotelId,
        writeApplied: false,
      };
    }
  }

  const backup = backupHotelOpportunityStore(hotelId, { root, label: `pre-${label}` });
  if (!backup.ok) {
    return { ok: false, code: "BACKUP_FAILED", backup, writeApplied: false };
  }

  // Preserve current 54 bodies exactly; append recovered bodies (Map iteration keeps current order then adds).
  const priorById = new Map(
    loaded.opportunities.map((r) => [text(r.id || r.opportunityId), r])
  );
  const priorJsonById = new Map(
    [...priorById.entries()].map(([id, row]) => [id, JSON.stringify(row)])
  );
  const recoveredById = new Map(
    recoveredBodies.map((b) => [text(b.id || b.opportunityId), b])
  );

  const merged = [];
  for (const row of loaded.opportunities) {
    merged.push(row); // exact current object reference / content
  }
  for (const id of protectedIds) {
    merged.push(structuredClone(recoveredById.get(id)));
  }

  const nextDoc = {
    ...(loaded.doc && typeof loaded.doc === "object" && !Array.isArray(loaded.doc)
      ? loaded.doc
      : {}),
    hotelId,
    opportunities: merged,
    updatedAt: new Date().toISOString(),
    integrityRepair: {
      version: GDI_HOTEL_STORE_INTEGRITY_GUARD_V1,
      mode: "MERGE_ADD_PROTECTED_IDS",
      approvalToken: BETHESDA_SC_MERGE_ADD_APPROVAL_TOKEN,
      addedIds: [...protectedIds],
      priorCount: loaded.count,
      priorSha256: loaded.sha256,
      backupPath: backup.backupPath,
      appliedAt: new Date().toISOString(),
    },
  };

  // Atomic write
  const targetPath = loaded.path;
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });
  const body = `${JSON.stringify(nextDoc, null, 2)}\n`;
  const tmp = `${targetPath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, body);
  try {
    fs.renameSync(tmp, targetPath);
  } catch (err) {
    if (err?.code === "EPERM" || err?.code === "EEXIST" || err?.code === "EACCES") {
      try {
        fs.writeFileSync(targetPath, body);
      } finally {
        try {
          fs.rmSync(tmp, { force: true });
        } catch {
          /* ignore */
        }
      }
    } else {
      try {
        fs.rmSync(tmp, { force: true });
      } catch {
        /* ignore */
      }
      throw err;
    }
  }

  const after = loadHotelOpportunityDoc(hotelId, { root });
  const unchanged = [];
  const changed = [];
  for (const [id, priorJson] of priorJsonById.entries()) {
    const now = after.opportunities.find((r) => text(r.id || r.opportunityId) === id);
    if (!now) {
      changed.push({ id, issue: "MISSING_AFTER_WRITE" });
      continue;
    }
    if (JSON.stringify(now) === priorJson) unchanged.push(id);
    else changed.push({ id, issue: "CONTENT_CHANGED" });
  }

  const restoredMatch = [];
  for (const id of protectedIds) {
    const expected = JSON.stringify(recoveredById.get(id));
    const now = after.opportunities.find((r) => text(r.id || r.opportunityId) === id);
    restoredMatch.push({
      id,
      present: Boolean(now),
      matchesSource: now ? JSON.stringify(now) === expected : false,
    });
  }

  const allPriorPreserved =
    unchanged.length === loaded.count && changed.length === 0;
  const allRestoredOk = restoredMatch.every((r) => r.present && r.matchesSource);

  return {
    ok: after.count === 59 && allPriorPreserved && allRestoredOk,
    code:
      after.count === 59 && allPriorPreserved && allRestoredOk
        ? "MERGE_ADD_APPLIED"
        : "MERGE_ADD_VERIFY_FAILED",
    writeApplied: true,
    hotelId,
    backup,
    priorCount: loaded.count,
    priorSha256: loaded.sha256,
    afterCount: after.count,
    afterSha256: after.sha256,
    added: [...protectedIds],
    priorPreservedCount: unchanged.length,
    priorChanged: changed,
    restoredMatch,
    targetPath,
  };
}
