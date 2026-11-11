/**
 * ADP_EXTERNAL_SHARE_LIVE_VERIFY_LEDGER_V1
 *
 * Persists independent HTTP verification evidence for Admin Customer Access.
 * Verified is never invented — only EXTERNAL_ACCESS_VERIFIED rows with ok=true
 * and matching publication identity promote the Admin column.
 *
 * Prefer durable volume path so evidence survives image deploys.
 * Fail closed: missing/stale/mismatched entries → Verification required / Error.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync, renameSync } from "fs";
import { dirname, join } from "path";
import { resolveAdpDurablePublishedRoot } from "../durable-publication/resolve-durable-root-v1.js";
import { EXTERNAL_SHARE_ACCESS_STATUS } from "./adp-external-share-access-gate-v1.js";

export const ADP_EXTERNAL_SHARE_LIVE_VERIFY_LEDGER_V1 =
  "ADP_EXTERNAL_SHARE_LIVE_VERIFY_LEDGER_V1";

export const LIVE_VERIFY_LEDGER_FILENAME =
  "external-share-live-verify-v1.json";

const CONSIDERATION_TOLERANCE = 0.05;

function emptyLedger() {
  return {
    version: ADP_EXTERNAL_SHARE_LIVE_VERIFY_LEDGER_V1,
    updatedAt: null,
    hotels: {},
  };
}

export function resolveLiveVerifyLedgerPaths(env = process.env) {
  const durableRoot = resolveAdpDurablePublishedRoot(env);
  const durablePath = join(durableRoot, "registry", LIVE_VERIFY_LEDGER_FILENAME);
  const configPath = join(
    process.cwd(),
    "config/client-share",
    LIVE_VERIFY_LEDGER_FILENAME
  );
  return { durablePath, configPath, durableRoot };
}

export function loadExternalShareLiveVerifyLedger(env = process.env) {
  const { durablePath, configPath } = resolveLiveVerifyLedgerPaths(env);
  for (const p of [durablePath, configPath]) {
    if (!existsSync(p)) continue;
    try {
      const doc = JSON.parse(readFileSync(p, "utf8"));
      if (!doc || typeof doc !== "object") continue;
      return {
        ...emptyLedger(),
        ...doc,
        hotels: doc.hotels && typeof doc.hotels === "object" ? doc.hotels : {},
        _loadedFrom: p,
      };
    } catch {
      continue;
    }
  }
  return { ...emptyLedger(), _loadedFrom: null };
}

/**
 * Return live verify evidence for one property if it still matches the
 * authoritative published identity. Fail closed on identity drift.
 */
export function getLiveExternalShareVerifyForProperty(
  propertyId,
  expectedIdentity = null,
  env = process.env
) {
  const pid = String(propertyId || "").trim();
  if (!pid) return null;
  const doc = loadExternalShareLiveVerifyLedger(env);
  const row = doc.hotels?.[pid];
  if (!row || typeof row !== "object") return null;

  const expectedPeriod = expectedIdentity?.measurementPeriodId
    ? String(expectedIdentity.measurementPeriodId)
    : null;
  const expectedConsideration =
    expectedIdentity?.considerationRate != null
      ? Number(expectedIdentity.considerationRate)
      : null;
  const expectedPdf = expectedIdentity?.pdfArtifactSha256
    ? String(expectedIdentity.pdfArtifactSha256).toLowerCase()
    : null;
  const expectedManifest = expectedIdentity?.publicationManifestSha256
    ? String(expectedIdentity.publicationManifestSha256).toLowerCase()
    : null;
  const expectedFp = expectedIdentity?.methodologyFingerprintSha256
    ? String(expectedIdentity.methodologyFingerprintSha256).toLowerCase()
    : null;

  const reasons = Array.isArray(row.reasons) ? [...row.reasons] : [];
  let ok = row.ok === true;

  if (expectedPeriod && row.periodId && String(row.periodId) !== expectedPeriod) {
    ok = false;
    reasons.push(
      `ledger_period_stale:got_${row.periodId}_expected_${expectedPeriod}`
    );
  }
  if (
    expectedConsideration != null &&
    Number.isFinite(expectedConsideration) &&
    row.consideration != null &&
    Number.isFinite(Number(row.consideration)) &&
    Math.abs(Number(row.consideration) - expectedConsideration) >
      CONSIDERATION_TOLERANCE
  ) {
    ok = false;
    reasons.push(
      `ledger_consideration_stale:got_${row.consideration}_expected_${expectedConsideration}`
    );
  }
  if (
    expectedPdf &&
    row.pdfArtifactSha256 &&
    String(row.pdfArtifactSha256).toLowerCase() !== expectedPdf
  ) {
    ok = false;
    reasons.push("ledger_pdf_sha_stale");
  }
  if (
    expectedManifest &&
    row.publicationManifestSha256 &&
    String(row.publicationManifestSha256).toLowerCase() !== expectedManifest
  ) {
    ok = false;
    reasons.push("ledger_manifest_sha_stale");
  }
  if (
    expectedFp &&
    row.methodologyFingerprintSha256 &&
    String(row.methodologyFingerprintSha256).toLowerCase() !== expectedFp
  ) {
    ok = false;
    reasons.push("ledger_methodology_stale");
  }

  return {
    ok,
    status: ok
      ? EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED
      : row.status ||
        EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_SHARE_ACCESS_BLOCKED,
    reasons,
    verifiedAt: row.verifiedAt || null,
    tokenId: row.tokenId || null,
    tokenFingerprint: row.tokenFingerprint || null,
    urlRedacted: row.urlRedacted || null,
    periodId: row.periodId || null,
    consideration: row.consideration ?? null,
    pdfArtifactSha256: row.pdfArtifactSha256 || null,
    publicationManifestSha256: row.publicationManifestSha256 || null,
    methodologyFingerprintSha256: row.methodologyFingerprintSha256 || null,
    source: doc._loadedFrom || null,
  };
}

/**
 * Invalidate Verified for a property after publication identity changes.
 * Fail closed: Admin falls back to Verification required until re-verify.
 */
export function invalidateLiveVerifyForProperty(propertyId, reason, env = process.env) {
  const pid = String(propertyId || "").trim();
  if (!pid) return { ok: false, error: "property_id_required" };
  const doc = loadExternalShareLiveVerifyLedger(env);
  const prev = doc.hotels[pid] || {};
  doc.hotels[pid] = {
    ...prev,
    ok: false,
    status: EXTERNAL_SHARE_ACCESS_STATUS.VERIFICATION_PENDING,
    reasons: [`invalidated_after_publication:${reason || "identity_changed"}`],
    invalidatedAt: new Date().toISOString(),
  };
  doc.updatedAt = new Date().toISOString();
  return persistLiveVerifyLedger(doc, env);
}

export function upsertLiveVerifyLedgerEntries(entries, env = process.env) {
  const doc = loadExternalShareLiveVerifyLedger(env);
  const now = new Date().toISOString();
  for (const entry of entries || []) {
    const pid = String(entry?.propertyId || "").trim();
    if (!pid) continue;
    doc.hotels[pid] = {
      propertyId: pid,
      ok: entry.ok === true,
      status: entry.status || null,
      reasons: Array.isArray(entry.reasons) ? entry.reasons : [],
      verifiedAt: entry.verifiedAt || now,
      tokenId: entry.tokenId || null,
      tokenFingerprint: entry.tokenFingerprint || null,
      urlRedacted: entry.urlRedacted || null,
      periodId: entry.periodId || null,
      consideration:
        entry.consideration != null ? Number(entry.consideration) : null,
      pdfArtifactSha256: entry.pdfArtifactSha256 || null,
      publicationManifestSha256: entry.publicationManifestSha256 || null,
      methodologyFingerprintSha256:
        entry.methodologyFingerprintSha256 || null,
      deploymentId: entry.deploymentId || null,
    };
  }
  doc.updatedAt = now;
  return persistLiveVerifyLedger(doc, env);
}

export function persistLiveVerifyLedger(doc, env = process.env) {
  const { durablePath, configPath } = resolveLiveVerifyLedgerPaths(env);
  const payload = {
    version: ADP_EXTERNAL_SHARE_LIVE_VERIFY_LEDGER_V1,
    updatedAt: doc.updatedAt || new Date().toISOString(),
    hotels: doc.hotels || {},
  };
  const text = `${JSON.stringify(payload, null, 2)}\n`;
  const written = [];
  for (const target of [durablePath, configPath]) {
    try {
      mkdirSync(dirname(target), { recursive: true });
      const tmp = `${target}.tmp`;
      writeFileSync(tmp, text, "utf8");
      renameSync(tmp, target);
      written.push(target);
    } catch {
      // Prefer durable; config may be read-only on production image.
    }
  }
  if (!written.length) {
    return { ok: false, error: "live_verify_ledger_write_failed", written };
  }
  return { ok: true, written, hotelCount: Object.keys(payload.hotels).length };
}
