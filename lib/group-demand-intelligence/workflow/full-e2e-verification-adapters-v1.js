/**
 * Full E2E local/production verification adapters.
 * Execute supported API checks; never mark passed from not_run/asset-only.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";

function text(v) {
  return String(v || "").trim();
}

/**
 * Probe local GDI opportunities API for a hotel.
 */
export async function runLocalDeliveryVerification({
  hotelId,
  runId = null,
  baseUrl = "http://127.0.0.1:8080",
  dataRevision = null,
  buildSha = null,
  fetchImpl = globalThis.fetch,
} = {}) {
  const url = `${baseUrl.replace(/\/$/, "")}/api/group-demand-intelligence/hotels/${encodeURIComponent(hotelId)}/opportunities`;
  const startedAt = new Date().toISOString();
  try {
    const res = await fetchImpl(url, { method: "GET" });
    const body = await res.json().catch(() => ({}));
    const opps = body.opportunities || [];
    const leak = JSON.stringify(opps).match(
      /trun_|enrichmentId|surfe|parallel\.ai|creditsConsumed/i
    );
    if (!res.ok) {
      return {
        status: "failed",
        machineStatus: "failed",
        hotelId,
        runId,
        verifiedAt: startedAt,
        buildSha,
        dataRevision,
        notes: `local API status ${res.status}`,
        checks: { httpStatus: res.status, count: opps.length },
      };
    }
    return {
      status: "passed",
      machineStatus: "passed",
      hotelId,
      runId,
      verifiedAt: startedAt,
      buildSha,
      dataRevision: dataRevision || body.updatedAt || null,
      notes: `local list count=${opps.length}; leaks=${leak ? 1 : 0}`,
      checks: {
        httpStatus: res.status,
        count: opps.length,
        providerMetadataLeaks: leak ? 1 : 0,
        sampleIds: opps.slice(0, 5).map((o) => o.id),
      },
    };
  } catch (err) {
    return {
      status: "blocked",
      machineStatus: "blocked",
      hotelId,
      runId,
      verifiedAt: startedAt,
      buildSha,
      dataRevision,
      notes: `local serving unreachable: ${err?.message || err}`,
      checks: { error: String(err?.message || err) },
    };
  }
}

/**
 * Probe production GDI opportunities API.
 */
export async function runProductionDeliveryVerification({
  hotelId,
  runId = null,
  baseUrl = process.env.GDI_PRODUCTION_BASE_URL ||
    "https://my-operators-backend-production.up.railway.app",
  dataRevision = null,
  buildSha = null,
  fetchImpl = globalThis.fetch,
} = {}) {
  const url = `${String(baseUrl).replace(/\/$/, "")}/api/group-demand-intelligence/hotels/${encodeURIComponent(hotelId)}/opportunities`;
  const startedAt = new Date().toISOString();
  try {
    const res = await fetchImpl(url, { method: "GET" });
    const body = await res.json().catch(() => ({}));
    const opps = body.opportunities || [];
    const leak = JSON.stringify(opps).match(
      /trun_|enrichmentId|surfe|parallel\.ai|creditsConsumed/i
    );
    if (!res.ok) {
      return {
        status: "failed",
        machineStatus: "failed",
        hotelId,
        runId,
        verifiedAt: startedAt,
        buildSha,
        dataRevision,
        notes: `production API status ${res.status}`,
        checks: { httpStatus: res.status },
      };
    }
    return {
      status: "passed",
      machineStatus: "passed",
      hotelId,
      runId,
      verifiedAt: startedAt,
      buildSha,
      dataRevision: dataRevision || body.updatedAt || null,
      notes: `production list count=${opps.length}; leaks=${leak ? 1 : 0}`,
      checks: {
        httpStatus: res.status,
        count: opps.length,
        providerMetadataLeaks: leak ? 1 : 0,
      },
    };
  } catch (err) {
    return {
      status: "blocked",
      machineStatus: "blocked",
      hotelId,
      runId,
      verifiedAt: startedAt,
      buildSha,
      dataRevision,
      notes: `production unreachable: ${err?.message || err}`,
      checks: { error: String(err?.message || err) },
    };
  }
}

/**
 * Authoritative budget reconciliation from hotel/product ledger JSON.
 */
export function reconcileAuthoritativeBudget({
  ledgerPath = null,
  authorizationId = null,
  hotelId = null,
  ledgerDoc = null,
} = {}) {
  let doc = ledgerDoc;
  if (!doc && ledgerPath && fs.existsSync(ledgerPath)) {
    try {
      doc = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    } catch {
      doc = null;
    }
  }
  if (!doc) {
    return {
      reconciled: false,
      authoritative: false,
      reason: "ledger_missing",
    };
  }

  const auth =
    authorizationId ||
    doc.authorization ||
    doc.authorizationId ||
    null;
  if (authorizationId && auth && String(authorizationId) !== String(auth)) {
    return {
      reconciled: false,
      authoritative: true,
      reason: "authorization_mismatch",
      authorizationId: auth,
    };
  }
  if (hotelId && doc.hotelId && String(hotelId) !== String(doc.hotelId)) {
    return {
      reconciled: false,
      authoritative: true,
      reason: "hotel_mismatch",
      authorizationId: auth,
    };
  }

  const entries = Array.isArray(doc.entries) ? doc.entries : [];
  const spent = entries
    .filter((e) => e.status === "spent")
    .reduce((s, e) => s + Number(e.estimatedUsd || 0), 0);
  const reserved = entries
    .filter((e) => e.status === "reserved")
    .reduce((s, e) => s + Number(e.estimatedUsd || 0), 0);
  const cap = Number(doc.budgetUsd ?? doc.capUsd ?? 0);
  const hold = Number(doc.accountingHoldUsd || 0);
  const estimated =
    doc.estimatedSpentUsd != null ? Number(doc.estimatedSpentUsd) : spent;
  const available = Number((cap - estimated - reserved - hold).toFixed(2));

  // Detect duplicate reservation/spend jobKeys
  const keys = entries.map((e) => e.jobKey || e.reservationId).filter(Boolean);
  const dup = keys.filter((k, i) => keys.indexOf(k) !== i);

  return {
    reconciled: dup.length === 0 && Boolean(auth),
    authoritative: true,
    authorizationId: auth,
    capUsd: cap,
    estimatedIncurredUsd: estimated,
    actualBilledUsd:
      doc.actualBilledUsd === undefined ? null : doc.actualBilledUsd,
    outstandingReservationsUsd: reserved,
    accountingHoldUsd: hold,
    availableUsd: available,
    reservationsAccounted: true,
    duplicateJobKeys: [...new Set(dup)],
    exhausted: available <= 0 && reserved === 0,
    ledgerPath: ledgerPath || null,
  };
}

/**
 * Fingerprint run artifacts for resume invalidation.
 */
export function computeRunArtifactFingerprint(runDir) {
  if (!runDir || !fs.existsSync(runDir)) return null;
  const parts = [];
  const walk = (dir, depth = 0) => {
    if (depth > 3) return;
    let ents = [];
    try {
      ents = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const ent of ents) {
      const p = path.join(dir, ent.name);
      if (ent.isDirectory()) {
        if (
          [
            "full_e2e",
            "buyer_contact_enrichment",
            "paid_multisegment",
            "reuse_prior_intelligence",
          ].includes(ent.name)
        ) {
          walk(p, depth + 1);
        }
      } else if (/\.(json|csv|md)$/i.test(ent.name)) {
        try {
          const st = fs.statSync(p);
          parts.push(`${path.relative(runDir, p)}:${st.size}:${st.mtimeMs}`);
        } catch {
          /* ignore */
        }
      }
    }
  };
  walk(runDir);
  parts.sort();
  return createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 24);
}

export function verificationNeedsRerun(prevStage = {}, fingerprints = {}) {
  if (!prevStage || prevStage.status !== "completed") return true;
  const vs = String(prevStage.verificationStatus || "").toLowerCase();
  if (!vs || vs === "not_run" || vs === "blocked" || vs === "failed" || vs === "stale") {
    return true;
  }
  if (
    fingerprints.dataRevision &&
    prevStage.dataRevision &&
    String(fingerprints.dataRevision) !== String(prevStage.dataRevision)
  ) {
    return true;
  }
  if (
    fingerprints.buildSha &&
    prevStage.buildSha &&
    String(fingerprints.buildSha) !== String(prevStage.buildSha)
  ) {
    return true;
  }
  if (
    fingerprints.artifactFingerprint &&
    prevStage.artifactFingerprint &&
    String(fingerprints.artifactFingerprint) !==
      String(prevStage.artifactFingerprint)
  ) {
    return true;
  }
  return false;
}

export function textTrim(v) {
  return text(v);
}
