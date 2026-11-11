#!/usr/bin/env node
/**
 * Audit ADP external customer reports for Provider Presence ↔ Consideration grain lock.
 * Reconstructs existing share tokens (no mint/rotate). Redacts tokens in all outputs.
 *
 *   node scripts/audit-adp-external-report-grain-lock-v1.mjs
 *   node scripts/audit-adp-external-report-grain-lock-v1.mjs --base=http://127.0.0.1:8080
 *   node scripts/audit-adp-external-report-grain-lock-v1.mjs --packages-only
 */
import "../load-env.js";
import { createHash, createHmac } from "crypto";
import { mkdirSync, readFileSync, readdirSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import {
  ADP_SHARE_TOKEN_PREFIX,
  ADP_SHARE_TOKEN_VERSION,
  getShareCapabilitySecret,
  readShareRegistry,
  verifyShareCapability,
} from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { auditProviderPresenceGrain } from "../lib/ai-demand-positioning/metrics/adp-provider-presence-grain-lock-v1.js";

const OUT =
  "reports/dealality-adp-external-customer-report-regression-v1";
const BASE =
  process.argv.find((a) => a.startsWith("--base="))?.slice("--base=".length) ||
  "https://my-operators-backend-production.up.railway.app";
const PACKAGES_ONLY = process.argv.includes("--packages-only");

function b64url(buf) {
  return Buffer.from(buf)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}
function b64urlJson(obj) {
  return b64url(Buffer.from(JSON.stringify(obj), "utf8"));
}
function tokenFingerprint(token) {
  return createHash("sha256").update(String(token || "")).digest("hex").slice(0, 12);
}
function reconstructToken(row, secret) {
  const iat = Math.floor(new Date(row.issuedAt).getTime() / 1000);
  const payload = {
    v: ADP_SHARE_TOKEN_VERSION,
    tid: row.tokenId,
    propertyId: row.propertyId,
    surfaces: [...(row.surfaces || [])],
    reportScope: row.reportScope || "current_published",
    iat,
    exp: row.expiresAt ? Math.floor(new Date(row.expiresAt).getTime() / 1000) : null,
  };
  const body = b64urlJson(payload);
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  return `${ADP_SHARE_TOKEN_PREFIX}${body}.${sig}`;
}

function clientWouldBlock(payload) {
  const em = payload?.executiveMetrics || {};
  const sp = em.scenarioPresence || {};
  const cr = em.considerationRate || {};
  const methId = cr.methodologyId || sp.methodologyId || "";
  const spEligible = Number(sp.eligibleScenarios);
  const consComparable = Number(cr.comparableObservations);
  const independentGrain =
    /INDEPENDENT_UNPROMPTED/i.test(String(methId)) ||
    (spEligible === 64 && consComparable === 256);
  if (!independentGrain || !Array.isArray(payload?.evidence?.providers)) {
    return { blocked: false, independentGrain: false };
  }
  const execPresent = Number(cr.presentObservations);
  const execComparable = Number(cr.comparableObservations);
  let mentionSum = 0;
  let comparableSum = 0;
  const badProv = payload.evidence.providers.filter((row) => {
    const c = Number(row.comparable != null ? row.comparable : row.total);
    const sched = Number(
      row.scenariosScheduled != null ? row.scenariosScheduled : row.scheduled
    );
    const m = Number(row.mentioned || 0);
    if (Number.isFinite(m)) mentionSum += m;
    if (Number.isFinite(c)) comparableSum += c;
    return (
      c === 78 ||
      sched === 78 ||
      (spEligible > 0 && Number.isFinite(c) && c > spEligible) ||
      (spEligible > 0 && Number.isFinite(sched) && sched > spEligible)
    );
  });
  const sumMismatch =
    (Number.isFinite(execPresent) && mentionSum !== execPresent) ||
    (Number.isFinite(execComparable) && comparableSum !== execComparable);
  return {
    blocked: Boolean(badProv.length || sumMismatch),
    independentGrain: true,
    spEligible,
    eligibleIds: Array.isArray(cr.eligibleScenarioIds)
      ? cr.eligibleScenarioIds.length
      : null,
    mentionSum,
    execPresent,
    comparableSum,
    execComparable,
    badProvCount: badProv.length,
    sumMismatch,
    idsVsSpEligibleMismatch:
      Array.isArray(cr.eligibleScenarioIds) &&
      Number.isFinite(spEligible) &&
      cr.eligibleScenarioIds.length !== spEligible,
  };
}

function summarizePayload(propertyId, payload, extra = {}) {
  const audit = auditProviderPresenceGrain(payload || {});
  const block = clientWouldBlock(payload || {});
  const cr = payload?.executiveMetrics?.considerationRate || {};
  const sp = payload?.executiveMetrics?.scenarioPresence || {};
  return {
    propertyId,
    ...extra,
    periodId: payload?.period?.periodId || payload?.periodId || null,
    consideration: cr.rate ?? cr.percentage ?? null,
    eligibleIds: Array.isArray(cr.eligibleScenarioIds)
      ? cr.eligibleScenarioIds.length
      : null,
    spEligible: sp.eligibleScenarios ?? null,
    auditPass: audit.pass,
    mixedGrain: audit.mixedGrain,
    clientBlocked: block.blocked,
    block,
    providers: (payload?.evidence?.providers || []).map((p) => ({
      provider: p.provider,
      mentioned: p.mentioned,
      comparable: p.comparable,
      scheduled: p.scenariosScheduled ?? p.scheduled,
    })),
  };
}

function auditDurablePackages() {
  const currentRoot = "data/adp-durable-published/current";
  const snapRoot = "data/adp-durable-published/snapshots";
  const hotels = readdirSync(currentRoot).filter((d) => d.startsWith("adp_"));
  const rows = [];
  for (const pid of hotels) {
    const man = JSON.parse(
      readFileSync(join(currentRoot, pid, "manifest.json"), "utf8")
    );
    const path = join(snapRoot, pid, man.reportFile);
    if (!existsSync(path)) {
      rows.push({ propertyId: pid, error: "report_missing" });
      continue;
    }
    const wrap = JSON.parse(readFileSync(path, "utf8"));
    const payload = wrap.payload || wrap;
    rows.push(
      summarizePayload(pid, payload, {
        source: "durable_package",
        methodologyFingerprintSha256: wrap.methodologyFingerprintSha256 || null,
        publicationManifestSha256: wrap.publicationManifestSha256 || null,
        certificationStatus: wrap._certification?.certificationStatus || null,
      })
    );
  }
  return rows;
}

async function auditLiveShares(base) {
  const secret = getShareCapabilitySecret();
  if (!secret) throw new Error("ADP_SHARE_CAPABILITY_SECRET missing");
  const reg = readShareRegistry();
  const packageRows = auditDurablePackages();
  const propertyIds = packageRows.map((r) => r.propertyId).filter(Boolean);
  const rows = [];
  for (const propertyId of propertyIds) {
    const toks = Object.values(reg.tokens || {}).filter(
      (t) => t.propertyId === propertyId && t.status === "ACTIVE"
    );
    // Prefer tokens that grant the report surface (some production-distribution
    // rows are pdf/owner_report-only). Skip expired LOCAL_TEST_ONLY first hits.
    const ranked = [
      ...toks.filter(
        (t) =>
          String(t.label || "").startsWith("production-distribution:") &&
          (t.surfaces || []).includes("report")
      ),
      ...toks.filter((t) => String(t.label || "").includes("client-share")),
      ...toks.filter((t) => (t.surfaces || []).includes("report")),
      ...toks,
    ];
    let preferred = null;
    let token = null;
    let verified = null;
    for (const candidate of ranked) {
      const t = reconstructToken(candidate, secret);
      const v = verifyShareCapability(t, {
        expectedPropertyId: propertyId,
        requiredSurface: "report",
      });
      if (v.ok) {
        preferred = candidate;
        token = t;
        verified = v;
        break;
      }
      if (!preferred) {
        preferred = candidate;
        token = t;
        verified = v;
      }
    }
    if (!preferred || !verified?.ok) {
      rows.push({
        propertyId,
        error: "token_verify_failed",
        code: verified?.code || verified?.error || "no_active_token",
        tokenFingerprint: token ? tokenFingerprint(token) : null,
        tokenId: preferred?.tokenId || null,
      });
      continue;
    }
    const reportRes = await fetch(
      `${base.replace(/\/$/, "")}/api/ai-demand-positioning/property/${encodeURIComponent(propertyId)}/report?share=${encodeURIComponent(token)}`,
      { headers: { Accept: "application/json" }, cache: "no-store" }
    );
    const reportJson = await reportRes.json().catch(() => null);
    const payload = reportJson?.payload || reportJson;
    rows.push(
      summarizePayload(propertyId, payload, {
        source: "live_share_report",
        base,
        reportStatus: reportRes.status,
        apiOk: reportJson?.ok !== false && !!payload?.executiveMetrics,
        readSource: reportJson?.source || reportJson?.readSource || null,
        tokenFingerprint: tokenFingerprint(token),
        tokenId: preferred.tokenId,
      })
    );
  }
  return rows;
}

mkdirSync(OUT, { recursive: true });
const packages = auditDurablePackages();
const packageFails = packages.filter((r) => r.clientBlocked || r.auditPass === false);
writeFileSync(
  join(OUT, "01-durable-package-grain-audit.json"),
  JSON.stringify(
    {
      auditedAt: new Date().toISOString(),
      total: packages.length,
      clientBlocked: packageFails.length,
      pass: packages.length - packageFails.length,
      rows: packages,
    },
    null,
    2
  )
);

let live = null;
if (!PACKAGES_ONLY) {
  live = await auditLiveShares(BASE);
  const liveFails = live.filter((r) => r.clientBlocked || r.error || r.apiOk === false);
  writeFileSync(
    join(OUT, "02-live-share-grain-audit.json"),
    JSON.stringify(
      {
        auditedAt: new Date().toISOString(),
        base: BASE,
        total: live.length,
        clientBlocked: live.filter((r) => r.clientBlocked).length,
        errors: live.filter((r) => r.error || r.apiOk === false).length,
        rows: live,
      },
      null,
      2
    )
  );
  console.log(
    JSON.stringify(
      {
        packages: {
          total: packages.length,
          clientBlocked: packageFails.length,
          blockedIds: packageFails.map((r) => r.propertyId),
        },
        live: {
          base: BASE,
          total: live.length,
          clientBlocked: live.filter((r) => r.clientBlocked).length,
          blockedIds: live.filter((r) => r.clientBlocked).map((r) => r.propertyId),
          errors: live.filter((r) => r.error || r.apiOk === false).map((r) => ({
            id: r.propertyId,
            error: r.error,
            apiOk: r.apiOk,
          })),
        },
        hiltonPackage: packages.find((r) => r.propertyId === "adp_hilton_times_square"),
        hiltonLive: live.find((r) => r.propertyId === "adp_hilton_times_square"),
      },
      null,
      2
    )
  );
} else {
  console.log(
    JSON.stringify(
      {
        packages: {
          total: packages.length,
          clientBlocked: packageFails.length,
          blockedIds: packageFails.map((r) => r.propertyId),
        },
        hiltonPackage: packages.find((r) => r.propertyId === "adp_hilton_times_square"),
      },
      null,
      2
    )
  );
}
