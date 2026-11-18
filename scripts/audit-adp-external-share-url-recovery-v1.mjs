#!/usr/bin/env node
/**
 * Audit ADP external customer share URL recovery.
 * Does not mint tokens, grant access, or publish.
 * Verifies production HTTPS links for hotels that already have active share tokens.
 */
import fs from "node:fs";
import path from "node:path";
import {
  probeAdpClientShareAvailable,
} from "../lib/admin/report-external-client-links-v1.js";
import {
  PRODUCTION_PUBLIC_BASE,
  verifyAdpExternalShareHttpV1,
} from "../lib/ai-demand-positioning/share/adp-external-share-access-gate-v1.js";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import { listInternalOwnerAppPropertyInventory } from "../lib/ai-demand-positioning/share/adp-owner-app-admin-inventory-v1.js";

const BASE = (process.env.ADP_RESTORE_BASE_URL || "http://127.0.0.1:8080").replace(
  /\/$/,
  ""
);
const OUT = path.join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-external-share-url-recovery-audit-v1.json"
);

const cov = buildPublishedAdpReviewCoverageV1({});
const inv = listInternalOwnerAppPropertyInventory({});
const invIds = new Set(
  (Array.isArray(inv) ? inv : []).map((r) => r.propertyId).filter(Boolean)
);

const rows = [];
for (const p of cov.properties || []) {
  const id = p.propertyId;
  const probe = probeAdpClientShareAvailable(id);
  const baseRow = {
    hotel: p.propertyName || id,
    propertyId: id,
    dropdown: invIds.has(id) ? "Restored" : "Missing",
    review: p.coverageStatus,
    pdf: p.pdfStatus || (p.hasPdf ? "READY" : "MISSING"),
  };

  if (!probe.available) {
    rows.push({
      ...baseRow,
      externalUrl: "Unauthorized",
      customerAuthorization: "NO_AUTHORIZED_SHARE",
      outcome: "REQUIRES_APPROVAL",
      reason: probe.reason,
      tokenId: null,
    });
    continue;
  }

  const res = await fetch(
    `${BASE}/api/admin/ai-demand/hotels/${encodeURIComponent(id)}/external-client-links`,
    { headers: { Accept: "application/json" }, cache: "no-store" }
  );
  const j = await res.json().catch(() => ({}));
  const url = j?.adp?.url || null;
  if (!url || j?.adp?.available !== true) {
    rows.push({
      ...baseRow,
      externalUrl: "Missing",
      customerAuthorization: "ACTIVE_SHARE_TOKEN",
      outcome: "BLOCKED",
      reason: j?.adp?.reason || j?.error || `http_${res.status}`,
      tokenId: probe.tokenId || null,
    });
    continue;
  }

  const origin = new URL(url).origin;
  if (origin !== PRODUCTION_PUBLIC_BASE) {
    rows.push({
      ...baseRow,
      externalUrl: "Missing",
      customerAuthorization: "ACTIVE_SHARE_TOKEN",
      outcome: "BLOCKED",
      reason: `non_production_url:${origin}`,
      tokenId: j.adp.tokenId || probe.tokenId || null,
    });
    continue;
  }

  const verify = await verifyAdpExternalShareHttpV1({
    url,
    expectedPropertyId: id,
    baseUrl: PRODUCTION_PUBLIC_BASE,
    timeoutMs: 20000,
  });

  rows.push({
    ...baseRow,
    externalUrl: verify.ok ? "Restored" : "Missing",
    customerAuthorization: "ACTIVE_SHARE_TOKEN",
    outcome: verify.ok ? "RESTORED" : "BLOCKED",
    reason: verify.ok ? null : (verify.reasons || []).join(";") || verify.status,
    tokenId: j.adp.tokenId || probe.tokenId || null,
    periodId: verify.periodId || null,
    consideration: verify.consideration,
    propertyMatch: verify.propertyId === id,
    status: verify.status,
  });
}

const summary = {
  RESTORED: rows.filter((r) => r.outcome === "RESTORED").length,
  REQUIRES_APPROVAL: rows.filter((r) => r.outcome === "REQUIRES_APPROVAL")
    .length,
  BLOCKED: rows.filter((r) => r.outcome === "BLOCKED").length,
};

const report = {
  generatedAt: new Date().toISOString(),
  gate: "ADP_EXTERNAL_SHARE_URL_RECOVERY_AUDIT_V1",
  productionBase: PRODUCTION_PUBLIC_BASE,
  localAdminBase: BASE,
  note: "Does not mint shares or authorize customer publication. Aimbridge CALA hotels without prior share tokens remain REQUIRES_APPROVAL.",
  summary,
  rows,
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ ok: summary.BLOCKED === 0, out: OUT, summary }, null, 2));
process.exit(summary.BLOCKED === 0 ? 0 : 1);
