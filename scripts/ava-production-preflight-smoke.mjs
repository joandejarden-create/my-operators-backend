/**
 * AVA production preflight smoke (local). No deploy. No founder approval flips.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getAuditMeta, getLatestAuditVersion, loadAuditVersion } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const base = process.env.AVA_PREFLIGHT_BASE || "http://localhost:8081";
const out = {
  ok: true,
  at: new Date().toISOString(),
  base,
  checks: [],
  failed: [],
};

function record(id, pass, detail) {
  const row = { id, pass, detail };
  out.checks.push(row);
  if (!pass) {
    out.ok = false;
    out.failed.push(row);
  }
  console.log(pass ? "PASS" : "FAIL", id, typeof detail === "string" ? detail : JSON.stringify(detail));
}

async function j(method, p, body) {
  const r = await fetch(base + p, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await r.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text.slice(0, 120) };
  }
  return { status: r.status, data };
}

const hotels = await j("GET", "/api/admin/ai-visibility-audit/hotels");
record("hotels-load", hotels.status === 200 && hotels.data.ok === true, {
  status: hotels.status,
  count: hotels.data.count,
});
record(
  "bethesda-present",
  (hotels.data.hotels || []).some((h) => h.propertyId === "adp_bethesda_marriott"),
  (hotels.data.hotels || []).find((h) => h.propertyId === "adp_bethesda_marriott")
);
const beth = (hotels.data.hotels || []).find((h) => h.propertyId === "adp_bethesda_marriott");
record(
  "bethesda-evidence-aligned",
  beth?.auditStatus === "AUDIT_VALIDATED" && beth?.evidenceReviewStatus === "AUDIT_VALIDATED",
  { auditStatus: beth?.auditStatus, evidenceReviewStatus: beth?.evidenceReviewStatus }
);

const water = (hotels.data.hotels || []).find((h) => h.propertyId === "adp_waterstone_boca_raton");
record("waterstone-three-actions", water?.acceptedRecommendationCount === 3, water);

const beforeMeta = getAuditMeta("adp_bethesda_marriott");
const beforeLatest = getLatestAuditVersion("adp_bethesda_marriott");
const decide = await j(
  "POST",
  "/api/admin/ai-visibility-audit/adp_bethesda_marriott/actions/AVA-BM-004/decision",
  { decision: "DEFER", note: "preflight-safe-mutation" }
);
record("decision-persist", decide.status === 200 && decide.data.ok === true, {
  status: decide.status,
  evidenceReviewStatus: decide.data.meta?.evidenceReviewStatus,
  auditStatus: decide.data.meta?.status,
});
record(
  "decision-no-evidence-downgrade",
  decide.data.meta?.evidenceReviewStatus === "AUDIT_VALIDATED",
  decide.data.meta?.evidenceReviewStatus
);
record(
  "decision-no-status-change",
  decide.data.meta?.status === "AUDIT_VALIDATED",
  decide.data.meta?.status
);

const afterLatest = getLatestAuditVersion("adp_bethesda_marriott");
record(
  "version-history-grew",
  afterLatest?.versionId !== beforeLatest?.versionId,
  { before: beforeLatest?.versionId, after: afterLatest?.versionId }
);
const priorStill = loadAuditVersion("adp_bethesda_marriott", beforeLatest.versionId);
record("prior-version-accessible", Boolean(priorStill), beforeLatest.versionId);

const pub = await j("POST", "/api/admin/ai-visibility-audit/adp_bethesda_marriott/generate-pdf", {
  publish: true,
});
record("generate-cannot-publish", pub.status === 403 && pub.data.error === "PUBLICATION_BLOCKED", {
  status: pub.status,
  error: pub.data.error,
});

const founder = await j("POST", "/api/admin/ai-visibility-audit/adp_bethesda_marriott/status", {
  status: "FOUNDER_APPROVED_FOR_CUSTOMER",
});
record(
  "founder-requires-flag",
  founder.status === 403 && founder.data.error === "FOUNDER_APPROVAL_FLAG_REQUIRED",
  { status: founder.status, error: founder.data.error }
);

const pdf = await fetch(`${base}/api/admin/ai-visibility-audit/adp_bethesda_marriott/pdf`);
record("pdf-preview", pdf.status === 200 && pdf.headers.get("content-type")?.includes("pdf"), {
  status: pdf.status,
  type: pdf.headers.get("content-type"),
});

const recs = await j("GET", "/api/admin/ai-visibility-audit/adp_bethesda_marriott/recommendations");
record("recommendations", recs.status === 200 && (recs.data.actions || []).length === 4, {
  count: (recs.data.actions || []).length,
});

// Regression probes for sibling admin surfaces (auth-gated; 200 or 401/403 both mean mounted)
const siblings = [
  "/api/admin/ai-demand-positioning/monthly-reviews",
  "/api/admin/ai-demand-positioning/action-plan-catalog",
  "/app/admin/ai-demand-admin.html",
];
for (const p of siblings) {
  const r = await fetch(base + p);
  record(`regression:${p}`, r.status === 200 || r.status === 401 || r.status === 403, r.status);
}

// Confirm current meta still not customer-ready
const finalMeta = getAuditMeta("adp_bethesda_marriott");
record(
  "no-customer-ready",
  finalMeta.customerReadinessStatus === "NOT_CUSTOMER_READY" &&
    finalMeta.publicationAuthorizationStatus === "NOT_AUTHORIZED",
  {
    customer: finalMeta.customerReadinessStatus,
    publication: finalMeta.publicationAuthorizationStatus,
    status: finalMeta.status,
  }
);

const outPath = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_PREFLIGHT_SMOKE.json"
);
fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
console.log(JSON.stringify({ ok: out.ok, failed: out.failed.length, outPath }, null, 2));
if (!out.ok) process.exit(1);
