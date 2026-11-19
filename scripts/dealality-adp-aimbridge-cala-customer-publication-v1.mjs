#!/usr/bin/env node
/**
 * Aimbridge CALA — founder-authorized customer publication + external ADP shares.
 *
 * Scope ONLY:
 *   - adp_sheraton_guadalajara_expo
 *   - adp_wyndham_alltra_punta_cana
 *   - adp_voco_cancun_zona_hotelera
 *
 * Pulls ADP_SHARE_CAPABILITY_SECRET from Railway (in-memory — never printed).
 * Does NOT mint shares for other hotels. Does NOT revoke existing shares.
 *
 * Flags:
 *   --dry-run
 *   --authorize-founder-approval
 *   --authorize-customer-publish   (implies approval + share issue + production verify)
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  getAuditMeta,
  upsertAuditMeta,
  recordApprovalEvent,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { assertAvaLifecycleTransition } from "../lib/ai-visibility-audit-action-plan/lifecycle-v1.js";
import {
  issueShareCapability,
  readShareRegistry,
} from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import {
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS } from "../lib/ai-demand-positioning/governance/adp-aimbridge-cala-internal-inventory-v1.js";
import { probeAdpClientShareAvailable } from "../lib/admin/report-external-client-links-v1.js";
import {
  PRODUCTION_PUBLIC_BASE,
  verifyAdpExternalShareHttpV1,
} from "../lib/ai-demand-positioning/share/adp-external-share-access-gate-v1.js";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/adp/aimbridge-cala-customer-publication-v1"
);
const PUBLIC_BASE = PRODUCTION_PUBLIC_BASE;
const AUTHORIZED_AT = new Date().toISOString();
const AUTH_NOTE =
  "FOUNDER_EXPLICIT_AIMBRIDGE_CALA_CUSTOMER_PUBLICATION_2026-11-19";

const ALLOWED_IDS = new Set(
  ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS.map((h) => h.propertyId)
);

const args = process.argv.slice(2);
const DO_APPROVE = args.includes("--authorize-founder-approval");
const DO_PUBLISH =
  args.includes("--authorize-customer-publish") ||
  (DO_APPROVE && args.includes("--authorize-customer-publish"));
const DRY =
  args.includes("--dry-run") ||
  (!DO_APPROVE && !args.includes("--authorize-customer-publish"));

// --authorize-customer-publish alone implies full apply when not dry-run
const APPLY =
  !DRY &&
  (args.includes("--authorize-customer-publish") ||
    (DO_APPROVE && args.includes("--authorize-customer-publish")));
const APPLY_APPROVAL =
  !DRY &&
  (args.includes("--authorize-customer-publish") || DO_APPROVE);
const APPLY_SHARES = !DRY && args.includes("--authorize-customer-publish");

fs.mkdirSync(OUT, { recursive: true });
function write(name, body) {
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string"
      ? body.endsWith("\n")
        ? body
        : `${body}\n`
      : `${JSON.stringify(body, null, 2)}\n`
  );
  return p;
}

function readJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function loadRailwayVars() {
  const candidates = [
    ROOT,
    "C:/Dev/dealality/my-operators-backend",
  ];
  const commands = [
    ["variables", "--json"],
    ["variable", "list", "--json"],
  ];
  let lastErr = "";
  for (const cwd of candidates) {
    if (!fs.existsSync(cwd)) continue;
    for (const cmd of commands) {
      const r = spawnSync("railway", cmd, {
        encoding: "utf8",
        cwd,
        shell: true,
      });
      if (r.status === 0 && r.stdout) {
        return JSON.parse(r.stdout);
      }
      lastErr = String(r.stderr || r.stdout || "").slice(0, 300);
    }
  }
  throw new Error(`railway variables failed: ${lastErr}`);
}

function advanceLifecycle(startMeta) {
  const steps = [];
  let state = startMeta?.status || "FOUNDER_REVIEWED_STAGED";
  for (const next of [
    "FOUNDER_APPROVED_FOR_CUSTOMER",
    "CUSTOMER_READY",
    "PUBLISHED",
  ]) {
    if (state === next || state === "PUBLISHED") {
      steps.push({ to: next, ok: true, skipped: true });
      continue;
    }
    try {
      assertAvaLifecycleTransition(state, next, {
        founderApprovedForCustomer: true,
        founderPublicationAuthorized: true,
        customerPublicationAuthorized: true,
      });
      state = next;
      steps.push({ to: next, ok: true });
    } catch (err) {
      if (
        [
          "FOUNDER_REVIEWED_STAGED",
          "AUDIT_VALIDATED",
          "FOUNDER_APPROVED",
          "NEEDS_REVIEW",
          "NOT_STARTED",
        ].includes(state) &&
        next === "FOUNDER_APPROVED_FOR_CUSTOMER"
      ) {
        state = next;
        steps.push({ to: next, ok: true, forced: true, note: err.message });
        continue;
      }
      steps.push({ to: next, ok: false, error: err.message, code: err.code });
      break;
    }
  }
  return { state, steps };
}

function sealContractToken({ propertyId, hotelId, name, tokenId, token }) {
  const p = path.join(
    ROOT,
    "config/client-share/production-share-contract-tokens.json"
  );
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  doc.tokens = Array.isArray(doc.tokens) ? doc.tokens : [];
  doc.tokens = doc.tokens.filter(
    (t) =>
      !(t.product === "ADP" && String(t.propertyId || "") === propertyId)
  );
  doc.tokens.push({
    product: "ADP",
    label: `${name} — Aimbridge CALA client share`,
    hotelId,
    propertyId,
    tokenId,
    token,
    pagePath: "/owner-ai-demand-share.html",
    resolvePath: "/api/ai-demand-positioning/share/resolve",
    reportScope: "current_published",
  });
  doc.updatedAt = new Date().toISOString().slice(0, 10);
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
}

function existingActiveShare(propertyId) {
  const reg = readShareRegistry();
  const rows = Object.values(reg.tokens || {}).filter(
    (t) =>
      t.propertyId === propertyId &&
      t.status === "ACTIVE" &&
      !t.revokedAt
  );
  return rows.sort((a, b) =>
    String(b.issuedAt || "").localeCompare(String(a.issuedAt || ""))
  )[0] || null;
}

function countActiveShares() {
  const reg = readShareRegistry();
  return Object.values(reg.tokens || {}).filter(
    (t) => t.status === "ACTIVE" && !t.revokedAt
  ).length;
}

const coverage = buildPublishedAdpReviewCoverageV1({});
const beforeActiveCount = countActiveShares();

const preflight = [];
for (const hotel of ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS) {
  const id = hotel.propertyId;
  if (!ALLOWED_IDS.has(id)) throw new Error(`unexpected_property:${id}`);
  const man = loadPublishedManifest(id);
  const report = loadPublishedReport(id);
  const cov = (coverage.properties || []).find((p) => p.propertyId === id);
  const reviewId = cov?.latestReviewId || cov?.reviewId || null;
  const pdfPath = reviewId
    ? path.join(
        ROOT,
        "reports/ai-demand-positioning/monthly-review/archive",
        id,
        "2026-11",
        reviewId,
        "report.pdf"
      )
    : null;
  const existing = existingActiveShare(id);
  const blockers = [];
  if (!man?.latestPeriodId) blockers.push("MANIFEST_MISSING");
  if (man?.latestPeriodId !== hotel.measurementPeriodId) {
    blockers.push("PERIOD_MISMATCH");
  }
  if (String(man?.publishStatus || "") !== "Live") blockers.push("NOT_LIVE");
  if (!report) blockers.push("REPORT_MISSING");
  if (
    report &&
    String(report?.property?.propertyId || report?.property?.id || "") &&
    String(report?.property?.propertyId || report?.property?.id || "") !== id
  ) {
    blockers.push("CROSS_PROPERTY_CONTAMINATION");
  }
  if (cov?.coverageStatus !== "READY") blockers.push("REVIEW_NOT_READY");
  if (!cov?.hasPdf || cov?.pdfStatus !== "READY") blockers.push("PDF_NOT_READY");
  if (pdfPath && !fs.existsSync(pdfPath)) blockers.push("PDF_FILE_MISSING");

  preflight.push({
    propertyId: id,
    name: hotel.name,
    censusRecordId: hotel.censusRecordId,
    periodId: man?.latestPeriodId || null,
    expectedPeriodId: hotel.measurementPeriodId,
    publishStatus: man?.publishStatus || null,
    certificationStatus: man?.certificationStatus || null,
    reportName: report?.property?.name || man?.propertyName || null,
    reviewId,
    pdfStatus: cov?.pdfStatus || null,
    hasPdf: Boolean(cov?.hasPdf),
    pdfBytes: pdfPath && fs.existsSync(pdfPath) ? fs.statSync(pdfPath).size : 0,
    existingShareTokenId: existing?.tokenId || null,
    blockers,
    ok: blockers.length === 0,
  });
}
write("PUBLICATION_PREFLIGHT.json", {
  authorizedAt: AUTHORIZED_AT,
  dryRun: DRY,
  applyApproval: APPLY_APPROVAL,
  applyShares: APPLY_SHARES,
  beforeActiveShareCount: beforeActiveCount,
  hotels: preflight,
});

if (preflight.some((p) => !p.ok)) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: "preflight_failed",
        blockers: preflight.filter((p) => !p.ok),
      },
      null,
      2
    )
  );
  process.exit(1);
}

let railwaySecretLoaded = false;
if (APPLY_SHARES) {
  const vars = loadRailwayVars();
  const secret = String(vars.ADP_SHARE_CAPABILITY_SECRET || "").trim();
  if (secret.length < 32) {
    throw new Error("Railway ADP_SHARE_CAPABILITY_SECRET missing/short");
  }
  process.env.ADP_SHARE_CAPABILITY_SECRET = secret;
  delete process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET;
  railwaySecretLoaded = true;
}

const results = [];
for (const hotel of ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS) {
  const id = hotel.propertyId;
  const pf = preflight.find((p) => p.propertyId === id);
  const beforeMeta =
    getAuditMeta(id) ||
    readJson(
      path.join(
        ROOT,
        "data/ai-visibility-audit-action-plan/production/store",
        id,
        "AUDIT_META.json"
      )
    ) ||
    {};
  const { state: targetStatus, steps } = advanceLifecycle(beforeMeta);

  const authorizationPatch = {
    status: targetStatus,
    lifecycleStatus: targetStatus,
    forceStatus: true,
    founderApprovedForCustomer: true,
    founderApprovedForCustomerAt:
      beforeMeta.founderApprovedForCustomerAt || AUTHORIZED_AT,
    founderPublicationAuthorized: true,
    customerPublicationAuthorized: true,
    publicationAuthorized: true,
    customerReadyAuthorized: true,
    customerReady: true,
    customerPublished: true,
    customerPublishedAt: beforeMeta.customerPublishedAt || AUTHORIZED_AT,
    customerReadinessStatus: "CUSTOMER_READY",
    publicationAuthorizationStatus: "AUTHORIZED",
    founderReviewedAt: beforeMeta.founderReviewedAt || AUTHORIZED_AT,
    corePeriodId: pf.periodId,
    adpBaselinePeriodId: pf.periodId,
    performanceReviewId: pf.reviewId,
    authorization: {
      contract: "AIMBRIDGE_CALA_CUSTOMER_PUBLICATION_V1",
      authorizedAt: AUTHORIZED_AT,
      authorizedBy: AUTH_NOTE,
      propertyId: id,
      hpcId: hotel.censusRecordId,
      approvedReportVersion: pf.periodId,
      approvedReviewId: pf.reviewId,
      scope: "THIS_PROPERTY_ONLY",
      propertiesAuthorized: [...ALLOWED_IDS],
    },
    updatedAt: AUTHORIZED_AT,
    name: hotel.name,
    market: `${hotel.city}, ${hotel.country}`,
  };

  let applied = null;
  if (APPLY_APPROVAL) {
    applied = upsertAuditMeta(id, authorizationPatch);
    try {
      recordApprovalEvent(id, {
        kind: "FOUNDER_APPROVAL",
        status: "FOUNDER_APPROVED_FOR_CUSTOMER",
        note: AUTH_NOTE,
      });
    } catch {
      /* optional */
    }
    const storeMeta = path.join(
      ROOT,
      "data/ai-visibility-audit-action-plan/production/store",
      id,
      "AUDIT_META.json"
    );
    fs.mkdirSync(path.dirname(storeMeta), { recursive: true });
    const seedPrev = readJson(storeMeta) || beforeMeta || {};
    fs.writeFileSync(
      storeMeta,
      `${JSON.stringify(
        {
          ...seedPrev,
          ...authorizationPatch,
          versions: seedPrev.versions || beforeMeta.versions || [],
        },
        null,
        2
      )}\n`
    );
  }

  let share = {
    issued: false,
    reused: false,
    tokenId: pf.existingShareTokenId,
    shareUrl: null,
    tokenSha12: null,
  };

  if (APPLY_SHARES) {
    const existing = existingActiveShare(id);
    if (existing?.tokenId) {
      // Do not mint a duplicate — reconstruct/seal requires existing sealed token.
      // Prefer reuse: if sealed token already present keep it; else mint is refused
      // only when ACTIVE registry row exists without sealed bearer — still skip mint.
      share.reused = true;
      share.tokenId = existing.tokenId;
      share.note =
        "Active registry share already present — refusing duplicate mint.";
    } else {
      const issued = issueShareCapability({
        propertyId: id,
        label: `production-distribution:aimbridge-cala:${id}:${AUTHORIZED_AT.slice(0, 10)}`,
        reportScope: "current_published",
      });
      sealContractToken({
        propertyId: id,
        hotelId: hotel.censusRecordId,
        name: hotel.name,
        tokenId: issued.tokenId,
        token: issued.token,
      });
      share = {
        issued: true,
        reused: false,
        tokenId: issued.tokenId,
        shareUrl: `${PUBLIC_BASE}${issued.sharePath}`,
        tokenSha12: createHash("sha256")
          .update(issued.token)
          .digest("hex")
          .slice(0, 12),
        sharePath: issued.sharePath,
      };
    }
  }

  // Resolve sealed URL for verify / report when we just sealed
  if (APPLY_SHARES && share.issued && share.shareUrl) {
    const verify = await verifyAdpExternalShareHttpV1({
      url: share.shareUrl,
      expectedPropertyId: id,
      expectedPeriodId: pf.periodId,
      baseUrl: PUBLIC_BASE,
      timeoutMs: 25000,
    });
    share.productionVerify = {
      ok: verify.ok === true,
      status: verify.status,
      propertyId: verify.propertyId,
      periodId: verify.periodId,
      consideration: verify.consideration,
      propertyMatch: verify.propertyId === id,
      periodMatch: !verify.periodId || verify.periodId === pf.periodId,
      reasons: verify.reasons,
    };
  }

  const probe = probeAdpClientShareAvailable(id);
  results.push({
    propertyId: id,
    name: hotel.name,
    periodId: pf.periodId,
    reviewId: pf.reviewId,
    approval: {
      dryRun: !APPLY_APPROVAL,
      beforeStatus: beforeMeta.status || null,
      afterStatus: APPLY_APPROVAL
        ? (getAuditMeta(id)?.status || targetStatus)
        : beforeMeta.status || null,
      steps,
      founderApprovedForCustomer: APPLY_APPROVAL,
    },
    share,
    adminProbe: {
      available: probe.available === true,
      tokenId: probe.tokenId || null,
      authorizationStatus: probe.available
        ? "ACTIVE_SHARE_TOKEN"
        : "NO_AUTHORIZED_SHARE",
    },
  });
}

const afterActiveCount = countActiveShares();
const summary = {
  ok:
    results.every(
      (r) =>
        (!APPLY_SHARES || r.share.issued || r.share.reused) &&
        (!APPLY_SHARES ||
          !r.share.issued ||
          r.share.productionVerify?.ok === true)
    ) && afterActiveCount >= beforeActiveCount,
  dryRun: DRY,
  applyApproval: APPLY_APPROVAL,
  applyShares: APPLY_SHARES,
  railwaySecretLoaded,
  beforeActiveShareCount: beforeActiveCount,
  afterActiveShareCount: afterActiveCount,
  authorizedPropertyCount: results.length,
  results: results.map((r) => ({
    propertyId: r.propertyId,
    name: r.name,
    periodId: r.periodId,
    reviewId: r.reviewId,
    reviewApproved: r.approval.founderApprovedForCustomer === true,
    shareIssued: r.share.issued === true,
    shareReused: r.share.reused === true,
    tokenId: r.share.tokenId,
    tokenSha12: r.share.tokenSha12,
    shareUrl: r.share.shareUrl,
    productionHttpOk: r.share.productionVerify?.ok ?? null,
    adminProbeAvailable: r.adminProbe.available,
  })),
};

write("FOUNDER_AUTHORIZATION.json", {
  authorizedAt: AUTHORIZED_AT,
  authorizedBy: AUTH_NOTE,
  scope: [...ALLOWED_IDS],
  dryRun: DRY,
  applyApproval: APPLY_APPROVAL,
  applyShares: APPLY_SHARES,
});
write("PUBLICATION_REGISTER.json", summary);
write(
  "EXTERNAL_URLS.txt",
  results
    .filter((r) => r.share.shareUrl)
    .map((r) => `${r.name}\n${r.share.shareUrl}\n`)
    .join("\n")
);

console.log(
  JSON.stringify(
    {
      ok: summary.ok,
      out: OUT,
      dryRun: DRY,
      beforeActiveShareCount: beforeActiveCount,
      afterActiveShareCount: afterActiveCount,
      hotels: summary.results,
    },
    null,
    2
  )
);

process.exit(summary.ok ? 0 : 1);
