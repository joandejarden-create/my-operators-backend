/**
 * DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1
 *
 * Single idempotent inventory reconciler for ADP + GDI admin recovery.
 * Dry-run by default. Never mints customer shares or grants publication.
 */

import { existsSync, readdirSync, readFileSync, statSync } from "fs";
import { join } from "path";
import {
  deriveGdiReportStatus,
  derivePdfStatus,
  normalizeGdiAdminReportRow,
  reconcileGdiAdminReportCounts,
  resolveGdiAdminShareFlags,
} from "./gdi-admin-report-row-v1.js";
import {
  probeAdpClientShareAvailable,
  probeGdiClientShareState,
} from "./report-external-client-links-v1.js";
import { getCensusRecordIdForAdpProperty } from "../ai-demand-positioning/census-link-registry.js";
import {
  loadAllPeriods,
  loadPropertyProfile,
} from "../ai-demand-positioning/data-model.js";
import { CANONICAL_ADP_31_PROPERTY_IDS } from "../ai-demand-positioning/governance/canonical-adp-31-property-scope-v1.js";
import { ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS } from "../ai-demand-positioning/governance/adp-aimbridge-cala-internal-inventory-v1.js";
import { buildPublishedAdpReviewCoverageV1 } from "../ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
} from "../ai-demand-positioning/published-snapshot.js";
import {
  assessOwnerAppReportArtifacts,
  listInternalOwnerAppPropertyInventory,
} from "../ai-demand-positioning/share/adp-owner-app-admin-inventory-v1.js";
import { hasGdiReportPdf } from "../group-demand-intelligence/reports/gdi-pdf-store-v1.js";
import { getGdiPdfReportData } from "../group-demand-intelligence/reports/gdi-pdf-report-data-v1.js";
import { listGdiSelectableHotels } from "../group-demand-intelligence/index.js";

export const DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1 =
  "DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1";

export const ADP_RESULT = Object.freeze({
  COMPLETE: "COMPLETE",
  REVIEW_REBUILDABLE: "REVIEW_REBUILDABLE",
  PDF_REBUILDABLE: "PDF_REBUILDABLE",
  REGISTRY_REPAIR: "REGISTRY_REPAIR",
  SHARE_RECOVERABLE: "SHARE_RECOVERABLE",
  REQUIRES_FOUNDER_APPROVAL: "REQUIRES_FOUNDER_APPROVAL",
  EVIDENCE_MISSING: "EVIDENCE_MISSING",
  BLOCKED: "BLOCKED",
});

export const GDI_RESULT = Object.freeze({
  COMPLETE: "COMPLETE",
  PDF_REBUILDABLE: "PDF_REBUILDABLE",
  SHARE_RECOVERABLE: "SHARE_RECOVERABLE",
  REQUIRES_FOUNDER_APPROVAL: "REQUIRES_FOUNDER_APPROVAL",
  EVIDENCE_MISSING: "EVIDENCE_MISSING",
  WATCH_ONLY: "WATCH_ONLY",
  /** WATCH_ONLY with zero eligible ops — PDF not required for commercial delivery. */
  WATCH_ONLY_PDF_NOT_APPLICABLE: "WATCH_ONLY_PDF_NOT_APPLICABLE",
  /** WATCH_ONLY awaiting founder decision on watchlist/monitoring PDF policy. */
  AWAITING_REPORT_POLICY: "AWAITING_REPORT_POLICY",
  BLOCKED: "BLOCKED",
});

function listDirNames(abs) {
  if (!existsSync(abs)) return [];
  return readdirSync(abs, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name);
}

function listJsonStemNames(abs) {
  if (!existsSync(abs)) return [];
  return readdirSync(abs)
    .filter((f) => f.endsWith(".json"))
    .map((f) => f.replace(/\.json$/i, ""));
}

function readAdpShareRegistry(cwd) {
  const p = join(cwd, "config/client-share/adp-share-registry/active-tokens.json");
  if (!existsSync(p)) return { tokens: {} };
  try {
    return JSON.parse(readFileSync(p, "utf8"));
  } catch {
    return { tokens: {} };
  }
}

function readGdiShareRegistry(cwd) {
  const p = join(cwd, "config/client-share/gdi-share-registry/active-tokens.json");
  if (!existsSync(p)) return { tokens: {} };
  try {
    return JSON.parse(readFileSync(p, "utf8"));
  } catch {
    return { tokens: {} };
  }
}

function activeAdpPropertyIds(cwd) {
  const reg = readAdpShareRegistry(cwd);
  const out = new Set();
  for (const t of Object.values(reg.tokens || {})) {
    if (String(t?.status || "").toUpperCase() !== "ACTIVE") continue;
    if (t?.propertyId) out.add(String(t.propertyId));
  }
  return out;
}

function activeGdiHotelIds(cwd) {
  const reg = readGdiShareRegistry(cwd);
  const out = new Set();
  for (const t of Object.values(reg.tokens || {})) {
    if (String(t?.status || "").toUpperCase() !== "ACTIVE") continue;
    if (t?.hotelId) out.add(String(t.hotelId));
  }
  return out;
}

function durableCurrentIds(cwd) {
  return listDirNames(join(cwd, "data/adp-durable-published/current"));
}

function runtimePropertyIds(cwd) {
  const runtime = join(cwd, "data/ai-demand-positioning/runtime");
  if (!existsSync(runtime)) return new Set();
  const ids = new Set();
  for (const f of readdirSync(runtime)) {
    if (!f.startsWith("adp_period_") || !f.endsWith(".json")) continue;
    try {
      const j = JSON.parse(readFileSync(join(runtime, f), "utf8"));
      if (j?.propertyId) ids.add(String(j.propertyId));
    } catch {
      // ignore corrupt
    }
  }
  return ids;
}

function fixtureProfileIds(cwd) {
  const dir = join(cwd, "fixtures/ai-demand-positioning");
  if (!existsSync(dir)) return [];
  const ids = [];
  for (const f of readdirSync(dir)) {
    if (!f.endsWith("-property-profile.json")) continue;
    try {
      const j = JSON.parse(readFileSync(join(dir, f), "utf8"));
      if (j?.propertyId) ids.push(String(j.propertyId));
    } catch {
      // ignore
    }
  }
  return ids;
}

function gdiConfigHotelIds(cwd) {
  const dir = join(cwd, "config/group-demand-intelligence/hotels");
  const out = [];
  for (const stem of listJsonStemNames(dir)) {
    if (stem.startsWith("gdi_hotel_")) continue;
    const file = join(dir, `${stem}.json`);
    try {
      const cfg = JSON.parse(readFileSync(file, "utf8"));
      if (cfg?.redirectTo || cfg?.deprecatedProvisionalId) continue;
      out.push(String(cfg.hotelId || stem));
    } catch {
      out.push(stem);
    }
  }
  return [...new Set(out)];
}

function gdiDataHotelIds(cwd) {
  const root = join(cwd, "data/group-demand-intelligence/hotels");
  return listDirNames(root).filter((id) => !id.startsWith("gdi_hotel_"));
}

/**
 * Discover full ADP universe from all registered sources.
 */
export function discoverAdpUniverse(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const adminInv = listInternalOwnerAppPropertyInventory({ cwd });
  const adminIds = (adminInv || []).map((r) => r.propertyId).filter(Boolean);
  const publishedIds = listPublishedPropertyIds();
  const durableIds = durableCurrentIds(cwd);
  const fixtureIds = fixtureProfileIds(cwd);
  const runtimeIds = [...runtimePropertyIds(cwd)];
  const shareIds = [...activeAdpPropertyIds(cwd)];
  const canonical31 = [...CANONICAL_ADP_31_PROPERTY_IDS];
  const aimbridge = ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS.map((h) => h.propertyId);

  const byId = new Map();
  const touch = (id, source) => {
    if (!id) return;
    const row = byId.get(id) || { propertyId: id, sources: new Set() };
    row.sources.add(source);
    byId.set(id, row);
  };
  for (const id of canonical31) touch(id, "canonical_31");
  for (const id of aimbridge) touch(id, "aimbridge_cala");
  for (const id of adminIds) touch(id, "admin_inventory");
  for (const id of publishedIds) touch(id, "published");
  for (const id of durableIds) touch(id, "durable_current");
  for (const id of fixtureIds) touch(id, "fixture_profile");
  for (const id of runtimeIds) touch(id, "runtime_period");
  for (const id of shareIds) touch(id, "active_share");

  const hotels = [...byId.values()]
    .map((r) => ({
      propertyId: r.propertyId,
      sources: [...r.sources].sort(),
      censusRecordId: getCensusRecordIdForAdpProperty(r.propertyId),
      profile: loadPropertyProfile(r.propertyId) || null,
    }))
    .sort((a, b) => a.propertyId.localeCompare(b.propertyId));

  return {
    hotels,
    counts: {
      distinct: hotels.length,
      canonical31: canonical31.length,
      aimbridge: aimbridge.length,
      adminInventory: adminIds.length,
      published: publishedIds.length,
      durableCurrent: durableIds.length,
      fixtureProfiles: fixtureIds.length,
      runtimeProperties: runtimeIds.length,
      activeShareProperties: shareIds.length,
      baselineComparisonAdpReviewsPrior: 39,
    },
  };
}

/**
 * Discover full GDI universe from config ∪ data ∪ shares ∪ selectable.
 */
export function discoverGdiUniverse(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const selectable = listGdiSelectableHotels() || [];
  const configIds = gdiConfigHotelIds(cwd);
  const dataIds = gdiDataHotelIds(cwd);
  const shareIds = [...activeGdiHotelIds(cwd)];
  const pdfIds = listDirNames(join(cwd, "reports/group-demand-intelligence/pdf"));

  const byId = new Map();
  const touch = (id, source, extra = {}) => {
    if (!id || String(id).startsWith("gdi_hotel_")) return;
    const row = byId.get(id) || { hotelId: id, sources: new Set() };
    row.sources.add(source);
    Object.assign(row, extra);
    byId.set(id, row);
  };
  for (const id of configIds) touch(id, "config");
  for (const id of dataIds) touch(id, "data_hotel");
  for (const h of selectable) {
    touch(h.hotelId || h.id, "selectable", {
      displayName: h.displayName || h.name || null,
      adpPropertyId: h.config?.aliases?.adpPropertyId || h.adpPropertyId || null,
    });
  }
  for (const id of shareIds) touch(id, "active_share");
  for (const id of pdfIds) touch(id, "pdf_on_disk");

  const hotels = [...byId.values()]
    .map((r) => ({
      hotelId: r.hotelId,
      sources: [...r.sources].sort(),
      displayName: r.displayName || null,
      adpPropertyId: r.adpPropertyId || null,
    }))
    .sort((a, b) => a.hotelId.localeCompare(b.hotelId));

  return {
    hotels,
    counts: {
      distinct: hotels.length,
      config: configIds.length,
      dataHotels: dataIds.length,
      selectable: selectable.length,
      activeShareHotels: shareIds.length,
      pdfOnDisk: pdfIds.length,
      baselineComparisonGdiCatalogPrior: 22,
    },
  };
}

function classifyAdpResult({
  hasEvidence,
  coverageStatus,
  pdfReady,
  shareAvailable,
  inAdmin,
}) {
  if (!hasEvidence) return ADP_RESULT.EVIDENCE_MISSING;
  if (coverageStatus === "BLOCKED") return ADP_RESULT.BLOCKED;
  if (shareAvailable && (!pdfReady || coverageStatus === "NEEDS_REBUILD")) {
    return pdfReady ? ADP_RESULT.REVIEW_REBUILDABLE : ADP_RESULT.PDF_REBUILDABLE;
  }
  if (!shareAvailable && hasEvidence && pdfReady && coverageStatus === "READY") {
    return ADP_RESULT.REQUIRES_FOUNDER_APPROVAL;
  }
  if (!inAdmin && hasEvidence) return ADP_RESULT.REGISTRY_REPAIR;
  if (coverageStatus === "NEEDS_REBUILD") return ADP_RESULT.REVIEW_REBUILDABLE;
  if (!pdfReady && hasEvidence) return ADP_RESULT.PDF_REBUILDABLE;
  if (shareAvailable && pdfReady && coverageStatus === "READY") {
    return ADP_RESULT.COMPLETE;
  }
  if (!shareAvailable) return ADP_RESULT.REQUIRES_FOUNDER_APPROVAL;
  return ADP_RESULT.BLOCKED;
}

/**
 * Classify one ADP property against coverage + evidence + share probes.
 */
export function classifyAdpHotel(propertyId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const coverageById = opts.coverageById || new Map();
  const adminIds = opts.adminIds || new Set();
  const cov = coverageById.get(propertyId) || null;
  const profile = loadPropertyProfile(propertyId);
  const periods = loadAllPeriods(propertyId) || [];
  const manifest = loadPublishedManifest(propertyId);
  const published = loadPublishedReport(propertyId);
  const artifacts = assessOwnerAppReportArtifacts(propertyId, { cwd });
  const periodId =
    cov?.publishedPeriodId ||
    manifest?.latestPeriodId ||
    artifacts.durablePeriodId ||
    periods[periods.length - 1]?.periodId ||
    null;
  const hasEvidence =
    periods.length > 0 ||
    Boolean(cov?.hasPublishedReport) ||
    Boolean(published?.ok || published?.period || published?.property) ||
    artifacts.hasDurableSnapshotReport === true ||
    artifacts.hasCurrentPointer === true;
  const pdfReady = cov?.hasPdf === true || cov?.pdfStatus === "READY";
  const share = probeAdpClientShareAvailable(propertyId);
  const coverageStatus =
    cov?.coverageStatus ||
    (hasEvidence ? (pdfReady ? "READY" : "NEEDS_REBUILD") : null);
  const result = classifyAdpResult({
    hasEvidence,
    coverageStatus,
    pdfReady,
    shareAvailable: share.available === true,
    inAdmin: adminIds.has(propertyId),
  });

  const repairs = [];
  if (!adminIds.has(propertyId) && hasEvidence) repairs.push("add_to_admin_inventory");
  if (hasEvidence && !pdfReady) repairs.push("rebuild_performance_review_pdf");
  if (hasEvidence && coverageStatus === "NEEDS_REBUILD") {
    repairs.push("rebuild_review_artifact");
  }
  if (!share.available && hasEvidence) repairs.push("founder_share_approval");
  if (!getCensusRecordIdForAdpProperty(propertyId) && !profile?.censusRecordId) {
    repairs.push("census_link_missing");
  }
  if (!hasEvidence) repairs.push("evidence_missing_no_paid_research");

  const evidenceBits = [];
  if (periods.length) evidenceBits.push(`${periods.length} runtime`);
  if (artifacts.hasDurableSnapshotReport) evidenceBits.push("durable");
  if (published?.ok || published?.period) evidenceBits.push("published");
  if (artifacts.hasCurrentPointer && !evidenceBits.length) {
    evidenceBits.push("current_pointer");
  }

  return {
    hotel: profile?.name || cov?.propertyName || propertyId,
    propertyId,
    censusRecordId:
      getCensusRecordIdForAdpProperty(propertyId) ||
      profile?.censusRecordId ||
      null,
    evidence: hasEvidence
      ? `OK (${evidenceBits.join("+") || "present"}${periodId ? `; current=${periodId}` : ""})`
      : "MISSING",
    review: coverageStatus || (hasEvidence ? "NOT_IN_COVERAGE" : "N/A"),
    pdf: pdfReady ? "READY" : "MISSING",
    externalUrl: share.available
      ? `AUTHORIZED (${share.tokenId || "token"})`
      : "UNAUTHORIZED",
    repairNeeded: repairs.length ? repairs.join("; ") : "none",
    result,
    shareReason: share.reason || null,
    sources: opts.sources || [],
  };
}

/**
 * Classify one GDI hotel (async — uses getGdiPdfReportData).
 */
export async function classifyGdiHotel(hotelId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const displayName = opts.displayName || null;
  let summary = {
    available: false,
    reason: "unavailable",
    ready: 0,
    watch: 0,
    hotelName: displayName || hotelId,
  };
  try {
    const data = await getGdiPdfReportData(hotelId);
    if (data?.readFailed) {
      summary = {
        available: false,
        readFailed: true,
        reason: data.reason || "canonical_opportunity_read_failed",
        ready: 0,
        watch: 0,
        hotelName: data.hotelName || displayName || hotelId,
      };
    } else if (data?.available) {
      summary = {
        available: true,
        ready: data.executiveSummary?.customerReadyCount ?? 0,
        watch: data.executiveSummary?.futureWatchCount ?? 0,
        hotelName: data.hotel?.name || displayName || hotelId,
      };
    } else {
      summary = {
        available: false,
        reason: data?.reason || "unavailable",
        ready: 0,
        watch: 0,
        hotelName: data?.hotelName || displayName || hotelId,
      };
    }
  } catch (err) {
    summary = {
      available: false,
      reason: err?.message || "read_error",
      ready: 0,
      watch: 0,
      hotelName: displayName || hotelId,
    };
  }

  const pdfReady = hasGdiReportPdf(hotelId);
  const shareFlags = resolveGdiAdminShareFlags(hotelId, { confirmUrls: false });
  const reportStatus = deriveGdiReportStatus(summary);
  const row = normalizeGdiAdminReportRow({
    hotelId,
    hotelName: summary.hotelName,
    readyCount: summary.ready,
    watchCount: summary.watch,
    reportStatus,
    pdfReady,
    pdfStatus: derivePdfStatus(pdfReady),
    gdiShareAvailable: shareFlags.gdiShareAvailable,
    gdiShareReason: shareFlags.gdiShareReason,
    available: summary.available,
  });

  let result = GDI_RESULT.BLOCKED;
  const repairs = [];
  if (!summary.available && summary.readFailed) {
    result = GDI_RESULT.EVIDENCE_MISSING;
    repairs.push("canonical_opportunity_read_failed");
  } else if (!summary.available) {
    result = GDI_RESULT.EVIDENCE_MISSING;
    repairs.push("gdi_data_unavailable");
  } else if (reportStatus === "WATCH_ONLY") {
    // Do not treat zero-eligible WATCH_ONLY as commercial PDF_REBUILDABLE.
    // Existing watchlist PDF (same generator) → WATCH_ONLY; otherwise await policy.
    if (pdfReady) {
      result = GDI_RESULT.WATCH_ONLY;
    } else if (Number(summary.ready || 0) === 0) {
      result = GDI_RESULT.AWAITING_REPORT_POLICY;
      repairs.push(
        "watch_only_zero_eligible_ops — do not auto-build empty commercial PDF; founder watchlist/monitoring PDF policy required"
      );
    } else {
      result = GDI_RESULT.WATCH_ONLY_PDF_NOT_APPLICABLE;
      repairs.push("watch_only_pdf_not_applicable");
    }
  } else if (reportStatus === "READY" && !pdfReady) {
    result = GDI_RESULT.PDF_REBUILDABLE;
    repairs.push("rebuild_gdi_pdf_from_existing_evidence");
  } else if (reportStatus === "READY" && pdfReady && shareFlags.gdiShareAvailable) {
    result = GDI_RESULT.COMPLETE;
  } else if (reportStatus === "READY" && pdfReady && !shareFlags.gdiShareAvailable) {
    result = GDI_RESULT.REQUIRES_FOUNDER_APPROVAL;
    repairs.push("founder_gdi_share_approval");
  } else if (shareFlags.gdiShareAvailable && !pdfReady) {
    result = GDI_RESULT.PDF_REBUILDABLE;
    repairs.push("rebuild_pdf_before_share_delivery");
  } else {
    result = GDI_RESULT.BLOCKED;
    repairs.push(summary.reason || reportStatus || "blocked");
  }

  // On-disk PDF without selectable summary still counts as registry signal
  if (!summary.available && pdfReady) {
    repairs.push("pdf_on_disk_but_catalog_unavailable");
  }

  let pdfLabel = pdfReady ? "READY" : "MISSING";
  if (
    !pdfReady &&
    (result === GDI_RESULT.AWAITING_REPORT_POLICY ||
      result === GDI_RESULT.WATCH_ONLY_PDF_NOT_APPLICABLE)
  ) {
    pdfLabel =
      result === GDI_RESULT.WATCH_ONLY_PDF_NOT_APPLICABLE
        ? "NOT_APPLICABLE"
        : "AWAITING_REPORT_POLICY";
  }

  return {
    hotel: row.hotelName || hotelId,
    hotelId,
    eligibleOps: summary.ready,
    watchOps: summary.watch,
    reportStatus,
    pdf: pdfLabel,
    externalUrl: shareFlags.gdiShareAvailable
      ? `AUTHORIZED (${shareFlags.gdiShareTokenId || "token"})`
      : "UNAUTHORIZED",
    publication: shareFlags.gdiShareAvailable ? "ACTIVE_SHARE" : "NOT_AUTHORIZED",
    repairNeeded: repairs.length ? repairs.join("; ") : "none",
    result,
    shareReason: shareFlags.gdiShareReason || null,
    sources: opts.sources || [],
  };
}

/**
 * Production ACTIVE ADP share health using sealed capability reconstruction.
 * Prefer the local admin API (runtime env / production-matching secret) over
 * process-local dotenv reconstruction, which can diverge and false-fail as
 * invalid_share_signature. Never mints tokens.
 */
export async function probeActiveAdpShareHealth(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const base =
    opts.productionBase ||
    "https://my-operators-backend-production.up.railway.app";
  const adminBase = (opts.adminBase || "http://127.0.0.1:8080").replace(
    /\/$/,
    ""
  );
  const { resolveExternalClientLinks } = await import(
    "./report-external-client-links-v1.js"
  );
  const propertyIds = [...activeAdpPropertyIds(cwd)].sort();
  const rows = [];
  for (const propertyId of propertyIds) {
    const row = {
      propertyId,
      available: false,
      tokenId: null,
      httpStatus: null,
      error: null,
      pageStatus: null,
      ok: false,
      sealSource: null,
    };
    try {
      let links = null;
      try {
        const adminRes = await fetch(
          `${adminBase}/api/admin/ai-demand/hotels/${encodeURIComponent(propertyId)}/external-client-links`
        );
        if (adminRes.ok) {
          links = await adminRes.json();
          row.sealSource = "admin_runtime";
        }
      } catch {
        links = null;
      }
      if (!links?.adp?.url) {
        links = resolveExternalClientLinks(propertyId, {
          createIfMissing: false,
        });
        row.sealSource = "local_reconstruct";
      }
      row.available = links.adp?.available === true;
      row.tokenId = links.adp?.tokenId || null;
      row.reason = links.adp?.reason || null;
      const url = links.adp?.url || null;
      if (!url) {
        row.error = row.reason || "no_sealed_url";
        rows.push(row);
        continue;
      }
      const share = new URL(url).searchParams.get("share");
      if (!share || !share.includes(".")) {
        row.error = "malformed_or_unsealed_share";
        rows.push(row);
        continue;
      }
      const api = await fetch(
        `${base}/api/ai-demand-positioning/property/${encodeURIComponent(propertyId)}/report`,
        {
          headers: {
            Accept: "application/json",
            "X-ADP-Share-Capability": share,
          },
        }
      );
      const body = await api.json().catch(() => ({}));
      row.httpStatus = api.status;
      row.ok = api.status === 200 && body?.ok !== false;
      row.error = body?.error || (row.ok ? null : `http_${api.status}`);
      const page = await fetch(url, { redirect: "manual" });
      row.pageStatus = page.status;
    } catch (err) {
      row.error = String(err?.message || err).slice(0, 200);
    }
    rows.push(row);
  }
  const failing = rows.filter(
    (r) => r.httpStatus === 401 || r.httpStatus === 403 || (r.available && !r.ok)
  );
  return {
    gate: "ADP_ACTIVE_EXTERNAL_SHARE_HEALTH_V1",
    generatedAt: new Date().toISOString(),
    productionBase: base,
    adminBase,
    activePropertyCount: propertyIds.length,
    probed: rows.length,
    okCount: rows.filter((r) => r.ok).length,
    fail401: rows.filter((r) => r.httpStatus === 401).length,
    fail403: rows.filter((r) => r.httpStatus === 403).length,
    unavailable: rows.filter((r) => !r.available).length,
    note:
      "Sealed URLs resolved via admin runtime when available — local dotenv secret divergence must not be treated as customer-link breakage.",
    failing,
    rows,
  };
}

/**
 * Full ADP + GDI reconciliation plan (read-only unless caller applies rebuilds).
 */
export async function buildFullE2eRecoveryPlan(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const adpUniverse = discoverAdpUniverse({ cwd });
  const gdiUniverse = discoverGdiUniverse({ cwd });

  const coverage = buildPublishedAdpReviewCoverageV1({});
  const coverageById = new Map(
    (coverage.properties || []).map((p) => [p.propertyId, p])
  );
  const adminInv = listInternalOwnerAppPropertyInventory({ cwd });
  const adminIds = new Set((adminInv || []).map((r) => r.propertyId).filter(Boolean));

  const adpRows = [];
  for (const h of adpUniverse.hotels) {
    adpRows.push(
      classifyAdpHotel(h.propertyId, {
        cwd,
        coverageById,
        adminIds,
        sources: h.sources,
      })
    );
  }

  const gdiRows = [];
  for (const h of gdiUniverse.hotels) {
    gdiRows.push(
      await classifyGdiHotel(h.hotelId, {
        cwd,
        displayName: h.displayName,
        sources: h.sources,
      })
    );
  }

  const adpByResult = {};
  for (const r of adpRows) {
    adpByResult[r.result] = (adpByResult[r.result] || 0) + 1;
  }
  const gdiByResult = {};
  for (const r of gdiRows) {
    gdiByResult[r.result] = (gdiByResult[r.result] || 0) + 1;
  }

  const gdiCounts = reconcileGdiAdminReportCounts(
    gdiRows.map((r) =>
      normalizeGdiAdminReportRow({
        hotelId: r.hotelId,
        hotelName: r.hotel,
        readyCount: r.eligibleOps,
        watchCount: r.watchOps,
        reportStatus: r.reportStatus,
        pdfReady: r.pdf === "READY",
        pdfStatus: r.pdf,
        gdiShareAvailable: r.publication === "ACTIVE_SHARE",
        available: r.reportStatus === "READY" || r.reportStatus === "WATCH_ONLY",
      })
    )
  );

  const authorizedAdp = adpRows.filter((r) =>
    String(r.externalUrl).startsWith("AUTHORIZED")
  ).length;
  const authorizedGdi = gdiRows.filter((r) =>
    String(r.externalUrl).startsWith("AUTHORIZED")
  ).length;

  return {
    gate: DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1,
    generatedAt: new Date().toISOString(),
    dryRun: opts.dryRun !== false,
    mintShares: false,
    paidResearch: false,
    adp: {
      universe: adpUniverse.counts,
      rows: adpRows,
      byResult: adpByResult,
      complete: adpByResult[ADP_RESULT.COMPLETE] || 0,
      authorizedExternalLinks: authorizedAdp,
      requiresFounderApproval:
        adpByResult[ADP_RESULT.REQUIRES_FOUNDER_APPROVAL] || 0,
      evidenceMissing: adpByResult[ADP_RESULT.EVIDENCE_MISSING] || 0,
      pdfRebuildable: adpByResult[ADP_RESULT.PDF_REBUILDABLE] || 0,
      reviewRebuildable: adpByResult[ADP_RESULT.REVIEW_REBUILDABLE] || 0,
      coveragePropertyCount: (coverage.properties || []).length,
      adminInventoryCount: adminIds.size,
    },
    gdi: {
      universe: gdiUniverse.counts,
      rows: gdiRows,
      byResult: gdiByResult,
      catalogCounts: gdiCounts,
      complete: gdiByResult[GDI_RESULT.COMPLETE] || 0,
      authorizedExternalLinks: authorizedGdi,
      requiresFounderApproval:
        gdiByResult[GDI_RESULT.REQUIRES_FOUNDER_APPROVAL] || 0,
      evidenceMissing: gdiByResult[GDI_RESULT.EVIDENCE_MISSING] || 0,
      pdfRebuildable: gdiByResult[GDI_RESULT.PDF_REBUILDABLE] || 0,
      watchOnly: gdiByResult[GDI_RESULT.WATCH_ONLY] || 0,
      watchOnlyPdfNotApplicable:
        gdiByResult[GDI_RESULT.WATCH_ONLY_PDF_NOT_APPLICABLE] || 0,
      awaitingReportPolicy:
        gdiByResult[GDI_RESULT.AWAITING_REPORT_POLICY] || 0,
      pdfReady: gdiCounts.pdfReady,
      pdfMissing: gdiCounts.needsPdf ?? gdiRows.filter((r) => r.pdf === "MISSING").length,
    },
    identityNotes: {
      adpToCensusLinks: adpRows.filter((r) => r.censusRecordId).length,
      adpWithoutCensusLink: adpRows.filter((r) => !r.censusRecordId).length,
      gdiWithAdpAlias: gdiUniverse.hotels.filter((h) => h.adpPropertyId).length,
    },
    inventoryDeltas: {
      adpDistinctVsPriorReviewsBaseline39:
        adpUniverse.counts.distinct - 39,
      gdiDistinctVsPriorCatalogBaseline22:
        gdiUniverse.counts.distinct - 22,
      explanation: [
        "ADP admin inventory (~35) = canonical 31 + Aimbridge 3 + eligible fixture extras; AI Demand Reviews coverage tracks published/durable (~27–39 historically).",
        "GDI selectable catalog (~22 previously) is a filtered subset of config∪data hotels; full discovery includes all onboarded configs, data dirs, ACTIVE shares, and PDF-on-disk hotels.",
      ],
    },
  };
}

export function renderFounderMarkdownTables(plan) {
  const adpLines = [
    "| Hotel | Evidence | Review | PDF | External URL | Repair needed | Result |",
    "|---|---|---|---|---|---|---|",
  ];
  for (const r of plan.adp.rows) {
    adpLines.push(
      `| ${r.hotel} | ${r.evidence} | ${r.review} | ${r.pdf} | ${r.externalUrl} | ${r.repairNeeded} | ${r.result} |`
    );
  }
  const gdiLines = [
    "| Hotel | Eligible ops | PDF | External URL | Publication | Repair needed | Result |",
    "|---|---|---|---|---|---|---|",
  ];
  for (const r of plan.gdi.rows) {
    gdiLines.push(
      `| ${r.hotel} | ${r.eligibleOps} | ${r.pdf} | ${r.externalUrl} | ${r.publication} | ${r.repairNeeded} | ${r.result} |`
    );
  }
  return { adpMarkdown: adpLines.join("\n"), gdiMarkdown: gdiLines.join("\n") };
}

/** Hotels eligible for deterministic GDI PDF rebuild (no new research, no share mint). */
export function listGdiPdfRebuildCandidates(plan) {
  return (plan?.gdi?.rows || []).filter(
    (r) =>
      r.result === GDI_RESULT.PDF_REBUILDABLE &&
      Number(r.eligibleOps || 0) > 0 &&
      r.pdf === "MISSING"
  );
}
