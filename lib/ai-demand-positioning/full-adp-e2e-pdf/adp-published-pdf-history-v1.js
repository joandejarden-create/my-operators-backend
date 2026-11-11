/**
 * ADP_PUBLISHED_PDF_HISTORY_V1
 *
 * Ledger of officially published Full ADP PDFs (current + historical).
 * Distinct from staged drafts and Performance Review archive PDFs.
 *
 * Storage (local / future durable mount):
 *   data/ai-demand-positioning/published-pdf-history/{propertyId}/index.json
 *   + copies under .../{periodId}/report.pdf when archived at publish time
 *
 * Also surfaces verified Full ADP artifact store versions that were marked
 * publicationStatus=PUBLISHED, and dealality-report-archive ADP PDF entries.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  FULL_ADP_E2E_PDF_ROOT,
  fullAdpE2ePdfPaths,
  sha256File,
  resolveFullAdpE2ePdfArtifact,
} from "./adp-full-e2e-pdf-artifact-store-v1.js";
import { loadPublishedManifest } from "../published-snapshot.js";
import { getHotelPublicationRecord } from "../review/adp-publication-registry-v1.js";
import { listReportArchiveEntries } from "../../dealality-report-archive/report-archive-store-v1.js";

export const ADP_PUBLISHED_PDF_HISTORY_V1 = "ADP_PUBLISHED_PDF_HISTORY_V1";

const MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

function monthYearFromPeriodId(periodId) {
  const m = String(periodId || "").match(
    /_(\d{4})(\d{2})(\d{2})\d{6}_[a-f0-9]{6}$/i
  );
  if (!m) return null;
  const monthIdx = Number(m[2]) - 1;
  if (monthIdx < 0 || monthIdx > 11) return null;
  return `${MONTH_NAMES[monthIdx]} ${Number(m[1])}`;
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const PUBLISHED_PDF_HISTORY_ROOT = path.join(
  ROOT,
  "data/ai-demand-positioning/published-pdf-history"
);

function historyIndexPath(propertyId) {
  return path.join(PUBLISHED_PDF_HISTORY_ROOT, propertyId, "index.json");
}

function emptyIndex(propertyId) {
  return {
    contract: ADP_PUBLISHED_PDF_HISTORY_V1,
    propertyId,
    updatedAt: null,
    entries: [],
  };
}

export function loadPublishedPdfHistoryIndex(propertyId) {
  const id = String(propertyId || "").trim();
  if (!id) return emptyIndex("");
  const p = historyIndexPath(id);
  if (!fs.existsSync(p)) return emptyIndex(id);
  try {
    const doc = JSON.parse(fs.readFileSync(p, "utf8"));
    if (!Array.isArray(doc.entries)) return emptyIndex(id);
    return doc;
  } catch {
    return emptyIndex(id);
  }
}

export function savePublishedPdfHistoryIndex(doc) {
  const id = String(doc.propertyId || "").trim();
  if (!id) throw new Error("propertyId_required");
  const dir = path.join(PUBLISHED_PDF_HISTORY_ROOT, id);
  fs.mkdirSync(dir, { recursive: true });
  const next = {
    ...doc,
    contract: ADP_PUBLISHED_PDF_HISTORY_V1,
    propertyId: id,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(historyIndexPath(id), `${JSON.stringify(next, null, 2)}\n`);
  return next;
}

/**
 * Record a newly published PDF as Current; demote prior Current → Historical.
 * Does not overwrite PDF bytes. Copies into history root when sourcePath provided.
 */
export function archivePublishedPdfOnPublish({
  propertyId,
  periodId,
  stagedVersionId,
  pdfSha256,
  pdfSourcePath = null,
  publishedAt = null,
  authorizationRecord = null,
  dryRun = true,
} = {}) {
  const id = String(propertyId || "").trim();
  if (!id || !periodId || !pdfSha256) {
    return { ok: false, error: "propertyId_periodId_pdfSha256_required" };
  }
  const index = loadPublishedPdfHistoryIndex(id);
  const existing = index.entries.find(
    (e) =>
      e.periodId === periodId &&
      String(e.pdfSha256 || "").toLowerCase() === String(pdfSha256).toLowerCase()
  );
  if (existing) {
    return {
      ok: true,
      idempotent: true,
      dryRun: !!dryRun,
      entry: existing,
      index,
    };
  }
  const ts = publishedAt || new Date().toISOString();
  const entry = {
    periodId,
    stagedVersionId: stagedVersionId || null,
    publishedAt: ts,
    reportMonthYear: monthYearFromPeriodId(periodId),
    pdfSha256,
    status: "CURRENT",
    authorizationRecord: authorizationRecord || null,
    pdfRelativePath: null,
    unavailable: false,
  };

  if (!dryRun && pdfSourcePath && fs.existsSync(pdfSourcePath)) {
    const sourceSha = sha256File(pdfSourcePath);
    if (sourceSha !== pdfSha256) {
      return {
        ok: false,
        error: "archive_source_sha_mismatch",
        expected: pdfSha256,
        actual: sourceSha,
      };
    }
    const destDir = path.join(PUBLISHED_PDF_HISTORY_ROOT, id, periodId);
    fs.mkdirSync(destDir, { recursive: true });
    const dest = path.join(destDir, "report.pdf");
    if (fs.existsSync(dest)) {
      const existing = sha256File(dest);
      if (existing !== pdfSha256) {
        // Preserve conflicting same-period bytes under a SHA-qualified name.
        // Never delete; never silently substitute. Then install the authorized PDF.
        const preserved = path.join(
          destDir,
          `report-${existing.slice(0, 16)}.pdf`
        );
        if (!fs.existsSync(preserved)) {
          fs.copyFileSync(dest, preserved);
        }
        fs.copyFileSync(pdfSourcePath, dest);
      }
    } else {
      fs.copyFileSync(pdfSourcePath, dest);
    }
    const live = sha256File(dest);
    if (live !== pdfSha256) {
      return {
        ok: false,
        error: "archive_sha_mismatch",
        expected: pdfSha256,
        actual: live,
      };
    }
    entry.pdfRelativePath = path.relative(ROOT, dest).replace(/\\/g, "/");
  }

  if (dryRun) {
    return { ok: true, dryRun: true, wouldArchive: entry, index };
  }

  for (const e of index.entries) {
    if (e.status === "CURRENT") e.status = "HISTORICAL";
  }
  index.entries.unshift(entry);
  const saved = savePublishedPdfHistoryIndex(index);
  return { ok: true, dryRun: false, entry, index: saved };
}

function listFullAdpVersionDirs(propertyId) {
  const dir = path.join(FULL_ADP_E2E_PDF_ROOT, propertyId);
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .filter((d) => d.isDirectory() && d.name.startsWith("staged_"))
    .map((d) => d.name);
}

/**
 * Build Admin-facing history rows: only versions that were actually published.
 * Prefer explicit history index; fall back to published manifest + artifact store
 * when marked published; never include pure staged drafts.
 */
export function listPublishedPdfHistoryForAdmin(propertyId) {
  const id = String(propertyId || "").trim();
  const index = loadPublishedPdfHistoryIndex(id);
  const rows = [];
  const seen = new Set();

  function pushRow(row) {
    const key = `${row.periodId || ""}:${String(row.pdfSha256 || "").toLowerCase()}`;
    if (seen.has(key) && row.pdfSha256) return;
    if (row.pdfSha256) seen.add(key);
    rows.push(row);
  }

  for (const e of index.entries || []) {
    let unavailable = e.unavailable === true;
    let verifiedSha = e.pdfSha256 || null;
    let absolutePath = null;
    if (e.pdfRelativePath) {
      absolutePath = path.join(ROOT, e.pdfRelativePath);
      if (!fs.existsSync(absolutePath)) {
        unavailable = true;
      } else {
        try {
          const live = sha256File(absolutePath);
          if (e.pdfSha256 && live !== e.pdfSha256) unavailable = true;
          else verifiedSha = live;
        } catch {
          unavailable = true;
        }
      }
    }
    pushRow({
      source: "published_pdf_history_index",
      status: e.status || "HISTORICAL",
      periodId: e.periodId,
      reportMonthYear: e.reportMonthYear || monthYearFromPeriodId(e.periodId),
      publishedAt: e.publishedAt || null,
      stagedVersionId: e.stagedVersionId || null,
      pdfSha256: verifiedSha,
      unavailable,
      absolutePath: unavailable ? null : absolutePath,
      authorizationRecord: e.authorizationRecord || null,
    });
  }

  // Current published binding via manifest period + Full ADP artifact match
  try {
    const manifest = loadPublishedManifest(id);
    const pubRec = getHotelPublicationRecord(id);
    const periodId = manifest?.latestPeriodId || null;
    const stagedVersionIdHint =
      manifest?.sourceStagedVersionId ||
      pubRec?.publishedReportVersion ||
      pubRec?.approvedReportVersion ||
      null;
    if (periodId) {
      let art = stagedVersionIdHint
        ? resolveFullAdpE2ePdfArtifact(id, {
            expectedStagedVersionId: stagedVersionIdHint,
          })
        : { available: false };
      if (!art.available) {
        // Match artifact whose meta.periodId equals published period
        for (const ver of listFullAdpVersionDirs(id)) {
          const { metaJson, reportPdf } = fullAdpE2ePdfPaths(id, ver);
          if (!fs.existsSync(metaJson) || !fs.existsSync(reportPdf)) continue;
          let meta;
          try {
            meta = JSON.parse(fs.readFileSync(metaJson, "utf8"));
          } catch {
            continue;
          }
          if (String(meta.periodId || "") !== String(periodId)) continue;
          art = resolveFullAdpE2ePdfArtifact(id, {
            expectedStagedVersionId: meta.stagedVersionId || ver,
          });
          if (art.available) break;
        }
      }
      if (!art.available) {
        // Last resort: latest Full ADP if its period matches published
        const latest = resolveFullAdpE2ePdfArtifact(id);
        if (
          latest.available &&
          String(latest.periodId || "") === String(periodId)
        ) {
          art = latest;
        }
      }
      if (art.available && art.pdfSha256) {
        const key = `${periodId}:${art.pdfSha256.toLowerCase()}`;
        if (!seen.has(key)) {
          pushRow({
            source: "current_published_binding",
            status: "CURRENT",
            periodId,
            reportMonthYear: monthYearFromPeriodId(periodId),
            publishedAt:
              manifest?.latestPublishedAt || pubRec?.publishedAt || null,
            stagedVersionId: art.stagedVersionId || stagedVersionIdHint,
            pdfSha256: art.pdfSha256,
            unavailable: false,
            absolutePath: art.pdfAbsolutePath || null,
            authorizationRecord: {
              publicationState: pubRec?.state || null,
              publishedReportVersion: pubRec?.publishedReportVersion || null,
              binding: "published_period_to_full_adp_pdf",
            },
          });
        }
      } else if (!seen.has(`${periodId}:`)) {
        // Published report exists but no immutable Full ADP PDF for that period
        pushRow({
          source: "current_published_binding_missing_pdf",
          status: "CURRENT",
          periodId,
          reportMonthYear: monthYearFromPeriodId(periodId),
          publishedAt:
            manifest?.latestPublishedAt || pubRec?.publishedAt || null,
          stagedVersionId: stagedVersionIdHint,
          pdfSha256: null,
          unavailable: true,
          absolutePath: null,
          authorizationRecord: {
            publicationState: pubRec?.state || null,
            publishedReportVersion: pubRec?.publishedReportVersion || null,
            binding: "published_period_pdf_unavailable",
            note: "Do not reconstruct from newer staged PDF",
          },
        });
      }
    }
  } catch {
    /* ignore */
  }

  // Artifact store versions explicitly marked PUBLISHED
  for (const ver of listFullAdpVersionDirs(id)) {
    const { metaJson, reportPdf } = fullAdpE2ePdfPaths(id, ver);
    if (!fs.existsSync(metaJson) || !fs.existsSync(reportPdf)) continue;
    let meta;
    try {
      meta = JSON.parse(fs.readFileSync(metaJson, "utf8"));
    } catch {
      continue;
    }
    const status = String(meta.publicationStatus || "");
    if (status !== "PUBLISHED" && meta.published !== true) continue;
    let liveSha;
    try {
      liveSha = sha256File(reportPdf);
    } catch {
      continue;
    }
    if (meta.pdfSha256 && meta.pdfSha256 !== liveSha) continue;
    pushRow({
      source: "full_adp_artifact_published_flag",
      status: "HISTORICAL",
      periodId: meta.periodId || null,
      reportMonthYear: monthYearFromPeriodId(meta.periodId),
      publishedAt: meta.publishedAt || meta.generatedAt || null,
      stagedVersionId: meta.stagedVersionId || ver,
      pdfSha256: liveSha,
      unavailable: false,
      absolutePath: reportPdf,
      authorizationRecord: {
        publicationStatus: status || "PUBLISHED",
      },
    });
  }

  // Report archive ADP PDF entries (if any)
  try {
    const archived = listReportArchiveEntries({ hotelId: id, reportType: "ADP" });
    for (const a of archived || []) {
      if (!a?.pdfPath && !a?.files?.pdf) continue;
      const pdfPath = a.pdfPath || a.files?.pdf;
      if (!pdfPath || !fs.existsSync(pdfPath)) {
        pushRow({
          source: "report_archive",
          status: "HISTORICAL",
          periodId: a.periodId || a.reportingPeriodId || null,
          reportMonthYear: monthYearFromPeriodId(
            a.periodId || a.reportingPeriodId
          ),
          publishedAt: a.publishedAt || a.createdAt || null,
          stagedVersionId: a.stagedVersionId || a.versionId || null,
          pdfSha256: a.pdfSha256 || a.checksum || null,
          unavailable: true,
          absolutePath: null,
          authorizationRecord: { archiveId: a.id || a.entryId || null },
        });
        continue;
      }
      const live = sha256File(pdfPath);
      pushRow({
        source: "report_archive",
        status: "HISTORICAL",
        periodId: a.periodId || a.reportingPeriodId || null,
        reportMonthYear: monthYearFromPeriodId(
          a.periodId || a.reportingPeriodId
        ),
        publishedAt: a.publishedAt || a.createdAt || null,
        stagedVersionId: a.stagedVersionId || a.versionId || null,
        pdfSha256: live,
        unavailable: false,
        absolutePath: pdfPath,
        authorizationRecord: { archiveId: a.id || a.entryId || null },
      });
    }
  } catch {
    /* archive optional */
  }

  // Ensure exactly one CURRENT when possible
  const currents = rows.filter((r) => r.status === "CURRENT" && !r.unavailable);
  if (currents.length > 1) {
    currents.sort((a, b) =>
      String(b.publishedAt || "").localeCompare(String(a.publishedAt || ""))
    );
    for (let i = 1; i < currents.length; i += 1) {
      currents[i].status = "HISTORICAL";
    }
  }

  // Refuse to treat CURRENT bytes as a distinct historical archive.
  const currentOk = rows.find((r) => r.status === "CURRENT" && !r.unavailable);
  for (const r of rows) {
    if (r.status !== "HISTORICAL" || r.unavailable) continue;
    if (
      currentOk &&
      r.pdfSha256 &&
      currentOk.pdfSha256 &&
      String(r.pdfSha256).toLowerCase() ===
        String(currentOk.pdfSha256).toLowerCase() &&
      String(r.periodId || "") !== String(currentOk.periodId || "")
    ) {
      r.unavailable = true;
      r.absolutePath = null;
      r.unavailableReason =
        "historical_bytes_not_distinct_from_current_refusing_substitution";
    }
  }

  rows.sort((a, b) => {
    if (a.status === "CURRENT" && b.status !== "CURRENT") return -1;
    if (b.status === "CURRENT" && a.status !== "CURRENT") return 1;
    return String(b.publishedAt || "").localeCompare(String(a.publishedAt || ""));
  });

  function historyPdfUrl(r, download) {
    if (r.unavailable || !r.periodId) return null;
    const q = new URLSearchParams();
    q.set("periodId", r.periodId);
    if (r.pdfSha256) q.set("pdfSha256", r.pdfSha256);
    if (download) q.set("download", "1");
    return `/api/admin/ai-demand-positioning/published-pdf-history/${encodeURIComponent(id)}/pdf?${q.toString()}`;
  }

  return {
    contract: ADP_PUBLISHED_PDF_HISTORY_V1,
    propertyId: id,
    count: rows.length,
    entries: rows.map((r) => ({
      status: r.status,
      periodId: r.periodId,
      reportMonthYear: r.reportMonthYear,
      publishedAt: r.publishedAt,
      stagedVersionId: r.stagedVersionId,
      pdfSha256: r.pdfSha256,
      unavailable: r.unavailable === true,
      unavailableReason: r.unavailableReason || null,
      source: r.source,
      viewUrl: historyPdfUrl(r, false),
      downloadUrl: historyPdfUrl(r, true),
      authorizationRecord: r.authorizationRecord || null,
      labelUnavailable: r.unavailable
        ? "Historical PDF unavailable"
        : null,
    })),
  };
}

/**
 * Resolve immutable published PDF bytes for a specific period/sha.
 * Never substitutes the current published PDF for a historical period.
 */
export function resolveHistoricalPublishedPdfBytes(
  propertyId,
  { periodId, pdfSha256, stagedVersionId } = {}
) {
  const id = String(propertyId || "").trim();
  const wantPeriod = String(periodId || "").trim() || null;
  const wantSha = String(pdfSha256 || "").trim().toLowerCase() || null;
  const wantStaged = String(stagedVersionId || "").trim() || null;

  if (!id || (!wantPeriod && !wantSha && !wantStaged)) {
    return { ok: false, error: "periodId_or_pdfSha256_required" };
  }

  const list = listPublishedPdfHistoryForAdmin(id);
  // Prefer exact period match (stable historical identity).
  let match =
    (wantPeriod &&
      (list.entries || []).find((e) => e.periodId === wantPeriod)) ||
    null;
  if (!match && wantSha) {
    const shaMatches = (list.entries || []).filter(
      (e) =>
        e.pdfSha256 &&
        String(e.pdfSha256).toLowerCase() === wantSha &&
        !e.unavailable
    );
    // Ambiguous SHA across periods → require periodId
    if (shaMatches.length === 1) match = shaMatches[0];
    else if (shaMatches.length > 1) {
      return {
        ok: false,
        error: "ambiguous_pdf_sha256_require_periodId",
        label: "Historical PDF unavailable",
      };
    }
  }
  if (!match && wantStaged) {
    match = (list.entries || []).find(
      (e) => e.stagedVersionId === wantStaged && e.status === "CURRENT"
    );
  }
  if (!match) return { ok: false, error: "not_found" };
  if (match.unavailable) {
    return {
      ok: false,
      error: "historical_pdf_unavailable",
      label: "Historical PDF unavailable",
      reason: match.unavailableReason || null,
      entry: match,
    };
  }
  if (wantSha && match.pdfSha256 && match.pdfSha256.toLowerCase() !== wantSha) {
    return {
      ok: false,
      error: "pdfSha256_mismatch_for_period",
      label: "Historical PDF unavailable",
      entry: match,
    };
  }

  const index = loadPublishedPdfHistoryIndex(id);
  const idxEntry = (index.entries || []).find(
    (e) => e.periodId === match.periodId
  );
  if (idxEntry?.pdfRelativePath) {
    const abs = path.join(ROOT, idxEntry.pdfRelativePath);
    if (fs.existsSync(abs)) {
      const live = sha256File(abs);
      if (idxEntry.pdfSha256 && live !== idxEntry.pdfSha256) {
        return {
          ok: false,
          error: "sha256_mismatch",
          label: "Historical PDF unavailable",
          entry: match,
        };
      }
      // Extra guard: HISTORICAL must not share CURRENT bytes
      if (match.status === "HISTORICAL") {
        const current = (list.entries || []).find(
          (e) => e.status === "CURRENT" && !e.unavailable
        );
        if (
          current?.pdfSha256 &&
          live.toLowerCase() === String(current.pdfSha256).toLowerCase() &&
          current.periodId !== match.periodId
        ) {
          return {
            ok: false,
            error: "historical_pdf_unavailable",
            label: "Historical PDF unavailable",
            reason:
              "historical_bytes_not_distinct_from_current_refusing_substitution",
            entry: match,
          };
        }
      }
      return {
        ok: true,
        absolutePath: abs,
        pdfSha256: live,
        periodId: match.periodId,
        status: match.status,
        entry: match,
      };
    }
  }

  // CURRENT only: may resolve via Full ADP artifact for the same period.
  // Never use staged artifact fallback for HISTORICAL (avoids current substitution).
  if (match.status === "CURRENT" && match.stagedVersionId) {
    const art = resolveFullAdpE2ePdfArtifact(id, {
      expectedStagedVersionId: match.stagedVersionId,
    });
    if (
      art.available &&
      (!match.periodId ||
        !art.periodId ||
        String(art.periodId) === String(match.periodId))
    ) {
      return {
        ok: true,
        absolutePath: art.pdfAbsolutePath,
        pdfSha256: art.pdfSha256,
        periodId: match.periodId,
        status: match.status,
        entry: match,
      };
    }
  }

  return {
    ok: false,
    error: "historical_pdf_unavailable",
    label: "Historical PDF unavailable",
    entry: match,
  };
}
