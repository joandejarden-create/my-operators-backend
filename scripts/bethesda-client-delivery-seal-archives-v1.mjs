#!/usr/bin/env node
/**
 * Bethesda Pilot — seal Report Archive golden entries + regenerate corrected GDI PDF.
 *
 * Does NOT rewrite Oct 1 ADP baseline metrics or GDI Day-1 opportunity IDs.
 * Does NOT fabricate a historical Oct 1 PDF if none existed.
 *
 * Usage:
 *   node scripts/bethesda-client-delivery-seal-archives-v1.mjs --dry-run
 *   node scripts/bethesda-client-delivery-seal-archives-v1.mjs --apply
 */

import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { publishReportArchiveEntry, listReportArchiveEntries } from "../lib/dealality-report-archive/report-archive-store-v1.js";
import { generateGdiReportPdfV1 } from "../lib/group-demand-intelligence/reports/generate-gdi-report-pdf-v1.mjs";
import { getGdiPdfReportData } from "../lib/group-demand-intelligence/reports/gdi-pdf-report-data-v1.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const APPLY = process.argv.includes("--apply");
const ROOT = process.cwd();
const HOTEL_ID = "recLuxvwwxID7U2B8";
const PROPERTY_ID = "adp_bethesda_marriott";
const OUT = join(ROOT, "reports/bethesda-pilot/day2/2026-10-02/client-delivery-closure");

function sha256File(p) {
  return createHash("sha256").update(readFileSync(p)).digest("hex");
}

mkdirSync(OUT, { recursive: true });

const adpBaseline = JSON.parse(
  readFileSync(join(ROOT, "data/pilots/bethesda-marriott-001/adp-october-baseline-v1.json"), "utf8")
);
const gdiDay1 = JSON.parse(
  readFileSync(join(ROOT, "data/pilots/bethesda-marriott-001/gdi-day1-snapshot-2026-10-01.json"), "utf8")
);
const pilot = JSON.parse(
  readFileSync(join(ROOT, "data/pilots/bethesda-marriott-001/pilot-master.json"), "utf8")
);
const published = loadPublishedReport(PROPERTY_ID);

const integrity = {
  adpBaselineId: adpBaseline.baselineId,
  adpBaselineDate: adpBaseline.baselineDate,
  adpImmutable: adpBaseline.immutable === true,
  adpPeriodId: adpBaseline.periodId,
  adpDemandCaptureRate: adpBaseline.demandCaptureRate,
  gdiDay1Id: gdiDay1.snapshotId || gdiDay1.id || "BETHESDA_GDI_DAY1_SNAPSHOT_2026_10_01",
  gdiDay1Ready: gdiDay1.counts?.strictReady ?? gdiDay1.counts?.visible,
  shareTokenId: pilot.gdi?.share_token_id,
  shareTokenMustNotChange: pilot.gdi?.share_token_must_not_change === true,
};

const contentQa = await getGdiPdfReportData(HOTEL_ID, { nowDate: "2026-10-02" });
const watchTitles = (contentQa.futureWatch || []).map((c) => c.opportunity);
const acts = [...(contentQa.immediatePursuits || []), ...(contentQa.topOpportunities || [])].find((c) =>
  /ACTS Translational/i.test(c.opportunity || "")
);
const acc = [...(contentQa.topOpportunities || [])].find((c) => /ACC Legislative/i.test(c.opportunity || ""));

const contentQaReport = {
  marketLabel: contentQa.hotel?.market,
  watchTitles,
  rnaDuplicateCollapsed: watchTitles.filter((t) => /NCI RNA Biology/i.test(t)).length <= 1,
  malformedNavTitlePresent: watchTitles.some((t) => /Skip to main content/i.test(t)),
  actsWho: acts?.who || null,
  actsContact: acts?.contactPath || null,
  actsProvenanceClear: Boolean(acts?.contactPath?.provenanceNote),
  accSegment: acc?.segment || null,
  accSegmentOk: acc?.segment === "Association" || acc?.segment === "Medical / Scientific",
};

const result = {
  mode: APPLY ? "APPLY" : "DRY_RUN",
  integrity,
  contentQaReport,
  historicalPdfProbe: {
    oct1AdpPdfExisted: false,
    oct1GdiPdfExisted: false,
    note: "No client-visible Oct 1 PDF binary was present in the production store (Day-1 lean deploy recorded bethesdaReportPdfBinary:false). Structured snapshots archived instead; corrected GDI PDF published as a new version.",
  },
  archives: [],
  gdiGenerate: null,
};

if (APPLY) {
  function sealIfMissing(archiveId, builder) {
    const existing =
      listReportArchiveEntries({}).find((e) => e.archiveId === archiveId) || null;
    if (existing) {
      result.archives.push({ ...existing, skipped: true, reason: "already_immutable" });
      return existing;
    }
    const published = builder();
    result.archives.push(published);
    return published;
  }

  // ADP Oct 1 baseline — structured snapshot only (no fabricated PDF)
  sealIfMissing("adp_baseline_2026-10-01_v1", () =>
    publishReportArchiveEntry({
      hotelId: HOTEL_ID,
      propertyId: PROPERTY_ID,
      hotelName: "Bethesda Marriott",
      reportType: "ADP",
      reportClass: "BASELINE",
      reportDate: "2026-10-01",
      snapshotDate: "2026-10-01",
      versionLabel: "v1",
      archiveId: "adp_baseline_2026-10-01_v1",
      baselineId: adpBaseline.baselineId,
      methodologyVersion: adpBaseline.methodologyVersion,
      querySetId: "BETHESDA_ADP_OCTOBER_CONTROL_QUERY_SET_V1",
      externalShareTokenId: null,
      pdfMissingReason: "NO_HISTORICAL_CLIENT_VISIBLE_PDF_BINARY_ON_2026-10-01",
      snapshot: {
        baseline: adpBaseline,
        publishedPayload: {
          period: published?.period || null,
          executiveMetrics: published?.executiveMetrics || null,
          demandCapture: published?.demandCapture || null,
          trends: published?.trends || null,
          baselineMonitoring: published?.baselineMonitoring || null,
        },
      },
      notes: "Official immutable October 1 ADP baseline (BETHESDA_ADP_BASELINE_V1).",
    })
  );

  // GDI Day-1 structured snapshot — immutable historical counts (37)
  sealIfMissing("gdi_day1_2026-10-01_v1", () =>
    publishReportArchiveEntry({
      hotelId: HOTEL_ID,
      propertyId: PROPERTY_ID,
      hotelName: "Bethesda Marriott",
      reportType: "GDI",
      reportClass: "DAY1",
      reportDate: "2026-10-01",
      snapshotDate: "2026-10-01",
      versionLabel: "v1",
      archiveId: "gdi_day1_2026-10-01_v1",
      externalShareTokenId: pilot.gdi?.share_token_id || null,
      pdfMissingReason: "NO_HISTORICAL_CLIENT_VISIBLE_PDF_BINARY_ON_2026-10-01",
      snapshot: gdiDay1,
      notes: "Frozen Day-1 GDI snapshot (ready=37). Live counts may differ; do not overwrite.",
    })
  );

  // Generate corrected current GDI PDF (v2 family for client delivery)
  const generated = await generateGdiReportPdfV1({
    hotelId: HOTEL_ID,
    persist: true,
    nowDate: "2026-10-02",
    reportClass: "CURRENT",
  });
  result.gdiGenerate = {
    ok: generated.ok,
    filename: generated.filename,
    byteLength: generated.byteLength,
    pageCountHint: generated.pageCountHint,
    version: generated.persisted?.meta?.version || null,
    checksum: generated.persisted?.meta?.checksumSha256 || null,
    market: generated.data?.hotel?.market,
    ready: generated.data?.executiveSummary?.customerReadyCount,
  };

  if (generated.ok && generated.buffer) {
    sealIfMissing("gdi_current_2026-10-02_v2", () =>
      publishReportArchiveEntry({
        hotelId: HOTEL_ID,
        propertyId: PROPERTY_ID,
        hotelName: "Bethesda Marriott",
        reportType: "GDI",
        reportClass: "CURRENT",
        reportDate: "2026-10-02",
        snapshotDate: generated.data?.reportMetadata?.dataSnapshotAt?.slice?.(0, 10) || "2026-10-02",
        versionLabel: "v2",
        archiveId: "gdi_current_2026-10-02_v2",
        supersedes: "gdi_day1_2026-10-01_v1",
        externalShareTokenId: pilot.gdi?.share_token_id || null,
        pdfBuffer: generated.buffer,
        snapshot: generated.data,
        notes: "Client-delivery corrected GDI PDF (canonical cover + content QA). Does not replace Day-1 snapshot.",
      })
    );
  }

  // Current ADP PDF (client delivery) — new version; Oct 1 baseline structured entry stays PDF-less
  const adpPdfPath = join(
    ROOT,
    "reports/ai-demand-positioning/current-report-pdf/adp_bethesda_marriott/report.pdf"
  );
  const adpPdfMetaPath = join(
    ROOT,
    "reports/ai-demand-positioning/current-report-pdf/adp_bethesda_marriott/meta.json"
  );
  if (existsSync(adpPdfPath)) {
    const adpMeta = existsSync(adpPdfMetaPath)
      ? JSON.parse(readFileSync(adpPdfMetaPath, "utf8"))
      : {};
    sealIfMissing("adp_current_2026-10-02_v1", () =>
      publishReportArchiveEntry({
        hotelId: HOTEL_ID,
        propertyId: PROPERTY_ID,
        hotelName: "Bethesda Marriott",
        reportType: "ADP",
        reportClass: "CURRENT",
        reportDate: "2026-10-02",
        snapshotDate: "2026-10-01",
        versionLabel: "v1",
        archiveId: "adp_current_2026-10-02_v1",
        supersedes: null,
        baselineId: adpBaseline.baselineId,
        methodologyVersion: adpBaseline.methodologyVersion,
        querySetId: "BETHESDA_ADP_OCTOBER_CONTROL_QUERY_SET_V1",
        externalShareTokenId: null,
        pdfPath: adpPdfPath,
        snapshot: {
          currentPdfMeta: adpMeta,
          baselineRef: "adp_baseline_2026-10-01_v1",
          periodId: adpBaseline.periodId,
          note: "Current client ADP PDF rendered from immutable Oct 1 published baseline payload.",
        },
        notes: "Client-delivery ADP PDF (Oct 1 baseline numbers). Not a fabricated historical Oct 1 binary.",
      })
    );
    result.adpPdf = {
      path: adpPdfPath,
      bytes: readFileSync(adpPdfPath).length,
      fingerprint: adpMeta.pdfFingerprint || sha256File(adpPdfPath),
      periodId: adpMeta.periodId || adpBaseline.periodId,
    };
  } else {
    result.adpPdf = { missing: true, path: adpPdfPath };
  }
}

const outPath = join(OUT, "SEAL_ARCHIVES_RESULT.json");
writeFileSync(outPath, JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify(result, null, 2));
console.log(APPLY ? `\nAPPLIED → ${outPath}` : "\nDRY_RUN only — re-run with --apply");
