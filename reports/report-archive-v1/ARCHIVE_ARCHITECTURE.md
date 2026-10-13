# Dealality Report Archive V1 — Architecture

## CURRENT_REPORT_STORAGE (Phase 0 audit)

| Artifact | Where stored | Persistent after deploy? | Historical versioning? | Immutable? | Admin retrieval? |
|----------|--------------|--------------------------|------------------------|------------|------------------|
| **ADP PDF (current)** | `reports/ai-demand-positioning/current-report-pdf/{propertyId}/report.pdf` | **NO** on ephemeral Railway FS unless volume/git; overwritten on regenerate | Weak (current pointer only) | **NO** (current pointer) | Via ADP Action Plan / PDF routes |
| **ADP published snapshot** | `data/ai-demand-positioning/published/{propertyId}/` | **YES** if shipped in deploy / volume | Latest + period files; not a full immutable ledger | Soft (can republish) | ADP Admin / customer APIs |
| **GDI PDF (current)** | `reports/group-demand-intelligence/pdf/{hotelId}/report.pdf` (+ `archive/vN/`) | **NO** on ephemeral FS unless `DEALALITY_REPORT_PDF_ROOT` volume | **YES** under `archive/vN/` | Version dirs yes; current pointer no | GDI Reports Admin |
| **GDI structured snapshot** | Beside GDI PDF version dir `snapshot.json` | Same as GDI PDF root | Yes per version | Yes per version | GDI Admin / archive |
| **Report Archive ledger** | `data/dealality-report-archive/` (override `DEALALITY_REPORT_ARCHIVE_ROOT`) | **YES** when root is volume or git-tracked golden entries | **YES** (archiveId never overwritten) | **YES** | **YES** — Report Archive tab + `/api/admin/report-archive*` |
| S3 / R2 / Blob | Not used for ADP/GDI PDFs | n/a | n/a | n/a | n/a |
| Airtable attachments | Not the SoT for these PDFs | n/a | n/a | n/a | n/a |
| External share registries | Token → live/current client view | Tokens persist; **payload is latest** | No historical PDF via share URL alone | n/a | Separate from Archive |

**Rule:** Current PDF pointers alone = **NOT ARCHIVED**. Archive ledger is the historical SoT.

## Storage law

1. Prefer `DEALALITY_REPORT_ARCHIVE_ROOT` on a Railway volume in production.
2. If unset, default `data/dealality-report-archive/` (repo-relative). Golden Bethesda Oct 1 entries are git-tracked.
3. `DEALALITY_REPORT_PDF_ROOT` also relocates archive to `{pdfRoot}/report-archive` when archive root unset.
4. No new object-storage vendor introduced.

## Data model

```
ReportArchiveRecord (meta.json)
  archiveId, hotelId, propertyId, hotelName
  reportType: ADP | GDI
  reportClass: BASELINE | OFFICIAL_BASELINE | DAY1 | WEEKLY | MONTHLY |
               REMEASUREMENT | CURRENT | MANUAL | …
  reportDate, snapshotDate, version, generatedAt, archivedAt
  immutable: true
  supersedes
  pdfChecksumSha256 | pdfMissingReason
  snapshotChecksumSha256
  methodologyVersion, querySetId, baselineId
  externalShareTokenId, codeSha, notes
```

Layout:

```
{root}/index.json
{root}/{hotelKey}/{ADP|GDI}/{archiveId}/
  meta.json
  snapshot.json + snapshot.sha256
  report.pdf + report.pdf.sha256   (optional)
```

## Immutability

`publishReportArchiveEntry` refuses existing `archiveId` (`ARCHIVE_IMMUTABLE`). Regeneration creates a new version / archiveId. Supersession via `supersedes` field when useful.

## Auto-archive triggers

| Trigger | Module | What is archived |
|---------|--------|------------------|
| ADP current PDF write | `writeCurrentReportPdf` → `autoArchiveAdpCurrentPdf` | PDF + published report snapshot |
| ADP certified publish | `publishExistingHotelAdpSnapshot` → `autoArchiveAdpPublishedSnapshot` | Structured snapshot (PDF may arrive later) |
| GDI client PDF write | `writeGdiReportPdf` → `autoArchiveGdiReportPdf` | PDF + reportSnapshot; skips PREVIEW |

Failures are non-fatal to publish/PDF write (logged + `reportArchive` on meta).

## Admin API

- `GET /api/admin/report-archive` — catalog (paths stripped)
- `GET /api/admin/report-archive/:archiveId` — meta + snapshot + checksum flags
- `GET /api/admin/report-archive/:archiveId/pdf` — inline/download PDF

Admin auth required (same AI Demand Admin gate). Raw snapshots Admin-only.

## Client share relationship

External share URLs may continue to show **latest**. Archive rows retrieve **frozen** PDF/snapshot for that archiveId. Do not treat live share URLs as historical truth.
