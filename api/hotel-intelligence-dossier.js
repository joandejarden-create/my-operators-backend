/**
 * Packet 2.6B — Hotel Intelligence Dossier API (read-only fixtures).
 * No Webhound runs. No auto-promotion to canonical truth.
 */

import {
  listDossiersForHotel,
  getDossierById,
  getDossierForHotelRecord,
} from "../lib/hotel-intelligence/dossier/index.js";
import { generateCanonicalDossierPdf } from "../lib/hotel-intelligence/dossier/generate-canonical-pdf.mjs";
import {
  buildReportDownloadFilename,
  contentDispositionAttachment,
} from "../lib/hotel-intelligence/dossier/report-filename.js";
import { enrichReportForPublishing } from "../lib/hotel-intelligence/dossier/report-hotel-identity.js";
import { getPersistedResearchAddendum } from "../lib/hotel-intelligence/research/addendum-store.js";

/**
 * Share-mode scope: when share=1 or share_hotel/hotelId is present,
 * the dossier must belong to that hotel or the request is denied.
 * @returns {{ denied: boolean, scopedHotelId: string|null }}
 */
export function assertDossierShareScope(dossier, query = {}) {
  const shareQ = String(query.share || "").toLowerCase();
  const shareFlag = shareQ === "1" || shareQ === "true";
  const scopedHotelId = String(
    query.share_hotel || query.shareHotel || query.hotelId || query.hotel_id || ""
  ).trim();
  if (!shareFlag && !scopedHotelId) {
    return { denied: false, scopedHotelId: null };
  }
  if (!scopedHotelId) {
    return { denied: true, scopedHotelId: null };
  }
  const candidates = [
    String(dossier?.hotel_airtable_record_id || "").trim(),
    String(dossier?.hotel_id || "").trim(),
  ].filter(Boolean);
  if (!candidates.includes(scopedHotelId)) {
    return { denied: true, scopedHotelId };
  }
  return { denied: false, scopedHotelId };
}

export function listHotelIntelligenceDossiers(req, res) {
  try {
    const hotelId = String(req.query.hotelId || req.query.hotel_id || "").trim();
    const recordId = String(
      req.query.recordId || req.query.airtableRecordId || req.params.recordId || ""
    ).trim();
    if (!hotelId && !recordId) {
      return res.status(400).json({
        ok: false,
        error: "hotelId_or_recordId_required",
        message: "Pass hotelId or recordId (Airtable census record).",
      });
    }
    const dossiers = listDossiersForHotel({
      hotelId: hotelId || undefined,
      airtableRecordId: recordId || undefined,
    });
    return res.json({
      ok: true,
      dossier_type: "FULL_HOTEL_INTELLIGENCE_INVESTIGATION",
      count: dossiers.length,
      dossiers,
    });
  } catch (err) {
    console.error("[hotel-intelligence-dossier] list", err);
    return res.status(500).json({ ok: false, error: "dossier_list_failed" });
  }
}

export function getHotelIntelligenceDossier(req, res) {
  try {
    const dossierId = String(req.params.dossierId || "").trim();
    const dossier = getDossierById(dossierId);
    if (!dossier) {
      return res.status(404).json({ ok: false, error: "dossier_not_found" });
    }
    const scope = assertDossierShareScope(dossier, req.query || {});
    if (scope.denied) {
      return res.status(403).json({ ok: false, error: "share_scope_denied" });
    }
    // Never expose raw provider dumps — normalized dossier only.
    const safe = { ...dossier };
    if (safe.raw_artifact_reference) {
      safe.raw_artifact_reference = {
        kind: safe.raw_artifact_reference.kind,
        path: safe.raw_artifact_reference.path,
        immutable: true,
      };
    }
    return res.json({ ok: true, dossier: safe });
  } catch (err) {
    console.error("[hotel-intelligence-dossier] get", err);
    return res.status(500).json({ ok: false, error: "dossier_get_failed" });
  }
}

/** Convenience: latest dossier for a Hotel Census / golden-demo record id. */
export function getHotelIntelligenceDossierForHotel(req, res) {
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!recordId.startsWith("rec")) {
      return res.status(400).json({ ok: false, error: "invalid_record_id" });
    }
    const list = listDossiersForHotel({ airtableRecordId: recordId });
    const dossier = getDossierForHotelRecord(recordId);
    return res.json({
      ok: true,
      recordId,
      count: list.length,
      dossiers: list,
      dossier: dossier || null,
      empty_state: !dossier
        ? {
            status: "NOT_STARTED",
            title: "Full Hotel Intelligence Investigation",
            description:
              "Conduct a comprehensive investigation of the property's ownership, corporate structure, operator and brand relationships, history, key people, portfolio connections, material changes and development implications. Findings are supported by source-level evidence and unresolved questions are explicitly identified.",
          }
        : null,
    });
  } catch (err) {
    console.error("[hotel-intelligence-dossier] for-hotel", err);
    return res.status(500).json({ ok: false, error: "dossier_for_hotel_failed" });
  }
}

/**
 * Packet 2.6B-R9 — Canonical Download PDF.
 * Same Playwright engine as automated QA. Not window.print().
 * Filename is report-specific (hotel + investigation + date).
 */
export async function downloadHotelIntelligenceDossierPdf(req, res) {
  try {
    const dossierId = String(req.params.dossierId || "").trim();
    const recordId = String(
      req.query.recordId || req.query.airtableRecordId || ""
    ).trim();
    let dossier = null;
    if (dossierId) {
      dossier = getDossierById(dossierId);
    } else if (recordId) {
      dossier = getDossierForHotelRecord(recordId);
    }
    if (!dossier) {
      return res.status(404).json({ ok: false, error: "dossier_not_found" });
    }

    const scope = assertDossierShareScope(dossier, req.query || {});
    if (scope.denied) {
      return res.status(403).json({ ok: false, error: "share_scope_denied" });
    }

    dossier = enrichReportForPublishing(dossier);

    // Lineage: the PDF must be for the same report_id requested (View Report ↔ Download).
    if (dossierId && String(dossier.dossier_id) !== dossierId) {
      return res.status(409).json({
        ok: false,
        error: "download_report_id_mismatch",
        message: "Download PDF must resolve the same report as the archive card.",
      });
    }

    const hotelKey = dossier.hotel_airtable_record_id || dossier.hotel_id || recordId;
    const siblingCards = listDossiersForHotel({
      hotelId: dossier.hotel_id,
      airtableRecordId: dossier.hotel_airtable_record_id || recordId || undefined,
    });
    const siblings = siblingCards
      .map((c) => getDossierById(c.dossier_id) || getPersistedResearchAddendum(c.dossier_id))
      .filter(Boolean)
      .map((d) => enrichReportForPublishing(d));

    const filename = buildReportDownloadFilename(dossier, { siblings });

    const proto = String(req.headers["x-forwarded-proto"] || req.protocol || "http");
    const host = String(req.headers["x-forwarded-host"] || req.get("host") || "127.0.0.1:8080");
    const baseUrl =
      process.env.HID_PDF_BASE_URL ||
      process.env.PUBLIC_BASE_URL ||
      `${proto}://${host}`;

    const result = await generateCanonicalDossierPdf({
      baseUrl,
      dossierId: dossier.dossier_id,
      recordId: dossier.hotel_airtable_record_id || hotelKey || undefined,
      filename,
      dossier,
    });

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", contentDispositionAttachment(result.filename));
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("X-Dealality-Pdf-Engine", "playwright-chromium-canonical-r9");
    res.setHeader("X-Dealality-Report-Id", String(dossier.dossier_id || ""));
    return res.status(200).send(result.buffer);
  } catch (err) {
    console.error("[hotel-intelligence-dossier] pdf", err);
    return res.status(500).json({
      ok: false,
      error: "dossier_pdf_failed",
      message: err && err.message ? String(err.message) : "pdf_failed",
    });
  }
}
