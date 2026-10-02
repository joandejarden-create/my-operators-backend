/**
 * GDI Admin Reports — normalized row contract + summary reconciliation.
 * Cards and table MUST derive from the same row model.
 */

import {
  probeAdpClientShareAvailable,
  resolveExternalClientLinks,
  BETHESDA_GDI_CONTRACT_TOKEN_ID,
  BETHESDA_HPC,
} from "./report-external-client-links-v1.js";
import {
  readGdiShareRegistry,
  getGdiShareCapabilitySecret,
} from "../group-demand-intelligence/share/gdi-signed-share-capability-v1.js";
import { existsSync, readFileSync } from "fs";
import { join } from "path";

export const GDI_ADMIN_REPORT_ROW_SCHEMA = "GDI_ADMIN_REPORT_ROW_V1";

const ROOT = process.cwd();
const CONTRACT_PATH = join(
  ROOT,
  "config/client-share/production-share-contract-tokens.json"
);

/**
 * Report status chips for Admin GDI Reports.
 * @param {{ available?: boolean, reason?: string|null, ready?: number, watch?: number }} summary
 */
export function deriveGdiReportStatus(summary) {
  if (!summary?.available) {
    const reason = String(summary?.reason || "").toLowerCase();
    if (reason.includes("not configured") || reason.includes("needs build")) {
      return "NEEDS_BUILD";
    }
    return "BLOCKED";
  }
  const ready = Number(summary.ready || 0);
  const watch = Number(summary.watch || 0);
  if (ready > 0) return "READY";
  if (watch > 0) return "WATCH_ONLY";
  return "NO_READY_OPPORTUNITIES";
}

export function derivePdfStatus(pdfReady) {
  return pdfReady ? "READY" : "MISSING";
}

/** External client URL usable for Open/Copy (never localhost / unsigned). */
export function isUsableExternalClientUrl(url) {
  if (!url || typeof url !== "string") return false;
  try {
    const u = new URL(url);
    if (/localhost|127\.0\.0\.1/i.test(u.hostname)) return false;
    if (!/^https?:$/i.test(u.protocol)) return false;
    if (!/share=/i.test(u.search || "")) return false;
    return true;
  } catch {
    return false;
  }
}

function loadBethesdaGdiContractPresent() {
  if (!existsSync(CONTRACT_PATH)) return false;
  try {
    const doc = JSON.parse(readFileSync(CONTRACT_PATH, "utf8"));
    return (doc.tokens || []).some(
      (t) =>
        t.product === "GDI" &&
        t.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID &&
        t.hotelId === BETHESDA_HPC &&
        typeof t.token === "string" &&
        t.token.length > 20
    );
  } catch {
    return false;
  }
}

/**
 * Cheap GDI share availability (no mint, no URL in response).
 * @param {string} hotelId — HPC rec…
 */
export function probeGdiClientShareAvailable(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) {
    return { available: false, tokenId: null, reason: "hotelId_required" };
  }
  try {
    if (id === BETHESDA_HPC && loadBethesdaGdiContractPresent()) {
      return {
        available: true,
        tokenId: BETHESDA_GDI_CONTRACT_TOKEN_ID,
        reason: null,
      };
    }
    const secret = getGdiShareCapabilitySecret();
    const reg = readGdiShareRegistry();
    const active = Object.values(reg.tokens || {}).filter(
      (t) =>
        t.hotelId === id &&
        t.status === "ACTIVE" &&
        !t.revokedAt
    );
    if (active.length && secret) {
      const preferred =
        active.find((t) => /pilot|client|production|rad/i.test(String(t.label || ""))) ||
        active.sort((a, b) =>
          String(b.issuedAt || "").localeCompare(String(a.issuedAt || ""))
        )[0];
      return {
        available: true,
        tokenId: preferred?.tokenId || null,
        reason: null,
      };
    }
    if (!secret) {
      return {
        available: false,
        tokenId: active[0]?.tokenId || null,
        reason: "GDI share secret not configured in this environment.",
      };
    }
    return {
      available: false,
      tokenId: null,
      reason: "No active GDI client link is available for this hotel.",
    };
  } catch (err) {
    return {
      available: false,
      tokenId: null,
      reason: String(err?.message || err).slice(0, 160),
    };
  }
}

/**
 * Resolve share availability for catalog rows.
 * Prefer cheap probes; optionally confirm with full resolve (usable URL check).
 * @param {string} hotelId
 * @param {{ req?: object, confirmUrls?: boolean }} [opts]
 */
export function resolveGdiAdminShareFlags(hotelId, opts = {}) {
  const gdiProbe = probeGdiClientShareAvailable(hotelId);
  const adpProbe = probeAdpClientShareAvailable(hotelId);

  let gdiAvailable = Boolean(gdiProbe.available);
  let adpAvailable = Boolean(adpProbe.available);

  // Confirm usable external URL when probes say available (esp. ADP localhost trap).
  if (opts.confirmUrls !== false && (gdiAvailable || adpAvailable)) {
    try {
      const links = resolveExternalClientLinks(hotelId, {
        req: opts.req || null,
        createIfMissing: false,
      });
      gdiAvailable =
        Boolean(links?.gdi?.available) &&
        isUsableExternalClientUrl(links?.gdi?.url);
      adpAvailable =
        Boolean(links?.adp?.available) &&
        isUsableExternalClientUrl(links?.adp?.url);
    } catch {
      // Keep probe results if confirm throws — Bethesda sealed probe remains true.
      if (!gdiAvailable) gdiAvailable = Boolean(gdiProbe.available);
      if (!adpAvailable) adpAvailable = false;
    }
  }

  return {
    gdiClient: { available: gdiAvailable },
    adpClient: { available: adpAvailable },
    // Flat aliases for existing Admin UI
    gdiShareAvailable: gdiAvailable,
    adpShareAvailable: adpAvailable,
  };
}

/**
 * Normalize one GDI Admin operating row.
 * @param {object} input
 */
export function normalizeGdiAdminReportRow(input = {}) {
  const hotelId = String(input.hotelId || "").trim();
  const available = Boolean(input.available);
  const readyCount = Number(input.readyCount ?? input.ready ?? 0) || 0;
  const actionSetCount = Number(input.actionSetCount ?? input.actionSet ?? 0) || 0;
  const watchCount = Number(input.watchCount ?? input.watch ?? 0) || 0;
  const pdfReady = Boolean(
    input.pdf?.available ?? input.pdfReady ?? input.pdfStatus === "READY"
  );
  const pdfStatus =
    input.pdf?.status ||
    input.pdfStatus ||
    derivePdfStatus(pdfReady);
  const reportStatus =
    input.reportStatus ||
    deriveGdiReportStatus({
      available,
      reason: input.reason,
      ready: readyCount,
      watch: watchCount,
    });
  const lastGeneratedAt =
    input.lastGeneratedAt ||
    input.pdf?.generatedAt ||
    null;
  const gdiAvailable = Boolean(
    input.gdiClient?.available ?? input.gdiShareAvailable
  );
  const adpAvailable = Boolean(
    input.adpClient?.available ?? input.adpShareAvailable
  );

  return {
    schema: GDI_ADMIN_REPORT_ROW_SCHEMA,
    hotelId,
    hotelName: input.hotelName || input.displayName || hotelId,
    displayName: input.displayName || input.hotelName || hotelId,
    market: input.market || null,
    available,
    reason: input.reason || null,
    readyCount,
    actionSetCount,
    watchCount,
    // Flat aliases (existing Admin table)
    ready: readyCount,
    actionSet: actionSetCount,
    watch: watchCount,
    reportStatus,
    pdf: {
      status: pdfStatus,
      available: pdfReady,
      generatedAt: lastGeneratedAt,
    },
    pdfReady,
    pdfStatus,
    gdiClient: { available: gdiAvailable },
    adpClient: { available: adpAvailable },
    gdiShareAvailable: gdiAvailable,
    adpShareAvailable: adpAvailable,
    lastGeneratedAt,
    archiveCount: Number(input.archiveCount || 0) || 0,
  };
}

/**
 * Summary cards from the SAME normalized rows as the table.
 * @param {object[]} rows
 */
export function reconcileGdiAdminReportCounts(rows = []) {
  const list = Array.isArray(rows) ? rows : [];
  return {
    hotels: list.length,
    reportReady: list.filter((r) => r.reportStatus === "READY").length,
    pdfReady: list.filter(
      (r) => r.pdfReady === true || r.pdfStatus === "READY" || r.pdf?.available
    ).length,
    needsPdf: list.filter(
      (r) =>
        r.available &&
        !(r.pdfReady === true || r.pdfStatus === "READY" || r.pdf?.available)
    ).length,
    blocked: list.filter(
      (r) => r.reportStatus === "BLOCKED" || r.reportStatus === "NEEDS_BUILD"
    ).length,
    watchOnly: list.filter((r) => r.reportStatus === "WATCH_ONLY").length,
  };
}
