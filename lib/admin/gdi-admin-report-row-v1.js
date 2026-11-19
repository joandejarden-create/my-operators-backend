/**
 * GDI Admin Reports — normalized row contract + summary reconciliation.
 * Cards and table MUST derive from the same row model.
 */

import {
  probeAdpClientShareAvailable,
  probeGdiClientShareState,
  resolveExternalClientLinks,
} from "./report-external-client-links-v1.js";
import { isProductionLikeRuntime } from "../group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

export const GDI_ADMIN_REPORT_ROW_SCHEMA = "GDI_ADMIN_REPORT_ROW_V1";

/** Disabled Open/Copy copy — matches ADP founder-approval gate wording. */
export const GDI_NO_AUTHORIZED_SHARE_REASON =
  "No authorized GDI share link — customer distribution requires founder approval.";

const PRODUCTION_ADMIN_BASE =
  "https://my-operators-backend-production.up.railway.app";

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

/**
 * Cheap GDI share availability (no mint, no URL in response).
 * @param {string} hotelId â€” HPC recâ€¦
 */
export function probeGdiClientShareAvailable(hotelId) {
  return probeGdiClientShareState(hotelId);
}

/**
 * Resolve share availability for catalog rows.
 * Prefer cheap probes; optionally confirm with full resolve (usable URL check).
 * @param {string} hotelId
 * @param {{ req?: object, confirmUrls?: boolean }} [opts]
 */
function normalizeUnauthorizedGdiReason(reason) {
  const text = String(reason || "").trim();
  if (!text) return GDI_NO_AUTHORIZED_SHARE_REASON;
  if (/founder approval/i.test(text)) return text;
  if (/no active gdi client share/i.test(text)) {
    return GDI_NO_AUTHORIZED_SHARE_REASON;
  }
  if (/not an external production url/i.test(text)) {
    // Local DEV reconstruction is not proof of absence — Open/Copy uses production.
    return GDI_NO_AUTHORIZED_SHARE_REASON;
  }
  return text;
}

export function resolveGdiAdminShareFlags(hotelId, opts = {}) {
  const gdiProbe = probeGdiClientShareAvailable(hotelId);
  const adpProbe = probeAdpClientShareAvailable(hotelId);

  let gdiAvailable = Boolean(gdiProbe.available);
  let adpAvailable = Boolean(adpProbe.available);
  let gdiShareReason = gdiProbe.reason || null;
  let gdiShareVerified = gdiProbe.signatureVerified === true;
  let needsProductionConfirm = false;

  // Confirm usable external URL when probes say available (esp. ADP localhost trap).
  if (opts.confirmUrls !== false && (gdiAvailable || adpAvailable)) {
    try {
      const links = resolveExternalClientLinks(hotelId, {
        req: opts.req || null,
        createIfMissing: false,
      });
      const gdiUrlOk = isUsableExternalClientUrl(links?.gdi?.url);
      const localGdiAvailable = Boolean(links?.gdi?.available) && gdiUrlOk;
      adpAvailable =
        Boolean(links?.adp?.available) &&
        isUsableExternalClientUrl(links?.adp?.url);

      if (localGdiAvailable) {
        gdiAvailable = true;
        gdiShareVerified = links?.gdi?.signatureVerified === true;
        gdiShareReason = null;
      } else if (Boolean(gdiProbe.available) && !isProductionLikeRuntime()) {
        // Local confirm often yields localhost DEV-signed URLs for registry shares
        // that are authorized in production. Defer to production enrich (catalog).
        gdiAvailable = false;
        needsProductionConfirm = true;
        gdiShareReason = normalizeUnauthorizedGdiReason(
          links?.gdi?.reason || gdiShareReason
        );
      } else {
        gdiAvailable = false;
        gdiShareVerified = false;
        gdiShareReason = normalizeUnauthorizedGdiReason(
          links?.gdi?.reason ||
            (links?.gdi?.available && !gdiUrlOk
              ? "GDI client link is not an external production URL."
              : gdiShareReason)
        );
      }
    } catch {
      // Keep probe results if confirm throws — sealed probe remains true.
      if (!gdiAvailable) gdiAvailable = Boolean(gdiProbe.available);
      if (!adpAvailable) adpAvailable = false;
      if (gdiAvailable && !isProductionLikeRuntime()) {
        needsProductionConfirm = true;
        gdiAvailable = false;
      }
    }
  }

  if (!gdiAvailable && !gdiShareReason) {
    gdiShareReason = GDI_NO_AUTHORIZED_SHARE_REASON;
  }

  return {
    gdiClient: {
      available: gdiAvailable,
      reason: gdiAvailable ? null : normalizeUnauthorizedGdiReason(gdiShareReason),
      signatureVerified: gdiShareVerified,
    },
    adpClient: { available: adpAvailable },
    // Flat aliases for existing Admin UI. Availability is not a ready-count gate.
    gdiShareAvailable: gdiAvailable,
    adpShareAvailable: adpAvailable,
    gdiShareVerified,
    gdiShareReason: gdiAvailable
      ? null
      : normalizeUnauthorizedGdiReason(gdiShareReason),
    needsProductionConfirm,
  };
}

/**
 * Local Admin catalog: confirm GDI share via production internal resolver
 * (same path as Open/Copy). Never mints shares.
 * @param {string} hotelId
 * @returns {Promise<{ available: boolean, reason: string|null, signatureVerified: boolean, resolvedViaProduction: boolean }>}
 */
export async function confirmGdiShareViaProductionResolver(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id || isProductionLikeRuntime()) {
    return {
      available: false,
      reason: GDI_NO_AUTHORIZED_SHARE_REASON,
      signatureVerified: false,
      resolvedViaProduction: false,
    };
  }
  const resolverSecret = String(
    process.env.EXTERNAL_CLIENT_LINKS_RESOLVER_SECRET || ""
  ).trim();
  if (!resolverSecret) {
    return {
      available: false,
      reason: GDI_NO_AUTHORIZED_SHARE_REASON,
      signatureVerified: false,
      resolvedViaProduction: false,
    };
  }
  try {
    const upstream = await fetch(
      `${PRODUCTION_ADMIN_BASE}/api/internal/external-client-links/${encodeURIComponent(id)}`,
      {
        method: "GET",
        headers: {
          "X-Dealality-Internal-Resolver": resolverSecret,
          Accept: "application/json",
        },
        signal: AbortSignal.timeout(15000),
      }
    );
    const body = await upstream.json().catch(() => null);
    const gdi = body?.gdi || null;
    const urlOk = isUsableExternalClientUrl(gdi?.url);
    const available = Boolean(upstream.ok && gdi?.available && urlOk);
    return {
      available,
      reason: available
        ? null
        : normalizeUnauthorizedGdiReason(gdi?.reason || body?.message),
      signatureVerified: available && gdi?.signatureVerified === true,
      resolvedViaProduction: true,
      tokenId: gdi?.tokenId || null,
    };
  } catch {
    return {
      available: false,
      reason: GDI_NO_AUTHORIZED_SHARE_REASON,
      signatureVerified: false,
      resolvedViaProduction: false,
    };
  }
}

/**
 * Apply production confirmation onto share flags when local confirm is inconclusive.
 * @param {ReturnType<typeof resolveGdiAdminShareFlags>} flags
 * @param {string} hotelId
 */
export async function enrichGdiAdminShareFlagsWithProduction(flags, hotelId) {
  const base = flags || resolveGdiAdminShareFlags(hotelId, { confirmUrls: true });
  if (base.gdiShareAvailable || !base.needsProductionConfirm) {
    return { ...base, needsProductionConfirm: false };
  }
  const prod = await confirmGdiShareViaProductionResolver(hotelId);
  if (prod.available) {
    return {
      ...base,
      gdiClient: {
        available: true,
        reason: null,
        signatureVerified: prod.signatureVerified === true,
      },
      gdiShareAvailable: true,
      gdiShareVerified: prod.signatureVerified === true,
      gdiShareReason: null,
      needsProductionConfirm: false,
      resolvedViaProduction: true,
    };
  }
  return {
    ...base,
    gdiClient: {
      available: false,
      reason: prod.reason || GDI_NO_AUTHORIZED_SHARE_REASON,
      signatureVerified: false,
    },
    gdiShareAvailable: false,
    gdiShareVerified: false,
    gdiShareReason: prod.reason || GDI_NO_AUTHORIZED_SHARE_REASON,
    needsProductionConfirm: false,
    resolvedViaProduction: prod.resolvedViaProduction === true,
  };
}

/**
 * Normalize one GDI Admin operating row.
 * @param {object} input
 */
export function normalizeGdiAdminReportRow(input = {}) {
  const hotelId = String(input.hotelId || "").trim();
  const available = Boolean(input.available);
  const readFailed = Boolean(input.readFailed);
  const metric = (value) => {
    if (readFailed) return null;
    if (value == null || value === "") return 0;
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  };
  const readyCount = metric(input.readyCount ?? input.ready);
  const actionSetCount = metric(input.actionSetCount ?? input.actionSet);
  const watchCount = metric(input.watchCount ?? input.watch);
  const watchTotal = readFailed
    ? null
    : input.watchTotal == null
      ? watchCount
      : metric(input.watchTotal);
  const watchCapped =
    !readFailed &&
    watchTotal != null &&
    watchCount != null &&
    watchTotal > watchCount;
  const clientRaw = input.clientOpportunityCount;
  const clientOpportunityCount = readFailed
    ? null
    : clientRaw == null || clientRaw === ""
      ? null
      : Number(clientRaw) || 0;
  const facingRaw = input.customerFacingCount ?? input.customerFacingProspectCount;
  const customerFacingCount =
    facingRaw == null || facingRaw === "" ? null : Number(facingRaw) || 0;
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
  const gdiShareReason = gdiAvailable
    ? null
    : normalizeUnauthorizedGdiReason(
        input.gdiShareReason || input.gdiClient?.reason || null
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
    watchTotal,
    watchCapped,
    clientOpportunityCount,
    customerFacingCount,
    readFailed,
    gdiShareReason,
    gdiShareVerified: input.gdiShareVerified === true || input.gdiClient?.signatureVerified === true,
    // Flat aliases (existing Admin table). Facing is not Ready.
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
    gdiClient: {
      available: gdiAvailable,
      reason: gdiShareReason,
      signatureVerified:
        input.gdiShareVerified === true ||
        input.gdiClient?.signatureVerified === true,
    },
    adpClient: { available: adpAvailable },
    gdiShareAvailable: gdiAvailable,
    adpShareAvailable: adpAvailable,
    lastGeneratedAt,
    archiveCount: Number(input.archiveCount || 0) || 0,
    // Full-content parity (universal publication gate visibility).
    fullContentParityStatus: input.fullContentParityStatus || null,
    publishedOpportunityCount:
      input.publishedOpportunityCount == null
        ? null
        : Number(input.publishedOpportunityCount) || 0,
    publicationVersion: input.publicationVersion || null,
    lastParityVerifiedAt: input.lastParityVerifiedAt || null,
    parityCriticalDifferenceCount:
      Number(input.parityCriticalDifferenceCount || 0) || 0,
    parityDifferences: Array.isArray(input.parityDifferences)
      ? input.parityDifferences
      : [],
    parityMigrationCandidate: input.parityMigrationCandidate || null,
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
