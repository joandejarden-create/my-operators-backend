/**
 * Admin API — external client-facing ADP + GDI share links (read/reconstruct).
 */

import {
  resolveExternalClientLinks,
  fingerprintBethesdaGdiContractToken,
  BETHESDA_GDI_CONTRACT_TOKEN_ID,
} from "../lib/admin/report-external-client-links-v1.js";

export async function getAdminExternalClientLinks(req, res) {
  try {
    const hotelId = String(req.params.hotelId || req.query.hotelId || "").trim();
    if (!hotelId) {
      return res.status(400).json({ ok: false, error: "hotelId_required" });
    }
    const createIfMissing = String(req.query.createIfMissing || "") === "1";
    const beforeFp = fingerprintBethesdaGdiContractToken();
    const links = resolveExternalClientLinks(hotelId, {
      req,
      createIfMissing,
    });
    const afterFp = fingerprintBethesdaGdiContractToken();
    if (
      beforeFp.present &&
      afterFp.present &&
      beforeFp.sha12 !== afterFp.sha12
    ) {
      return res.status(500).json({
        ok: false,
        error: "bethesda_share_token_mutated",
        message: "Refusing to return links — Bethesda GDI share token changed unexpectedly.",
      });
    }
    // Never echo full share token in logs; response includes URL for Admin copy UX only.
    res.json({
      ok: links.ok !== false,
      hotelId,
      hpcId: links.hpcId,
      adpPropertyId: links.adpPropertyId,
      publicBase: links.publicBase,
      adp: {
        available: links.adp.available,
        reportType: "ADP",
        label: "AI Demand Positioning",
        reason: links.adp.reason,
        tokenId: links.adp.tokenId,
        url: links.adp.url,
        path: links.adp.path,
        created: links.adp.created,
      },
      gdi: {
        available: links.gdi.available,
        reportType: "GDI",
        label: "Group & Demand Intelligence",
        reason: links.gdi.reason,
        tokenId: links.gdi.tokenId,
        url: links.gdi.url,
        path: links.gdi.path,
        created: links.gdi.created,
        bethesdaContractPreserved: links.gdi.bethesdaContractPreserved,
        contractTokenId:
          links.hpcId === "recLuxvwwxID7U2B8" ? BETHESDA_GDI_CONTRACT_TOKEN_ID : null,
      },
      bethesdaTokenFingerprint: afterFp,
    });
  } catch (err) {
    console.error("[admin-external-client-links]", err?.message || err);
    res.status(500).json({ ok: false, error: "external_links_failed" });
  }
}
