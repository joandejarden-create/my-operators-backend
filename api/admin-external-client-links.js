import { timingSafeEqual } from "node:crypto";
/**
 * Admin API — external client-facing ADP + GDI share links (read/reconstruct).
 */

import {
  resolveExternalClientLinks,
  fingerprintBethesdaGdiContractToken,
  BETHESDA_GDI_CONTRACT_TOKEN_ID,
} from "../lib/admin/report-external-client-links-v1.js";

import { isProductionLikeRuntime } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

const PRODUCTION_ADMIN_BASE =
  "https://my-operators-backend-production.up.railway.app";

function secureSecretMatch(provided, expected) {
  const a = Buffer.from(String(provided || ""), "utf8");
  const b = Buffer.from(String(expected || ""), "utf8");
  if (!a.length || !b.length || a.length !== b.length) return false;
  return timingSafeEqual(a, b);
}

function readInternalResolverSecret(req) {
  return String(req.headers["x-dealality-internal-resolver"] || "").trim();
}

export async function getAdminExternalClientLinks(req, res) {
  try {
    const hotelId = String(req.params.hotelId || req.query.hotelId || "").trim();
    if (!hotelId) {
      return res.status(400).json({ ok: false, error: "hotelId_required" });
    }
    const createIfMissing = String(req.query.createIfMissing || "") === "1";


    // Local Dealality demo remains an admin only on localhost/dev.
    // Production client links are resolved server-to-server so the demo
    // identity is never elevated to production admin.
    if (!isProductionLikeRuntime()) {
      const resolverSecret = String(
        process.env.EXTERNAL_CLIENT_LINKS_RESOLVER_SECRET || ""
      ).trim();

      if (!resolverSecret) {
        return res.status(503).json({
          ok: false,
          error: "internal_resolver_not_configured",
          message:
            "Production external-link resolver is not configured for this local environment.",
        });
      }

      const productionUrl =
        `${PRODUCTION_ADMIN_BASE}/api/internal/external-client-links/${encodeURIComponent(hotelId)}`;

      try {
        const upstream = await fetch(productionUrl, {
          method: "GET",
          headers: {
            "X-Dealality-Internal-Resolver": resolverSecret,
            Accept: "application/json",
          },
          signal: AbortSignal.timeout(15000),
        });

        const body = await upstream.json().catch(() => null);

        if (!upstream.ok || !body) {
          return res.status(upstream.status || 502).json({
            ok: false,
            error: body?.error || "production_external_links_unavailable",
            message:
              body?.message ||
              `Production external-link resolver returned HTTP ${upstream.status}.`,
          });
        }

        return res.status(upstream.status).json({
          ...body,
          resolvedViaProduction: true,
        });
      } catch (err) {
        console.warn(
          "[admin-external-client-links] internal production resolver failed:",
          String(err?.message || err).slice(0, 160)
        );

        return res.status(502).json({
          ok: false,
          error: "production_external_links_unavailable",
          message:
            "Could not reach the production external-link resolver. No local client link was generated.",
        });
      }
    }

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
        servedFromProductionHost: links.gdi.servedFromProductionHost || false,
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


export async function getInternalExternalClientLinks(req, res) {
  try {
    const expected = String(
      process.env.EXTERNAL_CLIENT_LINKS_RESOLVER_SECRET || ""
    ).trim();
    const provided = readInternalResolverSecret(req);

    if (!expected) {
      return res.status(503).json({
        ok: false,
        error: "internal_resolver_not_configured",
      });
    }

    if (!secureSecretMatch(provided, expected)) {
      return res.status(401).json({
        ok: false,
        error: "unauthorized_internal_resolver",
      });
    }

    if (!isProductionLikeRuntime()) {
      return res.status(403).json({
        ok: false,
        error: "internal_resolver_production_only",
      });
    }

    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) {
      return res.status(400).json({
        ok: false,
        error: "hotelId_required",
      });
    }

    // Existing shares only. Never mint/create from this endpoint.
    const links = resolveExternalClientLinks(hotelId, {
      req,
      createIfMissing: false,
    });

    return res.json({
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
        created: false,
      },
      gdi: {
        available: links.gdi.available,
        reportType: "GDI",
        label: "Group & Demand Intelligence",
        reason: links.gdi.reason,
        tokenId: links.gdi.tokenId,
        url: links.gdi.url,
        path: links.gdi.path,
        created: false,
        bethesdaContractPreserved: links.gdi.bethesdaContractPreserved,
        servedFromProductionHost:
          links.gdi.servedFromProductionHost || false,
      },
    });
  } catch (err) {
    console.error(
      "[internal-external-client-links]",
      err?.message || err
    );
    return res.status(500).json({
      ok: false,
      error: "external_links_failed",
    });
  }
}
