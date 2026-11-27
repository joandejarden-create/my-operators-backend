/**
 * GDI Executive Summary Phase 1 API — read-only.
 * Auth: FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_PHASE_1_20261127
 *
 * GET /api/group-demand-intelligence/hotels/:hotelId/executive-summary
 *
 * Customer path: CURRENT publication only (never STAGED fallback).
 * Founder preview: ?preview=staged|founder requires authenticated non-pilot-unauth.
 * Generation never triggers paid research / Surfe / Findymail.
 */

import {
  getGroupDemandIntelligenceFlagState,
  isGroupDemandIntelligenceEnabled,
  isGroupDemandIntelligencePilotReadAllowed,
} from "../lib/group-demand-intelligence/feature-flag.js";
import { loadHotelProfile } from "../lib/group-demand-intelligence/repository.js";
import { loadFeedback } from "../lib/group-demand-intelligence/repository.js";
import { listPursuits } from "../lib/group-demand-intelligence/pursuit/pursuit-store-v1.js";
import {
  buildGdiExecutiveSummaryV1,
  EXEC_SUMMARY_DATA_STATE,
  GDI_EXECUTIVE_SUMMARY_V1,
} from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import {
  resolvePreviewOpportunitiesFromPublicationPackage,
  resolveShareOpportunitiesFromApprovedPackage,
} from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

function flagGate(req, res) {
  if (
    isGroupDemandIntelligenceEnabled() ||
    isGroupDemandIntelligencePilotReadAllowed()
  ) {
    return true;
  }
  res.status(403).json({
    ok: false,
    error: "feature_disabled",
    message:
      "Group Demand Intelligence is disabled. Enable GROUP_DEMAND_INTELLIGENCE_V1 or GROUP_DEMAND_INTELLIGENCE_PILOT_READ.",
    flag: getGroupDemandIntelligenceFlagState(),
  });
  return false;
}

function text(v) {
  return String(v == null ? "" : v).trim();
}

/**
 * Resolve customer-authorized inventory for Executive Summary.
 * Never uses STAGED as automatic customer fallback.
 */
export function resolveExecutiveSummaryInventory(hotelId, opts = {}) {
  const preview = text(opts.preview).toLowerCase();
  const founderPreview =
    opts.founderPreview === true ||
    preview === "staged" ||
    preview === "founder";

  if (founderPreview) {
    const staged = resolvePreviewOpportunitiesFromPublicationPackage({
      hotelId,
      preferStaged: true,
      preview: "staged",
    });
    if (staged.ok && staged.mode === "PACKAGE") {
      return {
        ok: true,
        customerAuthorizedInventory: false,
        founderPreview: true,
        dataState: EXEC_SUMMARY_DATA_STATE.FOUNDER_PREVIEW,
        opportunities: staged.opportunities || [],
        publicationMeta: staged.meta || null,
      };
    }
    // Fall through to CURRENT for founder if STAGED missing
  }

  // Customer + default: CURRENT only via share resolver (no STAGED fallback).
  const current = resolveShareOpportunitiesFromApprovedPackage({ hotelId });
  if (current.ok && current.mode === "PACKAGE") {
    return {
      ok: true,
      customerAuthorizedInventory: true,
      founderPreview: false,
      dataState: EXEC_SUMMARY_DATA_STATE.CURRENT_AVAILABLE,
      opportunities: current.opportunities || [],
      publicationMeta: current.meta || null,
    };
  }

  // Inspect whether STAGED exists (for honest messaging — do not serve it).
  const store = createGdiPublicationPackageStore({});
  const stagedPtr = store.readPointer?.(hotelId, "STAGED");
  const hasStaged = Boolean(stagedPtr?.publicationVersion);
  const currentPtr = store.readPointer?.(hotelId, "CURRENT");

  if (hasStaged && !currentPtr?.publicationVersion) {
    return {
      ok: true,
      customerAuthorizedInventory: false,
      founderPreview: false,
      dataState: EXEC_SUMMARY_DATA_STATE.STAGED_ONLY_NOT_CUSTOMER,
      opportunities: [],
      publicationMeta: {
        reason: "STAGED_EXISTS_CURRENT_MISSING",
        stagedVersion: stagedPtr.publicationVersion,
      },
    };
  }

  return {
    ok: true,
    customerAuthorizedInventory: false,
    founderPreview: false,
    dataState: EXEC_SUMMARY_DATA_STATE.NO_ELIGIBLE_PUBLICATION,
    opportunities: [],
    publicationMeta: {
      reason: current.error || current.meta?.reason || "NO_CURRENT_PUBLICATION",
    },
  };
}

export async function getGdiExecutiveSummary(req, res) {
  try {
    if (!flagGate(req, res)) return;

    const hotelId = text(req.params.hotelId);
    if (!hotelId) {
      return res.status(400).json({ ok: false, error: "hotelId_required" });
    }

    const preview = text(req.query.preview).toLowerCase();
    const wantsFounderPreview =
      preview === "staged" || preview === "founder";

    if (wantsFounderPreview) {
      if (req.gdiPilotUnauthRead || !req.dealalityUser) {
        return res.status(401).json({
          ok: false,
          error: "authentication_required",
          message:
            "Founder Executive Summary preview requires authenticated access.",
        });
      }
    }

    const profile = loadHotelProfile(hotelId);
    const inventory = resolveExecutiveSummaryInventory(hotelId, {
      preview,
      founderPreview: wantsFounderPreview,
    });

    let pursuits = null;
    let feedback = null;
    try {
      pursuits = listPursuits(hotelId, {});
    } catch (err) {
      console.error(
        "[gdi-executive-summary] pursuits load failed",
        err?.message || err,
      );
      pursuits = null;
    }
    try {
      feedback = loadFeedback(hotelId);
    } catch (err) {
      console.error(
        "[gdi-executive-summary] feedback load failed",
        err?.message || err,
      );
      feedback = null;
    }

    const payload = buildGdiExecutiveSummaryV1({
      hotelId,
      hotelName:
        profile?.identity?.hotelName ||
        profile?.hotelName ||
        inventory.publicationMeta?.hotelName ||
        null,
      opportunities: inventory.opportunities,
      customerAuthorizedInventory: inventory.customerAuthorizedInventory,
      dataState: inventory.dataState,
      publicationMeta: inventory.publicationMeta,
      pursuits,
      feedback,
      lastResearchAt:
        inventory.publicationMeta?.publishedAt ||
        inventory.publicationMeta?.generatedAt ||
        null,
      founderPreview: inventory.founderPreview === true,
    });

    res.setHeader("X-GDI-Surface", "EXECUTIVE_SUMMARY_V1");
    res.setHeader("X-Robots-Tag", "noindex, nofollow, noarchive");
    return res.json({
      ...payload,
      experimentalLabel: "EXPERIMENTAL / PILOT",
      flag: getGroupDemandIntelligenceFlagState(),
      version: GDI_EXECUTIVE_SUMMARY_V1,
    });
  } catch (err) {
    console.error("[gdi-executive-summary]", err);
    return res.status(500).json({
      ok: false,
      error: "executive_summary_failed",
      message: String(err?.message || err),
    });
  }
}
