/**
 * Canonical customer-facing inventory resolution contract V1.
 *
 * Hotel-scoped. Deterministic. Does not mutate dispositions.
 *
 * Precedence (high → low):
 *  1. Explicit archive / prospectSuppressed / founderCustomerVisibilityDisable
 *  2. Waterstone clean primary sales inventory (PRIMARY_SALES_READY)
 *  3. Shared isCustomerFacingOpportunity (live-campaign shadow, inventory lanes,
 *     readiness, reviewed prospects, etc.)
 *
 * List API contract:
 *  - getGdiOpportunities returns the already-filtered CUSTOMER_FACING set
 *  - Response header X-GDI-List-Scope: CUSTOMER_FACING
 *  - Do NOT re-apply filterCustomerFacingOpportunities to list DTOs for count audits
 *    unless visibility stamps are present (customerVisible, waterstonePrimary*,
 *    localShadowOnly, liveCampaignV1Shadow).
 */
import { mergeLocalShadowCommercialOpportunities } from "./local-shadow-commercial-portfolio-v1.js";
import {
  filterCustomerFacingOpportunities,
  isCustomerFacingOpportunity,
} from "./customer-visibility.js";
import { createHash } from "node:crypto";

export const CANONICAL_CUSTOMER_FACING_INVENTORY_V1 =
  "gdi-canonical-customer-facing-inventory-v1";

export const GDI_LIST_SCOPE = Object.freeze({
  CUSTOMER_FACING: "CUSTOMER_FACING",
  INTERNAL_ALL: "INTERNAL_ALL",
  FULL_UNFILTERED: "FULL_UNFILTERED",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function fingerprintIdSet(ids = []) {
  const sorted = [...new Set((ids || []).map(String))].sort();
  return {
    count: sorted.length,
    ids: sorted,
    sha256_16: createHash("sha256")
      .update(JSON.stringify(sorted))
      .digest("hex")
      .slice(0, 16),
  };
}

/**
 * Resolve canonical customer-facing opportunities for a hotel.
 */
export function resolveCanonicalCustomerFacingInventory(
  hotelId,
  baseOpportunities = [],
  opts = {},
) {
  const hid = String(hotelId || "");
  const merged = mergeLocalShadowCommercialOpportunities(
    hid,
    Array.isArray(baseOpportunities) ? baseOpportunities : [],
    opts.mergeOpts || {},
  );
  const facing = filterCustomerFacingOpportunities(merged, {
    hotelId: hid,
    ...(opts.visibilityOpts || {}),
  });
  const fp = fingerprintIdSet(facing.map((o) => o.id));
  return {
    version: CANONICAL_CUSTOMER_FACING_INVENTORY_V1,
    hotelId: hid,
    listScope: GDI_LIST_SCOPE.CUSTOMER_FACING,
    mergedCount: merged.length,
    facingCount: facing.length,
    opportunities: facing,
    fingerprint: fp,
  };
}

/**
 * Compare canonical merge facing vs an API list payload.
 * If API already filtered (listScope/header), compare ID sets directly.
 */
export function reconcileMergeApiBrowserSurfaces({
  hotelId,
  mergeFacingIds = [],
  apiPayloadIds = [],
  apiAlreadyFiltered = true,
  apiFacingAfterRefilterIds = null,
  browserIds = null,
} = {}) {
  const merge = fingerprintIdSet(mergeFacingIds);
  const api = fingerprintIdSet(apiPayloadIds);
  const mergeOnly = merge.ids.filter((id) => !api.ids.includes(id));
  const apiOnly = api.ids.filter((id) => !merge.ids.includes(id));
  const both = merge.ids.filter((id) => api.ids.includes(id));

  let refilterArtifact = null;
  if (Array.isArray(apiFacingAfterRefilterIds)) {
    const re = fingerprintIdSet(apiFacingAfterRefilterIds);
    refilterArtifact = {
      count: re.count,
      mergeOnlyVsRefilter: merge.ids.filter((id) => !re.ids.includes(id)),
      note: "Refiltering already-filtered list DTOs is a measurement artifact when visibility stamps are stripped",
    };
  }

  let browser = null;
  if (Array.isArray(browserIds)) {
    const b = fingerprintIdSet(browserIds);
    browser = {
      count: b.count,
      browserOnly: b.ids.filter((id) => !merge.ids.includes(id)),
      mergeOnlyVsBrowser: merge.ids.filter((id) => !b.ids.includes(id)),
      parityWithMerge: b.sha256_16 === merge.sha256_16,
    };
  }

  const exactParity =
    merge.sha256_16 === api.sha256_16 &&
    (browser ? browser.parityWithMerge : true);

  return {
    version: CANONICAL_CUSTOMER_FACING_INVENTORY_V1,
    hotelId,
    apiAlreadyFiltered,
    merge,
    api,
    bothCount: both.length,
    mergeOnly,
    apiOnly,
    refilterArtifact,
    browser,
    exactParity,
    rootCauseIfMismatch:
      !exactParity && mergeOnly.length === 0 && apiOnly.length === 0
        ? null
        : mergeOnly.length || apiOnly.length
          ? "ID_SET_MISMATCH"
          : refilterArtifact?.mergeOnlyVsRefilter?.length
            ? "DOUBLE_FILTER_MEASUREMENT_ARTIFACT"
            : null,
  };
}

/**
 * Guard: list DTO must retain stamps needed for visibility re-evaluation.
 */
export function assertListDtoVisibilityStamps(dto = {}) {
  const missing = [];
  if (!("customerVisible" in dto) && !("gdiListScope" in dto)) {
    missing.push("customerVisible_or_gdiListScope");
  }
  return {
    ok: missing.length === 0 || dto.gdiListScope === GDI_LIST_SCOPE.CUSTOMER_FACING,
    missing,
    gdiListScope: dto.gdiListScope || null,
  };
}

export function explainFacingDecision(opportunity = {}, hotelId = "") {
  const facing = isCustomerFacingOpportunity(opportunity, { hotelId });
  const reasons = [];
  if (opportunity.archived || opportunity.isArchived) reasons.push("ARCHIVED");
  if (opportunity.prospectSuppressed) reasons.push("PROSPECT_SUPPRESSED");
  if (opportunity.waterstonePrimarySalesInventory && opportunity.waterstoneInventoryLane === "PRIMARY_SALES_READY") {
    reasons.push("WATERSTONE_PRIMARY_SALES_READY");
  }
  if (opportunity.certificationFixtureOnly) reasons.push("CERTIFICATION_FIXTURE");
  if (opportunity.liveCampaignV1Shadow && opportunity.localShadowOnly) {
    reasons.push("LIVE_CAMPAIGN_LOCAL_SHADOW");
  }
  return { facing, reasons, hotelId };
}
