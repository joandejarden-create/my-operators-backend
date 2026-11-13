/**
 * Universal GDI presentation pipeline — single prepare → DTO path for
 * authenticated and external-share surfaces.
 *
 * Hotel inventory selection remains upstream (visibility / published registry).
 * This module only normalizes presentation once inventory is selected.
 *
 * Published / locked snapshots are authoritative: hydrate + stamp only —
 * never rebuild titles, classifications, or drawer commercial copy.
 */
import { sanitizeOpportunityForCustomerPresentation } from "../customer-presentation-sanitize-v1.js";
import { sanitizeOpportunityForShare } from "../share/gdi-signed-share-capability-v1.js";
import {
  GDI_PRESENTATION_CONTRACT_V1,
  GDI_PRESENTATION_SURFACES,
  buildPresentationDetailDto,
  buildPresentationListDto,
  prepareCustomerFacingOpportunity,
} from "../contracts/gdi-presentation-contract-v1.js";
import { resolveGdiPresentationCapabilities } from "./gdi-presentation-capabilities-v1.js";
import { applyCustomerFacingContentRemediation } from "../publication-package/gdi-customer-facing-content-remediation-v1.js";

export const GDI_UNIVERSAL_PRESENTATION_PIPELINE_V1 =
  "gdi-universal-presentation-pipeline-v1";

export function isPublishedPresentationLocked(opportunity = {}) {
  return (
    opportunity?.publishedPresentationLocked === true ||
    opportunity?.gdiPublishedInventoryRegistry === true
  );
}

/**
 * Prepare one opportunity for presentation without surface-specific forks.
 * Locked published objects skip live commercial rebuild / sanitize rebuild.
 */
export function prepareOpportunityForUniversalPresentation(
  opportunity = {},
  opts = {},
) {
  const hotelId = opts.hotelId || opportunity.hotelId || null;
  const base = { ...opportunity, hotelId };
  if (isPublishedPresentationLocked(base) || opts.forceLocked === true) {
    return prepareCustomerFacingOpportunity(base);
  }
  // Bethesda internal research-stage reconstructions already carry honest
  // taxonomy + commercial copy. Customer sanitize can remint classifications
  // (e.g. VENUE_PARTNERSHIP → PLP) and must not rewrite this inventory.
  if (
    base.bethesdaInternalResearchInventory === true ||
    base.gdiInternalUnionOnly === true
  ) {
    return prepareCustomerFacingOpportunity({
      ...base,
      salesReady: false,
      customerFacing: false,
      customerVisible: false,
      publicationApproved: false,
    });
  }
  // Avoid double-sanitize when the API commercial-quality projector already
  // ran customer presentation (icons/contacts/titles). Re-sanitize can demote
  // evidence-backed revenue-stream icons after thesis prose scrub.
  // Content-quality remediation still runs (idempotent) so TBD/duplicates/conflicts
  // are corrected even when sanitize was skipped.
  if (base.gdiCustomerPresentationSanitizedV1 === true) {
    return prepareCustomerFacingOpportunity(
      applyCustomerFacingContentRemediation(base),
    );
  }
  const sanitized = sanitizeOpportunityForCustomerPresentation(base);
  const remediated = applyCustomerFacingContentRemediation(sanitized);
  return prepareCustomerFacingOpportunity({
    ...remediated,
    gdiCustomerPresentationSanitizedV1: true,
  });
}

/**
 * Project one opportunity into a customer-facing presentation DTO.
 *
 * @param {object} opportunity
 * @param {{ surface?: string, hotelId?: string, view?: "list"|"detail", shareResolve?: object, forceLocked?: boolean }} opts
 */
export function projectOpportunityForPresentation(opportunity = {}, opts = {}) {
  const surface = opts.surface || GDI_PRESENTATION_SURFACES.LOCAL_AUTH;
  const hotelId = opts.hotelId || opportunity.hotelId || null;
  const view = opts.view === "detail" ? "detail" : "list";
  const isShare = surface === GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE;

  const prepared = prepareOpportunityForUniversalPresentation(opportunity, {
    hotelId,
    forceLocked: opts.forceLocked,
  });
  const locked = isPublishedPresentationLocked(prepared) || opts.forceLocked === true;

  let dto;
  if (view === "detail") {
    // Locked published objects already carry approvedCustomerFacingDetail from
    // hydrate — do not run the share whitelist sanitize (it drops that object).
    // Unlocked share still uses sanitize for evidence-tier gating.
    const shareSanitized =
      isShare && !locked ? sanitizeOpportunityForShare(prepared) : null;
    dto = buildPresentationDetailDto(prepared, shareSanitized, { surface, hotelId });
  } else {
    dto = buildPresentationListDto(prepared, { surface, hotelId });
  }

  const capabilities = resolveGdiPresentationCapabilities(
    isShare ? "share" : "auth",
    opts.shareResolve || null,
  );

  // Final customer-facing remediation on DTO fields (tile/composites + action align).
  // Locked published packages skip rewrite — package content is already approved.
  const finalDto = locked
    ? dto
    : applyCustomerFacingContentRemediation(dto);

  return {
    ok: true,
    pipelineVersion: GDI_UNIVERSAL_PRESENTATION_PIPELINE_V1,
    presentationContract: GDI_PRESENTATION_CONTRACT_V1,
    surface,
    locked,
    capabilities,
    prepared,
    dto: finalDto,
  };
}

/**
 * Map an inventory array through the universal pipeline.
 */
export function projectOpportunitiesForPresentation(opportunities = [], opts = {}) {
  return (Array.isArray(opportunities) ? opportunities : []).map((o) =>
    projectOpportunityForPresentation(o, opts),
  );
}

/**
 * API helper: return DTO rows for list responses (auth or share).
 */
export function mapOpportunitiesToPresentationDtos(opportunities = [], opts = {}) {
  return projectOpportunitiesForPresentation(opportunities, {
    ...opts,
    view: "list",
  }).map((row) => row.dto);
}

/**
 * API helper: return a single detail DTO (auth or share).
 */
export function mapOpportunityToPresentationDetailDto(
  opportunity = {},
  opts = {},
) {
  return projectOpportunityForPresentation(opportunity, {
    ...opts,
    view: "detail",
  });
}
