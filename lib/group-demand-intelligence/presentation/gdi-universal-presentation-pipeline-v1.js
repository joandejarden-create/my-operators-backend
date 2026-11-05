/**
 * Universal GDI presentation pipeline — single prepare → DTO path for
 * authenticated and external-share surfaces.
 *
 * Hotel inventory selection remains upstream (visibility / published registry).
 * This module only normalizes presentation once inventory is selected.
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

export const GDI_UNIVERSAL_PRESENTATION_PIPELINE_V1 =
  "gdi-universal-presentation-pipeline-v1";

/**
 * Project one opportunity into a customer-facing presentation DTO.
 *
 * @param {object} opportunity
 * @param {{ surface?: string, hotelId?: string, view?: "list"|"detail", shareResolve?: object }} opts
 */
export function projectOpportunityForPresentation(opportunity = {}, opts = {}) {
  const surface = opts.surface || GDI_PRESENTATION_SURFACES.LOCAL_AUTH;
  const hotelId = opts.hotelId || opportunity.hotelId || null;
  const view = opts.view === "detail" ? "detail" : "list";
  const isShare = surface === GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE;

  // Auth + share both start from customer presentation sanitize for drawer models.
  const customerPresented = sanitizeOpportunityForCustomerPresentation({
    ...opportunity,
    hotelId,
  });
  const prepared = prepareCustomerFacingOpportunity(customerPresented);

  let dto;
  if (view === "detail") {
    const shareSanitized = isShare ? sanitizeOpportunityForShare(prepared) : null;
    dto = buildPresentationDetailDto(prepared, shareSanitized, { surface, hotelId });
  } else {
    dto = buildPresentationListDto(prepared, { surface, hotelId });
  }

  const capabilities = resolveGdiPresentationCapabilities(
    isShare ? "share" : "auth",
    opts.shareResolve || null,
  );

  return {
    ok: true,
    pipelineVersion: GDI_UNIVERSAL_PRESENTATION_PIPELINE_V1,
    presentationContract: GDI_PRESENTATION_CONTRACT_V1,
    surface,
    capabilities,
    dto,
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
