/**
 * GDI_PRESENTATION_CONTRACT_V1
 *
 * One presentation stack for local auth, production auth, and external share.
 * UI differences are capability-gated — not separate renderers.
 */
import {
  buildAuthCustomerFacingListDto,
  buildShareCustomerFacingDetailDto,
  buildShareCustomerFacingListDto,
  diffCustomerFacingParity,
  prepareCustomerFacingOpportunity,
} from "../share/gdi-customer-facing-opportunity-contract-v1.js";
import {
  SHARE_CUSTOMER_PRESENTATION_PARITY_V1,
  SHARE_PRESENTATION_PARITY_KEYS,
  diffSharePresentationFields,
} from "../share/share-customer-presentation-parity-v1.js";
import { resolveGdiPresentationCapabilities } from "../presentation/gdi-presentation-capabilities-v1.js";

export const GDI_PRESENTATION_CONTRACT_V1 = "gdi-presentation-contract-v1";

/** Surfaces that must share DealalityGdiUi + group-demand-intelligence.css. */
export const GDI_PRESENTATION_SURFACES = Object.freeze({
  LOCAL_AUTH: "local_auth",
  PRODUCTION_AUTH: "production_auth",
  EXTERNAL_SHARE: "external_share",
});

/** Shared renderer asset identity (cache-bust must stay aligned). */
export const GDI_SHARED_UI_ASSETS = Object.freeze({
  renderer: "/js/group-demand-intelligence/dealality-gdi-ui.js",
  stylesheet: "/css/group-demand-intelligence.css",
  sharedVisibilityCss: "/js/ai-visibility/ai-visibility-shared.css",
  cacheBust: "gdi-single-inventory-parity-v1",
});

export {
  SHARE_CUSTOMER_PRESENTATION_PARITY_V1,
  SHARE_PRESENTATION_PARITY_KEYS,
  prepareCustomerFacingOpportunity,
  buildAuthCustomerFacingListDto,
  buildShareCustomerFacingListDto,
  buildShareCustomerFacingDetailDto,
  diffCustomerFacingParity,
  diffSharePresentationFields,
  resolveGdiPresentationCapabilities,
};

/**
 * Build a presentation row for a surface. Auth and share share the same
 * prepare → list DTO path; share additionally strips internal-only keys and
 * stamps mode=read_only via buildShareCustomerFacingListDto.
 */
export function buildPresentationListDto(opportunity = {}, opts = {}) {
  const surface = opts.surface || GDI_PRESENTATION_SURFACES.LOCAL_AUTH;
  const hotelId = opts.hotelId || null;
  if (surface === GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE) {
    return buildShareCustomerFacingListDto(opportunity, { hotelId });
  }
  return buildAuthCustomerFacingListDto(opportunity, { hotelId });
}

export function buildPresentationDetailDto(
  opportunity = {},
  sanitized = null,
  opts = {},
) {
  const surface = opts.surface || GDI_PRESENTATION_SURFACES.LOCAL_AUTH;
  const hotelId = opts.hotelId || null;
  if (surface === GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE) {
    return buildShareCustomerFacingDetailDto(opportunity, sanitized, { hotelId });
  }
  const prepared = prepareCustomerFacingOpportunity(opportunity);
  const list = buildAuthCustomerFacingListDto(opportunity, { hotelId });
  const classification =
    prepared.commercialInventoryClass ||
    prepared.commercialClassification ||
    opportunity.commercialInventoryClass ||
    opportunity.commercialClassification ||
    null;
  return {
    ...prepared,
    ...list,
    commercialInventoryClass: classification,
    commercialClassification: classification,
    potentialBusiness:
      prepared.potentialBusiness ||
      prepared.potentialHotelBusiness ||
      list.potentialBusiness ||
      list.potentialHotelBusiness ||
      null,
    potentialHotelBusiness:
      prepared.potentialHotelBusiness ||
      prepared.potentialBusiness ||
      list.potentialHotelBusiness ||
      list.potentialBusiness ||
      null,
    mode: "authenticated",
  };
}

/**
 * Certify auth list DTO ↔ share list DTO parity for one opportunity.
 */
export function certifyPresentationParity(opportunity = {}, opts = {}) {
  const hotelId = opts.hotelId || null;
  const auth = buildAuthCustomerFacingListDto(opportunity, { hotelId });
  const share = buildShareCustomerFacingListDto(opportunity, { hotelId });
  const fieldDiffs = diffCustomerFacingParity(auth, share);
  const presentationDiffs = diffSharePresentationFields(auth, share);
  const substantive = presentationDiffs.filter((d) => d.severity === "substantive");
  return {
    ok: fieldDiffs.length === 0 && substantive.length === 0,
    contractVersion: GDI_PRESENTATION_CONTRACT_V1,
    fieldDiffs,
    presentationDiffs,
    auth,
    share,
  };
}
