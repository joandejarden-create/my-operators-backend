/**
 * GDI presentation capability model — permission-based feature controls.
 * Same UI renderer; capabilities differ by surface (auth vs external share).
 */
import { GDI_SHARE_CAPABILITY } from "../share/gdi-signed-share-capability-v1.js";

export const GDI_PRESENTATION_CAPABILITIES_V1 =
  "gdi-presentation-capabilities-v1";

/** Named product capabilities (surface-agnostic). */
export const GDI_UI_CAPABILITY = Object.freeze({
  canBrowseOpportunities: "canBrowseOpportunities",
  canOpenDrawer: "canOpenDrawer",
  canExportCSV: "canExportCSV",
  canFilterAndSort: "canFilterAndSort",
  canRunResearch: "canRunResearch",
  canEditOpportunity: "canEditOpportunity",
  canSaveHotelFeedback: "canSaveHotelFeedback",
  canEnrichContact: "canEnrichContact",
  canViewResearchHistory: "canViewResearchHistory",
  canSwitchHotel: "canSwitchHotel",
  canValidate: "canValidate",
  canRecordAction: "canRecordAction",
  canRecordOutcome: "canRecordOutcome",
  canViewAdminChrome: "canViewAdminChrome",
});

const AUTH_FULL = Object.freeze({
  [GDI_UI_CAPABILITY.canBrowseOpportunities]: true,
  [GDI_UI_CAPABILITY.canOpenDrawer]: true,
  [GDI_UI_CAPABILITY.canExportCSV]: true,
  [GDI_UI_CAPABILITY.canFilterAndSort]: true,
  [GDI_UI_CAPABILITY.canRunResearch]: true,
  [GDI_UI_CAPABILITY.canEditOpportunity]: true,
  [GDI_UI_CAPABILITY.canSaveHotelFeedback]: true,
  [GDI_UI_CAPABILITY.canEnrichContact]: true,
  [GDI_UI_CAPABILITY.canViewResearchHistory]: true,
  [GDI_UI_CAPABILITY.canSwitchHotel]: true,
  [GDI_UI_CAPABILITY.canValidate]: true,
  [GDI_UI_CAPABILITY.canRecordAction]: true,
  [GDI_UI_CAPABILITY.canRecordOutcome]: true,
  [GDI_UI_CAPABILITY.canViewAdminChrome]: true,
});

const SHARE_BASE = Object.freeze({
  [GDI_UI_CAPABILITY.canBrowseOpportunities]: true,
  [GDI_UI_CAPABILITY.canOpenDrawer]: true,
  [GDI_UI_CAPABILITY.canExportCSV]: true,
  [GDI_UI_CAPABILITY.canFilterAndSort]: true,
  [GDI_UI_CAPABILITY.canRunResearch]: false,
  [GDI_UI_CAPABILITY.canEditOpportunity]: false,
  [GDI_UI_CAPABILITY.canSaveHotelFeedback]: false,
  [GDI_UI_CAPABILITY.canEnrichContact]: false,
  [GDI_UI_CAPABILITY.canViewResearchHistory]: false,
  [GDI_UI_CAPABILITY.canSwitchHotel]: false,
  [GDI_UI_CAPABILITY.canValidate]: false,
  [GDI_UI_CAPABILITY.canRecordAction]: false,
  [GDI_UI_CAPABILITY.canRecordOutcome]: false,
  [GDI_UI_CAPABILITY.canViewAdminChrome]: false,
});

function hasShareCap(caps, key) {
  return Array.isArray(caps) && caps.map(String).includes(key);
}

/**
 * Resolve UI capabilities for a presentation surface.
 *
 * @param {"auth"|"share"} mode
 * @param {{ capabilities?: string[], canValidate?: boolean, canRecordAction?: boolean, canRecordOutcome?: boolean }} shareResolve
 */
export function resolveGdiPresentationCapabilities(mode = "auth", shareResolve = null) {
  if (mode !== "share") {
    return {
      version: GDI_PRESENTATION_CAPABILITIES_V1,
      mode: "auth",
      ...AUTH_FULL,
    };
  }

  const caps = shareResolve?.capabilities || [];
  const out = { ...SHARE_BASE, version: GDI_PRESENTATION_CAPABILITIES_V1, mode: "share" };

  out.canBrowseOpportunities =
    !caps.length ||
    hasShareCap(caps, GDI_SHARE_CAPABILITY.CAN_READ_OPPORTUNITIES);
  out.canOpenDrawer =
    !caps.length ||
    hasShareCap(caps, GDI_SHARE_CAPABILITY.CAN_READ_OPPORTUNITY_DETAIL) ||
    hasShareCap(caps, GDI_SHARE_CAPABILITY.CAN_READ_OPPORTUNITIES);
  out.canValidate =
    shareResolve?.canValidate === true ||
    hasShareCap(caps, GDI_SHARE_CAPABILITY.CAN_VALIDATE);
  out.canRecordAction =
    shareResolve?.canRecordAction === true ||
    hasShareCap(caps, GDI_SHARE_CAPABILITY.CAN_RECORD_ACTION);
  out.canRecordOutcome =
    shareResolve?.canRecordOutcome === true ||
    hasShareCap(caps, GDI_SHARE_CAPABILITY.CAN_RECORD_OUTCOME);
  // CSV is part of opportunities surface when browse is allowed.
  out.canExportCSV = out.canBrowseOpportunities;

  return out;
}

/** Permissions object expected by DealalityGdiUi shareCustomerValidationFormHtml. */
export function toDealalityGdiUiPermissionContext(capabilities = {}) {
  return {
    canValidate: !!capabilities.canValidate,
    canRecordAction: !!capabilities.canRecordAction,
    canRecordOutcome: !!capabilities.canRecordOutcome,
    canExportCSV: !!capabilities.canExportCSV,
    canRunResearch: !!capabilities.canRunResearch,
    canEditOpportunity: !!capabilities.canEditOpportunity,
    canSaveHotelFeedback: !!capabilities.canSaveHotelFeedback,
    canEnrichContact: !!capabilities.canEnrichContact,
    canViewResearchHistory: !!capabilities.canViewResearchHistory,
    canSwitchHotel: !!capabilities.canSwitchHotel,
    canViewAdminChrome: !!capabilities.canViewAdminChrome,
  };
}
