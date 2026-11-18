/**
 * Inspect STAGED / CURRENT publication availability for founder preview recovery.
 * Read-only — never falls back across hotels or invents packages.
 */

import { createGdiPublicationPackageStore } from "./gdi-publication-package-store-v1.js";

export const GDI_FOUNDER_PREVIEW_AVAILABILITY_V1 =
  "gdi-founder-preview-availability-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * @returns {{
 *   hotelId: string,
 *   hasStagedPublication: boolean,
 *   hasCurrentPublication: boolean,
 *   stagedVersion: string|null,
 *   currentVersion: string|null,
 *   recoveryActions: string[],
 * }}
 */
export function inspectFounderPreviewPublicationAvailability(input = {}) {
  const hotelId = text(input.hotelId);
  const store =
    input.store ||
    createGdiPublicationPackageStore({
      env: input.env || process.env,
      root: input.root,
    });
  if (!hotelId) {
    return {
      hotelId: "",
      hasStagedPublication: false,
      hasCurrentPublication: false,
      stagedVersion: null,
      currentVersion: null,
      recoveryActions: ["BACK_TO_HOTELS", "CHOOSE_ANOTHER_HOTEL"],
    };
  }

  const stagedPtr = store.readPointer(hotelId, "STAGED");
  const hasStagedPublication = Boolean(stagedPtr?.publicationVersion);
  const current = store.loadCustomerVisiblePackage(hotelId);
  const hasCurrentPublication = Boolean(current?.ok);

  const recoveryActions = ["BACK_TO_HOTELS", "CHOOSE_ANOTHER_HOTEL"];
  if (hasCurrentPublication) {
    recoveryActions.push("VIEW_PUBLISHED_VERSION");
  }

  return {
    version: GDI_FOUNDER_PREVIEW_AVAILABILITY_V1,
    hotelId,
    hasStagedPublication,
    hasCurrentPublication,
    stagedVersion: stagedPtr?.publicationVersion || null,
    currentVersion: current?.ok
      ? current.package?.publicationVersion ||
        current.pointer?.publicationVersion ||
        null
      : null,
    recoveryActions,
  };
}

export default {
  GDI_FOUNDER_PREVIEW_AVAILABILITY_V1,
  inspectFounderPreviewPublicationAvailability,
};
