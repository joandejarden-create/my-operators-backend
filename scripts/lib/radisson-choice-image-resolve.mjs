/**
 * Radisson (Choice) — downloadable image URLs when choicehotels.com/hoteldam blocks server fetch.
 * Sources: property press / architecture partner / IcePortal (public hotel photography).
 */
import { resolveFootprintOpeningImageUrl } from "../../scripts/lib/choice-footprint-opening-image-map.mjs";

export const RADISSON_CHOICE_PROPERTY_DIRECT_IMAGE = {
  mxpue: "https://banissimo.com.mx/wp-content/uploads/2024/10/hotel-radisson-fachada-1000x668.jpg.webp",
  slpap: "https://banissimo.com.mx/wp-content/uploads/2024/10/radisson-front-desk-1000x668.jpg.webp",
  pn018: "https://www.choicehotels.com/hoteldam/pn/pn018/images/1280/PN018AerialTemp1_1.jpg",
  sr001: "https://media.iceportal.com/175422/photos/82762885_XL.jpg",
};

function propertyIdFromPageUrl(url) {
  const parts = String(url || "")
    .replace(/\/$/, "")
    .split("/");
  return (parts[parts.length - 1] || "").toLowerCase();
}

/** Best-effort download URL for a Radisson by Choice property page. */
export function resolveRadissonChoiceImageDownloadUrl(propertyPageUrl) {
  const pid = propertyIdFromPageUrl(propertyPageUrl);
  if (RADISSON_CHOICE_PROPERTY_DIRECT_IMAGE[pid]) {
    return RADISSON_CHOICE_PROPERTY_DIRECT_IMAGE[pid];
  }
  return resolveFootprintOpeningImageUrl(propertyPageUrl);
}
