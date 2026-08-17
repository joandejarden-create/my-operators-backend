/**
 * Public Profile Stabilization — content pack registry.
 * Only brands currently failing public-full PVQL gates.
 * Passing public brands are intentionally absent (never patched here).
 */
import { EVERHOME_STABILIZATION_CONTENT } from "./brand-explorer-public-profile-stabilization-content-everhome.js";
import { KIMPTON_STABILIZATION_CONTENT } from "./brand-explorer-public-profile-stabilization-content-kimpton.js";
import { DESIGN_HOTELS_STABILIZATION_CONTENT } from "./brand-explorer-public-profile-stabilization-content-design-hotels.js";
import { ASCEND_STABILIZATION_CONTENT } from "./brand-explorer-public-profile-stabilization-content-ascend.js";
import { COMFORT_STABILIZATION_CONTENT } from "./brand-explorer-public-profile-stabilization-content-comfort.js";
import { CURIO_STABILIZATION_CONTENT } from "./brand-explorer-public-profile-stabilization-content-curio.js";
import { TRIBUTE_STABILIZATION_CONTENT } from "./brand-explorer-public-profile-stabilization-content-tribute.js";

/** Public-full brands that already pass — refuse writes. */
export const PUBLIC_STABILIZATION_PROTECTED_PASSING = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "radisson-individuals-by-choice",
  "small-luxury-hotels-of-the-world",
]);

/** Primary public-full failures (hard PVQL fail). */
export const PUBLIC_STABILIZATION_PRIMARY_TARGETS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "design-hotels",
]);

/** Legacy public-full failures (flagged until remediated for baseline freeze). */
export const PUBLIC_STABILIZATION_LEGACY_TARGETS = Object.freeze([
  "ascend",
  "comfort-inn-suites",
  "curio-collection",
  "tribute-portfolio",
]);

export const PUBLIC_STABILIZATION_TARGETS = Object.freeze([
  ...PUBLIC_STABILIZATION_PRIMARY_TARGETS,
  ...PUBLIC_STABILIZATION_LEGACY_TARGETS,
]);

export const PUBLIC_STABILIZATION_CONTENT_BY_SLUG = Object.freeze({
  "everhome-suites": EVERHOME_STABILIZATION_CONTENT,
  kimpton: KIMPTON_STABILIZATION_CONTENT,
  "design-hotels": DESIGN_HOTELS_STABILIZATION_CONTENT,
  ascend: ASCEND_STABILIZATION_CONTENT,
  "comfort-inn-suites": COMFORT_STABILIZATION_CONTENT,
  "curio-collection": CURIO_STABILIZATION_CONTENT,
  "tribute-portfolio": TRIBUTE_STABILIZATION_CONTENT,
});
