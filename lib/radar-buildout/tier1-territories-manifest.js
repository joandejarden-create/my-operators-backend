import { MEXICO_CITY_BUILD } from "./markets/mexico-city.build.js";
import { LOS_CABOS_BUILD } from "./markets/los-cabos.build.js";
import { GUADALAJARA_BUILD } from "./markets/guadalajara.build.js";
import { MONTERREY_BUILD } from "./markets/monterrey.build.js";
import { PUERTO_VALLARTA_RIVIERA_NAYARIT_BUILD } from "./markets/puerto-vallarta-riviera-nayarit.build.js";
import { MERIDA_YUCATAN_BUILD } from "./markets/merida-yucatan.build.js";
import { CUBA_BUILD } from "./markets/cuba.build.js";
import { HAITI_BUILD } from "./markets/haiti.build.js";
import { US_VIRGIN_ISLANDS_BUILD } from "./markets/us-virgin-islands.build.js";
import { MARTINIQUE_BUILD } from "./markets/martinique.build.js";
import { GUADELOUPE_BUILD } from "./markets/guadeloupe.build.js";
import { BONAIRE_BUILD } from "./markets/bonaire.build.js";
import { DOMINICAN_REPUBLIC_MATURE_BUILD } from "./markets/dominican-republic-mature.build.js";

/** Mexico market-by-market builds (post-Cancún). */
export const MEXICO_MARKET_BUILDS = [
  MEXICO_CITY_BUILD,
  LOS_CABOS_BUILD,
  GUADALAJARA_BUILD,
  MONTERREY_BUILD,
  PUERTO_VALLARTA_RIVIERA_NAYARIT_BUILD,
  MERIDA_YUCATAN_BUILD,
];

/** Caribbean / territory countrywide builds. */
export const TERRITORY_MARKET_BUILDS = [
  CUBA_BUILD,
  HAITI_BUILD,
  US_VIRGIN_ISLANDS_BUILD,
  MARTINIQUE_BUILD,
  GUADELOUPE_BUILD,
  BONAIRE_BUILD,
];

/** DR mature gap-fill (delta batch). */
export const DR_MATURE_MARKET_BUILD = DOMINICAN_REPUBLIC_MATURE_BUILD;

export const ALL_MARKET_BUILD_SPECS = [
  ...MEXICO_MARKET_BUILDS,
  DR_MATURE_MARKET_BUILD,
  ...TERRITORY_MARKET_BUILDS,
];
