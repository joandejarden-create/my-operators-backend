/**
 * ICP classification for GTM Owner Targets (internal only).
 * Separates asset owners from franchisors, brokers, and low-fit SPVs.
 */
import { normalizeOwnerKey } from "./normalize.js";
import { summarizePropertyFootprint } from "./cala-footprint.js";
import { BROKER_COMPANY_PATTERN } from "./owner-contact-sync.js";
import { VAL_GTM_ICP_STRIKE_ELIGIBLE } from "./field-map.js";
import { inferOwnerDealTrigger } from "./branding-decision-signals.js";

/**
 * Global franchisor / brand-platform True Owners — marketplace supply side, not owner ICP.
 * Keys are normalizeOwnerKey() values.
 */
export const FRANCHISOR_BRAND_OWNER_KEYS = new Set(
  [
    "Marriott International",
    "Marriott Vacations Worldwide",
    "Hyatt Corporation",
    "Hyatt Hotels Corporation",
    "Hilton Worldwide Holdings Inc",
    "Hilton Worldwide",
    "Hilton Domestic Operating Company Inc",
    "Accor",
    "Accor SA",
    "Accor S.A.",
    "InterContinental Hotels Group",
    "IHG Hotels & Resorts",
    "Wyndham Hotels & Resorts",
    "Wyndham Hotel Group",
    "Choice Hotels International",
    "Best Western Hotels & Resorts",
    "Radisson Hotel Group",
    "Melia Hotels International, S.A",
    "Melia Hotels International S.A",
    "NH Hotel Group",
    "Minor Hotel Group Limited",
    "Minor Europe",
    "Starwood Hotels",
    "Starwood Hotels & Resorts Worldwide",
    "Belmond Management Ltd",
    "Belmond Ltd",
    "Four Seasons Hotels and Resorts",
    "Langham Hospitality Group",
    "Loews Hotels",
    "Loews Hotels & Co",
    "Extended Stay America",
    "Extended Stay Hotels",
    "Sonesta International Hotels Corporation",
    "Sonesta International Hotels Corp",
    "Red Roof Inn",
    "G6 Hospitality LLC",
    "G6 Hospitality",
    "RLJ Lodging Trust",
    "Park Hotels & Resorts Inc",
    "Host Hotels & Resorts Inc",
    "Host Hotels & Resorts L.P.",
    "Apple Hospitality REIT Inc",
    "Ryman Hospitality Properties Inc",
    "Pebblebrook Hotel Trust",
    "Sunstone Hotel Investors Inc",
    "Xenia Hotels & Resorts Inc",
    "Ashford Hospitality Trust Inc",
    "Chatham Lodging Trust",
    "Summit Hotel Properties Inc",
    "Hersha Hospitality Trust",
    "LaSalle Hotel Properties",
    "DiamondRock Hospitality Company",
    "Rexford Industrial Realty Inc",
    "Whitestone REIT",
    "Strategic Hotels & Resorts Inc",
    "Strategic Hotels and Resorts",
    "Wyndham Destinations",
    "Travel + Leisure Co",
    "H World Group Limited",
    "Huazhu Group Limited",
    "Jin Jiang International",
    "Jin Jiang International Holdings",
    "BTG Hotels Group",
    "Shangri-La International Hotel Management Ltd",
    "Shangri-La International Hotel Management",
    "Kempinski Hotels S.A.",
    "Kempinski Hotels",
    "Rotana Hotel Management Corporation",
    "Rotana Hotel Management",
    "Louvre Hotels Group",
    "Louvre Hotels",
    "BWH Hotel Group",
    "Best Western International",
    "Groupe du Louvre",
    "Groupe Du Louvre",
  ].map(normalizeOwnerKey)
);

/**
 * Integrated owner-operators that share naming patterns with franchisors.
 */
export const INTEGRATED_OPERATOR_ALLOWLIST_KEYS = new Set(
  [
    "Grupo Posadas, S.A.B. DE C.V.",
    "Grupo Posadas S.A.B. de C.V.",
    "Norte 19",
    "Atlantica Hotels International (Brasil) Ltda.",
    "HM Hotels",
    "Grupo Brisas",
    "Grupo Hotelero Santa Fe",
    "Camino Real Hoteles",
    "Real Hotels & Resorts",
    "Parks Hospitality Holdings",
    "Irawadi Corp S.A.",
    "RCD Hotels",
    "Casa Andina Hoteles",
    "Hoteleria Peruana S.A.C.",
    "Enchanting Hotels",
    "Nacional Inn Hotéis",
    "ICH Administracao de Hoteis S.A.",
    "Cubanacan Hotel Group",
    "Howard Johnson Argentina",
    "Faranda Hotels & Resorts",
    "Milenium Grupo Hotelero Mexicano",
    "Estancias Extendidas",
    "Böëna Lodges",
    "Hodelpa",
    "Hoteles Hodelpa",
    "Grupo Hotelero Hodelpa",
    "Lifestyle Resort Management S.R.L.",
    "Lifestyle Holidays Vacation Club",
    "Servicios Corporativos Piñero S.L.",
    "Tortuga Resorts",
    "Caribe Hospitality",
    "Essendi",
    "Fibra Hotel Mexico",
    "Grupo Xcaret",
    "Palace Resorts",
    "Grupo Palace Resorts",
    "Ocean Hotels Barbados",
    "Karisma Hotels & Resorts",
    "Princess Hotels & Resorts",
    "Tafer Hotels & Resorts",
    "Porta Hotels",
    "HOLA Hotels & Resorts",
    "Bern Hotels & Resorts",
    "Pueblo Bonito Hotels and Resorts",
    "Grupo SIXSTAR Hotels",
    "Bourbon Hotels & Resorts",
    "Eurostars Hotel Company S.L.",
    "JHSF",
    "Pestana Management, S.A.",
    "Grupo de Turismo Gaviota",
    "Viva Wyndham Resorts",
    "Ocean Coral Spring",
    "Barcelo Gestion Hotelera S.L.",
    "Barceló Gestión Hotelera",
    "Iberostar Hotels & Resorts",
    "Riu Hotels & Resorts",
    "Sandals Resorts",
    "Royalton Hotels & Resorts",
    "Palladium Hotel Group",
    "Catalonia Hotels & Resorts",
  ].map(normalizeOwnerKey)
);

const FRANCHISOR_NAME_PATTERN =
  /\b(marriott international|hilton worldwide|hyatt corporation|hyatt hotels corporation|intercontinental hotels|ihg hotels|wyndham hotels & resorts|wyndham hotel group|choice hotels international|best western hotels|radisson hotel group|melia hotels international|minor hotel group|starwood hotels|belmond management|four seasons hotels|langham hospitality|host hotels & resorts|apple hospitality reit|park hotels & resorts)\b/i;

const OPAQUE_SPV_PATTERN =
  /\b(spe|s\.p\.e|sap[ií]\s*de\s*c\.?v\.?|s\.a\.p\.i|holding\s+llc|property\s+llc|investment\s+llc|fondo\s+de\s+inversion|fideicomiso)\b/i;

const HOTEL_OPERATOR_NAME_PATTERN =
  /\b(hoteles?|hotels?|resorts?|hospitality|hotelero|hotelaria|gestion\s+hotelera|lodges?|inn\b|suites)\b/i;

/**
 * @param {object} input
 * @param {string} input.ownerName
 * @param {string} [input.ownerType]
 * @param {string} [input.priorityTier]
 * @param {number} [input.propertyCount]
 * @param {object[]} [input.properties]
 * @param {object} [input.contact]
 * @param {string} [input.existingDealTrigger]
 * @param {number} [input.developmentPipelineCount]
 */
export function classifyOwnerIcp(input) {
  const ownerName = String(input.ownerName || "").trim();
  const ownerKey = normalizeOwnerKey(ownerName);
  const ownerType = String(input.ownerType || "unknown");
  const priorityTier = String(input.priorityTier || "C");
  const properties = input.properties || [];
  const footprint = summarizePropertyFootprint(properties);
  const propertyCount = input.propertyCount ?? footprint.totalPropertyCount;
  const calaPropertyCount = footprint.calaPropertyCount;

  const contact = input.contact || {};
  const hasVerifiedContact = Boolean(contact.hasVerifiedContact);
  const hasPrimaryEmail = Boolean(contact.primaryContactEmail);
  const hasReachableContact = hasVerifiedContact || hasPrimaryEmail;

  /** @type {string[]} */
  const reasons = [];
  let icpSegment = "unknown";

  if (!ownerName) {
    icpSegment = "skip";
    reasons.push("missing_owner_name");
  } else if (INTEGRATED_OPERATOR_ALLOWLIST_KEYS.has(ownerKey)) {
    icpSegment = "owner_operator";
    reasons.push("integrated_operator_allowlist");
  } else if (FRANCHISOR_BRAND_OWNER_KEYS.has(ownerKey) || FRANCHISOR_NAME_PATTERN.test(ownerName)) {
    icpSegment = "franchisor_brand";
    reasons.push("franchisor_or_brand_platform");
  } else if (BROKER_COMPANY_PATTERN.test(ownerName) || /\b(broker|brokerage|advisors?|consulting)\b/i.test(ownerName)) {
    icpSegment = "broker_advisor";
    reasons.push("broker_or_advisor_name");
  } else if (calaPropertyCount === 0) {
    icpSegment = "skip";
    reasons.push("no_cala_hotels");
  } else if (propertyCount === 1 && (ownerType === "spv" || OPAQUE_SPV_PATTERN.test(ownerName))) {
    icpSegment = "spv_single_asset";
    reasons.push("single_asset_spv");
  } else if (ownerType === "gaming_hospitality") {
    icpSegment = "gaming_hospitality";
    reasons.push("gaming_hospitality_owner_type");
  } else if (ownerType === "reit") {
    icpSegment = "reit";
    reasons.push("reit_owner_type");
  } else if (ownerType === "institutional") {
    icpSegment = "institutional";
    reasons.push("institutional_owner_type");
  } else if (ownerType === "integrated_operator" || HOTEL_OPERATOR_NAME_PATTERN.test(ownerName)) {
    icpSegment = "owner_operator";
    reasons.push(ownerType === "integrated_operator" ? "integrated_operator_type" : "hotel_operator_name");
  } else if (ownerType === "regional_operator") {
    icpSegment = "regional_operator";
    reasons.push("regional_operator_type");
  } else if (ownerType === "individual") {
    icpSegment = propertyCount >= 2 ? "asset_owner" : "individual";
    reasons.push(propertyCount >= 2 ? "individual_multi_asset" : "individual_single_asset");
  } else if (propertyCount === 1) {
    icpSegment = "spv_single_asset";
    reasons.push("single_property_default");
  } else {
    icpSegment = "asset_owner";
    reasons.push("default_asset_owner");
  }

  const strikeEligibleSegment = VAL_GTM_ICP_STRIKE_ELIGIBLE.includes(icpSegment);
  const strikeTier = priorityTier === "A" || priorityTier === "B";
  const strikePortfolio = calaPropertyCount >= 3;

  const strikeList =
    strikeEligibleSegment &&
    strikeTier &&
    strikePortfolio &&
    hasReachableContact &&
    icpSegment !== "franchisor_brand" &&
    icpSegment !== "broker_advisor" &&
    icpSegment !== "skip";

  if (strikeList) {
    reasons.push("strike_list_qualified");
  } else {
    if (!strikeEligibleSegment) reasons.push("strike_blocked_icp_segment");
    if (!strikeTier) reasons.push("strike_blocked_tier");
    if (!strikePortfolio) reasons.push("strike_blocked_cala_portfolio");
    if (!hasReachableContact) reasons.push("strike_blocked_no_contact");
  }

  const dealTrigger = inferOwnerDealTrigger(properties, {
    existingDealTrigger: input.existingDealTrigger,
    developmentPipelineCount: input.developmentPipelineCount,
    ownerName,
  });

  return {
    ownerName,
    ownerKey,
    icpSegment,
    strikeList,
    dealTrigger,
    calaPropertyCount,
    propertyCount,
    priorityTier,
    hasVerifiedContact,
    hasReachableContact,
    icpClassificationNotes: reasons.join("; "),
    strikeBlockers: strikeList
      ? []
      : reasons.filter((r) => r.startsWith("strike_blocked_") || r === "franchisor_or_brand_platform"),
  };
}

export {
  isVerifiedOwnerContact,
  isCostarVerifiedOwnerContact,
  isRegistryVerifiedOwnerContact,
} from "./registry-contact-verification.js";
