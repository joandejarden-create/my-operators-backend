/**
 * Permanent: Loyalty at a Glance — four meaningful KPI cards.
 *
 * Distinct from:
 *  - loyalty.proof (exact 6 Key Benefits)
 *  - loyalty.elite (actual official tier count)
 *
 * Contract: BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1
 * Lesson: BE-QL-LOYALTY-GLANCE-KPI-001
 *
 * Founder policy: Typical Loyalty Mix may be a clearly labeled
 * evidence-informed estimate when an actual is unavailable.
 */
import {
  resolveLoyaltyProgramId,
  getCanonicalLoyaltyProgram,
} from "./be-factory-permanent-loyalty-tier-completeness-v1.js";

export const BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1 =
  "BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1";

export const LOYALTY_GLANCE_LESSON_ID = "BE-QL-LOYALTY-GLANCE-KPI-001";

export const LOYALTY_KPI_SLOTS = Object.freeze([
  "loyalty.kpi.members",
  "loyalty.kpi.hotels",
  "loyalty.kpi.markets",
  "loyalty.kpi.mix",
]);

export const LOYALTY_GLANCE_FAIL = Object.freeze({
  MISSING: "LOYALTY_GLANCE_KPI_MISSING",
  ALL_NA: "LOYALTY_GLANCE_KPI_ALL_NA",
  UNSUPPORTED: "LOYALTY_GLANCE_KPI_UNSUPPORTED",
  WRONG_SCOPE: "LOYALTY_GLANCE_KPI_WRONG_SCOPE",
  STALE: "LOYALTY_GLANCE_KPI_STALE",
  MISLABELED: "LOYALTY_GLANCE_KPI_MISLABELED",
  DUPLICATE: "LOYALTY_GLANCE_KPI_DUPLICATE",
  CONTENT_REGRESSION: "LOYALTY_GLANCE_KPI_CONTENT_REGRESSION",
  PROGRAM_MISMATCH: "LOYALTY_GLANCE_PROGRAM_MISMATCH",
});

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function isNaValue(v) {
  return /^(n\/?a|not disclosed|unavailable|—|-|null|undefined)?$/i.test(nz(v));
}

function isMeaningfulKpiValue(v) {
  const t = nz(v);
  if (!t || isNaValue(t)) return false;
  // Reject diligence prose
  if (t.length > 64 || t.split(/\s+/).length > 10) return false;
  // Approved compact alternatives when exact headcount is undisclosed
  if (/^(growing|see footprint)$/i.test(t)) return true;
  // Must contain a digit or % or + scale marker
  if (!/[\d%]/.test(t)) return false;
  return true;
}

/**
 * Program-level KPI intelligence — research once, reuse across brands.
 * Values are program-wide unless noted; always label scope in kpiNote.
 * verifiedAt / asOf are observation dates for the cited figures.
 */
export const CANONICAL_LOYALTY_PROGRAM_KPIS_V1 = Object.freeze({
  marriott_bonvoy: Object.freeze({
    programId: "marriott_bonvoy",
    programName: "Marriott Bonvoy",
    officialUrl: "https://www.marriott.com/loyalty.mi",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "~271M",
      unit: "members",
      scope: "program-wide",
      asOf: "2025-12-31",
      source: "Marriott 2025 Annual Report",
      sourceUrl: "https://marriott.gcs-web.com/",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "9,800+",
      unit: "properties",
      scope: "participating Marriott portfolio (approx.)",
      asOf: "2025-12-31",
      source: "Marriott 2025 Annual Report",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "145",
      unit: "countries/territories",
      scope: "program / portfolio footprint",
      asOf: "2025-12-31",
      source: "Marriott 2025 Annual Report",
      kind: "verified",
    }),
    mix: Object.freeze({
      value: "~68%",
      unit: "of global room nights",
      scope: "Bonvoy member share — program-wide global",
      asOf: "2025-12-31",
      source: "Marriott 2025 Annual Report (member stays 68% global / 75% U.S. & Canada)",
      kind: "verified_program",
      label: "Typical Loyalty Mix",
    }),
  }),
  hilton_honors: Object.freeze({
    programId: "hilton_honors",
    programName: "Hilton Honors",
    officialUrl: "https://www.hilton.com/en/hilton-honors/",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "235M+",
      unit: "members",
      scope: "program-wide",
      asOf: "2025-10",
      source: "Hilton Q3 2025 Fact Sheet",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "9,000+",
      unit: "properties",
      scope: "Hilton system (incl. timeshare/strategic partners as disclosed)",
      asOf: "2025-10",
      source: "Hilton Q3 2025 Fact Sheet",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "141",
      unit: "countries/territories",
      scope: "Hilton system footprint",
      asOf: "2025-10",
      source: "Hilton Q3 2025 Fact Sheet",
      kind: "verified",
    }),
    mix: Object.freeze({
      value: null,
      kind: "estimate_required",
      label: "Estimated Loyalty Mix",
    }),
  }),
  world_of_hyatt: Object.freeze({
    programId: "world_of_hyatt",
    programName: "World of Hyatt",
    officialUrl: "https://world.hyatt.com/",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "~63M",
      unit: "members",
      scope: "program-wide",
      asOf: "2025-12-31",
      source: "Hyatt FY2025 disclosures (approx.)",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "1,300+",
      unit: "properties",
      scope: "Hyatt system (directional)",
      asOf: "2025",
      source: "Hyatt public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "70+",
      unit: "countries",
      scope: "Hyatt operating footprint (directional)",
      asOf: "2025",
      source: "Hyatt public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  ihg_one_rewards: Object.freeze({
    programId: "ihg_one_rewards",
    programName: "IHG One Rewards",
    officialUrl: "https://www.ihg.com/rewardsclub/content/us/en/home",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "160M+",
      unit: "members",
      scope: "program-wide",
      asOf: "2025-12-31",
      source: "IHG 2025 Form 20-F / public disclosures",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "6,000+",
      unit: "hotels",
      scope: "IHG system (directional)",
      asOf: "2025",
      source: "IHG public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "100+",
      unit: "countries",
      scope: "IHG footprint (directional)",
      asOf: "2025",
      source: "IHG public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  all_accor: Object.freeze({
    programId: "all_accor",
    programName: "ALL - Accor Live Limitless",
    officialUrl: "https://all.accor.com/",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "100M",
      unit: "members",
      scope: "program-wide",
      asOf: "2025",
      source: "Accor Overview 2025",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "5,700+",
      unit: "affiliated hotels",
      scope: "Accor affiliated hotels",
      asOf: "2025",
      source: "Accor Overview 2025",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "110+",
      unit: "countries",
      scope: "Accor footprint (directional)",
      asOf: "2025",
      source: "Accor public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  wyndham_rewards: Object.freeze({
    programId: "wyndham_rewards",
    programName: "Wyndham Rewards",
    officialUrl: "https://www.wyndhamhotels.com/wyndham-rewards",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "122M+",
      unit: "enrolled members",
      scope: "program-wide",
      asOf: "2025-12-31",
      source: "Wyndham Hotels & Resorts 2025 Annual Report",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "8,300+",
      unit: "properties",
      scope: "Wyndham system YE2025",
      asOf: "2025-12-31",
      source: "Wyndham 2025 Annual Report",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "95+",
      unit: "countries",
      scope: "Wyndham footprint (directional)",
      asOf: "2025",
      source: "Wyndham public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({
      value: "~37%",
      unit: "of global check-ins",
      scope: "Wyndham Rewards member check-ins — program-wide global (~53% U.S.)",
      asOf: "2025-12-31",
      source: "Wyndham 2025 Annual Report",
      kind: "verified_program",
      label: "Typical Loyalty Mix",
    }),
  }),
  choice_privileges: Object.freeze({
    programId: "choice_privileges",
    programName: "Choice Privileges",
    officialUrl: "https://www.choicehotels.com/choice-privileges",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "74M+",
      unit: "members",
      scope: "program-wide",
      asOf: "2025-12-31",
      source: "Choice Hotels FY2025 disclosures",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "7,100+",
      unit: "properties",
      scope: "Choice Privileges network (directional)",
      asOf: "2025",
      source: "Choice Hotels public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "40+",
      unit: "countries",
      scope: "Choice footprint (directional)",
      asOf: "2025",
      source: "Choice Hotels public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  best_western_rewards: Object.freeze({
    programId: "best_western_rewards",
    programName: "Best Western Rewards",
    officialUrl: "https://www.bestwestern.com/en_US/book/hotel-rewards.html",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "64M",
      unit: "members",
      scope: "Best Western Rewards program-wide",
      asOf: "2025-10",
      source: "BWH Hotels International Convention press release (Oct 2025)",
      sourceUrl:
        "https://www.bestwestern.com/en_US/about/press-media/2025-press-releases/bwh-hotels-international-convention.html",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "4,300+",
      unit: "hotels",
      scope: "BWH Hotels network (Best Western / WorldHotels / SureStay family; approx., may include pipeline)",
      asOf: "2025-10",
      source: "BWH Hotels International Convention press release (Oct 2025)",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "100+",
      unit: "countries/territories",
      scope: "BWH Hotels network footprint",
      asOf: "2025-10",
      source: "BWH Hotels International Convention press release (Oct 2025)",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  sonesta_travel_pass: Object.freeze({
    programId: "sonesta_travel_pass",
    programName: "Sonesta Travel Pass",
    officialUrl: "https://www.sonesta.com/sonesta-travel-pass",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "Growing",
      unit: "members",
      scope: "program-wide — exact headcount not consistently disclosed",
      asOf: "2025",
      source: "Sonesta public materials",
      kind: "alternative",
      label: "Program Scale",
      displayValue: "Growing",
      altNote: "Exact member count not consistently published — use hotels/markets/mix cards for owner metrics.",
    }),
    hotels: Object.freeze({
      value: "1,200+",
      unit: "properties",
      scope: "Sonesta system (directional)",
      asOf: "2025",
      source: "Sonesta public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "10+",
      unit: "countries",
      scope: "Sonesta footprint (directional)",
      asOf: "2025",
      source: "Sonesta public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  gha_discovery: Object.freeze({
    programId: "gha_discovery",
    programName: "GHA DISCOVERY",
    officialUrl: "https://www.ghadiscovery.com/",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "20M+",
      unit: "members",
      scope: "GHA DISCOVERY program-wide (directional)",
      asOf: "2024-2025",
      source: "GHA / member-brand public materials",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "500+",
      unit: "hotels",
      scope: "participating GHA hotels (directional)",
      asOf: "2025",
      source: "GHA DISCOVERY public materials",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "80+",
      unit: "countries",
      scope: "GHA participating brands footprint (directional)",
      asOf: "2025",
      source: "GHA public materials",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  melia_rewards: Object.freeze({
    programId: "melia_rewards",
    programName: "MeliáRewards",
    officialUrl: "https://www.melia.com/en/meliarewards",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "15M+",
      unit: "members",
      scope: "program-wide (directional)",
      asOf: "2024-2025",
      source: "Meliá public materials",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "350+",
      unit: "hotels",
      scope: "Meliá portfolio (directional)",
      asOf: "2025",
      source: "Meliá public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "40+",
      unit: "countries",
      scope: "Meliá footprint (directional)",
      asOf: "2025",
      source: "Meliá public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  my_barcelo: Object.freeze({
    programId: "my_barcelo",
    programName: "my Barceló Benefits",
    officialUrl: "https://www.barcelo.com/en-us/mybarcelo/",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "Growing",
      displayValue: "Growing",
      label: "Program Membership",
      unit: "members",
      scope: "my Barceló Benefits — exact headcount not consistently disclosed",
      asOf: "2025",
      source: "Barceló public materials",
      kind: "alternative",
    }),
    hotels: Object.freeze({
      value: "280+",
      unit: "hotels",
      scope: "Barceló portfolio (directional)",
      asOf: "2025",
      source: "Barceló public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "20+",
      unit: "countries",
      scope: "Barceló footprint (directional)",
      asOf: "2025",
      source: "Barceló public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  rotana_rewards: Object.freeze({
    programId: "rotana_rewards",
    programName: "Rotana Rewards",
    officialUrl: "https://www.rotana.com/rotanarewards",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "Growing",
      displayValue: "Growing",
      label: "Program Membership",
      kind: "alternative",
      scope: "exact headcount not consistently disclosed",
      asOf: "2025",
      source: "Rotana public materials",
    }),
    hotels: Object.freeze({
      value: "60+",
      unit: "hotels",
      scope: "Rotana portfolio (directional)",
      asOf: "2025",
      source: "Rotana public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "15+",
      unit: "countries",
      scope: "Rotana footprint (directional)",
      asOf: "2025",
      source: "Rotana public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  ascott_star_rewards: Object.freeze({
    programId: "ascott_star_rewards",
    programName: "Ascott Star Rewards",
    officialUrl: "https://www.the-ascott.com/en/ascott-star-rewards.html",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "3M+",
      unit: "members",
      scope: "program-wide (directional)",
      asOf: "2024-2025",
      source: "Ascott / CapitaLand public materials",
      kind: "verified",
    }),
    hotels: Object.freeze({
      value: "800+",
      unit: "properties",
      scope: "Ascott portfolio (directional)",
      asOf: "2025",
      source: "Ascott public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "30+",
      unit: "countries",
      scope: "Ascott footprint (directional)",
      asOf: "2025",
      source: "Ascott public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  golden_circle: Object.freeze({
    programId: "golden_circle",
    programName: "Golden Circle",
    officialUrl: "https://www.shangri-la.com/corporate/golden-circle/",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "Growing",
      displayValue: "Growing",
      label: "Program Membership",
      kind: "alternative",
      scope: "exact headcount not consistently disclosed",
      asOf: "2025",
      source: "Shangri-La public materials",
    }),
    hotels: Object.freeze({
      value: "100+",
      unit: "hotels",
      scope: "Shangri-La Group (directional)",
      asOf: "2025",
      source: "Shangri-La public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "25+",
      unit: "countries",
      scope: "Shangri-La footprint (directional)",
      asOf: "2025",
      source: "Shangri-La public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  dusit_gold: Object.freeze({
    programId: "dusit_gold",
    programName: "Dusit Gold",
    officialUrl: "https://www.dusit.com/dusitgold/",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "Growing",
      displayValue: "Growing",
      label: "Program Membership",
      kind: "alternative",
      asOf: "2025",
      source: "Dusit public materials",
    }),
    hotels: Object.freeze({
      value: "300+",
      unit: "properties",
      scope: "Dusit portfolio (directional)",
      asOf: "2025",
      source: "Dusit public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "15+",
      unit: "countries",
      scope: "Dusit footprint (directional)",
      asOf: "2025",
      source: "Dusit public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  pan_pacific_loyalty: Object.freeze({
    programId: "pan_pacific_loyalty",
    programName: "Pan Pacific Hotels Group Loyalty",
    officialUrl: "https://www.panpacific.com/",
    verifiedAt: "2026-11-20",
    members: Object.freeze({
      value: "Growing",
      displayValue: "Growing",
      label: "Program Membership",
      kind: "alternative",
      asOf: "2025",
      source: "Pan Pacific public materials",
    }),
    hotels: Object.freeze({
      value: "50+",
      unit: "hotels",
      scope: "PPHG portfolio (directional)",
      asOf: "2025",
      source: "Pan Pacific public disclosures",
      kind: "verified",
    }),
    markets: Object.freeze({
      value: "10+",
      unit: "countries",
      scope: "PPHG footprint (directional)",
      asOf: "2025",
      source: "Pan Pacific public disclosures",
      kind: "verified",
    }),
    mix: Object.freeze({ kind: "estimate_required", label: "Estimated Loyalty Mix", value: null }),
  }),
  non_tiered_network: Object.freeze({
    programId: "non_tiered_network",
    programName: "Membership network (non-tiered)",
    nonMeasurableLoyalty: true,
    verifiedAt: "2026-11-20",
    members: null,
    hotels: null,
    markets: null,
    mix: null,
  }),
});

/**
 * Brand positioning → estimated loyalty mix range (% of occupied room nights).
 * Reproducible framework — not arbitrary fixed % for all brands.
 */
export const LOYALTY_MIX_ESTIMATE_BANDS_V1 = Object.freeze({
  luxury: { low: 45, high: 60, central: 52, confidence: "Medium" },
  upper_upscale: { low: 40, high: 55, central: 47, confidence: "Medium" },
  upscale: { low: 35, high: 50, central: 42, confidence: "Medium" },
  upper_midscale: { low: 28, high: 42, central: 35, confidence: "Medium" },
  midscale: { low: 22, high: 38, central: 30, confidence: "Medium" },
  economy: { low: 18, high: 32, central: 25, confidence: "Low" },
  soft_brand: { low: 30, high: 48, central: 38, confidence: "Medium" },
  extended_stay: { low: 32, high: 48, central: 40, confidence: "Medium" },
  lifestyle: { low: 28, high: 45, central: 36, confidence: "Medium" },
  all_inclusive: { low: 20, high: 38, central: 28, confidence: "Low" },
  default: { low: 25, high: 40, central: 32, confidence: "Low" },
});

const BRAND_BAND_HINTS = [
  [/waldorf|ritz|st\.?\s*regis|four seasons|park hyatt|grand hyatt|conrad|mandarin|shangri|luxury|collection by|edition|w hotels|thompson|andaz|kimpton|curio|autograph|tribute|design hotels|unbound|caption|vignette|voco|swissôtel|fairmont|raffles|sofitel|m gallery|pullman/i, "luxury"],
  [/residence inn|homewood|home2|towneplace|element|extended|staybridge|candlewood|mainstay|suburban|woodspring|sonesta es|extended stay/i, "extended_stay"],
  [/aloft|moxy|motto|tempo|tru|even|hyatt place|hyatt house|courtyard|four points|springhill|fairfield|ac hotels|cambria|clarion pointe|avid|spark|garden inn|hampton|holiday inn express|crowne plaza|indigo|holiday inn(?! club)|doubletree|embassy|hilton garden/i, "upscale"],
  [/surestay|super 8|days inn|travelodge|econo|rodeway|americas best|microtel|wingate|baymont|la quinta|quality inn|comfort inn|sleep inn|days inn|motel/i, "economy"],
  [/best western(?! premier| plus)|bw signature|bw premier|bw plus|quality|clarion(?! pointe)|country inn|radisson|park inn|ascend/i, "midscale"],
  [/all.?inclusive|secrets|dreams|sunscape|zo[eë]try|breathless|hyatt ziva|hyatt zilara|wyndham alltra|occidental|barcel[oó]|meli[aá] (?!collection)/i, "all_inclusive"],
  [/25hours|morgans|mondrian|sbe|lifestyle|tribe|jo.?t|mama shelter|the hoxton|citizenm/i, "lifestyle"],
  [/tapestry|autograph|curio|tribute|collection|design hotels|unbound|ascend|preferred|slh/i, "soft_brand"],
];

export function inferLoyaltyMixBand({ brandName, positioning, chainScale } = {}) {
  const blob = `${nz(brandName)} ${nz(positioning)} ${nz(chainScale)}`;
  for (const [re, band] of BRAND_BAND_HINTS) {
    if (re.test(blob)) return band;
  }
  const cs = nz(chainScale).toLowerCase();
  if (/luxury/.test(cs)) return "luxury";
  if (/upper.?upscale|upper upscale/.test(cs)) return "upper_upscale";
  if (/upscale/.test(cs)) return "upscale";
  if (/upper.?mid/.test(cs)) return "upper_midscale";
  if (/midscale|mid-scale/.test(cs)) return "midscale";
  if (/economy|budget/.test(cs)) return "economy";
  return "default";
}

/**
 * Evidence-informed Estimated Loyalty Mix when no verified actual exists.
 */
export function estimateLoyaltyMix(input = {}) {
  const programId = input.programId || null;
  const progKpi = programId ? CANONICAL_LOYALTY_PROGRAM_KPIS_V1[programId] : null;

  // Hierarchy 1–2: verified brand or program actual
  if (progKpi?.mix?.kind === "verified_program" && progKpi.mix.value) {
    return {
      label: progKpi.mix.label || "Typical Loyalty Mix",
      value: progKpi.mix.value,
      displayValue: progKpi.mix.value,
      kind: "verified_program",
      confidence: "High",
      kpiNote: `${progKpi.mix.scope} · as of ${progKpi.mix.asOf}. ${progKpi.mix.source}`,
      asOf: progKpi.mix.asOf,
      methodology: "verified_program_disclosure",
      band: null,
      assumptions: ["Uses published program-wide member share of room nights/check-ins."],
    };
  }

  const bandKey = inferLoyaltyMixBand(input);
  const band = LOYALTY_MIX_ESTIMATE_BANDS_V1[bandKey] || LOYALTY_MIX_ESTIMATE_BANDS_V1.default;

  // Program-strength adjustments
  let low = band.low;
  let high = band.high;
  let central = band.central;
  const assumptions = [
    `Chain/positioning band: ${bandKey}`,
    `Base range ${low}–${high}% from comparable-hotel loyalty contribution benchmarks`,
  ];

  const largePrograms = new Set([
    "marriott_bonvoy",
    "hilton_honors",
    "ihg_one_rewards",
    "wyndham_rewards",
    "choice_privileges",
    "all_accor",
  ]);
  if (largePrograms.has(programId)) {
    low += 3;
    high += 3;
    central += 3;
    assumptions.push("Adjusted +3 pts for large global loyalty program reach");
  }
  if (programId === "best_western_rewards" || programId === "sonesta_travel_pass") {
    // mid-scale network — keep band, slight downward for economy family brands
    if (bandKey === "economy") {
      assumptions.push("Economy BWR/Sonesta family — keep lower mid of band");
    }
  }

  // Clamp
  low = Math.max(12, Math.min(low, 70));
  high = Math.max(low + 8, Math.min(high, 75));
  central = Math.round((low + high) / 2);

  const displayValue = `${low}–${high}%`;
  return {
    label: "Estimated Loyalty Mix",
    value: displayValue,
    displayValue,
    kind: "estimated",
    confidence: band.confidence,
    kpiNote: `Estimated share of occupied room nights from loyalty members. Dealality estimate based on brand positioning (${bandKey}), loyalty-program reach and comparable hotel benchmarks.`,
    asOf: new Date().toISOString().slice(0, 10),
    methodology: "dealality_loyalty_mix_estimate_v1",
    band: bandKey,
    centralEstimate: central,
    assumptions,
    calculatedAt: new Date().toISOString(),
  };
}

export function getProgramKpis(programId) {
  if (!programId) return null;
  return CANONICAL_LOYALTY_PROGRAM_KPIS_V1[programId] || null;
}

/**
 * Build the four loyalty.kpi.* presentation blocks for a brand/program.
 */
export function buildLoyaltyGlanceKpiBlocks(opts = {}) {
  const programId =
    opts.programId ||
    resolveLoyaltyProgramId({
      loyaltyProgram: opts.loyaltyProgram,
      parentCompany: opts.parentCompany,
      brandName: opts.brandName,
    });
  const prog = getProgramKpis(programId);
  const canon = getCanonicalLoyaltyProgram(programId);
  const sortStart = opts.sortStart != null ? opts.sortStart : 3200;

  if (!programId || programId === "none" || prog?.nonMeasurableLoyalty) {
    return {
      programId,
      blocks: [
        {
          slotKey: "loyalty.kpi.members",
          title: "Membership Network",
          body: "N/A",
          kpiNote: "Non-tiered / non-measurable loyalty network — numerical membership KPIs not applicable.",
          sort: sortStart,
          active: true,
          loyaltyKpiKind: "not_applicable",
        },
        {
          slotKey: "loyalty.kpi.hotels",
          title: "Network Hotels",
          body: "See Footprint",
          kpiNote: "Use Brand Footprint for property counts — not a franchise loyalty KPI.",
          sort: sortStart + 1,
          active: true,
          loyaltyKpiKind: "not_applicable",
        },
        {
          slotKey: "loyalty.kpi.markets",
          title: "Markets",
          body: "See Footprint",
          kpiNote: "Use Brand Footprint regional coverage.",
          sort: sortStart + 2,
          active: true,
          loyaltyKpiKind: "not_applicable",
        },
        {
          slotKey: "loyalty.kpi.mix",
          title: "Loyalty Mix",
          body: "N/A",
          kpiNote: "No conventional loyalty room-night mix for this network type.",
          sort: sortStart + 3,
          active: true,
          loyaltyKpiKind: "not_applicable",
        },
      ],
      classification: "KPI_NOT_APPLICABLE",
      ok: true,
      notApplicable: true,
    };
  }

  if (!prog) {
    return {
      programId,
      blocks: [],
      classification: "KPI_NEEDS_REMEDIATION",
      ok: false,
      flags: [LOYALTY_GLANCE_FAIL.MISSING, LOYALTY_GLANCE_FAIL.PROGRAM_MISMATCH],
    };
  }

  const mixEst = estimateLoyaltyMix({
    programId,
    brandName: opts.brandName,
    positioning: opts.positioning,
    chainScale: opts.chainScale,
  });

  function card(slotKey, title, metric, sort, mixOverride) {
    const m = mixOverride || metric;
    if (!m) {
      return {
        slotKey,
        title,
        body: "N/A",
        kpiNote: "Verified figure not available",
        sort,
        active: true,
        loyaltyKpiKind: "missing",
      };
    }
    const value = m.displayValue || m.value || "N/A";
    const label = m.label || title;
    const noteParts = [];
    if (m.kpiNote) noteParts.push(m.kpiNote);
    else {
      if (m.scope) noteParts.push(m.scope);
      if (m.asOf) noteParts.push(`as of ${m.asOf}`);
      if (m.source) noteParts.push(m.source);
    }
    return {
      slotKey,
      title: label,
      body: String(value),
      kpiNote: noteParts.join(" · "),
      sort,
      active: true,
      source: m.sourceUrl || prog.officialUrl || canon?.officialUrl || null,
      loyaltyKpiKind: m.kind || "verified",
      loyaltyKpiAsOf: m.asOf || null,
      loyaltyProgramId: programId,
      loyaltyMixConfidence: m.confidence || null,
      loyaltyMixMethodology: m.methodology || null,
    };
  }

  const membersMetric =
    prog.members?.kind === "alternative"
      ? {
          ...prog.members,
          value: prog.members.displayValue || prog.members.value,
          label: prog.members.label || "Members",
        }
      : prog.members;

  const blocks = [
    card("loyalty.kpi.members", "Members", membersMetric, sortStart),
    card("loyalty.kpi.hotels", "Participating Hotels", prog.hotels, sortStart + 1),
    card("loyalty.kpi.markets", "Markets", prog.markets, sortStart + 2),
    card("loyalty.kpi.mix", mixEst.label, null, sortStart + 3, mixEst),
  ];

  const verifiedCount = blocks.filter((b) => isMeaningfulKpiValue(b.body)).length;
  const classification =
    verifiedCount === 4
      ? "KPI_VERIFIED"
      : verifiedCount >= 2
        ? "KPI_PARTIALLY_VERIFIED"
        : verifiedCount === 0
          ? "KPI_MISSING"
          : "KPI_PARTIALLY_VERIFIED";

  return {
    programId,
    programName: prog.programName,
    blocks,
    verifiedCount,
    classification,
    ok: verifiedCount === 4,
    mix: mixEst,
  };
}

function kpiBlocksFrom(blocks) {
  const map = {};
  for (const sk of LOYALTY_KPI_SLOTS) map[sk] = null;
  for (const b of Array.isArray(blocks) ? blocks : []) {
    const sk = nz(b.slotKey || b.slot_key);
    if (LOYALTY_KPI_SLOTS.includes(sk)) map[sk] = b;
  }
  return map;
}

/**
 * Validate loyalty glance KPI set.
 */
export function validateLoyaltyGlanceKpis(blocks, opts = {}) {
  const map = kpiBlocksFrom(blocks);
  const present = LOYALTY_KPI_SLOTS.map((sk) => map[sk]).filter(Boolean);
  const flags = [];
  const reasons = [];

  if (opts.participates === false || opts.notApplicable === true) {
    return {
      ok: true,
      classification: "KPI_NOT_APPLICABLE",
      verifiedCount: 0,
      presentCount: present.length,
      flags: [],
      blocked: false,
      reasons: ["not_applicable"],
    };
  }

  if (present.length === 0) {
    flags.push(LOYALTY_GLANCE_FAIL.MISSING);
    reasons.push("no_loyalty_kpi_slots");
  } else if (present.length < 4) {
    flags.push(LOYALTY_GLANCE_FAIL.MISSING);
    reasons.push(`kpi_slots_${present.length}_of_4`);
  }

  const values = LOYALTY_KPI_SLOTS.map((sk) => (map[sk] ? nz(map[sk].body) : ""));
  const meaningful = values.filter(isMeaningfulKpiValue);
  const naCount = values.filter(isNaValue).length;

  if (present.length >= 4 && meaningful.length === 0) {
    flags.push(LOYALTY_GLANCE_FAIL.ALL_NA);
    reasons.push("all_four_kpi_na");
  }

  // Duplicate identical meaningful values across cards
  const seen = new Set();
  for (const v of meaningful) {
    const key = v.toLowerCase();
    if (seen.has(key)) {
      flags.push(LOYALTY_GLANCE_FAIL.DUPLICATE);
      reasons.push("duplicate_kpi_values");
      break;
    }
    seen.add(key);
  }

  const verifiedCount = meaningful.length;
  let classification = "KPI_MISSING";
  if (verifiedCount === 4) classification = "KPI_VERIFIED";
  else if (verifiedCount === 3) classification = "KPI_PARTIALLY_VERIFIED";
  else if (verifiedCount === 2) classification = "KPI_PARTIALLY_VERIFIED";
  else if (verifiedCount === 1) classification = "KPI_PARTIALLY_VERIFIED";
  else if (naCount >= 4 || present.length === 0) classification = "KPI_MISSING";

  if (flags.includes(LOYALTY_GLANCE_FAIL.ALL_NA)) classification = "KPI_MISSING";

  const ok = verifiedCount === 4 && !flags.includes(LOYALTY_GLANCE_FAIL.ALL_NA);
  return {
    ok,
    classification,
    verifiedCount,
    presentCount: present.length,
    naCount,
    flags: [...new Set(flags)],
    reasons,
    blocked: !ok && opts.blockOnFail !== false,
    valuesBySlot: Object.fromEntries(
      LOYALTY_KPI_SLOTS.map((sk) => [
        sk,
        map[sk]
          ? { title: nz(map[sk].title), body: nz(map[sk].body), kpiNote: nz(map[sk].kpiNote) }
          : null,
      ])
    ),
  };
}

/**
 * Assembler: never replace meaningful CURRENT KPIs with all-N/A or empty candidate.
 */
export function decideLoyaltyGlanceKpiCandidateApply(currentBlocks, candidateBlocks, opts = {}) {
  const cur = validateLoyaltyGlanceKpis(currentBlocks, opts);
  const cand = validateLoyaltyGlanceKpis(candidateBlocks, opts);

  const candMap = kpiBlocksFrom(candidateBlocks);
  const candPresent = LOYALTY_KPI_SLOTS.filter((sk) => candMap[sk]).length;

  if (candPresent === 0) {
    return {
      applyCandidate: false,
      reason: "loyalty_glance_no_candidate",
      retainCurrent: cur.verifiedCount > 0,
      flags: [],
      candidate: cand,
      current: cur,
    };
  }

  if (cand.flags.includes(LOYALTY_GLANCE_FAIL.ALL_NA) && cur.verifiedCount > 0) {
    return {
      applyCandidate: false,
      reason: LOYALTY_GLANCE_FAIL.CONTENT_REGRESSION,
      retainCurrent: true,
      flags: [LOYALTY_GLANCE_FAIL.CONTENT_REGRESSION, LOYALTY_GLANCE_FAIL.ALL_NA],
      candidate: cand,
      current: cur,
    };
  }

  if (cand.verifiedCount < cur.verifiedCount && cur.verifiedCount >= 2) {
    return {
      applyCandidate: false,
      reason: LOYALTY_GLANCE_FAIL.CONTENT_REGRESSION,
      retainCurrent: true,
      flags: [LOYALTY_GLANCE_FAIL.CONTENT_REGRESSION],
      candidate: cand,
      current: cur,
    };
  }

  if (cand.ok) {
    return {
      applyCandidate: true,
      reason: "loyalty_glance_kpi_verified",
      retainCurrent: false,
      flags: [],
      candidate: cand,
      current: cur,
      replacementBlocks: LOYALTY_KPI_SLOTS.map((sk) => candMap[sk]).filter(Boolean),
    };
  }

  if (cand.verifiedCount >= 2 && cand.verifiedCount >= cur.verifiedCount) {
    return {
      applyCandidate: true,
      reason: "loyalty_glance_kpi_partial_improvement",
      retainCurrent: false,
      flags: cand.flags,
      candidate: cand,
      current: cur,
      replacementBlocks: LOYALTY_KPI_SLOTS.map((sk) => candMap[sk]).filter(Boolean),
    };
  }

  return {
    applyCandidate: false,
    reason: cand.flags[0] || LOYALTY_GLANCE_FAIL.MISSING,
    retainCurrent: cur.presentCount > 0,
    flags: cand.flags,
    candidate: cand,
    current: cur,
  };
}

export function isMeaningfulLoyaltyKpiValue(v) {
  return isMeaningfulKpiValue(v);
}

export { resolveLoyaltyProgramId };
