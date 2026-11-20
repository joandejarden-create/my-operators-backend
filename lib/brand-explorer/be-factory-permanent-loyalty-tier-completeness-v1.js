/**
 * Permanent: Loyalty Elite Tiers — Illustrative Benefits completeness.
 *
 * Distinct from Key Benefits & Program Strengths (loyalty.proof = exact 6).
 * Elite Tiers (loyalty.elite) must show ALL applicable official membership /
 * elite tiers in correct order — never collapse four verified tiers into one
 * generic "Elite Recognition" card.
 *
 * Contract: BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1
 * Lesson: BE-QL-LOYALTY-TIER-COMPLETENESS-001
 */
export const BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1 =
  "BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1";

export const LOYALTY_ELITE_SLOT = "loyalty.elite";

export const LOYALTY_TIER_FAIL = Object.freeze({
  REGRESSION: "LOYALTY_TIER_REGRESSION",
  COLLAPSED: "LOYALTY_TIERS_COLLAPSED",
  INCOMPLETE: "LOYALTY_TIER_INCOMPLETE",
  BENEFIT_UNSUPPORTED: "LOYALTY_BENEFIT_UNSUPPORTED",
  PROGRAM_MISMATCH: "LOYALTY_PROGRAM_MISMATCH",
  PARTICIPATION_UNVERIFIED: "LOYALTY_PARTICIPATION_UNVERIFIED",
  STALE: "LOYALTY_TIER_STALE",
  GENERIC_CARD: "LOYALTY_TIER_GENERIC_PLACEHOLDER",
});

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function normTitle(s) {
  return nz(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Canonical parent loyalty programs — reusable across participating brands.
 * Benefits are illustrative / owner-facing and always property-qualified.
 * Verification: free public program pages; refresh when program rules change.
 */
export const CANONICAL_LOYALTY_PROGRAMS_V1 = Object.freeze({
  marriott_bonvoy: Object.freeze({
    id: "marriott_bonvoy",
    programName: "Marriott Bonvoy",
    officialUrl: "https://www.marriott.com/loyalty.mi",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.marriott.com/loyalty.mi", "https://www.marriott.com/loyalty/member-benefits.mi"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Member",
        order: 1,
        kind: "membership",
        benefits:
          "Base Bonvoy earn on qualifying stays; Member Rates on direct bookings; mobile check-in/key where offered. Benefits vary by hotel, rate, and market.",
      },
      {
        name: "Silver Elite",
        order: 2,
        kind: "elite",
        benefits:
          "Entry elite: enhanced earn versus Member, priority late checkout where offered, and recognition benefits per current Bonvoy rules. Confirm qualification nights/spend annually.",
      },
      {
        name: "Gold Elite",
        order: 3,
        kind: "elite",
        benefits:
          "Stronger room/upgrade preference and enhanced earn. Welcome amenities pattern and improved recognition vs Silver—delivery depends on property capability.",
      },
      {
        name: "Platinum Elite",
        order: 4,
        kind: "elite",
        benefits:
          "Upper elite: suite/room upgrade pathways and lounge access where the hotel offers a lounge. Material for suite inventory and arrival planning.",
      },
      {
        name: "Titanium Elite",
        order: 5,
        kind: "elite",
        benefits:
          "Near-top elite with elevated suite-night and upgrade economics. Model elite fulfillment cost against Bonvoy-driven demand.",
      },
      {
        name: "Ambassador Elite",
        order: 6,
        kind: "elite",
        benefits:
          "Top Bonvoy elite tier (qualification/invitation path per current Marriott Bonvoy terms). Highest service expectation—confirm published qualification before underwriting.",
      },
    ]),
  }),
  hilton_honors: Object.freeze({
    id: "hilton_honors",
    programName: "Hilton Honors",
    officialUrl: "https://www.hilton.com/en/hilton-honors/",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.hilton.com/en/hilton-honors/", "https://www.hilton.com/en/hilton-honors/member-benefits/"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Member",
        order: 1,
        kind: "membership",
        benefits:
          "Hilton Honors Points on eligible stays; Member Rates and digital tools on direct Hilton paths. Property-level delivery varies.",
      },
      {
        name: "Silver",
        order: 2,
        kind: "elite",
        benefits:
          "Entry elite: Fifth Night Free on standard room rewards (where program rules apply), Points pooling, and baseline recognition per current Honors rules.",
      },
      {
        name: "Gold",
        order: 3,
        kind: "elite",
        benefits:
          "Enhanced earn bonus, room upgrade preference where available, and stronger recognition versus Silver. Confirm current night/spend thresholds.",
      },
      {
        name: "Diamond",
        order: 4,
        kind: "elite",
        benefits:
          "Top mainstream elite: higher upgrade priority, executive lounge access where offered, and elevated service pathways. Not guaranteed at every hotel.",
      },
      {
        name: "Diamond Reserve",
        order: 5,
        kind: "elite",
        benefits:
          "Highest published Honors elite recognition (per current program rules). Confirm 2026+ qualification and benefit schedule on honors.hilton.com before underwriting.",
      },
    ]),
  }),
  world_of_hyatt: Object.freeze({
    id: "world_of_hyatt",
    programName: "World of Hyatt",
    officialUrl: "https://world.hyatt.com/",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://world.hyatt.com/", "https://world.hyatt.com/content/gp/en/benefits.html"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Member",
        order: 1,
        kind: "membership",
        benefits:
          "World of Hyatt Points on qualifying stays; member rates and digital experience on direct Hyatt paths where enabled.",
      },
      {
        name: "Discoverist",
        order: 2,
        kind: "elite",
        benefits:
          "Entry elite: bonus Points, late checkout where offered, and room upgrade preference per current World of Hyatt rules.",
      },
      {
        name: "Explorist",
        order: 3,
        kind: "elite",
        benefits:
          "Mid elite: higher bonus earn, stronger upgrade preference, and welcome amenities pattern where the hotel participates.",
      },
      {
        name: "Globalist",
        order: 4,
        kind: "elite",
        benefits:
          "Top elite: suite upgrades and Club lounge access where offered, breakfast benefits on qualifying rates, and elevated service expectations. Confirm current qualification.",
      },
    ]),
  }),
  ihg_one_rewards: Object.freeze({
    id: "ihg_one_rewards",
    programName: "IHG One Rewards",
    officialUrl: "https://www.ihg.com/rewardsclub/content/us/en/home",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.ihg.com/rewardsclub/content/us/en/home"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Club",
        order: 1,
        kind: "membership",
        benefits:
          "Base IHG One Rewards membership: earn toward Reward Nights, Member Rates on direct bookings. Benefits vary by brand and hotel.",
      },
      {
        name: "Silver Elite",
        order: 2,
        kind: "elite",
        benefits:
          "Entry elite: bonus points and recognition benefits per current IHG One Rewards rules. Confirm night/spend thresholds annually.",
      },
      {
        name: "Gold Elite",
        order: 3,
        kind: "elite",
        benefits:
          "Enhanced earn, room upgrades where available, and stronger recognition versus Silver. Property delivery varies.",
      },
      {
        name: "Platinum Elite",
        order: 4,
        kind: "elite",
        benefits:
          "Upper elite: higher upgrade priority and elite benefits including Milestone Rewards choices where program rules apply.",
      },
      {
        name: "Diamond Elite",
        order: 5,
        kind: "elite",
        benefits:
          "Top elite: highest published recognition, suite/upgrade pathways where offered, and dedicated support pathways per current program terms.",
      },
    ]),
  }),
  all_accor: Object.freeze({
    id: "all_accor",
    programName: "ALL - Accor Live Limitless",
    officialUrl: "https://all.accor.com/",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://all.accor.com/loyalty/earn/index.en.shtml"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Classic",
        order: 1,
        kind: "membership",
        benefits:
          "ALL Accor membership: earn Reward points on qualifying stays; member offers on Accor brands. Brand and hotel rules apply.",
      },
      {
        name: "Silver",
        order: 2,
        kind: "elite",
        benefits:
          "Entry status: enhanced earn and recognition benefits per current ALL Accor status rules.",
      },
      {
        name: "Gold",
        order: 3,
        kind: "elite",
        benefits:
          "Mid status: room upgrades where available, late checkout preference, and stronger recognition versus Silver.",
      },
      {
        name: "Platinum",
        order: 4,
        kind: "elite",
        benefits:
          "Upper status: suite/upgrade pathways and lounge access where offered. Confirm current qualification nights/stays.",
      },
      {
        name: "Diamond",
        order: 5,
        kind: "elite",
        benefits:
          "Top ALL Accor status: highest published recognition and service pathways. Delivery varies by Accor brand and hotel.",
      },
    ]),
  }),
  wyndham_rewards: Object.freeze({
    id: "wyndham_rewards",
    programName: "Wyndham Rewards",
    officialUrl: "https://www.wyndhamhotels.com/wyndham-rewards",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.wyndhamhotels.com/wyndham-rewards"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Blue",
        order: 1,
        kind: "membership",
        benefits:
          "Base Wyndham Rewards: earn points on qualifying stays; member rates on participating Wyndham brands.",
      },
      {
        name: "Gold",
        order: 2,
        kind: "elite",
        benefits:
          "Entry elite: bonus points and recognition benefits per current Wyndham Rewards rules.",
      },
      {
        name: "Platinum",
        order: 3,
        kind: "elite",
        benefits:
          "Mid elite: stronger earn and recognition versus Gold. Property-level delivery varies by brand.",
      },
      {
        name: "Diamond",
        order: 4,
        kind: "elite",
        benefits:
          "Top published Wyndham Rewards elite recognition. Confirm current qualification and benefit schedule before underwriting.",
      },
    ]),
  }),
  choice_privileges: Object.freeze({
    id: "choice_privileges",
    programName: "Choice Privileges",
    officialUrl: "https://www.choicehotels.com/choice-privileges",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.choicehotels.com/choice-privileges"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Member",
        order: 1,
        kind: "membership",
        benefits:
          "Choice Privileges membership: earn points on qualifying stays booked direct; member promotions where enabled.",
      },
      {
        name: "Gold",
        order: 2,
        kind: "elite",
        benefits:
          "Entry elite (nights or Elite Qualifying Credits per current rules): bonus points and recognition benefits.",
      },
      {
        name: "Platinum",
        order: 3,
        kind: "elite",
        benefits:
          "Higher bonus earn and stronger recognition versus Gold. Confirm current EQC/night thresholds.",
      },
      {
        name: "Diamond",
        order: 4,
        kind: "elite",
        benefits:
          "Upper elite recognition and elevated service benefits on participating Choice brands. Delivery varies by hotel.",
      },
      {
        name: "Titanium",
        order: 5,
        kind: "elite",
        benefits:
          "Highest published Choice Privileges tier with premium program benefits. Confirm current qualification path.",
      },
    ]),
  }),
  best_western_rewards: Object.freeze({
    id: "best_western_rewards",
    programName: "Best Western Rewards",
    officialUrl: "https://www.bestwestern.com/en_US/book/hotel-rewards.html",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.bestwestern.com/en_US/book/hotel-rewards.html"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Blue",
        order: 1,
        kind: "membership",
        benefits: "Base Best Western Rewards membership and member rates on participating hotels.",
      },
      {
        name: "Gold",
        order: 2,
        kind: "elite",
        benefits: "Enhanced earn and recognition benefits per current Best Western Rewards rules.",
      },
      {
        name: "Platinum",
        order: 3,
        kind: "elite",
        benefits: "Stronger recognition and earn versus Gold. Confirm current night thresholds.",
      },
      {
        name: "Diamond",
        order: 4,
        kind: "elite",
        benefits: "Top published Best Western Rewards status. Property delivery varies.",
      },
    ]),
  }),
  sonesta_travel_pass: Object.freeze({
    id: "sonesta_travel_pass",
    programName: "Sonesta Travel Pass",
    officialUrl: "https://www.sonesta.com/sonesta-travel-pass",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.sonesta.com/sonesta-travel-pass"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Member",
        order: 1,
        kind: "membership",
        benefits: "Sonesta Travel Pass membership: earn on qualifying stays; member offers where enabled.",
      },
      {
        name: "Silver",
        order: 2,
        kind: "elite",
        benefits: "Entry elite recognition and bonus earn per current Sonesta Travel Pass rules.",
      },
      {
        name: "Gold",
        order: 3,
        kind: "elite",
        benefits: "Mid elite: stronger recognition and earn versus Silver.",
      },
      {
        name: "Platinum",
        order: 4,
        kind: "elite",
        benefits: "Top published Sonesta Travel Pass elite recognition. Confirm current qualification.",
      },
    ]),
  }),
  gha_discovery: Object.freeze({
    id: "gha_discovery",
    programName: "GHA DISCOVERY",
    officialUrl: "https://www.ghadiscovery.com/",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.ghadiscovery.com/"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Discovery",
        order: 1,
        kind: "membership",
        benefits: "GHA DISCOVERY membership across participating Global Hotel Alliance brands.",
      },
      {
        name: "Gold",
        order: 2,
        kind: "elite",
        benefits: "Enhanced recognition and local experiences per GHA DISCOVERY status rules.",
      },
      {
        name: "Platinum",
        order: 3,
        kind: "elite",
        benefits: "Stronger recognition and experience benefits versus Gold.",
      },
      {
        name: "Titanium",
        order: 4,
        kind: "elite",
        benefits: "Upper GHA DISCOVERY status with elevated recognition across participating brands.",
      },
      {
        name: "Black",
        order: 5,
        kind: "elite",
        benefits: "Highest published GHA DISCOVERY recognition. Confirm current qualification path.",
      },
    ]),
  }),
  melia_rewards: Object.freeze({
    id: "melia_rewards",
    programName: "MeliáRewards",
    officialUrl: "https://www.melia.com/en/meliarewards",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.melia.com/en/meliarewards"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "MeliáRewards",
        order: 1,
        kind: "membership",
        benefits: "Base MeliáRewards membership on participating Meliá brands.",
      },
      {
        name: "Silver",
        order: 2,
        kind: "elite",
        benefits: "Entry elite: bonus earn and recognition per current MeliáRewards rules.",
      },
      {
        name: "Gold",
        order: 3,
        kind: "elite",
        benefits: "Mid elite recognition and enhanced benefits versus Silver.",
      },
      {
        name: "Platinum",
        order: 4,
        kind: "elite",
        benefits: "Top published MeliáRewards elite recognition. Confirm current qualification.",
      },
    ]),
  }),
  minor_discovery: Object.freeze({
    id: "minor_discovery",
    programName: "Minor DISCOVERY",
    officialUrl: "https://www.minorhotels.com/",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.minorhotels.com/"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Member",
        order: 1,
        kind: "membership",
        benefits: "Minor Hotels DISCOVERY / loyalty membership on participating Minor brands—confirm current program branding for the specific brand.",
      },
      {
        name: "Silver",
        order: 2,
        kind: "elite",
        benefits: "Entry status recognition per current Minor Hotels loyalty rules for participating brands.",
      },
      {
        name: "Gold",
        order: 3,
        kind: "elite",
        benefits: "Enhanced recognition versus Silver. Confirm brand-specific participation.",
      },
      {
        name: "Platinum",
        order: 4,
        kind: "elite",
        benefits: "Top published status for participating Minor Hotels brands. Verify current program name and tiers.",
      },
    ]),
  }),
  my_barcelo: Object.freeze({
    id: "my_barcelo",
    programName: "my Barceló Benefits",
    officialUrl: "https://www.barcelo.com/en-us/mybarcelo/",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.barcelo.com/en-us/mybarcelo/"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Essential",
        order: 1,
        kind: "membership",
        benefits:
          "Entry my Barceló Benefits level on signup: member rates and base recognition. Not a points program—benefits are stay/discount oriented and vary by region and hotel.",
      },
      {
        name: "Special",
        order: 2,
        kind: "elite",
        benefits:
          "Mid level after qualifying stays and spend within 24 months (per current Barceló rules). Stronger member discounts and in-stay attentions where offered.",
      },
      {
        name: "Unique",
        order: 3,
        kind: "elite",
        benefits:
          "Top published my Barceló Benefits level after higher stay/spend thresholds. Preferential room, welcome attentions, and flexible arrival/departure where hotel capability allows.",
      },
    ]),
  }),
  rotana_rewards: Object.freeze({
    id: "rotana_rewards",
    programName: "Rotana Rewards",
    officialUrl: "https://www.rotana.com/rotanarewards",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.rotana.com/rotanarewards"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Blue",
        order: 1,
        kind: "membership",
        benefits: "Base Rotana Rewards membership on participating Rotana brands. Confirm current earn/redeem rules on rotana.com.",
      },
      {
        name: "Silver",
        order: 2,
        kind: "elite",
        benefits: "Entry elite recognition and bonus benefits per current Rotana Rewards rules. Delivery varies by hotel.",
      },
      {
        name: "Gold",
        order: 3,
        kind: "elite",
        benefits: "Mid elite: enhanced recognition versus Silver. Confirm qualification nights/stays annually.",
      },
      {
        name: "Platinum",
        order: 4,
        kind: "elite",
        benefits: "Top published Rotana Rewards elite recognition. Confirm current benefit schedule before underwriting.",
      },
    ]),
  }),
  ascott_star_rewards: Object.freeze({
    id: "ascott_star_rewards",
    programName: "Ascott Star Rewards",
    officialUrl: "https://www.the-ascott.com/en/ascott-star-rewards.html",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.the-ascott.com/en/ascott-star-rewards.html"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Classic",
        order: 1,
        kind: "membership",
        benefits: "Base Ascott Star Rewards on participating Ascott brands (Citadines, lyf, Somerset, Oakwood, etc.).",
      },
      {
        name: "Silver",
        order: 2,
        kind: "elite",
        benefits: "Entry elite: bonus points and recognition per current Ascott Star Rewards rules.",
      },
      {
        name: "Gold",
        order: 3,
        kind: "elite",
        benefits: "Mid elite recognition and enhanced benefits versus Silver. Property delivery varies.",
      },
      {
        name: "Platinum",
        order: 4,
        kind: "elite",
        benefits: "Top published Ascott Star Rewards elite recognition. Confirm current qualification path.",
      },
    ]),
  }),
  golden_circle: Object.freeze({
    id: "golden_circle",
    programName: "Golden Circle",
    officialUrl: "https://www.shangri-la.com/corporate/golden-circle/",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.shangri-la.com/corporate/golden-circle/"],
    includeMemberLevel: true,
    tiers: Object.freeze([
      {
        name: "Jade",
        order: 1,
        kind: "membership",
        benefits: "Base Golden Circle membership on Shangri-La / participating sister brands. Confirm current earn rules.",
      },
      {
        name: "Gold",
        order: 2,
        kind: "elite",
        benefits: "Mid Golden Circle status with enhanced recognition versus Jade. Delivery varies by hotel.",
      },
      {
        name: "Diamond",
        order: 3,
        kind: "elite",
        benefits: "Upper Golden Circle elite recognition. Confirm current night/spend thresholds.",
      },
      {
        name: "Immersion",
        order: 4,
        kind: "elite",
        benefits:
          "Invitation / top recognition pathway within Golden Circle (per current Shangri-La terms). Label separately from standard elite ladder when presenting.",
      },
    ]),
  }),
  dusit_gold: Object.freeze({
    id: "dusit_gold",
    programName: "Dusit Gold",
    officialUrl: "https://www.dusit.com/dusitgold/",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.dusit.com/dusitgold/"],
    includeMemberLevel: true,
    note: "Some Dusit brands also participate in GHA DISCOVERY—confirm brand-specific applicability; do not silently substitute Discovery tiers without evidence.",
    tiers: Object.freeze([
      {
        name: "Member",
        order: 1,
        kind: "membership",
        benefits: "Base Dusit Gold membership on participating Dusit brands. Confirm whether the property also honors GHA DISCOVERY.",
      },
      {
        name: "Silver",
        order: 2,
        kind: "elite",
        benefits: "Entry Dusit Gold elite recognition per current program rules.",
      },
      {
        name: "Gold",
        order: 3,
        kind: "elite",
        benefits: "Mid Dusit Gold elite recognition versus Silver.",
      },
      {
        name: "Platinum",
        order: 4,
        kind: "elite",
        benefits: "Top published Dusit Gold elite recognition. Confirm current qualification.",
      },
    ]),
  }),
  pan_pacific_loyalty: Object.freeze({
    id: "pan_pacific_loyalty",
    programName: "Pan Pacific Hotels Group Loyalty",
    officialUrl: "https://www.panpacific.com/",
    verifiedAt: "2026-11-20",
    sourceUrls: ["https://www.panpacific.com/"],
    includeMemberLevel: true,
    note: "Confirm whether the specific PARKROYAL / Pan Pacific brand uses group loyalty, GHA DISCOVERY, or both before publication.",
    tiers: Object.freeze([
      {
        name: "Member",
        order: 1,
        kind: "membership",
        benefits: "Base Pan Pacific Hotels Group loyalty membership on participating brands—confirm current program name and earn rules.",
      },
      {
        name: "Silver",
        order: 2,
        kind: "elite",
        benefits: "Entry elite recognition per current Pan Pacific Hotels Group loyalty rules.",
      },
      {
        name: "Gold",
        order: 3,
        kind: "elite",
        benefits: "Mid elite recognition versus Silver. Property delivery varies.",
      },
      {
        name: "Platinum",
        order: 4,
        kind: "elite",
        benefits: "Top published group elite recognition. Verify current tier names on the official program site.",
      },
    ]),
  }),
  non_tiered_network: Object.freeze({
    id: "non_tiered_network",
    programName: "Membership network (non-tiered)",
    officialUrl: null,
    verifiedAt: "2026-11-20",
    sourceUrls: [],
    includeMemberLevel: false,
    nonTieredTreatment: true,
    tiers: Object.freeze([
      {
        name: "Membership Network Recognition",
        order: 1,
        kind: "non_tiered",
        benefits:
          "This brand participates in a membership / soft-brand network rather than a multi-tier franchise loyalty ladder. Recognition and member rates (if any) follow network rules—do not invent elite tiers.",
      },
    ]),
  }),
  none: Object.freeze({
    id: "none",
    programName: null,
    officialUrl: null,
    verifiedAt: "2026-11-20",
    sourceUrls: [],
    includeMemberLevel: false,
    tiers: Object.freeze([]),
    nonTieredTreatment: true,
  }),
});

const PROGRAM_ALIASES = Object.freeze({
  "marriott bonvoy": "marriott_bonvoy",
  bonvoy: "marriott_bonvoy",
  "hilton honors": "hilton_honors",
  honors: "hilton_honors",
  "world of hyatt": "world_of_hyatt",
  hyatt: "world_of_hyatt",
  "ihg one rewards": "ihg_one_rewards",
  "ihg® one rewards": "ihg_one_rewards",
  "one rewards": "ihg_one_rewards",
  "all - accor live limitless": "all_accor",
  "all accor": "all_accor",
  "accor live limitless": "all_accor",
  "wyndham rewards": "wyndham_rewards",
  "choice privileges": "choice_privileges",
  "best western rewards": "best_western_rewards",
  "sonesta travel pass": "sonesta_travel_pass",
  "gha discovery": "gha_discovery",
  "gha discovery®": "gha_discovery",
  "meliárewards": "melia_rewards",
  meliarewards: "melia_rewards",
  "minor discovery": "minor_discovery",
  discovery: "gha_discovery",
  "my barceló": "my_barcelo",
  "my barcelo": "my_barcelo",
  "my barceló benefits": "my_barcelo",
  "my barcelo benefits": "my_barcelo",
  "rotana rewards": "rotana_rewards",
  "ascott star rewards": "ascott_star_rewards",
  "golden circle": "golden_circle",
  "dusit gold": "dusit_gold",
  "pan pacific hotels group loyalty": "pan_pacific_loyalty",
  "pan pacific": "pan_pacific_loyalty",
  "preferred hotels": "non_tiered_network",
  "preferred hotels & resorts membership network": "non_tiered_network",
  "preferred hotels & resorts membership network (not a franchise loyalty program)":
    "non_tiered_network",
  "slh membership": "non_tiered_network",
  "slh membership network (not a franchise loyalty program)": "non_tiered_network",
  "small luxury hotels": "non_tiered_network",
  "small luxury hotels of the world": "non_tiered_network",
});

const PARENT_TO_PROGRAM = Object.freeze({
  marriott: "marriott_bonvoy",
  hilton: "hilton_honors",
  hyatt: "world_of_hyatt",
  ihg: "ihg_one_rewards",
  "intercontinental hotels": "ihg_one_rewards",
  accor: "all_accor",
  wyndham: "wyndham_rewards",
  choice: "choice_privileges",
  "best western": "best_western_rewards",
  sonesta: "sonesta_travel_pass",
  meliá: "melia_rewards",
  melia: "melia_rewards",
  minor: "minor_discovery",
  barceló: "my_barcelo",
  barcelo: "my_barcelo",
  rotana: "rotana_rewards",
  ascott: "ascott_star_rewards",
  citadines: "ascott_star_rewards",
  shangri: "golden_circle",
  dusit: "dusit_gold",
  "pan pacific": "pan_pacific_loyalty",
  parkroyal: "pan_pacific_loyalty",
  preferred: "non_tiered_network",
  "small luxury": "non_tiered_network",
  ascend: "choice_privileges",
  curio: "hilton_honors",
  autograph: "marriott_bonvoy",
});

export function resolveLoyaltyProgramId({ loyaltyProgram, parentCompany, brandName } = {}) {
  const prog = nz(loyaltyProgram).toLowerCase();
  if (prog && PROGRAM_ALIASES[prog]) return PROGRAM_ALIASES[prog];
  if (prog && prog.length >= 4) {
    // Prefer program title containing a known alias (not reverse match — avoids
    // short titles like "Elite" falsely matching "non_tiered_network" aliases).
    for (const [alias, id] of Object.entries(PROGRAM_ALIASES)) {
      if (alias.length >= 4 && prog.includes(alias)) return id;
    }
  }
  const parent = nz(parentCompany).toLowerCase();
  const brand = nz(brandName).toLowerCase();
  const blob = `${parent} ${brand}`;
  // Longer parent needles first (e.g. "pan pacific" before "pan")
  const parents = Object.entries(PARENT_TO_PROGRAM).sort((a, b) => b[0].length - a[0].length);
  for (const [needle, id] of parents) {
    if (blob.includes(needle)) return id;
  }
  return null;
}

export function getCanonicalLoyaltyProgram(programId) {
  if (!programId) return null;
  return CANONICAL_LOYALTY_PROGRAMS_V1[programId] || null;
}

export function buildLoyaltyEliteBlocksFromProgram(programId, opts = {}) {
  const prog = getCanonicalLoyaltyProgram(programId);
  if (!prog || !prog.tiers?.length) return [];
  const sortStart = opts.sortStart != null ? opts.sortStart : 3400;
  const source = prog.officialUrl || opts.source || null;
  return prog.tiers.map((t, i) => ({
    slotKey: LOYALTY_ELITE_SLOT,
    title: t.name,
    body: t.benefits,
    sort: sortStart + i,
    active: true,
    externalDisplayStatus: "Internal Preview",
    source,
    loyaltyProgramId: prog.id,
    loyaltyTierKind: t.kind,
    loyaltyTierOrder: t.order,
  }));
}

function eliteBlocks(blocks) {
  return (Array.isArray(blocks) ? blocks : []).filter(
    (b) => nz(b.slotKey || b.slot_key) === LOYALTY_ELITE_SLOT
  );
}

function isGenericElitePlaceholder(block) {
  const title = normTitle(block?.title);
  const body = nz(block?.body).toLowerCase();
  if (/^(elite recognition|elite|tier|elite tiers|illustrative)$/i.test(nz(block?.title))) return true;
  if (title === "elite recognition") return true;
  if (/elite status recognition \(upgrades/i.test(body) && !/silver|gold|platinum|diamond|titanium|ambassador|globalist|discoverist|explorist/i.test(title)) {
    return true;
  }
  return false;
}

function namedTierTitles(blocks) {
  return eliteBlocks(blocks)
    .filter((b) => !isGenericElitePlaceholder(b) && nz(b.title))
    .map((b) => nz(b.title));
}

/**
 * Validate loyalty.elite tier set against canonical program (when known).
 */
export function validateLoyaltyEliteTier(blocks, opts = {}) {
  const list = eliteBlocks(blocks);
  const substantive = list.filter((b) => nz(b.title) || nz(b.body));
  const named = substantive.filter((b) => !isGenericElitePlaceholder(b));
  const reasons = [];
  const flags = [];
  const programId = opts.programId || null;
  const prog = programId ? getCanonicalLoyaltyProgram(programId) : null;

  if (opts.participates === false) {
    return {
      ok: true,
      count: substantive.length,
      namedCount: named.length,
      expectedCount: 0,
      reasons: ["non_participating_or_non_tiered"],
      flags,
      blocked: false,
      missingTierNames: [],
      programId,
    };
  }

  if (substantive.length === 1 && isGenericElitePlaceholder(substantive[0])) {
    reasons.push(LOYALTY_TIER_FAIL.GENERIC_CARD);
    flags.push(LOYALTY_TIER_FAIL.COLLAPSED);
  }

  let expectedNames = [];
  if (prog?.tiers?.length) {
    expectedNames = prog.tiers.map((t) => t.name);
    const have = new Set(named.map((b) => normTitle(b.title)));
    const missing = expectedNames.filter((n) => !have.has(normTitle(n)));
    if (missing.length) {
      reasons.push(LOYALTY_TIER_FAIL.INCOMPLETE);
      flags.push(LOYALTY_TIER_FAIL.INCOMPLETE);
    }
    if (named.length > 0 && named.length < expectedNames.length && substantive.length === 1) {
      flags.push(LOYALTY_TIER_FAIL.COLLAPSED);
    }
  } else if (opts.requireProgram === true) {
    reasons.push(LOYALTY_TIER_FAIL.PARTICIPATION_UNVERIFIED);
    flags.push(LOYALTY_TIER_FAIL.PARTICIPATION_UNVERIFIED);
  }

  const missingTierNames =
    prog?.tiers?.length && named.length
      ? prog.tiers
          .map((t) => t.name)
          .filter((n) => !named.some((b) => normTitle(b.title) === normTitle(n)))
      : prog?.tiers?.map((t) => t.name) || [];

  const expectedCount = prog?.tiers?.length || 0;
  const ok =
    reasons.length === 0 &&
    (expectedCount === 0 || named.length >= expectedCount) &&
    !(substantive.length === 1 && isGenericElitePlaceholder(substantive[0]));

  return {
    ok,
    count: substantive.length,
    namedCount: named.length,
    expectedCount,
    expectedNames,
    missingTierNames,
    reasons,
    flags: [...new Set(flags)],
    blocked: !ok && (opts.blockOnFail !== false),
    programId,
    programName: prog?.programName || null,
  };
}

/**
 * Assembler decision: never let a collapsed/generic elite candidate wipe CURRENT named tiers.
 */
export function decideLoyaltyEliteCandidateApply(currentBlocks, candidateBlocks, opts = {}) {
  const curNamed = namedTierTitles(currentBlocks);
  const candNamed = namedTierTitles(candidateBlocks);
  const candElite = eliteBlocks(candidateBlocks);
  const curElite = eliteBlocks(currentBlocks);
  const candGenericOnly =
    candElite.length > 0 && candElite.every(isGenericElitePlaceholder);
  const programId = opts.programId || null;
  const candVal = validateLoyaltyEliteTier(candidateBlocks, {
    programId,
    participates: opts.participates !== false,
  });
  const curVal = validateLoyaltyEliteTier(currentBlocks, {
    programId,
    participates: opts.participates !== false,
  });

  if (candElite.length === 0) {
    return {
      applyCandidate: false,
      reason: "loyalty_elite_no_candidate",
      retainCurrent: curNamed.length > 0,
      flags: [],
      candidate: candVal,
      current: curVal,
    };
  }

  if (candGenericOnly && curNamed.length >= 2) {
    return {
      applyCandidate: false,
      reason: LOYALTY_TIER_FAIL.REGRESSION,
      retainCurrent: true,
      flags: [LOYALTY_TIER_FAIL.REGRESSION, LOYALTY_TIER_FAIL.COLLAPSED],
      candidate: candVal,
      current: curVal,
    };
  }

  if (candNamed.length > 0 && curNamed.length > candNamed.length && candNamed.length < 3) {
    return {
      applyCandidate: false,
      reason: LOYALTY_TIER_FAIL.REGRESSION,
      retainCurrent: true,
      flags: [LOYALTY_TIER_FAIL.REGRESSION],
      candidate: candVal,
      current: curVal,
    };
  }

  if (candVal.ok && candNamed.length >= (candVal.expectedCount || candNamed.length)) {
    return {
      applyCandidate: true,
      reason: "loyalty_elite_canonical_ok",
      retainCurrent: false,
      flags: [],
      candidate: candVal,
      current: curVal,
      replacementBlocks: candElite,
    };
  }

  if (curNamed.length >= 2 && !candVal.ok) {
    return {
      applyCandidate: false,
      reason: candVal.flags[0] || LOYALTY_TIER_FAIL.INCOMPLETE,
      retainCurrent: true,
      flags: candVal.flags,
      candidate: candVal,
      current: curVal,
    };
  }

  if (candNamed.length >= 2 && !candGenericOnly) {
    return {
      applyCandidate: true,
      reason: "loyalty_elite_named_tiers_present",
      retainCurrent: false,
      flags: candVal.flags,
      candidate: candVal,
      current: curVal,
    };
  }

  return {
    applyCandidate: false,
    reason: LOYALTY_TIER_FAIL.INCOMPLETE,
    retainCurrent: curElite.length > 0,
    flags: [LOYALTY_TIER_FAIL.INCOMPLETE, ...candVal.flags],
    candidate: candVal,
    current: curVal,
  };
}

export function isGenericLoyaltyElitePlaceholder(block) {
  return isGenericElitePlaceholder(block);
}
