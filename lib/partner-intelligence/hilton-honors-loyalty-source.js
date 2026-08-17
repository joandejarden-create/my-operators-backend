/**
 * Hilton Honors — canonical loyalty copy from official Hilton sources.
 * Primary: Nov 2025 program changes press release (effective Jan 2026).
 * Secondary: hilton.com/hilton-honors consumer pages (website capture).
 */

export const HILTON_HONORS_PROGRAM_URL = "https://www.hilton.com/en/hilton-honors/";
export const HILTON_HONORS_POINTS_URL = "https://www.hilton.com/en/hilton-honors/points/";
export const HILTON_HONORS_2026_PRESS_RELEASE_URL =
  "https://stories.hilton.com/releases/loyalty-upgraded-hilton-honors-introduces-faster-path-to-elite-status-and-reveals-new-premium-tier-diamond-reserve";

export const HILTON_HONORS_PRESS_RELEASE_CAPTURED =
  "Hilton/press/Hilton Honors 2026 Program Changes Press Release.md";
export const HILTON_HONORS_POINTS_CAPTURED =
  "Hilton/website/Hilton Honors Points and Miles.html";
export const HILTON_HONORS_OVERVIEW_CAPTURED =
  "Hilton/website/Hilton Honors Program Overview.html";

/** @type {{ name: string; qualification: string; headline: string; body: string }[]} */
export const HILTON_HONORS_ELITE_TIERS = [
  {
    name: "Member",
    qualification: "Join for free",
    headline: "Hilton Honors Member",
    body:
      "Earn Points on stays; Member Rates; Digital Key at participating hotels; flexible Points + Money payment; no blackout dates on reward stays booked with all Points.",
  },
  {
    name: "Silver",
    qualification: "10 nights, 4 stays, or $2,500 USD eligible annual spend",
    headline: "Silver",
    body:
      "Entry elite tier (thresholds unchanged for 2026). Fifth Night Free on standard room rewards; Points pooling with up to 10 members.",
  },
  {
    name: "Gold",
    qualification: "25 nights, 15 stays, or $6,000 USD eligible annual spend (2026)",
    headline: "Gold",
    body:
      "Space-available room upgrades; Daily Food & Beverage Credit or continental breakfast (varies by brand/region); 80% Points bonus on stays. Threshold reduced from 40 nights in 2026.",
  },
  {
    name: "Diamond",
    qualification: "50 nights, 25 stays, or $11,500 USD eligible annual spend (2026)",
    headline: "Diamond",
    body:
      "Executive lounge access where available; 48-hour room guarantee; Daily F&B Credit or breakfast; 100% Points bonus; space-available upgrades confirmed earlier. Threshold reduced from 60 nights in 2026.",
  },
  {
    name: "Diamond Reserve",
    qualification: "80 nights or 40 stays AND $18,000 USD eligible annual spend (2026)",
    headline: "Diamond Reserve",
    body:
      "Confirmable Upgrade Reward at booking (up to 7 nights, suite at select properties); guaranteed 4 p.m. late checkout; 24/7 dedicated support; Premium Clubs access; highest upgrade priority; 120% Points bonus. No credit-card shortcut.",
  },
];

export const HILTON_HONORS_ECOSYSTEM_SUMMARY = [
  "Hilton Honors spans 28 Hilton brands and 9,000+ hotels globally.",
  "Members earn and redeem with no blackout dates on all-Points reward stays; Fifth Night Free on standard room awards.",
  "2026 program shifts elite qualification to nights, stays, or eligible USD spend (base points no longer qualify).",
  "Diamond Reserve adds confirmable suite upgrades and guaranteed late checkout for top-tier loyalists.",
].join("\n");

/** Third-party valuation band for owner modeling — not Hilton official. */
export const HILTON_HONORS_POINTS_VALUE_BAND = {
  source: "Hilton/inbox/Best Hilton Hotels Points Value Guide 2026.html",
  averageCpp: "0.5–0.6",
  strongCpp: "0.7+",
  note: "Dynamic award pricing; model property-level contribution, not headline cpp.",
};

export const HILTON_HONORS_OWNER_LENS_BULLETS = [
  "Model loyalty as net room revenue after member discounts, elite benefits, and program chargebacks.",
  "{{brand}} guests receive Hilton Honors benefits (Digital Key, member rates, free Wi-Fi) while keeping independent hotel character.",
  "Confirm 2026 tier thresholds and {{brand}}-specific program fees in your FDD and Hilton franchise disclosure schedule.",
];

export const HILTON_HONORS_EARN_BULLETS = [
  "Earn Hilton Honors Points on eligible stays; elite tiers add 80%–120% bonus Points (Gold through Diamond Reserve, per 2026 program rules).",
  "Member Rates and member promotions on direct Hilton bookings.",
  "Milestone Rewards from 2026: 10,000 bonus Points every 10 nights after 40 nights; 30,000 Points at 60 nights; at 120 nights choose 30,000 Points or a second Confirmable Upgrade Reward.",
  "Points pooling with up to 10 Hilton Honors members.",
];

export const HILTON_HONORS_REDEEM_BULLETS = [
  "Redeem Points for reward nights across 28 Hilton brands — no blackout dates on all-Points reward stays.",
  "Fifth Night Free on standard room rewards booked entirely with Points.",
  "Points + Money to combine Points and cash toward a stay.",
  "No resort fees on reward stays booked with all Points.",
];

export const HILTON_HONORS_PROOF_POINTS = [
  {
    title: "9,000+ Hotels",
    body: "Hilton Honors spans 28 brands and 9,000+ hotels globally (Nov 2025 Hilton press release).",
  },
  {
    title: "No Blackout Dates",
    body: "No blackout dates on reward stays booked with all Points (Hilton Honors 2026 press release).",
  },
  {
    title: "Fifth Night Free",
    body: "Fifth Night Free on standard room rewards booked entirely with Points.",
  },
  {
    title: "2026 Tier Refresh",
    body: "Gold at 25 nights; Diamond at 50 nights; new Diamond Reserve at 80 nights + $18k eligible spend.",
  },
  {
    title: "Confirmable Upgrades",
    body: "Diamond Reserve Confirmable Upgrade Reward locks premium room or suite at booking (up to 7 nights).",
  },
  {
    title: "Digital Key",
    body: "Digital Key and member benefits at participating {{brand}} properties.",
  },
];

/** Replace {{brand}} tokens (and legacy Curio Collection phrasing) with the live brand name. */
export function localizeHiltonHonorsCopy(text, brandName) {
  const name = String(brandName || "this Hilton brand").trim();
  return String(text || "")
    .replace(/\{\{brand\}\}/g, name)
    .replace(/Curio Collection by Hilton/gi, name)
    .replace(/Curio Collection/gi, name)
    .replace(/Curio-specific/gi, `${name}-specific`);
}