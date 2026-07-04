/**
 * Brand Explorer presentation rows for loyalty.* slots (Choice Privileges system program).
 * Sources: docs/choice-privileges-web-reference.md, fixtures/choice-privileges-web/*.md
 */
import { FDD_ITEM19 } from "./choice-fdd-item19.mjs";
import { GLOBAL_MEMBERS_M } from "./choice-loyalty-commercial-fixtures.mjs";

/** @param {{ title?: string; body?: string; sort?: number; slotKey: string }} row */
function row(slotKey, body, { title = "", sort = 0 } = {}) {
  return { slotKey, title, body, sort };
}

/**
 * @param {string} brandName — exact Brand Basics name
 * @returns {{ slotKey: string; title: string; body: string; sort: number }[]}
 */
export function buildChoicePrivilegesLoyaltyRows(brandName) {
  const name = String(brandName || "").trim();
  const item19 = FDD_ITEM19[name] || {};
  const loyaltyLine = item19.loyaltyPct
    ? `FDD Item 19 (${item19.performanceYear || "FY 2025"} sample): ~${item19.loyaltyPct}% of rooms from Choice Privileges contribution.`
    : "Confirm Choice Privileges room-mix contribution in your FDD Item 19—no published % in current disclosure for this brand.";
  const crsLine = item19.enterprisePct
    ? `~${item19.enterprisePct}% enterprise/CRS booking mix in same sample.`
    : item19.proprietaryPct
      ? `~${item19.proprietaryPct}% proprietary (non-OTA) booking mix in same sample.`
      : "Confirm CRS/enterprise or proprietary mix in FDD Item 19.";

  const mixBody =
    item19.loyaltyPct != null
      ? `~${item19.loyaltyPct}% of rooms from loyalty (est.)`
      : "Confirm in FDD Item 19 (est.)";

  return [
    row("loyalty.kpi.members", `~${GLOBAL_MEMBERS_M}M members (program-wide)`, { sort: 0 }),
    row("loyalty.kpi.hotels", "7,100+ properties (Choice Privileges network)", { sort: 0 }),
    row("loyalty.kpi.markets", "Global · 20+ brands in portfolio", { sort: 0 }),
    row("loyalty.kpi.mix", mixBody, { sort: 0 }),
    row("loyalty.hero_title", `${name} · Choice Privileges® — loyalty at a glance`, { sort: 0 }),
    row(
      "loyalty.ecosystem",
      "7,100+ hotels worldwide across 20+ brands; earn up to 10 points per $1 on eligible direct stays; reward nights from 8,000 points; Gold in 5 nights; Titanium top tier; Return & Earn and partner ecosystem—confirm chargeback and program rules in your FDD.",
      { sort: 0 }
    ),
    row(
      "loyalty.owner_lens",
      item19.loyaltyPct
        ? `Model loyalty as net contribution—member discounts, fulfillment, and ~${item19.loyaltyPct}% room mix from Choice Privileges in Item 19 sample (not headline RevPAR).`
        : "Model loyalty as net contribution—member discounts, fulfillment, and FDD-reported room mix from Choice Privileges in Item 19 sample.",
      { sort: 0 }
    ),
    row("loyalty.proof", loyaltyLine, { title: "Repeat Guest Capture", sort: 0 }),
    row(
      "loyalty.proof",
      `Member rates (10%+ savings) and choicehotels.com retail defend contribution versus OTA—${crsLine}`,
      { title: "Direct Channel & Member Pricing", sort: 1 }
    ),
    row(
      "loyalty.proof",
      "Gold (5 nights / 10k EQCs), Platinum (15n / 30k), Diamond (35n / 70k), Titanium (55n / 110k)—points bonuses and recognition scale by tier; fulfillment cost hits reviews if understaffed.",
      { title: "Elite Member Value", sort: 2 }
    ),
    row(
      "loyalty.proof",
      "7,100+ hotels worldwide; members earn across 20+ brands—relevant when your market sees blended corporate and leisure portfolios.",
      { title: "Cross-Brand Traveler Flow", sort: 3 }
    ),
    row(
      "loyalty.proof",
      "Choice Privileges business travel, cobrand cards, and partners (resorts, airlines, experiences)—model only what your agreement authorizes.",
      { title: "Corporate & SME Pull", sort: 4 }
    ),
    row(
      "loyalty.proof",
      "U.S. News #1 hotel rewards program (consumer site, 14 programs evaluated); Return & Earn (1,000 pts after 2nd & 3rd qualifying stays/year)—pair with property execution.",
      { title: "Campaign Scale", sort: 5 }
    ),
    row(
      "loyalty.earn",
      "Base earn: up to 10 points per $1 on qualifying stays booked direct at 7,100+ properties.\nFast Gold: 5 nights or 10,000 Elite Qualifying Credits (EQCs); cardholders can unlock Gold via Choice Privileges Mastercard spend.\nReturn & Earn: 1,000 bonus points after 2nd and 3rd qualifying stays each calendar year.\nCobrand: up to 16x–22x total points on stays with Choice Privileges Mastercard per choicehotels.com/choice-privileges.",
      { sort: 0 }
    ),
    row(
      "loyalty.redeem",
      "Reward nights from 8,000 points (property and date dependent; demand-responsive pricing).\nPoints + cash where offered on eligible stays.\nPartner redemptions (resorts, gift cards, airline miles, charity)—storytelling unless your FDD authorizes economics.",
      { sort: 0 }
    ),
    row("loyalty.elite", "Member — baseline rates, milestone rewards path, points never expire.", {
      title: "Member",
      sort: 0,
    }),
    row("loyalty.elite", "Gold — 5 nights or 10,000 EQCs; bonus points on stays and enhanced recognition.", {
      title: "Gold",
      sort: 1,
    }),
    row(
      "loyalty.elite",
      "Platinum — 15 nights or 30,000 EQCs; stronger earn bonus and elite benefits where available.",
      { title: "Platinum", sort: 2 }
    ),
    row(
      "loyalty.elite",
      "Diamond — 35 nights or 70,000 EQCs; top mainstream tier with upgrades, late checkout, and milestone rewards where published.",
      { title: "Diamond", sort: 3 }
    ),
    row(
      "loyalty.elite",
      "Titanium — 55 nights or 110,000 EQCs; highest published tier including Titanium Travel Award where offered.",
      { title: "Titanium", sort: 4 }
    ),
    row(
      "loyalty.implications.pnl",
      "Model net after distribution, member discounts, and redemption—strongest when direct/member mix supports your brand tier ADR; use Item 19 loyalty % where disclosed.",
      { sort: 0 }
    ),
    row(
      "loyalty.implications.ops",
      "Elite recognition, welcome gifts, and breakfast/F&B promises at top tiers become labor and COGS—staff to brand standards before campaigns.",
      { sort: 0 }
    ),
    row(
      "loyalty.implications.systems",
      "CRS/PMS loyalty integration and campaign tooling are mandatory—budget cutover, training, and choicehotels.com rate parity discipline.",
      { sort: 0 }
    ),
  ];
}
