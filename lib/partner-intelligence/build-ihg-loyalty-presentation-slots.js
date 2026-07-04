/**
 * Build Brand Explorer loyalty.* presentation rows from IHG official tier benefits
 * plus Kimpton-specific contribution stats from extracted brand facts.
 */
import {
  IHG_ONE_REWARDS_TIER_BENEFITS_URL,
  IHG_ONE_REWARDS_TIER_BENEFITS_CAPTURED,
  IHG_ONE_REWARDS_ELITE_TIERS,
  IHG_ONE_REWARDS_ECOSYSTEM_SUMMARY,
  IHG_ONE_REWARDS_EARN_BULLETS,
  IHG_ONE_REWARDS_REDEEM_BULLETS,
  IHG_ONE_REWARDS_PROOF_POINTS,
} from "./ihg-one-rewards-tier-benefits-source.js";

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function factValue(mergedFacts, fieldKey) {
  const hit = (mergedFacts || []).find((f) => f.fieldKey === fieldKey && f.dataGap !== "Yes");
  return hit ? nz(hit.extractedValue) : "";
}

/**
 * Replace all loyalty.* rows in a presentation fixture with IHG-aligned content.
 * @param {object[]} templateRows
 * @param {object[]} mergedFacts
 * @param {{ brandName?: string }} [opts]
 * @returns {object[]}
 */
export function applyIhgLoyaltyPresentationSlots(templateRows, mergedFacts, opts = {}) {
  const brandName = opts.brandName || "Kimpton Hotels";
  const loyaltyPct = factValue(mergedFacts, "be.loyalty.roomContributionPct");
  const enterprisePct = factValue(mergedFacts, "be.loyalty.enterpriseBookingPct");
  const memberMillions = factValue(mergedFacts, "be.loyalty.memberCount");

  const kimptonStats = [];
  if (loyaltyPct) {
    kimptonStats.push(
      `IHG One Rewards generated an average of ${loyaltyPct}% of Kimpton Hotels bookings (2025 U.S. comparable hotels per Kimpton development brochure / FDD source note).`
    );
  }
  if (enterprisePct) {
    kimptonStats.push(
      `IHG booking channels represented an average of ${enterprisePct}% of Kimpton reservations in the same period.`
    );
  }

  const ownerLens = [
    "Model loyalty as net contribution after member discounts, elite benefits, and IHG program chargebacks — not headline ADR.",
    kimptonStats.join(" "),
    memberMillions
      ? `IHG One Rewards reports ${memberMillions}M+ members globally (Kimpton development web capture).`
      : "",
    "Confirm Kimpton-specific loyalty P&L in your FDD Item 19 sample and IHG program fee schedule.",
  ]
    .filter(Boolean)
    .join("\n");

  const loyaltyRows = [
    {
      slotKey: "loyalty.hero_title",
      title: "",
      body: `${brandName} · IHG One Rewards — tier benefits at a glance`,
      sort: 0,
    },
    {
      slotKey: "loyalty.ecosystem",
      title: "",
      body: IHG_ONE_REWARDS_ECOSYSTEM_SUMMARY,
      sort: 0,
    },
    {
      slotKey: "loyalty.owner_lens",
      title: "",
      body: ownerLens,
      sort: 0,
    },
    ...IHG_ONE_REWARDS_PROOF_POINTS.map((p, i) => ({
      slotKey: "loyalty.proof",
      title: p.title,
      body: p.body,
      sort: i,
    })),
    {
      slotKey: "loyalty.earn",
      title: "",
      body: IHG_ONE_REWARDS_EARN_BULLETS.join("\n"),
      sort: 0,
    },
    {
      slotKey: "loyalty.redeem",
      title: "",
      body: IHG_ONE_REWARDS_REDEEM_BULLETS.join("\n"),
      sort: 0,
    },
    ...IHG_ONE_REWARDS_ELITE_TIERS.map((tier, i) => ({
      slotKey: "loyalty.elite",
      title: tier.headline,
      body: `${tier.qualification} — ${tier.body}`,
      sort: i,
    })),
    {
      slotKey: "loyalty.implications.pnl",
      title: "",
      body:
        "Underwrite net room revenue after IHG One Rewards participation, member-rate leakage, Reward Night redemptions, and loyalty marketing assessments in your FDD — Kimpton brochure cites ~50.8% loyalty booking mix and ~88.5% IHG channel mix (illustrative; confirm in your disclosure).",
      sort: 0,
    },
    {
      slotKey: "loyalty.implications.ops",
      title: "",
      body:
        "Elite welcome amenities (points, drink/snack, or breakfast at Diamond), upgrades, and late check-out create operating load — staff to IHG elite recognition standards before campaigns.",
      sort: 0,
    },
    {
      slotKey: "loyalty.implications.systems",
      title: "",
      body:
        "Mandatory IHG CRS/PMS loyalty integration, member pricing, and Milestone Rewards fulfillment — budget cutover, training, and ongoing program compliance.",
      sort: 0,
    },
  ];

  const nonLoyalty = (templateRows || []).filter((r) => !nz(r.slotKey).startsWith("loyalty."));
  return [...nonLoyalty, ...loyaltyRows];
}
