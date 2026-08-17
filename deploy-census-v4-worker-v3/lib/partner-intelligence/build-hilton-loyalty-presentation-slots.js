/**
 * Build Brand Explorer loyalty.* presentation rows from Hilton Honors official sources
 * plus brand-specific contribution stats from extracted facts when available.
 */
import {
  HILTON_HONORS_ELITE_TIERS,
  HILTON_HONORS_ECOSYSTEM_SUMMARY,
  HILTON_HONORS_EARN_BULLETS,
  HILTON_HONORS_REDEEM_BULLETS,
  HILTON_HONORS_PROOF_POINTS,
  HILTON_HONORS_POINTS_VALUE_BAND,
  HILTON_HONORS_OWNER_LENS_BULLETS,
  localizeHiltonHonorsCopy,
} from "./hilton-honors-loyalty-source.js";

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function factValue(mergedFacts, fieldKey) {
  const hit = (mergedFacts || []).find((f) => f.fieldKey === fieldKey && f.dataGap !== "Yes");
  return hit ? nz(hit.extractedValue) : "";
}

/**
 * Replace all loyalty.* rows in a presentation fixture with Hilton Honors content.
 * @param {object[]} templateRows
 * @param {object[]} mergedFacts
 * @param {{ brandName?: string }} [opts]
 * @returns {object[]}
 */
export function applyHiltonLoyaltyPresentationSlots(templateRows, mergedFacts, opts = {}) {
  const brandName = opts.brandName || "Curio Collection by Hilton";
  const loyaltyPct = factValue(mergedFacts, "be.loyalty.roomContributionPct");
  const enterprisePct = factValue(mergedFacts, "be.loyalty.enterpriseBookingPct");
  const memberMillions = factValue(mergedFacts, "be.loyalty.memberCount");

  const brandStats = [];
  if (loyaltyPct && !/ihg/i.test(loyaltyPct)) {
    brandStats.push(
      `Hilton Honors generated an average of ${loyaltyPct}% of ${brandName} bookings (per extracted FDD / development source — confirm in your disclosure).`
    );
  }
  if (enterprisePct && !/ihg/i.test(enterprisePct)) {
    brandStats.push(
      `Hilton enterprise channels represented an average of ${enterprisePct}% of ${brandName} reservations in the same period.`
    );
  }

  const ownerLens = [
    ...HILTON_HONORS_OWNER_LENS_BULLETS.map((b) => localizeHiltonHonorsCopy(b, brandName)),
    brandStats.join(" "),
    memberMillions && !/ihg/i.test(memberMillions)
      ? `Hilton Honors reports ${memberMillions}M+ members globally (per captured Hilton source).`
      : "",
    `Third-party points valuation band ~${HILTON_HONORS_POINTS_VALUE_BAND.averageCpp}¢ per Point average; strong redemptions ${HILTON_HONORS_POINTS_VALUE_BAND.strongCpp}¢+ (${HILTON_HONORS_POINTS_VALUE_BAND.note}).`,
  ]
    .filter(Boolean)
    .join("\n");

  const loyaltyRows = [
    {
      slotKey: "loyalty.hero_title",
      title: "",
      body: `${brandName} · Hilton Honors — tier benefits at a glance (2026 program)`,
      sort: 0,
    },
    {
      slotKey: "loyalty.ecosystem",
      title: "",
      body: localizeHiltonHonorsCopy(HILTON_HONORS_ECOSYSTEM_SUMMARY, brandName),
      sort: 0,
    },
    {
      slotKey: "loyalty.owner_lens",
      title: "",
      body: ownerLens,
      sort: 0,
    },
    ...HILTON_HONORS_PROOF_POINTS.map((p, i) => ({
      slotKey: "loyalty.proof",
      title: p.title,
      body: localizeHiltonHonorsCopy(p.body, brandName),
      sort: i,
    })),
    {
      slotKey: "loyalty.earn",
      title: "",
      body: HILTON_HONORS_EARN_BULLETS.map((b) => localizeHiltonHonorsCopy(b, brandName)).join("\n"),
      sort: 0,
    },
    {
      slotKey: "loyalty.redeem",
      title: "",
      body: HILTON_HONORS_REDEEM_BULLETS.map((b) => localizeHiltonHonorsCopy(b, brandName)).join("\n"),
      sort: 0,
    },
    ...HILTON_HONORS_ELITE_TIERS.map((tier, i) => ({
      slotKey: "loyalty.elite",
      title: tier.headline,
      body: `${tier.qualification} — ${tier.body}`,
      sort: i,
    })),
    {
      slotKey: "loyalty.implications.pnl",
      title: "",
      body: localizeHiltonHonorsCopy(
        "Underwrite net room revenue after Hilton Honors participation, member-rate leakage, reward-night redemptions, and loyalty marketing assessments in your FDD — confirm {{brand}}-specific program fees and chargebacks in your franchise disclosure.",
        brandName
      ),
      sort: 0,
    },
    {
      slotKey: "loyalty.implications.ops",
      title: "",
      body: localizeHiltonHonorsCopy(
        "Elite F&B credits, upgrades, lounge access, and guaranteed late checkout (Diamond Reserve) create operating load — staff to Hilton elite recognition standards before campaigns.",
        brandName
      ),
      sort: 0,
    },
    {
      slotKey: "loyalty.implications.systems",
      title: "",
      body: localizeHiltonHonorsCopy(
        "Mandatory Hilton CRS/PMS loyalty integration, member pricing, and Milestone Rewards fulfillment — budget cutover, training, and ongoing program compliance.",
        brandName
      ),
      sort: 0,
    },
  ];

  const nonLoyalty = (templateRows || []).filter((r) => !nz(r.slotKey).startsWith("loyalty."));
  return [...nonLoyalty, ...loyaltyRows];
}
