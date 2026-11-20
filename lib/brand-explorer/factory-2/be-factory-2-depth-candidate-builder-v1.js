/**
 * Build Factory 2.0 depth candidate overlay from a research pack + live CURRENT.
 * Does not invent fee amounts. Enforces exact-six loyalty.proof cards.
 */
import { assembleCumulativeProposal } from "../be-cumulative-proposal-assembler-v1.js";
import { hashPresentationBlocks } from "../be-founder-approval-publication-v1.js";
import {
  evaluateFactoryPermanentStandardsGate,
  normalizeRegionalAcronymsInHeading,
} from "../be-factory-permanent-loyalty-region-standards-v1.js";
import { getDepthPack } from "./be-factory-2-depth-research-packs-v1.js";

function block(slotKey, title, body, sort, extra = {}) {
  return {
    slotKey,
    title: normalizeRegionalAcronymsInHeading(title || ""),
    body: body || "",
    sort: sort || 0,
    active: true,
    imageUrl: extra.imageUrl || null,
    externalDisplayStatus: "Internal Preview",
    source: extra.source || "factory-2-0-depth-research",
  };
}

export function buildDepthGapFillBlocks(pack) {
  let s = 2000;
  const next = () => ++s;
  const src = pack.sources || {};
  const program = pack.loyaltyProgram;
  const short = pack.brandName;

  const out = [
    block(
      "commercial.intro",
      "Commercial Engine",
      `${short} commercial distribution rides ${program} direct channels, GDS, OTAs, and parent sales systems. ${pack.positioning} Owners should underwrite channel mix and loyalty contribution for this asset—not corporate averages alone.`,
      next(),
      { source: src.brandPage || src.development }
    ),
    block(
      "commercial.differentiator",
      "Commercial Differentiator",
      pack.commercialDifferentiator,
      next()
    ),
    block(
      "commercial.kpi.channels",
      "Channels",
      `Brand.com / ${program} app · major OTAs · GDS · metasearch (where active for the hotel)`,
      next()
    ),
    block(
      "commercial.kpi.campaigns",
      "Campaigns",
      `${program} and parent portfolio campaigns where the hotel is eligible`,
      next()
    ),
    block(
      "commercial.kpi.b2b",
      "B2B Programs",
      `Corporate, SME, and RFP programs routed through parent sales infrastructure where active for this flag`,
      next()
    ),
    block(
      "commercial.kpi.lens",
      "Owner Lens",
      `Model ${short} contribution after loyalty chargebacks, preferred-agency commissions, and local competitive set reality—not headline RevPAR alone.`,
      next()
    ),
    block(
      "commercial.demand",
      "Demand Pattern",
      `${short} demand follows destination seasonality plus ${program} campaign calendars. Treat portfolio averages as directional; diligence this hotel's source markets and corporate vs leisure mix.`,
      next()
    ),
    block(
      "commercial.lever.distribution",
      "Distribution & Channel Mix",
      `Parent central reservations, ${program} app, and designated online channels—confirm commission structures and rate parity obligations in the current agreement exhibit.`,
      next()
    ),
    block(
      "commercial.lever.revenue_management",
      "Revenue Management",
      `Parent revenue tools and ${program} demand signals support pricing experiments; owners still need property-level strategy for this brand's product promise.`,
      next()
    ),
    block(
      "commercial.lever.digital_marketing",
      "Digital Marketing & Performance Media",
      `Paid and owned media templates at portfolio scale, with creative that must still feel brand- and hotel-specific rather than generic parent advertising.`,
      next()
    ),
    block(
      "commercial.lever.corporate_group",
      "Corporate, SME & Group Pull",
      `Contracted travelers, small meetings, and negotiated programs where ${short} fits the account's brand ladder.`,
      next()
    ),
    block(
      "commercial.perception",
      "Owner Perception Risk",
      `Owners sometimes expect lifestyle lightness without systems cost; ${short} still carries parent systems, ${program} economics, and brand QA. Align IC expectations early.`,
      next()
    ),

    block(
      "loyalty.program",
      program,
      `${short} participates in ${program}. Members earn and redeem points and receive Elite recognition across participating sister brands. Confirm current tier names and earn rules on the official program site before IC.`,
      next(),
      { source: src.bonvoy || src.oneRewards || src.honors }
    ),
    block("loyalty.hero_title", "Loyalty", `${program} at ${short}`, next()),
    block(
      "loyalty.earn",
      "Earn",
      `Guests earn ${program} points on qualifying stays at ${short}. Elite members may receive enhanced earn and recognition benefits per current program rules.`,
      next()
    ),
    block(
      "loyalty.redeem",
      "Redeem",
      `Points redeem for stays across the ${program} portfolio, including ${short}, subject to availability and current redemption charts.`,
      next()
    ),
    block(
      "loyalty.elite",
      "Elite Recognition",
      `Elite status recognition (upgrades, late checkout, lounge access where offered) applies per property capability—delivery varies by hotel.`,
      next()
    ),
    block(
      "loyalty.ecosystem",
      "Ecosystem",
      `${program} partnerships, co-branded cards, and bonus campaigns expand the loyalty halo; ${short} stays remain the property-level earn/redeem node.`,
      next()
    ),
    block(
      "loyalty.owner_lens",
      "Owner Lens",
      `${program} drives distribution and repeat demand but adds loyalty chargebacks and program economics. Underwrite net contribution after loyalty—not membership vanity metrics.`,
      next()
    ),
    block(
      "loyalty.implications.pnl",
      "P&L Implications",
      `Loyalty assessments and program contribution affect net rooms revenue. Use current agreement exhibits; do not invent undisclosed rates.`,
      next()
    ),
    block(
      "loyalty.implications.ops",
      "Operating Implications",
      `Front desk and systems must support ${program} enrollment, Elite recognition, and redemption stays without erasing the brand's guest experience.`,
      next()
    ),
    block(
      "loyalty.implications.systems",
      "Systems Implications",
      `Parent property and reservation systems integration is required for ${program} participation—budget cutover and training in the conversion plan.`,
      next()
    ),
  ];

  for (const card of pack.loyaltyProof || []) {
    out.push(
      block("loyalty.proof", card.title, card.body, next(), {
        source: src.bonvoy || src.oneRewards || src.honors,
      })
    );
  }

  // Fee buckets — structure only; no invented amounts
  out.push(
    block(
      "economics.fee.join",
      "To Join",
      pack.feeNote,
      next(),
      { source: src.development || src.brandPage }
    ),
    block(
      "economics.fee.operate",
      "To Operate",
      `Steady-state royalty, program contribution, and ${program} loyalty assessments — confirm definitions, bases, and current percentages in the live disclosure exhibit for ${short}. Do not underwrite from peer rumor.`,
      next(),
      { source: src.development || src.brandPage }
    ),
    block(
      "economics.fee.change",
      "When Things Change",
      `Renewal PIP; conversion PIP revisions; transfer / change-of-ownership fees; termination-related obligations; owner-funded reserves—triggered at renewal, repositioning, or exit. Confirm liquidated damages and transfer mechanics in the current agreement.`,
      next(),
      { source: src.development || src.brandPage }
    ),
    block(
      "economics.fee_variability",
      "Fee Variability",
      `Initial vs conversion paths differ; program contribution and loyalty assessments may change by year under agreement terms. Lock the fee exhibit before IC—never extrapolate from a peer hotel's rumor. Geography note: ${pack.feeGeography}`,
      next()
    )
  );

  const whyLines = [
    `Keep Local Product Intent—Add ${program}: retain ${short}'s brand promise while accessing loyalty earn/redeem and parent commercial systems a standalone hotel cannot buy alone.`,
    pack.positioning,
    `Sibling Clarity: diligence fit versus same-parent peers before assuming identical owner use case or fee path.`,
    `Operator & Owner Fit: strongest when the asset can deliver the brand's guest promise with QA discipline—weakest when sponsors want the flag without the product investment.`,
    `Disclosure Discipline: model economics only from current fee exhibits—Factory 2.0 does not invent undisclosed percentages.`,
  ];
  out.push(
    block(
      "overview.why_value",
      "Why Value Is Strongest in These Scenarios",
      whyLines.join("\n"),
      next(),
      { source: src.brandPage || src.development }
    )
  );

  (pack.scenarios || []).forEach((sc, i) => {
    out.push(block(`overview.scenario.${i + 1}`, sc.title, sc.body, next()));
  });

  return out.map((b) => ({
    ...b,
    title: String(b.title || "").replace(/\bAnd\b/g, "&"),
  }));
}

/**
 * Build staged candidate document for a brand pack + live blocks.
 */
export function buildDepthCandidate(pack, liveBlocks) {
  const overlayBlocks = buildDepthGapFillBlocks(pack);
  const assembly = assembleCumulativeProposal(liveBlocks, overlayBlocks, {
    brandId: pack.brandId,
    brandName: pack.brandName,
  });
  const proposedBlocks = assembly.blocks || assembly.proposedBlocks || [];
  const overlayHash = hashPresentationBlocks(overlayBlocks);
  const assembledHash = hashPresentationBlocks(proposedBlocks);
  const standards = evaluateFactoryPermanentStandardsGate({
    blocks: proposedBlocks,
    loyaltyParticipates: true,
    requireLoyaltyProof: true,
  });

  const candidate = {
    contract: "BE_FACTORY_2_0_DEPTH_CANDIDATE_V1",
    candidateVersion: "factory-2-0-depth-batch-five-v1",
    brandId: pack.brandId,
    brandName: pack.brandName,
    slug: pack.slug,
    generatedAt: new Date().toISOString(),
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    researchSpendUsd: pack.researchSpendUsd || 0,
    researchNotes: [
      "Factory 2.0 depth batch — free authoritative sources only ($0 paid).",
      "Exact six Key Benefits & Program Strengths cards staged for participating loyalty.",
      "Fee amounts not invented — disclosure confirmation language pending verified exhibits.",
      "Production Presentation unchanged until exact-candidate founder approval.",
    ],
    sources: pack.sources,
    brandExplorer: { version: 1, blocks: overlayBlocks },
    presentationBlocksHashSha256: overlayHash,
  };

  return {
    pack,
    candidate,
    overlayBlocks,
    proposedBlocks,
    overlayHash,
    assembledHash,
    assembly,
    standards,
  };
}

export function resolvePackOrThrow(brandId) {
  const pack = getDepthPack(brandId);
  if (!pack) throw new Error(`No depth research pack for ${brandId}`);
  return pack;
}
