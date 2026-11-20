/**
 * Build Factory 2.0 depth candidate overlay from a research pack + live CURRENT.
 * Does not invent fee amounts. Enforces exact-six loyalty.proof cards and
 * exact-six Demand Scenario Fit cards (BE-QL-DEMAND-SCENARIOS-SIX-001).
 */
import { assembleCumulativeProposal } from "../be-cumulative-proposal-assembler-v1.js";
import { hashPresentationBlocks } from "../be-founder-approval-publication-v1.js";
import {
  evaluateFactoryPermanentStandardsGate,
  normalizeRegionalAcronymsInHeading,
} from "../be-factory-permanent-loyalty-region-standards-v1.js";
import { toEditorialTitleCase } from "../be-editorial-title-case-v1.js";
import {
  DEMAND_FIT_LABELS,
  DEMAND_SCENARIO_SLOT,
} from "../be-factory-permanent-demand-scenarios-six-v1.js";
import { getDepthPack } from "./be-factory-2-depth-research-packs-v1.js";
import {
  buildEmptyBrandCompletionBlocks,
  buildCurioBarDifferentiatorBodies,
  loadGalleryCardsFromFixture,
  pickScenarioHotelImages,
} from "./be-factory-2-empty-brand-completion-slots-v1.js";
import {
  resolveLoyaltyProgramId,
  buildLoyaltyEliteBlocksFromProgram,
} from "../be-factory-permanent-loyalty-tier-completeness-v1.js";

/** Default six Demand Scenario Fit assessments when pack omits demandScenarios. */
function defaultDemandScenariosForPack(pack) {
  const short = pack.brandName || "This brand";
  const program = pack.loyaltyProgram || "the parent loyalty program";
  const pos = pack.positioning || "brand positioning";
  return [
    {
      title: "Gateway Urban",
      fit: DEMAND_FIT_LABELS.STRONG_FIT,
      explanation: `${short} fits gateway urban demand where ${pos} and ${program} distribution support transient and corporate mixes—underwrite city competitive set, not portfolio averages.`,
    },
    {
      title: "Corporate-Led Urban",
      fit: DEMAND_FIT_LABELS.MODERATE_FIT,
      explanation: `Corporate-led urban accounts can pull ${short} when the product matches account brand ladders; confirm negotiated rates and meeting capacity before assuming contracted volume.`,
    },
    {
      title: "Regional & Secondary Markets",
      fit: DEMAND_FIT_LABELS.MODERATE_FIT,
      explanation: `Secondary markets work when ${short}'s product and fee load clear local ADR reality; diligence source-market seasonality rather than gateway RevPAR comps.`,
    },
    {
      title: "Resort & Coastal Leisure",
      fit: DEMAND_FIT_LABELS.LIMITED_FIT,
      explanation: `Leisure resort fit depends on whether ${short}'s service and amenity model matches resort operating complexity; do not assume urban transient strengths transfer to coastal seasonality.`,
    },
    {
      title: "Conversion & Repositioning",
      fit: DEMAND_FIT_LABELS.STRONG_FIT,
      explanation: `Conversion / repositioning is a core owner path when independent assets seek ${short} affiliation plus ${program} access—underwrite PIP scope and design latitude before fee assumptions.`,
    },
    {
      title: "Highway & Interstate Transient",
      fit: DEMAND_FIT_LABELS.NOT_A_FIT,
      explanation: `Highway / interstate limited-service economics generally sit outside ${short}'s product and fee structure; owners should not force-fit this brand into pure economy transient boxes.`,
    },
  ];
}

function block(slotKey, title, body, sort, extra = {}) {
  const out = {
    slotKey,
    title: normalizeRegionalAcronymsInHeading(title || ""),
    body: body || "",
    sort: sort || 0,
    active: true,
    imageUrl: extra.imageUrl || null,
    externalDisplayStatus: "Internal Preview",
    source: extra.source || "factory-2-0-depth-research",
  };
  if (extra.demandScenarioDescription) {
    out.demandScenarioDescription = extra.demandScenarioDescription;
  }
  return out;
}

export function buildDepthGapFillBlocks(pack) {
  let s = 2000;
  const next = () => ++s;
  const src = pack.sources || {};
  const program = pack.loyaltyProgram;
  const short = pack.brandName;
  const diffBodies = buildCurioBarDifferentiatorBodies(pack);

  const out = [
    // Curio / Ascend Key Differentiators bar — exactly four newline bullets per column
    block(
      "overview.differentiators.identity",
      "Experience & Identity",
      diffBodies.identity,
      next(),
      { source: src.brandPage || src.development }
    ),
    block(
      "overview.differentiators.commercial",
      "Commercial & Distribution",
      diffBodies.commercial,
      next(),
      { source: src.brandPage || src.development }
    ),
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
    // Demand Scenario Fit — exact six (pack.demandScenarios or brand-aware defaults)
    ...(Array.isArray(pack.demandScenarios) && pack.demandScenarios.length === 6
      ? pack.demandScenarios
      : defaultDemandScenariosForPack(pack)
    ).map((row) =>
      block(
        DEMAND_SCENARIO_SLOT,
        row.title,
        row.fit || row.body || DEMAND_FIT_LABELS.MODERATE_FIT,
        next(),
        {
          source: src.brandPage || src.development,
          demandScenarioDescription: row.explanation || row.demandScenarioDescription || "",
        }
      )
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
      {
        source:
          src.bonvoy ||
          src.oneRewards ||
          src.honors ||
          src.worldOfHyatt ||
          src.choicePrivileges ||
          src.wyndhamRewards ||
          src.bestWesternRewards ||
          src.sonestaTravelPass,
      }
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
  ];

  // Elite Tiers — Illustrative Benefits: emit ALL official tiers (never one generic card).
  {
    const programId = resolveLoyaltyProgramId({
      loyaltyProgram: pack.loyaltyProgram || program,
      parentCompany: pack.parentCompany,
      brandName: pack.brandName,
    });
    const eliteBlocks = buildLoyaltyEliteBlocksFromProgram(programId, {
      sortStart: next(),
      source:
        src.bonvoy ||
        src.oneRewards ||
        src.honors ||
        src.worldOfHyatt ||
        src.choicePrivileges ||
        src.wyndhamRewards ||
        src.bestWesternRewards ||
        src.sonestaTravelPass ||
        src.brandPage,
    });
    if (eliteBlocks.length) {
      for (const eb of eliteBlocks) {
        out.push({
          ...eb,
          sort: next(),
          title: normalizeRegionalAcronymsInHeading(eb.title),
        });
      }
    } else if (pack.loyaltyParticipates === false || pack.noLoyaltyProgram) {
      out.push(
        block(
          "loyalty.elite",
          "No Tiered Loyalty Program",
          `${short} does not participate in a multi-tier parent loyalty ladder as a standard hotel affiliation product—or participation is brand-specific. Confirm current recognition treatment with the brand before underwriting loyalty-driven demand.`,
          next()
        )
      );
    } else {
      out.push(
        block(
          "loyalty.elite",
          "Loyalty Participation Unverified",
          `Official elite-tier structure for ${program || "this brand's loyalty program"} is not yet mapped in the Factory canonical registry. Do not invent tiers—block certification until verified.`,
          next()
        )
      );
    }
  }

  out.push(
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
        source:
          src.bonvoy ||
          src.oneRewards ||
          src.honors ||
          src.worldOfHyatt ||
          src.choicePrivileges ||
          src.wyndhamRewards ||
          src.bestWesternRewards ||
          src.sonestaTravelPass,
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

  {
    const scenarioImages = pickScenarioHotelImages(
      process.cwd(),
      pack.slug,
      pack.brandName || "This brand",
      3
    );
    (pack.scenarios || []).forEach((sc, i) => {
      const img = scenarioImages[i];
      out.push(
        block(`overview.scenario.${i + 1}`, sc.title, sc.body, next(), {
          imageUrl: img?.imageUrl || null,
          source: img?.source || undefined,
        })
      );
    });
  }

  // Ten-tab scaffold for brands with empty CURRENT Presentation (Batch 5+).
  if (pack.operatingModel) {
    out.push(
      block(
        "operations.model",
        "Operating Model",
        pack.operatingModel,
        next(),
        { source: src.development || src.brandPage }
      )
    );
    out.push(
      block(
        "operations.owner_lens",
        "Owner Operating Lens",
        `Underwrite ${short} staffing, F&B intensity, and brand QA against this asset's segment—do not import peer-brand operating assumptions from a different ladder rung.`,
        next()
      )
    );
    out.push(
      block(
        "operations.systems",
        "Systems & Brand Standards",
        `Parent PMS/CRS, ${program} enrollment, and brand QA audits apply. Budget conversion training and standards compliance in the PIP / opening plan.`,
        next()
      )
    );
  }

  if (pack.footprint) {
    out.push(
      block("footprint.overview", "Footprint & Growth", pack.footprint, next(), {
        source: src.brandPage || src.news || src.development,
      })
    );
    out.push(
      block(
        "footprint.regions",
        "Regional Presence",
        `Confirm current ${short} openings and pipeline by APAC, EMEA, CALA / LATAM, and North America from parent development disclosures—not stale media counts.`,
        next()
      )
    );
    out.push(
      block(
        "footprint.owner_lens",
        "Owner Footprint Lens",
        `Portfolio scale helps distribution; hotel-level contribution still depends on local competitive set, product delivery, and ${program} mix for this asset.`,
        next()
      )
    );
  }

  {
    // Prefer verified CDN fixture pools over caption-only pack gallery stubs.
    const packGalleryWithImages = (Array.isArray(pack.gallery) ? pack.gallery : []).filter((g) =>
      /^https?:\/\//i.test(String(g.imageUrl || ""))
    );
    let galleryCards = packGalleryWithImages.slice(0, 6).map((g) => ({
      title: g.title,
      body: g.body,
      imageUrl: g.imageUrl,
      source: g.source || src.brandPage,
    }));
    if (galleryCards.length < 6) {
      const fromFx = loadGalleryCardsFromFixture(
        pack._repoRoot || process.cwd(),
        pack.slug,
        short
      );
      if (fromFx.length) galleryCards = fromFx;
    }
    galleryCards.slice(0, 6).forEach((g, i) => {
      const title = /\|\s*\S+/.test(String(g.title || ""))
        ? g.title
        : `${g.title || short} · Brand Photography | International Reference`;
      out.push(
        block(`materials.gallery.${i + 1}`, title, g.body, next(), {
          imageUrl: g.imageUrl || null,
          source: g.source || src.brandPage,
        })
      );
    });
  }

  if (pack.insight) {
    out.push(
      block("insight.headline", "Dealality Insight", pack.insight, next(), {
        source: src.brandPage || src.development,
      })
    );
    out.push(
      block(
        "insight.owner",
        "Owner Takeaway",
        `Use ${short} when the asset can deliver the brand promise and the fee exhibit clears IC—not because the parent portfolio is large.`,
        next()
      )
    );
  }

  if (Array.isArray(pack.standards) && pack.standards.length) {
    out.push(
      block(
        "standards.intro",
        "Brand Standards",
        `Key ${short} product and operating expectations owners should diligence before conversion or new-build IC.`,
        next()
      )
    );
    pack.standards.forEach((st) => {
      out.push(
        block(
          "standards.requirement",
          toEditorialTitleCase(st.title || ""),
          st.body,
          next()
        )
      );
    });
  }

  if (Array.isArray(pack.valueOwnerScenarios) && pack.valueOwnerScenarios.length) {
    out.push(
      block(
        "valueOwners.overview",
        "Value Creation For Owners",
        `Four owner-facing scenarios where ${short} can create value—and where it fails if product or capital cannot match the brand promise.`,
        next()
      )
    );
    pack.valueOwnerScenarios.slice(0, 4).forEach((sc, i) => {
      out.push(block(`valueOwners.scenario.${i + 1}`, sc.title, sc.body, next()));
    });
  }

  // Autograph/Curio slot-parity completion for empty CURRENT brands (and thin overlays).
  // Fills ops flexibility/compliance, footprint regions, economics opening path, overview
  // proofs, value lifecycle, materials gallery (fixture CDN when available), etc.
  // Assembler still refuses weaker overwrite of rich CURRENT content.
  const existingKeys = new Set(out.map((b) => String(b.slotKey || "")));
  const completion = buildEmptyBrandCompletionBlocks(pack, existingKeys, {
    repoRoot: pack._repoRoot || process.cwd(),
    sortStart: s + 1,
  });
  out.push(...completion);

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
    loyaltyParticipates: pack.loyaltyParticipates !== false && !pack.noLoyaltyProgram,
    requireLoyaltyProof: true,
    requireLoyaltyElite: true,
    loyaltyProgram: pack.loyaltyProgram,
    parentCompany: pack.parentCompany,
    brandName: pack.brandName,
    loyaltyProgramId: resolveLoyaltyProgramId({
      loyaltyProgram: pack.loyaltyProgram,
      parentCompany: pack.parentCompany,
      brandName: pack.brandName,
    }),
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
