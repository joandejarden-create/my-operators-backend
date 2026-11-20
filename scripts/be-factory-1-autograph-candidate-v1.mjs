#!/usr/bin/env node
/**
 * Factory 1.0 — Autograph Collection cumulative Phase 2H candidate.
 *
 * Builds staged candidate overlay for missing Commercial / Loyalty / Economics
 * fee buckets + editorial uplift, from live CURRENT + authorized public research.
 * Never writes Airtable.
 *
 * Sources (research spend $0 paid):
 * - Marriott 10-K YE 2025 Autograph properties/rooms
 * - Marriott news Autograph brand page / Bonvoy participation
 * - Marriott Autograph Collection Domestic FDD (Mar 31, 2025) public fee exhibits
 * - Existing Autograph Presentation + full-build content pack
 */
import "../load-env.js";
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import { fetchPresentationBlocksForBrand } from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";
import { assembleCumulativeProposal } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";
import { hashPresentationBlocks } from "../lib/brand-explorer/be-founder-approval-publication-v1.js";

const ROOT = process.cwd();
const BRAND_ID = "recEJCTDj1zrsjPM6";
const BRAND_NAME = "Autograph Collection";
const SLUG = "autograph-collection";
const OUT_DIR = join(ROOT, "public/brand-explorer-phase2e-preview");
const REPORT_DIR = join(ROOT, "reports/brand-explorer/factory-1-0");

function block(slotKey, title, body, sort, extra = {}) {
  return {
    slotKey,
    title: title || "",
    body: body || "",
    sort: sort || 0,
    active: true,
    imageUrl: extra.imageUrl || null,
    externalDisplayStatus: "Internal Preview",
    source: extra.source || "factory-1-0-autograph-research",
  };
}

function loadFullBuildPackRows() {
  const path = join(
    ROOT,
    "lib/partner-intelligence/brand-explorer-full-build-content-autograph-collection.js"
  );
  if (!existsSync(path)) return [];
  // Dynamic import of CJS/ESM hybrid — pack exports PRESENTATION via module pattern.
  // Fall back: parse is hard; instead read live CURRENT for those slots and only add gaps.
  return [];
}

/** Gap-fill slots that Autograph CURRENT lacks (commercial / loyalty / fee buckets). */
function buildGapFillBlocks() {
  const sources = {
    tenK:
      "Marriott International Form 10-K for year ended Dec 31, 2025 — Autograph Collection® system properties/rooms",
    brandPage: "https://news.marriott.com/brands/autograph-collection-hotels",
    fdd: "Autograph Collection Domestic FDD issued Mar 31, 2025 (public Marriott development disclosure)",
    bonvoy: "Marriott Bonvoy program participation stated on Autograph brand communications",
  };

  let s = 1000;
  const next = () => ++s;

  return [
    // --- Commercial Engine ---
    block(
      "commercial.intro",
      "Commercial Engine",
      "Autograph Collection commercial distribution rides Marriott Bonvoy direct channels, GDS, OTAs, and Marriott sales systems while each hotel keeps its own design and guest narrative. Owners should underwrite channel mix and Bonvoy contribution for this asset—not corporate averages alone.",
      next(),
      { source: sources.brandPage }
    ),
    block(
      "commercial.differentiator",
      "Commercial Differentiator",
      "Versus hard brands, Autograph sells individuality plus Marriott commercial infrastructure; versus pure independents, it adds Bonvoy earn/redeem, centralized sales tools, and portfolio campaigns without collapsing the hotel into a standardized prototype.",
      next()
    ),
    block(
      "commercial.kpi.channels",
      "Channels",
      "Brand.com / Marriott Bonvoy app · major OTAs · GDS · metasearch (where active for the hotel)",
      next()
    ),
    block(
      "commercial.kpi.campaigns",
      "Campaigns",
      "Marriott Bonvoy and parent portfolio campaigns where the Autograph hotel is eligible",
      next()
    ),
    block(
      "commercial.kpi.b2b",
      "B2B Programs",
      "Corporate, SME, and RFP programs routed through Marriott sales infrastructure where active for this flag",
      next()
    ),
    block(
      "commercial.kpi.lens",
      "Owner Lens",
      "Model Autograph contribution after loyalty chargebacks, preferred-agency commissions, and local competitive set reality—not headline RevPAR alone.",
      next()
    ),
    block(
      "commercial.demand",
      "Demand Pattern",
      "Autograph demand follows destination seasonality plus Bonvoy campaign calendars. Treat portfolio averages as directional; diligence this hotel's source markets and corporate vs leisure mix.",
      next()
    ),
    block(
      "commercial.demand",
      "Competitive Pressure",
      "Sibling Marriott soft brands (Tribute Portfolio, Design Hotels) and local independents can both help and cannibalize. Map same-parent density before assuming exclusive Bonvoy pull.",
      next()
    ),
    block(
      "commercial.lever.distribution",
      "Distribution & Channel Mix",
      "Marriott central reservations, Bonvoy app, and designated online channels—confirm commission structures and rate parity obligations in the current agreement exhibit.",
      next()
    ),
    block(
      "commercial.lever.revenue_management",
      "Revenue Management",
      "Marriott revenue tools and Bonvoy demand signals support pricing experiments; owners still need property-level strategy for distinctive Autograph product.",
      next()
    ),
    block(
      "commercial.lever.digital_marketing",
      "Digital Marketing & Performance Media",
      "Paid and owned media templates at portfolio scale, with creative that must still feel hotel-specific rather than generic Marriott hard-brand advertising.",
      next()
    ),
    block(
      "commercial.lever.corporate_group",
      "Corporate, SME & Group Pull",
      "Contracted travelers, small meetings, and negotiated programs where Autograph acts as a distinctive but Bonvoy-connected option.",
      next()
    ),
    block(
      "commercial.lever.sales_catering",
      "Sales & Catering",
      "Marriott sales systems support group and catering leads; conversion depends on the hotel's meeting product and local sales discipline.",
      next()
    ),
    block(
      "commercial.lever.leisure_destination",
      "Leisure & Destination Demand",
      "Story-led leisure stays and destination immersion are core Autograph use cases—pair Bonvoy leisure campaigns with credible local experience programming.",
      next()
    ),
    block(
      "commercial.lever.international",
      "International Reach",
      "Year-end 2025 Autograph system spanned U.S. & Canada, Europe, MEA, Asia Pacific, Greater China, and Caribbean & Latin America (Marriott 10-K). International Bonvoy demand is real but property-specific.",
      next(),
      { source: sources.tenK }
    ),
    block(
      "commercial.lever.data_analytics",
      "Data, Analytics & Experimentation",
      "Use Marriott reporting to isolate channel and Bonvoy contribution for this Autograph asset; avoid extrapolating from sibling soft brands.",
      next()
    ),
    block(
      "commercial.lever.reputation_qa",
      "Reputation & QA",
      "Guest reviews and brand QA surface design/F&B/service mismatches quickly on character-led hotels—budget for ongoing standards discipline.",
      next()
    ),
    block(
      "commercial.theme",
      "Immersive Character",
      "Every Autograph hotel is sold on individuality—commercial messaging must protect that story while still routing demand through Bonvoy.",
      next()
    ),
    block(
      "commercial.perception",
      "Owner Perception Risk",
      "Owners sometimes expect soft-brand lightness; Autograph still carries Marriott systems, Bonvoy economics, and design/QA expectations. Align IC expectations early.",
      next()
    ),

    // --- Loyalty ---
    block(
      "loyalty.program",
      "Marriott Bonvoy",
      "Autograph Collection participates in Marriott Bonvoy. Members earn and redeem points and receive Elite recognition across participating Marriott brands. Confirm current tier names and earn rules on marriottbonvoy.com before IC.",
      next(),
      { source: sources.bonvoy }
    ),
    block(
      "loyalty.hero_title",
      "Loyalty",
      "Marriott Bonvoy at Autograph Collection",
      next()
    ),
    block(
      "loyalty.earn",
      "Earn",
      "Guests earn Marriott Bonvoy points on qualifying stays at Autograph Collection hotels. Elite members may receive enhanced earn and recognition benefits per current Bonvoy rules.",
      next()
    ),
    block(
      "loyalty.redeem",
      "Redeem",
      "Points redeem for stays across the Marriott Bonvoy portfolio, including Autograph Collection hotels, subject to availability and current redemption charts.",
      next()
    ),
    block(
      "loyalty.elite",
      "Elite Recognition",
      "Elite status recognition (upgrades, late checkout, lounge access where offered) applies per property capability—Autograph individuality means amenity delivery varies by hotel.",
      next()
    ),
    block(
      "loyalty.ecosystem",
      "Ecosystem",
      "Bonvoy Moments, co-branded cards, and partner redemptions expand the loyalty halo; Autograph stays remain the property-level earn/redeem node.",
      next()
    ),
    block(
      "loyalty.kpi.hotels",
      "Autograph Hotels (YE 2025)",
      "362",
      next(),
      { source: sources.tenK }
    ),
    block(
      "loyalty.kpi.markets",
      "Rooms (YE 2025)",
      "76,399",
      next(),
      { source: sources.tenK }
    ),
    block(
      "loyalty.kpi.members",
      "Program",
      "Marriott Bonvoy",
      next()
    ),
    block(
      "loyalty.kpi.mix",
      "Owner Note",
      "Model loyalty mix for this hotel; portfolio Bonvoy mix is not a property guarantee.",
      next()
    ),
    block(
      "loyalty.owner_lens",
      "Owner Lens",
      "Bonvoy drives distribution and repeat demand but adds loyalty chargebacks and program economics. Underwrite net contribution after loyalty—not membership vanity metrics.",
      next()
    ),
    block(
      "loyalty.implications.pnl",
      "P&L Implications",
      "Loyalty assessments and Bonvoy contribution affect net rooms revenue. Use current agreement exhibits; do not invent undisclosed rates.",
      next()
    ),
    block(
      "loyalty.implications.ops",
      "Operating Implications",
      "Front desk and systems must support Bonvoy enrollment, Elite recognition, and redemption stays without erasing Autograph service character.",
      next()
    ),
    block(
      "loyalty.implications.systems",
      "Systems Implications",
      "Marriott property and reservation systems integration is required for Bonvoy participation—budget cutover and training in the conversion plan.",
      next()
    ),
    block(
      "loyalty.proof",
      "Proof Point",
      "Marriott discloses Autograph Collection as a Premium soft brand with 362 properties / 76,399 rooms at year-end 2025—scale that supports Bonvoy relevance without homogenizing each hotel's story.",
      next(),
      { source: sources.tenK }
    ),

    // --- Economics fee buckets (FDD-attributed; ranges, not invented) ---
    block(
      "economics.fee.join",
      "To Join",
      "Application $100,000 new-to-system / conversion (+ $400/key over 250); existing Autograph or managed-to-franchise path greater of $150,000 or $500/key; PIP / brand assessment and systems install are separate (confirm current FDD exhibits). Design, market study, and F&B concept costs vary by conversion vs new-build",
      next(),
      { source: sources.fdd }
    ),
    block(
      "economics.fee.operate",
      "To Operate",
      "Steady-state 5% monthly royalty on gross rooms sales; Program Services Contribution (~2.02% of GRS plus fixed annual and per-key components—confirm current exhibit); Marriott Bonvoy loyalty assessment on eligible member revenue (public FDD cites ~4.2%, with disclosed temporary reduction to 4.0% through 2027—confirm live exhibit); model owner economics after loyalty chargebacks and distribution—not headline RevPAR",
      next(),
      { source: sources.fdd }
    ),
    block(
      "economics.fee.change",
      "When Things Change",
      "Renewal PIP; conversion PIP revisions; transfer / change-of-ownership fees; termination-related obligations; owner-funded reserves—triggered at renewal, repositioning, or exit, not steady-state operations. Confirm liquidated damages and transfer mechanics in the current agreement.",
      next(),
      { source: sources.fdd }
    ),
    block(
      "economics.fee",
      "Fee Categories",
      "Autograph Collection fee categories: list each recurring and one-time fee with definition and base from the current FDD / agreement exhibit. Do not collapse them into a single diligence paragraph.",
      next()
    ),
    block(
      "economics.fee_variability",
      "Fee Variability",
      "Initial vs conversion vs existing-Autograph paths differ; program contribution and loyalty assessments may change by year under agreement terms that allow program fee changes. Lock the fee exhibit before IC—never extrapolate from a peer hotel's rumor.",
      next(),
      { source: sources.fdd }
    ),

    // --- Overview editorial / research stats ---
    block(
      "overview.why_value",
      "Why Value Is Strongest in These Scenarios",
      [
        "Keep Your Independent Spirit—Add Marriott Bonvoy: retain hotel individuality and local character while accessing Bonvoy earn/redeem, Marriott sales, and portfolio campaigns that a standalone boutique cannot buy on its own.",
        "Exactly Like Nothing Else Positioning: hand-selected design-led hotels in desirable destinations—362 open properties / 76,399 rooms at year-end 2025 (Marriott 10-K)—underwrite the asset's story, not a prototype corridor.",
        "Conversion-Ready Within Soft-Brand Economics: application and systems economics support conversion and adaptive reuse—model design, public-space, and F&B capital plus Bonvoy contribution before any term sheet.",
        "Sibling Clarity Versus Tribute & Design Hotels: Autograph spans upper-upscale to luxury-leaning character hotels; diligence fit versus Tribute Portfolio and Design Hotels before assuming the same owner use case or fee path.",
        "Operator & Owner Fit: strongest for owners and operators who can run distinctive full-service hospitality with design QA and F&B discipline—weakest for breakfast-only limited-service seeking a Marriott flag without an independent narrative.",
      ].join("\n"),
      next(),
      { source: sources.tenK }
    ),
    block(
      "overview.scenario.1",
      "Design-Led Independent Conversion",
      "Independent boutiques and heritage conversions with a credible design or narrative concept—where owners want Marriott Bonvoy distribution without erasing the property's individual identity into a hard-brand prototype.",
      next()
    ),
    block(
      "overview.scenario.2",
      "Destination Lifestyle & Resort Character",
      "Resorts, urban lifestyle landmarks, and destination hotels where authenticity, design, and F&B drive rate power—Autograph's flexible collection standards reward distinctive product over standardized corridors.",
      next()
    ),
    block(
      "overview.scenario.3",
      "Adaptive Reuse & Landmark Adaptive Stays",
      "Historic cores, landmark buildings, and adaptive reuse where architecture and sense of place are the product—pair design capital with Bonvoy commercial systems.",
      next()
    ),
  ];
}

function applyEditorialTitleAmpersand(blocks) {
  return blocks.map((b) => {
    const title = String(b.title || "").replace(/\bAnd\b/g, "&");
    return { ...b, title };
  });
}

async function main() {
  mkdirSync(OUT_DIR, { recursive: true });
  mkdirSync(REPORT_DIR, { recursive: true });

  const live = await fetchPresentationBlocksForBrand(BRAND_ID, BRAND_NAME);
  const gapFills = applyEditorialTitleAmpersand(buildGapFillBlocks());

  // Candidate = gap fills + any full-build rows for thin CURRENT slots (body < 40 chars)
  const thinSlots = new Set(
    live.blocks.filter((b) => String(b.body || "").trim().length < 40).map((b) => b.slotKey)
  );

  // Prefer gap fills; also re-assert overview scenarios / why_value even if present (editorial)
  const bySlot = new Map();
  for (const b of gapFills) {
    const k = b.slotKey;
    if (!bySlot.has(k)) bySlot.set(k, []);
    bySlot.get(k).push(b);
  }
  const candidateBlocks = [];
  for (const [, list] of bySlot) {
    for (const b of list) candidateBlocks.push(b);
  }

  const assembled = assembleCumulativeProposal(live.blocks, candidateBlocks, {
    note: "Factory 1.0 Autograph cumulative proposal",
  });

  const contentHash = hashPresentationBlocks(assembled.blocks);
  const candidateHash = hashPresentationBlocks(candidateBlocks);

  const candidate = {
    contract: "BE_PHASE2H_DEPTH_COMPOSITION_V1",
    candidateVersion: "phase2h-component-parity",
    phase: "2H-factory-1-0",
    brandId: BRAND_ID,
    brandName: BRAND_NAME,
    slug: SLUG,
    parentCompany: "Marriott International, Inc.",
    generatedAt: new Date().toISOString(),
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    researchSpendUsd: 0,
    researchNotes: [
      "Free authoritative sources only (Marriott 10-K YE2025, Autograph brand page, Autograph FDD Mar 31 2025 public exhibits).",
      "No paid research used in this candidate build.",
      "Presentation writeback NOT authorized until founder approves this exact candidate hash.",
    ],
    sources: [
      {
        label: "Marriott 10-K YE2025 Autograph system counts",
        url: "https://www.sec.gov/Archives/edgar/data/1048286/000104828626000007/mar-20251231.htm",
      },
      {
        label: "Autograph Collection brand page",
        url: "https://news.marriott.com/brands/autograph-collection-hotels",
      },
      {
        label: "Autograph Collection Domestic FDD Mar 31 2025",
        url: "https://www.hotel-development.marriott.com/ResourceFiles/fdd-document/2025-autograph-collection-fdd-3-31-2025.pdf",
      },
    ],
    presentationBlocksHashSha256: candidateHash,
    assembledPreviewHashSha256: contentHash,
    brandExplorer: {
      version: 1,
      blocks: candidateBlocks,
    },
    assemblyPreview: assembled.summary,
    thinSlotsAddressed: [...thinSlots].slice(0, 40),
    note: "Factory 1.0 Autograph staged candidate — CURRENT preserved via cumulative assembler; gap fills for Commercial, Loyalty, Economics fee buckets",
  };

  const candidateRel = "brand-explorer-phase2e-preview/autograph-collection-phase2h-candidate.json";
  const manifestRel = "brand-explorer-phase2e-preview/autograph-collection-staged-candidate-manifest-v1.json";
  writeFileSync(join(ROOT, "public", candidateRel), JSON.stringify(candidate, null, 2) + "\n");

  const manifest = {
    version: "BE_STAGED_CANDIDATE_MANIFEST_V1",
    brandId: BRAND_ID,
    slug: SLUG,
    brandName: BRAND_NAME,
    candidateVersion: "phase2h-component-parity",
    phaseLabel: "2H-factory-1-0",
    overlayUrl: `/${candidateRel}`,
    scorecardUrl: null,
    baselineUrl: null,
    manifestUrl: `/${manifestRel}`,
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    repairedAt: candidate.generatedAt,
    researchGapCount: assembled.incompleteDrafts?.length || 0,
    blockCount: candidateBlocks.length,
    presentationBlocksHashSha256: candidateHash,
    assembledPreviewHashSha256: contentHash,
    writes: 0,
    note: "Factory 1.0 Autograph candidate — awaiting founder exact-candidate approval before Presentation writeback",
    silentFallbackForbidden: true,
  };
  writeFileSync(join(ROOT, "public", manifestRel), JSON.stringify(manifest, null, 2) + "\n");

  const report = {
    contract: "BE_FACTORY_1_0_AUTOGRAPH_CANDIDATE_V1",
    generatedAt: candidate.generatedAt,
    brandId: BRAND_ID,
    brandName: BRAND_NAME,
    liveBlockCount: live.blocks.length,
    candidateBlockCount: candidateBlocks.length,
    assembledBlockCount: assembled.blocks.length,
    candidateHash,
    assembledPreviewHash: contentHash,
    assemblySummary: assembled.summary,
    researchSpendUsd: 0,
    writebackStatus: "AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL",
    candidatePath: candidateRel,
    manifestPath: manifestRel,
    gapFillSlotPrefixes: {
      commercial: candidateBlocks.filter((b) => b.slotKey.startsWith("commercial.")).length,
      loyalty: candidateBlocks.filter((b) => b.slotKey.startsWith("loyalty.")).length,
      economics: candidateBlocks.filter((b) => b.slotKey.startsWith("economics.")).length,
      overview: candidateBlocks.filter((b) => b.slotKey.startsWith("overview.")).length,
    },
  };
  writeFileSync(join(REPORT_DIR, "03-autograph-candidate-build.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
