/**
 * Brand Explorer Active Profile Copy Governance config v35B.
 *
 * Brand-specific slot rewrites — generic process, brand-specific output.
 * Imported by copy-governance-builder; not interchangeable boilerplate.
 */
import { WOODSPRING_BACKFILL } from "./brand-explorer-woodspring-presentation-cleanup-backfill-writer.js";
import { EVERHOME_BACKFILL } from "./brand-explorer-everhome-presentation-cleanup-writer.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import {
  AFFILIATION_COPY_MODES,
  buildLifestyleCopyGovernanceConfig,
} from "./brand-explorer-lifestyle-affiliation-copy-governance.js";

export const COPY_GOVERNANCE_VERSION = "v35B";

/** Copy that could apply to any brand without modification — blocked as rewrite output. */
export const GENERIC_BOILERPLATE_PATTERNS = Object.freeze([
  /owners should evaluate brand fit, market demand, and operating model/i,
  /owners should evaluate market fit, operating model, and competitive context/i,
  /evaluate brand fit and market demand during diligence/i,
  /owners should review brand standards and operating requirements/i,
  /confirm during owner diligence without brand-specific context/i,
]);

export const COPY_SANITIZE_REPLACEMENTS = Object.freeze([
  { re: /\bowner should confirm in (the )?fdd\b/gi, replace: "owners should validate during commercial model review" },
  { re: /\bverify with (the )?fdd\b/gi, replace: "confirm during owner diligence" },
  { re: /\bconfirm in (the )?fdd\b/gi, replace: "confirm during owner diligence" },
  { re: /\bconfirm choice privileges[^.]*item 19[^.]*\./gi, replace: "Review Choice Privileges participation during owner diligence." },
  { re: /\bfranchise disclosure document\b/gi, replace: "commercial model review materials" },
  { re: /\bfranchise disclosure\b/gi, replace: "commercial model review" },
  { re: /\bitem\s*19\b/gi, replace: "operating economics review" },
  { re: /\bitem\s*20\b/gi, replace: "agreement terms review" },
  { re: /\bperformance representation\b/gi, replace: "operating performance considerations" },
  { re: /\bconfirm fees\b/gi, replace: "fee structure diligence" },
  { re: /\bconfirm flag\b/gi, replace: "brand participation diligence" },
  { re: /\bestimated contribution\b/gi, replace: "loyalty channel contribution" },
  { re: /\bnet contribution\b/gi, replace: "loyalty channel mix" },
  { re: /\bfee stack\b/gi, replace: "fee components" },
  { re: /\brooms from loyalty\b/gi, replace: "loyalty-driven room nights" },
  { re: /\brevpar\b/gi, replace: "weekly rate positioning" },
  { re: /\badr\b/gi, replace: "weekly rate" },
  { re: /\bactive property page\b/gi, replace: "official property positioning" },
  { re: /\bconsumer site\b/gi, replace: "official brand positioning" },
  { re: /\bconsumer path\b/gi, replace: "guest booking channels" },
  { re: /\bbooking path\b/gi, replace: "distribution channels" },
  { re: /\bsource[- ]capture\b/gi, replace: "reference review" },
  { re: /\bsource data\b/gi, replace: "reference materials" },
  { re: /\bmetadata\b/gi, replace: "profile details" },
  { re: /\bextraction\b/gi, replace: "reference review" },
  { re: /\bfdd\b/gi, replace: "owner diligence materials" },
  { re: /\(consumer site,\s*14 programs evaluated\)/gi, replace: "(third-party ranking, 14 programs evaluated)" },
]);

function suburbanSlotRewrites() {
  return {
    "overview.featured_application": {
      title: "Extended-Stay Studio Owner Fit",
      body:
        "Suburban Studios is a Choice extended-stay brand oriented to value-conscious weekly and longer-stay guests with studio-style rooms and kitchenette expectations. Owners evaluating Suburban should diligence local economy extended-stay demand, competitive weekly-stay supply, required studio prototype fit, and Choice platform participation.",
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "overview.portfolio_context": {
      title: "Extended-Stay Portfolio Context",
      body:
        "Suburban Studios sits within Choice Hotels' extended-stay portfolio—positioned for value-oriented weekly stays with studio-style guestrooms, kitchenette-equipped units, and lean operating models suited to economy extended-stay corridors.",
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    portfolio_context: {
      title: "Choice Extended-Stay Context",
      body:
        "Suburban participates in Choice's extended-stay platform alongside WoodSpring and Everhome—owners compare studio prototype fit, weekly-rate positioning, and operating simplicity when selecting an economy extended-stay affiliation path.",
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "overview.scenario.1": {
      title: "Extended-Stay Studio Conversion",
      body:
        "Economy extended-stay studios for contractors, project crews, and temporary housing—Suburban fits when owners need weekly-rate positioning, in-room kitchenettes, and Choice Privileges distribution without full-service operating load.",
      sourceRefs: ["consumerUrl"],
    },
    "overview.scenario.2": {
      title: "Weekly Corporate Demand Corridor",
      body:
        "Weekly-stay corridors near employment centers, hospitals, or training campuses—Suburban works when demand is project-driven and owners can align housekeeping and kitchenette FF&E to Suburban prototype bands.",
      sourceRefs: ["consumerUrl"],
    },
    "overview.scenario.3": {
      title: "Kitchenette Conversion From Select-Service",
      body:
        "Kitchenette conversions from older select-service or independent extended-stay formats—Suburban competes when room modules support cooking facilities and owners want Choice scale without upscale public-space requirements.",
      sourceRefs: ["consumerUrl"],
    },
    "valueOwners.scenario.1": {
      title: "Extended-Stay Studio Conversion",
      body:
        "Independent or economy extended-stay assets needing weekly-rate positioning and in-room kitchenettes—Suburban Studios fits when owners want Choice Privileges distribution without full-service F&B or daily housekeeping intensity.",
      sourceRefs: ["consumerUrl"],
    },
    "valueOwners.scenario.2": {
      title: "Weekly Corporate Demand Corridor",
      body:
        "Markets with project-based crews, training rotations, or insurance housing—Suburban works when weekly rates support kitchenette operations and owners can staff lean extended-stay housekeeping within Choice standards.",
      sourceRefs: ["consumerUrl"],
    },
    "valueOwners.scenario.3": {
      title: "Conversion From Independent Extended Stay",
      body:
        "Mature extended-stay properties needing affiliation lift—Suburban competes when room modules already support kitchenettes and owners need CRS, loyalty, and revenue tools without repositioning to select-service economics.",
      sourceRefs: ["consumerUrl"],
    },
    "loyalty.ecosystem": {
      title: "Choice Privileges Ecosystem",
      body:
        "Suburban Studios participates in Choice Privileges, connecting extended-stay guests to enterprise and transient demand across the Choice network. Owners should evaluate loyalty contribution and channel mix during commercial model review—not as a performance guarantee.",
      sourceRefs: ["consumerUrl"],
    },
    "loyalty.proof": {
      title: "Loyalty Demand Context",
      body:
        "Choice Privileges supports guest recognition and booking channels across the Choice platform. Owners evaluating Suburban should assess loyalty mix and distribution reach during underwriting without treating program scale as property-level guidance.",
      sourceRefs: ["consumerUrl"],
    },
    "loyalty.redeem": {
      title: "Redemption & Channel Mix",
      body:
        "Extended-stay owners evaluating Suburban should understand how Choice Privileges redemption and channel participation affect booking economics. Validate channel and loyalty mix assumptions during owner diligence.",
      sourceRefs: ["consumerUrl"],
    },
    "loyalty.implications.pnl": {
      title: "Loyalty P&L Considerations",
      body:
        "Loyalty-driven bookings can supplement Suburban extended-stay demand but vary by market and operator. Owners should review channel assumptions during commercial model review without treating program participation as forecast guidance.",
      sourceRefs: ["consumerUrl"],
    },
    "loyalty.owner_lens": {
      title: "Owner Loyalty Lens",
      body:
        "Suburban owners should map how Choice Privileges participation affects weekly-stay channel mix and corporate contract capture—validate assumptions during diligence, not as disclosed performance guidance.",
      sourceRefs: ["consumerUrl"],
    },
    "economics.intro": {
      title: "Economics & Owner Considerations",
      body:
        "Suburban extended-stay underwriting should focus on weekly rate positioning, housekeeping intensity, and conversion scope. Owners should complete commercial model review with qualified advisors—Dealality does not present specific fee amounts or performance representations.",
      sourceRefs: ["developmentUrl"],
    },
    "economics.fee": {
      title: "Fee Structure Diligence",
      body:
        "Owners evaluating Suburban should validate franchise and operating fee components during commercial model review. Confirm economics with Choice development counsel during owner diligence.",
      sourceRefs: ["developmentUrl"],
    },
    "economics.fee.operate": {
      title: "Operating Cost Considerations",
      body:
        "Suburban operating economics depend on weekly-rate positioning, kitchenette FF&E, and lean housekeeping models. Owners should validate operating assumptions during commercial model review.",
      sourceRefs: ["developmentUrl"],
    },
    "economics.fee.join": {
      title: "Initial Investment Considerations",
      body:
        "Joining costs for Suburban vary by conversion scope, market, and studio prototype alignment. Owners should review development and conversion estimates during diligence with qualified advisors.",
      sourceRefs: ["developmentUrl"],
    },
    "economics.kpi.fee_stack": {
      title: "Fee Components Diligence",
      body:
        "Owners evaluating Suburban should map franchise, marketing, and technology fee components during underwriting. Dealality summarizes considerations only—confirm details with Choice development representatives.",
      sourceRefs: ["developmentUrl"],
    },
    "operations.operator_compat.fit": {
      title: "Operator Compatibility",
      body:
        "Suburban suits operators experienced in weekly billing, extended-stay housekeeping, and economy studio operations within Choice extended-stay standards. Owners should confirm operator depth and prototype execution during diligence.",
      sourceRefs: ["developmentUrl"],
    },
    "operations.model.systems_integration": {
      title: "Operating Systems Integration",
      body:
        "Suburban properties rely on Choice CRS, revenue tools, and extended-stay operating standards. Owners should validate systems integration and operating model fit during commercial model review.",
      sourceRefs: ["developmentUrl"],
    },
    "commercial.kpi.lens": {
      title: "Commercial KPI Lens",
      body:
        "Suburban commercial diligence should emphasize weekly occupancy drivers, project-housing demand, and economy extended-stay competitive supply—not published performance metrics. Owners should underwrite locally with qualified advisors.",
      sourceRefs: ["consumerUrl"],
    },
    "commercial.lever.data_analytics": {
      title: "Revenue & Analytics Tools",
      body:
        "Choice platform tools support Suburban owners on pricing and channel management. Owners should evaluate how analytics and CRS participation fit weekly-stay operating models during diligence.",
      sourceRefs: ["consumerUrl"],
    },
    "hero.benefit_zones": {
      title: "Owner Benefit Zones",
      body:
        "Suburban Studios offers Choice extended-stay affiliation for value-oriented weekly stays—owners diligence studio prototype fit, economy extended-stay competitive supply, and Choice platform participation during brand selection.",
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "insight.summary": {
      title: "Owner Planning Summary",
      body:
        "Suburban Studios fits owner diligence when value-oriented extended-stay demand, studio-style room expectations, and Choice distribution support align with market underwriting—confirm prototype and operating assumptions during commercial model review.",
      sourceRefs: ["consumerUrl"],
    },
    "overview.differentiators.commercial": {
      title: "Commercial Differentiators",
      body:
        "Suburban differentiates through economy extended-stay studio positioning, kitchenette-equipped units, and Choice Privileges distribution—owners compare weekly-stay corridors, operating simplicity, and competitive extended-stay supply during diligence.",
      sourceRefs: ["consumerUrl"],
    },
    "overview.proof.5": {
      title: "Extended-Stay Platform Context",
      body:
        "Suburban participates in Choice's extended-stay growth strategy with studio-oriented product for weekly-stay demand. Owners should validate local extended-stay supply and operating model fit—not treat platform context as property-level performance guidance.",
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "overview.scenarios": {
      title: "Value Scenario Overview",
      body:
        "Suburban value scenarios focus on studio conversion fit, weekly corporate demand corridors, and Choice platform context—owners compare economy extended-stay supply, kitchenette prototype requirements, and operating model discipline during diligence.",
      sourceRefs: ["consumerUrl"],
    },
    "footprint.growth.narrative": {
      title: "Geographic Footprint",
      body:
        "Suburban maintains a U.S.-oriented presence within the Choice extended-stay portfolio. Owners should compare market fit, competitive economy extended-stay supply, and corridor demand drivers when evaluating affiliation.",
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "footprint.growth_editorial": {
      title: "Growth Context",
      body:
        "Suburban's footprint reflects Choice's value extended-stay platform strategy in North America. Owners should validate local weekly-stay demand and studio prototype alignment during market diligence.",
      sourceRefs: ["developmentUrl"],
    },
    "footprint.editorial": {
      title: "Footprint Context",
      body:
        "Suburban Studios targets economy extended-stay corridors with studio-style product. Owners should diligence competitive weekly-stay supply and operating model fit in local markets.",
      sourceRefs: ["consumerUrl"],
    },
    "standards.intro": {
      title: "Brand Standards Overview",
      body:
        "Suburban standards emphasize studio-style extended-stay rooms, kitchenette-equipped units, and economy extended-stay operating models. Owners should review brand participation requirements and prototype specifications during diligence.",
      sourceRefs: ["developmentUrl"],
    },
    "materials.file": {
      title: "Owner Materials",
      body:
        "Suburban development and operating materials support owner diligence on studio prototype fit, weekly-stay positioning, and Choice extended-stay standards. Review with qualified advisors during commercial model review.",
      sourceRefs: ["developmentUrl"],
    },
    "economics.opening.financials": {
      title: "Opening Financial Planning",
      body:
        "Suburban Studios opening diligence should separate owner capex, kitchenette FF&E, technology implementation, and working capital through ramp. Owners should map franchise, marketing, and technology fee components during underwriting—not generic fee labels. Confirm opening and stabilized assumptions with Choice development representatives.",
      sourceRefs: ["developmentUrl"],
    },
    "overview.proof.3": {
      title: "Economy Extended-Stay Positioning",
      body:
        "Suburban Studios targets value-oriented weekly and longer-stay guests—owners compare economy extended-stay competitive supply, studio prototype fit, and Choice platform participation when evaluating affiliation alongside other Choice extended-stay brands.",
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "overview.proof.4": {
      title: "Kitchen & Utility Economics",
      body:
        "Suburban studio economics depend on kitchenette wear, utility recovery, and extended-stay housekeeping cadence—owners should underwrite appliance lifecycle and utility pass-through alongside weekly-rate positioning during diligence.",
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "overview.proof_operator": {
      title: "Operator Fit",
      body:
        "Suburban suits extended-stay operators experienced in kitchenette operations, weekly billing, and lean housekeeping at economy extended-stay weekly-rate positioning within Choice standards.",
      sourceRefs: ["developmentUrl"],
    },
    "loyalty.kpi.mix": {
      title: "Loyalty Mix Considerations",
      body:
        "Loyalty-driven bookings can supplement Suburban extended-stay demand but vary by market and operator. Owners evaluating Suburban should review channel and loyalty mix assumptions during commercial model review without treating program participation as forecast guidance.",
      sourceRefs: ["consumerUrl"],
    },
    "valueOwners.watchouts": {
      title: "Owner Watchouts",
      body:
        "Suburban owners should flag markets that cannot support economy extended-stay weekly rates or required kitchenette amenity stacks; conversion PIP scope misaligned with building constraints; OTA-heavy mixes without channel diligence; and operator tier mismatch that surfaces in QA before financial underperformance.",
      sourceRefs: ["developmentUrl"],
    },
  };
}

function mapBackfillToGovernance(backfill, sourceRefs = ["consumerUrl", "developmentUrl"]) {
  const out = {};
  for (const [slotKey, pkg] of Object.entries(backfill)) {
    if (!pkg?.body && !pkg?.title) continue;
    out[slotKey] = {
      title: pkg.title || null,
      body: pkg.body,
      sourceRefs,
    };
  }
  return out;
}

export const COPY_GOVERNANCE_BY_BRAND = Object.freeze({
  "suburban-studios": {
    brandName: "Suburban Studios",
    parentPlatform: "Choice Hotels",
    segment: "economy extended-stay studio",
    positioningPillars: [
      "value-oriented weekly and longer-stay demand",
      "studio-style rooms with kitchenette expectations",
      "Choice Privileges and Choice extended-stay platform context",
      "economy / midscale extended-stay competitive positioning",
      "conversion and new-build relevance where prototype supports kitchenettes",
    ],
    founderNotes: [
      "Use U.S. property examples with explicit labeling when CALA examples unavailable.",
      "Never surface FDD, item 19, ADR, or net contribution language.",
    ],
    developmentUrl:
      "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/suburban-studios",
    slotRewrites: suburbanSlotRewrites(),
    founderQueueResolutions: {
      "economics.opening.financials": {
        strategy: "rewrite",
        reason: "secondary_economics_slot_brand_specific_diligence",
      },
      "overview.proof.3": { strategy: "rewrite", reason: "proof_card_adr_removal" },
      "overview.proof.4": { strategy: "rewrite", reason: "proof_card_revpar_removal" },
      "overview.proof_operator": { strategy: "rewrite", reason: "operator_proof_adr_removal" },
      "insight.similar": {
        strategy: "hide",
        reason: "competitor_ihg_comparison_not_owner_safe",
      },
      "loyalty.kpi.mix": {
        strategy: "rewrite",
        reason: "remove_rooms_from_loyalty_percentage",
        fallbackStrategy: "hide",
      },
      "valueOwners.watchouts": { strategy: "rewrite", reason: "watchouts_adr_removal" },
      "loyalty.proof": { strategy: "rewrite", reason: "campaign_scale_consumer_site_removal" },
    },
    founderQueueTargets: [
      {
        recordId: "recAGBgGgv2hT0Ux1",
        slotKey: "economics.opening.financials",
        strategy: "rewrite",
        reason: "secondary_economics_slot_brand_specific_diligence",
      },
      {
        recordId: "recARhIhZtw0DQEnC",
        slotKey: "overview.proof.3",
        strategy: "rewrite",
        reason: "proof_card_adr_removal",
      },
      {
        recordId: "recUlSKh8i2HnIZv9",
        slotKey: "overview.proof.4",
        strategy: "rewrite",
        reason: "proof_card_revpar_removal",
      },
      {
        recordId: "recCld2KPx78qehp2",
        slotKey: "overview.proof_operator",
        strategy: "rewrite",
        reason: "operator_proof_adr_removal",
      },
      {
        recordId: "recHnpkJMbV1A1qCV",
        slotKey: "insight.similar",
        strategy: "hide",
        reason: "competitor_ihg_comparison_not_owner_safe",
      },
      {
        recordId: "recU8R1WXLcSvs2QC",
        slotKey: "loyalty.kpi.mix",
        strategy: "rewrite",
        reason: "remove_rooms_from_loyalty_percentage",
        fallbackStrategy: "hide",
      },
      {
        recordId: "recgCuvvQROhGGYgW",
        slotKey: "valueOwners.watchouts",
        strategy: "rewrite",
        reason: "watchouts_adr_removal",
      },
      {
        recordId: "recwkEX8j5Ks2uPSI",
        slotKey: "loyalty.proof",
        strategy: "rewrite",
        reason: "campaign_scale_consumer_site_removal",
      },
    ],
    copyRepairTargets: [],
  },
  "woodspring-suites": {
    brandName: "WoodSpring Suites",
    parentPlatform: "Choice Hotels",
    segment: "practical extended-stay",
    positioningPillars: [
      "weekly-stay and longer-stay demand",
      "kitchen-equipped room expectations",
      "lean/simple operating model",
      "Choice platform context",
      "U.S. extended-stay property examples",
    ],
    founderNotes: ["Emphasize practical extended-stay positioning and operating simplicity."],
    developmentUrl:
      "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/woodspring-suites",
    slotRewrites: mapBackfillToGovernance(WOODSPRING_BACKFILL),
    copyRepairTargets: [],
  },
  "everhome-suites": {
    brandName: "Everhome Suites",
    parentPlatform: "Choice Hotels",
    segment: "new-construction extended-stay",
    positioningPillars: [
      "new-construction extended-stay prototype",
      "apartment-style suite model",
      "midscale extended-stay positioning",
      "longer-stay demand",
      "Choice platform context",
    ],
    founderNotes: ["Emphasize purpose-built extended-stay and residential-style suites."],
    developmentUrl: "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/everhome-suites",
    slotRewrites: mapBackfillToGovernance(EVERHOME_BACKFILL),
    copyRepairTargets: [],
  },
});

const LIFESTYLE_COPY_GOVERNANCE_SLUGS = Object.freeze([
  "design-hotels",
  "small-luxury-hotels-of-the-world",
  "autograph-collection",
  "tribute-portfolio",
  "vignette-collection",
  "mgallery-collection",
  "hotel-indigo",
  "handwritten-collection",
]);

function buildLifestyleGovernanceFromBrandConfig(brandSlug) {
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig?.copyGovernanceMode) return null;
  return buildLifestyleCopyGovernanceConfig(brandConfig);
}

export function getCopyGovernanceConfig(brandSlug) {
  const slug = String(brandSlug || "").toLowerCase();
  const explicit = COPY_GOVERNANCE_BY_BRAND[slug];
  if (explicit) return explicit;
  if (LIFESTYLE_COPY_GOVERNANCE_SLUGS.includes(slug)) {
    return buildLifestyleGovernanceFromBrandConfig(slug);
  }
  return null;
}

export function getCopyGovernanceMode(brandSlug) {
  const config = getCopyGovernanceConfig(brandSlug);
  if (!config) return null;
  return config.copyGovernanceMode || null;
}

export { AFFILIATION_COPY_MODES };
