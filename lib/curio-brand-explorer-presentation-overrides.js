/**
 * Curio Collection by Hilton — Brand Explorer presentation slot overrides.
 * Replaces Kimpton/IHG template carryover from build-curio-presentation-template.mjs.
 *
 * Sources: Curio fact sheet (May 2026), Hilton Develop APAC brochure, Curio FDD,
 * lib/curio-brand-explorer-cala-materials.js case studies.
 * Illustrative only — confirm in countersigned FDD before external use.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { applyHiltonLoyaltyPresentationSlots } from "./partner-intelligence/build-hilton-loyalty-presentation-slots.js";
import { decodeFddPlainTextBuffer, readFddPlainTextFile } from "./partner-intelligence/decode-fdd-plain-text.mjs";
import { parseCurioFddEconomics } from "./partner-intelligence/parse-curio-fdd-economics.mjs";
import { applyCurioEconomicsPresentationSlots } from "./partner-intelligence/build-curio-economics-presentation-slots.js";

const __curioDir = path.dirname(fileURLToPath(import.meta.url));
const __curioRoot = path.resolve(__curioDir, "..");

/** @type {Record<string, string | { title?: string; body: string }>} */
export const CURIO_SLOT_OVERRIDES = {
  // ── Overview — owner value scenarios (primary fix) ─────────────────────────
  "overview.scenarios":
    "Independent and soft-brand full-service conversions where the asset already has story, F&B complexity, and local character—Curio adds Hilton Honors and distribution without erasing individuality.\n\nDestination resort and experiential repositioning—beach, golf, nature lodge, or all-inclusive formats where authenticity and culinary moments justify upper-upscale ADR with Hilton's revenue engine.\n\nAdaptive reuse and heritage repositioning—historic urban, landmark, or architecturally distinctive buildings where conversion PIP and design narrative unlock premium positioning versus select-service reflag economics.",
  "overview.scenario.1::Independent & Soft-Brand Conversion": {
    title: "Independent & Soft-Brand Conversion",
    body:
      "Full-service independent or soft-brand assets with existing story and F&B complexity—Curio retains local character while adding Hilton Honors, global sales, supply management, and distribution. Best when the building can support upper-upscale full-service economics, not limited-service reflag.",
  },
  "overview.scenario.2::Destination Resort & Experiential Repositioning": {
    title: "Destination Resort & Experiential Repositioning",
    body:
      "Resorts, beach, golf, nature lodges, and all-inclusive properties where destination authenticity and culinary experiences drive ADR—Curio's flexible brand standards accommodate format diversity (à la carte urban to large-format AI) while connecting to Hilton's loyalty and commercial stack.",
  },
  "overview.scenario.3::Adaptive Reuse & Heritage Repositioning": {
    title: "Adaptive Reuse & Heritage Repositioning",
    body:
      "Historic urban cores, walled cities, landmark buildings, and adaptive reuse where architecture and sense of place are the product—conversion PIP and design narrative unlock premium positioning. Confirm Hilton fees, incentives, and heritage constraints before modeling from U.S. gateway prototypes.",
  },

  // Legacy Kimpton titles (replaced when fixture still has old titles)
  "overview.scenario.1::Urban Lifestyle Conversion": {
    title: "Independent & Soft-Brand Conversion",
    body:
      "Full-service independent or soft-brand assets with existing story and F&B complexity—Curio retains local character while adding Hilton Honors, global sales, supply management, and distribution. Best when the building can support upper-upscale full-service economics, not limited-service reflag.",
  },
  "overview.scenario.2::Gateway New-Build or Adaptive Reuse": {
    title: "Destination Resort & Experiential Repositioning",
    body:
      "Resorts, beach, golf, nature lodges, and all-inclusive properties where destination authenticity and culinary experiences drive ADR—Curio's flexible brand standards accommodate format diversity (à la carte urban to large-format AI) while connecting to Hilton's loyalty and commercial stack.",
  },
  "overview.scenario.3::Portfolio Lifestyle Standardization": {
    title: "Adaptive Reuse & Heritage Repositioning",
    body:
      "Historic urban cores, walled cities, landmark buildings, and adaptive reuse where architecture and sense of place are the product—conversion PIP and design narrative unlock premium positioning. Confirm Hilton fees, incentives, and heritage constraints before modeling from U.S. gateway prototypes.",
  },

  "overview.scenario.1": {
    title: "Independent & Soft-Brand Conversion",
    body:
      "Full-service independent or soft-brand assets with existing story and F&B complexity—Curio retains local character while adding Hilton Honors, global sales, supply management, and distribution. Best when the building can support upper-upscale full-service economics, not limited-service reflag.",
  },
  "overview.scenario.2": {
    title: "Destination Resort & Experiential Repositioning",
    body:
      "Resorts, beach, golf, nature lodges, and all-inclusive properties where destination authenticity and culinary experiences drive ADR—Curio's flexible brand standards accommodate format diversity (à la carte urban to large-format AI) while connecting to Hilton's loyalty and commercial stack.",
  },
  "overview.scenario.3": {
    title: "Adaptive Reuse & Heritage Repositioning",
    body:
      "Historic urban cores, walled cities, landmark buildings, and adaptive reuse where architecture and sense of place are the product—conversion PIP and design narrative unlock premium positioning. Confirm Hilton fees, incentives, and heritage constraints before modeling from U.S. gateway prototypes.",
  },

  // ── Overview — positioning & template leak fixes ───────────────────────────
  "overview.relative_positioning":
    "Upper-upscale soft collection within Hilton—individually remarkable hotels with distinctive architecture, culinary-forward F&B, and destination-native experiences—not select-service, extended-stay, or rigid chain retail.",
  "overview.portfolio_context":
    "Upper-upscale soft collection within Hilton—Curio sits with Tapestry as the independent-character tier; below Waldorf Astoria, Conrad, and LXR luxury; above Hilton Hotels & Resorts core full-service, DoubleTree, and Garden Inn—not Hampton, Tru, Spark, or Homewood extended-stay formats.",
  "overview.development_model":
    "Conversion and repositioning of independent and soft-brand full-service assets across urban, resort, and experiential destinations; selective new-build and adaptive reuse. FDD paths include New Development, Conversion, Change of Ownership, and Re-licensing—confirm pipeline and incentives with Hilton development.",
  "overview.typical_use_case":
    "Historic urban conversions, destination resorts, nature lodges, golf and beach properties, and selective new-build in sought-after markets where guests pay for one-of-a-kind character, culinary moments, and local authenticity—not commodity limited-service or economy extended-stay.",
  "overview.why_value":
    "Keep your independent spirit—just add Hilton: retain hotel individuality and local character while accessing Hilton Honors, global sales, Hilton Supply Management, and revenue systems.\nRemarkable characters: hand-picked upper-upscale hotels in sought-after urban and resort destinations—198 open across 47 countries (May 2026 fact sheet).\nCulinary-forward positioning: world-class F&B from beachfront cocktails to chef-driven tasting menus—not generic hotel dining.\nConversion-friendly economics: FDD supports conversion, adaptive reuse, and new development—model ~75% avg Hilton Honors occupancy contribution from your Item 19 sample (2025 U.S. comparable hotels).\nFit: owners and operators who can run full-service experiential hospitality with design QA and F&B discipline—not breakfast-only limited-service.",
  "overview.owner_experience":
    "Typical guest: experience-led leisure and business travelers seeking one-of-a-kind discoveries, local culinary moments, and destination immersion.\nOwner journey: feasibility on conversion scope → design narrative and F&B plan aligned to Curio brand standards → Hilton systems cutover → opening QA on service and culinary execution.\nRamp: Hilton Honors and enterprise mix build over 12–24 months—model net contribution after loyalty chargebacks and program fees.\nOngoing: flexible upper-upscale standards require sustained design, F&B, and service investment through PIP and QA cycles—each property remains distinct.",
  "overview.proof_operator":
    "Operators who excel at full-service experiential hospitality, culinary execution, design compliance, and upper-upscale guest experience—with third-party management common across the collection.",

  "overview.differentiators.identity":
    "Individuality is the hallmark—each Curio hotel is hand-picked for unique character; no two properties are the same.\nRemarkable characters and destinations unto themselves—architecture, rituals, and locally inspired design.\nCulinary-forward F&B positioned to compete locally—not generic hotel dining.\nSoft collection positioning: independent hotel character with Hilton infrastructure—not rigid chain retail.",
  "overview.differentiators.commercial":
    "Hilton Honors: portfolio-wide member demand across 28 Hilton brands and 9,000+ hotels.\nHilton distribution: brand.com, GDS, OTAs, and enterprise channels—stress-test net after fees.\nConfirm royalty, marketing, loyalty, and technology fees in your Curio FDD—underwrite from your disclosure schedule, not another Hilton brand's fee stack.\nConversion-weighted growth globally—119 hotels in development (May 2026); align capex with experiential PIP, not economy reflag economics.",

  "overview.bestAt.1::Independent & Soft-Brand Conversion": {
    title: "Independent & Soft-Brand Conversion",
    body:
      "Strongest fit when an independent or soft-brand full-service asset needs Hilton distribution and Honors without losing local identity—core Curio owner pitch per Hilton development materials.",
  },
  "overview.bestAt.2::Destination Resort & Experiential": {
    title: "Destination Resort & Experiential",
    body:
      "Resort, beach, golf, nature, and all-inclusive formats where destination authenticity drives ADR—CALA examples include Miches all-inclusive, Indura golf/beach, and Royal Palm Galápagos lodge.",
  },
  "overview.bestAt.3::Hilton Honors & Distribution": {
    title: "Hilton Honors & Distribution",
    body:
      "Hilton Honors and enterprise demand participation—model net contribution after program fees; Curio launched October 2014 as Hilton's upper-upscale soft collection.",
  },
  "overview.bestAt.1::Gateway Urban Lifestyle": {
    title: "Independent & Soft-Brand Conversion",
    body:
      "Strongest fit when an independent or soft-brand full-service asset needs Hilton distribution and Honors without losing local identity—core Curio owner pitch per Hilton development materials.",
  },
  "overview.bestAt.2::Conversion Repositioning": {
    title: "Destination Resort & Experiential",
    body:
      "Resort, beach, golf, nature, and all-inclusive formats where destination authenticity drives ADR—CALA examples include Miches all-inclusive, Indura golf/beach, and Royal Palm Galápagos lodge.",
  },
  "overview.bestAt.3::Hilton Loyalty & Distribution": {
    title: "Hilton Honors & Distribution",
    body:
      "Hilton Honors and enterprise demand participation—model net contribution after program fees; Curio launched October 2014 as Hilton's upper-upscale soft collection.",
  },

  "overview.proof.1::198 Hotels · 47 Countries": {
    title: "198 Hotels · 47 Countries",
    body:
      "198 Curio Collection hotels open across 47 countries and territories; 119 in development (May 2026 fact sheet)—global soft-collection footprint, not a single-market lifestyle play.",
  },
  "overview.proof.2::Conversion-Weighted Pipeline": {
    title: "Conversion-Weighted Pipeline",
    body:
      "Conversion and repositioning remain core growth paths—confirm authorized geography, area of protection, and Hilton development incentives for your market.",
  },
  "overview.proof.3::Curio Since 2014": {
    title: "Curio Since 2014",
    body:
      "Curio Collection launched October 2014 as Hilton's upper-upscale soft collection—owners gain independent character plus Hilton Honors, sales, supply, and revenue infrastructure.",
  },
  "overview.proof.4::Culinary-Forward F&B": {
    title: "Culinary-Forward F&B",
    body:
      "World-class culinary moments—from beachfront cocktails to chef-driven tasting menus—define guest promise and operating labor plan, not complimentary breakfast-led select-service.",
  },
  "overview.proof.5::Hilton Honors®": {
    title: "Hilton Honors®",
    body:
      "Hilton Honors spans 28 Hilton brands and 9,000+ hotels globally; 2026 program adds Diamond Reserve tier. Model this brand's loyalty contribution from your FDD Item 19 sample and local comp set—not system-wide averages alone.",
  },
  "overview.proof.6::Experiential Operators": {
    title: "Experiential Operators",
    body:
      "Wins with operators who execute culinary-forward F&B, design standards, and destination-native service—not breakfast-only limited-service models.",
  },
  "overview.proof.1::Pet-Friendly Lifestyle": {
    title: "198 Hotels · 47 Countries",
    body:
      "198 Curio Collection hotels open across 47 countries and territories; 119 in development (May 2026 fact sheet)—global soft-collection footprint, not a single-market lifestyle play.",
  },
  "overview.proof.2::60+ Americas Hotels": {
    title: "Conversion-Weighted Pipeline",
    body:
      "Conversion and repositioning remain core growth paths—confirm authorized geography, area of protection, and Hilton development incentives for your market.",
  },
  "overview.proof.3::Hilton Integration Since 2015": {
    title: "Curio Since 2014",
    body:
      "Curio Collection launched October 2014 as Hilton's upper-upscale soft collection—owners gain independent character plus Hilton Honors, sales, supply, and revenue infrastructure.",
  },
  "overview.proof.4::Wine Hour & Social F&B": {
    title: "Culinary-Forward F&B",
    body:
      "World-class culinary moments—from beachfront cocktails to chef-driven tasting menus—define guest promise and operating labor plan, not complimentary breakfast-led select-service.",
  },
  "overview.proof.6::Lifestyle Operators": {
    title: "Experiential Operators",
    body:
      "Wins with operators who execute culinary-forward F&B, design standards, and destination-native service—not breakfast-only limited-service models.",
  },

  // ── Hero ───────────────────────────────────────────────────────────────────
  "hero.benefit_zones":
    "Sought-after urban districts, destination resorts, heritage conversions, nature lodges, and experiential markets where independent character drives ADR",
  "hero.operator_compat":
    "Deliver destination-native, culinary-forward full-service stays—operators who run upper-upscale experiential hospitality with strong QA on F&B, design, and guest experience.",

  // ── Value owners (mirror scenario cards) ───────────────────────────────────
  "valueOwners.overview":
    "Guests: One-of-a-kind discoveries, local culinary moments, and destination immersion at individually remarkable hotels.\n\nOwners: Upper-upscale Hilton soft collection—retain independence and character while gaining Honors, distribution, sales, and supply scale.\n\nUnderwrite contribution after fees, loyalty costs, F&B labor, and channel mix versus comp set.",
  "valueOwners.scenarios":
    "Independent or soft-brand full-service conversion needing Hilton distribution without sacrificing local identity.\n\nResort, beach, golf, nature, or all-inclusive repositioning where experiential ADR justifies upper-upscale F&B and design investment.\n\nHeritage or adaptive reuse in historic urban or landmark buildings—conversion PIP matched to local ADR and operating complexity.\n\nThird-party operator-led: fits when the operator can run full-service culinary-forward operations, Curio design compliance, and Hilton systems cutover—management is common while Hilton development approves brand milestones.",

  // ── Portfolio context ladder tier (title = 0–3) ───────────────────────────
  "overview.portfolio_context": {
    title: "3",
    body:
      "Upper-upscale soft collection within Hilton—Curio sits with Tapestry as the independent-character tier; below Waldorf Astoria, Conrad, and LXR luxury; above Hilton Hotels & Resorts core full-service, DoubleTree, and Garden Inn—not Hampton, Tru, Spark, or Homewood extended-stay formats.",
  },

  // ── Dealality Insight ─────────────────────────────────────────────────────
  "insight.summary":
    "Curio Collection by Hilton fits when you want upper-upscale soft-collection positioning with retained independence, Hilton Honors distribution, and conversion-friendly experiential PIP—not a mismatched tier (e.g. Tapestry vs Curio without comparing F&B scope and design narrative). Best with full-service operators, honest ramp plan, and FDD-backed channel mix. Weaker where market ADR cannot support culinary-forward F&B, design investment, or conversion ROI does not clear hurdle.",

  // ── Footprint (Kimpton-shaped growth copy) ──────────────────────────────────
  "footprint.geo_intro":
    "Curio Collection by Hilton is an upper-upscale soft collection in the Hilton Worldwide portfolio—198 hotels open across 47 countries and territories; 119 in development (May 2026 fact sheet). Evaluate fit on market tier, F&B capability, design narrative, loyalty contribution, and competitive set—not select-service or economy assumptions.",
  "footprint.region.am":
    "Americas\n\nPrimary growth region for Curio—independent and soft-brand conversions across urban, resort, and experiential markets. Confirm area-of-protection and comp set versus other Hilton soft-collection and full-service flags in your corridor.",
  "footprint.region.eu":
    "Europe\n\nGrowing Curio presence—confirm authorized geography, conversion incentives, and Hilton standards for your asset.",
  "footprint.growth_themes":
    "Independent and soft-brand full-service conversions\nDestination resort and experiential repositioning\nHeritage and adaptive reuse in sought-after markets\nCulinary-forward F&B and design narrative differentiation",
  "footprint.growth_editorial":
    "Curio is Hilton's upper-upscale soft collection—conversion-weighted growth globally per Hilton development materials and May 2026 fact sheet. Evaluate fit on F&B operating model, design PIP, and Hilton Honors contribution in your comp set.",
  "footprint.growth_fit":
    "Independent and soft-brand full-service conversions\nDestination resort and experiential repositioning\nHeritage and adaptive reuse in sought-after markets\nCulinary-forward F&B and design narrative differentiation",
  "footprint.editorial":
    "Curio is Hilton's upper-upscale soft collection—conversion-weighted growth globally per Hilton development materials. Evaluate fit on F&B operating model, design PIP, and Hilton Honors contribution in your comp set.",
  "footprint.editorial_bullets":
    "Conversion-weighted pipeline across urban, resort, and experiential markets\nCulinary-forward F&B as operating differentiator\nHilton Honors cross-brand demand\nFlexible design narrative—not economy reflag economics",
  "footprint.growth.narrative":
    "Curio growth emphasizes independent and soft-brand conversions where destination character, culinary moments, and design narrative justify upper-upscale ADR. Confirm pipeline and incentives with Hilton development for your asset.",

  // ── Operations ────────────────────────────────────────────────────────────
  "operations.model.systems_integration":
    "Mandatory Hilton stack (OnQ PMS, Hilton CRS, Hilton Honors, revenue tools)—confirm cutover timeline and fees in FDD Item 6.",
  "operations.model.typical_ownership":
    "Institutional and entrepreneurial owners with conversion or new-build experience in upper-upscale full-service and experiential markets.",
  "operations.operator_compat.summary":
    "Deliver destination-native, culinary-forward full-service stays—operators who run upper-upscale experiential hospitality with strong QA on F&B, design, and guest experience.",
  "operations.operator_compat.tags":
    "Experiential · Full-service F&B · Conversion · Hilton systems",

  // ── Commercial ────────────────────────────────────────────────────────────
  "commercial.intro":
    "Curio Collection by Hilton — Individually remarkable hotels for experience-led travelers. Upper-upscale soft collection with distinctive architecture, culinary-forward F&B, and Hilton Honors—not select-service or extended-stay. Commercial strengths below show how this flag's systems can affect demand, rate, and channel mix on your asset (illustrative; not a performance guarantee).",
  "commercial.differentiator":
    "Sought-after urban, resort, and experiential destinations where destination character, culinary moments, and design narrative differentiate versus select-service and rigid chain competitors.",
  "commercial.lever.international":
    "Hilton global portfolio context—confirm authorized geography, disclosure counts, and international ramp curves for your market.",
  "commercial.theme::Breakfast & Amenity Story": {
    title: "Breakfast & Amenity Story",
    body: "Culinary moments, local rituals, and destination-native design—not complimentary breakfast as the primary guest promise.",
  },
  "commercial.theme::Pet-Friendly & Design Story": {
    title: "Destination & Culinary Story",
    body: "Remarkable characters and world-class culinary moments—not standardized chain retail or economy amenity led positioning.",
  },
  "commercial.demand::Gateway Urban Lifestyle": {
    title: "Independent / Soft-Brand Conversion",
    body: "Strong",
  },
  "commercial.demand::Urban Lifestyle / Gateway": {
    title: "Heritage & Experiential Repositioning",
    body: "Strong",
  },

};

/** Illustrative peer brands for Dealality Insight Similar Brands cards. */
export const CURIO_INSIGHT_SIMILAR_ROWS = [
  {
    slotKey: "insight.similar",
    title: "Tapestry Collection by Hilton",
    body: "Same-parent soft collection peer—compare conversion PIP, design flexibility, F&B scope, and fee stack on your asset.",
    sort: 0,
  },
  {
    slotKey: "insight.similar",
    title: "Autograph Collection (Marriott)",
    body: "Cross-parent soft-brand benchmark—compare owner independence, loyalty contribution, and upper-upscale ADR capture.",
    sort: 1,
  },
  {
    slotKey: "insight.similar",
    title: "Unbound Collection by Hyatt",
    body: "Cross-parent independent-character collection—compare distribution reach, conversion economics, and experiential positioning.",
    sort: 2,
  },
];

/** Kimpton/IHG terms that must not remain after Curio overrides. */
export const CURIO_FORBIDDEN_PATTERN =
  /\b(IHG|IHG One|InterContinental Ambassador|Hotel Indigo|voco|EVEN Hotels|InterContinental, Regent|Six Senses|Kimpton|wine hour|Pet-friendly and design|Pet-friendly policy|Fortune|Condé Nast|Portfolio Lifestyle Standardization|Urban Lifestyle Conversion|Gateway New-Build or Adaptive Reuse|~60\+ Americas|Platinum \/ Diamond \/ InterContinental|6,600\+ Hilton hotels worldwide; earn and redeem across Hilton brands; elite tiers \(Silver, Gold, Platinum)\b|6\.0% royalty|~50\.8%|~88\.5%/i;

/** Load parsed Curio FDD economics from cache (plain text or JSON). */
export function loadCurioFddEconomics() {
  const jsonPath = path.join(__curioRoot, "fixtures", "curio-fdd-economics.json");
  if (fs.existsSync(jsonPath)) {
    const cached = JSON.parse(fs.readFileSync(jsonPath, "utf8"));
    if (cached?.parsed) return cached.parsed;
  }
  const plainPath = path.join(__curioRoot, "reports", "curio-fdd-plain.txt");
  if (fs.existsSync(plainPath)) {
    return parseCurioFddEconomics(readFddPlainTextFile(plainPath));
  }
  return null;
}

export function overrideKey(slotKey, title = "", sort = null) {
  const t = String(title || "").trim();
  if (t) return `${slotKey}::${t}`;
  if (sort != null && sort !== "") return `${slotKey}@${sort}`;
  return slotKey;
}

/**
 * @param {{ slotKey: string, title?: string, body?: string, sort?: number }} row
 */
export function applyCurioOverride(row) {
  const override =
    CURIO_SLOT_OVERRIDES[overrideKey(row.slotKey, row.title, row.sort)] ??
    CURIO_SLOT_OVERRIDES[row.slotKey];

  if (!override) return false;

  if (typeof override === "string") {
    row.body = override;
    return true;
  }

  if (override.title) row.title = override.title;
  if (override.body !== undefined) row.body = override.body;
  return true;
}

/**
 * @param {{ rows: object[] }} fixture
 */
export function overlayCurioPresentationOverrides(fixture) {
  for (const row of fixture.rows) {
    applyCurioOverride(row);
  }
  return fixture.rows;
}

/** Rows touched by overview + valueOwners overrides (for Airtable patch fixture). */
export function getCurioOverviewOverrideRows(allRows) {
  const prefixes = ["overview.", "valueOwners.", "hero.benefit_zones", "hero.operator_compat"];
  return allRows.filter((r) => prefixes.some((p) => r.slotKey === p || r.slotKey.startsWith(p)));
}

/** Rows for full Kimpton-template cleanup across tabs (excludes CALA materials slots). */
export function getCurioCleanupPatchRows(allRows) {
  const keepPrefixes = [
    "overview.",
    "valueOwners.",
    "hero.",
    "insight.",
    "loyalty.",
    "commercial.",
    "footprint.geo_intro",
    "footprint.region.am",
    "footprint.region.eu",
    "footprint.growth",
    "footprint.editorial",
    "operations.model.",
    "operations.operator_compat.",
    "economics.",
  ];
  const skipExact = new Set([
    "footprint.momentum",
    "footprint.momentum_label",
    "footprint.openings",
    "footprint.region.cala",
    "footprint.portfolio_mix",
  ]);
  return allRows.filter((r) => {
    if (skipExact.has(r.slotKey)) return false;
    return keepPrefixes.some((p) => r.slotKey === p || r.slotKey.startsWith(p));
  });
}

/**
 * Merge Hilton Honors loyalty rows and insight.similar peers into fixture rows.
 * @param {object[]} rows
 * @param {object[]} [mergedFacts]
 */
export function injectCurioGeneratedRows(rows, mergedFacts = []) {
  const out = rows.filter(
    (r) => !r.slotKey.startsWith("loyalty.") && r.slotKey !== "insight.similar"
  );
  out.push(
    ...applyHiltonLoyaltyPresentationSlots([], mergedFacts, {
      brandName: "Curio Collection by Hilton",
    })
  );
  out.push(...CURIO_INSIGHT_SIMILAR_ROWS);
  return out;
}

export function auditCurioForbiddenCopy(rows) {
  const violations = [];
  for (const row of rows) {
    const blob = JSON.stringify(row);
    if (CURIO_FORBIDDEN_PATTERN.test(blob)) {
      violations.push(`${row.slotKey}${row.title ? ` :: ${row.title}` : ""}`);
    }
  }
  return violations;
}

/** Apply overrides + generated rows to a fixture in place. */
export function buildCurioCleanFixture(fixture, mergedFacts = []) {
  overlayCurioPresentationOverrides(fixture);
  const econ = loadCurioFddEconomics();
  if (econ) {
    fixture.rows = applyCurioEconomicsPresentationSlots(fixture.rows, econ, {
      brandName: "Curio Collection by Hilton",
    });
  }
  fixture.rows = injectCurioGeneratedRows(fixture.rows, mergedFacts);
  return fixture;
}
