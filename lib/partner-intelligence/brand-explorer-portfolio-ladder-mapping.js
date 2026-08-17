/**
 * Brand Explorer portfolio ladder mapping — shared constants + render simulation.
 * Used by v25C-4D ladder mapping repair and visual defect audit.
 *
 * Owner-planning context only — not Marriott company-validated hierarchy.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const ATELIER_JS_PATH = "public/js/brand-explorer-atelier-from-api.js";

export const GENERIC_LADDER_FALLBACK_LABELS = [
  "Lower-scale brands",
  "Mid-scale brands",
  "Upscale brands",
  "Upper-scale brands",
];

/** Dealality owner-planning ladder — Marriott International. */
export const MARRIOTT_PORTFOLIO_LADDER_TIER_LABELS = [
  "Select Service & Extended Stay",
  "Upper Midscale / Focused Service",
  "Lifestyle & Soft Collections",
  "Luxury & Lifestyle Flagship",
];

export const MARRIOTT_PORTFOLIO_TIER_BRANDS = [
  ["Fairfield by Marriott", "Courtyard by Marriott", "Residence Inn", "TownePlace Suites"],
  ["SpringHill Suites", "Four Points", "Aloft", "AC Hotels"],
  [
    "Autograph Collection",
    "Tribute Portfolio",
    "Design Hotels",
    "Moxy Hotels",
    "Element Hotels",
  ],
  ["The Ritz-Carlton", "St. Regis", "W Hotels", "The Luxury Collection", "Edition"],
];

export const MARRIOTT_SOFT_COLLECTION_BRAND_NAMES = new Set(
  ["autograph collection", "tribute portfolio", "design hotels", "moxy hotels", "element hotels"].map(
    (s) => s.toLowerCase()
  )
);

export const HILTON_PORTFOLIO_LADDER_TIER_LABELS = [
  "Focused Service & Extended Stay",
  "Mainstream Upscale",
  "Premium Full-Service",
  "Luxury & Lifestyle Collections",
];

export const CHOICE_PORTFOLIO_LADDER_TIER_LABELS = [
  "Economy / Core Midscale",
  "Upper Mid / Mainstream Upscale",
  "Premium / Upper Upscale",
  "Luxury & Lifestyle Flagship",
];

/** IHG Hotels & Resorts portfolio ladder — owner-planning context (not company-validated). */
export const IHG_PORTFOLIO_LADDER_TIER_LABELS = [
  "Essential & Extended Stay",
  "Mainstream Upscale",
  "Premium Upscale",
  "Luxury & Lifestyle",
];

export const IHG_PORTFOLIO_TIER_BRANDS = [
  ["avid hotels", "Candlewood Suites", "Holiday Inn Express", "Staybridge Suites"],
  ["Holiday Inn", "Garner Hotels", "Atwell Suites"],
  ["Crowne Plaza", "Hotel Indigo", "voco", "EVEN Hotels", "HUALUXE"],
  ["InterContinental", "Regent", "Six Senses", "Vignette Collection", "Kimpton Hotels"],
];

export function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

export function normalizeParentKey(parent) {
  return nz(parent).toLowerCase();
}

export function isMarriottParent(parent) {
  return normalizeParentKey(parent).includes("marriott");
}

export function isHiltonParent(parent) {
  const key = normalizeParentKey(parent);
  return (
    key.includes("hilton worldwide") ||
    key === "hilton" ||
    (key.includes("hilton") && !key.includes("hilton garden"))
  );
}

export function isChoiceParent(parent) {
  return normalizeParentKey(parent).includes("choice hotels");
}

export function isIhgParent(parent) {
  const key = normalizeParentKey(parent);
  return key.includes("ihg hotels") || key.includes("intercontinental hotels group") || key === "ihg";
}

export function ladderTierIndexFromRaw(raw) {
  if (!hasVal(raw)) return null;
  const text = nz(raw);
  const firstToken = text.split(/\s+/)[0];
  const n = parseInt(firstToken, 10);
  if (!Number.isNaN(n) && n >= 0 && n <= 3) return n;
  const key = text.toLowerCase();
  if (key === "economy" || key === "tier0" || key === "tier-0") return 0;
  if (key === "upper_mid" || key === "upper-mid" || key === "midscale" || key === "tier1" || key === "tier-1")
    return 1;
  if (key === "upscale" || key === "premium" || key === "tier2" || key === "tier-2") return 2;
  if (
    key === "upper_upscale" ||
    key === "upper-upscale" ||
    key === "luxury" ||
    key === "flagship" ||
    key === "tier3" ||
    key === "tier-3"
  )
    return 3;
  return null;
}

export function firstBlock(brand, slotKey) {
  const blocks = brand?.brandExplorer?.blocks || [];
  return blocks.find((b) => nz(b.slotKey) === slotKey) || null;
}

export function portfolioLadderTierIndex(brand) {
  if (typeof brand?.portfolioLadderTier === "number" && brand.portfolioLadderTier >= 0 && brand.portfolioLadderTier <= 3) {
    return brand.portfolioLadderTier;
  }
  const ctxRow = firstBlock(brand, "overview.portfolio_context");
  if (ctxRow) {
    let tierRaw = hasVal(ctxRow.title) ? nz(ctxRow.title) : "";
    if (!tierRaw && hasVal(ctxRow.body)) {
      const firstLine = nz(ctxRow.body).split(/\n+/)[0];
      if (/^\d$/.test(firstLine)) tierRaw = firstLine;
    }
    const fromCtx = ladderTierIndexFromRaw(tierRaw);
    if (fromCtx != null) return fromCtx;
  }
  const legacy = firstBlock(brand, "overview.portfolio_ladder_tier");
  if (legacy) {
    const fromLegacy = ladderTierIndexFromRaw(legacy.title || legacy.body);
    if (fromLegacy != null) return fromLegacy;
  }
  return 2;
}

export function ladderTierFallbackLabelsForParent(parent) {
  if (isChoiceParent(parent)) return CHOICE_PORTFOLIO_LADDER_TIER_LABELS.slice();
  if (isIhgParent(parent)) return IHG_PORTFOLIO_LADDER_TIER_LABELS.slice();
  if (isHiltonParent(parent)) return HILTON_PORTFOLIO_LADDER_TIER_LABELS.slice();
  if (isMarriottParent(parent)) return MARRIOTT_PORTFOLIO_LADDER_TIER_LABELS.slice();
  return GENERIC_LADDER_FALLBACK_LABELS.slice();
}

export function portfolioSiblingNamesByLadderTier(brand) {
  const parent = brand?.parentCompany;
  const tiers = [[], [], [], []];
  const currentNm = nz(brand?.name).toLowerCase();
  if (isMarriottParent(parent)) {
    for (let i = 0; i < 4; i++) {
      tiers[i] = MARRIOTT_PORTFOLIO_TIER_BRANDS[i].filter(
        (nm) => !currentNm || nz(nm).toLowerCase() !== currentNm
      );
    }
  } else if (isIhgParent(parent)) {
    for (let i = 0; i < 4; i++) {
      tiers[i] = IHG_PORTFOLIO_TIER_BRANDS[i].filter(
        (nm) => !currentNm || nz(nm).toLowerCase() !== currentNm
      );
    }
  }
  return tiers;
}

export function portfolioLadderStepLabel(tierNames, fallback, active, brandName) {
  if (active) return hasVal(brandName) ? nz(brandName) : fallback;
  if (tierNames?.length) return tierNames.join(", ");
  return fallback;
}

export function simulatePortfolioLadderCells(brand) {
  const fallbacks = ladderTierFallbackLabelsForParent(brand?.parentCompany);
  const tierNames = portfolioSiblingNamesByLadderTier(brand);
  const ladderIdx = portfolioLadderTierIndex(brand);
  const brandName = nz(brand?.name);
  return fallbacks.map((fallback, i) => {
    const active = i === ladderIdx;
    return {
      tierIndex: i,
      active,
      label: portfolioLadderStepLabel(tierNames[i], fallback, active, brandName),
      fallbackOnly: !active && !(tierNames[i]?.length),
      usesGenericFallback:
        !active &&
        GENERIC_LADDER_FALLBACK_LABELS.some(
          (g) => g.toLowerCase() === portfolioLadderStepLabel(tierNames[i], fallback, active, brandName).toLowerCase()
        ),
    };
  });
}

export function portfolioContextNarrative(brand) {
  const row = firstBlock(brand, "overview.portfolio_context");
  return hasVal(row?.body) ? nz(row.body) : "";
}

export function diagnosePortfolioLadderMapping(brand, frontendSource = "") {
  const parent = nz(brand?.parentCompany);
  const ctxRow = firstBlock(brand, "overview.portfolio_context");
  const cells = simulatePortfolioLadderCells(brand);
  const ladderIdx = portfolioLadderTierIndex(brand);
  const activeCell = cells.find((c) => c.active);
  const inactiveGeneric = cells.filter((c) => !c.active && c.fallbackOnly);
  const hasMarriottFrontendMapping =
    /MARRIOTT_PORTFOLIO_TIER_BRANDS/.test(frontendSource) &&
    /MARRIOTT_PORTFOLIO_LADDER_TIER_LABELS/.test(frontendSource) &&
    /portfolioContextNarrativeHtml/.test(frontendSource);
  const hasIhgFrontendMapping =
    /IHG_PORTFOLIO_TIER_BRANDS/.test(frontendSource) &&
    /IHG_PORTFOLIO_LADDER_TIER_LABELS/.test(frontendSource) &&
    /isIhgParentCompanyKey/.test(frontendSource);
  const hasSoftCollectionPeers =
    /Autograph Collection/.test(frontendSource) &&
    /Design Hotels/.test(frontendSource) &&
    /Tribute Portfolio/.test(frontendSource);
  const narrativeRenders = hasVal(portfolioContextNarrative(brand));
  const tributeHighlighted =
    activeCell &&
    nz(activeCell.label).toLowerCase() === nz(brand?.name).toLowerCase();
  const usesGenericLabels = cells.some((c) =>
    GENERIC_LADDER_FALLBACK_LABELS.some((g) => c.label.toLowerCase() === g.toLowerCase())
  );
  const marriottSoftCollectionPeersMapped =
    isMarriottParent(parent) &&
    MARRIOTT_PORTFOLIO_TIER_BRANDS[2].some((n) => /autograph collection/i.test(n)) &&
    MARRIOTT_PORTFOLIO_TIER_BRANDS[2].some((n) => /design hotels/i.test(n)) &&
    MARRIOTT_PORTFOLIO_TIER_BRANDS[2].some((n) => /tribute portfolio/i.test(n));
  const inactiveTierShowsMarriottPeers = cells.some(
    (c) => !c.active && /autograph collection|design hotels|fairfield|ritz-carlton|springhill/i.test(c.label)
  );
  const marriottSiblingLabelsRender =
    marriottSoftCollectionPeersMapped &&
    (inactiveTierShowsMarriottPeers || (tributeHighlighted && ladderIdx === 2));
  const ihgLuxuryPeersMapped =
    isIhgParent(parent) &&
    IHG_PORTFOLIO_TIER_BRANDS[3].some((n) => /intercontinental/i.test(n)) &&
    IHG_PORTFOLIO_TIER_BRANDS[3].some((n) => /regent/i.test(n));
  const inactiveTierShowsIhgPeers = cells.some(
    (c) => !c.active && /intercontinental|regent|hotel indigo|holiday inn express|crowne plaza/i.test(c.label)
  );
  const ihgSiblingLabelsRender =
    ihgLuxuryPeersMapped &&
    (inactiveTierShowsIhgPeers || (tributeHighlighted && ladderIdx === 3));

  let rootCause = "unknown";
  if (!ctxRow) rootCause = "missing_portfolio_context_row";
  else if (!narrativeRenders) rootCause = "portfolio_context_body_empty";
  else if (isIhgParent(parent) && !hasIhgFrontendMapping) rootCause = "frontend_ihg_ladder_mapping_missing";
  else if (isIhgParent(parent) && usesGenericLabels) rootCause = "generic_ladder_fallback_still_rendering";
  else if (isIhgParent(parent) && !tributeHighlighted) rootCause = "active_brand_not_highlighted_in_ladder";
  else if (isIhgParent(parent) && !ihgSiblingLabelsRender) rootCause = "ihg_sibling_labels_not_rendering";
  else if (isIhgParent(parent)) rootCause = "resolved_after_v30A_R1";
  else if (!hasMarriottFrontendMapping) rootCause = "frontend_marriott_ladder_mapping_missing";
  else if (usesGenericLabels) rootCause = "generic_ladder_fallback_still_rendering";
  else if (!tributeHighlighted) rootCause = "active_brand_not_highlighted_in_ladder";
  else if (!marriottSiblingLabelsRender) rootCause = "marriott_sibling_labels_not_rendering";
  else rootCause = "resolved_after_v25C_4D";

  return {
    portfolioContextRowExists: Boolean(ctxRow),
    portfolioContextRecordId: ctxRow?.recordId || null,
    portfolioContextTitle: nz(ctxRow?.title),
    portfolioContextBodyPreview: nz(ctxRow?.body).slice(0, 160),
    portfolioContextTierIndex: ladderIdx,
    narrativeRenders,
    apiExposesPortfolioContext: Boolean(ctxRow),
    frontendMarriottMappingPresent: hasMarriottFrontendMapping,
    frontendIhgMappingPresent: hasIhgFrontendMapping,
    frontendSoftCollectionPeersPresent: hasSoftCollectionPeers,
    tributeHighlightedInLadder: tributeHighlighted,
    marriottSiblingLabelsRender,
    ihgSiblingLabelsRender,
    usesGenericScaleLabels: usesGenericLabels,
    inactiveGenericTierCount: inactiveGeneric.length,
    ladderCells: cells,
    activeCellLabel: activeCell?.label || "",
    rootCause,
    ownerPlanningContext: true,
  };
}

export function readAtelierFrontendSource() {
  const full = path.join(ROOT, ATELIER_JS_PATH);
  return fs.readFileSync(full, "utf8");
}

/**
 * Shared portfolio-context readiness gate — used by visual defect audit, Final QA, and complete-build.
 * IHG Basics rows may use "InterContinental Hotels Group" (not only "IHG Hotels & Resorts").
 */
export function evaluatePortfolioContextGate(brand, frontendSource = "") {
  const parent = nz(brand?.parentCompany);
  const diagnosis = diagnosePortfolioLadderMapping(brand, frontendSource);
  const isHilton = isHiltonParent(parent);
  const isChoice = isChoiceParent(parent);
  const isMarriott = isMarriottParent(parent);
  const isIhg = isIhgParent(parent);

  const marriottFrontendMapped =
    isMarriott &&
    /MARRIOTT_PORTFOLIO_TIER_BRANDS/.test(frontendSource) &&
    /Autograph Collection/.test(frontendSource);
  const ihgFrontendMapped =
    isIhg &&
    /IHG_PORTFOLIO_TIER_BRANDS/.test(frontendSource) &&
    /InterContinental/.test(frontendSource);

  const usesParentStaticLadder = isHilton || isChoice || isIhg || marriottFrontendMapped;
  const marriottPortfolioReady =
    isMarriott && marriottFrontendMapped && diagnosis.marriottSiblingLabelsRender;
  const ihgPortfolioReady = isIhg && ihgFrontendMapped && diagnosis.ihgSiblingLabelsRender;
  const hiltonPortfolioReady = isHilton;
  const choicePortfolioReady = isChoice;
  const parentPortfolioReady =
    marriottPortfolioReady || ihgPortfolioReady || hiltonPortfolioReady || choicePortfolioReady;

  const rowAndNarrativeReady =
    diagnosis.portfolioContextRowExists && diagnosis.narrativeRenders;
  const missingPeerDefect =
    !rowAndNarrativeReady ||
    (!parentPortfolioReady &&
      (diagnosis.usesGenericScaleLabels || !usesParentStaticLadder));

  return {
    ...diagnosis,
    parentCompany: parent,
    usesParentStaticLadder,
    marriottPortfolioReady,
    ihgPortfolioReady,
    hiltonPortfolioReady,
    choicePortfolioReady,
    parentPortfolioReady,
    rowAndNarrativeReady,
    missingPeerDefect,
    portfolioContextReady: rowAndNarrativeReady && parentPortfolioReady,
  };
}

export function frontendMappingRepairNeeded(frontendSource = "") {
  const needs = [];
  if (!/MARRIOTT_PORTFOLIO_TIER_BRANDS/.test(frontendSource)) {
    needs.push("add_marriott_portfolio_tier_brands");
  }
  if (!/portfolioContextNarrativeHtml/.test(frontendSource)) {
    needs.push("render_portfolio_context_narrative");
  }
  if (!/Autograph Collection/.test(frontendSource) || !/Design Hotels/.test(frontendSource)) {
    needs.push("soft_collection_peer_labels");
  }
  if (!/owner-planning context/i.test(frontendSource)) {
    needs.push("marriott_owner_planning_hint_copy");
  }
  return needs;
}
