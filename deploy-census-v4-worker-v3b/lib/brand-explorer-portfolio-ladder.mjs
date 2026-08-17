/**
 * Portfolio Context ladder tier (0–3) for Brand Explorer Overview.
 * Keep in sync with public/js/brand-explorer-atelier-from-api.js ladder labels.
 */

export const PORTFOLIO_LADDER_TIER_LABELS = [
  "Economy / Core Midscale",
  "Upper Mid / Mainstream Upscale",
  "Premium / Upper Upscale",
  "Luxury & Lifestyle Flagship",
];

/** @param {string} scale */
export function ladderTierIndexFromChainScale(scale) {
  const s = String(scale || "").toLowerCase();
  if (!s) return 2;
  if (s.includes("luxury") || s.includes("upper upscale")) return 3;
  if (s.includes("upscale") && !s.includes("upper")) return 2;
  if (s.includes("upper mid") || s.includes("midscale")) return 1;
  if (s.includes("economy")) return 0;
  return 2;
}

/**
 * Parse presentation Body/Title for overview.portfolio_ladder_tier.
 * @param {string} raw
 * @returns {number | null}
 */
export function ladderTierIndexFromPresentationBody(raw) {
  const text = String(raw ?? "").trim();
  if (!text) return null;

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

/**
 * Consolidated slot: overview.portfolio_context — Title = tier (0–3), Body = relative positioning.
 * @param {{ slotKey?: string, title?: string, body?: string } | null} row
 * @returns {{ tier: number | null, relativePositioning: string }}
 */
export function parsePortfolioContextRow(row) {
  if (!row) return { tier: null, relativePositioning: "" };
  const slotKey = String(row.slotKey ?? "");
  if (slotKey !== "overview.portfolio_context") {
    return { tier: null, relativePositioning: "" };
  }

  let tierRaw = String(row.title ?? "").trim();
  let body = String(row.body ?? "").trim();

  if (!tierRaw && body) {
    const lines = body.split(/\n+/).map((s) => s.trim()).filter(Boolean);
    if (lines.length && /^\d$/.test(lines[0])) {
      tierRaw = lines[0];
      body = lines.slice(1).join("\n\n").trim();
    }
  }

  const tier = tierRaw ? ladderTierIndexFromPresentationBody(tierRaw) : null;
  return { tier, relativePositioning: body };
}

/**
 * @param {{ hotelChainScale?: string, chainScale?: string, brandExplorer?: { blocks?: Array<{ slotKey?: string, body?: string, title?: string }> }, portfolioLadderTier?: number }} brand
 * @returns {0|1|2|3}
 */
export function resolvePortfolioLadderTier(brand) {
  if (!brand || typeof brand !== "object") return 2;

  const preset = brand.portfolioLadderTier;
  if (typeof preset === "number" && preset >= 0 && preset <= 3) return /** @type {0|1|2|3} */ (preset);

  const blocks = brand.brandExplorer?.blocks;
  if (Array.isArray(blocks)) {
    const consolidated = blocks.find((b) => b && String(b.slotKey) === "overview.portfolio_context");
    const parsed = parsePortfolioContextRow(consolidated);
    if (parsed.tier != null) return parsed.tier;

    const legacyTier = blocks.find((b) => b && String(b.slotKey) === "overview.portfolio_ladder_tier");
    if (legacyTier) {
      const raw = String(legacyTier.body ?? "").trim() || String(legacyTier.title ?? "").trim();
      const fromSlot = ladderTierIndexFromPresentationBody(raw);
      if (fromSlot != null) return fromSlot;
    }
  }

  return ladderTierIndexFromChainScale(brand.hotelChainScale || brand.chainScale);
}

/**
 * @param {{ brandExplorer?: { blocks?: Array<{ slotKey?: string, body?: string, title?: string }> } }} brand
 * @returns {string}
 */
export function resolvePortfolioRelativePositioning(brand) {
  const blocks = brand?.brandExplorer?.blocks;
  if (!Array.isArray(blocks)) return "";

  const consolidated = blocks.find((b) => b && String(b.slotKey) === "overview.portfolio_context");
  const parsed = parsePortfolioContextRow(consolidated);
  if (parsed.relativePositioning) return parsed.relativePositioning;

  const legacy = blocks.find((b) => b && String(b.slotKey) === "overview.relative_positioning");
  if (legacy) {
    const body = String(legacy.body ?? "").trim();
    if (body) return body;
    return String(legacy.title ?? "").trim();
  }
  return "";
}

/** @param {{ portfolioLadderTier?: number, hotelChainScale?: string, chainScale?: string }} brand */
export function resolvePortfolioLadderTierForListBrand(brand) {
  if (!brand || typeof brand !== "object") return 2;
  if (typeof brand.portfolioLadderTier === "number" && brand.portfolioLadderTier >= 0 && brand.portfolioLadderTier <= 3) {
    return brand.portfolioLadderTier;
  }
  return ladderTierIndexFromChainScale(brand.hotelChainScale || brand.chainScale);
}
