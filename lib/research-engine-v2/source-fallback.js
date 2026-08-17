/**
 * Official-source fallback ladder — design + helpers.
 * Never invents material claims from blocked primary sources alone.
 */

export const FALLBACK_LADDER = Object.freeze([
  { step: 1, id: "official_property_page", label: "Official property page" },
  { step: 2, id: "official_brand_directory", label: "Official brand directory API/feed/sitemap" },
  { step: 3, id: "official_parent_sitemap", label: "Official parent sitemap" },
  { step: 4, id: "official_cached_index", label: "Official cached/index metadata (if reliable)" },
  { step: 5, id: "official_announcement", label: "Secondary official announcement" },
  { step: 6, id: "trade_corroboration_only", label: "High-quality trade source (corroboration only)" },
]);

/**
 * @param {object[]} attempts - [{ ladderId, sourceState, url?, notes? }]
 */
export function summarizeFallbackAttempts(attempts = []) {
  const used = attempts.filter((a) => a.sourceState === "Available");
  const blocked = attempts.filter((a) => a.sourceState === "Blocked" || a.sourceState === "Failed");
  const allFailed = attempts.length > 0 && used.length === 0;

  return {
    ladder: FALLBACK_LADDER,
    attempts,
    availableStep: used[0]?.ladderId || null,
    fallbackUsed: used.length > 0 && attempts[0]?.sourceState !== "Available",
    terminal:
      allFailed
        ? {
            classification: "Source Blocked / Needs External Research",
            escalation: "Webhound candidate or manual source retrieval (explicit auth required)",
            doNotInfer: ["closed", "removed", "reflagged", "discontinued", "missing"],
          }
        : null,
  };
}

/**
 * Trade press may corroborate only — never sole High material update.
 * @param {boolean} hasOfficialPrimary
 * @param {boolean} hasTradeOnly
 */
export function tradePressAllowedRole(hasOfficialPrimary, hasTradeOnly) {
  if (hasOfficialPrimary && hasTradeOnly) return "secondary_corroboration";
  if (!hasOfficialPrimary && hasTradeOnly) return "insufficient_alone";
  return "n/a";
}
