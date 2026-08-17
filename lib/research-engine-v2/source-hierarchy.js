/**
 * Claim-specific source hierarchy for Research Engine V2.
 * Documented in config — not buried only in prompts.
 */

/** @typedef {"official_brand_directory"|"official_hotel_website"|"official_parent_page"|"official_opening_announcement"|"official_development_pipeline"|"owner_announcement"|"operator_announcement"|"management_portfolio"|"reputable_trade_press"|"secondary"|"unknown"} SourceType */

/**
 * Higher index = lower priority within a claim type.
 * Adapters should prefer the earliest matching source type that yields evidence.
 */
export const SOURCE_HIERARCHY_BY_CLAIM = Object.freeze({
  OPERATING_STATUS: [
    "official_brand_directory",
    "official_hotel_website",
    "official_parent_page",
    "official_opening_announcement",
    "reputable_trade_press",
    "secondary",
  ],
  CURRENT_BRAND: [
    "official_brand_directory",
    "official_hotel_website",
    "official_parent_page",
    "reputable_trade_press",
    "secondary",
  ],
  CURRENT_PARENT: [
    "official_parent_page",
    "official_brand_directory",
    "official_hotel_website",
    "secondary",
  ],
  PIPELINE_STATUS: [
    "official_development_pipeline",
    "official_opening_announcement",
    "official_brand_directory",
    "official_parent_page",
    "reputable_trade_press",
    "secondary",
  ],
  OPENING_STATUS: [
    "official_opening_announcement",
    "official_brand_directory",
    "official_hotel_website",
    "reputable_trade_press",
    "secondary",
  ],
  REFLAG_STATUS: [
    "official_brand_directory",
    "official_hotel_website",
    "official_parent_page",
    "official_opening_announcement",
    "reputable_trade_press",
    "secondary",
  ],
  CURRENT_OPERATOR: [
    "owner_announcement",
    "operator_announcement",
    "management_portfolio",
    "official_hotel_website",
    "reputable_trade_press",
    "secondary",
  ],
  HOTEL_EXISTS: [
    "official_brand_directory",
    "official_hotel_website",
    "official_parent_page",
    "secondary",
  ],
});

/**
 * @param {string} claimType
 * @param {string} sourceType
 * @returns {number} priority rank (0 = best); Infinity if unknown
 */
export function sourcePriorityRank(claimType, sourceType) {
  const list = SOURCE_HIERARCHY_BY_CLAIM[claimType] || [];
  const idx = list.indexOf(sourceType);
  return idx >= 0 ? idx : Number.POSITIVE_INFINITY;
}

/**
 * Prefer newer evidence when authority (source type) is comparable.
 * @param {{ sourceType?: string, sourceDate?: string|null, evidenceRetrievalDate?: string|null }} a
 * @param {{ sourceType?: string, sourceDate?: string|null, evidenceRetrievalDate?: string|null }} b
 * @param {string} claimType
 */
export function preferEvidence(a, b, claimType) {
  const rankA = sourcePriorityRank(claimType, a.sourceType || "unknown");
  const rankB = sourcePriorityRank(claimType, b.sourceType || "unknown");
  if (rankA !== rankB) return rankA < rankB ? a : b;

  const dateA = parseLooseDate(a.sourceDate) || parseLooseDate(a.evidenceRetrievalDate);
  const dateB = parseLooseDate(b.sourceDate) || parseLooseDate(b.evidenceRetrievalDate);
  if (dateA && dateB && dateA.getTime() !== dateB.getTime()) {
    return dateA > dateB ? a : b;
  }
  return a;
}

/**
 * @param {string|null|undefined} raw
 * @returns {Date|null}
 */
export function parseLooseDate(raw) {
  if (!raw) return null;
  const d = new Date(String(raw));
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * When older Pipeline evidence conflicts with newer official Open → Superseded.
 * @param {{ value: string, sourceType?: string, sourceDate?: string|null, evidenceRetrievalDate?: string|null }} older
 * @param {{ value: string, sourceType?: string, sourceDate?: string|null, evidenceRetrievalDate?: string|null }} newer
 * @param {string} claimType
 */
export function resolveTemporalConflict(older, newer, claimType) {
  const preferred = preferEvidence(newer, older, claimType);
  const dateOlder = parseLooseDate(older.sourceDate) || parseLooseDate(older.evidenceRetrievalDate);
  const dateNewer = parseLooseDate(newer.sourceDate) || parseLooseDate(newer.evidenceRetrievalDate);
  const ranksComparable =
    Math.abs(
      sourcePriorityRank(claimType, older.sourceType || "unknown") -
        sourcePriorityRank(claimType, newer.sourceType || "unknown")
    ) <= 1;

  if (
    ranksComparable &&
    dateOlder &&
    dateNewer &&
    dateNewer > dateOlder &&
    normalizeStatusPair(older.value, newer.value)
  ) {
    return {
      claimStatus: "Superseded",
      winningValue: preferred.value,
      reason: "Newer comparable-authority evidence supersedes older value",
    };
  }

  const rankGap =
    sourcePriorityRank(claimType, older.sourceType || "unknown") -
    sourcePriorityRank(claimType, newer.sourceType || "unknown");
  if (Math.abs(rankGap) >= 2) {
    return {
      claimStatus: "Contradicted",
      winningValue: preferEvidence(newer, older, claimType).value,
      reason: "Higher-authority source preferred over weaker conflicting source",
    };
  }

  return {
    claimStatus: "Conflicting Evidence",
    winningValue: null,
    reason: "Evidence dates unclear or authority differs materially",
  };
}

function normalizeStatusPair(a, b) {
  const na = String(a || "").toLowerCase();
  const nb = String(b || "").toLowerCase();
  if (!na || !nb || na === nb) return false;
  const pipelineOpen =
    (/pipeline|coming soon/.test(na) && /open|operating|bookable/.test(nb)) ||
    (/pipeline|coming soon/.test(nb) && /open|operating|bookable/.test(na));
  return pipelineOpen || true;
}
