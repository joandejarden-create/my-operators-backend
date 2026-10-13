/**
 * Competitor-use evidence strength.
 * Phone/address/name co-occurrence alone = DISCOVERY_ONLY (never proof of stay).
 */

export const EVIDENCE_CLASS = Object.freeze({
  DIRECT_CONFIRMED: "DIRECT_CONFIRMED",
  STRONG_ASSOCIATION: "STRONG_ASSOCIATION",
  WEAK_ASSOCIATION: "WEAK_ASSOCIATION",
  DISCOVERY_ONLY: "DISCOVERY_ONLY",
});

const DIRECT_RE =
  /\b(official hotel|host hotel|hotel block|room block|accommodated at|staying at|housing at|preferred hotel|hébergement officiel|hotel oficial|bloque de habitaciones|venue:\s*|held at)\b/i;
const STRONG_RE =
  /\b(accommodation|housing|lodging|hébergement|alojamiento|hotel list|partner hotel|overflow hotel|conference hotel)\b/i;
const WEAK_RE =
  /\b(nearby hotels|hotels near|where to stay|accommodation options|area hotels|hotels in)\b/i;

/**
 * Classify page-level competitor-use evidence.
 * @param {{ competitorName, pageText, pageUrl, pivotType, title, snippet }} ctx
 */
export function classifyCompetitorUseEvidence(ctx = {}) {
  const name = String(ctx.competitorName || "");
  const blob = `${ctx.title || ""} ${ctx.snippet || ""} ${ctx.pageText || ""} ${ctx.pageUrl || ""}`;
  const norm = (s) =>
    String(s || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/ö/g, "o")
      .replace(/ä/g, "a")
      .replace(/ü/g, "u");
  const nName = norm(name);
  const nBlob = norm(blob);
  const namePresent =
    Boolean(name) &&
    (nBlob.includes(nName) ||
      nName
        .split(/\s+/)
        .filter((w) => w.length > 3)
        .slice(0, 3)
        .every((w) => nBlob.includes(w)));

  const phonePivot = /PUBLIC_PHONE/i.test(String(ctx.pivotType || ""));
  const groupHint =
    /\b(conference|congress|congrès|congreso|meeting|summit|tournament|delegation|wedding|incentive|association|society|federation|kickoff|retreat|symposium)\b/i.test(
      blob
    );

  if (!namePresent && phonePivot) {
    return {
      evidenceClass: EVIDENCE_CLASS.DISCOVERY_ONLY,
      reason: "PHONE_CO_OCCURRENCE_WITHOUT_COMPETITOR_CONTEXT",
      feedsDeeperResearch: false,
      fact: "Phone appeared on a page; competitor stay not confirmed.",
      inference: null,
      unknown: "Whether any group used this hotel",
    };
  }

  if (!namePresent) {
    return {
      evidenceClass: EVIDENCE_CLASS.DISCOVERY_ONLY,
      reason: "COMPETITOR_NAME_NOT_CONFIRMED_ON_PAGE",
      feedsDeeperResearch: false,
      fact: null,
      inference: null,
      unknown: "Page may be unrelated to competitor hotel",
    };
  }

  if (DIRECT_RE.test(blob) && groupHint) {
    return {
      evidenceClass: EVIDENCE_CLASS.DIRECT_CONFIRMED,
      reason: "OFFICIAL_OR_EXPLICIT_HOTEL_HOUSING_NAMING",
      feedsDeeperResearch: true,
      fact: `${name} explicitly named in hotel/housing/venue context with group/event language.`,
      inference: null,
      unknown: "Room block size and buyer identity may still be unknown",
    };
  }

  if (STRONG_RE.test(blob) && groupHint) {
    return {
      evidenceClass: EVIDENCE_CLASS.STRONG_ASSOCIATION,
      reason: "EVENT_GROUP_TIED_TO_COMPETITOR_ACCOMMODATION_CONTEXT",
      feedsDeeperResearch: true,
      fact: `${name} appears in accommodation/housing context for a group/event.`,
      inference: "Likely lodging relationship; exact block not necessarily stated.",
      unknown: "Whether competitor was official host vs listed option",
    };
  }

  if (WEAK_RE.test(blob) || (namePresent && groupHint && !STRONG_RE.test(blob) && !DIRECT_RE.test(blob))) {
    return {
      evidenceClass: EVIDENCE_CLASS.WEAK_ASSOCIATION,
      reason: "GENERIC_NEARBY_OR_LOOSE_MENTION",
      feedsDeeperResearch: false,
      fact: `${name} mentioned near event/group content.`,
      inference: "Insufficient to claim lodging use.",
      unknown: "Whether any attendees stayed at competitor",
    };
  }

  if (phonePivot || /NAME|ADDRESS|DOMAIN|BRAND/i.test(String(ctx.pivotType || ""))) {
    return {
      evidenceClass: EVIDENCE_CLASS.DISCOVERY_ONLY,
      reason: "PIVOT_HIT_INSUFFICIENT_GROUP_CONTEXT",
      feedsDeeperResearch: false,
      fact: "Public identifier matched a page.",
      inference: null,
      unknown: "Group/event lodging relationship",
    };
  }

  return {
    evidenceClass: EVIDENCE_CLASS.DISCOVERY_ONLY,
    reason: "INSUFFICIENT_CONTEXT",
    feedsDeeperResearch: false,
    fact: null,
    inference: null,
    unknown: "Competitor-use relationship",
  };
}

export function canFeedDeeperOpportunityResearch(evidenceClass) {
  return (
    evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED ||
    evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION
  );
}
