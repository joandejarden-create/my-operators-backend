/**
 * Brand Explorer — Recent Momentum permanent contract (template for all brands).
 *
 * Product standard (active + future brands):
 * - Named opening / conversion / membership / development-press cards
 * - Body = date line + blank line + summary + blank line + https announcement URL
 * - Newest → oldest display order
 * - Proper Case hyperlink labels generated from the trailing URL
 * - No untitled diligence blobs, no development-page filler without openings framing
 *
 * Used by section_pattern_parity, content packs, and momentum restore tooling.
 */

import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

export const RECENT_MOMENTUM_CONTRACT_VERSION = "recent-momentum-contract-v1";

export const RECENT_MOMENTUM_SLOT = "footprint.momentum";
export const RECENT_MOMENTUM_LABEL_SLOT = "footprint.momentum_label";

/** Minimum named cards for an active / release-candidate profile. */
export const RECENT_MOMENTUM_MIN_CARDS = 2;
export const RECENT_MOMENTUM_MIN_LINKED_URLS = 2;
export const RECENT_MOMENTUM_MIN_STRUCTURED_DATES = 2;

export const RECENT_MOMENTUM_DEFAULT_LABEL = "Recent openings & pipeline · linked announcements";

export const RECENT_MOMENTUM_FORBIDDEN_PATTERNS = Object.freeze([
  /\bowner diligence:\b/i,
  /\bwhere (?:named |property-level )?(?:opening )?press is (?:thin|limited)\b/i,
  /\bnot press-kit filler\b/i,
  /\billustrative activity\b/i,
  /\bdirectional themes?\b/i,
  /\bbrand press resources updated\b/i,
  /\bdevelopment page signals\b/i,
]);

/**
 * Canonical Body shape for one momentum card.
 * @param {{ dateLine: string, summary: string, sourceUrl: string }} args
 */
export function buildRecentMomentumBody(args) {
  return buildMomentumBody(args);
}

/**
 * Template helper for content packs / future brand builds.
 * Always embeds the announcement URL (PVQL exceptSlots: footprint.momentum / openings).
 */
export function buildRecentMomentumCard({ title, dateLine, summary, url, sort = 1 }) {
  const t = String(title || "").trim();
  const d = String(dateLine || "").trim();
  const s = String(summary || "").trim();
  const u = String(url || "").trim();
  if (!t || !d || !s || !u) {
    throw new Error(
      "Recent Momentum card requires title, dateLine, summary, and https sourceUrl (openings/press template)."
    );
  }
  if (!/^https?:\/\//i.test(u)) {
    throw new Error(`Recent Momentum sourceUrl must be https: ${u}`);
  }
  return {
    title: t,
    dateLine: d,
    summary: s,
    url: u,
    sort: Number.isFinite(sort) ? sort : 1,
    body: buildRecentMomentumBody({ dateLine: d, summary: s, sourceUrl: u }),
  };
}

/**
 * Sort cards newest → oldest by dateLine (YYYY, Mon YYYY, YYYY–YYYY, Qn YYYY).
 */
export function recentMomentumDateSortKey(dateLabel) {
  const t = String(dateLabel || "").trim();
  if (!t) return 0;
  const range = t.match(/^(\d{4})\s*[–—-]\s*(\d{4})$/);
  if (range) return parseInt(range[2], 10) * 100 + 12;
  const mon = t.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{4})$/i);
  if (mon) {
    const monthMap = {
      jan: 1,
      feb: 2,
      mar: 3,
      apr: 4,
      may: 5,
      jun: 6,
      jul: 7,
      aug: 8,
      sep: 9,
      oct: 10,
      nov: 11,
      dec: 12,
    };
    const m = monthMap[mon[1].slice(0, 3).toLowerCase()] || 12;
    return parseInt(mon[2], 10) * 100 + m;
  }
  const q = t.match(/^Q([1-4])\s+(\d{4})$/i);
  if (q) return parseInt(q[2], 10) * 100 + parseInt(q[1], 10) * 3;
  const y = t.match(/^(\d{4})$/);
  if (y) return parseInt(y[1], 10) * 100 + 12;
  return 0;
}

export function sortRecentMomentumCardsNewestFirst(cards) {
  return [...(cards || [])].sort((a, b) => {
    const db = recentMomentumDateSortKey(b.dateLine || b.date);
    const da = recentMomentumDateSortKey(a.dateLine || a.date);
    if (db !== da) return db - da;
    return String(b.title || b.headline || "").localeCompare(String(a.title || a.headline || ""));
  });
}

/**
 * Assign Sort Order 1..n after newest-first ordering (Airtable + renderer alignment).
 */
export function withRecentMomentumSortOrder(cards) {
  return sortRecentMomentumCardsNewestFirst(cards).map((c, i) => ({
    ...c,
    sort: i + 1,
  }));
}

export function isStructuredMomentumDateLine(dateLine) {
  const t = String(dateLine || "").trim();
  return (
    /^\d{4}(\s*[–—-]\s*\d{4})?$/i.test(t) ||
    /^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{4}$/i.test(t) ||
    /^Q[1-4]\s+\d{4}$/i.test(t) ||
    /^(Directory|Collection|Editorial|Affiliation|Pipeline)$/i.test(t)
  );
}

export function looksLikeDiligenceFillerMomentum(text) {
  const blob = String(text || "");
  return RECENT_MOMENTUM_FORBIDDEN_PATTERNS.some((re) => re.test(blob));
}

/**
 * Contract checklist summary for docs / audits.
 */
export function recentMomentumContractChecklist() {
  return {
    version: RECENT_MOMENTUM_CONTRACT_VERSION,
    slots: [RECENT_MOMENTUM_SLOT, RECENT_MOMENTUM_LABEL_SLOT],
    bodyFormat: "dateLine\\n\\nsummary\\n\\nhttps://announcement-url",
    displayOrder: "newest_to_oldest",
    linkLabels: "Proper Case from trailing Body URL (frontend)",
    minCards: RECENT_MOMENTUM_MIN_CARDS,
    minLinkedUrls: RECENT_MOMENTUM_MIN_LINKED_URLS,
    minStructuredDates: RECENT_MOMENTUM_MIN_STRUCTURED_DATES,
    forbidden: [
      "untitled directional blobs",
      "owner-diligence filler without openings/press",
      "raw Body without trailing announcement URL when section is shown",
      "illustrative / source-note framing",
    ],
    futureBrandRequirement:
      "Add brand-explorer-section-pattern-parity-content-<slug>.js with momentumCards via buildRecentMomentumCard, register in CONTENT_BY_SLUG, then run section-pattern-parity + momentum-announcement-link-restore.",
  };
}
