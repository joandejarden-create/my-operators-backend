/**
 * Shared Target Guest Segment vocabulary (Brand Basics multi + Deal SI multi).
 * Authority: docs/target-guest-segment-vocabulary.md + reports/brand-vs-deal-select-options-cleanup-checklist.md
 */

/** Canonical KEEP — identical option strings on Brand and Deal (both multi-select). */
export const TARGET_GUEST_SEGMENT_KEEP = Object.freeze([
  "Corporate / Business",
  "Leisure",
  "Bleisure",
  "Family",
  "Solo Traveler",
  "Wellness Seeker",
  "Group / MICE",
  "Contract / Extended Stay",
  "Government / Military",
  "International Inbound",
  "Staycation / Local",
  "Digital Nomad",
  "Luxury / Discerning",
  "Experience-Oriented",
]);

/** @deprecated Deal form no longer uses Other; kept for Meta prune / legacy */
export const TARGET_GUEST_SEGMENT_DEAL_ONLY = Object.freeze(["Other"]);

/** Deal KEEP matches Brand (Other is obsolete on form) */
export const TARGET_GUEST_SEGMENT_DEAL_KEEP = TARGET_GUEST_SEGMENT_KEEP;

/** Brand multi-select KEEP (same as Deal) */
export const TARGET_GUEST_SEGMENT_BRAND_KEEP = TARGET_GUEST_SEGMENT_KEEP;

/** Exact old → new for structured remaps */
export const TARGET_GUEST_SEGMENT_REMAP = Object.freeze({
  Business: "Corporate / Business",
  "Group / Events": "Group / MICE",
  "Bleisure (Business + Leisure)": "Bleisure",
  "Family Leisure": "Family",
  "Convention / Meetings": "Group / MICE",
  "Tour Groups": "Group / MICE",
});

export const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
export const BRAND_GUEST_SEGMENTS_FIELD = "Target Guest Segments";
export const DEAL_SI_TABLE = "Strategic Intent - Operational - Key Challenges";
export const DEAL_GUEST_SEGMENT_FIELD = "Target Guest Segment";
export const DEAL_GUEST_SEGMENT_OTHER_FIELD = "Target Guest Segment Other Text";

export function isCanonicalBrandSegment(name) {
  return TARGET_GUEST_SEGMENT_BRAND_KEEP.includes(String(name || "").trim());
}

export function isCanonicalDealSegment(name) {
  return TARGET_GUEST_SEGMENT_DEAL_KEEP.includes(String(name || "").trim());
}

/**
 * Remap one Brand multi-select list. Drops unknowns after remap (should not happen if Meta rename ran).
 * @param {string[]} values
 * @returns {{ next: string[], remapped: { from: string, to: string }[], dropped: string[] }}
 */
export function remapBrandGuestSegments(values) {
  const remapped = [];
  const dropped = [];
  const out = [];
  const seen = new Set();
  for (const raw of values || []) {
    const v = String(raw || "").trim();
    if (!v) continue;
    const to = TARGET_GUEST_SEGMENT_REMAP[v] || v;
    if (to !== v) remapped.push({ from: v, to });
    if (!isCanonicalBrandSegment(to)) {
      dropped.push(v);
      continue;
    }
    if (seen.has(to)) continue;
    seen.add(to);
    out.push(to);
  }
  return { next: out, remapped, dropped };
}

/**
 * Remap Deal guest segment value(s). Accepts string or string[].
 * Free-text / non-canonical values are dropped (wording preserved in otherText when provided).
 * @param {string|string[]|null|undefined} value
 * @param {string|null|undefined} existingOther
 */
export function remapDealGuestSegment(value, existingOther = "") {
  const other = String(existingOther || "").trim();
  const rawList = Array.isArray(value)
    ? value
    : value == null || value === ""
      ? []
      : [value];
  const remapped = [];
  const dropped = [];
  const out = [];
  const seen = new Set();
  for (const raw of rawList) {
    const v = String(raw || "").trim();
    if (!v) continue;
    const to = TARGET_GUEST_SEGMENT_REMAP[v] || v;
    if (to !== v) remapped.push({ from: v, to });
    if (!isCanonicalDealSegment(to)) {
      // Drop obsolete Meta choice "Other" without appending the word into Other Text
      if (v !== "Other") dropped.push(v);
      continue;
    }
    if (seen.has(to)) continue;
    seen.add(to);
    out.push(to);
  }
  const mergedOther =
    dropped.length > 0
      ? other
        ? `${other} | ${dropped.join(" | ")}`
        : dropped.join(" | ")
      : other || null;
  return {
    next: out.length ? out : null,
    otherText: mergedOther,
    remapped: remapped.length > 0 || dropped.length > 0,
    toOther: dropped.length > 0,
    from: remapped.map((r) => r.from).concat(dropped),
  };
}
