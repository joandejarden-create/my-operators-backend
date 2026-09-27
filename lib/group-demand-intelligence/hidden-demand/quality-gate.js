/**
 * Quality gates for hidden-demand entity names / evidence.
 * Rejects SERP noise (stopwords, SVG, CSS, truncated tokens).
 */

const STOP_ORGS = new Set(
  [
    "a",
    "an",
    "the",
    "your",
    "our",
    "this",
    "that",
    "these",
    "those",
    "require",
    "required",
    "legit",
    "hidden",
    "official",
    "accept",
    "student",
    "home",
    "new",
    "york",
    "city",
    "http",
    "https",
    "www",
    "svg",
    "pdf",
    "click",
    "here",
    "read",
    "more",
    "post",
    "by",
    "uk",
    "us",
    "agenda",
    "speakers",
    "who",
    "attends",
    "participating",
    "entity",
    "trade",
    "shows",
    "appliances",
    "united",
    "state",
    "times",
    "square",
    "luxury",
    "hotels",
    "small",
    "travel",
    "groups",
    "must",
    "contact",
    "style",
    "dream",
    "team",
    "music",
    "cities",
    "america",
    "gift",
    "certificates",
    "rkiye",
    "turkey",
    "global",
    "influencer",
    "marketing",
  ].map((s) => s.toLowerCase())
);

export function isPlausibleOrganizationName(name = "") {
  const raw = String(name || "").trim();
  if (raw.length < 4 || raw.length > 100) return false;
  if (/^[^A-Za-z]/.test(raw)) return false; // no leading punctuation / digits-only junk
  if (/^https?:/i.test(raw) || /w3\.org|\/svg|:style=/i.test(raw)) return false;
  if (/participating entity$/i.test(raw)) return false;
  if (!/[a-zA-Z]{3,}/.test(raw)) return false;

  const tokens = raw
    .toLowerCase()
    .replace(/[^a-z0-9\s&.]/g, " ")
    .split(/\s+/)
    .filter(Boolean);
  if (!tokens.length) return false;
  if (tokens.length === 1) return false; // single-token org names too noisy from SERP

  const meaningful = tokens.filter((t) => !STOP_ORGS.has(t) && t.length > 2);
  if (meaningful.length < 1) return false;

  // Require at least one Capitalized token in original (proper-noun signal)
  const caps = String(raw).match(/\b[A-Z][a-zA-Z0-9&.-]{2,}\b/g) || [];
  if (caps.length < 1) return false;

  // Reject if majority of tokens are stopwords
  if (meaningful.length / tokens.length < 0.35) return false;

  return true;
}

/**
 * Hidden-demand row must pass entity + evidence minimums before hotel match / promote.
 */
export function passesHiddenDemandQualityGate(hd = {}) {
  const org = hd.organizationName || "";
  const title = hd.title || "";
  if (!isPlausibleOrganizationName(org) && !isPlausibleOrganizationName(title)) {
    return { ok: false, reason: "IMPLAUSIBLE_ORG" };
  }
  // Prefer organizationName when present
  if (org && !isPlausibleOrganizationName(org)) {
    return { ok: false, reason: "IMPLAUSIBLE_ORG_NAME" };
  }
  if (!hd.evidence?.entityExists && !hd.entityFirst) {
    return { ok: false, reason: "NO_ENTITY_EVIDENCE" };
  }
  if (hd.sourceUrl && /w3\.org|\/svg|javascript:|:style=/i.test(hd.sourceUrl)) {
    return { ok: false, reason: "JUNK_SOURCE" };
  }
  // Prefer lodging-supported or two-layer depth for promotion path
  return { ok: true, reason: null };
}
