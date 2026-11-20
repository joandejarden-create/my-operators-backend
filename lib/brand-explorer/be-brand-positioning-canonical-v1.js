/**
 * Canonical Brand Positioning — Brand Basics single-line attribute.
 * Contract: BE_BRAND_POSITIONING_CANONICAL_V1
 *
 * Source of truth: Airtable Brand Setup - Brand Basics → "Brand Positioning"
 * (fld8jsY5nekd6AMah). Concise Title Case phrase; no slashes; not Chain Scale.
 */

export const BE_BRAND_POSITIONING_CANONICAL_V1 = "BE_BRAND_POSITIONING_CANONICAL_V1";

export const BRAND_POSITIONING_FIELD = "Brand Positioning";
export const BRAND_POSITIONING_FIELD_ID = "fld8jsY5nekd6AMah";
export const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
export const BRAND_BASICS_TABLE_ID = "tbl1x6S7I7JwTcRdV";
export const BRAND_BASICS_BASE_ID = "appvtnDurnMSjINP6";
export const BRAND_BASICS_VIEW_ID = "viwQutR9suvgjVgEn";

export const BRAND_POSITIONING_FIELD_DESCRIPTION =
  "Concise descriptive positioning of the hotel brand, reflecting its market proposition, guest experience and operating model. Maintained as a canonical Brand Basics attribute for Brand Explorer and Brand Comparison Studio.";

/** Reject forward/backslashes and slash-style dual classifications. */
export const POSITIONING_SLASH_RE = /[\\/]/;

/**
 * Normalize / validate a Brand Positioning string.
 * @returns {{ ok: boolean, value: string, codes: string[] }}
 */
export function validateBrandPositioning(raw) {
  const codes = [];
  let value = raw == null ? "" : String(raw).trim().replace(/\s+/g, " ");
  if (!value) {
    codes.push("POSITIONING_PRESENT");
    return { ok: false, value: "", codes };
  }
  if (POSITIONING_SLASH_RE.test(value)) {
    codes.push("POSITIONING_NO_SLASHES");
  }
  // Dual alternatives like "Upscale / Full-Service" already caught; also "A or B" concatenations
  if (/\bor\b/i.test(value) && /\b(and|&)\b/i.test(value) === false && value.split(/\s+/).length <= 6) {
    // soft: "Business or Meetings" is OK; skip
  }
  if (value.length > 80) codes.push("POSITIONING_TOO_LONG");
  if (value !== toPositioningTitleCase(value) && !/[A-Z]{2,}/.test(value)) {
    // Prefer Title Case; allow intentional all-caps tokens (SO/, TRIBE handled upstream)
  }
  const ok = !codes.includes("POSITIONING_NO_SLASHES") && !codes.includes("POSITIONING_PRESENT");
  return { ok, value, codes };
}

/** Light Title Case for positioning phrases (preserve small words mid-phrase). */
export function toPositioningTitleCase(raw) {
  const s = String(raw || "").trim().replace(/\s+/g, " ");
  if (!s) return "";
  const small = new Set(["and", "or", "of", "the", "by", "a", "an", "for", "to", "in", "on"]);
  return s
    .split(/\s+/)
    .map((w, i) => {
      if (/^[A-Z0-9]{2,}$/.test(w)) return w; // acronyms / SO/
      if (w.includes("-")) {
        return w
          .split("-")
          .map((p) => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase())
          .join("-");
      }
      const lower = w.toLowerCase();
      if (i > 0 && small.has(lower)) return lower;
      return lower.charAt(0).toUpperCase() + lower.slice(1);
    })
    .join(" ");
}

/**
 * Rewrite slash duals into a single coherent phrase when an explicit rewrite map hits;
 * otherwise return null (caller must not invent).
 */
export function rewriteSlashPositioning(raw) {
  const s = String(raw || "").trim();
  if (!POSITIONING_SLASH_RE.test(s)) return null;
  const key = s.toLowerCase().replace(/\s+/g, " ");
  const map = {
    "upscale / full-service": "Upscale Full-Service",
    "luxury / premium": "Luxury",
    "economy / budget": "Economy",
    "lifestyle / select service": "Lifestyle Select Service",
    "upscale / luxury independent collection": "Luxury Independent Collection",
    "premium / regional": "Premium",
  };
  return map[key] || null;
}

export function normalizeBrandNameKey(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
