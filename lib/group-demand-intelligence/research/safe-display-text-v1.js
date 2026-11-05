/**
 * Safe display-text extraction for GDI research mappers.
 *
 * Never coerces nested objects via String(obj) → "[object Object]".
 * Shared by Scaling / Events / Apify / FullE2E orchestrators.
 */
export const SAFE_DISPLAY_TEXT_V1 = "gdi-safe-display-text-v1";

const NESTED_KEYS = Object.freeze([
  "legalOrTradingName",
  "canonicalName",
  "summary",
  "claim",
  "text",
  "body",
  "thesis",
  "mechanism",
  "label",
  "title",
  "name",
  "companyName",
  "organizationName",
  "note",
  "value",
  "triggerType",
  "whyThisHotel",
]);

/**
 * Coerce any research field to a clean display string.
 * Objects → first nested scalar key; arrays → joined scalars; else String.
 */
export function safeDisplayText(v) {
  if (v == null) return "";
  if (typeof v === "string") {
    return v.replace(/\s+/g, " ").trim().replace(/^\[object Object\]$/i, "");
  }
  if (typeof v === "number" || typeof v === "boolean") {
    return String(v).replace(/\s+/g, " ").trim();
  }
  if (Array.isArray(v)) {
    return v
      .map((x) => safeDisplayText(x))
      .filter(Boolean)
      .join(" · ")
      .replace(/\s+/g, " ")
      .trim();
  }
  if (typeof v === "object") {
    for (const k of NESTED_KEYS) {
      if (v[k] != null && v[k] !== v) {
        const t = safeDisplayText(v[k]);
        if (t) return t;
      }
    }
    return "";
  }
  return String(v)
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^\[object Object\]$/i, "");
}

/** First non-empty safeDisplayText among candidates. */
export function firstSafeDisplayText(...vals) {
  for (const v of vals) {
    const t = safeDisplayText(v);
    if (t && !/^\[object Object\]$/i.test(t)) return t;
  }
  return "";
}

/**
 * Resolve a company / opportunity identity from mixed discovery shapes.
 */
export function resolveCompanyIdentityName(candidate = {}) {
  const id = candidate.companyIdentity;
  if (typeof id === "string") return safeDisplayText(id);
  if (id && typeof id === "object") {
    return firstSafeDisplayText(
      id.legalOrTradingName,
      id.canonicalName,
      id.name,
      id.companyName,
      id.legalName,
    );
  }
  return firstSafeDisplayText(
    candidate.organizationName,
    candidate.companyName,
    candidate.venueName,
    candidate.plannerCompanyName,
    candidate.organizingEntity,
    candidate.verifiedEventName,
    candidate.eventIdentity,
    candidate.name,
    candidate.title,
  );
}

/**
 * True when a string is the classic broken serialization artifact.
 */
export function isBrokenObjectString(v) {
  return /\[object Object\]|Unresolved — target supplied/i.test(
    safeDisplayText(v),
  );
}
