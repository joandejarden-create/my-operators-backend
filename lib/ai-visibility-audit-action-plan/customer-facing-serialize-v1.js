/**
 * AVA customer-facing serialization — never emit [object Object] / raw enums.
 */
export const AVA_CUSTOMER_FACING_SERIALIZE_V1 =
  "AVA_CUSTOMER_FACING_SERIALIZE_V1";

const FORBIDDEN_VISIBLE = [
  /\[object Object\]/i,
  /\[object Array\]/i,
  /\bundefined\b/,
  /\bnull\b/,
  /^\s*\{\s*"/,
  /^\s*\[\s*\{/,
];

/**
 * Extract a usable http(s) URL from mixed evidence shapes.
 */
export function extractHttpUrl(value) {
  if (!value) return null;
  if (typeof value === "string") {
    const t = value.trim();
    if (/^https?:\/\//i.test(t)) return t;
    return null;
  }
  if (typeof value === "object") {
    for (const k of [
      "url",
      "href",
      "referringUrl",
      "linkingPageUrl",
      "evidenceUrl",
      "targetOrganizationUrl",
      "hotelDestinationUrl",
      "sourceUrl",
    ]) {
      const u = extractHttpUrl(value[k]);
      if (u) return u;
    }
  }
  return null;
}

/**
 * Human-readable label for a page / platform reference.
 */
export function customerPageLabel(value, fallback = "Verified page") {
  if (value == null || value === "") return fallback;
  if (typeof value === "string") {
    const t = value.trim();
    if (!t || FORBIDDEN_VISIBLE.some((re) => re.test(t))) return fallback;
    if (/^https?:\/\//i.test(t)) {
      try {
        const u = new URL(t);
        const host = u.hostname.replace(/^www\./, "");
        const path = u.pathname.replace(/\/$/, "");
        if (/meeting/i.test(path)) return "Hotel Meetings Page";
        if (/restaurant|dining/i.test(path)) return "Hotel Dining Page";
        if (path === "" || path === "/") return host.includes("hilton")
          ? "Hilton Property Profile"
          : host.includes("marriott")
            ? "Brand Property Profile"
            : "Official Hotel Website";
        return `${host}${path.length > 40 ? path.slice(0, 40) + "…" : path}`;
      } catch {
        return fallback;
      }
    }
    // Internal enum-ish → readable
    if (/^[A-Z0-9_]+$/.test(t) && t.includes("_")) {
      return t
        .split("_")
        .map((w) => w.charAt(0) + w.slice(1).toLowerCase())
        .join(" ");
    }
    return t;
  }
  if (typeof value === "object") {
    const label =
      value.label ||
      value.title ||
      value.name ||
      value.platform ||
      value.pageLabel ||
      null;
    if (label && typeof label === "string" && label.trim()) {
      return customerPageLabel(label.trim(), fallback);
    }
    const url = extractHttpUrl(value);
    if (url) return customerPageLabel(url, fallback);
  }
  return fallback;
}

export function customerVisibleText(value, fallback = "") {
  if (value == null) return fallback;
  if (typeof value === "string") {
    const t = value.trim();
    if (FORBIDDEN_VISIBLE.some((re) => re.test(t))) return fallback;
    return t;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) {
    return value
      .map((v) => customerVisibleText(v, ""))
      .filter(Boolean)
      .join("; ");
  }
  if (typeof value === "object") {
    return customerPageLabel(value, fallback);
  }
  return fallback;
}

/**
 * Scan HTML/text for forbidden serialization artifacts.
 */
export function detectSerializationArtifacts(text, section = "customer") {
  const defects = [];
  const s = String(text || "");
  for (const re of FORBIDDEN_VISIBLE) {
    if (re.test(s)) {
      defects.push({
        type: "CUSTOMER_SERIALIZATION_ARTIFACT",
        severity: "CRITICAL",
        section,
        message: `Customer-facing text contains forbidden serialization artifact matching ${re}`,
        match: (s.match(re) || [])[0] || null,
      });
    }
  }
  return defects;
}
