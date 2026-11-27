/**
 * Customer-facing SUPPORTS labels for Hotel Explorer Sources & Evidence.
 * Internal field codes / HI graph notes stay in research records; the SUPPORTS
 * column must never show Airtable IDs, HI graph jargon, or relationship-edge copy.
 */

export const HE_CUSTOMER_SUPPORTS_LABEL_V1 = "he-customer-supports-label-v1";

/** Technical / internal tokens forbidden in customer-facing SUPPORTS cells. */
export const FORBIDDEN_SUPPORTS_PATTERNS = [
  /\brec[a-zA-Z0-9]{10,}\b/i,
  /\bHI\s*graph\b/i,
  /\bHI Graph Evidence\b/i,
  /\bownership edges?\b/i,
  /\brelationship edges?\b/i,
  /\bValidated HI\b/i,
  /\bAirtable\b/i,
  /\bUBO\b/,
  /\bfield[_ ]?name\b/i,
  /\bVERIFIED_HI\b/i,
  /\bsourcesForUi\b/i,
  /\bHI Hotel–Org\b/i,
  /\bHI Hotel-Org\b/i,
];

const SUPPORTS_CODE_LABELS = {
  ownership: "Hotel Ownership Structure",
  history: "Property Acquisition",
  operator: "Hotel Management",
  brand: "Brand Affiliation",
  brand_census: "Brand Affiliation",
  contact_research: "Hotel Leadership",
  market_alerts_hotel_fact: "Property News & Developments",
  guest_sentiment: "Guest Reputation",
  meeting_space: "Meeting Space & Facilities",
  rooms: "Guest Rooms & Facilities",
};

const FIELD_CODE_LABELS = [
  { re: /\b(legal[_ ]?owner|property[_ ]?company|propco|legal[_ ]?property)\b/i, label: "Legal Property Ownership" },
  { re: /\b(economic[_ ]?owner|economic[_ ]?sponsor|sponsor[_ ]?attribution)\b/i, label: "Economic Sponsorship" },
  { re: /\b(operator|managed[_ ]?by|management[_ ]?company|asset_agreement_profile|ops\s*&\s*investment)\b/i, label: "Hotel Management" },
  { re: /\b(brand|affiliation|franchise|star[_ ]?rating|starrating|reflag|breathless|hyatt|marriott|hilton)\b/i, label: "Brand Affiliation" },
  { re: /\b(property[_ ]?history|property[_ ]?acquisition|sale announced|sold to|acquisition financing)\b/i, label: "Property Acquisition" },
  { re: /\b(cmbs|financing|loan|mortgage|debt)\b/i, label: "Property Financing" },
  { re: /\b(meeting[_ ]?space|ballroom|meeting[_ ]?room)\b/i, label: "Meeting Space & Facilities" },
  { re: /\b(guest[_ ]?room|guestrooms?|keys|rooms?)\b/i, label: "Guest Rooms & Facilities" },
  { re: /\b(leadership|general[_ ]?manager|\bgm\b|director(?:a)? de ventas|director of sales|linkedin|hotel leadership)\b/i, label: "Hotel Leadership" },
  { re: /\b(ownership[_ ]?structure|ownership[_ ]?control|third-party owned|reporte anual|issuer profile|ownership)\b/i, label: "Hotel Ownership Structure" },
  { re: /\b(renovation|ampliaci[oó]n|expansion)\b/i, label: "Property News & Developments" },
];

function firstNonEmpty() {
  for (let i = 0; i < arguments.length; i += 1) {
    const v = arguments[i];
    if (v == null) continue;
    const s = String(v).trim();
    if (s) return s;
  }
  return "";
}

export function containsForbiddenSupportsTerminology(text) {
  const s = String(text || "");
  if (!s) return false;
  return FORBIDDEN_SUPPORTS_PATTERNS.some((re) => re.test(s));
}

function labelsFromFieldBlob(blob) {
  const text = String(blob || "");
  if (!text) return [];
  const out = [];
  const seen = new Set();
  for (const row of FIELD_CODE_LABELS) {
    if (!row.re.test(text)) continue;
    if (seen.has(row.label)) continue;
    seen.add(row.label);
    out.push(row.label);
  }
  return out;
}

function labelsFromClaims(claims) {
  const out = [];
  const seen = new Set();
  for (const raw of claims || []) {
    const blob = String(raw || "").trim();
    if (!blob) continue;
    const fieldKey = blob.includes(":") ? blob.slice(0, blob.indexOf(":")) : blob;
    const found = [
      ...labelsFromFieldBlob(fieldKey),
      ...labelsFromFieldBlob(blob),
    ];
    for (const label of found) {
      if (seen.has(label)) continue;
      seen.add(label);
      out.push(label);
    }
  }
  return out;
}

/**
 * Build a concise customer-facing SUPPORTS cell from a citation / evidence row.
 * Prefer source-specific claims over a shared register-level claim.
 */
export function customerSupportsLabel(citation = {}) {
  const supportsCode = String(citation.supports || citation.support || "").trim();
  const perSourceClaim = firstNonEmpty(
    citation.source_claim,
    Array.isArray(citation.claims) ? citation.claims.join(" | ") : "",
    citation.fieldName,
    citation.field_name,
    // Prefer explicit per-source claim only when it is not the shared register dump.
    citation.claim && !containsForbiddenSupportsTerminology(citation.claim)
      ? citation.claim
      : "",
    citation.proves && !containsForbiddenSupportsTerminology(citation.proves)
      ? citation.proves
      : "",
    citation.evidence_excerpt,
    citation.excerpt,
    citation.title
  );

  const claimList = Array.isArray(citation.claims) && citation.claims.length
    ? citation.claims
    : perSourceClaim
      ? [perSourceClaim]
      : [];

  let labels = labelsFromClaims(claimList);

  // Prefer title / publisher cues before the coarse supports-code fallback so
  // sources that only carry supports:"ownership" can still differentiate
  // (e.g. leadership LinkedIn vs brand announcement vs ownership filing).
  if (!labels.length) {
    const fromTitle = labelsFromFieldBlob(
      [
        citation.title,
        citation.provider,
        citation.publisher,
        citation.source_type,
        citation.category,
        citation.excerpt,
        citation.evidence_excerpt,
      ]
        .filter(Boolean)
        .join(" ")
    );
    if (fromTitle.length) labels = fromTitle;
  }

  if (!labels.length && supportsCode && SUPPORTS_CODE_LABELS[supportsCode]) {
    labels = [SUPPORTS_CODE_LABELS[supportsCode]];
  }

  if (!labels.length) {
    labels = ["Property Research Evidence"];
  }

  let out = labels.join("; ");
  if (containsForbiddenSupportsTerminology(out)) {
    out = SUPPORTS_CODE_LABELS[supportsCode] || "Property Research Evidence";
  }
  // Hard scrub residual IDs if any slipped through.
  out = out.replace(/\brec[a-zA-Z0-9]{10,}\b/gi, "").replace(/\s{2,}/g, " ").trim();
  if (!out || containsForbiddenSupportsTerminology(out)) {
    return "Property Research Evidence";
  }
  return out;
}

/**
 * Whether a citation is Guest Sentiment–only (should hide when that tab is disabled).
 */
export function isGuestSentimentOnlyCitation(citation = {}) {
  const supports = String(citation.supports || "").toLowerCase();
  const category = String(citation.category || "").toLowerCase();
  const sourceType = String(citation.source_type || "").toLowerCase();
  if (supports === "guest_sentiment" || category === "sentiment") return true;
  if (/guest reputation|guest sentiment/.test(sourceType)) return true;
  return false;
}

export default {
  HE_CUSTOMER_SUPPORTS_LABEL_V1,
  FORBIDDEN_SUPPORTS_PATTERNS,
  containsForbiddenSupportsTerminology,
  customerSupportsLabel,
  isGuestSentimentOnlyCitation,
};
