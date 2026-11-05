/**
 * Evidence-grounded email and telephone extraction.
 *
 * A digit run becomes a telephone only when the source quote labels it as a
 * phone and the number matches a known country plan. Functional organization
 * inboxes do not need a named person. Venue calendars and generic homepages
 * stay research pages, not hotel-sales routes.
 */

export const EVIDENCE_CONTACT_EXTRACTION_VERSION = "gdi-evidence-contact-extraction-v1";

const COUNTRY_RULES = [
  { callingCode: "1", iso: "US", national: [10, 10], nanp: true },
  { callingCode: "52", iso: "MX", national: [10, 10] },
  { callingCode: "41", iso: "CH", national: [9, 9] },
  { callingCode: "49", iso: "DE", national: [10, 11] },
  { callingCode: "44", iso: "GB", national: [10, 10] },
  { callingCode: "34", iso: "ES", national: [9, 9] },
  { callingCode: "33", iso: "FR", national: [9, 9] },
].sort((a, b) => b.callingCode.length - a.callingCode.length);

const TRUNK_BY_ISO = {
  CH: { callingCode: "41", trunkDigits: 10, national: 9 },
  FR: { callingCode: "33", trunkDigits: 10, national: 9 },
  GB: { callingCode: "44", trunkDigits: 11, national: 10 },
  DE: { callingCode: "49", trunkDigits: [11, 12], national: [10, 11] },
};

const PHONE_LABEL_RE =
  /(?:^|[\s|(])((?:t[eé]l[eé]?(?:phone|éphone)?|phone|mobile|cell(?:ular)?|switchboard|direct\s+line))\s*[:.]?\s*(\+|00)?/gi;

const EMAIL_RE = /\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b/g;
const URL_RE = /https?:\/\/[^\s)>\]]+/gi;
const DIGIT_RUN_RE = /(?<!\d)\d{10,16}(?!\d)/g;

const REJECT_EMAIL_DOMAIN_RE =
  /(?:^|\.)(?:example\.(?:com|org|net)|sentry\.io|wixpress\.com|schema\.org|cloudflare\.com)$/i;

const HOMEPAGE_RE =
  /^https?:\/\/[^/?#]+\/?(?:en\/|es\/|de\/|fr\/)?(?:home\/?)?(?:index\.(?:html?|php))?$/i;

const CALENDAR_OR_LISTING_RE =
  /\/(?:whats-on|whats_on|things-to-do|fairs?)\b|[-/]calendar\b/i;

function clean(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function digitsOnly(value) {
  return String(value ?? "").replace(/\D/g, "");
}

export function classifyEvidenceSourcePage(url) {
  const value = clean(url);
  if (!value) return "unknown";
  if (HOMEPAGE_RE.test(value)) return "homepage";
  if (CALENDAR_OR_LISTING_RE.test(value)) return "calendar";
  return "document";
}

function nanpNational(national) {
  if (!/^[2-9]\d{2}[2-9]\d{6}$/.test(national)) return false;
  if (/^(\d)\1{9}$/.test(national)) return false;
  const exchange = national.slice(3, 6);
  if (/^[2-9]11$/.test(exchange)) return false;
  return true;
}

function nationalInRange(length, range) {
  return length >= range[0] && length <= range[1];
}

function ruleForCallingCode(digits) {
  return COUNTRY_RULES.find((rule) => digits.startsWith(rule.callingCode)) || null;
}

function acceptInternational(digits) {
  const rule = ruleForCallingCode(digits);
  if (!rule) return null;
  const national = digits.slice(rule.callingCode.length);
  if (!nationalInRange(national.length, rule.national)) return null;
  if (rule.nanp && !nanpNational(national)) return null;
  if (/^(\d)\1{6,}$/.test(national)) return null;
  return {
    normalized: `+${rule.callingCode}${national}`,
    country: rule.iso,
    national,
  };
}

function timestampLike(digits) {
  if (digits.length !== 13) return false;
  const value = Number(digits);
  return value >= 1_500_000_000_000 && value <= 2_000_000_000_000;
}

/**
 * Validate a telephone already stored on a contact field, or a labeled quote span.
 * Unlabeled 10–15 digit strings are not accepted unless they are a valid NANP
 * number or an explicit international number. Tracking ids and millisecond
 * timestamps fail that test.
 */
export function parseCrediblePhone(raw, { countryHint = "" } = {}) {
  const original = clean(raw);
  if (!original || /^not\s*established$/i.test(original)) return null;
  if (/^https?:\/\//i.test(original) || /@/.test(original)) return null;
  if (/[a-z]/i.test(original.replace(/\bext(?:ension)?\.?\s*\d+$/i, ""))) return null;

  const withoutTrunkMarker = original.replace(/\(0\)/g, " ");
  let compact = withoutTrunkMarker.replace(/[^\d+]/g, "");
  let international = false;
  if (compact.startsWith("+")) {
    international = true;
    compact = compact.slice(1);
  } else if (compact.startsWith("00")) {
    international = true;
    compact = compact.slice(2);
  }
  if (!compact || compact.length > 15) return null;
  if (timestampLike(compact)) return null;
  if (/^(?:0123456789|1234567890|9876543210)/.test(compact)) return null;

  if (international) return acceptInternational(compact);

  if (compact.length === 10 && nanpNational(compact)) {
    return { normalized: `+1${compact}`, country: "US", national: compact };
  }
  if (compact.length === 11 && compact.startsWith("1") && nanpNational(compact.slice(1))) {
    return { normalized: `+${compact}`, country: "US", national: compact.slice(1) };
  }

  const hint = TRUNK_BY_ISO[String(countryHint || "").toUpperCase()];
  if (hint && compact.startsWith("0")) {
    const national = compact.slice(1);
    const trunkLength = Array.isArray(hint.trunkDigits)
      ? nationalInRange(compact.length, hint.trunkDigits)
      : compact.length === hint.trunkDigits;
    const nationalLength = Array.isArray(hint.national)
      ? nationalInRange(national.length, hint.national)
      : national.length === hint.national;
    if (trunkLength && nationalLength) {
      return acceptInternational(`${hint.callingCode}${national}`);
    }
  }

  if (compact.length === 11 && compact.startsWith("0")) {
    const national = compact.slice(1);
    if (national.length === 10 && /^[1-9]/.test(national)) {
      return acceptInternational(`44${national}`);
    }
  }
  return null;
}

function urlSpans(quote) {
  const spans = [];
  for (const match of String(quote || "").matchAll(URL_RE)) {
    spans.push([match.index, match.index + match[0].length]);
  }
  return spans;
}

function insideSpan(index, spans) {
  return spans.some(([start, end]) => index >= start && index < end);
}

function pageAllowsSalesRoute(sourceUrl) {
  const page = classifyEvidenceSourcePage(sourceUrl);
  return page !== "homepage" && page !== "calendar";
}

function rejection(token, reason, sourceUrl) {
  return {
    token: clean(token).slice(0, 80),
    reason,
    sourceUrl: sourceUrl || "",
    usableForSalesRoute: false,
  };
}

function findLabeledPhone(quote, sourceUrl, countryHint) {
  const rejected = [];
  const spans = urlSpans(quote);
  let found = null;
  for (const match of String(quote || "").matchAll(PHONE_LABEL_RE)) {
    const start = match.index + match[0].length;
    const rest = String(quote).slice(start, start + 48);
    const numberMatch = rest.match(
      /^(\+?\d{1,4}(?:\s*\(0\))?(?:[\s.-]*\d{2,4}){2,6})/
    );
    if (!numberMatch) continue;
    const raw = `${match[2] || ""}${numberMatch[1]}`.trim();
    if (insideSpan(match.index, spans) || insideSpan(start, spans)) {
      rejected.push(rejection(raw, "inside_url", sourceUrl));
      continue;
    }
    const parsed = parseCrediblePhone(raw, { countryHint });
    if (!parsed) {
      rejected.push(rejection(raw, "not_a_telephone", sourceUrl));
      continue;
    }
    if (!String(quote).includes(numberMatch[1])) {
      rejected.push(rejection(raw, "quote_span_missing", sourceUrl));
      continue;
    }
    const usable = pageAllowsSalesRoute(sourceUrl);
    found = {
      kind: "phone",
      value: clean(raw),
      normalized: parsed.normalized,
      country: parsed.country,
      quoteSpan: numberMatch[1],
      sourceUrl: sourceUrl || "",
      confidence: "high",
      usableForSalesRoute: usable,
      namedPersonRequired: false,
      reason: usable ? "labeled_telephone" : "source_page_not_a_sales_route",
    };
    if (!usable) rejected.push(rejection(raw, "source_page_not_a_sales_route", sourceUrl));
    break;
  }
  return { phone: found, rejected };
}

function findEmails(quote, sourceUrl) {
  const rejected = [];
  const spans = urlSpans(quote);
  let found = null;
  for (const match of String(quote || "").matchAll(EMAIL_RE)) {
    const value = match[0].toLowerCase();
    if (insideSpan(match.index, spans)) {
      rejected.push(rejection(value, "inside_url", sourceUrl));
      continue;
    }
    const domain = value.split("@")[1] || "";
    if (REJECT_EMAIL_DOMAIN_RE.test(domain)) {
      rejected.push(rejection(value, "unusable_email_domain", sourceUrl));
      continue;
    }
    if (!String(quote).toLowerCase().includes(value)) {
      rejected.push(rejection(value, "quote_span_missing", sourceUrl));
      continue;
    }
    const usable = pageAllowsSalesRoute(sourceUrl);
    found = {
      kind: "email",
      value,
      normalized: value,
      quoteSpan: match[0],
      sourceUrl: sourceUrl || "",
      confidence: "high",
      usableForSalesRoute: usable,
      namedPersonRequired: false,
      reason: usable ? "published_email" : "source_page_not_a_sales_route",
    };
    if (!usable) rejected.push(rejection(value, "source_page_not_a_sales_route", sourceUrl));
    break;
  }
  return { email: found, rejected };
}

function rejectUnlabeledRuns(quote, sourceUrl, acceptedDigits) {
  const rejected = [];
  const spans = urlSpans(quote);
  for (const match of String(quote || "").matchAll(DIGIT_RUN_RE)) {
    const token = match[0];
    if (acceptedDigits.some((digits) => digits.includes(token) || token.includes(digits))) {
      continue;
    }
    let reason = "unlabeled_digit_sequence";
    if (insideSpan(match.index, spans)) reason = "inside_url";
    else if (timestampLike(token)) reason = "timestamp_or_tracking_id";
    else if (!parseCrediblePhone(token)) reason = "not_a_telephone";
    else reason = "unlabeled_digit_sequence";
    rejected.push(rejection(token, reason, sourceUrl));
  }
  return rejected;
}

export function extractEvidenceContacts({
  quote = "",
  sourceUrl = "",
  countryHint = "",
} = {}) {
  const text = String(quote || "");
  const labeled = findLabeledPhone(text, sourceUrl, countryHint);
  const mailed = findEmails(text, sourceUrl);
  const acceptedDigits = [labeled.phone?.normalized, labeled.phone?.value]
    .filter(Boolean)
    .map((value) => digitsOnly(value));
  const rejected = [
    ...labeled.rejected,
    ...mailed.rejected,
    ...rejectUnlabeledRuns(text, sourceUrl, acceptedDigits),
  ];
  return {
    version: EVIDENCE_CONTACT_EXTRACTION_VERSION,
    sourcePage: classifyEvidenceSourcePage(sourceUrl),
    email: mailed.email?.usableForSalesRoute ? mailed.email : null,
    phone: labeled.phone?.usableForSalesRoute ? labeled.phone : null,
    observed: {
      email: mailed.email,
      phone: labeled.phone,
    },
    rejected,
  };
}
