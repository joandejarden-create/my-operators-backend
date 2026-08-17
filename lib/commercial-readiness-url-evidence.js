import * as cheerio from "cheerio";
import dns from "dns/promises";
import net from "net";

const DEFAULT_TIMEOUT_MS = Number(process.env.COMMERCIAL_READINESS_URL_TIMEOUT_MS || 8000);
const DEFAULT_MAX_BYTES = Number(process.env.COMMERCIAL_READINESS_URL_MAX_BYTES || 350000);

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function splitSentences(text) {
  return nz(text)
    .replace(/\s+/g, " ")
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function pickSignals(sentences, patterns, max = 8) {
  const out = [];
  for (const s of sentences) {
    if (patterns.some((p) => p.test(s))) {
      out.push(s);
      if (out.length >= max) break;
    }
  }
  return out;
}

function isPrivateIp(ip) {
  if (!ip) return true;
  if (net.isIPv4(ip)) {
    const [a, b] = ip.split(".").map((x) => Number(x));
    if (a === 10) return true;
    if (a === 127) return true;
    if (a === 169 && b === 254) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 192 && b === 168) return true;
    return false;
  }
  if (net.isIPv6(ip)) {
    const v = ip.toLowerCase();
    if (v === "::1") return true;
    if (v.startsWith("fc") || v.startsWith("fd")) return true; // unique-local
    if (v.startsWith("fe80")) return true; // link-local
    return false;
  }
  return true;
}

async function validatePublicHttpUrl(rawUrl, { skipDns = false } = {}) {
  const value = nz(rawUrl);
  if (!value) return { ok: false, reason: "not_provided", url: "" };
  let parsed;
  try {
    parsed = new URL(value);
  } catch (_err) {
    return { ok: false, reason: "invalid_url", url: value };
  }
  if (!["http:", "https:"].includes(parsed.protocol)) {
    return { ok: false, reason: "invalid_protocol", url: value };
  }
  const host = nz(parsed.hostname).toLowerCase();
  if (!host) return { ok: false, reason: "invalid_url", url: value };
  if (host === "localhost" || host.endsWith(".local")) {
    return { ok: false, reason: "blocked_localhost", url: value };
  }
  if (net.isIP(host) && isPrivateIp(host)) {
    return { ok: false, reason: "blocked_private_ip", url: value };
  }
  if (!skipDns) {
    try {
      const records = await dns.lookup(host, { all: true });
      if (!records || !records.length) return { ok: false, reason: "dns_lookup_failed", url: value };
      if (records.some((r) => isPrivateIp(r.address))) {
        return { ok: false, reason: "blocked_private_ip", url: value };
      }
    } catch (_err) {
      return { ok: false, reason: "dns_lookup_failed", url: value };
    }
  }
  return { ok: true, url: parsed.toString() };
}

async function fetchHtmlSafe(url, { fetchImpl = fetch, timeoutMs = DEFAULT_TIMEOUT_MS, maxBytes = DEFAULT_MAX_BYTES } = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetchImpl(url, {
      headers: {
        "User-Agent": "DealalityCommercialReadiness/1.0 (+https://dealality.com)",
        Accept: "text/html,application/xhtml+xml",
      },
      redirect: "follow",
      signal: controller.signal,
    });

    if ([401, 403, 429].includes(res.status)) {
      return { ok: false, reason: "blocked", statusCode: res.status };
    }
    if (!res.ok) {
      return { ok: false, reason: "failed", statusCode: res.status };
    }
    const ctype = nz(res.headers.get("content-type")).toLowerCase();
    if (!ctype.includes("text/html")) {
      return { ok: false, reason: "non_html", statusCode: res.status };
    }
    const text = await res.text();
    if (text.length > maxBytes) {
      return { ok: false, reason: "response_too_large", statusCode: res.status };
    }
    return { ok: true, html: text, statusCode: res.status };
  } catch (err) {
    if (err?.name === "AbortError") return { ok: false, reason: "timeout" };
    return { ok: false, reason: "failed" };
  } finally {
    clearTimeout(timeout);
  }
}

function parseBaseHtml(html) {
  const $ = cheerio.load(html);
  $("script,style,noscript,svg").remove();
  const title = nz($("title").first().text());
  const metaDescription = nz($('meta[name="description"]').attr("content"));
  const headings = $("h1,h2,h3")
    .toArray()
    .map((el) => nz($(el).text()))
    .filter(Boolean)
    .slice(0, 20);
  const bodyText = nz($("body").text()).replace(/\s+/g, " ").trim();
  const sentences = splitSentences([title, metaDescription, bodyText].filter(Boolean).join(". "));
  return { title, metaDescription, headings, bodyText, sentences };
}

function buildHotelEvidence(url, parsed) {
  const bookingSignals = pickSignals(parsed.sentences, [/book/i, /reservation/i, /reserve/i, /availability/i]);
  const directBookingSignals = pickSignals(parsed.sentences, [/book direct/i, /best rate/i, /exclusive/i, /official site/i, /member rate/i]);
  const roomSignals = pickSignals(parsed.sentences, [/room/i, /suite/i, /accommodation/i, /amenit/i, /bed/i]);
  const locationSignals = pickSignals(parsed.sentences, [/location/i, /near/i, /minutes from/i, /beach/i, /airport/i, /district/i]);
  const trustSignals = pickSignals(parsed.sentences, [/free cancellation/i, /secure/i, /guarantee/i, /reviews?/i, /trusted/i, /policy/i]);
  const guestSegmentSignals = pickSignals(parsed.sentences, [/business/i, /family/i, /couples?/i, /leisure/i, /group/i]);
  const crmSignals = pickSignals(parsed.sentences, [/newsletter/i, /email/i, /subscribe/i, /whatsapp/i, /chat/i, /loyalty/i]);

  return {
    url,
    status: "extracted",
    title: parsed.title,
    metaDescription: parsed.metaDescription,
    headings: parsed.headings.slice(0, 12),
    bookingSignals,
    directBookingSignals,
    roomSignals,
    locationSignals,
    trustSignals,
    guestSegmentSignals,
    crmSignals,
    rawEvidenceSummary: parsed.sentences.slice(0, 8).join(" "),
  };
}

function buildOtaEvidence(url, parsed) {
  const listingSignals = pickSignals(parsed.sentences, [/hotel/i, /property/i, /stay/i, /accommodation/i, /resort/i]);
  const roomSignals = pickSignals(parsed.sentences, [/room/i, /suite/i, /bed/i, /amenit/i]);
  const locationSignals = pickSignals(parsed.sentences, [/location/i, /near/i, /district/i, /airport/i, /beach/i]);
  const reviewSignals = pickSignals(parsed.sentences, [/review/i, /rating/i, /guest/i, /score/i]);
  const trustSignals = pickSignals(parsed.sentences, [/free cancellation/i, /pay/i, /refund/i, /policy/i, /secure/i]);

  return {
    url,
    status: "extracted",
    title: parsed.title,
    listingSignals,
    roomSignals,
    locationSignals,
    reviewSignals,
    trustSignals,
    rawEvidenceSummary: parsed.sentences.slice(0, 8).join(" "),
  };
}

function failedSource(url, reason, kind = "general") {
  const base = { url: nz(url), status: reason === "not_provided" ? "not_provided" : reason === "blocked" ? "blocked" : "failed" };
  if (kind === "google") {
    return { ...base, notes: reason };
  }
  return {
    ...base,
    title: "",
    metaDescription: "",
    headings: [],
    bookingSignals: [],
    directBookingSignals: [],
    roomSignals: [],
    locationSignals: [],
    trustSignals: [],
    guestSegmentSignals: [],
    crmSignals: [],
    listingSignals: [],
    reviewSignals: [],
    rawEvidenceSummary: "",
    reason,
  };
}

function buildOwnedVsOtaComparison(sources) {
  const hotel = sources.hotelWebsite;
  const booking = sources.bookingCom;
  const expedia = sources.expedia;
  const otaExtracted = [booking, expedia].filter((s) => s.status === "extracted");
  const hotelExtracted = hotel.status === "extracted";

  if (!hotelExtracted && !otaExtracted.length) {
    return {
      assessment: "Insufficient extracted evidence",
      confidence: "Low",
      ownedChannelStrengths: [],
      otaStrengths: [],
      contentGaps: [],
      directBookingGaps: [],
      guestReassuranceGaps: [],
      dealImplication:
        "URL-level analysis was attempted, but content could not be extracted from provided sources. Use owner-provided inputs and manual comparison.",
    };
  }

  if (hotelExtracted && !otaExtracted.length) {
    return {
      assessment: "Mixed / needs manual review",
      confidence: "Low",
      ownedChannelStrengths: hotel.directBookingSignals.slice(0, 3).concat(hotel.trustSignals.slice(0, 2)),
      otaStrengths: [],
      contentGaps: ["Booking.com / Expedia extraction unavailable; OTA-side comparison is incomplete."],
      directBookingGaps: hotel.directBookingSignals.length ? [] : ["No strong direct-booking value signals found in extracted hotel text."],
      guestReassuranceGaps: hotel.trustSignals.length ? [] : ["Limited visible reassurance/policy signals in extracted hotel text."],
      dealImplication:
        "Hotel website content was extracted, but OTA comparison is incomplete due to blocked/unavailable OTA extraction.",
    };
  }

  const otaTrust = otaExtracted.reduce((n, s) => n + (s.reviewSignals?.length || 0) + (s.trustSignals?.length || 0), 0);
  const ownedDirect = (hotel.directBookingSignals?.length || 0) + (hotel.bookingSignals?.length || 0);
  const ownedTrust = hotel.trustSignals?.length || 0;
  const otaListing = otaExtracted.reduce((n, s) => n + (s.listingSignals?.length || 0) + (s.roomSignals?.length || 0), 0);
  const ownedRoom = hotel.roomSignals?.length || 0;

  let assessment = "Mixed / needs manual review";
  if (ownedDirect + ownedTrust > otaTrust + otaListing) assessment = "Owned channel appears stronger";
  else if (otaTrust + otaListing > ownedDirect + ownedTrust) assessment = "OTA presentation appears stronger";

  const confidence = otaExtracted.length === 2 && hotelExtracted ? "Moderate" : "Low";

  return {
    assessment,
    confidence,
    ownedChannelStrengths: []
      .concat(hotel.directBookingSignals?.slice(0, 3) || [])
      .concat(hotel.trustSignals?.slice(0, 2) || []),
    otaStrengths: []
      .concat(otaExtracted.flatMap((s) => s.reviewSignals || []).slice(0, 3))
      .concat(otaExtracted.flatMap((s) => s.trustSignals || []).slice(0, 2)),
    contentGaps: ownedRoom < otaListing ? ["OTA listings appear to provide stronger structured room/listing detail in extracted text."] : [],
    directBookingGaps: ownedDirect ? [] : ["Limited extracted direct-booking value signals on owned site."],
    guestReassuranceGaps: ownedTrust >= otaTrust ? [] : ["OTA-side trust/review cues appear stronger than owned-site cues in extracted text."],
    dealImplication:
      "Based on extracted page evidence, compare whether direct channel gives guests a clear reason to book direct after OTA comparison shopping.",
  };
}

async function extractOne(url, kind, opts) {
  const validated = await validatePublicHttpUrl(url, { skipDns: opts.skipDns });
  if (!validated.ok) {
    if (validated.reason === "not_provided") return failedSource(url, "not_provided", kind);
    return failedSource(url, validated.reason, kind);
  }
  if (kind === "google") {
    return {
      url: validated.url,
      status: "failed",
      notes: "Google Business Profile URL provided, but content extraction was not available in this MVP.",
      reason: "extraction_unavailable",
    };
  }

  const fetched = await fetchHtmlSafe(validated.url, opts);
  if (!fetched.ok) {
    return failedSource(validated.url, fetched.reason === "blocked" ? "blocked" : fetched.reason || "failed", kind);
  }
  const parsed = parseBaseHtml(fetched.html);
  if (kind === "hotelWebsite") return buildHotelEvidence(validated.url, parsed);
  return buildOtaEvidence(validated.url, parsed);
}

/**
 * Extract URL evidence and owned-vs-OTA comparison from commercial readiness inputs.
 */
export async function extractCommercialReadinessUrlEvidence(inputs, opts = {}) {
  const sources = {
    hotelWebsite: await extractOne(inputs?.hotelWebsiteUrl, "hotelWebsite", opts),
    bookingCom: await extractOne(inputs?.bookingComUrl, "bookingCom", opts),
    expedia: await extractOne(inputs?.expediaUrl, "expedia", opts),
    googleBusinessProfile: await extractOne(inputs?.googleBusinessProfileUrl, "google", opts),
  };

  return {
    enabled: true,
    sources,
    ownedVsOtaComparison: buildOwnedVsOtaComparison(sources),
  };
}

