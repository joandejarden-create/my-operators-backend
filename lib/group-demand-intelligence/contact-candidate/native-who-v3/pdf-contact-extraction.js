/**
 * Official PDF / prospectus contact extraction for Native WHO V3.
 */

import { PDFParse } from "pdf-parse";
import { isSocialOrAggregator } from "./official-domain.js";
import { isStrictPersonName } from "./person-boundary.js";
import { classifySectionFromUrl, sectionIsReject } from "./section-semantics.js";

export const PDF_ROLE_CONTEXT = Object.freeze({
  ORGANIZER: "ORGANIZER",
  EVENT_STAFF: "EVENT_STAFF",
  HOUSING: "HOUSING",
  REGISTRATION: "REGISTRATION",
  SPONSORSHIP: "SPONSORSHIP",
  PROGRAM: "PROGRAM",
  SPEAKER: "SPEAKER",
  BOARD_MEMBER: "BOARD_MEMBER",
  VENDOR: "VENDOR",
  UNKNOWN: "UNKNOWN",
});

const CONTACT_CONTEXT_RE =
  /\b(contact|questions|conference|meetings|events|housing|registration|sponsorship|exhibitors?|program|director|manager|coordinator|chair)\b/i;

const SPEAKER_SECTION_RE =
  /\b(speakers?|keynote|panelist|presenter|faculty)\b/i;
const BOARD_SECTION_RE = /\b(board of directors|board members?|trustees)\b/i;
const HOUSING_SECTION_RE = /\b(housing|hotel block|room block|accommodations?)\b/i;
const REG_SECTION_RE = /\b(registration|registrar|attendee services)\b/i;
const SPONSOR_SECTION_RE = /\b(sponsor(?:ship)?|exhibitor|exhibit hall)\b/i;
const ORGANIZER_SECTION_RE =
  /\b(organizer|conference (?:director|manager|staff)|meeting(?:s)? (?:director|manager)|event(?:s)? (?:director|manager)|staff contact)\b/i;

const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
const PHONE_RE =
  /(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}(?:\s*(?:x|ext\.?)\s*\d+)?/g;
const NAME_NEAR_RE =
  /\b([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){1,3})\b/g;

/**
 * @param {Buffer} buf
 */
async function extractPdfText(buf) {
  const parser = new PDFParse({ data: buf });
  try {
    const parsed = await parser.getText();
    return String(parsed?.text || "");
  } finally {
    if (typeof parser.destroy === "function") await parser.destroy();
  }
}

/**
 * @param {string} url
 * @param {{ timeoutMs?: number, maxBytes?: number }} [opts]
 */
export async function fetchPdfText(url, opts = {}) {
  if (!url || !/^https?:/i.test(url) || isSocialOrAggregator(url)) {
    return { ok: false, url, reason: "invalid_or_blocked" };
  }
  const timeoutMs = opts.timeoutMs ?? 22000;
  const maxBytes = opts.maxBytes ?? 2_500_000;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      redirect: "follow",
      headers: {
        "User-Agent": "DealalityNativeWhoV3/1.0",
        Accept: "application/pdf,*/*",
      },
    });
    const buf = Buffer.from(await res.arrayBuffer());
    const body = buf.slice(0, maxBytes);
    const ctype = String(res.headers.get("content-type") || "");
    const looksPdf =
      /pdf/i.test(ctype) || /\.pdf($|\?)/i.test(url) || body.slice(0, 5).toString() === "%PDF-";
    if (!looksPdf) {
      return { ok: false, url: res.url || url, reason: "not_pdf", status: res.status };
    }
    let text = "";
    try {
      text = await extractPdfText(body);
    } catch (err) {
      text = body.toString("latin1").replace(/[^\x20-\x7E\n\r]/g, " ");
    }
    return {
      ok: res.ok && Boolean(text.trim()),
      url: res.url || url,
      status: res.status,
      text: text.replace(/\s+/g, " ").trim().slice(0, 100_000),
      byteLength: buf.length,
    };
  } catch (err) {
    return {
      ok: false,
      url,
      reason: err?.name === "AbortError" ? "timeout" : String(err?.message || err),
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Classify local PDF section context around a match index.
 * @param {string} text
 * @param {number} idx
 */
export function classifyPdfRoleContext(text, idx) {
  const start = Math.max(0, idx - 220);
  const end = Math.min(text.length, idx + 220);
  const window = text.slice(start, end);
  if (SPEAKER_SECTION_RE.test(window) && !ORGANIZER_SECTION_RE.test(window)) {
    return PDF_ROLE_CONTEXT.SPEAKER;
  }
  if (BOARD_SECTION_RE.test(window) && !ORGANIZER_SECTION_RE.test(window)) {
    return PDF_ROLE_CONTEXT.BOARD_MEMBER;
  }
  if (HOUSING_SECTION_RE.test(window)) return PDF_ROLE_CONTEXT.HOUSING;
  if (REG_SECTION_RE.test(window)) return PDF_ROLE_CONTEXT.REGISTRATION;
  if (SPONSOR_SECTION_RE.test(window)) return PDF_ROLE_CONTEXT.SPONSORSHIP;
  if (ORGANIZER_SECTION_RE.test(window)) return PDF_ROLE_CONTEXT.ORGANIZER;
  if (/\b(program committee|session chair)\b/i.test(window)) {
    return PDF_ROLE_CONTEXT.PROGRAM;
  }
  if (CONTACT_CONTEXT_RE.test(window)) return PDF_ROLE_CONTEXT.EVENT_STAFF;
  return PDF_ROLE_CONTEXT.UNKNOWN;
}

/**
 * Extract candidate contact blocks from PDF text.
 * @param {string} text
 * @param {string} sourceUrl
 */
export function extractContactsFromPdfText(text, sourceUrl) {
  const people = [];
  const functional = [];
  if (!text) return { people, functional };

  const emails = text.match(EMAIL_RE) || [];
  for (const email of [...new Set(emails)].slice(0, 20)) {
    const idx = text.toLowerCase().indexOf(email.toLowerCase());
    const window = text.slice(Math.max(0, idx - 180), idx + email.length + 80);
    const ctx = classifyPdfRoleContext(text, idx);
    const isFunctional = /^(info|events|contact|hello|office|admin|support|meetings|housing|registration|sponsors?)@/i.test(
      email
    );
    if (isFunctional) {
      functional.push({
        name: "Functional inbox",
        email,
        phone: null,
        role: ctx,
        sourceUrl,
        evidenceType: "PDF_FUNCTIONAL",
      });
      continue;
    }
    if (ctx === PDF_ROLE_CONTEXT.SPEAKER || ctx === PDF_ROLE_CONTEXT.BOARD_MEMBER) {
      continue;
    }
    let name = null;
    const before = window.slice(0, Math.max(0, window.toLowerCase().indexOf(email.toLowerCase())));
    const nameMatches = [...before.matchAll(NAME_NEAR_RE)].map((m) => m[1]);
    name = nameMatches.length ? nameMatches[nameMatches.length - 1] : null;
    const phoneMatch = window.match(PHONE_RE);
    people.push({
      name,
      role: inferTitleNear(window, name),
      email,
      phone: phoneMatch ? phoneMatch[0] : null,
      pdfRoleContext: ctx,
      sourceUrl,
      evidenceType: "PDF_CONTACT_BLOCK",
      evidenceQuote: window.replace(/\s+/g, " ").trim().slice(0, 220),
    });
  }

  // Phone-only contact blocks near organizer vocabulary
  const phoneHits = text.match(PHONE_RE) || [];
  for (const phone of [...new Set(phoneHits)].slice(0, 10)) {
    const idx = text.indexOf(phone);
    if (idx < 0) continue;
    const ctx = classifyPdfRoleContext(text, idx);
    if (
      ctx === PDF_ROLE_CONTEXT.SPEAKER ||
      ctx === PDF_ROLE_CONTEXT.BOARD_MEMBER ||
      ctx === PDF_ROLE_CONTEXT.UNKNOWN
    ) {
      continue;
    }
    const window = text.slice(Math.max(0, idx - 160), idx + phone.length + 40);
    if (!CONTACT_CONTEXT_RE.test(window)) continue;
    const names = [...window.matchAll(NAME_NEAR_RE)].map((m) => m[1]);
    if (!names.length) continue;
    const name = names[names.length - 1];
    if (people.some((p) => p.name === name && p.phone === phone)) continue;
    people.push({
      name,
      role: inferTitleNear(window, name),
      email: null,
      phone,
      pdfRoleContext: ctx,
      sourceUrl,
      evidenceType: "PDF_PHONE_BLOCK",
      evidenceQuote: window.replace(/\s+/g, " ").trim().slice(0, 220),
    });
  }

  return {
    people: people.filter((p) => p.name && isStrictPersonName(p.name)).slice(0, 8),
    functional: functional.slice(0, 6),
  };
}

function inferTitleNear(window, name) {
  if (!name) return null;
  const after = window.split(name)[1] || "";
  const m = after.match(
    /^\s*[,:\-–]?\s*((?:VP|Vice President|Director|Manager|Coordinator|Chair|Executive Director|President|CEO|COO)[^.\n]{0,60})/i
  );
  return m ? m[1].trim().slice(0, 80) : null;
}

/**
 * Discover PDF URLs from SERP organic results + page link lists.
 * @param {Array<{url?: string, title?: string}>} serpHits
 * @param {string[]} pagePdfLinks
 */
export function collectPdfCandidates(serpHits = [], pagePdfLinks = []) {
  const out = [];
  for (const h of serpHits) {
    const u = h?.url;
    if (!u) continue;
    if (/\.pdf($|\?)/i.test(u) || /filetype:pdf/i.test(String(h.title || ""))) {
      out.push(u);
    }
  }
  for (const u of pagePdfLinks) {
    if (u && /\.pdf($|\?)/i.test(u)) out.push(u);
  }
  return [...new Set(out)].filter((u) => !isSocialOrAggregator(u)).slice(0, 6);
}

/**
 * @param {string[]} urls
 */
export async function extractFromPdfUrls(urls) {
  const inspected = [];
  const people = [];
  const functional = [];
  for (const url of (urls || []).slice(0, 3)) {
    const sec = classifySectionFromUrl(url);
    if (sectionIsReject(sec) || /volunteer/i.test(url)) {
      inspected.push({ url, ok: false, reason: "section_rejected", section: sec });
      continue;
    }
    const doc = await fetchPdfText(url);
    inspected.push({
      url,
      ok: doc.ok,
      reason: doc.reason || null,
      chars: doc.text?.length || 0,
    });
    if (!doc.ok || !doc.text) continue;
    // Prefer pages/chunks containing contact vocabulary
    if (!CONTACT_CONTEXT_RE.test(doc.text) && !EMAIL_RE.test(doc.text)) continue;
    const extracted = extractContactsFromPdfText(doc.text, doc.url || url);
    people.push(...extracted.people);
    functional.push(...extracted.functional);
  }
  return { inspected, people, functional };
}
