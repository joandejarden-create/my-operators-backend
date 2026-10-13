/**
 * Mexico company-identity discovery (P1.7B).
 * Hotel website / footer / privacy / terms first.
 * RFC + razón social are entity-resolution seeds — not PropCo OWNED_BY.
 * Does not use CoStar True Owner or GTM licensed fields.
 */

import { websiteHost } from "../../../independent-census/match-current-census.js";
import { createOwnershipEvidenceCandidate } from "./evidence-provider.js";
import { FIELD_SEMANTICS } from "./source-semantics.js";
import { companySeedFromRegistryRow, isWeakBusinessDirectoryHost } from "../company-seed.js";
import { createMethodAttempt } from "../discovery-methods.js";

export const MEXICO_COMPANY_IDENTITY_VERSION = "ownership-mexico-company-identity-v1";

/** Persona moral 12 / persona física 13. */
export const RFC_RE = /\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b/g;

const RAZON_RE =
  /raz[oó]n\s+social\s*[:\-–]\s*([A-ZÁÉÍÓÚÑÜ0-9][^<\n|]{8,90})/gi;
const SA_DE_CV_RE =
  /\b([A-ZÁÉÍÓÚÑÜ][A-ZÁÉÍÓÚÑÜa-záéíóúñü0-9&.\s]{4,70}S\.?\s*A\.?\s*(?:P\.?\s*I\.?\s*)?de\s*C\.?\s*V\.?)\b/g;

/**
 * @param {string} text
 * @returns {string[]}
 */
export function extractRfcCandidatesFromText(text) {
  const found = new Set();
  const body = String(text || "").toUpperCase();
  RFC_RE.lastIndex = 0;
  let m;
  while ((m = RFC_RE.exec(body)) !== null) {
    const v = m[1].replace(/\s/g, "");
    if (v.length === 12 || v.length === 13) found.add(v);
  }
  return [...found];
}

/**
 * @param {string} text
 */
export function extractRazonSocialCandidatesFromText(text) {
  const found = new Set();
  const body = String(text || "");
  RAZON_RE.lastIndex = 0;
  let m;
  while ((m = RAZON_RE.exec(body)) !== null) {
    const name = String(m[1] || "")
      .replace(/<[^>]+>/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 90);
    if (name.length >= 8) found.add(name);
  }
  SA_DE_CV_RE.lastIndex = 0;
  while ((m = SA_DE_CV_RE.exec(body)) !== null) {
    const name = String(m[1] || "").replace(/\s+/g, " ").trim().slice(0, 90);
    if (name.length >= 8) found.add(name);
  }
  return [...found];
}

function hotelWebsiteUrls(hotel) {
  const website = String(hotel?.website || hotel?.digital?.website || "").trim();
  if (!website || isWeakBusinessDirectoryHost(website)) return [];
  let base = website;
  if (!/^https?:\/\//i.test(base)) base = `https://${base}`;
  try {
    const u = new URL(base);
    const origin = `${u.protocol}//${u.host}`;
    return [
      origin + (u.pathname || "/"),
      `${origin}/aviso-de-privacidad`,
      `${origin}/privacidad`,
      `${origin}/privacy`,
      `${origin}/terminos`,
      `${origin}/terms`,
      `${origin}/legal`,
      `${origin}/contacto`,
      `${origin}/facturacion`,
      `${origin}/factura`,
    ];
  } catch {
    return [];
  }
}

/**
 * Fetch official hotel pages for RFC / razón social.
 * @param {object} hotel
 * @param {{ timeoutMs?: number, fetchImpl?: typeof fetch }} [opts]
 */
export async function discoverMexicoCompanyFromHotelWebsite(hotel, opts = {}) {
  const started = Date.now();
  const notes = [];
  const fetchImpl = opts.fetchImpl || fetch;
  const timeoutMs = Number(opts.timeoutMs || 12000);
  const urls = hotelWebsiteUrls(hotel);
  if (!urls.length) {
    return {
      ok: false,
      rfc: null,
      razon_social: null,
      url: null,
      notes: ["mexico_no_official_website"],
      latency_ms: Date.now() - started,
    };
  }

  for (const url of urls.slice(0, 8)) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      const res = await fetchImpl(url, {
        signal: controller.signal,
        headers: {
          Accept: "text/html",
          "User-Agent": "DealalityOwnershipResearch/1.7",
        },
        redirect: "follow",
      });
      clearTimeout(timer);
      if (!res.ok) continue;
      const html = await res.text();
      const rfcs = extractRfcCandidatesFromText(html);
      const razons = extractRazonSocialCandidatesFromText(html);
      if (rfcs.length || razons.length) {
        notes.push(`mexico_website_identity:${url}`);
        return {
          ok: true,
          rfc: rfcs[0] || null,
          rfc_candidates: rfcs.slice(0, 3),
          razon_social: razons[0] || null,
          razon_candidates: razons.slice(0, 3),
          url,
          notes,
          latency_ms: Date.now() - started,
        };
      }
    } catch {
      notes.push(`mexico_website_fetch_failed:${websiteHost(url)}`);
    }
  }

  return {
    ok: false,
    rfc: null,
    razon_social: null,
    url: null,
    notes: notes.length ? notes : ["mexico_website_rfc_not_found"],
    latency_ms: Date.now() - started,
  };
}

/**
 * @param {object} hotel
 * @param {{ env?: object, fetchImpl?: typeof fetch }} [ctx]
 */
export async function lookupMexicoCompanyIdentity(hotel, ctx = {}) {
  const started = Date.now();
  const notes = [];
  const candidates = [];
  const disc = await discoverMexicoCompanyFromHotelWebsite(hotel, {
    fetchImpl: ctx.fetchImpl,
  });
  notes.push(...(disc.notes || []));

  const legal = disc.razon_social;
  const rfc = disc.rfc;
  if (legal || rfc) {
    const semRfc = FIELD_SEMANTICS["hotel_website.rfc"];
    const semRazon = FIELD_SEMANTICS["hotel_website.razon_social"];
    candidates.push(
      createOwnershipEvidenceCandidate({
        hotel_id: hotel?.hotel_id,
        source_provider: "mexico_hotel_website",
        source_country: "Mexico",
        source_url: disc.url,
        entity_candidate: {
          legal_name: legal || rfc,
          display_name: legal || rfc,
          jurisdiction: "MX",
          identifiers: rfc ? [{ kind: "rfc", value: rfc, country: "Mexico" }] : [],
        },
        supported_relationship: null,
        source_semantics: (semRazon || semRfc)?.notes,
        extracted_claim: `Hotel website company identity${legal ? `: ${legal}` : ""}${rfc ? ` RFC ${rfc}` : ""}`,
        source_authority: 0.72,
        max_verification: "needs_review",
        adapter_notes: ["entity_resolution_only", "not_automatic_owned_by", "corporate_web_first"],
      })
    );
  }

  const seed =
    legal || rfc
      ? companySeedFromRegistryRow(
          hotel,
          {
            legal_name: legal,
            rfc,
            website: hotel?.website,
            city: hotel?.city,
            source_url: disc.url,
          },
          legal && rfc
            ? { status: "PROBABLE", composite_score: 0.72, reasons: ["website_rfc_and_razon"] }
            : { status: "AMBIGUOUS", composite_score: 0.5, reasons: ["partial_website_identity"] },
          {
            source: "mexico_hotel_website",
            discovery_method: "hotel_name_to_company",
            jurisdiction: "MX",
            identifier: rfc ? { kind: "rfc", value: rfc, country: "Mexico" } : null,
            source_semantics:
              "Hotel legal/footer/privacy identity — RFC/razón social for entity resolution, not PropCo",
          }
        )
      : null;

  return {
    ok: true,
    candidates,
    seed,
    notes,
    metrics: {
      provider: "mexico_company_identity",
      website_ok: disc.ok,
      rfc_found: Boolean(rfc),
      razon_found: Boolean(legal),
      latency_ms: Date.now() - started,
    },
    method_attempt: createMethodAttempt({
      method: "hotel_name_to_company",
      success: Boolean(legal || rfc),
      candidate_count: candidates.length,
      country: "Mexico",
      pages_fetched: disc.ok ? 1 : 0,
      latency_ms: Date.now() - started,
      notes,
    }),
  };
}
