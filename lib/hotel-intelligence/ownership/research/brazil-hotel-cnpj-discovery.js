/**
 * Hotel name + city → CNPJ discovery fallback (P1.6 / A′-BR-05).
 */

import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { normalizeCnpjDigits, formatCnpj, fetchCnpjRegistry } from "./brazil-cnpj-client.js";
import { validateCnpjSeed } from "./brazil-cnpj-seed-validation.js";
import { createMethodAttempt } from "../discovery-methods.js";
import { findCnpjFixtureByHotelIdentity } from "./brazil-cnpj-fixtures.js";
import { isWeakBusinessDirectoryHost } from "../company-seed.js";

export const HOTEL_CNPJ_DISCOVERY_VERSION = "ownership-hotel-cnpj-discovery-v1";

const CNPJ_RE =
  /\b(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2}|\d{14})\b/g;

/**
 * @param {object} hotel
 * @param {{ maxQueries?: number }} [opts]
 */
export function buildHotelNameCnpjDiscoveryQueries(hotel, opts = {}) {
  const name = String(hotel?.hotel_name || hotel?.name || "").trim();
  const city = String(hotel?.city || "").trim();
  if (!name) return [];
  const loc = city ? `"${name}" ${city}` : `"${name}"`;
  const pool = [
    `${loc} CNPJ`,
    `${loc} razão social CNPJ`,
    `${loc} nome fantasia CNPJ site:econodata.com.br OR site:cnpja.com OR site:casadosdados.com.br`,
    `${loc} CNPJ site:terrazza.com.br OR site:booking.com`,
  ];
  const max = Math.min(4, Number(opts.maxQueries || 3) || 3);
  return pool.slice(0, max);
}

/**
 * @param {string} text
 * @returns {string[]}
 */
export function extractCnpjCandidatesFromText(text) {
  const found = new Set();
  const body = String(text || "");
  let m;
  CNPJ_RE.lastIndex = 0;
  while ((m = CNPJ_RE.exec(body)) !== null) {
    const d = normalizeCnpjDigits(m[1]);
    if (d) found.add(d);
  }
  return [...found];
}

/**
 * Try hotel website / about page for displayed CNPJ (A′-BR-05 website path).
 * @param {object} hotel
 * @param {{ timeoutMs?: number }} [opts]
 */
export async function discoverCnpjFromHotelWebsite(hotel, opts = {}) {
  const notes = [];
  const urls = [];
  const website = String(hotel?.website || hotel?.digital?.website || "").trim();
  if (website) urls.push(website);
  const timeoutMs = Number(opts.timeoutMs || 5000);
  for (const base of [...new Set(urls)].slice(0, 1)) {
    const paths = [
      "",
      "/sobre",
      "/about",
      "/contato",
      "/privacidade",
      "/legal",
    ];
    for (const suffix of paths) {
      const url = `${base.replace(/\/$/, "")}${suffix}`;
      try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        const res = await fetch(url, {
          signal: controller.signal,
          headers: { Accept: "text/html", "User-Agent": "DealalityOwnershipResearch/1.6" },
        });
        clearTimeout(timer);
        if (!res.ok) continue;
        const html = await res.text();
        const hits = extractCnpjCandidatesFromText(html);
        if (hits.length) {
          notes.push(`website_cnpj:${url}:${hits[0]}`);
          return { ok: true, cnpj: hits[0], url, notes };
        }
      } catch {
        continue;
      }
    }
  }
  return { ok: false, cnpj: null, notes: notes.length ? notes : ["website_cnpj_not_found"] };
}

/**
 * @param {object} ctx
 */
export async function discoverHotelCnpjSeed(ctx) {
  const { hotel, hotelId, env } = ctx;
  const started = Date.now();
  const notes = [];
  const metrics = {
    searches: 0,
    cnpj_candidates: 0,
    validated: 0,
    rejected: 0,
  };

  /** @type {Set<string>} */
  const cnpjHits = new Set();

  const webDisc = await discoverCnpjFromHotelWebsite(hotel);
  notes.push(...(webDisc.notes || []));
  if (webDisc.ok && webDisc.cnpj) cnpjHits.add(webDisc.cnpj);

  const allowSerpapi =
    String(env?.OWNERSHIP_RESEARCH_USE_SERPAPI || "1").trim() !== "0" &&
    Boolean(String(env?.SERPAPI_KEY || env?.SERPAPI_API_KEY || "").trim());

  if (!allowSerpapi && !cnpjHits.size) {
    notes.push("cnpj_discovery_skipped:serpapi_disabled");
    return {
      ok: false,
      seeds: [],
      metrics,
      notes,
      method_attempt: createMethodAttempt({
        method: "hotel_name_to_company",
        success: false,
        notes: ["serpapi_disabled_no_website_cnpj"],
      }),
    };
  }

  const queries = buildHotelNameCnpjDiscoveryQueries(hotel, { maxQueries: 3 });

  for (const q of queries) {
    if (!allowSerpapi) break;
    metrics.searches += 1;
    let serp;
    try {
      serp = await serpapiSearch(
        { engine: "google", q, num: 8, hl: "pt", gl: "br" },
        { timeoutMs: 45000, env }
      );
    } catch (err) {
      notes.push(`serp_error:${String(err?.message || err).slice(0, 60)}`);
      continue;
    }
    const organic = serp?.organic_results || serp?.results || [];
    for (const row of organic) {
      const blob = [row.title, row.snippet, row.link].filter(Boolean).join(" ");
      for (const c of extractCnpjCandidatesFromText(blob)) cnpjHits.add(c);
      if (row.link && isWeakBusinessDirectoryHost(row.link)) {
        notes.push(`directory_hit_unvalidated:${row.link}`);
      }
    }
  }

  metrics.cnpj_candidates = cnpjHits.size;

  if (!cnpjHits.size) {
    const fx = findCnpjFixtureByHotelIdentity(hotel);
    if (fx?.cnpj) {
      cnpjHits.add(fx.cnpj);
      notes.push(`fixture_identity_match:${fx.cnpj}`);
      metrics.cnpj_candidates = cnpjHits.size;
    }
  }

  /** @type {object[]} */
  const seeds = [];

  for (const cnpj of [...cnpjHits].slice(0, 5)) {
    const reg = await fetchCnpjRegistry(cnpj, { env });
    if (!reg.ok || !reg.record) {
      notes.push(`cnpj_lookup_failed:${cnpj}`);
      continue;
    }
    const validation = validateCnpjSeed(hotel, { legal_name: reg.record.razao_social, cnpj }, reg.record);
    if (validation.reject_from_ownership_research) {
      metrics.rejected += 1;
      notes.push(`discovered_cnpj_rejected:${formatCnpj(cnpj)}:${validation.status}`);
      continue;
    }
    metrics.validated += 1;
    seeds.push({
      cnpj,
      registry: reg.record,
      validation,
      discovery_method: "hotel_name_to_company",
      source: webDisc.ok && webDisc.cnpj === cnpj ? "hotel_website" : "serp_cnpj_extraction",
    });
  }

  seeds.sort((a, b) => (b.validation.composite_score || 0) - (a.validation.composite_score || 0));

  return {
    ok: seeds.length > 0,
    seeds,
    metrics,
    notes,
    latency_ms: Date.now() - started,
    method_attempt: createMethodAttempt({
      method: "hotel_name_to_company",
      success: seeds.length > 0,
      candidate_count: seeds.length,
      searches: metrics.searches,
      latency_ms: Date.now() - started,
      notes,
    }),
  };
}
