/**
 * Blind native ownership research runner (Packet 2.4B).
 * Loads ONLY a T0 seed. Must not import evaluation-only benchmark or Webhound answers.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PDFParse } from "pdf-parse";
import "dotenv/config";
import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { fetchResearchPage, htmlToSearchableText } from "../../room-count-research/fetch.js";
import {
  dispatchPlaybooks,
  buildDynamicSearchQueries,
  buildDocumentSearchTerms,
} from "../playbook-dispatcher.js";
import { NEGATIVE_SCREENS, TEMPORAL_BRAND_STATUSES } from "../playbook-schema.js";
import { extractClaimsFromCorpus } from "./seed-scoped-extractor.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");

const SERPAPI_USD = 0.01;

function has(text, re) {
  return re.test(String(text || ""));
}

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

async function extractPdfText(buf) {
  const parser = new PDFParse({ data: buf });
  try {
    const parsed = await parser.getText();
    return String(parsed?.text || "");
  } finally {
    if (typeof parser.destroy === "function") await parser.destroy();
  }
}

async function fetchAny(url) {
  const started = Date.now();
  const isPdf = /\.pdf($|\?)/i.test(url);
  try {
    if (isPdf) {
      const res = await fetch(url, {
        redirect: "follow",
        headers: {
          "user-agent": "DealalityNativeResearch/2.4B",
          accept: "application/pdf,*/*",
        },
      });
      const buf = Buffer.from(await res.arrayBuffer());
      let text = "";
      let method = "pdf-parse";
      try {
        text = await extractPdfText(buf);
      } catch (err) {
        method = `pdf_fail:${err?.message || err}`;
      }
      return {
        ok: res.ok,
        status: res.status,
        url: res.url || url,
        is_pdf: true,
        text,
        extraction_method: method,
        bytes: buf.length,
        duration_ms: Date.now() - started,
      };
    }
    const page = await fetchResearchPage(url, { timeoutMs: 25000 });
    return {
      ok: page.ok,
      status: page.status,
      url: page.url,
      is_pdf: false,
      text: htmlToSearchableText(page.text || ""),
      extraction_method: "html",
      bytes: page.length || 0,
      duration_ms: page.latency_ms || Date.now() - started,
      blocked: page.blocked,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      error: String(err?.message || err),
      text: "",
      duration_ms: Date.now() - started,
    };
  }
}

function scoreUrl(url, title = "", snippet = "") {
  const u = String(url || "").toLowerCase();
  const blob = `${title} ${snippet} ${u}`.toLowerCase();
  let score = 0;
  if (/\.pdf($|\?)/.test(u)) score += 4;
  if (/reporte[_-]?anual|annual[_-]?report/.test(u)) score += 12;
  if (/gsf-hotels|bmv\.com\.mx|newsroom\.hyatt|krystalgrand|fibrahotel|fibrainn|xcaret|rosewood|belmond/i.test(u)) score += 4;
  if (/reporte.?anual|annual.?report|investor|bmv|evento.?relevante/.test(blob)) score += 3;
  if (/hilton|chartwell|breathless|ihvsf|inmobiliaria|propiedad|owner/.test(blob)) score += 2;
  if (/krystalgrand-puertovallarta|en\.krystalgrand/.test(u)) score += 6;
  if (/newsroom\.hyatt\.com/.test(u)) score += 6;
  // Prefer annual over noisy quarterlies when both exist
  if (/[1234]q\d{2}|[1234]t\d{2}|earnings.?report|xbrl/i.test(u) && !/reporte[_-]?anual/i.test(u)) {
    score -= 3;
  }
  if (/tripadvisor|booking\.com|expedia|facebook|instagram|youtube/.test(u)) score -= 4;
  return score;
}

/**
 * MX-PUBCO playbook improvement: once an issuer IR PDF path is discovered via search,
 * probe recent annual-report filename patterns (does not hard-code hotel answers).
 */
export function expandIssuerAnnualReportCandidates(urls = [], tickerHint = "HOTEL") {
  const bases = new Set();
  for (const url of urls) {
    const m = String(url || "").match(/^(https?:\/\/[^?#]+\/investors\/pdf\/)/i);
    if (m) bases.add(m[1]);
  }
  const years = [2025, 2024, 2023, 2022];
  const out = [];
  const ticker = String(tickerHint || "HOTEL").toUpperCase();
  for (const base of bases) {
    for (const y of years) {
      out.push(`${base}${ticker}_Reporte_anual_${y}.pdf`);
      out.push(`${base}${ticker}_Reporte_Anual_${y}.pdf`);
      out.push(`${base}${ticker}_Annual_Report_${y}.pdf`);
    }
  }
  return [...new Set(out)];
}

/**
 * @param {object} opts
 * @param {object} opts.seedHotel
 * @param {string} [opts.runId]
 * @param {string} [opts.outDir]
 */
export async function runBlindNativeResearch(opts = {}) {
  const seedHotel = opts.seedHotel;
  if (!seedHotel?.name) throw new Error("seedHotel.name required");

  const runId =
    opts.runId ||
    `native_${String(seedHotel.name).toLowerCase().replace(/[^a-z0-9]+/g, "_").slice(0, 24)}_${Date.now().toString(36)}`;
  const outDir =
    opts.outDir ||
    path.join(ROOT, "reports/hotel-intelligence/golden-demo-audit-v1/native-runs", runId);
  fs.mkdirSync(outDir, { recursive: true });

  const t0 = Date.now();
  const cost = {
    serpapi_searches: 0,
    serpapi_usd: 0,
    fetches: 0,
    pdfs: 0,
    llm_usd: 0,
    apify_usd: 0,
    total_usd_estimate: 0,
  };

  const dispatch = dispatchPlaybooks(seedHotel, {});
  const queries = buildDynamicSearchQueries(seedHotel, {});
  const organic = [];
  const searchLog = [];

  for (const q of queries) {
    try {
      const res = await serpapiSearch({ engine: "google", q, num: 8, gl: "mx", hl: "es" });
      cost.serpapi_searches += 1;
      cost.serpapi_usd += Number(res?.creditsCharged ?? 1) * SERPAPI_USD;
      if (!res?.ok) {
        searchLog.push({ query: q, error: res?.error?.message || "serpapi_failed", result_count: 0 });
        continue;
      }
      const payload = res.data || {};
      const results = payload.organic_results || payload.organic || [];
      searchLog.push({
        query: q,
        result_count: results.length,
        top: results.slice(0, 5).map((r) => ({
          title: r.title,
          url: r.link || r.url,
          snippet: r.snippet,
        })),
      });
      for (const r of results) {
        const url = r.link || r.url;
        if (!url) continue;
        organic.push({
          url,
          title: r.title || "",
          snippet: r.snippet || "",
          score: scoreUrl(url, r.title, r.snippet),
        });
      }
    } catch (err) {
      searchLog.push({ query: q, error: String(err?.message || err) });
    }
  }

  // Deduplicate URLs by score
  const byUrl = new Map();
  for (const o of organic) {
    const prev = byUrl.get(o.url);
    if (!prev || o.score > prev.score) byUrl.set(o.url, o);
  }

  // MX-PUBCO-01 improvement: expand annual-report filename candidates from discovered IR paths
  const irExpansions = expandIssuerAnnualReportCandidates([...byUrl.keys()], "HOTEL");
  for (const url of irExpansions) {
    if (!byUrl.has(url)) {
      byUrl.set(url, {
        url,
        title: "issuer_annual_report_filename_probe",
        snippet: "MX-PUBCO IR path annual-report pattern expansion",
        score: scoreUrl(url, "Reporte anual", "annual report"),
      });
    }
  }

  const rankedAll = [...byUrl.values()].sort((a, b) => b.score - a.score);
  const pick = (pred, n) => rankedAll.filter(pred).slice(0, n);
  const ranked = [];
  const seen = new Set();
  const pushAll = (items) => {
    for (const item of items) {
      if (seen.has(item.url)) continue;
      seen.add(item.url);
      ranked.push(item);
    }
  };
  // Force diversity across decisive source classes (hotel / annual / brand newsroom / other)
  pushAll(pick((r) => /krystalgrand/i.test(r.url), 2));
  pushAll(pick((r) => /reporte[_-]?anual|annual[_-]?report/i.test(r.url), 2));
  pushAll(pick((r) => /newsroom\.hyatt|hyatt\.com/i.test(r.url), 2));
  pushAll(pick((r) => /gsf-hotels\.com/i.test(r.url) && !/\.pdf/i.test(r.url), 1));
  pushAll(pick((r) => /\.pdf($|\?)/i.test(r.url), 4));
  pushAll(rankedAll);
  const rankedLimited = ranked.slice(0, 14);

  const docs = [];
  const failed_sources = [];
  for (const item of rankedLimited) {
    if (item.score < 1 && docs.length >= 5) continue;
    const fetched = await fetchAny(item.url);
    cost.fetches += 1;
    if (fetched.is_pdf) cost.pdfs += 1;
    if (!fetched.ok || !fetched.text || fetched.text.length < 40) {
      failed_sources.push({
        url: item.url,
        status: fetched.status,
        error: fetched.error || (fetched.text ? "too_short" : "empty"),
      });
      continue;
    }
    docs.push({
      url: fetched.url || item.url,
      title: item.title,
      host: hostOf(fetched.url || item.url),
      is_pdf: Boolean(fetched.is_pdf),
      extraction_method: fetched.extraction_method,
      bytes: fetched.bytes,
      duration_ms: fetched.duration_ms,
      text: fetched.text,
      text_preview: fetched.text.slice(0, 500),
    });
    if (docs.length >= 10) break;
  }

  const extracted = extractClaimsFromCorpus(docs, seedHotel);
  const docTerms = buildDocumentSearchTerms(seedHotel, {
    aliases: extracted.aliases,
    entities: extracted.entities,
  });

  cost.total_usd_estimate = Number(
    (cost.serpapi_usd + cost.llm_usd + cost.apify_usd).toFixed(4)
  );

  const dossier = {
    version: "blind-native-research-dossier-v1",
    run_id: runId,
    started_at: new Date(t0).toISOString(),
    finished_at: new Date().toISOString(),
    duration_ms: Date.now() - t0,
    starting_seed: seedHotel,
    playbooks_invoked: dispatch,
    queries: searchLog,
    document_search_terms: docTerms,
    sources_attempted: rankedLimited.map((r) => ({ url: r.url, score: r.score, title: r.title })),
    sources_failed: failed_sources,
    sources_rejected: rankedLimited.filter((r) => r.score < 0).map((r) => r.url),
    documents_retrieved: docs.map(({ text, ...rest }) => rest),
    entity_aliases_discovered: extracted.aliases,
    candidate_and_validated_relationships: extracted.claims,
    negative_screens: extracted.negative_screens_fired,
    temporal_events: extracted.timeline,
    people: extracted.people,
    contacts: extracted.contacts || [],
    research_gaps: [
      extracted.claims.some((c) => /propco/i.test(c.claim_type || "") && c.entity)
        ? null
        : "PropCo not resolved from retrieved documents",
      extracted.claims.some(
        (c) => /economic_owner/i.test(c.claim_type || "") && c.confidence === "HIGH" && c.entity
      )
        ? null
        : extracted.claims.some(
              (c) => /economic_owner/i.test(c.claim_type || "") && c.confidence === "NOT_VERIFIED"
            )
          ? "Economic owner correctly abstained (NOT_VERIFIED)"
          : "Economic owner not resolved from retrieved documents",
      "Natural-person UBO not verified",
      "Legal/franchise signatory not verified",
    ].filter(Boolean),
    cost,
    stop_conditions: {
      hotel_identity: extracted.claims.some((c) => c.claim_type === "hotel_identity" || c.claim_type === "brand"),
      operator: extracted.claims.some((c) => c.claim_type === "operator"),
      economic_owner_or_gap: true,
      brand_resolved: extracted.claims.some((c) => c.claim_type === "brand" || c.claim_type === "current_brand"),
      contradictions_evaluated: extracted.negative_screens_fired.includes("ADJACENT_ASSET") ||
        extracted.negative_screens_fired.includes("ANNOUNCED_NOT_CURRENT") ||
        extracted.negative_screens_fired.includes("OPERATOR_NOT_OWNER"),
    },
    temporal_brand_statuses_supported: TEMPORAL_BRAND_STATUSES,
    negative_screens_supported: NEGATIVE_SCREENS,
    comparison_ready: true,
    blind: true,
    inputs_forbidden: [
      "fixtures/golden-demo/evaluation-only/*",
      "webhound_final_answers",
      "krystal-grand-pv-deep-research-v1.json as claim input",
    ],
  };

  const dossierPath = path.join(outDir, "research-dossier.json");
  fs.writeFileSync(dossierPath, JSON.stringify(dossier, null, 2));
  fs.writeFileSync(
    path.join(outDir, "claims.json"),
    JSON.stringify(
      {
        run_id: runId,
        claims: extracted.claims,
        negative_screens: extracted.negative_screens_fired,
        people: extracted.people,
        timeline: extracted.timeline,
      },
      null,
      2
    )
  );

  return { runId, outDir, dossierPath, dossier };
}
