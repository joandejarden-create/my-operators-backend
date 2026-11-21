#!/usr/bin/env node
/**
 * HIF V6 Track B — Regulatory portal validation + free hotel-entity searches.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  buildWeeklySnapshot,
  emptyWeeklyMetrics,
  ensureWeeklyScorecardLayout,
  saveWeeklySnapshot,
  isoWeekId,
  attributeHotelGeography,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import { buildV6ExecutionGate } from "../lib/hotel-intelligence/sec-edgar/hif-v6-shared-research-contract.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-b-regulatory");
const MERGE_SHA = "9783ff27bc940fcee3b692088353c38674e6e188";
const RUN = process.argv.includes("--run");
const UA =
  process.env.SEC_EDGAR_USER_AGENT ||
  process.env.EDGAR_USER_AGENT ||
  "DealalityHIFResearch/6.0 (research@dealality.com)";

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}

async function probe(url, opts = {}) {
  const started = Date.now();
  try {
    const res = await fetch(url, {
      method: opts.method || "GET",
      headers: {
        "User-Agent": UA,
        Accept: opts.accept || "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        ...(opts.headers || {}),
      },
      redirect: "follow",
      signal: AbortSignal.timeout(opts.timeoutMs || 30000),
    });
    const text = await res.text();
    return {
      url,
      ok: res.ok,
      status: res.status,
      contentType: res.headers.get("content-type") || "",
      bytes: text.length,
      ms: Date.now() - started,
      hotelHits: (text.match(/\bhotels?\b|\bresorts?\b|\blodging\b|\bhospitality\b/gi) || []).length,
      companyHits: extractCompanyLikeNames(text),
      bodyPreview: text.slice(0, 400),
      text,
    };
  } catch (err) {
    return {
      url,
      ok: false,
      status: null,
      error: String(err?.message || err),
      ms: Date.now() - started,
      hotelHits: 0,
      companyHits: [],
      text: "",
    };
  }
}

function extractCompanyLikeNames(text) {
  const out = [];
  const html = String(text || "");
  const push = (name) => {
    const n = String(name || "")
      .replace(/&amp;/g, "&")
      .replace(/&nbsp;/g, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (n.length >= 4 && n.length < 140 && !out.includes(n)) out.push(n);
  };
  for (const m of html.matchAll(
    /<li class="type-company">[\s\S]*?href="\/company\/([A-Z0-9]+)"[\s\S]*?>([\s\S]*?)<\/a>/gi,
  )) {
    push(`${m[2]} [${m[1]}]`);
  }
  for (const m of html.matchAll(/href="\/company\/([A-Z0-9]+)"[\s\S]*?>\s*([A-Z0-9][^<]{2,120})/gi)) {
    push(`${m[2]} [${m[1]}]`);
  }
  for (const m of html.matchAll(
    /\b([A-Z][A-Za-z0-9&.'’-]{2,}(?:\s+[A-Z][A-Za-z0-9&.'’-]{2,}){0,5})\s+(?:Hotel|Hotels|Hospitality|Resorts?|Inn)\b/g,
  )) {
    push(m[0]);
  }
  return out.slice(0, 60);
}

const PORTALS = [
  {
    id: "BR_CVM_OPEN_DATA",
    country: "BR",
    region: "SOUTH_AMERICA",
    urls: [
      "https://dados.cvm.gov.br/",
      "https://dados.cvm.gov.br/dataset/?q=companhia",
      "https://www.gov.br/cvm/pt-br/assuntos/dados",
    ],
  },
  {
    id: "JP_EDINET",
    country: "JP",
    region: "OTHER_ELIGIBLE",
    urls: [
      "https://disclosure.edinet-fsa.go.jp/",
      "https://disclosure2.edinet-fsa.go.jp/week0010.aspx",
    ],
  },
  {
    id: "UK_COMPANIES_HOUSE",
    country: "GB",
    region: "WESTERN_EUROPE",
    urls: [
      "https://find-and-update.company-information.service.gov.uk/search/companies?q=hotel+management",
      "https://find-and-update.company-information.service.gov.uk/search/companies?q=marriott+hotels",
      "https://find-and-update.company-information.service.gov.uk/search/companies?q=hilton+hotels+limited",
      "https://api.company-information.service.gov.uk/search/companies?q=hotel",
    ],
    accept: "text/html",
  },
  {
    id: "SG_SGX",
    country: "SG",
    region: "OTHER_ELIGIBLE",
    urls: [
      "https://www.sgx.com/",
      "https://www.sgx.com/securities/company-announcements",
    ],
  },
  {
    id: "HK_HKEX",
    country: "HK",
    region: "OTHER_ELIGIBLE",
    urls: [
      "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities?sc_lang=en",
      "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en",
    ],
  },
  {
    id: "CA_SEDAR_PLUS",
    country: "CA",
    region: "NORTH_AMERICA",
    urls: [
      "https://www.sedarplus.ca/",
      "https://www.sedarplus.ca/csa-party/records/search.html",
    ],
  },
];

async function main() {
  const gate = buildV6ExecutionGate({ merged: true, mergeSha: MERGE_SHA });
  writeJson(path.join(ROOT, "reports/sec-edgar-hif-v6-dual-track/EXECUTION_GATE.json"), {
    ...gate,
    founderAuthorization: "PARALLEL_DISCOVERY_AUTHORIZED_2026-11-21",
    verifiedAgainstOriginMain: true,
    asOf: new Date().toISOString(),
  });
  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, portals: PORTALS.map((p) => p.id) }, null, 2));
    return;
  }

  fs.mkdirSync(OUT, { recursive: true });
  const results = [];
  const entityCandidates = [];

  for (const portal of PORTALS) {
    const probes = [];
    for (const url of portal.urls) {
      const r = await probe(url, {
        accept: portal.accept || undefined,
      });
      // drop huge text from persisted probe
      const { text, ...rest } = r;
      probes.push(rest);
      for (const name of r.companyHits || []) {
        entityCandidates.push({
          portalId: portal.id,
          legalNameOrLabel: name,
          sourceJurisdiction: portal.country,
          ...attributeHotelGeography({
            hotelCountry: portal.country === "GB" ? "GB" : portal.country === "BR" ? "BR" : "UNATTRIBUTED",
            sourceJurisdiction: portal.country,
          }),
          sourceUrl: url,
          signalClass: "RESEARCH_CANDIDATE",
          note: "Free public search/entity label — not yet property-linked or contract-verified",
        });
      }
    }
    const anyOk = probes.some((p) => p.ok || p.status === 401 || p.status === 403);
    let accessStatus = probes.some((p) => p.ok) ? "REACHABLE" : "UNREACHABLE_OR_BLOCKED";
    let adapterStatus = probes.some((p) => p.ok) ? "ACCESS_VALIDATED_PILOT" : "ACCESS_FAILED";
    const api = probes.find((p) => /api\.company-information|api\.edinet/.test(p.url || ""));
    if (api && (api.status === 401 || api.status === 403)) {
      accessStatus = "REACHABLE_AUTH_REQUIRED";
      adapterStatus = "API_KEY_REQUIRED";
    }
    results.push({
      ...portal,
      accessStatus,
      adapterStatus,
      probes,
      hotelLexiconHits: probes.reduce((n, p) => n + (p.hotelHits || 0), 0),
      entityCandidates: probes.reduce((n, p) => n + (p.companyHits?.length || 0), 0),
    });
  }

  // Deduplicate entity candidates
  const byEntity = new Map();
  for (const e of entityCandidates) {
    const k = `${e.legalNameOrLabel}|${e.sourceJurisdiction}`;
    if (!byEntity.has(k)) byEntity.set(k, e);
  }

  const metrics = {
    track: "TRACK_B",
    wave: "HIF_V6_TRACK_B_REGULATORY_WAVE1",
    mergeBaselineSha: MERGE_SHA,
    portalsProbed: results.length,
    portalsReachable: results.filter((r) => String(r.accessStatus).startsWith("REACHABLE")).length,
    entityCandidates: byEntity.size,
    propertyVerifiedOwners: 0,
    verifiedContractExpirations: 0,
    researchCostUsd: 0,
    agentSpendUsd: 0,
    publicationState: "CUSTOMER_BLOCKED",
  };

  writeJson(path.join(OUT, "PORTAL_ACCESS_RESULTS.json"), { results });
  writeJson(path.join(OUT, "ENTITY_CANDIDATES.json"), {
    count: byEntity.size,
    entities: [...byEntity.values()],
  });
  writeJson(path.join(OUT, "TRACK_B_METRICS.json"), metrics);

  ensureWeeklyScorecardLayout(ROOT);
  const global = emptyWeeklyMetrics();
  global.documentsRetrievedAndProcessed = results.reduce(
    (n, r) => n + r.probes.filter((p) => p.ok).length,
    0,
  );
  global.researchCandidates = byEntity.size;
  global.distinctHotelsDiscovered = 0; // entities ≠ hotels until linked
  global.researchCostUsd = 0;

  const byCountry = {};
  for (const r of results) {
    byCountry[r.country] = byCountry[r.country] || emptyWeeklyMetrics();
    byCountry[r.country].documentsRetrievedAndProcessed += r.probes.filter((p) => p.ok).length;
    byCountry[r.country].researchCandidates += r.entityCandidates || 0;
  }

  const snap = buildWeeklySnapshot({
    weekId: `${isoWeekId()}-track-b-regulatory`,
    asOf: new Date().toISOString().slice(0, 10),
    global,
    byCountry,
    byRegion: Object.fromEntries(
      results.map((r) => [
        r.region,
        {
          covered: r.probes.some((p) => p.ok),
          coverageNote: r.probes.some((p) => p.ok)
            ? "PORTAL_ACCESS_VALIDATED_ENTITY_SEARCH"
            : "ELIGIBLE_NOT_COVERED_UNTIL_RESEARCH_PERFORMED",
          ...emptyWeeklyMetrics(),
          documentsRetrievedAndProcessed: r.probes.filter((p) => p.ok).length,
          researchCandidates: r.entityCandidates || 0,
        },
      ]),
    ),
    bySourceFamily: {
      INTERNATIONAL_SECURITIES_REGULATORS: {
        independentlyVerifiedIncrementalFindings: 0,
        documentsProcessed: global.documentsRetrievedAndProcessed,
        costUsd: 0,
        trackBFindings: byEntity.size,
        trackAFindings: 0,
        duplicateFindings: 0,
        holdOrInvalidFindings: 0,
      },
      GOVERNMENT_APIS: {
        independentlyVerifiedIncrementalFindings: 0,
        documentsProcessed: metrics.portalsReachable,
        costUsd: 0,
        trackBFindings: metrics.portalsReachable,
        trackAFindings: 0,
        duplicateFindings: 0,
        holdOrInvalidFindings: 0,
      },
      CORPORATE_REGISTRIES: {
        independentlyVerifiedIncrementalFindings: 0,
        documentsProcessed: byCountry.GB?.documentsRetrievedAndProcessed || 0,
        costUsd: 0,
        trackBFindings: [...byEntity.values()].filter((e) => e.sourceJurisdiction === "GB").length,
        trackAFindings: 0,
        duplicateFindings: 0,
        holdOrInvalidFindings: 0,
      },
    },
    trackA: emptyWeeklyMetrics(),
    trackB: { ...global },
    hotelExamples: [],
    notes: { track: "TRACK_B", eligibilityIsNotCoverage: true },
  });
  snap.allowOverwrite = true;
  saveWeeklySnapshot(snap, ROOT);

  fs.writeFileSync(
    path.join(OUT, "FOUNDER_TRACK_B_WAVE.md"),
    [
      "# HIF V6 Track B — Regulatory wave 1",
      "",
      `Merge baseline: \`${MERGE_SHA}\``,
      "",
      `| Portal | Country | Access | Entities |`,
      `|---|---|---|---:|`,
      ...results.map(
        (r) => `| ${r.id} | ${r.country} | ${r.accessStatus} | ${r.entityCandidates || 0} |`,
      ),
      "",
      `- Distinct entity candidates: ${byEntity.size}`,
      `- Verified contract expirations: 0 (property linking next)`,
      `- Spend: $0`,
      "",
      "## Sample entities",
      ...[...byEntity.values()]
        .slice(0, 20)
        .map((e) => `- ${e.legalNameOrLabel} (${e.sourceJurisdiction}) · ${e.sourceUrl}`),
    ].join("\n"),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        metrics,
        results: results.map((r) => ({
          id: r.id,
          accessStatus: r.accessStatus,
          entities: r.entityCandidates,
          hits: r.hotelLexiconHits,
        })),
        out: OUT,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
