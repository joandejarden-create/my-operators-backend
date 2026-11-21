#!/usr/bin/env node
/**
 * Source Intelligence Audit — evidence review + name-free discovery validation.
 * Held-out: excludes known successful CMBS URLs from prior waves.
 * CUSTOMER_BLOCKED · $0 · free EDGAR only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import {
  HIF_V6_SOURCE_INTELLIGENCE_AUDIT_V6,
  SOURCE_FAMILY_TAXONOMY,
  DISCOVERY_STRATEGIES,
  scoreCmbsScheduleYield,
  listNameFreeQueries,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-source-intelligence-audit-v6.js";
import { attributeHotelGeography } from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-source-intelligence-audit");
const RUN = process.argv.includes("--run");
const LIMIT = Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 24;
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const asOf = new Date().toISOString().slice(0, 10);

const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function hotelKey(name, country = "US") {
  return `${String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()}|${String(country || "US").toUpperCase()}`;
}

function load(rel) {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

function collectKnownCorpus() {
  const successUrls = new Set();
  const knownHotels = new Set();
  const rejectedReasons = [];
  const successDocs = [];

  const wave2 = load("reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json");
  for (const h of wave2?.hotels || []) {
    if (h.sourceUrl) successUrls.add(h.sourceUrl.replace(/\/0+(\d)/, "/$1"));
    knownHotels.add(hotelKey(h.hotelName, h.hotelCountry || "US"));
    successDocs.push({ hotel: h.hotelName, url: h.sourceUrl, wave: "WAVE2", type: "VERIFIED_DATE" });
  }

  const cmbs = load("reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json");
  for (const h of [...(cmbs?.verified || []), ...(cmbs?.probable || [])]) {
    const u = h.source?.documentUrl || h.sourceUrl;
    if (u) successUrls.add(u.replace(/\/0+(\d)/, "/$1"));
    if (h.hotelName) knownHotels.add(hotelKey(h.hotelName, "US"));
  }

  const w3 = load("reports/sec-edgar-hif-v6-track-a-wave34/WAVE3_NET_NEW.json");
  for (const h of w3?.hotels || []) {
    if (h.sourceUrl) successUrls.add(h.sourceUrl);
    knownHotels.add(hotelKey(h.hotelName, h.hotelCountry || "US"));
  }

  const yieldQ = load("reports/sec-edgar-hif-v6-yield-wave/YIELD_PROSPECTS_QUARANTINED.json");
  for (const h of yieldQ?.hotels || []) {
    rejectedReasons.push({ reason: h.reason || "YIELD_QUARANTINE", hotel: h.hotelName, url: h.sourceUrl });
  }

  const geo = load("reports/sec-edgar-hif-v6-country-concentration/GEOGRAPHY_SANITY.json");
  const geoHold = geo?.geographyUnverifiedHold || geo?.hold || [];

  const wave2Urls = load("reports/sec-edgar-hif-v6-track-a-wave2/WAVE2_CMBS_URLS.json");
  const priorAttemptUrls = new Set((wave2Urls?.cmbsUrls || []).map((x) => x.url));

  // Distinguishing features of success: count unique success URLs that produced verified dates
  const successUrlToHotels = new Map();
  for (const h of wave2?.hotels || []) {
    const u = h.sourceUrl;
    if (!u) continue;
    if (!successUrlToHotels.has(u)) successUrlToHotels.set(u, []);
    successUrlToHotels.get(u).push(h.hotelName);
  }

  return {
    successUrls,
    knownHotels,
    successDocs,
    rejectedReasons,
    geoHoldCount: Array.isArray(geoHold) ? geoHold.length : 0,
    priorAttemptUrls,
    successUrlToHotels: [...successUrlToHotels.entries()].map(([url, hotels]) => ({
      url,
      hotelCount: hotels.length,
      sample: hotels.slice(0, 5),
    })),
  };
}

/**
 * Build EDGAR archive URL from an EFTS hit.
 * Path CIK = depositor/issuer ciks[0] (NOT accession CIK 1539497 filing agent).
 * Keep alternate CIKs for retry on 404.
 */
function edgarUrlFromEftsHit(h) {
  const src = h._source || {};
  const adshDashed = String(src.adsh || "");
  const adsh = adshDashed.replace(/-/g, "");
  const fileName = String(h._id || "").split(":").pop();
  const cikList = [...new Set((src.ciks || []).map((c) => String(c).replace(/^0+/, "")).filter(Boolean))];
  const cik = cikList[0] || "";
  return {
    id: h._id,
    url: `https://www.sec.gov/Archives/edgar/data/${cik}/${adsh}/${fileName}`,
    altUrls: cikList.slice(1).map((c) => `https://www.sec.gov/Archives/edgar/data/${c}/${adsh}/${fileName}`),
    form: src.form || (src.root_forms || [])[0] || src.file_type || null,
    fileDate: src.file_date,
    displayNames: src.display_names || [],
    pathCik: cik,
    adsh: adshDashed,
  };
}

async function efts(q, forms, size = 20) {
  const p = new URLSearchParams({
    q,
    dateRange: "custom",
    startdt: "2016-01-01",
    enddt: "2026-11-21",
    from: "0",
    size: String(size),
  });
  if (forms) p.set("forms", forms);
  const r = await fetch(`https://efts.sec.gov/LATEST/search-index?${p}`, {
    headers: { "User-Agent": UA, Accept: "application/json" },
  });
  if (!r.ok) return { ok: false, status: r.status, hits: [] };
  const j = await r.json();
  const hits = (j.hits?.hits || []).map(edgarUrlFromEftsHit);
  return { ok: true, hits };
}

function isNoisy(name) {
  const n = String(name || "").trim();
  if (n.length < 12) return true;
  if (/^(the\s+)?(hilton|marriott|hyatt|embassy suites|courtyard|holiday inn)\b/i.test(n) && n.split(/\s+/).length < 4) {
    return true;
  }
  if (/\b(same-property|system-wide|portfolio|borrower|mortgaged property)\b/i.test(n)) return true;
  return false;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const corpus = collectKnownCorpus();

  const evidenceAudit = {
    version: HIF_V6_SOURCE_INTELLIGENCE_AUDIT_V6,
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    priorSuccessDistinctDocs: corpus.successUrlToHotels.length,
    priorSuccessDocProfiles: corpus.successUrlToHotels,
    knownHotels: corpus.knownHotels.size,
    knownSuccessUrls: corpus.successUrls.size,
    priorCmbsAttemptUrls: corpus.priorAttemptUrls.size,
    quarantineSamples: corpus.rejectedReasons.slice(0, 40),
    distinguishingSuccessFactors: [
      "Property-level franchise/management expiration COLUMN present in CMBS 424B annex HTML",
      "File naming pattern n*_424b2-x*.htm / n*_x*-424b2.htm (annex, not marketing TOC only)",
      "Multiple hotels extracted from ONE document (schedule density)",
      "Independent of hotel-name-first search — hotels unknown a priori were discovered from the schedule",
    ],
    distinguishingFailureFactors: [
      "Narrative Hotel Properties sections without franchise expiration columns",
      "Loan maturity / ARD columns mistaken for contract expirations (guarded by extractor)",
      "Country-keyword search without physical geography gate",
      "EX-10 / 8-K name NER without schedule structure → high quarantine",
      "Portal reachability / company registration ≠ contractual intelligence",
    ],
    taxonomy: SOURCE_FAMILY_TAXONOMY,
    strategies: DISCOVERY_STRATEGIES,
  };
  writeJson(path.join(OUT, "EVIDENCE_AUDIT.json"), evidenceAudit);

  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, knownHotels: corpus.knownHotels.size }, null, 2));
    return;
  }

  const queries = listNameFreeQueries().filter((q) => q.sourceFamily === "SEC_CMBS" || q.sourceFamily === "SEC_EX10");
  // Add hybrid + hotel-name-first control (small) for comparison
  const strategyQueries = [
    ...queries.map((q) => ({ ...q, heldOut: true })),
    {
      id: "control_hotel_name_first",
      q: '"Holiday Inn Express" "Expiration/Termination of Related License"',
      forms: "424B2",
      strategy: DISCOVERY_STRATEGIES.A_HOTEL_NAME_FIRST,
      sourceFamily: "SEC_CMBS",
      heldOut: true,
    },
    {
      id: "hybrid_related_license_hotel",
      q: '"Expiration/Termination of Related License" (Hotel OR Courtyard OR Hilton OR Marriott)',
      forms: "424B2",
      strategy: DISCOVERY_STRATEGIES.F_HYBRID,
      sourceFamily: "SEC_CMBS",
      heldOut: true,
    },
  ];

  const byUrl = new Map();
  const queryStats = [];
  for (const sq of strategyQueries) {
    const res = await efts(sq.q, sq.forms, 25);
    const heldOutHits = (res.hits || []).filter((h) => {
      const norm = h.url.replace(/\/0+(\d)/, "/$1");
      return !corpus.successUrls.has(h.url) && !corpus.successUrls.has(norm) && !corpus.priorAttemptUrls.has(h.url);
    });
    queryStats.push({
      id: sq.id,
      strategy: sq.strategy,
      q: sq.q,
      forms: sq.forms,
      hitsTotal: (res.hits || []).length,
      hitsHeldOut: heldOutHits.length,
      ok: res.ok,
    });
    for (const h of heldOutHits) {
      if (!byUrl.has(h.url)) byUrl.set(h.url, { ...h, strategies: [sq.strategy], queryIds: [sq.id] });
      else {
        const e = byUrl.get(h.url);
        e.strategies = [...new Set([...e.strategies, sq.strategy])];
        e.queryIds = [...new Set([...e.queryIds, sq.id])];
      }
    }
    await sleep(280);
  }
  writeJson(path.join(OUT, "NAME_FREE_QUERY_STATS.json"), { queryStats });

  const docs = [...byUrl.values()].slice(0, LIMIT);
  const edgar = createSecEdgarClient();
  const docResults = [];
  const netNew = [];
  const duplicates = [];
  const falsePositives = [];
  const byStrategy = {};

  for (const d of docs) {
    const row = {
      url: d.url,
      strategies: d.strategies,
      queryIds: d.queryIds,
      ok: false,
      yieldScore: null,
      extractable: false,
      findings: 0,
      netNew: 0,
      error: null,
    };
    try {
      const tryUrls = [d.url, ...(d.altUrls || [])];
      let res = null;
      let usedUrl = d.url;
      for (const u of tryUrls) {
        res = await edgar.request(u, {
          accept: "text/html,*/*",
          maxAgeMs: 14 * 24 * 3600 * 1000,
        });
        const body = res.text || "";
        if (res.ok && body.length >= 5000 && !/<Code>NoSuchKey<\/Code>/i.test(body)) {
          usedUrl = u;
          break;
        }
        res = { ...res, ok: false };
      }
      d.url = usedUrl;
      row.url = usedUrl;
      if (!res?.ok) {
        row.error = `HTTP_${res?.status || res?.error || "FETCH_FAIL"}`;
        docResults.push(row);
        continue;
      }
      const text = res.text || "";
      row.ok = true;
      const ys = scoreCmbsScheduleYield(text, d.url);
      row.yieldScore = ys;
      row.extractable = ys.likelyExtractableSchedule;

      const extracted = extractCmbsPropertySchedule(
        { text, url: d.url },
        { sourceUrl: d.url, formType: d.form },
      );
      const findings = extracted.hotelContractFindings || [];
      row.findings = findings.length;
      row.loanMaturitiesIsolated = (extracted.loanMaturityFindings || []).length;

      for (const f of findings) {
        const hotelName = f.hotelName || f.attributedHotelName || f.propertyName;
        if (!hotelName || isNoisy(hotelName)) {
          falsePositives.push({ hotelName, url: d.url, reason: "NOISY_OR_MISSING" });
          continue;
        }
        const country = "US";
        const key = hotelKey(hotelName, country);
        const exp = f.primaryDate || f.expirationDate || f.isoDate || null;
        const meaning = f.dateMeaning || f.agreementType || "";
        // Skip loan maturities (also isolated by extractor)
        if (/LOAN_MATURITY/i.test(String(meaning))) continue;

        if (corpus.knownHotels.has(key)) {
          duplicates.push({ hotelName, url: d.url, expirationDate: exp });
          continue;
        }
        corpus.knownHotels.add(key);
        const agreementType = /MANAGE/i.test(String(meaning) + String(f.agreementType || ""))
          ? "MANAGEMENT"
          : "FRANCHISE";
        const prospect = {
          hotelName,
          ...attributeHotelGeography({ hotelCountry: country, sourceJurisdiction: "US" }),
          agreementType,
          expirationDate: exp,
          dateMeaning: meaning,
          verificationClass: exp ? "VERIFIED_EXPIRATION_CANDIDATE" : "SCHEDULE_ROW_NO_DATE",
          qualificationLevel: exp
            ? String(exp) >= asOf
              ? "ACTIVE_DATED"
              : "HISTORICAL_OR_SUPERSEDED"
            : "EVIDENCE_BACKED_COMMERCIAL_PROSPECT",
          countsToward1000: Boolean(exp ? String(exp) >= asOf : true) && !isNoisy(hotelName),
          commercialList:
            agreementType === "MANAGEMENT"
              ? "MANAGEMENT_CONTRACT_TARGETS"
              : "BRAND_CONVERSION_TARGETS",
          commercialThesis: exp
            ? "Name-free CMBS schedule extraction — hotel discovered from franchise/management column without prior hotel-name query"
            : "Name-free CMBS schedule row — property identified; date missing or unparsed",
          sourceUrl: d.url,
          sourceFamily: "SEC_CMBS",
          discoveryStrategies: d.strategies,
          discoveryMethod: "NAME_FREE_DOCUMENT_OR_PHRASE_FIRST",
          wave: "SOURCE_INTELLIGENCE_AUDIT",
          track: "TRACK_A",
          ownershipRequired: false,
        };
        if (prospect.countsToward1000 || prospect.qualificationLevel === "HISTORICAL_OR_SUPERSEDED") {
          netNew.push(prospect);
          row.netNew += 1;
        }
      }

      for (const s of d.strategies) {
        byStrategy[s] = byStrategy[s] || {
          docs: 0,
          extractableDocs: 0,
          findings: 0,
          netNewHotels: 0,
        };
        byStrategy[s].docs += 1;
        if (row.extractable) byStrategy[s].extractableDocs += 1;
        byStrategy[s].findings += row.findings;
        byStrategy[s].netNewHotels += row.netNew;
      }
    } catch (e) {
      row.error = String(e?.message || e).slice(0, 160);
    }
    docResults.push(row);
    await sleep(140);
  }

  const activeDated = netNew.filter((h) => h.qualificationLevel === "ACTIVE_DATED");
  const historical = netNew.filter((h) => h.qualificationLevel === "HISTORICAL_OR_SUPERSEDED");
  const undated = netNew.filter((h) => h.qualificationLevel === "EVIDENCE_BACKED_COMMERCIAL_PROSPECT");
  const toward1000 = netNew.filter((h) => h.countsToward1000);

  const metrics = {
    version: HIF_V6_SOURCE_INTELLIGENCE_AUDIT_V6,
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    heldOutDocsAttempted: docs.length,
    heldOutDocsOk: docResults.filter((d) => d.ok).length,
    extractableScheduleDocs: docResults.filter((d) => d.extractable).length,
    netNewHotels: netNew.length,
    activeDated: activeDated.length,
    historical: historical.length,
    undatedEvidenceBacked: undated.length,
    toward1000: toward1000.length,
    duplicatesVsPrior: duplicates.length,
    falsePositives: falsePositives.length,
    byStrategy,
    costPerIncrementalToward1000: toward1000.length ? 0 : null,
    nameFreeValidated: toward1000.length > 0 || activeDated.length > 0 || undated.length > 0,
  };

  writeJson(path.join(OUT, "HELD_OUT_DOC_RESULTS.json"), { docs: docResults });
  writeJson(path.join(OUT, "NET_NEW_FROM_NAME_FREE.json"), {
    count: netNew.length,
    toward1000: toward1000.length,
    hotels: netNew,
  });
  writeJson(path.join(OUT, "DUPLICATES.json"), { count: duplicates.length, rows: duplicates.slice(0, 100) });
  writeJson(path.join(OUT, "FALSE_POSITIVES.json"), {
    count: falsePositives.length,
    rows: falsePositives.slice(0, 80),
  });
  writeJson(path.join(OUT, "STRATEGY_COMPARISON.json"), { byStrategy, queryStats });
  writeJson(path.join(OUT, "AUDIT_METRICS.json"), metrics);

  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
