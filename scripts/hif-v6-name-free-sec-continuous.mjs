#!/usr/bin/env node
/**
 * HIF V6 — Continuous uncapped name-free SEC discovery (incremental).
 * Continues beyond the first scale-up wave; skips processed URLs/events;
 * yield checkpoints; never auto-labels combined columns as franchise.
 *
 * Usage:
 *   node scripts/hif-v6-name-free-sec-continuous.mjs --run
 *   node scripts/hif-v6-name-free-sec-continuous.mjs --run --max-docs=60 --pages=10 --start-page=0
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import { scoreCmbsScheduleYield } from "../lib/hotel-intelligence/sec-edgar/hif-v6-source-intelligence-audit-v6.js";
import { DATE_MEANING } from "../lib/hotel-intelligence/sec-edgar/contract-timing-types-v1.js";
import { attributeHotelGeography } from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-name-free-sec-continuous");
const RUN = process.argv.includes("--run");
const MAX_DOCS = Number((process.argv.find((a) => a.startsWith("--max-docs=")) || "").split("=")[1]) || 50;
const PAGE_SIZE = 25;
const MAX_PAGES = Number((process.argv.find((a) => a.startsWith("--pages=")) || "").split("=")[1]) || 10;
const START_PAGE = Number((process.argv.find((a) => a.startsWith("--start-page=")) || "").split("=")[1]) || 0;
const QUERY_FILTER = new Set(
  ((process.argv.find((a) => a.startsWith("--query-ids=")) || "").split("=")[1] || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
);
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const asOf = new Date().toISOString().slice(0, 10);
const WAVE = "NAME_FREE_SEC_CONTINUOUS";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};

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

function isNoisy(name) {
  const n = String(name || "").trim();
  if (n.length < 10) return true;
  if (/^(same-property|system-wide|portfolio|borrower|mortgaged property|total|aggregate)\b/i.test(n)) {
    return true;
  }
  if (/maturity|turity date|related mortgage loan|loan number|cut-?off date|appraised value/i.test(n)) {
    return true;
  }
  if (n.split(/\s+/).length > 14) return true;
  if (/^(the\s+)?(hilton|marriott|hyatt|embassy suites)\b/i.test(n) && n.split(/\s+/).length < 4) {
    return true;
  }
  return false;
}

function collectPrior() {
  const hotelKeys = new Set();
  const urls = new Set();
  const eventKeys = new Set();

  const paths = [
    "reports/sec-edgar-commercial-targets-v1/BRAND_CONVERSION_TARGETS.json",
    "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS.json",
    "reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json",
    "reports/sec-edgar-hif-v6-track-a-wave34/FINALIZED_OPPORTUNITY_INVENTORY.json",
    "reports/sec-edgar-hif-v6-inventory-reconciliation/TOWARD_1000_ROWS.json",
    "reports/sec-edgar-hif-v6-source-intelligence-audit/NET_NEW_FROM_NAME_FREE.json",
    "reports/sec-edgar-hif-v6-name-free-sec-scaleup/NET_NEW_EVENTS.json",
    "reports/sec-edgar-hif-v6-name-free-sec-continuous/NET_NEW_EVENTS.json",
    "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json",
    "reports/sec-edgar-hif-v6-canonical-inventory/TOWARD_1000.json",
    "reports/sec-edgar-hif-v6-canonical-inventory/ACTIVE_DATED.json",
  ];

  for (const rel of paths) {
    const j = load(rel);
    if (!j) continue;
    const rows = [
      ...(j.targets || []),
      ...(j.hotels || []),
      ...(j.rows || []),
      ...(j.datedActiveOpportunities || []),
      ...(j.evidenceBackedSignalsWithoutDates || []),
      ...(j.verified || []),
      ...(j.probable || []),
    ];
    for (const r of rows) {
      const n = r.hotelName || r.propertyName || r.hotel || r.name;
      const c = r.hotelCountry || r.country || "US";
      if (n) hotelKeys.add(hotelKey(n, c));
      const u = r.sourceUrl || r.source?.documentUrl;
      if (u) {
        urls.add(u);
        urls.add(u.replace(/\/0+(\d)/, "/$1"));
      }
      const d = r.expirationDate || r.primaryDate || r.verifiedEndDate;
      if (n && d) eventKeys.add(`${hotelKey(n, c)}|${d}`);
    }
  }

  for (const rel of [
    "reports/sec-edgar-hif-v6-source-intelligence-audit/HELD_OUT_DOC_RESULTS.json",
    "reports/sec-edgar-hif-v6-name-free-sec-scaleup/DOC_RESULTS.json",
    "reports/sec-edgar-hif-v6-name-free-sec-continuous/DOC_RESULTS.json",
  ]) {
    const j = load(rel);
    for (const d of j?.docs || []) {
      if (d.url) {
        urls.add(d.url);
        urls.add(d.url.replace(/\/0+(\d)/, "/$1"));
      }
    }
  }

  return { hotelKeys, urls, eventKeys };
}

function edgarUrlFromEftsHit(h) {
  const src = h._source || {};
  const adsh = String(src.adsh || "").replace(/-/g, "");
  const fileName = String(h._id || "").split(":").pop();
  const cikList = [...new Set((src.ciks || []).map((c) => String(c).replace(/^0+/, "")).filter(Boolean))];
  const cik = cikList[0] || "";
  return {
    id: h._id,
    url: `https://www.sec.gov/Archives/edgar/data/${cik}/${adsh}/${fileName}`,
    altUrls: cikList.slice(1).map((c) => `https://www.sec.gov/Archives/edgar/data/${c}/${adsh}/${fileName}`),
    form: src.form || (src.root_forms || [])[0] || null,
    fileDate: src.file_date,
  };
}

async function eftsPage(q, forms, from, size) {
  const p = new URLSearchParams({
    q,
    dateRange: "custom",
    startdt: "2015-01-01",
    enddt: "2026-11-21",
    from: String(from),
    size: String(size),
  });
  if (forms) p.set("forms", forms);
  const r = await fetch(`https://efts.sec.gov/LATEST/search-index?${p}`, {
    headers: { "User-Agent": UA, Accept: "application/json" },
  });
  if (!r.ok) return { ok: false, status: r.status, hits: [], total: 0 };
  const j = await r.json();
  const total = j.hits?.total?.value ?? j.hits?.total ?? 0;
  return { ok: true, hits: (j.hits?.hits || []).map(edgarUrlFromEftsHit), total };
}

async function fetchDoc(edgar, d) {
  for (const u of [d.url, ...(d.altUrls || [])]) {
    const res = await edgar.request(u, {
      accept: "text/html,*/*",
      maxAgeMs: 14 * 24 * 3600 * 1000,
    });
    const text = res.text || "";
    if (res.ok && text.length >= 5000 && !/<Code>NoSuchKey<\/Code>/i.test(text)) {
      return { ok: true, url: u, text };
    }
  }
  return { ok: false, url: d.url, text: "" };
}

function classifyCommercial(f, asOfDate) {
  const exp = f.primaryDate;
  const historical = exp && String(exp) < asOfDate;
  const agreementUnresolved =
    f.dateMeaning === DATE_MEANING.COMBINED_LICENSE_FRANCHISE_OPERATING_OR_MANAGEMENT ||
    f.agreementTypeResolved === false ||
    /UNKNOWN/i.test(String(f.agreementType || ""));

  return {
    expirationDate: exp,
    dateMeaning: f.dateMeaning,
    agreementType: f.agreementType,
    agreementTypeResolved: !agreementUnresolved,
    qualificationLevel: historical ? "HISTORICAL_OR_SUPERSEDED" : "ACTIVE_DATED",
    commercialThesis: historical
      ? "Verified historical agreement expiration — preserved, not an active opportunity"
      : agreementUnresolved
        ? "Verified hotel-level agreement expiration from CMBS schedule; agreement type unresolved (combined column) — evidence-backed prospect, not auto franchise/management"
        : "Verified hotel-level agreement expiration with resolved agreement type",
    commercialList: agreementUnresolved
      ? "CONTRACT_DATE_AGREEMENT_TYPE_UNRESOLVED"
      : /MANAGE/i.test(String(f.agreementType || ""))
        ? "MANAGEMENT_CONTRACT_TARGETS"
        : "BRAND_CONVERSION_TARGETS",
    countsToward1000: !historical && !isNoisy(f.hotelName),
    commerciallyAvailable: false,
    commerciallyAvailableNote:
      "Future agreement date is not automatic commercial availability — renewal/termination status unconfirmed",
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, maxDocs: MAX_DOCS, pages: MAX_PAGES }, null, 2));
    return;
  }

  const prior = collectPrior();
  const queries = [
    { id: "gold_related_license_424b2", q: '"Expiration/Termination of Related License"', forms: "424B2" },
    { id: "gold_related_license_424h", q: '"Expiration/Termination of Related License"', forms: "424H" },
    { id: "related_license_franchise_424b2", q: '"Related License/ Franchise Agreement"', forms: "424B2" },
    { id: "related_operating_mgmt_424b2", q: '"Operating Agreement or Management Agreement"', forms: "424B2" },
    { id: "gold_related_license_424b5", q: '"Expiration/Termination of Related License"', forms: "424B5" },
  ].filter((q) => QUERY_FILTER.size === 0 || QUERY_FILTER.has(q.id));

  const byUrl = new Map();
  const pageStats = [];
  for (const sq of queries) {
    let consecutiveExhausted = 0;
    for (let page = START_PAGE; page < START_PAGE + MAX_PAGES; page++) {
      const from = page * PAGE_SIZE;
      const res = await eftsPage(sq.q, sq.forms, from, PAGE_SIZE);
      pageStats.push({
        id: sq.id,
        page,
        from,
        ok: res.ok,
        hits: res.hits.length,
        total: res.total,
      });
      let newOnPage = 0;
      for (const h of res.hits) {
        const norm = h.url.replace(/\/0+(\d)/, "/$1");
        if (prior.urls.has(h.url) || prior.urls.has(norm) || byUrl.has(h.url)) continue;
        byUrl.set(h.url, { ...h, queryId: sq.id });
        newOnPage += 1;
      }
      pageStats[pageStats.length - 1].newOnPage = newOnPage;
      if (res.hits.length === 0 || newOnPage === 0) {
        pageStats[pageStats.length - 1].exhausted = true;
        consecutiveExhausted += 1;
        if (consecutiveExhausted >= 2) break;
      } else {
        consecutiveExhausted = 0;
      }
      await sleep(300);
    }
  }
  writeJson(path.join(OUT, "PAGINATION_STATS.json"), {
    pageStats,
    uniqueNewUrls: byUrl.size,
    priorUrlCount: prior.urls.size,
    priorEventCount: prior.eventKeys.size,
  });

  const docs = [...byUrl.values()].slice(0, MAX_DOCS);
  const edgar = createSecEdgarClient();
  const docResults = [];
  const netNew = [];
  const duplicates = [];
  const quarantined = [];
  const yieldCheckpoints = [];
  let cumulativeNetNew = 0;

  for (let i = 0; i < docs.length; i++) {
    const d = docs[i];
    const row = {
      url: d.url,
      queryId: d.queryId,
      form: d.form,
      fileDate: d.fileDate,
      ok: false,
      extractable: false,
      findings: 0,
      netNew: 0,
      error: null,
    };
    try {
      const fetched = await fetchDoc(edgar, d);
      if (!fetched.ok) {
        row.error = "FETCH_FAIL";
        docResults.push(row);
        continue;
      }
      row.url = fetched.url;
      row.ok = true;
      const ys = scoreCmbsScheduleYield(fetched.text, fetched.url);
      row.yieldScore = ys;
      row.extractable = ys.likelyExtractableSchedule;

      if (ys.reserveTrap && ys.headerHits === 0) {
        row.error = "RESERVE_TRIGGER_PROSE_ONLY";
        docResults.push(row);
        continue;
      }

      const extracted = extractCmbsPropertySchedule(
        { text: fetched.text, url: fetched.url },
        { sourceUrl: fetched.url, formType: d.form },
      );
      const findings = extracted.hotelContractFindings || [];
      row.findings = findings.length;

      for (const f of findings) {
        if (!f.hotelName || isNoisy(f.hotelName)) {
          quarantined.push({ hotelName: f.hotelName, url: fetched.url, reason: "NOISY_NAME" });
          continue;
        }
        const country = "US";
        const hk = hotelKey(f.hotelName, country);
        const exp = f.primaryDate;
        const ek = exp ? `${hk}|${exp}` : null;
        if (ek && prior.eventKeys.has(ek)) {
          duplicates.push({ hotelName: f.hotelName, expirationDate: exp, url: fetched.url, reason: "EVENT_DUP" });
          continue;
        }
        if (!exp && prior.hotelKeys.has(hk)) {
          duplicates.push({ hotelName: f.hotelName, url: fetched.url, reason: "HOTEL_DUP_NO_NEW_EVENT" });
          continue;
        }

        const commercial = classifyCommercial(f, asOf);
        if (ek) prior.eventKeys.add(ek);
        prior.hotelKeys.add(hk);

        netNew.push({
          hotelName: f.hotelName,
          ...attributeHotelGeography({ hotelCountry: country, sourceJurisdiction: "US" }),
          ...commercial,
          verificationClass: f.verificationClass,
          sourceUrl: fetched.url,
          sourceFamily: "SEC_CMBS",
          discoveryMethod: f.discoveryMethod || "NAME_FREE_CONTINUOUS",
          queryId: d.queryId,
          track: "TRACK_A",
          wave: WAVE,
          ownershipRequired: false,
          notes: f.notes || [],
        });
        row.netNew += 1;
        cumulativeNetNew += 1;
      }
    } catch (e) {
      row.error = String(e?.message || e).slice(0, 160);
    }
    docResults.push(row);

    if ((i + 1) % 5 === 0 || i === docs.length - 1) {
      const last5 = docResults.slice(-5);
      const yieldPerDoc = last5.reduce((s, x) => s + (x.netNew || 0), 0) / Math.max(1, last5.length);
      yieldCheckpoints.push({
        docsProcessed: i + 1,
        cumulativeNetNew,
        trailing5NetNewPerDoc: Number(yieldPerDoc.toFixed(2)),
        exhaustedSignal: yieldPerDoc < 0.2,
      });
    }
    await sleep(140);
  }

  const activeDated = netNew.filter((h) => h.qualificationLevel === "ACTIVE_DATED");
  const historical = netNew.filter((h) => h.qualificationLevel === "HISTORICAL_OR_SUPERSEDED");
  const unresolvedType = netNew.filter((h) => !h.agreementTypeResolved);
  const franchiseResolved = netNew.filter(
    (h) => h.agreementTypeResolved && /FRANCHISE/i.test(String(h.agreementType || "")),
  );
  const managementResolved = netNew.filter(
    (h) => h.agreementTypeResolved && /MANAGE/i.test(String(h.agreementType || "")),
  );

  const metrics = {
    wave: WAVE,
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    milestonePolicy: "MILESTONES_NOT_CEILINGS",
    documentsAttempted: docs.length,
    documentsOk: docResults.filter((d) => d.ok).length,
    extractableDocs: docResults.filter((d) => d.extractable).length,
    netNewHotelsEvents: netNew.length,
    activeDated: activeDated.length,
    historical: historical.length,
    toward1000Incremental: activeDated.filter((h) => h.countsToward1000).length,
    agreementTypeUnresolved: unresolvedType.length,
    verifiedFranchiseExpirationsResolved: franchiseResolved.length,
    verifiedManagementExpirationsResolved: managementResolved.length,
    duplicates: duplicates.length,
    quarantined: quarantined.length,
    yieldCheckpoints,
    byQuery: docs.reduce((acc, d) => {
      acc[d.queryId] = (acc[d.queryId] || 0) + 1;
      return acc;
    }, {}),
    evidenceExamples: activeDated.slice(0, 8).map((h) => ({
      hotelName: h.hotelName,
      expirationDate: h.expirationDate,
      agreementType: h.agreementType,
      commercialList: h.commercialList,
      sourceUrl: h.sourceUrl,
    })),
  };

  // Preserve prior cumulative NET_NEW_EVENTS.json; write this run to a wave-tagged file.
  const waveTag = `WAVE_${asOf.replace(/-/g, "")}_${docs.length}DOC`;
  const priorNet = load("reports/sec-edgar-hif-v6-name-free-sec-continuous/NET_NEW_EVENTS.json");
  const mergedHotels = [...(priorNet?.hotels || []), ...netNew];
  writeJson(path.join(OUT, "DOC_RESULTS.json"), { docs: docResults });
  writeJson(path.join(OUT, `NET_NEW_EVENTS_${waveTag}.json`), { count: netNew.length, hotels: netNew, waveTag });
  writeJson(path.join(OUT, "NET_NEW_EVENTS.json"), { count: mergedHotels.length, hotels: mergedHotels });
  writeJson(path.join(OUT, "DUPLICATES.json"), { count: duplicates.length, rows: duplicates.slice(0, 200) });
  writeJson(path.join(OUT, "QUARANTINED.json"), { count: quarantined.length, rows: quarantined.slice(0, 100) });
  writeJson(path.join(OUT, "CONTINUOUS_METRICS.json"), { ...metrics, waveTag });
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
