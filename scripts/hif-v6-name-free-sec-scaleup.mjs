#!/usr/bin/env node
/**
 * HIF V6 — Name-free SEC CMBS scale-up (424B2 / 424H).
 * Paginates EFTS gold-column queries, extracts with CMBS schedule engine,
 * checkpoints yield, and writes net-new hotels for canonical reconcile.
 * CUSTOMER_BLOCKED · $0 · never auto-labels combined columns as franchise.
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

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-name-free-sec-scaleup");
const RUN = process.argv.includes("--run");
const MAX_DOCS = Number((process.argv.find((a) => a.startsWith("--max-docs=")) || "").split("=")[1]) || 40;
const PAGE_SIZE = 25;
const MAX_PAGES = Number((process.argv.find((a) => a.startsWith("--pages=")) || "").split("=")[1]) || 6;
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const asOf = new Date().toISOString().slice(0, 10);
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
    "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json",
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
      if (u) urls.add(u.replace(/\/0+(\d)/, "/$1"));
      const d = r.expirationDate || r.primaryDate || r.verifiedEndDate;
      if (n && d) eventKeys.add(`${hotelKey(n, c)}|${d}`);
    }
  }

  // Prior audit held-out docs
  const held = load("reports/sec-edgar-hif-v6-source-intelligence-audit/HELD_OUT_DOC_RESULTS.json");
  for (const d of held?.docs || []) if (d.url) urls.add(d.url);

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
    // Future date ≠ commercially available; keep as dated research opportunity
    commerciallyAvailable: false,
    commerciallyAvailableNote:
      "Future agreement date is not automatic commercial availability — renewal/termination status unconfirmed",
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, maxDocs: MAX_DOCS }, null, 2));
    return;
  }

  const prior = collectPrior();
  const queries = [
    { id: "gold_related_license_424b2", q: '"Expiration/Termination of Related License"', forms: "424B2" },
    { id: "gold_related_license_424h", q: '"Expiration/Termination of Related License"', forms: "424H" },
    { id: "related_license_franchise_424b2", q: '"Related License/ Franchise Agreement"', forms: "424B2" },
  ];

  const byUrl = new Map();
  const pageStats = [];
  for (const sq of queries) {
    for (let page = 0; page < MAX_PAGES; page++) {
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
      // Exhaustion checkpoint: no new URLs on this page
      if (res.hits.length === 0 || newOnPage === 0) {
        pageStats[pageStats.length - 1].exhausted = true;
        break;
      }
      await sleep(300);
    }
  }
  writeJson(path.join(OUT, "PAGINATION_STATS.json"), { pageStats, uniqueNewUrls: byUrl.size });

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

      // Skip reserve/trigger-only prose
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
          discoveryMethod: f.discoveryMethod || "NAME_FREE_SCALEUP",
          queryId: d.queryId,
          track: "TRACK_A",
          wave: "NAME_FREE_SEC_SCALEUP",
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
    wave: "NAME_FREE_SEC_SCALEUP",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    pr519Merged: true,
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
  };

  writeJson(path.join(OUT, "DOC_RESULTS.json"), { docs: docResults });
  writeJson(path.join(OUT, "NET_NEW_EVENTS.json"), { count: netNew.length, hotels: netNew });
  writeJson(path.join(OUT, "DUPLICATES.json"), { count: duplicates.length, rows: duplicates.slice(0, 200) });
  writeJson(path.join(OUT, "QUARANTINED.json"), { count: quarantined.length, rows: quarantined.slice(0, 100) });
  writeJson(path.join(OUT, "SCALEUP_METRICS.json"), metrics);
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
