#!/usr/bin/env node
/**
 * HIF V6 yield wave — brand conversion, reflagging, termination, operator replacement,
 * management appointments, transaction+conversion signals. Free EDGAR. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { attributeHotelGeography } from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-yield-wave");
const RUN = process.argv.includes("--run");
const LIMIT = Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 40;
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function hotelKey(name, country = "US") {
  return `${String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()}|${country}`;
}

function isNoisy(name) {
  const n = String(name || "").trim();
  if (n.length < 12 || n.length > 80) return true;
  if (/^(the\s+)?(particular|per|interim|includes|about|for|owned|pro forma|awarded|currency)\b/i.test(n)) {
    return true;
  }
  if (/\b(agreement|amendment|exhibit|system-wide|currency-neutral)\b/i.test(n)) return true;
  // brand-only
  if (/^(the\s+)?(hilton|marriott|hyatt|holiday inn|hampton inn|courtyard|residence inn|comfort (inn|suites)|la quinta)(\s+hotel|\s+inn|\s+suites)?$/i.test(n)) {
    return true;
  }
  return false;
}

function extractHotels(text, limit = 8) {
  const out = [];
  const re =
    /\b((?:The\s+)?[A-Z][A-Za-z0-9&'.\-]{1,35}(?:\s+[A-Z][A-Za-z0-9&'.\-]{1,25}){0,5}\s+(?:Hotel|Resort|Inn|Suites))\b/g;
  let m;
  while ((m = re.exec(text)) && out.length < limit) {
    const n = m[1].replace(/\s+/g, " ").trim();
    if (!isNoisy(n)) out.push(n);
  }
  return [...new Set(out)];
}

function classifyEvent(text) {
  const t = String(text || "").toLowerCase();
  if (/reflag|re-flag|brand conversion|convert(?:ed|ing)? to|flag change/.test(t)) {
    return {
      event: "BRAND_CONVERSION_OR_REFLAG",
      list: "BRAND_CONVERSION_TARGETS",
      agreementType: "FRANCHISE",
      thesis: "Property-specific brand conversion / reflagging disclosure",
    };
  }
  if (/franchise (?:terminat|non-?renew)|terminated the franchise|nonrenewal of (?:the )?franchise/.test(t)) {
    return {
      event: "FRANCHISE_TERMINATION_OR_NONRENEWAL",
      list: "BRAND_CONVERSION_TARGETS",
      agreementType: "FRANCHISE",
      thesis: "Franchise termination or nonrenewal — conversion research prospect",
    };
  }
  if (/operator (?:replace|replaced|replacement)|new (?:hotel )?operator|management compan(?:y|ies) (?:replaced|appointed|terminated)|appointed .* as manager|terminated .* management/.test(t)) {
    return {
      event: "OPERATOR_OR_MANAGEMENT_CHANGE",
      list: "MANAGEMENT_CONTRACT_TARGETS",
      agreementType: "MANAGEMENT",
      thesis: "Operator/management company appointment or departure — management-contract prospect",
    };
  }
  if (/acquired the .*hotel|sale of the .*hotel|disposed of the .*hotel/.test(t) && /(franchise|brand|manage|reflag|convert)/.test(t)) {
    return {
      event: "TRANSACTION_WITH_CONVERSION_OR_MGMT_IMPLICATION",
      list: /manage/.test(t) ? "MANAGEMENT_CONTRACT_TARGETS" : "BRAND_CONVERSION_TARGETS",
      agreementType: /manage/.test(t) ? "MANAGEMENT" : "FRANCHISE",
      thesis: "Hotel transaction with explicit franchise/management/reflag implication",
    };
  }
  if (/notice of (?:non-?renewal|termination)|renewal notice/.test(t) && /(franchise|management agreement)/.test(t)) {
    return {
      event: "CONTRACT_NOTICE",
      list: /management/.test(t) ? "MANAGEMENT_CONTRACT_TARGETS" : "BRAND_CONVERSION_TARGETS",
      agreementType: /management/.test(t) ? "MANAGEMENT" : "FRANCHISE",
      thesis: "Contractual renewal/termination notice for franchise or management agreement",
    };
  }
  return null;
}

async function efts(q, forms, size = 25) {
  const p = new URLSearchParams({
    q,
    dateRange: "custom",
    startdt: "2018-01-01",
    enddt: "2026-11-21",
    from: "0",
    size: String(size),
  });
  if (forms) p.set("forms", forms);
  const r = await fetch(`https://efts.sec.gov/LATEST/search-index?${p}`, {
    headers: { "User-Agent": UA, Accept: "application/json" },
  });
  const j = await r.json();
  return (j.hits?.hits || []).map((h) => {
    const src = h._source || {};
    const adsh = String(src.adsh || "").replace(/-/g, "");
    const cik = String((src.ciks || [])[0] || "").replace(/^0+/, "");
    const fileName = String(h._id || "").split(":").pop();
    return {
      id: h._id,
      url: `https://www.sec.gov/Archives/edgar/data/${cik}/${adsh}/${fileName}`,
      q,
      fileDate: src.file_date,
    };
  });
}

function loadPrior() {
  const keys = new Set();
  const paths = [
    "reports/sec-edgar-commercial-targets-v1/BRAND_CONVERSION_TARGETS.json",
    "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS.json",
    "reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json",
    "reports/sec-edgar-hif-v6-track-a-wave34/FINALIZED_OPPORTUNITY_INVENTORY.json",
    "reports/sec-edgar-hif-v6-inventory-reconciliation/TOWARD_1000_ROWS.json",
  ];
  for (const rel of paths) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    const j = JSON.parse(fs.readFileSync(fp, "utf8"));
    const rows = [
      ...(j.targets || []),
      ...(j.hotels || []),
      ...(j.rows || []),
      ...(j.datedActiveOpportunities || []),
      ...(j.evidenceBackedSignalsWithoutDates || []),
    ];
    for (const r of rows) {
      const n = r.hotelName || r.propertyName || r.name;
      if (n) keys.add(hotelKey(n, r.hotelCountry || "US"));
    }
  }
  return keys;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, limit: LIMIT }, null, 2));
    return;
  }

  const queries = [
    ['"reflag" OR "re-flagged" OR "brand conversion" hotel', "8-K"],
    ['"franchise" (termination OR nonrenewal OR "non-renewal") hotel', "8-K"],
    ['"management company" (appointed OR terminated OR replaced) hotel', "8-K"],
    ['"new operator" OR "operator replacement" hotel', "8-K"],
    ['"notice of termination" ("franchise agreement" OR "management agreement")', null],
    ['hotel (acquired OR acquisition) (reflag OR franchise OR "management agreement")', "8-K"],
  ];

  const byUrl = new Map();
  const queryStats = [];
  for (const [q, forms] of queries) {
    const hits = await efts(q, forms, 30);
    queryStats.push({ q, forms, hits: hits.length });
    for (const h of hits) if (!byUrl.has(h.url)) byUrl.set(h.url, h);
    await sleep(250);
  }

  const docs = [...byUrl.values()].slice(0, LIMIT);
  writeJson(path.join(OUT, "YIELD_QUERY_STATS.json"), { queryStats, docCount: docs.length });
  const edgar = createSecEdgarClient();
  const prior = loadPrior();
  const prospects = [];
  const rejected = [];
  const docResults = [];

  for (const d of docs) {
    const row = { url: d.url, q: d.q, ok: false, netNew: 0, error: null };
    try {
      const res = await edgar.request(d.url, {
        accept: "text/html,*/*",
        maxAgeMs: 14 * 24 * 3600 * 1000,
      });
      if (!res.ok) {
        row.error = `HTTP_${res.status || res.error}`;
        docResults.push(row);
        continue;
      }
      row.ok = true;
      const text = res.text || "";
      const event = classifyEvent(text);
      if (!event) {
        rejected.push({ url: d.url, reason: "NO_QUALIFYING_COMMERCIAL_EVENT" });
        docResults.push(row);
        await sleep(120);
        continue;
      }
      // Require renovation/transaction alone NOT enough — classifyEvent already requires franchise/mgmt/reflag language for transactions
      const hotels = extractHotels(text, 6);
      if (!hotels.length) {
        rejected.push({ url: d.url, reason: "NO_PROPERTY_SPECIFIC_HOTEL_NAME", event: event.event });
        docResults.push(row);
        await sleep(120);
        continue;
      }
      for (const hotelName of hotels) {
        const key = hotelKey(hotelName, "US");
        if (prior.has(key)) {
          rejected.push({ hotelName, url: d.url, reason: "DUPLICATE_PRIOR_INVENTORY" });
          continue;
        }
        prior.add(key);
        prospects.push({
          hotelName,
          ...attributeHotelGeography({ hotelCountry: "US", sourceJurisdiction: "US" }),
          agreementType: event.agreementType,
          expirationDate: null,
          eventType: event.event,
          commercialList: event.list,
          commercialThesis: event.thesis,
          qualificationLevel: "EVIDENCE_BACKED_COMMERCIAL_PROSPECT",
          verificationClass: "COMMERCIAL_EVENT_SIGNAL",
          countsToward1000: true,
          countsAsActiveOpportunity: false,
          countsAsEvidenceBackedProspect: true,
          ownershipRequired: false,
          ownershipStatus: "DEFERRED_OR_INLINE_IF_PRESENT",
          sourceUrl: d.url,
          sourceFamily: "SEC_8K_OR_DISCLOSURE",
          track: "TRACK_A",
          wave: "YIELD_CONVERSION_OPERATOR",
        });
        row.netNew += 1;
      }
    } catch (e) {
      row.error = String(e?.message || e).slice(0, 120);
    }
    docResults.push(row);
    await sleep(120);
  }

  const metrics = {
    track: "TRACK_A",
    wave: "YIELD_CONVERSION_OPERATOR",
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    documentsAttempted: docs.length,
    documentsOk: docResults.filter((d) => d.ok).length,
    netNewEvidenceBackedProspects: prospects.length,
    brandConversionTargets: prospects.filter((p) => p.commercialList === "BRAND_CONVERSION_TARGETS").length,
    managementContractTargets: prospects.filter((p) => p.commercialList === "MANAGEMENT_CONTRACT_TARGETS")
      .length,
    rejected: rejected.length,
    byEventType: prospects.reduce((acc, p) => {
      acc[p.eventType] = (acc[p.eventType] || 0) + 1;
      return acc;
    }, {}),
  };

  writeJson(path.join(OUT, "YIELD_DOC_RESULTS.json"), { docs: docResults });
  writeJson(path.join(OUT, "YIELD_PROSPECTS.json"), { count: prospects.length, hotels: prospects });
  writeJson(path.join(OUT, "YIELD_REJECTED.json"), { count: rejected.length, rows: rejected.slice(0, 100) });
  writeJson(path.join(OUT, "YIELD_METRICS.json"), metrics);
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
