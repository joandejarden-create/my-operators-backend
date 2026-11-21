#!/usr/bin/env node
/**
 * HIF V6 — Management agreement discovery pass (EX-10 / 8-K / 10-K).
 * Extracts hotel-level management signals; does not auto-qualify open MCT
 * from historical events alone.
 *
 * Usage: node scripts/hif-v6-management-discovery-pass.mjs --run [--max-docs=25]
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

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-management-discovery");
const RUN = process.argv.includes("--run");
const MAX_DOCS = Number((process.argv.find((a) => a.startsWith("--max-docs=")) || "").split("=")[1]) || 25;
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const asOf = new Date().toISOString().slice(0, 10);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};

function load(rel) {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

function hotelKey(name, country = "US") {
  return `${String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()}|${String(country || "US").toUpperCase()}`;
}

function edgarUrlFromEftsHit(h) {
  const src = h._source || {};
  const adsh = String(src.adsh || "").replace(/-/g, "");
  const fileName = String(h._id || "").split(":").pop();
  const cik = String((src.ciks || [])[0] || "").replace(/^0+/, "");
  return {
    id: h._id,
    url: `https://www.sec.gov/Archives/edgar/data/${cik}/${adsh}/${fileName}`,
    form: src.form || (src.root_forms || [])[0] || null,
    fileDate: src.file_date,
    company: (src.display_names || [])[0] || null,
  };
}

async function efts(q, forms, size = 12) {
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
  if (!r.ok) return [];
  const j = await r.json();
  return (j.hits?.hits || []).map(edgarUrlFromEftsHit);
}

function decode(html) {
  return String(html || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/<br\s*\/?>/gi, " | ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseDate(raw) {
  const s = String(raw || "").trim();
  const mdy = s.match(/\b(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\b/);
  if (mdy) {
    let y = Number(mdy[3]);
    if (y < 100) y += y >= 70 ? 1900 : 2000;
    return `${y}-${String(mdy[1]).padStart(2, "0")}-${String(mdy[2]).padStart(2, "0")}`;
  }
  const mon = s.match(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2}|19\d{2})\b/i,
  );
  if (mon) {
    const months = {
      january: "01",
      february: "02",
      march: "03",
      april: "04",
      may: "05",
      june: "06",
      july: "07",
      august: "08",
      september: "09",
      october: "10",
      november: "11",
      december: "12",
    };
    return `${mon[3]}-${months[mon[1].toLowerCase()]}-${String(mon[2]).padStart(2, "0")}`;
  }
  return null;
}

function extractFromDoc(text, meta) {
  const plain = decode(text);
  const findings = [];
  const seen = new Set();

  const re =
    /([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z0-9][A-Za-z0-9&.'\-]*){1,8}\s+(?:Hotel|Inn|Resort|Suites|Lodge))\b[^.]{0,240}?(?:hotel management agreement|management agreement|\bHMA\b|managed by|operator)[^.]{0,180}?(?:expir(?:es|ation|ing)?|terminat(?:es|ion)?|renew(?:s|al)?|commenc(?:es|ement)?|effective)\D{0,40}((?:\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+20\d{2})/gi;

  let m;
  while ((m = re.exec(plain)) !== null) {
    const hotelName = m[1].replace(/\s+/g, " ").trim();
    const expirationDate = parseDate(m[2]);
    if (!hotelName || !expirationDate || hotelName.length < 8) continue;
    const key = `${hotelKey(hotelName)}|${expirationDate}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const window = plain.slice(Math.max(0, m.index), Math.min(plain.length, m.index + m[0].length + 100));
    const manager =
      (window.match(
        /\b((?:Aimbridge|Remington|Highgate|White Lodging|Interstate|Hersha|Pyramid|CoralTree|Coral Tree|Destination|Sage|Pacific Hospitality|Sherman)[A-Za-z &]*)\b/i,
      ) || [])[1] || null;
    findings.push({
      hotelName,
      expirationDate,
      managerName: manager,
      agreementType: "MANAGEMENT",
      agreementTypeResolved: true,
      dateMeaning: "MANAGEMENT_AGREEMENT_EVENT",
      renewalLanguagePresent: /renew/i.test(window),
      terminationLanguagePresent: /terminat/i.test(window),
      sourceUrl: meta.url,
      sourceFamily: /8-?K/i.test(String(meta.form || ""))
        ? "SEC_8K"
        : /EX-?10/i.test(String(meta.form || meta.url || ""))
          ? "SEC_EX10"
          : "SEC_10K",
      form: meta.form,
      company: meta.company,
      evidenceSnippet: window.slice(0, 280),
      discoveryMethod: "MANAGEMENT_DISCOVERY_PASS",
      wave: "MGMT_DISCOVERY",
      verificationClass: "EXTRACTED_MGMT_EVENT_CANDIDATE",
    });
  }

  // 8-K operator replacement / appointment without hard date → undated signal
  if (/8-?K/i.test(String(meta.form || "")) && /appoint.*(?:manager|operator)|new manager|management agreement.*terminat|operator replacement/i.test(plain)) {
    for (const hm of plain.match(
      /\b([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z0-9][A-Za-z0-9&.'\-]*){1,6}\s+(?:Hotel|Inn|Resort|Suites))\b/g,
    ) || []) {
      const hotelName = hm.replace(/\s+/g, " ").trim();
      if (hotelName.length < 10) continue;
      const key = `${hotelKey(hotelName)}|NODATE`;
      if (seen.has(key)) continue;
      seen.add(key);
      findings.push({
        hotelName,
        expirationDate: null,
        agreementType: "MANAGEMENT",
        agreementTypeResolved: true,
        dateMeaning: "MANAGEMENT_OPERATOR_CHANGE_EVENT",
        sourceUrl: meta.url,
        sourceFamily: "SEC_8K",
        form: meta.form,
        company: meta.company,
        evidenceSnippet: plain.slice(0, 200),
        discoveryMethod: "MANAGEMENT_DISCOVERY_PASS",
        wave: "MGMT_DISCOVERY",
        verificationClass: "EIGHT_K_OPERATOR_EVENT_UNDATED",
        commercialThesis:
          "8-K operator/management event — historical or announced; not automatically an open MCT prospect",
      });
    }
  }

  return findings;
}

function priorKeys() {
  const keys = new Set();
  for (const rel of [
    "reports/sec-edgar-hif-v6-canonical-inventory/TOWARD_1000.json",
    "reports/sec-edgar-hif-v6-canonical-inventory/HISTORICAL.json",
    "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS.json",
    "reports/sec-edgar-hif-v6-10k-management-schedules/INCREMENTAL_EVENTS.json",
  ]) {
    const j = load(rel);
    for (const r of j?.rows || j?.hotels || j?.targets || []) {
      const n = r.hotelName || r.hotel;
      const d = r.expirationDate || r.primaryDate || r.futureExpirationDate || "NODATE";
      const c = r.hotelCountry || r.country || "US";
      if (n) keys.add(`${hotelKey(n, c)}|${d || "NODATE"}`);
    }
  }
  return keys;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ dryRun: true, maxDocs: MAX_DOCS }, null, 2));
    return;
  }

  const queries = [
    { id: "ex10_hma", q: '"Hotel Management Agreement"', forms: "EX-10" },
    { id: "ex10_mgmt_amend", q: '"management agreement" hotel (amendment OR termination OR renewal)', forms: "EX-10" },
    { id: "8k_operator", q: "hotel (manager OR operator) (appoint OR terminate OR replace OR engagement)", forms: "8-K" },
    { id: "10k_mgmt_term", q: '"management agreement" (expiration OR terminates OR renewal) hotel', forms: "10-K" },
  ];

  const byUrl = new Map();
  for (const q of queries) {
    const hits = await efts(q.q, q.forms, 10);
    for (const h of hits) {
      if (!byUrl.has(h.url)) byUrl.set(h.url, { ...h, queryId: q.id });
    }
    await sleep(280);
  }

  const docs = [...byUrl.values()].slice(0, MAX_DOCS);
  const edgar = createSecEdgarClient();
  const prior = priorKeys();
  const extracted = [];
  const incremental = [];
  const duplicates = [];
  const docResults = [];

  for (const d of docs) {
    const res = await edgar.request(d.url, { accept: "text/html,*/*", maxAgeMs: 7 * 24 * 3600 * 1000 });
    const row = { url: d.url, form: d.form, queryId: d.queryId, ok: false, findings: 0 };
    if (!res.ok || !(res.text || "").length) {
      row.error = "FETCH_FAIL";
      docResults.push(row);
      continue;
    }
    row.ok = true;
    const findings = extractFromDoc(res.text, d);
    row.findings = findings.length;
    for (const f of findings) {
      const country = "US";
      const geo = attributeHotelGeography({ hotelCountry: country, sourceJurisdiction: "US" });
      const ek = `${hotelKey(f.hotelName, country)}|${f.expirationDate || "NODATE"}`;
      const historical = f.expirationDate && f.expirationDate < asOf;
      const undated = !f.expirationDate;
      const rec = {
        ...f,
        ...geo,
        qualificationLevel: historical
          ? "HISTORICAL_OR_SUPERSEDED"
          : undated
            ? "ACTIVE_UNDATED_EVIDENCE_BACKED"
            : "ACTIVE_DATED",
        commercialList:
          historical || undated
            ? null
            : "MANAGEMENT_CONTRACT_TARGETS",
        commerciallyAvailable: false,
        countsToward1000: !historical,
        track: "TRACK_A",
        commercialThesis:
          f.commercialThesis ||
          (historical
            ? "Historical management agreement event — preserved"
            : undated
              ? "Management/operator event without verified expiration — undated research signal"
              : "Dated management agreement event — MCT candidate; renewal/availability unconfirmed"),
      };
      extracted.push(rec);
      if (prior.has(ek)) duplicates.push({ hotelName: f.hotelName, expirationDate: f.expirationDate, url: d.url });
      else {
        prior.add(ek);
        incremental.push(rec);
      }
    }
    docResults.push(row);
    await sleep(140);
  }

  const metrics = {
    wave: "MGMT_DISCOVERY",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    documentsAttempted: docs.length,
    documentsOk: docResults.filter((d) => d.ok).length,
    extractedRows: extracted.length,
    incrementalEvents: incremental.length,
    newVerifiedManagementContractualDates: incremental.filter((h) => h.expirationDate && h.agreementType === "MANAGEMENT")
      .length,
    newManagementEventEvidence: incremental.length,
    newManagementOpportunityCandidates: incremental.filter(
      (h) => h.qualificationLevel === "ACTIVE_DATED" && h.commercialList === "MANAGEMENT_CONTRACT_TARGETS",
    ).length,
    newQualifiedMct: 0, // never auto-promote without commercial thesis gate
    historicalManagementEvents: incremental.filter((h) => h.qualificationLevel === "HISTORICAL_OR_SUPERSEDED")
      .length,
    duplicates: duplicates.length,
    bySourceFamily: incremental.reduce((a, h) => {
      a[h.sourceFamily] = (a[h.sourceFamily] || 0) + 1;
      return a;
    }, {}),
  };

  writeJson(path.join(OUT, "DOC_RESULTS.json"), { docs: docResults });
  writeJson(path.join(OUT, "EXTRACTED.json"), { count: extracted.length, hotels: extracted });
  writeJson(path.join(OUT, "INCREMENTAL.json"), { count: incremental.length, hotels: incremental });
  writeJson(path.join(OUT, "METRICS.json"), metrics);
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
