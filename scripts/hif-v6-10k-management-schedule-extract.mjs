#!/usr/bin/env node
/**
 * HIF V6 — Extract property-level management agreement dates from 10-K schedule candidates.
 * Does not auto-promote historical events to open MCT prospects without commercial thesis.
 *
 * Usage: node scripts/hif-v6-10k-management-schedule-extract.mjs --run
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

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-10k-management-schedules");
const RUN = process.argv.includes("--run");
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

function decode(html) {
  return String(html || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#160;/g, " ")
    .replace(/<br\s*\/?>/gi, " | ")
    .replace(/<\/t[dh]>/gi, " | ")
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
    const mm = String(mdy[1]).padStart(2, "0");
    const dd = String(mdy[2]).padStart(2, "0");
    return `${y}-${mm}-${dd}`;
  }
  const ymd = s.match(/\b(20\d{2}|19\d{2})-(\d{2})-(\d{2})\b/);
  if (ymd) return `${ymd[1]}-${ymd[2]}-${ymd[3]}`;
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
  const yearOnly = s.match(/\b(20\d{2}|19\d{2})\b/);
  if (yearOnly && /expir|termin|renew/i.test(s)) return `${yearOnly[1]}-12-31`;
  return null;
}

/**
 * Extract hotel + management agreement expiration/term rows from 10-K prose/tables.
 */
function extractManagementRows(text, meta) {
  const plain = decode(text);
  const findings = [];
  const seen = new Set();

  // Pattern: Hotel Name ... management agreement ... expires/terminates DATE
  const re =
    /([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z0-9][A-Za-z0-9&.'\-]*){1,8}\s+(?:Hotel|Inn|Resort|Suites|Lodge))\b[^.]{0,220}?(?:management agreement|hotel management agreement|\bHMA\b)[^.]{0,160}?(?:expir(?:es|ation|ing)?|terminat(?:es|ion|ing)?|renew(?:s|al|able)?|term(?:inates)?)\D{0,40}((?:\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+20\d{2}|20\d{2})/gi;

  let m;
  while ((m = re.exec(plain)) !== null) {
    const hotelName = m[1].replace(/\s+/g, " ").trim();
    const expirationDate = parseDate(m[2]);
    if (!hotelName || hotelName.length < 8 || !expirationDate) continue;
    const key = `${hotelKey(hotelName)}|${expirationDate}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const window = plain.slice(Math.max(0, m.index - 40), Math.min(plain.length, m.index + m[0].length + 80));
    const renewal = /automatic renewal|renewable|extension option|renewal option/i.test(window);
    const termination = /terminat|notice period|without cause|for cause/i.test(window);
    findings.push({
      hotelName,
      expirationDate,
      agreementType: "MANAGEMENT",
      agreementTypeResolved: true,
      dateMeaning: "MANAGEMENT_AGREEMENT_EXPIRATION",
      renewalLanguagePresent: renewal,
      terminationLanguagePresent: termination,
      sourceUrl: meta.url,
      sourceFamily: "SEC_10K",
      form: meta.form,
      company: meta.company,
      verificationClass: "EXTRACTED_10K_MGMT_SCHEDULE_CANDIDATE",
      evidenceSnippet: window.slice(0, 280),
      discoveryMethod: "TEN_K_MANAGEMENT_SCHEDULE",
      wave: "TEN_K_MGMT_EXTRACT",
    });
  }

  // Table-ish: "Property | Manager | Expiration"
  const lines = plain.split(/\s\|\s/).join("\n").split(/\n/);
  // Also scan for lines containing Hotel + year near "management"
  for (const chunk of plain.match(/.{0,80}Hotel.{0,120}/gi) || []) {
    if (!/management/i.test(chunk)) continue;
    const hotelMatch = chunk.match(
      /([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z0-9][A-Za-z0-9&.'\-]*){1,8}\s+(?:Hotel|Inn|Resort|Suites))\b/,
    );
    const date = parseDate(chunk);
    if (!hotelMatch || !date) continue;
    const hotelName = hotelMatch[1].replace(/\s+/g, " ").trim();
    const key = `${hotelKey(hotelName)}|${date}`;
    if (seen.has(key)) continue;
    seen.add(key);
    findings.push({
      hotelName,
      expirationDate: date,
      agreementType: "MANAGEMENT",
      agreementTypeResolved: true,
      dateMeaning: "MANAGEMENT_AGREEMENT_EXPIRATION",
      renewalLanguagePresent: /renew/i.test(chunk),
      terminationLanguagePresent: /terminat/i.test(chunk),
      sourceUrl: meta.url,
      sourceFamily: "SEC_10K",
      form: meta.form,
      company: meta.company,
      verificationClass: "EXTRACTED_10K_MGMT_NEAR_HOTEL_LINE",
      evidenceSnippet: chunk.slice(0, 280),
      discoveryMethod: "TEN_K_MANAGEMENT_SCHEDULE",
      wave: "TEN_K_MGMT_EXTRACT",
    });
  }

  void lines;
  return findings;
}

function collectPriorEventKeys() {
  const keys = new Set();
  for (const rel of [
    "reports/sec-edgar-hif-v6-canonical-inventory/TOWARD_1000.json",
    "reports/sec-edgar-hif-v6-canonical-inventory/ACTIVE_DATED.json",
    "reports/sec-edgar-hif-v6-canonical-inventory/HISTORICAL.json",
    "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS.json",
  ]) {
    const j = load(rel);
    for (const r of j?.rows || j?.hotels || j?.targets || []) {
      const n = r.hotelName || r.hotel || r.propertyName;
      const d = r.expirationDate || r.primaryDate || r.futureExpirationDate || r.verifiedEndDate;
      const c = r.hotelCountry || r.country || "US";
      if (n && d) keys.add(`${hotelKey(n, c)}|${d}`);
    }
  }
  return keys;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const family = load("reports/sec-edgar-hif-v6-sec-family-expansion/FAMILY_DOC_RESULTS.json");
  const candidates = (family?.docs || []).filter(
    (d) => d.ok && (d.classification === "MGMT_SCHEDULE_CANDIDATE" || d.signals?.managementAgreementTable),
  );
  if (!RUN) {
    console.log(JSON.stringify({ dryRun: true, candidates: candidates.length }, null, 2));
    return;
  }

  const edgar = createSecEdgarClient();
  const prior = collectPriorEventKeys();
  const docResults = [];
  const extracted = [];
  const incremental = [];
  const duplicates = [];

  for (const d of candidates) {
    const res = await edgar.request(d.url, { accept: "text/html,*/*", maxAgeMs: 7 * 24 * 3600 * 1000 });
    const row = { url: d.url, company: d.company, form: d.form, ok: false, findings: 0 };
    if (!res.ok || !(res.text || "").length) {
      row.error = "FETCH_FAIL";
      docResults.push(row);
      continue;
    }
    row.ok = true;
    const findings = extractManagementRows(res.text, d);
    row.findings = findings.length;
    for (const f of findings) {
      const country = "US"; // 10-K US issuers; physical country still verified when geo cues exist
      const geo = attributeHotelGeography({ hotelCountry: country, sourceJurisdiction: "US" });
      const ek = `${hotelKey(f.hotelName, country)}|${f.expirationDate}`;
      const historical = f.expirationDate < asOf;
      const rec = {
        ...f,
        ...geo,
        qualificationLevel: historical ? "HISTORICAL_OR_SUPERSEDED" : "ACTIVE_DATED",
        commercialList: historical
          ? null
          : "MANAGEMENT_CONTRACT_TARGETS",
        commercialThesis: historical
          ? "Historical 10-K management agreement expiration — preserved, not an open prospect"
          : "Property-level management agreement expiration extracted from 10-K schedule — management thesis candidate (renewal/availability unconfirmed)",
        commerciallyAvailable: false,
        countsToward1000: !historical,
        track: "TRACK_A",
      };
      extracted.push(rec);
      if (prior.has(ek)) {
        duplicates.push({ hotelName: f.hotelName, expirationDate: f.expirationDate, url: d.url });
      } else {
        prior.add(ek);
        incremental.push(rec);
      }
    }
    docResults.push(row);
    await sleep(140);
  }

  const metrics = {
    wave: "TEN_K_MGMT_EXTRACT",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    candidatesProcessed: candidates.length,
    documentsOk: docResults.filter((d) => d.ok).length,
    extractedRows: extracted.length,
    incrementalEvents: incremental.length,
    incrementalActiveDated: incremental.filter((h) => h.qualificationLevel === "ACTIVE_DATED").length,
    incrementalHistorical: incremental.filter((h) => h.qualificationLevel === "HISTORICAL_OR_SUPERSEDED")
      .length,
    duplicates: duplicates.length,
    autoOutreachReady: 0,
    note: "Management dates extracted; commercial availability / renewal status not assumed",
  };

  writeJson(path.join(OUT, "DOC_RESULTS.json"), { docs: docResults });
  writeJson(path.join(OUT, "EXTRACTED_ROWS.json"), { count: extracted.length, hotels: extracted });
  writeJson(path.join(OUT, "INCREMENTAL_EVENTS.json"), { count: incremental.length, hotels: incremental });
  writeJson(path.join(OUT, "DUPLICATES.json"), { count: duplicates.length, rows: duplicates });
  writeJson(path.join(OUT, "METRICS.json"), metrics);
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
