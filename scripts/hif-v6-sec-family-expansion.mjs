#!/usr/bin/env node
/**
 * HIF V6 — SEC document-family expansion probe (10-K / 8-K / EX-10).
 * Discovers candidate disclosures; extracts hotel+agreement signals without
 * auto-promoting historical conversions to open prospects.
 *
 * Usage: node scripts/hif-v6-sec-family-expansion.mjs --run [--max-docs=20]
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-sec-family-expansion");
const RUN = process.argv.includes("--run");
const MAX_DOCS = Number((process.argv.find((a) => a.startsWith("--max-docs=")) || "").split("=")[1]) || 18;
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const asOf = new Date().toISOString().slice(0, 10);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};

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

async function efts(q, forms, size = 15) {
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

function analyze(text, meta) {
  const t = String(text || "");
  const lower = t.toLowerCase();
  const signals = {
    managementAgreementTable:
      /management agreement.*(expir|term|renewal)|hotel management agreement.*table|expiration of.*management agreement/i.test(
        t,
      ),
    franchiseSchedule: /franchise agreement.*(expir|term)|franchise expiration date/i.test(t),
    conversionReflag:
      /reflag|re-brand|rebrand|conversion to|will operate as|discontinued.*(hilton|marriott|hyatt|ihg)/i.test(t),
    operatorChange: /new manager|management agreement.*terminat|appoint.*manager|operator replacement/i.test(t),
    exhibit10Hotel: /ex-?10|hotel management agreement|franchise agreement/i.test(t),
    hotelNameHits: [...t.matchAll(/\b([A-Z][A-Za-z0-9&.'-]+(?:\s+[A-Z][A-Za-z0-9&.'-]+){1,6}\s+Hotel)\b/g)]
      .slice(0, 12)
      .map((m) => m[1]),
  };
  const family = /10-?K/i.test(String(meta.form || ""))
    ? "10K"
    : /8-?K/i.test(String(meta.form || ""))
      ? "8K"
      : /EX-?10|ex10/i.test(String(meta.form || meta.url || ""))
        ? "EX10"
        : "OTHER";

  let classification = "RESEARCH_SIGNAL";
  let commercialList = null;
  if (signals.conversionReflag) {
    classification = "HISTORICAL_OR_ANNOUNCED_EVENT_REVIEW";
    commercialList = null; // do not auto-open prospect
  }
  if (signals.managementAgreementTable) {
    classification = "MGMT_SCHEDULE_CANDIDATE";
  }
  if (signals.franchiseSchedule) {
    classification = "FRANCHISE_SCHEDULE_CANDIDATE";
  }

  return {
    ...meta,
    family,
    signals,
    classification,
    commercialList,
    note:
      "Family expansion probe — hotel lexicon / schedule cues only; not auto-qualified commercial opportunities",
    textBytes: t.length,
    hasStrongHotelContractLexicon:
      signals.managementAgreementTable || signals.franchiseSchedule || signals.exhibit10Hotel,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ dryRun: true, maxDocs: MAX_DOCS }, null, 2));
    return;
  }

  const queries = [
    { id: "10k_mgmt_exp", q: '"management agreement" hotel expiration', forms: "10-K" },
    { id: "10k_franchise_sched", q: '"Franchise Expiration" hotel', forms: "10-K" },
    { id: "8k_reflag", q: "hotel (reflag OR rebrand OR conversion) franchise", forms: "8-K" },
    { id: "ex10_hma", q: '"Hotel Management Agreement"', forms: "EX-10" },
    { id: "ex10_franchise", q: '"Franchise Agreement" hotel', forms: "EX-10" },
  ];

  const byUrl = new Map();
  for (const q of queries) {
    const hits = await efts(q.q, q.forms, 12);
    for (const h of hits) {
      if (!byUrl.has(h.url)) byUrl.set(h.url, { ...h, queryId: q.id });
    }
    await sleep(280);
  }

  const docs = [...byUrl.values()].slice(0, MAX_DOCS);
  const edgar = createSecEdgarClient();
  const results = [];
  for (const d of docs) {
    const res = await edgar.request(d.url, { accept: "text/html,*/*", maxAgeMs: 7 * 24 * 3600 * 1000 });
    if (!res.ok || !(res.text || "").length) {
      results.push({ ...d, ok: false, error: "FETCH_FAIL" });
      continue;
    }
    results.push({ ok: true, ...analyze(res.text, d) });
    await sleep(140);
  }

  const metrics = {
    wave: "SEC_FAMILY_EXPANSION",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    documentsAttempted: docs.length,
    documentsOk: results.filter((r) => r.ok).length,
    byFamily: results.reduce((a, r) => {
      if (!r.ok) return a;
      a[r.family] = (a[r.family] || 0) + 1;
      return a;
    }, {}),
    managementScheduleCandidates: results.filter((r) => r.classification === "MGMT_SCHEDULE_CANDIDATE").length,
    franchiseScheduleCandidates: results.filter((r) => r.classification === "FRANCHISE_SCHEDULE_CANDIDATE").length,
    conversionReviewOnly: results.filter((r) => r.classification === "HISTORICAL_OR_ANNOUNCED_EVENT_REVIEW").length,
    autoQualifiedOpportunities: 0,
    note: "No automatic BCT/MCT promotion from this probe wave",
  };

  writeJson(path.join(OUT, "FAMILY_DOC_RESULTS.json"), { count: results.length, docs: results });
  writeJson(path.join(OUT, "FAMILY_METRICS.json"), metrics);
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
