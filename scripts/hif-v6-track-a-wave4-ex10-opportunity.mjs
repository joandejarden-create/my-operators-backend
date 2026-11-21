#!/usr/bin/env node
/**
 * HIF V6 Track A Wave 4 supplement — scale EX-10 hotel agreement harvest for opportunities.
 * Free EDGAR only. Ownership deferred. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { harvestExhibitDocument } from "../lib/hotel-intelligence/sec-edgar/ex10-contract-harvest-v1.js";
import { attributeHotelGeography } from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-wave34");
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

async function efts(q, forms, size = 50) {
  const p = new URLSearchParams({
    q,
    dateRange: "custom",
    startdt: "2012-01-01",
    enddt: "2026-11-21",
    from: "0",
    size: String(size),
  });
  // NOTE: forms=EX-10 returns 0 hits on EFTS; exhibits are filed under 8-K/10-K/10-Q with file_type EX-10.*
  if (forms) p.set("forms", forms);
  const r = await fetch(`https://efts.sec.gov/LATEST/search-index?${p}`, {
    headers: { "User-Agent": UA, Accept: "application/json" },
  });
  const j = await r.json();
  return (j.hits?.hits || [])
    .map((h) => {
      const src = h._source || {};
      const adsh = String(src.adsh || "").replace(/-/g, "");
      const cik = String((src.ciks || [])[0] || "").replace(/^0+/, "");
      const fileName = String(h._id || "").split(":").pop();
      const fileType = String(src.file_type || "");
      return {
        id: h._id,
        url: `https://www.sec.gov/Archives/edgar/data/${cik}/${adsh}/${fileName}`,
        fileDate: src.file_date,
        fileType,
        form: src.form,
        q,
      };
    })
    .filter((h) => /ex-?\s*10/i.test(h.fileType) || /ex-?10|dex10|exhibit/i.test(h.id || ""));
}

async function main() {
  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, limit: LIMIT }, null, 2));
    return;
  }
  const queries = [
    ['"Hotel Management Agreement"', "10-K"],
    ['"Hotel Management Agreement"', "8-K"],
    ['"Hotel Management Agreement"', "10-Q"],
    ["franchise agreement hotel", "8-K"],
    ['"franchise agreement" hotel', "10-K"],
  ];
  const byUrl = new Map();
  for (const [q, forms] of queries) {
    for (const h of await efts(q, forms, 40)) {
      if (!byUrl.has(h.url)) byUrl.set(h.url, h);
    }
    await sleep(250);
  }

  // Prior wave1 queue
  const wave1 = path.join(ROOT, "reports/sec-edgar-hif-v6-wave1-ex10/WAVE1_EXHIBIT_QUEUE.json");
  if (fs.existsSync(wave1)) {
    const j = JSON.parse(fs.readFileSync(wave1, "utf8"));
    for (const item of j.items || j.exhibits || j.queue || []) {
      const url = item.url || item.documentUrl || item.archiveUrl;
      if (url && !byUrl.has(url)) byUrl.set(url, { url, q: "WAVE1_QUEUE", id: item.id || url });
    }
  }

  const docs = [...byUrl.values()].slice(0, LIMIT);
  writeJson(path.join(OUT, "WAVE4_EX10_URLS.json"), { count: docs.length, docs });

  const prior = new Set();
  for (const rel of [
    "reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json",
    "reports/sec-edgar-hif-v6-track-a-wave34/OPPORTUNITY_INVENTORY.json",
    "reports/sec-edgar-hif-v6-track-a-wave34/WAVE3_NET_NEW.json",
    "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json",
  ]) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    const j = JSON.parse(fs.readFileSync(fp, "utf8"));
    for (const r of [
      ...(j.hotels || []),
      ...(j.opportunities || []),
      ...(j.verified || []),
      ...(j.probable || []),
    ]) {
      const n = r.hotelName || r.propertyName;
      if (n) prior.add(hotelKey(n, r.hotelCountry || "US"));
    }
  }

  const edgar = createSecEdgarClient();
  const asOf = new Date().toISOString().slice(0, 10);
  const results = [];
  const netNewActive = [];
  const netNewHistorical = [];
  const signals = [];
  let franchise = 0;
  let management = 0;

  for (const d of docs) {
    const row = { url: d.url, ok: false, error: null, harvest: null };
    try {
      const res = await edgar.request(d.url, {
        accept: "text/html,*/*",
        maxAgeMs: 30 * 24 * 3600 * 1000,
      });
      if (!res.ok) {
        row.error = `HTTP_${res.status || res.error}`;
        results.push(row);
        continue;
      }
      row.ok = true;
      const harvest = harvestExhibitDocument(
        { html: res.text, text: res.text, url: d.url },
        { sourceUrl: d.url },
      );
      const plain = String(res.text || "")
        .replace(/<[^>]+>/g, " ")
        .replace(/\s+/g, " ");
      const hotelNames = [];
      const nameRe =
        /\b((?:The\s+)?[A-Z][A-Za-z0-9&'.\-]{1,35}(?:\s+[A-Z][A-Za-z0-9&'.\-]{1,25}){0,4}\s+(?:Hotel|Resort|Inn|Suites))\b/g;
      let nm;
      while ((nm = nameRe.exec(plain)) && hotelNames.length < 8) {
        const n = nm[1].replace(/\s+/g, " ").trim();
        if (n.length >= 8 && !/Management Agreement|Franchise Agreement/i.test(n)) {
          hotelNames.push(n);
        }
      }
      const terms = harvest.extracted || {};
      const exp =
        terms.expirationDate ||
        terms.scheduledExpirationDate ||
        terms.termEndDate ||
        terms.initialTermEndDate ||
        null;
      row.harvest = {
        instrumentClass: harvest.instrumentClass,
        scopeClass: harvest.scopeClass,
        agreementType: harvest.agreementType,
        disclosedFieldCount: harvest.disclosedFieldCount,
        hotelNames,
        exp,
        gates: harvest.gates,
      };
      const agType = /manage/i.test(String(harvest.agreementType || ""))
        ? "MANAGEMENT"
        : /franchise|license/i.test(String(harvest.agreementType || ""))
          ? "FRANCHISE"
          : /management/i.test(d.q || "")
            ? "MANAGEMENT"
            : "FRANCHISE";
      // Portfolio-level form/amendments without hotel attribution are research candidates only
      const namedOk =
        harvest.scopeClass === "NAMED_HOTEL" ||
        (hotelNames.length > 0 && harvest.hotelAttributionAllowed !== false && exp);
      if (!namedOk && hotelNames.length === 0) {
        results.push(row);
        await sleep(150);
        continue;
      }
      for (const hotelName of [...new Set(hotelNames)].slice(0, 5)) {
        const key = hotelKey(hotelName, "US");
        const historical = exp && String(exp) < asOf;
        const hasVerifiedTerms = Boolean(exp && harvest.disclosedFieldCount >= 1);
        const opp = {
          hotelName,
          ...attributeHotelGeography({ hotelCountry: "US", sourceJurisdiction: "US" }),
          agreementType: agType,
          expirationDate: exp,
          verificationClass: hasVerifiedTerms
            ? "EX10_TERM_EXTRACTED"
            : "EX10_HOTEL_NAMED_NO_VERIFIED_DATE",
          sourceUrl: d.url,
          sourceFamily: "EX10",
          track: "TRACK_A",
          wave: "WAVE4_EX10",
          commercialList:
            agType === "MANAGEMENT" ? "MANAGEMENT_CONTRACT_TARGETS" : "BRAND_CONVERSION_TARGETS",
          trackingBucket: hasVerifiedTerms
            ? historical
              ? "HISTORICAL_CLOSED_SUPERSEDED"
              : "QUALIFIED_ACTIONABLE_OPPORTUNITIES"
            : "EVIDENCE_BACKED_COMMERCIAL_SIGNALS_WITHOUT_DATES",
          countsAsActiveOpportunity: Boolean(hasVerifiedTerms && !historical),
          countsAsEvidenceBackedSignal: !hasVerifiedTerms,
          ownershipRequired: false,
          ownershipStatus: "DEFERRED",
          instrumentClass: harvest.instrumentClass,
          scopeClass: harvest.scopeClass,
        };
        if (prior.has(key)) continue;
        prior.add(key);
        if (opp.countsAsActiveOpportunity) {
          netNewActive.push(opp);
          if (agType === "FRANCHISE") franchise += 1;
          else management += 1;
        } else if (historical) netNewHistorical.push(opp);
        else signals.push(opp);
      }
    } catch (e) {
      row.error = String(e?.message || e).slice(0, 120);
    }
    results.push(row);
    await sleep(150);
  }

  // Merge into inventory
  const inventoryPath = path.join(OUT, "OPPORTUNITY_INVENTORY.json");
  let inventory = { opportunities: [], historical: [], signalsWithoutDates: [] };
  if (fs.existsSync(inventoryPath)) inventory = JSON.parse(fs.readFileSync(inventoryPath, "utf8"));
  const invKeys = new Set(
    [...(inventory.opportunities || []), ...(inventory.historical || [])].map((o) =>
      hotelKey(o.hotelName, o.hotelCountry || "US"),
    ),
  );
  for (const o of netNewActive) {
    const k = hotelKey(o.hotelName, o.hotelCountry || "US");
    if (invKeys.has(k)) continue;
    invKeys.add(k);
    inventory.opportunities.push(o);
  }
  for (const o of netNewHistorical) {
    const k = hotelKey(o.hotelName, o.hotelCountry || "US");
    if (invKeys.has(k)) continue;
    invKeys.add(k);
    inventory.historical.push(o);
  }
  inventory.signalsWithoutDates = [
    ...(inventory.signalsWithoutDates || []),
    ...signals,
  ].slice(0, 800);

  const metrics = {
    track: "TRACK_A",
    wave: "WAVE4_EX10",
    documentsAttempted: docs.length,
    documentsOk: results.filter((r) => r.ok).length,
    netNewActiveOpportunities: netNewActive.length,
    netNewFranchiseExpirations: franchise,
    netNewManagementExpirations: management,
    netNewHistorical: netNewHistorical.length,
    evidenceBackedSignalsWithoutDates: signals.length,
    inventoryActive: inventory.opportunities.length,
    researchCostUsd: 0,
    publicationState: "CUSTOMER_BLOCKED",
  };

  writeJson(path.join(OUT, "WAVE4_EX10_RESULTS.json"), { results: results.slice(0, 80) });
  writeJson(path.join(OUT, "WAVE4_EX10_NET_NEW.json"), {
    active: netNewActive,
    historical: netNewHistorical,
    signals: signals.slice(0, 100),
  });
  writeJson(inventoryPath, {
    ...inventory,
    countActive: inventory.opportunities.length,
    countHistorical: inventory.historical.length,
    updatedAt: new Date().toISOString(),
  });
  writeJson(path.join(OUT, "WAVE4_EX10_METRICS.json"), metrics);
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
