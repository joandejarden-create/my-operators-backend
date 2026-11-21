#!/usr/bin/env node
/**
 * HIF V6 Track A Wave 2 — owner/portfolio reverse search + additional CMBS issuer discovery.
 * Free EDGAR full-text / company search only. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import { attributeHotelGeography } from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-wave2");
const RUN = process.argv.includes("--run");
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function loadPriorKeys() {
  const keys = new Set();
  for (const rel of [
    "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json",
    "reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json",
  ]) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    const j = JSON.parse(fs.readFileSync(fp, "utf8"));
    for (const r of [...(j.verified || []), ...(j.probable || []), ...(j.hotels || [])]) {
      const name = r.hotelName || r.propertyName;
      if (name) keys.add(`${String(name).toLowerCase()}|US`);
    }
  }
  return keys;
}

function extractBorrowerHints(html) {
  const text = String(html || "").replace(/<[^>]+>/g, " ");
  const hints = new Set();
  const patterns = [
    /Borrower[:\s]+([A-Z][A-Za-z0-9 &.,'\-]{5,80})/g,
    /Sponsor[:\s]+([A-Z][A-Za-z0-9 &.,'\-]{5,80})/g,
    /Property\s+Manager[:\s]+([A-Z][A-Za-z0-9 &.,'\-]{5,80})/g,
    /Hotel\s+Manager[:\s]+([A-Z][A-Za-z0-9 &.,'\-]{5,60})/g,
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(text))) {
      const v = m[1].replace(/\s+/g, " ").trim();
      if (v.length >= 5 && !/see\s+annex|table|below|above/i.test(v)) hints.add(v.slice(0, 80));
    }
  }
  return [...hints].slice(0, 20);
}

async function main() {
  // Additional CMBS annex URLs discovered from prior deal families / free archives
  const extraUrls = [
    "https://www.sec.gov/Archives/edgar/data/850779/000153949718000207/n1164_424b2-x16.htm",
    "https://www.sec.gov/Archives/edgar/data/1013611/000153949720000579/n2032_x17-424b2.htm",
    "https://www.sec.gov/Archives/edgar/data/1721864/000153949718001374/n1781_x14-424b2.htm",
    "https://www.sec.gov/Archives/edgar/data/1721864/000153949719000234/n1863_x10-424b2.htm",
    "https://www.sec.gov/Archives/edgar/data/1749100/000153949718001691/n1797_x5-424b2.htm",
    "https://www.sec.gov/Archives/edgar/data/1704897/000153949717001254/n1613_x8-424b2.htm",
    "https://www.sec.gov/Archives/edgar/data/1697259/000153949717000804/n1545_x6-424b2.htm",
    "https://www.sec.gov/Archives/edgar/data/1734525/000153949718001042/n1752_x7-424b2.htm",
  ];

  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, extraUrls: extraUrls.length }, null, 2));
    return;
  }

  const edgar = createSecEdgarClient();
  const priorKeys = loadPriorKeys();
  const ownerHints = [];
  const incremental = [];
  const managementRows = [];
  const docs = [];

  for (const url of extraUrls) {
    const doc = { url, ok: false, hotels: 0, owners: [], error: null };
    try {
      const res = await edgar.request(url, {
        accept: "text/html,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      if (!res.ok) {
        doc.error = `HTTP_${res.status || res.error}`;
        docs.push(doc);
        continue;
      }
      doc.ok = true;
      const owners = extractBorrowerHints(res.text);
      doc.owners = owners;
      for (const o of owners) ownerHints.push({ ownerOrOperatorHint: o, sourceUrl: url, verified: false });

      const extracted = extractCmbsPropertySchedule(
        { html: res.text, text: res.text },
        { sourceUrl: url, sourceFamily: "CMBS_424B" },
      );
      for (const row of extracted.hotelContractFindings || []) {
        const hotelName = row.hotelName || row.propertyName;
        const exp = row.primaryDate || row.expirationDate;
        if (!hotelName || !exp) continue;
        const agreementType = /manage/i.test(String(row.agreementType || row.dateMeaning || ""))
          ? "MANAGEMENT"
          : "FRANCHISE";
        const n = {
          hotelName,
          ...attributeHotelGeography({ hotelCountry: "US", city: row.city, sourceJurisdiction: "US" }),
          agreementType,
          expirationDate: exp,
          verificationClass: row.verificationClass || "SCHEDULE_EXTRACTED",
          sourceUrl: url,
          track: "TRACK_A",
          wave: "WAVE2_EXPAND",
        };
        doc.hotels += 1;
        if (agreementType === "MANAGEMENT") managementRows.push(n);
        const key = `${String(hotelName).toLowerCase()}|US`;
        if (!priorKeys.has(key)) {
          priorKeys.add(key);
          incremental.push(n);
        }
      }
    } catch (err) {
      doc.error = String(err?.message || err).slice(0, 100);
    }
    docs.push(doc);
    await sleep(120);
  }

  // Merge incremental into NET_NEW if any
  const netPath = path.join(OUT, "NET_NEW_HOTEL_DATES.json");
  let net = { count: 0, hotels: [] };
  if (fs.existsSync(netPath)) net = JSON.parse(fs.readFileSync(netPath, "utf8"));
  const seen = new Set((net.hotels || []).map((h) => `${String(h.hotelName).toLowerCase()}|US`));
  for (const h of incremental) {
    const key = `${String(h.hotelName).toLowerCase()}|US`;
    if (seen.has(key)) continue;
    seen.add(key);
    net.hotels.push(h);
  }
  net.count = net.hotels.length;

  writeJson(netPath, net);
  writeJson(path.join(OUT, "OWNER_OPERATOR_HINTS.json"), {
    count: ownerHints.length,
    note: "Borrower/sponsor/manager name hints from annex text — NOT independently verified ownership",
    hints: ownerHints,
  });
  writeJson(path.join(OUT, "MANAGEMENT_DATE_ROWS.json"), {
    count: managementRows.length,
    rows: managementRows,
  });
  writeJson(path.join(OUT, "EXPAND_DOCS.json"), { docs, incrementalCount: incremental.length });

  // Update metrics
  const metricsPath = path.join(OUT, "TRACK_A_WAVE2_METRICS.json");
  const metrics = fs.existsSync(metricsPath)
    ? JSON.parse(fs.readFileSync(metricsPath, "utf8"))
    : {};
  metrics.netNewHotelsWithDates = net.count;
  metrics.netNewFranchiseExpirations = net.hotels.filter((h) => h.agreementType === "FRANCHISE").length;
  metrics.netNewManagementExpirations = net.hotels.filter((h) => h.agreementType === "MANAGEMENT").length;
  metrics.ownerOperatorHintsUnverified = ownerHints.length;
  metrics.expandDocsOk = docs.filter((d) => d.ok).length;
  writeJson(metricsPath, metrics);

  console.log(
    JSON.stringify(
      {
        ok: true,
        docsOk: docs.filter((d) => d.ok).length,
        incremental: incremental.length,
        netNewTotal: net.count,
        managementRows: managementRows.length,
        ownerHints: ownerHints.length,
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
