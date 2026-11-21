#!/usr/bin/env node
/**
 * HIF V6 — Targeted agreement-type resolution for combined CMBS columns.
 * Re-reads source documents; looks for Franchisor / Manager / Flag / Operator
 * columns or footnotes near the hotel row. Does not invent types from brand alone.
 *
 * Usage: node scripts/hif-v6-agreement-type-resolution.mjs --run [--max=40]
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

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-agreement-type-resolution");
const RUN = process.argv.includes("--run");
const MAX = Number((process.argv.find((a) => a.startsWith("--max=")) || "").split("=")[1]) || 40;
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

function collectUnresolved() {
  const rows = [];
  const seen = new Set();
  for (const rel of [
    "reports/sec-edgar-hif-v6-canonical-inventory/AGREEMENT_TYPE_UNRESOLVED.json",
    "reports/sec-edgar-hif-v6-name-free-sec-scaleup/NET_NEW_EVENTS.json",
    "reports/sec-edgar-hif-v6-name-free-sec-continuous/NET_NEW_EVENTS.json",
  ]) {
    const j = load(rel);
    const list = j?.hotels || j?.rows || [];
    for (const r of list) {
      if (r.agreementTypeResolved === true) continue;
      if (!/UNRESOLVED|UNKNOWN|COMBINED/i.test(String(r.commercialList || r.agreementType || r.dateMeaning || ""))) {
        if (r.agreementTypeResolved !== false) continue;
      }
      const key = `${r.hotelName}|${r.expirationDate}|${r.sourceUrl}`;
      if (seen.has(key)) continue;
      seen.add(key);
      rows.push(r);
    }
  }
  // Prefer near-term expirations
  rows.sort((a, b) => String(a.expirationDate || "9999").localeCompare(String(b.expirationDate || "9999")));
  return rows;
}

function decode(html) {
  return String(html || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#160;/g, " ")
    .replace(/<br\s*\/?>/gi, " | ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Heuristic: find hotel name in plaintext; look ±400 chars for franchisor/manager cues.
 * MULTIPLE when both franchise and management cues present without exclusive column.
 */
function resolveFromContext(text, hotelName) {
  const plain = decode(text);
  const needle = String(hotelName || "").replace(/\s+/g, " ").trim();
  if (!needle || needle.length < 6) return { status: "SKIP", reason: "WEAK_NAME" };
  const idx = plain.toLowerCase().indexOf(needle.toLowerCase());
  if (idx < 0) return { status: "UNRESOLVED", reason: "HOTEL_NOT_FOUND_IN_DOC" };
  const window = plain.slice(Math.max(0, idx - 120), Math.min(plain.length, idx + needle.length + 500));
  const w = window.toLowerCase();

  const hasFranchisor =
    /\bfranchisor\b|\bfranchise\s+brand\b|\bflag\b|\bbrand\s+affiliation\b|\blicensor\b/.test(w) &&
    !/\bfranchise\s+expiration\s+(reserve|trigger)\b/.test(w);
  const hasManager =
    /\bmanager\b|\bmanagement\s+company\b|\boperator\b|\bhma\b|\bhotel\s+manager\b/.test(w);
  const franchiseBrandToken =
    /\b(marriott|hilton|hyatt|ihg|intercontinental|holiday inn|courtyard|hampton|residence inn|springhill|fairfield|embassy|autograph|tapestry|curio|westin|sheraton|w hotels|st\.?\s*regis|four points|aloft|element|moxy|ac hotels|kimpton|radisson|wyndham|best western|choice|ascend|comfort|quality inn|clarion|econo|rodeway|mainstay|suburban|sleep inn|cambria)\b/i.test(
      window,
    );
  const managerNameToken =
    /\b(aimbridge|remington|white lodging|highgate|hesp|crescent hotels|interstate hotels|pyramid|coral ?tree|hersha|destination hotels|sage hospitality|pacific hospitality|sherman lodging)\b/i.test(
      window,
    );

  // Adjacent schedule columns often list Franchisor and Manager separately
  const colFranchise = /franchisor|franchise brand|flag\b/i.test(window);
  const colManager = /manager|operator\b/i.test(window);

  if (colFranchise && colManager) {
    return {
      status: "MULTIPLE",
      agreementType: "MULTIPLE",
      agreementTypeResolved: true,
      evidence: "Same-row franchisor and manager labels present — combined column not exclusively franchise or management",
      windowSnippet: window.slice(0, 220),
    };
  }
  if (colFranchise && !colManager && (hasFranchisor || franchiseBrandToken)) {
    return {
      status: "FRANCHISE",
      agreementType: "FRANCHISE",
      agreementTypeResolved: true,
      evidence: "Adjacent franchisor/flag column supports franchise agreement classification",
      windowSnippet: window.slice(0, 220),
    };
  }
  if (colManager && !colFranchise && (hasManager || managerNameToken)) {
    return {
      status: "MANAGEMENT",
      agreementType: "MANAGEMENT",
      agreementTypeResolved: true,
      evidence: "Adjacent manager/operator column supports management agreement classification",
      windowSnippet: window.slice(0, 220),
    };
  }
  if (hasFranchisor && !hasManager) {
    return {
      status: "FRANCHISE",
      agreementType: "FRANCHISE",
      agreementTypeResolved: true,
      evidence: "Local franchisor/license cues without manager column",
      windowSnippet: window.slice(0, 220),
    };
  }
  if (hasManager && !hasFranchisor && !franchiseBrandToken) {
    return {
      status: "MANAGEMENT",
      agreementType: "MANAGEMENT",
      agreementTypeResolved: true,
      evidence: "Local manager/operator cues without franchisor column",
      windowSnippet: window.slice(0, 220),
    };
  }
  if (hasFranchisor && hasManager) {
    return {
      status: "MULTIPLE",
      agreementType: "MULTIPLE",
      agreementTypeResolved: true,
      evidence: "Both franchise and management cues near property row — keep MULTIPLE",
      windowSnippet: window.slice(0, 220),
    };
  }

  return {
    status: "UNRESOLVED",
    agreementType: "UNKNOWN",
    agreementTypeResolved: false,
    evidence: "No exclusive franchisor/manager column evidence near hotel row",
    windowSnippet: window.slice(0, 180),
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    const u = collectUnresolved();
    console.log(JSON.stringify({ dryRun: true, unresolvedCandidates: u.length, max: MAX }, null, 2));
    return;
  }

  const candidates = collectUnresolved().slice(0, MAX);
  const edgar = createSecEdgarClient();
  const byUrl = new Map();
  for (const c of candidates) {
    if (!c.sourceUrl) continue;
    if (!byUrl.has(c.sourceUrl)) byUrl.set(c.sourceUrl, []);
    byUrl.get(c.sourceUrl).push(c);
  }

  const results = [];
  let resolvedFranchise = 0;
  let resolvedManagement = 0;
  let resolvedMultiple = 0;
  let stillUnresolved = 0;

  for (const [url, hotels] of byUrl) {
    const res = await edgar.request(url, { accept: "text/html,*/*", maxAgeMs: 14 * 24 * 3600 * 1000 });
    const text = res.text || "";
    if (!res.ok || text.length < 2000) {
      for (const h of hotels) {
        results.push({
          hotelName: h.hotelName,
          expirationDate: h.expirationDate,
          sourceUrl: url,
          status: "FETCH_FAIL",
          agreementTypeResolved: false,
        });
        stillUnresolved += 1;
      }
      continue;
    }
    for (const h of hotels) {
      const r = resolveFromContext(text, h.hotelName);
      const out = {
        hotelName: h.hotelName,
        expirationDate: h.expirationDate,
        sourceUrl: url,
        priorCommercialList: h.commercialList,
        ...r,
        wave: "AGREEMENT_TYPE_RESOLUTION",
        asOf,
        note: "Does not auto-promote to BCT/MCT commercial lists unless commercial thesis separately confirmed",
      };
      results.push(out);
      if (r.status === "FRANCHISE") resolvedFranchise += 1;
      else if (r.status === "MANAGEMENT") resolvedManagement += 1;
      else if (r.status === "MULTIPLE") resolvedMultiple += 1;
      else stillUnresolved += 1;
    }
    await sleep(140);
  }

  const metrics = {
    wave: "AGREEMENT_TYPE_RESOLUTION",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    candidatesAttempted: candidates.length,
    documentsFetched: byUrl.size,
    resolvedFranchise,
    resolvedManagement,
    resolvedMultiple,
    stillUnresolved,
    resolutionRate: Number(
      (((resolvedFranchise + resolvedManagement + resolvedMultiple) / Math.max(1, candidates.length)) * 100).toFixed(1),
    ),
  };

  writeJson(path.join(OUT, "RESOLUTION_RESULTS.json"), { count: results.length, results });
  writeJson(path.join(OUT, "RESOLUTION_METRICS.json"), metrics);
  writeJson(
    path.join(OUT, "RESOLVED_FRANCHISE.json"),
    { hotels: results.filter((r) => r.status === "FRANCHISE") },
  );
  writeJson(
    path.join(OUT, "RESOLVED_MANAGEMENT.json"),
    { hotels: results.filter((r) => r.status === "MANAGEMENT") },
  );
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
