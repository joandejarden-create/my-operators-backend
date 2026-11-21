#!/usr/bin/env node
/**
 * Track B — Hotel universe coverage by physical country (HPC live counts).
 * Read-only. Does not fabricate external denominators.
 * CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL, HPC_TABLE_ID } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}

/** HPC Country field uses full English names (verified live scan 2026-11-21). */
const PRIORITY = [
  { code: "US", labels: ["United States", "US", "USA"] },
  { code: "MX", labels: ["Mexico", "MX", "México"] },
  { code: "BR", labels: ["Brazil", "BR", "Brasil"] },
  { code: "GB", labels: ["United Kingdom", "GB", "UK", "England", "Scotland", "Wales"] },
  { code: "CA", labels: ["Canada", "CA"] },
];

/** Credible external denominators — cite sources; UNKNOWN when not defensible. */
const EXTERNAL_DENOMINATORS = {
  US: {
    brandedHotelsEstimate: 37000,
    totalHotelsEstimate: 65000,
    confidence: "MEDIUM",
    note: "Derived: ~65k US hotels; ~57% chain-affiliated hotels (CoStar/STR cited via 10minhotel Global Supply 2027). Branded ≈ 0.57×65k ≈ 37k — approximate.",
    sources: ["https://en.10minhotel.com/2026/11/05/global-hotel-supply-2027-edition-china-chains-independents/"],
  },
  CA: {
    brandedHotelsEstimate: 2148,
    totalHotelsEstimate: 7556,
    confidence: "HIGH",
    note: "Horwath HTL Canada Hotel & Chains 2026: 2,148 chain hotels / ~7,560 total sample as of 2025.",
    sources: ["https://horwathhtl.com/wp-content/uploads/2026/04/1-Canada-Hotel-Chains-Report-2026-FINAL.pdf"],
  },
  MX: {
    brandedHotelsEstimate: null,
    totalHotelsEstimate: 27948,
    confidence: "MEDIUM",
    note: "Horwath HTL Mexico 2025: 27,948 total hotels YE2025 (SECTUR/DATATUR). Branded share UNKNOWN — TierStay major-5 only = 814 (not full branded universe).",
    sources: [
      "https://horwathhtl.com/wp-content/uploads/2026/08/Mexico-Hotel-Industry-Report-2025-English.pdf",
      "https://tierstay.com/en/hotels/",
    ],
  },
  BR: {
    brandedHotelsEstimate: null,
    totalHotelsEstimate: null,
    confidence: "LOW",
    note: "No defensible full branded census in free sources this batch. TierStay major-5 = 390 (not universe).",
    sources: ["https://tierstay.com/en/hotels/"],
  },
  GB: {
    brandedHotelsEstimate: null,
    totalHotelsEstimate: 13415,
    confidence: "MEDIUM",
    note: "ONS UK hotels 13,415 (Mar 2026) cited via 10minhotel. Full branded share UNKNOWN; TierStay major-5 = 1,121.",
    sources: [
      "https://en.10minhotel.com/2026/11/05/global-hotel-supply-2027-edition-china-chains-independents/",
      "https://tierstay.com/en/hotels/",
    ],
  },
};

async function countByFormula(formula) {
  let n = 0;
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) qs.set("offset", offset);
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    n += (json.records || []).length;
    offset = json.offset;
  } while (offset);
  return n;
}

function orCountryFormula(labels) {
  return `OR(${labels.map((l) => `{Country}="${l.replace(/"/g, '\\"')}"`).join(",")})`;
}

const out = {
  asOf: new Date().toISOString(),
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  stream: "TRACK_B",
  baseId: GRAPH_BASE_ID_CANONICAL,
  hpcTableId: HPC_TABLE_ID,
  note: "HPC counts are verified hotels in Dealality census — not equal to national branded universe. Coverage % only when external branded denominator is not UNKNOWN.",
  priorityMarkets: {},
  worldwideEligibility: "UNRESTRICTED",
};

for (const m of PRIORITY) {
  process.stderr.write(`counting HPC ${m.code}...\n`);
  const hpcCount = await countByFormula(orCountryFormula(m.labels));
  const ext = EXTERNAL_DENOMINATORS[m.code];
  const brandedDenom = ext.brandedHotelsEstimate;
  const coverage =
    brandedDenom && brandedDenom > 0
      ? {
          hpcOverBrandedEstimatePct: Math.round((hpcCount / brandedDenom) * 1000) / 10,
          brandedDenominator: brandedDenom,
          denominatorStatus: "ESTIMATED",
        }
      : {
          hpcOverBrandedEstimatePct: null,
          brandedDenominator: null,
          denominatorStatus: "UNKNOWN",
        };
  out.priorityMarkets[m.code] = {
    hpcVerifiedHotels: hpcCount,
    external: ext,
    coverage,
    researchCoverageLabel:
      hpcCount === 0 ? "UNDER_RESEARCHED" : coverage.denominatorStatus === "UNKNOWN" ? "PARTIAL_NO_DENOMINATOR" : "PARTIAL",
  };
  await new Promise((r) => setTimeout(r, 200));
}

const outDir = path.join(ROOT, "reports/sec-edgar-hif-track-b-historical-foundation");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(path.join(outDir, "HOTEL_UNIVERSE_COVERAGE.json"), JSON.stringify(out, null, 2));
console.log(JSON.stringify({ ok: true, priority: Object.fromEntries(Object.entries(out.priorityMarkets).map(([k, v]) => [k, v.hpcVerifiedHotels])), outDir }, null, 2));
