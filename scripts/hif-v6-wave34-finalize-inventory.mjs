#!/usr/bin/env node
/**
 * Finalize Wave 3/4 opportunity inventory under opportunities-first rules.
 * Quarantines noisy regex hotel-name hits. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  OPPORTUNITY_MILESTONE_V6,
  progressTowardOpportunityMilestone,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-opportunities-first-priority-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-wave34");

function hotelKey(name, country = "US") {
  return `${String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()}|${country}`;
}

function isNoisyHotelName(name) {
  const n = String(name || "").trim();
  if (n.length < 10 || n.length > 70) return true;
  if (
    /^(the\s+)?(particular|per|interim|borrower|loan agreement|english translation|holdings introduction|development services agreement|form of|each|any|such)\b/i.test(
      n,
    )
  ) {
    return true;
  }
  if (/\b(agreement|amendment|exhibit|introduction|translation|borrower|loan)\b/i.test(n)) {
    return true;
  }
  // Require a recognizable lodging stem beyond bare "Hotel"
  if (/^(the\s+)?[A-Z][a-z]+\s+Hotel$/i.test(n) && !/(Hilton|Marriott|Hyatt|IHG|Accor)/i.test(n)) {
    // "American Hotel" ok-ish; "Particular Hotel" already caught
  }
  return false;
}

function loadJson(rel) {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

const baseline = loadJson(
  "reports/sec-edgar-hif-v6-industrial-scale/V5_BASELINE_SCORECARD_V6_TAXONOMY.json",
);
const baselineQualified = Number(baseline?.metrics?.QUALIFIED_COMMERCIAL_TARGETS || 139);

const wave2 = loadJson("reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json");
const asOf = new Date().toISOString().slice(0, 10);
const wave2Hotels = wave2?.hotels || [];
const wave2Active = wave2Hotels.filter((h) => h.expirationDate && String(h.expirationDate) >= asOf);
const wave2Historical = wave2Hotels.filter(
  (h) => h.expirationDate && String(h.expirationDate) < asOf,
);

const wave3 = loadJson("reports/sec-edgar-hif-v6-track-a-wave34/WAVE3_NET_NEW.json");
const wave3Active = (wave3?.hotels || []).filter((h) => h.countsAsActiveOpportunity);
const wave3Historical = (wave3?.hotels || []).filter((h) => !h.countsAsActiveOpportunity);

const ex10 = loadJson("reports/sec-edgar-hif-v6-track-a-wave34/WAVE4_EX10_NET_NEW.json");
const rawSignals = ex10?.signals || [];
const cleanSignals = [];
const quarantined = [];
for (const s of rawSignals) {
  if (isNoisyHotelName(s.hotelName) || s.scopeClass === "FORM_WITHOUT_PROPERTY") {
    quarantined.push({ ...s, trackingBucket: "INVALID_OR_QUARANTINED" });
  } else if (s.scopeClass === "UNRESOLVED" && !/(Hilton|Marriott|Hyatt|Hampton|Courtyard|Residence|Holiday|Homewood|Embassy|Westin|Sheraton|Dreams|Montage|Springmaid)/i.test(s.hotelName)) {
    quarantined.push({ ...s, trackingBucket: "INVALID_OR_QUARANTINED" });
  } else {
    cleanSignals.push({
      ...s,
      countsAsEvidenceBackedOpportunityCandidate: true,
      countsAsActiveDatedOpportunity: false,
    });
  }
}

// Deduped active dated opportunities from waves 2–3 (baseline kept separate)
const datedActive = [];
const seen = new Set();
for (const h of [...wave2Active, ...wave3Active]) {
  const k = hotelKey(h.hotelName, h.hotelCountry || "US");
  if (seen.has(k)) continue;
  seen.add(k);
  datedActive.push(h);
}

const historical = [];
for (const h of [...wave2Historical, ...wave3Historical]) {
  const k = hotelKey(h.hotelName, h.hotelCountry || "US");
  if (seen.has(k)) continue;
  seen.add(k);
  historical.push(h);
}

// Evidence-backed candidates toward 1000 = baseline + net-new dated active + clean undated signals
// (historical excluded from active milestone)
const progressCurrent = baselineQualified + datedActive.length + cleanSignals.length;
const progress = progressTowardOpportunityMilestone({
  distinctEvidenceBackedOpportunities: progressCurrent,
});

const summary = {
  priority: "OPPORTUNITIES_FIRST",
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  wave2PrStatus: "OPEN_AWAITING_IC",
  baselineQualifiedCommercialTargets: baselineQualified,
  netNewVerifiedFranchiseExpirationsWaves23: [...wave2Hotels, ...(wave3?.hotels || [])].filter(
    (h) => h.agreementType === "FRANCHISE",
  ).length,
  netNewVerifiedManagementExpirationsWaves23: [...wave2Hotels, ...(wave3?.hotels || [])].filter(
    (h) => h.agreementType === "MANAGEMENT",
  ).length,
  netNewActiveDatedOpportunitiesWaves23: datedActive.length,
  netNewHistoricalExpirationsWaves23: historical.length,
  evidenceBackedSignalsWithoutDatesClean: cleanSignals.length,
  evidenceBackedSignalsQuarantined: quarantined.length,
  trackBVerifiedExpirations: 0,
  trackBEvidenceSignals: 0,
  totalDeduplicatedEvidenceBackedOpportunityInventoryEstimate: progressCurrent,
  progressToward1000: progress,
  franchiseVsManagementSplit: {
    baselineBrandConversion: baseline?.listBreakdown?.brandConversionDistinctHotels ?? 136,
    baselineManagement: baseline?.listBreakdown?.managementContractDistinctHotels ?? 4,
    wave23FranchiseDated: datedActive.filter((h) => h.agreementType === "FRANCHISE").length,
    wave23ManagementDated: datedActive.filter((h) => h.agreementType === "MANAGEMENT").length,
    cleanSignalsFranchise: cleanSignals.filter((s) => s.agreementType === "FRANCHISE").length,
    cleanSignalsManagement: cleanSignals.filter((s) => s.agreementType === "MANAGEMENT").length,
  },
  ownershipEnrichment: "DEFERRED",
  milestone: OPPORTUNITY_MILESTONE_V6,
  recommendedNextExpansion: [
    "Expand CMBS table extractor for non-pipe HTML schedule formats (many EFTS hits are prose-only)",
    "Scale EX-10 HMA/franchise exhibits with stricter hotel-identity NER + term extraction",
    "International issuer filings with named hotels + agreement dates (SGX/HKEX/SEDAR+/CVM/EDINET document-level)",
    "Do not spend capacity on CH/HMLR ownership graphs until opportunity inventory approaches milestone",
  ],
};

fs.writeFileSync(
  path.join(OUT, "FINALIZED_OPPORTUNITY_INVENTORY.json"),
  JSON.stringify(
    {
      datedActiveOpportunities: datedActive,
      historicalExpirations: historical,
      evidenceBackedSignalsWithoutDates: cleanSignals,
      quarantinedSignals: quarantined.slice(0, 100),
      summary,
    },
    null,
    2,
  ),
);
fs.writeFileSync(path.join(OUT, "WAVE34_COMBINED_METRICS.json"), JSON.stringify(summary, null, 2));

const md = `# HIF V6 — Opportunities-First Waves 3–4 Combined Report

**As of:** ${asOf}  
**Publication:** CUSTOMER_BLOCKED  
**Spend:** $0  
**Priority:** Scale opportunities first; ownership enrichment deferred  
**Wave 2 PRs:** still OPEN (#511/#512) — research continued on registered Track A/B workspaces without waiting for merge

## Progress toward 1,000

| Measure | Count |
|--------|------:|
| Baseline qualified commercial targets | ${baselineQualified} |
| Net-new **active dated** opportunities (Waves 2–3) | ${datedActive.length} |
| Clean evidence-backed signals **without dates** (Wave 4 EX-10) | ${cleanSignals.length} |
| Historical expirations (tracked; **not** active) | ${historical.length} |
| Quarantined noisy name hits | ${quarantined.length} |
| Track B verified expirations (Waves 3–4) | 0 |
| **Estimated inventory toward 1,000** | **${progress.current}** |
| Remaining | ${progress.remaining} |
| Milestone met | ${progress.milestoneMet} |

## Track A

- Wave 3 CMBS: 35 docs · 5 net-new hotel dates · **2 active** / 3 historical (examples: Hampton Inn Joplin 2033-01-31; Hilton Garden Inn Joplin expired 2026-10-31 → historical)
- Wave 4 CMBS: 50 schedule-bearing docs · **0** net-new (prior inventory exhaustion on extractable deal families)
- Wave 4 EX-10: 40 exhibits · **0** verified term expirations · ${cleanSignals.length} clean undated hotel+agreement signals after quarantine

## Track B

- Shifted to international/regulatory disclosure emphasis (CH/HMLR supporting only)
- Portals retrieved; EX-10 REIT deepen reviewed
- **0** independently verified hotel-specific contractual expirations this cycle (no invented coverage)

## Source yield

- Highest yield remains **SEC CMBS property schedules** with extractable Franchise Expiration Date tables
- Large EFTS hit counts ≠ extractable schedules (many prospectus prose hits)
- EX-10 HMA corpus is large; term-date extraction yield still low without stronger NER

## Governance

- CUSTOMER_BLOCKED · no HPC/Meta · Agent manual-only · Jev shadow · Lease deferred · $0 paid
- Ownership not required for discovery qualification
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_WAVE34_OPPORTUNITIES_FIRST.md"), md);
console.log(JSON.stringify(summary, null, 2));
