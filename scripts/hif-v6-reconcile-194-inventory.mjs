#!/usr/bin/env node
/**
 * Exact reconciliation of the reported ~194 HIF opportunity inventory.
 * Counts toward the 1,000 milestone only when commercially relevant and active
 * (dated future/current OR clean undated evidence-backed). Historical preserved separately.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-inventory-reconciliation");

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

const asOf = new Date().toISOString().slice(0, 10);
const GENERIC_BRAND_ONLY =
  /^(the\s+)?(holiday inn(?: express)?|hampton inn(?: & suites)?|comfort (?:inn|suites)|fairfield inn(?: & suites)?|residence inn|courtyard|homewood suites|springhill suites|embassy suites|hilton garden inn|marriott|hilton|hyatt|wyndham|ramada|days inn|super 8|motel 6|best western|la quinta(?: inn)?|quality inn|crowne plaza|doubletree|sheraton|westin|radisson|premier inn)\s*(hotel|inn|suites|resort)?$/i;

function isNoisy(name) {
  const n = String(name || "").trim();
  if (n.length < 10) return true;
  if (GENERIC_BRAND_ONLY.test(n)) return true;
  if (/^(owned hotel|pro forma|awarded|particular|per |interim |includes |currency-neutral|about |for |north america hotel)/i.test(n)) {
    return true;
  }
  if (/\b(agreement|amendment|exhibit|pro forma)\b/i.test(n)) return true;
  return false;
}

/** Build row registry */
const byKey = new Map();

function upsert(row) {
  const name = row.hotelName || row.propertyName;
  if (!name) return;
  const country = row.hotelCountry || row.country || "US";
  const key = hotelKey(name, country);
  const prev = byKey.get(key);
  const next = {
    key,
    hotelName: name,
    hotelCountry: String(country).toUpperCase(),
    agreementType: String(row.agreementType || "UNCLASSIFIED").toUpperCase().includes("MANAGE")
      ? "MANAGEMENT"
      : String(row.agreementType || "").toUpperCase().includes("FRANCH")
        ? "FRANCHISE"
        : /MANAGE/i.test(row.commercialList || "")
          ? "MANAGEMENT"
          : "FRANCHISE",
    expirationDate: row.expirationDate || row.primaryDate || null,
    commercialList:
      row.commercialList ||
      (String(row.agreementType || "").toUpperCase().includes("MANAGE")
        ? "MANAGEMENT_CONTRACT_TARGETS"
        : "BRAND_CONVERSION_TARGETS"),
    sourceUrl: row.sourceUrl || row.source?.documentUrl || null,
    sourceFamily: row.sourceFamily || "UNKNOWN",
    track: row.track || null,
    wave: row.wave || row.sourceWave || null,
    provenance: [...(prev?.provenance || []), row._prov].filter(Boolean),
  };

  // Classification
  let status;
  if (isNoisy(name)) status = "QUARANTINED_OR_INVALID";
  else if (next.expirationDate && String(next.expirationDate) < asOf) status = "HISTORICAL_OR_SUPERSEDED";
  else if (next.expirationDate && String(next.expirationDate) >= asOf) status = "ACTIVE_DATED";
  else if (row.qualificationLevel === "HOLD_RESEARCH_CANDIDATE" || row.trackingBucket === "RESEARCH_CANDIDATES") {
    status = "RESEARCH_CANDIDATE_OR_HOLD";
  } else if (
    row.countsAsEvidenceBackedProspect ||
    row.countsAsEvidenceBackedOpportunityCandidate ||
    row.trackingBucket === "EVIDENCE_BACKED_COMMERCIAL_SIGNALS_WITHOUT_DATES" ||
    row.qualificationLevel === "EVIDENCE_BACKED_COMMERCIAL_PROSPECT" ||
    (!next.expirationDate && (row.verificationClass || "").includes("EX10"))
  ) {
    status = "ACTIVE_UNDATED_EVIDENCE_BACKED";
  } else if (row.countsAsActiveOpportunity === false && next.expirationDate) {
    status = "HISTORICAL_OR_SUPERSEDED";
  } else if (!next.expirationDate) {
    status = "RESEARCH_CANDIDATE_OR_HOLD";
  } else {
    status = "ACTIVE_DATED";
  }
  next.status = status;
  next.countsToward1000 =
    status === "ACTIVE_DATED" || status === "ACTIVE_UNDATED_EVIDENCE_BACKED";

  // Prefer richer record if upgrading from hold → active, or adding date
  if (!prev) {
    byKey.set(key, next);
    return;
  }
  const rank = (s) =>
    ({
      ACTIVE_DATED: 4,
      ACTIVE_UNDATED_EVIDENCE_BACKED: 3,
      HISTORICAL_OR_SUPERSEDED: 2,
      RESEARCH_CANDIDATE_OR_HOLD: 1,
      QUARANTINED_OR_INVALID: 0,
    })[s] ?? 0;
  if (rank(next.status) > rank(prev.status) || (next.expirationDate && !prev.expirationDate)) {
    next.provenance = [...new Set([...(prev.provenance || []), ...(next.provenance || [])])];
    byKey.set(key, next);
  } else {
    prev.provenance = [...new Set([...(prev.provenance || []), ...(next.provenance || [])])];
  }
}

// --- Ingest sources that compose the ~194 claim ---

// 1) Baseline V5/V6 qualified commercial targets (count-only if no hotel list)
const baseline = load(
  "reports/sec-edgar-hif-v6-industrial-scale/V5_BASELINE_SCORECARD_V6_TAXONOMY.json",
);
const baselineQualified = Number(baseline?.metrics?.QUALIFIED_COMMERCIAL_TARGETS || 139);
const baselineBrand = Number(baseline?.listBreakdown?.brandConversionDistinctHotels || 136);
const baselineMgmt = Number(baseline?.listBreakdown?.managementContractDistinctHotels || 4);

// Try to find explicit baseline hotel lists
// Strict baseline lists that underpin the 139 QUALIFIED_COMMERCIAL_TARGETS figure.
// Do NOT ingest expanded V5_* files into the 1,000 milestone — they inflate undated counts.
for (const rel of [
  "reports/sec-edgar-commercial-targets-v1/BRAND_CONVERSION_TARGETS.json",
  "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS.json",
]) {
  const j = load(rel);
  if (!j) continue;
  const rows = j.hotels || j.rows || j.targets || j.items || (Array.isArray(j) ? j : []);
  for (const r of rows) {
    const hotelName = r.hotelName || r.propertyName || r.hotel || r.name;
    const exp =
      r.expirationDate ||
      r.primaryDate ||
      r.franchiseExpirationDate ||
      r.futureExpirationDate ||
      r.verifiedEndDate ||
      r.latestCorroboratedExpirationDate ||
      null;
    // Prefer futureExpirationDate when present for active classification
    const activeExp = r.futureExpirationDate || (exp && String(exp) >= asOf ? exp : null);
    const histExp = r.historicalExpirationDate || (exp && String(exp) < asOf ? exp : null);
    const useExp = activeExp || histExp || exp;
    upsert({
      ...r,
      hotelName,
      expirationDate: useExp,
      agreementType: r.agreementType || (rel.includes("MANAGEMENT") ? "MANAGEMENT" : "FRANCHISE"),
      commercialList: rel.includes("MANAGEMENT")
        ? "MANAGEMENT_CONTRACT_TARGETS"
        : "BRAND_CONVERSION_TARGETS",
      hotelCountry: r.hotelCountry || r.country || "US",
      _prov: rel,
      countsAsActiveOpportunity: Boolean(activeExp) || (!useExp && !histExp),
      countsAsEvidenceBackedProspect: !useExp,
      qualificationLevel: histExp && !activeExp
        ? "HISTORICAL"
        : activeExp
          ? "ACTIVE_DATED"
          : "EVIDENCE_BACKED_COMMERCIAL_PROSPECT",
    });
  }
}

// 2) Wave 2 net-new hotel dates
const wave2 = load("reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json");
for (const h of wave2?.hotels || []) {
  upsert({
    ...h,
    _prov: "wave2_NET_NEW_HOTEL_DATES",
    countsAsActiveOpportunity: h.expirationDate && String(h.expirationDate) >= asOf,
  });
}

// 3) Wave 3 net-new
const wave3 = load("reports/sec-edgar-hif-v6-track-a-wave34/WAVE3_NET_NEW.json");
for (const h of wave3?.hotels || []) {
  upsert({ ...h, _prov: "wave3_NET_NEW", countsAsActiveOpportunity: h.countsAsActiveOpportunity });
}

// 4) Wave 4 EX10 signals (pre-quarantine file + finalized)
const wave4ex = load("reports/sec-edgar-hif-v6-track-a-wave34/WAVE4_EX10_NET_NEW.json");
for (const h of [...(wave4ex?.active || []), ...(wave4ex?.signals || []), ...(wave4ex?.historical || [])]) {
  upsert({ ...h, _prov: "wave4_EX10_NET_NEW" });
}

const yieldClean = load("reports/sec-edgar-hif-v6-yield-wave/YIELD_PROSPECTS_CLEAN.json");
for (const h of yieldClean?.hotels || []) {
  upsert({
    ...h,
    _prov: "yield_CONVERSION_OPERATOR_CLEAN",
    countsAsEvidenceBackedProspect: true,
    qualificationLevel: "EVIDENCE_BACKED_COMMERCIAL_PROSPECT",
  });
}

const finalized = load(
  "reports/sec-edgar-hif-v6-track-a-wave34/FINALIZED_OPPORTUNITY_INVENTORY.json",
);
for (const h of [
  ...(finalized?.datedActiveOpportunities || []),
  ...(finalized?.historicalExpirations || []),
  ...(finalized?.evidenceBackedSignalsWithoutDates || []),
]) {
  upsert({
    ...h,
    _prov: "wave34_FINALIZED",
    countsAsEvidenceBackedOpportunityCandidate: h.countsAsEvidenceBackedOpportunityCandidate,
  });
}
for (const h of finalized?.quarantinedSignals || []) {
  upsert({ ...h, _prov: "wave34_QUARANTINED", qualificationLevel: "HOLD_RESEARCH_CANDIDATE" });
}

// 5) Prior CMBS probable findings (may underpin baseline expirations)
const cmbs = load("reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json");
for (const h of [...(cmbs?.verified || []), ...(cmbs?.probable || [])]) {
  upsert({
    hotelName: h.hotelName,
    hotelCountry: "US",
    agreementType: h.agreementType,
    expirationDate: h.primaryDate || h.expirationDate,
    sourceUrl: h.source?.documentUrl,
    sourceFamily: "SEC_CMBS",
    _prov: "cmbs_VERIFIED_FINDINGS",
    verificationClass: h.verificationClass,
  });
}

const rows = [...byKey.values()];

function bucket(status) {
  return rows.filter((r) => r.status === status);
}

const activeDated = bucket("ACTIVE_DATED");
const activeUndated = bucket("ACTIVE_UNDATED_EVIDENCE_BACKED");
const historical = bucket("HISTORICAL_OR_SUPERSEDED");
const hold = bucket("RESEARCH_CANDIDATE_OR_HOLD");
const quarantined = bucket("QUARANTINED_OR_INVALID");
const toward1000 = rows.filter((r) => r.countsToward1000);

// Explain the reported 194 formula vs exact registry
const reportedFormula = {
  baselineQualifiedCommercialTargets: baselineQualified,
  wave23ActiveDatedClaimed: 11,
  wave34CleanUndatedClaimed: 44,
  reportedSum: baselineQualified + 11 + 44,
  note: "194 = 139 baseline + 11 active dated (W2/W3) + 44 clean undated EX-10 signals (additive estimate; may double-count if baseline already includes some hotels)",
};

// Country / franchise split for toward-1000 set
const byCountry = {};
const franchiseVsMgmt = { FRANCHISE: 0, MANAGEMENT: 0, OTHER: 0 };
for (const r of toward1000) {
  byCountry[r.hotelCountry] = (byCountry[r.hotelCountry] || 0) + 1;
  if (r.agreementType === "FRANCHISE") franchiseVsMgmt.FRANCHISE += 1;
  else if (r.agreementType === "MANAGEMENT") franchiseVsMgmt.MANAGEMENT += 1;
  else franchiseVsMgmt.OTHER += 1;
}

const byStatusCountry = {};
for (const r of rows) {
  byStatusCountry[r.status] = byStatusCountry[r.status] || {};
  byStatusCountry[r.status][r.hotelCountry] =
    (byStatusCountry[r.status][r.hotelCountry] || 0) + 1;
}

const reconciliation = {
  asOf,
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  reportedClaim: reportedFormula,
  exactRegistry: {
    distinctHotelsInRegistry: rows.length,
    ACTIVE_DATED: activeDated.length,
    ACTIVE_UNDATED_EVIDENCE_BACKED: activeUndated.length,
    HISTORICAL_OR_SUPERSEDED: historical.length,
    RESEARCH_CANDIDATE_OR_HOLD: hold.length,
    QUARANTINED_OR_INVALID: quarantined.length,
    countsToward1000Milestone: toward1000.length,
  },
  countsToward1000Breakdown: {
    activeDated: activeDated.length,
    activeUndatedEvidenceBacked: activeUndated.length,
    total: toward1000.length,
    franchiseVsManagement: franchiseVsMgmt,
    byCountry,
  },
  baselineReference: {
    QUALIFIED_COMMERCIAL_TARGETS: baselineQualified,
    brandConversion: baselineBrand,
    managementContract: baselineMgmt,
    baselineHotelListPresentInRepo: byKey.size > 0 && rows.some((r) => (r.provenance || []).some((p) => /baseline|commercial-targets|QUALIFIED/i.test(p))),
    note: "If baseline hotel-level list is absent, baseline 139 remains a count-only layer and exact hotel-level dedupe vs baseline is partial",
  },
  varianceVsReported194: {
    reported: 194,
    exactToward1000: toward1000.length,
    delta: toward1000.length - 194,
    explanation:
      toward1000.length === 194
        ? "Exact match"
        : "Variance expected when baseline hotel IDs are unavailable for dedupe and/or when CMBS probable rows expand the registry beyond the additive 139+11+44 estimate",
  },
  milestoneRule: {
    counts: ["ACTIVE_DATED", "ACTIVE_UNDATED_EVIDENCE_BACKED"],
    doesNotCount: [
      "HISTORICAL_OR_SUPERSEDED",
      "RESEARCH_CANDIDATE_OR_HOLD",
      "QUARANTINED_OR_INVALID",
    ],
  },
};

writeJson(path.join(OUT, "RECONCILIATION_SUMMARY.json"), reconciliation);
writeJson(path.join(OUT, "TOWARD_1000_ROWS.json"), {
  count: toward1000.length,
  rows: toward1000.sort((a, b) => a.hotelName.localeCompare(b.hotelName)),
});
writeJson(path.join(OUT, "ACTIVE_DATED.json"), { count: activeDated.length, rows: activeDated });
writeJson(path.join(OUT, "ACTIVE_UNDATED.json"), { count: activeUndated.length, rows: activeUndated });
writeJson(path.join(OUT, "HISTORICAL.json"), { count: historical.length, rows: historical });
writeJson(path.join(OUT, "HOLD.json"), { count: hold.length, rows: hold.slice(0, 300) });
writeJson(path.join(OUT, "QUARANTINED.json"), {
  count: quarantined.length,
  rows: quarantined.slice(0, 200),
});
writeJson(path.join(OUT, "BY_STATUS_COUNTRY.json"), byStatusCountry);

const md = `# HIF V6 — Inventory Reconciliation (~194 claim)

**As of:** ${asOf}  
**Publication:** CUSTOMER_BLOCKED

## Reported claim
- Baseline qualified commercial targets: **${baselineQualified}**
- Waves 2–3 active dated (claimed): **11**
- Wave 4 clean undated signals (claimed): **44**
- **Reported sum: 194**

## Exact registry (hotel-key deduped)

| Classification | Count | Counts toward 1,000? |
|----------------|------:|----------------------|
| Active dated | ${activeDated.length} | YES |
| Active undated evidence-backed | ${activeUndated.length} | YES |
| Historical / superseded | ${historical.length} | NO (preserved) |
| Research candidate / HOLD | ${hold.length} | NO |
| Quarantined / invalid | ${quarantined.length} | NO |
| **Exact toward 1,000** | **${toward1000.length}** | — |

## Franchise vs management (toward 1,000)
- Franchise: ${franchiseVsMgmt.FRANCHISE}
- Management: ${franchiseVsMgmt.MANAGEMENT}
- Other: ${franchiseVsMgmt.OTHER}

## Country (toward 1,000)
${Object.entries(byCountry)
  .sort((a, b) => b[1] - a[1])
  .map(([c, n]) => `- ${c}: ${n}`)
  .join("\n") || "- (none)"}

## Variance
Reported 194 vs exact **${toward1000.length}** (delta ${toward1000.length - 194}).
${reconciliation.varianceVsReported194.explanation}

Baseline hotel-level list present for dedupe: **${reconciliation.baselineReference.baselineHotelListPresentInRepo}**
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_RECONCILIATION.md"), md);
console.log(JSON.stringify(reconciliation, null, 2));
