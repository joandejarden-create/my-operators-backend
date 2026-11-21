#!/usr/bin/env node
/**
 * Canonical HIF inventory reconcile after #519 + name-free scale-up.
 * Hotel-key + event-level dedupe. Does NOT naively add 192+89.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-canonical-inventory");
const asOf = new Date().toISOString().slice(0, 10);
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const load = (rel) => {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
};

function hotelKey(name, country = "US") {
  return `${String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()}|${String(country || "US").toUpperCase()}`;
}

function eventKey(name, country, date) {
  return `${hotelKey(name, country)}|${date || "NODATE"}`;
}

const byEvent = new Map();

function upsert(row, provenance) {
  const name = row.hotelName || row.propertyName || row.hotel || row.name;
  if (!name) return;
  const country = String(row.hotelCountry || row.country || "US").toUpperCase();
  const exp =
    row.expirationDate ||
    row.primaryDate ||
    row.futureExpirationDate ||
    row.verifiedEndDate ||
    row.latestCorroboratedExpirationDate ||
    null;
  const key = eventKey(name, country, exp);
  const historical = exp && String(exp) < asOf;
  const undated = !exp;
  const agreementUnresolved =
    row.agreementTypeResolved === false ||
    /COMBINED_LICENSE|UNRESOLVED|UNKNOWN/i.test(
      `${row.dateMeaning || ""} ${row.agreementType || ""} ${row.commercialList || ""}`,
    );
  const status = historical
    ? "HISTORICAL_OR_SUPERSEDED"
    : undated
      ? "ACTIVE_UNDATED_EVIDENCE_BACKED"
      : "ACTIVE_DATED";

  const next = {
    key,
    hotelName: name,
    hotelCountry: country,
    expirationDate: exp,
    status,
    agreementType: row.agreementType || null,
    dateMeaning: row.dateMeaning || null,
    agreementTypeResolved: !agreementUnresolved,
    commercialList: row.commercialList || null,
    sourceUrl: row.sourceUrl || row.source?.documentUrl || null,
    sourceFamily: row.sourceFamily || provenance,
    wave: row.wave || provenance,
    countsToward1000: status === "ACTIVE_DATED" || status === "ACTIVE_UNDATED_EVIDENCE_BACKED",
    provenance: [provenance],
  };

  const prev = byEvent.get(key);
  if (!prev) {
    byEvent.set(key, next);
    return;
  }
  prev.provenance = [...new Set([...(prev.provenance || []), provenance])];
  // Prefer resolved agreement type when upgrading
  if (!prev.agreementTypeResolved && next.agreementTypeResolved) {
    Object.assign(prev, next, { provenance: prev.provenance });
  }
}

// Baseline strict lists
for (const rel of [
  "reports/sec-edgar-commercial-targets-v1/BRAND_CONVERSION_TARGETS.json",
  "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS.json",
]) {
  const j = load(rel);
  for (const r of j?.targets || []) {
    upsert(
      {
        ...r,
        hotelName: r.hotel || r.hotelName,
        expirationDate: r.futureExpirationDate || r.verifiedEndDate,
        agreementType: r.agreementType,
        commercialList: rel.includes("MANAGEMENT")
          ? "MANAGEMENT_CONTRACT_TARGETS"
          : "BRAND_CONVERSION_TARGETS",
      },
      "baseline_strict",
    );
  }
}

// Wave2/34
const w2 = load("reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json");
for (const h of w2?.hotels || []) upsert(h, "wave2");
const fin = load("reports/sec-edgar-hif-v6-track-a-wave34/FINALIZED_OPPORTUNITY_INVENTORY.json");
for (const h of [
  ...(fin?.datedActiveOpportunities || []),
  ...(fin?.historicalExpirations || []),
  ...(fin?.evidenceBackedSignalsWithoutDates || []),
]) {
  upsert(h, "wave34");
}

// Yield wave clean undated
const yieldC = load("reports/sec-edgar-hif-v6-yield-wave/YIELD_PROSPECTS_CLEAN.json");
for (const h of yieldC?.hotels || []) upsert(h, "yield_wave");

// Source intelligence audit (#519)
const sia = load("reports/sec-edgar-hif-v6-source-intelligence-audit/NET_NEW_FROM_NAME_FREE.json");
for (const h of sia?.hotels || []) {
  upsert(
    {
      ...h,
      // Retag: audit originally labeled many as FRANCHISE; combined column → unresolved
      dateMeaning: "COMBINED_LICENSE_FRANCHISE_OPERATING_OR_MANAGEMENT",
      agreementType: "UNKNOWN_AGREEMENT",
      agreementTypeResolved: false,
      commercialList: "CONTRACT_DATE_AGREEMENT_TYPE_UNRESOLVED",
    },
    "sia_name_free_audit",
  );
}

// Scale-up
const scale = load("reports/sec-edgar-hif-v6-name-free-sec-scaleup/NET_NEW_EVENTS.json");
for (const h of scale?.hotels || []) upsert(h, "name_free_scaleup");

const rows = [...byEvent.values()];
const toward = rows.filter((r) => r.countsToward1000);
const activeDated = rows.filter((r) => r.status === "ACTIVE_DATED");
const undated = rows.filter((r) => r.status === "ACTIVE_UNDATED_EVIDENCE_BACKED");
const historical = rows.filter((r) => r.status === "HISTORICAL_OR_SUPERSEDED");
const unresolvedType = toward.filter((r) => !r.agreementTypeResolved);
const franchise = toward.filter((r) => /FRANCHISE/i.test(String(r.agreementType || "")) && r.agreementTypeResolved);
const management = toward.filter((r) => /MANAGE/i.test(String(r.agreementType || "")) && r.agreementTypeResolved);

const byCountry = {};
for (const r of toward) byCountry[r.hotelCountry] = (byCountry[r.hotelCountry] || 0) + 1;

const byProvenance = {};
for (const r of toward) {
  for (const p of r.provenance || []) byProvenance[p] = (byProvenance[p] || 0) + 1;
}

const priorTowardClaim = load(
  "reports/sec-edgar-hif-v6-inventory-reconciliation/RECONCILIATION_SUMMARY.json",
)?.exactRegistry?.countsToward1000Milestone;
const siaToward = (sia?.hotels || []).filter((h) => {
  const exp = h.expirationDate || h.primaryDate;
  return exp && String(exp) >= asOf;
}).length;

const summary = {
  asOf,
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  methodology:
    "Event-key dedupe (hotel|country|date). Combined CMBS columns counted as dated prospects with unresolved agreement type — not auto-franchise.",
  priorReportedToward1000: priorTowardClaim ?? 192,
  siaActiveDatedClaimed: 89,
  naiveSumWouldBe: (priorTowardClaim ?? 192) + 89,
  exactCanonical: {
    distinctEvents: rows.length,
    ACTIVE_DATED: activeDated.length,
    ACTIVE_UNDATED_EVIDENCE_BACKED: undated.length,
    HISTORICAL_OR_SUPERSEDED: historical.length,
    countsToward1000: toward.length,
    agreementTypeUnresolvedAmongToward1000: unresolvedType.length,
    verifiedFranchiseResolved: franchise.length,
    verifiedManagementResolved: management.length,
    byCountry,
    byProvenance,
  },
  deltaVsNaive: toward.length - ((priorTowardClaim ?? 192) + 89),
  note:
    "Naive 192+89 overstates when SIA hotels overlap baseline events or when historical baseline dates are excluded; this reconcile is authoritative.",
};

writeJson(path.join(OUT, "CANONICAL_SUMMARY.json"), summary);
writeJson(path.join(OUT, "TOWARD_1000.json"), { count: toward.length, rows: toward });
writeJson(path.join(OUT, "ACTIVE_DATED.json"), { count: activeDated.length, rows: activeDated });
writeJson(path.join(OUT, "HISTORICAL.json"), { count: historical.length, rows: historical });
writeJson(path.join(OUT, "AGREEMENT_TYPE_UNRESOLVED.json"), {
  count: unresolvedType.length,
  rows: unresolvedType,
});

console.log(JSON.stringify(summary, null, 2));
