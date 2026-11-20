/**
 * Consolidated Marriott & Hilton Historical Contract Inventory (CSV + JSON).
 */

import { DISCOVERY_STATUS, HISTORICAL_APPLICABILITY } from "./contract.js";
import { detectBrandFamily, brandFamilySummary } from "./brand-families-v2.js";

export const MARRIOTT_HILTON_INVENTORY_V2 = "marriott-hilton-historical-contract-inventory-v2";

const COLUMNS = [
  "hotelName",
  "historicalNames",
  "city",
  "stateRegion",
  "country",
  "brandFamily",
  "historicalBrand",
  "currentBrand",
  "agreementType",
  "contractSigningDate",
  "effectiveDate",
  "initialExpiration",
  "currentlyVerifiedExpiration",
  "renewalOptions",
  "terminationDate",
  "owner",
  "operator",
  "secRegistrant",
  "cik",
  "sourceFilingUrl",
  "filingDate",
  "evidenceStatus",
  "historicalApplicability",
  "hpcMatchStatus",
  "hpcRecordId",
  "persistenceStatus",
  "conversionOpportunityStatus",
  "discoveryId",
];

function pickFinding(findings = [], clsPrefix) {
  return findings.find((f) => String(f.verificationClass || "").includes(clsPrefix));
}

export function buildMarriottHiltonInventory(inventoryHotels = [], opts = {}) {
  const rows = [];
  for (const h of inventoryHotels) {
    const brand =
      h.brandFamily ||
      detectBrandFamily([h.hotelName, h.historicalBrand, h.brandHint].filter(Boolean).join(" "));
    const family = typeof brand === "string" ? brand : brand.family;
    if (opts.marriottHiltonOnly !== false) {
      if (family !== "MARRIOTT" && family !== "HILTON") continue;
    }
    const findings = h.contractFindings || [];
    const exp = pickFinding(findings, "EXPIRATION") || findings.find((f) => f.initialExpiration);
    const sign = pickFinding(findings, "SIGNING");
    const eff = pickFinding(findings, "EFFECTIVE");
    const renew = pickFinding(findings, "RENEWAL");
    const term = pickFinding(findings, "TERMINATION");
    const applicability =
      exp?.historicalApplicability ||
      findings[0]?.historicalApplicability ||
      HISTORICAL_APPLICABILITY.REQUIRES_FURTHER_RESEARCH;

    rows.push({
      hotelName: h.hotelName,
      historicalNames: (h.historicalNames || []).join("; "),
      city: h.city || "",
      stateRegion: h.stateRegion || "",
      country: h.country || "",
      brandFamily: family || "",
      historicalBrand: h.historicalBrand || (typeof brand === "object" ? brand.brandLabel : "") || "",
      currentBrand: h.currentBrand || "",
      agreementType: exp?.agreementType || findings[0]?.agreementType || "",
      contractSigningDate: sign?.primaryDate || "",
      effectiveDate: eff?.primaryDate || "",
      initialExpiration: exp?.initialExpiration || exp?.primaryDate || "",
      currentlyVerifiedExpiration:
        applicability === HISTORICAL_APPLICABILITY.CURRENTLY_VERIFIED
          ? exp?.currentlyVerifiedExpiration || exp?.primaryDate || ""
          : "",
      renewalOptions: renew?.primaryDate || renew?.snippet?.slice(0, 80) || "",
      terminationDate: term?.primaryDate || "",
      owner: h.owner || "",
      operator: h.operator || "",
      secRegistrant: h.registrant || "",
      cik: h.cik || "",
      sourceFilingUrl: h.sourceUrl || exp?.source?.documentUrl || "",
      filingDate: exp?.source?.filingDate || "",
      evidenceStatus: exp?.verificationClass || findings[0]?.verificationClass || "DISCOVERED_ONLY",
      historicalApplicability: applicability,
      hpcMatchStatus: h.hpcMatchStatus || DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
      hpcRecordId: h.hpcRecordId || "",
      persistenceStatus: h.persistenceStatus || "INVENTORY_ONLY",
      conversionOpportunityStatus: h.conversionOpportunityStatus || "NOT_ASSESSED",
      discoveryId: h.discoveryId || "",
    });
  }

  const marriott = rows.filter((r) => r.brandFamily === "MARRIOTT");
  const hilton = rows.filter((r) => r.brandFamily === "HILTON");

  return {
    version: MARRIOTT_HILTON_INVENTORY_V2,
    columns: COLUMNS,
    generatedAt: new Date().toISOString(),
    rowCount: rows.length,
    summaries: {
      marriott: {
        hotels: marriott.length,
        withExpiration: marriott.filter((r) => r.initialExpiration).length,
        historicallyVerified: marriott.filter(
          (r) => r.historicalApplicability === HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
        ).length,
        currentlyVerified: marriott.filter(
          (r) => r.historicalApplicability === HISTORICAL_APPLICABILITY.CURRENTLY_VERIFIED,
        ).length,
        inHpc: marriott.filter((r) => r.hpcMatchStatus === DISCOVERY_STATUS.DISCOVERED_MATCHED_HPC)
          .length,
        missingHpc: marriott.filter(
          (r) => r.hpcMatchStatus === DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
        ).length,
      },
      hilton: {
        hotels: hilton.length,
        withExpiration: hilton.filter((r) => r.initialExpiration).length,
        historicallyVerified: hilton.filter(
          (r) => r.historicalApplicability === HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
        ).length,
        currentlyVerified: hilton.filter(
          (r) => r.historicalApplicability === HISTORICAL_APPLICABILITY.CURRENTLY_VERIFIED,
        ).length,
        inHpc: hilton.filter((r) => r.hpcMatchStatus === DISCOVERY_STATUS.DISCOVERED_MATCHED_HPC)
          .length,
        missingHpc: hilton.filter(
          (r) => r.hpcMatchStatus === DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
        ).length,
      },
      brandFamilySummary: brandFamilySummary(
        rows.map((r) => ({ brandFamily: r.brandFamily, hotelName: r.hotelName })),
      ),
    },
    rows,
    currentView: rows.filter(
      (r) =>
        r.historicalApplicability === HISTORICAL_APPLICABILITY.CURRENTLY_VERIFIED ||
        r.historicalApplicability === HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
    ),
    historicalView: rows,
  };
}

export function marriottHiltonInventoryToCsv(report) {
  const cols = report.columns || COLUMNS;
  const esc = (v) => {
    const s = String(v ?? "");
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const lines = [cols.join(",")];
  for (const row of report.rows || []) {
    lines.push(cols.map((c) => esc(row[c])).join(","));
  }
  return lines.join("\n") + "\n";
}
