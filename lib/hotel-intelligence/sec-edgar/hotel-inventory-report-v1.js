/**
 * Hotel-by-hotel research report (CSV + JSON).
 */

import { DISCOVERY_STATUS } from "./contract.js";

export const HOTEL_INVENTORY_REPORT_VERSION = "sec-edgar-hotel-inventory-report-v1";

const COLUMNS = [
  "hotelName",
  "city",
  "stateRegion",
  "country",
  "registrant",
  "cik",
  "sourceFiling",
  "sourceUrl",
  "agreementType",
  "verifiedSigningDate",
  "verifiedEffectiveDate",
  "verifiedExpirationDate",
  "renewalOrTermination",
  "evidenceStatus",
  "hpcMatchStatus",
  "hpcRecordId",
  "persistenceStatus",
  "recommendedNextAction",
  "discoveryId",
  "mentionCount",
];

function findingDates(findings = []) {
  const out = {
    agreementType: null,
    verifiedSigningDate: null,
    verifiedEffectiveDate: null,
    verifiedExpirationDate: null,
    renewalOrTermination: null,
    evidenceStatus: "DISCOVERED_ONLY",
  };
  for (const f of findings) {
    const cls = String(f.verificationClass || "");
    if (!out.agreementType && f.agreementType) out.agreementType = f.agreementType;
    if (cls === "VERIFIED_SIGNING_DATE" && f.primaryDate) out.verifiedSigningDate = f.primaryDate;
    if (cls === "VERIFIED_EFFECTIVE_DATE" && f.primaryDate) out.verifiedEffectiveDate = f.primaryDate;
    if (cls === "VERIFIED_EXPIRATION" && f.primaryDate) out.verifiedExpirationDate = f.primaryDate;
    if (
      (cls === "VERIFIED_RENEWAL_OPTION" ||
        cls === "VERIFIED_RENEWAL_NOTICE_DEADLINE" ||
        cls === "VERIFIED_TERMINATION") &&
      !out.renewalOrTermination
    ) {
      out.renewalOrTermination = `${cls}:${f.primaryDate || "see_snippet"}`;
    }
    if (cls.startsWith("VERIFIED_")) out.evidenceStatus = cls;
    else if (out.evidenceStatus === "DISCOVERED_ONLY" && cls) out.evidenceStatus = cls;
  }
  return out;
}

function nextAction(hotel) {
  const st = hotel.hpcMatchStatus || DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC;
  if (st === DISCOVERY_STATUS.DISCOVERED_MATCHED_HPC) {
    return (hotel.contractFindings || []).some((f) =>
      String(f.verificationClass || "").startsWith("VERIFIED_"),
    )
      ? "Run persistence apply for matched verified contracts (read-back required)"
      : "Matched HPC — continue exhibit research for contract dates";
  }
  if (st === DISCOVERY_STATUS.CONTRACT_VERIFIED_PENDING_HPC) {
    return "Retain verified contracts in inventory; reconcile when hotel enters HPC";
  }
  if (st === DISCOVERY_STATUS.MATCH_AMBIGUOUS) {
    return "Manual identity review — do not auto-match or apply";
  }
  if (st === DISCOVERY_STATUS.PORTFOLIO_ONLY_EVIDENCE) {
    return "Portfolio-wide disclosure — do not attribute to this hotel without property evidence";
  }
  return "Keep in discovery inventory; no HPC create; revisit on HPC expansion";
}

export function buildHotelInventoryRows(inventory, extras = {}) {
  const portfolioOnly = extras.portfolioOnlyFindings || [];
  const unattributed = extras.unattributedFindings || [];
  const rows = [];

  for (const h of inventory.hotels || []) {
    const dates = findingDates(h.contractFindings || []);
    rows.push({
      hotelName: h.hotelName,
      city: h.city || "",
      stateRegion: h.stateRegion || "",
      country: h.country || "",
      registrant: h.registrant || "",
      cik: h.cik || "",
      sourceFiling: h.sourceFiling || "",
      sourceUrl: h.sourceUrl || "",
      agreementType: dates.agreementType || "",
      verifiedSigningDate: dates.verifiedSigningDate || "",
      verifiedEffectiveDate: dates.verifiedEffectiveDate || "",
      verifiedExpirationDate: dates.verifiedExpirationDate || "",
      renewalOrTermination: dates.renewalOrTermination || "",
      evidenceStatus: dates.evidenceStatus,
      hpcMatchStatus: h.hpcMatchStatus || DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
      hpcRecordId: h.hpcRecordId || "",
      persistenceStatus: h.persistenceStatus || "INVENTORY_ONLY",
      recommendedNextAction: nextAction(h),
      discoveryId: h.discoveryId,
      mentionCount: h.mentionCount || 1,
    });
  }

  // Portfolio-only / unattributed contract language as explicit non-hotel rows
  for (const f of portfolioOnly) {
    rows.push({
      hotelName: "",
      city: "",
      stateRegion: "",
      country: "",
      registrant: f.source?.companyName || "",
      cik: f.source?.cik || "",
      sourceFiling: f.source?.formType || "",
      sourceUrl: f.source?.documentUrl || "",
      agreementType: f.agreementType || "",
      verifiedSigningDate: "",
      verifiedEffectiveDate: "",
      verifiedExpirationDate: "",
      renewalOrTermination: "",
      evidenceStatus: "PORTFOLIO_ONLY_EVIDENCE",
      hpcMatchStatus: DISCOVERY_STATUS.PORTFOLIO_ONLY_EVIDENCE,
      hpcRecordId: "",
      persistenceStatus: "NOT_PERSISTABLE",
      recommendedNextAction: "Do not attribute to individual hotels",
      discoveryId: "",
      mentionCount: 1,
    });
  }
  for (const f of unattributed) {
    rows.push({
      hotelName: "",
      city: "",
      stateRegion: "",
      country: "",
      registrant: f.source?.companyName || "",
      cik: f.source?.cik || "",
      sourceFiling: f.source?.formType || "",
      sourceUrl: f.source?.documentUrl || "",
      agreementType: f.agreementType || "",
      verifiedSigningDate: "",
      verifiedEffectiveDate: "",
      verifiedExpirationDate: "",
      renewalOrTermination: "",
      evidenceStatus: "UNATTRIBUTED_CONTRACT_LANGUAGE",
      hpcMatchStatus: DISCOVERY_STATUS.UNATTRIBUTED_CONTRACT_LANGUAGE,
      hpcRecordId: "",
      persistenceStatus: "NOT_PERSISTABLE",
      recommendedNextAction: "Needs hotel attribution before verification/persist",
      discoveryId: "",
      mentionCount: 1,
    });
  }

  return {
    version: HOTEL_INVENTORY_REPORT_VERSION,
    columns: COLUMNS,
    rowCount: rows.length,
    rows,
  };
}

export function hotelInventoryToCsv(report) {
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
