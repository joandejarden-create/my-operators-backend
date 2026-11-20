#!/usr/bin/env node
/**
 * Migrate V1/V2/V3 contract findings → unique hotels / agreements / milestones (dry-run).
 * No Airtable writes. No HPC creates.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  aggregateFindingsToHotels,
  isUnitedStatesHotel,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v4-us-pilot-selection-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-contract-timing-v4/MIGRATION_DRY_RUN.json");

function sha(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 16);
}

function loadAll() {
  const rows = [];
  const v3 = path.join(ROOT, "reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json");
  if (fs.existsSync(v3)) {
    for (const f of JSON.parse(fs.readFileSync(v3, "utf8")).findings || []) rows.push({ ...f, wave: "V3" });
  }
  const v2 = path.join(ROOT, "reports/sec-edgar-contract-timing-v2/V2_FINDINGS_INCREMENTAL.json");
  if (fs.existsSync(v2)) {
    for (const f of JSON.parse(fs.readFileSync(v2, "utf8")).combined || []) rows.push({ ...f, wave: "V2" });
  }
  const v1 = path.join(ROOT, "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json");
  if (fs.existsSync(v1)) {
    const j = JSON.parse(fs.readFileSync(v1, "utf8"));
    for (const f of [...(j.verified || []), ...(j.probable || [])]) rows.push({ ...f, wave: "V1" });
  }
  return rows;
}

function agreementKey(f) {
  return [
    String(f.hotelName || "").toLowerCase(),
    String(f.city || "").toLowerCase(),
    String(f.stateRegion || "").toLowerCase(),
    String(f.agreementType || f.agreementFamily || "FRANCHISE").toUpperCase(),
    String(f.brandFamily || f.brandLabel || "").toLowerCase(),
  ].join("|");
}

function milestoneKey(f) {
  return [agreementKey(f), f.dateMeaning, f.primaryDate].join("|");
}

const findings = loadAll();
const contractFindings = findings.filter(
  (f) => f.isHotelContractDate && !f.isLoanMaturity && f.primaryDate && f.hotelName,
);
const loanIsolated = findings.filter((f) => f.isLoanMaturity);
const hotels = aggregateFindingsToHotels(contractFindings);

const agreements = new Map();
const milestones = new Map();
const rejected = [];
for (const f of contractFindings) {
  if (!f.hotelAttributed || f.portfolioWide) {
    rejected.push({ findingId: f.findingId, reason: "portfolio_or_unattributed", wave: f.wave });
    continue;
  }
  const ak = agreementKey(f);
  if (!agreements.has(ak)) {
    agreements.set(ak, {
      contractId: `ctr_${sha(ak)}`,
      hotelName: f.hotelName,
      city: f.city,
      stateRegion: f.stateRegion,
      country: isUnitedStatesHotel(f) ? "United States" : f.country || "UNKNOWN",
      agreementType: String(f.agreementType || "FRANCHISE").includes("MANAGEMENT")
        ? "MANAGEMENT"
        : String(f.agreementType || "").includes("LEASE")
          ? "LEASE"
          : String(f.agreementType || "").includes("LICENSE")
            ? "LICENSE"
            : "FRANCHISE",
      brandFamily: f.brandFamily || null,
      sourceAccessions: new Set(),
      findingIds: [],
      waves: new Set(),
    });
  }
  const a = agreements.get(ak);
  a.findingIds.push(f.findingId);
  a.waves.add(f.wave);
  if (f.source?.accessionNumber) a.sourceAccessions.add(f.source.accessionNumber);

  const mk = milestoneKey(f);
  if (!milestones.has(mk)) {
    milestones.set(mk, {
      milestoneId: `ms_${sha(mk)}`,
      contractId: a.contractId,
      primaryDate: f.primaryDate,
      dateMeaning: f.dateMeaning,
      currentStatus: f.currentStatus || "EXPIRED_STATUS_UNCONFIRMED",
      publicationState: "CUSTOMER_BLOCKED",
      waves: new Set([f.wave]),
      findingIds: [f.findingId],
      sourceAccessions: new Set(f.source?.accessionNumber ? [f.source.accessionNumber] : []),
    });
  } else {
    const m = milestones.get(mk);
    m.findingIds.push(f.findingId);
    m.waves.add(f.wave);
    if (f.source?.accessionNumber) m.sourceAccessions.add(f.source.accessionNumber);
  }
}

const report = {
  module: "V4_CONTRACT_MIGRATION_DRY_RUN",
  at: new Date().toISOString(),
  mode: "DRY_RUN",
  noAirtableWrites: true,
  noHpcCreates: true,
  publicationState: "CUSTOMER_BLOCKED",
  input: {
    findingsLoaded: findings.length,
    contractFindings: contractFindings.length,
    loanMaturityIsolated: loanIsolated.length,
    reportedTotalHint: "57+19+405=481 reported finding total — not automatic unique agreements",
  },
  unique: {
    physicalHotels: hotels.length,
    agreements: agreements.size,
    milestones: milestones.size,
    rejectedOrAmbiguous: rejected.length,
  },
  byAgreementType: [...agreements.values()].reduce((a, x) => {
    a[x.agreementType] = (a[x.agreementType] || 0) + 1;
    return a;
  }, {}),
  byStatus: [...milestones.values()].reduce((a, x) => {
    a[x.currentStatus] = (a[x.currentStatus] || 0) + 1;
    return a;
  }, {}),
  proposedAirtableOps: {
    inventoryUpserts: hotels.length,
    contractCreates: agreements.size,
    milestoneCreates: milestones.size,
    findingLinks: contractFindings.length,
    note: "Execute only after HI Hotel Contracts/Milestones Meta apply authorization + Research Mode PERSIST",
  },
  sampleAgreements: [...agreements.values()].slice(0, 8).map((a) => ({
    contractId: a.contractId,
    hotelName: a.hotelName,
    city: a.city,
    stateRegion: a.stateRegion,
    agreementType: a.agreementType,
    waves: [...a.waves],
  })),
  rejectedSample: rejected.slice(0, 10),
};

fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ out: OUT, unique: report.unique, proposed: report.proposedAirtableOps }, null, 2));
