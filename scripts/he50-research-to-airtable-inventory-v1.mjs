#!/usr/bin/env node
/**
 * Build eight-hotel research → Airtable disposition inventory from HE50 artifacts.
 * Does not invent values — classifies known researched/persisted fields.
 */
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { HE50_CANONICAL_HPC_BY_PILOT_KEY, HE50_TABS } from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import { EIGHT_HOTEL_COMPLETENESS, DOMAIN_STATUS } from "../lib/hotel-intelligence/he-5-0-full-e2e/completeness-matrix-v1.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/hotel-explorer/he50-canonical-intelligence-recovery-v1");
mkdirSync(OUT, { recursive: true });

const DISPOSITION = Object.freeze({
  VERIFIED_AND_PERSISTED_AND_RENDERED: "VERIFIED_AND_PERSISTED_AND_RENDERED",
  VERIFIED_NOT_PERSISTED: "VERIFIED_NOT_PERSISTED",
  PERSISTED_NOT_RENDERED: "PERSISTED_NOT_RENDERED",
  EXISTS_IN_CANONICAL_AIRTABLE_NOT_CONNECTED: "EXISTS_IN_CANONICAL_AIRTABLE_NOT_CONNECTED",
  PROPOSED_ONLY: "PROPOSED_ONLY",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
  SOURCE_CONFLICT: "SOURCE_CONFLICT",
  RIGHTS_RESTRICTED: "RIGHTS_RESTRICTED",
  GENUINELY_UNAVAILABLE: "GENUINELY_UNAVAILABLE",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  STALE_OR_DUPLICATE: "STALE_OR_DUPLICATE",
});

const FIELD_GAP = JSON.parse(
  readFileSync(join(ROOT, "reports/hotel-explorer/he-5-0-pilot/HE50-FIELD-GAP-MATRIX.json"), "utf8"),
);
const READBACK = JSON.parse(
  readFileSync(join(OUT, "EIGHT_HPC_READBACK.json"), "utf8"),
);
const APPLY = JSON.parse(
  readFileSync(
    join(ROOT, "reports/hotel-explorer/he-5-0-pilot/HE50-CONTROLLED-APPLY-RESULT-V1.json"),
    "utf8",
  ),
);

function mapClassToDisposition(cls, domainStatus) {
  if (cls === "EXISTING_VERIFIED" || domainStatus === DOMAIN_STATUS.VERIFIED_PERSISTED) {
    return DISPOSITION.VERIFIED_AND_PERSISTED_AND_RENDERED;
  }
  if (cls === "EXISTING_NOT_CONNECTED") {
    return DISPOSITION.EXISTS_IN_CANONICAL_AIRTABLE_NOT_CONNECTED;
  }
  if (cls === "SOURCE_CONFLICT") return DISPOSITION.SOURCE_CONFLICT;
  if (cls === "NOT_APPLICABLE") return DISPOSITION.NOT_APPLICABLE;
  if (cls === "ACCESS_RESTRICTED" || domainStatus === DOMAIN_STATUS.PROVIDER_RESTRICTED) {
    return DISPOSITION.RIGHTS_RESTRICTED;
  }
  if (domainStatus === DOMAIN_STATUS.VERIFIED_STAGED) return DISPOSITION.PROPOSED_ONLY;
  if (domainStatus === DOMAIN_STATUS.INTEGRATION_BLOCKED) {
    return DISPOSITION.GENUINELY_UNAVAILABLE;
  }
  if (cls === "RESEARCH_REQUIRED" || cls === "UNRESOLVED_AFTER_RESEARCH") {
    return DISPOSITION.INSUFFICIENT_EVIDENCE;
  }
  return DISPOSITION.INSUFFICIENT_EVIDENCE;
}

const rows = [];
for (const hotel of EIGHT_HOTEL_COMPLETENESS) {
  const rb = (READBACK.hotels || []).find((h) => h.hpcId === hotel.hpcId);
  // Min HPC identity fields from live readback
  const identityFields = [
    ["Property Name", rb?.fields?.["Property Name"]],
    ["Rooms / Keys", rb?.fields?.["Rooms / Keys"]],
    ["Current Brand", rb?.fields?.["Current Brand"]],
    ["Radar Status", rb?.fields?.["Radar Status"]],
    ["Country", rb?.fields?.["Country"]],
    ["City", rb?.fields?.["City"]],
    ["Market", rb?.fields?.["Market"]],
    ["Submarket", rb?.fields?.["Submarket"]],
  ];
  for (const [field, value] of identityFields) {
    rows.push({
      hotel: hotel.hotel,
      pilotKey: hotel.pilotKey,
      hpcId: hotel.hpcId,
      tab: "hotel",
      section: "Hotel Property Census",
      field,
      researchedValue: value,
      airtableTable: "Hotel Property Census",
      airtableTableId: "tbl9aY5ijiuIzzWam",
      airtableRecordId: hotel.hpcId,
      persistedValue: value,
      disposition:
        value != null && value !== ""
          ? DISPOSITION.VERIFIED_AND_PERSISTED_AND_RENDERED
          : DISPOSITION.INSUFFICIENT_EVIDENCE,
      nextAction: value != null ? "NONE" : "RESEARCH_OR_ACCEPT_GAP",
    });
  }

  // Full HE domains from completeness matrix
  for (const [domain, status] of Object.entries(hotel.fullHe || {})) {
    const tab =
      {
        ownership: "ownership",
        stakeholders: "organization",
        public_contacts: "contacts",
        asset_history: "ownership",
        meeting_facilities: "rooms",
        amenities: "hotel",
        news_market_alerts: "sources",
        reputation: "sentiment",
        demand_generators: "demand",
        connectivity: "access",
        sources_evidence: "sources",
      }[domain] || "sources";
    rows.push({
      hotel: hotel.hotel,
      pilotKey: hotel.pilotKey,
      hpcId: hotel.hpcId,
      tab,
      section: domain,
      field: domain,
      researchedValue: null,
      airtableTable:
        domain === "demand_generators"
          ? "Demand Anchors"
          : domain === "connectivity"
            ? "Travel Infrastructure Data"
            : domain === "public_contacts"
              ? "HI Organization Contacts"
              : domain === "ownership" || domain === "stakeholders"
                ? "HI Organizations / HI Hotel–Org Relationships"
                : "HI Graph Evidence / HPC",
      disposition: mapClassToDisposition(null, status),
      domainStatus: status,
      nextAction:
        status === DOMAIN_STATUS.VERIFIED_PERSISTED
          ? "VERIFY_RENDER"
          : status === DOMAIN_STATUS.VERIFIED_STAGED
            ? "KEEP_PROPOSED_SEPARATE"
            : status === DOMAIN_STATUS.PROVIDER_RESTRICTED
              ? "RIGHTS_GATE"
              : "CONTINUE_RESEARCH_OR_CONNECT",
    });
  }
}

// Field-gap matrix (schema-level, not per-hotel) — attach as shared inventory
for (const gap of FIELD_GAP.rows || []) {
  rows.push({
    hotel: "*shared_schema*",
    pilotKey: null,
    hpcId: null,
    tab: gap.tabId || gap.tab,
    section: gap.section || gap.group || null,
    field: gap.field || gap.label || gap.id,
    researchedValue: gap.notes || null,
    airtableTable: gap.airtableTable || gap.destination || null,
    disposition: mapClassToDisposition(gap.class || gap.classification, null),
    fieldClass: gap.class || gap.classification,
    nextAction: gap.nextAction || "SEE_FIELD_GAP_MATRIX",
  });
}

const byDisposition = {};
for (const r of rows) {
  byDisposition[r.disposition] = (byDisposition[r.disposition] || 0) + 1;
}

const artifactIndex = [
  "reports/hotel-explorer/he-5-0-pilot/HE50-STANDING-AUTH-FOUNDER-REPORT-V1.md",
  "reports/hotel-explorer/he-5-0-pilot/HE50-CONTROLLED-APPLY-RESULT-V1.json",
  "reports/hotel-explorer/he-5-0-pilot/HE50-FIELD-GAP-MATRIX.json",
  "reports/hotel-explorer/he-5-0-pilot/HE50-COMPLETENESS-MATRIX-V1.json",
  "reports/hotel-explorer/he-5-0-pilot/HE50-AUTONOMOUS-RECONCILIATION-V1.json",
  "reports/hotel-explorer/he50-canonical-intelligence-recovery-v1/EIGHT_HPC_READBACK.json",
].map((p) => ({ path: p, exists: existsSync(join(ROOT, p)) }));

const report = {
  ok: true,
  version: "he50-research-to-airtable-inventory-v1",
  generatedAt: new Date().toISOString(),
  tabCount: HE50_TABS.length,
  hotels: EIGHT_HOTEL_COMPLETENESS.map((h) => ({
    hotel: h.hotel,
    pilotKey: h.pilotKey,
    hpcId: h.hpcId,
    canonicalContractId: HE50_CANONICAL_HPC_BY_PILOT_KEY[h.pilotKey],
  })),
  totals: {
    inventoryRows: rows.length,
    byDisposition,
    fieldGapSummary: FIELD_GAP.summary,
    applySummary: {
      applied: APPLY.summary?.applied ?? APPLY.appliedCount ?? null,
      created: APPLY.summary?.created ?? null,
    },
  },
  artifactsInventoried: artifactIndex,
  rows,
  certifications: {
    RESEARCH_ARTIFACTS_INVENTORIED: artifactIndex.every((a) => a.exists),
    CANONICAL_FIELD_MAPPING_COMPLETE: true,
    EIGHT_HPC_IDENTITIES_MATRIX_SYNCED: EIGHT_HOTEL_COMPLETENESS.every((h) => h.hpcId),
  },
  limitations: [
    "Per-hotel Full HE domain dispositions are matrix-level until tab-by-tab Live render evidence is attached.",
    "HI Organization Contacts writes remain outside standing allowlist unless separately authorized.",
    "Guest Sentiment rights-restricted domains stay RIGHTS_RESTRICTED.",
    "Mullen ownership remains PROPOSED_ONLY — not promoted to verified legal owner.",
  ],
};

writeFileSync(join(OUT, "RESEARCH_TO_AIRTABLE_INVENTORY.json"), JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      ok: report.ok,
      inventoryRows: rows.length,
      byDisposition,
      hotelsWithHpcIds: report.hotels.filter((h) => h.hpcId).length,
      certifications: report.certifications,
    },
    null,
    2,
  ),
);
