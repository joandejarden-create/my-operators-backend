#!/usr/bin/env node
/**
 * Track B global historical waves — assemble verified dossiers, match HPC, write the matrix.
 * Does not write Airtable (see hif-track-b-global-waves-persist.mjs).
 * CUSTOMER_BLOCKED. $0 metered.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL, HPC_TABLE_ID } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { isTrackAReservedId } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-historical-foundation-v1.js";
import {
  isPortfolioOrArtifactName,
  timingBucket,
  lackOfNewerFilingProvesCurrent,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";
import {
  GLOBAL_WAVE_DOSSIERS,
  STAGED_PORTFOLIO_FINDINGS,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-wave-dossiers-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-global-historical-waves");
const AS_OF = "2026-11-21";
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;

function loadJson(rel) {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

function secDossiers() {
  const files = [
    "reports/sec-edgar-hif-v6-name-free-sec-continuous/NET_NEW_EVENTS_WAVE_20261121_14DOC.json",
    "reports/sec-edgar-hif-v6-name-free-sec-continuous/NET_NEW_EVENTS_WAVE_20261121_8DOC.json",
  ];
  const rows = [];
  for (const rel of files) {
    const j = loadJson(rel);
    for (const h of j?.hotels || []) rows.push(h);
  }
  const out = [];
  const seen = new Set();
  for (const h of rows) {
    const key = `${h.hotelName}|${h.expirationDate}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const artifact = isPortfolioOrArtifactName(h.hotelName);
    const slug = String(h.hotelName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_|_$/g, "")
      .slice(0, 48);
    const vintage = /\/000\d{7}(\d{2})/.test(h.sourceUrl || "")
      ? `20${(h.sourceUrl.match(/\/000\d{7}(\d{2})/) || [])[1] || "00"}`
      : null;
    out.push({
      wave: "B1",
      discoveryPropertyId: `disc_tb_sec_${slug}`,
      findingId: `find_tb_sec_${slug}_${String(h.expirationDate || "nodate").replace(/-/g, "")}`,
      hotelName: h.hotelName,
      hotelNameNormalized: String(h.hotelName || "").toLowerCase(),
      physicalCountry: h.hotelCountry || "US",
      identityClass: artifact ? "EXTRACTION_ARTIFACT" : "VERIFIED_PROPERTY",
      evidenceConfidence: artifact ? "LOW" : "MEDIUM",
      persistEligible: !artifact,
      contractEligible: false,
      agreementType: null,
      agreementTypeResolved: false,
      expirationDate: h.expirationDate || null,
      commercialCategory: "CONTRACT_EXPIRATION_UNRESOLVED",
      findingType: "CONTRACT_MILESTONE",
      sourceUrl: h.sourceUrl,
      sourceVintage: vintage,
      sourceFamily: "SEC_CMBS",
      qualificationLevel: h.qualificationLevel,
      evidenceSnippet: h.commercialThesis,
      reconciliation: {
        currentStatusClass: h.qualificationLevel === "HISTORICAL_OR_SUPERSEDED"
          ? "HISTORICALLY_VERIFIED_CURRENT_UNKNOWN"
          : "HISTORICALLY_VERIFIED_CURRENT_UNKNOWN",
        basis: "CMBS_SCHEDULE_DATE_IS_NOT_CURRENT_STATUS",
      },
      supersessionGap:
        "Combined license/franchise/operating/management column. Agreement type left unresolved. A 2017-vintage schedule date is not treated as a currently effective expiration.",
      hpcQuery: artifact ? null : String(h.hotelName || "").toLowerCase(),
      commerciallyAvailable: false,
    });
  }
  return out;
}

async function hpcNameField() {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${GRAPH_BASE_ID_CANONICAL}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`HPC_META_${res.status}`);
  const table = (json.tables || []).find((t) => t.id === HPC_TABLE_ID);
  const names = new Set((table?.fields || []).map((f) => f.name));
  const nameField = ["Property Name", "Canonical Property Name", "name", "Name", "Hotel Name"].find((n) => names.has(n));
  const countryField = ["Country", "country"].find((n) => names.has(n));
  const cityField = ["City", "city"].find((n) => names.has(n));
  if (!nameField) throw new Error(`HPC_NAME_FIELD_MISSING:${[...names].slice(0, 12).join(",")}`);
  return { nameField, countryField, cityField };
}

function countryMatches(recordCountry, expected) {
  const c = String(recordCountry || "").toLowerCase();
  const map = {
    MX: ["mexico", "méxico", "mx"],
    BR: ["brazil", "brasil", "br"],
    DO: ["dominican", "do"],
    JM: ["jamaica", "jm"],
    US: ["united states", "usa", "us"],
    CA: ["canada", "ca"],
    GB: ["united kingdom", "uk", "gb", "england"],
  };
  return (map[expected] || [String(expected || "").toLowerCase()]).some((x) => c.includes(x));
}

async function searchHpc(fields, query, country) {
  if (!query) return { hpcMatchStatus: "NOT_SEARCHED", hpcRecordId: null };
  const q = query.replace(/"/g, "").slice(0, 40);
  const formula = `FIND("${q}", LOWER({${fields.nameField}}&""))`;
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "8", pageSize: "8" });
  if (fields.countryField) qs.append("fields[]", fields.countryField);
  if (fields.cityField) qs.append("fields[]", fields.cityField);
  qs.append("fields[]", fields.nameField);
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) return { hpcMatchStatus: "SEARCH_ERROR", hpcError: json?.error?.type || "HTTP", hpcRecordId: null };
  const hits = (json.records || []).filter((r) =>
    fields.countryField ? countryMatches(r.fields?.[fields.countryField], country) : true,
  );
  if (hits.length === 1) {
    return {
      hpcMatchStatus: "MATCHED_UNIQUE_NAME_AND_COUNTRY",
      hpcRecordId: hits[0].id,
      hpcName: hits[0].fields?.[fields.nameField] || null,
    };
  }
  if (hits.length > 1) return { hpcMatchStatus: "AMBIGUOUS_MULTIPLE", hpcRecordId: null, hpcHitCount: hits.length };
  return { hpcMatchStatus: "DISCOVERED_NOT_IN_HPC", hpcRecordId: null, hpcNameHitsUnfiltered: (json.records || []).length };
}

function matrix(dossiers) {
  const countries = {};
  for (const d of dossiers) {
    const c = d.physicalCountry || "UNKNOWN";
    if (!countries[c]) {
      countries[c] = {
        country: c,
        hotels: 0,
        stagedPortfolios: 0,
        byAgreementType: {},
        byTiming: {},
        byStatus: {},
        byWave: {},
      };
    }
    const row = countries[c];
    row.byWave[d.wave] = (row.byWave[d.wave] || 0) + 1;
    if (d.persistEligible && d.identityClass === "VERIFIED_PROPERTY") row.hotels += 1;
    else row.stagedPortfolios += 1;
    const t = d.agreementType || (d.agreementTypeResolved === false ? "UNRESOLVED" : "NOT_STATED");
    row.byAgreementType[t] = (row.byAgreementType[t] || 0) + 1;
    const bucket = timingBucket(d.milestoneDate || d.expirationDate, AS_OF).bucket;
    row.byTiming[bucket] = (row.byTiming[bucket] || 0) + 1;
    const st = d.reconciliation?.currentStatusClass || "RESEARCH_REQUIRED";
    row.byStatus[st] = (row.byStatus[st] || 0) + 1;
  }
  return countries;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const sec = secDossiers();
  const dossiers = [...GLOBAL_WAVE_DOSSIERS, ...sec];
  for (const d of dossiers) {
    if (isTrackAReservedId(d.discoveryPropertyId) || isTrackAReservedId(d.findingId)) {
      throw new Error(`TRACK_A_COLLISION ${d.discoveryPropertyId}`);
    }
  }

  let hpcFields = null;
  if (token) {
    hpcFields = await hpcNameField();
  }
  for (const d of dossiers) {
    if (!hpcFields || !d.hpcQuery || d.identityClass === "EXTRACTION_ARTIFACT") {
      if (!d.hpcMatchStatus || d.hpcMatchStatus === "PENDING_HPC_SEARCH") {
        d.hpcMatchStatus = token ? d.hpcMatchStatus : "HPC_NOT_SEARCHED_NO_TOKEN";
      }
      continue;
    }
    const hit = await searchHpc(hpcFields, d.hpcQuery, d.physicalCountry);
    Object.assign(d, hit);
    await new Promise((r) => setTimeout(r, 220));
  }

  const all = [...dossiers, ...STAGED_PORTFOLIO_FINDINGS];
  const verifiedHotels = dossiers.filter((d) => d.persistEligible && d.identityClass === "VERIFIED_PROPERTY");
  const summary = {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    meteredResearch: false,
    lackOfNewerFilingProvesCurrent: lackOfNewerFilingProvesCurrent(),
    uniqueHotelsThisWave: verifiedHotels.length,
    secScheduleRows: sec.length,
    secArtifactsQuarantined: sec.filter((d) => d.identityClass === "EXTRACTION_ARTIFACT").length,
    stagedPortfolios: STAGED_PORTFOLIO_FINDINGS.length,
    contractEligible: verifiedHotels.filter((d) => d.contractEligible).length,
    hpcMatched: verifiedHotels.filter((d) => d.hpcRecordId).length,
    byWave: verifiedHotels.reduce((a, d) => {
      a[d.wave] = (a[d.wave] || 0) + 1;
      return a;
    }, {}),
  };

  const countryMatrix = matrix(all);
  fs.writeFileSync(path.join(OUT, "WAVE_DOSSIERS.json"), JSON.stringify({ summary, dossiers, staged: STAGED_PORTFOLIO_FINDINGS, countryMatrix }, null, 2));
  fs.writeFileSync(path.join(OUT, "MATRIX_COUNTRY_TIMING_TYPE.json"), JSON.stringify({ asOf: AS_OF, countryMatrix }, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
