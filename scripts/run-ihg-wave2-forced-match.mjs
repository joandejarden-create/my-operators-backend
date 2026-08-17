#!/usr/bin/env node
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import {
  normalizeIhgHotelNameForMatch,
  ihgBrandFamilyFromName,
  ihgBrandFamiliesAlign,
} from "../lib/hotel-census/plan-ihg-census-directory-match.js";
import { nameSimilarity, countriesMatch } from "../lib/independent-census/match-current-census.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";

const APPLY = process.argv.includes("--apply");
const WAVE2 = ["Kimpton Hotels", "Hotel Indigo"];
const MAP = { website: "Website", propertyId: CENSUS_PROPERTY_ID_FIELD };

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

const extract = JSON.parse(readFileSync("reports/ihg-cala-directory-extract.json", "utf8"));
const dirAll = (extract.propertyRows || []).filter(
  (d) =>
    /kimpton|hotelindigo/i.test(d.brand || "") ||
    /\/kimptonhotels\/|\/hotelindigo\//i.test(d.propertyUrl || "")
);

// All IHG census with Property IDs to detect claims
const ihgClaimed = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", CENSUS_FIELDS.affiliation, CENSUS_PROPERTY_ID_FIELD, "Website"],
    filterByFormula: `OR(FIND("IHG", {${CENSUS_FIELDS.parentCompany}}), FIND("InterContinental", {${CENSUS_FIELDS.parentCompany}}))`,
    pageSize: 100,
  })
  .all();

const claimedByPid = new Map();
for (const r of ihgClaimed) {
  const pid = String(r.fields[CENSUS_PROPERTY_ID_FIELD] || "")
    .trim()
    .toUpperCase();
  if (pid) claimedByPid.set(pid, { id: r.id, name: r.fields.name, aff: r.fields.Affiliation });
}

const wave2 = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: [
      "name",
      CENSUS_FIELDS.affiliation,
      CENSUS_FIELDS.country,
      CENSUS_FIELDS.city,
      CENSUS_FIELDS.status,
      "Website",
      CENSUS_PROPERTY_ID_FIELD,
    ],
    filterByFormula: `OR(${WAVE2.map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`).join(",")})`,
  })
  .all();

/** Manual high-confidence pairs: census name substring → propertyId (verified against directory) */
const FORCE_PAIRS = [
  {
    censusNameRe: /^hotel indigo grand cayman$/i,
    propertyId: "GCMSM",
    minNameSim: 0.9,
  },
  {
    censusNameRe: /^kimpton virgilio$/i,
    propertyId: "MEXPL",
    minNameSim: 0.7,
  },
  {
    censusNameRe: /kimpton mas olas/i,
    propertyId: "SJDTD",
    minNameSim: 0.35,
  },
];

const planRows = [];
const steward = [];

for (const rec of wave2) {
  if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
  const blankWeb = isBlankCensusValue(rec.fields.Website);
  const blankPid = isBlankCensusValue(rec.fields[CENSUS_PROPERTY_ID_FIELD]);
  if (!blankWeb && !blankPid) continue;

  const name = String(rec.fields.name || "");
  const aff = String(rec.fields[CENSUS_FIELDS.affiliation] || "");
  const country = String(rec.fields[CENSUS_FIELDS.country] || "");
  const status = rec.fields[CENSUS_FIELDS.status];
  const family = ihgBrandFamilyFromName(name);

  let best = null;
  for (const d of dirAll) {
    if (!ihgBrandFamiliesAlign(name, d.propertyUrl, d.name || d.inferredHotelName)) continue;
    if (!countriesMatch(country, d.country) && !FORCE_PAIRS.some((f) => f.censusNameRe.test(name) && f.propertyId === String(d.propertyId || "").toUpperCase())) {
      // allow force pairs to bypass country soft-mismatch (e.g. Cayman labels)
      const force = FORCE_PAIRS.find(
        (f) => f.censusNameRe.test(name) && f.propertyId === String(d.propertyId || "").toUpperCase()
      );
      if (!force && !countriesMatch(country, d.country)) continue;
    }
    const ns = nameSimilarity(
      normalizeIhgHotelNameForMatch(d.name || d.inferredHotelName),
      normalizeIhgHotelNameForMatch(name)
    );
    const force = FORCE_PAIRS.find(
      (f) => f.censusNameRe.test(name) && f.propertyId === String(d.propertyId || "").toUpperCase()
    );
    const score = force ? Math.max(ns, force.minNameSim + 0.2) : ns;
    if (!best || score > best.score) best = { score, ns, d, force: Boolean(force) };
  }

  // Also try force by propertyId lookup
  if (!best || best.score < 0.6) {
    for (const f of FORCE_PAIRS) {
      if (!f.censusNameRe.test(name)) continue;
      const d = dirAll.find((x) => String(x.propertyId || "").toUpperCase() === f.propertyId);
      if (!d) continue;
      const ns = nameSimilarity(
        normalizeIhgHotelNameForMatch(d.name || d.inferredHotelName),
        normalizeIhgHotelNameForMatch(name)
      );
      if (ns >= f.minNameSim || true) {
        best = { score: Math.max(ns, 0.8), ns, d, force: true };
      }
    }
  }

  if (!best || best.score < 0.6) {
    steward.push({
      censusRecordId: rec.id,
      censusName: name,
      affiliation: aff,
      country,
      status,
      reason: Array.isArray(status) && status.some((s) => /pipeline/i.test(String(s)))
        ? "pipeline_not_on_open_directory"
        : "no_safe_directory_match",
      bestNs: best?.ns ?? null,
      bestName: best?.d?.name || null,
    });
    continue;
  }

  // Block known false-friend city matches (Providencia ≠ Expo)
  if (
    /guadalajara providencia/i.test(name) &&
    /guadalajara expo/i.test(best.d.name || best.d.inferredHotelName || "")
  ) {
    steward.push({
      censusRecordId: rec.id,
      censusName: name,
      affiliation: aff,
      country,
      reason: "false_friend_blocked_guadalajara_providencia_vs_expo",
      bestName: best.d.name,
      propertyUrl: best.d.propertyUrl,
    });
    continue;
  }

  // Require stronger name sim unless force-paired
  if (!best.force && best.ns < 0.7) {
    steward.push({
      censusRecordId: rec.id,
      censusName: name,
      affiliation: aff,
      country,
      reason: "below_name_sim_0_7",
      bestNs: best.ns,
      bestName: best.d.name,
    });
    continue;
  }

  const pid = String(best.d.propertyId || "").toUpperCase();
  const url = String(best.d.propertyUrl || "").replace(/\/$/, "");
  const claim = claimedByPid.get(pid);
  if (claim && claim.id !== rec.id) {
    // If claimer has blank name conflict — still allow Website fill on this row if claimer is clearly same hotel
    const claimNs = nameSimilarity(
      normalizeIhgHotelNameForMatch(claim.name),
      normalizeIhgHotelNameForMatch(name)
    );
    if (claimNs < 0.5) {
      steward.push({
        censusRecordId: rec.id,
        censusName: name,
        affiliation: aff,
        country,
        reason: "property_id_claimed_elsewhere",
        claimedBy: claim.id,
        claimedName: claim.name,
        propertyId: pid,
        propertyUrl: url,
      });
      continue;
    }
    // Same hotel duplicate — fill blank Website/PID only on this row if claimer already has them; skip PID to avoid duplicate IDs
    const applyFields = {};
    if (blankWeb && url) applyFields[MAP.website] = url;
    // Do not write duplicate Property ID onto second record
    if (!Object.keys(applyFields).length) {
      steward.push({
        censusRecordId: rec.id,
        censusName: name,
        reason: "duplicate_of_claimed_record",
        claimedBy: claim.id,
        propertyId: pid,
      });
      continue;
    }
    planRows.push({
      censusRecordId: rec.id,
      censusName: name,
      affiliation: aff,
      propertyId: pid,
      propertyUrl: url,
      nameSim: best.ns,
      matchNote: "website_only_pid_claimed_on_sibling",
      claimedBy: claim.id,
      applyFields,
    });
    continue;
  }

  const applyFields = {};
  if (blankWeb && url) applyFields[MAP.website] = url;
  if (blankPid && pid) applyFields[MAP.propertyId] = pid;
  if (!Object.keys(applyFields).length) continue;
  // Require Website on medium+ when blank
  if (blankWeb && !applyFields[MAP.website]) {
    steward.push({ censusRecordId: rec.id, censusName: name, reason: "missing_website_on_match" });
    continue;
  }

  planRows.push({
    censusRecordId: rec.id,
    censusName: name,
    affiliation: aff,
    propertyId: pid,
    propertyUrl: url,
    nameSim: best.ns,
    force: best.force,
    applyFields,
  });
}

mkdirSync("reports", { recursive: true });
writeFileSync(
  "reports/ihg-wave2-forced-match-plan.json",
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      mode: APPLY ? "apply" : "dry-run",
      fieldMapping: MAP,
      readyToApply: planRows.length,
      planRows,
      steward,
    },
    null,
    2
  )
);

console.log("Ready:", planRows.length);
for (const r of planRows) console.log(" ", r.censusName, r.applyFields, "sim", r.nameSim);
console.log("Steward:", steward.length);
for (const s of steward.slice(0, 20)) console.log(" ", s.censusName, s.reason);

if (!APPLY) {
  console.log("DRY-RUN");
  process.exit(0);
}

let updated = 0;
for (const row of planRows) {
  await base(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
    typecast: true,
  });
  updated++;
  console.log("UPDATED", row.censusRecordId, row.censusName);
}
writeFileSync(
  "reports/ihg-wave2-forced-match-apply-log.json",
  JSON.stringify({ generatedAt: new Date().toISOString(), updated, planRows }, null, 2)
);
console.log("Updated:", updated);
