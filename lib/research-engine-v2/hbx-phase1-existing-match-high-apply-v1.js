/**
 * HBX Phase 1 apply — existing_match_high only.
 * Hotel Property Census field-completion writes. No inserts.
 * No Rooms / Keys, coordinates, images, descriptions, facilities.
 *
 * Objective: hbx-phase1-existing-match-high-apply-v1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import {
  resolveCensusMode,
  assertNoInsertInFieldCompletionMode,
} from "./census-autopilot-full-latam-v3.js";
import { isRejectedDiscoveryHost } from "./census-discovery-host-policy.js";
import { isTrustedSecondaryHost } from "./census-source-trust-policy.js";
import {
  isChoiceCentralReservationPhone,
  normalizePhoneNumber,
} from "./census-phone-number-enrichment.js";
import {
  buildPhoneProvenanceNote,
  mergeStewardPhoneNote,
} from "./census-confidence-tiered-internal-completion.js";
import { toProperCasePlace } from "./census-city-state-normalizer.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const HBX_PHASE1_OBJECTIVE = "hbx-phase1-existing-match-high-apply-v1";
export const HBX_PHASE1_VERSION = "hbx-phase1-existing-match-high-apply-v1";

export const HBX_PHASE1_STATUS = Object.freeze({
  COMPLETE: "production_census_hbx_phase1_existing_match_high_apply_v1_complete",
  PARTIAL_SCHEMA:
    "production_census_hbx_phase1_existing_match_high_apply_v1_partial_schema_remaining",
  PARTIAL_LICENSE:
    "production_census_hbx_phase1_existing_match_high_apply_v1_partial_license_policy_needed",
  PARTIAL_SOURCE:
    "production_census_hbx_phase1_existing_match_high_apply_v1_partial_source_remaining",
  BLOCKED: "production_census_hbx_phase1_existing_match_high_apply_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] ||
  productionHotelPropertyCensus.tableId ||
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const CANDIDATE_PACK_PATH = path.join(
  ROOT,
  "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"
);

/** Fields this mission may write (must exist on Census + autopilot allowlist). */
const ALLOWED_WRITE = new Set([
  "Address",
  "Address Confidence",
  "Address Source URL",
  "City",
  "Country",
  "Official Property URL",
  "Phone",
  "Notes for Steward",
  "Enrichment Status",
  "Last Reviewed Date",
  "Source Type",
  "Source URL",
  "Source Confidence",
  "Data Confidence Tier",
  "Public Display Review Status",
  "Radar Display Status",
  "Human Review Required",
  "Production Use Status",
]);

const FORBIDDEN_THIS_MISSION = new Set([
  "Latitude",
  "Longitude",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Owner Name",
  "Operator / Management Company",
  "Developer Name",
  "Opening Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Brand Verified",
  "Brand Status",
  "Current Brand",
  "Hotel Description - AI Summary",
  "Hotel Description - Source Text",
  "Amenities - Structured Tags",
  "Amenities - Source Text",
]);

/** Desired HBX / phone provenance fields — checked live against schema. */
const SCHEMA_CHECK_FIELDS = Object.freeze([
  "HBX Hotel Code",
  "HBX Chain Code",
  "HBX Category Code",
  "HBX Category Name",
  "HBX Accommodation Type",
  "HBX License / Registration Number",
  "HBX Last Update",
  "HBX Source Type",
  "HBX Content Review Status",
  "Hotelbeds Code",
  "HBX External ID",
  "Postal Code",
  "Phone Confidence",
  "Phone Source URL",
  "Phone Source Type",
  "Phone Review Status",
  "Phone Reviewed Date",
  "Phone Notes",
]);

const HELD_LICENSE = Object.freeze([
  "coordinates",
  "images",
  "descriptions",
  "facilities",
]);

function isBlank(v) {
  return v == null || !String(v).trim();
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

function hostFromUrl(url) {
  try {
    let s = String(url || "").trim();
    if (!s) return "";
    if (!/^https?:\/\//i.test(s)) s = `https://${s}`;
    return new URL(s).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function normalizeWebsite(url) {
  let s = String(url || "").trim();
  if (!s) return null;
  if (!/^https?:\/\//i.test(s)) s = `https://${s}`;
  try {
    const u = new URL(s);
    if (!u.hostname) return null;
    return u.toString().replace(/\/$/, "");
  } catch {
    return null;
  }
}

function normCountry(c) {
  return String(c || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function countriesMatch(a, b) {
  const x = normCountry(a);
  const y = normCountry(b);
  if (!x || !y) return true;
  if (x === y) return true;
  const aliases = {
    mexico: ["méxico", "mx"],
    "dominican republic": ["dominican rep", "do", "república dominicana"],
    colombia: ["co"],
    "costa rica": ["cr"],
    panama: ["panamá", "pa"],
  };
  for (const [canon, list] of Object.entries(aliases)) {
    const set = new Set([canon, ...list]);
    if (set.has(x) && set.has(y)) return true;
  }
  return false;
}

function hasCentralReservationPhoneIndicator(phone, ctx = {}) {
  if (isChoiceCentralReservationPhone(phone)) return true;
  const blob = [ctx.name, ctx.city, ctx.notes].map((x) => String(x || "").toLowerCase()).join(" ");
  if (/central\s*reserv|reservation(s)?\s*(center|centre|line|desk)|call\s*center|toll[- ]?free\s*reserv/i.test(blob)) {
    return true;
  }
  const digits = String(phone || "").replace(/[^\d]/g, "");
  if (/^1?8(00|33|44|55|66|77|88)\d{7}$/.test(digits) && /marriott|hilton|hyatt|ihg|wyndham|choice|accor|barcelo/i.test(blob)) {
    return true;
  }
  return false;
}

function mergeHbxLinkageNote(existing, meta = {}) {
  const line = [
    "hbx_linkage",
    `hotel_code=${meta.hbx_hotel_code}`,
    `source=hbx_content_api`,
    `match=existing_match_high`,
    `exposure=internal_only`,
    `reviewed=${meta.reviewed_date || todayIsoDate()}`,
  ].join(" | ");
  const prev = String(existing || "").trim();
  if (!prev) return line;
  if (prev.includes("hbx_linkage")) {
    return prev.replace(/hbx_linkage[\s\S]*?(?=\n\n|$)/, line).trim();
  }
  return `${prev}\n\n${line}`.trim();
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 */
export function resolveHbxPhase1Gates(env = process.env) {
  const blockers = [];
  const flag = (k) => String(env[k] || "0").trim() === "1";

  if (!flag("ENABLE_HBX_CONTENT_API") && !flag("ENABLE_HBX_AS_SOURCE_LANE") && !flag("ENABLE_HBX_CENSUS_WRITES")) {
    // Allow dry-run without all flags; apply requires ENABLE_HBX_CENSUS_WRITES
  }
  if (flag("ENABLE_HBX_INSERTS")) blockers.push("ENABLE_HBX_INSERTS_must_be_0");
  if (flag("ENABLE_HBX_NEW_CANDIDATE_INSERTS")) {
    blockers.push("ENABLE_HBX_NEW_CANDIDATE_INSERTS_must_be_0");
  }
  if (flag("ENABLE_HBX_EXISTING_MATCH_MEDIUM_WRITES")) {
    blockers.push("ENABLE_HBX_EXISTING_MATCH_MEDIUM_WRITES_must_be_0");
  }
  if (flag("ENABLE_HBX_COORDINATE_WRITES")) blockers.push("ENABLE_HBX_COORDINATE_WRITES_must_be_0");
  if (flag("ENABLE_HBX_IMAGE_WRITES")) blockers.push("ENABLE_HBX_IMAGE_WRITES_must_be_0");
  if (flag("ENABLE_HBX_DESCRIPTION_WRITES")) blockers.push("ENABLE_HBX_DESCRIPTION_WRITES_must_be_0");
  if (flag("ENABLE_HBX_FACILITY_WRITES")) blockers.push("ENABLE_HBX_FACILITY_WRITES_must_be_0");
  if (flag("ENABLE_HBX_ROOM_WRITES")) blockers.push("ENABLE_HBX_ROOM_WRITES_must_be_0");
  if (flag("ENABLE_HBX_PHONEBOOKING_WRITES")) blockers.push("ENABLE_HBX_PHONEBOOKING_WRITES_must_be_0");
  if (flag("ENABLE_HBX_PHONEMANAGEMENT_WRITES")) {
    blockers.push("ENABLE_HBX_PHONEMANAGEMENT_WRITES_must_be_0");
  }

  return {
    ok: blockers.length === 0,
    blockers,
    census_writes: flag("ENABLE_HBX_CENSUS_WRITES"),
    existing_match_high: flag("ENABLE_HBX_EXISTING_MATCH_HIGH_WRITES"),
    phonehotel: flag("ENABLE_HBX_PHONEHOTEL_WRITES"),
    inserts: false,
    coordinates: false,
    images: false,
    descriptions: false,
    facilities: false,
    rooms: false,
  };
}

export async function fetchCensusFieldNames(baseId, token) {
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`schema_meta_failed:${res.status}:${json?.error?.message || ""}`);
  }
  const table = (json.tables || []).find(
    (t) => t.id === CENSUS_TABLE_ID || t.name === "Hotel Property Census"
  );
  if (!table) throw new Error("hotel_property_census_table_not_found_in_meta");
  return {
    table_id: table.id,
    table_name: table.name,
    field_names: (table.fields || []).map((f) => f.name),
    field_set: new Set((table.fields || []).map((f) => f.name)),
  };
}

export function runSchemaCheck(fieldSet) {
  const present = [];
  const missing = [];
  for (const name of SCHEMA_CHECK_FIELDS) {
    if (fieldSet.has(name)) present.push(name);
    else missing.push(name);
  }
  const coreExisting = ["Address", "City", "Country", "Phone", "Official Property URL"].filter(
    (f) => fieldSet.has(f)
  );
  return {
    present,
    missing,
    core_writable_present: coreExisting,
    hbx_identity_field_present: present.some((n) =>
      /HBX Hotel Code|Hotelbeds Code|HBX External ID/i.test(n)
    ),
    phone_provenance_fields_present: present.some((n) => /^Phone (Confidence|Source|Review)/i.test(n)),
    too_many_missing:
      missing.length >= 10 &&
      !present.some((n) => /HBX Hotel Code|Hotelbeds Code|HBX External ID/i.test(n)),
  };
}

function loadExistingMatchHigh() {
  if (!fs.existsSync(CANDIDATE_PACK_PATH)) {
    throw new Error(`missing_candidate_pack:${CANDIDATE_PACK_PATH}`);
  }
  const pack = JSON.parse(fs.readFileSync(CANDIDATE_PACK_PATH, "utf8"));
  const all = pack.candidates || [];
  const high = all.filter(
    (c) => c.match_class === "existing_match_high" && c.census_record_id
  );
  return { pack, high };
}

async function fetchCensusRecordsByIds(baseId, token, tableId, recordIds) {
  const out = [];
  const ids = [...new Set(recordIds.filter(Boolean))];
  for (let i = 0; i < ids.length; i += 20) {
    const chunk = ids.slice(i, i + 20);
    const or = chunk.map((id) => `RECORD_ID()='${id}'`).join(",");
    const filterByFormula = `OR(${or})`;
    const params = new URLSearchParams({ pageSize: "100", filterByFormula });
    const fields = [
      "Property Name",
      "Canonical Property Name",
      "Country",
      "City",
      "Address",
      "Address Confidence",
      "Official Property URL",
      "Phone",
      "Notes for Steward",
      "Enrichment Status",
      "Last Reviewed Date",
      "Source Type",
      "Source URL",
      "Source Confidence",
      "Data Confidence Tier",
      "Public Display Review Status",
      "Radar Display Status",
      "Production Use Status",
      "Human Review Required",
    ];
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`census_read_failed:${res.status}:${json?.error?.message || ""}`);
    }
    out.push(...(json.records || []));
    await sleep(120);
  }
  return out;
}

/**
 * Build patch for one existing_match_high candidate + live census fields.
 */
export function buildPhase1Patch(candidate, censusFields, schema, gates) {
  const held = [];
  const conflicts = [];
  const skipped = [];
  const patch = {};
  const fieldMap = {};

  // Always hold license-gated content
  for (const h of HELD_LICENSE) held.push({ field: h, reason: "license_policy_needed" });
  held.push({ field: "Rooms / Keys", reason: "hbx_rooms_catalog_only_unsupported" });

  // Schema-missing HBX identity — never invent alternate Census field
  if (!schema.hbx_identity_field_present) {
    skipped.push({
      field: "HBX Hotel Code",
      reason: "schema_missing",
      value: candidate.hbx_hotel_code,
    });
  }
  for (const f of [
    "HBX Chain Code",
    "HBX Category Code",
    "HBX Accommodation Type",
    "HBX License / Registration Number",
    "HBX Last Update",
    "Postal Code",
  ]) {
    if (schema.missing.includes(f) || !schema.present.includes(f)) {
      skipped.push({
        field: f,
        reason: "schema_missing",
        value:
          f === "HBX Chain Code"
            ? candidate.chain_code
            : f === "HBX Category Code"
              ? candidate.category
              : null,
      });
    }
  }

  // Country
  if (!isBlank(candidate.country)) {
    if (isBlank(censusFields.Country)) {
      patch.Country = candidate.country;
      fieldMap.Country = "hbx.country";
    } else if (!countriesMatch(censusFields.Country, candidate.country)) {
      conflicts.push({
        field: "Country",
        existing: censusFields.Country,
        candidate: candidate.country,
      });
    } else {
      skipped.push({ field: "Country", reason: "already_present" });
    }
  }

  // City — blank only, Proper Case
  if (!isBlank(candidate.city)) {
    const cityVal = toProperCasePlace(candidate.city) || String(candidate.city).trim();
    if (isBlank(censusFields.City)) {
      patch.City = cityVal;
      fieldMap.City = "hbx.city";
    } else {
      skipped.push({ field: "City", reason: "already_present" });
    }
  }

  // Address — blank only; prefer street-level
  if (!isBlank(candidate.address)) {
    if (isBlank(censusFields.Address)) {
      if (isStreetLevelAddress(candidate.address) || String(candidate.address).length >= 12) {
        patch.Address = String(candidate.address).trim();
        patch["Address Confidence"] = "Medium";
        const src = normalizeWebsite(candidate.website);
        if (src) patch["Address Source URL"] = src;
        fieldMap.Address = "hbx.address";
      } else {
        skipped.push({ field: "Address", reason: "not_street_level_enough" });
      }
    } else {
      skipped.push({ field: "Address", reason: "already_present" });
    }
  }

  // Website — blank only; reject OTA/affiliate/directory
  if (!isBlank(candidate.website)) {
    const url = normalizeWebsite(candidate.website);
    const host = hostFromUrl(url);
    if (!url || !host) {
      skipped.push({ field: "Official Property URL", reason: "invalid_url" });
    } else if (isRejectedDiscoveryHost(host) || isTrustedSecondaryHost(host)) {
      skipped.push({
        field: "Official Property URL",
        reason: "rejected_ota_affiliate_or_directory_host",
        host,
      });
    } else if (!isBlank(censusFields["Official Property URL"])) {
      skipped.push({
        field: "Official Property URL",
        reason: "preserve_existing_stronger_or_present",
      });
    } else {
      patch["Official Property URL"] = url;
      fieldMap["Official Property URL"] = "hbx.web";
    }
  }

  // Phone — PHONEHOTEL only
  if (gates.phonehotel && !isBlank(candidate.phonehotel)) {
    const phone = normalizePhoneNumber(candidate.phonehotel) || String(candidate.phonehotel).trim();
    if (
      hasCentralReservationPhoneIndicator(phone, {
        name: candidate.name,
        city: candidate.city,
      })
    ) {
      skipped.push({ field: "Phone", reason: "central_reservation_phone_rejected" });
    } else if (!isBlank(censusFields.Phone)) {
      const existing = normalizePhoneNumber(censusFields.Phone) || censusFields.Phone;
      const a = String(existing).replace(/[^\d]/g, "");
      const b = String(phone).replace(/[^\d]/g, "");
      if (a && b && (a === b || a.endsWith(b) || b.endsWith(a))) {
        skipped.push({ field: "Phone", reason: "phone_already_same" });
      } else {
        conflicts.push({ field: "Phone", existing, candidate: phone });
      }
    } else {
      patch.Phone = phone;
      fieldMap.Phone = "hbx.phones.PHONEHOTEL";
      const phoneNote = buildPhoneProvenanceNote({
        confidence: "Medium",
        source: "hbx_content_api",
        source_url: normalizeWebsite(candidate.website),
        match_class: "existing_match_high",
        reviewed_date: todayIsoDate(),
      });
      const withPhone = mergeStewardPhoneNote(censusFields["Notes for Steward"], phoneNote);
      patch["Notes for Steward"] = mergeHbxLinkageNote(withPhone, {
        hbx_hotel_code: candidate.hbx_hotel_code,
        reviewed_date: todayIsoDate(),
      });
      // Phone Confidence / Review fields → schema_missing (documented)
      for (const f of [
        "Phone Confidence",
        "Phone Source Type",
        "Phone Review Status",
        "Phone Reviewed Date",
        "Phone Notes",
      ]) {
        skipped.push({
          field: f,
          reason: "schema_missing",
          intended: f === "Phone Confidence" ? "Medium" : f === "Phone Source Type" ? "hbx_content_api" : f === "Phone Review Status" ? "Internal Only" : todayIsoDate(),
        });
      }
    }
  } else if (!gates.phonehotel) {
    skipped.push({ field: "Phone", reason: "ENABLE_HBX_PHONEHOTEL_WRITES_not_1" });
  } else {
    skipped.push({ field: "Phone", reason: "no_phonehotel" });
  }

  // If we wrote anything without phone note, still attach HBX linkage
  if (Object.keys(patch).length && !patch["Notes for Steward"]) {
    patch["Notes for Steward"] = mergeHbxLinkageNote(censusFields["Notes for Steward"], {
      hbx_hotel_code: candidate.hbx_hotel_code,
      reviewed_date: todayIsoDate(),
    });
  }

  if (Object.keys(patch).length) {
    patch["Last Reviewed Date"] = todayIsoDate();
    patch["Enrichment Status"] =
      censusFields["Enrichment Status"] === "Complete" ? "Complete" : "Partial";
    patch["Data Confidence Tier"] = "Medium";
    // Source Type select has no hbx_content_api option — use allowed "other" + Notes provenance
    if (isBlank(censusFields["Source Type"])) {
      patch["Source Type"] = "other";
    }
    patch["Source Confidence"] = "Medium";
    if (normalizeWebsite(candidate.website) && isBlank(censusFields["Source URL"])) {
      patch["Source URL"] = normalizeWebsite(candidate.website);
    }
    // Keep public/radar hold — do not make HBX-derived public-ready
    if (isBlank(censusFields["Public Display Review Status"])) {
      patch["Public Display Review Status"] = "Hold";
    }
    if (isBlank(censusFields["Radar Display Status"])) {
      patch["Radar Display Status"] = "Hold";
    }
    if (isBlank(censusFields["Production Use Status"])) {
      patch["Production Use Status"] = "Census Only / Not Owner-Facing";
    }
    patch["Human Review Required"] = true;
  }

  // Strip forbidden / disallowed
  for (const k of Object.keys(patch)) {
    if (FORBIDDEN_THIS_MISSION.has(k) || isForbiddenAutopilotField(k) || !ALLOWED_WRITE.has(k)) {
      delete patch[k];
    }
  }

  return {
    ok: Object.keys(patch).length > 0,
    patch,
    field_map: fieldMap,
    held,
    conflicts,
    skipped,
    validation: {
      pass: Object.keys(patch).length > 0 && conflicts.length === 0,
      failed_checks: conflicts.map((c) => `conflict:${c.field}`),
    },
  };
}

async function applyPatches(proposals, { baseId, token, tableId, log }) {
  let updatesApplied = 0;
  const writeErrors = [];
  const counts = {
    address: 0,
    website: 0,
    phone: 0,
    city: 0,
    country: 0,
  };

  for (let i = 0; i < proposals.length; i += 10) {
    const chunk = proposals.slice(i, i + 10);
    const records = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (FORBIDDEN_THIS_MISSION.has(k)) continue;
          if (isForbiddenAutopilotField(k)) continue;
          if (!ALLOWED_WRITE.has(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
          if (k === "Address") counts.address += 1;
          if (k === "Official Property URL") counts.website += 1;
          if (k === "Phone") counts.phone += 1;
          if (k === "City") counts.city += 1;
          if (k === "Country") counts.country += 1;
        }
        return { id: p.record_id, fields };
      })
      .filter((u) => Object.keys(u.fields).length > 0);

    if (!records.length) continue;

    const tryWrite = async (recs) => {
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
        {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ records: recs, typecast: true }),
        }
      );
      const json = await res.json().catch(() => ({}));
      return { res, json };
    };

    const { res, json } = await tryWrite(records);
    if (!res.ok) {
      writeErrors.push({ status: res.status, error: json.error || json, batch: true });
      log?.(`[hbx-phase1] batch ${res.status}; retrying one-by-one`);
      for (const rec of records) {
        const one = await tryWrite([rec]);
        if (!one.res.ok) {
          writeErrors.push({
            status: one.res.status,
            error: one.json.error || one.json,
            record_id: rec.id,
          });
        } else {
          updatesApplied += 1;
        }
        await sleep(150);
      }
    } else {
      updatesApplied += records.length;
    }
    await sleep(200);
  }

  return { updatesApplied, writeErrors, counts };
}

function completionSnapshot(records) {
  let address = 0;
  let phone = 0;
  let website = 0;
  for (const r of records) {
    const f = r.fields || {};
    if (!isBlank(f.Address)) address += 1;
    if (!isBlank(f.Phone)) phone += 1;
    if (!isBlank(f["Official Property URL"])) website += 1;
  }
  return {
    n: records.length,
    address,
    phone,
    website,
    external_id_hbx: 0, // schema missing
  };
}

function renderReportMd(report) {
  return `# HBX Phase 1 — Existing Match High Apply v1

**Status:** \`${report.status}\`  
**Objective:** \`${report.objective}\`  
**Generated:** ${report.generated_at}  
**Dry run:** ${report.dry_run}  
**Airtable writes:** **${report.airtable_writes}**

## Target
- Base: Deal Capture Platform
- Table: Hotel Property Census (\`${CENSUS_TABLE_ID}\`)
- Match class: **existing_match_high only**
- Mode: field-completion-only · no inserts

## Summary
- existing_match_high reviewed: **${report.existing_match_high_reviewed}**
- records updated: **${report.records_updated}**
- HBX Hotel Codes written (dedicated field): **${report.hbx_hotel_codes_written}**
- address writes: **${report.address_writes}**
- website writes: **${report.website_writes}**
- PHONEHOTEL writes: **${report.phonehotel_writes}**
- city writes: **${report.city_writes}**
- country writes: **${report.country_writes}**
- category / chain / accommodation / license / lastUpdate writes: **0** (schema_missing or held)
- conflicts: **${report.conflicts_count}**
- proposals ready: **${report.proposals_ready}**

## Schema check
- Present: ${(report.schema_check?.present || []).join(", ") || "(none of HBX/phone-provenance fields)"}
- Missing: ${(report.schema_check?.missing || []).map((m) => `\`${m}\``).join(", ")}

## Fields held (license policy)
${(report.fields_held_license || HELD_LICENSE).map((f) => `- ${f}`).join("\n")}

## Completion before → after (reviewed set)
| Field | Before | After |
| --- | ---: | ---: |
| Address | ${report.completion_before?.address ?? "—"} | ${report.completion_after?.address ?? "—"} |
| Phone | ${report.completion_before?.phone ?? "—"} | ${report.completion_after?.phone ?? "—"} |
| Official Property URL | ${report.completion_before?.website ?? "—"} | ${report.completion_after?.website ?? "—"} |
| HBX External ID | ${report.completion_before?.external_id_hbx ?? 0} | ${report.completion_after?.external_id_hbx ?? 0} |

## Confirmations
- No inserts: **${report.confirmations?.no_inserts}**
- No Rooms / Keys from HBX: **${report.confirmations?.no_rooms_keys}**
- No coordinates/images/descriptions/facilities: **${report.confirmations?.no_license_gated_writes}**
- PHONEHOTEL only: **${report.confirmations?.phonehotel_only}**
- PHONEBOOKING / PHONEMANAGEMENT rejected: **${report.confirmations?.booking_management_phones_rejected}**
- existing_match_high only: **${report.confirmations?.existing_match_high_only}**
- Brand Explorer / Brand Setup / VIC writes: **0**
- No secrets logged: **true**

## Recommended next
- Create Census fields for HBX Hotel Code (+ optional Chain/Category/Accommodation/Last Update) so identity linkage is first-class.
- Keep ENABLE_HBX_INSERTS=0 until insert policy approved.
- Send HBX room-count support pack before any Rooms / Keys consideration.
- License review before coordinates/images/descriptions/facilities.
`;
}

/**
 * @param {object} opts
 */
export async function runHbxPhase1ExistingMatchHighApplyV1(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const generated_at = new Date().toISOString();
  const censusMode =
    opts.censusMode ||
    resolveCensusMode(opts.argv || [], opts.args || { censusMode: "field-completion-only" });
  assertNoInsertInFieldCompletionMode(censusMode, 0);

  const gates = resolveHbxPhase1Gates(env);
  if (!gates.ok) {
    const report = {
      ok: false,
      status: HBX_PHASE1_STATUS.BLOCKED,
      objective: HBX_PHASE1_OBJECTIVE,
      generated_at,
      reason: "gate_blockers",
      blockers: gates.blockers,
      airtable_writes: 0,
      dry_run: true,
    };
    persistReports(report);
    return report;
  }

  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      gates.census_writes &&
      gates.existing_match_high
  );

  let token;
  let baseId;
  try {
    token = resolvePat();
    const base = resolveTargetBase();
    baseId = base?.baseId || env.AIRTABLE_BASE_ID_ALT;
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
  } catch (err) {
    const report = {
      ok: false,
      status: HBX_PHASE1_STATUS.BLOCKED,
      objective: HBX_PHASE1_OBJECTIVE,
      generated_at,
      reason: String(err?.message || err).slice(0, 300),
      airtable_writes: 0,
      dry_run: true,
    };
    persistReports(report);
    return report;
  }

  const schemaMeta = await fetchCensusFieldNames(baseId, token);
  const schema_check = runSchemaCheck(schemaMeta.field_set);

  const { high } = loadExistingMatchHigh();
  const calaFilter = String(env.HBX_CALA_COUNTRIES || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const filtered = calaFilter.length
    ? high.filter((c) => calaFilter.includes(c.country))
    : high;

  log(`[hbx-phase1] existing_match_high=${filtered.length} dry_run=${!enableWrites}`);

  const censusRecords = await fetchCensusRecordsByIds(
    baseId,
    token,
    CENSUS_TABLE_ID,
    filtered.map((c) => c.census_record_id)
  );
  const byId = new Map(censusRecords.map((r) => [r.id, r]));
  const completion_before = completionSnapshot(censusRecords);

  const proposals = [];
  const conflicts = [];
  const skippedReasons = {};
  let phonebookingRejected = 0;
  let phonemanagementRejected = 0;

  for (const c of filtered) {
    // Explicit reject booking/management if ever present on pack
    if (c.phone_rejected?.length) {
      for (const r of c.phone_rejected) {
        if (/BOOKING/i.test(r.type || r.reason || "")) phonebookingRejected += 1;
        if (/MANAGEMENT/i.test(r.type || r.reason || "")) phonemanagementRejected += 1;
      }
    }
    const rec = byId.get(c.census_record_id);
    if (!rec) {
      skippedReasons.census_record_missing = (skippedReasons.census_record_missing || 0) + 1;
      continue;
    }
    const built = buildPhase1Patch(c, rec.fields || {}, schema_check, gates);
    for (const s of built.skipped) {
      skippedReasons[s.reason] = (skippedReasons[s.reason] || 0) + 1;
    }
    for (const conf of built.conflicts) {
      conflicts.push({ ...conf, record_id: c.census_record_id, hbx_hotel_code: c.hbx_hotel_code });
    }
    if (!built.ok) continue;
    proposals.push({
      record_id: c.census_record_id,
      hbx_hotel_code: c.hbx_hotel_code,
      name: c.name,
      country: c.country,
      patch: built.patch,
      field_map: built.field_map,
      validation: built.validation,
      sanitized_payload_preview: Object.fromEntries(
        Object.entries(built.patch).map(([k, v]) => [
          k,
          typeof v === "string" && k === "Phone" ? `${String(v).slice(0, 4)}…` : v,
        ])
      ),
    });
  }

  // Dedupe by census record (multiple HBX hotels can match same Census row)
  const byRecord = new Map();
  for (const p of proposals) {
    const prev = byRecord.get(p.record_id);
    if (!prev) {
      byRecord.set(p.record_id, p);
      continue;
    }
    byRecord.set(p.record_id, {
      ...prev,
      patch: { ...prev.patch, ...p.patch },
      field_map: { ...prev.field_map, ...p.field_map },
      hbx_hotel_code: prev.hbx_hotel_code,
      note: "deduped_multiple_hbx_matches_same_census_record",
    });
  }
  const uniqueProposals = [...byRecord.values()];

  let updatesApplied = 0;
  let writeErrors = [];
  let counts = { address: 0, website: 0, phone: 0, city: 0, country: 0 };
  let completion_after = completion_before;

  if (enableWrites && uniqueProposals.length) {
    const applied = await applyPatches(uniqueProposals, {
      baseId,
      token,
      tableId: CENSUS_TABLE_ID,
      log,
    });
    updatesApplied = applied.updatesApplied;
    writeErrors = applied.writeErrors;
    counts = applied.counts;
    const afterRecords = await fetchCensusRecordsByIds(
      baseId,
      token,
      CENSUS_TABLE_ID,
      filtered.map((c) => c.census_record_id)
    );
    completion_after = completionSnapshot(afterRecords);
  } else if (!enableWrites) {
    completion_after = { ...completion_before };
    for (const p of uniqueProposals) {
      if (p.patch.Address && isBlank(byId.get(p.record_id)?.fields?.Address)) {
        completion_after.address += 1;
        counts.address += 1;
      }
      if (p.patch.Phone && isBlank(byId.get(p.record_id)?.fields?.Phone)) {
        completion_after.phone += 1;
        counts.phone += 1;
      }
      if (
        p.patch["Official Property URL"] &&
        isBlank(byId.get(p.record_id)?.fields?.["Official Property URL"])
      ) {
        completion_after.website += 1;
        counts.website += 1;
      }
      if (p.patch.City) counts.city += 1;
      if (p.patch.Country) counts.country += 1;
    }
  }

  let status = HBX_PHASE1_STATUS.COMPLETE;
  if (writeErrors.length && updatesApplied === 0 && enableWrites) {
    status = HBX_PHASE1_STATUS.BLOCKED;
  } else if (schema_check.too_many_missing || !schema_check.hbx_identity_field_present) {
    status = HBX_PHASE1_STATUS.PARTIAL_SCHEMA;
  } else if (proposals.length < filtered.length * 0.1 && filtered.length > 50) {
    status = HBX_PHASE1_STATUS.PARTIAL_SOURCE;
  }

  const report = {
    ok: status !== HBX_PHASE1_STATUS.BLOCKED,
    status,
    objective: HBX_PHASE1_OBJECTIVE,
    version: HBX_PHASE1_VERSION,
    generated_at,
    dry_run: !enableWrites,
    airtable_writes: enableWrites ? updatesApplied : 0,
    inserts: 0,
    census_mode: censusMode,
    target: {
      base: "Deal Capture Platform",
      table: "Hotel Property Census",
      table_id: CENSUS_TABLE_ID,
    },
    gates,
    schema_check,
    existing_match_high_reviewed: filtered.length,
    proposals_ready: uniqueProposals.length,
    proposals_before_dedupe: proposals.length,
    records_updated: enableWrites ? updatesApplied : 0,
    hbx_hotel_codes_written: 0,
    address_writes: counts.address,
    website_writes: counts.website,
    phonehotel_writes: counts.phone,
    city_writes: counts.city,
    country_writes: counts.country,
    category_writes: 0,
    chain_code_writes: 0,
    accommodation_type_writes: 0,
    license_registration_writes: 0,
    last_update_writes: 0,
    conflicts_count: conflicts.length,
    conflicts: conflicts.slice(0, 50),
    skipped_reasons: skippedReasons,
    phonebooking_rejected: phonebookingRejected,
    phonemanagement_rejected: phonemanagementRejected,
    fields_held_license: HELD_LICENSE,
    schema_missing_fields: schema_check.missing,
    completion_before,
    completion_after,
    write_errors: writeErrors.slice(0, 20),
    sample_proposals: uniqueProposals.slice(0, 15),
    confirmations: {
      no_inserts: true,
      no_rooms_keys: true,
      no_license_gated_writes: true,
      phonehotel_only: true,
      booking_management_phones_rejected: true,
      existing_match_high_only: true,
      hotel_property_census_only: true,
      no_brand_explorer: true,
      no_brand_setup: true,
      no_vic: true,
    },
  };

  persistReports(report);
  log(`[hbx-phase1] status=${report.status} updated=${report.records_updated} dry_run=${report.dry_run}`);
  return report;
}

function persistReports(report) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  writeJson(path.join(reportsDir, "hbx-phase1-existing-match-high-apply-v1.json"), report);
  const md = renderReportMd(report);
  writeMd(path.join(reportsDir, "hbx-phase1-existing-match-high-apply-v1.md"), md);
  writeMd(path.join(docsDir, "hbx-phase1-existing-match-high-apply-v1.md"), md);
}
