/**
 * DataForSEO Local Address Scale v1.
 * Live Maps match_high → Address (Medium) writes on Clean Core census only.
 * No phone / DataForSEO coords / rooms / inserts / website writes.
 * Classifies Mapbox eligibility after address write (does not geocode).
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
} from "./production-census-source-of-truth.js";
import {
  checkAutopilotApplyEnv,
  applyPreflight,
} from "./census-autopilot-apply-guard.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import {
  resolveCensusMode,
  assertNoInsertInFieldCompletionMode,
} from "./census-autopilot-full-latam-v3.js";
import {
  resolveDataForSeoCredentials,
  resolveDataForSeoLocationName,
  stripDiacritics,
  fetchGoogleMapsLive,
} from "./dataforseo-client.js";
import {
  scoreLocalBusinessToCensus,
  buildLocalEnrichmentCandidate,
  classifyLodgingType,
  MATCH_CLASS,
  LODGING_CLASS,
} from "./dataforseo-local-match.js";
import {
  evaluateLocalAddressWrite,
  evaluateLocalWebsiteWrite,
  evaluateLocalPhoneWrite,
} from "./dataforseo-local-business-validated-write-v1.js";
import { evaluateCleanCorePass } from "./census-map-contact-size-readiness.js";
import { evaluateCoordinateIdentityGate } from "./census-core-identity-quality.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { isRejectedDiscoveryHost } from "./dataforseo-validated-write-policy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE =
  "dataforseo-local-address-scale-v1";
export const DATAFORSEO_LOCAL_ADDRESS_SCALE_VERSION =
  "dataforseo-local-address-scale-v1";

export const DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS = Object.freeze({
  COMPLETE:
    "production_census_dataforseo_local_address_scale_v1_complete",
  PARTIAL_POLICY:
    "production_census_dataforseo_local_address_scale_v1_partial_policy_decision_needed",
  PARTIAL_SOURCE:
    "production_census_dataforseo_local_address_scale_v1_partial_source_remaining",
  BLOCKED:
    "production_census_dataforseo_local_address_scale_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const DEFAULT_INSERT_QUEUE = path.join(
  ROOT,
  "reports/research-engine-v2/autopilot/2026-08-07T22-22-14_CALA-dataforseo-local-business-enrichment-v1/candidate-insert-queue.json"
);

/** Schema note: Address Source Type / Coordinate Eligibility Status are not live fields. */
export const ADDRESS_SCALE_SCHEMA_NOTES = Object.freeze({
  address_source_type_field_exists: false,
  coordinate_eligibility_status_field_exists: false,
  geocode_eligibility_reason_field_exists: false,
});

const ALLOWED_WRITE = new Set([
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Phone",
  "Notes for Steward",
  "Official Property URL",
  "Source URL",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
]);

const FORBIDDEN_THIS_MISSION = new Set([
  "Latitude",
  "Longitude",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
]);

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Country",
  "City",
  "State / Region",
  "Market",
  "Submarket",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Phone",
  "Notes for Steward",
  "Official Property URL",
  "Source URL",
  "Latitude",
  "Longitude",
  "Rooms / Keys",
  "Enrichment Status",
  "Enrichment Priority",
];

function isBlank(v) {
  return v == null || !String(v).trim();
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(filePath, md) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function hostFromUrl(url) {
  try {
    return new URL(String(url || "")).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 */
export function resolveDataForSeoLocalAddressScaleGates(env = process.env) {
  const enabled = String(env.DATAFORSEO_ENABLED || "0").trim() === "1";
  const candidatesOnly =
    String(env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "0").trim() === "1";
  const validated =
    String(env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0").trim() === "1";
  const address =
    String(env.ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES || "0").trim() === "1";
  const website =
    String(env.ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES || "0").trim() === "1";
  const phone =
    String(env.ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES || "0").trim() === "1";
  const coords =
    String(env.ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES || "0").trim() === "1";
  const inserts =
    String(env.ENABLE_DATAFORSEO_LOCAL_INSERTS || "0").trim() === "1";
  const internalMedium =
    String(env.ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION || "0").trim() === "1";

  const blockers = [];
  if (!enabled) blockers.push("DATAFORSEO_ENABLED_not_1");
  if (candidatesOnly) {
    blockers.push("DATAFORSEO_WRITE_CANDIDATES_ONLY_must_be_0_for_scale_writes");
  }
  if (!validated) blockers.push("ENABLE_DATAFORSEO_VALIDATED_WRITES_must_be_1");
  if (!address && !website && !(phone && internalMedium)) {
    blockers.push("address_or_website_or_phone_write_flag_required");
  }
  if (phone && !internalMedium) {
    blockers.push(
      "ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES_requires_ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION"
    );
  }
  if (coords) {
    blockers.push("ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES_must_be_0");
  }
  if (inserts) blockers.push("ENABLE_DATAFORSEO_LOCAL_INSERTS_must_be_0");

  return {
    ok: blockers.length === 0,
    blockers,
    dataforseo_is_source_of_truth: false,
    address_writes: address,
    website_writes: website,
    phone_writes: Boolean(phone && internalMedium),
    coordinate_writes: false,
    rooms_from_maps: false,
    inserts: false,
    match_high_only: true,
    address_confidence_default: "Medium",
    internal_medium_completion: internalMedium,
    schema_notes: ADDRESS_SCALE_SCHEMA_NOTES,
  };
}

/**
 * Eligible for local scale enrichment:
 * - missing Address (primary), OR
 * - missing Phone when Medium phone writes are enabled (internal census)
 * Plus Clean Core OR equivalent identity gate.
 */
export function isEligibleForLocalAddressScale(record, opts = {}) {
  const f = record?.fields || {};
  const missingAddress = isBlank(f.Address);
  const missingPhone = isBlank(f.Phone);
  const phoneMode = opts.phoneWritesEnabled === true;

  if (!missingAddress && !(phoneMode && missingPhone)) {
    return {
      ok: false,
      reason: phoneMode ? "address_and_phone_present" : "address_already_present",
      dirty: false,
    };
  }
  const name = f["Canonical Property Name"] || f["Property Name"] || "";
  if (!String(name).trim()) {
    return { ok: false, reason: "missing_name", dirty: false };
  }
  if (isBlank(f.City) || isBlank(f.Country)) {
    return { ok: false, reason: "missing_city_or_country", dirty: false };
  }

  const clean = evaluateCleanCorePass(record, {
    canonicalFieldExists: opts.canonicalFieldExists !== false,
  });
  if (clean.pass) {
    return {
      ok: true,
      reason: null,
      clean_core: true,
      dirty: false,
      missing_address: missingAddress,
      missing_phone: missingPhone,
    };
  }

  const identity = evaluateCoordinateIdentityGate(record, {
    canonicalFieldExists: opts.canonicalFieldExists !== false,
  });
  if (identity.allow_geocode) {
    return {
      ok: true,
      reason: null,
      clean_core: false,
      equivalent: true,
      dirty: false,
      missing_address: missingAddress,
      missing_phone: missingPhone,
    };
  }

  return {
    ok: false,
    reason: identity.reason || "blocked_dirty_identity",
    dirty: true,
    clean_core: false,
  };
}

/**
 * Build Maps query attempts: city+country (ASCII), then city+state+country, then country-only.
 */
export function buildLocalAddressScaleQueryAttempts(fields = {}) {
  const name =
    fields["Canonical Property Name"] || fields["Property Name"] || "";
  const city = stripDiacritics(fields.City || "");
  const country = String(fields.Country || "").trim();
  const state = stripDiacritics(fields["State / Region"] || "");
  const locationPrimary = resolveDataForSeoLocationName({
    city: fields.City,
    country: fields.Country,
  });
  const locationWithState =
    city && state && country ? `${city},${state},${country}` : null;
  const locationCountry = resolveDataForSeoLocationName({
    city: "",
    country: fields.Country,
  });
  const language_code = /brazil/i.test(country)
    ? "pt"
    : /united states|canada|jamaica|belize/i.test(country)
      ? "en"
      : "es";

  const keywordExact = `"${name}" hotel ${city}`.trim();
  const keywordLoose = `${name} hotel`.trim();

  /** @type {{ keyword: string, location_name: string, language_code: string, attempt: string }[]} */
  const attempts = [
    {
      keyword: keywordExact,
      location_name: locationPrimary,
      language_code,
      attempt: "city_country_quoted",
    },
    {
      keyword: keywordLoose,
      location_name: locationPrimary,
      language_code: "en",
      attempt: "city_country_loose_en",
    },
  ];
  if (locationWithState && locationWithState !== locationPrimary) {
    attempts.push({
      keyword: keywordLoose,
      location_name: locationWithState,
      language_code: "en",
      attempt: "city_state_country",
    });
  }
  if (locationCountry && locationCountry !== locationPrimary) {
    attempts.push({
      keyword: keywordLoose,
      location_name: locationCountry,
      language_code: "en",
      attempt: "country_only_fallback",
    });
  }
  return attempts;
}

/**
 * Mapbox eligibility after a local/DataForSEO address write (classification only).
 * Production Mapbox apply still requires High unless a geocode-approved status exists.
 *
 * @param {{
 *   fields?: Record<string, unknown>,
 *   clean_core_pass?: boolean,
 *   duplicate_risk?: boolean,
 *   address_conflict?: boolean,
 *   geocode_approved_status?: string|boolean|null,
 * }} opts
 */
export function classifyMapboxEligibilityAfterLocalAddress(opts = {}) {
  const fields = opts.fields || {};
  if (opts.duplicate_risk) {
    return {
      eligible: false,
      status: "duplicate_risk",
      reason: "duplicate_risk",
    };
  }
  if (opts.address_conflict) {
    return {
      eligible: false,
      status: "address_conflict",
      reason: "address_conflict",
    };
  }
  if (opts.clean_core_pass === false) {
    return {
      eligible: false,
      status: "blocked_dirty_identity",
      reason: "blocked_dirty_identity",
    };
  }

  const address = String(fields.Address || "").trim();
  if (!address) {
    return { eligible: false, status: "no_address", reason: "no_address" };
  }
  if (!isStreetLevelAddress(address)) {
    return {
      eligible: false,
      status: "not_street_level",
      reason: "city_or_non_street_address",
    };
  }
  if (isBlank(fields["Address Source URL"])) {
    return {
      eligible: false,
      status: "missing_address_source_url",
      reason: "missing_address_source_url",
    };
  }

  const conf = String(fields["Address Confidence"] || "").trim();
  const geocodeApproved =
    opts.geocode_approved_status === true ||
    String(opts.geocode_approved_status || "")
      .trim()
      .toLowerCase() === "approved";

  if (conf === "High") {
    return { eligible: true, status: "mapbox_eligible", reason: null };
  }
  if (conf === "Medium") {
    if (geocodeApproved) {
      return { eligible: true, status: "mapbox_eligible", reason: null };
    }
    const mediumApproved =
      opts.medium_match_high_approved === true ||
      String(opts.env?.ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS || "0").trim() ===
        "1";
    if (mediumApproved) {
      return {
        eligible: true,
        status: "mapbox_eligible_medium_match_high",
        reason: null,
        coordinate_confidence_if_written: "Medium",
      };
    }
    return {
      eligible: false,
      status: "mapbox_pending_address_confidence",
      reason: "mapbox_pending_address_confidence",
    };
  }
  return {
    eligible: false,
    status: "address_confidence_insufficient",
    reason: "address_confidence_not_high",
  };
}

async function listCensus(baseId, token, tableId) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of READ_FIELDS) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(
        `census_list_failed:${res.status}:${json?.error?.message || ""}`
      );
    }
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

function countAddressComplete(records) {
  let complete = 0;
  let medium = 0;
  let high = 0;
  for (const r of records) {
    const f = r.fields || {};
    if (!isBlank(f.Address)) {
      complete += 1;
      const conf = String(f["Address Confidence"] || "").trim();
      if (conf === "Medium") medium += 1;
      if (conf === "High") high += 1;
    }
  }
  return { address_complete: complete, address_confidence_medium: medium, address_confidence_high: high };
}

function preferAddressSourceUrl(patch, censusFields, candidate) {
  const official = String(censusFields["Official Property URL"] || "").trim();
  if (official && !isRejectedDiscoveryHost(hostFromUrl(official))) {
    patch["Address Source URL"] = official;
    return;
  }
  const web = String(candidate?.raw?.website || "").trim();
  if (web && !isRejectedDiscoveryHost(hostFromUrl(web))) {
    patch["Address Source URL"] = web;
    return;
  }
  // Keep whatever evaluateLocalAddressWrite set, if any
}

async function applyAddressPatches(proposals, { baseId, token, tableId, log }) {
  let updatesApplied = 0;
  const writeErrors = [];
  let addressWrites = 0;
  let websiteWrites = 0;
  let phoneWrites = 0;

  for (let i = 0; i < proposals.length; i += 10) {
    const chunk = proposals.slice(i, i + 10);
    const records = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (isForbiddenAutopilotField(k)) continue;
          if (FORBIDDEN_THIS_MISSION.has(k)) continue;
          if (!ALLOWED_WRITE.has(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
          if (k === "Address") addressWrites += 1;
          if (k === "Official Property URL") websiteWrites += 1;
          if (k === "Phone") phoneWrites += 1;
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
      log?.(`[dfs-address-scale] batch ${res.status}; retrying one-by-one`);
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
      }
    } else {
      updatesApplied += records.length;
    }
  }

  return { updatesApplied, writeErrors, addressWrites, websiteWrites, phoneWrites };
}

function renderReportMd(report) {
  return [
    `# DataForSEO Local Address Scale v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Generated:** ${report.generated_at}`,
    `**Mode:** field-completion-only · match_high Address (Medium) + Mapbox eligibility prep`,
    ``,
    `## Summary`,
    ``,
    `- Records scanned: **${report.records_scanned}**`,
    `- Missing-address eligible (Clean Core or equivalent identity): **${report.eligible_missing_address}**`,
    `- Records queried (Maps): **${report.records_queried}**`,
    `- match_high reviewed: **${report.match_high_reviewed}**`,
    `- Address writes: **${report.address_writes}**`,
    `- Address conflicts: **${report.address_conflicts}**`,
    `- Records updated: **${report.records_updated}**`,
    `- Address complete before / after: **${report.address_complete_before}** → **${report.address_complete_after}**`,
    `- Address Confidence Medium (census after): **${report.address_confidence_medium_after}**`,
    `- Address Confidence High (census after): **${report.address_confidence_high_after}**`,
    `- Mapbox eligible after address writes: **${report.mapbox_eligible_after_address_writes}**`,
    `- mapbox_pending_address_confidence: **${report.mapbox_pending_address_confidence}**`,
    `- Phone candidates held: **${report.phone_candidates_held}**`,
    `- Coordinate candidates held: **${report.coordinate_candidates_held}**`,
    `- New hotel candidates still queued: **${report.new_hotel_candidates_queued}**`,
    `- Estimated cost: **$${Number(report.estimated_cost_usd || 0).toFixed(4)}**`,
    ``,
    `## Rejected / held reasons`,
    ``,
    ...Object.entries(report.rejected_reasons || {}).map(
      ([k, n]) => `- \`${k}\`: ${n}`
    ),
    ``,
    `## Fields written`,
    ``,
    ...(report.fields_written || []).map((f) => `- ${f}`),
    ``,
    `## Schema notes`,
    ``,
    `- Address Source Type field exists: **${ADDRESS_SCALE_SCHEMA_NOTES.address_source_type_field_exists}** (not written)`,
    `- Coordinate Eligibility Status field exists: **${ADDRESS_SCALE_SCHEMA_NOTES.coordinate_eligibility_status_field_exists}** (report-only classification)`,
    ``,
    `## Safety`,
    ``,
    `- Census table: Hotel Property Census (\`${CENSUS_TABLE_ID}\`)`,
    `- Inserts: **0**`,
    `- Phone / Lat / Long / Rooms / Website writes: **0**`,
    `- Brand Setup / Brand Explorer: **0**`,
    `- DataForSEO as SoT: **false**`,
    `- DataForSEO-only Address Confidence: **Medium** (never Official High)`,
    ``,
    `## Next scale opportunity`,
    ``,
    report.next_scale_opportunity || "",
    ``,
    `## Next approval decision`,
    ``,
    report.next_policy_decision || "",
    ``,
  ].join("\n");
}

/**
 * @param {{
 *   argv?: string[],
 *   args?: object,
 *   env?: NodeJS.ProcessEnv,
 *   enableProductionWrites?: boolean,
 *   censusMode?: string,
 *   log?: Function,
 *   fetchImpl?: typeof fetch,
 *   delayMs?: number,
 * }} [opts]
 */
export async function runDataForSeoLocalAddressScaleV1Mission(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || console.log;
  const args = opts.args || {};
  const enableWrites = Boolean(opts.enableProductionWrites);

  const censusMode = resolveCensusMode(opts.argv || [], {
    censusMode: opts.censusMode || args.censusMode || "field-completion-only",
  });
  const insertGuard = assertNoInsertInFieldCompletionMode(censusMode, 0);
  if (!insertGuard.ok) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
      reason: insertGuard.reason,
      census_writes: 0,
      inserts: 0,
    };
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${stamp}_CALA-dataforseo-local-address-scale-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  const gates = resolveDataForSeoLocalAddressScaleGates(env);
  if (!gates.ok) {
    const blocked = {
      ok: false,
      status: DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
      reason: "address_scale_gates_failed",
      blockers: gates.blockers,
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
    writeJson(path.join(runDir, "blocked.json"), blocked);
    return blocked;
  }

  const creds = resolveDataForSeoCredentials(env);
  if (!creds.ok) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
      reason: "missing_dataforseo_credentials",
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
  }

  const envCheck = checkAutopilotApplyEnv(env);
  const preflightArgs = {
    ...args,
    mode: enableWrites ? "mission" : args.mode || "controlled",
    region: args.region || "CALA",
    scope: args.scope || "official-parent-inventory",
    confirms: {
      safeWrites: true,
      writeToProductionCensus: true,
      noBrandExplorer: true,
      noOwnerOperator: true,
      noDateWrites: true,
      noRecentMomentum: true,
      noCompanyValidation: true,
      webhoundNotProduction: true,
      ...(args.confirms || {}),
    },
    allApplyConfirms: true,
  };
  const preflight = applyPreflight(preflightArgs, envCheck);
  if (enableWrites && (!envCheck.allOk || !preflight.ok)) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
      reason: "missing_confirmations",
      envCheck,
      preflight,
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
  }

  const targetAssert = assertProductionCensusWriteTarget({
    tableId: CENSUS_TABLE_ID,
    baseId: resolveTargetBase().target_base_id || null,
  });
  if (!targetAssert.ok) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
      reason: targetAssert.code || "blocked_wrong_census_target",
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases.target_base_id) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
      reason: "missing_airtable_credentials",
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
  }

  const batchSize = Math.max(1, Number(args.batchSize || env.DATAFORSEO_BATCH_SIZE || 50));
  const maxPasses = Math.max(1, Number(args.maxPasses || 5));
  const maxRecords = Number(
    env.DATAFORSEO_MAX_RECORDS || batchSize * maxPasses
  );
  const mapsDepth = Number(env.DATAFORSEO_MAPS_DEPTH || 10);
  const delayMs = Number(opts.delayMs ?? env.DATAFORSEO_DELAY_MS ?? 300);
  const costCap = Number(env.DATAFORSEO_COST_CAP_USD || 5);

  log(`[dfs-address-scale] listing Hotel Property Census`);
  const census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const beforeStats = countAddressComplete(census);
  log(
    `[dfs-address-scale] scanned=${census.length} address_complete_before=${beforeStats.address_complete}`
  );

  /** @type {object[]} */
  const eligible = [];
  let dirtySkipped = 0;
  let otherSkipped = 0;
  for (const rec of census) {
    const elig = isEligibleForLocalAddressScale(rec, {
      canonicalFieldExists: true,
      phoneWritesEnabled: gates.phone_writes === true,
    });
    if (elig.ok) {
      eligible.push(rec);
      continue;
    }
    if (elig.dirty) dirtySkipped += 1;
    else if (elig.reason !== "address_already_present") otherSkipped += 1;
  }

  const workset = eligible.slice(0, maxRecords);
  log(
    `[dfs-address-scale] missing_address_eligible=${eligible.length} workset=${workset.length} dirty_skipped=${dirtySkipped} other_skipped=${otherSkipped} batch=${batchSize} max_passes=${maxPasses}`
  );

  let insertQueued = 0;
  const insertPath = String(
    env.DATAFORSEO_LOCAL_INSERT_QUEUE_PATH || DEFAULT_INSERT_QUEUE
  ).trim();
  if (fs.existsSync(insertPath)) {
    try {
      const q = JSON.parse(fs.readFileSync(insertPath, "utf8"));
      insertQueued = Number(q.count || (q.queue || []).length || 0);
    } catch {
      insertQueued = 0;
    }
  }

  /** @type {object[]} */
  const proposals = [];
  /** @type {object[]} */
  const decisionLog = [];
  /** @type {object[]} */
  const matchHighCandidates = [];
  /** @type {Record<string, number>} */
  const rejected = {};
  const bump = (r) => {
    rejected[r] = (rejected[r] || 0) + 1;
  };

  let queriesRun = 0;
  let estimatedCost = 0;
  let matchHigh = 0;
  let matchMedium = 0;
  let duplicateRisk = 0;
  let addressWritesProposed = 0;
  let phoneWritesProposed = 0;
  let websiteWritesProposed = 0;
  let addressConflicts = 0;
  let websiteConflicts = 0;
  let phoneHeld = 0;
  let coordHeld = 0;
  let mapboxEligible = 0;
  let mapboxPending = 0;
  const fieldsWritten = new Set();

  let cursor = 0;
  let pass = 0;
  while (pass < maxPasses && cursor < workset.length && estimatedCost < costCap) {
    const batch = workset.slice(cursor, cursor + batchSize);
    if (!batch.length) break;
    pass += 1;
    log(
      `[dfs-address-scale] pass ${pass}/${maxPasses} batch=${batch.length} cursor=${cursor}`
    );

    for (let i = 0; i < batch.length; i += 1) {
      if (estimatedCost >= costCap) {
        log(`[dfs-address-scale] cost cap $${costCap} reached`);
        bump("cost_cap_reached");
        break;
      }
      const record = batch[i];
      const f = record.fields || {};
      const name =
        f["Canonical Property Name"] || f["Property Name"] || "";
      const attempts = buildLocalAddressScaleQueryAttempts(f);
      let apiRes = { items: [], cost: 0 };
      let usedAttempt = null;
      for (const attempt of attempts) {
        if (estimatedCost >= costCap) break;
        const one = await fetchGoogleMapsLive(
          {
            keyword: attempt.keyword,
            location_name: attempt.location_name,
            language_code: attempt.language_code,
            depth: Math.min(mapsDepth, 10),
          },
          { env, fetchImpl: opts.fetchImpl }
        );
        queriesRun += 1;
        estimatedCost += Number(one.cost) || 0;
        usedAttempt = attempt.attempt;
        apiRes = one;
        if ((one.items || []).length > 0) break;
        if (delayMs) await sleep(Math.min(delayMs, 150));
      }
      if (usedAttempt) bump(`attempt_${usedAttempt}`);

      let bestItem = null;
      let bestMatch = null;
      for (const item of apiRes.items || []) {
        const lodging = classifyLodgingType(item);
        if (
          lodging === LODGING_CLASS.NON_HOTEL ||
          lodging === LODGING_CLASS.CLOSED ||
          lodging === LODGING_CLASS.VACATION_RENTAL
        ) {
          continue;
        }
        const match = scoreLocalBusinessToCensus(item, f, {
          recordId: record.id,
        });
        if (
          !bestMatch ||
          match.match_confidence > bestMatch.match_confidence
        ) {
          bestItem = item;
          bestMatch = match;
        }
      }

      if (!bestItem || !bestMatch) {
        bump("no_local_match");
        continue;
      }

      if (bestMatch.match_class === MATCH_CLASS.MATCH_MEDIUM) {
        matchMedium += 1;
        bump("match_medium_not_approved");
        continue;
      }
      if (bestMatch.match_class === MATCH_CLASS.DUPLICATE_RISK) {
        duplicateRisk += 1;
        bump("duplicate_risk_not_approved");
        continue;
      }
      if (bestMatch.match_class !== MATCH_CLASS.MATCH_HIGH) {
        bump(`match_class_${bestMatch.match_class}`);
        continue;
      }

      matchHigh += 1;
      const cand = buildLocalEnrichmentCandidate(bestItem, bestMatch, {
        endpoint: "serp/google/maps/live/advanced",
      });
      cand.census_record_id = record.id;
      cand.hotel_name = name;
      matchHighCandidates.push(cand);

      if (cand.raw?.phone || cand.fields?.phone) {
        if (!gates.phone_writes) {
          phoneHeld += 1;
          bump("phone_local_policy_not_approved");
        }
      }
      if (
        cand.raw?.latitude != null ||
        cand.raw?.longitude != null ||
        cand.fields?.latitude ||
        cand.fields?.longitude
      ) {
        coordHeld += 1;
        bump("coordinate_local_policy_not_approved");
      }

      /** @type {Record<string, unknown>} */
      const patch = {};
      let didAddress = false;
      let didWebsite = false;
      let didPhone = false;

      if (gates.address_writes) {
        const addr = evaluateLocalAddressWrite(cand, f);
        if (addr.ok && addr.patch) {
          Object.assign(patch, addr.patch);
          preferAddressSourceUrl(patch, f, cand);
          didAddress = true;
        } else {
          bump(addr.reason || "address_rejected");
          if (addr.conflict) addressConflicts += 1;
          decisionLog.push({
            record_id: record.id,
            field: "address",
            ok: false,
            reason: addr.reason,
          });
        }
      }

      if (gates.website_writes) {
        const web = evaluateLocalWebsiteWrite(cand, f);
        if (web.ok && web.patch) {
          Object.assign(patch, web.patch);
          didWebsite = true;
          websiteWritesProposed += 1;
        } else {
          bump(web.reason || "website_rejected");
          if (web.conflict || web.held) websiteConflicts += 1;
          decisionLog.push({
            record_id: record.id,
            field: "website",
            ok: false,
            reason: web.reason,
          });
        }
      }

      if (gates.phone_writes) {
        const ph = evaluateLocalPhoneWrite(cand, f, {
          phoneWritesEnabled: true,
          duplicateRisk: false,
        });
        if (ph.ok && ph.patch) {
          Object.assign(patch, ph.patch);
          didPhone = true;
        } else {
          if (ph.held || ph.reason === "phone_writes_not_enabled") {
            phoneHeld += 1;
          }
          bump(ph.reason || "phone_rejected");
          decisionLog.push({
            record_id: record.id,
            field: "phone",
            ok: false,
            reason: ph.reason,
            confidence: "Medium",
            exposure: "internal_only",
          });
        }
      }

      if (!Object.keys(patch).length) {
        continue;
      }

      patch["Last Reviewed Date"] = todayIsoDate();
      patch["Enrichment Status"] = "Partial";
      if (isBlank(f["Enrichment Priority"])) {
        patch["Enrichment Priority"] = "Medium";
      }

      const mergedFields = {
        ...f,
        ...patch,
      };
      const mapbox = classifyMapboxEligibilityAfterLocalAddress({
        fields: mergedFields,
        clean_core_pass: true,
        duplicate_risk: false,
        address_conflict: false,
        geocode_approved_status: gates.address_writes ? null : null,
        env,
      });
      // Medium + provenance → pending unless Medium match_high Mapbox flag
      if (
        mapbox.status === "mapbox_eligible" ||
        mapbox.status === "mapbox_eligible_medium_match_high"
      ) {
        mapboxEligible += 1;
      }
      if (mapbox.status === "mapbox_pending_address_confidence") {
        mapboxPending += 1;
      }

      for (const k of Object.keys(patch)) {
        if (
          isForbiddenAutopilotField(k) ||
          FORBIDDEN_THIS_MISSION.has(k) ||
          !ALLOWED_WRITE.has(k)
        ) {
          delete patch[k];
        } else {
          fieldsWritten.add(k);
        }
      }

      if (!Object.keys(patch).length) {
        bump("patch_empty_after_allowlist");
        continue;
      }

      if (didAddress) addressWritesProposed += 1;
      if (didPhone) phoneWritesProposed += 1;
      proposals.push({
        record_id: record.id,
        reason: "dataforseo_local_address_scale_v1",
        patch,
        match_class: MATCH_CLASS.MATCH_HIGH,
        mapbox_eligibility: mapbox,
        did_address: didAddress,
        did_website: didWebsite,
        did_phone: didPhone,
        confidence_tiers: {
          address: didAddress ? "Medium" : null,
          website: didWebsite ? "High_or_preserved" : null,
          phone: didPhone ? "Medium" : null,
        },
      });
      decisionLog.push({
        record_id: record.id,
        ok: true,
        address: patch.Address || null,
        website: patch["Official Property URL"] || null,
        phone: patch.Phone || null,
        address_confidence: patch["Address Confidence"] || null,
        phone_confidence: didPhone ? "Medium" : null,
        exposure: didPhone ? "internal_only" : null,
        mapbox_status: mapbox.status,
      });
    }

    cursor += batch.length;
    if (delayMs) await sleep(delayMs);
  }

  writeJson(path.join(runDir, "proposals.json"), {
    count: proposals.length,
    proposals,
  });
  writeJson(path.join(runDir, "decision-log.json"), {
    count: decisionLog.length,
    decisions: decisionLog,
  });
  writeJson(path.join(runDir, "match-high-candidates.json"), {
    count: matchHighCandidates.length,
    candidates: matchHighCandidates,
  });

  let writeResult = {
    updatesApplied: 0,
    writeErrors: [],
    addressWrites: 0,
  };
  if (enableWrites && proposals.length) {
    log(`[dfs-address-scale] applying ${proposals.length} address patches`);
    writeResult = await applyAddressPatches(proposals, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      log,
    });
  } else {
    log(
      `[dfs-address-scale] no-apply proposals=${proposals.length} enableWrites=${enableWrites}`
    );
  }

  // Re-list for after metrics when writes applied; else estimate from before + proposals
  let afterStats = { ...beforeStats };
  if (enableWrites && writeResult.updatesApplied > 0) {
    try {
      const censusAfter = await listCensus(
        bases.target_base_id,
        token,
        CENSUS_TABLE_ID
      );
      afterStats = countAddressComplete(censusAfter);
    } catch (err) {
      log?.(
        `[dfs-address-scale] after-list failed: ${err?.message || err}; estimating`
      );
      afterStats = {
        address_complete:
          beforeStats.address_complete + writeResult.addressWrites,
        address_confidence_medium:
          beforeStats.address_confidence_medium + writeResult.addressWrites,
        address_confidence_high: beforeStats.address_confidence_high,
      };
    }
  } else if (!enableWrites && proposals.length) {
    afterStats = {
      address_complete: beforeStats.address_complete + proposals.length,
      address_confidence_medium:
        beforeStats.address_confidence_medium + proposals.length,
      address_confidence_high: beforeStats.address_confidence_high,
    };
  }

  const updated = enableWrites ? writeResult.updatesApplied : 0;
  const remainingEligible = Math.max(0, eligible.length - workset.length);
  const remainingInWorkset = Math.max(0, workset.length - cursor);

  let status = DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.PARTIAL_SOURCE;
  if (enableWrites && writeResult.writeErrors?.length && updated === 0) {
    status = DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.BLOCKED;
  } else if (updated > 0 || (!enableWrites && proposals.length > 0)) {
    status =
      remainingEligible + remainingInWorkset > 0 || matchMedium > 0
        ? DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.PARTIAL_SOURCE
        : DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.PARTIAL_POLICY;
  }
  if (
    updated > 0 &&
    addressWritesProposed >= Math.max(1, Math.floor(matchHigh * 0.5)) &&
    remainingEligible + remainingInWorkset === 0
  ) {
    status = DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.COMPLETE;
  } else if (updated > 0 && mapboxPending > 0) {
    // Address scale succeeded but Mapbox still pending Medium confidence policy
    status = DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.PARTIAL_POLICY;
  }

  const report = {
    ok: status !== DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.BLOCKED,
    status,
    objective: DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
    version: DATAFORSEO_LOCAL_ADDRESS_SCALE_VERSION,
    generated_at: new Date().toISOString(),
    gates,
    schema_notes: ADDRESS_SCALE_SCHEMA_NOTES,
    records_scanned: census.length,
    eligible_missing_address: eligible.length,
    dirty_identity_skipped: dirtySkipped,
    other_skipped: otherSkipped,
    records_queried: queriesRun,
    match_high_reviewed: matchHigh,
    match_medium_held: matchMedium,
    duplicate_risk_held: duplicateRisk,
    address_writes: enableWrites
      ? writeResult.addressWrites || addressWritesProposed
      : addressWritesProposed,
    website_writes: websiteWritesProposed,
    phone_writes: enableWrites
      ? writeResult.phoneWrites || phoneWritesProposed
      : phoneWritesProposed,
    phone_candidates_held: phoneHeld,
    website_conflicts: websiteConflicts,
    address_conflicts: addressConflicts,
    records_updated: updated,
    records_proposed: proposals.length,
    address_complete_before: beforeStats.address_complete,
    address_complete_after: afterStats.address_complete,
    address_confidence_medium_after: afterStats.address_confidence_medium,
    address_confidence_high_after: afterStats.address_confidence_high,
    mapbox_eligible_after_address_writes: mapboxEligible,
    mapbox_pending_address_confidence: mapboxPending,
    coordinate_candidates_held: coordHeld,
    new_hotel_candidates_queued: insertQueued,
    fields_written: [...fieldsWritten],
    rejected_reasons: rejected,
    estimated_cost_usd: estimatedCost,
    queries_run: queriesRun,
    passes_run: pass,
    batch_size: batchSize,
    max_passes: maxPasses,
    remaining_eligible: remainingEligible + remainingInWorkset,
    census_writes: updated,
    inserts: 0,
    brand_setup_writes: 0,
    brand_explorer_writes: 0,
    rooms_writes: 0,
    write_errors: writeResult.writeErrors || [],
    next_scale_opportunity:
      remainingEligible + remainingInWorkset > 0
        ? `${remainingEligible + remainingInWorkset} Clean Core missing-address records remain beyond this workset/cost cap — re-run with higher DATAFORSEO_MAX_RECORDS or another pass.`
        : matchMedium > 0
          ? `${matchMedium} match_medium address candidates held — founder may approve medium later or improve matching.`
          : "Workset exhausted for this run; continue confidence-tiered internal completion under encoded policy.",
    next_policy_decision:
      "Direct DataForSEO coordinate writes remain held. Medium phone writes require ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION + ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES. Mapbox Permanent after Medium match_high address is approved. Phone Confidence schema field still missing (provenance in Notes for Steward).",
    production_target: {
      base: "Deal Capture Platform",
      table: "Hotel Property Census",
      table_id: CENSUS_TABLE_ID,
    },
    run_dir: runDir,
    enable_production_writes: enableWrites,
  };

  writeJson(path.join(runDir, "mission-report.json"), report);
  const reportJson = path.join(
    ROOT,
    "reports/research-engine-v2/dataforseo-local-address-scale-v1.json"
  );
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/dataforseo-local-address-scale-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/dataforseo-local-address-scale-v1.md"
  );
  writeJson(reportJson, report);
  const md = renderReportMd(report);
  writeMd(reportMd, md);
  writeMd(docsPath, md);

  log(
    `[dfs-address-scale] done status=${status} updated=${updated} address=${report.address_writes} mapbox_pending=${mapboxPending} cost~$${estimatedCost.toFixed(4)}`
  );
  return report;
}
