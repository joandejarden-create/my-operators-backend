/**
 * DataForSEO Local Business Validated Write v1 — Website + Address + optional Medium phone.
 * match_high only. No direct DataForSEO coords / rooms / Brand Explorer.
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
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import {
  isBrandOfficialHost,
  isTrustedSecondaryHost,
} from "./dataforseo-candidate-classifier.js";
import { isRejectedDiscoveryHost } from "./dataforseo-validated-write-policy.js";
import { MATCH_CLASS } from "./dataforseo-local-match.js";
import { nameSimilarity } from "../independent-census/match-current-census.js";
import { tokenSimilarity } from "./adapters/adapter-utils.js";
import {
  isChoiceCentralReservationPhone,
  normalizePhoneNumber,
} from "./census-phone-number-enrichment.js";
import {
  buildPhoneProvenanceNote,
  mergeStewardPhoneNote,
} from "./census-confidence-tiered-internal-completion.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE =
  "dataforseo-local-business-validated-write-v1";
export const DATAFORSEO_LOCAL_VALIDATED_WRITE_VERSION =
  "dataforseo-local-business-validated-write-v1";

export const DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS = Object.freeze({
  COMPLETE:
    "production_census_dataforseo_local_business_validated_write_v1_complete",
  PARTIAL_POLICY:
    "production_census_dataforseo_local_business_validated_write_v1_partial_policy_decision_needed",
  PARTIAL_SOURCE:
    "production_census_dataforseo_local_business_validated_write_v1_partial_source_remaining",
  BLOCKED:
    "production_census_dataforseo_local_business_validated_write_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const DEFAULT_ENRICHMENT_CANDIDATES = path.join(
  ROOT,
  "reports/research-engine-v2/autopilot/2026-08-07T22-22-14_CALA-dataforseo-local-business-enrichment-v1/enrichment-candidates.json"
);

const DEFAULT_INSERT_QUEUE = path.join(
  ROOT,
  "reports/research-engine-v2/autopilot/2026-08-07T22-22-14_CALA-dataforseo-local-business-enrichment-v1/candidate-insert-queue.json"
);

const ALLOWED_WRITE = new Set([
  "Official Property URL",
  "Source URL",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Phone",
  "Notes for Steward",
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
  "Owner",
  "Owner Name",
  "Operator",
  "Operator Name",
  "Developer",
  "Opening Date",
  "Renovation Date",
  "Recent Momentum",
  "Company Validated",
  "Brand Verified",
  "Brand Status",
]);

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
export function resolveDataForSeoLocalValidatedWriteGates(env = process.env) {
  const enabled = String(env.DATAFORSEO_ENABLED || "0").trim() === "1";
  const candidatesOnly =
    String(env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "0").trim() === "1";
  const validated =
    String(env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0").trim() === "1";
  const website =
    String(env.ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES || "0").trim() === "1";
  const address =
    String(env.ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES || "0").trim() === "1";
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
    blockers.push("DATAFORSEO_WRITE_CANDIDATES_ONLY_must_be_0_for_validated_writes");
  }
  if (!validated) blockers.push("ENABLE_DATAFORSEO_VALIDATED_WRITES_must_be_1");
  if (!website && !address && !(phone && internalMedium)) {
    blockers.push("website_or_address_or_phone_write_flag_required");
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
    website_writes: website,
    address_writes: address,
    phone_writes: Boolean(phone && internalMedium),
    coordinate_writes: false,
    rooms_from_maps: false,
    inserts: false,
    match_high_only: true,
    internal_medium_completion: internalMedium,
  };
}

/**
 * Website write eligibility for local/business candidate.
 */
export function evaluateLocalWebsiteWrite(candidate, censusFields = {}) {
  const url = String(
    candidate?.raw?.website || candidate?.fields?.website?.value || ""
  ).trim();
  if (!url) {
    return { ok: false, reason: "no_website_candidate", patch: null };
  }
  if (candidate?.match_class !== MATCH_CLASS.MATCH_HIGH) {
    return { ok: false, reason: "not_match_high", patch: null };
  }
  const host = hostFromUrl(url);
  if (!host || isRejectedDiscoveryHost(host) || isTrustedSecondaryHost(host)) {
    return { ok: false, reason: "rejected_ota_affiliate_or_directory_host", patch: null };
  }
  // Require brand-official OR independent hotel-looking host (not maps google)
  const brand = isBrandOfficialHost(host);
  const independentOk =
    !/google\./i.test(host) &&
    (/hotel|resort|inn|suites|lodge|posada/i.test(host) ||
      /hoteldetail|\/hotels?\//i.test(url) ||
      brand);
  if (!brand && !independentOk) {
    // Allow clean independent domains when name already matched high
    if (!/\.(com|mx|co|do|pa|cr|pe|cl|ar|br|net|org)$/i.test(host)) {
      return { ok: false, reason: "website_host_not_approved", patch: null };
    }
  }

  const existing = String(censusFields["Official Property URL"] || "").trim();
  if (!isBlank(existing)) {
    const existingHost = hostFromUrl(existing);
    if (isBrandOfficialHost(existingHost)) {
      return {
        ok: false,
        reason: "existing_brand_official_url_preserved",
        patch: null,
        held: true,
      };
    }
    if (existingHost === host) {
      return { ok: false, reason: "website_already_same_host", patch: null };
    }
    return {
      ok: false,
      reason: "website_conflict_existing_url",
      patch: null,
      conflict: true,
      existing,
      candidate: url,
    };
  }

  return {
    ok: true,
    reason: null,
    patch: {
      "Official Property URL": url,
      ...(isBlank(censusFields["Source URL"]) ? { "Source URL": url } : {}),
    },
  };
}

/**
 * Address write eligibility for local/business candidate.
 */
export function evaluateLocalAddressWrite(candidate, censusFields = {}) {
  const address = String(
    candidate?.raw?.address || candidate?.fields?.address?.value || ""
  ).trim();
  if (!address) {
    return { ok: false, reason: "no_address_candidate", patch: null };
  }
  if (candidate?.match_class !== MATCH_CLASS.MATCH_HIGH) {
    return { ok: false, reason: "not_match_high", patch: null };
  }
  if (!isStreetLevelAddress(address)) {
    return { ok: false, reason: "address_not_street_level", patch: null };
  }

  const hotelName =
    censusFields["Canonical Property Name"] ||
    censusFields["Property Name"] ||
    candidate?.hotel_name ||
    "";
  const title = candidate?.raw?.title || "";
  const nameSim = Math.max(
    nameSimilarity(hotelName, title),
    tokenSimilarity(hotelName, title)
  );
  if (hotelName && title && nameSim < 0.35) {
    return { ok: false, reason: "address_name_mismatch", patch: null };
  }

  const city = String(censusFields.City || "").toLowerCase();
  if (city.length > 2 && !address.toLowerCase().includes(city)) {
    // Soft: many MX addresses use municipality names; don't hard-fail if country present
    const country = String(censusFields.Country || "").toLowerCase();
    if (country && !address.toLowerCase().includes(country.split(" ")[0])) {
      // still allow if match_high already city-matched during enrichment
    }
  }

  const existing = String(censusFields.Address || "").trim();
  if (!isBlank(existing)) {
    const same =
      existing.toLowerCase().replace(/\s+/g, " ") ===
      address.toLowerCase().replace(/\s+/g, " ");
    if (same) {
      return { ok: false, reason: "address_already_same", patch: null };
    }
    return {
      ok: false,
      reason: "address_conflict",
      patch: null,
      conflict: true,
      existing,
      candidate: address,
    };
  }

  const sourceUrl =
    String(candidate?.raw?.website || "").trim() ||
    String(censusFields["Official Property URL"] || "").trim() ||
    null;

  /** @type {Record<string, unknown>} */
  const patch = {
    Address: address,
    "Address Confidence": "Medium",
    "Last Reviewed Date": todayIsoDate(),
    "Enrichment Status": "Partial",
  };
  if (sourceUrl && !isRejectedDiscoveryHost(hostFromUrl(sourceUrl))) {
    patch["Address Source URL"] = sourceUrl;
  }

  return { ok: true, reason: null, patch };
}

/**
 * Detect central-reservation / non-property phone indicators.
 */
export function hasCentralReservationPhoneIndicator(phone, context = {}) {
  if (isChoiceCentralReservationPhone(phone)) return true;
  const digits = String(phone || "").replace(/[^\d]/g, "");
  const blob = [
    context.title,
    context.category,
    context.snippet,
    context.raw_label,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");
  if (/central\s*reserv|reservation(s)?\s*(center|centre|line|desk)|call\s*center|toll[- ]?free\s*reserv/i.test(blob)) {
    return true;
  }
  // Shared US toll-free brand lines without property extension — reject when
  // only 1-8xx with no local area-code length for the market.
  if (/^1?8(00|33|44|55|66|77|88)\d{7}$/.test(digits) && /marriott|hilton|hyatt|ihg|holiday\s*inn|wyndham|choice|best\s*western|accor/i.test(blob)) {
    return true;
  }
  return false;
}

/**
 * Medium phone write from DataForSEO local match_high (internal census only).
 * Phone Confidence field does not exist — provenance goes to Notes for Steward.
 */
export function evaluateLocalPhoneWrite(candidate, censusFields = {}, opts = {}) {
  if (opts.phoneWritesEnabled !== true) {
    return { ok: false, reason: "phone_writes_not_enabled", patch: null, held: true };
  }
  if (candidate?.match_class !== MATCH_CLASS.MATCH_HIGH) {
    return { ok: false, reason: "not_match_high", patch: null };
  }
  if (
    candidate?.match?.match_class === MATCH_CLASS.DUPLICATE_RISK ||
    opts.duplicateRisk === true
  ) {
    return { ok: false, reason: "duplicate_risk", patch: null };
  }

  const rawPhone = String(
    candidate?.raw?.phone || candidate?.fields?.phone?.value || ""
  ).trim();
  const phone = normalizePhoneNumber(rawPhone);
  if (!phone) {
    return { ok: false, reason: "no_phone_candidate", patch: null };
  }

  const hotelName =
    censusFields["Canonical Property Name"] ||
    censusFields["Property Name"] ||
    candidate?.hotel_name ||
    "";
  const title = candidate?.raw?.title || "";
  const nameSim = Math.max(
    nameSimilarity(hotelName, title),
    tokenSimilarity(hotelName, title)
  );
  if (hotelName && title && nameSim < 0.45) {
    return { ok: false, reason: "phone_name_mismatch_not_exact", patch: null };
  }

  if (
    hasCentralReservationPhoneIndicator(phone, {
      title,
      category: candidate?.raw?.category,
      snippet: candidate?.raw?.snippet,
    })
  ) {
    return {
      ok: false,
      reason: "central_reservation_phone_rejected",
      patch: null,
    };
  }

  const existing = normalizePhoneNumber(censusFields.Phone);
  if (existing) {
    const a = String(existing).replace(/[^\d]/g, "");
    const b = String(phone).replace(/[^\d]/g, "");
    if (a === b || a.endsWith(b) || b.endsWith(a)) {
      return { ok: false, reason: "phone_already_same", patch: null };
    }
    return {
      ok: false,
      reason: "phone_conflict_existing",
      patch: null,
      conflict: true,
      existing,
      candidate: phone,
    };
  }

  const sourceUrl =
    String(candidate?.raw?.website || "").trim() ||
    String(censusFields["Official Property URL"] || "").trim() ||
    String(censusFields["Address Source URL"] || "").trim() ||
    null;

  const note = buildPhoneProvenanceNote({
    confidence: "Medium",
    source: "dataforseo_local_match_high",
    source_url: sourceUrl,
    place_id: candidate?.raw?.place_id || null,
    match_class: candidate?.match_class || MATCH_CLASS.MATCH_HIGH,
  });

  /** @type {Record<string, unknown>} */
  const patch = {
    Phone: phone,
    "Last Reviewed Date": todayIsoDate(),
    "Enrichment Status": "Partial",
    "Notes for Steward": mergeStewardPhoneNote(
      censusFields["Notes for Steward"],
      note
    ),
  };

  return {
    ok: true,
    reason: null,
    patch,
    confidence: "Medium",
    exposure: "internal_only",
    schema_note:
      "Phone Confidence field missing — provenance stored in Notes for Steward",
  };
}

async function fetchCensusRecord(baseId, token, tableId, id) {
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}/${encodeURIComponent(id)}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) return null;
  return json;
}

async function applyPatches(proposals, { baseId, token, tableId, log }) {
  let updatesApplied = 0;
  const writeErrors = [];
  let websiteWrites = 0;
  let addressWrites = 0;

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
          if (k === "Official Property URL") websiteWrites += 1;
          if (k === "Address") addressWrites += 1;
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
      log?.(`[dfs-local-write] batch ${res.status}; retrying one-by-one`);
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

  return { updatesApplied, writeErrors, websiteWrites, addressWrites };
}

function renderReportMd(report) {
  return [
    `# DataForSEO Local Business Validated Write v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Generated:** ${report.generated_at}`,
    `**Mode:** field-completion-only · match_high website + address only`,
    ``,
    `## Summary`,
    ``,
    `- match_high reviewed: **${report.match_high_reviewed}**`,
    `- Website writes: **${report.website_writes}**`,
    `- Address writes: **${report.address_writes}**`,
    `- Records updated: **${report.records_updated}**`,
    `- Address conflicts: **${report.address_conflicts}**`,
    `- Website conflicts / preserved: **${report.website_conflicts}**`,
    `- Phone candidates held: **${report.phone_candidates_held}**`,
    `- Coordinate candidates held: **${report.coordinate_candidates_held}**`,
    `- New hotel candidates still queued: **${report.new_hotel_candidates_queued}**`,
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
    `## Safety`,
    ``,
    `- Census table: Hotel Property Census (\`${CENSUS_TABLE_ID}\`)`,
    `- Inserts: **0**`,
    `- Phone / Lat / Long / Rooms writes: **0**`,
    `- Brand Setup / Brand Explorer: **0**`,
    `- DataForSEO as SoT: **false**`,
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
 * }} [opts]
 */
export async function runDataForSeoLocalBusinessValidatedWriteV1Mission(
  opts = {}
) {
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
      status: DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
      reason: insertGuard.reason,
      census_writes: 0,
      inserts: 0,
    };
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${stamp}_CALA-dataforseo-local-business-validated-write-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  const gates = resolveDataForSeoLocalValidatedWriteGates(env);
  if (!gates.ok) {
    const blocked = {
      ok: false,
      status: DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
      reason: "validated_write_gates_failed",
      blockers: gates.blockers,
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
    writeJson(path.join(runDir, "blocked.json"), blocked);
    return blocked;
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
      status: DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
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
      status: DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
      reason: targetAssert.code || "blocked_wrong_census_target",
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
  }

  const candidatesPath = String(
    env.DATAFORSEO_LOCAL_ENRICHMENT_CANDIDATES_PATH ||
      DEFAULT_ENRICHMENT_CANDIDATES
  ).trim();
  if (!fs.existsSync(candidatesPath)) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
      reason: "enrichment_candidates_missing",
      candidates_path: candidatesPath,
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
  }

  const loaded = JSON.parse(fs.readFileSync(candidatesPath, "utf8"));
  const all = loaded.candidates || [];
  const matchHigh = all.filter((c) => c.match_class === MATCH_CLASS.MATCH_HIGH);

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

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases.target_base_id) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
      reason: "missing_airtable_credentials",
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
  }

  log(
    `[dfs-local-write] match_high=${matchHigh.length} from ${candidatesPath}`
  );

  /** @type {object[]} */
  const proposals = [];
  /** @type {Record<string, number>} */
  const rejected = {};
  const bump = (r) => {
    rejected[r] = (rejected[r] || 0) + 1;
  };

  let websiteWrites = 0;
  let addressWrites = 0;
  let addressConflicts = 0;
  let websiteConflicts = 0;
  let phoneHeld = 0;
  let coordHeld = 0;
  const fieldsWritten = new Set();
  /** @type {object[]} */
  const decisionLog = [];

  for (let i = 0; i < matchHigh.length; i += 1) {
    const cand = matchHigh[i];
    const recordId = cand.census_record_id || cand.matched_census_record_id;
    if (!recordId) {
      bump("missing_census_record_id");
      continue;
    }

    const rec = await fetchCensusRecord(
      bases.target_base_id,
      token,
      CENSUS_TABLE_ID,
      recordId
    );
    if (!rec) {
      bump("census_record_not_found");
      continue;
    }
    const f = rec.fields || {};

    if (cand.raw?.phone || cand.fields?.phone) {
      phoneHeld += 1;
      bump("phone_local_policy_not_approved");
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
    const merged = {};

    if (gates.website_writes) {
      const web = evaluateLocalWebsiteWrite(cand, f);
      if (web.ok && web.patch) {
        Object.assign(merged, web.patch);
        websiteWrites += 1;
        decisionLog.push({
          record_id: recordId,
          field: "website",
          ok: true,
          url: web.patch["Official Property URL"],
        });
      } else {
        bump(web.reason || "website_rejected");
        if (web.conflict || web.held) websiteConflicts += 1;
        decisionLog.push({
          record_id: recordId,
          field: "website",
          ok: false,
          reason: web.reason,
        });
      }
    }

    if (gates.address_writes) {
      const addr = evaluateLocalAddressWrite(cand, f);
      if (addr.ok && addr.patch) {
        Object.assign(merged, addr.patch);
        addressWrites += 1;
        decisionLog.push({
          record_id: recordId,
          field: "address",
          ok: true,
          address: addr.patch.Address,
        });
      } else {
        bump(addr.reason || "address_rejected");
        if (addr.conflict) addressConflicts += 1;
        decisionLog.push({
          record_id: recordId,
          field: "address",
          ok: false,
          reason: addr.reason,
        });
      }
    }

    if (Object.keys(merged).length) {
      merged["Last Reviewed Date"] = todayIsoDate();
      if (!merged["Enrichment Status"]) merged["Enrichment Status"] = "Partial";
      for (const k of Object.keys(merged)) {
        if (
          isForbiddenAutopilotField(k) ||
          FORBIDDEN_THIS_MISSION.has(k) ||
          !ALLOWED_WRITE.has(k)
        ) {
          delete merged[k];
        } else {
          fieldsWritten.add(k);
        }
      }
      if (Object.keys(merged).length) {
        proposals.push({
          record_id: recordId,
          reason: "dataforseo_local_business_validated_write_v1",
          patch: merged,
          match_class: MATCH_CLASS.MATCH_HIGH,
        });
      }
    }

    if (i % 10 === 9) await sleep(150);
  }

  writeJson(path.join(runDir, "proposals.json"), {
    count: proposals.length,
    proposals,
  });
  writeJson(path.join(runDir, "decision-log.json"), {
    count: decisionLog.length,
    decisions: decisionLog,
  });

  let writeResult = {
    updatesApplied: 0,
    writeErrors: [],
    websiteWrites: 0,
    addressWrites: 0,
  };
  if (enableWrites && proposals.length) {
    log(`[dfs-local-write] applying ${proposals.length} patches`);
    writeResult = await applyPatches(proposals, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      log,
    });
  } else {
    log(
      `[dfs-local-write] no-apply proposals=${proposals.length} enableWrites=${enableWrites}`
    );
  }

  let status = DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.PARTIAL_SOURCE;
  const updated = enableWrites ? writeResult.updatesApplied : 0;
  if (enableWrites && writeResult.writeErrors?.length && updated === 0) {
    status = DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.BLOCKED;
  } else if (updated > 0 || (!enableWrites && proposals.length > 0)) {
    status = DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.PARTIAL_POLICY;
  }
  if (
    updated > 0 &&
    addressWrites + websiteWrites >= Math.max(1, Math.floor(matchHigh.length * 0.5))
  ) {
    status = DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.COMPLETE;
  }

  const report = {
    ok: status !== DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.BLOCKED,
    status,
    objective: DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
    version: DATAFORSEO_LOCAL_VALIDATED_WRITE_VERSION,
    generated_at: new Date().toISOString(),
    gates,
    candidates_path: candidatesPath,
    match_high_reviewed: matchHigh.length,
    website_writes: enableWrites
      ? writeResult.websiteWrites || websiteWrites
      : websiteWrites,
    address_writes: enableWrites
      ? writeResult.addressWrites || addressWrites
      : addressWrites,
    records_updated: updated,
    records_proposed: proposals.length,
    fields_written: [...fieldsWritten],
    rejected_reasons: rejected,
    address_conflicts: addressConflicts,
    website_conflicts: websiteConflicts,
    phone_candidates_held: phoneHeld,
    coordinate_candidates_held: coordHeld,
    new_hotel_candidates_queued: insertQueued,
    census_writes: updated,
    inserts: 0,
    brand_setup_writes: 0,
    brand_explorer_writes: 0,
    rooms_writes: 0,
    write_errors: writeResult.writeErrors || [],
    next_policy_decision:
      "Spot-check address Medium writes; decide phone/coordinate approvals separately; keep new-hotel insert queue held until growth mode + duplicate review.",
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
    "reports/research-engine-v2/dataforseo-local-business-validated-write-v1.json"
  );
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/dataforseo-local-business-validated-write-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/dataforseo-local-business-validated-write-v1.md"
  );
  writeJson(reportJson, report);
  const md = renderReportMd(report);
  writeMd(reportMd, md);
  writeMd(docsPath, md);

  log(
    `[dfs-local-write] done status=${status} updated=${updated} website=${report.website_writes} address=${report.address_writes}`
  );
  return report;
}
