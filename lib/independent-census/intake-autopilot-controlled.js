/**
 * Census Intake Autopilot — controlled dry-run insert proposals.
 *
 * Builds exact Hotel Property Census create payloads from plan auto_insert rows.
 * Default: dry-run only (no Airtable writes).
 * Legacy Hotel Census: forbidden.
 */

import {
  AUTOPILOT_FORBIDDEN_FIELDS,
  isForbiddenAutopilotField,
} from "../research-engine-v2/census-autopilot-field-allowlist.js";
import {
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  assertProductionCensusWriteTarget,
} from "../research-engine-v2/production-census-source-of-truth.js";
import {
  DEFAULT_APPLY_CONFIRMS,
} from "../research-engine-v2/census-autopilot-apply-guard.js";
import {
  evaluateIntakeAutopilotGate,
  INTAKE_DECISIONS,
  WEBSITE_DENYLIST_HOSTS,
} from "./intake-autopilot-gates.js";
import { normalizeIntakeCensusFamilyFields } from "./intake-census-field-normalize.js";

// re-export denylist helper usage — isDeniedWebsite may not be exported; check
// Actually isDeniedWebsite is not exported. Inline check via evaluate gate.

export const INTAKE_CONTROLLED_VERSION = "census-intake-autopilot-controlled-v1";

/** Fields allowed on Hotel Property Census **insert** for intake Autopilot. */
export const INTAKE_INSERT_ALLOWED_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Current Brand",
  "Brand Family",
  "Affiliation Status",
  "Independent Hotel Flag",
  "Country",
  "State / Region",
  "City",
  "Official Property URL",
  "Source URL",
  "Family / Source Family",
  "Source Type",
  "Source Confidence",
  "Identity Confidence",
  "VIC Freeze Hash",
  "Production Use Status",
  "Enrichment Status",
  "Enrichment Priority",
  "Human Review Required",
  "Data Eligible",
  "Discovery Date",
  "Last Reviewed Date",
  "Latitude",
  "Longitude",
  "Phone",
  "Address",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Rooms / Keys",
  "Brand-Unassigned Reason",
]);

export const INTAKE_INSERT_REQUIRED_FIELDS = Object.freeze([
  "Property Name",
  "Property Identity Key",
  "Country",
  "City",
  "Current Brand",
  "Affiliation Status",
  // Family / Source Family required for branded only — see validateIntakeInsertProposal
  "Source URL",
  "VIC Freeze Hash",
  "Production Use Status",
  "Enrichment Status",
  "Human Review Required",
]);

export const INTAKE_INSERT_REQUIRED_WHEN_BRANDED = Object.freeze([
  "Family / Source Family",
]);

export const INTAKE_APPLY_CONFIRMS = Object.freeze([
  ...DEFAULT_APPLY_CONFIRMS,
  "--confirm-intake-inserts",
  "--confirm-no-legacy-hotel-census",
]);

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function isBlank(v) {
  return v == null || (typeof v === "string" && !String(v).trim());
}

/**
 * Sanitize a dual-lane / plan payload into an insert fields object.
 * @param {object} row - plan row with payload + gate fields
 */
export function buildIntakeInsertFields(row) {
  const src = row.payload || row.sanitized_payload_preview || {};
  const hr = row.human_review_required === true;
  const identity =
    row.identity_confidence ||
    (hr ? "Medium" : "High");

  /** @type {Record<string, unknown>} */
  const fields = {};

  const copy = (key, fallback = undefined) => {
    const v = src[key];
    if (!isBlank(v)) fields[key] = v;
    else if (fallback !== undefined && !isBlank(fallback)) fields[key] = fallback;
  };

  copy("Property Name");
  copy("Canonical Property Name", src["Property Name"]);
  copy("Property Identity Key");
  copy("Current Brand");
  copy("Brand Family");
  copy("Affiliation Status");
  copy("Country");
  copy("State / Region"); // only if already known — never default Unknown
  copy("City"); // never default to Unknown — normalize may extract from name/URL
  copy("Official Property URL");
  copy("Source URL");
  copy("Family / Source Family");
  copy("Source Type", "other");
  copy("Source Confidence", "High");
  copy("VIC Freeze Hash");
  copy("Phone");
  copy("Address");
  copy("Continent");
  copy("Sub-Continent");
  copy("Market");
  copy("Submarket");
  if (
    src["Rooms / Keys"] != null &&
    src["Rooms / Keys"] !== "" &&
    Number(src["Rooms / Keys"]) > 0
  ) {
    fields["Rooms / Keys"] = Number(src["Rooms / Keys"]);
  }

  fields["Identity Confidence"] = identity === "High" || identity === "Medium" ? identity : "Medium";
  fields["Production Use Status"] = "Census Only / Not Owner-Facing";
  fields["Enrichment Status"] = "Discovered — pending enrichment";
  fields["Enrichment Priority"] = row.enrichment_priority || "High";
  fields["Human Review Required"] = hr;
  fields["Data Eligible"] = true;
  fields["Discovery Date"] = todayIsoDate();
  fields["Last Reviewed Date"] = todayIsoDate();

  if (src["Independent Hotel Flag"] === true || src["Affiliation Status"] === "Independent") {
    fields["Independent Hotel Flag"] = true;
  } else {
    fields["Independent Hotel Flag"] = false;
  }

  if (!isBlank(src["Brand-Unassigned Reason"])) {
    fields["Brand-Unassigned Reason"] = src["Brand-Unassigned Reason"];
  }

  // Coords only if non-zero
  const lat = Number(src.Latitude);
  const lng = Number(src.Longitude);
  if (
    Number.isFinite(lat) &&
    Number.isFinite(lng) &&
    !(lat === 0 && lng === 0) &&
    Math.abs(lat) <= 90 &&
    Math.abs(lng) <= 180
  ) {
    fields.Latitude = lat;
    fields.Longitude = lng;
  }

  // Match existing HPC conventions (parent family; no Unknown city/state; no invented selects)
  const normalized = normalizeIntakeCensusFamilyFields(fields);

  // Strip anything not on insert allowlist / forbidden
  /** @type {Record<string, unknown>} */
  const out = {};
  const stripped = [];
  for (const [k, v] of Object.entries(normalized)) {
    if (isForbiddenAutopilotField(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
      stripped.push(k);
      continue;
    }
    if (!INTAKE_INSERT_ALLOWED_FIELDS.includes(k)) {
      stripped.push(k);
      continue;
    }
    out[k] = v;
  }

  return { fields: out, stripped_fields: stripped };
}

/**
 * Validate insert fields for controlled dry-run / future apply.
 * @param {Record<string, unknown>} fields
 * @param {object} gateRow
 */
export function validateIntakeInsertProposal(fields, gateRow = {}) {
  /** @type {string[]} */
  const failures = [];

  for (const req of INTAKE_INSERT_REQUIRED_FIELDS) {
    if (isBlank(fields[req]) && fields[req] !== false && fields[req] !== 0) {
      // Human Review Required can be boolean false
      if (req === "Human Review Required" && typeof fields[req] === "boolean") continue;
      // City may be blank for HR branded backlog — never invent "Unknown"
      if (
        req === "City" &&
        fields["Human Review Required"] === true &&
        String(fields["Affiliation Status"] || "").trim() !== "Independent"
      ) {
        continue;
      }
      failures.push(`missing_required:${req}`);
    }
  }

  const affiliation = String(fields["Affiliation Status"] || "").trim();
  const brand = String(fields["Current Brand"] || "").trim();
  const isIndependent =
    /^independent$/i.test(affiliation) || /^independent$/i.test(brand);
  if (!isIndependent) {
    for (const req of INTAKE_INSERT_REQUIRED_WHEN_BRANDED) {
      if (isBlank(fields[req])) failures.push(`missing_required:${req}`);
    }
  }

  if (typeof fields["Human Review Required"] !== "boolean") {
    failures.push("human_review_required_not_boolean");
  }

  for (const k of Object.keys(fields)) {
    if (!INTAKE_INSERT_ALLOWED_FIELDS.includes(k)) {
      failures.push(`field_not_on_insert_allowlist:${k}`);
    }
    if (isForbiddenAutopilotField(k)) {
      failures.push(`forbidden_field:${k}`);
    }
  }

  const url = String(fields["Official Property URL"] || "");
  if (url) {
    const host = (() => {
      try {
        const withProto = /^https?:\/\//i.test(url) ? url : `https://${url}`;
        return new URL(withProto).hostname.replace(/^www\./i, "").toLowerCase();
      } catch {
        return "";
      }
    })();
    if (
      WEBSITE_DENYLIST_HOSTS.some((d) => host === d || host.endsWith(`.${d}`))
    ) {
      failures.push("official_url_denylisted");
    }
  } else {
    failures.push("missing_official_property_url");
  }

  // Re-run gate on proposed fields
  const regate = evaluateIntakeAutopilotGate({
    lane: gateRow.lane,
    intake_class: gateRow.intake_class,
    hpc_recommended_action: gateRow.hpc_recommended_action || "likely_new_candidate",
    human_review_required: fields["Human Review Required"],
    identity_confidence: fields["Identity Confidence"],
      quality_score: gateRow.quality_score ?? null,
    sanitized_payload_preview: fields,
  });

  if (regate.decision !== INTAKE_DECISIONS.AUTO_INSERT) {
    failures.push(`regate_not_auto_insert:${regate.decision}`);
    for (const r of regate.reasons || []) failures.push(`regate:${r}`);
  }

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: productionHotelPropertyCensus.tableId || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    failures.push(`wrong_write_target:${writeTarget.reason}`);
  }

  return {
    pass: failures.length === 0,
    failures,
    regate,
  };
}

/**
 * Apply-time validation for a bundle row (trusts controlled dry-run gate,
 * re-checks allowlist / required / denylist / cohort).
 * When lane + quality metadata are present, also re-runs the Autopilot gate.
 * @param {Record<string, unknown>} fields
 * @param {object} [meta]
 */
export function validateIntakeApplyRow(fields, meta = {}) {
  /** @type {string[]} */
  const failures = [];
  const cohort = meta.cohort || "no_hr";

  for (const req of INTAKE_INSERT_REQUIRED_FIELDS) {
    if (req === "Human Review Required") {
      if (typeof fields[req] !== "boolean") {
        failures.push("human_review_required_not_boolean");
      }
      continue;
    }
    if (isBlank(fields[req]) && fields[req] !== false && fields[req] !== 0) {
      if (
        req === "City" &&
        fields["Human Review Required"] === true &&
        String(fields["Affiliation Status"] || "").trim() !== "Independent"
      ) {
        continue;
      }
      failures.push(`missing_required:${req}`);
    }
  }

  const affiliation = String(fields["Affiliation Status"] || "").trim();
  const brand = String(fields["Current Brand"] || "").trim();
  const isIndependent =
    /^independent$/i.test(affiliation) || /^independent$/i.test(brand);
  if (!isIndependent) {
    for (const req of INTAKE_INSERT_REQUIRED_WHEN_BRANDED) {
      if (isBlank(fields[req])) failures.push(`missing_required:${req}`);
    }
  }

  for (const k of Object.keys(fields)) {
    if (!INTAKE_INSERT_ALLOWED_FIELDS.includes(k)) {
      failures.push(`field_not_on_insert_allowlist:${k}`);
    }
    if (isForbiddenAutopilotField(k)) {
      failures.push(`forbidden_field:${k}`);
    }
  }

  const url = String(fields["Official Property URL"] || "");
  if (!url) failures.push("missing_official_property_url");
  else {
    const host = (() => {
      try {
        const withProto = /^https?:\/\//i.test(url) ? url : `https://${url}`;
        return new URL(withProto).hostname.replace(/^www\./i, "").toLowerCase();
      } catch {
        return "";
      }
    })();
    if (
      WEBSITE_DENYLIST_HOSTS.some((d) => host === d || host.endsWith(`.${d}`))
    ) {
      failures.push("official_url_denylisted");
    }
  }

  if (cohort === "no_hr" && fields["Human Review Required"] === true) {
    failures.push("human_review_required_excluded_from_no_hr_apply");
  }
  if (cohort === "hr_only" && fields["Human Review Required"] !== true) {
    failures.push("missing_human_review_required_for_hr_only_apply");
  }

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId:
      productionHotelPropertyCensus.tableId ||
      PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    failures.push(`wrong_write_target:${writeTarget.reason}`);
  }

  if (meta.lane || meta.intake_class || meta.quality_score != null) {
    const regate = evaluateIntakeAutopilotGate({
      lane: meta.lane,
      intake_class: meta.intake_class,
      hpc_recommended_action:
        meta.hpc_recommended_action || "likely_new_candidate",
      quality_score: meta.quality_score ?? null,
      wikidata_match_confidence: meta.wikidata_match_confidence || "",
      sanitized_payload_preview: fields,
    });
    if (regate.decision !== INTAKE_DECISIONS.AUTO_INSERT) {
      failures.push(`regate_not_auto_insert:${regate.decision}`);
      for (const r of regate.reasons || []) failures.push(`regate:${r}`);
    }
    if (cohort === "no_hr" && regate.human_review_required === true) {
      failures.push("regate_human_review_required");
    }
    if (cohort === "hr_only" && regate.human_review_required !== true) {
      failures.push("regate_expected_human_review_for_hr_only");
    }
  }

  return { pass: failures.length === 0, failures };
}

/**
 * Build controlled dry-run bundle from intake Autopilot plan report.
 * @param {object} planReport
 * @param {{ maxRecords?: number|null, cohort?: 'all'|'no_hr'|'hr_only' }} [opts]
 */
export function buildIntakeControlledDryRun(planReport, opts = {}) {
  const cohort = opts.cohort || "all";
  const maxRecords = opts.maxRecords ?? null;

  let candidates = (planReport.rows || []).filter(
    (r) =>
      r.decision === INTAKE_DECISIONS.AUTO_INSERT &&
      r.production_writable_insert === true
  );

  if (cohort === "no_hr") {
    candidates = candidates.filter((r) => r.human_review_required !== true);
  } else if (cohort === "hr_only") {
    candidates = candidates.filter((r) => r.human_review_required === true);
  }

  if (maxRecords != null && Number.isFinite(maxRecords)) {
    candidates = candidates.slice(0, maxRecords);
  }

  const proposals = [];
  let validation_pass = 0;
  let validation_fail = 0;

  for (const row of candidates) {
    const { fields, stripped_fields } = buildIntakeInsertFields(row);
    const validation = validateIntakeInsertProposal(fields, row);

    if (validation.pass) validation_pass += 1;
    else validation_fail += 1;

    proposals.push({
      source_record_id: row.source_record_id,
      lane: row.lane,
      intake_class: row.intake_class,
      decision: row.decision,
      human_review_required: fields["Human Review Required"],
      identity_confidence: fields["Identity Confidence"],
      quality_score: row.quality_score ?? null,
      queue_autopilot_enrichment: Boolean(row.queue_autopilot_enrichment),
      validation_pass: validation.pass,
      validation_failures: validation.failures,
      stripped_fields,
      field_mapping_used: Object.keys(fields),
      sanitized_payload_preview: fields,
      error_handling: {
        validation_error: "Do not POST; fix failures listed",
        api_error: "Steward queue; no silent retry loop without backoff",
        network_error: "Retry with backoff; user: intake delayed",
      },
      airtable_create: {
        method: "POST",
        base: productionHotelPropertyCensus.baseName,
        table: productionHotelPropertyCensus.tableName,
        table_id: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
        fields,
      },
    });
  }

  return {
    version: INTAKE_CONTROLLED_VERSION,
    generated_at: new Date().toISOString(),
    mode: "controlled_dry_run",
    airtable_writes: false,
    legacy_hotel_census_used: false,
    dedupe_source_of_truth: "Hotel Property Census",
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
    },
    apply_confirms_required: [...INTAKE_APPLY_CONFIRMS],
    cohort,
    input_auto_insert: candidates.length,
    counts: {
      proposals: proposals.length,
      validation_pass,
      validation_fail,
      no_hr: proposals.filter((p) => p.human_review_required === false).length,
      with_hr: proposals.filter((p) => p.human_review_required === true).length,
      queue_enrichment: proposals.filter((p) => p.queue_autopilot_enrichment).length,
    },
    proposals,
    approval_bundle_ready: validation_fail === 0 && proposals.length > 0,
  };
}
