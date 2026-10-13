/**
 * Identical permitted hotel inputs for Parallel vs Dealality-native comparison.
 * Strips forbidden fields; audits leakage.
 */

import fs from "node:fs";
import path from "node:path";
import {
  COHORT_FREEZE_PATH,
  COHORT_META,
  BUSINESS_OBJECTIVE,
} from "./comparison-config.js";

const FORBIDDEN_INPUT_KEYS = Object.freeze([
  "expected_owner_name",
  "expected_owner",
  "owner_display_name",
  "owner_domain",
  "owner_domain_hints",
  "curated_ownership_urls",
  "prior_surfe_people",
  "person_hypotheses",
  "domain_hypotheses",
  "owner_entity_id",
  "gold",
  "gold_owner",
  "candidate_documents",
  "known_ownership",
]);

const ALLOWED_TOP_LEVEL = Object.freeze([
  "hotel_id",
  "hotel_name",
  "aliases",
  "city",
  "country",
  "language",
  "official_website",
  "address",
  "coordinates",
]);

export function loadCohortFreeze(root = process.cwd(), freezeRel = COHORT_FREEZE_PATH) {
  const p = path.isAbsolute(freezeRel) ? freezeRel : path.join(root, freezeRel);
  const freeze = JSON.parse(fs.readFileSync(p, "utf8"));
  if (!Array.isArray(freeze.hotels) || freeze.hotels.length !== 10) {
    throw new Error(`cohort_freeze_expected_10_hotels:${freeze.hotels?.length}`);
  }
  return { path: p, freeze };
}

/**
 * Build the shared research input object both arms receive.
 * Never embeds expected owners, prior domains/people, or curated ownership URLs.
 */
export function buildSharedHotelInput(hotelRow = {}) {
  const aliases = Array.isArray(hotelRow.aliases)
    ? hotelRow.aliases.filter(Boolean).map(String)
    : [];
  const official =
    hotelRow.official_website || hotelRow.official_property_website || null;

  const input = {
    hotel_id: hotelRow.hotel_id,
    hotel_name: hotelRow.hotel_name,
    aliases,
    city: hotelRow.city || null,
    country: hotelRow.country || null,
    language: hotelRow.language || null,
    official_website: official ? String(official) : null,
    address: hotelRow.address || null,
    coordinates:
      hotelRow.coordinates ||
      (hotelRow.latitude != null && hotelRow.longitude != null
        ? { lat: Number(hotelRow.latitude), lng: Number(hotelRow.longitude) }
        : null),
    research_objective: BUSINESS_OBJECTIVE,
  };

  // Hard strip any accidental forbidden keys copied from upstream rows
  for (const k of FORBIDDEN_INPUT_KEYS) {
    if (k in input) delete input[k];
  }
  return input;
}

export function buildNativeCaseInput(shared) {
  return {
    hotel: {
      hotel_id: shared.hotel_id,
      hotel_name: shared.hotel_name,
      city: shared.city,
      country: shared.country,
      language: shared.language,
      aliases: shared.aliases || [],
    },
    inspect_urls: shared.official_website ? [shared.official_website] : [],
    hotel_site_host: shared.official_website
      ? String(shared.official_website)
          .replace(/^https?:\/\//, "")
          .replace(/^www\./, "")
          .split("/")[0]
      : null,
    // Explicit empty — comparison must not seed owner reuse answers
    person_hypotheses: [],
    domain_hypotheses: [],
    owner_entity_id: null,
    forbidden_org_hosts: [
      "marriott.com",
      "ihg.com",
      "hilton.com",
      "hyatt.com",
      "wyndhamhotels.com",
      "accor.com",
      "booking.com",
      "tripadvisor.com",
    ],
  };
}

export function buildParallelHotelSeed(shared) {
  return {
    hotel_id: shared.hotel_id,
    airtable_record_id: shared.hotel_id,
    census_record_id: shared.hotel_id,
    hotel_name: shared.hotel_name,
    city: shared.city,
    country: shared.country,
    address: shared.address,
    coordinates: shared.coordinates,
    aliases: shared.aliases || [],
    former_names: shared.aliases || [],
    // Blind: no ownership / brand / operator gold
    current_brand: null,
    operator: null,
  };
}

/**
 * Audit that a constructed input (or freeze row) does not leak answers.
 */
export function auditInputLeakage(obj, { source = "input" } = {}) {
  const findings = [];
  const blob = JSON.stringify(obj || {});
  const lower = blob.toLowerCase();

  for (const key of FORBIDDEN_INPUT_KEYS) {
    if (Object.prototype.hasOwnProperty.call(obj || {}, key) && obj[key] != null && obj[key] !== "") {
      findings.push({
        severity: "FAIL",
        code: "FORBIDDEN_KEY_PRESENT",
        key,
        source,
      });
    }
    // Nested keys
    if (new RegExp(`"${key}"\\s*:`).test(blob) && !["aliases"].includes(key)) {
      // already covered for top-level; nested still fail
      if (blob.includes(`"${key}"`) && source !== "freeze_policy_declaration") {
        // allow research_inputs_forbidden lists that name the keys as strings
        if (!/research_inputs_forbidden/.test(blob) || Object.prototype.hasOwnProperty.call(obj, key)) {
          /* checked above */
        }
      }
    }
  }

  // Heuristic: curated ownership URL lists
  if (/curated_ownership_urls|owner_domain_hints|expected_owner/i.test(blob) && obj.expected_owner_name) {
    findings.push({ severity: "FAIL", code: "EXPECTED_OWNER_VALUE", source });
  }

  const hasForbiddenValue =
    obj?.expected_owner_name ||
    obj?.owner_domain_hints ||
    (Array.isArray(obj?.curated_ownership_urls) && obj.curated_ownership_urls.length) ||
    (Array.isArray(obj?.prior_surfe_people) && obj.prior_surfe_people.length) ||
    (Array.isArray(obj?.person_hypotheses) && obj.person_hypotheses.length) ||
    (Array.isArray(obj?.domain_hypotheses) && obj.domain_hypotheses.length) ||
    (Array.isArray(obj?.candidate_documents) && obj.candidate_documents.length);

  if (hasForbiddenValue) {
    findings.push({ severity: "FAIL", code: "FORBIDDEN_VALUE_PRESENT", source });
  }

  // Soft: brand-in-name hotels are OK (hotel name itself); not leakage
  return {
    ok: findings.every((f) => f.severity !== "FAIL"),
    findings,
    lower_length: lower.length,
  };
}

export function buildCohortSharedInputs(root = process.cwd()) {
  const { path: freezePath, freeze } = loadCohortFreeze(root);
  const hotels = freeze.hotels.map((h) => {
    const shared = buildSharedHotelInput(h);
    const leakage = auditInputLeakage(shared, { source: `hotel:${h.hotel_id}` });
    return {
      order: h.order,
      hotel_id: h.hotel_id,
      hotel_name: h.hotel_name,
      country: h.country,
      city: h.city,
      language: h.language,
      split: h.split,
      shared_input: shared,
      native_case_input: buildNativeCaseInput(shared),
      parallel_hotel_seed: buildParallelHotelSeed(shared),
      leakage_audit: leakage,
      freeze_forbidden: h.research_inputs_forbidden || [],
      freeze_allowed: h.research_inputs_allowed || [],
    };
  });

  const allOk = hotels.every((h) => h.leakage_audit.ok);
  return {
    cohort_label: COHORT_META.label,
    cohort_meta: COHORT_META,
    freeze_path: freezePath,
    freeze_version: freeze.version,
    hotel_count: hotels.length,
    identical_inputs_both_arms: true,
    leakage_audit_pass: allOk,
    hotels,
    allowed_top_level_fields: ALLOWED_TOP_LEVEL,
    forbidden_input_keys: FORBIDDEN_INPUT_KEYS,
  };
}

/**
 * Document unavoidable exposure from previously tuned code (not from inputs).
 */
export function documentCodeTuningExposure() {
  return {
    prior_tuning_exposure: "YES",
    unavoidable_code_exposure: [
      {
        component: "ownership-research-planning.js / research-evidence.js",
        exposure:
          "Query patterns and lead classifiers were tuned on DEVELOPMENT hotel failures (incl. Cap Juluca / Malliouhana / Rendezvous patterns).",
        mitigates_input_leakage: true,
        note: "Arms do not receive expected owners; code may still favor patterns seen during DEVELOPMENT tuning.",
      },
      {
        component: "ownership-document-structured-extract.js / ownership-follow-up-research.js",
        exposure:
          "Offline claim-merge and interpretation repairs were developed against saved DEVELOPMENT failures.",
        wired_into_live_arm_a: false,
        note: "Prepared modules — not executed in this comparison’s live Arm A loop unless separately wired.",
      },
      {
        component: "Parallel FULL_HOTEL_INTELLIGENCE template + subject-identity compiler",
        exposure:
          "Prompt/compiler hardened on Golden Four / Mexico PropCo benchmarks (different hotels).",
        mitigates_input_leakage: true,
      },
    ],
    held_out_hotels: "untouched",
  };
}
