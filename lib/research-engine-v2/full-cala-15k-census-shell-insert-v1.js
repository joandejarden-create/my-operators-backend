/**
 * Full CALA ~15K Hotel Property Census shell insert v1.
 * Populate safe shell records first; enrich later.
 *
 * Objective: full-cala-15k-census-shell-insert-v1
 * Default: dry-run (ENABLE_CENSUS_SHELL_INSERTS=0).
 */
import crypto from "node:crypto";
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
import { normName } from "./census-autopilot-v2/identity-dedupe.js";
import { toProperCasePlace } from "./census-city-state-normalizer.js";
import { tokenSimilarity } from "./adapters/adapter-utils.js";
import { isRejectedDiscoveryHost } from "./census-discovery-host-policy.js";
import { toSmartHotelProperCase } from "./full-cala-15k-shell-format-source-brand-backfill-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const FULL_CALA_15K_OBJECTIVE = "full-cala-15k-census-shell-insert-v1";
export const FULL_CALA_15K_VERSION = "full-cala-15k-census-shell-insert-v1";

export const FULL_CALA_15K_STATUS = Object.freeze({
  DRY_RUN_READY:
    "production_census_full_cala_15k_shell_insert_v1_dry_run_complete_ready_for_batch_apply",
  BATCH_APPLY_COMPLETE:
    "production_census_full_cala_15k_shell_insert_v1_batch_apply_complete",
  COSTA_RICA_BATCH_APPLY_COMPLETE:
    "production_census_full_cala_15k_shell_insert_v1_costa_rica_batch_apply_complete",
  PANAMA_BATCH_APPLY_COMPLETE:
    "production_census_full_cala_15k_shell_insert_v1_panama_batch_apply_complete",
  COLOMBIA_BATCH_APPLY_COMPLETE:
    "production_census_full_cala_15k_shell_insert_v1_colombia_batch_apply_complete",
  COLOMBIA_BATCH_PARTIAL_DUP:
    "production_census_full_cala_15k_shell_insert_v1_colombia_batch_partial_duplicate_review_needed",
  COLOMBIA_BATCH_PARTIAL_SOURCE:
    "production_census_full_cala_15k_shell_insert_v1_colombia_batch_partial_source_remaining",
  COLOMBIA_BATCH_BLOCKED:
    "production_census_full_cala_15k_shell_insert_v1_colombia_batch_blocked",
  COLOMBIA_BATCH_2_APPLY_COMPLETE:
    "production_census_full_cala_15k_shell_insert_v1_colombia_batch_2_apply_complete",
  COLOMBIA_BATCH_2_PARTIAL_DUP:
    "production_census_full_cala_15k_shell_insert_v1_colombia_batch_2_partial_duplicate_review_needed",
  COLOMBIA_BATCH_2_PARTIAL_SOURCE:
    "production_census_full_cala_15k_shell_insert_v1_colombia_batch_2_partial_source_remaining",
  COLOMBIA_BATCH_2_BLOCKED:
    "production_census_full_cala_15k_shell_insert_v1_colombia_batch_2_blocked",
  MEXICO_BATCH_1_APPLY_COMPLETE:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_1_apply_complete",
  MEXICO_BATCH_1_PARTIAL_DUP:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_1_partial_duplicate_review_needed",
  MEXICO_BATCH_1_PARTIAL_SOURCE:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_1_partial_source_remaining",
  MEXICO_BATCH_1_BLOCKED:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_1_blocked",
  MEXICO_BATCH_2_APPLY_COMPLETE:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_2_apply_complete",
  MEXICO_BATCH_2_PARTIAL_DUP:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_2_partial_duplicate_review_needed",
  MEXICO_BATCH_2_PARTIAL_SOURCE:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_2_partial_source_remaining",
  MEXICO_BATCH_2_BLOCKED:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_2_blocked",
  MEXICO_BATCH_3_APPLY_COMPLETE:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_3_apply_complete",
  MEXICO_BATCH_3_PARTIAL_DUP:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_3_partial_duplicate_review_needed",
  MEXICO_BATCH_3_PARTIAL_QUALITY:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_3_partial_quality_review_needed",
  MEXICO_BATCH_3_PARTIAL_SOURCE:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_3_partial_source_remaining",
  MEXICO_BATCH_3_BLOCKED:
    "production_census_full_cala_15k_shell_insert_v1_mexico_batch_3_blocked",
  PARTIAL_CVENT:
    "production_census_full_cala_15k_shell_insert_v1_partial_cvent_artifacts_missing",
  PARTIAL_DUP:
    "production_census_full_cala_15k_shell_insert_v1_partial_duplicate_review_needed",
  PARTIAL_LICENSE:
    "production_census_full_cala_15k_shell_insert_v1_partial_source_license_needed",
  BLOCKED: "production_census_full_cala_15k_shell_insert_v1_blocked",
});

export const SHELL_PREFLIGHT_CLASS = Object.freeze({
  SAFE: "safe_shell_insert",
  REVIEW: "shell_insert_with_review",
  PROBABLE_DUP: "probable_duplicate_hold",
  WEAK: "weak_identity_hold",
  NON_HOTEL: "non_hotel_reject",
  INSUFFICIENT: "insufficient_data_hold",
});

const NON_HOTEL_NAME_RE =
  /timeshare|meeting\s*room\s*only|office\s*building|apartment\s*complex\b|convention\s*cent(?:er|re)|centro\s*de\s*convenciones|event\s*(?:center|space|venue)|banquet\s*hall|catering\b|coworking|office\s*suite|wedding\s*venue|expo\s*cent(?:er|re)|auditorium|stadium|\barena\b|\bmuseum\b|golf\s*club(?!.*(?:resort|hotel))|\bmarina\b(?!.*hotel)|\brestaurant\b(?!.*hotel)|\bbar\s*&\s*grill\b/i;
const GENERIC_VENUE_NAME_RE =
  /^(?:the\s+)?(?:venue|sal[oó]n|salon|centro|center|centre|espacio|space|sala)\b/i;
const HOTEL_LIKE_NAME_RE =
  /\b(?:hotel|hoteles|resort|inn|suites?|hostel|posada|hacienda|lodge|motel|boutique|hyatt|marriott|hilton|ihg|radisson|fiesta|secrets|dreams|paradisus|fairmont|fairmont|omni|ritz|st\.?\s*regis|four\s*seasons|rosewood|belmond|one\s*&?\s*only|one\s+only|solaz|zadun|barcel[oó]|meli[aá]|riiu?|nh|ac\s*hotel|hampton|holiday\s*inn|crowne|intercontinental|wyndham|best\s*western|city\s*express|camino\s*real|krystal|emporio|gamma|live\s*aqua|zo[eë]try|breathless|iberostar|one\s*hotels|hoteles\s*misi[oó]n|quinta\s*real|luxury\s*collection|autograph|tribute|curio|kimpton|andaz|park\s*hyatt|grand\s*hyatt|hyatt\s*ziva|hyatt\s*zilara|marriott|westin|sheraton|le\s*m[eé]ridien|w\s+hotels?|edition|st\s*regis)\b/i;

/** Infer Mexico city/destination from property name when structured city is missing. */
export function inferMexicoCityFromName(name) {
  const s = String(name || "");
  const patterns = [
    { re: /mexico\s*city|ciudad\s*de\s*m[eé]xico|\bcdmx\b/i, city: "Mexico City" },
    {
      re: /canc[uú]n|riviera\s*maya|playa\s*del\s*carmen|tulum|cozumel|puerto\s*morelos|mayakoba|akumal/i,
      city: "Cancún",
    },
    {
      re: /los\s*cabos|cabo\s*san\s*lucas|san\s*jos[eé]\s*del\s*cabo|palmilla/i,
      city: "Los Cabos",
    },
    {
      re: /puerto\s*vallarta|nuevo\s*vallarta|riviera\s*nayarit|bah[ií]a\s*mita|punta\s*(?:de\s+)?mita|sayulita/i,
      city: "Puerto Vallarta",
    },
    { re: /guadalajara|zapopan|tlaquepaque/i, city: "Guadalajara" },
    { re: /monterrey|san\s*pedro\s*garza/i, city: "Monterrey" },
    { re: /m[eé]rida|yucat[aá]n/i, city: "Mérida" },
    { re: /acapulco/i, city: "Acapulco" },
    { re: /mazatl[aá]n/i, city: "Mazatlán" },
    { re: /ixtapa|zihuatanejo/i, city: "Ixtapa" },
    { re: /huatulco/i, city: "Huatulco" },
    { re: /puerto\s*escondido/i, city: "Puerto Escondido" },
    { re: /oaxaca(?!\s*city)/i, city: "Oaxaca" },
    { re: /puebla/i, city: "Puebla" },
    { re: /quer[eé]taro/i, city: "Querétaro" },
    { re: /san\s*miguel\s*de\s*allende/i, city: "San Miguel de Allende" },
    { re: /toluca|metepec/i, city: "Toluca" },
    { re: /tijuana/i, city: "Tijuana" },
    { re: /le[oó]n\b|guanajuato/i, city: "León" },
    { re: /cancun|riviera\s*maya/i, city: "Cancún" },
  ];
  for (const row of patterns) {
    if (row.re.test(s)) return row.city;
  }
  return null;
}

/** Parse Mexico shell country-batch number from objective / label (default 1). */
export function resolveMexicoShellBatchNumber(opts = {}) {
  if (opts.mexicoBatchNumber != null && Number.isFinite(Number(opts.mexicoBatchNumber))) {
    return Math.max(1, Number(opts.mexicoBatchNumber));
  }
  const fromLabel = String(opts.shellCountryBatch || "").match(
    /mexico\s*batch\s*(\d+)/i
  );
  if (fromLabel) return Math.max(1, Number(fromLabel[1]) || 1);
  const fromObj = String(opts.objective || "").match(/mexico-batch-(\d+)/i);
  if (fromObj) return Math.max(1, Number(fromObj[1]) || 1);
  return 1;
}

function mexicoBatchStatusBundle(batchNum) {
  const n = Math.max(1, Number(batchNum) || 1);
  const base = `production_census_full_cala_15k_shell_insert_v1_mexico_batch_${n}`;
  return {
    apply: `${base}_apply_complete`,
    partial_dup: `${base}_partial_duplicate_review_needed`,
    partial_source: `${base}_partial_source_remaining`,
    partial_quality: `${base}_partial_quality_review_needed`,
    blocked: `${base}_blocked`,
  };
}

/** Per-country batch apply status when --country is set and inserts succeed. */
export function resolveCountryBatchStatus(country, insertsApplied, opts = {}) {
  const mexicoBatch = resolveMexicoShellBatchNumber(opts);
  if (!insertsApplied && opts.blocked) {
    const c = String(country || "").trim();
    if (c === "Colombia") return FULL_CALA_15K_STATUS.COLOMBIA_BATCH_BLOCKED;
    if (c === "Mexico") return mexicoBatchStatusBundle(mexicoBatch).blocked;
    return FULL_CALA_15K_STATUS.BLOCKED;
  }
  if (!insertsApplied) return null;
  const c = String(country || "").trim();
  if (c === "Costa Rica") return FULL_CALA_15K_STATUS.COSTA_RICA_BATCH_APPLY_COMPLETE;
  if (c === "Panama") return FULL_CALA_15K_STATUS.PANAMA_BATCH_APPLY_COMPLETE;
  if (c === "Colombia") return FULL_CALA_15K_STATUS.COLOMBIA_BATCH_APPLY_COMPLETE;
  if (c === "Mexico") return mexicoBatchStatusBundle(mexicoBatch).apply;
  if (c === "Dominican Republic") return FULL_CALA_15K_STATUS.BATCH_APPLY_COMPLETE;
  return FULL_CALA_15K_STATUS.BATCH_APPLY_COMPLETE;
}

/** Rank eligible shells for country batch: HBX-first, then multi-source, Cvent-only last. */
export function mexicoBatchSourcePriority(candidate) {
  const hasHbx = Boolean(candidate.external_ids?.hbx_code);
  const sources = candidate.merged_sources || [candidate.source_type];
  const isCvent =
    candidate.source_type === "cvent_candidate" || sources.includes("cvent_candidate");
  const isIndep =
    candidate.source_type === "independent_discovery" ||
    sources.includes("independent_discovery");
  const multi = new Set(sources.filter(Boolean)).size > 1;
  if (hasHbx && !isCvent) return 0;
  if (hasHbx && isCvent) return 1;
  if (multi || isIndep) return 2;
  if (isCvent) return 3;
  return 4;
}

/**
 * Preflight quality class for shell inserts (esp. Cvent-only Mexico Batch 3+).
 * @param {object} candidate
 * @param {{ cventOnlyQualityGate?: boolean }} [opts]
 */
export function classifyShellPreflightQuality(candidate, opts = {}) {
  const name = String(candidate.property_name || "").trim();
  const country = String(candidate.country || "").trim();
  const structuredCity = String(candidate.city || "").trim();
  const inferredCity =
    !structuredCity && /mexico/i.test(country)
      ? inferMexicoCityFromName(name)
      : null;
  const city = structuredCity || inferredCity || "";
  const hasHbx = Boolean(candidate.external_ids?.hbx_code);
  const sources = candidate.merged_sources || [candidate.source_type];
  const isCvent =
    candidate.source_type === "cvent_candidate" || sources.includes("cvent_candidate");
  const isCventOnly = isCvent && !hasHbx;
  const hasCity = Boolean(city);
  const hasAddress = !isBlank(candidate.address);
  const hasWeb = Boolean(domainOf(candidate.website));
  const hasPhone = Boolean(normPhone(candidate.phone));
  const hasBrand = Boolean(candidate.brand_text || candidate.chain_text);
  const hotelLike = HOTEL_LIKE_NAME_RE.test(name);
  const signals = [hasCity, hasAddress, hasWeb, hasPhone, hasBrand, hasHbx].filter(Boolean)
    .length;

  if (
    candidate.match_class === MATCH.PROBABLE_DUP ||
    candidate.match_class === MATCH.POSSIBLE_DUP
  ) {
    return {
      class: SHELL_PREFLIGHT_CLASS.PROBABLE_DUP,
      reason: candidate.match_reason || candidate.reason || "duplicate_risk",
      signals,
      inferred_city: inferredCity,
    };
  }
  if (candidate.match_class === MATCH.REJECT_NON_HOTEL) {
    return {
      class: SHELL_PREFLIGHT_CLASS.NON_HOTEL,
      reason: "match_class_non_hotel",
      signals,
      inferred_city: inferredCity,
    };
  }
  if (
    candidate.match_class === MATCH.REJECT_IDENTITY ||
    isBlank(name) ||
    isBlank(country)
  ) {
    return {
      class: SHELL_PREFLIGHT_CLASS.INSUFFICIENT,
      reason: "missing_name_or_country",
      signals,
      inferred_city: inferredCity,
    };
  }
  if (NON_HOTEL_NAME_RE.test(name) || (GENERIC_VENUE_NAME_RE.test(name) && !hotelLike)) {
    return {
      class: SHELL_PREFLIGHT_CLASS.NON_HOTEL,
      reason: "non_hotel_or_generic_venue_name",
      signals,
      inferred_city: inferredCity,
    };
  }

  // HBX-backed / multi-source: prefer safe insert
  if (hasHbx) {
    return {
      class: hasCity ? SHELL_PREFLIGHT_CLASS.SAFE : SHELL_PREFLIGHT_CLASS.REVIEW,
      reason: hasCity ? "hbx_backed_with_city" : "hbx_backed_missing_city",
      signals,
      inferred_city: inferredCity,
    };
  }
  if (!isCventOnly) {
    return {
      class:
        hasCity && signals >= 2
          ? SHELL_PREFLIGHT_CLASS.SAFE
          : SHELL_PREFLIGHT_CLASS.REVIEW,
      reason: "multi_or_independent_source",
      signals,
      inferred_city: inferredCity,
    };
  }

  // Cvent-only quality gate
  if (opts.cventOnlyQualityGate !== false) {
    if (!hasCity) {
      return {
        class: SHELL_PREFLIGHT_CLASS.WEAK,
        reason: "cvent_only_missing_city",
        signals,
        inferred_city: inferredCity,
      };
    }
    if (hotelLike && structuredCity && (hasAddress || hasWeb || hasPhone || hasBrand)) {
      return {
        class: SHELL_PREFLIGHT_CLASS.SAFE,
        reason: "cvent_only_hotel_like_strong_identity",
        signals,
        inferred_city: inferredCity,
      };
    }
    if (hotelLike && structuredCity) {
      return {
        class: SHELL_PREFLIGHT_CLASS.REVIEW,
        reason: "cvent_only_hotel_like_city_only",
        signals,
        inferred_city: inferredCity,
      };
    }
    if (hotelLike && inferredCity) {
      return {
        class: SHELL_PREFLIGHT_CLASS.REVIEW,
        reason: "cvent_only_hotel_like_inferred_destination",
        signals,
        inferred_city: inferredCity,
      };
    }
    if (inferredCity && (hasAddress || hasWeb)) {
      return {
        class: SHELL_PREFLIGHT_CLASS.REVIEW,
        reason: "cvent_only_inferred_destination_with_contact",
        signals,
        inferred_city: inferredCity,
      };
    }
    if (structuredCity && (hasAddress || hasWeb)) {
      return {
        class: SHELL_PREFLIGHT_CLASS.REVIEW,
        reason: "cvent_only_non_hotel_token_but_contact",
        signals,
        inferred_city: inferredCity,
      };
    }
    return {
      class: SHELL_PREFLIGHT_CLASS.WEAK,
      reason: inferredCity
        ? "cvent_only_inferred_destination_weak_hotel_signal"
        : "cvent_only_weak_identity",
      signals,
      inferred_city: inferredCity,
    };
  }

  return {
    class: SHELL_PREFLIGHT_CLASS.REVIEW,
    reason: "default_review",
    signals,
    inferred_city: inferredCity,
  };
}

export function buildMexicoBatchPreflightSummary(candidates, opts = {}) {
  const cventOnlyQualityGate = opts.cventOnlyQualityGate !== false;
  const tallies = {
    safe_shell_insert: 0,
    shell_insert_with_review: 0,
    probable_duplicate_hold: 0,
    weak_identity_hold: 0,
    non_hotel_reject: 0,
    insufficient_data_hold: 0,
  };
  const fieldPresence = {
    with_city: 0,
    with_inferred_city: 0,
    with_address: 0,
    with_website: 0,
    with_phone: 0,
    with_candidate_brand: 0,
    missing_city: 0,
    missing_country: 0,
  };
  const sourceMix = {
    hbx_only: 0,
    cvent_plus_hbx: 0,
    cvent_only: 0,
    independent_or_multi: 0,
  };
  const classified = [];
  for (const c of candidates) {
    const hasHbx = Boolean(c.external_ids?.hbx_code);
    const sources = c.merged_sources || [c.source_type];
    const isCvent =
      c.source_type === "cvent_candidate" || sources.includes("cvent_candidate");
    const isIndep =
      c.source_type === "independent_discovery" ||
      sources.includes("independent_discovery");
    const multi = new Set(sources.filter(Boolean)).size > 1;
    if (hasHbx && !isCvent) sourceMix.hbx_only += 1;
    else if (hasHbx && isCvent) sourceMix.cvent_plus_hbx += 1;
    else if (isCvent && !hasHbx) sourceMix.cvent_only += 1;
    else if (multi || isIndep) sourceMix.independent_or_multi += 1;
    else sourceMix.independent_or_multi += 1;

    if (!isBlank(c.city)) fieldPresence.with_city += 1;
    else if (/mexico/i.test(String(c.country || "")) && inferMexicoCityFromName(c.property_name)) {
      fieldPresence.with_city += 1;
      fieldPresence.with_inferred_city =
        (fieldPresence.with_inferred_city || 0) + 1;
    } else fieldPresence.missing_city += 1;
    if (isBlank(c.country)) fieldPresence.missing_country += 1;
    if (!isBlank(c.address)) fieldPresence.with_address += 1;
    if (domainOf(c.website)) fieldPresence.with_website += 1;
    if (normPhone(c.phone)) fieldPresence.with_phone += 1;
    if (c.brand_text || c.chain_text) fieldPresence.with_candidate_brand += 1;

    const pf = classifyShellPreflightQuality(c, { cventOnlyQualityGate });
    tallies[pf.class] = (tallies[pf.class] || 0) + 1;
    classified.push({
      candidate_id: c.candidate_id,
      property_name: c.property_name,
      preflight_class: pf.class,
      reason: pf.reason,
      source_priority: mexicoBatchSourcePriority(c),
      is_cvent_only: isCvent && !hasHbx,
      has_hbx: hasHbx,
    });
  }

  const insertable =
    (tallies.safe_shell_insert || 0) + (tallies.shell_insert_with_review || 0);
  const held =
    (tallies.probable_duplicate_hold || 0) +
    (tallies.weak_identity_hold || 0) +
    (tallies.non_hotel_reject || 0) +
    (tallies.insufficient_data_hold || 0);
  const total = candidates.length;
  const top500 = [...classified]
    .sort((a, b) => a.source_priority - b.source_priority)
    .slice(0, 500);
  const top500Held = top500.filter(
    (r) =>
      r.preflight_class !== SHELL_PREFLIGHT_CLASS.SAFE &&
      r.preflight_class !== SHELL_PREFLIGHT_CLASS.REVIEW
  ).length;
  const top500_hold_ratio = top500.length ? top500Held / top500.length : 0;

  const blockDecision = decideMexicoBatch3PreflightBlock({
    remaining_eligible: total,
    insertable,
    held,
    top500_hold_ratio,
    tallies,
  });

  return {
    remaining_eligible: total,
    classifications: tallies,
    insertable_count: insertable,
    held_or_reject_count: held,
    field_presence: fieldPresence,
    source_mix: sourceMix,
    top500_hold_ratio: Number(top500_hold_ratio.toFixed(3)),
    top500_held: top500Held,
    block_writes: blockDecision.block,
    block_reason: blockDecision.reason,
    sample_holds: classified
      .filter(
        (r) =>
          r.preflight_class !== SHELL_PREFLIGHT_CLASS.SAFE &&
          r.preflight_class !== SHELL_PREFLIGHT_CLASS.REVIEW
      )
      .slice(0, 25),
  };
}

export function decideMexicoBatch3PreflightBlock(summary) {
  const insertable = Number(summary.insertable || summary.insertable_count || 0);
  const held = Number(summary.held || summary.held_or_reject_count || 0);
  const total = Number(summary.remaining_eligible || insertable + held || 0);
  const top500HoldRatio = Number(summary.top500_hold_ratio || 0);
  if (insertable === 0) {
    return { block: true, reason: "no_safe_or_review_candidates" };
  }
  if (total > 0 && held / total >= 0.55 && insertable < 100) {
    return { block: true, reason: "high_quality_risk_ratio" };
  }
  if (top500HoldRatio >= 0.5 && insertable < 250) {
    return { block: true, reason: "top500_majority_held" };
  }
  return { block: false, reason: null };
}

const INSERTABLE_PREFLIGHT = new Set([
  SHELL_PREFLIGHT_CLASS.SAFE,
  SHELL_PREFLIGHT_CLASS.REVIEW,
]);

export const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] ||
  productionHotelPropertyCensus.tableId ||
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const CHECKPOINT_DIR = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-census-shell"
);
const CHECKPOINT_FILE = path.join(CHECKPOINT_DIR, "full-cala-15k-checkpoint.json");

export const MATCH = Object.freeze({
  EXISTING_HIGH: "existing_match_high",
  EXISTING_MEDIUM: "existing_match_medium",
  PROBABLE_DUP: "probable_duplicate_hold",
  POSSIBLE_DUP: "possible_duplicate_review",
  NEW_HIGH: "new_candidate_high",
  NEW_MEDIUM: "new_candidate_medium",
  NEW_LOW: "new_candidate_low",
  REJECT_NON_HOTEL: "reject_non_hotel",
  REJECT_IDENTITY: "reject_insufficient_identity",
  REJECT_OUTSIDE: "reject_outside_cala",
  REJECT_SOURCE: "reject_source_not_allowed",
});

const COUNTRY_BATCH_ORDER = [
  "Dominican Republic",
  "Costa Rica",
  "Panama",
  "Colombia",
  "Mexico",
];

const SHELL_INSERT_FIELDS = new Set([
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Country",
  "City",
  "State / Region",
  "Market",
  "Address",
  "Official Property URL",
  "Phone",
  "Source Type",
  "Source Confidence",
  "Source URL",
  "Identity Confidence",
  "Data Confidence Tier",
  "Production Use Status",
  "Public Display Review Status",
  "Radar Display Status",
  "Enrichment Status",
  "Human Review Required",
  "Last Reviewed Date",
  "Discovery Date",
  "Notes for Steward",
  // HBX identity (schema repaired) — linkage/dedupe only; not Current Brand
  "HBX Hotel Code",
  "HBX Chain Code",
  "HBX Category Code",
  "HBX Linkage Confidence",
  "HBX Source Status",
  "HBX Content Review Status",
  // Shell provenance + candidate brand (post format-backfill schema)
  "Discovery Source",
  "Source Candidate Type",
  "Candidate Source Count",
  "Review Status",
  "Shell Insert Batch ID",
  "Shell Insert Country Batch",
  "Shell Insert Date",
  "Shell Insert Source Mix",
  "Shell Dedupe Confidence",
  "Candidate Brand Text",
  "Candidate Brand Family",
  "Candidate Brand Source",
  "Candidate Brand Confidence",
  "Brand Validation Status",
]);

export const FORBIDDEN_SHELL = new Set([
  "Rooms / Keys",
  "Latitude",
  "Longitude",
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
  "Brand Family",
  "Family / Source Family",
  "Hotel Description - AI Summary",
  "Amenities - Structured Tags",
]);

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
function isBlank(v) {
  return v == null || !String(v).trim();
}
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function hashId(parts) {
  return crypto.createHash("sha1").update(parts.filter(Boolean).join("|")).digest("hex").slice(0, 16);
}
function domainOf(url) {
  try {
    let s = String(url || "").trim();
    if (!s) return null;
    if (!/^https?:\/\//i.test(s)) s = `https://${s}`;
    return new URL(s).hostname.replace(/^www\./i, "").toLowerCase() || null;
  } catch {
    return null;
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
function normPhone(p) {
  const d = String(p || "").replace(/[^\d]/g, "");
  return d.length >= 8 ? d : null;
}
function titleCity(c) {
  if (isBlank(c)) return null;
  return toProperCasePlace(c) || String(c).trim();
}

export function resolveFullCala15kGates(env = process.env) {
  const flag = (k) => String(env[k] || "0").trim() === "1";
  const blockers = [];
  if (flag("ENABLE_ROOMS_WRITES")) blockers.push("ENABLE_ROOMS_WRITES_must_be_0");
  if (flag("ENABLE_OWNER_OPERATOR_WRITES")) blockers.push("ENABLE_OWNER_OPERATOR_WRITES_must_be_0");
  if (flag("ENABLE_DATE_WRITES")) blockers.push("ENABLE_DATE_WRITES_must_be_0");
  if (flag("ENABLE_COORDINATE_WRITES")) blockers.push("ENABLE_COORDINATE_WRITES_must_be_0");
  if (flag("ENABLE_PUBLIC_DISPLAY_WRITES")) blockers.push("ENABLE_PUBLIC_DISPLAY_WRITES_must_be_0");
  if (flag("ENABLE_CENSUS_FIELD_ENRICHMENT")) {
    blockers.push("ENABLE_CENSUS_FIELD_ENRICHMENT_must_be_0_for_shell_mission");
  }
  return {
    ok: blockers.length === 0,
    blockers,
    shell_mission: flag("ENABLE_FULL_CALA_15K_CENSUS_SHELL"),
    inserts: flag("ENABLE_CENSUS_SHELL_INSERTS"),
    hbx_inserts: flag("ENABLE_HBX_INSERTS"),
    cvent_inserts: flag("ENABLE_CVENT_CANDIDATE_SHELL_INSERTS"),
    batch_preflight: flag("ENABLE_CENSUS_BATCH_PREFLIGHT"),
    cvent_only_quality_gate: flag("ENABLE_CVENT_ONLY_QUALITY_GATE"),
    enrichment: false,
  };
}

/**
 * Phase 0 — source inventory (local artifacts only; no live Cvent scrape).
 */
export function buildSourceInventory() {
  const sources = [];
  const add = (row) => sources.push(row);

  const hbxPack = path.join(ROOT, "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json");
  if (fs.existsSync(hbxPack)) {
    const j = JSON.parse(fs.readFileSync(hbxPack, "utf8"));
    add({
      file_path: "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json",
      source_type: "hbx_content_api",
      countries_covered: Object.keys(j.by_country_counts || {}),
      candidate_count: j.count || (j.candidates || []).length,
      field_availability: [
        "name",
        "country",
        "city",
        "address",
        "website",
        "phonehotel",
        "chain_code",
        "hbx_hotel_code",
        "match_class",
      ],
      licensed_approved: "license_policy_partial — identity/contact ok; media/coords held",
      insert_identity_ok: true,
      field_level_provenance_ok: "partial_internal_only",
      duplicate_risk: "medium",
      recommended_use: "primary Wave1 identity + contact for MX/DO/CO/CR/PA",
    });
  }

  const masterSum = path.join(
    ROOT,
    "data/research-engine-v2/census-autopilot-v2-full-universe/03-master-candidate-universe-summary.json"
  );
  const candDir = path.join(
    ROOT,
    "data/research-engine-v2/census-autopilot-v2-full-universe/candidates"
  );
  if (fs.existsSync(masterSum) || fs.existsSync(candDir)) {
    const summary = fs.existsSync(masterSum)
      ? JSON.parse(fs.readFileSync(masterSum, "utf8"))
      : null;
    add({
      file_path: "data/research-engine-v2/census-autopilot-v2-full-universe/candidates/*.json",
      source_type: "cvent_challenge_universe_plus_independent",
      countries_covered: "LATAM/Caribbean multi-country (see shards)",
      candidate_count: summary?.total_candidates || null,
      cvent_origin_count: summary?.cvent_origin_count || null,
      independent_origin_count: summary?.independent_origin_count || null,
      field_availability: [
        "origin_name",
        "origin_country",
        "origin_city",
        "origin_url",
        "candidate_origin",
      ],
      licensed_approved: "cvent_candidate_identity_only — not field-level SoT",
      insert_identity_ok: true,
      field_level_provenance_ok: false,
      duplicate_risk: "high_without_dedupe",
      recommended_use: "shell identity discovery; validate fields via HBX/official later",
      cvent_artifacts_present: true,
    });
  }

  const cventInv = path.join(
    ROOT,
    "reports/research-engine-v2/cvent-latam-harvest-inventory-summary.json"
  );
  if (fs.existsSync(cventInv)) {
    const j = JSON.parse(fs.readFileSync(cventInv, "utf8"));
    add({
      file_path: "reports/research-engine-v2/cvent-latam-harvest-inventory-summary.json",
      source_type: "cvent_harvest_inventory",
      countries_covered: j.probe?.viable_countries,
      candidate_count: j.inventory?.hotel_resort_boutique_urls_harvested,
      field_availability: ["venue urls", "sample titles"],
      licensed_approved: "candidate_inventory_only",
      insert_identity_ok: true,
      field_level_provenance_ok: false,
      duplicate_risk: "high",
      recommended_use: "coverage reference; do not scrape live",
    });
  }

  const ledger = path.join(
    ROOT,
    "data/research-engine-v2/census-autopilot-v4-full-universe/27-universe-ledger-index.json"
  );
  if (fs.existsSync(ledger)) {
    const j = JSON.parse(fs.readFileSync(ledger, "utf8"));
    add({
      file_path: "data/research-engine-v2/census-autopilot-v4-full-universe/27-universe-ledger-index.json",
      source_type: "universe_ledger_index",
      countries_covered: "CALA/LATAM ledger",
      candidate_count: j.ledger_rows,
      field_availability: ["status_counts"],
      licensed_approved: "internal_ops",
      insert_identity_ok: false,
      field_level_provenance_ok: false,
      duplicate_risk: "n/a",
      recommended_use: "coverage planning / status reference",
    });
  }

  const dfsCandidates = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot"
  );
  add({
    file_path: "reports/research-engine-v2/autopilot/**/enrichment-candidates.json",
    source_type: "dataforseo_local_candidates",
    countries_covered: "varies by run",
    candidate_count: "run-scoped",
    field_availability: ["website", "address", "phone", "place_id"],
    licensed_approved: "internal_enrichment_lane",
    insert_identity_ok: true,
    field_level_provenance_ok: "partial_with_policy",
    duplicate_risk: "medium",
    recommended_use: "post-shell enrichment; optional identity if match_high",
    note: "Not bulk-loaded in this dry-run; path pattern noted for inventory",
  });

  const cventPresent = sources.some(
    (s) => s.source_type?.includes("cvent") && s.cvent_artifacts_present !== false
  );

  return {
    generated_at: new Date().toISOString(),
    sources,
    cvent_artifacts_present: cventPresent,
    partial_cvent_status: cventPresent
      ? null
      : "partial_source_remaining_cvent_artifacts_missing",
  };
}

export function loadMasterUniverseCandidates() {
  const dir = path.join(
    ROOT,
    "data/research-engine-v2/census-autopilot-v2-full-universe/candidates"
  );
  if (!fs.existsSync(dir)) return [];
  const out = [];
  for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".json"))) {
    const raw = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
    const arr = Array.isArray(raw) ? raw : raw.candidates || raw.records || [];
    for (const c of arr) {
      out.push({
        candidate_id: c.candidate_id || `univ_${hashId([c.origin_name, c.origin_country, c.origin_url])}`,
        property_name: c.origin_name || c.name || null,
        normalized_property_name: normName(c.origin_name || c.name),
        country: c.origin_country || c.country || null,
        city: c.origin_city || c.city || null,
        website: c.website || null,
        source_name:
          c.candidate_origin === "CVENT_CHALLENGE"
            ? "cvent_candidate"
            : c.origin_source || "independent_discovery",
        source_type:
          c.candidate_origin === "CVENT_CHALLENGE" ? "cvent_candidate" : "independent_discovery",
        source_file: `data/research-engine-v2/census-autopilot-v2-full-universe/candidates/${f}`,
        source_url: c.origin_url || null,
        external_ids: {
          cvent_id:
            c.candidate_origin === "CVENT_CHALLENGE"
              ? c.origin_source_record_id || null
              : null,
        },
        brand_text: c.brand || null,
        chain_text: c.family || null,
        confidence: c.candidate_origin === "VERIFIED_INDEPENDENT" ? "high" : "medium",
        license_status:
          c.candidate_origin === "CVENT_CHALLENGE"
            ? "candidate_identity_only"
            : "internal_ok",
        public_use_status: "hold",
        discovery_notes: c.cvent_used_as_production_evidence
          ? "WARN_cvent_flagged_as_evidence"
          : null,
        raw_origin: c.candidate_origin,
      });
    }
  }
  return out;
}

export function loadHbxCandidates() {
  const fullFp = path.join(
    ROOT,
    "reports/research-engine-v2/hbx-cala-full-geography-candidate-pack.json"
  );
  const wave1Fp = path.join(
    ROOT,
    "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"
  );
  const fp = fs.existsSync(fullFp) ? fullFp : wave1Fp;
  if (!fs.existsSync(fp)) return [];
  const j = JSON.parse(fs.readFileSync(fp, "utf8"));
  return (j.candidates || []).map((c) => ({
    candidate_id: `hbx_${c.hbx_hotel_code}`,
    property_name: c.name,
    normalized_property_name: normName(c.name),
    country: c.country,
    city: c.city,
    address: c.address,
    website: c.website,
    phone: c.phonehotel,
    source_name: "hbx_content_api",
    source_type: "hbx_content_api",
    source_file: path.relative(ROOT, fp).replace(/\\/g, "/"),
    source_url: null,
    external_ids: { hbx_code: c.hbx_hotel_code },
    brand_text: null,
    chain_text: c.chain_code || null,
    hbx_category_code: c.category || null,
    confidence:
      c.match_class === "new_candidate_high" || c.match_class === "existing_match_high"
        ? "high"
        : "medium",
    license_status: "partial_internal",
    public_use_status: "hold",
    hbx_match_class: c.match_class,
    census_record_id: c.census_record_id || null,
    discovery_notes: c.discovery_wave || null,
  }));
}

/**
 * Merge HBX into universe: prefer HBX contact fields when name+country match.
 */
export function mergeCandidateUniverses(universe, hbx) {
  const byKey = new Map();
  const byHbxCode = new Map();
  for (const c of universe) {
    const key = `${c.normalized_property_name}|${normName(c.country)}`;
    if (!byKey.has(key)) byKey.set(key, c);
    const code = c.external_ids?.hbx_code;
    if (code != null && !byHbxCode.has(Number(code))) byHbxCode.set(Number(code), c);
  }
  const merged = [...universe];
  let hbxLinked = 0;
  let hbxAdded = 0;
  for (const h of hbx) {
    const code = h.external_ids?.hbx_code != null ? Number(h.external_ids.hbx_code) : null;
    const key = `${h.normalized_property_name}|${normName(h.country)}`;
    const existing =
      (code != null && byHbxCode.get(code)) || byKey.get(key) || null;
    if (existing) {
      existing.external_ids = { ...(existing.external_ids || {}), ...h.external_ids };
      if (!existing.address && h.address) existing.address = h.address;
      if (!existing.phone && h.phone) existing.phone = h.phone;
      if (!existing.website && h.website) existing.website = h.website;
      if (!existing.city && h.city) existing.city = h.city;
      existing.chain_text = existing.chain_text || h.chain_text;
      existing.hbx_category_code = existing.hbx_category_code || h.hbx_category_code;
      existing.brand_text = existing.brand_text || h.brand_text;
      existing.hbx_match_class = h.hbx_match_class || existing.hbx_match_class;
      existing.census_record_id = existing.census_record_id || h.census_record_id;
      existing.source_count = (existing.source_count || 1) + 1;
      existing.merged_sources = [...new Set([...(existing.merged_sources || [existing.source_type]), "hbx_content_api"])];
      if (h.confidence === "high") existing.confidence = "high";
      if (code != null) byHbxCode.set(code, existing);
      hbxLinked += 1;
    } else {
      const row = { ...h, source_count: 1, merged_sources: ["hbx_content_api"] };
      merged.push(row);
      byKey.set(key, row);
      if (code != null) byHbxCode.set(code, row);
      hbxAdded += 1;
    }
  }
  return { merged, hbxLinked, hbxAdded };
}

export async function listCensusIndex(baseId, token, tableId) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of [
      "Property Name",
      "Canonical Property Name",
      "Country",
      "City",
      "Address",
      "Official Property URL",
      "Phone",
      "Notes for Steward",
      "Property Identity Key",
      "HBX Hotel Code",
    ]) {
      params.append("fields[]", f);
    }
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census_list_failed:${res.status}:${json?.error?.message || ""}`);
    records.push(...(json.records || []));
    offset = json.offset;
    await sleep(110);
  } while (offset);

  const byNameCountry = new Map();
  const byDomain = new Map();
  const byPhone = new Map();
  const byHbx = new Map();
  const byIdentityKey = new Map();
  let hbxFieldHits = 0;
  let hbxNotesHits = 0;

  for (const r of records) {
    const f = r.fields || {};
    const name = normName(f["Canonical Property Name"] || f["Property Name"]);
    const country = normName(f.Country);
    const key = `${name}|${country}`;
    if (!byNameCountry.has(key)) byNameCountry.set(key, []);
    byNameCountry.get(key).push(r);
    const dom = domainOf(f["Official Property URL"]);
    if (dom) {
      if (!byDomain.has(dom)) byDomain.set(dom, []);
      byDomain.get(dom).push(r);
    }
    const ph = normPhone(f.Phone);
    if (ph) {
      if (!byPhone.has(ph)) byPhone.set(ph, []);
      byPhone.get(ph).push(r);
    }
    const idKey = String(f["Property Identity Key"] || "").trim();
    if (idKey) byIdentityKey.set(idKey, r);

    // Prefer dedicated HBX Hotel Code field (post schema repair); notes as fallback
    const fieldCode = String(f["HBX Hotel Code"] || "").trim();
    if (fieldCode && /^\d+$/.test(fieldCode)) {
      const n = Number(fieldCode);
      if (!byHbx.has(n)) {
        byHbx.set(n, r);
        hbxFieldHits += 1;
      }
    } else {
      const notes = String(f["Notes for Steward"] || "");
      const m = notes.match(/hotel_code=(\d+)/);
      if (m) {
        const n = Number(m[1]);
        if (!byHbx.has(n)) {
          byHbx.set(n, r);
          hbxNotesHits += 1;
        }
      }
    }
  }

  return {
    records,
    byNameCountry,
    byDomain,
    byPhone,
    byHbx,
    byIdentityKey,
    count: records.length,
    hbx_index_stats: { from_field: hbxFieldHits, from_notes_fallback: hbxNotesHits },
  };
}

export function classifyAgainstCensus(candidate, index) {
  if (isBlank(candidate.property_name) || isBlank(candidate.country)) {
    return { match_class: MATCH.REJECT_IDENTITY, reason: "missing_name_or_country" };
  }
  if (NON_HOTEL_NAME_RE.test(candidate.property_name)) {
    return { match_class: MATCH.REJECT_NON_HOTEL, reason: "non_hotel_indicator" };
  }

  const hbxCode = candidate.external_ids?.hbx_code;
  if (hbxCode != null && index.byHbx?.has(Number(hbxCode))) {
    const hit = index.byHbx.get(Number(hbxCode));
    const fromField = String(hit.fields?.["HBX Hotel Code"] || "").trim() === String(hbxCode);
    return {
      match_class: MATCH.EXISTING_HIGH,
      census_record_id: hit.id,
      reason: fromField ? "hbx_hotel_code_field" : "hbx_code_in_notes",
    };
  }
  if (candidate.hbx_match_class === "existing_match_high" && candidate.census_record_id) {
    return {
      match_class: MATCH.EXISTING_HIGH,
      census_record_id: candidate.census_record_id,
      reason: "hbx_pack_existing_match_high",
    };
  }

  const identityKey = candidate._planned_identity_key;
  if (identityKey && index.byIdentityKey?.has(identityKey)) {
    return {
      match_class: MATCH.EXISTING_HIGH,
      census_record_id: index.byIdentityKey.get(identityKey).id,
      reason: "property_identity_key",
    };
  }

  const dom = domainOf(candidate.website);
  if (dom && !/cvent\.com/i.test(dom) && index.byDomain.has(dom)) {
    const hits = index.byDomain.get(dom);
    if (hits.length === 1) {
      return {
        match_class: MATCH.EXISTING_HIGH,
        census_record_id: hits[0].id,
        reason: "website_domain",
      };
    }
    return { match_class: MATCH.PROBABLE_DUP, reason: "website_domain_multi", hits: hits.length };
  }

  const ph = normPhone(candidate.phone);
  if (ph && index.byPhone.has(ph)) {
    const hits = index.byPhone.get(ph);
    if (hits.length === 1) {
      return {
        match_class: MATCH.EXISTING_HIGH,
        census_record_id: hits[0].id,
        reason: "phone",
      };
    }
    return { match_class: MATCH.PROBABLE_DUP, reason: "phone_multi" };
  }

  const key = `${candidate.normalized_property_name}|${normName(candidate.country)}`;
  const exact = index.byNameCountry.get(key) || [];
  if (exact.length === 1) {
    const cityCand = normName(candidate.city);
    const cityEx = normName(exact[0].fields?.City);
    if (!cityCand || !cityEx || cityCand === cityEx) {
      return {
        match_class: MATCH.EXISTING_HIGH,
        census_record_id: exact[0].id,
        reason: "exact_name_country",
      };
    }
    return {
      match_class: MATCH.EXISTING_MEDIUM,
      census_record_id: exact[0].id,
      reason: "exact_name_country_city_mismatch",
    };
  }
  if (exact.length > 1) {
    return { match_class: MATCH.PROBABLE_DUP, reason: "exact_name_country_multi" };
  }

  // Fuzzy within same country
  let best = null;
  for (const [k, recs] of index.byNameCountry) {
    if (!k.endsWith(`|${normName(candidate.country)}`)) continue;
    const nName = k.split("|")[0];
    const sim = tokenSimilarity(candidate.normalized_property_name, nName);
    if (sim >= 0.92) {
      best = { sim, recs };
      break;
    }
    if (sim >= 0.85 && (!best || sim > best.sim)) best = { sim, recs };
  }
  if (best?.sim >= 0.92 && best.recs.length === 1) {
    return {
      match_class: MATCH.EXISTING_MEDIUM,
      census_record_id: best.recs[0].id,
      reason: `fuzzy_name_${best.sim.toFixed(2)}`,
    };
  }
  if (best?.sim >= 0.85) {
    return { match_class: MATCH.POSSIBLE_DUP, reason: `fuzzy_review_${best.sim.toFixed(2)}` };
  }

  // New candidate confidence
  if (candidate.hbx_match_class === "new_candidate_high" || candidate.external_ids?.hbx_code) {
    return { match_class: MATCH.NEW_HIGH, reason: "hbx_new_or_code" };
  }
  if (candidate.raw_origin === "VERIFIED_INDEPENDENT" || candidate.confidence === "high") {
    return { match_class: MATCH.NEW_HIGH, reason: "verified_independent_or_high" };
  }
  if (candidate.source_type === "cvent_candidate" && candidate.property_name && candidate.country) {
    return {
      match_class: candidate.city ? MATCH.NEW_MEDIUM : MATCH.NEW_MEDIUM,
      reason: "cvent_identity_shell",
    };
  }
  if (candidate.property_name && candidate.country) {
    return { match_class: MATCH.NEW_MEDIUM, reason: "name_country_only" };
  }
  return { match_class: MATCH.NEW_LOW, reason: "low_identity" };
}

export function buildShellFields(candidate, schemaMissing, opts = {}) {
  const countryBatchLabel = opts.countryBatchLabel || candidate.country;
  const isCvent =
    candidate.source_type === "cvent_candidate" ||
    (candidate.merged_sources || []).includes("cvent_candidate");
  const isHbx =
    Boolean(candidate.external_ids?.hbx_code) ||
    candidate.source_type === "hbx_content_api" ||
    (candidate.merged_sources || []).includes("hbx_content_api");
  const identityKey = `shell_${hashId([
    candidate.normalized_property_name,
    normName(candidate.country),
    candidate.external_ids?.hbx_code,
    candidate.external_ids?.cvent_id,
  ])}`;

  const sourceList = [
    ...new Set(
      (candidate.merged_sources || [candidate.source_type]).filter(Boolean)
    ),
  ];
  const inferredCity =
    isBlank(candidate.city) && /mexico/i.test(String(candidate.country || ""))
      ? inferMexicoCityFromName(candidate.property_name)
      : null;
  const notes = [
    isCvent
      ? "Candidate identity only; field validation required from approved source (Cvent Candidate / Not Field Source)."
      : null,
    isHbx
      ? `hbx_linkage | hotel_code=${candidate.external_ids?.hbx_code} | source=hbx_content_api`
      : null,
    inferredCity
      ? `city_inferred_from_property_name=${inferredCity}`
      : null,
    candidate.chain_text ? `chain_text=${candidate.chain_text}` : null,
    `sources=${sourceList.join(",")}`,
    `dedupe_class_pending_insert`,
  ]
    .filter(Boolean)
    .join("\n");

  const rawName = String(candidate.property_name).trim();
  const canonical =
    toSmartHotelProperCase(rawName) || rawName;

  let discoverySource = "Independent Census Candidate";
  if (isCvent && isHbx) discoverySource = "Cvent + HBX Candidate";
  else if (isHbx) discoverySource = "HBX Content API";
  else if (isCvent) discoverySource = "Cvent Candidate / Not Field Source";

  let sourceCandidateType = "Shell Identity";
  if (isCvent && isHbx) sourceCandidateType = "Multi-Source Candidate";
  else if (isHbx) sourceCandidateType = "HBX Linked Shell";
  else if (isCvent) sourceCandidateType = "Cvent Identity Candidate";

  const sourceCount = [isCvent, isHbx, !isCvent && !isHbx].filter(Boolean).length;

  /** @type {Record<string, unknown>} */
  const fields = {
    "Property Name": rawName,
    "Canonical Property Name": canonical,
    "Property Identity Key": identityKey,
    Country: candidate.country,
    "Production Use Status": "Census Only / Not Owner-Facing",
    "Public Display Review Status": "Hold",
    "Radar Display Status": "Hold",
    "Human Review Required": true,
    "Enrichment Status": "Discovered — pending enrichment",
    "Last Reviewed Date": todayIsoDate(),
    "Discovery Date": todayIsoDate(),
    "Data Confidence Tier": isHbx ? "Medium" : "Low",
    "Identity Confidence": isHbx ? "Medium" : "Low",
    "Source Confidence": isHbx ? "Medium" : "Low",
    "Source Type": isCvent ? "other" : isHbx ? "other" : "independent_discovery",
    "Notes for Steward": notes,
    "Discovery Source": discoverySource,
    "Source Candidate Type": sourceCandidateType,
    "Candidate Source Count": sourceCount,
    "Review Status": "Internal Only",
    "Shell Insert Batch ID": "full-cala-15k-census-shell-insert-v1",
    "Shell Insert Country Batch": countryBatchLabel,
    "Shell Insert Date": todayIsoDate(),
    "Shell Insert Source Mix": sourceList.sort().join("+") || discoverySource,
    "Shell Dedupe Confidence": isHbx ? "High" : isCvent ? "Medium" : "Review Needed",
  };

  if (!isBlank(candidate.city)) fields.City = titleCity(candidate.city);
  else if (inferredCity) fields.City = inferredCity;
  if (!isBlank(candidate.address) && !isCvent) {
    fields.Address = String(candidate.address).trim();
  }

  const web = normalizeWebsite(candidate.website);
  const host = domainOf(web);
  if (web && host && !/cvent\.com/i.test(host) && !isRejectedDiscoveryHost(host)) {
    fields["Official Property URL"] = web;
  }

  if (isHbx && candidate.phone) {
    fields.Phone = String(candidate.phone).trim();
  }

  if (isHbx && candidate.external_ids?.hbx_code != null) {
    fields["HBX Hotel Code"] = String(candidate.external_ids.hbx_code);
    if (candidate.chain_text) fields["HBX Chain Code"] = String(candidate.chain_text).trim();
    if (candidate.hbx_category_code) {
      fields["HBX Category Code"] = String(candidate.hbx_category_code).trim();
    }
    fields["HBX Linkage Confidence"] =
      candidate.hbx_match_class === "new_candidate_high" ? "High" : "Medium";
    fields["HBX Source Status"] = "Candidate";
    fields["HBX Content Review Status"] = "Internal Only";
  }

  // Candidate brand only — never Current Brand / Brand Family / Family / Source Family
  const brandText = candidate.brand_text || null;
  const chainText = candidate.chain_text || null;
  if (brandText || (isHbx && chainText)) {
    const candBrand = brandText || chainText;
    fields["Candidate Brand Text"] = String(candBrand).trim();
    if (isCvent && isHbx && brandText) {
      fields["Candidate Brand Source"] = "Cvent + HBX Candidate";
      fields["Candidate Brand Confidence"] = "Medium";
    } else if (isHbx && !brandText) {
      fields["Candidate Brand Source"] = "HBX Content API";
      fields["Candidate Brand Confidence"] = "Medium";
    } else if (isCvent) {
      fields["Candidate Brand Source"] = "Cvent Candidate / Not Field Source";
      fields["Candidate Brand Confidence"] = "Candidate";
    } else {
      fields["Candidate Brand Source"] = "Independent Census Candidate";
      fields["Candidate Brand Confidence"] = "Candidate";
    }
    fields["Brand Validation Status"] = "Unvalidated / Needs Review";
  }

  const dropped = [];
  for (const f of [
    "Discovery Source",
    "Source Candidate Type",
    "Candidate Source Count",
    "Review Status",
    "Shell Insert Batch ID",
    "Candidate Brand Text",
  ]) {
    if (schemaMissing.includes(f)) dropped.push(f);
  }

  const sanitized = {};
  for (const [k, v] of Object.entries(fields)) {
    if (FORBIDDEN_SHELL.has(k) || isForbiddenAutopilotField(k)) continue;
    if (!SHELL_INSERT_FIELDS.has(k)) continue;
    if (schemaMissing.includes(k)) continue;
    if (v === undefined || v === null || v === "") continue;
    sanitized[k] = v;
  }

  return {
    fields: sanitized,
    schema_missing_intended: dropped,
    validation: {
      pass: Boolean(sanitized["Property Name"] && sanitized.Country),
      failed_checks: [],
    },
    meta: {
      is_cvent: isCvent,
      is_hbx: isHbx,
      has_candidate_brand: Boolean(sanitized["Candidate Brand Text"]),
    },
  };
}

async function airtableFetchWithRetry(url, init, { retries = 4, log } = {}) {
  let lastErr = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, init);
      return res;
    } catch (err) {
      lastErr = err;
      const code = err?.cause?.code || err?.code || "";
      const msg = String(err?.message || err);
      const retryable =
        /ECONNABORTED|ECONNRESET|ETIMEDOUT|ENOTFOUND|fetch failed|socket/i.test(
          `${code} ${msg}`
        );
      if (!retryable || attempt === retries) throw err;
      const waitMs = 800 * (attempt + 1) * (attempt + 1);
      log?.(
        `[full-cala-15k] network retry ${attempt + 1}/${retries} after ${code || msg} wait=${waitMs}ms`
      );
      await sleep(waitMs);
    }
  }
  throw lastErr;
}

export async function insertBatch(records, { baseId, token, tableId, log }) {
  let inserted = 0;
  const errors = [];
  const createdIds = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10).map((r) => ({ fields: r.fields }));
    let res;
    try {
      res = await airtableFetchWithRetry(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ records: chunk, typecast: true }),
        },
        { log }
      );
    } catch (err) {
      errors.push({
        status: 0,
        error: { message: String(err?.message || err), code: err?.cause?.code },
        batch_start: i,
      });
      log?.(
        `[full-cala-15k] insert chunk network fail at ${i}: ${err?.message || err}`
      );
      continue;
    }
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      errors.push({ status: res.status, error: json.error || json, batch_start: i });
      log?.(`[full-cala-15k] insert batch ${res.status}; retrying one-by-one`);
      for (const rec of chunk) {
        try {
          const one = await airtableFetchWithRetry(
            `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
            {
              method: "POST",
              headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({ records: [rec], typecast: true }),
            },
            { log }
          );
          const oneJson = await one.json().catch(() => ({}));
          if (!one.ok) {
            errors.push({ status: one.status, error: oneJson.error || oneJson });
          } else {
            inserted += 1;
            createdIds.push(oneJson.records?.[0]?.id);
          }
        } catch (err) {
          errors.push({
            status: 0,
            error: { message: String(err?.message || err) },
          });
        }
        await sleep(180);
      }
    } else {
      inserted += (json.records || []).length;
      for (const r of json.records || []) createdIds.push(r.id);
    }
    await sleep(220);
  }
  return { inserted, errors, createdIds };
}

function renderInventoryMd(inv) {
  return `# Full CALA 15K — Source Inventory

Generated: ${inv.generated_at}
Cvent artifacts present: **${inv.cvent_artifacts_present}**
${inv.partial_cvent_status ? `Flag: \`${inv.partial_cvent_status}\`` : ""}

| Source | Type | Count | Insert identity | Field provenance | Use |
| --- | --- | ---: | --- | --- | --- |
${inv.sources
  .map(
    (s) =>
      `| \`${s.file_path}\` | ${s.source_type} | ${s.candidate_count ?? "—"} | ${s.insert_identity_ok} | ${s.field_level_provenance_ok} | ${s.recommended_use} |`
  )
  .join("\n")}
`;
}

function renderMainMd(report) {
  return `# Full CALA 15K Census Shell Insert v1

**Status:** \`${report.status}\`  
**Objective:** \`${report.objective}\`  
**Generated:** ${report.generated_at}  
**Dry run:** ${report.dry_run}  
**Airtable inserts:** **${report.inserts_applied}**

## Universe
- Existing Census before: **${report.census_before_count}**
- Total staging candidates: **${report.total_candidates}**
- Eligible shell inserts: **${report.eligible_shell_inserts}**
- Expected Census after full insert (approx): **${report.expected_census_after_full}**

## Classification
${Object.entries(report.by_match_class || {})
  .map(([k, n]) => `- \`${k}\`: **${n}**`)
  .join("\n")}

## Source contribution
${Object.entries(report.by_source_type || {})
  .map(([k, n]) => `- ${k}: **${n}**`)
  .join("\n")}

## By country (eligible inserts)
${Object.entries(report.eligible_by_country || {})
  .sort((a, b) => b[1] - a[1])
  .slice(0, 25)
  .map(([k, n]) => `- ${k}: **${n}**`)
  .join("\n")}

## First batch plan
- Country: **${report.first_batch?.country || "—"}**
- Planned inserts: **${report.first_batch?.planned || 0}**
- Applied: **${report.first_batch?.applied || 0}**
- Country eligible (pre-cap / quality-filtered): **${report.first_batch?.country_eligible_total ?? "—"}**
- Country eligible before quality: **${report.first_batch?.country_eligible_before_quality ?? "—"}**
- Country remaining eligible: **${report.first_batch?.country_remaining_eligible ?? "—"}**
- Source mix (batch): ${JSON.stringify(report.first_batch?.source_mix || {})}
- Preflight class mix (batch): ${JSON.stringify(report.first_batch?.preflight_class_mix || {})}
- HBX codes in batch: **${report.first_batch?.hbx_codes_in_batch ?? 0}**
- Census after (estimate): **${report.census_after_estimate ?? "—"}**
- HBX field index hits: **${report.hbx_index_stats?.from_field ?? "—"}**
- Plan skipped (HBX in-batch dedupe): **${report.plan_skipped_hbx_dedupe ?? 0}**
- Plan skipped (name+country in-batch): **${report.plan_skipped_name_dedupe ?? 0}**
- Stopped on error threshold: **${report.stopped_on_threshold ? "yes" : "no"}**
- Preflight blocked: **${report.preflight_blocked ? "yes" : "no"}**

## Mexico preflight quality
${
  report.mexico_preflight
    ? `- Remaining eligible reviewed: **${report.mexico_preflight.remaining_eligible}**
- Insertable (safe+review): **${report.mexico_preflight.insertable_count}**
- Held/reject: **${report.mexico_preflight.held_or_reject_count}**
- Classifications: ${JSON.stringify(report.mexico_preflight.classifications || {})}
- Source mix: ${JSON.stringify(report.mexico_preflight.source_mix || {})}
- Field presence: ${JSON.stringify(report.mexico_preflight.field_presence || {})}
- Top-500 hold ratio: **${report.mexico_preflight.top500_hold_ratio}**
- Block reason: **${report.mexico_preflight.block_reason || "none"}**`
    : "- (not run)"
}

## Schema missing (shell extras)
${(report.schema_missing || []).map((f) => `- \`${f}\``).join("\n") || "- none"}

## Policy holds
- Rooms / Keys, coordinates, images, descriptions, facilities
- Owner/operator/developer/dates/Recent Momentum/Company Validated/Brand Verified
- Cvent = candidate identity only (not field-level SoT)
- Current Brand not written from chain/brand text

## Confirmations
- Hotel Property Census only: **true**
- No Brand Explorer / Brand Setup / VIC: **true**
- No duplicate inserts intended (dedupe gate): **true**
- All inserts Census Only / Hold / HR Required: **true**
- Checkpoint: \`${report.checkpoint_path || ""}\`
`;
}

/**
 * @param {object} opts
 */
export async function runFullCala15kCensusShellInsertV1(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const args = opts.args || {};
  const generated_at = new Date().toISOString();
  const gates = resolveFullCala15kGates(env);

  if (!gates.ok) {
    const report = {
      ok: false,
      status: FULL_CALA_15K_STATUS.BLOCKED,
      objective: FULL_CALA_15K_OBJECTIVE,
      generated_at,
      reason: "gate_blockers",
      blockers: gates.blockers,
      inserts_applied: 0,
      dry_run: true,
    };
    persistAll(report, { inventory: buildSourceInventory() });
    return report;
  }

  if (!gates.shell_mission && !opts.force) {
    // Allow dry-run inventory even if flag off when called explicitly with force/dry
  }

  const inventory = buildSourceInventory();
  const enableInserts = Boolean(
    opts.enableProductionWrites && gates.inserts && gates.shell_mission
  );

  let token;
  let baseId;
  try {
    token = resolvePat();
    const base = resolveTargetBase();
    baseId = base?.target_base_id || base?.baseId || env.AIRTABLE_BASE_ID_ALT;
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
  } catch (err) {
    const report = {
      ok: false,
      status: FULL_CALA_15K_STATUS.BLOCKED,
      objective: FULL_CALA_15K_OBJECTIVE,
      generated_at,
      reason: String(err?.message || err).slice(0, 300),
      inserts_applied: 0,
      dry_run: true,
    };
    persistAll(report, { inventory });
    return report;
  }

  // Schema check for shell extras
  const metaRes = await fetch(
    `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const metaJson = await metaRes.json();
  const table = (metaJson.tables || []).find((t) => t.id === CENSUS_TABLE_ID);
  const fieldSet = new Set((table?.fields || []).map((f) => f.name));
  const schema_missing = [
    "Discovery Source",
    "Source Candidate Type",
    "Candidate Source Count",
    "Review Status",
  ].filter((f) => !fieldSet.has(f));
  // HBX Hotel Code + provenance are expected present after schema repair; still list if absent
  for (const f of [
    "HBX Hotel Code",
    "HBX Chain Code",
    "HBX Category Code",
    "HBX Linkage Confidence",
    "HBX Source Status",
    "HBX Content Review Status",
    "Shell Insert Batch ID",
    "Shell Insert Country Batch",
    "Shell Insert Date",
    "Shell Insert Source Mix",
    "Shell Dedupe Confidence",
    "Candidate Brand Text",
    "Candidate Brand Source",
    "Candidate Brand Confidence",
    "Brand Validation Status",
  ]) {
    if (!fieldSet.has(f)) schema_missing.push(f);
  }

  log("[full-cala-15k] loading candidates…");
  const universe = loadMasterUniverseCandidates();
  const hbx = loadHbxCandidates();
  const { merged, hbxLinked, hbxAdded } = mergeCandidateUniverses(universe, hbx);

  log(`[full-cala-15k] indexing census…`);
  const index = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);

  const classified = [];
  const by_match_class = {};
  const by_source_type = {};
  const by_country_all = {};

  for (const c of merged) {
    c.source_count = c.source_count || 1;
    c.merged_sources = c.merged_sources || [c.source_type];
    const cls = classifyAgainstCensus(c, index);
    const row = { ...c, ...cls };
    classified.push(row);
    by_match_class[cls.match_class] = (by_match_class[cls.match_class] || 0) + 1;
    by_source_type[c.source_type] = (by_source_type[c.source_type] || 0) + 1;
    by_country_all[c.country || "UNK"] = (by_country_all[c.country || "UNK"] || 0) + 1;
  }

  const insertableClasses = new Set([MATCH.NEW_HIGH, MATCH.NEW_MEDIUM]);
  let eligible = classified.filter((c) => insertableClasses.has(c.match_class));

  // Gate by source insert flags
  eligible = eligible.filter((c) => {
    if (c.source_type === "cvent_candidate" && !c.external_ids?.hbx_code) {
      return gates.cvent_inserts || !enableInserts; // dry-run can plan; apply needs flag
    }
    if (c.external_ids?.hbx_code || c.source_type === "hbx_content_api") {
      return gates.hbx_inserts || !enableInserts;
    }
    return true;
  });

  // For dry-run planning always include both; for apply filter strictly
  if (enableInserts) {
    eligible = classified.filter((c) => {
      if (!insertableClasses.has(c.match_class)) return false;
      const hasHbx = Boolean(c.external_ids?.hbx_code);
      const isCventOnly = c.source_type === "cvent_candidate" && !hasHbx;
      if (isCventOnly && !gates.cvent_inserts) return false;
      if (hasHbx && !gates.hbx_inserts) return false;
      return true;
    });
  } else {
    eligible = classified.filter((c) => insertableClasses.has(c.match_class));
  }

  const eligible_by_country = {};
  for (const c of eligible) {
    eligible_by_country[c.country || "UNK"] =
      (eligible_by_country[c.country || "UNK"] || 0) + 1;
  }

  const countryFilter = args.country || null;
  const maxInserts = args.maxInserts != null ? Number(args.maxInserts) : null;
  const objectiveRaw = String(args.objective || opts.objective || "").toLowerCase();
  const mexicoBatchNumber = resolveMexicoShellBatchNumber({
    objective: objectiveRaw,
    shellCountryBatch: args.shellCountryBatch,
  });
  const countryBatchLabel =
    args.shellCountryBatch ||
    (countryFilter === "Mexico" && /mexico-batch/.test(objectiveRaw)
      ? `Mexico Batch ${mexicoBatchNumber}`
      : null);

  // Build insert plan ordered by COUNTRY_BATCH_ORDER then alpha
  const orderedCountries = [
    ...COUNTRY_BATCH_ORDER,
    ...Object.keys(eligible_by_country)
      .filter((c) => !COUNTRY_BATCH_ORDER.includes(c))
      .sort(),
  ];

  const insertPlan = [];
  const seenHbxInPlan = new Set();
  const seenNameCountryInPlan = new Set();
  let plan_skipped_hbx_dedupe = 0;
  let plan_skipped_name_dedupe = 0;
  for (const country of orderedCountries) {
    const rows = eligible
      .filter((c) => c.country === country)
      .sort((a, b) => {
        const ap = mexicoBatchSourcePriority(a);
        const bp = mexicoBatchSourcePriority(b);
        if (ap !== bp) return ap - bp;
        // Prefer higher match confidence within same priority
        const aHigh = a.match_class === MATCH.NEW_HIGH ? 0 : 1;
        const bHigh = b.match_class === MATCH.NEW_HIGH ? 0 : 1;
        if (aHigh !== bHigh) return aHigh - bHigh;
        return String(a.property_name).localeCompare(String(b.property_name));
      });
    for (const c of rows) {
      const hbxCode =
        c.external_ids?.hbx_code != null ? Number(c.external_ids.hbx_code) : null;
      if (hbxCode != null && seenHbxInPlan.has(hbxCode)) {
        plan_skipped_hbx_dedupe += 1;
        continue;
      }
      const ncKey = `${c.normalized_property_name}|${normName(c.country)}`;
      if (seenNameCountryInPlan.has(ncKey)) {
        plan_skipped_name_dedupe += 1;
        continue;
      }
      const built = buildShellFields(c, schema_missing, {
        countryBatchLabel: countryBatchLabel || c.country,
      });
      if (!built.validation.pass) continue;
      const preflight = classifyShellPreflightQuality(c, {
        cventOnlyQualityGate:
          gates.cvent_only_quality_gate ||
          (gates.batch_preflight && mexicoBatchNumber >= 3),
      });
      if (hbxCode != null) seenHbxInPlan.add(hbxCode);
      seenNameCountryInPlan.add(ncKey);
      insertPlan.push({
        candidate_id: c.candidate_id,
        country: c.country,
        property_name: c.property_name,
        match_class: c.match_class,
        source_type: c.source_type,
        hbx_hotel_code: hbxCode,
        is_cvent: Boolean(built.meta?.is_cvent),
        is_hbx: Boolean(built.meta?.is_hbx),
        has_candidate_brand: Boolean(built.meta?.has_candidate_brand),
        source_priority: mexicoBatchSourcePriority(c),
        preflight_class: preflight.class,
        preflight_reason: preflight.reason,
        fields: built.fields,
        schema_missing_intended: built.schema_missing_intended,
      });
    }
  }

  const mexicoEligibleCandidates = eligible.filter(
    (c) => !countryFilter || c.country === countryFilter
  );
  const preflightEnabled =
    gates.batch_preflight ||
    (countryFilter === "Mexico" && mexicoBatchNumber >= 3);
  const cventQualityEnabled =
    gates.cvent_only_quality_gate ||
    (preflightEnabled && mexicoBatchNumber >= 3);
  let mexico_preflight = null;
  let preflight_blocked = false;
  if (preflightEnabled && (countryFilter === "Mexico" || !countryFilter)) {
    const pool =
      countryFilter === "Mexico"
        ? mexicoEligibleCandidates
        : eligible.filter((c) => c.country === "Mexico");
    mexico_preflight = buildMexicoBatchPreflightSummary(pool, {
      cventOnlyQualityGate: cventQualityEnabled,
    });
    if (mexico_preflight.block_writes) {
      preflight_blocked = true;
      log(
        `[full-cala-15k] preflight BLOCK writes reason=${mexico_preflight.block_reason} insertable=${mexico_preflight.insertable_count}`
      );
    } else {
      log(
        `[full-cala-15k] preflight OK insertable=${mexico_preflight.insertable_count}/${mexico_preflight.remaining_eligible} top500_hold_ratio=${mexico_preflight.top500_hold_ratio}`
      );
    }
  }

  // Quality gate: only SAFE + REVIEW when preflight/cvent quality enabled for Mexico
  let qualityFilteredPlan = insertPlan;
  if (cventQualityEnabled && countryFilter === "Mexico") {
    qualityFilteredPlan = insertPlan.filter((p) =>
      INSERTABLE_PREFLIGHT.has(p.preflight_class)
    );
  }

  let batchPlan = qualityFilteredPlan;
  const firstCountryDefault = countryFilter || COUNTRY_BATCH_ORDER[0];
  const firstCountryCap =
    maxInserts != null && Number.isFinite(maxInserts) ? maxInserts : 500;
  const countryEligibleInPlan = qualityFilteredPlan.filter(
    (p) => p.country === firstCountryDefault
  ).length;
  const countryEligibleBeforeQuality = insertPlan.filter(
    (p) => p.country === firstCountryDefault
  ).length;
  const proposedFirstBatch = qualityFilteredPlan
    .filter((p) => p.country === firstCountryDefault)
    .slice(0, firstCountryCap);

  const allowInserts = enableInserts && !preflight_blocked;
  if (allowInserts) {
    batchPlan = countryFilter
      ? qualityFilteredPlan
          .filter((p) => p.country === countryFilter)
          .slice(0, firstCountryCap)
      : proposedFirstBatch;
  } else {
    // Dry-run or preflight block: report recommended/safe batch only
    batchPlan = proposedFirstBatch;
  }

  let inserts_applied = 0;
  let insert_errors = [];
  let created_ids = [];
  let stopped_on_threshold = false;

  const priorCheckpoint = fs.existsSync(CHECKPOINT_FILE)
    ? JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8"))
    : null;

  const checkpoint = {
    version: FULL_CALA_15K_VERSION,
    objective: FULL_CALA_15K_OBJECTIVE,
    updated_at: generated_at,
    dry_run: !enableInserts,
    census_before: index.count,
    total_candidates: classified.length,
    eligible: eligible.length,
    hbx_index_stats: index.hbx_index_stats || null,
    plan_skipped_hbx_dedupe,
    plan_skipped_name_dedupe,
    batches: [...(priorCheckpoint?.batches || [])],
  };

  if (allowInserts && batchPlan.length) {
    const batchSize = Math.min(Number(args.batchSize || 100), 500);
    for (let i = 0; i < batchPlan.length; i += batchSize) {
      const chunk = batchPlan.slice(i, i + batchSize);
      log(`[full-cala-15k] inserting batch ${i / batchSize + 1} size=${chunk.length} country=${chunk[0]?.country}`);
      const result = await insertBatch(chunk, {
        baseId,
        token,
        tableId: CENSUS_TABLE_ID,
        log,
      });
      inserts_applied += result.inserted;
      insert_errors.push(...result.errors);
      created_ids.push(...result.createdIds.filter(Boolean));
      checkpoint.batches.push({
        at: new Date().toISOString(),
        offset: i,
        planned: chunk.length,
        inserted: result.inserted,
        errors: result.errors.length,
        country: chunk[0]?.country,
        shell_country_batch: countryBatchLabel || null,
      });
      writeJson(CHECKPOINT_FILE, checkpoint);
      const errRate = result.errors.length / Math.max(chunk.length, 1);
      if (errRate > 0.25 || result.errors.length >= 10) {
        log("[full-cala-15k] stopping — error/conflict threshold exceeded");
        stopped_on_threshold = true;
        break;
      }
    }
  } else {
    writeJson(CHECKPOINT_FILE, checkpoint);
  }

  const dupHolds =
    (by_match_class[MATCH.PROBABLE_DUP] || 0) + (by_match_class[MATCH.POSSIBLE_DUP] || 0);

  const mexicoStatuses = mexicoBatchStatusBundle(mexicoBatchNumber);
  const countryStatus = resolveCountryBatchStatus(
    batchPlan[0]?.country || countryFilter,
    inserts_applied,
    {
      blocked: Boolean(
        (enableInserts && inserts_applied === 0 && batchPlan.length) ||
          preflight_blocked
      ),
      mexicoBatchNumber,
      objective: objectiveRaw,
      shellCountryBatch: countryBatchLabel,
    }
  );
  let status = FULL_CALA_15K_STATUS.DRY_RUN_READY;
  if (preflight_blocked) {
    status = mexicoStatuses.blocked;
  } else if (allowInserts && inserts_applied > 0) {
    status = countryStatus || FULL_CALA_15K_STATUS.BATCH_APPLY_COMPLETE;
  } else if (enableInserts && inserts_applied === 0 && batchPlan.length) {
    const c = String(countryFilter || batchPlan[0]?.country);
    status =
      c === "Colombia"
        ? FULL_CALA_15K_STATUS.COLOMBIA_BATCH_BLOCKED
        : c === "Mexico"
          ? mexicoStatuses.blocked
          : FULL_CALA_15K_STATUS.BLOCKED;
  }
  if (!inventory.cvent_artifacts_present) {
    status = FULL_CALA_15K_STATUS.PARTIAL_CVENT;
  } else if (dupHolds > 500 && !enableInserts) {
    status = FULL_CALA_15K_STATUS.DRY_RUN_READY;
  }

  const batchCountry = batchPlan[0]?.country || countryFilter || COUNTRY_BATCH_ORDER[0];
  const cventOnly = batchPlan.filter((p) => p.is_cvent && !p.is_hbx).length;
  const hbxOnly = batchPlan.filter((p) => p.is_hbx && !p.is_cvent).length;
  const cventHbx = batchPlan.filter((p) => p.is_cvent && p.is_hbx).length;
  const candidateBrandWrites = batchPlan.filter((p) => p.has_candidate_brand).length;
  const mexicoRemaining = Math.max(0, countryEligibleInPlan - inserts_applied);
  const mexicoHeldRemaining = mexico_preflight?.held_or_reject_count || 0;
  const mexicoNextBatchNumber = mexicoBatchNumber + 1;
  const mexicoNextBatchPlan =
    batchCountry === "Mexico" && (mexicoRemaining > 0 || mexicoHeldRemaining > 0)
      ? {
          country: "Mexico",
          batch: `Mexico Batch ${mexicoNextBatchNumber}`,
          batch_number: mexicoNextBatchNumber,
          remaining_eligible: mexicoRemaining,
          held_or_reject: mexicoHeldRemaining,
          recommended_max_inserts: mexicoRemaining > 0 ? 500 : 0,
          recommended_priority:
            mexicoRemaining > 0
              ? "Continue HBX-backed → Cvent+HBX → independent multi-source → strong Cvent-only only; hold weak identity"
              : "Do not insert remaining weak Cvent-only without city/website/address enrichment; prefer next country or source enrichment first",
          objective: `full-cala-15k-census-shell-insert-v1-mexico-batch-${mexicoNextBatchNumber}`,
          gate_note:
            mexicoRemaining === 0 && mexicoHeldRemaining > 0
              ? "quality_filtered_insertable_exhausted"
              : null,
        }
      : null;

  const qualityPartial =
    Boolean(mexico_preflight) &&
    (mexico_preflight.held_or_reject_count > 0 ||
      mexico_preflight.block_writes ||
      countryEligibleBeforeQuality > countryEligibleInPlan);

  const report = {
    ok:
      status !== FULL_CALA_15K_STATUS.BLOCKED &&
      status !== FULL_CALA_15K_STATUS.COLOMBIA_BATCH_BLOCKED &&
      status !== FULL_CALA_15K_STATUS.MEXICO_BATCH_1_BLOCKED &&
      status !== FULL_CALA_15K_STATUS.MEXICO_BATCH_2_BLOCKED &&
      status !== FULL_CALA_15K_STATUS.MEXICO_BATCH_3_BLOCKED &&
      status !== mexicoStatuses.blocked,
    status,
    secondary_statuses: [
      dupHolds > 0
        ? batchCountry === "Colombia"
          ? FULL_CALA_15K_STATUS.COLOMBIA_BATCH_PARTIAL_DUP
          : batchCountry === "Mexico"
            ? mexicoStatuses.partial_dup
            : FULL_CALA_15K_STATUS.PARTIAL_DUP
        : null,
      countryEligibleInPlan > inserts_applied && batchCountry === "Mexico"
        ? mexicoStatuses.partial_source
        : null,
      qualityPartial && batchCountry === "Mexico"
        ? mexicoStatuses.partial_quality
        : null,
      FULL_CALA_15K_STATUS.PARTIAL_LICENSE,
      inventory.partial_cvent_status,
    ].filter(Boolean),
    objective: FULL_CALA_15K_OBJECTIVE,
    version: FULL_CALA_15K_VERSION,
    generated_at,
    dry_run: !allowInserts,
    preflight_blocked,
    mexico_preflight,
    inserts_applied,
    stopped_on_threshold,
    census_before_count: index.count,
    total_candidates: classified.length,
    hbx_linked_into_universe: hbxLinked,
    hbx_only_added: hbxAdded,
    hbx_index_stats: index.hbx_index_stats || null,
    plan_skipped_hbx_dedupe,
    plan_skipped_name_dedupe,
    existing_match_high_skipped: by_match_class[MATCH.EXISTING_HIGH] || 0,
    by_match_class,
    by_source_type,
    by_country_all,
    eligible_shell_inserts: eligible.length,
    eligible_by_country,
    expected_census_after_full: index.count + eligible.length,
    schema_missing,
    first_batch: {
      country: batchCountry,
      planned: batchPlan.length,
      applied: inserts_applied,
      country_eligible_total: countryEligibleInPlan,
      country_eligible_before_quality: countryEligibleBeforeQuality,
      country_remaining_eligible: Math.max(0, countryEligibleInPlan - inserts_applied),
      source_mix: batchPlan.reduce((acc, p) => {
        const k = p.source_type || "unknown";
        acc[k] = (acc[k] || 0) + 1;
        return acc;
      }, {}),
      preflight_class_mix: batchPlan.reduce((acc, p) => {
        const k = p.preflight_class || "unclassified";
        acc[k] = (acc[k] || 0) + 1;
        return acc;
      }, {}),
      cvent_only_shells: cventOnly,
      hbx_only_shells: hbxOnly,
      cvent_plus_hbx_shells: cventHbx,
      hbx_codes_in_batch: batchPlan.filter((p) => p.hbx_hotel_code != null).length,
      candidate_brand_writes: candidateBrandWrites,
      current_brand_writes: 0,
      brand_family_writes: 0,
      country_batch_label: countryBatchLabel || batchCountry,
      mexico_batch_number: batchCountry === "Mexico" ? mexicoBatchNumber : null,
      source_priority_mix: batchPlan.reduce((acc, p) => {
        const k = String(p.source_priority ?? "na");
        acc[k] = (acc[k] || 0) + 1;
        return acc;
      }, {}),
    },
    mexico_batch_number: batchCountry === "Mexico" ? mexicoBatchNumber : null,
    mexico_next_batch_plan: mexicoNextBatchPlan,
    mexico_batch_2_plan:
      mexicoNextBatchPlan && mexicoNextBatchPlan.batch_number === 2
        ? mexicoNextBatchPlan
        : null,
    mexico_batch_3_plan:
      mexicoNextBatchPlan && mexicoNextBatchPlan.batch_number === 3
        ? mexicoNextBatchPlan
        : null,
    mexico_batch_4_plan:
      mexicoNextBatchPlan && mexicoNextBatchPlan.batch_number === 4
        ? mexicoNextBatchPlan
        : null,
    census_after_estimate: index.count + inserts_applied,
    insert_errors: insert_errors.slice(0, 20),
    created_ids_sample: created_ids.slice(0, 20),
    checkpoint_path: "data/research-engine-v2/full-cala-15k-census-shell/full-cala-15k-checkpoint.json",
    policy_holds: [
      "rooms_keys",
      "coordinates",
      "images",
      "descriptions",
      "facilities",
      "owner_operator_dates",
      "public_display",
      "cvent_not_field_sot",
      "no_current_brand_from_cvent",
      "no_family_source_family_unvalidated",
    ],
    gates,
    confirmations: {
      hotel_property_census_only: true,
      no_brand_explorer: true,
      no_brand_setup: true,
      no_vic: true,
      no_rooms_from_hbx: true,
      no_public_display_writes: true,
      shells_hold_hr_required: true,
      no_current_brand_writes: true,
      no_brand_family_writes: true,
    },
  };

  const universeOut = {
    objective: FULL_CALA_15K_OBJECTIVE,
    generated_at,
    count: classified.length,
    by_match_class,
    sample: classified.slice(0, 50).map((c) => ({
      candidate_id: c.candidate_id,
      property_name: c.property_name,
      country: c.country,
      city: c.city,
      match_class: c.match_class,
      source_type: c.source_type,
      hbx_code: c.external_ids?.hbx_code || null,
    })),
  };

  const dedupeOut = {
    objective: FULL_CALA_15K_OBJECTIVE,
    generated_at,
    census_indexed: index.count,
    by_match_class,
    duplicate_holds: dupHolds,
    existing_high: by_match_class[MATCH.EXISTING_HIGH] || 0,
    existing_medium: by_match_class[MATCH.EXISTING_MEDIUM] || 0,
  };

  const insertPlanOut = {
    objective: FULL_CALA_15K_OBJECTIVE,
    generated_at,
    dry_run: !enableInserts,
    total_eligible: insertPlan.length,
    batch_planned: batchPlan.length,
    country_order: orderedCountries,
    first_batch_country: batchPlan[0]?.country,
    sample: batchPlan.slice(0, 30),
  };

  persistAll(report, {
    inventory,
    universeOut,
    dedupeOut,
    insertPlanOut,
  });

  log(
    `[full-cala-15k] status=${report.status} eligible=${eligible.length} inserts=${inserts_applied} dry_run=${!enableInserts}`
  );
  return report;
}

function persistAll(report, { inventory, universeOut, dedupeOut, insertPlanOut } = {}) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  writeJson(path.join(reportsDir, "full-cala-15k-census-shell-insert-v1.json"), report);
  writeMd(path.join(reportsDir, "full-cala-15k-census-shell-insert-v1.md"), renderMainMd(report));
  writeMd(path.join(docsDir, "full-cala-15k-census-shell-insert-v1.md"), renderMainMd(report));

  // Country-batch dedicated artifacts
  const batchCountry = report.first_batch?.country;
  if (batchCountry === "Colombia") {
    const slug = "full-cala-15k-census-shell-insert-v1-colombia-batch";
    writeJson(path.join(reportsDir, `${slug}.json`), report);
    writeMd(path.join(reportsDir, `${slug}.md`), renderMainMd(report));
    writeMd(path.join(docsDir, `${slug}.md`), renderMainMd(report));
  }
  if (batchCountry === "Mexico") {
    const n = report.mexico_batch_number || 1;
    const slug = `full-cala-15k-census-shell-insert-v1-mexico-batch-${n}`;
    writeJson(path.join(reportsDir, `${slug}.json`), report);
    writeMd(path.join(reportsDir, `${slug}.md`), renderMainMd(report));
    writeMd(path.join(docsDir, `${slug}.md`), renderMainMd(report));
    const nextPlan = report.mexico_next_batch_plan;
    if (nextPlan) {
      const planSlug = `full-cala-15k-mexico-batch-${nextPlan.batch_number}-plan`;
      writeJson(path.join(reportsDir, `${planSlug}.json`), nextPlan);
      writeMd(
        path.join(reportsDir, `${planSlug}.md`),
        `# ${nextPlan.batch} Plan\n\n- Remaining eligible: **${nextPlan.remaining_eligible}**\n- Recommended max inserts: **${nextPlan.recommended_max_inserts}**\n- Priority: ${nextPlan.recommended_priority}\n- Objective: \`${nextPlan.objective}\`\n`
      );
    }
  }

  if (inventory) {
    writeJson(path.join(reportsDir, "full-cala-15k-source-inventory.json"), inventory);
    writeMd(path.join(reportsDir, "full-cala-15k-source-inventory.md"), renderInventoryMd(inventory));
  }
  if (universeOut) {
    writeJson(path.join(reportsDir, "full-cala-15k-candidate-universe.json"), universeOut);
  }
  if (dedupeOut) {
    writeJson(path.join(reportsDir, "full-cala-15k-dedupe-report.json"), dedupeOut);
  }
  if (insertPlanOut) {
    writeJson(path.join(reportsDir, "full-cala-15k-insert-plan.json"), insertPlanOut);
    writeMd(
      path.join(reportsDir, "full-cala-15k-insert-plan.md"),
      `# Full CALA 15K Insert Plan

Generated: ${insertPlanOut.generated_at}
Dry run: ${insertPlanOut.dry_run}

- Total eligible: **${insertPlanOut.total_eligible}**
- This batch planned: **${insertPlanOut.batch_planned}**
- First batch country: **${insertPlanOut.first_batch_country}**

## Country order
${(insertPlanOut.country_order || []).map((c) => `- ${c}`).join("\n")}

## Sample
${(insertPlanOut.sample || [])
  .map((s) => `- ${s.country}: ${s.property_name} (${s.match_class} / ${s.source_type})`)
  .join("\n")}
`
    );
  }
}

/**
 * Read-only: inventory remaining CALA shell stock + recommend next batch preflight.
 * NEVER writes to Airtable.
 */
export async function runFullCala15kNextShellBatchPreflightV1(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const generated_at = new Date().toISOString();
  const maxInserts = Number(opts.maxInserts || opts.args?.maxInserts || 500) || 500;

  const COMPLETED_SHELL_INSERTS = Object.freeze({
    "Dominican Republic": 416,
    "Costa Rica": 500,
    Panama: 280,
    Colombia: 500,
    Mexico: 1265,
  });

  let token;
  let baseId;
  try {
    token = resolvePat();
    const base = resolveTargetBase();
    baseId = base?.target_base_id || base?.baseId || env.AIRTABLE_BASE_ID_ALT;
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
  } catch (err) {
    return {
      ok: false,
      status:
        "production_census_full_cala_15k_shell_insert_v1_next_batch_preflight_blocked",
      reason: String(err?.message || err).slice(0, 300),
      inserts_applied: 0,
      dry_run: true,
      generated_at,
    };
  }

  log("[full-cala-15k-preflight] loading candidates…");
  const universe = loadMasterUniverseCandidates();
  const hbx = loadHbxCandidates();
  const { merged, hbxLinked, hbxAdded } = mergeCandidateUniverses(universe, hbx);

  log("[full-cala-15k-preflight] indexing live Hotel Property Census (read-only)…");
  const index = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);

  const classified = [];
  const by_match_class = {};
  for (const c of merged) {
    c.source_count = c.source_count || 1;
    c.merged_sources = c.merged_sources || [c.source_type];
    const cls = classifyAgainstCensus(c, index);
    const row = { ...c, ...cls };
    classified.push(row);
    by_match_class[cls.match_class] = (by_match_class[cls.match_class] || 0) + 1;
  }

  const insertableClasses = new Set([MATCH.NEW_HIGH, MATCH.NEW_MEDIUM]);
  const potentialNew = classified.filter((c) => insertableClasses.has(c.match_class));

  const byCountry = {};
  const ensureCountry = (country) => {
    const key = country || "UNK";
    if (!byCountry[key]) {
      byCountry[key] = {
        country: key,
        total_source_candidates: 0,
        existing_match_high: 0,
        existing_match_medium: 0,
        probable_duplicate_hold: 0,
        possible_duplicate_review: 0,
        potential_new_shell_candidates: 0,
        hbx_only: 0,
        cvent_plus_hbx: 0,
        cvent_only: 0,
        independent_or_other: 0,
        with_structured_city: 0,
        with_structured_address: 0,
        with_website: 0,
        with_phone: 0,
        with_candidate_brand: 0,
        safe_shell_insert: 0,
        shell_insert_with_review: 0,
        hold_weak_identity: 0,
        non_hotel_or_invalid: 0,
        insufficient_data_hold: 0,
        estimated_safely_insertable: 0,
        completed_shell_inserts: COMPLETED_SHELL_INSERTS[key] || 0,
        mexico_hold_only: key === "Mexico",
      };
    }
    return byCountry[key];
  };

  for (const c of classified) {
    const row = ensureCountry(c.country);
    row.total_source_candidates += 1;
    if (c.match_class === MATCH.EXISTING_HIGH) row.existing_match_high += 1;
    if (c.match_class === MATCH.EXISTING_MEDIUM) row.existing_match_medium += 1;
    if (c.match_class === MATCH.PROBABLE_DUP) row.probable_duplicate_hold += 1;
    if (c.match_class === MATCH.POSSIBLE_DUP) row.possible_duplicate_review += 1;
    if (c.match_class === MATCH.REJECT_NON_HOTEL) row.non_hotel_or_invalid += 1;

    const hasHbx = Boolean(c.external_ids?.hbx_code);
    const sources = c.merged_sources || [c.source_type];
    const isCvent =
      c.source_type === "cvent_candidate" || sources.includes("cvent_candidate");
    if (hasHbx && !isCvent) row.hbx_only += 1;
    else if (hasHbx && isCvent) row.cvent_plus_hbx += 1;
    else if (isCvent && !hasHbx) row.cvent_only += 1;
    else row.independent_or_other += 1;

    if (!isBlank(c.city)) row.with_structured_city += 1;
    if (!isBlank(c.address)) row.with_structured_address += 1;
    if (domainOf(c.website)) row.with_website += 1;
    if (normPhone(c.phone)) row.with_phone += 1;
    if (c.brand_text || c.chain_text) row.with_candidate_brand += 1;
  }

  for (const c of potentialNew) {
    const row = ensureCountry(c.country);
    row.potential_new_shell_candidates += 1;
    const pf = classifyShellPreflightQuality(c, { cventOnlyQualityGate: true });
    if (pf.class === SHELL_PREFLIGHT_CLASS.SAFE) row.safe_shell_insert += 1;
    else if (pf.class === SHELL_PREFLIGHT_CLASS.REVIEW) row.shell_insert_with_review += 1;
    else if (pf.class === SHELL_PREFLIGHT_CLASS.WEAK) row.hold_weak_identity += 1;
    else if (pf.class === SHELL_PREFLIGHT_CLASS.NON_HOTEL) row.non_hotel_or_invalid += 1;
    else if (pf.class === SHELL_PREFLIGHT_CLASS.INSUFFICIENT) row.insufficient_data_hold += 1;
  }

  for (const row of Object.values(byCountry)) {
    row.estimated_safely_insertable =
      (row.safe_shell_insert || 0) + (row.shell_insert_with_review || 0);
    row.hbx_backed = (row.hbx_only || 0) + (row.cvent_plus_hbx || 0);
    const denom = Math.max(row.potential_new_shell_candidates, 1);
    row.identity_strength_score = Number(
      (
        row.estimated_safely_insertable * 2 +
        row.hbx_backed * 3 +
        (row.with_structured_city / Math.max(row.total_source_candidates, 1)) * 80 +
        (row.with_website / Math.max(row.total_source_candidates, 1)) * 40 -
        (row.hold_weak_identity / denom) * 120
      ).toFixed(1)
    );
  }

  const countryRows = Object.values(byCountry).sort(
    (a, b) => b.estimated_safely_insertable - a.estimated_safely_insertable
  );

  const MAJOR_CALA = new Set([
    "Colombia",
    "Costa Rica",
    "Panama",
    "Dominican Republic",
    "Mexico",
    "Puerto Rico",
    "Jamaica",
    "Guatemala",
    "Honduras",
    "Nicaragua",
    "El Salvador",
    "Belize",
    "Cuba",
    "Bahamas",
    "Trinidad and Tobago",
    "Barbados",
    "Aruba",
    "Curaçao",
    "Cayman Islands",
    "Peru",
    "Ecuador",
    "Chile",
    "Argentina",
    "Uruguay",
    "Paraguay",
    "Bolivia",
    "Venezuela",
    "Brazil",
  ]);

  const selectable = countryRows.filter((r) => {
    if (r.country === "Mexico") return false;
    if (r.estimated_safely_insertable < 25) return false;
    return MAJOR_CALA.has(r.country) || r.estimated_safely_insertable >= 100;
  });

  selectable.sort((a, b) => {
    const aHbxShare =
      a.hbx_backed / Math.max(a.estimated_safely_insertable || a.potential_new_shell_candidates, 1);
    const bHbxShare =
      b.hbx_backed / Math.max(b.estimated_safely_insertable || b.potential_new_shell_candidates, 1);
    if (Math.abs(bHbxShare - aHbxShare) > 0.05) return bHbxShare - aHbxShare;
    if (b.estimated_safely_insertable !== a.estimated_safely_insertable) {
      return b.estimated_safely_insertable - a.estimated_safely_insertable;
    }
    return b.identity_strength_score - a.identity_strength_score;
  });

  const colombiaRow = byCountry.Colombia || null;
  const costaRicaRow = byCountry["Costa Rica"] || null;

  let recommended = selectable[0] || null;
  if (
    colombiaRow &&
    colombiaRow.estimated_safely_insertable >= 100 &&
    (colombiaRow.hbx_backed >= 50 ||
      colombiaRow.estimated_safely_insertable >=
        (recommended?.estimated_safely_insertable || 0) * 0.7)
  ) {
    recommended = colombiaRow;
  }

  const recommendedCountry = recommended?.country || null;
  const batchNumber =
    recommendedCountry === "Colombia" ||
    recommendedCountry === "Costa Rica" ||
    recommendedCountry === "Panama" ||
    recommendedCountry === "Dominican Republic"
      ? 2
      : 1;
  const shellCountryBatch = recommendedCountry
    ? `${recommendedCountry} Batch ${batchNumber}`
    : null;

  let batchPreflight = null;
  let nextAction = "STOP_FOR_FOUNDER_REVIEW";

  if (recommendedCountry) {
    const countryCandidates = potentialNew
      .filter((c) => c.country === recommendedCountry)
      .sort((a, b) => {
        const ap = mexicoBatchSourcePriority(a);
        const bp = mexicoBatchSourcePriority(b);
        if (ap !== bp) return ap - bp;
        const aHigh = a.match_class === MATCH.NEW_HIGH ? 0 : 1;
        const bHigh = b.match_class === MATCH.NEW_HIGH ? 0 : 1;
        if (aHigh !== bHigh) return aHigh - bHigh;
        return String(a.property_name).localeCompare(String(b.property_name));
      });

    const seenHbx = new Set();
    const seenName = new Set();
    let plan_skipped_hbx_dedupe = 0;
    let plan_skipped_name_dedupe = 0;
    const planned = [];
    const classTallies = {
      safe_insert: 0,
      shell_insert_with_review: 0,
      hold_weak_identity: 0,
      existing_match_high: 0,
      duplicate_candidate: 0,
      non_hotel_or_invalid: 0,
      needs_manual_review: 0,
      insufficient_data_hold: 0,
    };

    for (const c of countryCandidates) {
      const pf = classifyShellPreflightQuality(c, { cventOnlyQualityGate: true });
      if (pf.class === SHELL_PREFLIGHT_CLASS.SAFE) classTallies.safe_insert += 1;
      else if (pf.class === SHELL_PREFLIGHT_CLASS.REVIEW)
        classTallies.shell_insert_with_review += 1;
      else if (pf.class === SHELL_PREFLIGHT_CLASS.WEAK)
        classTallies.hold_weak_identity += 1;
      else if (pf.class === SHELL_PREFLIGHT_CLASS.NON_HOTEL)
        classTallies.non_hotel_or_invalid += 1;
      else if (pf.class === SHELL_PREFLIGHT_CLASS.INSUFFICIENT)
        classTallies.insufficient_data_hold += 1;
      else if (pf.class === SHELL_PREFLIGHT_CLASS.PROBABLE_DUP)
        classTallies.duplicate_candidate += 1;
      else classTallies.needs_manual_review += 1;

      if (
        pf.class !== SHELL_PREFLIGHT_CLASS.SAFE &&
        pf.class !== SHELL_PREFLIGHT_CLASS.REVIEW
      ) {
        continue;
      }
      const hbxCode =
        c.external_ids?.hbx_code != null ? Number(c.external_ids.hbx_code) : null;
      if (hbxCode != null && seenHbx.has(hbxCode)) {
        plan_skipped_hbx_dedupe += 1;
        continue;
      }
      const ncKey = `${c.normalized_property_name}|${normName(c.country)}`;
      if (seenName.has(ncKey)) {
        plan_skipped_name_dedupe += 1;
        continue;
      }
      if (hbxCode != null) seenHbx.add(hbxCode);
      seenName.add(ncKey);
      planned.push({
        candidate_id: c.candidate_id,
        property_name: c.property_name,
        country: c.country,
        city: c.city || null,
        match_class: c.match_class,
        source_type: c.source_type,
        hbx_hotel_code: hbxCode,
        is_cvent:
          c.source_type === "cvent_candidate" ||
          (c.merged_sources || []).includes("cvent_candidate"),
        is_hbx: Boolean(hbxCode),
        has_candidate_brand: Boolean(c.brand_text || c.chain_text),
        preflight_class: pf.class,
        preflight_reason: pf.reason,
        source_priority: mexicoBatchSourcePriority(c),
      });
    }

    const batch = planned.slice(0, maxInserts);
    const cventOnly = batch.filter((p) => p.is_cvent && !p.is_hbx).length;
    const hbxOnly = batch.filter((p) => p.is_hbx && !p.is_cvent).length;
    const cventHbx = batch.filter((p) => p.is_cvent && p.is_hbx).length;
    const candidateBrand = batch.filter((p) => p.has_candidate_brand).length;

    batchPreflight = {
      country: recommendedCountry,
      batch_label: shellCountryBatch,
      batch_number: batchNumber,
      candidate_pool_reviewed: countryCandidates.length,
      existing_match_high_in_country:
        byCountry[recommendedCountry]?.existing_match_high || 0,
      within_plan_hbx_dedupe_skips: plan_skipped_hbx_dedupe,
      within_plan_name_dedupe_skips: plan_skipped_name_dedupe,
      classifications: classTallies,
      expected_insert_count: batch.length,
      source_mix: batch.reduce((acc, p) => {
        const k = p.source_type || "unknown";
        acc[k] = (acc[k] || 0) + 1;
        return acc;
      }, {}),
      hbx_only: hbxOnly,
      cvent_plus_hbx: cventHbx,
      cvent_only: cventOnly,
      hbx_hotel_code_count: batch.filter((p) => p.hbx_hotel_code != null).length,
      candidate_brand_count: candidateBrand,
      proposed_current_brand_writes: 0,
      proposed_brand_family_writes: 0,
      preflight_class_mix: batch.reduce((acc, p) => {
        acc[p.preflight_class] = (acc[p.preflight_class] || 0) + 1;
        return acc;
      }, {}),
      source_priority_mix: batch.reduce((acc, p) => {
        const k = String(p.source_priority);
        acc[k] = (acc[k] || 0) + 1;
        return acc;
      }, {}),
      sample: batch.slice(0, 20),
      quality_gate:
        "ENABLE_CVENT_ONLY_QUALITY_GATE=1 — SAFE/REVIEW only; HBX-first priority; Cvent = identity only; no Current Brand / Brand Family; no rooms/coords/media/owner/dates",
    };

    if (batch.length >= 50) nextAction = "APPLY_NEXT_BATCH";
    else if (batch.length > 0) nextAction = "STOP_FOR_FOUNDER_REVIEW";
    else if (selectable.length > 1) nextAction = "SELECT_DIFFERENT_COUNTRY";
    else nextAction = "REMEDIATE_BEFORE_APPLY";
  }

  const mexicoNote = {
    decision: "hold_enrichment_only",
    do_not_weaken_gate: true,
    do_not_insert_weak_cvent_only: true,
    weak_holds: byCountry.Mexico?.hold_weak_identity || 1277,
    insertable_remaining: byCountry.Mexico?.estimated_safely_insertable || 0,
    batch_4_artifact:
      "reports/research-engine-v2/full-cala-15k-mexico-batch-4-plan.json",
  };

  const slugCountry = String(recommendedCountry || "none")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
  const reportSlug = recommendedCountry
    ? `full-cala-15k-census-shell-insert-v1-${slugCountry}-batch-${batchNumber}-preflight`
    : "full-cala-15k-census-shell-insert-v1-next-batch-preflight";

  const status = recommendedCountry
    ? `production_census_full_cala_15k_shell_insert_v1_${slugCountry.replace(
        /-/g,
        "_"
      )}_batch_${batchNumber}_preflight_ready`
    : "production_census_full_cala_15k_shell_insert_v1_next_batch_preflight_blocked";

  const report = {
    ok: Boolean(recommendedCountry && batchPreflight?.expected_insert_count > 0),
    status,
    objective: "full-cala-15k-census-shell-insert-v1-next-batch-preflight",
    generated_at,
    dry_run: true,
    inserts_applied: 0,
    production_writes: false,
    census_before_count: index.count,
    total_candidates: classified.length,
    hbx_linked_into_universe: hbxLinked,
    hbx_only_added: hbxAdded,
    by_match_class,
    completed_shell_inserts: COMPLETED_SHELL_INSERTS,
    cumulative_shell_inserts: Object.values(COMPLETED_SHELL_INSERTS).reduce(
      (a, b) => a + b,
      0
    ),
    mexico_hold: mexicoNote,
    colombia_batch_2_evaluation: colombiaRow
      ? {
          potential_new: colombiaRow.potential_new_shell_candidates,
          estimated_safely_insertable: colombiaRow.estimated_safely_insertable,
          hbx_backed: colombiaRow.hbx_backed,
          cvent_only: colombiaRow.cvent_only,
          with_structured_city: colombiaRow.with_structured_city,
          hold_weak_identity: colombiaRow.hold_weak_identity,
          recommended: recommendedCountry === "Colombia",
        }
      : null,
    costa_rica_batch_2_evaluation: costaRicaRow
      ? {
          potential_new: costaRicaRow.potential_new_shell_candidates,
          estimated_safely_insertable: costaRicaRow.estimated_safely_insertable,
          hbx_backed: costaRicaRow.hbx_backed,
          recommended: recommendedCountry === "Costa Rica",
        }
      : null,
    country_inventory: countryRows,
    recommended_next_country: recommendedCountry,
    recommended_batch_number: batchNumber,
    recommended_batch_label: shellCountryBatch,
    batch_preflight: batchPreflight,
    proposed_current_brand_writes: 0,
    proposed_brand_family_writes: 0,
    provenance_concerns: [
      "Cvent remains candidate identity only — not field-level SoT",
      "Current Brand / Brand Family proposed writes = 0",
      "Mexico weak Cvent-only remains held (no gate weakening)",
    ],
    schema_write_concerns: [
      "No production writes in this preflight",
      "Shell apply (if approved) must keep Census Only / Hold / HR Required",
    ],
    quality_gate_used:
      "cvent_only_quality_gate=SAFE|REVIEW only; HBX-first source priority; skip existing_match_high / probable_dup / weak / non-hotel; within-plan HBX+name dedupe",
    NEXT_RECOMMENDED_ACTION: nextAction,
    report_slug: reportSlug,
  };

  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  writeJson(path.join(reportsDir, `${reportSlug}.json`), report);
  writeMd(path.join(reportsDir, `${reportSlug}.md`), renderNextBatchPreflightMd(report));
  writeJson(
    path.join(reportsDir, "full-cala-15k-census-shell-insert-v1-next-batch-preflight.json"),
    report
  );
  writeMd(
    path.join(reportsDir, "full-cala-15k-census-shell-insert-v1-next-batch-preflight.md"),
    renderNextBatchPreflightMd(report)
  );

  log(
    `[full-cala-15k-preflight] status=${report.status} next=${recommendedCountry || "none"} expected=${batchPreflight?.expected_insert_count || 0} action=${nextAction}`
  );
  return report;
}

function renderNextBatchPreflightMd(report) {
  const bp = report.batch_preflight || {};
  const top = (report.country_inventory || [])
    .slice(0, 20)
    .map(
      (r) =>
        `| ${r.country} | ${r.total_source_candidates} | ${r.existing_match_high} | ${r.potential_new_shell_candidates} | ${r.hbx_only} | ${r.cvent_plus_hbx} | ${r.cvent_only} | ${r.with_structured_city} | ${r.estimated_safely_insertable} | ${r.hold_weak_identity} | ${r.completed_shell_inserts} |`
    )
    .join("\n");
  return `# Full CALA 15K — Next Shell Batch Preflight (NO WRITES)

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Production Census observed:** **${report.census_before_count}**  
**Dry run / no writes:** **true**  
**NEXT_RECOMMENDED_ACTION:** \`${report.NEXT_RECOMMENDED_ACTION}\`

## Recommended next batch
- Country: **${report.recommended_next_country || "—"}**
- Batch: **${report.recommended_batch_label || "—"}**
- Expected inserts: **${bp.expected_insert_count ?? 0}**
- Proposed Current Brand writes: **${report.proposed_current_brand_writes}**
- Proposed Brand Family writes: **${report.proposed_brand_family_writes}**

## Mexico hold (do not weaken)
- Decision: **${report.mexico_hold?.decision}**
- Weak holds: **${report.mexico_hold?.weak_holds}**
- Insertable remaining under gate: **${report.mexico_hold?.insertable_remaining}**
- Artifact: \`${report.mexico_hold?.batch_4_artifact}\`

## Colombia Batch 2 evaluation
\`\`\`json
${JSON.stringify(report.colombia_batch_2_evaluation || {}, null, 2)}
\`\`\`

## Costa Rica Batch 2 evaluation
\`\`\`json
${JSON.stringify(report.costa_rica_batch_2_evaluation || {}, null, 2)}
\`\`\`

## Country inventory (top 20 by insertable)
| Country | Total | Exist High | Potential New | HBX-only | Cvent+HBX | Cvent-only | City | Safe insertable | Weak hold | Completed shells |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
${top}

## Batch preflight detail
- Candidate pool reviewed: **${bp.candidate_pool_reviewed ?? "—"}**
- existing_match_high (country): **${bp.existing_match_high_in_country ?? "—"}**
- Within-plan HBX dedupe skips: **${bp.within_plan_hbx_dedupe_skips ?? 0}**
- Within-plan name dedupe skips: **${bp.within_plan_name_dedupe_skips ?? 0}**
- Classifications: ${JSON.stringify(bp.classifications || {})}
- Source mix: ${JSON.stringify(bp.source_mix || {})}
- HBX-only / Cvent+HBX / Cvent-only: **${bp.hbx_only ?? 0}** / **${bp.cvent_plus_hbx ?? 0}** / **${bp.cvent_only ?? 0}**
- HBX Hotel Codes: **${bp.hbx_hotel_code_count ?? 0}**
- Candidate Brand: **${bp.candidate_brand_count ?? 0}**
- Quality gate: ${bp.quality_gate || report.quality_gate_used}

## Policy
- Cvent = candidate identity only
- No Rooms/Keys, coords, images, descriptions, facilities, owner/operator/dates, Recent Momentum
- No Current Brand / Brand Family from unvalidated / Cvent-only data
- Shells remain Census Only / Hold / HR Required if later applied
`;
}

/** Approved Colombia Batch 2 preflight fingerprint (SAFE-only, HBX-first). */
export const COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT = Object.freeze({
  expected_insert_count: 293,
  hbx_only: 236,
  cvent_plus_hbx: 57,
  cvent_only: 0,
  first_candidate_id: "hbx_810924",
  approved_census_before: 5522,
  batch_label: "Colombia Batch 2",
});

/**
 * Rebuild Colombia SAFE-only allowlist (same algorithm as next-batch preflight).
 * Does not include REVIEW or weak holds.
 */
export function buildColombiaBatch2SafeAllowlist(potentialNewColombia) {
  const countryCandidates = [...potentialNewColombia].sort((a, b) => {
    const ap = mexicoBatchSourcePriority(a);
    const bp = mexicoBatchSourcePriority(b);
    if (ap !== bp) return ap - bp;
    const aHigh = a.match_class === MATCH.NEW_HIGH ? 0 : 1;
    const bHigh = b.match_class === MATCH.NEW_HIGH ? 0 : 1;
    if (aHigh !== bHigh) return aHigh - bHigh;
    return String(a.property_name).localeCompare(String(b.property_name));
  });

  const seenHbx = new Set();
  const seenName = new Set();
  let plan_skipped_hbx_dedupe = 0;
  let plan_skipped_name_dedupe = 0;
  const planned = [];

  for (const c of countryCandidates) {
    const pf = classifyShellPreflightQuality(c, { cventOnlyQualityGate: true });
    // Colombia Batch 2: SAFE ONLY (not REVIEW)
    if (pf.class !== SHELL_PREFLIGHT_CLASS.SAFE) continue;
    const hbxCode =
      c.external_ids?.hbx_code != null ? Number(c.external_ids.hbx_code) : null;
    if (hbxCode == null) continue; // must have HBX Hotel Code
    const isCventOnly =
      (c.source_type === "cvent_candidate" ||
        (c.merged_sources || []).includes("cvent_candidate")) &&
      !hbxCode;
    if (isCventOnly) continue;
    if (seenHbx.has(hbxCode)) {
      plan_skipped_hbx_dedupe += 1;
      continue;
    }
    const ncKey = `${c.normalized_property_name}|${normName(c.country)}`;
    if (seenName.has(ncKey)) {
      plan_skipped_name_dedupe += 1;
      continue;
    }
    seenHbx.add(hbxCode);
    seenName.add(ncKey);
    planned.push({
      candidate: c,
      candidate_id: c.candidate_id,
      property_name: c.property_name,
      country: c.country,
      city: c.city || null,
      match_class: c.match_class,
      source_type: c.source_type,
      hbx_hotel_code: hbxCode,
      is_cvent:
        c.source_type === "cvent_candidate" ||
        (c.merged_sources || []).includes("cvent_candidate"),
      is_hbx: true,
      has_candidate_brand: Boolean(c.brand_text || c.chain_text),
      preflight_class: pf.class,
      preflight_reason: pf.reason,
      source_priority: mexicoBatchSourcePriority(c),
    });
  }

  return {
    planned,
    plan_skipped_hbx_dedupe,
    plan_skipped_name_dedupe,
    pool_reviewed: countryCandidates.length,
  };
}

/**
 * Allowlist-bound Colombia Batch 2 apply — SAFE only, no substitutions.
 */
export async function runFullCala15kColombiaBatch2ApplyV1(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const generated_at = new Date().toISOString();
  const fp = COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT;
  const enableWrites = Boolean(opts.enableProductionWrites);

  let token;
  let baseId;
  try {
    token = resolvePat();
    const base = resolveTargetBase();
    baseId = base?.target_base_id || base?.baseId || env.AIRTABLE_BASE_ID_ALT;
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
  } catch (err) {
    return {
      ok: false,
      status: FULL_CALA_15K_STATUS.COLOMBIA_BATCH_2_BLOCKED,
      reason: String(err?.message || err).slice(0, 300),
      inserts_applied: 0,
      dry_run: true,
      generated_at,
    };
  }

  log("[colombia-batch-2] loading candidates…");
  const universe = loadMasterUniverseCandidates();
  const hbx = loadHbxCandidates();
  const { merged } = mergeCandidateUniverses(universe, hbx);

  log("[colombia-batch-2] indexing live Hotel Property Census…");
  const index = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
  const census_before_count = index.count;

  const censusApproxOk =
    Math.abs(census_before_count - fp.approved_census_before) <= 25;
  if (!censusApproxOk) {
    const report = {
      ok: false,
      status: FULL_CALA_15K_STATUS.COLOMBIA_BATCH_2_BLOCKED,
      reason: `census_count_drift_before_apply: observed=${census_before_count} expected_approx=${fp.approved_census_before}`,
      census_before_count,
      inserts_applied: 0,
      dry_run: true,
      production_writes: false,
      generated_at,
      NEXT_RECOMMENDED_ACTION: "STOP_FOR_FOUNDER_REVIEW",
    };
    persistColombiaBatch2Report(report);
    return report;
  }

  const insertableClasses = new Set([MATCH.NEW_HIGH, MATCH.NEW_MEDIUM]);
  const classified = [];
  for (const c of merged) {
    c.merged_sources = c.merged_sources || [c.source_type];
    const cls = classifyAgainstCensus(c, index);
    classified.push({ ...c, ...cls });
  }
  const colombiaPotential = classified.filter(
    (c) => c.country === "Colombia" && insertableClasses.has(c.match_class)
  );

  const built = buildColombiaBatch2SafeAllowlist(colombiaPotential);
  const allowlist = built.planned.slice(0, fp.expected_insert_count);
  const hbxOnly = allowlist.filter((p) => p.is_hbx && !p.is_cvent).length;
  const cventHbx = allowlist.filter((p) => p.is_hbx && p.is_cvent).length;
  const cventOnly = allowlist.filter((p) => p.is_cvent && !p.is_hbx).length;

  const fingerprint = {
    count: allowlist.length,
    hbx_only: hbxOnly,
    cvent_plus_hbx: cventHbx,
    cvent_only: cventOnly,
    first_candidate_id: allowlist[0]?.candidate_id || null,
    all_have_hbx: allowlist.every((p) => p.hbx_hotel_code != null),
    all_safe: allowlist.every((p) => p.preflight_class === SHELL_PREFLIGHT_CLASS.SAFE),
  };

  const materialDrift =
    fingerprint.count !== fp.expected_insert_count ||
    fingerprint.hbx_only !== fp.hbx_only ||
    fingerprint.cvent_plus_hbx !== fp.cvent_plus_hbx ||
    fingerprint.cvent_only !== fp.cvent_only ||
    fingerprint.first_candidate_id !== fp.first_candidate_id ||
    !fingerprint.all_have_hbx ||
    !fingerprint.all_safe;

  // Freeze allowlist artifact
  const allowlistPath =
    "reports/research-engine-v2/full-cala-15k-colombia-batch-2-allowlist.json";
  writeJson(path.join(ROOT, allowlistPath), {
    generated_at,
    fingerprint,
    expected: fp,
    material_drift: materialDrift,
    records: allowlist.map((p) => ({
      candidate_id: p.candidate_id,
      property_name: p.property_name,
      hbx_hotel_code: p.hbx_hotel_code,
      source_type: p.source_type,
      preflight_class: p.preflight_class,
      city: p.city,
    })),
  });

  if (materialDrift) {
    const report = {
      ok: false,
      status: FULL_CALA_15K_STATUS.COLOMBIA_BATCH_2_BLOCKED,
      reason: "material_allowlist_drift_vs_approved_preflight",
      census_before_count,
      fingerprint,
      expected: fp,
      inserts_applied: 0,
      dry_run: true,
      production_writes: false,
      allowlist_path: allowlistPath,
      generated_at,
      NEXT_RECOMMENDED_ACTION: "STOP_FOR_FOUNDER_REVIEW",
    };
    persistColombiaBatch2Report(report);
    log(`[colombia-batch-2] BLOCKED material drift ${JSON.stringify(fingerprint)}`);
    return report;
  }

  // Revalidate each allowlist row against live index (no substitutions)
  const schema_missing = [];
  const revalidated = [];
  const revalidation_skips = [];
  for (const row of allowlist) {
    const cls = classifyAgainstCensus(row.candidate, index);
    if (!insertableClasses.has(cls.match_class)) {
      revalidation_skips.push({
        candidate_id: row.candidate_id,
        reason: `match_class_${cls.match_class}`,
      });
      continue;
    }
    const pf = classifyShellPreflightQuality(
      { ...row.candidate, ...cls },
      { cventOnlyQualityGate: true }
    );
    if (pf.class !== SHELL_PREFLIGHT_CLASS.SAFE) {
      revalidation_skips.push({
        candidate_id: row.candidate_id,
        reason: `preflight_${pf.class}`,
      });
      continue;
    }
    if (row.hbx_hotel_code == null) {
      revalidation_skips.push({
        candidate_id: row.candidate_id,
        reason: "missing_hbx_hotel_code",
      });
      continue;
    }
    if (index.byHbx?.has(Number(row.hbx_hotel_code))) {
      revalidation_skips.push({
        candidate_id: row.candidate_id,
        reason: "hbx_now_existing_match_high",
      });
      continue;
    }
    if (row.is_cvent && !row.is_hbx) {
      revalidation_skips.push({
        candidate_id: row.candidate_id,
        reason: "cvent_only_rejected",
      });
      continue;
    }
    const builtFields = buildShellFields(row.candidate, schema_missing, {
      countryBatchLabel: fp.batch_label,
    });
    if (!builtFields.validation.pass) {
      revalidation_skips.push({
        candidate_id: row.candidate_id,
        reason: "shell_validation_failed",
      });
      continue;
    }
    let forbiddenHit = null;
    for (const k of Object.keys(builtFields.fields)) {
      if (FORBIDDEN_SHELL.has(k) || isForbiddenAutopilotField(k)) {
        forbiddenHit = k;
        break;
      }
    }
    if (forbiddenHit) {
      revalidation_skips.push({
        candidate_id: row.candidate_id,
        reason: `forbidden_field_${forbiddenHit}`,
      });
      continue;
    }
    if (
      builtFields.fields["Current Brand"] != null ||
      builtFields.fields["Brand Family"] != null ||
      builtFields.fields["Family / Source Family"] != null
    ) {
      revalidation_skips.push({
        candidate_id: row.candidate_id,
        reason: "brand_fields_present",
      });
      continue;
    }
    if (!builtFields.fields["HBX Hotel Code"]) {
      revalidation_skips.push({
        candidate_id: row.candidate_id,
        reason: "missing_hbx_hotel_code_field",
      });
      continue;
    }
    revalidated.push({
      ...row,
      fields: builtFields.fields,
      meta: builtFields.meta,
    });
  }

  // Final gate checks on revalidated set
  const finalCventOnly = revalidated.filter((p) => p.is_cvent && !p.is_hbx).length;
  const currentBrandWrites = 0;
  const brandFamilyWrites = 0;
  if (finalCventOnly > 0) {
    const report = {
      ok: false,
      status: FULL_CALA_15K_STATUS.COLOMBIA_BATCH_2_BLOCKED,
      reason: "cvent_only_entered_plan",
      census_before_count,
      inserts_applied: 0,
      dry_run: true,
      production_writes: false,
      generated_at,
      NEXT_RECOMMENDED_ACTION: "STOP_FOR_FOUNDER_REVIEW",
    };
    persistColombiaBatch2Report(report);
    return report;
  }

  let inserts_applied = 0;
  let insert_errors = [];
  let created_ids = [];
  const priorCheckpoint = fs.existsSync(CHECKPOINT_FILE)
    ? JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8"))
    : null;
  const checkpoint = {
    version: FULL_CALA_15K_VERSION,
    objective: "full-cala-15k-census-shell-insert-v1-colombia-batch-2",
    updated_at: generated_at,
    dry_run: !enableWrites,
    census_before: census_before_count,
    batches: [...(priorCheckpoint?.batches || [])],
  };

  if (enableWrites && revalidated.length) {
    const batchSize = 100;
    for (let i = 0; i < revalidated.length; i += batchSize) {
      const chunk = revalidated.slice(i, i + batchSize);
      log(
        `[colombia-batch-2] inserting batch ${i / batchSize + 1} size=${chunk.length}`
      );
      const result = await insertBatch(chunk, {
        baseId,
        token,
        tableId: CENSUS_TABLE_ID,
        log,
      });
      inserts_applied += result.inserted;
      insert_errors.push(...result.errors);
      created_ids.push(...result.createdIds.filter(Boolean));
      checkpoint.batches.push({
        at: new Date().toISOString(),
        offset: i,
        planned: chunk.length,
        inserted: result.inserted,
        errors: result.errors.length,
        country: "Colombia",
        shell_country_batch: fp.batch_label,
      });
      writeJson(CHECKPOINT_FILE, checkpoint);
      const errRate = result.errors.length / Math.max(chunk.length, 1);
      if (errRate > 0.25 || result.errors.length >= 10) {
        log("[colombia-batch-2] stopping — error threshold exceeded");
        break;
      }
    }
  } else {
    writeJson(CHECKPOINT_FILE, checkpoint);
  }

  const census_after_estimate = census_before_count + inserts_applied;
  const attempted = revalidated;
  const mixHbxOnly = attempted.filter((p) => p.is_hbx && !p.is_cvent).length;
  const mixCventHbx = attempted.filter((p) => p.is_hbx && p.is_cvent).length;

  const colombiaRemainingNote = {
    approved_allowlist: fp.expected_insert_count,
    revalidated_for_insert: attempted.length,
    inserted: inserts_applied,
    revalidation_skips: revalidation_skips.length,
    hold_weak_identity_unchanged: 303,
    note: "Do not insert Colombia weak holds; next shell = Costa Rica Batch 2 preflight",
  };

  const status =
    enableWrites && inserts_applied > 0
      ? FULL_CALA_15K_STATUS.COLOMBIA_BATCH_2_APPLY_COMPLETE
      : enableWrites
        ? FULL_CALA_15K_STATUS.COLOMBIA_BATCH_2_BLOCKED
        : "production_census_full_cala_15k_shell_insert_v1_colombia_batch_2_preflight_ready";

  const report = {
    ok: enableWrites ? inserts_applied > 0 && insert_errors.length === 0 : true,
    status,
    secondary_statuses: [
      FULL_CALA_15K_STATUS.COLOMBIA_BATCH_2_PARTIAL_SOURCE,
      FULL_CALA_15K_STATUS.PARTIAL_DUP,
    ],
    objective: "full-cala-15k-census-shell-insert-v1-colombia-batch-2",
    generated_at,
    dry_run: !enableWrites,
    production_writes: enableWrites,
    production_table_id: CENSUS_TABLE_ID,
    production_table_name: "Hotel Property Census",
    census_before_count,
    insert_attempted: attempted.length,
    inserts_applied,
    insert_errors: insert_errors.slice(0, 20),
    census_after_estimate,
    census_delta: inserts_applied,
    revalidation_skips_count: revalidation_skips.length,
    revalidation_skips: revalidation_skips.slice(0, 30),
    fingerprint,
    expected: fp,
    source_mix_attempted: {
      hbx_only: mixHbxOnly,
      cvent_plus_hbx: mixCventHbx,
      cvent_only: 0,
    },
    hbx_hotel_codes_in_attempted: attempted.filter((p) => p.hbx_hotel_code != null)
      .length,
    candidate_brand_in_attempted: attempted.filter((p) => p.has_candidate_brand)
      .length,
    current_brand_writes: currentBrandWrites,
    brand_family_writes: brandFamilyWrites,
    forbidden_field_writes: 0,
    owner_facing_or_public_writes: 0,
    prohibited_table_writes: 0,
    created_ids_sample: created_ids.slice(0, 20),
    allowlist_path: allowlistPath,
    colombia_remaining: colombiaRemainingNote,
    next_batch_comparison: {
      costa_rica_batch_2_safe_insertable: 141,
      colombia_weak_holds: 303,
      mexico_weak_holds: 1276,
      mexico_insertable_remaining: 1,
    },
    NEXT_RECOMMENDED_ACTION: "PREFLIGHT_NEXT_COUNTRY_BATCH",
    next_recommended_detail:
      "Preflight Costa Rica Batch 2 next (≈141 SAFE). Do not enrich Mexico/Colombia weak holds yet.",
  };

  persistColombiaBatch2Report(report);
  log(
    `[colombia-batch-2] status=${report.status} inserts=${inserts_applied} before=${census_before_count} after_est=${census_after_estimate}`
  );
  return report;
}

function persistColombiaBatch2Report(report) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const slug = "full-cala-15k-census-shell-insert-v1-colombia-batch-2";
  writeJson(path.join(reportsDir, `${slug}.json`), report);
  writeMd(
    path.join(reportsDir, `${slug}.md`),
    `# Colombia Batch 2 Shell Apply

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Production writes:** ${report.production_writes}  
**Table:** Hotel Property Census (\`${report.production_table_id || CENSUS_TABLE_ID}\`)

## Counts
- Census before: **${report.census_before_count}**
- Insert attempted: **${report.insert_attempted ?? "—"}**
- Inserts applied: **${report.inserts_applied}**
- Census after (estimate): **${report.census_after_estimate ?? "—"}**
- Revalidation skips: **${report.revalidation_skips_count ?? 0}**
- Errors: **${(report.insert_errors || []).length}**

## Source mix (attempted)
${JSON.stringify(report.source_mix_attempted || {}, null, 2)}

## Brand / protected
- Current Brand writes: **${report.current_brand_writes ?? 0}**
- Brand Family writes: **${report.brand_family_writes ?? 0}**
- Forbidden field writes: **${report.forbidden_field_writes ?? 0}**
- Owner-facing/public writes: **${report.owner_facing_or_public_writes ?? 0}**
- Prohibited table writes: **${report.prohibited_table_writes ?? 0}**

## Next
**NEXT_RECOMMENDED_ACTION:** \`${report.NEXT_RECOMMENDED_ACTION || "—"}\`  
${report.next_recommended_detail || ""}

## Drift / block reason
${report.reason || "none"}
`
  );
  writeMd(
    path.join(ROOT, "docs/data-intelligence", `${slug}.md`),
    fs.readFileSync(path.join(reportsDir, `${slug}.md`), "utf8")
  );
}
