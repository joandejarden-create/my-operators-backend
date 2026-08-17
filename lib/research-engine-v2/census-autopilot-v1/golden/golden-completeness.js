/**
 * Golden Census Priority Completeness scoring.
 *
 * RAW = supported_applicable / total_applicable
 * MATERIAL WEIGHTED = sum(weight*supported) / sum(weight*applicable)
 *
 * Denominator excludes: NOT_APPLICABLE, SEPARATE MODULE, GOVERNANCE, IMAGE, non-bearing OPTIONAL.
 */

import { GOLDEN_FIELD_REGISTRY, priorityFields } from "./golden-schema.js";
import {
  MEXICO_COASTAL_MARKETS,
  MEXICO_AI_APPLICABLE_MARKETS,
} from "./golden-geography.js";

export const VALUE_STATUS = Object.freeze({
  SUPPORTED: "SUPPORTED",
  UNKNOWN: "UNKNOWN",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  DERIVED: "DERIVED",
});

/**
 * @param {*} v
 */
export function hasSupportedValue(v) {
  if (v == null) return false;
  if (typeof v === "string") {
    const t = v.trim();
    if (!t) return false;
    if (/^(unknown|n\/a|na|null|undefined)$/i.test(t)) return false;
  }
  if (typeof v === "number" && !Number.isFinite(v)) return false;
  return true;
}

/**
 * Resolve applicability for a priority field given hotel context.
 * @param {object} entry registry entry
 * @param {object} ctx { market, family, brand, values }
 */
export function resolveApplicability(entry, ctx = {}) {
  const field = entry.field;
  const market = ctx.market || ctx.Market || null;
  const brand = String(ctx.brand || ctx["Current Brand"] || "").toLowerCase();
  const values = ctx.values || {};

  if (entry.applicability === "REQUIRED") {
    return { applicable: true, reason: "required" };
  }

  if (field === "Ski") {
    return { applicable: false, reason: "ski_not_applicable_mexico_benchmark" };
  }

  if (field === "Beach / Beachfront") {
    if (market && MEXICO_COASTAL_MARKETS.has(market)) {
      return { applicable: true, reason: "coastal_market" };
    }
    return { applicable: false, reason: "non_coastal_market" };
  }

  if (field === "All-Inclusive") {
    if (market && MEXICO_AI_APPLICABLE_MARKETS.has(market)) {
      return { applicable: true, reason: "ai_applicable_market" };
    }
    // Still applicable if brand name implies AI
    if (/all.?inclusive|iberostar|secrets|dreams|hyatt ziva|hyatt zilara/i.test(brand)) {
      return { applicable: true, reason: "brand_implies_ai" };
    }
    return { applicable: false, reason: "ai_not_typical_market" };
  }

  if (field === "Suites") {
    // Applicable only when we have a suite count OR brand is all-suite family
    if (hasSupportedValue(values.Suites) || /homewood|home2|extended|embassy suites|staybridge|candlewood|element/i.test(brand)) {
      return { applicable: true, reason: "suite_disclosure_or_all_suite_brand" };
    }
    return { applicable: false, reason: "suite_count_not_disclosed_in_source_ecosystem" };
  }

  if (field === "All-Suite Flag") {
    if (/homewood|home2|embassy suites|staybridge|candlewood|element/i.test(brand)) {
      return { applicable: true, reason: "all_suite_brand" };
    }
    return { applicable: false, reason: "not_all_suite_brand" };
  }

  if (field === "Asset Context") {
    return { applicable: true, reason: "dealality_classification_expected" };
  }

  if (field === "Restaurant Count") {
    if (hasSupportedValue(values["Restaurant Count"])) {
      return { applicable: true, reason: "defensible_count_present" };
    }
    // Not applicable until defensible — does not punish missing restaurant counts
    return { applicable: false, reason: "no_defensible_restaurant_count" };
  }

  if (field === "Total Meeting Space" || field === "Largest Meeting Room / Ballroom" || field === "Number of Meeting Rooms") {
    // Only applicable when a defensible metric exists — Yes on Meeting Space alone does NOT require sq ft / room counts
    if (hasSupportedValue(values[field])) {
      return { applicable: true, reason: "defensible_meeting_metric_present" };
    }
    return { applicable: false, reason: "meeting_metric_not_disclosed_na" };
  }

  if (field === "Hotel Description - AI Summary") {
    if (hasSupportedValue(values["Hotel Description - Source Text"]) || hasSupportedValue(values["Property Name"])) {
      return { applicable: true, reason: "enough_facts_for_summary" };
    }
    return { applicable: false, reason: "insufficient_facts" };
  }

  if (field === "Official Property ID") {
    if (ctx.family || ctx.property_id || values["Official Property ID"]) {
      return { applicable: true, reason: "brand_directory_property_codes" };
    }
    return { applicable: false, reason: "no_official_property_id_ecosystem" };
  }

  if (entry.applicability === "CONDITIONAL") {
    return { applicable: true, reason: "conditional_default_applicable" };
  }

  // OPTIONAL — not completeness-bearing by default
  return { applicable: false, reason: "optional_excluded_from_priority_denominator" };
}

/**
 * Whether OPTIONAL/CONDITIONAL field is completeness-bearing when applicable.
 * REQUIRED always bearing; CONDITIONAL bearing when applicable; OPTIONAL never (unless flagged).
 */
export function isCompletenessBearingWhenApplicable(entry) {
  if (entry.applicability === "OPTIONAL") return false;
  return entry.track === "PRIORITY";
}

/**
 * Score one hotel's golden field map.
 * @param {Record<string, {value?:*, status?:string, derived?:boolean, source?:string}>} fieldMap
 * @param {object} ctx
 */
export function scoreHotelGoldenCompleteness(fieldMap, ctx = {}) {
  const applicable = [];
  const supported = [];
  const unknown = [];
  const na = [];
  const optional_report = [];
  let weight_total = 0;
  let weight_supported = 0;

  for (const entry of priorityFields()) {
    const cell = fieldMap[entry.field] || {};
    const value = cell.value;
    const app = resolveApplicability(entry, {
      ...ctx,
      values: Object.fromEntries(
        Object.entries(fieldMap).map(([k, v]) => [k, v?.value])
      ),
    });

    if (entry.applicability === "OPTIONAL") {
      optional_report.push({
        field: entry.field,
        supported: hasSupportedValue(value),
        value: hasSupportedValue(value) ? value : null,
      });
    }

    if (!app.applicable || !isCompletenessBearingWhenApplicable(entry)) {
      if (!app.applicable && entry.applicability !== "OPTIONAL") {
        na.push({ field: entry.field, reason: app.reason });
      }
      continue;
    }

    applicable.push(entry.field);
    weight_total += entry.weight;

    const derivedOk = cell.status === VALUE_STATUS.DERIVED && hasSupportedValue(value);
    const supportedOk =
      cell.status === VALUE_STATUS.SUPPORTED ||
      derivedOk ||
      (hasSupportedValue(value) && cell.status !== VALUE_STATUS.UNKNOWN && cell.status !== VALUE_STATUS.NOT_APPLICABLE);

    // Explicit Yes/No for amenity-like counts as supported; Unknown does not
    if (supportedOk) {
      supported.push(entry.field);
      weight_supported += entry.weight;
    } else {
      unknown.push(entry.field);
    }
  }

  const raw =
    applicable.length === 0 ? 0 : Math.round((1000 * supported.length) / applicable.length) / 10;
  const weighted =
    weight_total === 0 ? 0 : Math.round((1000 * weight_supported) / weight_total) / 10;

  return {
    raw_priority_completeness_pct: raw,
    material_weighted_completeness_pct: weighted,
    applicable_count: applicable.length,
    supported_count: supported.length,
    unknown_count: unknown.length,
    not_applicable_count: na.length,
    applicable_fields: applicable,
    supported_fields: supported,
    unknown_fields: unknown,
    not_applicable: na,
    optional_report,
    meets_95: raw >= 95,
    meets_100: raw >= 100,
  };
}

/**
 * Aggregate portfolio scores.
 * @param {object[]} hotelScores
 */
export function aggregatePortfolioScores(hotelScores) {
  const n = hotelScores.length || 1;
  const avgRaw =
    hotelScores.reduce((s, h) => s + (h.raw_priority_completeness_pct || 0), 0) / n;
  const avgW =
    hotelScores.reduce((s, h) => s + (h.material_weighted_completeness_pct || 0), 0) / n;

  const buckets = {
    "100%": 0,
    "95–99.9%": 0,
    "90–94.9%": 0,
    "80–89.9%": 0,
    "<80%": 0,
  };
  for (const h of hotelScores) {
    const p = h.raw_priority_completeness_pct || 0;
    if (p >= 100) buckets["100%"] += 1;
    else if (p >= 95) buckets["95–99.9%"] += 1;
    else if (p >= 90) buckets["90–94.9%"] += 1;
    else if (p >= 80) buckets["80–89.9%"] += 1;
    else buckets["<80%"] += 1;
  }

  const ge95 = buckets["100%"] + buckets["95–99.9%"];

  return {
    hotels: hotelScores.length,
    average_raw_priority_completeness_pct: Math.round(avgRaw * 10) / 10,
    average_material_weighted_completeness_pct: Math.round(avgW * 10) / 10,
    hotels_at_or_above_95_pct: ge95,
    hotels_at_or_above_95_share_pct: Math.round((1000 * ge95) / n) / 10,
    buckets,
  };
}

/**
 * Field-level missingness across hotels.
 */
export function buildFieldMissingness(hotelFieldMaps, contexts) {
  const byField = {};
  for (const entry of priorityFields()) {
    byField[entry.field] = {
      field: entry.field,
      group: entry.group,
      weight: entry.weight,
      weight_band: entry.weight_band,
      applicability: entry.applicability,
      hotels_applicable: 0,
      hotels_supported: 0,
      hotels_unknown: 0,
      hotels_na: 0,
    };
  }

  hotelFieldMaps.forEach((fieldMap, i) => {
    const ctx = contexts[i] || {};
    for (const entry of priorityFields()) {
      const row = byField[entry.field];
      const app = resolveApplicability(entry, {
        ...ctx,
        values: Object.fromEntries(
          Object.entries(fieldMap).map(([k, v]) => [k, v?.value])
        ),
      });
      if (!app.applicable || !isCompletenessBearingWhenApplicable(entry)) {
        row.hotels_na += 1;
        continue;
      }
      row.hotels_applicable += 1;
      const cell = fieldMap[entry.field] || {};
      const ok =
        (cell.status === VALUE_STATUS.SUPPORTED ||
          cell.status === VALUE_STATUS.DERIVED ||
          (hasSupportedValue(cell.value) && cell.status !== VALUE_STATUS.UNKNOWN)) &&
        hasSupportedValue(cell.value);
      if (ok) row.hotels_supported += 1;
      else row.hotels_unknown += 1;
    }
  });

  const ranked = Object.values(byField)
    .map((r) => {
      const miss = r.hotels_applicable - r.hotels_supported;
      const completion_pct =
        r.hotels_applicable === 0
          ? 100
          : Math.round((1000 * r.hotels_supported) / r.hotels_applicable) / 10;
      const impact = miss * r.weight;
      return { ...r, hotels_missing: miss, completion_pct, total_completeness_impact: impact };
    })
    .sort((a, b) => b.total_completeness_impact - a.total_completeness_impact);

  return ranked;
}

export function groupCompletion(hotelFieldMaps, contexts, groupPrefix) {
  const entries = priorityFields().filter((e) => e.group.startsWith(groupPrefix) || e.group === groupPrefix);
  let app = 0;
  let sup = 0;
  hotelFieldMaps.forEach((fm, i) => {
    const ctx = contexts[i] || {};
    for (const entry of entries) {
      const a = resolveApplicability(entry, {
        ...ctx,
        values: Object.fromEntries(Object.entries(fm).map(([k, v]) => [k, v?.value])),
      });
      if (!a.applicable || !isCompletenessBearingWhenApplicable(entry)) continue;
      app += 1;
      const cell = fm[entry.field] || {};
      if (
        hasSupportedValue(cell.value) &&
        cell.status !== VALUE_STATUS.UNKNOWN &&
        cell.status !== VALUE_STATUS.NOT_APPLICABLE
      ) {
        sup += 1;
      }
    }
  });
  return {
    group: groupPrefix,
    applicable_cells: app,
    supported_cells: sup,
    completion_pct: app === 0 ? 100 : Math.round((1000 * sup) / app) / 10,
  };
}

export function separateTrackScore(hotelFieldMaps, track) {
  const entries = GOLDEN_FIELD_REGISTRY.filter((e) => e.track === track);
  let app = 0;
  let sup = 0;
  for (const fm of hotelFieldMaps) {
    for (const entry of entries) {
      app += 1;
      const cell = fm[entry.field] || {};
      if (hasSupportedValue(cell.value)) sup += 1;
    }
  }
  return {
    track,
    applicable_cells: app,
    supported_cells: sup,
    completion_pct: app === 0 ? 0 : Math.round((1000 * sup) / app) / 10,
  };
}
