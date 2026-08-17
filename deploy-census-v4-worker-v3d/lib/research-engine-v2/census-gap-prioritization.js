/**
 * Gap prioritization + source strategy selector for Autopilot Controller v3.
 */

import { FIELD_PRIORITY } from "./census-gap-ledger.js";

export const SOURCE_STRATEGY = Object.freeze({
  OFFICIAL_PARENT_DIRECTORY: "official_parent_directory",
  OFFICIAL_PROPERTY_PAGE: "official_property_page",
  OFFICIAL_JSON_LD: "official_json_ld",
  OFFICIAL_CATALOG_API: "official_catalog_api",
  OFFICIAL_SITEMAP_METADATA: "official_sitemap_metadata",
  OFFICIAL_LINKED_HOTEL_WEBSITE: "official_linked_hotel_website",
  OFFICIAL_FACTSHEET: "official_factsheet",
  OFFICIAL_PRESS_ROOMS_ONLY: "official_press_release_rooms_only",
  MAPBOX_PERMANENT: "mapbox_permanent",
  DETERMINISTIC_CITY_STATE_MAP: "deterministic_city_state_map",
  APPROVED_MARKET_MAP: "approved_market_map",
  PARENT_COMPANY_NORMALIZATION: "parent_company_normalization",
  STEWARD: "steward_review",
});

/** Strategies that Autopilot can execute today (adapters exist). */
export const EXECUTABLE_STRATEGIES = Object.freeze([
  SOURCE_STRATEGY.DETERMINISTIC_CITY_STATE_MAP,
  SOURCE_STRATEGY.APPROVED_MARKET_MAP,
  SOURCE_STRATEGY.OFFICIAL_PARENT_DIRECTORY,
  SOURCE_STRATEGY.OFFICIAL_PROPERTY_PAGE,
  SOURCE_STRATEGY.OFFICIAL_JSON_LD,
  SOURCE_STRATEGY.OFFICIAL_SITEMAP_METADATA,
  SOURCE_STRATEGY.MAPBOX_PERMANENT,
  SOURCE_STRATEGY.PARENT_COMPANY_NORMALIZATION,
  SOURCE_STRATEGY.OFFICIAL_FACTSHEET,
  SOURCE_STRATEGY.OFFICIAL_CATALOG_API,
]);

const ADAPTER_EXISTS = Object.freeze({
  [SOURCE_STRATEGY.DETERMINISTIC_CITY_STATE_MAP]: true,
  [SOURCE_STRATEGY.APPROVED_MARKET_MAP]: true,
  [SOURCE_STRATEGY.OFFICIAL_PARENT_DIRECTORY]: true,
  [SOURCE_STRATEGY.OFFICIAL_PROPERTY_PAGE]: true,
  [SOURCE_STRATEGY.OFFICIAL_JSON_LD]: true,
  [SOURCE_STRATEGY.OFFICIAL_SITEMAP_METADATA]: true,
  [SOURCE_STRATEGY.MAPBOX_PERMANENT]: true,
  [SOURCE_STRATEGY.PARENT_COMPANY_NORMALIZATION]: true,
  [SOURCE_STRATEGY.OFFICIAL_FACTSHEET]: true,
  [SOURCE_STRATEGY.OFFICIAL_CATALOG_API]: true,
  [SOURCE_STRATEGY.OFFICIAL_LINKED_HOTEL_WEBSITE]: false,
  [SOURCE_STRATEGY.OFFICIAL_PRESS_ROOMS_ONLY]: false,
  [SOURCE_STRATEGY.STEWARD]: false,
});

/**
 * Normalize strategy labels from gap classifier into canonical SOURCE_STRATEGY values.
 */
export function normalizeStrategy(raw) {
  const s = String(raw || "").trim();
  if (!s) return SOURCE_STRATEGY.OFFICIAL_PROPERTY_PAGE;
  if (/steward/i.test(s)) return SOURCE_STRATEGY.STEWARD;
  if (/deterministic_city_state|city_state_map/i.test(s)) {
    return SOURCE_STRATEGY.DETERMINISTIC_CITY_STATE_MAP;
  }
  if (/approved_market_map|^approved_market$/i.test(s)) {
    return SOURCE_STRATEGY.APPROVED_MARKET_MAP;
  }
  if (/parent_directory|official_parent/i.test(s)) {
    return SOURCE_STRATEGY.OFFICIAL_PARENT_DIRECTORY;
  }
  if (/json.?ld/i.test(s)) return SOURCE_STRATEGY.OFFICIAL_JSON_LD;
  if (/mapbox/i.test(s)) return SOURCE_STRATEGY.MAPBOX_PERMANENT;
  if (/factsheet|catalog/i.test(s)) return SOURCE_STRATEGY.OFFICIAL_FACTSHEET;
  if (/sitemap/i.test(s)) return SOURCE_STRATEGY.OFFICIAL_SITEMAP_METADATA;
  if (/parent_company/i.test(s)) return SOURCE_STRATEGY.PARENT_COMPANY_NORMALIZATION;
  if (/property_page|official_property/i.test(s)) {
    return SOURCE_STRATEGY.OFFICIAL_PROPERTY_PAGE;
  }
  return s;
}

/**
 * Rank next Autopilot actions from a gap ledger.
 * @param {object} ledger — buildCensusGapLedger result
 * @param {{ limit?: number }} [opts]
 */
export function prioritizeGapActions(ledger, opts = {}) {
  const limit = opts.limit || 10;
  const gaps = ledger.gaps || [];
  /** @type {Map<string, object>} */
  const buckets = new Map();

  for (const g of gaps) {
    if (!g.autopilot_eligible && !String(g.next_best_source_strategy || "").includes("map")) {
      // Still surface steward backlog separately
      if (g.steward_required) {
        const sk = `steward|${g.missing_field}|${g.reason}`;
        const b = buckets.get(sk) || {
          action_key: sk,
          kind: "steward",
          field: g.missing_field,
          strategy: normalizeStrategy(g.next_best_source_strategy),
          reason: g.reason,
          records_affected: 0,
          record_ids: [],
          parents: new Set(),
          countries: new Set(),
          brands: new Set(),
          adapter_exists: false,
          expected_high_yield: 0,
          field_importance: FIELD_PRIORITY[g.missing_field] ?? 99,
        };
        b.records_affected += 1;
        if (b.record_ids.length < 25) b.record_ids.push(g.record_id);
        if (g.brand_family) b.parents.add(g.brand_family);
        if (g.country) b.countries.add(g.country);
        if (g.brand) b.brands.add(g.brand);
        buckets.set(sk, b);
      }
      continue;
    }

    const strategy = normalizeStrategy(g.next_best_source_strategy);
    const key = `${strategy}|${g.missing_field}`;
    const b = buckets.get(key) || {
      action_key: key,
      kind: "autopilot",
      field: g.missing_field,
      strategy,
      reason: g.reason,
      records_affected: 0,
      record_ids: [],
      parents: new Set(),
      countries: new Set(),
      brands: new Set(),
      adapter_exists: Boolean(ADAPTER_EXISTS[strategy]),
      expected_high_yield: 0,
      field_importance: FIELD_PRIORITY[g.missing_field] ?? 99,
      unlocks: [],
    };
    b.records_affected += 1;
    if (b.record_ids.length < 50) b.record_ids.push(g.record_id);
    if (g.brand_family) b.parents.add(g.brand_family);
    if (g.country) b.countries.add(g.country);
    if (g.brand) b.brands.add(g.brand);
    buckets.set(key, b);
  }

  const scored = [...buckets.values()].map((b) => {
    const adapterBoost = b.adapter_exists ? 1.5 : 0.4;
    const yieldRate =
      b.strategy === SOURCE_STRATEGY.DETERMINISTIC_CITY_STATE_MAP ||
      b.strategy === SOURCE_STRATEGY.APPROVED_MARKET_MAP
        ? 0.9
        : b.strategy === SOURCE_STRATEGY.MAPBOX_PERMANENT
          ? 0.7
          : b.strategy === SOURCE_STRATEGY.OFFICIAL_PROPERTY_PAGE ||
              b.strategy === SOURCE_STRATEGY.OFFICIAL_JSON_LD
            ? 0.35
            : 0.2;
    const importanceWeight = Math.max(1, 10 - (b.field_importance || 9));
    const blocksOthers =
      b.field === "Official Property URL" ||
      b.field === "Address" ||
      b.field === "State / Region"
        ? 1.4
        : 1;
    const score =
      b.records_affected * importanceWeight * adapterBoost * yieldRate * blocksOthers;
    const expected = Math.round(b.records_affected * yieldRate);
    /** @type {string[]} */
    const unlocks = [];
    if (b.field === "Official Property URL") {
      unlocks.push("Address", "Phone", "Rooms / Keys");
    }
    if (b.field === "Address") unlocks.push("Latitude", "Longitude");
    if (b.field === "State / Region") unlocks.push("Market");
    return {
      ...b,
      parents: [...b.parents].slice(0, 12),
      countries: [...b.countries].slice(0, 12),
      brands: [...b.brands].slice(0, 12),
      score: Math.round(score * 10) / 10,
      expected_high_yield: expected,
      expected_fields_unlocked: unlocks,
      source_family: b.parents[0] || null,
    };
  });

  scored.sort((a, b) => {
    if (a.kind !== b.kind) return a.kind === "autopilot" ? -1 : 1;
    if (b.score !== a.score) return b.score - a.score;
    return a.field_importance - b.field_importance;
  });

  return {
    top_actions: scored.filter((s) => s.kind === "autopilot").slice(0, limit),
    steward_backlog: scored.filter((s) => s.kind === "steward").slice(0, limit),
    all_scored: scored,
  };
}

/**
 * Select next executable source strategies for a pass.
 * @param {ReturnType<typeof prioritizeGapActions>} ranked
 * @param {{ pass?: number, censusMode?: string }} [opts]
 */
export function selectSourceStrategiesForPass(ranked, opts = {}) {
  const pass = opts.pass || 1;
  const mode = opts.censusMode || "growth";
  const top = ranked.top_actions || [];

  /** @type {string[]} */
  let preferred = [];
  if (pass === 1) {
    preferred = [
      SOURCE_STRATEGY.OFFICIAL_PARENT_DIRECTORY,
      SOURCE_STRATEGY.DETERMINISTIC_CITY_STATE_MAP,
      SOURCE_STRATEGY.APPROVED_MARKET_MAP,
      SOURCE_STRATEGY.PARENT_COMPANY_NORMALIZATION,
    ];
  } else if (pass === 2) {
    preferred = [
      SOURCE_STRATEGY.OFFICIAL_PROPERTY_PAGE,
      SOURCE_STRATEGY.OFFICIAL_JSON_LD,
      SOURCE_STRATEGY.OFFICIAL_FACTSHEET,
      SOURCE_STRATEGY.OFFICIAL_CATALOG_API,
    ];
  } else if (pass === 3) {
    preferred = [SOURCE_STRATEGY.MAPBOX_PERMANENT, SOURCE_STRATEGY.OFFICIAL_PROPERTY_PAGE];
  } else {
    preferred = EXECUTABLE_STRATEGIES;
  }

  if (mode === "governance-only") {
    preferred = [SOURCE_STRATEGY.PARENT_COMPANY_NORMALIZATION];
  }

  const selected = [];
  for (const p of preferred) {
    const hits = top.filter((a) => a.strategy === p && a.adapter_exists);
    if (hits.length) selected.push(...hits);
  }
  // Fill with other high-score executable actions
  for (const a of top) {
    if (!a.adapter_exists) continue;
    if (selected.some((s) => s.action_key === a.action_key)) continue;
    selected.push(a);
    if (selected.length >= 8) break;
  }

  return {
    pass,
    census_mode: mode,
    selected_actions: selected.slice(0, 8),
    deferred_adapter_backlog: top.filter((a) => !a.adapter_exists).slice(0, 10),
    founder_gate_required: false,
  };
}

/**
 * Build executable continuation backlog for the founder report.
 */
export function buildExecutableBacklog(input = {}) {
  const {
    completed = [],
    attempted_insufficient = [],
    ranked = {},
    region = "CALA",
    censusMode = "growth",
  } = input;

  const nextAdapters = (ranked.top_actions || [])
    .filter((a) => !a.adapter_exists)
    .slice(0, 8)
    .map((a) => ({
      field: a.field,
      strategy: a.strategy,
      records_affected: a.records_affected,
      expected_high_yield: a.expected_high_yield,
      parents: a.parents,
      countries: a.countries,
    }));

  const nextBatches = (ranked.top_actions || []).slice(0, 5).map((a) => ({
    field: a.field,
    strategy: a.strategy,
    parent_targets: a.parents,
    country_targets: a.countries,
    expected_yield: a.expected_high_yield,
  }));

  const continueCmd = [
    "ALLOW_CENSUS_AUTOPILOT_APPLY=1 \\",
    "CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \\",
    "CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \\",
    "CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \\",
    `npm run census:autopilot -- --region ${region} --scope official-parent-inventory --mode mission \\`,
    "  --objective full-latam-census-autopilot-v3 \\",
    `  --census-mode ${censusMode} \\`,
    "  --strategy highest-yield-safe \\",
    "  --run-until-complete \\",
    "  --max-passes 10 \\",
    "  --batch-size 100 \\",
    "  --confirm-safe-writes \\",
    "  --confirm-write-to-production-census \\",
    "  --confirm-no-brand-explorer-writes \\",
    "  --confirm-no-owner-operator \\",
    "  --confirm-no-date-writes \\",
    "  --confirm-no-recent-momentum \\",
    "  --confirm-no-company-validation \\",
    "  --confirm-webhound-not-production-source \\",
    "  --enable-production-writes",
  ].join("\n");

  return {
    actions_completed: completed,
    actions_attempted_source_insufficient: attempted_insufficient,
    next_source_adapters_to_build: nextAdapters,
    next_source_families_to_target: [
      ...new Set(nextBatches.flatMap((b) => b.parent_targets || [])),
    ].slice(0, 12),
    next_country_parent_batches: nextBatches,
    expected_yield_summary: nextBatches.reduce((s, b) => s + (b.expected_yield || 0), 0),
    command_to_continue: continueCmd,
    founder_gate_between_passes: false,
  };
}
