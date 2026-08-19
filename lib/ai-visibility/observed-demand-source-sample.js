/**
 * Observed-demand source sample — DataForSEO Keywords + SERP PAA.
 * Not AI monitoring. Not Census. Cost-capped. No Airtable writes.
 */

export const OBSERVED_DEMAND_SOURCE_SAMPLE_VERSION =
  "ai_visibility_observed_demand_source_sample_v1";

export const DEMAND_SIGNAL_TYPES = Object.freeze([
  "SEARCH_QUERY",
  "SEARCH_VOLUME",
  "SEARCH_RANK",
  "PAA",
  "RELATED_SEARCH",
  "AUTOCOMPLETE",
  "PUBLIC_QUESTION",
  "FIRST_PARTY_QUERY",
  "FIRST_PARTY_PRODUCT_BEHAVIOR",
  "OTHER",
]);

export const SEED_CONCEPTS_EN = Object.freeze([
  {
    seed: "hotel brand affiliation",
    intent: "BRAND_SELECTION",
    scenarioId: "scenario_soft_brand_collection_affiliation_v1",
  },
  {
    seed: "best hotel brands for independent hotels",
    intent: "BRAND_SELECTION",
    scenarioId: "scenario_independent_uu_conversion_v1",
  },
  {
    seed: "soft brand hotel",
    intent: "SOFT_BRAND_HARD_BRAND",
    scenarioId: "scenario_soft_brand_collection_affiliation_v1",
  },
  {
    seed: "hotel franchise vs management agreement",
    intent: "FRANCHISE_VS_HMA",
    scenarioId: "scenario_owner_flexibility_control_v1",
  },
  {
    seed: "hotel conversion brand",
    intent: "REFLAGGING_CONVERSION",
    scenarioId: "scenario_independent_uu_conversion_v1",
  },
  {
    seed: "hotel franchise fees",
    intent: "FEES_ECONOMICS",
    scenarioId: "scenario_owner_flexibility_control_v1",
  },
  {
    seed: "hotel brand distribution",
    intent: "DISTRIBUTION_LOYALTY",
    scenarioId: null,
  },
  {
    seed: "hotel loyalty program affiliation",
    intent: "DISTRIBUTION_LOYALTY",
    scenarioId: null,
  },
  {
    seed: "branded residences hotel brands",
    intent: "BRANDED_RESIDENCES",
    scenarioId: "scenario_branded_residences_capability_v1",
  },
  {
    seed: "hotel reflagging",
    intent: "REFLAGGING_CONVERSION",
    scenarioId: "scenario_independent_uu_conversion_v1",
  },
  {
    seed: "hotel management company selection",
    intent: "OPERATOR_SELECTION",
    scenarioId: null,
  },
  {
    seed: "hotel franchise flexibility",
    intent: "OWNER_CONTROL_FLEXIBILITY",
    scenarioId: "scenario_owner_flexibility_control_v1",
  },
  {
    seed: "soft brand vs hard brand hotel",
    intent: "SOFT_BRAND_HARD_BRAND",
    scenarioId: "scenario_soft_brand_collection_affiliation_v1",
  },
  {
    seed: "hotel conversion franchise",
    intent: "CONVERSION",
    scenarioId: "scenario_independent_uu_conversion_v1",
  },
  {
    seed: "new build hotel brand selection",
    intent: "NEW_BUILD",
    scenarioId: "scenario_newbuild_uu_brand_selection_v1",
  },
]);

export const SEED_CONCEPTS_ES = Object.freeze([
  {
    seed: "franquicia hotelera",
    intent: "FRANCHISE_VS_HMA",
    scenarioId: "scenario_owner_flexibility_control_v1",
  },
  {
    seed: "contrato de gestion hotelera",
    intent: "FRANCHISE_VS_HMA",
    scenarioId: "scenario_owner_flexibility_control_v1",
  },
  {
    seed: "marcas hoteleras para hoteles independientes",
    intent: "BRAND_SELECTION",
    scenarioId: "scenario_independent_uu_conversion_v1",
  },
  {
    seed: "marca blanda hotel",
    intent: "SOFT_BRAND_HARD_BRAND",
    scenarioId: "scenario_soft_brand_collection_affiliation_v1",
  },
  {
    seed: "conversion hotelera marca",
    intent: "CONVERSION",
    scenarioId: "scenario_independent_uu_conversion_v1",
  },
  {
    seed: "tarifas franquicia hotelera",
    intent: "FEES_ECONOMICS",
    scenarioId: "scenario_owner_flexibility_control_v1",
  },
]);

export const SERP_SAMPLE_SEEDS = Object.freeze([
  "hotel franchise vs management agreement",
  "best hotel brands for independent hotels",
  "soft brand hotel",
  "hotel franchise fees",
  "branded residences hotel brands",
  "hotel reflagging",
]);

/** Published DataForSEO list prices (pay-as-you-go, fetched 2026-08-17). */
export const DATAFORSEO_LIST_PRICES_USD = Object.freeze({
  keywords_google_ads_search_volume_live: 0.09,
  keywords_google_ads_search_volume_standard: 0.06,
  serp_google_organic_live_page1: 0.002,
  source: "https://dataforseo.com/pricing/keywords-data/google-ads and https://dataforseo.com/pricing/serp/google-organic-serp-api",
  asOf: "2026-08-17",
});

/** Account top-up is funding, not a project budget. */
export const ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET = true;
export const MAX_SOURCE_SAMPLE_COST_USD = 1;
export const MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD = 2;
export const MAX_REFINEMENT_INCREMENTAL_USD = 0.75;
export const DATAFORSEO_BUDGET_APPROVAL_REQUIRED = "DATAFORSEO_BUDGET_APPROVAL_REQUIRED";

/** Already validated in the 2026-08-17 sample. Do not re-query. */
export const VALIDATED_OBSERVED_THEMES_V1 = Object.freeze([
  "hotel franchise fees",
  "soft brand hotel",
  "franquicia hotelera",
  "contrato de gestion hotelera",
  "hotel franchise vs management agreement",
  "hotel reflagging",
]);

/**
 * Smallest distinct-theme probe set. One concept per uncovered territory.
 * Exact strings already tested in the first sample are excluded.
 */
export const REFINEMENT_SEEDS_EN = Object.freeze([
  {
    seed: "hotel affiliation agreement",
    intent: "BRAND_SELECTION",
    scenarioId: "scenario_soft_brand_collection_affiliation_v1",
    territory: "HOTEL_BRAND_AFFILIATION",
  },
  {
    seed: "independent hotel franchise",
    intent: "BRAND_SELECTION",
    scenarioId: "scenario_independent_uu_conversion_v1",
    territory: "INDEPENDENT_HOTEL_BRANDING",
  },
  {
    seed: "convert independent hotel to franchise",
    intent: "REFLAGGING_CONVERSION",
    scenarioId: "scenario_conversion_suitability_v1",
    territory: "HOTEL_CONVERSION",
  },
  {
    seed: "hotel franchise owner control",
    intent: "OWNER_CONTROL_FLEXIBILITY",
    scenarioId: "scenario_owner_flexibility_control_v1",
    territory: "OWNER_CONTROL_FLEXIBILITY",
  },
  {
    seed: "hotel franchise distribution benefits",
    intent: "DISTRIBUTION_LOYALTY",
    scenarioId: "scenario_distribution_loyalty_v1",
    territory: "DISTRIBUTION_LOYALTY",
  },
  {
    seed: "hotel brand selection",
    intent: "NEW_BUILD",
    scenarioId: "scenario_newbuild_uu_brand_selection_v1",
    territory: "NEW_BUILD_BRAND_SELECTION",
  },
  {
    seed: "choosing a hotel management company",
    intent: "OPERATOR_SELECTION",
    scenarioId: null,
    territory: "OPERATOR_SELECTION",
  },
  {
    seed: "hotel branded residences",
    intent: "BRANDED_RESIDENCES",
    scenarioId: "scenario_branded_residences_capability_v1",
    territory: "BRANDED_RESIDENCES",
  },
  {
    seed: "hotel franchise royalties",
    intent: "FEES_ECONOMICS",
    scenarioId: "scenario_owner_economics_v1",
    territory: "ECONOMICS",
  },
  {
    seed: "hotel management agreement fees",
    intent: "FEES_ECONOMICS",
    scenarioId: "scenario_owner_economics_v1",
    territory: "ECONOMICS",
  },
]);

export const REFINEMENT_SEEDS_ES = Object.freeze([
  {
    seed: "afiliacion de marca hotelera",
    intent: "BRAND_SELECTION",
    scenarioId: "scenario_soft_brand_collection_affiliation_v1",
    territory: "HOTEL_BRAND_AFFILIATION",
  },
  {
    seed: "marca hotelera para hotel independiente",
    intent: "BRAND_SELECTION",
    scenarioId: "scenario_independent_uu_conversion_v1",
    territory: "INDEPENDENT_HOTEL_BRANDING",
  },
  {
    seed: "conversion de hotel a franquicia",
    intent: "REFLAGGING_CONVERSION",
    scenarioId: "scenario_conversion_suitability_v1",
    territory: "HOTEL_CONVERSION",
  },
  {
    seed: "coste franquicia hotelera",
    intent: "FEES_ECONOMICS",
    scenarioId: "scenario_owner_economics_v1",
    territory: "ECONOMICS",
  },
  {
    seed: "seleccion de operador hotelero",
    intent: "OPERATOR_SELECTION",
    scenarioId: null,
    territory: "OPERATOR_SELECTION",
  },
  {
    seed: "empresa gestora hotelera",
    intent: "OPERATOR_SELECTION",
    scenarioId: null,
    territory: "OPERATOR_SELECTION",
  },
  {
    seed: "residencias de marca hotelera",
    intent: "BRANDED_RESIDENCES",
    scenarioId: "scenario_branded_residences_capability_v1",
    territory: "BRANDED_RESIDENCES",
  },
]);

export const REFINEMENT_VOLUME_TASKS = 2;
export const REFINEMENT_MAX_SERP_TASKS = 6;

/** Canonical theme keys that collapse near-duplicates. */
export const THEME_DUPLICATE_CANONICAL = Object.freeze({
  "hotel franchise cost": "hotel franchise fees",
  "hotel franchise costs": "hotel franchise fees",
  "cost of hotel franchise": "hotel franchise fees",
  "coste franquicia hotelera": "hotel franchise fees",
  "tarifas franquicia hotelera": "hotel franchise fees",
  "branded residences hotel brand": "hotel branded residences",
  "branded residences hotel brands": "hotel branded residences",
  "residencias de marca hotelera": "hotel branded residences",
  "hotel operator selection": "choosing a hotel management company",
  "hotel management company selection": "choosing a hotel management company",
  "seleccion de operador hotelero": "choosing a hotel management company",
  "empresa gestora hotelera": "choosing a hotel management company",
});

export const CORE_SCENARIO_IDS_FOR_COVERAGE = Object.freeze([
  "scenario_independent_uu_conversion_v1",
  "scenario_newbuild_uu_brand_selection_v1",
  "scenario_soft_brand_collection_affiliation_v1",
  "scenario_owner_flexibility_control_v1",
  "scenario_owner_economics_v1",
  "scenario_distribution_loyalty_v1",
  "scenario_conversion_suitability_v1",
  "scenario_branded_residences_capability_v1",
  "scenario_chainscale_positioning_fit_v1",
  "scenario_market_entry_geographic_relevance_v1",
  "scenario_lifestyle_individuality_positioning_v1",
  "scenario_hma_vs_franchise_v1",
]);

const CONSUMER_NOISE_RE =
  /\b(book(ing)?|cheap hotels?|tripadvisor|hotels\.com|job[s]?|career|salary|hiring|restaurant franchise|redeem|free night|elite status|points club)\b/i;

export function resolveObservedDemandCostCapUsd(envCap) {
  const n = Number(envCap);
  const requested = Number.isFinite(n) && n > 0 ? n : MAX_SOURCE_SAMPLE_COST_USD;
  return Math.min(MAX_SOURCE_SAMPLE_COST_USD, requested);
}

/**
 * Hard stop if this sample or this-phase total would exceed project caps.
 * Does not use account balance as a budget.
 */
export function evaluateDataForSeoBudgetGuard(input = {}) {
  const projectedSampleUsd = Number(input.projectedSampleUsd) || 0;
  const phaseSpentUsd = Number(input.phaseSpentUsd) || 0;
  const sampleCap = MAX_SOURCE_SAMPLE_COST_USD;
  const phaseCap = MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD;
  const projectedPhaseUsd = Number((phaseSpentUsd + projectedSampleUsd).toFixed(4));
  const sampleOver = projectedSampleUsd > sampleCap;
  const phaseOver = projectedPhaseUsd > phaseCap;
  if (sampleOver || phaseOver) {
    return {
      allowed: false,
      code: DATAFORSEO_BUDGET_APPROVAL_REQUIRED,
      projectedSampleUsd,
      phaseSpentUsd,
      projectedPhaseUsd,
      sampleCap,
      phaseCap,
      ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET,
    };
  }
  return {
    allowed: true,
    code: null,
    projectedSampleUsd,
    phaseSpentUsd,
    projectedPhaseUsd,
    sampleCap,
    phaseCap,
    remainingPhaseUsd: Number((phaseCap - projectedPhaseUsd).toFixed(4)),
    ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET,
  };
}

/**
 * Refinement pass: preferred incremental <= $0.75; hard phase total <= $2.00.
 * Does not spend remaining phase budget automatically.
 */
export function evaluateRefinementBudgetGuard(input = {}) {
  const projectedSampleUsd = Number(input.projectedSampleUsd) || 0;
  const phaseSpentUsd = Number(input.phaseSpentUsd) || 0;
  const preferredCap = MAX_REFINEMENT_INCREMENTAL_USD;
  const base = evaluateDataForSeoBudgetGuard(input);
  if (!base.allowed) return { ...base, preferredCap, preferredOver: projectedSampleUsd > preferredCap };
  if (projectedSampleUsd > preferredCap) {
    return {
      allowed: false,
      code: DATAFORSEO_BUDGET_APPROVAL_REQUIRED,
      projectedSampleUsd,
      phaseSpentUsd,
      projectedPhaseUsd: base.projectedPhaseUsd,
      sampleCap: preferredCap,
      phaseCap: MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD,
      preferredCap,
      preferredOver: true,
      ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET,
    };
  }
  return { ...base, preferredCap, preferredOver: false };
}

export function estimateObservedDemandRefinementCost(opts = {}) {
  const volumeTasks = opts.volumeTasks ?? REFINEMENT_VOLUME_TASKS;
  const serpTasks = opts.serpTasks ?? REFINEMENT_MAX_SERP_TASKS;
  const volume = volumeTasks * DATAFORSEO_LIST_PRICES_USD.keywords_google_ads_search_volume_live;
  const serp = serpTasks * DATAFORSEO_LIST_PRICES_USD.serp_google_organic_live_page1;
  const sampleUsd = Number((volume + serp).toFixed(4));
  const budget = evaluateRefinementBudgetGuard({
    projectedSampleUsd: sampleUsd,
    phaseSpentUsd: opts.phaseSpentUsd || 0,
  });
  return {
    SAMPLE_COST_USD: sampleUsd,
    VOLUME_TASKS: volumeTasks,
    SERP_TASKS: serpTasks,
    VOLUME_COST_USD: Number(volume.toFixed(4)),
    SERP_COST_USD: Number(serp.toFixed(4)),
    PREFERRED_INCREMENTAL_CAP_USD: MAX_REFINEMENT_INCREMENTAL_USD,
    PHASE_CAP_USD: MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD,
    PHASE_SPENT_USD: Number(opts.phaseSpentUsd) || 0,
    DATAFORSEO_BUDGET_APPROVAL_REQUIRED: budget.allowed ? false : true,
    budget,
    AI_PROVIDER_CALLS: 0,
    ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET,
  };
}

export function canonicalObservedTheme(theme) {
  const key = String(theme || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/ñ/g, "n")
    .trim();
  return THEME_DUPLICATE_CANONICAL[key] || key;
}

export function classifyCommercialRelevance(row = {}) {
  const vol = row.search_volume;
  const licensedVolume = typeof vol === "number" && vol > 0;
  const blob = [row.queryText, row.normalizedTheme, ...(row.paaQuestions || [])]
    .filter(Boolean)
    .join(" ");
  if (CONSUMER_NOISE_RE.test(String(row.queryText || "")) && !licensedVolume) {
    return { code: "CONSUMER_NOISE", usable: false };
  }
  if (licensedVolume) {
    return { code: "OWNER_DECISION_RELEVANT", usable: true };
  }
  const paaBlob = (row.paaQuestions || []).join(" ");
  const entrepreneurial =
    /(cheapest hotel franchise|franchise can i open|open with \$\d|franchise owner make|most profitable)/i;
  const ownerDecision =
    /(affiliation agreement|management agreement|reflag|conversion|branded residence|soft brand|management company|franchise fee|cost to franchise|operator|loyalty|distribution|gds)/i;
  const paaList = row.paaQuestions || [];
  if (paaList.length) {
    const entHits = paaList.filter((q) => entrepreneurial.test(String(q))).length;
    const ownerHits = paaList.filter((q) => ownerDecision.test(String(q))).length;
    if (entHits >= 2 && ownerHits === 0) {
      return { code: "CONSUMER_NOISE", usable: false, why: "generic_franchise_entrepreneur_paa" };
    }
    const q = String(row.queryText || row.normalizedTheme || "");
    if (/distribution/i.test(q) && !/distribution|loyalty|gds|ota/i.test(paaBlob)) {
      return { code: "CONSUMER_NOISE", usable: false, why: "paa_does_not_support_distribution_theme" };
    }
  }
  if (CONSUMER_NOISE_RE.test(blob) && !licensedVolume) {
    return { code: "CONSUMER_NOISE", usable: false };
  }
  return { code: "OWNER_DECISION_RELEVANT", usable: true };
}

export function classifyRefinementEvidence(row = {}) {
  const vol = row.search_volume;
  const paa = (row.paaQuestions || []).length;
  const related = (row.relatedSearches || []).length;
  if (typeof vol === "number" && vol > 0) return "LICENSED_VOLUME";
  if (paa > 0) return "PAA_SUPPORTED";
  if (related > 0) return "RELATED_SEARCH_SUPPORTED";
  return "NO_SIGNAL";
}

export function estimateObservedDemandSampleCost(opts = {}) {
  const volumeTasks = opts.volumeTasks ?? 3;
  const serpTasks = opts.serpTasks ?? SERP_SAMPLE_SEEDS.length;
  const volume = volumeTasks * DATAFORSEO_LIST_PRICES_USD.keywords_google_ads_search_volume_live;
  const serp = serpTasks * DATAFORSEO_LIST_PRICES_USD.serp_google_organic_live_page1;
  const costCapUsd = resolveObservedDemandCostCapUsd(opts.costCapUsd);
  const sampleUsd = Number((volume + serp).toFixed(4));
  const budget = evaluateDataForSeoBudgetGuard({
    projectedSampleUsd: sampleUsd,
    phaseSpentUsd: opts.phaseSpentUsd || 0,
  });
  return {
    SAMPLE_COST_USD: sampleUsd,
    VOLUME_TASKS: volumeTasks,
    SERP_TASKS: serpTasks,
    VOLUME_COST_USD: Number(volume.toFixed(4)),
    SERP_COST_USD: Number(serp.toFixed(4)),
    COST_CAP_USD: costCapUsd,
    PHASE_CAP_USD: MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD,
    PHASE_SPENT_USD: Number(opts.phaseSpentUsd) || 0,
    DATAFORSEO_BUDGET_APPROVAL_REQUIRED: budget.allowed ? false : true,
    budget,
    AI_PROVIDER_CALLS: 0,
    ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET,
  };
}

export function extractPeopleAlsoAsk(serpItems = []) {
  const out = [];
  for (const item of serpItems) {
    if (item?.type !== "people_also_ask") continue;
    for (const el of item.items || []) {
      const title = String(el.title || "").trim();
      if (title) out.push({ question: title, rank: el.rank_absolute ?? null });
    }
  }
  return out;
}

export function extractRelatedSearches(serpItems = []) {
  const out = [];
  for (const item of serpItems) {
    if (item?.type !== "related_searches") continue;
    for (const el of item.items || []) {
      const title = String(el.title || el.keyword || "").trim();
      if (title) out.push({ query: title, rank: el.rank_absolute ?? el.rank_group ?? null });
    }
  }
  return out;
}

/**
 * Relative HIGH/MEDIUM/LOW within one country+language licensed-volume set.
 * Null volume → UNKNOWN. Zero volume stays measured LOW if others are higher.
 */
export function assignRelativeDemandTiers(rows = []) {
  const measured = rows
    .map((r, i) => ({ i, v: r.search_volume }))
    .filter((x) => typeof x.v === "number" && Number.isFinite(x.v) && x.v > 0)
    .sort((a, b) => b.v - a.v);
  const n = measured.length;
  const out = rows.map((r) => ({
    ...r,
    demandTier: "UNKNOWN",
    demandTierBasis: "UNKNOWN",
    relativeRank: null,
  }));
  if (!n) return out;
  measured.forEach((m, idx) => {
    const rank = idx + 1;
    let tier = "LOW";
    if (n === 1) tier = "MEDIUM";
    else if (rank <= Math.ceil(n / 3)) tier = "HIGH";
    else if (rank <= Math.ceil((2 * n) / 3)) tier = "MEDIUM";
    out[m.i].demandTier = tier;
    out[m.i].demandTierBasis = "LICENSED_SEARCH_VOLUME";
    out[m.i].relativeRank = rank;
  });
  for (const r of out) {
    if (r.search_volume === 0) {
      r.demandTier = "LOW";
      r.demandTierBasis = "LICENSED_SEARCH_VOLUME";
    }
  }
  return out;
}

export function usableAsObserved(row = {}) {
  const vol = row.search_volume;
  const paa = (row.paaQuestions || []).length;
  const related = (row.relatedSearches || []).length;
  if (typeof vol === "number" && vol > 0) return { yes: true, why: "licensed_search_volume_gt_0" };
  if (paa > 0) return { yes: true, why: "paa_questions_present" };
  if (related > 0) return { yes: true, why: "related_searches_present" };
  if (vol === 0) {
    return { yes: false, why: "licensed_volume_zero_and_no_paa_or_related" };
  }
  return { yes: false, why: "no_licensed_volume_or_serp_question_signal" };
}

/** Descriptive source scores — no 0–100 composite. */
export const SOURCE_CANDIDATE_EVALUATION = Object.freeze([
  {
    source: "DataForSEO Google Ads Search Volume",
    OWNER_DECISION_RELEVANCE: "GOOD",
    QUERY_SPECIFICITY: "STRONG",
    GEOGRAPHIC_COVERAGE: "GOOD",
    LANGUAGE_COVERAGE: "GOOD",
    HISTORICAL_DATA: "STRONG",
    VOLUME_SIGNAL: "STRONG",
    REPEATABILITY: "STRONG",
    API_ACCESS: "GOOD",
    LICENSING_SAFETY: "STRONG",
    COST: "GOOD",
    IMPLEMENTATION_EFFORT: "GOOD",
    FRESHNESS: "GOOD",
    notes:
      "Licensed Keyword Planner-compatible volume. Location+language explicit (US, Mexico, etc.). Not owner-identity. CALA is not a Google location — use country locations. Account must have positive balance.",
  },
  {
    source: "DataForSEO Google Organic SERP (PAA / related searches)",
    OWNER_DECISION_RELEVANCE: "GOOD",
    QUERY_SPECIFICITY: "GOOD",
    GEOGRAPHIC_COVERAGE: "GOOD",
    LANGUAGE_COVERAGE: "GOOD",
    HISTORICAL_DATA: "LIMITED",
    VOLUME_SIGNAL: "WEAK",
    REPEATABILITY: "STRONG",
    API_ACCESS: "GOOD",
    LICENSING_SAFETY: "STRONG",
    COST: "STRONG",
    IMPLEMENTATION_EFFORT: "GOOD",
    FRESHNESS: "STRONG",
    notes: "Best governed PAA/related mechanism. Do not scrape Google. Depth 10 live ~$0.002/SERP.",
  },
  {
    source: "Dealality Search Console",
    OWNER_DECISION_RELEVANCE: "LIMITED",
    QUERY_SPECIFICITY: "LIMITED",
    GEOGRAPHIC_COVERAGE: "WEAK",
    LANGUAGE_COVERAGE: "LIMITED",
    HISTORICAL_DATA: "UNKNOWN",
    VOLUME_SIGNAL: "LIMITED",
    REPEATABILITY: "GOOD",
    API_ACCESS: "WEAK",
    LICENSING_SAFETY: "STRONG",
    COST: "STRONG",
    IMPLEMENTATION_EFFORT: "LIMITED",
    FRESHNESS: "UNKNOWN",
    notes:
      "Not connected as an owner-intent query source. Brand-website discoverability SC is a different product. Dealality-branded queries are not market demand.",
  },
  {
    source: "Public hospitality articles / forums",
    OWNER_DECISION_RELEVANCE: "STRONG",
    QUERY_SPECIFICITY: "LIMITED",
    GEOGRAPHIC_COVERAGE: "WEAK",
    LANGUAGE_COVERAGE: "LIMITED",
    HISTORICAL_DATA: "LIMITED",
    VOLUME_SIGNAL: "WEAK",
    REPEATABILITY: "LIMITED",
    API_ACCESS: "WEAK",
    LICENSING_SAFETY: "LIMITED",
    COST: "STRONG",
    IMPLEMENTATION_EFFORT: "LIMITED",
    FRESHNESS: "LIMITED",
    notes: "Establishes topic existence. Not search volume. Do not scrape at scale.",
  },
  {
    source: "Dealality first-party product behavior",
    OWNER_DECISION_RELEVANCE: "UNKNOWN",
    QUERY_SPECIFICITY: "UNKNOWN",
    GEOGRAPHIC_COVERAGE: "UNKNOWN",
    LANGUAGE_COVERAGE: "UNKNOWN",
    HISTORICAL_DATA: "WEAK",
    VOLUME_SIGNAL: "WEAK",
    REPEATABILITY: "UNKNOWN",
    API_ACCESS: "LIMITED",
    LICENSING_SAFETY: "STRONG",
    COST: "STRONG",
    IMPLEMENTATION_EFFORT: "LIMITED",
    FRESHNESS: "WEAK",
    notes:
      "Landing analytics are page/session events, not owner-intent query logs. No Explorer/deal-workflow query demand dataset found.",
  },
  {
    source: "Semrush / Ahrefs / Similarweb",
    OWNER_DECISION_RELEVANCE: "GOOD",
    QUERY_SPECIFICITY: "GOOD",
    GEOGRAPHIC_COVERAGE: "GOOD",
    LANGUAGE_COVERAGE: "GOOD",
    HISTORICAL_DATA: "STRONG",
    VOLUME_SIGNAL: "STRONG",
    REPEATABILITY: "STRONG",
    API_ACCESS: "UNKNOWN",
    LICENSING_SAFETY: "GOOD",
    COST: "LIMITED",
    IMPLEMENTATION_EFFORT: "LIMITED",
    FRESHNESS: "GOOD",
    notes: "Not in repo. Do not buy until DataForSEO sample succeeds.",
  },
]);

