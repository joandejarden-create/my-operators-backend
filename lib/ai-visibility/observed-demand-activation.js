/**
 * Controlled V1 observed-demand activation (no DataForSEO, no monitoring).
 * Origin is orthogonal to scenarioId. Existing SCENARIO prompt IDs stay SCENARIO.
 */

export const OBSERVED_DEMAND_ACTIVATION_VERSION = "ai_visibility_observed_demand_activation_v1";
export const OBSERVED_DEMAND_SEED_V1_VALIDATED = "OBSERVED_DEMAND_SEED_V1_VALIDATED";
export const OBSERVED_PROMPT_MIX_MIN_THEMES_V1 = 8;

export const DEMAND_METHODOLOGY_V1 =
  "Demand tier is relative to comparable observed queries within the source country and language cohort. Licensed Google Ads search volume via DataForSEO. PAA-only themes stay UNKNOWN. United States English and Mexico Spanish are not one absolute scale. Source geography is independent of Brand AI monitoring geography and is not CALA unless the evidence country is a CALA country.";

export const V1_VALIDATED_THEMES = Object.freeze([
  {
    theme: "hotel franchise fees",
    intent: "FEES_ECONOMICS",
    scenarioId: "scenario_owner_economics_v1",
    intentTerritory: "Owner Economics",
    language: "en",
    monitoringCountry: "United States",
    commercialRegion: "North America",
    demandSignalIds: [
      "ds_hotel_franchise_fees_united_states_en",
      "ds_hotel_franchise_fees_mexico_en",
    ],
    demandTier: "HIGH",
    evidenceStrength: "STRONG_OBSERVED",
    originSourceType: "LICENSED_SEO_DATASET",
    samplingPriority: "HIGH",
  },
  {
    theme: "soft brand hotel",
    intent: "SOFT_BRAND_HARD_BRAND",
    scenarioId: "scenario_soft_brand_collection_affiliation_v1",
    intentTerritory: "Collection / Soft Brand",
    language: "en",
    monitoringCountry: "United States",
    commercialRegion: "North America",
    demandSignalIds: [
      "ds_soft_brand_hotel_united_states_en",
      "ds_soft_brand_hotel_mexico_en",
    ],
    demandTier: "MEDIUM",
    evidenceStrength: "STRONG_OBSERVED",
    originSourceType: "LICENSED_SEO_DATASET",
    samplingPriority: "CRITICAL",
  },
  {
    theme: "franquicia hotelera",
    intent: "FRANCHISE_VS_HMA",
    scenarioId: "scenario_hma_vs_franchise_v1",
    intentTerritory: "HMA vs Franchise",
    language: "es",
    monitoringCountry: "Mexico",
    commercialRegion: "CALA",
    demandSignalIds: ["ds_franquicia_hotelera_mexico_es"],
    demandTier: "HIGH",
    evidenceStrength: "STRONG_OBSERVED",
    originSourceType: "LICENSED_SEO_DATASET",
    samplingPriority: "HIGH",
  },
  {
    theme: "contrato de gestion hotelera",
    intent: "FRANCHISE_VS_HMA",
    scenarioId: "scenario_hma_vs_franchise_v1",
    intentTerritory: "HMA vs Franchise",
    language: "es",
    monitoringCountry: "Mexico",
    commercialRegion: "CALA",
    demandSignalIds: ["ds_contrato_de_gestion_hotelera_mexico_es"],
    demandTier: "MEDIUM",
    evidenceStrength: "STRONG_OBSERVED",
    originSourceType: "LICENSED_SEO_DATASET",
    samplingPriority: "STANDARD",
  },
  {
    theme: "hotel franchise vs management agreement",
    intent: "FRANCHISE_VS_HMA",
    scenarioId: "scenario_hma_vs_franchise_v1",
    intentTerritory: "HMA vs Franchise",
    language: "en",
    monitoringCountry: "United States",
    commercialRegion: "North America",
    demandSignalIds: [
      "ds_hotel_franchise_vs_management_agreement_united_states_en",
      "ds_hotel_franchise_vs_management_agreement_mexico_en",
    ],
    demandTier: "UNKNOWN",
    evidenceStrength: "SUPPORTED",
    originSourceType: "PAA",
    samplingPriority: "HIGH",
  },
  {
    theme: "hotel reflagging",
    intent: "REFLAGGING_CONVERSION",
    scenarioId: "scenario_conversion_suitability_v1",
    intentTerritory: "Conversion",
    language: "en",
    monitoringCountry: "United States",
    commercialRegion: "North America",
    demandSignalIds: [
      "ds_hotel_reflagging_united_states_en",
      "ds_hotel_reflagging_mexico_en",
    ],
    demandTier: "UNKNOWN",
    evidenceStrength: "SUPPORTED",
    originSourceType: "PAA",
    samplingPriority: "STANDARD",
  },
  {
    theme: "hotel affiliation agreement",
    intent: "BRAND_SELECTION",
    scenarioId: "scenario_soft_brand_collection_affiliation_v1",
    intentTerritory: "Brand Selection",
    language: "en",
    monitoringCountry: "United States",
    commercialRegion: "North America",
    demandSignalIds: ["ds_hotel_affiliation_agreement_united_states_en"],
    demandTier: "UNKNOWN",
    evidenceStrength: "SUPPORTED",
    originSourceType: "PAA",
    samplingPriority: "STANDARD",
  },
  {
    theme: "convert independent hotel to franchise",
    intent: "REFLAGGING_CONVERSION",
    scenarioId: "scenario_conversion_suitability_v1",
    intentTerritory: "Conversion",
    language: "en",
    monitoringCountry: "United States",
    commercialRegion: "North America",
    demandSignalIds: ["ds_convert_independent_hotel_to_franchise_united_states_en"],
    demandTier: "UNKNOWN",
    evidenceStrength: "SUPPORTED",
    originSourceType: "PAA",
    samplingPriority: "STANDARD",
  },
  {
    theme: "hotel branded residences",
    intent: "BRANDED_RESIDENCES",
    scenarioId: "scenario_branded_residences_capability_v1",
    intentTerritory: "Branded Residences",
    language: "en",
    monitoringCountry: "United States",
    commercialRegion: "North America",
    demandSignalIds: ["ds_hotel_branded_residences_united_states_en"],
    demandTier: "UNKNOWN",
    evidenceStrength: "STRONG_OBSERVED",
    originSourceType: "LICENSED_SEO_DATASET",
    samplingPriority: "CRITICAL",
  },
]);

function slug(theme) {
  return String(theme)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

export function observedPromptId(theme, language) {
  return `p_obs_${slug(theme)}_${language}_v1`;
}

export const DERIVED_PROMPT_SPECS = Object.freeze([
  {
    promptId: "p_obs_hotel_franchise_fees_derived_en_v1",
    parentTheme: "hotel franchise fees",
    parentPromptId: observedPromptId("hotel franchise fees", "en"),
    language: "en",
    promptText:
      "What hotel franchise fees should an owner compare when evaluating competing brand affiliations?",
    whyDerived: "Literal observed query is too terse for a commercially useful owner-affiliation test.",
    intentTerritory: "Owner Economics",
    scenarioId: "scenario_owner_economics_v1",
    ownerIntentFamily: "FEES_ECONOMICS",
    samplingPriority: "HIGH",
  },
  {
    promptId: "p_obs_franquicia_hotelera_derived_es_v1",
    parentTheme: "franquicia hotelera",
    parentPromptId: observedPromptId("franquicia hotelera", "es"),
    language: "es",
    promptText:
      "¿Qué implica una franquicia hotelera para un propietario que compara afiliación de marca frente a un contrato de gestión?",
    whyDerived:
      "Literal Spanish observed query is too terse; derived keeps owner franchise-vs-management meaning without translating an English query.",
    intentTerritory: "HMA vs Franchise",
    scenarioId: "scenario_hma_vs_franchise_v1",
    ownerIntentFamily: "FRANCHISE_VS_HMA",
    samplingPriority: "HIGH",
  },
]);

export const DEDUP_REPORT = Object.freeze({
  EXACT_DUPLICATES: [],
  SEMANTIC_NEAR_DUPLICATES: [
    {
      theme: "soft brand hotel",
      existingPromptFamily: "showcase_collection_soft_affiliation",
      action: "KEEP_SCENARIO_ORIGIN",
      note: "Existing collection/soft-brand scenario prompts remain SCENARIO. New OBSERVED row uses the literal query.",
    },
    {
      theme: "hotel branded residences",
      existingPromptFamily: "branded_residences",
      action: "KEEP_SCENARIO_ORIGIN",
      note: "Existing branded-residences scenario prompts remain SCENARIO.",
    },
    {
      theme: "hotel reflagging",
      existingPromptFamily: "upper_upscale_conversion_brand_selection",
      action: "KEEP_SCENARIO_ORIGIN",
      note: "Conversion scenario prompts remain SCENARIO; observed row is the literal reflagging query.",
    },
    {
      theme: "convert independent hotel to franchise",
      existingPromptFamily: "upper_upscale_conversion_brand_selection",
      action: "KEEP_SCENARIO_ORIGIN",
      note: "Near the conversion scenario family; origin stays distinct.",
    },
    {
      theme: "hotel affiliation agreement",
      existingPromptFamily: "showcase_collection_soft_affiliation",
      action: "KEEP_SCENARIO_ORIGIN",
      note: "Affiliation scenario prompts remain SCENARIO.",
    },
    {
      theme: "contrato de gestion hotelera",
      existingPromptFamily: "hma_vs_franchise operators",
      action: "KEEP_SCENARIO_ORIGIN",
      note: "English HMA operator prompts remain SCENARIO. Spanish observed literal is net-new wording.",
    },
  ],
  REUSED_LINKAGES: [
    "Observed themes map to existing scenarioId values. Historical prompt IDs are unchanged.",
  ],
  NET_NEW_OBSERVED_PROMPTS: 9,
  NET_NEW_DERIVED_PROMPTS: 2,
});

function promptName(theme, language) {
  const lang = language === "es" ? "ES" : "EN";
  return `Observed — ${theme} — ${lang}`;
}

export function buildObservedDemandPromptRows() {
  const observed = V1_VALIDATED_THEMES.map((t) => ({
    promptId: observedPromptId(t.theme, t.language),
    promptName: promptName(t.theme, t.language),
    promptFamily: `observed_${slug(t.theme)}`,
    version: "1",
    language: t.language,
    semanticPairId: null,
    intentTerritory: t.intentTerritory,
    stakeholderRelevance: ["Brand", "Owner"],
    entityScope: "Brand",
    geographyScope: "Country",
    commercialRegion: t.commercialRegion,
    country: t.monitoringCountry,
    countryCode: t.monitoringCountry === "Mexico" ? "MX" : "US",
    developmentType: "Either",
    brandedResidencesRelevance: t.intent === "BRANDED_RESIDENCES",
    active: true,
    monitoringEligible: false,
    cadence: "Paused",
    governanceStatus: "Approved",
    reviewStatus: "Reviewed",
    promptText: t.theme,
    sourceRationale: `Observed-demand V1 seed. Literal query. Source geography retained on evidence rows. Not CALA unless evidence country is CALA. Monitoring eligible off until REPEATED_TESTING_AND_STABILITY.`,
    promptOrigin: "OBSERVED",
    observedTheme: t.theme,
    observedQuery: t.theme,
    scenarioId: t.scenarioId,
  }));

  const derived = DERIVED_PROMPT_SPECS.map((d) => {
    const parent = V1_VALIDATED_THEMES.find((t) => t.theme === d.parentTheme);
    return {
      promptId: d.promptId,
      promptName: promptName(`${d.parentTheme} (derived)`, d.language),
      promptFamily: `observed_${slug(d.parentTheme)}_derived`,
      version: "1",
      language: d.language,
      semanticPairId: null,
      intentTerritory: d.intentTerritory,
      stakeholderRelevance: ["Brand", "Owner"],
      entityScope: "Brand",
      geographyScope: "Country",
      commercialRegion: parent.commercialRegion,
      country: parent.monitoringCountry,
      countryCode: parent.monitoringCountry === "Mexico" ? "MX" : "US",
      developmentType: "Either",
      brandedResidencesRelevance: false,
      active: true,
      monitoringEligible: false,
      cadence: "Paused",
      governanceStatus: "Approved",
      reviewStatus: "Reviewed",
      promptText: d.promptText,
      sourceRationale: `Derived from observed theme "${d.parentTheme}". ${d.whyDerived} Monitoring eligible off.`,
      promptOrigin: "DERIVED",
      observedTheme: d.parentTheme,
      derivedFromObservedPromptId: d.parentPromptId,
      scenarioId: d.scenarioId,
    };
  });

  return [...observed, ...derived];
}

export function evidenceFromSignal(sig) {
  if (!sig) return null;
  return {
    demandSignalId: sig.demandSignalId,
    sourceType: sig.sourceType,
    sourceName: sig.sourceName || "DataForSEO",
    queryText: sig.queryText,
    evidenceReference: sig.evidenceReference,
    sourceConfidence: sig.sourceEvidenceStrength || sig.sourceConfidence || "UNKNOWN",
    originSourceNamespace: "PROMPT_ORIGIN_SOURCE",
    geography: sig.geography,
    language: sig.language,
    dateObserved: sig.dateObserved,
    metricType: sig.metricType,
    metricValue: sig.metricValue,
    demandTier: sig.demandTier,
    demandTierBasis: sig.demandTierBasis,
  };
}

export function buildObservedDemandOverlayClassifications(signalRegistry) {
  const byId = signalRegistry?.byId || new Map((signalRegistry?.signals || []).map((s) => [s.demandSignalId, s]));
  const observed = V1_VALIDATED_THEMES.map((t) => {
    const evidence = t.demandSignalIds.map((id) => evidenceFromSignal(byId.get(id))).filter(Boolean);
    const geos = [...new Set(evidence.map((e) => `${e.geography}/${e.language}`))];
    return {
      promptId: observedPromptId(t.theme, t.language),
      promptOrigin: "OBSERVED",
      originSourceType: t.originSourceType,
      originSourceName: "DataForSEO",
      originSourceReference: t.demandSignalIds[0],
      observedQuery: t.theme,
      observedTheme: t.theme,
      demandTier: t.demandTier,
      demandSignalType: t.originSourceType === "PAA" ? "PAA" : "SEARCH_VOLUME",
      demandGeography: geos.join("; "),
      dateObserved: "2026-08-17",
      demandMethodology: t.demandTier === "UNKNOWN" ? null : DEMAND_METHODOLOGY_V1,
      demandSignalIds: t.demandSignalIds,
      demandEvidence: evidence,
      scenarioId: t.scenarioId,
      ownerIntentFamily: t.intent,
      provenanceStatus: "VALIDATED",
      createdByMethod: "OBSERVED_DEMAND",
      samplingPriority: t.samplingPriority,
      lastProvenanceReviewAt: "2026-08-17",
    };
  });
  const derived = DERIVED_PROMPT_SPECS.map((d) => {
    const parent = V1_VALIDATED_THEMES.find((t) => t.theme === d.parentTheme);
    return {
      promptId: d.promptId,
      promptOrigin: "DERIVED",
      originSourceType: parent.originSourceType,
      originSourceName: "DataForSEO",
      originSourceReference: parent.demandSignalIds[0],
      observedQuery: d.parentTheme,
      observedTheme: d.parentTheme,
      demandTier: "UNKNOWN",
      demandGeography: parent.monitoringCountry,
      dateObserved: "2026-08-17",
      derivedFromObservedPromptId: d.parentPromptId,
      derivedFromDemandSignalId: parent.demandSignalIds[0],
      scenarioId: d.scenarioId,
      ownerIntentFamily: d.ownerIntentFamily,
      provenanceStatus: "VALIDATED",
      createdByMethod: "DERIVED_FROM_OBSERVED",
      samplingPriority: d.samplingPriority,
      provenanceNotes: d.whyDerived,
      lastProvenanceReviewAt: "2026-08-17",
    };
  });
  return [...observed, ...derived];
}

export function evaluateV1ActivationGate(input = {}) {
  const themes = Number(input.distinctThemes) || 0;
  const intents = Number(input.ownerIntentFamilies) || 0;
  const cohorts = Number(input.geoLanguageCohorts) || 0;
  const quality = input.provenanceQuality === "PASS";
  const dup = input.noDuplicateInflation === "PASS";
  const pass =
    themes >= OBSERVED_PROMPT_MIX_MIN_THEMES_V1 &&
    intents >= 3 &&
    cohorts >= 2 &&
    quality &&
    dup;
  return {
    MIN_DISTINCT_VALIDATED_THEMES: themes >= OBSERVED_PROMPT_MIX_MIN_THEMES_V1 ? "PASS" : "FAIL",
    MIN_OWNER_INTENT_FAMILIES: intents >= 3 ? "PASS" : "FAIL",
    MIN_GEO_LANGUAGE_COHORTS: cohorts >= 2 ? "PASS" : "FAIL",
    PROVENANCE_QUALITY: quality ? "PASS" : "FAIL",
    NO_DUPLICATE_INFLATION: dup ? "PASS" : "FAIL",
    pass,
  };
}
