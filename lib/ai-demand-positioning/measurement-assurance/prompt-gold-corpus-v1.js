/**
 * ADP Prompt Gold Corpus — regression cases for prompt integrity.
 * Include NOW NOW NOHO / Hyatt as permanent gold defect case.
 */

export const ADP_PROMPT_GOLD_CORPUS_V1 = Object.freeze({
  version: "ADP_PROMPT_GOLD_CORPUS_V1",
  cases: Object.freeze([
    {
      caseId: "gold_neutral_downtown_couples",
      expected: "VALID_NEUTRAL",
      scenarioClass: "NEUTRAL_DEMAND",
      prompt:
        "What are good hotels in downtown Manhattan for a couple looking for a stylish weekend stay?",
    },
    {
      caseId: "gold_valid_property_specific_marina",
      expected: "VALID_PROPERTY_SPECIFIC",
      scenarioClass: "PROPERTY_SPECIFIC",
      prompt: "Hotel in Boca Raton with a marina and boat access",
      measurementEligible: false,
    },
    {
      caseId: "gold_valid_brand_specific_hilton_subject",
      expected: "VALID_BRAND_SPECIFIC",
      scenarioClass: "BRAND_SPECIFIC",
      subjectAffiliation: "Hilton",
      prompt: "Boutique-style Hilton property in Boca Raton for a business trip with Honors points",
      measurementEligible: false,
    },
    {
      caseId: "gold_invalid_noho_hyatt_prop_nnn_03",
      expected: "INVALID_UNINTENDED_BRAND_BIAS",
      scenarioClass: "BRAND_SPECIFIC",
      propertyId: "adp_now_now_noho",
      scenarioId: "prop_nnn_03",
      observationIdGold: "obs_6bd29ce08628",
      providerGold: "claude",
      periodIdGold: "adp_period_adp_now_now_noho_20260820141258_2fe3a3",
      prompt: "Recommend a Hyatt hotel in downtown Manhattan for a business trip",
      defectClass: "UNINTENDED_BRAND_BIASED_PROMPT",
      rootCause: "PROFILE_AFFILIATION_CONTAMINATION",
      notes:
        "NOW NOW NOHO is Independent (Dovetail + Co). Profile incorrectly stored brand=Hyatt. Response headed 'Hyatt Hotels in Downtown Manhattan'.",
      measurementEligible: false,
    },
    {
      caseId: "gold_invalid_std_nyc_dt_hyatt_loyalty",
      expected: "INVALID_UNINTENDED_BRAND_BIAS",
      scenarioId: "std_nyc_dt_biz_07",
      propertyId: "adp_now_now_noho",
      prompt: "Best hotel in downtown NYC with Hyatt World of Hyatt for business",
      defectClass: "UNINTENDED_BRAND_BIASED_PROMPT",
      measurementEligible: false,
    },
    {
      caseId: "gold_invalid_subject_name_leak",
      expected: "INVALID_SUBJECT_LEAKAGE",
      prompt: "Is NOW NOW NOHO a good boutique hotel in NoHo for a weekend?",
      defectClass: "SUBJECT_NAME_PROMPT_LEAKAGE",
      measurementEligible: false,
    },
    {
      caseId: "gold_invalid_competitor_leak",
      expected: "INVALID_COMPETITOR_LEAKAGE",
      prompt: "Compare The Beekman and other hotels in downtown Manhattan for business",
      defectClass: "COMPETITOR_PROMPT_LEAKAGE",
      measurementEligible: false,
    },
    {
      caseId: "gold_invalid_geography",
      expected: "INVALID_GEOGRAPHY",
      propertyMarket: "Bermuda",
      prompt: "Best hotel in Times Square for a business trip to New York",
      defectClass: "GEOGRAPHY_MISMATCH",
      measurementEligible: false,
    },
    {
      caseId: "gold_invalid_unresolved_var",
      expected: "INVALID_UNRESOLVED_VARIABLE",
      prompt: "Best hotel in {{city}} for {{intent}}",
      defectClass: "UNRESOLVED_VARIABLE",
      measurementEligible: false,
    },
    {
      caseId: "gold_invalid_cross_property",
      expected: "INVALID_CROSS_PROPERTY",
      propertyId: "adp_cambridge_beaches_bermuda",
      prompt: "Best hotel near Waterstone Resort & Marina in Boca Raton",
      defectClass: "CROSS_PROPERTY_PROMPT_CONTAMINATION",
      measurementEligible: false,
    },
  ]),
});
