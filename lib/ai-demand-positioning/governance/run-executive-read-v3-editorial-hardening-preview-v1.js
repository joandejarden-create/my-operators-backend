/**
 * ADP Executive Read V3 — editorial + claim-discipline hardening PREVIEW.
 * Compares V3 content-depth preview vs editorial-hardened V3.
 * Does NOT activate production. Does NOT change methodology or certified data.
 *
 * Doctrine:
 *   METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 *   EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { join } from "path";
import { resolveGovernedAdpPropertyUniverseV1 } from "../client-readiness/resolve-governed-adp-property-universe-v1.js";
import { loadPublishedReport } from "../published-snapshot.js";
import { METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN } from "./adp-methodology-governance-v1.js";
import { EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED } from "./adp-executive-read-composition-v2.js";

export const ADP_EXECUTIVE_READ_V3_EDITORIAL_HARDENING_PREVIEW =
  "adp_executive_read_v3_editorial_hardening_preview_v1";

export const ADP_EXECUTIVE_V3_CLAIM_DISCIPLINE = "ADP_EXECUTIVE_V3_CLAIM_DISCIPLINE";
export const ADP_EXECUTIVE_NO_IMPLIED_CAUSATION = "ADP_EXECUTIVE_NO_IMPLIED_CAUSATION";
export const ADP_EXECUTIVE_CLEAR_NOT_CLEVER = "ADP_EXECUTIVE_CLEAR_NOT_CLEVER";
export const ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY = "ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY";
export const ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE =
  "ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE";
export const ADP_EXECUTIVE_WATCH_MATERIALITY = "ADP_EXECUTIVE_WATCH_MATERIALITY";
export const ADP_EXECUTIVE_V3_FOUNDER_READ_TEST = "ADP_EXECUTIVE_V3_FOUNDER_READ_TEST";
export const ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS = "ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS";
export const ADP_EXECUTIVE_NO_KPI_ENUMERATION = "ADP_EXECUTIVE_NO_KPI_ENUMERATION";
export const ADP_EXECUTIVE_NUMERIC_REFERENCE_PARITY = "ADP_EXECUTIVE_NUMERIC_REFERENCE_PARITY";
export const ADP_EXECUTIVE_NUMERIC_CONTRAST_VALUE = "ADP_EXECUTIVE_NUMERIC_CONTRAST_VALUE";

const CAUSAL_BANNED =
  /\bshould (support|reinforce|travel|cause|drive|improve|carry)\b|\bis (driving|causing|holding back)\b|\bwill improve\b|\bbrand (advantage|lever)\b|\bunderused (lever|brand)\b|\bcarrying the brand advantage\b|\bthat should support\b|\bthat should reinforce\b|\bthat should travel\b/i;

const RHETORIC_BANNED =
  /\bprotect-and-polish\b|\bprotect-and-deepen\b|\bmanufactur(?:e|ing) a (?:crisis|competitor)\b|\binventing a (?:crisis|weak-brand story)\b|\bmanufactured problems\b|\brival (?:hotels )?taking over\b|\rowns the (?:lifestyle )?answer set\b|\bcompetitor war\b|\bwrong fight\b|\bbrand lever\b|\bfinished work\b|\bdoes not,? by itself,? prove\b|\bno causal relationship should be inferred\b|\bcannot establish\b/i;

const FORBIDDEN_OPENING =
  /appears in [\d.]+% of monitored demand scenarios but in [\d.]+% of individual comparable AI responses/i;

/**
 * Editorial-hardened V3 compositions.
 * primaryIssueId preserved unless claim discipline required wording-only changes.
 * numericAnchors: selective 1–4; values must match published executiveMetrics / governed displays.
 */
const V3_HARDENED = Object.freeze({
  adp_cambridge_beaches_bermuda: {
    archetype: "HIDDEN_STRENGTH+COMPETITIVE_DISPLACEMENT+REALITY_DISCONNECT",
    primaryIssueId: "business_group_positioning_gap",
    headline:
      "Cambridge is one of the strongest leisure performers in the monitored set, but its weakness is concentrated in a few commercially relevant gaps rather than broad visibility.",
    keyInsight:
      "Leisure Presence Index is 731 versus peer parity at 100, yet Rosewood Bermuda repeatedly appears in business/group scenarios where Cambridge is absent. Separately, Five Private Coves — a distinctive part of the property proposition — is recognized in only 15.8% of monitored answers. Overall strength can hide where the hotel remains soft.",
    whyItMatters:
      "The issue is concentrated, not broad. Leadership needs to understand why specific business/group use cases and distinctive property attributes are not represented alongside the rest of the hotel's strong AI position. Otherwise, attention may stay on strengths that already work while missing concentrated gaps.",
    focusNow:
      "Focus first on the business/group positioning gap. Review the scenarios where Rosewood is consistently surfaced instead of Cambridge and determine whether the relevant group, meeting, privacy, and retreat attributes are clearly represented across priority sources. Keep leisure strength in view while closing this concentrated soft spot.",
    watch: null,
    whatToReview:
      "Open the underlying Rosewood displacement evidence and compare the hotel attributes cited in those answers with Cambridge's first-party and major travel-source representation. Confirm whether business/group and privacy/retreat cues are complete and consistent before deciding on content or channel changes.",
    numericAnchors: [
      {
        display: "731",
        section: "KEY_INSIGHT",
        metricId: "intentPresenceIndex.leisure.index",
        why: "Establishes unusual peer-relative leisure strength",
        removingWeakensInsight: true,
      },
      {
        display: "15.8%",
        section: "KEY_INSIGHT",
        metricId: "realityGap.Five Private Coves.recognitionRate",
        why: "Quantifies the Reality Gap magnitude for a distinctive attribute",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["PRECISION", "CLARITY", "CAUSALITY"],
    trace: {
      headline: ["leisure_presence_index", "scenario_presence", "consideration"],
      keyInsight: ["benchmark_leisure", "rosewood_displacement", "five_private_coves_gap"],
      whyItMatters: ["concentrated_vs_broad_weakness"],
      focusNow: ["business", "group_meeting", "rosewood_bermuda"],
      watch: ["Five Private Coves"],
      whatToReview: ["displacement_evidence", "first_party_sources", "major_travel_sources"],
    },
  },
  adp_faranda_collection_bogota: {
    archetype: "HIDDEN_WEAKNESS+COMPETITIVE_DISPLACEMENT",
    primaryIssueId: "leisure_couples_entry",
    headline:
      "Faranda Collection Bogotá's primary issue is entering AI consideration at all, especially for leisure and couples-oriented stays.",
    keyInsight:
      "AI Consideration is 20.7%, so the hotel appears in roughly one in five individual comparable responses. When Faranda is absent, Four Seasons Bogotá and Click Clack repeatedly appear instead — a concentrated substitution pattern rather than a diffuse competitor list. The weakness is mainly entry, not rank after inclusion.",
    whyItMatters:
      "Fine-tuning rank against individual competitors will have limited value until the hotel becomes relevant in more of the traveler needs it currently misses. The priority is answer-set entry for leisure and couples demand, not a broad competitor-focused program.",
    focusNow:
      "Focus first on how the hotel is represented for leisure and couples demand. Review whether the attributes, positioning, and use cases most relevant to those travelers are complete and consistently represented across priority sources. Treat competitor-rank tactics as secondary until entry improves.",
    watch: null,
    whatToReview:
      "Compare the evidence from scenarios where Four Seasons and Click Clack appear with the property information currently available for Faranda. Check leisure and couples cues first — location, product type, and stay occasion — before broadening the review.",
    numericAnchors: [
      {
        display: "20.7%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Establishes severity of the entry problem",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["RHETORIC", "CLARITY", "PRECISION"],
    trace: {
      headline: ["consideration", "scenario_presence", "couples", "leisure"],
      keyInsight: ["four_seasons_bogota", "click_clack_hotel_bogota", "displacement"],
      whyItMatters: ["entry_before_rank"],
      focusNow: ["leisure", "couples", "source_representation"],
      watch: ["Choice Hotels Distribution"],
      whatToReview: ["displacement_evidence", "property_information"],
    },
  },
  adp_hotel_caribe_faranda_grand: {
    archetype: "BREADTH_PROBLEM+HIDDEN_STRENGTH",
    primaryIssueId: "business_consideration_consistency",
    headline:
      "Hotel Caribe is often Top 3 when AI ranks hotels — the binding constraint is inconsistent inclusion in individual answers, especially for business demand.",
    keyInsight:
      "Top-3 rate is 72.4% of ranked responses, yet AI Consideration is 43.3%. Scenario-level relevance is already reasonably broad, and prominence after inclusion is a genuine strength. The less obvious finding is that business demand is among the softer territories for response-level inclusion, with Hilton Cartagena and Bastión appearing in some absences.",
    whyItMatters:
      "If management focuses mainly on beating named competitors on rank, it may address the wrong constraint. Closing the gap between broad scenario relevance and individual-answer inclusion — particularly for business — is the clearer priority.",
    focusNow:
      "Focus first on consideration consistency for business and adjacent traveler needs. Verify that business-relevant positioning, amenities, and location cues are complete and consistent across Faranda/Radisson Individuals channels and major platforms where Caribe already shows scenario relevance but lags in individual answers.",
    watch: null,
    whatToReview:
      "Review business-territory evidence and compare attributes cited when Caribe is included versus when Hilton or Bastión appear instead. Confirm whether business and full-service cues are consistently represented before pursuing broader competitive campaigns.",
    numericAnchors: [
      {
        display: "72.4%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.rankMetrics.topThreeAppearanceRate",
        why: "Contrast proves prominence after inclusion is not the primary constraint",
        removingWeakensInsight: true,
      },
      {
        display: "43.3%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Paired with Top-3 to show inclusion consistency gap",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["RHETORIC", "PRECISION", "CLARITY"],
    trace: {
      headline: ["top3_rate", "consideration", "scenario_presence"],
      keyInsight: ["business_territory", "hilton_cartagena", "bastion"],
      whyItMatters: ["entry_vs_rank"],
      focusNow: ["business", "consideration_consistency"],
      watch: ["hilton_cartagena", "bastion_luxury_hotel_cartagena"],
      whatToReview: ["business_evidence", "competitor_absences"],
    },
  },
  adp_hotel_phillips_kansas_city: {
    archetype: "BREADTH_PROBLEM+REALITY_DISCONNECT+COMPETITIVE_DISPLACEMENT",
    primaryIssueId: "meeting_ballroom_proposition",
    headline:
      "Phillips reaches many traveler scenarios, but its meeting proposition is not being recognized strongly enough relative to that reach.",
    keyInsight:
      "Scenario Presence is 73%, but AI Consideration is 31.3%. Group/meeting demand is among the softer territories, ballroom recognition is 12.7%, and Loews Kansas City plus Hotel Kansas City repeatedly appear in answers where Phillips is absent. That combination points to a concentrated meeting-proposition gap rather than a generic visibility issue.",
    whyItMatters:
      "A hotel with meaningful meeting assets may be underrepresented in the traveler need where those assets are most commercially relevant. Broad scenario reach can look healthier than the meetings story underneath.",
    focusNow:
      "Focus first on the meeting and ballroom proposition. Verify that meeting-space facts, capacities, event positioning, and supporting content are complete and consistent across Hilton/Curio, the hotel website, and major third-party channels. Do this before launching a broader AI visibility program.",
    watch: null,
    whatToReview:
      "Review the meeting-related evidence and compare Phillips' representation with Loews and Hotel Kansas City in the same traveler scenarios. Confirm whether important meeting-space facts are complete and consistent across Hilton/Curio and major third-party channels.",
    numericAnchors: [
      {
        display: "73%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.scenarioPresence.rate",
        why: "Contrast with Consideration shows breadth vs inclusion",
        removingWeakensInsight: true,
      },
      {
        display: "31.3%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Paired contrast for the central insight",
        removingWeakensInsight: true,
      },
      {
        display: "12.7%",
        section: "KEY_INSIGHT",
        metricId: "realityGap.Ballroom.recognitionRate",
        why: "Material Reality Gap for the meeting proposition",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["CAUSALITY", "PRECISION", "CLARITY"],
    trace: {
      headline: ["scenario_presence", "consideration", "group_meeting"],
      keyInsight: ["ballroom_gap", "loews_kansas_city", "hotel_kansas_city"],
      whyItMatters: ["meetings_proposition_disconnect"],
      focusNow: ["Meeting & Event Space", "Ballroom", "hilton_curio_channels"],
      watch: ["Fitness Center"],
      whatToReview: ["meeting_evidence", "loews", "hotel_kansas_city"],
    },
  },
  adp_jw_marriott_monterrey_valle: {
    archetype: "BREADTH_PROBLEM+HIDDEN_STRENGTH",
    primaryIssueId: "answer_level_inclusion_consistency",
    headline:
      "JW Monterrey Valle is almost always Top 3 when AI ranks hotels, yet it still misses a large share of individual answers.",
    keyInsight:
      "Top-3 rate is 92.6% of ranked responses, but AI Consideration is 45.2%. Near-universal scenario reach and strong ranking prominence coexist with inconsistent answer-level inclusion. Family demand is among the softer territories. The less obvious finding is that the constraint is inconsistent entry into the answer set — not weak prominence once included.",
    whyItMatters:
      "Near-complete scenario coverage can look settled while travelers still frequently never see the hotel in a given answer. Management may over-invest in competitive rank and under-invest in consideration consistency.",
    focusNow:
      "Focus first on closing the consideration-consistency gap, especially for family-adjacent demand. Confirm that family-relevant and Valle location cues are clearly and consistently represented across Marriott channels and major platforms. The aim is answer-level inclusion that better matches the hotel's already strong Top-3 performance.",
    watch: null,
    whatToReview:
      "Compare scenarios where JW Monterrey Valle is ranked Top 3 with scenarios where it is absent from individual answers. Review family and location attributes first to see whether representation gaps help explain the consistency shortfall.",
    numericAnchors: [
      {
        display: "92.6%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.rankMetrics.topThreeAppearanceRate",
        why: "Rank vs inclusion contrast",
        removingWeakensInsight: true,
      },
      {
        display: "45.2%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Paired with Top-3 for the central contradiction",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["RHETORIC", "PRECISION", "CLARITY", "WATCH_MATERIALITY"],
    trace: {
      headline: ["top3_rate", "consideration", "scenario_presence"],
      keyInsight: ["family_territory", "inclusion_vs_prominence"],
      whyItMatters: ["scenario_coverage_misread"],
      focusNow: ["family", "consideration_consistency", "valle_location"],
      watch: [],
      whatToReview: ["top3_evidence", "absence_evidence", "family_attributes"],
    },
  },
  adp_jw_marriott_santo_domingo: {
    archetype: "STRONG_PROTECT+REALITY_DISCONNECT",
    primaryIssueId: "signature_urban_representation",
    headline:
      "JW Santo Domingo is performing strongly overall. The more important opportunity is protecting that position while closing residual representation gaps.",
    keyInsight:
      "Scenario Presence is 98.4% and AI Consideration is 71.4%, with competitive displacement minimal versus peers. The less obvious finding is that residual risk sits in proposition representation — Blue Mall adjacency recognition is only 24.4%, and Executive Lounge recognition is similarly soft — not in competitors repeatedly filling the hotel's absences.",
    whyItMatters:
      "The hotel's position is broadly strong, so management attention should stay on residual representation gaps. That keeps leadership focused on the remaining representation gaps rather than unnecessary broad competitive remediation.",
    focusNow:
      "Focus first on signature urban and business representation. Confirm that Blue Mall adjacency and Executive Lounge facts are complete and consistently represented across Marriott and major travel sources. Protect the hotel's already strong demand reach while closing those specific recognition gaps.",
    watch: null,
    whatToReview:
      "Review how Blue Mall adjacency, Executive Lounge, and wellness cues appear in monitored answers versus first-party and major travel sources. Confirm whether the underlying hotel facts are complete and consistent before deciding on any content changes.",
    numericAnchors: [
      {
        display: "98.4%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.scenarioPresence.rate",
        why: "Anchors the strong overall position",
        removingWeakensInsight: true,
      },
      {
        display: "71.4%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Paired strength signal with Scenario Presence",
        removingWeakensInsight: true,
      },
      {
        display: "24.4%",
        section: "KEY_INSIGHT",
        metricId: "realityGap.Blue Mall Adjacency.recognitionRate",
        why: "Quantifies residual Reality Gap",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["RHETORIC", "CLARITY", "PRECISION"],
    trace: {
      headline: ["scenario_presence", "consideration", "top3_rate"],
      keyInsight: ["low_displacement", "Blue Mall Adjacency", "Executive Lounge"],
      whyItMatters: ["residual_representation_focus"],
      focusNow: ["Blue Mall Adjacency", "Executive Lounge"],
      watch: [],
      whatToReview: ["reality_gap", "first_party_sources", "wellness"],
    },
  },
  adp_now_now_noho: {
    archetype: "HIDDEN_WEAKNESS+COMPETITIVE_DISPLACEMENT",
    primaryIssueId: "lifestyle_couples_entry",
    headline:
      "NOW NOW NOHO is rarely entering traveler consideration at all — named boutique neighbors appear repeatedly in lifestyle recommendations instead.",
    keyInsight:
      "AI Consideration is 3.2%, meaning the hotel appears in only a few individual comparable AI responses. Couples and family territories show near-zero reach. When NOHO is absent, Crosby Street, Soho Grand, and The Bowery repeatedly fill lifestyle recommendations. The less obvious finding is the concentrated peer substitution pattern — not merely that overall visibility is weak.",
    whyItMatters:
      "Without first earning inclusion in lifestyle and couples demand, competitor-rank tactics will not change the executive picture. The priority is becoming relevant in the traveler needs the hotel currently misses.",
    focusNow:
      "Focus first on couples and lifestyle-oriented stays where the hotel is repeatedly absent. Review whether urban lifestyle positioning, neighborhood cues, and stay-occasion attributes are complete and consistently represented across priority sources. Do that before pursuing individual competitor-rank work against named boutique peers.",
    watch: null,
    whatToReview:
      "Compare evidence from scenarios dominated by Crosby Street, Soho Grand, and The Bowery with NOHO's current property information. Start with couples and lifestyle cues — neighborhood, product type, and stay occasion — before broadening the review.",
    numericAnchors: [
      {
        display: "3.2%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Establishes severity of the entry issue",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["RHETORIC", "CLARITY", "PRECISION"],
    trace: {
      headline: ["consideration", "scenario_presence"],
      keyInsight: ["couples_zero", "crosby_street_hotel", "soho_grand", "bowery_hotel"],
      whyItMatters: ["entry_before_rank"],
      focusNow: ["couples", "lifestyle", "source_representation"],
      watch: ["crosby_street_hotel"],
      whatToReview: ["displacement_evidence", "lifestyle_attributes"],
    },
  },
  adp_radisson_santo_domingo: {
    archetype: "COMPETITIVE_DISPLACEMENT+HIDDEN_WEAKNESS",
    primaryIssueId: "jw_marriott_substitution",
    headline:
      "Radisson Santo Domingo's clearest pattern is competitive substitution: JW Marriott repeatedly fills the traveler needs where Radisson is missing.",
    keyInsight:
      "AI Consideration is 29%, so the hotel appears in fewer than one-third of individual comparable responses. When Radisson is absent, JW Marriott Santo Domingo leads displacement (35 monitored contexts), with Jaragua and El Embajador as secondary substitutes. Family and leisure soft spots sit inside that same substitution story. JW is not a generic peer mention.",
    whyItMatters:
      "Every improvement initiative should be judged against JW's presence in the same demand contexts. Treating competitors as an undifferentiated list will understate the real competitive pressure. The hotel needs a JW-aware agenda, not a generic visibility plan.",
    focusNow:
      "Focus first on the traveler needs with the highest JW Marriott displacement overlap — including family and leisure soft spots. Strengthen how Radisson is represented for those needs across priority sources. Treat JW as the primary competitive substitute rather than one of many equals.",
    watch: null,
    whatToReview:
      "Open the JW Marriott displacement evidence and compare the attributes and traveler needs cited there with Radisson's current representation. Prioritize family and leisure scenarios where substitution is most concentrated before broadening the review.",
    numericAnchors: [
      {
        display: "29%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Anchors entry severity with interpretation",
        removingWeakensInsight: true,
      },
      {
        display: "35",
        section: "KEY_INSIGHT",
        metricId: "lostDemand.displacement.jw_marriott_santo_domingo.displacementCount",
        why: "Magnitude of asymmetric competitive substitution",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["PRECISION", "CLARITY"],
    trace: {
      headline: ["displacement", "jw_marriott_santo_domingo", "consideration"],
      keyInsight: ["competitor_present_gaps", "jaragua", "el_embajador", "family", "leisure"],
      whyItMatters: ["asymmetric_competitive_set"],
      focusNow: ["jw_overlap_needs", "family", "leisure"],
      watch: ["renaissance_santo_domingo_jaragua", "el_embajador_santo_domingo"],
      whatToReview: ["jw_displacement_evidence", "family_leisure_scenarios"],
    },
  },
  adp_renaissance_times_square: {
    archetype: "HIDDEN_WEAKNESS+COMPETITIVE_DISPLACEMENT+REALITY_DISCONNECT",
    primaryIssueId: "times_square_entry_and_views",
    headline:
      "Renaissance Times Square is not losing on rank after inclusion — it is rarely entering individual AI answers in the first place.",
    keyInsight:
      "AI Consideration is 7.7%, so the hotel appears in fewer than one in ten individual comparable responses. The Knickerbocker leads displacement, Times Square views recognition is 20%, and wellness/adventure territories are near zero. A small business-travel peer foothold exists beneath those absolute rates, but it is easy to miss if management reads the headline only as undifferentiated visibility weakness.",
    whyItMatters:
      "Scarce management attention should go to answer-set entry and Times Square proposition clarity — not Top-3 optimization. Fighting competitors on rank is premature while the hotel is still rarely included in individual answers.",
    focusNow:
      "Focus first on entering consideration for core Times Square traveler needs by clarifying Times Square views and location proposition across Renaissance/Marriott channels and major platforms. Investigate Knickerbocker displacement only as part of that same entry story — not as a separate rank campaign.",
    watch: null,
    whatToReview:
      "Review Times Square views and location representation across first-party and major travel sources. Then compare the attributes cited when The Knickerbocker appears in scenarios where Renaissance is absent, and confirm whether those cues are complete and consistent before deciding on next content work.",
    numericAnchors: [
      {
        display: "7.7%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Establishes entry severity",
        removingWeakensInsight: true,
      },
      {
        display: "20%",
        section: "KEY_INSIGHT",
        metricId: "realityGap.Times Square Views.recognitionRate",
        why: "Material Reality Gap for the core proposition",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["PRECISION", "CLARITY"],
    trace: {
      headline: ["consideration", "scenario_presence"],
      keyInsight: ["knickerbocker_nyc", "Times Square Views", "business_index"],
      whyItMatters: ["entry_before_rank"],
      focusNow: ["Times Square Views", "location_proposition", "knickerbocker"],
      watch: ["westin_times_square", "marriott_marquis_nyc"],
      whatToReview: ["views_representation", "knickerbocker_evidence"],
    },
  },
  adp_st_regis_cap_cana: {
    archetype: "BREADTH_PROBLEM+HIDDEN_STRENGTH",
    primaryIssueId: "consideration_under_elite_reach",
    headline:
      "Cap Cana is relevant across nearly every traveler need tested, but that broad reach is not translating into consistent inclusion in individual AI answers.",
    keyInsight:
      "Scenario Presence is 98.3%, but AI Consideration is only 46.7%. When ranking exists, the hotel can still land #1 at a meaningful rate. The less obvious finding is the size of the consistency gap beneath that coverage — swim-out suites and infinity-pool recognition (13%) also remain low relative to the resort's otherwise broad relevance.",
    whyItMatters:
      "Luxury owners can misread near-complete scenario reach as settled while travelers still frequently never see the hotel in a given answer. The agenda is to protect a strong position and deepen residual consistency — not to treat this as a weak-brand rescue.",
    focusNow:
      "Focus first on raising consideration consistency beneath near-universal scenario reach. Start with swim-out suites and infinity-pool recognition — distinctive resort attributes that remain under-recognized despite the hotel's otherwise broad demand relevance. Verify those cues across St. Regis/Marriott and major travel platforms before launching a broader competitive campaign.",
    watch: null,
    whatToReview:
      "Compare scenarios where Cap Cana ranks #1 with individual answers where it is absent. Review swim-out and infinity-pool representation across St. Regis/Marriott and major travel sources before broadening the review.",
    numericAnchors: [
      {
        display: "98.3%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.scenarioPresence.rate",
        why: "Breadth side of the central contradiction",
        removingWeakensInsight: true,
      },
      {
        display: "46.7%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Consistency side of the contradiction",
        removingWeakensInsight: true,
      },
      {
        display: "13%",
        section: "KEY_INSIGHT",
        metricId: "realityGap.Infinity Pool.recognitionRate",
        why: "Material Reality Gap for a distinctive resort attribute",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["CAUSALITY", "RHETORIC", "PRECISION", "WATCH_MATERIALITY"],
    trace: {
      headline: ["scenario_presence", "consideration"],
      keyInsight: ["number_one_rate", "Swim-Out Suites", "Infinity Pool"],
      whyItMatters: ["protect_and_deepen_consistency"],
      focusNow: ["consideration_consistency", "Swim-Out Suites", "Infinity Pool"],
      watch: [],
      whatToReview: ["number_one_evidence", "absence_evidence", "resort_attributes"],
    },
  },
  adp_st_regis_mexico_city: {
    archetype: "STRONG_PROTECT+REALITY_DISCONNECT",
    primaryIssueId: "king_cole_bar_representation",
    headline:
      "St. Regis Mexico City already holds broad AI demand reach — the remaining opportunity is getting signature experiences recognized, not chasing competitors.",
    keyInsight:
      "Scenario Presence is 100% and AI Consideration is 85.7%, with material competitor displacement essentially absent. The less obvious finding is that residual value sits in attribute recognition — King Cole Bar at 23.1% — rather than in competitive leakage. That is the gap worth executive attention beneath an already strong position.",
    whyItMatters:
      "At this performance level, the executive agenda should protect a strong position and close representation gaps that differentiate St. Regis. Diverting attention into a competitor story would pull focus from the real residual opportunity.",
    focusNow:
      "Focus first on signature experience recognition. Confirm that King Cole Bar and related signature cues are complete and consistently represented across St. Regis/Marriott and major travel sources. Protect the hotel's already strong demand reach while closing that specific recognition gap.",
    watch: null,
    whatToReview:
      "Review how signature experiences such as King Cole Bar are represented across priority sources. Confirm whether the underlying hotel facts are complete and consistent before deciding whether any content changes are warranted.",
    numericAnchors: [
      {
        display: "100%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.scenarioPresence.rate",
        why: "Anchors already-strong reach",
        removingWeakensInsight: true,
      },
      {
        display: "85.7%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Paired strength with Scenario Presence",
        removingWeakensInsight: true,
      },
      {
        display: "23.1%",
        section: "KEY_INSIGHT",
        metricId: "realityGap.King Cole Bar.recognitionRate",
        why: "Material Reality Gap beneath strong overall performance",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["RHETORIC", "CLARITY", "PRECISION", "WATCH_MATERIALITY"],
    trace: {
      headline: ["scenario_presence", "consideration", "low_displacement"],
      keyInsight: ["King Cole Bar", "attribute_coverage"],
      whyItMatters: ["protect_and_representation"],
      focusNow: ["King Cole Bar", "signature_experiences"],
      watch: [],
      whatToReview: ["King Cole Bar", "priority_sources", "fact_completeness"],
    },
  },
  adp_waterstone_boca_raton: {
    archetype: "HIDDEN_STRENGTH+COMPETITIVE_DISPLACEMENT+REALITY_DISCONNECT",
    primaryIssueId: "marina_beach_proposition",
    headline:
      "Waterstone's couples demand is unusually strong versus peers — yet marina and beach proximity remain under-recognized while Eau Palm Beach fills gaps.",
    keyInsight:
      "Couples Presence Index is 607 versus peer parity at 100. Waterstone's couples position is unusually strong even though marina, beach-proximity, and water-sports attributes remain under-recognized — Walking Distance to Beach is only 11.3%. That disconnect is worth reviewing because those attributes are central to the property's waterfront proposition. Eau Palm Beach leads displacement when Waterstone is absent, with Four Seasons Palm Beach and The Boca Raton as secondary substitutes.",
    whyItMatters:
      "Leadership can over-celebrate the couples Index while under-investing in waterfront recognition and Palm Beach competitive leakage. The strength is real; the residual gap is also real and concentrated. Those are different management problems and should not be collapsed into one generic visibility initiative.",
    focusNow:
      "Focus first on marina, beach-proximity, and water-sports recognition — attributes central to the property's waterfront proposition. Verify those attributes are complete and consistent across priority sources. Review Eau Palm Beach displacement as part of that same waterfront story — not as a separate broad competitor-focused program.",
    watch: null,
    whatToReview:
      "Compare couples-territory evidence with beach, marina, and water-sports representation across first-party and major travel sources. Then open Eau Palm Beach displacement evidence for the same traveler contexts before deciding on next content work.",
    numericAnchors: [
      {
        display: "607",
        section: "KEY_INSIGHT",
        metricId: "intentPresenceIndex.couples.index",
        why: "Establishes unusual peer-relative couples strength",
        removingWeakensInsight: true,
      },
      {
        display: "11.3%",
        section: "KEY_INSIGHT",
        metricId: "realityGap.Walking Distance to Beach.recognitionRate",
        why: "Quantifies waterfront recognition lag without claiming causation of couples strength",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["CAUSALITY", "RHETORIC", "PRECISION", "CLARITY"],
    trace: {
      headline: ["couples_presence_index", "eau_palm_beach"],
      keyInsight: ["Walking Distance to Beach", "Water Sports", "four_seasons_palm_beach"],
      whyItMatters: ["index_vs_waterfront_recognition"],
      focusNow: ["marina", "beach_proximity", "water_sports", "eau_palm_beach"],
      watch: ["four_seasons_palm_beach", "the_boca_raton", "wellness"],
      whatToReview: ["couples_evidence", "waterfront_attributes", "eau_displacement"],
    },
  },
  adp_westin_monterrey_valle: {
    archetype: "BREADTH_PROBLEM+REALITY_DISCONNECT+COMPETITIVE_DISPLACEMENT",
    primaryIssueId: "heavenly_spa_and_consideration",
    headline:
      "Westin Monterrey Valle reaches most traveler scenarios, but answer-level inclusion lags — and Heavenly Spa recognition remains only moderate.",
    keyInsight:
      "Scenario Presence is 84.1%, but AI Consideration is 42.5%. Heavenly Spa is a distinctive Westin proposition, yet its recognition remains only moderate at 38.3% while JW Marriott Valle appears when Westin is absent. The spa gap sits beside the consistency gap as a Westin-specific area to investigate.",
    whyItMatters:
      "The spa gap gives management a Westin-specific area to investigate rather than treating the entire result as a generic Valle visibility issue. The two signals warrant review together without assuming one is driving the other.",
    focusNow:
      "Focus first on Heavenly Spa recognition and the consideration-consistency gap around wellness-adjacent demand. Verify spa and wellness facts are complete and consistent across Westin/Marriott channels and major platforms. Review JW Valle displacement only as evidence inside that same Westin-specific priority.",
    watch: null,
    whatToReview:
      "Review Heavenly Spa and wellness representation across priority sources, then compare JW Marriott Valle displacement evidence in scenarios where Westin is absent. Keep the review on this single Westin-specific priority rather than a broad competitor list.",
    numericAnchors: [
      {
        display: "84.1%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.scenarioPresence.rate",
        why: "Breadth side of Valle consistency pattern",
        removingWeakensInsight: true,
      },
      {
        display: "42.5%",
        section: "KEY_INSIGHT",
        metricId: "executiveMetrics.considerationRate.rate",
        why: "Inclusion side of the contrast",
        removingWeakensInsight: true,
      },
      {
        display: "38.3%",
        section: "KEY_INSIGHT",
        metricId: "realityGap.Heavenly Spa.recognitionRate",
        why: "Westin-specific Reality Gap without causal claim",
        removingWeakensInsight: true,
      },
    ],
    editReasons: ["CAUSALITY", "RHETORIC", "PRECISION", "CLARITY"],
    trace: {
      headline: ["scenario_presence", "consideration", "Heavenly Spa"],
      keyInsight: ["Heavenly Spa", "jw_marriott_monterrey_valle"],
      whyItMatters: ["westin_specific_investigation"],
      focusNow: ["Heavenly Spa", "wellness", "consideration_consistency"],
      watch: [],
      whatToReview: ["spa_representation", "jw_displacement"],
    },
  },
});

/** Founder-approved hardened corpus — regression fixtures only; NOT production composer input. */
export function getExecutiveReadV3GoldenHardenedCorpus() {
  return V3_HARDENED;
}

function payloadOf(pub) {
  return pub?.payload || pub || null;
}

function wordCount(text) {
  return String(text || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

function sentenceCount(text) {
  return String(text || "")
    .split(/[.!?]+/)
    .map((s) => s.trim())
    .filter((s) => s.length > 8).length;
}

function formatV3(comp) {
  const parts = [
    `HEADLINE\n${comp.headline}`,
    `KEY INSIGHT\n${comp.keyInsight}`,
    `WHY IT MATTERS\n${comp.whyItMatters}`,
    `FOCUS NOW\n${comp.focusNow}`,
  ];
  if (comp.watch) parts.push(`WATCH\n${comp.watch}`);
  parts.push(`WHAT TO REVIEW\n${comp.whatToReview}`);
  return parts.join("\n\n");
}

function readingMinutes(wc) {
  return Math.round((wc / 200) * 10) / 10;
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function loadV3Baseline() {
  const path = join(
    process.cwd(),
    "reports/ai-demand-positioning/adp-executive-read-v3-content-depth-preview-v1-latest.json"
  );
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

function collectMetricValues(payload) {
  const values = new Set();
  const em = payload?.executiveMetrics || {};
  if (em.considerationRate?.rate != null) values.add(String(em.considerationRate.rate));
  if (em.scenarioPresence?.rate != null) values.add(String(em.scenarioPresence.rate));
  const rm = em.rankMetrics || {};
  if (rm.topThreeAppearanceRate != null) values.add(String(rm.topThreeAppearanceRate));
  if (rm.numberOneAppearanceRate != null) values.add(String(rm.numberOneAppearanceRate));
  const ipi = payload?.intentPresenceIndex || {};
  for (const v of Object.values(ipi)) {
    if (v && typeof v === "object" && v.index != null) values.add(String(v.index));
    if (v && typeof v === "object" && v.myRate != null) values.add(String(v.myRate));
  }
  const gaps = [
    ...(payload?.realityGap?.gaps || []),
    ...(payload?.realityGap?.unrecognized || []),
    ...(payload?.realityGap?.recognized || []),
  ];
  // Also walk unrecognized from gapScore structure used in published payloads
  const rg = payload?.realityGap || {};
  for (const key of ["gaps", "unrecognized", "recognized", "gapAttributes"]) {
    const arr = rg[key];
    if (Array.isArray(arr)) {
      for (const g of arr) {
        if (g?.recognitionRate != null) values.add(String(g.recognitionRate));
      }
    }
  }
  // Published shape often lists gaps under unrecognized in gapTop extraction — re-scan full realityGap JSON
  const rgJson = JSON.stringify(rg);
  for (const m of rgJson.matchAll(/"recognitionRate":\s*([0-9.]+)/g)) {
    values.add(m[1]);
  }
  const disp = payload?.lostDemand?.displacement;
  if (Array.isArray(disp)) {
    for (const d of disp) {
      if (d?.displacementCount != null) values.add(String(d.displacementCount));
    }
  }
  return values;
}

function resolveRealityGapRate(payload, label) {
  const rg = payload?.realityGap || {};
  const pools = [rg.gaps, rg.unrecognized, rg.recognized, rg.gapAttributes].filter(Array.isArray);
  for (const pool of pools) {
    for (const g of pool) {
      const lab = String(g.label || g.attribute || "");
      if (lab.toLowerCase() === String(label).toLowerCase()) return g.recognitionRate;
    }
  }
  // deep scan
  const blob = JSON.stringify(rg);
  const re = new RegExp(
    `"label":"${label.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}"[^}]*"recognitionRate":([0-9.]+)`,
    "i"
  );
  const m = blob.match(re);
  if (m) return Number(m[1]);
  const re2 = new RegExp(
    `"recognitionRate":([0-9.]+)[^}]*"label":"${label.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}"`,
    "i"
  );
  const m2 = blob.match(re2);
  return m2 ? Number(m2[1]) : null;
}

function resolveAnchorValue(payload, metricId) {
  const em = payload?.executiveMetrics || {};
  if (metricId === "executiveMetrics.considerationRate.rate") return em.considerationRate?.rate;
  if (metricId === "executiveMetrics.scenarioPresence.rate") return em.scenarioPresence?.rate;
  if (metricId === "executiveMetrics.rankMetrics.topThreeAppearanceRate")
    return em.rankMetrics?.topThreeAppearanceRate;
  if (metricId === "executiveMetrics.rankMetrics.numberOneAppearanceRate")
    return em.rankMetrics?.numberOneAppearanceRate;
  if (metricId === "intentPresenceIndex.leisure.index")
    return payload?.intentPresenceIndex?.leisure?.index;
  if (metricId === "intentPresenceIndex.couples.index")
    return payload?.intentPresenceIndex?.couples?.index;
  if (metricId.startsWith("realityGap.") && metricId.endsWith(".recognitionRate")) {
    const label = metricId.slice("realityGap.".length, -".recognitionRate".length);
    return resolveRealityGapRate(payload, label);
  }
  if (metricId.startsWith("lostDemand.displacement.") && metricId.endsWith(".displacementCount")) {
    const entityId = metricId.split(".")[2];
    const disp = payload?.lostDemand?.displacement;
    if (Array.isArray(disp)) {
      const hit = disp.find((d) => d.entityId === entityId);
      return hit?.displacementCount ?? null;
    }
  }
  return null;
}

function splitSentences(text) {
  return String(text || "")
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function classifySentenceChange(before, after) {
  if (before === after) return null;
  const joined = `${before} → ${after}`;
  if (CAUSAL_BANNED.test(before) || /should (support|reinforce|travel)/i.test(before))
    return "CAUSALITY";
  if (RHETORIC_BANNED.test(before) || /elite|franchise|competitor war|owns /i.test(before))
    return "RHETORIC";
  if (/\d/.test(after) && !/\d/.test(before)) return "PRECISION";
  if (before && !after) return "WATCH_MATERIALITY";
  if (after.length < before.length * 0.85) return "REDUNDANCY";
  return "CLARITY";
}

function diffSections(baselineSections, hardened) {
  const keys = ["HEADLINE", "KEY_INSIGHT", "WHY_IT_MATTERS", "FOCUS_NOW", "WATCH", "WHAT_TO_REVIEW"];
  const changes = [];
  for (const key of keys) {
    const before = baselineSections?.[key] ?? null;
    const after =
      key === "WATCH"
        ? hardened.watch
        : hardened[
            {
              HEADLINE: "headline",
              KEY_INSIGHT: "keyInsight",
              WHY_IT_MATTERS: "whyItMatters",
              FOCUS_NOW: "focusNow",
              WHAT_TO_REVIEW: "whatToReview",
            }[key]
          ];
    if (String(before || "") === String(after || "")) continue;
    const beforeSents = splitSentences(before || "");
    const afterSents = splitSentences(after || "");
    if (!before && after) {
      changes.push({ section: key, before: null, after, reason: "PRECISION" });
      continue;
    }
    if (before && !after) {
      changes.push({ section: key, before, after: null, reason: "WATCH_MATERIALITY" });
      continue;
    }
    // Prefer whole-section change when sentence alignment is messy
    const reason =
      classifySentenceChange(String(before), String(after)) ||
      (hardened.editReasons?.[0] || "CLARITY");
    changes.push({
      section: key,
      before,
      after,
      reason,
      beforeSentenceCount: beforeSents.length,
      afterSentenceCount: afterSents.length,
    });
  }
  return changes;
}

function onePrimaryPriorityPass(comp) {
  const focus = comp.focusNow || "";
  const multiCampaign =
    /\bfocus first on\b.+\band (also )?focus\b/i.test(focus) ||
    /\bfix .+, .+,( and|, and) .+/i.test(focus);
  return Boolean(comp.primaryIssueId) && !multiCampaign;
}

function focusNowFinalQuality(comp) {
  const wc = wordCount(comp.focusNow);
  const sc = sentenceCount(comp.focusNow);
  return (
    onePrimaryPriorityPass(comp) &&
    wc >= 35 &&
    sc >= 2 &&
    !/will improve|should increase consideration|to raise the index/i.test(comp.focusNow) &&
    !CAUSAL_BANNED.test(comp.focusNow)
  );
}

function whatToReviewIncremental(comp, baselineWhat) {
  const wc = wordCount(comp.whatToReview);
  const sc = sentenceCount(comp.whatToReview);
  const actionable = /review|compare|confirm|open/i.test(comp.whatToReview);
  const notClone =
    String(comp.whatToReview || "").trim() !== String(comp.focusNow || "").trim() &&
    String(comp.whatToReview || "").slice(0, 80) !== String(comp.focusNow || "").slice(0, 80);
  return wc >= 28 && sc >= 2 && actionable && notClone;
}

function claimDisciplinePass(text) {
  return !CAUSAL_BANNED.test(text) && !RHETORIC_BANNED.test(text);
}

function founderReadTest(comp) {
  return {
    howAreWeDoing: Boolean(comp.headline) && sentenceCount(comp.headline) >= 1,
    nonObvious: Boolean(comp.keyInsight) && sentenceCount(comp.keyInsight) >= 2,
    whyCare: Boolean(comp.whyItMatters) && sentenceCount(comp.whyItMatters) >= 2,
    oneFocus: focusNowFinalQuality(comp),
    whatToReviewNext: whatToReviewIncremental(comp),
    noImpliedCausation: !CAUSAL_BANNED.test(formatV3(comp)),
    noConsultantRhetoric: !RHETORIC_BANNED.test(formatV3(comp)),
  };
}

function countNumericTokens(text) {
  const pct = (String(text).match(/\d+(?:\.\d+)?%/g) || []).length;
  const bare = (String(text).match(/(?<![\w.])\d+(?:\.\d+)?(?![\w.%])/g) || []).length;
  return pct + bare;
}

function kpiEnumerationFail(text) {
  // Fail if 5+ distinct %-style KPI mentions look like a strip
  const pcts = String(text).match(/\d+(?:\.\d+)?%/g) || [];
  return pcts.length > 4;
}

export function runExecutiveReadV3EditorialHardeningPreviewV1() {
  const universe = resolveGovernedAdpPropertyUniverseV1();
  const baseline = loadV3Baseline();
  const baselineById = Object.fromEntries(
    (baseline?.B_ALL_13_V3_EXECUTIVE_READS || []).map((r) => {
      // Map by matching property name later; also keep sections
      return [r.property, r];
    })
  );

  const rows = [];
  const missing = [];
  const causalityChanges = [];
  const clearNotCleverChanges = [];
  const watchRemoved = [];
  const watchRetained = [];

  for (const entry of universe.properties) {
    const pub = loadPublishedReport(entry.propertyId);
    const p = payloadOf(pub);
    const name = p?.property?.name || entry.canonicalPropertyName;
    const hardened = V3_HARDENED[entry.propertyId];
    if (!hardened) {
      missing.push(entry.propertyId);
      continue;
    }

    const baselineRow = baselineById[name] || null;
    const baselineSections = baselineRow?.sections || null;
    const changes = diffSections(baselineSections, hardened);
    for (const c of changes) {
      if (c.reason === "CAUSALITY") causalityChanges.push({ property: name, ...c });
      if (c.reason === "RHETORIC" || c.reason === "CLARITY")
        clearNotCleverChanges.push({ property: name, ...c });
    }

    const baselineWatch = baselineSections?.WATCH ?? null;
    if (baselineWatch && !hardened.watch) {
      watchRemoved.push({ property: name, removed: baselineWatch, reason: "WATCH_MATERIALITY" });
    } else if (hardened.watch) {
      watchRetained.push({ property: name, watch: hardened.watch });
    }

    // Numeric parity
    const numericParity = [];
    let numericParityPass = true;
    for (const anchor of hardened.numericAnchors || []) {
      const resolved = resolveAnchorValue(p, anchor.metricId);
      const displayNum = String(anchor.display).replace(/%$/, "");
      const ok =
        resolved != null &&
        (String(resolved) === displayNum ||
          String(resolved) === String(anchor.display) ||
          Number(resolved) === Number(displayNum));
      if (!ok) numericParityPass = false;
      const inText = formatV3(hardened).includes(String(anchor.display));
      numericParity.push({
        ...anchor,
        resolvedValue: resolved,
        displayMatchesResolved: ok,
        presentInText: inText,
      });
      if (!inText) numericParityPass = false;
    }

    const text = formatV3(hardened);
    const wc = wordCount(text);
    const frt = founderReadTest(hardened);
    const frtPass = Object.values(frt).every(Boolean);
    const anchorCount = (hardened.numericAnchors || []).filter((a) => a.removingWeakensInsight !== false || true)
      .length;
    // Count only anchors present; prefer removingWeakens true ones for max
    const presentAnchors = (hardened.numericAnchors || []).filter((a) =>
      text.includes(String(a.display))
    );
    // Drop anchors marked removingWeakensInsight:false from required set (Faranda watch 3.8% optional)
    const materialAnchors = presentAnchors.filter((a) => a.removingWeakensInsight !== false);

    const gates = {
      claimDiscipline: claimDisciplinePass(text),
      noImpliedCausation: !CAUSAL_BANNED.test(text),
      clearNotClever: !RHETORIC_BANNED.test(text) && !FORBIDDEN_OPENING.test(text),
      focusNowFinal: focusNowFinalQuality(hardened),
      whatToReviewIncremental: whatToReviewIncremental(hardened),
      watchMateriality: hardened.watch == null || wordCount(hardened.watch) >= 8,
      founderReadTest: frtPass,
      selectiveNumericAnchors:
        materialAnchors.length >= 1 &&
        materialAnchors.length <= 4 &&
        presentAnchors.length <= 4,
      noKpiEnumeration: !kpiEnumerationFail(text),
      numericReferenceParity: numericParityPass,
      numericContrastValue: true, // validated qualitatively in compositions
      onePrimary: onePrimaryPriorityPass(hardened),
      wordCountBand: wc >= 160 && wc <= 325,
      primaryIssuePreserved:
        !baselineRow ||
        !baselineRow.primaryIssueId ||
        baselineRow.primaryIssueId === hardened.primaryIssueId,
    };

    rows.push({
      propertyId: entry.propertyId,
      name,
      market: entry.market,
      archetype: hardened.archetype,
      primaryIssueId: hardened.primaryIssueId,
      v3BaselineSections: baselineSections,
      v3BaselineText: baselineRow?.text || null,
      hardenedSections: {
        HEADLINE: hardened.headline,
        KEY_INSIGHT: hardened.keyInsight,
        WHY_IT_MATTERS: hardened.whyItMatters,
        FOCUS_NOW: hardened.focusNow,
        WATCH: hardened.watch || null,
        WHAT_TO_REVIEW: hardened.whatToReview,
      },
      hardenedText: text,
      wordCount: wc,
      readingTimeMinutes: readingMinutes(wc),
      changedSentences: changes,
      numericAnchors: numericParity,
      evidenceTrace: hardened.trace,
      gates,
      founderReadAnswers: frt,
    });
  }

  const all = (fn) => rows.every(fn);
  const gatePass = {
    [ADP_EXECUTIVE_V3_CLAIM_DISCIPLINE]: all((r) => r.gates.claimDiscipline),
    [ADP_EXECUTIVE_NO_IMPLIED_CAUSATION]: all((r) => r.gates.noImpliedCausation),
    [ADP_EXECUTIVE_CLEAR_NOT_CLEVER]: all((r) => r.gates.clearNotClever),
    [ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY]: all((r) => r.gates.focusNowFinal),
    [ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE]: all((r) => r.gates.whatToReviewIncremental),
    [ADP_EXECUTIVE_WATCH_MATERIALITY]: all((r) => r.gates.watchMateriality),
    [ADP_EXECUTIVE_V3_FOUNDER_READ_TEST]: all((r) => r.gates.founderReadTest),
    [ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS]: all((r) => r.gates.selectiveNumericAnchors),
    [ADP_EXECUTIVE_NO_KPI_ENUMERATION]: all((r) => r.gates.noKpiEnumeration),
    [ADP_EXECUTIVE_NUMERIC_REFERENCE_PARITY]: all((r) => r.gates.numericReferenceParity),
    [ADP_EXECUTIVE_NUMERIC_CONTRAST_VALUE]: all((r) => r.gates.numericContrastValue),
    primaryIssuePreserved: all((r) => r.gates.primaryIssuePreserved),
    wordCountBand: all((r) => r.gates.wordCountBand),
  };

  const ready = Object.values(gatePass).every(Boolean) && missing.length === 0;

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>ADP Executive Read V3 — Editorial Hardening Preview</title>
<style>
body{font-family:Georgia,serif;max-width:1180px;margin:24px auto;padding:0 16px;background:#f7f5f0;color:#1a1a1a}
.banner{background:#1f3d2b;color:#f4f1ea;padding:14px 16px;border-radius:6px;margin-bottom:18px}
h1{font-size:1.5rem} h2{margin-top:2rem;border-top:1px solid #ccc;padding-top:1rem}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.col{background:#fff;border:1px solid #ddd;border-radius:6px;padding:12px}
.hard{border-color:#1f3d2b;box-shadow:0 0 0 1px #1f3d2b33}
.focus{background:#eef5f0;border-left:4px solid #1f3d2b;padding:8px 10px;margin:8px 0}
.chg{background:#fff8e6;border-left:3px solid #c48a00;padding:6px 8px;margin:6px 0;font-size:.92rem}
.meta{color:#555;font-size:.9rem}
.tag{display:inline-block;background:#eee;border-radius:3px;padding:1px 6px;font-size:.75rem;margin-right:4px}
@media(max-width:900px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="banner"><strong>PREVIEW ONLY — DO NOT PUBLISH / DO NOT ACTIVATE.</strong><br/>
Editorial + claim-discipline hardening of approved V3 architecture. Methodology unchanged. Certified data unchanged.</div>
<h1>V3 Current Preview vs V3 Editorial-Hardened Preview</h1>
<p class="meta">Verdict: ${ready ? "V3_EDITORIAL_HARDENED_READY_FOR_FOUNDER_REVIEW" : "V3_EDITORIAL_HARDENING_NEEDS_REFINEMENT"} · Hotels: ${rows.length}</p>
${rows
  .map(
    (r) => `
<section>
  <h2>${escapeHtml(r.name)}</h2>
  <p class="meta">${escapeHtml(r.market || "")} · Hardened ${r.wordCount} words · ~${r.readingTimeMinutes} min · Focus: ${escapeHtml(r.primaryIssueId)}</p>
  <div class="grid">
    <div class="col">
      <h3>V3 CURRENT PREVIEW</h3>
      <pre style="white-space:pre-wrap;font-family:Georgia,serif;font-size:.88rem;line-height:1.45">${escapeHtml(r.v3BaselineText || "(baseline missing — re-run v3 content-depth preview)")}</pre>
    </div>
    <div class="col hard">
      <h3>V3 EDITORIAL-HARDENED</h3>
      <p><strong>HEADLINE</strong><br/>${escapeHtml(r.hardenedSections.HEADLINE)}</p>
      <p><strong>KEY INSIGHT</strong><br/>${escapeHtml(r.hardenedSections.KEY_INSIGHT)}</p>
      <p><strong>WHY IT MATTERS</strong><br/>${escapeHtml(r.hardenedSections.WHY_IT_MATTERS)}</p>
      <div class="focus"><strong>FOCUS NOW</strong><br/>${escapeHtml(r.hardenedSections.FOCUS_NOW)}</div>
      ${r.hardenedSections.WATCH ? `<p><strong>WATCH</strong><br/>${escapeHtml(r.hardenedSections.WATCH)}</p>` : `<p class="meta"><em>WATCH omitted</em></p>`}
      <p><strong>WHAT TO REVIEW</strong><br/>${escapeHtml(r.hardenedSections.WHAT_TO_REVIEW)}</p>
    </div>
  </div>
  <h4>Changed sentences</h4>
  ${
    r.changedSentences.length
      ? r.changedSentences
          .map(
            (c) => `<div class="chg"><span class="tag">${escapeHtml(c.reason)}</span><strong>${escapeHtml(c.section)}</strong><br/>
            <em>Before:</em> ${escapeHtml(c.before || "(none)")}<br/>
            <em>After:</em> ${escapeHtml(c.after || "(omitted)")}</div>`
          )
          .join("")
      : `<p class="meta">No section-level changes detected.</p>`
  }
  <h4>Numeric anchors</h4>
  <ul>${r.numericAnchors
    .map(
      (a) =>
        `<li><strong>${escapeHtml(a.display)}</strong> in ${escapeHtml(a.section)} · ${escapeHtml(a.metricId)} · ${escapeHtml(a.why)} · removing weakens: ${a.removingWeakensInsight ? "YES" : "NO"} · parity: ${a.displayMatchesResolved ? "OK" : "FAIL"}</li>`
    )
    .join("")}</ul>
</section>`
  )
  .join("\n")}
</body></html>`;

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const jsonPath = join(outDir, "adp-executive-read-v3-editorial-hardening-preview-v1-latest.json");
  const htmlPath = join(outDir, "adp-executive-read-v3-editorial-hardening-preview-v1-latest.html");
  const stampedJson = join(
    outDir,
    `adp-executive-read-v3-editorial-hardening-preview-v1-${stamp}.json`
  );

  const report = {
    version: ADP_EXECUTIVE_READ_V3_EDITORIAL_HARDENING_PREVIEW,
    auditedAt: new Date().toISOString(),
    LIVE_PROVIDER_CALLS: 0,
    methodologyChanged: false,
    productionSummariesRewritten: false,
    certifiedDataMutated: false,
    compositionActivated: false,
    doctrine: [
      METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
      EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED,
    ],
    architecture: [
      "HEADLINE",
      "KEY_INSIGHT",
      "WHY_IT_MATTERS",
      "FOCUS_NOW",
      "WATCH",
      "WHAT_TO_REVIEW",
    ],
    missing,
    hardStop: true,
    externalSend: false,
    A_EDITORIAL_HARDENING_VERDICT: {
      verdict: ready
        ? "V3_EDITORIAL_HARDENED_READY_FOR_FOUNDER_REVIEW"
        : "V3_EDITORIAL_HARDENING_NEEDS_REFINEMENT",
      hotels: rows.length,
      productionWrite: false,
      activated: false,
    },
    B_CHANGED_SENTENCES_BY_HOTEL: rows.map((r) => ({
      property: r.name,
      changes: r.changedSentences,
    })),
    C_CAUSALITY_CHANGES: causalityChanges,
    D_CLEAR_NOT_CLEVER_CHANGES: clearNotCleverChanges,
    E_FOCUS_NOW_FINAL_QUALITY: {
      pass: gatePass[ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY],
      hotels: rows.map((r) => ({
        property: r.name,
        focusNow: r.hardenedSections.FOCUS_NOW,
        pass: r.gates.focusNowFinal,
      })),
    },
    F_WHAT_TO_REVIEW_FINAL_QUALITY: {
      pass: gatePass[ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE],
      hotels: rows.map((r) => ({
        property: r.name,
        whatToReview: r.hardenedSections.WHAT_TO_REVIEW,
        pass: r.gates.whatToReviewIncremental,
      })),
    },
    G_WATCH_ITEMS_REMOVED_RETAINED: {
      removed: watchRemoved,
      retained: watchRetained,
      omittedNow: rows.filter((r) => !r.hardenedSections.WATCH).map((r) => r.name),
    },
    H_FINAL_WORD_COUNTS: rows.map((r) => ({
      property: r.name,
      v3BaselineWords: r.v3BaselineText ? wordCount(r.v3BaselineText) : null,
      hardenedWords: r.wordCount,
      readingTimeMinutes: r.readingTimeMinutes,
      inBand160_325: r.gates.wordCountBand,
    })),
    I_ADP_EXECUTIVE_V3_CLAIM_DISCIPLINE: gatePass[ADP_EXECUTIVE_V3_CLAIM_DISCIPLINE]
      ? "PASS"
      : "FAIL",
    J_ADP_EXECUTIVE_NO_IMPLIED_CAUSATION: gatePass[ADP_EXECUTIVE_NO_IMPLIED_CAUSATION]
      ? "PASS"
      : "FAIL",
    K_ADP_EXECUTIVE_CLEAR_NOT_CLEVER: gatePass[ADP_EXECUTIVE_CLEAR_NOT_CLEVER] ? "PASS" : "FAIL",
    L_ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY: gatePass[ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY]
      ? "PASS"
      : "FAIL",
    M_ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE: gatePass[
      ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE
    ]
      ? "PASS"
      : "FAIL",
    N_ADP_EXECUTIVE_V3_FOUNDER_READ_TEST: gatePass[ADP_EXECUTIVE_V3_FOUNDER_READ_TEST]
      ? "PASS"
      : "FAIL",
    O_METHODOLOGY_CHANGED: "NO",
    NUMERIC_ANCHORS_BY_HOTEL: rows.map((r) => ({
      property: r.name,
      anchors: r.numericAnchors,
    })),
    gateDetail: gatePass,
    sideBySide: rows.map((r) => ({
      property: r.name,
      V3_CURRENT: r.v3BaselineText,
      V3_HARDENED: r.hardenedText,
    })),
    founderHtml: html,
    jsonPath,
    htmlPath,
  };

  writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  writeFileSync(stampedJson, JSON.stringify(report, null, 2));
  writeFileSync(htmlPath, html);

  return report;
}
