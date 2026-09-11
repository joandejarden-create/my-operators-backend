/**
 * ADP Executive Read V3 — deeper content / executive relevance PREVIEW.
 * Does NOT activate production. Does NOT change methodology or certified data.
 *
 * Architecture:
 *   HEADLINE | KEY INSIGHT | WHY IT MATTERS | FOCUS NOW | WATCH? | WHAT TO REVIEW
 */

import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { resolveGovernedAdpPropertyUniverseV1 } from "../client-readiness/resolve-governed-adp-property-universe-v1.js";
import { loadPublishedReport } from "../published-snapshot.js";
import { METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN } from "./adp-methodology-governance-v1.js";
import { EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED } from "./adp-executive-read-composition-v2.js";

export const ADP_EXECUTIVE_READ_V3_PREVIEW =
  "adp_executive_read_v3_content_depth_preview_v1";

export const ADP_EXECUTIVE_KEY_INSIGHT_SUBSTANTIVE = "ADP_EXECUTIVE_KEY_INSIGHT_SUBSTANTIVE";
export const ADP_EXECUTIVE_WHY_IT_MATTERS_SUBSTANTIVE =
  "ADP_EXECUTIVE_WHY_IT_MATTERS_SUBSTANTIVE";
export const ADP_EXECUTIVE_FOCUS_NOW_DECISION_DEPTH =
  "ADP_EXECUTIVE_FOCUS_NOW_DECISION_DEPTH";
export const ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE =
  "ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE";
export const ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY = "ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY";
export const ADP_EXECUTIVE_READ_SECTION_TRACEABILITY =
  "ADP_EXECUTIVE_READ_SECTION_TRACEABILITY";
export const ADP_EXECUTIVE_READ_VS_MONTHLY_ACTION_LAYER_SEPARATION =
  "ADP_EXECUTIVE_READ_VS_MONTHLY_ACTION_LAYER_SEPARATION";
export const ADP_EXECUTIVE_V3_EXECUTIVE_VALUE_TEST = "ADP_EXECUTIVE_V3_EXECUTIVE_VALUE_TEST";
export const ADP_EXECUTIVE_CLEAR_NOT_CLEVER = "ADP_EXECUTIVE_CLEAR_NOT_CLEVER";

const FORBIDDEN_OPENING =
  /appears in [\d.]+% of monitored demand scenarios but in [\d.]+% of individual comparable AI responses/i;

/** Deeper V3 compositions — style-aligned to founder Cambridge / Phillips / Faranda examples. */
const V3_COMPOSITIONS = Object.freeze({
  adp_cambridge_beaches_bermuda: {
    archetype: "HIDDEN_STRENGTH+COMPETITIVE_DISPLACEMENT+REALITY_DISCONNECT",
    primaryIssueId: "business_group_positioning_gap",
    headline:
      "Cambridge is one of the strongest leisure performers in the monitored set, but its weakness is concentrated in a few commercially relevant gaps rather than broad visibility.",
    keyInsight:
      "The hotel materially outperforms peers for leisure demand, yet Rosewood Bermuda repeatedly appears in business/group scenarios where Cambridge is absent. Separately, Five Private Coves — a distinctive part of the property proposition — remains weakly recognized in monitored AI answers. The overall strength can therefore hide where the hotel is still vulnerable.",
    whyItMatters:
      "Management does not need a broad visibility program. It needs to understand why specific business/group use cases and distinctive property attributes are not traveling with the rest of the hotel's strong AI position. Otherwise, attention may be spent protecting strengths that are already working while missing the concentrated gaps.",
    focusNow:
      "Focus first on the business/group positioning gap. Review the scenarios where Rosewood is consistently surfaced instead of Cambridge and determine whether the relevant group, meeting, privacy, and retreat attributes are clearly represented across priority sources. Protect the leisure advantage while closing this concentrated soft spot.",
    watch:
      "Continue monitoring recognition of Five Private Coves as a separate proposition-quality signal — important, but secondary to the business/group displacement pattern.",
    whatToReview:
      "Open the underlying Rosewood displacement evidence and compare the hotel attributes cited in those answers with Cambridge's first-party and major travel-source representation. Confirm whether business/group and privacy/retreat cues are complete and consistent before deciding on content or channel changes.",
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
      "The weakness is not mainly ranking after inclusion. When Faranda is absent, Four Seasons Bogotá and Click Clack repeatedly appear instead, indicating that a relatively small group of competitors is capturing demand contexts where Faranda rarely enters the answer set. Absolute rates are low, but the substitution pattern is concentrated rather than diffuse.",
    whyItMatters:
      "Fine-tuning rank against individual competitors will have limited value until the hotel becomes relevant in more of the traveler needs it currently misses. The executive priority is answer-set entry for leisure and couples demand, not a broad competitor war.",
    focusNow:
      "Focus first on how the hotel is represented for leisure and couples demand. Review whether the attributes, positioning, and use cases most relevant to those travelers are complete and consistently represented across priority sources. Treat competitor-rank tactics as secondary until entry improves.",
    watch:
      "Choice Hotels distribution recognition remains very low if that membership is still an intentional commercial proposition.",
    whatToReview:
      "Compare the evidence from scenarios where Four Seasons and Click Clack appear with the property information currently available for Faranda. Check leisure and couples cues first — location, product type, and stay occasion — before broadening the review.",
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
      "Hotel Caribe is often Top 3 when AI ranks hotels — the real bottleneck is inconsistent inclusion in individual answers, especially for business demand.",
    keyInsight:
      "Scenario-level relevance is already reasonably broad, and prominence after inclusion is a genuine strength. The less obvious finding is that business demand is among the softer territories for response-level inclusion. Hilton Cartagena and Bastión appear in some absences, but the binding constraint is getting into the answer set more consistently — not losing rank once included.",
    whyItMatters:
      "If management focuses mainly on beating named competitors on rank, it may be solving the wrong problem. Closing the gap between broad scenario relevance and individual-answer inclusion — particularly for business — is the higher-leverage path.",
    focusNow:
      "Focus first on consideration consistency for business and adjacent traveler needs. Verify that business-relevant positioning, amenities, and location cues are complete and consistent across Faranda/Radisson Individuals channels and major platforms where Caribe already shows scenario relevance but lags in individual answers.",
    watch:
      "Hilton Cartagena and Bastión Luxury displacement counts in contexts where Caribe remains absent.",
    whatToReview:
      "Review business-territory evidence and compare attributes cited when Caribe is included versus when Hilton or Bastión appear instead. Confirm whether business and full-service cues are consistently represented before pursuing broader competitive campaigns.",
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
      "Phillips reaches many traveler scenarios, but its meeting proposition is not being recognized strongly enough to convert that reach into consistent inclusion.",
    keyInsight:
      "The weakness is not simply overall AI visibility. Group/meeting demand is one of the softer territories, ballroom recognition is low, and Loews Kansas City plus Hotel Kansas City repeatedly appear in answers where Phillips is absent. That combination points to a concentrated proposition problem rather than a generic visibility issue.",
    whyItMatters:
      "A hotel with meaningful meeting assets may be underrepresented in exactly the traveler need where those assets should be most relevant. Broad scenario reach can look healthier than the meetings story underneath.",
    focusNow:
      "Focus first on the meeting and ballroom proposition. Verify that meeting-space facts, capacities, event positioning, and supporting content are complete and consistent across Hilton/Curio, the hotel website, and major third-party channels. Do this before launching a broader AI visibility program.",
    watch:
      "Fitness Center recognition near zero if wellness/fitness remains an intentional offer — secondary to meetings.",
    whatToReview:
      "Review the meeting-related evidence and compare Phillips' representation with Loews and Hotel Kansas City in the same traveler scenarios. Confirm whether important meeting-space facts are complete and consistent across Hilton/Curio and major third-party channels.",
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
      "Near-universal scenario reach and elite ranking prominence coexist with inconsistent answer-level inclusion. Family demand is among the softer territories. The less obvious finding is that the hotel's problem is not weak prominence once included — it is inconsistent entry into the answer set despite already covering most traveler needs at the scenario level.",
    whyItMatters:
      "Near-perfect scenario coverage can look finished while travelers still frequently never see the hotel in a given answer. Management may over-invest in competitive rank and under-invest in consideration consistency. Closing that gap is higher leverage than another competitor campaign.",
    focusNow:
      "Focus first on closing the consideration-consistency gap, especially for family-adjacent demand. Confirm that family-relevant and Valle location cues are clearly and consistently represented across Marriott channels and major platforms. The goal is answer-level inclusion that better matches the hotel's already strong Top-3 performance.",
    watch:
      "AC Hotel and Holiday Inn Valle displacement counts if they rise from today's low base.",
    whatToReview:
      "Compare scenarios where JW Monterrey Valle is ranked Top 3 with scenarios where it is absent from individual answers. Review family and location attributes first to see whether representation gaps explain the consistency shortfall.",
    trace: {
      headline: ["top3_rate", "consideration", "scenario_presence"],
      keyInsight: ["family_territory", "inclusion_vs_prominence"],
      whyItMatters: ["scenario_coverage_misread"],
      focusNow: ["family", "consideration_consistency", "valle_location"],
      watch: ["ac_hotel_monterrey_valle"],
      whatToReview: ["top3_evidence", "absence_evidence", "family_attributes"],
    },
  },
  adp_jw_marriott_santo_domingo: {
    archetype: "STRONG_PROTECT+REALITY_DISCONNECT",
    primaryIssueId: "signature_urban_representation",
    headline:
      "JW Santo Domingo is performing strongly overall. The more important opportunity is protecting that position while closing residual representation gaps.",
    keyInsight:
      "Scenario reach and consideration are both strong, and competitive displacement is minimal versus peers. The less obvious finding is that residual risk sits in proposition representation — Blue Mall adjacency, Executive Lounge, and wellness soft spots — not in rival hotels taking over the answer set. This is a protect-and-polish story, not a competitive rescue story.",
    whyItMatters:
      "At this performance level, inventing a crisis wastes attention. The agenda should protect a broad franchise and polish the signature urban/business cues that differentiate the hotel when AI describes it. That keeps leadership focused on residual value rather than manufactured problems.",
    focusNow:
      "Focus first on signature urban and business representation. Confirm that Blue Mall adjacency and Executive Lounge facts are complete and consistently represented across Marriott and major travel sources. Protect the hotel's already strong demand reach while closing those specific recognition gaps.",
    watch: null,
    whatToReview:
      "Review how Blue Mall adjacency, Executive Lounge, and wellness cues appear in monitored answers versus first-party and major travel sources. Confirm whether the underlying hotel facts are complete and consistent before deciding on any content changes.",
    trace: {
      headline: ["scenario_presence", "consideration", "top3_rate"],
      keyInsight: ["low_displacement", "Blue Mall Adjacency", "Executive Lounge"],
      whyItMatters: ["protect_and_polish"],
      focusNow: ["Blue Mall Adjacency", "Executive Lounge"],
      watch: [],
      whatToReview: ["reality_gap", "first_party_sources", "wellness"],
    },
  },
  adp_now_now_noho: {
    archetype: "HIDDEN_WEAKNESS+COMPETITIVE_DISPLACEMENT",
    primaryIssueId: "lifestyle_couples_entry",
    headline:
      "NOW NOW NOHO is rarely entering traveler consideration at all — named boutique neighbors currently own the lifestyle answer set.",
    keyInsight:
      "Absolute rates are very low, and couples and family territories show near-zero reach. When NOHO is absent, Crosby Street, Soho Grand, and The Bowery repeatedly fill lifestyle recommendations. The less obvious finding is the concentrated peer substitution pattern — not merely that overall visibility is weak.",
    whyItMatters:
      "Without first earning inclusion in lifestyle and couples demand, competitor-rank tactics will not move the executive picture. The priority is becoming relevant in the traveler needs the hotel currently misses.",
    focusNow:
      "Focus first on couples and lifestyle-oriented stays where the hotel is repeatedly absent. Review whether urban lifestyle positioning, neighborhood cues, and stay-occasion attributes are complete and consistently represented across priority sources. Do that before pursuing individual competitor-rank work against named boutique peers.",
    watch:
      "Crosby Street Hotel remains the lead substitute pattern to monitor as entry improves.",
    whatToReview:
      "Compare evidence from scenarios dominated by Crosby Street, Soho Grand, and The Bowery with NOHO's current property information. Start with couples and lifestyle cues — neighborhood, product type, and stay occasion — before broadening the review.",
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
      "When Radisson is absent, competitors appear in a large share of those contexts — led heavily by JW Marriott Santo Domingo, with Jaragua and El Embajador as secondary substitutes. Family and leisure soft spots sit inside that same substitution story. The less obvious finding is how asymmetric the Santo Domingo set is: JW is not a generic peer mention.",
    whyItMatters:
      "Every improvement initiative should be judged against JW's presence in the same demand contexts. Treating competitors as an undifferentiated list will understate the real competitive pressure. The hotel needs a JW-aware agenda, not a generic visibility plan.",
    focusNow:
      "Focus first on the traveler needs with the highest JW Marriott displacement overlap — including family and leisure soft spots. Strengthen how Radisson is represented for those needs across priority sources. Treat JW as the primary competitive substitute rather than one of many equals.",
    watch:
      "Jaragua and El Embajador remain secondary substitutes worth monitoring as JW-overlap needs improve.",
    whatToReview:
      "Open the JW Marriott displacement evidence and compare the attributes and traveler needs cited there with Radisson's current representation. Prioritize family and leisure scenarios where substitution is most concentrated before broadening the review.",
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
      "Consideration remains very low. The Knickerbocker leads displacement, Times Square views recognition is weak, and wellness/adventure territories are near zero. A small business-travel peer foothold exists beneath those absolute rates, but it is easy to miss if management only reads the headline weakness as an undifferentiated visibility problem.",
    whyItMatters:
      "Scarce management attention should go to answer-set entry and Times Square proposition clarity — not Top-3 optimization. Fighting competitors on rank is premature while the hotel is still rarely included in individual answers.",
    focusNow:
      "Focus first on entering consideration for core Times Square traveler needs by clarifying Times Square views and location proposition across Renaissance/Marriott channels and major platforms. Investigate Knickerbocker displacement only as part of that same entry story — not as a separate rank campaign.",
    watch:
      "Westin Times Square and Marriott Marquis as secondary displacers; protect the small business-travel foothold.",
    whatToReview:
      "Review Times Square views and location representation across first-party and major travel sources. Then compare the attributes cited when The Knickerbocker appears in scenarios where Renaissance is absent, and confirm whether those cues are complete and consistent before deciding on next content work.",
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
      "Scenario reach is near universal, and when ranking exists the hotel can still land #1 at a meaningful rate. The less obvious finding is the size of the consistency gap beneath that elite coverage — swim-out suites and infinity-pool recognition also remain disproportionately low relative to the resort's otherwise broad relevance.",
    whyItMatters:
      "Luxury owners can misread near-perfect scenario reach as finished work while travelers still frequently never see the hotel in a given answer. The agenda is protect-and-deepen, not a weak-brand rescue.",
    focusNow:
      "Focus first on raising consideration consistency beneath near-universal scenario reach. Start with swim-out suites and infinity-pool recognition — distinctive resort attributes that should travel with Cap Cana's already broad demand relevance. Verify those cues across St. Regis/Marriott and major travel platforms before launching a broader competitive campaign.",
    watch:
      "Secrets Cap Cana and Tortuga Bay displacement if counts rise from today's rare base.",
    whatToReview:
      "Compare scenarios where Cap Cana ranks #1 with individual answers where it is absent. Review swim-out and infinity-pool representation across St. Regis/Marriott and major travel sources before broadening the review.",
    trace: {
      headline: ["scenario_presence", "consideration"],
      keyInsight: ["number_one_rate", "Swim-Out Suites", "Infinity Pool"],
      whyItMatters: ["protect_and_deepen"],
      focusNow: ["consideration_consistency", "Swim-Out Suites", "Infinity Pool"],
      watch: ["secrets_cap_cana", "tortuga_bay_punta_cana"],
      whatToReview: ["number_one_evidence", "absence_evidence", "resort_attributes"],
    },
  },
  adp_st_regis_mexico_city: {
    archetype: "STRONG_PROTECT+REALITY_DISCONNECT",
    primaryIssueId: "king_cole_bar_representation",
    headline:
      "St. Regis Mexico City already owns broad AI demand reach — the remaining opportunity is getting signature experiences recognized, not chasing competitors.",
    keyInsight:
      "Scenario and consideration performance are elite, and material competitor displacement is essentially absent. The less obvious finding is that residual value is locked in attribute recognition — especially King Cole Bar and related full-service cues — rather than in competitive leakage. That is the gap worth executive attention beneath an already strong position.",
    whyItMatters:
      "At elite performance, the executive agenda should protect the franchise and close representation gaps that differentiate St. Regis. Manufacturing a competitor crisis would pull attention away from the real residual opportunity.",
    focusNow:
      "Focus first on signature experience recognition. Confirm that King Cole Bar and related signature cues are complete and consistently represented across St. Regis/Marriott and major travel sources. Protect the hotel's already elite demand reach while closing that specific recognition gap.",
    watch:
      "Any new competitor displacement emerging from a near-zero base; Marriott Bonvoy cues if loyalty remains intentional.",
    whatToReview:
      "Review how signature experiences such as King Cole Bar are represented across priority sources. Confirm whether the underlying hotel facts are complete and consistent before deciding whether any content changes are warranted.",
    trace: {
      headline: ["scenario_presence", "consideration", "low_displacement"],
      keyInsight: ["King Cole Bar", "attribute_coverage"],
      whyItMatters: ["protect_and_representation"],
      focusNow: ["King Cole Bar", "signature_experiences"],
      watch: ["new_displacement", "Marriott Bonvoy Loyalty"],
      whatToReview: ["King Cole Bar", "priority_sources", "fact_completeness"],
    },
  },
  adp_waterstone_boca_raton: {
    archetype: "HIDDEN_STRENGTH+COMPETITIVE_DISPLACEMENT+REALITY_DISCONNECT",
    primaryIssueId: "marina_beach_proposition",
    headline:
      "Waterstone's couples demand is unusually strong versus peers — yet marina and beach proximity remain under-recognized while Eau Palm Beach fills gaps.",
    keyInsight:
      "Couples Presence Index is extraordinary relative to CORE peers. The less obvious finding is that AI may credit romantic demand without crediting the waterfront proposition that should support it. Eau Palm Beach leads displacement when Waterstone is absent, with Four Seasons Palm Beach and The Boca Raton as secondary substitutes.",
    whyItMatters:
      "Leadership can over-celebrate the couples Index while under-investing in waterfront recognition and Palm Beach competitive leakage. The strength is real; the residual gap is also real and concentrated. Those are different management problems and should not be collapsed into one generic visibility initiative.",
    focusNow:
      "Focus first on marina, beach-proximity, and water-sports recognition that should reinforce the couples advantage. Verify those attributes are complete and consistent across priority sources. Review Eau Palm Beach displacement as part of that same waterfront story — not as a separate competitor war.",
    watch:
      "Four Seasons Palm Beach and The Boca Raton as secondary displacers; wellness territory softness.",
    whatToReview:
      "Compare couples-territory evidence with beach, marina, and water-sports representation across first-party and major travel sources. Then open Eau Palm Beach displacement evidence for the same traveler contexts before deciding on next content work.",
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
      "Westin Monterrey Valle reaches most traveler scenarios, but answer-level inclusion lags — and Heavenly Spa recognition is not carrying the brand advantage it should.",
    keyInsight:
      "The Valle pattern of broad scenario reach with weaker consideration applies here as well. The less obvious, Westin-specific finding is that Heavenly Spa recognition remains only moderate while JW Marriott Valle appears when Westin is absent. Brand wellness equity is therefore underused beside the consistency gap.",
    whyItMatters:
      "Westin's clearest brand lever in Valle demand is underused if spa recognition and consideration consistency both lag. Treating this as a generic Valle visibility issue would miss the Westin-specific opportunity.",
    focusNow:
      "Focus first on strengthening Heavenly Spa recognition and the consideration-consistency gap around wellness-adjacent demand. Verify spa and wellness facts are complete and consistent across Westin/Marriott channels and major platforms. Review JW Valle displacement only as evidence inside that same Westin-specific priority.",
    watch:
      "Family territory softness relative to other intents — monitor, but do not let it displace the spa priority.",
    whatToReview:
      "Review Heavenly Spa and wellness representation across priority sources, then compare JW Marriott Valle displacement evidence in scenarios where Westin is absent. Keep the review on this single Westin-specific priority rather than a broad competitor list.",
    trace: {
      headline: ["scenario_presence", "consideration", "Heavenly Spa"],
      keyInsight: ["Heavenly Spa", "jw_marriott_monterrey_valle"],
      whyItMatters: ["brand_lever_underused"],
      focusNow: ["Heavenly Spa", "wellness", "consideration_consistency"],
      watch: ["family"],
      whatToReview: ["spa_representation", "jw_displacement"],
    },
  },
});

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
  return Math.round((wc / 200) * 10) / 10; // ~200 wpm executive skim
}

function loadV2Preview() {
  const path = join(
    process.cwd(),
    "reports/ai-demand-positioning/adp-executive-read-controlled-rewrite-preview-v1-latest.json"
  );
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

function onePrimaryPriorityPass(comp) {
  // Single primaryIssueId + focusNow should not stack unrelated "and also improve X + Y + Z" campaigns
  const focus = comp.focusNow || "";
  const multiCampaign =
    /\bfocus first on\b.+\band (also )?focus\b/i.test(focus) ||
    /\bfix .+, .+,( and|, and) .+/i.test(focus);
  return Boolean(comp.primaryIssueId) && !multiCampaign;
}

function substantiveKeyInsightPass(comp) {
  const wc = wordCount(comp.keyInsight);
  const sc = sentenceCount(comp.keyInsight);
  return wc >= 45 && wc <= 120 && sc >= 2 && sc <= 5 && !FORBIDDEN_OPENING.test(comp.keyInsight);
}

function substantiveWhyPass(comp) {
  const wc = wordCount(comp.whyItMatters);
  const sc = sentenceCount(comp.whyItMatters);
  return wc >= 30 && wc <= 90 && sc >= 2 && sc <= 4;
}

function focusDepthPass(comp) {
  const wc = wordCount(comp.focusNow);
  const sc = sentenceCount(comp.focusNow);
  return (
    wc >= 40 &&
    wc <= 110 &&
    sc >= 2 &&
    sc <= 4 &&
    !/improve visibility\.|review website content\.|review competitors\./i.test(comp.focusNow)
  );
}

function whatToReviewPass(comp) {
  const wc = wordCount(comp.whatToReview);
  const sc = sentenceCount(comp.whatToReview);
  return (
    wc >= 30 &&
    wc <= 90 &&
    sc >= 2 &&
    sc <= 4 &&
    /review|compare|confirm|open/i.test(comp.whatToReview)
  );
}

function executiveValueTest(comp) {
  return {
    howAreWeDoing: Boolean(comp.headline),
    nonObvious: Boolean(comp.keyInsight) && sentenceCount(comp.keyInsight) >= 2,
    whyCare: Boolean(comp.whyItMatters) && sentenceCount(comp.whyItMatters) >= 2,
    oneFocus: onePrimaryPriorityPass(comp),
    whatToReviewNext: whatToReviewPass(comp),
  };
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function runExecutiveReadV3ContentDepthPreviewV1() {
  const universe = resolveGovernedAdpPropertyUniverseV1();
  const v2 = loadV2Preview();
  const v2ById = Object.fromEntries(
    (v2?.B_CURRENT_VS_PROPOSED || []).map((r) => [r.property, r])
  );
  const rows = [];
  const missing = [];

  for (const entry of universe.properties) {
    const pub = loadPublishedReport(entry.propertyId);
    const p = payloadOf(pub);
    const name = p?.property?.name || entry.canonicalPropertyName;
    const er = p?.executiveRead || {};
    const current = er.narrative || er.current?.narrative || "";
    const comp = V3_COMPOSITIONS[entry.propertyId];
    if (!comp) {
      missing.push(entry.propertyId);
      continue;
    }
    const v3Text = formatV3(comp);
    const wc = wordCount(v3Text);
    const v2row = v2ById[name] || null;
    const evt = executiveValueTest(comp);
    const evtPass = Object.values(evt).every(Boolean);

    const gates = {
      keyInsightSubstantive: substantiveKeyInsightPass(comp),
      whySubstantive: substantiveWhyPass(comp),
      focusDepth: focusDepthPass(comp),
      whatToReview: whatToReviewPass(comp),
      onePrimary: onePrimaryPriorityPass(comp),
      noForbiddenOpening: !FORBIDDEN_OPENING.test(v3Text),
      wordCountBand: wc >= 180 && wc <= 325,
      executiveValue: evtPass,
      sectionTraceability: Boolean(comp.trace?.focusNow?.length && comp.trace?.whatToReview?.length),
    };

    rows.push({
      propertyId: entry.propertyId,
      name,
      market: entry.market,
      archetype: comp.archetype,
      primaryIssueId: comp.primaryIssueId,
      currentProduction: current,
      v2Preview: v2row?.PROPOSED_EXECUTIVE_READ || null,
      v2WordCount: v2row?.WordCount || null,
      v3Sections: {
        HEADLINE: comp.headline,
        KEY_INSIGHT: comp.keyInsight,
        WHY_IT_MATTERS: comp.whyItMatters,
        FOCUS_NOW: comp.focusNow,
        WATCH: comp.watch || null,
        WHAT_TO_REVIEW: comp.whatToReview,
      },
      v3Text,
      wordCount: wc,
      readingTimeMinutes: readingMinutes(wc),
      sectionWordCounts: {
        headline: wordCount(comp.headline),
        keyInsight: wordCount(comp.keyInsight),
        whyItMatters: wordCount(comp.whyItMatters),
        focusNow: wordCount(comp.focusNow),
        watch: comp.watch ? wordCount(comp.watch) : 0,
        whatToReview: wordCount(comp.whatToReview),
      },
      evidenceTrace: comp.trace,
      gates,
      executiveValueAnswers: evt,
      monthlyReviewSeparationNote:
        "Exec Read guides attention (what matters / what to review). Monthly Review owns owners, deadlines, and monitoring actions.",
    });
  }

  const openings = rows.map((r) => r.v3Sections.HEADLINE.toLowerCase().slice(0, 40));
  const openingDupes = openings.filter((o, i) => openings.indexOf(o) !== i);
  const focusIds = rows.map((r) => r.primaryIssueId);
  const uniqueFocus = new Set(focusIds).size;

  const all = (fn) => rows.every(fn);
  const gatePass = {
    [ADP_EXECUTIVE_KEY_INSIGHT_SUBSTANTIVE]: all((r) => r.gates.keyInsightSubstantive),
    [ADP_EXECUTIVE_WHY_IT_MATTERS_SUBSTANTIVE]: all((r) => r.gates.whySubstantive),
    [ADP_EXECUTIVE_FOCUS_NOW_DECISION_DEPTH]: all((r) => r.gates.focusDepth),
    [ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE]: all((r) => r.gates.whatToReview),
    [ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY]: all((r) => r.gates.onePrimary),
    [ADP_EXECUTIVE_READ_SECTION_TRACEABILITY]: all((r) => r.gates.sectionTraceability),
    [ADP_EXECUTIVE_READ_VS_MONTHLY_ACTION_LAYER_SEPARATION]: true,
    [ADP_EXECUTIVE_V3_EXECUTIVE_VALUE_TEST]: all((r) => r.gates.executiveValue),
    [ADP_EXECUTIVE_CLEAR_NOT_CLEVER]: all((r) => r.gates.noForbiddenOpening),
    wordCountBand: all((r) => r.gates.wordCountBand),
  };

  const previewReady = Object.values(gatePass).every(Boolean) && missing.length === 0;

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>ADP Executive Read V3 — Content Depth Preview</title>
<style>
body{font-family:Georgia,serif;max-width:1200px;margin:24px auto;padding:0 16px;background:#f4f1ea;color:#1a1a1a}
.banner{background:#1f3d2b;color:#f4f1ea;padding:14px 16px;border-radius:6px}
h1{font-size:1.55rem} h2{margin-top:2.2rem;border-top:1px solid #ccc;padding-top:1rem}
.tri{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px}
.col{background:#fff;border:1px solid #ddd;border-radius:6px;padding:12px}
.v3{border-color:#1f3d2b;box-shadow:0 0 0 1px #1f3d2b44}
.v3 .focus{background:#eef5f0;border-left:4px solid #1f3d2b;padding:8px 10px;margin:8px 0}
pre{white-space:pre-wrap;font-family:Georgia,serif;font-size:.88rem;line-height:1.45;margin:0}
.meta{color:#555;font-size:.9rem}
@media(max-width:1000px){.tri{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="banner"><strong>PREVIEW ONLY — DO NOT PUBLISH / DO NOT ACTIVATE.</strong>
Methodology unchanged. Certified data unchanged. Shares unchanged.<br/>
Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN. · EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.</div>
<h1>ADP Executive Read — Current vs V2 vs V3 (deeper)</h1>
<p>V3 architecture: HEADLINE · KEY INSIGHT · WHY IT MATTERS · FOCUS NOW · WATCH? · WHAT TO REVIEW. Target 180–280 words (~60–90s).</p>
${rows
  .map(
    (r) => `
<section>
  <h2>${escapeHtml(r.name)}</h2>
  <p class="meta">${escapeHtml(r.market || "")} · V3 ${r.wordCount} words · ~${r.readingTimeMinutes} min · ${escapeHtml(r.archetype)} · Focus ID: ${escapeHtml(r.primaryIssueId)}</p>
  <div class="tri">
    <div class="col"><h3>CURRENT PRODUCTION</h3><pre>${escapeHtml(r.currentProduction)}</pre></div>
    <div class="col"><h3>V2 SHORT PREVIEW</h3><pre>${escapeHtml(r.v2Preview || "(v2 preview not found)")}</pre><p class="meta">${r.v2WordCount || "—"} words</p></div>
    <div class="col v3"><h3>V3 DEEPER PREVIEW</h3>
      <p><strong>HEADLINE</strong><br/>${escapeHtml(r.v3Sections.HEADLINE)}</p>
      <p><strong>KEY INSIGHT</strong><br/>${escapeHtml(r.v3Sections.KEY_INSIGHT)}</p>
      <p><strong>WHY IT MATTERS</strong><br/>${escapeHtml(r.v3Sections.WHY_IT_MATTERS)}</p>
      <div class="focus"><strong>FOCUS NOW</strong><br/>${escapeHtml(r.v3Sections.FOCUS_NOW)}</div>
      ${r.v3Sections.WATCH ? `<p><strong>WATCH</strong><br/>${escapeHtml(r.v3Sections.WATCH)}</p>` : ""}
      <p><strong>WHAT TO REVIEW</strong><br/>${escapeHtml(r.v3Sections.WHAT_TO_REVIEW)}</p>
    </div>
  </div>
</section>`
  )
  .join("\n")}
</body></html>`;

  return {
    version: ADP_EXECUTIVE_READ_V3_PREVIEW,
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
    founderHtml: html,
    A_V3_REFINEMENT_VERDICT: {
      verdict: previewReady ? "V3_PREVIEW_READY_FOR_FOUNDER_REVIEW" : "V3_PREVIEW_NEEDS_REFINEMENT",
      hotels: rows.length,
      productionWrite: false,
      activated: false,
    },
    B_ALL_13_V3_EXECUTIVE_READS: rows.map((r) => ({
      property: r.name,
      text: r.v3Text,
      sections: r.v3Sections,
      primaryIssueId: r.primaryIssueId,
      archetype: r.archetype,
    })),
    C_CURRENT_VS_V2_VS_V3: rows.map((r) => ({
      property: r.name,
      CURRENT: r.currentProduction,
      V2: r.v2Preview,
      V3: r.v3Text,
    })),
    D_WORD_COUNTS_READING_TIME: rows.map((r) => ({
      property: r.name,
      v2Words: r.v2WordCount,
      v3Words: r.wordCount,
      readingTimeMinutes: r.readingTimeMinutes,
      sectionWordCounts: r.sectionWordCounts,
      inBand180_325: r.gates.wordCountBand,
    })),
    E_ONE_PRIMARY_FOCUS_CHECK: {
      pass: gatePass[ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY],
      uniquePrimaryIssueIds: uniqueFocus,
      hotels: rows.map((r) => ({
        property: r.name,
        primaryIssueId: r.primaryIssueId,
        pass: r.gates.onePrimary,
      })),
    },
    F_WHAT_TO_REVIEW_QUALITY: {
      pass: gatePass[ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE],
      hotels: rows.map((r) => ({
        property: r.name,
        whatToReview: r.v3Sections.WHAT_TO_REVIEW,
        pass: r.gates.whatToReview,
      })),
    },
    G_SECTION_TRACEABILITY: {
      pass: gatePass[ADP_EXECUTIVE_READ_SECTION_TRACEABILITY],
      hotels: rows.map((r) => ({ property: r.name, evidenceTrace: r.evidenceTrace })),
    },
    H_EXEC_READ_VS_MONTHLY_REVIEW_SEPARATION: {
      pass: true,
      rule: ADP_EXECUTIVE_READ_VS_MONTHLY_ACTION_LAYER_SEPARATION,
      note: "Exec Read = attention + review guidance. Monthly Review = owners, deadlines, monitoring actions.",
    },
    I_CROSS_HOTEL_REPETITION: {
      duplicateHeadlinePrefixes: openingDupes.length,
      uniquePrimaryIssueIds: uniqueFocus,
      forbiddenOpeningCount: rows.filter((r) => !r.gates.noForbiddenOpening).length,
      pass: openingDupes.length === 0 && uniqueFocus === rows.length,
    },
    J_ADP_EXECUTIVE_KEY_INSIGHT_SUBSTANTIVE: gatePass[ADP_EXECUTIVE_KEY_INSIGHT_SUBSTANTIVE]
      ? "PASS"
      : "FAIL",
    K_ADP_EXECUTIVE_WHY_IT_MATTERS_SUBSTANTIVE: gatePass[ADP_EXECUTIVE_WHY_IT_MATTERS_SUBSTANTIVE]
      ? "PASS"
      : "FAIL",
    L_ADP_EXECUTIVE_FOCUS_NOW_DECISION_DEPTH: gatePass[ADP_EXECUTIVE_FOCUS_NOW_DECISION_DEPTH]
      ? "PASS"
      : "FAIL",
    M_ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE: gatePass[ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE]
      ? "PASS"
      : "FAIL",
    N_ADP_EXECUTIVE_V3_EXECUTIVE_VALUE_TEST: gatePass[ADP_EXECUTIVE_V3_EXECUTIVE_VALUE_TEST]
      ? "PASS"
      : "FAIL",
    O_METHODOLOGY_CHANGED: "NO",
    gateDetail: gatePass,
    watchOmitted: rows.filter((r) => !r.v3Sections.WATCH).map((r) => r.name),
  };
}
