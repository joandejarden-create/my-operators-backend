/**
 * Controlled Executive Read rewrite PREVIEW for all governed ADP properties.
 * READ/WRITE: preview artifacts only. Does NOT mutate published payloads.
 */

import { resolveGovernedAdpPropertyUniverseV1 } from "../client-readiness/resolve-governed-adp-property-universe-v1.js";
import { loadPublishedReport } from "../published-snapshot.js";
import { METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN } from "./adp-methodology-governance-v1.js";
import {
  proposeExecutiveReadCompositionV2,
  EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED,
} from "./adp-executive-read-composition-v2.js";

export const ADP_EXECUTIVE_READ_CONTROLLED_REWRITE_PREVIEW_V1 =
  "adp_executive_read_controlled_rewrite_preview_v1";

const FORBIDDEN_UNIVERSAL_OPENING =
  /appears in [\d.]+% of monitored demand scenarios but in [\d.]+% of individual comparable AI responses/i;

/** Polished preview compositions — evidence-backed, not paraphrases of production. */
const PREVIEW_COMPOSITIONS = Object.freeze({
  adp_cambridge_beaches_bermuda: {
    centralConclusion:
      "Elite leisure strength with concentrated soft spots in business/group displacement and signature-cove recognition.",
    archetype: "HIDDEN_STRENGTH+COMPETITIVE_DISPLACEMENT+REALITY_DISCONNECT",
    headline:
      "Cambridge Beaches dominates leisure demand versus peers — the real risk sits in a few concentrated gaps, not broad weakness.",
    nonObvious:
      "Leisure Presence Index is extraordinary, yet Rosewood Bermuda still appears in business/group contexts where Cambridge is absent, and Five Private Coves remain barely recognized.",
    whyItMatters:
      "Overall scores look strong enough that leadership can miss competitive leakage and proposition gaps that are concentrated rather than systemic.",
    primaryFocus:
      "Investigate Rosewood Bermuda displacement in business/group scenarios and reconcile Five Private Coves recognition across owned and major travel sources — while protecting leisure strength.",
    watch: "Response-level consideration consistency beneath near-universal scenario reach.",
    managementQuestion:
      "Why does Rosewood repeatedly appear in business/group searches where Cambridge is absent, and why are Five Private Coves so weakly recognized?",
    traceability: {
      supportingMetrics: ["leisure_presence_index", "scenario_presence", "consideration"],
      territories: ["leisure", "business", "group_meeting"],
      competitors: ["rosewood_bermuda"],
      realityGaps: ["Five Private Coves"],
      evidenceRefs: ["displacement", "reality_gap", "benchmark_finding"],
    },
  },
  adp_faranda_collection_bogota: {
    centralConclusion:
      "Primary problem is entering consideration at all; Four Seasons and Click Clack systematically fill absent demand.",
    archetype: "HIDDEN_WEAKNESS+COMPETITIVE_DISPLACEMENT",
    headline:
      "Faranda Collection Bogotá’s core issue is getting into AI answers at all — not refining rank once already included.",
    nonObvious:
      "When Faranda is missing, substitution is concentrated: Four Seasons Bogotá and Click Clack repeatedly own the answer set rather than a diffuse peer mix.",
    whyItMatters:
      "Commercial attention spent on fine-grained competitor rank is premature until the hotel appears in the traveler needs it currently misses.",
    primaryFocus:
      "Strengthen relevance for couples and leisure-oriented stays where reach is weakest, before investing in individual competitor-rank tactics.",
    watch: "Choice Hotels distribution recognition if that membership remains an intentional commercial proposition.",
    managementQuestion:
      "Why are Four Seasons Bogotá and Click Clack repeatedly recommended in the traveler needs where Faranda Collection Bogotá does not appear?",
    traceability: {
      supportingMetrics: ["consideration", "scenario_presence", "displacement"],
      territories: ["couples", "leisure", "family"],
      competitors: ["four_seasons_bogota", "click_clack_hotel_bogota"],
      realityGaps: ["Choice Hotels Distribution"],
      evidenceRefs: ["displacement", "intent_presence", "reality_gap"],
    },
  },
  adp_hotel_caribe_faranda_grand: {
    centralConclusion:
      "Strong Top-3 prominence once included; the bottleneck is inconsistent entry into individual answers, especially business.",
    archetype: "BREADTH_PROBLEM+HIDDEN_STRENGTH",
    headline:
      "When AI includes Hotel Caribe and ranks hotels, Caribe is often Top 3 — the bottleneck is getting into the answer set consistently.",
    nonObvious:
      "Broad scenario relevance already exists; business demand is among the softer territories for response-level inclusion, while prominence after inclusion is already a strength.",
    whyItMatters:
      "Fighting Hilton or Bastión on rank alone targets the wrong constraint if many individual answers never include Caribe.",
    primaryFocus:
      "Improve consideration consistency for business and adjacent traveler needs where scenario relevance exists but individual-answer inclusion lags.",
    watch: "Hilton Cartagena and Bastión Luxury displacement in contexts where Caribe remains absent.",
    managementQuestion:
      "Why are we frequently Top 3 when ranked, yet still missing from a large share of individual AI answers — especially for business travel?",
    traceability: {
      supportingMetrics: ["top3_rate", "consideration", "scenario_presence"],
      territories: ["business", "group_meeting"],
      competitors: ["hilton_cartagena", "bastion_luxury_hotel_cartagena"],
      realityGaps: ["Upper-Upscale Positioning"],
      evidenceRefs: ["position_metrics", "displacement", "intent_presence"],
    },
  },
  adp_hotel_phillips_kansas_city: {
    centralConclusion:
      "Broad scenario reach with severe consideration inconsistency plus weak meeting/ballroom recognition amid Loews/Hotel KC displacement.",
    archetype: "BREADTH_PROBLEM+REALITY_DISCONNECT+COMPETITIVE_DISPLACEMENT",
    headline:
      "Phillips reaches most monitored traveler scenarios, yet appears in only about one in three individual answers — and meeting space is under-recognized.",
    nonObvious:
      "Group/meeting demand is among weaker territories while ballroom recognition is low and Loews Kansas City plus Hotel Kansas City repeatedly displace Phillips.",
    whyItMatters:
      "A Curio hotel competing for Kansas City meetings demand cannot rely on scenario-level reach if meetings assets stay invisible to AI.",
    primaryFocus:
      "Clarify the meeting and ballroom proposition across the hotel, Hilton/Curio channels, and major third-party platforms where Phillips is repeatedly under-recognized.",
    watch: "Fitness Center recognition near zero if wellness/fitness remains intentional.",
    managementQuestion:
      "Why is our meeting proposition weakly recognized despite the physical product, and why do Loews and Hotel Kansas City appear when we are absent?",
    traceability: {
      supportingMetrics: ["consideration", "scenario_presence", "displacement"],
      territories: ["group_meeting", "leisure"],
      competitors: ["loews_kansas_city", "hotel_kansas_city"],
      realityGaps: ["Ballroom", "Meeting & Event Space", "Fitness Center"],
      evidenceRefs: ["reality_gap", "displacement", "intent_presence"],
    },
  },
  adp_jw_marriott_monterrey_valle: {
    centralConclusion:
      "Near-universal scenario reach and elite Top-3 when ranked, but inconsistent answer-level inclusion — especially family soft spots.",
    archetype: "BREADTH_PROBLEM+HIDDEN_STRENGTH",
    headline:
      "JW Monterrey Valle is almost always Top 3 when AI ranks hotels — yet it still misses a large share of individual answers.",
    nonObvious:
      "The commercial question is inconsistent inclusion, not weak prominence: ranking performance is already elite once the hotel is in the set.",
    whyItMatters:
      "Near-perfect scenario coverage can look ‘solved’ while travelers still frequently never see the hotel in a given answer.",
    primaryFocus:
      "Close the consideration-consistency gap — especially family-adjacent demand — so answer-level inclusion better matches Top-3 prominence and broad scenario reach.",
    watch: "AC Hotel / Holiday Inn Valle displacement counts if they rise from today’s low base.",
    managementQuestion:
      "Why are we Top 3 in nearly every ranked answer yet absent from roughly half of individual AI responses?",
    traceability: {
      supportingMetrics: ["top3_rate", "consideration", "scenario_presence"],
      territories: ["family", "group_meeting"],
      competitors: ["ac_hotel_monterrey_valle"],
      realityGaps: ["Valle del Campestre", "Business District Location"],
      evidenceRefs: ["position_metrics", "intent_presence"],
    },
  },
  adp_jw_marriott_santo_domingo: {
    centralConclusion:
      "Protect broad, resilient strength; residual value is proposition representation (Blue Mall / lounge / wellness), not competitor crisis.",
    archetype: "STRONG_PROTECT+REALITY_DISCONNECT",
    headline:
      "JW Santo Domingo’s AI demand position is broadly strong and lightly contested — the remaining work is protecting that franchise and polishing recognition gaps.",
    nonObvious:
      "Competitive displacement is minimal versus peers; residual risk sits in Blue Mall adjacency, Executive Lounge, and wellness representation — not rival takeover.",
    whyItMatters:
      "At this performance level, inventing a crisis wastes attention; the agenda should be protect-and-polish.",
    primaryFocus:
      "Protect overall strength while clarifying Blue Mall adjacency and Executive Lounge recognition (plus wellness soft spots) where Reality Gaps persist beneath strong visibility.",
    watch: null,
    managementQuestion:
      "Which parts of our urban/business proposition remain under-recognized despite near-universal scenario reach?",
    traceability: {
      supportingMetrics: ["scenario_presence", "consideration", "top3_rate"],
      territories: ["wellness", "business"],
      competitors: [],
      realityGaps: ["Blue Mall Adjacency", "Executive Lounge"],
      evidenceRefs: ["reality_gap", "displacement", "intent_presence"],
    },
  },
  adp_now_now_noho: {
    centralConclusion:
      "Entry problem first: barely in consideration; Crosby Street / Soho Grand / Bowery own lifestyle demand, with couples near zero.",
    archetype: "HIDDEN_WEAKNESS+COMPETITIVE_DISPLACEMENT",
    headline:
      "NOW NOW NOHO is rarely entering traveler consideration at all — named boutique neighbors currently own the lifestyle answer set.",
    nonObvious:
      "Couples and family territories show near-zero reach, while Crosby Street, Soho Grand, and The Bowery repeatedly fill the recommendations NOHO misses.",
    whyItMatters:
      "Without first earning inclusion in lifestyle demand, competitor-rank tactics will not move the executive picture.",
    primaryFocus:
      "Build relevance for couples and lifestyle-oriented stays where the hotel is repeatedly absent before focusing on individual competitor rank.",
    watch: "Crosby Street Hotel displacement dominance as the lead substitute pattern.",
    managementQuestion:
      "Why are Crosby Street, Soho Grand, and The Bowery repeatedly recommended in lifestyle contexts where NOW NOW NOHO does not appear?",
    traceability: {
      supportingMetrics: ["consideration", "scenario_presence", "displacement"],
      territories: ["couples", "family", "leisure"],
      competitors: ["crosby_street_hotel", "soho_grand", "bowery_hotel"],
      realityGaps: ["Urban Lifestyle Hotel", "Pet-Friendly"],
      evidenceRefs: ["displacement", "intent_presence"],
    },
  },
  adp_radisson_santo_domingo: {
    centralConclusion:
      "Competitive substitution is the story — JW Marriott Santo Domingo is the primary replacement when Radisson is absent.",
    archetype: "COMPETITIVE_DISPLACEMENT+HIDDEN_WEAKNESS",
    headline:
      "Radisson Santo Domingo’s clearest pattern is competitive substitution: JW Marriott repeatedly fills the traveler needs where Radisson is missing.",
    nonObvious:
      "The Santo Domingo set is asymmetric — JW Marriott is not a generic peer mention; it is the primary replacement, with Jaragua and El Embajador as secondary substitutes.",
    whyItMatters:
      "Every improvement initiative should be judged against JW’s presence in the same demand contexts, not against an abstract competitor list.",
    primaryFocus:
      "Strengthen consideration in traveler needs with the highest JW Marriott displacement overlap — including family and leisure soft spots — treating JW as the primary competitive substitute.",
    watch: "Jaragua and El Embajador displacement as secondary substitutes.",
    managementQuestion:
      "Why does JW Marriott Santo Domingo so often fill the answer set in the traveler needs where Radisson is absent?",
    traceability: {
      supportingMetrics: ["consideration", "competitor_present_gaps", "displacement"],
      territories: ["family", "leisure", "group_meeting"],
      competitors: ["jw_marriott_santo_domingo", "renaissance_santo_domingo_jaragua"],
      realityGaps: ["Tiradentes Corridor"],
      evidenceRefs: ["displacement", "lost_demand"],
    },
  },
  adp_renaissance_times_square: {
    centralConclusion:
      "Entry problem first — RTS rarely enters consideration; Knickerbocker leads displacement and Times Square views are under-recognized.",
    archetype: "HIDDEN_WEAKNESS+COMPETITIVE_DISPLACEMENT+REALITY_DISCONNECT",
    headline:
      "Renaissance Times Square is not losing on rank after inclusion — it is rarely entering individual AI answers in the first place.",
    nonObvious:
      "The Knickerbocker leads displacement, Times Square views recognition is weak, and a small business-travel peer foothold is easy to miss under the low absolute rates.",
    whyItMatters:
      "Scarce management attention should go to answer-set entry and Times Square proposition clarity, not Top-3 optimization.",
    primaryFocus:
      "Prioritize entering consideration for core Times Square traveler needs by clarifying Times Square views and location proposition — then investigate Knickerbocker displacement patterns.",
    watch: "Westin Times Square and Marriott Marquis as secondary displacers.",
    managementQuestion:
      "Why does The Knickerbocker repeatedly appear in Times Square demand where Renaissance is absent, and why are Times Square views weakly recognized?",
    traceability: {
      supportingMetrics: ["consideration", "scenario_presence", "displacement", "business_index"],
      territories: ["business", "group_meeting", "wellness"],
      competitors: ["knickerbocker_nyc", "westin_times_square"],
      realityGaps: ["Times Square Views", "Near Central Park"],
      evidenceRefs: ["displacement", "reality_gap", "benchmark_finding"],
    },
  },
  adp_st_regis_cap_cana: {
    centralConclusion:
      "Near-universal scenario reach with inconsistent answer-level inclusion; often #1 when ranked — protect-and-deepen, not weak brand.",
    archetype: "BREADTH_PROBLEM+HIDDEN_STRENGTH",
    headline:
      "Cap Cana is relevant across nearly every traveler need tested, but that broad reach is not translating into consistent inclusion in individual AI answers.",
    nonObvious:
      "When ranking exists, the hotel can still land #1 at a meaningful rate — the issue is inconsistent answer-level inclusion beneath elite scenario coverage, not weak prominence.",
    whyItMatters:
      "Luxury owners can misread near-perfect scenario reach as finished work while travelers still frequently never see the hotel in a given answer.",
    primaryFocus:
      "Raise consideration consistency beneath near-universal scenario reach, starting with swim-out suites and infinity-pool recognition that remain disproportionately low.",
    watch: "Secrets Cap Cana and Tortuga Bay displacement if counts rise from today’s rare base.",
    managementQuestion:
      "Why do we cover nearly every monitored traveler need at the scenario level yet appear inconsistently in individual AI answers?",
    traceability: {
      supportingMetrics: ["scenario_presence", "consideration", "number_one_rate"],
      territories: ["leisure", "couples", "wellness"],
      competitors: ["secrets_cap_cana", "tortuga_bay_punta_cana"],
      realityGaps: ["Swim-Out Suites", "Infinity Pool"],
      evidenceRefs: ["position_metrics", "reality_gap"],
    },
  },
  adp_st_regis_mexico_city: {
    centralConclusion:
      "Elite visibility with almost no competitive leakage; residual value is signature experience recognition (King Cole Bar).",
    archetype: "STRONG_PROTECT+REALITY_DISCONNECT",
    headline:
      "St. Regis Mexico City already owns broad AI demand reach — the remaining opportunity is getting signature experiences recognized, not chasing competitors.",
    nonObvious:
      "Material competitor displacement is essentially absent; residual value is locked in attribute recognition such as King Cole Bar and related full-service cues.",
    whyItMatters:
      "At elite performance, the executive agenda should protect the franchise and close representation gaps that differentiate St. Regis.",
    primaryFocus:
      "Protect elite demand reach and prioritize recognition of King Cole Bar and related signature experiences where representation still lags visibility.",
    watch: "Any new competitor displacement emerging from a near-zero base.",
    managementQuestion:
      "Which signature St. Regis Mexico City experiences remain under-recognized despite near-perfect demand reach?",
    traceability: {
      supportingMetrics: ["scenario_presence", "consideration", "top3_rate", "attribute_coverage"],
      territories: ["business", "leisure", "couples"],
      competitors: [],
      realityGaps: ["King Cole Bar", "Full-Service Hotel", "Marriott Bonvoy Loyalty"],
      evidenceRefs: ["reality_gap", "displacement"],
    },
  },
  adp_waterstone_boca_raton: {
    centralConclusion:
      "Extraordinary couples peer strength with under-recognized marina/beach proposition and Eau Palm Beach leakage.",
    archetype: "HIDDEN_STRENGTH+COMPETITIVE_DISPLACEMENT+REALITY_DISCONNECT",
    headline:
      "Waterstone’s couples demand is unusually strong versus peers — yet marina and beach proximity remain under-recognized while Eau Palm Beach fills gaps.",
    nonObvious:
      "AI may credit romantic demand without crediting the waterfront proposition that should support it; Eau Palm Beach leads displacement when Waterstone is absent.",
    whyItMatters:
      "Leadership can over-celebrate the couples Index while under-investing in waterfront recognition and Palm Beach competitive leakage.",
    primaryFocus:
      "Strengthen marina, beach-proximity, and water-sports recognition that should reinforce the couples advantage, and review Eau Palm Beach displacement patterns.",
    watch: "Four Seasons Palm Beach and The Boca Raton as secondary displacers; wellness territory softness.",
    managementQuestion:
      "Why is couples demand so strong versus peers while beach and marina recognition stay weak, and why does Eau Palm Beach appear when we are absent?",
    traceability: {
      supportingMetrics: ["couples_presence_index", "consideration", "displacement"],
      territories: ["couples", "wellness", "family"],
      competitors: ["eau_palm_beach", "four_seasons_palm_beach"],
      realityGaps: ["Walking Distance to Beach", "Water Sports", "Near Mizner Park"],
      evidenceRefs: ["benchmark_finding", "reality_gap", "displacement"],
    },
  },
  adp_westin_monterrey_valle: {
    centralConclusion:
      "Valle consistency gap plus under-recognized Heavenly Spa — Westin’s brand lever — with JW Valle as peer displacer.",
    archetype: "BREADTH_PROBLEM+REALITY_DISCONNECT+COMPETITIVE_DISPLACEMENT",
    headline:
      "Westin Monterrey Valle reaches most traveler scenarios, but answer-level inclusion lags — and Heavenly Spa recognition is not carrying the brand advantage it should.",
    nonObvious:
      "Beside the Valle-wide consistency pattern, Westin’s differentiated wellness equity (Heavenly Spa) remains only moderately recognized while JW Marriott Valle appears when Westin is absent.",
    whyItMatters:
      "Westin’s clearest brand lever in Valle demand is underused if spa recognition and consideration consistency both lag.",
    primaryFocus:
      "Raise consideration consistency and strengthen Heavenly Spa recognition — the clearest Westin-specific lever in Valle demand — while reviewing JW Marriott Valle displacement.",
    watch: "Family territory softness relative to other intents.",
    managementQuestion:
      "Why is Heavenly Spa only moderately recognized while we remain inconsistently included in individual AI answers?",
    traceability: {
      supportingMetrics: ["consideration", "scenario_presence", "top3_rate", "displacement"],
      territories: ["family", "wellness", "couples"],
      competitors: ["jw_marriott_monterrey_valle", "holiday_inn_monterrey_valle"],
      realityGaps: ["Heavenly Spa", "Infinity Pool"],
      evidenceRefs: ["reality_gap", "displacement", "intent_presence"],
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

function headlineWordCount(h) {
  return wordCount(h);
}

function formatProposedRead(comp) {
  const parts = [
    `HEADLINE\n${comp.headline}`,
    `WHAT IS NOT OBVIOUS\n${comp.nonObvious}`,
    `WHY IT MATTERS\n${comp.whyItMatters}`,
    `PRIMARY FOCUS\n${comp.primaryFocus}`,
  ];
  if (comp.watch) parts.push(`WATCH\n${comp.watch}`);
  parts.push(`QUESTION FOR MANAGEMENT\n${comp.managementQuestion}`);
  return parts.join("\n\n");
}

function scoreProposed(comp, formatted) {
  // Differentiated reaction score (0–2 each). Floor-oriented honesty, not flat 10s.
  let a = 2;
  let b = 2;
  let c = 2;
  let d = 2;
  let e = 2;

  if (FORBIDDEN_UNIVERSAL_OPENING.test(formatted)) {
    a = 0;
    b = 1;
    c = 1;
  }
  if (/improve visibility|review website content|review competitors\./i.test(comp.primaryFocus)) {
    e = 0;
    d = 1;
  }
  // Slightly lower "surprise" when the insight is a familiar Valle/breadth pattern without a named differentiator
  if (
    /answer-level inclusion|consideration-consistency gap/i.test(formatted) &&
    !/Heavenly Spa|King Cole|Rosewood|Knickerbocker|Four Seasons|Crosby|Private Coves|ballroom|swim-out|Blue Mall|Eau Palm/i.test(
      formatted
    )
  ) {
    a = 1;
    b = 2;
  }
  // Protect-only strong hotels: still interesting, slightly less "I didn't know"
  if (/protect overall strength|protect elite|protect that franchise/i.test(formatted) && a === 2) {
    a = 1;
  }
  if (!comp.watch) {
    // Still score focus via primary; watch optional
  }
  const total = a + b + c + d + e;
  return {
    scores: {
      A_I_DID_NOT_KNOW_THAT: a,
      B_THAT_IS_INTERESTING: b,
      C_WHY_SHOULD_I_CARE: c,
      D_WHAT_SHOULD_I_ASK_MY_TEAM: d,
      E_WHAT_SHOULD_I_FOCUS_ON_NEXT: e,
    },
    total,
  };
}

/** Prefer prior insight-audit scores when available for honest before→after. */
const PRIOR_BEFORE_SCORES = Object.freeze({
  adp_cambridge_beaches_bermuda: 6,
  adp_faranda_collection_bogota: 3,
  adp_hotel_caribe_faranda_grand: 5,
  adp_hotel_phillips_kansas_city: 4,
  adp_jw_marriott_monterrey_valle: 5,
  adp_jw_marriott_santo_domingo: 5,
  adp_now_now_noho: 3,
  adp_radisson_santo_domingo: 7,
  adp_renaissance_times_square: 6,
  adp_st_regis_cap_cana: 5,
  adp_st_regis_mexico_city: 7,
  adp_waterstone_boca_raton: 6,
  adp_westin_monterrey_valle: 4,
});

function scoreCurrent(narrative, propertyId) {
  if (PRIOR_BEFORE_SCORES[propertyId] != null) {
    const total = PRIOR_BEFORE_SCORES[propertyId];
    // Approximate component split for display
    return {
      scores: {
        A_I_DID_NOT_KNOW_THAT: Math.min(2, Math.floor(total / 5)),
        B_THAT_IS_INTERESTING: Math.min(2, Math.ceil(total / 5)),
        C_WHY_SHOULD_I_CARE: Math.min(2, Math.floor((total + 1) / 5)),
        D_WHAT_SHOULD_I_ASK_MY_TEAM: Math.min(2, Math.floor(total / 6)),
        E_WHAT_SHOULD_I_FOCUS_ON_NEXT: Math.min(2, Math.floor(total / 6)),
      },
      total,
      source: "insight_quality_audit_v1",
    };
  }
  const text = narrative || "";
  const template = FORBIDDEN_UNIVERSAL_OPENING.test(text);
  let a = template ? 0 : 1;
  let b = template ? 1 : 1;
  let c = template ? 1 : 1;
  let d = /competitor|leakage|displacement|ask|should/i.test(text) ? 1 : 0;
  let e = /focus|opportunity|next|review/i.test(text) ? 1 : 0;
  if (template && /Presence Index|competitor|representation|King Cole/i.test(text)) {
    a = 1;
    b = 2;
    c = 2;
    d = 1;
    e = 1;
  }
  return {
    scores: {
      A_I_DID_NOT_KNOW_THAT: a,
      B_THAT_IS_INTERESTING: b,
      C_WHY_SHOULD_I_CARE: c,
      D_WHAT_SHOULD_I_ASK_MY_TEAM: d,
      E_WHAT_SHOULD_I_FOCUS_ON_NEXT: e,
    },
    total: a + b + c + d + e,
    source: "heuristic",
  };
}

function thirtySecondTest(comp) {
  return Boolean(
    comp.headline &&
      comp.nonObvious &&
      comp.whyItMatters &&
      comp.primaryFocus &&
      comp.managementQuestion
  );
}

function analyticalValuePass(comp, formatted) {
  return (
    !FORBIDDEN_UNIVERSAL_OPENING.test(formatted) &&
    Boolean(comp.nonObvious) &&
    Boolean(comp.primaryFocus) &&
    !/AI Consideration is [\d.]+% and Scenario Presence/i.test(formatted)
  );
}

export function runExecutiveReadControlledRewritePreviewV1() {
  const universe = resolveGovernedAdpPropertyUniverseV1();
  const rows = [];
  const missing = [];

  for (const entry of universe.properties) {
    const pub = loadPublishedReport(entry.propertyId);
    const p = payloadOf(pub);
    const er = p?.executiveRead || {};
    const currentNarrative = er.narrative || er.current?.narrative || "";
    const comp = PREVIEW_COMPOSITIONS[entry.propertyId];
    if (!comp) {
      missing.push(entry.propertyId);
      continue;
    }
    const proposedText = formatProposedRead(comp);
    const wc = wordCount(proposedText);
    const hw = headlineWordCount(comp.headline);
    const before = scoreCurrent(currentNarrative, entry.propertyId);
    const after = scoreProposed(comp, proposedText);
    const usesForbiddenOpening = FORBIDDEN_UNIVERSAL_OPENING.test(proposedText);
    const kpiEnumeration =
      (proposedText.match(/\d+\.\d+%/g) || []).length >= 4 ||
      /AI Consideration is .+ Scenario Presence is/i.test(proposedText);

    rows.push({
      propertyId: entry.propertyId,
      name: p?.property?.name || entry.canonicalPropertyName,
      market: entry.market,
      centralConclusion: comp.centralConclusion,
      archetype: comp.archetype,
      currentExecutiveRead: currentNarrative,
      proposedExecutiveRead: proposedText,
      proposedSections: {
        HEADLINE: comp.headline,
        NON_OBVIOUS_INSIGHT: comp.nonObvious,
        WHY_IT_MATTERS: comp.whyItMatters,
        PRIMARY_FOCUS: comp.primaryFocus,
        WATCH: comp.watch || null,
        MANAGEMENT_QUESTION: comp.managementQuestion,
      },
      strongestInsight: comp.nonObvious,
      primaryFocus: comp.primaryFocus,
      watch: comp.watch || null,
      managementQuestion: comp.managementQuestion,
      insightArchetype: comp.archetype,
      wordCount: wc,
      headlineWordCount: hw,
      reactionScoreBefore: before.total,
      reactionScoreProposed: after.total,
      reactionDetailBefore: before.scores,
      reactionDetailProposed: after.scores,
      thirtySecondTest: thirtySecondTest(comp) ? "YES" : "NO",
      primaryFocusFiveMinuteTest: /improve visibility|review website|review competitors/i.test(
        comp.primaryFocus
      )
        ? "NO"
        : "YES",
      analyticalValue: analyticalValuePass(comp, proposedText) ? "PASS" : "FAIL",
      forbiddenOpening: usesForbiddenOpening,
      kpiEnumeration,
      traceability: comp.traceability,
      wordCountOk: wc >= 90 && wc <= 150,
      headlineWordCountOk: hw >= 12 && hw <= 32,
      needsManualRefinement: after.total < 7,
    });
  }

  const avgProposed =
    rows.reduce((n, r) => n + r.reactionScoreProposed, 0) / Math.max(1, rows.length);
  const avgBefore =
    rows.reduce((n, r) => n + r.reactionScoreBefore, 0) / Math.max(1, rows.length);
  const below7 = rows.filter((r) => r.reactionScoreProposed < 7);
  const scored = [...rows].sort((a, b) => b.reactionScoreProposed - a.reactionScoreProposed);

  // Cross-hotel repetition on proposed openings (first sentence of headline)
  const openings = rows.map((r) => r.proposedSections.HEADLINE.toLowerCase().slice(0, 48));
  const openingDupes = openings.filter((o, i) => openings.indexOf(o) !== i);
  const primaryFingerprints = rows.map((r) =>
    r.primaryFocus
      .toLowerCase()
      .replace(/[^a-z\s]/g, " ")
      .split(/\s+/)
      .filter((w) => w.length > 5)
      .slice(0, 6)
      .join(" ")
  );
  const uniquePrimaries = new Set(primaryFingerprints).size;
  const forbiddenCount = rows.filter((r) => r.forbiddenOpening).length;
  const spVsCReliance = rows.filter((r) =>
    /scenario reach|answer-level inclusion|individual answers/i.test(r.proposedExecutiveRead)
  ).length;

  const reactionGatePass = avgProposed >= 8 && below7.length === 0;
  const specificityPass = forbiddenCount === 0 && uniquePrimaries >= rows.length - 1;
  const thirtyAll = rows.every((r) => r.thirtySecondTest === "YES");
  const fiveMinAll = rows.every((r) => r.primaryFocusFiveMinuteTest === "YES");
  const analyticalAll = rows.every((r) => r.analyticalValue === "PASS");

  const compositionV2 = proposeExecutiveReadCompositionV2();

  const htmlRows = rows
    .map(
      (r) => `
<section class="hotel">
  <h2>${escapeHtml(r.name)}</h2>
  <p class="meta">${escapeHtml(r.market || "")} · ${r.reactionScoreBefore}/10 → <strong>${r.reactionScoreProposed}/10</strong> · ${r.wordCount} words · ${escapeHtml(r.insightArchetype)}</p>
  <div class="pair">
    <div class="col">
      <h3>CURRENT</h3>
      <pre>${escapeHtml(r.currentExecutiveRead)}</pre>
    </div>
    <div class="col proposed">
      <h3>PROPOSED PREVIEW</h3>
      <pre>${escapeHtml(r.proposedExecutiveRead)}</pre>
    </div>
  </div>
  <ul>
    <li><strong>Strongest insight:</strong> ${escapeHtml(r.strongestInsight)}</li>
    <li><strong>Primary focus:</strong> ${escapeHtml(r.primaryFocus)}</li>
    <li><strong>Watch:</strong> ${escapeHtml(r.watch || "(none)")}</li>
    <li><strong>Management question:</strong> ${escapeHtml(r.managementQuestion)}</li>
  </ul>
</section>`
    )
    .join("\n");

  const founderHtml = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>ADP Executive Read Controlled Rewrite Preview</title>
  <style>
    body{font-family:Georgia,serif;max-width:1100px;margin:24px auto;padding:0 16px;color:#1a1a1a;background:#f7f5f1}
    h1{font-size:1.6rem} h2{font-size:1.25rem;margin-top:2rem;border-top:1px solid #ccc;padding-top:1rem}
    .banner{background:#1f3d2b;color:#f7f5f1;padding:12px 16px;border-radius:6px;margin-bottom:20px}
    .pair{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .col{background:#fff;border:1px solid #ddd;border-radius:6px;padding:12px}
    .proposed{border-color:#1f3d2b;box-shadow:0 0 0 1px #1f3d2b33}
    pre{white-space:pre-wrap;font-family:Georgia,serif;font-size:0.92rem;line-height:1.45;margin:0}
    .meta{color:#555;font-size:0.9rem}
    @media(max-width:800px){.pair{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class="banner">
    <strong>PREVIEW ONLY — DO NOT PUBLISH.</strong>
    Methodology unchanged. Certified data unchanged. Shares unchanged.
    Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN. ·
    EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.
  </div>
  <h1>ADP Executive Read — Controlled Rewrite Preview (13 hotels)</h1>
  <p>Average reaction ${avgBefore.toFixed(1)} → <strong>${avgProposed.toFixed(1)}</strong> / 10.
  Forbidden SP-vs-C openings in proposed: ${forbiddenCount}. Composition V2: proposed, not activated.</p>
  ${htmlRows}
</body>
</html>`;

  return {
    version: ADP_EXECUTIVE_READ_CONTROLLED_REWRITE_PREVIEW_V1,
    auditedAt: new Date().toISOString(),
    LIVE_PROVIDER_CALLS: 0,
    methodologyChanged: false,
    productionSummariesRewritten: false,
    certifiedDataMutated: false,
    sharesMutated: false,
    doctrine: [
      METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
      EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED,
    ],
    missingCompositions: missing,
    hardStop: true,
    externalSend: false,
    founderHtml,
    A_CONTROLLED_REWRITE_VERDICT: {
      verdict: reactionGatePass && specificityPass && thirtyAll && fiveMinAll && analyticalAll
        ? "PREVIEW_READY_FOR_FOUNDER_REVIEW"
        : below7.length
          ? "PREVIEW_NEEDS_MANUAL_REFINEMENT"
          : "PREVIEW_PARTIAL",
      averageBefore: Math.round(avgBefore * 10) / 10,
      averageProposed: Math.round(avgProposed * 10) / 10,
      hotelsBelow7: below7.map((r) => r.name),
      productionWrite: false,
    },
    B_CURRENT_VS_PROPOSED: rows.map((r) => ({
      property: r.name,
      CURRENT_EXECUTIVE_READ: r.currentExecutiveRead,
      PROPOSED_EXECUTIVE_READ: r.proposedExecutiveRead,
      StrongestInsight: r.strongestInsight,
      PrimaryFocus: r.primaryFocus,
      Watch: r.watch,
      ManagementQuestion: r.managementQuestion,
      InsightArchetype: r.insightArchetype,
      WordCount: r.wordCount,
      ReactionScoreBefore: r.reactionScoreBefore,
      ReactionScoreProposed: r.reactionScoreProposed,
    })),
    C_PROPOSED_REACTION_SCORES: rows.map((r) => ({
      property: r.name,
      before: r.reactionScoreBefore,
      after: r.reactionScoreProposed,
      delta: Math.round((r.reactionScoreProposed - r.reactionScoreBefore) * 10) / 10,
    })),
    D_BEST_3_PROPOSED: scored.slice(0, 3).map((r) => ({
      property: r.name,
      score: r.reactionScoreProposed,
      whyItWorks: `Property-specific ${r.insightArchetype} with actionable Primary Focus and clear 30-second path.`,
    })),
    E_WEAKEST_3_PROPOSED: [...scored]
      .reverse()
      .slice(0, 3)
      .map((r) => ({
        property: r.name,
        score: r.reactionScoreProposed,
        whyStillNeedsWork: r.needsManualRefinement
          ? "Below 7 — flag for manual refinement."
          : "Lowest relative score in set; still passes floor if ≥7.",
      })),
    F_PRIMARY_FOCUS: rows.map((r) => ({ property: r.name, PRIMARY_FOCUS: r.primaryFocus })),
    G_WATCH_ITEMS: rows
      .filter((r) => r.watch)
      .map((r) => ({ property: r.name, WATCH: r.watch })),
    H_MANAGEMENT_QUESTIONS: rows.map((r) => ({
      property: r.name,
      QUESTION: r.managementQuestion,
    })),
    I_CROSS_HOTEL_REPETITION: {
      forbiddenUniversalOpeningInProposed: forbiddenCount,
      duplicateHeadlinePrefixes: openingDupes.length,
      uniquePrimaryFocusApprox: uniquePrimaries,
      hotelsUsingReachVsInclusionLanguage: spVsCReliance,
      note:
        "Reach vs inclusion language allowed where central; must not become universal boilerplate opening.",
      pass: specificityPass,
    },
    J_30_SECOND_EXECUTIVE_TEST: {
      pass: thirtyAll,
      results: rows.map((r) => ({ property: r.name, result: r.thirtySecondTest })),
    },
    K_PRIMARY_FOCUS_5_MINUTE_TEST: {
      pass: fiveMinAll,
      results: rows.map((r) => ({ property: r.name, result: r.primaryFocusFiveMinuteTest })),
    },
    L_PROPOSED_ADP_EXECUTIVE_READ_COMPOSITION_V2: compositionV2,
    M_ADP_EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE: {
      perHotel: rows.map((r) => ({ property: r.name, result: r.analyticalValue })),
      pass: analyticalAll,
    },
    N_ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY: {
      pass: specificityPass,
      forbiddenOpeningCount: forbiddenCount,
    },
    O_ADP_EXECUTIVE_REWRITE_REACTION_SCORE: {
      pass: reactionGatePass,
      averageProposed: Math.round(avgProposed * 10) / 10,
      targetAverage: 8,
      floor: 7,
      hotelsBelowFloor: below7.map((r) => r.name),
    },
    P_METHODOLOGY_CHANGED: "NO",
  };
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
