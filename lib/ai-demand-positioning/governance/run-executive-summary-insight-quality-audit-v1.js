/**
 * ADP Executive Summary Insight-Quality Audit V1 (READ-ONLY)
 *
 * Audits whether Executive Read synthesizes the most important insight.
 * Does NOT change methodology, metrics, certified data, or production copy.
 *
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

import { resolveGovernedAdpPropertyUniverseV1 } from "../client-readiness/resolve-governed-adp-property-universe-v1.js";
import { loadPublishedReport } from "../published-snapshot.js";
import { METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN } from "./adp-methodology-governance-v1.js";

export const ADP_EXECUTIVE_SUMMARY_INSIGHT_VALUE = "ADP_EXECUTIVE_SUMMARY_INSIGHT_VALUE";
export const ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY =
  "ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY";
export const ADP_EXECUTIVE_INSIGHT_CONTRADICTION_DETECTION =
  "ADP_EXECUTIVE_INSIGHT_CONTRADICTION_DETECTION";
export const ADP_EXECUTIVE_INSIGHT_CONCENTRATION_ANALYSIS =
  "ADP_EXECUTIVE_INSIGHT_CONCENTRATION_ANALYSIS";
export const ADP_EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE =
  "EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE";
export const ADP_EXECUTIVE_SUMMARY_PRIORITY_FOCUS = "ADP_EXECUTIVE_SUMMARY_PRIORITY_FOCUS";
export const ADP_EXECUTIVE_PRIORITY_MATERIALITY = "ADP_EXECUTIVE_PRIORITY_MATERIALITY";
export const ADP_EXECUTIVE_PRIORITY_SPECIFICITY = "ADP_EXECUTIVE_PRIORITY_SPECIFICITY";
export const ADP_EXECUTIVE_PRIORITY_EVIDENCE_TRACEABILITY =
  "ADP_EXECUTIVE_PRIORITY_EVIDENCE_TRACEABILITY";
export const ADP_EXECUTIVE_PRIORITY_5_MINUTE_TEST = "ADP_EXECUTIVE_PRIORITY_5_MINUTE_TEST";
export const ADP_EXECUTIVE_PRIORITY_PROPERTY_SPECIFICITY =
  "ADP_EXECUTIVE_PRIORITY_PROPERTY_SPECIFICITY";

export const INSIGHT_ARCHETYPES = Object.freeze({
  HIDDEN_STRENGTH: "HIDDEN_STRENGTH",
  HIDDEN_WEAKNESS: "HIDDEN_WEAKNESS",
  BREADTH_PROBLEM: "BREADTH_PROBLEM",
  CONSISTENCY_PROBLEM: "CONSISTENCY_PROBLEM",
  COMPETITIVE_DISPLACEMENT: "COMPETITIVE_DISPLACEMENT",
  REALITY_DISCONNECT: "REALITY_DISCONNECT",
  PROVIDER_FRAGMENTATION: "PROVIDER_FRAGMENTATION",
  PORTFOLIO_VS_NEUTRAL_CONTRADICTION: "PORTFOLIO_VS_NEUTRAL_CONTRADICTION",
  EMERGING_MOVEMENT: "EMERGING_MOVEMENT",
  STRONG_PROTECT: "STRONG_PROTECT",
  OTHER_EVIDENCE_BACKED: "OTHER_EVIDENCE_BACKED",
});

const GENERIC_PHRASE_PATTERNS = [
  /appears in [\d.]+% of monitored demand scenarios but in [\d.]+% of individual/i,
  /AI visibility is broader than consideration consistency/i,
  /relevant across monitored traveler needs but is not surfaced consistently/i,
  /This is the baseline monitoring period for this property/i,
  /Current strength is still developing/i,
  /management should review/i,
  /overall performance remains strong/i,
  /weakness is concentrated/i,
  /strongest governed signal/i,
  /primary constraint/i,
];

function payloadOf(published) {
  return published?.payload || published || null;
}

function extractTerritories(p) {
  const ipi = p?.intentPresenceIndex || {};
  const byIntent = ipi.byIntent || ipi.intents || {};
  const out = [];
  if (Array.isArray(byIntent)) {
    for (const t of byIntent) {
      out.push({
        intent: t.intent || t.label,
        label: t.label || t.name || t.intent,
        rate: t.rate ?? t.presence ?? t.aiPresencePct ?? null,
      });
    }
  } else if (byIntent && typeof byIntent === "object") {
    for (const [k, v] of Object.entries(byIntent)) {
      if (!v || typeof v !== "object") continue;
      out.push({
        intent: k,
        label: v.label || k,
        rate: v.rate ?? v.presence ?? v.aiPresencePct ?? null,
      });
    }
  }
  return out.filter((t) => typeof t.rate === "number");
}

function extractContext(entry) {
  const published = loadPublishedReport(entry.propertyId);
  const p = payloadOf(published);
  const er = p?.executiveRead || {};
  const territories = extractTerritories(p);
  const weakT = [...territories].sort((a, b) => a.rate - b.rate);
  const strongT = [...territories].sort((a, b) => b.rate - a.rate);
  const gaps = (p?.realityGap?.gaps || []).slice(0, 8).map((g) => ({
    label: g.label || g.attribute,
    severity: g.severity,
    recognition: g.recognitionRate ?? g.rate ?? null,
  }));
  const displacement = (p?.lostDemand?.displacement || []).slice(0, 6).map((d) => ({
    name: d.name,
    entityId: d.entityId,
    count: d.displacementCount,
  }));
  const narrative =
    (typeof er.narrative === "string" && er.narrative) ||
    er.current?.narrative ||
    er.writeup?.body ||
    "";
  const writeupBody = er.writeup?.body || "";
  const consideration = p?.executiveMetrics?.considerationRate?.rate ?? null;
  const scenarioPresence =
    p?.executiveMetrics?.scenarioPresence?.rate ?? p?.demandCapture?.overallRate ?? null;
  const top3 =
    p?.executiveMetrics?.positionMetrics?.top3Rate ??
    p?.executiveMetrics?.top3Rate ??
    null;
  const numberOne =
    p?.executiveMetrics?.positionMetrics?.numberOneRate ??
    p?.executiveMetrics?.numberOneRate ??
    null;
  const competitorPresentShare =
    p?.lostDemand?.competitorPresentShare ??
    p?.lostDemand?.highRelevanceLostShare ??
    null;
  const attributeCoverage =
    p?.realityGap?.overallRecognitionRate ??
    p?.realityGap?.coverageRate ??
    null;
  const benchmark = er.current?.benchmarkFinding || null;
  const pattern = er.current?.positionPattern || null;

  return {
    propertyId: entry.propertyId,
    name: p?.property?.name || entry.canonicalPropertyName,
    market: entry.market,
    consideration,
    scenarioPresence,
    demandCapture: p?.demandCapture?.overallRate ?? null,
    narrative,
    writeupBody,
    fullSummaryText: [narrative, writeupBody].filter(Boolean).join("\n\n"),
    primaryStrength: er.primaryStrength || er.current?.primaryStrength || null,
    primaryConstraint: er.primaryConstraint || er.current?.primaryConstraint || null,
    pattern,
    benchmark,
    top3,
    numberOne,
    competitorPresentShare,
    attributeCoverage,
    weakTerritories: weakT.slice(0, 5),
    strongTerritories: strongT.slice(0, 5),
    gaps,
    displacement,
    topAlt: p?.competitiveSet?.topObservedAlternative || null,
    topCompetitors: (p?.competitiveSet?.observed || []).slice(0, 5).map((c) => ({
      name: c.name,
      entityId: c.entityId,
      mentions: c.mentions,
      scenarioCount: c.scenarioCount,
    })),
    ux: er.ux || null,
  };
}

/**
 * Programmatic contradiction / concentration candidates (§9–10).
 */
export function detectInsightCandidates(ctx) {
  const candidates = [];
  const c = ctx.consideration;
  const sp = ctx.scenarioPresence;
  if (typeof c === "number" && typeof sp === "number" && sp - c >= 20) {
    candidates.push({
      type: "HIGH_SCENARIO_LOW_CONSIDERATION",
      archetype: INSIGHT_ARCHETYPES.BREADTH_PROBLEM,
      magnitudePp: Math.round((sp - c) * 10) / 10,
      note: `Scenario Presence ${sp}% vs AI Consideration ${c}% (Δ ${Math.round((sp - c) * 10) / 10} pp)`,
    });
  }
  if (typeof c === "number" && typeof sp === "number" && sp >= 65 && c < 50) {
    candidates.push({
      type: "BROAD_REACH_WEAK_CONSISTENCY",
      archetype: INSIGHT_ARCHETYPES.CONSISTENCY_PROBLEM,
      note: "Broad scenario reach with materially weaker response-level consideration",
    });
  }
  if (typeof ctx.top3 === "number" && ctx.top3 >= 70 && typeof c === "number" && c < 55) {
    candidates.push({
      type: "STRONG_WHEN_RANKED_WEAK_ENTRY",
      archetype: INSIGHT_ARCHETYPES.HIDDEN_STRENGTH,
      note: `Top-3 ${ctx.top3}% when ranked, but consideration only ${c}%`,
    });
  }
  if (ctx.benchmark?.index >= 200) {
    candidates.push({
      type: "PEER_RELATIVE_SURPRISE",
      archetype: INSIGHT_ARCHETYPES.HIDDEN_STRENGTH,
      note: `${ctx.benchmark.territory || ctx.benchmark.intent} Presence Index ${ctx.benchmark.index}`,
    });
  }
  if (ctx.displacement?.[0]?.count >= 10) {
    candidates.push({
      type: "CONCENTRATED_DISPLACEMENT",
      archetype: INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
      note: `${ctx.displacement[0].name} displacement count ${ctx.displacement[0].count}`,
      competitor: ctx.displacement[0].name,
    });
  }
  if (typeof ctx.competitorPresentShare === "number" && ctx.competitorPresentShare >= 0.45) {
    candidates.push({
      type: "HIGH_COMPETITOR_PRESENT_GAPS",
      archetype: INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
      note: `Competitor-present gaps share ${ctx.competitorPresentShare}`,
    });
  }
  const materialGap = (ctx.gaps || []).find(
    (g) =>
      (g.severity === "HIGH" && (g.recognition ?? 100) < 15) ||
      ((g.recognition ?? 100) < 20 && /bar|spa|cove|beach|meeting|ballroom|all.?inclusive|marina|view/i.test(g.label || ""))
  );
  if (materialGap) {
    candidates.push({
      type: "PROPOSITION_REALITY_GAP",
      archetype: INSIGHT_ARCHETYPES.REALITY_DISCONNECT,
      note: `${materialGap.label} recognition ${materialGap.recognition}% (${materialGap.severity})`,
      attribute: materialGap.label,
    });
  }
  if (typeof sp === "number" && sp >= 90 && typeof c === "number" && c >= 70) {
    candidates.push({
      type: "ELITE_BROAD_STRENGTH",
      archetype: INSIGHT_ARCHETYPES.STRONG_PROTECT,
      note: `Scenario ${sp}% / Consideration ${c}% — protect strength`,
    });
  }
  if (typeof c === "number" && c < 15 && typeof sp === "number" && sp < 30) {
    candidates.push({
      type: "ENTRY_PROBLEM_NOT_RANK",
      archetype: INSIGHT_ARCHETYPES.HIDDEN_WEAKNESS,
      note: "Primary issue is entering consideration at all",
    });
  }
  const rates = (ctx.weakTerritories || []).map((t) => t.rate).filter((n) => typeof n === "number");
  if (rates.length >= 3) {
    const min = Math.min(...rates);
    const max = Math.max(...(ctx.strongTerritories || []).map((t) => t.rate).filter((n) => typeof n === "number"));
    if (max - min >= 25) {
      candidates.push({
        type: "TERRITORY_CONCENTRATION",
        archetype: INSIGHT_ARCHETYPES.CONSISTENCY_PROBLEM,
        concentration: "CONCENTRATED",
        note: `Territory spread ${min}%–${max}% (weakest: ${ctx.weakTerritories[0]?.label})`,
      });
    }
  }
  return candidates;
}

function scoreReaction(ctx, editorial) {
  // 0–2 each for A–E; editorial can override after template detection
  const text = ctx.fullSummaryText || "";
  const genericHits = GENERIC_PHRASE_PATTERNS.filter((re) => re.test(text)).length;
  const mostlyTemplate = genericHits >= 3;

  let a = mostlyTemplate ? 0 : 1; // I DID NOT KNOW THAT
  let b = mostlyTemplate ? 1 : 1; // INTERESTING
  let c = mostlyTemplate ? 1 : 1; // WHY CARE
  let d = 0; // ASK TEAM
  let e = 0; // WATCH NEXT

  if (/Presence Index|times as often|compared with/i.test(text)) {
    a = Math.max(a, 1);
    b = Math.max(b, 2);
  }
  if (/competitor|displacement|leakage|absent/i.test(text)) {
    a = Math.max(a, 1);
    c = Math.max(c, 1);
    d = Math.max(d, 1);
  }
  if (/attribute|proposition|recognition|representation/i.test(text)) {
    a = Math.max(a, 1);
    c = Math.max(c, 1);
  }
  if (/next focus|opportunity|should|watch/i.test(text)) {
    d = Math.max(d, 1);
    e = Math.max(e, 1);
  }
  // Cap if pure KPI restatement of SP vs C
  if (
    /appears in [\d.]+% of monitored demand scenarios but in [\d.]+% of individual/i.test(text) &&
    !/Presence Index|displacement|competitor|attribute|Rosewood|Four Seasons|Knickerbocker|King Cole|Private Cove|Heavenly|Crosby/i.test(
      text
    )
  ) {
    a = Math.min(a, 0);
    b = Math.min(b, 1);
  }

  // Editorial lift when current summary actually names the strongest insight
  if (editorial.currentCapturesStrongest) {
    a = Math.max(a, 2);
    b = Math.max(b, 2);
    c = Math.max(c, 2);
    d = Math.max(d, 1);
  } else if (editorial.partialCapture) {
    a = Math.max(a, 1);
    b = Math.max(b, 1);
  }

  const scores = {
    A_I_DID_NOT_KNOW_THAT: a,
    B_THAT_IS_INTERESTING: b,
    C_WHY_SHOULD_I_CARE: c,
    D_WHAT_SHOULD_I_ASK_MY_TEAM: d,
    E_WHAT_SHOULD_I_WATCH_NEXT: e,
  };
  const total = a + b + c + d + e;
  return { scores, total, genericHits, mostlyTemplate };
}

/**
 * Property-specific editorial judgment — strongest insight from full report,
 * not from current summary wording. Evidence-backed; no methodology change.
 */
function editorialFor(ctx, candidates) {
  const id = ctx.propertyId;
  const name = ctx.name;
  const weak = ctx.weakTerritories[0];
  const strong = ctx.strongTerritories[0];
  const topDisp = ctx.displacement[0];
  const topGap = ctx.gaps[0];

  /** @type {Record<string, object>} */
  const byId = {
    adp_cambridge_beaches_bermuda: {
      archetypes: [
        INSIGHT_ARCHETYPES.HIDDEN_STRENGTH,
        INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
        INSIGHT_ARCHETYPES.REALITY_DISCONNECT,
      ],
      strongestInsight:
        "Leisure demand is extraordinarily strong versus CORE peers (Presence Index ~731), but signature assets like Five Private Coves are barely recognized and Rosewood Bermuda appears in the few business/group contexts where Cambridge is absent.",
      nonObvious:
        "The less obvious finding is not the leisure strength — it is that a hotel this strong still loses concentrated business/group answers to Rosewood and fails to get credit for its private-cove proposition.",
      whyMatters:
        "Overall scores look strong; management could miss that competitive leakage and proposition recognition are concentrated, not broad.",
      managementQuestion:
        "Why does Rosewood Bermuda repeatedly appear in the limited business/group contexts where we are absent, and why are Five Private Coves so rarely recognized?",
      currentCapturesStrongest: false,
      partialCapture: true,
      whatMisses:
        "Leads with the generic Scenario Presence vs Consideration template; underweights Rosewood displacement and signature-cove Reality Gaps.",
      rewritePriority: "HIGH",
      primaryFocus:
        "Investigate Rosewood Bermuda displacement in business/group scenarios and reconcile recognition of Five Private Coves across owned and major travel sources — the highest-value gap beneath an otherwise elite leisure position.",
      secondaryFocus:
        "Protect Leisure Travel peer-relative dominance; do not dilute the leisure narrative while closing business/group soft spots.",
      watch: "Consideration consistency beneath 95% scenario reach (71.3% response-level).",
      expectedSignal:
        "Fewer Rosewood-led business/group absences; higher recognition of private-cove / signature leisure attributes.",
      gmActTomorrow: "YES",
    },
    adp_faranda_collection_bogota: {
      archetypes: [INSIGHT_ARCHETYPES.HIDDEN_WEAKNESS, INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT],
      strongestInsight:
        "The hotel’s primary problem is entering monitored answers at all (~21% reach), and when it is absent Four Seasons Bogotá and Click Clack systematically fill the set.",
      nonObvious:
        "The less obvious finding is how concentrated competitive substitution is — Four Seasons alone accounts for a large share of displacement — not merely that absolute rates are low.",
      whyMatters:
        "Improving rank among answers that already include Faranda is secondary to being included; competitor patterns show who owns the demand the hotel is missing.",
      managementQuestion:
        "Why are Four Seasons Bogotá and Click Clack repeatedly surfaced in the traveler needs where Faranda Collection Bogotá is absent?",
      currentCapturesStrongest: false,
      partialCapture: false,
      whatMisses: "States limited reach but does not elevate the displacement concentration or Choice distribution recognition gap.",
      rewritePriority: "CRITICAL",
      primaryFocus:
        "Prioritize entering consideration for the traveler needs where Faranda is repeatedly absent before competitor-rank tactics — starting with couples/leisure contexts where reach is weakest.",
      secondaryFocus:
        "Address HIGH-severity Choice Hotels distribution recognition (very low) if it is part of the intended commercial proposition.",
      watch: "Four Seasons Bogotá displacement count as reach improves.",
      expectedSignal: "Higher Scenario Presence / Consideration; reduced Four Seasons / Click Clack displacement share.",
      gmActTomorrow: "YES",
    },
    adp_hotel_caribe_faranda_grand: {
      archetypes: [INSIGHT_ARCHETYPES.BREADTH_PROBLEM, INSIGHT_ARCHETYPES.HIDDEN_STRENGTH],
      strongestInsight:
        "When AI includes Hotel Caribe and ranks hotels, it places Caribe in the Top 3 ~72% of the time — the binding constraint is inconsistent entry into the answer set (69.8% scenarios vs 43.3% consideration), especially business demand.",
      nonObvious:
        "The less obvious finding is that prominence is already strong once included; management may over-index on ‘beating Hilton/Bastión’ instead of closing entry gaps.",
      whyMatters:
        "Effort on competitive ranking without fixing consideration consistency targets the wrong bottleneck.",
      managementQuestion:
        "Why are we frequently Top 3 when ranked, yet missing from a large share of individual AI answers — especially for business travel?",
      currentCapturesStrongest: false,
      partialCapture: true,
      whatMisses: "Mentions Top-3 but still frames the story as generic breadth vs consistency without a crisp primary focus.",
      rewritePriority: "HIGH",
      primaryFocus:
        "Improve consideration consistency for business (and adjacent) traveler needs where scenario reach already exists but response-level inclusion lags.",
      secondaryFocus: "Clarify upper-upscale / full-service proposition recognition where Reality Gaps remain material.",
      watch: "Hilton Cartagena / Bastión displacement in contexts where Caribe is absent.",
      expectedSignal: "Consideration rising toward Scenario Presence; stable or improving Top-3 when ranked.",
      gmActTomorrow: "YES",
    },
    adp_hotel_phillips_kansas_city: {
      archetypes: [
        INSIGHT_ARCHETYPES.BREADTH_PROBLEM,
        INSIGHT_ARCHETYPES.REALITY_DISCONNECT,
        INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
      ],
      strongestInsight:
        "Phillips reaches ~73% of monitored scenarios but only ~31% of individual answers — a severe consistency gap — while meeting/ballroom recognition is weak and Hotel Kansas City / Loews repeatedly displace it.",
      nonObvious:
        "The less obvious finding is the meetings proposition disconnect: group/meeting demand is among weaker territories while ballroom recognition is low and Fitness Center recognition is near zero.",
      whyMatters:
        "A Curio collection hotel competing in Kansas City meetings demand cannot rely on scenario-level reach alone if meetings assets are invisible to AI.",
      managementQuestion:
        "Why do Loews and Hotel Kansas City repeatedly appear when we are absent, and why is meeting/ballroom recognition so weak relative to our intended event proposition?",
      currentCapturesStrongest: false,
      partialCapture: false,
      whatMisses: "Repeats SP vs Consideration template; does not elevate meetings Reality Gaps or named displacement.",
      rewritePriority: "CRITICAL",
      primaryFocus:
        "Reconcile meeting & event / ballroom proposition across website, Hilton/Curio channels, and major platforms — the clearest actionable gap beneath broad scenario reach.",
      secondaryFocus:
        "Investigate repeated displacement by Loews Kansas City and Hotel Kansas City in the scenarios where Phillips is absent.",
      watch: "Fitness Center recognition (currently ~0%) if wellness/fitness is intentional.",
      expectedSignal: "Higher meeting-attribute recognition; narrower Scenario–Consideration gap; lower Loews/HKC displacement.",
      gmActTomorrow: "YES",
    },
    adp_jw_marriott_monterrey_valle: {
      archetypes: [INSIGHT_ARCHETYPES.BREADTH_PROBLEM, INSIGHT_ARCHETYPES.HIDDEN_STRENGTH],
      strongestInsight:
        "JW Monterrey Valle is included across ~92% of scenarios and, when ranked, lands Top 3 ~93% of the time — yet response-level consideration is only ~45%. Family demand is the softest territory.",
      nonObvious:
        "The less obvious finding is how strong the hotel is once AI decides to rank it; the commercial question is inconsistent inclusion, not weak prominence.",
      whyMatters:
        "Headline Scenario Presence can look ‘solved’ while half of individual answers still omit the hotel.",
      managementQuestion:
        "Why are we Top 3 in nearly every ranked answer yet absent from roughly half of individual AI responses?",
      currentCapturesStrongest: false,
      partialCapture: true,
      whatMisses: "States the pattern but does not make the Top-3-vs-entry contradiction the headline.",
      rewritePriority: "HIGH",
      primaryFocus:
        "Close the consideration-consistency gap (especially family-adjacent demand) so response-level inclusion better matches the hotel’s near-universal scenario reach and Top-3 prominence.",
      secondaryFocus: "Strengthen Valle / business-district location recognition where Reality Gaps persist.",
      watch: "AC Hotel / Holiday Inn Valle displacement (currently low count — monitor if it rises).",
      expectedSignal: "Consideration rising while Top-3 rate stays high.",
      gmActTomorrow: "YES",
    },
    adp_jw_marriott_santo_domingo: {
      archetypes: [INSIGHT_ARCHETYPES.STRONG_PROTECT, INSIGHT_ARCHETYPES.REALITY_DISCONNECT],
      strongestInsight:
        "JW Santo Domingo is broadly strong (≈98% scenario / ≈71% consideration, high Top-3). The important story is protecting that breadth while closing softer wellness recognition and Blue Mall / executive-lounge proposition gaps — not inventing a crisis.",
      nonObvious:
        "The less obvious finding is how little competitive displacement exists relative to peers; residual risk is proposition representation (e.g., Blue Mall adjacency, Executive Lounge), not competitor takeover.",
      whyMatters:
        "Strong hotels still need a clear protect-and-polish agenda so management does not under-invest in residual recognition gaps.",
      managementQuestion:
        "Which parts of our urban/business proposition (Blue Mall adjacency, Executive Lounge, wellness) remain under-recognized despite near-universal scenario reach?",
      currentCapturesStrongest: false,
      partialCapture: true,
      whatMisses: "Still uses the same SP vs C template as weaker hotels; underplays protect-the-franchise framing.",
      rewritePriority: "MEDIUM",
      primaryFocus:
        "Protect overall strength and prioritize recognition of Blue Mall adjacency / Executive Lounge (and wellness) where Reality Gaps persist beneath strong visibility.",
      secondaryFocus: null,
      watch: "Wellness territory relative softness vs other intents at 100% scenario rates.",
      expectedSignal: "Stable elite Scenario Presence; improved recognition of signature urban/business attributes.",
      gmActTomorrow: "YES",
    },
    adp_now_now_noho: {
      archetypes: [INSIGHT_ARCHETYPES.HIDDEN_WEAKNESS, INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT],
      strongestInsight:
        "NOHO’s primary issue is entering consideration at all (≈3% consideration / ≈13% scenario reach). Crosby Street, Soho Grand, and Bowery systematically occupy the lifestyle demand the hotel is missing; couples/family territories show ~0% reach.",
      nonObvious:
        "The less obvious finding is the peer substitution pattern — specific boutique neighbors own the answer set — not only that absolute rates are low.",
      whyMatters:
        "Without first earning inclusion, competitor-rank and attribute polish will not move executive KPIs.",
      managementQuestion:
        "Why are Crosby Street, Soho Grand, and The Bowery repeatedly recommended in the lifestyle demand contexts where NOW NOW NOHO does not appear at all?",
      currentCapturesStrongest: false,
      partialCapture: false,
      whatMisses: "Generic limited-reach language; no named competitor or territory zeros.",
      rewritePriority: "CRITICAL",
      primaryFocus:
        "Build relevance for the lifestyle/boutique traveler needs where the hotel is repeatedly absent — especially couples/leisure contexts currently near zero — before investing in fine-grained competitor displacement tactics.",
      secondaryFocus: "Clarify urban lifestyle / pet-friendly propositions where recognition is weak or zero if those are intentional offers.",
      watch: "Crosby Street Hotel displacement dominance.",
      expectedSignal: "Non-zero couples/family reach; rising Scenario Presence from a very low base.",
      gmActTomorrow: "YES",
    },
    adp_radisson_santo_domingo: {
      archetypes: [INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT, INSIGHT_ARCHETYPES.HIDDEN_WEAKNESS],
      strongestInsight:
        "Radisson’s most material story is competitive substitution: in a large share of scenarios where it is absent, competitors appear — led heavily by JW Marriott Santo Domingo (and Jaragua / El Embajador).",
      nonObvious:
        "The less obvious finding is how asymmetric the Santo Domingo set is — JW Marriott is not a generic peer mention; it is the primary replacement when Radisson is missing.",
      whyMatters:
        "Management should treat JW presence as the competitive context for every improvement initiative, not as a footnote.",
      managementQuestion:
        "Why does JW Marriott Santo Domingo so often fill the answer set in the traveler needs where Radisson is absent?",
      currentCapturesStrongest: true,
      partialCapture: true,
      whatMisses: "Names competitive leakage but could more sharply name JW as the primary substitute and pair with family/wellness soft spots.",
      rewritePriority: "MEDIUM",
      primaryFocus:
        "Strengthen consideration in the traveler needs with the highest JW Marriott displacement overlap (including family/leisure soft spots), treating JW as the primary competitive substitute.",
      secondaryFocus: "Improve recognition of Tiradentes corridor / business-district location cues that support those needs.",
      watch: "Jaragua and El Embajador displacement as secondary substitutes.",
      expectedSignal: "Higher Consideration; lower JW displacement share when Radisson is measured.",
      gmActTomorrow: "YES",
    },
    adp_renaissance_times_square: {
      archetypes: [
        INSIGHT_ARCHETYPES.HIDDEN_WEAKNESS,
        INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
        INSIGHT_ARCHETYPES.REALITY_DISCONNECT,
      ],
      strongestInsight:
        "RTS is not primarily a ranking problem — it rarely enters consideration (≈8%). The Knickerbocker leads displacement; Times Square views / Central Park proximity recognition is weak; wellness/adventure territories are ~0%.",
      nonObvious:
        "The less obvious finding is a slight business-travel peer-relative foothold (Index ~119) that is buried under a summary that still emphasizes generic consistency language.",
      whyMatters:
        "Scarce management attention should go to answer-set entry and Times Square proposition clarity, not Top-3 optimization.",
      managementQuestion:
        "Why does The Knickerbocker repeatedly appear in Times Square demand contexts where Renaissance is absent, and why are Times Square views so weakly recognized?",
      currentCapturesStrongest: false,
      partialCapture: true,
      whatMisses: "Mentions business benchmark but still leads with SP vs C boilerplate.",
      rewritePriority: "CRITICAL",
      primaryFocus:
        "Prioritize entering consideration for core Times Square traveler needs — clarifying Times Square views / location proposition — before competitor-rank tactics; investigate Knickerbocker displacement patterns.",
      secondaryFocus: "Protect the small business-travel peer-relative foothold while expanding leisure-adjacent reach from near-zero wellness/adventure.",
      watch: "Westin Times Square and Marriott Marquis as secondary displacers.",
      expectedSignal: "Consideration and Scenario Presence rising from low base; higher Times Square view recognition.",
      gmActTomorrow: "YES",
    },
    adp_st_regis_cap_cana: {
      archetypes: [INSIGHT_ARCHETYPES.BREADTH_PROBLEM, INSIGHT_ARCHETYPES.HIDDEN_STRENGTH],
      strongestInsight:
        "Cap Cana has near-universal scenario reach (~98%) and meaningful #1 rates when ranked, yet consideration is only ~47% — a large consistency gap for a ultra-luxury resort.",
      nonObvious:
        "The less obvious finding is how often the hotel can be #1 when ranking exists, while still missing roughly half of individual answers — a protect-and-deepen problem, not a weak-brand problem.",
      whyMatters:
        "Luxury owners can misread 98% scenario reach as ‘solved’ while travelers still frequently never see the hotel in a given answer.",
      managementQuestion:
        "Why do we cover nearly every monitored traveler need at the scenario level yet appear in only about half of individual AI answers?",
      currentCapturesStrongest: false,
      partialCapture: true,
      whatMisses: "Notes #1 prominence but does not make the elite-reach / mid consideration contradiction the management focus.",
      rewritePriority: "HIGH",
      primaryFocus:
        "Raise consideration consistency beneath near-universal scenario reach — starting with swim-out / infinity-pool / signature resort attributes that remain weakly recognized.",
      secondaryFocus: null,
      watch: "Secrets Cap Cana and Tortuga Bay displacement (currently rare — watch for growth).",
      expectedSignal: "Consideration rising toward Scenario Presence; stable #1 share when ranked.",
      gmActTomorrow: "YES",
    },
    adp_st_regis_mexico_city: {
      archetypes: [INSIGHT_ARCHETYPES.STRONG_PROTECT, INSIGHT_ARCHETYPES.REALITY_DISCONNECT],
      strongestInsight:
        "St. Regis Mexico City is the portfolio’s elite visibility case (100% scenario / ~86% consideration). The surprising gap is proposition representation — e.g., King Cole Bar and full-service cues — not competitor displacement (none material).",
      nonObvious:
        "The less obvious finding is that with almost no competitive leakage, residual value is locked in attribute recognition (King Cole Bar, loyalty/full-service cues), not in chasing competitors.",
      whyMatters:
        "At this performance level, the executive agenda should be protect + representation polish, not manufactured weakness.",
      managementQuestion:
        "Which signature St. Regis Mexico City experiences (starting with King Cole Bar) remain under-recognized despite near-perfect demand reach?",
      currentCapturesStrongest: true,
      partialCapture: true,
      whatMisses: "Does call out property representation — one of the better current summaries — but still opens with the generic SP vs C sentence.",
      rewritePriority: "LOW",
      primaryFocus:
        "Protect elite demand reach and prioritize recognition of King Cole Bar (and related signature experiences) where representation lags visibility.",
      secondaryFocus: "Clarify Marriott Bonvoy / full-service cues if they are intentional commercial differentiators.",
      watch: "Any new competitor displacement emerging from a near-zero base.",
      expectedSignal: "Stable Scenario/Consideration; higher King Cole Bar recognition.",
      gmActTomorrow: "YES",
    },
    adp_waterstone_boca_raton: {
      archetypes: [
        INSIGHT_ARCHETYPES.HIDDEN_STRENGTH,
        INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
        INSIGHT_ARCHETYPES.REALITY_DISCONNECT,
      ],
      strongestInsight:
        "Couples/romantic demand is an extraordinary peer-relative strength (Presence Index ~607), while Eau Palm Beach and Four Seasons Palm Beach lead displacement and beach/marina proximity attributes remain weakly recognized.",
      nonObvious:
        "The less obvious finding is the marina/beach recognition gap beneath a couples advantage — AI may credit romance demand without crediting the waterfront proposition that should support it.",
      whyMatters:
        "Waterstone can over-celebrate couples Index while under-investing in waterfront recognition and Palm Beach competitive leakage.",
      managementQuestion:
        "Why is couples demand so strong versus peers while walking-distance-to-beach / water-sports recognition stays weak, and why does Eau Palm Beach repeatedly appear when we are absent?",
      currentCapturesStrongest: false,
      partialCapture: true,
      whatMisses: "Highlights couples Index but not Eau/Four Seasons displacement or beach proximity Reality Gaps as primary focus.",
      rewritePriority: "HIGH",
      primaryFocus:
        "Strengthen recognition of marina / beach-proximity / water-sports propositions that should reinforce the couples advantage, and review Eau Palm Beach displacement patterns.",
      secondaryFocus: "Monitor Four Seasons Palm Beach and The Boca Raton as secondary displacers.",
      watch: "Wellness territory (relatively softer vs couples).",
      expectedSignal: "Higher beach/marina attribute recognition; stable couples Index; lower Eau displacement.",
      gmActTomorrow: "YES",
    },
    adp_westin_monterrey_valle: {
      archetypes: [
        INSIGHT_ARCHETYPES.BREADTH_PROBLEM,
        INSIGHT_ARCHETYPES.REALITY_DISCONNECT,
        INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
      ],
      strongestInsight:
        "Westin Monterrey Valle shows the same Valle pattern as JW: high scenario reach (~84%) with ~43% consideration. Heavenly Spa recognition is still only moderate, and JW Marriott Valle appears as a peer displacer.",
      nonObvious:
        "The less obvious finding is brand-proposition under-recognition (Heavenly Spa) beside a consistency gap — not simply ‘another Valle hotel with SP > C’.",
      whyMatters:
        "Westin’s wellness brand equity should be visible in AI; if it is not, management is leaving a differentiated lever unused.",
      managementQuestion:
        "Why is Heavenly Spa recognition only moderate while consideration consistency lags scenario reach — and when we are absent, how often does JW Marriott Valle take the slot?",
      currentCapturesStrongest: false,
      partialCapture: false,
      whatMisses: "Generic Top-3 / consistency template; does not elevate Heavenly Spa or JW displacement.",
      rewritePriority: "HIGH",
      primaryFocus:
        "Raise consideration consistency and strengthen Heavenly Spa / wellness proposition recognition — the clearest Westin-specific lever in Valle demand.",
      secondaryFocus: "Review JW Marriott Monterrey Valle displacement in scenarios where Westin is absent.",
      watch: "Family territory softness vs other intents.",
      expectedSignal: "Higher spa recognition; narrower Scenario–Consideration gap; stable Top-3 when ranked.",
      gmActTomorrow: "YES",
    },
  };

  const ed = byId[id];
  if (!ed) {
    return {
      archetypes: candidates.map((c) => c.archetype).filter(Boolean),
      strongestInsight: candidates[0]?.note || "Insufficient editorial map — review manually.",
      nonObvious: candidates[0]?.note || null,
      whyMatters: "Requires founder review.",
      managementQuestion: `What is the single highest-value gap in ${name}'s AI demand positioning?`,
      currentCapturesStrongest: false,
      partialCapture: false,
      whatMisses: "No property-specific editorial map.",
      rewritePriority: "HIGH",
      primaryFocus: "Review full report for the strongest concentration or contradiction.",
      secondaryFocus: null,
      watch: weak ? `${weak.label} territory` : null,
      expectedSignal: "Clearer primary focus after rewrite.",
      gmActTomorrow: "NO",
      concentration:
        candidates.some((c) => c.concentration === "CONCENTRATED") || (spGap(ctx) ? "CONCENTRATED_ON_CONSISTENCY" : "BROAD"),
    };
  }
  return {
    ...ed,
    concentration: classifyConcentration(ctx, candidates),
    candidateTypes: candidates.map((c) => c.type),
  };
}

function spGap(ctx) {
  return (
    typeof ctx.scenarioPresence === "number" &&
    typeof ctx.consideration === "number" &&
    ctx.scenarioPresence - ctx.consideration >= 20
  );
}

function classifyConcentration(ctx, candidates) {
  if (candidates.some((c) => c.type === "TERRITORY_CONCENTRATION" || c.type === "CONCENTRATED_DISPLACEMENT")) {
    return "CONCENTRATED";
  }
  if (spGap(ctx)) return "CONSISTENCY_GAP_ACROSS_BROAD_REACH";
  if (typeof ctx.consideration === "number" && ctx.consideration < 20) return "BROAD_WEAKNESS";
  return "MIXED_OR_BROAD_STRENGTH";
}

function proposedSummary(ctx, ed) {
  const headline = ed.strongestInsight.split(".")[0] + ".";
  const parts = [
    `HEADLINE\n${headline}`,
    `NON-OBVIOUS INSIGHT\n${ed.nonObvious}`,
    `WHY IT MATTERS\n${ed.whyMatters}`,
    `PRIMARY FOCUS\n${ed.primaryFocus}`,
  ];
  if (ed.secondaryFocus) parts.push(`SECONDARY FOCUS\n${ed.secondaryFocus}`);
  if (ed.watch) parts.push(`WATCH\n${ed.watch}`);
  parts.push(`MANAGEMENT QUESTION\n${ed.managementQuestion}`);
  const body = parts.join("\n\n");
  const wordCount = body.split(/\s+/).length;
  return { structure: "HEADLINE / NON-OBVIOUS / WHY / PRIMARY / SECONDARY? / WATCH? / QUESTION", body, wordCount };
}

function fiveMinuteTest(ed) {
  const specific = !/improve (content|visibility)|strengthen visibility|optimize AI/i.test(
    ed.primaryFocus || ""
  );
  const actionable = ed.gmActTomorrow === "YES";
  return specific && actionable && Boolean(ed.primaryFocus);
}

export function runExecutiveSummaryInsightQualityAuditV1() {
  const universe = resolveGovernedAdpPropertyUniverseV1();
  const propertyAudits = [];

  for (const entry of universe.properties) {
    const ctx = extractContext(entry);
    const candidates = detectInsightCandidates(ctx);
    const ed = editorialFor(ctx, candidates);
    const reaction = scoreReaction(ctx, ed);
    const proposed = proposedSummary(ctx, ed);
    const insightValuePass =
      Boolean(ed.strongestInsight) &&
      (candidates.length > 0 || (ed.archetypes || []).length > 0) &&
      !/appears in [\d.]+% of monitored demand scenarios but in [\d.]+% of individual/i.test(
        ed.strongestInsight
      );
    const prioritySpecific = fiveMinuteTest(ed);

    propertyAudits.push({
      propertyId: ctx.propertyId,
      name: ctx.name,
      market: ctx.market,
      kpis: {
        consideration: ctx.consideration,
        scenarioPresence: ctx.scenarioPresence,
        demandCapture: ctx.demandCapture,
      },
      currentSummary: ctx.narrative,
      currentWriteupExcerpt: (ctx.writeupBody || "").slice(0, 400),
      currentScore10: reaction.total,
      reactionScores: reaction.scores,
      genericHits: reaction.genericHits,
      mostlyTemplate: reaction.mostlyTemplate,
      candidates,
      archetypes: ed.archetypes,
      strongestInsight: ed.strongestInsight,
      currentCapturesIt: ed.currentCapturesStrongest ? "YES" : ed.partialCapture ? "PARTIAL" : "NO",
      whatMisses: ed.whatMisses,
      nonObviousInsight: ed.nonObvious,
      whyItMatters: ed.whyMatters,
      managementQuestion: ed.managementQuestion,
      rewritePriority: ed.rewritePriority,
      concentration: ed.concentration,
      primaryFocus: ed.primaryFocus,
      secondaryFocus: ed.secondaryFocus || null,
      watch: ed.watch || null,
      whyThisIsThePriority: ed.whyMatters,
      evidenceSupport: {
        kpis: { consideration: ctx.consideration, scenarioPresence: ctx.scenarioPresence },
        weakTerritory: ctx.weakTerritories[0] || null,
        strongTerritory: ctx.strongTerritories[0] || null,
        topDisplacement: ctx.displacement[0] || null,
        topGap: ctx.gaps[0] || null,
        benchmark: ctx.benchmark,
        candidates: candidates.map((c) => c.type),
      },
      expectedSignalToMonitor: ed.expectedSignal,
      gmCouldActTomorrow: ed.gmActTomorrow,
      fiveMinuteTestPass: prioritySpecific,
      insightValueGatePass: insightValuePass,
      proposedSummary: proposed,
      proposedArchitectureDemo: proposed.body,
    });
  }

  // Cross-hotel repetition
  const openingRegex =
    /appears in [\d.]+% of monitored demand scenarios but in [\d.]+% of individual comparable AI responses/i;
  const openings = propertyAudits.filter((p) => openingRegex.test(p.currentSummary || ""));
  const constraintLabels = propertyAudits.map(
    (p) => p.evidenceSupport?.candidates?.[0] || p.archetypes?.[0]
  );

  const scored = [...propertyAudits].sort((a, b) => b.currentScore10 - a.currentScore10);
  const best3 = scored.slice(0, 3);
  const worst3 = [...scored].reverse().slice(0, 3);

  const priorityFingerprints = propertyAudits.map((p) =>
    String(p.primaryFocus || "")
      .toLowerCase()
      .replace(/[^a-z\s]/g, "")
      .split(/\s+/)
      .filter((w) => w.length > 4)
      .slice(0, 8)
      .join(" ")
  );
  const uniquePriorityApprox = new Set(priorityFingerprints).size;

  const avgScore =
    propertyAudits.reduce((n, p) => n + p.currentScore10, 0) / Math.max(1, propertyAudits.length);

  const verdict =
    avgScore < 5
      ? "FAIL_INSIGHT_QUALITY — summaries are analytically correct but largely template-driven; they rarely surface the most interesting synthesis or a 5-minute management priority."
      : avgScore < 7
        ? "PARTIAL — some properties show insight; most still under-deliver so-what / non-obvious value."
        : "PASS_WITH_RESERVATIONS";

  return {
    version: "adp_executive_summary_insight_quality_audit_v1",
    auditedAt: new Date().toISOString(),
    LIVE_PROVIDER_CALLS: 0,
    methodologyChanged: false,
    doctrine: METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
    productionSummariesRewritten: false,
    certifiedDataMutated: false,
    universeCount: universe.propertyIds.length,
    propertyIds: universe.propertyIds,
    gates: {
      [ADP_EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE]: {
        pass: propertyAudits.every((p) => p.insightValueGatePass && !p.mostlyTemplate),
        note: "Proposed permanent principle — current production summaries largely FAIL.",
      },
      [ADP_EXECUTIVE_INSIGHT_CONTRADICTION_DETECTION]: {
        pass: true,
        hotelsWithCandidates: propertyAudits.filter((p) => p.candidates.length).length,
      },
      [ADP_EXECUTIVE_INSIGHT_CONCENTRATION_ANALYSIS]: {
        pass: true,
        note: "Concentration classified per hotel from territories/displacement/SP–C gaps.",
      },
      [ADP_EXECUTIVE_SUMMARY_INSIGHT_VALUE]: {
        proposed: true,
        currentProductionPassCount: propertyAudits.filter((p) => !p.mostlyTemplate && p.currentCapturesIt !== "NO")
          .length,
      },
      [ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY]: {
        proposed: true,
        templateOpeningCount: openings.length,
        pass: openings.length <= 3,
      },
      [ADP_EXECUTIVE_SUMMARY_PRIORITY_FOCUS]: { proposed: true },
      [ADP_EXECUTIVE_PRIORITY_MATERIALITY]: { proposed: true },
      [ADP_EXECUTIVE_PRIORITY_SPECIFICITY]: {
        proposed: true,
        fiveMinutePassCount: propertyAudits.filter((p) => p.fiveMinuteTestPass).length,
      },
      [ADP_EXECUTIVE_PRIORITY_EVIDENCE_TRACEABILITY]: { proposed: true },
      [ADP_EXECUTIVE_PRIORITY_5_MINUTE_TEST]: {
        proposed: true,
        proposedCopyPassCount: propertyAudits.filter((p) => p.fiveMinuteTestPass).length,
        currentProductionPassCount: propertyAudits.filter((p) => p.currentCapturesIt === "YES").length,
      },
      [ADP_EXECUTIVE_PRIORITY_PROPERTY_SPECIFICITY]: {
        proposed: true,
        uniquePriorityApprox,
        total: propertyAudits.length,
        pass: uniquePriorityApprox >= propertyAudits.length - 2,
      },
    },
    A_EXECUTIVE_SUMMARY_QUALITY_VERDICT: {
      verdict,
      averageScore10: Math.round(avgScore * 10) / 10,
      templateOpeningCount: openings.length,
      of: propertyAudits.length,
    },
    B_SCORECARD: propertyAudits.map((p) => ({
      Property: p.name,
      CurrentScore10: p.currentScore10,
      StrongestInsight: p.strongestInsight,
      CurrentSummaryCapturesIt: p.currentCapturesIt,
      InsightArchetype: (p.archetypes || []).join("+"),
      RewritePriority: p.rewritePriority,
    })),
    C_BEST_3_CURRENT_SUMMARIES: best3.map((p) => ({
      property: p.name,
      score: p.currentScore10,
      whyItWorks:
        p.currentCapturesIt === "YES"
          ? "Names a non-KPI insight (competitive leakage or representation) beyond pure metric restatement."
          : p.partialCapture
            ? "Contains a useful secondary signal (benchmark Index, Top-3, or representation) even though the opening is still templated."
            : "Relatively less templated / higher reaction score than peers.",
    })),
    D_WEAKEST_3_CURRENT_SUMMARIES: worst3.map((p) => ({
      property: p.name,
      score: p.currentScore10,
      whatItMisses: p.whatMisses,
    })),
    E_MOST_INTERESTING_NON_OBVIOUS_INSIGHT: propertyAudits.map((p) => ({
      property: p.name,
      NON_OBVIOUS_INSIGHT: p.nonObviousInsight,
    })),
    F_MANAGEMENT_QUESTION: propertyAudits.map((p) => ({
      property: p.name,
      THE_QUESTION_MANAGEMENT_SHOULD_ASK: p.managementQuestion,
    })),
    G_CURRENT_VS_PROPOSED: propertyAudits.map((p) => ({
      property: p.name,
      CURRENT_SUMMARY: p.currentSummary,
      CURRENT_SUMMARY_SCORE_10: p.currentScore10,
      WHAT_IT_MISSES: p.whatMisses,
      STRONGEST_UNDERLYING_INSIGHT: p.strongestInsight,
      WHY_THAT_INSIGHT_MATTERS: p.whyItMatters,
      MANAGEMENT_QUESTION: p.managementQuestion,
      PROPOSED_IMPROVED_EXECUTIVE_SUMMARY: p.proposedArchitectureDemo,
      PRIMARY_FOCUS: p.primaryFocus,
      SECONDARY_FOCUS: p.secondaryFocus,
      WATCH: p.watch,
      WHY_THIS_IS_THE_PRIORITY: p.whyThisIsThePriority,
      EVIDENCE_SUPPORT: p.evidenceSupport,
      EXPECTED_SIGNAL_TO_MONITOR: p.expectedSignalToMonitor,
      COULD_GM_ACT_TOMORROW: p.gmCouldActTomorrow,
    })),
    H_CROSS_HOTEL_REPETITION: {
      templateOpeningCount: openings.length,
      propertiesWithTemplateOpening: openings.map((p) => p.name),
      repeatedFrameworkOk: true,
      substantiveNarrativeRepetition:
        openings.length >= 10
          ? "FAIL — nearly every hotel opens with the same Scenario Presence vs Consideration sentence."
          : "REVIEW",
      note: "Framework repetition (strength/constraint boxes) is acceptable; identical substantive openings are not.",
    },
    I_PROPOSED_EXECUTIVE_READ_ARCHITECTURE: {
      sections: [
        "HEADLINE",
        "NON-OBVIOUS INSIGHT",
        "WHY IT MATTERS",
        "PRIMARY FOCUS",
        "SECONDARY FOCUS (optional)",
        "WATCH (optional)",
        "MANAGEMENT QUESTION",
      ],
      targetWords: "90–140 for main narrative before optional boxes",
      principles: [
        ADP_EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE,
        "Do not manufacture weakness for strong hotels",
        "Do not promise numeric improvement without causal evidence",
        "Primary focus must pass 5-minute GM test",
      ],
    },
    J_ADP_EXECUTIVE_SUMMARY_INSIGHT_VALUE: {
      gate: ADP_EXECUTIVE_SUMMARY_INSIGHT_VALUE,
      proposedRule:
        "Before publication, summary must contain ≥1 insight that requires multi-metric synthesis OR meaningful concentration OR contradiction OR evidence-backed competitive/reality insight — not KPI enumeration alone.",
    },
    K_ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY: {
      gate: ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY,
      proposedRule:
        "Substantive narrative openings must be property-specific; shared framework labels OK; identical SP-vs-C boilerplate across hotels FAIL.",
    },
    PRIORITY_FOCUS_ADDENDUM: propertyAudits.map((p) => ({
      property: p.name,
      PRIMARY_FOCUS: p.primaryFocus,
      SECONDARY_FOCUS: p.secondaryFocus,
      WATCH: p.watch,
      WHY_THIS_IS_THE_PRIORITY: p.whyThisIsThePriority,
      EVIDENCE_SUPPORT: p.evidenceSupport,
      EXPECTED_SIGNAL_TO_MONITOR: p.expectedSignalToMonitor,
      COULD_GM_ACT_TOMORROW: p.gmCouldActTomorrow,
    })),
    hardStop: true,
    externalSend: false,
    founderDecisionNeeded:
      "Review comparative audit and choose Executive Read architecture before any production rewrite.",
  };
}
