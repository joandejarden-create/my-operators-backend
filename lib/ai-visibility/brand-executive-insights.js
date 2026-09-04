/**
 * Deterministic executive insight boxes for Brand AI Visibility.
 * Presence-certified metrics only. No freeform LLM. No causal language.
 * Internal fields: finding / evidence / soWhat / whatToWatch / takeaway.
 * Executive Summary tiles render `takeaway` as a clear senior-exec paragraph.
 */

import { isSignalClientPublishable } from "./signal-architecture/readiness.js";
import { SIGNAL_KEYS } from "./signal-architecture/production-signals.js";

export const EXECUTIVE_INSIGHT_LAYER_VERSION =
  "ai_visibility_executive_insight_layer_v2_3";

export const INSIGHT_BOX_TITLE = "Executive Summary";

/** Internal type → client-facing title */
export const INSIGHT_CLIENT_TITLES = Object.freeze({
  STRONGEST_PRESENCE_AREA: "Strongest Brand",
  WEAKEST_PRESENCE_AREA: "Visibility Gap",
  LARGEST_COMPETITIVE_GAP: "Competitive Gap",
  PROVIDER_DISAGREEMENT: "Provider Differences",
  QUESTIONS_MISSING_PATTERN: "Questions Missing",
  MEANINGFUL_PRESENCE_CHANGE: "Period Change",
  CROSS_PROVIDER_STRENGTH: "Strongest Provider",
});

export const PERMITTED_INSIGHT_TYPES = Object.freeze([
  "STRONGEST_PRESENCE_AREA",
  "WEAKEST_PRESENCE_AREA",
  "LARGEST_COMPETITIVE_GAP",
  "PROVIDER_DISAGREEMENT",
  "QUESTIONS_MISSING_PATTERN",
  "MEANINGFUL_PRESENCE_CHANGE",
  "CROSS_PROVIDER_STRENGTH",
]);

const MIN_BOXES = 0;
const MAX_BOXES = 5;
const TARGET_MIN = 3;

function pct(v) {
  if (v == null || !Number.isFinite(v)) return null;
  const n = v <= 1 ? v * 100 : v;
  return `${(Math.round(n * 10) / 10).toFixed(1)}%`;
}

function pushBox(out, box, suppressKeys = new Set()) {
  if (!box || !box.finding || !box.evidence) return;
  if (out.length >= MAX_BOXES) return;
  const dedupeKey = box.dedupeKey || box.type;
  if (suppressKeys.has(dedupeKey) || suppressKeys.has(box.type)) return;
  const takeaway =
    box.takeaway ||
    [box.finding, box.evidence, box.soWhat].filter(Boolean).join(" ");
  out.push({
    type: box.type,
    title: box.title || INSIGHT_CLIENT_TITLES[box.type] || box.type,
    finding: box.finding,
    evidence: box.evidence,
    soWhat: box.soWhat || null,
    whatToWatch: box.whatToWatch || null,
    takeaway,
    evidenceRefs: Array.isArray(box.evidenceRefs) ? box.evidenceRefs : [],
    evidenceDeepLinks: Array.isArray(box.evidenceDeepLinks)
      ? box.evidenceDeepLinks.slice(0, 5)
      : [],
    dedupeKey,
    CAUSAL_LANGUAGE_USED: false,
  });
}

/**
 * Deterministic suppression when the same observation is already shown as a KPI/block.
 * @param {object} input
 */
export function buildInsightSuppressionKeys(input = {}) {
  const keys = new Set(input.suppressInsightTypes || []);
  if (input.suppressStrongestBecauseKpi) keys.add("STRONGEST_PRESENCE_AREA");
  if (input.suppressQuestionsMissingBecausePanel) keys.add("QUESTIONS_MISSING_PATTERN");
  if (input.suppressCompetitiveBecausePanel) keys.add("LARGEST_COMPETITIVE_GAP");
  if (input.suppressProviderBecausePanel) {
    keys.add("PROVIDER_DISAGREEMENT");
    keys.add("CROSS_PROVIDER_STRENGTH");
  }
  return keys;
}

/**
 * @param {object} input
 */
function isExplicitGlobalGeography(input = {}) {
  const key = String(input.geographyKey || "").trim().toLowerCase();
  const scope = String(input.geographyScope || "").trim().toLowerCase();
  // Regional Gap (cross-region) is ONLY allowed on an explicit Global view.
  // Selected Region/Country (e.g. CALA) must never use geographySummary comparisons,
  // even if geographySummary rows are present on the payload.
  return scope === "global" || key === "global";
}

function isCommercialRegionSummaryRow(g) {
  if (!g || g.availability === "not_monitored" || !(g.brandsMonitored > 0)) return false;
  const name = String(g.geography || g.key || "").trim();
  if (!name) return false;
  // Global is an explicit scope row, not a peer region for gap comparisons.
  if (name.toLowerCase() === "global") return false;
  return true;
}

export function buildExecutiveInsightBoxes(input = {}) {
  const boxes = [];
  const geo = input.geographyKey || "selected geography";
  const isGlobalView = isExplicitGlobalGeography(input);
  const presenceOk = isSignalClientPublishable(SIGNAL_KEYS.PRESENCE);
  const deepLinks = Array.isArray(input.evidenceDeepLinks)
    ? input.evidenceDeepLinks
    : [];
  const suppressKeys = buildInsightSuppressionKeys(input);

  if (!presenceOk) {
    return {
      version: EXECUTIVE_INSIGHT_LAYER_VERSION,
      title: INSIGHT_BOX_TITLE,
      IMPLEMENTED: true,
      boxes: [],
      insightTypes: [],
      emptyReason: "Presence not client-publishable — insight layer suppressed",
      MIN_MATERIALITY_NOT_MET: true,
    };
  }

  // STRONGEST_PRESENCE_AREA — always scoped to selected geography
  if (input.topByPresence && (input.topByPresence.presence != null || input.topByPresence.display)) {
    const t = input.topByPresence;
    const name = t.brandName || t.brandId || "Brand";
    const display = t.display || pct(t.presence) || "—";
    const scope = geo;
    const portfolioBit = input.brandsMonitoredDisplay
      ? ` among ${input.brandsMonitoredDisplay} entitled brands`
      : " among your entitled brands";
    pushBox(
      boxes,
      {
        type: "STRONGEST_PRESENCE_AREA",
        finding: `${name} leads AI Presence in ${scope} at ${display}.`,
        evidence: display,
        soWhat: `${name} is the brand owners are most likely to see from your portfolio in ${scope}.`,
        whatToWatch: `Confirm the lead holds in the next comparable ${scope} monitoring period.`,
        takeaway: `${name} leads your portfolio in ${scope} at ${display} AI Presence${portfolioBit}. When owners ask AI which brands to consider in ${scope}, ${name} is the one from your set they are most likely to see in the answer.`,
        evidenceRefs: ["aiPresence", "portfolio.topByPresence"],
        evidenceDeepLinks: deepLinks,
      },
      suppressKeys
    );
  }

  // WEAKEST / GAP
  // Global view only: compare commercial regions from geographySummary (never Global vs Region).
  // Region/country view: stay inside the selected geography (weakest brand vs strongest brand).
  const geos = Array.isArray(input.geographySummary) ? input.geographySummary : [];
  const monitoredGeos = geos.filter(isCommercialRegionSummaryRow);
  if (isGlobalView && monitoredGeos.length >= 2) {
    const ranked = [...monitoredGeos].sort((a, b) => {
      const pa = a.topBrandByAiPresence?.presence ?? -1;
      const pb = b.topBrandByAiPresence?.presence ?? -1;
      return pa - pb;
    });
    const weak = ranked[0];
    const strong = ranked[ranked.length - 1];
    const weakPresence = weak?.topBrandByAiPresence?.presence;
    const strongPresence = strong?.topBrandByAiPresence?.presence;
    const materialGap =
      typeof weakPresence === "number" &&
      typeof strongPresence === "number" &&
      strongPresence - weakPresence >= 0.05;
    if (
      materialGap &&
      weak?.topBrandByAiPresence &&
      strong?.topBrandByAiPresence &&
      weak.geography !== strong.geography
    ) {
      const weakPct =
        weak.topBrandByAiPresence.display ||
        pct(weak.topBrandByAiPresence.presence) ||
        "—";
      const strongPct =
        strong.topBrandByAiPresence.display ||
        pct(strong.topBrandByAiPresence.presence) ||
        "—";
      const weakLead =
        weak.topBrandByAiPresence.brandName ||
        weak.topBrandByAiPresence.brandId ||
        null;
      const strongLead =
        strong.topBrandByAiPresence.brandName ||
        strong.topBrandByAiPresence.brandId ||
        null;
      const leadBit =
        weakLead && strongLead
          ? ` Leading brand Presence: ${weakLead} ${weakPct} in ${weak.geography}; ${strongLead} ${strongPct} in ${strong.geography}.`
          : "";
      pushBox(
        boxes,
        {
          type: "WEAKEST_PRESENCE_AREA",
          title: "Regional Gap",
          finding: `${weak.geography} (${weakPct}) trails ${strong.geography} (${strongPct}).`,
          evidence: `${weakPct} vs ${strongPct}.`,
          soWhat: `Across Global monitoring, owners researching ${weak.geography} are less likely to see your brands than those researching ${strong.geography}.`,
          whatToWatch: "Check whether the regional gap persists in future comparable runs.",
          takeaway: `Across regions, lead AI Presence is ${weakPct} in ${weak.geography} versus ${strongPct} in ${strong.geography}.${leadBit} Owners asking about ${weak.geography} development are less likely to see your brands than those asking about ${strong.geography}.`,
          evidenceRefs: ["geographySummary"],
        },
        suppressKeys
      );
    }
  } else if (!isGlobalView && input.weakestPresence && input.topByPresence) {
    const w = input.weakestPresence;
    const t = input.topByPresence;
    if ((w.brandId || w.brandName) !== (t.brandId || t.brandName) && w.presence != null) {
      const weakName = w.brandName || w.brandId;
      const leadName = t.brandName || t.brandId;
      const weakDisplay = w.display || pct(w.presence) || "—";
      const leadDisplay = t.display || pct(t.presence) || "—";
      pushBox(
        boxes,
        {
          type: "WEAKEST_PRESENCE_AREA",
          title: "Portfolio Gap",
          finding: `${weakName} is the weakest brand in ${geo}.`,
          evidence: `${weakDisplay} vs ${leadDisplay} for ${leadName}.`,
          soWhat: `In ${geo}, ${weakName} appears less often than ${leadName} when owners ask AI.`,
          whatToWatch: `Review Questions Missing for ${weakName} in ${geo} by prompt family.`,
          takeaway: `In ${geo}, ${weakName} has the lowest AI Presence (${weakDisplay}), versus ${leadName} at ${leadDisplay}. Owners asking AI about brands in ${geo} are less likely to see ${weakName} than your portfolio leader when comparing entitled options.`,
          evidenceRefs: ["portfolio.weakestPresence"],
        },
        suppressKeys
      );
    }
  }

  // LARGEST_COMPETITIVE_GAP
  const gap = input.competitiveGap;
  if (
    gap &&
    typeof gap.subjectPresence === "number" &&
    typeof gap.peerPresence === "number" &&
    gap.peerPresence - gap.subjectPresence >= 0.1
  ) {
    const peerName = gap.peerName || "A peer brand";
    const subjectName = gap.subjectName || "your brand";
    const peerPct = pct(gap.peerPresence);
    const subjectPct = pct(gap.subjectPresence);
    const scope = geo;
    pushBox(
      boxes,
      {
        type: "LARGEST_COMPETITIVE_GAP",
        finding: `${peerName} leads ${subjectName} in ${scope}.`,
        evidence: `${peerPct} vs ${subjectPct}.`,
        soWhat: `In ${scope}, peers appearing more often means owners may hear competitor options first.`,
        whatToWatch: `Watch whether the ${scope} gap narrows in the next comparable period.`,
        takeaway: `In ${scope}, ${peerName} appears more often than ${subjectName} (${peerPct} vs ${subjectPct} AI Presence). Owners asking AI for brand options in ${scope} are more likely to hear the peer before your brand.`,
        evidenceRefs: ["competitiveContext", "aiPresence"],
      },
      suppressKeys
    );
  }

  // PROVIDER_DISAGREEMENT / CROSS_PROVIDER_STRENGTH
  const xp = input.crossProvider;
  if (xp && xp.NOT_COMPARABLE !== true && xp.PROVIDER_DISAGREEMENT?.status === "DISAGREE") {
    const strong = xp.STRONGEST_PROVIDER_BY_PRESENCE;
    const weak = xp.WEAKEST_PROVIDER_BY_PRESENCE;
    const strongLabel = formatProvider(strong?.provider);
    const weakLabel = formatProvider(weak?.provider);
    const strongPct = pct(strong?.rate);
    const weakPct = pct(weak?.rate);
    const spread = pct(xp.PRESENCE_RANGE?.spread);
    const byProvider = formatProviderPresenceList(xp);
    const providerNames = listProviderNames(xp);
    pushBox(
      boxes,
      {
        type: "PROVIDER_DISAGREEMENT",
        finding: `${strongLabel} (${strongPct}) leads ${weakLabel} (${weakPct}) in ${geo}.`,
        evidence: byProvider || `${strongLabel} ${strongPct}; ${weakLabel} ${weakPct}; spread ${spread}.`,
        soWhat: `Owners on ${strongLabel} are more likely to see your brands than owners on ${weakLabel}.`,
        whatToWatch: "Confirm whether the gap persists on the next comparable window.",
        takeaway: `In ${geo}, Presence differs across ${providerNames || "AI providers"}: ${strongLabel} leads at ${strongPct}, ${weakLabel} trails at ${weakPct}${spread ? ` (spread ${spread})` : ""}. An owner on ${strongLabel} is more likely to see your brands than one on ${weakLabel}, so compare providers before drawing conclusions.`,
        evidenceRefs: ["crossProviderPresence.PROVIDER_DISAGREEMENT"],
      },
      suppressKeys
    );
  } else if (
    xp &&
    xp.NOT_COMPARABLE !== true &&
    xp.STRONGEST_PROVIDER_BY_PRESENCE &&
    (xp.PROVIDERS_MONITORED || []).length >= 2
  ) {
    const strong = xp.STRONGEST_PROVIDER_BY_PRESENCE;
    const strongLabel = formatProvider(strong.provider);
    const strongPct = pct(strong.rate);
    const byProvider = formatProviderPresenceList(xp);
    const providerNames = listProviderNames(xp);
    pushBox(
      boxes,
      {
        type: "CROSS_PROVIDER_STRENGTH",
        finding: `${strongLabel} shows the strongest Presence in ${geo} at ${strongPct}.`,
        evidence: byProvider || `${strongLabel} ${strongPct}.`,
        soWhat: `Owners using ${strongLabel} are the most likely to see your brands among ${providerNames || "these providers"}.`,
        whatToWatch: "Compare provider-level Presence on the next monitoring window.",
        takeaway: `Across ${providerNames || "comparable AI providers"} in ${geo}, Presence is highest on ${strongLabel} at ${strongPct}${byProvider ? ` (${byProvider})` : ""}. Owners on ${strongLabel} are the most likely to see your brands; weaker platforms under-represent you.`,
        evidenceRefs: ["crossProviderPresence.STRONGEST_PROVIDER_BY_PRESENCE"],
      },
      suppressKeys
    );
  }

  // QUESTIONS_MISSING_PATTERN
  const qm = input.questionsMissing;
  if (qm && typeof qm.value === "number" && qm.value > 0) {
    const missingN = qm.value;
    const denom = qm.denominator;
    const countPhrase =
      denom != null && Number.isFinite(denom)
        ? `${missingN} of ${denom} owner questions in ${geo}`
        : `${missingN} owner question${missingN === 1 ? "" : "s"} in ${geo}`;
    const shareBit =
      denom != null && denom > 0
        ? ` (${pct(missingN / denom)} of the ${geo} question set)`
        : qm.display
          ? ` (${qm.display})`
          : "";
    const fam = input.topMissingPromptFamily;
    const familyBit =
      fam && fam.promptFamily && fam.QUESTIONS_MISSING > 0
        ? ` The largest miss is ${fam.promptFamily} (${fam.QUESTIONS_MISSING} of ${fam.MONITORED_QUESTIONS} questions in that family), where owners asking AI did not see your portfolio.`
        : fam && fam.promptFamily
          ? ` Activity in this view concentrates in ${fam.promptFamily}, where owners asking AI may not see your portfolio.`
          : ` Those are owner questions where AI left your portfolio out of the answer set.`;
    pushBox(
      boxes,
      {
        type: "QUESTIONS_MISSING_PATTERN",
        finding: `Your brands were absent from ${countPhrase}.`,
        evidence: denom != null ? `${missingN} of ${denom}` : String(missingN),
        soWhat: `On those ${missingN} question${missingN === 1 ? "" : "s"}, AI answers did not include your portfolio.`,
        whatToWatch: "Review which prompt families drove the misses.",
        takeaway: `Your brands did not appear in ${countPhrase}${shareBit}.${familyBit}`,
        evidenceRefs: ["questionsMissing"],
        evidenceDeepLinks: deepLinks.filter(
          (l) => l && (l.presenceObserved === false || l.kind === "missing")
        ).length
          ? deepLinks.filter(
              (l) => l && (l.presenceObserved === false || l.kind === "missing")
            )
          : deepLinks,
      },
      suppressKeys
    );
  }

  // MEANINGFUL_PRESENCE_CHANGE
  const ch = input.presenceChange;
  if (ch && ch.comparable === true && typeof ch.deltaPp === "number" && Math.abs(ch.deltaPp) >= 5) {
    const up = ch.deltaPp > 0;
    const dir = up ? "increased" : "decreased";
    const delta = `${up ? "+" : ""}${Math.round(ch.deltaPp * 10) / 10} pp`;
    const brandBit = ch.brandName ? ` for ${ch.brandName}` : "";
    const scope = geo;
    pushBox(
      boxes,
      {
        type: "MEANINGFUL_PRESENCE_CHANGE",
        finding: `AI Presence ${dir}${brandBit} in ${scope}.`,
        evidence: `${delta} vs the prior comparable ${scope} run.`,
        soWhat: up
          ? `Owners in ${scope} are more likely to see this brand than in the prior comparable period.`
          : `Owners in ${scope} are less likely to see this brand than in the prior comparable period.`,
        whatToWatch: `Confirm whether the ${scope} direction continues next period.`,
        takeaway: `In ${scope}, AI Presence ${dir}${brandBit} by ${delta} versus the prior comparable monitoring run. Watch whether owner-facing visibility in ${scope} keeps moving in this direction.`,
        evidenceRefs: ["presenceTrend", "aiPresenceChange"],
      },
      suppressKeys
    );
  }

  return {
    version: EXECUTIVE_INSIGHT_LAYER_VERSION,
    title: INSIGHT_BOX_TITLE,
    IMPLEMENTED: true,
    boxes,
    insightTypes: boxes.map((b) => b.type),
    boxCount: boxes.length,
    emptyReason:
      boxes.length === 0
        ? "No insight met governed evidence thresholds for this view."
        : null,
    MIN_MATERIALITY_NOT_MET: boxes.length < TARGET_MIN && boxes.length > MIN_BOXES,
    MAX_BOXES,
    TARGET_MIN,
    QUOTA_NOT_FORCED: true,
    permittedTypes: PERMITTED_INSIGHT_TYPES,
    clientTitles: INSIGHT_CLIENT_TITLES,
    FREEFORM_LLM: false,
    CAUSAL_LANGUAGE_USED: false,
  };
}

function formatProvider(id) {
  if (!id) return "provider";
  const key = String(id).toLowerCase();
  const labels = {
    openai: "ChatGPT",
    chatgpt: "ChatGPT",
    gemini: "Gemini",
    perplexity: "Perplexity",
    claude: "Claude",
  };
  if (labels[key]) return labels[key];
  const s = String(id);
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function providerRate(row) {
  if (!row) return null;
  if (typeof row.presenceRate === "number") return row.presenceRate;
  if (typeof row.rate === "number") return row.rate;
  return null;
}

/** e.g. "ChatGPT 91.7%, Claude 80%, Gemini 70%, Perplexity 61.9%" */
function formatProviderPresenceList(xp) {
  const breakdown = Array.isArray(xp?.PROVIDER_PRESENCE_BREAKDOWN)
    ? xp.PROVIDER_PRESENCE_BREAKDOWN
    : [];
  const parts = breakdown
    .map((row) => {
      const rate = providerRate(row);
      if (rate == null || !Number.isFinite(rate)) return null;
      const label = formatProvider(row.provider || row.label || row.id);
      return `${label} ${pct(rate)}`;
    })
    .filter(Boolean);
  if (parts.length) return parts.join(", ");
  const monitored = Array.isArray(xp?.PROVIDERS_MONITORED)
    ? xp.PROVIDERS_MONITORED.map(formatProvider)
    : [];
  return monitored.length ? monitored.join(", ") : null;
}

function listProviderNames(xp) {
  const monitored = Array.isArray(xp?.PROVIDERS_MONITORED)
    ? xp.PROVIDERS_MONITORED.map(formatProvider)
    : [];
  if (monitored.length >= 2) {
    return `${monitored.slice(0, -1).join(", ")} and ${monitored[monitored.length - 1]}`;
  }
  return monitored[0] || null;
}

/**
 * Brand-scoped Detail Executive Summary tiles.
 * Uses the selected brand + geography/provider/language — never portfolio strongest/weakest.
 * @param {object} input
 */
export function buildBrandDetailInsightBoxes(input = {}) {
  const boxes = [];
  const geo = input.geographyKey || "selected geography";
  const brandName = input.brandName || input.brandId || "This brand";
  const providerLabel = input.providerLabel || formatProvider(input.provider) || "this provider";
  const languageLabel =
    input.language === "es"
      ? "Spanish"
      : input.language === "en"
        ? "English"
        : input.language || null;
  const filterBit = languageLabel
    ? `${geo} · ${providerLabel} · ${languageLabel}`
    : `${geo} · ${providerLabel}`;
  const presenceOk = isSignalClientPublishable(SIGNAL_KEYS.PRESENCE);
  if (!presenceOk) {
    return {
      version: EXECUTIVE_INSIGHT_LAYER_VERSION,
      title: INSIGHT_BOX_TITLE,
      scope: "brand_detail",
      IMPLEMENTED: true,
      boxes: [],
      insightTypes: [],
      emptyReason: "Presence not client-publishable — insight layer suppressed",
      brandId: input.brandId || null,
      brandName,
    };
  }

  const presenceDisplay =
    input.presenceDisplay ||
    pct(input.presenceValue) ||
    null;
  const rankDisplay = input.rankDisplay || null;
  if (presenceDisplay) {
    const rankBit = rankDisplay ? ` (${rankDisplay} among peers)` : "";
    pushBox(boxes, {
      type: "STRONGEST_PRESENCE_AREA",
      title: "Where You Stand",
      finding: `${brandName} AI Presence is ${presenceDisplay} in ${geo}.`,
      evidence: presenceDisplay,
      soWhat: `Under ${filterBit}, this is how often ${brandName} appears when owners ask AI.`,
      whatToWatch: "Compare peers and Questions Missing for the same filters.",
      takeaway: `${brandName} has ${presenceDisplay} AI Presence in ${geo} on ${providerLabel}${languageLabel ? ` (${languageLabel})` : ""}${rankBit}. That is the observed rate for this brand under the current filters—not a portfolio leaderboard.`,
      evidenceRefs: ["brand.kpis.aiPresence"],
    });
  }

  const gap = input.competitiveGap;
  if (
    gap &&
    typeof gap.subjectPresence === "number" &&
    typeof gap.peerPresence === "number" &&
    gap.peerPresence - gap.subjectPresence >= 0.1
  ) {
    const peerName = gap.peerName || "A peer brand";
    const peerPct = pct(gap.peerPresence);
    const subjectPct = pct(gap.subjectPresence);
    pushBox(boxes, {
      type: "LARGEST_COMPETITIVE_GAP",
      finding: `${peerName} leads ${brandName} in ${geo}.`,
      evidence: `${peerPct} vs ${subjectPct}.`,
      soWhat: `In ${geo}, owners are more likely to hear ${peerName} than ${brandName}.`,
      whatToWatch: `Watch whether the ${geo} gap narrows in the next comparable period.`,
      takeaway: `In ${geo} on ${providerLabel}, ${peerName} appears more often than ${brandName} (${peerPct} vs ${subjectPct} AI Presence). Owners asking AI for brand options under these filters are more likely to hear the peer before ${brandName}.`,
      evidenceRefs: ["brand.competitiveGap"],
    });
  }

  const qm = input.questionsMissing;
  if (qm && typeof qm.value === "number" && qm.value > 0) {
    const missingN = qm.value;
    const denom = qm.denominator;
    const countPhrase =
      denom != null && Number.isFinite(denom)
        ? `${missingN} of ${denom} owner questions in ${geo}`
        : `${missingN} owner question${missingN === 1 ? "" : "s"} in ${geo}`;
    const shareBit =
      denom != null && denom > 0
        ? ` (${pct(missingN / denom)} of the ${geo} question set)`
        : qm.display
          ? ` (${qm.display})`
          : "";
    const fam = input.topMissingPromptFamily;
    const familyBit =
      fam && fam.promptFamily && fam.QUESTIONS_MISSING > 0
        ? ` The largest miss is ${fam.promptFamily} (${fam.QUESTIONS_MISSING} of ${fam.MONITORED_QUESTIONS} questions in that family).`
        : fam && fam.intentTerritory && (fam.missingN > 0 || fam.presentN != null)
          ? ` Weakest coverage is in ${fam.intentTerritory}${fam.display ? ` (${fam.display})` : ""}.`
          : ` Those are owner questions where AI did not include ${brandName}.`;
    pushBox(boxes, {
      type: "QUESTIONS_MISSING_PATTERN",
      finding: `${brandName} was absent from ${countPhrase}.`,
      evidence: denom != null ? `${missingN} of ${denom}` : String(missingN),
      soWhat: `On those questions, AI answers did not include ${brandName}.`,
      whatToWatch: "Open the Questions Missing watchlist for this brand.",
      takeaway: `${brandName} did not appear in ${countPhrase}${shareBit} under ${filterBit}.${familyBit}`,
      evidenceRefs: ["brand.kpis.questionsMissing"],
    });
  }

  const xp = input.crossProvider;
  if (xp && xp.NOT_COMPARABLE !== true && xp.PROVIDER_DISAGREEMENT?.status === "DISAGREE") {
    const strong = xp.STRONGEST_PROVIDER_BY_PRESENCE;
    const weak = xp.WEAKEST_PROVIDER_BY_PRESENCE;
    const strongLabel = formatProvider(strong?.provider);
    const weakLabel = formatProvider(weak?.provider);
    const strongPct = pct(strong?.rate);
    const weakPct = pct(weak?.rate);
    const spread = pct(xp.PRESENCE_RANGE?.spread);
    const providerNames = listProviderNames(xp);
    pushBox(boxes, {
      type: "PROVIDER_DISAGREEMENT",
      finding: `${brandName} Presence differs: ${strongLabel} ${strongPct} vs ${weakLabel} ${weakPct}.`,
      evidence: `${strongLabel} ${strongPct}; ${weakLabel} ${weakPct}`,
      soWhat: `Owners on ${strongLabel} are more likely to see ${brandName} than owners on ${weakLabel}.`,
      whatToWatch: "Compare provider rows in Coverage Diagnostics.",
      takeaway: `For ${brandName} in ${geo}, Presence differs across ${providerNames || "AI providers"}: ${strongLabel} leads at ${strongPct}, ${weakLabel} trails at ${weakPct}${spread ? ` (spread ${spread})` : ""}. An owner on ${strongLabel} is more likely to see ${brandName} than one on ${weakLabel}.`,
      evidenceRefs: ["brand.providerPresencePanel"],
    });
  } else if (
    xp &&
    xp.NOT_COMPARABLE !== true &&
    xp.STRONGEST_PROVIDER_BY_PRESENCE &&
    (xp.PROVIDERS_MONITORED || []).length >= 2
  ) {
    const strong = xp.STRONGEST_PROVIDER_BY_PRESENCE;
    const strongLabel = formatProvider(strong.provider);
    const strongPct = pct(strong.rate);
    const providerNames = listProviderNames(xp);
    pushBox(boxes, {
      type: "CROSS_PROVIDER_STRENGTH",
      title: "Strongest Provider",
      finding: `${brandName} is strongest on ${strongLabel} at ${strongPct}.`,
      evidence: `${strongLabel} ${strongPct}`,
      soWhat: `Owners using ${strongLabel} are the most likely to see ${brandName}.`,
      whatToWatch: "Compare provider-level Presence on the next monitoring window.",
      takeaway: `Across ${providerNames || "comparable AI providers"} in ${geo}, ${brandName} Presence is highest on ${strongLabel} at ${strongPct}. Owners on ${strongLabel} are the most likely to see this brand.`,
      evidenceRefs: ["brand.providerPresencePanel"],
    });
  }

  const ch = input.presenceChange;
  if (ch && ch.comparable === true && typeof ch.deltaPp === "number" && Math.abs(ch.deltaPp) >= 5) {
    const up = ch.deltaPp > 0;
    const dir = up ? "increased" : "decreased";
    const delta = `${up ? "+" : ""}${Math.round(ch.deltaPp * 10) / 10} pp`;
    pushBox(boxes, {
      type: "MEANINGFUL_PRESENCE_CHANGE",
      finding: `${brandName} AI Presence ${dir} in ${geo}.`,
      evidence: `${delta} vs the prior comparable ${geo} run.`,
      soWhat: up
        ? `Owners in ${geo} are more likely to see ${brandName} than in the prior comparable period.`
        : `Owners in ${geo} are less likely to see ${brandName} than in the prior comparable period.`,
      whatToWatch: `Confirm whether the ${geo} direction continues next period.`,
      takeaway: `In ${geo} on ${providerLabel}, AI Presence for ${brandName} ${dir} by ${delta} versus the prior comparable monitoring run. Watch whether owner-facing visibility keeps moving in this direction.`,
      evidenceRefs: ["brand.kpis.aiPresence.delta"],
    });
  }

  return {
    version: EXECUTIVE_INSIGHT_LAYER_VERSION,
    title: INSIGHT_BOX_TITLE,
    scope: "brand_detail",
    IMPLEMENTED: true,
    boxes,
    insightTypes: boxes.map((b) => b.type),
    boxCount: boxes.length,
    emptyReason:
      boxes.length === 0
        ? "No brand-scoped insight met governed evidence thresholds for this filter selection."
        : null,
    brandId: input.brandId || null,
    brandName,
    geographyKey: geo,
    provider: input.provider || null,
    language: input.language || null,
    MAX_BOXES,
    TARGET_MIN,
    QUOTA_NOT_FORCED: true,
    FREEFORM_LLM: false,
    CAUSAL_LANGUAGE_USED: false,
  };
}

/**
 * Derive cross-provider insight input from Detail Provider Presence panel rows.
 * @param {{ rows?: object[] }|null|undefined} panel
 */
export function crossProviderFromProviderPresencePanel(panel) {
  const rows = (panel?.rows || []).filter(
    (r) => typeof r.PRESENCE_RATE === "number" && Number.isFinite(r.PRESENCE_RATE)
  );
  if (rows.length < 2) return null;
  const sorted = [...rows].sort((a, b) => b.PRESENCE_RATE - a.PRESENCE_RATE);
  const strong = sorted[0];
  const weak = sorted[sorted.length - 1];
  const spread = strong.PRESENCE_RATE - weak.PRESENCE_RATE;
  return {
    NOT_COMPARABLE: false,
    PROVIDERS_MONITORED: rows.map((r) => r.PROVIDER).filter(Boolean),
    PROVIDER_PRESENCE_BREAKDOWN: rows.map((r) => ({
      provider: r.PROVIDER,
      rate: r.PRESENCE_RATE,
    })),
    STRONGEST_PROVIDER_BY_PRESENCE: {
      provider: strong.PROVIDER,
      rate: strong.PRESENCE_RATE,
    },
    WEAKEST_PROVIDER_BY_PRESENCE: {
      provider: weak.PROVIDER,
      rate: weak.PRESENCE_RATE,
    },
    PRESENCE_RANGE: { spread },
    PROVIDER_DISAGREEMENT: {
      status: spread >= 0.05 ? "DISAGREE" : "AGREE",
    },
  };
}
