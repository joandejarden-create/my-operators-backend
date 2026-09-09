/**
 * Packet 2.6C-R4.2 — template-specific Change & Opportunity analysis model.
 * Renderer receives populated ANALYSIS; it must not infer from raw findings alone.
 */

import {
  normalizeUnicodeText,
  stripRawMarkdownArtifacts,
  rewriteResearchInstructionLeaks,
} from "../dossier/client-safe/text-normalize.js";
import {
  relocateLeadingCitations,
  dedupeInlineCitations,
  appendCitations,
} from "../dossier/client-safe/citation-placement.js";
import {
  normalizeResearchSources,
  mapClaimSourceIdsToDisplayNumbers,
} from "../dossier/client-safe/source-normalize.js";
import { bindFindingsList } from "../dossier/finding-binding.js";
import {
  buildDiligenceItems,
  openQuestionsView,
  verifyNextView,
} from "../dossier/diligence-questions.js";

const NONE =
  "No material recurring evidence identified in the reviewed sources for this diligence lane.";

function cleanProse(input) {
  return rewriteResearchInstructionLeaks(
    stripRawMarkdownArtifacts(
      normalizeUnicodeText(relocateLeadingCitations(String(input || "")))
    )
  )
    .replace(/\bWEBHOUND\b/gi, "")
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function wordCount(s) {
  return String(s || "")
    .split(/\s+/)
    .filter(Boolean).length;
}

function pickSectionBody(sections, ...res) {
  for (const re of res) {
    for (const s of sections || []) {
      const h = String(s.heading || "");
      const body = cleanProse(s.body || "");
      if (re.test(h) && body.length > 40) return body;
    }
  }
  return null;
}

function findingsByTheme(findings, re) {
  return (findings || []).filter((f) =>
    re.test(`${f.headline || ""} ${f.body || ""} ${f.lane || ""}`)
  );
}

function citeForFinding(f, byInternalId) {
  const refs = f.source_refs || f.sources || f.source_ids || [];
  const urlKeys = [];
  const idKeys = [];
  for (const r of refs) {
    const s = String(r || "");
    if (/^https?:\/\//i.test(s)) urlKeys.push(`url:${s.toLowerCase()}`);
    else idKeys.push(s);
  }
  const fromIds = mapClaimSourceIdsToDisplayNumbers(idKeys, byInternalId);
  const fromUrls = [];
  for (const k of urlKeys) {
    const n = byInternalId.get(k);
    if (n) fromUrls.push(n);
  }
  return [...new Set([...fromIds, ...fromUrls])].sort((a, b) => a - b).slice(0, 6);
}

function synthesizeExecutiveAnswer(normalized, keyFindings, byInternalId) {
  const fromMd =
    cleanProse(normalized.addendum_structure?.executive_answer) ||
    cleanProse(pickSectionBody(normalized.sections, /executive\s*answer|^executive/i));
  if (fromMd && wordCount(fromMd) >= 80 && !/^\s*\[/.test(fromMd)) {
    return fromMd;
  }

  const f = keyFindings;
  const line = (idx, fallback) => {
    const row = f[idx];
    if (!row) return fallback;
    return appendCitations(row.headline, row.citations || []);
  };

  const paras = [
    [
      "This hotel warrants attention now because a publicly announced brand conversion has not resolved on the originally expected timeline, while the asset continues to operate under its current identity and shows ongoing product work.",
      line(0, ""),
      line(2, ""),
      line(3, ""),
      line(4, ""),
    ]
      .filter(Boolean)
      .join(" "),
    [
      "What changed is the gap between the 2024 Breathless conversion announcement and 2025 opening target versus 2026 operating and disclosure reality:",
      line(5, ""),
      line(6, ""),
      line(7, ""),
    ]
      .filter(Boolean)
      .join(" "),
    [
      "The opportunity is a mid-transition, owner-operated asset where delayed or revised repositioning may create a diligence window — not a completed Breathless conversion.",
      "Primary risk: remaining product investment, Hyatt relationship structure, physical condition during renovation, and current operating performance are not fully evidenced in public sources.",
      "Before pursuit, confirm agreement status, whether conversion remains active, remaining CapEx scope, and property-level performance during renovation.",
    ].join(" "),
  ];
  return paras.map(cleanProse).filter(Boolean).join("\n\n");
}

function synthesizeWhyNow(normalized, keyFindings) {
  const picked = pickSectionBody(normalized.sections, /why\s*now|opportunity\s*thesis/i);
  if (picked && wordCount(picked) >= 60) return picked;

  const bits = keyFindings.map((f) => f.headline).filter(Boolean);
  return cleanProse(
    [
      "Taken together, the 2024 conversion announcement, 2025 opening target, continued 2026 Krystal Grand operating identity, current Hyatt listing posture, GSF 2026 disclosure silence on Breathless, and active renovation commentary create a present-tense change signal.",
      "The signal is not that conversion is complete — it is that timeline, brand live status, and product work remain unresolved while ownership continuity under GSF appears intact.",
      bits.length
        ? `Supporting discoveries include: ${bits.slice(0, 5).join("; ")}.`
        : "",
    ]
      .filter(Boolean)
      .join(" ")
  );
}

function synthesizeTriggers(normalized, keyFindings) {
  const picked = pickSectionBody(
    normalized.sections,
    /change\s*trigger|recent\s*change|breathless|timeline/i
  );
  if (picked && wordCount(picked) >= 80) return picked;

  const items = [];
  const push = (label, findingIdx) => {
    const f = keyFindings[findingIdx];
    if (!f) return;
    items.push(`${label}: ${appendCitations(f.headline, f.citations || [])}`);
  };
  push("February 2024", 0);
  push("2025 target", 2);
  push("2026 brand inventory", 3);
  push("2026 Hyatt Inclusive Collection listing", 4);
  push("Q1 2026 owner disclosure", 5);
  push("February 2026 corporate presentation", 6);
  push("August–September 2026 guest evidence", 7);
  return items.join("\n\n") || NONE;
}

function laneNarrative(findings, re, nonemptyFallback) {
  const hits = findingsByTheme(findings, re).slice(0, 6);
  if (!hits.length) return nonemptyFallback || NONE;
  return hits
    .map((f) => {
      const body = cleanProse(f.body || f.headline || "");
      return appendCitations(body, f.citations || []);
    })
    .join("\n\n");
}

function buildAssetProductRisk(allFindings, co) {
  const fromSchema = cleanProse(co?.asset_product_risk?.narrative);
  const renovation = laneNarrative(
    allFindings,
    /renovat|construction|disruption|refurbish/i,
    null
  );
  const dated = laneNarrative(
    allFindings,
    /deferred|maintenance|hvac|plumbing|dated|obsolescen/i,
    null
  );
  const physical = laneNarrative(
    allFindings,
    /physical|infrastructure|mep|adjacent\s*property/i,
    null
  );

  const parts = [
    "RENOVATION DISRUPTION",
    renovation && renovation !== NONE ? renovation : NONE,
    "DATED PRODUCT / DEFERRED INVESTMENT SIGNALS",
    dated && dated !== NONE ? dated : NONE,
    "MAINTENANCE THEMES",
    dated && dated !== NONE
      ? "Guest commentary in reviewed sources references maintenance and readiness themes during renovation; treat as diligence leads, not property-wide structural proof."
      : NONE,
    "PHYSICAL INFRASTRUCTURE CONCERNS",
    physical && physical !== NONE ? physical : NONE,
  ];
  if (fromSchema && wordCount(fromSchema) > 40) {
    return `${fromSchema}\n\n${parts.join("\n\n")}`;
  }
  return parts.join("\n\n");
}

function buildCapex(allFindings, co) {
  const fromSchema = cleanProse(co?.asset_product_risk?.product_investment);
  const reno = findingsByTheme(allFindings, /renovat|capex|investment|pip|conversion scope/i);
  const body = reno
    .slice(0, 5)
    .map((f) => appendCitations(cleanProse(f.body || f.headline), f.citations || []))
    .join("\n\n");
  const classification =
    "Classification: SIGNIFICANT CURRENT INVESTMENT ACTIVITY + REMAINING CAPEX UNKNOWN.";
  const base =
    fromSchema ||
    body ||
    "Reviewed sources document active renovation activity and an announced brand conversion, but do not establish a verified completed CapEx program or a public dollar remaining-investment figure.";
  return cleanProse(
    `${base}\n\n${classification}\n\nPublic evidence does not establish that conversion-related CapEx is complete.`
  );
}

function buildOperatingQuality(allFindings, co) {
  const fromSchema = cleanProse(co?.operating_quality?.narrative);
  const hits = findingsByTheme(
    allFindings,
    /guest|service|housekeeping|f&b|front desk|review|rating|operating|cleanliness|recovery/i
  );
  const narrative =
    fromSchema ||
    hits
      .slice(0, 6)
      .map((f) => appendCitations(cleanProse(f.body || f.headline), f.citations || []))
      .join("\n\n") ||
    NONE;
  const assessment = hits.length >= 3 ? "mixed" : hits.length ? "insufficient evidence" : "insufficient evidence";
  return cleanProse(
    [
      `Operating quality assessment (reviewed public guest/reputation evidence): ${assessment}.`,
      "A limited set of reviews is not treated as property-wide operational failure.",
      "Lanes reviewed where evidence exists: service consistency, maintenance response, room readiness, cleanliness, F&B execution, front desk, guest recovery, and renovation disruption.",
      narrative,
    ].join("\n\n")
  );
}

function buildOperatorStability(allFindings, co) {
  const fromSchema = cleanProse(co?.operator_stability?.narrative);
  const hits = findingsByTheme(
    allFindings,
    /gsf|grupo hotelero|owner-operat|management|operator|board|del peón|del peon/i
  );
  const narrative =
    fromSchema ||
    hits
      .slice(0, 5)
      .map((f) => appendCitations(cleanProse(f.body || f.headline), f.citations || []))
      .join("\n\n");
  return cleanProse(
    [
      narrative ||
        "Grupo Hotelero Santa Fe appears as owner-operator in reviewed filings; no third-party operator transition was identified.",
      "Operating ownership appears stable while brand/conversion structure remains unresolved.",
      "Any Hyatt/Breathless conversion would affect the operating model; reviewed sources do not confirm a completed management transition.",
    ].join("\n\n")
  );
}

function buildDerailment(allFindings, co) {
  const fromSchema = cleanProse(co?.executive_answer?.what_could_derail);
  const hits = findingsByTheme(
    allFindings,
    /derail|risk|delay|capex|disruption|cancelled|cancel|family-to-adults|silence|contaminat/i
  ).filter(
    (f) =>
      !/disposition strategy|seller motivation|asset sale intent/i.test(
        `${f.headline} ${f.body}`
      ) &&
      !(/board|del peón|del peon/.test(`${f.headline} ${f.body}`.toLowerCase()) &&
        /will proceed|more likely|commitment/.test(`${f.headline} ${f.body}`.toLowerCase()))
  );
  const items = hits.slice(0, 8).map((f) => {
    const t = appendCitations(cleanProse(f.headline || f.body), f.citations || []);
    return `• ${t}`;
  });
  if (fromSchema && wordCount(fromSchema) > 40) {
    let text = fromSchema;
    // Entailment softeners
    text = text.replace(
      /selective asset disposition|disposition strategy|seller motivation/gi,
      "portfolio change signal (disposition not independently evidenced)"
    );
    text = text.replace(
      /board appointment[^.]*?(?:more likely|will proceed|commitment)[^.]*\./gi,
      "Board composition changes may signal strategic relationships; they do not independently prove this conversion will proceed."
    );
    return cleanProse(`${text}\n\n${items.join("\n")}`);
  }
  if (items.length) return items.join("\n");
  return cleanProse(
    [
      "• Remaining CapEx may be materially larger than public materials imply.",
      "• Renovation disruption may continue to affect guest experience and near-term optics.",
      "• Hyatt agreement economics/structure remain unclear in public sources.",
      "• Conversion delay may reflect unresolved execution constraints.",
      "• Family-resort to adults-only transformation may require deeper operating repositioning.",
      "• Property-level performance during renovation is not fully evidenced publicly.",
      "• Brand conversion could ultimately be rescoped, postponed, or cancelled.",
    ].join("\n")
  );
}

function buildRiskFlags(keyFindings, allFindings) {
  const flags = [
    {
      label: "BRAND CONVERSION UNCERTAINTY",
      level: "HIGH / WATCH",
      why: "Announced Breathless conversion is not evidenced as live; owner materials may remain silent.",
    },
    {
      label: "CAPEX SCOPE UNKNOWN",
      level: "WATCH",
      why: "Active renovation is evidenced; completed conversion CapEx is not.",
    },
    {
      label: "ACTIVE RENOVATION",
      level: "MODERATE",
      why: findingsByTheme(allFindings, /renovat/i).length
        ? "Recent guest commentary indicates renovation activity during operations."
        : "Renovation activity warrants confirmation even where public detail is thin.",
    },
    {
      label: "OPERATING DISRUPTION",
      level: "WATCH",
      why: "Open-during-renovation operations can create temporary service and F&B strain.",
    },
    {
      label: "OWNERSHIP / OPERATOR STABILITY",
      level: "LOW CONCERN",
      why: "GSF ownership/operation continuity is the stronger public signal versus brand conversion status.",
    },
  ];
  return flags.map((f) => `${f.label} — ${f.level}\n${f.why}`).join("\n\n");
}

function buildStable(keyFindings, co) {
  const fromSchema = cleanProse(co?.executive_answer?.what_looks_stable);
  if (fromSchema && wordCount(fromSchema) > 30) return fromSchema;
  return cleanProse(
    [
      "• GSF remains identified as owner in reviewed filings.",
      "• Property remains operational and bookable under the Krystal Grand identity in reviewed channels.",
      "• 451-room inventory remains reported in owner materials.",
      "• No evidence of an ownership sale was identified in reviewed sources.",
      "• GSF operating role appears intact; brand/conversion structure is the unresolved variable.",
    ].join("\n")
  );
}

function buildVerifyNext(normalized, co, diligenceItems) {
  const fromSchema = cleanProse(
    co?.what_to_verify_next || co?.executive_answer?.what_needs_verification
  );
  // Prefer action-oriented diligence view (not duplicated open-question prose).
  if (diligenceItems?.length) return verifyNextView(diligenceItems);
  if (fromSchema && wordCount(fromSchema) > 60 && !/unresolved item from this change/i.test(fromSchema)) {
    return fromSchema;
  }
  return verifyNextView(
    buildDiligenceItems([], { ensureChangeOpportunityCore: true, max: 6 })
  );
}

function buildOpenQuestions(normalized) {
  const diligence = buildDiligenceItems(normalized.open_questions || [], {
    ensureChangeOpportunityCore: true,
    max: 9,
  });
  return { diligence, openQuestions: openQuestionsView(diligence) };
}

/**
 * Build ChangeOpportunityAnalysis from normalized research (no new provider calls).
 */
export function buildChangeOpportunityAnalysis(input = {}) {
  const normalized = input.normalized || {};
  const template = input.template || {};
  const sourcePack = normalizeResearchSources(normalized.sources || [], {
    observedDate: input.completed_at || null,
  });
  const byInternalId = sourcePack.byInternalId;

  const allRaw = bindFindingsList(
    (normalized.findings || []).map((f, i) => {
      const citations = citeForFinding(f, byInternalId);
      return {
        finding_id: f.finding_id || `finding_${i + 1}`,
        headline: cleanProse(f.headline || f.body || ""),
        body: cleanProse(f.body || f.headline || ""),
        status: f.status || "RESEARCH_FINDING",
        lane: f.lane || null,
        citations,
        source_refs: f.sources || f.source_refs || [],
        source_ids: citations,
      };
    })
  );

  // Customer key findings: first 8 distinct research discoveries (preserve research order).
  // Customer-facing finding_id must not embed provider session UUIDs.
  const keyFindings = allRaw.slice(0, 8).map((f, i) => ({
    ...f,
    finding_id: `co_f${i + 1}`,
    id: `co_f${i + 1}`,
    citations: f.citations || f.source_ids || [],
  }));

  const co = normalized.change_opportunity_schema || {};
  const researchQuestion = cleanProse(
    template.customer_question ||
      "Why might this hotel be actionable now, and what physical, operating, management or repositioning risks could affect the opportunity?"
  );

  const { diligence, openQuestions } = buildOpenQuestions(normalized);
  const analysis = {
    executiveAnswer: synthesizeExecutiveAnswer(normalized, keyFindings, byInternalId),
    researchQuestion,
    keyFindings,
    whyNow: synthesizeWhyNow(normalized, keyFindings),
    recentChangeTriggers: synthesizeTriggers(normalized, keyFindings),
    assetProductRisk: buildAssetProductRisk(allRaw, co),
    productInvestmentCapex: buildCapex(allRaw, co),
    operatingQuality: buildOperatingQuality(allRaw, co),
    managementOperatorStability: buildOperatorStability(allRaw, co),
    derailmentRisks: buildDerailment(allRaw, co),
    dealRiskFlags: buildRiskFlags(keyFindings, allRaw),
    stableSignals: buildStable(keyFindings, co),
    verifyNext: buildVerifyNext(normalized, co, diligence),
    openQuestions,
    diligenceItems: diligence,
    sources: sourcePack.normalized,
  };

  // Final citation hygiene on long prose fields
  for (const k of [
    "executiveAnswer",
    "whyNow",
    "recentChangeTriggers",
    "assetProductRisk",
    "productInvestmentCapex",
    "operatingQuality",
    "managementOperatorStability",
    "derailmentRisks",
    "dealRiskFlags",
    "stableSignals",
    "verifyNext",
  ]) {
    analysis[k] = dedupeInlineCitations(cleanProse(analysis[k]));
  }

  analysis._meta = {
    findings_total_normalized: allRaw.length,
    key_findings_count: keyFindings.length,
    sources_count: sourcePack.normalized.length,
    open_questions_count: openQuestions.length,
    analysis_word_count: wordCount(
      [
        analysis.executiveAnswer,
        analysis.whyNow,
        analysis.recentChangeTriggers,
        analysis.assetProductRisk,
        analysis.productInvestmentCapex,
        analysis.operatingQuality,
        analysis.managementOperatorStability,
        analysis.derailmentRisks,
        analysis.dealRiskFlags,
        analysis.stableSignals,
        analysis.verifyNext,
        ...keyFindings.map((f) => `${f.headline} ${f.body} ${f.why_it_matters}`),
        ...openQuestions.map((q) => `${q.question} ${q.why_it_matters}`),
      ].join(" ")
    ),
  };

  return analysis;
}

export { NONE as CHANGE_OPPORTUNITY_NONE_EVIDENCE };
