#!/usr/bin/env node
/**
 * OpenAI subject false-negative audit for Existing Hotel ADP recovery.
 * Read-only — no new LLM calls. Re-parses existing raw responses with expanded alias candidates.
 *
 *   node scripts/run-adp-openai-subject-false-negative-audit-v1.mjs
 */
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { selectLatestCertifiedOfficialPeriod } from "../lib/ai-demand-positioning/period-eligibility-v1.js";
import { detectPropertyMention } from "../lib/ai-demand-positioning/execution/response-parser.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";

const OUT = join(process.cwd(), "reports/ai-demand-positioning/adp-openai-subject-false-negative-audit-v1.json");
const PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
  "adp_hotel_phillips_kansas_city",
];

/** Extra governed aliases beyond profile — conservative, property-specific. */
const EXTRA_ALIASES = {
  adp_waterstone_boca_raton: [
    "Waterstone",
    "Waterstone Resort",
    "Waterstone Marina",
    "Waterstone Resort and Marina",
    "Waterstone Boca Raton",
    "Waterstone Boca",
    "the Waterstone",
    "Curio Collection Waterstone",
    "Waterstone Resort & Marina Boca Raton",
  ],
  adp_renaissance_times_square: [
    "Renaissance New York Times Square",
    "Renaissance Times Square",
    "Renaissance Hotel Times Square",
    "the Renaissance Times Square",
    "Renaissance NY Times Square",
    "Renaissance New York Times Square Hotel",
  ],
  adp_cambridge_beaches_bermuda: [
    "Cambridge Beaches",
    "Cambridge Beaches Resort",
    "Cambridge Beaches Resort and Spa",
    "Cambridge Beach",
    "Cambridge Beaches Bermuda",
  ],
  adp_now_now_noho: [
    "NOW NOW",
    "Now Now NoHo",
    "Now Now Noho",
    "NOW NOW NoHo",
    "NOW/NOW NOHO",
    "Now Now Hotel",
  ],
  adp_hotel_phillips_kansas_city: [
    "Hotel Phillips",
    "Hotel Phillips Kansas City",
    "the Hotel Phillips",
    "Phillips Hotel",
    "Hotel Phillips Curio",
    "Phillips Kansas City",
  ],
};

/** Competitors / collisions that must NOT count as subject (false-positive guards). */
const COLLISION_REJECT = {
  adp_renaissance_times_square: [
    /\brenaissance\s+(new york\s+)?midtown\b/i,
    /\brenaissance\s+downtown\b/i,
    /\brenaissance\s+hotel\b(?!.*times\s*square)/i,
  ],
  adp_hotel_phillips_kansas_city: [/\bphillips\s+(66|petroleum)\b/i],
  adp_now_now_noho: [/\bnow\s+now\s+then\b/i],
};

function normalizeForCompare(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildExpandedAliases(profile) {
  const set = new Set();
  const add = (v) => {
    const t = String(v || "").trim();
    if (t.length >= 4) set.add(t);
  };
  add(profile.name);
  for (const a of profile.identityAliases || profile.aliases || []) add(a);
  for (const a of EXTRA_ALIASES[profile.propertyId] || []) add(a);
  if (profile.name.includes("&")) add(profile.name.replace(/&/g, "and"));
  // Drop leading The
  for (const v of [...set]) {
    if (/^the\s+/i.test(v)) add(v.replace(/^the\s+/i, ""));
  }
  return [...set].sort((a, b) => b.length - a.length);
}

function findAliasHits(raw, aliases, propertyId) {
  const text = String(raw || "");
  const textNorm = normalizeForCompare(text);
  const hits = [];
  for (const alias of aliases) {
    const aNorm = normalizeForCompare(alias);
    if (aNorm.length < 4) continue;
    const idx = textNorm.indexOf(aNorm);
    if (idx === -1) continue;
    // Word-boundary-ish: previous/next char not alphanumeric in original norm string
    const before = idx === 0 ? " " : textNorm[idx - 1];
    const after = idx + aNorm.length >= textNorm.length ? " " : textNorm[idx + aNorm.length];
    if (/[a-z0-9]/.test(before) || /[a-z0-9]/.test(after)) {
      // Allow if alias itself is multi-token and matched fully
      if (!aNorm.includes(" ")) continue;
    }
    const rejects = COLLISION_REJECT[propertyId] || [];
    const window = text.slice(Math.max(0, text.toLowerCase().indexOf(alias.toLowerCase().slice(0, 8))), Math.max(0, text.toLowerCase().indexOf(alias.toLowerCase().slice(0, 8))) + alias.length + 40);
    if (rejects.some((re) => re.test(window) || re.test(text))) continue;
    hits.push(alias);
  }
  return hits;
}

function extractNearbyHotelishNames(raw, subjectName) {
  const names = [];
  const patterns = [
    /\*\*([^*]{4,80})\*\*/g,
    /(?:^|\n)\s*\d+[\.\)]\s+\*?\*?([^\n*]{4,80})/g,
    /(?:^|\n)\s*[-•]\s+\*?\*?([^\n*]{4,80})/g,
  ];
  for (const re of patterns) {
    let m;
    const r = new RegExp(re.source, re.flags);
    while ((m = r.exec(raw))) {
      const cand = String(m[1] || "")
        .replace(/\s+[—–-].*$/, "")
        .replace(/\s*\(.*$/, "")
        .trim();
      if (cand.length >= 4 && cand.length <= 80) names.push(cand);
    }
  }
  return [...new Set(names)].slice(0, 12);
}

function classifyAbsent({ currentMentioned, exactHits, knownAliasHits, likelyHits, ambiguous }) {
  if (currentMentioned) return "CURRENTLY_PRESENT";
  if (exactHits.length) return "EXACT_NAME_PRESENT";
  if (knownAliasHits.length) return "KNOWN_ALIAS_PRESENT";
  if (likelyHits.length) return "LIKELY_ALIAS_PRESENT";
  if (ambiguous) return "AMBIGUOUS";
  return "TRUE_ABSENT";
}

function providerPresence(observations, scenarios) {
  const byProvider = {};
  const scenarioMap = Object.fromEntries(scenarios.map((s) => [s.scenarioId, s]));
  for (const obs of observations) {
    if (!obs.parsed && obs.status !== "ok") continue;
    const p = obs.provider || "unknown";
    if (!byProvider[p]) {
      byProvider[p] = {
        observations: 0,
        mentioned: 0,
        missing: 0,
        byIntent: {},
        rankSum: 0,
        rankN: 0,
      };
    }
    const row = byProvider[p];
    row.observations += 1;
    if (obs.mentioned) {
      row.mentioned += 1;
      if (obs.position != null) {
        row.rankSum += Number(obs.position);
        row.rankN += 1;
      }
    } else row.missing += 1;
    const intent = scenarioMap[obs.scenarioId]?.intent || "unknown";
    if (!row.byIntent[intent]) row.byIntent[intent] = { observations: 0, mentioned: 0 };
    row.byIntent[intent].observations += 1;
    if (obs.mentioned) row.byIntent[intent].mentioned += 1;
  }
  for (const p of Object.keys(byProvider)) {
    const r = byProvider[p];
    r.presenceRate =
      r.observations > 0 ? Math.round((r.mentioned / r.observations) * 1000) / 10 : null;
    r.avgRank = r.rankN > 0 ? Math.round((r.rankSum / r.rankN) * 10) / 10 : null;
    for (const intent of Object.keys(r.byIntent)) {
      const i = r.byIntent[intent];
      i.presenceRate =
        i.observations > 0 ? Math.round((i.mentioned / i.observations) * 1000) / 10 : null;
    }
  }
  return byProvider;
}

function auditProperty(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const period = selectLatestCertifiedOfficialPeriod(loadAllPeriods(propertyId));
  if (!period) return { propertyId, error: "NO_CERTIFIED_PERIOD" };
  const scenarios = buildScenarioUniverse(profile);
  const scenarioMap = Object.fromEntries(scenarios.map((s) => [s.scenarioId, s]));
  const observations = (period.observations || []).filter((o) => o.parsed || o.rawResponse);
  const providerMatrix = providerPresence(observations, scenarios);

  const aliases = buildExpandedAliases(profile);
  const canonical = profile.name;
  const falseNegatives = [];
  const openaiAbsent = observations.filter(
    (o) => String(o.provider).toLowerCase() === "openai" && o.parsed && !o.mentioned
  );

  for (const obs of openaiAbsent) {
    const raw = obs.rawResponse || "";
    const current = detectPropertyMention(raw, profile);
    const hits = findAliasHits(raw, aliases, propertyId);
    const exactHits = hits.filter((h) => normalizeForCompare(h) === normalizeForCompare(canonical));
    const knownAliasHits = hits.filter((h) => normalizeForCompare(h) !== normalizeForCompare(canonical));
    // Likely: short distinctive tokens from canonical that appear as standalone hotel mentions
    const shortCore = canonical
      .split(/\s+/)
      .filter((w) => w.length >= 5 && !/^(hotel|resort|marina|collection|by|the|and|&)$/i.test(w));
    const likelyHits = [];
    for (const token of shortCore.slice(0, 2)) {
      const re = new RegExp(`\\b${token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
      if (re.test(raw) && !hits.length) {
        // Check not only inside a different hotel name collision
        likelyHits.push(token);
      }
    }
    const nearby = extractNearbyHotelishNames(raw, canonical);
    const ambiguous =
      !hits.length &&
      nearby.some((n) => {
        const nn = normalizeForCompare(n);
        return shortCore.some((t) => nn.includes(normalizeForCompare(t)));
      });

    let classification = classifyAbsent({
      currentMentioned: current.mentioned,
      exactHits,
      knownAliasHits,
      likelyHits,
      ambiguous,
    });
    if (classification === "CURRENTLY_PRESENT") classification = "PARSER_FAILURE"; // re-detect finds it with same pipeline

    const rawOpenAiName =
      exactHits[0] || knownAliasHits[0] || nearby.find((n) => shortCore.some((t) => n.toLowerCase().includes(t.toLowerCase()))) || null;

    const correctResult =
      classification === "TRUE_ABSENT" || classification === "AMBIGUOUS" || classification === "FUZZY_MATCH_POSSIBLE"
        ? "ABSENT"
        : classification === "LIKELY_ALIAS_PRESENT"
          ? "LIKELY_PRESENT"
          : "PRESENT";

    falseNegatives.push({
      propertyId,
      scenarioId: obs.scenarioId,
      intent: scenarioMap[obs.scenarioId]?.intent || null,
      rawOpenAiName,
      canonicalSubject: canonical,
      currentResult: "ABSENT",
      correctResult,
      classification,
      aliasHits: hits.slice(0, 5),
      likelyHits,
      nearbySample: nearby.slice(0, 6),
      cause:
        classification === "TRUE_ABSENT"
          ? "openai_did_not_name_subject"
          : classification === "KNOWN_ALIAS_PRESENT" || classification === "EXACT_NAME_PRESENT"
            ? "alias_not_in_detectPropertyMention_variants"
            : classification === "LIKELY_ALIAS_PRESENT"
              ? "short_token_present_needs_governed_alias"
              : classification === "AMBIGUOUS"
                ? "token_overlap_with_nearby_names"
                : "parser_or_unknown",
    });
  }

  const fnByClass = {};
  for (const row of falseNegatives) {
    fnByClass[row.classification] = (fnByClass[row.classification] || 0) + 1;
  }

  const openai = providerMatrix.openai || providerMatrix.OpenAI;
  const correctedMentioned =
    (openai?.mentioned || 0) +
    falseNegatives.filter((r) => r.correctResult === "PRESENT").length;
  const openaiObs = openai?.observations || 0;
  const correctedRate =
    openaiObs > 0 ? Math.round((correctedMentioned / openaiObs) * 1000) / 10 : null;

  return {
    propertyId,
    propertyName: canonical,
    periodId: period.periodId,
    aliasesUsed: aliases,
    providerMatrix,
    openaiAbsentCount: openaiAbsent.length,
    falseNegativeRows: falseNegatives,
    falseNegativeClassCounts: fnByClass,
    openaiPresenceCurrent: openai?.presenceRate ?? null,
    openaiPresenceIfAliasesApplied: correctedRate,
    deltaPp:
      openai?.presenceRate != null && correctedRate != null
        ? Math.round((correctedRate - openai.presenceRate) * 10) / 10
        : null,
  };
}

function conclude(propertyReports) {
  let presentFixes = 0;
  let trueAbsent = 0;
  let likely = 0;
  for (const p of propertyReports) {
    for (const row of p.falseNegativeRows || []) {
      if (row.correctResult === "PRESENT") presentFixes += 1;
      else if (row.classification === "TRUE_ABSENT") trueAbsent += 1;
      else if (row.correctResult === "LIKELY_PRESENT") likely += 1;
    }
  }
  if (presentFixes === 0 && likely === 0) return "REAL_PROVIDER_DIFFERENCE";
  if (trueAbsent === 0 && presentFixes > 0) return "MOSTLY_ENTITY_PARSING_DEFECT";
  return "MIXED";
}

const propertyReports = PROPERTIES.map(auditProperty);
const report = {
  generatedAt: new Date().toISOString(),
  title: "ADP_OPENAI_SUBJECT_FALSE_NEGATIVE_AUDIT_V1",
  subjectMatchPipeline: {
    function: "detectPropertyMention",
    file: "lib/ai-demand-positioning/execution/response-parser.js",
    method: "substring indexOf against buildNameVariants (exact/normalized lowercase)",
    variants: ["profile.name", "name with &→and", "identityAliases", "first 2–3 name tokens", "name + affiliation"],
    notUsed: ["canonical entity registry", "safe fuzzy score", "token-overlap gate", "LLM extraction for subject"],
  },
  propertyReports,
  summary: {
    properties: propertyReports.length,
    openaiAbsentTotal: propertyReports.reduce((s, p) => s + (p.openaiAbsentCount || 0), 0),
    classTotals: propertyReports.reduce((acc, p) => {
      for (const [k, v] of Object.entries(p.falseNegativeClassCounts || {})) {
        acc[k] = (acc[k] || 0) + v;
      }
      return acc;
    }, {}),
    providerDifferenceConclusion: conclude(propertyReports),
  },
};

mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ out: OUT, summary: report.summary }, null, 2));
for (const p of propertyReports) {
  console.log(
    p.propertyId,
    "openai",
    p.openaiPresenceCurrent,
    "→",
    p.openaiPresenceIfAliasesApplied,
    "Δ",
    p.deltaPp,
    p.falseNegativeClassCounts
  );
  const matrix = Object.fromEntries(
    Object.entries(p.providerMatrix || {}).map(([k, v]) => [k, { rate: v.presenceRate, n: v.observations, mentioned: v.mentioned }])
  );
  console.log("  providers", matrix);
}
