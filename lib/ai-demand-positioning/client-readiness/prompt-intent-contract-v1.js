/**
 * ADP_PROMPT_INTENT_CONTRACT_V1 — semantic intent parity across market packs.
 * Does NOT require verbatim prompt equality.
 */

import { PROMPT_INTENT_CLASS } from "./adp-client-readiness-contract-v1.js";

export const ADP_PROMPT_INTENT_CONTRACT_V1 = "ADP_PROMPT_INTENT_CONTRACT_V1";

/** Strip market localization tokens while preserving intent skeleton. */
export function canonicalizePromptTemplate(query) {
  return String(query || "")
    .toLowerCase()
    .replace(/\b(mexico city|ciudad de méxico|cdmx|paseo de la reforma|reforma|polanco)\b/gi, "{LOC}")
    .replace(/\b(cap cana|punta cana|dominican republic|dr)\b/gi, "{LOC}")
    .replace(/\b(santo domingo|colonial zone|naco|piantini)\b/gi, "{LOC}")
    .replace(/\b(cartagena|bocagrande|getsamaní|getsemani)\b/gi, "{LOC}")
    .replace(/\b(bogotá|bogota|norte|chico|usaquén|usaquen)\b/gi, "{LOC}")
    .replace(/\b(monterrey|valle)\b/gi, "{LOC}")
    .replace(/\s+/g, " ")
    .trim();
}

/** Extract slot suffix after market prefix: std_mexr_biz_01 → biz_01 */
export function scenarioSlotKey(scenarioId) {
  const parts = String(scenarioId || "").split("_");
  if (parts.length < 3) return scenarioId;
  // std_<market>_<intent>_<nn>  OR  prop_<slug>_<nn>
  if (parts[0] === "std" && parts.length >= 4) {
    return parts.slice(2).join("_");
  }
  if (parts[0] === "prop") {
    return `prop_${parts.slice(-1)[0]}`;
  }
  return parts.slice(-2).join("_");
}

const DRIFT_PAIRS = [
  { a: /\bluxury\b/i, b: /\bvalue\b|\bbudget\b|\baffordable\b/i, label: "luxury_vs_value" },
  { a: /\bfamily\b/i, b: /\bcouples?\b|\bromantic\b/i, label: "family_vs_couples" },
  { a: /\bdowntown\b|\bcentro\b|\breforma\b/i, b: /\bairport\b/i, label: "downtown_vs_airport" },
  { a: /\bbusiness\b|\bcorporate\b|\bexecutive\b/i, b: /\bleisure\b|\bweekend\b/i, label: "business_vs_leisure_cross" },
];

export function classifySlotParity(scenariosByMarket) {
  /** @type {Map<string, Array<{market, scenarioId, intent, frame, query}>>} */
  const bySlot = new Map();
  for (const [market, scenarios] of Object.entries(scenariosByMarket)) {
    for (const s of scenarios || []) {
      if (!String(s.scenarioId || "").startsWith("std_")) continue;
      const slot = scenarioSlotKey(s.scenarioId);
      if (!bySlot.has(slot)) bySlot.set(slot, []);
      bySlot.get(slot).push({
        market,
        scenarioId: s.scenarioId,
        intent: s.intent,
        frame: s.frame,
        query: s.query,
        template: canonicalizePromptTemplate(s.query),
      });
    }
  }

  const rows = [];
  let materialDrift = 0;
  let unknown = 0;
  let exact = 0;
  let localized = 0;

  for (const [slot, items] of bySlot) {
    const intents = new Set(items.map((i) => i.intent));
    const frames = new Set(items.map((i) => i.frame));
    const templates = new Set(items.map((i) => i.template));
    let classification = PROMPT_INTENT_CLASS.EXACT_INTENT_MATCH;
    let driftReasons = [];

    if (intents.size > 1 || frames.size > 1) {
      classification = PROMPT_INTENT_CLASS.MATERIAL_INTENT_DRIFT;
      driftReasons.push("intent_or_frame_mismatch");
      materialDrift += 1;
    } else if (templates.size === 1) {
      classification = PROMPT_INTENT_CLASS.EXACT_INTENT_MATCH;
      exact += 1;
    } else {
      // Different localization strings — check forbidden cross-dimension drift within slot
      const joined = items.map((i) => i.query).join(" || ");
      for (const pair of DRIFT_PAIRS) {
        // Only flag if SAME slot mixes both poles across markets AND intent doesn't explain it
        const hasA = items.some((i) => pair.a.test(i.query));
        const hasB = items.some((i) => pair.b.test(i.query));
        if (hasA && hasB && pair.label === "luxury_vs_value") {
          // business pack can mix luxury/upscale wording — treat as localization unless value/budget
          const hasValue = items.some((i) => /\bvalue\b|\bbudget\b|\baffordable\b/i.test(i.query));
          if (hasValue) {
            classification = PROMPT_INTENT_CLASS.MATERIAL_INTENT_DRIFT;
            driftReasons.push(pair.label);
          }
        } else if (hasA && hasB && pair.label !== "business_vs_leisure_cross") {
          // family vs couples within same slot is material
          if (pair.label === "family_vs_couples" || pair.label === "downtown_vs_airport") {
            classification = PROMPT_INTENT_CLASS.MATERIAL_INTENT_DRIFT;
            driftReasons.push(pair.label);
          }
        }
      }
      if (classification === PROMPT_INTENT_CLASS.EXACT_INTENT_MATCH) {
        classification = PROMPT_INTENT_CLASS.ACCEPTABLE_LOCALIZATION;
        localized += 1;
      } else if (classification === PROMPT_INTENT_CLASS.MATERIAL_INTENT_DRIFT) {
        materialDrift += 1;
      }
    }

    if (!items.length) {
      classification = PROMPT_INTENT_CLASS.UNKNOWN;
      unknown += 1;
    }

    rows.push({
      slot,
      classification,
      markets: items.map((i) => i.market),
      scenarioIds: items.map((i) => i.scenarioId),
      intents: [...intents],
      frames: [...frames],
      driftReasons,
      sampleQueries: items.map((i) => i.query).slice(0, 3),
    });
  }

  return {
    gate: "ADP_PROMPT_SEMANTIC_EQUIVALENCE",
    slotCount: rows.length,
    counts: {
      EXACT_INTENT_MATCH: exact,
      ACCEPTABLE_LOCALIZATION: localized,
      MATERIAL_INTENT_DRIFT: materialDrift,
      UNKNOWN: unknown,
    },
    pass: materialDrift === 0 && unknown === 0,
    rows,
  };
}

export function auditSubjectNeutrality(scenarios, subjectProfile) {
  const name = String(subjectProfile?.name || "").toLowerCase();
  const flags = [];
  for (const s of scenarios || []) {
    const id = String(s.scenarioId || "");
    // Governed property name probes (prop_*) may include the subject hotel by design.
    if (id.startsWith("prop_")) continue;
    const q = String(s.query || "");
    const ql = q.toLowerCase();
    if (name && name.length >= 6 && ql.includes(name)) {
      flags.push({ scenarioId: s.scenarioId, reason: "subject_name_in_prompt", query: q });
    }
  }
  return {
    gate: "ADP_UNGUIDED_PROMPT_SUBJECT_NEUTRALITY",
    pass: flags.length === 0,
    flags,
  };
}
