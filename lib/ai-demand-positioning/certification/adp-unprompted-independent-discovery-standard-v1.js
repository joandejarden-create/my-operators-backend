/**
 * ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1
 *
 * Governs which Core scenarios/observations may enter independent-discovery KPIs
 * (AI Consideration, Scenario Presence, Traveler Needs Appeared/Missing,
 * rank appearance, competitive displacement co-occurrence).
 *
 * Does NOT activate three-metric V2 endorsement framework.
 * Does NOT redefine mention-based Consideration — only the eligible universe.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyScenarioDiscoveryType,
  SCENARIO_DISCOVERY_CLASS,
  loadDurableArchiveObservation,
} from "./adp-foundational-engine-and-metrics-certification-v1.js";

export const ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1 =
  "ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1";
export const UNPROMPTED_METHODOLOGY_VERSION = "adp_unprompted_independent_discovery_v1";

/** Historical (contaminated-universe) mention methodology — preserved for lineage. */
export const HISTORICAL_MENTION_CONSIDERATION_METHODOLOGY_ID =
  "ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1";

/** Corrected customer appearance KPIs — independent unprompted universe only. */
export const CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID =
  "ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1";

/** Founder-facing eligibility classes (alias of internal D1–D5). */
export const PROMPT_ELIGIBILITY_CLASS = Object.freeze({
  INDEPENDENT_UNPROMPTED: "INDEPENDENT_UNPROMPTED",
  SUBJECT_PROMPTED: "SUBJECT_PROMPTED",
  COMPETITIVE_COMPARISON: "COMPETITIVE_COMPARISON",
  ATTRIBUTE_VERIFICATION: "ATTRIBUTE_VERIFICATION",
  AMBIGUOUS_REVIEW_REQUIRED: "AMBIGUOUS_REVIEW_REQUIRED",
});

export { SCENARIO_DISCOVERY_CLASS, classifyScenarioDiscoveryType };

export function toPromptEligibilityClass(discoveryClass, independentlyEligible, ambiguous) {
  if (ambiguous) return PROMPT_ELIGIBILITY_CLASS.AMBIGUOUS_REVIEW_REQUIRED;
  if (independentlyEligible) return PROMPT_ELIGIBILITY_CLASS.INDEPENDENT_UNPROMPTED;
  if (discoveryClass === SCENARIO_DISCOVERY_CLASS.D3_COMPETITIVE_COMPARISON) {
    return PROMPT_ELIGIBILITY_CLASS.COMPETITIVE_COMPARISON;
  }
  if (discoveryClass === SCENARIO_DISCOVERY_CLASS.D4_ATTRIBUTE_VERIFICATION) {
    return PROMPT_ELIGIBILITY_CLASS.ATTRIBUTE_VERIFICATION;
  }
  if (
    discoveryClass === SCENARIO_DISCOVERY_CLASS.D2_ASSISTED_CONSIDERATION ||
    discoveryClass === SCENARIO_DISCOVERY_CLASS.D1_INDEPENDENT_DISCOVERY
  ) {
    // Contaminated D1 (e.g. alias hit) or assisted → subject prompted / fail closed
    return PROMPT_ELIGIBILITY_CLASS.SUBJECT_PROMPTED;
  }
  return PROMPT_ELIGIBILITY_CLASS.AMBIGUOUS_REVIEW_REQUIRED;
}

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function escapeRe(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Broader subject contamination than exact canonical name.
 */
const BRAND_OR_GENERIC_NAME_TOKEN =
  /^(hotel|hotels|the|and|by|a|an|at|of|marriott|hilton|hyatt|ihg|accor|city|resort|collection|jw|w|st|le|westin|sheraton|autograph|tribute|curio|renaissance|regis|gaylord|aloft|element|delta|protea|design|radisson|blu|voco|novotel|pullman|sofitel|fairmont|raffles|swissotel|ibis|mercure|yotel|fiesta|americana|grand|faranda)$/i;

export function detectSubjectContamination(promptText, profile = {}) {
  const p = String(promptText || "");
  const tokens = new Set();
  // City / market geography must NEVER count as subject contamination — independent
  // discovery prompts legitimately name the destination (e.g. "A Coruña", "Madrid").
  const geoBlock = new Set(
    [profile?.city, profile?.market, profile?.submarket, profile?.region, profile?.state, profile?.country]
      .filter(Boolean)
      .flatMap((g) =>
        String(g)
          .split(/[\/,]/)
          .map((x) => x.trim())
          .filter((x) => x.length >= 3)
      )
      .map((x) => x.toLowerCase())
  );

  // Hotel names often end with destination labels ("… Monterrey Valle", "… Geneva Lake").
  // Those trailing location tokens are geography, not subject identity — absorb into geoBlock
  // when a brand/generic token appears earlier in the name.
  const rawNameParts = String(profile?.name || profile?.displayName || "")
    .split(/\s+/)
    .filter(Boolean);
  const brandIdx = rawNameParts.findIndex(
    (w) => BRAND_OR_GENERIC_NAME_TOKEN.test(w) && !/^(the|a|an|and|by|at|of)$/i.test(w)
  );
  if (brandIdx >= 0) {
    for (let j = brandIdx + 1; j < rawNameParts.length; j++) {
      const w = rawNameParts[j];
      if (BRAND_OR_GENERIC_NAME_TOKEN.test(w)) continue;
      if (String(w).length >= 4) geoBlock.add(String(w).toLowerCase());
    }
  }

  const isGenericOrGeoToken = (s) => {
    const low = String(s || "").toLowerCase();
    if (geoBlock.has(low)) return true;
    for (const g of geoBlock) {
      if (g.length >= 4 && low === g) return true;
      // Token equals a geo word inside multi-word city (e.g. "Coruña" in "A Coruña")
      if (g.split(/\s+/).some((part) => part.length >= 4 && part === low)) return true;
    }
    return BRAND_OR_GENERIC_NAME_TOKEN.test(s);
  };
  const pushIdentity = (t) => {
    const s = String(t || "").trim();
    // Full identity strings (name/brand/alias) — keep even if they contain a city.
    if (s.length >= 5) tokens.add(s);
  };
  pushIdentity(profile?.name);
  pushIdentity(profile?.displayName);
  pushIdentity(profile?.brand);
  pushIdentity(profile?.affiliation);
  for (const a of profile?.identityAliases || []) pushIdentity(a);

  // Parent-brand / loyalty stems from affiliation or name (profile.brand often unset).
  const affOrBrand = `${profile?.affiliation || ""} ${profile?.brand || ""} ${profile?.name || ""}`;
  if (/\bmarriott\b/i.test(affOrBrand) || /\bjw\b/i.test(affOrBrand)) {
    tokens.add("Marriott");
    tokens.add("Bonvoy");
    tokens.add("JW Marriott");
  }
  if (/\bwestin\b/i.test(affOrBrand)) tokens.add("Westin");
  if (/\bsheraton\b/i.test(affOrBrand)) tokens.add("Sheraton");
  if (/\bhilton\b/i.test(affOrBrand)) {
    tokens.add("Hilton");
    tokens.add("Honors");
  }
  if (/\bhyatt\b/i.test(affOrBrand)) tokens.add("Hyatt");
  if (/\byotel\b/i.test(affOrBrand)) tokens.add("YOTEL");

  // Significant non-geo name tokens only (never city/destination fragments like "Coruña" / "Valle")
  const nameParts = String(profile?.name || "")
    .split(/\s+/)
    .filter((w) => w.length >= 4 && !isGenericOrGeoToken(w));
  for (const w of nameParts) tokens.add(w);

  const hits = [];
  for (const t of tokens) {
    if (new RegExp(`\\b${escapeRe(t)}\\b`, "i").test(p)) hits.push(t);
  }
  const urlHit =
    Boolean(profile?.website) &&
    p.toLowerCase().includes(String(profile.website).toLowerCase().replace(/^https?:\/\//, ""));
  if (urlHit) hits.push("OFFICIAL_URL");
  if (profile?.officialPropertyPageUrl) {
    const u = String(profile.officialPropertyPageUrl).toLowerCase();
    if (u && p.toLowerCase().includes(u.replace(/^https?:\/\//, ""))) hits.push("PROPERTY_PAGE_URL");
  }

  const leadingAssist =
    hits.length > 0 &&
    /\b(?:recommend|best|ideal|should i (?:stay|book)|is .+ (?:good|suitable))\b/i.test(p);

  return {
    contaminated: hits.length > 0,
    hits,
    leadingAssist,
    hasOfficialUrl: urlHit,
  };
}

/**
 * Full eligibility judgment for one scenario prompt.
 */
export function assessIndependentDiscoveryEligibility(promptText, profile = {}) {
  const base = classifyScenarioDiscoveryType(promptText, profile);
  const contamination = detectSubjectContamination(promptText, profile);
  const p = String(promptText || "").trim();

  const accommodationNeed =
    p.length >= 20 &&
    /\b(hotel|hotels|stay|staying|accommodation|lodging|where (?:should|to) stay|overnight|visit|trip|weekend|meeting|meetings|offsite|celebration|business|leisure|family|host|board dinner|client meetings)\b/i.test(
      p
    );

  const geoRelevant =
    !profile?.city ||
    new RegExp(escapeRe(profile.city), "i").test(p) ||
    new RegExp(escapeRe(profile.market || ""), "i").test(p) ||
    /\b(madrid|centro|puerta del sol|cancun|mexico|washington|bethesda|london|rome)\b/i.test(p);

  let eligible = base.discoveryClass === SCENARIO_DISCOVERY_CLASS.D1_INDEPENDENT_DISCOVERY;
  const exclusions = [];

  if (contamination.contaminated) {
    eligible = false;
    exclusions.push("SUBJECT_OR_ALIAS_IN_PROMPT");
  }
  if (base.discoveryClass === SCENARIO_DISCOVERY_CLASS.D2_ASSISTED_CONSIDERATION) {
    eligible = false;
    exclusions.push("ASSISTED_CONSIDERATION");
  }
  if (base.discoveryClass === SCENARIO_DISCOVERY_CLASS.D3_COMPETITIVE_COMPARISON) {
    eligible = false;
    exclusions.push("COMPETITIVE_COMPARISON_OWN_CONTRACT");
  }
  if (base.discoveryClass === SCENARIO_DISCOVERY_CLASS.D4_ATTRIBUTE_VERIFICATION) {
    eligible = false;
    exclusions.push("ATTRIBUTE_VERIFICATION_OWN_CONTRACT");
  }
  if (!accommodationNeed) {
    eligible = false;
    exclusions.push("NOT_ACCOMMODATION_NEED");
  }
  if (!geoRelevant) {
    exclusions.push("GEO_RELEVANCE_WEAK");
  }
  if (contamination.leadingAssist) {
    eligible = false;
    exclusions.push("LEADING_ASSISTED_LANGUAGE");
  }

  // Brand-specific prompts naming a major chain/loyalty program that is NOT the
  // subject hotel's brand are not independent destination discovery (e.g. a
  // "Marriott/Bonvoy" prompt must not enter a Westin hotel's unprompted denom).
  const majorBrandInPrompt = p.match(
    /\b(marriott|bonvoy|hilton|honors|hyatt|ihg|westin|sheraton|radisson|novotel|sofitel|fairmont|yotel|accor)\b/gi
  );
  if (majorBrandInPrompt?.length) {
    const subjectBrandBlob = `${profile?.brand || ""} ${profile?.affiliation || ""} ${profile?.name || ""}`.toLowerCase();
    const foreignBrand = [...new Set(majorBrandInPrompt.map((x) => x.toLowerCase()))].some(
      (b) => {
        if (b === "bonvoy") return !/marriott|jw|bonvoy/.test(subjectBrandBlob);
        if (b === "honors") return !/hilton|honors/.test(subjectBrandBlob);
        if (b === "marriott") return !/marriott|jw|bonvoy|ac hotels/.test(subjectBrandBlob);
        return !subjectBrandBlob.includes(b);
      }
    );
    if (foreignBrand) {
      eligible = false;
      exclusions.push("OTHER_BRAND_SPECIFIC_PROMPT");
    }
  }

  // Ambiguous: short prompts with brand-like tokens not in profile aliases
  const ambiguous =
    !eligible &&
    base.discoveryClass === SCENARIO_DISCOVERY_CLASS.D5_OTHER;

  const eligibilityClass = toPromptEligibilityClass(
    base.discoveryClass,
    eligible,
    ambiguous
  );

  return {
    standard: ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
    version: UNPROMPTED_METHODOLOGY_VERSION,
    historicalMethodologyId: HISTORICAL_MENTION_CONSIDERATION_METHODOLOGY_ID,
    correctedMethodologyId: CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
    discoveryClass: base.discoveryClass,
    eligibilityClass,
    independentlyEligible: eligible,
    ambiguous,
    accommodationNeed,
    geoRelevant,
    contamination,
    exclusions,
    subjectNamed: base.subjectNamed,
    brandNamed: base.brandNamed,
  };
}

export function loadArchivePromptMap(propertyId, periodId) {
  const dir = path.join(
    ROOT,
    "data/ai-demand-positioning/observation-archive",
    propertyId,
    periodId,
    "observations"
  );
  const map = new Map();
  if (!fs.existsSync(dir)) return map;
  for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".json"))) {
    try {
      const o = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
      if (o.scenarioId && o.prompt && !map.has(o.scenarioId)) {
        map.set(o.scenarioId, o.prompt);
      }
    } catch {
      /* skip bad archive row */
    }
  }
  return map;
}

/**
 * Resolve prompt text for a scenario: archive > scenario.query > scenario.prompt.
 */
export function resolveScenarioPrompt(scenario, promptMap) {
  const id = scenario?.scenarioId || scenario?.id;
  return (
    (id && promptMap?.get(id)) ||
    scenario?.query ||
    scenario?.prompt ||
    scenario?.label ||
    ""
  );
}

/**
 * Partition scenarios into independent-eligible vs excluded.
 */
export function partitionScenariosForIndependentDiscovery(scenarios, profile, promptMap) {
  const eligible = [];
  const excluded = [];
  const ambiguous = [];
  const register = [];

  for (const s of scenarios || []) {
    const scenarioId = s.scenarioId || s.id;
    const prompt = resolveScenarioPrompt(s, promptMap);
    const assessment = assessIndependentDiscoveryEligibility(prompt, profile);
    const row = {
      scenarioId,
      prompt: String(prompt).slice(0, 400),
      ...assessment,
    };
    register.push(row);
    if (assessment.independentlyEligible) eligible.push(s);
    else if (assessment.ambiguous) ambiguous.push(s);
    else excluded.push(s);
  }

  return {
    standard: ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
    version: UNPROMPTED_METHODOLOGY_VERSION,
    eligibleScenarios: eligible,
    excludedScenarios: excluded,
    ambiguousScenarios: ambiguous,
    register,
    counts: {
      total: (scenarios || []).length,
      eligible: eligible.length,
      excluded: excluded.length,
      ambiguous: ambiguous.length,
    },
  };
}

export function filterObservationsToScenarios(observations, scenarioIds) {
  const set = scenarioIds instanceof Set ? scenarioIds : new Set(scenarioIds || []);
  return (observations || []).filter((o) => set.has(o.scenarioId));
}

/**
 * Coverage check — remaining D1 inventory should still span key demand segments.
 */
export function assessUnpromptedCoverageAdequacy(eligibleScenarios) {
  const text = (eligibleScenarios || [])
    .map((s) => `${s.query || s.prompt || s.label || ""} ${s.intent || ""}`)
    .join(" \n ")
    .toLowerCase();

  const segments = {
    business: /\b(business|executive|meeting|corporate|offsite)\b/.test(text),
    leisure: /\b(leisure|city break|weekend|romantic|couples|lifestyle)\b/.test(text),
    family: /\b(family|families|kids|children)\b/.test(text),
    luxury: /\b(luxury|five[- ]star|upscale|premium)\b/.test(text),
    events: /\b(event|celebration|anniversary|wedding|entertainment|nightlife)\b/.test(text),
    location: /\b(near|centro|downtown|district|location|airport)\b/.test(text),
    experience: /\b(design|boutique|culture|rooftop|dining|spa)\b/.test(text),
    amenities: /\b(wifi|wi-fi|pool|gym|meeting room|suite|amenit)\b/.test(text),
  };
  const covered = Object.values(segments).filter(Boolean).length;
  const gaps = Object.entries(segments)
    .filter(([, v]) => !v)
    .map(([k]) => k);

  return {
    eligibleCount: (eligibleScenarios || []).length,
    segments,
    coveredSegmentCount: covered,
    gaps,
    adequate: (eligibleScenarios || []).length >= 30 && covered >= 5,
    recommendation:
      gaps.length > 0
        ? `Propose replacement unprompted scenarios for gaps: ${gaps.join(", ")} (no paid execution without authorization)`
        : "D1 inventory covers key demand segments",
  };
}

/**
 * Negative fixtures — must fail independent eligibility.
 */
export function getUnpromptedNegativeFixtures() {
  const umusic = {
    name: "UMusic Hotel Madrid",
    brand: "UMusic Hotels",
    city: "Madrid",
    identityAliases: ["UMusic Hotel Madrid", "UMusic Madrid"],
    website: "https://umusichotels.com",
  };
  const acCoruna = {
    name: "AC Hotel A Coruña",
    brand: "AC Hotels by Marriott",
    city: "A Coruña",
    market: "A Coruña",
    identityAliases: ["AC Hotel A Coruna", "AC Hotel Coruña"],
    website: "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/overview/",
  };
  const jwMty = {
    name: "JW Marriott Hotel Monterrey Valle",
    affiliation: "JW Marriott",
    city: "San Pedro Garza García",
  };
  const westinMty = {
    name: "The Westin Monterrey Valle",
    affiliation: "Westin",
    city: "San Pedro Garza García",
  };
  return [
    {
      id: "SUBJECT_NAME_IN_PROMPT",
      prompt: "Is UMusic Hotel Madrid suitable for a corporate stay?",
      expectEligible: false,
      profile: umusic,
    },
    {
      id: "BRAND_IN_PROMPT",
      prompt: "Recommend a UMusic Hotels property in Madrid for executives",
      expectEligible: false,
      profile: umusic,
    },
    {
      id: "ALIAS_IN_PROMPT",
      prompt: "Where should I stay at UMusic Madrid this weekend?",
      expectEligible: false,
      profile: umusic,
    },
    {
      id: "LEADING_ASSISTED",
      prompt: "Why is UMusic Hotel Madrid the best luxury hotel in Centro?",
      expectEligible: false,
      profile: umusic,
    },
    {
      id: "CLEAN_INDEPENDENT",
      prompt: "Best luxury hotel in Puerta del Sol / Centro, Madrid for a business trip",
      expectEligible: true,
      profile: umusic,
    },
    {
      id: "COMPETITIVE_COMPARISON",
      prompt: "Should I stay at UMusic Hotel Madrid or Four Seasons Madrid?",
      expectEligible: false,
      profile: umusic,
    },
    // Permanent negative regression: destination geography must NOT exclude independent prompts.
    {
      id: "GEO_CITY_MUST_REMAIN_ELIGIBLE_AC_CORUNA",
      prompt:
        "Best business hotel in A Coruña near the marina for a two-night corporate stay",
      expectEligible: true,
      profile: acCoruna,
    },
    {
      id: "SUBJECT_NAME_MUST_EXCLUDE_AC_CORUNA",
      prompt: "Is AC Hotel A Coruña a good choice for a business trip?",
      expectEligible: false,
      profile: acCoruna,
    },
    {
      id: "MONTERREY_VALLE_DESTINATION_MUST_REMAIN_ELIGIBLE_JW",
      prompt: "Recommend an upscale hotel in Monterrey Valle for corporate meetings",
      expectEligible: true,
      profile: jwMty,
    },
    {
      id: "VALLE_ORIENTE_DESTINATION_MUST_REMAIN_ELIGIBLE_WESTIN",
      prompt: "Best hotel near Fashion Drive and Valle Oriente for leisure travelers",
      expectEligible: true,
      profile: westinMty,
    },
    {
      id: "MARRIOTT_BONVOY_BRAND_PROMPT_MUST_EXCLUDE_JW",
      prompt: "Upscale Marriott hotel in San Pedro Garza García with Bonvoy points",
      expectEligible: false,
      profile: jwMty,
    },
    {
      id: "COMPARE_HOTELS_PROMPT_MUST_EXCLUDE",
      prompt: "Compare upscale hotels in San Pedro Garza García for a 3-night leisure trip",
      expectEligible: false,
      profile: jwMty,
    },
    {
      id: "WESTIN_BRAND_PROMPT_MUST_EXCLUDE",
      prompt: "Is The Westin Monterrey Valle good for a weekend break?",
      expectEligible: false,
      profile: westinMty,
    },
    {
      id: "FOREIGN_MARRIOTT_PROMPT_MUST_EXCLUDE_FROM_WESTIN",
      prompt: "Upscale Marriott hotel in San Pedro Garza García with Bonvoy points",
      expectEligible: false,
      profile: westinMty,
    },
  ];
}

export function runUnpromptedNegativeFixtures() {
  const results = getUnpromptedNegativeFixtures().map((fx) => {
    const got = assessIndependentDiscoveryEligibility(fx.prompt, fx.profile);
    const pass = got.independentlyEligible === fx.expectEligible;
    return {
      id: fx.id,
      pass,
      expectEligible: fx.expectEligible,
      gotEligible: got.independentlyEligible,
      discoveryClass: got.discoveryClass,
      exclusions: got.exclusions,
    };
  });
  return {
    standard: ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
    ok: results.every((r) => r.pass),
    results,
  };
}

/**
 * Apply standard to period scenarios for metric calculation.
 * Returns filtered scenario list + metadata stamp.
 */
export function applyUnpromptedStandardToUniverse({
  propertyId,
  periodId,
  scenarios,
  propertyProfile,
  promptMap = null,
}) {
  const map =
    promptMap ||
    (propertyId && periodId ? loadArchivePromptMap(propertyId, periodId) : new Map());
  const partitioned = partitionScenariosForIndependentDiscovery(
    scenarios,
    propertyProfile,
    map
  );
  const coverage = assessUnpromptedCoverageAdequacy(partitioned.eligibleScenarios);
  const eligibleIds = new Set(
    partitioned.eligibleScenarios.map((s) => s.scenarioId || s.id).filter(Boolean)
  );

  return {
    ...partitioned,
    eligibleScenarioIds: eligibleIds,
    coverage,
    promptMapSize: map.size,
    methodologyStamp: {
      standard: ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
      version: UNPROMPTED_METHODOLOGY_VERSION,
      historicalMethodologyId: HISTORICAL_MENTION_CONSIDERATION_METHODOLOGY_ID,
      correctedMethodologyId: CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
      researchScenarioInventory: (scenarios || []).length,
      independentDiscoveryEligible: partitioned.counts.eligible,
      excludedPromptedOrIneligible: partitioned.counts.excluded,
      note: "Appearance KPIs use independent-discovery eligible scenarios only; Reality Gap may retain full subject-mentioned corpus; BPP uses a separate contract.",
    },
  };
}
