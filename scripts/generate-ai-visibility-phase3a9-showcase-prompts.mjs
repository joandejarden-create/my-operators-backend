#!/usr/bin/env node
/**
 * Phase 3A.9 — generate bilingual showcase prompt seed fixture.
 * Deterministic. No provider calls. No Airtable writes.
 *
 *   node scripts/generate-ai-visibility-phase3a9-showcase-prompts.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ACTIVE_SHOWCASE_INTENTS } from "../lib/ai-visibility/showcase-intents.js";
import { PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "fixtures", "ai-visibility", "phase3a9-showcase-prompt-seed.json");
const AUDIT_OUT = path.join(ROOT, "data", "ai-visibility", "phase3a9-existing-prompt-audit.json");
const PLAN_OUT = path.join(ROOT, "data", "ai-visibility", "phase3a9-execution-plan-dry-run.json");

const PEER = PEER_SET_ID_V2 || "peers_uu_collection_lifestyle_owner_decision_v2";

/** Two distinct owner-decision framings per intent (not paraphrases). */
const FRAMINGS = [
  {
    intentTerritory: "Conversion",
    variant: "existing_asset_reposition",
    decisionDistinction:
      "Repositioning / converting an existing hotel asset that already operates as a hotel.",
    promptFamily: "showcase_conversion_existing_asset_reposition",
    en: {
      Global:
        "Which hotel brands should an owner consider when converting an existing upper-upscale hotel asset globally?",
      CALA:
        "Which hotel brands should an owner consider when converting an existing upper-upscale hotel asset in the Caribbean and Latin America?",
      Europe:
        "Which hotel brands should an owner consider when converting an existing upper-upscale hotel asset in Europe?",
      "North America":
        "Which hotel brands should an owner consider when converting an existing upper-upscale hotel asset in North America?",
      Mexico:
        "Which hotel brands should an owner consider for converting an existing mid-size upper-upscale hotel in Mexico?",
    },
    es: {
      CALA:
        "¿Qué marcas hoteleras debería considerar un propietario al convertir un hotel upper-upscale existente en el Caribe y América Latina?",
      Mexico:
        "¿Qué marcas hoteleras debería considerar un propietario para convertir un hotel upper-upscale de tamaño medio existente en México?",
    },
  },
  {
    intentTerritory: "Conversion",
    variant: "independent_affiliation",
    decisionDistinction:
      "Bringing brand affiliation to an independent (or differently flagged) upper-upscale property.",
    promptFamily: "showcase_conversion_independent_affiliation",
    en: {
      Global:
        "Which hotel brands are commonly considered when an owner wants to affiliate an independent upper-upscale property through a conversion?",
      CALA:
        "Which hotel brands are commonly considered when an owner wants to affiliate an independent upper-upscale property through a conversion in the Caribbean and Latin America?",
      Europe:
        "Which hotel brands are commonly considered when an owner wants to affiliate an independent upper-upscale property through a conversion in Europe?",
      "North America":
        "Which hotel brands are commonly considered when an owner wants to affiliate an independent upper-upscale property through a conversion in North America?",
      Mexico:
        "For an independent upper-upscale hotel in Mexico, which brands should an owner consider for conversion into a branded affiliation?",
    },
    es: {
      CALA:
        "¿Qué marcas hoteleras se consideran habitualmente cuando un propietario quiere afiliar un hotel upper-upscale independiente mediante una conversión en el Caribe y América Latina?",
      Mexico:
        "Para un hotel upper-upscale independiente en México, ¿qué marcas debería considerar un propietario para una conversión hacia una afiliación de marca?",
    },
  },
  {
    intentTerritory: "Collection / Soft Brand",
    variant: "collection_affiliation",
    decisionDistinction:
      "Choosing a collection / soft-brand affiliation path (category decision).",
    promptFamily: "showcase_collection_soft_affiliation",
    en: {
      Global:
        "Which soft brand or collection hotel brands should an owner consider for affiliation while preserving the hotel’s individuality?",
      CALA:
        "Which soft brand or collection hotel brands should an owner consider for affiliation in the Caribbean and Latin America while preserving the hotel’s individuality?",
      Europe:
        "Which soft brand or collection hotel brands should an owner consider for affiliation in Europe while preserving the hotel’s individuality?",
      "North America":
        "Which soft brand or collection hotel brands should an owner consider for affiliation in North America while preserving the hotel’s individuality?",
      Mexico:
        "Which soft brand or collection hotel brands should an owner in Mexico consider when seeking affiliation without a fully standardized hard brand?",
    },
    es: {
      CALA:
        "¿Qué marcas hoteleras soft brand o de colección debería considerar un propietario en el Caribe y América Latina para una afiliación que preserve la individualidad del hotel?",
      Mexico:
        "¿Qué marcas hoteleras soft brand o de colección debería considerar un propietario en México cuando busca afiliación sin una marca hard totalmente estandarizada?",
    },
  },
  {
    intentTerritory: "Collection / Soft Brand",
    variant: "soft_brand_shortlist",
    decisionDistinction:
      "Owner shortlist of collection / soft-brand options for a conversion-ready asset.",
    promptFamily: "showcase_collection_soft_shortlist",
    en: {
      Global:
        "When an owner wants a collection or soft-brand option for a hotel conversion, which brands are commonly considered globally?",
      CALA:
        "When an owner wants a collection or soft-brand option for a hotel conversion in the Caribbean and Latin America, which brands are commonly considered?",
      Europe:
        "When an owner wants a collection or soft-brand option for a hotel conversion in Europe, which brands are commonly considered?",
      "North America":
        "When an owner wants a collection or soft-brand option for a hotel conversion in North America, which brands are commonly considered?",
      Mexico:
        "When an owner wants a collection or soft-brand option for a hotel conversion in Mexico, which brands are commonly considered?",
    },
    es: {
      CALA:
        "Cuando un propietario busca una opción collection o soft brand para la conversión de un hotel en el Caribe y América Latina, ¿qué marcas se consideran habitualmente?",
      Mexico:
        "Cuando un propietario busca una opción collection o soft brand para la conversión de un hotel en México, ¿qué marcas se consideran habitualmente?",
    },
  },
  {
    intentTerritory: "Lifestyle Positioning",
    variant: "lifestyle_strategy",
    decisionDistinction:
      "Selecting brands for an explicit lifestyle positioning strategy.",
    promptFamily: "showcase_lifestyle_positioning_strategy",
    en: {
      Global:
        "Which lifestyle hotel brands should an owner consider when positioning a hotel around differentiated guest experience and local character?",
      CALA:
        "Which lifestyle hotel brands should an owner consider when positioning a hotel in the Caribbean and Latin America around differentiated guest experience and local character?",
      Europe:
        "Which lifestyle hotel brands should an owner consider when positioning a hotel in Europe around differentiated guest experience and local character?",
      "North America":
        "Which lifestyle hotel brands should an owner consider when positioning a hotel in North America around differentiated guest experience and local character?",
      Mexico:
        "Which lifestyle hotel brands should an owner consider for a hotel in Mexico that needs strong lifestyle positioning rather than a conventional full-service flag?",
    },
    es: {
      CALA:
        "¿Qué marcas hoteleras lifestyle debería considerar un propietario al posicionar un hotel en el Caribe y América Latina alrededor de una experiencia diferenciada y carácter local?",
      Mexico:
        "¿Qué marcas hoteleras lifestyle debería considerar un propietario para un hotel en México que requiere un posicionamiento lifestyle claro, más que una bandera full-service convencional?",
    },
  },
  {
    intentTerritory: "Lifestyle Positioning",
    variant: "design_local_character",
    decisionDistinction:
      "Brands associated with design-led / distinctive character positioning (owner positioning, not traveler marketing).",
    promptFamily: "showcase_lifestyle_design_character",
    en: {
      Global:
        "Which hotel brands are commonly considered by owners for design-forward or distinctive-character lifestyle positioning globally?",
      CALA:
        "Which hotel brands are commonly considered by owners for design-forward or distinctive-character lifestyle positioning in the Caribbean and Latin America?",
      Europe:
        "Which hotel brands are commonly considered by owners for design-forward or distinctive-character lifestyle positioning in Europe?",
      "North America":
        "Which hotel brands are commonly considered by owners for design-forward or distinctive-character lifestyle positioning in North America?",
      Mexico:
        "Which hotel brands are commonly considered by owners in Mexico for design-forward or distinctive-character lifestyle positioning?",
    },
    es: {
      CALA:
        "¿Qué marcas hoteleras consideran habitualmente los propietarios para un posicionamiento lifestyle con diseño o carácter distintivo en el Caribe y América Latina?",
      Mexico:
        "¿Qué marcas hoteleras consideran habitualmente los propietarios en México para un posicionamiento lifestyle con diseño o carácter distintivo?",
    },
  },
  {
    intentTerritory: "Upper-Upscale Positioning",
    variant: "uu_positioning_strategy",
    decisionDistinction:
      "Upper-upscale chain-scale / positioning strategy for the asset.",
    promptFamily: "showcase_upper_upscale_positioning",
    chainScale: "Upper Upscale",
    en: {
      Global:
        "Which upper-upscale hotel brands should an owner consider for an upper-upscale hotel positioning strategy globally?",
      CALA:
        "Which upper-upscale hotel brands should an owner consider for an upper-upscale hotel positioning strategy in the Caribbean and Latin America?",
      Europe:
        "Which upper-upscale hotel brands should an owner consider for an upper-upscale hotel positioning strategy in Europe?",
      "North America":
        "Which upper-upscale hotel brands should an owner consider for an upper-upscale hotel positioning strategy in North America?",
      Mexico:
        "Which upper-upscale hotel brands should an owner consider when positioning a hotel as upper-upscale in Mexico?",
    },
    es: {
      CALA:
        "¿Qué marcas hoteleras upper-upscale debería considerar un propietario para una estrategia de posicionamiento upper-upscale en el Caribe y América Latina?",
      Mexico:
        "¿Qué marcas hoteleras upper-upscale debería considerar un propietario al posicionar un hotel como upper-upscale en México?",
    },
  },
  {
    intentTerritory: "Upper-Upscale Positioning",
    variant: "uu_owner_shortlist",
    decisionDistinction:
      "Owner shortlist of brand options appropriate to upper-upscale (not luxury / not upscale).",
    promptFamily: "showcase_upper_upscale_shortlist",
    chainScale: "Upper Upscale",
    en: {
      Global:
        "For an upper-upscale hotel project, which brands are commonly shortlisted by owners and developers globally?",
      CALA:
        "For an upper-upscale hotel project in the Caribbean and Latin America, which brands are commonly shortlisted by owners and developers?",
      Europe:
        "For an upper-upscale hotel project in Europe, which brands are commonly shortlisted by owners and developers?",
      "North America":
        "For an upper-upscale hotel project in North America, which brands are commonly shortlisted by owners and developers?",
      Mexico:
        "For an upper-upscale hotel project in Mexico, which brands are commonly shortlisted by owners and developers?",
    },
    es: {
      CALA:
        "Para un proyecto hotelero upper-upscale en el Caribe y América Latina, ¿qué marcas suelen entrar en la shortlist de propietarios y desarrolladores?",
      Mexico:
        "Para un proyecto hotelero upper-upscale en México, ¿qué marcas suelen entrar en la shortlist de propietarios y desarrolladores?",
    },
  },
  {
    intentTerritory: "Branded Residences",
    variant: "residences_capability",
    decisionDistinction:
      "Brands with branded-residences capability / association (not Mixed Use).",
    promptFamily: "showcase_branded_residences_capability",
    brandedResidencesRelevance: true,
    en: {
      Global:
        "Which hotel brands should an owner consider when evaluating branded residences as part of a hotel project globally?",
      CALA:
        "Which hotel brands should an owner consider when evaluating branded residences as part of a hotel project in the Caribbean and Latin America?",
      Europe:
        "Which hotel brands should an owner consider when evaluating branded residences as part of a hotel project in Europe?",
      "North America":
        "Which hotel brands should an owner consider when evaluating branded residences as part of a hotel project in North America?",
      Mexico:
        "Which hotel brands should an owner consider when evaluating branded residences for a hotel project in Mexico?",
    },
    es: {
      CALA:
        "¿Qué marcas hoteleras debería considerar un propietario al evaluar branded residences como parte de un proyecto hotelero en el Caribe y América Latina?",
      Mexico:
        "¿Qué marcas hoteleras debería considerar un propietario al evaluar branded residences para un proyecto hotelero en México?",
    },
  },
  {
    intentTerritory: "Branded Residences",
    variant: "residences_hotel_project",
    decisionDistinction:
      "Hotel + branded residences project framing (residences-led; excludes Mixed Use retail/office).",
    promptFamily: "showcase_branded_residences_hotel_project",
    brandedResidencesRelevance: true,
    en: {
      Global:
        "Which hotel brands are commonly considered for a hotel with branded residences component globally?",
      CALA:
        "Which hotel brands are commonly considered for a hotel with branded residences component in the Caribbean and Latin America?",
      Europe:
        "Which hotel brands are commonly considered for a hotel with branded residences component in Europe?",
      "North America":
        "Which hotel brands are commonly considered for a hotel with branded residences component in North America?",
      Mexico:
        "Which hotel brands are commonly considered for a hotel with branded residences component in Mexico?",
    },
    es: {
      CALA:
        "¿Qué marcas hoteleras se consideran habitualmente para un hotel con componente de branded residences en el Caribe y América Latina?",
      Mexico:
        "¿Qué marcas hoteleras se consideran habitualmente para un hotel con componente de branded residences en México?",
    },
  },
  {
    intentTerritory: "Soft-Brand Affiliation Flexibility",
    variant: "affiliation_flexibility",
    decisionDistinction:
      "Prioritizing affiliation flexibility under a soft/collection model (not fee economics).",
    promptFamily: "showcase_soft_brand_affiliation_flexibility",
    en: {
      Global:
        "Which soft brand or collection brands should an owner consider when affiliation flexibility is a priority for a hotel conversion?",
      CALA:
        "Which soft brand or collection brands should an owner consider when affiliation flexibility is a priority for a hotel conversion in the Caribbean and Latin America?",
      Europe:
        "Which soft brand or collection brands should an owner consider when affiliation flexibility is a priority for a hotel conversion in Europe?",
      "North America":
        "Which soft brand or collection brands should an owner consider when affiliation flexibility is a priority for a hotel conversion in North America?",
      Mexico:
        "Which soft brand or collection brands should an owner in Mexico consider when affiliation flexibility is a priority for a hotel conversion?",
    },
    es: {
      CALA:
        "¿Qué marcas soft brand o de colección debería considerar un propietario cuando la flexibilidad de afiliación es prioritaria en una conversión hotelera en el Caribe y América Latina?",
      Mexico:
        "¿Qué marcas soft brand o de colección debería considerar un propietario en México cuando la flexibilidad de afiliación es prioritaria en una conversión hotelera?",
    },
  },
  {
    intentTerritory: "Soft-Brand Affiliation Flexibility",
    variant: "individuality_preservation",
    decisionDistinction:
      "Preserving hotel individuality under brand affiliation (affiliation approach, not underwriting).",
    promptFamily: "showcase_soft_brand_individuality",
    en: {
      Global:
        "Which hotel brands are commonly considered when an owner wants brand affiliation but needs to preserve greater hotel individuality?",
      CALA:
        "Which hotel brands are commonly considered when an owner in the Caribbean and Latin America wants brand affiliation but needs to preserve greater hotel individuality?",
      Europe:
        "Which hotel brands are commonly considered when an owner in Europe wants brand affiliation but needs to preserve greater hotel individuality?",
      "North America":
        "Which hotel brands are commonly considered when an owner in North America wants brand affiliation but needs to preserve greater hotel individuality?",
      Mexico:
        "Which hotel brands are commonly considered when an owner in Mexico wants brand affiliation but needs to preserve greater hotel individuality?",
    },
    es: {
      CALA:
        "¿Qué marcas hoteleras se consideran habitualmente cuando un propietario en el Caribe y América Latina quiere afiliación de marca pero necesita preservar mayor individualidad del hotel?",
      Mexico:
        "¿Qué marcas hoteleras se consideran habitualmente cuando un propietario en México quiere afiliación de marca pero necesita preservar mayor individualidad del hotel?",
    },
  },
];

const GEO_SLOTS = [
  { key: "Global", geographyScope: "Global", commercialRegion: null, country: null, language: "en" },
  { key: "CALA", geographyScope: "Region", commercialRegion: "CALA", country: null, language: "en" },
  { key: "CALA", geographyScope: "Region", commercialRegion: "CALA", country: null, language: "es" },
  { key: "Europe", geographyScope: "Region", commercialRegion: "Europe", country: null, language: "en" },
  {
    key: "North America",
    geographyScope: "Region",
    commercialRegion: "North America",
    country: null,
    language: "en",
  },
  {
    key: "Mexico",
    geographyScope: "Country",
    commercialRegion: "CALA",
    country: "Mexico",
    countryCode: "MX",
    language: "en",
  },
  {
    key: "Mexico",
    geographyScope: "Country",
    commercialRegion: "CALA",
    country: "Mexico",
    countryCode: "MX",
    language: "es",
  },
];

function geoSlug(slot) {
  if (slot.key === "Global") return "global";
  if (slot.key === "CALA") return "cala";
  if (slot.key === "Europe") return "europe";
  if (slot.key === "North America") return "na";
  if (slot.key === "Mexico") return "mx";
  return String(slot.key).toLowerCase().replace(/\s+/g, "_");
}

function buildPrompts() {
  const prompts = [];
  const pairs = [];
  for (const framing of FRAMINGS) {
    if (!ACTIVE_SHOWCASE_INTENTS.includes(framing.intentTerritory)) {
      throw new Error(`Framing intent not in active showcase: ${framing.intentTerritory}`);
    }
    for (const slot of GEO_SLOTS) {
      const textMap = slot.language === "es" ? framing.es : framing.en;
      const promptText = textMap?.[slot.key];
      if (!promptText) {
        if (slot.language === "es") continue; // Wave-1: ES only CALA/Mexico
        throw new Error(`Missing EN text for ${framing.variant} / ${slot.key}`);
      }

      const slug = geoSlug(slot);
      const langSuffix = slot.language === "es" ? "_es" : "";
      const promptId = `p_${slug}_${framing.variant}${langSuffix}_v1`;
      const semanticPairId =
        slot.key === "CALA" || slot.key === "Mexico"
          ? `${framing.variant}_${slug}_owner_decision_v1`
          : null;

      const promptName = [
        slot.key,
        framing.intentTerritory,
        framing.variant.replace(/_/g, " "),
        slot.language === "es" ? "ES" : "EN",
      ].join(" — ");

      prompts.push({
        promptId,
        promptName,
        promptFamily: framing.promptFamily,
        version: "1",
        language: slot.language,
        semanticPairId,
        intentTerritory: framing.intentTerritory,
        stakeholderRelevance: ["Brand", "Owner"],
        entityScope: "Brand",
        geographyScope: slot.geographyScope,
        commercialRegion: slot.commercialRegion,
        country: slot.country,
        countryCode: slot.countryCode || undefined,
        chainScale: framing.chainScale || undefined,
        developmentType: framing.intentTerritory === "Conversion" ? "Conversion" : undefined,
        brandedResidencesRelevance: Boolean(framing.brandedResidencesRelevance),
        peerSetId: PEER,
        active: true,
        monitoringEligible: true,
        cadence: "Monthly",
        governanceStatus: "Approved",
        reviewStatus: "Reviewed",
        promptText,
        decisionDistinction: framing.decisionDistinction,
        eligibilityUsedForPromptText: false,
        eligibilityUsedForDownstreamAnalysis: true,
        promptEntityMode: "OPEN_ENDED",
        showcaseWave: "phase3a9_v1",
        sourceRationale: `Phase 3A.9 showcase Wave-1 · ${framing.decisionDistinction}`,
      });

      if (semanticPairId && slot.language === "en") {
        pairs.push({
          semanticPairId,
          intentTerritory: framing.intentTerritory,
          geographyKey: slot.key,
          enPromptId: promptId,
          esPromptId: `p_${slug}_${framing.variant}_es_v1`,
        });
      }
    }
  }
  return { prompts, pairs };
}

function auditExistingEnglish() {
  const seed = JSON.parse(
    fs.readFileSync(path.join(ROOT, "fixtures/ai-visibility/phase2d-prompt-seed.json"), "utf8")
  );
  const showcaseFamilies = new Set(FRAMINGS.map((f) => f.promptFamily));
  const rows = (seed.prompts || []).map((p) => {
    const isBrand =
      p.entityScope === "Brand" ||
      (Array.isArray(p.stakeholderRelevance) && p.stakeholderRelevance.includes("Brand"));
    let disposition = "NOT_USED_IN_SHOWCASE";
    let targetIntent = null;
    let reason = "Phase 2D library retained for historical provenance; not Wave-1 showcase.";

    if (p.intentTerritory === "Mixed Use" || /mixed-use/i.test(p.promptText || "")) {
      disposition = "NOT_USED_IN_SHOWCASE";
      reason = "Mixed Use deferred from showcase wave.";
    } else if (/new-build|select-service/i.test(p.promptFamily || "") || p.intentTerritory === "New Build") {
      disposition = "NOT_USED_IN_SHOWCASE";
      reason = "New Build deferred from first showcase wave.";
    } else if (p.entityScope === "Operator" || p.intentTerritory === "Operator Selection") {
      disposition = "NOT_USED_IN_SHOWCASE";
      reason = "Operator monitoring outside Brand showcase wave.";
    } else if (p.intentTerritory === "Conversion" || /conversion/i.test(p.promptFamily || "")) {
      targetIntent = "Conversion";
      disposition = "NOT_USED_IN_SHOWCASE";
      reason = "Superseded by Phase 3A.9 Conversion families with peer v2 + language metadata.";
    } else if (p.intentTerritory === "Branded Residences") {
      targetIntent = "Branded Residences";
      disposition = "NOT_USED_IN_SHOWCASE";
      reason = "Superseded by Phase 3A.9 Branded Residences families (peer v2).";
    } else if (p.intentTerritory === "Owner Flexibility" || /soft_brand/i.test(p.promptFamily || "")) {
      targetIntent = "Soft-Brand Affiliation Flexibility";
      disposition = "NOT_USED_IN_SHOWCASE";
      reason = "Superseded by Soft-Brand Affiliation Flexibility + Collection / Soft Brand families.";
    } else if (p.intentTerritory === "Owner Economics") {
      disposition = "NOT_USED_IN_SHOWCASE";
      reason = "Broad Owner Economics not active in showcase; replaced by Soft-Brand Affiliation Flexibility.";
    } else if (/lifestyle/i.test(p.promptFamily || "")) {
      targetIntent = "Lifestyle Positioning";
      disposition = "NOT_USED_IN_SHOWCASE";
      reason = "Superseded by Phase 3A.9 Lifestyle Positioning families.";
    } else if (p.intentTerritory === "Chain Scale / Positioning") {
      targetIntent = "Upper-Upscale Positioning";
      disposition = "NOT_USED_IN_SHOWCASE";
      reason = "Superseded by Upper-Upscale Positioning families.";
    } else if (!isBrand) {
      disposition = "NOT_USED_IN_SHOWCASE";
    }

    return {
      PROMPT_ID: p.promptId,
      VERSION: p.version,
      CURRENT_TEXT: p.promptText,
      CURRENT_INTENT: p.intentTerritory,
      TARGET_INTENT: targetIntent,
      GEOGRAPHY: [p.geographyScope, p.commercialRegion, p.country].filter(Boolean).join("/"),
      LANGUAGE: "en",
      KEEP: false,
      EDIT_VERSION: false,
      NOT_USED_IN_SHOWCASE: disposition === "NOT_USED_IN_SHOWCASE",
      REASON: reason,
      showcaseFamilyOverlap: showcaseFamilies.has(p.promptFamily),
    };
  });

  return {
    generatedAt: new Date().toISOString(),
    KEEP: rows.filter((r) => r.KEEP).length,
    EDIT_VERSION: rows.filter((r) => r.EDIT_VERSION).length,
    NOT_USED_IN_SHOWCASE: rows.filter((r) => r.NOT_USED_IN_SHOWCASE).length,
    rows,
  };
}

function buildExecutionPlan(prompts) {
  const elig = JSON.parse(
    fs.readFileSync(path.join(ROOT, "fixtures/ai-visibility/brand-decision-eligibility-v1.json"), "utf8")
  );
  function counts(territory) {
    const rows = elig.entries.filter((e) => e.decisionTerritory === territory);
    return {
      eligibleBrandCount: rows.filter((r) => r.eligibility === "ELIGIBLE").length,
      notEligibleBrandCount: rows.filter((r) => r.eligibility === "NOT_ELIGIBLE").length,
      unknownBrandCount: rows.filter((r) => r.eligibility === "UNKNOWN").length,
    };
  }

  const executions = prompts.map((p) => ({
    provider: "openai",
    geography:
      p.geographyScope === "Global"
        ? "GLOBAL"
        : p.country
          ? String(p.country).toUpperCase()
          : String(p.commercialRegion || "").toUpperCase().replace(/\s+/g, "_"),
    language: p.language,
    intent: p.intentTerritory,
    promptId: p.promptId,
    version: p.version,
    semanticPairId: p.semanticPairId,
    peerSet: PEER,
    ...counts(p.intentTerritory),
  }));

  const byBucket = {
    GLOBAL_EN: executions.filter((e) => e.geography === "GLOBAL" && e.language === "en").length,
    CALA_EN: executions.filter((e) => e.geography === "CALA" && e.language === "en").length,
    CALA_ES: executions.filter((e) => e.geography === "CALA" && e.language === "es").length,
    EUROPE_EN: executions.filter((e) => e.geography === "EUROPE" && e.language === "en").length,
    NORTH_AMERICA_EN: executions.filter(
      (e) => e.geography === "NORTH_AMERICA" && e.language === "en"
    ).length,
    MEXICO_EN: executions.filter((e) => e.geography === "MEXICO" && e.language === "en").length,
    MEXICO_ES: executions.filter((e) => e.geography === "MEXICO" && e.language === "es").length,
  };

  const historicalMean = 0.677;
  const historicalLow = 0.35;
  const historicalHigh = 1.33;
  const total = executions.length;

  return {
    generatedAt: new Date().toISOString(),
    PROMPTS_PER_INTENT: 2,
    TOTAL_GOVERNED_PROMPT_FAMILIES: FRAMINGS.length,
    TOTAL_PROMPT_ROWS: prompts.length,
    ...byBucket,
    TOTAL_CALLS: total,
    COST_PER_CALL_HISTORICAL: { LOW: historicalLow, EXPECTED: historicalMean, HIGH: historicalHigh },
    WAVE_COST: {
      LOW: Number((total * historicalLow).toFixed(2)),
      EXPECTED: Number((total * historicalMean).toFixed(2)),
      HIGH: Number((total * historicalHigh).toFixed(2)),
    },
    BIWEEKLY: {
      CALLS_PER_PERIOD: total,
      CALLS_PER_MONTH: total * 2,
      EXPECTED_MONTHLY_OPENAI_COST_LOW: Number((total * 2 * historicalLow).toFixed(2)),
      EXPECTED_MONTHLY_OPENAI_COST: Number((total * 2 * historicalMean).toFixed(2)),
      EXPECTED_MONTHLY_OPENAI_COST_HIGH: Number((total * 2 * historicalHigh).toFixed(2)),
    },
    executions,
  };
}

function main() {
  const { prompts, pairs } = buildPrompts();
  const seed = {
    seedId: "ai_visibility_phase3a9_showcase_prompt_seed_v1",
    governanceVersion: "phase3a9_v1",
    peerSetId: PEER,
    activeShowcaseIntents: [...ACTIVE_SHOWCASE_INTENTS],
    promptsPerIntentPerGeoLanguage: 2,
    promptEntityMode: "OPEN_ENDED",
    eligibilityUsedForPromptText: false,
    eligibilityUsedForDownstreamAnalysis: true,
    notes: [
      "Wave-1 bilingual Brand showcase prompts for Marriott / Hilton / Choice.",
      "OPEN_ENDED owner questions; peer v2 entity resolution downstream; Eligibility not injected into prompt text.",
      "ES only for CALA and Mexico. No Mixed Use / New Build / broad Owner Economics.",
      "Historical Phase 2D prompts preserved; not overwritten.",
    ],
    semanticPairs: pairs,
    framings: FRAMINGS.map((f) => ({
      intentTerritory: f.intentTerritory,
      variant: f.variant,
      promptFamily: f.promptFamily,
      decisionDistinction: f.decisionDistinction,
    })),
    prompts,
  };

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(seed, null, 2) + "\n");

  const audit = auditExistingEnglish();
  fs.mkdirSync(path.dirname(AUDIT_OUT), { recursive: true });
  fs.writeFileSync(AUDIT_OUT, JSON.stringify(audit, null, 2) + "\n");

  const plan = buildExecutionPlan(prompts);
  fs.writeFileSync(PLAN_OUT, JSON.stringify(plan, null, 2) + "\n");

  console.log(
    JSON.stringify(
      {
        prompts: prompts.length,
        en: prompts.filter((p) => p.language === "en").length,
        es: prompts.filter((p) => p.language === "es").length,
        pairs: pairs.length,
        TOTAL_CALLS: plan.TOTAL_CALLS,
        WAVE_COST_EXPECTED: plan.WAVE_COST.EXPECTED,
        auditNotUsed: audit.NOT_USED_IN_SHOWCASE,
      },
      null,
      2
    )
  );
}

main();
