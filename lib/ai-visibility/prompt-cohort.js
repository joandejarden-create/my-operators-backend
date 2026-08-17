/**
 * Deterministic prompt cohort builder.
 *
 * Headline regional metrics: geographyScope=Region only (no country rollup).
 * Phase 3A.6: language is first-class in cohort filter + fingerprint.
 */

import { createHash } from "crypto";
import { HEADLINE_REGION_METRIC_COHORT_RULE } from "./commercial-geography.js";
import {
  requireSupportedLanguage,
  resolveRecordLanguage,
} from "./language-dimension.js";

export const PROMPT_COHORT_VERSION = "ai_visibility_prompt_cohort_v2";

function normScope(s) {
  if (!s) return null;
  const x = String(s).trim().toLowerCase();
  if (x === "global") return "Global";
  if (x === "region") return "Region";
  if (x === "subregion") return "Subregion";
  if (x === "country") return "Country";
  if (x === "market") return "Market";
  return s;
}

/**
 * @param {object} args
 * @param {object[]} args.prompts already-loaded prompt rows
 * @param {string} [args.language] language-aware cohorts (en|es)
 */
export function buildPromptCohort(args = {}) {
  const {
    prompts = [],
    geographyScope = null,
    region = null,
    commercialRegion = null,
    country = null,
    stakeholder = null,
    entityScope = null,
    intentTerritories = null,
    monitoringEligible = true,
    activeOnly = true,
    includeCountryRollup = false,
    language: languageArg = null,
    requireLanguage = false,
  } = args;

  const scope = normScope(geographyScope);
  const regionKey = region || commercialRegion || null;
  const intents = intentTerritories
    ? new Set(
        (Array.isArray(intentTerritories) ? intentTerritories : [intentTerritories]).map((t) =>
          String(t).toLowerCase()
        )
      )
    : null;

  let language = null;
  if (languageArg != null && String(languageArg).trim() !== "") {
    const req = requireSupportedLanguage(languageArg);
    if (!req.ok) {
      return {
        cohortVersion: PROMPT_COHORT_VERSION,
        ok: false,
        error: req.reasonCode,
        message: req.message,
        count: 0,
        promptIds: [],
        members: [],
        fingerprint: null,
      };
    }
    language = req.language;
  } else if (requireLanguage) {
    return {
      cohortVersion: PROMPT_COHORT_VERSION,
      ok: false,
      error: "LANGUAGE_REQUIRED",
      message: "language (en|es) is required for language-aware cohorts.",
      count: 0,
      promptIds: [],
      members: [],
      fingerprint: null,
    };
  }

  const enforceHeadlinePurity =
    scope === "Region" && !includeCountryRollup && HEADLINE_REGION_METRIC_COHORT_RULE;

  const selected = [];
  for (const p of prompts) {
    if (activeOnly && !p.active) continue;
    if (monitoringEligible === true && !p.monitoringEligible) continue;
    if (monitoringEligible === false && p.monitoringEligible) continue;

    if (language) {
      const pLang = resolveRecordLanguage(p, { treatMissingAsEn: true });
      if (pLang !== language) continue;
    }

    if (scope && String(p.geographyScope) !== scope) {
      if (
        includeCountryRollup &&
        scope === "Region" &&
        p.geographyScope === "Country" &&
        regionKey &&
        String(p.commercialRegion || "").toLowerCase() === String(regionKey).toLowerCase()
      ) {
        // allow
      } else {
        continue;
      }
    }

    if (enforceHeadlinePurity && p.geographyScope !== "Region") continue;

    if (regionKey) {
      if (String(p.commercialRegion || "").toLowerCase() !== String(regionKey).toLowerCase()) {
        if (scope === "Global") {
          // ok
        } else {
          continue;
        }
      }
    }

    if (country) {
      if (String(p.country || "").toLowerCase() !== String(country).toLowerCase()) continue;
    }

    if (entityScope) {
      if (String(p.entityScope).toLowerCase() !== String(entityScope).toLowerCase()) continue;
    }

    if (stakeholder) {
      const list = (p.stakeholderRelevance || []).map((s) => String(s).toLowerCase());
      if (!list.includes(String(stakeholder).toLowerCase())) continue;
    }

    if (intents && !intents.has(String(p.intentTerritory || "").toLowerCase())) continue;

    selected.push(p);
  }

  selected.sort((a, b) => String(a.promptId).localeCompare(String(b.promptId)));

  const members = selected.map((p) => ({
    promptId: p.promptId,
    version: p.version,
    promptFamily: p.promptFamily,
    geographyScope: p.geographyScope,
    commercialRegion: p.commercialRegion || null,
    country: p.country || null,
    intentTerritory: p.intentTerritory,
    entityScope: p.entityScope,
    language: resolveRecordLanguage(p, { treatMissingAsEn: true }),
    semanticPairId: p.semanticPairId || null,
  }));

  const fingerprintSource = JSON.stringify({
    v: PROMPT_COHORT_VERSION,
    scope,
    regionKey,
    country,
    language: language || null,
    entityScope,
    stakeholder,
    intents: intents ? [...intents].sort() : null,
    monitoringEligible,
    includeCountryRollup: Boolean(includeCountryRollup),
    members: members.map((m) => ({
      promptId: m.promptId,
      version: m.version,
      language: m.language,
      semanticPairId: m.semanticPairId,
    })),
  });
  const fingerprint = createHash("sha256").update(fingerprintSource).digest("hex").slice(0, 16);

  return {
    ok: true,
    cohortVersion: PROMPT_COHORT_VERSION,
    headlineRegionRule: HEADLINE_REGION_METRIC_COHORT_RULE,
    filter: {
      geographyScope: scope,
      commercialRegion: regionKey,
      country,
      language: language || null,
      stakeholder,
      entityScope,
      intentTerritories: intents ? [...intents] : null,
      monitoringEligible,
      includeCountryRollup: Boolean(includeCountryRollup),
    },
    language: language || null,
    count: members.length,
    promptIds: members.map((m) => m.promptId),
    members,
    fingerprint,
  };
}
