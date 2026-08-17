/**
 * Operator Explorer — Section Pattern Parity
 *
 * A section does not pass simply because it has non-empty text.
 * It must match the Arbor Lodging + Hotel Equities product pattern.
 *
 * Permanent Tab Factory gate: section_pattern_parity.
 */
import {
  OPERATOR_QUALITY_BASELINE_OPERATORS,
  getOperatorQualityBaselineEntry,
} from "./operator-explorer-quality-baseline.js";

export const OPERATOR_SECTION_PATTERN_PARITY_VERSION = "operator-section-pattern-parity-v1";

export const OPERATOR_SECTION_PATTERN_STATUSES = Object.freeze([
  "pass",
  "wrong_pattern",
  "too_generic",
  "missing_cala_lens",
  "missing_owner_interpretation",
  "honest_zero_unlabeled",
  "below_benchmark_depth",
  "needs_patch",
]);

export const OPERATOR_SECTION_PATTERN_BENCHMARK_REFS = Object.freeze({
  company_story:
    "Arbor/HE: operator-specific history + CALA lens + owner-useful differentiators (not name-swappable)",
  operating_platform_pillars:
    "Arbor/HE: commercial / reporting / transition pillars with titled items + descriptions",
  brand_relationships:
    "Arbor/HE: named brand families + compliance/relationship narrative; CALA vs enterprise labeled",
  markets_footprint:
    "Arbor/HE: honest CALA zero footprint labeled OR active markets; team experience markets explicit",
  leadership:
    "Arbor/HE: org structure cards + team depth by function + language capability",
  owner_engagement:
    "Arbor/HE: reporting cadence / collaboration model with owner-facing substance",
  deal_fit:
    "Arbor/HE: fit criteria + not-ideal-for + project types (clear owner usefulness)",
  proof_track_record:
    "Arbor/HE: case studies and/or diligence Q&A with substantive bodies",
});

export const OPERATOR_SECTION_PATTERN_IDS = Object.freeze(
  Object.keys(OPERATOR_SECTION_PATTERN_BENCHMARK_REFS)
);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(s) {
  return nz(s)
    .split(/\s+/)
    .filter(Boolean).length;
}

function parseMaybeJson(raw) {
  if (raw == null) return null;
  if (typeof raw === "object") return raw;
  if (typeof raw !== "string") return null;
  const t = raw.trim();
  if (!(t.startsWith("{") || t.startsWith("["))) return null;
  try {
    return JSON.parse(t);
  } catch {
    return null;
  }
}

function getPrefill(prefill, ...keys) {
  for (const k of keys) {
    if (k && prefill && Object.prototype.hasOwnProperty.call(prefill, k) && prefill[k] != null && prefill[k] !== "") {
      return prefill[k];
    }
  }
  return undefined;
}

function jsonItemCount(raw) {
  const parsed = parseMaybeJson(raw) ?? raw;
  if (Array.isArray(parsed)) return parsed.length;
  if (parsed && typeof parsed === "object" && Array.isArray(parsed.items)) return parsed.items.length;
  return 0;
}

function jsonCorpus(raw) {
  const parsed = parseMaybeJson(raw) ?? raw;
  try {
    return typeof parsed === "string" ? parsed : JSON.stringify(parsed || "");
  } catch {
    return nz(raw);
  }
}

function hasOperatorSpecificity(text, operatorName, slug) {
  const corpus = nz(text);
  if (!corpus) return false;
  const tokens = [];
  const name = nz(operatorName);
  if (name) {
    tokens.push(...name.split(/\s+/).filter((t) => t.length >= 4 && !/^\(CALA\)$/i.test(t)));
  }
  if (/arbor/i.test(`${name} ${slug}`)) tokens.push("Arbor");
  if (/equities|hotel equities/i.test(`${name} ${slug}`)) tokens.push("Equities", "Hotel Equities");
  if (!tokens.length) return words(corpus) >= 20;
  return tokens.some((t) => new RegExp(t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(corpus));
}

function hasCalaLens(text) {
  return /\b(cala|latin america|caribbean|mexico|latam|mexico city|canc[uú]n|miami hub)\b/i.test(
    nz(text)
  );
}

function hasOwnerSignal(text) {
  return /\b(owner|reporting|governance|management agreement|third-party|transitions?)\b/i.test(
    nz(text)
  );
}

function hasNamedBrand(text) {
  return /\b(marriott|hilton|hyatt|ihg|choice|wyndham|accor|radisson|independent|soft brand)\b/i.test(
    nz(text)
  );
}

function hasHonestZeroLabel(text) {
  return /\b(0|zero|does not currently manage|no managed|not currently manage)\b/i.test(nz(text)) &&
    /\b(cala|region|managed|hotel|footprint|portfolio)\b/i.test(nz(text));
}

function sectionResult(sectionId, status, detail, extras = {}) {
  return {
    sectionId,
    status,
    pass: status === "pass",
    benchmarkRef: OPERATOR_SECTION_PATTERN_BENCHMARK_REFS[sectionId],
    detail,
    ...extras,
  };
}

export function evaluateCompanyStoryPattern({ prefill = {}, operatorName, operatorSlug } = {}) {
  const history = nz(getPrefill(prefill, "companyHistory"));
  const mission = nz(getPrefill(prefill, "missionStatement"));
  const diffs = nz(getPrefill(prefill, "differentiators"));
  const philosophy = nz(getPrefill(prefill, "managementPhilosophy"));
  const corpus = [history, mission, diffs, philosophy].join("\n");

  if (words(history) < 25) {
    return sectionResult("company_story", "below_benchmark_depth", "companyHistory thinner than Arbor/HE narrative bar");
  }
  if (!hasOperatorSpecificity(corpus, operatorName, operatorSlug)) {
    return sectionResult("company_story", "too_generic", "Story lacks operator-specific identity");
  }
  if (!hasCalaLens(corpus)) {
    return sectionResult("company_story", "missing_cala_lens", "CALA/regional lens missing from company story");
  }
  if (!hasOwnerSignal(corpus) && words(diffs) < 8) {
    return sectionResult(
      "company_story",
      "missing_owner_interpretation",
      "Need owner-useful differentiators or owner signals in story block"
    );
  }
  return sectionResult("company_story", "pass", "Operator-specific story with CALA lens");
}

export function evaluateOperatingPlatformPillarsPattern({ prefill = {} } = {}) {
  const keys = [
    "op_commercial_engine_json",
    "op_owner_reporting_json",
    "op_preopening_transition_json",
    "op_conversion_repositioning_json",
    "op_fb_lifestyle_resort_json",
  ];
  const present = keys.filter((k) => jsonItemCount(getPrefill(prefill, k)) >= 2);
  if (present.length < 3) {
    return sectionResult(
      "operating_platform_pillars",
      "below_benchmark_depth",
      `Need ≥3 platform pillar JSON blocks with ≥2 items each (found ${present.length})`,
      { present }
    );
  }
  const corpus = present.map((k) => jsonCorpus(getPrefill(prefill, k))).join("\n");
  if (!hasOwnerSignal(corpus)) {
    return sectionResult(
      "operating_platform_pillars",
      "missing_owner_interpretation",
      "Platform pillars lack owner/reporting/transition owner signals"
    );
  }
  return sectionResult("operating_platform_pillars", "pass", `Pillars present: ${present.join(", ")}`, {
    present,
  });
}

export function evaluateBrandRelationshipsPattern({ prefill = {}, operatorName, operatorSlug } = {}) {
  const families = nz(getPrefill(prefill, "brandFamiliesOperated"));
  const mix = nz(getPrefill(prefill, "brandedVsIndependentMix"));
  const soft = nz(getPrefill(prefill, "brand_soft_independent_narrative"));
  const narrative =
    nz(getPrefill(prefill, "brand_narrative_relationship")) ||
    nz(getPrefill(prefill, "brand_narrative_compliance"));
  const portfolio = getPrefill(prefill, "brand_portfolio_mix_json", "brand_relationship_depth_json");
  const corpus = [families, mix, soft, narrative, jsonCorpus(portfolio)].join("\n");

  if (!hasNamedBrand(corpus) && jsonItemCount(portfolio) < 2) {
    return sectionResult(
      "brand_relationships",
      "below_benchmark_depth",
      "Need named brand families or portfolio/depth JSON cards"
    );
  }
  if (words(narrative || soft || families) < 12 && jsonItemCount(portfolio) < 2) {
    return sectionResult(
      "brand_relationships",
      "wrong_pattern",
      "Brand section needs narrative depth or structured portfolio cards"
    );
  }
  if (!hasOperatorSpecificity(corpus, operatorName, operatorSlug) && !hasNamedBrand(corpus)) {
    return sectionResult("brand_relationships", "too_generic", "Brand copy is interchangeable");
  }
  return sectionResult("brand_relationships", "pass", "Named brands / relationship depth present");
}

export function evaluateMarketsFootprintPattern({ prefill = {} } = {}) {
  const activeCountries = getPrefill(prefill, "activeCountries", "Active Countries");
  const activeMarkets = getPrefill(prefill, "activeMarkets", "Active Markets / Cities");
  const teamMarkets = getPrefill(
    prefill,
    "teamExperienceMarkets",
    "lead_team_markets_json",
    "mkt_regional_expertise_json"
  );
  const regional = getPrefill(prefill, "markets_regional_portfolio_json", "mkt_regional_expertise_json");
  const priorities = nz(getPrefill(prefill, "priorityMarkets", "targetGrowthMarkets"));
  const corpus = [
    nz(typeof activeCountries === "string" ? activeCountries : JSON.stringify(activeCountries || "")),
    nz(typeof activeMarkets === "string" ? activeMarkets : JSON.stringify(activeMarkets || "")),
    jsonCorpus(teamMarkets),
    jsonCorpus(regional),
    priorities,
    nz(getPrefill(prefill, "specificMarkets")),
  ].join("\n");

  const hasActive =
    (Array.isArray(activeCountries) && activeCountries.length > 0) ||
    (Array.isArray(activeMarkets) && activeMarkets.length > 0) ||
    words(nz(activeCountries)) >= 2 ||
    words(nz(activeMarkets)) >= 2;

  const hasTeamXp = jsonItemCount(teamMarkets) >= 1 || jsonItemCount(regional) >= 2 || words(jsonCorpus(teamMarkets)) >= 20;
  const honestZero = hasHonestZeroLabel(corpus);

  if (!hasActive && !honestZero && !hasTeamXp) {
    return sectionResult(
      "markets_footprint",
      "honest_zero_unlabeled",
      "No active markets and no labeled zero footprint / team experience pattern"
    );
  }
  if (!hasActive && honestZero && !hasTeamXp) {
    return sectionResult(
      "markets_footprint",
      "below_benchmark_depth",
      "Honest zero present but missing team experience / regional expertise cards"
    );
  }
  if (!hasCalaLens(corpus)) {
    return sectionResult("markets_footprint", "missing_cala_lens", "Markets section missing CALA/regional lens");
  }
  return sectionResult(
    "markets_footprint",
    "pass",
    hasActive
      ? "Active markets present with regional lens"
      : "Honest zero + team/regional experience pattern"
  );
}

export function evaluateLeadershipPattern({ prefill = {} } = {}) {
  const org = getPrefill(prefill, "lead_org_structure_json");
  const depth = getPrefill(prefill, "lead_team_depth_json");
  const langs = getPrefill(prefill, "lead_language_capability_json");
  const execs = getPrefill(prefill, "leadership_executives_json", "leadershipTeam", "executives");
  const orgN = jsonItemCount(org);
  const depthN = jsonItemCount(depth);
  const langN = jsonItemCount(langs);
  const execN = jsonItemCount(execs);

  if (orgN < 2 || depthN < 3) {
    return sectionResult(
      "leadership",
      "below_benchmark_depth",
      `Need org≥2 and team-depth≥3 cards (org=${orgN}, depth=${depthN})`
    );
  }
  if (langN < 2 && execN < 2) {
    return sectionResult(
      "leadership",
      "wrong_pattern",
      "Need language capability cards or named executive profiles"
    );
  }
  return sectionResult("leadership", "pass", `org=${orgN} depth=${depthN} langs=${langN} execs=${execN}`);
}

export function evaluateOwnerEngagementPattern({ prefill = {} } = {}) {
  const level = nz(getPrefill(prefill, "ownerReportingLevel"));
  const cadence = nz(getPrefill(prefill, "ownerReportingCadence", "reportingFrequency"));
  const collab = nz(getPrefill(prefill, "operatingCollaborationMode"));
  const responseTime = nz(getPrefill(prefill, "ownerResponseTime"));
  const reportTypes = getPrefill(prefill, "reportTypes", "op_owner_reporting_json", "ov_reporting_rhythm_json");
  const corpus = [level, cadence, collab, responseTime, jsonCorpus(reportTypes)].join("\n");

  if (words(corpus) < 8 && jsonItemCount(reportTypes) < 2) {
    return sectionResult(
      "owner_engagement",
      "below_benchmark_depth",
      "Need reporting level/cadence narrative or reporting rhythm cards"
    );
  }
  if (!hasOwnerSignal(corpus)) {
    return sectionResult(
      "owner_engagement",
      "missing_owner_interpretation",
      "Engagement copy missing owner/reporting signals"
    );
  }
  return sectionResult("owner_engagement", "pass", "Owner reporting / engagement pattern present");
}

export function evaluateDealFitPattern({ prefill = {} } = {}) {
  const criteria = getPrefill(prefill, "bf_fit_criteria_json");
  const notIdeal = nz(getPrefill(prefill, "bf_not_ideal_for", "lessIdealSituations"));
  const projectTypes = getPrefill(prefill, "bf_best_fit_project_types_json");
  const owners = nz(getPrefill(prefill, "bestFitOwnerTypes"));
  const situations = nz(getPrefill(prefill, "bf_operating_situations", "bf_selected_situation_types"));

  const criteriaN = jsonItemCount(criteria);
  const projectsN = jsonItemCount(projectTypes);

  if (criteriaN < 4 && words(notIdeal) < 20 && projectsN < 3) {
    return sectionResult(
      "deal_fit",
      "below_benchmark_depth",
      "Need fit-criteria JSON (≥4), substantive not-ideal-for, or project-type cards"
    );
  }
  if (words(notIdeal) < 12 && criteriaN < 4) {
    return sectionResult(
      "deal_fit",
      "missing_owner_interpretation",
      "Deal fit needs clear not-ideal-for or fit criteria for owners"
    );
  }
  if (!owners && !situations && criteriaN < 4) {
    return sectionResult("deal_fit", "wrong_pattern", "Missing owner types / situations / fit criteria");
  }
  return sectionResult("deal_fit", "pass", `criteria=${criteriaN} projects=${projectsN}`);
}

export function evaluateProofTrackRecordPattern({ prefill = {} } = {}) {
  const cases = getPrefill(prefill, "case_studies_json", "caseStudiesDetail", "caseStudies");
  const diligence = getPrefill(prefill, "owner_diligence_json", "ownerDiligenceQa");
  const recognition = nz(
    getPrefill(prefill, "industryRecognition", "notableAchievements", "certifications")
  );
  const caseN = jsonItemCount(cases);
  const dilN = jsonItemCount(diligence);

  if (caseN < 1 && dilN < 2 && words(recognition) < 8) {
    return sectionResult(
      "proof_track_record",
      "below_benchmark_depth",
      "Need case studies, diligence Q&A, or recognition substance"
    );
  }
  return sectionResult("proof_track_record", "pass", `cases=${caseN} diligence=${dilN}`);
}

/**
 * Full section pattern parity evaluation from prefill payload.
 */
export function evaluateOperatorSectionPatternParity({
  operatorSlug,
  operatorName,
  recordId = null,
  prefill = {},
  source = "fixtures",
} = {}) {
  const entry = getOperatorQualityBaselineEntry(operatorSlug) ||
    getOperatorQualityBaselineEntry(recordId);
  const name = operatorName || entry?.companyName || operatorSlug;
  const slug = operatorSlug || entry?.slug || "";

  const sections = [
    evaluateCompanyStoryPattern({ prefill, operatorName: name, operatorSlug: slug }),
    evaluateOperatingPlatformPillarsPattern({ prefill }),
    evaluateBrandRelationshipsPattern({ prefill, operatorName: name, operatorSlug: slug }),
    evaluateMarketsFootprintPattern({ prefill }),
    evaluateLeadershipPattern({ prefill }),
    evaluateOwnerEngagementPattern({ prefill }),
    evaluateDealFitPattern({ prefill }),
    evaluateProofTrackRecordPattern({ prefill }),
  ];

  const failing = sections.filter((s) => !s.pass);
  const pass = failing.length === 0;

  return {
    version: OPERATOR_SECTION_PATTERN_PARITY_VERSION,
    operatorSlug: slug,
    operatorName: name,
    recordId: recordId || entry?.recordId || null,
    source,
    protectedBaseline: Boolean(entry),
    pass,
    failCount: failing.length,
    passCount: sections.filter((s) => s.pass).length,
    sectionCount: sections.length,
    sections,
    failingSectionIds: failing.map((s) => s.sectionId),
    gates: {
      section_pattern_parity: pass,
      company_story_pattern_pass: sections.find((s) => s.sectionId === "company_story")?.pass === true,
      operating_platform_pillars_pattern_pass:
        sections.find((s) => s.sectionId === "operating_platform_pillars")?.pass === true,
      brand_relationships_pattern_pass:
        sections.find((s) => s.sectionId === "brand_relationships")?.pass === true,
      markets_footprint_pattern_pass:
        sections.find((s) => s.sectionId === "markets_footprint")?.pass === true,
      leadership_pattern_pass: sections.find((s) => s.sectionId === "leadership")?.pass === true,
      owner_engagement_pattern_pass:
        sections.find((s) => s.sectionId === "owner_engagement")?.pass === true,
      deal_fit_pattern_pass: sections.find((s) => s.sectionId === "deal_fit")?.pass === true,
      proof_track_record_pattern_pass:
        sections.find((s) => s.sectionId === "proof_track_record")?.pass === true,
    },
    benchmarkOperators: OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug),
  };
}
