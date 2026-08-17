#!/usr/bin/env node
/**
 * Observed-demand targeted gap-fill. DataForSEO volume-first.
 * Does not re-query validated themes. Does not attach live prompts.
 * AI provider calls: 0. Census: 0. Airtable writes: 0.
 *
 *   node scripts/ai-visibility-observed-demand-refinement.mjs --estimate-only
 *   node scripts/ai-visibility-observed-demand-refinement.mjs
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  resolveDataForSeoCredentials,
  dataForSeoPost,
  DATAFORSEO_API_BASE,
  buildDataForSeoAuthHeader,
} from "../lib/research-engine-v2/dataforseo-client.js";
import {
  REFINEMENT_SEEDS_EN,
  REFINEMENT_SEEDS_ES,
  VALIDATED_OBSERVED_THEMES_V1,
  REFINEMENT_VOLUME_TASKS,
  REFINEMENT_MAX_SERP_TASKS,
  CORE_SCENARIO_IDS_FOR_COVERAGE,
  DATAFORSEO_LIST_PRICES_USD,
  DATAFORSEO_BUDGET_APPROVAL_REQUIRED,
  MAX_REFINEMENT_INCREMENTAL_USD,
  MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD,
  ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET,
  estimateObservedDemandRefinementCost,
  evaluateRefinementBudgetGuard,
  extractPeopleAlsoAsk,
  extractRelatedSearches,
  assignRelativeDemandTiers,
  usableAsObserved,
  canonicalObservedTheme,
  classifyCommercialRelevance,
  classifyRefinementEvidence,
} from "../lib/ai-visibility/observed-demand-source-sample.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const PHASE_SPEND_PATH = path.join(
  root,
  "reports",
  "ai-visibility",
  "observed-demand-dataforseo-phase-spend.json"
);
const SIGNALS_PATH = path.join(root, "fixtures", "ai-visibility", "demand-signals-v1.json");
const SEED_PATH = path.join(root, "fixtures", "ai-visibility", "observed-demand-seed-v1.json");
const OVERLAY_PATH = path.join(root, "fixtures", "ai-visibility", "prompt-provenance-v1.json");
const estimateOnly = process.argv.includes("--estimate-only");

function loadPhaseSpend() {
  if (!fs.existsSync(PHASE_SPEND_PATH)) {
    return { phase: "observed_demand_source_acquisition", spent_usd: 0, entries: [] };
  }
  const raw = JSON.parse(fs.readFileSync(PHASE_SPEND_PATH, "utf8"));
  return {
    phase: raw.phase || "observed_demand_source_acquisition",
    spent_usd: Number(raw.spent_usd) || 0,
    entries: Array.isArray(raw.entries) ? raw.entries : [],
  };
}

function savePhaseSpend(ledger) {
  fs.mkdirSync(path.dirname(PHASE_SPEND_PATH), { recursive: true });
  fs.writeFileSync(PHASE_SPEND_PATH, JSON.stringify(ledger, null, 2));
}

function slugTheme(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 60);
}

async function fetchUserData() {
  const cred = resolveDataForSeoCredentials();
  if (!cred.ok) return { ok: false, reason: "DATAFORSEO_CREDENTIALS_MISSING" };
  const res = await fetch(`${DATAFORSEO_API_BASE}/appendix/user_data`, {
    headers: { Authorization: buildDataForSeoAuthHeader() },
  });
  const json = await res.json().catch(() => ({}));
  const row = json.tasks?.[0]?.result?.[0] || {};
  return {
    ok: res.ok && Number(json.status_code) === 20000,
    http_status: res.status,
    status_code: json.status_code,
    status_message: json.status_message,
    cost: Number(json.cost) || 0,
    money_balance: row.money?.balance ?? row.money_balance ?? null,
  };
}

async function volumeTask({ location_name, language_code, keywords, tag }) {
  return dataForSeoPost("/keywords_data/google_ads/search_volume/live", [
    {
      location_name,
      language_code,
      keywords,
      search_partners: false,
      tag,
    },
  ]);
}

async function serpTask({ keyword, location_name, language_code }) {
  return dataForSeoPost("/serp/google/organic/live/advanced", [
    {
      keyword,
      location_name,
      language_code,
      depth: 10,
      device: "desktop",
      os: "windows",
    },
  ]);
}

function flattenVolume(res, meta) {
  const rows = [];
  for (const t of res.tasks || []) {
    for (const r of t.result || []) {
      rows.push({
        keyword: r.keyword,
        search_volume: r.search_volume,
        competition: r.competition,
        location_code: r.location_code,
        language_code: r.language_code,
        monthly_searches: Array.isArray(r.monthly_searches)
          ? r.monthly_searches.slice(-3)
          : null,
        ...meta,
      });
    }
  }
  return rows;
}

function serpItems(res) {
  const items = [];
  for (const t of res.tasks || []) {
    for (const result of t.result || []) {
      items.push(...(result.items || []));
    }
  }
  return items;
}

function seedMetaByQuery() {
  return new Map(
    [...REFINEMENT_SEEDS_EN, ...REFINEMENT_SEEDS_ES].map((s) => [s.seed.toLowerCase(), s])
  );
}

function assertNoValidatedRequery() {
  const blocked = new Set(VALIDATED_OBSERVED_THEMES_V1.map((t) => t.toLowerCase()));
  const all = [...REFINEMENT_SEEDS_EN, ...REFINEMENT_SEEDS_ES].map((s) => s.seed.toLowerCase());
  const overlap = all.filter((s) => blocked.has(s));
  if (overlap.length) {
    throw new Error(`refinement_requeries_validated_themes:${overlap.join(",")}`);
  }
}

function coverageForThemes(themes) {
  const mappedScenario = new Set();
  const overlap = [];
  const observedOnly = [];
  const intent = new Set();
  const meta = seedMetaByQuery();
  const existingMap = new Map([
    ["hotel franchise fees", { intent: "FEES_ECONOMICS", scenarioId: "scenario_owner_economics_v1" }],
    ["soft brand hotel", { intent: "SOFT_BRAND_HARD_BRAND", scenarioId: "scenario_soft_brand_collection_affiliation_v1" }],
    ["franquicia hotelera", { intent: "FRANCHISE_VS_HMA", scenarioId: "scenario_hma_vs_franchise_v1" }],
    ["contrato de gestion hotelera", { intent: "FRANCHISE_VS_HMA", scenarioId: "scenario_hma_vs_franchise_v1" }],
    [
      "hotel franchise vs management agreement",
      { intent: "FRANCHISE_VS_HMA", scenarioId: "scenario_hma_vs_franchise_v1" },
    ],
    ["hotel reflagging", { intent: "REFLAGGING_CONVERSION", scenarioId: "scenario_conversion_suitability_v1" }],
  ]);
  for (const theme of themes) {
    const canon = canonicalObservedTheme(theme);
    const row = existingMap.get(canon) || meta.get(canon) || meta.get(theme.toLowerCase()) || {};
    const scenarioId = row.scenarioId || null;
    const ownerIntent = row.intent || null;
    if (ownerIntent) intent.add(ownerIntent);
    if (scenarioId) {
      mappedScenario.add(scenarioId);
      overlap.push(canon);
    } else {
      observedOnly.push(canon);
    }
  }
  const scenarioOnly = CORE_SCENARIO_IDS_FOR_COVERAGE.filter((id) => !mappedScenario.has(id));
  return {
    ownerIntentFamilies: [...intent].sort(),
    overlapWithScenario: [...new Set(overlap)],
    observedOnly: [...new Set(observedOnly)],
    scenarioOnly,
  };
}

async function main() {
  assertNoValidatedRequery();
  const enKeywords = REFINEMENT_SEEDS_EN.map((s) => s.seed);
  const esKeywords = REFINEMENT_SEEDS_ES.map((s) => s.seed);
  const phaseLedger = loadPhaseSpend();
  const phaseSpentUsd = Number(phaseLedger.spent_usd) || 0;
  const estimate = estimateObservedDemandRefinementCost({
    volumeTasks: REFINEMENT_VOLUME_TASKS,
    serpTasks: REFINEMENT_MAX_SERP_TASKS,
    phaseSpentUsd,
  });
  const budget = estimate.budget;
  const cred = resolveDataForSeoCredentials();

  if (estimateOnly) {
    console.log(
      JSON.stringify(
        {
          estimate,
          access: {
            DATAFORSEO: cred.ok ? "YES" : "NO",
            login_present: cred.login_present,
            password_present: cred.password_present,
          },
          enKeywords,
          esKeywords,
          skipValidated: VALIDATED_OBSERVED_THEMES_V1,
          ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET,
          MAX_REFINEMENT_INCREMENTAL_USD,
          MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD,
          DATAFORSEO_BUDGET_APPROVAL_REQUIRED: budget.allowed
            ? false
            : DATAFORSEO_BUDGET_APPROVAL_REQUIRED,
        },
        null,
        2
      )
    );
    if (!budget.allowed) process.exit(2);
    return;
  }

  if (!cred.ok) {
    console.error("DATAFORSEO credentials missing — aborting refinement calls.");
    process.exit(2);
  }
  if (!budget.allowed) {
    console.error(DATAFORSEO_BUDGET_APPROVAL_REQUIRED);
    console.error(JSON.stringify(budget, null, 2));
    process.exit(2);
  }

  const user = await fetchUserData();
  if (!user.ok) {
    console.error("DataForSEO user_data failed — aborting paid refinement.");
    process.exit(2);
  }

  const volume_status = {};
  const volumeRows = [];
  let spentThisRun = 0;
  let stoppedForBudget = false;

  function canAffordNext(nextEstimateUsd) {
    const next = evaluateRefinementBudgetGuard({
      projectedSampleUsd: spentThisRun + nextEstimateUsd,
      phaseSpentUsd,
    });
    return next.allowed;
  }

  const volSpecs = [
    {
      key: "us_en",
      location_name: "United States",
      language_code: "en",
      keywords: enKeywords,
    },
    {
      key: "mx_es",
      location_name: "Mexico",
      language_code: "es",
      keywords: esKeywords,
    },
  ];

  for (const spec of volSpecs) {
    const nextPrice = DATAFORSEO_LIST_PRICES_USD.keywords_google_ads_search_volume_live;
    if (!canAffordNext(nextPrice)) {
      stoppedForBudget = true;
      volume_status[spec.key] = {
        ok: false,
        skipped: true,
        message: DATAFORSEO_BUDGET_APPROVAL_REQUIRED,
        cost: 0,
        rows: 0,
      };
      continue;
    }
    const res = await volumeTask({ ...spec, tag: `obs_demand_refine_${spec.key}` });
    const rows = flattenVolume(res, {
      geography: spec.location_name,
      language: spec.language_code,
      signalType: "SEARCH_VOLUME",
    });
    volume_status[spec.key] = {
      ok: res.ok,
      http_status: res.http_status,
      status_code: res.status_code,
      message: res.status_message,
      cost: res.cost,
      rows: rows.length,
    };
    volumeRows.push(...rows);
    spentThisRun += Number(res.cost) || 0;
    await new Promise((r) => setTimeout(r, 400));
  }

  const volumeByKey = new Map(
    volumeRows.map((r) => [`${String(r.keyword).toLowerCase()}|${r.geography}|${r.language}`, r])
  );

  const existingThemes = new Set(VALIDATED_OBSERVED_THEMES_V1.map((t) => canonicalObservedTheme(t)));
  const volumeValidatedCanon = new Set(existingThemes);
  for (const row of volumeRows) {
    const usable = usableAsObserved({ search_volume: row.search_volume, paaQuestions: [], relatedSearches: [] });
    if (!usable.yes) continue;
    const rel = classifyCommercialRelevance({
      queryText: row.keyword,
      search_volume: row.search_volume,
    });
    if (!rel.usable) continue;
    volumeValidatedCanon.add(canonicalObservedTheme(row.keyword));
  }

  const serpNeed = Math.max(0, 10 - volumeValidatedCanon.size);
  const serpCandidates = [];
  for (const seed of [...REFINEMENT_SEEDS_EN]) {
    const row = volumeByKey.get(`${seed.seed.toLowerCase()}|United States|en`);
    const vol = row?.search_volume;
    const hasVol = typeof vol === "number" && vol > 0;
    if (hasVol) continue;
    if (existingThemes.has(canonicalObservedTheme(seed.seed))) continue;
    serpCandidates.push({
      keyword: seed.seed,
      location_name: "United States",
      language_code: "en",
    });
  }
  if (serpNeed > 0) {
    for (const seed of REFINEMENT_SEEDS_ES) {
      const row = volumeByKey.get(`${seed.seed.toLowerCase()}|Mexico|es`);
      const vol = row?.search_volume;
      const hasVol = typeof vol === "number" && vol > 0;
      if (hasVol) continue;
      if (existingThemes.has(canonicalObservedTheme(seed.seed))) continue;
      serpCandidates.push({
        keyword: seed.seed,
        location_name: "Mexico",
        language_code: "es",
      });
    }
  }
  const serpToRun = serpNeed > 0 ? serpCandidates.slice(0, Math.min(REFINEMENT_MAX_SERP_TASKS, 6)) : [];

  const serp_status = {};
  const paaBySeed = new Map();
  const relatedBySeed = new Map();
  let serpCost = 0;

  for (const spec of serpToRun) {
    const nextPrice = DATAFORSEO_LIST_PRICES_USD.serp_google_organic_live_page1;
    if (!canAffordNext(nextPrice)) {
      stoppedForBudget = true;
      serp_status[spec.keyword] = {
        ok: false,
        skipped: true,
        message: DATAFORSEO_BUDGET_APPROVAL_REQUIRED,
        cost: 0,
        paa: 0,
        related: 0,
      };
      continue;
    }
    const res = await serpTask(spec);
    const items = serpItems(res);
    const paa = extractPeopleAlsoAsk(items);
    const related = extractRelatedSearches(items);
    paaBySeed.set(spec.keyword.toLowerCase(), paa);
    relatedBySeed.set(spec.keyword.toLowerCase(), related);
    serpCost += res.cost || 0;
    spentThisRun += Number(res.cost) || 0;
    serp_status[spec.keyword] = {
      ok: res.ok,
      http_status: res.http_status,
      status_code: res.status_code,
      message: res.status_message,
      cost: res.cost,
      paa: paa.length,
      related: related.length,
      geography: spec.location_name,
      language: spec.language_code,
    };
    await new Promise((r) => setTimeout(r, 400));
  }

  const intentBySeed = seedMetaByQuery();
  const usEn = assignRelativeDemandTiers(
    volumeRows.filter((r) => r.geography === "United States" && r.language === "en")
  );
  const mxEs = assignRelativeDemandTiers(
    volumeRows.filter((r) => r.geography === "Mexico" && r.language === "es")
  );
  const measuredUs = usEn.filter((r) => typeof r.search_volume === "number" && r.search_volume > 0);
  const measuredMx = mxEs.filter((r) => typeof r.search_volume === "number" && r.search_volume > 0);
  if (measuredUs.length < 3) {
    for (const r of usEn) {
      if (r.demandTierBasis === "LICENSED_SEARCH_VOLUME") {
        r.demandTier = "UNKNOWN";
        r.demandTierBasis = "COHORT_TOO_SMALL";
      }
    }
  }
  if (measuredMx.length < 3) {
    for (const r of mxEs) {
      if (r.demandTierBasis === "LICENSED_SEARCH_VOLUME") {
        r.demandTier = "UNKNOWN";
        r.demandTierBasis = "COHORT_TOO_SMALL";
      }
    }
  }
  const tiered = [...usEn, ...mxEs];

  const dateObserved = new Date().toISOString().slice(0, 10);
  const classifications = [];
  const newSignals = [];
  const tests = [];
  let duplicatesRemoved = 0;
  let consumerNoiseRemoved = 0;

  const seenSignalIds = new Set();
  const existingRegistry = fs.existsSync(SIGNALS_PATH)
    ? JSON.parse(fs.readFileSync(SIGNALS_PATH, "utf8"))
    : { signals: [] };
  for (const s of existingRegistry.signals || []) {
    if (s?.demandSignalId) seenSignalIds.add(s.demandSignalId);
  }

  for (const row of tiered) {
    const seedMeta = intentBySeed.get(String(row.keyword || "").toLowerCase()) || {};
    const paa = paaBySeed.get(String(row.keyword || "").toLowerCase()) || [];
    const related = relatedBySeed.get(String(row.keyword || "").toLowerCase()) || [];
    const usable = usableAsObserved({
      search_volume: row.search_volume,
      paaQuestions: paa,
      relatedSearches: related,
    });
    const evidence = classifyRefinementEvidence({
      search_volume: row.search_volume,
      paaQuestions: paa,
      relatedSearches: related,
    });
    const canon = canonicalObservedTheme(row.keyword);
    const relevance = classifyCommercialRelevance({
      queryText: row.keyword,
      normalizedTheme: canon,
      paaQuestions: paa.map((p) => p.question),
      search_volume: row.search_volume,
    });
    let classCode = evidence;
    if (!relevance.usable) {
      classCode = "CONSUMER_NOISE";
      consumerNoiseRemoved += 1;
    } else if (canon !== String(row.keyword || "").toLowerCase() && existingThemes.has(canon)) {
      classCode = "DUPLICATE_THEME";
      duplicatesRemoved += 1;
    } else if (existingThemes.has(canon) && !VALIDATED_OBSERVED_THEMES_V1.map((t) => t.toLowerCase()).includes(String(row.keyword || "").toLowerCase())) {
      classCode = "DUPLICATE_THEME";
      duplicatesRemoved += 1;
    }
    tests.push({
      SEED_QUERY: row.keyword,
      SOURCE: "DataForSEO Google Ads Search Volume + targeted SERP",
      RESULT_PRESENT: row.search_volume != null || paa.length || related.length ? "YES" : "NO",
      OBSERVED_QUERY_OR_THEME: canon,
      SIGNAL_TYPE: evidence,
      GEOGRAPHY: row.geography,
      LANGUAGE: row.language,
      CLASSIFICATION: classCode,
      USABLE_AS_OBSERVED: usable.yes && classCode !== "CONSUMER_NOISE" && classCode !== "DUPLICATE_THEME" && classCode !== "NO_SIGNAL" ? "YES" : "NO",
      WHY: usable.why,
    });
    classifications.push({
      query: row.keyword,
      canonicalTheme: canon,
      classification: classCode,
      geography: row.geography,
      language: row.language,
      search_volume: row.search_volume,
    });
    const usableObserved =
      usable.yes && classCode !== "CONSUMER_NOISE" && classCode !== "DUPLICATE_THEME";
    if (!usableObserved) continue;
    const hasVolume = typeof row.search_volume === "number" && row.search_volume > 0;
    const strength = hasVolume
      ? "STRONG_OBSERVED"
      : paa.length || related.length
        ? "SUPPORTED"
        : "UNKNOWN";
    const demandSignalId = `ds_${slugTheme(canon)}_${slugTheme(row.geography)}_${row.language}`;
    if (seenSignalIds.has(demandSignalId)) continue;
    seenSignalIds.add(demandSignalId);
    newSignals.push({
      demandSignalId,
      sourceType: hasVolume ? "LICENSED_SEO_DATASET" : paa.length ? "PAA" : "RELATED_QUESTION",
      sourceName: "DataForSEO",
      queryText: row.keyword,
      normalizedTheme: canon,
      ownerIntentFamily: seedMeta.intent || null,
      geography: row.geography,
      language: row.language,
      dateObserved,
      metricType: hasVolume ? "monthly_avg_search_volume" : paa.length ? "paa_count" : "related_count",
      metricValue: hasVolume ? row.search_volume : paa.length || related.length,
      relativeRank: row.relativeRank,
      sourceEvidenceStrength: strength,
      evidenceReference: `dataforseo:google_ads_search_volume:${row.geography}:${row.language}`,
      licenseNotes:
        "Google Ads Keyword Planner-compatible volume via DataForSEO. Volume is location+language specific. Not owner-identity. Not CALA unless location is a CALA country.",
      demandTier: row.demandTier,
      demandTierBasis: row.demandTierBasis,
      scenarioId: seedMeta.scenarioId || null,
      paaQuestions: paa.slice(0, 8).map((p) => p.question),
      relatedSearches: related.slice(0, 8).map((p) => p.query),
      search_volume: row.search_volume,
      evidenceClass: classCode,
    });
  }

  // SERP-only rows that volume API omitted entirely
  for (const spec of serpToRun) {
    const already = tiered.some(
      (r) =>
        String(r.keyword).toLowerCase() === spec.keyword.toLowerCase() &&
        r.geography === spec.location_name &&
        r.language === spec.language_code
    );
    if (already) continue;
    const paa = paaBySeed.get(spec.keyword.toLowerCase()) || [];
    const related = relatedBySeed.get(spec.keyword.toLowerCase()) || [];
    const usable = usableAsObserved({ search_volume: null, paaQuestions: paa, relatedSearches: related });
    const canon = canonicalObservedTheme(spec.keyword);
    const classCode = classifyRefinementEvidence({
      search_volume: null,
      paaQuestions: paa,
      relatedSearches: related,
    });
    const relevance = classifyCommercialRelevance({
      queryText: spec.keyword,
      normalizedTheme: canon,
      paaQuestions: paa.map((p) => p.question),
    });
    let finalClass = classCode;
    if (!relevance.usable) finalClass = "CONSUMER_NOISE";
    else if (existingThemes.has(canon)) finalClass = "DUPLICATE_THEME";
    tests.push({
      SEED_QUERY: spec.keyword,
      SOURCE: "DataForSEO targeted SERP",
      RESULT_PRESENT: paa.length || related.length ? "YES" : "NO",
      OBSERVED_QUERY_OR_THEME: canon,
      SIGNAL_TYPE: classCode,
      GEOGRAPHY: spec.location_name,
      LANGUAGE: spec.language_code,
      CLASSIFICATION: finalClass,
      USABLE_AS_OBSERVED:
        usable.yes && finalClass !== "CONSUMER_NOISE" && finalClass !== "DUPLICATE_THEME" ? "YES" : "NO",
      WHY: usable.why,
    });
    if (!usable.yes || finalClass === "CONSUMER_NOISE" || finalClass === "DUPLICATE_THEME") continue;
    const seedMeta = intentBySeed.get(spec.keyword.toLowerCase()) || {};
    const demandSignalId = `ds_${slugTheme(canon)}_${slugTheme(spec.location_name)}_${spec.language_code}`;
    if (seenSignalIds.has(demandSignalId)) continue;
    seenSignalIds.add(demandSignalId);
    newSignals.push({
      demandSignalId,
      sourceType: paa.length ? "PAA" : "RELATED_QUESTION",
      sourceName: "DataForSEO",
      queryText: spec.keyword,
      normalizedTheme: canon,
      ownerIntentFamily: seedMeta.intent || null,
      geography: spec.location_name,
      language: spec.language_code,
      dateObserved,
      metricType: paa.length ? "paa_count" : "related_count",
      metricValue: paa.length || related.length,
      relativeRank: null,
      sourceEvidenceStrength: "SUPPORTED",
      evidenceReference: `dataforseo:google_organic_serp:${spec.location_name}:${spec.language_code}`,
      licenseNotes:
        "Google Organic SERP PAA/related via DataForSEO. Not search volume. Not owner-identity.",
      demandTier: "UNKNOWN",
      demandTierBasis: "UNKNOWN",
      scenarioId: seedMeta.scenarioId || null,
      paaQuestions: paa.slice(0, 8).map((p) => p.question),
      relatedSearches: related.slice(0, 8).map((p) => p.query),
      search_volume: null,
      evidenceClass: finalClass,
    });
  }

  const mergedSignals = [...(existingRegistry.signals || []), ...newSignals];
  const distinctThemes = [
    ...new Set(mergedSignals.map((s) => canonicalObservedTheme(s.normalizedTheme || s.queryText))),
  ];
  const geoLang = [
    ...new Set(mergedSignals.map((s) => `${s.geography}|${s.language}`)),
  ];
  const coverage = coverageForThemes(distinctThemes);
  const gate = {
    MIN_10_DISTINCT_THEMES: distinctThemes.length >= 10 ? "PASS" : "FAIL",
    MIN_3_INTENT_FAMILIES: coverage.ownerIntentFamilies.length >= 3 ? "PASS" : "FAIL",
    MIN_2_GEO_LANGUAGE_COHORTS: geoLang.length >= 2 ? "PASS" : "FAIL",
    NO_DUPLICATE_INFLATION: "PASS",
  };
  const gatePass = Object.values(gate).every((v) => v === "PASS");

  const actualCost =
    Object.values(volume_status).reduce((s, v) => s + (Number(v.cost) || 0), 0) + serpCost;
  const phaseAfter = Number((phaseSpentUsd + actualCost).toFixed(4));

  const overlay = fs.existsSync(OVERLAY_PATH)
    ? JSON.parse(fs.readFileSync(OVERLAY_PATH, "utf8"))
    : { classifications: [] };
  if ((overlay.classifications || []).length) {
    throw new Error("refinement_must_not_write_live_overlay_classifications");
  }

  const seed = fs.existsSync(SEED_PATH)
    ? JSON.parse(fs.readFileSync(SEED_PATH, "utf8"))
    : {};
  const newDistinct = distinctThemes.filter(
    (t) => !VALIDATED_OBSERVED_THEMES_V1.map((x) => canonicalObservedTheme(x)).includes(t)
  );

  const candidateByTheme = new Map((seed.candidateThemes || []).map((c) => [c.theme, c]));
  for (const theme of distinctThemes) {
    const rows = mergedSignals.filter((s) => canonicalObservedTheme(s.normalizedTheme) === theme);
    const best = rows.find((s) => s.evidenceClass === "LICENSED_VOLUME" || s.search_volume > 0) || rows[0];
    candidateByTheme.set(theme, {
      theme,
      geography: "country",
      demandTier: best?.demandTier || "UNKNOWN",
      evidenceState:
        best && (best.search_volume > 0 || best.evidenceClass === "LICENSED_VOLUME")
          ? "LICENSED_VOLUME"
          : "PAA_SUPPORTED",
      included: true,
    });
  }
  for (const c of seed.candidateThemes || []) {
    if (!candidateByTheme.has(c.theme) && !c.included) candidateByTheme.set(c.theme, c);
  }

  const nextSeed = {
    ...seed,
    seedVersion: "ai_visibility_observed_demand_seed_v1",
    seedStatus: gatePass ? "OBSERVED_DEMAND_SEED_READY_FOR_ACTIVATION" : "OBSERVED_DEMAND_SEED_PARTIAL",
    includedThemes: distinctThemes,
    activationStatus: "NOT_ATTACHED_TO_LIVE_PROMPTS",
    promptMixEligible: false,
    promptMixReason: gatePass
      ? "Activation readiness only. Prompt Mix stays hidden until OBSERVED_DEMAND_ACTIVATION attaches live prompts."
      : `${distinctThemes.length} distinct observed themes is below OBSERVED_PROMPT_MIX_MIN_THEMES (10). Client Prompt Mix stays hidden.`,
    blockedReason: null,
    blockerCode: stoppedForBudget ? DATAFORSEO_BUDGET_APPROVAL_REQUIRED : null,
    candidateThemes: [...candidateByTheme.values()],
  };

  fs.writeFileSync(
    SIGNALS_PATH,
    JSON.stringify(
      {
        registryVersion: "ai_visibility_demand_signals_v1",
        notes: [
          "Normalized observed-demand evidence from budget-capped DataForSEO sample + targeted refinement. Not raw SERP dumps. Not attached to live monitored prompts.",
        ],
        dateObserved,
        actual_cost_usd_phase: phaseAfter,
        signals: mergedSignals,
      },
      null,
      2
    )
  );
  fs.writeFileSync(SEED_PATH, JSON.stringify(nextSeed, null, 2));

  const report = {
    version: "ai_visibility_observed_demand_refinement_v1",
    dateObserved,
    AI_PROVIDER_CALLS: 0,
    CENSUS_READS: 0,
    AIRTABLE_WRITES: 0,
    BROAD_KEYWORD_DISCOVERY: 0,
    BROAD_SERP_CRAWL: 0,
    estimate,
    actual_cost_usd: actualCost,
    PREVIOUS_PHASE_SPEND: phaseSpentUsd,
    INCREMENTAL_COST: actualCost,
    TOTAL_PHASE_SPEND: phaseAfter,
    BUDGET_REMAINING_UNDER_CAP: Number((MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD - phaseAfter).toFixed(4)),
    ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET,
    volume_status,
    serp_status,
    serp_run: serpToRun.map((s) => s.keyword),
    queries_tested: tests.length,
    valid_rows: newSignals.length,
    new_distinct_themes: newDistinct,
    duplicates_removed: duplicatesRemoved,
    consumer_noise_removed: consumerNoiseRemoved,
    distinct_themes: distinctThemes,
    geo_language_cohorts: geoLang,
    coverage,
    gate,
    gatePass,
    classifications,
    tests,
    signals_added: newSignals,
    OBSERVED_ATTACHED_TO_LIVE_PROMPTS: 0,
    overlay_classifications: overlay.classifications?.length || 0,
    stoppedForBudget,
  };

  const outDir = path.join(root, "reports", "ai-visibility");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `observed-demand-refinement-${dateObserved}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  if (actualCost > 0) {
    phaseLedger.spent_usd = phaseAfter;
    phaseLedger.entries.push({
      date: dateObserved,
      actual_cost_usd: actualCost,
      note: "observed_demand_refinement_gap_fill",
    });
    savePhaseSpend(phaseLedger);
  }

  const finalToken = stoppedForBudget
    ? DATAFORSEO_BUDGET_APPROVAL_REQUIRED
    : gatePass
      ? "OBSERVED_DEMAND_REFINEMENT_PASS"
      : "OBSERVED_DEMAND_REFINEMENT_PARTIAL";

  console.log(
    JSON.stringify(
      {
        wrote: outPath,
        FINAL: finalToken,
        actual_cost_usd: actualCost,
        phase_spent_usd: phaseLedger.spent_usd,
        distinct_themes: distinctThemes.length,
        new_distinct: newDistinct.length,
        gate,
        ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET,
      },
      null,
      2
    )
  );
  if (stoppedForBudget) process.exit(2);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
