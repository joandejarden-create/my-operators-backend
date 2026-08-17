#!/usr/bin/env node
/**
 * Observed-demand source sample — DataForSEO Google Ads volume + SERP PAA.
 * AI provider calls: 0. Census reads: 0. Airtable writes: 0.
 *
 * Account top-up is funding, not a project budget.
 * Hard caps: MAX_SOURCE_SAMPLE_COST_USD = 1.00
 *            MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD = 2.00
 *
 *   node scripts/ai-visibility-observed-demand-source-sample.mjs --estimate-only
 *   node scripts/ai-visibility-observed-demand-source-sample.mjs
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
  SEED_CONCEPTS_EN,
  SEED_CONCEPTS_ES,
  SERP_SAMPLE_SEEDS,
  estimateObservedDemandSampleCost,
  extractPeopleAlsoAsk,
  extractRelatedSearches,
  assignRelativeDemandTiers,
  usableAsObserved,
  OBSERVED_DEMAND_SOURCE_SAMPLE_VERSION,
  DATAFORSEO_LIST_PRICES_USD,
  MAX_SOURCE_SAMPLE_COST_USD,
  MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD,
  DATAFORSEO_BUDGET_APPROVAL_REQUIRED,
  evaluateDataForSeoBudgetGuard,
  resolveObservedDemandCostCapUsd,
} from "../lib/ai-visibility/observed-demand-source-sample.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const PHASE_SPEND_PATH = path.join(
  root,
  "reports",
  "ai-visibility",
  "observed-demand-dataforseo-phase-spend.json"
);
const COST_CAP_USD = resolveObservedDemandCostCapUsd(
  process.env.OBSERVED_DEMAND_SAMPLE_COST_CAP_USD
);
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
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 60);
}

async function fetchUserData() {
  const cred = resolveDataForSeoCredentials();
  if (!cred.ok) {
    return { ok: false, reason: "DATAFORSEO_CREDENTIALS_MISSING" };
  }
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
    login_present: true,
    // never echo login email
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

async function main() {
  const volumeTasks = 3;
  const serpTasks = SERP_SAMPLE_SEEDS.length;
  const phaseLedger = loadPhaseSpend();
  const phaseSpentUsd = Number(phaseLedger.spent_usd) || 0;
  const estimate = estimateObservedDemandSampleCost({
    volumeTasks,
    serpTasks,
    costCapUsd: COST_CAP_USD,
    phaseSpentUsd,
  });
  const budget = estimate.budget;

  const cred = resolveDataForSeoCredentials();
  const access = {
    DATAFORSEO: cred.ok ? "YES" : "NO",
    login_present: cred.login_present,
    password_present: cred.password_present,
  };

  if (estimateOnly) {
    console.log(
      JSON.stringify(
        {
          estimate,
          access,
          ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET: true,
          MAX_SOURCE_SAMPLE_COST_USD,
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
    console.error("DATAFORSEO credentials missing — aborting sample calls.");
    process.exit(2);
  }
  if (!budget.allowed) {
    console.error(DATAFORSEO_BUDGET_APPROVAL_REQUIRED);
    console.error(
      JSON.stringify(
        {
          projectedSampleUsd: budget.projectedSampleUsd,
          phaseSpentUsd: budget.phaseSpentUsd,
          projectedPhaseUsd: budget.projectedPhaseUsd,
          sampleCap: budget.sampleCap,
          phaseCap: budget.phaseCap,
          ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET: true,
        },
        null,
        2
      )
    );
    process.exit(2);
  }

  const user = await fetchUserData();
  console.log(
    JSON.stringify(
      {
        phase: "access_check",
        DATAFORSEO: user.ok ? "YES" : "NO",
        user_data_cost: user.cost,
        account_funded: Number(user.money_balance) > 0,
        ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET: true,
        project_sample_cap_usd: MAX_SOURCE_SAMPLE_COST_USD,
        project_phase_cap_usd: MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD,
        phase_spent_usd: phaseSpentUsd,
        estimate,
      },
      null,
      2
    )
  );
  if (!user.ok) {
    console.error("DataForSEO user_data failed — aborting paid sample.");
    process.exit(2);
  }

  const enKeywords = SEED_CONCEPTS_EN.map((s) => s.seed);
  const esKeywords = SEED_CONCEPTS_ES.map((s) => s.seed);

  const volume_status = {};
  const volumeRows = [];
  let spentThisRun = 0;
  let stoppedForBudget = false;

  function canAffordNext(nextEstimateUsd) {
    const next = evaluateDataForSeoBudgetGuard({
      projectedSampleUsd: spentThisRun + nextEstimateUsd,
      phaseSpentUsd,
    });
    return next.allowed;
  }

  const volSpecs = [
    { key: "us_en", location_name: "United States", language_code: "en", keywords: enKeywords },
    { key: "mx_en", location_name: "Mexico", language_code: "en", keywords: enKeywords },
    { key: "mx_es", location_name: "Mexico", language_code: "es", keywords: esKeywords },
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
    const res = await volumeTask({ ...spec, tag: `obs_demand_${spec.key}` });
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

  const serp_status = {};
  const paaBySeed = new Map();
  const relatedBySeed = new Map();
  let serpCost = 0;

  for (const keyword of SERP_SAMPLE_SEEDS) {
    const nextPrice = DATAFORSEO_LIST_PRICES_USD.serp_google_organic_live_page1;
    if (!canAffordNext(nextPrice)) {
      stoppedForBudget = true;
      serp_status[keyword] = {
        ok: false,
        skipped: true,
        message: DATAFORSEO_BUDGET_APPROVAL_REQUIRED,
        cost: 0,
        paa: 0,
        related: 0,
      };
      continue;
    }
    const res = await serpTask({
      keyword,
      location_name: "United States",
      language_code: "en",
    });
    const items = serpItems(res);
    const paa = extractPeopleAlsoAsk(items);
    const related = extractRelatedSearches(items);
    paaBySeed.set(keyword, paa);
    relatedBySeed.set(keyword, related);
    serpCost += res.cost || 0;
    spentThisRun += Number(res.cost) || 0;
    serp_status[keyword] = {
      ok: res.ok,
      http_status: res.http_status,
      status_code: res.status_code,
      message: res.status_message,
      cost: res.cost,
      paa: paa.length,
      related: related.length,
    };
    await new Promise((r) => setTimeout(r, 400));
  }

  const intentBySeed = new Map(
    [...SEED_CONCEPTS_EN, ...SEED_CONCEPTS_ES].map((s) => [s.seed, s])
  );

  const usEn = assignRelativeDemandTiers(
    volumeRows.filter((r) => r.geography === "United States" && r.language === "en")
  );
  const mxEn = assignRelativeDemandTiers(
    volumeRows.filter((r) => r.geography === "Mexico" && r.language === "en")
  );
  const mxEs = assignRelativeDemandTiers(
    volumeRows.filter((r) => r.geography === "Mexico" && r.language === "es")
  );
  const tiered = [...usEn, ...mxEn, ...mxEs];

  const dateObserved = new Date().toISOString().slice(0, 10);
  const signals = [];
  const tests = [];

  for (const row of tiered) {
    const seedMeta = intentBySeed.get(row.keyword) || {};
    const paa = paaBySeed.get(row.keyword) || [];
    const related = relatedBySeed.get(row.keyword) || [];
    const usable = usableAsObserved({
      search_volume: row.search_volume,
      paaQuestions: paa,
      relatedSearches: related,
    });
    const hasVolume = typeof row.search_volume === "number" && row.search_volume > 0;
    const strength = hasVolume
      ? "STRONG_OBSERVED"
      : paa.length || related.length
        ? "SUPPORTED"
        : "UNKNOWN";
    tests.push({
      SEED_QUERY: row.keyword,
      SOURCE: "DataForSEO Google Ads Search Volume + SERP PAA (US EN SERP only)",
      RESULT_PRESENT: row.search_volume != null || paa.length || related.length ? "YES" : "NO",
      OBSERVED_QUERY_OR_THEME: row.keyword,
      SIGNAL_TYPE: hasVolume ? "SEARCH_VOLUME" : paa.length ? "PAA" : related.length ? "RELATED_SEARCH" : "SEARCH_QUERY",
      GEOGRAPHY: row.geography,
      LANGUAGE: row.language,
      METRIC_AVAILABLE: row.search_volume != null ? "search_volume" : paa.length ? "paa_count" : "none",
      DATE: dateObserved,
      USABLE_AS_OBSERVED: usable.yes ? "YES" : "NO",
      WHY: usable.why,
    });
    if (!usable.yes) continue;
    signals.push({
      demandSignalId: `ds_${slugTheme(row.keyword)}_${slugTheme(row.geography)}_${row.language}`,
      sourceType: hasVolume ? "LICENSED_SEO_DATASET" : paa.length ? "PAA" : "RELATED_QUESTION",
      sourceName: "DataForSEO",
      queryText: row.keyword,
      normalizedTheme: row.keyword,
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
    });
  }

  const actualCost =
    Object.values(volume_status).reduce((s, v) => s + (Number(v.cost) || 0), 0) + serpCost;

  const paymentRequired = Object.values(volume_status)
    .concat(Object.values(serp_status))
    .some((s) => s.http_status === 402 || s.status_code === 40200);

  const report = {
    version: OBSERVED_DEMAND_SOURCE_SAMPLE_VERSION,
    dateObserved,
    AI_PROVIDER_CALLS: 0,
    CENSUS_READS: 0,
    AIRTABLE_WRITES: 0,
    estimate,
    actual_cost_usd: actualCost,
    blockerCode: paymentRequired
      ? "DATAFORSEO_PAYMENT_REQUIRED"
      : stoppedForBudget
        ? DATAFORSEO_BUDGET_APPROVAL_REQUIRED
        : null,
    ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET: true,
    project_sample_cap_usd: MAX_SOURCE_SAMPLE_COST_USD,
    project_phase_cap_usd: MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD,
    phase_spent_before_usd: phaseSpentUsd,
    phase_spent_after_usd: Number((phaseSpentUsd + actualCost).toFixed(4)),
    user_data: {
      ok: user.ok,
      money_balance_present: user.money_balance != null,
    },
    volume_status,
    serp_status,
    queries_tested: tests.length,
    valid_signals: signals.filter((s) => s.search_volume > 0 || (s.paaQuestions || []).length).length,
    signals,
    tests,
  };

  const outDir = path.join(root, "reports", "ai-visibility");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `observed-demand-source-sample-${dateObserved}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  if (actualCost > 0) {
    phaseLedger.spent_usd = Number((phaseSpentUsd + actualCost).toFixed(4));
    phaseLedger.entries.push({
      date: dateObserved,
      actual_cost_usd: actualCost,
      note: "observed_demand_source_sample",
    });
    savePhaseSpend(phaseLedger);
  }
  console.log(
    JSON.stringify(
      {
        wrote: outPath,
        actual_cost_usd: actualCost,
        valid_signals: report.valid_signals,
        queries_tested: report.queries_tested,
        phase_spent_usd: phaseLedger.spent_usd,
        ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET: true,
        DATAFORSEO_BUDGET_APPROVAL_REQUIRED: stoppedForBudget
          ? DATAFORSEO_BUDGET_APPROVAL_REQUIRED
          : false,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
