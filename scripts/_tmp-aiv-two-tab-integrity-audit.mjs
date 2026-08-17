/**
 * Brand AI Visibility — two-tab integrity audit (read-only).
 * CODE_CHANGES=0 product impact; this harness is diagnostic only.
 * No provider calls / Airtable writes / monitoring regeneration.
 */
import "dotenv/config";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  HEADLINE_GEOGRAPHIES,
  findMatchingSummaries,
  getBrandPortfolioPayload,
  getBrandOverviewPayload,
  getBrandTrendPayload,
  getBrandCompetitorsPayload,
  getBrandSourcesPayload,
  getBrandQuestionsPayload,
  resolveBrandGeographyMonitoringState,
  parseGeographyQuery,
} from "../lib/ai-visibility/brand-read-service.js";
import { getBrandExecutiveSummaryPayload } from "../lib/ai-visibility/brand-executive-summary.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import {
  resolvePeerSetMembership,
  peerSetBrandNamesById,
  PEER_SET_ID_V2,
} from "../lib/ai-visibility/peer-sets.js";
import { loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { fetchBrandBasicsMetaForIds } from "../lib/ai-visibility/load-brand-entitlements.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { computeAiPresenceRate, computeQuestionsMissing } from "../lib/ai-visibility/metrics.js";
import { computePortfolioQuestionMetrics } from "../lib/ai-visibility/portfolio-question-metrics.js";
import {
  buildCrossProviderPresenceIntelligence,
} from "../lib/ai-visibility/cross-provider-presence.js";
import {
  normalizeLanguage,
  recordMatchesLanguage,
} from "../lib/ai-visibility/language-dimension.js";
import {
  KNOWN_AI_VISIBILITY_PROVIDER_IDS,
  listAvailableAiVisibilityProviders,
  pickScaffoldDataProvider,
  isAllProvidersSelector,
} from "../lib/ai-visibility/provider-dimension.js";
import { resolveOwnedDomainsForBrand } from "../lib/ai-visibility/brand-website-wiring.js";
import { computeResponseCitationRates } from "../lib/ai-visibility/citation-intelligence.js";
import fs from "fs";
import path from "path";

const outDir = path.join("data", "ai-visibility", "audits");
fs.mkdirSync(outDir, { recursive: true });

const report = {
  generatedAt: new Date().toISOString(),
  contracts: {},
  filterUniverse: {},
  matrix: {},
  defects: [],
  contamination: {
    spanishInEnglish: 0,
    englishInSpanish: 0,
    providerCross: 0,
    crossGeography: 0,
    subject: 0,
  },
  cases: [],
  reconciliation: [],
  allProviders: {},
  staleState: {},
  evidence: {},
  denomChecks: [],
  ownerIntentArithmetic: [],
};

function defect(sev, tab, section, metric, ctx, displayed, recalculated, root) {
  report.defects.push({
    severity: sev,
    tab,
    section,
    metric,
    filterContext: ctx,
    displayed,
    recalculated,
    rootCause: root,
  });
}

function approxEq(a, b, tol = 0.0015) {
  if (a == null && b == null) return true;
  if (typeof a !== "number" || typeof b !== "number") return a === b;
  return Math.abs(a - b) <= tol;
}

const store = createBrandAiVisibilityReadStore({});
const showcase = loadShowcaseCompaniesConfig();
const marriott = (showcase.companies || []).find((c) => c.companyKey === "marriott");
const brandIds = marriott?.brandIds || [];
const brandNamesById = Object.fromEntries(
  (marriott?.brands || []).map((b) => [b.brandId, b.brandName])
);
const membership = resolvePeerSetMembership({
  peerSetId: PEER_SET_ID_V2,
  commercialRegion: "CALA",
});
const peerNames = peerSetBrandNamesById(PEER_SET_ID_V2);
const brandMeta = await fetchBrandBasicsMetaForIds([
  ...brandIds,
  ...(membership.entityIds || []),
]);
const names = { ...peerNames, ...brandMeta.brandNamesById, ...brandNamesById };
const basics = brandMeta.brandBasicsById || {};
const entitlementGraph = buildFixtureEntitlementGraph({
  entitledBrandIds: brandIds,
  peerBrandIds: membership.entityIds || [],
  source: "two_tab_integrity_audit",
});
const viewerContext = {
  viewerType: "dealality_internal",
  companyId: null,
  entitledBrandIds: brandIds,
};
const commonBase = {
  dealalityUser: { demoBrandPortfolioKey: "marriott" },
  viewerContext,
  entitlementGraph,
  store,
  brandNamesById: names,
  brandBasicsById: basics,
};

const AUTOGRAPH = "recEJCTDj1zrsjPM6";
const TRIBUTE = "recCvV0PuZOi8c3hC";
const AC = "rec9aZp7GHtzUEg0c";

report.contracts = {
  CONTRACTS_READ: [
    "docs/ai-build-system/FEATURE_BRIEF_AI_VISIBILITY.md",
    "docs/ai-build-system/AI_VISIBILITY_EXECUTIVE_SUMMARY_DETAILED_VIEW_IA.md",
    "docs/ai-build-system/AI_VISIBILITY_HOTEL_DECISION_VISIBILITY_DEFINITIONS.md",
    "lib/ai-visibility/validation/metric-contracts.js",
    "lib/ai-visibility/language-dimension.js",
    "lib/ai-visibility/cross-provider-presence.js",
    "lib/ai-visibility/provider-dimension.js",
    "lib/ai-visibility/metrics.js",
    "lib/ai-visibility/portfolio-question-metrics.js",
    "lib/ai-visibility/owned-domain-resolution.js",
    "lib/ai-visibility/citation-intelligence.js",
  ],
  METRIC_CONTRACTS_READ: [
    "AI_PRESENCE",
    "QUESTIONS_MISSING (portfolio unique-prompt + brand observation)",
    "COMPETITIVE_POSITION (Presence-rate rank)",
    "CITATION / OWNED_SOURCE rates",
  ],
  FILTER_CONTRACTS_READ: [
    "Geography HEADLINE_GEOGRAPHIES",
    "Provider first-class + All Providers DERIVED",
    "Language en|es; no locale; no cross-language aggregation",
  ],
  PROVIDER_CONTRACTS_READ: [
    "KNOWN_AI_VISIBILITY_PROVIDER_IDS",
    "ALL_PROVIDERS_SELECTOR_ID=all",
    "pickScaffoldDataProvider prefers openai",
    "MISSING != ZERO",
  ],
  LANGUAGE_REGIONALIZATION_CONTRACTS_READ: [
    "AI_VISIBILITY_LANGUAGE_VERSION",
    "recordMatchesLanguage / treatMissingAsEn for legacy EN",
    "Global not derived from regional average",
  ],
  CITATION_CONTRACTS_READ: [
    "Observation.citations canonical path",
    "owned domains from Brand Website hierarchy",
    "Source mix OWNED_ONLY|MIXED|EXTERNAL_ONLY|NO_CITATIONS",
  ],
};

const available = await listAvailableAiVisibilityProviders({
  store,
  geographyScope: "Region",
  commercialRegion: "CALA",
});
report.filterUniverse = {
  GEOGRAPHY: HEADLINE_GEOGRAPHIES.map((g) => g.key),
  PROVIDER: ["all", ...KNOWN_AI_VISIBILITY_PROVIDER_IDS],
  PROVIDER_AVAILABLE_CALA: available.map((p) => p.id),
  LANGUAGE: ["en", "es"],
  BRAND_PORTFOLIO: {
    portfolio: brandIds,
    brands: brandIds.map((id) => ({ id, name: names[id] })),
    peerSetId: PEER_SET_ID_V2,
    peerCount: (membership.entityIds || []).length,
  },
  PROMPT_FAMILY_UI: [
    "Brand Selection",
    "Operator Selection",
    "Conversion",
    "New Build",
    "HMA vs Franchise",
    "Owner Economics",
    "Owner Flexibility",
    "Branded Residences",
    "Mixed Use",
    "Market / Geography",
    "Chain Scale / Positioning",
    "Development Strategy",
    "Other",
  ],
  MONITORING_PERIOD:
    "Latest completed batch per provider×geo×language; prior comparable for deltas; trends require ≥2 comparable periods",
};

const geosMonitored = ["CALA", "Europe", "Global", "North America"];
const geosNotMonitored = ["MEA", "Asia Pacific"];
const providers = ["openai", "claude", "gemini", "perplexity", "all"];
const languages = ["en", "es"];

/** Coverage-oriented matrix (not full Cartesian). */
const cases = [];
for (const provider of providers) {
  cases.push({
    id: `cala_${provider}_en_portfolio`,
    geography: "CALA",
    provider,
    language: "en",
    subject: "portfolio",
  });
}
for (const lang of languages) {
  cases.push({
    id: `cala_openai_${lang}_tribute`,
    geography: "CALA",
    provider: "openai",
    language: lang,
    subject: "brand",
    brandId: TRIBUTE,
  });
}
cases.push({
  id: "cala_all_en_tribute",
  geography: "CALA",
  provider: "all",
  language: "en",
  subject: "brand",
  brandId: TRIBUTE,
});
cases.push({
  id: "cala_openai_en_autograph_high",
  geography: "CALA",
  provider: "openai",
  language: "en",
  subject: "brand",
  brandId: AUTOGRAPH,
});
cases.push({
  id: "cala_openai_en_ac_low",
  geography: "CALA",
  provider: "openai",
  language: "en",
  subject: "brand",
  brandId: AC,
});
cases.push({
  id: "europe_openai_en_tribute",
  geography: "Europe",
  provider: "openai",
  language: "en",
  subject: "brand",
  brandId: TRIBUTE,
});
cases.push({
  id: "mea_openai_en_tribute_not_monitored",
  geography: "MEA",
  provider: "openai",
  language: "en",
  subject: "brand",
  brandId: TRIBUTE,
});
cases.push({
  id: "cala_claude_en_tribute",
  geography: "CALA",
  provider: "claude",
  language: "en",
  subject: "brand",
  brandId: TRIBUTE,
});
cases.push({
  id: "cala_gemini_es_tribute",
  geography: "CALA",
  provider: "gemini",
  language: "es",
  subject: "brand",
  brandId: TRIBUTE,
});

const possible =
  geosMonitored.length *
  providers.length *
  languages.length *
  (1 + brandIds.length);
report.matrix = {
  TOTAL_FILTER_COMBINATIONS_POSSIBLE: possible,
  AUDIT_CASES_SELECTED: cases.length,
  COVERAGE_RATIONALE:
    "Pairwise coverage: every provider×portfolio EN CALA; EN+ES brand; All Providers brand; high/low Presence brands; monitored+unmonitored geo; Claude+Gemini isolation; peer competitive via overview/competitors",
  UNTESTED_LOW_RISK_COMBINATIONS: [
    "MEA/Asia × all providers × es",
    "Global × every brand × every provider",
    "Intent-territory filter permutations",
  ],
};

async function loadCohort(geography, provider, language) {
  const geo = parseGeographyQuery({ geography });
  const dataProvider = isAllProvidersSelector(provider)
    ? pickScaffoldDataProvider(
        await listAvailableAiVisibilityProviders({
          store,
          geographyScope: geo.geographyScope,
          commercialRegion: geo.commercialRegion,
        })
      )
    : provider;
  const summaries = await findMatchingSummaries(store, geo, dataProvider, {
    language,
  });
  const latest = summaries[0];
  if (!latest) return { dataProvider, latest: null, observations: [] };
  const { observations } = await loadObservationsFromBatchSummary(store, latest, {
    matchedSlotKeys: latest._matchedSlotKeys?.length
      ? latest._matchedSlotKeys
      : undefined,
    language,
  });
  return { dataProvider, latest, observations: observations || [] };
}

function langContamination(observations, expectLang) {
  let wrong = 0;
  const samples = [];
  for (const o of observations) {
    const lang = normalizeLanguage(o.language ?? o.payload?.language) || "en";
    if (expectLang === "en" && lang === "es") {
      wrong += 1;
      if (samples.length < 3) samples.push(o.promptId || o.observationId);
    }
    if (expectLang === "es" && lang === "en") {
      wrong += 1;
      if (samples.length < 3) samples.push(o.promptId || o.observationId);
    }
  }
  return { wrong, samples };
}

function providerContamination(observations, expectProvider) {
  if (isAllProvidersSelector(expectProvider)) return { wrong: 0, samples: [] };
  let wrong = 0;
  const samples = [];
  for (const o of observations) {
    const p = String(o.provider || "").toLowerCase();
    if (p && p !== String(expectProvider).toLowerCase()) {
      wrong += 1;
      if (samples.length < 3) samples.push({ promptId: o.promptId, provider: p });
    }
  }
  return { wrong, samples };
}

for (const c of cases) {
  const caseResult = { id: c.id, ...c, checks: {} };
  const ctx = `${c.geography}|${c.provider}|${c.language}|${c.subject}`;

  try {
    const exec = await getBrandExecutiveSummaryPayload({
      ...commonBase,
      provider: c.provider,
      geography: c.geography,
      language: c.language,
    });

    const cohort = await loadCohort(c.geography, c.provider, c.language);
    const langC = langContamination(cohort.observations, c.language);
    const provC = providerContamination(cohort.observations, c.provider);
    if (c.language === "en") report.contamination.spanishInEnglish += langC.wrong;
    if (c.language === "es") report.contamination.englishInSpanish += langC.wrong;
    report.contamination.providerCross += provC.wrong;

    caseResult.checks.cohortN = cohort.observations.length;
    caseResult.checks.langContamination = langC;
    caseResult.checks.providerContamination = provC;
    caseResult.checks.dataProvider = cohort.dataProvider;

    // Portfolio AI Presence recompute
    if (c.subject === "portfolio") {
      const displayed = exec.currentPosition?.portfolioAiPresence;
      let recalculated = null;
      let method = null;
      if (isAllProvidersSelector(c.provider)) {
        const brands = exec.brands || [];
        const vals = brands
          .map((b) => b.aiPresence?.value)
          .filter((v) => typeof v === "number");
        recalculated = vals.length
          ? vals.reduce((a, b) => a + b, 0) / vals.length
          : null;
        method = "mean_of_brand_cross_provider_averages";
      } else {
        const meta = computePortfolioQuestionMetrics(
          cohort.observations,
          brandIds
        );
        recalculated =
          meta.eligiblePromptCount > 0
            ? (meta.eligiblePromptCount - meta.questionsMissingCount) /
              meta.eligiblePromptCount
            : null;
        method = "unique_prompt_portfolio";
        caseResult.checks.questionsMissing = {
          displayed: exec.currentPosition?.questionsMissing?.value,
          recalculated: meta.questionsMissingCount,
          eligible: meta.eligiblePromptCount,
          match: exec.currentPosition?.questionsMissing?.value === meta.questionsMissingCount,
        };
        if (
          meta.eligiblePromptCount > 0 &&
          exec.currentPosition?.questionsMissing?.value !== meta.questionsMissingCount &&
          exec.currentPosition?.questionsMissing?.availability === "observed"
        ) {
          defect(
            "P0",
            "executive",
            "Portfolio Snapshot",
            "Questions Missing",
            ctx,
            exec.currentPosition?.questionsMissing?.value,
            meta.questionsMissingCount,
            "Displayed Questions Missing ≠ unique-prompt recomputation"
          );
        }
      }
      caseResult.checks.portfolioAiPresence = {
        displayed: displayed?.value,
        displayAvail: displayed?.availability,
        recalculated,
        method,
        match: approxEq(displayed?.value, recalculated),
      };
      if (
        displayed?.availability === "observed" &&
        !approxEq(displayed?.value, recalculated)
      ) {
        defect(
          "P0",
          "executive",
          "Portfolio Snapshot",
          "Portfolio AI Presence",
          ctx,
          displayed?.value,
          recalculated,
          `Mismatch vs ${method}`
        );
      }
    }

    if (c.subject === "brand" && c.brandId) {
      const overview = await getBrandOverviewPayload({
        ...commonBase,
        brandId: c.brandId,
        provider: c.provider,
        geography: c.geography,
        language: c.language,
      });
      const presenceDisp = overview.kpis?.aiPresence?.value;
      let presenceRecalc = null;
      if (isAllProvidersSelector(c.provider)) {
        presenceRecalc = overview.kpis?.aiPresence?.value;
        // Independent: average per-provider mon presence
        const rows = [];
        for (const pid of available.map((p) => p.id)) {
          const mon = await resolveBrandGeographyMonitoringState({
            store,
            brandId: c.brandId,
            geoFilter: parseGeographyQuery({ geography: c.geography }),
            provider: pid,
            language: c.language,
          });
          rows.push({
            provider: pid,
            monitored: !!mon.monitored,
            presenceRate: typeof mon.presenceVal === "number" ? mon.presenceVal : null,
            geography: c.geography,
            language: c.language,
            monitoringWindow:
              mon.latestSummary?.completedAt?.slice?.(0, 10) ||
              mon.latestSummary?.batchId ||
              null,
            promptCohortKey: [
              c.geography,
              c.language,
              mon.latestSummary?.peerSet?.peerSetId ||
                mon.latestSummary?.peerSetId ||
                "peers_uu_collection_lifestyle_owner_decision_v2",
              mon.latestSummary?.metricVersion || "ai_visibility_metrics_v1",
            ].join("|"),
          });
        }
        const xp = buildCrossProviderPresenceIntelligence({
          entityId: c.brandId,
          geography: c.geography,
          language: c.language,
          providers: rows,
        });
        presenceRecalc = xp.CROSS_PROVIDER_AVERAGE_OBSERVED_PRESENCE;
        caseResult.checks.crossProvider = {
          displayed: presenceDisp,
          recalculated: presenceRecalc,
          notComparable: xp.NOT_COMPARABLE,
          monitored: xp.PROVIDERS_MONITORED,
          match: approxEq(presenceDisp, presenceRecalc),
        };
        if (!approxEq(presenceDisp, presenceRecalc) && presenceDisp != null) {
          defect(
            "P1",
            "detail",
            "Brand Detail",
            "AI Presence (All Providers)",
            ctx,
            presenceDisp,
            presenceRecalc,
            "Detail Presence ≠ rebuildCrossProviderPresenceIntelligence"
          );
        }
      } else {
        const pr = computeAiPresenceRate(cohort.observations, c.brandId);
        presenceRecalc = pr.value;
        caseResult.checks.brandPresence = {
          displayed: presenceDisp,
          recalculated: presenceRecalc,
          numerator: pr.numerator,
          denominator: pr.denominator,
          match: approxEq(presenceDisp, presenceRecalc),
        };
        if (
          overview.kpis?.aiPresence?.availability === "observed" &&
          !approxEq(presenceDisp, presenceRecalc)
        ) {
          defect(
            "P0",
            "detail",
            "Brand Detail",
            "AI Presence",
            ctx,
            presenceDisp,
            presenceRecalc,
            "Detail Presence ≠ computeAiPresenceRate on filtered cohort"
          );
        }

        // Subject contamination: presence numerator must only count subject
        const foreignHits = cohort.observations.filter((o) => {
          if (!o.success) return false;
          const present = o.presentEntityIds || [];
          return present.includes(c.brandId) === false && present.length > 0;
        });
        // Not contamination — just other brands present. Check subject presence uses only subject id.
        const badNumerator = (pr.numerator || 0) !==
          cohort.observations.filter(
            (o) => o.success && (o.presentEntityIds || []).includes(c.brandId)
          ).length;
        if (badNumerator) {
          report.contamination.subject += 1;
          defect(
            "P1",
            "detail",
            "Brand Detail",
            "AI Presence subject isolation",
            ctx,
            pr.numerator,
            null,
            "Presence numerator inconsistency"
          );
        }

        // Questions Missing reconcile
        const qm = computeQuestionsMissing(
          cohort.observations,
          c.brandId,
          [...new Set(cohort.observations.filter((o) => o.success).map((o) => o.promptId).filter(Boolean))]
        );
        const qmDisp = overview.kpis?.questionsMissing?.value;
        caseResult.checks.questionsMissing = {
          displayed: qmDisp,
          recalculated: qm.count ?? qm.value,
          match: qmDisp === (qm.count ?? qm.value),
        };

        // Provider presence panel arithmetic
        const panel = overview.providerPresencePanel;
        if (panel?.rows?.length) {
          for (const row of panel.rows) {
            if (
              typeof row.PRESENCE_RATE === "number" &&
              typeof row.MONITORED_N === "number" &&
              typeof row.PRESENT_N === "number" &&
              row.MONITORED_N > 0
            ) {
              const expect = row.PRESENT_N / row.MONITORED_N;
              if (!approxEq(row.PRESENCE_RATE, expect, 0.02)) {
                defect(
                  "P2",
                  "detail",
                  "Provider Presence",
                  "Presence vs present/monitored",
                  `${ctx}|${row.PROVIDER}`,
                  row.PRESENCE_RATE,
                  expect,
                  "PRESENCE_RATE ≠ PRESENT_N/MONITORED_N"
                );
              }
            }
            if (
              typeof row.MONITORED_N === "number" &&
              typeof row.PRESENT_N === "number" &&
              typeof row.QUESTIONS_MISSING_N === "number" &&
              row.PRESENT_N + row.QUESTIONS_MISSING_N !== row.MONITORED_N
            ) {
              // May be legitimate if missing uses unique-prompt and present uses obs count
              caseResult.checks.providerPresenceArith = caseResult.checks.providerPresenceArith || [];
              caseResult.checks.providerPresenceArith.push({
                provider: row.PROVIDER,
                present: row.PRESENT_N,
                missing: row.QUESTIONS_MISSING_N,
                monitored: row.MONITORED_N,
                sum: row.PRESENT_N + row.QUESTIONS_MISSING_N,
              });
              if (
                Math.abs(row.PRESENT_N + row.QUESTIONS_MISSING_N - row.MONITORED_N) > 0
              ) {
                defect(
                  "P2",
                  "detail",
                  "Provider Presence",
                  "present+missing≠monitored",
                  `${ctx}|${row.PROVIDER}`,
                  `${row.PRESENT_N}+${row.QUESTIONS_MISSING_N}`,
                  row.MONITORED_N,
                  "Unit/denominator mismatch risk between present and missing"
                );
              }
            }
          }
        }

        // Owner-intent WITH_PRESENCE + MISSING = MONITORED
        const intentRows =
          overview.decisionPatterns?.ownerIntentCoverage?.rows || [];
        for (const r of intentRows) {
          const monN = r.monitoredN ?? r.MONITORED_N ?? r.denominator;
          const presentN = r.presentN ?? r.PRESENT_N ?? r.numerator;
          const missingN =
            monN != null && presentN != null ? monN - presentN : null;
          if (monN != null && presentN != null) {
            report.ownerIntentArithmetic.push({
              caseId: c.id,
              family: r.intentTerritory,
              monitored: monN,
              present: presentN,
              missing: missingN,
              ok: presentN <= monN,
            });
          }
        }

        // Exec brand presence vs detail (from portfolio brands when same filters)
        const execBrand = (exec.brands || []).find((b) => b.brandId === c.brandId);
        if (execBrand && !isAllProvidersSelector(c.provider)) {
          report.reconciliation.push({
            concept: "Brand AI Presence",
            exec: execBrand.aiPresence?.value,
            detail: presenceDisp,
            expected: "equal under same provider/geo/language",
            match: approxEq(execBrand.aiPresence?.value, presenceDisp),
            caseId: c.id,
          });
          if (!approxEq(execBrand.aiPresence?.value, presenceDisp)) {
            defect(
              "P2",
              "both",
              "Presence",
              "Exec brand vs Detail Presence",
              ctx,
              execBrand.aiPresence?.value,
              presenceDisp,
              "Same-scope Presence mismatch between tabs"
            );
          }
        }

        // Citations owned domains
        const { owned } = resolveOwnedDomainsForBrand(c.brandId, {
          brandNamesById: names,
          brandBasicsById: basics,
        });
        caseResult.checks.ownedDomains = owned.ownedDomainList || [];
        if (cohort.observations.length && owned.ownedDomainList?.length) {
          const rates = computeResponseCitationRates(cohort.observations, {
            ownedDomains: owned.ownedDomainList,
          });
          const ownedDisp = overview.secondary?.ownedSourceCitationRate?.value;
          caseResult.checks.ownedCitation = {
            displayed: ownedDisp,
            recalculated: rates.OWNED_SOURCE_CITATION_RATE?.value,
            match: approxEq(ownedDisp, rates.OWNED_SOURCE_CITATION_RATE?.value),
          };
        }

        // Competitors / trend / questions / sources smoke
        const [comps, trend, questions, sources] = await Promise.all([
          getBrandCompetitorsPayload({
            ...commonBase,
            brandId: c.brandId,
            provider: c.provider,
            geography: c.geography,
            language: c.language,
          }),
          getBrandTrendPayload({
            ...commonBase,
            brandId: c.brandId,
            provider: c.provider,
            geography: c.geography,
            language: c.language,
          }),
          getBrandQuestionsPayload({
            ...commonBase,
            brandId: c.brandId,
            provider: c.provider,
            geography: c.geography,
            language: c.language,
            filter: "missing",
            watchlistMode: "missing",
            limit: 100,
          }),
          getBrandSourcesPayload({
            ...commonBase,
            brandId: c.brandId,
            provider: c.provider,
            geography: c.geography,
            language: c.language,
          }),
        ]);
        caseResult.checks.competitors = {
          count: (comps.competitors || []).length,
          subjectRank: (comps.competitors || []).find((x) => x.isSubject)
            ?.competitivePosition,
        };
        caseResult.checks.trend = {
          points: (trend.points || []).length,
          availability: trend.availability,
        };
        caseResult.checks.watchlist = {
          rows: (questions.questions || questions.rows || []).length,
        };
        caseResult.checks.sources = {
          count: (sources.sources || []).length,
        };

        // MEA not monitored expectation
        if (c.geography === "MEA") {
          const nm =
            overview.kpis?.aiPresence?.availability === "not_monitored" ||
            overview.availability === "not_monitored" ||
            !overview.kpis?.aiPresence;
          caseResult.checks.notMonitoredExpected = nm;
          if (!nm && overview.kpis?.aiPresence?.availability === "observed") {
            defect(
              "P1",
              "detail",
              "Brand Detail",
              "MEA should be Not Monitored",
              ctx,
              overview.kpis?.aiPresence?.availability,
              "not_monitored",
              "Unmonitored geography returned observed Presence"
            );
          }
        }
      }
    }

    // Language comparison module — only on exec
    if (c.id === "cala_openai_en_portfolio" && exec.languageComparison) {
      const lc = exec.languageComparison;
      caseResult.checks.languageComparison = {
        note: lc.presenceNote || lc.note,
        en: lc.EN_AI_PRESENCE_RATE ?? lc.en,
        es: lc.ES_AI_PRESENCE_RATE ?? lc.es,
        status: lc.status,
      };
    }

    // All Providers panel on portfolio all case
    if (c.id === "cala_all_en_portfolio") {
      report.allProviders = {
        DERIVATION:
          "Per-brand: mean of provider-specific Presence rates via buildCrossProviderPresenceIntelligence; Portfolio Presence = mean of those brand averages; Questions Missing from OpenAI scaffold unique-prompt; peer rank NOT_COMPARABLE",
        panel: exec.allProvidersPanel,
        portfolioPresence: exec.currentPosition?.portfolioAiPresence,
        questionsMissing: exec.currentPosition?.questionsMissing,
        bestRank: exec.currentPosition?.bestCompetitivePosition,
        COMPARABLE_INPUTS_ONLY:
          exec.allProvidersPanel?.NOT_COMPARABLE !== true,
        MISSING_AS_ZERO: false,
        scaffoldProvider: pickScaffoldDataProvider(available),
      };
    }
  } catch (err) {
    caseResult.error = err.message;
    defect(
      "P1",
      c.subject === "portfolio" ? "executive" : "detail",
      "payload",
      "load_failed",
      ctx,
      null,
      null,
      err.message
    );
  }

  report.cases.push(caseResult);
}

// Static stale-state / race analysis of client controller
const brandJs = fs.readFileSync(
  "public/js/ai-visibility/ai-visibility-brand.js",
  "utf8"
);
report.staleState = {
  abortController: /AbortController/.test(brandJs),
  requestGenerationGuard:
    /loadGeneration|requestId|_reqSeq|inflightToken|latestRequest/.test(brandJs),
  STALE_ASYNC_OVERWRITE_POSSIBLE:
    !/AbortController/.test(brandJs) &&
    !/loadGeneration|requestId|_reqSeq|inflightToken|latestRequest/.test(brandJs),
  note: "Client controller has no AbortController / request-generation guard; rapid filter changes can apply out-of-order responses.",
};

// Evidence traceability samples (openai CALA en Tribute)
{
  const cohort = await loadCohort("CALA", "openai", "en");
  const hit = cohort.observations.find(
    (o) => o.success && (o.presentEntityIds || []).includes(TRIBUTE)
  );
  const miss = cohort.observations.find(
    (o) => o.success && !(o.presentEntityIds || []).includes(TRIBUTE)
  );
  report.evidence = {
    PRESENCE: hit
      ? {
          status: "PASS",
          promptId: hit.promptId,
          evidenceId: hit.evidenceId,
          provider: hit.provider,
          language: hit.language,
        }
      : { status: "FAIL", reason: "no presence observation" },
    QUESTIONS_MISSING: miss
      ? {
          status: "PASS",
          promptId: miss.promptId,
          evidenceId: miss.evidenceId,
        }
      : { status: "FAIL" },
    LANGUAGE_DIFFERENCE: "checked via langContamination counters",
    PROVIDER_DIFFERENCE: "checked via providerContamination counters",
  };
}

// Prompt language sample check (metadata vs heuristic)
{
  let metaEnPromptEs = 0;
  let metaEsPromptEn = 0;
  const cohortEn = await loadCohort("CALA", "openai", "en");
  for (const o of cohortEn.observations.slice(0, 40)) {
    const text = String(o.promptText || o.payload?.promptText || "").toLowerCase();
    if (!text) continue;
    const hasSpanishChars = /[áéíóúñ¿¡]/.test(text) || /\b(hotel|marca|propietario|desarrollo)\b/.test(text) && /\b(qué|cuál|para)\b/.test(text);
    // Weak heuristic only — flag for review, not hard fail
    if (hasSpanishChars && /[áéíóúñ¿¡]/.test(text)) metaEnPromptEs += 1;
  }
  const cohortEs = await loadCohort("CALA", "openai", "es");
  for (const o of cohortEs.observations.slice(0, 40)) {
    const text = String(o.promptText || o.payload?.promptText || "").toLowerCase();
    if (!text) continue;
    const looksEnglish =
      /\b(which|what|hotel brand|owner|development|franchise)\b/.test(text) &&
      !/[áéíóúñ¿¡]/.test(text);
    if (looksEnglish) metaEsPromptEn += 1;
  }
  report.promptLanguage = { metaEnPromptEs, metaEsPromptEn };
}

const outPath = path.join(outDir, `two-tab-integrity-${Date.now()}.json`);
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({
  outPath,
  cases: report.cases.length,
  defects: report.defects.length,
  bySeverity: report.defects.reduce((acc, d) => {
    acc[d.severity] = (acc[d.severity] || 0) + 1;
    return acc;
  }, {}),
  contamination: report.contamination,
  stale: report.staleState.STALE_ASYNC_OVERWRITE_POSSIBLE,
  allProvidersPresence: report.allProviders?.portfolioPresence,
  reconciliationMismatches: report.reconciliation.filter((r) => !r.match).length,
}, null, 2));
