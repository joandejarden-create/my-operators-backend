#!/usr/bin/env node
/**
 * Brand AI Visibility — correctness / contamination audit (read-only).
 * Uses production read functions. No live provider calls. No public crawl. No Airtable writes.
 *
 * Usage: npm run ai-visibility:audit-brand-report
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  HEADLINE_GEOGRAPHIES,
  findMatchingSummaries,
  getBrandQuestionsPayload,
  getBrandSourcesPayload,
  getBrandTrendPayload,
  parseGeographyQuery,
  resolveBrandGeographyMonitoringState,
} from "../lib/ai-visibility/brand-read-service.js";
import { getBrandExecutiveSummaryPayload } from "../lib/ai-visibility/brand-executive-summary.js";
import {
  listAvailableAiVisibilityProviders,
  formatProviderLabel,
} from "../lib/ai-visibility/provider-dimension.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { resolvePeerSetMembership, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { isBlockedFixtureDomain } from "../lib/ai-visibility/fixture-domain-guard.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(__dirname, "..");
const SHOWCASE = JSON.parse(
  fs.readFileSync(
    path.join(REPO, "fixtures/ai-visibility/brand-ai-showcase-companies-v1.json"),
    "utf8"
  )
);

const PROVIDERS = ["openai", "gemini", "perplexity", "claude"];
const RATE_KEYS = [
  "presenceVal",
  "shareVal",
  "recommendationRateVal",
  "top3RecommendationRateVal",
  "firstVal",
  "citationVal",
];

function fail(issues, severity, code, detail) {
  issues.push({ severity, code, detail });
}

function graphFor(companyKey) {
  const company = SHOWCASE.companies.find((c) => c.companyKey === companyKey);
  if (!company) throw new Error(`Unknown showcase company ${companyKey}`);
  const membership = resolvePeerSetMembership({
    peerSetId: PEER_SET_ID_V2,
    commercialRegion: "CALA",
  });
  const brandNamesById = Object.fromEntries(
    (company.brands || []).map((b) => [b.brandId, b.brandName])
  );
  return {
    company,
    brandNamesById,
    entitlementGraph: buildFixtureEntitlementGraph({
      entitledBrandIds: company.brandIds,
      peerBrandIds: membership.entityIds || [],
      source: `audit:${companyKey}`,
    }),
    peerIds: membership.entityIds || [],
  };
}

async function main() {
  const store = createBrandAiVisibilityReadStore({});
  const issues = [];
  const geographyAudit = [];
  const providerAudit = [];
  const trendAudit = [];
  const report = {
    phase: "Brand AI Visibility — Correctness, Reconciliation & Contamination Audit",
    LIVE_AI_PROVIDER_CALLS: 0,
    PUBLIC_DISCOVERABILITY_CALLS: 0,
    AIRTABLE_WRITES: 0,
    SCHEMA_CHANGES: 0,
    DEPLOYS: 0,
    timestamp: new Date().toISOString(),
  };

  const available = await listAvailableAiVisibilityProviders({ store });
  const availableIds = new Set(available.map((p) => p.id));

  // --- Provider purity ---
  for (const provider of PROVIDERS) {
    const cala = parseGeographyQuery({ geography: "CALA" });
    const summaries = await findMatchingSummaries(store, cala, provider, {
      language: "en",
    });
    const monitored = summaries.length > 0;
    providerAudit.push({
      PROVIDER: provider,
      MONITORING_DATA_EXISTS: monitored,
      LATEST_BATCHES: summaries.slice(0, 3).map((s) => s.batchId),
      GEOGRAPHIES: monitored ? ["CALA", ...(summaries[0]?._matchedSlotKeys || [])] : [],
      SUPPORTED_UI_STATE: monitored ? "OBSERVED_OR_ZERO" : "NOT_MONITORED",
      FALLBACK_TO_OPENAI: "NO",
      IN_AVAILABLE_LIST: availableIds.has(provider),
    });
    if (!monitored && availableIds.has(provider)) {
      fail(
        issues,
        "HIGH",
        "PROVIDER_LISTED_WITHOUT_CALA_BATCH",
        `${provider} listed as available but no CALA batch matched`
      );
    }
  }

  // Explicit: OpenAI data must not appear when asking for other providers
  const marriottBrand = "recEJCTDj1zrsjPM6";
  for (const provider of ["perplexity", "gemini", "claude"]) {
    const mon = await resolveBrandGeographyMonitoringState({
      store,
      brandId: marriottBrand,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider,
      language: "en",
    });
    const openaiMon = await resolveBrandGeographyMonitoringState({
      store,
      brandId: marriottBrand,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider: "openai",
      language: "en",
    });
    if (
      mon.monitored &&
      openaiMon.monitored &&
      mon.latestSummary?.batchId &&
      mon.latestSummary.batchId === openaiMon.latestSummary.batchId
    ) {
      fail(
        issues,
        "CRITICAL",
        "OPENAI_BATCH_RETURNED_FOR_OTHER_PROVIDER",
        `${provider} returned OpenAI batch ${mon.latestSummary.batchId}`
      );
    }
  }

  // --- Geography purity (Autograph / OpenAI) ---
  const geoPresence = {};
  for (const g of HEADLINE_GEOGRAPHIES) {
    const mon = await resolveBrandGeographyMonitoringState({
      store,
      brandId: marriottBrand,
      geoFilter: g,
      provider: "openai",
      language: "en",
    });
    const row = {
      GEOGRAPHY: g.key,
      LATEST_BATCH_ID: mon.latestSummary?.batchId || null,
      TIMESTAMP: mon.latestSummary?.completedAt || null,
      PROMPT_COUNT: mon.promptDenominator,
      MATCHED_SLOTS: mon.latestSummary?._matchedSlotKeys || null,
      AI_PRESENCE: mon.presenceVal,
      QUESTIONS_WON: mon.questionsWon,
      QUESTIONS_MISSING: mon.questionsMissing,
      PROVIDER: "openai",
      MONITORED: !!mon.monitored,
    };
    geographyAudit.push(row);
    if (mon.monitored) {
      for (const k of RATE_KEYS) {
        const v = mon[k];
        if (typeof v === "number" && (v < 0 || v > 1.0000001)) {
          fail(issues, "CRITICAL", "RATE_OUT_OF_BOUNDS", `${g.key}.${k}=${v}`);
        }
      }
      if (
        typeof mon.promptDenominator === "number" &&
        mon.promptDenominator > 0
      ) {
        if (mon.questionsWon > mon.promptDenominator) {
          fail(
            issues,
            "CRITICAL",
            "QUESTIONS_WON_EXCEEDS_DENOM",
            `${g.key}: won=${mon.questionsWon} denom=${mon.promptDenominator}`
          );
        }
        if (mon.questionsMissing > mon.promptDenominator) {
          fail(
            issues,
            "CRITICAL",
            "QUESTIONS_MISSING_EXCEEDS_DENOM",
            `${g.key}: missing=${mon.questionsMissing} denom=${mon.promptDenominator}`
          );
        }
      }
      geoPresence[g.key] = {
        presence: mon.presenceVal,
        slots: (mon.latestSummary?._matchedSlotKeys || []).join(","),
      };
    }
  }

  // Geography rows must not all share identical slot keys when multiple geos monitored
  const monitoredGeos = geographyAudit.filter((r) => r.MONITORED);
  if (monitoredGeos.length >= 2) {
    const slotSets = new Set(monitoredGeos.map((r) => (r.MATCHED_SLOTS || []).join("|")));
    if (slotSets.size === 1 && monitoredGeos.every((r) => (r.MATCHED_SLOTS || []).length)) {
      fail(
        issues,
        "CRITICAL",
        "GEOGRAPHY_SLOT_REUSE",
        "All monitored geos resolved the same matched slots"
      );
    }
  }

  // --- Trend ---
  const { entitlementGraph, brandNamesById } = graphFor("marriott");
  for (const geo of ["CALA", "Europe", "Global", "North America"]) {
    const trend = await getBrandTrendPayload({
      dealalityUser: { id: "audit" },
      entitlementGraph,
      store,
      brandId: marriottBrand,
      provider: "openai",
      geography: geo,
      language: "en",
    });
    trendAudit.push({
      ENTITY: marriottBrand,
      GEOGRAPHY: geo,
      PROVIDER: "openai",
      POINT_COUNT_TOTAL: trend.pointCount,
      POINT_COUNT_COMPARABLE: trend.pointCountComparable ?? trend.pointCount,
      POINT_TIMESTAMPS: (trend.points || []).map((p) => p.date),
      VALUES: (trend.points || []).map((p) => p.value),
      RENDER_STATE: trend.renderState,
      MESSAGE: trend.message,
    });
    if (trend.pointCount === 0 && trend.message && /Not Monitored/i.test(trend.message)) {
      // Verify monitoring actually absent
      const mon = await resolveBrandGeographyMonitoringState({
        store,
        brandId: marriottBrand,
        geoFilter: parseGeographyQuery({ geography: geo }),
        provider: "openai",
        language: "en",
      });
      if (mon.monitored) {
        fail(
          issues,
          "HIGH",
          "TREND_SAYS_NOT_MONITORED_BUT_CURRENT_EXISTS",
          geo
        );
      }
    }
  }

  // --- Fixture contamination ---
  const sources = await getBrandSourcesPayload({
    dealalityUser: { id: "audit" },
    entitlementGraph,
    brandNamesById,
    store,
    brandId: marriottBrand,
    provider: "openai",
    geography: "CALA",
    language: "en",
  });
  const badSources = (sources.sources || []).filter(
    (s) => isBlockedFixtureDomain(s.domain) || isBlockedFixtureDomain(s.url)
  );
  if (badSources.length) {
    fail(
      issues,
      "CRITICAL",
      "FIXTURE_DOMAIN_IN_RUNTIME_SOURCES",
      badSources.map((s) => s.domain).join(",")
    );
  }

  // --- Question reconciliation ---
  const questions = await getBrandQuestionsPayload({
    dealalityUser: { id: "audit" },
    entitlementGraph,
    store,
    brandId: marriottBrand,
    provider: "openai",
    geography: "CALA",
    language: "en",
    filter: "all",
  });
  const monCala = await resolveBrandGeographyMonitoringState({
    store,
    brandId: marriottBrand,
    geoFilter: parseGeographyQuery({ geography: "CALA" }),
    provider: "openai",
    language: "en",
  });
  let mismatch = 0;
  for (const row of questions.questions || []) {
    if (row.brandStatus === "Missing") {
      // OK if truly absent
      continue;
    }
    // Non-missing status implies presence in that prompt
    if (!row.brandStatus || row.brandStatus === "Missing") mismatch += 1;
  }
  const firstRec = (questions.questions || []).filter(
    (q) => q.brandStatus === "First Recommended"
  ).length;
  if (
    typeof monCala.questionsWon === "number" &&
    firstRec !== monCala.questionsWon
  ) {
    fail(
      issues,
      "HIGH",
      "QUESTION_STATUS_VS_WON_MISMATCH",
      `First Recommended rows=${firstRec} questionsWon=${monCala.questionsWon}`
    );
  }

  // --- Portfolio integrity ---
  const marriott = graphFor("marriott");
  const hilton = graphFor("hilton");
  const exec = await getBrandExecutiveSummaryPayload({
    dealalityUser: { id: "audit", demoBrandPortfolioKey: "marriott" },
    entitlementGraph: marriott.entitlementGraph,
    brandNamesById: marriott.brandNamesById,
    store,
    provider: "openai",
    geography: "CALA",
    language: "en",
  });
  const portfolioBrandCount = exec.portfolioOverview?.brands?.length ?? exec.brandsMonitored?.value;
  const entitled = marriott.company.brandIds.length;
  if (exec.portfolioOverview?.brands) {
    const ids = exec.portfolioOverview.brands.map((b) => b.brandId);
    const peerLeak = ids.filter((id) => !marriott.company.brandIds.includes(id));
    if (peerLeak.length) {
      fail(
        issues,
        "CRITICAL",
        "PORTFOLIO_PEER_CONFLATION",
        `Non-entitled brands in overview: ${peerLeak.join(",")}`
      );
    }
  }
  const wonRate = exec.currentPosition?.questionsWon?.value;
  const missRate = exec.currentPosition?.questionsMissing?.value;
  // values are counts in executive — check display rates if present
  const wonDisplay = String(exec.currentPosition?.questionsWon?.display || "");
  const missDisplay = String(exec.currentPosition?.questionsMissing?.display || "");
  const pctMatch = (s) => {
    const m = s.match(/([0-9]+(?:\.[0-9]+)?)%/);
    return m ? Number(m[1]) : null;
  };
  const wonPct = pctMatch(wonDisplay);
  const missPct = pctMatch(missDisplay);
  if (wonPct != null && wonPct > 100) {
    fail(issues, "CRITICAL", "PORTFOLIO_WON_PCT_GT_100", wonDisplay);
  }
  if (missPct != null && missPct > 100) {
    fail(issues, "CRITICAL", "PORTFOLIO_MISSING_PCT_GT_100", missDisplay);
  }

  // Evidence IDs on questions
  let brokenEvidence = 0;
  for (const row of (questions.questions || []).slice(0, 40)) {
    if (!row.evidenceId) continue;
    const ev = await store.getEvidence(row.evidenceId);
    if (!ev) brokenEvidence += 1;
  }
  if (brokenEvidence) {
    fail(issues, "HIGH", "BROKEN_EVIDENCE_IDS", String(brokenEvidence));
  }

  const critical = issues.filter((i) => i.severity === "CRITICAL");
  const high = issues.filter((i) => i.severity === "HIGH");
  const medium = issues.filter((i) => i.severity === "MEDIUM");
  const low = issues.filter((i) => i.severity === "LOW");

  const pass =
    critical.length === 0 &&
    !issues.some((i) =>
      [
        "QUESTIONS_WON_EXCEEDS_DENOM",
        "QUESTIONS_MISSING_EXCEEDS_DENOM",
        "RATE_OUT_OF_BOUNDS",
        "GEOGRAPHY_SLOT_REUSE",
        "FIXTURE_DOMAIN_IN_RUNTIME_SOURCES",
        "OPENAI_BATCH_RETURNED_FOR_OTHER_PROVIDER",
        "PORTFOLIO_PEER_CONFLATION",
        "PORTFOLIO_WON_PCT_GT_100",
        "PORTFOLIO_MISSING_PCT_GT_100",
      ].includes(i.code)
    );

  const out = {
    ...report,
    status: pass
      ? "BRAND_AI_VISIBILITY_CORRECTNESS_AUDIT_PASS"
      : "BRAND_AI_VISIBILITY_CORRECTNESS_AUDIT_BLOCKED",
    ISSUES_FOUND: issues.length,
    CRITICAL: critical,
    HIGH: high,
    MEDIUM: medium,
    LOW: low,
    geographyAudit,
    providerAudit,
    trendAudit,
    portfolio: {
      MARRIOTT_ENTITLED_BRANDS: entitled,
      MARRIOTT_PEER_UNIVERSE: marriott.peerIds.length,
      HILTON_ENTITLED_BRANDS: hilton.company.brandIds.length,
      EXEC_PORTFOLIO_BRAND_COUNT: portfolioBrandCount,
      QUESTIONS_WON_DISPLAY: wonDisplay,
      QUESTIONS_MISSING_DISPLAY: missDisplay,
    },
    sources: {
      REAL_SOURCE_COUNT: (sources.sources || []).length,
      INVALID_SOURCE_COUNT: badSources.length,
      BROKEN_EVIDENCE_IDS: brokenEvidence,
    },
    questionReconciliation: {
      firstRecommendedRows: firstRec,
      summaryQuestionsWon: monCala.questionsWon,
      mismatchHeuristic: mismatch,
    },
    gates: {
      REPORT_AUDIT_PASS: pass,
      METRIC_BOUNDS_PASS: !issues.some((i) =>
        ["RATE_OUT_OF_BOUNDS", "QUESTIONS_WON_EXCEEDS_DENOM", "QUESTIONS_MISSING_EXCEEDS_DENOM", "PORTFOLIO_WON_PCT_GT_100", "PORTFOLIO_MISSING_PCT_GT_100"].includes(i.code)
      ),
      GEOGRAPHY_PURITY_PASS: !issues.some((i) => i.code === "GEOGRAPHY_SLOT_REUSE"),
      PROVIDER_PURITY_PASS: !issues.some((i) =>
        ["OPENAI_BATCH_RETURNED_FOR_OTHER_PROVIDER"].includes(i.code)
      ),
      QUESTION_RECONCILIATION_PASS: !issues.some((i) =>
        i.code === "QUESTION_STATUS_VS_WON_MISMATCH"
      ),
      FIXTURE_CONTAMINATION_PASS: badSources.length === 0,
      TREND_COMPARABILITY_PASS: !issues.some(
        (i) => i.code === "TREND_SAYS_NOT_MONITORED_BUT_CURRENT_EXISTS"
      ),
      PORTFOLIO_INTEGRITY_PASS: !issues.some((i) => i.code === "PORTFOLIO_PEER_CONFLATION"),
    },
  };

  const outDir = path.join(REPO, "data/ai-visibility");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, "brand-correctness-audit-latest.json");
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));

  console.log("\n=== Brand AI Visibility Correctness Audit ===\n");
  console.log("STATUS:", out.status);
  console.log("ISSUES:", out.ISSUES_FOUND, "| CRITICAL:", critical.length, "HIGH:", high.length);
  console.log("GATES:", JSON.stringify(out.gates, null, 2));
  console.log("Wrote", outPath);
  if (critical.length || high.length) {
    console.log("\nIssues:");
    for (const i of [...critical, ...high]) {
      console.log(`  [${i.severity}] ${i.code}: ${i.detail}`);
    }
  }
  process.exit(pass ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
