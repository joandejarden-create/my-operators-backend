/**
 * Discoverability read service — UI/API payloads (Phase 3C.1).
 * Provider-neutral. No fake values.
 */

import { DATA_STATE, CONNECTION_STATE, CONNECTION_REQUIRED_COPY, resolveDataStateDisplay } from "./discoverability-data-states.js";
import { COMPOSITE_SCORE, PRODUCT_DEFINITIONS, VISIBILITY_VS_DISCOVERABILITY_GUARD } from "./discoverability-taxonomy.js";
import { DISCOVERABILITY_METRICS_V1, PRIORITY_DEVELOPMENT_PAGE_TYPES, PRIORITY_PAGE_STATUS } from "./discoverability-dimensions.js";
import { PROVIDER_CRAWLER_READINESS_MATRIX } from "./ai-crawler-registry.js";
import { REFERRAL_METRICS_V1, PROVIDER_REFERRAL_CAPABILITY } from "./referral-intelligence.js";
import { BUSINESS_IMPACT_METRICS_V1, READERSHIP_ENRICHMENT } from "./business-impact.js";
import { CAPABILITY_MATRIX, buildDiscoverabilityReadContract } from "./discoverability-contracts.js";
import { LIVE_ANALYTICS_CONNECTION, LIVE_LOG_CONNECTION } from "./discoverability-adapters.js";
import { runPublicChecksFromFixtures } from "./public-check-engine.js";

export const DISCOVERABILITY_READ_SERVICE_VERSION = "ai_visibility_discoverability_read_v1";

function metricRow(id, label, dataState, opts = {}) {
  const display = resolveDataStateDisplay({
    dataState,
    value: opts.value,
    connectionCopy: opts.connectionCopy,
  });
  return { id, label, ...display };
}

/**
 * Executive Summary compact block — Discoverability & Business Impact.
 */
export function buildDiscoverabilityExecutiveBlock(opts = {}) {
  const publicChecks = opts.publicChecks || null;
  const priorityPagesMonitored = opts.priorityPages?.length || 0;

  let crawlReadinessDisplay = "Public crawl baseline not yet run";
  let crawlerAccessDisplay = "Public crawl baseline not yet run";
  let developmentContentDisplay = "Public crawl baseline not yet run";

  if (publicChecks) {
    crawlReadinessDisplay =
      publicChecks.indexability === "technically_indexable" ? "Technically indexable" : publicChecks.indexability || "Checked";
    crawlerAccessDisplay = publicChecks.robots?.oaiSearchBot?.status?.replace(/_/g, " ") || "Checked";
    developmentContentDisplay = publicChecks.contentInInitialHtml || "Checked";
  } else if (!opts.hasGovernedUrl) {
    crawlReadinessDisplay = CONNECTION_REQUIRED_COPY.URL_INVENTORY;
    crawlerAccessDisplay = CONNECTION_REQUIRED_COPY.URL_INVENTORY;
    developmentContentDisplay = CONNECTION_REQUIRED_COPY.URL_INVENTORY;
  }

  return {
    version: DISCOVERABILITY_READ_SERVICE_VERSION,
    title: "Discoverability & Business Impact",
    COMPOSITE_SCORE: COMPOSITE_SCORE.ALLOWED,
    discoverability: {
      title: "Discoverability",
      priorityDevelopmentPages: metricRow(
        "priority_pages_monitored",
        "Priority development pages",
        priorityPagesMonitored > 0 ? DATA_STATE.MEASURED : DATA_STATE.MEASURABLE_PUBLICLY,
        { value: priorityPagesMonitored > 0 ? priorityPagesMonitored : null, connectionCopy: "Priority Page URL Required" }
      ),
      crawlerAccess: metricRow("ai_crawler_access", "Crawler access", publicChecks ? DATA_STATE.MEASURED : DATA_STATE.MEASURABLE_PUBLICLY, {
        value: null,
        connectionCopy: crawlReadinessDisplay,
      }),
      developmentContentCrawlable: metricRow(
        "pages_with_crawlable_development_content",
        "Development content crawlable",
        publicChecks ? DATA_STATE.MEASURED : DATA_STATE.MEASURABLE_PUBLICLY,
        { value: null, connectionCopy: developmentContentDisplay }
      ),
      display: {
        priorityDevelopmentPages: priorityPagesMonitored > 0 ? String(priorityPagesMonitored) : crawlReadinessDisplay,
        crawlerAccess: crawlerAccessDisplay,
        developmentContentCrawlable: developmentContentDisplay,
      },
    },
    referral: {
      title: "Referral",
      aiReferralSessions: metricRow("ai_referral_sessions", "AI referral sessions", DATA_STATE.CONNECTION_REQUIRED, {
        connectionCopy: CONNECTION_REQUIRED_COPY.ANALYTICS,
      }),
      display: { aiReferralSessions: CONNECTION_REQUIRED_COPY.ANALYTICS },
      connectionStatus: CONNECTION_STATE.CONNECTION_REQUIRED,
    },
    businessImpact: {
      title: "Business Impact",
      qualifiedDevelopmentActions: metricRow(
        "qualified_development_actions_from_ai",
        "Qualified development actions",
        DATA_STATE.CONNECTION_REQUIRED,
        { connectionCopy: CONNECTION_REQUIRED_COPY.ANALYTICS }
      ),
      display: { qualifiedDevelopmentActions: CONNECTION_REQUIRED_COPY.ANALYTICS },
      connectionStatus: CONNECTION_STATE.CONNECTION_REQUIRED,
    },
    LIVE_ANALYTICS_CONNECTION,
    LIVE_LOG_CONNECTION,
    SYNTHETIC_VALUES: false,
  };
}

/**
 * Detailed View — Discoverability / Referral / Business Impact sections.
 */
export function buildDiscoverabilityDetailBlock(opts = {}) {
  const publicChecks = opts.publicChecks || null;

  return {
    version: DISCOVERABILITY_READ_SERVICE_VERSION,
    title: "Discoverability · Referral · Business Impact",
    providerNeutral: true,
    COMPOSITE_SCORE: COMPOSITE_SCORE.ALLOWED,
    modules: {
      crawlReadiness: {
        label: "Crawl Readiness",
        dataState: publicChecks ? DATA_STATE.MEASURED : DATA_STATE.MEASURABLE_PUBLICLY,
        metrics: DISCOVERABILITY_METRICS_V1.map((m) =>
          metricRow(m.id, m.label, publicChecks ? DATA_STATE.MEASURED : DATA_STATE.MEASURABLE_PUBLICLY)
        ),
        checks: publicChecks || null,
      },
      priorityDevelopmentPages: {
        label: "Priority Development Pages",
        pageTypes: PRIORITY_DEVELOPMENT_PAGE_TYPES,
        statusValues: PRIORITY_PAGE_STATUS,
        pages: opts.priorityPages || [],
      },
      aiCrawlerAccess: {
        label: "AI Crawler Access",
        providerMatrix: PROVIDER_CRAWLER_READINESS_MATRIX,
        note: "robots permission != actual crawl",
        oaiSearchBot: publicChecks?.robots?.oaiSearchBot || null,
      },
      crawlerActivity: {
        label: "Crawler Activity",
        dataState: DATA_STATE.CONNECTION_REQUIRED,
        display: CONNECTION_REQUIRED_COPY.SERVER_CDN_LOG,
        connectionStatus: CONNECTION_STATE.CONNECTION_REQUIRED,
      },
      technicalEvidence: {
        label: "Technical Evidence",
        items: publicChecks ? [{ type: "public_check", summary: publicChecks.indexability }] : [],
      },
      referral: {
        label: "Referral",
        metrics: REFERRAL_METRICS_V1.map((m) =>
          metricRow(m.id, m.label, DATA_STATE.CONNECTION_REQUIRED, {
            connectionCopy: CONNECTION_REQUIRED_COPY.ANALYTICS,
          })
        ),
        providerReferralCapability: PROVIDER_REFERRAL_CAPABILITY,
        connectionStatus: CONNECTION_STATE.CONNECTION_REQUIRED,
      },
      businessImpact: {
        label: "Business Impact",
        metrics: BUSINESS_IMPACT_METRICS_V1.map((m) =>
          metricRow(m.id, m.label, DATA_STATE.CONNECTION_REQUIRED, {
            connectionCopy: CONNECTION_REQUIRED_COPY.ANALYTICS,
          })
        ),
        connectionStatus: CONNECTION_STATE.CONNECTION_REQUIRED,
        eventConfigurationRequired: CONNECTION_REQUIRED_COPY.EVENT_CONFIG,
      },
    },
    capabilityMatrix: CAPABILITY_MATRIX,
    taxonomy: PRODUCT_DEFINITIONS,
    guard: VISIBILITY_VS_DISCOVERABILITY_GUARD,
    readership: READERSHIP_ENRICHMENT,
    contract: buildDiscoverabilityReadContract(opts.brandId || null, opts),
    SYNTHETIC_VALUES: false,
  };
}

/**
 * Build from fixture-based public check (for pilot/dry-run).
 */
export function buildDiscoverabilityFromFixtureCheck(brandRow, fixtureInput) {
  const publicChecks = runPublicChecksFromFixtures(fixtureInput);
  return {
    executive: buildDiscoverabilityExecutiveBlock({
      publicChecks,
      priorityPages: [{ url: fixtureInput.pageUrl, status: "PRESENT" }],
      hasGovernedUrl: true,
    }),
    detail: buildDiscoverabilityDetailBlock({
      brandId: brandRow.brandId,
      publicChecks,
      priorityPages: [{ url: fixtureInput.pageUrl, pageType: "brand_development_page", status: "PRESENT" }],
    }),
  };
}
