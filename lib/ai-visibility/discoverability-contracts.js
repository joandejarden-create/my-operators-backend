/**
 * Data connections, capability matrix, read contracts (Phase 3C.1).
 */

import { DATA_STATE, CONNECTION_STATE, CONNECTION_REQUIRED_COPY } from "./discoverability-data-states.js";

export const DISCOVERABILITY_CONTRACTS_VERSION = "ai_visibility_discoverability_contracts_v1";

export const CONNECTION_TYPES = Object.freeze({
  ANALYTICS_CONNECTION: "ANALYTICS_CONNECTION",
  SERVER_CDN_LOG_CONNECTION: "SERVER_CDN_LOG_CONNECTION",
  SEARCH_CONSOLE_CONNECTION: "SEARCH_CONSOLE_CONNECTION",
  CRM_CONNECTION: "CRM_CONNECTION",
  MANUAL_EXPORT: "MANUAL_EXPORT",
});

export const DATA_FRESHNESS_CADENCE = Object.freeze({
  publicCrawlReadiness: "weekly_or_biweekly",
  crawlerLogs: "daily_or_period_aggregate",
  referral: "daily_or_weekly",
  businessImpact: "daily_or_weekly",
});

export const CAPABILITY_MATRIX = Object.freeze([
  { metric: "AI Presence", PUBLIC_NOW: false, ANALYTICS_REQUIRED: false, LOG_REQUIRED: false, CRM_REQUIRED: false, NOT_SUPPORTED: false, dataState: "MEASURED" },
  { metric: "Crawl Readiness", PUBLIC_NOW: true, ANALYTICS_REQUIRED: false, LOG_REQUIRED: false, CRM_REQUIRED: false, NOT_SUPPORTED: false, dataState: "MEASURABLE_PUBLICLY" },
  { metric: "robots access", PUBLIC_NOW: true, ANALYTICS_REQUIRED: false, LOG_REQUIRED: false, CRM_REQUIRED: false, NOT_SUPPORTED: false, dataState: "MEASURABLE_PUBLICLY" },
  { metric: "sitemap", PUBLIC_NOW: true, ANALYTICS_REQUIRED: false, LOG_REQUIRED: false, CRM_REQUIRED: false, NOT_SUPPORTED: false, dataState: "MEASURABLE_PUBLICLY" },
  { metric: "page indexability", PUBLIC_NOW: true, ANALYTICS_REQUIRED: false, LOG_REQUIRED: false, CRM_REQUIRED: false, NOT_SUPPORTED: false, dataState: "MEASURABLE_PUBLICLY" },
  { metric: "actual AI crawler activity", PUBLIC_NOW: false, ANALYTICS_REQUIRED: false, LOG_REQUIRED: true, CRM_REQUIRED: false, NOT_SUPPORTED: false, dataState: "CONNECTION_REQUIRED" },
  { metric: "AI referral sessions", PUBLIC_NOW: false, ANALYTICS_REQUIRED: true, LOG_REQUIRED: false, CRM_REQUIRED: false, NOT_SUPPORTED: false, dataState: "CONNECTION_REQUIRED" },
  { metric: "development page AI visits", PUBLIC_NOW: false, ANALYTICS_REQUIRED: true, LOG_REQUIRED: false, CRM_REQUIRED: false, NOT_SUPPORTED: false, dataState: "CONNECTION_REQUIRED" },
  { metric: "qualified development actions", PUBLIC_NOW: false, ANALYTICS_REQUIRED: true, LOG_REQUIRED: false, CRM_REQUIRED: true, NOT_SUPPORTED: false, dataState: "CONNECTION_REQUIRED" },
  { metric: "AI-assisted leads", PUBLIC_NOW: false, ANALYTICS_REQUIRED: true, LOG_REQUIRED: false, CRM_REQUIRED: true, NOT_SUPPORTED: false, dataState: "CONNECTION_REQUIRED", note: "Requires explicit attribution model" },
]);

export const DISCOVERABILITY_READ_CONTRACT = Object.freeze({
  brandId: "string",
  domain: "string|null",
  priorityPages: "PriorityPage[]",
  crawlReadiness: "CrawlReadinessSummary",
  crawlerAccess: "Record<provider, CrawlerAccessStatus>",
  crawlerActivity: "CrawlerActivitySummary|null",
  lastCheckedAt: "ISO8601|null",
  dataState: "DATA_STATE",
  evidence: "TechnicalEvidence[]",
  providerNeutral: true,
  RULE: "Separate general page readiness from provider-specific crawler access/activity",
});

export const REFERRAL_READ_CONTRACT = Object.freeze({
  brandId: "string",
  period: "string|null",
  referrerClassification: "ReferrerClassification[]",
  sessions: "number|null",
  engagedSessions: "number|null",
  developmentPageVisits: "number|null",
  actions: "number|null",
  dataSource: "string|null",
  connectionStatus: "CONNECTION_STATE",
  RULE: "No fake zeros when not connected",
});

export const BUSINESS_IMPACT_READ_CONTRACT = Object.freeze({
  brandId: "string",
  period: "string|null",
  provider: "string|null",
  qualifiedActions: "number|null",
  directReferralActions: "number|null",
  conversionRate: "number|null",
  eventDefinitionVersion: "string|null",
  dataSource: "string|null",
  connectionStatus: "CONNECTION_STATE",
  RULE: "Only display when event definitions exist",
});

export const STORAGE_MAPPING = Object.freeze({
  RAILWAY_POSTGRES: [
    "discoverability_snapshots",
    "page_checks",
    "connection_status",
    "referral_aggregates",
    "business_impact_aggregates",
    "event_definition_versions",
  ],
  OBJECT_STORAGE: ["raw_technical_fetch_artifacts", "raw_log_extracts"],
  AIRTABLE_CONFIG_ROLE: [
    "priority_page_definitions",
    "connection_status",
    "event_definitions",
    "manual_review_items",
  ],
  AIRTABLE_NOT_FOR: ["high_volume_analytics", "log_data", "session_level_data"],
});

export const BRAND_PORTFOLIO_HIERARCHY = Object.freeze({
  SUBJECT: "Brand",
  AGGREGATION: "Company portfolio only when Brands are entitled",
  RULE: "Do not conflate Brand website traffic with company-level metrics without mapping",
});

export const PRIVACY_MODEL = Object.freeze({
  READY: true,
  DEFAULT: "aggregated_analytics_preferred",
  DO_NOT_STORE: ["ip_address", "full_user_identity", "unnecessary_personal_data"],
  RULE: "Referral/session data may contain user-level info — prefer aggregates unless governance allows",
});

/**
 * Build empty discoverability read payload with correct states.
 */
export function buildDiscoverabilityReadContract(brandId, opts = {}) {
  return {
    version: DISCOVERABILITY_CONTRACTS_VERSION,
    brandId,
    domain: opts.domain || null,
    priorityPages: opts.priorityPages || [],
    crawlReadiness: opts.crawlReadiness || { dataState: DATA_STATE.MEASURABLE_PUBLICLY },
    crawlerAccess: opts.crawlerAccess || {},
    crawlerActivity: opts.crawlerActivity || null,
    lastCheckedAt: opts.lastCheckedAt || null,
    dataState: opts.dataState || DATA_STATE.MEASURABLE_PUBLICLY,
    evidence: opts.evidence || [],
    referral: {
      connectionStatus: CONNECTION_STATE.CONNECTION_REQUIRED,
      display: CONNECTION_REQUIRED_COPY.ANALYTICS,
      sessions: null,
    },
    businessImpact: {
      connectionStatus: CONNECTION_STATE.CONNECTION_REQUIRED,
      display: CONNECTION_REQUIRED_COPY.ANALYTICS,
      qualifiedActions: null,
    },
  };
}
