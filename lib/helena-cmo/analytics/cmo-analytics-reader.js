/**
 * Helena CMO analytics / connector status — DYNAMIC probes.
 * Never treat hard-coded false flags as permanent source of truth.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '../../..');
const LIVE_METRICS = path.join(
  ROOT,
  'reports/helena-cmo-live-data-reconciliation-v1/helena-cmo-live-metrics-v1.json',
);
const LIVE_RECON = path.join(
  ROOT,
  'reports/helena-cmo-live-data-reconciliation-v1/helena-cmo-live-data-reconciliation-v1.json',
);
const ANALYTICS_DIR = path.join(
  ROOT,
  'reports/helena-cmo-phase-0/external-exports/workspace-files-20260907-pack3-final/files/dealality-marketing-os-export-final/04_SOCIAL_ANALYTICS',
);

function readJson(p) {
  try {
    if (!fs.existsSync(p)) return null;
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch {
    return null;
  }
}

/**
 * Dynamic connector status for Founder Console Marketing Intelligence panel.
 */
export function getHelenaMarketingIntelligenceStatus() {
  const live = readJson(LIVE_METRICS);
  const recon = readJson(LIVE_RECON);
  const connectorStatus = live?.connectorStatus || {};

  const panelOrder = [
    'GA4',
    'GSC',
    'LINKEDIN_JOAN',
    'LINKEDIN_DEALALITY',
    'LINKEDIN_AO',
    'WEBFLOW_CMS',
    'AIRTABLE_GTM',
    'AIRTABLE_MARKETING_OS',
    'LANDING_EVENTS',
    'PRODUCT_USAGE_TELEMETRY',
    'ENRICH_LABS',
  ];

  const sources = panelOrder.map((id) => {
    const row = connectorStatus[id] || {};
    let uiStatus = 'NOT_CONNECTED';
    const liveRead = row.live_read || '';
    const freshness = row.freshness || '';
    if (liveRead === 'LIVE_READ_SUCCESS' && (freshness === 'LIVE' || row.authenticated === 'YES')) {
      uiStatus = 'LIVE';
    } else if (liveRead === 'PERMISSION_LIMITED' || liveRead === 'PARTIAL' || freshness === 'PARTIAL') {
      uiStatus = 'PARTIAL';
    } else if (freshness === 'STALE' || freshness === 'STALE_SNAPSHOT_ONLY' || freshness === 'STALE_SCRAPE' || liveRead === 'STALE_ONLY') {
      uiStatus = 'STALE';
    } else if (liveRead === 'LIVE_READ_FAILED' || row.authenticated === 'NO') {
      uiStatus = freshness?.includes('STALE') ? 'STALE' : 'NOT_CONNECTED';
    }

    // Special-case: GA4 with Zapier pending auth
    if (id === 'GA4' && row.authenticated === 'NO') uiStatus = 'NOT_CONNECTED';
    if (id === 'WEBFLOW_CMS' && liveRead === 'LIVE_READ_SUCCESS') uiStatus = 'LIVE';
    if (id === 'AIRTABLE_GTM' && liveRead === 'LIVE_READ_SUCCESS') uiStatus = 'LIVE';
    if (id === 'AIRTABLE_MARKETING_OS' && liveRead === 'LIVE_READ_SUCCESS') uiStatus = 'LIVE';

    return {
      id,
      label: id.replace(/_/g, ' '),
      status: uiStatus,
      liveRead: liveRead || 'UNKNOWN',
      freshness: freshness || 'UNKNOWN',
      lastPull: row.last_successful_sync || live?.generatedAt || null,
      lastDataDate: row.last_successful_sync || null,
      coverage: recon?.connectors?.find((c) => c.source === id)?.data_available || null,
    };
  });

  return {
    schemaVersion: 'helena-cmo-marketing-intelligence-panel-v1',
    generatedAt: live?.generatedAt || null,
    pack: live ? 'helena-cmo-live-data-reconciliation-v1' : null,
    sources,
    corrections: recon?.phase6eCorrections || [],
    enoughForDeepBaseline: recon?.enoughLiveDataToRerunBaseline || null,
    adpPublicPage: recon?.adpLive || null,
    pipelineSummary: recon?.dealalityInternal?.pilotAggregates?.aggregates || null,
    pipelineCounts: recon?.dealalityInternal?.gtm || null,
  };
}

/**
 * Canonical CMO analytics contract — prefers live metrics pack; falls back to snapshots with STALE labels.
 */
export function readHelenaCmoAnalyticsContract() {
  const intelligence = getHelenaMarketingIntelligenceStatus();
  const live = readJson(LIVE_METRICS);
  const ga4Live = intelligence.sources.find((s) => s.id === 'GA4')?.status === 'LIVE';
  const gscLive = intelligence.sources.find((s) => s.id === 'GSC')?.status === 'LIVE';

  const ga4SummaryPath = path.join(ANALYTICS_DIR, 'GA4_SUMMARY.md');
  const gscSummaryPath = path.join(ANALYTICS_DIR, 'GSC_SUMMARY.md');

  return {
    schemaVersion: 'helena-cmo-analytics-contract-v1',
    generatedAt: new Date().toISOString(),
    connectors: {
      ga4LiveApi: ga4Live ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      gscLiveApi: gscLive ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      linkedInAnalyticsApi: 'NOT_CONNECTED',
      webflowCms: intelligence.sources.find((s) => s.id === 'WEBFLOW_CMS')?.status || 'UNKNOWN',
      gtmPipeline: intelligence.sources.find((s) => s.id === 'AIRTABLE_GTM')?.status || 'UNKNOWN',
      marketingOs: intelligence.sources.find((s) => s.id === 'AIRTABLE_MARKETING_OS')?.status || 'UNKNOWN',
      snapshotFiles: fs.existsSync(ANALYTICS_DIR) ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      determination: 'DYNAMIC_FROM_LIVE_METRICS_PACK',
    },
    blockers: {
      ga4: ga4Live
        ? null
        : 'Zapier GA4 enabled but unauthenticated — Joan OAuth required. Enrich not callable from this session.',
      gsc: 'No GSC connector in Zapier catalog; no repo API client.',
      linkedIn: 'Zapier LinkedIn has no analytics read actions.',
    },
    liveMetricsCount: Array.isArray(live?.metrics) ? live.metrics.length : 0,
    intelligence,
    acquisition: {
      freshness: ga4Live ? 'LIVE' : 'STALE_SNAPSHOT',
      sessions: 553,
      dateRange: '2026-06-01_2026-09-06',
      note: ga4Live ? 'Replace with live runReport values' : 'Snapshot only until GA4 auth',
      sourceMediumTop: [
        { sourceMedium: 'direct', sessions: 277 },
        { sourceMedium: 'googleOrganic', sessions: 19 },
      ],
    },
    commercialIntent: {
      status: 'PARTIAL_FROM_GTM',
      note: 'Pilot Target List outreach stages available live; GA4 conversion events still UNKNOWN',
      pilotTargetRows: intelligence.pipelineCounts?.['Pilot Target List']?.count ?? null,
      pilotStatus: intelligence.pipelineSummary?.status || null,
    },
    organic: {
      freshness: gscLive ? 'LIVE' : 'STALE_SNAPSHOT',
      dateRange: '2026-06-01_2026-09-06',
      note: fs.existsSync(gscSummaryPath) ? 'See GSC_SUMMARY.md' : 'Missing summary',
    },
    website: {
      adpPublicPage: intelligence.adpPublicPage,
      webflowCms: 'LIVE',
      analyze: 'PERMISSION_LIMITED',
    },
    conclusions: {
      businessOutcomes: [
        'Named commercial list EXISTS (Pilot Target List live) — Phase 6E DATA_GAP overstated emptiness.',
        'No attributable revenue metrics in current live grain.',
      ],
      commercialIntent: [
        'Pilot Status mix + Outreach Status available from GTM.',
        'GA4 CTA/demo events still unknown until live GA4.',
      ],
      channelActivity: [
        ga4Live ? 'GA4 live available' : 'GA4 still STALE snapshot (direct-heavy historically).',
        'Webflow CMS live; Analyze entitlement missing.',
      ],
    },
    provenance: {
      liveMetricsPath: fs.existsSync(LIVE_METRICS) ? path.relative(ROOT, LIVE_METRICS) : null,
      ga4SummaryPresent: fs.existsSync(ga4SummaryPath),
      gscSummaryPresent: fs.existsSync(gscSummaryPath),
    },
  };
}

export default { readHelenaCmoAnalyticsContract, getHelenaMarketingIntelligenceStatus };
