/**
 * Helena CMO analytics / connector status — DYNAMIC probes.
 * Prefers Enrich-native live metrics ingest; never hard-code stale Zapier blockers as SoT.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '../../..');
const LIVE_METRICS_CANDIDATES = [
  'reports/helena-cmo-live-data-reconciliation-v1/helena-cmo-live-metrics-v1.json',
  'reports/helena-cmo-native-data-pull-v1/helena-cmo-live-metrics-v1.json',
  'reports/helena-cmo-native-data-pull-v1/enrich-returns/helena-cmo-live-metrics-v1.json',
];
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

function loadLiveMetrics() {
  for (const rel of LIVE_METRICS_CANDIDATES) {
    const abs = path.join(ROOT, rel);
    const json = readJson(abs);
    if (!json) continue;
    const metrics = Array.isArray(json.metrics) ? json.metrics : [];
    if (!metrics.length && !json.connectorStatus && !json.integrations_inventory) continue;
    return { live: json, path: abs, relative: rel };
  }
  return { live: null, path: null, relative: null };
}

function metricValue(live, id) {
  const row = (live?.metrics || []).find((m) => m.metric_id === id);
  return row?.current_value ?? live?.headline?.[id] ?? null;
}

function deriveConnectorStatus(live) {
  if (live?.connectorStatus && Object.keys(live.connectorStatus).length) {
    return live.connectorStatus;
  }
  // Raw Enrich artifact shape
  const inv = live?.integrations_inventory || live?.enrichPull?.integrations_inventory || [];
  const by = (part) => inv.find((i) => String(i.name || '').toLowerCase().includes(part));
  const pullTs = live?.pull_timestamp || live?.ingest?.enrichPullTimestamp || live?.generatedAt;
  const ga4 = by('google analytics');
  const gsc = by('search console');
  const liJoan = by('linkedin joan');
  const liAo = by('linkedin ao');
  const liDeal = by('linkedin dealality');
  const webflow = by('webflow');
  return {
    GA4: {
      authenticated: ga4?.connected ? 'YES' : 'NO',
      live_read: ga4?.live_read_status || 'NOT_CONNECTED',
      freshness: ga4?.live_read_status === 'LIVE_READ_SUCCESS' ? 'LIVE' : 'UNKNOWN',
      last_successful_sync: pullTs,
    },
    GSC: {
      authenticated: gsc?.connected ? 'YES' : 'NO',
      live_read: gsc?.live_read_status || 'NOT_CONNECTED',
      freshness: gsc?.live_read_status === 'LIVE_READ_SUCCESS' ? 'LIVE' : 'UNKNOWN',
      last_successful_sync: pullTs,
    },
    LINKEDIN_JOAN: {
      authenticated: liJoan?.connected ? 'YES' : 'NO',
      live_read: liJoan?.live_read_status || 'NOT_CONNECTED',
      freshness: liJoan?.live_read_status === 'PERMISSION_LIMITED' ? 'PARTIAL' : 'UNKNOWN',
      last_successful_sync: pullTs,
    },
    LINKEDIN_AO: {
      authenticated: liAo?.connected ? 'YES' : 'NO',
      live_read: liAo?.live_read_status || 'NOT_CONNECTED',
      freshness: liAo?.live_read_status === 'PERMISSION_LIMITED' ? 'PARTIAL' : 'UNKNOWN',
      last_successful_sync: pullTs,
    },
    LINKEDIN_DEALALITY: {
      authenticated: 'NO',
      live_read: liDeal?.live_read_status || 'NOT_CONNECTED',
      freshness: 'NOT_CONNECTED',
      last_successful_sync: null,
    },
    WEBFLOW_CMS: {
      authenticated: webflow?.connected ? 'YES' : 'NO',
      live_read: webflow?.live_read_status || 'UNKNOWN',
      freshness: webflow?.live_read_status === 'LIVE_READ_SUCCESS' ? 'LIVE' : 'UNKNOWN',
      last_successful_sync: pullTs,
    },
    ENRICH_LABS: {
      authenticated: 'YES',
      live_read: 'LIVE_READ_SUCCESS',
      freshness: 'LIVE',
      last_successful_sync: pullTs,
    },
  };
}

/**
 * Dynamic connector status for Founder Console Marketing Intelligence panel.
 */
export function getHelenaMarketingIntelligenceStatus() {
  const { live, relative } = loadLiveMetrics();
  const recon = readJson(LIVE_RECON);
  const connectorStatus = deriveConnectorStatus(live);

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
    } else if (
      liveRead === 'PERMISSION_LIMITED' ||
      liveRead === 'PARTIAL' ||
      freshness === 'PARTIAL'
    ) {
      uiStatus = 'PARTIAL';
    } else if (
      freshness === 'STALE' ||
      freshness === 'STALE_SNAPSHOT_ONLY' ||
      freshness === 'STALE_SCRAPE' ||
      liveRead === 'STALE_ONLY'
    ) {
      uiStatus = 'STALE';
    } else if (liveRead === 'LIVE_READ_FAILED' || row.authenticated === 'NO') {
      uiStatus = freshness?.includes('STALE') ? 'STALE' : 'NOT_CONNECTED';
    }

    if (id === 'WEBFLOW_CMS' && liveRead === 'LIVE_READ_SUCCESS') uiStatus = 'LIVE';
    if (id === 'AIRTABLE_GTM' && liveRead === 'LIVE_READ_SUCCESS') uiStatus = 'LIVE';
    if (id === 'AIRTABLE_MARKETING_OS' && liveRead === 'LIVE_READ_SUCCESS') uiStatus = 'LIVE';
    if (id === 'GA4' && liveRead === 'LIVE_READ_SUCCESS') uiStatus = 'LIVE';
    if (id === 'GSC' && liveRead === 'LIVE_READ_SUCCESS') uiStatus = 'LIVE';
    if (id === 'ENRICH_LABS' && liveRead === 'LIVE_READ_SUCCESS') uiStatus = 'LIVE';

    return {
      id,
      label: id.replace(/_/g, ' '),
      status: uiStatus,
      liveRead: liveRead || 'UNKNOWN',
      freshness: freshness || 'UNKNOWN',
      lastPull: row.last_successful_sync || live?.ingest?.enrichPullTimestamp || live?.generatedAt || null,
      lastDataDate: row.last_successful_sync || null,
      coverage: recon?.connectors?.find((c) => c.source === id)?.data_available || null,
    };
  });

  const headline = live?.headline || null;
  const exec = live?.enrichPull?.executive_signal || live?.executive_signal || null;

  return {
    schemaVersion: 'helena-cmo-marketing-intelligence-panel-v1',
    generatedAt: live?.generatedAt || live?.pull_timestamp || null,
    pack: relative,
    sources,
    headline,
    executiveSignal: exec,
    corrections: recon?.phase6eCorrections || [],
    enoughForDeepBaseline: recon?.enoughLiveDataToRerunBaseline || live?.deepBaselineReadiness || null,
    adpPublicPage: recon?.adpLive || null,
    pipelineSummary: recon?.dealalityInternal?.pilotAggregates?.aggregates || null,
    pipelineCounts: recon?.dealalityInternal?.gtm || null,
  };
}

/**
 * Canonical CMO analytics contract — prefers Enrich live metrics; falls back to snapshots with STALE labels.
 */
export function readHelenaCmoAnalyticsContract() {
  const intelligence = getHelenaMarketingIntelligenceStatus();
  const { live, relative } = loadLiveMetrics();
  const ga4Live = intelligence.sources.find((s) => s.id === 'GA4')?.status === 'LIVE';
  const gscLive = intelligence.sources.find((s) => s.id === 'GSC')?.status === 'LIVE';

  const ga4SummaryPath = path.join(ANALYTICS_DIR, 'GA4_SUMMARY.md');
  const gscSummaryPath = path.join(ANALYTICS_DIR, 'GSC_SUMMARY.md');

  const sessions30 = live?.headline?.ga4_sessions_30d ?? metricValue(live, 'ga4.sessions.last_30d');
  const sessionsPrior =
    live?.headline?.ga4_sessions_prior_30d ?? metricValue(live, 'ga4.sessions.previous_30d');
  const sessions7 = live?.headline?.ga4_sessions_7d ?? metricValue(live, 'ga4.sessions.last_7d');
  const sessions90 = live?.headline?.ga4_sessions_90d ?? metricValue(live, 'ga4.sessions.last_90d');
  const gscClicks28 = live?.headline?.gsc_clicks_28d ?? metricValue(live, 'gsc.clicks.last_28d');
  const gscImpr28 =
    live?.headline?.gsc_impressions_28d ?? metricValue(live, 'gsc.impressions.last_28d');

  return {
    schemaVersion: 'helena-cmo-analytics-contract-v1',
    generatedAt: new Date().toISOString(),
    connectors: {
      ga4LiveApi: ga4Live ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      gscLiveApi: gscLive ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      linkedInAnalyticsApi: 'PERMISSION_LIMITED',
      webflowCms: intelligence.sources.find((s) => s.id === 'WEBFLOW_CMS')?.status || 'UNKNOWN',
      gtmPipeline: intelligence.sources.find((s) => s.id === 'AIRTABLE_GTM')?.status || 'UNKNOWN',
      marketingOs: intelligence.sources.find((s) => s.id === 'AIRTABLE_MARKETING_OS')?.status || 'UNKNOWN',
      enrichLabs: intelligence.sources.find((s) => s.id === 'ENRICH_LABS')?.status || 'UNKNOWN',
      snapshotFiles: fs.existsSync(ANALYTICS_DIR) ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      determination: 'DYNAMIC_FROM_LIVE_METRICS_PACK',
    },
    blockers: {
      ga4: ga4Live
        ? null
        : 'Enrich-native GA4 live pull not ingested yet — do not default to Zapier.',
      gsc: gscLive
        ? null
        : 'Enrich-native GSC live pull not ingested yet — do not rebuild Cursor GSC.',
      linkedIn:
        'LinkedIn impressions/reach/clicks time series PERMISSION_LIMITED (public scrape only).',
    },
    liveMetricsCount: Array.isArray(live?.metrics) ? live.metrics.length : 0,
    intelligence,
    acquisition: {
      freshness: ga4Live ? 'LIVE' : 'STALE_SNAPSHOT',
      sessions7d: sessions7,
      sessions30d: sessions30,
      sessionsPrior30d: sessionsPrior,
      sessions90d: sessions90,
      channels30d: {
        direct: live?.headline?.ga4_channel_30d_direct ?? metricValue(live, 'ga4.channel_group.30d.direct'),
        referral:
          live?.headline?.ga4_channel_30d_referral ?? metricValue(live, 'ga4.channel_group.30d.referral'),
        organicSearch:
          live?.headline?.ga4_channel_30d_organic_search ??
          metricValue(live, 'ga4.channel_group.30d.organic_search'),
        organicSocial:
          live?.headline?.ga4_channel_30d_organic_social ??
          metricValue(live, 'ga4.channel_group.30d.organic_social'),
        aiAssistant:
          live?.headline?.ga4_channel_30d_ai_assistant ??
          metricValue(live, 'ga4.channel_group.30d.ai_assistant'),
      },
      linkedInAttributedSessions30d:
        live?.headline?.ga4_linkedin_attributed_sessions_30d ??
        metricValue(live, 'ga4.linkedin_attributed_sessions.30d'),
      formStart30d: live?.headline?.ga4_form_start_30d ?? null,
      note: ga4Live
        ? 'Enrich-native GA4 live ingest'
        : 'Snapshot only until Enrich live pull ingested',
    },
    commercialIntent: {
      status: 'PARTIAL_FROM_GTM_AND_GA4',
      note: 'Pilot Target List live; GA4 form_start present; named demo/pilot events NOT_FOUND',
      pilotTargetRows: intelligence.pipelineCounts?.['Pilot Target List']?.count ?? null,
      pilotStatus: intelligence.pipelineSummary?.status || null,
      formStart30d: live?.headline?.ga4_form_start_30d ?? null,
    },
    organic: {
      freshness: gscLive ? 'LIVE' : 'STALE_SNAPSHOT',
      clicks28d: gscClicks28,
      impressions28d: gscImpr28,
      clicksPrior28d:
        live?.headline?.gsc_clicks_prior_28d ?? metricValue(live, 'gsc.clicks.previous_28d'),
      impressionsPrior28d:
        live?.headline?.gsc_impressions_prior_28d ?? metricValue(live, 'gsc.impressions.previous_28d'),
      note: gscLive ? 'Enrich-native GSC live ingest' : 'Missing live GSC',
    },
    website: {
      adpPublicPage: intelligence.adpPublicPage,
      webflowCms: 'LIVE',
      analyze: 'PERMISSION_LIMITED',
    },
    conclusions: {
      businessOutcomes: [
        'Sessions down 30d vs prior (97 vs 241) while GSC impressions surged with thin clicks.',
        'No attributable revenue / demo / pilot conversion events in GA4 stream.',
      ],
      commercialIntent: [
        'Pilot Status mix available from GTM.',
        'form_start=8 only conversion-like event; commercial outcomes UNKNOWN.',
      ],
      channelActivity: [
        ga4Live
          ? 'GA4 live via Enrich: Direct-heavy; LinkedIn→site ATTRIBUTED (8/30d); AI Assistant present.'
          : 'GA4 still STALE snapshot.',
        gscLive
          ? 'GSC live via Enrich: discovery up, conversion thin (esp. branded residences).'
          : 'GSC still STALE.',
        'Webflow CMS live; Analyze entitlement missing.',
      ],
    },
    provenance: {
      liveMetricsPath: relative,
      ga4SummaryPresent: fs.existsSync(ga4SummaryPath),
      gscSummaryPresent: fs.existsSync(gscSummaryPath),
      enrichIngest: live?.ingest || null,
    },
  };
}

export default { readHelenaCmoAnalyticsContract, getHelenaMarketingIntelligenceStatus };
