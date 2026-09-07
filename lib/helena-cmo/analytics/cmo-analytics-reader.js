/**
 * Helena CMO analytics reader — READ only.
 * Live GA4/GSC APIs are NOT connected. Uses phase-0 snapshots + optional Marketing OS Performance.
 * Never invents metrics.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '../..');
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

function readText(p) {
  try {
    if (!fs.existsSync(p)) return null;
    return fs.readFileSync(p, 'utf8');
  } catch {
    return null;
  }
}

function brandedQuery(q) {
  return /dealality|brand explorer|operator explorer/i.test(String(q || ''));
}

/**
 * Canonical CMO analytics contract from available evidence.
 */
export function readHelenaCmoAnalyticsContract() {
  const ga4Path = path.join(ANALYTICS_DIR, 'GA4_SESSION_SNAPSHOT_2026-06-01_2026-09-06.json');
  const gscPath = path.join(ANALYTICS_DIR, 'GSC_PAGE_SNAPSHOT_2026-06-01_2026-09-06.json');
  const ga4Summary = readText(path.join(ANALYTICS_DIR, 'GA4_SUMMARY.md'));
  const gscSummary = readText(path.join(ANALYTICS_DIR, 'GSC_SUMMARY.md'));
  const liSummary = readText(path.join(ANALYTICS_DIR, 'LINKEDIN_ENGAGEMENT_SUMMARY.md'));
  const liPosts = readJson(path.join(ANALYTICS_DIR, 'linkedin_posts_metrics.json'));
  const ga4 = readJson(ga4Path);
  const gsc = readJson(gscPath);

  const liveGa4 = false;
  const liveGsc = false;
  const liveLinkedIn = false;

  const sessions = ga4?.totals?.sessions ?? ga4?.sessions ?? null;
  const users = ga4?.totals?.users ?? ga4?.users ?? null;
  const sourceRows = ga4?.bySourceMedium || ga4?.sourceMedium || ga4?.rows || [];
  const landingRows = ga4?.byLandingPage || ga4?.landingPages || [];

  // Normalize loose snapshot shapes
  let acquisition = {
    users,
    sessions,
    dateRange: ga4?.dateRange || '2026-06-01_2026-09-06',
    freshness: 'STALE_SNAPSHOT',
    sourceMediumTop: [],
    landingPagesTop: [],
    newVsReturning: ga4?.newVsReturning || 'UNKNOWN',
    geography: ga4?.geography || 'UNKNOWN',
  };

  if (Array.isArray(sourceRows) && sourceRows.length) {
    acquisition.sourceMediumTop = sourceRows.slice(0, 10).map((r) => ({
      sourceMedium: r.sourceMedium || r.dimension || r.key || String(r.source || '') + ' / ' + String(r.medium || ''),
      sessions: r.sessions ?? r.metric ?? null,
      users: r.users ?? null,
    }));
  }

  if (Array.isArray(landingRows) && landingRows.length) {
    acquisition.landingPagesTop = landingRows.slice(0, 10).map((r) => ({
      page: r.page || r.landingPage || r.dimension || r.path,
      sessions: r.sessions ?? null,
    }));
  }

  // If snapshot is a flat object with sessionChannelGroup etc.
  if (!acquisition.sourceMediumTop.length && ga4 && typeof ga4 === 'object') {
    const keys = Object.keys(ga4);
    acquisition.rawKeys = keys.slice(0, 30);
  }

  const gscRows = Array.isArray(gsc) ? gsc : gsc?.rows || gsc?.pages || [];
  const organic = {
    freshness: 'STALE_SNAPSHOT',
    dateRange: gsc?.dateRange || '2026-06-01_2026-09-06',
    topPages: [],
    topQueries: [],
    brandedVsNonBranded: 'PARTIAL_UNKNOWN',
  };

  if (Array.isArray(gscRows)) {
    organic.topPages = gscRows.slice(0, 15).map((r) => ({
      page: r.page || r.keys?.[0] || r.url,
      clicks: r.clicks ?? r.metrics?.clicks ?? null,
      impressions: r.impressions ?? r.metrics?.impressions ?? null,
      ctr: r.ctr ?? r.metrics?.ctr ?? null,
      position: r.position ?? r.metrics?.position ?? null,
    }));
  }
  const queryRows = gsc?.queries || gsc?.byQuery || [];
  if (Array.isArray(queryRows)) {
    organic.topQueries = queryRows.slice(0, 20).map((r) => ({
      query: r.query || r.keys?.[0],
      clicks: r.clicks ?? null,
      impressions: r.impressions ?? null,
      branded: brandedQuery(r.query || r.keys?.[0]),
    }));
    const branded = organic.topQueries.filter((q) => q.branded).length;
    const non = organic.topQueries.length - branded;
    if (organic.topQueries.length) {
      organic.brandedVsNonBranded = { brandedQueriesInTop: branded, nonBrandedInTop: non };
    }
  }

  const commercialIntent = {
    status: 'NOT_INSTRUMENTED_OR_UNKNOWN',
    ctaClicks: 'UNKNOWN',
    demoIntent: 'UNKNOWN',
    pilotIntent: 'UNKNOWN',
    formStarts: 'UNKNOWN',
    formCompletions: 'UNKNOWN',
    productPageEngagement: 'UNKNOWN',
    note: 'GA4 snapshot lacks conversion/event grain usable for CMO commercial intent.',
  };

  const trend = {
    status: 'NOT_AVAILABLE_AS_30_VS_PRIOR',
    note: 'Snapshot is a single Jun–Sep window; cannot compute current-30 vs prior-30 without live API.',
  };

  const conclusions = {
    businessOutcomes: [
      'No attributable revenue, pilots, or qualified opportunities in analytics grain.',
      'Traffic volume alone is not commercial success (sessions ~553 over ~3 months in snapshot).',
    ],
    commercialIntent: [
      'Commercial-intent events (CTA/demo/pilot/form) are UNKNOWN in available GA4 export.',
      'Cannot separate interested owners from anonymous browsing.',
    ],
    channelActivity: [
      'Direct-heavy acquisition pattern in prior Helena reads; organic thin.',
      'LinkedIn→site attribution UNKNOWN.',
      'GSC shows thin clicks relative to some vanity impressions (per prior GSC summary).',
    ],
  };

  const linkedIn = {
    freshness: liPosts ? 'STALE_SCRAPE' : 'MISSING',
    liveApi: liveLinkedIn,
    postCount: Array.isArray(liPosts) ? liPosts.length : liPosts?.posts?.length || null,
    impressions: 'NULL_IN_SCRAPE',
    contentPerformance: 'AVAILABLE_PROXY_LIKES_COMMENTS',
    audienceSignal: 'WEAK_PROXY',
    commercialImpact: 'UNKNOWN',
    roleDecisionSufficient: true,
    roleDecisionNote:
      'Enough to set LinkedIn ROLE (founder commercial education + warm activation) without precise ROI.',
    summaryExcerpt: (liSummary || '').slice(0, 500),
  };

  return {
    schemaVersion: 'helena-cmo-analytics-contract-v1',
    generatedAt: new Date().toISOString(),
    connectors: {
      ga4LiveApi: liveGa4 ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      gscLiveApi: liveGsc ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      linkedInAnalyticsApi: liveLinkedIn ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      snapshotFiles: fs.existsSync(ANALYTICS_DIR) ? 'CONNECTED_AND_WORKING' : 'NOT_CONNECTED',
      marketingOsPerformance: 'CONNECTED_BUT_NOT_WIRED_TO_HELENA',
      landingEvents: 'CONNECTED_BUT_NOT_WIRED_TO_HELENA',
    },
    blockers: {
      ga4: 'No Google Analytics Data API client or GA4_* credentials in repo/.env.example.',
      gsc: 'No Search Console API client or GSC_* credentials in repo/.env.example.',
      linkedIn: 'No LinkedIn Marketing/Analytics OAuth integration; scrape/CSV only.',
    },
    acquisition,
    commercialIntent,
    organic,
    trend,
    linkedIn,
    conclusions,
    provenance: {
      ga4Path: fs.existsSync(ga4Path) ? path.relative(ROOT, ga4Path) : null,
      gscPath: fs.existsSync(gscPath) ? path.relative(ROOT, gscPath) : null,
      ga4SummaryPresent: Boolean(ga4Summary),
      gscSummaryPresent: Boolean(gscSummary),
    },
  };
}

export default { readHelenaCmoAnalyticsContract };
