/**
 * Deal Capture™ Dashboard Home API
 * GET /api/dashboard/home?role=owner|brand|operator
 * Returns a single ViewModel contract. Swap mock logic for real APIs later.
 */

import { getKpiLabels, getPipelineLabels, ROLES } from '../lib/dashboardRoleConfig.js';

const STORAGE_KEY = 'dc_dashboard_role_view';
const WINNING_LOI_TABLE =
  process.env.AIRTABLE_TABLE_WINNING_LOI_DEALS ||
  process.env.AIRTABLE_TABLE_LOI_DEALS ||
  'Winning_LOI_Deals';
const WINNING_LOI_TABLE_CANDIDATES = [
  WINNING_LOI_TABLE,
  'Winning LOI Deals',
  'Winning LOI Deal',
  'Winning_LOI_Deals',
  'Winning LOI'
];

function getAirtableConfig() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return null;
  return { baseId, apiKey };
}

async function fetchAllAirtableRecords(baseId, apiKey, tableName) {
  const encodedTable = encodeURIComponent(tableName);
  const records = [];
  let offset = null;
  do {
    let url = `https://api.airtable.com/v0/${baseId}/${encodedTable}?pageSize=100`;
    if (offset) url += `&offset=${encodeURIComponent(offset)}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.error) {
      throw new Error((data && data.error && data.error.message) || `Failed to fetch ${tableName}`);
    }
    records.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);
  return records;
}

async function fetchWinningLoiRecords(baseId, apiKey) {
  const attempted = [];
  for (const candidate of WINNING_LOI_TABLE_CANDIDATES) {
    const tableName = String(candidate || '').trim();
    if (!tableName || attempted.includes(tableName)) continue;
    attempted.push(tableName);
    try {
      const records = await fetchAllAirtableRecords(baseId, apiKey, tableName);
      return records;
    } catch (error) {
      const msg = String(error?.message || error || '');
      const tableMissingOrDenied = /invalid permissions|model was not found|not found|unknown table/i.test(msg);
      if (!tableMissingOrDenied) throw error;
    }
  }
  throw new Error('Unable to access Winning LOI table. Tried: ' + attempted.join(', '));
}

function normalizeQuarter(rawValue) {
  if (!rawValue) return null;
  const raw = String(rawValue).trim();
  const quarterMatch = raw.match(/Q([1-4])\s*[-,]?\s*(20\d{2})/i);
  if (quarterMatch) return `Q${quarterMatch[1]} ${quarterMatch[2]}`;
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return null;
  const q = Math.floor(dt.getMonth() / 3) + 1;
  return `Q${q} ${dt.getFullYear()}`;
}

function quarterSortKey(quarterLabel) {
  const m = String(quarterLabel || '').match(/^Q([1-4])\s+(20\d{2})$/);
  if (!m) return Number.MAX_SAFE_INTEGER;
  return (parseInt(m[2], 10) * 10) + parseInt(m[1], 10);
}

function pickFirstField(fields, candidates) {
  for (let i = 0; i < candidates.length; i++) {
    const key = candidates[i];
    const value = fields[key];
    if (value != null && String(value).trim() !== '') return value;
  }
  return null;
}

function classifyDealStructure(rawValue) {
  const value = String(rawValue || '').toLowerCase();
  return {
    isFranchise: value.includes('franchise'),
    isManagement: value.includes('management') || value.includes('mgmt')
  };
}

function classifyDealStructureExclusive(rawValue) {
  const value = String(rawValue || '').toLowerCase();
  const hasFranchise = value.includes('franchise');
  const hasManagement = value.includes('management') || value.includes('mgmt');
  if (hasFranchise && hasManagement) return 'management';
  if (hasManagement) return 'management';
  if (hasFranchise) return 'franchise';
  return null;
}

function aggregateSeriesByQuarter(records, opts) {
  const byQuarter = new Map();
  const dateFieldCandidates = opts.dateFieldCandidates || [];
  const structureFieldCandidates = opts.structureFieldCandidates || [];
  for (const rec of records) {
    const fields = rec.fields || {};
    const quarter = normalizeQuarter(pickFirstField(fields, dateFieldCandidates));
    if (!quarter) continue;
    const structure = pickFirstField(fields, structureFieldCandidates);
    const { isFranchise, isManagement } = classifyDealStructure(structure);
    if (!isFranchise && !isManagement) continue;
    if (!byQuarter.has(quarter)) byQuarter.set(quarter, { franchise: 0, management: 0 });
    const row = byQuarter.get(quarter);
    if (isFranchise) row.franchise += 1;
    if (isManagement) row.management += 1;
  }
  return byQuarter;
}

function sliceRecentQuarters(sortedQuarters, count) {
  if (sortedQuarters.length <= count) return sortedQuarters;
  return sortedQuarters.slice(sortedQuarters.length - count);
}

function getRecentQuarterLabels(count = 4, now = new Date()) {
  const labels = [];
  const anchor = new Date(now);
  anchor.setMonth(Math.floor(anchor.getMonth() / 3) * 3, 1);
  anchor.setHours(0, 0, 0, 0);
  for (let i = count - 1; i >= 0; i--) {
    const dt = new Date(anchor);
    dt.setMonth(anchor.getMonth() - i * 3);
    const q = Math.floor(dt.getMonth() / 3) + 1;
    labels.push(`Q${q} ${dt.getFullYear()}`);
  }
  return labels;
}

function buildZeroChartData(labels) {
  return {
    labels,
    franchiseDeals: labels.map(() => 0),
    managementDeals: labels.map(() => 0),
    regionFranchiseDeals: labels.map(() => 0),
    regionManagementDeals: labels.map(() => 0)
  };
}

function buildMockLoiChartData() {
  return {
    labels: ['Q3 2024', 'Q4 2024', 'Q1 2025', 'Q2 2025'],
    franchiseDeals: [14, 16, 18, 20],
    managementDeals: [9, 11, 14, 18],
    regionFranchiseDeals: [14, 16, 18, 20],
    regionManagementDeals: [9, 11, 14, 18]
  };
}

function buildMockSignedLoiRoomsByRegionOverlay() {
  return {
    country: 'North America',
    value: '2.1K'
  };
}

function toNumber(value) {
  if (value == null) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  const n = parseFloat(String(value).replace(/[^0-9.-]/g, ''));
  return Number.isFinite(n) ? n : 0;
}

function formatCompactRooms(value) {
  const n = Math.max(0, Math.round(toNumber(value)));
  if (n >= 1000000) return (n / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'K';
  return String(n);
}

function isSignedLoiStatus(rawStatus) {
  const s = String(rawStatus || '').trim().toLowerCase();
  if (!s) return true; // Winning table rows are treated as signed unless status says otherwise.
  if (s.includes('declined') || s.includes('lost') || s.includes('withdrawn') || s.includes('cancel')) return false;
  return s.includes('signed') || s.includes('won') || s.includes('executed') || s.includes('closed');
}

async function buildSignedLoiRoomsByRegionOverlay() {
  const airtable = getAirtableConfig();
  if (!airtable) return { country: 'No Data', value: '0' };

  const records = await fetchWinningLoiRecords(airtable.baseId, airtable.apiKey);
  const roomsByRegion = new Map();

  for (const rec of records) {
    const fields = rec.fields || {};
    const status = pickFirstField(fields, ['LOI Status', 'Status', 'Deal Status', 'Proposal Status']);
    if (!isSignedLoiStatus(status)) continue;
    const region = String(pickFirstField(fields, ['Region', 'Market Region', 'Geography', 'Country']) || '').trim() || 'Unknown';
    const rooms = toNumber(pickFirstField(fields, ['Rooms', 'Key Count', 'Room Count', 'Total Rooms']));
    if (rooms <= 0) continue;
    roomsByRegion.set(region, (roomsByRegion.get(region) || 0) + rooms);
  }

  if (roomsByRegion.size === 0) return { country: 'No Data', value: '0' };

  let topRegion = 'Unknown';
  let topRooms = 0;
  for (const [region, totalRooms] of roomsByRegion.entries()) {
    if (totalRooms > topRooms) {
      topRegion = region;
      topRooms = totalRooms;
    }
  }

  return {
    country: topRegion,
    value: formatCompactRooms(topRooms)
  };
}

async function buildLoiDealVolumeChartData() {
  const airtable = getAirtableConfig();
  const fallbackLabels = getRecentQuarterLabels(4);
  if (!airtable) return buildZeroChartData(fallbackLabels);

  const winningLoiDeals = await fetchWinningLoiRecords(airtable.baseId, airtable.apiKey);
  const series = new Map();
  for (const rec of winningLoiDeals) {
    const fields = rec.fields || {};
    const quarter = normalizeQuarter(
      pickFirstField(fields, ['Executed', 'Execution Date', 'Closed Date', 'Date', 'Created'])
    );
    if (!quarter) continue;
    const bucket = classifyDealStructureExclusive(
      pickFirstField(fields, ['Deal Structure', 'Structure', 'Deal Type', 'Project Type'])
    );
    if (!bucket) continue;
    if (!series.has(quarter)) series.set(quarter, { franchise: 0, management: 0 });
    const row = series.get(quarter);
    if (bucket === 'franchise') row.franchise += 1;
    if (bucket === 'management') row.management += 1;
  }

  const labels = sliceRecentQuarters([...series.keys()].sort((a, b) => quarterSortKey(a) - quarterSortKey(b)), 6);
  if (labels.length === 0) return buildZeroChartData(fallbackLabels);

  const franchise = labels.map((q) => series.get(q)?.franchise || 0);
  const management = labels.map((q) => series.get(q)?.management || 0);

  return {
    labels,
    franchiseDeals: franchise,
    managementDeals: management,
    regionFranchiseDeals: franchise,
    regionManagementDeals: management
  };
}

/**
 * Build the home dashboard ViewModel. Sample preview metrics for layout; wire live data later.
 * @param {{ userId?: string, role: string }} opts
 * @returns {Object} ViewModel
 */
export function buildHomeDashboardViewModel(opts = {}) {
  const role = ROLES.includes(opts.role) ? opts.role : 'owner';
  const kpiConfig = getKpiLabels(role);
  const pipelineConfig = getPipelineLabels(role);

  // Mock KPI values (role-specific counts – placeholder for real API)
  const kpiValues = {
    owner: [12, 5, 3, 4, 1, 2],
    brand: [18, 4, 5, 6, 3, 2],
    operator: [14, 6, 4, 3, 2, 1]
  };
  const kpiDeltas = {
    owner: [
      { deltaLabel: '+2 vs last week', deltaType: 'up' },
      { deltaLabel: '−1 vs last week', deltaType: 'down' },
      { deltaLabel: '+1 vs last week', deltaType: 'up' },
      { deltaLabel: 'same', deltaType: 'neutral' },
      { deltaLabel: 'same', deltaType: 'neutral' },
      { deltaLabel: 'same', deltaType: 'neutral' }
    ],
    brand: [
      { deltaLabel: '+3 vs last week', deltaType: 'up' },
      { deltaLabel: '−1 vs last week', deltaType: 'down' },
      { deltaLabel: '+2 vs last week', deltaType: 'up' },
      { deltaLabel: 'same', deltaType: 'neutral' },
      { deltaLabel: '+1 vs last week', deltaType: 'up' },
      { deltaLabel: 'same', deltaType: 'neutral' }
    ],
    operator: [
      { deltaLabel: '+2 vs last week', deltaType: 'up' },
      { deltaLabel: 'same', deltaType: 'neutral' },
      { deltaLabel: '+1 vs last week', deltaType: 'up' },
      { deltaLabel: 'same', deltaType: 'neutral' },
      { deltaLabel: 'same', deltaType: 'neutral' },
      { deltaLabel: 'same', deltaType: 'neutral' }
    ]
  };
  const vals = kpiValues[role] || kpiValues.owner;
  const deltas = kpiDeltas[role] || kpiDeltas.owner;

  const kpis = kpiConfig.map((c, i) => ({
    key: c.key,
    label: c.label,
    value: vals[i] ?? 0,
    deltaLabel: deltas[i]?.deltaLabel ?? '—',
    deltaType: deltas[i]?.deltaType ?? 'neutral'
  }));

  // Pipeline stages with mock counts and metaBadges
  const pipelineCounts = [3, 4, 5, 2, 1, 2];
  const pipelineMeta = [
    [{ label: 'New: 1', type: 'info' }],
    [{ label: 'Adv: 1', type: 'info' }, { label: 'Stalled: 1', type: 'warn' }],
    [{ label: 'New: 1', type: 'info' }, { label: 'Adv: 2', type: 'info' }],
    [{ label: 'Adv: 1', type: 'info' }],
    [{ label: 'Adv: 1', type: 'info' }],
    []
  ];
  const pipeline = pipelineConfig.map((c, i) => ({
    stageKey: c.stageKey,
    label: c.label,
    count: pipelineCounts[i] ?? 0,
    metaBadges: pipelineMeta[i] ?? []
  }));

  return {
    success: true,
    role,
    contentMode: 'sample',
    header: {
      greeting: 'Welcome to your Dealality command center',
      lastSyncLabel: '2 min ago',
      ctas: [
        { key: 'new-deal', label: 'New Deal', href: '/new-deal-setup', primary: true },
        { key: 'outreach', label: 'Start Outreach', href: '/outreach' },
        { key: 'messages', label: 'Messages', href: '/outreach/inbox' }
      ]
    },
    kpis,
    pipeline,
    signals: [
      { type: 'risk', title: '2 threads stalled (3–7 days)', subtitle: 'Sample scenario: partner replies pending on two deals.', ctaLabel: 'Fix', ctaHref: '/outreach/inbox' },
      { type: 'opportunity', title: '3 deals have strong engagement but no next step scheduled', subtitle: 'Sample scenario: brands viewed PIP; schedule follow-up.', ctaLabel: 'View', ctaHref: '/my-deals' },
      { type: 'watch', title: 'Response frequency dipped below 95% this week', subtitle: 'Sample scenario: a few partners slowed replies.', ctaLabel: 'Open messages', ctaHref: '/outreach/inbox' },
      { type: 'opportunity', title: '1 deal advanced to negotiation', subtitle: 'Sample scenario: deal moved to term review.', ctaLabel: 'Open deal', ctaHref: '/my-deals' }
    ],
    nextActions: [
      { priority: 'urgent', title: 'LOI due in 48 hours', contextLabel: 'Sample deal · Conversion project', dueLabel: 'Today', ctaLabel: 'Open', ctaHref: '/my-deals' },
      { priority: 'medium', title: 'Review PIP feedback', contextLabel: 'Sample deal · Boutique project', dueLabel: 'Tomorrow', ctaLabel: 'Open', ctaHref: '/my-deals' },
      { priority: 'urgent', title: 'Follow up on stalled thread', contextLabel: 'Sample deal · Resort project', dueLabel: 'In 2 days', ctaLabel: 'Open', ctaHref: '/outreach/inbox' },
      { priority: 'low', title: 'Deals needing follow-up', contextLabel: '4 deals (sample)', dueLabel: '—', ctaLabel: 'View', ctaHref: '/my-deals' }
    ],
    recentActivity: [
      { type: 'deal', title: 'Response received', contextLabel: 'Sample deal activity', timeAgo: '2h ago', badgeLabel: 'Deal Activity', badgeType: 'info', ctaHref: '/outreach/inbox' },
      { type: 'deal', title: 'Deal advanced to Shared', contextLabel: 'Sample deal activity', timeAgo: '4h ago', badgeLabel: 'Deal Activity', badgeType: 'info', ctaHref: '/my-deals' },
      { type: 'market', title: 'Sample: RevPAR trend highlight', contextLabel: null, timeAgo: '5h ago', badgeLabel: 'Market Action', badgeType: 'info', ctaHref: '/market-alerts' },
      { type: 'deal', title: 'Message sent', contextLabel: 'Sample deal activity', timeAgo: '5h ago', badgeLabel: 'Deal Activity', badgeType: 'info', ctaHref: '/outreach/inbox' },
      { type: 'deal', title: 'Proposal viewed', contextLabel: 'Sample deal activity', timeAgo: '6h ago', badgeLabel: 'Deal Activity', badgeType: 'info', ctaHref: '/my-deals' },
      { type: 'news', title: 'Sample: Market headline', contextLabel: null, timeAgo: '8h ago', badgeLabel: 'Market News', badgeType: 'info', ctaHref: '/market-alerts' },
      { type: 'deal', title: 'Follow-up reminder', contextLabel: 'Sample deal activity', timeAgo: '1d ago', badgeLabel: 'Deal Activity', badgeType: 'info', ctaHref: '/my-deals' },
      { type: 'news', title: 'Sample: Brand launch headline', contextLabel: null, timeAgo: '1d ago', badgeLabel: 'Market News', badgeType: 'info', ctaHref: '/market-alerts' }
    ],
    marketIntel: [
      { type: 'brand', title: 'Sample: Hilton expands in Caribbean', subtitle: 'Illustrative market headline for layout preview.', timeAgo: '2h ago', ctaLabel: 'View alerts', ctaHref: '/market-alerts' },
      { type: 'supply', title: 'Sample: PIP-ready properties in Mexico City', subtitle: 'Illustrative supply signal for layout preview.', timeAgo: '5h ago', ctaLabel: 'View alerts', ctaHref: '/market-alerts' },
      { type: 'airport', title: 'Sample: Airport expansion timeline', subtitle: 'Illustrative market signal for layout preview.', timeAgo: '8h ago', ctaLabel: 'View alerts', ctaHref: '/market-alerts' },
      { type: 'financing', title: 'Sample: Regional lender rate update', subtitle: 'Illustrative financing signal for layout preview.', timeAgo: '1d ago', ctaLabel: 'View alerts', ctaHref: '/market-alerts' },
      { type: 'brand', title: 'Sample: Soft brand launch in CALA', subtitle: 'Illustrative brand signal for layout preview.', timeAgo: '3h ago', ctaLabel: 'View alerts', ctaHref: '/market-alerts' }
    ],
    charts: { keepExisting: true },
    loiDealVolumeChartData: {
      labels: ['Q3 2024', 'Q4 2024', 'Q1 2025', 'Q2 2025'],
      franchiseDeals: [18, 22, 28, 32],
      managementDeals: [12, 15, 18, 20],
      regionFranchiseDeals: [120, 145, 165, 190],
      regionManagementDeals: [95, 110, 125, 140]
    },
    dealsByCountryMapOverlay: { country: 'Mexico', value: '2.1 K' },
    executionReliability: {
      badgeLabel: 'Very Fast · Frequently',
      badgeIcon: '🚀📬',
      avgFirstResponseHours: '2.4',
      responseFrequencyPct: 94,
      trendLabel: '+3% vs prior period',
      whyItMatters: 'Execution Reliability measures how quickly and consistently you and your partners respond. Fast response times and high frequency help close deals faster and build trust.',
      drivers: [
        { title: '2 threads stalled > 3 days', subtitle: 'Sample scenario: replies pending', ctaLabel: 'Open Messages', ctaHref: '/outreach/inbox' },
        { title: 'Response frequency dipped', subtitle: 'Sample scenario: slower partner replies', ctaLabel: 'Open Deal', ctaHref: '/my-deals' }
      ]
    },
    toolboxLinks: [
      { id: 'my-deals', label: 'My Deals', labelHtml: 'My<br>Deals', iconKey: 'briefcase', href: '/my-deals', status: 'Live' },
      { id: 'outreach', label: 'Outreach Plans', labelHtml: 'Outreach<br>Plans', iconKey: 'mail', href: '/outreach', status: 'Live' },
      { id: 'partner-directory', label: 'Partner Directory', labelHtml: 'Partner<br>Directory', iconKey: 'users', href: '/partner-directory', status: 'Live' },
      { id: 'market-intel', label: 'Fee Estimator', labelHtml: 'Fee<br>Estimator', iconKey: 'trending-up', href: '/franchise-fee-estimator', status: 'Live' },
      { id: 'financial-term', label: 'Term Library', labelHtml: 'Term<br>Library', iconKey: 'file-text', href: '/financial-term-library', status: 'Live' },
      { id: 'brand-explorer', label: 'Brand Explorer', labelHtml: 'Brand<br>Explorer', iconKey: 'building', href: '/brand-explorer-combined', status: 'Live' },
      { id: 'legal-clause', label: 'Clause Library', labelHtml: 'Clause<br>Library', iconKey: 'file', href: '/clause-library', status: 'Live' },
      { id: 'message-center', label: 'Message Center', labelHtml: 'Message<br>Center', iconKey: 'message', href: '/outreach/inbox', status: 'Beta' },
      { id: 'deal-compare', label: 'Deal Compare', labelHtml: 'Deal<br>Compare', iconKey: 'scale', href: '/deal-compare', status: 'Beta' }
    ]
  };
}

/**
 * GET /api/dashboard/home
 * Query: role=owner|brand|operator
 */
export async function getDashboardHome(req, res) {
  const roleParam = (req.query.role || '').toLowerCase();
  const role = ['owner', 'brand', 'operator'].includes(roleParam) ? roleParam : 'owner';
  const userId = req.user?.id || req.headers['x-user-id'] || null;
  const vm = buildHomeDashboardViewModel({ userId, role });
  try {
    const liveChart = await buildLoiDealVolumeChartData();
    vm.loiDealVolumeChartData = liveChart;
  } catch (error) {
    vm.loiDealVolumeChartData = buildMockLoiChartData();
    console.warn('[dashboard-home] LOI chart fallback to mock:', error?.message || error);
  }
  try {
    vm.dealsByCountryMapOverlay = await buildSignedLoiRoomsByRegionOverlay();
  } catch (error) {
    vm.dealsByCountryMapOverlay = buildMockSignedLoiRoomsByRegionOverlay();
    console.warn('[dashboard-home] Signed LOI rooms by region fallback to mock:', error?.message || error);
  }
  res.json(vm);
}
