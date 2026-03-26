/**
 * Dealality Dashboard – Data Adapter Layer
 *
 * Maps API/Airtable data to view models. NO Airtable field names in UI components.
 *
 * To wire real APIs later:
 * - Replace getDashboardViewModel() implementation with fetch to /api/dashboard (or similar)
 * - Add API module that reads ResponsivenessScores, ResponsivenessInteractions, Deals, etc.
 * - Set window.USE_MOCK_DASHBOARD = false to toggle mock vs real (or use env/query param)
 */

(function () {
  'use strict';

  // Response Time Categories (exact spec)
  var RESPONSE_TIME_BANDS = [
    { maxMinutes: 60, category: 'Lightning Fast', icon: '⚡' },
    { maxMinutes: 360, category: 'Very Fast', icon: '🚀' },
    { maxMinutes: 1440, category: 'Responsive', icon: '✅' },
    { maxMinutes: 4320, category: 'Slow', icon: '⏳' },
    { maxMinutes: 10080, category: 'Stalled', icon: '❌' },
    { maxMinutes: Infinity, category: 'Unresponsive', icon: '🔕' }
  ];

  var RESPONSE_FREQUENCY_BANDS = [
    { minPercent: 90, category: 'Frequently', icon: '📬' },
    { minPercent: 50, category: 'Occasionally', icon: '💬' },
    { minPercent: 0, category: 'Rarely', icon: '💤' }
  ];

  /**
   * Compute response time category from elapsed minutes.
   * @param {number} elapsedMinutes
   * @returns {{ category: string, icon: string }}
   */
  function computeResponseTimeCategory(elapsedMinutes) {
    if (elapsedMinutes == null || elapsedMinutes < 0) {
      return { category: 'Unresponsive', icon: '🔕' };
    }
    for (var i = 0; i < RESPONSE_TIME_BANDS.length; i++) {
      if (elapsedMinutes <= RESPONSE_TIME_BANDS[i].maxMinutes) {
        return {
          category: RESPONSE_TIME_BANDS[i].category,
          icon: RESPONSE_TIME_BANDS[i].icon
        };
      }
    }
    return { category: 'Unresponsive', icon: '🔕' };
  }

  /**
   * Compute response frequency category from percent.
   * @param {number} percent - 0–100
   * @returns {{ category: string, icon: string }}
   */
  function computeResponseFrequencyCategory(percent) {
    if (percent == null || percent < 0) {
      return { category: 'Rarely', icon: '💤' };
    }
    for (var i = 0; i < RESPONSE_FREQUENCY_BANDS.length; i++) {
      if (percent >= RESPONSE_FREQUENCY_BANDS[i].minPercent) {
        return {
          category: RESPONSE_FREQUENCY_BANDS[i].category,
          icon: RESPONSE_FREQUENCY_BANDS[i].icon
        };
      }
    }
    return { category: 'Rarely', icon: '💤' };
  }

  /**
   * Map ResponsivenessScores row to Execution Reliability widget view model.
   * @param {Object} scoresRow - Raw API/Airtable row (use generic keys or field IDs)
   * @param {Object} opts - Optional: rollingWindowDays, whyItMatters, mostResponsivePartners, atRiskPartners
   * @returns {Object} Widget view model
   */
  function mapResponsivenessScoresToWidget(scoresRow, opts) {
    if (!scoresRow) return null;
    opts = opts || {};
    var elapsedMinutes = scoresRow.medianElapsedMinutes ?? scoresRow.MedianElapsedMinutes;
    var freqPercent = scoresRow.responseFrequencyPercent ?? scoresRow.ResponseFrequencyPercent;
    var speed = computeResponseTimeCategory(elapsedMinutes);
    var freq = computeResponseFrequencyCategory(freqPercent);
    var combinedBadge = (speed.icon + freq.icon + ' ' + speed.category + ' · ' + freq.category);
    var trend = scoresRow.trend ?? 'flat';
    return {
      combinedBadge: combinedBadge,
      avgFirstResponseTimeHours: elapsedMinutes != null ? (elapsedMinutes / 60).toFixed(1) : null,
      responseFrequencyPercent: freqPercent,
      trend: trend,
      trendLabel: scoresRow.trendLabel ?? null,
      rollingWindowDays: scoresRow.rollingWindowDays ?? scoresRow.RollingWindowDays ?? opts.rollingWindowDays ?? 60,
      totalReceived: scoresRow.totalInteractionsReceived ?? scoresRow.TotalInteractionsReceived ?? 0,
      totalResponded: scoresRow.totalResponded ?? scoresRow.TotalResponded ?? 0,
      tips: Array.isArray(scoresRow.tips) ? scoresRow.tips : [],
      whyItMatters: scoresRow.whyItMatters ?? opts.whyItMatters ?? null,
      mostResponsivePartners: Array.isArray(scoresRow.mostResponsivePartners) ? scoresRow.mostResponsivePartners : (opts.mostResponsivePartners || []),
      atRiskPartners: Array.isArray(scoresRow.atRiskPartners) ? scoresRow.atRiskPartners : (opts.atRiskPartners || [])
    };
  }

  /**
   * Map ResponsivenessInteractions to alerts / signals.
   * @param {Array} interactions
   * @returns {Array} Alert view models
   */
  function mapResponsivenessInteractionsToAlerts(interactions) {
    if (!Array.isArray(interactions)) return [];
    return interactions.map(function (i) {
      var category = i.responseTimeCategory ?? i.ResponseTimeCategory;
      var elapsedMinutes = i.elapsedMinutes ?? i.ElapsedMinutes;
      var cat = category ? null : computeResponseTimeCategory(elapsedMinutes);
      return {
        id: i.id ?? i.Id,
        threadId: i.threadId ?? i.Thread,
        dealId: i.dealId ?? i.Deal,
        category: category || (cat && cat.category),
        icon: (cat && cat.icon) || '🔕',
        elapsedHours: elapsedMinutes != null ? (elapsedMinutes / 60).toFixed(1) : null,
        label: i.label ?? i.interaction_label ?? 'Thread needs follow-up'
      };
    });
  }

  /**
   * Mock dashboard view model. Replace with API call when wiring.
   */
  function getDashboardViewModel() {
    var userName = 'Joan';
    var lastSync = '2 min ago';

    return {
      header: {
        greeting: 'Welcome back, ' + userName,
        lastSync: lastSync,
        lastSyncLabel: lastSync,
        ctas: [
          { label: 'New Deal', href: '/deal-setup', primary: true },
          { label: 'Start Outreach', href: '/outreach-plans' },
          { label: 'Messages', href: '/outreach-inbox' },
          { label: 'Invite Partner', href: '#' },
          { label: 'Export', href: '#' }
        ]
      },
      chips: [
        { id: 'follow-up', icon: '📌', label: 'Deals needing follow-up', count: 4, href: '/my-deals?filter=follow-up' },
        { id: 'responses', icon: '💬', label: 'New responses received', count: 3, href: '/outreach-inbox' },
        { id: 'deadlines', icon: '⏰', label: 'Deadlines in next 48h', count: 2, href: '/my-deals?filter=deadlines' },
        { id: 'at-risk', icon: '⚠', label: 'Deals at risk (stalled/unresponsive)', count: 2, href: '/my-deals?filter=at-risk' }
      ],
      heroMetric: {
        label: 'Needs Action',
        value: 5,
        context: 'vs 6 last week',
        trend: 'down'
      },
      trendChartData: {
        labels: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
        values: [8, 6, 7, 5, 4, 5, 5]
      },
      loiDealVolumeChartData: {
        labels: ['Q3 2024', 'Q4 2024', 'Q1 2025', 'Q2 2025'],
        franchiseDeals: [18, 22, 28, 32],
        managementDeals: [12, 15, 18, 20],
        regionFranchiseDeals: [120, 145, 165, 190],
        regionManagementDeals: [95, 110, 125, 140],
        totalValue: 100
      },
      regionalDistributionChartData: {
        labels: ['Mexico', 'Dominican Republic', 'Colombia', 'Costa Rica', 'Panama', 'Jamaica', 'Brazil', 'Argentina'],
        values: [28, 18, 14, 12, 10, 8, 6, 4],
        total: 100
      },
      dealsByCountryMapOverlay: { country: 'Mexico', value: '2.1 K' },
      todayFocus: [
        { id: 'follow-up', icon: '📌', label: 'Deals needing follow-up', count: 4, href: '/my-deals?filter=follow-up', variant: 'action' },
        { id: 'responses', icon: '💬', label: 'New responses', count: 3, href: '/outreach-inbox', variant: null },
        { id: 'deadlines', icon: '⏰', label: 'Deadlines in 48h', count: 2, href: '/my-deals?filter=deadlines', variant: 'warn' },
        { id: 'at-risk', icon: '⚠', label: 'At risk', count: 2, href: '/my-deals?filter=at-risk', variant: 'action' }
      ],
      kpis: [
        { id: 'active', label: 'Active Deals', value: 12, subtext: '+2 vs last week', trend: 'up' },
        { id: 'needs-action', label: 'Needs Action', value: 5, subtext: '−1 vs last week', trend: 'down' },
        { id: 'new-inquiries', label: 'New Inquiries (7d)', value: 3, subtext: '+1 vs last week', trend: 'up' },
        { id: 'under-review', label: 'Under Review', value: 4, subtext: 'same', trend: 'flat' },
        { id: 'shared-brands', label: 'Shared w/ Brands', value: 6, subtext: '+1 vs last week', trend: 'up' },
        { id: 'shared-operators', label: 'Shared w/ Operators', value: 8, subtext: 'same', trend: 'flat' },
        { id: 'at-risk', label: 'At Risk', value: 2, subtext: '2 stalled', trend: 'up' }
      ],
      signalsToday: [
        { tag: 'Risk', tagClass: 'risk', headline: '2 threads stalled (3–7 days)', why: 'Punta Cana & Cancun deals—no reply in 4+ days.', cta: 'Fix', href: '/outreach-inbox' },
        { tag: 'Opportunity', tagClass: 'opportunity', headline: '3 deals have strong engagement but no next step scheduled', why: 'Brands viewed PIP; schedule follow-up.', cta: 'View', href: '/my-deals' },
        { tag: 'Watch', tagClass: 'watch', headline: 'Response frequency dipped below 95% this week', why: 'A few partners slowed replies.', cta: 'Open responsiveness', href: '#' },
        { tag: 'Opportunity', tagClass: 'opportunity', headline: '1 deal advanced to negotiation', why: 'Hilton Guadalajara moved to term review.', cta: 'Open deal', href: '/my-deals' }
      ],
      responsivenessSummary: {
        combinedBadge: '🚀📬 Very Fast · Frequently',
        avgFirstResponseTimeHours: '2.4',
        responseFrequencyPercent: 94,
        gaugeScore: 94,
        trend: 'up',
        trendLabel: '+3% vs prior period',
        rollingWindowDays: 60,
        tips: [
          'Keep first replies under 6 hours to maintain Very Fast badge.',
          'Schedule weekly follow-ups on open threads.',
          'Flag stalled threads for priority follow-up.'
        ],
        whyItMatters: 'Execution Reliability measures how quickly and consistently you and your partners respond. Fast response times and high frequency help close deals faster and build trust. Use the Watchlist to focus on your best and at-risk partners.',
        mostResponsivePartners: [
          { name: 'Marriott Intl.', badge: '⚡📬' },
          { name: 'IHG Hotels', badge: '🚀📬' },
          { name: 'Hilton', badge: '✅📬' }
        ],
        atRiskPartners: [
          { name: 'Operator XYZ', badge: '❌ Stalled', dealName: 'Cancun Resort' },
          { name: 'Partner ABC', badge: '🔕 Unresponsive', dealName: 'Punta Cana' },
          { name: 'Brand Co.', badge: '⏳ Slow', dealName: 'Madrid Hotel' }
        ]
      },
      pipelineSnapshot: {
        maxCount: 5,
        stages: [
          { id: 'submitted', label: 'Submitted', count: 3, newThisWeek: 1, advancedThisWeek: 0, stalledThisWeek: 0 },
          { id: 'under-review', label: 'Under Review', count: 4, newThisWeek: 0, advancedThisWeek: 1, stalledThisWeek: 1 },
          { id: 'shared', label: 'Shared', count: 5, newThisWeek: 1, advancedThisWeek: 2, stalledThisWeek: 0 },
          { id: 'engaged', label: 'Bid Submitted', count: 2, newThisWeek: 0, advancedThisWeek: 1, stalledThisWeek: 0 },
          { id: 'negotiation', label: 'Term Review', count: 1, newThisWeek: 0, advancedThisWeek: 1, stalledThisWeek: 0 },
          { id: 'closed', label: 'Closed', count: 2, newThisWeek: 0, advancedThisWeek: 0, stalledThisWeek: 0 }
        ],
        cta: { label: 'View My Deals', href: '/my-deals' }
      },
      recentActivity: [
        { id: '1', iconKey: 'message', event: 'Response received', deal: 'Guadalajara Conversion', time: '2h ago', tag: 'Deal Activity', type: 'deal', href: '/outreach-inbox' },
        { id: '2', iconKey: 'file', event: 'Deal advanced to Shared', deal: 'Cancun Boutique', time: '4h ago', tag: 'Deal Activity', type: 'deal', href: '/my-deals' },
        { id: '3', iconKey: 'trending-up', event: 'Santo Domingo RevPAR up 12% YOY', deal: null, time: '5h ago', tag: 'Market Action', type: 'market', href: '/market-alerts' },
        { id: '4', iconKey: 'mail', event: 'Message sent', deal: 'Madrid Hotel', time: '5h ago', tag: 'Deal Activity', type: 'deal', href: '/outreach-inbox' },
        { id: '5', iconKey: 'eye', event: 'Proposal viewed', deal: 'Miami Beach', time: '6h ago', tag: 'Deal Activity', type: 'deal', href: '/my-deals' },
        { id: '6', iconKey: 'building', event: 'Mexico City Airport expansion timeline announced', deal: null, time: '8h ago', tag: 'Market News', type: 'news', href: '/market-alerts' },
        { id: '7', iconKey: 'pin', event: 'Follow-up reminder', deal: 'Punta Cana Resort', time: '1d ago', tag: 'Deal Activity', type: 'deal', href: '/my-deals' },
        { id: '8', iconKey: 'building', event: 'Soft brand launch in Caribbean', deal: null, time: '1d ago', tag: 'Market News', type: 'news', href: '/market-alerts' },
        { id: '9', iconKey: 'clipboard', event: 'LOI signed – Santiago conversion', deal: null, time: '2d ago', tag: 'Market Action', type: 'market', href: '/market-alerts' }
      ],
      nextActions: [
        { id: '1', title: 'LOI due in 48 hours', deal: 'Guadalajara Conversion', due: 'Today', severity: 'high', href: '/my-deals' },
        { id: '2', title: 'Review PIP feedback', deal: 'Cancun Boutique', due: 'Tomorrow', severity: 'medium', href: '/my-deals' },
        { id: '3', title: 'Follow up on stalled thread', deal: 'Punta Cana Resort', due: 'In 2 days', severity: 'high', href: '/outreach-inbox' },
        { id: '4', title: 'Deals needing follow-up', deal: '4 deals', due: '—', severity: null, href: '/my-deals' }
      ],
      topMarkets: [
        { market: 'Mexico City', count: 8, pct: 28 },
        { market: 'Cancun', count: 6, pct: 21 },
        { market: 'Punta Cana', count: 5, pct: 18 },
        { market: 'Madrid', count: 4, pct: 14 },
        { market: 'Miami', count: 5, pct: 18 }
      ],
      performanceSignals: [
        { label: 'Brand Response Rate', value: '72%', trend: 'up' },
        { label: 'Operator Response Rate', value: '68%', trend: 'flat' },
        { label: 'Avg First Response Time', value: '2.4h', trend: 'down' },
        { label: 'Avg Time to First Shortlist', value: '5.2 days', trend: 'flat' },
        { label: 'Deals at Risk (stalled > 3d)', value: 2, trend: 'up' },
        { label: 'Most Viewed Deal', value: 'Guadalajara Conversion', trend: null }
      ],
      marketIntelligence: [
        { tag: 'Brand Move', headline: 'Hilton expands in Caribbean', why: 'New flag announced for Punta Cana region.', time: '2h ago', href: '#' },
        { tag: 'Supply', headline: '3 new PIP-ready properties in Mexico City', why: 'Opportunity for reflag/conversion.', time: '5h ago', href: '#' },
        { tag: 'Airport', headline: 'Cancún Airport expansion timeline', why: 'May affect demand and valuations.', time: '8h ago', href: '#' },
        { tag: 'Financing', headline: 'Regional lender rate update', why: 'Rates down 25 bps for hospitality.', time: '1d ago', href: '#' },
        { tag: 'Brand Move', headline: 'Marriott soft brand launch in CALA', why: 'New conversion opportunity for lifestyle properties.', time: '3h ago', href: '#' },
        { tag: 'Supply', headline: 'Dominican Republic pipeline update', why: '5 new hotel projects announced in Santo Domingo.', time: '6h ago', href: '#' },
        { tag: 'Financing', headline: 'Caribbean hospitality fund closes', why: '$150M targeting resort acquisitions.', time: '12h ago', href: '#' },
        { tag: 'Market News', headline: 'Mexico City RevPAR forecast revised', why: 'Strong Q1 demand drives upward revision.', time: '2d ago', href: '#' }
      ],
      toolboxLinks: [
        { id: 'my-deals', label: 'My Deals', labelHtml: 'My<br>Deals', iconKey: 'briefcase', href: '/my-deals', status: 'Live' },
        { id: 'outreach', label: 'Outreach Plans', labelHtml: 'Outreach<br>Plans', iconKey: 'mail', href: '/outreach-plans', status: 'Live' },
        { id: 'partner-directory', label: 'Partner Directory', iconKey: 'users', href: '/partner-directory', status: 'Live' },
        { id: 'market-intel', label: 'Fee Estimator', iconKey: 'trending-up', href: '/franchise-fee-estimator', status: 'Live' },
        { id: 'financial-term', label: 'Term Library', iconKey: 'file-text', href: '/financial-term-library', status: 'Live' },
        { id: 'legal-clause', label: 'Clause Library', iconKey: 'file', href: '/clause-library', status: 'Live' },
        { id: 'message-center', label: 'Message Center', iconKey: 'message', href: '/outreach-inbox', status: 'Beta' },
        { id: 'company-profiles', label: 'Brand Explorer', iconKey: 'building', href: '/brand-library', status: 'Live' },
        { id: 'deal-compare', label: 'Deal Compare', iconKey: 'scale', href: '/deal-compare', status: 'Beta' }
      ]
    };
  }

  window.DashboardAdapter = {
    getDashboardViewModel: getDashboardViewModel,
    computeResponseTimeCategory: computeResponseTimeCategory,
    computeResponseFrequencyCategory: computeResponseFrequencyCategory,
    mapResponsivenessScoresToWidget: mapResponsivenessScoresToWidget,
    mapResponsivenessInteractionsToAlerts: mapResponsivenessInteractionsToAlerts
  };
})();
