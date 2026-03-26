/**
 * Dealality Command Center – Original version
 * Renders: Header, KPI strip, Signals Today, Next Actions, Execution Reliability,
 * Pipeline Snapshot, Market Intelligence, Deal Toolbox
 */

(function () {
  'use strict';

  function escapeHtml(s) {
    if (s == null) return '';
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  function escapeAttr(s) {
    if (s == null) return '';
    return String(s).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function renderHeader(vm) {
    if (!vm || !vm.header) return;
    var h = vm.header;
    var greetingEl = document.getElementById('dc-greeting');
    if (greetingEl) greetingEl.textContent = h.greeting || '';
    var syncEl = document.getElementById('dc-sync');
    if (syncEl) syncEl.textContent = (h.lastSync ? '· ' + h.lastSync : '');
    var actionsEl = document.getElementById('dc-header-actions');
    if (actionsEl && h.ctas && h.ctas.length) {
      actionsEl.innerHTML = h.ctas.map(function (c) {
        return '<a href="' + escapeAttr(c.href) + '" class="dc-btn' + (c.primary ? ' dc-btn--primary' : '') + '">' + escapeHtml(c.label) + '</a>';
      }).join('');
    }
  }

  function renderKPIStrip(vm) {
    var kpis = vm && vm.kpis;
    var el = document.getElementById('dc-kpi-strip');
    if (!el) return;
    if (!kpis || !kpis.length) {
      el.innerHTML = '';
      return;
    }
    var html = kpis.map(function (k) {
      var badgeClass = k.trend === 'up' ? 'positive' : k.trend === 'down' ? 'negative' : '';
      var badgeArrow = k.trend === 'up' ? '▲ ' : k.trend === 'down' ? '▼ ' : '';
      var cardClass = 'dc-kpi-card' + (k.id === 'needs-action' ? ' dc-kpi-card--highlight' : '');
      return '<div class="' + cardClass + '">' +
        '<span class="dc-kpi-card__name">' + escapeHtml(k.label) + '</span>' +
        '<span class="dc-kpi-card__value">' + escapeHtml(String(k.value)) + '</span>' +
        '<span class="dc-kpi-card__badge ' + badgeClass + '">' + badgeArrow + escapeHtml(k.subtext || '—') + '</span>' +
        '</div>';
    }).join('');
    el.innerHTML = html;
  }

  function renderSignals(vm) {
    var signals = vm && vm.signalsToday;
    var el = document.getElementById('dc-signals');
    if (!el) return;
    if (!signals || !signals.length) {
      el.innerHTML = '<p class="dc-empty">No signals today.</p>';
      return;
    }
    var html = signals.map(function (s) {
      return '<div class="dc-signal">' +
        '<div class="dc-signal__row">' +
        '<span class="dc-signal__tag dc-signal__tag--' + escapeAttr(s.tagClass || 'watch') + '">' + escapeHtml(s.tag) + '</span>' +
        '<span class="dc-signal__headline">' + escapeHtml(s.headline) + '</span>' +
        '<a href="' + escapeAttr(s.href || '#') + '" class="dc-link">' + escapeHtml(s.cta || 'View') + '</a>' +
        '</div>' +
        (s.why ? '<p class="dc-signal__why">' + escapeHtml(s.why) + '</p>' : '') +
        '</div>';
    }).join('');
    el.innerHTML = html;
  }

  function renderNextActions(vm) {
    var actions = vm && vm.nextActions;
    var el = document.getElementById('dc-next-actions');
    if (!el) return;
    if (!actions || !actions.length) {
      el.innerHTML = '<p class="dc-empty">No upcoming actions.</p>';
      return;
    }
    var html = actions.map(function (a) {
      return '<div class="dc-action-item">' +
        '<div class="dc-action-item__top">' +
        '<span class="dc-action-item__title">' + escapeHtml(a.title) + '</span>' +
        '<span class="dc-action-item__due">' + escapeHtml(a.due) + '</span>' +
        '</div>' +
        '<div class="dc-action-item__deal">' + escapeHtml(a.deal) +
        (a.severity ? ' <span class="dc-action-item__tag dc-action-item__tag--' + escapeAttr(a.severity) + '">' + escapeHtml(a.severity) + '</span>' : '') +
        '</div>' +
        '<a href="' + escapeAttr(a.href) + '" class="dc-action-item__cta">Open</a>' +
        '</div>';
    }).join('');
    el.innerHTML = html;
  }

  function renderResponsiveness(vm) {
    var r = vm && vm.responsivenessSummary;
    var el = document.getElementById('dc-responsiveness');
    if (!el) return;
    if (!r) {
      el.innerHTML = '';
      return;
    }
    var score = r.gaugeScore != null ? r.gaugeScore : r.responseFrequencyPercent || 0;
    var trendClass = r.trend === 'up' ? 'dc-exec-gauge__trend--up' : r.trend === 'down' ? 'dc-exec-gauge__trend--down' : '';
    el.innerHTML = '<div class="dc-exec-gauge">' +
      '<div class="dc-exec-gauge__score">' + escapeHtml(String(score)) + '%</div>' +
      '<div class="dc-exec-gauge__bar"><div class="dc-exec-gauge__fill" style="width:' + score + '%"></div></div>' +
      '<div class="dc-exec-gauge__meta ' + trendClass + '">' + escapeHtml(r.trendLabel || r.combinedBadge || '') + '</div>' +
      '</div>';
  }

  function renderPipeline(vm) {
    var snap = vm && vm.pipelineSnapshot;
    var el = document.getElementById('dc-pipeline');
    if (!el) return;
    if (!snap || !snap.stages || !snap.stages.length) {
      el.innerHTML = '';
      return;
    }
    var html = '<div class="dc-pipeline-stages">' + snap.stages.map(function (s) {
      var meta = [];
      if (s.newThisWeek) meta.push('New: ' + escapeHtml(String(s.newThisWeek)));
      if (s.advancedThisWeek) meta.push('Adv: ' + escapeHtml(String(s.advancedThisWeek)));
      if (s.stalledThisWeek) meta.push('<span class="dc-m--warn">Stalled: ' + escapeHtml(String(s.stalledThisWeek)) + '</span>');
      return '<div class="dc-pipeline-stage">' +
        '<span class="dc-pipeline-stage__name">' + escapeHtml(s.label) + '</span>' +
        '<span class="dc-pipeline-stage__value">' + escapeHtml(String(s.count)) + '</span>' +
        '<span class="dc-pipeline-stage__meta">' + (meta.length ? meta.join(', ') : '—') + '</span>' +
        '</div>';
    }).join('') + '</div>';
    el.innerHTML = html;
  }

  function renderMarketIntel(vm) {
    var intel = vm && vm.marketIntelligence;
    var el = document.getElementById('dc-market-intel');
    if (!el) return;
    if (!intel || !intel.length) {
      el.innerHTML = '<p class="dc-empty">No market intel.</p>';
      return;
    }
    var html = intel.map(function (i) {
      return '<div class="dc-market-item">' +
        '<span class="dc-market-item__tag">' + escapeHtml(i.tag) + '</span>' +
        '<div class="dc-market-item__headline">' + escapeHtml(i.headline) + '</div>' +
        (i.why ? '<p class="dc-market-item__why">' + escapeHtml(i.why) + '</p>' : '') +
        '<a href="' + escapeAttr(i.href || '#') + '" class="dc-market-item__link">View</a>' +
        '</div>';
    }).join('');
    el.innerHTML = html;
  }

  function renderToolbox(vm) {
    var links = vm && vm.toolboxLinks;
    var el = document.getElementById('dc-toolbox');
    if (!el) return;
    if (!links || !links.length) {
      el.innerHTML = '';
      return;
    }
    var html = '<div class="dc-toolbox-list">' + links.map(function (t) {
      var statusClass = t.status === 'Live' ? 'live' : t.status === 'Beta' ? 'beta' : 'soon';
      return '<a href="' + escapeAttr(t.href) + '" class="dc-toolbox-item">' +
        '<span class="dc-toolbox-item__icon">' + escapeHtml(t.icon || '•') + '</span>' +
        '<span>' + escapeHtml(t.label) + '</span>' +
        '<span class="dc-toolbox-item__status dc-toolbox-item__status--' + statusClass + '">' + escapeHtml(t.status) + '</span>' +
        '</a>';
    }).join('') + '</div>';
    el.innerHTML = html;
  }

  function init() {
    var vm = window.DashboardAdapter && window.DashboardAdapter.getDashboardViewModel
      ? window.DashboardAdapter.getDashboardViewModel()
      : null;

    if (!vm) {
      var container = document.getElementById('dc-dashboard');
      if (container) container.innerHTML = '<div class="dc-card"><div class="dc-card__body dc-empty">Unable to load dashboard data.</div></div>';
      return;
    }

    renderHeader(vm);
    renderKPIStrip(vm);
    renderSignals(vm);
    renderNextActions(vm);
    renderResponsiveness(vm);
    renderPipeline(vm);
    renderMarketIntel(vm);
    renderToolbox(vm);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
