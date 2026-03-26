/**
 * Dealality Command Center – Redesigned components
 * Hero metric, trend chart, funnel, Today Focus, Next Actions, Signals+Intel merged.
 */

(function () {
  'use strict';

  var MAX_NEXT_ACTIONS = 6;
  var MAX_SIGNALS_MARKET = 6;
  var MAX_RECENT_ACTIVITY = 12;
  var MAX_MARKET_INTEL = 8;
  var ROLE_STORAGE_KEY = 'dc_dashboard_role_view';
  var trendChartInstance = null;
  var regionalDistChartInstance = null;
  var marketIntelTicker = {
    intervalId: null,
    rafId: null,
    pauseHover: false,
    userPaused: false
  };

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

  function isExternalHref(href) {
    if (!href) return false;
    return /^https?:\/\//i.test(String(href));
  }

  function linkTargetAttrs(href) {
    return isExternalHref(href) ? ' target="_blank" rel="noopener noreferrer"' : '';
  }

  function toShellHref(href) {
    if (!href) return '#';
    var s = String(href).trim();
    if (!s || s === '#') return '#';
    if (/^https?:\/\//i.test(s)) return s;
    if (s.indexOf('/app#') === 0) return s;
    if (s.charAt(0) !== '/') s = '/' + s;
    return '/app#' + s;
  }

  function timeAgoFromDate(dateStr) {
    if (!dateStr) return '';
    var d = new Date(dateStr);
    if (isNaN(d.getTime())) return '';
    var now = new Date();
    var diff = now - d;
    if (diff < 60000) return 'Just now';
    if (diff < 3600000) return Math.floor(diff / 60000) + 'm ago';
    if (diff < 86400000) return Math.floor(diff / 3600000) + 'h ago';
    if (diff < 172800000) return 'Yesterday';
    if (diff < 604800000) return Math.floor(diff / 86400000) + 'd ago';
    return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
  }

  function mapMarketAlertsToIntel(items) {
    if (!Array.isArray(items)) return [];
    return items.map(function (row) {
      var f = row && row.fields ? row.fields : {};
      var title = f['Title'] || f.title || 'Market alert';
      var summary = f['Summary'] || f.summary || '';
      var category = f['Category'] || f.category || 'Market News';
      var publishedAt = f['Published At'] || f.publishedAt || null;
      var sourceUrl = f['Source URL'] || f.sourceUrl || '/market-alerts';
      return {
        id: row.id || title,
        type: category,
        tag: category,
        title: title,
        headline: title,
        subtitle: summary,
        why: summary,
        time: timeAgoFromDate(publishedAt),
        timeAgo: timeAgoFromDate(publishedAt),
        href: sourceUrl || '/market-alerts',
        ctaHref: sourceUrl || '/market-alerts'
      };
    }).filter(function (x) { return !!x.title; });
  }

  function fetchMarketIntelFromAlerts() {
    return fetch('/api/market-alerts?timeWindow=7d&limit=20')
      .then(function (res) {
        if (!res.ok) throw new Error('alerts unavailable');
        return res.json();
      })
      .then(function (json) {
        return mapMarketAlertsToIntel((json && json.items) || []);
      })
      .catch(function () {
        return null;
      });
  }

  function fetchRecentDealActivity() {
    return fetch('/api/outreach/deal-activity-log?limit=20')
      .then(function (res) {
        if (!res.ok) throw new Error('deal activity unavailable');
        return res.json();
      })
      .then(function (json) {
        if (!json || json.success === false || !Array.isArray(json.entries)) return null;
        return json.entries.map(function (e) {
          return {
            type: e.type || 'deal',
            title: e.title || e.action || 'Activity Updated',
            contextLabel: e.contextLabel || e.dealName || 'Untitled Deal',
            stakeholder: e.stakeholder || '',
            timeAgo: e.timeAgo || '',
            badgeLabel: e.badgeLabel || 'Deal Activity',
            badgeType: e.badgeType || 'info',
            ctaHref: e.ctaHref || '/outreach-deal-activity-log'
          };
        });
      })
      .catch(function () {
        return null;
      });
  }

  function renderHeader(vm) {
    if (!vm || !vm.header) return;
    var h = vm.header;
    var syncEl = document.getElementById('dc-sync');
    if (syncEl) syncEl.textContent = (h.lastSyncLabel ? 'Data Last Updated · ' + h.lastSyncLabel : 'Data Last Updated');
    var actionsEl = document.getElementById('dc-header-actions');
    if (actionsEl && h.ctas && h.ctas.length) {
      actionsEl.innerHTML = h.ctas.map(function (c) {
        return '<a href="' + escapeAttr(c.href) + '" class="dc-btn' + (c.primary ? ' dc-btn--primary' : '') + '">' + escapeHtml(c.label) + '</a>';
      }).join('');
    }
  }

  function renderHeroMetric(vm) {
    var hero = vm && vm.heroMetric;
    var el = document.getElementById('dc-hero-metric');
    if (!el) return;
    if (!hero) {
      el.innerHTML = '';
      return;
    }
    var trendClass = hero.trend === 'up' ? 'dc-hero__metric-context--up' : hero.trend === 'down' ? 'dc-hero__metric-context--down' : '';
    el.innerHTML = '<div class="dc-hero__metric-label">' + escapeHtml(hero.label) + '</div>' +
      '<div class="dc-hero__metric-value">' + escapeHtml(String(hero.value)) + '</div>' +
      '<div class="dc-hero__metric-context ' + trendClass + '">' + escapeHtml(hero.context || '') + '</div>';
  }

  function renderTrendChart(vm) {
    var loiData = vm && vm.loiDealVolumeChartData;
    var canvas = document.getElementById('dc-trend-chart-canvas');
    if (!canvas) return;

    if (trendChartInstance) {
      trendChartInstance.destroy();
      trendChartInstance = null;
    }

    var ctx = canvas.getContext('2d');
    var data = loiData || (vm && vm.trendChartData);

    if (!data) return;

    if (loiData && loiData.labels && loiData.franchiseDeals && loiData.managementDeals) {
      var shareEl = document.getElementById('dc-loi-share');
      var sharePcts = [];
      if (loiData.regionFranchiseDeals && loiData.regionManagementDeals) {
        for (var i = 0; i < loiData.franchiseDeals.length; i++) {
          var myTotal = (loiData.franchiseDeals[i] || 0) + (loiData.managementDeals[i] || 0);
          var regionTotal = (loiData.regionFranchiseDeals[i] || 0) + (loiData.regionManagementDeals[i] || 0);
          sharePcts.push(regionTotal > 0 ? Math.round((myTotal / regionTotal) * 100) : 0);
        }
        if (shareEl && sharePcts.length > 0) {
          var latestShare = sharePcts[sharePcts.length - 1];
          shareEl.textContent = '(REGIONAL SHARE: ' + latestShare + '%)';
        }
      } else if (shareEl) {
        shareEl.textContent = '';
      }

      var datasets = [
        {
          type: 'bar',
          label: 'My Franchise Deals',
          data: loiData.franchiseDeals,
          backgroundColor: 'rgba(154, 145, 251, 0.6)',
          borderColor: '#9a91fb',
          borderWidth: 1,
          yAxisID: 'y',
          order: 1
        },
        {
          type: 'bar',
          label: 'My Management Deals',
          data: loiData.managementDeals,
          backgroundColor: 'rgba(87, 195, 255, 0.6)',
          borderColor: '#57c3ff',
          borderWidth: 1,
          yAxisID: 'y',
          order: 1
        }
      ];
      if (loiData.regionFranchiseDeals && loiData.regionManagementDeals) {
        datasets.push(
          {
            type: 'line',
            label: 'Region Franchise Deals',
            data: loiData.regionFranchiseDeals,
            borderColor: '#f59e0b',
            backgroundColor: 'rgba(245, 158, 11, 0.1)',
            fill: true,
            tension: 0.3,
            borderWidth: 2,
            borderDash: [5, 5],
            pointRadius: 3,
            pointHoverRadius: 6,
            pointBackgroundColor: '#f59e0b',
            pointBorderColor: '#fff',
            pointBorderWidth: 1,
            yAxisID: 'y1',
            order: 0
          },
          {
            type: 'line',
            label: 'Region Management Deals',
            data: loiData.regionManagementDeals,
            borderColor: '#14b8a6',
            backgroundColor: 'rgba(20, 184, 166, 0.1)',
            fill: true,
            tension: 0.3,
            borderWidth: 2,
            borderDash: [5, 5],
            pointRadius: 3,
            pointHoverRadius: 6,
            pointBackgroundColor: '#14b8a6',
            pointBorderColor: '#fff',
            pointBorderWidth: 1,
            yAxisID: 'y1',
            order: 0
          }
        );
      }
      trendChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
          labels: loiData.labels,
          datasets: datasets
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: 'index', intersect: false },
          plugins: {
            legend: { display: false },
            tooltip: {
              backgroundColor: 'rgba(16, 25, 53, 0.95)',
              titleColor: '#e2e8f0',
              bodyColor: '#94a3b8',
              borderColor: 'rgba(126, 137, 172, 0.3)',
              borderWidth: 1,
              padding: 12,
              callbacks: {
                label: function (ctx) {
                  var base = (ctx.dataset.label || '') + ': ' + ctx.parsed.y + ' deals';
                  if (ctx.dataIndex < sharePcts.length && (ctx.dataset.label === 'My Franchise Deals' || ctx.dataset.label === 'My Management Deals')) {
                    var myTot = (loiData.franchiseDeals[ctx.dataIndex] || 0) + (loiData.managementDeals[ctx.dataIndex] || 0);
                    var regTot = (loiData.regionFranchiseDeals && loiData.regionManagementDeals)
                      ? ((loiData.regionFranchiseDeals[ctx.dataIndex] || 0) + (loiData.regionManagementDeals[ctx.dataIndex] || 0)) : 0;
                    if (regTot > 0) base += ' (' + Math.round((myTot / regTot) * 100) + '% of region)';
                  }
                  return base;
                }
              }
            }
          },
          scales: {
            x: {
              grid: { display: false, drawBorder: false },
              ticks: { color: '#7e89ac', font: { size: 11 } }
            },
            y: {
              type: 'linear',
              position: 'left',
              beginAtZero: true,
              grid: { color: 'rgba(126, 137, 172, 0.1)', drawBorder: false },
              ticks: {
                color: '#7e89ac',
                font: { size: 11 },
                callback: function (v) { return v + ' deals'; },
                stepSize: 10
              }
            },
            y1: {
              type: 'linear',
              position: 'right',
              beginAtZero: true,
              grid: { drawOnChartArea: false },
              ticks: {
                color: '#7e89ac',
                font: { size: 11 },
                callback: function (v) { return v + ' deals'; },
                stepSize: 50
              }
            }
          }
        }
      });
    } else {
      var oldData = data;
      trendChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
          labels: oldData.labels,
          datasets: [{
            label: 'Needs Action',
            data: oldData.values,
            borderColor: '#c53030',
            backgroundColor: 'rgba(197, 48, 48, 0.15)',
            fill: true,
            tension: 0.3,
            pointRadius: 3,
            pointHoverRadius: 5
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: {
              grid: { color: 'rgba(255,255,255,0.06)' },
              ticks: { color: '#94a3b8', font: { size: 10 } }
            },
            y: {
              grid: { color: 'rgba(255,255,255,0.06)' },
              ticks: { color: '#94a3b8', font: { size: 10 } },
              beginAtZero: true
            }
          }
        }
      });
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
    var atRiskTooltip = 'Deals where the counterparty has not responded in 3+ days or the thread is stalled. We define at-risk as any deal with no reply from the other party within this threshold.';
    var html = kpis.map(function (k, i) {
      var dt = k.deltaType || k.trend;
      var badgeClass = dt === 'up' ? 'positive' : dt === 'down' ? 'negative' : 'neutral';
      var badgeArrow = dt === 'up' ? '▲' : dt === 'down' ? '▼' : '';
      var isAtRisk = (k.key || k.id) === 'at-risk';
      var cardClass = 'dc-insight-card' + ((k.key || k.id) === 'needs-action' ? ' highlight' : '') + (isAtRisk ? ' dc-insight-card--at-risk' : '');
      var deltaLabel = k.deltaLabel != null ? k.deltaLabel : (k.subtext || '—');
      var displayLabel = isAtRisk ? 'AT RISK' : k.label;
      var labelHtml = '<span class="dc-insight-card__name">' + escapeHtml(displayLabel) + '</span>';
      var infoBlock = isAtRisk ? '<span class="dc-kpi-info-wrap dc-kpi-info-wrap--card"><button type="button" class="dc-exec-why-btn dc-exec-why-btn--kpi" id="dc-at-risk-why-btn" aria-label="' + escapeAttr(atRiskTooltip) + '">ℹ</button></span>' : '';
      return '<div class="' + cardClass + '">' +
        '<div class="dc-insight-card__header">' + labelHtml + '</div>' +
        infoBlock +
        '<div class="dc-insight-card__body">' +
        '<span class="dc-insight-card__value">' + escapeHtml(String(k.value)) + '</span>' +
        '<span class="dc-insight-card__badge ' + badgeClass + '">' + (badgeArrow ? badgeArrow + ' ' : '') + escapeHtml(deltaLabel) + '</span>' +
        '</div></div>';
    }).join('');
    el.innerHTML = html;

    var atRiskBtn = document.getElementById('dc-at-risk-why-btn');
    var atRiskModal = document.getElementById('dc-at-risk-why-modal');
    var atRiskBody = document.getElementById('dc-at-risk-why-modal-body');
    var atRiskClose = document.getElementById('dc-at-risk-why-modal-close');
    if (atRiskBtn && atRiskModal && atRiskBody) {
      atRiskBody.textContent = atRiskTooltip;
      atRiskBtn.onclick = function () {
        atRiskModal.hidden = false;
        atRiskModal.classList.add('dc-modal--open');
      };
    }
    if (atRiskClose && atRiskModal) {
      function closeAtRiskModal() {
        atRiskModal.hidden = true;
        atRiskModal.classList.remove('dc-modal--open');
      }
      atRiskClose.onclick = closeAtRiskModal;
      var atRiskBackdrop = atRiskModal && atRiskModal.querySelector('.dc-modal__backdrop');
      if (atRiskBackdrop) atRiskBackdrop.onclick = closeAtRiskModal;
      document.addEventListener('keydown', function onAtRiskModalKey(e) {
        if (e.key === 'Escape' && atRiskModal.classList.contains('dc-modal--open')) closeAtRiskModal();
      });
    }
  }

  function renderTodayFocus(vm) {
    var chips = vm && vm.todayFocus;
    var el = document.getElementById('dc-today-focus-body');
    if (!el) return;
    if (!chips || !chips.length) {
      el.innerHTML = '<p class="dc-activity-log__empty">Nothing urgent. <a href="/my-deals" class="dc-link">View deals</a></p>';
      return;
    }
    var html = '<div class="dc-focus-chips">' + chips.map(function (c) {
      var chipClass = 'dc-focus-chip' + (c.variant ? ' dc-focus-chip--' + c.variant : '');
      return '<a href="' + escapeAttr(c.href) + '" class="' + chipClass + '">' +
        '<span class="dc-focus-chip__count">' + escapeHtml(String(c.count)) + '</span>' +
        escapeHtml(c.icon || '') + ' ' + escapeHtml(c.label) +
        '</a>';
    }).join('') + '</div>';
    el.innerHTML = html;
  }

  function renderNextActions(vm) {
    var actions = vm && vm.nextActions;
    var el = document.getElementById('dc-next-actions');
    if (!el) return;
    if (!actions || !actions.length) {
      el.innerHTML = '<p class="dc-activity-log__empty">No upcoming actions. <a href="/my-deals" class="dc-link">Create follow-ups</a></p>';
      return;
    }
    var items = actions.slice(0, MAX_NEXT_ACTIONS);
    var html = items.map(function (a) {
      var p = a.priority || a.severity;
      var tagClass = (p === 'urgent' || p === 'high') ? 'risk' : p === 'medium' ? 'watch' : 'trend';
      var tag = p ? (p === 'urgent' || p === 'high' ? 'Urgent' : p === 'medium' ? 'Medium' : 'Action') : 'Action';
      var why = (a.contextLabel || a.deal || '') + (a.dueLabel && a.dueLabel !== '—' ? ' · Due ' + a.dueLabel : '');
      return '<div class="dc-signal">' +
        '<div class="dc-signal__row">' +
        '<span class="dc-signal__tag dc-signal__tag--' + escapeAttr(tagClass) + '">' + escapeHtml(tag) + '</span>' +
        '<span class="dc-signal__headline">' + escapeHtml(toTitleCase(a.title)) + '</span>' +
        '<a href="' + escapeAttr(a.ctaHref || a.href || '#') + '" class="dc-link">' + escapeHtml(a.ctaLabel || 'Open') + '</a>' +
        '</div>' +
        '<p class="dc-signal__why">' + escapeHtml(why) + '</p>' +
        '</div>';
    }).join('');
    el.innerHTML = html;
  }

  function renderPipelineFunnel(vm) {
    var pipeline = vm && vm.pipeline;
    if (!pipeline && vm && vm.pipelineSnapshot && vm.pipelineSnapshot.stages) {
      pipeline = vm.pipelineSnapshot.stages.map(function (s) {
        var metaBadges = [];
        if (s.newThisWeek) metaBadges.push({ label: 'New: ' + s.newThisWeek, type: 'info' });
        if (s.advancedThisWeek) metaBadges.push({ label: 'Adv: ' + s.advancedThisWeek, type: 'info' });
        if (s.stalledThisWeek) metaBadges.push({ label: 'Stalled: ' + s.stalledThisWeek, type: 'warn' });
        return { stageKey: s.id, label: s.label, count: s.count, metaBadges: metaBadges };
      });
    }
    var el = document.getElementById('dc-pipeline');
    if (!el) return;
    if (!pipeline || !pipeline.length) {
      el.innerHTML = '';
      return;
    }
    var html = pipeline.map(function (s) {
      var displayLabel = (s.stageKey === 'engaged' || (s.label && s.label.toLowerCase() === 'engaged')) ? 'Bid Submitted' : s.label;
      var metaParts = [];
      if (s.metaBadges && s.metaBadges.length) {
        metaParts = s.metaBadges.map(function (b) {
          var cls = b.type === 'warn' ? 'dc-m--warn' : '';
          return cls ? '<span class="' + cls + '">' + escapeHtml(b.label) + '</span>' : escapeHtml(b.label);
        });
      }
      var meta = metaParts.length ? metaParts.join(', ') : '—';
      var labelHtml = displayLabel && displayLabel.indexOf('|') !== -1
        ? displayLabel.split('|').map(function (part) { return escapeHtml(part.trim()); }).join('<br>')
        : escapeHtml(displayLabel);
      return '<div class="dc-pipeline-stage">' +
        '<div class="dc-pipeline-stage__header">' +
        '<span class="dc-pipeline-stage__name">' + labelHtml + '</span>' +
        '</div>' +
        '<div class="dc-pipeline-stage__body">' +
        '<span class="dc-pipeline-stage__value">' + escapeHtml(String(s.count)) + '</span>' +
        '<span class="dc-pipeline-stage__meta">' + meta + '</span>' +
        '</div></div>';
    }).join('');
    el.innerHTML = html;
  }

  function renderSignals(vm) {
    var signals = vm && (vm.signals || vm.signalsToday) || [];
    var items = signals.slice(0, MAX_SIGNALS_MARKET);
    var el = document.getElementById('dc-signals');
    if (!el) return;
    if (!items.length) {
      el.innerHTML = '<p class="dc-activity-log__empty">No signals. <a href="/market-alerts" class="dc-link">View market alerts</a></p>';
      return;
    }
    var typeToTag = { risk: 'Risk', opportunity: 'Opportunity', watch: 'Watch', trend: 'Trend', action: 'Action' };
    var typeToClass = { risk: 'risk', opportunity: 'opportunity', watch: 'watch', trend: 'trend', action: 'trend' };
    var html = items.map(function (s) {
      var tag = s.tag || typeToTag[s.type] || 'Watch';
      var tagClass = s.tagClass || typeToClass[s.type] || 'watch';
      return '<div class="dc-signal">' +
        '<div class="dc-signal__row">' +
        '<span class="dc-signal__tag dc-signal__tag--' + escapeAttr(tagClass) + '">' + escapeHtml(tag) + '</span>' +
        '<span class="dc-signal__headline">' + escapeHtml(toTitleCase(s.title || s.headline)) + '</span>' +
        '<a href="' + escapeAttr(s.ctaHref || s.href || '#') + '" class="dc-link">' + escapeHtml(s.ctaLabel || s.cta || 'View') + '</a>' +
        '</div>' +
        ((s.subtitle || s.why) ? '<p class="dc-signal__why">' + escapeHtml(s.subtitle || s.why) + '</p>' : '') +
        '</div>';
    }).join('');
    el.innerHTML = html;
  }

  function marketIntelIconForTag(tag) {
    if (!tag) return 'dc-activity-feed__icon--default';
    var t = String(tag).toLowerCase();
    if (t.indexOf('brand') !== -1) return 'dc-activity-feed__icon--deal';
    if (t.indexOf('supply') !== -1 || t.indexOf('airport') !== -1) return 'dc-activity-feed__icon--market';
    return 'dc-activity-feed__icon--news';
  }

  function stopMarketIntelAutoScroll() {
    if (marketIntelTicker.intervalId) {
      window.clearInterval(marketIntelTicker.intervalId);
      marketIntelTicker.intervalId = null;
    }
    if (marketIntelTicker.rafId) {
      window.cancelAnimationFrame(marketIntelTicker.rafId);
      marketIntelTicker.rafId = null;
    }
  }

  function updateMarketIntelToggleButton() {
    var btn = document.getElementById('dc-market-intel-toggle');
    if (!btn) return;
    var paused = !!marketIntelTicker.userPaused;
    btn.textContent = paused ? 'Start Scroll' : 'Pause Scroll';
    btn.setAttribute('aria-pressed', paused ? 'true' : 'false');
  }

  function startMarketIntelAutoScroll(container) {
    stopMarketIntelAutoScroll();
    if (!container) return;
    var feed = container.querySelector('.dc-activity-feed');
    if (!feed) return;
    var items = Array.prototype.slice.call(feed.querySelectorAll('.dc-activity-feed__item'));
    if (!items || items.length < 2) return;
    if (container.scrollHeight <= container.clientHeight + 4) return;

    var itemOffsets = items.map(function (item) {
      return item.offsetTop;
    });
    var currentIndex = 0;
    container.scrollTop = itemOffsets[0] || 0;

    function scrollToIndex(nextIndex) {
      currentIndex = nextIndex >= itemOffsets.length ? 0 : nextIndex;
      container.scrollTo({
        top: itemOffsets[currentIndex] || 0,
        behavior: 'smooth'
      });
    }

    marketIntelTicker.intervalId = window.setInterval(function () {
      if (marketIntelTicker.userPaused || marketIntelTicker.pauseHover || document.hidden) return;
      marketIntelTicker.rafId = window.requestAnimationFrame(function () {
        scrollToIndex(currentIndex + 1);
      });
    }, 3200);

    container.onmouseenter = function () { marketIntelTicker.pauseHover = true; };
    container.onmouseleave = function () { marketIntelTicker.pauseHover = false; };
    container.onfocusin = function () { marketIntelTicker.pauseHover = true; };
    container.onfocusout = function () { marketIntelTicker.pauseHover = false; };
  }

  function renderMarketIntel(vm) {
    stopMarketIntelAutoScroll();
    var intel = vm && (vm.marketIntel || vm.marketIntelligence) || [];
    var items = intel.slice(0, MAX_MARKET_INTEL);
    var el = document.getElementById('dc-market-intel');
    if (!el) return;
    if (!items.length) {
      el.innerHTML = '<p class="dc-activity-log__empty">No Market Intelligence. <a href="/market-alerts" class="dc-link">View market alerts</a></p>';
      return;
    }
    var html = '<ul class="dc-activity-feed">' + items.map(function (i) {
      var tag = i.type || i.tag;
      var iconClass = 'dc-activity-feed__icon ' + marketIntelIconForTag(tag);
      var iconSvg = activityIconMap.building;
      if ((tag || '').toLowerCase().indexOf('supply') !== -1 || (tag || '').toLowerCase().indexOf('airport') !== -1) iconSvg = activityIconMap['trending-up'];
      else if ((tag || '').toLowerCase().indexOf('financing') !== -1) iconSvg = activityIconMap.clipboard;
      var href = i.ctaHref || i.href;
      var linkStart = href ? '<a href="' + escapeAttr(href) + '" class="dc-activity-feed__link"' + linkTargetAttrs(href) + '>' : '';
      var linkEnd = href ? '</a>' : '';
      return '<li class="dc-activity-feed__item">' +
        '<span class="dc-activity-feed__line" aria-hidden="true"></span>' +
        '<span class="' + iconClass + '" aria-hidden="true" title="' + escapeAttr(tag || '') + '">' + iconSvg + '</span>' +
        '<div class="dc-activity-feed__content">' + linkStart +
        '<span class="dc-activity-feed__time">' + escapeHtml(i.timeAgo || i.time || '') + '</span>' +
        '<div class="dc-activity-feed__event">' + escapeHtml(toTitleCase(i.title || i.headline)) + '</div>' +
        (tag ? '<span class="dc-activity-feed__tag">' + escapeHtml(String(tag).charAt(0).toUpperCase() + String(tag).slice(1)) + '</span>' : '') +
        ((i.subtitle || i.why) ? '<div class="dc-activity-feed__deal">' + escapeHtml(i.subtitle || i.why) + '</div>' : '') +
        linkEnd + '</div></li>';
    }).join('') + '</ul>';
    el.innerHTML = html;
    startMarketIntelAutoScroll(el);
  }

  function toTitleCase(s) {
    if (s == null || s === '') return '';
    var small = ['a','an','the','and','or','but','in','on','of','to','for','with','from','by','at','as','is','it'];
    return String(s).split(' ').map(function (word, i, arr) {
      var lower = word.toLowerCase();
      if (word.length >= 2 && word === word.toUpperCase()) return word; /* preserve acronyms: CALA, RevPAR, etc. */
      if (small.indexOf(lower) !== -1 && i !== 0 && i !== arr.length - 1) return lower;
      return lower.charAt(0).toUpperCase() + lower.slice(1);
    }).join(' ');
  }

  var activityIconMap = {
    message: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>',
    file: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/><path d="M16 13H8"/><path d="M16 17H8"/></svg>',
    'trending-up': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M23 6l-9.5 9.5-5-5L1 18"/><path d="M17 6h6v6"/></svg>',
    mail: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><path d="M22 6l-10 7L2 6"/></svg>',
    eye: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>',
    building: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 21h18"/><path d="M9 8h1"/><path d="M9 12h1"/><path d="M9 16h1"/><path d="M14 8h1"/><path d="M14 12h1"/><path d="M14 16h1"/><path d="M5 21V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16"/></svg>',
    pin: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>',
    clipboard: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><path d="M15 2H9a1 1 0 0 0-1 1v2a1 1 0 0 0 1 1h6a1 1 0 0 0 1-1V3a1 1 0 0 0-1-1z"/><path d="M16 13H8"/><path d="M16 17H8"/></svg>'
  };

  function iconClassForType(type) {
    return type === 'deal' ? 'dc-activity-feed__icon--deal' :
           type === 'market' ? 'dc-activity-feed__icon--market' :
           type === 'news' ? 'dc-activity-feed__icon--news' : 'dc-activity-feed__icon--default';
  }

  function renderRecentActivity(vm) {
    var activities = vm && vm.recentActivity;
    var el = document.getElementById('dc-recent-activity');
    if (!el) return;
    if (!activities || !activities.length) {
      el.innerHTML = '<p class="dc-activity-log__empty">No recent activity.</p>';
      return;
    }
    var items = activities.slice(0, MAX_RECENT_ACTIVITY);
    var html = '<ul class="dc-activity-feed">' + items.map(function (a) {
      var iconClass = 'dc-activity-feed__icon ' + iconClassForType(a.type || 'deal');
      var rawHref = a.ctaHref || a.href;
      var href = toShellHref(rawHref);
      var targetAttrs = /^\/app#/.test(href)
        ? ' target="_top"'
        : linkTargetAttrs(href);
      var linkStart = href ? '<a href="' + escapeAttr(href) + '" class="dc-activity-feed__link"' + targetAttrs + '>' : '';
      var linkEnd = href ? '</a>' : '';
      var iconSvg = activityIconMap[a.iconKey || 'message'] || activityIconMap.message;
      var stakeholder = a.stakeholder ? String(a.stakeholder).trim() : '';
      var tagText = (a.badgeLabel || a.tag) ? toTitleCase(a.badgeLabel || a.tag) : '';
      if (stakeholder) tagText = tagText ? (tagText + ' · ' + stakeholder) : stakeholder;
      return '<li class="dc-activity-feed__item">' +
        '<span class="dc-activity-feed__line" aria-hidden="true"></span>' +
        '<span class="' + iconClass + '" aria-hidden="true" title="' + escapeAttr(a.badgeLabel || a.tag || '') + '">' + iconSvg + '</span>' +
        '<div class="dc-activity-feed__content">' + linkStart +
        '<span class="dc-activity-feed__time">' + escapeHtml(a.timeAgo || a.time || '') + '</span>' +
        '<div class="dc-activity-feed__event">' + escapeHtml(toTitleCase(a.title || a.event)) + '</div>' +
        (tagText ? '<span class="dc-activity-feed__tag">' + escapeHtml(tagText) + '</span>' : '') +
        ((a.contextLabel || a.deal) ? '<div class="dc-activity-feed__deal">' + escapeHtml(toTitleCase(a.contextLabel || a.deal)) + '</div>' : '') +
        linkEnd + '</div></li>';
    }).join('') + '</ul>';
    el.innerHTML = html;
  }

  function renderResponsiveness(vm) {
    var r = vm && vm.responsivenessSummary;
    var er = vm && vm.executionReliability;
    var el = document.getElementById('dc-responsiveness');
    if (!el) return;
    if (!r && !er) {
      el.innerHTML = '';
      return;
    }
    var combinedBadge = er ? ((er.badgeIcon || '') + ' ' + (er.badgeLabel || '')).trim() : (r && r.combinedBadge);
    var avgHours = er ? er.avgFirstResponseHours : (r && r.avgFirstResponseTimeHours);
    var freqPct = er ? er.responseFrequencyPct : (r && r.responseFrequencyPercent);
    var score = (r && r.gaugeScore != null) ? r.gaugeScore : (freqPct || 0);
    var trend = (r && r.trend) || (er && er.trend) || 'flat';
    var trendClass = trend === 'up' ? 'dc-exec-gauge__trend--up' : trend === 'down' ? 'dc-exec-gauge__trend--down' : '';
    var trendIcon = trend === 'up' ? '↑' : trend === 'down' ? '↓' : '→';
    var trendLabel = (r && r.trendLabel) || (er && er.trendLabel) || 'vs prior period';
    var drivers = er && er.drivers && er.drivers.length ? er.drivers.slice(0, 2) : [];
    var driversHtml = drivers.length ? '<div class="dc-exec-drivers"><div class="dc-exec-drivers__title">What\'s hurting your badge?</div>' + drivers.map(function (d) {
      return '<div class="dc-exec-driver">' + '<div class="dc-exec-driver__title">' + escapeHtml(d.title) + '</div>' + (d.subtitle ? '<div class="dc-exec-driver__sub">' + escapeHtml(d.subtitle) + '</div>' : '') + (d.ctaHref ? '<a href="' + escapeAttr(d.ctaHref) + '" class="dc-link dc-exec-driver__cta">' + escapeHtml(d.ctaLabel || 'Open') + '</a>' : '') + '</div>';
    }).join('') + '</div>' : '';
    var scoreVal = freqPct != null ? escapeHtml(String(freqPct)) + '%' : '—';
    var html = '<div class="dc-exec-summary">' +
      '<div class="dc-exec-badge">' + escapeHtml(combinedBadge || '') + '</div>' +
      '<div class="dc-exec-metrics">' +
        '<div class="dc-exec-metric"><span class="dc-exec-metric__label dc-exec-metric__label--wrap">Avg first<br>response</span><span class="dc-exec-metric__value dc-exec-gauge__score">' + (avgHours != null ? escapeHtml(String(avgHours)) + 'h' : '—') + '</span></div>' +
        '<div class="dc-exec-metric dc-exec-metric--freq"><span class="dc-exec-metric__label dc-exec-metric__label--wrap">Response<br>frequency</span><div class="dc-exec-freq-row">' +
          '<span class="dc-exec-metric__value dc-exec-gauge__score">' + scoreVal + '</span>' +
          '<div class="dc-exec-gauge__bar"><div class="dc-exec-gauge__fill" style="width:' + score + '%"></div></div>' +
        '</div><div class="dc-exec-gauge__meta ' + trendClass + '">' + trendIcon + ' ' + escapeHtml(trendLabel) + '</div></div>' +
      '</div>' + driversHtml;

    el.innerHTML = html;

    var whyBtn = document.getElementById('dc-exec-why-btn');
    var modal = document.getElementById('dc-exec-why-modal');
    var modalBody = document.getElementById('dc-exec-why-modal-body');
    var modalClose = document.getElementById('dc-exec-why-modal-close');
    if (whyBtn && modal && modalBody) {
      modalBody.textContent = (r && r.whyItMatters) || (er && er.whyItMatters) || 'Execution Reliability measures how quickly and consistently you and your partners respond. Fast response times help close deals faster.';
      whyBtn.onclick = function () {
        modal.hidden = false;
        modal.classList.add('dc-modal--open');
      };
    }
    if (modalClose && modal) {
      function closeModal() {
        modal.hidden = true;
        modal.classList.remove('dc-modal--open');
      }
      modalClose.onclick = closeModal;
      var backdrop = modal && modal.querySelector('.dc-modal__backdrop');
      if (backdrop) backdrop.onclick = closeModal;
      document.addEventListener('keydown', function onExecModalKey(e) {
        if (e.key === 'Escape' && modal.classList.contains('dc-modal--open')) closeModal();
      });
    }
  }

  function renderRegionalDistChart(vm) {
    var data = vm && vm.regionalDistributionChartData;
    var canvas = document.getElementById('dc-regional-chart-canvas');
    if (!canvas) return;

    if (regionalDistChartInstance) {
      regionalDistChartInstance.destroy();
      regionalDistChartInstance = null;
    }

    if (!data || !data.labels || !data.values) return;

    var ctx = canvas.getContext('2d');
    regionalDistChartInstance = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: data.labels,
        datasets: [{
          data: data.values,
          backgroundColor: '#6c72ff',
          borderRadius: 4,
          barThickness: 20
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: 'rgba(16, 25, 53, 0.95)',
            titleColor: '#e2e8f0',
            bodyColor: '#94a3b8',
            borderColor: 'rgba(148, 163, 184, 0.2)',
            borderWidth: 1,
            padding: 10,
            displayColors: false,
            callbacks: {
              label: function (context) {
                return context.parsed.y + ' deals';
              }
            }
          }
        },
        scales: {
          x: {
            grid: { display: false, drawBorder: false },
            ticks: {
              color: '#7e89ac',
              font: { size: 10 },
              maxRotation: 45,
              minRotation: 35
            }
          },
          y: {
            beginAtZero: true,
            grid: { color: 'rgba(126, 137, 172, 0.1)', drawBorder: false },
            ticks: {
              color: '#7e89ac',
              font: { size: 10 },
              stepSize: 5
            },
            max: Math.max(Math.ceil(Math.max.apply(null, data.values) / 5) * 5 + 5, 20)
          }
        }
      }
    });
  }

  function renderMapOverlay(vm) {
    var overlay = vm && vm.dealsByCountryMapOverlay;
    var valueEl = document.getElementById('dc-map-overlay-value');
    var labelEl = document.getElementById('dc-map-overlay-label');
    if (!valueEl || !labelEl) return;
    var label = overlay && (overlay.region || overlay.country);
    if (overlay && label && overlay.value != null) {
      valueEl.textContent = overlay.value;
      labelEl.textContent = label;
    }
  }

  var toolboxIconMap = {
    briefcase: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="7" width="20" height="14" rx="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/></svg>',
    mail: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><path d="M22 6l-10 7L2 6"/></svg>',
    users: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
    'trending-up': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M23 6l-9.5 9.5-5-5L1 18"/><path d="M17 6h6v6"/></svg>',
    radio: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="2"/><path d="M16.24 7.76a6 6 0 0 1 0 8.49m-8.48-.01a6 6 0 0 1 0-8.49m11.31-2.82a10 10 0 0 1 0 14.14m-14.14 0a10 10 0 0 1 0-14.14"/></svg>',
    'file-text': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/><path d="M16 13H8"/><path d="M16 17H8"/></svg>',
    file: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/><path d="M16 13H8"/><path d="M16 17H8"/></svg>',
    message: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>',
    building: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 21h18"/><path d="M9 8h1"/><path d="M9 12h1"/><path d="M9 16h1"/><path d="M14 8h1"/><path d="M14 12h1"/><path d="M14 16h1"/><path d="M5 21V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16"/></svg>',
    scale: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M16 16h6"/><path d="M19 13v6"/><path d="M12 15V3"/><path d="M9 6l3-3 3 3"/><path d="M12 15a3 3 0 1 1 0 6 3 3 0 0 1 0-6z"/></svg>'
  };

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
      var iconSvg = toolboxIconMap[t.iconKey] || toolboxIconMap.briefcase;
      var href = t.href || '#';
      var shellHref = (/^https?:\/\//i.test(href) || href.indexOf('/app#') === 0 || href === '#')
        ? href
        : '/app#' + (href.charAt(0) === '/' ? href : ('/' + href));
      return '<a href="' + escapeAttr(shellHref) + '" class="dc-toolbox-item" target="_top">' +
        '<span class="dc-toolbox-item__icon dc-toolbox-item__icon--svg" aria-hidden="true">' + iconSvg + '</span>' +
        '<span>' + (t.labelHtml || escapeHtml(t.label)) + '</span>' +
        '<span class="dc-toolbox-item__status dc-toolbox-item__status--' + statusClass + '">' + escapeHtml(t.status) + '</span>' +
        '</a>';
    }).join('') + '</div>';
    el.innerHTML = html;
  }

  function getStoredRole() {
    try {
      var s = localStorage.getItem(ROLE_STORAGE_KEY);
      return (s === 'owner' || s === 'brand' || s === 'operator') ? s : 'owner';
    } catch (_) { return 'owner'; }
  }

  function setStoredRole(role) {
    try { localStorage.setItem(ROLE_STORAGE_KEY, role); } catch (_) {}
  }

  function zeroLoiChartData() {
    return {
      labels: ['Q3 2024', 'Q4 2024', 'Q1 2025', 'Q2 2025'],
      franchiseDeals: [0, 0, 0, 0],
      managementDeals: [0, 0, 0, 0],
      regionFranchiseDeals: [0, 0, 0, 0],
      regionManagementDeals: [0, 0, 0, 0]
    };
  }

  function applyVm(vm) {
    if (!vm) return;
    renderHeader(vm);
    renderTrendChart(vm);
    renderMapOverlay(vm);
    renderKPIStrip(vm);
    renderNextActions(vm);
    renderPipelineFunnel(vm);
    renderSignals(vm);
    renderMarketIntel(vm);
    renderRecentActivity(vm);
    renderResponsiveness(vm);
    renderToolbox(vm);
  }

  function updateRoleToggle(role) {
    var toggle = document.getElementById('dc-role-toggle');
    if (!toggle) return;
    var btns = toggle.querySelectorAll('.dc-role-btn');
    btns.forEach(function (b) {
      var r = b.getAttribute('data-role');
      b.classList.toggle('active', r === role);
      b.setAttribute('aria-pressed', r === role ? 'true' : 'false');
    });
  }

  function fetchAndRender(role) {
    var roleParam = role || getStoredRole();
    Promise.all([
      fetch('/api/dashboard/home?role=' + encodeURIComponent(roleParam)).then(function (res) { return res.json(); }),
      fetchMarketIntelFromAlerts(),
      fetchRecentDealActivity()
    ])
      .then(function (results) {
        var json = results[0];
        var liveIntel = results[1];
        var liveRecent = results[2];
        if (json && json.success !== false) {
          if (liveIntel && liveIntel.length) {
            json.marketIntel = liveIntel;
            json.marketIntelligence = liveIntel;
          }
          if (liveRecent && liveRecent.length) {
            json.recentActivity = liveRecent;
          }
          applyVm(json);
          updateRoleToggle(json.role || roleParam);
          setStoredRole(json.role || roleParam);
        } else {
          var container = document.getElementById('dc-dashboard');
          if (container) container.innerHTML = '<div class="dc-section"><div class="dc-section__body">Unable to load dashboard data.</div></div>';
        }
      })
      .catch(function (err) {
        var fallback = window.DashboardAdapter && window.DashboardAdapter.getDashboardViewModel ? window.DashboardAdapter.getDashboardViewModel() : null;
        if (fallback) {
          // Do not show inflated mock chart values if API fails.
          fallback.loiDealVolumeChartData = zeroLoiChartData();
          Promise.all([fetchMarketIntelFromAlerts(), fetchRecentDealActivity()]).then(function (liveData) {
            var liveIntel = liveData[0];
            var liveRecent = liveData[1];
            if (liveIntel && liveIntel.length) {
              fallback.marketIntel = liveIntel;
              fallback.marketIntelligence = liveIntel;
            }
            if (liveRecent && liveRecent.length) {
              fallback.recentActivity = liveRecent;
            }
            applyVm(fallback);
            updateRoleToggle(getStoredRole());
          }).catch(function () {
            applyVm(fallback);
            updateRoleToggle(getStoredRole());
          });
        } else {
          var container = document.getElementById('dc-dashboard');
          if (container) container.innerHTML = '<div class="dc-section"><div class="dc-section__body">Unable to load dashboard data.</div></div>';
        }
      });
  }

  function init() {
    var role = getStoredRole();
    updateRoleToggle(role);
    var toggle = document.getElementById('dc-role-toggle');
    if (toggle) {
      toggle.addEventListener('click', function (e) {
        var btn = e.target.closest('.dc-role-btn');
        if (btn) {
          var r = btn.getAttribute('data-role');
          if (r) fetchAndRender(r);
        }
      });
    }
    var marketIntelToggleBtn = document.getElementById('dc-market-intel-toggle');
    if (marketIntelToggleBtn) {
      updateMarketIntelToggleButton();
      marketIntelToggleBtn.addEventListener('click', function () {
        marketIntelTicker.userPaused = !marketIntelTicker.userPaused;
        updateMarketIntelToggleButton();
      });
    }
    fetchAndRender(role);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  window.DashboardComponents = {
    renderHeader: renderHeader,
    renderRecentActivity: renderRecentActivity,
    renderTrendChart: renderTrendChart,
    renderKPIStrip: renderKPIStrip,
    renderTodayFocus: renderTodayFocus,
    renderNextActions: renderNextActions,
    renderPipelineFunnel: renderPipelineFunnel,
    renderSignals: renderSignals,
    renderMarketIntel: renderMarketIntel,
    renderResponsiveness: renderResponsiveness,
    renderToolbox: renderToolbox
  };
})();
