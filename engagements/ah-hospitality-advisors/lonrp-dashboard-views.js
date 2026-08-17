/* LONRP Property Dashboard — Tableau workbook shell (illustrative) */
(function (global) {
  'use strict';

  var LONRP_NAV = [
    { id: 'intro', label: 'Intro', color: '#a65200' },
    { id: 'info', label: 'Info Page', color: '#e15759' },
    { id: 'executive', label: 'Executive Overview', color: '#e15759', default: true },
    { id: 'finance', label: 'Finance', color: '#f66273' },
    { id: 'segmentation', label: 'Segmentation', color: '#6e597f' },
    { id: 'revpar-index', label: 'RevPAR Index', color: '#dec77b' },
    { id: 'premium', label: 'Premium Room', color: '#a65200' },
    { id: 'amped', label: 'System Adoption', color: '#6e597f' },
    { id: 'distribution', label: 'Distribution', color: '#a65200' },
    { id: 'bonvoy', label: 'Loyalty Occupancy', color: '#6e597f' },
    { id: 'pace', label: 'Booking Pace', color: '#17927d' },
    { id: 'src', label: 'Source Country', color: '#72bcbb' },
    { id: 'gross', label: 'Gross Booking', color: '#4f779a' },
    { id: 'scorecard', label: 'Scorecard', color: '#6e597f' },
    { id: 'losbw', label: 'LOS-Booking Window', color: '#dec77b' },
    { id: 'spe', label: 'Special Corporate', color: '#17927d' },
    { id: 'promos', label: 'Promotion Summary', color: '#f66273' },
    { id: 'accuracy', label: 'Forecast Accuracy', color: '#4f779a' },
    { id: 'accounts', label: 'Account Insights', color: '#72bcbb' },
    { id: 'enrolments', label: 'Enrolments', color: '#dec77b' }
  ];

  var LONRP_HOTEL = {
    code: 'NG01',
    name: 'Northgate Riverside — pilot property',
    owner: 'Private ownership (illustrative)',
    area: 'UK & Ireland',
    country: 'United Kingdom',
    miMarket: 'Metro centre',
    strMarket: 'Primary market',
    status: 'Open',
    accountingComp: 'Y',
    marketShareComp: 'Y',
    management: 'Owner-operated',
    runAsof: '10 June 2025',
    miCompCount: 7,
    strCompset: 'Primary comp set · 6 hotels (illustrative)',
    premiumPools: 'Club/Concierge · Suite · Premium (keyword logic)'
  };

  var DASH_META = {
    intro: { title: 'INTRO', subtitle: 'Northgate Riverside · Property dashboard' },
    info: { title: 'INFO', subtitle: 'Version 1.8 , Report Guideline and Useful Information' },
    executive: { title: 'EXECUTIVE', subtitle: 'YTD High Level Summary of your business' },
    finance: { title: 'FINANCE', subtitle: 'ADR · Occupancy · RevPAR · quarterly P&L' },
    segmentation: { title: 'SEGMENTATION', subtitle: 'Segment mix vs LY' },
    'revpar-index': { title: 'REVPAR INDEX', subtitle: 'RPI vs comp set · market share' },
    premium: { title: 'PREMIUM', subtitle: 'Premium room pool · member level' },
    amped: { title: 'AMPED', subtitle: 'System adoption · GPO · RPO · FCST' },
    distribution: { title: 'DISTRIBUTION', subtitle: 'Channel mix · digital share · OTA vs LY' },
    bonvoy: { title: 'LOYALTY OCCUPANCY', subtitle: 'Loyalty program paid occupancy penetration' },
    pace: { title: 'PACE', subtitle: 'Group & transient booking pace' },
    src: { title: 'SOURCE COUNTRY', subtitle: 'Geographic source mix' },
    gross: { title: 'GROSS BOOKING', subtitle: 'Gross bookings trend vs market' },
    scorecard: { title: 'SCORECARD', subtitle: 'Revenue scorecard vs budget & target' },
    losbw: { title: 'LOS-BOOKING WINDOW', subtitle: 'Length of stay · lead time' },
    spe: { title: 'SPE', subtitle: 'Special corporate programs' },
    promos: { title: 'PROMOS', subtitle: 'Promotion summary by channel' },
    accuracy: { title: 'ACCURACY', subtitle: 'Forecast accuracy · MAPE · bias' },
    accounts: { title: 'ACCOUNT INSIGHTS', subtitle: 'Displacement · scenarios · attribution' },
    enrolments: { title: 'ENROLMENTS', subtitle: 'Loyalty enrolment vs goal' }
  };

  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  }

  function periodLabel(state, PERIOD_META) {
    if (state.lonrpPeriod === 'ytd') return 'YTD';
    if (state.lonrpPeriod === 'fy') return 'Full Year';
    return PERIOD_META[state.period] ? PERIOD_META[state.period].periodTag : 'MTD';
  }

  function renderNav(state) {
    var nav = document.getElementById('lonrpNav');
    if (!nav) return;
    nav.innerHTML = LONRP_NAV.map(function (item) {
      var active = state.dashboard === item.id;
      return '<button type="button" class="lonrp-nav-btn' + (active ? ' active' : '') + '" data-dashboard="' + item.id + '" style="--tab-color:' + item.color + '">' +
        esc(item.label) + '</button>';
    }).join('');
  }

  function renderDashHeader(state, v, PERIOD_META) {
    var el = document.getElementById('lonrpDashHeader');
    if (!el) return;
    var builtIn = ['pace', 'accounts'];
    if (state.dashboard === 'info' || state.dashboard === 'intro' || state.dashboard === 'executive') {
      el.classList.add('hidden');
      el.innerHTML = '';
      return;
    }
    el.classList.remove('hidden');
    var navItem = LONRP_NAV.find(function (n) { return n.id === state.dashboard; }) || {};
    var meta = DASH_META[state.dashboard] || { title: state.dashboard.toUpperCase(), subtitle: '' };
    var displayTitle = navItem.label || meta.title;
    var period = periodLabel(state, PERIOD_META);
    el.innerHTML =
      '<div class="lonrp-header-row">' +
      '<div class="lonrp-header-titles">' +
      '<h2>' + esc(displayTitle) + (builtIn.indexOf(state.dashboard) >= 0 ? '' : ' SUMMARY | ' + period) + '</h2>' +
      '<p class="lonrp-header-sub">' + esc(meta.subtitle) + (builtIn.indexOf(state.dashboard) < 0 ? ' · Revenue in <em>' + esc(state.lonrpCurrency) + '</em> · Comparison <em>' + esc(state.lonrpCompare) + '</em>' : '') + '</p>' +
      '</div>' +
      '<div class="lonrp-header-meta">' +
      '<span class="lonrp-hotel">' + esc(v.short) + '</span>' +
      '<span class="lonrp-run">Run date · 9 Jun 2025</span>' +
      '</div></div>';
    el.style.setProperty('--dash-accent', navItem.color || '#22d3ee');
  }

  function tile(title, bodyHtml, wide) {
    return '<div class="lonrp-tile' + (wide ? ' lonrp-tile--wide' : '') + '">' +
      '<h4>' + esc(title) + '</h4><div class="lonrp-tile-body">' + bodyHtml + '</div></div>';
  }

  function kpiRow(items) {
    return '<div class="lonrp-kpi-row">' + items.map(function (it) {
      return '<div class="lonrp-kpi-cell"><span class="k">' + esc(it.k) + '</span><b class="' + (it.sc || '') + '">' + esc(it.v) + '</b>' +
        (it.s ? '<span class="s ' + (it.sc || '') + '">' + esc(it.s) + '</span>' : '') + '</div>';
    }).join('') + '</div>';
  }

  function renderIntro(mount) {
    mount.innerHTML =
      '<div class="lonrp-intro-hero">' +
      '<h1>Northgate Riverside</h1>' +
      '<p class="lead">Commercial performance dashboard · AH Hospitality Advisors concept layout</p>' +
      '<ul><li>22 dashboard tabs · navigation pane on the left</li>' +
      '<li>Illustrative metrics — not live client data</li>' +
      '<li>Executive Overview is the default landing tab</li></ul>' +
      '<button type="button" class="btn btn-primary" data-goto-dashboard="executive">Open Executive Overview →</button></div>';
  }

  function iconRow(glyphClass, glyphChar, html) {
    return '<div class="lonrp-icon-row"><span class="lonrp-icon-glyph ' + glyphClass + '" aria-hidden="true">' + glyphChar + '</span><div>' + html + '</div></div>';
  }

  function featureRow(iconKind, html) {
    return '<div class="lonrp-feature-row"><span class="feat-icon feat-icon--' + iconKind + '" aria-hidden="true"></span><div>' + html + '</div></div>';
  }

  function benchBox(iconKind, html, seeLabel) {
    return '<div class="lonrp-bench-box">' +
      '<div class="lonrp-bench-box-top">' +
      '<span class="bench-icon bench-icon--' + iconKind + '" aria-hidden="true"></span>' +
      '<a href="#" class="lonrp-see-list" data-see-list="' + esc(seeLabel) + '">See .. <span class="arrow">↗</span></a></div>' +
      '<div class="lonrp-bench-box-body">' + html + '</div></div>';
  }

  function renderInfo(mount, v, state) {
    var h = LONRP_HOTEL;
    if (state && state.scope !== 'lonrp') {
      h = Object.assign({}, h, {
        name: v.name,
        owner: 'Portfolio view',
        area: 'Multi-market',
        country: '—',
        miMarket: '—',
        strMarket: '—'
      });
    }
    mount.innerHTML =
      '<div class="lonrp-info-page">' +
      '<div class="lonrp-info-topbar">' +
      '<div class="lonrp-info-title-block">' +
      '<h1>INFO PAGE</h1>' +
      '<p class="lonrp-info-version">Version 1.8, Report Guideline and Useful Information</p>' +
      '</div>' +
      '<div class="lonrp-info-logos" aria-hidden="true">' +
      '<span class="lonrp-logo-mi">A&amp;H Hospitality</span>' +
      '<span class="lonrp-logo-prop">Commercial performance hub</span>' +
      '</div></div>' +
      '<div class="lonrp-info-columns">' +
      '<div class="lonrp-info-col">' +
      '<h3 class="lonrp-info-section-title">Hotel information</h3>' +
      '<div class="lonrp-info-box lonrp-hotel-info">' +
      '<p class="hotel-name">' + esc(h.name) + '</p>' +
      '<p><strong>Owner:</strong> ' + esc(h.owner) + '</p>' +
      '<p><strong>Area:</strong> ' + esc(h.area) + '</p>' +
      '<p><strong>Country:</strong> ' + esc(h.country) + '</p>' +
      '<p><strong>Commercial submarket:</strong> ' + esc(h.miMarket) + '</p>' +
      '<p><strong>STR Market:</strong> ' + esc(h.strMarket) + '</p>' +
      '<p><strong>Status:</strong> ' + esc(h.status) + '</p>' +
      '<p><strong>Accounting Comp:</strong> ' + esc(h.accountingComp) + '</p>' +
      '<p><strong>Market Share Comp:</strong> ' + esc(h.marketShareComp) + '</p>' +
      '<p><strong>Management:</strong> ' + esc(h.management) + '</p>' +
      '<p><strong>Run asof Date:</strong> ' + esc(h.runAsof) + '</p>' +
      '</div>' +
      '<h3 class="lonrp-info-section-title">Icons explained</h3>' +
      '<div class="lonrp-icon-stack">' +
      iconRow('email', '✉',
        'Click on the icon to request support <a class="lonrp-support-link" href="mailto:info@ahhospitalityadvisors.com?subject=Support%3ACommercial%20Performance%20Hub">info@ahhospitalityadvisors.com</a>') +
      iconRow('filter', '▾',
        'Icon means the chart or table can be used as filter, click on the dimension or value to filter the entire dashboard. Hold Ctrl key to add to the selections.') +
      iconRow('touch', '☝',
        'Icon means that the dimension can be used to change the chart values, click or hover over the KPI to select your preferred measure to view.') +
      iconRow('info', 'i',
        'This icon is available in each dashboard, click to view the report guidelines. A new popup will appear to view important information and how to utilize the dashboard. To close, click X.') +
      '</div></div>' +
      '<div class="lonrp-info-col">' +
      '<h3 class="lonrp-info-section-title">Interaction &amp; features</h3>' +
      '<div class="lonrp-feature-stack">' +
      featureRow('nav', '<strong>Navigation Pane:</strong> Use the pane on left hand side to visit the required dashboard, the current selected dashboard shows in <strong class="lonrp-red">red</strong>.') +
      featureRow('param', '<strong>Parameters:</strong> Can be used to change measure or dimension to drill into further details. <strong>Choose to compare</strong> parameter will change the %change to compare to either Last Year, 2019 or Benchmark. <strong>Currency</strong> parameter will change the revenue reporting value to either Constant USD or Local Currency.') +
      featureRow('filter', '<strong>Filters:</strong> <strong>Period filter</strong> can be used to filter out monthly finance data for either YTD or full year, while the rest of dashboard has YTD actual by default. When you choose full year data, the next <strong>Date</strong> filter will change to see all months.<br><br><strong>Date filter</strong> is available when Period Filter set to Full year, you will have a list of months to choose. While YTD, will present the YTD month that is one month only that should be kept as is. Make sure to clear this filter before switching to YTD.') +
      featureRow('currency', '<strong>Currency :</strong> The dashboard contains an option to switch the currency between <strong>Local</strong> or <strong>Constant USD.</strong> Local Currency displays data for your property according to the selling currency.<br>Constant US Dollars uses agreed monthly FX rates for the current year.') +
      featureRow('ppt', 'To export a workbook to PowerPoint:<br>Select File &gt; Export as PowerPoint. Include: Specific sheets from this <strong>Workbook</strong> then select the <strong>Dashboards</strong> that you want to include in the presentation. The exported PowerPoint file reflects the file name of your workbook.') +
      '</div></div>' +
      '<div class="lonrp-info-col">' +
      '<h3 class="lonrp-info-section-title">Benchmarking and compset</h3>' +
      '<div class="lonrp-bench-stack">' +
      benchBox('chart',
        '<strong>Market benchmarking (Comp hotels count ' + h.miCompCount + ')</strong><br>' +
        'A market benchmark has been added to some views so you can compare your hotel performance to peer properties in the same commercial submarket. Market metrics include accounting-comparable hotels in the same market, including the subject hotel. Market data is shown when the market contains a minimum of five comparable hotels including the subject property. If that threshold is not met, the value will be set to null.',
        'Market comp list') +
      benchBox('hotel',
        '<strong>STR Competitive Set</strong><br>The competitive Set is used in Market Share performance. A competitive set is a group of hotels that compete with your property for business and is selected with the purpose of benchmarking your performance against the competition. In this report only the Primary Compset data is available.',
        'STR comp list') +
      benchBox('bed',
        '<strong>Premium Room Pool Classification</strong><br>Room Pool Descriptions are used to assign classifications to each room pool based on Keyword Logic – assigning “Club/Concierge”, “Suite”, or “Premium”. Assigned Classifications are compared to HPP: Room pool considered “Standard” based on keyword logic despite listing as “Premium” in HPP.',
        'Premium pools') +
      '</div></div></div>' +
      '<p class="lonrp-info-footer-copy">©2026 AH Hospitality Advisors · Confidential concept mockup</p>' +
      '</div>';
  }

  function renderFinance(mount, v, fmtEuro, fmtPct, pctClass) {
    var vsBud = v.vsBudPct;
    mount.innerHTML =
      '<div class="lonrp-tile-grid lonrp-tile-grid--4">' +
      tile('ADR', kpiRow([{ k: 'Actual', v: fmtEuro(v.adr), s: fmtPct(vsBud * 0.3, true), sc: pctClass(vsBud) }])) +
      tile('Occupancy', kpiRow([{ k: 'Actual', v: v.occ.toFixed(1) + '%', s: fmtPct(v.occ - v.bud.occ, true) + ' pp', sc: pctClass(v.occ - v.bud.occ) }])) +
      tile('RevPAR', kpiRow([{ k: 'Actual', v: fmtEuro(v.revpar), s: fmtPct(vsBud, true) + ' vs bud', sc: pctClass(vsBud) }])) +
      tile('Total revenue', kpiRow([{ k: 'Rooms', v: fmtEuro(v.roomsRevK) + 'K', s: 'GOP ' + fmtEuro(v.gopK) + 'K', sc: 'pos' }])) +
      '</div>' +
      '<div class="lonrp-tile lonrp-tile--wide"><h4>Finance trend · monthly</h4><div class="lonrp-tile-body chart-box"><canvas id="lonrpFinTrend"></canvas></div></div>';
  }

  function renderDistribution(mount, v, fmtPct, pctClass) {
    mount.innerHTML =
      '<div class="lonrp-tile-grid lonrp-tile-grid--3">' +
      tile('Digital share', '<div class="lonrp-big-val">' + (100 - v.ota - v.other).toFixed(0) + '%</div><p>Direct ' + v.direct + '% · OTA ' + v.ota + '%</p>') +
      tile('OTA vs LY', '<div class="lonrp-big-val ' + pctClass(-4) + '">+4.5pp</div><p>Parity watch active</p>') +
      tile('DIS goal', '<div class="lonrp-big-val">72</div><p>Goal performance index</p>') +
      '</div>' +
      '<div class="lonrp-tile lonrp-tile--wide"><h4>OTA vs digital mix trend</h4><div class="chart-box"><canvas id="lonrpDistTrend"></canvas></div></div>';
  }

  function renderScorecardDash(mount, v, fmtEuro, fmtPct, pctClass) {
    var rows = [
      ['Total revenue', fmtEuro(v.roomsRevK) + 'K', fmtPct(v.vsBudPct, true), pctClass(v.vsBudPct)],
      ['RevPAR', fmtEuro(v.revpar), fmtPct(((v.revpar - v.stly.revpar) / v.stly.revpar) * 100, true), 'pos'],
      ['ADR', fmtEuro(v.adr), '+0.6%', 'pos'],
      ['Occupancy', v.occ.toFixed(1) + '%', fmtPct(v.occ - v.bud.occ, true), pctClass(v.occ - v.bud.occ)]
    ];
    mount.innerHTML =
      '<div class="lonrp-scorecard-table-wrap">' +
      '<table class="lonrp-scorecard-table"><thead><tr><th>Measure</th><th>Actual</th><th>vs BD</th><th>vs Target</th></tr></thead><tbody>' +
      rows.map(function (r) {
        return '<tr><td>' + r[0] + '</td><td>' + r[1] + '</td><td class="' + r[3] + '">' + r[2] + '</td><td class="' + r[3] + '">' + r[2] + '</td></tr>';
      }).join('') +
      '</tbody></table></div>';
  }

  function renderAccuracy(mount, v) {
    mount.innerHTML =
      '<div class="lonrp-tile-grid lonrp-tile-grid--2">' +
      tile('MAPE · 30d', '<div class="lonrp-big-val">4.2%</div><p>Within 5% threshold</p>') +
      tile('Bias', '<div class="lonrp-big-val neg">−1.8%</div><p>Slight under-forecast</p>') +
      '</div>' +
      '<div class="lonrp-tile lonrp-tile--wide"><h4>Forecast accuracy · rolling</h4><div id="lonrpAccuracyBars"></div></div>';
  }

  function renderEnrolments(mount) {
    mount.innerHTML =
      '<div class="lonrp-tile-grid lonrp-tile-grid--3">' +
      tile('MTD enrolments', '<div class="lonrp-big-val">1,240</div><p>vs goal 1,180 (+5%)</p>') +
      tile('YTD enrolments', '<div class="lonrp-big-val">6,820</div><p>On track</p>') +
      tile('Opportunities', '<ul class="lonrp-bullet"><li>Front desk capture +8%</li><li>Digital pre-arrival</li></ul>') +
      '</div>';
  }

  function renderGeneric(mount, id, v) {
    var meta = DASH_META[id] || { title: id };
    mount.innerHTML =
      '<div class="lonrp-generic-msg">' +
      '<p><strong>' + esc(meta.title) + '</strong> — illustrative module matching Tableau tab layout.</p>' +
      '<p>Property: ' + esc(v.short) + ' · Use filters in the top bar to change period and comparison.</p></div>' +
      '<div class="lonrp-tile-grid lonrp-tile-grid--3">' +
      tile('Trend', '<div class="chart-box sm"><canvas id="lonrpChart_' + id.replace(/-/g, '_') + '"></canvas></div>') +
      tile('vs LY band', '<div class="lonrp-big-val">—</div><p>Compare: vs LY</p>') +
      tile('Alert', '<p class="lonrp-muted">Noise-suppressed outliers · demo</p>') +
      '</div>';
  }

  function renderBuiltInDashboard(id, mount, ctx) {
    if (id === 'intro') { renderIntro(mount); return; }
    if (id === 'info') { renderInfo(mount, ctx.v, ctx.state); return; }
    if (id === 'executive' && global.LonrpExecutiveView) {
      global.LonrpExecutiveView.destroyCharts();
      global.LonrpExecutiveView.render(mount, ctx.state);
      return;
    }
    if (id === 'finance') { renderFinance(mount, ctx.v, ctx.fmtEuro, ctx.fmtPct, ctx.pctClass); return; }
    if (id === 'distribution') { renderDistribution(mount, ctx.v, ctx.fmtPct, ctx.pctClass); return; }
    if (id === 'scorecard') { renderScorecardDash(mount, ctx.v, ctx.fmtEuro, ctx.fmtPct, ctx.pctClass); return; }
    if (id === 'accuracy') { renderAccuracy(mount, ctx.v); return; }
    if (id === 'enrolments') { renderEnrolments(mount); return; }
    renderGeneric(mount, id, ctx.v);
  }

  function renderSimpleCharts(id, ctx) {
    if (typeof Chart === 'undefined') return;
    var v = ctx.v;
    if (id === 'executive' && global.LonrpExecutiveView) {
      requestAnimationFrame(function () { global.LonrpExecutiveView.initCharts(); });
      return;
    }
    if (id === 'finance') {
      var c = document.getElementById('lonrpFinTrend');
      if (c && !c._lonrp) {
        c._lonrp = true;
        new Chart(c, {
          type: 'line',
          data: {
            labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May'],
            datasets: [{ label: 'RevPAR', data: [98, 102, 105, 108, v.revpar], borderColor: '#3284ff', tension: 0.3 }]
          },
          options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } },
            scales: {
              x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(148,163,184,0.1)' } },
              y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(148,163,184,0.1)' } }
            }
          }
        });
      }
    }
    if (id === 'distribution') {
      var d = document.getElementById('lonrpDistTrend');
      if (d && !d._lonrp) {
        d._lonrp = true;
        new Chart(d, {
          type: 'bar',
          data: {
            labels: ['W1', 'W2', 'W3', 'W4'],
            datasets: [
              { label: 'Direct', data: [38, 39, 40, v.direct], backgroundColor: '#26b14c' },
              { label: 'OTA', data: [42, 44, 43, v.ota], backgroundColor: '#ff3424' }
            ]
          },
          options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { labels: { color: '#94a3b8' } } },
            scales: {
              x: { stacked: true, ticks: { color: '#94a3b8' }, grid: { color: 'rgba(148,163,184,0.1)' } },
              y: { stacked: true, max: 100, ticks: { color: '#94a3b8' }, grid: { color: 'rgba(148,163,184,0.1)' } }
            }
          }
        });
      }
    }
    var gen = document.getElementById('lonrpChart_' + id.replace(/-/g, '_'));
    if (gen && !gen._lonrp) {
      gen._lonrp = true;
      new Chart(gen, {
        type: 'line',
        data: { labels: ['M1', 'M2', 'M3', 'M4'], datasets: [{ data: [10, 12, 11, 14], borderColor: '#4f779a', tension: 0.3 }] },
        options: {
          responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(148,163,184,0.1)' } },
            y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(148,163,184,0.1)' } }
          }
        }
      });
    }
    if (id === 'accuracy') {
      var bars = document.getElementById('lonrpAccuracyBars');
      if (bars && !bars.innerHTML) {
        bars.innerHTML = [7, 14, 30, 60].map(function (d) {
          return '<div class="forecast-bar"><div class="row"><span class="label">' + d + 'd</span><span>' + (3 + d * 0.08).toFixed(1) + '%</span></div><div class="track"><i style="width:' + (40 + d) + '%"></i></div></div>';
        }).join('');
      }
    }
  }

  function showDashboardSection(state) {
    var builtIn = ['pace', 'accounts'];
    document.querySelectorAll('.lonrp-dash-built').forEach(function (el) {
      el.classList.toggle('hidden', builtIn.indexOf(state.dashboard) < 0 || el.getAttribute('data-dash-built') !== state.dashboard);
    });
    var generic = document.getElementById('lonrpGenericMount');
    if (generic) {
      var isGeneric = builtIn.indexOf(state.dashboard) < 0;
      generic.classList.toggle('hidden', !isGeneric);
    }
  }

  global.LonrpViews = {
    NAV: LONRP_NAV,
    META: DASH_META,
    HOTEL: LONRP_HOTEL,
    renderNav: renderNav,
    renderDashHeader: renderDashHeader,
    showDashboardSection: showDashboardSection,
    renderBuiltInDashboard: renderBuiltInDashboard,
    renderSimpleCharts: renderSimpleCharts,
    defaultDashboard: 'executive',
    tableauDashboardIds: [
      'intro', 'info', 'executive', 'finance', 'segmentation', 'revpar-index', 'premium', 'amped',
      'distribution', 'bonvoy', 'src', 'gross', 'scorecard', 'losbw', 'spe', 'promos', 'accuracy', 'enrolments'
    ]
  };
})(typeof window !== 'undefined' ? window : this);
