/* LONRP — Executive Overview (Tableau EXECUTIVE SUMMARY | YTD) */
(function (global) {
  'use strict';

  var _charts = {};

  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  }

  function arr(dir) {
    if (dir === 'up') return '▲';
    if (dir === 'down') return '▼';
    return '—';
  }

  function finKpi(name, block) {
    function line(row) {
      var c = row.dir === 'up' ? 'up' : 'dn';
      var ic = row.dir === 'flat' ? '▲' : arr(row.dir);
      return '<div class="tb-fin-cmp ' + c + '"><span class="ic">' + ic + '</span> ' + esc(row.t) + '</div>';
    }
    return '<div class="tb-fin-kpi">' +
      '<div class="tb-fin-top"><span class="tb-fin-name">' + esc(name) + '</span><span class="tb-fin-val">' + esc(block.value) + '</span></div>' +
      line(block.vsBud) + line(block.vsLy) + line(block.vsLyMi) +
      '</div>';
  }

  function msMetric(it) {
    var c = it.dir === 'up' ? 'up' : 'dn';
    return '<div class="tb-ms-row">' +
      '<div class="tb-ms-val">' + esc(it.value) + '</div>' +
      '<div class="tb-ms-sub ' + c + '">(' + arr(it.dir) + ' ' + esc(it.delta) + ') ' + esc(it.label) + '</div></div>';
  }

  function loyaltyMetric(it) {
    var c = it.dir === 'up' ? 'up' : it.dir === 'flat' ? 'flat' : 'dn';
    var sub = it.dir === 'flat'
      ? '(' + esc(it.sub) + ')'
      : '(' + arr(it.dir) + ' ' + esc(it.sub) + ')';
    return '<div class="tb-ms-row">' +
      '<div class="tb-ms-val">' + esc(it.value) + '</div>' +
      '<div class="tb-ms-sub ' + c + '">' + sub + ' ' + esc(it.label) + '</div></div>';
  }

  function distMetric(it) {
    var c = it.dir === 'up' ? 'up' : it.dir === 'flat' ? 'flat' : 'dn';
    return '<div class="tb-ms-row">' +
      '<div class="tb-ms-val">' + esc(it.value) + '</div>' +
      '<div class="tb-ms-sub ' + c + '">(' + arr(it.dir) + ' ' + esc(it.sub) + ') ' + esc(it.label) + '</div></div>';
  }

  function paceChart(pace) {
    var maxAbs = 30;
    var months = pace.months || [];
    var bars = months.map(function (m) {
      var h1 = Math.min(100, (Math.abs(m.roomRev) / maxAbs) * 100);
      var h2 = Math.min(100, (Math.abs(m.totPace) / maxAbs) * 100);
      var c1 = m.roomRev >= 0 ? 'pos' : 'neg';
      var c2 = m.totPace >= 0 ? 'pos' : 'neg';
      return '<div class="tb-pace-col">' +
        '<div class="tb-pace-pcts"><span class="' + c1 + '">' + m.roomRev + '%</span><span class="' + c2 + '">' + m.totPace + '%</span></div>' +
        '<div class="tb-pace-barwrap">' +
        '<div class="tb-pace-bar dark ' + c1 + '" style="height:' + h1 + '%"></div>' +
        '<div class="tb-pace-bar light ' + c2 + '" style="height:' + h2 + '%"></div>' +
        '</div><div class="tb-pace-lbl">' + esc(m.label) + '</div></div>';
    }).join('');
    return '<div class="tb-pace">' +
      '<div class="tb-pace-legend">' +
      '<span><i class="sw dark"></i>' + esc(pace.legend[0]) + '</span>' +
      '<span><i class="sw light"></i>' + esc(pace.legend[1]) + '</span></div>' +
      '<div class="tb-pace-cols">' + bars + '</div></div>';
  }

  function scorecardHtml(rows) {
    return rows.map(function (row) {
      if (row.details) {
        return '<div class="tb-sc-item">' +
          '<div class="tb-sc-main">' + esc(row.text) + '</div>' +
          row.details.map(function (d) {
            return '<div class="tb-sc-sub up"><span class="ic">' + arr(d.dir) + '</span> ' + esc(d.t) + '</div>';
          }).join('') + '</div>';
      }
      var mainCls = row.textColor === 'green' ? 'text-green' : row.dir === 'warn' ? 'warn' : row.dir === 'down' ? 'dn' : '';
      var subCls = row.accent ? 'accent-purple' : row.textColor === 'green' ? 'text-green' : 'dn';
      var sub = row.detail
        ? '<div class="tb-sc-sub ' + subCls + '"><span class="ic">' + arr(row.textColor === 'green' ? 'down' : row.dir) + '</span> ' + esc(row.detail) + '</div>'
        : '';
      return '<div class="tb-sc-item"><div class="tb-sc-main ' + mainCls + '">' + esc(row.text) + '</div>' + sub + '</div>';
    }).join('');
  }

  function renderExecutive(mount, state) {
    var d = (global.LonrpTableauData && global.LonrpTableauData.EXECUTIVE_YTD) || {};
    var period = state.lonrpPeriod === 'mtd' ? 'MTD' : state.lonrpPeriod === 'fy' ? 'FULL YEAR' : 'YTD';
    var currency = state.lonrpCurrency || 'Local';
    var compare = state.lonrpCompare || 'vs. LY';

    mount.innerHTML =
      '<div class="tb-exec">' +
      '<header class="tb-exec-head">' +
      '<div><h1>EXECUTIVE SUMMARY | ' + esc(period) + '</h1>' +
      '<p class="tb-exec-sub">YTD High Level Summary of your business | Revenue in ' + esc(currency) +
      ' Currency | Comparison <em>' + esc(compare) + '</em></p></div>' +
      '<button type="button" class="tb-info-i" aria-label="Guidelines">i</button>' +
      '</header>' +

      '<section class="tb-exec-finance">' +
      '<div class="tb-exec-fin-label">Finance as of ' + esc(d.financeAsOf || '') + '</div>' +
      '<div class="tb-exec-fin-kpis">' +
      finKpi('Occ', (d.finance && d.finance.occ) || {}) +
      finKpi('ADR', (d.finance && d.finance.adr) || {}) +
      finKpi('RevPAR', (d.finance && d.finance.revpar) || {}) +
      '</div></section>' +

      '<div class="tb-exec-grid">' +
      '<div class="tb-ws tb-ws-ms"><div class="tb-ws-title">Market Share</div><div class="tb-ws-body">' +
      (d.marketShare.items || []).map(msMetric).join('') +
      '<div class="tb-ws-foot">As of ' + esc(d.marketShare.asOf) + '</div></div></div>' +

      '<div class="tb-ws tb-ws-lo"><div class="tb-ws-title">Loyalty Occupancy</div><div class="tb-ws-body">' +
      (d.loyalty.items || []).map(loyaltyMetric).join('') +
      '<div class="tb-ws-foot">As of ' + esc(d.loyalty.asOf) + '</div></div></div>' +

      '<div class="tb-ws tb-ws-di"><div class="tb-ws-title">Distribution</div><div class="tb-ws-body">' +
      (d.distribution.items || []).map(distMetric).join('') +
      '<div class="tb-ws-foot">As of ' + esc(d.distribution.asOf) + '</div></div></div>' +

      '<div class="tb-ws tb-ws-pace"><div class="tb-ws-title">' + esc(d.pace.title) + '</div><div class="tb-ws-body">' +
      paceChart(d.pace) +
      '<div class="tb-ws-foot">' + esc(d.pace.footer) + '</div></div></div>' +

      '<aside class="tb-ws tb-ws-sc"><div class="tb-ws-body tb-sc-body">' + scorecardHtml(d.scorecard || []) + '</aside>' +

      '<div class="tb-ws tb-ws-geo"><div class="tb-ws-title">' + esc(d.geo.title) + '</div>' +
      '<div class="tb-ws-body tb-donut-body">' +
      '<div class="tb-donut-wrap"><canvas id="lonrpExecGeo"></canvas>' +
      '<div class="tb-donut-ctr"><span class="t">' + esc(d.geo.title) + '</span><strong>' + esc(d.geo.center) + '</strong></div></div>' +
      '<div class="tb-donut-legend" id="lonrpExecGeoLeg"></div></div>' +
      '<div class="tb-ws-foot">As of ' + esc(d.geo.asOf) + '</div></div>' +

      '<div class="tb-ws tb-ws-gross"><div class="tb-ws-title">' + esc(d.gross.title) + ' ' +
      '<span class="tb-gross-hdr">' + esc(d.gross.total) + ' vs. LY <span class="dn">▼ ' + esc(d.gross.vsLy) + '</span></span></div>' +
      '<div class="tb-ws-body"><div class="tb-gross-chart"><canvas id="lonrpExecGross"></canvas></div></div>' +
      '<div class="tb-ws-foot">As of ' + esc(d.gross.asOf) + '</div></div>' +

      '<div class="tb-ws tb-ws-prem"><div class="tb-ws-title">' + esc(d.premium.title) + '</div>' +
      '<div class="tb-ws-body tb-donut-body">' +
      '<div class="tb-donut-wrap"><canvas id="lonrpExecPremium"></canvas>' +
      '<div class="tb-donut-ctr"><span class="t">' + esc(d.premium.title) + '</span><strong>' + esc(d.premium.center) + '</strong></div></div>' +
      '<div class="tb-donut-legend" id="lonrpExecPremLeg"></div></div>' +
      '<div class="tb-ws-foot">As of ' + esc(d.premium.asOf) + '</div></div>' +

      '</div></div>';
  }

  function destroyChart(key) {
    if (_charts[key]) { _charts[key].destroy(); delete _charts[key]; }
  }

  function renderDonut(canvasId, legId, spec) {
    var el = document.getElementById(canvasId);
    if (!el || !spec) return;
    var key = canvasId;
    destroyChart(key);
    var segs = spec.segments.filter(function (s) { return s.pct > 0; });
    _charts[key] = new Chart(el, {
      type: 'doughnut',
      data: {
        labels: segs.map(function (s) { return s.label; }),
        datasets: [{
          data: segs.map(function (s) { return s.pct; }),
          backgroundColor: segs.map(function (s) { return s.color; }),
          borderWidth: 2,
          borderColor: '#1a2438'
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '62%',
        plugins: { legend: { display: false } }
      }
    });
    var leg = document.getElementById(legId);
    if (leg) {
      leg.innerHTML = spec.segments.map(function (s) {
        return '<div class="tb-leg-row"><i style="background:' + s.color + '"></i>' +
          esc(s.label) + ' <b>' + s.pct + '%</b></div>';
      }).join('');
    }
  }

  function initExecutiveCharts() {
    if (typeof Chart === 'undefined' || !global.LonrpTableauData) return;
    var d = global.LonrpTableauData.EXECUTIVE_YTD;
    renderDonut('lonrpExecGeo', 'lonrpExecGeoLeg', d.geo);
    renderDonut('lonrpExecPremium', 'lonrpExecPremLeg', d.premium);

    destroyChart('gross');
    var grossEl = document.getElementById('lonrpExecGross');
    if (grossEl && d.gross) {
      var weeks = d.gross.weeks;
      _charts.gross = new Chart(grossEl, {
        type: 'bar',
        data: {
          labels: weeks.map(function (w) { return w.label; }),
          datasets: [{
            data: weeks.map(function (w) { return w.pct; }),
            backgroundColor: '#e8a735',
            borderSkipped: false,
            barPercentage: 0.85,
            categoryPercentage: 0.95
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: {
              grid: { display: false },
              ticks: { font: { size: 7 }, maxRotation: 90, minRotation: 90, autoSkip: false, color: '#94a3b8' }
            },
            y: {
              min: -15,
              max: 20,
              ticks: { stepSize: 5, callback: function (v) { return v + '%'; }, font: { size: 8 }, color: '#94a3b8' },
              grid: { color: 'rgba(148,163,184,0.12)' }
            }
          }
        }
      });
    }
  }

  global.LonrpExecutiveView = {
    render: renderExecutive,
    initCharts: initExecutiveCharts,
    destroyCharts: function () {
      Object.keys(_charts).forEach(function (k) { destroyChart(k); });
    }
  };
})(typeof window !== 'undefined' ? window : this);
