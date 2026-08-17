/* AH Commercial Performance Hub — illustrative data engine */
(function () {
  'use strict';

  if (typeof Chart !== 'undefined') {
    Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";
    Chart.defaults.font.size = 11;
    Chart.defaults.color = '#64748b';
  }

  var PROPS = [
    { id: 'lonrp', name: 'Northgate Riverside', short: 'Northgate', keys: 420 },
    { id: 'madrid', name: 'Madrid Aeropuerto Select', short: 'Madrid Aeropuerto', keys: 198 },
    { id: 'barcelona', name: 'Barcelona CBD Lifestyle', short: 'Barcelona CBD', keys: 186 },
    { id: 'lisbon', name: 'Lisbon Riverside', short: 'Lisbon Riverside', keys: 164 },
    { id: 'porto', name: 'Porto Historic Collection', short: 'Porto Historic', keys: 142 }
  ];

  var PERIOD_META = {
    mtd: { label: 'May 2026 MTD', periodTag: 'MTD', snapshot: 'Month-to-date P&L snapshot' },
    ytd: { label: 'Jan–May 2026 YTD', periodTag: 'YTD', snapshot: 'Year-to-date P&L snapshot' },
    fcst: { label: 'Jun–Aug 2026 forecast', periodTag: 'Forecast', snapshot: 'Forecast P&L (Jun–Aug)' }
  };

  /** @type {Record<string, Record<string, object>>} */
  var METRICS = {
    madrid: {
      mtd: {
        occ: 79, adr: 142, revpar: 112, roomsRevK: 580, gopK: 195, gopMargin: 33.6,
        bud: { revpar: 108, adr: 138, occ: 80, roomsRevK: 560, gopK: 188 },
        stly: { revpar: 105, adr: 136, occ: 77, roomsRevK: 545 },
        direct: 41, ota: 38, other: 21, rgi: 104, vsBudPct: 4.1,
        pillars: { sales: 82, dist: 71, yield: 85, digital: 76 },
        drivers: { sales: 'Group pipeline strong; 120 rms wash risk', dist: 'Direct 41%; on target', yield: 'RGI 104 vs comp', digital: 'Airport campaign live' },
        wf: { rooms: 580, fb: 145, other: 42, opex: 572, gop: 195 },
        pickup: { rms7: 48, rev7: 34, rms14: 92, otbVsFcst30: -1.2 },
        pace: { otb: [76, 78, 77, 75, 74, 73, 72, 71, 72, 71, 70, 69], stly: [74, 75, 76, 75, 74, 75, 76, 75, 74, 75, 76, 75], fcst: [78, 79, 78, 77, 76, 76, 75, 75, 74, 74, 73, 73] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [76, 74, 72], revpar: [118, 116, 114] },
        heatmap: ['ok', 'ok', 'strong', 'strong', 'ok', 'ok', 'ok', 'strong', 'strong', 'ok', 'ok', 'ok', 'ok', 'soft'],
        market: '<p><strong>Madrid airport submarket:</strong> RevPAR +5.1% STLY · Property +4.1% vs budget.</p><p style="margin-top:8px">Corporate &amp; crew demand stable; compression on Fri–Sun.</p>',
        commentary: 'Madrid holding vs budget on RevPAR; monitor group tentative wash (120 rms). Airport digital campaign performing.',
        briefBullets: ['RevPAR +4.1% vs budget on strong ADR.', 'Group wash risk on 120 tentative rooms — confirm by 5 Jun.', 'Rate index 104 — maintain BAR on compression weekends.']
      },
      ytd: {
        occ: 77, adr: 138, revpar: 106, roomsRevK: 2980, gopK: 998, gopMargin: 33.5,
        bud: { revpar: 104, adr: 135, occ: 78, roomsRevK: 2890, gopK: 960 },
        stly: { revpar: 100, adr: 132, occ: 76, roomsRevK: 2750 },
        direct: 40, ota: 39, other: 21, rgi: 103, vsBudPct: 1.9,
        pillars: { sales: 80, dist: 70, yield: 83, digital: 75 },
        drivers: { sales: 'YTD group +8%', dist: 'Direct stable', yield: 'RGI 103 YTD', digital: 'Always-on search' },
        wf: { rooms: 2980, fb: 720, other: 210, opex: 2912, gop: 998 },
        pickup: { rms7: 52, rev7: 38, rms14: 108, otbVsFcst30: -0.8 },
        pace: { otb: [75, 76, 75, 74, 73, 73, 72, 71, 71, 70, 70, 69], stly: [73, 74, 74, 73, 72, 73, 74, 73, 72, 73, 74, 73], fcst: [76, 77, 76, 75, 74, 74, 73, 73, 72, 72, 71, 71] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [75, 73, 71], revpar: [112, 110, 108] },
        heatmap: ['ok', 'ok', 'ok', 'strong', 'ok', 'ok', 'ok', 'ok', 'strong', 'ok', 'ok', 'soft', 'ok', 'ok'],
        market: '<p><strong>YTD market:</strong> Madrid cluster RevPAR +4.2% · Asset tracking in line.</p>',
        commentary: 'YTD RevPAR 106 vs budget 104; GOP margin in line. Group segment outperforming transient at airport.',
        briefBullets: ['YTD RevPAR €106 (+1.9% vs budget).', 'GOP €998K estimated YTD.', 'Forward Q3 pace aligned with forecast.']
      },
      fcst: {
        occ: 74, adr: 148, revpar: 109, roomsRevK: 1860, gopK: 610, gopMargin: 32.8,
        bud: { revpar: 112, adr: 145, occ: 76, roomsRevK: 1920, gopK: 640 },
        stly: { revpar: 104, adr: 142, occ: 73, roomsRevK: 1780 },
        direct: 42, ota: 37, other: 21, rgi: 105, vsBudPct: -2.7,
        pillars: { sales: 78, dist: 72, yield: 82, digital: 77 },
        drivers: { sales: 'Fcst group softer Aug', dist: 'Direct push planned', yield: 'Rate lift Jun weekends', digital: 'Retargeting Q3' },
        wf: { rooms: 1860, fb: 460, other: 130, opex: 1840, gop: 610 },
        pickup: { rms7: 41, rev7: 29, rms14: 78, otbVsFcst30: -2.4 },
        pace: { otb: [74, 73, 72, 71, 70, 69, 68, 67, 68, 67, 66, 65], stly: [72, 73, 73, 72, 71, 72, 73, 72, 71, 72, 73, 72], fcst: [75, 76, 75, 74, 73, 73, 72, 72, 71, 71, 70, 70] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [74, 72, 70], revpar: [109, 107, 105] },
        heatmap: ['soft', 'ok', 'ok', 'strong', 'strong', 'need', 'need', 'soft', 'ok', 'ok', 'soft', 'soft', 'ok', 'ok'],
        market: '<p><strong>Q3 fcst:</strong> Supply +2% keys in submarket; rate opportunity Jun 10–12.</p>',
        commentary: 'Forecast RevPAR 109 vs budget 112 for Jun–Aug; yield action on compression dates.',
        briefBullets: ['Fcst RevPAR €109 (−2.7% vs budget for quarter).', 'Jun compression weekends — BAR uplift planned.', 'Group wash scenario modeled in RMS.']
      }
    },
    barcelona: {
      mtd: {
        occ: 72, adr: 182, revpar: 131, roomsRevK: 610, gopK: 182, gopMargin: 29.8,
        bud: { revpar: 133, adr: 178, occ: 75, roomsRevK: 620, gopK: 195 },
        stly: { revpar: 128, adr: 175, occ: 73, roomsRevK: 595 },
        direct: 29, ota: 58, other: 13, rgi: 99, vsBudPct: -1.2,
        pillars: { sales: 74, dist: 48, yield: 72, digital: 70 },
        drivers: { sales: 'MICE pipeline soft', dist: 'OTA 58% — critical', yield: 'RGI 99; BAR gap weekends', digital: 'Reviews 4.4' },
        wf: { rooms: 610, fb: 198, other: 55, opex: 681, gop: 182 },
        pickup: { rms7: 32, rev7: 28, rms14: 58, otbVsFcst30: -4.8 },
        pace: { otb: [70, 71, 69, 67, 66, 65, 64, 63, 65, 64, 62, 61], stly: [71, 72, 71, 70, 69, 70, 71, 70, 69, 70, 71, 70], fcst: [73, 74, 73, 72, 71, 71, 70, 70, 69, 69, 68, 68] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [71, 68, 66], revpar: [128, 125, 122] },
        heatmap: ['soft', 'soft', 'ok', 'strong', 'strong', 'need', 'need', 'need', 'soft', 'ok', 'strong', 'strong', 'ok', 'soft'],
        market: '<p><strong>Barcelona CBD:</strong> Market RevPAR +3.2% STLY · Property −1.2% vs budget → <strong>share / channel issue.</strong></p><p style="margin-top:8px">MWC spillover 10–12 Jun — compression if BAR aligned.</p>',
        commentary: 'Barcelona OTA 58% (+6pp vs target). Parity audit and BAR reset required before Jun 14 weekend.',
        briefBullets: ['RevPAR below budget (−1.2%) despite strong ADR.', 'OTA dependency — parity audit in progress.', 'Yield opportunity Jun 14–16 if rate lifted €8–12.']
      },
      ytd: {
        occ: 74, adr: 179, revpar: 132, roomsRevK: 3120, gopK: 920, gopMargin: 29.5,
        bud: { revpar: 135, adr: 176, occ: 76, roomsRevK: 3180, gopK: 980 },
        stly: { revpar: 126, adr: 172, occ: 73, roomsRevK: 2980 },
        direct: 31, ota: 55, other: 14, rgi: 100, vsBudPct: -2.2,
        pillars: { sales: 72, dist: 52, yield: 74, digital: 71 },
        drivers: { sales: 'YTD MICE −6%', dist: 'OTA elevated all year', yield: 'RGI 100', digital: 'Brand.com project Q2' },
        wf: { rooms: 3120, fb: 1010, other: 280, opex: 3490, gop: 920 },
        pickup: { rms7: 35, rev7: 30, rms14: 62, otbVsFcst30: -3.9 },
        pace: { otb: [71, 72, 70, 69, 68, 68, 67, 66, 67, 66, 65, 64], stly: [70, 71, 71, 70, 69, 70, 71, 70, 69, 70, 71, 70], fcst: [72, 73, 72, 71, 70, 70, 69, 69, 68, 68, 67, 67] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [70, 67, 65], revpar: [130, 127, 124] },
        heatmap: ['ok', 'soft', 'ok', 'strong', 'need', 'need', 'need', 'soft', 'ok', 'ok', 'strong', 'ok', 'soft', 'soft'],
        market: '<p><strong>YTD:</strong> Citywide RevPAR +2.8% · Asset lagging on distribution.</p>',
        commentary: 'YTD channel mix driving underperformance vs comp set. Initiative: OTA parity audit (due 8 Jun).',
        briefBullets: ['YTD RevPAR €132 (−2.2% vs budget).', 'Distribution pillar weakest — OTA 55% YTD.', 'Recovery plan tied to direct + BAR reset.']
      },
      fcst: {
        occ: 69, adr: 185, revpar: 128, roomsRevK: 1780, gopK: 520, gopMargin: 29.2,
        bud: { revpar: 134, adr: 180, occ: 74, roomsRevK: 1850, gopK: 560 },
        stly: { revpar: 125, adr: 178, occ: 70, roomsRevK: 1720 },
        direct: 32, ota: 54, other: 14, rgi: 101, vsBudPct: -4.5,
        pillars: { sales: 70, dist: 55, yield: 75, digital: 72 },
        drivers: { sales: 'Fcst events partial', dist: 'OTA plan −4pp target', yield: 'Post-reset RGI 101', digital: 'Summer creative' },
        wf: { rooms: 1780, fb: 580, other: 160, opex: 2000, gop: 520 },
        pickup: { rms7: 28, rev7: 24, rms14: 48, otbVsFcst30: -5.2 },
        pace: { otb: [68, 67, 66, 65, 64, 63, 62, 61, 62, 61, 60, 59], stly: [69, 70, 69, 68, 67, 68, 69, 68, 67, 68, 69, 68], fcst: [70, 71, 70, 69, 68, 68, 67, 67, 66, 66, 65, 65] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [69, 66, 64], revpar: [128, 125, 123] },
        heatmap: ['need', 'need', 'soft', 'strong', 'strong', 'need', 'need', 'need', 'soft', 'ok', 'strong', 'strong', 'soft', 'soft'],
        market: '<p><strong>Q3 fcst:</strong> Events calendar moderate vs 2025.</p>',
        commentary: 'Forecast assumes OTA −4pp by Jul after parity program.',
        briefBullets: ['Fcst RevPAR €128 (−4.5% vs budget).', 'Parity program completion required for fcst.', 'Jun 14–16 yield action in RMS.']
      }
    },
    lisbon: {
      mtd: {
        occ: 68, adr: 168, revpar: 114, roomsRevK: 520, gopK: 168, gopMargin: 32.3,
        bud: { revpar: 114, adr: 165, occ: 69, roomsRevK: 518, gopK: 170 },
        stly: { revpar: 118, adr: 170, occ: 69, roomsRevK: 535 },
        direct: 36, ota: 48, other: 16, rgi: 97, vsBudPct: 0.3,
        pillars: { sales: 76, dist: 65, yield: 68, digital: 73 },
        drivers: { sales: 'Corporate steady', dist: 'OTA 48%', yield: 'RGI 97 — pace issue', digital: 'Review 4.5' },
        wf: { rooms: 520, fb: 132, other: 38, opex: 522, gop: 168 },
        pickup: { rms7: 22, rev7: 18, rms14: 44, otbVsFcst30: -8.0 },
        pace: { otb: [66, 65, 64, 62, 61, 60, 59, 58, 60, 59, 57, 56], stly: [69, 70, 69, 68, 67, 68, 69, 68, 67, 68, 69, 68], fcst: [68, 69, 68, 67, 66, 66, 65, 65, 64, 64, 63, 63] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [64, 62, 60], revpar: [112, 110, 108] },
        heatmap: ['ok', 'ok', 'soft', 'soft', 'need', 'need', 'need', 'need', 'need', 'soft', 'ok', 'ok', 'ok', 'soft'],
        market: '<p><strong>Lisbon:</strong> Market RevPAR +4% STLY · Asset −3.1% vs STLY on mid-June pace.</p>',
        commentary: 'Lisbon pace −8% for 12–18 Jun vs forecast. Recovery plan due 5 Jun.',
        briefBullets: ['Pace alert: mid-June OTB soft vs forecast.', 'RevPAR in line with budget but behind STLY.', 'RGI 97 — monitor vs improving market.']
      },
      ytd: {
        occ: 70, adr: 166, revpar: 116, roomsRevK: 2680, gopK: 865, gopMargin: 32.3,
        bud: { revpar: 117, adr: 164, occ: 71, roomsRevK: 2700, gopK: 870 },
        stly: { revpar: 119, adr: 168, occ: 71, roomsRevK: 2720 },
        direct: 37, ota: 46, other: 17, rgi: 98, vsBudPct: -0.9,
        pillars: { sales: 75, dist: 66, yield: 70, digital: 74 },
        drivers: { sales: 'YTD corporate +3%', dist: 'Direct improving', yield: 'RGI 98', digital: 'Stable' },
        wf: { rooms: 2680, fb: 680, other: 195, opex: 2690, gop: 865 },
        pickup: { rms7: 26, rev7: 20, rms14: 50, otbVsFcst30: -5.5 },
        pace: { otb: [68, 69, 68, 67, 66, 65, 64, 63, 64, 63, 62, 61], stly: [70, 71, 70, 69, 68, 69, 70, 69, 68, 69, 70, 69], fcst: [69, 70, 69, 68, 67, 67, 66, 66, 65, 65, 64, 64] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [65, 63, 61], revpar: [114, 112, 110] },
        heatmap: ['ok', 'ok', 'ok', 'soft', 'need', 'need', 'need', 'soft', 'ok', 'ok', 'ok', 'soft', 'ok', 'ok'],
        market: '<p><strong>YTD market +3.5%</strong> · Asset flat vs budget.</p>',
        commentary: 'YTD tracking budget on revenue; June pace risk emerging.',
        briefBullets: ['YTD RevPAR €116 (−0.9% vs budget).', 'Mid-June pace watch — recovery plan active.', 'Market growing faster than asset — share focus.']
      },
      fcst: {
        occ: 63, adr: 172, revpar: 108, roomsRevK: 1580, gopK: 495, gopMargin: 31.3,
        bud: { revpar: 115, adr: 168, occ: 68, roomsRevK: 1620, gopK: 520 },
        stly: { revpar: 114, adr: 169, occ: 67, roomsRevK: 1550 },
        direct: 38, ota: 45, other: 17, rgi: 99, vsBudPct: -6.1,
        pillars: { sales: 74, dist: 68, yield: 72, digital: 75 },
        drivers: { sales: 'Fcst groups lighter', dist: 'Direct +2pp plan', yield: 'Pace recovery built in Jul', digital: 'Q3 campaign' },
        wf: { rooms: 1580, fb: 400, other: 115, opex: 1600, gop: 495 },
        pickup: { rms7: 18, rev7: 14, rms14: 36, otbVsFcst30: -6.8 },
        pace: { otb: [62, 61, 60, 59, 58, 57, 56, 55, 56, 55, 54, 53], stly: [66, 67, 66, 65, 64, 65, 66, 65, 64, 65, 66, 65], fcst: [64, 65, 64, 63, 62, 62, 61, 61, 60, 60, 59, 59] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [63, 61, 59], revpar: [108, 106, 104] },
        heatmap: ['soft', 'need', 'need', 'need', 'need', 'need', 'need', 'soft', 'ok', 'ok', 'soft', 'ok', 'ok', 'soft'],
        market: '<p><strong>Q3:</strong> Tourism forecasts +5% arrivals — capture rate key.</p>',
        commentary: 'Fcst assumes pace recovery from Jul after Jun intervention.',
        briefBullets: ['Fcst RevPAR €108 (−6.1% vs budget).', 'Jun intervention critical to Jul fcst.', 'STR shows market still positive — execution gap.']
      }
    },
    porto: {
      mtd: {
        occ: 81, adr: 195, revpar: 158, roomsRevK: 700, gopK: 272, gopMargin: 38.9,
        bud: { revpar: 147, adr: 188, occ: 78, roomsRevK: 650, gopK: 248 },
        stly: { revpar: 144, adr: 182, occ: 79, roomsRevK: 665 },
        direct: 44, ota: 35, other: 21, rgi: 105, vsBudPct: 7.8,
        pillars: { sales: 80, dist: 78, yield: 88, digital: 79 },
        drivers: { sales: 'Leisure strong', dist: 'Direct 44%', yield: 'RGI 105', digital: 'Campaign renewed' },
        wf: { rooms: 700, fb: 185, other: 52, opex: 665, gop: 272 },
        pickup: { rms7: 84, rev7: 44, rms14: 118, otbVsFcst30: 2.1 },
        pace: { otb: [80, 81, 82, 81, 80, 79, 78, 77, 79, 78, 77, 76], stly: [78, 79, 80, 79, 78, 79, 80, 79, 78, 79, 80, 79], fcst: [81, 82, 81, 80, 79, 79, 78, 78, 77, 77, 76, 76] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [80, 78, 76], revpar: [160, 158, 155] },
        heatmap: ['strong', 'strong', 'ok', 'ok', 'strong', 'strong', 'ok', 'ok', 'strong', 'strong', 'ok', 'ok', 'soft', 'ok'],
        market: '<p><strong>Porto historic:</strong> Market +6% STLY · Asset outperforming (+7.8% vs budget).</p>',
        commentary: 'Porto best-in-portfolio; direct campaign renewal complete. Share best practices.',
        briefBullets: ['RevPAR +7.8% vs budget — portfolio leader.', 'Direct 44% exceeds target.', 'RGI 105 — maintain rate discipline through summer.']
      },
      ytd: {
        occ: 80, adr: 192, revpar: 154, roomsRevK: 3580, gopK: 1390, gopMargin: 38.8,
        bud: { revpar: 145, adr: 186, occ: 78, roomsRevK: 3380, gopK: 1280 },
        stly: { revpar: 141, adr: 180, occ: 78, roomsRevK: 3420 },
        direct: 43, ota: 36, other: 21, rgi: 104, vsBudPct: 6.2,
        pillars: { sales: 79, dist: 77, yield: 86, digital: 78 },
        drivers: { sales: 'YTD leisure +11%', dist: 'Direct leader', yield: 'RGI 104', digital: 'Strong ROAS' },
        wf: { rooms: 3580, fb: 950, other: 265, opex: 3405, gop: 1390 },
        pickup: { rms7: 88, rev7: 48, rms14: 125, otbVsFcst30: 1.8 },
        pace: { otb: [79, 80, 81, 80, 79, 78, 77, 76, 78, 77, 76, 75], stly: [77, 78, 79, 78, 77, 78, 79, 78, 77, 78, 79, 78], fcst: [80, 81, 80, 79, 78, 78, 77, 77, 76, 76, 75, 75] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [79, 77, 75], revpar: [158, 156, 153] },
        heatmap: ['strong', 'ok', 'ok', 'strong', 'strong', 'ok', 'ok', 'ok', 'strong', 'strong', 'ok', 'soft', 'ok', 'ok'],
        market: '<p><strong>YTD:</strong> Porto market +5.5% · Asset +6.2% vs budget.</p>',
        commentary: 'YTD standout; use as benchmark for Iberia cluster.',
        briefBullets: ['YTD RevPAR €154 (+6.2% vs budget).', 'GOP margin ~39% estimated.', 'Direct + digital model scalable to other assets.']
      },
      fcst: {
        occ: 77, adr: 198, revpar: 152, roomsRevK: 2100, gopK: 810, gopMargin: 38.6,
        bud: { revpar: 150, adr: 192, occ: 78, roomsRevK: 2050, gopK: 790 },
        stly: { revpar: 146, adr: 186, occ: 78, roomsRevK: 1980 },
        direct: 45, ota: 34, other: 21, rgi: 106, vsBudPct: 1.3,
        pillars: { sales: 81, dist: 79, yield: 87, digital: 80 },
        drivers: { sales: 'Fcst leisure sustained', dist: 'Direct 45% target', yield: 'RGI 106', digital: 'Always-on' },
        wf: { rooms: 2100, fb: 555, other: 155, opex: 2000, gop: 810 },
        pickup: { rms7: 76, rev7: 40, rms14: 102, otbVsFcst30: 0.9 },
        pace: { otb: [78, 77, 76, 75, 74, 73, 72, 71, 73, 72, 71, 70], stly: [76, 77, 78, 77, 76, 77, 78, 77, 76, 77, 78, 77], fcst: [79, 80, 79, 78, 77, 77, 76, 76, 75, 75, 74, 74] },
        forward: { labels: ['Jun', 'Jul', 'Aug'], occ: [78, 76, 74], revpar: [154, 152, 150] },
        heatmap: ['strong', 'strong', 'strong', 'ok', 'ok', 'strong', 'ok', 'ok', 'strong', 'ok', 'ok', 'ok', 'soft', 'soft'],
        market: '<p><strong>Q3 fcst:</strong> Leisure demand remains strong; limited new supply.</p>',
        commentary: 'Fcst continues to lead portfolio on RevPAR and margin.',
        briefBullets: ['Fcst RevPAR €152 (+1.3% vs budget).', 'Maintain direct share ≥44%.', 'Benchmark for cluster best practices.']
      }
    }
  };

  var ALERTS = [
    { id: 'a1', propId: 'lisbon', type: 'Pace', sev: 'crit', text: 'OTB −8% vs forecast for arrival week 12–18 Jun', impact: '~€42K', system: 'RMS', period: ['mtd', 'fcst'] },
    { id: 'a2', propId: 'barcelona', type: 'Channel', sev: 'crit', text: 'OTA share 58% (+6pp vs 4-wk avg & target)', impact: '~€28K', system: 'Channel manager', period: ['mtd', 'ytd'] },
    { id: 'a3', propId: 'barcelona', type: 'Rate', sev: 'warn', text: 'RGI < 100 for 3 days; BAR ~€12 below recommended', impact: '~€15K', system: 'RMS', period: ['mtd'] },
    { id: 'a4', propId: 'madrid', type: 'Group', sev: 'warn', text: '120 rms tentative wash risk; cutoff 5 Jun', impact: '~€22K', system: 'CRM', period: ['mtd', 'ytd', 'fcst'] },
    { id: 'a5', propId: 'lisbon', type: 'Market', sev: 'warn', text: 'Market RevPAR +4% STLY — asset underperforming (−3.1% vs STLY)', impact: '—', system: 'STR', period: ['mtd', 'ytd'] },
    { id: 'a6', propId: 'porto', type: 'Pace', sev: 'warn', text: 'Aug OTB pacing +2% vs fcst — confirm staffing', impact: '—', system: 'PMS', period: ['fcst'] }
  ];

  var PILLAR_LINKS = {
    sales: { title: 'Sales & group', system: 'CRM / sales tracker', url: 'https://example.com/crm' },
    dist: { title: 'Distribution', system: 'Channel manager', url: 'https://example.com/channel' },
    yield: { title: 'Pricing & yield', system: 'RMS / STR', url: 'https://example.com/rms' },
    digital: { title: 'Digital', system: 'Ads & reputation', url: 'https://example.com/ads' }
  };

  var GP_COLORS = {
    cyan: '#22d3ee', cyanSoft: 'rgba(34,211,238,0.2)', teal: '#2dd4bf',
    blue: '#38bdf8', blueSoft: 'rgba(56,189,248,0.15)',
    lime: '#4ade80', limeSoft: 'rgba(74,222,128,0.18)',
    slate: '#64748b', slateSoft: 'rgba(100,116,139,0.35)',
    track: '#334155', text: '#94a3b8', grid: 'rgba(148,163,184,0.12)'
  };

  function gpDarkScales(extra) {
    var base = {
      x: { ticks: { color: GP_COLORS.text }, grid: { color: GP_COLORS.grid } },
      y: { ticks: { color: GP_COLORS.text }, grid: { color: GP_COLORS.grid } }
    };
    if (!extra) return base;
    Object.keys(extra).forEach(function (k) { base[k] = extra[k]; });
    return base;
  }

  function gpDarkLegend() {
    return { labels: { color: '#cbd5e1', boxWidth: 10, usePointStyle: true, padding: 12 } };
  }

  var state = {
    period: 'ytd',
    scope: 'lonrp',
    dashboard: 'executive',
    lonrpDateFilter: '(All)',
    report: 'command',
    lonrpPeriod: 'ytd',
    lonrpCurrency: 'Local',
    lonrpCompare: 'vs. LY',
    lonrpBudget: 'Show',
    groupGranularity: 'daily',
    groupCompare: 'stly',
    scenario: { adrPct: 0, washPct: 8, conversionPct: 74 }
  };

  var lastPaceKpiItems = null;
  var lastPaceInsights = [];
  var lastPaceAnomalyByRow = {};
  var lastPaceContext = null;

  var INITIATIVES_BY_SCOPE = {
    portfolio: [
      { action: 'OTA parity audit (Barcelona)', start: '28 May', metric: 'Direct share', delta: '+2.1pp', revparK: 18, confidence: 'High' },
      { action: 'Airport digital campaign (Madrid)', start: '15 May', metric: 'Transient ADR', delta: '+€6', revparK: 12, confidence: 'Med' },
      { action: 'BAR reset weekends (Barcelona)', start: '6 Jun', metric: 'RGI', delta: '+3 pts', revparK: 15, confidence: 'Med' },
      { action: 'Group wash mitigation (Madrid)', start: '1 Jun', metric: 'Group rooms', delta: '−42 rms risk', revparK: 22, confidence: 'High' }
    ],
    madrid: [
      { action: 'Airport digital campaign', start: '15 May', metric: 'Transient ADR', delta: '+€6', revparK: 8, confidence: 'Med' },
      { action: 'Group wash mitigation playbook', start: '1 Jun', metric: 'Tentative wash', delta: '−120 → −78 rms', revparK: 22, confidence: 'High' }
    ],
    barcelona: [
      { action: 'OTA parity audit', start: '28 May', metric: 'OTA share', delta: '−4pp target', revparK: 28, confidence: 'High' },
      { action: 'BAR reset Jun 14–16', start: '6 Jun', metric: 'RGI', delta: '+4 pts', revparK: 15, confidence: 'Med' }
    ],
    lisbon: [
      { action: 'Corporate rate ladder refresh', start: '20 May', metric: 'Corp ADR', delta: '+€4', revparK: 6, confidence: 'Low' },
      { action: 'Pace recovery task force', start: '2 Jun', metric: 'OTB vs fcst', delta: '+1.8%', revparK: 9, confidence: 'Med' }
    ],
    porto: [
      { action: 'Heritage festival packaging', start: '10 May', metric: 'Aug OTB', delta: '+2.1% vs fcst', revparK: 11, confidence: 'High' }
    ]
  };

  var GROUP_REQUESTS = {
    madrid: [
      { name: 'Iberia Crew Training', dates: '12–14 Jun', rms: 85, rate: 118, transientDisp: 14200, decision: 'accept', band: '€118–122', rationale: 'Displacement €14.2K below incremental GOP €19.8K; fills soft Tue–Wed.' },
      { name: 'Tech Summit Block', dates: '18–20 Jun', rms: 120, rate: 105, transientDisp: 24800, decision: 'counter', band: '€112–118', rationale: 'Rate €13 below BAR; counter at €115 min or reduce to 80 rms on peak night.' },
      { name: 'Association Annual', dates: '2–5 Aug', rms: 200, rate: 98, transientDisp: 41000, decision: 'reject', band: '—', rationale: 'Aug compression — expected transient RevPAR €148; group ADR gap −34% vs displacement threshold.' }
    ],
    barcelona: [
      { name: 'MWC Spillover Block', dates: '10–12 Jun', rms: 60, rate: 195, transientDisp: 8200, decision: 'accept', band: '€195–205', rationale: 'Compression window; rate at BAR+; low displacement vs citywide demand.' },
      { name: 'Pharma Congress', dates: '22–25 Jul', rms: 140, rate: 168, transientDisp: 18500, decision: 'counter', band: '€175–182', rationale: 'Accept if ≥90 rms at €178+; otherwise reject peak night overlap.' }
    ],
    lisbon: [
      { name: 'River Cruise Alliance', dates: '8–10 Jul', rms: 45, rate: 155, transientDisp: 5200, decision: 'accept', band: '€155–160', rationale: 'Shoulder dates; displacement within tolerance.' }
    ],
    porto: [
      { name: 'Wine Expo Block', dates: '15–17 Sep', rms: 75, rate: 142, transientDisp: 6800, decision: 'accept', band: '€142–148', rationale: 'Festival halo; attributed initiative lift supports accept.' }
    ],
    portfolio: [
      { name: 'Iberia Crew Training (MAD)', dates: '12–14 Jun', rms: 85, rate: 118, transientDisp: 14200, decision: 'accept', band: '€118–122', rationale: 'Portfolio-level GOP positive; Madrid only.' },
      { name: 'Tech Summit Block (MAD)', dates: '18–20 Jun', rms: 120, rate: 105, transientDisp: 24800, decision: 'counter', band: '€112–118', rationale: 'Highest displacement in portfolio this week — counter required.' },
      { name: 'MWC Spillover (BCN)', dates: '10–12 Jun', rms: 60, rate: 195, transientDisp: 8200, decision: 'accept', band: '€195–205', rationale: 'Compression capture; aligns with BAR reset initiative.' }
    ]
  };
  var charts = {};

  function fmtEuro(n, dec) {
    if (n >= 1000) return '€' + (n / 1000).toFixed(1) + 'M';
    return '€' + Math.round(n) + (dec ? '' : '');
  }
  function fmtEuroK(k) {
    if (k >= 1000) return '€' + (k / 1000).toFixed(2).replace(/\.?0+$/, '') + 'M';
    return '€' + Math.round(k) + 'K';
  }
  function fmtPct(n, signed) {
    var s = (n > 0 ? '+' : '') + n.toFixed(1) + '%';
    return signed ? s : n.toFixed(1) + '%';
  }
  function fmtPp(n) {
    return (n > 0 ? '+' : '') + n.toFixed(1) + 'pp';
  }
  function fmtNum(n) {
    return Math.round(n).toLocaleString('en');
  }
  function fmtMoney(n) {
    return Number(n || 0).toLocaleString('en', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  }
  function sum(arr) {
    return arr.reduce(function (a, b) { return a + b; }, 0);
  }
  function pctClass(n) { return n >= 0 ? 'pos' : 'neg'; }
  function statusFromVsBud(v) {
    if (v >= 2) return 'g';
    if (v <= -1) return 'r';
    return 'a';
  }
  function pillarColor(score) {
    if (score >= 75) return 'var(--positive)';
    if (score < 60) return 'var(--negative)';
    return 'var(--primary)';
  }

  function getMetrics(propId, period) {
    return METRICS[propId][period];
  }

  function aggregatePortfolio(period) {
    var ids = PROPS.map(function (p) { return p.id; });
    var totalKeys = 0;
    var sumRevparKeys = 0;
    var sumOccKeys = 0;
    var sumAdrSold = 0;
    var soldKeys = 0;
    var roomsRevK = 0;
    var gopK = 0;
    var sumDirectSold = 0;
    var sumRgiKeys = 0;
    var pillars = { sales: 0, dist: 0, yield: 0, digital: 0 };
    var wf = { rooms: 0, fb: 0, other: 0, opex: 0, gop: 0 };
    var pickup = { rms7: 0, rev7: 0, rms14: 0, otbSum: 0 };
    var budRevparKeys = 0;
    var stlyRevparKeys = 0;
    var budRoomsK = 0;
    var budGopK = 0;
    var stlyRoomsK = 0;

    ids.forEach(function (id) {
      var p = PROPS.find(function (x) { return x.id === id; });
      var m = getMetrics(id, period);
      var k = p.keys;
      totalKeys += k;
      sumRevparKeys += m.revpar * k;
      sumOccKeys += m.occ * k;
      sumAdrSold += m.adr * (m.occ / 100) * k;
      soldKeys += (m.occ / 100) * k;
      sumDirectSold += m.direct * (m.occ / 100) * k;
      sumRgiKeys += m.rgi * m.revpar * k;
      roomsRevK += m.roomsRevK;
      gopK += m.gopK;
      pillars.sales += m.pillars.sales;
      pillars.dist += m.pillars.dist;
      pillars.yield += m.pillars.yield;
      pillars.digital += m.pillars.digital;
      wf.rooms += m.wf.rooms;
      wf.fb += m.wf.fb;
      wf.other += m.wf.other;
      wf.opex += m.wf.opex;
      wf.gop += m.wf.gop;
      pickup.rms7 += m.pickup.rms7;
      pickup.rev7 += m.pickup.rev7;
      pickup.rms14 += m.pickup.rms14;
      pickup.otbSum += m.pickup.otbVsFcst30;
      budRevparKeys += m.bud.revpar * k;
      stlyRevparKeys += m.stly.revpar * k;
      budRoomsK += m.bud.roomsRevK;
      budGopK += m.bud.gopK;
      stlyRoomsK += m.stly.roomsRevK;
    });

    var n = ids.length;
    var revpar = sumRevparKeys / totalKeys;
    var occ = sumOccKeys / totalKeys;
    var adr = sumAdrSold / soldKeys;
    var direct = sumDirectSold / soldKeys;
    var rgi = sumRgiKeys / sumRevparKeys;
    var budRevpar = budRevparKeys / totalKeys;
    var stlyRevpar = stlyRevparKeys / totalKeys;
    var vsBudPct = ((revpar - budRevpar) / budRevpar) * 100;
    var vsStlyPct = ((revpar - stlyRevpar) / stlyRevpar) * 100;
    var gopMargin = roomsRevK > 0 ? (gopK / roomsRevK) * 100 * (roomsRevK / (roomsRevK + wf.fb + wf.other)) : 31;

    return {
      id: 'portfolio',
      name: 'Portfolio (4 properties)',
      short: 'Portfolio',
      keys: totalKeys,
      occ: occ,
      adr: adr,
      revpar: revpar,
      roomsRevK: roomsRevK,
      gopK: gopK,
      gopMargin: (gopK / (roomsRevK + wf.fb + wf.other)) * 100,
      bud: { revpar: budRevpar, adr: budRevpar / (occ / 100) * 0.98, occ: occ + 2.1, roomsRevK: budRoomsK, gopK: budGopK },
      stly: { revpar: stlyRevpar, roomsRevK: stlyRoomsK },
      direct: direct,
      rgi: Math.round(rgi),
      vsBudPct: vsBudPct,
      vsStlyPct: vsStlyPct,
      pillars: {
        sales: Math.round(pillars.sales / n),
        dist: Math.round(pillars.dist / n),
        yield: Math.round(pillars.yield / n),
        digital: Math.round(pillars.digital / n)
      },
      drivers: {
        sales: 'Portfolio pipeline €1.4M; Madrid wash risk',
        dist: 'Weighted direct ' + direct.toFixed(0) + '% vs 42% target',
        yield: 'Weighted RGI ' + Math.round(rgi),
        digital: 'Mixed campaigns across 4 assets'
      },
      wf: wf,
      pickup: {
        rms7: pickup.rms7,
        rev7: pickup.rev7,
        rms14: pickup.rms14,
        otbVsFcst30: pickup.otbSum / n
      },
      pace: blendPace(ids, period),
      forward: blendForward(ids, period),
      heatmap: ['soft', 'ok', 'ok', 'strong', 'strong', 'need', 'need', 'soft', 'ok', 'strong', 'strong', 'ok', 'ok', 'soft'],
      market: '<p><strong>Iberia portfolio:</strong> Weighted RevPAR ' + fmtEuro(revpar) + ' · ' + fmtPct(vsBudPct, true) + ' vs budget · ' + fmtPct(vsStlyPct, true) + ' vs STLY.</p><p style="margin-top:8px">Outliers: Porto ahead; Barcelona &amp; Lisbon need action.</p>',
      commentary: 'Portfolio −2.1pp occ vs budget (weighted). Barcelona OTA and Lisbon pace are primary risks. Porto outperforming.',
      briefBullets: [
        'Portfolio RevPAR ' + fmtEuro(revpar) + ' (' + fmtPct(vsBudPct, true) + ' vs budget).',
        'Barcelona distribution & Lisbon pace — P1 initiatives active.',
        'Porto leading; GOP €' + Math.round(gopK) + 'K ' + (period === 'fcst' ? 'forecast' : 'estimated') + ' on €' + Math.round(roomsRevK) + 'K rooms revenue.'
      ]
    };
  }

  function blendPace(ids, period) {
    var otb = [], stly = [], fcst = [];
    for (var i = 0; i < 12; i++) {
      var o = 0, s = 0, f = 0;
      ids.forEach(function (id) {
        var p = getMetrics(id, period).pace;
        o += p.otb[i]; s += p.stly[i]; f += p.fcst[i];
      });
      otb.push(o / ids.length);
      stly.push(s / ids.length);
      fcst.push(f / ids.length);
    }
    return { otb: otb, stly: stly, fcst: fcst };
  }

  function blendForward(ids, period) {
    var m0 = getMetrics(ids[0], period).forward;
    var occ = [0, 0, 0], revpar = [0, 0, 0];
    ids.forEach(function (id) {
      var f = getMetrics(id, period).forward;
      for (var i = 0; i < 3; i++) {
        occ[i] += f.occ[i];
        revpar[i] += f.revpar[i];
      }
    });
    return {
      labels: m0.labels,
      occ: occ.map(function (v) { return v / ids.length; }),
      revpar: revpar.map(function (v) { return v / ids.length; })
    };
  }

  function getView() {
    if (state.scope === 'portfolio') return aggregatePortfolio(state.period);
    var scopeId = state.scope === 'lonrp' ? 'madrid' : state.scope;
    var base = getMetrics(scopeId, state.period);
    var p = PROPS.find(function (x) { return x.id === state.scope; });
    var view = Object.assign({ id: p.id, name: p.name, short: p.short, keys: p.keys }, base);
    if (state.scope === 'lonrp') {
      view.name = 'Northgate Riverside';
      view.short = 'Northgate';
      view.keys = 420;
      view.market = '<p><strong>Metro centre submarket:</strong> RevPAR +3.2% STLY · Property tracking in line with comp set.</p>';
    }
    return view;
  }

  function syncLonrpPeriod() {
    state.lonrpPeriod = state.period === 'mtd' ? 'mtd' : state.period === 'fcst' ? 'fy' : 'ytd';
  }

  function syncPeriodSegButtons() {
    document.querySelectorAll('#periodSeg button').forEach(function (b) {
      b.classList.toggle('active', b.getAttribute('data-period') === state.period);
    });
  }

  function showToast(msg) {
    var t = document.getElementById('toast');
    t.textContent = msg;
    t.classList.add('show');
    clearTimeout(showToast._tid);
    showToast._tid = setTimeout(function () { t.classList.remove('show'); }, 2800);
  }

  function getExecutiveKpiItems(v) {
    var vsStly = ((v.revpar - v.stly.revpar) / v.stly.revpar) * 100;
    var varRoomsK = v.roomsRevK - v.bud.roomsRevK;
    return [
      { k: 'RevPAR', v: fmtEuro(v.revpar), a: fmtEuro(v.revpar), b: fmtEuro(v.bud.revpar), s: fmtEuro(v.stly.revpar) + ' (' + fmtPct(vsStly, true) + ')', sc: pctClass(vsStly) },
      { k: 'ADR', v: fmtEuro(v.adr), a: fmtEuro(v.adr), b: fmtEuro(v.bud.adr), s: fmtPct(((v.adr - v.stly.revpar / (v.occ / 100) * 0.95) / v.adr) * 100, true), sc: 'pos' },
      { k: 'Occupancy', v: v.occ.toFixed(1) + '%', a: v.occ.toFixed(1) + '%', b: v.bud.occ.toFixed(1) + '%', s: fmtPp(v.occ - v.bud.occ), sc: pctClass(v.occ - v.bud.occ) },
      { k: 'Rooms revenue', v: fmtEuroK(v.roomsRevK), a: fmtEuroK(v.roomsRevK), b: fmtEuroK(v.bud.roomsRevK), s: (varRoomsK >= 0 ? '+' : '') + '€' + Math.round(varRoomsK) + 'K', sc: pctClass(varRoomsK) },
      { k: state.period === 'fcst' ? 'GOP (fcst)' : 'GOP (est.)', v: fmtEuroK(v.gopK), a: fmtEuroK(v.gopK), b: fmtEuroK(v.bud.gopK), s: v.gopMargin.toFixed(1) + '% margin', sc: 'pos' },
      { k: 'RGI / Direct', v: String(v.rgi), a: 'Direct ' + v.direct.toFixed(0) + '%', b: 'Target 42%', s: fmtPp(v.direct - 42), sc: pctClass(v.direct - 42) }
    ];
  }

  function buildPaceKpiItems(currTotal, prevTotal, avgCurrAdr, avgPrevAdr, avgCurrRevpar, avgPrevRevpar, rowsAgg) {
    var compareMap = { stly: 'STLY', budget: 'Budget', forecast: 'Forecast' };
    var cmp = compareMap[state.groupCompare] || 'STLY';
    var paceRooms = currTotal.t - prevTotal.t;
    var paceRev = currTotal.rev - prevTotal.rev;
    var adrDelta = avgCurrAdr - avgPrevAdr;
    var revparDelta = avgCurrRevpar - avgPrevRevpar;
    var pickup7 = rowsAgg.slice(-Math.min(3, rowsAgg.length)).reduce(function (acc, r) { return acc + (r.currRooms - r.prevRooms); }, 0);
    var needCount = rowsAgg.filter(function (r) { return (r.currOcc - r.prevOcc) < -1.2; }).length;
    return [
      { k: 'Rooms pace', v: (paceRooms >= 0 ? '+' : '') + fmtNum(paceRooms), a: 'Δ vs ' + cmp, b: 'Group room nights', s: (paceRooms >= 0 ? '+' : '') + fmtNum(paceRooms) + ' rms', sc: pctClass(paceRooms) },
      { k: 'Revenue pace', v: (paceRev >= 0 ? '+' : '') + fmtMoney(paceRev), a: 'Δ vs ' + cmp, b: 'Group revenue', s: (paceRev >= 0 ? '+' : '') + fmtMoney(paceRev), sc: pctClass(paceRev) },
      { k: 'ADR Δ', v: (adrDelta >= 0 ? '+' : '') + adrDelta.toFixed(1), a: 'Pace period', b: 'Avg daily rate', s: (adrDelta >= 0 ? '+' : '') + adrDelta.toFixed(1), sc: pctClass(adrDelta) },
      { k: 'RevPAR Δ', v: (revparDelta >= 0 ? '+' : '') + revparDelta.toFixed(2), a: 'Pace period', b: 'RevPAR', s: (revparDelta >= 0 ? '+' : '') + revparDelta.toFixed(2), sc: pctClass(revparDelta) },
      { k: 'Pickup recent', v: (pickup7 >= 0 ? '+' : '') + fmtNum(pickup7), a: 'Last periods', b: 'vs ' + cmp, s: (pickup7 >= 0 ? '+' : '') + fmtNum(pickup7) + ' rms', sc: pctClass(pickup7) },
      { k: 'Need dates', v: String(needCount), a: 'Below threshold', b: 'Pace periods', s: needCount <= 1 ? 'On track' : 'Review', sc: needCount <= 1 ? 'pos' : 'neg' }
    ];
  }

  function renderKpiMiniHtml(items) {
    return items.map(function (it) {
      return '<div class="kpi-mini"><div class="k">' + it.k + '</div><div class="v">' + it.v + '</div>' +
        '<div class="row"><span>Actual</span><span>' + it.a + '</span></div>' +
        '<div class="row"><span>Budget</span><span>' + it.b + '</span></div>' +
        '<div class="row"><span>STLY / Var</span><span class="' + it.sc + '">' + it.s + '</span></div></div>';
    }).join('');
  }

  function renderGpHeroTile(it) {
    return '<div class="gp-hero-tile" title="' + escAttr(it.k + ': ' + it.v + ' · ' + it.s) + '">' +
      '<span class="gp-hero-tile-k">' + it.k + '</span>' +
      '<b>' + it.v + '</b>' +
      '<span class="gp-hero-tile-sub ' + it.sc + '">' + it.s + '</span></div>';
  }

  function renderKpiStrip(v) {
    var el = document.getElementById('kpiStrip');
    if (!el) return;
    /* Pace workspace uses the unified teal hero band — avoid duplicating the same metrics in white cards */
    if (state.report === 'pace') {
      el.innerHTML = '';
      el.classList.remove('kpi-strip--twelve');
      el.classList.add('hidden');
      return;
    }
    el.classList.remove('hidden');
    var items = getExecutiveKpiItems(v);
    el.innerHTML = renderKpiMiniHtml(items);
    el.classList.toggle('kpi-strip--twelve', false);
  }

  function renderAlerts() {
    var list = ALERTS.filter(function (a) {
      if (a.period.indexOf(state.period) < 0) return false;
      if (state.scope !== 'portfolio' && a.propId !== state.scope) return false;
      return true;
    });
    list = list.slice(0, 7);
    document.querySelector('#alertsCard .card-h h3').innerHTML =
      'Exception inbox <span class="sub">' + list.length + ' shown (max 7) · ' + PERIOD_META[state.period].periodTag + '</span>';
    document.getElementById('alertsList').innerHTML = list.length ? list.map(function (a) {
      var prop = PROPS.find(function (p) { return p.id === a.propId; });
      return '<div class="alert-item">' +
        '<span class="alert-sev ' + a.sev + '"></span>' +
        '<span class="alert-type">' + a.type + '</span>' +
        '<span>' + prop.short + ' — ' + a.text + '</span>' +
        '<span class="alert-impact">' + a.impact + '</span>' +
        '<a class="link-ext" href="#" data-system="' + a.system + '" data-prop="' + a.propId + '">' + a.system + ' →</a></div>';
    }).join('') : '<p style="font-size:12px;color:var(--muted)">No exceptions for this view.</p>';
  }

  function renderPillars(v) {
    var keys = ['sales', 'dist', 'yield', 'digital'];
    var labels = { sales: 'Sales & group', dist: 'Distribution', yield: 'Pricing & yield', digital: 'Digital' };
    document.getElementById('pillarAsOf').textContent = (state.scope === 'portfolio' ? 'Portfolio' : v.short) + ' · ' + PERIOD_META[state.period].label;
    document.getElementById('pillarGrid').innerHTML = keys.map(function (key) {
      var sc = v.pillars[key];
      var col = pillarColor(sc);
      return '<div class="pillar" data-pillar="' + key + '">' +
        '<div class="name">' + labels[key] + '</div>' +
        '<div class="score" style="color:' + col + '">' + sc + '</div>' +
        '<div class="score-bar"><i style="width:' + sc + '%;background:' + col + '"></i></div>' +
        '<div class="driver">' + v.drivers[key] + '</div></div>';
    }).join('');
  }

  function renderPickup(v) {
    document.getElementById('pickupStats').innerHTML =
      '<div class="pickup-stat"><div class="n">+' + v.pickup.rms7 + '</div><div class="l">Pickup rms · 7d</div></div>' +
      '<div class="pickup-stat"><div class="n">+' + v.pickup.rev7 + 'K</div><div class="l">Pickup rev · 7d</div></div>' +
      '<div class="pickup-stat"><div class="n">+' + v.pickup.rms14 + '</div><div class="l">Pickup rms · 14d</div></div>' +
      '<div class="pickup-stat"><div class="n" style="color:' + (v.pickup.otbVsFcst30 < 0 ? 'var(--negative)' : 'var(--positive)') + '">' +
      fmtPct(v.pickup.otbVsFcst30, true) + '</div><div class="l">OTB vs forecast · 30d</div></div>';
    renderPaceHealthSummary(v);
  }

  function getForecastError30d(v) {
    var row = getForecastAccuracy(v).find(function (r) { return r.days === 30; });
    return row ? row.errorPct : 0;
  }

  function renderPaceHealthSummary(v) {
    var bar = document.getElementById('paceHealthBar');
    if (!bar) return;
    var err30 = getForecastError30d(v);
    var fails = getFailingAssumptions(v);
    var anomalies = detectAnomalies(v);
    var highAnom = anomalies.filter(function (a) { return a.severity === 'High'; }).length;
    var errCls = err30 <= 3 ? 'ok' : err30 <= 6 ? 'warn' : 'bad';
    var anomCls = highAnom > 0 ? 'bad' : anomalies.length > 0 ? 'warn' : 'ok';
    bar.innerHTML =
      '<span class="chip ' + errCls + '"><strong>Forecast trust @ 30d</strong> ' + err30.toFixed(1) + '% error</span>' +
      '<span class="chip ' + (fails.length ? 'warn' : 'ok') + '"><strong>Failing assumptions</strong> ' + fails.length + '</span>' +
      '<span class="chip ' + anomCls + '"><strong>Pace anomalies</strong> ' + anomalies.length + (highAnom ? ' (' + highAnom + ' high)' : '') + '</span>' +
      '<button type="button" class="link-pace" id="paceHealthGoGroup">Open pace workspace →</button>';
  }

  function renderExecutiveTiles(v) {
    var el = document.getElementById('lonrpExecTiles');
    if (!el) return;
    var paceRooms = v.pace && v.pace.otb.length ? v.pace.otb[v.pace.otb.length - 1] - v.pace.stly[v.pace.stly.length - 1] : -4.2;
    el.innerHTML =
      '<div class="lonrp-tile"><h4>MS Index</h4><div class="lonrp-big-val">104</div><p class="' + pctClass(2) + '">+2.1 vs LY</p></div>' +
      '<div class="lonrp-tile"><h4>Penetration</h4><div class="lonrp-big-val">' + v.direct.toFixed(0) + '%</div><p>Direct share</p></div>' +
      '<div class="lonrp-tile"><h4>Distribution</h4><div class="lonrp-big-val ' + pctClass(v.direct - 42) + '">' + (v.direct - 42 >= 0 ? '+' : '') + (v.direct - 42).toFixed(1) + 'pp</div><p>vs 42% target</p></div>' +
      '<div class="lonrp-tile"><h4>Booking pace</h4><div class="lonrp-big-val ' + pctClass(paceRooms) + '">' + (paceRooms >= 0 ? '+' : '') + paceRooms.toFixed(1) + 'pp</div><p><button type="button" class="btn-text" data-goto-dashboard="pace">Open pace →</button></p></div>';
  }

  function setDashboard(dashboard) {
    state.dashboard = dashboard;
    state.report = dashboard === 'executive' ? 'command' : dashboard === 'pace' ? 'pace' : dashboard === 'accounts' ? 'decisions' : dashboard;
    renderAll();
    if (dashboard === 'pace') {
      var sec = document.getElementById('groupPaceSection');
      if (sec) sec.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }

  function setReport(report) {
    var map = { command: 'executive', pace: 'pace', decisions: 'accounts' };
    setDashboard(map[report] || report);
  }

  function gpGradient(chart, c0, c1) {
    var ctx = chart.ctx;
    var area = chart.chartArea;
    if (!area) return c0;
    var g = ctx.createLinearGradient(0, area.bottom, 0, area.top);
    g.addColorStop(0, c0);
    g.addColorStop(1, c1);
    return g;
  }

  function gpSparkBar(delta, maxAbs) {
    var pct = maxAbs > 0 ? Math.min(100, (Math.abs(delta) / maxAbs) * 100) : 0;
    var barCls = delta < 0 ? 'neg' : '';
    var sign = delta >= 0 ? '+' : '';
    return '<div class="gp-spark"><div class="bar"><i class="' + barCls + '" style="width:' + pct + '%"></i></div>' +
      '<span class="num ' + (delta >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + sign + fmtNum(delta) + '</span></div>';
  }

  function gpHeatClass(dRooms, dOcc) {
    if (dRooms > 80 || dOcc > 2) return 'ahead';
    if (dRooms < -80 || dOcc < -2) return 'risk';
    if (dRooms < 0 || dOcc < -0.5) return 'behind';
    return 'on';
  }

  function renderGroupPace(v) {
    var body = document.getElementById('groupPaceBody');
    if (!body) return;
    var prevHdr = document.getElementById('groupPrevHdr');
    var compareMap = { stly: 'Prior year', budget: 'Budget', forecast: 'Forecast' };
    var compareKeyMap = { stly: 'stly', budget: 'bud', forecast: 'fcst' };
    var compareKey = compareKeyMap[state.groupCompare] || 'stly';
    if (prevHdr) prevHdr.textContent = compareMap[state.groupCompare] || 'Prior year';
    var periodLabel = PERIOD_META[state.period].label;
    var priorLabel = state.groupCompare === 'stly' ? 'Comparable prior period' : ('Comparable ' + compareMap[state.groupCompare].toLowerCase() + ' view');
    document.getElementById('groupPaceTitleLeft').textContent =
      (state.scope === 'portfolio' ? 'Portfolio' : v.short) + ' · ' + periodLabel;
    document.getElementById('groupPaceTitleRight').textContent = priorLabel;
    document.getElementById('groupPaceAsOf').textContent = 'Generated on ' + new Date().toLocaleString('en-GB', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' });

    var labelsDaily = state.period === 'mtd'
      ? ['10/01', '10/03', '10/05', '10/07', '10/09', '10/11', '10/13', '10/15', '10/17', '10/19', '10/21', '10/23']
      : state.period === 'ytd'
        ? ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        : ['Jun W1', 'Jun W2', 'Jun W3', 'Jun W4', 'Jul W1', 'Jul W2', 'Jul W3', 'Jul W4', 'Aug W1', 'Aug W2', 'Aug W3', 'Aug W4'];

    var roomFactor = state.period === 'mtd' ? 0.23 : state.period === 'ytd' ? 0.85 : 0.3;
    var currTotal = { t: 0, occ: 0, rev: 0, adr: 0, revpar: 0 };
    var prevTotal = { t: 0, occ: 0, rev: 0, adr: 0, revpar: 0 };
    var rows = [];

    var rowsRaw = [];
    for (var i = 0; i < 12; i++) {
      var currOcc = Math.max(0, v.pace.otb[i]);
      var prevOcc = compareKey === 'stly'
        ? Math.max(0, v.pace.stly[i])
        : compareKey === 'fcst'
          ? Math.max(0, v.pace.fcst[i])
          : Math.max(0, v.bud.occ + (i % 2 === 0 ? 0.6 : -0.4));
      var currRooms = Math.max(0, Math.round(v.keys * (currOcc / 100) * roomFactor));
      var prevRooms = Math.max(0, Math.round(v.keys * (prevOcc / 100) * roomFactor));
      var currAdr = v.adr * (0.93 + (i % 3) * 0.02);
      var prevAdr = (compareKey === 'stly' ? (v.stly.revpar / Math.max(0.4, prevOcc / 100)) : (compareKey === 'fcst' ? v.adr * 0.99 : v.bud.adr)) * (0.91 + (i % 3) * 0.02);
      var currRev = currRooms * currAdr;
      var prevRev = prevRooms * prevAdr;
      var currRevpar = currRooms > 0 ? currRev / v.keys : 0;
      var prevRevpar = prevRooms > 0 ? prevRev / v.keys : 0;
      currTotal.t += currRooms; currTotal.occ += currOcc; currTotal.rev += currRev; currTotal.adr += currAdr; currTotal.revpar += currRevpar;
      prevTotal.t += prevRooms; prevTotal.occ += prevOcc; prevTotal.rev += prevRev; prevTotal.adr += prevAdr; prevTotal.revpar += prevRevpar;

      var dT = currRooms - prevRooms;
      var dOcc = currOcc - prevOcc;
      var dRev = currRev - prevRev;
      var dAdr = currAdr - prevAdr;
      var dRevpar = currRevpar - prevRevpar;
      rowsRaw.push({
        label: labelsDaily[i],
        currRooms: currRooms, currOcc: currOcc, currRev: currRev, currAdr: currAdr, currRevpar: currRevpar,
        prevRooms: prevRooms, prevOcc: prevOcc, prevRev: prevRev, prevAdr: prevAdr, prevRevpar: prevRevpar
      });
    }

    function aggregateRows(granularity, input) {
      if (granularity === 'daily') return input.slice();
      var groups = [];
      var size = granularity === 'weekly' ? 2 : 4;
      for (var g = 0; g < input.length; g += size) {
        var chunk = input.slice(g, g + size);
        var cRooms = sum(chunk.map(function (r) { return r.currRooms; }));
        var pRooms = sum(chunk.map(function (r) { return r.prevRooms; }));
        var cRev = sum(chunk.map(function (r) { return r.currRev; }));
        var pRev = sum(chunk.map(function (r) { return r.prevRev; }));
        var cOcc = sum(chunk.map(function (r) { return r.currOcc; })) / chunk.length;
        var pOcc = sum(chunk.map(function (r) { return r.prevOcc; })) / chunk.length;
        var cAdr = cRooms > 0 ? cRev / cRooms : 0;
        var pAdr = pRooms > 0 ? pRev / pRooms : 0;
        var cRevpar = cRev / v.keys;
        var pRevpar = pRev / v.keys;
        groups.push({
          label: chunk[0].label + (chunk.length > 1 ? '–' + chunk[chunk.length - 1].label : ''),
          currRooms: cRooms, currOcc: cOcc, currRev: cRev, currAdr: cAdr, currRevpar: cRevpar,
          prevRooms: pRooms, prevOcc: pOcc, prevRev: pRev, prevAdr: pAdr, prevRevpar: pRevpar
        });
      }
      return groups;
    }

    var rowsAgg = aggregateRows(state.groupGranularity, rowsRaw);
    lastPaceInsights = enrichAnomaliesWithRows(v, rowsAgg);
    lastPaceAnomalyByRow = buildAnomalyByRowIndex(lastPaceInsights);
    var maxAbsRooms = Math.max.apply(null, rowsAgg.map(function (r) { return Math.abs(r.currRooms - r.prevRooms); }).concat([1]));
    rowsAgg.forEach(function (r, idx) {
      var dT = r.currRooms - r.prevRooms;
      var dOcc = r.currOcc - r.prevOcc;
      var dRev = r.currRev - r.prevRev;
      var dAdr = r.currAdr - r.prevAdr;
      var dRevpar = r.currRevpar - r.prevRevpar;
      var anom = lastPaceAnomalyByRow[idx];
      var rowSignalCls = anom ? ' row-has-signal ' + insightSignalClass(anom.severity) : '';
      var dateTip = anom ? ' data-insight="' + escAttr(insightTooltipText(anom)) + '"' : '';
      var signalDot = anom ? '<span class="gp-signal-dot" aria-hidden="true"></span> ' : '';
      rows.push(
        '<tr class="' + rowSignalCls.trim() + '">' +
        '<td class="date gp-signal-cell"' + dateTip + '>' + signalDot + r.label + '</td>' +
        '<td class="curr-col">' + fmtNum(r.currRooms) + '</td><td class="curr-col">' + r.currOcc.toFixed(1) + '</td><td class="curr-col">' + fmtMoney(r.currRev) + '</td><td class="curr-col">' + r.currAdr.toFixed(0) + '</td><td class="curr-col">' + r.currRevpar.toFixed(2) + '</td>' +
        '<td class="prev-col">' + fmtNum(r.prevRooms) + '</td><td class="prev-col">' + r.prevOcc.toFixed(1) + '</td><td class="prev-col">' + fmtMoney(r.prevRev) + '</td><td class="prev-col">' + r.prevAdr.toFixed(0) + '</td><td class="prev-col">' + r.prevRevpar.toFixed(2) + '</td>' +
        '<td class="diff-col">' + gpSparkBar(dT, maxAbsRooms) + '</td>' +
        '<td class="diff-col ' + (dOcc >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + (dOcc >= 0 ? '+' : '') + dOcc.toFixed(1) + '</td>' +
        '<td class="diff-col ' + (dRev >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + (dRev >= 0 ? '+' : '') + fmtMoney(dRev) + '</td>' +
        '<td class="diff-col ' + (dAdr >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + (dAdr >= 0 ? '+' : '') + dAdr.toFixed(0) + '</td>' +
        '<td class="diff-col ' + (dRevpar >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + (dRevpar >= 0 ? '+' : '') + dRevpar.toFixed(2) + '</td>' +
        '</tr>'
      );
    });

    var avgCurrOcc = currTotal.occ / 12;
    var avgPrevOcc = prevTotal.occ / 12;
    var avgCurrAdr = currTotal.adr / 12;
    var avgPrevAdr = prevTotal.adr / 12;
    var avgCurrRevpar = currTotal.revpar / 12;
    var avgPrevRevpar = prevTotal.revpar / 12;
    rows.push(
      '<tr class="total">' +
      '<td class="date">TOTAL</td>' +
      '<td>' + fmtNum(currTotal.t) + '</td><td>' + avgCurrOcc.toFixed(1) + '</td><td>' + fmtMoney(currTotal.rev) + '</td><td>' + avgCurrAdr.toFixed(0) + '</td><td>' + avgCurrRevpar.toFixed(2) + '</td>' +
      '<td>' + fmtNum(prevTotal.t) + '</td><td>' + avgPrevOcc.toFixed(1) + '</td><td>' + fmtMoney(prevTotal.rev) + '</td><td>' + avgPrevAdr.toFixed(0) + '</td><td>' + avgPrevRevpar.toFixed(2) + '</td>' +
      '<td class="' + ((currTotal.t - prevTotal.t) >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + ((currTotal.t - prevTotal.t) >= 0 ? '+' : '') + fmtNum(currTotal.t - prevTotal.t) + '</td>' +
      '<td class="' + ((avgCurrOcc - avgPrevOcc) >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + ((avgCurrOcc - avgPrevOcc) >= 0 ? '+' : '') + (avgCurrOcc - avgPrevOcc).toFixed(1) + '</td>' +
      '<td class="' + ((currTotal.rev - prevTotal.rev) >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + ((currTotal.rev - prevTotal.rev) >= 0 ? '+' : '') + fmtMoney(currTotal.rev - prevTotal.rev) + '</td>' +
      '<td class="' + ((avgCurrAdr - avgPrevAdr) >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + ((avgCurrAdr - avgPrevAdr) >= 0 ? '+' : '') + (avgCurrAdr - avgPrevAdr).toFixed(0) + '</td>' +
      '<td class="' + ((avgCurrRevpar - avgPrevRevpar) >= 0 ? 'group-pace-pos' : 'group-pace-neg') + '">' + ((avgCurrRevpar - avgPrevRevpar) >= 0 ? '+' : '') + (avgCurrRevpar - avgPrevRevpar).toFixed(2) + '</td>' +
      '</tr>'
    );

    body.innerHTML = rows.join('');
    renderGroupPaceSummary(currTotal, prevTotal, avgCurrOcc, avgPrevOcc, avgCurrAdr, avgPrevAdr, avgCurrRevpar, avgPrevRevpar, rowsAgg);
    renderGroupPaceVisuals(v, rowsAgg, currTotal, prevTotal, paceRevFromSummary(currTotal, prevTotal));
    renderGroupPaceDiagnostics(v, rowsAgg);
    lastPaceContext = buildPaceContext(v, rowsAgg, currTotal, prevTotal, avgCurrOcc, avgPrevOcc, avgCurrAdr, avgPrevAdr, avgCurrRevpar, avgPrevRevpar);
  }

  function buildPaceContext(v, rowsAgg, currTotal, prevTotal, avgCurrOcc, avgPrevOcc, avgCurrAdr, avgPrevAdr, avgCurrRevpar, avgPrevRevpar) {
    var compareMap = { stly: 'STLY', budget: 'Budget', forecast: 'Forecast' };
    var cmpLabel = compareMap[state.groupCompare] || 'STLY';
    var paceRooms = currTotal.t - prevTotal.t;
    var paceRev = currTotal.rev - prevTotal.rev;
    var pacePct = prevTotal.t > 0 ? ((paceRooms / prevTotal.t) * 100) : 0;
    var revPct = prevTotal.rev > 0 ? ((paceRev / prevTotal.rev) * 100) : 0;
    var adrDelta = avgCurrAdr - avgPrevAdr;
    var revparDelta = avgCurrRevpar - avgPrevRevpar;
    var occDelta = avgCurrOcc - avgPrevOcc;
    var pickup7 = rowsAgg.slice(-Math.min(3, rowsAgg.length)).reduce(function (acc, r) { return acc + (r.currRooms - r.prevRooms); }, 0);
    var needDates = rowsAgg.filter(function (r) { return (r.currOcc - r.prevOcc) < -1.2; });
    var opportunities = rowsAgg
      .map(function (r) { return { label: r.label, dRev: r.currRev - r.prevRev, dRooms: r.currRooms - r.prevRooms, dOcc: r.currOcc - r.prevOcc }; })
      .sort(function (a, b) { return b.dRev - a.dRev; })
      .slice(0, 3);
    var risks = rowsAgg
      .map(function (r) { return { label: r.label, dRev: r.currRev - r.prevRev, dRooms: r.currRooms - r.prevRooms, dOcc: r.currOcc - r.prevOcc }; })
      .sort(function (a, b) { return a.dRev - b.dRev; })
      .slice(0, 3);
    return {
      cmpLabel: cmpLabel,
      periodLabel: PERIOD_META[state.period].label,
      granularity: state.groupGranularity,
      currTotal: currTotal,
      prevTotal: prevTotal,
      paceRooms: paceRooms,
      paceRev: paceRev,
      pacePct: pacePct,
      revPct: revPct,
      adrDelta: adrDelta,
      revparDelta: revparDelta,
      occDelta: occDelta,
      pickup7: pickup7,
      needDates: needDates,
      opportunities: opportunities,
      risks: risks,
      insights: (lastPaceInsights || []).slice(),
      failingAssumptions: getFailingAssumptions(v).slice(0, 2)
    };
  }

  function buildOwnerPaceNarrative(v, ctx) {
    var analysis = [];
    var actions = [];
    var cmp = ctx.cmpLabel;
    var pr = ctx.paceRooms;
    var prv = ctx.paceRev;
    var signRooms = pr >= 0 ? 'ahead' : 'behind';
    var roomWord = Math.abs(pr) === 1 ? 'room night' : 'room nights';

    analysis.push(
      'Group on-the-books pace is <strong>' + signRooms + ' ' + cmp + ' by ' + fmtNum(Math.abs(pr)) + ' ' + roomWord + '</strong> (' +
      (ctx.pacePct >= 0 ? '+' : '') + ctx.pacePct.toFixed(1) + '%), with <strong>' + fmtNum(ctx.currTotal.t) + ' group rooms</strong> booked for ' + ctx.periodLabel + '.'
    );

    if (pr < 0 && prv > 0) {
      analysis.push(
        'Volume is soft but <strong>rate is carrying results</strong>: revenue pace is +' + fmtMoney(prv) + ' (' + (ctx.revPct >= 0 ? '+' : '') + ctx.revPct.toFixed(1) + '%) despite fewer rooms — ADR is ' +
        (ctx.adrDelta >= 0 ? '+' : '') + ctx.adrDelta.toFixed(0) + ' vs ' + cmp + '.'
      );
      actions.push('<strong>Protect ADR</strong> on compression dates; use targeted group conversion and negotiated corporate fills rather than broad discounting to close the room gap.');
    } else if (pr < 0 && prv <= 0) {
      analysis.push(
        'Both <strong>volume and revenue trail ' + cmp + '</strong> (revenue ' + fmtMoney(prv) + ', RevPAR Δ ' + (ctx.revparDelta >= 0 ? '+' : '') + ctx.revparDelta.toFixed(2) + ') — this is a dual pressure on the pace curve.'
      );
      actions.push('<strong>Run a joint revenue + sales war room</strong> on the weakest periods below; prioritize BAR/segment review and group tentative conversion before adding OTA depth.');
    } else if (pr >= 0 && prv >= 0) {
      analysis.push(
        'Pace is <strong>positive on rooms and revenue</strong> (+' + fmtMoney(prv) + ' revenue pace). RevPAR Δ ' + (ctx.revparDelta >= 0 ? '+' : '') + ctx.revparDelta.toFixed(2) + ' vs ' + cmp + '.'
      );
      actions.push('<strong>Hold rate discipline</strong> on strong windows; shift marketing weight to periods already pacing ahead to lock in share before competitors react.');
    } else {
      analysis.push(
        'Room pace is positive but <strong>revenue lags</strong> (+' + fmtNum(pr) + ' rooms vs ' + fmtMoney(prv) + ' revenue) — check group rate mix and wash on tentative blocks.'
      );
      actions.push('<strong>Audit group ADR and wash</strong> on in-house blocks; re-price or re-segment low-rate group before accepting additional volume.');
    }

    if (ctx.risks.length && ctx.risks[0].dRev < 0) {
      var weak = ctx.risks.filter(function (r) { return r.dRev < 0; }).slice(0, 2);
      var weakLabels = weak.map(function (r) { return r.label + ' (' + fmtMoney(r.dRev) + ')'; }).join(', ');
      analysis.push('Largest pace gaps concentrate in <strong>' + weakLabels + '</strong> — these are the primary drag on the period total.');
      actions.push('<strong>Assign owner for each weak window</strong> (' + weak.map(function (r) { return r.label; }).join(', ') + '): confirm group status, transient pickup plan, and whether BAR should flex within guardrails.');
    }

    if (ctx.opportunities.length && ctx.opportunities[0].dRev > 0) {
      var strong = ctx.opportunities[0];
      analysis.push(
        'Best relative strength is <strong>' + strong.label + '</strong> (+' + fmtMoney(strong.dRev) + ' revenue vs ' + cmp + ') — use as the benchmark for what is working in the current mix.'
      );
    }

    if (ctx.needDates.length > 1) {
      analysis.push(
        '<strong>' + ctx.needDates.length + ' periods</strong> sit more than 1.2pp below ' + cmp + ' on occupancy — flagged as need dates in the pace grid.'
      );
    }

    var highSignals = ctx.insights.filter(function (a) { return a.severity === 'High'; });
    if (highSignals.length) {
      highSignals.slice(0, 2).forEach(function (a) {
        analysis.push('Signal: <strong>' + a.label + '</strong> (' + a.value + ') — ' + a.detail + '.');
      });
    } else if (ctx.insights.length) {
      analysis.push('Pace signals: ' + ctx.insights.slice(0, 2).map(function (a) { return a.label + ' (' + a.value + ')'; }).join('; ') + '.');
    }

    if (ctx.pickup7 < 0) {
      analysis.push('Recent pickup is <strong>' + fmtNum(ctx.pickup7) + ' rooms</strong> over the last three pace periods vs ' + cmp + ' — short-term momentum is fading.');
      actions.push('<strong>Increase 7-day pickup cadence</strong> with sales and RMS: daily OTB vs forecast check, tentative follow-up, and transient campaigns on soft dates.');
    } else if (ctx.pickup7 > 0) {
      analysis.push('Recent pickup shows <strong>+' + fmtNum(ctx.pickup7) + ' rooms</strong> in the latest periods — near-term trend is improving.');
    }

    analysis.push(
      'P&amp;L context: RevPAR <strong>' + fmtEuro(v.revpar) + '</strong> (' + fmtPct(v.vsBudPct, true) + ' vs budget), occupancy <strong>' + v.occ.toFixed(1) + '%</strong> (' + fmtPp(v.occ - v.bud.occ) + ' vs budget), RGI <strong>' + v.rgi + '</strong>, direct <strong>' + v.direct.toFixed(0) + '%</strong> (target 42%).'
    );

    if (v.vsBudPct < -1) {
      actions.push('<strong>Close the budget gap</strong> (' + fmtPct(v.vsBudPct, true) + ' RevPAR vs budget) by prioritizing high-RevPAR segments on need dates rather than pure occupancy buys.');
    }
    if (v.direct < 42) {
      actions.push('<strong>Shift share to direct</strong> (currently ' + v.direct.toFixed(0) + '%) — parity review and loyalty offers on periods where pace is behind.');
    }
    if (v.rgi < 100) {
      actions.push('<strong>Regain comp-set share</strong> (RGI ' + v.rgi + ') — align BAR with market index on compression nights identified in the heatmap.');
    }
    if (ctx.failingAssumptions.length) {
      ctx.failingAssumptions.forEach(function (a) {
        actions.push('<strong>Model fix — ' + a.name + '</strong> (score ' + a.score + '): ' + a.detail + '.');
      });
    }
    if (v.pickup && v.pickup.otbVsFcst30 <= -3) {
      actions.push('<strong>Recalibrate 30-day forecast</strong> — OTB is ' + fmtPct(v.pickup.otbVsFcst30, true) + ' vs forecast; align owner expectations before month-end close.');
    }

    if (!actions.length) {
      actions.push('<strong>Maintain current strategy</strong> — pace, revenue, and P&amp;L indicators are aligned; monitor weekly and escalate if need dates increase.');
    }

    var headline;
    if (pr < 0 && prv > 0) {
      headline = 'Group rooms trail ' + cmp + ', but rate strength is offsetting volume — owners should focus on filling identified weak dates without eroding ADR.';
    } else if (pr < 0) {
      headline = 'Group booking pace is behind ' + cmp + ' on both rooms and revenue — immediate action is needed on the weak periods called out below.';
    } else if (pr >= 0 && v.vsBudPct >= 0) {
      headline = 'Group pace is ahead of ' + cmp + ' and RevPAR is tracking above budget — sustain rate discipline and convert strength in ' + (ctx.opportunities[0] ? ctx.opportunities[0].label : 'leading periods') + '.';
    } else {
      headline = 'Group room pace is ahead of ' + cmp + ', but watch P&amp;L vs budget — translate OTB strength into forecasted GOP delivery.';
    }

    var printBullets = [headline]
      .concat(analysis.slice(0, 4).map(function (s) { return s.replace(/<[^>]+>/g, ''); }))
      .concat(actions.slice(0, 3).map(function (s) { return s.replace(/<[^>]+>/g, ''); }));

    return { headline: headline, analysis: analysis, actions: actions, printBullets: printBullets };
  }

  function buildOwnerGeneralNarrative(v) {
    var analysis = (v.briefBullets || []).map(function (b) { return b; });
    var actions = [];
    if (v.vsBudPct < -1) {
      actions.push('<strong>Prioritize RevPAR recovery</strong> — currently ' + fmtPct(v.vsBudPct, true) + ' vs budget; review pace, rate, and channel mix on the Booking Pace workspace.');
    }
    if (v.direct < 42) {
      actions.push('<strong>Improve direct share</strong> (now ' + v.direct.toFixed(0) + '%) through parity, CRM, and member offers.');
    }
    if (!actions.length) {
      actions.push('<strong>Review Booking Pace tab</strong> for period-level OTB detail and export the weekly commercial pack for owner distribution.');
    }
    var headline = state.scope === 'portfolio'
      ? 'Portfolio RevPAR ' + fmtEuro(v.revpar) + ' (' + fmtPct(v.vsBudPct, true) + ' vs budget) across ' + PROPS.length + ' assets — open Booking Pace for property-level OTB analysis.'
      : v.name + ': RevPAR ' + fmtEuro(v.revpar) + ' (' + fmtPct(v.vsBudPct, true) + ' vs budget), occupancy ' + v.occ.toFixed(1) + '% — see Booking Pace for group OTB detail and recommended actions.';
    return {
      headline: headline,
      analysis: analysis,
      actions: actions,
      printBullets: [headline].concat(analysis).concat(actions.map(function (s) { return s.replace(/<[^>]+>/g, ''); }))
    };
  }

  function paceRevFromSummary(curr, prev) {
    return curr.rev - prev.rev;
  }

  function renderGroupPaceVisuals(v, rowsAgg, currTotal, prevTotal, paceRev) {
    var paceRooms = currTotal.t - prevTotal.t;
    var pacePct = prevTotal.t > 0 ? ((paceRooms / prevTotal.t) * 100) : 0;
    var revPct = prevTotal.rev > 0 ? ((paceRev / prevTotal.rev) * 100) : 0;
    var hero = document.getElementById('groupPaceHero');
    if (hero) {
      var up = pacePct >= 0;
      var cmpLabel = state.groupCompare === 'stly' ? 'STLY' : state.groupCompare === 'budget' ? 'Budget' : 'Forecast';
      var execTiles = getExecutiveKpiItems(v).map(renderGpHeroTile).join('');
      var paceForGrid = (lastPaceKpiItems || []).filter(function (it) {
        return it.k !== 'Rooms pace' && it.k !== 'Revenue pace';
      });
      var paceTiles = paceForGrid.length ? paceForGrid.map(renderGpHeroTile).join('') : '';
      hero.innerHTML =
        '<div class="gp-hero-top">' +
        '<div class="gp-scope"><span>Group booking pace</span><strong id="groupPaceHeroScope">' + (state.scope === 'portfolio' ? 'Portfolio' : v.short) + '</strong>' +
        '<div style="margin-top:6px;font-size:10px;opacity:0.8">' + PERIOD_META[state.period].label + ' · vs ' + cmpLabel + '</div></div>' +
        '<div class="gp-index"><div class="big">' + (up ? '+' : '') + pacePct.toFixed(1) + '%</div><div class="lbl">Rooms pace vs ' + cmpLabel + '</div>' +
        '<div class="gp-pill ' + (up ? 'up' : 'down') + '">' + (up ? '▲' : '▼') + ' ' + (up ? '+' : '') + fmtNum(paceRooms) + ' group rms</div></div>' +
        '<div class="gp-quick">' +
        '<span>Revenue pace<b>' + (paceRev >= 0 ? '+' : '') + fmtMoney(paceRev) + '</b></span>' +
        '<span>Rev %<b>' + (revPct >= 0 ? '+' : '') + revPct.toFixed(1) + '%</b></span>' +
        '<span>On books<b>' + fmtNum(currTotal.t) + '</b></span></div></div>' +
        '<div class="gp-hero-kpi-section">' +
        '<div class="gp-hero-kpi-label">Key metrics <span>· P&amp;L &amp; group pace</span></div>' +
        '<div class="gp-hero-kpi-grid gp-hero-kpi-grid--all">' + execTiles + paceTiles + '</div>' +
        '</div>';
      hero.classList.add('gp-hero--unified');
    }
    var callout = document.getElementById('groupPaceCallout');
    if (callout) callout.textContent = (revPct >= 0 ? '+' : '') + revPct.toFixed(1) + '% revenue pace';
    renderGroupPaceHeroChart(rowsAgg);
    renderGroupPaceGauge(currTotal, prevTotal);
    renderGroupPaceHeatmap(rowsAgg);
  }

  function renderGroupPaceHeroChart(rowsAgg) {
    var canvas = document.getElementById('groupPaceHeroChart');
    if (!canvas) return;
    var labels = rowsAgg.map(function (r) { return r.label; });
    var curr = rowsAgg.map(function (r) { return r.currRooms; });
    var prev = rowsAgg.map(function (r) { return r.prevRooms; });
    var pickup = rowsAgg.map(function (r) { return r.currRooms - r.prevRooms; });
    dc('ghero');
    charts.ghero = new Chart(canvas, {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [
          {
            type: 'bar', label: 'Current', data: curr, order: 2,
            backgroundColor: function (ctx) { return gpGradient(ctx.chart, 'rgba(8,145,178,0.9)', 'rgba(34,211,238,0.95)'); },
            borderRadius: 10, borderSkipped: false, barPercentage: 0.55, categoryPercentage: 0.7
          },
          {
            type: 'bar', label: 'Compare', data: prev, order: 3,
            backgroundColor: GP_COLORS.slateSoft, borderRadius: 10, borderSkipped: false,
            barPercentage: 0.55, categoryPercentage: 0.7
          },
          {
            type: 'line', label: 'Pickup delta', data: pickup, order: 1,
            borderColor: GP_COLORS.lime, backgroundColor: GP_COLORS.limeSoft,
            borderWidth: 3, tension: 0.45, fill: true, pointRadius: 4,
            pointBackgroundColor: GP_COLORS.lime, pointBorderColor: '#0a0f1a', pointBorderWidth: 2,
            yAxisID: 'y1'
          }
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { position: 'bottom', labels: gpDarkLegend().labels },
          tooltip: { backgroundColor: '#1e293b', titleColor: '#f8fafc', bodyColor: '#cbd5e1', padding: 10, cornerRadius: 10, borderColor: 'rgba(34,211,238,0.3)', borderWidth: 1 }
        },
        scales: gpDarkScales({
          x: { grid: { display: false }, ticks: { font: { size: 10 }, maxRotation: 0, color: GP_COLORS.text } },
          y1: {
            position: 'right', grid: { drawOnChartArea: false },
            ticks: { font: { size: 10 }, color: GP_COLORS.lime, callback: function (v) { return (v > 0 ? '+' : '') + v; } }
          }
        })
      }
    });
  }

  function renderGroupPaceGauge(currTotal, prevTotal) {
    var canvas = document.getElementById('groupPaceGauge');
    if (!canvas) return;
    var pct = prevTotal.t > 0 ? Math.min(120, Math.round((currTotal.t / prevTotal.t) * 100)) : 100;
    var label = document.getElementById('groupPaceGaugeLabel');
    if (label) label.innerHTML = pct + '%<span>of compare volume</span>';
    dc('ggauge');
    charts.ggauge = new Chart(canvas, {
      type: 'doughnut',
      data: {
        labels: ['On books', 'Gap'],
        datasets: [{
          data: [pct, Math.max(0, 100 - Math.min(100, pct))],
          backgroundColor: [GP_COLORS.cyan, GP_COLORS.track],
          borderWidth: 0, circumference: 270, rotation: 225
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, cutout: '72%',
        plugins: { legend: { display: false }, tooltip: { enabled: false } }
      }
    });
  }

  function renderGroupPaceHeatmap(rowsAgg) {
    var el = document.getElementById('groupPaceHeatmap');
    if (!el) return;
    el.innerHTML = rowsAgg.map(function (r, idx) {
      var dT = r.currRooms - r.prevRooms;
      var dOcc = r.currOcc - r.prevOcc;
      var cls = gpHeatClass(dT, dOcc);
      var anom = lastPaceAnomalyByRow[idx];
      var signalCls = anom ? ' has-signal ' + insightSignalClass(anom.severity) : '';
      var baseTitle = r.label + ': ' + (dT >= 0 ? '+' : '') + fmtNum(dT) + ' rooms vs compare';
      var tipAttr = anom
        ? ' data-insight="' + escAttr(insightTooltipText(anom)) + '" title="' + escAttr(baseTitle) + '"'
        : ' title="' + escAttr(baseTitle) + '"';
      return '<div class="cell ' + cls + signalCls.trim() + '"' + tipAttr + '>' +
        (anom ? '<span class="gp-signal-dot gp-signal-dot--cell" aria-hidden="true"></span>' : '') +
        '<span class="cell-date">' + r.label + '</span>' +
        '<span class="cell-delta">' + (dT >= 0 ? '+' : '') + fmtNum(dT) + '</span></div>';
    }).join('');
  }

  function renderGroupPaceDiagnostics(v, rowsAgg) {
    renderForecastAccuracy(v);
    renderAnomalies(v, rowsAgg);
  }

  function renderGroupPaceSummary(currTotal, prevTotal, avgCurrOcc, avgPrevOcc, avgCurrAdr, avgPrevAdr, avgCurrRevpar, avgPrevRevpar, rowsAgg) {
    lastPaceKpiItems = buildPaceKpiItems(currTotal, prevTotal, avgCurrAdr, avgPrevAdr, avgCurrRevpar, avgPrevRevpar, rowsAgg);
    renderGroupMiniCharts(rowsAgg);
    renderGroupActions(rowsAgg);
  }

  function renderGroupMiniCharts(rowsAgg) {
    var labels = rowsAgg.map(function (r) { return r.label; });
    var currRooms = rowsAgg.map(function (r) { return r.currRooms; });
    var prevRooms = rowsAgg.map(function (r) { return r.prevRooms; });
    var currCum = [];
    var prevCum = [];
    currRooms.reduce(function (a, b, i) { currCum[i] = a + b; return currCum[i]; }, 0);
    prevRooms.reduce(function (a, b, i) { prevCum[i] = a + b; return prevCum[i]; }, 0);

    dc('gtrend');
    charts.gtrend = new Chart(document.getElementById('groupPaceMiniTrend'), {
      type: 'line',
      data: {
        labels: labels,
        datasets: [
          {
            label: 'Current', data: currRooms, borderColor: GP_COLORS.cyan, tension: 0.45, fill: true, pointRadius: 0, borderWidth: 2.5,
            backgroundColor: function (ctx) { return gpGradient(ctx.chart, 'rgba(34,211,238,0.35)', 'rgba(34,211,238,0)'); }
          },
          { label: 'Compare', data: prevRooms, borderColor: GP_COLORS.slate, borderDash: [5, 5], tension: 0.45, pointRadius: 0, borderWidth: 2 }
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom', labels: gpDarkLegend().labels } },
        scales: { x: { display: false }, y: { grid: { color: GP_COLORS.grid }, ticks: { font: { size: 9 }, color: GP_COLORS.text } } }
      }
    });

    dc('gcume');
    charts.gcume = new Chart(document.getElementById('groupPaceMiniCume'), {
      type: 'line',
      data: {
        labels: labels,
        datasets: [
          {
            label: 'Current cumulative', data: currCum, borderColor: GP_COLORS.teal, tension: 0.45, pointRadius: 0, fill: true, borderWidth: 2.5,
            backgroundColor: function (ctx) { return gpGradient(ctx.chart, 'rgba(45,212,191,0.3)', 'rgba(45,212,191,0)'); }
          },
          { label: 'Compare cumulative', data: prevCum, borderColor: '#fbbf24', tension: 0.45, borderDash: [5, 5], pointRadius: 0, borderWidth: 2 }
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom', labels: gpDarkLegend().labels } },
        scales: { x: { display: false }, y: { grid: { color: GP_COLORS.grid }, ticks: { font: { size: 9 }, color: GP_COLORS.text } } }
      }
    });
  }

  function renderGroupActions(rowsAgg) {
    var opp = rowsAgg
      .map(function (r) { return { label: r.label, dRev: r.currRev - r.prevRev, dOcc: r.currOcc - r.prevOcc }; })
      .sort(function (a, b) { return b.dRev - a.dRev; })
      .slice(0, 3);
    var risk = rowsAgg
      .map(function (r) { return { label: r.label, dRev: r.currRev - r.prevRev, dOcc: r.currOcc - r.prevOcc }; })
      .sort(function (a, b) { return a.dRev - b.dRev; })
      .slice(0, 3);
    document.getElementById('groupOpportunities').innerHTML = opp.map(function (o) {
      return '<li><span class="badge done" style="margin-right:4px">▲</span><strong>' + o.label + '</strong><br><span style="color:var(--muted);font-size:10px">Rev +' + fmtMoney(o.dRev) + ' · Occ ' + fmtPp(o.dOcc) + '</span></li>';
    }).join('');
    document.getElementById('groupRisks').innerHTML = risk.map(function (r) {
      return '<li><span class="badge open" style="margin-right:4px">▼</span><strong>' + r.label + '</strong><br><span style="color:var(--muted);font-size:10px">Rev ' + fmtMoney(r.dRev) + ' · Occ ' + fmtPp(r.dOcc) + '</span></li>';
    }).join('');
  }

  function renderScorecard() {
    var period = state.period;
    document.getElementById('scorecardBody').innerHTML = PROPS.map(function (p) {
      var m = getMetrics(p.id, period);
      var st = statusFromVsBud(m.vsBudPct);
      var rowCls = state.scope === p.id ? ' class="row-selected"' : '';
      return '<tr data-prop-id="' + p.id + '"' + rowCls + '>' +
        '<td><span class="status-dot ' + st + '"></span></td>' +
        '<td><strong>' + p.short + '</strong></td>' +
        '<td class="num">' + m.pillars.sales + '</td><td class="num">' + m.pillars.dist + '</td>' +
        '<td class="num">' + m.pillars.yield + '</td><td class="num">' + m.pillars.digital + '</td>' +
        '<td class="num">' + fmtEuro(m.revpar) + '</td>' +
        '<td class="num" style="color:var(--' + (m.vsBudPct >= 0 ? 'positive' : 'negative') + ')">' + fmtPct(m.vsBudPct, true) + '</td>' +
        '<td class="num">' + m.rgi + '</td>' +
        '<td><a class="link-ext" href="#" data-system="PMS" data-prop="' + p.id + '">Open</a></td></tr>';
    }).join('');
  }

  function renderOwnerNarrative(v) {
    var narrative = (state.dashboard === 'pace' && lastPaceContext)
      ? buildOwnerPaceNarrative(v, lastPaceContext)
      : buildOwnerGeneralNarrative(v);

    var headlineEl = document.getElementById('gpOwnerHeadline');
    var analysisEl = document.getElementById('gpOwnerAnalysisList');
    var actionsEl = document.getElementById('gpOwnerActionsList');
    if (headlineEl) headlineEl.textContent = narrative.headline;
    if (analysisEl) {
      analysisEl.innerHTML = narrative.analysis.map(function (line) { return '<li>' + line + '</li>'; }).join('');
    }
    if (actionsEl) {
      actionsEl.innerHTML = narrative.actions.map(function (line) { return '<li>' + line + '</li>'; }).join('');
    }

    var briefName = document.getElementById('briefPropertyName');
    var briefPeriod = document.getElementById('briefPeriod');
    var briefKpis = document.getElementById('briefKpis');
    var briefSummary = document.getElementById('briefSummary');
    if (!briefName) return;
    briefName.textContent = state.scope === 'portfolio' ? 'Portfolio summary' : v.name;
    briefPeriod.textContent = PERIOD_META[state.period].label + ' · Week of 2 June 2026';
    var vsBud = fmtPct(v.vsBudPct, true);
    var occVar = fmtPp(v.occ - v.bud.occ);
    var paceKpiThird = (state.dashboard === 'pace' && lastPaceContext)
      ? '<div><div class="val">' + (lastPaceContext.pacePct >= 0 ? '+' : '') + lastPaceContext.pacePct.toFixed(1) + '%</div><div style="font-size:12px;color:var(--muted)">Rooms pace</div><div style="font-size:11px;color:var(--' + pctClass(lastPaceContext.paceRooms) + ')">vs ' + lastPaceContext.cmpLabel + '</div></div>'
      : '<div><div class="val">' + v.direct.toFixed(0) + '%</div><div style="font-size:12px;color:var(--muted)">Direct</div><div style="font-size:11px">Target 42%</div></div>';
    briefKpis.innerHTML =
      '<div><div class="val">' + fmtEuro(v.revpar) + '</div><div style="font-size:12px;color:var(--muted)">RevPAR</div><div style="font-size:11px;color:var(--' + pctClass(v.vsBudPct) + ')">' + vsBud + ' vs budget</div></div>' +
      '<div><div class="val">' + v.occ.toFixed(0) + '%</div><div style="font-size:12px;color:var(--muted)">Occupancy</div><div style="font-size:11px;color:var(--' + pctClass(v.occ - v.bud.occ) + ')">' + occVar + ' vs budget</div></div>' +
      paceKpiThird;
    briefSummary.innerHTML = narrative.printBullets.map(function (b) { return '<li>' + b + '</li>'; }).join('');
  }

  function printOwnerBrief() {
    renderOwnerNarrative(getView());
    document.body.classList.add('print-brief');
    requestAnimationFrame(function () {
      window.print();
      setTimeout(function () { document.body.classList.remove('print-brief'); }, 500);
    });
  }

  function printPaceExec() {
    setDashboard('pace');
    document.body.classList.add('print-pace');
    requestAnimationFrame(function () {
      ['ghero', 'ggauge', 'gtrend', 'gcume'].forEach(function (k) {
        if (charts[k]) charts[k].resize();
      });
      setTimeout(function () {
        window.print();
        setTimeout(function () { document.body.classList.remove('print-pace'); }, 500);
      }, 120);
    });
  }

  function renderCommentary(v) {
    var label = state.scope === 'portfolio' ? 'Portfolio narrative' : v.short + ' narrative';
    document.getElementById('commentLabel').textContent = label;
    var ta = document.getElementById('commentPortfolio');
    if (document.activeElement !== ta) ta.value = v.commentary;
  }

  function renderHeader(v) {
    var pm = PERIOD_META[state.period];
    var titleEl = document.getElementById('pageTitle');
    var ctxEl = document.getElementById('pageContext');
    var navList = (typeof LonrpViews !== 'undefined' && LonrpViews.NAV) ? LonrpViews.NAV : [];
    var navItem = navList.find(function (n) { return n.id === state.dashboard; });
    if (navItem && (state.dashboard === 'pace' || state.dashboard === 'accounts')) {
      var dashMeta = (typeof LonrpViews !== 'undefined' && LonrpViews.META) ? LonrpViews.META[state.dashboard] : {};
      if (titleEl) titleEl.textContent = navItem.label;
      if (ctxEl) ctxEl.textContent = dashMeta.subtitle || pm.label;
      return;
    }
    if (state.dashboard === 'info' || state.dashboard === 'intro' || state.dashboard === 'executive') {
      if (titleEl) titleEl.textContent = 'Northgate Riverside — Commercial performance';
      if (ctxEl) {
        if (state.dashboard === 'info') {
          ctxEl.textContent = 'INFO PAGE · Version 1.8, Report Guideline and Useful Information';
        } else if (state.dashboard === 'executive') {
          var p = state.lonrpPeriod === 'mtd' ? 'MTD' : state.lonrpPeriod === 'fy' ? 'FULL YEAR' : 'YTD';
          ctxEl.textContent = 'EXECUTIVE SUMMARY | ' + p;
        } else {
          ctxEl.textContent = 'Property dashboard introduction';
        }
      }
      return;
    }
    if (titleEl) titleEl.textContent =
      (state.scope === 'portfolio' ? 'Portfolio' : v.name) + ' — ' + pm.periodTag;
    if (ctxEl) ctxEl.textContent =
      state.scope === 'portfolio'
        ? '4 assets · ' + pm.label + ' · Rooms €' + Math.round(v.roomsRevK).toLocaleString('en') + 'K (sum) · RevPAR weighted by keys'
        : v.keys + ' keys · ' + pm.label + ' · EUR';
    document.getElementById('plSnapshotTitle').textContent = pm.snapshot;
    document.getElementById('plSnapshotLine').innerHTML =
      '<strong>Rooms ' + fmtEuroK(v.roomsRevK) + '</strong> · GOP <strong>' + fmtEuroK(v.gopK) + (state.period === 'fcst' ? ' (fcst)' : ' (est.)') + '</strong> ' + v.gopMargin.toFixed(1) + '%';
    document.getElementById('marketCard').classList.toggle('hidden', state.scope === 'portfolio');
    if (state.scope !== 'portfolio') {
      document.getElementById('marketContent').innerHTML = v.market + '<div class="sources-row"><a href="#" class="link-ext" data-system="STR" data-prop="' + state.scope + '">STR market report →</a></div>';
    } else {
      document.getElementById('marketCard').classList.remove('hidden');
      document.getElementById('marketContent').innerHTML = v.market;
    }
  }

  function dc(key) {
    if (charts[key]) { charts[key].destroy(); delete charts[key]; }
  }

  function updateCharts(v) {
    var wf = v.wf;
    dc('wf');
    charts.wf = new Chart(document.getElementById('chartWaterfall'), {
      type: 'bar',
      data: {
        labels: ['Rooms', 'F&B', 'Other', 'Total', 'Opex', 'GOP'],
        datasets: [{
          data: [wf.rooms, wf.fb, wf.other, wf.rooms + wf.fb + wf.other, -wf.opex, wf.gop],
          backgroundColor: function (ctx) {
            var i = ctx.dataIndex;
            if (i === 5) return '#059669';
            if (i === 4) return '#fca5a5';
            return '#6366f1';
          },
          borderRadius: 4
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } },
        scales: { y: { ticks: { callback: function (val) { return '€' + val + 'K'; } } } }
      }
    });

    dc('occ');
    charts.occ = new Chart(document.getElementById('chartOcc'), {
      type: 'doughnut',
      data: { datasets: [{ data: [v.occ, 100 - v.occ], backgroundColor: ['#6366f1', '#e2e8f0'], borderWidth: 0 }] },
      options: { responsive: true, maintainAspectRatio: false, cutout: '70%', plugins: { legend: { display: false } } }
    });
    document.getElementById('occCaption').innerHTML = '<strong>' + v.occ.toFixed(1) + '%</strong> · ' + fmtPp(v.occ - v.bud.occ) + ' vs budget';

    dc('adr');
    charts.adr = new Chart(document.getElementById('chartAdrRevpar'), {
      type: 'bar',
      data: {
        labels: ['ADR', 'RevPAR'],
        datasets: [
          { label: 'STLY', data: [v.stly.revpar / (v.occ / 100) * 0.92, v.stly.revpar], backgroundColor: '#94a3b8', barThickness: 12 },
          { label: 'Budget', data: [v.bud.adr, v.bud.revpar], backgroundColor: '#c7d2fe', barThickness: 12 },
          { label: 'Actual', data: [v.adr, v.revpar], backgroundColor: '#6366f1', barThickness: 12 }
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom' } },
        scales: { y: { ticks: { callback: function (val) { return '€' + val; } } } }
      }
    });

    var weeks = ['W1', 'W2', 'W3', 'W4', 'W1', 'W2', 'W3', 'W4', 'W1', 'W2', 'W3', 'W4'];
    dc('pace');
    charts.pace = new Chart(document.getElementById('chartPace'), {
      type: 'line',
      data: {
        labels: weeks,
        datasets: [
          { label: 'OTB occ %', data: v.pace.otb, borderColor: '#6366f1', tension: 0.25, pointRadius: 2 },
          { label: 'STLY occ %', data: v.pace.stly, borderColor: '#94a3b8', borderDash: [4, 4], tension: 0.25, pointRadius: 0 },
          { label: 'Forecast occ %', data: v.pace.fcst, borderColor: '#059669', tension: 0.25, pointRadius: 0 }
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom' } },
        scales: { y: { min: 50, max: 90, title: { display: true, text: 'Occupancy %' } } }
      }
    });

    var fwd = v.forward;
    dc('fwd');
    charts.fwd = new Chart(document.getElementById('chartForward'), {
      type: 'line',
      data: {
        labels: fwd.labels,
        datasets: [
          { label: 'OTB occ %', data: fwd.occ, borderColor: '#6366f1', yAxisID: 'y' },
          { label: 'Fcst RevPAR €', data: fwd.revpar, borderColor: '#059669', borderDash: [4, 4], yAxisID: 'y1' }
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        scales: {
          y: { position: 'left', title: { display: true, text: 'Occ %' } },
          y1: { position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'RevPAR €' } }
        }
      }
    });

    var chLabels, chData;
    if (state.scope === 'portfolio') {
      chLabels = PROPS.map(function (p) { return p.short.split(' ')[0]; });
      chData = PROPS.map(function (p) {
        var m = getMetrics(p.id, state.period);
        return { direct: m.direct, ota: m.ota, other: m.other };
      });
    } else {
      chLabels = [v.short];
      chData = [{ direct: v.direct, ota: v.ota, other: v.other }];
    }
    dc('ch');
    charts.ch = new Chart(document.getElementById('chartChannel'), {
      type: 'bar',
      data: {
        labels: chLabels,
        datasets: [
          { label: 'Direct', data: chData.map(function (d) { return d.direct; }), backgroundColor: '#6366f1', stack: 's' },
          { label: 'OTA', data: chData.map(function (d) { return d.ota; }), backgroundColor: '#a5b4fc', stack: 's' },
          { label: 'Other', data: chData.map(function (d) { return d.other; }), backgroundColor: '#e2e8f0', stack: 's' }
        ]
      },
      options: {
        indexAxis: state.scope === 'portfolio' ? 'y' : 'x',
        responsive: true, maintainAspectRatio: false,
        scales: {
          x: { stacked: true, max: state.scope === 'portfolio' ? 100 : undefined, ticks: { callback: function (val) { return val + '%'; } } },
          y: { stacked: true }
        }
      }
    });
  }

  function scopeSeed(str) {
    var h = 0;
    for (var i = 0; i < str.length; i++) h = ((h << 5) - h) + str.charCodeAt(i);
    return Math.abs(h);
  }

  function getForecastAccuracy(v) {
    var seed = scopeSeed(v.id + state.period);
    var horizons = [7, 14, 30, 60];
    return horizons.map(function (days, i) {
      var err = 2.1 + ((seed + i * 17) % 120) / 10;
      if (v.id === 'barcelona' && days >= 30) err += 2.8;
      if (v.id === 'porto' && days <= 14) err -= 1.2;
      var actualRevpar = v.revpar;
      var fcstRevpar = actualRevpar * (1 + err / 100);
      return { days: days, errorPct: err, actual: actualRevpar, forecast: fcstRevpar };
    });
  }

  function getFailingAssumptions(v) {
    var base = [
      { name: 'Group wash rate', score: 72, detail: 'Tentative conversion 8pp below model' },
      { name: 'Transient pickup curve', score: 58, detail: '7-day pace vs 14-day forecast diverging' },
      { name: 'Corporate BAR elasticity', score: 41, detail: 'Within tolerance' },
      { name: 'OTA share drift', score: v.id === 'barcelona' ? 81 : 35, detail: v.id === 'barcelona' ? '+6pp vs 4-wk baseline' : 'Stable vs model' },
      { name: 'Event compression uplift', score: 48, detail: 'Jun 10–12 uplift under-applied in 30d horizon' }
    ];
    return base.filter(function (a) { return a.score >= 55; }).sort(function (a, b) { return b.score - a.score; });
  }

  function computeScenario(v) {
    var sc = state.scenario;
    var baseRevpar = v.revpar;
    var baseGopK = v.gopK;
    var adrFactor = 1 + sc.adrPct / 100;
    var washPenalty = 1 - (sc.washPct / 100) * 0.35;
    var convBoost = 1 + ((sc.conversionPct - 70) / 100) * 0.12;
    var revpar = baseRevpar * adrFactor * washPenalty * convBoost;
    var revparDelta = revpar - baseRevpar;
    var gopK = baseGopK * (1 + (revparDelta / baseRevpar) * 1.35);
    var risk = Math.min(99, Math.max(8,
      22 + Math.abs(sc.adrPct) * 1.2 + sc.washPct * 1.8 + (sc.conversionPct < 65 ? 18 : 0) + (v.vsBudPct < -2 ? 12 : 0)
    ));
    return { revpar: revpar, gopK: gopK, risk: risk, revparDelta: revparDelta, gopDeltaK: gopK - baseGopK };
  }

  function escAttr(s) {
    return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
  }

  function insightSignalClass(severity) {
    return severity === 'High' ? 'signal-high' : 'signal-med';
  }

  function insightTooltipText(a) {
    return a.label + ' · ' + a.value + ' — ' + a.detail;
  }

  function dayIndexToRowIndex(dayNum, granularity, rowCount) {
    var rawIndex = dayNum - 1;
    if (granularity === 'daily') return Math.min(rowCount - 1, rawIndex);
    if (granularity === 'weekly') return Math.min(rowCount - 1, Math.floor(rawIndex / 2));
    return Math.min(rowCount - 1, Math.floor(rawIndex / 4));
  }

  function enrichAnomaliesWithRows(v, rowsAgg) {
    return detectAnomalies(v).map(function (a) {
      var m = a.label.match(/^Day (\d+)/);
      if (m) {
        var rowIndex = dayIndexToRowIndex(parseInt(m[1], 10), state.groupGranularity, rowsAgg.length);
        a.rowIndex = rowIndex;
        a.rowLabel = rowsAgg[rowIndex] ? rowsAgg[rowIndex].label : null;
      }
      return a;
    });
  }

  function buildAnomalyByRowIndex(insights) {
    var map = {};
    insights.forEach(function (a) {
      if (a.rowIndex == null) return;
      var prev = map[a.rowIndex];
      if (!prev || (a.severity === 'High' && prev.severity !== 'High')) map[a.rowIndex] = a;
    });
    return map;
  }

  function detectAnomalies(v) {
    var pace = v.pace;
    var anomalies = [];
    var n = pace.otb.length;
    for (var i = Math.max(0, n - 5); i < n; i++) {
      var otb = pace.otb[i];
      var fcst = pace.fcst[i];
      var stly = pace.stly[i];
      var diffFcst = otb - fcst;
      var diffStly = otb - stly;
      if (Math.abs(diffFcst) >= 3.5) {
        anomalies.push({
          label: 'Day ' + (i + 1) + ' OTB vs forecast',
          value: fmtPp(diffFcst),
          severity: Math.abs(diffFcst) >= 5 ? 'High' : 'Med',
          detail: 'OTB ' + otb.toFixed(0) + '% vs fcst ' + fcst.toFixed(0) + '%'
        });
      }
      if (Math.abs(diffStly) >= 4 && anomalies.length < 6) {
        anomalies.push({
          label: 'Day ' + (i + 1) + ' OTB vs STLY',
          value: fmtPp(diffStly),
          severity: 'Med',
          detail: 'Unusual vs same time last year'
        });
      }
    }
    var pu = v.pickup;
    if (pu.otbVsFcst30 <= -3) {
      anomalies.push({
        label: '30-day OTB vs forecast',
        value: fmtPct(pu.otbVsFcst30, true),
        severity: 'High',
        detail: 'Sustained pace shortfall — not single-day noise'
      });
    }
    if (pu.rms7 > 0 && pu.rms14 > 0 && pu.rms7 / (pu.rms14 / 2) > 1.35) {
      anomalies.push({
        label: 'Pickup acceleration',
        value: '+' + Math.round((pu.rms7 / (pu.rms14 / 2) - 1) * 100) + '%',
        severity: 'Med',
        detail: '7d pickup rate vs 14d baseline'
      });
    }
    return anomalies.slice(0, 5);
  }

  function renderForecastAccuracy(v) {
    var el = document.getElementById('forecastAccuracyPanel');
    if (!el) return;
    var rows = getForecastAccuracy(v);
    var html = rows.map(function (r) {
      var cls = r.errorPct <= 3 ? 'good' : r.errorPct <= 6 ? 'warn' : 'bad';
      var width = Math.min(100, r.errorPct * 8);
      return '<div class="forecast-bar"><div class="row">' +
        '<span class="label">' + r.days + 'd</span>' +
        '<div class="track"><div class="fill ' + cls + '" style="width:' + width + '%"></div></div>' +
        '<span style="width:32px;text-align:right;font-weight:600">' + r.errorPct.toFixed(1) + '%</span>' +
        '</div></div>';
    }).join('');
    var fails = getFailingAssumptions(v).slice(0, 2);
    if (fails.length) {
      html += '<div class="forecast-fails"><div class="fail-lbl">Failing assumptions</div>';
      fails.forEach(function (a) {
        html += '<div class="fail-row"><span title="' + a.detail + '">' + a.name + '</span>' +
          '<span class="badge-pill" style="font-size:8px;padding:1px 5px;background:' + (a.score >= 70 ? '#fee2e2' : '#fef3c7') + ';color:' + (a.score >= 70 ? '#b91c1c' : '#b45309') + '">' + a.score + '</span></div>';
      });
      html += '</div>';
    }
    el.innerHTML = html;
  }

  function renderDisplacement(v) {
    var reqs = GROUP_REQUESTS[v.id] || GROUP_REQUESTS.portfolio;
    var html = reqs.map(function (g) {
      return '<div class="disp-card">' +
        '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px">' +
        '<strong style="font-size:12px">' + g.name + '</strong>' +
        '<span class="decision ' + g.decision + '">' + g.decision.toUpperCase() + '</span></div>' +
        '<div style="font-size:11px;color:var(--muted);margin-bottom:6px">' + g.dates + ' · ' + g.rms + ' rms @ €' + g.rate + ' · Est. displacement €' + fmtMoney(g.transientDisp) + '</div>' +
        (g.band !== '—' ? '<div style="font-size:11px;margin-bottom:4px"><strong>Counter band:</strong> ' + g.band + '</div>' : '') +
        '<p style="font-size:11px;margin:0;line-height:1.45">' + g.rationale + '</p></div>';
    }).join('');
    document.getElementById('displacementPanel').innerHTML = html || '<p style="font-size:12px;color:var(--muted)">No open group requests for this scope.</p>';
  }

  function renderScenarioSliders(v) {
    var sc = state.scenario;
    var out = computeScenario(v);
    document.getElementById('scenarioSliders').innerHTML =
      '<label>ADR adjustment <span>' + (sc.adrPct > 0 ? '+' : '') + sc.adrPct + '%</span></label>' +
      '<input type="range" id="scAdr" min="-8" max="15" step="1" value="' + sc.adrPct + '">' +
      '<label>Group wash % <span>' + sc.washPct + '%</span></label>' +
      '<input type="range" id="scWash" min="0" max="25" step="1" value="' + sc.washPct + '">' +
      '<label>Tentative → definite conversion <span>' + sc.conversionPct + '%</span></label>' +
      '<input type="range" id="scConv" min="50" max="95" step="1" value="' + sc.conversionPct + '">';
    document.getElementById('scenarioOutputs').innerHTML =
      '<div class="out-card"><div class="val ' + pctClass(out.revparDelta) + '">€' + Math.round(out.revpar) + '</div><div class="lbl">RevPAR (' + (out.revparDelta >= 0 ? '+' : '') + '€' + Math.round(out.revparDelta) + ')</div></div>' +
      '<div class="out-card"><div class="val ' + pctClass(out.gopDeltaK) + '">' + fmtEuroK(out.gopK) + '</div><div class="lbl">GOP (' + (out.gopDeltaK >= 0 ? '+' : '') + fmtEuroK(Math.abs(out.gopDeltaK)) + ')</div></div>' +
      '<div class="out-card"><div class="val" style="color:' + (out.risk > 55 ? 'var(--negative)' : out.risk > 35 ? 'var(--warn)' : 'var(--positive)') + '">' + Math.round(out.risk) + '</div><div class="lbl">Risk exposure index</div></div>';
  }

  function renderInitiativeAttribution(v) {
    var rows = INITIATIVES_BY_SCOPE[v.id] || INITIATIVES_BY_SCOPE.portfolio;
    var html = '<table class="init-table"><thead><tr><th>Initiative</th><th>Since</th><th>Metric</th><th>Attributed Δ</th><th>RevPAR (€K)</th><th>Conf.</th></tr></thead><tbody>';
    rows.forEach(function (r) {
      var impactCls = r.revparK >= 15 ? 'impact-pos' : r.revparK >= 8 ? '' : 'impact-neg';
      html += '<tr><td>' + r.action + '</td><td>' + r.start + '</td><td>' + r.metric + '</td><td class="' + impactCls + '">' + r.delta + '</td><td class="' + impactCls + '">+' + r.revparK + 'K</td><td>' + r.confidence + '</td></tr>';
    });
    html += '</tbody></table>';
    html += '<p style="font-size:10px;color:var(--muted);margin-top:8px">Attribution uses holdout windows vs control dates — consulting impact is measurable in weekly pack.</p>';
    document.getElementById('initiativePanel').innerHTML = html;
  }

  function renderAnomalies(v, rowsAgg) {
    var list = lastPaceInsights.length ? lastPaceInsights : enrichAnomaliesWithRows(v, rowsAgg || []);
    var el = document.getElementById('anomalyList');
    if (!el) return;
    if (!list.length) {
      el.innerHTML = '<li class="anomaly-empty">No statistically unusual signals — noise suppressed.</li>';
      return;
    }
    el.innerHTML =
      '<li class="anomaly-legend"><span class="gp-legend-dot signal-med"></span> Med · <span class="gp-legend-dot signal-high"></span> High — hover <strong>orange rings</strong> on heatmap or grid rows.</li>' +
      list.map(function (a) {
        var dotCls = insightSignalClass(a.severity);
        return '<li class="anomaly-compact ' + dotCls + '"><span class="gp-legend-dot ' + dotCls + '"></span>' +
          '<span><strong>' + a.label + '</strong> <span class="anomaly-val">' + a.value + '</span>' +
          '<span class="anomaly-detail">' + a.detail + '</span></span></li>';
      }).join('');
  }

  function renderAdvanced(v) {
    var asOf = document.getElementById('advancedAsOf');
    if (asOf) asOf.textContent = v.short + ' · ' + PERIOD_META[state.period].label;
    renderDisplacement(v);
    renderScenarioSliders(v);
    renderInitiativeAttribution(v);
    var bindOnce = renderAdvanced._bound;
    if (!bindOnce) {
      renderAdvanced._bound = true;
      document.getElementById('scenarioSliders').addEventListener('input', function (e) {
        var t = e.target;
        if (t.id === 'scAdr') state.scenario.adrPct = Number(t.value);
        if (t.id === 'scWash') state.scenario.washPct = Number(t.value);
        if (t.id === 'scConv') state.scenario.conversionPct = Number(t.value);
        renderScenarioSliders(getView());
      });
    }
  }

  function renderAll() {
    syncLonrpPeriod();
    syncPeriodSegButtons();
    var v = getView();
    if (typeof LonrpViews !== 'undefined') {
      LonrpViews.renderNav(state);
      LonrpViews.renderDashHeader(state, v, PERIOD_META);
      LonrpViews.showDashboardSection(state);
      var mount = document.getElementById('lonrpGenericMount');
      var builtIn = ['pace', 'accounts'];
      if (mount && builtIn.indexOf(state.dashboard) < 0) {
        mount.classList.remove('hidden');
        LonrpViews.renderBuiltInDashboard(state.dashboard, mount, {
          v: v, state: state, fmtEuro: fmtEuro, fmtPct: fmtPct, pctClass: pctClass
        });
        LonrpViews.renderSimpleCharts(state.dashboard, { v: v, state: state });
      } else if (mount) {
        if (typeof LonrpExecutiveView !== 'undefined') LonrpExecutiveView.destroyCharts();
        mount.classList.add('hidden');
        mount.innerHTML = '';
      }
    }
    renderHeader(v);
    if (state.dashboard === 'pace') {
      renderGroupPace(v);
    }
    if (state.dashboard === 'accounts') {
      renderAdvanced(v);
    }
    renderKpiStrip(v);
    renderOwnerNarrative(v);
    var kpiStrip = document.getElementById('kpiStrip');
    if (kpiStrip) kpiStrip.classList.add('hidden');
    var dashHeader = document.getElementById('lonrpDashHeader');
    document.getElementById('commandSection').classList.add('hidden');
    document.getElementById('groupPaceSection').classList.toggle('hidden', state.dashboard !== 'pace');
    document.body.classList.add('gp-theme-active');
    document.getElementById('decisionsSection').classList.toggle('hidden', state.dashboard !== 'accounts');
    var propSel = document.getElementById('lonrpPropertySelect');
    if (propSel && propSel.value !== state.scope) propSel.value = state.scope;
    document.querySelectorAll('#groupGranularitySeg button').forEach(function (b) {
      b.classList.toggle('active', b.getAttribute('data-granularity') === state.groupGranularity);
    });
    var cmp = document.getElementById('groupCompareSelect');
    if (cmp) cmp.value = state.groupCompare;
    if (state.dashboard === 'pace') {
      requestAnimationFrame(function () {
        ['ghero', 'ggauge', 'gtrend', 'gcume'].forEach(function (k) {
          if (charts[k]) charts[k].resize();
        });
      });
    }
    var tableauShell = ['intro', 'info', 'executive', 'finance', 'segmentation', 'revpar-index', 'premium',
      'amped', 'distribution', 'bonvoy', 'src', 'gross', 'scorecard', 'losbw', 'spe', 'promos', 'accuracy', 'enrolments'];
    document.body.classList.toggle('lonrp-info-mode', state.dashboard === 'info' || state.dashboard === 'intro');
    document.body.classList.toggle('lonrp-tableau-mode', tableauShell.indexOf(state.dashboard) >= 0);
    var topbar = document.querySelector('body.lonrp-app .topbar');
    if (topbar) topbar.classList.toggle('hidden', tableauShell.indexOf(state.dashboard) >= 0);
    var unifiedControls = document.getElementById('lonrpUnifiedControls');
    if (unifiedControls) {
      unifiedControls.classList.toggle('hidden', tableauShell.indexOf(state.dashboard) >= 0);
    }
    document.querySelectorAll('.lonrp-uc-pace-only').forEach(function (el) {
      el.classList.toggle('hidden', state.dashboard !== 'pace');
    });
    document.querySelectorAll('.lonrp-uc-global-only').forEach(function (el) {
      el.classList.toggle('hidden', state.dashboard === 'pace');
    });
  }

  function setScope(scope) {
    state.scope = scope;
    renderAll();
  }

  function setPeriod(period) {
    state.period = period;
    state.lonrpPeriod = period === 'mtd' ? 'mtd' : period === 'fcst' ? 'fy' : 'ytd';
    syncPeriodSegButtons();
    var sidePeriod = document.getElementById('lonrpSidebarPeriod');
    if (sidePeriod) sidePeriod.value = state.lonrpPeriod;
    renderAll();
  }

  function openPillarModal(key) {
    var pl = PILLAR_LINKS[key];
    var v = getView();
    document.getElementById('pillarModalTitle').textContent = pl.title;
    document.getElementById('pillarModalBody').innerHTML =
      '<p><strong>' + v.name + '</strong> · ' + PERIOD_META[state.period].label + '</p>' +
      '<p style="margin:12px 0">Score: <strong>' + v.pillars[key] + '</strong> — ' + v.drivers[key] + '</p>' +
      '<p style="color:var(--muted)">Opens <strong>' + pl.system + '</strong> for this property in production.</p>' +
      '<button type="button" class="btn btn-primary" id="pillarOpenLink" style="margin-top:12px">Open ' + pl.system + '</button>';
    document.getElementById('modalPillar').classList.add('open');
    document.getElementById('pillarOpenLink').onclick = function () {
      showToast('Opening ' + pl.system + ' for ' + v.short + '…');
      document.getElementById('modalPillar').classList.remove('open');
    };
  }

  function bindUi() {
    var lonrpNav = document.getElementById('lonrpNav');
    if (lonrpNav) {
      lonrpNav.addEventListener('click', function (e) {
        var btn = e.target.closest('[data-dashboard]');
        if (btn) setDashboard(btn.getAttribute('data-dashboard'));
      });
    }
    document.addEventListener('click', function (e) {
      var goto = e.target.closest('[data-goto-dashboard]');
      if (goto) {
        e.preventDefault();
        setDashboard(goto.getAttribute('data-goto-dashboard'));
      }
    });
    var propSel = document.getElementById('lonrpPropertySelect');
    if (propSel) {
      propSel.addEventListener('change', function () { setScope(propSel.value); });
    }
    function syncLonrpParamControls() {
      var map = [
        ['lonrpCurrencySelect', 'lonrpSidebarCurrency', 'lonrpCurrency'],
        ['lonrpCompareSelect', 'lonrpSidebarCompare', 'lonrpCompare']
      ];
      map.forEach(function (row) {
        var top = document.getElementById(row[0]);
        var side = document.getElementById(row[1]);
        if (top) top.value = state[row[2]];
        if (side) side.value = state[row[2]];
      });
      ['lonrpBudgetSelect', 'lonrpSidebarBudget'].forEach(function (id) {
        var el = document.getElementById(id);
        if (el) el.value = state.lonrpBudget;
      });
      ['lonrpSidebarPeriod'].forEach(function (id) {
        var el = document.getElementById(id);
        if (el) el.value = state.lonrpPeriod;
      });
      var dateEl = document.getElementById('lonrpSidebarDate');
      if (dateEl) dateEl.value = state.lonrpDateFilter;
    }
    syncLonrpParamControls();
    function onLonrpParamChange(key, value) {
      state[key] = value;
      syncLonrpParamControls();
      renderAll();
    }
    ['lonrpCurrencySelect', 'lonrpCompareSelect', 'lonrpBudgetSelect'].forEach(function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      el.addEventListener('change', function () {
        if (id === 'lonrpCurrencySelect') onLonrpParamChange('lonrpCurrency', el.value);
        if (id === 'lonrpCompareSelect') onLonrpParamChange('lonrpCompare', el.value);
        if (id === 'lonrpBudgetSelect') onLonrpParamChange('lonrpBudget', el.value);
      });
    });
    ['lonrpSidebarCurrency', 'lonrpSidebarCompare', 'lonrpSidebarBudget'].forEach(function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      el.addEventListener('change', function () {
        if (id === 'lonrpSidebarCurrency') onLonrpParamChange('lonrpCurrency', el.value);
        if (id === 'lonrpSidebarCompare') onLonrpParamChange('lonrpCompare', el.value);
        if (id === 'lonrpSidebarBudget') onLonrpParamChange('lonrpBudget', el.value);
      });
    });
    var sidePeriod = document.getElementById('lonrpSidebarPeriod');
    if (sidePeriod) {
      sidePeriod.addEventListener('change', function () {
        var lp = sidePeriod.value;
        state.lonrpPeriod = lp;
        state.period = lp === 'mtd' ? 'mtd' : lp === 'fy' ? 'fcst' : 'ytd';
        syncPeriodSegButtons();
        renderAll();
      });
    }
    var sideDate = document.getElementById('lonrpSidebarDate');
    if (sideDate) {
      sideDate.addEventListener('change', function () {
        state.lonrpDateFilter = sideDate.value;
        renderAll();
      });
    }
    var genericMount = document.getElementById('lonrpGenericMount');
    if (genericMount) {
      genericMount.addEventListener('click', function (e) {
        var see = e.target.closest('[data-see-list]');
        if (see) {
          e.preventDefault();
          showToast('See .. — ' + see.getAttribute('data-see-list') + ' (illustrative; comp list popup in Tableau)');
          return;
        }
        if (e.target.closest('.lonrp-exec-info-btn') || e.target.closest('.tb-info-i')) {
          e.preventDefault();
          setDashboard('info');
        }
      });
    }

    document.getElementById('periodSeg').addEventListener('click', function (e) {
      var btn = e.target.closest('button[data-period]');
      if (btn) setPeriod(btn.getAttribute('data-period'));
    });

    document.addEventListener('click', function (e) {
      if (e.target.id === 'paceHealthGoGroup') {
        e.preventDefault();
        setReport('pace');
      }
    });
    var btnOpenPace = document.getElementById('btnOpenPaceWorkspace');
    if (btnOpenPace) btnOpenPace.addEventListener('click', function () { setDashboard('pace'); });

    document.getElementById('groupGranularitySeg').addEventListener('click', function (e) {
      var btn = e.target.closest('button[data-granularity]');
      if (!btn) return;
      state.groupGranularity = btn.getAttribute('data-granularity');
      document.querySelectorAll('#groupGranularitySeg button').forEach(function (b) {
        b.classList.toggle('active', b === btn);
      });
      renderAll();
    });
    document.getElementById('groupCompareSelect').addEventListener('change', function (e) {
      state.groupCompare = e.target.value;
      renderAll();
    });
    document.getElementById('btnGroupExportExec').addEventListener('click', function () {
      printPaceExec();
      showToast('Exec PDF print opened for Group Pace');
    });
    var btnOwnerPrint = document.getElementById('btnOwnerNarrativePrint');
    if (btnOwnerPrint) {
      btnOwnerPrint.addEventListener('click', function (e) {
        e.preventDefault();
        e.stopPropagation();
        printOwnerBrief();
        showToast('Owner brief PDF opened');
      });
    }
    document.getElementById('btnGroupExportCsv').addEventListener('click', function () {
      var rows = Array.from(document.querySelectorAll('#groupPaceBody tr')).map(function (tr) {
        return Array.from(tr.querySelectorAll('td')).map(function (td) { return '"' + td.textContent.replace(/"/g, '""') + '"'; }).join(',');
      });
      var csv = 'Date,Current Total,Current Occ,Current Revenue,Current ADR,Current RevPAR,Prev Total,Prev Occ,Prev Revenue,Prev ADR,Prev RevPAR,Diff Total,Diff Occ,Diff Revenue,Diff ADR,Diff RevPAR\\n' + rows.join('\\n');
      var blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
      var a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'group-booking-pace-' + state.scope + '-' + state.period + '.csv';
      document.body.appendChild(a);
      a.click();
      a.remove();
      showToast('CSV exported');
    });

    document.getElementById('scorecardBody').addEventListener('click', function (e) {
      var row = e.target.closest('tr[data-prop-id]');
      if (!row || e.target.closest('a')) return;
      setScope(row.getAttribute('data-prop-id'));
    });

    document.addEventListener('click', function (e) {
      var link = e.target.closest('a.link-ext');
      if (link) {
        e.preventDefault();
        var sys = link.getAttribute('data-system') || 'Source system';
        var pid = link.getAttribute('data-prop') || state.scope;
        var prop = PROPS.find(function (p) { return p.id === pid; });
        showToast('Would open ' + sys + (prop ? ' — ' + prop.short : '') + ' (demo link)');
      }
    });

    document.getElementById('pillarGrid').addEventListener('click', function (e) {
      var p = e.target.closest('.pillar');
      if (p) openPillarModal(p.getAttribute('data-pillar'));
    });

    document.getElementById('btnKpiDict').addEventListener('click', function () { document.getElementById('modalKpi').classList.add('open'); });
    document.getElementById('btnSourceMap').addEventListener('click', function () { document.getElementById('modalSource').classList.add('open'); });
    document.getElementById('closeKpi').addEventListener('click', function () { document.getElementById('modalKpi').classList.remove('open'); });
    document.getElementById('closeSource').addEventListener('click', function () { document.getElementById('modalSource').classList.remove('open'); });
    document.getElementById('closePillar').addEventListener('click', function () { document.getElementById('modalPillar').classList.remove('open'); });
    document.querySelectorAll('.modal-overlay').forEach(function (m) {
      m.addEventListener('click', function (e) {
        if (e.target === m) m.classList.remove('open');
      });
    });

    document.getElementById('commentPortfolio').addEventListener('input', function () {
      var now = new Date();
      document.getElementById('auditPortfolio').textContent =
        'Last edited by D. Amburn · ' + now.toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' }) + ' CET';
    });

    document.getElementById('btnWeeklyPack').addEventListener('click', function () {
      if (state.dashboard === 'pace') {
        printPaceExec();
        showToast('Weekly commercial pack opened (pace workspace)');
      } else {
        printOwnerBrief();
        showToast('Weekly commercial pack opened (owner brief)');
      }
    });
    document.getElementById('btnPrint').addEventListener('click', function () {
      if (state.dashboard === 'pace') printPaceExec();
      else window.print();
    });

    document.getElementById('btnPartnership').addEventListener('click', function () {
      document.getElementById('drawerOverlay').classList.add('open');
      document.getElementById('drawer').classList.add('open');
    });
    document.getElementById('drawerClose').addEventListener('click', closeDrawer);
    document.getElementById('drawerOverlay').addEventListener('click', closeDrawer);
    function closeDrawer() {
      document.getElementById('drawerOverlay').classList.remove('open');
      document.getElementById('drawer').classList.remove('open');
    }
  }

  window.AHHub = { renderAll: renderAll, setPeriod: setPeriod, setScope: setScope, setReport: setReport, setDashboard: setDashboard };
  bindUi();
  renderAll();
})();
