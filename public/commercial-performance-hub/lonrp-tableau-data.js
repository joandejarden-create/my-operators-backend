/* LONRP Tableau workbook — static snapshot values (Executive Overview screenshot) */
(function (global) {
  'use strict';

  var EXECUTIVE_YTD = {
    financeAsOf: '202505',
    finance: {
      occ: { value: '80%', vsBud: { t: '3%pt vs. Budget', dir: 'down' }, vsLy: { t: '3%pt vs. LY', dir: 'down' }, vsLyMi: { t: '0%pt vs. LY market', dir: 'flat' } },
      adr: { value: '175', vsBud: { t: '7.8% vs. Budget', dir: 'down' }, vsLy: { t: '6% vs. LY', dir: 'down' }, vsLyMi: { t: '4% vs. LY market', dir: 'down' } },
      revpar: { value: '140', vsBud: { t: '11.2% vs. Budget', dir: 'down' }, vsLy: { t: '9% vs. LY', dir: 'down' }, vsLyMi: { t: '4% vs. LY market', dir: 'down' } }
    },
    marketShare: {
      asOf: '30 April 2025',
      items: [
        { label: 'RevPAR Index (RPI)', value: '142.8', delta: '-10.8 Pts', dir: 'down' },
        { label: 'Occupancy Index (MPI)', value: '114.1', delta: '-2.9 Pts', dir: 'down' },
        { label: 'Average Rate Index (ARI)', value: '125.2', delta: '-6.2 Pts', dir: 'down' }
      ]
    },
    loyalty: {
      asOf: '31 May 2025',
      items: [
        { label: 'Loyalty occupancy paid', value: '69%', sub: '+5% vs. LY', dir: 'up' },
        { label: 'Loyalty occupancy redeemed', value: '5%', sub: '0% vs. LY', dir: 'flat' },
        { label: 'Loyalty occupancy', value: '74%', sub: '+5% vs. LY', dir: 'up' }
      ]
    },
    distribution: {
      asOf: '31 May 2025',
      items: [
        { label: 'Digital Share', value: '50.0%', sub: '2%pt', dir: 'down' },
        { label: 'OTA Mix', value: '22%', sub: '0%pt', dir: 'up' },
        { label: 'Direct Mix', value: '64%', sub: '1%pt', dir: 'down' }
      ]
    },
    pace: {
      title: 'Future OTB Revenue Pace vs. LY Same Time | Upcoming 4 months',
      footer: 'Transient/Contract as of 31 May 2025 | Group as of 31 May 2025',
      legend: ['Room Reven..', 'Tot Pace Re..'],
      months: [
        { label: 'Jun-25', roomRev: -6, totPace: -12 },
        { label: 'Jul-25', roomRev: 3, totPace: 5 },
        { label: 'Aug-25', roomRev: -9, totPace: -27 },
        { label: 'Sep-25', roomRev: 1, totPace: 4 }
      ]
    },
    scorecard: [
      { text: 'Total Sales', detail: '9.7%pt vs budget', dir: 'down' },
      { text: 'Digital Direct Share 50.0%', detail: '0.6%pt vs goal', dir: 'down' },
      { text: 'Loyalty Enrollments 1,969', detail: '5.1%pt vs goal', dir: 'down', textColor: 'green' },
      { text: 'Loyalty Occupancy 74%', details: [
        { t: '4.8%pt vs LY', dir: 'up' },
        { t: '19.8%pt vs goal', dir: 'up' }
      ]},
      { text: 'Crossover Goal 10% Achieved', detail: '', dir: 'down' },
      { text: 'RevPAR Index', detail: '-10.8pt vs LY', dir: 'down' },
      { text: 'System Adoption 75%', detail: '', dir: 'warn' },
      { text: 'Premium Revenue', detail: '-20% vs LY', dir: 'down', accent: 'purple' }
    ],
    geo: {
      title: 'Geo Source',
      asOf: '9 June 2025',
      center: '5.72M Revenue',
      segments: [
        { label: 'Domestic', pct: 39, color: '#2c5282' },
        { label: 'International', pct: 49, color: '#5ba3a8' },
        { label: 'Regional', pct: 12, color: '#e8a735' },
        { label: 'Unknown', pct: 0, color: '#e8a0c8' }
      ]
    },
    gross: {
      title: 'Gross Revenue last 3 months',
      asOf: '8 June 2025',
      total: '24,128,407',
      vsLy: '-4%',
      vsLyDir: 'down',
      weeks: [
        { label: '09/03', pct: -8 }, { label: '16/03', pct: -5 }, { label: '23/03', pct: -12 },
        { label: '30/03', pct: -6 }, { label: '06/04', pct: -10 }, { label: '13/04', pct: -4 },
        { label: '20/04', pct: -7 }, { label: '27/04', pct: -3 }, { label: '04/05', pct: -9 },
        { label: '11/05', pct: -2 }, { label: '18/05', pct: -6 }, { label: '25/05', pct: -1 },
        { label: '01/06', pct: 7 }, { label: '08/06', pct: 5 }, { label: '15/06', pct: 17 }
      ]
    },
    premium: {
      title: 'Premium Rooms',
      asOf: '30 April 2025',
      center: '1.37M Prem Paid Revenue',
      segments: [
        { label: 'Club/Concierge', pct: 73, color: '#26b14c' },
        { label: 'Suite', pct: 22, color: '#4f779a' },
        { label: 'Premium', pct: 5, color: '#e15759' }
      ]
    }
  };

  global.LonrpTableauData = { EXECUTIVE_YTD: EXECUTIVE_YTD };
})(typeof window !== 'undefined' ? window : this);
