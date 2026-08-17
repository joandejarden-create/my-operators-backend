const url = 'https://www.dealality.com/old-home?cb=' + Date.now();
fetch(url, { headers: { 'Cache-Control': 'no-cache' } })
  .then((r) => r.text())
  .then((t) => {
    const outcomes = [
      'Reveal More Credible Futures',
      'Strengthen the Opportunity',
      'Reach Better-Fit Counterparties',
      'Create Competitive Market Tension',
      'Compare Value-Creation Trade-Offs',
      'Pursue the Strongest Direction',
    ];
    const platform = [
      'Opportunity Assessment',
      'Strategic Path Intelligence',
      'Counterparty Intelligence',
      'Opportunity Packaging and Engagement',
      'Proposal and Value Comparison',
      'Negotiation and Decision Workspace',
    ];
    for (const s of outcomes) console.log('OUT', s, t.includes(s));
    for (const s of platform) console.log('PLAT', s, t.includes(s));
    const p = t.indexOf('id="modules-panel-platform"');
    console.log('platform panel snippet', t.slice(p, p + 120));
    console.log('modp-1', t.includes('id="modp-1"'));
    console.log('modules-grid-platform', t.includes('modules-grid-platform'));
  });
