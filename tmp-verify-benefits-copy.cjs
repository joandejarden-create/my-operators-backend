const checks = [
  'How Dealality Helps',
  'See More Possibilities',
  'Present the Opportunity Better',
  'Reach the Right Partners',
  'Create Real Competition',
  'Compare the Options Clearly',
  'Choose What to Pursue',
  'Opportunity Review',
  'Strategic Path Review',
  'Partner Research',
  'Opportunity Package and Outreach',
  'Proposal Comparison',
  'Negotiation and Decision Support',
  'Pursue the Strongest Direction',
  'Platform Capabilities',
];
fetch('https://www.dealality.com/old-home?cb=' + Date.now(), { headers: { 'Cache-Control': 'no-cache' } })
  .then((r) => r.text())
  .then((t) => {
    for (const c of checks) console.log((c === 'Pursue the Strongest Direction' || c === 'Platform Capabilities' ? 'gone' : 'has'), c, t.includes(c));
  });
