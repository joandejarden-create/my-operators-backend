const checks = [
  'perspectives',
  'One Platform. Three Perspectives.',
  'Hotel Owner / Investor',
  'Brand / Operator',
  'Advisor / Service Partner',
  'Confidential by Default',
  'Broker Attribution, Preserved',
  'perspectives.v20260728.css',
  'perspectives.v20260728.js',
  'id="logos"',
];
fetch('https://www.dealality.com/old-home?cb=' + Date.now(), { headers: { 'Cache-Control': 'no-cache' } })
  .then((r) => r.text())
  .then((t) => {
    for (const c of checks) console.log(c, t.includes(c));
  });
