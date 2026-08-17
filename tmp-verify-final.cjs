const url = 'https://www.dealality.com/old-home?cb=' + Date.now();
fetch(url, { headers: { 'Cache-Control': 'no-cache' } })
  .then((r) => r.text())
  .then((t) => {
    console.log('css b', t.includes('v20260728b.css'));
    console.log('js v2', t.includes('v20260728b.js') || t.includes('oldhomebenefitstabsv2'));
    const p = t.indexOf('id="modules-panel-platform"');
    console.log(t.slice(Math.max(0, p - 40), p + 140));
    console.log('Owner Outcomes', t.includes('Owner Outcomes'));
    console.log('Negotiation', t.includes('Negotiation and Decision Workspace'));
  });
